"Small dependency-free client for Codex's local app-server control socket.\n\nNormal CCX-owned Codex workers use :mod:`ccx.codex.daemon`; this transport is only for\nwaking a *parent* Codex thread owned by the user's primary Codex runtime/UI.\n"

from __future__ import annotations

from ccx.version import ccx_version

import base64
import hashlib
import json
import os
from pathlib import Path
import queue
import select
import socket
import struct
import threading
from typing import Any, Callable

_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


class CodexControlError(RuntimeError):
    def __init__(self, method: str, error: Any):
        self.method = method
        self.error = error
        self.code = error.get("code") if isinstance(error, dict) else None
        self.data = error.get("data") if isinstance(error, dict) else None
        message = error.get("message") if isinstance(error, dict) else str(error)
        self.message = str(message or error)
        super().__init__(f"{method} failed: {self.message}")


def _masked_frame(opcode: int, payload: bytes = b"") -> bytes:
    """Wire bytes for a client-to-server WebSocket frame (always masked)."""
    mask = os.urandom(4)
    size = len(payload)
    if size < 126:
        header = bytes([0x80 | opcode, 0x80 | size])
    elif size < 65536:
        header = bytes([0x80 | opcode, 0x80 | 126]) + struct.pack("!H", size)
    else:
        header = bytes([0x80 | opcode, 0x80 | 127]) + struct.pack("!Q", size)
    return header + mask + bytes(value ^ mask[i % 4] for i, value in enumerate(payload))


class _UnixWebSocket:
    def __init__(self, path: str, *, handshake_timeout: float | None = None):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        if handshake_timeout is not None:
            self.sock.settimeout(handshake_timeout)
        self.sock.connect(path)
        self._write_lock = threading.Lock()
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        request = (
            "GET /rpc HTTP/1.1\r\n"
            "Host: localhost\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n"
        ).encode("ascii")
        self.sock.sendall(request)
        head = b""
        while b"\r\n\r\n" not in head:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("Codex control socket closed during WebSocket handshake")
            head += chunk
            if len(head) > 65536:
                raise ConnectionError("oversized Codex control WebSocket handshake")
        header, self._buffer = head.split(b"\r\n\r\n", 1)
        lines = header.decode("latin1").split("\r\n")
        if not lines or " 101 " not in (" " + lines[0] + " "):
            raise ConnectionError(
                f"Codex control WebSocket upgrade failed: {lines[0] if lines else header!r}"
            )
        headers: dict[str, str] = {}
        for line in lines[1:]:
            if ":" in line:
                k, v = line.split(":", 1)
                headers[k.strip().lower()] = v.strip()
        expected = base64.b64encode(
            hashlib.sha1((key + _GUID).encode("ascii")).digest()
        ).decode("ascii")
        if headers.get("sec-websocket-accept") != expected:
            raise ConnectionError("Codex control WebSocket accept key mismatch")
        if handshake_timeout is not None:
            self.sock.settimeout(None)

    def _read_exact(self, size: int) -> bytes:
        out = b""
        if self._buffer:
            take = min(size, len(self._buffer))
            out, self._buffer = self._buffer[:take], self._buffer[take:]
        while len(out) < size:
            try:
                chunk = self.sock.recv(size - len(out))
            except BlockingIOError:
                # Socket-mode codexd writes are bounded/nonblocking; the
                # single WS reader tolerates that flag without busy-spinning.
                select.select([self.sock], [], [], 0.1)
                continue
            if not chunk:
                raise EOFError("Codex control WebSocket closed")
            out += chunk
        return out

    def _send_frame(self, opcode: int, payload: bytes = b"") -> None:
        with self._write_lock:
            self.sock.sendall(_masked_frame(opcode, payload))

    def send_text(self, text: str) -> None:
        self._send_frame(0x1, text.encode("utf-8"))

    def recv_text(self, *, on_ping: Callable[[bytes], None] | None = None) -> str:
        pieces: list[bytes] = []
        started = False
        while True:
            b1, b2 = self._read_exact(2)
            fin = bool(b1 & 0x80)
            opcode = b1 & 0x0F
            masked = bool(b2 & 0x80)
            size = b2 & 0x7F
            if size == 126:
                size = struct.unpack("!H", self._read_exact(2))[0]
            elif size == 127:
                size = struct.unpack("!Q", self._read_exact(8))[0]
            mask = self._read_exact(4) if masked else None
            payload = self._read_exact(size)
            if mask:
                payload = bytes(value ^ mask[i % 4] for i, value in enumerate(payload))
            if opcode == 0x8:
                raise EOFError("Codex control WebSocket close frame")
            if opcode == 0x9:
                if on_ping is not None:
                    on_ping(payload)
                else:
                    self._send_frame(0xA, payload)
                continue
            if opcode == 0xA:
                continue
            if opcode == 0x1:
                pieces = [payload]
                started = True
            elif opcode == 0x0 and started:
                pieces.append(payload)
            else:
                continue
            if fin:
                return b"".join(pieces).decode("utf-8")

    def close(self) -> None:
        try:
            self._send_frame(0x8)
        except Exception:
            pass
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass
        try:
            self.sock.close()
        except Exception:
            pass


def control_socket_path() -> Path:
    # Like Codex itself, an empty CODEX_HOME means unset (not the current directory).
    codex_home = Path(os.environ.get("CODEX_HOME") or "~/.codex").expanduser()
    return codex_home / "app-server-control" / "app-server-control.sock"


class CodexControlClient:
    def __init__(
        self,
        *,
        on_notification: Callable[[str, dict[str, Any]], None] | None = None,
        socket_path: str | None = None,
        handshake_timeout: float | None = None,
    ):
        self.ws = _UnixWebSocket(socket_path or str(control_socket_path()),
                                 handshake_timeout=handshake_timeout)
        self.on_notification = on_notification or (lambda method, params: None)
        self._next_id = 1
        self._pending: dict[int, queue.Queue] = {}
        self._lock = threading.Lock()
        self._closed = threading.Event()
        self._failure: BaseException | None = None
        self._reader = threading.Thread(
            target=self._read_loop, name="ccx-codex-parent-control", daemon=True
        )
        self._reader.start()

    def _send(self, value: dict[str, Any]) -> None:
        if self._failure:
            raise RuntimeError(f"Codex control connection failed: {self._failure!r}")
        self.ws.send_text(json.dumps(value, separators=(",", ":")))

    def request(
        self, method: str, params: dict[str, Any] | None = None, *, timeout: float = 30
    ) -> dict[str, Any]:
        with self._lock:
            if self._failure:
                raise RuntimeError(f"Codex control connection failed: {self._failure!r}")
            rid = self._next_id
            self._next_id += 1
            result_queue: queue.Queue = queue.Queue(maxsize=1)
            self._pending[rid] = result_queue
            message: dict[str, Any] = {"id": rid, "method": method}
            if params is not None:
                message["params"] = params
            self._send(message)
        try:
            response = result_queue.get(timeout=timeout)
        except queue.Empty as error:
            with self._lock:
                self._pending.pop(rid, None)
            raise TimeoutError(f"{method} timed out") from error
        if isinstance(response, BaseException):
            raise response
        if "error" in response:
            raise CodexControlError(method, response["error"])
        result = response.get("result")
        return result if isinstance(result, dict) else {"value": result}

    def notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        message: dict[str, Any] = {"method": method}
        if params is not None:
            message["params"] = params
        self._send(message)

    def initialize(self, *, timeout: float = 20) -> None:
        self.request(
            "initialize",
            {
                "clientInfo": {
                    "name": "ccx-parent-notify",
                    "title": "CCX parent notification",
                    "version": ccx_version(),
                },
                "capabilities": {"experimentalApi": False},
            },
            timeout=timeout,
        )
        self.notify("initialized")

    def _read_loop(self) -> None:
        try:
            while not self._closed.is_set():
                message = json.loads(self.ws.recv_text())
                rid = message.get("id")
                if rid is not None and ("result" in message or "error" in message):
                    with self._lock:
                        result_queue = self._pending.pop(rid, None)
                    if result_queue:
                        result_queue.put(message)
                    continue
                method = message.get("method")
                params = message.get("params")
                if not isinstance(params, dict):
                    params = {}
                if rid is not None:
                    self._send(
                        {
                            "id": rid,
                            "error": {
                                "code": -32601,
                                "message": f"CCX parent notification does not handle {method}",
                            },
                        }
                    )
                elif method:
                    self.on_notification(method, params)
        except BaseException as error:
            if not self._closed.is_set():
                self._failure = error
            with self._lock:
                pending = list(self._pending.values())
                self._pending.clear()
            for result_queue in pending:
                try:
                    result_queue.put_nowait(error)
                except queue.Full:
                    pass

    def close(self) -> None:
        self._closed.set()
        self.ws.close()
