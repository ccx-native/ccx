from __future__ import annotations

import errno
import json
import os
import select
import queue
import random
import signal
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from typing import Any

from ccx.codex.control import _UnixWebSocket, _masked_frame


class ProtocolError(RuntimeError):
    """A JSON-RPC or local protocol failure with machine-readable context."""

    def __init__(
        self,
        message: str = "protocol error",
        *,
        method: str | None = None,
        code: int | None = None,
        data: Any = None,
        request_id: Any = None,
    ):
        self.method = method
        self.code = code
        self.data = data
        self.request_id = request_id
        self.message = str(message)
        # A message that already names its method (a local timeout reads
        # "<method> timed out") keeps its own wording; only an unqualified
        # server message gets the "<method> failed:" prefix.
        detail = (
            f"{method} failed: {self.message}"
            if method and not self.message.startswith(f"{method} ")
            else self.message
        )
        super().__init__(detail)


class ProtocolTransportError(ProtocolError):
    """The local app-server protocol channel became unusable."""


class ProtocolNotSent(ProtocolTransportError):
    """This request is proven to have emitted zero bytes to app-server stdin.

    It can mean a previously failed channel or a local write/lock deadline
    expiring while the shared channel is healthy. The app-server cannot have
    seen this request, but that is not permission to clear a separately
    fsynced CCX submitted-unknown claim or automatically retry native creation.
    """


class ServerOverloaded(ProtocolError):
    """The server explicitly rejected a request as temporarily overloaded."""


def _error_from_response(
    method: str,
    error: Any,
    request_id: Any,
) -> ProtocolError:
    if isinstance(error, dict):
        code = error.get("code")
        if isinstance(code, bool) or not isinstance(code, int):
            code = None
        message = error.get("message")
        if not isinstance(message, str):
            message = str(message if message is not None else error)
        data = error.get("data")
    else:
        code = None
        message = str(error)
        data = None
    error_type = ServerOverloaded if code == -32001 else ProtocolError
    return error_type(
        message,
        method=method,
        code=code,
        data=data,
        request_id=request_id,
    )


def _result_from_response(
    method: str, response: Any, request_id: Any
) -> dict[str, Any]:
    if isinstance(response, BaseException):
        raise response
    if "error" in response:
        raise _error_from_response(method, response["error"], request_id)
    result = response.get("result")
    return result if isinstance(result, dict) else {"value": result}


class PendingRequest:
    """One sent request whose answer stays observable after callers stop waiting.

    The client's reader delivers the response, or the transport failure that
    ends the connection, through the same `put` interface as a result queue,
    so exactly one definitive outcome always arrives.
    """

    def __init__(self, method: str):
        self.method = method
        self.request_id: Any = None
        self._lock = threading.Lock()
        self._done = threading.Event()
        self._response: Any = None
        self._callbacks: list[Callable[["PendingRequest"], None]] = []

    def put(self, response: Any, block: bool = True, timeout: Any = None) -> None:
        with self._lock:
            if self._done.is_set():
                return
            self._response = response
            self._done.set()
            callbacks, self._callbacks = self._callbacks, []
        for callback in callbacks:
            self._run(callback)

    put_nowait = put

    def done(self) -> bool:
        return self._done.is_set()

    def wait(self, timeout: float) -> bool:
        return self._done.wait(max(0.0, timeout))

    def add_done_callback(self, callback: Callable[["PendingRequest"], None]) -> None:
        """Run `callback` once answered: now if it already is, else on the reader."""
        with self._lock:
            if not self._done.is_set():
                self._callbacks.append(callback)
                return
        self._run(callback)

    def result(self) -> dict[str, Any]:
        if not self._done.is_set():
            raise ProtocolError(
                f"{self.method} has not been answered yet", method=self.method
            )
        return _result_from_response(self.method, self._response, self.request_id)

    def original_provider_rejection(self) -> dict[str, Any] | None:
        """Exact coded JSON-RPC error *reply* for this original request only.

        A locally raised ProtocolError may itself have an integer code; a
        transport exception, timeout, or mismatched response ID is never
        positive evidence that the provider refused the native operation.
        Large/unstructured provider errors also remain uncertain rather than
        silently truncating the original rejection on durable replay.
        """
        import hashlib
        with self._lock:
            if not self._done.is_set():
                return None
            response = self._response
            request_id = self.request_id
        if (not isinstance(response, dict)
                or response.get("jsonrpc") != "2.0"
                or type(response.get("id")) is not type(request_id)
                or response.get("id") != request_id
                or not isinstance(response.get("error"), dict)):
            return None
        error = response["error"]
        code, message = error.get("code"), error.get("message")
        if (type(code) is not int or not isinstance(message, str)
                or len(message.encode("utf-8")) > 8192):
            return None
        try:
            raw = json.dumps(response, sort_keys=True, separators=(",", ":"),
                             ensure_ascii=False, allow_nan=False).encode("utf-8")
        except (TypeError, ValueError, UnicodeError):
            return None
        if len(raw) > 64 * 1024:
            return None
        return {
            "code": code, "message": message,
            "requestId": request_id,
            "nativeReplySha256": hashlib.sha256(raw).hexdigest(),
        }

    def _run(self, callback: Callable[["PendingRequest"], None]) -> None:
        # Callbacks usually run on the client's reader thread, where an
        # escaping exception would stop every later message from being read.
        try:
            callback(self)
        except Exception as error:  # noqa: BLE001
            print(
                f"ccx: {self.method} completion callback failed: {error!r}",
                file=sys.stderr, flush=True,
            )


def error_payload(error: ProtocolError) -> dict[str, Any]:
    """Project one ``ProtocolError`` into a JSON-compatible RPC member."""
    return {
        "method": error.method,
        "code": error.code,
        "message": error.message,
        "data": error.data,
        "requestId": error.request_id,
        "transport": isinstance(error, ProtocolTransportError),
    }


def _user_command_env() -> dict[str, str]:
    """Environment for app-server, whose shell commands are the user's.

    codexd itself runs with CCX's package on PYTHONPATH (``package_env``);
    restore the caller's original value so it never shadows project packages.
    """
    from ccx.codex.worker_environment import scrub_shared_codex_identity
    env = scrub_shared_codex_identity(os.environ.copy())
    # The app-server serves many threads. A caller's BASH_ENV belongs only
    # inside that worker's generated, full-UUID thread policy, not globally.
    env.pop("BASH_ENV", None)
    if "CCX_CALLER_PYTHONPATH" in env:
        original = env.pop("CCX_CALLER_PYTHONPATH")
        if original:
            env["PYTHONPATH"] = original
        else:
            env.pop("PYTHONPATH", None)
    return env


# One ephemeral watcher per socket-mode app-server, not a daemon/service.
# Its stdin writer belongs only to codexd. If codexd is SIGKILLed, EOF
# terminates the watcher + launcher + native child group, even though the
# WebSocket listener would otherwise keep running without its CCX owner.
_PARENT_LIFETIME_WATCHER = (
    "import os, signal, sys, time\n"
    "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
    "sys.stdin.buffer.read(1)\n"
    "os.killpg(os.getpgrp(), signal.SIGTERM)\n"
    "time.sleep(0.7)\n"
    "os.killpg(os.getpgrp(), signal.SIGKILL)\n"
)


class AppServerClient:
    """Small stable-API JSONL client for one dedicated Codex app-server."""

    _MAX_OVERLOAD_RETRIES = 5
    _OVERLOAD_DELAY_S = 0.1
    _OVERLOAD_DELAY_CAP_S = 1.0
    _WRITE_TIMEOUT_S = 2.0  # bounded even for notifications/server responses
    _MAX_JSONL_FRAME_BYTES = 4 * 1024 * 1024

    def __init__(
        self,
        command: list[str],
        *,
        cwd: str,
        on_notification: Callable[[str, dict[str, Any]], None],
        on_server_request: Callable[[Any, str, dict[str, Any]], None],
        stderr=None,
        socket_path: str | None = None,
    ):
        self.on_notification = on_notification
        self.on_server_request = on_server_request
        self._ws: _UnixWebSocket | None = None
        self._socket_shutdown = False
        self._parent_watcher: subprocess.Popen | None = None
        self._owned_pgid: int | None = None
        if socket_path:
            # process_group=0 makes this watcher the sole leader of a new
            # process group within codexd's session. The native launcher
            # joins it, but .process remains the actual app-server PID.
            # The watcher has no user env/credentials; only a stdin pipe
            # held by codexd. The app-server inherits neither pipe endpoint.
            watcher = subprocess.Popen(
                [sys.executable, "-I", "-c", _PARENT_LIFETIME_WATCHER],
                cwd=cwd, env={}, stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                close_fds=True, process_group=0,
            )
            self._parent_watcher = watcher
            self._owned_pgid = watcher.pid
        try:
            self.process = subprocess.Popen(
                command,
                cwd=cwd,
                env=_user_command_env(),
                stdin=subprocess.DEVNULL if socket_path else subprocess.PIPE,
                stdout=subprocess.DEVNULL if socket_path else subprocess.PIPE,
                stderr=stderr,
                text=True,
                bufsize=1,
                **({"umask": 0o077, "process_group": self._owned_pgid}
                   if socket_path else {}),
            )
        except BaseException:
            if self._parent_watcher is not None:
                # Only the group we just created; the native launch failed.
                try:
                    os.killpg(self._parent_watcher.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                self._parent_watcher.wait()
                self._parent_watcher.stdin.close()
            raise
        if socket_path:
            try:
                deadline = time.monotonic() + 20
                while True:
                    if self.process.poll() is not None:
                        raise ProtocolTransportError("Codex socket app-server exited during startup")
                    try:
                        self._ws = _UnixWebSocket(
                            socket_path, handshake_timeout=min(1., max(.1, deadline - time.monotonic())))
                        self._ws.sock.setblocking(False)
                        break
                    except OSError as error:
                        if time.monotonic() >= deadline:
                            raise ProtocolTransportError(
                                f"Codex app-server WebSocket not ready: {error}") from error
                        time.sleep(.05)
            except BaseException:
                self.close()
                raise
        self._next_id = 1
        self._pending: dict[Any, queue.Queue] = {}
        # Registry/reader/failure state is separate from the original client
        # writer lock. A blocked writer never delays reader-side _fail.
        self._state_lock = threading.Lock()
        self._lock = threading.Lock()
        if self.process.stdin is not None:
            # Exclusively use raw UTF-8 os.write from this point onward. Do not
            # use TextIOWrapper.write/flush alongside this nonblocking fd.
            os.set_blocking(self.process.stdin.fileno(), False)
        self._closed = threading.Event()
        self._failure: ProtocolTransportError | None = None
        self._reader = threading.Thread(
            target=self._read_loop, name="ccx-codex-appserver", daemon=True
        )
        self._reader.start()

    @property
    def failure(self) -> ProtocolTransportError | None:
        """The latched transport failure, if this client has stopped reading.

        The reader is the only consumer of app-server notifications, so a
        latched failure means the connection is unusable even while the
        subprocess is still alive. Supervisors must treat it as fatal.
        """
        with self._state_lock:
            return self._failure

    @classmethod
    def _encode_frame(cls, message: dict[str, Any]) -> bytes:
        # Keep the JSON ASCII escape convention of the original text writer:
        # it also encodes surrogateescape filenames/argv (e.g. \udcff) as
        # valid JSON without attempting to UTF-8 encode an unpaired surrogate.
        # Real Unicode still round-trips through json.loads on the provider.
        frame = (json.dumps(message, separators=(",", ":"),
                            ensure_ascii=True, allow_nan=False) + "\n").encode("utf-8")
        if len(frame) > cls._MAX_JSONL_FRAME_BYTES:
            raise ProtocolError("app-server JSONL frame exceeds bounded write size")
        return frame

    def _wire_frame(self, message: dict[str, Any]) -> bytes:
        raw = self._encode_frame(message)
        return _masked_frame(0x1, raw[:-1]) if self._ws is not None else raw

    def _write_frame_locked(self, frame: bytes, deadline: float, *,
                            method: str | None = None) -> None:
        """Deadline covers every byte, including a full or stalled stdin pipe.

        The caller owns `_lock`. No text buffer or arbitrary worker wait
        is involved; the reader can fail pending requests through `_state_lock` even
        during a stalled write. Once any byte might have escaped, a timeout or
        partial frame is transport-unknown, never a safe retry permission.
        """
        sent = 0
        pipe_blocked = False
        full_write_window = False
        while sent < len(frame):
            with self._state_lock:
                failed = self._failure
            if failed is not None:
                if sent == 0:
                    raise ProtocolNotSent(failed.message, method=method) from failed
                raise ProtocolTransportError(
                    "app-server channel failed after partial JSONL frame",
                    method=method,
                ) from failed
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                if sent > 0:
                    raise ProtocolTransportError(
                        "app-server write deadline exceeded after partial frame",
                        method=method,
                    )
                if full_write_window:
                    # An unresponsive pipe over the entire normal write
                    # window is a shared transport failure, even at zero bytes.
                    raise ProtocolTransportError(
                        "app-server pipe failed to make progress for full write window",
                        method=method,
                    )
                # This request's shorter admission budget expired before any
                # native bytes escaped. No shared pipe corruption or proof of
                # provider rejection: siblings may still use the live client.
                raise ProtocolNotSent(
                    "app-server write deadline exceeded before first byte",
                    method=method,
                )
            fd = (self._ws.sock.fileno() if self._ws is not None
                  else self.process.stdin.fileno() if self.process.stdin is not None
                  else None)
            if fd is None:
                raise ProtocolTransportError(
                    "app-server write channel unavailable", method=method,
                )
            try:
                written = os.write(fd, frame[sent:])
                if written <= 0:
                    raise OSError(errno.EPIPE, "app-server pipe made no write progress")
                sent += written
                continue
            except InterruptedError:
                continue
            except BlockingIOError:
                if not pipe_blocked:
                    pipe_blocked = True
                    # Tolerate only scheduling/serialization jitter when the
                    # full normal window was allotted to this pipe write.
                    # A caller whose original <=2s admission was consumed by
                    # claim/marker fsync gets a local zero-byte timeout instead.
                    slack = min(.01, self._WRITE_TIMEOUT_S / 10)
                    full_write_window = (
                        deadline - time.monotonic() >= self._WRITE_TIMEOUT_S - slack
                    )
            except (OSError, ValueError) as error:
                # EPIPE / a genuinely broken fd is fatal regardless of byte
                # count. Only a proven budget expiry has local semantics.
                raise ProtocolTransportError(
                    f"app-server pipe write failed: {error}", method=method,
                ) from error
            try:
                # Check for reader failure at least once per tenth-second,
                # without resetting the one absolute end-to-end deadline.
                select.select([], [fd], [], min(.1, max(0., deadline - time.monotonic())))
            except InterruptedError:
                continue
            except (OSError, ValueError) as error:
                # A broken poll descriptor is a shared transport fault, not a
                # caller-local timeout even if this frame sent no bytes.
                raise ProtocolTransportError(
                    f"app-server pipe wait failed: {error}", method=method,
                ) from error

    def _write(self, message: dict[str, Any]) -> None:
        """Bounded notification/response write; never share text buffering."""
        self._write_bytes(self._wire_frame(message))

    def _write_bytes(self, frame: bytes) -> None:
        """Bounded serialized write for JSON messages and WS pong frames."""
        deadline = time.monotonic() + self._WRITE_TIMEOUT_S
        if not self._lock.acquire(timeout=max(0., deadline - time.monotonic())):
            # No bytes from this notification escaped; an unrelated writer
            # owning the lock is not evidence that its channel is corrupt.
            raise ProtocolNotSent("app-server writer lock deadline exceeded")
        error = None
        pending = []
        failure = None
        try:
            try:
                self._write_frame_locked(frame, deadline)
            except ProtocolNotSent as exc:
                # A short local write deadline cannot poison unrelated sends.
                error = exc
            except ProtocolTransportError as exc:
                error = exc
                failure = ProtocolTransportError(exc.message)
                pending = self._latch_failure(failure)
        finally:
            self._lock.release()
        if error is not None:
            # Fatal state (if any) was latched before releasing the writer
            # lock. A caller-local zero-byte timeout has no sibling callbacks.
            # All actual failure callbacks remain outside both client locks.
            if failure is not None:
                self._deliver_failure(pending, failure)
            raise error

    def _latch_failure(self, failure: ProtocolTransportError) -> list[Any]:
        """Publish fatal channel state without acquiring the writer lock."""
        with self._state_lock:
            if self._failure is not None:
                return []
            self._failure = failure
            self._closed.set()
            pending = list(self._pending.values())
            self._pending.clear()
            return pending

    @staticmethod
    def _deliver_failure(pending: list[Any], failure: ProtocolTransportError) -> None:
        """Deliver outside both client locks, including registered callbacks."""
        for result_queue in pending:
            try:
                result_queue.put_nowait(failure)
            except queue.Full:
                pass

    def _fail(self, failure: ProtocolTransportError) -> None:
        self._deliver_failure(self._latch_failure(failure), failure)

    def _send(
        self, method: str, params: dict[str, Any] | None, sink: Any,
        *, write_deadline: float | None = None,
    ) -> int:
        """One JSONL request with bounded lock and pipe admission.

        An unexpired, locally shortened write deadline can fail before any
        byte without damaging the shared transport or siblings. Lock deadline
        expiry has the same request-only semantics. A partial frame, EPIPE or
        independently failed channel remains fatal and wakes all siblings.
        None of these outcomes undo a CCX submitted-unknown activation claim.
        """
        deadline = (time.monotonic() + self._WRITE_TIMEOUT_S
                    if write_deadline is None else write_deadline)
        if not self._lock.acquire(timeout=max(0., deadline - time.monotonic())):
            # Writer-lock contention proves zero bytes from this request.
            # The existing pending siblings and physical JSONL stream survive.
            raise ProtocolNotSent(
                "app-server writer lock deadline exceeded", method=method,
            )
        error = None
        pending = []
        failure = None
        try:
            with self._state_lock:
                if self._failure is not None:
                    raise ProtocolNotSent(
                        self._failure.message, method=method,
                    ) from self._failure
                request_id = self._next_id
                self._next_id += 1
            message = {"jsonrpc": "2.0", "id": request_id, "method": method}
            if params is not None:
                message["params"] = params
            frame = self._wire_frame(message)
            with self._state_lock:
                if self._failure is not None:
                    raise ProtocolNotSent(
                        self._failure.message, method=method,
                    ) from self._failure
                self._pending[request_id] = sink
            try:
                self._write_frame_locked(frame, deadline, method=method)
            except ProtocolNotSent as exc:
                # The frame was not emitted: forget this request sink only.
                # Another request's still-pending reply remains observable.
                with self._state_lock:
                    if self._pending.get(request_id) is sink:
                        del self._pending[request_id]
                error = exc
            except ProtocolTransportError as exc:
                error = exc
                failure = ProtocolTransportError(exc.message, method=method)
                pending = self._latch_failure(failure)
        finally:
            self._lock.release()
        if error is not None:
            # Shared transport failures were latched before unlocking, and
            # callbacks may safely re-enter client methods. A zero-byte local
            # deadline has no shared failure or pending-sibling deliveries.
            if failure is not None:
                self._deliver_failure(pending, failure)
            raise error
        return request_id

    def begin_request(
        self, method: str, params: dict[str, Any] | None = None,
        *, write_deadline: float | None = None,
    ) -> "PendingRequest":
        """Send one request without waiting for it.

        Unlike `request`, no caller timeout ever forgets it: the answer, or
        the transport failure that ends the connection, always reaches the
        returned `PendingRequest`. Use it for a mutation whose completion
        something else must be able to wait for later.
        """
        pending = PendingRequest(method)
        pending.request_id = self._send(
            method, params, pending, write_deadline=write_deadline,
        )
        return pending

    def _request_once(
        self,
        method: str,
        params: dict[str, Any] | None,
        timeout: float,
    ) -> dict[str, Any]:
        result_queue: queue.Queue = queue.Queue(maxsize=1)
        # Preserve the response-read timeout as a separate observation wait
        # after frame submission. Write admission has its own bounded budget,
        # and a failed write is never a provider rejection or read timeout.
        request_id = self._send(
            method, params, result_queue,
            write_deadline=time.monotonic() + self._WRITE_TIMEOUT_S,
        )

        try:
            response = result_queue.get(timeout=max(0., timeout))
        except queue.Empty as error:
            with self._state_lock:
                self._pending.pop(request_id, None)
            raise ProtocolError(f"{method} timed out", method=method) from error
        return _result_from_response(method, response, request_id)

    def request(
        self, method: str, params: dict[str, Any] | None = None, timeout: float = 30
    ) -> dict[str, Any]:
        deadline = time.monotonic() + max(0.0, timeout)
        delay = self._OVERLOAD_DELAY_S
        last_overload: ServerOverloaded | None = None
        for attempt in range(self._MAX_OVERLOAD_RETRIES + 1):
            remaining = max(0.0, deadline - time.monotonic())
            if attempt > 0 and remaining <= 0:
                # Never write an automatic retry after its shared observation
                # budget has expired: the overload rejection only proved that
                # the previous request was not processed.
                assert last_overload is not None
                raise last_overload
            try:
                return self._request_once(method, params, remaining)
            except ServerOverloaded as error:
                last_overload = error
                if attempt >= self._MAX_OVERLOAD_RETRIES:
                    raise
                remaining = max(0.0, deadline - time.monotonic())
                sleep_for = min(
                    remaining,
                    delay + random.uniform(0.0, delay * 0.25),
                )
                if sleep_for <= 0:
                    raise error
                time.sleep(sleep_for)
                delay = min(self._OVERLOAD_DELAY_CAP_S, delay * 2)

    def notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        message = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            message["params"] = params
        self._write(message)

    def respond(self, request_id: Any, result: dict[str, Any]) -> None:
        self._write({"jsonrpc": "2.0", "id": request_id, "result": result})

    def reject_request(self, request_id: Any, message: str) -> None:
        self._write({
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": -32601, "message": message},
        })

    def _messages(self):
        if self._ws is None:
            assert self.process.stdout is not None
            yield from self.process.stdout
        else:
            while not self._closed.is_set():
                yield self._ws.recv_text(
                    on_ping=lambda payload: self._write_bytes(_masked_frame(0xA, payload)))

    def _read_loop(self) -> None:
        try:
            for raw in self._messages():
                try:
                    message = json.loads(raw)
                except ValueError as error:
                    excerpt = repr(raw[:200])
                    raise ProtocolTransportError(
                        f"invalid JSON from app-server stdout: {excerpt}"
                    ) from error
                if not isinstance(message, dict):
                    raise ProtocolTransportError(
                        "invalid app-server protocol message: expected object"
                    )
                request_id = message.get("id")
                if request_id is not None and (
                    "result" in message or "error" in message
                ):
                    with self._state_lock:
                        pending = self._pending.pop(request_id, None)
                    if pending:
                        pending.put(message)
                    continue
                method = message.get("method")
                if not method:
                    if request_id is not None:
                        raise ProtocolTransportError(
                            "invalid app-server response: missing result or error"
                        )
                    continue
                params = message.get("params")
                if not isinstance(params, dict):
                    params = {}
                try:
                    if request_id is not None:
                        self.on_server_request(request_id, method, params)
                    else:
                        self.on_notification(method, params)
                except Exception as error:
                    # A message whose processing raised cannot be treated as
                    # consumed, and skipping it would leave the daemon's view
                    # of a worker permanently behind the runtime. Fail the
                    # shared client with the real cause instead.
                    raise ProtocolTransportError(
                        f"app-server {method} handler failed: "
                        f"{error!r}"[:400]
                    ) from error
        except ProtocolTransportError as error:
            self._fail(error)
        except (OSError, EOFError, ValueError) as error:
            self._fail(ProtocolTransportError(f"Codex app-server channel closed: {error}"))
        finally:
            self._fail(
                ProtocolTransportError("Codex app-server exited")
            )

    def close(self) -> None:
        if self._ws is not None or self.process.stdin is None:
            if self._socket_shutdown:
                return
            self._socket_shutdown = True
            if hasattr(self, "_closed"):
                self._closed.set()
            # The socket-mode launcher spawns the vendor binary. Terminating
            # only the launcher can orphan it; the child has its own process
            # group, separate from codexd and every unrelated Codex process.
            ws = self._ws
            if ws is not None:
                try:
                    ws.sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                ws.sock.close()
            watcher = getattr(self, "_parent_watcher", None)
            if watcher is not None and watcher.stdin is not None and not watcher.stdin.closed:
                watcher.stdin.close()
            pgid = getattr(self, "_owned_pgid", None) or self.process.pid
            try:
                os.killpg(pgid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                self.process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                pass
            # A vendor child can survive its launcher. The watcher anchors
            # this group until it is terminated/reaped even if the leader
            # app-server exited first.
            try:
                os.killpg(pgid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            try:
                self.process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                pass
            if watcher is not None:
                try:
                    watcher.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    watcher.kill()
                    watcher.wait()
            return
        if self.process.poll() is not None:
            return
        # stdin EOF is the app-server's clean shutdown signal and the only one
        # the `codex` launcher reliably propagates: SIGTERM kills the launcher
        # while orphaning the vendor-binary grandchild, which then keeps
        # polling its backing service from under init.
        try:
            if self.process.stdin is not None:
                self.process.stdin.close()
        except OSError:
            pass
        try:
            self.process.wait(timeout=3)
            return
        except subprocess.TimeoutExpired:
            pass
        self.process.terminate()
        try:
            self.process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=3)
