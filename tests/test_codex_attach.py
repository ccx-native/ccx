"""Shared Codex attach: socket transport, subscription hold and native TUI."""
from __future__ import annotations
import ccx.store.root as store_root
import ccx.cli.commands.control as commands_control

import errno
import json
import os
import queue
import signal
import socket
import shutil
import struct
import subprocess
import sys
import tempfile
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from ccx.cli import main as cli
from ccx.cli import codex as codex_commands
from ccx.cli.commands import _common as commands
from ccx import constants
from ccx.codex import protocol as codex_protocol
from ccx.codex.daemon import daemon as codexd
from ccx.codex.protocol import AppServerClient, ProtocolNotSent, ProtocolTransportError
from test_generation_store import B, short_root


# A disposable, standalone fake WS app-server; NO Codex provider is launched.
_WS_SERVER = r'''
import base64, hashlib, json, os, socket, struct, sys
path = sys.argv[-1].removeprefix("unix://")
server = socket.socket(socket.AF_UNIX)
server.bind(path)
server.listen(1)
sock, _ = server.accept()
header = b""
while b"\r\n\r\n" not in header:
    header += sock.recv(4096)
key = header.split(b"Sec-WebSocket-Key: ")[1].split(b"\r\n")[0].decode()
accept = base64.b64encode(hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()).decode()
sock.sendall(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + accept + "\r\n\r\n").encode())
def exact(n):
    out=b""
    while len(out)<n:
        part=sock.recv(n-len(out))
        if not part: raise EOFError()
        out+=part
    return out
def send(m):
    data=json.dumps(m).encode()
    header=bytes([0x81, len(data)]) if len(data)<126 else bytes([0x81, 126])+struct.pack("!H",len(data))
    sock.sendall(header+data)
while True:
    try: h=exact(2)
    except EOFError: break
    size=h[1]&127
    if size==126: size=struct.unpack("!H",exact(2))[0]
    if size==127: size=struct.unpack("!Q",exact(8))[0]
    mask=exact(4)
    payload=bytes(v^mask[i%4] for i,v in enumerate(exact(size)))
    if h[0]&15==8: break
    if h[0]&15!=1: continue
    m=json.loads(payload)
    if "id" in m:
        send({"jsonrpc":"2.0","id":m["id"],"result":{"ok":True,"echo":m["method"]}})
        if m["method"]=="initialize":
            send({"jsonrpc":"2.0","method":"thread/status/changed","params":{"threadId":"test-thread"}})
'''


def test_real_disposable_ws_appserver_keeps_existing_rpc_machine(tmp_path):
    script = tmp_path / "fake-ws.py"
    script.write_text(_WS_SERVER)
    # Short directory: macOS caps AF_UNIX paths at 104 bytes.
    sock_dir = Path(tempfile.mkdtemp(prefix="ccxws.", dir="/tmp"))
    path = sock_dir / "app.sock"
    events = []
    client = AppServerClient(
        [sys.executable, str(script), f"unix://{path}"],
        cwd=str(tmp_path), socket_path=str(path),
        on_notification=lambda method, params: events.append((method, params)),
        on_server_request=lambda *_: None,
        stderr=subprocess.DEVNULL,
    )
    try:
        assert client.request("initialize")["echo"] == "initialize"
        assert client.request("thread/read", {"threadId": "t"})["echo"] == "thread/read"
        assert client.process.poll() is None
        assert client._ws is not None
        until = time.monotonic() + 2
        while not events and time.monotonic() < until:
            time.sleep(.01)
        assert events[0][0] == "thread/status/changed"
        assert os.stat(path).st_mode & 0o077 == 0
    finally:
        child = client.process
        client.close()
        shutil.rmtree(sock_dir, ignore_errors=True)
        assert child.poll() is not None


def _wire_client():
    reader, writer = socket.socketpair()
    reader.setblocking(False)
    client = AppServerClient.__new__(AppServerClient)
    client._ws = SimpleNamespace(sock=reader)
    client._socket_shutdown = False
    client.process = SimpleNamespace(stdin=None)
    client._lock = threading.Lock()
    client._state_lock = threading.Lock()
    client._closed = threading.Event()
    client._failure = None
    client._pending = {}
    client._next_id = 1
    return client, writer


def _masked_payload(sock):
    sock.settimeout(1)
    first, second = sock.recv(2)
    assert first & 0x0f == 1
    assert second & 0x80
    n = second & 0x7f
    if n == 126:
        n = struct.unpack("!H", sock.recv(2))[0]
    if n == 127:
        n = struct.unpack("!Q", sock.recv(8))[0]
    mask = sock.recv(4)
    data = b""
    while len(data) < n:
        data += sock.recv(n - len(data))
    return bytes(value ^ mask[i % 4] for i, value in enumerate(data))


def test_socket_frame_is_masked_json_and_retains_request_id():
    client, peer = _wire_client()
    try:
        client._send("turn/start", {"threadId": "owned"}, queue.Queue())
        assert json.loads(_masked_payload(peer)) == {
            "jsonrpc": "2.0", "id": 1, "method": "turn/start",
            "params": {"threadId": "owned"}}
        assert list(client._pending) == [1]
    finally:
        peer.close()
        client._ws.sock.close()


@pytest.mark.parametrize("partial", [False, True])
def test_socket_write_deadline_distinguishes_zero_bytes_from_partial(monkeypatch, partial):
    client, peer = _wire_client()
    real_write = os.write
    calls = [0]

    def injected(fd, data):
        if fd != client._ws.sock.fileno():
            return real_write(fd, data)
        calls[0] += 1
        if partial and calls[0] == 1:
            return 1
        raise BlockingIOError(errno.EAGAIN, "blocked")

    monkeypatch.setattr(codex_protocol.os, "write", injected)
    try:
        if partial:
            with pytest.raises(ProtocolTransportError):
                client._send("turn/start", {}, queue.Queue(),
                             write_deadline=time.monotonic() + .04)
            assert client.failure is not None
        else:
            with pytest.raises(ProtocolNotSent):
                client._send("turn/start", {}, queue.Queue(),
                             write_deadline=time.monotonic() + .04)
            assert client.failure is None and client._pending == {}
    finally:
        peer.close()
        client._ws.sock.close()


def test_ws_disconnect_latches_failure_and_wakes_pending_request():
    client, peer = _wire_client()
    pending = queue.Queue()
    client._pending[5] = pending
    # The actual WS decoder sees EOF; the reader must fail the whole connection.
    from ccx.codex.control import _UnixWebSocket
    ws = _UnixWebSocket.__new__(_UnixWebSocket)
    ws.sock = client._ws.sock
    ws._buffer = b""
    client._ws = ws
    peer.close()
    client._read_loop()
    assert isinstance(pending.get_nowait(), ProtocolTransportError)
    assert client.failure is not None
    ws.sock.close()


def test_socket_shutdown_signals_only_owned_process_group(monkeypatch):
    client, peer = _wire_client()
    killed = []
    child = SimpleNamespace(pid=89765, stdin=None, poll=lambda: None,
                            wait=lambda timeout: 0)
    client.process = child
    monkeypatch.setattr(codex_protocol.os, "killpg",
                        lambda pid, sig: killed.append((pid, sig)))
    try:
        client.close()
        assert killed == [(child.pid, signal.SIGTERM), (child.pid, signal.SIGKILL)]
    finally:
        peer.close()


def _attach_daemon(state="idle"):
    daemon = codexd.CodexDaemon()
    owner = SimpleNamespace(engine="codex", worker_id=B, worker=f"codex:{B}")
    daemon._uuid_store = SimpleNamespace(resolve=lambda target: owner)
    worker = SimpleNamespace(
        loaded=True, teardown_claimed=False, lock=threading.RLock(),
        attach_count=0, last_activity=time.monotonic() - 120,
        record={"lifecycle": {"state": state},
                "native": {"threadId": "native-thread"}, "cwd": "/tmp"},
    )
    daemon.workers[B] = worker
    daemon.app_server_path = Path("/tmp/app.sock")
    return daemon, worker


@pytest.mark.parametrize("state", ["idle", "busy", "waiting"])
def test_attach_loads_original_then_holds_subscribed_worker(monkeypatch, state):
    daemon, worker = _attach_daemon(state)
    calls = []
    def load(worker_id, timeout):
        calls.append((worker_id, timeout))
        return {"ok": True, "loaded": True, "state": state}
    monkeypatch.setattr(daemon, "_load_original_native_worker", load)
    result, held = daemon._open_attach({"short": B})
    assert calls == [(B, 90)]
    assert held is worker and worker.attach_count == 1
    assert result["threadId"] == "native-thread"
    assert result["socket"].endswith("/app.sock")


def test_attached_idle_worker_is_not_janitor_unloaded_but_explicit_kill_can_win(monkeypatch):
    daemon, worker = _attach_daemon()
    worker.short = B
    worker.record["createArgs"] = {"idleTimeout": 1}
    worker.load_lock = threading.Lock()
    worker.operation_lock = threading.Lock()
    worker.kill_intent = threading.Event()
    worker.killed = threading.Event()
    worker.attach_count = 1
    idle_kills = []
    monkeypatch.setattr(daemon, "_idle_kill", lambda target: idle_kills.append(target))
    daemon._janitor_worker(worker)
    assert idle_kills == []
    worker.kill = lambda seconds: {"ok": True, "killed": True}
    assert daemon._kill_worker(worker)["ok"] is True
    worker.attach_count = 0
    worker.kill_intent.clear()
    daemon._janitor_worker(worker)
    assert idle_kills == [worker]


def test_idle_unload_claim_rechecks_new_attach_hold_but_explicit_kill_wins(monkeypatch):
    daemon, worker = _attach_daemon()
    worker.short = B
    worker.native_threads = set()
    worker.record["lifecycle"]["since"] = 0
    worker.record["pendingApprovals"] = []
    worker._journal_lifecycle_locked = lambda **kw: None
    worker._save = lambda: None
    worker.evidence = SimpleNamespace(observed=False, close=lambda: None)
    monkeypatch.setattr(daemon, "_release_native_threads", lambda *_: None)

    # Reproduce the janitor having already seen expiration: attach wins the
    # daemon lock just before _idle_kill attempts its teardown claim.
    with daemon.lock, worker.lock:
        worker.attach_count = 1
    daemon._idle_kill(worker)
    assert not worker.teardown_claimed
    assert daemon.workers[B] is worker
    assert worker.record["lifecycle"]["state"] == "idle"

    # Explicit kill is not an idle-unload operation and may still win.
    daemon._unload(worker, terminal_state="dead")
    assert worker.teardown_claimed
    assert B not in daemon.workers
    assert worker.record["lifecycle"]["state"] == "dead"


def test_attach_dispatch_errors_are_reported_with_exit_code(monkeypatch, capsys):
    from ccx.store import store as generation
    from ccx.store.identity import EngineMismatch
    from ccx.store.root import StaleGeneration

    errors = [EngineMismatch("target is owned by Codex, not Claude"),
              StaleGeneration("CCX generation changed")]
    for error in errors:
        def resolve(_target, *, failure=error):
            raise failure
        monkeypatch.setattr(store_root, "require_ccx_active",
                            lambda _root: SimpleNamespace(resolve=resolve))
        with pytest.raises(SystemExit) as outcome:
            commands_control.cmd_attach(SimpleNamespace(target=f"claude:{B}", no_status=False))
        assert outcome.value.code == constants.EXIT_UNAVAILABLE
        assert capsys.readouterr().err.startswith(f"[ccx] attach: {error}")


def test_attach_connection_eof_releases_hold_and_resets_idle_clock(monkeypatch):
    daemon, worker = _attach_daemon()
    worker.attach_count = 1
    daemon._uuid_store.revalidate = lambda: None
    daemon._uuid_plan = SimpleNamespace(
        evidence=lambda: {"role": "codexd", "generation": "test", "rootRealpath": "/tmp",
                          "rootHash": "test", "workerId": None, "engine": None},
        role="codexd")
    daemon._uuid_daemon_lease = SimpleNamespace(nonce="0" * 32)
    monkeypatch.setattr(daemon, "_open_attach",
                        lambda request: ({"ok": True, "threadId": "native-thread"}, worker))
    server, caller = socket.socketpair()
    started = threading.Thread(target=daemon._serve_connection, args=(server,))
    started.start()
    def line():
        data = b""
        while not data.endswith(b"\n"):
            data += caller.recv(4096)
        return json.loads(data)
    try:
        caller.sendall((json.dumps({"op": "handshake",
            "identity": {"protocolVersion": 2, **daemon._uuid_plan.evidence()}}) + "\n").encode())
        assert line()["ok"]
        caller.sendall((json.dumps({"op": "attach", "short": B,
                                   "rpcGeneration": constants.CODEXD_RPC_GENERATION}) + "\n").encode())
        assert line()["ok"]
        before = worker.last_activity
    finally:
        caller.close()
        started.join(2)
    assert not started.is_alive()
    assert worker.attach_count == 0 and worker.last_activity >= before


def test_cmd_attach_runs_native_binary_and_propagates_exit(monkeypatch, tmp_path):
    calls = []
    @contextmanager
    def attached(target):
        calls.append(("target", target))
        yield {"threadId": "native-thread", "socket": "/tmp/app.sock",
               "cwd": str(tmp_path), "workerId": B}
    monkeypatch.setattr(codex_commands.runtime, "attached_worker", attached)
    monkeypatch.setattr(codex_commands.sys, "stdin", SimpleNamespace(isatty=lambda: True))
    monkeypatch.setattr(codex_commands.sys, "stdout", SimpleNamespace(isatty=lambda: True))
    monkeypatch.setenv("CCX_CODEX_BIN", sys.executable)
    from ccx.codex import attach_tty as codex_attach_tty
    monkeypatch.setattr(codex_attach_tty, "run_status_tui",
                        lambda argv, **kwargs: calls.append((argv, kwargs)) or 7)
    with pytest.raises(SystemExit) as exc:
        codex_commands.cmd_attach(SimpleNamespace(target=f"codex:{B}", no_status=False))
    assert exc.value.code == 7
    assert calls[1][0] == [sys.executable, "resume", "native-thread",
                           "-c", "check_for_update_on_startup=false",
                           "--remote", "unix:///tmp/app.sock"]
    assert calls[1][1] == {"cwd": str(tmp_path), "worker_id": B, "status": True}


@pytest.mark.parametrize("has_turn", [False, True])
def test_failed_attach_hints_at_a_first_turn_only_without_one(monkeypatch, tmp_path, capsys, has_turn):
    @contextmanager
    def attached(target):
        yield {"threadId": "native-thread", "socket": "/tmp/app.sock",
               "cwd": str(tmp_path), "workerId": B, "hasTurn": has_turn}
    monkeypatch.setattr(codex_commands.runtime, "attached_worker", attached)
    monkeypatch.setattr(codex_commands.sys, "stdin", SimpleNamespace(isatty=lambda: True))
    monkeypatch.setattr(codex_commands.sys, "stdout", SimpleNamespace(isatty=lambda: True))
    monkeypatch.setenv("CCX_CODEX_BIN", sys.executable)
    from ccx.codex import attach_tty as codex_attach_tty
    monkeypatch.setattr(codex_attach_tty, "run_status_tui", lambda argv, **kwargs: 1)
    with pytest.raises(SystemExit) as exc:
        codex_commands.cmd_attach(SimpleNamespace(target=f"codex:{B}", no_status=False))
    assert exc.value.code == 1
    assert ("no turn yet" in capsys.readouterr().err) is not has_turn


def test_cmd_attach_passes_worker_model_and_effort(monkeypatch, tmp_path):
    calls = []
    @contextmanager
    def attached(target):
        yield {"threadId": "native-thread", "socket": "/tmp/app.sock",
               "cwd": str(tmp_path), "workerId": B,
               "model": "gpt-6-astra", "effort": "high"}
    monkeypatch.setattr(codex_commands.runtime, "attached_worker", attached)
    monkeypatch.setattr(codex_commands.sys, "stdin", SimpleNamespace(isatty=lambda: True))
    monkeypatch.setattr(codex_commands.sys, "stdout", SimpleNamespace(isatty=lambda: True))
    monkeypatch.setenv("CCX_CODEX_BIN", sys.executable)
    from ccx.codex import attach_tty as codex_attach_tty
    monkeypatch.setattr(codex_attach_tty, "run_status_tui",
                        lambda argv, **kwargs: calls.append(argv) or 0)
    codex_commands.cmd_attach(SimpleNamespace(target=f"codex:{B}", no_status=False))
    assert calls[0] == [sys.executable, "resume", "native-thread",
                        "-c", "check_for_update_on_startup=false",
                        "-m", "gpt-6-astra", "-c", 'model_reasoning_effort="high"',
                        "--remote", "unix:///tmp/app.sock"]


def test_attach_engine_dispatch_preserves_claude_handler():
    callback = lambda args: None
    store = SimpleNamespace(resolve=lambda x: SimpleNamespace(engine="codex" if x.startswith("codex:") else "claude"))
    for engine in ("claude", "codex"):
        args = SimpleNamespace(cmd="attach", target=f"{engine}:{B}", func=callback)
        assert cli._native_handler(args, store) is callback


def test_tui_first_approval_resolution_makes_ccx_gate_stale(short_root, tmp_path, monkeypatch):
    # Existing Codex event reducer and approve handler; no new broker.
    from test_codex_native_effects import _f1_observation_worker, _f1_turn_notification
    store, daemon, worker = _f1_observation_worker(short_root, tmp_path, monkeypatch)
    _f1_turn_notification(daemon, "turn/started", "native-turn")
    worker.on_server_request(15, "item/commandExecution/requestApproval",
                             {"threadId": "native-f1-thread", "turnId": "native-turn",
                              "itemId": "cmd-1"})
    saved = store.record(B)["pendingApprovals"]
    assert len(saved) == 1
    daemon._on_notification("serverRequest/resolved",
                            {"threadId": "native-f1-thread", "requestId": 15})
    assert store.record(B)["pendingApprovals"] == []
    result = worker.handle_operation({"op": "approve", "gateId": saved[0]["gateId"]})
    assert result["ok"] is False and result["definitive"] is True
    assert "no current original Codex approval" in result["error"]


def test_sigkill_of_socket_owner_terminates_owned_appserver_group(
    tmp_path, socket_tmp_path,
):
    """An abrupt codexd-style parent death must not orphan its app-server.

    This uses a throwaway fake WebSocket provider and a separate disposable
    owner Python process, never the real Codex or an existing daemon. No
    clean AppServerClient.close() runs in the killed owner.
    """
    fake = tmp_path / "fake-ws-server.py"
    fake.write_text(_WS_SERVER)
    sock = socket_tmp_path / "app.sock"
    pids_path = socket_tmp_path / "owned-pids.json"
    owner_code = '\nimport json, os, subprocess, sys, time\nfrom pathlib import Path\nfrom ccx.codex.protocol import AppServerClient\nserver, endpoint, marker = sys.argv[1:]\nclient = AppServerClient(\n    [sys.executable, server, f"unix://{endpoint}"],\n    socket_path=endpoint, cwd=str(Path(endpoint).parent),\n    on_notification=lambda *args: None,\n    on_server_request=lambda *args: None,\n    stderr=subprocess.DEVNULL,\n)\nPath(marker).write_text(json.dumps({\n    "server": client.process.pid,\n    "watcher": client._parent_watcher.pid,\n    "pgid": client._owned_pgid,\n    "owner": os.getpid(),\n}))\ntime.sleep(60)\n'
    owner = subprocess.Popen(
        [sys.executable, "-c", owner_code, str(fake), str(sock), str(pids_path)],
        cwd=str(Path(__file__).resolve().parent.parent),
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL, start_new_session=True,
    )
    pids = None

    def exited(pid: int) -> bool:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        # A child reparented to PID 1 might briefly be a zombie on Linux.
        stat = subprocess.run(
            ["ps", "-o", "stat=", "-p", str(pid)], capture_output=True,
            text=True, timeout=2,
        )
        return stat.returncode != 0 or not stat.stdout.strip() or stat.stdout.lstrip().startswith("Z")

    try:
        until = time.monotonic() + 10
        while not pids_path.exists() and time.monotonic() < until:
            assert owner.poll() is None, "socket owner failed during startup"
            time.sleep(.05)
        assert pids_path.exists(), "fake server never became ready"
        pids = json.loads(pids_path.read_text())
        assert pids["owner"] == owner.pid
        assert pids["pgid"] == pids["watcher"] != pids["server"]
        assert os.getpgid(pids["server"]) == pids["pgid"]
        assert not exited(pids["server"])

        os.kill(owner.pid, signal.SIGKILL)  # exact owner process launched above
        owner.wait(timeout=3)
        until = time.monotonic() + 7
        while time.monotonic() < until and not all(
            exited(pids[name]) for name in ("server", "watcher")
        ):
            time.sleep(.1)
        assert exited(pids["server"]), "orphan app-server survived owner SIGKILL"
        assert exited(pids["watcher"]), "group watcher did not reap its own lifetime"
    finally:
        if owner.poll() is None:
            owner.kill()
            owner.wait(timeout=3)
        # Defensive test cleanup is restricted to the group this test spawned.
        if pids is not None and not exited(pids["watcher"]):
            try:
                os.killpg(pids["pgid"], signal.SIGKILL)
            except ProcessLookupError:
                pass
