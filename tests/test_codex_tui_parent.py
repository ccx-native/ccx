"""External Codex caller tests use disposable Unix WebSocket app-servers only."""
from __future__ import annotations
import ccx.notify.daemon as notify_daemon

import base64
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import socket
import struct
from threading import Event, Thread
from types import SimpleNamespace

import pytest

from ccx.cli import main as cli
from ccx.codex import control as codex_control
from ccx.cli import controller as controller_commands
from ccx.notify import daemon as notifyd
from ccx.controllers import orchestrator
from ccx.controllers import policy as orchestrator_policy
from ccx.notify import registry as parent_notify
from ccx.errors import InvalidRequest
from ccx.notify.client import ParentNotificationClient
from test_generation_store import A, B, C, short_root  # noqa: F401 - pytest fixture
from test_notify_ext import _frozen_child
from test_notify_registration import originals  # noqa: F401 - disposable actual worker records


THREAD = "plain-codex-tui-return-thread"


def _native_environment(monkeypatch, socket_tmp_path):
    for key in ("CCX_SELF_SHORT", "CCX_SELF_WORKER_ID", "CCX_SELF_ENGINE",
                "CLAUDE_CODE_SESSION_ID"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("CODEX_THREAD_ID", THREAD)
    monkeypatch.setenv("CODEX_HOME", str(socket_tmp_path / "codex-home"))


def _exact(sock, n):
    out = b""
    while len(out) < n:
        part = sock.recv(n - len(out))
        if not part:
            raise EOFError("fake socket closed")
        out += part
    return out


def _frame(sock):
    first, second = _exact(sock, 2)
    size = second & 127
    if size == 126:
        size = struct.unpack("!H", _exact(sock, 2))[0]
    elif size == 127:
        size = struct.unpack("!Q", _exact(sock, 8))[0]
    mask = _exact(sock, 4) if second & 128 else b""
    data = _exact(sock, size)
    if mask:
        data = bytes(value ^ mask[i % 4] for i, value in enumerate(data))
    return first & 15, data


@contextmanager
def fake_control_socket(socket_tmp_path, monkeypatch, *, loaded):
    """An in-process fake of the real control-socket handshake + loaded/list."""
    home = socket_tmp_path / "codex-home"
    endpoint = home / "app-server-control" / "app-server-control.sock"
    endpoint.parent.mkdir(parents=True)
    monkeypatch.setenv("CODEX_HOME", str(home))
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(str(endpoint))
    listener.listen(4)
    listener.settimeout(.1)
    stop = Event()
    called = []
    errors = []

    def serve():
        while not stop.is_set():
            try:
                connection, _ = listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            try:
                with connection:
                    connection.settimeout(2)
                    header = b""
                    while b"\r\n\r\n" not in header:
                        header += connection.recv(4096)
                    key = next(line.split(":", 1)[1].strip() for line in
                               header.decode("ascii").split("\r\n") if line.startswith("Sec-WebSocket-Key:"))
                    accept = base64.b64encode(hashlib.sha1(
                        (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()).decode()
                    connection.sendall(("HTTP/1.1 101 Switching Protocols\r\n"
                                        "Upgrade: websocket\r\nConnection: Upgrade\r\n"
                                        f"Sec-WebSocket-Accept: {accept}\r\n\r\n").encode())
                    while True:
                        opcode, payload = _frame(connection)
                        if opcode == 8:
                            break
                        msg = json.loads(payload)
                        called.append(msg.get("method"))
                        if "id" not in msg:
                            continue
                        method = msg["method"]
                        if method == "initialize":
                            result = {}
                        elif method == "thread/loaded/list":
                            result = {"data": list(loaded)}
                        else:
                            raise AssertionError(f"unexpected native RPC: {method}")
                        value = json.dumps({"id": msg["id"], "result": result}).encode()
                        head = (bytes((0x81, len(value))) if len(value) < 126
                                else bytes((0x81, 126)) + struct.pack("!H", len(value)))
                        connection.sendall(head + value)
            except (EOFError, ConnectionError, OSError):
                pass
            except BaseException as error:
                errors.append(error)

    task = Thread(target=serve, daemon=True)
    task.start()
    try:
        yield called
    finally:
        stop.set()
        listener.close()
        task.join(timeout=3)
        assert not errors, errors


@pytest.mark.parametrize("argv", [
    ["wait", f"codex:{B}"],
    ["send", f"codex:{B}", "hello"],
    ["new", "--engine", "codex", "hello", "--wait"],
    ["fork", f"codex:{B}", "hello", "--wait"],
    ["compact", f"codex:{B}"],
])
def test_plain_tui_without_socket_allows_blocking(argv, originals, monkeypatch, tmp_path, socket_tmp_path):
    _native_environment(monkeypatch, socket_tmp_path)
    assert not codex_control.control_socket_path().exists()
    caller = orchestrator.current_caller()
    assert caller.kind == "codex" and not caller.supports_parent_notify
    orchestrator_policy.reject_codex_blocking_call(argv)
    assert controller_commands._prepare_completion(SimpleNamespace(
        detach=False, notify_parent=False)) is None


@pytest.mark.parametrize("argv", [
    ["new", "--engine", "codex", "--worker-id", C, "--notify-parent", "prompt"],
    ["send", f"codex:{B}", "hello", "--delivery-id", "tui-guard", "--notify-parent"],
    ["fork", f"codex:{B}", "--notify-parent"],
    ["compact", f"codex:{B}", "--notify-parent"],
    ["notify", f"codex:{B}"],
    ["controller", "run", "--notify-parent", "flow.py"],
])
def test_plain_tui_refuses_notifications_without_arm(argv, originals, monkeypatch, tmp_path, socket_tmp_path, capsys):
    _native_environment(monkeypatch, socket_tmp_path)
    store, trace, _ = originals
    before = trace.read_text()
    monkeypatch.setattr(notify_daemon, "ensure_notify_daemon",
                        lambda **_kw: pytest.fail("must not start notifyd"))
    if argv[0] == "controller":
        script = tmp_path / "flow.py"
        script.write_text("# must not run\n")
        argv = [*argv[:-1], str(script)]
    with pytest.raises(SystemExit) as stopped:
        cli._main(argv)
    assert stopped.value.code == 1
    error = capsys.readouterr().err
    assert "no Codex app-server control socket" in error
    assert ("ccx controller wait TARGET" if argv[0] == "controller"
            else "--wait / ccx wait TARGET") in error
    assert ParentNotificationClient().list()["count"] == 0
    assert trace.read_text() == before
    assert store.list_workers(engine="codex", limit=None) and all(
        item["worker"] != f"codex:{C}" for item in store.list_workers(engine="codex", limit=None))


def test_fake_control_server_owns_thread_and_codex_wait_is_refused(originals, monkeypatch, tmp_path, socket_tmp_path):
    _native_environment(monkeypatch, socket_tmp_path)
    with fake_control_socket(socket_tmp_path, monkeypatch, loaded=[THREAD]) as called:
        caller = orchestrator.current_caller()
        assert caller.supports_parent_notify
        assert orchestrator.require_notify_parent() == {
            "kind": "codex-native-thread", "threadId": THREAD}
        with pytest.raises(SystemExit) as refused:
            orchestrator_policy.reject_codex_blocking_call(["wait", f"codex:{B}"])
        assert refused.value.code == 1
    assert called.count("thread/loaded/list") >= 2
    assert "thread/read" not in called and "thread/resume" not in called


def test_daemon_socket_cannot_claim_separately_running_tui_thread(originals, monkeypatch, tmp_path, socket_tmp_path):
    _native_environment(monkeypatch, socket_tmp_path)
    with fake_control_socket(socket_tmp_path, monkeypatch, loaded=["daemon-owned-other-thread"]) as called:
        assert not orchestrator.current_caller().supports_parent_notify
        with pytest.raises(InvalidRequest, match="not loaded in the app-server") as refused:
            orchestrator.require_notify_parent()
        assert "ccx wait TARGET" in str(refused.value)
        orchestrator_policy.reject_codex_blocking_call(["wait", f"codex:{B}"])
    assert called and set(called) <= {"initialize", "initialized", "thread/loaded/list"}
    assert ParentNotificationClient().list()["count"] == 0


def test_managed_parent_never_requires_external_control_socket(originals, monkeypatch, tmp_path, socket_tmp_path):
    _native_environment(monkeypatch, socket_tmp_path)
    store, _, _ = originals
    monkeypatch.setenv("CCX_SELF_ENGINE", "codex")
    monkeypatch.setenv("CCX_SELF_WORKER_ID", A)
    monkeypatch.setenv("CODEX_THREAD_ID", store.record(A)["native"]["threadId"])
    monkeypatch.setattr(orchestrator, "_external_codex_unavailable",
                        lambda _thread: pytest.fail("managed parent must not probe a socket"))
    caller = orchestrator.current_caller()
    assert caller.managed and caller.supports_parent_notify
    assert orchestrator.require_notify_parent()["kind"] == "ccx-codex"
    with pytest.raises(SystemExit) as refused:
        orchestrator_policy.reject_codex_blocking_call(["wait", f"codex:{B}"])
    assert refused.value.code == 1


def test_historical_external_event_without_socket_eventually_terminal(originals, monkeypatch, tmp_path, socket_tmp_path):
    _native_environment(monkeypatch, socket_tmp_path)
    store, trace, _ = originals
    parent = {"kind": "codex-native-thread", "threadId": THREAD}
    reg = _frozen_child(store, token="tui-historical-undeliverable", parent=parent)
    before = trace.read_text()
    # A short socket gap (daemon restart) keeps retrying; only a gap that
    # outlasts the expiry window since readiness terminalizes the event.
    for attempts, age_ms in ((1, 0), (2, 0), (3, 0), (4, notifyd.EXTERNAL_PARENT_EXPIRY_MS)):
        saved = parent_notify.read_registration(reg["eventId"])
        if attempts > 1:
            parent_notify.mutate_registration(reg["eventId"],
                lambda value: value.update(nextAttemptAtMs=0))
            saved = parent_notify.read_registration(reg["eventId"])
        if age_ms:
            monkeypatch.setattr(notifyd, "now_ms", lambda: int(saved["readyAtMs"]) + age_ms)
        notifyd.process_registration(saved)
        saved = parent_notify.read_registration(reg["eventId"])
        assert saved["attempts"] == attempts
        assert saved["state"] == ("undeliverable" if age_ms else "ready")
        assert "external Codex parent control socket absent" in saved["lastError"]
    assert saved["nextAttemptAtMs"] == 0
    assert ParentNotificationClient().list()["count"] == 0
    assert trace.read_text() == before


def test_inaccessible_control_socket_is_reported_not_raised(monkeypatch):
    # A sandbox can deny even stat() on ~/.codex; that is "unavailable", not a traceback.
    class Denied:
        def is_socket(self):
            raise PermissionError(1, "Operation not permitted")

        def __str__(self):
            return "/denied/app-server-control.sock"

    monkeypatch.setattr(codex_control, "control_socket_path", lambda: Denied())
    reason = orchestrator._external_codex_unavailable("019a0000-0000-7000-8000-000000000000")
    assert reason.startswith("Codex app-server control socket at /denied/app-server-control.sock is not accessible")


def test_empty_codex_home_means_default(monkeypatch, tmp_path):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("CODEX_HOME", "")
    assert codex_control.control_socket_path() == (
        tmp_path / ".codex" / "app-server-control" / "app-server-control.sock")
