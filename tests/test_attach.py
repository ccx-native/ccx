"""Shared PTY attachment: bounded protocol, one native host, terminal hygiene.

All integration workers here are disposable fake-Claude processes under
pytest's short private root. Nothing uses a real provider or operator workers.
"""
from __future__ import annotations
import ccx.store.root as store_root
import ccx.claude.host.pty as H_pty
import ccx.ops.cancel as ops_cancel

import base64
import fcntl
import json
import os
import pty
import select
import signal
import socket
import struct
import subprocess
import sys
import termios
import threading
import time
from pathlib import Path

import pytest

from ccx import CcxClient
from ccx.cli.commands import _common as C
from ccx import constants
from ccx.rpc import rpc
from ccx.claude import attach as A
from ccx.store.root import require_ccx_active
from ccx.store.liveness import owned_pid_alive
from ccx.store.paths import events_ndjson_path
from test_create_cross_surface import _deadline_fake_claude, _first_wait_original_claude_ready
from test_claude_native_cli import _stop_disposable_original
from test_generation_store import A as WORKER_ID, short_root  # fixture re-export


def _size(fd):
    return struct.unpack("HHHH", fcntl.ioctl(fd, termios.TIOCGWINSZ, b"\0" * 8))


def _nonblocking_flag(fd):
    """Only O_NONBLOCK is mutable/owned; Darwin adds other kernel state bits."""
    return bool(fcntl.fcntl(fd, fcntl.F_GETFL) & os.O_NONBLOCK)


def _termios_without_pendin(attrs):
    """PENDIN is a macOS kernel-set reinput bit, not a saved tty mode."""
    values = list(attrs)
    values[3] &= ~getattr(termios, "PENDIN", 0x20000000)
    return values


def _wait(predicate, timeout=7):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        val = predicate()
        if val:
            return val
        time.sleep(.04)
    raise AssertionError("condition did not occur within test deadline")


@pytest.fixture
def hub():
    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 30, 97, 3, 4))
    events = []
    disconnected = []
    release = []
    sizes = []

    def resize(rows, cols):
        sizes.append((rows, cols))
        fcntl.ioctl(master, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))

    h = A.HostAttachment(master, resize, events.append, disconnected.append, lambda: release.append(True))
    try:
        yield h, slave, events, disconnected, release, sizes
    finally:
        os.close(master)
        os.close(slave)


class FakeConnection:
    def __init__(self):
        self.sent = []
    def send(self, data):
        self.sent.append(data)
        return len(data)


def _open(hub, conn=None, rows=25, cols=90):
    if conn is None:
        conn = FakeConnection()
    reply = hub.open(conn, {"version": 1, "rows": rows, "cols": cols}, epoch="test")
    assert reply["ok"], reply
    return conn, reply["token"]


def _input(token, raw):
    return {"token": token, "data": base64.b64encode(raw).decode()}


def test_protocol_and_atomic_admission(hub):
    h, slave, events, disconnected, release, sizes = hub
    assert "version" in h.open(object(), {"version": 77, "rows": 20, "cols": 70}, epoch="e")["error"]
    assert not h.open(object(), {"version": 1, "rows": 0, "cols": 70}, epoch="e")["ok"]
    c, token = _open(h)
    assert h.attached
    assert sizes == [(25, 89)]  # native redraw nudge
    assert "already attached" in h.open(c, {"version": 1, "rows": 30, "cols": 80}, epoch="e")["error"]
    assert _size(slave)[:2] == (25, 89)
    h.repaint_at = time.monotonic() - 1
    h.tick(lambda raw: True)
    assert sizes[-1] == (25, 90)
    h.accept_resize(c, {"token": token, "rows": 31, "cols": 101})
    assert _size(slave)[:2] == (31, 101)
    with pytest.raises(ValueError, match="token"):
        h.accept_resize(object(), {"token": token, "rows": 22, "cols": 80})
    h.release(c, "detached")
    h.release(c, "duplicate")
    assert release == [True]
    assert _size(slave) == (30, 97, 3, 4)  # restore complete original ioctl payload once
    assert [e["type"] for e in events] == ["attach_opened", "attach_closed"]
    assert not disconnected


def test_several_terminals_share_output_input_and_the_smallest_size(hub):
    h, slave, events, disconnected, release, sizes = hub
    a, a_token = _open(h, rows=25, cols=90)
    b, b_token = _open(h, rows=40, cols=120)
    assert sizes[-1] == (25, 89)  # the newcomer's redraw nudge, at the shared size
    h.repaint_at = time.monotonic() - 1
    h.tick(lambda raw: True)
    assert _size(slave)[:2] == (25, 90)
    c, c_token = _open(h, rows=20, cols=100)
    h.repaint_at = time.monotonic() - 1
    h.tick(lambda raw: True)
    assert _size(slave)[:2] == (20, 90)  # smallest rows and columns

    # Every terminal sees the same output; each one may type.
    h.feed_output(b"shared")
    for conn in (a, b, c):
        h.flush(conn)
        assert [base64.b64decode(json.loads(x)["data"]) for x in conn.sent] == [b"shared"]
    h.accept_input(b, _input(b_token, b"from b"))
    h.accept_input(a, _input(a_token, b"from a"))
    writes = []
    h.tick(lambda part: writes.append(part) or True)
    assert sorted(writes) == [b"from a", b"from b"]
    with pytest.raises(ValueError, match="token"):
        h.accept_input(a, _input(b_token, b"wrong lease"))

    h.accept_resize(b, {"token": b_token, "rows": 18, "cols": 200})
    assert _size(slave)[:2] == (18, 90)
    h.release(a, "detached")  # the others may grow, the original stays saved
    assert _size(slave)[:2] == (18, 100)
    h.release(b, "detached")
    assert _size(slave)[:2] == (20, 100)
    h.release(c, "detached")
    assert _size(slave) == (30, 97, 3, 4)
    assert len(release) == 3 and not disconnected
    assert [e["type"] for e in events] == ["attach_opened"] * 3 + ["attach_closed"] * 3


def test_attached_terminals_are_capped_and_one_overflow_drops_only_it(hub):
    h, _slave, events, disconnected, _release, _sizes = hub
    conns = [_open(h)[0] for _ in range(A.MAX_VIEWERS)]
    assert "too many" in h.open(FakeConnection(), {"version": 1, "rows": 20, "cols": 70},
                                epoch="e")["error"]
    slow, fast = conns[0], conns[1]
    for _ in range(2 * A.MAX_PENDING_OUTPUT // 3000):
        if not h.has(slow):
            break
        h.feed_output(b"y" * 3000)
        for conn in conns[1:]:
            if h.has(conn):
                h.flush(conn)
    assert not h.has(slow) and disconnected == [slow]
    assert h.has(fast) and len(h.viewers) == A.MAX_VIEWERS - 1
    assert [e.get("reason") for e in events if e["type"] == "attach_closed"] == ["output-overflow"]


def test_binary_input_frames_and_bounded_queue(hub):
    h, _slave, _events, disconnected, _, _ = hub
    c, token = _open(h)
    raw = b"\x00\xffunicode:\xe2\x98\x83\x1b[200~\x03"
    h.accept_input(c, _input(token, raw))
    writes = []
    h.tick(lambda part: writes.append(part) or True)
    assert b"".join(writes) == raw
    assert A.decode_input(base64.b64encode(b"x" * A.MAX_INPUT_FRAME).decode()) == b"x" * A.MAX_INPUT_FRAME
    with pytest.raises(ValueError, match="large"):
        A.decode_input(base64.b64encode(b"x" * (A.MAX_INPUT_FRAME + 1)).decode())
    with pytest.raises(ValueError, match="base64"):
        A.decode_input("what!?not-base64")
    with pytest.raises(ValueError, match="token"):
        h.accept_input(object(), _input(token, b"tamper"))
    # 32 KiB is the maximum *frame*, not the pending input budget.
    h.accept_input(c, _input(token, b"a" * 32768))
    h.accept_input(c, _input(token, b"b" * 32768))
    assert len(h.viewers[c].input) == A.MAX_PENDING_INPUT
    h.accept_input(c, _input(token, b"c"))
    assert not h.attached and disconnected == [c]


def test_byte_exact_output_fanout_and_backpressure(hub):
    h, _, events, disconnected, _, _ = hub
    left, right = socket.socketpair()
    try:
        c, token = _open(h, left)
        # Two distinct paths, ordinary PTY poll and a write_all drain, feed
        # through the same read helper. No replay, dedup, or UTF-8 loss.
        pieces = [b"\x1b[0mhello", b"\x00\xfe\xff\r\n", b"\xe2\x98\x83"]
        for part in pieces:
            h.feed_output(part)
        while h.viewers[c].output:
            h.flush(c)
        right.settimeout(.3)
        payload = b""
        while payload.count(b"\n") < len(pieces):
            payload += right.recv(1024)
        decoded = [base64.b64decode(json.loads(line)["data"])
                   for line in payload.splitlines()]
        assert decoded == pieces
        h.feed_output(b"x" * 4096)
        for _ in range(250):
            if not h.attached:
                break
            h.feed_output(b"y" * 4096)
        assert not h.attached and disconnected == [left]
        assert any(e.get("reason") == "output-overflow" for e in events)
    finally:
        left.close()
        right.close()


def test_stream_fragmentation_size_and_detach_filter():
    assert A.frame("attach-resize", rows=20, cols=90).endswith(b"\n")
    keys = A.DetachFilter()
    assert keys.feed(b"one\x1d") == b"one\x1d"  # Ctrl-] is an ordinary key
    assert keys.feed(b"\x1b[2") == b""
    assert keys.feed(b"00~\x1a") == b"\x1b[200~\x1a"  # literal inside paste
    assert keys.feed(b"\x1b[20") == b""
    assert keys.feed(b"1~") == b"\x1b[201~"
    assert not keys.detached
    assert keys.feed(b"abc\x1axyz") == b"abc" and keys.detached
    plain = A.DetachFilter()
    assert plain.feed(b"\x1b") == b""
    assert plain.flush_escape() == b"\x1b"
    assert not plain.detached


def test_terminal_cleanup_on_host_eof_and_sigwinch(monkeypatch):
    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 21, 83, 0, 0))
    prior = termios.tcgetattr(slave)
    left, right = socket.socketpair()
    observed = []
    captured_tty = bytearray()
    stop_tty_reader = threading.Event()
    sent = []
    orig_send = left.send

    def send(part):
        sent.append(part)
        return orig_send(part)

    try:
        # A peer thread sends output, checks first resize and ends the stream.
        def server():
            blob = bytearray()
            while not any(b'attach-resize' in row for row in blob.splitlines()):
                chunk = right.recv(4096)
                if not chunk:
                    return
                blob.extend(chunk)
            observed.extend(json.loads(line) for line in blob.splitlines())
            right.sendall(b"".join(A.frame(
                "attach-output", data=base64.b64encode(f"display{i:02d}".encode()).decode())
                for i in range(40)))
            os.kill(os.getpid(), signal.SIGWINCH)
            right.close()

        def terminal_consumer():
            # Darwin PTYs can be too small to hold the whole final screen.
            # A real terminal is reading concurrently; without this, a test
            # that reads only AFTER run_terminal exits cannot prove output
            # preservation on small-kernel-buffer systems.
            while not stop_tty_reader.is_set():
                ready, _, _ = select.select([master], [], [], .05)
                if ready:
                    try:
                        captured_tty.extend(os.read(master, 4096))
                    except OSError:
                        return

        reader_thread = threading.Thread(target=terminal_consumer, daemon=True)
        reader_thread.start()
        thread = threading.Thread(target=server, daemon=True)
        thread.start()
        reason = A.run_terminal(left, "t" * 32, input_fd=slave, output_fd=slave)
        thread.join(timeout=2)
        _wait(lambda: b"display39" in captured_tty, 2)
        assert reason == "connection-ended"
        assert _termios_without_pendin(termios.tcgetattr(slave)) == _termios_without_pendin(prior)
        assert any(item["op"] == "attach-resize" for item in observed)
        assert b"display00" in captured_tty and b"display39" in captured_tty
    finally:
        stop_tty_reader.set()
        if 'reader_thread' in locals():
            reader_thread.join(timeout=1)
        left.close()
        right.close()
        os.close(master)
        os.close(slave)


@pytest.mark.parametrize("write_failure", ["epipe", "zero"])
def test_host_write_failure_preserves_last_socket_frames_and_restores_tty(write_failure):
    """A failed resize to a closed host cannot discard queued final paint.

    The peer has already sent its native output and CLOSED before the client's
    initial attach-resize is sent. AF_UNIX stays readable *and* writable then;
    the first attempted write raises EPIPE (or returns zero), but read/paint
    must still complete. No further input or resize may be submitted.
    """
    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 22, 89, 0, 0))
    original = termios.tcgetattr(slave)
    old_nonblock = _nonblocking_flag(slave)
    client, peer = socket.socketpair()
    texts = [f"final-frame-{i:02d}".encode() for i in range(40)]
    framed_output = b"".join(A.frame(
        "attach-output", data=base64.b64encode(value).decode("ascii"))
        for value in texts)
    painted = bytearray()
    stop_reader = threading.Event()

    class FailedWriteClient:
        # Preserve the real kernel socket for select and recv. Only the
        # deliberately failed host write is simulated for the return-zero case.
        def __init__(self, inner):
            self.inner = inner
            self.failed_writes = []

        def fileno(self):
            return self.inner.fileno()

        def recv(self, size):
            return self.inner.recv(size)

        def send(self, value):
            if b'"op":"attach-close"' in value:
                return self.inner.send(value)  # best-effort cleanup on closed peer
            self.failed_writes.append(bytes(value))
            if write_failure == "zero":
                return 0
            return self.inner.send(value)  # real BrokenPipeError: closed peer

        def close(self):
            self.inner.close()

    def consume_outer_terminal():
        while not stop_reader.is_set():
            ready, _, _ = select.select([master], [], [], .05)
            if ready:
                try:
                    painted.extend(os.read(master, 4096))
                except OSError:
                    return

    client_proxy = FailedWriteClient(client)
    try:
        peer.sendall(framed_output)
        peer.close()  # before the first pending resize is written
        reader = threading.Thread(target=consume_outer_terminal, daemon=True)
        reader.start()
        result = A.run_terminal(client_proxy, "t" * 32, input_fd=slave, output_fd=slave)
        _wait(lambda: texts[-1] in painted, 2)
        assert result == "connection-ended"
        assert len(client_proxy.failed_writes) == 1, client_proxy.failed_writes
        assert b'"op":"attach-resize"' in client_proxy.failed_writes[0]
        last = -1
        for value in texts:
            index = painted.find(value, last + 1)
            assert index > last, (value, len(painted))
            last = index
        assert _termios_without_pendin(termios.tcgetattr(slave)) == _termios_without_pendin(original)
        assert _nonblocking_flag(slave) == old_nonblock
    finally:
        stop_reader.set()
        if 'reader' in locals():
            reader.join(timeout=1)
        client_proxy.close()
        peer.close()
        os.close(master)
        os.close(slave)


def test_terminal_cleanup_on_output_error(monkeypatch):
    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 95, 0, 0))
    prior = termios.tcgetattr(slave)
    a, b = socket.socketpair()
    original = A.os.write
    calls = [0]

    def broken(fd, data):
        if fd == slave:
            calls[0] += 1
            if calls[0] == 1:
                raise OSError("terminal cannot draw")
        return original(fd, data)

    monkeypatch.setattr(A.os, "write", broken)
    try:
        with pytest.raises(OSError, match="cannot draw"):
            A.run_terminal(a, "t" * 32, input_fd=slave, output_fd=slave)
        assert _termios_without_pendin(termios.tcgetattr(slave)) == _termios_without_pendin(prior)
    finally:
        a.close()
        b.close()
        os.close(master)
        os.close(slave)


def test_old_host_normal_ops_and_attach_version_refusal(monkeypatch):
    # Simulate the old, still-running owner: same authentication works for
    # ordinary status, but it has no attach-open operation.
    import ccx.claude.attach as attach_module
    from ccx.store import store as generation
    from ccx.rpc import transport
    class Store:
        def revalidate(self):
            pass
    class Plan:
        final = "/tmp/not-used"
    client, server = socket.socketpair()
    observed = []
    def old_host():
        incoming = bytearray()
        while len(observed) < 3:
            incoming.extend(server.recv(4096))
            while b"\n" in incoming:
                line, _, rest = incoming.partition(b"\n")
                incoming[:] = rest
                msg = json.loads(line)
                observed.append(msg["op"])
                if msg["op"] == "handshake":
                    server.sendall(A.frame("reply", ok=True, identity={"old": True}))
                elif msg["op"] == "status":
                    server.sendall(A.frame("reply", ok=True, state="idle"))
                else:
                    server.sendall(A.frame("reply", ok=False, error="unknown op 'attach-open'"))
        server.close()
    t = threading.Thread(target=old_host, daemon=True)
    t.start()
    class SocketWrapper:
        def __init__(self, *a, **kw):
            self.inner = client
        def __getattr__(self, name):
            return getattr(self.inner, name)
        def __enter__(self):
            return self
        def __exit__(self, *args):
            self.close()
        def close(self):
            pass
        def connect(self, address):
            pass  # already-connected socketpair
    monkeypatch.setattr(A.socket, "socket", SocketWrapper)
    monkeypatch.setattr(store_root, "require_ccx_active", lambda root: Store())
    monkeypatch.setattr(transport, "plan_transport", lambda store, target: Plan())
    monkeypatch.setattr(transport, "handshake_payload", lambda plan: {"old": True})
    monkeypatch.setattr(transport, "verify_handshake", lambda *args, **kw: None)
    try:
        client.sendall(A.frame("status"))
        assert json.loads(client.recv(4096))['state'] == 'idle'
        with pytest.raises(RuntimeError, match="restart the worker"):
            A.open_stream("claude:old", rows=30, cols=100, initial_output=bytearray())
        t.join(timeout=2)
        assert observed == ["status", "handshake", "attach-open"]
    finally:
        client.close()
        server.close()


def test_fake_original_host_stays_single_and_shared_attach_receives_human_turn(
    short_root, tmp_path, monkeypatch,
):
    launches = _deadline_fake_claude(short_root, tmp_path, monkeypatch)
    monkeypatch.setenv("FAKE_HOOKS_VIA_SETTINGS", "1")
    native_log = tmp_path / "native-input.log"
    monkeypatch.setenv("FAKE_LOG", str(native_log))
    store = require_ccx_active(short_root)
    sock = None
    try:
        _first_wait_original_claude_ready(tmp_path, WORKER_ID)
        record = store.record(WORKER_ID)
        proof = record["lifecycle"]["nativeStartProof"]
        original_host, original_child = proof["hostPid"], proof["workerPid"]
        assert launches.read_text().splitlines() == ["provider-start"]
        handle = "claude:" + WORKER_ID
        sock, token = A.open_stream(handle, rows=24, cols=99, initial_output=bytearray())
        assert _wait(lambda: rpc._rpc(handle, {"op": "status"}).get("attached"))
        # A second terminal attaches alongside, then detaches on its own.
        second, second_token = A.open_stream(handle, rows=32, cols=88, initial_output=bytearray())
        try:
            assert _wait(lambda: rpc._rpc(handle, {"op": "status"}).get("attachedTerminals") == 2)
            second.sendall(A.frame("attach-close", token=second_token))
            assert _wait(lambda: rpc._rpc(handle, {"op": "status"}).get("attachedTerminals") == 1)
        finally:
            second.close()
        # A human prompt travels over the *same host PTY* without a second
        # launch. Native hooks record it as an ordinary non-semantic user turn.
        sock.sendall(A.frame("attach-input", token=token,
                             data=base64.b64encode(b"human shared turn\r").decode()))
        _wait(lambda: b"human shared turn" in native_log.read_bytes() if native_log.exists() else False)
        _wait(lambda: (store.record(WORKER_ID).get("lastSettledTurn") or {}).get("turn") == 1)
        assert owned_pid_alive(original_host, proof["hostStart"])
        assert owned_pid_alive(original_child, proof["workerStart"])
        assert launches.read_text().splitlines() == ["provider-start"]
        assert rpc._rpc(handle, {"op": "status"})["attached"]
        semantic = CcxClient().send(handle, "managed send alongside human", wait=True, timeout=15)
        assert semantic.settled
        assert rpc._rpc(handle, {"op": "status"})["attached"]
        assert launches.read_text().splitlines() == ["provider-start"]
        sock.sendall(A.frame("attach-close", token=token))
        _wait(lambda: not rpc._rpc(handle, {"op": "status"}).get("attached"))
        events = [json.loads(row) for row in events_ndjson_path(WORKER_ID).read_text().splitlines()]
        assert [e["type"] for e in events if e["type"].startswith("attach_")] == [
            "attach_opened", "attach_opened", "attach_closed", "attach_closed"]
        # An oversized/fragmented attach frame terminates only its viewer,
        # never the original host or Claude, and normal RPCs keep answering.
        overflow_sock, overflow_token = A.open_stream(handle, rows=26, cols=90, initial_output=bytearray())
        overflow_sock.setblocking(True)
        try:
            giant = A.frame("attach-input", token=overflow_token,
                            data=base64.b64encode(b"z" * (A.MAX_INPUT_FRAME + 1)).decode())
            overflow_sock.sendall(giant[:17])
            overflow_sock.sendall(giant[17:])
            _wait(lambda: not rpc._rpc(handle, {"op": "status"}).get("attached"))
            assert rpc._rpc(handle, {"op": "status"}).get("ok")
            assert owned_pid_alive(original_child, proof["workerStart"])
        finally:
            overflow_sock.close()
    finally:
        if sock:
            sock.close()
        _stop_disposable_original(short_root)

@pytest.mark.parametrize("failure", [False, True])
def test_native_permission_hook_reconciliation_releases_without_second_decision(
    short_root, tmp_path, monkeypatch, failure,
):
    from ccx.errors import CreatePartialFailure
    launches = _deadline_fake_claude(short_root, tmp_path, monkeypatch)
    monkeypatch.setenv("FAKE_HOOKS_VIA_SETTINGS", "1")
    monkeypatch.setenv("FAKE_LOG", str(tmp_path / "fake-native-input.log"))
    result_log = tmp_path / "native-hook-return.jsonl"
    monkeypatch.setenv("FAKE_NATIVE_RECONCILE_LOG", str(result_log))
    store = require_ccx_active(short_root)
    try:
        deadline = time.monotonic() + 13
        while True:
            try:
                CcxClient().new(cwd=str(tmp_path), worker_id=WORKER_ID,
                                skip_permissions=False, start_timeout=3)
                break
            except CreatePartialFailure:
                assert time.monotonic() < deadline
        handle = "claude:" + WORKER_ID
        label = "native permission failure" if failure else "native permission completion"
        from ccx.ops.wait import reconcile_turn
        result = CcxClient().send(handle, label, wait=False, timeout=20)
        # The native tool resolves this gate itself; observe its original receipt
        # without asking send(wait=True) to wait through required input.
        _wait(lambda: reconcile_turn(handle, delivery_id=result.delivery_id)["status"]
              == "settled", timeout=20)
        logged = [json.loads(line) for line in result_log.read_text().splitlines()]
        assert len(logged) == 1 and logged[0]["failure"] is failure
        assert logged[0]["rc"] == 0
        assert json.loads(logged[0]["stdout"]) == {}  # No repeated approval/denial.
        assert rpc._rpc(handle, {"op": "status"})["waitingFor"] is None
        assert launches.read_text().splitlines() == ["provider-start"]
        rows = [json.loads(line) for line in events_ndjson_path(WORKER_ID).read_text().splitlines()]
        settled = [line for line in rows if line["type"] == "question_answered_external"]
        assert len(settled) == 1
        assert settled[0]["outcome"] == ("failure" if failure else "completed")
        assert sum(line["type"] == "permission_rejected" for line in rows) == 0
    finally:
        _stop_disposable_original(short_root)


def test_idle_suppression_detach_reset_and_revive_then_attach(
    short_root, tmp_path, monkeypatch,
):
    """No terminal-owned Claude: idle unload -> ordinary revive -> same host."""
    from ccx.errors import CreatePartialFailure
    launches = _deadline_fake_claude(short_root, tmp_path, monkeypatch)
    monkeypatch.setenv("FAKE_HOOKS_VIA_SETTINGS", "1")
    monkeypatch.setenv("FAKE_LOG", str(tmp_path / "fake-input.log"))
    store = require_ccx_active(short_root)
    handle = "claude:" + WORKER_ID
    master = None
    proc = None
    try:
        deadline = time.monotonic() + 15
        while True:
            try:
                CcxClient().new("initial history for promptless resume", cwd=str(tmp_path),
                                worker_id=WORKER_ID, idle_timeout=3, wait=True,
                                start_timeout=4, timeout=15)
                break
            except CreatePartialFailure:
                assert time.monotonic() < deadline
        before = store.record(handle)
        assert before["lifecycle"]["state"] == "idle"
        _wait(lambda: store.record(handle)["lifecycle"]["state"] == "idle-killed", 8)
        assert launches.read_text().splitlines() == ["provider-start"]
        master, slave = pty.openpty()
        fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 98, 0, 0))
        proc = subprocess.Popen([sys.executable, "-m", "ccx", "attach", handle],
                                env=os.environ, cwd=str(tmp_path),
                                stdin=slave, stdout=slave, stderr=slave,
                                start_new_session=True)
        os.close(slave)
        _wait(lambda: rpc._rpc(handle, {"op": "status"}, timeout=.5).get("attached"), 26)
        second = store.record(handle)
        newproof = second["lifecycle"]["nativeStartProof"]
        assert newproof["workerPid"] != before["lifecycle"]["nativeStartProof"]["workerPid"]
        assert launches.read_text().splitlines() == ["provider-start", "provider-start"]
        time.sleep(4.0)  # exceeds idle timeout while attached
        assert rpc._rpc(handle, {"op": "status"})["attached"] is True
        assert store.record(handle)["lifecycle"]["state"] == "idle"
        os.write(master, b"\x1a")
        proc.wait(timeout=8)
        assert proc.returncode == 0
        _wait(lambda: not rpc._rpc(handle, {"op": "status"}, timeout=.5).get("attached"), 4)
        # Idle clock starts on detach, not at the original pre-attach IDLE.
        assert store.record(handle)["lifecycle"]["state"] == "idle"
        _wait(lambda: store.record(handle)["lifecycle"]["state"] == "idle-killed", 8)
        assert launches.read_text().splitlines() == ["provider-start", "provider-start"]
    finally:
        if proc is not None and proc.poll() is None:
            proc.terminate()  # only this test's exact ccx attach PID
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=3)
        if master is not None:
            os.close(master)
        _stop_disposable_original(short_root)


def test_busy_steer_eligibility_unchanged_while_attached(short_root, tmp_path, monkeypatch):
    from ccx.errors import CreatePartialFailure
    launches = _deadline_fake_claude(short_root, tmp_path, monkeypatch)
    monkeypatch.setenv("FAKE_HOOKS_VIA_SETTINGS", "1")
    monkeypatch.setenv("FAKE_MODE", "nostop")
    monkeypatch.setenv("FAKE_LOG", str(tmp_path / "fake-pty.log"))
    handle = "claude:" + WORKER_ID
    sock = None
    try:
        end = time.monotonic() + 14
        while True:
            try:
                CcxClient().new("long-running fake turn", cwd=str(tmp_path),
                                worker_id=WORKER_ID, start_timeout=4, wait=False)
                break
            except CreatePartialFailure:
                assert time.monotonic() < end
        _wait(lambda: rpc._rpc(handle, {"op": "status"}).get("state") == "busy")
        sock, token = A.open_stream(handle, rows=25, cols=98, initial_output=bytearray())
        assert rpc._rpc(handle, {"op": "status"})["attached"]
        result = CcxClient().steer(handle, "steer alongside human", wait=False)
        assert result.accepted
        assert launches.read_text().splitlines() == ["provider-start"]
        assert rpc._rpc(handle, {"op": "status"})["attached"]
    finally:
        if sock:
            sock.close()
        _stop_disposable_original(short_root)

@pytest.mark.parametrize("late_stop", [False, True])
def test_passive_human_interrupt_marker_settles_once_during_attachment(
    short_root, tmp_path, monkeypatch, late_stop,
):
    from ccx.errors import CreatePartialFailure
    launches = _deadline_fake_claude(short_root, tmp_path, monkeypatch)
    monkeypatch.setenv("FAKE_HOOKS_VIA_SETTINGS", "1")
    monkeypatch.setenv("FAKE_MODE", "nostop,manual_interrupt_marker" +
                       (",manual_interrupt_late_stop" if late_stop else ""))
    log = tmp_path / "native-writes.bin"
    monkeypatch.setenv("FAKE_LOG", str(log))
    handle = "claude:" + WORKER_ID
    store = require_ccx_active(short_root)
    sock = None
    try:
        end = time.monotonic() + 15
        while True:
            try:
                CcxClient().new("turn that will be interrupted", cwd=str(tmp_path),
                                worker_id=WORKER_ID, start_timeout=4, wait=False)
                break
            except CreatePartialFailure:
                assert time.monotonic() < end
        ref = _wait(lambda: rpc._rpc(handle, {"op": "status"}).get("activeTurnRef"))
        # Semantic staging may be BUSY before the native UPS starts. Only a
        # genuine PreToolUse after UPS proves fake Claude reached its input
        # read loop; a preceding ESC would belong to no real active turn.
        _wait(lambda: any(
            json.loads(line).get("type") == "tool_started"
            for line in events_ndjson_path(WORKER_ID).read_text().splitlines()), 12)
        sock, token = A.open_stream(handle, rows=24, cols=99, initial_output=bytearray())
        sock.sendall(A.frame("attach-input", token=token,
                             data=base64.b64encode(b"\x1b").decode()))
        _wait(lambda: b"\x1b" in log.read_bytes() if log.exists() else False)
        _wait(lambda: (store.record(WORKER_ID).get("lastSettledTurn") or {}).get("outcome")
              == "interrupted", timeout=8)
        time.sleep(2.7 if late_stop else .3)
        status = rpc._rpc(handle, {"op": "status"})
        events = [json.loads(line) for line in events_ndjson_path(WORKER_ID).read_text().splitlines()]
        assert status["state"] == "idle" and status["turns"] == 1, [
            (e["type"], e.get("source"), e.get("turn_ref"), e.get("turn")) for e in events]
        assert store.record(WORKER_ID)["lastSettledTurn"]["turnRef"] == ref
        assert sum(e["type"] == "turn_complete" and e.get("turn_ref") == ref
                   and e.get("interrupted") is True for e in events) == 1
        assert launches.read_text().splitlines() == ["provider-start"]
    finally:
        if sock:
            sock.close()
        _stop_disposable_original(short_root)


def _native_backstop_fake(short_root, tmp_path, monkeypatch, *, mode="nostop",
                          prompt="original fake native-idle backstop turn"):
    """One exact original fake provider with its own verified native pid registry."""
    launches = _deadline_fake_claude(short_root, tmp_path, monkeypatch)
    home = tmp_path / "isolated-native-provider-home"
    from ccx.store import paths
    from ccx.claude import settings
    # The pytest caller imported constants before switching to the disposable
    # provider profile; pin both imported native-path aliases for CREATE.
    monkeypatch.setattr(constants, "CLAUDE_HOME", home)
    monkeypatch.setattr(constants, "CLAUDE_DOTFILE", home / ".claude.json")
    monkeypatch.setattr(paths, "CLAUDE_HOME", home)
    monkeypatch.setattr(settings, "CLAUDE_DOTFILE", home / ".claude.json")
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(home))
    monkeypatch.setenv("FAKE_NATIVE_SESSION_METADATA", "1")
    monkeypatch.setenv("FAKE_HOOKS_VIA_SETTINGS", "1")
    monkeypatch.setenv("FAKE_MODE", mode)
    monkeypatch.setenv("FAKE_LOG", str(tmp_path / "native-pty-writes.bin"))
    _first_wait_original_claude_ready(tmp_path, WORKER_ID)
    handle = "claude:" + WORKER_ID
    if prompt is not None:
        assert CcxClient().send(handle, prompt, wait=False)
    record = require_ccx_active(short_root).record(handle)
    proof = record["lifecycle"]["nativeStartProof"]
    source = home / "sessions" / (str(proof["workerPid"]) + ".json")
    _wait(lambda: source.exists())
    return handle, record, source, launches


def _native_registry(source: Path, *, state: str, token: int):
    original = json.loads(source.read_text())
    source.write_text(json.dumps({**original, "status": state,
                                  "statusUpdatedAt": token}))


def _native_backstop_events():
    return [json.loads(line) for line in events_ndjson_path(WORKER_ID).read_text().splitlines()]


def _native_stop_report(handle, monkeypatch):
    run = subprocess.run([sys.executable, "-m", "ccx", "report", handle],
                         capture_output=True, text=True, env=os.environ, timeout=12)
    if run.returncode:
        from ccx.ops.report import report_worker
        try:
            report_worker(handle)
        except Exception as e:
            raise AssertionError((run.stdout, run.stderr, str(e), repr(e.__cause__))) from e
    assert run.stdout.strip() == "(turn interrupted; no reply)"
    raw = subprocess.run([sys.executable, "-m", "ccx", "report", handle, "--json"],
                         capture_output=True, text=True, env=os.environ, timeout=12)
    assert raw.returncode == 0, (raw.stdout, raw.stderr)
    assert json.loads(raw.stdout)["status"] == "interrupted"
    assert json.loads(raw.stdout)["outcome"] == "interrupted"


@pytest.mark.parametrize("initial_status", ["busy", "unreadable", "stale", "wrong-session"])
def test_active_backstop_requires_fresh_verified_native_idle(
    short_root, tmp_path, monkeypatch, initial_status,
):
    handle = None
    try:
        handle, rec, source, launches = _native_backstop_fake(
            short_root, tmp_path, monkeypatch)
        _wait(lambda: any(e["type"] == "tool_started" for e in _native_backstop_events()), 12)
        ref = rpc._rpc(handle, {"op": "status"})["activeTurnRef"]
        baseline = int(time.time() * 1000)
        if initial_status == "busy":
            _native_registry(source, state="busy", token=baseline + 100)
        elif initial_status == "unreadable":
            source.write_text("{malformed session status")
        elif initial_status == "stale":
            _native_registry(source, state="idle", token=1)
        else:
            row = json.loads(source.read_text())
            source.write_text(json.dumps({**row, "sessionId": "another-native-session",
                                          "status": "idle", "statusUpdatedAt": baseline + 100}))
        time.sleep(2.8)
        assert rpc._rpc(handle, {"op": "status"})["activeTurnRef"] == ref
        assert not any(e["type"] == "turn_complete" and e.get("turn_ref") == ref
                       for e in _native_backstop_events())
        # Restore original provenance, then show one fresh original idle edge.
        if initial_status in {"unreadable", "wrong-session"}:
            from test_cancel_core import _provider_native_identity
            pid = rec["lifecycle"]["workerPid"]
            start = rec["lifecycle"]["workerStart"]
            domain_start, domain = _provider_native_identity(start)
            source.write_text(json.dumps({"pid": pid, "sessionId": rec["native"]["sessionId"],
                                          "kind": "interactive", "procStart": domain_start,
                                          **domain, "status": "busy",
                                          "statusUpdatedAt": baseline + 100}))
        _native_registry(source, state="idle", token=int(time.time() * 1000) + 100)
        _wait(lambda: (require_ccx_active(short_root).record(handle).get("lastSettledTurn") or {}).get("turnRef") == ref, 9)
        last = require_ccx_active(short_root).record(handle)["lastSettledTurn"]
        assert last["outcome"] == "interrupted"
        assert rpc._rpc(handle, {"op": "status"})["state"] == "idle"
        assert sum(e["type"] == "turn_complete" and e.get("turn_ref") == ref
                   for e in _native_backstop_events()) == 1
        _native_stop_report(handle, monkeypatch)
        assert launches.read_text().splitlines() == ["provider-start"]
    finally:
        _stop_disposable_original(short_root)


def test_ups_blocked_original_turn_settles_native_idle_without_a_stop(
    short_root, tmp_path, monkeypatch,
):
    try:
        handle, rec, source, _ = _native_backstop_fake(
            short_root, tmp_path, monkeypatch, mode="nostop,ups_blocked")
        ref = _wait(lambda: rpc._rpc(handle, {"op": "status"})["activeTurnRef"], 8)
        _native_registry(source, state="idle", token=int(time.time() * 1000) + 100)
        _wait(lambda: (require_ccx_active(short_root).record(handle).get("lastSettledTurn") or {}).get("turnRef") == ref, 8)
        assert require_ccx_active(short_root).record(handle)["lastSettledTurn"]["outcome"] == "interrupted"
        _wait(lambda: any(e["type"] == "turn_complete" and e.get("turn_ref") == ref
                          for e in _native_backstop_events()), 3)
        assert sum(e["type"] == "turn_complete" and e.get("turn_ref") == ref
                   for e in _native_backstop_events()) == 1
    finally:
        _stop_disposable_original(short_root)


@pytest.mark.parametrize("release_stop", ["before-idle-grace", "after-idle-grace"])
def test_stop_and_native_idle_backstop_race_settles_one_original_turn(
    short_root, tmp_path, monkeypatch, release_stop,
):
    gate = tmp_path / "original-stop-permitted"
    monkeypatch.setenv("FAKE_LATE_STOP_GATE", str(gate))
    try:
        handle, rec, source, _ = _native_backstop_fake(
            short_root, tmp_path, monkeypatch,
            mode="nostop,manual_interrupt_marker,late_stop_gate")
        _wait(lambda: any(e["type"] == "tool_started" for e in _native_backstop_events()), 12)
        ref = rpc._rpc(handle, {"op": "status"})["activeTurnRef"]
        _native_registry(source, state="idle", token=int(time.time() * 1000) + 100)
        if release_stop == "after-idle-grace":
            _wait(lambda: (require_ccx_active(short_root).record(handle).get("lastSettledTurn") or {}).get("turnRef") == ref, 9)
            time.sleep(2.3)  # beyond the ordinary 1.6s late-hook guard
        gate.write_text("release")
        _wait(lambda: any(e["type"] == "turn_complete" and e.get("turn_ref") == ref
                          for e in _native_backstop_events()), 8)
        time.sleep(.6)
        expected = "completed" if release_stop == "before-idle-grace" else "interrupted"
        assert require_ccx_active(short_root).record(handle)["lastSettledTurn"]["outcome"] == expected
        assert rpc._rpc(handle, {"op": "status"})["turns"] == 1
        assert sum(e["type"] == "turn_complete" and e.get("turn_ref") == ref
                   for e in _native_backstop_events()) == 1
    finally:
        gate.write_text("release")
        _stop_disposable_original(short_root)


def test_explicit_stop_native_idle_is_interrupted_report(
    short_root, tmp_path, monkeypatch,
):
    try:
        handle, _, source, _ = _native_backstop_fake(short_root, tmp_path, monkeypatch)
        _wait(lambda: any(e["type"] == "tool_started" for e in _native_backstop_events()), 12)
        ref = rpc._rpc(handle, {"op": "status"})["activeTurnRef"]
        result = rpc._rpc(handle, {"op": "interrupt", "expected_turn_ref": ref})
        assert result["ok"] and result["sent"]
        _native_registry(source, state="idle", token=int(time.time() * 1000) + 300)
        _wait(lambda: (require_ccx_active(short_root).record(handle).get("lastSettledTurn") or {}).get("turnRef") == ref, 8)
        assert require_ccx_active(short_root).record(handle)["lastSettledTurn"]["outcome"] == "interrupted"
        _native_stop_report(handle, monkeypatch)
    finally:
        _stop_disposable_original(short_root)


@pytest.mark.parametrize("split", ["full", "partial"])
def test_admission_reply_keeps_coalesced_and_fragmented_terminal_output(split):
    """AF_UNIX doesn't preserve a boundary between opened and first paint."""
    client, host = socket.socketpair()
    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 95, 0, 0))
    original = termios.tcgetattr(slave)
    text = b"FIRST-PAINT-RETAINED"
    opened = A.frame("attach-opened", ok=True, version=A.VERSION, token="t" * 32)
    output = A.frame("attach-output", data=base64.b64encode(text).decode())
    try:
        packet = opened + (output if split == "full" else output[:15])
        host.sendall(packet)
        buffered = bytearray()
        msg = A._read_line(client, buffered)
        assert msg["op"] == "attach-opened"
        assert buffered == (output if split == "full" else output[:15])
        host.sendall((b"" if split == "full" else output[15:]) +
                     A.frame("attach-ended", reason="finished"))
        assert A.run_terminal(client, msg["token"], input_fd=slave,
                              output_fd=slave, initial_output=buffered) == "finished"
        assert _termios_without_pendin(termios.tcgetattr(slave)) == _termios_without_pendin(original)
        # PTY reads are byte streams, not atomic paint frames: under parallel
        # load the initial escape codes and first text paint can arrive apart.
        painted = bytearray()
        deadline = time.monotonic() + 2
        while text not in painted and time.monotonic() < deadline:
            ready, _, _ = select.select([master], [], [], max(0, deadline - time.monotonic()))
            if not ready:
                break
            painted.extend(os.read(master, 8192))
        assert text in painted
    finally:
        host.close()
        client.close()
        os.close(master)
        os.close(slave)


@pytest.mark.parametrize("stopping_signal", [signal.SIGTERM, signal.SIGHUP])
def test_blocked_tty_consumer_sigterm_cannot_strand_raw_terminal(stopping_signal):
    """A full output PTY must not strand termios restoration on TERM/HUP."""
    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 95, 0, 0))
    original = termios.tcgetattr(slave)
    flags = _nonblocking_flag(slave)
    client, host = socket.socketpair()
    pid = os.fork()
    if pid == 0:
        try:
            host.close()
            A.run_terminal(client, "t" * 32, input_fd=slave, output_fd=slave)
        finally:
            os._exit(0)
    client.close()
    try:
        _wait(lambda: termios.tcgetattr(slave) != original, 3)
        packet = A.frame("attach-output", data=base64.b64encode(b"T" * 4000).decode())
        host.sendall(packet * 7)  # deliberately never drain master
        time.sleep(2.0)  # more than 3x the old 0.6s timeout
        exited, _ = os.waitpid(pid, os.WNOHANG)
        if exited:
            pid = None  # reaped: never signal this reused PID during cleanup
        assert exited == 0, "paused tty consumer caused premature client exit"
        os.kill(pid, stopping_signal)
        deadline = time.monotonic() + 1.8
        while time.monotonic() < deadline:
            result, _ = os.waitpid(pid, os.WNOHANG)
            if result:
                pid = None
                break
            time.sleep(.02)
        assert pid is None, "TERM was unable to unwind blocked TTY output"
        assert _termios_without_pendin(termios.tcgetattr(slave)) == _termios_without_pendin(original)
        assert _nonblocking_flag(slave) == flags
    finally:
        if pid is not None:
            try:os.kill(pid, signal.SIGKILL)  # exactly this test's own child
            except ProcessLookupError:pass
            os.waitpid(pid, 0)
        host.close()
        os.close(master)
        os.close(slave)


def test_pty_write_absolute_budget_is_not_extended_by_positive_short_writes(monkeypatch):
    from ccx.claude.host import session as H
    clock = [0.0]
    writes = []
    def short_write(fd, data):
        assert fd == 123456 and data
        clock[0] += .009
        writes.append(data[:1])
        return 1
    with monkeypatch.context() as patch:
        patch.setattr(H.os, "write", short_write)
        patch.setattr(H.time, "monotonic", lambda: clock[0])
        assert not H_pty.write_all(123456, b"a" * 4096, timeout_s=.01,
                               absolute_timeout_s=.01)
        assert len(writes) <= 2  # at most 18ms modeled, not 36 seconds
        writes.clear()
        clock[0] = 0.0
        # Ordinary send retains the progress-only deadline until last byte.
        assert H_pty.write_all(123456, b"xyz", timeout_s=.01)
        assert len(writes) == 3 and clock[0] > .01


def test_marker_a_then_ups_b_late_stop_a_cannot_finalize_successor_b(
    short_root, tmp_path, monkeypatch,
):
    """One host, exact parent IDs: marker(A), UPS(B), Stop(A), Stop(B)."""
    from ccx.errors import CreatePartialFailure
    gate = tmp_path / "allow-B-real-Stop"
    monkeypatch.setenv("FAKE_SUCCESSOR_STOP_GATE", str(gate))
    launches = _deadline_fake_claude(short_root, tmp_path, monkeypatch)
    monkeypatch.setenv("FAKE_HOOKS_VIA_SETTINGS", "1")
    monkeypatch.setenv("FAKE_LOG", str(tmp_path / "fake-input.log"))
    handle = "claude:" + WORKER_ID
    store = require_ccx_active(short_root)
    try:
        until = time.monotonic() + 15
        while True:
            try:
                CcxClient().new(cwd=str(tmp_path), worker_id=WORKER_ID,
                                start_timeout=4)
                break
            except CreatePartialFailure:
                assert time.monotonic() < until
        CcxClient().send(handle, "old prompt Stop arrives after successor UPS",
                         wait=False)
        ref_a = _wait(lambda: next((e["turn_ref"] for e in _native_backstop_events()
                                    if e["type"] == "turn_complete" and e.get("interrupted")), None), 10)
        ref_b = _wait(lambda: next((e["turn_ref"] for e in _native_backstop_events()
                                    if e["type"] == "turn_start" and e.get("turn_ref") != ref_a), None), 10)
        _wait(lambda: any(e["type"] == "late_hook_ignored" and e.get("turn_ref") == ref_a
                          for e in _native_backstop_events()), 8)
        assert ref_b != ref_a
        assert rpc._rpc(handle, {"op": "status"})["activeTurnRef"] == ref_b
        assert not any(e["type"] == "turn_complete" and e.get("turn_ref") == ref_b
                       for e in _native_backstop_events())
        # Deliberately beyond the old 1.60 s heuristic guard expiry.
        time.sleep(2.2)
        assert rpc._rpc(handle, {"op": "status"})["activeTurnRef"] == ref_b
        gate.write_text("release")
        _wait(lambda: any(e["type"] == "turn_complete" and e.get("turn_ref") == ref_b
                          for e in _native_backstop_events()), 10)
        journal = _native_backstop_events()
        assert sum(e["type"] == "turn_complete" and e.get("turn_ref") == ref_a for e in journal) == 1
        assert sum(e["type"] == "turn_complete" and e.get("turn_ref") == ref_b for e in journal) == 1
        assert rpc._rpc(handle, {"op": "status"})["turns"] == 2
        assert launches.read_text().splitlines() == ["provider-start"]
    finally:
        gate.write_text("release")
        _stop_disposable_original(short_root)


@pytest.mark.parametrize("contradiction", ["busy", "unreadable", "shell"])
def test_passive_native_idle_grace_restarts_on_contradictory_sample(
    short_root, tmp_path, monkeypatch, contradiction,
):
    handle = None
    try:
        handle, _rec, source, _ = _native_backstop_fake(short_root, tmp_path, monkeypatch)
        _wait(lambda: any(e["type"] == "tool_started" for e in _native_backstop_events()), 10)
        ref = rpc._rpc(handle, {"op": "status"})["activeTurnRef"]
        token = int(time.time() * 1000) + 50
        _native_registry(source, state="idle", token=token)
        time.sleep(.55)  # let the original host observe the candidate
        if contradiction == "unreadable":
            source.write_text("{malformed")
        else:
            _native_registry(source, state=contradiction, token=token + 100)
        time.sleep(2.25)  # exceeds first candidate's 2s grace
        assert rpc._rpc(handle, {"op": "status"})["activeTurnRef"] == ref
        assert not any(e["type"] == "turn_complete" and e.get("turn_ref") == ref
                       for e in _native_backstop_events())
        if contradiction == "unreadable":
            from test_cancel_core import _provider_native_identity
            store = require_ccx_active(short_root)
            rec = store.record(handle)
            pid = rec["lifecycle"]["workerPid"]
            native_start, domain = _provider_native_identity(rec["lifecycle"]["workerStart"])
            source.write_text(json.dumps({
                "pid": pid, "sessionId": rec["native"]["sessionId"],
                "kind": "interactive", "procStart": native_start, **domain,
                "status": "idle", "statusUpdatedAt": int(time.time() * 1000) + 100}))
        else:
            _native_registry(source, state="idle", token=int(time.time() * 1000) + 100)
        time.sleep(.6)
        assert rpc._rpc(handle, {"op": "status"})["activeTurnRef"] == ref
        _wait(lambda: (require_ccx_active(short_root).record(handle).get("lastSettledTurn") or {}).get("turnRef") == ref, 6)
        assert require_ccx_active(short_root).record(handle)["lastSettledTurn"]["outcome"] == "interrupted"
    finally:
        _stop_disposable_original(short_root)


def test_successor_ups_with_no_observed_idle_edge_cannot_invent_interruption(
    short_root, tmp_path, monkeypatch,
):
    from ccx.errors import CreatePartialFailure
    gate = tmp_path / "release-next-prompt-stop"
    monkeypatch.setenv("FAKE_SUCCESSOR_STOP_GATE", str(gate))
    _deadline_fake_claude(short_root, tmp_path, monkeypatch)
    monkeypatch.setenv("FAKE_HOOKS_VIA_SETTINGS", "1")
    monkeypatch.setenv("FAKE_LOG", str(tmp_path / "fake-native-input.log"))
    handle = "claude:" + WORKER_ID
    try:
        deadline = time.monotonic() + 13
        while True:
            try:
                CcxClient().new(cwd=str(tmp_path), worker_id=WORKER_ID,
                                start_timeout=4)
                break
            except CreatePartialFailure:
                assert time.monotonic() < deadline
        CcxClient().send(handle, "UPS successor before any native idle is observable", wait=False)
        _wait(lambda: any(e["type"] == "turn_start" for e in _native_backstop_events()), 9)
        time.sleep(2.3)
        assert rpc._rpc(handle, {"op": "status"})["state"] == "busy"
        assert not any(e["type"] == "turn_complete" and e.get("interrupted")
                       for e in _native_backstop_events())
        gate.write_text("release")
        _wait(lambda: any(e["type"] == "turn_complete" for e in _native_backstop_events()), 9)
        assert not any(e["type"] == "turn_complete" and e.get("interrupted")
                       for e in _native_backstop_events())
    finally:
        gate.write_text("release")
        _stop_disposable_original(short_root)



def test_successor_ups_during_observed_idle_grace_splits_original_turn_once(
    short_root, tmp_path, monkeypatch,
):
    """An observed native idle edge is ownership evidence before B's UPS."""
    ups_gate = tmp_path / "release-successor-ups"
    stop_gate = tmp_path / "release-successor-stop"
    monkeypatch.setenv("FAKE_SUCCESSOR_UPS_GATE", str(ups_gate))
    monkeypatch.setenv("FAKE_SUCCESSOR_STOP_GATE", str(stop_gate))
    handle = None
    try:
        handle, _rec, source, _ = _native_backstop_fake(
            short_root, tmp_path, monkeypatch,
            prompt="successor UPS during observed idle grace")
        ref_a = _wait(lambda: next((e["turn_ref"] for e in _native_backstop_events()
                                    if e["type"] == "turn_start"), None), 10)
        _wait(lambda: any(e["type"] == "tool_started" for e in _native_backstop_events()), 10)
        _native_registry(source, state="idle", token=int(time.time() * 1000) + 120)
        time.sleep(.55)  # observer sees A idle, inside the 2s grace
        assert not any(e["type"] == "turn_complete" and e.get("turn_ref") == ref_a
                       for e in _native_backstop_events())
        ups_gate.write_text("continue")
        _wait(lambda: any(e["type"] == "turn_complete" and e.get("turn_ref") == ref_a
                          for e in _native_backstop_events()), 8)
        ref_b = _wait(lambda: next((e["turn_ref"] for e in _native_backstop_events()
                                    if e["type"] == "turn_start" and e.get("turn_ref") != ref_a), None), 8)
        assert require_ccx_active(short_root).record(handle)["lastSettledTurn"]["outcome"] == "interrupted"
        assert rpc._rpc(handle, {"op": "status"})["activeTurnRef"] == ref_b
        assert not any(e["type"] == "turn_complete" and e.get("turn_ref") == ref_b
                       for e in _native_backstop_events())
        stop_gate.write_text("continue")
        _wait(lambda: any(e["type"] == "turn_complete" and e.get("turn_ref") == ref_b
                          for e in _native_backstop_events()), 8)
        events = _native_backstop_events()
        assert sum(e["type"] == "turn_complete" and e.get("turn_ref") == ref_a for e in events) == 1
        assert sum(e["type"] == "turn_complete" and e.get("turn_ref") == ref_b for e in events) == 1
        assert rpc._rpc(handle, {"op": "status"})["turns"] == 2
    finally:
        ups_gate.write_text("continue")
        stop_gate.write_text("continue")
        _stop_disposable_original(short_root)


def test_paused_terminal_consumer_resumes_byte_exact_output_without_detach():
    """A paused PTY reader is NOT an attach failure; output drains after resume.

    The payload is much larger than the tiny Darwin PTY output buffer. A real
    process runs the client on its own tty while the parent deliberately stops
    reading for several seconds, then drains and checks the exact byte stream.
    """
    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 32, 108, 0, 0))
    original = termios.tcgetattr(slave)
    original_flags = _nonblocking_flag(slave)
    client, host = socket.socketpair()
    pid = os.fork()
    if pid == 0:
        try:
            host.close()
            A.run_terminal(client, "t" * 32, input_fd=slave, output_fd=slave)
        finally:
            os._exit(0)
    client.close()
    try:
        _wait(lambda: _termios_without_pendin(termios.tcgetattr(slave)) !=
              _termios_without_pendin(original), 3)
        payload = bytes(range(256)) * 132  # 33,792 raw bytes, including NUL/FF
        for offset in range(0, len(payload), 3000):
            host.sendall(A.frame("attach-output", data=base64.b64encode(
                payload[offset:offset + 3000]).decode()))
        time.sleep(2.1)  # no consumer, much longer than the former .6s timeout
        exited, _ = os.waitpid(pid, os.WNOHANG)
        if exited:
            pid = None
        assert not exited, "paused PTY reader unexpectedly dropped attachment"
        displayed = bytearray()
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and payload not in displayed:
            ready, _, _ = select.select([master], [], [], .15)
            if ready:
                try:
                    part = os.read(master, 16384)
                except OSError:
                    break
                displayed.extend(part)
        assert payload in displayed, (len(displayed), len(payload))
        # A fully recovered terminal consumer can still receive a final frame.
        host.sendall(A.frame("attach-ended", reason="finished"))
        deadline = time.monotonic() + 4
        while time.monotonic() < deadline:
            exited, _ = os.waitpid(pid, os.WNOHANG)
            if exited:
                pid = None
                break
            ready, _, _ = select.select([master], [], [], .1)
            if ready:
                try:
                    os.read(master, 16384)
                except OSError:
                    pass
        assert pid is None, "client did not exit after explicit attach-ended"
        assert _termios_without_pendin(termios.tcgetattr(slave)) == _termios_without_pendin(original)
        assert _nonblocking_flag(slave) == original_flags
    finally:
        if pid is not None:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            os.waitpid(pid, 0)
        host.close()
        os.close(master)
        os.close(slave)



def test_external_stop_on_active_human_typed_turn_without_opening_receipt(
    short_root, tmp_path, monkeypatch,
):
    """The host's current exact parent ref suffices for receiptless Claude stop.

    The native interrupt is still owned by one durable cancel intent and its
    acknowledgement; no synthetic send/opening receipt is created. Fake native
    idle confirms terminality while the worker remains alive and attached.
    """
    from ccx.claude.delivery import iter_receipts
    from ccx.ops.cancel import cancel_turn
    from ccx.errors import DeliveryRejected
    handle = None
    sock = None
    gate = threading.Event()
    def mark_native_idle():
        if gate.wait(8):
            _native_registry(source, state="idle", token=int(time.time() * 1000) + 250)
    try:
        handle, record, source, launches = _native_backstop_fake(
            short_root, tmp_path, monkeypatch, prompt=None)
        sock, token = A.open_stream(handle, rows=27, cols=101, initial_output=bytearray())
        manual = b"Only a human typed this original prompt"
        sock.sendall(A.frame("attach-input", token=token,
                             data=base64.b64encode(manual + b"\r").decode()))
        _wait(lambda: any(e.get("type") == "tool_started"
                          for e in _native_backstop_events()), 12)
        ref = rpc._rpc(handle, {"op": "status"})["activeTurnRef"]
        assert ref and rpc._rpc(handle, {"op": "status"})["attached"]
        assert not any(r.get("turnRef") == ref and r.get("state") == "accepted"
                       for r in iter_receipts(handle))
        # Existing forged/stale exact-turn protection must not be weakened.
        log = tmp_path / "native-pty-writes.bin"
        before = log.read_bytes().count(b"\x1b")
        with pytest.raises(DeliveryRejected, match="exact active turn"):
            cancel_turn(handle, cancellation_id="fabricated-human-ref",
                        turn_ref="ccx-turn:claude:v1:a-foreign-turn", timeout=.2)
        assert log.read_bytes().count(b"\x1b") == before
        _native_registry(source, state="busy", token=int(time.time() * 1000) + 120)
        worker = threading.Thread(target=mark_native_idle, daemon=True)
        worker.start()
        def watch_interrupt():
            _wait(lambda: any(e.get("type") == "interrupt_sent" and e.get("turn_ref") == ref
                              for e in _native_backstop_events()), 9)
            gate.set()
        watcher = threading.Thread(target=watch_interrupt, daemon=True)
        watcher.start()
        command = subprocess.run(
            [sys.executable, "-m", "ccx", "stop", handle, "--json"],
            capture_output=True, text=True, env=os.environ, timeout=16)
        gate.set()
        worker.join(timeout=3)
        watcher.join(timeout=3)
        assert command.returncode == 0, (command.stdout, command.stderr)
        reply = json.loads(command.stdout)
        assert reply["ok"] is True and reply["turn_ref"] == ref
        assert reply["requested"] is True
        _wait(lambda: (require_ccx_active(short_root).record(handle).get("lastSettledTurn") or {}).get("turnRef") == ref, 8)
        assert require_ccx_active(short_root).record(handle)["lastSettledTurn"]["outcome"] == "interrupted"
        _wait(lambda: any(e.get("type") == "turn_complete" and e.get("turn_ref") == ref
                          for e in _native_backstop_events()), 4)
        assert sum(e.get("type") == "interrupt_sent" and e.get("turn_ref") == ref
                   for e in _native_backstop_events()) == 1
        assert sum(e.get("type") == "turn_complete" and e.get("turn_ref") == ref
                   for e in _native_backstop_events()) == 1
        assert rpc._rpc(handle, {"op": "status"})["attached"] is True
        assert launches.read_text().splitlines() == ["provider-start"]
        _native_stop_report(handle, monkeypatch)
        with pytest.raises(DeliveryRejected, match="no active turn"):
            from ccx.ops.cancel import stop_worker
            stop_worker(handle, timeout=.2)
    finally:
        gate.set()
        if sock is not None:
            sock.close()
        _stop_disposable_original(short_root)


@pytest.mark.parametrize("ending", ["attach-ended", "socket-eof"])
def test_host_ended_stalled_tty_still_accepts_local_detach(ending):
    """On host-ended/EOF, Ctrl-Z must break a blocked final paint drain."""
    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 29, 103, 0, 0))
    original = termios.tcgetattr(slave)
    flags = _nonblocking_flag(slave)
    client, host = socket.socketpair()
    pid = os.fork()
    if pid == 0:
        try:
            host.close()
            A.run_terminal(client, "t" * 32, input_fd=slave, output_fd=slave)
        finally:
            os._exit(0)
    client.close()
    try:
        _wait(lambda: _termios_without_pendin(termios.tcgetattr(slave)) !=
              _termios_without_pendin(original), 3)
        for _ in range(20):
            host.sendall(A.frame("attach-output", data=base64.b64encode(b"F" * 3000).decode()))
        if ending == "attach-ended":
            host.sendall(A.frame("attach-ended", reason="finished"))
        else:
            host.shutdown(socket.SHUT_WR)
        # Never read the outer PTY. It is tiny on macOS, so the queued native
        # final screen cannot drain. An explicit detach still has priority.
        time.sleep(.3)
        exited, _ = os.waitpid(pid, os.WNOHANG)
        if exited:
            pid = None
        assert not exited, "client left before final paint could drain"
        os.write(master, b"discard-ordinary-after-host-end\x1a")
        deadline = time.monotonic() + 1.7
        while time.monotonic() < deadline:
            exited, _ = os.waitpid(pid, os.WNOHANG)
            if exited:
                pid = None
                break
            time.sleep(.025)
        assert pid is None, "Ctrl-Z was ignored after host-ended"
        assert _termios_without_pendin(termios.tcgetattr(slave)) == _termios_without_pendin(original)
        assert _nonblocking_flag(slave) == flags
        host.settimeout(1)
        sent_after = bytearray()
        while True:
            try:
                part = host.recv(8192)
            except socket.timeout:
                break
            if not part:
                break
            sent_after.extend(part)
        assert b'"op":"attach-input"' not in sent_after
    finally:
        if pid is not None:
            try:
                os.kill(pid, signal.SIGKILL)  # exact test-owned child
            except ProcessLookupError:
                pass
            os.waitpid(pid, 0)
        host.close()
        os.close(master)
        os.close(slave)


def test_explicit_stop_native_busy_successor_before_ups_keeps_two_turns(
    short_root, tmp_path, monkeypatch,
):
    """Explicit A idle edge survives B busy token until B's UPS arrives.

    Clearing settling_turn_ref when native BUSY(B) arrives used to reuse A's
    ref for B. The resulting B Stop corrupted both settlement and reporting.
    """
    from ccx.ops.report import report_worker
    ups_gate = tmp_path / "release-B-UPS"
    stop_gate = tmp_path / "release-B-Stop"
    monkeypatch.setenv("FAKE_SUCCESSOR_UPS_GATE", str(ups_gate))
    monkeypatch.setenv("FAKE_SUCCESSOR_STOP_GATE", str(stop_gate))
    handle = None
    try:
        handle, _rec, source, _ = _native_backstop_fake(
            short_root, tmp_path, monkeypatch,
            prompt="successor UPS during observed idle grace")
        _wait(lambda: any(e["type"] == "tool_started" for e in _native_backstop_events()), 9)
        ref_a = rpc._rpc(handle, {"op": "status"})["activeTurnRef"]
        assert ref_a
        from ccx.claude.delivery import iter_receipts
        _wait(lambda: any(r.get("turnRef") == ref_a and r.get("state") == "accepted"
                          for r in iter_receipts(handle)), 8)
        sent = rpc._rpc(handle, {"op": "interrupt", "expected_turn_ref": ref_a})
        assert sent.get("ok") and sent.get("sent") and sent.get("turnRef") == ref_a
        token_idle = int(time.time() * 1000) + 200
        _native_registry(source, state="idle", token=token_idle)
        time.sleep(.28)  # host's .1s native poll observes the first idle edge
        assert not any(e["type"] == "turn_complete" and e.get("turn_ref") == ref_a
                       for e in _native_backstop_events())
        # A queued native successor now starts BEFORE its UPS reaches CCX.
        _native_registry(source, state="busy", token=token_idle + 300)
        time.sleep(.14)  # let host sample busy(B) while A is still settling
        assert rpc._rpc(handle, {"op": "status"})["activeTurnRef"] == ref_a
        ups_gate.write_text("continue")
        _wait(lambda: any(e["type"] == "turn_complete" and e.get("turn_ref") == ref_a
                          for e in _native_backstop_events()), 10)
        ref_b = _wait(lambda: next((e["turn_ref"] for e in _native_backstop_events()
                                    if e["type"] == "turn_start" and e.get("turn_ref") != ref_a), None), 9)
        assert ref_b != ref_a
        journal = _native_backstop_events()
        assert any(e["type"] == "turn_complete" and e["turn_ref"] == ref_a
                   and e.get("interrupted") for e in journal)
        assert rpc._rpc(handle, {"op": "status"})["activeTurnRef"] == ref_b
        assert not any(e["type"] == "turn_complete" and e.get("turn_ref") == ref_b
                       for e in journal)
        stop_gate.write_text("continue")
        _wait(lambda: any(e["type"] == "turn_complete" and e.get("turn_ref") == ref_b
                          for e in _native_backstop_events()), 10)
        journal = _native_backstop_events()
        assert sum(e["type"] == "turn_complete" and e.get("turn_ref") == ref_a
                   for e in journal) == 1
        assert sum(e["type"] == "turn_complete" and e.get("turn_ref") == ref_b
                   and not e.get("interrupted") for e in journal) == 1
        last = require_ccx_active(short_root).record(handle)["lastSettledTurn"]
        assert last["turnRef"] == ref_b and last["outcome"] == "completed"
        latest = report_worker(handle)
        assert latest["status"] == "completed"
        assert "native B done" in latest["text"]
    finally:
        ups_gate.write_text("continue")
        stop_gate.write_text("continue")
        _stop_disposable_original(short_root)


def test_receipt_backed_worker_stop_reports_interrupted_exact_terminal(
    short_root, tmp_path, monkeypatch,
):
    """A positive interrupted host CAS drives both V1 cancel and WorkerClient."""
    from ccx.ops import common as protocol_ops
    from ccx.workers import WorkerClient
    from ccx.claude.delivery import iter_receipts
    gate = threading.Event()
    try:
        handle, _rec, source, _ = _native_backstop_fake(short_root, tmp_path, monkeypatch)
        _wait(lambda: any(e["type"] == "tool_started" for e in _native_backstop_events()), 10)
        ref = rpc._rpc(handle, {"op": "status"})["activeTurnRef"]
        assert any(row.get("turnRef") == ref and row.get("state") == "accepted"
                   for row in iter_receipts(handle))

        def observe_native_interrupt():
            # This is the fake provider's real, original session registry; only
            # a physical host interrupt event unlocks the new idle transition.
            _wait(lambda: any(e["type"] == "interrupt_sent" and e.get("turn_ref") == ref
                              for e in _native_backstop_events()), 10)
            _native_registry(source, state="idle", token=int(time.time() * 1000) + 250)
            gate.set()
        watcher = threading.Thread(target=observe_native_interrupt, daemon=True)
        watcher.start()
        # Preserve the actual stop_worker implementation and one-shot intent,
        # only lengthen its observation budget under parallel CI scheduling.
        real_stop = ops_cancel.stop_worker
        with monkeypatch.context() as patch:
            patch.setattr(ops_cancel, "stop_worker", lambda target, **kw:
                          real_stop(target, timeout=5, **{k:v for k,v in kw.items()
                                                       if k != "timeout"}))
            outcome = WorkerClient().stop(handle)
        watcher.join(timeout=3)
        assert gate.is_set(), "native interrupt was never issued"
        assert outcome["confirmed"] is True and outcome["outcome"] == "interrupted", outcome
        assert outcome["cancelled"] is True
        last = require_ccx_active(short_root).record(handle)["lastSettledTurn"]
        assert last["turnRef"] == ref and last["outcome"] == "interrupted"
        _wait(lambda: any(e["type"] == "turn_complete" and e.get("turn_ref") == ref
                          for e in _native_backstop_events()), 3)
        assert sum(e["type"] == "interrupt_sent" and e.get("turn_ref") == ref
                   for e in _native_backstop_events()) == 1
    finally:
        _stop_disposable_original(short_root)


def test_large_buffered_eof_expires_final_paint_deadline_without_tty_reader():
    """EOF behind >1 MiB of framed paint cannot strand a raw terminal.

    A never-reading outer PTY remains full, but the bounded NDJSON buffer can
    still observe socket EOF beyond a full local output queue. The 3s grace
    starts at EOF, not when the last complete frame fits in the tty queue.
    """
    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 27, 98, 0, 0))
    original = termios.tcgetattr(slave)
    original_nonblock = _nonblocking_flag(slave)
    client, host = socket.socketpair()
    pid = os.fork()
    if pid == 0:
        try:
            host.close()
            A.run_terminal(client, "t" * 32, input_fd=slave, output_fd=slave)
        finally:
            os._exit(0)
    client.close()
    try:
        _wait(lambda: _termios_without_pendin(termios.tcgetattr(slave)) !=
              _termios_without_pendin(original), 3)
        host.settimeout(9)
        packet = A.frame("attach-output", data=base64.b64encode(b"E" * 4096).decode())
        assert len(packet) < 8192
        # 285 * 4096 = 1,167,360 raw bytes, larger than the 1 MiB tty queue.
        # This reproduces the review's complete-frame head-of-line block.
        for _ in range(285):
            host.sendall(packet)
        eof_at = time.monotonic()
        host.shutdown(socket.SHUT_WR)
        exited, _ = os.waitpid(pid, os.WNOHANG)
        if exited:
            pid = None
        assert not exited, "socket close bypassed end-drain handling"
        deadline = eof_at + 4.5
        while time.monotonic() < deadline:
            exited, _ = os.waitpid(pid, os.WNOHANG)
            if exited:
                pid = None
                break
            time.sleep(.04)
        assert pid is None, "EOF behind a full terminal queue blocked cleanup past its deadline"
        assert time.monotonic() - eof_at < 4.5
        assert _termios_without_pendin(termios.tcgetattr(slave)) == _termios_without_pendin(original)
        assert _nonblocking_flag(slave) == original_nonblock
    finally:
        if pid is not None:
            try:
                os.kill(pid, signal.SIGKILL)  # only this test's forked child
            except ProcessLookupError:
                pass
            os.waitpid(pid, 0)
        host.close()
        os.close(master)
        os.close(slave)


@pytest.mark.parametrize("observer", ["passive-grace", "explicit-native-unreadable"])
def test_settling_observer_never_starves_real_host_pty_or_attachment(
    short_root, tmp_path, monkeypatch, observer,
):
    """Live fake-host select loop must keep PTY writes AND output fanout alive.

    A negative `continue` in the outer settlement branch skipped hub.tick,
    PTY-read select and semantic pumps. Passive-grace stalled them for two
    seconds; explicit-stop then unreadable native status stalled indefinitely.
    Neither case is allowed to wait for cancellation evidence before servicing
    normal traffic on the exact original host and native PTY.
    """
    from ccx.claude.delivery import iter_receipts

    sock = None
    try:
        handle, _rec, source, _ = _native_backstop_fake(
            short_root, tmp_path, monkeypatch,
            mode="nostop,echo_attached_probe")
        _wait(lambda: any(e["type"] == "tool_started"
                          for e in _native_backstop_events()), 12)
        original_ref = rpc._rpc(handle, {"op": "status"})["activeTurnRef"]
        assert original_ref
        _wait(lambda: any(row.get("turnRef") == original_ref
                          and row.get("state") == "accepted"
                          for row in iter_receipts(handle)), 8)
        sock, token = A.open_stream(
            handle, rows=28, cols=104, initial_output=bytearray())
        assert rpc._rpc(handle, {"op": "status"})["attached"] is True
        if observer == "explicit-native-unreadable":
            sent = rpc._rpc(handle, {"op": "interrupt", "expected_turn_ref": original_ref})
            assert sent.get("ok") and sent.get("sent")
        _native_registry(source, state="idle", token=int(time.time() * 1000) + 150)
        time.sleep(.35)  # host's .1s native poll has observed original idle
        if observer == "explicit-native-unreadable":
            source.write_text("{unreadable original native registry")
            time.sleep(.20)  # the explicit settling observer now cannot settle
        assert rpc._rpc(handle, {"op": "status"})["activeTurnRef"] == original_ref
        probe = b"CCX_SETTLE_SERVICE_" + observer.encode()
        start = time.monotonic()
        sock.sendall(A.frame("attach-input", token=token,
                             data=base64.b64encode(probe).decode("ascii")))
        native_log = tmp_path / "native-pty-writes.bin"
        _wait(lambda: probe in native_log.read_bytes() if native_log.exists() else False,
              timeout=1.0)
        assert time.monotonic() - start < 1.4
        # Output fanout requires the same host to reach select/read_pty_output
        # and hub.flush, not just its already-drained inbound hook socket.
        wire = bytearray()
        def got_fanout():
            ready, _, _ = select.select([sock], [], [], .03)
            if ready:
                try:
                    wire.extend(sock.recv(32768))
                except BlockingIOError:
                    pass
            for row in wire.splitlines():
                try:
                    packet = json.loads(row)
                    if packet.get("op") == "attach-output" and (
                            b"FANOUT_SETTLE_SERVICE_OK" in base64.b64decode(packet["data"])):
                        return True
                except (ValueError, KeyError):
                    continue
            return False
        _wait(got_fanout, timeout=1.0)
        assert time.monotonic() - start < 1.8  # before passive 2s grace can expire
        assert rpc._rpc(handle, {"op": "status"})["attached"] is True
        assert rpc._rpc(handle, {"op": "status"})["activeTurnRef"] == original_ref
    finally:
        if sock is not None:
            sock.close()
        _stop_disposable_original(short_root)


def test_receiptless_stop_reports_completed_when_real_stop_beats_ineffective_escape(
    short_root, tmp_path, monkeypatch,
):
    """A normal native Stop is not cancellation, even after an Escape request.

    Return the exact durable completed outcome for a human-owned turn without
    a synthetic receipt, and keep WorkerClient.stop().cancelled false.
    """
    from ccx.ops import common as protocol_ops
    from ccx.claude.delivery import iter_receipts
    from ccx.workers import WorkerClient

    stop_gate = tmp_path / "release-human-original-Stop"
    monkeypatch.setenv("FAKE_LATE_STOP_GATE", str(stop_gate))
    sock = None
    try:
        handle, _rec, source, _ = _native_backstop_fake(
            short_root, tmp_path, monkeypatch,
            mode="nostop,late_stop_gate", prompt=None)
        sock, token = A.open_stream(handle, rows=27, cols=102,
                                    initial_output=bytearray())
        prompt = b"Receiptless human turn whose real Stop completes normally"
        sock.sendall(A.frame("attach-input", token=token,
                             data=base64.b64encode(prompt + b"\r").decode()))
        _wait(lambda: any(e["type"] == "tool_started"
                          for e in _native_backstop_events()), 12)
        ref = rpc._rpc(handle, {"op": "status"})["activeTurnRef"]
        assert ref and not any(row.get("turnRef") == ref
                               for row in iter_receipts(handle))
        _native_registry(source, state="busy", token=int(time.time() * 1000) + 200)
        def release_real_stop():
            _wait(lambda: any(e.get("type") == "interrupt_sent"
                              and e.get("turn_ref") == ref
                              for e in _native_backstop_events()), 10)
            stop_gate.write_text("release")
        watcher = threading.Thread(target=release_real_stop, daemon=True)
        watcher.start()
        native_stop = ops_cancel.stop_worker
        with monkeypatch.context() as patch:
            patch.setattr(ops_cancel, "stop_worker", lambda target, **kw:
                          native_stop(target, timeout=5,
                                      **{k: v for k, v in kw.items()
                                         if k != "timeout"}))
            result = WorkerClient().stop(handle)
        watcher.join(timeout=3)
        assert result["confirmed"] is True, result
        assert result["outcome"] == "completed" and result["cancelled"] is False
        latest = require_ccx_active(short_root).record(handle)["lastSettledTurn"]
        assert latest["turnRef"] == ref and latest["outcome"] == "completed"
        # Durable CAS is intentionally ahead of turn_complete journal append.
        _wait(lambda: any(e["type"] == "turn_complete" and e.get("turn_ref") == ref
                          for e in _native_backstop_events()), 3)
        assert sum(e["type"] == "turn_complete" and e.get("turn_ref") == ref
                   for e in _native_backstop_events()) == 1
    finally:
        stop_gate.write_text("release")
        if sock is not None:
            sock.close()
        _stop_disposable_original(short_root)
