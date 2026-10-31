"""Codex PTY bar: fake local TUI only; never launch a real provider."""
from __future__ import annotations

import fcntl
import json
import os
import pty
import select
import signal
import struct
import subprocess
import sys
import termios
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from ccx.claude import attach as shared_attach
from ccx.codex import attach_tty as tui
from ccx.cli import codex as codex_commands
from ccx.codex import runtime as codex_runtime
from ccx import constants
from test_generation_store import B


def _tty_attrs(fd):
    # macOS sets PENDIN in lflag on its own; it is not part of what attach restores.
    attrs = termios.tcgetattr(fd)
    attrs[3] &= ~getattr(termios, "PENDIN", 0)
    return attrs


def _fd_flags(fd):
    # Compare only the flags attach changes (macOS adds kernel-internal bits).
    return fcntl.fcntl(fd, fcntl.F_GETFL) & (os.O_ACCMODE | os.O_NONBLOCK)


def test_statusbar_codex_and_claude_share_detach_hint():
    codex = shared_attach.StatusBar(B[:8])
    codex.resize(30, 90)
    codex.polled = "busy"
    codex.update()
    assert codex.text() == f" ccx {B[:8]} · busy · Ctrl-Z detach"
    assert b"Ctrl-Z detach" in codex.poll()
    claude = shared_attach.StatusBar(B[:8])
    claude.resize(30, 90)
    assert "Ctrl-Z detach" in claude.text()


@pytest.mark.parametrize(("reply", "compact", "expected"), [
    ({"ok": True, "state": "busy"}, {}, "busy"),
    ({"ok": True, "state": "idle"}, {}, "idle"),
    ({"ok": True, "state": "waiting", "pendingApprovals": [{}]}, {}, "waiting for approval"),
    ({"ok": True, "state": "busy"}, {"ok": True, "activeCompact": {"id": "x"}}, "compacting"),
])
def test_statusbar_codex_poll_uses_existing_daemon_reads(
    monkeypatch, reply, compact, expected,
):
    calls = []
    monkeypatch.setattr(
        codex_runtime, "observe_worker",
        lambda worker_id, timeout: calls.append(("status", worker_id, timeout)) or
        {"presence": "served", "reply": reply},
    )
    monkeypatch.setattr(
        codex_runtime, "daemon_rpc",
        lambda message, timeout: calls.append(("compact", message, timeout)) or compact,
    )
    assert tui._codex_status(B) == {"ok": True, "state": expected}
    assert calls[0] == ("status", B, .8)
    if reply["state"] == "busy":
        assert calls[1] == ("compact", {"op": "compact-observe", "short": B}, .8)
    else:
        assert len(calls) == 1


def test_codex_poll_unavailable_does_not_guess_state(monkeypatch):
    monkeypatch.setattr(
        codex_runtime, "observe_worker",
        lambda worker_id, timeout: {"presence": "inconclusive",
                                    "reply": {"ok": True, "state": "idle"}},
    )
    assert tui._codex_status(B) is None


@pytest.mark.parametrize("no_status", (False, True))
def test_cmd_attach_always_uses_detach_proxy_even_without_status(
    monkeypatch, tmp_path, no_status,
):
    calls = []

    @contextmanager
    def attached(target):
        calls.append(("attach", target))
        yield {"threadId": "native-thread", "socket": "/tmp/app.sock",
               "cwd": str(tmp_path), "workerId": B}

    monkeypatch.setattr(codex_runtime, "attached_worker", attached)
    monkeypatch.setattr(codex_commands.sys, "stdin", SimpleNamespace(isatty=lambda: True))
    monkeypatch.setattr(codex_commands.sys, "stdout", SimpleNamespace(isatty=lambda: True))
    monkeypatch.setenv("CCX_CODEX_BIN", sys.executable)
    monkeypatch.setattr(
        tui, "run_status_tui",
        lambda argv, **kwargs: calls.append(("bar", argv, kwargs)) or 7,
    )
    monkeypatch.setattr(
        codex_commands.subprocess, "run",
        lambda *_args, **_kwargs: pytest.fail("Codex attach bypassed local PTY proxy"),
    )
    with pytest.raises(SystemExit) as outcome:
        codex_commands.cmd_attach(SimpleNamespace(target=B, no_status=no_status))
    assert outcome.value.code == 7
    expected = [sys.executable, "resume", "native-thread",
                "-c", "check_for_update_on_startup=false", "--remote", "unix:///tmp/app.sock"]
    assert calls == [
        ("attach", B), ("bar", expected,
         {"cwd": str(tmp_path), "worker_id": B, "status": not no_status})
    ]


_FAKE_TUI = r'''
import fcntl, json, os, signal, struct, sys, termios, tty, time
marker = sys.argv[1]
def size():
    return struct.unpack("HHHH", fcntl.ioctl(0, termios.TIOCGWINSZ, b"\0"*8))[:2]
def record(event):
    with open(marker, "a") as handle:
        handle.write(json.dumps([event, size()]) + "\n")
record("initial")
signal.signal(signal.SIGWINCH, lambda *_: record("resize"))
tty.setraw(0)
os.write(1, b"\x1b[?1049h\x1b[?2026h\x1b[H\x1b[JFAKE-CODEX-BODY\x1b[?2026l")
data = b""
while b"/quit\r" not in data:
    part = os.read(0, 256)
    if not part: break
    data += part
os.write(1, b"\x1b[?1049l")
sys.exit(7)
'''


def test_fake_codex_tui_pty_bar_resize_input_restore_and_exit_code(tmp_path):
    """Run a controllable fake PTY child through the real statusbar pump."""
    outer_master, outer_slave = pty.openpty()
    fcntl.ioctl(outer_slave, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 92, 0, 0))
    original_attrs = _tty_attrs(outer_slave)
    original_flags = _fd_flags(outer_slave)
    saved_sigwinch = signal.getsignal(signal.SIGWINCH)
    script = tmp_path / "fake-codex.py"
    script.write_text(_FAKE_TUI)
    marker = tmp_path / "sizes.jsonl"
    captured = bytearray()
    events = []
    done = threading.Event()

    def feed_and_read():
        step = 0
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline and not done.is_set():
            ready, _, _ = select.select([outer_master], [], [], .05)
            if ready:
                try:
                    captured.extend(os.read(outer_master, 65536))
                except OSError:
                    break
            if step == 0 and marker.exists() and b"FAKE-CODEX-BODY" in captured:
                fcntl.ioctl(outer_slave, termios.TIOCSWINSZ,
                            struct.pack("HHHH", 30, 100, 0, 0))
                os.kill(os.getpid(), signal.SIGWINCH)
                step = 1
            if step == 1 and marker.exists():
                try:
                    entries = [json.loads(row) for row in marker.read_text().splitlines()]
                except json.JSONDecodeError:
                    continue
                if any(name == "resize" and dims == [29, 100] for name, dims in entries):
                    events.append("resized")
                    os.write(outer_master, b"/quit\r")
                    step = 2

    thread = threading.Thread(target=feed_and_read, daemon=True)
    thread.start()
    try:
        rc = tui.run_status_tui(
            [sys.executable, str(script), str(marker)],
            cwd=str(tmp_path), worker_id=B,
            input_fd=outer_slave, output_fd=outer_slave,
            status_fetch=lambda: {"ok": True, "state": "busy"},
        )
        assert rc == 7
        assert events == ["resized"]
        assert _fd_flags(outer_slave) == original_flags
        assert _tty_attrs(outer_slave) == original_attrs
        assert signal.getsignal(signal.SIGWINCH) is saved_sigwinch
        assert b"FAKE-CODEX-BODY" in captured
        assert b"Ctrl-Z detach" in captured
        assert b"ccx " + B[:8].encode() in captured
        assert b"quit Codex to detach" not in captured
        observed = [json.loads(line) for line in marker.read_text().splitlines()]
        assert observed[0] == ["initial", [23, 92]]
        assert ["resize", [29, 100]] in observed
    finally:
        done.set()
        thread.join(timeout=2)
        os.close(outer_master)
        os.close(outer_slave)


_FAKE_KEYS_TUI = r"""
import fcntl, json, os, signal, struct, sys, termios, tty
marker = sys.argv[1]
def event(kind, value=""):
    with open(marker, "a") as handle:
        handle.write(json.dumps({"event": kind, "value": value,
                                 "pid": os.getpid()}) + "\n")
signal.signal(signal.SIGHUP, lambda *_: (event("SIGHUP"), os._exit(0)))
signal.signal(signal.SIGTERM, lambda *_: (event("SIGTERM"), os._exit(0)))
tty.setraw(0)
event("READY", struct.unpack("HHHH", fcntl.ioctl(0, termios.TIOCGWINSZ, b"\0"*8))[:2])
os.write(1, b"\x1b[?1049hFAKE-CODEX-KEYS\x1b[?1049l")
while True:
    data = os.read(0, 4096)
    if not data:
        break
    event("input", data.hex())
"""


@pytest.mark.parametrize("status", (True, False))
def test_codex_keys_are_filtered_in_real_pty_and_detach_sends_hup(tmp_path, status):
    """Real Codex path: plain Ctrl-], fragmented paste, Ctrl-C/D,
    Ctrl-Z; cleanup of only the fake child and terminal restoration.
    """
    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 95, 0, 0))
    old_attrs = _tty_attrs(slave)
    old_flags = _fd_flags(slave)
    old_winch = signal.getsignal(signal.SIGWINCH)
    fake = tmp_path / "fake-keys.py"
    fake.write_text(_FAKE_KEYS_TUI)
    marker = tmp_path / "received.jsonl"
    captured = bytearray()
    errors = []

    def events():
        if not marker.exists():
            return []
        try:
            return [json.loads(line) for line in marker.read_text().splitlines()]
        except (ValueError, OSError):
            return []

    def received():
        return bytes.fromhex("".join(item["value"] for item in events()
                                      if item["event"] == "input"))

    def wait_for(predicate, seconds=4):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            if predicate():
                return True
            time.sleep(.015)
        return False

    # Paste markers are intentionally fragmented; *embedded* detachment keys
    # must reach Codex untouched. Ctrl-C and Ctrl-D likewise belong to Codex.
    passthrough = b"\x1d\x1b[200~paste\x1a\x1b[201~\x03\x04"

    def drive():
        try:
            deadline = time.monotonic() + 8
            while time.monotonic() < deadline and not errors:
                readable, _, _ = select.select([master], [], [], .05)
                if readable:
                    captured.extend(os.read(master, 65536))
                if any(event["event"] == "READY" for event in events()):
                    break
            assert any(event["event"] == "READY" for event in events())
            # Ctrl-] is an ordinary key now.
            os.write(master, b"\x1d")
            assert wait_for(lambda: received() == b"\x1d")
            os.write(master, b"\x1b[20")
            time.sleep(.04)
            os.write(master, b"0~paste\x1a")
            time.sleep(.04)
            os.write(master, b"\x1b[201~\x03\x04")
            assert wait_for(lambda: passthrough in received(), 4), received()
            assert not any(event["event"] == "SIGHUP" for event in events())
            os.write(master, b"\x1a")  # Ctrl-Z detaches, with or without the bar
        except BaseException as exc:
            errors.append(exc)
            try:
                os.write(master, b"\x1a")
            except OSError:
                pass

    driver = threading.Thread(target=drive, daemon=True)
    driver.start()
    try:
        rc = tui.run_status_tui(
            [sys.executable, str(fake), str(marker)],
            cwd=str(tmp_path), worker_id=B,
            input_fd=slave, output_fd=slave, status=status,
            status_fetch=lambda: {"ok": True, "state": "idle"},
        )
        driver.join(timeout=3)
        assert not driver.is_alive() and not errors, errors
        assert rc == 0  # local detach, not a remote Codex turn cancellation
        assert wait_for(lambda: any(event["event"] == "SIGHUP" for event in events()))
        assert received() == passthrough
        pid = next(e["pid"] for e in events() if e["event"] == "READY")
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)  # child has been terminated and reaped
        assert _tty_attrs(slave) == old_attrs
        assert _fd_flags(slave) == old_flags
        assert signal.getsignal(signal.SIGWINCH) is old_winch
        assert (b"Ctrl-Z detach" in captured) is status
        assert b"FAKE-CODEX-KEYS" in captured
    finally:
        driver.join(timeout=1)
        os.close(master)
        os.close(slave)


_FAKE_STUBBORN_TUI = r"""
import os, signal, sys, time, tty
signal.signal(signal.SIGHUP, signal.SIG_IGN)
signal.signal(signal.SIGTERM, signal.SIG_IGN)
tty.setraw(0)
with open(sys.argv[1], "w") as marker:
    marker.write(str(os.getpid()))
while True:
    time.sleep(1)
"""


def test_codex_detach_escalates_only_owned_unresponsive_tui(tmp_path, monkeypatch):
    """HUP -> TERM -> KILL if the local client ignores both soft signals."""
    outer_master, outer_slave = pty.openpty()
    fcntl.ioctl(outer_slave, termios.TIOCSWINSZ, struct.pack("HHHH", 25, 90, 0, 0))
    original = _tty_attrs(outer_slave)
    script = tmp_path / "ignore-signals.py"
    script.write_text(_FAKE_STUBBORN_TUI)
    marker = tmp_path / "client.pid"
    calls = []
    real_killpg = os.killpg

    def traced_killpg(pid, sig):
        calls.append((pid, sig))
        real_killpg(pid, sig)

    monkeypatch.setattr(tui.os, "killpg", traced_killpg)

    def drive():
        until = time.monotonic() + 4
        while not marker.exists() and time.monotonic() < until:
            select.select([outer_master], [], [], .02)
        if marker.exists():
            os.write(outer_master, b"\x1a")
        else:
            # Fail the test rather than hang forever if the child never boots.
            os.write(outer_master, b"\x1a")

    driver = threading.Thread(target=drive, daemon=True)
    driver.start()
    try:
        result = tui.run_status_tui(
            [sys.executable, str(script), str(marker)], cwd=str(tmp_path),
            worker_id=B, input_fd=outer_slave, output_fd=outer_slave,
            status=False,
        )
        driver.join(timeout=2)
        assert result == 0 and marker.exists()
        client_pid = int(marker.read_text())
        assert calls == [
            (client_pid, signal.SIGHUP),
            (client_pid, signal.SIGTERM),
            (client_pid, signal.SIGKILL),
        ]
        with pytest.raises(ProcessLookupError):
            os.kill(client_pid, 0)
        assert _tty_attrs(outer_slave) == original
    finally:
        driver.join(timeout=1)
        os.close(outer_master)
        os.close(outer_slave)


def test_codex_attach_help_shares_detach_keys_and_status_optout(capsys):
    from ccx.cli.parser import build_parser
    with pytest.raises(SystemExit) as result:
        build_parser().parse_args(["attach", "--help"])
    assert result.value.code == 0
    help_text = " ".join(capsys.readouterr().out.split())
    assert "Ctrl-Z detaches" in help_text
    assert "status bar is shown unless --no-status" in help_text
    assert "/quit also exits" in help_text
