"""The one-row CCX status bar of `ccx attach`.

Pure tracker tests drive `StatusBar.feed`; terminal tests run the real
`run_terminal` loop on a PTY pair against a socketpair peer. The screen-level
check needs `pyte` and is skipped where it is not installed.
"""
from __future__ import annotations

import base64
import fcntl
import json
import os
import pty
import select
import signal
import socket
import struct
import sys
import termios
import threading
import time
import types

import pytest

from ccx.claude import attach as A
from ccx.cli.parser import build_parser
from test_attach import _nonblocking_flag, _termios_without_pendin, _wait

PREFIX = b"\x1b[?1049h\x1b[?2004h\x1b[?25h\x1b[H\x1b[2J"
CLEANUP = b"\x1b[?2004l\x1b[?1000l\x1b[?1002l\x1b[?1006l\x1b[?1004l\x1b[?25h\x1b[?1049l"


def _bar(rows=24, cols=80, label="busy"):
    bar = A.StatusBar("970ed6cc")
    bar.polled = label
    bar.update()
    assert bar.resize(rows, cols) == rows - 1
    assert bar.poll().startswith(b"\x1b7")
    return bar


def _region(rows):
    return b"\x1b[1;%dr" % (rows - 1)


def _output(raw):
    return A.frame("attach-output", data=base64.b64encode(raw).decode("ascii"))


def _run(bar, script, *, rows=24, cols=80):
    """Run the real terminal loop; `script(send, frames)` is the host peer.

    Returns (reason, outer terminal bytes, client->host frames)."""
    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))
    original = termios.tcgetattr(slave)
    nonblock = _nonblocking_flag(slave)
    client, peer = socket.socketpair()
    captured = bytearray()
    frames = []
    stop = threading.Event()

    def consume():
        while not stop.is_set():
            if select.select([master], [], [], .02)[0]:
                try:
                    captured.extend(os.read(master, 65536))
                except OSError:
                    return

    def host():
        blob = bytearray()

        def read_until(predicate):
            while not predicate():
                if not select.select([peer], [], [], 3)[0]:
                    raise AssertionError("client sent nothing")
                chunk = peer.recv(65536)
                if not chunk:
                    return
                blob.extend(chunk)
                frames[:] = [json.loads(line) for line in blob.splitlines()]

        def send(raw, pause=.15):
            peer.sendall(_output(raw))
            time.sleep(pause)

        try:
            read_until(lambda: any(f["op"] == "attach-resize" for f in frames))
            script(send, frames, read_until, slave)
            time.sleep(.2)
        finally:
            peer.close()

    threads = [threading.Thread(target=consume, daemon=True),
               threading.Thread(target=host, daemon=True)]
    try:
        for thread in threads:
            thread.start()
        reason = A.run_terminal(client, "t" * 32, input_fd=slave, output_fd=slave,
                                status_bar=bar)
        threads[1].join(timeout=5)
        _wait(lambda: captured.endswith(CLEANUP), 3)
        assert _termios_without_pendin(termios.tcgetattr(slave)) == _termios_without_pendin(original)
        assert _nonblocking_flag(slave) == nonblock
        return reason, bytes(captured), list(frames)
    finally:
        stop.set()
        threads[0].join(timeout=1)
        client.close()
        peer.close()
        os.close(master)
        os.close(slave)


# --- labels ----------------------------------------------------------------

@pytest.mark.parametrize("reply, label", [
    ({"ok": True, "state": "idle"}, "idle"),
    ({"ok": True, "state": "busy", "compacting": True}, "compacting"),
    ({"ok": True, "state": "waiting",
      "waitingFor": {"type": "permission", "tool": "Bash"}}, "waiting for permission (Bash)"),
    ({"ok": True, "state": "waiting",
      "waitingFor": {"type": "permission", "tool": "AskUserQuestion"}}, "waiting for answer"),
    ({"ok": True, "state": "waiting", "waitingFor": {"type": "elicitation"}}, "waiting for input"),
    ({"ok": True, "state": "waiting", "waitingFor": None}, "waiting"),
    ({"ok": True, "state": "waiting",
      "waitingFor": {"type": "permission", "tool": "x\x1b[2Jy"}}, "waiting for permission (x[2Jy)"),
    ({"ok": False, "error": "timeout"}, None),
    ({"ok": True}, None),
    ("garbage", None),
])
def test_status_label_from_existing_host_status_fields(reply, label):
    assert A.status_label(reply) == label


def test_bar_text_is_truncated_to_width_and_omits_unknown_state():
    bar = _bar(rows=10, cols=20, label=None)
    assert bar.text() == " ccx 970ed6cc · Ctr"
    painted = bar._paint()
    assert b"\x1b[1;9r\x1b[10;1H" in painted and painted.endswith(b"\x1b[0m\x1b8")
    assert (" ccx 970ed6cc · Ctr".encode()) in painted
    wide = _bar(cols=200)
    assert wide.text() == " ccx 970ed6cc · busy · Ctrl-Z detach"


# --- stream tracking ---------------------------------------------------------

@pytest.mark.parametrize("sequence", [
    b"\x1b[2J", b"\x1b[3J", b"\x1b[J", b"\x1b[r", b"\x1b[;r", b"\x1bc",
    b"\x1b[?1049h", b"\x1b[?1049l", b"\x1b[?1047h", b"\x1b[?47l", b"\x1b[!p",
])
def test_repair_sequences_redraw_region_and_bar_right_after_them(sequence):
    bar = _bar()
    out = bar.feed(b"before" + sequence + b"after")
    head, _, rest = out.partition(sequence)
    assert head == b"before"
    assert rest.startswith(b"\x1b7" + _region(24) + b"\x1b[24;1H")
    assert rest.endswith(b"\x1b8after")
    assert out.count(b"\x1b7") == 1


@pytest.mark.parametrize("sequence", [
    b"\x1b[2K", b"\x1b[?25l", b"\x1b[?2004h", b"\x1b[5A", b"\x1b]0;title\x07", b"\x1b7",
    b"\x1b[?1r",  # XTRESTORE, not DECSTBM
])
def test_ordinary_sequences_do_not_redraw(sequence):
    bar = _bar()
    assert bar.feed(b"a" + sequence + b"b") == b"a" + sequence + b"b"


@pytest.mark.parametrize("first, second", [
    (b"x\x1b", b"[31mY"),            # split CSI after ESC
    (b"x\x1b[3", b"1mY"),            # split CSI inside parameters
    (b"x\x1b]0;ti", b"tle\x07Y"),    # split OSC
    (b"x\x1b]0;ti\x1b", b"\\Y"),     # split OSC ST
    (b"x\x1b(", b"BY"),              # split ESC intermediate
    ("xé".encode()[:-1], "é".encode()[-1:] + b"Y"),   # split UTF-8
    ("x│".encode()[:-1], "│".encode()[-1:] + b"Y"),
])
def test_label_change_is_never_injected_mid_sequence(first, second):
    bar = _bar()
    assert bar.feed(first) == first
    bar.polled = "idle"
    bar.update()
    assert bar.poll() == b""  # a loop tick between the two frames
    out = bar.feed(second)
    paint = out.index(b"\x1b7\x1b[1;23r")
    end = out.index(b"\x1b8", paint) + 2
    assert out[:paint] + out[end:] == second
    assert paint >= len(second) - 1  # only after the split sequence completed
    assert b" idle " in out[paint:end]


def test_bar_waits_for_synchronized_update_to_end():
    bar = _bar()
    out = bar.feed(b"\x1b[?2026h\x1b[2Jframe")
    assert out == b"\x1b[?2026h\x1b[2Jframe"
    bar.polled = "idle"
    bar.update()
    assert bar.poll() == b""
    out = bar.feed(b"more\x1b[?2026lafter")
    assert out.startswith(b"more\x1b[?2026l\x1b7" + _region(24))
    assert out.endswith(b"\x1b8after") and out.count(b"\x1b7") == 1


def test_tiny_terminal_disables_bar_and_passes_through():
    bar = A.StatusBar("970ed6cc")
    bar.polled = "busy"
    bar.update()
    assert bar.resize(5, 80) == 5
    assert bar.resize(24, 19) == 24
    assert bar.poll() == b""
    data = b"x\x1b[2J\x1b[r\x1bc\x1b[?1049hy"
    assert bar.feed(data) == data
    bar.polled = "idle"
    bar.update()
    assert bar.poll() == b""
    assert bar.restore() == b""


def test_resize_redraws_and_shrinking_to_tiny_resets_region():
    bar = _bar(rows=24)
    assert bar.resize(30, 80) == 29
    painted = bar.poll()
    # The old bar row is cleared, then margins and bar move to the new last row.
    assert painted.startswith(b"\x1b7\x1b[24;1H\x1b[0m\x1b[2K" + _region(30) + b"\x1b[30;1H")
    assert bar.resize(4, 80) == 4
    assert bar.restore() == b"\x18\x1b[r"  # reset not yet written
    assert bar.poll() == b"\x1b7\x1b[r\x1b8"
    assert bar.feed(b"\x1b[2J") == b"\x1b[2J"
    assert bar.restore() == b""


# --- the real terminal loop ------------------------------------------------

def test_region_and_bar_drawn_and_resize_sends_rows_minus_one():
    bar = A.StatusBar("970ed6cc")
    bar.polled = "busy"

    def script(send, frames, read_until, slave):
        send(b"hello from claude")
        fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 30, 100, 0, 0))
        os.kill(os.getpid(), signal.SIGWINCH)
        read_until(lambda: sum(f["op"] == "attach-resize" for f in frames) >= 2)
        send(b"\x1b[2Jrepainted")

    reason, captured, frames = _run(bar, script, rows=24, cols=80)
    assert reason == "connection-ended"
    resizes = [(f["rows"], f["cols"]) for f in frames if f["op"] == "attach-resize"]
    assert resizes == [(23, 80), (29, 100)]
    assert captured.startswith(PREFIX + b"\x1b7" + _region(24) + b"\x1b[24;1H")
    assert b" ccx 970ed6cc \xc2\xb7 busy \xc2\xb7 Ctrl-Z detach" in captured
    after_resize = captured.index(b"\x1b7\x1b[24;1H\x1b[0m\x1b[2K" + _region(30))
    assert captured.index(b"hello from claude") < after_resize
    assert captured.index(b"\x1b[2J\x1b7" + _region(30) + b"\x1b[30;1H") > after_resize
    # Exit: abort partial sequences, reset margins, clear bar, then old cleanup.
    assert captured.endswith(b"\x18\x1b[r\x1b[30;1H\x1b[0m\x1b[2K" + CLEANUP)


def test_live_label_change_and_failed_polls_keep_attach_alive():
    calls = []
    replies = iter([{"ok": True, "state": "busy"}, {"ok": True, "state": "idle"}])

    def fetch():
        calls.append(time.monotonic())
        try:
            return next(replies)
        except StopIteration:
            raise ConnectionError("host status unavailable") from None

    bar = A.StatusBar("970ed6cc", fetch, interval=.1)

    def script(send, frames, read_until, slave):
        for i in range(8):
            send(b"line %d\r\n" % i, pause=.1)

    reason, captured, frames = _run(bar, script)
    assert reason == "connection-ended"
    assert len(calls) >= 4  # kept polling after the failure
    busy = captured.index(b"\xc2\xb7 busy \xc2\xb7")
    idle = captured.index(b"\xc2\xb7 idle \xc2\xb7", busy)
    blank = captured.index(b" ccx 970ed6cc \xc2\xb7 Ctrl-Z detach", idle)
    assert b"line 7" in captured[blank:]
    assert [f["op"] for f in frames].count("attach-resize") == 1


def test_no_status_is_byte_exact_pass_through():
    payload = [b"\x1b[2Jone", b"\x1b[r\x1b[3", b"1mtwo\x1bc", "é".encode()]

    def script(send, frames, read_until, slave):
        for part in payload:
            send(part, pause=.05)

    reason, captured, frames = _run(None, script, rows=24, cols=80)
    assert captured == PREFIX + b"".join(payload) + CLEANUP
    assert [(f["rows"], f["cols"]) for f in frames if f["op"] == "attach-resize"] == [(24, 80)]


def test_tiny_terminal_loop_matches_no_status():
    def script(send, frames, read_until, slave):
        send(b"\x1b[2Jsmall")

    bar = A.StatusBar("970ed6cc")
    bar.polled = "busy"
    _, captured, frames = _run(bar, script, rows=5, cols=80)
    assert captured == PREFIX + b"\x1b[2Jsmall" + CLEANUP
    assert [(f["rows"], f["cols"]) for f in frames if f["op"] == "attach-resize"] == [(5, 80)]


@pytest.mark.parametrize("status", [True, False])
def test_attach_opens_rows_minus_one_only_with_status(monkeypatch, status):
    seen = {}
    tty_like = types.SimpleNamespace(isatty=lambda: True, fileno=lambda: 1)
    monkeypatch.setattr(sys, "stdin", tty_like)
    monkeypatch.setattr(sys, "stdout", tty_like)
    monkeypatch.setattr(A, "_tty_size", lambda fd: (40, 120))

    class Sock:
        def close(self):
            pass

    def open_stream(handle, *, rows, cols, initial_output):
        seen["open"] = (rows, cols)
        return Sock(), "t" * 32

    def run_terminal(sock, token, *, initial_output, status_bar):
        seen["bar"] = status_bar
        return "detached"

    monkeypatch.setattr(A, "open_stream", open_stream)
    monkeypatch.setattr(A, "run_terminal", run_terminal)
    assert A.attach("claude:970ed6cc-0000-4000-8000-000000000000", status=status) == "detached"
    if status:
        assert seen["open"] == (39, 120)
        assert seen["bar"].short_id == "970ed6cc" and seen["bar"].fetch is not None
    else:
        assert seen["open"] == (40, 120) and seen["bar"] is None


def test_no_status_flag_parses():
    assert build_parser().parse_args(["attach", "ab12"]).no_status is False
    assert build_parser().parse_args(["attach", "--no-status", "ab12"]).no_status is True


# --- screen level ----------------------------------------------------------

def _claude_like_frames(rows, cols):
    """Ink-style relative repaints that fill and scroll a (rows-1) screen."""
    frames = [b"\x1b[?25l"]
    for i in range(rows * 2):  # more lines than fit: forces scrolling
        frames.append(b"\x1b[2K\x1b[1mline %03d\x1b[0m \xe2\x94\x82 content\r\n" % i)
    prompt = b"\x1b[2K> typed prompt"
    frames += [prompt, b"\x1b[1A\x1b[2K\x1b[1B\x1b[2K\x1b[G> ", b"typed again", b"\x1b[?25h"]
    frames += [b"\x1b[2J\x1b[3J\x1b[H", b"fresh top\r\n", b"\x1b[3", b"2mred\x1b[0m\r\n"]
    frames += ["wide é│".encode()[:-2], "wide é│".encode()[-2:] + b"\r\n"]
    frames += [b"\x1b[?2026h\x1b[H\x1b[2Ksynced", b"\x1b[?2026l"]
    for i in range(rows):
        frames.append(b"tail %02d\r\n" % i)
    return frames


@pytest.mark.parametrize("label_every", [1, 3])
def test_screen_bar_on_last_row_and_claude_content_intact(label_every):
    pyte = pytest.importorskip("pyte")
    rows, cols = 12, 50
    reference = pyte.Screen(cols, rows - 1)
    ref_stream = pyte.ByteStream(reference)
    outer = pyte.Screen(cols, rows)
    out_stream = pyte.ByteStream(outer)
    bar = A.StatusBar("970ed6cc")
    out_stream.feed(PREFIX)
    bar.resize(rows, cols)
    for i, raw in enumerate(_claude_like_frames(rows, cols)):
        if i % label_every == 0:
            bar.polled = ("busy", "idle", None)[i % 3]
            bar.update()
            out_stream.feed(bar.poll())
        ref_stream.feed(raw)
        out_stream.feed(bar.feed(raw))
        assert outer.display[:rows - 1] == reference.display, i
        assert (outer.cursor.x, outer.cursor.y) == (reference.cursor.x, reference.cursor.y), i
        if not bar.pending:
            assert outer.display[-1].rstrip().startswith(" ccx 970ed6cc"), (i, outer.display[-1])
    assert outer.buffer[rows - 1][1].reverse is True
    assert outer.buffer[0][0].reverse is False
