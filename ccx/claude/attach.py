"""One authenticated, bounded shared terminal stream for a managed Claude PTY.

Only the host select loop reads/writes the PTY.  This module owns framing and
local TTY lifecycle; it never launches, parks or resumes a native Claude.
"""
from __future__ import annotations

import base64
import binascii
import fcntl
import json
import os
import select
import signal
import socket
import struct
import sys
import termios
import threading
import time
import tty
import uuid

VERSION = 1
MAX_INPUT_FRAME = 32 * 1024
MAX_PENDING_INPUT = 64 * 1024
MAX_PENDING_OUTPUT = 1024 * 1024
MAX_ATTACH_LINE = 48 * 1024  # 32 KiB base64 + envelope
MAX_DIMENSION = 1000


def frame(op: str, **fields) -> bytes:
    return (json.dumps({"op": op, **fields}, separators=(",", ":")) + "\n").encode()


def dimensions(rows, cols) -> tuple[int, int]:
    if (type(rows) is not int or type(cols) is not int
            or not 1 <= rows <= MAX_DIMENSION or not 1 <= cols <= MAX_DIMENSION):
        raise ValueError("invalid terminal dimensions")
    return rows, cols


def decode_input(data) -> bytes:
    if (not isinstance(data, str) or len(data) > ((MAX_INPUT_FRAME + 2) // 3) * 4):
        raise ValueError("attach input frame too large")
    try:
        raw = base64.b64decode(data, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise ValueError("invalid attach base64 input") from exc
    if len(raw) > MAX_INPUT_FRAME:
        raise ValueError("attach input frame too large")
    return raw


class HostAttachment:
    """Hot-only lease and byte queues. Host invokes all methods on its I/O loop."""

    def __init__(self, master_fd, resize, event, disconnect, on_release):
        self.master_fd = master_fd
        self.resize = resize
        self.event = event
        self.disconnect = disconnect
        self.on_release = on_release
        self.conn = None
        self.token = None
        self.input = bytearray()
        self.output = bytearray()
        self.saved_size = None
        self.requested = None
        self.repaint_at = None

    @property
    def attached(self):
        return self.conn is not None

    def open(self, conn, msg, *, epoch):
        if self.attached:
            return {"ok": False, "error": "another terminal is already attached"}
        if msg.get("version") != VERSION or type(msg.get("version")) is not int:
            return {"ok": False, "error": "unsupported shared attach protocol version"}
        try:
            rows, cols = dimensions(msg.get("rows"), msg.get("cols"))
            saved = fcntl.ioctl(self.master_fd, termios.TIOCGWINSZ, b"\0" * 8)
            # Save the complete ioctl payload, including pixel dimensions.
            if len(saved) != 8:
                raise OSError("PTY size unavailable")
            self.resize(rows, cols - 1 if cols > 1 else 2)
        except (ValueError, OSError) as exc:
            return {"ok": False, "error": str(exc)}
        self.conn = conn
        self.token = uuid.uuid4().hex
        self.saved_size = saved
        self.requested = (rows, cols)
        self.repaint_at = time.monotonic() + .12
        self.input.clear()
        self.output.clear()
        self.event({"type": "attach_opened"})
        return {"ok": True, "op": "attach-opened", "version": VERSION,
                "token": self.token, "epoch": epoch}

    def owns(self, conn, msg):
        return (self.conn is conn and isinstance(msg.get("token"), str)
                and msg["token"] == self.token)

    def accept_input(self, conn, msg):
        if not self.owns(conn, msg):
            raise ValueError("invalid attachment token or connection")
        raw = decode_input(msg.get("data"))
        if len(self.input) + len(raw) > MAX_PENDING_INPUT:
            self.release(conn, "input-overflow")
            self.disconnect(conn)
            return
        self.input.extend(raw)

    def accept_resize(self, conn, msg):
        if not self.owns(conn, msg):
            raise ValueError("invalid attachment token or connection")
        rows, cols = dimensions(msg.get("rows"), msg.get("cols"))
        self.requested = (rows, cols)
        if self.repaint_at is None:
            self.resize(rows, cols)

    def tick(self, write):
        if self.conn is None:
            return
        if self.repaint_at is not None and time.monotonic() >= self.repaint_at:
            self.repaint_at = None
            self.resize(*self.requested)
        if self.input:
            # Fairness: at most one small PTY write, then serve ordinary RPCs.
            part = bytes(self.input[:4096])
            del self.input[:len(part)]
            if not write(part):
                conn = self.conn
                self.release(conn, "pty-write-error")
                self.disconnect(conn)

    def feed_output(self, data: bytes):
        if not self.attached or not data:
            return
        packet = frame("attach-output", data=base64.b64encode(data).decode("ascii"))
        if len(self.output) + len(packet) > MAX_PENDING_OUTPUT:
            conn = self.conn
            self.release(conn, "output-overflow")
            self.disconnect(conn)
        else:
            self.output.extend(packet)

    def flush(self):
        if not self.attached or not self.output:
            return
        conn = self.conn
        try:
            sent = conn.send(self.output[:32768])
            if not sent:
                raise OSError("attachment connection closed")
            del self.output[:sent]
        except BlockingIOError:
            return
        except OSError:
            self.release(conn, "connection-lost")
            self.disconnect(conn)

    def release(self, conn, reason):
        if self.conn is not conn or conn is None:
            return
        self.conn = None  # exactly-once restore, even when callbacks re-enter
        self.token = None
        self.input.clear()
        self.output.clear()
        self.requested = None
        self.repaint_at = None
        saved, self.saved_size = self.saved_size, None
        if saved is not None:
            try:
                fcntl.ioctl(self.master_fd, termios.TIOCSWINSZ, saved)
            except OSError:
                pass
        self.on_release()
        self.event({"type": "attach_closed", "reason": reason})
        if reason != "client-eof":
            try:
                conn.send(frame("attach-ended", reason=reason))
            except (OSError, BlockingIOError):
                pass


class DetachFilter:
    """Ctrl-Z detaches (instead of suspending); keys inside a paste pass through.

    Paste markers are decoded across fragmented reads, with no examination of
    any characters inside a bracketed paste. A lone ESC can be flushed on timeout.
    """
    def __init__(self):
        self.paste = False
        self.escape = bytearray()
        self.detached = False

    def _key(self, key: int, result: bytearray):
        if key == 0x1a and not self.paste:
            self.detached = True
        else:
            result.append(key)

    def feed(self, chunk: bytes) -> bytes:
        output = bytearray()
        marker = b"\x1b[201~" if self.paste else b"\x1b[200~"
        for key in chunk:
            if self.detached:
                break
            self.escape.append(key)
            while self.escape and not marker.startswith(self.escape):
                self._key(self.escape.pop(0), output)
                if self.detached:
                    break
            if self.escape == marker:
                output.extend(self.escape)
                self.escape.clear()
                self.paste = not self.paste
                marker = b"\x1b[201~" if self.paste else b"\x1b[200~"
        return bytes(output)

    def flush_escape(self):
        result = bytearray()
        while self.escape:
            self._key(self.escape.pop(0), result)
        return bytes(result)


def status_label(reply) -> str | None:
    """One short live state from the existing host `status` reply, or None."""
    if not isinstance(reply, dict) or reply.get("ok") is not True:
        return None
    state = reply.get("state")
    if reply.get("compacting") is True:
        state = "compacting"
    elif state == "waiting":
        waiting = reply.get("waitingFor")
        waiting = waiting if isinstance(waiting, dict) else {}
        tool = waiting.get("tool")
        if waiting.get("type") == "elicitation":
            state = "waiting for input"
        elif tool == "AskUserQuestion":
            state = "waiting for answer"
        elif waiting.get("type") == "permission" and isinstance(tool, str):
            state = f"waiting for permission ({tool})"
    if not isinstance(state, str):
        return None
    return "".join(ch for ch in state if ch.isprintable())[:80] or None


GROUND, ESCAPE, ESCAPE_INTERMEDIATE, CSI, STRING, STRING_ESCAPE = range(6)
_ALT_SCREENS = {b"47", b"1047", b"1049"}


def _utf8_complete(tail: bytes) -> bool:
    for back in range(1, len(tail) + 1):
        byte = tail[-back]
        if byte < 0x80:
            return True
        if byte >= 0xC0:
            return back >= (2 if byte < 0xE0 else 3 if byte < 0xF0 else 4)
    return True  # continuation bytes only: complete (or malformed)


class StatusBar:
    """Reserve the outer terminal's last row for one CCX status line.

    Claude's PTY is one row shorter and DECSTBM keeps outer scrolling above
    the bar. The bar is only drawn where the outer stream is in ground state:
    never inside an escape/string sequence, a synchronized update or a UTF-8
    character. Output that resets the margins or clears/switches the screen
    is followed at once by a redraw. The label comes from a polling thread.
    """
    MIN_ROWS, MIN_COLS = 6, 20

    def __init__(self, short_id: str, fetch=None, *, interval=2.0):
        self.short_id = short_id
        self.fetch = fetch
        self.interval = interval
        self.polled = None  # written only by the poll thread
        self.label = None
        self.rows = self.cols = 0
        self.active = False
        self.pending = False
        self.stale_row = None
        self.margins = False  # outer DECSTBM currently ours
        self.state = GROUND
        self.seq = bytearray()
        self.sync = False
        self.tail = b""
        self._stop = threading.Event()

    # --- status poll (off the select loop) --------------------------------
    def start(self):
        if self.fetch is not None:
            threading.Thread(target=self._poll, name="ccx-attach-status", daemon=True).start()

    def stop(self):
        self._stop.set()

    def _poll(self):
        while not self._stop.is_set():
            try:
                self.polled = status_label(self.fetch())
            except Exception:  # a failed poll only hides the state
                self.polled = None
            self._stop.wait(self.interval)

    def update(self):
        label = self.polled
        if label != self.label:
            self.label = label
            self.pending = self.pending or self.active

    # --- geometry and painting --------------------------------------------
    def resize(self, rows: int, cols: int) -> int:
        """Record the outer size; return the row count Claude's PTY gets."""
        was_active, old_rows = self.active, self.rows
        self.rows, self.cols = rows, cols
        self.active = rows >= self.MIN_ROWS and cols >= self.MIN_COLS
        if was_active and self.active and old_rows < rows:
            self.stale_row = old_rows  # the old bar row is now Claude's
        self.pending = self.pending or self.active or was_active
        return rows - 1 if self.active else rows

    def text(self) -> str:
        parts = ["ccx " + self.short_id] + ([self.label] if self.label else [])
        return (" " + " · ".join(parts + ["Ctrl-Z detach"]))[:self.cols - 1]

    def _paint(self) -> bytes:
        out = bytearray(b"\x1b7")  # DECSC/DECRC: Claude never uses them
        if self.stale_row is not None:
            out += b"\x1b[%d;1H\x1b[0m\x1b[2K" % self.stale_row
            self.stale_row = None
        if self.active:
            out += b"\x1b[1;%dr\x1b[%d;1H\x1b[0m\x1b[2K\x1b[7m" % (self.rows - 1, self.rows)
            out += self.text().ljust(self.cols - 1).encode() + b"\x1b[0m"
        else:
            out += b"\x1b[r"
        self.margins = self.active
        return bytes(out + b"\x1b8")

    def poll(self, *, sequence_end=False) -> bytes:
        """The pending redraw, if the stream is at a safe point now."""
        if (not self.pending or self.state != GROUND or self.sync
                or not (sequence_end or _utf8_complete(self.tail))):
            return b""
        self.pending = False
        return self._paint()

    def restore(self) -> bytes:
        """Exit: abort any partial sequence, reset margins, clear the bar."""
        if not (self.margins or self.active):
            return b""  # never drawn: exactly like --no-status
        out = b"\x18\x1b[r"
        if self.active:
            out += b"\x1b[%d;1H\x1b[0m\x1b[2K" % self.rows
        return out

    # --- output tracking --------------------------------------------------
    def _completed(self) -> bool:
        """Classify the just-finished sequence; True if it breaks the bar."""
        seq = bytes(self.seq)
        self.state = GROUND
        if seq == b"\x1bc":  # RIS
            self.sync = False
            return True
        if not seq.startswith(b"\x1b[") or len(seq) < 3:
            return False
        final, body = seq[-1:], seq[2:-1]
        if (final == b"J" or (final == b"r" and not body.startswith((b"?", b">")))
                or (final == b"p" and body == b"!")):  # ED, DECSTBM, DECSTR
            return True
        if body[:1] == b"?" and final in b"hl":
            modes = set(body[1:].split(b";"))
            if b"2026" in modes:
                self.sync = final == b"h"
            return bool(modes & _ALT_SCREENS)
        return False

    def feed(self, data: bytes) -> bytes:
        """Pass Claude's bytes through, with any redraw at a safe point."""
        out = bytearray()
        start = i = 0
        while i < len(data):
            if self.state == GROUND:
                i = data.find(b"\x1b", i)
                if i < 0:
                    break
            byte = data[i]
            i += 1
            if self.state == STRING:
                if byte == 0x07:
                    self.state = GROUND
                elif byte == 0x1b:
                    self.state = STRING_ESCAPE
                continue
            if self.state == STRING_ESCAPE:
                if byte == 0x5c:
                    self.state = GROUND
                    continue
                self.state = ESCAPE  # ESC aborted the string: new sequence
                self.seq = bytearray(b"\x1b")
            if byte == 0x1b:
                self.state, self.seq = ESCAPE, bytearray(b"\x1b")
                continue
            if byte in (0x18, 0x1a):  # CAN/SUB abort a sequence
                self.state = GROUND
                continue
            if byte < 0x20 or byte == 0x7f:
                continue  # C0 controls execute without ending the sequence
            if len(self.seq) < 64:
                self.seq.append(byte)
            done = False
            if self.state == ESCAPE:
                if byte == 0x5b:
                    self.state = CSI
                elif byte in b"]P_^X":
                    self.state = STRING
                elif byte < 0x30:
                    self.state = ESCAPE_INTERMEDIATE
                else:
                    done = True
            elif self.state == ESCAPE_INTERMEDIATE:
                done = byte >= 0x30
            else:  # CSI
                done = byte >= 0x40
            if not done:
                continue
            if self._completed() and self.active:
                self.pending = True
            if self.pending and not self.sync:  # also: a sync update ended
                out += data[start:i]
                out += self.poll(sequence_end=True)
                start = i
        out += data[start:]
        self.tail = (self.tail + data[-3:])[-3:]
        out += self.poll()
        return bytes(out)


def _read_line(sock, pending: bytearray, *, max_size=16384):
    """Read one reply without eating the following PTY stream's bytes."""
    while b"\n" not in pending:
        if len(pending) > max_size:
            raise ValueError("oversized attachment reply")
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("managed host closed the connection")
        pending.extend(chunk)
    line, _, rest = pending.partition(b"\n")
    if len(line) > max_size:
        raise ValueError("oversized attachment reply")
    pending[:] = rest
    result = json.loads(line)
    if not isinstance(result, dict):
        raise ValueError("invalid attachment reply")
    return result


def open_stream(handle, *, rows, cols, initial_output: bytearray):
    from ccx import constants
    from ccx.store.root import require_ccx_active
    from ccx.rpc.transport import plan_transport, handshake_payload, verify_handshake

    store = require_ccx_active(constants.CCX_ROOT)
    plan = plan_transport(store, target=handle)
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.settimeout(5.0)
        sock.connect(str(plan.final))
        sock.sendall(frame("handshake", identity=handshake_payload(plan)))
        buffered = bytearray()
        hello = _read_line(sock, buffered)
        if not hello.get("ok"):
            raise RuntimeError("unverified managed host: " + str(hello.get("error")))
        verify_handshake(store, plan, received=hello.get("identity"), connected_socket=sock)
        store.revalidate()
        sock.sendall(frame("attach-open", version=VERSION, rows=rows, cols=cols))
        reply = _read_line(sock, buffered)
        if not reply.get("ok"):
            error = str(reply.get("error", "no attach response"))
            if "unknown op" in error or "no attach response" in error:
                error = "host is too old for shared attach; restart the worker"
            raise RuntimeError(error)
        if (reply.get("op") != "attach-opened" or reply.get("version") != VERSION
                or not isinstance(reply.get("token"), str) or len(reply["token"]) != 32):
            raise RuntimeError("host has no compatible shared attach protocol; restart the worker")
        initial_output.extend(buffered)  # complete and partial first-paint frames
        sock.setblocking(False)
        return sock, reply["token"]
    except BaseException:
        sock.close()
        raise


def _tty_size(fd):
    rows, cols, _, _ = struct.unpack("HHHH", fcntl.ioctl(fd, termios.TIOCGWINSZ, b"\0" * 8))
    return dimensions(rows, cols)


def _write_tty(fd, data, *, timeout_s=0.1):
    """Best-effort terminal *cleanup* only; never block termios restoration."""
    deadline = time.monotonic() + timeout_s
    pos = 0
    while pos < len(data):
        if time.monotonic() >= deadline:
            raise TimeoutError("terminal output backpressure")
        try:
            written = os.write(fd, data[pos:])
        except (InterruptedError, BlockingIOError):
            written = 0
        if written > 0:
            pos += written
            continue
        if written < 0:
            raise OSError("terminal output unavailable")
        select.select([], [fd], [], min(0.05, max(0, deadline - time.monotonic())))


def run_terminal(sock, token, *, input_fd=0, output_fd=1, initial_output=b"",
                 status_bar: StatusBar | None = None):
    """Restore terminal and signal dispositions on every exit, including TERM/HUP.

    Without `status_bar` the native PTY output is passed through byte-exact.
    """
    bar = status_bar
    old_attrs = termios.tcgetattr(input_fd)
    old_flags = fcntl.fcntl(output_fd, fcntl.F_GETFL)
    old_handlers = {}
    resized = [False]
    stopping = [False]

    def on_resize(sig, frame_):
        resized[0] = True

    def on_stop(sig, frame_):
        stopping[0] = True

    try:
        for sig, handler in ((signal.SIGWINCH, on_resize),
                             (signal.SIGTERM, on_stop), (signal.SIGHUP, on_stop)):
            old_handlers[sig] = signal.getsignal(sig)
            signal.signal(sig, handler)
        fcntl.fcntl(output_fd, fcntl.F_SETFL, old_flags | os.O_NONBLOCK)
        tty.setraw(input_fd)
        if bar is not None:
            bar.start()
        # Render the native PTY through one nonblocking, bounded local queue.
        # Paused terminal consumers must backpressure the host, not cause a
        # time-based client exit. The finalizer alone uses a bounded tty write.
        tty_output = bytearray(b"\x1b[?1049h\x1b[?2004h\x1b[?25h\x1b[H\x1b[2J")
        pending = bytearray()
        incoming = bytearray(initial_output)
        filter_ = DetachFilter()
        escape_at = None
        ended_reason = None
        ended_at = None
        socket_ended = False
        host_writable = True
        resized[0] = True

        def render_frames(limit=64):
            nonlocal ended_at
            for _ in range(limit):
                newline = incoming.find(b"\n")
                if newline < 0:
                    return None
                if newline > 8192:
                    raise ValueError("oversized attachment output frame")
                msg = json.loads(incoming[:newline])
                if not isinstance(msg, dict):
                    raise ValueError("invalid attachment output frame")
                op = msg.get("op")
                if op == "attach-ended":
                    if ended_at is None:
                        ended_at = time.monotonic()
                    del incoming[:newline + 1]
                    return msg.get("reason") or "ended"
                if op != "attach-output":
                    raise ValueError("unexpected attachment message")
                raw = base64.b64decode(msg["data"], validate=True)
                if len(raw) > MAX_PENDING_OUTPUT:
                    raise ValueError("oversized attachment output frame")
                if len(tty_output) + len(raw) > MAX_PENDING_OUTPUT:
                    # Do not consume this NDJSON frame until the output queue
                    # can accept all its bytes in order. No partial replay.
                    return None
                del incoming[:newline + 1]
                tty_output.extend(raw if bar is None else bar.feed(raw))
            return None

        while not stopping[0]:
            if ended_reason is None:
                found_end = render_frames()
                if found_end is not None:
                    ended_reason = found_end
            if socket_ended and ended_reason is None and b"\n" not in incoming:
                ended_reason = "connection-ended"
            # EOF establishes the end-drain deadline even with complete
            # output frames held behind a full terminal buffer. It must never
            # depend on render_frames() consuming the final NDJSON newline.
            if ended_at is not None and time.monotonic() - ended_at > 3.0:
                return ended_reason or "connection-ended"
            if ended_reason is not None and not tty_output:
                return ended_reason
            if resized[0] and ended_reason is None and not socket_ended and host_writable:
                resized[0] = False
                rows, cols = _tty_size(output_fd)
                if bar is not None:
                    rows = bar.resize(rows, cols)
                pending.extend(frame("attach-resize", token=token, rows=rows, cols=cols))
            if bar is not None:
                bar.update()
                tty_output.extend(bar.poll())

            # The output queue stays capped at 1 MiB. The existing bounded
            # NDJSON input buffer can absorb remaining socket frames while
            # the terminal is full, so EOF just behind the backlog is still
            # observable; once both queues are full, backpressure reaches the
            # host's own bounded output queue. No live-connection timeout.
            readers = []
            if (ended_reason is None and not socket_ended
                    and len(incoming) < MAX_PENDING_OUTPUT + 8192):
                readers.append(sock)
            # Even after the host ends, keep reading the detach escape. All
            # other keystrokes are discarded once no host can receive them.
            if (ended_reason is not None or socket_ended or not host_writable
                    or len(pending) < 65536):
                readers.append(input_fd)
            writers = []
            if tty_output:
                writers.append(output_fd)
            if pending and ended_reason is None and not socket_ended and host_writable:
                writers.append(sock)
            # Buffered complete frames may advance without another host read.
            ready, writable, _ = select.select(
                readers, writers, [],
                0 if (ended_reason is None and b"\n" in incoming
                      and len(tty_output) < MAX_PENDING_OUTPUT - 32768) else .05)
            if output_fd in writable:
                try:
                    written = os.write(output_fd, tty_output[:16384])
                    if not written:
                        raise OSError("terminal output unavailable")
                    del tty_output[:written]
                except (InterruptedError, BlockingIOError):
                    pass
            if sock in writable and ended_reason is None and not socket_ended and host_writable:
                try:
                    sent = sock.send(pending[:16384])
                    if not sent:
                        raise OSError("attachment host no longer receives input")
                    del pending[:sent]
                except BlockingIOError:
                    pass
                except OSError:
                    # A closed host may still have final output already in the
                    # socket receive buffer. Stop writes, not reads: consume
                    # every available output frame through the normal EOF/end
                    # drain, with the existing three-second deadline beginning
                    # at this first terminal host-write failure.
                    host_writable = False
                    pending.clear()
                    if ended_at is None:
                        ended_at = time.monotonic()
            if input_fd in ready:
                try:
                    data = os.read(input_fd, 16384)
                except BlockingIOError:
                    data = None
                if data == b"":
                    break
                if data:
                    output = filter_.feed(data)
                    escape_at = time.monotonic() if filter_.escape else None
                    if ended_reason is None and not socket_ended and host_writable and output:
                        pending.extend(frame("attach-input", token=token,
                                             data=base64.b64encode(output).decode("ascii")))
                    if filter_.detached:
                        # An explicit human detach may discard still-buffered
                        # final paint. Do not force the terminal to resume.
                        if ended_reason is None and not socket_ended and host_writable:
                            try:
                                if pending:
                                    sock.send(pending)
                            except (OSError, BlockingIOError):
                                pass
                        break
            if sock in ready:
                try:
                    data = sock.recv(min(
                        32768, MAX_PENDING_OUTPUT + 8192 - len(incoming)))
                except BlockingIOError:
                    data = None
                except OSError:
                    # A closed/reset host cannot send anything more. Keep any
                    # frames already read in `incoming` for the bounded drain.
                    data = b""
                if data == b"":
                    socket_ended = True
                    if ended_at is None:
                        ended_at = time.monotonic()
                elif data:
                    incoming.extend(data)
                    if len(incoming) > MAX_PENDING_OUTPUT + 8192:
                        raise ValueError("oversized attachment output")
            # A lone Escape must still reach Claude without a following key.
            if filter_.escape and escape_at is not None and time.monotonic() - escape_at >= .2:
                output = filter_.flush_escape()
                escape_at = None
                if ended_reason is None and not socket_ended and host_writable and output:
                    pending.extend(frame("attach-input", token=token,
                                         data=base64.b64encode(output).decode("ascii")))
        return "detached" if filter_.detached else "connection-ended"
    except InterruptedError:
        return "connection-ended"
    finally:
        # Terminal mode restoration is independent of output drain, or the
        # presence of a responsive host. Preserve the original FD flags too.
        if bar is not None:
            bar.stop()
        try:
            sock.send(frame("attach-close", token=token))
        except OSError:
            pass
        sock.close()
        try:
            try:
                _write_tty(output_fd, (b"" if bar is None else bar.restore()) +
                           b"\x1b[?2004l\x1b[?1000l\x1b[?1002l\x1b[?1006l\x1b[?1004l\x1b[?25h\x1b[?1049l",
                           timeout_s=0.1)
            except (OSError, TimeoutError, InterruptedError):
                pass  # terminal reader may be completely stalled or gone
        finally:
            try:
                termios.tcsetattr(input_fd, termios.TCSANOW, old_attrs)
            finally:
                try:
                    fcntl.fcntl(output_fd, fcntl.F_SETFL, old_flags)
                finally:
                    for sig, handler in old_handlers.items():
                        signal.signal(sig, handler)


def attach(handle, *, status=True):
    if not sys.stdin.isatty() or not sys.stdout.isatty():
        raise RuntimeError("attach requires a terminal on both stdin and stdout")
    rows, cols = _tty_size(sys.stdout.fileno())
    bar = None
    if status:
        from ccx.rpc.rpc import _rpc
        bar = StatusBar(handle.rpartition(":")[2][:8],
                        lambda: _rpc(handle, {"op": "status"}, timeout=1.0))
        rows = bar.resize(rows, cols)
    initial_output = bytearray()
    sock, token = open_stream(handle, rows=rows, cols=cols, initial_output=initial_output)
    try:
        return run_terminal(sock, token, initial_output=initial_output, status_bar=bar)
    finally:
        sock.close()  # also covers an early tcgetattr failure before raw mode
