from __future__ import annotations
import fcntl
import os
import re
import select
import struct
import termios
import time
from ccx.constants import (
    CLEAR_KILL_REPEAT,
    POST_CLEAR_SETTLE_S,
    PASTE_INLINE_MAX_NEWLINES,
    PASTE_INLINE_MAX_UNITS,
    PASTE_INLINE_ROWS_RESERVED,
    PASTE_SUBMIT_DELAY_S,
    PASTE_STAGED_BYTES_PER_S,
    PASTE_STAGED_MAX_DELAY_S,
    PASTE_STAGED_THRESHOLD_BYTES,
    PTY_QUIESCENCE_S,
    PTY_WRITE_TIMEOUT_S,
)


def set_winsize(fd, rows, cols):
    fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))


def paste_bytes(text: str) -> bytes:
    """Frame text as one bracketed paste, without the submitting Enter.

    Claude's decoder emits the pasted text only after the closing marker. The
    caller sends Enter separately after this complete payload has drained.
    """
    return b"\x1b[200~" + text.encode() + b"\x1b[201~"


def paste_submit_delay(text: str) -> float:
    """How long Ink gets to materialize a paste before CCX presses Enter.

    Small prompts remain effectively immediate. Large prompts are staged: Ink
    asynchronously turns them into a collapsed ``[Pasted text …]`` attachment,
    and an Enter delivered before that work finishes is silently discarded.
    The caller must wait without blocking the host loop so PTY output continues
    to drain during this interval.
    """
    size = len(text.encode())
    if size <= PASTE_STAGED_THRESHOLD_BYTES:
        return PASTE_SUBMIT_DELAY_S
    return min(PASTE_STAGED_MAX_DELAY_S, size / PASTE_STAGED_BYTES_PER_S)


def get_winsize_rows(fd) -> int:
    """The PTY's current row count (viewers may resize it at any time)."""
    rows, _cols, _x, _y = struct.unpack(
        "HHHH", fcntl.ioctl(fd, termios.TIOCGWINSZ, b"\0" * 8))
    return rows


def inline_paste_newline_cap(rows: int) -> int:
    """Line breaks one paste may carry and still be inserted as typed text."""
    return max(0, min(rows - PASTE_INLINE_ROWS_RESERVED, PASTE_INLINE_MAX_NEWLINES))


def paste_chunks(text: str, max_newlines: int,
                 max_units: int = PASTE_INLINE_MAX_UNITS) -> list[str] | None:
    """Split text into pastes small enough for Claude to insert as typed text.

    Sizes are measured the way Claude's paste handler measures them: UTF-16
    units after it normalizes CRLF/CR to LF and expands tabs to four spaces.
    A CRLF pair is never split. Returns None when the text has a line break but
    no paste may carry one (a PTY under ``PASTE_INLINE_ROWS_RESERVED + 1``
    rows), so the caller falls back to a single paste.
    """
    chunks: list[str] = []
    cur: list[str] = []
    units = newlines = 0
    i = 0
    while i < len(text):
        ch = text[i]
        if ch == "\r" and text[i + 1:i + 2] == "\n":
            ch = "\r\n"
        is_newline = ch in ("\n", "\r", "\r\n")
        if is_newline and max_newlines < 1:
            return None
        cost = 4 if ch == "\t" else 2 if ord(ch[0]) > 0xFFFF else 1
        if cur and units + cost > max_units:
            chunks.append("".join(cur))
            cur, units, newlines = [], 0, 0
        cur.append(ch)
        units += cost
        i += len(ch)
        if is_newline:
            newlines += 1
            if newlines >= max_newlines:
                chunks.append("".join(cur))
                cur, units, newlines = [], 0, 0
    if cur:
        chunks.append("".join(cur))
    return chunks


def prompt_paste(text: str, rows: int) -> tuple[bytes, float, int]:
    """Frame a prompt for Claude's editor: ``(payload, submit_delay, pastes)``.

    Normally a run of small pastes (see ``paste_chunks``) that Claude inserts
    verbatim, so the submitted prompt reaches the model as the user's own
    words rather than wrapped in <pasted_content> tags, and Enter can follow
    immediately. A PTY too short for inline line breaks falls back to one
    paste with the size-aware staged submit.
    """
    chunks = paste_chunks(text, inline_paste_newline_cap(rows))
    if chunks is None:
        return paste_bytes(text), paste_submit_delay(text), 1
    chunks = chunks or [text]
    return b"".join(paste_bytes(c) for c in chunks), PASTE_SUBMIT_DELAY_S, len(chunks)


def clear_input_bytes() -> bytes:
    """Ctrl-C (Claude's app:interrupt). At an idle prompt it empties the whole
    editor however many lines it holds, where a Ctrl-U burst only removes one
    wrapped visual line per press. On an empty editor it only arms Claude's
    exit confirmation, so a second press within 800ms would exit the worker:
    the host spaces these presses at least ``CLEAR_INPUT_MIN_GAP_S`` apart."""
    return b"\x03"


def stalled_paste_clear_action(slot: dict, now: float, last_pty: float) -> str | None:
    """Only reject a partial PTY write after an explicit quiet editor clear."""
    if not last_pty or now - last_pty < PTY_QUIESCENCE_S:
        return None
    unstuck = slot.get("stalled_unstuck_at")
    if unstuck is None:
        return "unstick"
    if now - unstuck < POST_CLEAR_SETTLE_S:
        return None
    cleared = slot.get("stalled_cleared_at")
    if cleared is None:
        return "clear"
    if now - cleared < POST_CLEAR_SETTLE_S:
        return None
    return "reject"


def write_all(fd: int, payload: bytes, *, timeout_s: float = PTY_WRITE_TIMEOUT_S,
              absolute_timeout_s: float | None = None,
              on_partial=None, on_read=None) -> bool:
    """Drain short PTY writes, reading peer output to avoid bidirectional stalls.

    The timeout bounds *lack of write progress*, not total payload time. A busy
    Ink renderer can fill the opposite PTY buffer, stop reading input, and
    deadlock a write-only host. ``on_read`` drains that output through the same
    path as the host loop; it returns False on EOF. PTY write boundaries do not
    change the logical bracketed pastes. ``absolute_timeout_s`` is an
    optional non-renewable elapsed-time budget used only by attached-terminal
    input; normal semantic writes preserve their progress-based timeout.
    """
    start = time.monotonic()
    deadline = start + timeout_s
    absolute_deadline = (start + absolute_timeout_s
                         if absolute_timeout_s is not None else None)
    sent = 0
    while sent < len(payload):
        if absolute_deadline is not None and time.monotonic() >= absolute_deadline:
            return False
        try:
            written = os.write(fd, payload[sent:])
        except InterruptedError:
            continue
        except BlockingIOError:
            written = 0
        except OSError:
            return False

        if written > 0:
            sent += written
            deadline = time.monotonic() + timeout_s
            if sent < len(payload) and on_partial is not None:
                on_partial(written, sent, len(payload))
            continue

        remaining = deadline - time.monotonic()
        if absolute_deadline is not None:
            remaining = min(remaining, absolute_deadline - time.monotonic())
        if remaining <= 0:
            return False
        try:
            readable, writable, _ = select.select(
                [fd] if on_read is not None else [], [fd], [], remaining
            )
        except InterruptedError:
            continue
        except OSError:
            return False
        if readable and on_read() is False:
            return False
        if not readable and not writable:
            return False
    return True


_ANSI_RE = re.compile(
    rb"\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)"
    rb"|\x1b\[[0-9;?]*[ -/]*[@-~]"
    rb"|\x1b[@-Z\\-_]"
)


def pty_tail_reason(buf: bytes | bytearray, *, max_chars: int = 300) -> str | None:
    """Distill a dead worker's last PTY output into a one-line failure reason.

    A worker that dies at boot (an unknown `--agent`, a rejected `--model`, an
    onboarding/trust gate) prints why on the PTY and exits. Nothing else records
    that: the host log holds only the launcher's stdio, so without this the
    record kept an opaque state=crashed with lastError=None and the operator had
    no way to tell a typo'd flag from a real crash.

    Returns None when there is nothing quotable. Box-drawing and spinner residue
    from a half-rendered TUI is dropped — a line must carry alphanumeric text to
    count as a message — and consecutive duplicate lines (Ink re-renders the same
    frame) collapse to one.
    """
    text = _ANSI_RE.sub(b"", bytes(buf)).decode("utf-8", "replace")
    lines: list[str] = []
    for raw in text.replace("\r", "\n").split("\n"):
        line = "".join(c for c in raw if c.isprintable()).strip()
        if not line or not any(c.isalnum() for c in line):
            continue
        if not lines or lines[-1] != line:
            lines.append(line)
    if not lines:
        return None
    return " ".join(lines[-3:])[:max_chars]


def unstick_bytes() -> bytes:
    """A lone closing bracketed-paste marker (\\x1b[201~). If a lost first paste
    left Claude's decoder stuck IN_PASTE (its closing marker never arrived),
    control keys are swallowed into the paste buffer and ignored; this marker
    forces the decoder back to normal and materializes whatever partial it held
    into the visible editor, where a kill key can then remove it."""
    return b"\x1b[201~"


def drain_bytes(kill_repeat: int = CLEAR_KILL_REPEAT) -> bytes:
    """A burst of Ctrl-U (\\x15), used for the single-line ``/compact``
    residue. Ctrl-U is bound to delete-to-line-start, not whole-buffer clear:
    each press removes one wrapped visual line, so it cannot empty a long
    multi-line prompt (see clear_input_bytes()). No-op once the buffer is
    empty."""
    return b"\x15" * kill_repeat
