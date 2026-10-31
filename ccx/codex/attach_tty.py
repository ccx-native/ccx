"""Local Codex TUI PTY with the existing CCX terminal status painter.

Codex owns its native input, including /quit, Ctrl-C and Ctrl-D. CCX
filters only the shared Claude detach keys before forwarding to the local
Codex TUI client. Codexd retains the native thread independently.
"""
from __future__ import annotations

import fcntl
import os
import pty
import select
import signal
import struct
import termios
import time
import tty

from ccx.claude.attach import DetachFilter, MAX_PENDING_OUTPUT, StatusBar, _tty_size, _write_tty
from ccx.codex import runtime as codex_runtime

_MAX_INPUT = 64 * 1024


def _codex_status(worker_id: str) -> dict | None:
    """One cheap read of the *existing* worker; never start a daemon or turn."""
    observation = codex_runtime.observe_worker(worker_id, timeout=0.8)
    reply = observation.get("reply") or {}
    if observation.get("presence") != "served" or reply.get("ok") is not True:
        return None
    state = reply.get("state")
    if state == "waiting" and reply.get("pendingApprovals"):
        state = "waiting for approval"
    elif state == "busy":
        # Compact owns the busy state too. Only inspect its existing marker,
        # never call the native provider or acquire an operation lock.
        compact = codex_runtime.daemon_rpc(
            {"op": "compact-observe", "short": worker_id}, timeout=0.8,
        )
        if compact.get("ok") is True and isinstance(compact.get("activeCompact"), dict):
            state = "compacting"
    return {"ok": True, "state": state}


def _spawn_tui(argv: list[str], cwd: str, rows: int, cols: int) -> tuple[int, int]:
    """Start only our own controlling PTY child, with its size set before exec."""
    gate_r, gate_w = os.pipe()
    try:
        pid, master = pty.fork()
    except BaseException:
        os.close(gate_r)
        os.close(gate_w)
        raise
    if pid == 0:
        os.close(gate_w)
        try:
            if os.read(gate_r, 1) != b"1":
                os._exit(125)
            os.close(gate_r)
            os.chdir(cwd)
            os.execvp(argv[0], argv)
        except BaseException as error:
            os.write(2, f"ccx attach: cannot start Codex: {error}\n".encode()[:512])
            os._exit(127)
    os.close(gate_r)
    try:
        fcntl.ioctl(master, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))
        os.write(gate_w, b"1")
        return pid, master
    except BaseException:
        try:
            os.killpg(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        os.waitpid(pid, 0)
        os.close(master)
        raise
    finally:
        os.close(gate_w)


def _reap_owned_tui(pid: int) -> None:
    """Detach only the local TUI group, never the codexd-owned worker.

    SIGHUP lets the Codex remote client exit cleanly; escalation is bounded
    even when the client or its launcher ignores hangup/termination.
    """
    try:
        if os.getpgid(pid) != pid:
            return  # do not signal a group that cannot be proven ours
    except ProcessLookupError:
        return
    for sig in (signal.SIGHUP, signal.SIGTERM):
        try:
            os.killpg(pid, sig)
        except ProcessLookupError:
            pass
        deadline = time.monotonic() + .6
        while time.monotonic() < deadline:
            try:
                exited, _ = os.waitpid(pid, os.WNOHANG)
                if exited:
                    return
            except ChildProcessError:
                return
            time.sleep(.03)
    try:
        os.killpg(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    try:
        os.waitpid(pid, 0)
    except ChildProcessError:
        pass


def run_status_tui(
    argv: list[str], *, cwd: str, worker_id: str, input_fd: int = 0,
    output_fd: int = 1, status_fetch=None, status=True,
) -> int:
    """Proxy Codex through a PTY, with optional CCX status bar and detach keys."""
    bar = (StatusBar(
        worker_id[:8],
        fetch=status_fetch if status_fetch is not None
        else lambda: _codex_status(worker_id),
    ) if status else None)
    old_attrs = termios.tcgetattr(input_fd)
    old_flags = fcntl.fcntl(output_fd, fcntl.F_GETFL)
    rows, cols = _tty_size(output_fd)
    pid, master = _spawn_tui(argv, cwd, bar.resize(rows, cols) if bar else rows, cols)
    os.set_blocking(master, False)
    handlers = {}
    resized = [False]
    stopped = [None]
    child_exit = None
    child_exited_at = None

    def on_resize(_sig, _frame):
        resized[0] = True

    def on_stop(sig, _frame):
        stopped[0] = sig

    try:
        for sig, fn in ((signal.SIGWINCH, on_resize),
                        (signal.SIGTERM, on_stop), (signal.SIGHUP, on_stop)):
            handlers[sig] = signal.getsignal(sig)
            signal.signal(sig, fn)
        fcntl.fcntl(output_fd, fcntl.F_SETFL, old_flags | os.O_NONBLOCK)
        tty.setraw(input_fd)
        if bar is not None:
            bar.start()
        output = bytearray()
        pending_input = bytearray()
        filter_ = DetachFilter()
        escape_at = None
        master_ended = False

        while stopped[0] is None:
            if resized[0]:
                resized[0] = False
                rows, cols = _tty_size(output_fd)
                child_rows = bar.resize(rows, cols) if bar else rows
                fcntl.ioctl(master, termios.TIOCSWINSZ,
                            struct.pack("HHHH", child_rows, cols, 0, 0))
            if bar is not None:
                bar.update()
                output.extend(bar.poll())
            if child_exit is None:
                exited, status = os.waitpid(pid, os.WNOHANG)
                if exited:
                    child_exit = os.waitstatus_to_exitcode(status)
                    child_exited_at = time.monotonic()
            if child_exit is not None and master_ended and not output:
                return child_exit
            # A stopped terminal reader must never pin termios restoration
            # after the native child has exited.
            if child_exited_at is not None and time.monotonic() - child_exited_at > 2:
                return child_exit

            readers = []
            if not master_ended and len(output) < MAX_PENDING_OUTPUT - 32768:
                readers.append(master)
            if child_exit is None and len(pending_input) < _MAX_INPUT:
                readers.append(input_fd)
            writers = []
            if output:
                writers.append(output_fd)
            if pending_input and not master_ended:
                writers.append(master)
            try:
                ready, writable, _ = select.select(readers, writers, [], .05)
            except InterruptedError:
                continue
            if output_fd in writable:
                try:
                    n = os.write(output_fd, output[:16384])
                    if not n:
                        raise OSError("terminal output unavailable")
                    del output[:n]
                except (BlockingIOError, InterruptedError):
                    pass
            if master in writable:
                try:
                    n = os.write(master, pending_input[:16384])
                    if not n:
                        raise OSError("Codex PTY no longer receives input")
                    del pending_input[:n]
                except (BlockingIOError, InterruptedError):
                    pass
                except OSError:
                    pending_input.clear()
                    master_ended = True
            if input_fd in ready:
                data = os.read(input_fd, 16384)
                if not data:
                    break
                filtered = filter_.feed(data)
                if filter_.detached:
                    # CCX detaches only its local remote TUI; the codexd
                    # subscription and any running turn keep their lifecycle.
                    return 0
                pending_input.extend(filtered)
                escape_at = time.monotonic() if filter_.escape else None
            if (filter_.escape and escape_at is not None
                    and time.monotonic() - escape_at >= .2):
                # Same lone-ESC flush rule as shared Claude attach.
                pending_input.extend(filter_.flush_escape())
                escape_at = None
            if master in ready:
                try:
                    data = os.read(master, 16384)
                except (BlockingIOError, InterruptedError):
                    data = None
                except OSError:  # EIO is normal PTY EOF on Linux.
                    data = b""
                if data == b"":
                    master_ended = True
                elif data:
                    output.extend(bar.feed(data) if bar is not None else data)

        return 128 + stopped[0] if stopped[0] is not None else 1
    finally:
        if bar is not None:
            bar.stop()
        if child_exit is None:
            _reap_owned_tui(pid)
        os.close(master)
        try:
            try:
                _write_tty(
                    output_fd, (bar.restore() if bar is not None else b"") +
                    b"\x1b[?2004l\x1b[?1000l\x1b[?1002l\x1b[?1006l"
                    b"\x1b[?1004l\x1b[?25h\x1b[?1049l",
                    timeout_s=0.1,
                )
            except (OSError, TimeoutError, InterruptedError):
                pass
        finally:
            try:
                termios.tcsetattr(input_fd, termios.TCSANOW, old_attrs)
            finally:
                try:
                    fcntl.fcntl(output_fd, fcntl.F_SETFL, old_flags)
                finally:
                    for sig, fn in handlers.items():
                        signal.signal(sig, fn)
