"""Linux OS facts: kernel parser goldens, ownership and fail-closed boundaries.

The production callers are routed through oslayer; these tests exercise the
kernel-facing parser, original PID/start, credential and path assumptions.
"""

from __future__ import annotations

import errno
import os
import socket
import struct
import sys

import pytest

from ccx.system import oslayer

_LINUX_ONLY = pytest.mark.skipif(
    sys.platform != "linux",
    reason="Linux-specific kernel ABI golden; Darwin has separate OS-fact coverage",
)


def _stat(state="S", start="999", comm="(worker) with ) parens)"):
    return (f"123 {comm} {state} 42 " + " ".join(["0"] * 17 + [start, "0"])).encode()


@_LINUX_ONLY
def test_stat_goldens_and_unreadable_process(monkeypatch):
    # The comm field can contain parentheses, spaces and non-UTF-8 bytes.
    # Kernel start ticks (field 22) remain the original decimal string.
    raw = _stat(start="000987654321").replace(b"(worker)", b"(\xff worker)")
    assert oslayer._linux_parse_stat(123, raw) == oslayer.ProcessInfo(
        123, "alive", 42, "S", "000987654321")
    for code in ("Z", "X"):
        item = oslayer._linux_parse_stat(123, _stat(state=code))
        assert item.presence == "dead" and item.start_token is None and item.state == code
    assert oslayer._linux_parse_stat(123, b"123 (broken) Z 42").presence == "dead"
    assert oslayer._linux_parse_stat(123, b"bad stat").presence == "unprovable"
    class Denied:
        def __init__(self, path):
            pass

        def read_bytes(self):
            raise PermissionError(errno.EACCES, "denied")
    monkeypatch.setattr(oslayer, "Path", Denied)
    assert oslayer.process_info(123).presence == "unprovable"
    assert oslayer.process_info(None).presence == "dead"


@pytest.mark.parametrize("error,expected", [
    (None, "exists"), (ProcessLookupError(errno.ESRCH, "gone"), "missing"),
    (PermissionError(errno.EPERM, "denied"), "denied"), (OverflowError(), "missing"),
])
def test_kill_zero_kernel_probe_states(monkeypatch, error, expected):
    def probe(pid, sig):
        assert (pid, sig) == (123, 0)
        if error is not None:
            raise error
    monkeypatch.setattr(oslayer.os, "kill", probe)
    assert oslayer.probe_pid(123) == expected


@_LINUX_ONLY
def test_unix_socket_byte_budget_including_nul_and_unicode():
    assert oslayer.UNIX_SUN_PATH_BYTES == 108
    for value, fits in (("x"*107, True), ("x"*108, False),
                        ("é"*53, True), ("é"*54, False)):
        assert oslayer.fits_unix_path(value) is fits


@_LINUX_ONLY
def test_same_connection_kernel_peer_credentials_and_failure():
    # The accepted-socket same-user path is covered by private_state.
    # Here, incomplete kernel credentials must fail instead of falling back
    # to the socket mode or assuming a same-UID peer.
    class Truncated:
        def getsockopt(self, *args):
            return b"\0" * 8

    with pytest.raises(oslayer.PeerIdentityUnavailable):
        oslayer.peer_credentials(Truncated())
    class Packed:
        def getsockopt(self, level, option, length):
            assert (level, option, length) == (socket.SOL_SOCKET, socket.SO_PEERCRED, 12)
            return struct.pack("3i", 456, 789, 123)
    assert oslayer.peer_credentials(Packed()) == oslayer.PeerCredentials(789, 456, 123)


def test_root_traversal_is_pinned_nofollow_and_closes_fd(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    with oslayer.open_root_dir(root) as (absolute, fd):
        assert absolute == root and os.fstat(fd).st_ino == root.stat().st_ino
    with pytest.raises(OSError):
        os.fstat(fd)
    link = tmp_path / "linked"
    link.symlink_to(root, target_is_directory=True)
    with pytest.raises(oslayer.RootOpenError):
        with oslayer.open_root_dir(link):
            pytest.fail("followed symlink")
    with pytest.raises(OSError, match="disk full"):
        with oslayer.open_root_dir(root):
            raise OSError(errno.ENOSPC, "disk full")


@pytest.mark.parametrize("failure", ("none", "missing", "oserror", "nonzero", "parent-changed"))
@_LINUX_ONLY
def test_parent_death_arming_is_fail_closed(monkeypatch, failure):
    class Exited(BaseException):
        pass

    calls = []

    class Libc:
        def prctl(self, *args):
            calls.append(args)
            if failure == "oserror":
                raise OSError("prctl refused")
            return -1 if failure == "nonzero" else 0

    monkeypatch.setattr(oslayer.ctypes, "CDLL", lambda *a, **k:
                        object() if failure == "missing" else Libc())
    monkeypatch.setattr(oslayer.os, "getppid", lambda: 999 if failure == "parent-changed" else 123)
    def stop(code):
        assert code == 0
        raise Exited()
    monkeypatch.setattr(oslayer.os, "_exit", stop)
    if failure == "none":
        oslayer.arm_parent_death(123)
    else:
        with pytest.raises(Exited):
            oslayer.arm_parent_death(123)
    if failure != "missing":
        assert calls == [(1, oslayer.signal.SIGKILL, 0, 0, 0)]
