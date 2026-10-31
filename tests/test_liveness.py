'Process liveness + session-identity parsing (ccx.store.liveness).\n\nThese read the running process table (os.kill / /proc) but spawn nothing and\ndepend on no Claude session. The /proc-backed checks are Linux-only and skip\nelsewhere.\n'

from __future__ import annotations

import os
import sys

import pytest

from ccx.store import liveness as L

_IS_LINUX = sys.platform.startswith("linux")


# --- pid_alive ---------------------------------------------------------------

def test_pid_alive_none_and_zero_are_dead():
    assert L.pid_alive(None) is False
    assert L.pid_alive(0) is False


def test_pid_alive_self_is_alive():
    assert L.pid_alive(os.getpid()) is True


def test_pid_alive_garbage_pid_is_dead():
    # An absurd pid overflows the syscall -> treated as dead, not an exception.
    assert L.pid_alive(2 ** 60) is False


def test_pid_alive_unused_pid_is_dead():
    # Very high but in-range pid that is almost certainly not running.
    assert L.pid_alive(4_000_000_000) is False


# --- proc_argv ---------------------------------------------------------------

@pytest.mark.skipif(not _IS_LINUX, reason="proc_argv reads /proc (Linux only)")
def test_proc_argv_self_returns_argv():
    argv = L.proc_argv(os.getpid())
    assert isinstance(argv, list) and argv
    assert any("python" in part.lower() for part in argv)


def test_proc_argv_none_pid():
    assert L.proc_argv(None) is None


@pytest.mark.skipif(not _IS_LINUX, reason="proc_argv reads /proc (Linux only)")
def test_proc_argv_dead_pid_is_none():
    assert L.proc_argv(4_000_000_000) is None


# --- session_pid_alive -------------------------------------------------------

def test_session_pid_alive_dead_pid_is_false():
    assert L.session_pid_alive(None) is False
    assert L.session_pid_alive(4_000_000_000, "abcd1234-0000") is False


def test_session_pid_alive_matches_session_id_in_argv(monkeypatch):
    # Gate 1: a session short embedded in the live process's argv is definitive.
    # Use a controlled argv (as `--resume <short>` would produce) instead of the
    # pytest process's own command line — the latter varies by launcher and made
    # this check pass or fail depending on where space boundaries fell.
    session_id = "abcd1234-5678-90ab-cdef-1234567890ab"
    short = session_id[:8]
    assert len(short) == 8
    monkeypatch.setattr(L, "proc_argv", lambda pid: ["node", "claude", "--resume", short])
    # Real, live pid so the pid_alive gate passes; argv is the controlled stub.
    assert L.session_pid_alive(os.getpid(), session_id) is True


@pytest.mark.skipif(not _IS_LINUX, reason="reads /proc (Linux only)")
def test_session_pid_alive_non_claude_self_without_id_is_false():
    # Gate 2 fails: our argv0 is python, not the claude binary, and no id given.
    assert L.session_pid_alive(os.getpid(), None) is False


def test_recorded_host_loaded_proves_claude_host_and_leaves_codex_unobserved():
    me = {"hostPid": os.getpid(), "hostStart": L.proc_start_token(os.getpid())}
    assert L.recorded_host_loaded("claude", me) is True
    assert L.recorded_host_loaded("claude", {**me, "hostStart": "1"}) is False
    assert L.recorded_host_loaded("claude", {**me, "hostStart": None}) is None
    assert L.recorded_host_loaded("claude", {"hostPid": None}) is None
    assert L.recorded_host_loaded("claude", None) is None
    assert L.recorded_host_loaded("codex", me) is None
