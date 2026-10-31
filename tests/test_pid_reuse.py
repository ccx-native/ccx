"""A recorded host/worker pid that now names an unrelated process.

After an unclean host death (or a reboot) the pid in the record can be reused.
The /proc start token recorded beside it is what tells CCX the process is not
its own: the worker must read as revivable and nothing may be signalled.
"""

from __future__ import annotations

import subprocess

import pytest

from ccx.store.liveness import lifecycle_host_alive, owned_pid_alive, owned_process, proc_start_token


@pytest.fixture
def stranger():
    proc = subprocess.Popen(["sleep", "600"])
    yield proc
    proc.kill()
    proc.wait()


def test_owned_pid_alive_matches_the_start_token(stranger):
    token = proc_start_token(stranger.pid)
    assert token is not None
    assert owned_pid_alive(stranger.pid, token)
    assert not owned_pid_alive(stranger.pid, token + "-different-original")
    # D1: a live bare PID is not an owned process, even when kill(0) succeeds.
    assert not owned_pid_alive(stranger.pid)
    assert owned_process(stranger.pid, None) == "unprovable"
    assert lifecycle_host_alive({"hostPid": stranger.pid, "hostStart": token})
    assert owned_process(stranger.pid, token + "-different-original") == "dead"


@pytest.mark.parametrize("probe,expected", [
    ("missing", "dead"),
    ("exists", "unprovable"),
    ("denied", "unprovable"),
])
def test_tokenless_pid_is_dead_only_on_kernel_confirmed_absence(monkeypatch, probe, expected):
    from ccx.system import oslayer

    monkeypatch.setattr(oslayer, "probe_pid", lambda pid: probe)
    monkeypatch.setattr(oslayer, "process_info", lambda pid: pytest.fail(
        "bare PID cannot prove a matching start token"))
    assert owned_process(12345678, None) == expected
    assert not owned_pid_alive(12345678)
