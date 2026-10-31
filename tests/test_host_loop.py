"""Three host-loop controls: original permission FIFO, bounded relay and PTY reap.

Use the existing real full-UUID fake-Claude host; no provider login or daemon.
"""
from __future__ import annotations
import ccx.claude.host.hooks as H_hooks

import os
import signal

import pytest

from ccx import WorkerClient
from ccx.store.liveness import owned_pid_alive
from ccx.store.paths import sock_path
from ccx.rpc.rpc import _rpc
from ccx.store.root import require_ccx_active
from test_generation_store import A, short_root as short_root
from test_create_cross_surface import (
    _deadline_fake_claude, _first_wait_original_claude_ready,
)
from test_claude_native_cli import _stop_disposable_original


def test_concurrent_permission_requests_queue_fifo(short_root, tmp_path, monkeypatch):
    """An exact decision never approves a different published/queued permission."""
    _deadline_fake_claude(short_root, tmp_path, monkeypatch)
    store = require_ccx_active(short_root)
    try:
        assert _first_wait_original_claude_ready(tmp_path, A).worker_id == A

        def call(op, **fields):
            return _rpc(f"claude:{A}", {"op": op, **fields}, timeout=5)

        def request(command):
            result = call("permission-request", data={
                "tool_name": "Bash", "tool_input": {"command": command},
            })
            assert result["ok"], result
            return result["request_id"]

        def poll(request_id):
            result = call("permission-poll", request_id=request_id)
            assert result["ok"], result
            return result

        first, second = request("first"), request("second")
        status = call("status")
        assert status["ok"] and status["state"] == "waiting"
        assert status["waitingFor"]["id"] == first
        assert status["queuedInputs"] == 1
        assert store.record(A)["waitingFor"]["id"] == first

        wrong = call("permission-decision", request_id=second, decision="allow")
        assert wrong["ok"] is False and "request id mismatch" in wrong["error"]
        assert poll(first)["decision"] is None
        assert poll(second)["decision"] is None
        status = call("status")
        assert status["waitingFor"]["id"] == first and status["queuedInputs"] == 1

        allowed = call("permission-decision", request_id=first, decision="allow")
        assert allowed["ok"] and allowed["request_id"] == first
        assert poll(first)["decision"]["decision"] == "allow"
        status = call("status")
        assert status["state"] == "waiting"
        assert status["waitingFor"]["id"] == second and status["queuedInputs"] == 0
        assert store.record(A)["waitingFor"]["id"] == second
        assert poll(second)["decision"] is None

        third = request("third")
        assert call("status")["queuedInputs"] == 1
        timed_out = call("permission-timeout", request_id=third)
        assert timed_out["ok"], timed_out
        assert poll(third)["decision"]["decision"] == "deny"
        assert poll(second)["decision"] is None
        status = call("status")
        assert status["waitingFor"]["id"] == second and status["queuedInputs"] == 0

        orphan = poll("permission-from-previous-host")
        assert orphan["decision"] is None and orphan["unknown"] is True
        denied = call("permission-decision", request_id=second, decision="deny")
        assert denied["ok"] and denied["request_id"] == second
        assert poll(second)["decision"]["decision"] == "deny"
        status = call("status")
        assert status["state"] == "busy" and status["waitingFor"] is None
        assert status["queuedInputs"] == 0
        assert "waitingFor" not in store.record(A)
    finally:
        _stop_disposable_original(short_root)


def test_relay_connect_retries_a_full_backlog_only_within_budget(socket_tmp_path, monkeypatch):
    import errno
    import socket

    from ccx.claude.host import session as H

    path = socket_tmp_path / "s.sock"
    monkeypatch.setattr(H_hooks, "sock_path", lambda short: path)
    # A backlog of exactly zero admits different queue depths on Linux and
    # Darwin. Inject the documented kernel EAGAIN at connect(), not a fixed
    # number of fillers; retain the real AF_UNIX socket on successful retry.
    attempts = []
    base_socket = socket.socket

    class BusySocket(base_socket):
        def connect(self, destination):
            attempts.append(destination)
            if len(attempts) <= 3:
                raise BlockingIOError(errno.EAGAIN, "synthetic full backlog")
            return super().connect(destination)

    with base_socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
        server.bind(str(path))
        server.listen(1)
        monkeypatch.setattr(H.socket, "socket", BusySocket)
        with pytest.raises(BlockingIOError, match="full backlog"):
            H_hooks._relay_connect("x", timeout=0.2, retry_s=0.0).close()
        with H_hooks._relay_connect("x", timeout=0.2, retry_s=5.0):
            assert len(attempts) == 4
    from ccx.constants import HOOK_RELAY_RETRY_S
    assert H_hooks.hook_relay_retry_s({}) == HOOK_RELAY_RETRY_S
    assert H_hooks.hook_relay_retry_s({"agent_id": "a1"}) == 0.0


def test_shutdown_sigkills_a_worker_that_ignores_sigterm(short_root, tmp_path, monkeypatch):
    """The original native transcript writer is reaped even after ignoring TERM."""
    _deadline_fake_claude(short_root, tmp_path, monkeypatch)
    monkeypatch.setenv("FAKE_MODE", "ignore_term")
    store = require_ccx_active(short_root)
    owned = []
    try:
        assert _first_wait_original_claude_ready(tmp_path, A).worker_id == A
        original = store.record(A)
        native_session = original["native"]["sessionId"]
        lifecycle = original["lifecycle"]
        host = (lifecycle["hostPid"], lifecycle["hostStart"])
        child = (lifecycle["workerPid"], lifecycle["workerStart"])
        owned.extend((host, child))
        assert owned_pid_alive(*host) and owned_pid_alive(*child)

        result = WorkerClient().kill(f"claude:{A}", timeout=20)
        assert result["killed"] is True and result["worker"] == f"claude:{A}"
        final = store.record(A)
        assert final["lifecycle"]["state"] == "dead"
        assert final["native"]["sessionId"] == native_session
        assert not owned_pid_alive(*host)
        assert not owned_pid_alive(*child)
        assert not sock_path(A).exists()
    finally:
        _stop_disposable_original(short_root)
        # A terminal record can clear its PIDs before cleanup inspects it.
        # Only the original disposable PID+start pairs may be signalled.
        for pid, start in owned:
            if owned_pid_alive(pid, start):
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
