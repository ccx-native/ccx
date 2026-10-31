"""Confirmed Claude kill against hosts that cannot answer their socket."""

from __future__ import annotations

import signal
import socket
import subprocess
import sys
import threading
import time

import pytest

from ccx import constants
from ccx.ops import terminate
from ccx.store.liveness import owned_pid_alive, proc_start_token
from ccx.store.root import require_ccx_active
from ccx.rpc.transport import ensure_transport_parent, plan_transport
from test_generation_store import A, add_worker, short_root

_EXIT_ON_TERM = (
    "import signal, sys, time\n"
    "signal.signal(signal.SIGTERM, lambda *a: sys.exit(0))\n"
    "time.sleep(600)\n"
)


def _fake_host():
    """A host process that exits cleanly on SIGTERM, reaped promptly so the
    pid disappears like an init-parented host's would."""
    proc = subprocess.Popen([sys.executable, "-c", _EXIT_ON_TERM])
    threading.Thread(target=proc.wait, daemon=True).start()
    return proc


@pytest.mark.parametrize("sock_kind", ["wedged", "stale"])
def test_kill_sigterms_a_host_that_cannot_answer(sock_kind, short_root, monkeypatch):
    """Only the exact original PID+start owner can be terminated on RPC failure."""
    add_worker(short_root, A, "claude")
    store = require_ccx_active(short_root)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    host = _fake_host()
    start_token = proc_start_token(host.pid)
    assert start_token and owned_pid_alive(host.pid, start_token)
    store.mutate_record(A, patches={
        "lifecycle.state": "idle",
        "lifecycle.hostPid": host.pid,
        "lifecycle.hostStart": start_token,
        "lifecycle.workerPid": None,
        "lifecycle.workerStart": None,
    })
    original_native = store.record(A)["native"]
    plan = plan_transport(store, target=f"claude:{A}")
    ensure_transport_parent(store, plan)
    path = plan.final
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(path))
    if sock_kind == "wedged":
        server.listen(1)  # accepts into the backlog, never replies
    else:
        server.close()    # stale socket file: connection refused
    started = time.monotonic()
    try:
        result = terminate.confirmed_kill_uuid(store, f"claude:{A}", timeout=8)
    finally:
        server.close()
        if host.poll() is None:
            host.kill()
        host.wait(timeout=2)
    assert result["short"] == A[:8]
    assert store.record(A)["workerId"] == A
    assert result["killed"] is True
    assert store.record(A)["lifecycle"]["state"] == "dead"
    assert store.record(A)["native"] == original_native
    assert not owned_pid_alive(host.pid, start_token)
    # Exit code 0 (its handler ran) or -SIGTERM (it ran before the handler was
    # installed) — anything but the SIGKILL escalation.
    assert host.returncode in (0, -signal.SIGTERM), host.returncode
    assert time.monotonic() - started < 10
