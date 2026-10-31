"""Delayed confirmed kill cannot abandon a concurrently revived child's steer."""
from __future__ import annotations

import fcntl
import time

import pytest

from ccx.claude import delivery as claude_delivery
from ccx.store import liveness
from ccx.rpc import rpc
from ccx.ops import steer_ops
from ccx.ops import terminate
from ccx.store.paths import respawn_lock_path
from test_generation_store import A, short_root  # noqa: F401
from test_notify_claude import owners  # noqa: F401
from test_explorer_fixes2_kill_steer import _fixture


def test_paused_kill_commit_then_new_live_child_steer_remains_pending(
    owners, tmp_path, monkeypatch,
):
    store, handle, path, token = _fixture(owners, tmp_path, monkeypatch)
    # All old-child liveness observations prove dead; the replacement B
    # is a separate live PID/start pair on the SAME original transcript.
    def presence(pid, start):
        return ("dead" if (pid, start) == (12345, "old-start") or pid is None
                else "alive")
    monkeypatch.setattr(terminate, "owned_process", presence)
    monkeypatch.setattr(liveness, "owned_process", presence)
    monkeypatch.setattr(terminate, "owned_pid_alive",
                        lambda pid, start: presence(pid, start) == "alive")
    monkeypatch.setattr(rpc, "_rpc", lambda *_a, **_kw: {"ok": False})
    finalizations = []
    real_finalize = rpc._finalize_killed_steers
    def seen_finalize(*args, **kwargs):
        finalizations.append(True)
        return real_finalize(*args, **kwargs)
    monkeypatch.setattr(rpc, "_finalize_killed_steers", seen_finalize)
    replacement = "new-live-steer"

    def paused_after_commit(_short, _deadline, *, terminal, **_kwargs):
        # Kill committed old A dead. A revive took the revival flock, finalized
        # A's old pending receipt, started B and then durably owned B's steer.
        assert terminal() == "dead"
        assert steer_ops.finalize_dead_claude_steers(
            store, handle, child_pid=12345, child_start="old-start") == [token]
        store.mutate_record(handle, patches={
            "lifecycle.state": "busy",
            "lifecycle.workerPid": 22222,
            "lifecycle.workerStart": "new-live-start",
            "lifecycle.nativeStartProof": {
                "workerPid": 22222, "workerStart": "new-live-start",
            },
        })
        steer_ops.begin_delivery(
            A, delivery_id=replacement, text="new child work",
            transcript=str(path), host_epoch="host-new-live", state="busy",
            route="steer", requested_mode="steer_only",
            target_turn_ref="ccx-turn:claude:v1:new",
        )
        steer_ops.transition_delivery(A, replacement, "pasted", attempts=1)
        claude_delivery.prepare(handle, replacement, "new child work",
                                mode="steer_only")
        claude_delivery.mark_owned(
            handle, replacement, route="steer",
            target_turn_ref="ccx-turn:claude:v1:new",
        )
        return {"engine": "claude", "short": A, "killed": True,
                "changed": True, "interrupted": False,
                "resumable": True, "state": "dead"}

    monkeypatch.setattr(terminate, "_claude_confirmed_kill",
                        paused_after_commit)
    result = terminate.confirmed_kill_uuid(store, handle, timeout=5)
    assert result["killed"] is True
    assert finalizations  # kill attempted guarded cleanup after its commit
    assert steer_ops.get_delivery(A, token)["status"] == "abandoned"
    assert steer_ops.get_delivery(A, replacement)["status"] == "pasted"
    assert claude_delivery.read_receipt(handle, replacement)["state"] == "owned"
    assert claude_delivery.read_receipt(handle, replacement)["promptText"] == "new child work"
    assert [r["delivery_id"] for r in steer_ops.unproven_deliveries(A)] == [replacement]



def test_contested_reviver_flock_skips_cleanup_and_kill_returns_promptly(
    owners, tmp_path, monkeypatch,
):
    store, handle, _path, token = _fixture(owners, tmp_path, monkeypatch)
    monkeypatch.setattr(terminate, "owned_process", lambda *_: "dead")
    monkeypatch.setattr(terminate, "owned_pid_alive", lambda *_: False)
    monkeypatch.setattr(rpc, "_rpc", lambda *_a, **_kw: {"ok": False})
    lock_path = respawn_lock_path(A)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a") as held:
        fcntl.flock(held, fcntl.LOCK_EX)
        start = time.monotonic()
        result = terminate.confirmed_kill_uuid(store, handle, timeout=3)
        elapsed = time.monotonic() - start
        assert result["killed"] is True, result
        assert elapsed < 0.7, f"contested cleanup delayed confirmed kill: {elapsed:.3f}s"
        assert claude_delivery.read_receipt(handle, token)["state"] == "owned"
        assert steer_ops.get_delivery(A, token)["status"] == "accepted"
    # An uncontested proven-death cleanup can still repair that same token.
    rpc._finalize_killed_steers(
        store, handle, child_pid=12345, child_start="old-start",
    )
    assert claude_delivery.read_receipt(handle, token)["state"] == "abandoned"


def test_reviver_already_holding_flock_forwards_park_kill_without_reacquire(
    owners, tmp_path, monkeypatch,
):
    store, handle, _path, token = _fixture(owners, tmp_path, monkeypatch)
    store.mutate_record(handle, patches={"lifecycle.state": "idle"})
    monkeypatch.setattr(rpc, "owned_process", lambda *_: "dead")
    monkeypatch.setattr(terminate, "owned_process", lambda *_: "dead")
    monkeypatch.setattr(terminate, "owned_pid_alive", lambda *_: False)
    monkeypatch.setattr(rpc, "_rpc", lambda *_a, **_kw: {"ok": False})
    monkeypatch.setattr(rpc, "_drain_old_host", lambda *_a, **_kw: True)
    monkeypatch.setattr(rpc, "_missing_claude_bin",
                        lambda: "deliberate fake stop after cleanup")
    monkeypatch.setattr(rpc, "_acquire_respawn_lock",
                        lambda *_a, **_kw: pytest.fail("reacquired held revive flock"))
    lock_path = respawn_lock_path(A)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a") as held:
        fcntl.flock(held, fcntl.LOCK_EX)
        start = time.monotonic()
        assert rpc._respawn_locked(A, deadline=time.time() + 1) == 1
        assert time.monotonic() - start < 1
    assert claude_delivery.read_receipt(handle, token)["state"] == "abandoned"
