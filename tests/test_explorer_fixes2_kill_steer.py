"""Kill/revive Claude steer receipts: pinned fake transcript, no native process."""
from __future__ import annotations
import ccx.ops.wait as ops_wait

from contextlib import contextmanager
import json
import os

import pytest

from ccx.claude import delivery as claude_delivery
from ccx.ops import steer_ops
from ccx.claude import transcript
from ccx.store import liveness
from test_generation_store import A, short_root  # noqa: F401
from test_notify_claude import owners  # noqa: F401


def _fixture(owners, tmp_path, monkeypatch, *, append_evidence=False):
    _, store = owners
    path = tmp_path / "original-claude-native.jsonl"
    path.write_text(json.dumps({"type":"user","message":{"content":"prior turn"}}) + "\n")
    token = "kill-pending"
    handle = "claude:" + A
    store.mutate_record(handle, patches={
        "lifecycle.workerPid": 12345, "lifecycle.workerStart": "old-start",
        "lifecycle.nativeStartProof": {
            "workerPid": 12345, "workerStart": "old-start",
        },
    })
    steer_ops.begin_delivery(
        A, delivery_id=token, text="PLEASE STEER ONCE",
        transcript=str(path), host_epoch="host-old",
        state="busy", route="steer",
        requested_mode="steer_only",
        target_turn_ref="ccx-turn:claude:v1:old",
    )
    claude_delivery.prepare(handle, token, "PLEASE STEER ONCE", mode="steer_only")
    claude_delivery.mark_owned(handle, token, route="steer",
                               target_turn_ref="ccx-turn:claude:v1:old")
    if append_evidence:
        with path.open("a") as stream:
            stream.write(json.dumps({
                "type":"queue-operation","operation":"enqueue",
                "content":"PLEASE STEER ONCE","uuid":"native-evidence-1",
            }) + "\n")
    @contextmanager
    def pinned(_store, _owner, _record):
        fd = os.open(path, os.O_RDONLY)
        try:
            st = os.fstat(fd)
            yield transcript.PinnedNativeSource(fd), st, st.st_size, path, None
        finally:
            os.close(fd)
    monkeypatch.setattr(transcript, "open_original_claude_source", pinned)
    monkeypatch.setattr(
        liveness, "owned_process",
        lambda pid, start: (
            "dead" if (pid, start) == (12345, "old-start") else "alive"
        ),
    )
    return store, handle, path, token


@pytest.mark.parametrize("evidence,fate", [(False,"abandoned"), (True,"delivered")])
def test_confirmed_dead_child_fences_steer_and_repairs_both_journals(
    owners, tmp_path, monkeypatch, evidence, fate,
):
    store, handle, path, token = _fixture(
        owners, tmp_path, monkeypatch, append_evidence=evidence)
    assert steer_ops.finalize_dead_claude_steers(
        store, handle, child_pid=12345, child_start="old-start") == [token]
    hot = steer_ops.get_delivery(A, token)
    semantic = claude_delivery.read_receipt(handle, token)
    assert hot["status"] == fate and semantic["state"] == fate
    assert not semantic["maybeWritten"] and semantic["promptText"] is None
    if evidence:
        assert hot["evidence"] == "enqueued"
        assert semantic["evidenceUuid"] == "native-evidence-1"
        assert type(semantic["evidenceOffset"]) is int
    assert steer_ops.unproven_deliveries(A) == []
    assert steer_ops.finalize_dead_claude_steers(
        store, handle, child_pid=12345, child_start="old-start") == [token]


def test_unproven_child_death_and_transcript_inode_replacement_fail_closed(
    owners, tmp_path, monkeypatch,
):
    store, handle, path, token = _fixture(owners, tmp_path, monkeypatch)
    monkeypatch.setattr(liveness, "owned_process", lambda *_a: "unprovable")
    assert steer_ops.finalize_dead_claude_steers(
        store, handle, child_pid=12345, child_start="old-start") == []
    monkeypatch.setattr(
        liveness, "owned_process",
        lambda pid, start: (
            "dead" if (pid, start) == (12345, "old-start") else "alive"
        ),
    )
    replacement = tmp_path / "switched-native-source.jsonl"
    replacement.write_text('{"type":"user","message":{"content":"anything"}}\n')
    os.replace(replacement, path)
    assert steer_ops.finalize_dead_claude_steers(
        store, handle, child_pid=12345, child_start="old-start") == []
    assert steer_ops.get_delivery(A, token)["status"] == "accepted"
    assert claude_delivery.read_receipt(handle, token)["state"] == "owned"


def test_second_kill_repairs_crash_between_hot_and_semantic_receipts(
    owners, tmp_path, monkeypatch,
):
    store, handle, _path, token = _fixture(owners, tmp_path, monkeypatch)
    real = claude_delivery.finalize_dead_steer
    monkeypatch.setattr(claude_delivery, "finalize_dead_steer",
                        lambda *_a, **_kw: (_ for _ in ()).throw(OSError("fake crash")))
    with pytest.raises(OSError, match="fake crash"):
        steer_ops.finalize_dead_claude_steers(
            store, handle, child_pid=12345, child_start="old-start")
    assert steer_ops.get_delivery(A, token)["status"] == "abandoned"
    assert claude_delivery.read_receipt(handle, token)["state"] == "owned"
    monkeypatch.setattr(claude_delivery, "finalize_dead_steer", real)
    assert steer_ops.finalize_dead_claude_steers(
        store, handle, child_pid=12345, child_start="old-start") == [token]
    assert claude_delivery.read_receipt(handle, token)["state"] == "abandoned"


@pytest.mark.parametrize("fate,evidence", [
    ("abandoned", False), ("delivered", True),
])
def test_terminal_steer_public_consumers_never_expose_pending_or_retry(
    owners, tmp_path, monkeypatch, fate, evidence,
):
    from ccx.ops import common as protocol_ops
    from ccx.rpc import local_rpc
    from ccx.client import CcxClient

    store, handle, _path, token = _fixture(
        owners, tmp_path, monkeypatch, append_evidence=evidence)
    assert steer_ops.finalize_dead_claude_steers(
        store, handle, child_pid=12345, child_start="old-start") == [token]

    hot = steer_ops.public_delivery(steer_ops.get_delivery(A, token))
    assert hot["pending"] is False
    if fate == "abandoned":
        assert hot["status"] == "unknown"

    semantic = claude_delivery.public_receipt(
        claude_delivery.read_receipt(handle, token))
    assert semantic["owned"] is True
    assert semantic["recommended_action"] == "none"
    assert semantic["status"] == ("unknown" if fate == "abandoned" else "accepted")
    reconciled = ops_wait.reconcile_turn(
        handle, delivery_id=token, include_worker=False)
    assert reconciled["pending"] is False
    assert reconciled["recommended_action"] == "none"
    assert reconciled["owned"] is True

    typed = CcxClient().steer_status(handle, token)
    assert typed.pending is False
    assert typed.accepted is True
    assert typed.recommended_action == "none"
    assert typed.status == "unknown"  # no fabricated turn-placement verdict

    observed = []
    original = local_rpc.reconcile_turn
    def counted(*args, **kwargs):
        observed.append((args, kwargs))
        return original(*args, **kwargs)
    monkeypatch.setattr(local_rpc, "reconcile_turn", counted)
    v1 = local_rpc._op_turn_reconcile(
        {"target": handle, "delivery_id": token, "hold_while_pending": True},
        timeout=1.5)
    assert v1["pending"] is False
    assert v1["recommended_action"] == "none"
    assert len(observed) <= 2  # the hold loop does not poll terminal unknown
