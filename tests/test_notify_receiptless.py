"""Exact CCX-native model turn observations with no opening SEND receipt."""
from __future__ import annotations
import ccx.notify.deliver as notify_deliver
import ccx.notify.select as notify_select
import ccx.notify.settle as notify_settle
import ccx.ops.wait as ops_wait

import json
from pathlib import Path
import time

import pytest

from ccx.codex import delivery as codex_delivery
from ccx.codex import runtime as codex_runtime
from ccx import constants
from ccx.claude import managed_turns
from ccx.notify import daemon as notifyd
from ccx.notify import registry as parent_notify
from ccx.ops import common as protocol_ops
from ccx.errors import OperationConflict, SessionUnavailable
from ccx.notify.client import ParentNotificationClient
from test_generation_store import A, B, short_root
from test_notify_claude import owners
from test_notify_registration import machine_originals, originals
from test_notify_standalone import _native_caller, _managed_caller
from test_notify_ext import FakeControl


def _claude_sources(store, monkeypatch, tmp_path):
    """Preserve the original native SID/create intent under a private fake home."""
    from ccx.store import paths
    monkeypatch.setenv("CCX_ROOT", str(constants.CCX_ROOT))
    monkeypatch.setattr(paths, "CLAUDE_HOME", tmp_path / "claude-home")
    rec = store.record(A)
    sid = rec["native"]["sessionId"]
    original = paths.transcript_path_for(rec["cwd"], sid)
    for path in (original.parent.parent, original.parent):
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
        path.chmod(0o700)
    original.touch(mode=0o600)
    original.chmod(0o600)
    # The production create binding is immutable; only this fake's record view
    # follows its separately isolated Claude home in this test process.
    from ccx.store.store import GenerationStore
    prior = GenerationStore.record
    def original_fake_source(self, target):
        value = prior(self, target)
        return ({**value, "transcript": str(original)}
                if target in {A, "claude:" + A} else value)
    monkeypatch.setattr(GenerationStore, "record", original_fake_source)
    return original


def _claude_line(path, body):
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(body) + "\n")
    path.chmod(0o600)


def _claude_hook_events(store, *events):
    path = store.worker_path(A, "events.ndjson")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as handle:
        for event in events:
            handle.write(json.dumps({"ts": time.time(), **event}) + "\n")
    path.chmod(0o600)


def _claude_wait(monkeypatch, ref, *, state="idle", last=None, gate=None):
    from ccx.ops import compact_ops
    from ccx.rpc import rpc
    monkeypatch.setattr(compact_ops, "active_operation", lambda *_: None)
    monkeypatch.setattr(rpc, "_rpc", lambda *_a, **_k: {
        "ok": True, "compacting": False,
        "activeTurnRef": ref if state in {"busy", "waiting"} else None,
    })
    def snapshot(*_args, **_kwargs):
        return {"worker": "claude:" + A, "state": state,
                "status": state if state != "idle" else "idle",
                "reason": "worker is idle" if state == "idle" else None,
                "active_turn_ref": ref if state in {"busy", "waiting"} else None,
                "last_settled_turn": {"turn_ref": last} if last else None,
                "waiting_for": gate}
    monkeypatch.setattr(ops_wait, "wait_worker", snapshot)


def _arm_claude(store, monkeypatch, ref, *, state="idle", gate=None):
    _native_caller(monkeypatch)
    _claude_wait(monkeypatch, ref, state=state,
                 last=ref if state == "idle" else None, gate=gate)
    selected = notify_select.select_current_turn("claude:" + A)
    reg = parent_notify.create_registration(
        parent=_native_caller(monkeypatch), worker_engine="claude", worker_id=A,
        worker_name=None, operation=selected["operation"], source="notify")
    return selected, reg


@pytest.mark.parametrize("state,start_event", [
    ("idle", "turn_start"), ("busy", "turn_start"),
    ("idle", "turn_identity_recovered"),
])
def test_claude_receiptless_exact_start_zero_and_later_turn_cannot_lend_report(
    owners, monkeypatch, tmp_path, state, start_event,
):
    _root, store = owners
    original = _claude_sources(store, monkeypatch, tmp_path)
    r1 = "ccx-turn:claude:v1:receiptless-R1"
    r2 = "ccx-turn:claude:v1:later-R2"
    _claude_hook_events(store, {"type": start_event, "turn_ref": r1,
                                "turn": 1, "report_start_offset": 0})
    _claude_line(original, {"type": "user", "message": {"content": "R1 model prompt"}})
    _claude_line(original, {"type": "assistant", "message": {
        "content": [{"type": "text", "text": "R1 exact native assistant"}]}})
    first_end = original.stat().st_size
    selected, reg = _arm_claude(store, monkeypatch, r1, state=state)
    assert selected["operation"]["nativeSourceId"] == store.record(A)["native"]["sessionId"]
    assert selected["operation"]["nativeProof"] == "original-native-turn"
    assert selected["operation"]["selectedTurnRef"] == r1
    assert "deliveryId" not in selected["operation"]
    assert notify_settle.settlement_for_registration(reg) is None
    _claude_hook_events(store, {"type": "turn_complete", "turn_ref": r1, "turn": 1},
                        {"type": "turn_start", "turn_ref": r2, "turn": 2,
                         "report_start_offset": first_end})
    _claude_line(original, {"type": "assistant", "message": {
        "content": [{"type": "text", "text": "WRONG R2 later assistant"}]}})
    store.mutate_record("claude:" + A, patches={"lastReport": {"text": "WRONG latest"}})
    result = notify_settle.settlement_for_registration(reg)
    assert result["status"] == result["turnStatus"] == "completed"
    assert result["turnRef"] == r1
    assert result["report"] == "R1 exact native assistant"
    assert notify_deliver.payload_for_registration(reg, result)["selectedOriginalDeliveryId"] is None
    another = parent_notify.create_registration(
        parent=_native_caller(monkeypatch), worker_engine="claude", worker_id=A,
        worker_name=None, operation=selected["operation"], source="notify")
    assert another["eventId"] != reg["eventId"]
    assert another["operation"] == reg["operation"]
    assert ParentNotificationClient().get(reg["eventId"])["delivery_id"] is None
    if state == "idle" and start_event == "turn_start":
        # One real receiptless v3 event traverses the same external parent W1
        # placement/retry history as the already-proven receipt-backed N2 path.
        from ccx.codex import control as codex_control
        control = FakeControl()
        monkeypatch.setattr(codex_control, "CodexControlClient", lambda: control)
        notifyd.process_registration(reg)
        final = parent_notify.read_registration(reg["eventId"])
        assert final["state"] == "delivered"
        assert final["payload"]["report"] == "R1 exact native assistant"
        assert final["payload"]["selectedTurnRef"] == r1
        notifyd.process_registration(final)
        assert len(control.methods("turn/start")) == 1


@pytest.mark.parametrize("terminal,expected", [
    ("interrupted", "interrupted"), ("liveness-completed", "unknown"),
    ("liveness-unknown", "unknown"),
])
def test_claude_semantic_negative_or_liveness_never_yields_model_report(
    owners, monkeypatch, tmp_path, terminal, expected,
):
    _root, store = owners
    original = _claude_sources(store, monkeypatch, tmp_path)
    ref = "ccx-turn:claude:v1:receiptless-negative"
    _claude_hook_events(store, {"type": "turn_start", "turn_ref": ref,
                                "turn": 1, "report_start_offset": 0})
    _claude_line(original, {"type": "assistant", "message": {
        "content": [{"type": "text", "text": "NO terminal model report"}]}})
    _, reg = _arm_claude(store, monkeypatch, ref)
    event = {"type": "turn_complete", "turn_ref": ref, "turn": 1,
             "interrupted": True} if terminal == "interrupted" else {
        "type": "turn_settled", "turn_ref": ref, "turn": 1,
        "outcome": "completed" if terminal == "liveness-completed" else "unknown"}
    _claude_hook_events(store, event)
    result = notify_settle.settlement_for_registration(reg)
    assert result["status"] == result["turnStatus"] == expected
    assert result["report"] is None


@pytest.mark.parametrize("case", ["no-start", "unknown-successor-offset", "source-swapped"])
def test_claude_q1_or_unsafe_window_is_never_completed_receiptless_text(
    owners, monkeypatch, tmp_path, case,
):
    _root, store = owners
    original = _claude_sources(store, monkeypatch, tmp_path)
    ref = "ccx-turn:claude:v1:compact-late-stop"
    _claude_hook_events(store, {"type": "turn_start", "turn_ref": ref, "turn": 1,
                                **({"report_start_offset": 0} if case != "no-start" else {})},
                        {"type": "turn_complete", "turn_ref": ref, "turn": 1})
    _claude_line(original, {"type": "assistant", "message": {
        "content": [{"type": "text", "text": "old text not a safe report"}]}})
    if case == "unknown-successor-offset":
        _claude_hook_events(store, {"type": "turn_start", "turn_ref": "ccx-turn:claude:v1:R2",
                                    "turn": 2})
    selected, reg = _arm_claude(store, monkeypatch, ref)
    if case == "no-start":
        assert selected["operation"]["selectionMode"] == "status-only"
        assert notify_settle.settlement_for_registration(reg)["report"] is None
        return
    if case == "source-swapped":
        original.unlink()
        original.symlink_to(tmp_path / "unowned-untrusted.jsonl")
    result = notify_settle.settlement_for_registration(reg)
    assert result["status"] == "completed" and result["report"] is None


def test_claude_wait_gate_only_frozen_active_ref(owners, monkeypatch, tmp_path):
    _root, store = owners
    _claude_sources(store, monkeypatch, tmp_path)
    r1 = "ccx-turn:claude:v1:waiting-R1"
    _claude_hook_events(store, {"type": "turn_start", "turn_ref": r1,
                                "turn": 1, "report_start_offset": 0})
    _, reg = _arm_claude(store, monkeypatch, r1, state="waiting",
                         gate={"id": "gate-r1"})
    waiting = notify_settle.settlement_for_registration(reg)
    assert waiting["status"] == "waiting" and waiting["waitingFor"]["id"] == "gate-r1"
    _claude_wait(monkeypatch, "ccx-turn:claude:v1:later", state="waiting",
                 gate={"id": "later-gate"})
    assert notify_settle.settlement_for_registration(reg) is None


def _native_parent_wake_turn(store, previous):
    """Await the real machine-input model R, never the parent's earlier SEND."""
    deadline = time.monotonic() + 12
    while time.monotonic() < deadline:
        turn = (store.record(A).get("lastTurn") or {}).get("turnId")
        if isinstance(turn, str) and turn and turn != previous:
            return turn
        time.sleep(.06)
    pytest.fail("native machine wake did not settle a new parent model turn")


@pytest.mark.parametrize("mode", ["last-settled", "active"])
def test_codex_native_without_opener_uses_exact_evidence_and_served_history(
    machine_originals, monkeypatch, mode,
):
    client, store, trace, daemon_rpc = machine_originals
    # The original managed parent's machine wake is a native model turn, not a
    # SEND delivery addressed to this turn. Reuse the accepted W1 parent path.
    from test_notify_registration import _settled_ready
    child_send = _settled_ready(client, store, token="native-parent-wake-child")
    previous = store.record(A)["lastTurn"]["turnId"]
    from ccx.notify import daemon as notifier
    notifier.process_registration(child_send)
    assert parent_notify.read_registration(child_send["eventId"])["state"] == "delivered"
    ref_turn = _native_parent_wake_turn(store, previous)
    ref = "ccx-turn:codex:v1:" + ref_turn
    assert codex_delivery.receipt_for_turn("codex:" + A, ref_turn) is None
    writes = trace.read_text().count("RPC turn/start")
    _native_caller(monkeypatch)
    if mode == "active":
        monkeypatch.setattr(ops_wait, "wait_worker", lambda *_a, **_k: {
            "worker": "codex:" + A, "state": "busy", "status": "timeout",
            "active_turn_ref": ref})
        monkeypatch.setattr(codex_runtime, "observe_worker", lambda *_a, **_k: {
            "daemon_served": True, "reply": {"ok": True, "loaded": True,
                                               "activeTurnId": ref_turn}})
    else:
        monkeypatch.setattr(ops_wait, "wait_worker", lambda *_a, **_k: {
            "worker": "codex:" + A, "state": "idle", "status": "idle",
            "reason": "worker is idle", "active_turn_ref": None,
            "last_settled_turn": {"turn_ref": ref}})
    selected = notify_select.select_current_turn("codex:" + A)
    assert selected["operation"]["nativeProof"] == "original-native-turn"
    assert selected["operation"]["turnId"] == ref_turn
    assert selected["operation"]["selectionMode"] == mode
    assert "deliveryId" not in selected["operation"]
    reg = parent_notify.create_registration(
        parent=_native_caller(monkeypatch), worker_engine="codex", worker_id=A,
        worker_name=None, operation=selected["operation"], source="notify")
    read = codex_runtime.daemon_call(
        {"op": "thread-history", "workerId": A,
         "threadId": store.record(A)["native"]["threadId"],
         "turnId": ref_turn, "observeSeconds": 2},
        timeout=2, short="codex:" + A)
    assert read.get("ok") is True, read
    assert read["thread"]["threadSource"] == "ccx-worker:" + A
    exact = [turn for turn in read["thread"].get("turns", [])
             if turn.get("id") == ref_turn]
    assert len(exact) == 1, read
    from ccx.codex.reports import report_from_turn
    assert exact[0]["status"] == "completed" and report_from_turn(exact[0]) is None
    # The real original parent wake has tool-output only, NOT a final assistant
    # message. Completion is still exact; never promote tool output to a report.
    no_final = notify_settle.settlement_for_registration(reg)
    assert no_final["status"] == "completed" and no_final["report"] is None
    # The existing fake app-server's source-verified read is enriched here with
    # an optional final item and a later R2, without any extra native action.
    # This tests the exact original turn selector, not provider capabilities.
    from copy import deepcopy
    served = deepcopy(read)
    original_turn = next(turn for turn in served["thread"]["turns"]
                         if turn["id"] == ref_turn)
    original_turn["items"].append({"type": "agentMessage", "phase": "final_answer",
                                   "text": "R1 exact native assistant final"})
    served["thread"]["turns"].append({"id": "later-model-R2", "status": "completed",
                                       "items": [{"type": "agentMessage", "phase": "final_answer",
                                                  "text": "WRONG later native final"}]})
    monkeypatch.setattr(codex_runtime, "daemon_call", lambda *_a, **_k: served)
    report = notify_settle.settlement_for_registration(reg)
    assert report["status"] == "completed", report
    assert report["turnId"] == ref_turn
    assert report["report"] == "R1 exact native assistant final"
    assert trace.read_text().count("RPC turn/start") == writes  # observer never starts native model
    # Offline journal can still prove R terminal, but no unserved report text.
    monkeypatch.setattr(codex_runtime, "daemon_call", lambda *_a, **_k: {
        "ok": False, "transport": "absent"})
    offline = notify_settle.settlement_for_registration(reg)
    assert offline["status"] == "completed" and offline["report"] is None
    assert trace.read_text().count("RPC turn/start") == writes
    assert ParentNotificationClient().get(reg["eventId"])["selected_turn_ref"] == ref


def test_codex_steer_fallback_not_opener_but_independent_native_proof_is_enough(
    machine_originals, monkeypatch,
):
    client, store, _trace, _rpc = machine_originals
    from test_notify_registration import _settled_ready
    notifier_reg = _settled_ready(client, store, token="native-joined-steer-parent")
    previous = store.record(A)["lastTurn"]["turnId"]
    notifyd.process_registration(notifier_reg)
    native_turn = _native_parent_wake_turn(store, previous)
    ref = "ccx-turn:codex:v1:" + native_turn
    monkeypatch.setattr(codex_delivery, "receipt_for_turn", lambda *_a, **_k: {
        "state": "accepted", "turnId": native_turn,
        "route": "steer", "placement": "current_turn", "requestedMode": "steer_only"})
    monkeypatch.setattr(ops_wait, "wait_worker", lambda *_a, **_k: {
        "worker": "codex:" + A, "state": "idle", "status": "idle",
        "reason": "worker is idle", "active_turn_ref": None,
        "last_settled_turn": {"turn_ref": ref}})
    selected = notify_select.select_current_turn("codex:" + A)
    assert selected["operation"]["nativeProof"] == "original-native-turn"
    # This exact native turn cannot be rebranded as a model R if the original
    # worker's compact marker owns it, even after the compact settled.
    store.mutate_record("codex:" + A, patches={"lastCompact": {"turnId": native_turn}})
    assert notify_select.select_current_turn("codex:" + A)["operation"]["selectionMode"] == "status-only"


def test_codex_no_evidence_or_compact_id_never_arms_receiptless(
    originals, monkeypatch,
):
    store, trace, _ = originals
    _native_caller(monkeypatch)
    ref = "ccx-turn:codex:v1:no-native-proof"
    monkeypatch.setattr(ops_wait, "wait_worker", lambda *_a, **_k: {
        "worker": "codex:" + B, "state": "busy", "status": "timeout",
        "active_turn_ref": ref})
    before = trace.read_bytes()
    with pytest.raises(SessionUnavailable, match="eligible opening receipt or native proof"):
        notify_select.select_current_turn("codex:" + B)
    assert parent_notify.iter_registrations() == []
    assert trace.read_bytes() == before


def test_original_source_binding_is_frozen_and_no_forged_semantic_delivery(
    owners, monkeypatch, tmp_path,
):
    _root, store = owners
    original = _claude_sources(store, monkeypatch, tmp_path)
    ref = "ccx-turn:claude:v1:original-source"
    _claude_hook_events(store, {"type": "turn_start", "turn_ref": ref,
                                "turn": 1, "report_start_offset": 0})
    _claude_line(original, {"type": "assistant", "message": {
        "content": [{"type": "text", "text": "one exact report"}]}})
    _, reg = _arm_claude(store, monkeypatch, ref)
    assert reg["registrationKeySha256"] is None
    invalid = {**reg, "operation": {**reg["operation"], "deliveryId": "fabricated"}}
    with pytest.raises(SessionUnavailable, match="receiptless native turn identity invalid"):
        notify_settle.settlement_for_registration(invalid)
    mismatched = {**reg, "operation": {**reg["operation"],
                                      "nativeSourceId": "other-provider-session"}}
    with pytest.raises(SessionUnavailable, match="original native source or turn proof changed"):
        notify_settle.settlement_for_registration(mismatched)
    assert reg["operation"]["nativeSourceId"] == store.record(A)["native"]["sessionId"]
    # A caller cannot bypass select_current_turn to durably arm a made-up R.
    fake_op = {**reg["operation"], "selectedTurnRef": "ccx-turn:claude:v1:forged"}
    initial = {r["eventId"] for r in parent_notify.iter_registrations()}
    with pytest.raises(SessionUnavailable, match="turn proof changed"):
        parent_notify.create_registration(
            parent=_native_caller(monkeypatch), worker_engine="claude",
            worker_id=A, worker_name=None, operation=fake_op, source="notify")
    assert {r["eventId"] for r in parent_notify.iter_registrations()} == initial
