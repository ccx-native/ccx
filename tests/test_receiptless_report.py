"""S2 public exact native R reporting with no opening CCX SEND delivery receipt."""
from __future__ import annotations
import ccx.ops.report as ops_report
import ccx.ops.wait as ops_wait

import pytest

from ccx.codex import delivery as codex_delivery
from ccx.codex import evidence as codex_evidence
from ccx.codex import runtime as codex_runtime
from ccx import constants
from ccx.ops import common as protocol_ops
from ccx.errors import InvalidRequest, SessionNotFound, SessionUnavailable
from ccx.store.store import GenerationStore
from test_generation_store import A, short_root
from test_notify_claude import owners
from test_notify_receiptless import (
    _claude_sources, _claude_hook_events, _claude_line,
)
from test_worker_telemetry import (
    original_sources, original_codex, _journal_turn,
)


def test_public_claude_exact_r1_not_latest_r2_empty_and_d1_reply(
    owners, tmp_path, monkeypatch,
):
    from ccx import CcxClient

    _root, store = owners
    source = _claude_sources(store, monkeypatch, tmp_path)
    r1 = "ccx-turn:claude:v1:public-no-send-r1"
    r2 = "ccx-turn:claude:v1:later-unrelated-r2"
    empty = "ccx-turn:claude:v1:tool-only-r3"
    later = "ccx-turn:claude:v1:later-r4"

    _claude_hook_events(store, {
        "type": "turn_start", "turn_ref": r1, "turn": 1,
        "report_start_offset": 0,
    })
    _claude_line(source, {"type": "user", "message": {"content": "actual original input"}})
    _claude_line(source, {
        "type": "assistant", "message": {
            "content": [{"type": "text", "text": "R1 original native model final"}],
        },
    })
    offset_r2 = source.stat().st_size
    _claude_hook_events(
        store,
        {"type": "turn_complete", "turn_ref": r1, "turn": 1},
        {"type": "turn_start", "turn_ref": r2, "turn": 2,
         "report_start_offset": offset_r2},
    )
    _claude_line(source, {"type": "assistant", "message": {
        "content": [{"type": "text", "text": "WRONG R2 later answer"}],
    }})
    offset_empty = source.stat().st_size
    _claude_hook_events(store, {
        "type": "turn_complete", "turn_ref": r2, "turn": 2,
    }, {
        "type": "turn_start", "turn_ref": empty, "turn": 3,
        "report_start_offset": offset_empty,
    })
    _claude_line(source, {"type": "assistant", "message": {
        "content": [{"type": "tool_use", "id": "tool-r3", "name": "Bash",
                     "input": {"command": "true"}}],
    }})
    offset_r4 = source.stat().st_size
    _claude_hook_events(store, {
        "type": "turn_complete", "turn_ref": empty, "turn": 3,
    }, {
        "type": "turn_start", "turn_ref": later, "turn": 4,
        "report_start_offset": offset_r4,
    })
    store.mutate_record("claude:" + A, patches={
        "lastReport": {"turnRef": r2, "text": "WRONG latest cached"},
    })
    result = ops_report.report_worker("claude:" + A, turn_ref=r1)
    assert result["worker"] == "claude:" + A and result["worker_id"] == A
    assert result["turn_ref"] == r1 and result["turn_id"] is None
    assert result["status"] == "completed" and result["text"] == "R1 original native model final"
    assert result["source"] == "original-native-exact-turn"
    assert result["has_final_message"] is True and not result["truncated"]
    assert (result["delivery_id"], result["delivery"],
            result["native_evidence_sha256"]) == (None, None, None)
    clipped = ops_report.report_worker(A, turn_ref=r1, max_chars=0)
    assert clipped["text"] == "" and clipped["truncated"] is True
    assert clipped["has_final_message"] is True
    empty_result = ops_report.report_worker(A, turn_ref=empty, max_chars=0)
    assert empty_result["status"] == "completed"
    assert empty_result["text"] == "" and not empty_result["truncated"]
    assert empty_result["has_final_message"] is False
    # S3 public WorkerClient facade carries the S2 flag, not len(text).
    from ccx import WorkerClient
    facade = WorkerClient()
    whole = facade.report("claude:" + A, turn_ref=r1)
    assert whole["text"] == "R1 original native model final"
    assert whole["has_final_message"] is True
    zero = facade.report("claude:" + A, turn_ref=r1, max_chars=0)
    assert zero["text"] == "" and zero["truncated"] is True
    assert zero["has_final_message"] is True
    tool_only = facade.report("claude:" + A, turn_ref=empty, max_chars=0)
    assert tool_only["status"] == "completed" and tool_only["text"] == ""
    assert tool_only["has_final_message"] is False

    # D1 already recorded that native R1 settled when original input was
    # accepted. It must now use the same public exact source, not retry input.
    recorded = GenerationStore.record
    def settled_record(self, target):
        value = recorded(self, target)
        if target in (A, "claude:" + A):
            return {**value, "lastSettledTurn": {
                "turnRef": r1, "outcome": "completed",
            }}
        return value
    with monkeypatch.context() as patch:
        patch.setattr(GenerationStore, "record", settled_record)
        answered = CcxClient()._input_wait_reply(
            A, "question-request-original", "decision-id", r1, None, None,
        )
    assert answered.reply == result["text"] and answered.turn_ref == r1
    assert answered.settled and not answered.queued
    # An in-flight original steer naming R1 (even without turnRef yet) is
    # an authoritative receipt: never bypass it with native-only reporting.
    from ccx.claude import delivery as claude_delivery
    token = "unresolved-owned-steer-on-r1"
    claude_delivery.prepare("claude:" + A, token, "potential current input",
                            mode="steer_only")
    claude_delivery.mark_owned("claude:" + A, token, route="steer",
                               target_turn_ref=r1)
    with pytest.raises(SessionUnavailable, match="No verified completed reply"):
        ops_report.report_worker(A, turn_ref=r1)
    with pytest.raises(SessionNotFound):
        ops_report.report_worker(A, turn_ref="ccx-turn:claude:v1:never-started")
    assert source.is_file()


def test_public_codex_journal_exact_r1_final_r2_excluded_d2_empty(
    original_codex, monkeypatch,
):
    from ccx import WorkerClient
    from ccx.codex.reports import report_from_turn

    store, cwd, wid, thread_id = original_codex
    _journal_turn(wid, thread_id)
    original = "native-turn-1"
    ref = f"ccx-turn:codex:v1:{original}"
    history = {
        "id": thread_id, "threadSource": f"ccx-worker:{wid}",
        "turns": [
            {"id": original, "status": "completed",
             "completedAt": 1789818001,
             "items": [
                 {"type": "agentMessage", "phase": "commentary",
                  "text": "not final"},
                 {"type": "agentMessage", "text": "fallback answer"},
                 {"id": "native-r1-final", "type": "agentMessage",
                  "phase": "final_answer", "text": "R1 final, not R2"},
             ]},
            {"id": "later-r2", "status": "completed", "items": [
                {"type": "agentMessage", "phase": "final_answer",
                 "text": "WRONG R2 latest reply"},
            ]},
        ],
    }
    called = []
    def original_history(requested, *, cwd, worker_id, timeout,
                         include_turns=True, expected_source=None):
        assert expected_source == store.codex_source(wid)
        called.append((requested, worker_id, cwd, timeout))
        assert requested == thread_id and worker_id == wid
        assert cwd == str(store.record("codex:" + wid)["cwd"])
        assert include_turns
        return history
    monkeypatch.setattr(codex_runtime, "read_thread", original_history)
    report = ops_report.report_worker("codex:" + wid, turn_ref=ref)
    assert report["turn_ref"] == ref and report["turn_id"] == original
    assert report["item_id"] == "native-r1-final" and report["phase"] == "final_answer"
    assert report["text"] == "R1 final, not R2"
    assert report["has_final_message"] is True and not report["truncated"]
    assert report["source"] == "original-native-exact-turn"
    assert (report["delivery_id"], report["delivery"],
            report["native_evidence_sha256"]) == (None, None, None)
    clipped = ops_report.report_worker("codex:" + wid, turn_ref=ref, max_chars=0)
    assert clipped["text"] == "" and clipped["truncated"]
    assert clipped["has_final_message"] is True
    assert clipped["turn_id"] == original
    from ccx import WorkerClient
    facade = WorkerClient()
    zero = facade.report("codex:" + wid, turn_ref=ref, max_chars=0)
    assert zero["text"] == "" and zero["truncated"] is True
    assert zero["has_final_message"] is True and zero["turn_id"] == original
    with monkeypatch.context() as patch:
        def wrong_native(_id, **_kw):
            return {**history, "threadSource": "ccx-worker:FOREIGN"}
        patch.setattr(codex_runtime, "read_thread", wrong_native)
        with pytest.raises(SessionUnavailable, match="belongs to another worker"):
            ops_report.report_worker("codex:" + wid, turn_ref=ref)
    history["turns"][0]["items"] = [
        {"type": "commandExecution", "id": "one-tool"},
        {"type": "agentMessage", "phase": "commentary", "text": "not-final"},
    ]
    assert report_from_turn(history["turns"][0]) is None
    empty = ops_report.report_worker("codex:" + wid, turn_ref=ref)
    assert empty["text"] == "" and empty["has_final_message"] is False
    assert empty["status"] == "completed" and empty["turn_id"] == original
    tool_only = facade.report("codex:" + wid, turn_ref=ref)
    assert tool_only["status"] == "completed" and tool_only["text"] == ""
    assert tool_only["has_final_message"] is False

    # D2's existing wait/approve branch only observes the exact model R.
    # No new input decision/native SEND is authorized to manufacture a report.
    history["turns"][0]["items"] = [
        {"id": "r1-final-again", "type": "agentMessage",
         "phase": "final_answer", "text": "D2 exact accepted reply"},
    ]
    from ccx import workers as worker_mod
    wait_calls = []
    def already_settled(_self, worker, *, timeout=None):
        assert worker == "codex:" + wid
        wait_calls.append(worker)
        return {
            "state": "idle", "native_state": "idle", "status": "idle",
            "last_settled_turn": {"turn_ref": ref, "turn_id": original,
                                  "outcome": "completed"},
            "last_turn": {"turnId": original, "status": "completed"},
        }
    with monkeypatch.context() as patch:
        patch.setattr(worker_mod.WorkerClient, "wait", already_settled)
        worker_client = WorkerClient()
        exact = worker_client._codex_settle(
            store.resolve("codex:" + wid), None, "approve",
            gate_id="gate-accepted-once", turn_id=original,
        )
    assert exact == "D2 exact accepted reply"
    assert wait_calls == ["codex:" + wid]
    assert len(called) >= 4
    assert store.read_receipt("codex:" + wid, "codex-send", "gate-accepted-once") is None


def test_codex_default_latest_proves_receiptless_message_past_tool_only_and_inflight(
    original_codex, monkeypatch,
):
    from ccx import WorkerClient

    store, cwd, wid, native_id = original_codex
    target = "codex:" + wid
    _journal_turn(wid, native_id)
    token = "settled-send-r1"
    codex_delivery.prepare(target, token, "original SEND", mode="new_turn_only")
    codex_delivery.mark_submitted(target, token)
    codex_delivery.mark_accepted(target, token, turn_id="native-turn-1")
    codex_delivery.mark_settled(
        target, token, turn_id="native-turn-1",
        original_positive_completion={
            "workerId": wid, "engine": "codex", "deliveryId": token,
            "turnId": "native-turn-1", "threadId": native_id,
            "nativeSource": f"ccx-worker:{wid}", "status": "completed",
            "text": "RECEIPT R1", "nativeEvidenceSha256": "a" * 64,
        },
    )
    journal = codex_evidence.EvidenceJournal(wid)
    for turn_id in ("native-turn-2", "native-turn-3"):
        journal.record("turn/started", {"turn": {"id": turn_id}}, thread_id=native_id)
        journal.record("turn/completed", {
            "turn": {"id": turn_id, "status": "completed"},
        }, thread_id=native_id)
    journal.close()
    history = {
        "id": native_id, "threadSource": f"ccx-worker:{wid}",
        "turns": [
            {"id": "native-turn-1", "status": "completed", "items": [
                {"type": "agentMessage", "phase": "final_answer", "text": "RECEIPT R1"},
            ]},
            {"id": "native-turn-2", "status": "completed", "items": [
                {"id": "notification-final", "type": "agentMessage",
                 "phase": "final_answer", "text": "NOTIFICATION R2"},
            ]},
            {"id": "native-turn-3", "status": "completed", "items": [
                {"type": "commandExecution", "id": "tool-only-r3"},
            ]},
        ],
    }
    reads = []

    def read_native(thread_id, *, cwd, worker_id, timeout, include_turns=True,
                    expected_source=None):
        assert expected_source == store.codex_source(wid)
        reads.append((thread_id, cwd, worker_id))
        assert include_turns and timeout > 0
        return history

    monkeypatch.setattr(codex_runtime, "read_thread", read_native)
    store.mutate_record(target, patches={
        "lastTurn": {"turnId": "native-turn-3", "status": "completed"},
        "lastReport": {"turnId": "native-turn-2", "text": "UNTRUSTED CACHE"},
    })
    for report in (ops_report.report_worker(target), WorkerClient().report(target)):
        # The last *completed turn*, not the last turn having an agentMessage,
        # decides the default. Tool-only R3 must not borrow R2's final text.
        assert report["turn_id"] == "native-turn-3"
        assert report["turn_ref"] == "ccx-turn:codex:v1:native-turn-3"
        assert report["text"] == "" and report["has_final_message"] is False
        assert report["source"] == "native-transcript-latest"
        assert report["delivery_id"] is None
    assert reads == [(native_id, str(cwd), wid)] * 2

    # item/completed can advance lastReport before its turn/completed event.
    # A single native read must reselect the last completed message, not R4.
    history["turns"].append({
        "id": "native-turn-4", "status": "inProgress", "items": [
            {"type": "agentMessage", "phase": "final_answer", "text": "INFLIGHT R4"},
        ],
    })
    store.mutate_record(target, patches={
        "lastReport": {"turnId": "native-turn-4", "text": "UNPROVEN R4"},
        "lifecycle.activeTurnId": "native-turn-4",
    })
    reads.clear()
    for report in (ops_report.report_worker(target), WorkerClient().report(target)):
        assert report["text"] == "NOTIFICATION R2"
        assert report["turn_id"] == "native-turn-2"
        assert report["source"] == "native-transcript-latest"
        assert report["note"] == "a newer turn is still running"
    assert reads == [(native_id, str(cwd), wid)] * 2
    reads.clear()

    explicit = ops_report.report_worker(
        target, turn_ref="ccx-turn:codex:v1:native-turn-1",
    )
    assert explicit["text"] == "RECEIPT R1"
    assert explicit["source"] == "original-native-completion-receipt"
    assert explicit["delivery_id"] == token
    assert reads == []


def test_public_receipt_authority_negative_and_receipt_backed_zero(
    original_codex, monkeypatch,
):
    store, _cwd, wid, native_id = original_codex
    _journal_turn(wid, native_id)
    ref = "ccx-turn:codex:v1:native-turn-1"
    reads = []
    thread = {
        "id": native_id, "threadSource": f"ccx-worker:{wid}",
        "turns": [{"id": "native-turn-1", "status": "completed",
                   "items": [{"type": "agentMessage", "phase": "final_answer",
                              "text": "original model final"}]}],
    }
    def native_history(_thread_id, *, cwd, worker_id, timeout,
                       include_turns=True, expected_source=None):
        assert expected_source == store.codex_source(wid)
        reads.append(worker_id)
        return thread
    monkeypatch.setattr(codex_runtime, "read_thread", native_history)

    # An owned steer names this R through targetTurnId even before its
    # receipt has a turnRef. It must prevent receiptless native bypass.
    pending_steer = "owned-target-turn-unreconciled"
    codex_delivery.prepare("codex:" + wid, pending_steer,
                           "steer input uncertain", mode="steer_only")
    codex_delivery.mark_owned("codex:" + wid, pending_steer,
                              route_kind="steer", target_turn_id="native-turn-1")
    with pytest.raises(SessionUnavailable, match="not confirmed complete"):
        ops_report.report_worker("codex:" + wid, turn_ref=ref)
    assert reads == []

    # A live/unknown SEND receipt naming R retains its OWN authority.
    # A matching native completed R must not silently resolve that receipt.
    token = "receipt-known-but-not-settled"
    codex_delivery.prepare("codex:" + wid, token, "incomplete SEND",
                           mode="new_turn_only")
    codex_delivery.mark_submitted("codex:" + wid, token)
    codex_delivery.mark_accepted("codex:" + wid, token, turn_id="native-turn-1")
    with monkeypatch.context() as patch:
        def still_unknown(*_a, **_kw):
            return {"status": "accepted", "pending": True}
        patch.setattr(ops_wait, "reconcile_turn", still_unknown)
        with pytest.raises(SessionUnavailable, match="not confirmed complete"):
            ops_report.report_worker("codex:" + wid, turn_ref=ref)
    assert reads == []  # No receiptless bypass or native history access.

    # Receipt-backed control is unchanged except additive final existence
    # determined BEFORE max_chars=0 clipping.
    proof = {
        "workerId": wid, "engine": "codex", "deliveryId": token,
        "turnId": "native-turn-1", "threadId": native_id,
        "nativeSource": f"ccx-worker:{wid}", "status": "completed",
        "text": "receipt-owned text", "nativeEvidenceSha256": "b" * 64,
    }
    # The existing receipt may positively settle DURING original
    # reconciliation. Report must re-read that same token, not treat its
    # earlier un-settled snapshot as a terminal failure or native fallback.
    def complete_during_reconcile(target, *, delivery_id, timeout):
        assert (target, delivery_id) == ("codex:" + wid, token)
        assert 0 < timeout <= 90
        codex_delivery.mark_settled(
            target, delivery_id, turn_id="native-turn-1",
            original_positive_completion=proof,
        )
        return {"status": "settled"}
    with monkeypatch.context() as patch:
        patch.setattr(ops_wait, "reconcile_turn", complete_during_reconcile)
        backed = ops_report.report_worker(
            "codex:" + wid, turn_ref=ref, max_chars=0,
        )
    assert backed["source"] == "original-native-completion-receipt"
    assert backed["delivery_id"] == token and backed["delivery"] is not None
    assert backed["text"] == "" and backed["truncated"]
    assert backed["has_final_message"] is True
    assert reads == []
    with pytest.raises(InvalidRequest):
        ops_report.report_worker("codex:" + wid, turn_ref="bad-ref")
    with pytest.raises(SessionUnavailable):
        ops_report.report_worker(
            "codex:" + wid, turn_ref="ccx-turn:codex:v1:later-r2")
    # Original journal never proved R2; no latest-turn substitute.
    assert reads[-1:] == [wid]
    # No journal-start/completion for this ref. A later history R cannot
    # make the named original model R exist.
    with pytest.raises(SessionUnavailable):
        ops_report.report_worker(
            "codex:" + wid, turn_ref="ccx-turn:codex:v1:unknown-native")
