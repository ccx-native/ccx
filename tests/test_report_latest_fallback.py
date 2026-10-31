"""Default native latest-answer reporting; exact delivery reports stay strict."""

from __future__ import annotations
import ccx.cli.commands.read as commands_read
import ccx.ops.report as ops_report
import ccx.ops.wait as ops_wait

import json

import pytest

from ccx import WorkerClient
from ccx.claude import delivery as claude_delivery
from ccx.codex import runtime as codex_runtime
from ccx.cli.commands import _common as commands
from ccx.ops import common as protocol_ops
from ccx.errors import DeliveryRejected, SessionNotFound, SessionUnavailable
from ccx.cli.parser import build_parser
from test_generation_store import A, short_root  # noqa: F401
from test_notify_claude import owners  # noqa: F401
from test_notify_receiptless import _claude_line, _claude_sources, _claude_hook_events
from test_worker_telemetry import original_codex, original_sources  # noqa: F401


def _native_exchange(source, prompt: str, answer: str):
    _claude_line(source, {"type": "user", "message": {"content": prompt}})
    _claude_line(source, {"type": "assistant", "message": {
        "content": [{"type": "text", "text": answer}],
    }})


def test_default_claude_large_history_parses_only_latest_native_window(
    owners, tmp_path, monkeypatch,
):
    """Count parsed records, not wall time: no full-history iteration."""
    from ccx.ops import conversation_paging
    from ccx.claude import transcript

    _root, store = owners
    source = _claude_sources(store, monkeypatch, tmp_path)
    # Thousands of earlier ordinary-user boundaries, in a single synthetic
    # JSONL. The size grows with HISTORY while one final report's work must
    # stay bounded to the most recent ordinary user plus its answer.
    with source.open("w", encoding="utf-8") as output:
        for i in range(6_000):
            output.write(json.dumps({
                "type": "user", "message": {"content": f"old request {i}"},
            }) + "\n")
            output.write(json.dumps({
                "type": "assistant", "message": {"content": [
                    {"type": "text", "text": f"old reply {i}"},
                ]},
            }) + "\n")
        output.write(json.dumps({
            "type": "user", "message": {"content": "last user boundary"},
        }) + "\n")
        output.write(json.dumps({
            "type": "assistant", "message": {"content": [
                {"type": "text", "text": "correct final native reply"},
            ]},
        }) + "\n")

    parsed = {"forward": 0, "reverse": 0}
    original_iter = transcript._iter_transcript
    original_reverse = conversation_paging._reverse_lines

    def measured_iter(*args, **kwargs):
        for record in original_iter(*args, **kwargs):
            parsed["forward"] += 1
            yield record

    def measured_reverse(*args, **kwargs):
        for value in original_reverse(*args, **kwargs):
            parsed["reverse"] += 1
            yield value

    monkeypatch.setattr(transcript, "_iter_transcript", measured_iter)
    monkeypatch.setattr(conversation_paging, "_reverse_lines", measured_reverse)
    # The cost contract is for report_worker; WorkerClient.report also reads
    # independent session-wide background-task facts via the existing API.
    report = ops_report.report_worker("claude:" + A)
    assert report["text"] == "correct final native reply"
    assert report["source"] == "native-transcript-latest"
    assert 0 < parsed["forward"] <= 6, parsed
    assert 0 < parsed["reverse"] <= 6, parsed


def test_migrated_claude_no_receipts_latest_reply_and_cli_plain_and_json(
    owners, tmp_path, monkeypatch, capsys,
):
    _root, store = owners
    source = _claude_sources(store, monkeypatch, tmp_path)
    target = "claude:" + A
    _native_exchange(source, "first ordinary user", "OLDER reply")
    _native_exchange(source, "second ordinary user", "latest native answer")
    # The continuation sentence is ordinary user input; its reply is latest.
    _native_exchange(source, "Continue from where you left off.", "continued work")
    assert list(claude_delivery.iter_receipts(target)) == []

    report = WorkerClient().report(target)
    assert report["text"] == "continued work"
    assert report["source"] == "native-transcript-latest"
    assert report["turn_ref"] is None and report["delivery_id"] is None
    assert report["has_final_message"] and not report["truncated"]
    zero = WorkerClient().report(target, max_chars=0)
    assert zero["text"] == "" and zero["truncated"] and zero["has_final_message"]

    commands_read.cmd_report(build_parser().parse_args(["report", target]))
    plain = capsys.readouterr()
    assert plain.out == "continued work\n" and plain.err == ""
    commands_read.cmd_report(build_parser().parse_args(["report", target, "--json"]))
    payload = json.loads(capsys.readouterr().out)
    assert payload["text"] == "continued work"
    assert payload["source"] == "native-transcript-latest"

    # Explicit refs remain source- and turn-proof strict; the default-only
    # native fallback does not grant a synthetic managed Claude turn ID.
    with pytest.raises((SessionNotFound, SessionUnavailable)):
        WorkerClient().report(target, turn_ref="ccx-turn:claude:v1:nonexistent")


def test_newer_claude_receiptless_turn_displaces_older_settled_receipt(
    owners, tmp_path, monkeypatch,
):
    _root, store = owners
    source = _claude_sources(store, monkeypatch, tmp_path)
    target = "claude:" + A
    ref = "ccx-turn:claude:v1:older-with-receipt"
    _claude_hook_events(store, {"type": "turn_start", "turn_ref": ref,
                                "turn": 1, "report_start_offset": 0})
    _native_exchange(source, "old prompt", "OLD settled answer")
    claude_delivery.prepare(target, "older-delivery", "old prompt", mode="new_turn_only")
    claude_delivery.mark_owned(target, "older-delivery", route="new_turn")
    claude_delivery.mark_accepted(target, "older-delivery", placement="new_turn",
                                 turn_ref=ref)
    claude_delivery.mark_settled(target, "older-delivery", turn_ref=ref,
        original_positive_completion={
            "turnRef": ref, "status": "completed", "text": "OLD settled answer",
            "nativeEvidenceSha256": "a" * 64,
        })
    _claude_hook_events(store, {"type": "turn_complete", "turn_ref": ref, "turn": 1})
    _native_exchange(source, "new notification without receipt", "NEW native answer")
    store.mutate_record(target, patches={
        "lastSettledTurn": {"turnRef": ref, "settledAtMs": 1, "outcome": "completed"},
    })

    latest = WorkerClient().report(target)
    assert latest["text"] == "NEW native answer"
    assert latest["source"] == "native-transcript-latest"
    assert latest["delivery_id"] is None and latest["turn_ref"] is None
    assert claude_delivery.read_receipt(target, "older-delivery")["settlement"]["text"] == "OLD settled answer"


def test_latest_claude_receipt_window_still_selects_strict_proof(
    owners, tmp_path, monkeypatch,
):
    _root, store = owners
    source = _claude_sources(store, monkeypatch, tmp_path)
    target = "claude:" + A
    ref = "ccx-turn:claude:v1:latest-boundary-with-receipt"
    _claude_hook_events(store, {
        "type": "turn_start", "turn_ref": ref, "turn": 1,
        "report_start_offset": 0,
    })
    _native_exchange(source, "owned latest prompt", "latest exact answer")
    claude_delivery.prepare(target, "latest-delivery", "owned latest prompt", mode="new_turn_only")
    claude_delivery.mark_owned(target, "latest-delivery", route="new_turn")
    claude_delivery.mark_accepted(target, "latest-delivery", placement="new_turn", turn_ref=ref)
    claude_delivery.mark_settled(target, "latest-delivery", turn_ref=ref,
        original_positive_completion={
            "workerId": A, "turnRef": ref, "status": "completed",
            "text": "latest exact answer", "nativeEvidenceSha256": "a" * 64,
        })
    _claude_hook_events(store, {"type": "turn_complete", "turn_ref": ref, "turn": 1})
    store.mutate_record(target, patches={
        "lastSettledTurn": {"turnRef": ref, "settledAtMs": 1, "outcome": "completed"},
    })
    def verified_owner(target, *, delivery_id):
        assert target == "claude:" + A and delivery_id == "latest-delivery"
        return {"status": "settled"}
    monkeypatch.setattr(ops_wait, "reconcile_turn", verified_owner)
    report = WorkerClient().report(target)
    assert report["text"] == "latest exact answer"
    assert report["delivery_id"] == "latest-delivery"
    assert report["source"] == "original-native-completion-receipt"


def test_codex_receiptless_uses_last_completed_turn_final_phase_and_no_older_receipt(
    original_codex, monkeypatch,
):
    store, cwd, wid, tid = original_codex
    target = "codex:" + wid
    history = {"id": tid, "threadSource": store.codex_source(target), "turns": [
        {"id": "prior", "status": "completed", "items": [
            {"type": "agentMessage", "phase": "final_answer", "text": "OLD"}]},
        {"id": "newest", "status": "completed", "items": [
            {"type": "agentMessage", "phase": "commentary", "text": "commentary"},
            {"type": "agentMessage", "phase": "final_answer", "text": "LATEST"},
            {"type": "agentMessage", "text": "wrong phase-less tail"}]},
    ]}
    calls = []
    def history_read(thread_id, *, cwd, worker_id, expected_source, timeout,
                     include_turns=True):
        calls.append((thread_id, worker_id, expected_source))
        assert include_turns and timeout > 0
        return history
    monkeypatch.setattr(codex_runtime, "read_thread", history_read)
    store.mutate_record(target, patches={
        "lastTurn": {"turnId": "newest", "status": "completed"},
    })
    latest = WorkerClient().report(target)
    assert latest["text"] == "LATEST"
    assert latest["turn_ref"] == "ccx-turn:codex:v1:newest"
    assert latest["delivery_id"] is None
    assert latest["source"] == "native-transcript-latest"
    assert calls == [(tid, wid, store.codex_source(target))]


@pytest.mark.parametrize("outcome,items,expected_text", [
    ("failed", [], ""),
    # A stopped turn never surfaces partial text as a final reply.
    ("interrupted", [{"type": "agentMessage", "phase": "final_answer",
                      "text": "LATEST partial answer"}], ""),
])
def test_codex_latest_failed_or_interrupted_turn_is_not_an_older_success(
    original_codex, monkeypatch, outcome, items, expected_text,
):
    store, _cwd, wid, tid = original_codex
    target = "codex:" + wid
    history = {"id": tid, "threadSource": store.codex_source(target), "turns": [
        {"id": "old-success", "status": "completed", "items": [
            {"type": "agentMessage", "phase": "final_answer", "text": "STALE OLD ANSWER"}]},
        {"id": "latest-terminal", "status": outcome, "items": items},
    ]}
    monkeypatch.setattr(codex_runtime, "read_thread", lambda *_a, **_kw: history)
    store.mutate_record(target, patches={
        "lastTurn": {"turnId": "latest-terminal", "status": outcome},
    })

    latest = WorkerClient().report(target)
    assert latest["status"] == outcome
    assert latest["turn_id"] == "latest-terminal"
    assert latest["turn_ref"] == "ccx-turn:codex:v1:latest-terminal"
    assert latest["text"] == expected_text
    assert latest["has_final_message"] is bool(expected_text)
    assert latest["delivery_id"] is None


@pytest.mark.parametrize("answer", ["", "LATEST partial answer"])
def test_claude_latest_interrupted_turn_does_not_claim_completion(
    owners, tmp_path, monkeypatch, answer,
):
    _root, store = owners
    source = _claude_sources(store, monkeypatch, tmp_path)
    target = "claude:" + A
    prior = "ccx-turn:claude:v1:older"
    latest_ref = "ccx-turn:claude:v1:interrupted-latest"
    _claude_hook_events(store, {"type": "turn_start", "turn_ref": prior,
                                "turn": 1, "report_start_offset": 0})
    _native_exchange(source, "old user prompt", "STALE OLD ANSWER")
    end_old = source.stat().st_size
    _claude_hook_events(store, {"type": "turn_complete", "turn_ref": prior, "turn": 1},
                        {"type": "turn_start", "turn_ref": latest_ref,
                         "turn": 2, "report_start_offset": end_old})
    _claude_line(source, {"type": "user", "message": {"content": "interrupted prompt"}})
    if answer:
        _claude_line(source, {"type": "assistant", "message": {
            "content": [{"type": "text", "text": answer}]}})
    _claude_hook_events(store, {"type": "turn_complete", "turn_ref": latest_ref,
                                "turn": 2, "interrupted": True})
    store.mutate_record(target, patches={
        "lastSettledTurn": {"turnRef": latest_ref, "turn": 2,
                            "settledAtMs": 1000, "outcome": "interrupted"},
    })

    result = WorkerClient().report(target)
    assert result["status"] == "interrupted"
    assert result["text"] == answer
    assert result["has_final_message"] is bool(answer)
    assert result["delivery_id"] is None


@pytest.mark.parametrize("engine", ["claude", "codex"])
def test_receiptless_midturn_is_busy_not_partial_text(
    engine, request, tmp_path, monkeypatch,
):
    if engine == "claude":
        _root, store = request.getfixturevalue("owners")
        target = "claude:" + A
        source = _claude_sources(store, monkeypatch, tmp_path)
        _native_exchange(source, "still running", "INTERMEDIATE only")
        store.mutate_record(target, patches={"lifecycle.state": "busy"})
    else:
        store, _cwd, wid, _tid = request.getfixturevalue("original_codex")
        target = "codex:" + wid
        store.mutate_record(target, patches={
            "lifecycle.state": "busy", "lifecycle.activeTurnId": "pending-native",
        })
        monkeypatch.setattr(codex_runtime, "read_thread", lambda *_a, **_k:
                            pytest.fail("busy worker must not hydrate history"))
    with pytest.raises(SessionUnavailable, match="no completed reply yet; a turn is still running"):
        WorkerClient().report(target)
