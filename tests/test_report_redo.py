"""Default report: completed receipts survive later activity, never partial output."""
import ccx.ops.report as ops_report

import pytest

from ccx.claude import delivery as claude_delivery
from ccx.codex import delivery as codex_delivery
from ccx.codex import runtime as codex_runtime
from ccx.ops import common as protocol_ops
from ccx.errors import SessionUnavailable
from test_generation_store import A, short_root  # noqa: F401
from test_notify_claude import owners  # noqa: F401
from test_notify_receiptless import _claude_line, _claude_sources
from test_worker_telemetry import original_codex, original_sources  # noqa: F401


def _settle_codex(target, wid, tid, token, turn_id, text):
    codex_delivery.prepare(target, token, token, mode="new_turn_only")
    codex_delivery.mark_submitted(target, token)
    codex_delivery.mark_accepted(target, token, turn_id=turn_id)
    codex_delivery.mark_settled(target, token, turn_id=turn_id,
        original_positive_completion={
            "workerId": wid, "engine": "codex", "deliveryId": token,
            "turnId": turn_id, "threadId": tid,
            "nativeSource": f"ccx-worker:{wid}", "status": "completed",
            "text": text, "nativeEvidenceSha256": "a" * 64})


def test_busy_codex_last_completed_receipt_and_no_partial_history(original_codex, monkeypatch):
    store, _cwd, wid, tid = original_codex
    target = "codex:" + wid
    _settle_codex(target, wid, tid, "done", "old-turn", "SAFE REPLY")
    store.mutate_record(target, patches={
        "lastTurn": {"turnId": "old-turn", "status": "completed"},
        "lifecycle.state": "busy", "lifecycle.activeTurnId": "active-turn"})
    monkeypatch.setattr(codex_runtime, "read_thread", lambda *_a, **_kw:
                        pytest.fail("busy report must not read partial native history"))
    selected = ops_report.report_worker(target)
    assert selected["text"] == "SAFE REPLY"
    assert selected["note"] == "a newer turn is still running"


def test_codex_receipt_shortcut_ignores_prepared_and_notes_unknown_newer(original_codex):
    store, _cwd, wid, tid = original_codex
    target = "codex:" + wid
    _settle_codex(target, wid, tid, "done", "old-turn", "SAFE REPLY")
    store.mutate_record(target, patches={
        "lastTurn": {"turnId": "old-turn", "status": "completed"}})
    codex_delivery.prepare(target, "draft", "unsent", mode="new_turn_only")
    selected = ops_report.report_worker(target)
    assert selected["text"] == "SAFE REPLY" and selected["note"] is None

    codex_delivery.prepare(target, "next", "sent", mode="new_turn_only")
    codex_delivery.mark_submitted(target, "next")
    selected = ops_report.report_worker(target)
    assert selected["text"] == "SAFE REPLY"
    assert selected["note"] == "a newer message's outcome is unknown"


def test_failed_claude_reads_native_answer_with_note(owners, tmp_path, monkeypatch):
    _root, store = owners
    target = "claude:" + A
    source = _claude_sources(store, monkeypatch, tmp_path)
    _claude_line(source, {"type": "user", "message": {"content": "request"}})
    _claude_line(source, {"type": "assistant", "message": {
        "content": [{"type": "text", "text": "completed before crash"}]}})
    store.mutate_record(target, patches={"lifecycle.state": "crashed"})
    selected = ops_report.report_worker(target)
    assert selected["text"] == "completed before crash"
    assert selected["note"] == "the worker failed"


def test_codex_native_read_failure_uses_recorded_completed_receipt(original_codex, monkeypatch):
    store, _cwd, wid, tid = original_codex
    target = "codex:" + wid
    _settle_codex(target, wid, tid, "done", "old-turn", "SAFE REPLY")
    store.mutate_record(target, patches={
        "lastTurn": {"turnId": "failed-turn", "status": "failed"}})

    def unreadable(*args, **kwargs):
        raise SessionUnavailable("native history unavailable")
    monkeypatch.setattr(codex_runtime, "read_thread", unreadable)
    selected = ops_report.report_worker(target)
    assert selected["text"] == "SAFE REPLY"
    assert selected["note"] == "native history unavailable; showing the last recorded reply"


def test_claude_compact_local_command_is_not_latest_report(owners, tmp_path, monkeypatch):
    _root, store = owners
    target = "claude:" + A
    source = _claude_sources(store, monkeypatch, tmp_path)
    _claude_line(source, {"type": "user", "message": {"content": "prompt"}})
    _claude_line(source, {"type": "assistant", "message": {
        "content": [{"type": "text", "text": "answer before compact"}]}})
    _claude_line(source, {"type": "user", "message": {"content": "/compact"}})
    _claude_line(source, {"type": "user", "message": {
        "content": "<command-name>/compact</command-name><command-args></command-args>"}})
    _claude_line(source, {"type": "user", "message": {
        "content": "<local-command-stdout>Compacted</local-command-stdout>"}})
    selected = ops_report.report_worker(target)
    assert selected["text"] == "answer before compact"
