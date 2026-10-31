"""Trimmed owner-approved report wording and native turn selection; no providers."""
from __future__ import annotations
import ccx.ops.report as ops_report
import json
import pytest
from ccx.cli import main as cli
from ccx.codex import runtime as codex_runtime
from ccx.cli.commands import _common as commands
from ccx.ops import common as protocol_ops
from ccx.errors import SessionUnavailable
from test_generation_store import A, B, short_root  # noqa: F401
from test_explorer_findings import both, run_cli  # noqa: F401
from test_worker_telemetry import original_codex, original_sources  # noqa: F401


@pytest.mark.parametrize("status,text,expected", [
    ("completed", "", "(turn completed; no reply)"),
    ("failed", "", "(turn failed; no reply)"),
    ("interrupted", "", "(turn interrupted; no reply)"),
    ("completed", "REAL reply", "REAL reply"),
])
def test_plain_report_markers_do_not_replace_json_empty_text(
    both, capsys, monkeypatch, status, text, expected
):
    called = []
    def fake_report(*a, **kw):
        called.append((a, kw))
        return {
            "worker": f"claude:{A}", "status": status, "text": text,
            "has_final_message": bool(text), "outcome": (
                "interrupted" if status == "interrupted" else None),
            "source": "native-transcript-latest"}
    monkeypatch.setattr(ops_report, "report_worker", fake_report)
    plain = run_cli(capsys, "report", A)
    assert called
    assert plain.out.strip() == expected
    json_out = json.loads(run_cli(capsys, "report", A, "--json").out)
    assert json_out["text"] == text and json_out["has_final_message"] == bool(text)


def _thread(store, turns):
    source = store.codex_source(f"codex:{B}")
    return {"id": store.record(B)["native"]["threadId"],
            "threadSource": source, "turns": turns}



def test_report_failed_worker_without_native_failed_turn_proof(both, capsys):
    both.mutate_record(B, patches={
        "lifecycle.state": "crashed",
        "lifecycle.activeTurnId": "native-unsettled"})
    value = run_cli(capsys, "report", B, exit_code=3)
    assert "busy" not in value.err
    obj = json.loads(run_cli(capsys, "report", B, "--json", exit_code=3).out)
    assert obj["errors"]


def test_report_no_turn_yet_both_engines(both, capsys):
    for target in (A, B):
        value = run_cli(capsys, "report", target, exit_code=3)
        assert "no conversation turn yet" in value.err


def test_report_busy_without_completed_reply(both, capsys):
    both.mutate_record(B, patches={"lifecycle.state": "busy"})
    value = run_cli(capsys, "report", B, exit_code=3)
    assert "no completed reply yet; a turn is still running" in value.err


def test_report_native_selection_and_compaction_from_verified_source(original_codex, monkeypatch):
    store, _cwd, wid, tid = original_codex
    handle = "codex:" + wid
    assert wid != B or tid
    store.mutate_record(handle, patches={
        "lifecycle.state": "idle", "lastCompact": {"turnId": "t-compact"}})
    thread = {"id": tid, "threadSource": store.codex_source(handle), "turns": [
        {"id": "t-user", "status": "completed", "items": [
            {"type": "agentMessage", "phase": "final_answer", "text": "GENUINE"}]},
        {"id": "t-compact", "status": "completed", "items": [
            {"type": "contextCompaction", "id": "compaction-item"}]}]}
    monkeypatch.setattr(codex_runtime, "read_thread", lambda *_a, **_kw: thread)
    selected = ops_report.report_worker(handle, timeout=2)
    assert selected["text"] == "GENUINE"
    assert selected["outcome"] is None
    assert selected["source"] == "native-transcript-latest"

    thread["turns"] = [{"id": "t-failed", "status": "failed", "items": []}]
    failed = ops_report.report_worker(handle, timeout=2)
    assert failed["status"] == "failed"
    assert failed["text"] == "" and failed["has_final_message"] is False
    assert failed["outcome"] is None

    thread["turns"] = [{"id": "t-interrupted", "status": "interrupted", "items": []}]
    interrupted = ops_report.report_worker(handle, timeout=2)
    assert interrupted["status"] == "interrupted"
    assert interrupted["outcome"] == "interrupted"
