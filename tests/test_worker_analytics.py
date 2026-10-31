"""Shipped Claude analytics on original full-UUID owners; synthetic native files only."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from ccx.claude import analytics
from ccx.cli import main as cli
from ccx.cli.commands import _common as commands
from ccx.store import paths
from ccx.ops.creation_steps import prepare_worker
from ccx.store.root import StorageIntegrityError, _write_json
from test_generation_store import A, B, C, short_root as short_root  # noqa: F401 - fixture
from test_worker_telemetry import (
    _append_jsonl, _claude_entries, _private, original_sources,  # noqa: F401 - fixture
)


def test_analytics_cli_removed_but_telemetry_remains(original_sources):
    from ccx.cli.parser import build_parser
    from ccx.ops.report import telemetry_worker
    store, _cwd = original_sources
    native = Path(store.record(A)["transcript"])
    _append_jsonl(native, *_claude_entries("a", 11))
    with pytest.raises(SystemExit):
        build_parser().parse_args(["analytics", A])
    before = store.record(A)
    page = telemetry_worker(A)
    assert page["turns"][0]["tokens"]["input"] == 11
    assert page["totals"]["tokens"]["input"] == 11
    assert store.record(A) == before
    assert "input_tokens" not in next(row for row in store.list_workers()
                                      if row["worker_id"] == A)
    from ccx.client import CcxClient
    assert "input_tokens" not in CcxClient().list()[0].as_dict()
    with pytest.raises(StorageIntegrityError):
        store.worker_path(A, "analytics.json")
    with pytest.raises(StorageIntegrityError):
        store.read_worker_sidecar(A, "analytics.json")


def test_analytics_owned_claude_fork_subagent_usage_and_lifecycle_stats(
    original_sources,
):
    store, cwd = original_sources
    source = store.record(A)
    anchor = {
        "engine": "claude", "sourceWorkerId": A,
        "sourceNativeSessionId": source["native"]["sessionId"],
        "resumeAt": "copied-final", "transcript": source["transcript"],
        "cwd": str(cwd), "createArgs": source["createArgs"],
        "seed": {"stats": {"turns": 0}, "lastTurnAt": None},
        "overrides": {"model": None, "effort": None}, "forkedAtMs": 1000,
    }
    prepared = prepare_worker(
        store, engine="claude", cwd=str(cwd), worker_id=C, fork_anchor=anchor,
    )
    sid = prepared.record["native"]["sessionId"]
    assert sid not in (A, B, C, source["native"]["sessionId"])
    original = Path(store.record(C)["transcript"])
    _private(original.parent)
    inherited = _claude_entries("source-owned", 999)
    inherited_raw = b"".join((json.dumps(event) + "\n").encode() for event in inherited)
    current = [
        {"type": "user", "timestamp": "2026-01-01T00:00:00Z", "promptId": "p1",
         "message": {"content": "child prompt"}},
        {"type": "assistant", "timestamp": "2026-01-01T00:00:01Z", "message": {
            "id": "m1", "model": "claude-old", "usage": {
                "input_tokens": 4, "output_tokens": 2, "cache_read_input_tokens": 6,
            }, "content": [{"type": "tool_use", "name": "Bash", "id": "t1"}]}},
        {"type": "assistant", "timestamp": "2026-01-01T00:00:02Z", "message": {
            "id": "m1", "model": "claude-main", "usage": {
                "output_tokens": 3, "cache_creation_input_tokens": 2,
            }, "content": [{"type": "tool_use", "name": "Bash", "id": "t1"},
                           {"type": "tool_use", "name": "AskUserQuestion", "id": "q1"}]}},
        {"type": "user", "timestamp": "2026-01-01T00:00:03Z", "message": {
            "content": [{"type": "tool_result", "is_error": True, "tool_use_id": "t1"}]}},
        {"type": "queue-operation", "timestamp": "2026-01-01T00:00:04Z",
         "operation": "enqueue"},
        {"type": "attachment", "timestamp": "2026-01-01T00:00:05Z", "message": {
            "content": [{"type": "queued_command", "prompt": "steer"}]}},
    ]
    original.write_bytes(inherited_raw + b"".join(
        (json.dumps(event) + "\n").encode() for event in current
    ) + b'{"broken"\n' + b'{"type":"assistant","message":')
    original.chmod(0o600)
    proof = {
        "workerId": C, "nativeSessionId": sid, "forkAnchor": anchor,
        "copiedAtMs": 1001, "inheritedBytes": len(inherited_raw),
        "inheritedOpenTaskIds": [],
    }
    store.mutate_record(C, patches={
        "lifecycle.nativeForkProof": proof, "lifecycle.state": "idle",
        "stats.turns": 7, "createArgs.model": "opus",
    })
    sub = paths.subagent_dir_for(str(cwd), sid) / "agent-a.jsonl"
    _private(sub.parent)
    _append_jsonl(sub,
        {"type": "user", "promptId": "p1", "attributionAgent": "Explore",
         "message": {"content": "sub prompt"}},
        {"type": "assistant", "promptId": "p1", "attributionAgent": "Explore",
         "message": {"id": "s1", "model": "claude-sub", "usage": {
             "input_tokens": 7, "output_tokens": 8,
         }, "content": [{"type": "text", "text": "sub result"}]}},
    )
    sub.chmod(0o600)

    doc = analytics.compute_session_analytics({
        **store.record(C), "short": C[:8], "sessionId": sid,
    })
    assert doc["sessionId"] == sid
    assert doc["totals"]["turns"] == 1 and len(doc["prompts"]) == 1
    assert doc["totals"]["tokens"] == {
        "input": 11, "output": 11, "cacheRead": 6, "cacheCreation": 2,
    }
    assert doc["totals"]["tools"] == {"Bash": 1, "AskUserQuestion": 1}
    assert doc["totals"]["toolErrors"] == 1
    assert doc["totals"]["askUserQuestions"] == 1
    assert doc["totals"]["steering"] == {"enqueued": 1, "boundary": 1, "idle": 0}
    assert doc["totals"]["activeMs"] == 5000
    assert doc["totals"]["subagents"]["byAgent"]["Explore"]["tokens"]["input"] == 7
    assert doc["totals"]["byModel"]["claude-main"]["tokens"]["input"] == 4
    assert doc["totals"]["byModel"]["claude-main"]["events"] == 1
    assert doc["totals"]["byModel"]["claude-sub"]["level"] == "subagent"
    assert doc["stats"]["malformedLines"] == 2
    assert doc["stats"]["subagentFilesUnreadable"] == 0
    assert "source-owned" not in json.dumps(doc) and str(original) not in json.dumps(doc)
    stats = store.record(C)["stats"]
    assert stats == {"turns": 7}
    assert store.record(C)["createArgs"]["model"] == "opus"
    assert store.record(C)["lifecycle"]["state"] == "idle"
    assert not (store.worker_path(C, "record.json").parent / "analytics.json").exists()


def test_old_analytics_fields_inert(original_sources):
    store, _cwd = original_sources
    old = store.record(A)
    old["analytics"] = {"sampleCount": 3}
    with store.lease():
        with store._gen("workers", "claude", A) as fd:
            _write_json(fd, "record.json", old)
            _write_json(fd, "analytics.json", {"legacy": True})
    before = store.record(A)
    from ccx.ops.report import telemetry_worker
    telemetry_worker(A)
    assert store.record(A) == before
    assert (store.worker_path(A, "record.json").parent / "analytics.json").exists()
    with pytest.raises(StorageIntegrityError):
        store.mutate_record(A, patches={"analytics.sampleCount": 4})
