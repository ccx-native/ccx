"""Source-bound original full-UUID worker telemetry, synthetic native evidence only."""
from __future__ import annotations
import ccx.ops.report as ops_report

from contextlib import contextmanager
import json
from pathlib import Path

import pytest

from ccx.claude import telemetry as claude_telemetry
from ccx.codex import evidence as codex_evidence
from ccx.codex import runtime as codex_runtime
from ccx.codex import telemetry as codex_telemetry
from ccx import constants
from ccx.store import paths
from ccx.ops import common as protocol_ops
from ccx.errors import SessionUnavailable
from ccx.rpc.local_rpc import dispatch_full_uuid
from ccx.ops.creation_steps import prepare_worker, activate_prepared_worker
from ccx.store.root import require_ccx_active
from test_generation_store import A, B, short_root
from test_v1_worker_protocol import request as v1_request


def _private(path: Path):
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.chmod(0o700)


@pytest.fixture
def original_sources(short_root, tmp_path, monkeypatch):
    """Genuine generation creation/claim; no native executable or runtime."""
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    monkeypatch.setenv("CCX_ROOT", str(short_root))
    home = tmp_path / "claude-home"
    _private(home)
    monkeypatch.setattr(paths, "CLAUDE_HOME", home)
    cwd = tmp_path / "project"
    _private(cwd)
    store = require_ccx_active(short_root)
    prepare_worker(store, engine="claude", cwd=str(cwd), worker_id=A)
    prepare_worker(store, engine="claude", cwd=str(cwd), worker_id=B)
    for wid in (A, B):
        path = Path(store.record(wid)["transcript"])
        _private(path.parent)
        path.write_text("", encoding="utf-8")
        path.chmod(0o600)
    return store, cwd


def _claude_entries(prompt: str, value: int):
    return [
        {"type": "user", "promptId": prompt,
         "timestamp": "2026-10-21T00:00:00Z",
         "message": {"content": [{"type": "text", "text": "private " + prompt}]}},
        {"type": "assistant", "timestamp": "2026-10-21T00:00:01Z",
         "message": {"id": "response-" + prompt, "model": "claude-fake",
                     "usage": {"input_tokens": value, "output_tokens": 2},
                     "content": [{"type": "text", "text": "synthetic reply"}]}},
    ]


def _append_jsonl(path, *rows):
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")


def test_claude_original_pinned_analytics_full_uuid_cursor_and_source(original_sources):
    store, _ = original_sources
    source_a = Path(store.record(A)["transcript"])
    source_b = Path(store.record(B)["transcript"])
    _append_jsonl(source_a, *_claude_entries("prompt-a", 11))
    _append_jsonl(source_b, *_claude_entries("prompt-b", 33))
    record = store.worker_path(A, "record.json")
    before = (record.read_bytes(), record.stat().st_mtime_ns)

    first = ops_report.telemetry_worker(A, limit=1)
    assert first["worker"]["short"] == A[:8]
    assert first["worker"]["session_id"] == store.record(A)["native"]["sessionId"]
    assert first["turns"][0]["telemetry_id"] == "claude-prompt:prompt-a"
    assert first["turns"][0]["turn_id"] is None
    assert first["turns"][0]["tokens"]["input"] == 11
    assert first["turns"][0]["coverage"]["identity"] == "analytics-prompt"
    assert "private prompt-a" not in json.dumps(first)
    assert str(source_a) not in json.dumps(first)
    assert (record.read_bytes(), record.stat().st_mtime_ns) == before
    via_v1 = dispatch_full_uuid(
        v1_request("worker.telemetry", A, limit=1), store,
    )
    assert via_v1["ok"] is True
    assert via_v1["result"]["turns"][0]["telemetry_id"] == "claude-prompt:prompt-a"
    assert via_v1["result"]["turns"][0]["turn_id"] is None
    assert (record.read_bytes(), record.stat().st_mtime_ns) == before

    # The same eight display characters are not the cursor owner.
    foreign = ops_report.telemetry_worker(
        B, after_cursor=first["next_cursor"],
    )
    assert foreign["coverage"]["gap"]["reason"] == "cursor_worker_mismatch"
    assert foreign["turns"][0]["prompt_id"] == "prompt-b"

    _append_jsonl(source_a, *_claude_entries("prompt-new", 17))
    appended = ops_report.telemetry_worker(
        A, after_cursor=first["next_cursor"],
    )
    assert [x["prompt_id"] for x in appended["turns"]] == ["prompt-new"]
    assert appended["coverage"]["gap"] is None

    # Pinned last-complete-line boundary excludes an unfinished native row.
    with source_a.open("a") as handle:
        handle.write('{"type":"user","promptId":"incomplete"')
    tail = ops_report.telemetry_worker(A)
    assert [x["prompt_id"] for x in tail["turns"]] == [
        "prompt-a", "prompt-new",
    ]

    # Enrichment of a previously acknowledged prompt forces explicit resync.
    source_a.write_text(
        "".join(json.dumps(x) + "\n" for x in (
            *_claude_entries("prompt-a", 12),
            *_claude_entries("prompt-new", 17),
        )), encoding="utf-8",
    )
    enriched = ops_report.telemetry_worker(
        A, after_cursor=appended["next_cursor"],
    )
    assert enriched["coverage"]["gap"]["reason"] == "cursor_prompt_prefix_changed"
    assert enriched["turns"][0]["tokens"]["input"] == 12
    # Source-swapped native inode is rejected, not read from a short-key fallback.
    source_a.unlink()
    source_a.symlink_to(source_b)
    with pytest.raises(SessionUnavailable, match="Cannot read Claude activity"):
        ops_report.telemetry_worker(A)

    source_b.unlink()
    missing = ops_report.telemetry_worker(B)
    assert missing["turns"] == []
    assert missing["coverage"]["status"] == "partial"
    assert any("has not appeared" in warning
               for warning in missing["coverage"]["warnings"])


def _codex_turn(tid: str, wid: str, turn_id: str = "native-turn-1"):
    return {
        "id": tid, "threadSource": f"ccx-worker:{wid}",
        "turns": [{
            "id": turn_id, "status": "completed",
            "startedAt": 1789818000, "completedAt": 1789818001,
            "items": [
                {"id": "native-user-1", "type": "userMessage",
                 "content": [{"type": "text", "text": "synthetic instruction"}]},
                {"id": "native-final-1", "type": "agentMessage",
                 "text": "synthetic", "phase": "final_answer"},
            ],
        }],
    }


@pytest.fixture
def original_codex(original_sources):
    store, cwd = original_sources
    # A distinct original Codex worker, no actual native daemon/provider call.
    from test_generation_store import C
    prepared = prepare_worker(store, engine="codex", cwd=str(cwd), worker_id=C)
    tid = "original-native-thread-1"

    def fake_admission(_budget):
        native = store.native_activation(C)
        return {
            "accepted": True, "workerId": C, "engine": "codex",
            "source": f"ccx-worker:{C}",
            "submissionId": native["submissionId"],
            "nativeEvidenceSha256": "a" * 64,
            "nativeBinding": {"threadId": tid},
        }

    confirmed = activate_prepared_worker(
        store, prepared, bounded_native_submit=fake_admission,
    )
    assert confirmed.activation_state == "confirmed"
    assert store.record(C)["native"]["threadSource"] == f"ccx-worker:{C}"
    return store, cwd, C, tid


def _journal_turn(wid, tid):
    journal = codex_evidence.EvidenceJournal(wid)
    journal.record(
        "turn/started", {"turn": {"id": "native-turn-1"}}, thread_id=tid,
    )
    journal.record(
        "thread/tokenUsage/updated",
        {"turnId": "native-turn-1",
         "tokenUsage": {
             "last": {"inputTokens": 25, "cachedInputTokens": 4,
                      "outputTokens": 3, "reasoningOutputTokens": 2},
             "total": {"inputTokens": 25, "cachedInputTokens": 4,
                       "outputTokens": 3, "reasoningOutputTokens": 2,
                       "totalTokens": 28},
             "modelContextWindow": 1000,
         }},
        thread_id=tid,
    )
    journal.record(
        "turn/completed",
        {"turn": {"id": "native-turn-1", "status": "completed"}},
        thread_id=tid,
    )
    journal.close()
    return journal


def test_codex_original_thread_evidence_cursor_upsert_filter_and_v1(
    original_codex, monkeypatch,
):
    store, _cwd, wid, tid = original_codex
    valid_thread = _codex_turn(tid, wid)
    calls = []

    def history(requested, **kw):
        calls.append((requested, kw))
        assert requested == tid
        assert kw["worker_id"] == wid
        return valid_thread

    monkeypatch.setattr(codex_runtime, "read_thread", history)
    journal = _journal_turn(wid, tid)
    record = store.worker_path(wid, "record.json")
    source = store.worker_path(wid, "evidence.jsonl")
    baseline = [(f.read_bytes(), f.stat().st_mtime_ns) for f in (record, source)]

    first = ops_report.telemetry_worker(f"codex:{wid}")
    assert first["worker"]["short"] == wid[:8]
    assert first["worker"]["thread_id"] == tid
    assert first["turns"][0]["turn_id"] == "native-turn-1"
    assert first["turns"][0]["tokens"]["input"] == 25
    assert first["turns"][0]["context_after"]["tokens_used"] == 25
    assert first["coverage"]["status"] == "complete"
    assert first["turns"][0]["sources"]
    assert len(calls) == 1
    cursor = codex_telemetry._parse_cursor(first["next_cursor"])
    assert cursor["short"] == wid  # cursor authority is UUID, not display short
    foreign_cursor = codex_telemetry._cursor(
        B, cursor["journal_instance_id"], cursor["sequence"],
    )
    foreign = ops_report.telemetry_worker(
        wid, after_cursor=foreign_cursor,
    )
    assert foreign["coverage"]["gap"]["reason"] == "cursor_worker_mismatch"
    changed_journal_cursor = codex_telemetry._cursor(
        wid, "other-journal-instance", cursor["sequence"],
    )
    stale_journal = ops_report.telemetry_worker(
        wid, after_cursor=changed_journal_cursor,
    )
    assert stale_journal["coverage"]["gap"]["reason"] == "cursor_journal_mismatch"

    page = dispatch_full_uuid(
        v1_request("worker.telemetry", wid, limit=200), store,
    )
    assert page["ok"] is True, page
    assert page["result"]["worker"] == f"codex:{wid}"
    assert page["result"]["turns"][0]["turn_id"] == "native-turn-1"
    for params in (
        {"target": wid, "after_cursor": "not-a-native-cursor"},
        {"target": wid, "limit": 0},
        {"target": wid[:8], "limit": 1},
    ):
        bad = dispatch_full_uuid(v1_request("worker.telemetry", **params), store)
        assert bad["ok"] is False
        assert bad["error"]["code"] == "invalid_request", bad
    assert [(f.read_bytes(), f.stat().st_mtime_ns) for f in (record, source)] == baseline

    # Later ORIGINAL native usage reemits the already returned turn for upsert.
    journal = codex_evidence.EvidenceJournal(wid)
    journal.record(
        "thread/tokenUsage/updated",
        {"turnId": "native-turn-1",
         "tokenUsage": {
             "last": {"inputTokens": 30, "cachedInputTokens": 4,
                      "outputTokens": 3, "reasoningOutputTokens": 2},
             "total": {"inputTokens": 30, "cachedInputTokens": 4,
                       "outputTokens": 3, "reasoningOutputTokens": 2,
                       "totalTokens": 33},
             "modelContextWindow": 1000,
         }},
        thread_id=tid,
    )
    journal.close()
    updated = ops_report.telemetry_worker(
        wid, after_cursor=first["next_cursor"],
    )
    assert [row["turn_id"] for row in updated["turns"]] == ["native-turn-1"]
    assert updated["turns"][0]["tokens"]["input"] == 30

    # A foreign native event with coincident turnId cannot enrich this worker.
    journal = codex_evidence.EvidenceJournal(wid)
    journal.record(
        "turn/completed", {"turn": {"id": "native-turn-1", "status": "failed"}},
        thread_id="foreign-thread",
    )
    journal.close()
    filtered = ops_report.telemetry_worker(wid)
    assert filtered["coverage"]["status"] == "partial"
    assert any("original Codex thread" in x for x in filtered["coverage"]["warnings"])
    assert filtered["turns"][0]["outcome"] == "completed"

    # A malformed/nonexistent native source is not guessed or used as a fallback.
    with pytest.raises(Exception):
        ops_report.telemetry_worker(wid[:8])
    assert store.record(wid)["native"]["threadId"] == tid


def test_read_thread_r1_served_and_fallback_source_and_v1_capabilities(
    original_codex, monkeypatch,
):
    store, cwd, wid, tid = original_codex
    good = _codex_turn(tid, wid)
    captured = []

    monkeypatch.setattr(codex_runtime, "observation_daemon",
                        lambda *_a, **_kw: True)

    def daemon(message, **kw):
        captured.append(message)
        assert message["op"] == "thread-history"
        return {"ok": True, "workerId": wid, "threadId": tid,
                "thread": {**good, "historySource": "thread/turns/list"}}

    monkeypatch.setattr(codex_runtime, "daemon_call", daemon)
    result = codex_runtime.read_thread(
        tid, cwd=str(cwd), worker_id=wid, timeout=5,
        expected_source=store.codex_source(wid),
    )
    assert result["turns"] == good["turns"]
    assert result["historySource"] == "thread/turns/list"
    assert len(captured) == 1
    assert {key: val for key, val in captured[0].items()
            if key != "observeSeconds"} == {
        "op": "thread-history", "threadId": tid,
        "includeTurns": False, "workerId": wid,
    }
    assert 0 < captured[0]["observeSeconds"] <= 5
    with pytest.raises(SessionUnavailable, match="belongs to another worker"):
        monkeypatch.setattr(codex_runtime, "daemon_call", lambda *_a, **_kw: {
            "ok": True, "workerId": "wrong-worker", "threadId": tid,
            "thread": good,
        })
        codex_runtime.read_thread(
            tid, cwd=str(cwd), worker_id=wid, expected_source=store.codex_source(wid))

    monkeypatch.setattr(codex_runtime, "observation_daemon",
                        lambda *_a, **_kw: False)

    @contextmanager
    def short_lived(**_kwargs):
        class Stub:
            def request(self, method, payload, **_kw):
                assert method == "thread/read"
                assert payload == {"threadId": tid, "includeTurns": False}
                return {"thread": {**good, "threadSource": "foreign-source"}}
        yield Stub()
    monkeypatch.setattr(codex_runtime, "_short_lived_app_server", short_lived)
    with pytest.raises(SessionUnavailable, match="belongs to another worker"):
        codex_runtime.read_thread(
            tid, cwd=str(cwd), worker_id=wid, expected_source=store.codex_source(wid))

    capabilities = dispatch_full_uuid(
        v1_request("system.info"), store,
    )
    assert capabilities["ok"] is True
    assert set(capabilities["result"]) == {"protocol_version", "ccx_version"}
    assert capabilities["result"]["protocol_version"] == 2


def test_codexclient_typed_original_telemetry_cursor_owner_and_error_boundary(
    original_codex, monkeypatch,
):
    """Stage B public page uses Stage A original source, not short-keyed lookup."""
    from dataclasses import FrozenInstanceError
    import inspect

    from ccx import CodexClient
    from ccx.codex import client as codex_client
    from ccx.codex import telemetry as codex_telemetry
    from ccx.codex.types import CodexTelemetryPage
    from ccx.errors import InvalidRequest
    from ccx.store.root import StorageIntegrityError

    store, _cwd, wid, tid = original_codex
    _journal_turn(wid, tid)
    record = store.worker_path(wid, "record.json")
    evidence = store.worker_path(wid, "evidence.jsonl")
    untouched = [(p.read_bytes(), p.stat().st_mtime_ns)
                 for p in (record, evidence)]
    actual_core = codex_client.telemetry_worker
    core_calls = []
    history_calls = []

    def core(target, **kwargs):
        core_calls.append((target, dict(kwargs)))
        return actual_core(target, **kwargs)

    def native_history(requested, **kwargs):
        history_calls.append((requested, dict(kwargs)))
        assert requested == tid
        assert kwargs["worker_id"] == wid
        return _codex_turn(tid, wid)

    monkeypatch.setattr(codex_client, "telemetry_worker", core)
    monkeypatch.setattr(codex_runtime, "read_thread", native_history)
    assert list(inspect.signature(CodexClient.telemetry).parameters) == [
        "self", "target", "after_cursor", "limit", "timeout",
    ]

    client = CodexClient()
    first = client.telemetry(wid, limit=200, timeout=7)
    assert type(first) is CodexTelemetryPage
    assert first.short == wid[:8]
    assert first.thread_id == tid
    assert first.engine == "codex"
    assert first.turns[0]["turn_id"] == "native-turn-1"
    assert first.turns[0]["tokens"]["input"] == 25
    assert first.coverage["status"] == "complete"
    assert first.compactions == [] and first.has_more is False
    assert first.as_dict()["turns"] == first.turns
    assert first.as_dict()["next_cursor"] == first.next_cursor
    assert codex_telemetry._parse_cursor(first.next_cursor)["short"] == wid
    assert core_calls == [(
        f"codex:{wid}",
        {"after_cursor": None, "limit": 200, "timeout": 7},
    )]
    assert history_calls[0][1]["worker_id"] == wid
    with pytest.raises(FrozenInstanceError):
        first.short = "other"

    replay = client.telemetry(
        f"codex:{wid}", after_cursor=first.next_cursor, limit=10, timeout=6,
    )
    assert type(replay) is CodexTelemetryPage
    assert replay.turns == [] and replay.compactions == []
    assert replay.has_more is False
    assert core_calls[1] == (
        f"codex:{wid}",
        {"after_cursor": first.next_cursor, "limit": 10, "timeout": 6},
    )
    assert [(p.read_bytes(), p.stat().st_mtime_ns)
            for p in (record, evidence)] == untouched

    # Wrong engine and display prefix must fail BEFORE any telemetry core call.
    for target in (wid[:8], f"claude:{A}", A):
        with pytest.raises(InvalidRequest):
            client.telemetry(target)
    assert len(core_calls) == 2
    assert len(history_calls) == 2

    # Only source/GenerationStore faults are translated to typed unavailable.
    def generation_fault(*_args, **_kwargs):
        raise StorageIntegrityError("original evidence inode no longer owned")

    monkeypatch.setattr(codex_client, "telemetry_worker", generation_fault)
    with pytest.raises(SessionUnavailable, match="Cannot read Codex activity") as unavailable:
        client.telemetry(wid)
    assert isinstance(unavailable.value.__cause__, StorageIntegrityError)

    # Existing typed CcxError must remain intact, never double-wrapped.
    def request_fault(*_args, **_kwargs):
        raise InvalidRequest("invalid original telemetry cursor")

    monkeypatch.setattr(codex_client, "telemetry_worker", request_fault)
    with pytest.raises(InvalidRequest, match="invalid original telemetry cursor"):
        client.telemetry(wid)
    assert [(p.read_bytes(), p.stat().st_mtime_ns)
            for p in (record, evidence)] == untouched
