"""Original full-UUID full-conversation reads; sealed fake sources, no providers."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from ccx.codex import evidence as codex_evidence
from ccx.codex import runtime as codex_runtime
from ccx import constants
from ccx.errors import InvalidRequest, SessionNotFound, SessionUnavailable
from ccx.ops.creation_steps import prepare_worker
from ccx.store.store import GenerationStore
from ccx.store.root import require_ccx_active
from ccx.workers import WorkerClient
from test_generation_store import A, B, C, G2, short_root, switch_pointer
from test_worker_telemetry import original_codex, original_sources, _codex_turn


def _append(path: Path, *events: dict) -> None:
    with path.open("a", encoding="utf-8") as stream:
        for event in events:
            stream.write(json.dumps(event, ensure_ascii=False) + "\n")


def _ordered(turn: dict) -> list[dict]:
    return sorted(turn["input"] + turn["output"], key=lambda item: item["ordinal"])


def _entry(kind: str, text: str, *, uuid: str) -> dict:
    return {"type": kind, "uuid": uuid, "message": {
        "role": kind, "content": (text if kind == "user" else
                                 [{"type": "text", "text": text}]),
        **({"model": "fake-original-claude"} if kind == "assistant" else {}),
    }}


def test_full_conversation_claude_same_short_distinct_sid_complete_eof_and_tool_bounds(
    original_sources,
):
    store, _ = original_sources
    rec_a, rec_b = store.record(A), store.record(B)
    assert A[:8] == B[:8] and rec_a["native"]["sessionId"] != rec_b["native"]["sessionId"]
    assert rec_a["native"]["sessionId"] != A
    payload = "é" * 5500
    native_a = Path(rec_a["transcript"])
    _append(
        native_a, _entry("user", "prompt A", uuid="u-a"),
        {"type": "assistant", "uuid": "a-tool", "message": {
            "role": "assistant", "model": "fake-original-claude", "content": [
                {"type": "tool_use", "id": "tool-a", "name": "Read", "input": {"file_path": "a"}},
            ]}},
        {"type": "user", "uuid": "result-a", "message": {"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "tool-a", "content": payload},
        ]}},
        _entry("assistant", "final A", uuid="a-final"),
    )
    # A valid but unfinished JSON object must NOT become published content.
    with native_a.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(_entry("assistant", "UNCOMMITTED TAIL", uuid="partial")))
    _append(Path(rec_b["transcript"]), _entry("assistant", "only B", uuid="b1"))
    store.mutate_record(A, patches={"lifecycle.state": "dead"})
    client = WorkerClient()
    full = client.conversation(A)
    assert full["schemaVersion"] == 5
    assert full["worker"]["handle"] == f"claude:{A}"
    assert full["worker"]["short"] == A[:8]
    assert full["worker"]["nativeSessionId"] == rec_a["native"]["sessionId"]
    assert full["state"] == "stopped"
    assert full["completeness"]["status"] == "complete"
    assert full["evidence"]["status"] == "absent"
    assert full["nativeArtifacts"][0]["path"] == str(native_a)
    assert "/proc/self/fd/" not in json.dumps(full)
    assert "UNCOMMITTED TAIL" not in json.dumps(full)
    items = [item for turn in full["turns"] for item in _ordered(turn)]
    call = next(item for item in items if item["kind"] == "tool-call")
    assert call["result"]["content"] == payload and not call["result"]["truncated"]
    assert [item["ordinal"] for item in _ordered(full["turns"][0])] == list(
        range(len(_ordered(full["turns"][0])))
    )
    clipped = client.conversation(f"claude:{A}", max_tool_result_bytes=5)
    trimmed = next(item for turn in clipped["turns"] for item in _ordered(turn)
                   if item["kind"] == "tool-call")["result"]
    assert trimmed["content"] == "éé" and trimmed["returnedByteCount"] == 4
    assert trimmed["originalByteCount"] == len(payload.encode())
    assert trimmed["contentSha256"] == hashlib.sha256(payload.encode()).hexdigest()
    assert clipped["completeness"]["status"] == "bounded"
    zero = client.conversation(A, max_tool_result_bytes=0)
    assert next(item for turn in zero["turns"] for item in _ordered(turn)
                if item["kind"] == "tool-call")["result"]["content"] == ""
    other = client.conversation(f"claude:{B}")
    assert other["worker"]["nativeSessionId"] == rec_b["native"]["sessionId"]
    assert "only B" in json.dumps(other) and "only B" not in json.dumps(full)
    for wid in (A, B):
        assert not (store.worker_path(wid).parent / "transcript.json").exists()
        assert not store.worker_path(wid, "context-observation.json").exists()
    assert not (constants.CCX_ROOT / "sessions").exists()
    with pytest.raises(TypeError, match="detail"):
        client.conversation(A, detail="full")
    with pytest.raises(TypeError, match="detail"):
        client.export_conversation(A, "unused", detail="full")
    for bad in ({"max_tool_result_bytes": -1}, {"max_tool_result_bytes": True}):
        with pytest.raises(InvalidRequest):
            client.conversation(A, **bad)


def test_full_conversation_codex_original_history_timing_foreign_and_known_empty(
    original_codex, monkeypatch,
):
    store, cwd, wid, tid = original_codex
    thread = _codex_turn(tid, wid)
    turn = thread["turns"][0]
    turn["items"].insert(1, {"id": "cmd", "type": "commandExecution",
                              "command": "printf ok", "status": "completed",
                              "aggregatedOutput": "ok", "exitCode": 0})
    turn["items"].insert(2, {"id": "hidden", "type": "reasoning",
                              "content": [{"type": "reasoningText", "text": "SECRET"}],
                              "summary": [{"type": "summaryText", "text": "safe summary"}]})
    calls = []

    def history(thread_id, *, cwd, timeout, worker_id, expected_source):
        assert expected_source == store.codex_source(wid)
        calls.append((thread_id, cwd, timeout, worker_id))
        assert (thread_id, worker_id) == (tid, wid)
        return thread

    monkeypatch.setattr(codex_runtime, "read_thread", history)
    journal = codex_evidence.EvidenceJournal(wid)
    journal.record("turn/started", {"turn": {"id": "native-turn-1"}}, thread_id=tid)
    journal.record("item/started", {
        "turnId": "native-turn-1", "startedAtMs": 1710000000123,
        "item": {"id": "cmd", "type": "commandExecution"},
    }, thread_id=tid)
    journal.record("item/completed", {
        "turnId": "native-turn-1", "completedAtMs": 1710000000456,
        "item": {"id": "cmd", "type": "commandExecution"},
    }, thread_id=tid)
    journal.record("turn/completed", {
        "turn": {"id": "native-turn-1", "status": "completed"},
    }, thread_id=tid)
    journal.close()

    client = WorkerClient()
    doc = client.conversation(wid)
    assert calls == [(tid, str(cwd), 120, wid)]
    assert doc["schemaVersion"] == 5
    assert doc["worker"]["handle"] == f"codex:{wid}"
    assert doc["worker"]["nativeSessionId"] == tid
    assert doc["evidence"]["status"] == "complete"
    items = _ordered(doc["turns"][0])
    assert [i["id"] for i in items if i["id"] in ("native-user-1", "cmd", "native-final-1")] == [
        "native-user-1", "cmd", "native-final-1",
    ]
    command = next(item for item in items if item["id"] == "cmd")
    assert command["startedAtMs"] == 1710000000123
    assert command["completedAtMs"] == 1710000000456
    assert command["timingSource"] == "ccx-live-item-notification"
    assert "SECRET" not in json.dumps(doc)

    # Only positively complete original evidence can prove native history omitted an item.
    journal = codex_evidence.EvidenceJournal(wid)
    journal.record("item/started", {
        "turnId": "native-turn-1", "startedAtMs": 1710000000777,
        "item": {"id": "missing-native-item", "type": "commandExecution"},
    }, thread_id=tid)
    journal.close()
    # Simulate a source-limited thread/read that omits native items observed
    # in the complete original journal, without changing journal identities.
    original_items = turn["items"]
    turn["items"] = original_items[:1]
    limited = client.conversation(wid)
    turn["items"] = original_items
    assert limited["completeness"]["status"] == "source-limited"
    assert any("omits live turn items" in str(w) for w in limited["completeness"]["warnings"])

    journal = codex_evidence.EvidenceJournal(wid)
    journal.record("item/started", {
        "turnId": "native-turn-1", "startedAtMs": 9999999999999,
        "item": {"id": "native-final-1", "type": "agentMessage"},
    }, thread_id="foreign-thread")
    journal.close()
    filtered = client.conversation(wid)
    assert filtered["evidence"]["status"] == "degraded"
    assert filtered["completeness"]["status"] == "complete"
    final = next(item for item in _ordered(filtered["turns"][0])
                 if item["id"] == "native-final-1")
    assert final.get("startedAtMs") is None

    store.worker_path(wid, "evidence.jsonl").unlink()
    absent = client.conversation(wid)
    assert absent["evidence"]["status"] == "absent"
    assert absent["completeness"]["status"] == "complete"
    store.mutate_record(wid, patches={"historyKnownEmpty": True})
    calls.clear()
    empty = client.conversation(wid)
    assert empty["schemaVersion"] == 5 and empty["turns"] == []
    assert empty["completeness"]["status"] == "complete"
    assert calls == []


def test_full_conversation_typed_invalid_missing_source_binding_foreign_and_generation(
    original_codex, monkeypatch,
):
    store, cwd, wid, tid = original_codex
    client = WorkerClient()
    for value in (A[:8], "display is not a key", "native-thread-xyz"):
        with pytest.raises(InvalidRequest):
            client.conversation(value)
    with pytest.raises(SessionNotFound):
        client.conversation("cccccccc-1234-4000-8000-000000000009")
    Path(store.record(A)["transcript"]).unlink()
    with pytest.raises(SessionNotFound):
        client.conversation(A)  # Owned but original native transcript missing.
    native = store.record(A)["native"]["sessionId"]
    with pytest.raises(SessionNotFound):
        client.conversation(native)  # Native ID is not a CCX identity.

    recorded = GenerationStore.record

    def unbound(self, target):
        row = recorded(self, target)
        if row["engine"] == "codex":
            row["native"] = {**row["native"], "threadSource": "ccx-worker:other"}
        return row

    with monkeypatch.context() as patch:
        patch.setattr(GenerationStore, "record", unbound)
        with pytest.raises(SessionUnavailable, match="original codex conversation source unavailable"):
            client.conversation(wid)

    # Exercise real runtime read_thread identity gate with fake daemon replies.
    monkeypatch.setattr(codex_runtime, "observation_daemon", lambda *_a, **_k: True)
    monkeypatch.setattr(codex_runtime, "daemon_call", lambda *_a, **_k: {
        "ok": True, "workerId": wid, "threadId": tid,
        "thread": {"id": tid, "threadSource": "ccx-worker:foreign", "turns": []},
    })
    with pytest.raises(SessionUnavailable, match="belongs to another worker"):
        client.conversation(wid)

    # The Claude source opener refuses a symlink; no legacy JSON/cache fallback.
    path = Path(store.record(B)["transcript"])
    path.unlink()
    path.symlink_to(store.record(A)["transcript"])
    with pytest.raises(SessionUnavailable, match="original claude conversation source unavailable"):
        client.conversation(B)
    path.unlink()
    root = constants.CCX_ROOT
    switch_pointer(root, G2)
    new_store = require_ccx_active(root)
    prepare_worker(new_store, engine="claude", cwd=str(cwd), worker_id=A)
    with pytest.raises(SessionUnavailable, match="generation"):
        client.conversation(A)
