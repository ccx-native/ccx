"""Full-UUID export effects against accepted original-source fakes, no providers."""
from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path

import pytest

from ccx.codex import evidence as codex_evidence
from ccx.codex import runtime as codex_runtime
from ccx import constants
from ccx.ops import conversation as _conv
from ccx.errors import SessionUnavailable
from ccx.ops.creation_steps import prepare_worker
from ccx.store.root import require_ccx_active
from ccx.workers import WorkerClient
from test_generation_store import A, B, G1, G2, switch_pointer, short_root as short_root
from test_native_conversation import _append, _entry
from test_worker_telemetry import (
    original_codex as original_codex, original_sources as original_sources, _codex_turn,
)


def _read_export(result):
    return json.loads(Path(result["conversation_path"]).read_text())


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def test_export_claude_held_original_inode_raw_hash_and_reference(original_sources, tmp_path,
                                                                  monkeypatch):
    store, _cwd = original_sources
    rec_a, rec_b = store.record(A), store.record(B)
    assert A[:8] == B[:8] and rec_a["native"]["sessionId"] != rec_b["native"]["sessionId"]
    source = Path(rec_a["transcript"])
    payload = "é" * 2800
    _append(
        source, _entry("user", "prompt A", uuid="user-a"),
        {"type": "assistant", "uuid": "tool-a", "message": {
            "role": "assistant", "model": "synthetic", "content": [
                {"type": "tool_use", "id": "call-a", "name": "Read",
                 "input": {"file_path": "a"}},
            ]}},
        {"type": "user", "uuid": "result-a", "message": {
            "role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "call-a", "content": payload},
            ]}},
        _entry("assistant", "final A", uuid="final-a"),
    )
    with source.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(_entry("assistant", "UNCOMMITTED TAIL", uuid="tail")))
    raw = source.read_bytes()

    client = WorkerClient()
    referenced = client.export_conversation(
        A, tmp_path / "reference", include_native=False, max_tool_result_bytes=5,
    )
    clipped = _read_export(referenced)
    assert referenced["native_path"] is None and referenced["native_paths"] == []
    assert not (tmp_path / "reference" / "native").exists()
    assert clipped["nativeArtifacts"] == [{
        "kind": "claude-transcript-jsonl", "path": str(source),
        "byteCount": len(raw), "sha256": None, "availability": "referenced",
    }]
    assert clipped["completeness"]["status"] == "bounded"
    result = next(item["result"] for turn in clipped["turns"]
                  for item in turn["input"] + turn["output"] if item["kind"] == "tool-call")
    assert result["content"] == "éé" and result["returnedByteCount"] == 4
    assert result["originalByteCount"] == len(payload.encode())
    assert result["contentSha256"] == _sha(payload.encode())
    assert "UNCOMMITTED TAIL" not in json.dumps(clipped)

    actual_writer = _conv.write_export
    held_source = []

    def swap_after_pin(document, destination, **kwargs):
        from ccx.claude.transcript import PinnedNativeSource
        assert document["nativeArtifacts"][0]["path"] == str(source)
        assert isinstance(kwargs["native_source"], PinnedNativeSource)
        assert os.fstat(kwargs["native_source"].fd).st_ino == source.stat().st_ino
        held_source.append(kwargs["native_source"])
        # A concurrent replacement of the VERIFIED DISPLAY PATH must not change
        # the native inode copied by the already-pinned exporter.
        source.unlink()
        source.write_text('{"type":"assistant","message":"wrong native"}\n')
        return actual_writer(document, destination, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(_conv, "write_export", swap_after_pin)
        exported = client.export_conversation(f"claude:{A}", tmp_path / "raw")
    assert held_source
    with pytest.raises(OSError):
        os.fstat(held_source[0].fd)  # owner closed the original pin after export
    assert set(exported) == {
        "worker", "engine", "short", "destination", "conversation_path",
        "native_path", "native_paths", "turn_count", "completeness", "evidence",
    }
    assert (exported["worker"], exported["engine"], exported["short"]) == (
        f"claude:{A}", "claude", A[:8],
    )
    assert exported["native_paths"] == [exported["native_path"]]
    assert Path(exported["native_path"]).read_bytes() == raw
    full = _read_export(exported)
    assert full["worker"]["nativeSessionId"] == rec_a["native"]["sessionId"]
    assert full["completeness"]["status"] == "complete"
    assert full["evidence"]["status"] == "absent"
    assert exported["turn_count"] == len(full["turns"])
    assert exported["completeness"] == full["completeness"]
    assert exported["evidence"] == full["evidence"]
    artifact = full["nativeArtifacts"][0]
    assert artifact == {
        "kind": "claude-transcript-jsonl", "path": "native/claude-transcript.jsonl",
        "byteCount": len(raw), "sha256": _sha(raw), "availability": "exported",
    }
    assert "UNCOMMITTED TAIL" not in json.dumps(full)
    assert "only B" not in json.dumps(full)
    assert "/proc/self/fd/" not in json.dumps(full)


def test_export_codex_exact_native_object_evidence_and_known_empty(original_codex, tmp_path,
                                                                  monkeypatch):
    store, cwd, wid, tid = original_codex
    thread = _codex_turn(tid, wid)
    thread["turns"][0]["items"].insert(1, {
        "id": "cmd", "type": "commandExecution", "command": "printf ok",
        "aggregatedOutput": "ok", "status": "completed", "exitCode": 0,
    })
    thread["turns"][0]["items"].append({
        "id": "unknown", "type": "futureWrapper",
        "nested": {"type": "reasoning", "content": "SYNTHETIC SECRET",
                   "summary": ["visible"]},
    })
    untouched = copy.deepcopy(thread)
    calls = []

    def read_thread(thread_id, *, cwd, timeout, worker_id, expected_source):
        assert expected_source == store.codex_source(wid)
        assert (thread_id, cwd, worker_id) == (tid, str(cwd_path), wid)
        assert timeout == 120
        calls.append(thread_id)
        return thread

    cwd_path = cwd
    monkeypatch.setattr(codex_runtime, "read_thread", read_thread)
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
    source_journal = store.worker_path(wid, "evidence.jsonl")
    raw_journal = source_journal.read_bytes()
    client = WorkerClient()

    exported = client.export_conversation(wid, tmp_path / "native")
    assert calls == [tid]
    assert thread == untouched  # build_codex never rewrites raw native history
    assert len(exported["native_paths"]) == 2
    doc = _read_export(exported)
    assert doc["worker"]["handle"] == f"codex:{wid}"
    assert doc["worker"]["nativeSessionId"] == tid
    assert doc["completeness"]["status"] == "complete"
    assert doc["evidence"]["status"] == "complete"
    assert doc["evidence"]["enrichedItemCount"] >= 1
    assert "SYNTHETIC SECRET" not in json.dumps(doc)
    command = next(item for turn in doc["turns"] for item in turn["output"]
                   if item["id"] == "cmd")
    assert command["startedAtMs"] == 1710000000123
    assert command["completedAtMs"] == 1710000000456
    assert command["timingSource"] == "ccx-live-item-notification"
    raw_thread = Path(exported["native_paths"][0]).read_bytes()
    assert json.loads(raw_thread) == untouched
    assert b"SYNTHETIC SECRET" in raw_thread
    assert Path(exported["native_paths"][1]).read_bytes() == raw_journal
    assert [(a["kind"], a["path"], a["byteCount"], a["sha256"]) for a in
            doc["nativeArtifacts"]] == [
        ("codex-thread-read", "native/codex-thread.json",
         len(raw_thread), _sha(raw_thread)),
        ("codex-evidence-jsonl", "native/codex-evidence.jsonl",
         len(raw_journal), _sha(raw_journal)),
    ]

    original_opener = codex_evidence._owned_evidence_fd

    def forbid_extra_open(target, flags, *, store=None):
        if store is not None:
            pytest.fail("include_native=False opened an export journal descriptor")
        return original_opener(target, flags, store=store)

    with monkeypatch.context() as patch:
        patch.setattr(codex_evidence, "_owned_evidence_fd", forbid_extra_open)
        disabled = client.export_conversation(wid, tmp_path / "no-native",
                                               include_native=False)
    assert disabled["native_paths"] == [] and disabled["native_path"] is None
    assert _read_export(disabled)["nativeArtifacts"] == []
    assert not (tmp_path / "no-native" / "native").exists()

    journal = codex_evidence.EvidenceJournal(wid)
    journal.record("item/started", {
        "turnId": "native-turn-1", "startedAtMs": 999999999999,
        "item": {"id": "native-final-1", "type": "agentMessage"},
    }, thread_id="foreign-thread")
    journal.close()
    foreign = client.export_conversation(wid, tmp_path / "foreign")
    foreign_doc = _read_export(foreign)
    assert foreign_doc["evidence"]["status"] == "degraded"
    assert foreign_doc["completeness"]["status"] == "complete"
    final = next(item for turn in foreign_doc["turns"] for item in turn["output"]
                 if item["id"] == "native-final-1")
    assert final.get("startedAtMs") is None
    assert b"foreign-thread" in Path(foreign["native_paths"][1]).read_bytes()

    source_journal.unlink()
    absent = client.export_conversation(wid, tmp_path / "absent")
    assert [a["kind"] for a in _read_export(absent)["nativeArtifacts"]] == [
        "codex-thread-read",
    ]
    assert _read_export(absent)["evidence"]["status"] == "absent"

    # A confirmed 'known empty' is not a fetched native thread/read payload.
    store.mutate_record(wid, patches={"historyKnownEmpty": True})
    empty_journal = codex_evidence.EvidenceJournal(wid)
    empty_journal.record("ccx/lifecycle", {"from": "idle", "to": "stopped"}, thread_id=tid)
    empty_journal.close()
    calls_before = len(calls)
    empty = client.export_conversation(wid, tmp_path / "known-empty")
    assert len(calls) == calls_before
    assert empty["turn_count"] == 0
    assert [a["kind"] for a in _read_export(empty)["nativeArtifacts"]] == [
        "codex-evidence-jsonl",
    ]
    assert empty["native_paths"] == [empty["native_path"]]
    assert Path(empty["native_path"]).name == "codex-evidence.jsonl"
    assert not (tmp_path / "known-empty" / "native" / "codex-thread.json").exists()


def test_export_source_preflight_journal_failures_and_output_error(original_codex, tmp_path,
                                                                  monkeypatch):
    store, cwd, wid, tid = original_codex
    client = WorkerClient()
    thread = _codex_turn(tid, wid)
    monkeypatch.setattr(
        codex_runtime, "read_thread",
        lambda thread_id, *, cwd, timeout, worker_id, expected_source: (
            thread if expected_source == store.codex_source(wid) else None
        ),
    )
    journal = codex_evidence.EvidenceJournal(wid)
    journal.record("turn/started", {"turn": {"id": "native-turn-1"}}, thread_id=tid)
    journal.close()
    evidence_path = store.worker_path(wid, "evidence.jsonl")
    real_open = codex_evidence._owned_evidence_fd

    def unsafe_only_at_second_open(target, flags, *, store=None):
        if store is not None:
            evidence_path.chmod(0o644)
        return real_open(target, flags, store=store)

    with monkeypatch.context() as patch:
        patch.setattr(codex_evidence, "_owned_evidence_fd", unsafe_only_at_second_open)
        with pytest.raises(SessionUnavailable, match="original codex conversation source"):
            client.export_conversation(wid, tmp_path / "unsafe")
    assert not (tmp_path / "unsafe").exists()

    evidence_path.chmod(0o600)

    def disappears_only_at_second_open(target, flags, *, store=None):
        if store is not None:
            evidence_path.unlink()
            raise FileNotFoundError("original journal removed after read")
        return real_open(target, flags, store=store)

    with monkeypatch.context() as patch:
        patch.setattr(codex_evidence, "_owned_evidence_fd", disappears_only_at_second_open)
        missing = client.export_conversation(wid, tmp_path / "disappeared")
    assert [a["kind"] for a in _read_export(missing)["nativeArtifacts"]] == [
        "codex-thread-read",
    ]
    assert len(missing["native_paths"]) == 1
    assert not (tmp_path / "disappeared" / "native" / "codex-evidence.jsonl").exists()

    # A destination failure is an OUTPUT error, never a source-unavailable error.
    held = []
    def destination_denied(_doc, _destination, **kwargs):
        held.append(kwargs["native_source"])
        raise PermissionError("synthetic destination denied")

    with monkeypatch.context() as patch:
        patch.setattr(_conv, "write_export", destination_denied)
        with pytest.raises(PermissionError, match="synthetic destination denied"):
            client.export_conversation(A, tmp_path / "denied")
    assert held
    with pytest.raises(OSError):
        os.fstat(held[0].fd)  # errors do not leak held native sources

    # Prove the before-yield ACTIVE fence, even with same owned full UUID
    # in a newly active generation; NO artifact can be published before yield.
    root = constants.CCX_ROOT
    switch_pointer(root, G2)
    newer = require_ccx_active(root)
    prepare_worker(newer, engine="claude", cwd=str(cwd), worker_id=A)
    switch_pointer(root, G1)
    store.revalidate()
    accepted_build = _conv.build_claude

    def flip_after_original_build(*args, **kwargs):
        result = accepted_build(*args, **kwargs)
        switch_pointer(root, G2)
        return result

    with monkeypatch.context() as patch:
        patch.setattr(_conv, "build_claude", flip_after_original_build)
        with pytest.raises(SessionUnavailable, match="generation"):
            client.export_conversation(A, tmp_path / "flipped")
    assert not (tmp_path / "flipped").exists()
