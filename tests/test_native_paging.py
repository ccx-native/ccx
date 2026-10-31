"""Original full-UUID Claude pages over sealed native JSONL; no provider calls."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pytest

from ccx import constants
from ccx.ops import conversation_paging as P
from ccx.errors import CursorInvalidated, InvalidRequest, SessionNotFound, SessionUnavailable
from ccx.ops.creation_steps import prepare_worker
from ccx.store.root import require_ccx_active
from ccx.workers import WorkerClient
from test_generation_store import A, B, G2, switch_pointer
from test_generation_store import short_root as short_root
from test_native_conversation import _append, _entry
from test_worker_telemetry import original_codex as original_codex
from test_worker_telemetry import original_sources as original_sources


def _ids(page):
    return [r["source_record_id"] for r in page["records"]]


def test_native_source_counts_cursor_walk_compaction_tool_join_and_complete_eof(original_sources):
    store, _cwd = original_sources
    assert A[:8] == B[:8]
    assert store.record(A)["native"]["sessionId"] != store.record(B)["native"]["sessionId"]
    assert store.record(A)["native"]["sessionId"] != A
    native = Path(store.record(A)["transcript"])
    rows = [_entry("user", "start", uuid="u0")]
    rows.extend([
        {"type": "system", "subtype": "compact_boundary", "uuid": "compact",
         "content": "Conversation compacted"},
        {"type": "user", "uuid": "linked-summary", "parentUuid": "compact",
         "isCompactSummary": True, "isVisibleInTranscriptOnly": True,
         "message": {"role": "user", "content": "original compact summary"}},
    ])
    rows.extend(_entry("assistant", f"part {i}", uuid=f"a{i}") for i in range(24))
    rows.append({
        "type": "assistant", "uuid": "a24",
        "message": {"id": "tool-msg", "model": "fake-claude",
                    "content": [{"type": "tool_use", "id": "tool-1", "name": "Bash",
                                 "input": {"command": "echo hi",
                                           "description": "The model's tool description"}}]},
    })
    # The result follows the a24 cut in native source order, and joins a24
    # when the next public page normalizes its bounded semantic lookahead.
    rows.append({"type": "user", "uuid": "r-tool", "message": {"content": [
        {"type": "tool_result", "tool_use_id": "tool-1", "content": "hi", "is_error": False}
    ]}})
    rows.extend(_entry("assistant", f"part {i}", uuid=f"a{i}") for i in range(25, 30))
    rows.insert(15, {"type": "queue-operation", "uuid": "internal",
                     "operation": "enqueue", "content": "not conversation"})
    _append(native, *rows)
    with native.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(_entry("assistant", "UNFINISHED TAIL", uuid="partial")))
    _append(Path(store.record(B)["transcript"]), _entry("assistant", "only B", uuid="b0"))
    store.mutate_record(A, patches={"lifecycle.state": "dead"})
    client = WorkerClient()
    page = client.conversation_page(f"claude:{A}", source_record_target=5)
    assert page["worker"] == f"claude:{A}" and _ids(page) == [f"a{i}" for i in range(25, 30)]
    assert page["completeness"]["returned_source_record_count"] == 5
    assert page["completeness"]["requested_source_record_target"] == 5
    assert page["older_cursor"].startswith("ccxcp2.")
    assert P._decode_cursor(page["older_cursor"], engine="claude", worker_id=A)["session_id"] == (
        store.record(A)["native"]["sessionId"]
    )
    older = client.conversation_page(A, cursor=page["older_cursor"], source_record_target=5)
    # The intervening native tool-result consumes one source slot, while its
    # public content is joined to a24 rather than fabricated as a new record.
    assert _ids(older) == [f"a{i}" for i in range(21, 25)]
    assert older["completeness"]["returned_source_record_count"] == 4
    call = next(item for record in older["records"] for item in record["items"]
                if item["kind"] == "tool-call")
    assert call["id"] == "tool-1"
    assert call["result"]["content"] == "hi" and call["status"] == "completed"
    assert call["summary"]["description"] == "The model's tool description"

    pages = [page, older]
    while pages[-1]["older_cursor"] is not None:
        assert len(pages) < 20, "cursor must progress to the earliest native source"
        pages.append(client.conversation_page(
            A, cursor=pages[-1]["older_cursor"], source_record_target=5,
        ))
    seen = [rid for p in pages for rid in _ids(p)]
    assert len(seen) == len(set(seen))
    assert sorted((rid for rid in seen if rid.startswith("a")), key=lambda s: int(s[1:])) == [
        f"a{i}" for i in range(30)
    ]
    assert "internal" not in seen and "r-tool" not in seen
    assert "partial" not in seen and "linked-summary" not in seen
    compact = next(r for p in pages for r in p["records"] if r["source_record_id"] == "compact")
    assert compact["items"][0]["nativeType"] == "compact_boundary"
    assert compact["items"][0]["data"]["summary"] == "original compact summary"
    assert all(p["completeness"]["returned_source_record_count"] == len(p["records"]) for p in pages)
    assert client.conversation_page(B)["records"][0]["source_record_id"] == "b0"
    assert not store.worker_path(A, "transcript.json").exists()
    assert not (constants.CCX_ROOT / "sessions").exists()


def test_cursor_original_scope_typed_refusals_and_same_fd_mutation(original_codex, monkeypatch):
    store, cwd, _codex_worker, _thread_id = original_codex
    native_a = Path(store.record(A)["transcript"])
    native_b = Path(store.record(B)["transcript"])
    rows = [_entry("user", "start", uuid="u0")]
    rows += [_entry("assistant", f"part {i}", uuid=f"a{i}") for i in range(8)]
    _append(native_a, *rows)
    client = WorkerClient()
    first = client.conversation_page(A, source_record_target=2)
    token = first["older_cursor"]
    assert _ids(first) == ["a6", "a7"] and token
    payload = P._decode_cursor(token, engine="claude", worker_id=A)
    assert payload["root"] == hashlib.sha256(os.fsencode(store.root)).hexdigest()
    assert payload["generation"] == store.generation and payload["worker_id"] == A
    with pytest.raises(InvalidRequest, match="another worker"):
        client.conversation_page(B, cursor=token)
    with pytest.raises(InvalidRequest, match="another worker"):
        client.conversation_page(A, cursor=P._encode_cursor({**payload, "engine": "codex"}))
    with pytest.raises(CursorInvalidated):
        client.conversation_page(A, cursor=P._encode_cursor({**payload, "v": 2}))
    for changed in ({"root": "other"}, {"generation": G2},
                    {"session_id": B}, {"dev": -1}, {"ino": -1},
                    {"source_eof": payload["cut_offset"]},
                    {"cut_offset": 1}, {"cut_fingerprint": "0" * 64},
                    {"anchor_offset": 1},
                    {"anchor_id": None, "anchor_fingerprint": None},
                    {"context_end_offset": -1},
                    {"context_end_offset": payload["cut_offset"] + 1}):
        with pytest.raises(CursorInvalidated):
            client.conversation_page(A, cursor=P._encode_cursor({**payload, **changed}))

    _append(native_a, _entry("assistant", "appended", uuid="a8"))
    assert _ids(client.conversation_page(A, cursor=token, source_record_target=2)) == ["a4", "a5"]
    original = P._normalize_claude_window

    def truncated_during_read(*args, **kwargs):
        result = original(*args, **kwargs)
        native_a.write_bytes(b"")  # same held inode, after-read fstat observes truncation
        return result

    with monkeypatch.context() as patch:
        patch.setattr(P, "_normalize_claude_window", truncated_during_read)
        with pytest.raises(SessionUnavailable, match="non-append-only"):
            client.conversation_page(A, source_record_target=2)
    _append(native_a, *rows)
    with monkeypatch.context() as patch:
        patch.setattr(P, "_normalize_claude_window", truncated_during_read)
        with pytest.raises(CursorInvalidated, match="source changed"):
            client.conversation_page(A, cursor=token, source_record_target=2)

    assert client.conversation_page(B)["completeness"]["status"] == "complete"
    assert client.conversation_page(B)["records"] == []
    for target in (A[:8], "display name", "claude-session:" + A):
        with pytest.raises(InvalidRequest):
            client.conversation_page(target)
    with pytest.raises(SessionNotFound):
        client.conversation_page(store.record(A)["native"]["sessionId"])
    for options in ({"source_record_target": 0}, {"source_record_target": True},
                    {"source_record_target": 101}, {"max_tool_result_bytes": -1},
                    {"max_tool_result_bytes": True}, {"timeout": 0}):
        with pytest.raises(InvalidRequest):
            client.conversation_page(A, **options)
    with pytest.raises(InvalidRequest):
        client.conversation_page(A, cursor="malformed")
    native_b.unlink()
    with pytest.raises(SessionNotFound):
        client.conversation_page(B)
    with pytest.raises(CursorInvalidated):
        client.conversation_page(B, cursor=token)
    native_b.symlink_to(native_a)
    with pytest.raises(SessionUnavailable):
        client.conversation_page(B)

    switch_pointer(constants.CCX_ROOT, G2)
    with pytest.raises(SessionUnavailable, match="generation"):
        client.conversation_page(A)
    new_store = require_ccx_active(constants.CCX_ROOT)
    prepare_worker(new_store, engine="claude", cwd=str(cwd), worker_id=A)
    fresh = Path(new_store.record(A)["transcript"])
    fresh.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    _append(fresh, *rows)
    fresh.chmod(0o600)
    with pytest.raises(CursorInvalidated):
        WorkerClient().conversation_page(A, cursor=token)


def test_original_budget_clips_and_frontier_no_gaps(original_sources, monkeypatch):
    store, _cwd = original_sources
    native = Path(store.record(A)["transcript"])
    huge = "é" * (90 * 1024)
    rows = [_entry("user", "start", uuid="u0")]
    rows += [_entry("assistant", "x" * 4000, uuid=f"a{i}") for i in range(10)]
    rows.append({"type": "assistant", "uuid": "tool", "message": {
        "id": "msg-tool", "model": "fake-claude", "content": [
            {"type": "tool_use", "id": "call", "name": "Bash",
             "input": {"command": "echo safe", "description": "safe"}}
        ],
    }})
    rows.append({"type": "user", "uuid": "result", "message": {"content": [
        {"type": "tool_result", "tool_use_id": "call", "content": huge}
    ]}})
    rows.append(_entry("assistant", huge, uuid="oversize"))
    _append(native, *rows)
    with native.open("a", encoding="utf-8") as stream:
        stream.write("{invalid-json}\n")
        stream.write(json.dumps(_entry("assistant", "unfinished", uuid="partial")))
    client = WorkerClient()
    # The tool_result row is a native source record, even though it has no
    # standalone public projection after joining its preceding tool call.
    zero = client.conversation_page(A, source_record_target=3, max_tool_result_bytes=0)
    positive = client.conversation_page(A, source_record_target=3,
                                        max_tool_result_bytes=100 * 1024)
    assert _ids(positive) == ["tool", "oversize"]
    for page in (zero, positive):
        assert page["completeness"]["status"] == "partial"
        assert page["completeness"]["malformed_line_count"] == 1
        assert P._encoded_size(page) <= P.PAGE_RESULT_MAX_BYTES
        assert "unfinished" not in json.dumps(page)
        assert "_ccx_" not in json.dumps(page)
        oversized = next(r for r in page["records"] if r["source_record_id"] == "oversize")
        clipped = next(i for i in oversized["items"] if i["kind"] == "assistant-text")
        assert len(clipped["text"].encode()) <= P._PAGE_FIELD_MAX_BYTES
        assert clipped["truncations"]
    expected_digest = hashlib.sha256(huge.encode("utf-8")).hexdigest()
    tool_zero = next(i for r in zero["records"] for i in r["items"] if i["kind"] == "tool-call")
    tool_positive = next(i for r in positive["records"] for i in r["items"]
                         if i["kind"] == "tool-call")
    assert tool_zero["result"]["returnedByteCount"] == 0
    assert tool_positive["result"]["contentSha256"] == expected_digest
    assert tool_positive["result"]["originalByteCount"] == len(huge.encode())
    by_stage = {t["source"]: t for t in tool_positive["truncations"]}
    assert by_stage["max_tool_result_bytes"]["sha256"] == expected_digest
    assert by_stage["ccx-page"]["sha256"] == expected_digest

    monkeypatch.setattr(P, "PAGE_RESULT_MAX_BYTES", 15_000)
    page = client.conversation_page(A, source_record_target=10)
    assert 0 < len(page["records"]) < 10 and page["older_cursor"]
    assert page["completeness"]["response_budget_limited"] is True
    seen = []
    while True:
        assert P._encoded_size(page) <= 15_000
        assert page["completeness"]["returned_source_record_count"] == len(page["records"])
        seen.extend(_ids(page))
        if not page["older_cursor"]:
            break
        assert len(seen) < 40
        page = client.conversation_page(A, cursor=page["older_cursor"], source_record_target=10)
    assert len(seen) == len(set(seen))
    assert sorted(seen, key=lambda s: (-1 if s == "u0" else 100
                                       if s == "tool" else 101
                                       if s == "oversize" else int(s[1:]))) == [
        "u0", *(f"a{i}" for i in range(10)), "tool", "oversize"
    ]
