"""Original full-UUID Codex PUBLIC pages: sealed fake native source, no providers."""
from __future__ import annotations

import hashlib
import os
from types import SimpleNamespace

import pytest

from ccx.codex import evidence as codex_evidence
from ccx.codex import runtime as codex_runtime
from ccx.codex import telemetry as codex_telemetry
from ccx.ops import conversation_paging as P
from ccx.errors import (
    CursorInvalidated, InvalidRequest, OperationTimeout, ProviderProtocolError,
    SessionNotFound, SessionUnavailable,
)
from ccx.ops.creation_steps import activate_prepared_worker, prepare_worker
from ccx.workers import WorkerClient
from test_generation_store import A, G2, short_root  # noqa: F401 - fixture
from test_worker_telemetry import original_codex, original_sources  # noqa: F401 - fixtures


def _scope(store, wid, tid):
    return {
        "root": hashlib.sha256(os.fsencode(store.root)).hexdigest(),
        "generation": store.generation,
        "engine": "codex",
        "worker_id": wid,
        "thread_id": tid,
    }


def _item(i, text="native"):
    return {
        "turnId": f"t{i}",
        "item": {"type": "agentMessage", "id": f"i{i}", "text": text},
    }


def _ids(page):
    return [row["source_record_id"] for row in page["records"]]


def test_public_original_source_filtered_evidence_known_empty_and_recorded_cwd(
    original_codex, monkeypatch,
):
    store, cwd, wid, tid = original_codex
    client = WorkerClient()
    assert "conversation_page" not in client.capabilities("codex")
    assert "conversation_page" not in client.capabilities("claude")
    store.mutate_record(wid, patches={
        "historyKnownEmpty": True, "lifecycle.state": "dead",
    })
    reads = []
    original = codex_telemetry._original_source

    def source(*args):
        reads.append(args)
        return original(*args)

    monkeypatch.setattr(codex_telemetry, "_original_source", source)
    monkeypatch.setattr(codex_runtime, "read_thread", lambda *_a, **_k: pytest.fail(
        "public paging must not read full native history"
    ))
    monkeypatch.setattr(codex_runtime, "list_thread_items", lambda *_a, **_k: pytest.fail(
        "source-proven known-empty original must not request native items"
    ))
    empty = client.conversation_page(wid, source_record_target=2)
    assert empty["worker"] == f"codex:{wid}"
    assert empty["records"] == [] and empty["older_cursor"] is None
    assert empty["completeness"]["status"] == "complete"
    assert empty["completeness"]["returned_source_record_count"] == 0
    assert len(reads) == 1
    assert reads[0][0] is client._uuid_store
    assert reads[0][1].worker_id == wid

    genuine_older = P._encode_cursor({**_scope(store, wid, tid), "provider_cursor": "old"})
    with pytest.raises(CursorInvalidated, match="no earlier messages"):
        client.conversation_page(wid, cursor=genuine_older)
    with pytest.raises(InvalidRequest):
        client.conversation_page(wid, cursor="not-ccxcp2")
    with pytest.raises(InvalidRequest, match="another worker"):
        client.conversation_page(wid, cursor=P._encode_cursor({
            **_scope(store, wid, tid), "worker_id": A, "provider_cursor": "old",
        }))
    assert len(reads) == 4

    # The exact native item/turn evidence for this original owner may enrich
    # it; a later foreign event with the SAME IDs must never overwrite it.
    store.mutate_record(wid, patches={"historyKnownEmpty": False})
    journal = codex_evidence.EvidenceJournal(wid)
    journal.record("item/completed", {
        "turnId": "ta", "item": {"id": "assistant-a", "type": "agentMessage"},
        "completedAtMs": 222,
    }, thread_id=tid)
    journal.record("item/completed", {
        "turnId": "ta", "item": {"id": "assistant-a", "type": "agentMessage"},
        "completedAtMs": 999,
    }, thread_id="foreign-native-thread")
    journal.close()
    monkeypatch.setattr(codex_evidence, "read_index", lambda *_a: pytest.fail(
        "private short-based/latest evidence read forbidden"
    ))
    native = [
        {"turnId": "ta", "item": {
            "type": "agentMessage", "id": "assistant-a", "text": "original reply",
        }},
        {"turnId": "tu", "item": {
            "type": "userMessage", "id": "user-a", "content": [
                {"type": "text", "text": "one"}, {"type": "text", "text": "two"},
            ],
        }},
    ]
    calls = []

    def bounded(thread_id, *, worker_id, expected_source, cwd, cursor, limit, sort_direction, timeout):
        assert expected_source == store.codex_source(wid) == f"ccx-worker:{wid}"
        calls.append((thread_id, worker_id, cwd, cursor, limit, sort_direction, timeout))
        return {"data": native, "nextCursor": None}

    monkeypatch.setattr(codex_runtime, "list_thread_items", bounded)
    # Read-only observation uses recorded cwd, even when the project vanishes.
    cwd.rmdir()
    before = store.worker_path(wid, "record.json").read_bytes()
    page = client.conversation_page(f"codex:{wid}", source_record_target=2)
    assert _ids(page) == ["user-a", "assistant-a"]
    assert [len(row["items"]) for row in page["records"]] == [2, 1]
    assert page["completeness"]["returned_source_record_count"] == 2
    assert page["completeness"]["requested_source_record_target"] == 2
    assert page["records"][1]["timestamp_ms"] == 222
    assert page["records"][1]["duration_ms"] is None
    assert page["records"][1]["items"][0]["text"] == "original reply"
    assert page["older_cursor"] is None
    assert len(page["completeness"]["warnings"]) == 1
    assert "foreign" in page["completeness"]["warnings"][0]
    assert calls and calls[0][:6] == (tid, wid, str(cwd), None, 2, "desc")
    assert 0 < calls[0][6] <= 30
    assert store.worker_path(wid, "record.json").read_bytes() == before
    assert len(reads) == 5  # exactly one source proof per public invocation


def test_public_v3_original_cursor_walk_and_typed_refusals(original_codex, monkeypatch):
    store, cwd, wid, tid = original_codex
    store.mutate_record(wid, patches={"historyKnownEmpty": False})
    other = "bbbbbbbb-5678-4000-8000-000000000005"
    assert wid[:8] == other[:8] and wid != other
    prepared = prepare_worker(store, engine="codex", cwd=str(cwd), worker_id=other)

    def accepted(_budget):
        activation = store.native_activation(other)
        return {
            "accepted": True, "workerId": other, "engine": "codex",
            "source": f"ccx-worker:{other}",
            "submissionId": activation["submissionId"],
            "nativeEvidenceSha256": "f" * 64,
            "nativeBinding": {"threadId": "native-other"},
        }

    assert activate_prepared_worker(
        store, prepared, bounded_native_submit=accepted
    ).activation_state == "confirmed"
    store.mutate_record(other, patches={"historyKnownEmpty": False})
    client = WorkerClient()
    entries = [_item(i, "x" * 4000) for i in range(12)]
    calls = []

    def source_page(thread_id, *, worker_id, expected_source, cwd, cursor, limit, sort_direction, timeout):
        assert expected_source == store.codex_source(wid) == f"ccx-worker:{wid}"
        assert thread_id == tid and worker_id == wid
        assert cwd == str(cwd_recorded) and sort_direction == "desc"
        calls.append((cursor, limit, timeout))
        if cursor is None:
            # Empty native page is NOT EOF: the provider still gives a cursor.
            return {"data": [], "nextCursor": "12"}
        end = int(cursor)
        start = max(0, end - min(limit, 7))  # Native under-returns vs requested.
        return {
            "data": list(reversed(entries[start:end])),
            "nextCursor": str(start) if start else None,
        }

    cwd_recorded = cwd
    monkeypatch.setattr(codex_runtime, "list_thread_items", source_page)
    monkeypatch.setattr(P, "PAGE_RESULT_MAX_BYTES", 15_000)
    first = client.conversation_page(wid, source_record_target=8)
    assert first["records"] == [] and first["older_cursor"]
    payload = P._decode_cursor(first["older_cursor"], engine="codex", worker_id=wid)
    assert payload == {
        "v": 3, **_scope(store, wid, tid), "provider_cursor": "12",
    }
    assert "short" not in payload and "thread_source" not in payload
    assert P._encoded_size(first) <= 15_000

    pages = [first]
    while pages[-1]["older_cursor"]:
        assert len(pages) < 12
        pages.append(client.conversation_page(
            wid, cursor=pages[-1]["older_cursor"], source_record_target=8,
        ))
    historical = []
    for page in pages:
        assert page["completeness"]["returned_source_record_count"] == len(page["records"])
        assert _ids(page) == sorted(_ids(page), key=lambda x: int(x[1:]))
        assert P._encoded_size(page) <= 15_000
        historical = _ids(page) + historical
    assert historical == [f"i{i}" for i in range(12)]
    assert len(set(historical)) == len(historical)
    assert calls[0][:2] == (None, 8)
    assert ("12", 8) in [(c, n) for c, n, _ in calls]
    assert ("12", 3) in [(c, n) for c, n, _ in calls]
    assert any(p["completeness"]["response_budget_limited"] for p in pages)
    assert all(0 < t <= 30 for _, _, t in calls)

    cursor = first["older_cursor"]
    prior = len(calls)
    with pytest.raises(InvalidRequest, match="another worker"):
        client.conversation_page(other, cursor=cursor)
    with pytest.raises(InvalidRequest, match="another worker"):
        client.conversation_page(wid, cursor=P._encode_cursor({
            **_scope(store, wid, tid), "engine": "claude", "provider_cursor": "12",
        }))
    for patch in (
        {"v": 2},
        {"root": "different"},
        {"generation": G2},
        {"thread_id": "foreign-thread"},
        {"provider_cursor": ""},
        {"provider_cursor": None},
    ):
        with pytest.raises(CursorInvalidated):
            client.conversation_page(wid, cursor=P._encode_cursor({
                **payload, **patch,
            }))
    with pytest.raises(InvalidRequest):
        client.conversation_page(wid[:8])
    with pytest.raises(InvalidRequest):
        client.conversation_page("display-name")
    with pytest.raises(SessionNotFound):
        client.conversation_page("dddddddd-1234-4000-8000-000000000006")
    assert len(calls) == prior, "all refused cursors must avoid native IO"

    # Original activation/source mismatch refuses before item transport.
    original_activation = client._uuid_store.native_activation
    monkeypatch.setattr(client._uuid_store, "native_activation", lambda target: (
        {**original_activation(target), "nativeBinding": {"threadId": "wrong"}}
        if target in (wid, f"codex:{wid}") else original_activation(target)
    ))
    with pytest.raises(SessionUnavailable):
        client.conversation_page(wid)
    assert len(calls) == prior


def test_public_budget_projection_stubs_native_cursor_and_deadline(original_codex, monkeypatch):
    store, cwd, wid, tid = original_codex
    store.mutate_record(wid, patches={"historyKnownEmpty": False})
    client = WorkerClient()
    calls = []
    huge = "x" * (256 * 1024)
    command = "é" * 400 + "\nnot a summary line"
    native = [
        {"turnId": "tn", "item": {
            "type": "futureBlob", "id": "unknown", "payload": huge,
        }},
        {"turnId": "tf", "item": {
            "type": "fileChange", "id": "file-1", "status": "completed",
            "changes": [{"path": "/synthetic/f", "kind": "update", "diff": huge}],
        }},
        {"turnId": "ta", "item": {
            "type": "agentMessage", "id": "assistant-1",
            "text": huge, "phase": {"blob": "p" * (512 * 1024)},
        }},
        {"turnId": "tc", "item": {
            "type": "commandExecution", "id": "command-1",
            "command": command, "aggregatedOutput": "ok",
            "cwd": str(cwd), "status": "completed", "exitCode": 0,
        }},
    ]

    def clipped(thread_id, *, worker_id, expected_source, cwd, cursor, limit, sort_direction, timeout):
        assert expected_source == store.codex_source(wid) == f"ccx-worker:{wid}"
        calls.append((cursor, limit, timeout))
        assert (thread_id, worker_id, cwd, sort_direction) == (tid, wid, str(project), "desc")
        return {"data": native, "nextCursor": None}

    project = cwd
    monkeypatch.setattr(codex_runtime, "list_thread_items", clipped)
    page = client.conversation_page(wid, source_record_target=4)
    assert _ids(page) == ["command-1", "assistant-1", "file-1", "unknown"]
    assert P._encoded_size(page) <= P.PAGE_RESULT_MAX_BYTES
    assert page["completeness"]["status"] == "bounded"
    assert page["completeness"]["returned_source_record_count"] == 4
    assert any(item["kind"] == "native" for r in page["records"] for item in r["items"]) or any(
        "native" in item for r in page["records"] for item in r["items"]
    )
    paths = [
        t["path"] for r in page["records"] for item in r["items"]
        for t in item.get("truncations") or []
    ]
    assert "text" in paths and "changes" in paths and "phase" in paths
    cmd = page["records"][0]["items"][0]
    assert cmd["arguments"]["command"] == command
    assert cmd["summary"]["target_truncated"] is True
    assert len(cmd["summary"]["target"].encode("utf-8")) <= 512
    assert "\n" not in cmd["summary"]["target"]
    assert calls[0][:2] == (None, 4)

    # Native provider cardinality is not the requested 40; shrink from 7 to
    # 3 and keep the ORIGINAL incoming native cursor until the smaller fetch.
    monkeypatch.setattr(P, "PAGE_RESULT_MAX_BYTES", 15_000)
    entries = [_item(i, "x" * 4000) for i in range(20)]
    calls.clear()

    def capped(thread_id, *, worker_id, expected_source, cwd, cursor, limit, sort_direction, timeout):
        assert expected_source == store.codex_source(wid) == f"ccx-worker:{wid}"
        calls.append((cursor, limit, timeout))
        actual = min(limit, 7)
        end = len(entries) if cursor is None else int(cursor)
        start = max(0, end - actual)
        return {
            "data": list(reversed(entries[start:end])),
            "nextCursor": str(start) if start else None,
        }

    monkeypatch.setattr(codex_runtime, "list_thread_items", capped)
    smaller = client.conversation_page(wid, source_record_target=40, timeout=10)
    assert calls[0][0:2] == (None, 40)
    assert calls[1][0:2] == (None, 3)
    assert calls[1][2] < calls[0][2] <= 10
    assert smaller["completeness"]["response_budget_limited"] is True
    assert smaller["completeness"]["returned_source_record_count"] == len(smaller["records"])
    assert P._encoded_size(smaller) <= 15_000

    # A single oversized record is not an impassable cursor barrier.
    monkeypatch.setattr(P, "PAGE_RESULT_MAX_BYTES", 1200)
    monkeypatch.setattr(codex_runtime, "list_thread_items", lambda *_a, **_k: {
        "data": [_item(1, "x" * 4000)], "nextCursor": "older-original",
    })
    stub = client.conversation_page(wid, source_record_target=1)
    assert _ids(stub) == ["i1"]
    assert stub["records"][0]["items"] == []
    assert stub["older_cursor"]
    assert stub["completeness"]["omitted_item_count"] >= 1
    assert P._encoded_size(stub) <= 1200

    # When the provider over-returns the final limit, no cursor can safely
    # advance past the dropped sibling.
    monkeypatch.setattr(codex_runtime, "list_thread_items", lambda *_a, **_k: {
        "data": [_item(2, "x" * 4000), _item(1, "x" * 4000)],
        "nextCursor": "older-than-both",
    })
    with pytest.raises(ProviderProtocolError, match="exceeded requested limit"):
        client.conversation_page(wid, source_record_target=1)

    # Empty string from native isn't EOF, even if it came with zero records.
    monkeypatch.setattr(codex_runtime, "list_thread_items", lambda *_a, **_k: {
        "data": [], "nextCursor": "",
    })
    with pytest.raises(ProviderProtocolError, match="cursor is invalid"):
        client.conversation_page(wid)

    monkeypatch.setattr(codex_runtime, "list_thread_items", lambda *_a, **_k: {
        "data": "not a native list", "nextCursor": None,
    })
    with pytest.raises(SessionUnavailable, match="malformed"):
        client.conversation_page(wid)

    # The public timeout is also consumed by refetch; an exhausted budget
    # raises, rather than publishing the first oversized candidate as complete.
    calls.clear()
    monkeypatch.setattr(P, "PAGE_RESULT_MAX_BYTES", 1500)
    monkeypatch.setattr(P, "time", SimpleNamespace(
        monotonic=iter([100.0, 100.1, 102.0]).__next__,
    ))
    monkeypatch.setattr(codex_runtime, "list_thread_items", capped)
    with pytest.raises(OperationTimeout, match="deadline expired"):
        client.conversation_page(wid, source_record_target=4, timeout=1)
    assert len(calls) == 1 and calls[0][1] == 4
