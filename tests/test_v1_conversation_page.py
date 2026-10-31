"""Real Protocol V1 conversation.page over sealed original sources, no providers."""
from __future__ import annotations
import ccx.store.root as store_root

import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from ccx.codex import runtime as codex_runtime
from ccx import constants
from ccx.ops import conversation_paging as paging
from ccx.rpc import local_rpc
from ccx.errors import SessionUnavailable
from test_generation_store import A, B, C, G2, short_root  # noqa: F401 - fixture
from test_native_conversation import _append, _entry
from test_worker_telemetry import original_codex, original_sources  # noqa: F401 - fixtures


def _request(op="conversation.page", *, target=None, timeout_ms=None, **options):
    params = {"target": target, **options} if target is not None else options
    value = {"protocol_version": 2, "request_id": "v1-page", "op": op, "params": params}
    if timeout_ms is not None:
        value["timeout_ms"] = timeout_ms
    return value


def _error(result, code):
    assert result["protocol_version"] == 2 and result["request_id"] == "v1-page"
    assert result["ok"] is False and set(result["error"]) == {"code", "message", "details"}
    assert result["error"]["code"] == code
    assert "Traceback" not in json.dumps(result)


def _ids(page):
    return [row["source_record_id"] for row in page["records"]]


def test_v1_page_both_original_engines_and_native_cursor_boundaries(original_codex, monkeypatch):
    store, cwd, codex_id, thread_id = original_codex
    assert A[:8] == B[:8] and A != B
    _append(Path(store.record(A)["transcript"]),
            _entry("user", "hello", uuid="cu0"),
            _entry("assistant", "reply", uuid="ca0"),
            {"type": "user", "uuid": "cu1", "message": {"role": "user", "content": [
                {"type": "text", "text": "one"}, {"type": "text", "text": "two"}]}},
            _entry("assistant", "final", uuid="ca1"))
    store.mutate_record(codex_id, patches={"historyKnownEmpty": False})
    before = {wid: store.worker_path(wid, "record.json").read_bytes()
              for wid in (A, B, codex_id)}
    monkeypatch.setattr(codex_runtime, "read_thread", lambda *_a, **_k: pytest.fail(
        "V1 page may not read full native history"))
    native = [
        {"turnId": "t0", "item": {"type": "agentMessage", "id": "i0", "text": "old"}},
        {"turnId": "t1", "item": {"type": "agentMessage", "id": "i1", "text": "middle"}},
        {"turnId": "t2", "item": {"type": "userMessage", "id": "i2", "content": [
            {"type": "text", "text": "one"}, {"type": "text", "text": "two"}]}},
        {"turnId": "t3", "item": {"type": "agentMessage", "id": "i3", "text": "new"}},
    ]
    calls = []

    def list_items(tid, *, worker_id, expected_source, cwd, cursor, limit, sort_direction, timeout):
        assert (tid, worker_id, cwd, sort_direction) == (
            thread_id, codex_id, str(cwd_recorded), "desc")
        assert expected_source == store.codex_source(codex_id) == f"ccx-worker:{codex_id}"
        assert 0 < timeout <= 90 and limit == 2
        calls.append((cursor, timeout))
        return {"data": list(reversed(native[2:])) if cursor is None else
                list(reversed(native[:2])),
                "nextCursor": "older-two" if cursor is None else None}

    cwd_recorded = cwd
    monkeypatch.setattr(codex_runtime, "list_thread_items", list_items)
    wire = local_rpc.respond(lambda: _request(target=A, source_record_target=2))
    assert wire.endswith(b"\n") and len(wire) <= local_rpc.MAX_MESSAGE_BYTES
    claude = json.loads(wire)
    assert claude["ok"] is True
    page = claude["result"]
    assert page["worker"] == f"claude:{A}" and _ids(page) == ["cu1", "ca1"]
    assert page["completeness"]["returned_source_record_count"] == 2
    assert page["completeness"]["requested_source_record_target"] == 2
    assert len(page["records"][0]["items"]) == 2
    cursor = paging._decode_cursor(page["older_cursor"], engine="claude", worker_id=A)
    assert cursor["v"] == 3 and cursor["session_id"] == store.record(A)["native"]["sessionId"]
    assert cursor["root"] == hashlib.sha256(os.fsencode(store.root)).hexdigest()
    assert cursor["generation"] == store.generation
    older = local_rpc.dispatch(_request(target=f"claude:{A}", cursor=page["older_cursor"],
                                        source_record_target=2))
    assert older["ok"] is True and _ids(older["result"]) == ["cu0", "ca0"]
    assert older["result"]["older_cursor"] is None

    first = local_rpc.dispatch(_request(target=f"codex:{codex_id}", source_record_target=2))
    assert first["ok"] is True and _ids(first["result"]) == ["i2", "i3"]
    assert first["result"]["worker"] == f"codex:{codex_id}"
    assert len(first["result"]["records"][0]["items"]) == 2
    assert first["result"]["completeness"]["returned_source_record_count"] == 2
    token = paging._decode_cursor(first["result"]["older_cursor"], engine="codex",
                                  worker_id=codex_id)
    assert token["v"] == 3 and token["thread_id"] == thread_id
    assert token["worker_id"] == codex_id and token["provider_cursor"] == "older-two"
    second = local_rpc.dispatch(_request(target=codex_id,
                                         cursor=first["result"]["older_cursor"],
                                         source_record_target=2))
    assert second["ok"] is True and _ids(second["result"]) == ["i0", "i1"]
    assert second["result"]["older_cursor"] is None
    assert [c for c, _ in calls] == [None, "older-two"]

    # The dispatcher must pass the envelope's *remaining* budget, not reset 2s.
    with monkeypatch.context() as patch:
        ticks = iter((100.0, 100.5))
        patch.setattr(local_rpc, "time", SimpleNamespace(monotonic=lambda: next(ticks)))
        budget = local_rpc.dispatch(_request(target=codex_id,
                                             source_record_target=2, timeout_ms=2000))
    assert budget["ok"] is True and 0 < calls[-1][1] <= 1.5
    assert all(store.worker_path(wid, "record.json").read_bytes() == raw
               for wid, raw in before.items())
    assert not (constants.CCX_ROOT / "sessions").exists()
    assert not (constants.CCX_ROOT / "run").exists()


def test_v1_page_capability_purity_and_closed_params(original_sources, monkeypatch, tmp_path):
    store, _cwd = original_sources
    with monkeypatch.context() as patch:
        missing = tmp_path / "uninitialized"
        patch.setattr(constants, "CCX_ROOT", missing)
        from ccx.store import store as generation
        patch.setattr(store_root, "require_ccx_active", lambda *_a, **_k: pytest.fail(
            "pure V1 envelope must not access the ACTIVE store"))
        caps = local_rpc.dispatch(_request("system.info"))
        assert caps["ok"] is True
        info = caps["result"]
        assert set(info) == {"protocol_version", "ccx_version"}
        assert info["protocol_version"] == 2
        for name in ("limit", "sort_direction", "timeout"):
            _error(local_rpc.dispatch(_request(target=A, **{name: 2})), "invalid_request")
        _error(local_rpc.dispatch(_request(target=A, timeout_ms=0)), "invalid_request")
        assert not missing.exists()

    # Existing V1 parser and public backend own value types and defaults.
    newest = local_rpc.dispatch(_request(target=A, cursor=None,
                                         max_tool_result_bytes=None))
    assert newest["ok"] is True and newest["result"]["records"] == []
    assert newest["result"]["completeness"]["requested_source_record_target"] == 40
    for options in ({"cursor": []}, {"source_record_target": True},
                    {"source_record_target": 101}, {"max_tool_result_bytes": -1}):
        _error(local_rpc.dispatch(_request(target=A, **options)), "invalid_request")
    assert not (store.root / "sessions").exists()


def test_v1_page_typed_refusals_and_effect_absence(original_codex, monkeypatch):
    store, _cwd, codex_id, _tid = original_codex
    _append(Path(store.record(A)["transcript"]),
            _entry("user", "start", uuid="u0"),
            _entry("assistant", "middle", uuid="a0"),
            _entry("assistant", "new", uuid="a1"))
    store.mutate_record(codex_id, patches={"historyKnownEmpty": False})
    first = local_rpc.dispatch(_request(target=A, source_record_target=1))
    assert first["ok"] is True and first["result"]["older_cursor"]
    cursor = paging._decode_cursor(first["result"]["older_cursor"],
                                   engine="claude", worker_id=A)
    before = {wid: store.worker_path(wid, "record.json").read_bytes()
              for wid in (A, B, codex_id)}
    native_calls = []
    monkeypatch.setattr(codex_runtime, "read_thread", lambda *_a, **_k: pytest.fail(
        "no full thread read"))
    monkeypatch.setattr(codex_runtime, "list_thread_items", lambda *_a, **_k:
                        native_calls.append("native") or {"data": [], "nextCursor": ""})
    _error(local_rpc.dispatch(_request(target=A[:8])), "invalid_request")
    _error(local_rpc.dispatch(_request(target="dddddddd-1234-4000-8000-000000000006")),
           "not_found")
    _error(local_rpc.dispatch(_request(target=A, cursor=paging._encode_cursor(
        {**cursor, "v": 2}))), "refused")
    _error(local_rpc.dispatch(_request(target=A), expected_generation=G2),
           "unavailable")
    with monkeypatch.context() as patch:
        ticks = iter((100.0, 102.0))
        patch.setattr(local_rpc, "time", SimpleNamespace(monotonic=lambda: next(ticks)))
        _error(local_rpc.dispatch(_request(target=codex_id, timeout_ms=1000)), "incomplete")
    assert native_calls == []
    _error(local_rpc.dispatch(_request(target=codex_id, source_record_target=1)),
           "invalid_request")
    assert native_calls == ["native"]
    with monkeypatch.context() as patch:
        from ccx.codex import telemetry as codex_telemetry
        patch.setattr(codex_telemetry, "_original_source", lambda *_a: (
            _ for _ in ()).throw(SessionUnavailable("original native source unsafe")))
        _error(local_rpc.dispatch(_request(target=codex_id)), "unavailable")
    assert native_calls == ["native"]
    assert all(store.worker_path(wid, "record.json").read_bytes() == raw
               for wid, raw in before.items())
    assert not (constants.CCX_ROOT / "sessions").exists()
    assert not (constants.CCX_ROOT / "run").exists()
