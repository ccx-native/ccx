"""Real V1 full-UUID transcript locator/read over sealed original sources only."""
from __future__ import annotations
import ccx.store.root as store_root
import ccx.ops.report as ops_report

import hashlib
import json
import os
from pathlib import Path

import pytest

from ccx.codex import runtime as codex_runtime
from ccx import constants
from ccx.rpc import local_rpc
from ccx.ops import common as protocol_ops
from ccx.errors import OperationTimeout
from ccx.ops.creation_steps import activate_prepared_worker, prepare_worker
from ccx.store.store import GenerationStore
from ccx.workers import WorkerClient
from test_generation_store import A, B, G2, short_root  # noqa: F401 - fixture
from test_native_conversation import _append, _entry
from test_worker_telemetry import (  # noqa: F401 - fixtures
    _codex_turn, original_codex, original_sources,
)


def _request(op: str, *, timeout_ms: int | None = None, **params) -> dict:
    envelope = {
        "protocol_version": 2, "request_id": "transcript-v1",
        "op": op, "params": params,
    }
    if timeout_ms is not None:
        envelope["timeout_ms"] = timeout_ms
    return envelope


def _result(op: str, **params) -> dict:
    answer = local_rpc.dispatch(_request(op, **params))
    assert answer["ok"] is True, answer
    return answer["result"]


def _refusal(code: str, op: str = "transcript.read", **params) -> None:
    answer = local_rpc.dispatch(_request(op, **params))
    assert answer["protocol_version"] == 2 and answer["request_id"] == "transcript-v1"
    assert answer["ok"] is False and answer["error"]["code"] == code, answer
    assert set(answer["error"]) == {"code", "message", "details"}
    assert "Traceback" not in json.dumps(answer)


def _confirm_claude(store, worker_id: str) -> None:
    rec = store.record(worker_id)
    prepared = prepare_worker(
        store, engine="claude", cwd=rec["cwd"], worker_id=worker_id,
    )
    session_id = rec["native"]["sessionId"]

    def admission(_budget):
        native = store.native_activation(worker_id)
        return {
            "accepted": True, "workerId": worker_id, "engine": "claude",
            "source": f"claude-session:{session_id}",
            "submissionId": native["submissionId"],
            "nativeEvidenceSha256": "b" * 64,
            "nativeBinding": {"sessionId": session_id},
        }

    claimed = activate_prepared_worker(
        store, prepared, bounded_native_submit=admission,
    )
    assert claimed.activation_state == "confirmed"


def _codex_history(tid: str, wid: str, count: int) -> dict:
    rows = []
    for i in range(count):
        row = _codex_turn(tid, wid, turn_id=f"original-turn-{i}")["turns"][0]
        row["items"][0]["id"] = f"original-user-{i}"
        row["items"][1]["id"] = f"original-assistant-{i}"
        row["items"][1]["text"] = f"original reply {i}"
        rows.append(row)
    return {"id": tid, "threadSource": f"ccx-worker:{wid}", "turns": rows}


def _decoded(value: str, prefix: str) -> dict:
    return protocol_ops._decode_opaque(prefix, value, label="test token")


def test_v1_transcript_locator_public_inspect_both_engines_and_embedded_unbound(
    original_codex, monkeypatch, tmp_path,
):
    store, _cwd, codex_id, _thread_id = original_codex
    assert A[:8] == B[:8] and A != B
    # An original owner may exist before the native activation is confirmed.
    unbound = _result("worker.inspect", target=A)
    assert unbound["transcript_locator"] is None
    owner = store.resolve(A)
    embedded = local_rpc._rpc_compact_result(
        store, owner, "nonexistent-operation", {"operation_status": "unknown"},
        observing=True,
    )
    assert embedded["worker"]["transcript_locator"] is None
    corrupt = {**store.record(A), "native": {"sessionId": "contradictory"}}
    assert protocol_ops._transcript_locator_owned(store, owner, corrupt) is None

    _confirm_claude(store, A)
    _confirm_claude(store, B)
    before = {wid: store.worker_path(wid, "record.json").read_bytes()
              for wid in (A, B, codex_id)}
    locators = {}
    for wid, engine in ((A, "claude"), (B, "claude"), (codex_id, "codex")):
        row = _result("worker.inspect", target=wid)
        assert row["worker"] == f"{engine}:{wid}"
        locator = row["transcript_locator"]
        assert isinstance(locator, str) and locator.startswith("ccxt2.")
        assert locator == _result("worker.inspect", target=f"{engine}:{wid}")["transcript_locator"]
        assert locator == protocol_ops.transcript_locator(wid)
        fields = _decoded(locator, "ccxt2.")
        assert fields == {
            "v": 2, "engine": engine, "worker_id": wid,
            "root": hashlib.sha256(os.fsencode(store.root)).hexdigest(),
            "generation": store.generation,
        }
        assert "/" not in locator and "native-thread" not in locator
        locators[wid] = locator
    assert locators[A] != locators[B]

    # The total composer itself never invokes store.read/resolve.
    bound_record = store.record(A)
    with monkeypatch.context() as patch:
        patch.setattr(store, "resolve", lambda *_: pytest.fail("locator resolved again"))
        patch.setattr(store, "record", lambda *_: pytest.fail("locator read a record"))
        assert protocol_ops._transcript_locator_owned(
            store, owner, bound_record,
        ) == locators[A]
    # Pure capability and closed params cannot reach a missing generation.
    with monkeypatch.context() as patch:
        missing = tmp_path / "uninitialized"
        patch.setattr(constants, "CCX_ROOT", missing)
        from ccx.store import store as generation
        patch.setattr(store_root, "require_ccx_active",
                      lambda *_a, **_k: pytest.fail("pure V1 accessed ACTIVE"))
        caps = local_rpc.dispatch(_request("system.info"))
        assert caps["ok"] and set(caps["result"]) == {"protocol_version", "ccx_version"}
        assert caps["result"]["protocol_version"] == 2
        _refusal("invalid_request", locator=locators[A], unexpected=1)
        assert not missing.exists()
    assert all(store.worker_path(wid, "record.json").read_bytes() == data
               for wid, data in before.items())
    assert not (constants.CCX_ROOT / "sessions").exists()
    assert not (constants.CCX_ROOT / "run").exists()


@pytest.mark.parametrize("engine", ("claude", "codex"))
def test_v1_transcript_real_full_history_turn_offset_and_content_revision(
    original_codex, monkeypatch, engine,
):
    store, _cwd, codex_id, tid = original_codex
    calls = []
    if engine == "claude":
        _confirm_claude(store, A)
        wid = A
        source = Path(store.record(A)["transcript"])
        for index in range(3):
            _append(source, _entry("user", f"prompt {index}", uuid=f"cu{index}"),
                    _entry("assistant", f"reply {index}", uuid=f"ca{index}"))
    else:
        wid = codex_id
        thread = _codex_history(tid, wid, 3)

        def history(thread_id, *, cwd, timeout, worker_id, expected_source):
            assert expected_source == store.codex_source(wid)
            assert thread_id == tid and worker_id == wid
            assert cwd == store.record(wid)["cwd"] and 0 < timeout <= 120
            calls.append(timeout)
            return thread

        monkeypatch.setattr(codex_runtime, "read_thread", history)
    monkeypatch.setattr(codex_runtime, "list_thread_items",
                        lambda *_a, **_k: pytest.fail("source-record pager used"))
    locator = _result("worker.inspect", target=wid)["transcript_locator"]
    assert locator
    owner = f"{engine}:{wid}"
    original = WorkerClient().conversation(owner, max_tool_result_bytes=0)
    assert len(original["turns"]) >= 3
    wire = local_rpc.respond(lambda: _request(
        "transcript.read", locator=locator, limit=1, max_tool_result_bytes=0,
    ))
    assert wire.endswith(b"\n") and len(wire) <= local_rpc.MAX_MESSAGE_BYTES
    first = json.loads(wire)
    assert first["ok"] is True, first
    first = first["result"]
    assert first["worker"] == owner
    assert "state" not in first  # Paging keeps a handle, not a WorkerView.
    assert first["turns"] == original["turns"][:1]
    assert first["conversation_schema_version"] == original["schemaVersion"]
    assert first["completeness"] == original["completeness"]
    assert first["evidence"] == original["evidence"]
    assert first["source_revision"] == ops_report._conversation_revision(original)
    assert first["has_more"] and first["next_cursor"].startswith("ccxtc2.")
    token = _decoded(first["next_cursor"], "ccxtc2.")
    assert token["worker_id"] == wid and token["offset"] == 1
    assert token["revision"] == first["source_revision"]
    assert first["source_changed"] is False

    # Worker metadata changes do not change an ordered normalized-turn revision.
    store.mutate_record(wid, patches={"lifecycle.state": "dead"})
    after_metadata = store.worker_path(wid, "record.json").read_bytes()
    second = _result("transcript.read", locator=locator,
                     cursor=first["next_cursor"], limit=1,
                     max_tool_result_bytes=0)
    assert second["source_revision"] == first["source_revision"]
    assert second["source_changed"] is False
    assert second["turns"] == original["turns"][1:2]
    assert second["next_cursor"]

    # Native content changes rebase the position; they do not invalidate it.
    if engine == "claude":
        _append(source, _entry("user", "appended", uuid="cu-next"),
                _entry("assistant", "appended result", uuid="ca-next"))
    else:
        thread["turns"].extend(_codex_history(tid, wid, 4)["turns"][3:])
    third = _result("transcript.read", locator=locator,
                    cursor=second["next_cursor"], limit=1,
                    max_tool_result_bytes=0)
    assert third["source_changed"] is True
    assert third["source_revision"] != first["source_revision"]
    assert third["turns"] and third["has_more"]
    assert _decoded(third["next_cursor"], "ccxtc2.")["offset"] == 3
    fourth = _result("transcript.read", locator=locator,
                     cursor=third["next_cursor"], limit=1,
                     max_tool_result_bytes=0)
    assert fourth["source_changed"] is False
    assert fourth["source_revision"] == third["source_revision"]
    assert fourth["has_more"] is False and fourth["next_cursor"] is None
    if engine == "codex":
        assert calls[0] == 120  # direct Python default unchanged
        assert all(0 < budget <= 90 for budget in calls[1:])
    assert store.worker_path(wid, "record.json").read_bytes() == after_metadata
    assert not store.worker_path(wid, "transcript.json").exists()
    assert not (constants.CCX_ROOT / "sessions").exists()
    assert not (constants.CCX_ROOT / "run").exists()


def test_v1_transcript_typed_token_source_refusals_and_effect_absence(
    original_codex, monkeypatch, tmp_path,
):
    store, _cwd, codex_id, tid = original_codex
    _confirm_claude(store, A)
    _confirm_claude(store, B)
    source_a = Path(store.record(A)["transcript"])
    _append(source_a, _entry("user", "first", uuid="u1"),
            _entry("assistant", "first answer", uuid="a1"),
            _entry("user", "second", uuid="u2"),
            _entry("assistant", "second answer", uuid="a2"))
    native_calls = []
    thread = _codex_history(tid, codex_id, 2)

    def read_thread(thread_id, *, cwd, timeout, worker_id, expected_source):
        assert expected_source == store.codex_source(codex_id)
        native_calls.append((thread_id, worker_id, timeout))
        assert thread_id == tid and worker_id == codex_id and 0 < timeout <= 90
        return thread

    monkeypatch.setattr(codex_runtime, "read_thread", read_thread)
    locator = _result("worker.inspect", target=A)["transcript_locator"]
    b_locator = _result("worker.inspect", target=B)["transcript_locator"]
    codex_locator = _result("worker.inspect", target=codex_id)["transcript_locator"]
    first = _result("transcript.read", locator=locator, limit=1)
    cursor = first["next_cursor"]
    assert cursor is not None
    baseline = {wid: store.worker_path(wid, "record.json").read_bytes()
                for wid in (A, B, codex_id)}
    checks = (
        {"locator": "ccxt1.any"},
        {"locator": "ccxt2.!!!"},
        {"locator": locator, "cursor": "ccxtc1.any"},
        {"locator": locator, "cursor": "ccxcp2.any"},
        {"locator": locator, "cursor": cursor, "limit": True},
        {"locator": locator, "limit": 0},
        {"locator": locator, "detail": "full"},
        {"locator": locator, "detail": "summary"},
        {"locator": locator, "max_tool_result_bytes": -1},
        {"locator": b_locator, "cursor": cursor},
        {"locator": locator, "cursor": protocol_ops._encode_opaque(
            "ccxtc2.", {**_decoded(cursor, "ccxtc2."), "v": 1})},
        {"locator": protocol_ops._encode_opaque(
            "ccxt2.", {**_decoded(locator, "ccxt2."), "root": "0" * 64})},
    )
    for options in checks:
        _refusal("invalid_request", **options)
    assert not native_calls

    _refusal("refused", locator=protocol_ops._encode_opaque(
        "ccxt2.", {**_decoded(locator, "ccxt2."), "generation": G2}))
    _refusal("not_found", locator=protocol_ops._encode_opaque(
        "ccxt2.", {**_decoded(locator, "ccxt2."),
                  "worker_id": "dddddddd-1234-4000-8000-000000000006"}))
    Path(store.record(B)["transcript"]).unlink()
    _refusal("not_found", locator=b_locator)
    assert not native_calls

    # Source identity is proven by the accepted reader, never by a claimed token.
    original_record = GenerationStore.record

    def foreign_source(self, target):
        record = original_record(self, target)
        if record["engine"] == "codex":
            return {**record, "native": {**record["native"],
                                       "threadSource": "ccx-worker:foreign"}}
        return record

    with monkeypatch.context() as patch:
        patch.setattr(GenerationStore, "record", foreign_source)
        _refusal("unavailable", locator=codex_locator)
    assert not native_calls

    with monkeypatch.context() as patch:
        missing = tmp_path / "absent-root"
        patch.setattr(constants, "CCX_ROOT", missing)
        _refusal("invalid_request", locator=locator)
        from ccx.store.root import require_ccx_active
        assert require_ccx_active(missing).list_workers() == []

    with monkeypatch.context() as patch:
        patch.setattr(codex_runtime, "read_thread",
                      lambda *_a, **_k: (_ for _ in ()).throw(
                          OperationTimeout("fake original thread/read deadline")))
        _refusal("incomplete", locator=codex_locator, limit=1)
    assert all(store.worker_path(wid, "record.json").read_bytes() == data
               for wid, data in baseline.items())
    assert not (constants.CCX_ROOT / "sessions").exists()
    assert not (constants.CCX_ROOT / "run").exists()
