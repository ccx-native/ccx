"""V2 worker projection does not mutate records or read native sources."""
from __future__ import annotations

from copy import deepcopy

from ccx.rpc.view import WORKER_KEYS, background_view, context_view, project_worker_view
from test_generation_store import A, B, record


def test_pure_recorded_worker_shape_and_provenance():
    for engine, worker_id in (("claude", A), ("codex", B)):
        rec = record(worker_id, engine)
        rec.update(lifecycle={"state": "idle-killed"}, stats={"turns": 3},
                   createdAt=123456, createArgs={"model": "test", "effort": "high"})
        before = deepcopy(rec)
        row = project_worker_view(f"{engine}:{worker_id}", rec)
        assert rec == before
        assert tuple(row) == WORKER_KEYS and len(row) == 21
        assert row["worker"] == f"{engine}:{worker_id}"
        assert row["state"] == "stopped"
        assert row["native_state"] == "idle-killed"
        assert row["state_source"] == "recorded"
        assert row["loaded"] is None
        assert row["pending_input_observed"] is False
        assert row["context"] is None
        assert row["background_tasks"] is None
        assert row["managed_turns"] == 3


def test_completed_no_signal_context_and_unsupported_background():
    ctx = context_view(None)
    assert ctx == {"tokens_used": None, "window_size": None,
                   "percent_used": None, "reason": "no-signal", "as_of_ms": None,
                   "as_of_turn_ref": None}
    assert background_view({"background_tasks_supported": False}) == {
        "supported": False, "tasks": [], "running_count": None, "truncated": False}
    tasks = background_view({"background_tasks_supported": True,
                             "background_tasks": [{"id": "bg"}],
                             "running_background_task_count": 1,
                             "background_task_details_truncated": True})
    assert tasks == {"supported": True, "tasks": [{"id": "bg"}],
                     "running_count": 1, "truncated": True}
    snapshot = {"runningCount": 1, "kinds": {"bash": 1, "monitor": 0,
                "subagent": 0}, "monitors": [], "asOfMs": 1234}
    enriched = background_view({"background_tasks_supported": True,
                                "background_task_summary": snapshot})
    assert enriched["summary"] == snapshot
    assert enriched["tasks"] == []  # no invented detail from the record snapshot


def test_nested_settlement_is_normalized_without_changing_receipt():
    receipt = {"turn_ref": "exact-ref", "outcome": "completed",
               "turn_id": None, "completed_at": 12, "extra": "private"}
    before = receipt.copy()
    row = project_worker_view("claude:" + A, record(A, "claude"),
                              last_settled_turn=receipt)
    assert receipt == before
    assert row["last_settled_turn"] == {
        "turn_ref": "exact-ref", "turn_id": None, "outcome": "completed",
        "completed_at": 12, "context": None}


def _v2(op, **params):
    return {"protocol_version": 2, "request_id": "v2-smoke", "op": op,
            "params": params}


def test_root_free_info_rejects_v1_without_looking_up_root(monkeypatch, tmp_path):
    from ccx import constants
    from ccx.rpc import local_rpc
    monkeypatch.setattr(constants, "CCX_ROOT", tmp_path / "nonexistent")
    info = local_rpc.dispatch(_v2("system.info"))
    assert info["ok"] and info["protocol_version"] == 2
    assert set(info["result"]) == {"protocol_version", "ccx_version"}
    assert info["result"]["protocol_version"] == 2
    for old in (_v2("system.capabilities"),
                dict(_v2("system.info"), protocol_version=1)):
        rejected = local_rpc.dispatch(old)
        assert rejected["ok"] is False
        assert rejected["error"]["code"] == "invalid_request"
        assert set(rejected["error"]) == {"code", "message", "details"}


def test_v2_inspect_context_and_wait_recorded_with_no_extra_native(tmp_path, monkeypatch):
    from ccx.rpc import local_rpc
    from ccx.store.root import require_ccx_active
    from test_generation_store import fixture_root, add_worker
    root = fixture_root(tmp_path)
    from ccx import constants
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    store = require_ccx_active(root)
    from ccx.ops.creation_steps import prepare_worker
    prepare_worker(store, engine="claude", cwd=str(tmp_path), worker_id=A)
    # Does not read a live host. A synthetic no-signal context is still an object.
    inspected = local_rpc.dispatch_full_uuid(_v2("worker.inspect", target=A), store)
    assert inspected["ok"], inspected
    row = inspected["result"]["worker_view"]
    assert inspected["result"]["worker"] == "claude:" + A
    assert tuple(row) == WORKER_KEYS
    assert row["state_source"] == "recorded"
    assert row["loaded"] is None
    assert row["context"]["reason"] == "no-signal"
    assert "transcript_locator" in inspected["result"]

    calls = []
    def wait(target, *, timeout, **selectors):
        calls.append((target, timeout, selectors))
        return {"worker": "claude:" + A, "status": "timeout",
                "timed_out": True, "reason": "still busy",
                "last_settled_turn": None, "pending_input": None}
    monkeypatch.setattr(local_rpc, "wait_worker", wait)
    waited = local_rpc.dispatch_full_uuid(_v2("worker.wait", target=A), store)
    assert waited["ok"], waited
    assert calls and calls[0][0] == A
    assert waited["result"]["status"] == "timeout"
    assert waited["result"]["worker"] == "claude:" + A
    assert waited["result"]["worker_view"]["state_source"] == "recorded"
    assert tuple(waited["result"]["worker_view"]) == WORKER_KEYS
    assert waited["result"]["worker_view"]["loaded"] is None


def test_v2_unsupported_typed_and_report_claude_null(tmp_path, monkeypatch):
    from ccx.rpc import local_rpc
    from ccx.errors import CapabilityUnsupported
    from ccx.store.root import require_ccx_active
    from test_generation_store import fixture_root, add_worker
    root = fixture_root(tmp_path)
    add_worker(root, B, "codex")
    add_worker(root, A, "claude")
    store = require_ccx_active(root)

    def unsupported(*args, **kwargs):
        raise CapabilityUnsupported("Codex cannot answer questions",
                                    engine="codex", capability="answer_question")
    monkeypatch.setattr(local_rpc, "answer_input", unsupported)
    err = local_rpc.dispatch_full_uuid(
        _v2("input.answer", target="codex:" + B,
            request_id="gate", response_id="answer", answers={}), store)
    assert err["error"]["code"] == "unsupported"
    assert err["error"]["details"]["engine"] == "codex"
    assert err["error"]["details"]["operation"] == "input.answer"

    raw = {"worker": "claude:" + A, "turn_ref": "turn-ref", "text": "",
           "status": "completed", "outcome": None,
           "truncated": False, "has_final_message": False}
    monkeypatch.setattr(local_rpc, "report_worker", lambda *args, **kwargs: raw)
    result = local_rpc.dispatch_full_uuid(_v2("worker.report", target=A), store)
    assert result["ok"], result
    assert result["result"]["worker"] == "claude:" + A
    assert result["result"]["turn_id"] is None
    assert "outcome" in result["result"] and result["result"]["outcome"] is None
    assert raw.get("turn_id") is None and "turn_id" not in raw


def test_v2_worker_kill_name_without_alias(tmp_path, monkeypatch):
    from ccx.rpc import local_rpc
    from ccx.store.root import require_ccx_active
    from test_generation_store import fixture_root, add_worker
    root = fixture_root(tmp_path)
    add_worker(root, A)
    store = require_ccx_active(root)
    monkeypatch.setattr(local_rpc, "park_worker",
                        lambda *args, **kwargs: {
                            "worker": "claude:" + A, "killed": True,
                            "changed": True, "interrupted": False,
                            "resumable": True, "state": "stopped"})
    old = local_rpc.dispatch_full_uuid(_v2("worker.park", target=A), store)
    assert not old["ok"]
    new = local_rpc.dispatch_full_uuid(_v2("worker.kill", target=A), store)
    assert new["ok"] and new["result"]["worker"] == "claude:" + A
    assert "worker_id" not in new["result"]



def test_v2_worker_report_keeps_outcome_for_completed_failed_and_interrupted(
    tmp_path, monkeypatch,
):
    from ccx.rpc import local_rpc
    from ccx.store.root import require_ccx_active
    from test_generation_store import fixture_root, add_worker

    root = fixture_root(tmp_path)
    add_worker(root, A, "claude")
    store = require_ccx_active(root)
    for status, outcome in (
        ("completed", None), ("failed", None), ("interrupted", "interrupted"),
    ):
        raw = {
            "worker": "claude:" + A, "status": status, "outcome": outcome,
            "text": "", "has_final_message": False,
        }
        monkeypatch.setattr(local_rpc, "report_worker",
                            lambda *args, **kwargs: raw)
        response = local_rpc.dispatch_full_uuid(
            _v2("worker.report", target=A), store)
        assert response["ok"], response
        result = response["result"]
        assert result["status"] == status and result["outcome"] == outcome
        assert result["text"] == "" and result["has_final_message"] is False
