"""Original V1 worker.inspect background facts on sealed, file-backed owners.

The existing original Claude source and parser are exercised, never a fake
handler or a real provider. Only background fields are newly projected.
"""
from __future__ import annotations
import ccx.store.root as store_root
import ccx.ops.report as ops_report
import ccx.ops.wait as ops_wait

import json

import pytest

from ccx import CcxClient, WorkerClient
from ccx.rpc import local_rpc
from ccx.rpc.local_rpc import dispatch_full_uuid
from ccx.ops.creation_steps import prepare_worker
from test_generation_store import A, B, C, add_worker, short_root
from test_v1_worker_protocol import request
from test_worker_task_reads import originals, _original_jsonl, _unchanged
from test_transcript import (
    _agent_launch_events,
    _bg_launch_events,
    _bg_notification_event,
    _monitor_event_notification,
    _monitor_launch_events,
)


def _inspect(store, worker):
    result = dispatch_full_uuid(request("worker.inspect", worker), store)
    assert result["protocol_version"] == 2
    return result


def test_recorded_snapshot_in_existing_background_rpc_block(originals):
    store, home, cwd, sid, _ = originals
    _original_jsonl(home, cwd, sid, _bg_launch_events(
        task_id="task-1", tool_use_id="tool-1"))
    summary = {"runningCount": 1, "kinds": {"bash": 1, "monitor": 0,
               "subagent": 0}, "asOfMs": 1700000010000, "monitors": []}
    store.mutate_record(A, patches={"backgroundTaskSummary": summary})
    row = _inspect(store, A)["result"]["worker_view"]["background_tasks"]
    assert row["summary"] == summary
    assert row["running_count"] == 1
    # Ordinary RPC inspect uses the recorded snapshot, not a transcript scan.
    assert row["tasks"] == []
    assert WorkerClient().status(A)["background_task_summary"] == summary


def test_worker_facade_wait_and_report_share_bounded_background_facts(originals, monkeypatch):
    from ccx.ops import common as protocol_ops

    store, home, cwd, sid_a, _sid_b = originals
    _original_jsonl(home, cwd, sid_a, _bg_launch_events(
        task_id="worker-background", tool_use_id="tool-background",
    ))
    add_worker(store.root, C, "codex")
    monkeypatch.setattr(ops_wait, "wait_worker", lambda handle, **_kw: {
        "worker": handle, "state": "idle", "status": "idle",
    })
    monkeypatch.setattr(ops_report, "report_worker", lambda _handle, **_kw: {
        "text": "finished", "truncated": False, "has_final_message": True,
        "status": "completed", "source": "original",
    })
    client = WorkerClient()
    claude_wait = client.wait(A, timeout=0)
    claude_report = client.report(A)
    for row in (claude_wait, claude_report):
        assert row["background_tasks_supported"] is True
        assert row["background_task_count"] == 1
        assert row["running_background_task_count"] == 1
        assert row["open_background_task_count"] == 1
        assert row["active_background_task_count"] == 0
        assert [task["task_id"] for task in row["background_tasks"]] == ["worker-background"]
        assert row["background_task_details_truncated"] is False

    with monkeypatch.context() as patch:
        patch.setattr(CcxClient, "_uuid_background_task_facts_owned", staticmethod(
            lambda *_a: pytest.fail("Codex must not read Claude background tasks")))
        codex_wait = client.wait(C, timeout=0)
        codex_report = client.report(C)
    for row in (codex_wait, codex_report):
        assert row["background_tasks_supported"] is False
        assert "background_tasks" not in row
        assert not any("background_task_count" in key for key in row)


def test_original_claude_siblings_and_main_inspect_fields_remain_isolated(originals):
    store, home, cwd, first_sid, second_sid = originals
    first_events = [
        *_bg_launch_events(task_id="bash-settled", tool_use_id="bash-tool"),
        _bg_notification_event(task_id="bash-settled", tool_use_id="bash-tool"),
        *_agent_launch_events(agent_id="agent-open", tool_use_id="agent-tool"),
        *_monitor_launch_events(task_id="monitor-open", tool_use_id="monitor-tool"),
        _monitor_event_notification(task_id="monitor-open", event="tick"),
    ]
    first_file = _original_jsonl(home, cwd, first_sid, first_events)
    second_file = _original_jsonl(
        home, cwd, second_sid,
        _bg_launch_events(task_id="sibling-only", tool_use_id="sibling-tool"),
    )
    paths = (store.worker_path(A), store.worker_path(B), first_file, second_file)
    snapshots = {path: _unchanged(path) for path in paths}
    record_a, record_b = store.record(A), store.record(B)
    assert first_sid != second_sid and A[:8] == B[:8]

    a = _inspect(store, f"claude:{A}")
    b = _inspect(store, B)
    assert a["ok"] and b["ok"], (a, b)
    aa, bb = a["result"], b["result"]

    # V2 groups the already-bounded background facts without another source read.
    from ccx.rpc.view import WORKER_KEYS
    for public, worker, record in ((aa, A, record_a), (bb, B, record_b)):
        row = public["worker_view"]
        assert public["worker"] == f"claude:{worker}"
        assert set(row) == set(WORKER_KEYS)
        assert row["worker"] == f"claude:{worker}"
        assert row["name"] == record["name"] and row["cwd"] == record["cwd"]
        assert row["state_source"] == "recorded" and row["loaded"] is None
        assert row["native_state"] == record["lifecycle"]["state"]
        assert row["native_id"] == record["native"]["sessionId"]
        assert row["context"]["reason"] == "no-signal"
        assert row["background_tasks"]["supported"] is True
        assert row["background_tasks"]["truncated"] is False
    aa_tasks = {task["task_id"]: task for task in aa["worker_view"]["background_tasks"]["tasks"]}
    assert set(aa_tasks) == {"bash-settled", "agent-open", "monitor-open"}
    assert aa_tasks["bash-settled"]["status"] == "completed"
    assert aa_tasks["agent-open"]["liveness"] == "stale"
    assert aa_tasks["monitor-open"]["last_event"] == "tick"
    assert aa["worker_view"]["background_tasks"]["running_count"] == 2
    assert [task["task_id"] for task in bb["worker_view"]["background_tasks"]["tasks"]] == ["sibling-only"]
    assert store.record(A) == record_a and store.record(B) == record_b
    assert {path: _unchanged(path) for path in paths} == snapshots
    assert not (store.root / "run").exists()


def test_bounded_v1_background_text_and_list_count_in_one_real_jsonl(originals):
    store, home, cwd, native_sid, _ = originals
    events = []
    for i in range(15):
        events.extend(_bg_launch_events(
            task_id=f"task-{i:02d}", tool_use_id=f"tool-{i:02d}",
            command="long-command:" + "x" * 4900,
            description="long-description:" + "y" * 4400,
        ))
    source = _original_jsonl(home, cwd, native_sid, events)
    before = _unchanged(source)
    result = _inspect(store, A)
    assert result["ok"], result
    row = result["result"]

    # V2 keeps the original 64 KiB bound and clips text in the same way.
    block = row["worker_view"]["background_tasks"]
    assert block["supported"] is True
    assert block["running_count"] == 15
    assert len(block["tasks"]) == 10
    assert block["truncated"] is True
    assert block["tasks"][0]["task_id"] == "task-05"
    assert block["tasks"][-1]["task_id"] == "task-14"
    for task in block["tasks"]:
        assert len(task["command"]) == 1024
        assert len(task["description"]) == 1024
        assert task["liveness"] == "stale" and task["active"] is False
    assert len(json.dumps(block, ensure_ascii=False).encode("utf-8")) <= 64 * 1024
    assert _unchanged(source) == before


def test_missing_original_is_empty_but_foreign_binding_fails_entire_inspect(originals):
    store, home, cwd, sid_a, sid_b = originals
    record_a, record_b = store.record(A), store.record(B)
    rec_path_a, rec_path_b = store.worker_path(A), store.worker_path(B)
    before = {p: _unchanged(p) for p in (rec_path_a, rec_path_b)}
    native_root_before = sorted(str(p) for p in home.iterdir())
    empty = _inspect(store, A)
    assert empty["ok"], empty
    facts = empty["result"]["worker_view"]["background_tasks"]
    assert facts == {"supported": True, "tasks": [],
                     "running_count": 0, "truncated": False}
    assert {p: _unchanged(p) for p in before} == before
    assert sorted(str(p) for p in home.iterdir()) == native_root_before
    assert store.record(A) == record_a and store.record(B) == record_b

    # A valid stopped original without upstream JSONL is still an empty read,
    # not an integrity fault or a reason to revive the native provider.
    store.mutate_record(A, patches={"lifecycle.state": "dead"})
    stopped_before = _unchanged(rec_path_a)
    stopped = _inspect(store, A)
    assert stopped["ok"] and stopped["result"]["worker_view"]["state"] == "stopped"
    assert stopped["result"]["worker_view"]["native_state"] == "dead"
    assert stopped["result"]["worker_view"]["background_tasks"]["tasks"] == []
    assert _unchanged(rec_path_a) == stopped_before
    assert sorted(str(p) for p in home.iterdir()) == native_root_before

    original = _original_jsonl(
        home, cwd, sid_a,
        _bg_launch_events(task_id="owner-a", tool_use_id="original-tool"),
    )
    sibling = _original_jsonl(
        home, cwd, sid_b,
        _bg_launch_events(task_id="owner-b", tool_use_id="sibling-tool"),
    )
    source_before = {p: _unchanged(p) for p in (original, sibling)}
    assert _inspect(store, A)["result"]["worker_view"]["background_tasks"]["tasks"][0]["task_id"] == "owner-a"
    assert _inspect(store, B)["result"]["worker_view"]["background_tasks"]["tasks"][0]["task_id"] == "owner-b"

    # A test-only foreign pointer; the production CAS correctly forbids it.
    corrupted = json.loads(rec_path_a.read_text())
    corrupted["transcript"] = str(sibling)
    rec_path_a.write_text(json.dumps(corrupted), encoding="utf-8")
    damaged_before = _unchanged(rec_path_a)
    error = _inspect(store, A)
    assert error["ok"] is False and error["error"]["code"] == "unavailable"
    assert "result" not in error  # No misleading partial recorded+empty inventory.
    assert _unchanged(rec_path_a) == damaged_before
    assert _unchanged(rec_path_b) == before[rec_path_b]
    assert {p: _unchanged(p) for p in source_before} == source_before
    assert _inspect(store, B)["result"]["worker_view"]["background_tasks"]["tasks"][0]["task_id"] == "owner-b"


def test_codex_unsupported_and_root_free_capabilities_no_new_operations(
    originals, monkeypatch,
):
    store, home, cwd, _sid_a, _sid_b = originals
    prepare_worker(store, engine="codex", cwd=str(cwd), worker_id=C)
    before = store.record(C)
    path = store.worker_path(C)
    raw = _unchanged(path)
    # Codex's background block must not ask for any Claude native JSONL.
    with monkeypatch.context() as patch:
        patch.setattr(
            CcxClient, "_uuid_background_task_facts_owned",
            staticmethod(lambda *_: pytest.fail("Codex must not read Claude source")),
        )
        value = _inspect(store, f"codex:{C}")
    assert value["ok"], value
    row = value["result"]["worker_view"]
    assert row["worker"] == f"codex:{C}"
    assert row["state_source"] == "recorded"
    assert row["background_tasks"]["supported"] is False
    assert row["background_tasks"]["tasks"] == []
    assert store.record(C) == before and _unchanged(path) == raw

    from ccx.store import store as generation

    with monkeypatch.context() as patch:
        patch.setattr(
            store_root, "require_ccx_active",
            lambda *_a, **_k: pytest.fail("pure V1 request touched ACTIVE root"),
        )
        capabilities = local_rpc.dispatch(request("system.info"))
        invalid = local_rpc.dispatch(request("worker.inspect", A, surprise="no"))
        ghost_tasks = local_rpc.dispatch(request("worker.tasks", A))
        ghost_background = local_rpc.dispatch(request("worker.background_tasks", A))

    assert capabilities["ok"] is True
    value = capabilities["result"]
    assert set(value) == {"protocol_version", "ccx_version"}
    assert value["protocol_version"] == 2
    assert invalid["error"]["code"] == "invalid_request"
    assert ghost_tasks["error"]["code"] == "invalid_request"
    assert ghost_background["error"]["code"] == "invalid_request"
    for target in (A[:8], "claude:" + A[:8], "claude-session:" + A):
        rejected = _inspect(store, target)
        assert rejected["ok"] is False
        assert rejected["error"]["code"] == "invalid_request"
    assert not (store.root / "run").exists()

def test_codex_background_record_status_and_rpc_inspect(originals):
    from ccx.ops.background import summary
    from test_generation_store import add_worker, C

    store, *_ = originals
    add_worker(store.root, C, "codex")
    expected = summary([{
        "id": "command:exec-test", "kind": "command",
        "status": "running", "label": "native shell",
    }], as_of_ms=1793548540000)
    store.mutate_record(C, patches={"backgroundTaskSummary": expected})
    assert WorkerClient().status(C)["background_task_summary"] == expected
    block = _inspect(store, C)["result"]["worker_view"]["background_tasks"]
    assert block["supported"] is True
    assert block["running_count"] == 1
    assert block["summary"] == expected
    assert block["tasks"] == []  # recorded, never native thread/read
