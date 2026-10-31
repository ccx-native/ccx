"""Original full-UUID Claude task reads against isolated real native files.

No provider or host is launched. The native session UUID differs from each
public CCX worker UUID; two workers share the same eight display characters.
"""
from __future__ import annotations

import json

import pytest

from ccx import CcxClient
from ccx import ClaudeTask
from ccx import constants
from ccx.store import paths
from ccx.errors import InvalidRequest, SessionNotFound, SessionUnavailable
from ccx.ops.creation_steps import prepare_worker
from ccx.store.root import require_ccx_active
from test_generation_store import A, B, C, short_root
from test_transcript import (
    _agent_launch_events,
    _bg_launch_events,
    _bg_notification_event,
    _monitor_launch_events,
    _monitor_event_notification,
)


@pytest.fixture
def originals(short_root, tmp_path, monkeypatch):
    home = tmp_path / "private-native-home"
    home.mkdir(mode=0o700)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    monkeypatch.setattr(constants, "CLAUDE_HOME", home)
    monkeypatch.setattr(paths, "CLAUDE_HOME", home)
    store = require_ccx_active(short_root)
    for worker_id in (A, B):
        prepare_worker(store, engine="claude", cwd=str(tmp_path), worker_id=worker_id)
    first = store.record(A)
    second = store.record(B)
    first_sid = first["native"]["sessionId"]
    second_sid = second["native"]["sessionId"]
    assert A[:8] == B[:8] and first_sid != second_sid
    assert first_sid not in (A, B) and second_sid not in (A, B)
    assert first["short"] == second["short"] == A[:8]
    return store, home, tmp_path, first_sid, second_sid


def _native_task(home, sid, filename, payload):
    root = home / "tasks" / sid
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    root.chmod(0o700)
    path = root / filename
    path.write_text(json.dumps(payload), encoding="utf-8")
    path.chmod(0o600)
    return path


def _original_jsonl(home, cwd, sid, events):
    path = paths.transcript_path_for(str(cwd), sid)
    assert path.is_relative_to(home)
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    (home / "projects").chmod(0o700)
    path.parent.chmod(0o700)
    path.write_text(
        "\n".join(json.dumps(event, separators=(",", ":")) for event in events) + "\n",
        encoding="utf-8",
    )
    path.chmod(0o600)
    return path


def _unchanged(path):
    stat = path.stat()
    return path.read_bytes(), stat.st_mtime_ns


def test_structured_original_native_sids_are_isolated_tolerant_and_read_only(originals):
    store, home, _cwd, sid_a, sid_b = originals
    earlier = _native_task(
        home, sid_a, "10.json",
        {"id": "10", "subject": "later", "status": "pending",
         "blocks": ["11", 12], "activeForm": "later task"},
    )
    first = _native_task(
        home, sid_a, "2.json",
        {"id": "2", "subject": "first", "status": "in_progress"},
    )
    _native_task(
        home, sid_a, "internal.json",
        {"id": "1", "subject": "hidden", "metadata": {"_internal": True}},
    )
    bad = home / "tasks" / sid_a / "broken.json"
    bad.write_text("{", encoding="utf-8")
    _native_task(home, sid_a, "no-id.json", {"subject": "partial"})
    _native_task(home, sid_a, ".hidden.json", {"id": "0", "subject": "hidden"})
    sibling = _native_task(
        home, sid_b, "1.json",
        {"id": "1", "subject": "sibling only", "status": "completed"},
    )
    # The unchanged native task reader must continue refusing file symlinks.
    (home / "tasks" / sid_a / "symlink.json").symlink_to(sibling)
    before = {p: _unchanged(p) for p in (earlier, first, sibling, bad)}
    record_a, record_b = store.record(A), store.record(B)
    client = CcxClient()
    tasks_a = client.tasks("claude:" + A)
    assert all(isinstance(task, ClaudeTask) for task in tasks_a)
    assert [task.id for task in tasks_a] == ["2", "10"]
    assert [task.subject for task in tasks_a] == ["first", "later"]
    assert tasks_a[1].blocks == ["11"]
    assert tasks_a[1].active_form == "later task"
    assert [(task.id, task.subject) for task in client.tasks(B)] == [
        ("1", "sibling only"),
    ]
    with pytest.raises(InvalidRequest):
        client.tasks(A[:8])
    with pytest.raises(SessionNotFound):
        client.tasks(sid_a)  # Native SID is not a public worker target.
    with pytest.raises(SessionNotFound):
        client.tasks(C)
    with pytest.raises(SessionUnavailable, match="live=True"):
        client.tasks(A, live=True)
    assert {p: _unchanged(p) for p in before} == before
    assert store.record(A) == record_a and store.record(B) == record_b
    assert not (home / "tasks" / sid_a / ".lock").exists()


def test_original_native_background_bash_agent_monitor_and_dead_liveness(originals):
    from ccx.ops.forking import inherited_task_filter

    store, home, cwd, sid_a, sid_b = originals
    a_events = [
        *_bg_launch_events(task_id="bash-done", tool_use_id="tool-bash-done"),
        _bg_notification_event(task_id="bash-done", tool_use_id="tool-bash-done"),
        *_agent_launch_events(agent_id="agent-running", tool_use_id="tool-agent"),
        *_monitor_launch_events(task_id="monitor-running", tool_use_id="tool-mon"),
        _monitor_event_notification(task_id="monitor-running", event="heartbeat"),
    ]
    original_source = _original_jsonl(home, cwd, sid_a, a_events)
    sibling_source = _original_jsonl(
        home, cwd, sid_b,
        _bg_launch_events(task_id="sibling-only", tool_use_id="tool-sibling"),
    )
    original_snapshot = _unchanged(original_source)
    sibling_snapshot = _unchanged(sibling_source)
    first_record, second_record = store.record(A), store.record(B)
    client = CcxClient()

    # A prepared record has no host socket: even with no terminal lifecycle
    # marker, advisory background liveness must use the full owner handle.
    tasks = client.background_tasks("claude:" + A)
    by_id = {task["task_id"]: task for task in tasks}
    assert set(by_id) == {"bash-done", "agent-running", "monitor-running"}
    assert by_id["bash-done"]["status"] == "completed"
    assert by_id["bash-done"]["liveness"] == "settled"
    assert by_id["bash-done"]["active"] is False
    assert by_id["agent-running"]["kind"] == "subagent"
    assert by_id["agent-running"]["status"] == "running"
    assert by_id["agent-running"]["liveness"] == "stale"
    assert by_id["agent-running"]["active"] is False
    assert by_id["monitor-running"]["kind"] == "monitor"
    assert by_id["monitor-running"]["status"] == "running"
    assert by_id["monitor-running"]["event_count"] == 1
    assert by_id["monitor-running"]["last_event"] == "heartbeat"
    assert by_id["monitor-running"]["liveness"] == "stale"
    assert by_id["monitor-running"]["active"] is False

    # The single existing pure inherited filter stays in the composed reader;
    # background tasks forked from source history are not the fork's open work.
    # The cutoff is authorized by the original child/native/anchor proof,
    # never by a mutable short-keyed forkedFrom.inheritedOpenTaskIds field.
    anchor = {"engine": "claude", "sourceWorkerId": A,
              "sourceNativeSessionId": sid_a, "resumeAt": "source-final"}
    child_sid = "cccccccc-1234-4000-8000-000000000007"
    own = inherited_task_filter({
        "engine": "claude", "workerId": C, "native": {"sessionId": child_sid},
        "forkAnchor": anchor, "forkedFrom": dict(anchor),
        "lifecycle": {"nativeForkProof": {
            "workerId": C, "nativeSessionId": child_sid, "forkAnchor": anchor,
            "copiedAtMs": 1, "inheritedBytes": 80,
            "inheritedOpenTaskIds": ["agent-running"],
        }},
    }, tasks)
    assert "agent-running" not in {task["task_id"] for task in own}
    assert [task["task_id"] for task in client.background_tasks(B)] == [
        "sibling-only",
    ]
    info = client._uuid_background_task_facts(A)
    assert info["background_task_count"] == 3
    assert info["running_background_task_count"] == 2
    assert info["open_background_task_count"] == 2
    assert info["active_background_task_count"] == 0
    assert info["stale_background_task_count"] == 2
    assert info["background_tasks_truncated"] is False

    # Explicitly stopped original remains transcript-running / advisory stale,
    # not an assertion about a current native turn or reason to revive.
    store.mutate_record(A, patches={"lifecycle.state": "dead"})
    assert all(
        task["liveness"] in {"settled", "stale"}
        for task in client.background_tasks(A)
    )
    assert store.record(A)["lifecycle"]["state"] == "dead"
    assert _unchanged(original_source) == original_snapshot
    assert _unchanged(sibling_source) == sibling_snapshot
    assert store.record(B) == second_record
    assert store.record(A)["stats"] == first_record["stats"]


def test_live_background_liveness_keys_the_host_lookup_by_full_worker_id(
    originals, monkeypatch,
):
    from ccx.cli.commands import _common as commands

    store, home, cwd, sid_a, _sid_b = originals
    _original_jsonl(home, cwd, sid_a, _monitor_launch_events(
        task_id="monitor-live", tool_use_id="tool-live",
    ))
    keys = []

    def keyed_lookup(key, _rec):
        keys.append(key)
        return key == A  # the display short would never be live

    monkeypatch.setattr("ccx.rpc.rpc.host_socket_live", keyed_lookup)
    facts = CcxClient()._uuid_background_task_facts(A)
    assert keys == [A]
    [task] = facts["background_tasks"]
    assert task["liveness"] == "live" and task["active"] is True
    assert f"ccx wait {A} --next-turn" in facts["background_task_hint"]


def test_missing_original_jsonl_is_empty_no_revive_or_record_change(originals):
    store, home, _cwd, _sid_a, _sid_b = originals
    # Absence is distinct from an unsafe source and does not make a cache.
    before = store.record(A)
    native_before = list(home.iterdir())
    assert CcxClient().background_tasks(A) == []
    assert CcxClient()._uuid_background_task_facts(A)["background_task_count"] == 0
    assert CcxClient().tasks(A) == []
    assert store.record(A) == before
    assert list(home.iterdir()) == native_before
    with pytest.raises(InvalidRequest):
        CcxClient().background_tasks(A[:8])
    with pytest.raises(SessionUnavailable, match="live=True"):
        CcxClient().background_tasks(A, live=True)


def test_foreign_transcript_path_refuses_instead_of_borrowing_source(originals):
    store, home, cwd, sid_a, sid_b = originals
    original = _original_jsonl(
        home, cwd, sid_a,
        _bg_launch_events(task_id="original-only", tool_use_id="tool-original"),
    )
    sibling = _original_jsonl(
        home, cwd, sid_b,
        _bg_launch_events(task_id="foreign-only", tool_use_id="tool-foreign"),
    )
    snapshots = {_path: _unchanged(_path) for _path in (original, sibling)}
    assert [task["task_id"] for task in CcxClient().background_tasks(A)] == [
        "original-only",
    ]
    # Only corrupt this disposable fixture: the real generation API correctly
    # prohibits changing its immutable provider transcript field via CAS.
    record_path = store.worker_path(A)
    tampered = json.loads(record_path.read_text(encoding="utf-8"))
    tampered["transcript"] = str(sibling)
    record_path.write_text(json.dumps(tampered), encoding="utf-8")
    record_bytes = _unchanged(record_path)
    # A mismatched original source cannot be borrowed as a tolerant empty or
    # as the sibling's task list; no native file or record is rewritten on read.
    with pytest.raises(SessionUnavailable):
        CcxClient().background_tasks(A)
    assert _unchanged(record_path) == record_bytes
    assert {_path: _unchanged(_path) for _path in snapshots} == snapshots
    assert [task["task_id"] for task in CcxClient().background_tasks(B)] == [
        "foreign-only",
    ]
