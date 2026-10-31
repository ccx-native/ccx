from __future__ import annotations

import json

from ccx import ClaudeTask
from ccx.store.paths import task_list_dir_for
from ccx.claude.tasks import read_task_list


def _write_task(root, name: str, payload) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / name).write_text(json.dumps(payload), encoding="utf-8")


def test_read_task_list_normalizes_and_sorts_numeric_ids():
    sid = "12345678-0000-0000-0000-000000000000"
    root = task_list_dir_for(sid)
    assert root is not None
    _write_task(root, "10.json", {
        "id": "10", "subject": "Later", "description": "d", "activeForm": "Doing later",
        "status": "pending", "owner": "agent-a", "blocks": ["11", 12], "blockedBy": ["2"],
        "futureField": {"safe": True},
    })
    _write_task(root, "2.json", {
        "id": "2", "subject": "Sooner", "status": "in_progress",
    })

    tasks = read_task_list(sid)

    assert [task.id for task in tasks] == ["2", "10"]
    assert tasks[0] == ClaudeTask(id="2", subject="Sooner", status="in_progress")
    assert tasks[1].as_dict() == {
        "id": "10",
        "subject": "Later",
        "status": "pending",
        "active_form": "Doing later",
        "description": "d",
        "owner": "agent-a",
        "blocks": ["11"],
        "blocked_by": ["2"],
    }


def test_read_task_list_tolerates_missing_corrupt_partial_and_internal_files():
    sid = "abcdefab-0000-0000-0000-000000000000"
    root = task_list_dir_for(sid)
    assert root is not None
    assert read_task_list(sid) == []

    root.mkdir(parents=True)
    (root / ".lock").write_text("", encoding="utf-8")
    (root / ".highwatermark").write_text("10", encoding="utf-8")
    (root / "bad.json").write_text("{", encoding="utf-8")
    _write_task(root, "internal.json", {
        "id": "7", "subject": "hidden", "status": "pending", "metadata": {"_internal": True},
    })
    _write_task(root, "missing-id.json", {"subject": "partial", "status": "pending"})
    _write_task(root, "future.json", {"id": "future", "subject": "future", "status": "blocked-new-status"})

    tasks = read_task_list(sid)
    assert [task.id for task in tasks] == ["future"]
    assert tasks[0].status == "blocked-new-status"


def test_reader_does_not_follow_symlink(tmp_path):
    sid = "feedfeed-0000-0000-0000-000000000000"
    root = task_list_dir_for(sid)
    assert root is not None
    root.mkdir(parents=True)
    outside = tmp_path / "outside.json"
    outside.write_text(json.dumps({"id": "1", "subject": "outside", "status": "pending"}))
    (root / "1.json").symlink_to(outside)
    assert read_task_list(sid) == []


def test_malformed_session_id_cannot_escape_task_root():
    assert task_list_dir_for("../outside") is None
    assert task_list_dir_for("") is None
