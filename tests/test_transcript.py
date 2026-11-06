'Transcript cleanup behavior (ccx.claude.transcript).\n\nAll functions here operate on plain dicts or JSONL files we hand them, so the\ntests need no Claude process — only fixture text on disk.\n'

from __future__ import annotations

import json
import os
from pathlib import Path
import stat

import pytest

from ccx.claude import transcript as T


# --- _parse_event_ts_ms ------------------------------------------------------

def test_parse_event_ts_iso8601():
    evt = {"timestamp": "2026-01-01T00:00:00Z"}
    ms = T._parse_event_ts_ms(evt)
    assert isinstance(ms, int)
    # 2026-01-01T00:00:00Z in epoch ms
    assert ms == 1767225600000


def test_parse_event_ts_epoch_seconds_promoted_to_ms():
    # A value that is clearly seconds (< 1e12) is multiplied up to ms.
    assert T._parse_event_ts_ms({"timestamp": 1000}) == 1000 * 1000


def test_parse_event_ts_epoch_ms_passthrough():
    assert T._parse_event_ts_ms({"timestamp": 1767225600000}) == 1767225600000


def test_parse_event_ts_missing_or_garbage():
    assert T._parse_event_ts_ms({}) is None
    assert T._parse_event_ts_ms({"timestamp": "not-a-date"}) is None


# --- queued_prompt_accepted --------------------------------------------------

def test_queued_prompt_accepted_detects_native_queue_operation(tmp_path):
    p = tmp_path / "session.jsonl"
    p.write_text("\n".join([
        json.dumps({"type": "queue-operation", "operation": "enqueue",
                    "timestamp": "2026-01-01T00:00:01Z", "content": "hello"}),
        json.dumps({"type": "attachment", "attachment": {"type": "queued_command",
                    "prompt": "hello"}}),
    ]))

    assert T.queued_prompt_accepted(p, "hello")


def test_queued_prompt_accepted_rejects_coalesced_text(tmp_path):
    # A doubled paste is a different prompt and cannot acknowledge this delivery.
    p = tmp_path / "session.jsonl"
    p.write_text(json.dumps({
        "type": "queue-operation",
        "operation": "enqueue",
        "timestamp": "2026-01-01T00:00:01Z",
        "content": "hellohello",
    }))

    assert not T.queued_prompt_accepted(p, "hello")
    assert not T.queued_prompt_accepted(p, "hello world")
    assert not T.queued_prompt_accepted(p, "goodbye")


def test_queued_prompt_accepted_honors_after_ms(tmp_path):
    p = tmp_path / "session.jsonl"
    p.write_text("\n".join([
        json.dumps({"type": "queue-operation", "operation": "enqueue",
                    "timestamp": "2026-01-01T00:00:01Z", "content": "hello"}),
        json.dumps({"type": "queue-operation", "operation": "enqueue",
                    "timestamp": "2026-01-01T00:00:03Z", "content": "hello"}),
    ]))

    assert T.queued_prompt_accepted(p, "hello", after_ms=1767225602000)
    assert not T.queued_prompt_accepted(p, "hello", after_ms=1767225604000)


# --- extract_background_tasks ------------------------------------------------

def _bg_launch_events(task_id="b2e5smis8", tool_use_id="toolu_01",
                      command="npm run dev", description="Dev server",
                      launch_ts="2026-01-01T00:00:01Z"):
    """A run_in_background Bash tool_use + its launching tool_result."""
    out_file = f"/tmp/claude-1000/proj/sess/tasks/{task_id}.output"
    return [
        {"type": "assistant", "timestamp": launch_ts, "message": {"content": [
            {"type": "tool_use", "id": tool_use_id, "name": "Bash",
             "input": {"command": command, "description": description,
                       "run_in_background": True}},
        ]}},
        {"type": "user", "timestamp": launch_ts,
         "toolUseResult": {"stdout": "", "stderr": "", "interrupted": False,
                           "backgroundTaskId": task_id},
         "message": {"content": [
            {"type": "tool_result", "tool_use_id": tool_use_id,
             "content": (f"Command running in background with ID: {task_id}. "
                         f"Output is being written to: {out_file}. "
                         "You will be notified when it completes."),
             "is_error": False},
        ]}},
    ]


def _bg_notification_event(task_id="b2e5smis8", tool_use_id="toolu_01",
                           status="completed", exit_code=0,
                           ts="2026-01-01T00:00:13Z"):
    content = (
        "<task-notification>\n"
        f"  <task-id>{task_id}</task-id>\n"
        f"  <tool-use-id>{tool_use_id}</tool-use-id>\n"
        f"  <output-file>/tmp/claude-1000/proj/sess/tasks/{task_id}.output</output-file>\n"
        f"  <status>{status}</status>\n"
        f"  <summary>Background command \"Dev server\" completed (exit code {exit_code})</summary>\n"
        "</task-notification>"
    )
    return {"type": "queue-operation", "operation": "enqueue",
            "timestamp": ts, "content": content}


def _auto_bg_launch_events(task_id="bauto123", tool_use_id="toolu_auto",
                           command="make slow-test", description="Slow test",
                           structured=True,
                           launch_ts="2026-01-01T00:00:01Z"):
    """A foreground Bash result that Claude moved to the background."""
    out_file = f"/tmp/claude-1000/proj/sess/tasks/{task_id}.output"
    result = {
        "type": "user",
        "timestamp": launch_ts,
        "message": {"content": [
            {"type": "tool_result", "tool_use_id": tool_use_id,
             "content": ("Command did not complete within its 120s timeout and "
                         f"was moved to the background (ID: {task_id}). Output "
                         f"is being written to: {out_file}."),
             "is_error": False},
        ]},
    }
    if structured:
        result["toolUseResult"] = {
            "stdout": "", "stderr": "", "interrupted": False,
            "backgroundTaskId": task_id, "timedOutAfterMs": 120000,
        }
    return [
        {"type": "assistant", "timestamp": launch_ts, "message": {"content": [
            {"type": "tool_use", "id": tool_use_id, "name": "Bash",
             "input": {"command": command, "description": description}},
        ]}},
        result,
    ]


def _task_stop_events(task_id="b2e5smis8", tool_use_id="toolu_stop",
                      result_task_id=None, success=True, is_error=False,
                      command="npm run dev", ts="2026-01-01T00:00:20Z"):
    result_task_id = task_id if result_task_id is None else result_task_id
    message = (
        f"Successfully stopped task: {result_task_id} ({command})"
        if success else f"Task not found: {result_task_id}"
    )
    return [
        {"type": "assistant", "timestamp": ts, "message": {"content": [
            {"type": "tool_use", "id": tool_use_id, "name": "TaskStop",
             "input": {"task_id": task_id}},
        ]}},
        {"type": "user", "timestamp": ts,
         "toolUseResult": {
             "message": message,
             "task_id": result_task_id,
             "task_type": "local_bash",
             "command": command,
         },
         "message": {"content": [
             {"type": "tool_result", "tool_use_id": tool_use_id,
              "content": json.dumps({
                  "message": message,
                  "task_id": result_task_id,
                  "task_type": "local_bash",
                  "command": command,
              }),
              "is_error": is_error},
         ]}},
    ]


def _monitor_launch_events(task_id="bmon123", tool_use_id="toolu_mon",
                           command="while true; do date; sleep 1800; done",
                           description="Clock wake", persistent=True,
                           timeout_ms=3600000,
                           launch_ts="2026-01-01T00:00:01Z"):
    return [
        {"type": "assistant", "timestamp": launch_ts, "message": {"content": [
            {"type": "tool_use", "id": tool_use_id, "name": "Monitor",
             "input": {"command": command, "description": description,
                       "persistent": persistent, "timeout_ms": timeout_ms}},
        ]}},
        {"type": "user", "timestamp": launch_ts,
         "toolUseResult": {"stdout": "", "stderr": "", "interrupted": False,
                           "taskId": task_id, "persistent": persistent,
                           "timeoutMs": timeout_ms},
         "message": {"content": [
            {"type": "tool_result", "tool_use_id": tool_use_id,
             "content": f"Monitor started (task {task_id}).",
             "is_error": False},
        ]}},
    ]


def _monitor_event_notification(task_id="bmon123", event="PROBE_TICK",
                                ts="2026-01-01T00:00:13Z"):
    content = (
        "<task-notification>\n"
        f"  <task-id>{task_id}</task-id>\n"
        f"  <event>{event}</event>\n"
        "</task-notification>"
    )
    return {"type": "queue-operation", "operation": "enqueue",
            "timestamp": ts, "content": content}


def _monitor_terminal_notification(task_id="bmon123", tool_use_id="toolu_mon",
                                   status="stopped", summary="Monitor stopped.",
                                   ts="2026-01-01T00:00:20Z"):
    content = (
        "<task-notification>\n"
        f"  <task-id>{task_id}</task-id>\n"
        f"  <tool-use-id>{tool_use_id}</tool-use-id>\n"
        f"  <status>{status}</status>\n"
        f"  <summary>{summary}</summary>\n"
        "</task-notification>"
    )
    return {"type": "queue-operation", "operation": "enqueue",
            "timestamp": ts, "content": content}


def _write_bg_jsonl(path, events):
    path.write_text("\n".join(json.dumps(e) for e in events))


def _real_background_events(name):
    # Sanitized native rows: source paths/line numbers are in the fixture README.
    path = Path(__file__).parent / "fixtures" / "background_tasks" / name
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_background_interrupted_launch_without_id(tmp_path):
    events = _real_background_events("interrupted.jsonl")
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, events)
    [task] = T.extract_background_tasks(p)
    assert (task["task_id"], task["status"], task["completed_at"]) == (
        None, "stopped", 1792516990509)
    assert task["summary"] == events[1]["message"]["content"][0]["content"]


@pytest.mark.parametrize("persistent", [False, True])
def test_background_monitor_expiry_notice(tmp_path, persistent):
    events = _real_background_events("monitors.jsonl")
    for event in events:
        if "toolUseResult" in event:
            event["toolUseResult"]["persistent"] = persistent
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, events)
    tasks = T.extract_background_tasks(p)
    assert [(t["task_id"], t["status"], t["completed_at"]) for t in tasks] == [
        ("bhkgwpste", "stopped", 1792714699332),
        ("bwl2pjwzo", "stopped", 1792714704203),
    ]
    for task in tasks:
        assert task["persistent"] is persistent
        assert task["event_count"] == 1
        assert task["last_event"].startswith("[Monitor expired")
        assert task["last_event_at"] == task["completed_at"]


def test_background_monitor_older_timeout_notice(tmp_path):
    events = _monitor_launch_events(task_id="bz0edk7ap")
    events += _real_background_events("timed_out.jsonl")
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, events)
    [task] = T.extract_background_tasks(p)
    assert task["status"] == "stopped"
    assert task["completed_at"] == T._parse_event_ts_ms(events[-1])
    assert task["event_count"] == 1
    assert task["last_event"] == "[Monitor timed out — re-arm if needed.]"
    assert task["last_event_at"] == task["completed_at"]


@pytest.mark.parametrize("name", ["Bash", "Monitor"])
def test_background_launch_error_without_id(tmp_path, name):
    events = _real_background_events("interrupted.jsonl")
    events[0]["message"]["content"][0]["name"] = name
    events[1].pop("toolUseResult")
    body = "Launch failed: " + "x" * 1500
    events[1]["message"]["content"][0]["content"] = body
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, events)
    [task] = T.extract_background_tasks(p)
    assert (task["status"], task["completed_at"]) == ("stopped", 1792516990509)
    assert task["summary"] == body[:1000] + "..."


@pytest.mark.parametrize("name", ["Bash", "Monitor"])
def test_background_launch_without_result_stays_pending(tmp_path, name):
    events = _real_background_events("interrupted.jsonl")[:1]
    events[0]["message"]["content"][0]["name"] = name
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, events)
    [task] = T.extract_background_tasks(p)
    assert (task["task_id"], task["status"], task["completed_at"]) == (
        None, "running", None)


def test_background_monitor_normal_event_stays_running(tmp_path):
    events = _real_background_events("monitors.jsonl")[:2]
    events.append(_monitor_event_notification(
        task_id="bhkgwpste", event="build finished; old watch expired"))
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, events)
    [task] = T.extract_background_tasks(p)
    assert (task["status"], task["completed_at"]) == ("running", None)
    assert task["event_count"] == 1
    assert task["last_event"] == "build finished; old watch expired"


def test_extract_background_tasks_running_when_no_notification(tmp_path):
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, _bg_launch_events())
    tasks = T.extract_background_tasks(p)
    assert len(tasks) == 1
    t = tasks[0]
    assert t["task_id"] == "b2e5smis8"
    assert t["tool_use_id"] == "toolu_01"
    assert t["command"] == "npm run dev"
    assert t["description"] == "Dev server"
    assert t["status"] == "running"
    assert t["exit_code"] is None
    assert t["completed_at"] is None
    assert t["launched_at"] == 1767225601000
    assert t["output_file"].endswith("b2e5smis8.output")
    assert t["kind"] == "bash"


def test_extract_background_tasks_pairs_completion(tmp_path):
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, [*_bg_launch_events(), _bg_notification_event()])
    tasks = T.extract_background_tasks(p)
    assert len(tasks) == 1
    t = tasks[0]
    assert t["status"] == "completed"
    assert t["exit_code"] == 0
    assert t["completed_at"] == 1767225613000
    assert "completed" in (t["summary"] or "")


def test_extract_background_tasks_pairs_successful_task_stop(tmp_path):
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, [*_bg_launch_events(), *_task_stop_events()])
    t = T.extract_background_tasks(p)[0]
    assert t["status"] == "stopped"
    assert t["completed_at"] == 1767225620000
    assert t["exit_code"] is None
    assert t["summary"].startswith("Successfully stopped task: b2e5smis8")


def test_extract_background_tasks_auto_background_preserves_metadata(tmp_path):
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, _auto_bg_launch_events())
    t = T.extract_background_tasks(p)[0]
    assert t["task_id"] == "bauto123"
    assert t["status"] == "running"
    assert t["command"] == "make slow-test"
    assert t["description"] == "Slow test"
    assert t["launched_at"] == 1767225601000
    assert t["output_file"].endswith("bauto123.output")


def test_extract_background_tasks_auto_background_text_only_fallback(tmp_path):
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, _auto_bg_launch_events(structured=False))
    t = T.extract_background_tasks(p)[0]
    assert t["task_id"] == "bauto123"
    assert t["command"] == "make slow-test"
    assert t["status"] == "running"


def test_extract_background_tasks_auto_background_then_stopped(tmp_path):
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, [
        *_auto_bg_launch_events(),
        *_task_stop_events(task_id="bauto123", command="fallback command"),
    ])
    t = T.extract_background_tasks(p)[0]
    assert t["status"] == "stopped"
    assert t["command"] == "make slow-test"
    assert t["description"] == "Slow test"


@pytest.mark.parametrize(
    ("result_task_id", "success", "is_error"),
    [
        ("different", True, False),
        ("b2e5smis8", False, False),
        ("b2e5smis8", True, True),
    ],
)
def test_extract_background_tasks_ignores_untrusted_task_stop(
    tmp_path, result_task_id, success, is_error,
):
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, [
        *_bg_launch_events(),
        *_task_stop_events(result_task_id=result_task_id, success=success,
                           is_error=is_error),
    ])
    assert T.extract_background_tasks(p)[0]["status"] == "running"


def test_extract_background_tasks_ignores_task_stop_without_structured_result(tmp_path):
    p = tmp_path / "s.jsonl"
    stop_events = _task_stop_events()
    stop_events[1].pop("toolUseResult")
    _write_bg_jsonl(p, [*_bg_launch_events(), *stop_events])
    assert T.extract_background_tasks(p)[0]["status"] == "running"


def test_extract_background_tasks_failed_via_status_and_exit_code(tmp_path):
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, [*_bg_launch_events(),
                     _bg_notification_event(status="failed", exit_code=2)])
    tasks = T.extract_background_tasks(p)
    assert tasks[0]["status"] == "failed"
    assert tasks[0]["exit_code"] == 2


def test_extract_background_tasks_nonzero_exit_implies_failed(tmp_path):
    # status tag absent/unknown but exit code nonzero → failed.
    p = tmp_path / "s.jsonl"
    notif = _bg_notification_event(status="done", exit_code=1)
    _write_bg_jsonl(p, [*_bg_launch_events(), notif])
    assert T.extract_background_tasks(p)[0]["status"] == "failed"


def test_extract_background_tasks_parses_id_from_content_without_structured_field(tmp_path):
    # No toolUseResult.backgroundTaskId — id must come from the result content.
    events = _bg_launch_events()
    events[1].pop("toolUseResult")
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, events)
    tasks = T.extract_background_tasks(p)
    assert len(tasks) == 1
    assert tasks[0]["task_id"] == "b2e5smis8"


def test_extract_background_tasks_ignores_foreground_bash(tmp_path):
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, [
        {"type": "assistant", "timestamp": "2026-01-01T00:00:01Z", "message": {"content": [
            {"type": "tool_use", "id": "toolu_fg", "name": "Bash",
             "input": {"command": "ls"}},
        ]}},
        {"type": "user", "timestamp": "2026-01-01T00:00:01Z", "message": {"content": [
            {"type": "tool_result", "tool_use_id": "toolu_fg", "content": "a\nb\n"},
        ]}},
    ])
    assert T.extract_background_tasks(p) == []


def test_extract_background_tasks_empty_for_missing_file(tmp_path):
    assert T.extract_background_tasks(tmp_path / "nope.jsonl") == []


def test_extract_background_tasks_multiple_sorted_by_launch(tmp_path):
    events = [
        *_bg_launch_events(task_id="aaa", tool_use_id="toolu_a",
                           launch_ts="2026-01-01T00:00:05Z"),
        *_bg_launch_events(task_id="bbb", tool_use_id="toolu_b",
                           launch_ts="2026-01-01T00:00:01Z"),
        _bg_notification_event(task_id="bbb", tool_use_id="toolu_b"),
    ]
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, events)
    tasks = T.extract_background_tasks(p)
    assert [t["task_id"] for t in tasks] == ["bbb", "aaa"]
    assert tasks[0]["status"] == "completed"  # bbb has a notification
    assert tasks[1]["status"] == "running"    # aaa does not


def test_extract_background_tasks_monitor_running_when_no_notification(tmp_path):
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, _monitor_launch_events())
    tasks = T.extract_background_tasks(p)
    assert len(tasks) == 1
    t = tasks[0]
    assert t["kind"] == "monitor"
    assert t["task_id"] == "bmon123"
    assert t["tool_use_id"] == "toolu_mon"
    assert t["status"] == "running"
    assert t["persistent"] is True
    assert t["timeout_ms"] == 3600000
    assert t["event_count"] == 0
    assert t["last_event"] is None


def test_extract_background_tasks_monitor_events_without_tool_use_id(tmp_path):
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, [
        *_monitor_launch_events(),
        _monitor_event_notification(event="tick one", ts="2026-01-01T00:00:13Z"),
        _monitor_event_notification(event="tick two", ts="2026-01-01T00:00:14Z"),
    ])
    t = T.extract_background_tasks(p)[0]
    assert t["status"] == "running"
    assert t["event_count"] == 2
    assert t["last_event"] == "tick two"
    assert t["last_event_at"] == 1767225614000
    rows = list(T._iter_transcript(p))
    assert t["last_open_offset"] == rows[-1]["_ccx_start_offset"]
    assert t["last_open_offset"] > rows[1]["_ccx_start_offset"]


def test_extract_background_tasks_monitor_terminal_stopped(tmp_path):
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, [
        *_monitor_launch_events(),
        _monitor_event_notification(event="tick one"),
        _monitor_terminal_notification(status="stopped",
                                       summary="No completion record found."),
    ])
    t = T.extract_background_tasks(p)[0]
    assert t["status"] == "stopped"
    assert t["summary"] == "No completion record found."
    assert t["event_count"] == 1
    assert t["completed_at"] == 1767225620000


def test_extract_background_tasks_monitor_task_stop_retains_events(tmp_path):
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, [
        *_monitor_launch_events(),
        _monitor_event_notification(event="tick one"),
        *_task_stop_events(task_id="bmon123", command="monitor command"),
    ])
    t = T.extract_background_tasks(p)[0]
    assert t["kind"] == "monitor"
    assert t["status"] == "stopped"
    assert t["event_count"] == 1
    assert t["last_event"] == "tick one"
    assert t["persistent"] is True
    assert t["timeout_ms"] == 3600000


def test_extract_background_tasks_later_terminal_evidence_wins(tmp_path):
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, [
        *_bg_launch_events(),
        _bg_notification_event(status="completed", exit_code=0,
                               ts="2026-01-01T00:00:13Z"),
        *_task_stop_events(ts="2026-01-01T00:00:20Z"),
    ])
    assert T.extract_background_tasks(p)[0]["status"] == "stopped"


def test_extract_background_tasks_completion_after_stop_wins(tmp_path):
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, [
        *_bg_launch_events(),
        *_task_stop_events(ts="2026-01-01T00:00:13Z"),
        _bg_notification_event(status="completed", exit_code=0,
                               ts="2026-01-01T00:00:20Z"),
    ])
    assert T.extract_background_tasks(p)[0]["status"] == "completed"


def test_extract_background_tasks_monitor_resume_old_stopped_new_running(tmp_path):
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, [
        *_monitor_launch_events(task_id="oldmon", tool_use_id="toolu_old",
                                launch_ts="2026-01-01T00:00:01Z"),
        _monitor_terminal_notification(task_id="oldmon", tool_use_id="toolu_old",
                                       status="stopped",
                                       summary="stopped during resume reconcile",
                                       ts="2026-01-01T00:00:30Z"),
        *_monitor_launch_events(task_id="newmon", tool_use_id="toolu_new",
                                launch_ts="2026-01-01T00:00:40Z"),
    ])
    tasks = T.extract_background_tasks(p)
    assert [(t["task_id"], t["status"]) for t in tasks] == [
        ("oldmon", "stopped"),
        ("newmon", "running"),
    ]


def test_extract_background_tasks_ignores_unpaired_monitor_like_result(tmp_path):
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, [
        {"type": "user", "timestamp": "2026-01-01T00:00:01Z",
         "toolUseResult": {"taskId": "stray"},
         "message": {"content": [
            {"type": "tool_result", "tool_use_id": "toolu_stray",
             "content": "ordinary foreground result"},
        ]}},
    ])
    assert T.extract_background_tasks(p) == []


# --- async Agent / Task background tasks -------------------------------------
# Shapes mirror real Claude Code 2.1.2xx transcripts (see the private increment
# claude-background-agent-accounting/live-probe.md); ids and paths sanitized.

_ABSENT = object()


def _agent_launch_events(agent_id="a424942a9e00ccf87", tool_use_id="toolu_agent",
                         flag=_ABSENT, name="Agent", structured=True,
                         tur_overrides=None, description="sleep probe",
                         ts="2026-01-01T00:00:01Z"):
    inp = {"description": description, "prompt": "do the thing",
           "subagent_type": "general-purpose"}
    if flag is not _ABSENT:
        inp["run_in_background"] = flag
    out_file = f"/tmp/claude-1000/proj/sess/tasks/{agent_id}.output"
    text = (
        "Async agent launched successfully. (This tool result is internal "
        "metadata.)\n"
        f"agentId: {agent_id} (internal ID - do not mention to user.)\n"
        "The agent is working in the background. You will be notified "
        "automatically when it completes.\n"
        f"output_file: {out_file}\n"
        "Do NOT Read or tail this file via the shell tool."
    )
    result = {"type": "user", "timestamp": ts, "message": {"content": [
        {"type": "tool_result", "tool_use_id": tool_use_id,
         "content": [{"type": "text", "text": text}]},
    ]}}
    if structured:
        tur = {"isAsync": True, "status": "async_launched", "agentId": agent_id,
               "description": description, "resolvedModel": "claude-haiku-4-5",
               "prompt": "do the thing", "outputFile": out_file,
               "canReadOutputFile": False}
        tur.update(tur_overrides or {})
        result["toolUseResult"] = {k: v for k, v in tur.items() if v is not _ABSENT}
    return [
        {"type": "assistant", "timestamp": ts, "message": {"content": [
            {"type": "tool_use", "id": tool_use_id, "name": name, "input": inp},
        ]}},
        result,
    ]


def _agent_notification(agent_id="a424942a9e00ccf87", tool_use_id="toolu_agent",
                        status="completed", result="PROBE_DONE",
                        ts="2026-01-01T00:00:13Z"):
    tool = f"<tool-use-id>{tool_use_id}</tool-use-id>\n" if tool_use_id else ""
    content = (
        "<task-notification>\n"
        f"<task-id>{agent_id}</task-id>\n"
        f"{tool}"
        f"<output-file>/tmp/claude-1000/proj/sess/tasks/{agent_id}.output</output-file>\n"
        f"<status>{status}</status>\n"
        "<summary>Agent \"sleep probe\" finished</summary>\n"
        "<note>A task-notification fires each time this agent stops with no "
        "live background children of its own.</note>\n"
        f"<result>{result}</result>\n"
        "</task-notification>"
    )
    return {"type": "queue-operation", "operation": "enqueue",
            "timestamp": ts, "content": content}


def _send_message_events(agent_id="a424942a9e00ccf87", tool_use_id="toolu_send",
                         resumed=True, ts="2026-01-01T00:00:20Z"):
    tur = (
        {"success": True, "message": f"Resuming agent {agent_id[:7]}",
         "resumedAgentId": agent_id,
         "pin": {"id": agent_id, "name": agent_id, "ref": "6c5bc2"}}
        if resumed else
        {"success": True,
         "message": f"Message queued for delivery to {agent_id} at its next tool round.",
         "pin": {"id": agent_id, "name": agent_id, "ref": "00232e"}}
    )
    return [
        {"type": "assistant", "timestamp": ts, "message": {"content": [
            {"type": "tool_use", "id": tool_use_id, "name": "SendMessage",
             "input": {"to": agent_id, "message": "continue"}},
        ]}},
        {"type": "user", "timestamp": ts, "toolUseResult": tur,
         "message": {"content": [
             {"type": "tool_result", "tool_use_id": tool_use_id,
              "content": json.dumps(tur)},
         ]}},
    ]


@pytest.mark.parametrize("flag", [True, "true", _ABSENT])
def test_extract_background_tasks_async_agent_any_launch_flag(tmp_path, flag):
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, _agent_launch_events(flag=flag))
    tasks = T.extract_background_tasks(p)
    assert len(tasks) == 1
    t = tasks[0]
    assert t["kind"] == "subagent"
    assert t["task_id"] == "a424942a9e00ccf87"
    assert t["tool_use_id"] == "toolu_agent"
    assert t["status"] == "running"
    assert t["description"] == "sleep probe"
    assert t["output_file"].endswith("a424942a9e00ccf87.output")
    assert t["launched_at"] == 1767225601000


@pytest.mark.parametrize("overrides", [
    {"status": _ABSENT},     # isAsync alone
    {"isAsync": _ABSENT},    # status alone
])
def test_extract_background_tasks_async_agent_structured_variants(tmp_path, overrides):
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, _agent_launch_events(tur_overrides=overrides))
    [t] = T.extract_background_tasks(p)
    assert (t["kind"], t["task_id"], t["status"]) == (
        "subagent", "a424942a9e00ccf87", "running")


def test_extract_background_tasks_async_task_tool_name(tmp_path):
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, _agent_launch_events(name="Task"))
    [t] = T.extract_background_tasks(p)
    assert (t["kind"], t["status"]) == ("subagent", "running")


def test_extract_background_tasks_async_agent_text_only_fallback(tmp_path):
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, _agent_launch_events(structured=False))
    [t] = T.extract_background_tasks(p)
    assert t["task_id"] == "a424942a9e00ccf87"
    assert t["output_file"] == "/tmp/claude-1000/proj/sess/tasks/a424942a9e00ccf87.output"
    assert t["description"] == "sleep probe"
    assert t["status"] == "running"


def test_extract_background_tasks_parallel_agent_results_use_text(tmp_path):
    """Two results in one event: the event-level toolUseResult is ignored and
    each result's own text identifies its agent."""
    a = _agent_launch_events(agent_id="aaaa", tool_use_id="toolu_a")
    b = _agent_launch_events(agent_id="bbbb", tool_use_id="toolu_b")
    merged = {"type": "user", "timestamp": "2026-01-01T00:00:01Z",
              "toolUseResult": a[1]["toolUseResult"],
              "message": {"content": a[1]["message"]["content"]
                          + b[1]["message"]["content"]}}
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, [a[0], b[0], merged])
    tasks = T.extract_background_tasks(p)
    assert sorted((t["tool_use_id"], t["task_id"]) for t in tasks) == [
        ("toolu_a", "aaaa"), ("toolu_b", "bbbb")]


def test_extract_background_tasks_sync_or_errored_agent_is_not_a_task(tmp_path):
    sync = _agent_launch_events(agent_id="async1", tool_use_id="toolu_sync", flag=False)
    sync[1]["toolUseResult"] = {"status": "completed", "isAsync": True,
                                "agentId": "async1", "content": []}
    sync[1]["message"]["content"][0]["content"] = "the agent's final answer"
    errored = _agent_launch_events(agent_id="async2", tool_use_id="toolu_err", flag=True)
    errored[1]["message"]["content"][0]["is_error"] = True
    del errored[1]["toolUseResult"]
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, [*sync, *errored])
    assert T.extract_background_tasks(p) == []


def test_extract_background_tasks_agent_completion_pairs_by_agent_id(tmp_path):
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, [*_agent_launch_events(), _agent_notification()])
    [t] = T.extract_background_tasks(p)
    assert t["status"] == "completed"
    assert t["completed_at"] == 1767225613000
    assert t["summary"] == 'Agent "sleep probe" finished'


def test_extract_background_tasks_agent_failed(tmp_path):
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, [*_agent_launch_events(), _agent_notification(status="failed")])
    assert T.extract_background_tasks(p)[0]["status"] == "failed"


def test_extract_background_tasks_agent_task_stop_killed(tmp_path):
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, [
        *_agent_launch_events(),
        _agent_notification(status="killed", ts="2026-01-01T00:00:20Z"),
        *_task_stop_events(task_id="a424942a9e00ccf87", command="sleep probe",
                           ts="2026-01-01T00:00:20Z"),
    ])
    [t] = T.extract_background_tasks(p)
    assert t["status"] == "stopped"
    assert t["summary"].startswith("Successfully stopped task: a424942a9e00ccf87")


def test_extract_background_tasks_bash_killed_is_stopped(tmp_path):
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, [*_bg_launch_events(),
                        _bg_notification_event(status="killed", exit_code=137)])
    t = T.extract_background_tasks(p)[0]
    assert t["status"] == "stopped"
    assert t["exit_code"] == 137


def test_extract_background_tasks_multi_id_sweep_settles_every_task(tmp_path):
    sweep = (
        "<task-notification>\n"
        "<task-id>aaaa</task-id>\n<task-id>bbbb</task-id>\n<task-id>b2e5smis8</task-id>\n"
        "<status>stopped</status>\n"
        "<summary>No completion record was found for 3 background agents from "
        "the previous session.</summary>\n"
        "</task-notification>"
    )
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, [
        *_agent_launch_events(agent_id="aaaa", tool_use_id="toolu_a"),
        *_agent_launch_events(agent_id="bbbb", tool_use_id="toolu_b"),
        *_bg_launch_events(),
        {"type": "queue-operation", "operation": "enqueue",
         "timestamp": "2026-01-01T00:01:00Z", "content": sweep},
    ])
    tasks = T.extract_background_tasks(p)
    assert {t["task_id"]: t["status"] for t in tasks} == {
        "aaaa": "stopped", "bbbb": "stopped", "b2e5smis8": "stopped"}


def test_extract_background_tasks_quoted_task_id_in_result_is_ignored(tmp_path):
    quoted = _agent_notification(
        agent_id="aaaa", tool_use_id="toolu_a",
        result="I saw <task-notification><task-id>bbbb</task-id>"
               "<status>completed</status></task-notification> in a log")
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, [
        *_agent_launch_events(agent_id="aaaa", tool_use_id="toolu_a"),
        *_agent_launch_events(agent_id="bbbb", tool_use_id="toolu_b"),
        quoted,
    ])
    tasks = T.extract_background_tasks(p)
    assert {t["task_id"]: t["status"] for t in tasks} == {
        "aaaa": "completed", "bbbb": "running"}


def test_extract_background_tasks_agent_resume_reopens_then_settles(tmp_path):
    p = tmp_path / "s.jsonl"
    events = [
        *_agent_launch_events(),
        _agent_notification(ts="2026-01-01T00:00:13Z"),
        *_send_message_events(ts="2026-01-01T00:00:20Z"),
    ]
    _write_bg_jsonl(p, events)
    [t] = T.extract_background_tasks(p)
    assert t["status"] == "running"
    assert t["completed_at"] is None and t["summary"] is None
    assert t["tool_use_id"] == "toolu_agent"
    rows = list(T._iter_transcript(p))
    assert t["last_open_offset"] == rows[-1]["_ccx_start_offset"]
    assert t["last_open_offset"] > rows[1]["_ccx_start_offset"]

    # The resumed run's notification carries the SendMessage tool-use id.
    events.append(_agent_notification(tool_use_id="toolu_send",
                                      ts="2026-01-01T00:00:40Z"))
    _write_bg_jsonl(p, events)
    [t] = T.extract_background_tasks(p)
    assert t["status"] == "completed"
    assert t["completed_at"] == 1767225640000


def test_extract_background_tasks_resume_ignores_superseded_invocation(tmp_path):
    p = tmp_path / "s.jsonl"
    events = [
        *_agent_launch_events(),
        _agent_notification(status="killed", ts="2026-01-01T00:00:13Z"),
        *_send_message_events(ts="2026-01-01T00:00:20Z"),
        # A late copy naming the original launch must not close resumed work.
        _agent_notification(tool_use_id="toolu_agent", ts="2026-01-01T00:00:30Z"),
    ]
    _write_bg_jsonl(p, events)
    assert T.extract_background_tasks(p)[0]["status"] == "running"
    # A sweep without a tool-use id (e.g. after a restart) still applies.
    events.append(_agent_notification(tool_use_id=None, status="stopped",
                                      ts="2026-01-01T00:00:50Z"))
    _write_bg_jsonl(p, events)
    assert T.extract_background_tasks(p)[0]["status"] == "stopped"


def test_extract_background_tasks_resume_of_unknown_agent_creates_task(tmp_path):
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, _send_message_events(agent_id="aprior", tool_use_id="toolu_send"))
    [t] = T.extract_background_tasks(p)
    assert (t["kind"], t["task_id"], t["tool_use_id"], t["status"]) == (
        "subagent", "aprior", "toolu_send", "running")
    assert t["last_open_offset"] == list(T._iter_transcript(p))[-1]["_ccx_start_offset"]


def test_extract_background_tasks_queued_send_message_does_not_reopen(tmp_path):
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, [
        *_agent_launch_events(),
        _agent_notification(ts="2026-01-01T00:00:13Z"),
        *_send_message_events(resumed=False, ts="2026-01-01T00:00:20Z"),
    ])
    assert T.extract_background_tasks(p)[0]["status"] == "completed"


def test_extract_background_tasks_fold_uses_transcript_order(tmp_path):
    """A terminal written after a resume wins even if its timestamp is older."""
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, [
        *_agent_launch_events(),
        _agent_notification(ts="2026-01-01T00:00:13Z"),
        *_send_message_events(ts="2026-01-01T00:00:30Z"),
        _agent_notification(tool_use_id="toolu_send", status="failed",
                            ts="2026-01-01T00:00:25Z"),
    ])
    assert T.extract_background_tasks(p)[0]["status"] == "failed"


def test_extract_background_tasks_task_id_wins_over_tool_use_id(tmp_path):
    """A record naming task A with task B's launch tool-use id settles only A."""
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, [
        *_agent_launch_events(agent_id="aaaa", tool_use_id="toolu_a"),
        *_agent_launch_events(agent_id="bbbb", tool_use_id="toolu_b"),
        _agent_notification(agent_id="aaaa", tool_use_id="toolu_b"),
    ])
    tasks = T.extract_background_tasks(p)
    assert {t["task_id"]: t["status"] for t in tasks} == {
        "aaaa": "completed", "bbbb": "running"}


def test_extract_background_tasks_ignores_unknown_child_notification(tmp_path):
    """A subagent's own background child notifies into the parent transcript
    under ids the parent never launched; it settles nothing here."""
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, [
        *_agent_launch_events(),
        _agent_notification(ts="2026-01-01T00:00:05Z"),
        _bg_notification_event(task_id="bchild", tool_use_id="toolu_in_subagent"),
    ])
    [t] = T.extract_background_tasks(p)
    assert (t["task_id"], t["status"]) == ("a424942a9e00ccf87", "completed")


def test_background_task_summary_counts():
    tasks = [
        {"status": "running"}, {"status": "completed"},
        {"status": "stopped"}, {"status": "running"},
    ]
    s = T.background_task_summary(tasks)
    assert s == {"background_task_count": 4, "running_background_task_count": 2}
    assert T.background_task_summary([]) == {
        "background_task_count": 0, "running_background_task_count": 0}


def test_background_task_native_boundary_and_open_offset_are_conservative(tmp_path):
    p = tmp_path / "native.jsonl"
    p.write_bytes(b"first\nsecond\n")
    size = p.stat().st_size
    assert T.native_transcript_start(0, p) == 0
    assert T.native_transcript_start(size, p) == size
    for invalid in (None, True, False, -1, 2.5, size + 1):
        assert T.native_transcript_start(invalid, p) is None
    assert T.native_transcript_start(2, tmp_path / "missing.jsonl") is None

    old = {"status": "running", "last_open_offset": 3}
    current = {"status": "running", "last_open_offset": size}
    assert T.background_task_current(old, size) is False
    assert T.background_task_current(current, size) is True
    assert T.background_task_current(old, None) is True
    for missing in (None, -1, True, False, "3"):
        assert T.background_task_current(
            {"status": "running", "last_open_offset": missing}, size,
        ) is True
    assert T.background_task_current({"status": "completed", "last_open_offset": size},
                                     size) is False


# --- compact_result_since: failure -------------------------------------------

def _local_command(content):
    return json.dumps({
        "type": "system", "subtype": "local_command",
        "timestamp": "2026-01-01T00:00:01Z", "uuid": "u1", "content": content,
    })


def _compact_transcript(tmp_path, stderr_text):
    p = tmp_path / "session.jsonl"
    p.write_text("\n".join([
        json.dumps({"type": "user", "timestamp": "2026-01-01T00:00:00Z",
                    "message": {"role": "user", "content": "/compact"}}),
        _local_command(f"<local-command-stderr>{stderr_text}</local-command-stderr>"),
    ]) + "\n")
    return p


def test_compact_result_since_detects_api_error_failure(tmp_path):
    """The real 529 shape: compaction is not retried by Claude Code, so this
    stderr record is the operation's terminal verdict."""
    p = _compact_transcript(
        tmp_path,
        "Error during compaction: API Error: 529 Overloaded. This is a "
        "server-side issue, usually temporary — try again in a moment.",
    )

    out = T.compact_result_since(p)

    assert out["ok"] is False
    assert out["compacted"] is False
    assert out["message"].startswith("Error during compaction: ")
    assert out["api_error"]["status"] == 529
    assert out["api_error"]["transient"] is True
    assert out["api_error"]["rate_limited"] is False


def test_compact_result_since_detects_non_api_failure(tmp_path):
    p = _compact_transcript(
        tmp_path,
        "Compaction failed · conversation could not be reduced below the "
        "context limit",
    )

    out = T.compact_result_since(p)

    assert out["ok"] is False
    assert out["message"].startswith("Compaction failed · ")
    # Nothing to attribute to the API — the conversation itself is the problem.
    assert out["api_error"] is None


def test_compact_result_since_strips_rethrow_error_prefix(tmp_path):
    p = _compact_transcript(tmp_path, "Error: Error during compaction: boom")

    assert T.compact_result_since(p)["message"] == "Error during compaction: boom"


def test_compact_result_since_api_error_without_status_keeps_attribution(tmp_path):
    """A stream that dies after a 200 carries no status. Dropping the API cause
    there would downgrade a retryable server fault to 'compaction is
    impossible'."""
    p = _compact_transcript(
        tmp_path,
        "Error during compaction: API Error: Connection lost mid-response. "
        "This is usually temporary — try again.",
    )

    api_error = T.compact_result_since(p)["api_error"]

    assert api_error is not None
    assert api_error["status"] is None
    assert api_error["transient"] is True


@pytest.mark.parametrize("stderr_text", [
    "some other command blew up",
    # The marker appears, but only in passing — `failed` is monotonic, so
    # accepting this would terminally fail a compact that may still be running.
    "diagnostic failed: Error during compaction appeared in the logs",
    "see also: Compaction failed · (historical note)",
])
def test_compact_result_since_ignores_unrelated_stderr(tmp_path, stderr_text):
    """Matching the stderr wrapper, or the marker anywhere in it, would let
    another command's failure pose as a compact verdict."""
    p = _compact_transcript(tmp_path, stderr_text)

    assert T.compact_result_since(p) is None


def test_compact_result_since_still_reports_success(tmp_path):
    p = tmp_path / "session.jsonl"
    p.write_text(json.dumps({
        "type": "system", "subtype": "compact_boundary", "uuid": "u2",
        "timestamp": "2026-01-01T00:00:01Z",
        "compactMetadata": {"preTokens": 100, "postTokens": 10,
                            "trigger": "manual"},
    }) + "\n")

    out = T.compact_result_since(p)

    assert out["ok"] is True and out["compacted"] is True
    assert out["api_error"] is None


def test_pending_compact_request_settled_by_stderr(tmp_path):
    """A failed compaction must not read as still-pending; that is what pinned
    a session BUSY until its deadline."""
    p = _compact_transcript(tmp_path, "Error during compaction: boom")

    assert T.pending_compact_request(p) is None


# --- api_error_from_event ----------------------------------------------------

def test_api_error_from_event_detects_terminal_error():
    evt = {
        "type": "assistant",
        "isApiErrorMessage": True,
        "apiErrorStatus": 500,
        "error": "server_error",
        "requestId": "req_123",
        "timestamp": "2026-01-01T00:00:01Z",
        "message": {"content": [
            {"type": "text", "text": "API Error: 500 Internal server error."},
        ]},
    }

    out = T.api_error_from_event(evt)

    assert out == {
        "status": 500,
        "error": "server_error",
        "request_id": "req_123",
        "text": "API Error: 500 Internal server error.",
        "ts": 1767225601000,
        "transient": True,
        "rate_limited": False,
        "reset_text": None,
    }


def test_api_error_from_event_ignores_normal_assistant():
    evt = {
        "type": "assistant",
        "message": {"content": [{"type": "text", "text": "normal reply"}]},
    }
    assert T.api_error_from_event(evt) is None


def _api_error_event(text, *, error, status=None, transient=None):
    evt = {
        "type": "assistant",
        "isApiErrorMessage": True,
        "error": error,
        "message": {"content": [{"type": "text", "text": text}]},
    }
    if status is not None:
        evt["apiErrorStatus"] = status
    if transient is not None:
        evt["apiErrorIsTransient"] = transient
    return evt


def test_api_error_from_event_529_is_transient_not_rate_limited():
    out = T.api_error_from_event(_api_error_event(
        "API Error: 529 Overloaded. This is a server-side issue, usually "
        "temporary — try again in a moment.",
        error="server_error", status=529,
    ))
    assert out["transient"] is True
    assert out["rate_limited"] is False
    assert out["reset_text"] is None


def test_api_error_from_event_rate_limit_captures_reset_text_verbatim():
    out = T.api_error_from_event(_api_error_event(
        "You've hit your session limit · resets 8:40pm (Europe/Madrid)",
        error="rate_limit", status=429,
    ))
    assert out["rate_limited"] is True
    # A quota wall, not a capacity throttle: retrying now is pointless.
    assert out["transient"] is False
    assert out["reset_text"] == "resets 8:40pm (Europe/Madrid)"


def test_api_error_from_event_throttled_429_is_both():
    """Claude Code marks a capacity-throttle 429 transient; a quota wall it does
    not. The two flags are independent, so both can be set at once."""
    out = T.api_error_from_event(_api_error_event(
        "API Error: Request rejected (429) · this may be a temporary "
        "capacity issue.",
        error="rate_limit", status=429, transient=True,
    ))
    assert out["transient"] is True
    assert out["rate_limited"] is True


def test_api_error_from_event_overloaded_kind_is_transient():
    out = T.api_error_from_event(_api_error_event(
        "API Error: Repeated 529 Overloaded errors.", error="overloaded",
    ))
    assert out["transient"] is True


def test_api_error_from_event_explicit_false_still_transient_by_kind():
    """apiErrorIsTransient is an override that can only ADD transience; the kind
    alone is sufficient, matching Claude Code's own `||` rule."""
    out = T.api_error_from_event(_api_error_event(
        "API Error: 500 Internal server error.",
        error="server_error", status=500, transient=False,
    ))
    assert out["transient"] is True


def test_api_error_from_event_permanent_kind_is_not_transient():
    out = T.api_error_from_event(_api_error_event(
        "Login expired · Please run /login", error="authentication_failed",
    ))
    assert out["transient"] is False
    assert out["rate_limited"] is False
    # No HTTP status is carried for failures that have none.
    assert out["status"] is None


def test_api_error_from_event_midstream_loss_has_no_status_but_is_transient():
    out = T.api_error_from_event(_api_error_event(
        "API Error: Connection lost mid-response. The response above may be "
        "incomplete.",
        error="server_error",
    ))
    assert out["status"] is None
    assert out["transient"] is True


# --- _process_command_message ------------------------------------------------

def test_process_command_caveat_dropped():
    assert T._process_command_message("<local-command-caveat>x</local-command-caveat>") is None


def test_process_command_stdout_unwrapped_and_empty_dropped():
    assert T._process_command_message("<local-command-stdout>hi</local-command-stdout>") == "hi"
    assert T._process_command_message("<local-command-stdout>   </local-command-stdout>") is None


def test_process_command_clear_dropped():
    assert T._process_command_message("<command-name>/clear</command-name>") is None


def test_process_command_name_with_args():
    out = T._process_command_message(
        "<command-name>/foo</command-name><command-args>bar baz</command-args>"
    )
    assert out == "/foo bar baz"


def test_process_command_name_without_args():
    assert T._process_command_message("<command-name>/foo</command-name>") == "/foo"


def test_process_command_plain_text_passthrough():
    assert T._process_command_message("just a normal prompt") == "just a normal prompt"


# --- _format_tool_call -------------------------------------------------------

def test_format_bash():
    assert T._format_tool_call("Bash", {"command": "ls -la"}) == ("Bash: ls -la", None)


def test_format_read_with_range():
    header, body = T._format_tool_call("Read", {"file_path": "/a", "offset": 100, "limit": 50})
    assert header == "Read: /a:100-150"
    assert body is None


def test_format_read_plain():
    assert T._format_tool_call("Read", {"file_path": "/a"}) == ("Read: /a", None)


def test_format_grep_path_and_glob():
    header, _ = T._format_tool_call("Grep", {"pattern": "x", "path": "/p", "glob": "*.py"})
    assert header == 'Grep: "x" in /p/*.py'


def test_format_edit_produces_diff_body():
    header, body = T._format_tool_call("Edit", {"file_path": "/a", "old_string": "foo", "new_string": "bar"})
    assert header == "Edit: /a"
    assert "- foo" in body and "+ bar" in body


def test_format_unknown_tool_dumps_json_body():
    header, body = T._format_tool_call("Mystery", {"k": "v"})
    assert header == "Mystery:"
    assert json.loads(body) == {"k": "v"}


def test_format_non_dict_input_is_safe():
    # Defensive: a non-dict input must not raise.
    assert T._format_tool_call("Bash", None) == ("Bash: ", None)


# --- _format_tool_result_content ---------------------------------------------

def test_tool_result_string_passthrough():
    assert T._format_tool_result_content("hello") == "hello"


def test_tool_result_none_sentinel():
    assert T._format_tool_result_content(None) == "<no content>"


def test_tool_result_text_block_list():
    content = [{"type": "text", "text": "a"}, {"type": "text", "text": "b"}]
    assert T._format_tool_result_content(content) == "ab"


# --- _usage_tokens / _cache_miss_reason --------------------------------------

def test_usage_tokens_basic_and_cache_fields():
    usage = {
        "input_tokens": 10,
        "output_tokens": 5,
        "cache_read_input_tokens": 3,
        "cache_creation_input_tokens": 2,
    }
    tok = T._usage_tokens({}, usage)
    assert tok["input"] == 10
    assert tok["output"] == 5
    assert tok["cache_read_input"] == 3
    assert tok["cache_creation_input"] == 2


def test_cache_miss_reason_from_server_diag():
    usage = {"server_tool_use": {"diagnostics": {"cache_miss_reason": "ttl"}}}
    assert T._cache_miss_reason({}, usage) == "ttl"


def test_cache_miss_reason_from_event_diag():
    evt = {"diagnostics": {"cache_miss_reason": "evicted"}}
    assert T._cache_miss_reason(evt, {}) == "evicted"


# --- _iter_clean_turns (integration over a JSONL fixture) --------------------

def _write_jsonl(tmp_path, events):
    p = tmp_path / "transcript.jsonl"
    p.write_text("\n".join(json.dumps(e) for e in events))
    return p


def test_iter_clean_turns_pairs_tools_and_results(tmp_path):
    events = [
        {"type": "user", "message": {"content": "Hello"}, "timestamp": "2026-01-01T00:00:00Z"},
        {"type": "assistant", "message": {
            "content": [
                {"type": "text", "text": "Hi"},
                {"type": "tool_use", "id": "t1", "name": "Bash", "input": {"command": "ls"}},
            ],
            "usage": {"input_tokens": 10, "output_tokens": 5},
            "model": "claude-x",
        }, "timestamp": "2026-01-01T00:00:01Z"},
        {"type": "user", "message": {"content": [
            {"type": "tool_result", "tool_use_id": "t1", "content": "file1\nfile2", "is_error": False},
        ]}},
        {"type": "assistant", "message": {
            "content": [{"type": "text", "text": "Done"}],
            "usage": {"input_tokens": 12, "output_tokens": 4},
        }},
    ]
    p = _write_jsonl(tmp_path, events)
    turns = list(T._iter_clean_turns(p))

    # user "Hello", assistant (Hi + Bash tool), assistant "Done". The pure
    # tool-result user echo is folded into the assistant turn, not its own turn.
    assert [t["role"] for t in turns] == ["user", "assistant", "assistant"]
    assert turns[0]["text"] == "Hello"

    a = turns[1]
    assert a["text"] == "Hi"
    assert len(a["tools"]) == 1
    tool = a["tools"][0]
    assert tool["name"] == "Bash"
    assert tool["summary"] == "Bash: ls"
    # tool input is preserved so dashboard can surface it (e.g. TodoWrite.todos)
    assert tool["input"] == {"command": "ls"}
    assert tool["result"]["content"] == "file1\nfile2"
    assert tool["result"]["is_error"] is False
    assert a["model"] == "claude-x"

    assert turns[2]["text"] == "Done"


def test_iter_clean_turns_keeps_same_message_tools_across_results(tmp_path):
    events = [
        {"type": "user", "message": {"content": "Do it"}},
        {"type": "assistant", "message": {
            "id": "msg_1",
            "content": [
                {"type": "text", "text": "I'll inspect both files."},
                {"type": "tool_use", "id": "t1", "name": "Read", "input": {"file_path": "/a"}},
            ],
        }},
        {"type": "user", "message": {"content": [
            {"type": "tool_result", "tool_use_id": "t1", "content": "A"},
        ]}},
        {"type": "assistant", "message": {
            "id": "msg_1",
            "content": [
                {"type": "tool_use", "id": "t2", "name": "Read", "input": {"file_path": "/b"}},
            ],
        }},
        {"type": "user", "message": {"content": [
            {"type": "tool_result", "tool_use_id": "t2", "content": "B"},
        ]}},
    ]
    p = _write_jsonl(tmp_path, events)
    turns = list(T._iter_clean_turns(p))

    assert [t["role"] for t in turns] == ["user", "assistant"]
    assistant = turns[1]
    assert assistant["text"] == "I'll inspect both files."
    assert [tool["name"] for tool in assistant["tools"]] == ["Read", "Read"]
    assert [tool["result"]["content"] for tool in assistant["tools"]] == ["A", "B"]


def test_iter_clean_turns_surfaces_compact_boundary(tmp_path):
    events = [
        {"type": "user", "message": {"content": "before compaction"}},
        {"type": "assistant", "message": {"content": [{"type": "text", "text": "ack"}]}},
        {"type": "system", "subtype": "compact_boundary",
         "content": "summary of the conversation so far",
         "compactMetadata": {"trigger": "manual", "preTokens": 12000,
                             "postTokens": 3000, "durationMs": 450},
         "timestamp": "2026-01-01T00:00:05Z"},
        {"type": "assistant", "message": {"content": [{"type": "text", "text": "after compaction"}]}},
    ]
    p = _write_jsonl(tmp_path, events)
    turns = list(T._iter_clean_turns(p))

    assert [t["role"] for t in turns] == ["user", "assistant", "system", "assistant"]
    sys_turns = [t for t in turns if t["role"] == "system" and t.get("kind") == "compact_boundary"]
    assert len(sys_turns) == 1
    s = sys_turns[0]
    assert s["ts"] is not None
    assert s["data"] == {
        "summary": "summary of the conversation so far",
        "pre_tokens": 12000,
        "post_tokens": 3000,
        "duration_ms": 450,
        "trigger": "manual",
    }
    # "text"/"tools" are not part of a system turn.
    assert "text" not in s and "tools" not in s


def test_iter_clean_turns_compact_boundary_auto_trigger(tmp_path):
    events = [
        {"type": "system", "subtype": "compact_boundary",
         "content": "auto summary",
         "compactMetadata": {"trigger": "auto"}},
    ]
    p = _write_jsonl(tmp_path, events)
    turns = list(T._iter_clean_turns(p))

    assert len(turns) == 1
    s = turns[0]
    assert s["role"] == "system" and s["kind"] == "compact_boundary"
    # Missing metadata fields degrade to None, not KeyError.
    assert s["data"] == {
        "summary": "auto summary",
        "pre_tokens": None,
        "post_tokens": None,
        "duration_ms": None,
        "trigger": "auto",
    }


def test_iter_clean_turns_renders_queued_command_attachment(tmp_path):
    # A prompt queued onto a busy session is delivered as a `queued_command`
    # attachment, never as a `user` event — it must still render as a user
    # turn (flagged `queued`) or steering messages vanish from the transcript.
    events = [
        {"type": "user", "message": {"content": "start"},
         "timestamp": "2026-01-01T00:00:00Z"},
        {"type": "assistant", "message": {
            "id": "m1", "content": [{"type": "text", "text": "working"}]},
         "timestamp": "2026-01-01T00:00:01Z"},
        {"type": "queue-operation", "operation": "enqueue", "content": "steer left",
         "timestamp": "2026-01-01T00:00:02Z"},
        {"type": "queue-operation", "operation": "remove", "content": "steer left",
         "timestamp": "2026-01-01T00:00:03Z"},
        {"type": "attachment", "attachment": {"type": "queued_command",
                                              "prompt": "steer left"},
         "timestamp": "2026-01-01T00:00:02Z"},
        {"type": "assistant", "message": {
            "id": "m2", "content": [{"type": "text", "text": "steered"}]},
         "timestamp": "2026-01-01T00:00:04Z"},
    ]
    p = _write_jsonl(tmp_path, events)
    turns = list(T._iter_clean_turns(p))
    assert [t["role"] for t in turns] == ["user", "assistant", "user", "assistant"]
    q = turns[2]
    assert q["text"] == "steer left"
    assert q["queued"] is True
    # `ts` is the delivery moment (clamped to the latest preceding event — the
    # attachment's own stamp is the enqueue time and would run backwards);
    # `queued_at` preserves when it was queued.
    assert q["ts"] == 1767225603000
    assert q["queued_at"] == 1767225602000
    # Ordinary prompts carry no queued flag.
    assert "queued" not in turns[0]


def test_iter_clean_turns_ignores_non_queued_attachments(tmp_path):
    events = [
        {"type": "user", "message": {"content": "hi"}},
        {"type": "attachment", "attachment": {"type": "file", "path": "/x"}},
        {"type": "attachment", "attachment": {"type": "queued_command", "prompt": "  "}},
        {"type": "assistant", "message": {"content": [{"type": "text", "text": "ok"}]}},
    ]
    p = _write_jsonl(tmp_path, events)
    turns = list(T._iter_clean_turns(p))
    assert [t["role"] for t in turns] == ["user", "assistant"]



def test_iter_clean_turns_drops_unknown_system_subtype(tmp_path):
    events = [
        {"type": "user", "message": {"content": "hi"}},
        {"type": "system", "subtype": "local_command", "content": "/compact"},
        {"type": "assistant", "message": {"content": [{"type": "text", "text": "ok"}]}},
    ]
    p = _write_jsonl(tmp_path, events)
    turns = list(T._iter_clean_turns(p))
    assert [t["role"] for t in turns] == ["user", "assistant"]




def test_iter_clean_turns_truncates_large_tool_result(tmp_path):
    big = "x" * (T._TOOL_RESULT_TRUNC_JSON + 500)
    events = [
        {"type": "assistant", "message": {"content": [
            {"type": "tool_use", "id": "t1", "name": "Bash", "input": {"command": "cat big"}},
        ]}},
        {"type": "user", "message": {"content": [
            {"type": "tool_result", "tool_use_id": "t1", "content": big},
        ]}},
    ]
    p = _write_jsonl(tmp_path, events)
    turns = list(T._iter_clean_turns(p))
    result = turns[0]["tools"][0]["result"]
    assert result["truncated"] is True
    assert len(result["content"]) == T._TOOL_RESULT_TRUNC_JSON


def test_iter_clean_turns_preserves_todowrite_input(tmp_path):
    # TodoWrite (and other tools) must carry their full input dict so the
    # dashboard can surface input.todos without re-parsing the raw JSONL.
    todos = [{"id": "1", "content": "Fix bug", "status": "pending", "priority": 1}]
    events = [
        {"type": "assistant", "message": {"content": [
            {"type": "tool_use", "id": "tw1", "name": "TodoWrite",
             "input": {"todos": todos}},
        ]}},
        {"type": "user", "message": {"content": [
            {"type": "tool_result", "tool_use_id": "tw1",
             "content": "Todos have been modified", "is_error": False},
        ]}},
    ]
    p = _write_jsonl(tmp_path, events)
    turns = list(T._iter_clean_turns(p))
    assert len(turns) == 1
    tool = turns[0]["tools"][0]
    assert tool["name"] == "TodoWrite"
    assert tool["input"] == {"todos": todos}
    assert tool["input"]["todos"][0]["content"] == "Fix bug"


def test_iter_transcript_skips_malformed_lines(tmp_path):
    p = tmp_path / "t.jsonl"
    p.write_text('{"type":"user","message":{"content":"ok"}}\nnot json\n\n')
    events = list(T._iter_transcript(p))
    assert len(events) == 1
    assert events[0]["type"] == "user"


def test_iter_transcript_missing_file_is_empty():
    assert list(T._iter_transcript("/no/such/file.jsonl")) == []


def test_iter_transcript_counts_malformed_lines_into_stats(tmp_path):
    p = tmp_path / "t.jsonl"
    p.write_text(
        '{"type":"user","message":{"content":"ok"}}\n'
        "not json\n"
        "{also bad\n"
        "\n"  # blank lines are skipped silently, not counted
    )
    stats = {}
    events = list(T._iter_transcript(p, stats))
    assert len(events) == 1
    assert stats["malformedLines"] == 2


def test_iter_clean_turns_counts_lossy_cleaning_into_stats(tmp_path):
    big = "x" * (T._TOOL_RESULT_TRUNC_JSON + 10)
    p = tmp_path / "transcript.jsonl"
    p.write_text(
        json.dumps({"type": "user", "message": {"content": "hi"}}) + "\n"
        "garbage-not-json\n"
        + json.dumps({"type": "assistant", "message": {"content": [
            {"type": "tool_use", "id": "t1", "name": "Bash", "input": {"command": "cat big"}},
        ]}}) + "\n"
        + json.dumps({"type": "user", "message": {"content": [
            {"type": "tool_result", "tool_use_id": "t1", "content": big},
        ]}}) + "\n"
    )
    stats = {}
    list(T._iter_clean_turns(p, stats))
    assert stats["malformedLines"] == 1
    assert stats["truncatedToolResults"] == 1


# --- exact_assistant_text ------------------

def test_exact_assistant_text_never_reads_past_byte_window(tmp_path):
    p = tmp_path / "conversation.jsonl"
    first_user = {"type": "user", "timestamp": "2026-09-25T10:00:00Z",
                  "message": {"content": "first"}}
    first_assistant = {
        "type": "assistant", "timestamp": "2026-09-25T10:00:01Z",
        "message": {"id": "m1", "content": [{"type": "text", "text": "FIRST"}]},
    }
    second_user = {"type": "user", "timestamp": "2026-09-25T10:00:02Z",
                   "message": {"content": "second"}}
    second_assistant = {
        "type": "assistant", "timestamp": "2026-09-25T10:00:03Z",
        "message": {"id": "m2", "content": [{"type": "text", "text": "SECOND"}]},
    }
    lines = [
        json.dumps(first_user) + "\n",
        json.dumps(first_assistant) + "\n",
        json.dumps(second_user) + "\n",
        json.dumps(second_assistant) + "\n",
    ]
    p.write_text("".join(lines))
    second_start = len("".join(lines[:2]).encode())

    assert T.exact_assistant_text(
        p, after_offset=0, before_offset=second_start
    ) == "FIRST"
    assert T.exact_assistant_text(
        p, after_offset=second_start
    ) == "SECOND"


# --- pending_ask_user_question -----------------------------------------------

def test_pending_question_detected(tmp_path):
    questions = [{"question": "Pick one", "options": [{"label": "A"}, {"label": "B"}]}]
    events = [
        {"type": "assistant", "message": {"content": [
            {"type": "tool_use", "id": "q1", "name": "AskUserQuestion", "input": {"questions": questions}},
        ]}},
    ]
    p = _write_jsonl(tmp_path, events)
    pend = T.pending_ask_user_question(p)
    assert pend is not None
    assert pend["toolUseId"] == "q1"
    assert pend["questions"] == questions


def test_pending_question_answered_returns_none(tmp_path):
    events = [
        {"type": "assistant", "message": {"content": [
            {"type": "tool_use", "id": "q1", "name": "AskUserQuestion", "input": {"questions": []}},
        ]}},
        {"type": "user", "message": {"content": [
            {"type": "tool_result", "tool_use_id": "q1", "content": "answered"},
        ]}},
    ]
    p = _write_jsonl(tmp_path, events)
    assert T.pending_ask_user_question(p) is None


# --- render_transcript / refresh_context_observation --------------------------


def _render_owned_source(tmp_path, monkeypatch, source, *, worker_id=None):
    """An original V2 owner/native claim and a native JSONL at its true path.

    The old short-store test fixture couldn't exercise the renderer's source
    and owner security boundary. This is an explicit disposable ACTIVE root;
    the provider is a local test JSONL, never a Claude process.
    """
    from test_generation_store import A, fixture_root, privdir
    from ccx import constants
    from ccx.store import paths
    from ccx.ops.creation_steps import prepare_worker
    from ccx.store.root import require_ccx_active

    owner_id = worker_id or A
    root = fixture_root(tmp_path)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    monkeypatch.setattr(paths, "CLAUDE_HOME", tmp_path / "native-claude-home")
    store = require_ccx_active(root)
    prepared = prepare_worker(
        store, engine="claude", cwd=str(tmp_path), worker_id=owner_id,
        name="demo", model="opus",
    )
    rec = store.record(prepared.worker)
    expected = paths.transcript_path_for(rec["cwd"], rec["native"]["sessionId"])
    # Intermediate native-profile dirs must be private even under umask 0002.
    privdir(paths.CLAUDE_HOME)
    privdir(paths.CLAUDE_HOME / "projects")
    privdir(expected.parent)
    # A real provider appends newline-terminated JSONL records. These tests
    # exercise completed turns, not the separate partial-final-line case.
    native_jsonl = source.read_bytes()
    expected.write_bytes(native_jsonl + (b"\n" if native_jsonl and not native_jsonl.endswith(b"\n") else b""))
    expected.chmod(0o600)
    return owner_id, store.worker_path(prepared.worker, "context-observation.json"), rec



def test_render_transcript_shape_and_writes_nothing(tmp_path, monkeypatch):
    """render returns the cleaned document and publishes no file."""
    src = _write_jsonl(tmp_path, [
        {"type": "user", "message": {"content": "hello"}},
        {"type": "assistant", "message": {"content": [{"type": "text", "text": "hi"}]}},
    ])
    owner_id, dst, rec = _render_owned_source(tmp_path, monkeypatch, src)
    out = T.render_transcript(owner_id)

    # Contract metadata: a small integer schema version and a render timestamp.
    assert out["schemaVersion"] == T.TRANSCRIPT_SCHEMA_VERSION
    assert isinstance(out["schemaVersion"], int)
    assert isinstance(out["generatedAt"], int)
    assert out["generatedAt"] > 0
    assert out["worker"] == f"claude:{owner_id}"
    assert out["sessionId"] == rec["native"]["sessionId"]
    assert out["name"] == "demo"
    assert out["cwd"] == rec["cwd"]
    assert out["transcriptJsonl"] == rec["transcript"]
    assert [t["role"] for t in out["turns"]] == ["user", "assistant"]
    # Lossy-cleaning stats are always present and zeroed for a clean transcript.
    assert out["stats"] == {"malformedLines": 0, "truncatedToolResults": 0}
    assert not dst.exists() and not list(dst.parent.glob("*.json.new-*"))


def test_render_transcript_embeds_context_usage(tmp_path, monkeypatch):
    """The rendered transcript carries the latest input footprint."""
    src = _write_jsonl(tmp_path, [
        {"type": "user", "message": {"content": "hello"}},
        {"type": "assistant", "message": {
            "model": "claude-opus-4-8[1m]",
            "content": [{"type": "text", "text": "hi"}],
            "usage": {"input_tokens": 300000, "cache_read_input_tokens": 50000,
                      "output_tokens": 1000},
        }},
    ])
    owner_id, dst, rec = _render_owned_source(tmp_path, monkeypatch, src)
    ctx = T.render_transcript(owner_id)["context"]
    assert ctx == {
        "state": "known", "tokens_used": 350000,
        "as_of_turn_ref": None, "as_of_ms": None,
    }



def test_render_transcript_fences_usage_after_compact(tmp_path, monkeypatch):
    src = _write_jsonl(tmp_path, [
        {"type": "assistant", "message": {
            "content": [{"type": "text", "text": "before"}],
            "usage": {"input_tokens": 350000, "output_tokens": 1000},
        }},
        {"type": "system", "subtype": "compact_boundary",
         "content": "Conversation compacted",
         "compactMetadata": {
             "trigger": "manual", "preTokens": 351000, "postTokens": 16000,
         },
         "timestamp": "2026-06-19T12:00:01Z"},
    ])
    owner_id, dst, rec = _render_owned_source(tmp_path, monkeypatch, src)
    assert T.render_transcript(owner_id)["context"] == {
        "state": "unknown",
        "tokens_used": None,
        "reason": "just_compacted",
        "as_of_turn_ref": None,
        "as_of_ms": 1781870401000,
    }


def test_render_transcript_fences_usage_while_compacting(tmp_path, monkeypatch):
    src = _write_jsonl(tmp_path, [
        {"type": "assistant", "message": {
            "content": [{"type": "text", "text": "before"}],
            "usage": {"input_tokens": 350000, "output_tokens": 1000},
        }},
        {"type": "user", "message": {
            "content": "<command-name>/compact</command-name>\n"
                       "<command-message>compact</command-message>",
        }, "timestamp": "2026-06-19T12:00:01Z"},
    ])
    owner_id, dst, rec = _render_owned_source(tmp_path, monkeypatch, src)
    assert T.render_transcript(owner_id)["context"] == {
        "state": "unknown",
        "tokens_used": None,
        "reason": "compacting",
        "as_of_turn_ref": None,
        "as_of_ms": 1781870401000,
    }


def test_render_transcript_surfaces_lossy_stats(tmp_path, monkeypatch):
    big = "y" * (T._TOOL_RESULT_TRUNC_JSON + 1)
    src = tmp_path / "transcript.jsonl"
    src.write_text(
        json.dumps({"type": "user", "message": {"content": "hi"}}) + "\n"
        "}} not json {{\n"
        + json.dumps({"type": "assistant", "message": {"content": [
            {"type": "tool_use", "id": "t1", "name": "Bash", "input": {"command": "cat"}},
        ]}}) + "\n"
        + json.dumps({"type": "user", "message": {"content": [
            {"type": "tool_result", "tool_use_id": "t1", "content": big},
        ]}}) + "\n"
    )
    owner_id, dst, rec = _render_owned_source(tmp_path, monkeypatch, src)
    out = T.render_transcript(owner_id)
    assert out["stats"]["malformedLines"] == 1
    assert out["stats"]["truncatedToolResults"] == 1


@pytest.mark.parametrize("produce", ["render_transcript", "refresh_context_observation"])
def test_render_unknown_full_uuid_fails_without_creating_files(tmp_path, monkeypatch, produce):
    from test_generation_store import A, fixture_root
    from ccx import constants
    from ccx.store.root import require_ccx_active
    from ccx.store.identity import UnknownWorker

    root = fixture_root(tmp_path)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    with pytest.raises(UnknownWorker):
        getattr(T, produce)(A)
    assert require_ccx_active(root).list_workers() == []
    assert not (root / "identity" / "generations" / ("g-" + "1" * 32) / "workers").exists()


def test_compact_result_since_detects_boundary(tmp_path):
    src = _write_jsonl(tmp_path, [
        {"type": "user", "message": {"content": "<command-name>/compact</command-name>"},
         "timestamp": "2026-06-19T12:00:00Z"},
        {"type": "system", "subtype": "compact_boundary",
         "content": "Conversation compacted",
         "compactMetadata": {
             "trigger": "manual",
             "preTokens": 12000,
             "postTokens": 3000,
             "durationMs": 42,
         },
         "timestamp": "2026-06-19T12:00:01Z"},
    ])

    result = T.compact_result_since(src)

    assert result["compacted"] is True
    assert result["pre_tokens"] == 12000
    assert result["post_tokens"] == 3000
    assert result["duration_ms"] == 42


def test_compact_result_since_detects_noop(tmp_path):
    src = _write_jsonl(tmp_path, [
        {"type": "system", "subtype": "local_command",
         "content": "<local-command-stdout>Not enough messages to compact.</local-command-stdout>",
         "timestamp": "2026-06-19T12:00:01Z"},
    ])

    result = T.compact_result_since(src)

    assert result["compacted"] is False
    assert result["message"] == "Not enough messages to compact."


def test_compact_command_seen_since_detects_command_record(tmp_path):
    src = _write_jsonl(tmp_path, [
        {"type": "user", "message": {"content": "unrelated prompt"},
         "timestamp": "2026-06-19T12:00:00Z"},
        {"type": "user",
         "message": {"content": "<command-name>/compact</command-name>\n"
                                "<command-message>compact</command-message>"},
         "timestamp": "2026-06-19T12:00:05Z"},
    ])

    assert T.compact_command_seen_since(src) is True
    # Submission acked only for a window that includes it.
    after_late = 1781870410000  # 2026-06-19T12:00:10Z, past both events
    assert T.compact_command_seen_since(src, after_ms=after_late) is False


def test_compact_command_seen_since_detects_bare_command_text(tmp_path):
    src = _write_jsonl(tmp_path, [
        {"type": "user", "message": {"content": "/compact"},
         "timestamp": "2026-06-19T12:00:00Z"},
    ])
    assert T.compact_command_seen_since(src) is True


def test_compact_command_seen_since_ignores_ordinary_turns(tmp_path):
    src = _write_jsonl(tmp_path, [
        {"type": "user", "message": {"content": "please run /compact for me"},
         "timestamp": "2026-06-19T12:00:00Z"},
        {"type": "assistant",
         "message": {"content": [{"type": "text", "text": "/compact"}]},
         "timestamp": "2026-06-19T12:00:01Z"},
    ])
    assert T.compact_command_seen_since(src) is False


def test_pending_compact_request_detects_unsettled_command(tmp_path):
    src = _write_jsonl(tmp_path, [
        {"type": "assistant", "message": {
            "content": [{"type": "text", "text": "before"}],
        }},
        {"type": "user",
         "message": {"content": "<command-name>/compact</command-name>\n"
                                "<command-message>compact</command-message>"},
         "timestamp": "2026-06-19T12:00:05Z"},
    ])

    assert T.pending_compact_request(src) == {
        "as_of_ms": 1781870405000,
    }


def test_pending_compact_request_clears_on_success_boundary(tmp_path):
    src = _write_jsonl(tmp_path, [
        {"type": "user", "message": {"content": "/compact"}},
        {"type": "system", "subtype": "compact_boundary",
         "content": "Conversation compacted",
         "compactMetadata": {"preTokens": 12000, "postTokens": 3000}},
    ])
    assert T.pending_compact_request(src) is None


def test_pending_compact_request_handles_command_replay_after_boundary(tmp_path):
    # Claude journals the boundary, then replays the command and its stdout into
    # the compacted transcript. The stdout is the terminal bookend for that
    # replayed command, not evidence of a second pending compact.
    src = _write_jsonl(tmp_path, [
        {"type": "user", "message": {"content": "/compact"}},
        {"type": "system", "subtype": "compact_boundary",
         "content": "Conversation compacted",
         "compactMetadata": {"preTokens": 12000, "postTokens": 3000}},
        {"type": "user", "message": {
            "content": "<command-name>/compact</command-name>\n"
                       "<command-message>compact</command-message>",
        }},
        {"type": "user", "message": {
            "content": "<local-command-stdout>Compacted.</local-command-stdout>",
        }},
    ])
    assert T.pending_compact_request(src) is None


def test_pending_compact_request_clears_on_noop_stdout(tmp_path):
    src = _write_jsonl(tmp_path, [
        {"type": "user", "message": {"content": "/compact"}},
        {"type": "user", "message": {
            "content": "<local-command-stdout>Not enough messages to compact.</local-command-stdout>",
        }},
    ])
    assert T.pending_compact_request(src) is None


_BARE_COMPACT = {"type": "user", "message": {"content": "/compact"}}
_CAVEAT = {"type": "user", "isMeta": True, "message": {
    "content": "<local-command-caveat>The command below was run directly in Claude Code"
               "</local-command-caveat>"}}
_WRAPPED_COMPACT = {"type": "user", "message": {
    "content": "<command-name>/compact</command-name>\n<command-message>compact</command-message>"}}


@pytest.mark.parametrize("require_command", [False, True])
@pytest.mark.parametrize("stdout, status", [
    ("<local-command-stdout>Not enough messages to compact.</local-command-stdout>", "noop"),
    ("<local-command-stderr>Error during compaction: conversation cannot be compacted"
     "</local-command-stderr>", "failed"),
])
def test_bounded_compact_result_treats_bare_and_wrapped_command_as_one(
        tmp_path, require_command, stdout, status):
    # Live Claude 2.1.312 shape of one pasted `/compact` that does not compact.
    src = _write_jsonl(tmp_path, [
        _BARE_COMPACT, _CAVEAT, _WRAPPED_COMPACT,
        {"type": "system", "subtype": "local_command", "content": stdout, "uuid": "verdict"},
    ])
    result = T.compact_result_since(src, bounded_manual=True, require_command=require_command)
    assert result is not None and result["event_uuid"] == "verdict"
    assert ("noop" if result["ok"] else "failed") == status and result["compacted"] is False


@pytest.mark.parametrize("second", [_BARE_COMPACT, _WRAPPED_COMPACT])
def test_bounded_compact_result_stops_at_a_second_submission(tmp_path, second):
    src = _write_jsonl(tmp_path, [
        _BARE_COMPACT, _WRAPPED_COMPACT, second,
        {"type": "system", "subtype": "local_command",
         "content": "<local-command-stdout>Not enough messages to compact.</local-command-stdout>"},
    ])
    assert T.compact_result_since(src, bounded_manual=True) is None
    wrapped_first = _write_jsonl(tmp_path, [
        _WRAPPED_COMPACT, _BARE_COMPACT,
        {"type": "system", "subtype": "local_command",
         "content": "<local-command-stdout>Not enough messages to compact.</local-command-stdout>"},
    ])
    assert T.compact_result_since(wrapped_first, bounded_manual=True) is None


def test_pending_compact_request_clears_on_unparsed_failure_stdout(tmp_path):
    src = _write_jsonl(tmp_path, [
        {"type": "user", "message": {"content": "/compact"}},
        {"type": "system", "subtype": "local_command",
         "content": "<local-command-stdout>Some future compact failure text.</local-command-stdout>"},
    ])
    assert T.pending_compact_request(src) is None


# --- read_ai_title -----------------------------------------------------------

def test_read_ai_title_returns_title_from_early_event(tmp_path):
    src = _write_jsonl(tmp_path, [
        {"type": "user", "message": {"content": "analyze this"}},
        {"type": "agent-name", "name": "worker"},
        {"type": "ai-title", "aiTitle": "Analyze monthly sales data"},
        {"type": "assistant", "message": {"content": [{"type": "text", "text": "ok"}]}},
    ])
    assert T.read_ai_title(src) == "Analyze monthly sales data"


def test_read_ai_title_accepts_legacy_title_key(tmp_path):
    # Older test fixtures use "title" rather than "aiTitle" — accept both.
    src = _write_jsonl(tmp_path, [
        {"type": "ai-title", "title": "Legacy title key"},
    ])
    assert T.read_ai_title(src) == "Legacy title key"


def test_read_ai_title_returns_none_when_no_ai_title(tmp_path):
    src = _write_jsonl(tmp_path, [
        {"type": "user", "message": {"content": "hello"}},
        {"type": "assistant", "message": {"content": [{"type": "text", "text": "hi"}]}},
    ])
    assert T.read_ai_title(src) is None


def test_read_ai_title_missing_file_returns_none(tmp_path):
    assert T.read_ai_title(tmp_path / "nonexistent.jsonl") is None


def test_read_ai_title_respects_max_lines_limit(tmp_path):
    # Title is beyond the scan window — should return None.
    events = [{"type": "user", "message": {"content": "hi"}}] * 10
    events.append({"type": "ai-title", "aiTitle": "Should not be seen"})
    src = _write_jsonl(tmp_path, events)
    assert T.read_ai_title(src, max_lines=5) is None


# --- foreign_prompt_since ----------------------------------------------------
#
# The transcript-side proof that Ink handed its input surface to an ordinary
# turn while an un-submitted `/compact` was sitting in the editor. Used by the
# compact pump when the UserPromptSubmit hook was missed.

def _jsonl(tmp_path, *events):
    p = tmp_path / "session.jsonl"
    p.write_text("".join(json.dumps(e) + "\n" for e in events))
    return p


def test_foreign_prompt_since_detects_real_prompt(tmp_path):
    p = _jsonl(tmp_path, {"type": "user", "message": {"content": "run the audit"}})
    assert T.foreign_prompt_since(p)


def test_foreign_prompt_since_ignores_own_compact_command(tmp_path):
    # A submitted /compact is itself a user event; it is not a foreign turn.
    p = _jsonl(tmp_path, {"type": "user", "message": {"content": "/compact"}})
    assert not T.foreign_prompt_since(p)


def test_foreign_prompt_since_ignores_tool_results(tmp_path):
    # Tool results are also type="user" but carry block lists, not a string.
    p = _jsonl(tmp_path, {"type": "user", "message": {"content": [
        {"type": "tool_result", "content": "ok"}]}})
    assert not T.foreign_prompt_since(p)


def test_foreign_prompt_since_ignores_meta_and_blank(tmp_path):
    p = _jsonl(
        tmp_path,
        {"type": "user", "isMeta": True, "message": {"content": "synthetic"}},
        {"type": "user", "message": {"content": "   "}},
        {"type": "assistant", "message": {"content": "thinking"}},
    )
    assert not T.foreign_prompt_since(p)


def test_foreign_prompt_since_respects_the_fence(tmp_path):
    # Only prompts AFTER the compact fence count; history must not trip it.
    first = json.dumps({"type": "user", "message": {"content": "old turn"}}) + "\n"
    p = tmp_path / "session.jsonl"
    p.write_text(first + json.dumps(
        {"type": "user", "message": {"content": "new turn"}}) + "\n")
    assert T.foreign_prompt_since(p, after_offset=0)
    assert T.foreign_prompt_since(p, after_offset=len(first))
    assert not T.foreign_prompt_since(p, after_offset=p.stat().st_size)


# --- foreign_prompt_since past a FINISHED compact boundary --------------------
#
# `settle_compact_terminal` fences past a compact's own boundary to ask "did a
# turn start while we were settling?". Three records land there that look like
# user prompts but are compaction's own bookkeeping. Shapes below are copied
# from real transcripts (d2baaa3e, 290a6b26) captured while diagnosing the
# stomped-BUSY race.

def test_foreign_prompt_since_ignores_the_generated_compact_summary(tmp_path):
    p = _jsonl(tmp_path, {
        "type": "user",
        "isCompactSummary": True,
        "message": {"content": "This session is being continued from a previous "
                               "conversation that ran out of context. The summary "
                               "below covers the earlier portion."},
    })
    assert not T.foreign_prompt_since(p)


def test_foreign_prompt_since_ignores_local_command_output(tmp_path):
    # The command's RESULT, journaled as a user event. Not a prompt.
    p = _jsonl(
        tmp_path,
        {"type": "user", "message": {
            "content": "<local-command-stdout>Compacted (ctrl+o to see full "
                       "summary)</local-command-stdout>"}},
        {"type": "user", "message": {
            "content": "<local-command-stderr>Not enough messages to compact"
                       "</local-command-stderr>"}},
    )
    assert not T.foreign_prompt_since(p)


def test_foreign_prompt_since_quiet_after_a_clean_compaction(tmp_path):
    """The full post-boundary record set of a compaction nothing interrupted.

    Regression for the false positive that would have reported every settled
    compact as a busy worker.
    """
    p = _jsonl(
        tmp_path,
        {"type": "user", "isCompactSummary": True,
         "message": {"content": "This session is being continued..."}},
        {"type": "user", "isMeta": True,
         "message": {"content": "<local-command-caveat>Caveat: ...</local-command-caveat>"}},
        {"type": "user", "message": {
            "content": "<command-name>/compact</command-name>\n"
                       "<command-message>compact</command-message>"}},
        {"type": "user", "message": {
            "content": "<local-command-stdout>Compacted (ctrl+o to see full "
                       "summary)</local-command-stdout>"}},
    )
    assert not T.foreign_prompt_since(p)


def test_foreign_prompt_since_sees_a_turn_flushed_after_the_boundary(tmp_path):
    """The failure case: a queued prompt flushed the instant compaction ended.

    Claude journals this user record ~200ms before firing UserPromptSubmit, and
    that hook is a no-op while the host still holds BUSY for the compact — so
    this record is the only evidence the turn exists.
    """
    p = _jsonl(
        tmp_path,
        {"type": "user", "isCompactSummary": True,
         "message": {"content": "This session is being continued..."}},
        {"type": "user", "message": {
            "content": "<local-command-stdout>Compacted</local-command-stdout>"}},
        {"type": "user", "message": {
            "content": "<task-notification>\n<task-id>b9tnm6vy2</task-id>\n"
                       "<status>completed</status>\n</task-notification>"}},
    )
    assert T.foreign_prompt_since(p)


def test_is_compact_command_text():
    assert T.is_compact_command_text("/compact")
    assert T.is_compact_command_text("<command-name>/compact</command-name>")
    assert not T.is_compact_command_text("please /compact the session")
    assert not T.is_compact_command_text(None)


def test_semantic_prompt_normalizer_unwraps_native_paste_envelope():
    wrapped = (
        '\n\n<pasted_content id="9f3c">\n'
        'do the thing\n'
        '</pasted_content id="9f3c">\n'
    )
    assert T.normalize_semantic_prompt_text(wrapped) == "do the thing"


def test_semantic_prompt_normalizer_rejects_mismatched_wrapper_ids():
    wrapped = (
        '<pasted_content id="one">\n'
        'do the thing\n'
        '</pasted_content id="two">'
    )
    assert T.normalize_semantic_prompt_text(wrapped) == wrapped


def test_exact_user_prompt_event_matches_wrapped_native_paste(tmp_path):
    p = tmp_path / "session.jsonl"
    p.write_text(json.dumps({
        "type": "user",
        "uuid": "u-semantic",
        "message": {"content": (
            '\n<pasted_content id="9f3c">\n'
            'semantic prompt\n'
            '</pasted_content id="9f3c">\n'
        )},
    }) + "\n")
    event = T.exact_user_prompt_event(p, "semantic prompt")
    assert event["uuid"] == "u-semantic"


def test_exact_user_prompt_event_matches_inline_paste_with_tabs_and_crlf(tmp_path):
    # Chunked inline pastes arrive untagged, with Claude's paste edits applied:
    # tabs expanded to four spaces and CR/CRLF turned into LF.
    p = tmp_path / "session.jsonl"
    p.write_text(json.dumps({
        "type": "user",
        "uuid": "u-inline",
        "message": {"content": "first\n    indented\nlast"},
    }) + "\n")
    event = T.exact_user_prompt_event(p, "first\r\n\tindented\rlast")
    assert event["uuid"] == "u-inline"


def test_paste_normalized_is_idempotent():
    once = T.paste_normalized("a\tb\r\nc\rd")
    assert once == "a    b\nc\nd"
    assert T.paste_normalized(once) == once


def test_cleaned_transcript_preserves_native_row_and_tool_ids(tmp_path):
    events = [
        {"type": "user", "uuid": "user-1", "message": {"content": "go"}},
        {"type": "assistant", "uuid": "frag-1", "message": {
            "id": "msg-1",
            "content": [
                {"type": "text", "text": "working"},
                {"type": "tool_use", "id": "tool-1", "name": "Bash",
                 "input": {"command": "true"}},
            ],
        }},
        {"type": "assistant", "uuid": "frag-2", "message": {
            "id": "msg-1",
            "content": [{"type": "text", "text": "done"}],
        }},
        {"type": "user", "uuid": "tool-result-row", "message": {"content": [
            {"type": "tool_result", "tool_use_id": "tool-1", "content": "ok"}
        ]}},
        {"type": "attachment", "uuid": "queue-1",
         "attachment": {"type": "queued_command", "prompt": "steer"}},
        {"type": "system", "uuid": "compact-1", "subtype": "compact_boundary",
         "content": "summary", "compactMetadata": {}},
    ]
    path = _write_jsonl(tmp_path, events)
    turns = list(T._iter_clean_turns(path))
    user = next(turn for turn in turns if turn.get("text") == "go")
    assistant = next(turn for turn in turns if turn.get("role") == "assistant")
    queued = next(turn for turn in turns if turn.get("queued"))
    system = next(turn for turn in turns if turn.get("role") == "system")
    assert user["id"] == "user-1"
    assert assistant["id"] == "msg-1"
    assert assistant["tools"][0]["id"] == "tool-1"
    assert queued["id"] == "queue-1"
    assert system["id"] == "compact-1"


def test_cleaned_transcript_does_not_fabricate_missing_native_id(tmp_path):
    path = _write_jsonl(tmp_path, [
        {"type": "user", "message": {"content": "no id"}},
        {"type": "assistant", "message": {
            "content": [{"type": "text", "text": "also no id"}],
        }},
    ])
    turns = list(T._iter_clean_turns(path))
    assert all("id" not in turn for turn in turns)


def test_iter_transcript_skips_sidechain_rows_unless_requested(tmp_path):
    p = tmp_path / "s.jsonl"
    p.write_text("\n".join(json.dumps(e) for e in [
        {"type": "user", "uuid": "parent"},
        {"type": "assistant", "uuid": "child", "isSidechain": True},
        {"type": "assistant", "uuid": "parent2", "isSidechain": False},
    ]))
    assert [e["uuid"] for e in T._iter_transcript(p)] == ["parent", "parent2"]
    assert [e["uuid"] for e in T._iter_transcript(p, include_sidechain=True)] == [
        "parent", "child", "parent2"]


def test_read_ai_title_ignores_sidechain_title(tmp_path):
    p = tmp_path / "s.jsonl"
    p.write_text("\n".join(json.dumps(e) for e in [
        {"type": "ai-title", "aiTitle": "CHILD_TITLE", "isSidechain": True},
        {"type": "ai-title", "aiTitle": "PARENT_TITLE"},
    ]))
    assert T.read_ai_title(p) == "PARENT_TITLE"


# V2 producer proofs: no arbitrary legacy record path read/write, no crossing
# workers with identical display prefix, and no native provider invocation.
def test_v2_observation_source_original_native_owner_and_private_atomic_file(tmp_path, monkeypatch):
    from ccx import constants
    from ccx.store.root import require_ccx_active
    from test_generation_store import A
    import ccx.store.store as uuid_store_mod

    source = _write_jsonl(tmp_path, [
        {"type": "user", "message": {"content": "task"}},
        {"type": "assistant", "message": {
            "model": "claude-opus-production-evidence",
            "content": [{"type": "text", "text": "done"}],
            "usage": {"input_tokens": 120, "cache_read_input_tokens": 80,
                      "output_tokens": 19},
        }},
    ])
    owner, dst, rec = _render_owned_source(tmp_path, monkeypatch, source)
    assert owner == A
    called = []
    real_open = os.open

    def prove_temp_is_private(path, flags, mode=0o777, *, dir_fd=None):
        if isinstance(path, str) and path.startswith(".new-"):
            called.append((flags, mode))
            assert mode == 0o600
            assert flags & os.O_CREAT and flags & os.O_EXCL and flags & os.O_NOFOLLOW
        return real_open(path, flags, mode, dir_fd=dir_fd)

    with monkeypatch.context() as patch:
        patch.setattr(uuid_store_mod.os, "open", prove_temp_is_private)
        observed = T.refresh_context_observation(owner)
    assert len(called) == 1  # only the observation, private
    assert stat.S_IMODE(dst.stat().st_mode) == 0o600
    assert not list(dst.parent.glob(".new-*"))
    assert observed["workerId"] == A
    assert observed["nativeSessionId"] == rec["native"]["sessionId"] != A
    assert observed["context"]["state"] == "known"
    assert observed["context"]["tokens_used"] == 200
    assert observed["observedModel"] == "claude-opus-production-evidence"
    # The existing nofollow descriptor-based store reader recognizes this
    # producer output without another path-based open or 0o644 exposure.
    store = require_ccx_active(constants.CCX_ROOT)
    assert store.read_worker_sidecar(A, "context-observation.json") == observed


def test_v2_render_ignores_untrusted_destination_record_field(tmp_path, monkeypatch):
    from ccx import constants
    from ccx.store.store import GenerationStore

    src = _write_jsonl(tmp_path, [{"type": "user", "message": {"content": "safe"}}])
    owner, dst, _rec = _render_owned_source(tmp_path, monkeypatch, src)
    outsider = tmp_path / "must-not-overwrite.json"
    outsider.write_text('{"private":"other-worker"}')
    original_record = GenerationStore.record

    def poisoned_display_path(self, target):
        result = original_record(self, target)
        result["contextObservation"] = str(outsider)
        return result

    with monkeypatch.context() as patched:
        patched.setattr(GenerationStore, "record", poisoned_display_path)
        T.refresh_context_observation(owner)
    assert json.loads(outsider.read_text()) == {"private": "other-worker"}
    assert json.loads(dst.read_text())["workerId"] == owner
    assert dst != outsider


def test_v2_render_mismatched_native_record_path_refuses_before_jsonl_read(tmp_path, monkeypatch):
    from ccx.store.store import GenerationStore
    from ccx.store.root import StorageIntegrityError

    source = _write_jsonl(tmp_path, [{"type": "user", "message": {"content": "original"}}])
    owner, dst, _rec = _render_owned_source(tmp_path, monkeypatch, source)
    foreign = tmp_path / "other.jsonl"
    foreign.write_text('{"type":"user","message":{"content":"wrong"}}\n')
    original_record = GenerationStore.record

    def switched_source(self, target):
        rec = original_record(self, target)
        rec["transcript"] = str(foreign)
        return rec

    with monkeypatch.context() as patched:
        patched.setattr(GenerationStore, "record", switched_source)
        patched.setattr(T, "_iter_transcript", lambda *_a, **_k:
                        pytest.fail("source mismatch must refuse before native parse"))
        with pytest.raises(StorageIntegrityError, match="original native project JSONL"):
            T.refresh_context_observation(owner)
    assert not dst.exists() and foreign.exists()


@pytest.mark.parametrize("source_kind", ["symlink", "fifo"])
def test_v2_render_refuses_unsafe_native_source_before_publish(tmp_path, monkeypatch, source_kind):
    from ccx import constants
    from ccx.store.root import StorageIntegrityError

    source = _write_jsonl(tmp_path, [{"type": "user", "message": {"content": "good"}}])
    owner, dst, rec = _render_owned_source(tmp_path, monkeypatch, source)
    T.refresh_context_observation(owner)
    before = dst.read_bytes()
    native = Path(rec["transcript"])
    native.unlink()
    if source_kind == "symlink":
        native.symlink_to(source)
    else:
        os.mkfifo(native, mode=0o600)
    with pytest.raises(StorageIntegrityError, match="native transcript"):
        T.refresh_context_observation(owner)
    assert dst.read_bytes() == before


@pytest.mark.parametrize("target_kind", ["symlink", "permissive"])
def test_v2_render_refuses_unsafe_existing_destination(tmp_path, monkeypatch, target_kind):
    from ccx.store.root import StorageIntegrityError

    src = _write_jsonl(tmp_path, [{"type": "user", "message": {"content": "original"}}])
    owner, dst, _rec = _render_owned_source(tmp_path, monkeypatch, src)
    outsider = tmp_path / "another-file.json"
    outsider.write_text("not owned")
    if target_kind == "symlink":
        dst.symlink_to(outsider)
    else:
        dst.write_text("not private")
        dst.chmod(0o644)
    with pytest.raises(StorageIntegrityError, match="unsafe owned JSON target"):
        T.refresh_context_observation(owner)
    assert outsider.read_text() == "not owned"
    assert not list(dst.parent.glob(".new-*"))


def test_v2_render_stale_generation_cannot_publish_into_old_worker(tmp_path, monkeypatch):
    from test_generation_store import switch_pointer
    from ccx import constants
    from ccx.store.root import StaleGeneration

    src = _write_jsonl(tmp_path, [{"type": "user", "message": {"content": "task"}}])
    owner, dst, _rec = _render_owned_source(tmp_path, monkeypatch, src)
    original_reader = T._iter_transcript
    rotated = []

    def rotate_after_pinned_read(*args, **kwargs):
        turns = list(original_reader(*args, **kwargs))
        if not rotated:
            rotated.append(True)
            switch_pointer(constants.CCX_ROOT)
        yield from turns

    with monkeypatch.context() as patched:
        patched.setattr(T, "_iter_transcript", rotate_after_pinned_read)
        with pytest.raises(StaleGeneration):
            T.refresh_context_observation(owner)
    assert rotated and not dst.exists()


def test_v2_render_missing_original_native_jsonl_creates_no_empty_render(tmp_path, monkeypatch):
    from test_generation_store import A, fixture_root
    from ccx import constants
    from ccx.store import paths
    from ccx.ops.creation_steps import prepare_worker
    from ccx.store.root import require_ccx_active

    root = fixture_root(tmp_path)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    monkeypatch.setattr(paths, "CLAUDE_HOME", tmp_path / "not-yet-used-provider-home")
    store = require_ccx_active(root)
    prepare_worker(store, engine="claude", cwd=str(tmp_path), worker_id=A)
    out = store.worker_path(A, "context-observation.json")
    assert T.refresh_context_observation(A) is None and T.render_transcript(A) is None
    assert not out.exists()
    assert store.record(A)["stats"]["turns"] == 0


def test_v2_render_refuses_original_owner_claim_native_binding_mismatch(tmp_path, monkeypatch):
    from ccx import constants
    from ccx.store.root import StorageIntegrityError
    from test_generation_store import G1, put

    src = _write_jsonl(tmp_path, [{"type": "user", "message": {"content": "original"}}])
    owner, dst, rec = _render_owned_source(tmp_path, monkeypatch, src)
    owner_path = (constants.CCX_ROOT / "identity" / "generations" / G1
                  / "owners" / f"{owner}.json")
    immutable = json.loads(owner_path.read_text())
    immutable["createIntent"]["nativeBinding"]["sessionId"] = owner
    put(owner_path, immutable)
    # A changed owner intent cannot be accepted as if it still named the
    # original native session present in the immutable worker record.
    with pytest.raises(StorageIntegrityError, match="original native binding"):
        T.refresh_context_observation(owner)
    assert not dst.exists() and rec["native"]["sessionId"] != owner


def test_v2_render_refuses_symlink_native_project_directory(tmp_path, monkeypatch):
    from ccx.store.root import StorageIntegrityError

    src = _write_jsonl(tmp_path, [{"type": "user", "message": {"content": "original"}}])
    owner, dst, rec = _render_owned_source(tmp_path, monkeypatch, src)
    original_source = Path(rec["transcript"])
    outside = tmp_path / "outside-native-project"
    outside.mkdir(mode=0o700)
    replacement = outside / original_source.name
    original_source.rename(replacement)
    original_source.parent.rmdir()
    original_source.parent.symlink_to(outside, target_is_directory=True)
    with pytest.raises(StorageIntegrityError, match="unsafe Claude native transcript source"):
        T.refresh_context_observation(owner)
    assert not dst.exists() and replacement.exists()


def test_v2_render_native_jsonl_replaced_during_parse_cannot_publish_stale_source(
    tmp_path, monkeypatch,
):
    from ccx.store.root import StorageIntegrityError

    src = _write_jsonl(tmp_path, [{"type": "user", "message": {"content": "first"}}])
    owner, dst, rec = _render_owned_source(tmp_path, monkeypatch, src)
    original_reader = T._iter_transcript

    def replace_after_pinned_read(*args, **kwargs):
        rows = list(original_reader(*args, **kwargs))
        real_source = Path(rec["transcript"])
        replacement = real_source.with_suffix(".replacement.jsonl")
        replacement.write_text(json.dumps({
            "type": "user", "message": {"content": "different new native inode"},
        }) + "\n")
        replacement.chmod(0o600)
        os.replace(replacement, real_source)
        yield from rows

    with monkeypatch.context() as patch:
        patch.setattr(T, "_iter_transcript", replace_after_pinned_read)
        with pytest.raises(StorageIntegrityError, match="changed identity"):
            T.refresh_context_observation(owner)
    assert not dst.exists()


# R1: provider-owned trees are not the stricter CCX 0700 store. On a confirmed
# user-private primary group, a provider may create 0775 directories with umask
# 0002; explicit/primary memberships must BOTH be examined before trusting one.
def _prove_test_private_primary_group(monkeypatch, *, group_name=None,
                                      supplementary=(), primary_peers=(),
                                      enumerate_self=True):
    import grp
    import pwd
    from types import SimpleNamespace

    username = "test-native-account"
    gid, uid = os.getgid(), os.getuid()
    current = SimpleNamespace(pw_name=username, pw_uid=uid, pw_gid=gid)
    other = [SimpleNamespace(pw_name=name, pw_uid=uid + i + 100,
                             pw_gid=gid) for i, name in enumerate(primary_peers)]
    group = SimpleNamespace(gr_name=group_name or username, gr_gid=gid,
                            gr_mem=list(supplementary))
    monkeypatch.setattr(pwd, "getpwuid", lambda candidate: current)
    monkeypatch.setattr(pwd, "getpwall",
                        lambda: ([current] if enumerate_self else []) + other)
    monkeypatch.setattr(grp, "getgrgid", lambda candidate: group)


@pytest.mark.parametrize("group_writable_parts", [
    ("root",), ("projects",), ("project",),
    ("root", "projects", "project"),
])
def test_r1_private_primary_group_0775_source_dirs_render(
    tmp_path, monkeypatch, group_writable_parts,
):
    from ccx import constants
    from ccx.store import paths
    from ccx.store.root import require_ccx_active

    source = _write_jsonl(tmp_path, [{
        "type": "assistant", "message": {
            "model": "model-from-original-provider",
            "content": [{"type": "text", "text": "real-group-writable-source"}],
            "usage": {"input_tokens": 1200, "output_tokens": 6},
        },
    }])
    owner, dst, record = _render_owned_source(tmp_path, monkeypatch, source)
    _prove_test_private_primary_group(monkeypatch)
    native = Path(record["transcript"])
    parts = {"root": paths.CLAUDE_HOME,
             "projects": paths.CLAUDE_HOME / "projects",
             "project": native.parent}
    for name, directory in parts.items():
        directory.chmod(0o775 if name in group_writable_parts else 0o700)
    T.refresh_context_observation(owner)
    observed = require_ccx_active(constants.CCX_ROOT).read_worker_sidecar(
        owner, "context-observation.json",
    )
    assert observed["workerId"] == owner and observed["nativeSessionId"] == record["native"]["sessionId"]
    assert observed["observedModel"] == "model-from-original-provider"
    assert observed["context"]["tokens_used"] == 1200
    assert stat.S_IMODE(dst.stat().st_mode) == 0o600
    assert all(stat.S_IMODE(directory.stat().st_mode) ==
               (0o775 if name in group_writable_parts else 0o700)
               for name, directory in parts.items())


@pytest.mark.parametrize("untrusted_kind", [
    "supplementary_other", "other_primary", "not_user_named",
    "missing_current", "unresolved_group", "unresolved_users",
])
def test_r1_group_write_refuses_unproven_or_shared_group(
    tmp_path, monkeypatch, untrusted_kind,
):
    import grp
    import pwd
    from ccx.store import paths
    from ccx.store.root import StorageIntegrityError

    source = _write_jsonl(tmp_path, [{"type": "user", "message": {"content": "task"}}])
    owner, dst, _record = _render_owned_source(tmp_path, monkeypatch, source)
    _prove_test_private_primary_group(
        monkeypatch,
        supplementary=("foreign-user",) if untrusted_kind == "supplementary_other" else (),
        primary_peers=("another-primary-member",) if untrusted_kind == "other_primary" else (),
        group_name="shared-projects" if untrusted_kind == "not_user_named" else None,
        enumerate_self=untrusted_kind != "missing_current",
    )
    if untrusted_kind == "unresolved_group":
        monkeypatch.setattr(grp, "getgrgid", lambda gid: (_ for _ in ()).throw(KeyError(gid)))
    if untrusted_kind == "unresolved_users":
        monkeypatch.setattr(pwd, "getpwall", lambda: (_ for _ in ()).throw(OSError("NSS unavailable")))
    paths.CLAUDE_HOME.chmod(0o775)
    with pytest.raises(StorageIntegrityError, match="unsafe native Claude source directory"):
        T.refresh_context_observation(owner)
    assert not dst.exists()


@pytest.mark.parametrize("component", ["root", "projects", "project"])
def test_r1_world_writable_provider_directory_stays_refused(
    tmp_path, monkeypatch, component,
):
    from ccx.store import paths
    from ccx.store.root import StorageIntegrityError

    source = _write_jsonl(tmp_path, [{"type": "user", "message": {"content": "task"}}])
    owner, dst, record = _render_owned_source(tmp_path, monkeypatch, source)
    _prove_test_private_primary_group(monkeypatch)
    directory = {"root": paths.CLAUDE_HOME,
                 "projects": paths.CLAUDE_HOME / "projects",
                 "project": Path(record["transcript"]).parent}[component]
    directory.chmod(0o777)
    with pytest.raises(StorageIntegrityError, match="unsafe native Claude source directory"):
        T.refresh_context_observation(owner)
    assert not dst.exists()


def test_r1_foreign_group_writable_directory_remains_refused(tmp_path, monkeypatch):
    from types import SimpleNamespace

    _prove_test_private_primary_group(monkeypatch)
    foreign = SimpleNamespace(st_mode=stat.S_IFDIR | 0o775,
                              st_uid=os.getuid(), st_gid=os.getgid() + 10000)
    assert not T._native_source_directory_safe(foreign)
    own_private = SimpleNamespace(st_mode=stat.S_IFDIR | 0o775,
                                  st_uid=os.getuid(), st_gid=os.getgid())
    assert T._native_source_directory_safe(own_private)
    foreign_owner = SimpleNamespace(st_mode=stat.S_IFDIR | 0o775,
                                    st_uid=os.getuid() + 1, st_gid=os.getgid())
    assert not T._native_source_directory_safe(foreign_owner)


def test_r2_original_native_surrogate_and_real_emoji_round_trip_without_metadata_change(
    tmp_path, monkeypatch,
):
    from ccx import constants
    from ccx.store.root import require_ccx_active

    # Construct actual Unicode code points, not literal \u escapes: the native
    # JSONL encoder must supply the escaped lone surrogate and emoji pair.
    broken_pair = "unfinished surrogate " + chr(0xD83D)
    emoji = "Rocket " + chr(0x1F680) + " / café / 日本語"
    source = _write_jsonl(tmp_path, [{
        "type": "assistant", "message": {
            "model": "claude-opus-unicode",
            "content": [{"type": "text", "text": broken_pair + " + " + emoji}],
            "usage": {"input_tokens": 123, "output_tokens": 12},
        },
    }])
    original = source.read_bytes()
    assert b"\\ud83d" in original and b"\\ud83d\\ude80" in original
    owner, dst, record = _render_owned_source(tmp_path, monkeypatch, source)
    restored = T.render_transcript(owner)
    assert restored["turns"][-1]["text"] == broken_pair + " + " + emoji
    assert restored["sessionId"] == record["native"]["sessionId"]
    assert restored["context"]["tokens_used"] == 123
    # The ordinary metadata writer still emits normal UTF-8 (ensure_ascii=False).
    store = require_ccx_active(constants.CCX_ROOT)
    store.mutate_record(owner, patches={"name": emoji})
    metadata = store.worker_path(owner, "record.json").read_bytes()
    assert "🚀".encode("utf-8") in metadata
    assert b"\\ud83d\\ude80" not in metadata


def test_v2_context_observation_one_completed_prefix_and_compaction(tmp_path, monkeypatch):
    """Partial native last line is not evidence or a malformed completed line."""
    from ccx import constants
    from ccx.store.root import require_ccx_active

    src = _write_jsonl(tmp_path, [{
        "type": "assistant", "message": {
            "model": "claude-real-native-model",
            "content": [{"type": "text", "text": "before compact"}],
            "usage": {"input_tokens": 1200, "output_tokens": 20},
        },
    }, {
        "type": "user", "timestamp": "2026-06-19T12:00:01Z",
        "message": {"content": "<command-name>/compact</command-name>\n"
                               "<command-message>compact</command-message>"},
    }])
    worker_id, rendered, rec = _render_owned_source(tmp_path, monkeypatch, src)
    native = Path(rec["transcript"])
    boundary = json.dumps({
        "type": "system", "subtype": "compact_boundary",
        "timestamp": "2026-06-19T12:00:02Z",
        "content": "Conversation compacted", "compactMetadata": {
            "trigger": "manual", "preTokens": 1200, "postTokens": 250,
        },
    }).encode()
    with native.open("ab") as out:
        out.write(boundary)  # Valid JSON but not yet LF-terminated.
    original_complete_size = native.stat().st_size - len(boundary)
    T.refresh_context_observation(worker_id)
    store = require_ccx_active(constants.CCX_ROOT)
    first = store.read_worker_sidecar(worker_id, "context-observation.json")
    first_render = T.render_transcript(worker_id)
    assert first["source"]["endOffset"] == original_complete_size
    assert first["context"]["state"] == "unknown"
    assert first["context"]["reason"] == "compacting"
    assert first["observedModel"] == "claude-real-native-model"
    assert first_render["stats"]["malformedLines"] == 0
    with native.open("ab") as out:
        out.write(b"\n")
    T.refresh_context_observation(worker_id)
    second = store.read_worker_sidecar(worker_id, "context-observation.json")
    assert second["source"]["endOffset"] == native.stat().st_size
    assert second["context"]["state"] == "unknown"
    assert second["context"]["reason"] == "just_compacted"
    assert second["context"]["tokens_used"] is None


def test_v2_context_observation_current_truncated_source_can_refresh(tmp_path, monkeypatch):
    """No permanent reset latch: a fresh render may replace old advisory facts."""
    from ccx import constants
    from ccx.store.root import require_ccx_active

    src = _write_jsonl(tmp_path, [{
        "type": "assistant", "message": {
            "model": "old-real-model", "content": [{"type": "text", "text": "old"}],
            "usage": {"input_tokens": 1200, "output_tokens": 5},
        },
    }])
    owner, _rendered, rec = _render_owned_source(tmp_path, monkeypatch, src)
    store = require_ccx_active(constants.CCX_ROOT)
    T.refresh_context_observation(owner)
    before = store.read_worker_sidecar(owner, "context-observation.json")
    native = Path(rec["transcript"])
    # 'w' truncates the same inode; the new source is still exactly owned.
    native.write_text(json.dumps({
        "type": "assistant", "message": {
            "model": "new-real-model", "content": [{"type": "text", "text": "new"}],
            "usage": {"input_tokens": 37, "output_tokens": 3},
        },
    }) + "\n")
    T.refresh_context_observation(owner)
    after = store.read_worker_sidecar(owner, "context-observation.json")
    assert after["source"]["ino"] == before["source"]["ino"]
    assert after["source"]["endOffset"] < before["source"]["endOffset"]
    assert after["context"]["tokens_used"] == 37
    assert after["observedModel"] == "new-real-model"


def test_v2_context_observation_ignores_synthetic_model_and_zero_usage(tmp_path, monkeypatch):
    from ccx import constants
    from ccx.store.root import require_ccx_active

    source = _write_jsonl(tmp_path, [
        {"type": "assistant", "message": {
            "model": "real-native-observed", "content": [{"type": "text", "text": "real"}],
            "usage": {"input_tokens": 42, "output_tokens": 3},
        }},
        {"type": "assistant", "message": {
            "model": "<synthetic>", "content": [{"type": "text", "text": "API error"}],
            "usage": {"input_tokens": 0, "output_tokens": 0},
        }},
    ])
    owner, _dst, _rec = _render_owned_source(tmp_path, monkeypatch, source)
    T.refresh_context_observation(owner)
    observed = require_ccx_active(constants.CCX_ROOT).read_worker_sidecar(
        owner, "context-observation.json",
    )
    assert observed["observedModel"] == "real-native-observed"
    assert observed["context"]["state"] == "known"
    assert observed["context"]["tokens_used"] == 42

def test_background_reducer_incremental_monitor_event_and_terminal(tmp_path):
    """A combined Monitor event and completed status is not silently lost."""
    events = _monitor_launch_events()
    done = _monitor_terminal_notification(
        status="completed", summary="watch ended")
    done["content"] = done["content"].replace(
        "<status>completed</status>",
        "<status>completed</status><event>build completed</event>")
    events.append(done)
    p = tmp_path / "s.jsonl"
    _write_bg_jsonl(p, events)
    [task] = T.extract_background_tasks(p)
    assert (task["status"], task["event_count"], task["last_event"]) == (
        "completed", 1, "build completed")
    reducer = T.BackgroundTaskReducer()
    for event in T._iter_transcript(p):
        reducer.feed(event)
    assert reducer.tasks() == T.extract_background_tasks(p)


def test_background_append_ignores_partial_and_reuses_state(tmp_path):
    p = tmp_path / "s.jsonl"
    source = _bg_launch_events() + [_bg_notification_event()]
    first = b"".join((json.dumps(evt) + "\n").encode() for evt in source[:2])
    last = (json.dumps(source[2]) + "\n").encode()
    p.write_bytes(first + last[:17])
    reducer = T.BackgroundTaskReducer()
    records, cursor = T.read_background_append(p, 0)
    assert len(records) == 2 and cursor == len(first)
    for evt in records:
        reducer.feed(evt)
    assert reducer.tasks()[0]["status"] == "running"
    with p.open("ab") as out:
        out.write(last[17:])
    records, cursor = T.read_background_append(p, cursor)
    assert len(records) == 1 and cursor == p.stat().st_size
    for evt in records:
        reducer.feed(evt)
    assert reducer.tasks()[0]["status"] == "completed"
    assert T.read_background_append(p, cursor) == ([], cursor)

def test_background_summary_materiality_and_bounds():
    from ccx.ops.background import summary, materially_changed, ITEM_LIMIT
    items = [{"id": f"command:{i}", "kind": "command", "status": "running",
              "label": "task"} for i in range(ITEM_LIMIT + 3)]
    snap = summary(items, as_of_ms=10)
    assert snap["running_count"] == ITEM_LIMIT + 3
    assert snap["items_truncated"] == 3
    assert len(snap["items"]) == ITEM_LIMIT
    assert not materially_changed(snap, summary(items, as_of_ms=20))
    assert materially_changed(snap, summary([], as_of_ms=20))


def test_background_claude_fold_diff_transcript_only(tmp_path):
    from ccx.ops.background import claude_task_item, diff_items
    source = _bg_launch_events()
    p = tmp_path / "s.jsonl"
    reducer = T.BackgroundTaskReducer()
    _write_bg_jsonl(p, source)
    for event in T._iter_transcript(p):
        reducer.feed(event)
    before = [x for t in reducer.tasks() if (x := claude_task_item(t))]
    assert before[0]["status"] == "running"
    notify = _bg_notification_event()
    reducer.feed(notify)
    after = [x for t in reducer.tasks() if (x := claude_task_item(t))]
    assert diff_items(before, after)[0][0] == "finished"


def test_background_reducer_irrelevant_rows_do_not_trigger_snapshots():
    reducer = T.BackgroundTaskReducer()
    assert reducer.feed({"type": "assistant", "message": {
        "content": [{"type": "text", "text": "x" * 1000}]}}) is False
    assert reducer.feed({"type": "queue-operation", "operation": "enqueue",
                         "content": "ordinary text"}) is False
    assert reducer.feed({"type": "assistant", "message": {"content": [{
        "type": "tool_use", "id": "tu", "name": "Bash",
        "input": {"command": "date", "run_in_background": True}
    }]}}) is True

def test_shared_background_summary_is_stable_bounded_and_monitor_event_large():
    from ccx.ops.background import summary, diff_items, materially_changed, ITEM_LIMIT

    active = [
        {"id": f"monitor:{i:03}", "kind": "monitor", "status": "running",
         "label": "watch", "started_at_ms": 2000 - i,
         "expires_at_ms": 3000, "event_count": 0}
        for i in range(ITEM_LIMIT + 1)
    ]
    first = summary(active, as_of_ms=100)
    assert "monitors" not in first
    assert first["running_count"] == ITEM_LIMIT + 1
    assert first["items_truncated"] == 1
    assert len(first["items"]) == ITEM_LIMIT
    assert first["items"][0]["id"] == "monitor:100"  # earliest start
    assert first["items"][-1]["id"] == "monitor:001"
    assert all(item.get("expires_at_ms") == 3000 for item in first["items"])

    # The final omitted item changes, but bounded counts/preview do not.
    later = [dict(item) for item in active]
    later[0]["event_count"] = 1
    later[0]["event"] = "x" * 1400
    updated = summary(later, as_of_ms=200)
    assert not materially_changed(first, updated)
    changes = diff_items(active, later)
    assert len(changes) == 1
    assert changes[0][0] == "event"
    assert changes[0][1]["id"] == "monitor:000"

    # An event within the public preview retains up to 1000 chars,
    # independent of the 120-char label limit.
    later[-1]["event"] = "y" * 1400
    preview = summary(later, as_of_ms=201)
    assert len(preview["items"][0]["event"]) == 1000
    assert preview["items"][0]["event"] == "y" * 1000
    from ccx.ops.background import bounded_event
    assert bounded_event("one\n  two\tthree") == "one two three"


def test_monitor_estimated_deadline_does_not_stop_without_transcript_evidence(tmp_path):
    from ccx.ops.background import claude_task_item, summary

    transcript = tmp_path / "deadline.jsonl"
    events = _monitor_launch_events(
        task_id="monitor-late", timeout_ms=1000,
        launch_ts="2026-01-01T00:00:00Z")
    _write_bg_jsonl(transcript, events)
    reducer = T.BackgroundTaskReducer()
    for event in T._iter_transcript(transcript):
        reducer.feed(event)
    [task] = reducer.tasks()
    item = claude_task_item(task)
    assert item["expires_at_ms"] == 1767225601000
    assert task["status"] == item["status"] == "running"
    # This snapshot is five seconds after the estimated expiry. A timestamp
    # is presentation metadata, not evidence of TaskStop or a terminal enqueue.
    snap = summary([item], as_of_ms=1767225606000)
    assert snap["running_count"] == 1
    assert snap["items"][0]["expires_at_ms"] == item["expires_at_ms"]
    assert T.background_task_current(task, 0)

    late = _monitor_event_notification(
        task_id="monitor-late", event="late event",
        ts="2026-01-01T00:00:07Z")
    reducer.feed(late)
    [still_running] = reducer.tasks()
    assert still_running["status"] == "running"
    assert still_running["event_count"] == 1
    done = _monitor_terminal_notification(
        task_id="monitor-late", status="completed",
        ts="2026-01-01T00:00:08Z")
    reducer.feed(done)
    assert reducer.tasks()[0]["status"] == "completed"


# --- incremental context observation ------------------------------------------

def _ts(second):
    return f"2026-06-19T12:00:{second:02d}Z"


def _observation_events():
    """Every shape the context/model/pending-compact facts depend on."""
    def assistant(mid, content, second, *, model=None, usage=None):
        message = {"content": content}
        if mid is not None:
            message["id"] = mid
        if model is not None:
            message["model"] = model
        if usage is not None:
            message["usage"] = usage
        return {"type": "assistant", "message": message, "timestamp": _ts(second)}

    text = lambda value: {"type": "text", "text": value}  # noqa: E731
    return [
        {"type": "permission-mode", "permissionMode": "default"},
        # A thinking-only response emits no turn: not even "pending" yet.
        assistant("m0", [{"type": "thinking", "thinking": "hm"}], 1,
                  model="claude-thinking-only", usage={"input_tokens": 5, "output_tokens": 1}),
        {"type": "user", "message": {"content": "first task"}, "timestamp": _ts(2)},
        # One response over three lines: model/usage from the last informative line.
        assistant("m1", [{"type": "thinking", "thinking": "plan"}], 3, model="claude-a"),
        assistant("m1", [{"type": "tool_use", "id": "t1", "name": "Bash",
                          "input": {"command": "ls"}}], 4,
                  model="claude-a", usage={"input_tokens": 10, "output_tokens": 2}),
        {"type": "user", "message": {"content": [
            {"type": "tool_result", "tool_use_id": "t1", "content": "ok"}]},
         "timestamp": _ts(5)},  # a pure tool-result echo does not end the response
        assistant("m1", [text("done")], 6, model="claude-b",
                  usage={"input_tokens": 100, "cache_read_input_tokens": 50,
                         "cache_creation_input_tokens": 7, "output_tokens": 9}),
        {"type": "assistant", "isSidechain": True, "message": {
            "id": "side", "model": "claude-subagent", "content": [text("child")],
            "usage": {"input_tokens": 999, "output_tokens": 1}}, "timestamp": _ts(7)},
        # Synthetic API-error reply: never a model, never usage.
        assistant("m2", [text("API Error")], 8, model="<synthetic>",
                  usage={"input_tokens": 0, "output_tokens": 0}),
        # Id-less lines never group.
        assistant(None, [text("no id")], 9, model="claude-c",
                  usage={"input_tokens": 3000, "output_tokens": 4}),
        assistant(None, [], 10, model="claude-empty"),
        {"type": "attachment", "attachment": {"type": "queued_command",
                                              "prompt": "steer while busy"},
         "timestamp": _ts(11)},
        {"type": "user", "message": {"content": "<command-name>/compact</command-name>\n"
                                                "<command-message>compact</command-message>"},
         "timestamp": _ts(12)},
        {"type": "system", "subtype": "local_command",
         "content": "<local-command-stderr>Error: nope</local-command-stderr>",
         "timestamp": _ts(13)},
        {"type": "user", "message": {"content": "<command-name>/compact</command-name>"},
         "timestamp": _ts(14)},
        {"type": "system", "subtype": "compact_boundary", "content": "Conversation compacted",
         "compactMetadata": {"trigger": "manual", "preTokens": 3000, "postTokens": 200},
         "timestamp": _ts(15)},
        {"type": "user", "message": {"content": [text("after compact")]}, "timestamp": _ts(16)},
        assistant("m3", [text("fresh")], 17, model="claude-d",
                  usage={"input_tokens": 40, "output_tokens": 2}),
        {"type": "system", "subtype": "turn_duration", "durationMs": 5, "timestamp": _ts(18)},
    ]


def _full_observation(path):
    """What the former whole-history render published (context, model)."""
    from ccx.claude.context import compaction_unknown_context, compute_context_usage
    turns = list(T._iter_clean_turns(path))
    context = compute_context_usage(turns)
    pending = T.pending_compact_request(path)
    if pending is not None:
        context = compaction_unknown_context("compacting", as_of_ms=pending.get("as_of_ms"))
    model = next((turn["model"] for turn in reversed(turns)
                  if turn.get("role") == "assistant" and T._valid_observed_model(turn.get("model"))),
                 None)
    return context, model


def test_observation_reducer_matches_full_render_at_every_cut_and_resume(tmp_path):
    lines = [json.dumps(event) for event in _observation_events()]
    lines.insert(5, "}} not json {{")
    seen_states = set()
    for end in range(len(lines) + 1):
        path = tmp_path / f"prefix-{end}.jsonl"
        path.write_text("".join(line + "\n" for line in lines[:end]))
        expected = _full_observation(path)
        events = list(T._iter_transcript(path))
        for split in range(len(events) + 1):
            first = T._ObservationReducer()
            for event in events[:split]:
                first.feed(event)
            # The persisted cursor is JSON; resume from exactly that.
            state = json.loads(json.dumps(first.state()))
            assert T._ObservationReducer.valid_state(state)
            resumed = T._ObservationReducer(state)
            for event in events[split:]:
                resumed.feed(event)
            assert resumed.observe() == expected, (end, split)
        seen_states.add(json.dumps(expected[0], sort_keys=True))
    # The fixture really walks through every context state.
    reasons = {json.loads(s)["state"] if s != "null" else None for s in seen_states}
    assert reasons == {None, "pending", "known", "unknown"}


def _observation_store(tmp_path, monkeypatch, events):
    src = _write_jsonl(tmp_path, events)
    owner, observation, rec = _render_owned_source(tmp_path, monkeypatch, src)
    return owner, observation, Path(rec["transcript"])


def _count_parse_starts(monkeypatch):
    starts = []
    original = T._iter_transcript

    def recording(path, *args, **kwargs):
        starts.append(kwargs.get("after_offset", 0))
        return original(path, *args, **kwargs)

    monkeypatch.setattr(T, "_iter_transcript", recording)
    return starts


def test_refresh_parses_only_appended_complete_lines(tmp_path, monkeypatch):
    events = _observation_events()
    owner, _observation, native = _observation_store(tmp_path, monkeypatch, events[:7])
    starts = _count_parse_starts(monkeypatch)
    first = T.refresh_context_observation(owner)
    assert starts == [0] and first["context"]["tokens_used"] == 157
    with native.open("ab") as out:
        for event in events[7:12]:
            out.write((json.dumps(event) + "\n").encode())
        out.write(json.dumps(events[12]).encode())  # partial: no newline yet
    second = T.refresh_context_observation(owner)
    assert starts == [0, first["source"]["endOffset"]]
    assert second["source"]["endOffset"] == native.stat().st_size - len(json.dumps(events[12]))
    with native.open("ab") as out:
        out.write(b"\n")
        for event in events[13:]:
            out.write((json.dumps(event) + "\n").encode())
    third = T.refresh_context_observation(owner)
    assert starts[-1] == second["source"]["endOffset"]
    assert (third["context"], third["observedModel"]) == _full_observation(native)
    assert third["context"]["tokens_used"] == 40 and third["observedModel"] == "claude-d"
    # Nothing new: resume at the end, same facts.
    again = T.refresh_context_observation(owner)
    assert starts[-1] == native.stat().st_size
    assert again["context"] == third["context"] and again["cursor"] == third["cursor"]


@pytest.mark.parametrize("damage", ["rewritten_longer", "cursor_version", "bad_state",
                                    "other_inode", "missing"])
def test_refresh_starts_again_when_the_cursor_cannot_be_trusted(tmp_path, monkeypatch, damage):
    from ccx import constants
    from ccx.store.root import _write_json, require_ccx_active

    events = _observation_events()
    owner, observation, native = _observation_store(tmp_path, monkeypatch, events[:7])
    first = T.refresh_context_observation(owner)
    store = require_ccx_active(constants.CCX_ROOT)
    doc = json.loads(observation.read_text())
    if damage == "rewritten_longer":
        # Same inode, truncated and rewritten past the old offset: the bytes
        # before the old offset changed, so resuming there would be wrong.
        rewritten = [{"type": "user", "message": {"content": "x" * 2000}, "timestamp": _ts(1)},
                     *events[16:]]
        native.write_text("".join(json.dumps(e) + "\n" for e in rewritten))
        assert native.stat().st_size > first["source"]["endOffset"]
    elif damage == "cursor_version":
        doc["cursor"]["version"] = 999
    elif damage == "bad_state":
        doc["cursor"]["state"] = {"anyTurn": "yes"}
    elif damage == "other_inode":
        doc["source"]["ino"] += 1
    else:
        doc.pop("cursor")
    if damage != "rewritten_longer":
        with store._gen("workers", "claude", owner) as fd:
            _write_json(fd, "context-observation.json", doc)
    starts = _count_parse_starts(monkeypatch)
    refreshed = T.refresh_context_observation(owner)
    assert starts == [0]
    assert (refreshed["context"], refreshed["observedModel"]) == _full_observation(native)

