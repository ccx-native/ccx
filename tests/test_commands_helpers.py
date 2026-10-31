'Pure helpers inside ccx.cli.main.commands: list filtering, prompt-source validation,\nrecency/relative-time math.\n\nThe command *functions* (cmd_new, cmd_send, …) drive real sessions and are out\nof scope here. Only their side-effect-free helpers are exercised, using plain\nSimpleNamespace stand-ins for the argparse result.\n'

from __future__ import annotations
import ccx.cli.commands.read as commands_read

from types import SimpleNamespace

import pytest

from ccx.cli.commands import _common as C
from ccx.claude import spawn as claude_spawn
from ccx.ops import semantics as semantic_helpers
from ccx import constants
from ccx.ops import creation_steps as creation_validation
from ccx.errors import InvalidRequest








# --- systemd scope wrapping (CCX_SPAWN_SCOPE) --------------------------------

def test_scope_enabled_truthy_values():
    for v in ("1", "true", "TRUE", "yes", "on", " 1 "):
        assert claude_spawn._scope_enabled(v) is True, v


def test_scope_enabled_falsey_values():
    for v in (None, "", "0", "false", "no", "off", "maybe"):
        assert claude_spawn._scope_enabled(v) is False, v


def test_scope_unit_name_sanitizes_unsafe_chars():
    assert claude_spawn._scope_unit_name("a/b c.d", "run/1") == "ccx-a-b-c-d-run-1"


def test_host_exec_argv_direct_when_env_falsey():
    prog, argv = claude_spawn._host_exec_argv(
        "abc12345", executable="/py", script="/s", scope_env="0",
        systemd_run="/usr/bin/systemd-run",
    )
    # Falsey env wins even if systemd-run is available.
    assert prog == "/py"
    assert argv == ["/py", "/s", "host", "abc12345"]


def test_host_exec_argv_wraps_when_enabled_and_available():
    prog, argv = claude_spawn._host_exec_argv(
        "abc12345", executable="/py", script="/s", scope_env="1",
        systemd_run="/usr/bin/systemd-run", scope_instance="run-1",
    )
    assert prog == "/usr/bin/systemd-run"
    assert argv == [
        "/usr/bin/systemd-run", "--user", "--scope", "--quiet", "--collect",
        "--unit=ccx-abc12345-run-1", "--",
        "/py", "/s", "host", "abc12345",
    ]


def test_host_exec_argv_falls_back_when_systemd_run_absent():
    prog, argv = claude_spawn._host_exec_argv(
        "abc12345", executable="/py", script="/s", scope_env="1",
        systemd_run="",  # explicit "not found" — no fork to a missing binary
    )
    assert prog == "/py"
    assert argv == ["/py", "/s", "host", "abc12345"]


# --- _resolve_prompt_source --------------------------------------------------

def _prompt_args(attr="prompt", value=None, stdin=False, file=None):
    return SimpleNamespace(**{attr: value, "stdin": stdin, "file": file})


def test_resolve_prompt_none_allowed_when_optional():
    args = _prompt_args(value=None)
    assert C._resolve_prompt_source(args, "prompt", "new", required=False) is None


def test_resolve_prompt_required_missing_is_a_usage_error():
    """Bad input exits EXIT_ERROR; 2 is reserved for "not found"."""
    args = _prompt_args(attr="text", value=None)
    with pytest.raises(SystemExit) as ei:
        C._resolve_prompt_source(args, "text", "send", required=True)
    assert ei.value.code == C.EXIT_ERROR


def test_resolve_prompt_multiple_sources_exits_1():
    args = _prompt_args(value="hi", stdin=True)
    with pytest.raises(SystemExit) as ei:
        C._resolve_prompt_source(args, "prompt", "new", required=False)
    assert ei.value.code == 1


def test_resolve_prompt_from_file(tmp_path):
    f = tmp_path / "prompt.txt"
    f.write_text("from a file")
    args = _prompt_args(file=str(f))
    assert C._resolve_prompt_source(args, "prompt", "new", required=False) == "from a file"
























# --- _handle_turn_final ------------------------------------------------------

from ccx.constants import BUDGET_EXIT, EXIT_TURN_INCOMPLETE, EXIT_UNAVAILABLE, EXIT_WAITING


# --- _nbwait_exit_code -------------------------------------------------------

def test_nbwait_exit_code_blocked():
    assert C._nbwait_exit_code("blocked") == EXIT_WAITING


def test_nbwait_exit_code_gone():
    assert C._nbwait_exit_code("gone") == EXIT_UNAVAILABLE


def test_nbwait_exit_code_budget_stopped():
    assert C._nbwait_exit_code("budget-stopped") == BUDGET_EXIT


def test_nbwait_exit_code_timeout():
    assert C._nbwait_exit_code("timeout") == EXIT_TURN_INCOMPLETE


def test_question_answer_message_formats_verbatim_pairs():
    msg = semantic_helpers.question_answer_message(
        {
            "Which inbox?": "Gmail (claude.ai)",
            "Scope?": "Unread only",
        }
    )
    assert msg == (
        "User provided answer to the previous question:\n"
        "Question: Which inbox?\n"
        "Answer: Gmail (claude.ai)\n"
        "Question: Scope?\n"
        "Answer: Unread only"
    )


def test_nbwait_exit_code_host_dead_exits_unavailable():
    assert C._nbwait_exit_code("host-dead") == EXIT_UNAVAILABLE


# --- _background_task_facts --------------------------------------------------

def _bg_transcript(tmp_path):
    """Write a minimal transcript with one running + one completed bg task."""
    import json
    p = tmp_path / "raw.jsonl"
    events = [
        {"type": "assistant", "timestamp": "2026-01-01T00:00:01Z", "message": {"content": [
            {"type": "tool_use", "id": "toolu_a", "name": "Bash",
             "input": {"command": "npm run dev", "run_in_background": True}}]}},
        {"type": "user", "timestamp": "2026-01-01T00:00:01Z",
         "toolUseResult": {"backgroundTaskId": "aaa"},
         "message": {"content": [
            {"type": "tool_result", "tool_use_id": "toolu_a",
             "content": "Command running in background with ID: aaa. "
                        "Output is being written to: /tmp/x/aaa.output."}]}},
        {"type": "assistant", "timestamp": "2026-01-01T00:00:02Z", "message": {"content": [
            {"type": "tool_use", "id": "toolu_b", "name": "Bash",
             "input": {"command": "make test", "run_in_background": True}}]}},
        {"type": "user", "timestamp": "2026-01-01T00:00:02Z",
         "toolUseResult": {"backgroundTaskId": "bbb"},
         "message": {"content": [
            {"type": "tool_result", "tool_use_id": "toolu_b",
             "content": "Command running in background with ID: bbb. "
                        "Output is being written to: /tmp/x/bbb.output."}]}},
        {"type": "queue-operation", "operation": "enqueue",
         "timestamp": "2026-01-01T00:00:05Z",
         "content": "<task-notification><task-id>bbb</task-id>"
                    "<status>completed</status><summary>done (exit code 0)</summary>"
                    "</task-notification>"},
    ]
    p.write_text("\n".join(json.dumps(e) for e in events))
    return p


def _stopped_bg_transcript(tmp_path):
    """Write one task settled through a successful TaskStop result."""
    import json
    p = tmp_path / "stopped.jsonl"
    events = [
        {"type": "assistant", "timestamp": "2026-01-01T00:00:01Z",
         "message": {"content": [
             {"type": "tool_use", "id": "toolu_a", "name": "Bash",
              "input": {"command": "npm run dev", "run_in_background": True}},
         ]}},
        {"type": "user", "timestamp": "2026-01-01T00:00:01Z",
         "toolUseResult": {"backgroundTaskId": "aaa"},
         "message": {"content": [
             {"type": "tool_result", "tool_use_id": "toolu_a",
              "content": "Command running in background with ID: aaa."},
         ]}},
        {"type": "assistant", "timestamp": "2026-01-01T00:00:05Z",
         "message": {"content": [
             {"type": "tool_use", "id": "toolu_stop", "name": "TaskStop",
              "input": {"task_id": "aaa"}},
         ]}},
        {"type": "user", "timestamp": "2026-01-01T00:00:05Z",
         "toolUseResult": {
             "task_id": "aaa",
             "message": "Successfully stopped task: aaa (npm run dev)",
             "command": "npm run dev",
         },
         "message": {"content": [
             {"type": "tool_result", "tool_use_id": "toolu_stop",
              "content": "stopped", "is_error": False},
         ]}},
    ]
    p.write_text("\n".join(json.dumps(e) for e in events))
    return p


def test_background_task_facts_counts_and_list(tmp_path):
    rec = {"transcript": str(_bg_transcript(tmp_path))}
    facts = commands_read._background_task_facts(rec)
    assert facts["background_task_count"] == 2
    assert facts["running_background_task_count"] == 1
    assert facts["open_background_task_count"] == 1
    assert facts["active_background_task_count"] == 0
    assert facts["stale_background_task_count"] == 0
    assert facts["background_tasks_truncated"] is False
    assert facts["background_task_limit"] == constants.BACKGROUND_TASK_SURFACE_LIMIT
    statuses = {t["task_id"]: t["status"] for t in facts["background_tasks"]}
    assert statuses == {"aaa": "running", "bbb": "completed"}
    liveness = {t["task_id"]: t["liveness"] for t in facts["background_tasks"]}
    assert liveness == {"aaa": "unknown", "bbb": "settled"}


def test_background_task_facts_treats_task_stop_as_settled(monkeypatch, tmp_path):
    monkeypatch.setattr(commands_read, "_background_task_host_live", lambda rec: True)
    facts = commands_read._background_task_facts({
        "short": "abc12345",
        "transcript": str(_stopped_bg_transcript(tmp_path)),
    })
    assert facts["background_task_count"] == 1
    assert facts["running_background_task_count"] == 0
    assert facts["open_background_task_count"] == 0
    assert facts["active_background_task_count"] == 0
    assert facts["stale_background_task_count"] == 0
    assert facts["background_tasks"][0]["status"] == "stopped"
    assert facts["background_tasks"][0]["liveness"] == "settled"
    assert "background_task_hint" not in facts


def _async_agent_transcript(tmp_path, *, completed=False):
    """One async Agent launch in the real Claude 2.1.287 shape (no input flag)."""
    import json
    p = tmp_path / "agent.jsonl"
    events = [
        {"type": "assistant", "timestamp": "2026-01-01T00:00:01Z",
         "message": {"content": [
             {"type": "tool_use", "id": "toolu_agent", "name": "Agent",
              "input": {"description": "sleep probe", "prompt": "p",
                        "subagent_type": "general-purpose"}},
         ]}},
        {"type": "user", "timestamp": "2026-01-01T00:00:01Z",
         "toolUseResult": {"isAsync": True, "status": "async_launched",
                           "agentId": "a424942a9e00ccf87",
                           "description": "sleep probe",
                           "outputFile": "/tmp/x/a424942a9e00ccf87.output"},
         "message": {"content": [
             {"type": "tool_result", "tool_use_id": "toolu_agent",
              "content": [{"type": "text",
                           "text": "Async agent launched successfully.\n"
                                   "agentId: a424942a9e00ccf87"}]},
         ]}},
    ]
    if completed:
        events.append({
            "type": "queue-operation", "operation": "enqueue",
            "timestamp": "2026-01-01T00:00:09Z",
            "content": "<task-notification><task-id>a424942a9e00ccf87</task-id>"
                       "<tool-use-id>toolu_agent</tool-use-id>"
                       "<status>completed</status>"
                       "<summary>Agent \"sleep probe\" finished</summary>"
                       "</task-notification>"})
    p.write_text("\n".join(json.dumps(e) for e in events))
    return p


def test_background_task_facts_live_async_agent_is_active(monkeypatch, tmp_path):
    monkeypatch.setattr(commands_read, "_background_task_host_live", lambda rec: True)
    facts = commands_read._background_task_facts({
        "short": "abc12345",
        "transcript": str(_async_agent_transcript(tmp_path)),
    })
    assert facts["active_background_task_count"] == 1
    assert facts["stale_background_task_count"] == 0
    [task] = facts["background_tasks"]
    assert (task["kind"], task["status"], task["liveness"]) == (
        "subagent", "running", "live")
    assert "background_task_hint" in facts


def test_background_task_facts_async_agent_with_dead_host_is_stale(monkeypatch, tmp_path):
    monkeypatch.setattr(commands_read, "_background_task_host_live", lambda rec: False)
    facts = commands_read._background_task_facts({
        "short": "abc12345",
        "transcript": str(_async_agent_transcript(tmp_path)),
    })
    assert facts["active_background_task_count"] == 0
    assert facts["stale_background_task_count"] == 1


def test_background_task_facts_caps_exposed_list_but_preserves_counts(tmp_path):
    import json

    p = tmp_path / "many.jsonl"
    events = []
    for i in range(constants.BACKGROUND_TASK_SURFACE_LIMIT + 3):
        task_id = f"task{i:02d}"
        tool_use_id = f"toolu_{i:02d}"
        ts = f"2026-01-01T00:00:{i:02d}Z"
        events.extend([
            {"type": "assistant", "timestamp": ts, "message": {"content": [
                {"type": "tool_use", "id": tool_use_id, "name": "Bash",
                 "input": {"command": f"cmd {i}", "run_in_background": True}}]}},
            {"type": "user", "timestamp": ts,
             "toolUseResult": {"backgroundTaskId": task_id},
             "message": {"content": [
                {"type": "tool_result", "tool_use_id": tool_use_id,
                 "content": f"Command running in background with ID: {task_id}. "
                            f"Output is being written to: /tmp/x/{task_id}.output."}]}},
        ])
    p.write_text("\n".join(json.dumps(e) for e in events))

    facts = commands_read._background_task_facts({"transcript": str(p)})

    assert facts["background_task_count"] == constants.BACKGROUND_TASK_SURFACE_LIMIT + 3
    assert facts["running_background_task_count"] == constants.BACKGROUND_TASK_SURFACE_LIMIT + 3
    assert facts["open_background_task_count"] == constants.BACKGROUND_TASK_SURFACE_LIMIT + 3
    assert facts["active_background_task_count"] == 0
    assert facts["stale_background_task_count"] == 0
    assert facts["background_tasks_truncated"] is True
    assert facts["background_task_limit"] == constants.BACKGROUND_TASK_SURFACE_LIMIT
    assert len(facts["background_tasks"]) == constants.BACKGROUND_TASK_SURFACE_LIMIT
    assert [t["task_id"] for t in facts["background_tasks"]][0] == "task03"
    assert [t["task_id"] for t in facts["background_tasks"]][-1] == "task12"


def test_background_task_facts_empty_without_transcript():
    facts = commands_read._background_task_facts({})
    assert facts == {"background_tasks": [],
                     "background_task_count": 0,
                     "open_background_task_count": 0,
                     "active_background_task_count": 0,
                     "stale_background_task_count": 0,
                     "running_background_task_count": 0,
                     "background_tasks_truncated": False,
                     "background_task_limit": constants.BACKGROUND_TASK_SURFACE_LIMIT}
    # None record is tolerated too.
    assert commands_read._background_task_facts(None)["background_task_count"] == 0


# --- _reject_model_as_agent ------------------------------------------------
# `--agent` names a subagent; a model belongs in `--model`. `claude --agent
# opus` exits before turn 1, which ccx can only report as state=crashed. The
# guard sits on the shared spawn path so the CLI and CcxClient both fail before
# a host is launched.

@pytest.mark.parametrize("alias", ["fable", "opus", "sonnet", "haiku"])
def test_reject_model_as_agent_rejects_every_alias(alias):
    with pytest.raises(InvalidRequest) as e:
        creation_validation._reject_model_as_agent(alias)
    assert f"--model {alias}" in str(e.value)


def test_reject_model_as_agent_is_case_and_space_insensitive():
    with pytest.raises(InvalidRequest):
        creation_validation._reject_model_as_agent("  Opus ")


def test_reject_model_as_agent_allows_real_agents_and_unset():
    for ok in (None, "", "Explore", "general-purpose", "claude", "my-opus-reviewer"):
        creation_validation._reject_model_as_agent(ok)  # must not raise
