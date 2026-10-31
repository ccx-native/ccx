from __future__ import annotations

import json

from ccx.claude import analytics as A
from ccx.store import paths as P


def _write_jsonl(path, events):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(e) for e in events) + "\n")
    return path


def _record(short="an100000", session_id="an100000-session", cwd="/work"):
    # Pure compute input: only the metadata compute_session_analytics reads.
    return {"short": short, "sessionId": session_id, "name": "analytics", "cwd": cwd}


def test_analytics_counts_repeated_content_block_usage_once_per_message_id(tmp_path):
    src = tmp_path / "session.jsonl"
    repeated_usage = {
        "input_tokens": 100,
        "output_tokens": 20,
        "cache_read_input_tokens": 1000,
        "cache_creation_input_tokens": 50,
    }
    events = [
        {
            "type": "user",
            "promptId": "p1",
            "message": {"content": "do work"},
        },
        {
            "type": "assistant",
            "message": {
                "id": "msg_same",
                "model": "claude-test",
                "usage": repeated_usage,
                "content": [{"type": "thinking", "thinking": "hidden"}],
            },
        },
        {
            "type": "assistant",
            "message": {
                "id": "msg_same",
                "model": "claude-test",
                "usage": repeated_usage,
                "content": [{
                    "type": "tool_use",
                    "id": "tool-1",
                    "name": "Bash",
                    "input": {"command": "true"},
                }],
            },
        },
        {
            "type": "assistant",
            "message": {
                "id": "msg_same",
                "model": "claude-test",
                "usage": repeated_usage,
                "content": [{"type": "text", "text": "done"}],
            },
        },
    ]
    rec = _record()
    rec["transcript"] = str(_write_jsonl(src, events))

    doc = A.compute_session_analytics(rec)

    expected = {
        "input": 100,
        "output": 20,
        "cacheRead": 1000,
        "cacheCreation": 50,
    }
    assert doc["totals"]["tokens"] == expected
    assert doc["prompts"][0]["tokens"] == expected
    assert doc["totals"]["byModel"]["claude-test"]["events"] == 1
    assert doc["totals"]["byModel"]["claude-test"]["tokens"] == expected
    # Content blocks are still individually meaningful even though usage is
    # message-scoped.
    assert doc["totals"]["tools"] == {"Bash": 1}


def test_analytics_same_message_id_uses_latest_usage_without_double_counting(tmp_path):
    src = tmp_path / "session.jsonl"
    events = [
        {"type": "user", "promptId": "p1", "message": {"content": "go"}},
        {
            "type": "assistant",
            "message": {
                "id": "msg_same",
                "model": "claude-test",
                "usage": {"input_tokens": 10, "output_tokens": 2},
                "content": [{"type": "thinking", "thinking": "hidden"}],
            },
        },
        {
            "type": "assistant",
            "message": {
                "id": "msg_same",
                "model": "claude-test",
                "usage": {"input_tokens": 12, "output_tokens": 3},
                "content": [{"type": "text", "text": "done"}],
            },
        },
    ]
    rec = _record()
    rec["transcript"] = str(_write_jsonl(src, events))

    doc = A.compute_session_analytics(rec)

    assert doc["totals"]["tokens"] == {
        "input": 12,
        "output": 3,
        "cacheRead": 0,
        "cacheCreation": 0,
    }
    assert doc["totals"]["byModel"]["claude-test"]["events"] == 1


def test_subagent_rollup_dedupes_repeated_message_usage(tmp_path, monkeypatch):
    sub = tmp_path / "subagent.jsonl"
    repeated = {"input_tokens": 7, "output_tokens": 8}
    _write_jsonl(sub, [
        {
            "type": "assistant",
            "message": {
                "id": "sub-msg",
                "model": "claude-sub",
                "usage": repeated,
                "content": [{"type": "thinking", "thinking": "hidden"}],
            },
        },
        {
            "type": "assistant",
            "message": {
                "id": "sub-msg",
                "model": "claude-sub",
                "usage": repeated,
                "content": [{"type": "text", "text": "done"}],
            },
        },
    ])
    stats = {}

    rollup = A._read_subagent_rollup(sub, stats)

    assert rollup["tokens"] == {
        "input": 7,
        "output": 8,
        "cacheRead": 0,
        "cacheCreation": 0,
    }
    assert rollup["byModel"]["claude-sub"]["events"] == 1


def test_compute_session_analytics_segments_and_boundary_steering(tmp_path):
    src = tmp_path / "session.jsonl"
    events = [
        {"type": "user", "timestamp": "2026-01-01T00:00:00Z", "promptId": "p1",
         "message": {"content": "do work"}},
        {"type": "assistant", "timestamp": "2026-01-01T00:00:01Z", "message": {
            "model": "claude-test",
            "usage": {"input_tokens": 10, "output_tokens": 2,
                      "cache_read_input_tokens": 100, "cache_creation_input_tokens": 5},
            "content": [{"type": "tool_use", "id": "t1", "name": "Bash", "input": {"command": "false"}}],
        }},
        {"type": "user", "timestamp": "2026-01-01T00:00:02Z", "promptId": "p1",
         "message": {"content": [
             {"type": "tool_result", "tool_use_id": "t1", "content": "bad", "is_error": True}
         ]}},
        {"type": "queue-operation", "timestamp": "2026-01-01T00:00:03Z",
         "operation": "enqueue", "content": "steer now"},
        {"type": "attachment", "timestamp": "2026-01-01T00:00:04Z",
         "message": {"content": [{"type": "queued_command", "prompt": "steer now"}]}},
        {"type": "assistant", "timestamp": "2026-01-01T00:00:05Z", "message": {
            "model": "claude-test",
            "usage": {"input_tokens": 20, "output_tokens": 3,
                      "cache_read_input_tokens": 200, "cache_creation_input_tokens": 7},
            "content": [{"type": "text", "text": "done"}],
        }},
    ]
    rec = _record()
    rec["transcript"] = str(_write_jsonl(src, events))

    doc = A.compute_session_analytics(rec)

    assert doc["totals"]["turns"] == 1
    assert len(doc["prompts"]) == 1
    prompt = doc["prompts"][0]
    assert prompt["promptId"] == "p1"
    assert prompt["tokens"] == {"input": 30, "output": 5, "cacheRead": 300, "cacheCreation": 12}
    assert doc["totals"]["tokens"] == prompt["tokens"]
    assert prompt["tools"] == {"Bash": 1}
    assert doc["totals"]["toolErrors"] == 1
    assert prompt["steering"] == {"enqueued": 1, "boundary": 1, "idle": 0}
    assert doc["totals"]["steering"] == {"enqueued": 1, "boundary": 1, "idle": 0}
    assert doc["totals"]["startedAt"] == 1767225600000
    assert doc["totals"]["endedAt"] == 1767225605000
    assert doc["totals"]["activeMs"] == 5000
    assert doc["totals"]["transcriptSpanMs"] == 5000
    assert "source" not in doc
    assert "textPreview" not in prompt
    assert "do work" not in json.dumps(doc)
    assert str(src) not in json.dumps(doc)


def test_idle_queued_prompt_starts_new_segment_when_not_boundary(tmp_path):
    src = tmp_path / "session.jsonl"
    events = [
        {"type": "user", "timestamp": "2026-01-01T00:00:00Z", "promptId": "p1",
         "message": {"content": "first"}},
        {"type": "assistant", "timestamp": "2026-01-01T00:00:01Z", "message": {
            "model": "claude-test", "usage": {"input_tokens": 1, "output_tokens": 1},
            "content": [{"type": "text", "text": "one"}],
        }},
        {"type": "queue-operation", "timestamp": "2026-01-01T00:00:02Z",
         "operation": "enqueue", "content": "later"},
        {"type": "user", "timestamp": "2026-01-01T00:00:03Z", "promptId": "p2",
         "message": {"content": "later"}},
        {"type": "assistant", "timestamp": "2026-01-01T00:00:04Z", "message": {
            "model": "claude-test", "usage": {"input_tokens": 2, "output_tokens": 2},
            "content": [{"type": "text", "text": "two"}],
        }},
    ]
    rec = _record()
    rec["transcript"] = str(_write_jsonl(src, events))

    doc = A.compute_session_analytics(rec)

    assert [p["promptId"] for p in doc["prompts"]] == ["p1", "p2"]
    assert doc["prompts"][1]["steering"] == {"enqueued": 0, "boundary": 0, "idle": 1}
    assert doc["totals"]["steering"] == {"enqueued": 1, "boundary": 0, "idle": 1}


def test_subagent_rollup_attributes_tokens_to_parent_prompt(tmp_path):
    rec = _record(short="sub10000", session_id="sub10000-session", cwd="/work")
    rec["transcript"] = str(_write_jsonl(tmp_path / "parent.jsonl", [
        {"type": "user", "timestamp": "2026-01-01T00:00:00Z", "promptId": "p1",
         "message": {"content": "launch task"}},
        {"type": "assistant", "timestamp": "2026-01-01T00:00:01Z", "message": {
            "model": "claude-main", "usage": {"input_tokens": 10, "output_tokens": 1},
            "content": [{"type": "tool_use", "id": "task1", "name": "Task", "input": {"description": "check"}}],
        }},
    ]))
    sub_path = P.subagent_dir_for(rec["cwd"], rec["sessionId"]) / "agent-a.jsonl"
    _write_jsonl(sub_path, [
        {"type": "user", "timestamp": "2026-01-01T00:00:02Z", "sessionId": rec["sessionId"],
         "promptId": "p1", "agentId": "agent-a", "attributionAgent": "Explore",
         "message": {"content": "sub"}},
        {"type": "assistant", "timestamp": "2026-01-01T00:00:03Z", "sessionId": rec["sessionId"],
         "promptId": "p1", "agentId": "agent-a", "attributionAgent": "Explore",
         "message": {"model": "claude-sub", "usage": {"input_tokens": 7, "output_tokens": 8},
                     "content": [{"type": "text", "text": "sub done"}]}},
    ])

    doc = A.compute_session_analytics(rec)

    assert doc["totals"]["subagents"]["count"] == 1
    assert doc["totals"]["subagents"]["tokens"]["input"] == 7
    assert doc["totals"]["tokens"]["input"] == 17
    assert doc["totals"]["subagents"]["byAgent"]["Explore"]["count"] == 1
    assert doc["prompts"][0]["subagents"]["tokens"]["output"] == 8
    assert doc["prompts"][0]["tokens"]["output"] == 9
    assert doc["totals"]["byModel"]["claude-sub"]["tokens"]["input"] == 7


def test_duplicate_prompt_id_and_synthetic_messages_do_not_fragment_segments(tmp_path):
    src = tmp_path / "session.jsonl"
    events = [
        {"type": "user", "timestamp": "2026-01-01T00:00:00Z", "promptId": "p1",
         "message": {"content": "real prompt"}},
        {"type": "user", "timestamp": "2026-01-01T00:00:01Z", "promptId": "p1",
         "message": {"content": "<bash-input>ignored</bash-input>"}},
        {"type": "user", "timestamp": "2026-01-01T00:00:02Z", "promptId": "p1",
         "message": {"content": "<local-command-caveat>ignored</local-command-caveat>"}},
        {"type": "assistant", "timestamp": "2026-01-01T00:00:03Z", "message": {
            "model": "claude-test", "usage": {"input_tokens": 3, "output_tokens": 4},
            "content": [{"type": "text", "text": "done"}],
        }},
        {"type": "user", "timestamp": "2026-01-01T00:00:04Z", "promptId": "auto",
         "message": {"content": "Continue from where you left off."}},
        {"type": "assistant", "timestamp": "2026-01-01T00:00:05Z", "message": {
            "model": "<synthetic>", "usage": {"input_tokens": 1000, "output_tokens": 1000},
            "content": [{"type": "text", "text": "synthetic"}],
        }},
    ]
    rec = _record()
    rec["transcript"] = str(_write_jsonl(src, events))

    doc = A.compute_session_analytics(rec)

    assert [p["promptId"] for p in doc["prompts"]] == ["p1"]
    assert doc["totals"]["tokens"] == {"input": 3, "output": 4, "cacheRead": 0, "cacheCreation": 0}
    assert "<synthetic>" not in doc["totals"]["byModel"]
    assert doc["prompts"][0]["durationMs"] == 3000
    assert doc["totals"]["activeMs"] == 3000
    assert doc["totals"]["transcriptSpanMs"] == 5000
    assert "wallClockMs" not in doc["totals"]


def test_queue_remove_does_not_become_idle_steering(tmp_path):
    src = tmp_path / "session.jsonl"
    events = [
        {"type": "queue-operation", "timestamp": "2026-01-01T00:00:00Z",
         "operation": "enqueue", "content": "cancel me"},
        {"type": "queue-operation", "timestamp": "2026-01-01T00:00:01Z",
         "operation": "remove", "content": "cancel me"},
        {"type": "user", "timestamp": "2026-01-01T00:00:02Z", "promptId": "p1",
         "message": {"content": "normal prompt"}},
        {"type": "assistant", "timestamp": "2026-01-01T00:00:03Z", "message": {
            "model": "claude-test", "usage": {"input_tokens": 1, "output_tokens": 1},
            "content": [{"type": "text", "text": "ok"}],
        }},
    ]
    rec = _record()
    rec["transcript"] = str(_write_jsonl(src, events))

    doc = A.compute_session_analytics(rec)

    assert doc["totals"]["steering"] == {"enqueued": 1, "boundary": 0, "idle": 0}
    assert doc["prompts"][0]["steering"] == {"enqueued": 0, "boundary": 0, "idle": 0}


def test_same_message_id_model_change_is_attributed_once_to_latest_model(
    tmp_path,
):
    src = tmp_path / "session.jsonl"
    events = [
        {
            "type": "user",
            "promptId": "p1",
            "message": {"content": "go"},
        },
        {
            "type": "assistant",
            "message": {
                "id": "msg-model-switch",
                "model": "claude-a",
                "usage": {
                    "input_tokens": 10,
                    "output_tokens": 2,
                    "cache_read_input_tokens": 100,
                },
                "content": [{"type": "thinking", "thinking": "hidden"}],
            },
        },
        {
            "type": "assistant",
            "message": {
                "id": "msg-model-switch",
                "model": "claude-b",
                # Later snapshot omits input/cache fields; they remain known
                # facts from the earlier content-block record.
                "usage": {"output_tokens": 3},
                "content": [{"type": "text", "text": "done"}],
            },
        },
    ]
    rec = _record()
    rec["transcript"] = str(_write_jsonl(src, events))

    doc = A.compute_session_analytics(rec)

    expected = {
        "input": 10,
        "output": 3,
        "cacheRead": 100,
        "cacheCreation": 0,
    }
    assert doc["totals"]["tokens"] == expected
    assert set(doc["totals"]["byModel"]) == {"claude-b"}
    assert doc["totals"]["byModel"]["claude-b"] == {
        "events": 1,
        "tokens": expected,
        "level": "main",
    }


def test_reused_message_id_keeps_first_prompt_attribution(tmp_path):
    src = tmp_path / "session.jsonl"
    events = [
        {
            "type": "user",
            "promptId": "p1",
            "message": {"content": "first"},
        },
        {
            "type": "assistant",
            "message": {
                "id": "msg-reused",
                "model": "claude-test",
                "usage": {"input_tokens": 10, "output_tokens": 2},
                "content": [{"type": "text", "text": "first answer"}],
            },
        },
        {
            "type": "user",
            "promptId": "p2",
            "message": {"content": "second"},
        },
        {
            "type": "assistant",
            "message": {
                "id": "msg-reused",
                "model": "claude-test",
                "usage": {"input_tokens": 10, "output_tokens": 2},
                "content": [{"type": "text", "text": "duplicate record"}],
            },
        },
    ]
    rec = _record()
    rec["transcript"] = str(_write_jsonl(src, events))

    doc = A.compute_session_analytics(rec)

    by_prompt = {item["promptId"]: item for item in doc["prompts"]}
    assert by_prompt["p1"]["tokens"]["input"] == 10
    assert by_prompt["p1"]["tokens"]["output"] == 2
    assert by_prompt["p2"]["tokens"] == {
        "input": 0,
        "output": 0,
        "cacheRead": 0,
        "cacheCreation": 0,
    }
    assert doc["totals"]["byModel"]["claude-test"]["events"] == 1


def test_subagent_message_model_change_and_partial_usage_reduce_once(
    tmp_path,
):
    sub = tmp_path / "subagent.jsonl"
    _write_jsonl(
        sub,
        [
            {
                "type": "assistant",
                "message": {
                    "id": "sub-switch",
                    "model": "claude-a",
                    "usage": {
                        "input_tokens": 7,
                        "output_tokens": 2,
                        "cache_creation_input_tokens": 4,
                    },
                    "content": [{"type": "thinking", "thinking": "hidden"}],
                },
            },
            {
                "type": "assistant",
                "message": {
                    "id": "sub-switch",
                    "model": "claude-b",
                    "usage": {"output_tokens": 5},
                    "content": [{"type": "text", "text": "done"}],
                },
            },
        ],
    )
    stats = {}

    rollup = A._read_subagent_rollup(sub, stats)

    assert rollup["tokens"] == {
        "input": 7,
        "output": 5,
        "cacheRead": 0,
        "cacheCreation": 4,
    }
    assert set(rollup["byModel"]) == {"claude-b"}
    assert rollup["byModel"]["claude-b"]["events"] == 1


def test_subagent_model_event_count_preserves_multiple_messages(
    tmp_path, monkeypatch
):
    root = tmp_path / "work"
    root.mkdir()
    transcript = tmp_path / "main.jsonl"
    _write_jsonl(
        transcript,
        [{
            "type": "user",
            "promptId": "p1",
            "message": {"content": "go"},
        }],
    )
    sub = tmp_path / "sub.jsonl"
    _write_jsonl(
        sub,
        [
            {
                "type": "assistant",
                "promptId": "p1",
                "message": {
                    "id": "s1",
                    "model": "claude-sub",
                    "usage": {"input_tokens": 2, "output_tokens": 1},
                    "content": [{"type": "text", "text": "one"}],
                },
            },
            {
                "type": "assistant",
                "promptId": "p1",
                "message": {
                    "id": "s2",
                    "model": "claude-sub",
                    "usage": {"input_tokens": 3, "output_tokens": 1},
                    "content": [{"type": "text", "text": "two"}],
                },
            },
        ],
    )
    rec = _record(cwd=str(root))
    rec["transcript"] = str(transcript)
    monkeypatch.setattr(
        A, "iter_subagent_transcripts", lambda cwd, session_id: [sub]
    )

    doc = A.compute_session_analytics(rec)

    assert doc["totals"]["byModel"]["claude-sub"]["events"] == 2
    assert doc["prompts"][0]["byModel"]["claude-sub"]["events"] == 2


def test_repeated_same_tool_block_id_counts_once_per_message(tmp_path):
    src = tmp_path / "session.jsonl"
    tool = {
        "type": "tool_use",
        "id": "tool-same",
        "name": "Bash",
        "input": {"command": "true"},
    }
    events = [
        {
            "type": "user",
            "promptId": "p1",
            "message": {"content": "go"},
        },
        {
            "type": "assistant",
            "message": {
                "id": "msg-tools",
                "model": "claude-test",
                "usage": {"input_tokens": 10, "output_tokens": 2},
                "content": [tool],
            },
        },
        {
            "type": "assistant",
            "message": {
                "id": "msg-tools",
                "model": "claude-test",
                "usage": {"input_tokens": 10, "output_tokens": 2},
                "content": [tool],
            },
        },
    ]
    rec = _record()
    rec["transcript"] = str(_write_jsonl(src, events))

    doc = A.compute_session_analytics(rec)

    assert doc["totals"]["tools"] == {"Bash": 1}
    assert doc["prompts"][0]["tools"] == {"Bash": 1}


def test_idless_repeated_tool_blocks_remain_individually_countable(tmp_path):
    src = tmp_path / "session.jsonl"
    tool = {
        "type": "tool_use",
        "id": "tool-same",
        "name": "Bash",
        "input": {"command": "true"},
    }
    events = [
        {
            "type": "user",
            "promptId": "p1",
            "message": {"content": "go"},
        },
        {
            "type": "assistant",
            "message": {
                "model": "claude-test",
                "usage": {"input_tokens": 1, "output_tokens": 1},
                "content": [tool],
            },
        },
        {
            "type": "assistant",
            "message": {
                "model": "claude-test",
                "usage": {"input_tokens": 1, "output_tokens": 1},
                "content": [tool],
            },
        },
    ]
    rec = _record()
    rec["transcript"] = str(_write_jsonl(src, events))

    doc = A.compute_session_analytics(rec)

    assert doc["totals"]["tools"] == {"Bash": 2}
    assert doc["totals"]["byModel"]["claude-test"]["events"] == 2
