from __future__ import annotations

from copy import deepcopy
import json

import pytest

from ccx.claude import telemetry as T
from ccx.errors import InvalidRequest


def _record(*, state: str = "idle", session_id: str = "session-1") -> dict:
    return {
        "short": "abc12345",
        "sessionId": session_id,
        "name": "claude-worker",
        "lifecycle": {"state": state},
    }


def _prompt(
    prompt_id: str,
    index: int,
    *,
    input_tokens: int = 10,
    output_tokens: int = 2,
) -> dict:
    return {
        "promptId": prompt_id,
        "turnIndex": index,
        "tsMs": 1_700_000_000_000 + index * 1000,
        "durationMs": 500,
        "tokens": {
            "input": input_tokens,
            "output": output_tokens,
            "cacheRead": 3,
            "cacheCreation": 4,
        },
        "byModel": {
            "claude-test": {
                "events": 1,
                "tokens": {
                    "input": input_tokens,
                    "output": output_tokens,
                    "cacheRead": 3,
                    "cacheCreation": 4,
                },
            }
        },
        "tools": {"Read": 1},
        "toolErrors": 0,
        "askUserQuestions": 0,
        "steering": {"enqueued": 0, "boundary": 0, "idle": 0},
        "subagents": {"count": 0, "tokens": {}, "byAgent": {}},
    }


def _doc(prompts: list[dict]) -> dict:
    return {
        "analyticsSchemaVersion": T.ANALYTICS_SCHEMA_VERSION,
        "prompts": prompts,
        "totals": {"turns": len(prompts)},
        "stats": {"malformedLines": 0, "subagentFilesUnreadable": 0},
    }


def test_claude_telemetry_pages_analytics_prompt_segments(monkeypatch):
    doc = _doc([_prompt("p1", 1), _prompt("p2", 2), _prompt("p3", 3)])
    monkeypatch.setattr(T, "compute_session_analytics", lambda rec: deepcopy(doc))

    first = T.read_telemetry("abc12345", record=_record(), limit=2)
    assert first["telemetry_kind"] == "claude-analytics-v3"
    assert [item["telemetry_id"] for item in first["turns"]] == [
        "claude-prompt:p1",
        "claude-prompt:p2",
    ]
    assert first["turns"][0]["turn_id"] is None
    assert first["turns"][0]["tokens"] == {
        "input": 10,
        "output": 2,
        "cached_input": 3,
        "cache_creation": 4,
        "reasoning_output": None,
    }
    assert first["coverage"]["identity"] == "analytics-prompt"
    assert first["coverage"]["status"] == "complete"
    assert first["has_more"] is True

    second = T.read_telemetry(
        "abc12345",
        record=_record(),
        after_cursor=first["next_cursor"],
        limit=2,
    )
    assert [item["telemetry_id"] for item in second["turns"]] == [
        "claude-prompt:p3"
    ]
    assert second["has_more"] is False


def test_claude_telemetry_cursor_advances_on_append_without_replay(monkeypatch):
    current = _doc([_prompt("p1", 1)])
    monkeypatch.setattr(
        T, "compute_session_analytics", lambda rec: deepcopy(current)
    )
    first = T.read_telemetry("abc12345", record=_record())

    current["prompts"].append(_prompt("p2", 2))
    current["totals"]["turns"] = 2
    second = T.read_telemetry(
        "abc12345",
        record=_record(),
        after_cursor=first["next_cursor"],
    )
    assert [item["prompt_id"] for item in second["turns"]] == ["p2"]
    assert second["coverage"]["gap"] is None


def test_claude_telemetry_cursor_resyncs_when_old_prompt_is_enriched(monkeypatch):
    current = _doc([_prompt("p1", 1, input_tokens=10)])
    monkeypatch.setattr(
        T, "compute_session_analytics", lambda rec: deepcopy(current)
    )
    first = T.read_telemetry("abc12345", record=_record())

    current["prompts"][0] = _prompt("p1", 1, input_tokens=11)
    current["prompts"].append(_prompt("p2", 2))
    current["totals"]["turns"] = 2
    second = T.read_telemetry(
        "abc12345",
        record=_record(),
        after_cursor=first["next_cursor"],
    )

    assert [item["prompt_id"] for item in second["turns"]] == ["p1", "p2"]
    assert second["coverage"]["status"] == "partial"
    assert (
        second["coverage"]["gap"]["reason"]
        == "cursor_prompt_prefix_changed"
    )


def test_claude_telemetry_cursor_is_bound_to_native_session(monkeypatch):
    doc = _doc([_prompt("p1", 1)])
    monkeypatch.setattr(T, "compute_session_analytics", lambda rec: deepcopy(doc))
    first = T.read_telemetry(
        "abc12345", record=_record(session_id="session-a")
    )

    second = T.read_telemetry(
        "abc12345",
        record=_record(session_id="session-b"),
        after_cursor=first["next_cursor"],
    )
    assert second["coverage"]["gap"]["reason"] == "cursor_session_mismatch"
    assert [item["prompt_id"] for item in second["turns"]] == ["p1"]


def test_claude_telemetry_busy_snapshot_is_explicitly_partial(monkeypatch):
    doc = _doc([_prompt("p1", 1)])
    monkeypatch.setattr(T, "compute_session_analytics", lambda rec: deepcopy(doc))

    result = T.read_telemetry("abc12345", record=_record(state="busy"))
    assert result["coverage"]["status"] == "partial"
    assert any(
        "not settled" in warning for warning in result["coverage"]["warnings"]
    )


def test_claude_telemetry_rejects_malformed_cursor(monkeypatch):
    monkeypatch.setattr(
        T,
        "compute_session_analytics",
        lambda rec: _doc([_prompt("p1", 1)]),
    )
    with pytest.raises(InvalidRequest, match="cursor"):
        T.read_telemetry(
            "abc12345", record=_record(), after_cursor="not-base64-json"
        )


def test_claude_telemetry_uses_real_analytics_reducer_without_leaking_source(
    tmp_path,
):
    transcript = tmp_path / "session.jsonl"
    prompt_text = "private prompt text should not be telemetry output"
    transcript.write_text(
        "\n".join(
            json.dumps(item)
            for item in [
                {
                    "type": "user",
                    "timestamp": "2026-01-01T00:00:00Z",
                    "promptId": "real-p1",
                    "message": {"content": prompt_text},
                },
                {
                    "type": "assistant",
                    "timestamp": "2026-01-01T00:00:01Z",
                    "message": {
                        "id": "msg-1",
                        "model": "claude-test",
                        "usage": {
                            "input_tokens": 9,
                            "output_tokens": 3,
                            "cache_read_input_tokens": 2,
                        },
                        "content": [{"type": "text", "text": "done"}],
                    },
                },
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    rec = _record()
    rec["cwd"] = str(tmp_path)
    rec["transcript"] = str(transcript)

    result = T.read_telemetry("abc12345", record=rec)
    assert result["turns"][0]["prompt_id"] == "real-p1"
    assert result["turns"][0]["tokens"]["input"] == 9
    serialized = json.dumps(result)
    assert prompt_text not in serialized
    assert str(transcript) not in serialized
