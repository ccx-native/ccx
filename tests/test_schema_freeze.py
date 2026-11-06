"""W5: frozen public data shapes.

These are the golden-shape tests external orchestrators rely on: the exact key sets of every public `as_dict()` projection, the
rendered transcript document, and the event envelope + tool-telemetry event
shapes. A failure here means a BREAKING schema change — either fix the change
or bump the relevant schema version and update the downstream consumers and
`docs/python/` deliberately.

Everything asserted here must round-trip through ``json.dumps`` unchanged.
"""

from __future__ import annotations

import json

import ccx.claude.host.events as H
from ccx import types as T
from ccx.constants import EVENT_SCHEMA_VERSION
from ccx.claude.transcript import TRANSCRIPT_SCHEMA_VERSION, render_transcript


def _json_stable(d):
    assert json.loads(json.dumps(d)) == d
    return d


# --- public dataclass projections ---------------------------------------------------

def test_session_info_shape():
    d = _json_stable(T.SessionInfo(short="s").as_dict())
    assert sorted(d) == sorted([
        "short", "worker_id", "session_id", "name", "cwd", "state", "transcript",
        "model", "effort", "last_turn_at",
        "created_at", "turns",
    ])


_CONTEXT_BLOCK = ["tokens_used", "window_size", "percent_used", "as_of_turn_ref", "as_of_ms", "reason"]


def test_claude_context_shape():
    d = _json_stable(T.ClaudeContext(short="s").as_dict())
    assert sorted(d) == sorted([
        "engine", "short", "worker_id", "name", "cwd", "state",
        *_CONTEXT_BLOCK,
    ])
    assert "worker" not in d
    owner = "aaaaaaaa-1234-4000-8000-000000000001"
    bound = _json_stable(T.ClaudeContext(short=owner[:8], worker_id=owner).as_dict())
    assert set(bound) == {*d, "worker"}
    assert bound["worker"] == f"claude:{owner}"


def test_codex_context_shape_shares_the_context_block():
    from ccx.codex.types import CodexContext, CodexSessionInfo
    identity = set(CodexSessionInfo(short="s").as_dict())
    d = _json_stable(CodexContext(short="s").as_dict())
    assert set(d) - identity == set(_CONTEXT_BLOCK)
    assert "worker" not in d
    owner = "aaaaaaaa-1234-4000-8000-000000000001"
    bound = _json_stable(CodexContext(short=owner[:8], worker_id=owner).as_dict())
    assert set(bound) == {*d, "worker"}
    assert bound["worker"] == f"codex:{owner}"


def test_session_status_shape_minimal_and_full():
    d = _json_stable(T.SessionStatus(short="s").as_dict())
    # Optional keys (mode/needs/error/context) are DROPPED when absent.
    assert sorted(d) == sorted([
        "short", "state", "turn", "last_turn_age_s", "host_status",
        "waiting_for",
    ])
    wf = {"type": "permission", "id": "r1", "tool": "AskUserQuestion",
          "input": {"questions": [{"question": "Q?", "options": [{"label": "a"}]}]},
          "requested_at": 1, "message": None}
    full = T.SessionStatus.from_projection({
        "short": "s", "state": "waiting", "turn": 3, "last_turn_age_s": 5,
        "host_status": "ok", "waiting_for": wf,
        "mode": "managed", "needs": "x", "context": {"pct": 10}, "error": None,
    })
    d2 = _json_stable(full.as_dict())
    # waiting_for projects the RAW host preview (CLI-compatible), not the
    # typed PendingInput field names.
    assert d2["waiting_for"] == wf


def test_report_shape():
    d = _json_stable(T.Report(short="s", turn=1, text="t").as_dict())
    assert sorted(d) == sorted([
        "short", "turn", "text", "transcript", "context",
        "background_tasks", "background_task_count",
        "open_background_task_count", "active_background_task_count",
        "stale_background_task_count", "running_background_task_count",
        "background_tasks_truncated", "background_task_limit",
        "background_task_hint", "worker_id", "engine", "turn_ref", "delivery_id", "note",
    ])
    assert "worker" not in d
    owner = "aaaaaaaa-1234-4000-8000-000000000001"
    bound = _json_stable(T.Report(
        short=owner[:8], worker_id=owner, turn=1, text="t",
    ).as_dict())
    assert set(bound) == {*d, "worker"}
    assert bound["worker"] == f"claude:{owner}"


def test_send_result_shape():
    d = _json_stable(T.SendResult(short="s").as_dict())
    assert sorted(d) == sorted([
        "short", "reply", "transcript", "state", "queued",
        "worker_id", "engine", "delivery_id", "delivery_status", "settled", "turn_ref",
    ])
    assert "worker" not in d
    owner = "aaaaaaaa-1234-4000-8000-000000000001"
    bound = _json_stable(T.SendResult(short=owner[:8], worker_id=owner).as_dict())
    assert set(bound) == {*d, "worker"}
    assert bound["worker"] == f"claude:{owner}"


def test_compact_result_shape():
    d = _json_stable(T.CompactResult(short="s").as_dict())
    assert sorted(d) == sorted([
        "short", "state", "compacted", "message", "pre_tokens", "post_tokens",
        "duration_ms",
    ])


def test_compact_operation_result_extends_legacy_shape_additively():
    detached = _json_stable(T.CompactOperationResult(short="s").as_dict())
    assert sorted(detached) == sorted([
        "short", "state", "compacted", "message", "pre_tokens", "post_tokens",
        "duration_ms", "operation_id", "operation_status", "evidence_uuid",
        "reason", "worker_id",
    ])
    assert "worker" not in detached
    owner = "aaaaaaaa-1234-4000-8000-000000000001"
    bound = _json_stable(T.CompactOperationResult(
        short=owner[:8], worker_id=owner,
    ).as_dict())
    assert set(bound) == {*detached, "worker"}
    assert bound["worker"] == f"claude:{owner}"
    assert isinstance(T.CompactOperationResult(short="s"), T.CompactResult)


def test_wait_result_shape():
    d = _json_stable(T.WaitResult(status="idle").as_dict())
    # waiting_for is omitted when absent (additive-only when present).
    assert sorted(d) == sorted([
        "status", "mode", "state", "reason", "wait_kind",
        "wait_mode", "next_turn_observed", "next_turn_event",
        # Additive: managed exact latest settlement (the next after_turn
        # checkpoint) and the host's active turn ref; None when unknown.
        "last_settled_turn", "active_turn_ref",
        "background_tasks", "background_task_count",
        "open_background_task_count", "active_background_task_count",
        "stale_background_task_count", "running_background_task_count",
        "background_tasks_truncated", "background_task_limit",
        "background_task_hint",
        # Additive: Claude's API-error classification, None unless the session
        # stopped on one. Present as a key even when None so a consumer can
        # branch on it without a getattr dance.
        "api_error",
    ])
    assert T.WaitResult(status="idle").api_error is None


def test_event_as_dict_is_raw_passthrough():
    raw = {"type": "tool_started", "seq": 2, "short": "s", "epoch": "e1",
           "ts": 1.5, "v": 1, "tool": "Bash", "input_preview": "{}", "turn": 1}
    ev = T.Event.from_dict(raw)
    assert _json_stable(ev.as_dict()) == raw
    assert ev.cursor == ("e1", 2)


def test_pending_input_and_question_are_raw_passthrough():
    wf = {"type": "permission", "id": "r", "tool": "AskUserQuestion",
          "input": {"questions": [{"question": "Q?", "options": []}]},
          "requested_at": 1, "message": None}
    assert _json_stable(T.PendingInput.from_waiting_for(wf).as_dict()) == wf


def test_usage_shape():
    d = _json_stable(T.Usage(summary={"five_hour": {"utilization": 1}},
                             raw={"x": 1}).as_dict())
    assert sorted(d) == ["five_hour_reset", "raw", "summary"]


# --- transcript document -------------------------------------------------------------

def test_rendered_transcript_document_shape(tmp_path, monkeypatch):
    from test_transcript import _render_owned_source

    tp = tmp_path / "source.jsonl"
    tp.write_text("\n".join(json.dumps(e) for e in [
        {"type": "user", "message": {"content": "Hello"},
         "timestamp": "2026-01-01T00:00:00Z"},
        {"type": "assistant", "message": {
            "content": [{"type": "text", "text": "Hi"}],
            "usage": {"input_tokens": 3, "output_tokens": 2}},
         "timestamp": "2026-01-01T00:00:01Z"},
    ]))
    owner, _observation, _ = _render_owned_source(tmp_path, monkeypatch, tp)
    doc = render_transcript(owner)
    assert doc["schemaVersion"] == TRANSCRIPT_SCHEMA_VERSION == 2
    assert sorted(doc) == sorted([
        "schemaVersion", "generatedAt", "worker", "sessionId", "name", "cwd",
        "transcriptJsonl", "turns", "context", "stats",
    ])
    assert doc["worker"] == f"claude:{owner}"
    # The turn shape downstream parsers rely on: role + text.
    roles = [t.get("role") for t in doc["turns"]]
    assert "user" in roles and "assistant" in roles
    for t in doc["turns"]:
        assert "role" in t and "text" in t
    assert sorted(doc["stats"]) == ["malformedLines", "truncatedToolResults"]


# --- event envelope + tool telemetry --------------------------------------------------

def test_event_envelope_shape():
    stamped = H._stamp_event({"type": "state", "from": "idle", "to": "busy"},
                             short="s", seq=7, epoch="pid-123", ts=1.0)
    _json_stable(stamped)
    assert stamped["v"] == EVENT_SCHEMA_VERSION == 1
    for key in ("v", "type", "short", "seq", "epoch", "ts"):
        assert key in stamped
    # Event-specific fields pass through unchanged.
    assert stamped["from"] == "idle" and stamped["to"] == "busy"


def test_tool_started_event_shape():
    evt = H._tool_started_event(
        {"tool_name": "WebSearch", "tool_use_id": "tu1",
         "tool_input": {"query": "x"}}, turn=4)
    assert sorted(evt) == sorted([
        "type", "tool_use_id", "tool", "input_preview", "turn"])
    assert evt["type"] == "tool_started"
    assert evt["tool"] == "WebSearch"


def test_tool_finished_event_shape():
    evt = H._tool_finished_event(
        {"tool_name": "WebFetch", "tool_use_id": "tu2",
         "tool_response": {"ok": True}}, turn=4, duration_ms=120)
    assert sorted(evt) == sorted([
        "type", "tool_use_id", "tool", "duration_ms", "ok", "result_preview",
        "turn"])
    assert evt["ok"] is True


def test_subagent_finished_event_shape():
    evt = H._subagent_finished_event(
        {"agent_id": "a1", "agent_type": "Explore", "last_message": "done"},
        turn=9)
    assert sorted(evt) == sorted([
        "type", "agent_id", "agent_type", "last_message", "turn"])


def test_turn_complete_event_shape_is_frozen():
    evt = H._stamp_event(
        {
            "type": "turn_complete",
            "turn": 3,
            "turn_ref": "ccx-turn:claude:v1:abc",
            "interrupted": True,
        },
        short="s", seq=8, epoch="e1", ts=2.0,
    )
    assert sorted(evt) == sorted([
        "type", "turn", "turn_ref", "interrupted",
        "v", "short", "seq", "epoch", "ts",
    ])


def test_turn_settled_is_additive_event_shape():
    evt = H._stamp_event(
        {
            "type": "turn_settled",
            "turn": 3,
            "turn_ref": "ccx-turn:claude:v1:abc",
            "outcome": "unknown",
            "evidence": "native-session-inactive",
            "native_status": "idle",
        },
        short="s", seq=8, epoch="e1", ts=2.0,
    )
    assert evt["v"] == EVENT_SCHEMA_VERSION == 1
    assert sorted(evt) == sorted([
        "type", "turn", "turn_ref", "outcome", "evidence", "native_status",
        "v", "short", "seq", "epoch", "ts",
    ])


def test_neutral_compact_terminal_outcomes_are_additive_raw_shapes_unchanged():
    from ccx.ops.event_feed import _claude_project, _codex_project
    for raw_type, expected_outcome in (
        ("compact_complete", "completed"),
        ("compact_failed", "failed"),
        ("compact_unknown", "unknown"),
    ):
        raw = H._stamp_event(
            {"type": raw_type, "operation_id": "one-compact"},
            short="s", seq=9, epoch="host-epoch", ts=1.0,
        )
        assert "outcome" not in raw
        result = _json_stable(_claude_project(raw))
        assert set(result) == {*raw, "kind", "outcome"}
        assert result["type"] == raw_type
        assert result["kind"] == "compact.ended"
        assert result["outcome"] == expected_outcome

    completed_native = {
        "method": "item/completed", "sequence": 3, "observedAtMs": 2000,
        "turnId": "turn-1", "itemId": "compact-item",
        "params": {"item": {"id": "compact-item", "type": "contextCompaction"}},
    }
    original = json.loads(json.dumps(completed_native))
    # Native provider evidence remains unchanged. Only CCX-owned sparse
    # publications are neutral compaction events.
    assert _codex_project(completed_native) is None
    assert completed_native == original
    for method, expected_kind, outcome in (
        ("ccx/compact/started", "compact.started", None),
        ("ccx/compact/ended", "compact.ended", "completed"),
    ):
        synthetic = {
            "method": method, "sequence": 4, "observedAtMs": 2200,
            "turnId": "turn-1", "params": {
                "turnId": "turn-1", "compactId": "turn-1:compact-item",
                "itemId": "compact-item", "outcome": outcome,
            },
        }
        projected = _json_stable(_codex_project(synthetic))
        assert projected["type"] == method
        assert projected["kind"] == expected_kind
        assert projected["compact_id"] == "turn-1:compact-item"
        assert projected["turn_ref"] == "ccx-turn:codex:v1:turn-1"
        if outcome is None:
            assert "outcome" not in projected
        else:
            assert projected["outcome"] == outcome

    for native_status in ("failed", "unknown"):
        synthetic = {
            "method": "ccx/compact/ended", "sequence": 4, "observedAtMs": 2200,
            "turnId": "turn-2", "params": {
                "turnId": "turn-2", "compactId": "operation:compact-2",
                "operationId": "compact-2", "outcome": native_status,
            },
        }
        projection = _json_stable(_codex_project(synthetic))
        assert projection["type"] == "ccx/compact/ended"
        assert projection["kind"] == "compact.ended"
        assert projection["outcome"] == native_status
        assert projection["operation_id"] == "compact-2"
        assert projection["turn_ref"] == "ccx-turn:codex:v1:turn-2"
