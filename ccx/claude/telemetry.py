"""Cursor-based Protocol V1 projection over existing Claude analytics.

Claude already has durable/recomputable per-prompt analytics. This adapter exposes
that information without pretending those prompt segments are the managed host's
exact turn identities: local commands and other user-like transcript records can
exist outside a managed model turn.

The cursor is bound to worker + native Claude session and to a digest of the
prompt prefix the caller has acknowledged. Normal append-only growth advances
cheaply. If later transcript/subagent evidence enriches an already-returned
prompt, the prefix digest changes and the reader explicitly resynchronizes
instead of silently skipping the correction.
"""

from __future__ import annotations

import base64
import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from ccx.claude.analytics import ANALYTICS_SCHEMA_VERSION, compute_session_analytics
from ccx.errors import InvalidRequest, SessionNotFound, SessionUnavailable
from ccx.store.records import read_record

_CURSOR_VERSION = 1
_MAX_LIMIT = 1000


def _opaque_cursor(payload: dict[str, Any]) -> str:
    raw = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _parse_cursor(value: str | None) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise InvalidRequest("telemetry cursor must be a non-empty opaque string")
    try:
        raw = value + "=" * (-len(value) % 4)
        payload = json.loads(base64.urlsafe_b64decode(raw.encode("ascii")))
    except Exception as error:
        raise InvalidRequest("telemetry cursor is invalid") from error
    if not isinstance(payload, dict) or payload.get("v") != _CURSOR_VERSION:
        raise InvalidRequest("telemetry cursor is invalid")
    count = payload.get("count")
    if not isinstance(count, int) or isinstance(count, bool) or count < 0:
        raise InvalidRequest("telemetry cursor is invalid")
    for key in ("short", "session_id", "prefix_sha256"):
        item = payload.get(key)
        if not isinstance(item, str) or not item:
            raise InvalidRequest("telemetry cursor is invalid")
    schema = payload.get("analytics_schema_version")
    if not isinstance(schema, int) or isinstance(schema, bool) or schema <= 0:
        raise InvalidRequest("telemetry cursor is invalid")
    return payload


def _prompt_prefix_digest(prompts: list[dict[str, Any]], count: int) -> str:
    raw = json.dumps(
        prompts[:count],
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _cursor(
    *,
    short: str,
    session_id: str,
    prompts: list[dict[str, Any]],
    count: int,
) -> str:
    return _opaque_cursor(
        {
            "v": _CURSOR_VERSION,
            "short": short,
            "session_id": session_id,
            "analytics_schema_version": ANALYTICS_SCHEMA_VERSION,
            "count": count,
            "prefix_sha256": _prompt_prefix_digest(prompts, count),
        }
    )


def _iso_ms(value: Any) -> str | None:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return None
    try:
        return (
            datetime.fromtimestamp(float(value) / 1000.0, tz=timezone.utc)
            .isoformat()
            .replace("+00:00", "Z")
        )
    except (OverflowError, OSError, ValueError):
        return None


def _turn_entry(prompt: dict[str, Any]) -> dict[str, Any]:
    prompt_id = str(prompt.get("promptId") or f"turn-{prompt.get('turnIndex') or 0}")
    tokens = prompt.get("tokens") if isinstance(prompt.get("tokens"), dict) else {}
    started_ms = prompt.get("tsMs")
    duration_ms = prompt.get("durationMs")
    activity_end_ms = (
        int(started_ms) + int(duration_ms)
        if isinstance(started_ms, int)
        and not isinstance(started_ms, bool)
        and isinstance(duration_ms, int)
        and not isinstance(duration_ms, bool)
        and duration_ms >= 0
        else None
    )
    return {
        # Stable telemetry identity. Deliberately not exposed as turn_id/ref:
        # analytics prompt identity and managed-host turn identity are different.
        "telemetry_id": f"claude-prompt:{prompt_id}",
        "turn_id": None,
        "turn_ref": None,
        "prompt_id": prompt_id,
        "turn_index": prompt.get("turnIndex"),
        "kind": "analytics-prompt",
        "started_at": _iso_ms(started_ms),
        "completed_at": None,
        "activity_ended_at": _iso_ms(activity_end_ms),
        "outcome": None,
        "tokens": {
            "input": int(tokens.get("input") or 0),
            "output": int(tokens.get("output") or 0),
            "cached_input": int(tokens.get("cacheRead") or 0),
            "cache_creation": int(tokens.get("cacheCreation") or 0),
            "reasoning_output": None,
        },
        "by_model": dict(prompt.get("byModel") or {}),
        "tools": dict(prompt.get("tools") or {}),
        "tool_errors": int(prompt.get("toolErrors") or 0),
        "ask_user_questions": int(prompt.get("askUserQuestions") or 0),
        "steering": dict(prompt.get("steering") or {}),
        "subagents": dict(prompt.get("subagents") or {}),
        "coverage": {
            "identity": "analytics-prompt",
            "instruction": "not-exported-by-analytics",
            "steers": "counts-only",
            "lifecycle": "not-correlated-to-managed-turn",
            "context_after": "unavailable",
        },
        "sources": [f"claude-analytics-v{ANALYTICS_SCHEMA_VERSION}"],
    }


def read_telemetry(
    short: str,
    *,
    after_cursor: str | None = None,
    limit: int = 200,
    record: dict[str, Any] | None = None,
    cursor_worker_id: str | None = None,
    before_offset: int | None = None,
    source_missing: bool = False,
) -> dict[str, Any]:
    if not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0:
        raise InvalidRequest("telemetry limit must be a positive integer")
    limit = min(limit, _MAX_LIMIT)

    rec = record or read_record(short)
    if not isinstance(rec, dict):
        raise SessionNotFound(f"no such Claude worker: {short}", short=short)
    short = str(rec.get("short") or short)
    session_id = rec.get("sessionId")
    if not isinstance(session_id, str) or not session_id:
        raise SessionUnavailable(
            f"Claude worker {short} has no durable session identity",
            short=short,
        )

    # Read-only recomputation; full-UUID callers supply a pinned original
    # source and its last complete line, never an analytics sidecar.
    doc = (compute_session_analytics(rec)
           if before_offset is None
           else compute_session_analytics(rec, before_offset=before_offset))
    cursor_owner = cursor_worker_id or short
    prompts = [
        value for value in (doc.get("prompts") or []) if isinstance(value, dict)
    ]
    parsed = _parse_cursor(after_cursor)
    start = 0
    gap = None
    if parsed is not None:
        reason = None
        if parsed.get("short") != cursor_owner:
            reason = "cursor_worker_mismatch"
        elif parsed.get("session_id") != session_id:
            reason = "cursor_session_mismatch"
        elif parsed.get("analytics_schema_version") != ANALYTICS_SCHEMA_VERSION:
            reason = "cursor_analytics_schema_mismatch"
        elif int(parsed["count"]) > len(prompts):
            reason = "cursor_ahead_of_prompt_history"
        elif parsed.get("prefix_sha256") != _prompt_prefix_digest(
            prompts, int(parsed["count"])
        ):
            reason = "cursor_prompt_prefix_changed"

        if reason is None:
            start = int(parsed["count"])
        else:
            gap = {
                "reason": reason,
                "requested_count": parsed.get("count"),
                "current_count": len(prompts),
            }

    page = prompts[start : start + limit]
    processed = start + len(page)
    has_more = processed < len(prompts)

    stats = doc.get("stats") if isinstance(doc.get("stats"), dict) else {}
    warnings: list[str] = []
    if source_missing:
        warnings.append("original Claude native source has not appeared")
    if int(stats.get("malformedLines") or 0):
        warnings.append("Claude analytics skipped malformed transcript lines")
    if int(stats.get("subagentFilesUnreadable") or 0):
        warnings.append("one or more Claude subagent transcripts were unreadable")
    if gap is not None:
        warnings.append(
            "requested telemetry cursor no longer matches the acknowledged "
            "Claude analytics prefix; response resynchronized from the start"
        )

    lifecycle = (
        rec.get("lifecycle") if isinstance(rec.get("lifecycle"), dict) else {}
    )
    state = lifecycle.get("state")
    if state in {"booting", "busy", "waiting", "resuming"}:
        warnings.append(
            "worker is not settled; the current analytics tail may be enriched "
            "and a later cursor read can request resynchronization"
        )

    totals = doc.get("totals") if isinstance(doc.get("totals"), dict) else {}
    return {
        "worker": {
            "engine": "claude",
            "short": short,
            "name": rec.get("name"),
            "session_id": session_id,
        },
        "telemetry_kind": f"claude-analytics-v{ANALYTICS_SCHEMA_VERSION}",
        "turns": [_turn_entry(value) for value in page],
        # Existing Claude analytics do not provide the Codex evidence-journal
        # compaction projection. Keep this explicit rather than fabricating it.
        "compactions": [],
        "totals": totals,
        "coverage": {
            "status": "partial" if warnings else "complete",
            "gap": gap,
            "warnings": warnings,
            "analytics_schema_version": ANALYTICS_SCHEMA_VERSION,
            "identity": "analytics-prompt",
            "native_turn_identity": "not-provided-by-source",
            "compactions": "not-provided-by-source",
            "worker_state": state,
        },
        "next_cursor": _cursor(
            short=cursor_owner,
            session_id=session_id,
            prompts=prompts,
            count=processed,
        ),
        "has_more": has_more,
    }


__all__ = ["read_telemetry"]
