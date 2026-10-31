"""Normalized replayable Codex telemetry over sparse CCX evidence."""

from __future__ import annotations

import base64
import json
from typing import Any

from ccx.codex import evidence as codex_evidence
from ccx.codex import runtime as codex_runtime
from ccx.codex.evidence import COMPACTION_CONTEXT_METHOD, TOKEN_USAGE_METHOD
from ccx.ops.conversation import build_codex
from ccx.errors import InvalidRequest

_CURSOR_VERSION = 2


def _cursor(
    short: str,
    journal_instance_id: str | None,
    sequence: int,
) -> str:
    raw = json.dumps(
        {
            "v": _CURSOR_VERSION,
            "short": short,
            "journal_instance_id": journal_instance_id,
            "sequence": sequence,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _parse_cursor(value: str | None) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise InvalidRequest("telemetry cursor must be a non-empty opaque string")
    try:
        raw = value + "=" * (-len(value) % 4)
        data = json.loads(base64.urlsafe_b64decode(raw.encode("ascii")))
    except Exception as error:
        raise InvalidRequest("telemetry cursor is invalid") from error
    if (
        not isinstance(data, dict)
        or not isinstance(data.get("sequence"), int)
        or isinstance(data.get("sequence"), bool)
        or data["sequence"] < 0
    ):
        raise InvalidRequest("telemetry cursor is invalid")
    version = data.get("v")
    if version == 1:
        return {
            "version": 1,
            "sequence": data["sequence"],
            "short": None,
            "journal_instance_id": None,
        }
    if version != _CURSOR_VERSION:
        raise InvalidRequest("telemetry cursor is invalid")
    short = data.get("short")
    instance = data.get("journal_instance_id")
    if not isinstance(short, str) or not short:
        raise InvalidRequest("telemetry cursor is invalid")
    if instance is not None and (
        not isinstance(instance, str) or not instance
    ):
        raise InvalidRequest("telemetry cursor is invalid")
    return {
        "version": _CURSOR_VERSION,
        "sequence": data["sequence"],
        "short": short,
        "journal_instance_id": instance,
    }


def _turn_id(event: dict[str, Any]) -> str | None:
    value = event.get("turnId")
    return value if isinstance(value, str) and value else None


def _item_id(event: dict[str, Any]) -> str | None:
    value = event.get("itemId")
    return value if isinstance(value, str) and value else None


def _usage_projection(event: dict[str, Any]) -> dict[str, Any]:
    params = event.get("params")
    params = params if isinstance(params, dict) else {}
    usage = params.get("tokenUsage")
    usage = usage if isinstance(usage, dict) else {}
    last = usage.get("last")
    last = last if isinstance(last, dict) else {}
    total = usage.get("total")
    total = total if isinstance(total, dict) else {}
    window = usage.get("modelContextWindow")
    input_tokens = last.get("inputTokens")
    zero_last = all(
        int(last.get(key) or 0) == 0
        for key in (
            "inputTokens",
            "cachedInputTokens",
            "outputTokens",
            "reasoningOutputTokens",
        )
    )
    percent = (
        input_tokens / window * 100
        if (
            isinstance(input_tokens, int)
            and isinstance(window, int)
            and window > 0
        )
        else None
    )
    return {
        "context_after": (
            {
                "tokens_used": input_tokens,
                "window_size": window if isinstance(window, int) else None,
                "percent_used": percent,
                "as_of_turn_ref": None,
                "observed_at_ms": event.get("observedAtMs"),
            }
            if isinstance(input_tokens, int)
            else None
        ),
        "cumulative": {
            "input": total.get("inputTokens")
            if isinstance(total.get("inputTokens"), int) else None,
            "output": total.get("outputTokens")
            if isinstance(total.get("outputTokens"), int) else None,
            "cached_input": total.get("cachedInputTokens")
            if isinstance(total.get("cachedInputTokens"), int) else None,
            "reasoning_output": total.get("reasoningOutputTokens")
            if isinstance(total.get("reasoningOutputTokens"), int) else None,
            "total": total.get("totalTokens")
            if isinstance(total.get("totalTokens"), int) else None,
        },
        "zero_last": zero_last,
        "reset": False,
        "sequence": event.get("sequence"),
        "observed_at_ms": event.get("observedAtMs"),
        "source": "codex-thread-token-usage-updated",
    }


def _zero_cumulative() -> dict[str, int]:
    return {
        "input": 0,
        "output": 0,
        "cached_input": 0,
        "reasoning_output": 0,
        "total": 0,
    }


def _turn_token_delta(
    end: dict[str, Any] | None,
    baseline: dict[str, Any] | None,
) -> dict[str, int] | None:
    if not isinstance(end, dict) or not isinstance(baseline, dict):
        return None
    out: dict[str, int] = {}
    for key in ("input", "output", "cached_input", "reasoning_output", "total"):
        newer = end.get(key)
        older = baseline.get(key)
        if not isinstance(newer, int) or not isinstance(older, int):
            return None
        delta = newer - older
        if delta < 0:
            return None
        out[key] = delta
    return out

def _user_text_items(turn: dict[str, Any]) -> list[dict[str, Any]]:
    items = [
        item
        for item in (turn.get("input") or [])
        if isinstance(item, dict) and item.get("kind") == "user-text"
    ]
    return sorted(
        items,
        key=lambda item: (
            item.get("ordinal")
            if isinstance(item.get("ordinal"), int)
            else 1 << 30
        ),
    )


def _instruction_and_steers(
    turn: dict[str, Any],
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    messages = _user_text_items(turn)
    if not messages:
        return None, []

    opening_index = 0
    for index, item in enumerate(messages):
        if item.get("origin") == "ccx-start":
            opening_index = index
            break
    opening = messages[opening_index]

    instruction = {
        "text": opening.get("text") if isinstance(opening.get("text"), str) else "",
        "source": opening.get("origin") or "native-user-message",
        "client_id": opening.get("clientId"),
        "ordinal": opening.get("ordinal"),
    }
    steers = []
    for index, item in enumerate(messages):
        if index == opening_index:
            continue
        steers.append({
            "text": item.get("text") if isinstance(item.get("text"), str) else "",
            "source": item.get("origin") or "native-user-message",
            "client_id": item.get("clientId"),
            "ordinal": item.get("ordinal"),
            "started_at_ms": item.get("startedAtMs"),
            "completed_at_ms": item.get("completedAtMs"),
        })
    return instruction, steers


def _compaction_id(
    *,
    turn_id: str | None,
    item_id: str | None,
    operation_id: str | None,
    sequence: int | None,
) -> str:
    if item_id:
        return f"item:{item_id}"
    if operation_id:
        return f"operation:{operation_id}"
    if turn_id:
        return f"turn:{turn_id}"
    return f"sequence:{sequence or 0}"


def _compactions(
    events: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[int, str]]:
    by_id: dict[str, dict[str, Any]] = {}
    sequence_to_id: dict[int, str] = {}

    for event in events:
        if event.get("method") != "item/completed":
            continue
        params = event.get("params")
        params = params if isinstance(params, dict) else {}
        item = params.get("item")
        item = item if isinstance(item, dict) else {}
        if item.get("type") != "contextCompaction":
            continue
        turn_id = _turn_id(event)
        item_id = _item_id(event) or (
            item.get("id") if isinstance(item.get("id"), str) else None
        )
        sequence = event.get("sequence")
        cid = _compaction_id(
            turn_id=turn_id,
            item_id=item_id,
            operation_id=None,
            sequence=sequence if isinstance(sequence, int) else None,
        )
        by_id[cid] = {
            "compaction_id": cid,
            "turn_id": turn_id,
            "item_id": item_id,
            "operation_id": None,
            "kind": "native",
            "completed_at_ms": (
                params.get("completedAtMs")
                if isinstance(params.get("completedAtMs"), int)
                else event.get("observedAtMs")
            ),
            "context_before": None,
            "context_after": None,
            "coverage": {
                "context_before": "unavailable",
                "context_after": "unavailable",
            },
            "sources": ["codex-context-compaction-item"],
        }
        if isinstance(sequence, int):
            sequence_to_id[sequence] = cid

    for event in events:
        if event.get("method") != COMPACTION_CONTEXT_METHOD:
            continue
        params = event.get("params")
        params = params if isinstance(params, dict) else {}
        item = params.get("item")
        item = item if isinstance(item, dict) else {}
        turn_id = _turn_id(event)
        item_id = _item_id(event) or (
            item.get("id") if isinstance(item.get("id"), str) else None
        )
        operation_id = (
            params.get("operationId")
            if isinstance(params.get("operationId"), str)
            else None
        )
        sequence = event.get("sequence")
        cid = _compaction_id(
            turn_id=turn_id,
            item_id=item_id,
            operation_id=operation_id,
            sequence=sequence if isinstance(sequence, int) else None,
        )
        entry = by_id.setdefault(cid, {
            "compaction_id": cid,
            "turn_id": turn_id,
            "item_id": item_id,
            "operation_id": operation_id,
            "kind": "explicit" if operation_id else "native",
            "completed_at_ms": params.get("completedAtMs"),
            "context_before": None,
            "context_after": None,
            "coverage": {},
            "sources": [],
        })
        entry["operation_id"] = operation_id or entry.get("operation_id")
        if operation_id:
            entry["kind"] = "explicit"
        entry["context_before"] = params.get("contextBefore")
        entry["context_after"] = params.get("contextAfter")
        entry["coverage"] = {
            "context_before": (
                "observed" if params.get("contextBefore") is not None else "unavailable"
            ),
            "context_after": (
                "observed"
                if params.get("contextAfter") is not None
                else "pending-next-turn"
            ),
        }
        if "ccx-compaction-context" not in entry["sources"]:
            entry["sources"].append("ccx-compaction-context")
        if isinstance(sequence, int):
            sequence_to_id[sequence] = cid

    return list(by_id.values()), sequence_to_id


def _original_source(store, owner):
    """Original owned Codex record, thread identity and filtered evidence.

    Shared with full conversation: evidence cannot attribute an event with a
    missing/foreign native threadId to this worker's original thread.
    """
    from ccx.store.root import NativeActionDenied

    source = store.record(owner.worker)
    native = source.get("native") or {}
    activation = store.native_activation(owner.worker)
    thread_id = native.get("threadId")
    expected_source = store.codex_source(owner.worker)
    if (not isinstance(thread_id, str) or not thread_id
            or native.get("threadSource") != expected_source
            or activation.get("state") != "confirmed"
            or activation.get("source") != expected_source
            or (activation.get("nativeBinding") or {}).get("threadId") != thread_id):
        raise NativeActionDenied("Codex telemetry original thread/source unconfirmed")
    record = {**source, "short": owner.short, "threadId": thread_id}
    journal = codex_evidence.read_journal(owner.worker)
    # The owned nofollow journal inode does not attest each native event's
    # threadId; header and CCX-only feed events are not native turn evidence.
    foreign_events = [
        event for event in journal.events
        if event.get("method") not in (
            codex_evidence.HEADER_METHOD, *codex_evidence.FEED_METHODS,
        ) and isinstance(event.get("threadId"), str)
        and event["threadId"] and event["threadId"] != thread_id
    ]
    journal.events = [
        event for event in journal.events
        if event.get("method") in (
            codex_evidence.HEADER_METHOD, *codex_evidence.FEED_METHODS,
        ) or event.get("threadId") == thread_id
    ]
    evidence_index = codex_evidence.build_index(
        journal,
        health=(record.get("evidence") if isinstance(record.get("evidence"), dict)
                else None),
    )
    return record, thread_id, journal, foreign_events, evidence_index


def read_telemetry(
    target: str,
    *,
    after_cursor: str | None = None,
    limit: int = 200,
    timeout: float = 120,
) -> dict[str, Any]:
    """Return normalized per-turn telemetry and replay cursor for one worker."""
    if not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0:
        raise InvalidRequest("telemetry limit must be a positive integer")
    if not isinstance(timeout, (int, float)) or isinstance(timeout, bool) or timeout <= 0:
        raise InvalidRequest("telemetry timeout must be greater than zero")
    limit = min(limit, 1000)

    from ccx import constants
    from ccx.store.root import require_ccx_active

    store = require_ccx_active(constants.CCX_ROOT)
    owner = store.resolve(target)
    if owner.engine != "codex":
        raise InvalidRequest("Codex telemetry requires an exact Codex full UUID")
    record, thread_id, journal, foreign_events, evidence_index = _original_source(
        store, owner,
    )
    short = owner.short
    cursor_owner = owner.worker_id
    parsed_cursor = _parse_cursor(after_cursor)
    after_sequence = (
        parsed_cursor.get("sequence")
        if isinstance(parsed_cursor, dict)
        else None
    )
    cursor_mismatch = None
    if isinstance(parsed_cursor, dict):
        if parsed_cursor.get("version") != _CURSOR_VERSION:
            cursor_mismatch = "legacy_cursor"
        elif parsed_cursor.get("short") != cursor_owner:
            cursor_mismatch = "cursor_worker_mismatch"
        elif (
            parsed_cursor.get("journal_instance_id")
            != journal.journal_instance_id
        ):
            cursor_mismatch = "cursor_journal_mismatch"
    cursor_ahead = (
        cursor_mismatch is None
        and after_sequence is not None
        and after_sequence > journal.last_sequence
    )
    effective_after_sequence = (
        None if cursor_mismatch is not None or cursor_ahead else after_sequence
    )

    events = journal.events
    delta_events = [
        event
        for event in events
        if (
            isinstance(event.get("sequence"), int)
            and (
                effective_after_sequence is None
                or event["sequence"] > effective_after_sequence
            )
        )
    ]
    has_more = len(delta_events) > limit
    processed_delta = delta_events[:limit]
    if processed_delta:
        processed_through = int(processed_delta[-1]["sequence"])
    elif effective_after_sequence is not None:
        processed_through = effective_after_sequence
    else:
        processed_through = journal.last_sequence
    if not has_more:
        processed_through = journal.last_sequence

    event_sequences = [
        event.get("sequence")
        for event in events
        if isinstance(event.get("sequence"), int)
    ]
    first_sequence = min(event_sequences) if event_sequences else None
    gap = None
    if cursor_mismatch is not None:
        gap = {
            "reason": cursor_mismatch,
            "requested_after_sequence": after_sequence,
            "first_retained_sequence": first_sequence,
            "last_retained_sequence": journal.last_sequence,
            "requested_worker": (
                parsed_cursor.get("short")
                if isinstance(parsed_cursor, dict) else None
            ),
            "current_worker": cursor_owner,
            "requested_journal_instance_id": (
                parsed_cursor.get("journal_instance_id")
                if isinstance(parsed_cursor, dict) else None
            ),
            "current_journal_instance_id": journal.journal_instance_id,
        }
    elif cursor_ahead:
        gap = {
            "reason": "cursor_ahead_of_retained_tail",
            "requested_after_sequence": after_sequence,
            "last_retained_sequence": journal.last_sequence,
            "first_retained_sequence": first_sequence,
        }
    elif (
        after_sequence is not None
        and first_sequence is not None
        and after_sequence < first_sequence - 1
    ):
        gap = {
            "reason": "cursor_predates_retained_evidence",
            "requested_after_sequence": after_sequence,
            "first_retained_sequence": first_sequence,
        }

    # Only an explicit successful native thread/start proves history is empty.
    # Legacy records can have native history even when their turn counter is 0.
    if record.get("historyKnownEmpty") is True:
        conversation = {"turns": [], "completeness": {"status": "complete"}}
    else:
        thread = codex_runtime.read_thread(
            thread_id, cwd=record["cwd"], timeout=float(timeout),
            worker_id=owner.worker_id,
            expected_source=store.codex_source(owner.worker),
        )
        conversation = build_codex(
            record,
            thread,
            state=(record.get("lifecycle") or {}).get("state"),
            worker=f"codex:{short}",
            max_tool_result_bytes=0,
            evidence=evidence_index,
        )

    latest_usage: dict[str, dict[str, Any]] = {}
    turn_cumulative: dict[str, dict[str, Any]] = {}
    touched_turns: set[str] = set()
    usage_touched_turns: set[str] = set()
    previous_observed_cumulative: dict[str, Any] | None = None
    for event in events:
        sequence = event.get("sequence")
        tid = _turn_id(event)
        in_delta = (
            tid
            and isinstance(sequence, int)
            and (
                effective_after_sequence is None
                or sequence > effective_after_sequence
            )
            and sequence <= processed_through
        )
        if in_delta:
            touched_turns.add(tid)
        if event.get("method") == TOKEN_USAGE_METHOD and tid:
            projected = _usage_projection(event)
            cumulative = projected.get("cumulative")
            projected["reset"] = bool(
                projected.get("zero_last")
                and isinstance(previous_observed_cumulative, dict)
                and isinstance(cumulative, dict)
                and cumulative == previous_observed_cumulative
            )
            if projected["reset"]:
                projected["context_after"] = None
            if isinstance(cumulative, dict) and any(
                isinstance(cumulative.get(key), int)
                for key in (
                    "input",
                    "output",
                    "cached_input",
                    "reasoning_output",
                    "total",
                )
            ):
                previous_observed_cumulative = dict(cumulative)
            current = latest_usage.get(tid)
            if current is None or int(event.get("sequence") or 0) >= int(
                current.get("sequence") or 0
            ):
                latest_usage[tid] = projected
                turn_cumulative[tid] = dict(
                    projected.get("cumulative") or {}
                )
            if in_delta:
                usage_touched_turns.add(tid)

    compactions, compaction_sequence_ids = _compactions(events)
    touched_compactions = {
        cid
        for sequence, cid in compaction_sequence_ids.items()
        if (
            (
                effective_after_sequence is None
                or sequence > effective_after_sequence
            )
            and sequence <= processed_through
        )
    }

    conversation_turns = [
        turn for turn in (conversation.get("turns") or [])
        if isinstance(turn, dict)
    ]
    evidence_turns = evidence_index.get("turns") or {}
    lifecycle_coverage_complete = all(
        (
            isinstance(turn.get("turnId"), str)
            and bool(
                (evidence_turns.get(turn.get("turnId")) or {}).get(
                    "sawTurnStarted"
                )
            )
            and bool(
                (evidence_turns.get(turn.get("turnId")) or {}).get(
                    "sawTurnCompleted"
                )
            )
        )
        for turn in conversation_turns
    )
    evidence_complete = (
        evidence_index.get("status") == "complete"
        and gap is None
        and not foreign_events
        and lifecycle_coverage_complete
    )

    # tokenUsage.total is cumulative across the native thread. Fold endpoints
    # independently of journal arrival order, then subtract only the immediately
    # preceding native turn endpoint. Missing predecessor evidence keeps a turn
    # partial rather than attributing multiple turns to one delta.
    turn_baseline: dict[str, dict[str, Any] | None] = {}
    previous_turn_id: str | None = None
    previous_endpoint: dict[str, Any] | None = None
    for index, turn in enumerate(conversation_turns):
        turn_id = turn.get("turnId")
        if not isinstance(turn_id, str) or not turn_id:
            previous_turn_id = None
            previous_endpoint = None
            continue
        ev = evidence_turns.get(turn_id) or {}
        if (
            index == 0
            and ev.get("sawTurnStarted")
        ):
            baseline: dict[str, Any] | None = _zero_cumulative()
        else:
            baseline = (
                dict(previous_endpoint)
                if isinstance(previous_endpoint, dict)
                else None
            )
        turn_baseline[turn_id] = baseline

        # A late endpoint for turn N changes the derived consumption of N+1,
        # even though no new native event belongs to N+1. Re-emit that direct
        # successor so cursor consumers can upsert the corrected projection.
        if (
            previous_turn_id in usage_touched_turns
            and turn_id in turn_cumulative
        ):
            touched_turns.add(turn_id)

        endpoint = turn_cumulative.get(turn_id)
        previous_endpoint = (
            dict(endpoint) if isinstance(endpoint, dict) else None
        )
        previous_turn_id = turn_id

    compaction_turn_ids = {
        item.get("turn_id")
        for item in compactions
        if isinstance(item.get("turn_id"), str)
    }
    turns = []
    for turn in conversation_turns:
        if not isinstance(turn, dict):
            continue
        turn_id = turn.get("turnId")
        ev = (evidence_index.get("turns") or {}).get(turn_id) or {}
        ev_sequences = ev.get("sequences") or []
        first_ev_sequence = min(
            (seq for seq in ev_sequences if isinstance(seq, int)),
            default=None,
        )
        if after_sequence is not None and evidence_complete:
            if turn_id not in touched_turns:
                continue
        elif after_sequence is None and has_more and first_ev_sequence is not None:
            if first_ev_sequence > processed_through:
                continue
        instruction, steers = _instruction_and_steers(turn)
        usage = latest_usage.get(turn_id)
        baseline = turn_baseline.get(turn_id)
        tokens = _turn_token_delta(turn_cumulative.get(turn_id), baseline)
        context_after = usage.get("context_after") if usage else None
        if (
            turn_id in compaction_turn_ids
            and usage is not None
            and usage.get("zero_last")
        ):
            context_after = None
        turns.append({
            "turn_id": turn_id,
            "kind": "compaction" if turn_id in compaction_turn_ids else "model",
            "instruction": instruction,
            "steers": steers,
            "started_at": turn.get("startedAt"),
            "completed_at": turn.get("completedAt"),
            "observed_started_at_ms": ev.get("startedAtMs"),
            "observed_completed_at_ms": ev.get("completedAtMs"),
            "outcome": turn.get("status"),
            "tokens": tokens,
            "context_after": context_after,
            "compaction_refs": [
                item["compaction_id"]
                for item in compactions
                if item.get("turn_id") == turn_id
            ],
            "coverage": {
                "lifecycle": (
                    "observed"
                    if ev.get("sawTurnStarted") and ev.get("sawTurnCompleted")
                    else "partial"
                ),
                "tokens": (
                    "observed" if tokens is not None
                    else ("partial" if usage is not None else "unavailable")
                ),
                "context_after": (
                    "observed" if context_after is not None
                    else (
                        "pending-next-turn"
                        if turn_id in compaction_turn_ids
                        else "unavailable"
                    )
                ),
            },
            "sources": [
                "codex-thread-turns-paged",
                *(
                    ["ccx-evidence-lifecycle"]
                    if ev else []
                ),
                *(
                    ["codex-thread-token-usage-updated"]
                    if usage else []
                ),
            ],
        })

    if evidence_complete:
        compactions = [
            item for item in compactions
            if item["compaction_id"] in touched_compactions
        ]

    warnings = list(evidence_index.get("warnings") or [])
    if foreign_events:
        warnings.append(
            "excluded native telemetry events without the original Codex thread identity"
        )
    if gap:
        warnings.append(
            "requested telemetry cursor is outside the retained evidence range"
        )
    status = (
        "complete"
        if evidence_complete
        else "partial"
    )
    store.revalidate()
    return {
        "worker": {
            "engine": "codex",
            "short": short,
            "name": record.get("name"),
            "thread_id": record.get("threadId"),
        },
        "turns": turns,
        "compactions": compactions,
        "coverage": {
            "status": status,
            "capture_started_at_ms": evidence_index.get("beganAtMs"),
            "gap": gap,
            "warnings": warnings,
            "conversation": conversation.get("completeness"),
        },
        "next_cursor": _cursor(
            cursor_owner, journal.journal_instance_id, processed_through
        ),
        "has_more": has_more,
    }


__all__ = ["read_telemetry"]
