"""Authoritative managed-Claude turn boundaries from the host event sidecar.

The managed host owns the worker-turn counter. Transcript prompts are not turn
identities: Claude can persist /compact, task notifications, queued commands and
other user-like records that do not correspond 1:1 with host turn boundaries.

``turn_complete`` is semantic completion/interruption evidence.
``turn_settled`` is liveness-only terminal evidence whose semantic outcome is
unknown; it never proves a semantic completion. Exact report windows come from
the transcript byte offsets recorded at each managed turn's start, not from
terminal timestamps.
"""

from __future__ import annotations

import json
import uuid
from typing import Any

from ccx.store.paths import events_ndjson_path


def new_turn_ref() -> str:
    """Mint one opaque, start-unique managed-Claude turn identity."""
    return f"ccx-turn:claude:v1:{uuid.uuid4()}"


def _ts_ms(value: Any) -> int | None:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return None
    # Host broadcast timestamps are wall-clock seconds. Accept milliseconds as
    # well so fixtures/future writers can use the record convention directly.
    return int(value * 1000) if abs(float(value)) < 10_000_000_000 else int(value)


def _terminal_events(short: str) -> list[dict[str, Any]] | None:
    """Normalized retained managed-turn terminals in sidecar order.

    ``None`` means the sidecar itself is unavailable. ``[]`` means it exists but
    currently contains no retained managed turn terminal.
    """
    path = events_ndjson_path(short)
    if not path.exists():
        return None
    out: list[dict[str, Any]] = []
    try:
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                try:
                    value = json.loads(line)
                except (TypeError, ValueError):
                    continue
                if not isinstance(value, dict):
                    continue
                event_type = value.get("type")
                if event_type not in {"turn_complete", "turn_settled"}:
                    continue
                turn = value.get("turn")
                if not isinstance(turn, int) or isinstance(turn, bool) or turn <= 0:
                    continue
                settled_at_ms = _ts_ms(value.get("ts"))
                if settled_at_ms is None:
                    continue
                if event_type == "turn_complete":
                    interrupted = bool(value.get("interrupted", False))
                    outcome = "interrupted" if interrupted else "completed"
                else:
                    interrupted = None
                    outcome = value.get("outcome") or "unknown"
                    if outcome not in {"completed", "interrupted", "unknown"}:
                        outcome = "unknown"
                item: dict[str, Any] = {
                    "turn": turn,
                    "settledAtMs": settled_at_ms,
                    "epoch": value.get("epoch"),
                    "seq": value.get("seq"),
                    "eventType": event_type,
                    "outcome": outcome,
                }
                if interrupted is not None:
                    item["interrupted"] = interrupted
                    item["completedAtMs"] = settled_at_ms
                if isinstance(value.get("turn_ref"), str) and value.get("turn_ref"):
                    item["turnRef"] = value["turn_ref"]
                if event_type == "turn_settled":
                    if isinstance(value.get("evidence"), str) and value.get("evidence"):
                        item["evidence"] = value["evidence"]
                    if isinstance(value.get("native_status"), str) and value.get("native_status"):
                        item["nativeStatus"] = value["native_status"]
                out.append(item)
    except OSError:
        return None
    return out


def turn_settlements(short: str) -> list[dict[str, Any]] | None:
    """Return all retained managed turn terminal boundaries in sidecar order."""
    values = _terminal_events(short)
    return None if values is None else [dict(value) for value in values]


def latest_settled_turn(
    short: str, *, fallback: int = 0, record_last_settled_at_ms: int | None = None
) -> int:
    """Latest host-owned settled turn, with a fail-open record fallback.

    ``lastSettledAt`` is written before the terminal event. If the retained
    sidecar's latest terminal predates that record timestamp, the sidecar is
    known stale/incomplete and must not decrement a valid persisted ordinal.
    """
    values = turn_settlements(short)
    if values:
        latest = values[-1]
        if (
            isinstance(record_last_settled_at_ms, int)
            and int(latest["settledAtMs"]) < record_last_settled_at_ms
        ):
            return max(0, int(fallback or 0))
        return int(latest["turn"])
    return max(0, int(fallback or 0))


def _with_previous_settlement(values: list[dict[str, Any]], index: int) -> dict[str, Any]:
    result = dict(values[index])
    result["previousSettledAtMs"] = (
        int(values[index - 1]["settledAtMs"]) if index > 0 else None
    )
    return result


def settlement_for_turn(
    short: str, turn_number: int
) -> tuple[dict[str, Any] | None, bool]:
    """Exact retained managed terminal for one lifecycle ordinal."""
    values = turn_settlements(short)
    if values is None:
        return None, False
    for index in range(len(values) - 1, -1, -1):
        if values[index]["turn"] == turn_number:
            return _with_previous_settlement(values, index), True
    return None, True


def settlement_for_turn_ref(
    short: str, turn_ref: str
) -> tuple[dict[str, Any] | None, bool]:
    """Exact retained managed terminal for one start-unique turn ref."""
    values = turn_settlements(short)
    if values is None:
        return None, False
    for index in range(len(values) - 1, -1, -1):
        if values[index].get("turnRef") == turn_ref:
            return _with_previous_settlement(values, index), True
    return None, True


def report_window_for_turn_ref(
    short: str, turn_ref: str
) -> tuple[dict[str, Any] | None, bool]:
    """Exact retained Claude transcript window metadata for one managed turn.

    ``None, False`` means the sidecar is unavailable. ``None, True`` means the
    sidecar exists but has never heard of this turn ref. A returned dict may have
    ``startOffset=None`` when CCX only recovered the turn identity after output
    had already begun; callers must treat that as unavailable rather than guess.

    The upper bound is the first later managed-turn identity. If that later turn
    lacks an exact start offset, ``endOffsetKnown`` is false: reading to current
    EOF would cross a known turn boundary and is forbidden.
    """
    path = events_ndjson_path(short)
    if not path.exists():
        return None, False
    starts: list[dict[str, Any]] = []
    terminal: dict[str, Any] | None = None
    seen_ref = False
    try:
        with path.open(encoding="utf-8") as handle:
            for index, line in enumerate(handle):
                try:
                    value = json.loads(line)
                except (TypeError, ValueError):
                    continue
                if not isinstance(value, dict):
                    continue
                ref = value.get("turn_ref")
                if not isinstance(ref, str) or not ref:
                    continue
                event_type = value.get("type")
                if event_type in {"turn_start", "turn_identity_recovered"}:
                    raw_offset = value.get("report_start_offset")
                    offset = (
                        raw_offset
                        if isinstance(raw_offset, int)
                        and not isinstance(raw_offset, bool)
                        and raw_offset >= 0
                        else None
                    )
                    starts.append({
                        "turnRef": ref,
                        "startOffset": offset,
                        "lineIndex": index,
                    })
                    if ref == turn_ref:
                        seen_ref = True
                    continue
                if event_type in {"turn_complete", "turn_settled"} and ref == turn_ref:
                    terminal = dict(value)
                    terminal["_ccx_line_index"] = index
                    seen_ref = True
    except OSError:
        return None, False

    if not seen_ref:
        return None, True

    start = next((value for value in starts if value["turnRef"] == turn_ref), None)
    start_offset = start.get("startOffset") if start is not None else None
    terminal_index = (
        terminal.get("_ccx_line_index") if isinstance(terminal, dict) else None
    )
    end_offset = None
    end_offset_known = True
    if isinstance(terminal_index, int):
        for value in starts:
            if value["lineIndex"] <= terminal_index or value["turnRef"] == turn_ref:
                continue
            if value.get("startOffset") is None:
                end_offset_known = False
            else:
                end_offset = value["startOffset"]
            break

    return {
        "turnRef": turn_ref,
        "startOffset": start_offset,
        "endOffset": end_offset,
        "endOffsetKnown": end_offset_known,
        "settled": terminal is not None,
    }, True
