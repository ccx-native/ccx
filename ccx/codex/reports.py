"""Phase-aware selection of native Codex final assistant reports."""

from __future__ import annotations

import math
from typing import Any

COMMENTARY_PHASE = "commentary"
FINAL_ANSWER_PHASE = "final_answer"


def native_timestamp_ms(value: Any) -> int | None:
    """Convert a finite native Unix-seconds timestamp to integer milliseconds."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            if math.isfinite(value):
                return int(value * 1000)
        except (OverflowError, ValueError):
            pass
    return None


def report_from_item(
    item: dict[str, Any] | None,
    *,
    turn_id: str | None,
    completed_at: int | None = None,
) -> dict[str, Any] | None:
    """Return a report candidate for one completed native item.

    Codex providers do not consistently populate ``phase``. A missing or null
    phase is therefore deliberately accepted as the legacy compatibility
    fallback, while explicit commentary is never a report.
    """
    if not isinstance(item, dict) or item.get("type") != "agentMessage":
        return None
    text = item.get("text")
    if not text:
        return None
    phase = item.get("phase")
    if phase == COMMENTARY_PHASE:
        return None
    if phase not in (None, FINAL_ANSWER_PHASE):
        return None
    return {
        "text": str(text),
        "turnId": turn_id,
        "itemId": item.get("id"),
        "phase": phase,
        "completedAt": completed_at,
    }


def report_from_turn(turn: dict[str, Any] | None) -> dict[str, Any] | None:
    """Select one completed turn's final answer, if it has one.

    A strong ``final_answer`` candidate wins over every phase-less fallback in
    the turn. Within either class, native item order determines the winner.
    """
    if not isinstance(turn, dict):
        return None
    turn_id = turn.get("id")
    completed_at = native_timestamp_ms(turn.get("completedAt"))
    strong = None
    fallback = None
    for item in turn.get("items") or []:
        candidate = report_from_item(
            item, turn_id=turn_id, completed_at=completed_at
        )
        if not candidate:
            continue
        if candidate["phase"] == FINAL_ANSWER_PHASE:
            strong = candidate
        else:
            fallback = candidate
    return strong or fallback


def select_report_for_turn(
    turn: dict[str, Any] | None, *,
    live_report: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Prefer an exact-turn streamed final answer to a weaker history fallback.

    Both codexd's live completion writer and its history observer use this
    phase-aware rule; an unrelated turn's cached report is never borrowed.
    """
    selected = report_from_turn(turn)
    turn_id = turn.get("id") if isinstance(turn, dict) else None
    if (isinstance(live_report, dict)
            and isinstance(turn_id, str) and turn_id
            and live_report.get("turnId") == turn_id
            and isinstance(live_report.get("text"), str)
            and (selected is None or (
                live_report.get("phase") == FINAL_ANSWER_PHASE
                and selected.get("phase") != FINAL_ANSWER_PHASE))):
        return live_report
    return selected


def report_from_thread(thread: dict[str, Any] | None) -> dict[str, Any] | None:
    """Select the latest report candidate from completed native turns."""
    if not isinstance(thread, dict):
        return None
    for turn in reversed(thread.get("turns") or []):
        if not isinstance(turn, dict) or turn.get("status") != "completed":
            continue
        candidate = report_from_turn(turn)
        if candidate:
            return candidate
    return None


def should_replace_live_report(
    current: dict[str, Any] | None,
    candidate: dict[str, Any],
) -> bool:
    """Keep a strong final candidate ahead of a later legacy fallback."""
    if not isinstance(current, dict):
        return True
    if candidate.get("phase") == FINAL_ANSWER_PHASE:
        return True
    return not (
        current.get("turnId") == candidate.get("turnId")
        and current.get("phase") == FINAL_ANSWER_PHASE
    )


def merge_report_cache(
    existing: dict[str, Any] | None,
    candidate: dict[str, Any],
) -> dict[str, Any]:
    """Merge historical evidence without degrading a precise live timestamp.

    Item IDs remain native to their observation surface. Explicit final-answer
    text and turn identity are the cross-surface evidence when those IDs differ.
    """
    if not isinstance(existing, dict):
        return candidate
    same_turn_and_text = (
        existing.get("turnId") is not None
        and candidate.get("turnId") is not None
        and existing.get("turnId") == candidate.get("turnId")
        and existing.get("text") == candidate.get("text")
    )
    same_item = (
        existing.get("itemId") is not None
        and candidate.get("itemId") is not None
        and existing.get("itemId") == candidate.get("itemId")
    )
    same_explicit_final = (
        existing.get("itemId") is not None
        and candidate.get("itemId") is not None
        and existing.get("phase") == FINAL_ANSWER_PHASE
        and candidate.get("phase") == FINAL_ANSWER_PHASE
    )
    if (
        same_turn_and_text
        and (same_item or same_explicit_final)
        and existing.get("phase") == candidate.get("phase")
        and existing.get("completedAt") is not None
    ):
        return {**candidate, "completedAt": existing["completedAt"]}
    return candidate


__all__ = [
    "COMMENTARY_PHASE",
    "FINAL_ANSWER_PHASE",
    "report_from_item",
    "native_timestamp_ms",
    "report_from_thread",
    "report_from_turn",
    "select_report_for_turn",
    "merge_report_cache",
    "should_replace_live_report",
]
