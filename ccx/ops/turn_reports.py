"""Exact managed-turn identity and report reads shared by public adapters."""

from __future__ import annotations

from typing import Any

from ccx.codex import records as codex_records
from ccx.codex.reports import native_timestamp_ms, report_from_turn
from ccx.errors import InvalidRequest, OperationConflict, SessionNotFound, SessionUnavailable
from ccx.claude.managed_turns import report_window_for_turn_ref, settlement_for_turn_ref, turn_settlements
from ccx.store.records import read_record as read_claude_record
from ccx.claude.transcript import exact_assistant_text

_CLAUDE_PREFIX = "ccx-turn:claude:v1:"
_CODEX_PREFIX = "ccx-turn:codex:v1:"


def _ref_id(turn_ref: str, *, engine: str) -> str:
    prefix = _CLAUDE_PREFIX if engine == "claude" else _CODEX_PREFIX
    if not isinstance(turn_ref, str) or not turn_ref.startswith(prefix):
        raise InvalidRequest(
            f"{engine.capitalize()} turn_ref must use {prefix}<native-turn-id>"
            if engine == "codex"
            else f"Claude turn_ref must use {prefix}<start-id>"
        )
    value = turn_ref[len(prefix):]
    if not value:
        raise InvalidRequest(f"turn_ref must contain an id after {prefix}")
    return value


def codex_settled_turn(
    value: dict[str, Any] | None,
    *,
    fallback_completed_at: int | None = None,
) -> dict[str, Any] | None:
    """Normalize one exact native Codex terminal projection."""
    if not isinstance(value, dict):
        return None
    turn_id = value.get("turnId")
    if not isinstance(turn_id, str) or not turn_id:
        return None
    completed_at = value.get("completedAtMs")
    if not isinstance(completed_at, int) or isinstance(completed_at, bool):
        completed_at = fallback_completed_at
    return {
        "turn_ref": f"{_CODEX_PREFIX}{turn_id}",
        "turn_id": turn_id,
        "outcome": str(value.get("status") or "unknown"),
        "completed_at": completed_at,
        "context": ({**value["context"], "as_of_turn_ref": f"{_CODEX_PREFIX}{turn_id}"}
                    if isinstance(value.get("context"), dict) else None),
    }


def _claude_settlement(value: Any) -> dict[str, Any] | None:
    """One usable ``{turnRef, outcome, settledAtMs}`` settlement, else None."""
    if not isinstance(value, dict):
        return None
    turn_ref = value.get("turnRef")
    settled_at = value.get("settledAtMs")
    if not isinstance(turn_ref, str) or not turn_ref.startswith(_CLAUDE_PREFIX):
        return None
    if not isinstance(settled_at, int) or isinstance(settled_at, bool):
        return None
    return value


def _latest_claude_settlement(record: dict[str, Any], short: str) -> dict[str, Any] | None:
    """Exact latest managed settlement, or None when it is unknown.

    The host stores the latest summary in the record (``lastSettledTurn``)
    together with ``lastSettledAt``. Records written before that field fall
    back to the latest retained sidecar terminal. A retained terminal older
    than the stored summary never replaces it, and one older than the record's
    ``lastSettledAt`` is known stale (rotated out, or not yet broadcast), so
    the latest settlement is unknown rather than that older ref.
    """
    stored = _claude_settlement(record.get("lastSettledTurn"))
    values = turn_settlements(short) or []
    retained = _claude_settlement(values[-1]) if values else None
    latest = stored
    if retained is not None and (
        stored is None
        or (
            retained["turnRef"] != stored["turnRef"]
            and retained["settledAtMs"] > stored["settledAtMs"]
        )
    ):
        # Only a strictly newer, different terminal (e.g. broadcast by a host
        # that predates the stored summary) supersedes the record.
        latest = retained
    if latest is None:
        return None
    last_settled_at = record.get("lastSettledAt")
    if (
        isinstance(last_settled_at, int)
        and not isinstance(last_settled_at, bool)
        and latest["settledAtMs"] < last_settled_at
    ):
        return None
    return latest


def last_settled_turn(engine: str, short: str) -> dict[str, Any] | None:
    """Normalized exact latest settled-turn identity for one owned worker.

    ``None`` means unknown, never "no newer turn": a caller comparing
    checkpoints must treat it as unavailable evidence.
    """
    if engine == "claude":
        record = read_claude_record(short)
        if record is None:
            return None
        value = _latest_claude_settlement(record, short)
        if value is None:
            return None
        return {
            "turn_ref": value["turnRef"],
            "turn_id": None,
            "outcome": str(value.get("outcome") or "unknown"),
            "completed_at": value["settledAtMs"],
            "context": ({**value["context"], "as_of_turn_ref": value["turnRef"]}
                        if isinstance(value.get("context"), dict) else None),
        }

    if engine == "codex":
        record = codex_records.read_record(short)
        if record is None:
            return None
        return codex_settled_turn(
            record.get("lastTurn"),
            fallback_completed_at=record.get("lastTurnAt"),
        )

    raise InvalidRequest(f"unknown engine: {engine!r}")


def codex_native_turn_evidence(
    child: str, record: dict, thread_id: str, turn_id: str,
) -> dict[str, Any] | None:
    """Existing original-owned Codex journal terminal proof; no native read."""
    from ccx.codex.evidence import build_index, read_journal

    if (not turn_id or (record.get("activeCompact") or {}).get("turnId") == turn_id
            or (record.get("lastCompact") or {}).get("turnId") == turn_id):
        return None
    journal = read_journal(child)
    index = build_index(journal, health=record.get("evidence"))
    turn = index["turns"].get(turn_id)
    if (not index["complete"] or not turn or not turn["sawTurnStarted"]
            or any(item.get("itemType") == "contextCompaction"
                   for item in index["items"].get(turn_id, {}).values())):
        return None
    events = [event for event in journal.events if event.get("turnId") == turn_id]
    if not events or any(event.get("threadId") != thread_id for event in events):
        return None
    terminals = [(event.get("params") or {}).get("turn") for event in events
                 if event.get("method") == "turn/completed"]
    if not terminals:
        return {"status": None, "error": None}
    if (any(not isinstance(turn, dict) or turn.get("id") != turn_id
            or turn.get("status") not in {"completed", "failed", "interrupted", "cancelled"}
            for turn in terminals)
            or any(turn != terminals[0] for turn in terminals[1:])):
        return None
    return {"status": terminals[0]["status"], "error": terminals[0].get("error")}


def codex_source_report(
    thread: dict, worker_id: str, thread_id: str, turn_id: str,
    *, expected_source: str,
) -> dict[str, Any]:
    """Select only the completed original native R from caller-fetched history."""
    if (not isinstance(thread, dict) or thread.get("id") != thread_id
            or thread.get("threadSource") != expected_source):
        raise SessionUnavailable("Codex conversation belongs to another worker; check the worker")
    turns = [turn for turn in (thread.get("turns") or [])
             if isinstance(turn, dict) and turn.get("id") == turn_id]
    if len(turns) != 1 or turns[0].get("status") != "completed":
        raise SessionUnavailable("Completed Codex turn is unavailable; inspect worker status")
    selected = report_from_turn(turns[0])
    return {
        "turn_ref": f"{_CODEX_PREFIX}{turn_id}", "turn_id": turn_id,
        "turn": None, "item_id": selected.get("itemId") if selected else None,
        "phase": selected.get("phase") if selected else None,
        "text": str(selected["text"]) if selected else "",
        "has_final_message": selected is not None,
        "completed_at": (selected.get("completedAt") if selected
                         else native_timestamp_ms(turns[0].get("completedAt"))),
    }


def claude_source_report(
    store, child: str, worker_id: str, ref: str,
    window: dict[str, Any],
) -> dict[str, Any]:
    """Pinned original-source Claude R window, including accepted EOF recheck.

    Unlike notification presentation, a verified empty final is "" rather than
    unavailable. All source/window failures are typed instead of coerced to None.
    """
    from ccx.claude.transcript import open_original_claude_source
    from ccx.store.root import StorageIntegrityError

    owner = store.resolve(child)
    if owner.engine != "claude" or owner.worker_id != worker_id:
        raise SessionUnavailable("Claude exact report owner/source mismatch")
    record = store.record(child)
    start, stop = window.get("startOffset"), window.get("endOffset")
    if (window.get("turnRef") != ref or not window.get("endOffsetKnown")
            or type(start) is not int or start < 0
            or (stop is not None and (type(stop) is not int or stop < start))):
        raise SessionUnavailable("Claude turn history is incomplete; inspect the worker")
    try:
        with open_original_claude_source(store, owner, record) as source:
            if source is None:
                raise SessionUnavailable("Claude history file is missing; check worker status")
            pinned, _stat, eof, _original_path, _directory_fd = source
            if start > eof or (stop is not None and stop > eof):
                raise SessionUnavailable("Claude turn history changed during reading; retry")
            text = exact_assistant_text(
                pinned, after_offset=start,
                before_offset=stop if stop is not None else eof,
            )
            if stop is None:
                # Lazy to avoid turn_reports -> protocol_ops -> turn_reports cycle.
                from ccx.ops.wait import wait_worker
                later, available = report_window_for_turn_ref(worker_id, ref)
                snapshot = wait_worker(
                    child, timeout=.3, expected_generation=store.generation,
                )
                if (not available or later != window
                        or (snapshot.get("last_settled_turn") or {}).get("turn_ref") != ref
                        or snapshot.get("active_turn_ref") not in {None, ref}):
                    raise SessionUnavailable(
                        "Claude original report EOF could include a successor turn"
                    )
            store.revalidate()
    except (StorageIntegrityError, OSError, ValueError) as exc:
        raise SessionUnavailable("Claude conversation is unavailable; inspect worker status") from exc
    return {
        "turn_ref": ref, "turn_id": None, "turn": None,
        "item_id": None, "phase": None, "text": text,
        "has_final_message": bool(text), "completed_at": None,
    }


def exact_report(
    store, owner, turn_ref: str, *, thread: dict | None = None,
) -> dict[str, Any]:
    """Exact CCX-owned native R; caller owns Codex history read transport."""
    turn_id = _ref_id(turn_ref, engine=owner.engine)
    child = owner.worker
    record = store.record(child)
    native = record.get("native") or {}
    activation = store.native_activation(child)
    original = native.get("sessionId" if owner.engine == "claude" else "threadId")
    source = (f"claude-session:{original}" if owner.engine == "claude"
              else store.codex_source(owner.worker))
    if (not isinstance(original, str) or not original
            or activation.get("state") != "confirmed"
            or activation.get("source") != source
            or (activation.get("nativeBinding") or {}).get(
                "sessionId" if owner.engine == "claude" else "threadId"
            ) != original
            or (owner.engine == "codex" and native.get("threadSource") != source)):
        raise SessionUnavailable("Conversation source could not be verified; inspect the worker")
    if owner.engine == "claude":
        terminal, available = settlement_for_turn_ref(owner.worker_id, turn_ref)
        window, window_available = report_window_for_turn_ref(owner.worker_id, turn_ref)
        if not available or not window_available:
            raise SessionUnavailable("Claude turn history is unavailable; inspect the worker")
        if terminal is None:
            if window is not None:
                raise OperationConflict("Claude turn is still running; wait for it to finish")
            raise SessionNotFound("Claude turn is no longer saved; inspect recent history")
        if (terminal.get("eventType") != "turn_complete"
                or terminal.get("outcome") != "completed"):
            raise SessionUnavailable("Claude turn is not complete; check worker status")
        if window is None or window.get("turnRef") != turn_ref:
            raise SessionUnavailable("Claude turn history is unavailable; retry later")
        result = claude_source_report(
            store, child, owner.worker_id, turn_ref, window,
        )
        result["turn"] = terminal.get("turn")
        result["completed_at"] = terminal.get("settledAtMs")
    elif owner.engine == "codex":
        proof = codex_native_turn_evidence(child, record, original, turn_id)
        if proof is None:
            raise SessionUnavailable("Codex completion record is missing; check status")
        if proof["status"] is None:
            raise OperationConflict("Codex turn is not complete; wait before reporting")
        if proof["status"] != "completed":
            raise SessionUnavailable("Codex turn ended without completion; inspect worker status")
        if thread is None:
            raise SessionUnavailable("Codex history is unavailable; retry this report")
        result = codex_source_report(
            thread, owner.worker_id, original, turn_id, expected_source=source,
        )
    else:
        raise InvalidRequest(f"unsupported engine {owner.engine!r}")
    store.revalidate()
    return result

__all__ = ["codex_settled_turn", "exact_report", "last_settled_turn"]
