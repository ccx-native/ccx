"""Pure V2 Local JSON worker/result presentation; never a native state adapter.

This module deliberately owns no store, host, or daemon. Its callers provide a
verified owned record and any already-read (bounded) enrichment. In particular,
nothing here changes WorkerClient, the CLI, or protocol_ops return contracts.
"""

from __future__ import annotations

from typing import Any

from ccx.store.worker_state import normalize_native_state

WORKER_KEYS = (
    "worker", "engine", "name", "cwd", "model", "effort", "state",
    "native_state", "state_source", "loaded", "native_id", "forked_from",
    "created_at", "last_turn_at", "managed_turns", "active_turn_ref",
    "last_settled_turn", "pending_input", "pending_input_observed", "context",
    "background_tasks",
)
CONTEXT_KEYS = (
    "tokens_used", "window_size", "percent_used", "reason", "as_of_ms",
    "as_of_turn_ref",
)


def context_view(value: dict[str, Any] | None) -> dict[str, Any]:
    """A completed recorded read has an object even without a token signal."""
    value = value if isinstance(value, dict) else {}
    out = {key: value.get(key) for key in CONTEXT_KEYS}
    if out["tokens_used"] is None and not out["reason"]:
        out["reason"] = "pending" if value.get("state") == "pending" else "no-signal"
    return out


def background_view(value: dict[str, Any] | None) -> dict[str, Any] | None:
    """Group the existing bounded protocol_ops background facts once."""
    if value is None:
        return None
    if not value.get("background_tasks_supported", False):
        return {"supported": False, "tasks": [], "running_count": None,
                "truncated": False}
    result = {
        "supported": True,
        "tasks": list(value.get("background_tasks") or []),
        "running_count": value.get("running_background_task_count"),
        "truncated": bool(value.get("background_tasks_truncated") or
                          value.get("background_task_details_truncated")),
    }
    if value.get("background_task_summary") is not None:
        result["summary"] = value["background_task_summary"]
    return result


def project_worker_view(
    handle: str,
    record: dict[str, Any],
    *,
    context: dict[str, Any] | None = None,
    background: dict[str, Any] | None = None,
    last_settled_turn: dict[str, Any] | None = None,
    active_turn_ref: str | None = None,
    pending_input: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Serialize one recorded owned worker with exactly 21 fixed keys.

    The handle has already been proven by the caller's generation/owner lookup.
    An observed native verdict remains separate: V2 wait intentionally uses
    this recorded worker view, never a second host/daemon observation.
    """
    from ccx.ops.forking import public_lineage

    engine, _, _worker_id = handle.partition(":")
    lifecycle = record.get("lifecycle")
    lifecycle = lifecycle if isinstance(lifecycle, dict) else {}
    native = record.get("native")
    native = native if isinstance(native, dict) else {}
    create_args = record.get("createArgs")
    create_args = create_args if isinstance(create_args, dict) else {}
    stats = record.get("stats")
    stats = stats if isinstance(stats, dict) else {}
    raw_state = lifecycle.get("state")
    lineage = public_lineage(record)
    if lineage is not None:
        lineage = {
            "worker": lineage.get("worker"),
            "native_id": (lineage.get("session_id") if engine == "claude"
                          else lineage.get("thread_id")),
            "forked_at_ms": lineage.get("forked_at_ms"),
        }
    if last_settled_turn is not None:
        last_settled_turn = {
            "turn_ref": last_settled_turn.get("turn_ref"),
            "turn_id": last_settled_turn.get("turn_id"),
            "outcome": last_settled_turn.get("outcome"),
            "completed_at": last_settled_turn.get("completed_at"),
            "context": (context_view(last_settled_turn["context"])
                        if isinstance(last_settled_turn.get("context"), dict) else None),
        }
    view = {
        "worker": handle,
        "engine": engine,
        "name": record.get("name"),
        "cwd": record.get("cwd"),
        "model": create_args.get("model"),
        "effort": create_args.get("effort"),
        "state": normalize_native_state(raw_state),
        "native_state": raw_state,
        "state_source": "recorded",
        "loaded": None,
        "native_id": (native.get("sessionId") if engine == "claude"
                      else native.get("threadId")),
        "forked_from": lineage,
        "created_at": record.get("createdAt"),
        "last_turn_at": record.get("lastTurnAt"),
        "managed_turns": stats.get("turns"),
        "active_turn_ref": active_turn_ref,
        "last_settled_turn": last_settled_turn,
        "pending_input": pending_input,
        "pending_input_observed": False,
        "context": context,
        "background_tasks": background_view(background),
    }
    assert tuple(view) == WORKER_KEYS
    return view
