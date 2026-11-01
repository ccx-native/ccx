"""A small engine-neutral recorded snapshot of background work.

Producers own these facts; consumers only read the saved summary/event.
"""
from __future__ import annotations

import re
import time

KINDS = ("command", "monitor", "subagent", "notification")
OPEN = {"running", "pending"}
LABEL_LIMIT = 120
EVENT_LIMIT = 1000
ITEM_LIMIT = 100


def label(value: object, fallback: str) -> str:
    text = re.sub(r"\s+", " ", str(value)) if isinstance(value, str) else ""
    text = "".join(ch for ch in text if ch.isprintable()).strip()
    return text[:LABEL_LIMIT] or fallback


def bounded_event(value: object) -> str | None:
    """Bound one sanitized Monitor notification; labels remain much shorter."""
    if not isinstance(value, str):
        return None
    return re.sub(r"\s+", " ", value).strip()[:EVENT_LIMIT]



def summary(items: list[dict], *, as_of_ms: int | None = None,
            available: bool = True) -> dict:
    if as_of_ms is None:
        as_of_ms = int(time.time() * 1000)
    if not available:
        return {
            "running_count": None, "pending_count": None, "by_kind": None,
            "items": [], "items_truncated": 0,
            "as_of_ms": as_of_ms, "available": False,
        }
    active = [dict(item) for item in items
              if item.get("status") in OPEN and item.get("kind") in KINDS]
    by_kind = {kind: sum(item["kind"] == kind for item in active) for kind in KINDS}
    # Sort once, before applying the display bound. Unknown start times are
    # after known starts, then every item is ordered by stable ID.
    active.sort(key=lambda item: (
        item.get("started_at_ms") if type(item.get("started_at_ms")) is int
        else float("inf"), item["id"]))
    # Keep snapshots small and deterministic; avoid leaking native content.
    preview = [{key: (bounded_event(item[key]) if key == "event" else item[key])
                for key in ("id", "kind", "status", "label", "event", "event_count",
                            "expires_at_ms", "notification_id", "delivery_id")
                if key in item} for item in active[:ITEM_LIMIT]]
    return {
        "running_count": sum(by_kind[kind] for kind in KINDS if kind != "notification"),
        "pending_count": by_kind["notification"],
        "by_kind": by_kind, "items": preview,
        "items_truncated": max(0, len(active) - len(preview)),
        "as_of_ms": as_of_ms, "available": True,
    }


def materially_changed(previous: dict | None, current: dict) -> bool:
    if previous is None:
        return True
    return {key: value for key, value in previous.items() if key != "as_of_ms"} != {
        key: value for key, value in current.items() if key != "as_of_ms"}


def claude_task_item(task: dict) -> dict | None:
    task_id = task.get("task_id")
    if not isinstance(task_id, str) or not task_id:
        return None  # launching tool-result has not yet proved native task identity
    native_kind = task.get("kind")
    kind = "command" if native_kind == "bash" else native_kind
    if kind not in ("command", "monitor", "subagent"):
        return None
    item = {
        "id": f"{kind}:{task_id}", "kind": kind,
        "status": task.get("status") or "unknown",
        "label": label(task.get("description"), f"background {kind}"),
        "tool_use_id": task.get("tool_use_id"),
        "started_at_ms": task.get("launched_at"),
    }
    if kind == "monitor":
        launched, timeout = task.get("launched_at"), task.get("timeout_ms")
        item["expires_at_ms"] = (launched + timeout if type(launched) is int
                                 and type(timeout) is int and timeout > 0 else None)
        item["event_count"] = task.get("event_count") or 0
        if task.get("last_event"):
            item["event"] = bounded_event(task["last_event"])
    if task.get("exit_code") is not None:
        item["exit_code"] = task["exit_code"]
    return item


def diff_items(before: list[dict], after: list[dict]) -> list[tuple[str, dict]]:
    """One source-row transition, never a second invented native observation."""
    old = {item["id"]: item for item in before}
    out = []
    for item in after:
        prior = old.get(item["id"])
        if item == prior:
            continue
        status = item.get("status")
        if prior is None or (prior.get("status") not in OPEN and status in OPEN):
            transition = "reopened" if prior is not None else "started"
        elif status not in OPEN and (prior is None or prior.get("status") in OPEN):
            transition = {"completed": "finished", "failed": "failed",
                          "stopped": "stopped"}.get(status, "event")
        else:
            transition = "event"
        out.append((transition, item))
    return out

