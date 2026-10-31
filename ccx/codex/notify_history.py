"""Targeted Codex parent-notification delivery reconciliation.

Notification delivery only needs to answer one question: did a native
``worker_notification`` tool output with this delivery id already land?
Hydrating an entire thread with ``thread/read(includeTurns=true)`` is both
unnecessary and harmful for long-lived paginated Codex threads. Modern Codex
exposes ``thread/items/list`` for incremental history access, so CCX pages
that newest-first and never hydrates the full history for reconciliation.
"""

from __future__ import annotations

import json
from typing import Any, Callable

ITEM_PAGE_SIZE = 100
MAX_ITEM_PAGES = 1000
ITEM_PAGE_TIMEOUT_S = 10.0


def _notification_payload(item: Any, *, name: str = "worker_notification") -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    # ``thread/items/list`` wraps each native item with its containing turn id.
    candidate = item.get("item") if isinstance(item.get("item"), dict) else item
    if not isinstance(candidate, dict):
        return None
    if (
        candidate.get("type") != "functionCallOutput"
        or candidate.get("name") != name
    ):
        return None
    raw = candidate.get("output")
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str):
        return None
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return value if isinstance(value, dict) else None


def _item_delivery_match(
    item: Any,
    delivery_id: str,
    *,
    legacy_event_id: str | None = None,
    name: str = "worker_notification",
) -> dict[str, Any] | None:
    value = _notification_payload(item, name=name)
    if value is None:
        return None
    matches = (
        value.get("deliveryId") == delivery_id
        or bool(
            legacy_event_id
            and value.get("deliveryId") is None
            and value.get("eventId") == legacy_event_id
        )
    )
    if not matches:
        return None
    turn_id = item.get("turnId") if isinstance(item, dict) else None
    return {
        "deliveryId": delivery_id,
        "eventId": value.get("eventId"),
        "turnId": turn_id if isinstance(turn_id, str) and turn_id else None,
    }


def _paged_delivery_match(
    request: Callable[..., dict[str, Any]],
    thread_id: str,
    delivery_id: str,
    *,
    legacy_event_id: str | None = None,
    stop_at_turn_id: str | None = None,
    name: str = "worker_notification",
) -> dict[str, Any] | None:
    cursor: str | None = None
    seen_cursors: set[str] = set()
    for _ in range(MAX_ITEM_PAGES):
        params: dict[str, Any] = {
            "threadId": thread_id,
            "limit": ITEM_PAGE_SIZE,
            "sortDirection": "desc",
        }
        if cursor is not None:
            params["cursor"] = cursor
        result = request("thread/items/list", params, timeout=ITEM_PAGE_TIMEOUT_S)
        data = result.get("data") if isinstance(result, dict) else None
        if not isinstance(data, list):
            raise RuntimeError("thread/items/list returned malformed data")
        for entry in data:
            match = _item_delivery_match(
                entry, delivery_id, legacy_event_id=legacy_event_id, name=name
            )
            if match is not None:
                return match
            if (
                stop_at_turn_id
                and isinstance(entry, dict)
                and entry.get("turnId") == stop_at_turn_id
            ):
                # Newest-first reached a turn that had already completed
                # before the first attempt; nothing older can hold it.
                return None
        next_cursor = result.get("nextCursor")
        if not isinstance(next_cursor, str) or not next_cursor:
            return None
        if next_cursor in seen_cursors:
            raise RuntimeError("thread/items/list repeated a pagination cursor")
        seen_cursors.add(next_cursor)
        cursor = next_cursor
    raise RuntimeError(
        f"thread/items/list exceeded {MAX_ITEM_PAGES} pages while reconciling notification"
    )


def notification_delivery_match(
    request: Callable[..., dict[str, Any]],
    thread_id: str,
    delivery_id: str,
    *,
    legacy_event_id: str | None = None,
    stop_at_turn_id: str | None = None,
    name: str = "worker_notification",
) -> dict[str, Any] | None:
    """Find a landed notification, newest first.

    ``stop_at_turn_id`` names a turn that had completed before the delivery's
    first attempt: the paged walk ends there instead of at the thread's start.
    """
    return _paged_delivery_match(
        request, thread_id, delivery_id,
        legacy_event_id=legacy_event_id, stop_at_turn_id=stop_at_turn_id,
        name=name,
    )
