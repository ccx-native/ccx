"""Reconstruct a native thread from source-attested paged Codex history."""
from __future__ import annotations

import time
from typing import Any, Callable

_PAGE_SIZE = 100
_MAX_PAGES = 10_000


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("Codex history paging timed out")
    return remaining


def _full_turn_items(request: Callable[..., dict], thread_id: str,
                     turn_id: str, deadline: float) -> list[dict]:
    items: list[dict] = []
    cursor: str | None = None
    seen: set[str] = set()
    for _ in range(_MAX_PAGES):
        params: dict[str, Any] = {
            "threadId": thread_id, "turnId": turn_id,
            "limit": _PAGE_SIZE, "sortDirection": "asc",
        }
        if cursor is not None:
            params["cursor"] = cursor
        page = request("thread/items/list", params, timeout=_remaining(deadline))
        entries = page.get("data") if isinstance(page, dict) else None
        if not isinstance(entries, list):
            raise ValueError("Codex item-history page is malformed")
        for wrapper in entries:
            if (not isinstance(wrapper, dict) or wrapper.get("turnId") != turn_id
                    or not isinstance(wrapper.get("item"), dict)):
                raise ValueError("Codex item-history page belongs to another turn")
            items.append(wrapper["item"])
        next_cursor = page.get("nextCursor")
        if next_cursor is None:
            return items
        if (not isinstance(next_cursor, str) or not next_cursor
                or next_cursor in seen or not entries):
            raise ValueError("Codex item-history cursor did not advance")
        seen.add(next_cursor)
        cursor = next_cursor
    raise ValueError("Codex item history exceeded page budget")


def fill_thread(
    thread: dict, request: Callable[..., dict], *, deadline: float,
    limit: int | None = None, newest_first: bool = False,
    exact_turn_id: str | None = None,
) -> dict:
    """Return the thread/read shape, with actual turn statuses and full items."""
    thread_id = thread.get("id") if isinstance(thread, dict) else None
    if not isinstance(thread_id, str) or not thread_id:
        raise ValueError("Codex source metadata has no original thread id")
    if limit is not None and (type(limit) is not int or limit < 1):
        raise ValueError("Codex history turn limit must be positive")
    if exact_turn_id is not None and (not isinstance(exact_turn_id, str)
                                       or not exact_turn_id):
        raise ValueError("Codex exact turn id is invalid")
    descending = newest_first or exact_turn_id is not None
    turns: list[dict] = []
    seen: set[str] = set()
    seen_turns: set[str] = set()
    cursor: str | None = None
    for _ in range(_MAX_PAGES):
        params: dict[str, Any] = {
            "threadId": thread_id,
            "limit": min(_PAGE_SIZE, limit - len(turns)) if limit else _PAGE_SIZE,
            "sortDirection": "desc" if descending else "asc",
            "itemsView": "notLoaded" if exact_turn_id else "full",
        }
        if cursor is not None:
            params["cursor"] = cursor
        page = request("thread/turns/list", params, timeout=_remaining(deadline))
        data = page.get("data") if isinstance(page, dict) else None
        if not isinstance(data, list):
            raise ValueError("Codex turn-history page is malformed")
        for turn in data:
            if (not isinstance(turn, dict) or not isinstance(turn.get("id"), str)
                    or not turn["id"] or not isinstance(turn.get("status"), str)
                    or not isinstance(turn.get("items"), list)
                    or turn["id"] in seen_turns):
                raise ValueError("Codex native turn-history entry is malformed")
            seen_turns.add(turn["id"])
            if exact_turn_id is not None and turn["id"] != exact_turn_id:
                continue
            result = dict(turn)
            if turn.get("itemsView", "full") != "full":
                result["items"] = _full_turn_items(request, thread_id, turn["id"], deadline)
                result["itemsView"] = "full"
            turns.append(result)
            if exact_turn_id is not None or (limit is not None and len(turns) >= limit):
                return {**thread, "turns": list(reversed(turns)) if descending else turns,
                        "historySource": "thread/turns/list"}
        next_cursor = page.get("nextCursor")
        if next_cursor is None:
            return {**thread, "turns": list(reversed(turns)) if descending else turns,
                    "historySource": "thread/turns/list"}
        if (not isinstance(next_cursor, str) or not next_cursor
                or next_cursor in seen or not data):
            raise ValueError("Codex turn-history cursor did not advance")
        seen.add(next_cursor)
        cursor = next_cursor
    raise ValueError("Codex turn history exceeded page budget")


def read_original_thread(request: Callable[..., dict], thread_id: str, *,
                         deadline: float, limit: int | None = None,
                         newest_first: bool = False,
                         exact_turn_id: str | None = None,
                         expected_source: str | None = None) -> dict:
    """One provider connection: metadata proof followed by bounded page reads."""
    reply = request("thread/read", {"threadId": thread_id, "includeTurns": False},
                    timeout=_remaining(deadline))
    thread = reply.get("thread") if isinstance(reply, dict) else None
    if not isinstance(thread, dict) or thread.get("id") != thread_id:
        raise ValueError("Codex source metadata did not return the expected thread")
    if expected_source is not None and thread.get("threadSource") != expected_source:
        raise ValueError("Codex original thread source changed before history paging")
    return fill_thread(thread, request, deadline=deadline, limit=limit,
                       newest_first=newest_first, exact_turn_id=exact_turn_id)


def read_fork_source_point(request: Callable[..., dict], thread_id: str, *,
                           deadline: float) -> dict:
    """Read only an unmanaged native source's metadata and newest turn.

    This is NOT worker history authorization. It never starts, resumes,
    unarchives or mutates the native source. Admission is a separate child
    thread/fork with its own independently verified source tag.
    """
    from uuid import UUID
    if (not isinstance(thread_id, str)
            or str(UUID(thread_id)) != thread_id):
        raise ValueError("import needs one exact native Codex thread UUID")
    reply = request(
        "thread/read", {"threadId": thread_id, "includeTurns": False},
        timeout=_remaining(deadline),
    )
    thread = reply.get("thread") if isinstance(reply, dict) else None
    if not isinstance(thread, dict) or thread.get("id") != thread_id:
        raise ValueError("Codex native import source ID is not proven")
    page = request("thread/turns/list", {
        "threadId": thread_id, "limit": 1,
        "sortDirection": "desc", "itemsView": "notLoaded",
    }, timeout=_remaining(deadline))
    items = page.get("data") if isinstance(page, dict) else None
    if not isinstance(items, list) or len(items) > 1:
        raise ValueError("Codex native import source turn page malformed")
    last = items[0] if items else None
    if last is not None and (not isinstance(last, dict)
                             or not isinstance(last.get("id"), str)):
        raise ValueError("Codex native import source turn ID unavailable")
    return {"thread": thread, "lastTurn": last}
