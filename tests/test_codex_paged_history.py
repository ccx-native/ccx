"""Schema-0.166 paginated Codex history; synthetic app-server only."""
from __future__ import annotations

import time

import pytest

from ccx.codex.history import fill_thread, read_original_thread


class FakePagedCodex:
    def __init__(self):
        self.calls = []
        self.turns = [
            {"id": f"t-{i:03}", "status": "completed", "items": [
                {"type": "agentMessage", "id": f"item-{i:03}", "text": f"answer {i}"},
            ], "itemsView": "full", "completedAt": i + 1}
            for i in range(205)
        ]
        self.turns[-1]["status"] = "failed"
        self.turns[-1]["error"] = {"message": "last turn failed"}
        self.turns[100]["itemsView"] = "summary"
        self.turns[100]["items"] = []

    def request(self, method, params, *, timeout):
        assert timeout > 0
        assert not (method == "thread/read" and params.get("includeTurns") is True), (
            "no CCX fake provider request may hydrate full thread/read")
        self.calls.append((method, dict(params)))
        assert params["threadId"] == "source-thread"
        if method == "thread/read":
            return {"thread": {"id": "source-thread", "threadSource": "ccx-worker:real-id",
                               "status": {"type": "idle"}, "turns": []}}
        if method == "thread/turns/list":
            ordered = (list(reversed(self.turns)) if params["sortDirection"] == "desc"
                       else self.turns)
            offset = int(params.get("cursor", "0"))
            end = min(offset + params["limit"], len(ordered))
            return {"data": ordered[offset:end],
                    "nextCursor": str(end) if end < len(ordered) else None}
        if method == "thread/items/list":
            assert params["turnId"] == "t-100"
            return {"data": [{"turnId": "t-100", "item": {
                "type": "agentMessage", "id": "item-100", "text": "answer 100",
            }}], "nextCursor": None}
        raise AssertionError(method)


def test_paged_full_thread_preserves_order_statuses_items_and_metadata():
    native = FakePagedCodex()
    result = read_original_thread(native.request, "source-thread",
                                  deadline=time.monotonic() + 5)
    assert result["threadSource"] == "ccx-worker:real-id"
    assert result["historySource"] == "thread/turns/list"
    assert len(result["turns"]) == 205
    assert result["turns"][0]["id"] == "t-000"
    assert result["turns"][-1]["status"] == "failed"
    assert result["turns"][100]["items"][0]["id"] == "item-100"
    assert [x[0] for x in native.calls].count("thread/turns/list") == 3
    assert [x[0] for x in native.calls].count("thread/items/list") == 1
    assert native.calls[0] == ("thread/read", {"threadId": "source-thread", "includeTurns": False})


def test_paged_latest_and_exact_use_descending_short_reads():
    native = FakePagedCodex()
    latest = read_original_thread(native.request, "source-thread",
                                  deadline=time.monotonic() + 5,
                                  limit=1, newest_first=True)
    assert [x["id"] for x in latest["turns"]] == ["t-204"]
    assert len([x for x in native.calls if x[0] == "thread/turns/list"]) == 1
    native.calls.clear()
    exact = read_original_thread(native.request, "source-thread",
                                 deadline=time.monotonic() + 5,
                                 exact_turn_id="t-100")
    assert [x["id"] for x in exact["turns"]] == ["t-100"]
    assert exact["turns"][0]["itemsView"] == "full"
    assert any(method == "thread/turns/list" and params["itemsView"] == "notLoaded"
               for method, params in native.calls)
    assert any(method == "thread/items/list" and params["turnId"] == "t-100"
               for method, params in native.calls)


def test_paged_history_rejects_repeated_cursors_and_expired_deadline():
    def looping(method, params, *, timeout):
        return {"data": [{"id": "one", "status": "completed", "items": []}],
                "nextCursor": "repeated"}

    with pytest.raises(ValueError, match="malformed"):
        fill_thread({"id": "source-thread"}, looping, deadline=time.monotonic() + 5)
    with pytest.raises(TimeoutError):
        fill_thread({"id": "source-thread"}, looping, deadline=time.monotonic() - 1)


def test_metadata_only_read_does_not_invoke_turn_pages():
    native = FakePagedCodex()
    # Metadata-only source proof is still the only requested method here.
    reply = native.request("thread/read", {"threadId": "source-thread", "includeTurns": False},
                           timeout=1)
    assert reply["thread"]["turns"] == []
    assert len(native.calls) == 1
