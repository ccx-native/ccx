from __future__ import annotations

import json

import pytest

from ccx.ops import turn_reports as R
from ccx.errors import SessionNotFound, SessionUnavailable
from test_notify_claude import owners
from test_worker_telemetry import original_codex, original_sources
from test_generation_store import short_root


def test_last_settled_turn_normalizes_engine_identity(monkeypatch):
    monkeypatch.setattr(R, "read_claude_record", lambda short: {"short": short})
    monkeypatch.setattr(
        R,
        "turn_settlements",
        lambda short: [{
            "turnRef": "ccx-turn:claude:v1:one",
            "outcome": "interrupted",
            "settledAtMs": 10,
        }],
    )
    monkeypatch.setattr(
        R.codex_records,
        "read_record",
        lambda short: {
            "lastTurn": {"turnId": "native-1", "status": "failed", "completedAtMs": 20}
        },
    )

    assert R.last_settled_turn("claude", "c1") == {
        "turn_ref": "ccx-turn:claude:v1:one",
        "turn_id": None,
        "outcome": "interrupted",
        "completed_at": 10, "context": None,
    }
    assert R.last_settled_turn("codex", "c2") == {
        "turn_ref": "ccx-turn:codex:v1:native-1",
        "turn_id": "native-1",
        "outcome": "failed",
        "completed_at": 20, "context": None,
    }


# --- durable latest settlement -------------------------------------------------

def _settlement(ref, at, outcome="completed"):
    return {"turnRef": f"ccx-turn:claude:v1:{ref}", "outcome": outcome,
            "settledAtMs": at}


def _claude_latest(monkeypatch, record, retained):
    monkeypatch.setattr(R, "read_claude_record", lambda short: record)
    monkeypatch.setattr(R, "turn_settlements", lambda short: retained)
    return R.last_settled_turn("claude", "c1")


def test_stored_latest_settlement_survives_sidecar_rotation(monkeypatch):
    record = {"short": "c1", "lastSettledAt": 500,
              "lastSettledTurn": {"turn": 5, **_settlement("five", 500)}}
    for retained in (None, [], [_settlement("two", 200)]):
        assert _claude_latest(monkeypatch, record, retained) == {
            "turn_ref": "ccx-turn:claude:v1:five", "turn_id": None,
            "outcome": "completed", "completed_at": 500, "context": None,
        }


def test_stored_summary_covers_the_record_before_broadcast_window(monkeypatch):
    # The host writes the record, then broadcasts: the sidecar still ends at
    # the previous turn while the record already names the new one.
    record = {"short": "c1", "lastSettledAt": 500,
              "lastSettledTurn": _settlement("five", 500, "unknown")}
    value = _claude_latest(monkeypatch, record, [_settlement("four", 400)])
    assert value["turn_ref"] == "ccx-turn:claude:v1:five"
    assert value["outcome"] == "unknown"


def test_same_turn_prefers_the_exact_stored_summary(monkeypatch):
    record = {"short": "c1", "lastSettledAt": 500,
              "lastSettledTurn": _settlement("five", 500)}
    # Broadcast ts lands after the record write for the same turn.
    value = _claude_latest(monkeypatch, record, [_settlement("five", 503)])
    assert value["completed_at"] == 500


def test_newer_retained_terminal_from_an_older_host_wins(monkeypatch):
    # A host predating the stored summary settled a later turn.
    record = {"short": "c1", "lastSettledAt": 900,
              "lastSettledTurn": _settlement("five", 500)}
    value = _claude_latest(monkeypatch, record, [_settlement("six", 901)])
    assert value["turn_ref"] == "ccx-turn:claude:v1:six"


def test_legacy_record_without_summary_falls_back_to_retained(monkeypatch):
    record = {"short": "c1", "lastSettledAt": 400}
    value = _claude_latest(monkeypatch, record, [_settlement("four", 401)])
    assert value["turn_ref"] == "ccx-turn:claude:v1:four"
    # A record without lastSettledAt at all still uses the sidecar.
    value = _claude_latest(monkeypatch, {"short": "c1"},
                           [_settlement("four", 401)])
    assert value["turn_ref"] == "ccx-turn:claude:v1:four"


def test_stale_retained_terminal_is_unknown_not_an_older_ref(monkeypatch):
    # Legacy record: a newer settlement happened (lastSettledAt) but its
    # terminal is not retained. Comparing the older ref would be a lie.
    record = {"short": "c1", "lastSettledAt": 900}
    assert _claude_latest(monkeypatch, record,
                          [_settlement("four", 400)]) is None
    # Same when an older host settled after a newer one stored its summary.
    record = {"short": "c1", "lastSettledAt": 900,
              "lastSettledTurn": _settlement("five", 500)}
    assert _claude_latest(monkeypatch, record,
                          [_settlement("five", 501)]) is None


def test_malformed_settlements_are_unknown(monkeypatch):
    record = {"short": "c1", "lastSettledTurn": {"turnRef": "bogus",
                                                 "settledAtMs": 1}}
    assert _claude_latest(monkeypatch, record,
                          [{"turnRef": "x", "settledAtMs": True}]) is None


def test_full_uuid_claude_core_completed_tool_only_is_empty_not_unavailable(
    owners, monkeypatch, tmp_path,
):
    """Original verified R with only tools is a completed empty model report."""
    from test_notify_receiptless import (
        _claude_sources, _claude_hook_events, _claude_line,
    )

    _root, store = owners
    native = _claude_sources(store, monkeypatch, tmp_path)
    first = "ccx-turn:claude:v1:original-empty"
    later = "ccx-turn:claude:v1:later-model"
    _claude_hook_events(
        store, {"type": "turn_start", "turn_ref": first, "turn": 1,
                "report_start_offset": 0},
    )
    _claude_line(native, {
        "type": "assistant",
        "message": {"content": [
            {"type": "tool_use", "id": "one", "name": "Bash",
             "input": {"command": "true"}},
        ]},
    })
    stop = native.stat().st_size
    _claude_hook_events(
        store, {"type": "turn_complete", "turn_ref": first, "turn": 1},
        {"type": "turn_start", "turn_ref": later, "turn": 2,
         "report_start_offset": stop},
    )
    _claude_line(native, {
        "type": "assistant", "message": {"content": [
            {"type": "text", "text": "WRONG later reply"},
        ]},
    })
    from test_generation_store import A
    owner = store.resolve("claude:" + A)
    result = R.exact_report(store, owner, first)
    assert result["turn_ref"] == first and result["text"] == ""
    assert result["has_final_message"] is False
    with pytest.raises(SessionNotFound):
        R.exact_report(store, owner, "ccx-turn:claude:v1:not-a-proven-terminal")


def test_full_uuid_codex_core_selects_exact_native_final_and_empty_without_send(
    original_codex,
):
    from test_worker_telemetry import _journal_turn

    store, _cwd, wid, thread_id = original_codex
    _journal_turn(wid, thread_id)
    owner = store.resolve(f"codex:{wid}")
    requested = "native-turn-1"
    thread = {
        "id": thread_id, "threadSource": f"ccx-worker:{wid}",
        "turns": [
            {"id": requested, "status": "completed", "completedAt": 1792500000,
             "items": [
                 {"type": "agentMessage", "phase": "commentary",
                  "text": "not a final"},
                 {"type": "agentMessage", "text": "phase-less fallback"},
                 {"type": "agentMessage", "phase": "final_answer",
                  "text": "exact original final"},
             ]},
            {"id": "later-turn", "status": "completed",
             "items": [{"type": "agentMessage", "phase": "final_answer",
                        "text": "WRONG successor report"}]},
        ],
    }
    first = R.exact_report(
        store, owner, f"ccx-turn:codex:v1:{requested}", thread=thread,
    )
    assert first["text"] == "exact original final"
    assert first["turn_id"] == requested and first["has_final_message"] is True
    assert first["phase"] == "final_answer"
    thread["turns"][0]["items"] = [
        {"type": "commandExecution", "id": "native-command"},
        {"type": "agentMessage", "phase": "commentary",
         "text": "non-final only"},
    ]
    empty = R.exact_report(
        store, owner, f"ccx-turn:codex:v1:{requested}", thread=thread,
    )
    assert empty["text"] == "" and empty["has_final_message"] is False
    with pytest.raises(SessionUnavailable):
        R.exact_report(store, owner, f"ccx-turn:codex:v1:{requested}",
                       thread={**thread, "threadSource": "foreign-source"})
