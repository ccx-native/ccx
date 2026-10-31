from __future__ import annotations

import json

from ccx.claude import managed_turns as M


def _events(tmp_path, monkeypatch, *values):
    path = tmp_path / "sessions" / "worker.events.ndjson"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(value) + "\n" for value in values))
    monkeypatch.setattr(M, "events_ndjson_path", lambda short: path)
    return path


def _complete(turn, ms, seq=None):
    return {
        "type": "turn_complete",
        "turn": turn,
        "ts": ms / 1000,
        "epoch": "host",
        "seq": seq if seq is not None else turn,
    }


def test_new_turn_ref_is_unique_and_opaque():
    first = M.new_turn_ref()
    second = M.new_turn_ref()
    assert first != second
    assert first.startswith("ccx-turn:claude:v1:")


def _settled(turn, ms, seq=None, *, turn_ref=None):
    value = {
        "type": "turn_settled",
        "turn": turn,
        "ts": ms / 1000,
        "epoch": "host",
        "seq": seq if seq is not None else turn,
        "outcome": "unknown",
        "evidence": "native-session-inactive",
        "native_status": "idle",
    }
    if turn_ref:
        value["turn_ref"] = turn_ref
    return value


def test_turn_settlements_include_semantic_and_unknown(tmp_path, monkeypatch):
    _events(tmp_path, monkeypatch, _complete(1, 1_000), _settled(2, 2_000))
    values = M.turn_settlements("worker")
    assert [value["outcome"] for value in values] == ["completed", "unknown"]
    assert values[1]["eventType"] == "turn_settled"
    assert values[1]["nativeStatus"] == "idle"


def test_latest_settled_turn_uses_last_settled_staleness_guard(tmp_path, monkeypatch):
    _events(tmp_path, monkeypatch, _complete(1, 1_000), _settled(2, 2_000))
    assert M.latest_settled_turn(
        "worker", fallback=3, record_last_settled_at_ms=2_500
    ) == 3
    assert M.latest_settled_turn(
        "worker", fallback=1, record_last_settled_at_ms=1_500
    ) == 2


def test_settlement_lookup_by_ref_returns_unknown_terminal(tmp_path, monkeypatch):
    ref = "ccx-turn:claude:v1:settled"
    _events(tmp_path, monkeypatch, _settled(4, 4_000, turn_ref=ref))
    settlement, available = M.settlement_for_turn_ref("worker", ref)
    assert available is True
    assert settlement["turn"] == 4
    assert settlement["outcome"] == "unknown"
    assert settlement["previousSettledAtMs"] is None


def test_report_window_is_bounded_by_next_exact_turn_start(tmp_path, monkeypatch):
    first = "ccx-turn:claude:v1:first"
    second = "ccx-turn:claude:v1:second"
    _events(
        tmp_path,
        monkeypatch,
        {"type": "turn_start", "turn_ref": first, "turn": 1,
         "report_start_offset": 10},
        {**_complete(1, 1_000), "turn_ref": first},
        {"type": "turn_start", "turn_ref": second, "turn": 2,
         "report_start_offset": 50},
        {**_complete(2, 2_000), "turn_ref": second},
    )

    window, available = M.report_window_for_turn_ref("worker", first)

    assert available is True
    assert window == {
        "turnRef": first,
        "startOffset": 10,
        "endOffset": 50,
        "endOffsetKnown": True,
        "settled": True,
    }


def test_report_window_refuses_to_cross_later_unfenced_turn(tmp_path, monkeypatch):
    first = "ccx-turn:claude:v1:first"
    second = "ccx-turn:claude:v1:second"
    _events(
        tmp_path,
        monkeypatch,
        {"type": "turn_start", "turn_ref": first, "turn": 1,
         "report_start_offset": 10},
        {**_complete(1, 1_000), "turn_ref": first},
        {"type": "turn_start", "turn_ref": second, "turn": 2},
    )

    window, available = M.report_window_for_turn_ref("worker", first)

    assert available is True
    assert window["startOffset"] == 10
    assert window["endOffset"] is None
    assert window["endOffsetKnown"] is False


def test_report_window_knows_started_but_not_settled_turn(tmp_path, monkeypatch):
    ref = "ccx-turn:claude:v1:running"
    _events(
        tmp_path,
        monkeypatch,
        {"type": "turn_start", "turn_ref": ref, "turn": 1,
         "report_start_offset": 12},
    )

    window, available = M.report_window_for_turn_ref("worker", ref)

    assert available is True
    assert window["settled"] is False
    assert window["startOffset"] == 12
