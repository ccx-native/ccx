"""Unit tests for the generic CCX event primitives.

Covers host-side envelopes, bounded previews, payload shaping, sidecar
rotation, and cursor filtering using local data only.
"""

from __future__ import annotations

import os
import stat

from ccx.claude.host import events as H
from ccx.ops.semantics import filter_events_after
from ccx.constants import (
    CCX_EVENTS_KEEP,
    CCX_EVENTS_MAX,
    EVENT_PREVIEW_LIMIT,
    EVENT_SCHEMA_VERSION,
)
from ccx.types import Event


# --- envelope stamping -------------------------------------------------------

def test_stamp_event_adds_envelope_and_preserves_fields():
    evt = {"type": "state", "from": "busy", "to": "idle"}
    out = H._stamp_event(evt, short="abc12345", seq=7, epoch="123-456", ts=1000.5)
    assert out["v"] == EVENT_SCHEMA_VERSION
    assert out["type"] == "state"
    assert out["short"] == "abc12345"
    assert out["seq"] == 7
    assert out["epoch"] == "123-456"
    assert out["ts"] == 1000.5
    # event-specific fields pass through untouched
    assert out["from"] == "busy"
    assert out["to"] == "idle"


def test_stamp_event_does_not_mutate_input():
    evt = {"type": "turn_complete", "turn": 3}
    H._stamp_event(evt, short="s", seq=1, epoch="e", ts=1.0)
    assert evt == {"type": "turn_complete", "turn": 3}


# --- bounded previews --------------------------------------------------------

def test_event_preview_bounds_long_value():
    out = H._event_preview({"content": "z" * 5000})
    assert out.endswith("…")
    assert len(out) <= EVENT_PREVIEW_LIMIT + 1


def test_event_preview_handles_none_and_unserializable():
    assert H._event_preview(None) == ""

    class Weird:
        def __repr__(self):
            return "weird"

    # default=str keeps it from raising
    assert "weird" in H._event_preview({"x": Weird()})


def test_bounded_text_truncates():
    assert H._bounded_text("a" * 10, limit=4) == "aaaa…"
    # None → None (not ""), so callers can omit/null a missing field.
    assert H._bounded_text(None) is None
    assert H._bounded_text(123) == "123"


# --- event payload shaping ---------------------------------------------------

def test_tool_started_event_shape():
    data = {"tool_use_id": "tu_1", "tool_name": "Write",
            "tool_input": {"file_path": "/a", "content": "hi"}}
    ev = H._tool_started_event(data, turn=4)
    assert ev["type"] == "tool_started"
    assert ev["tool_use_id"] == "tu_1"
    assert ev["tool"] == "Write"
    assert ev["turn"] == 4
    assert "file_path" in ev["input_preview"]


def test_tool_finished_event_shape_and_ok():
    data = {"tool_use_id": "tu_1", "tool_name": "Bash",
            "tool_response": {"stdout": "ok"}}
    ev = H._tool_finished_event(data, turn=2, duration_ms=150)
    assert ev["type"] == "tool_finished"
    assert ev["tool_use_id"] == "tu_1"
    assert ev["duration_ms"] == 150
    assert ev["ok"] is True
    assert "stdout" in ev["result_preview"]


def test_tool_finished_accepts_tool_output_alias():
    data = {"tool_use_id": "tu_2", "tool_name": "Bash",
            "tool_output": {"stdout": "from-output", "is_error": True}}
    ev = H._tool_finished_event(data, turn=1, duration_ms=None)
    assert "from-output" in ev["result_preview"]
    assert ev["ok"] is False


def test_tool_finished_ok_false_on_error_marker():
    assert H._tool_ok({"tool_response": {"is_error": True}}) is False
    assert H._tool_ok({"tool_response": {"error": "boom"}}) is False
    assert H._tool_ok({"tool_response": {"stdout": "fine"}}) is True
    # tool_output alias is honored for the ok flag too
    assert H._tool_ok({"tool_output": {"isError": True}}) is False
    # unknown / non-dict response → best-effort True
    assert H._tool_ok({"tool_response": "text"}) is True
    assert H._tool_ok({}) is True


def test_hook_tool_use_id_spellings():
    assert H._hook_tool_use_id({"tool_use_id": "a"}) == "a"
    assert H._hook_tool_use_id({"toolUseID": "b"}) == "b"
    assert H._hook_tool_use_id({"toolUseId": "c"}) == "c"
    assert H._hook_tool_use_id({}) is None


def test_subagent_finished_event_shape():
    data = {"agent_id": "ag_1", "agent_type": "Explore",
            "last_message": "x" * 5000}
    ev = H._subagent_finished_event(data, turn=9)
    assert ev["type"] == "subagent_finished"
    assert ev["agent_id"] == "ag_1"
    assert ev["agent_type"] == "Explore"
    assert ev["turn"] == 9
    assert ev["last_message"].endswith("…")
    assert len(ev["last_message"]) <= EVENT_PREVIEW_LIMIT + 1


def test_subagent_finished_alt_spellings():
    data = {"subagentId": "ag_2", "subagentType": "claude", "lastMessage": "done"}
    ev = H._subagent_finished_event(data, turn=1)
    assert ev["agent_id"] == "ag_2"
    assert ev["agent_type"] == "claude"
    assert ev["last_message"] == "done"


def test_subagent_finished_last_message_none_when_absent():
    ev = H._subagent_finished_event({"agent_id": "ag_3"}, turn=1)
    # No misleading empty string — the field is explicitly null.
    assert ev["last_message"] is None


# --- bounded rotation --------------------------------------------------------

def test_rotate_events_file_noop_under_max(tmp_path):
    p = tmp_path / "s.events.ndjson"
    p.write_text("".join(f"{i}\n" for i in range(10)))
    assert H._rotate_events_file(p, max_lines=100, keep_lines=50) is False
    assert len(p.read_text().splitlines()) == 10


def test_rotate_events_file_keeps_last_n_when_over_max(tmp_path):
    p = tmp_path / "s.events.ndjson"
    p.write_text("".join(f"{i}\n" for i in range(20)))
    rotated = H._rotate_events_file(p, max_lines=15, keep_lines=10)
    assert rotated is True
    lines = p.read_text().splitlines()
    assert len(lines) == 10
    # keeps the most recent 10 (10..19)
    assert lines[0] == "10"
    assert lines[-1] == "19"


def test_rotate_events_file_defaults_are_bounded():
    # Sanity on the shipped constants: KEEP < MAX so rotation actually shrinks.
    assert CCX_EVENTS_KEEP < CCX_EVENTS_MAX


def test_rotate_events_file_missing_is_noop(tmp_path):
    assert H._rotate_events_file(tmp_path / "nope.ndjson") is False


def test_count_lines(tmp_path):
    p = tmp_path / "s.events.ndjson"
    assert H._count_lines(p) == 0          # absent
    p.write_text("a\nb\nc\n")
    assert H._count_lines(p) == 3
    assert H._count_lines(tmp_path) == 0   # unreadable (is a dir) → 0, no raise


# --- epoch-aware cursor filtering (pure) -------------------------------------

def _ev(epoch, seq, **extra):
    return Event.from_dict({"type": "x", "epoch": epoch, "seq": seq, **extra})


def test_filter_no_cursor_returns_all():
    evs = [_ev("E1", 1), _ev("E1", 2)]
    assert filter_events_after(evs, None, None) == evs


def test_filter_legacy_seq_only():
    evs = [_ev("E1", 1), _ev("E1", 2), _ev("E1", 3)]
    out = filter_events_after(evs, None, 1)
    assert [e.seq for e in out] == [2, 3]


def test_filter_epoch_cursor_spans_respawn():
    # Cursor (E1, 2): the respawned epoch E2 restarts seq at 1 — those must NOT
    # be filtered out by the prior epoch's larger seq.
    evs = [_ev("E1", 1), _ev("E1", 2), _ev("E2", 1), _ev("E2", 2)]
    out = filter_events_after(evs, "E1", 2)
    assert [(e.epoch, e.seq) for e in out] == [("E2", 1), ("E2", 2)]


def test_filter_epoch_cursor_mid_epoch():
    evs = [_ev("E2", 1), _ev("E2", 2), _ev("E2", 3)]
    out = filter_events_after(evs, "E2", 2)
    assert [e.seq for e in out] == [3]


def test_filter_skips_older_epoch_before_cursor():
    # An even older epoch E0 precedes the cursor epoch E1 in the file; it has
    # already been consumed and must be skipped.
    evs = [_ev("E0", 5), _ev("E1", 1), _ev("E1", 2), _ev("E2", 1)]
    out = filter_events_after(evs, "E1", 1)
    assert [(e.epoch, e.seq) for e in out] == [("E1", 2), ("E2", 1)]


def test_filter_unknown_epoch_replays_all():
    # Cursor epoch rotated out / unknown → replay everything (never drop new).
    evs = [_ev("E2", 1), _ev("E2", 2)]
    out = filter_events_after(evs, "E1", 99)
    assert out == evs


def test_event_cursor_property():
    e = _ev("E1", 7)
    assert e.cursor == ("E1", 7)


# --- client.events() ---------------------------------------------------------


def test_rotate_events_file_keeps_journal_private_under_loose_umask(tmp_path):
    p = tmp_path / "events.ndjson"
    p.write_text("".join(f"{i}\n" for i in range(20)))
    os.chmod(p, 0o600)
    old = os.umask(0o002)
    try:
        assert H._rotate_events_file(p, max_lines=15, keep_lines=10) is True
    finally:
        os.umask(old)
    assert stat.S_IMODE(os.stat(p).st_mode) == 0o600
    assert p.read_text().splitlines() == [str(i) for i in range(10, 20)]


def test_open_event_journal_tightens_own_loose_journal(tmp_path, monkeypatch):
    import contextlib
    d = tmp_path / "w"
    d.mkdir()
    (d / "events.ndjson").write_text("x\n")
    os.chmod(d / "events.ndjson", 0o664)

    class _Store:
        @contextlib.contextmanager
        def lease(self):
            yield

        @contextlib.contextmanager
        def _gen(self, *parts):
            fd = os.open(d, os.O_RDONLY | os.O_DIRECTORY)
            try:
                yield fd
            finally:
                os.close(fd)

    handle = H._open_event_journal(_Store(), "w")
    try:
        handle.write(b"y\n")
    finally:
        handle.close()
    assert stat.S_IMODE(os.stat(d / "events.ndjson").st_mode) == 0o600
    assert (d / "events.ndjson").read_text() == "x\ny\n"
