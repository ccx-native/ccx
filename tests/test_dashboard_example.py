"""Dashboard contract tests: real isolated CLI JSON and fixture-light TUI behavior.

Session rows, conversation and status come from subprocess-invoked ccx, never
hand-written dataclass/old-schema approximations. No native provider is run.
"""
from __future__ import annotations

import importlib.util
import io
import json
import os
import pathlib
import subprocess
import sys
import threading
import time
from types import SimpleNamespace

import pytest

from ccx import constants
from ccx.store import paths
from test_generation_store import A, B, C, short_root
from test_worker_discovery import D
from test_worker_telemetry import original_sources, _claude_entries, _append_jsonl

_DASH_PATH = pathlib.Path(__file__).resolve().parent.parent / "examples" / "dashboard.py"
_spec = importlib.util.spec_from_file_location("dashboard_example", _DASH_PATH)
dash = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(dash)

class FakeStdscr:
    """Minimal stdscr-like object — satisfies App.__init__ without real curses."""
    def __init__(self, keys=None):
        self._keys = list(keys or [])

    def getmaxyx(self):
        return (40, 120)

    def getch(self):
        if self._keys:
            return self._keys.pop(0)
        return -1

    def erase(self):
        pass

    def noutrefresh(self):
        pass

    def timeout(self, _):
        pass


class CaptureStdscr(FakeStdscr):
    """stdscr-like object that records addstr() writes so render output can be
    asserted without a real terminal."""
    def __init__(self, keys=None, size=(40, 120)):
        super().__init__(keys=keys)
        self._size = size
        self.writes: list[tuple[int, int, str]] = []

    def getmaxyx(self):
        return self._size

    def addstr(self, y, x, text, attr=0):
        self.writes.append((y, x, text))

    def text(self) -> str:
        return "\n".join(t for _, _, t in self.writes)


def make_app(**overrides):
    """Return an App instance with a fake stdscr. Workers are created but NOT started."""
    app = dash.App(FakeStdscr(), "ccx", "active")
    for k, v in overrides.items():
        setattr(app, k, v)
    return app


def wait_until(predicate, *, timeout=2.0, interval=0.02, reason="condition"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    pytest.fail(f"timed out waiting for {reason}")


def test_fmt_age_none():
    assert dash.fmt_age(None) == "—"


def test_fmt_age_zero():
    # fmt_age uses `if not ms` so 0 is treated as falsy
    assert dash.fmt_age(0) == "—"


def test_fmt_age_now(monkeypatch):
    now = 1_700_000_000.0
    monkeypatch.setattr(dash.time, "time", lambda: now)
    assert dash.fmt_age(int(now * 1000)) == "now"


def test_fmt_age_seconds(monkeypatch):
    now = 1_700_000_000.0
    monkeypatch.setattr(dash.time, "time", lambda: now)
    assert dash.fmt_age(int((now - 45) * 1000)) == "45s"


def test_fmt_age_minutes(monkeypatch):
    now = 1_700_000_000.0
    monkeypatch.setattr(dash.time, "time", lambda: now)
    assert dash.fmt_age(int((now - 130) * 1000)) == "2m"


def test_fmt_age_hours(monkeypatch):
    now = 1_700_000_000.0
    monkeypatch.setattr(dash.time, "time", lambda: now)
    assert dash.fmt_age(int((now - 7200) * 1000)) == "2h"


def test_fmt_age_days(monkeypatch):
    now = 1_700_000_000.0
    monkeypatch.setattr(dash.time, "time", lambda: now)
    assert dash.fmt_age(int((now - 3 * 86400) * 1000)) == "3d"


def test_fmt_age_old_timestamp(monkeypatch):
    now = 1_700_000_000.0
    monkeypatch.setattr(dash.time, "time", lambda: now)
    # 1 ms since epoch → many years ago → rendered as days
    result = dash.fmt_age(1)
    assert result.endswith("d")


def test_fmt_secs_none():
    assert dash.fmt_secs(None) == "—"


def test_fmt_secs_zero():
    assert dash.fmt_secs(0) == "0s"


def test_fmt_secs_seconds():
    assert dash.fmt_secs(30) == "30s"


def test_fmt_secs_minutes():
    assert dash.fmt_secs(90) == "1m"


def test_fmt_secs_hours():
    assert dash.fmt_secs(3601) == "1h"


def test_fmt_secs_days():
    assert dash.fmt_secs(86400) == "1d"


def test_fmt_tokens_zero():
    assert dash.fmt_tokens(0) == "0"


def test_fmt_tokens_none():
    assert dash.fmt_tokens(None) == "—"


def test_fmt_tokens_small():
    assert dash.fmt_tokens(999) == "999"


def test_fmt_tokens_kilo():
    assert dash.fmt_tokens(1500) == "1.5k"


def test_fmt_tokens_mega():
    assert dash.fmt_tokens(1_500_000) == "1.5M"


def test_fit_cells_counts_wide_glyphs_as_two_columns():
    assert dash.fit_cells("abc", 5) == "abc"
    assert dash.fit_cells("日本語テキスト", 5) == "日本"
    assert dash.fit_cells("ok❓x", 3) == "ok"
    assert dash.fit_cells("e\u0301tat", 3) == "e\u0301ta"  # combining accent takes no cell


class _FakeScreen:
    def __init__(self, h, w):
        self.h, self.w, self.writes = h, w, []

    def getmaxyx(self):
        return self.h, self.w

    def addstr(self, y, x, text, attr=0):
        self.writes.append((y, x, text))


def test_put_never_overflows_row_or_bottom_right_cell():
    app = object.__new__(dash.App)
    app.stdscr = _FakeScreen(10, 8)
    app.put(2, 3, "日本語テキスト")
    app.put(9, 0, "x" * 20)
    (y1, x1, t1), (y2, x2, t2) = app.stdscr.writes
    assert sum(dash.cell_width(c) for c in t1) <= 8 - 3
    assert len(t2) == 7  # last row stops one cell short


def test_clean_none():
    assert dash.clean(None) == ""


def test_clean_empty():
    assert dash.clean("") == ""


def test_clean_whitespace():
    assert dash.clean("  hello   world  ") == "hello world"


def test_clean_newlines():
    assert dash.clean("hello\nworld") == "hello world"


def test_clean_limit_truncates():
    result = dash.clean("hello world extra", limit=8)
    assert len(result) <= 8
    assert result.endswith("…")


def test_clean_limit_fits():
    assert dash.clean("hi", limit=10) == "hi"


def test_state_style_idle():
    col, glyph = dash.state_style("idle")
    assert col == dash.C_GREEN
    assert glyph == "●"


def test_state_style_waiting():
    col, _ = dash.state_style("waiting")
    assert col == dash.C_MAGENTA


def test_state_style_failed():
    col, glyph = dash.state_style("failed")
    assert col == dash.C_RED
    assert glyph == "✗"


def test_state_style_unknown():
    col, glyph = dash.state_style("completely-unknown-state")
    assert col == dash.C_DEFAULT
    assert glyph == "?"


def test_state_style_empty():
    col, glyph = dash.state_style("")
    assert col == dash.C_DEFAULT
    assert glyph == "?"


def test_state_style_all_known():
    for state in dash.STATE_STYLE:
        col, glyph = dash.state_style(state)
        assert col in range(8)
        assert glyph


def test_fmt_event_state():
    result = dash.App.fmt_event({"kind": "state", "type": "state", "from": "idle", "to": "busy"})
    assert "idle" in result and "busy" in result


def test_fmt_event_turn_complete():
    result = dash.App.fmt_event({"kind": "turn.ended", "turn_ref": "turn-5", "outcome": "completed"})
    assert "completed" in result and "turn ended" in result


def test_fmt_event_permission():
    result = dash.App.fmt_event({"kind": "input.requested", "request_id": "permission-1"})
    assert "permission-1" in result


def test_fmt_event_budget_stopped():
    result = dash.App.fmt_event({"kind": "error", "message": "token budget stopped"})
    assert "budget" in result and "token" in result


def test_fmt_event_idle_killed():
    result = dash.App.fmt_event({"kind": "state", "from": "idle", "to": "idle-killed"})
    assert "idle-killed" in result


def test_fmt_event_unknown_underscores_replaced():
    result = dash.App.fmt_event({"type": "some_other_event"})
    assert "_" not in result


def test_cell_wrap_empty_and_wide_text_respects_terminal_cells():
    assert dash.wrap_cells("", 20) == [(0, "")]
    assert dash.wrap_cells("hello world", 40) == [(0, "hello world")]
    chunks = dash.wrap_cells("日本語 テキスト abc", 5)
    assert len(chunks) > 1
    assert all(sum(dash.cell_width(c) for c in line) <= 5 for _, line in chunks)
    assert dash.wrap_cells("superlongword", 5) == [
        (0, "super"), (5, "longw"), (10, "ord")]


def test_sync_selection_empty_rows():
    app = make_app()
    app.rows = []
    app.sel = 3
    app.sel_short = "abc"
    app._sync_selection()
    assert app.sel == 0
    assert app.sel_short is None


def test_gauge_returns_three_segments():
    segs = make_app().gauge("5h", 50.0, width=10)
    assert len(segs) == 3


def test_gauge_label_in_first_segment():
    segs = make_app().gauge("7d", 40.0, width=10)
    assert segs[0][0].startswith("7d")


def test_gauge_pct_in_last_segment():
    segs = make_app().gauge("opus", 75.0, width=10)
    assert "75" in segs[2][0]


def test_gauge_full_bar_no_dots():
    segs = make_app().gauge("x", 100.0, width=8)
    bar = segs[1][0]
    assert "·" not in bar and len(bar) == 8


def test_gauge_empty_bar_no_blocks():
    segs = make_app().gauge("x", 0.0, width=8)
    bar = segs[1][0]
    assert "█" not in bar and len(bar) == 8


def test_gauge_partial_fill():
    segs = make_app().gauge("x", 50.0, width=10)
    bar = segs[1][0]
    assert "█" in bar and "·" in bar


class FakePopen:
    """Minimal subprocess.Popen-alike for StreamTailer tests."""
    def __init__(self, lines: list[str]):
        self.stdout = io.StringIO("\n".join(lines) + "\n")
        self._killed = False
        self._waited = False

    def poll(self):
        return None if not self._killed else 0

    def terminate(self):
        self._killed = True

    def wait(self, timeout=None):
        self._waited = True
        return 0


def test_stream_tailer_popen_injection_accumulates_events():
    events_ndjson = [
        json.dumps({"kind": "state", "type": "state", "from": "idle", "to": "busy"}),
        json.dumps({"type": "turn_complete", "turn": 1}),
    ]

    def fake_popen(cmd, *, stdin, stdout, stderr, text, bufsize):
        assert stdin == subprocess.DEVNULL
        return FakePopen(events_ndjson)

    tailer = dash.StreamTailer("ccx", popen=fake_popen)
    tailer.set_target("abc")
    wait_until(lambda: len(tailer.get("abc")) == 2,
               reason="stream tailer events")

    events = tailer.get("abc")
    assert len(events) == 2
    types = {e["type"] for e in events}
    assert types == {"state", "turn_complete"}


def test_stream_tailer_events_have_ts():
    def fake_popen(cmd, *, stdin, stdout, stderr, text, bufsize):
        assert stdin == subprocess.DEVNULL
        return FakePopen([json.dumps({"type": "state", "from": "a", "to": "b"})])

    tailer = dash.StreamTailer("ccx", popen=fake_popen)
    tailer.set_target("x")
    wait_until(lambda: bool(tailer.get("x")),
               reason="stream tailer timestamped event")
    events = tailer.get("x")
    assert events and "_ts" in events[0]


def test_stream_tailer_get_wrong_short_returns_empty():
    tailer = dash.StreamTailer("ccx")
    assert tailer.get("wrong") == []


def test_stream_tailer_set_target_none_clears_events():
    def fake_popen(cmd, *, stdin, stdout, stderr, text, bufsize):
        assert stdin == subprocess.DEVNULL
        return FakePopen([json.dumps({"type": "turn_complete", "turn": 1})])

    tailer = dash.StreamTailer("ccx", popen=fake_popen)
    tailer.set_target("abc")
    wait_until(lambda: bool(tailer.get("abc")),
               reason="stream tailer event before clear")

    tailer.set_target(None)
    assert tailer.get("abc") == []


def test_stream_tailer_retarget_replaces_events():
    call_count = [0]

    def fake_popen(cmd, *, stdin, stdout, stderr, text, bufsize):
        assert stdin == subprocess.DEVNULL
        call_count[0] += 1
        return FakePopen([json.dumps({"type": "state", "from": "a", "to": "b", "call": call_count[0]})])

    tailer = dash.StreamTailer("ccx", popen=fake_popen)
    tailer.set_target("s1")
    wait_until(lambda: bool(tailer.get("s1")),
               reason="first stream target event")
    tailer.set_target("s2")
    wait_until(lambda: bool(tailer.get("s2")),
               reason="second stream target event")

    assert tailer.get("s1") == []
    events = tailer.get("s2")
    assert events


def test_stream_tailer_stale_reader_drops_events():
    """Old reader cannot inject events into a new session's deque (fix #1)."""
    # The old stream blocks until terminate() is called (i.e. until after retarget).
    # This lets us verify that its single event is discarded rather than appended
    # to the new session's deque.
    old_reader_unblocked = threading.Event()

    class _BlockingPopen:
        """Blocks stdout iteration until terminate() is called."""
        def __init__(self):
            self._killed = False

        @property
        def stdout(self):
            return self

        def __iter__(self):
            old_reader_unblocked.wait(timeout=3.0)
            if not self._killed:
                yield json.dumps({"type": "stale_event", "marker": "stale"})

        def poll(self):
            return 0 if self._killed else None

        def terminate(self):
            self._killed = True
            old_reader_unblocked.set()

        def wait(self, timeout=None):
            pass

    class _FastPopen:
        def __init__(self):
            self.stdout = io.StringIO(
                json.dumps({"type": "fresh_event", "marker": "fresh"}) + "\n"
            )
            self._killed = False

        def poll(self):
            return None

        def terminate(self):
            self._killed = True

        def wait(self, timeout=None):
            pass

    call_n = [0]

    def fake_popen(cmd, *, stdin, stdout, stderr, text, bufsize):
        assert stdin == subprocess.DEVNULL
        call_n[0] += 1
        return _BlockingPopen() if call_n[0] == 1 else _FastPopen()

    tailer = dash.StreamTailer("ccx", popen=fake_popen)
    tailer.set_target("s1")        # starts _BlockingPopen — blocks in __iter__
    time.sleep(0.05)
    tailer.set_target("s2")        # terminate()s the blocking popen, starts _FastPopen
    wait_until(lambda: any(e.get("marker") == "fresh" for e in tailer.get("s2")),
               reason="fresh stream event after retarget")

    events = tailer.get("s2")
    markers = {e.get("marker") for e in events}
    assert "stale" not in markers, "stale event from old reader must not appear in new deque"
    assert "fresh" in markers, "fresh event from new reader must appear"


def test_stream_tailer_retarget_reaps_old_subprocess():
    """set_target() calls wait() on the old process to prevent zombies (fix #2)."""
    popens = []

    def fake_popen(cmd, *, stdin, stdout, stderr, text, bufsize):
        assert stdin == subprocess.DEVNULL
        p = FakePopen([json.dumps({"type": "turn_complete", "turn": 1})])
        popens.append(p)
        return p

    tailer = dash.StreamTailer("ccx", popen=fake_popen)
    tailer.set_target("s1")
    wait_until(lambda: bool(tailer.get("s1")),
               reason="old stream event before retarget")
    tailer.set_target("s2")
    wait_until(lambda: popens[0]._waited, reason="old stream process reap")
    tailer.stop()

    assert popens, "expected at least one Popen"
    assert popens[0]._waited, "old process must be reaped via wait()"


def test_stream_tailer_stop_reaps_subprocess():
    """stop() calls wait() on the running process (fix #2)."""
    popens = []

    def fake_popen(cmd, *, stdin, stdout, stderr, text, bufsize):
        assert stdin == subprocess.DEVNULL
        p = FakePopen([json.dumps({"type": "turn_complete", "turn": 1})])
        popens.append(p)
        return p

    tailer = dash.StreamTailer("ccx", popen=fake_popen)
    tailer.set_target("abc")
    wait_until(lambda: bool(tailer.get("abc")),
               reason="stream event before stop")
    tailer.stop()  # blocking _reap

    assert popens and popens[0]._waited, "process must be reaped on stop()"


def test_main_json_without_once_errors(monkeypatch, capsys):
    """--json without --once must exit 2 with a clear error on stderr."""
    monkeypatch.setattr(
        dash, "run_ccx",
        lambda *a, **kw: type("CP", (), {"returncode": 0, "stdout": "", "stderr": ""})(),
    )

    rc = dash.main(["--json"])
    captured = capsys.readouterr()
    assert rc == 2
    assert "--json requires --once" in captured.err


def test_app_run_tolerates_curs_set_error(monkeypatch):
    """Some PTYs reject curs_set(); the dashboard should still start and quit."""
    class WorkerStub:
        def __init__(self):
            self.stop_event = threading.Event()
        def start(self):
            pass
        def kick(self):
            pass
        def wake(self):
            pass
        def stop(self):
            pass

    app = dash.App(FakeStdscr(keys=[ord("q")]), "ccx", "active")
    app.poller = WorkerStub()
    app.peeker = WorkerStub()
    app.streamer = WorkerStub()
    monkeypatch.setattr(app, "init_colors", lambda: None)
    monkeypatch.setattr(app, "draw", lambda: None)
    monkeypatch.setattr(dash.curses, "curs_set", lambda _value: (_ for _ in ()).throw(dash.curses.error()))

    app.run()


def test_main_tui_non_tty_exits_2(monkeypatch, capsys):
    """TUI mode (no --once) exits 2 with helpful stderr when stdout is not a tty.

    In the standard pytest environment sys.stdout is not a tty; no extra
    monkeypatching of isatty() is required.
    """
    monkeypatch.setattr(
        dash, "run_ccx",
        lambda *a, **kw: type("CP", (), {"returncode": 0, "stdout": "", "stderr": ""})(),
    )
    rc = dash.main([])
    captured = capsys.readouterr()
    assert rc == 2
    assert "terminal" in captured.err or "tty" in captured.err


def test_fmt_age_future_timestamp(monkeypatch):
    """A timestamp in the future clamps the delta to 0 and renders as 'now'."""
    now = 1_700_000_000.0
    monkeypatch.setattr(dash.time, "time", lambda: now)
    future_ms = int((now + 30) * 1000)  # 30 seconds ahead
    assert dash.fmt_age(future_ms) == "now"


def test_main_ccx_not_found_exits_2(monkeypatch, capsys):
    """When ccx is not executable (rc=127), main() exits 2 with helpful stderr."""
    monkeypatch.setattr(
        dash, "run_ccx",
        lambda *a, **kw: type("CP", (), {"returncode": 127, "stdout": "", "stderr": "No such file"})(),
    )
    rc = dash.main(["--once"])
    captured = capsys.readouterr()
    assert rc == 2
    assert "ccx" in captured.err
    assert "--ccx-bin" in captured.err or "$CCX_BIN" in captured.err


def test_main_ccx_probe_non_zero_exits_2(monkeypatch, capsys):
    """When the ccx probe exits with rc not in {0, 1}, main() exits 2 regardless of mode."""
    monkeypatch.setattr(
        dash, "run_ccx",
        lambda *a, **kw: type("CP", (), {"returncode": 2, "stdout": "", "stderr": "fatal"})(),
    )
    rc = dash.main([])
    captured = capsys.readouterr()
    assert rc == 2
    assert "ccx" in captured.err
    assert "--ccx-bin" in captured.err or "$CCX_BIN" in captured.err


# --------------------------------------------------------------------------- #
# Real JSON contracts: only ccx CLI subprocesses produce session/peek/status.
# --------------------------------------------------------------------------- #

@pytest.fixture
def source_cli(original_sources):
    """Genuine ACTIVE generation + synthetic native JSONL, zero live providers."""
    from test_generation_store import add_worker
    from ccx.ops.creation_steps import prepare_worker

    store, cwd = original_sources
    native = pathlib.Path(store.record(A)["transcript"])
    native.write_bytes((pathlib.Path(__file__).parent / "fixtures" /
                        "claude_conversation.jsonl").read_bytes())
    native.chmod(0o600)
    # Native transcript extension (not a fabricated CLI response): a Bash
    # tool whose returned result is failed, so both status glyphs are exercised.
    _append_jsonl(native,
                  {"type": "user", "timestamp": "2026-10-21T01:00:00Z",
                   "message": {"role": "user", "content": "Run failing command"}},
                  {"type": "assistant", "timestamp": "2026-10-21T01:00:01Z",
                   "message": {"id": "response-failed", "model": "claude-fake",
                               "content": [{"type": "tool_use", "id": "toolu_dashboard_failed",
                                            "name": "Bash", "input": {"command": "false"}}]}},
                  {"type": "user", "timestamp": "2026-10-21T01:00:02Z",
                   "message": {"role": "user", "content": [
                       {"type": "tool_result", "tool_use_id": "toolu_dashboard_failed",
                        "is_error": True, "content": "exit status 1"}]}})
    _append_jsonl(pathlib.Path(store.record(B)["transcript"]),
                  *_claude_entries("other-worker", 3))
    store.mutate_record(A, patches={
        "name": "Alpha dashboard",
        "lifecycle.state": "waiting",
        "waitingFor": {"id": "question-uuid-1", "tool": "AskUserQuestion",
                       "input": {"questions": [
                           {"question": "Deploy now?", "options": [
                               {"label": "Yes"}, {"label": "No"}]}]}},
        "stats.turns": 3, "stats.inputTokens": 1200,
        "stats.outputTokens": 300,
    })
    store.mutate_record(B, patches={
        "name": "Beta dashboard", "lifecycle.state": "idle",
        "stats.turns": 2, "stats.inputTokens": 42,
        "stats.outputTokens": 17,
    })
    # Claude per-record token counters are inert for every row, even when old
    # records still carry the fields. The dashboard must not invent zeroes.
    add_worker(constants.CCX_ROOT, C, "claude", terminal=True)
    # A real codex row from the same sealed generation, no Codex provider.
    prepare_worker(store, engine="codex", cwd=str(cwd), worker_id=D)
    store.mutate_record(D, patches={
        "name": "Codex dashboard", "lifecycle.state": "idle", "stats.turns": 1,
        "context": {"tokens_used": 123456, "window_size": 200000,
                    "percent_used": 61.728, "as_of_turn_ref": None, "as_of_ms": 1234},
    })
    from ccx.claude.transcript import render_transcript_json
    render_transcript_json(A, store=store)  # compact boundary: no stale tokens
    render_transcript_json(B, store=store)  # normal measured Claude context

    def cli(*args):
        proc = subprocess.run(
            [sys.executable, "-m", "ccx", *args],
            env={**os.environ, "CCX_ROOT": str(constants.CCX_ROOT),
                 "CLAUDE_CONFIG_DIR": str(paths.CLAUDE_HOME)},
            text=True, capture_output=True, timeout=20, check=False,
        )
        assert proc.returncode == 0, (args, proc.stdout, proc.stderr)
        return json.loads(proc.stdout)

    def runner(_binary, argv, *, timeout=20.0):
        if argv[0] == "usage":
            return {}, None  # quota endpoint excluded; would require a provider
        return cli(*argv), None

    rows = cli("list", "--json")
    rich = cli("peek", A, "--json", "--tail", "8")
    logs = cli("logs", A, "--tail", "60")
    status = cli("peek", A, "--status", "--json")["statuses"][0]
    return SimpleNamespace(store=store, cwd=cwd, cli=cli, runner=runner,
                           rows=rows, rich=rich, logs=logs, status=status,
                           a=next(r for r in rows if r["worker_id"] == A),
                           b=next(r for r in rows if r["worker_id"] == B),
                           c=next(r for r in rows if r["worker_id"] == C),
                           d=next(r for r in rows if r["worker_id"] == D))


def test_cli_list_rows_match_dashboard_without_shims(source_cli):
    rows, err = dash._list_rows("ccx", "all", source_cli.runner)
    assert err is None and rows == source_cli.rows
    for row in rows:
        assert row["worker"] == f"{row['engine']}:{row['worker_id']}"
        assert row["state_source"] == "recorded"
        assert not {"mode", "can_send", "can_import", "cost_usd"} & row.keys()
    assert "input_tokens" not in source_cli.a and "output_tokens" not in source_cli.a
    assert "input_tokens" not in source_cli.c and "output_tokens" not in source_cli.c
    assert source_cli.d["engine"] == "codex" and source_cli.d["worker"] == f"codex:{D}"
    assert (source_cli.d["input_tokens"], source_cli.d["output_tokens"]) == (None, None)


def test_cli_active_scope_filters_recorded_only(source_cli):
    rows, err = dash._list_rows("ccx", "active", source_cli.runner)
    assert err is None
    assert {r["worker"] for r in rows} <= {r["worker"] for r in source_cli.rows}
    assert source_cli.a["worker"] in {r["worker"] for r in rows}
    assert dash.SCOPES == ["active", "all"]


def test_session_context_sort_and_null_are_from_real_status(source_cli):
    handles = [row["worker"] for row in source_cli.rows]
    statuses, err = dash.fetch_statuses("ccx", handles, source_cli.runner)
    assert err is None
    app = make_app(sort="context")
    rows = app.filtered_rows(source_cli.rows, statuses)
    assert rows[0]["worker_id"] == D
    assert dash.context_tokens(rows[0]) == 123456
    assert dash.context_tokens(next(r for r in rows if r["worker_id"] == B)) == 3
    assert dash.context_tokens(next(r for r in rows if r["worker_id"] == C)) is None
    assert dash.fmt_tokens(dash.context_tokens(rows[0])) == "123.5k"
    assert "tokens" not in dash.SORTS and "context" in dash.SORTS


def test_sort_name_turns_state_age_and_search_from_cli(source_cli):
    app = make_app()
    app.sort = "name"
    assert [r["name"] for r in app.filtered_rows(source_cli.rows)] == sorted(
        [r["name"] for r in source_cli.rows], key=str.lower)
    app.sort = "turns"
    assert [r["turns"] for r in app.filtered_rows(source_cli.rows)][:2] == [3, 2]
    app.sort = "state"
    assert app.filtered_rows(source_cli.rows)[0]["worker_id"] == A
    app.search = "Alpha"
    assert [r["worker_id"] for r in app.filtered_rows(source_cli.rows)] == [A]
    app.search = str(source_cli.b["short"])
    # A and B intentionally share an eight-character display prefix: the UI
    # selection and actions must rely on their full worker identities.
    assert {r["worker_id"] for r in app.filtered_rows(source_cli.rows)} == {A, B, D}
    app.search = "no-matching-name"
    assert app.filtered_rows(source_cli.rows) == []


def test_selection_reanchors_by_full_uuid_after_resort(source_cli):
    app = make_app()
    app.rows = [source_cli.a, source_cli.b, source_cli.c]
    app.sel = 1
    app.sel_short = source_cli.b["worker"]
    app.rows.reverse()
    app._sync_selection()
    assert app.selected_row()["worker"] == source_cli.b["worker"]
    app._move(-1)
    assert app.sel_short == app.selected_row()["worker"]


def test_list_snapshot_actual_json_and_nulls(source_cli):
    output = dash.snapshot_json("ccx", "all", runner=source_cli.runner)
    assert json.loads(json.dumps(output)) == output
    assert output["scope"] == "all"
    assert len(output["sessions"]) == 4
    assert output["attention_count"] == 1
    assert output["live_count"] == 1
    rows = {r["worker_id"]: r for r in output["sessions"]}
    assert rows[A]["tokens"] is None
    assert rows[A]["input_tokens"] is None
    assert rows[A]["output_tokens"] is None
    assert rows[C]["tokens"] is None and rows[C]["turns"] is None
    assert rows[D]["tokens"] is None and rows[D]["turns"] == 1
    assert rows[D]["engine"] == "codex"
    assert not {"mode", "cost_usd", "can_send", "can_import"} & rows[A].keys()


def test_snapshot_text_has_no_cost_and_preserves_null(source_cli):
    output = dash.render_snapshot("ccx", "all", runner=source_cli.runner)
    assert "CONTEXT" in output and "TOKENS" not in output and "COST" not in output
    assert "123.5k" in output
    assert source_cli.c["short"] in output
    row = next(line for line in output.splitlines() if source_cli.c["short"] in line)
    assert "—" in row


def test_peek_cli_has_tools_and_chronological_engine_error(source_cli):
    peek = source_cli.rich
    assert "errors" not in peek and "recent_tools" not in peek
    assert peek["pending_input"] == {"kind": "question", "request_id": "question-uuid-1"}
    assert peek["waiting_for"]["tool"] == "AskUserQuestion"
    assert peek["tools"]
    assert peek["tools"][0]["status"] == "completed"
    assert peek["tools"][0]["summary"] == {"verb": "Ran", "target": "echo hi"}
    assert any(item["kind"] == "error" and item["message"] == "API Error: overloaded"
               for item in peek["recent_items"])
    assert dash.tool_summary(peek["tools"][0]) == "Bash Ran echo hi"
    assert "{" not in dash.tool_summary(peek["tools"][0])


def test_peek_status_context_parity_both_engines_and_compaction_fences(source_cli):
    from ccx.workers import WorkerClient
    from ccx.client import CcxClient
    handles = [row["worker"] for row in source_cli.rows]
    statuses, err = dash.fetch_statuses("ccx", handles, source_cli.runner)
    assert err is None and set(statuses) == set(handles)
    keys = {"tokens_used", "window_size", "percent_used", "reason",
            "as_of_ms", "as_of_turn_ref"}
    for row in statuses.values():
        assert set(row["context"]) == keys
        assert WorkerClient().status(row["worker"])["context"] == row["context"]
    assert CcxClient().status([source_cli.b["worker"]])[0].context == statuses[source_cli.b["worker"]]["context"]
    assert statuses[source_cli.b["worker"]]["context"]["tokens_used"] == 3
    assert statuses[source_cli.b["worker"]]["context"]["window_size"] is None
    assert statuses[source_cli.d["worker"]]["context"] == {
        "tokens_used": 123456, "window_size": 200000, "percent_used": 61.728,
        "reason": None, "as_of_ms": 1234, "as_of_turn_ref": None,
    }
    assert statuses[source_cli.c["worker"]]["context"]["reason"] == "no-signal"
    assert statuses[source_cli.c["worker"]]["context"]["tokens_used"] is None
    # Last assistant after the compact boundary has no measured input usage.
    assert statuses[source_cli.a["worker"]]["context"]["reason"] in {"compacting", "just_compacted", "pending"}
    assert statuses[source_cli.a["worker"]]["context"]["tokens_used"] is None
    source_cli.store.mutate_record(D, patches={"activeCompact": {"id": "fake-compact"}})
    fenced = source_cli.cli("peek", D, "--status", "--json")["statuses"][0]["context"]
    assert fenced["tokens_used"] is None and fenced["percent_used"] is None
    assert fenced["reason"] == "compacting" and fenced["window_size"] == 200000


def test_peek_status_text_context_from_real_cli(source_cli):
    proc = subprocess.run(
        [sys.executable, "-m", "ccx", "peek", D, "--status"],
        env={**os.environ, "CCX_ROOT": str(constants.CCX_ROOT),
             "CLAUDE_CONFIG_DIR": str(paths.CLAUDE_HOME)},
        capture_output=True, text=True, check=True,
    )
    assert "ctx=123k" in proc.stdout


def test_status_batch_is_capped_and_makes_one_cli_call():
    calls = []
    handles = [f"claude:{i:08x}-1234-4000-8000-{i:012x}" for i in range(50)]
    def runner(_bin, argv, **kw):
        calls.append(argv)
        return {"statuses": [{"worker": w, "context": {"tokens_used": i}}
                             for i, w in enumerate(argv[1:-2])]}, None
    statuses, err = dash.fetch_statuses("ccx", handles, runner)
    assert err is None and len(statuses) == dash.STATUS_BATCH_LIMIT == 40
    assert len(calls) == 1 and calls[0][0] == "peek" and calls[0][-2:] == ["--status", "--json"]


def test_status_cli_owns_pending_question_and_request_id(source_cli):
    status = source_cli.status
    assert "statuses" not in status
    assert status["pending_input"] == {"kind": "question", "request_id": "question-uuid-1"}
    assert status["waiting_for"]["tool"] == "AskUserQuestion"
    assert dash.pending_input(status)["request_id"] == "question-uuid-1"
    assert dash.first_question_payload(status)["question"] == "Deploy now?"
    assert [o["label"] for o in dash.first_question_payload(status)["options"]] == ["Yes", "No"]
    assert dash.first_question_payload(source_cli.rich) == dash.first_question_payload(status)


def test_detail_renders_real_schema5_conversation_and_pending_header(source_cli):
    screen = CaptureStdscr(size=(40, 140))
    app = make_app(stdscr=screen)
    app.store.update_statuses({source_cli.a["worker"]: source_cli.status}, None)
    app.store.update_conversation(source_cli.a["worker"], source_cli.logs)
    app.draw_detail(0, 34, 140, app.store.snapshot(), source_cli.a)
    drawn = screen.text()
    assert "Deploy now?" in drawn
    assert "pending: question" in drawn
    assert "Bash Ran echo hi" in drawn
    assert "API Error: overloaded" in drawn
    assert "tool:" in drawn and "assistant:" in drawn and "turn" in drawn
    assert "{'verb':" not in drawn


def test_detail_renders_failed_tool_marker_from_real_logs(source_cli):
    assert source_cli.logs["schemaVersion"] == 5
    lines = dash.conversation_entries(source_cli.logs)
    labels = "\n".join(row[1] for row in lines)
    assert "✗ tool: Bash" in labels and "✓ tool: Bash" in labels
    assert "✗ error: API Error: overloaded" in labels


def test_conversation_scroll_follow_refresh_anchor_and_rewrap_real_logs(source_cli):
    view = dash.ConversationView()
    worker = source_cli.a["worker"]
    view.configure(worker, source_cli.logs, 32, 5)
    assert view.follow and view.top == view._max_top() > 5
    view.scroll(-6)
    assert not view.follow
    old_anchor = view.anchor()

    # Grow the genuine native transcript, then fetch via real ccx logs --json.
    native = pathlib.Path(source_cli.store.record(A)["transcript"])
    _append_jsonl(native, *_claude_entries("dashboard-refresh", 17))
    grown = source_cli.cli("logs", A, "--tail", "60")
    assert grown["schemaVersion"] == 5 and grown != source_cli.logs
    view.configure(worker, grown, 32, 5)
    assert view.anchor() == old_anchor and not view.follow
    view.configure(worker, grown, 18, 7)
    assert view.anchor()[0] == old_anchor[0]
    assert view.anchor()[1] <= old_anchor[1]
    view.end()
    assert view.follow and view.top == view._max_top()
    view.configure(worker, source_cli.cli("logs", A, "--tail", "60"), 28, 7)
    assert view.top == view._max_top()
    view.home()
    assert view.top == 0 and not view.follow


def test_page_keys_scroll_conversation_no_pause_or_zoom(source_cli):
    app = make_app()
    app.rows = [source_cli.a]
    app.conversation_view.configure(source_cli.a["worker"], source_cli.logs, 22, 6)
    app.detail_page_rows = 6
    bottom = app.conversation_view.top
    app.handle_key(dash.curses.KEY_PPAGE)
    assert app.conversation_view.top < bottom
    app.handle_key(dash.curses.KEY_NPAGE)
    assert app.conversation_view.top == bottom
    app.handle_key(dash.curses.KEY_HOME)
    assert app.conversation_view.top == 0
    app.handle_key(dash.curses.KEY_END)
    assert app.conversation_view.follow
    app.handle_key(ord("p"))
    assert not hasattr(app, "paused") and not hasattr(app.poller, "set_paused")
    screen = CaptureStdscr()
    app.stdscr = screen
    app.draw_help(120, 40)
    assert "pause" not in screen.text().lower()


def test_contextual_footer_only_shows_valid_actions(source_cli):
    app = make_app()
    waiting = source_cli.a
    idle = source_cli.b
    app.rows = [waiting]
    question = source_cli.status
    app.store.update_statuses({waiting["worker"]: question}, None)
    hints = dash.footer_hints(waiting, question)
    assert "a answer" in hints and "A approve" not in hints
    assert "s send" not in hints and "K kill" in hints
    source_cli.store.mutate_record(A, patches={
        "waitingFor": {"id": "permission-uuid-7", "tool": "Bash", "input": {"command": "echo fixture"}},
    })
    permission = source_cli.cli("peek", A, "--status", "--json")["statuses"][0]
    hints = dash.footer_hints(waiting, permission)
    assert "A approve" in hints and "X reject" in hints
    assert "a answer" not in hints
    assert "s send" in dash.footer_hints(idle, None)
    assert "s send" not in dash.footer_hints(source_cli.c, None)
    screen = CaptureStdscr(size=(40, 240))
    app.stdscr = screen
    app.store.update_statuses({waiting["worker"]: permission}, None)
    app.draw_footer(39, 240, app.store.snapshot())
    assert "A approve" in screen.text() and "s send" not in screen.text()


def test_status_permission_gate_from_real_cli(source_cli):
    source_cli.store.mutate_record(A, patches={
        "waitingFor": {"id": "permission-uuid-7", "tool": "Bash",
                       "input": {"command": "echo fixture"}},
    })
    status = source_cli.cli("peek", A, "--status", "--json")["statuses"][0]
    assert dash.pending_input(status) == {"kind": "permission", "request_id": "permission-uuid-7"}
    assert dash.first_question_payload(status) is None


def test_approve_and_reject_use_real_status_request_id(source_cli, monkeypatch):
    source_cli.store.mutate_record(A, patches={
        "waitingFor": {"id": "permission-uuid-7", "tool": "Bash",
                       "input": {"command": "echo fixture"}},
    })
    status = source_cli.cli("peek", A, "--status", "--json")["statuses"][0]
    calls = []
    monkeypatch.setattr(dash, "run_ccx", lambda _binary, argv, **_kw:
                        calls.append(argv) or subprocess.CompletedProcess(argv, 0, "", ""))
    app = make_app()
    app.store.update_statuses({source_cli.a["worker"]: status}, None)
    app.action_permission(source_cli.a, approve=True)
    app.action_permission(source_cli.a, approve=False)
    wait_until(lambda: len(calls) == 2, reason="approve and reject CLI calls")
    assert [c[0] for c in calls] == ["approve", "reject"]
    for c in calls:
        assert c[1:] == [source_cli.a["worker"], "--json", "--request-id", "permission-uuid-7"]


def test_no_permission_request_id_prevents_action(source_cli, monkeypatch):
    calls = []
    monkeypatch.setattr(dash, "run_ccx", lambda *args, **kw: calls.append(args))
    app = make_app()
    app.store.update_statuses({source_cli.a["worker"]: source_cli.status}, None)
    app.action_permission(source_cli.a, approve=True)
    assert not calls
    assert "no permission request ID" in app.store.snapshot()["status_msg"]


@pytest.mark.parametrize("answer,expected", [
    ("2", ["--option", "1", "No"]),
    ("1", ["--option", "1", "Yes"]),
    ("5", ["5"]),
    ("Other", ["Other"]),
])
def test_answer_uses_real_question_and_request_id(source_cli, monkeypatch, answer, expected):
    calls = []
    app = make_app()
    app.store.update_statuses({source_cli.a["worker"]: source_cli.status}, None)
    monkeypatch.setattr(app, "prompt", lambda label: answer)
    monkeypatch.setattr(dash, "run_ccx", lambda _binary, argv, **_kw:
                        calls.append(argv) or subprocess.CompletedProcess(argv, 0, "", ""))
    app.action_answer(source_cli.a)
    wait_until(lambda: bool(calls), reason="answer CLI invocation")
    assert calls[0][:2] == ["answer", source_cli.a["worker"]]
    assert calls[0][2:-2] == expected
    assert calls[0][-2:] == ["--request-id", "question-uuid-1"]


def test_answer_refuses_nonfocused_or_missing_status(source_cli, monkeypatch):
    calls = []
    monkeypatch.setattr(dash, "run_ccx", lambda *args, **kw: calls.append(args))
    app = make_app()
    app.store.update_statuses({source_cli.b["worker"]: source_cli.status}, None)
    app.action_answer(source_cli.a)
    assert not calls
    assert "no question/request ID" in app.store.snapshot()["status_msg"]


def test_send_and_kill_use_full_worker_identity_without_fake_capability_flags(source_cli, monkeypatch):
    calls = []
    app = make_app()
    monkeypatch.setattr(app, "prompt", lambda _label: "hello")
    monkeypatch.setattr(app, "confirm", lambda _label: True)
    monkeypatch.setattr(dash, "run_ccx", lambda _binary, argv, **_kw:
                        calls.append(argv) or subprocess.CompletedProcess(argv, 0, "", ""))
    app.action_send(source_cli.b)
    app.action_kill(source_cli.b)
    wait_until(lambda: len(calls) >= 2, reason="full UUID action calls")
    assert {c[0] for c in calls} == {"send", "kill"}
    assert all(c[1] == source_cli.b["worker"] for c in calls)
    before = len(calls)
    app.action_send(source_cli.c)  # terminal-only; blocked locally
    assert len(calls) == before


def test_poller_batches_real_cli_status_and_logs_worker_reads_once(source_cli):
    calls = []
    def runner(binary, argv, **kw):
        calls.append(tuple(argv))
        return source_cli.runner(binary, argv, **kw)

    store = dash.Store()
    poller = dash.Poller("ccx", store, runner=runner)
    poller.set_scope("all")
    poller.set_visible([r["worker"] for r in source_cli.rows])
    poller.start()
    try:
        wait_until(lambda: len(store.snapshot()["statuses"]) == 4,
                   timeout=10.0,
                   reason="real CLI batched peek --status")
    finally:
        poller.stop_event.set()
        poller.kick()
        poller.join(timeout=2)
    assert store.snapshot()["sessions"] == source_cli.rows
    assert len([c for c in calls if c[0] == "peek"]) == 1
    assert store.snapshot()["statuses"][source_cli.d["worker"]]["context"]["tokens_used"] == 123456

    peeker = dash.ConversationWorker("ccx", store, runner=runner)
    peeker.start()
    peeker.set_focus(source_cli.a["worker"], ("waiting", 3, 1))
    try:
        wait_until(lambda: store.snapshot()["conversation"] is not None,
                   timeout=10.0,
                   reason="real CLI logs --tail 60")
        assert store.snapshot()["conversation"]["schemaVersion"] == 5
        assert store.snapshot()["conversation"]["turns"] == source_cli.logs["turns"]
        first = len([c for c in calls if c[0] == "logs"])
        time.sleep(0.1)
        assert len([c for c in calls if c[0] == "logs"]) == first
        peeker.set_focus(source_cli.a["worker"], ("waiting", 3, 2))
        wait_until(lambda: len([c for c in calls if c[0] == "logs"]) == first + 1,
                   reason="refetch on revision")
        peeker.wake()
        wait_until(lambda: len([c for c in calls if c[0] == "logs"]) == first + 2,
                   reason="force refresh")
    finally:
        peeker.stop_event.set()
        peeker.wake()
        peeker.join(timeout=2)
    assert all("--tail" in c for c in calls if c[0] == "logs")


def test_real_pty_resize_scroll_prompt_quit_offline(source_cli):
    """Real curses/PTY smoke; ccx points exclusively at the fake generation.

    Drive resize coalescing, conversation scrolling and a search prompt that
    survives another resize. No providers, daemon, or real worker is contacted.
    """
    import fcntl
    import pty
    import select
    import signal
    import struct
    import termios

    root = _DASH_PATH.parent.parent
    env = {**os.environ, "TERM": "xterm-256color",
           "CCX_ROOT": str(constants.CCX_ROOT),
           "CLAUDE_CONFIG_DIR": str(paths.CLAUDE_HOME),
           "PYTHONPATH": str(root),
           "CCX_CLAUDE_BIN": "__dashboard_no_claude_provider__",
           "CCX_CODEX_BIN": "__dashboard_no_codex_provider__",
           # ccx.py runs via its python3 shebang; use the test interpreter's.
           "PATH": os.pathsep.join([os.path.dirname(sys.executable),
                                    os.environ.get("PATH", "")])}
    master, slave = pty.openpty()
    proc = subprocess.Popen(
        [sys.executable, str(_DASH_PATH), "--ccx-bin", str(root / "ccx.py"),
         "--scope", "all"],
        stdin=slave, stdout=slave, stderr=slave, env=env, start_new_session=True,
    )
    os.close(slave)
    captured = bytearray()

    def drain(duration):
        until = time.monotonic() + duration
        while time.monotonic() < until and proc.poll() is None:
            ready, _, _ = select.select([master], [], [], min(0.04, until-time.monotonic()))
            if ready:
                try:
                    data = os.read(master, 65536)
                except OSError:
                    break
                captured.extend(data[:max(0, 1_000_000-len(captured))])

    def resize(rows, cols):
        fcntl.ioctl(master, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))
        if proc.poll() is None:
            os.kill(proc.pid, signal.SIGWINCH)

    try:
        resize(40, 120)
        # Wait for the first real ccx list to render; a loaded machine can
        # take seconds, and keys sent earlier hit the cooked terminal.
        until = time.monotonic() + 20
        while b"Alpha dashboard" not in captured and time.monotonic() < until:
            drain(0.1)
        assert b"Alpha dashboard" in captured, captured[-3000:].decode(errors="replace")
        for h, w in [(20, 60), (8, 26), (4, 12), (45, 160), (26, 90)]:
            resize(h, w)
            drain(0.07)
        os.write(master, b"\x1b[5~\x1b[6~\x1b[H\x1b[F")  # PgUp/PgDn/Home/End
        drain(0.20)
        os.write(master, b"/")  # search editor on bottom footer
        drain(0.15)
        resize(16, 50)
        drain(0.12)
        resize(30, 110)
        drain(0.18)
        os.write(master, b"\x1b")  # cancel search, then quit
        drain(0.25)
        os.write(master, b"q")
        # Keep reading while curses restores the terminal: endwin() drains its
        # output, and an unread macOS PTY fills long before a Linux one.
        drain(6)
        try:
            code = proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            pytest.fail("dashboard PTY did not quit after q")
        assert code == 0, captured[-3000:].decode(errors="replace")
        assert b"Traceback" not in captured
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=3)
        os.close(master)


def test_headless_entrypoint_once_and_json_with_real_list(source_cli, monkeypatch, capsys):
    monkeypatch.setattr(dash, "run_ccx", lambda *a, **kw:
                        subprocess.CompletedProcess(a, 0, "", ""))
    monkeypatch.setattr(dash, "run_ccx_json", source_cli.runner)
    assert dash.main(["--once", "--scope", "all"]) == 0
    rendered = capsys.readouterr().out
    assert "ccx dashboard snapshot" in rendered
    assert "COST" not in rendered and "123.5k" in rendered
    assert dash.main(["--once", "--scope", "all", "--json"]) == 0
    doc = json.loads(capsys.readouterr().out)
    assert {row["engine"] for row in doc["sessions"]} == {"claude", "codex"}
    assert len(doc["sessions"]) == 4


def test_snapshots_ignore_missing_usage_and_report_list_failure(source_cli):
    def missing_usage(_binary, argv, **_kw):
        if argv[0] == "list":
            return source_cli.cli(*argv), None
        return None, "offline quota unavailable"

    snap = dash.snapshot_json("ccx", "all", runner=missing_usage)
    assert snap["usage"] == {} and len(snap["sessions"]) == 4
    text = dash.render_snapshot("ccx", "all", runner=missing_usage)
    assert "USAGE: (no data)" in text

    def failed_list(_binary, argv, **_kw):
        return (None, "recorded generation unavailable") if argv[0] == "list" else ({}, None)

    failure = dash.snapshot_json("ccx", "all", runner=failed_list)
    assert failure["sessions"] == []
    assert "generation unavailable" in failure["error"]


def test_headless_usage_gauges_preserve_value_without_cost(source_cli):
    usage = {"five_hour": {"utilization": 43},
             "seven_day": {"utilization": 7},
             "model_quotas": [{"model": "opus", "utilization": 34}],
             "extra_usage": {"enabled": True, "used_credits": 1.5, "currency": "USD"}}

    def runner(_binary, argv, **_kw):
        if argv[0] == "usage":
            return usage, None
        return source_cli.cli(*argv), None

    snap = dash.snapshot_json("ccx", "all", runner=runner)
    # Fixture uses nonexistent CCX binaries; force just a fake Claude presence.
    # Actual engine/PATH detection is exercised separately below.
    # Without installed engines the one-shot mode never calls usage.
    assert snap["usage"] == {}
    assert "USAGE: (no data)" in dash.render_snapshot("ccx", "all", runner=runner)

# --------------------------------------------------------------------------- #
# Dual-engine usage: only CCX commands, always fake provider responses.
# --------------------------------------------------------------------------- #

_CLAUDE_QUOTA = {
    "five_hour": {"utilization": 43}, "seven_day": {"utilization": 7},
    "model_quotas": [{"model": "opus", "utilization": 34}],
    "extra_usage": {"enabled": True, "used_credits": 1.5, "currency": "USD"},
}
_CODEX_QUOTA = {
    "rateLimitsByLimitId": {"codex": {
        "primary": {"usedPercent": 31, "windowDurationMins": 300},
        "secondary": {"usedPercent": 8, "windowDurationMins": 10080},
    }},
}


def _fake_engines(monkeypatch, names):
    monkeypatch.delenv("CCX_CLAUDE_BIN", raising=False)
    monkeypatch.delenv("CCX_CODEX_BIN", raising=False)
    monkeypatch.setattr(dash.shutil, "which", lambda name:
                        f"/fake/{name}" if name in names else None)
    assert dash.available_engines() == tuple(e for e in dash.ENGINES if e in names)


@pytest.mark.parametrize("present", [(), ("claude",), ("codex",), ("claude", "codex")])
def test_usage_only_queries_installed_engines_and_renders_each(source_cli, monkeypatch, present):
    _fake_engines(monkeypatch, present)
    calls = []
    def runner(_bin, argv, *, timeout):
        calls.append(tuple(argv))
        if argv[0] == "list":
            return source_cli.rows, None
        if argv[0] == "peek":
            return source_cli.cli(*argv), None
        assert argv[0] == "usage"
        return (_CLAUDE_QUOTA if argv[argv.index("--engine") + 1] == "claude"
                else _CODEX_QUOTA), None
    snapshot = dash.snapshot_json("ccx", "all", runner=runner)
    assert set(snapshot["usage"]) == set(present)
    assert snapshot["usage_errors"] == {}
    assert [c for c in calls if c[0] == "usage"] == [tuple(dash.usage_args(e)) for e in present]
    assert "--json" in dash.usage_args("codex")
    assert "--cached" in dash.usage_args("claude")

    if "codex" in present:
        assert snapshot["usage"]["codex"]["windows"] == [
            {"label": "5h", "utilization": 31.0},
            {"label": "7d", "utilization": 8.0},
        ]
    if "claude" in present:
        assert snapshot["usage"]["claude"]["five_hour"]["utilization"] == 43
        assert snapshot["usage"]["claude"]["extra_usage"]["used_credits"] == 1.5

    text = dash.render_snapshot("ccx", "all", runner=runner)
    screen = CaptureStdscr(size=(40, 240))
    app = make_app(stdscr=screen)
    app.stdscr = screen
    app.store.set_usage_engines(present)
    for engine, usage in snapshot["usage"].items():
        app.store.update_usage(engine, usage, None)
    app.draw_usage(1, 240, app.store.snapshot())
    rendered = screen.text()
    for engine in dash.ENGINES:
        if engine in present:
            assert engine + " 5h" in text
            assert engine + " 5h" in rendered
        else:
            assert engine + " 5h" not in text
            assert engine + " 5h" not in rendered
    if not present:
        assert "no engine executables" in rendered
        assert "USAGE: (no data)" in text


def test_codex_quota_fallback_and_named_windows():
    data = {"rateLimits": {"primary": {"usedPercent": 24, "windowDurationMins": 60}}}
    assert dash.codex_quota_windows(data) == [{"label": "1h", "utilization": 24.0}]
    named = {"rateLimitsByLimitId": {"secondary-plan": {
        "limitName": "Foo", "primary": {"usedPercent": 12, "windowDurationMins": 30}}}}
    assert dash.codex_quota_windows(named) == [{"label": "30m Foo", "utilization": 12.0}]


def test_usage_auth_unavailable_shows_one_dim_warning_without_credentials(source_cli, monkeypatch):
    _fake_engines(monkeypatch, ("claude", "codex"))
    calls = []
    def runner(_bin, argv, *, timeout):
        calls.append(tuple(argv))
        if argv[0] == "list":
            return source_cli.rows, None
        if "codex" in argv:
            return None, "typed auth unavailable: not logged in"
        return _CLAUDE_QUOTA, None
    snap = dash.snapshot_json("ccx", "all", runner=runner)
    assert set(snap["usage"]) == {"claude"}
    assert snap["usage_errors"] == {"codex": "usage unavailable"}
    assert "auth" not in json.dumps(snap["usage_errors"])
    assert "codex: usage unavailable" in dash.render_snapshot("ccx", "all", runner=runner)
    screen = CaptureStdscr(size=(40, 240))
    app = make_app(stdscr=screen)
    app.stdscr = screen
    app.store.set_usage_engines(("claude", "codex"))
    app.store.update_usage("claude", snap["usage"]["claude"], None)
    app.store.update_usage("codex", None, "auth unavailable")
    app.draw_usage(1, 240, app.store.snapshot())
    assert "codex: usage unavailable" in screen.text()


def test_forced_refresh_never_exceeds_codex_five_minute_cadence(monkeypatch):
    monkeypatch.setattr(dash, "available_engines", lambda: ("claude", "codex"))
    calls = []
    lock = threading.Lock()
    def runner(_bin, argv, *, timeout):
        with lock:
            calls.append(tuple(argv))
        if argv[0] == "list":
            return [], None
        return (_CODEX_QUOTA if "codex" in argv else _CLAUDE_QUOTA), None
    def count(engine):
        with lock:
            return sum(c[0] == "usage" and engine in c for c in calls)

    store = dash.Store()
    poller = dash.Poller("ccx", store, runner=runner)
    poller.start()
    try:
        poller.kick()
        wait_until(lambda: count("codex") == 1, reason="first Codex quota attempt")
        for _ in range(3):
            poller.kick()
            time.sleep(0.06)
        wait_until(lambda: count("claude") >= 2, reason="forced cached Claude refresh")
        assert count("codex") == 1, "forced refresh must not spawn a second Codex app-server"
        # Simulate the monotonic clock crossing 300s without actually sleeping.
        poller._last_usage["codex"] -= dash.CODEX_USAGE_PERIOD_S + 1
        poller.kick()
        wait_until(lambda: count("codex") == 2, reason="next Codex quota after 300 seconds")
    finally:
        poller.stop_event.set()
        poller.kick()
        poller.join(timeout=2)
    assert dash.CODEX_USAGE_PERIOD_S >= 300


def test_codex_real_list_row_status_and_full_worker_actions(source_cli, monkeypatch):
    row = source_cli.d
    assert row["engine"] == "codex" and row["worker"] == f"codex:{D}"
    assert dash.token_total(row) is None
    status = source_cli.cli("peek", D, "--status", "--json")["statuses"][0]
    assert status["worker"] == row["worker"]
    screen = CaptureStdscr(size=(40, 240))
    app = make_app(stdscr=screen)
    app.stdscr = screen
    app.rows = source_cli.rows
    app.draw_table(1, 6, 240)
    assert "cx" in screen.text()  # short ENG column
    assert "ENG" in screen.text()

    calls = []
    monkeypatch.setattr(app, "prompt", lambda _label: "fixture-only")
    monkeypatch.setattr(app, "confirm", lambda _label: True)
    monkeypatch.setattr(dash, "run_ccx", lambda _bin, argv, **_kw:
                        calls.append(argv) or subprocess.CompletedProcess(argv, 0, "", ""))
    app.action_send(row)
    app.action_kill(row)
    wait_until(lambda: len(calls) >= 2, reason="Codex full-handle verbs")
    assert {call[0] for call in calls} == {"send", "kill"}
    assert all(call[1] == f"codex:{D}" for call in calls)
    app.action_answer(row)
    assert app.store.snapshot()["status_msg"] == "ccx answer is Claude-only"


def test_codex_gate_approve_uses_real_status_and_absent_peek_is_explained(source_cli, monkeypatch):
    source_cli.store.mutate_record(D, patches={
        "lifecycle.state": "waiting",
        "pendingApprovals": [{"gateId": "codex-gate-7", "kind": "commandExecution",
                              "command": "fake echo", "runtimeId": "faked"}],
    })
    status = source_cli.cli("peek", D, "--status", "--json")["statuses"][0]
    assert status["pending_input"]["kind"] == "permission"
    assert status["pending_input"]["request_id"] == "codex-gate-7"
    screen = CaptureStdscr(size=(40, 160))
    app = make_app(stdscr=screen)
    app.stdscr = screen
    app.store.update_statuses({source_cli.d["worker"]: status}, None)
    app.store.update_conversation(source_cli.d["worker"], None,
                                  "CCX original Codex conversation not available")
    app.draw_detail(0, 25, 160, app.store.snapshot(), source_cli.d)
    assert "conversation:" in screen.text()
    assert "CCX original Codex conversation not available" in screen.text()
    assert "codex" in screen.text()
    calls = []
    monkeypatch.setattr(dash, "run_ccx", lambda _bin, argv, **kw:
                        calls.append(argv) or subprocess.CompletedProcess(argv, 0, "", ""))
    app.action_permission(source_cli.d, approve=True)
    wait_until(lambda: bool(calls), reason="Codex permission action")
    assert calls[0] == ["approve", source_cli.d["worker"], "--json", "--request-id", "codex-gate-7"]


def test_dual_engine_gauges_remain_visible_on_120_column_terminal():
    screen = CaptureStdscr(size=(40, 120))
    app = make_app(stdscr=screen)
    app.stdscr = screen
    app.store.set_usage_engines(("claude", "codex"))
    app.store.update_usage("claude", dash.normalize_usage("claude", _CLAUDE_QUOTA), None)
    app.store.update_usage("codex", dash.normalize_usage("codex", _CODEX_QUOTA), None)
    app.draw_usage(1, 120, app.store.snapshot())
    shown = screen.text()
    assert "claude 5h" in shown and "codex 5h" in shown


@pytest.mark.parametrize("engine", ["claude", "codex"])
@pytest.mark.parametrize("exit_code", [0, 4])
def test_enter_attaches_real_terminal_and_restores_curses(
    monkeypatch, engine, exit_code,
):
    app = make_app()
    worker = f"{engine}:{A}"
    app.rows = [{"worker": worker, "engine": engine, "terminal_only": False}]
    events = []
    for name in ("def_prog_mode", "endwin", "reset_prog_mode"):
        monkeypatch.setattr(dash.curses, name,
                            lambda name=name: events.append(name))
    monkeypatch.setattr(dash.curses, "curs_set", lambda _: events.append("cursor"))
    monkeypatch.setattr(dash.subprocess, "run",
                        lambda argv, **kwargs: events.append(("run", argv, kwargs))
                        or SimpleNamespace(returncode=exit_code))
    monkeypatch.setattr("builtins.input", lambda prompt: events.append(("input", prompt)) or "")
    app.stdscr.clear = lambda: events.append("clear")
    app.on_resize = lambda: events.append("resize")
    app.poller.kick = lambda: events.append("poll")
    app.peeker.wake = lambda: events.append("peek")

    assert app.handle_key(dash.curses.KEY_ENTER)
    assert events[:3] == [
        "def_prog_mode", "endwin",
        ("run", ["ccx", "attach", worker], {"check": False})]
    if exit_code:
        assert ("input", f"attach exited {exit_code} — press Enter to return") in events
    else:
        assert not any(event[0] == "input" for event in events if isinstance(event, tuple))
    assert events[-6:] == ["reset_prog_mode", "cursor", "clear",
                            "resize", "poll", "peek"]


def test_enter_attach_eligibility_and_old_detail_aliases(monkeypatch):
    app = make_app()
    app.rows = [{"worker": "codex:test", "terminal_only": True}]
    events = []
    monkeypatch.setattr(app, "action_attach", lambda row: events.append(row))
    app.handle_key(10)
    assert events  # the UI passes the selected row to the guarded attach action
    assert not dash.can_attach(app.rows[0])
    assert "⏎ attach" not in dash.footer_hints(app.rows[0], None)
    app.rows = [{"worker": "codex:test", "terminal_only": False}]
    assert dash.can_attach(app.rows[0])
    assert "⏎ attach" in dash.footer_hints(app.rows[0], None)
    app.show_detail = False
    app.handle_key(ord("l"))
    assert app.show_detail is False
    app.handle_key(ord("h"))
    assert app.show_detail is False
    app.handle_key(ord("d"))
    assert app.show_detail is True
    app.search = "match"
    app.handle_key(27)
    assert app.search == "" and app.show_detail is True


def test_background_subprocesses_never_inherit_dashboard_stdin(monkeypatch):
    calls = []
    def fake_run(argv, **kwargs):
        calls.append((argv, kwargs))
        return subprocess.CompletedProcess(argv, 0, "{}", "")
    monkeypatch.setattr(dash.subprocess, "run", fake_run)
    dash.run_ccx("ccx", ["list"])
    dash.run_ccx("ccx", ["send"], input_text="hello")
    assert calls[0][1]["stdin"] == subprocess.DEVNULL
    assert calls[1][1]["stdin"] is None and calls[1][1]["input"] == "hello"

    popens = []
    def fake_popen(argv, **kwargs):
        popens.append(kwargs)
        return FakePopen([])
    tailer = dash.StreamTailer("ccx", popen=fake_popen)
    tailer.set_target("claude:test")
    assert popens[0]["stdin"] == subprocess.DEVNULL
    tailer.stop()

def test_background_event_updates_dashboard_snapshot_without_polling():
    store = dash.Store()
    store.update_statuses({"codex:owned": {
        "worker": "codex:owned", "state": "idle",
        "background_task_summary": {"as_of_ms": 10, "running_count": 0}
    }}, None)
    event = {"kind": "background.changed", "transition": "started",
             "item": {"id": "command:one", "label": "shell"},
             "summary": {"as_of_ms": 20, "running_count": 1, "pending_count": 0}}
    store.update_background_event("codex:owned", event)
    assert store.snapshot()["statuses"]["codex:owned"]["background_task_summary"] == event["summary"]
    # An older periodic status snapshot must not revert a newer feed update.
    store.update_statuses({"codex:owned": {
        "worker": "codex:owned", "state": "idle",
        "background_task_summary": {"as_of_ms": 15, "running_count": 0}
    }}, None)
    assert store.snapshot()["statuses"]["codex:owned"]["background_task_summary"] == event["summary"]
    # Authoritative newer status replaces stale event overlay.
    store.update_statuses({"codex:owned": {
        "worker": "codex:owned", "state": "idle",
        "background_task_summary": {"as_of_ms": 30, "running_count": 0}
    }}, None)
    assert store.snapshot()["statuses"]["codex:owned"]["background_task_summary"]["running_count"] == 0
    assert "background started: shell" in dash.App.fmt_event(event)
