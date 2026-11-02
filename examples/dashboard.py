#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""dashboard — a live, interactive terminal dashboard for your `ccx` sessions.

This is an example, not part of the `ccx` package. It composes
over the public `ccx` CLI using only the Python standard library (`curses`) — it
shells out to the same `ccx` binary you use by hand and never imports the `ccx`
package. Think of it as `htop` for CCX workers.

What it shows
-------------
- Available Claude/Codex quota gauges from `ccx usage`.
- A sortable worker table from `ccx list --json` enriched by a single bounded
  `ccx peek <visible workers> --status --json` batch for context and input gates.
- A scrollable schema-5 conversation from `ccx logs <worker> --tail 60` for the
  focused worker. Optional live `ccx events` feed with `e`.

Keys
----
Arrows/j/k move the worker selection. PgUp/PgDn scroll the conversation,
Home/End jump to its start/live bottom. Tab changes scope; o sorts; / searches;
r refreshes; Enter attaches; d/e toggle detail/events; ? opens full help; q quits.
Permission A/X, question a, send s and kill K are contextual actions.

Usage
-----

    examples/dashboard.py                 # watch active sessions
    examples/dashboard.py --scope all     # include stopped / failed / historical
    examples/dashboard.py --once          # one-shot text snapshot, no tty required
    examples/dashboard.py --once --json   # machine-readable JSON snapshot
    examples/dashboard.py --ccx-bin /path/to/ccx
    CCX_BIN=/path/to/ccx examples/dashboard.py

Requires a terminal (it takes over the screen) and Python 3.11+. No third-party
packages. If `ccx` isn't on your PATH, point at it with `--ccx-bin` or `$CCX_BIN`.
"""

from __future__ import annotations

import argparse
import curses
import json
import os
import pathlib
import shutil
import subprocess
import unicodedata
import sys
import threading
from datetime import datetime
import time
from collections import deque

# --------------------------------------------------------------------------- #
# ccx binary discovery
# --------------------------------------------------------------------------- #
def find_ccx_bin() -> str:
    """Locate the ccx executable: $CCX_BIN, then PATH, then the sibling launcher."""
    env = os.environ.get("CCX_BIN")
    if env:
        return env
    found = shutil.which("ccx")
    if found:
        return found
    sibling = pathlib.Path(__file__).resolve().parent.parent / "ccx.py"
    if sibling.exists():
        return str(sibling)
    return "ccx"  # last resort; will fail loudly if absent


def run_ccx(ccx_bin: str, argv: list[str], *, input_text: str | None = None,
            timeout: float = 20.0) -> subprocess.CompletedProcess:
    """Run a ccx subcommand, capturing output. Never raises on non-zero exit."""
    try:
        return subprocess.run(
            [ccx_bin, *argv],
            input=input_text,
            stdin=subprocess.DEVNULL if input_text is None else None,
            text=True,
            capture_output=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return subprocess.CompletedProcess(argv, returncode=124, stdout="", stderr="timeout")
    except OSError as e:
        return subprocess.CompletedProcess(argv, returncode=127, stdout="", stderr=str(e))


def run_ccx_json(ccx_bin: str, argv: list[str], *, timeout: float = 20.0):
    """Run a ccx subcommand that emits JSON; return (parsed | None, error_str | None)."""
    proc = run_ccx(ccx_bin, argv, timeout=timeout)
    if proc.returncode != 0 and not proc.stdout.strip():
        return None, (proc.stderr or f"exit {proc.returncode}").strip()
    try:
        return json.loads(proc.stdout), None
    except (ValueError, TypeError):
        return None, (proc.stderr or "unparseable output").strip()


# --------------------------------------------------------------------------- #
# Session-state styling
# --------------------------------------------------------------------------- #
# Logical colour buckets — mapped to curses colour pairs once curses is up.
C_DEFAULT, C_GREEN, C_CYAN, C_YELLOW, C_MAGENTA, C_RED, C_DIM, C_BLUE = range(8)

# State -> (colour bucket, glyph), for recorded CCX workers.
STATE_STYLE: dict[str, tuple[int, str]] = {
    "idle":              (C_GREEN,   "●"),
    "busy":              (C_CYAN,    "▸"),
    "starting":           (C_YELLOW,  "◌"),
    "waiting":           (C_MAGENTA, "?"),
    "budget-stopped":    (C_RED,     "$"),
    "failed":           (C_RED,     "✗"),
    "stopped":              (C_DIM,     "·"),
}

# States that still make progress — used to decide event streaming + "active" tint.
LIVE_STATES = frozenset({"busy", "starting", "waiting"})
# States where a permission/question may be pending — these get a highlight.
ATTENTION_STATES = frozenset({"waiting"})


def state_style(state: str) -> tuple[int, str]:
    return STATE_STYLE.get(state or "", (C_DEFAULT, "?"))


# --------------------------------------------------------------------------- #
# Formatting helpers
# --------------------------------------------------------------------------- #
def fmt_age(ms: int | None) -> str:
    if not ms:
        return "—"
    delta = max(0.0, time.time() - ms / 1000)
    for unit, n in (("d", 86400), ("h", 3600), ("m", 60), ("s", 1)):
        if delta >= n:
            return f"{int(delta // n)}{unit}"
    return "now"


def token_total(row: dict) -> int | None:
    """Unknown token counters stay unknown, not fabricated zeroes."""
    a, b = row.get("input_tokens"), row.get("output_tokens")
    return a + b if type(a) is int and type(b) is int else None


def cell_width(ch: str) -> int:
    """Terminal cells for one code point (wide East Asian/emoji = 2, combining = 0)."""
    if unicodedata.combining(ch) or unicodedata.category(ch) in ("Mn", "Me", "Cf"):
        return 0
    return 2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1


def fit_cells(text: str, cells: int) -> str:
    """Truncate to at most `cells` terminal columns, so wide glyphs never wrap a row."""
    used = 0
    for i, ch in enumerate(text):
        used += cell_width(ch)
        if used > cells:
            return text[:i]
    return text


def fmt_tokens(n: int | None) -> str:
    if n is None:
        return "—"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}k"
    return str(n)


STATUS_BATCH_LIMIT = 40
LOG_TAIL_TURNS = 60


def context_tokens(row: dict | None) -> int | None:
    block = (row or {}).get("context")
    value = block.get("tokens_used") if isinstance(block, dict) else None
    return value if type(value) is int else None


def can_send(row: dict | None) -> bool:
    return bool(row and session_identity(row) and not row.get("terminal_only")
                and row.get("state") in {"idle", "stopped"})


def can_attach(row: dict | None) -> bool:
    return bool(row and session_identity(row) and not row.get("terminal_only"))


def footer_hints(row: dict | None, status: dict | None) -> str:
    hints = ["↑↓ workers", "PgUp/PgDn scroll", "End live", "K kill"]
    gate = pending_input(status)
    if gate.get("kind") == "permission" and gate.get("request_id"):
        hints += ["A approve", "X reject"]
    if (row and row.get("engine") == "claude" and gate.get("kind") == "question"
            and gate.get("request_id") and first_question_payload(status)):
        hints.append("a answer")
    if can_send(row):
        hints.append("s send")
    if can_attach(row):
        hints.append("⏎ attach")
    hints += ["? help", "q quit"]
    return " · ".join(hints)


def clean(text: str | None, limit: int | None = None) -> str:
    """Collapse whitespace/newlines to a single safe line for table/row rendering."""
    if not text:
        return ""
    s = " ".join(str(text).split())
    if limit and len(s) > limit:
        s = s[: limit - 1] + "…"
    return s


def pending_input(status: dict | None) -> dict:
    """Typed gate of a recorded peek --status row, if any."""
    return (status or {}).get("pending_input") or {}


def first_question_payload(status: dict | None) -> dict | None:
    """First question from the current status gate (not rich conversation)."""
    if pending_input(status).get("kind") != "question":
        return None
    raw = (status or {}).get("waiting_for") or {}
    questions = (raw.get("input") or {}).get("questions") or pending_input(status).get("questions") or []
    if not isinstance(questions, list) or not questions:
        return None
    return questions[0] if isinstance(questions[0], dict) else None


def tool_summary(tool: dict) -> str:
    """Original conversation summaries are structured, never display repr(dict)."""
    summary = tool.get("summary")
    if isinstance(summary, dict):
        phrase = " ".join(str(summary.get(key) or "") for key in ("verb", "target")).strip()
    else:
        phrase = summary if isinstance(summary, str) else ""
    return clean(" ".join(str(part) for part in (tool.get("name") or tool.get("nativeType") or "tool", phrase) if part))


ENGINES = ("claude", "codex")
CLAUDE_USAGE_PERIOD_S = 12
CODEX_USAGE_PERIOD_S = 300


def available_engines() -> tuple[str, ...]:
    """Check executable availability only; CCX handles provider authentication.

    Respect CCX's explicit native executable overrides, and otherwise use
    PATH. No credential/config files are read or probed by the dashboard.
    """
    names = (os.environ.get("CCX_CLAUDE_BIN") or "claude",
             os.environ.get("CCX_CODEX_BIN") or "codex")
    return tuple(engine for engine, binary in zip(ENGINES, names) if shutil.which(binary))


def _codex_window_label(window: dict) -> str:
    minutes = window.get("windowDurationMins")
    if type(minutes) is not int or minutes <= 0:
        return "limit"
    if minutes % 1440 == 0:
        return f"{minutes // 1440}d"
    if minutes % 60 == 0:
        return f"{minutes // 60}h"
    return f"{minutes}m"


def codex_quota_windows(data: dict) -> list[dict]:
    """Normalize the documented account/rateLimits shape CCX usage returns."""
    by_id = data.get("rateLimitsByLimitId")
    if isinstance(by_id, dict) and by_id:
        buckets = sorted(by_id.items(), key=lambda pair: (
            str(pair[0]).casefold() != "codex", str(pair[0]).casefold()))
    else:
        buckets = [("", data.get("rateLimits"))]
    windows = []
    for limit_id, bucket in buckets:
        if not isinstance(bucket, dict):
            continue
        name = bucket.get("limitName") or limit_id
        suffix = ("" if not name or str(name).casefold() == "codex" else f" {name}")
        for field in ("primary", "secondary"):
            window = bucket.get(field)
            if not isinstance(window, dict):
                continue
            percent = window.get("usedPercent")
            if type(percent) not in (int, float):
                continue
            windows.append({"label": _codex_window_label(window) + suffix,
                            "utilization": float(percent)})
    return windows


def normalize_usage(engine: str, data: dict | None) -> dict | None:
    """Produce the dashboard's own bounded quota shape from actual CCX JSON."""
    if not isinstance(data, dict):
        return None
    if engine == "codex":
        windows = codex_quota_windows(data)
        return {"windows": windows} if windows else None
    summary = {}
    for key in ("five_hour", "seven_day", "seven_day_opus", "seven_day_sonnet"):
        value = data.get(key)
        if isinstance(value, dict) and type(value.get("utilization")) in (float, int):
            summary[key] = {"utilization": float(value["utilization"])}
    models = data.get("model_quotas")
    if isinstance(models, list):
        summary["model_quotas"] = [
            {"model": str(model.get("model") or "model"),
             "utilization": float(model["utilization"])}
            for model in models if isinstance(model, dict)
            and type(model.get("utilization")) in (float, int)
        ]
    extra = data.get("extra_usage")
    if isinstance(extra, dict):
        summary["extra_usage"] = {
            "enabled": bool(extra.get("enabled")),
            "used_credits": float(extra.get("used_credits") or 0.0),
            "currency": extra.get("currency") or "",
        }
    return summary if summary else None


def usage_args(engine: str) -> list[str]:
    return (["usage", "--engine", "claude", "--summary", "--cached"]
            if engine == "claude" else ["usage", "--engine", "codex", "--json"])


def fetch_usage(ccx_bin: str, engine: str, runner) -> tuple[dict | None, str | None]:
    data, error = runner(ccx_bin, usage_args(engine), timeout=25.0)
    if error:
        return None, "usage unavailable"
    normalized = normalize_usage(engine, data)
    return (normalized, None) if normalized else (None, "usage unavailable")


def usage_gauges(usage: dict) -> list[tuple[str, float]]:
    """Ordered small labeled gauges shared by TUI and one-shot modes."""
    rows = []
    claude = usage.get("claude") or {}
    for key, label in (("five_hour", "5h"), ("seven_day", "7d")):
        data = claude.get(key)
        if isinstance(data, dict):
            rows.append((f"claude {label}", float(data["utilization"])))
    models = claude.get("model_quotas")
    if isinstance(models, list):
        rows.extend((f"claude {q['model']}", float(q["utilization"])) for q in models)
    else:
        for key, label in (("seven_day_opus", "opus"), ("seven_day_sonnet", "sonnet")):
            data = claude.get(key)
            if isinstance(data, dict):
                rows.append((f"claude {label}", float(data["utilization"])))
    for window in (usage.get("codex") or {}).get("windows") or []:
        rows.append((f"codex {window['label']}", float(window["utilization"])))
    return rows


# --------------------------------------------------------------------------- #
# Data store (shared between the worker threads and the UI thread)
# --------------------------------------------------------------------------- #
class Store:
    """Thread-safe snapshot of everything the UI renders."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.sessions: list[dict] = []
        self.statuses: dict[str, dict] = {}
        self.statuses_error: str | None = None
        self.usage: dict[str, dict] = {}  # only successfully fetched engines
        self.usage_engines: tuple[str, ...] = ()
        self.usage_errors: dict[str, str] = {}
        self.conversation: dict | None = None   # schema-5 logs for the focused worker
        self.conversation_error: str | None = None
        self.conversation_worker: str | None = None
        self.list_error: str | None = None
        self.last_list_ts: float = 0.0
        self.status_msg: str = ""
        self.status_ts: float = 0.0

    def update_sessions(self, sessions, error):
        with self._lock:
            if sessions is not None:
                self.sessions = sessions
                self.last_list_ts = time.time()
            self.list_error = error

    def update_statuses(self, statuses, error, workers=()):
        with self._lock:
            if statuses is not None:
                self.statuses = statuses
            else:
                # A failed batch is not actionable evidence: never keep an old
                # permission/question request ID on the visible rows.
                self.statuses = {}
            self.statuses_error = error

    def set_usage_engines(self, engines):
        with self._lock:
            self.usage_engines = tuple(engines)

    def update_usage(self, engine, usage, error):
        with self._lock:
            if usage is None:
                self.usage.pop(engine, None)
                self.usage_errors[engine] = "usage unavailable"
            else:
                self.usage[engine] = usage
                self.usage_errors.pop(engine, None)

    def update_conversation(self, worker, doc, error=None):
        with self._lock:
            if worker != self.conversation_worker or doc is not None or error is None:
                # Only a failed refresh of the SAME full worker may retain a
                # previous document. Errors never preserve a different focus.
                self.conversation = doc
            self.conversation_worker = worker
            self.conversation_error = error

    def set_status(self, msg):
        with self._lock:
            self.status_msg = msg
            self.status_ts = time.time()

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "sessions": list(self.sessions),
                "statuses": dict(self.statuses),
                "statuses_error": self.statuses_error,
                "usage": dict(self.usage),
                "usage_engines": self.usage_engines,
                "usage_errors": dict(self.usage_errors),
                "conversation": self.conversation,
                "conversation_worker": self.conversation_worker,
                "conversation_error": self.conversation_error,
                "list_error": self.list_error,
                "last_list_ts": self.last_list_ts,
                "status_msg": self.status_msg,
                "status_ts": self.status_ts,
            }


SCOPES = ["active", "all"]
SCOPE_FLAGS = {"active": ["--active"], "all": []}


def session_identity(row: dict) -> str | None:
    """A full worker handle, never a short display prefix, selects a worker."""
    worker = row.get("worker")
    return worker if isinstance(worker, str) and worker else None


def fetch_statuses(ccx_bin: str, workers: list[str], runner) -> tuple[dict[str, dict] | None, str | None]:
    """Exactly one recorded, capped status batch; never per-selected-row RPCs."""
    handles = list(dict.fromkeys(workers))[:STATUS_BATCH_LIMIT]
    if not handles:
        return {}, None
    doc, err = runner(ccx_bin, ["peek", *handles, "--status", "--json"], timeout=12.0)
    rows = doc.get("statuses") if isinstance(doc, dict) else None
    if not isinstance(rows, list):
        return None, err or "malformed status batch"
    result = {r["worker"]: r for r in rows
              if isinstance(r, dict) and r.get("worker") in handles}
    failures = doc.get("errors") if isinstance(doc.get("errors"), dict) else {}
    notes = [f"{worker}: {failure.get('message', failure.get('error', 'unavailable'))}"
             for worker, failure in failures.items()
             if worker in handles and isinstance(failure, dict)]
    if err:
        notes.append(err)
    if len(result) + len(failures) < len(handles):
        notes.append("incomplete status batch")
    return result, "; ".join(notes) if notes else None


def _list_rows(ccx_bin: str, scope: str, runner) -> tuple[list[dict] | None, str | None]:
    """Fetch both engines from the canonical recorded CLI inventory."""
    return runner(ccx_bin, ["list", "--json", *SCOPE_FLAGS[scope]], timeout=15.0)


class Poller(threading.Thread):
    """Refresh the list, visible-row status batch and usage gauges off the UI thread."""

    def __init__(self, ccx_bin: str, store: Store, runner=None):
        super().__init__(daemon=True)
        self.ccx_bin = ccx_bin
        self.store = store
        self.stop_event = threading.Event()
        self._lock = threading.Lock()
        self._scope = "active"
        self._visible: tuple[str, ...] = ()
        self._last_status = 0.0
        self._force = threading.Event()
        self._last_list = 0.0
        self._last_usage: dict[str, float] = {}
        self._engines = available_engines()
        self.store.set_usage_engines(self._engines)
        self._runner = runner  # callable(ccx_bin, argv, *, timeout) -> (data, err); None → real subprocess

    # --- knobs the UI sets ------------------------------------------------- #
    def set_scope(self, scope: str):
        with self._lock:
            self._scope = scope
        self.kick()

    def set_visible(self, workers: list[str]):
        visible = tuple(dict.fromkeys(workers))[:STATUS_BATCH_LIMIT]
        with self._lock:
            if visible == self._visible:
                return
            self._visible = visible
        self.kick()

    def kick(self):
        self._force.set()

    def run(self):
        while not self.stop_event.is_set():
            forced = self._force.wait(timeout=0.25)
            self._force.clear()
            if self.stop_event.is_set():
                break
            now = time.monotonic()
            with self._lock:
                scope, visible = self._scope, self._visible

            _rj = self._runner if self._runner is not None else run_ccx_json
            if forced or now - self._last_list >= 2.0:
                sessions, err = _list_rows(self.ccx_bin, scope, _rj)
                self.store.update_sessions(sessions, err)
                self._last_list = now

            if forced or now - self._last_status >= 2.0:
                statuses, err = fetch_statuses(self.ccx_bin, list(visible), _rj)
                self.store.update_statuses(statuses, err, visible)
                self._last_status = now

            for engine in self._engines:
                previous = self._last_usage.get(engine)
                period = (CODEX_USAGE_PERIOD_S if engine == "codex"
                          else CLAUDE_USAGE_PERIOD_S)
                # Codex may spawn a short-lived app-server. A manual refresh
                # never bypasses the 300s minimum between attempts.
                if previous is None or (now - previous >= period) or (forced and engine == "claude"):
                    self._last_usage[engine] = now
                    usage, err = fetch_usage(self.ccx_bin, engine, _rj)
                    self.store.update_usage(engine, usage, err)


class ConversationWorker(threading.Thread):
    """Read bounded schema-5 logs only on focus/record changes or explicit refresh.

    The work runs off the UI thread. Status is already in Poller's visible-row
    batch: this thread never makes a redundant per-focus status call.
    """

    def __init__(self, ccx_bin: str, store: Store, runner=None):
        super().__init__(daemon=True)
        self.ccx_bin = ccx_bin
        self.store = store
        self.stop_event = threading.Event()
        self._lock = threading.Lock()
        self._focus: str | None = None
        self._signature: tuple | None = None
        self._force = False
        self._wake = threading.Event()
        self._runner = runner

    def set_focus(self, worker: str | None, signature: tuple | None = None):
        with self._lock:
            if (worker, signature) == (self._focus, self._signature):
                return
            changed_worker = worker != self._focus
            self._focus, self._signature = worker, signature
        if changed_worker:
            self.store.update_conversation(worker, None)
        self._wake.set()

    def wake(self):
        with self._lock:
            self._force = True
        self._wake.set()

    def run(self):
        last_key = None
        retry_key, retry_after = None, 0.0
        while not self.stop_event.is_set():
            self._wake.wait(timeout=0.5)
            self._wake.clear()
            with self._lock:
                worker, signature = self._focus, self._signature
                forced, self._force = self._force, False
            key = worker, signature
            if not worker:
                last_key = None
                retry_key = None
                continue
            if key == last_key and not forced:
                continue
            if key == retry_key and not forced and time.monotonic() < retry_after:
                continue
            runner = self._runner if self._runner is not None else run_ccx_json
            doc, error = runner(self.ccx_bin,
                                ["logs", worker, "--tail", str(LOG_TAIL_TURNS)],
                                timeout=12.0)
            if error or not isinstance(doc, dict) or doc.get("schemaVersion") != 5:
                doc, error = None, error or "invalid schema-5 conversation"
            with self._lock:
                if (worker, signature) == (self._focus, self._signature):
                    self.store.update_conversation(worker, doc, error)
            if error:
                last_key, retry_key = None, key
                retry_after = time.monotonic() + 2.0  # bounded retry rate
            else:
                last_key, retry_key = key, None


# --------------------------------------------------------------------------- #
# Live event feed (ccx events --follow) for the focused managed session
# --------------------------------------------------------------------------- #
class StreamTailer:
    """Runs `ccx events <short> --follow --json` for one managed session and
    accumulates the NDJSON events into a bounded buffer, starting with its 20
    most recent. Restarts when the focus changes; the feed keeps following a
    stopped worker, and heartbeat lines are skipped."""

    def __init__(self, ccx_bin: str, popen=None):
        self.ccx_bin = ccx_bin
        self._lock = threading.Lock()
        self.short: str | None = None
        self.events: deque = deque(maxlen=200)
        self._proc: subprocess.Popen | None = None
        self._thread: threading.Thread | None = None
        self._popen = popen  # Popen-compatible factory; None → real subprocess.Popen

    def _start_locked(self, short: str):
        try:
            _popen = self._popen if self._popen is not None else subprocess.Popen
            self._proc = _popen(
                [self.ccx_bin, "events", short, "--follow", "--json",
                 "--tail", "20", "--heartbeat", "0"],
                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL, text=True, bufsize=1,
            )
        except OSError:
            self._proc = None
            return
        target_events = self.events  # capture so _read can guard against stale appends
        self._thread = threading.Thread(target=self._read, args=(self._proc, target_events), daemon=True)
        self._thread.start()

    def _read(self, proc: subprocess.Popen, target_events: deque):
        try:
            for line in proc.stdout:  # type: ignore[union-attr]
                line = line.strip()
                if not line:
                    continue
                try:
                    evt = json.loads(line)
                except ValueError:
                    continue
                if not isinstance(evt, dict) or evt.get("kind") == "heartbeat":
                    continue
                evt["_ts"] = time.time()
                with self._lock:
                    # Guard: only append when target_events is still the current deque.
                    # Without this, an old reader that outlives a set_target() call would
                    # inject stale events from the previous session into the new one.
                    if self.events is target_events:
                        self.events.append(evt)
        except (OSError, ValueError):
            pass

    def _stop_locked(self):
        """Terminate the running process; returns it so the caller can .wait() outside the lock."""
        proc = self._proc
        if proc and proc.poll() is None:
            try:
                proc.terminate()
            except OSError:
                pass
        self._proc = None
        self._thread = None
        return proc

    @staticmethod
    def _reap(proc) -> None:
        """Wait for the process to exit after terminate(); escalates to kill() if needed."""
        if proc is None:
            return
        try:
            proc.wait(timeout=2.0)
        except subprocess.TimeoutExpired:
            try:
                proc.kill()
            except OSError:
                pass
        except OSError:
            pass

    def set_target(self, short: str | None):
        with self._lock:
            if short == self.short:
                return
            old_proc = self._stop_locked()
            self.short = short
            self.events = deque(maxlen=200)
            if short:
                self._start_locked(short)
        # Reap outside the lock in a daemon thread so set_target (called from the UI
        # thread on every draw) never blocks waiting for the old process to exit.
        if old_proc is not None:
            threading.Thread(target=self._reap, args=(old_proc,), daemon=True).start()

    def get(self, short: str | None) -> list[dict]:
        with self._lock:
            if short != self.short:
                return []
            return list(self.events)

    def stop(self):
        with self._lock:
            old_proc = self._stop_locked()
        self._reap(old_proc)  # blocking is fine during shutdown


# --------------------------------------------------------------------------- #
# Normalized conversation view / scroll anchor
# --------------------------------------------------------------------------- #
def conversation_entries(doc: dict | None) -> list[tuple[str, str, int]]:
    """Stable logical lines from schema-5; never expose raw native payload dicts."""
    if not isinstance(doc, dict) or doc.get("schemaVersion") != 5:
        return []
    entries = []
    for turn in doc.get("turns") or []:
        # `ccx logs --tail` numbers turns from the first one read, so key a
        # turn by its first message id to keep the scroll position stable.
        first = next((item.get("id") for side in ("input", "output")
                      for item in turn.get(side) or []
                      if isinstance(item, dict) and item.get("id")), None)
        tid = str(turn.get("turnId") or first or turn.get("sequence"))
        try:
            started = datetime.fromisoformat(
                str(turn.get("startedAt")).replace("Z", "+00:00")
            ).astimezone().strftime("%H:%M")
        except ValueError:
            started = ""
        heading = " · ".join(x for x in (started, turn.get("status") or "?") if x)
        entries.append((f"{tid}:sep", f"── {heading} ──", C_DIM))
        for side in ("input", "output"):
            for i, item in enumerate(turn.get(side) or []):
                if not isinstance(item, dict):
                    continue
                kind = item.get("kind")
                label = ""
                color = C_DEFAULT
                if kind in ("user-text", "assistant-text"):
                    label = ("you: " if kind == "user-text" else "assistant: ") + str(item.get("text") or "")
                    color = C_CYAN if kind == "user-text" else C_DEFAULT
                elif kind == "tool-call":
                    mark, color = {
                        "completed": ("✓", C_GREEN), "failed": ("✗", C_RED),
                        "declined": ("✗", C_RED), "error": ("✗", C_RED),
                        "interrupted": ("✗", C_RED), "inProgress": ("…", C_YELLOW),
                        "in-progress": ("…", C_YELLOW), "pending": ("…", C_YELLOW),
                    }.get(item.get("status"), ("·", C_DIM))
                    label = f"{mark} tool: {tool_summary(item)}"
                elif kind == "tool-result":
                    result = item.get("result") or {}
                    mark = "✗" if result.get("isError") else "✓"
                    color = C_RED if result.get("isError") else C_GREEN
                    label = f"{mark} result: {clean(result.get('content'), 140)}"
                elif kind == "error":
                    color = C_RED
                    label = "✗ error: " + str(item.get("message") or item.get("text") or "unknown error")
                elif kind == "command":
                    label = "you: /" + str(item.get("name") or "command")
                elif kind == "system-text":
                    label = "system: " + str(item.get("text") or "")
                    color = C_DIM
                if label:
                    for j, line in enumerate(label.splitlines() or [""]):
                        entries.append((f"{tid}:{side}:{i}:{j}", line, color))
    return entries


def wrap_cells(text: str, width: int) -> list[tuple[int, str]]:
    """Word-wrap in terminal cells and retain character offsets for resize anchors."""
    width = max(1, width)
    if not text:
        return [(0, "")]
    lines = []
    offset = 0
    while offset < len(text):
        prefix = fit_cells(text[offset:], width)
        if not prefix:  # a wide character cannot fit in a one-cell viewport
            lines.append((offset, " "))
            offset += 1
            continue
        end = offset + len(prefix)
        if end < len(text):
            space = prefix.rfind(" ")
            if space > 0:
                end = offset + space + 1
        lines.append((offset, text[offset:end].rstrip()))
        offset = end
    return lines


class ConversationView:
    """Bottom-following scrollback; preserve a logical text anchor on resize/refetch."""

    def __init__(self):
        self.worker = None
        self.doc = None
        self.width = 0
        self.height = 0
        self.lines: list[tuple[str, int, str, int]] = []
        self.top = 0
        self.follow = True

    def _max_top(self) -> int:
        return max(0, len(self.lines) - self.height)

    def anchor(self):
        return self.lines[self.top][:2] if self.lines and self.top < len(self.lines) else None

    def configure(self, worker, doc, width: int, height: int):
        width, height = max(1, width), max(1, height)
        changed = worker != self.worker
        if (not changed and doc is self.doc and
                width == self.width and height == self.height):
            return
        anchor = self.anchor() if not self.follow and not changed else None
        self.worker, self.doc = worker, doc
        self.width, self.height = width, height
        self.lines = [(key, offset, part, color)
                      for key, text, color in conversation_entries(doc)
                      for offset, part in wrap_cells(text, width)]
        if changed:
            self.follow = True
        if self.follow:
            self.top = self._max_top()
        elif anchor:
            key, offset = anchor
            candidates = [(i, start) for i, (k, start, _, _) in enumerate(self.lines) if k == key]
            if candidates:
                self.top = max((i for i, start in candidates if start <= offset),
                               default=candidates[0][0])
            else:
                self.top = min(self.top, self._max_top())
        self.top = min(self.top, self._max_top())

    def scroll(self, delta: int):
        self.top = max(0, min(self._max_top(), self.top + delta))
        self.follow = self.top == self._max_top()

    def home(self):
        self.top = 0
        self.follow = self._max_top() == 0

    def end(self):
        self.follow = True
        self.top = self._max_top()

    def visible(self):
        return self.lines[self.top:self.top + self.height]


# --------------------------------------------------------------------------- #
# The app / UI
# --------------------------------------------------------------------------- #
SORTS = ["state", "name", "turns", "context", "age"]


class App:
    def __init__(self, stdscr, ccx_bin: str, scope: str):
        self.stdscr = stdscr
        self.ccx_bin = ccx_bin
        self.store = Store()
        self.poller = Poller(ccx_bin, self.store)
        self.poller.set_scope(scope)
        self.peeker = ConversationWorker(ccx_bin, self.store)
        self.streamer = StreamTailer(ccx_bin)

        self.scope = scope
        self.sort = "state"
        self.search = ""
        self.sel = 0                       # index into the filtered/sorted rows
        self.sel_short: str | None = None  # full worker identity, not display short
        self.scroll = 0                    # first visible row
        self.show_detail = True
        self.show_events = False
        self.conversation_view = ConversationView()
        self.detail_page_rows = 10
        self.help = False
        self.colors: dict[int, int] = {}
        self.rows: list[dict] = []         # current filtered+sorted rows
        self._inflight: set[tuple[str, str]] = set()  # (full worker, action) currently running
        self._inflight_lock = threading.Lock()

    # --- colour setup ------------------------------------------------------ #
    def init_colors(self):
        if not curses.has_colors():
            self.colors = {b: 0 for b in range(8)}
            return
        curses.start_color()
        try:
            curses.use_default_colors()
            bg = -1
        except curses.error:
            bg = curses.COLOR_BLACK
        spec = {
            C_GREEN: curses.COLOR_GREEN,
            C_CYAN: curses.COLOR_CYAN,
            C_YELLOW: curses.COLOR_YELLOW,
            C_MAGENTA: curses.COLOR_MAGENTA,
            C_RED: curses.COLOR_RED,
            C_BLUE: curses.COLOR_BLUE,
        }
        for i, (bucket, fg) in enumerate(spec.items(), start=1):
            try:
                curses.init_pair(i, fg, bg)
                self.colors[bucket] = curses.color_pair(i)
            except curses.error:
                self.colors[bucket] = 0
        self.colors[C_DEFAULT] = 0
        self.colors[C_DIM] = curses.A_DIM

    def _inflight_guard(self, short: str, action: str) -> bool:
        """Claim an in-flight slot for (short, action). Returns False if already running."""
        with self._inflight_lock:
            key = (short, action)
            if key in self._inflight:
                return False
            self._inflight.add(key)
            return True

    def _inflight_release(self, short: str, action: str):
        with self._inflight_lock:
            self._inflight.discard((short, action))

    def attr(self, bucket: int, extra: int = 0) -> int:
        return self.colors.get(bucket, 0) | extra

    # --- low-level safe draw ---------------------------------------------- #
    def put(self, y, x, text, attr=0):
        h, w = self.stdscr.getmaxyx()
        if y < 0 or y >= h or x >= w:
            return
        text = str(text)
        if x < 0:
            text = text[-x:]
            x = 0
        # Never touch the bottom-right cell: writing it scrolls some terminals.
        avail = w - x - (1 if y == h - 1 else 0)
        if avail <= 0:
            return
        text = fit_cells(text, avail)
        try:
            self.stdscr.addstr(y, x, text, attr)
        except curses.error:
            pass  # bottom-right cell write always raises; ignore

    # --- data shaping ------------------------------------------------------ #
    def filtered_rows(self, sessions: list[dict], statuses: dict | None = None) -> list[dict]:
        q = self.search.lower().strip()
        rows = []
        for s in sessions:
            if q:
                hay = " ".join([
                    str(s.get("short", "")), str(s.get("name", "")),
                    str(s.get("cwd", "")), str(s.get("state") or ""),
                    str(s.get("engine") or ""),
                ]).lower()
                if q not in hay:
                    continue
            status = (statuses or {}).get(session_identity(s))
            rows.append({**s, "context": status.get("context") if status else s.get("context")})

        def key(s):
            if self.sort == "name":
                return (s.get("name") or s.get("short") or "").lower()
            if self.sort == "turns":
                return -(s.get("turns") or 0)
            if self.sort == "context":
                return -(context_tokens(s) or 0)
            if self.sort == "age":
                return -(s.get("last_turn_at") or s.get("created_at") or 0)
            # state: attention first, then live, then the rest; newest within bucket
            state = s.get("state") or ""
            rank = 0 if state in ATTENTION_STATES else (1 if state in LIVE_STATES else 2)
            return (rank, -(s.get("last_turn_at") or s.get("created_at") or 0))

        rows.sort(key=key)
        return rows

    def selected_row(self) -> dict | None:
        if 0 <= self.sel < len(self.rows):
            return self.rows[self.sel]
        return None

    def _sync_selection(self):
        """Re-anchor the selection to its full worker identity, not a row
        index. Sessions change state constantly, so the sorted order shifts under
        the cursor; without this the highlight would jump to a different session
        each refresh and the detail/peek would never settle."""
        if not self.rows:
            self.sel = 0
            self.sel_short = None
            return
        if self.sel_short is not None:
            for i, r in enumerate(self.rows):
                if session_identity(r) == self.sel_short:
                    self.sel = i
                    return
        # Unset, or the selected session left the view: clamp and adopt the row.
        self.sel = max(0, min(self.sel, len(self.rows) - 1))
        self.sel_short = session_identity(self.rows[self.sel])

    def _move(self, delta: int):
        if not self.rows:
            return
        self.sel = max(0, min(len(self.rows) - 1, self.sel + delta))
        self.sel_short = session_identity(self.rows[self.sel])

    def _goto(self, idx: int):
        if not self.rows:
            self.sel, self.sel_short = 0, None
            return
        self.sel = max(0, min(len(self.rows) - 1, idx))
        self.sel_short = session_identity(self.rows[self.sel])

    # --- rendering --------------------------------------------------------- #
    def draw(self):
        self.stdscr.erase()
        h, w = self.stdscr.getmaxyx()
        snap = self.store.snapshot()
        self.rows = self.filtered_rows(snap["sessions"], snap["statuses"])
        self._sync_selection()

        # Tell the background workers what's focused (stable identity).
        sel = self.selected_row()
        focus = session_identity(sel) if sel else None
        status = snap["statuses"].get(focus) if focus else None
        self.peeker.set_focus(focus, (
            (status or sel or {}).get("state"),
            (status or {}).get("turn", (sel or {}).get("turns")),
            (status or {}).get("record_revision"),
        ) if focus else None)
        # The existing focused-worker follower also owns the background
        # cards. A model-idle Codex worker can still run native terminals.
        self.streamer.set_target(focus if sel else None)

        self.draw_header(snap, w)

        # Layout: header(3) + table + optional detail + footer(1)
        table_top = 3
        detail_h = 0
        if self.show_detail:
            # Cap so the table always keeps at least one content row above the detail pane.
            max_detail_h = max(0, h - 2 - table_top)
            ideal_h = max(7, min(16, h // 2 - 1))
            detail_h = min(ideal_h, max_detail_h)
            if detail_h < 4:  # terminal too cramped; suppress the pane entirely
                detail_h = 0
        table_bottom = h - 1 - detail_h  # exclusive
        table_rows = max(1, table_bottom - table_top - 1)  # minus column header

        self.draw_table(table_top, table_rows, w)
        self.poller.set_visible([session_identity(r) for r in
                                 self.rows[self.scroll:self.scroll + min(table_rows, STATUS_BATCH_LIMIT)]
                                 if session_identity(r)])
        if self.show_detail and detail_h > 0:
            self.draw_detail(table_bottom, detail_h, w, snap, sel)
        self.draw_footer(h - 1, w, snap)

        if self.help:
            self.draw_help(w, h)

        self.stdscr.noutrefresh()
        curses.doupdate()

    def draw_header(self, snap, w):
        title = " ccx dashboard "
        self.put(0, 0, title, curses.A_REVERSE | curses.A_BOLD)
        n = len(snap["sessions"])
        live = sum(1 for s in snap["sessions"]
                   if s.get("state") in LIVE_STATES)
        attn = sum(1 for s in snap["sessions"]
                   if s.get("state") in ATTENTION_STATES)
        sub = f" {n} sessions · {live} live"
        if attn:
            sub += f" · {attn} need you"
        self.put(0, len(title), sub, curses.A_DIM)

        age = time.time() - snap["last_list_ts"] if snap["last_list_ts"] else None
        clock = f"{int(age)}s ago" if age is not None else "—"
        right = f"scope:{self.scope}  sort:{self.sort}  ⟳{clock} "
        self.put(0, max(0, w - len(right)), right, curses.A_DIM)

        # Usage gauges row.
        self.draw_usage(1, w, snap)

    def draw_usage(self, y, w, snap):
        engines = snap["usage_engines"]
        usage = snap["usage"]
        errors = snap["usage_errors"]
        if not engines:
            self.put(y, 1, "usage: no engine executables available", curses.A_DIM)
            return
        if not usage and not errors:
            self.put(y, 1, "loading usage…", curses.A_DIM)
            return
        x = 1
        # Failure notices must remain visible even on narrow terminals.
        for engine in engines:
            if engine in errors:
                warning = f"{engine}: usage unavailable"
                if x + len(warning) + 1 <= w:
                    self.put(y, x, warning, curses.A_DIM)
                    x += len(warning) + 2
        # Interleave engine windows before adding any extra per-model gauges;
        # otherwise four Claude gauges can hide every Codex gauge at 120 cols.
        gauges = usage_gauges(usage)
        claude = [row for row in gauges if row[0].startswith("claude ")]
        codex = [row for row in gauges if row[0].startswith("codex ")]
        ordered = []
        for index in range(max(len(claude), len(codex))):
            if index < len(claude):
                ordered.append(claude[index])
            if index < len(codex):
                ordered.append(codex[index])
        for label, pct in ordered:
            segments = self.gauge(label, pct, width=8)
            width = sum(len(text) for text, _ in segments) + 2
            if x + width > w:
                break
            for text, attr in segments:
                self.put(y, x, text, attr)
                x += len(text)
            x += 2
        extra = (usage.get("claude") or {}).get("extra_usage") or {}
        if extra.get("enabled") and extra.get("used_credits"):
            cur = extra.get("currency", "")
            tag = f"overage {extra['used_credits']:.2f}{cur}"
            if x + len(tag) + 1 <= w:
                self.put(y, max(x, w - len(tag) - 1), tag, self.attr(C_YELLOW))

    def gauge(self, label, pct, width):
        """Return [(text, attr), …] for one labelled bar gauge."""
        if pct >= 90:
            bucket = C_RED
        elif pct >= 70:
            bucket = C_YELLOW
        else:
            bucket = C_GREEN
        filled = int(round(min(100.0, pct) / 100 * width))
        bar = "█" * filled + "·" * (width - filled)
        return [
            (f"{label} ", curses.A_BOLD),
            (bar, self.attr(bucket)),
            (f" {pct:4.0f}%", self.attr(bucket)),
        ]

    # Column layout for the table.
    def draw_table(self, top, nrows, w):
        cols = [
            ("ENG", 5), ("STATE", 12), ("SHORT", 9), ("NAME", 18), ("TURNS", 6),
            ("CONTEXT", 8), ("LAST", 6), ("CWD", 0),
        ]
        # Header
        x = 0
        for name, cw in cols:
            label = name if cw == 0 else f"{name:<{cw}}"
            self.put(top, x, label, curses.A_BOLD | curses.A_UNDERLINE)
            x += (cw or (w - x)) + 1

        if not self.rows:
            self.put(top + 1, 0, "  no sessions in this scope — press Tab to widen, r to refresh",
                     curses.A_DIM)
            return

        # Keep selection visible.
        if self.sel < self.scroll:
            self.scroll = self.sel
        elif self.sel >= self.scroll + nrows:
            self.scroll = self.sel - nrows + 1

        for i in range(nrows):
            idx = self.scroll + i
            if idx >= len(self.rows):
                break
            self.draw_row(top + 1 + i, idx, cols, w)

        # Scroll indicator.
        if len(self.rows) > nrows:
            pos = f"{self.sel + 1}/{len(self.rows)}"
            self.put(top, max(0, w - len(pos) - 1), pos, curses.A_DIM)

    def draw_row(self, y, idx, cols, w):
        s = self.rows[idx]
        state = s.get("state") or "?"
        bucket, glyph = state_style(state)
        selected = idx == self.sel
        base = curses.A_REVERSE if selected else 0
        if state in ATTENTION_STATES and not selected:
            base |= curses.A_BOLD

        tokens = context_tokens(s)
        values = [
            ("ENG", "cl" if s.get("engine") == "claude" else "cx", base | curses.A_BOLD),
            ("STATE", f"{glyph} {state}", self.attr(bucket, base)),
            ("SHORT", s.get("short", "?")[:9], base | curses.A_DIM),
            ("NAME", clean(s.get("name"), 18) or "—", base),
            ("TURNS", str(s.get("turns") or 0), base),
            ("CONTEXT", fmt_tokens(tokens), base),
            ("LAST", fmt_age(s.get("last_turn_at") or s.get("created_at")), base),
            ("CWD", clean(s.get("cwd")), base | curses.A_DIM),
        ]
        # Fill the whole row with the selection bar first.
        if selected:
            self.put(y, 0, " " * (w - 1), base)
        x = 0
        cw_by = dict(cols)
        for name, text, attr in values:
            cw = cw_by[name]
            field = text if cw == 0 else f"{text:<{cw}}"[:cw]
            self.put(y, x, field, attr)
            x += (cw or (w - x)) + 1

    def draw_detail(self, top, height, w, snap, sel):
        self.put(top, 0, "─" * w, curses.A_DIM)
        if not sel:
            self.conversation_view.configure(None, None, w - 2, max(1, height - 2))
            self.put(top + 1, 1, "no session selected", curses.A_DIM)
            return
        worker = session_identity(sel)
        status = snap["statuses"].get(worker)
        state = (status or sel).get("state") or "?"
        bucket, glyph = state_style(state)
        name = sel.get("name") or sel.get("short")
        self.put(top, 0, f" {glyph} {name} [{sel.get('short')}] · {sel.get('engine')} ",
                 self.attr(bucket, curses.A_BOLD))

        gate = pending_input(status)
        turn = (status or {}).get("turn")
        if turn is None:
            turn = sel.get("turns")
        pending = gate.get("kind") or "none"
        detail = ""
        if first_question_payload(status):
            question = first_question_payload(status)
            detail = " · " + clean(question.get("question") or question.get("prompt"), 45)
        elif gate:
            raw = (status or {}).get("waiting_for") or {}
            detail = " · " + clean(raw.get("tool") or raw.get("message"), 40)
        bg = (status or {}).get("background_task_summary") or {}
        if bg.get("available") is False:
            bg_text = " · bg unavailable"
        elif bg.get("running_count") is not None:
            bg_text = (f" · bg {bg['running_count']} running,"
                       f" {bg.get('pending_count', 0)} pending")
        else:
            bg_text = ""
        header = (f" {state} · turn {turn if turn is not None else '—'} · "
                  f"ctx {fmt_tokens(context_tokens(status))} · pending: {pending}"
                  f"{bg_text}{detail}")
        self.put(top + 1, 1, header, self.attr(C_MAGENTA) if gate else curses.A_DIM)
        y, bottom = top + 2, top + height
        self.detail_page_rows = max(1, bottom - y)
        if y >= bottom:
            return
        if self.show_events:
            self.draw_events(y, bottom, w, worker)
            return
        doc = snap["conversation"] if snap["conversation_worker"] == worker else None
        self.conversation_view.configure(worker, doc, w - 2, bottom - y)
        if doc is None:
            error = (snap["conversation_error"] if snap["conversation_worker"] == worker
                     else None) or "fetching conversation…"
            self.put(y, 1, "conversation: " + clean(error, w - 17), curses.A_DIM)
            return
        error = snap["conversation_error"] if snap["conversation_worker"] == worker else None
        if error:
            self.put(y, 1, "conversation: stale — " + clean(error, w - 23), curses.A_DIM)
            y += 1
        for line_y, (_, _, text, color) in enumerate(self.conversation_view.visible(), start=y):
            if line_y >= bottom:
                break
            self.put(line_y, 1, text, self.attr(color))

    def draw_events(self, y, bottom, w, short):
        self.put(y, 1, "live events (ccx events):", curses.A_BOLD)
        y += 1
        events = self.streamer.get(short)
        if not events:
            self.put(y, 2, "waiting for events… (only managed, live sessions stream)", curses.A_DIM)
            return y + 1
        for evt in events[-(bottom - y):]:
            if y >= bottom:
                break
            self.put(y, 2, self.fmt_event(evt), curses.A_DIM)
            y += 1
        return y

    @staticmethod
    def fmt_event(evt) -> str:
        """Use the engine-neutral event-feed kind, not Claude's native type."""
        kind = evt.get("kind") or evt.get("type") or "?"
        if kind == "state":
            return f"state: {evt.get('from')} → {evt.get('to')}"
        if kind == "background.changed":
            item = evt.get("item") or {}
            bg = evt.get("summary") or {}
            return (f"background {evt.get('transition') or 'changed'}: "
                    f"{item.get('label') or item.get('id') or 'snapshot'} "
                    f"· running={bg.get('running_count', '?')} "
                    f"pending={bg.get('pending_count', '?')}")
        if kind == "turn.ended":
            return f"turn ended: {evt.get('outcome') or evt.get('turn_ref') or '?'}"
        if kind == "turn.started":
            return f"turn started: {evt.get('turn_ref') or '?'}"
        if kind in ("tool.started", "tool.finished"):
            return f"{kind}: {evt.get('tool') or evt.get('name') or '?'}"
        if kind in ("input.requested", "input.resolved"):
            return f"{kind}: {evt.get('request_id') or '?'}"
        if kind == "error":
            return f"error: {clean(evt.get('message') or evt.get('reason') or '', 80)}"
        return kind.replace("_", " ")

    def draw_footer(self, y, w, snap):
        msg = snap["status_msg"]
        if msg and time.time() - snap["status_ts"] < 4:
            self.put(y, 0, " " + clean(msg, w - 2), self.attr(C_CYAN, curses.A_REVERSE))
            return
        if self.search:
            self.put(y, 0, f" /{self.search}", self.attr(C_CYAN))
            keys = "  Esc clear search"
            self.put(y, max(0, w - len(keys) - 1), keys, curses.A_DIM)
            return
        sel = self.selected_row()
        worker = session_identity(sel) if sel else None
        status = snap["statuses"].get(worker) if worker else None
        hints = footer_hints(sel, status)
        self.put(y, 0, " " + hints, curses.A_DIM)

    def draw_help(self, w, h):
        lines = [
            "  ccx dashboard — keys",
            "",
            "  ↑ / k        previous worker    ↓ / j      next worker",
            "  g / G        first / last worker  Tab        active / all scope",
            "  PgUp/PgDn    scroll conversation   Home       start of conversation",
            "  End          live bottom           mouse wheel scroll conversation",
            "  Enter        attach selected worker  Esc        clear search",
            "  d            toggle detail         e          toggle event feed",
            "  o            cycle sort            /          search worker",
            "  r            refresh now           ?          this help",
            "",
            "  s            send (only idle/stopped, not terminal-only)",
            "  A / X        approve / reject a pending permission",
            "  a            answer a pending Claude question",
            "  K            kill selected worker (confirm)",
            "  q            quit",
            "",
            "  Enter opens the native worker UI; quit/detach to return.",
            "  Read-mostly: Enter / s / A / X / a / K operate on workers.",
            "  Press any key to close.",
        ]
        bw = min(w - 4, max(len(l) for l in lines) + 4)
        bh = len(lines) + 2
        y0 = max(0, (h - bh) // 2)
        x0 = max(0, (w - bw) // 2)
        for i in range(bh):
            self.put(y0 + i, x0, " " * bw, curses.A_REVERSE)
        for i, line in enumerate(lines):
            self.put(y0 + 1 + i, x0 + 1, f"{line:<{bw - 2}}", curses.A_REVERSE)

    def on_resize(self):
        """Coalesce a burst of resize events into one full repaint (no per-event flicker)."""
        self.stdscr.nodelay(True)
        try:
            ch = self.stdscr.getch()
            while ch == curses.KEY_RESIZE:
                ch = self.stdscr.getch()
            if ch != -1:
                curses.ungetch(ch)  # a key typed during the drag still counts
        finally:
            self.stdscr.nodelay(False)
            self.stdscr.timeout(200)
        if hasattr(curses, "update_lines_cols"):
            curses.update_lines_cols()
        self.stdscr.clearok(True)

    # --- input handling ---------------------------------------------------- #
    def prompt(self, label: str) -> str | None:
        """Blocking single-line editor on the footer row. Returns None on Esc."""
        buf = ""
        try:
            curses.curs_set(1)
        except curses.error:
            pass
        try:
            while True:
                h, w = self.stdscr.getmaxyx()
                y = h - 1
                self.put(y, 0, " " * (w - 1))
                self.put(y, 0, " " + label, self.attr(C_CYAN, curses.A_BOLD))
                self.put(y, len(label) + 2, buf)
                self.stdscr.move(y, min(w - 1, len(label) + 2 + len(buf)))
                self.stdscr.refresh()
                ch = self.stdscr.getch()
                if ch == curses.KEY_RESIZE:
                    self.on_resize()
                    self.draw()
                    continue
                if ch in (10, 13, curses.KEY_ENTER):
                    return buf
                if ch == 27:  # Esc
                    return None
                if ch in (curses.KEY_BACKSPACE, 127, 8):
                    buf = buf[:-1]
                elif 32 <= ch < 127:
                    buf += chr(ch)
        finally:
            try:
                curses.curs_set(0)
            except curses.error:
                pass

    def confirm(self, question: str) -> bool:
        ans = self.prompt(f"{question} (y/N): ")
        return bool(ans) and ans.strip().lower().startswith("y")

    # --- actions (run ccx, off the render path) --------------------------- #
    def action_send(self, sel):
        worker = session_identity(sel)
        if not worker:
            self.store.set_status("full worker identity unavailable; send refused")
            return
        if not can_send(sel):
            self.store.set_status("send requires recorded idle/stopped non-terminal worker")
            return
        text = self.prompt(f"send to {sel.get('name') or sel.get('short')}: ")
        if not text:
            return
        short = worker
        self.store.set_status(f"sending to {short}…")

        def work():
            proc = run_ccx(self.ccx_bin, ["send", short, text, "--no-wait", "--json"], timeout=30)
            if proc.returncode == 0:
                self.store.set_status(f"sent / queued to {short}")
            else:
                self.store.set_status(f"send failed: {clean(proc.stderr or proc.stdout, 80)}")
            self.poller.kick()

        threading.Thread(target=work, daemon=True).start()

    def action_kill(self, sel):
        short = session_identity(sel)
        if not short:
            self.store.set_status("full worker identity unavailable; kill refused")
            return
        if not self.confirm(f"kill {sel.get('name') or short}?"):
            self.store.set_status("kill cancelled")
            return
        self.store.set_status(f"killing {short}…")

        def work():
            # `ccx kill` now confirms termination, so bound its own budget
            # below this thread's subprocess timeout rather than racing it.
            proc = run_ccx(self.ccx_bin, ["kill", short, "--timeout", "15"],
                           timeout=20)
            self.store.set_status(f"killed {short}" if proc.returncode == 0
                                  else f"kill failed: {clean(proc.stderr, 80)}")
            self.poller.kick()

        threading.Thread(target=work, daemon=True).start()

    def action_permission(self, sel, approve: bool):
        short = session_identity(sel)
        if not short:
            self.store.set_status("full worker identity unavailable; input refused")
            return
        verb = "approve" if approve else "reject"
        snap = self.store.snapshot()
        status = snap["statuses"].get(short)
        gate = pending_input(status)
        request_id = gate.get("request_id")
        if gate.get("kind") != "permission" or not isinstance(request_id, str) or not request_id:
            self.store.set_status("no permission request ID — refresh the focused worker")
            return
        if not self._inflight_guard(short, verb):
            self.store.set_status(f"{verb} in progress…")
            return
        self.store.set_status(f"{verb} {short}…")

        def work():
            try:
                argv = [verb, short, "--json", "--request-id", request_id]
                proc = run_ccx(self.ccx_bin, argv, timeout=30)
                past = "approved" if approve else "rejected"
                self.store.set_status(f"{past} {short}" if proc.returncode == 0
                                      else f"{verb} failed: {clean(proc.stderr or proc.stdout, 80)}")
                self.poller.kick()
                self.peeker.wake()
            finally:
                self._inflight_release(short, verb)

        threading.Thread(target=work, daemon=True).start()

    def action_answer(self, sel):
        """Answer a pending AskUserQuestion for the full worker identity."""
        short = session_identity(sel)
        if not short:
            self.store.set_status("full worker identity unavailable; answer refused")
            return
        if sel.get("engine") == "codex":
            self.store.set_status("ccx answer is Claude-only")
            return
        snap = self.store.snapshot()
        status = snap["statuses"].get(short)
        q = first_question_payload(status)
        request_id = pending_input(status).get("request_id")
        if not q or not isinstance(request_id, str) or not request_id:
            self.store.set_status("no question/request ID — refresh the focused worker")
            return
        if not self._inflight_guard(short, "answer"):
            self.store.set_status("answer in progress…")
            return
        opts = q.get("options") or []
        opt_labels = [opt.get("label") if isinstance(opt, dict) else str(opt) for opt in opts]
        prompt_q = clean(q.get("question") or q.get("prompt") or "answer")
        if opt_labels:
            choices = "  ".join(f"{i + 1}:{lbl}" for i, lbl in enumerate(opt_labels[:6]))
            answer = self.prompt(f"{prompt_q} [{choices}]: ")
        else:
            answer = self.prompt(f"{prompt_q}: ")
        if not answer:
            self._inflight_release(short, "answer")
            return
        answer = answer.strip()
        # Answer-by-number: when the question has choices and the user typed a
        # valid 1-based option number, resolve it to the option label and submit
        # via the structured `--option N LABEL` path (N=1 — the dashboard surfaces
        # a single pending question). Anything else (a typed label, free text) is
        # passed through as positional answer text.
        if opt_labels and answer.isdigit() and 1 <= int(answer) <= len(opt_labels):
            argv = ["answer", short, "--option", "1", opt_labels[int(answer) - 1]]
        else:
            argv = ["answer", short, answer]
        argv += ["--request-id", request_id]
        self.store.set_status(f"answering {short}…")

        def work():
            try:
                proc = run_ccx(self.ccx_bin, argv, timeout=30)
                self.store.set_status(f"answered {short}" if proc.returncode == 0
                                      else f"answer failed: {clean(proc.stderr or proc.stdout, 80)}")
                self.poller.kick()
                self.peeker.wake()
            finally:
                self._inflight_release(short, "answer")

        threading.Thread(target=work, daemon=True).start()

    def action_attach(self, row: dict) -> None:
        """Give the native TUI the actual terminal, then restore curses."""
        worker = session_identity(row)
        if not worker or row.get("terminal_only"):
            self.store.set_status("attach unavailable for this worker")
            return
        curses.def_prog_mode()
        curses.endwin()
        try:
            try:
                result = subprocess.run([self.ccx_bin, "attach", worker], check=False)
                exit_code = result.returncode
            except OSError as error:
                print(f"attach failed: {error}", flush=True)
                exit_code = 127
            if exit_code:
                try:
                    input(f"attach exited {exit_code} — press Enter to return")
                except EOFError:
                    pass
        finally:
            curses.reset_prog_mode()
            try:
                curses.curs_set(0)
            except curses.error:
                pass
            self.stdscr.clear()
            self.on_resize()
            self.poller.kick()
            self.peeker.wake()

    def handle_key(self, ch) -> bool:
        """Return False to quit."""
        if self.help:
            self.help = False
            return True
        if ch in (ord("q"),):
            return False
        if ch in (curses.KEY_DOWN, ord("j")):
            self._move(1)
        elif ch in (curses.KEY_UP, ord("k")):
            self._move(-1)
        elif ch == ord("g"):
            self._goto(0)
        elif ch == ord("G"):
            self._goto(len(self.rows) - 1)
        elif ch == curses.KEY_NPAGE:
            if self.show_detail and not self.show_events:
                self.conversation_view.scroll(self.detail_page_rows)
        elif ch == curses.KEY_PPAGE:
            if self.show_detail and not self.show_events:
                self.conversation_view.scroll(-self.detail_page_rows)
        elif ch == curses.KEY_HOME:
            self.conversation_view.home()
        elif ch == curses.KEY_END:
            self.conversation_view.end()
        elif ch == curses.KEY_MOUSE and self.show_detail and not self.show_events:
            try:
                _, _, _, _, buttons = curses.getmouse()
                if buttons & getattr(curses, "BUTTON4_PRESSED", 0):
                    self.conversation_view.scroll(-3)
                elif buttons & getattr(curses, "BUTTON5_PRESSED", 0):
                    self.conversation_view.scroll(3)
            except curses.error:
                pass
        elif ch in (curses.KEY_ENTER, 10, 13):
            sel = self.selected_row()
            if sel:
                self.action_attach(sel)
        elif ch == 27:
            if self.search:
                self.search = ""
        elif ch == ord("d"):
            self.show_detail = not self.show_detail
        elif ch == ord("e"):
            self.show_events = not self.show_events
        elif ch == ord("\t"):
            self.scope = SCOPES[(SCOPES.index(self.scope) + 1) % len(SCOPES)]
            self.poller.set_scope(self.scope)
        elif ch == ord("o"):
            self.sort = SORTS[(SORTS.index(self.sort) + 1) % len(SORTS)]
        elif ch == ord("r"):
            self.poller.kick()
            self.peeker.wake()
            self.store.set_status("refreshing…")
        elif ch == ord("/"):
            res = self.prompt("search: ")
            self.search = res or ""
            self.sel_short = None  # let _sync_selection re-anchor to the new top row
            self._goto(0)
        elif ch == ord("?"):
            self.help = True
        elif ch in (ord("s"), ord("A"), ord("X"), ord("a"), ord("K")):
            sel = self.selected_row()
            if not sel:
                self.store.set_status("no session selected")
            else:
                status = self.store.snapshot()["statuses"].get(session_identity(sel))
                gate = pending_input(status)
                if ch == ord("K"):
                    self.action_kill(sel)
                elif ch == ord("s") and can_send(sel):
                    self.action_send(sel)
                elif ch in (ord("A"), ord("X")) and gate.get("kind") == "permission" and gate.get("request_id"):
                    self.action_permission(sel, approve=ch == ord("A"))
                elif (ch == ord("a") and sel.get("engine") == "claude"
                      and gate.get("kind") == "question" and gate.get("request_id")
                      and first_question_payload(status)):
                    self.action_answer(sel)
                else:
                    self.store.set_status("action unavailable for this worker's recorded state/input")
        return True

    # --- main loop --------------------------------------------------------- #
    def run(self):
        try:
            curses.curs_set(0)
        except curses.error:
            pass
        self.stdscr.timeout(200)  # ~5 fps redraw; key wakes us sooner
        self.init_colors()
        try:
            curses.mousemask(getattr(curses, "BUTTON4_PRESSED", 0) |
                             getattr(curses, "BUTTON5_PRESSED", 0))
        except curses.error:
            pass
        self.poller.start()
        self.peeker.start()
        self.poller.kick()
        try:
            while True:
                self.draw()
                ch = self.stdscr.getch()
                if ch == -1:
                    continue
                if ch == curses.KEY_RESIZE:
                    self.on_resize()
                    continue
                if not self.handle_key(ch):
                    break
        finally:
            self.poller.stop_event.set()
            self.peeker.stop_event.set()
            self.peeker.wake()
            self.streamer.stop()


# --------------------------------------------------------------------------- #
# Headless snapshot (--once / --snapshot mode)
# --------------------------------------------------------------------------- #
def snapshot_json(ccx_bin: str, scope: str, runner=None) -> dict:
    """One-shot recorded inventory plus installed-engine quota observations.

    `usage` is keyed by provider (claude/codex), present only after successful
    CCX reads. `usage_errors` names installed engines whose CCX quota read
    failed. Missing provider executables never cause usage commands or entries.
    Counters remain null when not yet observed, including new Codex threads.
    """
    _rj = runner if runner is not None else run_ccx_json
    sessions, list_err = _list_rows(ccx_bin, scope, _rj)
    statuses, status_error = fetch_statuses(
        ccx_bin, [session_identity(r) for r in (sessions or []) if session_identity(r)], _rj)
    statuses = statuses or {}
    engines = available_engines()
    usage, usage_errors = {}, {}
    for engine in engines:
        data, error = fetch_usage(ccx_bin, engine, _rj)
        if error:
            usage_errors[engine] = "usage unavailable"
        else:
            usage[engine] = data

    if sessions is None:
        return {"scope": scope, "error": list_err or "could not fetch sessions",
                "sessions": [], "live_count": 0, "attention_count": 0,
                "usage": usage, "usage_errors": usage_errors}

    normalized = [{
        "worker": s.get("worker"), "worker_id": s.get("worker_id"),
        "engine": s.get("engine"), "short": s.get("short"),
        "name": s.get("name"), "state": s.get("state"),
        "terminal_only": bool(s.get("terminal_only")),
        "cwd": s.get("cwd"), "turns": s.get("turns"),
        "input_tokens": s.get("input_tokens"),
        "output_tokens": s.get("output_tokens"),
        "tokens": token_total(s), "context": statuses.get(session_identity(s), {}).get("context"),
        "last_turn_at": s.get("last_turn_at"),
    } for s in sessions]
    return {
        "scope": scope,
        "live_count": sum(s.get("state") in LIVE_STATES for s in sessions),
        "attention_count": sum(s.get("state") in ATTENTION_STATES for s in sessions),
        "sessions": normalized,
        "usage": usage,
        "usage_errors": usage_errors,
    }


def render_snapshot(ccx_bin: str, scope: str, runner=None) -> str:
    """Render the same engine-neutral snapshot used by --once --json."""
    snap = snapshot_json(ccx_bin, scope, runner=runner)
    lines: list[str] = [f"=== ccx dashboard snapshot (scope: {scope}) ==="]
    if snap.get("error"):
        lines.append(f"ERROR: {snap['error']}")
        return "\n".join(lines) + "\n"
    sessions = snap["sessions"]
    lines.append(f"SESSIONS: {len(sessions)}  LIVE: {snap['live_count']}  "
                 f"NEED-ATTENTION: {snap['attention_count']}")
    quotas = [f"{label}:{pct:.0f}%" for label, pct in usage_gauges(snap["usage"])]
    quotas.extend(f"{engine}: usage unavailable" for engine in ENGINES
                  if engine in snap["usage_errors"])
    lines.append("USAGE: " + ("  ".join(quotas) if quotas else "(no data)"))
    if not sessions:
        lines.append("(no sessions)")
        return "\n".join(lines) + "\n"

    def sort_key(s):
        state = s.get("state") or ""
        rank = 0 if state in ATTENTION_STATES else (1 if state in LIVE_STATES else 2)
        return (rank, -(s.get("last_turn_at") or 0))

    W = {"ENG": 6, "STATE": 14, "SHORT": 9, "NAME": 18, "TURNS": 6,
         "CONTEXT": 8, "LAST": 6}
    hdr = (f"  {'ENG':<{W['ENG']}} {'STATE':<{W['STATE']}} {'SHORT':<{W['SHORT']}} "
           f"{'NAME':<{W['NAME']}} {'TURNS':>{W['TURNS']}} "
           f"{'CONTEXT':>{W['CONTEXT']}} {'LAST':>{W['LAST']}}  CWD")
    lines.extend([hdr, "  " + "-" * (len(hdr) - 2)])
    for s in sorted(sessions, key=sort_key):
        state = s.get("state") or "?"
        _, glyph = state_style(state)
        state_col = f"{glyph} {state}"
        row = (f"  {s.get('engine') or '?':<{W['ENG']}} "
               f"{state_col:<{W['STATE']}} {(s.get('short') or '')[:9]:<{W['SHORT']}} "
               f"{clean(s.get('name'), 18) or '—':<{W['NAME']}} "
               f"{str(s.get('turns') if s.get('turns') is not None else '—'):>{W['TURNS']}} "
               f"{fmt_tokens(context_tokens(s)):>{W['CONTEXT']}} "
               f"{fmt_age(s.get('last_turn_at')):>{W['LAST']}}  {clean(s.get('cwd'))}")
        lines.append(row)
    return "\n".join(lines) + "\n"


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #
def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="dashboard",
        description="Live interactive TUI dashboard for ccx sessions (htop-style).",
    )
    p.add_argument("--ccx-bin", default=None,
                   help="Path to the ccx executable (default: $CCX_BIN, then PATH, then ../ccx.py).")
    p.add_argument("--scope", default="active", choices=SCOPES,
                   help="Initial session scope (default: active). Cycle live with Tab.")
    p.add_argument("--once", "--snapshot", dest="once", action="store_true",
                   help="Print a one-shot snapshot to stdout and exit. "
                        "No curses or tty required; suitable for CI / scripts.")
    p.add_argument("--json", dest="json_output", action="store_true",
                   help="With --once: emit a machine-readable JSON snapshot instead of "
                        "human text. Has no effect (and is rejected) without --once.")
    args = p.parse_args(argv)
    ccx_bin = args.ccx_bin or find_ccx_bin()

    # Fail fast with a friendly message if ccx isn't runnable at all.
    probe = run_ccx(ccx_bin, ["--help"], timeout=10)
    if probe.returncode not in (0, 1):  # argparse --help exits 0; tolerate 1 too
        sys.stderr.write(
            f"[dashboard] could not run ccx ({ccx_bin!r}): "
            f"{clean(probe.stderr, 200) or 'not found'}\n"
            "Point at it with --ccx-bin or $CCX_BIN.\n"
        )
        return 2

    if args.json_output and not args.once:
        sys.stderr.write("[dashboard] --json requires --once\n")
        return 2

    if args.once:
        if args.json_output:
            snapshot = snapshot_json(ccx_bin, args.scope)
            sys.stdout.write(json.dumps(snapshot) + "\n")
            if snapshot.get("error"):
                return 1
        else:
            snapshot = render_snapshot(ccx_bin, args.scope)
            sys.stdout.write(snapshot)
            if "\nERROR: " in snapshot:
                return 1
        return 0

    if not sys.stdout.isatty():
        sys.stderr.write("[dashboard] needs an interactive terminal (stdout is not a tty)\n")
        return 2

    def _run(stdscr):
        App(stdscr, ccx_bin, args.scope).run()

    try:
        curses.wrapper(_run)
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
