"""Read one Codex thread's history from Codex's local history database.

Codex keeps a SQLite projection of every thread's rollout,
``$CODEX_HOME/thread_history_1.sqlite``. Its ``thread_items.item_json`` holds
the same ThreadItem JSON app-server returns, so a thread rebuilt from it feeds
:func:`ccx.ops.conversation.build_codex` unchanged and yields the same turn
sequence and item ordinals. Reading it takes milliseconds where app-server
paging can take minutes, and loads nothing into codexd.

It is Codex's private store, so every read is defensive: opened read-only
(``mode=ro``, never ``immutable=1``, so SQLite still honours the WAL), gated on
the tables and columns used here, short, and always closed. Any doubt returns
``None`` and the caller falls back to app-server ``thread/read``.

The thread id must come from the worker's verified original source
(``codex.telemetry._original_source``), never from a record field read here.
"""
from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from urllib.parse import quote

HISTORY_DB_NAME = "thread_history_1.sqlite"

# The schema this reader was checked against (2026-11, Codex `_sqlx_migrations`
# version 8). A newer version is still read when the required columns exist;
# the document then says so in a warning.
VERIFIED_SCHEMA_VERSION = 8

_REQUIRED = {
    "thread_turns": {"thread_id", "turn_id", "rollout_ordinal", "status",
                     "error_json", "started_at", "completed_at", "duration_ms"},
    "thread_items": {"thread_id", "turn_id", "item_id", "rollout_ordinal",
                     "item_json"},
    "_sqlx_migrations": {"version", "success"},
}
_TURN_STATUSES = frozenset({"completed", "failed", "inProgress", "interrupted"})
_BUSY_TIMEOUT_S = 2.0


def history_db_path() -> Path:
    # Like Codex itself, an empty CODEX_HOME means unset.
    return Path(os.environ.get("CODEX_HOME") or "~/.codex").expanduser() / HISTORY_DB_NAME


def _schema_ok(conn: sqlite3.Connection) -> bool:
    for table, columns in _REQUIRED.items():
        found = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        if not columns <= found:
            return False
    return True


def read_thread_db(thread_id: str, *, path: Path | None = None) -> dict | None:
    """Return the ``thread/read(includeTurns)`` shape for one thread, or ``None``.

    Turns come in ``rollout_ordinal`` order, including turns with no items;
    each turn's items come in their own ``rollout_ordinal`` order. The result
    carries ``historySource: "codex-history-db"`` and ``historyDbVersion``.
    """
    if not isinstance(thread_id, str) or not thread_id:
        return None
    db = path or history_db_path()
    if not db.is_file():
        return None
    try:
        conn = sqlite3.connect(f"file:{quote(str(db))}?mode=ro", uri=True,
                               timeout=_BUSY_TIMEOUT_S)
    except sqlite3.Error:
        return None
    try:
        if not _schema_ok(conn):
            return None
        row = conn.execute(
            "SELECT max(version) FROM _sqlx_migrations WHERE success").fetchone()
        version = row[0] if row else None
        # One read transaction: turns and items come from the same snapshot.
        conn.execute("BEGIN")
        turn_rows = conn.execute(
            "SELECT turn_id, status, error_json, started_at, completed_at, duration_ms"
            " FROM thread_turns WHERE thread_id = ? ORDER BY rollout_ordinal",
            (thread_id,)).fetchall()
        item_rows = conn.execute(
            "SELECT turn_id, item_json FROM thread_items"
            " WHERE thread_id = ? ORDER BY rollout_ordinal",
            (thread_id,)).fetchall()
        conn.execute("COMMIT")
    except sqlite3.Error:
        return None
    finally:
        conn.close()

    if not turn_rows:
        return None
    items: dict[str, list[dict]] = {}
    for turn_id, item_json in item_rows:
        try:
            item = json.loads(item_json)
        except (TypeError, ValueError):
            return None
        if not isinstance(item, dict):
            return None
        items.setdefault(turn_id, []).append(item)
    known = {r[0] for r in turn_rows}
    if set(items) - known:
        return None  # items of a turn the projection doesn't list: not coherent

    turns = []
    for turn_id, status, error_json, started, completed, duration in turn_rows:
        if status not in _TURN_STATUSES:
            return None
        error = None
        if error_json:
            try:
                error = json.loads(error_json)
            except ValueError:
                return None
        turns.append({
            "id": turn_id, "status": status, "error": error,
            "startedAt": started, "completedAt": completed, "durationMs": duration,
            "items": items.get(turn_id, []), "itemsView": "full",
        })
    return {"id": thread_id, "turns": turns, "historySource": "codex-history-db",
            "historyDbVersion": version}


__all__ = ["HISTORY_DB_NAME", "VERIFIED_SCHEMA_VERSION", "history_db_path",
           "read_thread_db"]
