from __future__ import annotations
import json
import os
import pathlib
import stat
import time
from ccx.ops.semantics import KIND_ELICITATION
from ccx.constants import (
    CCX_EVENTS_KEEP,
    CCX_EVENTS_MAX,
    EVENT_PREVIEW_LIMIT,
    EVENT_SCHEMA_VERSION,
)


def _permission_preview(req: dict | None) -> dict | None:
    """Build the persisted/broadcast preview of a pending permission request.

    Large arbitrary tool inputs (e.g. a big Write) are truncated to a short
    preview so the on-disk record and broadcast stay small. AskUserQuestion is the
    exception: its `input.questions` is the structure `ccx answer` must read to map
    options/text to the right question and validate labels. Truncating it away
    would leave the answer path with zero questions and produce misleading errors
    for long question sets, so AUQ input is always kept whole."""
    if not req:
        return None
    if req.get("kind") == KIND_ELICITATION:
        return _elicitation_preview(req)
    tool_name = req.get("tool_name")
    tool_input = req.get("tool_input")
    try:
        rendered = json.dumps(tool_input, ensure_ascii=False)
    except TypeError:
        rendered = json.dumps(str(tool_input))
    if tool_name == "AskUserQuestion":
        safe_input = tool_input
    elif len(rendered) > 2000:
        safe_input = {"preview": rendered[:2000] + "…", "truncated": True}
    else:
        safe_input = tool_input
    return {
        "type": "permission",
        "id": req.get("id"),
        "tool": tool_name,
        "input": safe_input,
        "requested_at": int(req.get("requested_at", time.time()) * 1000),
        "message": req.get("message"),
    }


def _elicitation_preview(req: dict) -> dict:
    """Persisted/broadcast preview of a pending MCP elicitation.

    The schema is kept whole (it is what a responder must satisfy) but the free
    text is bounded. ``id`` is ``None`` for a dialog CCX only observed from a
    notification: it has no brokered hook request, so it cannot be answered."""
    message = req.get("message")
    if isinstance(message, str) and len(message) > 4000:
        message = message[:4000] + "…"
    return {
        "type": KIND_ELICITATION,
        "id": req.get("id"),
        "tool": None,
        "server": req.get("server"),
        "mode": req.get("mode") or "form",
        "message": message,
        "url": req.get("url"),
        "elicitation_id": req.get("elicitation_id"),
        "schema": req.get("schema"),
        "requested_at": int(req.get("requested_at", time.time()) * 1000),
    }


def _elicitation_request(data: dict, request_id: str | None) -> dict:
    """Host-side pending gate for one Elicitation hook payload."""
    schema = data.get("requested_schema")
    return {
        "kind": KIND_ELICITATION,
        "id": request_id,
        "server": data.get("mcp_server_name"),
        "mode": data.get("mode") or "form",
        "message": data.get("message"),
        "url": data.get("url"),
        "elicitation_id": data.get("elicitation_id"),
        "schema": schema if isinstance(schema, dict) else None,
        "requested_at": time.time(),
        "raw": data,
    }


def _stamp_event(evt: dict, *, short: str, seq: int, epoch: str, ts: float,
                 v: int = EVENT_SCHEMA_VERSION) -> dict:
    """Return a copy of `evt` with the standard envelope fields stamped on.

    The envelope (v/type/short/seq/epoch/ts) is authoritative: `seq` is a
    per-host monotonic counter, `epoch` identifies this host life (it changes on
    every respawn, so a subscriber can tell that `seq` numbering restarted), and
    `ts` is the broadcast wall-clock. `type` is supplied by the caller and left
    untouched; any other event-specific fields pass through unchanged."""
    out = dict(evt)
    out["v"] = v
    out["short"] = short
    out["seq"] = seq
    out["epoch"] = epoch
    out["ts"] = ts
    return out


def _event_preview(value, limit: int = EVENT_PREVIEW_LIMIT) -> str:
    """Bounded JSON-ish preview of an arbitrary tool input/result for an event.

    Always returns a short string so the broadcast and sidecar line stay small.
    Falls back to `str()` for anything not JSON-serializable; never raises."""
    if value is None:
        return ""
    try:
        rendered = json.dumps(value, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        rendered = str(value)
    if len(rendered) > limit:
        return rendered[:limit] + "…"
    return rendered


def _bounded_text(value, limit: int = EVENT_PREVIEW_LIMIT) -> str | None:
    """Bounded plain-text preview (no JSON quoting) for message-like fields.

    Returns None for an absent value so callers can omit/null the field rather
    than emit a misleading empty string."""
    if value is None:
        return None
    s = value if isinstance(value, str) else str(value)
    if len(s) > limit:
        return s[:limit] + "…"
    return s


def _hook_tool_use_id(data: dict):
    """Best-effort extraction of the tool_use id across hook payload spellings."""
    return (data.get("tool_use_id") or data.get("toolUseID")
            or data.get("toolUseId"))


def _externally_completed_question(req: dict | None, data: dict) -> bool:
    """Exact original input/name match for a native tool outcome (incl. failure).

    Hook result content can contain user answers; *only* the tool_input at
    invocation time is the correlation key. Tool-use IDs may be absent or
    inconsistent between PermissionRequest and PostToolUse, and may not make
    different original inputs equivalent.
    """
    return bool(
        isinstance(req, dict) and req.get("kind") != KIND_ELICITATION
        and isinstance(req.get("tool_name"), str)
        and isinstance(req.get("tool_input"), dict)
        and isinstance(data, dict)
        and req["tool_name"] == data.get("tool_name")
        and isinstance(data.get("tool_input"), dict)
        and req["tool_input"] == data["tool_input"]
    )


def _oldest_native_gate(published: dict | None, queued: list[dict], data: dict):
    """Oldest arrival wins when two native calls have identical full input."""
    for req in ([published] if published is not None else []) + list(queued):
        if _externally_completed_question(req, data):
            return req
    return None


def _hook_tool_result(data: dict):
    """The tool's result blob, accepting either `tool_response` (newer hook
    payloads) or `tool_output` (older spelling) as the key."""
    resp = data.get("tool_response")
    if resp is None:
        resp = data.get("tool_output")
    return resp


def _tool_started_event(data: dict, turn: int) -> dict:
    return {
        "type": "tool_started",
        "tool_use_id": _hook_tool_use_id(data),
        "tool": data.get("tool_name"),
        "input_preview": _event_preview(data.get("tool_input")),
        "turn": turn,
    }


def _tool_ok(data: dict) -> bool:
    """Best-effort success flag for a finished tool. Defaults True; flips to
    False only when the hook's result blob carries an explicit error marker."""
    resp = _hook_tool_result(data)
    if isinstance(resp, dict):
        if resp.get("is_error") or resp.get("isError"):
            return False
        if resp.get("error"):
            return False
    return True


def _tool_finished_event(data: dict, turn: int, duration_ms: int | None) -> dict:
    return {
        "type": "tool_finished",
        "tool_use_id": _hook_tool_use_id(data),
        "tool": data.get("tool_name"),
        "duration_ms": duration_ms,
        "ok": _tool_ok(data),
        "result_preview": _event_preview(_hook_tool_result(data)),
        "turn": turn,
    }


def _subagent_finished_event(data: dict, turn: int) -> dict:
    agent_id = (data.get("agent_id") or data.get("subagent_id")
                or data.get("agentId") or data.get("subagentId"))
    agent_type = (data.get("agent_type") or data.get("subagent_type")
                  or data.get("agentType") or data.get("subagentType"))
    last_message = (data.get("last_message") or data.get("lastMessage")
                    or data.get("result"))
    return {
        "type": "subagent_finished",
        "agent_id": agent_id,
        "agent_type": agent_type,
        # None (not "") when absent, so consumers don't see a misleading empty
        # message for a subagent whose last text the hook didn't carry.
        "last_message": _bounded_text(last_message),
        "turn": turn,
    }


def _count_lines(path) -> int:
    """Fast newline count for the sidecar (binary, chunked, no parsing). Used to
    seed the in-memory line counter at host start. Returns 0 if absent/unreadable."""
    path = pathlib.Path(path)
    n = 0
    try:
        with path.open("rb") as f:
            while True:
                chunk = f.read(1 << 20)
                if not chunk:
                    break
                n += chunk.count(b"\n")
    except OSError:
        return 0
    return n


def _rotate_events_file(path, *, max_lines: int = CCX_EVENTS_MAX,
                        keep_lines: int = CCX_EVENTS_KEEP) -> bool:
    """Bounded rotation: if `path` holds more than `max_lines` lines, rewrite it
    keeping only the last `keep_lines`. Returns True if a rotation happened.

    Atomic via a temp file + os.replace, so a concurrent reader always sees a
    complete file. The caller must reopen its append handle afterward — the old
    handle still points at the now-unlinked inode."""
    path = pathlib.Path(path)
    try:
        with path.open("r", encoding="utf-8") as f:
            lines = f.readlines()
    except (OSError, ValueError):
        return False
    if len(lines) <= max_lines:
        return False
    tail = lines[-keep_lines:]
    tmp = path.with_suffix(path.suffix + ".tmp")
    # Private like the original journal: readers refuse group/other bits, and
    # a plain open() would inherit the host's umask (0664 under umask 002).
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW | os.O_CLOEXEC,
                 0o600)
    os.fchmod(fd, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.writelines(tail)
    os.replace(tmp, path)
    return True


def _open_event_journal(store, short: str):
    """Append handle on the worker's private event journal, or None.

    Created 0600; an existing journal we own with loose group/other bits (from
    an older rotation) is tightened rather than abandoned. Anything else —
    not a regular file, another owner, a symlink — is refused.
    """
    with store.lease():
        with store._gen("workers", "claude", short) as fd:
            event_fd = os.open(
                "events.ndjson", os.O_WRONLY | os.O_CREAT | os.O_APPEND |
                os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=fd,
            )
            stat_event = os.fstat(event_fd)
            if not stat.S_ISREG(stat_event.st_mode) or stat_event.st_uid != os.getuid():
                os.close(event_fd)
                raise PermissionError("worker event journal is untrusted")
            if stat_event.st_mode & 0o077:
                os.fchmod(event_fd, 0o600)
    return os.fdopen(event_fd, "ab", buffering=0)
