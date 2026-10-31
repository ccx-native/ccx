"""Read-side lifecycle projection over Claude and Codex journals.

Cursor placement uses epoch and sequence when its byte-offset hint no longer
matches the journal. Unplaceable cursors report a gap and replay retained events.
Provider message content remains in the conversation and telemetry surfaces."""

from __future__ import annotations

import base64
import json
import os
import pathlib
from typing import Any

from ccx.codex.delivery import turn_ref_for as codex_turn_ref
from ccx.codex.evidence import (
    HEADER_METHOD,
    COMPACT_STARTED_METHOD,
    COMPACT_ENDED_METHOD,
    DELIVERY_CHANGED_METHOD,
    INPUT_REQUESTED_METHOD,
    INPUT_RESOLVED_METHOD,
    LIFECYCLE_METHOD,
    header_instance_id,
    journal_path as codex_journal_path,
)
from ccx.errors import InvalidRequest
from ccx.store.paths import events_ndjson_path
from ccx.store.worker_state import normalize_native_state

CURSOR_PREFIX = "ev1."
POLL_S = 0.2
PREVIEW_LIMIT = 500
LIMIT_DEFAULT = 200
LIMIT_MAX = 1000
HEARTBEAT_DEFAULT_S = 30.0

KIND_STATE = "state"
KIND_TURN_STARTED = "turn.started"
KIND_TURN_ENDED = "turn.ended"
KIND_TOOL_STARTED = "tool.started"
KIND_TOOL_FINISHED = "tool.finished"
KIND_INPUT_REQUESTED = "input.requested"
KIND_INPUT_RESOLVED = "input.resolved"
KIND_COMPACT_STARTED = "compact.started"
KIND_COMPACT_ENDED = "compact.ended"
KIND_ERROR = "error"
KIND_DELIVERY_CHANGED = "delivery.changed"
KIND_HEARTBEAT = "heartbeat"

KINDS = frozenset({
    KIND_STATE,
    KIND_TURN_STARTED,
    KIND_TURN_ENDED,
    KIND_TOOL_STARTED,
    KIND_TOOL_FINISHED,
    KIND_INPUT_REQUESTED,
    KIND_INPUT_RESOLVED,
    KIND_COMPACT_STARTED,
    KIND_COMPACT_ENDED,
    KIND_ERROR,
    KIND_DELIVERY_CHANGED,
})

ENGINES = ("claude", "codex")

# ---- Claude ------------------------------------------------------------------

_CLAUDE_KINDS = {
    "state": KIND_STATE,
    "turn_start": KIND_TURN_STARTED,
    "turn_complete": KIND_TURN_ENDED,
    "turn_settled": KIND_TURN_ENDED,
    "tool_started": KIND_TOOL_STARTED,
    "tool_finished": KIND_TOOL_FINISHED,
    "permission_requested": KIND_INPUT_REQUESTED,
    "elicitation_requested": KIND_INPUT_REQUESTED,
    "permission_approved": KIND_INPUT_RESOLVED,
    "permission_rejected": KIND_INPUT_RESOLVED,
    "permission_timeout": KIND_INPUT_RESOLVED,
    "question_answered": KIND_INPUT_RESOLVED,
    "question_answered_external": KIND_INPUT_RESOLVED,
    "elicitation_resolved": KIND_INPUT_RESOLVED,
    "compact_started": KIND_COMPACT_STARTED,
    "compact_complete": KIND_COMPACT_ENDED,
    "compact_failed": KIND_COMPACT_ENDED,
    "compact_unknown": KIND_COMPACT_ENDED,
    "api_error": KIND_ERROR,
    "send_failed": KIND_ERROR,
    "boot_stalled": KIND_ERROR,
}


def _claude_position(obj: dict[str, Any]) -> tuple[str | None, int | None]:
    epoch, seq = obj.get("epoch"), obj.get("seq")
    if not isinstance(epoch, str) or not epoch:
        epoch = None
    if not isinstance(seq, int) or isinstance(seq, bool):
        seq = None
    return epoch, seq


def _claude_project(obj: dict[str, Any]) -> dict[str, Any] | None:
    event_type = obj.get("type")
    if not isinstance(event_type, str):
        return None
    event = dict(obj)
    kind = _CLAUDE_KINDS.get(event_type)
    if event_type == "turn_complete" and "outcome" not in event:
        event["outcome"] = "interrupted" if event.get("interrupted") else "completed"
    if event_type in ("compact_complete", "compact_failed", "compact_unknown"):
        event["outcome"] = {
            "compact_complete": "completed",
            "compact_failed": "failed",
            "compact_unknown": "unknown",
        }[event_type]
    event["kind"] = kind
    return event


# ---- Codex -------------------------------------------------------------------

# Codex items that are a tool call, with the name the feed reports for them.
# MCP calls use Claude's `mcp__<server>__<tool>` spelling so one consumer
# rule matches both engines.
_CODEX_TOOL_ITEMS = {
    "commandExecution": "command",
    "fileChange": "apply_patch",
    "mcpToolCall": None,
    "dynamicToolCall": None,
    "collabAgentToolCall": None,
    "webSearch": "web_search",
    "imageView": "view_image",
    "imageGeneration": "image_generation",
}


def _preview(value: Any, limit: int = PREVIEW_LIMIT) -> str:
    """Bounded JSON-ish preview, the same rule as the Claude host's."""
    if value is None:
        return ""
    if isinstance(value, str):
        rendered = value
    else:
        try:
            rendered = json.dumps(value, ensure_ascii=False, default=str)
        except (TypeError, ValueError):
            rendered = str(value)
    if len(rendered) > limit:
        return rendered[:limit] + "…"
    return rendered


def _codex_tool_name(item: dict[str, Any]) -> str:
    item_type = item.get("type")
    if item_type == "mcpToolCall":
        return f"mcp__{item.get('server')}__{item.get('tool')}"
    name = _CODEX_TOOL_ITEMS.get(item_type)
    return name or str(item.get("tool") or item_type)


def _codex_tool_input(item: dict[str, Any]) -> Any:
    item_type = item.get("type")
    if item_type == "commandExecution":
        return item.get("command")
    if item_type == "fileChange":
        changes = item.get("changes")
        if isinstance(changes, list):
            return [
                change.get("path") for change in changes
                if isinstance(change, dict)
            ]
        return None
    if item_type == "webSearch":
        return item.get("query")
    if item_type in {"imageView", "imageGeneration"}:
        return item.get("path") or item.get("prompt")
    return item.get("arguments")


def _codex_tool_result(item: dict[str, Any]) -> Any:
    item_type = item.get("type")
    if item_type == "commandExecution":
        return item.get("aggregatedOutput")
    if item.get("error") is not None:
        return item.get("error")
    return item.get("result") or item.get("contentItems")


def _codex_tool_ok(item: dict[str, Any]) -> bool:
    if item.get("status") != "completed":
        return False
    exit_code = item.get("exitCode")
    return exit_code is None or exit_code == 0


def _codex_position(obj: dict[str, Any]) -> int | None:
    seq = obj.get("sequence")
    if not isinstance(seq, int) or isinstance(seq, bool):
        return None
    return seq


def _codex_project(obj: dict[str, Any]) -> dict[str, Any] | None:
    method = obj.get("method")
    params = obj.get("params")
    params = params if isinstance(params, dict) else {}
    observed = obj.get("observedAtMs")
    event: dict[str, Any] = {
        "type": method,
        "ts": observed / 1000.0 if isinstance(observed, int) else None,
    }
    turn_id = obj.get("turnId")
    if method == LIFECYCLE_METHOD:
        event.update(kind=KIND_STATE, **{
            "from": params.get("from"), "to": params.get("to"),
        })
        if "reason" in params:
            event["reason"] = params.get("reason")
        return event
    if method in {COMPACT_STARTED_METHOD, COMPACT_ENDED_METHOD}:
        event.update(
            kind=(KIND_COMPACT_STARTED if method == COMPACT_STARTED_METHOD
                  else KIND_COMPACT_ENDED),
            turn_id=turn_id if isinstance(turn_id, str) else params.get("turnId"),
            operation_id=params.get("operationId"),
            item_id=params.get("itemId"),
            compact_id=params.get("compactId"),
        )
        if event["turn_id"]:
            event["turn_ref"] = codex_turn_ref(event["turn_id"])
        if method == COMPACT_ENDED_METHOD:
            value = params.get("outcome")
            event["outcome"] = value if value in {"completed", "failed", "unknown"} else "unknown"
            if params.get("reason"):
                event["reason"] = _preview(params["reason"])
        return event
    if method == INPUT_REQUESTED_METHOD:
        request = params.get("request")
        request = request if isinstance(request, dict) else {}
        turn_id = request.get("turnId")
        # The never-repeating gate id is the protocol request id; the native
        # app-server id restarts with the app-server.
        event.update(
            kind=KIND_INPUT_REQUESTED,
            request_id=request.get("gateId") or request.get("requestId"),
            native_request_id=request.get("requestId"),
            request=request,
        )
        if isinstance(turn_id, str) and turn_id:
            event["turn_id"] = turn_id
            event["turn_ref"] = codex_turn_ref(turn_id)
        return event
    if method == DELIVERY_CHANGED_METHOD:
        # A trigger to reconcile this delivery id, never acceptance proof.
        event.update(
            kind=KIND_DELIVERY_CHANGED,
            delivery_id=params.get("deliveryId"),
            status=params.get("state"),
            route=params.get("route"),
            placement=params.get("placement"),
            turn_ref=params.get("turnRef"),
        )
        return event
    if method == INPUT_RESOLVED_METHOD:
        event.update(
            kind=KIND_INPUT_RESOLVED,
            request_id=params.get("gateId") or params.get("requestId"),
            native_request_id=params.get("requestId"),
            request_kind=params.get("kind"),
            decision=params.get("decision"),
        )
        return event
    if method not in {"turn/started", "turn/completed", "item/started",
                      "item/completed", "error"}:
        # The header, token usage and compaction-context enrichment are
        # journal bookkeeping and telemetry, not lifecycle events.
        return None
    if isinstance(turn_id, str) and turn_id:
        event["turn_id"] = turn_id
        event["turn_ref"] = codex_turn_ref(turn_id)
    if method == "turn/started":
        event["kind"] = KIND_TURN_STARTED
        return event
    if method == "turn/completed":
        turn = params.get("turn")
        turn = turn if isinstance(turn, dict) else {}
        error = turn.get("error")
        event.update(
            kind=KIND_TURN_ENDED,
            outcome=turn.get("status"),
            error=(
                _preview(error.get("message") if isinstance(error, dict) else error)
                if error else None
            ),
        )
        return event
    if method == "error":
        error = params.get("error")
        error = error if isinstance(error, dict) else {}
        event.update(
            kind=KIND_ERROR,
            message=_preview(error.get("message")),
            error_type=error.get("type"),
            will_retry=params.get("willRetry"),
        )
        return event
    item = params.get("item")
    item = item if isinstance(item, dict) else {}
    item_type = item.get("type")
    started = method == "item/started"
    event.update(
        item_id=obj.get("itemId"),
        item_type=item_type,
        native_ts_ms=obj.get("nativeTimestampMs"),
    )
    if item_type == "contextCompaction":
        # Native provider item notifications are forensic evidence only.
        # codexd projects each *admitted* compact as two sparse CCX-owned
        # feed records; a reader never infers a terminal from a foreign item.
        return None
    if item_type not in _CODEX_TOOL_ITEMS:
        # Content items belong to the conversation, not the lifecycle feed.
        return None
    event.update(
        kind=KIND_TOOL_STARTED if started else KIND_TOOL_FINISHED,
        tool_use_id=item.get("id"),
        tool=_codex_tool_name(item),
    )
    if started:
        event["input_preview"] = _preview(_codex_tool_input(item))
    else:
        event.update(
            ok=_codex_tool_ok(item),
            status=item.get("status"),
            duration_ms=item.get("durationMs"),
            result_preview=_preview(_codex_tool_result(item)),
        )
    return event


# ---- the journal scan --------------------------------------------------------

class _Line:
    """One parsed journal line and where it sits."""

    __slots__ = ("epoch", "seq", "end", "obj")

    def __init__(self, epoch: str | None, seq: int, end: int, obj: dict):
        self.epoch = epoch
        self.seq = seq
        self.end = end  # byte offset just past this line
        self.obj = obj


def journal_path(engine: str, short: str) -> pathlib.Path:
    if engine == "claude":
        return events_ndjson_path(short)
    return codex_journal_path(short)


def _file_id(path: pathlib.Path) -> tuple[int | None, int]:
    try:
        st = path.stat()
    except OSError:
        return None, 0
    return st.st_ino, st.st_size


def _scan(
    engine: str,
    path: pathlib.Path,
    *,
    start: int = 0,
    epoch: str | None = None,
) -> list[_Line]:
    """Parse complete lines from byte ``start``. A partial last line (a write
    still in flight) is left for the next read; malformed lines are skipped.

    For Codex, every line's epoch is the journal instance its header names;
    ``epoch`` seeds it when the scan starts past the header.
    """
    out: list[_Line] = []
    try:
        handle = path.open("rb")
    except OSError:
        return out
    with handle:
        if start:
            handle.seek(start)
        offset = start
        for raw in handle:
            if not raw.endswith(b"\n"):
                break
            offset += len(raw)
            line = raw.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except ValueError:
                continue
            if not isinstance(obj, dict):
                continue
            if engine == "claude":
                line_epoch, seq = _claude_position(obj)
            else:
                seq = _codex_position(obj)
                if obj.get("method") == HEADER_METHOD:
                    if epoch is None:
                        epoch = header_instance_id(obj)
                line_epoch = epoch
            if seq is None:
                continue
            out.append(_Line(line_epoch, seq, offset, obj))
    return out


def _line_before(engine: str, path: pathlib.Path, offset: int) -> dict | None:
    """The complete line that ends exactly at ``offset``, parsed, or None."""
    if offset <= 0:
        return None
    try:
        with path.open("rb") as handle:
            window = min(offset, 1 << 20)
            handle.seek(offset - window)
            chunk = handle.read(window)
    except OSError:
        return None
    if not chunk.endswith(b"\n"):
        return None
    body = chunk[:-1]
    cut = body.rfind(b"\n")
    if cut < 0 and window < offset:
        return None  # the line is longer than the window: rescan instead
    try:
        obj = json.loads(body[cut + 1:])
    except ValueError:
        return None
    return obj if isinstance(obj, dict) else None


# ---- cursors -----------------------------------------------------------------

def encode_cursor(
    engine: str,
    short: str,
    epoch: str | None,
    seq: int,
    offset: int,
    inode: int | None,
) -> str:
    payload = {"e": engine, "s": short, "p": epoch, "q": seq, "o": offset,
               "i": inode}
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return CURSOR_PREFIX + base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_cursor(value: str, *, engine: str, short: str) -> dict[str, Any]:
    """Parse a feed cursor for this worker, or raise ``InvalidRequest``."""
    if not isinstance(value, str) or not value.startswith(CURSOR_PREFIX):
        raise InvalidRequest("event cursor is invalid")
    body = value[len(CURSOR_PREFIX):]
    try:
        data = json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4)))
    except Exception as error:
        raise InvalidRequest("event cursor is invalid") from error
    if not isinstance(data, dict):
        raise InvalidRequest("event cursor is invalid")
    seq, offset = data.get("q"), data.get("o")
    if (
        not isinstance(seq, int) or isinstance(seq, bool) or seq < 0
        or not isinstance(offset, int) or isinstance(offset, bool) or offset < 0
        or not (data.get("p") is None or isinstance(data.get("p"), str))
        or not (data.get("i") is None or isinstance(data.get("i"), int))
    ):
        raise InvalidRequest("event cursor is invalid")
    if data.get("e") != engine or data.get("s") != short:
        raise InvalidRequest(
            f"event cursor belongs to {data.get('e')}:{data.get('s')}, "
            f"not {engine}:{short}"
        )
    return data


def _same_position(engine: str, obj: dict, cursor: dict) -> bool:
    if engine == "claude":
        return _claude_position(obj) == (cursor.get("p"), cursor.get("q"))
    return _codex_position(obj) == cursor.get("q")


def _place(
    engine: str, lines: list[_Line], cursor: dict
) -> tuple[list[_Line], dict[str, Any] | None]:
    """The lines after ``cursor`` in a full scan, plus a gap when the cursor
    cannot be placed and everything retained is replayed instead."""
    epoch, seq = cursor.get("p"), cursor.get("q")
    if epoch is None and seq == 0:
        # Taken before the journal held anything: everything is new.
        return lines, None
    if engine == "codex":
        current = lines[0].epoch if lines else None
        if lines and epoch != current:
            return lines, {"reason": "cursor_journal_mismatch",
                           "requested_epoch": epoch, "current_epoch": current}
        last = lines[-1].seq if lines else 0
        if seq > last:
            return lines, {"reason": "cursor_ahead_of_retained_tail",
                           "requested_seq": seq, "last_retained_seq": last}
        if lines and seq < lines[0].seq - 1:
            return lines, {"reason": "cursor_predates_retained_events",
                           "requested_seq": seq,
                           "first_retained_seq": lines[0].seq}
        return [line for line in lines if line.seq > seq], None
    # Claude: seq restarts with every host epoch, and the sidecar spans many.
    block = [index for index, line in enumerate(lines) if line.epoch == epoch]
    if not block:
        return lines, {"reason": "cursor_not_retained",
                       "requested_epoch": epoch, "requested_seq": seq}
    first, last = block[0], block[-1]
    if seq < lines[first].seq - 1:
        return lines[first:], {"reason": "cursor_predates_retained_events",
                               "requested_epoch": epoch, "requested_seq": seq,
                               "first_retained_seq": lines[first].seq}
    out = [line for line in lines[first:last + 1] if line.seq > seq]
    return out + lines[last + 1:], None


def _after(
    engine: str, path: pathlib.Path, cursor: dict | None
) -> tuple[list[_Line], dict[str, Any] | None, tuple[str | None, int, int]]:
    """Lines after ``cursor``, a gap if any, and the end-of-file position
    ``(epoch, seq, offset)`` a caller resumes from when nothing new lands."""
    inode, size = _file_id(path)
    if cursor is not None:
        offset = cursor["o"]
        if (
            cursor.get("i") is not None and cursor.get("i") == inode
            and offset <= size
            and (offset == 0 and cursor["q"] == 0
                 or _same_position(engine, _line_before(engine, path, offset) or {}, cursor))
        ):
            lines = _scan(engine, path, start=offset, epoch=cursor.get("p"))
            end = (lines[-1].epoch, lines[-1].seq, lines[-1].end) if lines else (
                cursor.get("p"), cursor["q"], offset)
            return lines, None, end
    lines = _scan(engine, path)
    end = (lines[-1].epoch, lines[-1].seq, lines[-1].end) if lines else (None, 0, 0)
    if cursor is None:
        return lines, None, end
    placed, gap = _place(engine, lines, cursor)
    return placed, gap, end


# ---- reading -----------------------------------------------------------------

def _project(engine: str, line: _Line) -> dict[str, Any] | None:
    if engine == "claude":
        return _claude_project(line.obj)
    return _codex_project(line.obj)


def _envelope(
    event: dict[str, Any], engine: str, short: str, line: _Line,
    inode: int | None,
) -> dict[str, Any]:
    event["worker"] = f"{engine}:{short}"
    event["engine"] = engine
    event["seq"] = line.seq
    event["epoch"] = line.epoch
    event["cursor"] = encode_cursor(engine, short, line.epoch, line.seq,
                                    line.end, inode)
    if engine == "codex":
        event["short"] = short
    return event


def _check_kinds(kinds) -> frozenset[str] | None:
    if kinds is None:
        return None
    if isinstance(kinds, str) or not all(isinstance(kind, str) for kind in kinds):
        raise InvalidRequest("kinds must be a list of event kinds")
    unknown = sorted(set(kinds) - KINDS)
    if unknown:
        raise InvalidRequest(
            "unknown event kind(s): " + ", ".join(unknown)
            + "; expected " + ", ".join(sorted(KINDS))
        )
    return frozenset(kinds)


def _check_limit(limit: int) -> int:
    if not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0:
        raise InvalidRequest("limit must be a positive integer")
    return min(limit, LIMIT_MAX)


def _check_tail(tail, after) -> int | None:
    if tail is None:
        return None
    if after is not None:
        raise InvalidRequest("tail cannot be combined with a cursor")
    if not isinstance(tail, int) or isinstance(tail, bool) or tail < 0:
        raise InvalidRequest("tail must be zero or a positive integer")
    return tail


def state_of(record: dict[str, Any] | None) -> dict[str, Any]:
    """The worker's recorded state, normalized, for a page or heartbeat."""
    native = ((record or {}).get("lifecycle") or {}).get("state")
    return {"state": normalize_native_state(native), "native_state": native}


def read_page(
    engine: str,
    short: str,
    *,
    after: str | None = None,
    tail: int | None = None,
    limit: int = LIMIT_DEFAULT,
    kinds=None,
    native: bool = False,
) -> dict[str, Any]:
    """One page of events after ``after`` (oldest first), or the newest
    ``tail`` events when there is no cursor; ``tail=0`` is "from now".

    ``next_cursor`` always names where to resume: the last returned event,
    or the end of the journal when filtered or non-lifecycle lines followed
    it. ``has_more`` means a full page was cut short at ``limit``.

    ``native`` returns the journal lines themselves, unprojected and
    unfiltered — for the engine-level ``CcxClient`` surface, whose ``Event``
    is the stamped host object verbatim.
    """
    if engine not in ENGINES:
        raise InvalidRequest(f"unknown engine {engine!r}")
    limit = _check_limit(limit)
    tail = _check_tail(tail, after)
    wanted = _check_kinds(kinds)
    cursor = decode_cursor(after, engine=engine, short=short) if after else None
    path = journal_path(engine, short)
    inode, _size = _file_id(path)
    lines, gap, end = _after(engine, path, cursor)

    selected: list[tuple[_Line, dict[str, Any]]] = []
    for line in lines:
        if native:
            selected.append((line, line.obj))
            continue
        event = _project(engine, line)
        if event is None or (wanted is not None and event.get("kind") not in wanted):
            continue
        selected.append((line, event))
    if tail is not None:
        selected = selected[-tail:] if tail else []
        has_more = False
    else:
        has_more = len(selected) > limit
        selected = selected[:limit]
    events = [
        event if native else _envelope(event, engine, short, line, inode)
        for line, event in selected
    ]
    if has_more:
        line = selected[-1][0]
        next_cursor = encode_cursor(engine, short, line.epoch, line.seq,
                                    line.end, inode)
    else:
        next_cursor = encode_cursor(engine, short, end[0], end[1], end[2], inode)
    return {
        "worker": f"{engine}:{short}",
        "engine": engine,
        "events": events,
        "next_cursor": next_cursor,
        "has_more": has_more,
        "gap": gap,
    }


__all__ = [
    "CURSOR_PREFIX",
    "HEARTBEAT_DEFAULT_S",
    "KINDS",
    "KIND_HEARTBEAT",
    "LIMIT_DEFAULT",
    "LIMIT_MAX",
    "decode_cursor",
    "encode_cursor",
    "journal_path",
    "read_page",
    "state_of",
]
