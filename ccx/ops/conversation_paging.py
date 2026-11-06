"""Engine-neutral bounded normalized conversation paging."""

from __future__ import annotations

import base64
import binascii
import copy
import hashlib
import json
import os
import time
from typing import Any

from ccx.codex import runtime as codex_runtime
from ccx.ops import conversation as conv
from ccx.system.bounded import BoundedValue, bound_value, omit_value
from ccx.errors import (
    CursorInvalidated,
    InvalidRequest,
    OperationTimeout,
    ProviderProtocolError,
    SessionUnavailable,
)
from ccx.system.sanitize import safe_native
from ccx.claude.transcript import (
    PinnedNativeSource,
    _complete_line_end,
    _has_queued_command_marker,
    _is_local_command_output,
    _iter_transcript,
)

_CURSOR_PREFIX = "ccxcp2."
_CURSOR_VERSION = 3
_DEFAULT_TARGET = 40
_MAX_TARGET = 100

# V1 has a 4 MiB process-response ceiling. Keep the canonical conversation-page
# result a full MiB below it so the protocol envelope/request id can never push a
# valid page over the transport guard. This is a CCX policy, not a caller knob.
PAGE_RESULT_MAX_BYTES = 3 * 1024 * 1024
_PAGE_FIELD_MAX_BYTES = 64 * 1024
_PAGE_ITEM_MAX_BYTES = 256 * 1024
_PAGE_SINGLE_RECORD_MAX_BYTES = PAGE_RESULT_MAX_BYTES - 256 * 1024
_PROTECTED_STRING_MAX_BYTES = 4 * 1024
_PAGE_IDENTITY_STUB_MAX_ITEMS = 64
_PROTECTED_ITEM_FIELDS = frozenset({
    "kind", "id", "nativeType", "name", "status", "callId",
    "clientId", "origin", "model", "phase", "unpaired",
})
_PROTECTED_SUMMARY_FIELDS = frozenset({"verb", "target", "description"})


def _encode_cursor(payload: dict[str, Any]) -> str:
    raw = json.dumps(
        {"v": _CURSOR_VERSION, **payload},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return _CURSOR_PREFIX + base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _decode_cursor(cursor: str, *, engine: str, worker_id: str) -> dict[str, Any]:
    if not isinstance(cursor, str) or not cursor.startswith(_CURSOR_PREFIX):
        raise InvalidRequest("invalid conversation page cursor")
    token = cursor[len(_CURSOR_PREFIX):]
    try:
        raw = base64.urlsafe_b64decode(token + "=" * (-len(token) % 4))
        payload = json.loads(raw)
    except (ValueError, UnicodeDecodeError, binascii.Error) as exc:
        raise InvalidRequest("invalid conversation page cursor") from exc
    if not isinstance(payload, dict) or payload.get("v") != _CURSOR_VERSION:
        raise CursorInvalidated(
            "conversation page cursor version is no longer valid", short=worker_id[:8]
        )
    if payload.get("engine") != engine or payload.get("worker_id") != worker_id:
        raise InvalidRequest("conversation page cursor belongs to another worker")
    return payload


def _validate_options(
    source_record_target: int,
    max_tool_result_bytes: int | None,
    timeout: float,
) -> None:
    if (
        isinstance(source_record_target, bool)
        or not isinstance(source_record_target, int)
        or not 1 <= source_record_target <= _MAX_TARGET
    ):
        raise InvalidRequest(
            f"source_record_target must be between 1 and {_MAX_TARGET}"
        )
    conv._requested_label(max_tool_result_bytes)
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or timeout <= 0:
        raise InvalidRequest("conversation page timeout must be greater than zero")


def _encoded_size(value: Any) -> int:
    return len(json.dumps(
        value, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8"))


def _nested_path(prefix: str, relative: str) -> str:
    if not relative or relative == "$":
        return prefix
    if relative.startswith("["):
        return f"{prefix}{relative}"
    return f"{prefix}.{relative}"


def _truncation(
    path: str,
    bounded: BoundedValue,
    *,
    source: str = "ccx-page",
    force_omitted: bool = False,
    original_byte_count: int | None = None,
    returned_byte_count: int | None = None,
    sha256: str | None = None,
) -> dict[str, Any]:
    omitted = force_omitted or bounded.omitted
    value = {
        "path": path,
        "source": source,
        "original_byte_count": (
            bounded.original_byte_count if original_byte_count is None else original_byte_count
        ),
        "returned_byte_count": (
            (0 if omitted else bounded.returned_byte_count)
            if returned_byte_count is None
            else returned_byte_count
        ),
        "sha256": bounded.sha256 if sha256 is None else sha256,
    }
    if omitted:
        value["omitted"] = True
    if bounded.changed_paths:
        value["paths"] = [
            _nested_path(path, relative) for relative in bounded.changed_paths
        ]
    if bounded.changed_path_count:
        value["changed_path_count"] = bounded.changed_path_count
    if bounded.omitted_count:
        value["omitted_path_count"] = bounded.omitted_count
    return value


def _existing_tool_result_truncation(result: dict[str, Any]) -> dict[str, Any] | None:
    if result.get("truncated") is not True:
        return None
    original = result.get("originalByteCount")
    returned = result.get("returnedByteCount")
    digest = result.get("contentSha256")
    if (
        not isinstance(original, int)
        or isinstance(original, bool)
        or not isinstance(returned, int)
        or isinstance(returned, bool)
        or not isinstance(digest, str)
        or not digest
    ):
        return None
    return {
        "path": "result.content",
        "source": "max_tool_result_bytes",
        "original_byte_count": original,
        "returned_byte_count": returned,
        "sha256": digest,
    }


def _project_protected(
    key: str, entry: Any, truncations: list[dict[str, Any]]
) -> Any:
    bounded = bound_value(entry, _PROTECTED_STRING_MAX_BYTES)
    if bounded.truncated:
        truncations.append(_truncation(key, bounded))
    return bounded.value


def _project_summary(entry: Any, truncations: list[dict[str, Any]]) -> Any:
    if not isinstance(entry, dict):
        return _project_protected("summary", entry, truncations)
    value: dict[str, Any] = {}
    for key, field in entry.items():
        if field is None:
            continue
        limit = (
            _PROTECTED_STRING_MAX_BYTES
            if key in _PROTECTED_SUMMARY_FIELDS
            else _PAGE_FIELD_MAX_BYTES
        )
        bounded = bound_value(field, limit)
        if not bounded.omitted:
            value[key] = bounded.value
        if bounded.truncated:
            truncations.append(_truncation(f"summary.{key}", bounded))
    return value


def _project_item_once(item: dict[str, Any], field_limit: int) -> dict[str, Any]:
    value: dict[str, Any] = {}
    truncations: list[dict[str, Any]] = []
    for key, entry in item.items():
        if key in ("ordinal", "startedAtMs", "completedAtMs", "timingSource"):
            continue
        if entry is None:
            continue
        if key in _PROTECTED_ITEM_FIELDS:
            value[key] = _project_protected(key, entry, truncations)
            continue
        if key == "summary":
            value[key] = _project_summary(entry, truncations)
            continue
        if key == "result" and isinstance(entry, dict):
            projected = copy.deepcopy(entry)
            existing = _existing_tool_result_truncation(projected)
            if existing is not None:
                truncations.append(existing)
            if projected.get("content") is not None:
                bounded = bound_value(projected["content"], field_limit)
                if bounded.omitted:
                    projected.pop("content", None)
                else:
                    projected["content"] = bounded.value
                if bounded.truncated:
                    if existing is not None:
                        truncations.append({
                            "path": "result.content",
                            "source": "ccx-page",
                            "original_byte_count": existing["original_byte_count"],
                            "returned_byte_count": (
                                0 if bounded.omitted else bounded.returned_byte_count
                            ),
                            "sha256": existing["sha256"],
                            **({"omitted": True} if bounded.omitted else {}),
                        })
                        projected["originalByteCount"] = existing["original_byte_count"]
                        projected["contentSha256"] = existing["sha256"]
                    else:
                        truncations.append(_truncation("result.content", bounded))
                        projected["originalByteCount"] = bounded.original_byte_count
                        projected["contentSha256"] = bounded.sha256
                    projected["truncated"] = True
                    projected["returnedByteCount"] = (
                        0 if bounded.omitted else bounded.returned_byte_count
                    )
            value[key] = projected
            continue
        bounded = bound_value(entry, field_limit)
        if not bounded.omitted:
            value[key] = bounded.value
        if bounded.truncated:
            truncations.append(_truncation(key, bounded))
    if truncations:
        value["truncations"] = truncations
    return value


def _minimal_public_item(item: dict[str, Any]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    truncations: list[dict[str, Any]] = []
    for key, entry in item.items():
        if key in _PROTECTED_ITEM_FIELDS and entry is not None:
            value[key] = _project_protected(key, entry, truncations)
    if item.get("summary") is not None:
        value["summary"] = _project_summary(item["summary"], truncations)

    for key, entry in item.items():
        if (
            key in _PROTECTED_ITEM_FIELDS
            or key == "summary"
            or key in ("ordinal", "startedAtMs", "completedAtMs", "timingSource")
            or entry is None
        ):
            continue
        if key == "result" and isinstance(entry, dict):
            existing = _existing_tool_result_truncation(entry)
            if existing is not None:
                truncations.append(existing)
            content = entry.get("content")
            if content is not None:
                if existing is not None:
                    truncations.append({
                        "path": "result.content",
                        "source": "ccx-page",
                        "original_byte_count": existing["original_byte_count"],
                        "returned_byte_count": 0,
                        "sha256": existing["sha256"],
                        "omitted": True,
                    })
                else:
                    truncations.append(_truncation(
                        "result.content", omit_value(content), force_omitted=True
                    ))
            else:
                truncations.append(_truncation(
                    "result", omit_value(entry), force_omitted=True
                ))
            continue
        truncations.append(_truncation(
            key, omit_value(entry), force_omitted=True
        ))
    if truncations:
        value["truncations"] = truncations
    return value


def _public_item(
    item: dict[str, Any], *, item_budget: int = _PAGE_ITEM_MAX_BYTES
) -> dict[str, Any]:
    variable = sum(
        1
        for key, entry in item.items()
        if key not in _PROTECTED_ITEM_FIELDS
        and key != "summary"
        and key not in ("ordinal", "startedAtMs", "completedAtMs", "timingSource")
        and entry is not None
    )
    field_limit = min(
        _PAGE_FIELD_MAX_BYTES,
        max(256, item_budget // max(1, variable + 1)),
    )
    while True:
        value = _project_item_once(item, field_limit)
        if _encoded_size(value) <= item_budget:
            return value
        if field_limit <= 256:
            break
        field_limit = max(256, field_limit // 2)
    return _minimal_public_item(item)


def _project_record_identity(
    path: str, value: Any, truncations: list[dict[str, Any]]
) -> Any:
    if not isinstance(value, str):
        return copy.deepcopy(value)
    bounded = bound_value(value, _PROTECTED_STRING_MAX_BYTES)
    if bounded.truncated:
        truncations.append(_truncation(path, bounded))
    return bounded.value


def _record_value(
    *,
    turn_id: Any,
    source_record_id: Any,
    timestamp_ms: Any,
    duration_ms: Any,
    items: list[dict[str, Any]],
    usage: dict[str, Any] | None,
    record_truncations: list[dict[str, Any]],
) -> dict[str, Any]:
    value = {
        "turn_id": turn_id,
        "source_record_id": source_record_id,
        "timestamp_ms": timestamp_ms,
        "duration_ms": duration_ms,
        "usage": usage,
        "items": items,
    }
    if record_truncations:
        value["truncations"] = record_truncations
    return value


def _public_record(
    record: dict[str, Any], counters: conv._Counters | None = None
) -> dict[str, Any] | None:
    raw_items = [item for item in (record.get("items") or []) if isinstance(item, dict)]
    raw_usage = record.get("usage")
    if not raw_items and not raw_usage:
        return None
    counters = counters or conv._Counters()

    record_truncations: list[dict[str, Any]] = []
    turn_id = _project_record_identity(
        "turn_id", record.get("turn_id"), record_truncations
    )
    source_record_id = _project_record_identity(
        "source_record_id", record.get("source_record_id"), record_truncations
    )
    timestamp_ms = record.get("timestamp_ms")
    duration_ms = record.get("duration_ms")
    usage = copy.deepcopy(raw_usage) if isinstance(raw_usage, dict) else None
    if isinstance(raw_usage, dict):
        bounded_usage = bound_value(raw_usage, _PAGE_FIELD_MAX_BYTES)
        usage = None if bounded_usage.omitted else bounded_usage.value
        if bounded_usage.truncated:
            record_truncations.append(_truncation("usage", bounded_usage))

    fair_item_budget = max(
        2048, (_PAGE_SINGLE_RECORD_MAX_BYTES - 32 * 1024) // max(1, len(raw_items))
    )
    # Start with a richer per-item preview than the strict fair share, then let
    # the record-level loop shrink it only when the aggregate record actually
    # needs that reduction. This keeps the shrink loop meaningful while bounding
    # it to at most a small number of passes for high-cardinality records.
    item_budget = min(_PAGE_ITEM_MAX_BYTES, fair_item_budget * 2)
    while True:
        items = [_public_item(item, item_budget=item_budget) for item in raw_items]
        value = _record_value(
            turn_id=turn_id, source_record_id=source_record_id,
            timestamp_ms=timestamp_ms, duration_ms=duration_ms,
            items=items, usage=usage, record_truncations=list(record_truncations),
        )
        if _encoded_size(value) <= _PAGE_SINGLE_RECORD_MAX_BYTES:
            return value
        if item_budget <= 2048:
            break
        item_budget = max(2048, item_budget // 2)

    # Pathological child cardinality/protected metadata must not make one source
    # record a permanent cursor barrier. Keep a bounded source-order prefix of
    # identity-only stubs and explicitly count every omitted normalized item.
    stub_count = min(len(raw_items), _PAGE_IDENTITY_STUB_MAX_ITEMS)
    while True:
        stubs = [_minimal_public_item(item) for item in raw_items[:stub_count]]
        value = _record_value(
            turn_id=turn_id, source_record_id=source_record_id,
            timestamp_ms=timestamp_ms, duration_ms=duration_ms,
            items=stubs, usage=usage, record_truncations=list(record_truncations),
        )
        if _encoded_size(value) <= _PAGE_SINGLE_RECORD_MAX_BYTES or stub_count == 0:
            break
        stub_count //= 2

    omitted = len(raw_items) - stub_count
    if omitted:
        counters.omitted += omitted
        counters.warn(
            f"{omitted} normalized item(s) omitted from one conversation source "
            "record to fit the bounded page"
        )
    return value


def _truncated_field_count(records: list[dict[str, Any]]) -> int:
    return sum(
        len(record.get("truncations") or [])
        + sum(
            len(item.get("truncations") or [])
            for item in (record.get("items") or [])
            if isinstance(item, dict)
        )
        for record in records
    )


def _bounded_warnings(counters: conv._Counters) -> tuple[list[str], int]:
    warnings: list[str] = []
    truncated = 0
    for warning in counters.warning_list():
        bounded = bound_value(str(warning), _PROTECTED_STRING_MAX_BYTES)
        warnings.append(str(bounded.value))
        if bounded.truncated:
            truncated += 1
    return warnings, truncated


def _last_resort_record_stub(
    record: dict[str, Any], counters: conv._Counters
) -> dict[str, Any]:
    value = copy.deepcopy(record)
    omitted = len(value.get("items") or [])
    value["items"] = []
    if omitted:
        counters.omitted += omitted
        counters.warn(
            f"{omitted} projected item(s) omitted from one conversation source "
            "record at the final page-budget fence"
        )
    return value


def _page_completeness(
    counters: conv._Counters,
    *,
    requested_source_record_target: int,
    returned_source_record_count: int,
    response_budget_limited: bool,
    truncated_field_count: int,
) -> dict[str, Any]:
    if counters.malformed or counters.omitted:
        status = "partial"
    elif counters.truncated or truncated_field_count or response_budget_limited:
        status = "bounded"
    else:
        status = "complete"
    warnings, warning_truncated_count = _bounded_warnings(counters)
    return {
        "status": status,
        "truncated_item_count": counters.truncated,
        "truncated_field_count": truncated_field_count,
        "omitted_item_count": counters.omitted,
        "malformed_line_count": counters.malformed,
        "requested_source_record_target": requested_source_record_target,
        "returned_source_record_count": returned_source_record_count,
        "response_budget_limited": response_budget_limited,
        "warning_truncated_count": warning_truncated_count,
        "warnings": warnings,
    }


def _page_result(
    worker: str,
    records: list[dict[str, Any]],
    older_cursor: str | None,
    counters: conv._Counters,
    *,
    requested_source_record_target: int,
    response_budget_limited: bool,
) -> dict[str, Any]:
    return {
        "worker": worker,
        "records": records,
        "older_cursor": older_cursor,
        "completeness": _page_completeness(
            counters,
            requested_source_record_target=requested_source_record_target,
            returned_source_record_count=len(records),
            response_budget_limited=response_budget_limited,
            truncated_field_count=_truncated_field_count(records),
        ),
    }


def _event_fingerprint(evt: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            safe_native(evt),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def _ordinary_user_boundary(evt: dict[str, Any]) -> bool:
    if evt.get("type") != "user" or evt.get("isMeta") or evt.get("isCompactSummary"):
        return False
    if _has_queued_command_marker(evt):
        return False
    content = (evt.get("message") or {}).get("content")
    if isinstance(content, str):
        if conv._command_from_text(content) is not None:
            return False
        return bool(content.strip()) and not _is_local_command_output(content)
    if not isinstance(content, list):
        return False
    for block in content:
        if not isinstance(block, dict):
            continue
        kind = block.get("type")
        if kind == "tool_result":
            continue
        if kind == "text":
            if (block.get("text") or "").strip():
                return True
            continue
        if kind is not None:
            return True
    return False


def _boundary_from_event(evt: dict[str, Any]) -> dict[str, Any] | None:
    offset = evt.get("_ccx_start_offset")
    if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
        return None
    return {
        "offset": offset,
        "source_record_id": evt.get("uuid"),
        "fingerprint": _event_fingerprint(evt),
    }


def _validate_anchor(
    path: str,
    candidate_offset: int,
    *,
    before_offset: int,
) -> dict[str, Any] | None:
    for evt in _iter_transcript(
        path, after_offset=candidate_offset, before_offset=before_offset
    ):
        if not conv._claude_page_bearing(evt):
            continue
        if not _ordinary_user_boundary(evt):
            return None
        return _boundary_from_event(evt)
    return None


def _event_at(path: str, offset: int, *, before_offset: int) -> dict[str, Any] | None:
    for evt in _iter_transcript(path, after_offset=offset, before_offset=before_offset):
        if evt.get("_ccx_start_offset") != offset:
            return None
        return evt
    return None


def _context_end_offset(
    path: str,
    cut_offset: int,
    *,
    source_eof: int,
) -> int:
    """Upper normalization fence for the history immediately before a page cut.

    A real user prompt starts a fresh semantic group. To render records just
    before a cut, look ahead only until the next such prompt; that is enough to
    pair tool results and assistant fragments from the same turn without
    reparsing every newer turn through current EOF.
    """
    for evt in _iter_transcript(
        path, after_offset=cut_offset, before_offset=source_eof
    ):
        start = evt.get("_ccx_start_offset")
        if not isinstance(start, int) or start < cut_offset:
            continue
        if _ordinary_user_boundary(evt):
            return start
    return source_eof


def _record_matches(
    path: str,
    offset: int,
    source_record_id: Any,
    fingerprint: Any,
    *,
    before_offset: int,
) -> bool:
    evt = _event_at(path, offset, before_offset=before_offset)
    if evt is None:
        return False
    actual_id = evt.get("uuid")
    if isinstance(source_record_id, str) and source_record_id:
        return actual_id == source_record_id
    return isinstance(fingerprint, str) and _event_fingerprint(evt) == fingerprint


def _reverse_lines(
    path: str | PinnedNativeSource, *, before_offset: int,
    block_size: int = 64 * 1024,
):
    """Yield (start offset, raw line) backwards from a certified source fence.

    The pinned reader uses pread, not the mutable native pathname or shared fd
    seek position. A row longer than a read block is collected piece by piece
    and joined once, so a multi-MB row costs its size, not its size squared.
    """
    try:
        reader = (path.open_reader() if isinstance(path, PinnedNativeSource)
                  else open(path, "rb"))
        with reader as handle:
            position = before_offset
            pieces: list[bytes] = []  # the current row's tail, newest piece first
            while position > 0:
                size = min(block_size, position)
                position -= size
                handle.seek(position)
                data = handle.read(size)
                cut = len(data)
                while (newline := data.rfind(b"\n", 0, cut)) >= 0:
                    pieces.append(data[newline + 1:cut])
                    raw = b"".join(reversed(pieces))
                    pieces = []
                    if raw:
                        yield position + newline + 1, raw
                    cut = newline
                pieces.append(data[:cut])
            raw = b"".join(reversed(pieces))
            if raw:
                yield 0, raw
    except FileNotFoundError:
        return


def _reverse_boundary_events(path: str | PinnedNativeSource, *, before_offset: int):
    """Find ordinary-user starts newest-first, without hashing skipped rows."""
    for start, raw in _reverse_lines(path, before_offset=before_offset):
        raw = raw.strip()
        if not raw:
            continue
        try:
            evt = json.loads(raw)
        except (ValueError, UnicodeDecodeError):
            continue
        if isinstance(evt, dict):
            if evt.get("isSidechain") is True:
                continue
            evt["_ccx_start_offset"] = start
        if _ordinary_user_boundary(evt):
            yield evt


def claude_tail_offset(path: str | PinnedNativeSource, *, before_offset: int,
                       turns: int) -> int:
    """Where the newest ``turns`` user turns start, reading backwards; 0 if fewer."""
    seen = 0
    for evt in _reverse_boundary_events(path, before_offset=before_offset):
        seen += 1
        if seen >= turns:
            return evt["_ccx_start_offset"]
    return 0


def _normalize_claude_window(
    path: str,
    *,
    after_offset: int,
    before_offset: int,
    max_tool_result_bytes: int | None,
) -> tuple[list[dict[str, Any]], conv._Counters]:
    raw_stats: dict[str, int] = {"malformedLines": 0}
    events = list(
        _iter_transcript(
            path,
            raw_stats,
            after_offset=after_offset,
            before_offset=before_offset,
        )
    )
    counters = conv._Counters()
    counters.malformed = raw_stats.get("malformedLines", 0)
    if counters.malformed:
        counters.warn(
            f"{counters.malformed} malformed native JSONL line(s) skipped"
        )
    _turns, _model, records = conv._normalize_claude_events(
        events, counters, max_tool_result_bytes
    )
    return records, counters


def _bearing_before(
    records: list[dict[str, Any]],
    cut_offset: int,
) -> list[dict[str, Any]]:
    return [
        record for record in records
        if record.get("_ccx_page_bearing")
        and isinstance(record.get("_ccx_start_offset"), int)
        and record["_ccx_start_offset"] < cut_offset
    ]


def _choose_claude_window(
    path: str,
    source: dict[str, int],
    *,
    cut_offset: int,
    normalization_end_offset: int,
    target: int,
    max_tool_result_bytes: int | None,
    anchor_hint: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], conv._Counters]:
    def normalize_from(boundary: dict[str, Any]):
        records, counters = _normalize_claude_window(
            path,
            after_offset=boundary["offset"],
            before_offset=normalization_end_offset,
            max_tool_result_bytes=max_tool_result_bytes,
        )
        return _bearing_before(records, cut_offset), counters

    # Older-page cursors carry a validated normalization anchor.
    if (
        isinstance(anchor_hint, dict)
        and isinstance(anchor_hint.get("offset"), int)
        and 0 <= anchor_hint["offset"] < cut_offset
    ):
        eligible, counters = normalize_from(anchor_hint)
        if len(eligible) >= target or anchor_hint["offset"] == 0:
            return anchor_hint, eligible, counters

    # Probe the same 4, 8, 16... user starts as the old forward full scan, but
    # stop reading the pinned source as soon as a window has enough records.
    # If fewer than the next probe distance exist, also try the oldest start.
    distance = 4
    count = 0
    last_probed = 0
    oldest = None
    for evt in _reverse_boundary_events(path, before_offset=cut_offset):
        count += 1
        oldest = evt
        if count != distance:
            continue
        eligible, counters = normalize_from({"offset": evt["_ccx_start_offset"]})
        if len(eligible) >= target:
            return _boundary_from_event(evt), eligible, counters
        last_probed = count
        distance *= 2
    if oldest is not None and count != last_probed:
        eligible, counters = normalize_from({"offset": oldest["_ccx_start_offset"]})
        if len(eligible) >= target:
            return _boundary_from_event(oldest), eligible, counters

    zero = {"offset": 0, "source_record_id": None, "fingerprint": None}
    eligible, counters = normalize_from(zero)
    return zero, eligible, counters


def _source_transition_is_append_only(
    before: dict[str, int],
    after: dict[str, int],
) -> bool:
    """Whether the after snapshot is compatible with an as-of-before EOF read.

    Claude's native transcript is append-only in ordinary operation. Growth
    beyond the snapshotted complete-line EOF is harmless because every bounded
    parser is fenced at that earlier EOF. Replacement, truncation, or a same-size
    rewrite invalidates the read.
    """
    if before["dev"] != after["dev"] or before["ino"] != after["ino"]:
        return False
    if after["eof"] < before["eof"] or after["size"] < before["eof"]:
        return False
    if after["eof"] > before["eof"]:
        return True
    if after["size"] > before["size"]:
        # A partial trailing append need not advance the complete-line EOF yet.
        return True
    if after["size"] != before["size"]:
        return False
    return after["mtime_ns"] == before["mtime_ns"]


def _unstable_source_error(short: str, *, cursor: bool) -> Exception:
    if cursor:
        return CursorInvalidated(
            "conversation page source changed during the read; restart paging",
            short=short,
        )
    return SessionUnavailable(
        "Claude transcript changed non-append-only while reading; retry the page",
        short=short,
    )


def _validate_claude_cursor(
    payload: dict[str, Any],
    short: str,
    path: str,
    source: dict[str, int],
    scope: dict[str, str],
) -> tuple[int, int]:
    if any(payload.get(key) != scope[key] for key in (
        "root", "generation", "session_id"
    )):
        raise CursorInvalidated(
            "Conversation worker changed; restart paging", short=short
        )
    if payload.get("dev") != source["dev"] or payload.get("ino") != source["ino"]:
        raise CursorInvalidated(
            "conversation page source was replaced; restart paging", short=short
        )
    cut_offset = payload.get("cut_offset")
    if (
        not isinstance(cut_offset, int)
        or isinstance(cut_offset, bool)
        or cut_offset < 0
        or cut_offset >= source["eof"]
    ):
        raise CursorInvalidated(
            "conversation page source was truncated; restart paging", short=short
        )
    if not _record_matches(
        path,
        cut_offset,
        payload.get("cut_id"),
        payload.get("cut_fingerprint"),
        before_offset=source["eof"],
    ):
        raise CursorInvalidated(
            "Conversation history changed; restart paging",
            short=short,
        )
    # The fingerprint must match even when the original row has a native UUID.
    # Both proofs are carried so an edited cursor cannot silently loosen either.
    cut = _event_at(path, cut_offset, before_offset=source["eof"])
    if cut is None or payload.get("cut_fingerprint") != _event_fingerprint(cut):
        raise CursorInvalidated("conversation page cut fingerprint changed", short=short)
    # Check the original snapshot fence, not a newly appended tail. A cursor
    # may remain valid after append but must not shorten semantic lookahead.
    source_eof = payload.get("source_eof")
    if (type(source_eof) is not int or not cut_offset < source_eof <= source["eof"]):
        raise CursorInvalidated("Conversation source changed; restart paging", short=short)
    context_end_offset = payload.get("context_end_offset")
    if (
        not isinstance(context_end_offset, int)
        or isinstance(context_end_offset, bool)
        or context_end_offset < cut_offset
        or context_end_offset > source_eof
        or context_end_offset != _context_end_offset(
            path, cut_offset, source_eof=source_eof
        )
    ):
        raise CursorInvalidated(
            "Conversation context changed; restart paging",
            short=short,
        )

    anchor_offset = payload.get("anchor_offset")
    if (type(anchor_offset) is not int or anchor_offset < 0
            or anchor_offset >= cut_offset):
        raise CursorInvalidated("conversation page anchor is no longer valid", short=short)
    # Byte zero may be a synthetic fallback or a real ordinary-user anchor.
    anchor = _validate_anchor(path, anchor_offset, before_offset=source_eof)
    if anchor_offset == 0 and (anchor is None or anchor["offset"] != 0):
        if payload.get("anchor_id") is not None or payload.get("anchor_fingerprint") is not None:
            raise CursorInvalidated("conversation page zero anchor changed", short=short)
    else:
        if (
            anchor is None
            or anchor["offset"] != anchor_offset
            or _cursor_id(anchor.get("source_record_id")) != payload.get("anchor_id")
            or anchor.get("fingerprint") != payload.get("anchor_fingerprint")
        ):
            raise CursorInvalidated(
                "conversation page normalization anchor is no longer valid; restart paging",
                short=short,
            )
    return cut_offset, context_end_offset


def _cursor_id(value: Any) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    if len(value.encode("utf-8")) > _PROTECTED_STRING_MAX_BYTES:
        return None
    return value


def _claude_older_cursor(
    scope: dict[str, str],
    short: str,
    path: str,
    source: dict[str, int],
    anchor: dict[str, Any],
    eligible: list[dict[str, Any]],
    first: dict[str, Any] | None,
) -> str | None:
    if first is None:
        return None
    first_offset = first["_ccx_start_offset"]
    has_older_here = any(
        isinstance(record.get("_ccx_start_offset"), int)
        and record["_ccx_start_offset"] < first_offset
        for record in eligible
    )
    has_older_before_anchor = anchor["offset"] > 0
    if first_offset <= 0 or not (
        has_older_here or has_older_before_anchor
    ):
        return None
    if first_offset == anchor["offset"]:
        # The first retained record can be the normalization anchor itself.
        # An older cursor must point to a strictly earlier anchor, or its next
        # read fails validation even when the original transcript is unchanged.
        # Rebase on the preceding ordinary user, using the same pinned source;
        # if none exists, the byte-zero fallback covers the earlier prelude.
        previous = next(_reverse_boundary_events(path, before_offset=first_offset), None)
        anchor = (_boundary_from_event(previous) if previous is not None else
                  {"offset": 0, "source_record_id": None, "fingerprint": None})
    context_end_offset = _context_end_offset(
        path, first_offset, source_eof=source["eof"]
    )
    return _encode_cursor({
        "engine": "claude",
        "worker_id": scope["worker_id"],
        "root": scope["root"],
        "generation": scope["generation"],
        "session_id": scope["session_id"],
        "dev": source["dev"],
        "ino": source["ino"],
        "source_eof": source["eof"],
        "anchor_offset": anchor["offset"],
        "anchor_id": _cursor_id(anchor.get("source_record_id")),
        "anchor_fingerprint": anchor.get("fingerprint"),
        "cut_offset": first_offset,
        "context_end_offset": context_end_offset,
        "cut_id": _cursor_id(first.get("source_record_id")),
        "cut_fingerprint": first.get("_ccx_fingerprint"),
    })


def _read_claude_page_once(
    path: str,
    worker: str,
    source: dict[str, int],
    scope: dict[str, str],
    *,
    cursor: str | None,
    source_record_target: int,
    max_tool_result_bytes: int | None,
    timeout: float,
) -> dict[str, Any]:
    short = scope["worker_id"][:8]
    cut_offset = source["eof"]
    normalization_end_offset = source["eof"]
    anchor_hint = None
    if cursor is not None:
        payload = _decode_cursor(
            cursor, engine="claude", worker_id=scope["worker_id"]
        )
        cut_offset, normalization_end_offset = _validate_claude_cursor(
            payload, short, path, source, scope
        )
        anchor_hint = {
            "offset": payload.get("anchor_offset"),
            "source_record_id": payload.get("anchor_id"),
            "fingerprint": payload.get("anchor_fingerprint"),
        }

    anchor, eligible, counters = _choose_claude_window(
        path,
        source,
        cut_offset=cut_offset,
        normalization_end_offset=normalization_end_offset,
        target=source_record_target,
        max_tool_result_bytes=max_tool_result_bytes,
        anchor_hint=anchor_hint,
    )
    selected = eligible[-source_record_target:]
    public_pairs: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for record in selected:
        public = _public_record(record, counters)
        if public is not None:
            public_pairs.append((record, public))

    keep = len(public_pairs)
    response_budget_limited = False
    while True:
        retained = public_pairs[-keep:] if keep else []
        public_records = [public for _record, public in retained]
        budget_dropped_public_records = keep < len(public_pairs)
        if not selected:
            first = None
        elif not public_pairs or not budget_dropped_public_records:
            # Empty/non-public source records still advance the native cursor.
            # Keep the first selected source cut unless the byte budget actually
            # removed public records from the front of this page.
            first = selected[0]
        else:
            first = retained[0][0] if retained else selected[0]
        older_cursor = _claude_older_cursor(
            scope,
            short,
            path,
            source,
            anchor,
            eligible,
            first,
        )
        result = _page_result(
            worker,
            public_records,
            older_cursor,
            counters,
            requested_source_record_target=source_record_target,
            response_budget_limited=response_budget_limited,
        )
        if _encoded_size(result) <= PAGE_RESULT_MAX_BYTES:
            return result
        response_budget_limited = True
        if keep <= 1:
            if public_records:
                public_records = [_last_resort_record_stub(public_records[0], counters)]
            result = _page_result(
                worker,
                public_records,
                older_cursor,
                counters,
                requested_source_record_target=source_record_target,
                response_budget_limited=True,
            )
            return result
        keep = max(1, keep // 2)


def read_claude_page(
    pinned_path: str | PinnedNativeSource,
    worker: str,
    *,
    root: str | os.PathLike[str],
    generation: str,
    session_id: str,
    source_stat: os.stat_result,
    end_offset: int,
    cursor: str | None,
    source_record_target: int = _DEFAULT_TARGET,
    max_tool_result_bytes: int | None = 16_384,
    timeout: float = 30,
) -> dict[str, Any]:
    """One certified original-source Claude page, using the opener's held fd."""
    _validate_options(source_record_target, max_tool_result_bytes, timeout)
    scope = {
        "root": hashlib.sha256(os.fsencode(root)).hexdigest(),
        "generation": generation,
        "worker_id": worker.partition(":")[2],
        "session_id": session_id,
    }
    source = {
        "dev": int(source_stat.st_dev), "ino": int(source_stat.st_ino),
        "size": int(source_stat.st_size), "eof": end_offset,
        "mtime_ns": int(source_stat.st_mtime_ns),
    }
    page = _read_claude_page_once(
        pinned_path, worker, source, scope, cursor=cursor,
        source_record_target=source_record_target,
        max_tool_result_bytes=max_tool_result_bytes, timeout=timeout,
    )
    # The native opener still holds this same inode. Never reopen or restat the
    # original pathname, inspect another source, or retry against a new file.
    fd = (pinned_path.fd if isinstance(pinned_path, PinnedNativeSource)
          else int(pinned_path.rsplit("/", 1)[-1]))
    after_stat = os.fstat(fd)
    after = {
        "dev": int(after_stat.st_dev), "ino": int(after_stat.st_ino),
        "size": int(after_stat.st_size),
        "eof": _complete_line_end(fd, after_stat.st_size),
        "mtime_ns": int(after_stat.st_mtime_ns),
    }
    if not _source_transition_is_append_only(source, after):
        raise _unstable_source_error(scope["worker_id"][:8], cursor=cursor is not None)
    return page


def _codex_timestamp(evidence: dict[str, Any] | None) -> int | None:
    if not isinstance(evidence, dict):
        return None
    completed = evidence.get("completedAtMs")
    if isinstance(completed, int) and not isinstance(completed, bool):
        return completed
    started = evidence.get("startedAtMs")
    if isinstance(started, int) and not isinstance(started, bool):
        return started
    return None


def _codex_duration(evidence: dict[str, Any] | None) -> int | None:
    if not isinstance(evidence, dict):
        return None
    started = evidence.get("startedAtMs")
    completed = evidence.get("completedAtMs")
    if (
        isinstance(started, int)
        and not isinstance(started, bool)
        and isinstance(completed, int)
        and not isinstance(completed, bool)
        and completed >= started
    ):
        return completed - started
    return None


def read_codex_page(
    rec: dict[str, Any],
    worker: str,
    *,
    root: str | os.PathLike[str],
    generation: str,
    worker_id: str,
    thread_id: str,
    expected_source: str,
    evidence_index: dict[str, Any],
    foreign_evidence: bool,
    cursor: str | None,
    source_record_target: int = _DEFAULT_TARGET,
    max_tool_result_bytes: int | None = 16_384,
    timeout: float = 30,
) -> dict[str, Any]:
    _validate_options(source_record_target, max_tool_result_bytes, timeout)
    short = worker_id[:8]  # Diagnostic label only; never owner/source authority.
    scope = {
        "root": hashlib.sha256(os.fsencode(root)).hexdigest(),
        "generation": generation,
        "worker_id": worker_id,
        "thread_id": thread_id,
    }
    provider_cursor = None
    if cursor is not None:
        payload = _decode_cursor(cursor, engine="codex", worker_id=worker_id)
        if (payload.get("root") != scope["root"]
                or payload.get("generation") != generation
                or payload.get("thread_id") != thread_id):
            raise CursorInvalidated(
                "Codex conversation changed; restart paging", short=short,
            )
        provider_cursor = payload.get("provider_cursor")
        if not isinstance(provider_cursor, str) or not provider_cursor:
            raise CursorInvalidated(
                "Codex cursor expired; restart paging", short=short,
            )

    # Only a source-attested explicit empty-history record is a zero-RPC EOF;
    # an older cursor can never originate from that thread. Decode it first so
    # wrong-owner and legacy tokens keep their normal typed refusals.
    if rec.get("historyKnownEmpty") is True:
        if cursor is not None:
            raise CursorInvalidated(
                "Codex conversation has no earlier messages", short=short,
            )
        counters = conv._Counters()
        if foreign_evidence:
            counters.warn("excluded native evidence from a foreign Codex thread")
        return _page_result(
            worker, [], None, counters,
            requested_source_record_target=source_record_target,
            response_budget_limited=False,
        )

    evidence_items = evidence_index.get("items") or {}
    evidence_clients = evidence_index.get("clients") or {}
    observed_model = rec.get("resolvedModel")
    fetch_target = source_record_target
    response_budget_limited = False
    deadline = time.monotonic() + float(timeout)

    while True:
        remaining = max(0.0, deadline - time.monotonic())
        if remaining <= 0:
            raise OperationTimeout(
                "Codex conversation page deadline expired", short=short
            )
        page = codex_runtime.list_thread_items(
            thread_id,
            worker_id=worker_id,
            expected_source=expected_source,
            cwd=rec.get("cwd"),
            cursor=provider_cursor,
            limit=fetch_target,
            sort_direction="desc",
            timeout=remaining,
        )
        native_items = page.get("data") if isinstance(page, dict) else None
        if not isinstance(native_items, list):
            raise SessionUnavailable(
                "Codex thread item page returned malformed data", short=short
            )

        counters = conv._Counters()
        if foreign_evidence:
            counters.warn("excluded native evidence from a foreign Codex thread")
        records: list[dict[str, Any]] = []
        for wrapper in native_items:
            if not isinstance(wrapper, dict):
                counters.omitted += 1
                continue
            turn_id = wrapper.get("turnId")
            entry = wrapper.get("item")
            if not isinstance(entry, dict):
                counters.omitted += 1
                continue
            children = conv._codex_item(
                entry, counters, max_tool_result_bytes, observed_model
            )
            turn_evidence = evidence_items.get(turn_id) or {}
            turn_clients = evidence_clients.get(turn_id) or {}
            item_evidence = conv._evidence_entry(
                entry, turn_evidence, turn_clients
            )
            conv._apply_timing(children, item_evidence)
            public = _public_record({
                "turn_id": turn_id,
                "source_record_id": entry.get("id"),
                "timestamp_ms": _codex_timestamp(item_evidence),
                "duration_ms": _codex_duration(item_evidence),
                "usage": None,
                "items": children,
            }, counters)
            if public is not None:
                records.append(public)

        # Native desc is transport-efficient; public page order is chronological.
        records.reverse()
        next_cursor = page.get("nextCursor")
        if next_cursor is not None and (
            not isinstance(next_cursor, str) or not next_cursor
        ):
            raise ProviderProtocolError(
                "Codex history cursor is invalid; restart paging", short=short,
            )
        older_cursor = (
            _encode_cursor({
                **scope, "engine": "codex", "provider_cursor": next_cursor,
            })
            if next_cursor is not None else None
        )
        result = _page_result(
            worker,
            records,
            older_cursor,
            counters,
            requested_source_record_target=source_record_target,
            response_budget_limited=response_budget_limited,
        )
        if _encoded_size(result) <= PAGE_RESULT_MAX_BYTES:
            return result
        response_budget_limited = True
        if fetch_target <= 1:
            if len(native_items) > fetch_target:
                # A provider that ignores limit=1 gives us no cursor boundary
                # immediately after only the newest record. Returning its
                # nextCursor after dropping siblings would skip history. Refuse
                # this provider-contract violation rather than invent a cursor.
                raise ProviderProtocolError(
                    "Codex thread item page exceeded requested limit at final "
                    "conversation read limit",
                    short=short,
                )
            if records:
                records = [_last_resort_record_stub(records[-1], counters)]
            return _page_result(
                worker,
                records,
                older_cursor,
                counters,
                requested_source_record_target=source_record_target,
                response_budget_limited=True,
            )

        # Refetch from the same incoming provider cursor with a smaller native
        # limit. Base the next request on what the provider actually returned,
        # not merely what we asked for, and spend one shared caller deadline.
        previous_target = fetch_target
        fetch_target = max(1, min(fetch_target, len(native_items)) // 2)
        if fetch_target >= previous_target:
            fetch_target = max(1, previous_target // 2)


__all__ = ["read_claude_page", "read_codex_page"]
