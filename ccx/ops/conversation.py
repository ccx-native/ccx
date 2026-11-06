"Normalized conversation conversion for both CCX engines.\n\nOne versioned document shape is produced from two very different native\nsources:\n\n* Claude — the raw JSONL transcript ccx already records for every managed\n  worker. It is the authoritative source, so full mode reads it directly\n  instead of the cleaned renderer (whose 4 KB per-tool-result cap stays exactly\n  where it is, for ``logs()``/``peek``/dashboard previews).\n* Codex — the app-server ``thread/read(includeTurns=true)`` response. Rollout\n  files are never parsed, and the artifact is never called a raw transcript.\n\nThe document keys are camelCase because that is the shape the feature brief\nfroze and the shape ``transcript.render_transcript`` already returns;\n:class:`ccx.workers.WorkerClient` operation results stay snake_case. Only this\ndocument carries a ``schemaVersion``.\n\nEvery item carries an ``ordinal``: its position in the historical source this\nconverter was handed, per turn, dense and zero-based. The ``input``/``output``\nsplit is a classification, not an order — a Codex steer is user input that\nbelongs in the middle of a turn — so the ordinal is the only thing that\nreconstructs the source's own sequence.\n\nIt is not a claim about live chronology. Codex ``thread/read`` reconstructs a\nthread rather than replaying it: on 0.150.1 it has been observed to omit items\nthat certainly ran and to place user inputs differently from when they were\nactually delivered. The authoritative record of what CCX observed, in the order\nit observed it, is the evidence journal's ``sequence`` (:mod:`ccx.codex.evidence`).\nThe two orders are separate and must stay that way.\n\nMemory tradeoff (accepted for v1): both converters parse the whole native\nsource and materialize the whole normalized document, and\n:func:`write_export` serializes it in one ``json.dumps``. Peak memory is\nroughly 2-3x the raw native size, so a 50 MB transcript costs ~100-150 MB.\nTool results dominate large transcripts, so ``max_tool_result_bytes=N`` is the\nmitigation; the *native* copy is streamed to disk regardless, so full evidence\nsurvives even under a bound. Streaming conversion is deliberately deferred.\n"

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import os
import tempfile
import time
from datetime import datetime, timezone

from ccx.system.bounded import bound_value
from ccx.codex.provenance import origin_of, parse_client_message_id
from ccx.errors import InvalidRequest, SessionNotFound
from ccx.system.sanitize import safe_native
from ccx.claude.transcript import (
    PinnedNativeSource,
    _format_tool_result_content,
    _iter_transcript,
    _parse_event_ts_ms,
)

SCHEMA_VERSION = 5

# Beyond this many warnings the document records a single suppression note
# instead of an unbounded list (warnings are per-item on a pathological source).
_MAX_WARNINGS = 50

# Claude writes API-error replies under this pseudo-model. It is never an
# observed model identity.
_SYNTHETIC_MODEL = "<synthetic>"

# --- small shared helpers ----------------------------------------------------

def _iso_ms(ms) -> str | None:
    if not isinstance(ms, (int, float)):
        return None
    return datetime.fromtimestamp(ms / 1000.0, timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def _iso_unix_s(seconds) -> str | None:
    if not isinstance(seconds, (int, float)):
        return None
    return _iso_ms(seconds * 1000)


def _now_iso() -> str:
    return _iso_ms(int(time.time() * 1000))


class _Counters:
    """Mutable tallies threaded through a single conversion."""

    def __init__(self) -> None:
        self.truncated = 0
        self.omitted = 0
        self.malformed = 0
        self.reasoning_excluded = 0
        self.bookkeeping_excluded = 0
        self.source_limited = False
        self.warnings: list[str] = []
        self._suppressed = 0

    def warn(self, message: str) -> None:
        if message in self.warnings:
            return
        if len(self.warnings) < _MAX_WARNINGS:
            self.warnings.append(message)
        else:
            self._suppressed += 1

    def warning_list(self) -> list[str]:
        if not self._suppressed:
            return list(self.warnings)
        return [*self.warnings, f"{self._suppressed} more warnings suppressed"]


def _result_payload(content, is_error: bool, limit: int | None,
                    counters: _Counters) -> dict:
    """A tool result, byte-exact unless an explicit bound clips it.

    ``contentSha256`` is the digest of the full stringified content, so a
    consumer holding the truncated bytes can still verify a copy of the whole
    thing. Truncation cuts on a UTF-8 codepoint boundary, so the returned text
    is always valid UTF-8 and ``returnedByteCount <= limit``.
    """
    text = content if isinstance(content, str) else _format_tool_result_content(content)
    payload = {
        "content": text,
        "isError": bool(is_error),
        "truncated": False,
        "originalByteCount": None,
        "returnedByteCount": None,
        "contentSha256": None,
    }
    if limit is None:
        return payload
    bounded = bound_value(text, limit)
    if not bounded.truncated:
        return payload
    counters.truncated += 1
    payload.update({
        "content": bounded.value,
        "truncated": True,
        "originalByteCount": bounded.original_byte_count,
        "returnedByteCount": bounded.returned_byte_count,
        "contentSha256": bounded.sha256,
    })
    return payload


def _no_result() -> dict:
    return {
        "content": None,
        "isError": False,
        "truncated": False,
        "originalByteCount": None,
        "returnedByteCount": None,
        "contentSha256": None,
    }


_SUMMARY_TARGET_MAX_BYTES = 512


def _summary_target(target: str) -> tuple[str, bool]:
    """Return a one-line, UTF-8-safe display target and whether it was cut."""
    line_end = len(target)
    for separator in ("\r", "\n"):
        index = target.find(separator)
        if index >= 0:
            line_end = min(line_end, index)
    first_line = target[:line_end]
    bounded = bound_value(first_line, _SUMMARY_TARGET_MAX_BYTES)
    return bounded.value, line_end != len(target) or bounded.truncated


def _summary_field(verb: str, target) -> dict:
    """Optional display metadata derived only from known native semantics.

    Summary targets are intentionally previews: first line only and at most 512
    UTF-8 bytes. The exact provider value remains in ``arguments``/native fields.
    """
    if not isinstance(target, str) or not target:
        return {}
    preview, truncated = _summary_target(target)
    if not preview:
        return {}
    summary = {"verb": verb, "target": preview}
    if truncated:
        summary["target_truncated"] = True
    return {"summary": summary}


def _summary_with_description(value: dict, description) -> dict:
    """Add one native model-written description to an existing summary."""
    if not isinstance(description, str):
        return value
    description = description.strip()
    if not description:
        return value
    summary = value.get("summary")
    if not isinstance(summary, dict):
        return value
    preview, truncated = _summary_target(description)
    preview = preview.strip()
    if not preview:
        return value
    summary["description"] = preview
    if truncated:
        summary["description_truncated"] = True
    return value


def _called_summary(name) -> dict:
    return _summary_field("Called", name)


def _claude_tool_summary(name, arguments) -> dict:
    """Conservative summary for one native Claude ``tool_use`` block.

    Only first-party argument names whose meaning is fixed by the native tool
    contract are inspected. Unknown/MCP tools never infer a display target from
    arbitrary ``path``/``name``/``target``-looking arguments.
    """
    if not isinstance(name, str) or not name:
        return {}
    args = arguments if isinstance(arguments, dict) else {}

    if name == "Bash":
        summary = _summary_field("Ran", args.get("command")) or _called_summary(name)
    elif name == "Read":
        summary = _summary_field("Read", args.get("file_path")) or _called_summary(name)
    elif name == "Edit":
        summary = _summary_field("Edited", args.get("file_path")) or _called_summary(name)
    elif name == "Write":
        summary = _summary_field("Wrote", args.get("file_path")) or _called_summary(name)
    elif name in ("Grep", "Glob"):
        summary = _summary_field("Searched", args.get("pattern")) or _called_summary(name)
    elif name == "WebSearch":
        summary = _summary_field("Searched", args.get("query")) or _called_summary(name)
    else:
        summary = _called_summary(name)

    if name in ("Bash", "Task", "Agent"):
        return _summary_with_description(summary, args.get("description"))
    return summary


def _codex_file_change_summary(native_changes, normalized_changes: list[dict]) -> dict:
    if not isinstance(native_changes, list) or not native_changes:
        return {}
    distinct_paths: list[str] = []
    seen: set[str] = set()
    for change in normalized_changes:
        path = change.get("path")
        if not isinstance(path, str) or not path or path in seen:
            continue
        seen.add(path)
        distinct_paths.append(path)
    if not distinct_paths:
        return {}
    if len(distinct_paths) == 1:
        return _summary_field("Changed", distinct_paths[0])
    return _summary_field("Changed", f"{len(distinct_paths)} files")


def _is_claude_narration(block: dict) -> bool:
    """Is this Claude ``thinking`` block user-visible narration?

    Claude Code (2.1.3xx) stores interim status text the user saw as a
    ``thinking`` block whose opaque signature carries a ``narration`` tag; real
    reasoning is stored with empty text. Anything unrecognised stays excluded.
    """
    signature = block.get("signature")
    if not isinstance(signature, str) or not signature:
        return False
    try:
        head = base64.b64decode(signature[:96] + "=" * (-len(signature[:96]) % 4))
    except (ValueError, binascii.Error):
        return False
    return b"narration" in head


def _item(kind: str, native_type: str | None, item_id=None, **fields) -> dict:
    return {"kind": kind, "id": item_id, "ordinal": None,
            "nativeType": native_type, **fields}


def _place(turn: dict, bucket: str, item: dict) -> dict:
    """Append one normalized item, stamping its place in the source order.

    ``ordinal`` is the order of the historical source being normalized: dense,
    zero-based, per turn, and assigned as each item is emitted from that
    ordered source — before it is split into ``input`` and ``output``. So

        sorted(turn["input"] + turn["output"], key=lambda i: i["ordinal"])

    reproduces the source's own sequence exactly, which the two buckets alone
    cannot: a Codex steer belongs in the middle of a turn, and grouping alone
    would file it beside the turn's opening prompt.

    It is not a claim about when things happened live. What the source lists is
    what this reproduces — including a source that omitted items or ordered
    them differently from delivery. Live chronology lives in the evidence
    journal's ``sequence``, and the two are never conflated.

    Because every emitted item lands in exactly one bucket and neither bucket
    is ever reordered or pruned, the count already placed is the next ordinal.
    Updating an item that was already emitted (Claude pairing a tool result
    with its call) mutates it in place and does not consume one.
    """
    item["ordinal"] = len(turn["input"]) + len(turn["output"])
    turn[bucket].append(item)
    return item


def _place_all(turn: dict, bucket: str, items) -> None:
    """Place several children of one native item, in source-part order."""
    for item in items:
        _place(turn, bucket, item)


def _turn(sequence: int, turn_id=None) -> dict:
    return {
        "turnId": turn_id,
        "sequence": sequence,
        "status": "completed",
        "startedAt": None,
        "completedAt": None,
        "input": [],
        "output": [],
        "usage": _empty_usage(),
    }


def _empty_usage() -> dict:
    return {
        "inputTokens": None,
        "outputTokens": None,
        "cachedInputTokens": None,
        "cacheCreationTokens": None,
        "reasoningTokens": None,
    }


def _completeness(requested: str, counters: _Counters) -> dict:
    # partial > source-limited > bounded > complete. Anything we could not
    # represent at all (a malformed record, structurally unusable data) is
    # partial: an export that dropped content must never read as complete.
    if counters.malformed or counters.omitted:
        status = "partial"
    elif counters.source_limited:
        status = "source-limited"
    elif counters.truncated:
        status = "bounded"
    else:
        status = "complete"
    return {
        "requested": requested,
        "status": status,
        "sourceLimited": bool(counters.source_limited),
        "truncatedItemCount": counters.truncated,
        "omittedItemCount": counters.omitted,
        "warnings": counters.warning_list(),
    }


# What a document says when no live evidence journal backs it: a Claude
# document (Claude has no journal), or a Codex thread CCX observed before it
# began journaling. Deliberately a separate axis from ``completeness``: content
# read back from ``thread/read`` can be perfectly complete while CCX never saw
# the turn run.
_NO_EVIDENCE = {
    "source": None,
    "available": False,
    "status": "absent",
    "beganAtMs": None,
    "eventCount": 0,
    "enrichedItemCount": 0,
    "warnings": [],
}


def _evidence_block(index: dict | None, enriched: int) -> dict:
    if not isinstance(index, dict):
        return dict(_NO_EVIDENCE)
    status = index.get("status") or (
        "complete" if index.get("complete") else "degraded"
    )
    if status == "absent":
        return dict(_NO_EVIDENCE)
    return {
        "source": "ccx-codex-evidence-journal",
        # A degraded journal whose file is gone entirely is still not
        # ``absent``: CCX knows evidence was attempted and lost, and saying
        # "none" would erase that.
        "available": bool(index.get("available")),
        "status": status,
        "beganAtMs": index.get("beganAtMs"),
        "eventCount": index.get("eventCount") or 0,
        "enrichedItemCount": enriched,
        "warnings": list(index.get("warnings") or []),
    }


def _document(*, worker: dict, state: str | None, turns: list[dict],
              requested: str, counters: _Counters,
              artifacts: list[dict], evidence: dict | None = None) -> dict:
    return {
        "schemaVersion": SCHEMA_VERSION,
        "generatedAt": _now_iso(),
        "worker": worker,
        "state": state,
        "turns": turns,
        "completeness": _completeness(requested, counters),
        # Evidence health, never folded into `completeness`: a worker with no
        # journal still has complete content.
        "evidence": evidence if evidence is not None else dict(_NO_EVIDENCE),
        "nativeArtifacts": artifacts,
        "stats": {
            "turnCount": len(turns),
            "itemCount": sum(len(t["input"]) + len(t["output"]) for t in turns),
            "malformedNativeRecords": counters.malformed,
            "excludedReasoningBlocks": counters.reasoning_excluded,
            "excludedBookkeepingRecords": counters.bookkeeping_excluded,
        },
    }


def _requested_label(max_tool_result_bytes: int | None) -> str:
    if max_tool_result_bytes is None:
        return "full"
    # 0 is a legitimate bound ("metadata only"): it yields empty content with a
    # truthful original byte count and the full-content digest, so nothing is
    # claimed that was not delivered.
    if isinstance(max_tool_result_bytes, bool) or not isinstance(
        max_tool_result_bytes, int
    ) or max_tool_result_bytes < 0:
        raise InvalidRequest(
            "max_tool_result_bytes must be a non-negative integer"
        )
    return "bounded"


# --- Claude ------------------------------------------------------------------

def _command_from_text(text: str) -> tuple[str, str | None] | None:
    """``<command-name>/compact</command-name><command-args>x</command-args>``."""
    if "<command-name>" not in text or "</command-name>" not in text:
        return None
    i = text.find("<command-name>") + len("<command-name>")
    j = text.find("</command-name>")
    if i >= j:
        return None
    name = text[i:j].strip()
    args = None
    if "<command-args>" in text and "</command-args>" in text:
        ai = text.find("<command-args>") + len("<command-args>")
        aj = text.find("</command-args>")
        if ai < aj:
            args = text[ai:aj].strip() or None
    return name, args


def _plain_slash_command(text: str) -> tuple[str, str | None] | None:
    """Parse one literal user-entered slash command without interpreting prose."""
    value = text.strip()
    if not value.startswith("/") or value.startswith("//") or "\n" in value or "\r" in value:
        return None
    if value.startswith("<") or "<command-name>" in value:
        return None
    name, separator, rest = value.partition(" ")
    if len(name) <= 1:
        return None
    return name, (rest.strip() or None) if separator else None


def _claude_structural_links(
    events: list[dict],
) -> tuple[dict[int, str], set[int], set[int]]:
    """Resolve optional Claude-only enrichment by exact native identity.

    Returns ``(summary_by_boundary_index, consumed_summary_indexes,
    duplicate_local_command_indexes)``. No timestamp, adjacency, or prose
    heuristics participate. Ambiguous/unlinked compact summaries remain
    unconsumed so the ordinary normalization path preserves their text.
    """
    boundaries: dict[str, list[int]] = {}
    summaries: dict[str, list[tuple[int, str]]] = {}
    seen_plain_commands: dict[str, set[tuple[str, str | None]]] = {}
    duplicate_commands: set[int] = set()

    for index, evt in enumerate(events):
        if not isinstance(evt, dict):
            continue
        if evt.get("type") == "system" and evt.get("subtype") == "compact_boundary":
            boundary_id = evt.get("uuid")
            if isinstance(boundary_id, str) and boundary_id:
                boundaries.setdefault(boundary_id, []).append(index)
            continue

        if evt.get("type") != "user" or evt.get("isMeta"):
            continue
        message = evt.get("message") if isinstance(evt.get("message"), dict) else {}
        content = message.get("content")
        if evt.get("isCompactSummary"):
            parent = evt.get("parentUuid")
            if (
                isinstance(parent, str)
                and parent
                and isinstance(content, str)
                and content.strip()
            ):
                summaries.setdefault(parent, []).append((index, content))
            continue

        if not isinstance(content, str):
            continue
        prompt_id = evt.get("promptId")
        if not isinstance(prompt_id, str) or not prompt_id:
            continue
        wrapped = _command_from_text(content)
        if wrapped is not None:
            if wrapped in seen_plain_commands.get(prompt_id, set()):
                duplicate_commands.add(index)
            continue
        plain = _plain_slash_command(content)
        if plain is not None:
            seen_plain_commands.setdefault(prompt_id, set()).add(plain)

    summary_by_boundary_index: dict[int, str] = {}
    consumed_summary_indexes: set[int] = set()
    for boundary_id, summary_rows in summaries.items():
        boundary_rows = boundaries.get(boundary_id) or []
        if len(boundary_rows) != 1 or len(summary_rows) != 1:
            continue
        summary_index, summary_text = summary_rows[0]
        summary_by_boundary_index[boundary_rows[0]] = summary_text
        consumed_summary_indexes.add(summary_index)

    return summary_by_boundary_index, consumed_summary_indexes, duplicate_commands


def _claude_attachment(block: dict) -> dict:
    """Image/attachment metadata with its locator, never its inline bytes."""
    source = block.get("source")
    locator = None
    media_type = None
    native = dict(block)
    if isinstance(source, dict):
        locator = source.get("url") or source.get("file_id") or source.get("path")
        media_type = source.get("media_type")
        native["source"] = {
            k: v for k, v in source.items() if k not in ("data", "bytes")
        }
    return _item(
        "attachment", block.get("type"),
        locator=locator, mediaType=media_type, native=native,
    )


# Claude Code session bookkeeping: titles, modes, file-history and API-request
# snapshots, hook outcomes, and the context Claude injects into the model's
# prompt (system prompt, environment, tool and skill listings, account details).
# None of it is conversation. Only these known shapes are dropped; an unknown
# record type still surfaces as an `other` item so nothing new disappears.
_CLAUDE_BOOKKEEPING_TYPES = frozenset({
    "last-prompt", "custom-title", "ai-title", "agent-name", "mode",
    "permission-mode", "atis-latch", "file-history-snapshot",
    "file-history-delta", "api-request", "api-request-shape",
    "api-request-blob",
})
_CLAUDE_BOOKKEEPING_ATTACHMENTS = frozenset({
    "hook_success", "hook_non_blocking_error", "hook_permission_decision",
    "hook_additional_context", "environment", "model", "deferred_tools_delta",
    "agent_listing_delta", "mcp_instructions_delta", "skill_listing",
    "total_tokens_reminder", "session_context", "date", "credential_org",
    "remote_session_change", "prompt_snapshot",
})
_CLAUDE_BOOKKEEPING_SYSTEM = frozenset({"stop_hook_summary", "turn_duration"})


def _claude_bookkeeping(evt: dict) -> bool:
    """Whether a native Claude record is session bookkeeping, not conversation."""
    etype = evt.get("type")
    if etype in _CLAUDE_BOOKKEEPING_TYPES:
        return True
    if etype == "attachment":
        return (evt.get("attachment") or {}).get("type") in _CLAUDE_BOOKKEEPING_ATTACHMENTS
    if etype == "system":
        return evt.get("subtype") in _CLAUDE_BOOKKEEPING_SYSTEM
    return False


def _claude_page_bearing(evt: dict) -> bool:
    """Whether one native Claude record belongs to bounded conversation history."""
    etype = evt.get("type")
    if etype == "assistant":
        return True
    if etype == "attachment":
        return (evt.get("attachment") or {}).get("type") == "queued_command"
    if etype == "system":
        return evt.get("subtype") == "compact_boundary"
    if etype != "user" or evt.get("isMeta"):
        return False
    if evt.get("isCompactSummary"):
        # Contextual normalization suppresses only summaries that were consumed
        # into an exactly identified compact boundary. Unlinked future/provider
        # shapes stay page-bearing so their text cannot disappear silently.
        return True
    content = (evt.get("message") or {}).get("content")
    if isinstance(content, str):
        text = content.strip()
        if not text:
            return False
        if text.startswith(("<local-command-stdout>", "<local-command-stderr>")):
            return False
        return True
    if not isinstance(content, list):
        return False
    for block in content:
        if not isinstance(block, dict):
            continue
        btype = block.get("type")
        if btype == "text" and (block.get("text") or "").strip():
            return True
        if btype in {"tool_result", "image", "document"}:
            return True
        if btype not in {None, "text"}:
            return True
    return False


def _claude_usage(message: dict) -> dict | None:
    usage = message.get("usage")
    if not isinstance(usage, dict) or not usage:
        return None
    return {
        "inputTokens": usage.get("input_tokens"),
        "outputTokens": usage.get("output_tokens"),
        "cachedInputTokens": usage.get("cache_read_input_tokens"),
        "cacheCreationTokens": usage.get("cache_creation_input_tokens"),
        "reasoningTokens": None,
    }


def _normalize_claude_events(
    events: list[dict],
    counters: _Counters,
    max_tool_result_bytes: int | None,
) -> tuple[list[dict], str | None, list[dict]]:
    """Normalize Claude events once for full documents and bounded pages."""
    turns: list[dict] = []
    source_records: list[dict] = []
    observed_model: str | None = None
    current: dict | None = None
    awaiting: dict[str, dict] = {}
    usage_record_by_message: dict[str, dict] = {}
    (
        compact_summary_by_boundary_index,
        consumed_compact_summary_indexes,
        duplicate_local_command_indexes,
    ) = _claude_structural_links(events)

    def open_turn() -> dict:
        nonlocal current
        current = _turn(len(turns) + 1)
        turns.append(current)
        return current

    def stamp(turn: dict, ts_ms) -> None:
        iso = _iso_ms(ts_ms)
        if iso is None:
            return
        if turn["startedAt"] is None:
            turn["startedAt"] = iso
        turn["completedAt"] = iso

    for event_index, evt in enumerate(events):
        etype = evt.get("type")
        ts = _parse_event_ts_ms(evt)
        message = evt.get("message") or {}
        page_bearing = (
            _claude_page_bearing(evt)
            and event_index not in consumed_compact_summary_indexes
            and event_index not in duplicate_local_command_indexes
        )
        record = {
            "turn_id": None,
            "source_record_id": evt.get("uuid"),
            "timestamp_ms": ts,
            "duration_ms": None,
            "usage": None,
            "items": [],
            "_ccx_start_offset": evt.get("_ccx_start_offset"),
            "_ccx_end_offset": evt.get("_ccx_end_offset"),
            "_ccx_page_bearing": page_bearing,
            "_ccx_type": etype,
            "_ccx_fingerprint": hashlib.sha256(
                json.dumps(
                    safe_native(evt),
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                    default=str,
                ).encode("utf-8")
            ).hexdigest(),
        }
        source_records.append(record)

        def place(target: dict, bucket: str, item: dict) -> dict:
            placed = _place(target, bucket, item)
            if page_bearing:
                record["items"].append(placed)
            return placed

        if _claude_bookkeeping(evt):
            # Never page-bearing, so bounded pages are unaffected; the source
            # record above still counts toward paging positions.
            counters.bookkeeping_excluded += 1
            continue

        if etype == "user":
            if (
                event_index in consumed_compact_summary_indexes
                or event_index in duplicate_local_command_indexes
            ):
                continue
            content = message.get("content")
            blocks = (
                [{"type": "text", "text": content}]
                if isinstance(content, str)
                else (content if isinstance(content, list) else [])
            )
            opened: dict | None = None
            for block in blocks:
                if not isinstance(block, dict):
                    counters.omitted += 1
                    continue
                btype = block.get("type")
                if btype == "text":
                    text = block.get("text") or ""
                    command = _command_from_text(text)
                    if command is None and not text.strip():
                        continue
                    # A hidden meta row (e.g. Claude's image-source note)
                    # stays in the turn it follows instead of opening one.
                    if opened is None and evt.get("isMeta") and current is not None:
                        opened = current
                    opened = opened or open_turn()
                    if command is not None:
                        name, args = command
                        place(opened, "input", _item(
                            "command", "user", evt.get("uuid"),
                            name=name, arguments={"args": args} if args else {},
                        ))
                    else:
                        place(opened, "input", _item(
                            "user-text", "user", evt.get("uuid"), text=text,
                        ))
                    stamp(opened, ts)
                elif btype == "tool_result":
                    tid = block.get("tool_use_id")
                    payload = _result_payload(
                        block.get("content"), block.get("is_error"),
                        max_tool_result_bytes, counters,
                    )
                    call = awaiting.pop(tid, None) if tid else None
                    if call is not None:
                        call["result"] = payload
                        call["status"] = "failed" if payload["isError"] else "completed"
                        continue
                    counters.warn(
                        f"tool_result {tid or '<no id>'} has no unpaired "
                        "tool_use (unmatched or duplicate); kept standalone"
                    )
                    target = current or open_turn()
                    place(target, "output", _item(
                        "tool-result", "tool_result", tid,
                        callId=tid, unpaired=True, result=payload,
                    ))
                    stamp(target, ts)
                elif btype in ("image", "document"):
                    opened = opened or open_turn()
                    place(opened, "input", _claude_attachment(block))
                    stamp(opened, ts)
                else:
                    target = current or open_turn()
                    place(target, "output", _item(
                        "other", btype, None, native=safe_native(block),
                    ))
                    stamp(target, ts)
            continue

        if etype == "assistant":
            target = current or open_turn()
            stamp(target, ts)
            if evt.get("isApiErrorMessage"):
                text = message.get("content")
                if not isinstance(text, str):
                    text = _format_tool_result_content(text)
                place(target, "output", _item(
                    "error", "assistant", message.get("id"),
                    message=text,
                    data={"status": evt.get("status"),
                          "requestId": evt.get("requestId")},
                ))
                target["status"] = "failed"
                continue
            model = message.get("model")
            if isinstance(model, str) and model and model != _SYNTHETIC_MODEL:
                observed_model = model
            content = message.get("content")
            for block in (content if isinstance(content, list) else []):
                if not isinstance(block, dict):
                    counters.omitted += 1
                    continue
                btype = block.get("type")
                if btype == "text":
                    text = block.get("text") or ""
                    if not text:
                        continue
                    place(target, "output", _item(
                        "assistant-text", "assistant", message.get("id"),
                        text=text, model=model,
                    ))
                elif btype == "thinking" and _is_claude_narration(block):
                    # Claude Code persists some user-visible interim text as a
                    # narration-signed thinking block; it is not hidden reasoning.
                    text = block.get("thinking") or ""
                    if not text.strip():
                        continue
                    place(target, "output", _item(
                        "assistant-text", "assistant", message.get("id"),
                        text=text, model=model, narration=True,
                    ))
                elif btype in ("thinking", "redacted_thinking"):
                    counters.reasoning_excluded += 1
                elif btype == "tool_use":
                    tid = block.get("id")
                    tool_name = block.get("name")
                    tool_arguments = block.get("input") or {}
                    call = place(target, "output", _item(
                        "tool-call", "tool_use", tid,
                        callId=tid, name=tool_name,
                        arguments=tool_arguments,
                        status="in-progress", result=_no_result(),
                        **_claude_tool_summary(tool_name, tool_arguments),
                    ))
                    if tid:
                        awaiting[tid] = call
                else:
                    place(target, "output", _item(
                        "other", btype, None, native=safe_native(block),
                    ))
            usage = _claude_usage(message)
            if usage is not None:
                target["usage"] = usage
                mid = message.get("id")
                if isinstance(mid, str) and mid:
                    previous = usage_record_by_message.get(mid)
                    if previous is not None:
                        previous["usage"] = None
                    usage_record_by_message[mid] = record
                record["usage"] = dict(usage)
            continue

        if etype == "attachment":
            att = evt.get("attachment") or {}
            if att.get("type") == "queued_command":
                # A queued message is a string, or, when it carries an image
                # or document, a content-block list like a user message's.
                prompt = att.get("prompt")
                media: list[dict] = []
                others: list[dict] = []
                if isinstance(prompt, list):
                    parts = []
                    for block in prompt:
                        if isinstance(block, str):
                            parts.append(block)
                        elif not isinstance(block, dict):
                            counters.omitted += 1
                        elif block.get("type") == "text":
                            parts.append(block.get("text") or "")
                        elif block.get("type") in ("image", "document"):
                            media.append(block)
                        else:
                            others.append(block)
                    text = "\n".join(p for p in parts if p.strip()).strip()
                else:
                    text = prompt.strip() if isinstance(prompt, str) else ""
                if not text and not media and not others:
                    continue
                target = open_turn()
                if text:
                    place(target, "input", _item(
                        "user-text", "queued_command", evt.get("uuid"), text=text,
                    ))
                for block in media:
                    place(target, "input", _claude_attachment(block))
                for block in others:  # kept, as in a user message
                    place(target, "output", _item(
                        "other", block.get("type"), None, native=safe_native(block),
                    ))
                stamp(target, ts)
            else:
                target = current or open_turn()
                place(target, "output", _item(
                    "other", "attachment", evt.get("uuid"),
                    native=safe_native(evt),
                ))
                stamp(target, ts)
            continue

        if etype == "system" and evt.get("subtype") == "compact_boundary":
            meta = evt.get("compactMetadata") or {}
            compact_data = {
                "preTokens": meta.get("preTokens"),
                "postTokens": meta.get("postTokens"),
                "durationMs": meta.get("durationMs"),
                "trigger": meta.get("trigger"),
            }
            compact_summary = compact_summary_by_boundary_index.get(event_index)
            if compact_summary is not None:
                compact_data["summary"] = compact_summary
            target = current or open_turn()
            place(target, "output", _item(
                "system-text", "compact_boundary", evt.get("uuid"),
                text=evt.get("content") or "",
                data=compact_data,
            ))
            stamp(target, ts)
            continue

        target = current or open_turn()
        place(target, "output", _item(
            "other", etype, evt.get("uuid"), native=safe_native(evt),
        ))
        stamp(target, ts)

    if turns:
        last = turns[-1]
        if last["status"] == "completed" and any(
            i["kind"] == "tool-call" and i["status"] == "in-progress"
            for i in last["output"]
        ):
            last["status"] = "in-progress"

    return turns, observed_model, source_records


def build_claude(rec: dict, *, state: str | None, worker: str,
                 max_tool_result_bytes: int | None = None,
                 before_offset: int | None = None,
                 after_offset: int = 0) -> dict:
    """Normalize a managed Claude worker's raw JSONL transcript.

    ``after_offset`` starts at a turn boundary to read only the newest turns.
    """
    requested = _requested_label(max_tool_result_bytes)
    short = rec.get("short")
    src = rec.get("transcript")
    pinned = isinstance(src, PinnedNativeSource)
    if not src or (not pinned and not os.path.exists(src)):
        raise SessionNotFound(
            f"no native transcript available for {short}", short=short
        )

    counters = _Counters()
    raw_stats: dict = {"malformedLines": 0}
    try:
        events = list(_iter_transcript(src, raw_stats, after_offset=after_offset,
                                       before_offset=before_offset))
    except OSError as error:
        raise SessionNotFound(
            f"transcript not readable for {short}: {error.__class__.__name__}",
            short=short,
        ) from error
    counters.malformed = raw_stats.get("malformedLines", 0)
    if counters.malformed:
        counters.warn(
            f"{counters.malformed} malformed native JSONL line(s) skipped"
        )

    turns, observed_model, _records = _normalize_claude_events(
        events, counters, max_tool_result_bytes,
    )

    artifact = {
        "kind": "claude-transcript-jsonl",
        "path": "<pinned-original>" if pinned else src,
        "byteCount": None,
        "sha256": None,
        "availability": "referenced",
    }
    try:
        artifact["byteCount"] = src.size() if pinned else os.path.getsize(src)
    except OSError:
        pass

    return _document(
        worker={
            "handle": worker,
            "engine": "claude",
            "short": short,
            "nativeSessionId": rec.get("sessionId"),
            "cwd": rec.get("cwd"),
            "requestedModel": (rec.get("createArgs") or {}).get("model"),
            "observedModel": observed_model,
        },
        state=state,
        turns=turns,
        requested=requested,
        counters=counters,
        artifacts=[artifact],
    )


# --- Codex -------------------------------------------------------------------

def _codex_user_input(entry: dict, item_id, client_id, counters: _Counters) -> dict:
    """One part of a native ``userMessage``, carrying its provenance.

    ``clientId`` is exactly what the native item holds — CCX's own id, another
    client's, or ``None`` — and every part of one multipart message shares it.
    ``origin`` is only ever set when that id parses as an id CCX itself minted;
    it is never inferred from the message's position, text, or turn shape, so
    ``None`` truthfully means "not provably ours".
    """
    provenance = {"clientId": client_id, "origin": origin_of(client_id)}
    itype = entry.get("type")
    if itype == "text":
        return _item("user-text", "userMessage", item_id,
                     text=entry.get("text") or "", **provenance)
    locator = entry.get("url") or entry.get("path")
    if itype not in ("image", "localImage", "skill", "mention"):
        counters.warn(f"unrecognized Codex user input type {itype!r}")
    return _item("attachment", itype, item_id,
                 locator=locator, mediaType=None, native=safe_native(entry),
                 **provenance)


def _codex_data(entry: dict, *fields: str) -> dict:
    """Keep selected native metadata without copying the whole item."""
    data = {
        field: safe_native(entry[field])
        for field in fields
        if field in entry
    }
    return {"data": data} if data else {}


def _codex_tool_name(namespace, tool) -> str | None:
    if namespace:
        return f"{namespace}/{tool}"
    return tool


def _codex_item(entry: dict, counters: _Counters,
                limit: int | None, observed_model: str | None) -> list[dict]:
    """One native Codex thread item -> zero or more normalized items."""
    itype = entry.get("type")
    item_id = entry.get("id")

    if itype == "userMessage":
        content = entry.get("content")
        client_id = entry.get("clientId")
        client_id = client_id if isinstance(client_id, str) else None
        return [
            _codex_user_input(part, item_id, client_id, counters)
            for part in (content if isinstance(content, list) else [])
            if isinstance(part, dict)
        ]

    if itype == "hookPrompt":
        fragments = entry.get("fragments")
        fragments = fragments if isinstance(fragments, list) else []
        text = "\n".join(
            fragment.get("text") or ""
            for fragment in fragments
            if isinstance(fragment, dict)
        )
        # Hook input is deliberately not user-authored input. Keep it in the
        # input bucket, but make its source and native fragments explicit.
        return [_item(
            "system-text", itype, item_id, text=text,
            **_codex_data(entry, "fragments"),
        )]

    if itype == "agentMessage":
        return [_item("assistant-text", itype, item_id,
                      text=entry.get("text") or "", model=observed_model,
                      phase=entry.get("phase"),
                      memoryCitation=safe_native(entry.get("memoryCitation")))]

    if itype == "plan":
        return [_item("assistant-plan", itype, item_id,
                      text=entry.get("text") or "")]

    if itype == "reasoning":
        # Only the engine's own surfaced summary; ``content`` is raw reasoning
        # and is never emitted.
        counters.reasoning_excluded += 1
        summary = entry.get("summary")
        parts = [str(s) for s in summary] if isinstance(summary, list) else []
        return [_item("reasoning-summary", itype, item_id,
                      text="\n".join(parts))]

    if itype == "commandExecution":
        status = _CODEX_STATUS.get(entry.get("status"))
        exit_code = entry.get("exitCode")
        output = entry.get("aggregatedOutput")
        result = _result_payload(
            output if output is not None else "",
            exit_code not in (0, None) or status in ("failed", "declined"),
            limit, counters,
        )
        # Named "shell" so Codex command execution and Claude's Bash tool line
        # up in a cross-engine comparison.
        return [_item("tool-call", itype, item_id,
                      callId=item_id, name="shell",
                      arguments={"command": entry.get("command"),
                                 "cwd": entry.get("cwd")},
                      status=status, result=result,
                      **(_summary_field("Ran", entry.get("command"))
                         or _called_summary("shell")),
                      **_codex_data(
                          entry, "exitCode", "durationMs", "source",
                          "processId", "pluginId", "scriptPath",
                          "commandActions",
                      ))]

    if itype == "fileChange":
        changes = entry.get("changes")
        normalized_changes = [
            {"path": c.get("path"), "kind": c.get("kind"), "diff": c.get("diff")}
            for c in (changes if isinstance(changes, list) else [])
            if isinstance(c, dict)
        ]
        return [_item(
            "file-change", itype, item_id,
            status=_CODEX_STATUS.get(entry.get("status")),
            changes=normalized_changes,
            **_codex_file_change_summary(changes, normalized_changes),
        )]

    if itype == "mcpToolCall":
        error = entry.get("error")
        native_error = safe_native(error)
        native_result = safe_native(entry.get("result"))
        payload = _result_payload(
            native_error if native_error is not None else native_result,
            error is not None, limit, counters,
        ) if (error is not None or native_result is not None) else _no_result()
        server = entry.get("server")
        tool = entry.get("tool")
        tool_name = (
            f"{server}/{tool}"
            if isinstance(server, str) and server
            and isinstance(tool, str) and tool
            else None
        )
        return [_item("tool-call", itype, item_id,
                      callId=item_id,
                      name=tool_name,
                      arguments=entry.get("arguments")
                      if entry.get("arguments") is not None else {},
                      status=_CODEX_STATUS.get(entry.get("status")),
                      result=payload,
                      **_called_summary(tool_name),
                      **_codex_data(
                          entry, "appContext", "mcpAppResourceUri", "pluginId",
                          "readOnlyHint", "durationMs",
                      ))]

    if itype == "dynamicToolCall":
        status = _CODEX_STATUS.get(entry.get("status"))
        content_items = entry.get("contentItems")
        success = entry.get("success")
        if content_items is None:
            result = _no_result()
        else:
            result = _result_payload(
                safe_native(content_items),
                success is False or status == "failed",
                limit, counters,
            )
        tool_name = _codex_tool_name(entry.get("namespace"), entry.get("tool"))
        return [_item(
            "tool-call", itype, item_id, callId=item_id,
            name=tool_name,
            arguments=entry.get("arguments")
            if entry.get("arguments") is not None else {},
            status=status, result=result,
            **_called_summary(tool_name),
            **_codex_data(
                entry, "namespace", "success", "durationMs",
            ),
        )]

    if itype == "collabAgentToolCall":
        return [_item(
            "subagent-action", itype, item_id,
            callId=item_id, name=entry.get("tool"),
            status=_CODEX_STATUS.get(entry.get("status")),
            **_codex_data(
                entry, "senderThreadId", "receiverThreadIds", "prompt",
                "model", "reasoningEffort", "agentsStates",
            ),
        )]

    if itype == "subAgentActivity":
        return [_item(
            "subagent-activity", itype, item_id,
            **_codex_data(entry, "kind", "agentThreadId", "agentPath"),
        )]

    if itype == "webSearch":
        results = entry.get("results")
        result = (_result_payload(
            safe_native(results), False, limit, counters,
        ) if results is not None else _no_result())
        return [_item("tool-call", itype, item_id,
                      callId=item_id, name="web_search",
                      arguments={"query": entry.get("query"),
                                 "action": safe_native(entry.get("action"))},
                      status=None, result=result,
                      **(_summary_field("Searched", entry.get("query"))
                         or _called_summary("web_search")))]

    if itype == "imageView":
        return [_item(
            "attachment", itype, item_id,
            locator=entry.get("path"), mediaType=None,
        )]

    if itype == "sleep":
        return [_item(
            "action", itype, item_id, name="sleep",
            **_codex_data(entry, "durationMs"),
        )]

    if itype == "imageGeneration":
        return [_item(
            "action", itype, item_id, name="image_generation",
            status=_CODEX_STATUS.get(entry.get("status")),
            **_codex_data(
                entry, "revisedPrompt", "savedPath",
                "transparentBackground",
            ),
        )]

    if itype in ("enteredReviewMode", "exitedReviewMode"):
        return [_item(
            "system-text", itype, item_id, text=entry.get("review"),
            data={"transition": "entered" if itype == "enteredReviewMode"
                  else "exited"},
        )]

    if itype == "contextCompaction":
        return [_item("system-text", itype, item_id, text=None, data=None)]

    if itype == "functionCallOutput":
        return [_item(
            "tool-result", itype, item_id,
            callId=entry.get("callId") or item_id,
            name=entry.get("name"),
            result=_result_payload(entry.get("output"), False, limit, counters),
        )]

    counters.warn(f"unmapped Codex thread item type {itype!r} preserved as other")
    return [_item("other", itype, item_id, native=safe_native(entry))]


def _evidence_entry(
    entry: dict, turn_evidence: dict, turn_clients: dict
) -> dict | None:
    """The live evidence for one native item, by exact identity only.

    Two identities qualify, in this order:

    1. the native item id the notification itself carried; and
    2. for a ``userMessage`` only, the CCX client id — which the native
       protocol puts on both surfaces, so it is a real shared identity for the
       semantic message even when the two item ids differ.

    Nothing else is ever used: not text, not position, not ordinal, not
    timestamps, not similarity. Tool, reasoning, and agent-message ids stay
    disjoint until a protocol gives them a legitimate key of their own.
    """
    direct = turn_evidence.get(entry.get("id"))
    if isinstance(direct, dict):
        return direct
    if entry.get("type") != "userMessage":
        return None
    client_id = entry.get("clientId")
    if not parse_client_message_id(client_id):
        # Only an id CCX can prove it minted. A foreign or absent one is not an
        # identity, and neither is one that merely looks similar.
        return None
    # A falsy value is the index's ambiguity marker: the same CCX id was seen
    # on two different live items, so it names neither.
    item_id = turn_clients.get(client_id)
    return turn_evidence.get(item_id) if item_id else None


def _apply_timing(children: list[dict], entry: dict | None) -> int:
    """Stamp live item timing onto the children of one native item.

    Only the native millisecond timestamps the notifications actually carried
    are used, and only for the item they named. Turn-level times are never
    spread over the items inside the turn.
    """
    if not isinstance(entry, dict):
        return 0
    started = entry.get("startedAtMs")
    completed = entry.get("completedAtMs")
    if not isinstance(started, int) and not isinstance(completed, int):
        return 0
    for child in children:
        child["startedAtMs"] = started if isinstance(started, int) else None
        child["completedAtMs"] = (
            completed if isinstance(completed, int) else None
        )
        child["timingSource"] = "ccx-live-item-notification"
    return len(children)


_CODEX_STATUS = {
    "inProgress": "in-progress",
    "completed": "completed",
    "failed": "failed",
    "declined": "declined",
}

_CODEX_TURN_STATUS = {
    "completed": "completed",
    "interrupted": "interrupted",
    "failed": "failed",
    "inProgress": "in-progress",
}

# Items that are turn *input* rather than model output.
_CODEX_INPUT_TYPES = frozenset({"userMessage", "hookPrompt"})


def build_codex(rec: dict, thread: dict | None, *, state: str | None,
                worker: str,
                max_tool_result_bytes: int | None = None,
                evidence: dict | None = None) -> dict:
    "Normalize a Codex ``thread/read(includeTurns=true)`` response.\n\n    ``evidence`` is an optional index built from this worker's live evidence\n    journal (:func:`ccx.codex.evidence.read_index`). It only ever adds what\n    ``thread/read`` cannot carry — the exact native millisecond timestamps CCX\n    observed on ``item/started``/``item/completed``. Matching is by the exact\n    ``(turnId, itemId)`` the event stream itself recorded; an item with no such\n    evidence keeps no timing at all rather than borrowing its turn's.\n    "
    requested = _requested_label(max_tool_result_bytes)
    short = rec.get("short")
    if not isinstance(thread, dict):
        raise SessionNotFound(
            f"no Codex thread available for {short}", short=short
        )

    counters = _Counters()
    if thread.get("historySource") == "thread/items/list":
        # Native item pages are complete as items, but do not expose the
        # per-turn status/timestamps that thread/read(includeTurns) supplied.
        counters.source_limited = True
        counters.warn(
            "native turn listing is unsupported; item pages preserve original "
            "item history, but turn status and timestamps are unavailable"
        )
    observed_model = rec.get("resolvedModel")
    native_turns = thread.get("turns")
    native_turns = native_turns if isinstance(native_turns, list) else []
    if not native_turns and (rec.get("stats") or {}).get("turns"):
        counters.source_limited = True
        counters.warn(
            "app-server returned no turns although the worker has recorded "
            "turns; native history is unavailable"
        )

    evidence_items = (evidence or {}).get("items") or {}
    evidence_clients = (evidence or {}).get("clients") or {}
    evidence_turns = (evidence or {}).get("turns") or {}
    # Only evidence CCX can vouch for may be used to contradict the historical
    # source. A missing, partial, or degraded journal proves nothing about what
    # `thread/read` left out.
    evidence_proves = (evidence or {}).get("status") == "complete"
    enriched = 0

    turns: list[dict] = []
    for index, native in enumerate(native_turns):
        if not isinstance(native, dict):
            counters.omitted += 1
            continue
        turn = _turn(len(turns) + 1, native.get("id"))
        turn_evidence = evidence_items.get(native.get("id")) or {}
        turn_clients = evidence_clients.get(native.get("id")) or {}
        turn["status"] = _CODEX_TURN_STATUS.get(native.get("status"))
        turn["startedAt"] = _iso_unix_s(native.get("startedAt"))
        turn["completedAt"] = _iso_unix_s(native.get("completedAt"))
        view = native.get("itemsView", "full")
        if view != "full":
            counters.source_limited = True
            counters.warn(
                f"turn {native.get('id') or index} itemsView={view!r}: "
                "app-server did not return its full item list "
                "(omitted item count unknown)"
            )
        items = native.get("items")
        native_items = 0
        for entry in (items if isinstance(items, list) else []):
            if not isinstance(entry, dict):
                counters.omitted += 1
                continue
            native_items += 1
            bucket = ("input" if entry.get("type") in _CODEX_INPUT_TYPES
                      else "output")
            children = _codex_item(
                entry, counters, max_tool_result_bytes, observed_model,
            )
            enriched += _apply_timing(
                children, _evidence_entry(entry, turn_evidence, turn_clients)
            )
            _place_all(turn, bucket, children)
        coverage = evidence_turns.get(native.get("id")) or {}
        observed_items = coverage.get("distinctItemCount") or 0
        if (
            evidence_proves
            and coverage.get("sawTurnStarted")
            and coverage.get("sawTurnCompleted")
            and observed_items > native_items
        ):
            # CCX watched this turn from start to settlement and counted more
            # item lifecycles than the historical source returned. That is
            # positive proof the source is limited — the one direction evidence
            # can argue in. The missing items are not invented into the
            # document; they stay where they were actually observed.
            counters.source_limited = True
            counters.warn(
                f"live evidence observed {observed_items} item lifecycles for "
                f"turn {native.get('id')} but thread/read returned "
                f"{native_items}; the historical source omits live turn items; "
                f"the evidence journal retains them"
            )
        error = native.get("error")
        if error:
            _place(turn, "output", _item(
                "error", "turnError", None,
                message=(error.get("message") if isinstance(error, dict)
                         else str(error)),
                data=error if isinstance(error, dict) else None,
            ))
        turns.append(turn)

    return _document(
        worker={
            "handle": worker,
            "engine": "codex",
            "short": short,
            "nativeSessionId": rec.get("threadId"),
            "cwd": rec.get("cwd"),
            "requestedModel": (rec.get("createArgs") or {}).get("model"),
            "observedModel": observed_model,
        },
        state=state,
        turns=turns,
        requested=requested,
        # thread/read carries no per-turn usage, and tokenUsage.last is a
        # per-request figure, so inferring turn usage would be a fabrication.
        counters=counters,
        artifacts=[],
        evidence=_evidence_block(evidence, enriched),
    )


# --- export ------------------------------------------------------------------

def _atomic_write_bytes(path: str, payload: bytes) -> None:
    directory = os.path.dirname(path) or "."
    fd, tmp = tempfile.mkstemp(dir=directory, prefix=".conversation.", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
        raise


def _copy_hashed(src, dst: str) -> tuple[int, str]:
    """Stream-copy a native file, hashing as it goes."""
    digest = hashlib.sha256()
    size = 0
    reader = src.open_reader() if hasattr(src, "open_reader") else open(src, "rb")
    with reader as source, open(dst, "wb") as target:
        while True:
            chunk = source.read(1 << 20)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
            target.write(chunk)
    return size, digest.hexdigest()


def _exported(destination: str, kind: str, name: str, size: int,
              digest: str) -> dict:
    return {
        "kind": kind,
        "path": os.path.join("native", name),
        "byteCount": size,
        "sha256": digest,
        "availability": "exported",
    }


def _native_dir(destination: str) -> str:
    path = os.path.join(destination, "native")
    os.makedirs(path, exist_ok=True)
    return path


def write_export(doc: dict, destination, *, engine: str,
                 native_source=None, include_native: bool = True,
                 native_files=()) -> dict:
    """Write ``conversation.json`` (+ the native artifacts) under ``destination``.

    ``native_source`` is the primary native evidence: a path (Claude) or an
    already-materialized object (Codex ``thread/read`` response).
    ``native_files`` is any further on-disk native evidence the engine owns, as
    ``(kind, filename, path)`` triples — the Codex evidence journal is one. The
    document's ``nativeArtifacts`` is rewritten to describe exactly what was
    written, each with a real byte count and SHA-256.
    """
    destination = str(destination)
    os.makedirs(destination, exist_ok=True)
    artifacts: list[dict] = []

    if include_native and native_source is not None:
        native_dir = _native_dir(destination)
        if engine == "claude":
            name, kind = "claude-transcript.jsonl", "claude-transcript-jsonl"
            target = os.path.join(native_dir, name)
            try:
                size, digest = _copy_hashed(native_source, target)
            except OSError as error:
                raise SessionNotFound(
                    f"native transcript not readable: {error.__class__.__name__}",
                    short=doc.get("worker", {}).get("short"),
                ) from error
        else:
            name = "codex-thread.json"
            source = (native_source.get("historySource")
                      if isinstance(native_source, dict) else None)
            kind = ("codex-thread-turns-paged" if source == "thread/turns/list"
                    else "codex-thread-items-paged" if source == "thread/items/list"
                    else "codex-thread-read")
            target = os.path.join(native_dir, name)
            payload = json.dumps(native_source, indent=2).encode("utf-8")
            _atomic_write_bytes(target, payload)
            size, digest = len(payload), hashlib.sha256(payload).hexdigest()
        artifacts.append(_exported(destination, kind, name, size, digest))
    elif engine == "claude" and native_source is not None:
        artifacts = list(doc.get("nativeArtifacts") or [])

    if include_native:
        for kind, name, source in native_files:
            target = os.path.join(_native_dir(destination), name)
            try:
                size, digest = _copy_hashed(source, target)
            except FileNotFoundError:
                # Evidence that disappeared between listing and copying is
                # simply not claimed. An artifact entry is a promise that the
                # bytes are there.
                continue
            except OSError as error:
                raise SessionNotFound(
                    f"native {kind} not readable: {error.__class__.__name__}",
                    short=doc.get("worker", {}).get("short"),
                ) from error
            artifacts.append(_exported(destination, kind, name, size, digest))

    doc["nativeArtifacts"] = artifacts
    conversation_path = os.path.join(destination, "conversation.json")
    _atomic_write_bytes(
        conversation_path, json.dumps(doc, indent=2).encode("utf-8")
    )
    exported = [
        os.path.join(destination, artifact["path"])
        for artifact in artifacts if artifact["availability"] == "exported"
    ]
    return {
        "destination": destination,
        "conversation_path": conversation_path,
        # The primary native artifact, unchanged; `native_paths` is everything
        # that was written, in the order `nativeArtifacts` lists it.
        "native_path": exported[0] if exported else None,
        "native_paths": exported,
    }


__all__ = ["SCHEMA_VERSION", "build_claude", "build_codex", "write_export"]
