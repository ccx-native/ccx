"Transcript-derived carried-context usage for a worker session.\n\nClaude Code records token usage on every assistant turn (see\n``ccx.claude.transcript._usage_tokens``). The *occupancy* of the model's context window\nat any moment is just the latest completed assistant turn's input footprint:\n\n    tokens_used = input + cache_read_input + cache_creation_input\n\nwhich matches the input footprint surfaced by Claude Code's status line\n(``output`` is excluded). We derive this from the cleaned transcript turns, so\nit needs no extra capture, works retroactively, and updates exactly at turn\nboundaries.\n\nThe public telemetry deliberately reports only the carried token count. Window\ncapacity, occupancy percentage, and cost thresholds are policy rather than\nobservations needed by CCX's read surfaces; keeping them out also avoids model-\nspecific capacity guesses.\n"

from __future__ import annotations

import json
import time


STATUSLINE_CONTEXT_TTL_S = 90


# Settlement must not wait for a transcript render or parse a large history.
# A single pread of at most this many bytes is the only native work here.
_SETTLEMENT_TAIL_BYTES = 256 * 1024


def read_claude_settlement_context(
    path: str, *, start_offset: int | None, turn_ref: str, settled_ms: int,
) -> dict | None:
    """Best-effort bounded native tail for the exact in-flight managed turn.

    An unknown start fence is not license to borrow the previous turn's usage.
    Errors and unreadable/incomplete lines never block a settlement; the caller
    persists null when no trustworthy bounded reading can be made.
    """
    import os
    import stat
    from datetime import datetime

    if type(start_offset) is not int or start_offset < 0:
        return None
    fd = None
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0))
        size = os.fstat(fd)
        if not stat.S_ISREG(size.st_mode) or size.st_size < start_offset:
            return None
        begin = max(start_offset, size.st_size - _SETTLEMENT_TAIL_BYTES)
        raw = os.pread(fd, size.st_size - begin, begin)
    except (OSError, ValueError, TypeError):
        return None
    finally:
        if fd is not None:
            os.close(fd)
    if begin > start_offset:
        # The bounded tail may begin in the middle of a line.
        raw = raw.partition(bytes([10]))[2]
    if not raw.endswith(bytes([10])):
        raw = raw.rpartition(bytes([10]))[0]
    if not raw:
        return {"state": "pending", "tokens_used": None,
                "as_of_ms": settled_ms, "as_of_turn_ref": turn_ref}
    for line in reversed(raw.splitlines()):
        try:
            event = json.loads(line)
        except (ValueError, UnicodeError):
            continue
        if not isinstance(event, dict):
            continue
        stamp = event.get("timestamp")
        try:
            ms = int(datetime.fromisoformat(stamp.replace("Z", "+00:00")).timestamp() * 1000)
        except (AttributeError, ValueError, OverflowError, TypeError):
            ms = settled_ms
        if event.get("type") == "system" and event.get("subtype") == "compact_boundary":
            return {"state": "unknown", "tokens_used": None,
                    "reason": "just_compacted", "as_of_ms": ms,
                    "as_of_turn_ref": turn_ref}
        if event.get("type") != "assistant":
            continue
        message = event.get("message")
        usage = message.get("usage") if isinstance(message, dict) else None
        if not isinstance(usage, dict):
            continue
        inp = usage.get("input_tokens")
        if type(inp) is not int or inp < 0:
            continue
        cached = usage.get("cache_read_input_tokens") or 0
        created = usage.get("cache_creation_input_tokens") or 0
        if any(type(value) is not int or value < 0 for value in (cached, created)):
            continue
        total = inp + cached + created
        if total:
            return {"state": "known", "tokens_used": total,
                    "as_of_ms": ms, "as_of_turn_ref": turn_ref}
    return {"state": "pending", "tokens_used": None,
            "as_of_ms": settled_ms, "as_of_turn_ref": turn_ref}
_COMPACTION_UNKNOWN_REASONS = {"compacting", "just_compacted"}


def _usable_int(value) -> int | None:
    return value if isinstance(value, int) else None


def compaction_unknown_context(
    reason: str,
    *,
    as_of_ms: int | None = None,
) -> dict:
    """Explicitly fence off pre-compaction usage that is no longer current.

    Claude's compact boundary carries a local ``postTokens`` calculation, but
    neither it nor the statusline is an API-usage measurement for the newly
    compacted conversation. Keep ``tokens_used`` null until a later assistant
    response records a real post-boundary input footprint.
    """
    if reason not in _COMPACTION_UNKNOWN_REASONS:
        raise ValueError(f"unsupported compaction context reason: {reason}")
    return {
        "state": "unknown",
        "tokens_used": None,
        "reason": reason,
        "as_of_turn_ref": None,
        "as_of_ms": as_of_ms,
    }


def public_context(block: dict | None, *, engine: str,
                   compacting: bool = False) -> dict:
    """The one flat carried-context projection every ``context`` surface emits.

    ``block`` is the engine's own measurement: Claude's merged transcript /
    statusline block, or the Codex record's ``context``. Unknowns stay ``None``
    rather than guessed (Claude reports no window capacity). Whether a
    measurement exists is ``reason``, never ``state``, so ``state`` can keep
    meaning the worker lifecycle wherever this block is flattened in:

    - ``None``: ``tokens_used`` is a current measurement;
    - ``"pending"``: turns exist but none carried usable token data yet;
    - ``"compacting"`` / ``"just_compacted"``: pre-compact usage is fenced off;
    - ``"no-signal"``: there is no measurement at all.

    ``compacting`` is the Codex record's own ``activeCompact`` fence: its
    measurement predates a compaction still running, so it is withheld the way
    the nested ``peek`` / ``list`` block withholds it.
    """
    block = block if isinstance(block, dict) else None
    codex = engine == "codex"
    if compacting:
        reason = "compacting"
    elif block is None:
        reason = "no-signal"
    elif codex:
        reason = (block.get("reason") if block.get("reason") in _COMPACTION_UNKNOWN_REASONS
                  else None if block.get("tokens_used") is not None else "no-signal")
    elif block.get("state") == "pending":
        reason = "pending"
    elif block.get("state") == "unknown":
        reason = block.get("reason")
    else:
        reason = None
    block = block or {}
    return {
        "tokens_used": None if compacting or reason in _COMPACTION_UNKNOWN_REASONS else block.get("tokens_used"),
        "window_size": block.get("window_size") if codex else None,
        "percent_used": (block.get("percent_used") if codex and not compacting
                         and reason not in _COMPACTION_UNKNOWN_REASONS else None),
        "as_of_turn_ref": block.get("as_of_turn_ref"),
        "as_of_ms": block.get("as_of_ms"),
        "reason": reason,
    }


def settled_context(record: dict, *, engine: str) -> dict | None:
    """Only the owning process's turn-completion snapshot is a settlement."""
    last = record.get("lastSettledTurn" if engine == "claude" else "lastTurn")
    if not isinstance(last, dict) or not isinstance(last.get("context"), dict):
        return None
    block = dict(last["context"])
    if engine == "claude" and not _valid_claude_measurement(block):
        return None
    ref = (last.get("turnRef") if engine == "claude"
           else "ccx-turn:codex:v1:" + last["turnId"]
           if isinstance(last.get("turnId"), str) and last["turnId"] else None)
    if not ref:
        return None
    block["as_of_turn_ref"] = ref
    return block


def _valid_claude_measurement(block) -> bool:
    """Validate advisory usage independently of its strict source identity."""
    if (not isinstance(block, dict)
            or not isinstance(block.get("state"), str)
            or block["state"] not in {"known", "pending", "unknown"}):
        return False
    tokens = block.get("tokens_used")
    stamp = block.get("as_of_ms")
    return (not (tokens is not None and (type(tokens) is not int or tokens < 0))
            and not (stamp is not None and (type(stamp) is not int or stamp < 0))
            and (tokens is not None) == (block["state"] == "known"))


def current_context(record: dict, *, engine: str,
                    live: dict | None = None) -> dict | None:
    """A settled idle turn outranks a debounced newer *live* sidecar.

    On a running turn, the fresh live measurement wins. A newer compaction
    fence always wins; an unknown old record does not invent settlement.
    """
    settled = settled_context(record, engine=engine)
    lifecycle = record.get("lifecycle")
    lifecycle = lifecycle if isinstance(lifecycle, dict) else {}
    active = (lifecycle.get("activeTurnId") is not None if engine == "codex"
              else lifecycle.get("state") in {"busy", "waiting", "resuming"})
    if engine == "claude":
        # A compact fence is owned by the host, independently of advisory
        # statusline timestamps. A new statusline refresh can still contain
        # pre-compact tokens, so only a later genuinely settled assistant turn
        # is permitted to release the fence.
        fence = record.get("context")
        if (isinstance(fence, dict)
                and fence.get("reason") in _COMPACTION_UNKNOWN_REASONS):
            boundary = fence.get("as_of_ms")
            fresh = (isinstance(settled, dict)
                     and type(boundary) is int
                     and type(settled.get("as_of_ms")) is int
                     and settled["as_of_ms"] > boundary
                     and settled.get("tokens_used") is not None)
            newer_boundary = (
                isinstance(live, dict)
                and live.get("reason") in _COMPACTION_UNKNOWN_REASONS
                # The verified native compact boundary outranks a host-side
                # "compacting" latch even if the native timestamp is older
                # than the observation time of the pending local command.
                and (fence.get("reason") == "compacting"
                     and live.get("reason") == "just_compacted"
                     or type(live.get("as_of_ms")) is int
                     and (type(boundary) is not int or live["as_of_ms"] >= boundary)
                     and (fence.get("reason") != "just_compacted"
                          or live.get("reason") == "just_compacted"))
            )
            if not fresh and newer_boundary:
                return live
            if not fresh:
                return fence
    if engine == "codex" and isinstance(record.get("activeCompact"), dict):
        # Fence tokens, not the independently observed native window capacity.
        previous = live if isinstance(live, dict) else settled or {}
        return {**previous, "state": "unknown", "tokens_used": None,
                "reason": "compacting", "as_of_turn_ref": None}
    if engine == "codex":
        compact = record.get("lastCompact")
        if isinstance(compact, dict) and compact.get("status") == "succeeded":
            compact_ms = compact.get("completedAtMs")
            candidates = (settled, live, compact.get("contextAfter"))
            fresh = next((part for part in candidates
                          if isinstance(part, dict)
                          and type(part.get("as_of_ms")) is int
                          and type(compact_ms) is int
                          and part["as_of_ms"] > compact_ms
                          and part.get("tokens_used") is not None), None)
            if fresh is None and type(compact_ms) is int:
                previous = (compact.get("contextBefore") or {})
                return {**previous, "state": "unknown", "tokens_used": None,
                        "percent_used": None, "reason": "just_compacted",
                        "as_of_ms": compact_ms, "as_of_turn_ref": None}
    if isinstance(live, dict):
        live = {key: value for key, value in live.items()
                if key not in {"as_of_turn", "as_of_turn_id", "as_of_turn_ref"}}
        live["as_of_turn_ref"] = None
    if isinstance(live, dict) and live.get("reason") in _COMPACTION_UNKNOWN_REASONS:
        stamp = live.get("as_of_ms")
        previous = settled.get("as_of_ms") if settled else None
        if settled is None or type(stamp) is int and (type(previous) is not int or stamp > previous):
            return live
    if not active and settled is not None:
        return settled
    if not active:
        return live or settled
    # Once a turn is active, any valid live reading wins (its ref is null).
    # The only time comparison is the explicit post-settlement compact fence.
    return live if isinstance(live, dict) else settled


def read_claude_observation(store, owner, record) -> dict | None:
    """Read the small, exact-owner Claude context/model snapshot; never render."""
    from ccx.store.root import StorageIntegrityError

    if owner.engine != "claude":
        raise StorageIntegrityError("Claude context needs a Claude worker owner")
    doc = store.read_worker_sidecar(owner.worker, "context-observation.json")
    native = record.get("native")
    if doc is not None:
        source = doc.get("source")
        model = doc.get("observedModel")
        block = doc.get("context")
    if doc is not None and (doc.get("schemaVersion") != 1
            or doc.get("workerId") != owner.worker_id
            or not isinstance(native, dict)
            or doc.get("nativeSessionId") != native.get("sessionId")
            or doc.get("generation") != store.generation
            or not isinstance(source, dict)
            or any(type(source.get(key)) is not int or source[key] < 0
                   for key in ("dev", "ino", "endOffset", "observedAtMs"))
            or (model is not None and (not isinstance(model, str)
                                     or not 0 < len(model) <= 256
                                     or model == "<synthetic>"))):
        raise StorageIntegrityError("Claude observation identity/provenance mismatch")
    if doc is not None and block is not None and not _valid_claude_measurement(block):
        # Only the measurement is bad. Its owner/provenance was checked above;
        # keep its independent model and any valid recorded settlement.
        doc = {**doc, "context": None,
               "note": "invalid Claude context measurement discarded"}
    # The native statusline is advisory and expires quickly. Never let it
    # replace a genuine pending/just-compacted transcript fence.
    status = store.read_worker_sidecar(owner.worker, "statusline.json")
    if status is None or not isinstance(native, dict):
        chosen = current_context(record, engine="claude",
                                 live=doc.get("context") if doc else None)
        return {**(doc or {}), "context": chosen}
    now = int(time.time() * 1000)
    written = status.get("writtenAtMs")
    model_row = status.get("model")
    cw = status.get("contextWindow")
    if (status.get("schemaVersion") != 1
            or status.get("short") != owner.worker_id
            or status.get("sessionId") != native.get("sessionId")
            or type(written) is not int
            or written < now - STATUSLINE_CONTEXT_TTL_S * 1000
            or written > now + 5000):
        return {**(doc or {}), "context": current_context(
            record, engine="claude", live=doc.get("context") if doc else None)}
    live = statusline_context_from_sidecar(status, now_ms=now)
    if (live is not None and (not isinstance(cw, dict)
            or type(cw.get("total_input_tokens")) is not int
            or cw["total_input_tokens"] < 0)):
        live = None
    reported = model_row.get("id") if isinstance(model_row, dict) else None
    if (not isinstance(reported, str) or not 0 < len(reported) <= 256
            or reported == "<synthetic>"):
        reported = None
    combined = current_context(
        record, engine="claude",
        live=merge_context_usage(doc.get("context") if doc else None, live))
    model = doc.get("observedModel") if doc else None
    if combined is None and model is None and reported is None:
        return {**(doc or {}), "context": current_context(
            record, engine="claude", live=doc.get("context") if doc else None)}
    return {**(doc or {}), "context": combined,
            "observedModel": model or reported}


def compute_context_usage(turns, prefer_model: str | None = None) -> dict | None:
    """Carried context as of the latest trustworthy context anchor.

    Scans ``turns`` (cleaned-transcript shape, newest last) backwards. A valid
    assistant usage block supplies the current carried input footprint. A newer
    compact boundary instead returns ``unknown / just_compacted``: usage from
    before that boundary is stale, while Claude's statusline has no reliable
    post-compact measurement until another model response runs.

    ``prefer_model`` is retained for caller compatibility but intentionally
    unused: the public context shape no longer guesses model-window capacity.

    Returns None when there are no turns at all (no signal). Returns a block with
    ``state: "pending"`` when turns exist but none carry usable token data yet
    (e.g. a freshly-launched worker before its first completed turn).
    """
    turns = turns or []
    if not turns:
        return None

    found = None
    for turn in reversed(turns):
        if turn.get("role") == "system" and turn.get("kind") == "compact_boundary":
            return compaction_unknown_context(
                "just_compacted",
                as_of_ms=turn.get("ts"),
            )
        if turn.get("role") != "assistant":
            continue
        tokens = turn.get("tokens")
        if not isinstance(tokens, dict):
            continue
        inp = _usable_int(tokens.get("input"))
        if inp is None:
            continue
        # Skip degenerate all-zero turns (rate-limit / API-error synthetic replies
        # written with model "<synthetic>" and every counter zeroed out).
        cache_read = _usable_int(tokens.get("cache_read_input")) or 0
        cache_creation = _usable_int(tokens.get("cache_creation_input")) or 0
        if inp + cache_read + cache_creation == 0:
            continue
        found = turn
        break

    if found is None:
        return {
            "state": "pending",
            "tokens_used": None,
            "as_of_turn_ref": None,
            "as_of_ms": None,
        }

    tokens = found["tokens"]
    inp = _usable_int(tokens.get("input")) or 0
    cache_read = _usable_int(tokens.get("cache_read_input")) or 0
    cache_creation = _usable_int(tokens.get("cache_creation_input")) or 0
    tokens_used = inp + cache_read + cache_creation

    return {
        "state": "known",
        "tokens_used": tokens_used,
        "as_of_turn_ref": None,
        "as_of_ms": found.get("ts"),
    }


def statusline_context_from_sidecar(doc: dict, *, now_ms: int | None = None,
                                    mtime_ms: int | None = None,
                                    ttl_s: int = STATUSLINE_CONTEXT_TTL_S) -> dict | None:
    """Normalize a statusline sidecar into the public context block shape.

    Returns ``None`` when the sidecar is stale or doesn't contain a usable
    ``contextWindow.total_input_tokens``. That keeps the bridge an optional
    live overlay: callers can always fall back to transcript-derived context.
    """
    if not isinstance(doc, dict):
        return None
    written = _usable_int(doc.get("writtenAtMs"))
    freshness_ms = mtime_ms if mtime_ms is not None else written
    if freshness_ms is not None:
        now = now_ms if now_ms is not None else int(time.time() * 1000)
        if now - freshness_ms > ttl_s * 1000:
            return None

    cw = doc.get("contextWindow")
    if not isinstance(cw, dict):
        return None
    tokens_used = _usable_int(cw.get("total_input_tokens"))
    if tokens_used is None:
        return None

    return {
        "state": "known",
        "tokens_used": tokens_used,
        "as_of_turn_ref": None,
        "as_of_ms": written,
    }


def merge_context_usage(transcript_ctx: dict | None, statusline_ctx: dict | None) -> dict | None:
    """Prefer live statusline usage unless a compaction fence makes it unsafe."""
    if (
        isinstance(transcript_ctx, dict)
        and transcript_ctx.get("state") == "unknown"
        and transcript_ctx.get("reason") in _COMPACTION_UNKNOWN_REASONS
    ):
        return transcript_ctx
    if isinstance(statusline_ctx, dict):
        transcript_ms = (
            _usable_int(transcript_ctx.get("as_of_ms"))
            if isinstance(transcript_ctx, dict)
            else None
        )
        statusline_ms = _usable_int(statusline_ctx.get("as_of_ms"))
        if (
            transcript_ms is not None
            and statusline_ms is not None
            and statusline_ms < transcript_ms
        ):
            return transcript_ctx
        return statusline_ctx
    return transcript_ctx
