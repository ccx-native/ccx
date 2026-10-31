from __future__ import annotations
import hashlib
import json
import os
import time
from typing import Any
from ccx.codex import ops as codex_ops
from ccx.claude import telemetry as claude_telemetry
from ccx.errors import (
    DeliveryRejected,
    InvalidRequest,
    SessionUnavailable,
    worker_not_found,
)
from ccx.workers import WorkerClient
from ccx.store.identity import canonical_worker_id
from ccx.ops.common import _DEFAULT_REPORT_CHARS, _DEFAULT_TRANSCRIPT_PAGE, _DEFAULT_TRANSCRIPT_TOOL_BYTES, _ENGINES, _TRANSCRIPT_CURSOR_PREFIX, _TRANSCRIPT_LOCATOR_PREFIX, _WORKERS, _decode_opaque, _encode_opaque, recorded_inspector, split_target


def _latest_native_report(
    handle: str, engine: str, worker_id: str, *, deadline: float,
    max_chars: int | None,
) -> dict[str, Any]:
    """Latest native turn's reply, with notes about unfinished or failed work.

    Exact turn_ref reports and the send/new wait paths do not enter here.
    In particular a phase-less/latest cached message is not completion proof.
    """
    from ccx.claude import delivery as claude_delivery
    from ccx.codex import delivery as codex_delivery
    from ccx import constants
    from ccx.store.root import StorageIntegrityError, NativeActionDenied, require_ccx_active

    store = require_ccx_active(constants.CCX_ROOT)
    owner = store.resolve(handle)
    record = store.record(handle)
    lifecycle = record.get("lifecycle") or {}
    state = lifecycle.get("state")
    failed = state in {"crashed", "failed"}
    running = not failed and bool(
        lifecycle.get("activeTurnId") or lifecycle.get("activeTurnRef")
        or state in {"busy", "waiting", "booting", "resuming", "starting"})
    note = "the worker failed" if failed else None
    # A turn still open when the worker failed has no final reply: its last
    # text is partial (e.g. commentary before a tool call).
    unfinished = failed and bool(
        lifecycle.get("activeTurnId") or lifecycle.get("activeTurnRef"))
    running_without_receipt = False

    receipts = list((codex_delivery if engine == "codex" else
                     claude_delivery).iter_receipts(handle))
    # A never-started original, without any recorded native turn/effect,
    # cannot have a completed conversation. Avoid unsupported Codex listing.
    never_started = (not receipts and not record.get("lastTurn")
                     and not record.get("lastSettledTurn")
                     and not record.get("lastReport")
                     and not lifecycle.get("activeTurnId")
                     and not lifecycle.get("activeTurnRef")
                     and (record.get("stats") or {}).get("turns", 0) == 0)
    if never_started and record.get("historyKnownEmpty") is True:
        raise SessionUnavailable("no conversation turn yet")

    def settled(ref: str | None) -> dict[str, Any] | None:
        if not ref:
            return None
        matched = [r for r in receipts if r.get("turnRef") == ref
                   and isinstance(r.get("settlement"), dict)
                   and (engine != "codex" or codex_delivery.route(r) == "new_turn")]
        return matched[0] if len(matched) == 1 else None

    running_note = ("a newer turn is waiting for input (see ccx peek)"
                    if state == "waiting" or record.get("waitingFor")
                    else "a newer turn is still running")
    if running:
        # A current turn may already have streamed partial assistant output.
        # Only the last completed, receipt-proven turn is safe to show.
        if engine == "codex":
            last = record.get("lastTurn") or {}
            ref = ("ccx-turn:codex:v1:" + last["turnId"]
                   if last.get("status") == "completed"
                   and isinstance(last.get("turnId"), str) else None)
        else:
            last = record.get("lastSettledTurn") or {}
            ref = (last.get("turnRef") if last.get("outcome") not in
                   {"interrupted", "failed", "cancelled"} else None)
        confirmed = settled(ref)
        if confirmed is None:
            if not ref:
                raise SessionUnavailable("no completed reply yet; a turn is still running")
            running_without_receipt = True
            note = running_note
        else:
            result = report_worker(handle, turn_ref=confirmed["turnRef"],
                                   timeout=max(0.001, deadline - time.monotonic()),
                                   max_chars=max_chars)
            result.setdefault("outcome", None)
            result["note"] = running_note
            return result

    latest = None
    chosen = None
    latest_ref = None
    latest_status = "completed"
    if engine == "codex":
        last = record.get("lastTurn") or {}
        if last.get("status") == "completed" and isinstance(last.get("turnId"), str):
            latest_ref = "ccx-turn:codex:v1:" + last["turnId"]
            chosen = settled(latest_ref)
            if chosen is None:
                # An older codexd may have journaled the native completion
                # before this process learned to settle the accepted receipt.
                # Repair only the one exact opening delivery, never a steer
                # or an unrelated older accepted turn. No thread/read here.
                pending = [r for r in receipts
                           if r.get("turnRef") == latest_ref
                           and r.get("turnId") == last["turnId"]
                           and r.get("state") == "accepted"
                           and r.get("settlement") is None
                           and codex_delivery.route(r) == "new_turn"]
                if len(pending) == 1:
                    from ccx.codex.completion import settle_from_journal
                    settle_from_journal(store, handle, last["turnId"])
                    receipts = list(codex_delivery.iter_receipts(handle))
                    chosen = settled(latest_ref)
            if chosen is not None:
                # lastTurn can still name the completed turn after another
                # message was submitted and then killed or lost.
                chosen_at = chosen.get("createdAtMs")
                if type(chosen_at) is int and any(
                    r.get("state") in {"submitted", "accepted"}
                    and r.get("settlement") is None
                    and codex_delivery.route(r) == "new_turn"
                    and type(r.get("createdAtMs")) is int
                    and r["createdAtMs"] > chosen_at
                    for r in receipts
                ):
                    note = "a newer message's outcome is unknown"
        if chosen is None:
            from ccx.codex.runtime import read_thread
            from ccx.codex.reports import report_from_turn, native_timestamp_ms
            native = record.get("native") or {}
            thread_id = native.get("threadId")
            try:
                source = store.codex_source(handle)
            except (StorageIntegrityError, SessionUnavailable, NativeActionDenied) as exc:
                if never_started:
                    raise SessionUnavailable("no conversation turn yet") from exc
                raise
            if (not isinstance(thread_id, str) or not thread_id
                    or native.get("threadSource") != source
                    or not isinstance(record.get("cwd"), str)):
                if never_started:
                    raise SessionUnavailable("no conversation turn yet")
                raise SessionUnavailable("Codex conversation history is unavailable; check the worker and retry")
            try:
                thread = read_thread(
                    thread_id, cwd=record["cwd"], worker_id=worker_id,
                    expected_source=source,
                    timeout=max(0.001, deadline - time.monotonic()),
                )
            except SessionUnavailable as exc:
                # The native Codex API does not universally implement empty
                # thread/items/list. Only positively known empty histories
                # warrant this more useful diagnosis.
                if record.get("historyKnownEmpty") is True:
                    raise SessionUnavailable("no conversation turn yet") from exc
                # A receipt proves its own settled reply even when Codex's
                # thread history temporarily cannot be read. Do not imply
                # this is necessarily the latest native conversation turn.
                proven = [
                    r for r in receipts
                    if isinstance(r.get("settlement"), dict)
                    and codex_delivery.route(r) == "new_turn"
                    and type(r.get("createdAtMs")) is int
                ]
                if not proven:
                    raise
                chosen = max(proven, key=lambda r: r["createdAtMs"])
                result = report_worker(
                    handle, turn_ref=chosen["turnRef"],
                    timeout=max(0.001, deadline - time.monotonic()),
                    max_chars=max_chars,
                )
                result.setdefault("outcome", None)
                result["note"] = "native history unavailable; showing the last recorded reply"
                return result
            if (not isinstance(thread, dict)
                    or thread.get("id") != thread_id
                    or thread.get("threadSource") != source):
                raise SessionUnavailable("Codex report history does not match this worker")
            native_turns = [t for t in (thread.get("turns") or [])
                            if isinstance(t, dict)]
            compact_id = (record.get("lastCompact") or {}).get("turnId")
            # Only skip positively identified internal compaction turns.
            # No heuristic based on blank text or missing assistant output.
            while native_turns and (
                (isinstance(compact_id, str)
                 and native_turns[-1].get("id") == compact_id)
                or any(
                    isinstance(item, dict) and item.get("type") == "contextCompaction"
                    for item in (native_turns[-1].get("items") or [])
                )
            ) and not (
                any(
                    isinstance(item, dict) and item.get("type") == "userMessage"
                    for item in (native_turns[-1].get("items") or [])
                )
                or report_from_turn(native_turns[-1]) is not None
            ):
                # Automatic compaction inside a genuine user/reply turn is
                # not an internal-only turn; never discard its final answer.
                native_turns.pop()
            if running_without_receipt:
                while native_turns and (
                    native_turns[-1].get("status") != "completed"
                    or report_from_turn(native_turns[-1]) is None
                ):
                    native_turns.pop()
                if not native_turns:
                    raise SessionUnavailable("no completed reply yet; a turn is still running")
            if native_turns and native_turns[-1].get("status") in {
                "inProgress", "in_progress", "pending",
            }:
                raise DeliveryRejected(
                    "latest Codex turn is busy; no completed report is available",
                    short=worker_id[:8], state="busy", refusal="busy",
                )
            # The LAST native turn decides the outcome. A later failed or
            # interrupted turn must never borrow an older successful answer.
            if not native_turns:
                raise SessionUnavailable("no conversation turn yet")
            turn = native_turns[-1]
            status = turn.get("status")
            if status not in {"completed", "failed", "interrupted", "cancelled"}:
                raise SessionUnavailable("latest Codex turn has no terminal outcome")
            tid = turn.get("id")
            if not isinstance(tid, str) or not tid:
                raise SessionUnavailable("latest Codex terminal turn has no native identity")
            latest_ref = "ccx-turn:codex:v1:" + tid
            chosen = settled(latest_ref) if status == "completed" else None
            selected = report_from_turn(turn) if status == "completed" else None
            latest = {
                "turn_ref": latest_ref, "turn_id": tid, "status": status,
                "phase": selected.get("phase") if selected else None,
                "item_id": selected.get("itemId") if selected else None,
                "completed_at": (selected.get("completedAt") if selected else
                                 native_timestamp_ms(turn.get("completedAt"))),
                "text": selected["text"] if selected else "",
                "has_final_message": selected is not None,
            }
    else:
        from ccx.ops.conversation_paging import _reverse_boundary_events
        from ccx.claude.managed_turns import report_window_for_turn_ref
        from ccx.claude.transcript import _iter_clean_turns, open_original_claude_source
        last = record.get("lastSettledTurn") or {}
        ref = last.get("turnRef")
        candidate = settled(ref)
        try:
            with open_original_claude_source(store, owner, record) as source:
                if source is not None:
                    pinned, _stat, eof, _path, _parent = source
                    # A reverse read over the same pinned native fd finds the
                    # last genuine user boundary without decoding months of
                    # prior JSONL on every completion notification. Ignore
                    # Claude's synthetic auto-continue as the cleaner does.
                    ordinary_boundaries: list[int] = []
                    report_end = eof
                    if running_without_receipt and ref:
                        window, available = report_window_for_turn_ref(worker_id, ref)
                        end = window.get("endOffset") if available and window else None
                        if (type(end) is int and 0 <= end <= eof
                                and window.get("endOffsetKnown") is True):
                            report_end = end
                    if running_without_receipt and report_end == eof:
                        active_ref = lifecycle.get("activeTurnRef")
                        if isinstance(active_ref, str) and active_ref:
                            window, available = report_window_for_turn_ref(worker_id, active_ref)
                            start = window.get("startOffset") if available and window else None
                            if type(start) is int and 0 <= start <= eof:
                                report_end = start
                    for event in _reverse_boundary_events(pinned, before_offset=report_end):
                        content = (event.get("message") or {}).get("content")
                        if isinstance(content, list):
                            content = "\n".join(
                                block.get("text", "") for block in content
                                if isinstance(block, dict) and block.get("type") == "text"
                            )
                        if content == "Continue from where you left off.":
                            continue
                        if (not ordinary_boundaries and isinstance(content, str)
                                and (content.strip() == "/compact"
                                     or "<command-name>/compact</command-name>" in content)):
                            # Claude's local /compact and its command echo
                            # are not a conversation turn. Ignore the entire
                            # trailing command when selecting a reply.
                            report_end = min(report_end, event["_ccx_start_offset"])
                            continue
                        ordinary_boundaries.append(event["_ccx_start_offset"])
                        # A second boundary is needed only for an existing
                        # receipt: without the successor's managed start event,
                        # its endOffset is unknown and the latest turn may be
                        # an entirely different user request after that receipt.
                        if len(ordinary_boundaries) >= (2 if candidate else 1):
                            break
                    start_offset = ordinary_boundaries[0] if ordinary_boundaries else 0
                    text = ""
                    seen = False
                    for part in _iter_clean_turns(
                        pinned, after_offset=start_offset, before_offset=report_end,
                    ):
                        if part.get("role") == "user" and not part.get("queued"):
                            text = ""
                            seen = True
                        elif part.get("role") == "assistant":
                            seen = True
                            answer = part.get("text")
                            if isinstance(answer, str) and answer.strip():
                                text = answer.strip()
                    if seen:
                        latest = {"text": text, "has_final_message": bool(text)}
                    # The durable lastSettledTurn outcome belongs to this
                    # native answer only when its managed start/end offsets
                    # enclose the latest ordinary user boundary. An older
                    # receipt or interrupted terminal cannot be borrowed.
                    if ref and (candidate is not None or last.get("outcome") == "interrupted"):
                        window, available = report_window_for_turn_ref(worker_id, ref)
                        start = window.get("startOffset") if available and window else None
                        end = window.get("endOffset") if available and window else None
                        if (type(start) is int and start >= 0
                                and window.get("endOffsetKnown") is True
                                and window.get("settled") is True
                                and ordinary_boundaries
                                and start <= start_offset
                                and (end is None or start_offset < end)
                                and (len(ordinary_boundaries) == 1
                                     or ordinary_boundaries[1] < start)):
                            if last.get("outcome") == "interrupted":
                                latest_status = "interrupted"
                            else:
                                chosen = candidate
        except (StorageIntegrityError, OSError, ValueError) as exc:
            if never_started:
                raise SessionUnavailable("no conversation turn yet") from exc
            raise SessionUnavailable("Claude conversation history is unavailable; check the worker and retry") from exc
        if latest is None and record.get("historyKnownEmpty") is True:
            raise SessionUnavailable("no conversation turn yet")
        if latest is None:
            # A synthetic/legacy source can be absent even though a single
            # original receipt has already been positively proved. Preserve
            # that strict receipt path unless a newer recorded terminal names
            # a different turn. Never fabricate native transcript evidence.
            proven = [r for r in receipts if isinstance(r.get("settlement"), dict)]
            if candidate is not None:
                chosen = candidate
            elif not ref and len(proven) == 1:
                chosen = proven[0]

    if chosen is not None:
        # Invoke precisely the old receipt-backed path, with its original
        # effect reconciliation and strict identity checks intact.
        result = report_worker(handle, turn_ref=chosen["turnRef"],
                               timeout=max(0.001, deadline - time.monotonic()),
                               max_chars=max_chars)
        result.setdefault("outcome", None)
        result["note"] = note
        return result
    if latest is None:
        raise SessionUnavailable("no conversation turn yet")
    full_text = latest["text"]
    store.revalidate()
    return {
        "worker": handle, "worker_id": worker_id, "engine": engine,
        "short": worker_id[:8], "delivery_id": None, "delivery": None,
        "native_evidence_sha256": None,
        "turn_ref": latest.get("turn_ref"), "turn_id": latest.get("turn_id"),
        "turn": None, "item_id": latest.get("item_id"),
        "phase": latest.get("phase"),
        "status": "failed" if unfinished else latest.get("status", latest_status),
        # For Claude, the exact lastSettledTurn interrupted verdict is bound
        # above to this latest original user boundary. Keep its JSON outcome
        # alongside status; never relabel a different native turn.
        "outcome": (latest.get("status") if latest.get("status") in
                    {"interrupted", "cancelled"} else
                    latest_status if latest_status in {"interrupted", "cancelled"} else None),
        "text": full_text if max_chars is None else full_text[:max_chars],
        "truncated": max_chars is not None and len(full_text) > max_chars,
        "has_final_message": latest["has_final_message"] and not unfinished,
        "settled_at_ms": latest.get("completed_at"),
        "source": "native-transcript-latest",
        "note": note,
    }


def report_worker(
    target: str, *, timeout: float = 90,
    max_chars: int | None = _DEFAULT_REPORT_CHARS,
    turn_ref: str | None = None,
) -> dict[str, Any]:
    """Default latest turn, or one exact source-proven original turn_ref."""
    from ccx.ops.wait import reconcile_turn
    from ccx.claude import delivery as claude_delivery
    from ccx import constants
    from ccx.store.root import require_ccx_active
    if not isinstance(timeout, (int, float)) or timeout <= 0:
        raise InvalidRequest("report timeout must be positive")
    deadline = time.monotonic() + timeout
    if max_chars is not None and (
            type(max_chars) is not int or max_chars < 0):
        raise InvalidRequest("report max_chars must be nonnegative integer")
    handle, engine, worker_id = split_target(target)
    if turn_ref is None:
        return _latest_native_report(
            handle, engine, worker_id, deadline=deadline, max_chars=max_chars,
        )
    from ccx.codex import delivery as codex_delivery
    receipts = list(codex_delivery.iter_receipts(handle)
                    if engine == "codex" else claude_delivery.iter_receipts(handle))
    from ccx.ops import turn_reports
    native_turn = turn_reports._ref_id(turn_ref, engine=engine)
    has_receipt = any(
        row.get("turnRef") == turn_ref
        or (engine == "claude" and row.get("targetTurnRef") == turn_ref)
        or (engine == "codex" and native_turn in (
            row.get("turnId"), row.get("targetTurnId")
        ))
        for row in receipts
    )
    if not has_receipt:
        # No original semantic delivery of any state names this R. A
        # matching pending or uncertain token remains receipt-authoritative.
        store = require_ccx_active(constants.CCX_ROOT)
        owner = store.resolve(handle)
        if engine == "codex":
            from ccx.codex.runtime import read_thread
            record = store.record(handle)
            native_id = (record.get("native") or {}).get("threadId")
            if not isinstance(native_id, str) or not native_id:
                raise SessionUnavailable("Codex conversation is missing; inspect the worker")
            if not isinstance(record.get("cwd"), str):
                raise SessionUnavailable("Codex working directory is missing; inspect the worker")
            thread = read_thread(
                native_id, cwd=record["cwd"], worker_id=owner.worker_id,
                expected_source=store.codex_source(owner.worker),
                timeout=max(0.001, deadline - time.monotonic()),
            )
        else:
            thread = None
        exact = turn_reports.exact_report(store, owner, turn_ref, thread=thread)
        full_text = exact["text"]
        text = full_text if max_chars is None else full_text[:max_chars]
        return {
            "worker": handle, "worker_id": worker_id, "engine": engine,
            "short": worker_id[:8], "delivery_id": None,
            "delivery": None, "native_evidence_sha256": None,
            "turn_ref": exact["turn_ref"], "turn_id": exact.get("turn_id"),
            "turn": exact.get("turn"), "item_id": exact.get("item_id"),
            "phase": exact.get("phase"), "status": "completed", "outcome": None,
            "text": text, "truncated": len(text) != len(full_text),
            "has_final_message": exact["has_final_message"],
            "settled_at_ms": exact.get("completed_at"),
            "source": "original-native-exact-turn",
        }
    if engine == "codex":
        # Report observes the native original Codex thread/turn without any
        # new provider mutation. Each receipt remains the only owner token.
        for candidate in receipts:
            if (candidate.get("state") == "accepted"
                    and not candidate.get("settlement")
                    and candidate.get("turnRef") == turn_ref):
                reconcile_turn(
                    handle, delivery_id=candidate["deliveryId"],
                    timeout=max(0.001, deadline - time.monotonic()),
                )
        rows = [
            row for row in codex_delivery.iter_receipts(handle)
            if isinstance(row.get("settlement"), dict)
            and row.get("turnRef") == turn_ref
        ]
        if not rows:
            raise SessionUnavailable("Codex turn is not confirmed complete; try again later")
        rows.sort(key=lambda row: (
            -(row.get("settledAtMs") or 0), row.get("deliveryId") or ""))
        row = rows[0]
        actual = row["settlement"]
        if (actual.get("workerId") != worker_id
                or actual.get("turnId") != row.get("turnId")
                or actual.get("status") != "completed"):
            raise SessionUnavailable("Codex completion could not be verified; inspect the turn")
        raw = actual["text"]
        text = raw if max_chars is None else raw[:max_chars]
        return {
            "worker": handle, "worker_id": worker_id, "engine": "codex",
            "short": worker_id[:8], "delivery_id": row["deliveryId"],
            "turn_id": row["turnId"], "turn_ref": row["turnRef"],
            "phase": "completed", "status": "completed", "outcome": None,
            "text": text, "truncated": len(text) != len(raw),
            "has_final_message": bool(raw),
            "settled_at_ms": row.get("settledAtMs"),
            "native_evidence_sha256": actual.get("nativeEvidenceSha256"),
            "delivery": codex_delivery.public_receipt(row),
            "source": "original-native-completion-receipt",
        }
    store = require_ccx_active(constants.CCX_ROOT)
    matches = [
        row for row in receipts
        if isinstance(row.get("settlement"), dict)
        and row.get("turnRef") == turn_ref
    ]
    if not matches:
        raise SessionUnavailable(
            "No verified completed reply is available for this turn"
        )
    row = matches[0]
    # Host publication is a pair of fsynced effects: first the original
    # semantic Stop receipt, then the global native-effect accepted receipt.
    # A report racing those two writes (or arriving after a host crash) must
    # forward-reconcile the same original Stop source, not return a false
    # success while the globally durable native effect remains unknown.
    verified = reconcile_turn(handle, delivery_id=row["deliveryId"])
    if verified["status"] != "settled":
        raise SessionUnavailable(
            "Claude turn is not confirmed complete; reconcile its delivery"
        )
    settlement = row["settlement"]
    if (settlement.get("workerId") != worker_id
            or settlement.get("turnRef") != row.get("turnRef")
            or settlement.get("status") != "completed"):
        raise SessionUnavailable("Claude completion could not be verified; inspect the delivery")
    full_text = settlement["text"]
    text = full_text if max_chars is None else full_text[:max_chars]
    return {
        "worker": handle, "worker_id": worker_id, "engine": "claude",
        "short": worker_id[:8], "delivery_id": row["deliveryId"],
        "turn_ref": row["turnRef"],
        "phase": "completed", "status": "completed", "outcome": None,
        "text": text, "truncated": len(text) != len(full_text),
        "has_final_message": bool(full_text),
        "settled_at_ms": row.get("settledAtMs"),
        "native_evidence_sha256": settlement.get("nativeEvidenceSha256"),
        "delivery": claude_delivery.public_receipt(row),
        "source": "original-native-completion-receipt",
    }


def account_usage_read(
    *,
    engine: str,
    timeout: float = 30,
) -> dict[str, Any]:
    if engine == "codex":
        return codex_ops.account_usage_read(timeout=timeout)
    if engine != "claude":
        raise InvalidRequest(f"unsupported engine: {engine!r}")
    result = _WORKERS.usage("claude", timeout=timeout)
    windows = []
    for window in result.get("windows") or []:
        used = window.get("used_percent")
        windows.append({
            "provider": "claude",
            "window_id": window.get("id"),
            "limit_id": window.get("id"),
            "kind": window.get("label") or "quota",
            "window_duration_seconds": None,
            "used_percent": used,
            "remaining_percent": (
                max(0.0, 100.0 - float(used))
                if isinstance(used, (int, float))
                and not isinstance(used, bool)
                else None
            ),
            "resets_at": window.get("resets_at"),
        })
    return {
        "provider": "claude",
        "observed_at_ms": int(time.time() * 1000),
        "windows": windows,
    }


def _conversation_revision(document: dict[str, Any]) -> str:
    # This revision fences the paged conversation CONTENT, not the read operation.
    # generatedAt, live state and top-level coverage/evidence metadata can change
    # while the ordered normalized turns are byte-for-byte identical.
    revision_source = {
        "schemaVersion": document.get("schemaVersion"),
        "turns": document.get("turns") or [],
    }
    raw = json.dumps(
        revision_source,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def read_transcript(
    locator: str,
    *,
    cursor: str | None = None,
    limit: int = _DEFAULT_TRANSCRIPT_PAGE,
    max_tool_result_bytes: int | None = _DEFAULT_TRANSCRIPT_TOOL_BYTES,
    timeout: float = 120,
) -> dict[str, Any]:
    """Read normalized turns from the original owned full-UUID conversation."""
    from ccx.ops import conversation as normalized
    from ccx.errors import CursorInvalidated, OperationTimeout
    from ccx.store.identity import IdentityError, UnknownWorker

    # Independent from the source-record ccxcp2 pager and its cursor semantics.
    payload = _decode_opaque(_TRANSCRIPT_LOCATOR_PREFIX, locator,
                             label="transcript locator")
    expected = {"v", "engine", "worker_id", "root", "generation"}
    if (set(payload) != expected or type(payload["v"]) is not int
            or payload["v"] != 2
            or not isinstance(payload["engine"], str)
            or payload["engine"] not in _ENGINES
            or not isinstance(payload["root"], str)
            or len(payload["root"]) != 64
            or any(ch not in "0123456789abcdef" for ch in payload["root"])
            or not isinstance(payload["generation"], str)
            or not payload["generation"]):
        raise InvalidRequest("invalid transcript locator")
    try:
        canonical_worker_id(payload["worker_id"])
    except IdentityError as error:
        raise InvalidRequest("invalid transcript locator worker") from error

    offset = 0
    prior_revision = None
    if cursor is not None:
        token = _decode_opaque(_TRANSCRIPT_CURSOR_PREFIX, cursor,
                               label="transcript cursor")
        if (set(token) != expected | {"offset", "revision"}
                or type(token["v"]) is not int or token["v"] != 2
                or type(token["offset"]) is not int or token["offset"] < 0
                or not isinstance(token["revision"], str)
                or len(token["revision"]) != 64
                or any(ch not in "0123456789abcdef" for ch in token["revision"])
                or not isinstance(token["generation"], str)
                or not token["generation"]):
            raise InvalidRequest("invalid transcript cursor")
        if any(token[key] != payload[key] for key in
               ("engine", "worker_id", "root")):
            raise InvalidRequest("transcript cursor belongs to another source")
        offset, prior_revision = token["offset"], token["revision"]
    if type(limit) is not int or not 1 <= limit <= 100:
        raise InvalidRequest("transcript limit must be between 1 and 100")
    normalized._requested_label(max_tool_result_bytes)
    if (isinstance(timeout, bool) or not isinstance(timeout, (int, float))
            or not 0 < timeout < float("inf")):
        raise InvalidRequest("transcript timeout must be positive and finite")
    deadline = time.monotonic() + timeout

    client = WorkerClient()
    store = client._uuid_store
    root = hashlib.sha256(os.fsencode(store.root)).hexdigest()
    if payload["root"] != root:
        raise InvalidRequest("transcript locator belongs to another root")
    if (payload["generation"] != store.generation
            or (cursor is not None and token["generation"] != store.generation)):
        raise CursorInvalidated(
            "transcript generation changed; restart with a new locator"
        )
    try:
        owner = store.resolve(f"{payload['engine']}:{payload['worker_id']}")
    except UnknownWorker as error:
        raise worker_not_found(f"{payload['engine']}:{payload['worker_id']}") from error
    except IdentityError as error:
        raise InvalidRequest("transcript locator engine does not own worker") from error

    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise OperationTimeout("transcript read deadline expired")
    document = client.conversation(
        owner.worker, max_tool_result_bytes=max_tool_result_bytes,
        timeout=remaining,
    )
    revision = _conversation_revision(document)
    turns = list(document.get("turns") or [])
    page = turns[offset:offset + limit]
    next_offset = offset + len(page)
    has_more = next_offset < len(turns)
    next_cursor = (
        _encode_opaque(
            _TRANSCRIPT_CURSOR_PREFIX,
            {**payload, "offset": next_offset, "revision": revision},
        )
        if has_more
        else None
    )
    # Reuse the existing protocol recorded-only snapshot; never restore live inspect.
    inspected = recorded_inspector(store, owner.worker)
    store.revalidate()
    if time.monotonic() >= deadline:
        raise OperationTimeout("transcript read deadline expired")
    return {
        "worker": inspected,
        "transcript_locator": locator,
        "conversation_schema_version": document.get("schemaVersion"),
        "turns": page,
        "completeness": document.get("completeness"),
        "evidence": document.get("evidence"),
        "source_revision": revision,
        "source_changed": (
            prior_revision is not None and prior_revision != revision
        ),
        "next_cursor": next_cursor,
        "has_more": has_more,
    }


def conversation_page(
    target: str,
    *,
    cursor: str | None = None,
    source_record_target: int = 40,
    max_tool_result_bytes: int | None = 16_384,
    timeout: float = 30,
) -> dict[str, Any]:
    return WorkerClient().conversation_page(
        target,
        cursor=cursor,
        source_record_target=source_record_target,
        max_tool_result_bytes=max_tool_result_bytes,
        timeout=timeout,
    )


def telemetry_worker(
    target: str,
    *,
    after_cursor: str | None = None,
    limit: int = 200,
    timeout: float = 120,
) -> dict[str, Any]:
    """Original full-UUID source-bound Claude/Codex telemetry read."""
    from ccx import constants
    from ccx.store.root import require_ccx_active, GenerationError
    from ccx.claude.transcript import open_original_claude_source

    store = require_ccx_active(constants.CCX_ROOT)
    owner = store.resolve(target)
    if owner.engine == "codex":
        result = codex_ops.telemetry_worker(
            owner.worker, after_cursor=after_cursor,
            limit=limit, timeout=timeout,
        )
        store.revalidate()
        return result
    rec = store.record(owner.worker)
    sid = (rec.get("native") or {}).get("sessionId")
    try:
        with open_original_claude_source(store, owner, rec) as original:
            pinned = original[0] if original is not None else ""
            end_offset = original[2] if original is not None else 0
            view = {**rec, "short": owner.short, "sessionId": sid,
                    "transcript": pinned}
            result = claude_telemetry.read_telemetry(
                owner.short, after_cursor=after_cursor, limit=limit,
                record=view, cursor_worker_id=owner.worker_id,
                before_offset=end_offset, source_missing=original is None,
            )
        store.revalidate()
        return result
    except (GenerationError, OSError) as error:
        raise SessionUnavailable(
            f"Cannot read Claude activity; check the worker: {error}",
            short=owner.short,
        ) from error
