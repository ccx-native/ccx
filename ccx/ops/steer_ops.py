'Durable delivery journal for managed Claude steering (``ccx steer``).\n\nSteering a BUSY worker is the one CCX write whose delivery cannot be proven\nsynchronously: the prompt is pasted into Ink, and Claude decides — later, and\nin its own journal — whether it joins the running turn or the next one. The\nprompt must not be re-pasted merely because native evidence is delayed.\n\nThis module makes the prompt durable *before* it is ever written to the PTY,\nand makes every later decision evidence-driven:\n\n* the full pending prompt and its transcript byte fence are journaled\n  atomically before the host acknowledges ownership;\n* retrying the same ``delivery_id`` returns the original entry and never\n  produces a second paste;\n* placement is resolved from native Claude evidence fenced by transcript byte\n  offset (see :func:`ccx.claude.transcript.steer_evidence_events`), never from\n  timestamps or whole-file text containment;\n* when evidence is inconclusive the entry stays honestly pending, and a\n  delivery that was pasted with no native trace at all eventually becomes\n  ``unknown`` rather than being re-sent.\n\nThe journal is a bounded per-session sidecar (``<short>.deliveries.json``) for\nthe same reason the compact journal is: the session record has several\nindependent writers and a stale read-modify-write must never erase a terminal\ndelivery result. Those writers live in different processes, so every mutation\nholds an exclusive ``flock`` on ``<short>.deliveries.lock`` across its whole\nread -> mutate -> atomic-replace cycle (see :func:`journal_lock`). Reads stay\nlock-free: publication is one ``os.replace``, so a reader never sees a torn\njournal.\n'

from __future__ import annotations

import json
import os
import pathlib
import time
import uuid
from typing import Any

from ccx.store.journal_io import journal_lock as _journal_lock, write_json

from ccx.constants import EXIT_REJECTED, STEER_EVIDENCE_GRACE_S
from ccx.errors import CcxError
from ccx.store.paths import steer_deliveries_lock_path, steer_deliveries_path
from ccx.claude.transcript import STEER_EVIDENCE_RANK, steer_evidence_events

JOURNAL_VERSION = 1
JOURNAL_KEEP = 32
PREVIEW_LIMIT = 200
MAX_DELIVERY_ID = 128

#: Journaled but not yet proven to be placed in a turn.
PENDING_STATUSES = frozenset({"accepted", "pasted", "enqueued"})
#: Resolved: ``consumed`` (current turn), ``started`` (next turn),
#: ``unknown`` (accepted but unprovable), ``failed`` (proven never written).
TERMINAL_STATUSES = frozenset({"consumed", "started", "unknown", "failed",
                               "delivered", "abandoned"})
ALL_STATUSES = PENDING_STATUSES | TERMINAL_STATUSES

#: Statuses that prove Claude itself owns the prompt.
NATIVE_ACCEPTED_STATUSES = frozenset({"enqueued", "consumed", "started"})

# --- public placement vocabulary ---------------------------------------------
#
# Internal statuses record how far a delivery got through CCX's own machinery.
# Public surfaces (CLI, RPC, Python results) must speak only about placement,
# because that is the only thing a caller can act on:
#
#   accepted-pending      CCX durably owns the prompt; Claude has not visibly
#                         placed it in a turn yet (internally accepted/pasted/
#                         enqueued). Never a reason to resend.
#   consumed-current-turn the running turn saw it (queued_command attachment).
#   started-next-turn     it runs as the next turn (user event / dequeue).
#   unknown               genuinely indeterminate transport or evidence.
PUBLIC_ACCEPTED_PENDING = "accepted-pending"
PUBLIC_CONSUMED = "consumed-current-turn"
PUBLIC_STARTED = "started-next-turn"
PUBLIC_UNKNOWN = "unknown"

PUBLIC_STATUS = {
    "accepted": PUBLIC_ACCEPTED_PENDING,
    "pasted": PUBLIC_ACCEPTED_PENDING,
    "enqueued": PUBLIC_ACCEPTED_PENDING,
    "consumed": PUBLIC_CONSUMED,
    "started": PUBLIC_STARTED,
    "unknown": PUBLIC_UNKNOWN,
    # No code path produces `failed` any more: once a prompt may have reached
    # Ink, "proven undelivered" is not a claim CCX can make. Kept mapped so an
    # old journal never leaks a non-vocabulary status.
    "failed": PUBLIC_UNKNOWN,
    "delivered": PUBLIC_UNKNOWN,  # placement from exact evidence below
    "abandoned": PUBLIC_UNKNOWN,
}
PUBLIC_TERMINAL_STATUSES = frozenset({PUBLIC_CONSUMED, PUBLIC_STARTED, PUBLIC_UNKNOWN})


def public_status(status: str | None) -> str | None:
    """Map an internal status onto the approved placement vocabulary."""
    if status is None:
        return None
    return PUBLIC_STATUS.get(status, PUBLIC_UNKNOWN)


#: Statuses whose placement may still be decided by later native evidence.
#: ``unknown`` is included deliberately: it records missing evidence, not a
#: disproven delivery, so its prompt text is retained for correlation.
RECONCILABLE_STATUSES = PENDING_STATUSES | {"unknown"}

_ALLOWED_TRANSITIONS = {
    "accepted": frozenset({"accepted", "pasted", "enqueued", "consumed",
                           "started", "failed", "unknown", "delivered", "abandoned"}),
    # Once written to the PTY we can no longer claim it was never delivered,
    # so `failed` is unreachable from here — only evidence or `unknown`.
    "pasted": frozenset({"pasted", "enqueued", "consumed", "started", "unknown",
                         "delivered", "abandoned"}),
    "enqueued": frozenset({"enqueued", "consumed", "started", "unknown",
                           "delivered", "abandoned"}),
    "consumed": frozenset({"consumed"}),
    "started": frozenset({"started"}),
    "failed": frozenset({"failed"}),
    # `unknown` means evidence was missing, not that placement was disproven;
    # a late native trace is stronger and may still resolve it.
    "unknown": frozenset({"unknown", "consumed", "started",
                          "delivered", "abandoned"}),
    "delivered": frozenset({"delivered"}),
    "abandoned": frozenset({"abandoned"}),
}

_EVIDENCE_STATUS = {"enqueued": "enqueued", "started": "started",
                    "consumed": "consumed"}


class SteerJournalError(RuntimeError):
    """The durable steering journal exists but cannot be trusted."""


class SteerJournalBusy(CcxError, SteerJournalError):
    """Another process holds the journal's mutation lock right now.

    Distinct from corruption: the journal is fine, this caller simply refused
    to wait any longer. Callers must treat it as "nothing was written" — a
    delivery is never journaled, marked, or transitioned on this path — which
    is what keeps a lock timeout from turning into a second paste. A typed
    refusal (CLI exit 5): retrying the same delivery is safe.
    """

    exit_code = EXIT_REJECTED


def now_ms() -> int:
    return int(time.time() * 1000)


def new_delivery_id() -> str:
    return str(uuid.uuid4())


def valid_delivery_id(delivery_id) -> bool:
    return (isinstance(delivery_id, str) and bool(delivery_id)
            and len(delivery_id) <= MAX_DELIVERY_ID)


def preview(text: str | None, limit: int = PREVIEW_LIMIT) -> str | None:
    if not isinstance(text, str) or not text:
        return None
    return text if len(text) <= limit else text[:limit] + "…"


def transcript_offset(path: str | pathlib.Path | None) -> int:
    try:
        return pathlib.Path(path).stat().st_size
    except (OSError, TypeError):
        return 0


def transcript_identity(path: str | pathlib.Path | None) -> tuple[int | None, int | None]:
    try:
        stat = pathlib.Path(path).stat()
    except (OSError, TypeError):
        return None, None
    return stat.st_dev, stat.st_ino


def empty_journal() -> dict[str, Any]:
    return {"version": JOURNAL_VERSION, "deliveries": []}


def read_journal(short: str) -> dict[str, Any]:
    path = steer_deliveries_path(short)
    try:
        value = json.loads(path.read_text())
    except FileNotFoundError:
        return empty_journal()
    except OSError as exc:
        raise SteerJournalError(f"cannot read steering journal: {exc}") from exc
    except ValueError as exc:
        raise SteerJournalError("steering journal is not valid JSON") from exc
    if not isinstance(value, dict) or not isinstance(value.get("deliveries"), list):
        raise SteerJournalError("steering journal has an invalid shape")
    return value


def write_journal(short: str, journal: dict[str, Any]) -> None:
    path = steer_deliveries_path(short)
    path.parent.mkdir(parents=True, exist_ok=True)
    deliveries = list(journal.get("deliveries") or [])
    # Never evict a pending delivery: its full prompt is the only copy of work
    # the caller already owns.
    pending = [d for d in deliveries if d.get("status") in PENDING_STATUSES]
    terminal = [d for d in deliveries if d.get("status") not in PENDING_STATUSES]
    # Keep the newest terminals that still fit beside the pending entries. A
    # bare negative slice would turn "no room left" into `terminal[-0:]`, which
    # keeps every terminal — an unbounded journal exactly when it is fullest.
    room = max(0, JOURNAL_KEEP - len(pending))
    kept = (terminal[-room:] if room else []) + pending
    # Resolved entries keep identity and evidence, not the full prompt body.
    trimmed = []
    for entry in kept:
        if entry.get("status") in RECONCILABLE_STATUSES or not entry.get("text"):
            trimmed.append(entry)
        else:
            trimmed.append({**entry, "text": None})
    value = {
        "version": JOURNAL_VERSION,
        "updated_at": now_ms(),
        "deliveries": trimmed,
    }
    write_json(path, value)


# Hold the lock across read-modify-replace so concurrent writers cannot erase
# an acknowledged delivery. Atomic publication lets readers stay lock-free.

#: Bound for the Claude host, whose select loop is single-threaded: it must
#: never wait behind a SIGSTOPed or wedged client holding the sidecar lock.
#: Short enough that a stalled peer costs one skipped reconcile pass rather
#: than a frozen worker.
HOST_LOCK_TIMEOUT_S = 2.0
#: Bound for out-of-host callers (CLI/Python client reconciliation). They may
#: wait longer than the host — nothing else is blocked behind them — but never
#: indefinitely.
CLIENT_LOCK_TIMEOUT_S = 15.0


def journal_lock(short: str, *, timeout: float | None = None):
    return _journal_lock(
        steer_deliveries_lock_path(short),
        timeout=CLIENT_LOCK_TIMEOUT_S if timeout is None else timeout,
        busy_error=SteerJournalBusy, label=f"steering journal for {short}",
    )


def get_delivery(short: str, delivery_id: str | None) -> dict | None:
    if not delivery_id:
        return None
    for entry in reversed(read_journal(short).get("deliveries") or []):
        if entry.get("delivery_id") == delivery_id:
            return dict(entry)
    return None


def pending_deliveries(short: str) -> list[dict]:
    return [dict(d) for d in (read_journal(short).get("deliveries") or [])
            if d.get("status") in PENDING_STATUSES]


def unproven_deliveries(short: str) -> list[dict]:
    """Deliveries whose placement inside Ink is still unproven.

    A pending entry is obviously unproven.  So is a terminal ``unknown`` that
    carries no evidence: it means ccx wrote prompt text into the editor and
    never learned where that text went.  Claude may still be holding it as a
    queued prompt, so the input surface is not provably free — the text can
    surface as a turn at any later moment (observed in the wild ~33 minutes
    after the delivery gave up).  Anything that seizes the input surface for a
    local slash command must refuse while such an entry exists; the host's
    reconciliation pump clears it as soon as native evidence appears.
    """
    return [dict(d) for d in (read_journal(short).get("deliveries") or [])
            if d.get("status") in RECONCILABLE_STATUSES and not d.get("evidence")]


def begin_delivery(
    short: str,
    *,
    delivery_id: str,
    text: str,
    transcript: str | None,
    host_epoch: str,
    state: str | None = None,
    cap: dict | None = None,
    requested_mode: str | None = None,
    route: str | None = None,
    target_turn_ref: str | None = None,
    timeout: float | None = None,
) -> tuple[dict, bool]:
    """Journal a steer durably, or return the idempotent prior delivery.

    The second element is False when this ``delivery_id`` was already owned —
    the caller must then not queue anything: the original entry is the single
    source of truth for that delivery, whatever state it reached.

    Raises :class:`SteerJournalBusy` if the journal lock cannot be taken within
    ``timeout``. Nothing is journaled then, so no ownership is acknowledged and
    the caller may retry with the same ``delivery_id``.
    """
    with journal_lock(short, timeout=timeout):
        return _begin_delivery_locked(
            short, delivery_id=delivery_id, text=text, transcript=transcript,
            host_epoch=host_epoch, state=state, cap=cap,
            requested_mode=requested_mode, route=route,
            target_turn_ref=target_turn_ref,
        )


def _begin_delivery_locked(
    short: str,
    *,
    delivery_id: str,
    text: str,
    transcript: str | None,
    host_epoch: str,
    state: str | None = None,
    cap: dict | None = None,
    requested_mode: str | None = None,
    route: str | None = None,
    target_turn_ref: str | None = None,
) -> tuple[dict, bool]:
    journal = read_journal(short)
    deliveries = journal.get("deliveries") or []
    for entry in deliveries:
        if entry.get("delivery_id") == delivery_id:
            return dict(entry), False
    accepted_at = now_ms()
    fence_dev, fence_inode = transcript_identity(transcript)
    entry = {
        "delivery_id": delivery_id,
        "status": "accepted",
        "accepted_at": accepted_at,
        "updated_at": accepted_at,
        "host_epoch": host_epoch,
        "accepted_state": state,
        "transcript": str(transcript) if transcript else None,
        "fence_offset": transcript_offset(transcript),
        "fence_dev": fence_dev,
        "fence_inode": fence_inode,
        "text": text,
        "text_len": len(text or ""),
        "preview": preview(text),
        "cap": cap or None,
        "requested_mode": requested_mode,
        "route": route,
        "target_turn_ref": target_turn_ref,
        "attempts": 0,
        "pasted_at": None,
        "evidence": None,
        "evidence_offset": None,
        "evidence_uuid": None,
        "reason": None,
    }
    deliveries.append(entry)
    journal["deliveries"] = deliveries
    write_journal(short, journal)
    return dict(entry), True


def transition_delivery(short: str, delivery_id: str, status: str,
                        *, timeout: float | None = None, **patch: Any) -> dict:
    if status not in ALL_STATUSES:
        raise ValueError(f"invalid steering delivery status: {status}")
    with journal_lock(short, timeout=timeout):
        return _transition_delivery_locked(short, delivery_id, status, **patch)


def _transition_delivery_locked(short: str, delivery_id: str, status: str,
                                **patch: Any) -> dict:
    journal = read_journal(short)
    deliveries = journal.get("deliveries") or []
    for index, entry in enumerate(deliveries):
        if entry.get("delivery_id") != delivery_id:
            continue
        previous = entry.get("status")
        if status not in _ALLOWED_TRANSITIONS.get(previous, frozenset()):
            raise ValueError(
                f"non-monotonic steering transition {previous!r} -> {status!r}"
            )
        updated = {**entry, **patch, "status": status, "updated_at": now_ms()}
        deliveries[index] = updated
        journal["deliveries"] = deliveries
        write_journal(short, journal)
        return dict(updated)
    raise KeyError(f"unknown steering delivery: {delivery_id}")


def mark_pasted(short: str, delivery_id: str, *, offset: int | None = None,
                timeout: float | None = None) -> dict:
    """Record a PTY write attempt before it happens.

    Journaling the attempt first is what makes a crash mid-write safe: the
    recovery path can see that the text may already be in Ink and must not
    write it again.

    One delivery is written at a time; its own text and transcript byte fence
    distinguish it from any other delivery with identical text.
    """
    with journal_lock(short, timeout=timeout):
        # Read and increment inside one hold: the attempt counter is itself a
        # read-modify-write, and losing an attempt is how a recovered host
        # concludes a written prompt was never written and pastes it twice.
        entry = get_delivery(short, delivery_id)
        attempts = int((entry or {}).get("attempts") or 0) + 1
        return transition_delivery(
            short, delivery_id, "pasted",
            attempts=attempts,
            pasted_at=now_ms(),
            paste_offset=offset,
        )


def _fence_valid(entry: dict, transcript: str | None) -> bool:
    path = transcript or entry.get("transcript")
    if not path:
        return False
    fence_dev = entry.get("fence_dev")
    fence_inode = entry.get("fence_inode")
    if fence_dev is None or fence_inode is None:
        return True
    current_dev, current_inode = transcript_identity(path)
    return current_dev == fence_dev and current_inode == fence_inode


def _scan_from(entry: dict) -> int:
    """Byte fence for one entry: never earlier than its own paste attempt."""
    return max(int(entry.get("fence_offset") or 0),
               int(entry.get("paste_offset") or 0))


def reconcile_pending(
    short: str,
    *,
    transcript: str | None = None,
    now: float | None = None,
    grace_s: float = STEER_EVIDENCE_GRACE_S,
    timeout: float | None = None,
) -> list[dict]:
    """Resolve pending deliveries from native transcript evidence.

    Original deliveries are considered in acceptance order. One native
    evidence event belongs to at most one delivery, even with identical text.

    ``now``/``grace_s`` are injectable so tests never need a real wall clock.
    """
    plan = _reconcile_plan(short, transcript=transcript, now=now,
                           grace_s=grace_s)
    if not plan:
        return []
    return _apply_reconcile_plan(short, plan, timeout=timeout)


def _evidence_claim(offset, evidence_uuid):
    """Identity of one native evidence event, or None when it has neither.

    An offset of ``None`` is not an identity: putting it in the claimed set
    would make the first offset-less event block every later one, so fall back
    to the event's own uuid.
    """
    if offset is not None:
        return ("offset", offset)
    if evidence_uuid is not None:
        return ("uuid", evidence_uuid)
    return None


def _reconcile_plan(
    short: str,
    *,
    transcript: str | None = None,
    now: float | None = None,
    grace_s: float = STEER_EVIDENCE_GRACE_S,
) -> list[tuple[str, str, dict]]:
    """Decide what the evidence implies, holding no lock.

    Reading the journal and scanning the transcript are the slow parts of
    reconciliation, and doing them under the mutation lock would hold it for as
    long as a transcript takes to walk — while the single-threaded host may be
    waiting for it. They are pure reads over an atomically published journal,
    so they are safe here; :func:`_apply_reconcile_plan` re-validates every
    decision against the live journal before writing.
    """
    journal = read_journal(short)
    deliveries = journal.get("deliveries") or []
    pending = [d for d in deliveries if d.get("status") in RECONCILABLE_STATUSES]
    if not pending:
        return []
    claimed: dict[tuple, str] = {}
    for d in deliveries:
        claim = _evidence_claim(d.get("evidence_offset"), d.get("evidence_uuid"))
        if claim is not None:
            claimed.setdefault(claim, d["delivery_id"])
    now = time.time() if now is None else now
    plan: list[tuple[str, str, dict]] = []
    for entry in sorted(pending, key=lambda d: d.get("accepted_at") or 0):
        delivery_id = entry["delivery_id"]
        path = transcript or entry.get("transcript")
        # Only stronger evidence advances the original delivery.
        floor = STEER_EVIDENCE_RANK.get(entry.get("status"), 0)
        best = None
        if path and _fence_valid(entry, transcript):
            for evidence in steer_evidence_events(
                path, entry.get("text") or "", after_offset=_scan_from(entry),
            ):
                claim = _evidence_claim(evidence.get("offset"),
                                        evidence.get("uuid"))
                if (claim is not None and claim in claimed
                        and claimed[claim] != delivery_id):
                    continue
                if evidence["rank"] <= floor:
                    continue
                if best is None or evidence["rank"] > best["rank"]:
                    best = evidence
        if best is not None:
            best_claim = _evidence_claim(best.get("offset"), best.get("uuid"))
            if best_claim is not None:
                claimed[best_claim] = delivery_id
            plan.append((delivery_id, _EVIDENCE_STATUS[best["kind"]], {
                "evidence": best["kind"],
                "evidence_offset": best.get("offset"),
                "evidence_uuid": best.get("uuid"),
                "reason": None,
            }))
            continue
        # A possibly written input without native evidence stays unknown, never
        # eligible for a second paste.
        if entry.get("status") == "pasted":
            pasted_at = (entry.get("pasted_at") or entry.get("accepted_at") or 0) / 1000.0
            if grace_s is not None and now - pasted_at >= grace_s:
                plan.append((delivery_id, "unknown", {
                    "reason": "no native evidence within the delivery "
                              "grace window",
                }))
    return plan


def _apply_reconcile_plan(short: str, plan: list[tuple[str, str, dict]], *,
                          timeout: float | None = None) -> list[dict]:
    """Write a plan under one short hold, re-validating against the live journal.

    The plan was computed lock-free, so the journal may have moved on: another
    process may have resolved an entry, or claimed the very evidence event this
    plan wanted to use. Both are re-checked here, and a stale decision is
    dropped rather than written — a delivery only ever moves forward, and one
    native event still resolves at most one original delivery.
    """
    resolved: list[dict] = []
    with journal_lock(short, timeout=timeout):
        current = {d.get("delivery_id"): d
                   for d in (read_journal(short).get("deliveries") or [])}
        # Include both already settled and still planned deliveries: a claim
        # can be reused only by its own original delivery.
        claimed: dict[tuple, str] = {}
        for delivery_id, entry in current.items():
            claim = _evidence_claim(entry.get("evidence_offset"),
                                    entry.get("evidence_uuid"))
            if claim is not None:
                claimed.setdefault(claim, delivery_id)
        for delivery_id, status, patch in plan:
            entry = current.get(delivery_id)
            if entry is None:
                continue
            if status not in _ALLOWED_TRANSITIONS.get(entry.get("status"),
                                                      frozenset()):
                continue
            if STEER_EVIDENCE_RANK.get(status, 0) <= \
                    STEER_EVIDENCE_RANK.get(entry.get("status"), 0) \
                    and status != "unknown":
                continue
            claim = _evidence_claim(patch.get("evidence_offset"),
                                    patch.get("evidence_uuid"))
            if (claim is not None and claim in claimed
                    and claimed[claim] != delivery_id):
                continue
            updated = transition_delivery(short, delivery_id, status, **patch)
            if claim is not None:
                claimed[claim] = delivery_id
            current[delivery_id] = updated
            resolved.append(updated)
    return resolved


def reset_semantic_delivery_for_retry(
    short: str,
    delivery_id: str,
    *,
    timeout: float | None = None,
) -> dict:
    """Reset one semantic hot entry after positive non-submission proof.

    Only a proven unsubmitted idle paste may be reset after clearing Ink.
    A possibly placed native steer is never eligible for another paste.
    """
    with journal_lock(short, timeout=timeout):
        journal = read_journal(short)
        deliveries = journal.get("deliveries") or []
        for index, entry in enumerate(deliveries):
            if entry.get("delivery_id") != delivery_id:
                continue
            if entry.get("status") != "pasted":
                raise SteerJournalError(
                    f"semantic retry reset requires pasted status, got {entry.get('status')!r}"
                )
            updated = {
                **entry,
                "status": "accepted",
                "maybe_written": False,
                "cleared_at": now_ms(),
                "reason": "prior paste cleared after positive non-submission proof",
                "updated_at": now_ms(),
            }
            deliveries[index] = updated
            journal["deliveries"] = deliveries
            write_journal(short, journal)
            return dict(updated)
    raise KeyError(f"unknown steering delivery: {delivery_id}")


def fail_semantic_delivery_unwritten(
    short: str,
    delivery_id: str,
    *,
    reason: str,
    timeout: float | None = None,
) -> dict:
    """Terminalize a semantic hot entry after proven non-delivery + clear."""
    with journal_lock(short, timeout=timeout):
        journal = read_journal(short)
        deliveries = journal.get("deliveries") or []
        for index, entry in enumerate(deliveries):
            if entry.get("delivery_id") != delivery_id:
                continue
            if entry.get("status") in {"consumed", "started", "enqueued"}:
                raise SteerJournalError(
                    f"cannot fail natively accepted semantic delivery ({entry.get('status')})"
                )
            updated = {
                **entry,
                "status": "failed",
                "maybe_written": False,
                "reason": reason,
                "updated_at": now_ms(),
            }
            deliveries[index] = updated
            journal["deliveries"] = deliveries
            write_journal(short, journal)
            return dict(updated)
    raise KeyError(f"unknown steering delivery: {delivery_id}")


def reconcile_delivery(short: str, delivery_id: str, *,
                       transcript: str | None = None,
                       now: float | None = None,
                       timeout: float | None = None) -> dict | None:
    """Reconcile the whole journal, then return one delivery's snapshot.

    Not held under one lock end to end: reconciliation re-validates its own
    decisions, and the snapshot read afterwards is atomic on its own. That
    keeps the lock hold to the write itself.
    """
    entry = get_delivery(short, delivery_id)
    if entry is None:
        return None
    if entry.get("status") in PENDING_STATUSES or entry.get("status") == "unknown":
        try:
            reconcile_pending(short, transcript=transcript, now=now,
                              timeout=timeout)
        except SteerJournalError:
            raise
        except Exception:
            pass
        entry = get_delivery(short, delivery_id) or entry
    return entry


def public_delivery(entry: dict | None) -> dict | None:
    """Bounded RPC/CLI-safe projection.

    Identity, placement, and evidence only — never the full prompt, and never
    an internal status name: ``status`` is always one of the four approved
    placement values (see :data:`PUBLIC_STATUS`).
    """
    if entry is None:
        return None
    return {
        "delivery_id": entry.get("delivery_id"),
        "status": (PUBLIC_CONSUMED if entry.get("status") == "delivered"
                   and entry.get("evidence") == "consumed" else
                   PUBLIC_STARTED if entry.get("status") == "delivered"
                   and entry.get("evidence") == "started" else
                   public_status(entry.get("status"))),
        "accepted_at": entry.get("accepted_at"),
        "updated_at": entry.get("updated_at"),
        "attempts": int(entry.get("attempts") or 0),
        "text_len": entry.get("text_len"),
        "preview": entry.get("preview"),
        "evidence": entry.get("evidence"),
        "evidence_uuid": entry.get("evidence_uuid"),
        "reason": entry.get("reason"),
        "pending": public_status(entry.get("status")) == PUBLIC_ACCEPTED_PENDING,
    }


def observe_delivery(
    short: str,
    delivery_id: str,
    *,
    transcript: str | None,
    status_reader=None,
    timeout: float | None = None,
) -> tuple[dict | None, str | None]:
    """Read one reconciled delivery snapshot for CLI and Python callers.

    ``status_reader`` is an optional zero-argument callback returning the live
    host's RPC envelope; any unavailable/bad reply falls back to the durable
    journal plus transcript. Journal corruption deliberately propagates so
    public surfaces can report an indeterminate delivery instead of silently
    treating it as absent (and retrying it).
    """
    current = None
    native_state = None
    if status_reader is not None:
        try:
            reply = status_reader() or {}
        except Exception:
            reply = {}
        native_state = reply.get("state")
        if reply.get("ok"):
            current = reply.get("delivery")
    if current is None:
        current = get_delivery(short, delivery_id)
        if current is not None:
            current = public_delivery(
                reconcile_delivery(short, delivery_id, transcript=transcript,
                                   timeout=timeout)
                or current
            )
    return current, native_state

def finalize_dead_claude_steers(
    store, target: str, *, child_pid: int | None, child_start: str | None,
) -> list[str]:
    """Once the previous child is positively dead, settle fenced native steers.

    One owner-pinned fd and one bounded scan per unresolved delivery. An
    unreadable/replaced source, unproved child death, absent fence or malformed
    JSONL leaves the receipt unresolved. Hot journal first, then semantic:
    a crash between writes is repaired from the hot terminal on next kill/revive.
    No text is ever sent or replayed.
    """
    from ccx.claude.delivery import (read_receipt, finalize_dead_steer)
    from ccx.store.liveness import owned_process
    from ccx.claude.transcript import (
        _block_text, _has_queued_command_marker, _iter_transcript,
        normalize_semantic_prompt_text, open_original_claude_source, prompt_matches,
    )
    from ccx.store.root import StorageIntegrityError

    owner = store.resolve(target)
    if owner.engine != "claude":
        return []
    if (not isinstance(child_pid, int) or child_pid <= 0
            or not child_start or owned_process(child_pid, child_start) != "dead"):
        return []
    entries = [d for d in read_journal(owner.worker_id).get("deliveries", [])
               if d.get("status") in
               (RECONCILABLE_STATUSES | {"delivered", "abandoned", "consumed", "started"})]
    if not entries:
        return []
    resolved: list[str] = []
    record = store.record(owner.worker)
    # The native owner/source is checked with descriptor-pinned origin binding.
    with open_original_claude_source(store, owner, record) as source:
        if source is None:
            return []
        pinned, stat, eof, path, _parent = source
        claimed: dict[tuple, str] = {}
        for row in read_journal(owner.worker_id).get("deliveries", []):
            claim = _evidence_claim(row.get("evidence_offset"), row.get("evidence_uuid"))
            if claim is not None:
                claimed.setdefault(claim, row.get("delivery_id"))
        for item in entries:
            token = item.get("delivery_id")
            if not isinstance(token, str) or not token:
                continue
            receipt = read_receipt(owner.worker, token)
            route = (receipt or {}).get("route")
            if (route not in {"steer", "new_turn"}
                    or item.get("route") != route):
                continue  # no hot-only authority or contradictory routing
            if (route == "new_turn" and receipt.get("state") not in
                    {"owned", "delivered", "abandoned"}):
                continue  # an already accepted original turn needs no repair
            if (item.get("transcript") != str(path)
                    or item.get("fence_dev") != stat.st_dev
                    or item.get("fence_inode") != stat.st_ino
                    or type(item.get("fence_offset")) is not int
                    or not 0 <= _scan_from(item) <= eof):
                continue
            status = item.get("status")
            if status in {"delivered", "abandoned", "consumed", "started"}:
                fate = ("abandoned" if status == "abandoned" else "delivered")
                evidence = ({"kind": item.get("evidence"),
                             "offset": item.get("evidence_offset"),
                             "uuid": item.get("evidence_uuid")}
                            if fate == "delivered" else None)
                if (route == "steer" and fate == "delivered" and (
                    evidence["kind"] not in {"enqueued", "started", "consumed"}
                    or type(evidence["offset"]) is not int
                )):
                    continue
                # For owned new-turns, generic hot started/consumed is already
                # accepted delivery evidence, even without a native offset.
                # A proven-dead child must not strand the semantic receipt.
            else:
                prompt = item.get("text")
                if not isinstance(prompt, str) or not prompt.strip():
                    continue
                stats = {}
                best = None
                try:
                    if route == "new_turn":
                        # Unlike steering, new-turn acceptance is only a
                        # non-meta, non-queued ordinary USER row with exactly
                        # matching normalized text after the original fence.
                        import hashlib
                        if hashlib.sha256(prompt.encode("utf-8")).hexdigest() != receipt.get(
                            "promptSha256"
                        ):
                            continue
                        expected = normalize_semantic_prompt_text(prompt).strip()
                        for event in _iter_transcript(
                            pinned, stats=stats, after_offset=_scan_from(item),
                            before_offset=eof,
                        ):
                            if (event.get("type") != "user"
                                    or event.get("isMeta")
                                    or _has_queued_command_marker(event)):
                                continue
                            content = (event.get("message") or {}).get("content")
                            if not prompt_matches(expected, _block_text(content)):
                                continue
                            evidence = {"kind": "started",
                                        "offset": event.get("_ccx_end_offset"),
                                        "uuid": event.get("uuid")}
                            claim = _evidence_claim(evidence["offset"], evidence["uuid"])
                            if claim is None or claim not in claimed or claimed[claim] == token:
                                best = evidence
                                break
                    else:
                        for evidence in steer_evidence_events(
                            pinned, prompt, after_offset=_scan_from(item),
                            before_offset=eof, stats=stats,
                        ):
                            claim = _evidence_claim(evidence.get("offset"),
                                                    evidence.get("uuid"))
                            if (claim is not None and claim in claimed
                                    and claimed[claim] != token):
                                continue
                            if best is None or evidence["rank"] > best["rank"]:
                                best = evidence
                except (OSError, ValueError, StorageIntegrityError):
                    continue
                if stats.get("malformedLines"):
                    continue
                fate = "delivered" if best else "abandoned"
                evidence = best
            if status in RECONCILABLE_STATUSES:
                patch = {
                    "evidence": evidence.get("kind") if evidence else None,
                    "evidence_offset": evidence.get("offset") if evidence else None,
                    "evidence_uuid": evidence.get("uuid") if evidence else None,
                    "reason": ("previous native Claude child exited; no fenced "
                               f"native {route} evidence" if fate == "abandoned" else None),
                }
                try:
                    transition_delivery(owner.worker_id, token, fate, **patch)
                except ValueError:
                    # Concurrent evidence reconciliation won. Do not overwrite.
                    continue
            finalize_dead_steer(owner.worker, token, fate=fate, evidence=evidence)
            if evidence:
                claim = _evidence_claim(evidence.get("offset"), evidence.get("uuid"))
                if claim:
                    claimed[claim] = token
            resolved.append(token)
    store.revalidate()
    return resolved
