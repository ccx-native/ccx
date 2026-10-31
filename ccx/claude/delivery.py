"""Durable semantic-delivery receipts for managed Claude workers.

The bounded per-session deliveries sidecar remains the hot PTY-placement
journal. This module owns long-lived caller identity and reconciliation facts so
hot-journal eviction can never make a semantic delivery id reusable. Terminal
receipts are retained for ``RECEIPT_RETENTION_S`` after their last update (far
beyond any retry of a lost reply) and then pruned at host startup, unless the
hot journal still references them.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import math
import os
import threading
import time
from pathlib import Path
from typing import Any, Iterator


SCHEMA_VERSION = 2
MAX_DELIVERY_ID = 128
MODES = frozenset({"new_turn_only", "auto", "steer_only"})
ROUTES = frozenset({"new_turn", "steer"})
PLACEMENTS = frozenset({"new_turn", "current_turn"})
STATES = frozenset({"prepared", "owned", "accepted", "rejected",
                    "delivered", "abandoned"})
TERMINAL_STATES = frozenset({"accepted", "rejected", "delivered", "abandoned"})
_CALLER_METADATA_UNSET = object()

HOST_LOCK_TIMEOUT_S = 2.0
CLIENT_LOCK_TIMEOUT_S = 15.0
_LOCK_POLL_S = 0.02
RECEIPT_RETENTION_S = 30 * 24 * 60 * 60

_LOCAL = threading.local()
_THREAD_LOCKS: dict[str, threading.RLock] = {}
_THREAD_LOCKS_GUARD = threading.Lock()


class ClaudeDeliveryError(RuntimeError):
    """A Claude semantic receipt cannot be trusted or transitioned."""


class ClaudeDeliveryConflict(ClaudeDeliveryError):
    """The caller reused a delivery id with different semantic content."""


class ClaudeDeliveryBusy(ClaudeDeliveryError):
    """The receipt lock could not be acquired within the bounded wait."""


def now_ms() -> int:
    return int(time.time() * 1000)


def valid_delivery_id(value: object) -> bool:
    return (
        isinstance(value, str)
        and bool(value)
        and len(value) <= MAX_DELIVERY_ID
        and "\x00" not in value
    )


def _store():
    """Resolve one ACTIVE full-ID global owner; never initialize legacy root."""
    from ccx.constants import CCX_ROOT
    from ccx.store.root import require_ccx_active
    return require_ccx_active(CCX_ROOT)


def _owner(store, target: str):
    owner = store.resolve(target)
    if owner.engine != "claude":
        from ccx.store.identity import EngineMismatch
        raise EngineMismatch("Delivery requires a Claude worker handle")
    return owner


def _key(delivery_id: str) -> str:
    return hashlib.sha256(delivery_id.encode("utf-8")).hexdigest()


def receipt_path(target: str, delivery_id: str) -> Path:
    """Display-only diagnostic path; never open it for internal receipt I/O."""
    store = _store()
    owner = _owner(store, target)
    return (store.pointer.generation_root / "semantic-deliveries"
            / "claude" / owner.worker_id / (_key(delivery_id) + ".json"))


def _thread_lock(key: str) -> threading.RLock:
    with _THREAD_LOCKS_GUARD:
        lock = _THREAD_LOCKS.get(key)
        if lock is None:
            lock = _THREAD_LOCKS[key] = threading.RLock()
        return lock


@contextlib.contextmanager
def receipt_lock(
    short: str, delivery_id: str, *, timeout: float | None = None
) -> Iterator[None]:
    """Stable per-(generation,full UUID,semantic delivery) POSIX lock.

    Every actual read/write is fd-relative and nofollow. The outer ACTIVE
    lease covers the bounded receipt update but never a model turn. All
    receipt-state transition functions remain the original Claude code.
    """
    from ccx.store.root import _flock_at, GenerationBusy
    if not valid_delivery_id(delivery_id):
        raise ClaudeDeliveryError("invalid semantic delivery_id")
    store = _store()
    owner = _owner(store, short)
    budget = CLIENT_LOCK_TIMEOUT_S if timeout is None else max(0.0, float(timeout))
    if budget > 3:
        budget = 3.0
    key = f"{store.generation}:{owner.worker_id}:{_key(delivery_id)}"
    held = getattr(_LOCAL, "held", None)
    if held is None:
        held = _LOCAL.held = set()
    if key in held:
        with store.lease():
            yield
        return
    thread_lock = _thread_lock(key)
    if not thread_lock.acquire(timeout=budget):
        raise ClaudeDeliveryBusy(f"delivery_id {delivery_id!r} is locally locked")
    try:
        with store.lease():
            with store._gen("semantic-deliveries", "claude",
                            owner.worker_id, create=True) as fd:
                try:
                    with _flock_at(fd, _key(delivery_id) + ".lock",
                                   exclusive=True, create=True, timeout=budget):
                        held.add(key)
                        try:
                            yield
                        finally:
                            held.discard(key)
                except GenerationBusy as exc:
                    raise ClaudeDeliveryBusy("Claude receipt lock contested") from exc
    finally:
        thread_lock.release()


def validate_cap(cap: dict | None) -> dict | None:
    """One valid per-message Claude quota cap, never silently uncapped."""
    if cap is None:
        return None
    if not isinstance(cap, dict):
        raise ClaudeDeliveryError("cap must be an object of quota percentages")
    if not cap:
        return None
    if any(
        key not in {"5h", "7d", "opus"}
        or type(value) not in (int, float)
        or not math.isfinite(value)
        or not 0 <= value <= 100
        for key, value in cap.items()
    ):
        raise ClaudeDeliveryError("cap keys must be 5h/7d/opus and values finite 0..100")
    return dict(cap)


def spec_digest(receipt: dict) -> str:
    """Original Claude immutable identity; keep no-cap strict-send bytes.

    The native producer, caller and read-only completion reconciler use the
    same fields/serializer. Codex has a different message-id-bound identity.
    """
    from ccx.store.store import _digest
    return _digest({
        "deliveryId": receipt["deliveryId"],
        "promptSha256": receipt["promptSha256"],
        "promptByteCount": receipt["promptByteCount"],
        "requestedMode": receipt["requestedMode"],
        "callerMetadata": receipt.get("callerMetadata"),
        "cap": receipt.get("cap"),
    })


def _prompt_identity(text: str) -> tuple[str, int]:
    raw = text.encode("utf-8")
    return hashlib.sha256(raw).hexdigest(), len(raw)


def _validate(value: Any, *, short: str, delivery_id: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ClaudeDeliveryError("Claude delivery receipt has an invalid shape")
    if value.get("schemaVersion") != SCHEMA_VERSION:
        raise ClaudeDeliveryError("unsupported Claude delivery receipt version")
    store = _store()
    owner = _owner(store, short)
    if (value.get("workerId") != owner.worker_id
            or value.get("engine") != "claude"
            or value.get("generation") != store.generation
            or value.get("deliveryId") != delivery_id):
        raise ClaudeDeliveryError("Claude delivery belongs to another worker; inspect its ID")
    if value.get("state") not in STATES:
        raise ClaudeDeliveryError("Claude delivery receipt has an invalid state")
    if value.get("requestedMode") not in MODES:
        raise ClaudeDeliveryError("Claude delivery receipt has an invalid mode")
    route = value.get("route")
    if route is not None and route not in ROUTES:
        raise ClaudeDeliveryError("Claude delivery receipt has an invalid route")
    placement = value.get("placement")
    if placement is not None and placement not in PLACEMENTS:
        raise ClaudeDeliveryError("Claude delivery receipt has invalid placement")
    return value


def read_receipt(short: str, delivery_id: str) -> dict[str, Any] | None:
    if not valid_delivery_id(delivery_id):
        raise ClaudeDeliveryError("invalid delivery id")
    from ccx.store.root import _read_json
    store = _store()
    owner = _owner(store, short)
    with store.lease():
        try:
            with store._gen("semantic-deliveries", "claude", owner.worker_id) as fd:
                value = _read_json(fd, _key(delivery_id) + ".json")
        except FileNotFoundError:
            value = None
    return _validate(value, short=short, delivery_id=delivery_id) if value is not None else None


def _write(short: str, delivery_id: str, value: dict[str, Any]) -> None:
    from ccx.store.root import _write_json
    store = _store()
    owner = _owner(store, short)
    value["updatedAtMs"] = now_ms()
    value["workerId"] = owner.worker_id
    value["engine"] = "claude"
    value["generation"] = store.generation
    if "workerShort" in value:
        raise ClaudeDeliveryError("legacy workerShort authority forbidden")
    with store.lease():
        with store._gen("semantic-deliveries", "claude", owner.worker_id,
                        create=True) as fd:
            _write_json(fd, _key(delivery_id) + ".json", value)


def _assert_same(
    entry: dict[str, Any],
    text: str,
    mode: str,
    *,
    caller_metadata: Any = _CALLER_METADATA_UNSET,
    cap: dict | None = None,
) -> None:
    digest, byte_count = _prompt_identity(text)
    metadata_conflict = (
        caller_metadata is not _CALLER_METADATA_UNSET
        and entry.get("callerMetadata") != caller_metadata
    )
    if (
        entry.get("promptSha256") != digest
        or entry.get("promptByteCount") != byte_count
        or entry.get("requestedMode") != mode
        or metadata_conflict
        or entry.get("cap") != cap
    ):
        raise ClaudeDeliveryConflict(
            "delivery_id is already owned by different prompt, mode, caps, "
            "or caller metadata"
        )


def require_matching(
    target: str, delivery_id: str, text: str, *, mode: str,
    cap: dict | None = None,
) -> dict[str, Any] | None:
    """Read-only exact original content/mode/cap check; never prepare."""
    cap = validate_cap(cap)
    prior = read_receipt(target, delivery_id)
    if prior is not None:
        _assert_same(prior, text, mode, cap=cap)
    return prior


def prepare(
    short: str,
    delivery_id: str,
    text: str,
    *,
    mode: str,
    timeout: float | None = None,
    caller_metadata: Any = _CALLER_METADATA_UNSET,
    cap: dict | None = None,
) -> tuple[dict[str, Any], bool]:
    if not valid_delivery_id(delivery_id):
        raise ClaudeDeliveryError(
            "delivery_id must be a non-empty string of at most 128 characters"
        )
    if not isinstance(text, str) or not text.strip():
        raise ClaudeDeliveryError("prompt text is required")
    if mode not in MODES:
        raise ClaudeDeliveryError(f"unsupported Claude delivery mode: {mode!r}")
    cap = validate_cap(cap)
    with receipt_lock(short, delivery_id, timeout=timeout):
        prior = read_receipt(short, delivery_id)
        if prior is not None:
            _assert_same(
                prior,
                text,
                mode,
                caller_metadata=caller_metadata,
                cap=cap,
            )
            return prior, False
        digest, byte_count = _prompt_identity(text)
        created = now_ms()
        value = {
            "schemaVersion": SCHEMA_VERSION,
            "engine": "claude",
            "workerId": _owner(_store(), short).worker_id,
            "generation": _store().generation,
            "deliveryId": delivery_id,
            "promptSha256": digest,
            "promptByteCount": byte_count,
            "promptText": text,
            "requestedMode": mode,
            "cap": cap,
            "callerMetadata": (
                None
                if caller_metadata is _CALLER_METADATA_UNSET
                else caller_metadata
            ),
            "state": "prepared",
            "route": None,
            "placement": None,
            "targetTurnRef": None,
            "turnRef": None,
            "managedTurnOrdinal": None,
            "createdAtMs": created,
            "updatedAtMs": created,
            "ownedAtMs": None,
            "acceptedAtMs": None,
            "rejectedAtMs": None,
            "lastRefusalCode": None,
            "lastRefusalMessage": None,
            "lastRefusalState": None,
            "lastRefusalAtMs": None,
            "attempts": 0,
            "maybeWritten": False,
            "evidence": None,
            "evidenceUuid": None,
            "evidenceOffset": None,
            "evidenceStatus": None,
            "rejectionCode": None,
            "rejectionMessage": None,
        }
        _write(short, delivery_id, value)
        return dict(value), True


def _mutate(
    short: str,
    delivery_id: str,
    *,
    timeout: float | None,
    mutate,
) -> dict[str, Any]:
    with receipt_lock(short, delivery_id, timeout=timeout):
        value = read_receipt(short, delivery_id)
        if value is None:
            raise ClaudeDeliveryError(f"unknown Claude delivery: {delivery_id}")
        updated = mutate(dict(value))
        _write(short, delivery_id, updated)
        return updated


def mark_retryable_refusal(
    short: str,
    delivery_id: str,
    *,
    code: str,
    message: str,
    state: str | None = None,
    timeout: float | None = None,
) -> dict[str, Any]:
    def mutate(value: dict[str, Any]) -> dict[str, Any]:
        if value["state"] != "prepared":
            return value
        value.update(
            lastRefusalCode=code,
            lastRefusalMessage=message,
            lastRefusalState=state,
            lastRefusalAtMs=now_ms(),
        )
        return value

    return _mutate(short, delivery_id, timeout=timeout, mutate=mutate)


def mark_owned(
    short: str,
    delivery_id: str,
    *,
    route: str,
    target_turn_ref: str | None = None,
    managed_turn_ordinal: int | None = None,
    timeout: float | None = None,
) -> dict[str, Any]:
    if route not in ROUTES:
        raise ClaudeDeliveryError(f"invalid Claude delivery route: {route!r}")

    def mutate(value: dict[str, Any]) -> dict[str, Any]:
        if value["state"] == "owned":
            if (
                value.get("route") != route
                or value.get("targetTurnRef") != target_turn_ref
            ):
                raise ClaudeDeliveryConflict("owned delivery route/target mismatch")
            return value
        if value["state"] != "prepared":
            return value
        value.update(
            state="owned",
            route=route,
            targetTurnRef=target_turn_ref,
            managedTurnOrdinal=managed_turn_ordinal,
            ownedAtMs=now_ms(),
            lastRefusalCode=None,
            lastRefusalMessage=None,
            lastRefusalState=None,
            lastRefusalAtMs=None,
        )
        return value

    return _mutate(short, delivery_id, timeout=timeout, mutate=mutate)


def mark_attempt(
    short: str,
    delivery_id: str,
    *,
    maybe_written: bool = True,
    timeout: float | None = None,
) -> dict[str, Any]:
    def mutate(value: dict[str, Any]) -> dict[str, Any]:
        if value["state"] != "owned":
            return value
        value["attempts"] = int(value.get("attempts") or 0) + 1
        value["maybeWritten"] = bool(maybe_written)
        return value

    return _mutate(short, delivery_id, timeout=timeout, mutate=mutate)


def mark_evidence(
    short: str,
    delivery_id: str,
    *,
    evidence: str | None = None,
    evidence_uuid: str | None = None,
    evidence_offset: int | None = None,
    evidence_status: str | None = None,
    maybe_written: bool | None = None,
    timeout: float | None = None,
) -> dict[str, Any]:
    def mutate(value: dict[str, Any]) -> dict[str, Any]:
        if value["state"] in TERMINAL_STATES:
            return value
        if evidence is not None:
            value["evidence"] = evidence
        if evidence_uuid is not None:
            value["evidenceUuid"] = evidence_uuid
        if evidence_offset is not None:
            value["evidenceOffset"] = evidence_offset
        if evidence_status is not None:
            value["evidenceStatus"] = evidence_status
        if maybe_written is not None:
            value["maybeWritten"] = bool(maybe_written)
        return value

    return _mutate(short, delivery_id, timeout=timeout, mutate=mutate)


def mark_accepted(
    short: str,
    delivery_id: str,
    *,
    placement: str,
    turn_ref: str,
    managed_turn_ordinal: int | None = None,
    evidence: str | None = None,
    evidence_uuid: str | None = None,
    evidence_offset: int | None = None,
    timeout: float | None = None,
) -> dict[str, Any]:
    if placement not in PLACEMENTS:
        raise ClaudeDeliveryError(f"invalid Claude delivery placement: {placement!r}")
    if not isinstance(turn_ref, str) or not turn_ref:
        raise ClaudeDeliveryError("accepted Claude delivery requires turn_ref")

    def mutate(value: dict[str, Any]) -> dict[str, Any]:
        if value["state"] == "accepted":
            if value.get("placement") != placement or value.get("turnRef") != turn_ref:
                raise ClaudeDeliveryConflict("accepted delivery placement/turn mismatch")
            return value
        if value["state"] != "owned":
            raise ClaudeDeliveryError(
                f"cannot accept Claude delivery from state={value['state']}"
            )
        value.update(
            state="accepted",
            placement=placement,
            turnRef=turn_ref,
            managedTurnOrdinal=managed_turn_ordinal,
            acceptedAtMs=now_ms(),
            maybeWritten=False,
            evidenceStatus="accepted",
            evidence=evidence or value.get("evidence"),
            evidenceUuid=evidence_uuid or value.get("evidenceUuid"),
            evidenceOffset=(
                evidence_offset
                if evidence_offset is not None
                else value.get("evidenceOffset")
            ),
            promptText=None,
            rejectionCode=None,
            rejectionMessage=None,
        )
        return value

    return _mutate(short, delivery_id, timeout=timeout, mutate=mutate)



def finalize_dead_steer(
    target: str, delivery_id: str, *, fate: str,
    evidence: dict | None,
) -> dict[str, Any]:
    """Terminalize a source-scanned steer or new-turn after confirmed child death.

    No native replay. The new-turn delivered case requires exact ordinary-user
    evidence; a queued steer event cannot prove a strict new-turn submission.
    """
    if fate not in {"delivered", "abandoned"}:
        raise ClaudeDeliveryError("invalid dead-child steer outcome")
    def mutate(row: dict[str, Any]) -> dict[str, Any]:
        route = row.get("route")
        if route not in {"steer", "new_turn"} or row.get("state") == "prepared":
            raise ClaudeDeliveryError("dead-child finalizer requires owned delivery")
        if route == "steer" and fate == "delivered" and (
            not isinstance(evidence, dict)
            or evidence.get("kind") not in {"enqueued", "started", "consumed"}
            or type(evidence.get("offset")) is not int
        ):
            raise ClaudeDeliveryError("Steering cannot be verified; inspect its delivery")
        if route == "new_turn" and fate == "delivered" and not isinstance(evidence, dict):
            raise ClaudeDeliveryError("Message delivery is not confirmed; inspect the worker")
        if row.get("state") in {"accepted", "rejected"}:
            return row
        if row.get("state") in {"delivered", "abandoned"}:
            if row["state"] != fate:
                raise ClaudeDeliveryConflict("Claude stopped but delivery records disagree; inspect the worker")
            return row
        if row.get("state") != "owned":
            raise ClaudeDeliveryError("Steering status is invalid; inspect the delivery")
        row.update(
            state=fate, maybeWritten=False, promptText=None,
            evidence=(evidence or {}).get("kind"),
            evidenceUuid=(evidence or {}).get("uuid"),
            evidenceOffset=(evidence or {}).get("offset"),
            evidenceStatus=fate,
        )
        if route == "new_turn" and fate == "delivered":
            row["placement"] = "new_turn"
        return row

    return _mutate(target, delivery_id, timeout=None, mutate=mutate)


def mark_settled(
    short: str,
    delivery_id: str,
    *, turn_ref: str,
    original_positive_completion: dict[str, Any],
    timeout: float | None = None,
) -> dict[str, Any]:
    """Persist first exact original settled-turn evidence under receipt lock.

    Does not repeat a native send, change original delivery ID, modify old
    accepted timestamps or infer completion from a model wait timeout. It
    records only a positive exact source/turn/status completion proof. A
    subsequent same-ID response returns the identical settlement without
    extra native effects. Original accepted/rejected state machine retained.
    """
    if not isinstance(turn_ref, str) or not turn_ref:
        raise ClaudeDeliveryError("Completion requires a turn reference; inspect the delivery")
    if not isinstance(original_positive_completion, dict):
        raise ClaudeDeliveryError("Completion could not be verified; inspect the delivery")
    if (original_positive_completion.get("turnRef") != turn_ref
            or original_positive_completion.get("status") != "completed"
            or not isinstance(original_positive_completion.get("text"), str)
            or not isinstance(original_positive_completion.get("nativeEvidenceSha256"), str)
            or len(original_positive_completion["nativeEvidenceSha256"]) != 64):
        raise ClaudeDeliveryError("Claude turn is not confirmed complete; inspect its status")
    from ccx.store.root import _HASH
    if not _HASH.fullmatch(original_positive_completion["nativeEvidenceSha256"]):
        raise ClaudeDeliveryError("Claude completion record is invalid; inspect the worker")

    def mutate(value: dict[str, Any]) -> dict[str, Any]:
        if (value.get("state") != "accepted"
                or value.get("turnRef") != turn_ref):
            raise ClaudeDeliveryError("Turn is not accepted or belongs to another delivery; inspect its ID")
        old = value.get("settlement")
        if old is not None:
            if old != original_positive_completion:
                raise ClaudeDeliveryConflict("original positive turn settlement is immutable")
            return value
        value["settlement"] = dict(original_positive_completion)
        value["settledAtMs"] = now_ms()
        return value

    return _mutate(short, delivery_id, timeout=timeout, mutate=mutate)


def mark_rejected(
    short: str,
    delivery_id: str,
    *,
    code: str,
    message: str,
    timeout: float | None = None,
) -> dict[str, Any]:
    def mutate(value: dict[str, Any]) -> dict[str, Any]:
        if value["state"] == "rejected":
            return value
        if value["state"] == "accepted":
            raise ClaudeDeliveryError("accepted Claude delivery cannot be rejected")
        value.update(
            state="rejected",
            rejectedAtMs=now_ms(),
            rejectionCode=code,
            rejectionMessage=message,
            promptText=None,
            maybeWritten=False,
        )
        return value

    return _mutate(short, delivery_id, timeout=timeout, mutate=mutate)


def iter_receipts(short: str):
    """Original best-effort status inventory, strict exact owner for each row."""
    from ccx.store.root import _read_json, StorageIntegrityError
    store = _store()
    owner = _owner(store, short)
    with store.lease():
        try:
            with store._gen("semantic-deliveries", "claude", owner.worker_id) as fd:
                with os.scandir(fd) as entries:
                    names = sorted(e.name for e in entries if e.name.endswith(".json"))
                if len(names) > 100000:
                    raise ClaudeDeliveryError("semantic receipt inventory exceeds budget")
                rows = []
                for name in names:
                    try:
                        rows.append(_read_json(fd, name))
                    except StorageIntegrityError:
                        pass  # Preserve corrupt bytes when pruning cannot safely inspect them.
        except FileNotFoundError:
            rows = []
    for value in rows:
        identity = value.get("deliveryId") if isinstance(value, dict) else None
        if not valid_delivery_id(identity):
            continue
        try:
            yield _validate(value, short=short, delivery_id=identity)
        except ClaudeDeliveryError:
            continue


def prune_receipts(
    short: str, *, keep_ids=(), retention_s: float = RECEIPT_RETENTION_S,
    lock_timeout: float = 0.0,
) -> int:
    """Prune terminal JSON only; stable per-ID lock files must never unlink."""
    from ccx.store.root import _read_json
    store = _store()
    owner = _owner(store, short)
    keep = set(keep_ids)
    cutoff_ms = now_ms() - int(retention_s * 1000)
    total = 0
    for value in iter_receipts(short):
        delivery_id = value["deliveryId"]
        if delivery_id in keep or not _prunable(value, cutoff_ms):
            continue
        try:
            with receipt_lock(short, delivery_id, timeout=lock_timeout):
                current = read_receipt(short, delivery_id)
                if current is None or not _prunable(current, cutoff_ms):
                    continue
                with store.lease():
                    with store._gen("semantic-deliveries", "claude",
                                    owner.worker_id) as fd:
                        os.unlink(_key(delivery_id) + ".json", dir_fd=fd)
                        os.fsync(fd)
                total += 1
        except (ClaudeDeliveryBusy, FileNotFoundError):
            continue
    return total


def _prunable(value: dict[str, Any], cutoff_ms: int) -> bool:
    updated = value.get("updatedAtMs")
    return (
        value.get("state") in TERMINAL_STATES
        and isinstance(updated, int)
        and updated < cutoff_ms
    )


def public_receipt(value: dict[str, Any] | None) -> dict[str, Any] | None:
    if value is None:
        return None
    state = value.get("state")
    if state in {"accepted", "delivered"}:
        status = "accepted"
        action = "none"
    elif state == "abandoned":
        # Public placement remains unknown, not a native refusal. Old-child
        # death resolved its ownership: nothing remains pending/reconcilable.
        status = "unknown"
        action = "none"
    elif state == "rejected":
        status = "rejected"
        action = "none"
    elif state == "prepared":
        status = "pending"
        action = "retry_same_id"
    else:
        status = "pending"
        action = "wait"
    return {
        "delivery_id": value.get("deliveryId"),
        "status": status,
        "caller_metadata": value.get("callerMetadata"),
        "owned": state in {"owned", "accepted", "delivered", "abandoned"},
        "recommended_action": action,
        "route": value.get("route"),
        "placement": value.get("placement"),
        "target_turn_ref": value.get("targetTurnRef"),
        "turn_ref": value.get("turnRef"),
        "managed_turn_ordinal": value.get("managedTurnOrdinal"),
        "created_at_ms": value.get("createdAtMs"),
        "updated_at_ms": value.get("updatedAtMs"),
        "owned_at_ms": value.get("ownedAtMs"),
        "accepted_at_ms": value.get("acceptedAtMs"),
        "settled_at_ms": value.get("settledAtMs"),
        "settlement": value.get("settlement"),
        "rejected_at_ms": value.get("rejectedAtMs"),
        "attempts": int(value.get("attempts") or 0),
        "maybe_written": bool(value.get("maybeWritten")),
        "evidence": value.get("evidence"),
        "evidence_uuid": value.get("evidenceUuid"),
        "evidence_offset": value.get("evidenceOffset"),
        "evidence_status": value.get("evidenceStatus"),
        "rejection_code": value.get("rejectionCode"),
        "rejection_message": value.get("rejectionMessage"),
        "last_refusal": (
            {
                "code": value.get("lastRefusalCode"),
                "message": value.get("lastRefusalMessage"),
                "state": value.get("lastRefusalState"),
                "at_ms": value.get("lastRefusalAtMs"),
            }
            if value.get("lastRefusalCode") or value.get("lastRefusalMessage")
            else None
        ),
    }


__all__ = [
    "CLIENT_LOCK_TIMEOUT_S",
    "HOST_LOCK_TIMEOUT_S",
    "ClaudeDeliveryBusy",
    "ClaudeDeliveryConflict",
    "ClaudeDeliveryError",
    "MODES",
    "PLACEMENTS",
    "ROUTES",
    "TERMINAL_STATES",
    "iter_receipts",
    "mark_accepted",
    "mark_attempt",
    "mark_evidence",
    "mark_owned",
    "mark_rejected",
    "mark_retryable_refusal",
    "prepare",
    "prune_receipts",
    "public_receipt",
    "read_receipt",
    "receipt_lock",
    "receipt_path",
    "valid_delivery_id",
]
