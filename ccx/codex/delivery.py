"""Durable caller-known delivery receipts for Codex turn starts.

A receipt is persisted before a turn can be submitted. The daemon moves the
receipt to submitted immediately before the irreversible native turn/start
call, and to accepted only after a native turn id is known. A receipt observed
in submitted is never re-submitted.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import time
import uuid
from pathlib import Path
from typing import Any, Iterator

from ccx.codex.provenance import new_client_message_id

SCHEMA_VERSION = 2
MAX_DELIVERY_ID = 128
STATES = frozenset({"prepared", "owned", "submitted", "accepted", "rejected"})
TERMINAL_STATES = frozenset({"accepted", "rejected"})
MODES = frozenset({"new_turn_only", "auto", "steer_only"})
ROUTES = frozenset({"new_turn", "steer"})
PLACEMENTS = frozenset({"new_turn", "current_turn"})
_CALLER_METADATA_UNSET = object()


class DeliveryReceiptError(RuntimeError):
    pass


class DeliveryReceiptConflict(DeliveryReceiptError):
    pass


def now_ms() -> int:
    return int(time.time() * 1000)


def new_delivery_id() -> str:
    return str(uuid.uuid4())


def valid_delivery_id(value: object) -> bool:
    return (
        isinstance(value, str)
        and bool(value)
        and len(value) <= MAX_DELIVERY_ID
        and "\x00" not in value
    )


def _store():
    from ccx.constants import CCX_ROOT
    from ccx.store.root import require_ccx_active
    return require_ccx_active(CCX_ROOT)


def _owner(store, target: str):
    owner = store.resolve(target)
    if owner.engine != "codex":
        from ccx.store.identity import EngineMismatch
        raise EngineMismatch("Delivery requires a Codex worker handle")
    return owner


def _key(delivery_id: str) -> str:
    return hashlib.sha256(delivery_id.encode("utf-8")).hexdigest()


def receipt_path(target: str, delivery_id: str) -> Path:
    """Projection for diagnostics only; never opened by actual receipt I/O."""
    store = _store()
    owner = _owner(store, target)
    return (store.pointer.generation_root / "semantic-deliveries"
            / "codex" / owner.worker_id / (_key(delivery_id) + ".json"))


def _prompt_identity(text: str) -> tuple[str, int]:
    raw = text.encode("utf-8")
    return hashlib.sha256(raw).hexdigest(), len(raw)


@contextlib.contextmanager
def receipt_lock(short: str, delivery_id: str) -> Iterator[None]:
    """Original Codex per-delivery lock on a stable full-UUID generation inode."""
    from ccx.store.root import _flock_at
    if not valid_delivery_id(delivery_id):
        raise DeliveryReceiptError("invalid delivery id")
    store = _store()
    owner = _owner(store, short)
    with store.lease():
        with store._gen("semantic-deliveries", "codex",
                        owner.worker_id, create=True) as fd:
            with _flock_at(fd, _key(delivery_id) + ".lock",
                           exclusive=True, create=True, timeout=.5):
                yield


def _validate(value: Any, *, short: str, delivery_id: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise DeliveryReceiptError("Codex send receipt has invalid shape")
    if value.get("schemaVersion") != SCHEMA_VERSION:
        raise DeliveryReceiptError("unsupported Codex send receipt version")
    store = _store()
    owner = _owner(store, short)
    if (value.get("workerId") != owner.worker_id
            or value.get("engine") != "codex"
            or value.get("generation") != store.generation
            or value.get("deliveryId") != delivery_id):
        raise DeliveryReceiptError("Codex delivery belongs to another worker; check its ID")
    if value.get("state") not in STATES:
        raise DeliveryReceiptError("Codex send receipt invalid state")
    return value


def read_receipt(short: str, delivery_id: str) -> dict[str, Any] | None:
    from ccx.store.root import _read_json
    if not valid_delivery_id(delivery_id):
        raise DeliveryReceiptError("invalid delivery id")
    store = _store()
    owner = _owner(store, short)
    with store.lease():
        try:
            with store._gen("semantic-deliveries", "codex",
                            owner.worker_id) as fd:
                value = _read_json(fd, _key(delivery_id) + ".json")
        except FileNotFoundError:
            value = None
    return _validate(value, short=short, delivery_id=delivery_id) if value is not None else None


def _write(short: str, delivery_id: str, value: dict[str, Any]) -> None:
    from ccx.store.root import _write_json
    store = _store()
    owner = _owner(store, short)
    if "workerShort" in value:
        raise DeliveryReceiptError("legacy workerShort authority forbidden")
    value["updatedAtMs"] = now_ms()
    value["workerId"] = owner.worker_id
    value["engine"] = "codex"
    value["generation"] = store.generation
    with store.lease():
        with store._gen("semantic-deliveries", "codex",
                        owner.worker_id, create=True) as fd:
            _write_json(fd, _key(delivery_id) + ".json", value)


def requested_mode(entry: dict[str, Any]) -> str:
    """Old receipts predate modes and are strict new-turn deliveries."""
    value = entry.get("requestedMode")
    return value if value in MODES else "new_turn_only"


def intent_spec(entry: dict[str, Any]) -> dict[str, Any]:
    """Immutable pre-route native identity: auto/steer choose their client id later."""
    mode = requested_mode(entry)
    return {
        "deliveryId": entry["deliveryId"],
        "promptSha256": entry["promptSha256"],
        "promptByteCount": entry["promptByteCount"],
        "requestedMode": mode,
        "callerMetadata": entry.get("callerMetadata"),
        "cap": entry.get("cap"),
        "clientUserMessageId": (
            entry["clientUserMessageId"] if mode == "new_turn_only" else None
        ),
    }


def route(entry: dict[str, Any]) -> str | None:
    value = entry.get("route")
    if value in ROUTES:
        return value
    # A submitted/accepted old receipt could only have opened a new turn.
    if entry.get("state") in {"submitted", "accepted"}:
        return "new_turn"
    return None


def placement(entry: dict[str, Any]) -> str | None:
    value = entry.get("placement")
    if value in PLACEMENTS:
        return value
    if entry.get("state") == "accepted":
        return "current_turn" if route(entry) == "steer" else "new_turn"
    return None


def turn_ref_for(turn_id: str | None) -> str | None:
    if not isinstance(turn_id, str) or not turn_id:
        return None
    return f"ccx-turn:codex:v1:{turn_id}"


def _assert_semantics(
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
        or requested_mode(entry) != mode
        or metadata_conflict
        or entry.get("cap") != cap
    ):
        raise DeliveryReceiptConflict(
            "delivery_id is already owned by different prompt content, mode, "
            "caller metadata, or cap"
        )


def prepare(
    short: str,
    delivery_id: str,
    text: str,
    *,
    mode: str = "new_turn_only",
    caller_metadata: Any = _CALLER_METADATA_UNSET,
    cap: dict | None = None,
) -> tuple[dict[str, Any], bool]:
    if not valid_delivery_id(delivery_id):
        raise DeliveryReceiptError(
            "delivery_id must be a non-empty string of at most 128 characters"
        )
    if not isinstance(text, str) or not text.strip():
        raise DeliveryReceiptError("prompt text is required")
    if mode not in MODES:
        raise DeliveryReceiptError(f"unsupported delivery mode: {mode!r}")
    with receipt_lock(short, delivery_id):
        prior = read_receipt(short, delivery_id)
        if prior is not None:
            _assert_semantics(
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
            "workerId": _owner(_store(), short).worker_id,
            "engine": "codex",
            "generation": _store().generation,
            "deliveryId": delivery_id,
            "promptSha256": digest,
            "promptByteCount": byte_count,
            "requestedMode": mode,
            "cap": cap,
            "callerMetadata": (
                None
                if caller_metadata is _CALLER_METADATA_UNSET
                else caller_metadata
            ),
            # Preserve the landed strict-send contract: new_turn_only already
            # knows its only possible provenance kind at prepare time. Auto must
            # defer until codexd freezes start versus steer.
            "clientUserMessageId": (
                new_client_message_id(_owner(_store(), short).worker_id, "start")
                if mode == "new_turn_only"
                else None
            ),
            "state": "prepared",
            "route": None,
            "placement": None,
            "targetTurnId": None,
            "turnRef": None,
            "ownedAtMs": None,
            "createdAtMs": created,
            "updatedAtMs": created,
            "submittedAtMs": None,
            "acceptedAtMs": None,
            "rejectedAtMs": None,
            "turnId": None,
            "rejectionCode": None,
            "rejectionMessage": None,
            "lastRefusalCode": None,
            "lastRefusalMessage": None,
            "lastRefusalState": None,
            "lastRefusalAtMs": None,
        }
        _write(short, delivery_id, value)
        return dict(value), True


def require_matching(
    short: str,
    delivery_id: str,
    text: str,
    *,
    mode: str = "new_turn_only",
    caller_metadata: Any = _CALLER_METADATA_UNSET,
    cap: dict | None = None,
) -> dict[str, Any]:
    value = read_receipt(short, delivery_id)
    if value is None:
        raise DeliveryReceiptError(
            "Codex send receipt is missing; prepare the delivery before submission"
        )
    _assert_semantics(
        value,
        text,
        mode,
        caller_metadata=caller_metadata,
        cap=cap,
    )
    return value


def _transition(
    short: str,
    delivery_id: str,
    allowed: set[str],
    state: str,
    **patch: Any,
) -> dict[str, Any]:
    with receipt_lock(short, delivery_id):
        value = read_receipt(short, delivery_id)
        if value is None:
            raise DeliveryReceiptError(f"unknown Codex delivery: {delivery_id}")
        current = str(value.get("state"))
        if current == state:
            return value
        if current not in allowed:
            raise DeliveryReceiptError(
                f"invalid Codex delivery transition {current!r} -> {state!r}"
            )
        updated = {**value, **patch, "state": state}
        _write(short, delivery_id, updated)
        return updated


def mark_owned(
    short: str,
    delivery_id: str,
    *,
    route_kind: str,
    target_turn_id: str | None = None,
) -> dict[str, Any]:
    if route_kind not in ROUTES:
        raise DeliveryReceiptError(f"invalid delivery route: {route_kind!r}")
    if route_kind == "steer" and not target_turn_id:
        raise DeliveryReceiptError("steer route requires target turn id")
    with receipt_lock(short, delivery_id):
        value = read_receipt(short, delivery_id)
        if value is None:
            raise DeliveryReceiptError(f"unknown Codex delivery: {delivery_id}")
        current = value.get("state")
        if current in {"owned", "submitted", "accepted"}:
            if route(value) != route_kind:
                raise DeliveryReceiptConflict("owned delivery route mismatch")
            if route_kind == "steer" and value.get("targetTurnId") != target_turn_id:
                raise DeliveryReceiptConflict("owned delivery target-turn mismatch")
            return value
        if current != "prepared":
            return value
        existing_client = value.get("clientUserMessageId")
        client_id = (
            existing_client
            if isinstance(existing_client, str) and existing_client
            else new_client_message_id(
                _owner(_store(), short).worker_id,
                "steer" if route_kind == "steer" else "start"
            )
        )
        updated = {
            **value,
            "state": "owned",
            "route": route_kind,
            "targetTurnId": target_turn_id,
            "clientUserMessageId": client_id,
            "ownedAtMs": now_ms(),
            "lastRefusalCode": None,
            "lastRefusalMessage": None,
            "lastRefusalState": None,
            "lastRefusalAtMs": None,
        }
        _write(short, delivery_id, updated)
        return updated


def rollback_retryable(
    short: str,
    delivery_id: str,
    *,
    code: str,
    message: str,
    state: str | None = None,
) -> dict[str, Any]:
    """Return an explicitly non-mutated owned/submitted delivery to prepared."""
    with receipt_lock(short, delivery_id):
        value = read_receipt(short, delivery_id)
        if value is None:
            raise DeliveryReceiptError(f"unknown Codex delivery: {delivery_id}")
        if value.get("state") not in {"owned", "submitted"}:
            return value
        updated = _prepared_again(value, code=code, message=message, state=state)
        _write(short, delivery_id, updated)
        return updated


def _prepared_again(
    value: dict[str, Any], *, code: str, message: str, state: str | None
) -> dict[str, Any]:
    return {
        **value,
        "state": "prepared",
        "route": None,
        "placement": None,
        "targetTurnId": None,
        "turnRef": None,
        "clientUserMessageId": None,
        "ownedAtMs": None,
        "submittedAtMs": None,
        "submittedRuntimeId": None,
        "lastRefusalCode": code,
        "lastRefusalMessage": message,
        "lastRefusalState": state,
        "lastRefusalAtMs": now_ms(),
    }


def mark_submitted(
    short: str, delivery_id: str, *, runtime_id: str | None = None
) -> dict[str, Any]:
    """Freeze the delivery immediately before its native request is written.

    ``runtime_id`` names the codexd lifetime (and so the one app-server
    connection) that may still complete the request. It is diagnostic
    provenance only: a vanished runtime never proves the request did not land.
    """
    with receipt_lock(short, delivery_id):
        value = read_receipt(short, delivery_id)
        if value is None:
            raise DeliveryReceiptError(f"unknown Codex delivery: {delivery_id}")
        current = value.get("state")
        if current == "submitted":
            return value
        if current == "prepared":
            if requested_mode(value) != "new_turn_only":
                raise DeliveryReceiptError(
                    "Message has no destination; wait for a worker turn"
                )
            client_id = value.get("clientUserMessageId")
            if not isinstance(client_id, str) or not client_id:
                client_id = new_client_message_id(_owner(_store(), short).worker_id, "start")
            value = {
                **value,
                "state": "owned",
                "route": "new_turn",
                "targetTurnId": None,
                "clientUserMessageId": client_id,
                "ownedAtMs": now_ms(),
            }
        if value.get("state") != "owned":
            raise DeliveryReceiptError(
                f"invalid Codex delivery transition {value.get('state')!r} -> 'submitted'"
            )
        updated = {
            **value,
            "state": "submitted",
            "submittedAtMs": now_ms(),
            "submittedRuntimeId": runtime_id,
            "lastRefusalCode": None,
            "lastRefusalMessage": None,
            "lastRefusalState": None,
            "lastRefusalAtMs": None,
        }
        _write(short, delivery_id, updated)
        return updated


def mark_retryable_refusal(
    short: str,
    delivery_id: str,
    *,
    code: str,
    message: str,
    state: str | None = None,
) -> dict[str, Any]:
    """Persist one definitive pre-submission refusal without burning the id."""
    with receipt_lock(short, delivery_id):
        value = read_receipt(short, delivery_id)
        if value is None:
            raise DeliveryReceiptError(f"unknown Codex delivery: {delivery_id}")
        if value.get("state") != "prepared":
            return value
        updated = {
            **value,
            "lastRefusalCode": code,
            "lastRefusalMessage": message,
            "lastRefusalState": state,
            "lastRefusalAtMs": now_ms(),
        }
        _write(short, delivery_id, updated)
        return updated


def mark_accepted(
    short: str, delivery_id: str, *, turn_id: str
) -> dict[str, Any]:
    if not isinstance(turn_id, str) or not turn_id:
        raise DeliveryReceiptError("Accepted Codex message has no turn ID; inspect the delivery")
    with receipt_lock(short, delivery_id):
        value = read_receipt(short, delivery_id)
        if value is None:
            raise DeliveryReceiptError(f"unknown Codex delivery: {delivery_id}")
        if value.get("state") == "accepted":
            if value.get("turnId") != turn_id:
                raise DeliveryReceiptConflict("accepted delivery turn mismatch")
            return value
        if value.get("state") != "submitted":
            raise DeliveryReceiptError(
                f"cannot accept Codex delivery from state={value.get('state')!r}"
            )
        actual_route = route(value) or "new_turn"
        updated = {
            **value,
            "state": "accepted",
            "acceptedAtMs": now_ms(),
            "turnId": turn_id,
            "placement": "current_turn" if actual_route == "steer" else "new_turn",
            "turnRef": turn_ref_for(turn_id),
            "rejectionCode": None,
            "rejectionMessage": None,
        }
        _write(short, delivery_id, updated)
        return updated


def mark_settled(
    short: str, delivery_id: str, *, turn_id: str,
    original_positive_completion: dict[str, Any],
) -> dict[str, Any]:
    """Immutably attach original native turn/completed evidence to accepted ID."""
    from ccx.store.root import _HASH
    if (not isinstance(original_positive_completion, dict)
            or original_positive_completion.get("turnId") != turn_id
            or original_positive_completion.get("status") != "completed"
            or not isinstance(original_positive_completion.get("text"), str)
            or not isinstance(original_positive_completion.get("nativeEvidenceSha256"), str)
            or not _HASH.fullmatch(original_positive_completion["nativeEvidenceSha256"])):
        raise DeliveryReceiptError("Codex turn completion is unconfirmed; inspect the delivery")
    with receipt_lock(short, delivery_id):
        value = read_receipt(short, delivery_id)
        if (value is None or value.get("state") != "accepted"
                or value.get("turnId") != turn_id):
            raise DeliveryReceiptError("Codex turn was not accepted; inspect the delivery")
        prior = value.get("settlement")
        if prior is not None:
            # Live item/completed and source-fenced thread/read can render a
            # different answer (and therefore a different evidence digest) for
            # the very same completed native turn. The first proof is immutable;
            # only a different original identity is a conflicting observation.
            identity = ("workerId", "engine", "nativeSource", "threadId",
                        "turnId", "deliveryId", "status")
            if (not isinstance(prior, dict) or any(
                prior.get(field) != original_positive_completion.get(field)
                for field in identity
            )):
                raise DeliveryReceiptConflict("Codex completion record changed; inspect the worker")
            return value
        result = dict(value)
        result["settlement"] = dict(original_positive_completion)
        result["settledAtMs"] = now_ms()
        _write(short, delivery_id, result)
        return result


def mark_rejected(
    short: str,
    delivery_id: str,
    *,
    code: str,
    message: str,
) -> dict[str, Any]:
    return _transition(
        short,
        delivery_id,
        {"prepared", "submitted"},
        "rejected",
        rejectedAtMs=now_ms(),
        rejectionCode=code,
        rejectionMessage=message,
    )


def _receipt_rows(short: str, *, strict: bool) -> Iterator[dict[str, Any]]:
    """Reuse original Codex integrity distinction: status tolerates damage,
    native-turn ownership decisions must see and refuse unrelated corruption.
    """
    from ccx.store.root import _read_json, StorageIntegrityError
    store = _store()
    owner = _owner(store, short)
    with store.lease():
        try:
            with store._gen("semantic-deliveries", "codex",
                            owner.worker_id) as fd:
                with os.scandir(fd) as entries:
                    names = sorted(entry.name for entry in entries
                                   if entry.name.endswith(".json"))
                if len(names) > 100000:
                    raise DeliveryReceiptError("Too many Codex delivery records; narrow the request")
                rows = []
                for name in names:
                    try:
                        rows.append(_read_json(fd, name))
                    except StorageIntegrityError as exc:
                        if strict:
                            raise DeliveryReceiptError(
                                "Cannot read Codex turn record "
                                + name
                            ) from exc
        except FileNotFoundError:
            rows = []
    for value in rows:
        try:
            if not isinstance(value, dict):
                raise DeliveryReceiptError("Codex semantic receipt invalid shape")
            delivery_id = value.get("deliveryId")
            if not valid_delivery_id(delivery_id):
                raise DeliveryReceiptError("Codex delivery ID is invalid; use the saved delivery ID")
            yield _validate(value, short=short, delivery_id=delivery_id)
        except DeliveryReceiptError:
            if strict:
                raise


def iter_receipts(short: str) -> Iterator[dict[str, Any]]:
    """Original best-effort status listing; no native ownership decisions."""
    yield from _receipt_rows(short, strict=False)


def receipt_for_turn(short: str, turn_id: str) -> dict[str, Any] | None:
    """Resolve one native turn to its semantic delivery without hiding damage.

    General receipt enumeration is intentionally best-effort so status/reporting
    can survive an unrelated corrupt sidecar. Native-turn reconciliation is an
    identity decision, though: silently skipping corruption there could erase
    delivery provenance or conceal a second receipt claiming the same turn.
    """
    matches = [
        value for value in _receipt_rows(short, strict=True)
        if value.get("turnId") == turn_id
    ]
    # Steered deliveries legitimately share the turn they joined, so only the
    # delivery that opened the turn identifies it; two openers is damage.
    openers = [value for value in matches if route(value) != "steer"]
    if len(openers) > 1:
        raise DeliveryReceiptError(
            f"multiple Codex deliveries claim to have opened native turn {turn_id}"
        )
    if openers:
        return openers[0]
    # Opened by input without a receipt (a raw send or a parent notification):
    # the first delivery steered into it is the earliest semantic evidence.
    matches.sort(key=lambda value: (value.get("acceptedAtMs") or 0,
                                    str(value.get("deliveryId"))))
    return matches[0] if matches else None


def public_receipt(value: dict[str, Any] | None) -> dict[str, Any] | None:
    if value is None:
        return None
    state = value.get("state")
    mode = requested_mode(value)
    actual_route = route(value)
    actual_placement = placement(value)
    turn_id = value.get("turnId")
    if state in {"prepared", "owned"}:
        action = "retry_same_id"
    elif state == "submitted":
        action = "wait"
    else:
        action = "none"
    return {
        "delivery_id": value.get("deliveryId"),
        # Detailed durable receipt state; The public protocol normalizes its own
        # top-level reconciliation vocabulary separately.
        "status": state,
        "requested_mode": mode,
        "caller_metadata": value.get("callerMetadata"),
        "cap": value.get("cap"),
        "owned": state in {"owned", "submitted", "accepted"},
        "recommended_action": action,
        "route": actual_route,
        "placement": actual_placement,
        "target_turn_id": value.get("targetTurnId"),
        "turn_ref": value.get("turnRef") or (
            turn_ref_for(turn_id) if state == "accepted" else None
        ),
        "turn_id": turn_id,
        "client_user_message_id": value.get("clientUserMessageId"),
        "created_at_ms": value.get("createdAtMs"),
        "updated_at_ms": value.get("updatedAtMs"),
        "owned_at_ms": value.get("ownedAtMs"),
        "submitted_at_ms": value.get("submittedAtMs"),
        "accepted_at_ms": value.get("acceptedAtMs"),
        "settled_at_ms": value.get("settledAtMs"),
        "settlement": value.get("settlement"),
        "rejected_at_ms": value.get("rejectedAtMs"),
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
        "pending": state in {"prepared", "owned", "submitted"},
    }



__all__ = [
    "DeliveryReceiptConflict",
    "DeliveryReceiptError",
    "MAX_DELIVERY_ID",
    "TERMINAL_STATES",
    "iter_receipts",
    "mark_accepted",
    "mark_owned",
    "mark_rejected",
    "mark_retryable_refusal",
    "rollback_retryable",
    "mark_submitted",
    "new_delivery_id",
    "prepare",
    "placement",
    "public_receipt",
    "requested_mode",
    "route",
    "read_receipt",
    "receipt_for_turn",
    "require_matching",
    "turn_ref_for",
    "valid_delivery_id",
]
