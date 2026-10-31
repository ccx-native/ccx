"""Recover a Codex delivery left ``submitted`` from exact native evidence.

Only the V2 caller writes ``accepted``, after codexd's answer. When that
answer is lost (caller budget expired, native request timed out, caller
killed), the receipt would stay ``submitted`` although the input landed. The
native ``userMessage`` item carries the ``clientId`` CCX minted for exactly
this delivery, and codexd journals it before reducing it. That item, and
nothing else, is positive evidence of placement.

One resolver, two callers: codexd, when it journals a matching user message
(off its app-server reader thread), and ``reconcile_turn``, for recovery after
a restart. Acceptance goes through the receipt's own ``mark_accepted`` and the
global native intent's ``settle_native_intent`` with the very evidence digest
codexd's acknowledgment carries, so a late acknowledgment and this resolver
converge on one immutable answer in either order.

Missing, ambiguous, foreign or degraded evidence changes nothing: the receipt
stays ``submitted`` (unknown). Absence never proves rejection, and nothing
here ever sends anything native.
"""
from __future__ import annotations

import contextlib
from typing import Any

from ccx.codex import delivery as codex_delivery
from ccx.codex import evidence as codex_evidence
from ccx.codex.provenance import parse_client_message_id

USER_MESSAGE = "userMessage"
_ITEM_METHODS = frozenset({"item/started", "item/completed"})


def ccx_client_id(item: Any, worker_id: str) -> str | None:
    """The CCX-minted v2 client id of this worker's native user message, else None."""
    if not isinstance(item, dict) or item.get("type") != USER_MESSAGE:
        return None
    client_id = item.get("clientId")
    parsed = parse_client_message_id(client_id)
    if not parsed or parsed.get("workerId") != worker_id:
        return None
    return client_id


def _placement(read: codex_evidence.JournalRead, client_id: str,
               thread_id: str) -> str | None:
    """The one native turn the exact client id was placed in, or None.

    Every journaled item carrying the id must be the same user-message item
    of the same turn on the bound thread; anything else is ambiguous.
    """
    seen: set[tuple[Any, Any]] = set()
    for event in read.events:
        if event.get("method") not in _ITEM_METHODS:
            continue
        params = event.get("params")
        item = params.get("item") if isinstance(params, dict) else None
        if not isinstance(item, dict) or item.get("clientId") != client_id:
            continue
        if (item.get("type") != USER_MESSAGE
                or event.get("threadId") != thread_id
                or not isinstance(event.get("turnId"), str)
                or not event["turnId"]):
            return None
        seen.add((event["turnId"], event.get("itemId")))
    if len(seen) != 1:
        return None
    (turn_id, item_id), = seen
    return turn_id if isinstance(item_id, str) and item_id else None


def recover(store, handle: str, delivery_id: str, *,
            journal: codex_evidence.JournalRead | None = None) -> dict | None:
    """Accept a ``submitted`` receipt only from exact journaled placement.

    Also completes an ``accepted`` receipt whose global native intent was left
    ``submitted-unknown`` (a caller that died between the two writes), from
    the same evidence. Returns the current receipt (None if there is none).
    Receipt conflicts raise; a busy native-intent lock leaves the intent for
    a later call and still returns the accepted receipt.
    """
    owner = store.resolve(handle)
    if owner.engine != "codex":
        return None
    # Pin the original generation across every read and bounded local write:
    # a cooperating cutover needs LOCK_EX on the activation lock this shared
    # lease holds, and the receipt helpers resolve the CURRENT pointer, so it
    # must be this store's. No native call happens under it.
    with _pinned(store):
        return _recover_pinned(store, owner, delivery_id, journal)


@contextlib.contextmanager
def _pinned(store):
    from ccx import constants
    from ccx.store.root import StaleGeneration, require_ccx_active
    with store.lease():
        current = require_ccx_active(constants.CCX_ROOT,
                                     expected_generation=store.generation)
        if current.root != store.root:
            raise StaleGeneration("recovery store is not the active root")
        yield


def _recover_pinned(store, owner, delivery_id: str,
                    journal: codex_evidence.JournalRead | None) -> dict | None:
    from ccx.store.root import GenerationBusy
    from ccx.store.store import OperationIntent, _digest
    receipt = codex_delivery.read_receipt(owner.worker, delivery_id)
    if not isinstance(receipt, dict) or receipt.get("state") not in ("submitted", "accepted"):
        return receipt
    effect = store.read_receipt(owner.worker, "codex-send", delivery_id)
    if (not isinstance(effect, dict)
            or effect.get("action") != "send"
            or effect.get("workerId") != owner.worker_id
            or effect.get("engine") != "codex"
            or effect.get("generation") != store.generation
            or effect.get("intentSha256") != _digest(codex_delivery.intent_spec(receipt))
            or effect.get("state") not in ("submitted-unknown", "accepted")):
        return receipt
    if receipt["state"] == "accepted" and effect["state"] == "accepted":
        return receipt  # Both terminal: nothing to write, nothing asserted.

    route = codex_delivery.route(receipt)
    client_id = receipt.get("clientUserMessageId")
    target = receipt.get("targetTurnId")
    parsed = parse_client_message_id(client_id)
    if (route not in codex_delivery.ROUTES
            or not parsed or parsed.get("workerId") != owner.worker_id
            or parsed.get("kind") != ("steer" if route == "steer" else "start")
            or (route == "steer" and (not isinstance(target, str) or not target))
            or (route == "new_turn" and target is not None)):
        return receipt

    record = store.record(owner.worker)
    native = record.get("native") or {}
    thread_id = native.get("threadId")
    activation = store.native_activation(owner.worker)
    # The confirmed activation's source, not codex_source(): that one also
    # demands send capability, and recovery is a read of past evidence that
    # must work for a worker stopped since.
    source = activation.get("source")
    binding = activation.get("nativeBinding") or {}
    if (not isinstance(thread_id, str) or not thread_id
            or not isinstance(source, str) or not source
            or native.get("threadSource") != source
            or activation.get("state") != "confirmed"
            or not isinstance(binding, dict)
            or binding.get("threadId") != thread_id):
        return receipt

    read = journal if journal is not None else codex_evidence.read_journal(owner.worker_id)
    health = record.get("evidence")
    status, _warnings = codex_evidence._merged_health(
        read, health if isinstance(health, dict) else None)
    if status != "complete":
        return receipt  # Degraded evidence never decides.
    turn_id = _placement(read, client_id, thread_id)
    if turn_id is None:
        return receipt
    if route == "steer" and turn_id != target:
        return receipt
    if receipt["state"] == "accepted" and receipt.get("turnId") != turn_id:
        return receipt
    if route == "new_turn":
        try:
            opener = codex_delivery.receipt_for_turn(owner.worker, turn_id)
        except codex_delivery.DeliveryReceiptError:
            return receipt  # Damaged receipts never let evidence pick a side.
        if (isinstance(opener, dict) and opener.get("deliveryId") != delivery_id
                and codex_delivery.route(opener) != "steer"):
            return receipt

    # Exactly the digest codexd's native-send acknowledgment carries.
    evidence = _digest({
        "source": source, "threadId": thread_id, "turnId": turn_id,
        "deliveryId": delivery_id, "route": route, "targetTurnId": target,
        "clientUserMessageId": client_id,
    })
    if (effect["state"] == "accepted"
            and effect.get("nativeEvidenceSha256") != evidence):
        # An immutable terminal global proof of something else (or of
        # nothing valid): never fabricate a consistent answer over it.
        return receipt
    store.revalidate()
    accepted = codex_delivery.mark_accepted(owner.worker, delivery_id, turn_id=turn_id)
    if effect["state"] == "submitted-unknown":
        try:
            store.settle_native_intent(OperationIntent(
                owner.worker_id, "codex", "codex-send", delivery_id,
                effect["intentSha256"], "submitted-unknown", store.generation,
            ), state="accepted", native_evidence_sha256=evidence)
        except GenerationBusy:
            # The dispatching caller still holds this intent; it settles it
            # with the same digest, or a later call does.
            return accepted
    if route == "new_turn" and accepted.get("settlement") is None:
        from ccx.codex.completion import settle_from_journal
        try:
            settled = settle_from_journal(store, owner.worker, turn_id)
            if settled is not None:
                accepted = settled
        except Exception:
            pass  # Accepted placement stands; settlement has its own recovery.
    return codex_delivery.read_receipt(owner.worker, delivery_id)


def recover_for_turn(store, handle: str, turn_id: str) -> None:
    """Before a turn is called foreign: recover a submitted delivery placed in it."""
    owner = store.resolve(handle)
    if owner.engine != "codex":
        return
    with _pinned(store):
        pending = [value for value in codex_delivery.iter_receipts(owner.worker)
                   if value.get("state") == "submitted"
                   and codex_delivery.route(value) == "new_turn"]
        if not pending:
            return
        read = codex_evidence.read_journal(owner.worker_id)
        for value in pending:
            _recover_pinned(store, owner, value["deliveryId"], read)


def delivery_for_client(handle: str, client_id: str) -> str | None:
    """The delivery that minted this exact client id (submitted or accepted)."""
    matches = [value.get("deliveryId") for value in codex_delivery.iter_receipts(handle)
               if value.get("clientUserMessageId") == client_id
               and value.get("state") in ("submitted", "accepted")]
    return matches[0] if len(matches) == 1 else None


__all__ = ["ccx_client_id", "delivery_for_client", "recover", "recover_for_turn"]
