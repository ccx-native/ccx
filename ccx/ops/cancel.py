from __future__ import annotations
import time
from typing import Any
from ccx.errors import (
    DeliveryRejected,
    InvalidRequest,
    OperationConflict,
    SessionUnavailable,
)
from ccx.ops.common import split_target
from ccx.ops.wait import reconcile_turn


def _cancel_live_turn(handle: str, engine: str, worker_id: str,
                      *, timeout: float = 0.5) -> tuple[bool, str | None]:
    """Authenticated exact active turn, or UNKNOWN (never inferred from absence)."""
    if engine == "claude":
        from ccx.rpc.rpc import _rpc
        reply = _rpc(handle, {"op": "status"}, timeout=timeout)
        if reply.get("ok") is True and "activeTurnRef" in reply:
            return True, reply["activeTurnRef"]
        return False, None
    from ccx.codex.runtime import daemon_rpc
    from ccx.constants import CODEXD_RPC_GENERATION
    reply = daemon_rpc({"op": "status", "short": worker_id,
                        "rpcGeneration": CODEXD_RPC_GENERATION}, timeout=timeout)
    if (reply.get("ok") is True and reply.get("loaded") is True
            and "activeTurnId" in reply):
        return True, ("ccx-turn:codex:v1:" + reply["activeTurnId"]
                      if isinstance(reply["activeTurnId"], str) else None)
    return False, None


def cancel_turn(
    target: str, *, cancellation_id: str, turn_ref: str,
    timeout: float | None = 90,
    expected_generation: str | None = None,
) -> dict[str, Any]:
    """Request one exact full-UUID turn interrupt, never replay its native write.

    protocol cancellation_id is caller-owned. The existing GenerationStore 'cancel'
    intent is the only ledger, independent of legacy short-keyed turn_cancel.
    A native ACK means requested, not that model activity stopped.
    """
    from math import isfinite
    from ccx import constants
    from ccx.store.root import NativeEffectIndeterminate, require_ccx_active
    from ccx.store.store import _digest
    from ccx.store.identity import canonical_worker_id
    from ccx.codex.delivery import receipt_for_turn as codex_receipt_for_turn

    if (not isinstance(cancellation_id, str) or not cancellation_id
            or len(cancellation_id) > 128 or "\x00" in cancellation_id):
        raise InvalidRequest("cancellation_id must be nonempty and <=128 characters")
    if (timeout is not None and (type(timeout) not in (int, float)
                                 or not isfinite(timeout) or timeout <= 0)):
        raise InvalidRequest("cancel timeout must be positive finite seconds")
    handle, engine, worker_id = split_target(target)
    canonical_worker_id(worker_id)
    prefix = f"ccx-turn:{engine}:v1:"
    if not isinstance(turn_ref, str) or not turn_ref.startswith(prefix) or not turn_ref[len(prefix):]:
        raise InvalidRequest("turn_ref must name an exact managed turn for the selected engine")
    native_turn_id = turn_ref[len(prefix):] if engine == "codex" else None
    store = require_ccx_active(constants.CCX_ROOT,
                               expected_generation=expected_generation)
    rec = store.record(handle)
    source = rec.get("native") or {}
    if (engine == "claude" and not isinstance(source.get("sessionId"), str)) or (
        engine == "codex" and (not isinstance(source.get("threadId"), str)
        or source.get("threadSource") != store.codex_source(handle))
    ):
        raise SessionUnavailable("original native worker binding is not verified")
    identity = _digest({"workerId": worker_id, "engine": engine,
                        "turnRef": turn_ref, "operation": "cancel"})
    prior = store.read_receipt(handle, "cancel", cancellation_id)
    if prior is not None and prior.get("intentSha256") != identity:
        raise OperationConflict("cancellation_id already names a different turn",
                                short=worker_id[:8], worker_id=worker_id)
    # In particular, an invented native id is never a cancel capability.
    if engine == "codex":
        opening = codex_receipt_for_turn(handle, native_turn_id)
    else:
        from ccx.claude.delivery import iter_receipts
        opening = next((row for row in iter_receipts(handle)
                        if row.get("turnRef") == turn_ref
                        and row.get("route") == "new_turn"), None)
    if prior is None and (not isinstance(opening, dict)
                          or opening.get("state") != "accepted"):
        # Human input on the shared managed Claude PTY starts a genuine parent
        # turn without a CCX delivery receipt. Do not fabricate one: the live
        # authenticated host's exact activeTurnRef is sufficient to request a
        # one-shot native Escape, under the same durable cancel intent. This
        # exception never applies to Codex or an absent/invented/stale turn.
        if engine != "claude":
            raise DeliveryRejected("exact active turn has no original accepted delivery receipt",
                                   short=worker_id[:8])
        known, active = _cancel_live_turn(handle, engine, worker_id)
        store.revalidate()
        if not known or active != turn_ref:
            raise DeliveryRejected("exact active turn has no original accepted delivery receipt",
                                   short=worker_id[:8])
    deadline = None if timeout is None else time.monotonic() + timeout

    def public(state: str, *, outcome=None, reason=None, owned=False,
               receipt=None) -> dict[str, Any]:
        return {
            "worker": {"worker": handle, "worker_id": worker_id,
                       "engine": engine, "short": worker_id[:8]},
            "cancellation_id": cancellation_id, "turn_ref": turn_ref,
            "status": state, "confirmed": state == "confirmed",
            "owned": owned, "accepted": bool(receipt and receipt.get("state") == "accepted"),
            "recommended_action": ("retry_same_id" if state == "prepared"
                                   else "wait" if state == "submitted" else "none"),
            "outcome": outcome, "reason": reason,
            "receipt": receipt,
        }

    def observe(receipt: dict) -> dict | None:
        # Original semantic receipt is the only proof of completed assistant
        # text. A changed live ref proves INACTIVITY, not why the turn stopped.
        def exact_claude_terminal() -> dict | None:
            last = store.record(handle).get("lastSettledTurn") or {}
            if (last.get("turnRef") == turn_ref
                    and last.get("outcome") in {"interrupted", "completed", "failed"}):
                return public("confirmed", outcome=last["outcome"],
                              owned=True, receipt=receipt)
            return None

        if engine == "codex":
            seen = reconcile_turn(handle, turn_id=native_turn_id,
                                  expected_generation=store.generation)
            if seen.get("status") == "settled":
                return public("confirmed", outcome="completed", owned=True,
                              receipt=receipt)
            if seen.get("native_status") in ("interrupted", "cancelled"):
                return public("confirmed", outcome="interrupted", owned=True,
                              receipt=receipt)
        else:
            from ccx.claude.delivery import read_receipt
            latest = read_receipt(handle, opening["deliveryId"]) if opening else None
            if (latest and latest.get("turnRef") == turn_ref
                    and isinstance(latest.get("settlement"), dict)):
                return public("confirmed", outcome="completed", owned=True,
                              receipt=receipt)
            # Positive completed-answer receipt wins above. Otherwise the
            # exact original host finalizer proves this turn's outcome for
            # both human and receipt-backed cancellations; it does not invent
            # an opening delivery receipt or completed assistant text.
            terminal = exact_claude_terminal()
            if terminal is not None:
                return terminal
        known, active = _cancel_live_turn(handle, engine, worker_id)
        store.revalidate()
        if known and active != turn_ref:
            if engine == "claude":
                # The terminal CAS can land between our first record read and
                # the host status RPC. Reread the same exact source before
                # falling back to unknown; never report an invented outcome.
                terminal = exact_claude_terminal()
                if terminal is not None:
                    return terminal
            return public("confirmed", outcome="unknown", owned=True,
                          receipt=receipt)
        return None

    # No preflight can replace the provider's expected-turn check, but an
    # observed IDLE/other turn can reject without any native effect at all.
    if prior is None or prior.get("state") == "prepared":
        store.authorize_native_action(handle, "cancel")
        known, active = _cancel_live_turn(handle, engine, worker_id)
        if not known:
            return public("prepared", reason="live active turn not proven")
        if active != turn_ref:
            return public("rejected", reason="active turn changed or no active turn")
        attempt = store.begin_native_intent(
            handle, family="cancel", action="cancel", operation_id=cancellation_id,
            intent_sha256=identity,
        )
        if attempt.replayed:
            prior = store.read_receipt(handle, "cancel", cancellation_id)
        else:
            from ccx.constants import CODEXD_RPC_GENERATION
            from ccx.codex.runtime import daemon_rpc
            from ccx.rpc.rpc import _rpc
            budget = min(2., max(.01, deadline - time.monotonic())) if deadline else 2.
            def submit(seconds):
                if engine == "claude":
                    return _rpc(handle, {"op": "interrupt",
                                         "expected_turn_ref": turn_ref}, timeout=seconds)
                return daemon_rpc({"op": "interrupt", "short": worker_id,
                                   "expectedTurnId": native_turn_id,
                                   "rpcGeneration": CODEXD_RPC_GENERATION}, timeout=seconds)
            try:
                reply = store.dispatch_native_intent_once(
                    attempt, bounded_submit=submit, dispatch_budget_s=budget)
            except NativeEffectIndeterminate:
                # Durable submitted-unknown; a same-ID caller only observes.
                prior = store.read_receipt(handle, "cancel", cancellation_id)
                return public("submitted", reason="original interrupt reply unknown",
                              owned=True, receipt=prior)
            if (reply.get("ok") is True and
                    (engine == "codex" and reply.get("turnId") == native_turn_id
                     or engine == "claude" and reply.get("turnRef") == turn_ref
                     and (reply.get("sent") is True or
                          (reply.get("sent") is False and
                           reply.get("already_observing") is True)))):
                # A prior same-turn Escape is accepted, never proof of completion.
                state = "accepted"
            elif reply.get("ok") is False and reply.get("definitive") is True:
                state = "rejected"
            else:
                prior = store.read_receipt(handle, "cancel", cancellation_id)
                return public("submitted", reason="native interrupt not positively acknowledged",
                              owned=True, receipt=prior)
            prior = store.settle_native_intent(
                attempt, state=state, native_evidence_sha256=_digest({
                    "workerId": worker_id, "turnRef": turn_ref, "reply": reply,
                }),
            )
            if state == "rejected":
                return public("rejected", reason=str(reply.get("error")), receipt=prior)
    if prior is None:
        prior = store.read_receipt(handle, "cancel", cancellation_id)
    if prior.get("state") == "rejected":
        return public("rejected", reason="original native interrupt refused", receipt=prior)
    # Acceptance and caller timeout do not mutate the durable native effect:
    # a later same-ID call may still observe terminality, but cannot re-dispatch.
    while True:
        store.revalidate()
        try:
            verified = observe(prior)
        except (SessionUnavailable, OSError):
            store.revalidate()  # changed ACTIVE is not a temporary daemon loss
            verified = None  # temporary runtime loss is NOT changed-turn proof
        if verified is not None:
            return verified
        if deadline is not None and time.monotonic() >= deadline:
            return public("unknown", reason="exact target terminality not proven",
                          owned=True, receipt=prior)
        time.sleep(.2 if deadline is None else
                   min(.2, max(0., deadline-time.monotonic())))


def stop_worker(
    target: str, *, timeout: float | None = 1.0,
    expected_turn_ref: str | None = None,
    expected_generation: str | None = None,
) -> dict[str, Any]:
    """One explicit stop request, fresh ID, exact ref; acceptance is not completion."""
    from uuid import uuid4
    from ccx import constants
    from ccx.errors import TurnIncomplete
    from ccx.store.root import require_ccx_active

    handle, engine, worker_id = split_target(target)
    store = require_ccx_active(constants.CCX_ROOT,
                               expected_generation=expected_generation)
    if expected_turn_ref is None:
        known, active = _cancel_live_turn(handle, engine, worker_id)
        store.revalidate()
        if not known:
            raise SessionUnavailable("stop: active turn could not be observed",
                                     short=worker_id[:8])
        if not active:
            raise DeliveryRejected("stop: worker has no active turn",
                                   short=worker_id[:8])
    else:
        active = expected_turn_ref  # cancel_turn validates the exact ref.
    result = cancel_turn(handle, cancellation_id=str(uuid4()),
                         turn_ref=active, timeout=timeout,
                         expected_generation=store.generation)
    if result["status"] == "rejected":
        raise DeliveryRejected(result.get("reason") or "active turn changed",
                               short=worker_id[:8],
                               operation_id=result["cancellation_id"])
    if result["status"] == "prepared":
        raise SessionUnavailable("stop: original interrupt not submitted",
                                 short=worker_id[:8])
    if not result.get("accepted"):
        error = TurnIncomplete(
            "stop: native interrupt acceptance unconfirmed; "
            "reconcile the original cancellation ID, do not repeat it",
            short=worker_id[:8], turn_status=result["status"],
        )
        error.cancellation_id = result["cancellation_id"]
        error.turn_ref = result["turn_ref"]
        error.worker_id = worker_id
        raise error
    return result
