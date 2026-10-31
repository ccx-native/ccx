from __future__ import annotations
import time
from typing import Any
from ccx.ops import turn_reports
from ccx.notify.registry import public_registration as public_parent_notification, read_registration as read_parent_notification
from ccx.errors import (
    CapabilityUnsupported,
    InvalidRequest,
    SessionNotFound,
    SessionUnavailable,
)
from ccx.rpc.rpc import (
    _rpc as claude_host_rpc,
)
from ccx.ops.common import split_target


def park_worker(
    target: str, *, timeout: float | None = 90,
    expected_generation: str | None = None,
    _respawn_lock_held: bool = False,
) -> dict[str, Any]:
    """Confirmed teardown of one exact full-UUID runtime, native history kept."""
    from ccx import constants
    from ccx.store.root import require_ccx_active
    from ccx.ops.terminate import confirmed_kill_uuid
    handle, _engine, _worker_id = split_target(target)
    store = require_ccx_active(constants.CCX_ROOT,
                               expected_generation=expected_generation)
    return confirmed_kill_uuid(
        store, handle, timeout=timeout,
        _respawn_lock_held=_respawn_lock_held,
    )


def inspect_notification(event_id: str) -> dict[str, Any]:
    """Read the original notification's rich protocol projection, without delivery."""
    from ccx.notify.client import _event_id
    from ccx.notify.paths import registration_path

    event_id = _event_id(event_id)
    value = read_parent_notification(event_id)
    if value is None:
        # The existing reader returns None for both missing and damaged JSON.
        # One path check distinguishes the two without another parse/registry.
        if registration_path(event_id).exists():
            raise SessionUnavailable(f"notification registration is unreadable or corrupt: {event_id}")
        raise SessionNotFound(f"no such notification registration: {event_id}")
    if value.get("eventId") != event_id:
        raise SessionUnavailable(f"notification registration event ID differs from requested ID: {event_id}")
    public = public_parent_notification(value)
    operation = (
        value.get("operation")
        if isinstance(value.get("operation"), dict)
        else {}
    )
    return {
        **public,
        "source": value.get("source"),
        "attempts": int(value.get("attempts") or 0),
        "last_attempt_at_ms": value.get("lastAttemptAtMs"),
        "last_error": value.get("lastError"),
        "delivered_at_ms": value.get("deliveredAtMs"),
        "cancel_reason": value.get("cancelReason"),
        "caller_metadata": operation.get("callerMetadata"),
        "settlement": (
            value.get("settlement")
            if isinstance(value.get("settlement"), dict)
            else None
        ),
        "payload": (
            value.get("payload")
            if isinstance(value.get("payload"), dict)
            else None
        ),
        "delivery": (
            value.get("delivery")
            if isinstance(value.get("delivery"), dict)
            else None
        ),
        "operation": {
            "kind": operation.get("kind"),
            "delivery_id": operation.get("deliveryId"),
            "turn_id": operation.get("turnId"),
            "resume_waiting_request_id": operation.get(
                "resumeWaitingRequestId"
            ),
            "caller_metadata": operation.get("callerMetadata"),
        },
    }


def wait_worker(
    target: str, *, timeout: float | None = None,
    next_turn: bool = False, after_turn: str | None = None,
    known_input: str | None = None, expected_generation: str | None = None,
) -> dict[str, Any]:
    """Observe an exact full-UUID worker, never starting or replaying a turn.

    A stable-state or new-turn wait returns verdict data; the separate
    wait_delivery observes one original semantic receipt and raises on timeout.
    """
    from math import isfinite
    from ccx.codex import runtime as codex_runtime
    from ccx import constants
    from ccx.codex.read import pending_input as codex_pending
    from ccx.constants import CODEXD_RPC_GENERATION
    from ccx.store.liveness import owned_process
    from ccx.store.root import GenerationError, require_ccx_active
    from ccx.workers import _claude_pending

    if (timeout is not None and (
        type(timeout) not in (int, float) or not isfinite(timeout) or timeout < 0
    )):
        raise InvalidRequest("wait timeout must be finite and nonnegative")
    if after_turn is not None and not next_turn:
        raise InvalidRequest("after_turn requires next_turn=true")
    if known_input is not None:
        if not next_turn or not isinstance(known_input, str) or not known_input:
            raise InvalidRequest("known_input requires next_turn and a nonempty request id")
    handle, engine, worker_id = split_target(target)
    if engine == "codex" and next_turn:
        selector = ("wait_known_input" if known_input is not None else
                    "wait_after_turn" if after_turn is not None else "wait_next_turn")
        raise CapabilityUnsupported("Codex next-turn wait is not supported",
                                    engine=engine, capability=selector)
    if after_turn is not None:
        turn_reports._ref_id(after_turn, engine="claude")
    try:
        store = require_ccx_active(constants.CCX_ROOT, expected_generation=expected_generation)
        store.resolve(handle)
    except GenerationError as exc:
        raise SessionUnavailable(f"Cannot access worker records to wait; check CCX_ROOT: {exc}") from exc

    deadline = None if timeout is None else time.monotonic() + timeout
    terminal = {"dead", "crashed", "idle-killed", "budget-stopped"}
    host_missing_since = None

    def remaining() -> float:
        return float("inf") if deadline is None else max(0.0, deadline - time.monotonic())

    def claude_record_settlement(record: dict) -> dict | None:
        """One validated source-owned Claude checkpoint, or genuinely unknown."""
        last = record.get("lastSettledTurn")
        if not isinstance(last, dict):
            return None
        ref, stamped = last.get("turnRef"), last.get("settledAtMs")
        floor = record.get("lastSettledAt")
        if (isinstance(ref, str) and ref.startswith("ccx-turn:claude:v1:")
                and len(ref) > len("ccx-turn:claude:v1:")
                and type(stamped) is int
                and (type(floor) is not int or stamped >= floor)):
            return {
                "turn_ref": ref, "turn_id": None,
                "outcome": last.get("outcome") or "unknown",
                "completed_at": stamped,
                "context": ({**last["context"], "as_of_turn_ref": ref}
                            if isinstance(last.get("context"), dict) else None),
            }
        return None

    def codex_record_settlement(record: dict) -> dict | None:
        """Only a terminal native Codex turn is an exact checkpoint."""
        last = record.get("lastTurn")
        if (isinstance(last, dict)
                and isinstance(last.get("turnId"), str) and last.get("turnId")
                and last.get("status") in {"completed", "failed", "interrupted"}):
            return turn_reports.codex_settled_turn(
                last, fallback_completed_at=record.get("lastTurnAt"))
        return None

    def snapshot() -> dict[str, Any]:
        store.revalidate()
        rec = store.record(handle)
        lc = rec.get("lifecycle") or {}
        state = lc.get("state")
        waiting = None
        input_block = None
        active = None
        served = False
        unavailable = False
        pending = False
        last_turn = None
        last_settled = None

        # Each probe fits the transport limit; the overall deadline is unchanged.
        budget = 5.0 if deadline is None else min(600.0, max(0.01, remaining()))
        if engine == "claude":
            last_settled = claude_record_settlement(rec)
            if state not in terminal:
                try:
                    reply = claude_host_rpc(handle, {"op": "status"}, timeout=budget)
                except (OSError, ValueError, TimeoutError):
                    reply = {}
                if reply.get("ok") is True:
                    served = True
                    state = reply.get("state") or state
                    active = reply.get("activeTurnRef")
                    waiting = reply.get("waitingFor") if state == "waiting" else None
                    pending = bool(reply.get("pendingDeliveries"))
                    # Status may cross the original turn's durable CAS. A host
                    # that now certifies stable IDLE must not pair that new
                    # state with a stale pre-RPC null settlement snapshot.
                    if (state == "idle" and active is None and not pending
                            and last_settled is None):
                        store.revalidate()
                        last_settled = claude_record_settlement(store.record(handle))
            if state == "waiting" and served:
                waiting = waiting or rec.get("waitingFor")
                input_block = _claude_pending(waiting)
            if not served and state not in terminal:
                unavailable = owned_process(
                    lc.get("hostPid"), lc.get("hostStart")) == "dead"
        else:
            last_turn = rec.get("lastTurn")
            if state not in terminal:
                try:
                    reply = codex_runtime.daemon_rpc({
                        "op": "status", "short": worker_id,
                        "rpcGeneration": CODEXD_RPC_GENERATION,
                    }, timeout=budget)
                except (OSError, ValueError, TimeoutError):
                    reply = {}
                served = (reply.get("ok") is True and reply.get("loaded") is True
                          and reply.get("transport") == "served"
                          and codex_runtime.daemon_generation(reply) == CODEXD_RPC_GENERATION)
                if served:
                    state = reply.get("state") or state
                    active_id = reply.get("activeTurnId")
                    if isinstance(active_id, str) and active_id:
                        active = "ccx-turn:codex:v1:" + active_id
                    last_turn = reply.get("lastTurn") or last_turn
                    waiting = reply.get("pendingApprovals") if state == "waiting" else None
                else:
                    unavailable = (reply.get("transport") == "absent"
                                   or (reply.get("transport") == "served"
                                       and reply.get("loaded") is False))
            if state == "waiting" and served:
                input_block = codex_pending(waiting or rec.get("pendingApprovals"))
            last_settled = codex_record_settlement({
                "lastTurn": last_turn, "lastTurnAt": rec.get("lastTurnAt")})
            if (served and state == "idle" and active is None
                    and not reply.get("pendingApprovals")
                    and last_settled is None):
                # The original codexd status normally includes lastTurn, but
                # it can be a pre-commit snapshot while a separate record CAS
                # completes. Refresh the same generation once, never infer a
                # turn from an idle state or make another daemon RPC.
                store.revalidate()
                fresh = store.record(handle)
                last_settled = codex_record_settlement(fresh)
                if last_settled is not None:
                    last_turn = fresh.get("lastTurn")

        # The recorded turn error is useful even when its host is gone.
        # Prefer the exact terminal turn to a stale previous API error.
        sources = (
            (last_turn or {}).get("error") if engine == "codex" else None,
            rec.get("activeTurnError") if engine == "codex" else None,
            (lc.get("apiError") or {}).get("message") if engine == "claude" else None,
        )
        def error_text(value):
            if isinstance(value, str):
                return value or None
            if isinstance(value, dict):
                return value.get("message") or value.get("text") or value.get("error")
            return None
        failure = next((text for source in sources
                        if (text := error_text(source))), None)
        result = {
            "worker": handle, "worker_id": worker_id, "engine": engine,
            "short": worker_id[:8], "mode": "managed", "state": state,
            "native_state": state, "status": state,
            "reason": None, "waiting_for": waiting if state == "waiting" and served else None,
            "pending_input": input_block,
            "wait_kind": (input_block or {}).get("kind"),
            "last_settled_turn": last_settled, "active_turn_ref": active,
            "apiError": lc.get("apiError") if engine == "claude" else None,
            "error": failure if state in terminal else None,
            "lastTurn": last_turn if engine == "codex" else None,
            "pendingApprovals": waiting if engine == "codex" and served else None,
            "timed_out": False,
            "_served": served, "_unavailable": unavailable, "_pending": pending,
        }
        if engine == "codex" and state == "idle" and active is None:
            stop = rec.get("quotaStop") or {}
            if (isinstance(last_turn, dict)
                    and last_turn.get("turnId") == stop.get("turnId")
                    and last_turn.get("status") in {"interrupted", "cancelled", "failed"}
                    and stop.get("turnId")):
                result["status"] = "budget-stopped"
                result["reason"] = stop.get("reason") or "usage cap reached"
        store.revalidate()
        return result

    def finish(value: dict[str, Any], status: str, *, observed: bool = False,
               reason: str | None = None) -> dict[str, Any]:
        out = {key: item for key, item in value.items() if not key.startswith("_")}
        out.update(status=status, timed_out=status == "timeout")
        if reason is not None:
            out["reason"] = reason
        if next_turn:
            out.update(wait_mode="next-turn", next_turn_observed=observed,
                       next_turn_event=None)
        store.revalidate()
        return out

    entry = snapshot()
    entry_ref = (entry.get("last_settled_turn") or {}).get("turn_ref")
    entry_gate = ((entry.get("waiting_for") or {}).get("id")
                  if entry["_served"] and engine == "claude" else None)
    entry_waiting = entry["_served"] and entry["state"] == "waiting"
    value = entry
    while True:
        current = value["status"]
        is_terminal = current in terminal
        latest = (value.get("last_settled_turn") or {}).get("turn_ref")
        changed = (latest is not None and
                   latest != (after_turn if after_turn is not None else entry_ref))
        gate = value.get("waiting_for") if value["_served"] else None
        gate_id = gate.get("id") if isinstance(gate, dict) else None
        waiting_now = value["_served"] and current == "waiting"

        # A durable boundary precedes a later crash, without certifying that
        # the model produced a completed assistant message.
        if next_turn and changed:
            return finish(value, current, observed=True)
        if next_turn and known_input is not None:
            if value["_served"] and (not waiting_now or gate_id != known_input):
                return finish(value, current, observed=True)
            if after_turn is not None and latest is None and not is_terminal:
                raise SessionUnavailable(
                    "latest exact settlement is unknown; cannot compare after_turn")
        elif next_turn and after_turn is not None:
            if waiting_now:
                return finish(value, "waiting", observed=True)
            if latest is None and not is_terminal:
                raise SessionUnavailable("latest exact settlement is unknown; cannot compare after_turn")
        elif next_turn and waiting_now:
            if not entry_waiting or (gate_id is not None and gate_id != entry_gate):
                return finish(value, "waiting", observed=True)

        if is_terminal:
            return finish(value, current)
        if value["_unavailable"]:
            if engine == "codex":
                activation = store.native_activation(handle)
                unloaded = (current in {"prepared", "idle", "idle-killed"}
                            and activation.get("state") == "confirmed")
                return finish(value, "stopped" if unloaded else "unavailable",
                              reason="Codex runtime is absent; no worker was resumed")
            if host_missing_since is None:
                host_missing_since = time.monotonic()
            grace = 5.0 if current in {"booting", "resuming"} else 0.35
            if time.monotonic() - host_missing_since >= grace:
                return finish(value, "host-dead",
                              reason="owned Claude host is no longer alive")
        else:
            host_missing_since = None
        if not next_turn and value["_served"]:
            if waiting_now:
                return finish(value, "waiting")
            if (current == "idle" and not value["_pending"]
                    and value["active_turn_ref"] is None):
                return finish(value, "idle", reason="worker is idle")

        # The first observation wins when already stable, even at timeout=0.
        if deadline is not None and time.monotonic() >= deadline:
            return finish(value, "timeout", reason="wait deadline elapsed")
        delay = min(0.2, remaining())
        if delay > 0:
            time.sleep(delay)
        value = snapshot()


def reconcile_turn(
    target: str, *, delivery_id: str | None = None,
    turn_id: str | None = None, timeout: float | None = None,
    include_worker: bool = True,
    expected_generation: str | None = None,
) -> dict[str, Any]:
    """Original full-ID delivery receipt reconciliation.

    Never infer acceptance from a native transcript prefix or retry unknown
    effects; the existing semantic receipt and pinned generation are the
    only authority until a real host receipt adapter is connected. Its one
    write: a Codex receipt left submitted by a lost acknowledgment is accepted
    from the exact journaled native user message carrying its own client id
    (``codex_delivery_recovery``); anything less leaves it unchanged.
    """
    from ccx.claude import delivery as claude_delivery
    from ccx import constants
    from ccx.store.root import require_ccx_active
    handle, engine, worker_id = split_target(target)
    deadline = time.monotonic() + (timeout if timeout is not None else 30.0)
    if expected_generation is not None:
        require_ccx_active(constants.CCX_ROOT, expected_generation=expected_generation)
    if engine == "codex":
        from ccx.codex import delivery as codex_delivery
        if (delivery_id is None) == (turn_id is None):
            raise InvalidRequest("Codex reconcile needs exactly delivery_id or turn_id")
        from ccx.codex.delivery_recovery import recover, recover_for_turn
        from ccx.store.root import GenerationBusy
        recovery_store = require_ccx_active(constants.CCX_ROOT)
        if delivery_id is not None:
            value = codex_delivery.read_receipt(handle, delivery_id)
            if isinstance(value, dict) and value.get("state") in ("submitted", "accepted"):
                # A lost acknowledgment is recovered only from the exact
                # journaled native user message; otherwise unchanged.
                try:
                    value = recover(recovery_store, handle, delivery_id) or value
                except GenerationBusy:
                    pass
        else:
            value = codex_delivery.receipt_for_turn(handle, turn_id)
            if value is None:
                # Never call a turn foreign while a submitted delivery of ours
                # may have opened it: recover from exact evidence first.
                try:
                    recover_for_turn(recovery_store, handle, turn_id)
                except GenerationBusy:
                    pass
                value = codex_delivery.receipt_for_turn(handle, turn_id)
        info = {"worker": handle, "worker_id": worker_id, "engine": "codex"}
        if value is None:
            return {
                "worker": info if include_worker else None,
                "status": "not_found", "pending": False, "owned": False,
                "recommended_action": "none", "delivery": None,
            }
        native_observation = None
        if (value.get("state") == "accepted"
                and value.get("settlement") is None
                and isinstance(value.get("turnId"), str)
                and codex_delivery.route(value) != "steer"):
            # Original turn/completed events survived in the sparse evidence
            # journal, even when the native rollout is too large to hydrate.
            # This also repairs completions that preceded acceptance or a
            # daemon restart, without requiring the old codexd to run new code.
            from ccx.codex.completion import settle_from_journal
            store = require_ccx_active(constants.CCX_ROOT)
            completed = settle_from_journal(store, handle, value["turnId"])
            if completed is not None:
                value = completed
            else:
                # A concurrent native completion may have settled this same
                # accepted receipt while the journal was being observed.
                value = codex_delivery.read_receipt(handle, value["deliveryId"])
        if (value.get("state") == "accepted"
                and value.get("settlement") is None
                and isinstance(value.get("turnId"), str)
                and codex_delivery.route(value) != "steer"):
            # Read native provider-owned thread/read through the actual
            # authenticated singleton codexd. A missing daemon, pending
            # turn or lost observation never causes a second turn/start.
            from ccx.codex.runtime import daemon_rpc
            from ccx.constants import CODEXD_RPC_GENERATION
            from ccx.store.store import _digest
            budget = max(0.001, deadline - time.monotonic())
            observed = daemon_rpc({
                "op": "native-observe",
                "workerId": worker_id,
                "deliveryId": value["deliveryId"],
                "turnId": value["turnId"],
                "observeSeconds": budget,
                "rpcGeneration": CODEXD_RPC_GENERATION,
            }, timeout=budget)
            if (observed.get("ok") is True
                    and observed.get("status") in ("failed", "interrupted", "cancelled")
                    and observed.get("workerId") == worker_id
                    and observed.get("turnId") == value["turnId"]
                    and observed.get("deliveryId") == value["deliveryId"]):
                native_observation = observed
            if (observed.get("ok") is True
                    and observed.get("status") == "completed"):
                native = store.record(handle)["native"]
                source = store.codex_source(handle)
                if (observed.get("workerId") != worker_id
                        or observed.get("engine") != "codex"
                        or observed.get("nativeSource") != source
                        or observed.get("threadId") != native.get("threadId")
                        or native.get("threadSource") != source
                        or observed.get("turnId") != value["turnId"]
                        or observed.get("deliveryId") != value["deliveryId"]
                        or observed.get("nativeEvidenceSha256") != _digest({
                            "source": source, "threadId": native["threadId"],
                            "turnId": value["turnId"], "status": "completed",
                            "text": observed.get("text"),
                        })):
                    raise SessionUnavailable(
                        "Codex completion belongs to another turn; inspect the delivery"
                    )
                value = codex_delivery.mark_settled(
                    handle, value["deliveryId"],
                    turn_id=value["turnId"],
                    original_positive_completion=observed,
                )
        result = codex_delivery.public_receipt(value)
        status = result["status"]
        if value.get("state") == "prepared":
            # Auto/steer stay prepared until codexd routes them, but their
            # global intent is fsynced first: once it advanced, the request
            # may still land, so this is submission uncertainty, never retry.
            effect = require_ccx_active(constants.CCX_ROOT).read_receipt(
                handle, "codex-send", value["deliveryId"],
            )
            if isinstance(effect, dict) and effect.get("state") != "prepared":
                status = "submitted"
                result = {**result, "pending": True, "recommended_action": "wait"}
        steered = codex_delivery.route(value) == "steer"
        complete = isinstance(value.get("settlement"), dict) and not steered
        return {
            "worker": info if include_worker else None,
            "status": "settled" if complete else status,
            "pending": result["pending"] or (
                value["state"] == "accepted" and not complete and not steered),
            "owned": result["owned"],
            "recommended_action": result["recommended_action"],
            "route": result["route"], "placement": result["placement"],
            "turn_ref": result["turn_ref"],
            "caller_metadata": result["caller_metadata"],
            "delivery": result,
            **({"native_status": native_observation["status"],
                "native_error": native_observation.get("error")}
               if native_observation else {}),
        }
    if turn_id is not None:
        raise CapabilityUnsupported(
            "Claude reconcile does not support turn_id",
            engine="claude", capability="reconcile_turn_id",
        )
    if delivery_id is None or not claude_delivery.valid_delivery_id(delivery_id):
        raise InvalidRequest("Claude reconcile requires one valid delivery_id")
    store = require_ccx_active(constants.CCX_ROOT,
                               expected_generation=expected_generation)
    value = claude_delivery.read_receipt(handle, delivery_id)
    recorded = {"worker": handle, "worker_id": worker_id, "engine": "claude"}
    if value is None:
        return {
            "worker": recorded if include_worker else None,
            "status": "not_found", "pending": False, "owned": False,
            "recommended_action": "none", "delivery": None,
        }
    if (value.get("placement") == "current_turn"
            and isinstance(value.get("settlement"), dict)):
        from ccx.store.root import NativeEffectIndeterminate
        raise NativeEffectIndeterminate(
            "a steer joined the target turn; it cannot borrow its completion"
        )
    # Crash-window repair: the real host can fsync its original accepted
    # completion before it fsyncs the companion native-effect terminal.
    # Reconcile using the same full owner, immutable original prompt spec
    # and native source evidence; never dispatch or invent a new turn.
    if value.get("state") == "accepted" and isinstance(value.get("settlement"), dict):
        from ccx.store.store import OperationIntent, _digest
        from ccx.store.root import NativeEffectIndeterminate, _HASH
        native = store.record(handle).get("native") or {}
        proof = value["settlement"]
        effect = store.read_receipt(handle, "claude-delivery", delivery_id)
        original_spec = claude_delivery.spec_digest(value)
        if (not isinstance(effect, dict)
                or effect.get("intentSha256") != original_spec
                or effect.get("action") != "send"
                or effect.get("workerId") != worker_id
                or effect.get("engine") != "claude"
                or effect.get("generation") != store.generation
                or proof.get("workerId") != worker_id
                or proof.get("deliveryId") != delivery_id
                or proof.get("nativeSource")
                != f"claude-session:{native.get('sessionId')}"
                # Recovery from an UNKNOWN native effect requires the
                # original provider sessionId explicitly in Stop evidence.
                # Historical/direct positive callbacks already settled the
                # native effect before their independent completion callback;
                # they may contain only the exact nativeSource, not a separate
                # sessionId field. Never weaken submitted-unknown recovery.
                or (effect.get("state") == "submitted-unknown"
                    and proof.get("sessionId") != native.get("sessionId"))
                or proof.get("turnRef") != value["turnRef"]
                or proof.get("status") != "completed"
                or not isinstance(proof.get("nativeEvidenceSha256"), str)
                or not _HASH.fullmatch(proof["nativeEvidenceSha256"])):
            raise NativeEffectIndeterminate(
                "Completed turn has no matching delivery record; reconcile it"
                "native source/effect proof; no false settlement"
            )
        if effect.get("state") == "submitted-unknown":
            store.settle_native_intent(
                OperationIntent(
                    worker_id, "claude", "claude-delivery", delivery_id,
                    original_spec, "submitted-unknown", store.generation,
                ),
                state="accepted",
                native_evidence_sha256=proof["nativeEvidenceSha256"],
            )
        elif effect.get("state") != "accepted":
            raise NativeEffectIndeterminate(
                "native-effect state contradicts positively settled original turn"
            )
    projected = claude_delivery.public_receipt(value)
    complete = isinstance(value.get("settlement"), dict)
    status = "settled" if complete else projected["status"]
    return {
        "worker": recorded if include_worker else None,
        "status": status,
        "pending": value["state"] in ("prepared", "owned")
                   or value["state"] == "accepted" and not complete,
        "owned": projected["owned"],
        "recommended_action": projected["recommended_action"],
        "route": projected["route"],
        "placement": projected["placement"],
        "turn_ref": projected["turn_ref"],
        "caller_metadata": projected["caller_metadata"],
        "delivery": projected,
    }


def wait_delivery(
    target: str, *, delivery_id: str, timeout: float | None = None,
    expected_generation: str | None = None,
) -> dict[str, Any]:
    """Wait for one delivery, raising on failure or required input."""
    from ccx.ops.report import report_worker
    from ccx import constants
    from ccx.errors import (DeliveryRejected, SessionWaiting, TurnIncomplete,
                         SessionUnavailable, BudgetStopped)
    from ccx.store.liveness import owned_process
    from ccx.store.root import require_ccx_active

    handle, engine, worker_id = split_target(target)
    store = require_ccx_active(constants.CCX_ROOT,
                               expected_generation=expected_generation)
    deadline = None if timeout is None else time.monotonic() + timeout
    last_status, last_ref = None, None

    def fail(exc):
        # The high-level creator attaches the original full UUID; retain the
        # latest *observed* semantic identity on its typed cause as well.
        exc.delivery_id = delivery_id
        exc.delivery_status = last_status
        exc.turn_ref = last_ref
        raise exc

    while True:
        store.revalidate()
        observation = None
        try:
            observation = reconcile_turn(
                handle, delivery_id=delivery_id,
                expected_generation=store.generation,
                timeout=(None if deadline is None else
                         max(0.001, deadline - time.monotonic())),
            )
        except (SessionUnavailable, OSError):
            # A transiently unreachable native daemon cannot prove a new
            # effect or a terminal outcome. Keep the original receipt pending.
            pass
        store.revalidate()
        if observation is not None:
            last_status = observation.get("status")
            receipt = observation.get("delivery") or {}
            last_ref = receipt.get("turn_ref") or last_ref
            if last_status == "settled":
                if (receipt.get("delivery_id") != delivery_id
                        or not isinstance(last_ref, str) or not last_ref
                        or not isinstance(receipt.get("settlement"), dict)):
                    fail(TurnIncomplete("turn has no confirmed completed reply",
                                        short=worker_id[:8]))
                report = report_worker(handle, turn_ref=last_ref, max_chars=None)
                store.revalidate()
                if (report.get("worker") != handle
                        or report.get("delivery_id") != delivery_id
                        or report.get("turn_ref") != last_ref
                        or report.get("status") != "completed"):
                    fail(TurnIncomplete("turn reply does not match its delivery receipt",
                                        short=worker_id[:8]))
                return {"delivery": receipt, "delivery_status": "settled",
                        "turn_ref": last_ref, "report": report}
            if last_status == "rejected":
                fail(DeliveryRejected(
                    receipt.get("rejection_message") or "turn's prompt was rejected",
                    short=worker_id[:8], refusal=receipt.get("rejection_code"),
                    delivery_id=delivery_id, delivery_status="rejected",
                ))
            if (engine == "codex" and observation.get("native_status")
                    in ("failed", "interrupted", "cancelled")):
                marker = store.record(handle).get("quotaStop") or {}
                if marker.get("turnId") == receipt.get("turn_id"):
                    reason = marker.get("reason") or "usage cap reached"
                    fail(BudgetStopped(f"budget stop: {reason}",
                                       short=worker_id[:8], reason=reason, phase="turn"))
                fail(TurnIncomplete(
                    "Codex turn ended " + observation["native_status"],
                    short=worker_id[:8], turn_id=receipt.get("turn_id"),
                    turn_status=observation["native_status"],
                    turn_error=observation.get("native_error"),
                ))
            if last_status in ("not_found", "prepared"):
                fail(TurnIncomplete(
                    "Message submission is not confirmed; "
                    f"reconcile delivery_id={delivery_id!r}, or retry with that "
                    "same delivery ID and message",
                    short=worker_id[:8],
                ))
        # A missing Stop can leave a Claude owned receipt forever. The
        # original worker's own gate/death is conclusive only while this first
        # delivery remains unsettled (the exact receipt check runs first).
        record = store.record(handle)
        lifecycle = record.get("lifecycle") or {}
        state = lifecycle.get("state")
        if engine == "claude":
            settled = record.get("lastSettledTurn") or {}
            outcome = settled.get("outcome")
            if last_ref and settled.get("turnRef") == last_ref and outcome in {
                    "interrupted", "failed", "cancelled"}:
                fail(TurnIncomplete("Claude turn ended " + outcome,
                                    short=worker_id[:8], turn_status=outcome))
            pending = record.get("waitingFor")
            if pending or state == "waiting":
                fail(SessionWaiting(
                    "Claude turn needs input; use ccx answer or approve", short=worker_id[:8],
                    waiting_for=pending,
                ))
            if state == "budget-stopped":
                reason = lifecycle.get("lastError") or "usage cap reached"
                fail(BudgetStopped(
                    f"budget stop: {reason}", short=worker_id[:8],
                    reason=reason, phase="turn",
                ))
            if state in {"dead", "crashed", "idle-killed", "killed", "stopped", "budget-stopped"}:
                fail(TurnIncomplete("Claude turn cannot complete: " + state,
                                    short=worker_id[:8], state=state))
            if (lifecycle.get("hostPid") and owned_process(
                    lifecycle.get("hostPid"), lifecycle.get("hostStart")) == "dead"):
                fail(TurnIncomplete("Claude stopped before completing the turn",
                                    short=worker_id[:8], state=state))
        elif engine == "codex":
            # This stored original native turn is another source of positive
            # terminal evidence when codexd becomes temporarily unavailable.
            receipt_id = (observation or {}).get("delivery") or {}
            if not receipt_id:
                from ccx.codex.delivery import public_receipt, read_receipt
                receipt_id = public_receipt(read_receipt(handle, delivery_id)) or {}
            native_turn = receipt_id.get("turn_id")
            last = record.get("lastTurn") or {}
            if (native_turn and last.get("turnId") == native_turn
                    and last.get("status") in {"failed", "interrupted", "cancelled"}):
                marker = record.get("quotaStop") or {}
                if marker.get("turnId") == native_turn:
                    reason = marker.get("reason") or "usage cap reached"
                    fail(BudgetStopped(f"budget stop: {reason}",
                                       short=worker_id[:8], reason=reason, phase="turn"))
                fail(TurnIncomplete(
                    "Codex turn ended " + last["status"],
                    short=worker_id[:8], turn_id=native_turn,
                    turn_status=last["status"], turn_error=last.get("error"),
                ))
        if deadline is not None and time.monotonic() >= deadline:
            fail(TurnIncomplete("turn did not finish before timeout",
                                short=worker_id[:8], state=state))
        time.sleep(.2 if deadline is None else
                   min(.2, max(0., deadline - time.monotonic())))
