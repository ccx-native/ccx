from __future__ import annotations
import json
from typing import Any
from ccx.errors import SessionUnavailable
from ccx.notify.registry import _current_worker_name, _delivery_id, iter_registrations, read_registration
from ccx.notify.select import _original_notification_registration
from ccx.notify.settle import _truncate_report


class DeliveryDeferred(RuntimeError):
    """The wake is still valid but cannot be admitted right now."""


def _same_parent(a: dict[str, Any], b: dict[str, Any]) -> bool:
    return (a.get("kind") == b.get("kind") and a.get("threadId") == b.get("threadId")
            and (a.get("kind") != "ccx-codex" or
                 (a.get("workerId") == b.get("workerId") and
                  a.get("generation") == b.get("generation"))))


def outstanding_for_parent(reg: dict[str, Any], *, limit: int = 20) -> list[dict[str, Any]]:
    parent = reg.get("parent") or {}
    current = reg.get("eventId")
    out: list[dict[str, Any]] = []
    for other in iter_registrations():
        if other.get("eventId") == current or not _same_parent(parent, other.get("parent") or {}):
            continue
        worker = other.get("worker") or {}
        out.append({
            "eventId": other.get("eventId"),
            "worker": f"{worker.get('engine')}:{worker.get('workerId')}",
            "workerName": _current_worker_name(worker),
            "state": other.get("state"),
        })
        if len(out) >= limit:
            break
    return out


def payload_for_registration(
    reg: dict[str, Any], settlement: dict[str, Any]
) -> dict[str, Any]:
    _store, parent, child = _original_notification_registration(reg)
    controller_attempt = reg["worker"]["engine"] == "controller"
    report = settlement.get("report")
    report_truncated = bool(settlement.get("reportTruncated"))
    if reg["source"] == "notify":
        # The receipt-backed standalone path must obey the same bound as
        # receiptless standalone notifications.
        report, clipped = _truncate_report(report)
        report_truncated = report_truncated or clipped
    payload = {
        "kind": "ccx-worker-notification", "phase": "settled",
        "eventId": reg["eventId"],
        "deliveryId": _delivery_id(reg["eventId"]),
        "worker": child, "parent": parent,
        "source": reg["source"], "status": settlement["status"],
        **({"selectionMode": reg["operation"]["selectionMode"],
            "selectedTurnRef": reg["operation"].get("selectedTurnRef"),
            "selectedOriginalDeliveryId": reg["operation"].get("deliveryId")}
           if reg["source"] in {"notify", "new"} else {}),
        **({"selectedOriginalDeliveryId": reg["operation"]["deliveryId"]}
           if reg["source"] == "send" else {}),
        **({"selectedTurnRef": reg["operation"]["selectedTurnRef"]}
           if reg["source"] == "input" else {}),
        "workerState": settlement.get("workerState"),
        "report": report,
        "reportTruncated": report_truncated,
        "turnId": settlement.get("turnId"),
        **({"turnRef": settlement.get("turnRef")}
           if reg["worker"]["engine"] == "claude" else {}),
        "turnStatus": settlement.get("turnStatus"),
        "waitingFor": settlement.get("waitingFor"),
        "error": settlement.get("error"),
        **({"settle": True, "settlementVerdict": settlement.get("settlementVerdict"),
            "settlement": settlement.get("settlement")}
           if reg.get("settlementMode") == "orchestration" else {}),
        "activity": {"workerState": settlement.get("workerState")},
        "callerMetadata": reg["operation"].get("callerMetadata"),
    }
    if reg["operation"].get("kind") in {"claude-compact", "codex-compact"}:
        payload.update(operationType="compact",
                       operationId=reg["operation"]["operationId"],
                       operationStatus=settlement.get("operationStatus"),
                       compacted=settlement.get("compacted"),
                       evidenceId=settlement.get("evidenceId"),
                       compactTurnId=settlement.get("compactTurnId"),
                       contextItemId=settlement.get("contextItemId"),
                       nativeTurnStatus=settlement.get("nativeTurnStatus"),
                       preTokens=settlement.get("preTokens"),
                       postTokens=settlement.get("postTokens"),
                       contextAfter=settlement.get("contextAfter"))
    if controller_attempt:
        payload.update(
            workerName=settlement.get("workerName"),
            operationType="controller-attempt",
            operationId=settlement.get("operationId"),
            runId=settlement.get("runId"), attempt=settlement.get("attempt"),
            controller=settlement.get("controller"),
            controllerStatus=settlement.get("controllerStatus"),
            checkpoint=settlement.get("checkpoint"),
            summary=settlement.get("summary"),
            reportPath=settlement.get("reportPath"),
            activity=settlement["activity"],
            outstanding=settlement["outstanding"],
        )
        if payload["reportTruncated"] and isinstance(settlement.get("reportPath"), str):
            import shlex
            payload["reportCommand"] = f"cat {shlex.quote(settlement['reportPath'])}"
    return payload


def _external_codex_deliver(
    reg: dict[str, Any], payload: dict[str, Any], *, reconcile: bool,
) -> dict[str, Any]:
    """Wake the caller-captured Codex thread; uncertain retries only observe.

    The thread belongs to the user's own Codex UI, so it is only ever woken
    while that UI has it loaded. It is never resumed here: a resumed thread
    would run the notification turn headless, where nobody sees it.
    """
    from ccx.codex.control import CodexControlClient, CodexControlError
    from ccx.codex.notify_history import notification_delivery_match

    store, parent, _child = _original_notification_registration(reg)
    if not isinstance(parent, dict) or parent.get("kind") != "codex-native-thread":
        raise DeliveryDeferred("interactive Codex return address was not captured")
    thread_id = parent["threadId"]
    delivery_id = payload["deliveryId"]

    def defer(reason: object, *, no_effect: bool = False) -> None:
        error = DeliveryDeferred(str(reason))
        # On an earlier possible machine write, a missing/failed history walk
        # never authorizes the notifier to clear its durable attempt marker.
        error.no_effect = no_effect and not reconcile
        raise error

    try:
        client = CodexControlClient()  # existing primary CODEX_HOME socket
    except FileNotFoundError as error:
        missing = DeliveryDeferred(f"external Codex parent control socket absent: {error}")
        missing.no_effect = not reconcile
        raise missing from error
    except (OSError, ConnectionError, TimeoutError) as error:
        defer(error, no_effect=True)
    try:
        try:
            client.initialize()
        except Exception as error:
            defer(error, no_effect=True)  # no native machine request made

        if reconcile:
            try:
                match = notification_delivery_match(client.request, thread_id, delivery_id)
            except Exception as error:
                defer(error)  # e.g. its UI closed; never resume to look
            if (not isinstance(match, dict)
                    or match.get("deliveryId") != delivery_id
                    or match.get("eventId") != reg["eventId"]
                    or not isinstance(match.get("turnId"), str)
                    or not match["turnId"]):
                defer("prior native notification placement remains unproven")
            store.revalidate()
            return {"ok": True, "reconciled": True,
                    "eventId": reg["eventId"], "deliveryId": delivery_id,
                    "threadId": thread_id, "turnId": match["turnId"]}

        params = {
            "threadId": thread_id, "input": [],
            "turnTrigger": "ccx-worker-notification",
            "toolOutput": {
                "namespace": "ccx", "name": "worker_notification",
                "output": json.dumps(payload, separators=(",", ":")),
            },
        }
        try:
            store.revalidate()
            answer = client.request("turn/start", params, timeout=60)
        except CodexControlError as error:
            # Positive refusal, no write; "thread not found" means its UI is
            # closed, so wait for it to reopen (bounded by notifyd's expiry).
            defer(error, no_effect=True)
        except Exception as error:
            defer(error)  # possible native placement, history-only next time
        native_turn = answer.get("turn") if isinstance(answer, dict) else None
        turn_id = (answer.get("turnId") if isinstance(answer, dict) else None)
        if not isinstance(turn_id, str) or not turn_id:
            turn_id = native_turn.get("id") if isinstance(native_turn, dict) else None
        if not isinstance(turn_id, str) or not turn_id:
            defer("native machine wake returned no positive original turn ID")
        store.revalidate()
        # A native ACK proves machine tool output placement, not model completion.
        return {"ok": True, "reconciled": False,
                "eventId": reg["eventId"], "deliveryId": delivery_id,
                "threadId": thread_id, "turnId": turn_id}
    finally:
        client.close()


def _managed_codex_deliver(
    reg: dict[str, Any], payload: dict[str, Any], *, reconcile: bool,
) -> dict[str, Any]:
    """Deliver/reconcile once; reload only an idle-unloaded original parent."""
    from ccx.codex import runtime as runtime
    from ccx.constants import CODEXD_RPC_GENERATION

    store, parent, _child = _original_notification_registration(reg)
    if not (reg.get("state") == "ready" and reg.get("payload") == payload
            and isinstance(reg.get("settlement"), dict)
            and payload == payload_for_registration(reg, reg["settlement"])
            and payload.get("parent") == parent
            and payload.get("deliveryId") == _delivery_id(reg["eventId"])):
        raise DeliveryDeferred("original parent wake registration/payload not ready")
    parent_id = reg["parent"]["workerId"]
    lifecycle = (store.record(parent).get("lifecycle") or {})
    state = lifecycle.get("state")
    if state in {"dead", "budget-stopped"}:
        # An explicitly killed parent cannot be reactivated by a child wake.
        # The ready event and any earlier native attempt remain intact until
        # an independent user action makes that same parent live again.
        error = DeliveryDeferred(f"original parent {state}; machine wake still pending")
        error.no_effect = not reconcile
        raise error
    if state in {"idle", "idle-killed", "crashed"}:
        # Idle-killed means the janitor (or a daemon exit) unloaded the
        # original thread; crashed is a runtime loss, not an explicit stop.
        # A recorded idle parent also needs reload if the singleton has gone;
        # do not force an idle-only resume preflight on an ACTIVE busy parent.
        reload_needed = state in {"idle-killed", "crashed"}
        if not reload_needed:
            from ccx.store.worker_state import RUNTIME_ABSENT, RUNTIME_INCONCLUSIVE
            observed = runtime.observe_worker(parent_id, timeout=.5)
            if observed["presence"] == RUNTIME_INCONCLUSIVE:
                error = DeliveryDeferred("original parent runtime presence inconclusive")
                error.no_effect = not reconcile
                raise error
            reload_needed = observed["presence"] == RUNTIME_ABSENT
        if reload_needed:
            from ccx.ops.send import ensure_ready
            try:
                ready = ensure_ready(parent, timeout=60,
                                     expected_generation=store.generation)
            except Exception as exc:
                error = DeliveryDeferred(f"original idle parent reload unavailable: {exc}")
                # A failed reload is no evidence about a previous possible
                # machine wake: W1 must retain its attempted marker.
                error.no_effect = not reconcile
                raise error from exc
            if (ready.get("worker") != parent or ready.get("state") != "idle"
                    or ready.get("thread_id") != reg["parent"]["threadId"]):
                error = DeliveryDeferred("original parent reload has no exact idle/thread proof")
                error.no_effect = not reconcile
                raise error
    store.revalidate()
    reply = runtime.daemon_rpc({
        "op": "parent-notify", "short": parent_id,
        "rpcGeneration": CODEXD_RPC_GENERATION,
        "eventId": reg["eventId"], "deliveryId": payload["deliveryId"],
        "payload": payload, "reconcile": reconcile,
    }, timeout=65)
    if reply.get("transport") != "served":
        error = DeliveryDeferred("original parent runtime unavailable or inconclusive")
        # A positively absent daemon received no native request. A lost
        # response from a served daemon is never proof of native non-delivery.
        # A reconcile-only retry says nothing about the earlier attempt's
        # possible wake, so it must never clear that attempted marker.
        error.no_effect = not reconcile and reply.get("transport") == "absent"
        raise error
    try:
        runtime.require_served_generation(reply, short=parent_id)
    except SessionUnavailable as error:
        raise DeliveryDeferred(str(error)) from error
    if reply.get("ok") is not True:
        error = DeliveryDeferred(str(reply.get("error") or "original parent notification deferred"))
        error.no_effect = (not reconcile and reply.get("definitive") is True
                           and reply.get("pending") is not True)
        raise error
    if (reply.get("workerId") != parent_id
            or reply.get("threadId") != reg["parent"]["threadId"]
            or reply.get("eventId") != reg["eventId"]
            or reply.get("deliveryId") != payload["deliveryId"]
            or not isinstance(reply.get("turnId"), str)
            or not reply["turnId"]):
        raise DeliveryDeferred("positive parent wake lacks original UUID/thread/turn proof")
    store.revalidate()
    # Native ACK means machine output was placed, not parent model completed.
    return reply


def deliver_to_parent(reg: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    _original_notification_registration(reg)
    fresh = read_registration(reg["eventId"])
    if (fresh is None or not (
            fresh.get("state") == "ready" and fresh.get("payload") == payload
            and isinstance(fresh.get("settlement"), dict)
            and payload == payload_for_registration(fresh, fresh["settlement"])
    )):
        raise DeliveryDeferred("original notification cancelled or changed before parent delivery")
    # The supplied reg is the pre-attempt snapshot. First placement stays
    # fresh; ambiguous retries reconcile the same final delivery ID only.
    reconcile = reg.get("deliveryAttemptedAtMs") is not None
    if fresh["parent"]["kind"] == "codex-native-thread":
        return _external_codex_deliver(fresh, payload, reconcile=reconcile)
    return _managed_codex_deliver(fresh, payload, reconcile=reconcile)
