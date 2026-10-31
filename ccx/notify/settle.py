from __future__ import annotations
from typing import Any
from ccx.errors import SessionUnavailable
from ccx.ops.turn_reports import codex_native_turn_evidence as _codex_native_turn_evidence
from ccx.notify.registry import NOTIFY_REPORT_MAX_CHARS, mutate_registration, now_ms, read_registration
from ccx.notify.select import _compact_effect_receipt, _future_events, _future_ref_before, _original_notification_registration


def _future_settlement(reg: dict[str, Any]) -> dict[str, Any] | None:
    """Select the first future native R once, then reuse exact receiptless proof."""
    from ccx import constants
    from ccx.claude.managed_turns import report_window_for_turn_ref, settlement_for_turn_ref
    from ccx.ops.wait import wait_worker
    from ccx.store.root import require_ccx_active

    worker_id = reg["worker"]["workerId"]
    child = f"claude:{worker_id}"
    store = require_ccx_active(constants.CCX_ROOT)
    op = reg["operation"]
    cursor = op["armCursor"]
    events = _future_events(worker_id, cursor)
    ref = op["selectedTurnRef"]
    if ref is None and events is not None:
        seen: set[str] = set()
        for row in events:
            candidate = row.get("turn_ref")
            if not isinstance(candidate, str) or not candidate.startswith("ccx-turn:claude:v1:"):
                continue
            if candidate not in seen:
                seen.add(candidate)
                if (row.get("type") in {"turn_start", "turn_identity_recovered"}
                        and not _future_ref_before(worker_id, cursor, candidate)):
                    def freeze(value: dict[str, Any]) -> None:
                        if (value["state"] == "armed"
                                and value["operation"]["selectedTurnRef"] is None):
                            value["operation"]["selectedTurnRef"] = candidate
                    updated = mutate_registration(reg["eventId"], freeze)
                    if updated is not None:
                        reg = updated
                        ref = reg["operation"]["selectedTurnRef"]
                    break
    record = store.record(child)
    state = (record.get("lifecycle") or {}).get("state")
    base = {"workerState": state, "report": None, "error": None}
    if ref is not None:
        window, available = report_window_for_turn_ref(worker_id, ref)
        offset = window.get("startOffset") if isinstance(window, dict) else None
        if not available or type(offset) is not int or offset < 0:
            # Late compact Stop and missed-hook turns are unknown, not
            # a completed model turn or a permanently pending registration.
            terminal, retained = settlement_for_turn_ref(worker_id, ref)
            if retained and terminal and terminal.get("turnRef") == ref:
                return {**base, "status": "unknown", "turnStatus": "unknown",
                        "turnRef": ref}
        else:
            # The selected turn is immutable across later turns and terminal retries.
            result = _receiptless_settlement(reg)
            if result is not None and result.get("status") != "waiting":
                return result
    # Gate is actionable only when an after-arm request and this same native R
    # are both positively served; never adopt a pre-arm/foreign input gate.
    if ref is not None and events is not None and any(
            e.get("type") in {"permission_requested", "elicitation_requested"}
            or (e.get("type") == "state" and e.get("to") == "waiting")
            for e in events):
        try:
            status = wait_worker(child, timeout=.25,
                                 expected_generation=store.generation)
        except (SessionUnavailable, OSError):
            status = {}
        gate = status.get("waiting_for")
        if (status.get("worker") == child and status.get("state") == "waiting"
                and status.get("active_turn_ref") == ref
                and isinstance(gate, dict) and gate.get("id")
                and any((e.get("type") == "state" and e.get("to") == "waiting")
                        or (e.get("type") in {"permission_requested", "elicitation_requested"}
                            and str(e.get("request_id") or e.get("requestId")) == str(gate["id"]))
                        for e in events)):
            return {**base, "status": "waiting", "turnStatus": "waiting",
                    "turnRef": ref, "workerState": "waiting", "waitingFor": gate}
    if state in {"dead", "crashed", "idle-killed", "budget-stopped"}:
        return {**base, "status": state, "turnStatus": None,
                "error": (record.get("lifecycle") or {}).get("lastError")}
    if state in {"idle", "busy", "waiting", "booting", "resuming"}:
        from ccx.store.liveness import lifecycle_host_alive
        from ccx.store.paths import sock_path
        lifecycle = record.get("lifecycle") or {}
        since = lifecycle.get("since")
        pid = lifecycle.get("hostPid")
        grace = 30_000 if state in {"booting", "resuming"} else 3_000
        if (type(since) in {int, float} and now_ms() - since >= grace
                and (not sock_path(worker_id).exists()
                     or (pid and not lifecycle_host_alive(lifecycle)))):
            return {**base, "status": "host-dead", "turnStatus": None,
                    "error": "original Claude host is gone while future arm waits"}
    return None


def _receiptless_codex_report(store, child: str, worker_id: str, thread_id: str,
                              turn_id: str) -> str | None:
    """Only an already-serving original app-server may supply exact final text."""
    from ccx.codex import runtime as codex_runtime
    from ccx.ops.turn_reports import codex_source_report

    try:
        reply = codex_runtime.daemon_call(
            {"op": "thread-history", "workerId": worker_id,
             "threadId": thread_id, "turnId": turn_id,
             "observeSeconds": 2}, timeout=2,
            short=child,
        )
    except (SessionUnavailable, OSError):
        return None
    if (reply.get("transport") != "served" or reply.get("ok") is not True
            or reply.get("workerId") != worker_id or reply.get("threadId") != thread_id):
        return None
    try:
        report = codex_source_report(
            reply.get("thread"), worker_id, thread_id, turn_id,
            expected_source=store.codex_source(child),
        )
    except SessionUnavailable:
        return None
    return report["text"] or None


def _receiptless_settlement(reg: dict[str, Any]) -> dict[str, Any] | None:
    """Read the frozen native turn, never a guessed send receipt or newer turn."""
    from ccx import constants
    from ccx.ops.wait import wait_worker
    from ccx.store.root import require_ccx_active

    store = require_ccx_active(constants.CCX_ROOT)
    worker = reg["worker"]
    child = f"{worker['engine']}:{worker['workerId']}"
    operation = reg["operation"]
    ref = operation["selectedTurnRef"]
    record = store.record(child)
    base = {"workerState": (record.get("lifecycle") or {}).get("state"),
            "report": None}
    if worker["engine"] == "claude":
        from ccx.claude.managed_turns import report_window_for_turn_ref, settlement_for_turn_ref
        window, available = report_window_for_turn_ref(worker["workerId"], ref)
        start = window.get("startOffset") if isinstance(window, dict) else None
        if not available or type(start) is not int or start < 0:
            return None  # A fallback compact Stop is not a model completion.
        base["turnRef"] = ref
        terminal, available = settlement_for_turn_ref(worker["workerId"], ref)
        if available and terminal is not None and terminal.get("turnRef") == ref:
            if terminal.get("eventType") != "turn_complete":
                status = "unknown"  # turn_settled is liveness, never semantic success
            else:
                status = "interrupted" if terminal.get("interrupted") else "completed"
            result = {**base, "status": status, "turnStatus": status,
                      "error": terminal.get("evidence") if status == "unknown" else None}
            if status == "completed" and window.get("endOffsetKnown") is True:
                result["report"], result["reportTruncated"] = _truncate_report(
                    _receiptless_claude_report(store, child, worker["workerId"], ref, window))
            return result
    else:
        turn_id = operation["turnId"]
        base["turnId"] = turn_id
        proof = _codex_native_turn_evidence(child, record,
                                           operation["nativeSourceId"], turn_id)
        if proof is None:
            return None
        if proof["status"] is not None:
            status = proof["status"]
            report = (_receiptless_codex_report(store, child, worker["workerId"],
                      operation["nativeSourceId"], turn_id)
                      if status == "completed" else None)
            report, truncated = _truncate_report(report)
            return {**base, "status": status, "turnStatus": status,
                    "report": report, "reportTruncated": truncated, "error": proof["error"]}
    # A gate is actionable only while the exact frozen native R remains active.
    try:
        snapshot = wait_worker(child, timeout=0, expected_generation=store.generation)
    except (SessionUnavailable, OSError):
        return None
    store.revalidate()
    if snapshot.get("worker") != child or snapshot.get("active_turn_ref") != ref:
        return None
    if snapshot.get("state") != "waiting":
        return None
    gate = (snapshot.get("waiting_for") if worker["engine"] == "claude" else
            snapshot.get("pendingApprovals") or snapshot.get("pending_approvals"))
    if worker["engine"] == "claude" and not (isinstance(gate, dict) and gate.get("id")):
        return None
    if worker["engine"] == "codex" and not gate:
        return None
    return {**base, "status": "waiting", "turnStatus": "waiting",
            "workerState": "waiting", "waitingFor": gate}


def _receiptless_claude_report(store, child: str, worker_id: str, ref: str,
                               window: dict[str, Any]) -> str | None:
    from ccx.ops.turn_reports import claude_source_report
    from ccx.store.root import StorageIntegrityError

    try:
        result = claude_source_report(store, child, worker_id, ref, window)
        return result["text"] or None
    except (StorageIntegrityError, OSError, ValueError, SessionUnavailable):
        return None


def _truncate_report(text: str | None) -> tuple[str | None, bool]:
    if text is None:
        return None, False
    if NOTIFY_REPORT_MAX_CHARS <= 0 or len(text) <= NOTIFY_REPORT_MAX_CHARS:
        return text, False
    return text[:NOTIFY_REPORT_MAX_CHARS], True


def _compact_observation(reg: dict[str, Any]) -> dict[str, Any]:
    """One read-only original-operation view; never start or publish a compact."""
    from ccx.errors import CcxError
    from ccx.store.root import GenerationError
    store, _parent, child = _original_notification_registration(reg)
    operation = reg["operation"]
    engine = reg["worker"]["engine"]
    operation_id = operation["operationId"]
    receipt = _compact_effect_receipt(
        store, child, engine, reg["worker"]["workerId"], operation)
    state = receipt.get("state") if receipt else None
    if state in (None, "prepared"):
        return {"operationStatus": "not-started", "terminal": False}
    if state == "rejected":
        return {"operationStatus": "rejected", "terminal": True}
    try:
        if engine == "claude":
            from ccx.ops.compact import compact_status
            view = compact_status(child, operation_id=operation_id, publish=False)
            status = view.get("operation_status")
            terminal = status in {"succeeded", "noop", "failed"} or (
                status == "unknown" and not str(view.get("reason") or "").startswith(
                    "original Claude compact source unavailable"))
            return {**view, "operationStatus": status, "terminal": terminal}
        from ccx.codex.ops import compact_observation
        view = compact_observation(child, operation_id)
        status = view.get("status")
        record = store.record(child)
        last = record.get("lastCompact")
        last_terminal = (isinstance(last, dict)
                         and last.get("operationId") == operation_id
                         and (last.get("status") == "unknown"
                              or last.get("nativeTurnSettled") is True))
        return {**view, "operationStatus": status,
                "terminal": (status in {"succeeded", "failed"} or
                             (status == "unknown" and last_terminal))}
    except (CcxError, GenerationError, OSError, ValueError):
        # Lost original evidence is never a compact completion.
        return {"operationStatus": "unknown", "terminal": False}


def _compact_settlement(reg: dict[str, Any]) -> dict[str, Any] | None:
    """Freeze only the original native compact's proven verdict, never a model R."""
    viewed = _compact_observation(reg)
    if not viewed["terminal"]:
        return None
    status = viewed["operationStatus"]
    result = {
        "status": "completed" if status in {"succeeded", "noop"} else status,
        "workerState": (_original_notification_registration(reg)[0].record(
            f"{reg['worker']['engine']}:{reg['worker']['workerId']}"
        ).get("lifecycle") or {}).get("state"),
        "operationType": "compact",
        "operationId": reg["operation"]["operationId"],
        "operationStatus": status,
        "compacted": (status == "succeeded" if status in {"succeeded", "noop"}
                      else None),
        "report": None,
        "error": viewed.get("reason") if status in {"failed", "unknown"} else None,
    }
    if reg["worker"]["engine"] == "claude":
        result.update(message=viewed.get("message"),
                      preTokens=viewed.get("pre_tokens"),
                      postTokens=viewed.get("post_tokens"),
                      durationMs=viewed.get("duration_ms"),
                      evidenceId=viewed.get("evidence_uuid"))
    else:
        result.update(compactTurnId=viewed.get("compactTurnId"),
                      contextItemId=viewed.get("contextItemId"),
                      nativeTurnStatus=viewed.get("nativeTurnStatus"),
                      contextBefore=viewed.get("contextBefore"),
                      contextAfter=viewed.get("contextAfter"))
    return result


def _settlement_wake_marker(
    reg: dict[str, Any], *, active: bool, pending_wake: bool,
    last_turn_id: str | None
) -> int | None:
    """Persist/clear the durable wake-grace start for recursive settlement."""
    event_id = reg.get("eventId")
    if not isinstance(event_id, str) or not event_id:
        return None
    state = reg.get("settlementState")
    state = state if isinstance(state, dict) else {}
    current = state.get("pendingWakeSinceMs")
    if active or not pending_wake:
        if current is not None:
            def clear(value: dict[str, Any]) -> None:
                settlement_state = value.get("settlementState")
                if isinstance(settlement_state, dict):
                    settlement_state.pop("pendingWakeSinceMs", None)
                    if not settlement_state:
                        value.pop("settlementState", None)
            mutate_registration(event_id, clear)
        return None
    if (type(current) is int and
            state.get("lastTurnId") == last_turn_id):
        return current

    started = now_ms()
    def mark(value: dict[str, Any]) -> None:
        settlement_state = value.setdefault("settlementState", {})
        if settlement_state.get("lastTurnId") != last_turn_id:
            settlement_state["pendingWakeSinceMs"] = started
        else:
            settlement_state.setdefault("pendingWakeSinceMs", started)
        settlement_state["lastTurnId"] = last_turn_id
    updated = mutate_registration(event_id, mark)
    persisted = (
        (updated.get("settlementState") or {}).get("pendingWakeSinceMs")
        if isinstance(updated, dict)
        else None
    )
    return persisted if isinstance(persisted, int) else started


def _recursive_codex_settlement(
    reg: dict[str, Any],
    base: dict[str, Any],
    registration_snapshot: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    worker = reg.get("worker") if isinstance(reg.get("worker"), dict) else {}
    worker_id = worker["workerId"]
    from ccx.codex.settle import DEFAULT_WAKE_GRACE_S, settlement_progress

    progress = settlement_progress(f"codex:{worker_id}",
                                   registrations=registration_snapshot)
    active = bool(progress.get("activeTurnId"))
    pending_wake = bool(progress.get("pendingWakeCount")) and not bool(
        progress.get("outstandingCount")
    )
    wake_since = _settlement_wake_marker(
        reg, active=active, pending_wake=pending_wake,
        last_turn_id=(progress.get("lastTurn") or {}).get("turnId"))
    if (
        pending_wake
        and not active
        and isinstance(wake_since, int)
        and now_ms() - wake_since >= int(DEFAULT_WAKE_GRACE_S * 1000)
    ):
        return {
            **base,
            "status": "delivery_gap",
            "settlementVerdict": "delivery_gap",
            "settlement": {
                "verdict": "delivery_gap",
                "outstandingCount": progress.get("outstandingCount"),
                "outstanding": progress.get("outstanding"),
                "pendingWakeCount": progress.get("pendingWakeCount"),
                "pendingWake": progress.get("pendingWake"),
            },
            "error": (
                "a descendant final notification was delivered after the "
                "target's last completed turn, but no wake turn was observed "
                "inside the recursive settlement wake grace"
            ),
        }

    final = (
        None
        if progress.get("settlementVerdict") == "pending"
        else progress
    )
    if final is None:
        return None

    _settlement_wake_marker(reg, active=True, pending_wake=False,
                            last_turn_id=(progress.get("lastTurn") or {}).get("turnId"))
    verdict = final.get("settlementVerdict")
    summary = {
        "verdict": verdict,
        "outstandingCount": final.get("outstandingCount"),
        "outstanding": final.get("outstanding"),
        "pendingWakeCount": final.get("pendingWakeCount"),
        "pendingWake": final.get("pendingWake"),
    }
    if verdict == "settled":
        return {
            **base,
            "settlementVerdict": "settled",
            "settlement": summary,
        }
    return {
        **base,
        "status": verdict,
        "workerState": final.get("state") or base.get("workerState"),
        "settlementVerdict": verdict,
        "settlement": summary,
        "error": final.get("error") or base.get("error"),
    }


def _original_codex_settlement(reg: dict[str, Any]) -> dict[str, Any] | None:
    """Freeze only the fenced original semantic Codex delivery's outcome.

    This is a receipt consumer, not a second native turn observer. An optional
    single read-only native reconciliation uses the already accepted core.
    """
    from ccx.codex import delivery as codex_delivery
    from ccx.ops.wait import reconcile_turn, wait_worker
    from ccx.errors import SessionUnavailable

    store, _parent, child = _original_notification_registration(reg)
    operation = reg["operation"]
    token = operation["deliveryId"]
    receipt = codex_delivery.read_receipt(child, token)
    if receipt is None:
        # Publication requires this original prepared receipt. Its later loss
        # never turns an old cached report into a completed child result.
        return None
    if (receipt.get("promptSha256") != operation.get("promptSha256")
            or receipt.get("requestedMode") != operation.get("requestedMode")
            or receipt.get("callerMetadata") != operation.get("callerMetadata")
            or receipt.get("clientUserMessageId") != operation.get("clientUserMessageId")):
        raise SessionUnavailable("Notification does not match its delivery; inspect the worker")
    base = {"workerState": (store.record(child).get("lifecycle") or {}).get("state"),
            "turnId": receipt.get("turnId"), "report": None}
    if receipt["state"] == "rejected":
        return {**base, "status": "rejected", "turnStatus": "rejected",
                "error": receipt.get("rejectionMessage") or receipt.get("rejectionCode")}
    if receipt["state"] == "prepared":
        # Registration follows prepare; this send has never reached a
        # native call in this state. A retryable refusal is not a settlement.
        return None
    if receipt["state"] != "accepted":
        # owned/submitted and lost-ACK remain unresolved, never completed.
        return None
    turn_id = receipt.get("turnId")
    if (not isinstance(turn_id, str) or not turn_id
            or operation.get("turnId") not in (None, turn_id)):
        raise SessionUnavailable("Notification turn does not match its delivery; inspect the worker")

    def completed(value: dict[str, Any]) -> dict[str, Any] | None:
        proof = value.get("settlement")
        native = store.record(child).get("native") or {}
        if not isinstance(proof, dict):
            return None
        if (proof.get("workerId") != reg["worker"]["workerId"]
                or proof.get("deliveryId") != token
                or proof.get("turnId") != turn_id
                or proof.get("threadId") != native.get("threadId")
                or proof.get("nativeSource") != store.codex_source(child)
                or proof.get("status") != "completed"
                or not isinstance(proof.get("text"), str)
                or not isinstance(proof.get("nativeEvidenceSha256"), str)
                or len(proof["nativeEvidenceSha256"]) != 64):
            raise SessionUnavailable("original Codex completed receipt lacks turn/source proof")
        return {**base, "status": "completed", "turnStatus": "completed",
                "turnId": turn_id, "report": proof["text"], "error": None}

    existing = completed(receipt)
    if existing is not None:
        return existing
    try:
        observed = reconcile_turn(child, delivery_id=token,
                                  expected_generation=store.generation)
    except (SessionUnavailable, OSError):
        observed = None
    store.revalidate()
    if isinstance(observed, dict):
        row = observed.get("delivery") or {}
        parent = observed.get("worker") or {}
        if (row.get("delivery_id") == token and row.get("turn_id") == turn_id
                and parent.get("worker_id") == reg["worker"]["workerId"]):
            receipt = codex_delivery.read_receipt(child, token) or receipt
            existing = completed(receipt)
            if existing is not None:
                return existing
            native_outcome = observed.get("native_status")
            if native_outcome in {"failed", "interrupted", "cancelled"}:
                return {**base, "status": native_outcome,
                        "turnStatus": native_outcome, "turnId": turn_id,
                        "error": observed.get("native_error"), "report": None}
    # Current gate/terminal is advisory; it must belong to the same accepted
    # turn, and must never turn a submitted-unknown/old gate into completion.
    try:
        snapshot = wait_worker(child, timeout=0,
                               expected_generation=store.generation)
    except (SessionUnavailable, OSError):
        return None
    store.revalidate()
    if (snapshot.get("worker") != child
            or snapshot.get("active_turn_ref") != codex_delivery.turn_ref_for(turn_id)):
        return None
    if snapshot.get("state") == "waiting":
        pending = snapshot.get("pending_approvals") or snapshot.get("pendingApprovals") or []
        return {**base, "status": "waiting", "turnStatus": "waiting",
                "turnId": turn_id, "waitingFor": pending,
                "workerState": "waiting", "report": None}
    return None


def _original_claude_settlement(reg: dict[str, Any]) -> dict[str, Any] | None:
    """Only original Claude receipt/Stop or its exact start-unique turn gate."""
    from ccx.claude import delivery as claude_delivery
    from ccx.errors import SessionUnavailable
    from ccx.claude.managed_turns import settlement_for_turn_ref
    from ccx.ops.wait import reconcile_turn, wait_worker
    from ccx.store.root import NativeEffectIndeterminate

    store, _parent, child = _original_notification_registration(reg)
    operation = reg["operation"]
    token = operation["deliveryId"]
    try:
        receipt = claude_delivery.read_receipt(child, token)
    except claude_delivery.ClaudeDeliveryError as exc:
        raise SessionUnavailable("original Claude receipt unreadable") from exc
    if receipt is None:
        return None
    if (receipt.get("workerId") != reg["worker"]["workerId"]
            or receipt.get("deliveryId") != token
            or receipt.get("requestedMode") != operation["requestedMode"]
            or receipt.get("promptSha256") != operation["promptSha256"]
            or receipt.get("promptByteCount") != operation["promptByteCount"]
            or receipt.get("cap") != operation["cap"]
            or receipt.get("callerMetadata") != operation["callerMetadata"]):
        raise SessionUnavailable("Claude notification belongs to another worker; check its ID")
    native = store.record(child).get("native") or {}
    sid = native.get("sessionId")
    if not isinstance(sid, str) or not sid:
        raise SessionUnavailable("original Claude notification native session unbound")
    base = {"workerState": (store.record(child).get("lifecycle") or {}).get("state"),
            "turnRef": receipt.get("turnRef"), "report": None}
    if receipt["state"] == "rejected":
        return {**base, "status": "rejected", "turnStatus": "rejected",
                "error": receipt.get("rejectionMessage") or receipt.get("rejectionCode")}
    # Prepared (including submitted-unknown host refusal) and host-owned
    # before original placement have no proven native turn and never complete.
    if receipt["state"] != "accepted":
        return None
    turn_ref = receipt.get("turnRef")
    if (receipt.get("route") != "new_turn" or receipt.get("placement") != "new_turn"
            or not isinstance(turn_ref, str)
            or not turn_ref.startswith("ccx-turn:claude:v1:")
            or len(turn_ref) <= len("ccx-turn:claude:v1:")):
        return None

    # Existing original reconciler already validates global intent, native
    # SID/source/spec/turnRef and forward-repairs the two Stop/effect writes.
    try:
        observed = reconcile_turn(child, delivery_id=token,
                                  expected_generation=store.generation)
    except (SessionUnavailable, NativeEffectIndeterminate,
            claude_delivery.ClaudeDeliveryError, OSError):
        observed = None
    store.revalidate()
    if isinstance(observed, dict) and observed.get("status") == "settled":
        row = observed.get("delivery") or {}
        try:
            receipt = claude_delivery.read_receipt(child, token) or receipt
        except claude_delivery.ClaudeDeliveryError as exc:
            raise SessionUnavailable("original Claude settlement unreadable") from exc
        proof = receipt.get("settlement")
        if (row.get("delivery_id") == token and row.get("turn_ref") == turn_ref
                and isinstance(proof, dict) and proof.get("workerId") == reg["worker"]["workerId"]
                and proof.get("deliveryId") == token and proof.get("turnRef") == turn_ref
                and proof.get("nativeSource") == f"claude-session:{sid}"
                and proof.get("status") == "completed"
                and isinstance(proof.get("text"), str)):
            return {**base, "status": "completed", "turnStatus": "completed",
                    "turnRef": turn_ref, "report": proof["text"], "error": None}

    # A positive Stop receipt whose companion effect is still inconclusive
    # cannot be replaced by a contradictory negative sidecar observation.
    if isinstance(receipt.get("settlement"), dict):
        return None
    # Retained sidecar is exact by turnRef, and can close interrupted/unknown
    # without inventing a final assistant message, failed or cancelled result.
    terminal, available = settlement_for_turn_ref(reg["worker"]["workerId"], turn_ref)
    if available and isinstance(terminal, dict) and terminal.get("turnRef") == turn_ref:
        if terminal.get("outcome") in {"interrupted", "unknown"}:
            status = terminal["outcome"]
            return {**base, "status": status, "turnStatus": status,
                    "turnRef": turn_ref, "report": None,
                    "error": terminal.get("evidence")}

    # A currently blocked gate is actionable only on this accepted native
    # turn. An older/successor gate or host-owned pre-start slot cannot wake it.
    try:
        snapshot = wait_worker(child, timeout=0,
                               expected_generation=store.generation)
    except (SessionUnavailable, OSError):
        return None
    store.revalidate()
    waiting = snapshot.get("waiting_for")
    if (snapshot.get("worker") == child and snapshot.get("state") == "waiting"
            and snapshot.get("active_turn_ref") == turn_ref
            and isinstance(waiting, dict) and waiting.get("id")):
        return {**base, "status": "waiting", "turnStatus": "waiting",
                "turnRef": turn_ref, "workerState": "waiting",
                "waitingFor": waiting, "report": None}
    return None


def settlement_for_registration(
    reg: dict[str, Any],
    registration_snapshot: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    _original_notification_registration(reg)
    if reg.get("settlementMode") == "orchestration":
        # A notifier/client may hold an older snapshot. Read the persisted
        # original freeze and wake clock before any new turn/report observation.
        current = read_registration(reg["eventId"])
        if current is None:
            return None
        _original_notification_registration(current)
        reg = current
        frozen = reg.get("operationSettlement")
        if not isinstance(frozen, dict):
            base = _original_notification_settlement(reg)
            if base is None:
                return None
            def freeze(row):
                if row.get("state") == "armed":
                    row.setdefault("operationSettlement", dict(base))
            updated = mutate_registration(reg["eventId"], freeze)
            if updated is None or not isinstance(updated.get("operationSettlement"), dict):
                return None
            reg = updated
            frozen = reg["operationSettlement"]
        return _recursive_codex_settlement(reg, frozen, registration_snapshot)
    if reg["worker"]["engine"] == "controller":
        from ccx.controllers.notify import settlement
        return settlement(reg)
    return _original_notification_settlement(reg)


def _original_notification_settlement(reg: dict[str, Any]) -> dict[str, Any] | None:
    if reg["source"] == "compact" or (
            reg["source"] == "notify" and reg["operation"].get("kind") in {
                "claude-compact", "codex-compact"}):
        return _compact_settlement(reg)
    if reg["source"] == "notify" and reg["operation"]["kind"] == "claude-next-turn":
        return _future_settlement(reg)
    if (reg["source"] in {"notify", "new"}
            and reg["operation"].get("selectionMode") == "status-only"):
        state = reg["operation"]["stableState"]
        return {"status": state, "workerState": state,
                "turnStatus": None, "report": None, "error": None}
    if reg["source"] == "input":
        _store, _parent, child = _original_notification_registration(reg)
        operation = reg["operation"]
        intent = _store.read_receipt(child, "answer", operation["responseId"])
        if intent is None or intent["state"] != "accepted":
            return None  # A submitted-unknown / rejected gate is never completed.
        if reg["worker"]["engine"] == "claude":
            original = _store.record(child).get("waitingFor")
            if isinstance(original, dict) and original.get("id") == operation["gateId"]:
                return None  # Original permission remains unresolved, not a new gate.
        else:
            current = _store.record(child).get("pendingApprovals") or []
            if any(isinstance(gate, dict) and gate.get("gateId") == operation["gateId"]
                   and gate.get("turnId") == operation["turnId"] for gate in current):
                return None
        observed = _receiptless_settlement(reg)
        if observed is not None and observed.get("status") == "waiting":
            gates = observed.get("waitingFor") or []
            if reg["worker"]["engine"] == "claude":
                if not isinstance(gates, dict) or gates.get("id") in {None, operation["gateId"]}:
                    return None  # A new actionable gate, on the same frozen R.
            elif not any(isinstance(gate, dict) and gate.get("gateId") != operation["gateId"]
                         and gate.get("turnId") == operation["turnId"] for gate in gates):
                return None
        return observed
    if reg["source"] == "notify" and reg["operation"].get("nativeProof") == "original-native-turn":
        return _receiptless_settlement(reg)
    if reg["worker"]["engine"] == "claude":
        return _original_claude_settlement(reg)
    return _original_codex_settlement(reg)
