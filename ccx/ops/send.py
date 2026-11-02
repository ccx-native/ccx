from __future__ import annotations
import json
import time
from typing import Any, Callable
from ccx.codex import delivery as codex_delivery
from ccx.controllers.orchestrator import require_notify_parent
from ccx.notify.registry import create_registration as create_parent_notification, public_registration as public_parent_notification, read_registration as read_parent_notification
from ccx.errors import (
    DeliveryRejected,
    InvalidRequest,
    OperationConflict,
    SessionUnavailable,
)
from ccx.ops.common import _normalize_caller_metadata, split_target

# How long a send keeps checking the receipt after the host's reply timed out.
_OWNERSHIP_GRACE_S = 10.0


def send_turn(
    target: str, text: str, *, delivery_id: str,
    mode: str = "new_turn_only", timeout: float | None = None,
    cap: dict | None = None,
    notify_parent: bool = False,
    settle: bool = False,
    caller_metadata: dict[str, Any] | None = None,
    expected_generation: str | None = None,
    _native_submit: Callable | None = None,
    _observe_settlement: Callable | None = None,
) -> dict[str, Any]:
    """Actual Claude semantic send with full-ID native fence.

    Retains original claude_delivery prepare/owned/accepted/rejected state
    transitions, original delivery ID/prompt hash/turn ref. The injected
    bounded sender is fake/test-only until real host socket adoption is
    reviewed. With no sender, refuse before any receipt side effects.
    Native model completion is observed outside the activation lease.
    """
    from ccx.claude import delivery as claude_delivery
    from ccx import constants
    from ccx.store.root import require_ccx_active, NativeEffectIndeterminate, NativeActionDenied, _HASH
    real_native_transport = _native_submit is None
    if real_native_transport:
        owner_handle, owner_engine, owner_id = split_target(target)
        if owner_engine == "codex":
            from ccx.codex.runtime import daemon_rpc
            from ccx.codex.delivery import read_receipt as _read_native_semantic
            from ccx.constants import CODEXD_RPC_GENERATION
            def _native_submit(handle, native_thread, prompt, token, budget):
                original = _read_native_semantic(handle, token)
                if not isinstance(original, dict):
                    raise NativeEffectIndeterminate(
                        "Codex original semantic receipt missing before native request"
                    )
                return daemon_rpc({
                    "op": "native-send",
                    "workerId": owner_id,
                    "threadId": native_thread,
                    "text": prompt, "deliveryId": token,
                    "clientUserMessageId": original["clientUserMessageId"],
                    "cap": cap,
                    "rpcGeneration": CODEXD_RPC_GENERATION,
                }, timeout=budget)
        else:
            # Existing original Claude host IPC verifies the exact server
            # root/generation/full-worker peer and Linux SO_PEERCRED PID/start.
            from ccx.rpc.rpc import _rpc
            def _native_submit(handle, native_id, prompt, token, budget):
                msg = {"op": "deliver", "text": prompt,
                       "delivery_id": token, "mode": mode}
                if cap is not None:
                    msg["cap"] = cap
                return _rpc(handle, msg, timeout=budget)
    if not callable(_native_submit):
        raise SessionUnavailable("Worker connection unavailable; check its status and retry")
    if caller_metadata is not None and not notify_parent:
        raise InvalidRequest("send: caller_metadata requires notify_parent=true")
    handle, engine, worker_id = split_target(target)
    if settle and (engine != "codex" or not notify_parent):
        raise InvalidRequest("send: --settle requires Codex --notify-parent")
    if engine == "codex":
        if mode not in codex_delivery.MODES:
            raise InvalidRequest(f"send: unsupported Codex mode {mode!r}")
        if notify_parent and mode != "new_turn_only":
            raise InvalidRequest("Codex send --notify-parent requires new_turn_only")
        if cap is not None:
            from ccx.workers import _check_caps
            _check_caps(cap, engine="codex")
    if engine == "claude":
        if mode not in claude_delivery.MODES:
            raise InvalidRequest(f"send: unsupported Claude mode {mode!r}")
        if notify_parent and mode != "new_turn_only":
            raise InvalidRequest("Claude send --notify-parent requires new_turn_only")
        try:
            cap = claude_delivery.validate_cap(cap)
        except claude_delivery.ClaudeDeliveryError as exc:
            raise InvalidRequest(str(exc)) from exc
    if not isinstance(text, str) or not text.strip():
        raise InvalidRequest("send: prompt text is required")
    if not claude_delivery.valid_delivery_id(delivery_id):
        raise InvalidRequest("send: delivery_id is missing or unsafe")
    if engine == "codex":
        from ccx.errors import SendIndeterminate
        # codexd refuses a request frame over 1 MiB before Codex sees it;
        # refuse here, before any receipt, so the send is not left uncertain.
        if len(json.dumps(text)) > 1024 * 1024 - 64 * 1024:
            raise InvalidRequest("send: prompt is too large for Codex (limit is about 1 MB)")
        metadata = _normalize_caller_metadata(caller_metadata, notify_parent=notify_parent)
        parent = require_notify_parent() if notify_parent else None
        try:
            return _send_codex_original(
                handle, worker_id, text, delivery_id,
                _native_submit, _observe_settlement, mode=mode,
                real_native_transport=real_native_transport,
                expected_generation=expected_generation,
                parent=parent, caller_metadata=metadata, settle=settle,
                cap=cap,
            )
        except codex_delivery.DeliveryReceiptConflict as exc:
            raise OperationConflict(
                f"original Codex delivery {delivery_id!r} conflicts: {exc}",
                short=worker_id[:8], worker_id=worker_id,
            ) from exc
        except InvalidRequest as exc:
            if "notification operation is owned by a different parent or input" in str(exc):
                raise OperationConflict(str(exc), short=worker_id[:8],
                                        worker_id=worker_id) from exc
            raise
        except NativeEffectIndeterminate as exc:
            # Deliberate correction for BOTH ordinary and notification Codex
            # sends: a previously submitted native effect is never exit 3.
            current = codex_delivery.read_receipt(handle, delivery_id)
            raise SendIndeterminate(
                f"original Codex native delivery uncertain; reconcile {delivery_id!r}, never resend: {exc}",
                short=worker_id[:8], delivery_id=delivery_id,
                delivery_status=(current.get("state") if isinstance(current, dict)
                                 else "submitted-unknown"),
            ) from exc
    metadata = _normalize_caller_metadata(caller_metadata, notify_parent=notify_parent)
    parent = require_notify_parent() if notify_parent else None
    store = require_ccx_active(
        constants.CCX_ROOT, expected_generation=expected_generation,
    )
    rec = store.record(handle)
    native = rec.get("native")
    if not isinstance(native, dict) or not isinstance(native.get("sessionId"), str):
        raise NativeActionDenied("Claude session is missing; inspect the worker before sending")
    native_source = "claude-session:" + native["sessionId"]
    # No incidental receipt/lock directory creation for unactivated,
    # quarantined, wrong-engine or unowned workers. A terminal/owned original
    # delivery may be observed even if native capability was revoked later,
    # but a fresh or merely prepared send MUST pass the native guard first.
    try:
        prior = claude_delivery.require_matching(
            handle, delivery_id, text, mode=mode, cap=cap,
        )
    except claude_delivery.ClaudeDeliveryConflict as exc:
        if not notify_parent:
            raise
        raise OperationConflict(f"original Claude delivery {delivery_id!r} conflicts: {exc}",
                                short=worker_id[:8], worker_id=worker_id) from exc
    if (parent is not None and prior is not None
            and prior.get("callerMetadata") != metadata):
        raise OperationConflict("Delivery details changed; retry with the same message and delivery ID",
                                short=worker_id[:8], worker_id=worker_id)
    if prior is None or prior.get("state") == "prepared":
        store.authorize_native_action(handle, "send")
        # A prepared semantic receipt can coexist with an already submitted
        # original native effect. Never revive or dispatch this old ID again.
        effect = store.read_receipt(handle, "claude-delivery", delivery_id)
        if effect is not None and effect.get("state") != "prepared":
            raise NativeEffectIndeterminate(
                "original Claude delivery already submitted; reconcile exact id, never revive/replay"
            )
        if cap:
            from ccx.claude.usage import fetch_usage, cap_tripped
            from ccx.constants import USAGE_STALE_LIMIT_S
            from ccx.errors import BudgetStopped
            trip = cap_tripped(fetch_usage(stale_limit=USAGE_STALE_LIMIT_S), cap)
            if trip:
                key, used, limit = trip
                reason = (f"usage cap {key} already exceeded "
                          f"({used:g}% >= {limit:g}%); no turn delivered")
                raise BudgetStopped(f"budget stop: {reason}", short=worker_id[:8],
                                    reason=reason, phase="preflight")
    notification = None
    if parent is not None and prior is not None:
        # The semantic key rejects a different parent before even
        # an original Claude host resume. Never retroactively arm accepted
        # ordinary sends or re-arm a cancelled notification.
        try:
            notification = create_parent_notification(
                parent=parent, worker_engine="claude", worker_id=worker_id,
                worker_name=rec.get("name"), source="send",
                operation={"kind": "claude-turn", "deliveryId": delivery_id},
            )
        except InvalidRequest as error:
            if "notification operation is owned by a different parent or input" in str(error):
                raise OperationConflict(str(error), short=worker_id[:8],
                                        worker_id=worker_id) from error
            raise
    if prior is None or prior.get("state") == "prepared":
        if real_native_transport:
            # Strict sends may revive; auto may revive only a
            # recorded stopped original, never a served BUSY/BOOTING turn.
            # Same-token native effects were fenced before this branch.
            if (mode == "new_turn_only"
                    or (mode == "auto" and
                        (rec.get("lifecycle") or {}).get("state")
                        in ("dead", "crashed", "idle-killed", "budget-stopped"))):
                ensure_ready(handle, expected_generation=store.generation)
            rec = store.record(handle)
            # No real host -> no first semantic/native receipt creation.
            # Same-connection SO_PEERCRED server PID/start/root/owner proof
            # and independent original provider SessionStart must exist.
            from ccx.rpc.rpc import _rpc
            try:
                response = _rpc(handle, {"op": "startup-status"}, timeout=.5)
            except (OSError, ValueError) as exc:
                if mode == "steer_only":
                    raise SessionUnavailable(
                        f"steer: original Claude startup status unavailable: {exc}",
                        short=worker_id[:8],
                    ) from exc
                raise
            lifecycle = rec.get("lifecycle") or {}
            source = lifecycle.get("nativeStartProof")
            if (not isinstance(response,dict)
                    or response.get("ok") is not True
                    or response.get("workerId") != worker_id
                    or response.get("engine") != "claude"
                    or response.get("generation") != store.generation
                    or not isinstance(source, dict)
                    or source.get("sessionId") != native["sessionId"]
                    or response.get("nativeStartProof") != source):
                raise SessionUnavailable(
                    "Claude worker connection could not be verified for this session; "
                    "check its status; no delivery receipt written"
                )
            if mode == "steer_only":
                # A revived worker can have an owned new-turn receipt before
                # Claude reports the native BUSY state. Wait only for a recent
                # turn on a previously used owner, never for an idle worker.
                try:
                    recent = [
                        row for row in claude_delivery.iter_receipts(handle)
                        if row.get("requestedMode") == "new_turn_only"
                        and row.get("state") in {"owned", "accepted"}
                        and row.get("settlement") is None
                        and (row.get("ownedAtMs") or row.get("acceptedAtMs") or 0)
                        > (rec.get("lastSettledAt") or 0)
                        and 0 <= int(time.time() * 1000) - (
                            row.get("ownedAtMs") or row.get("acceptedAtMs") or 0
                        ) <= 10_000
                    ]
                except (claude_delivery.ClaudeDeliveryError, OSError, ValueError) as exc:
                    raise SessionUnavailable(
                        f"steer: original Claude receipt inventory unavailable: {exc}",
                        short=worker_id[:8],
                    ) from exc
                revived_pending = (
                    bool((rec.get("stats") or {}).get("turns"))
                    and bool(recent)
                    and lifecycle.get("state") in {"idle", "busy", "resuming", "booting"}
                )
                deadline_busy = time.monotonic() + (4.0 if revived_pending else 0.0)
                while True:
                    try:
                        status = _rpc(
                            handle, {"op": "status"},
                            timeout=min(.5, max(.05, deadline_busy-time.monotonic()))
                            if revived_pending else .5,
                        )
                    except (OSError, ValueError) as exc:
                        raise SessionUnavailable(
                            f"steer: original Claude host status unavailable: {exc}",
                            short=worker_id[:8],
                        ) from exc
                    if not isinstance(status, dict) or status.get("ok") is not True:
                        raise SessionUnavailable(
                            "steer: original Claude host status not authenticated",
                            short=worker_id[:8],
                        )
                    if status.get("state") == "busy":
                        break
                    if (not revived_pending or status.get("state") not in
                            {"idle", "resuming", "booting"}
                            or time.monotonic() >= deadline_busy):
                        raise DeliveryRejected(
                            "steer requires a currently busy Claude turn",
                            short=worker_id[:8], state=status.get("state"),
                            refusal="waiting" if status.get("state") == "waiting" else "idle",
                        )
                    store.revalidate()
                    fresh = store.record(handle)
                    if ((fresh.get("lifecycle") or {}).get("nativeStartProof") != source):
                        raise SessionUnavailable(
                            "steer: revived Claude host identity changed during readiness",
                            short=worker_id[:8],
                        )
                    time.sleep(min(.15, max(0.0, deadline_busy-time.monotonic())))
    # The original semantic receipt is the public acceptance/pending state.
    # A previously owned token must remain observable even if the worker
    # later becomes terminal-only; observation never invokes a provider.
    try:
        existing, _fresh = claude_delivery.prepare(
            handle, delivery_id, text, mode=mode, cap=cap, timeout=timeout,
            **({"caller_metadata": metadata} if parent is not None else {}),
        )
    except claude_delivery.ClaudeDeliveryConflict as exc:
        if not notify_parent:
            raise
        raise OperationConflict(f"original Claude delivery {delivery_id!r} conflicts: {exc}",
                                short=worker_id[:8], worker_id=worker_id) from exc
    except claude_delivery.ClaudeDeliveryBusy as exc:
        raise SessionUnavailable(
            f"steer: Claude delivery receipt lock contested: {exc}"
            if mode == "steer_only" else f"Claude delivery receipt lock contested: {exc}",
            short=worker_id[:8],
        ) from exc
    if parent is not None:
        if notification is None:
            notification = create_parent_notification(
                parent=parent, worker_engine="claude", worker_id=worker_id,
                worker_name=rec.get("name"), source="send",
                operation={"kind": "claude-turn", "deliveryId": delivery_id},
            )
        # The original notification's file AND directory are already fsynced.
        # Never submit original Claude host input if supervisor cannot start.
        from ccx.notify.daemon import ensure_notify_daemon
        ensure_notify_daemon()
    def public(value):
        return {
            "worker": {"worker": handle, "worker_id": worker_id,
                       "engine": "claude", "short": worker_id[:8]},
            "delivery": claude_delivery.public_receipt(value),
            "status": value["state"],
            "settled": isinstance(value.get("settlement"), dict),
            "notification": (public_parent_notification(
                read_parent_notification(notification["eventId"]) or notification,
            ) if notification is not None else None),
        }
    if existing["state"] in ("accepted", "rejected"):
        if (existing["state"] == "accepted"
                and existing.get("settlement") is None
                and _observe_settlement is not None
                and existing.get("placement") == "new_turn"):
            existing = _settle_claude_original(
                handle, worker_id, native_source, delivery_id,
                existing["turnRef"], _observe_settlement,
            )
        return public(existing)
    identity = claude_delivery.spec_digest(existing)
    if existing["state"] == "owned":
        durable_effect = store.read_receipt(
            handle, "claude-delivery", delivery_id,
        )
        if (isinstance(durable_effect, dict)
                and durable_effect.get("intentSha256") == identity
                and durable_effect.get("state") == "submitted-unknown"):
            # A retry may report the original HOST-OWNED pending receipt.
            # It MUST NOT dispatch again or infer a completed native turn.
            return public(existing)
        raise NativeEffectIndeterminate(
            "owned semantic token lacks original full-UUID native effect proof"
        )
    store.authorize_native_action(handle, "send")
    attempt = store.begin_native_intent(
        handle, action="send", family="claude-delivery",
        operation_id=delivery_id, intent_sha256=identity,
    )
    if attempt.replayed:
        raise NativeEffectIndeterminate(
            "native fence terminal but original placement receipt absent; "
            "reconcile original provider turn, never replay"
        )
    # The native-effect claim is the only pre-IPC submitted-unknown fence.
    # Leave the semantic receipt PREPARED until the host takes ownership.
    # If we pre-mark it owned, the real host correctly interprets deliver as
    # a replay and returns without ever sending bytes to the PTY.
    response = store.dispatch_native_intent_once(
        attempt,
        bounded_submit=lambda budget: _native_submit(
            handle, native["sessionId"], text, delivery_id, budget,
        ),
    )
    # The real host ACKs *durable ownership*, not native UserPromptSubmit/Stop.
    # Its reply can be lost (timeout) or carry a stale view, so the host's
    # durable receipt decides: if it owns or accepted the message, report
    # that instead of an indeterminate send.
    if "nativeEvidenceSha256" not in response:
        observed = claude_delivery.read_receipt(handle, delivery_id)
        # A busy host (e.g. just restarted) can take the message after our
        # short reply wait. Keep reading its receipt for a while; never resend.
        deadline = time.monotonic() + _OWNERSHIP_GRACE_S
        while (response.get("error") == "timeout"
               and (observed is None or observed.get("state") == "prepared")
               and time.monotonic() < deadline):
            time.sleep(0.1)
            observed = claude_delivery.read_receipt(handle, delivery_id)
        if not (response.get("ok") is True
                and isinstance(response.get("delivery"), dict)
                and response["delivery"].get("delivery_id") == delivery_id):
            try:
                import os
                from ccx.claude.host.events import _open_event_journal, _stamp_event
                preview = json.dumps({
                    "keys": sorted(str(key) for key in response),
                    "error": str(response.get("error") or ""),
                }, ensure_ascii=False)[:300]
                event = _stamp_event({
                    "type": "delivery_reply_unrecognized", "delivery_id": delivery_id,
                    "reply": preview,
                    "receipt_state": observed.get("state") if observed else None,
                }, short=worker_id, seq=1,
                    epoch=f"client-{os.getpid()}-{time.time_ns()}", ts=time.time())
                with _open_event_journal(store, worker_id) as events:
                    events.write((json.dumps(event) + "\n").encode())
            except Exception:
                pass
        if (observed is not None
                and observed.get("state") in ("owned", "accepted")
                and ((observed.get("route") == "new_turn"
                      and mode in ("new_turn_only", "auto"))
                     or (observed.get("route") == "steer"
                         and mode in ("auto", "steer_only")
                         and isinstance(observed.get("targetTurnRef"), str)
                         and observed.get("targetTurnRef")))):
            store.revalidate()
            return public(observed)
        raise NativeEffectIndeterminate(
            "host ownership of the message is unconfirmed"
            + (f" ({response['error']})" if response.get("error") else "")
        )
    if (response.get("workerId") != worker_id
            or response.get("engine") != "claude"
            or response.get("deliveryId") != delivery_id
            or response.get("nativeSource") != native_source
            or response.get("sessionId") != native["sessionId"]
            or not isinstance(response.get("nativeEvidenceSha256"), str)
            or not _HASH.fullmatch(response["nativeEvidenceSha256"])):
        raise NativeEffectIndeterminate(
            "native acknowledgement not tied to original source/delivery"
        )
    if (response.get("accepted") is False
            and response.get("nativeStatus") == "rejected"):
        code = response.get("rejectionCode")
        if not isinstance(code, str) or not code:
            raise NativeEffectIndeterminate("native rejection has no exact original code")
        rejected = claude_delivery.mark_rejected(
            handle, delivery_id, code=code,
            message=str(response.get("rejectionMessage") or code),
            timeout=timeout,
        )
        store.settle_native_intent(
            attempt, state="rejected",
            native_evidence_sha256=response["nativeEvidenceSha256"],
        )
        return public(rejected)
    if (response.get("accepted") is not True
            or not isinstance(response.get("turnRef"), str)
            or not response["turnRef"].startswith("ccx-turn:claude:")):
        raise NativeEffectIndeterminate(
            "original Claude native accepted turn reference unproven; no replay"
        )
    # The direct positive-ACK route is injected/test-only. Unlike the real
    # authenticated host's ownership receipt it must explicitly prove its
    # original route; auto must never silently become a new turn.
    ack_route = "new_turn" if mode == "new_turn_only" else response.get("route")
    target_ref = response.get("targetTurnRef")
    if (ack_route not in ({"steer"} if mode == "steer_only"
                          else {"new_turn", "steer"})
            or (ack_route == "steer"
                and (not isinstance(target_ref, str)
                     or target_ref != response["turnRef"]))):
        raise NativeEffectIndeterminate(
            "original direct native ACK lacks exact auto/steer route or target"
        )
    # After original positive acceptance only, advance existing semantics.
    # Ownership-only host replies are not valid positive native evidence.
    current_semantic = claude_delivery.read_receipt(handle, delivery_id)
    if current_semantic["state"] == "prepared":
        current_semantic = claude_delivery.mark_owned(
            handle, delivery_id,
            route=ack_route,
            target_turn_ref=target_ref if ack_route == "steer" else None,
            timeout=timeout,
        )
    if (current_semantic["state"] == "owned"
            and not current_semantic.get("maybeWritten")):
        claude_delivery.mark_attempt(
            handle, delivery_id, maybe_written=True, timeout=timeout,
        )
    accepted = claude_delivery.mark_accepted(
        handle, delivery_id,
        placement="current_turn" if ack_route == "steer" else "new_turn",
        turn_ref=response["turnRef"],
        evidence=response["nativeEvidenceSha256"], timeout=timeout,
    )
    store.settle_native_intent(
        attempt, state="accepted",
        native_evidence_sha256=response["nativeEvidenceSha256"],
    )
    if _observe_settlement is not None and accepted.get("placement") == "new_turn":
        accepted = _settle_claude_original(
            handle, worker_id, native_source, delivery_id,
            response["turnRef"], _observe_settlement,
        )
    return public(accepted)


def _settle_claude_original(
    handle: str, worker_id: str, source: str, delivery_id: str,
    turn_ref: str, observe_completion: Callable,
) -> dict[str, Any]:
    """Confirm original turn after release of the activation dispatch lease."""
    from ccx.claude import delivery as claude_delivery
    from ccx.store.root import NativeEffectIndeterminate
    proof = observe_completion(handle, turn_ref)
    if (not isinstance(proof, dict)
            or proof.get("workerId") != worker_id
            or proof.get("nativeSource") != source
            or proof.get("deliveryId") != delivery_id
            or proof.get("turnRef") != turn_ref
            or proof.get("status") != "completed"):
        raise NativeEffectIndeterminate(
            "provider-native completion source or status unproven"
        )
    return claude_delivery.mark_settled(
        handle, delivery_id, turn_ref=turn_ref,
        original_positive_completion=proof,
    )


def _ensure_original_claude_ready(
    handle: str, worker_id: str, store, timeout: float | None,
) -> dict[str, Any]:
    """One source-bound original host lifetime, with no semantic model turn."""
    from ccx.errors import DeliveryRejected, SessionUnavailable
    from ccx.rpc.rpc import _rpc, _respawn
    from ccx.store.liveness import owned_pid_alive
    from ccx.store.root import NativeActionDenied
    import os

    try:
        store.authorize_native_action(handle, "resume")
    except NativeActionDenied as exc:
        raise SessionUnavailable(
            f"Claude cannot resume; inspect the worker: {exc}",
            short=worker_id[:8],
        ) from exc
    initial = store.record(handle)
    original_session = (initial.get("native") or {}).get("sessionId")
    if not isinstance(original_session, str) or not original_session:
        raise SessionUnavailable("Claude session is unavailable; inspect the worker",
                                 short=worker_id[:8])
    deadline = time.monotonic() + (180 if timeout is None else timeout)

    def probe():
        # This same-connection source proof is read-only. In particular an
        # unproven host never receives even a general status/native send probe.
        attested = _rpc(handle, {"op": "startup-status"}, timeout=.35)
        current = store.record(handle)
        if not isinstance(attested, dict) or attested.get("ok") is not True:
            return False, current
        reply = _rpc(handle, {"op": "status"}, timeout=.35)
        lc = current.get("lifecycle") or {}
        state = reply.get("state") if isinstance(reply, dict) and reply.get("ok") is True else None
        if state in ("busy", "waiting"):
            refusal = "busy" if state == "busy" else "state_unavailable"
            raise DeliveryRejected(
                f"Worker is {state}; wait until idle to send a new message.",
                short=worker_id[:8], refusal=refusal, state=state,
                delivery_status="rejected",
            )
        proof = lc.get("nativeStartProof")
        identity = (isinstance(proof, dict)
                    and proof.get("sessionId") == original_session
                    and proof.get("hostPid") == lc.get("hostPid")
                    and str(proof.get("hostStart")) == str(lc.get("hostStart"))
                    and proof.get("workerPid") == lc.get("workerPid")
                    and str(proof.get("workerStart")) == str(lc.get("workerStart"))
                    and owned_pid_alive(lc.get("hostPid"), lc.get("hostStart"))
                    and owned_pid_alive(lc.get("workerPid"), lc.get("workerStart")))
        if (state == "idle" and identity
                and attested.get("workerId") == worker_id
                and attested.get("generation") == store.generation
                and attested.get("nativeStartProof") == proof):
            return True, current
        return False, current

    ready, current = probe()
    if ready:
        return {"worker": handle, "worker_id": worker_id,
                "engine": "claude", "short": worker_id[:8],
                "session_id": original_session, "state": "idle", "resumed": False}
    # Original cwd is required to *relaunch*, but not to talk to an existing
    # authenticated live host whose cwd inode may outlive its pathname.
    if not isinstance(initial.get("cwd"), str) or not os.path.isdir(initial["cwd"]):
        raise SessionUnavailable("Claude working directory is unavailable; check its path",
                                 short=worker_id[:8])
    prior = current.get("lifecycle") or {}
    new_host = False
    if prior.get("state") not in ("booting", "resuming"):
        # The same recorded native JSONL drives both this eligibility check
        # and host._run_host's --resume argv. No history => no replacement
        # conversation under the old CCX/native IDs.
        import stat
        transcript = current.get("transcript")
        try:
            history = os.stat(transcript) if isinstance(transcript, str) else None
        except OSError:
            history = None
        if (history is None or not stat.S_ISREG(history.st_mode)
                or history.st_size == 0):
            raise SessionUnavailable(
                "Claude cannot resume without saved conversation history; "
                "empty transcript cannot be safely restarted",
                short=worker_id[:8],
            )
        # Existing respawn.lock decides whether THIS caller launched; it
        # serializes independent Python/CLI/protocol processes for one full UUID.
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise SessionUnavailable("Claude resume observation budget expired",
                                     short=worker_id[:8])
        rc, new_host = _respawn(handle, deadline=time.time() + remaining,
                                report_launch=True)
        if rc != 0:
            raise SessionUnavailable(
                "Claude did not resume; check worker status before retrying",
                short=worker_id[:8],
            )
    # A concurrently BOOTING/RESUMING original is observed, never replaced.
    while time.monotonic() < deadline:
        ready, _current = probe()
        if ready:
            store.revalidate()
            return {"worker": handle, "worker_id": worker_id,
                    "engine": "claude", "short": worker_id[:8],
                    "session_id": original_session, "state": "idle",
                    "resumed": bool(new_host)}
        time.sleep(min(.08, max(.0, deadline - time.monotonic())))
    raise SessionUnavailable("Claude was not ready before the timeout; check status and retry",
                             short=worker_id[:8])


def ensure_ready(
    target: str, *, timeout: float | None = 90,
    expected_generation: str | None = None,
) -> dict[str, Any]:
    """Ensure one *original* full-UUID native runtime is positively idle.

    Runtime-only: the Claude original host or Codex original singleton is
    restored without starting any model turn or creating native identity.
    """
    from ccx import constants
    from ccx.errors import InvalidRequest, SessionUnavailable
    from ccx.store.root import require_ccx_active, NativeActionDenied
    handle, engine, worker_id = split_target(target)
    if engine == "claude":
        if timeout is not None and (type(timeout) not in (int, float)
                                    or not 0 < timeout <= 600):
            raise InvalidRequest("Claude resume timeout must be positive or None")
        store = require_ccx_active(constants.CCX_ROOT,
                                   expected_generation=expected_generation)
        return _ensure_original_claude_ready(handle, worker_id, store, timeout)
    from ccx.codex.runtime import ensure_daemon, daemon_rpc, require_served_generation
    from ccx.constants import CODEXD_RPC_GENERATION
    if timeout is not None and (type(timeout) not in (int, float)
                                or not 0 < timeout <= 600):
        raise InvalidRequest("Codex resume timeout must be positive and bounded or None")
    store = require_ccx_active(constants.CCX_ROOT,
                               expected_generation=expected_generation)
    try:
        store.authorize_native_action(handle, "resume")
    except NativeActionDenied as error:
        raise SessionUnavailable("Codex cannot resume; inspect the worker: " + str(error),
                                 short=worker_id[:8]) from error
    record = store.record(handle)
    from ccx.codex.runtime import require_workspace
    require_workspace(record)
    deadline = None if timeout is None else time.monotonic() + timeout
    ensure_daemon(deadline=deadline)
    store.revalidate()
    remaining = (90.0 if deadline is None else max(0.0, deadline-time.monotonic()))
    if remaining <= 0:
        raise SessionUnavailable("Codex native resume budget expired", short=worker_id[:8])
    result = daemon_rpc({
        "op": "load", "short": worker_id,
        "timeout": min(90., remaining), "rpcGeneration": CODEXD_RPC_GENERATION,
    }, timeout=min(90.,remaining))
    if result.get("transport") == "served":
        require_served_generation(result, short=worker_id)
    state = result.get("state")
    if (result.get("transport") == "served" and result.get("ok") is True
            and result.get("loaded") is True and state in ("busy", "waiting")):
        # The live subscribed original reports a running turn or open gate:
        # a definite state refusal (legacy Codex send shape), not unavailable.
        raise DeliveryRejected(f"worker is {state}", short=worker_id[:8],
                               reason=f"worker is {state}", state=state,
                               refusal=("compaction-busy" if result.get("compacting")
                                        else "worker-state"))
    if (result.get("transport") != "served"
            or result.get("ok") is not True
            or result.get("loaded") is not True
            or result.get("state") != "idle"):
        from ccx.codex.runtime import explain_native_failure
        raise SessionUnavailable("Codex could not resume to an idle state; check worker status: "
                                 + explain_native_failure(
                                     str(result.get("error") or result.get("state"))),
                                 short=worker_id[:8])
    store.revalidate()
    return {"worker": handle, "worker_id": worker_id,
            "engine": "codex", "short": worker_id[:8],
            "thread_id": record["native"]["threadId"],
            "state": "idle", "resumed": bool(result.get("changed")),
            "loaded": True}


def _send_codex_original(
    handle: str, worker_id: str, text: str, delivery_id: str,
    submit_native: Callable, observe_settlement: Callable | None,
    *, mode: str = "new_turn_only",
    real_native_transport: bool = False,
    expected_generation: str | None = None,
    parent: dict[str, Any] | None = None,
    caller_metadata: dict[str, Any] | None = None,
    settle: bool = False,
    cap: dict | None = None,
) -> dict[str, Any]:
    """Codex half of EXISTING protocol_ops.send_turn, original semantic IDs."""
    from ccx.codex import delivery as codex_delivery
    from ccx import constants
    from ccx.store.root import NativeActionDenied, NativeEffectIndeterminate, _HASH, require_ccx_active
    from ccx.store.store import _digest
    store = require_ccx_active(
        constants.CCX_ROOT, expected_generation=expected_generation,
    )
    rec = store.record(handle)
    native = rec.get("native")
    if (not isinstance(native, dict)
            or not isinstance(native.get("threadId"), str)
            or native.get("threadSource") != store.codex_source(handle)):
        raise NativeActionDenied("Codex conversation is not ready; check worker status and retry")
    original = codex_delivery.read_receipt(handle, delivery_id)
    notification = None
    if original is not None:
        # Validate the immutable original token before any daemon start, native
        # resume, new semantic receipt or turn/start side effect.
        codex_delivery.require_matching(
            handle, delivery_id, text, mode=mode, cap=cap,
            **({"caller_metadata": caller_metadata} if parent is not None else {}),
        )
    if original is None or original.get("state") == "prepared":
        store.authorize_native_action(handle, "send")
        original_effect = store.read_receipt(handle, "codex-send", delivery_id)
        if (original_effect is not None
                and original_effect.get("state") not in ("prepared",)):
            raise NativeEffectIndeterminate(
                "Message was already submitted; reconcile its delivery instead of resending"
            )
    # A preexisting token must reject a different parent before a possible
    # original native thread reload. Notification registration owns the same-ID reuse rules.
    if parent is not None and original is not None:
        notification = create_parent_notification(
            parent=parent, worker_engine="codex", worker_id=worker_id,
            worker_name=rec.get("name"), source="send",
            operation={"kind": "codex-turn", "deliveryId": delivery_id},
            settle=settle,
        )
    if original is None or original.get("state") == "prepared":
        # Native auto picks the route in codexd, not from this stale record.
        # The idle-only load path is needed for a cold worker, never for an
        # already busy turn; explicit steer_only never loads/revives.
        if real_native_transport and (mode == "new_turn_only" or (
            mode == "auto" and
            (rec.get("lifecycle") or {}).get("state") not in ("busy", "waiting")
        )):
            try:
                ensure_ready(handle, expected_generation=store.generation)
            except DeliveryRejected as error:
                error.delivery_id = delivery_id
                raise
    # Existing owned/submitted/accepted/rejected receipts are read-only and
    # must never start a provider merely to reconcile the same delivery id.
    receipt, _fresh = codex_delivery.prepare(
        handle, delivery_id, text, mode=mode, cap=cap,
        **({"caller_metadata": caller_metadata} if parent is not None else {}),
    )
    if parent is not None:
        if notification is None:
            # Registration fsyncs the original receipt-bound event before any native
            # intent/submitted marker or first turn/start can be issued.
            notification = create_parent_notification(
                parent=parent, worker_engine="codex", worker_id=worker_id,
                worker_name=rec.get("name"), source="send",
                operation={"kind": "codex-turn", "deliveryId": delivery_id},
                settle=settle,
            )
        from ccx.notify.daemon import ensure_notify_daemon
        ensure_notify_daemon()  # failure leaves a prepared receipt + event, no native effect
    def public(value):
        return {
            "worker": {"worker": handle, "worker_id": worker_id,
                       "engine": "codex", "short": worker_id[:8]},
            "delivery": codex_delivery.public_receipt(value),
            "status": value["state"],
            "settled": isinstance(value.get("settlement"), dict),
            "notification": (public_parent_notification(
                read_parent_notification(notification["eventId"]) or notification,
            ) if notification is not None else None),
        }
    if receipt["state"] in ("accepted", "rejected"):
        if receipt["state"] == "rejected" and receipt.get("rejectionCode") == "budget-stopped":
            from ccx.errors import BudgetStopped
            reason = receipt.get("rejectionMessage") or "usage cap already reached"
            raise BudgetStopped(f"budget stop: {reason}",
                                short=worker_id[:8], reason=reason, phase="preflight")
        if (receipt["state"] == "accepted"
                and receipt.get("settlement") is None
                and observe_settlement is not None
                and codex_delivery.route(receipt) != "steer"):
            receipt = _settle_codex_original(
                handle, worker_id, native, delivery_id,
                receipt["turnId"], observe_settlement,
                expected_source=store.codex_source(handle),
            )
        return public(receipt)
    # For native auto/steer the immutable intent precedes route selection:
    # its client ID remains None even after codexd owns a start/steer kind.
    identity = _digest(codex_delivery.intent_spec(receipt))
    intent = store.begin_native_intent(
        handle, family="codex-send", action="send",
        operation_id=delivery_id, intent_sha256=identity,
    )
    if intent.replayed:
        raise NativeEffectIndeterminate(
            "Codex message outcome is recorded but the turn is missing; reconcile the delivery"
        )
    # Only strict sends freeze the preminted start ID in the caller.
    # Auto/steer stay prepared; the daemon owns their route under its lock.
    if mode == "new_turn_only":
        submitted = codex_delivery.mark_submitted(handle, delivery_id)
        if submitted.get("state") != "submitted":
            raise NativeEffectIndeterminate("Codex message was not submitted; inspect its delivery status")
    answer = store.dispatch_native_intent_once(
        intent, bounded_submit=lambda budget: submit_native(
            handle, native["threadId"], text, delivery_id, budget,
        ),
    )
    # Auto/steer freeze the native route and kind-correct client id in codexd.
    # Read its original receipt afresh; never compare ACK to pre-route None.
    frozen = codex_delivery.read_receipt(handle, delivery_id)
    frozen_route = codex_delivery.route(frozen) if isinstance(frozen, dict) else None
    frozen_target = frozen.get("targetTurnId") if isinstance(frozen, dict) else None
    frozen_client = frozen.get("clientUserMessageId") if isinstance(frozen, dict) else None
    rejected = answer.get("accepted") is False and answer.get("nativeStatus") == "rejected"
    if (not isinstance(frozen, dict)
            or _digest(codex_delivery.intent_spec(frozen)) != identity
            or frozen.get("requestedMode") != mode
            or (not rejected and frozen.get("state") != "submitted"
                and not (frozen.get("state") == "accepted"
                         # Already recovered from this answer's own journaled
                         # user message: the same turn, never a different one.
                         and frozen.get("turnId") == answer.get("turnId")))
            or answer.get("workerId") != worker_id
            or answer.get("engine") != "codex"
            or answer.get("deliveryId") != delivery_id
            or answer.get("nativeSource") != store.codex_source(handle)
            or answer.get("threadId") != native["threadId"]
            or answer.get("clientUserMessageId") != frozen_client
            or (mode != "new_turn_only"
                and answer.get("route") != frozen_route)
            or (frozen_route == "steer"
                and (not isinstance(frozen_target, str) or not frozen_target
                     or answer.get("targetTurnId") != frozen_target
                     or (not rejected and answer.get("turnId") != frozen_target)))
            or not isinstance(answer.get("nativeEvidenceSha256"), str)
            or not _HASH.fullmatch(answer["nativeEvidenceSha256"])):
        raise NativeEffectIndeterminate(
            "Codex response did not match this worker; inspect the delivery"
            "thread/source/route/turn/client identity; never replay"
        )
    if answer.get("accepted") is False and answer.get("nativeStatus") == "rejected":
        code = answer.get("rejectionCode")
        if not isinstance(code, str) or not code:
            raise NativeEffectIndeterminate("native rejection has no code")
        finished = codex_delivery.mark_rejected(
            handle, delivery_id, code=code,
            message=str(answer.get("rejectionMessage") or code),
        )
        store.settle_native_intent(
            intent, state="rejected",
            native_evidence_sha256=answer["nativeEvidenceSha256"],
        )
        if code == "budget-stopped":
            from ccx.errors import BudgetStopped
            reason = str(answer.get("rejectionMessage") or "usage cap reached")
            raise BudgetStopped(f"budget stop: {reason}", short=worker_id[:8],
                                reason=reason, phase="preflight")
        return public(finished)
    if answer.get("accepted") is not True or not isinstance(answer.get("turnId"), str) or not answer["turnId"]:
        raise NativeEffectIndeterminate("Codex turn was not confirmed accepted; reconcile its delivery")
    accepted = codex_delivery.mark_accepted(
        handle, delivery_id, turn_id=answer["turnId"],
    )
    store.settle_native_intent(
        intent, state="accepted",
        native_evidence_sha256=answer["nativeEvidenceSha256"],
    )
    if frozen_route != "steer":
        # A fast app-server can publish turn/completed before the caller
        # writes acceptance. Its original fsynced notification is enough to
        # settle without hydrating history or issuing another native turn.
        from ccx.codex.completion import settle_from_journal
        try:
            completed = settle_from_journal(store, handle, answer["turnId"])
            if completed is not None:
                accepted = completed
        except Exception:
            # Accepted placement remains authoritative. A bad/missing journal
            # is recoverable later by source-fenced native observation.
            pass
    if (observe_settlement is not None and frozen_route != "steer"
            and not accepted.get("settlement")):
        accepted = _settle_codex_original(
            handle, worker_id, native, delivery_id, answer["turnId"],
            observe_settlement, expected_source=store.codex_source(handle),
        )
    return public(accepted)


def _settle_codex_original(
    handle: str, worker_id: str, native: dict, delivery_id: str,
    turn_id: str, observe_completion: Callable,
    *, expected_source: str,
) -> dict[str, Any]:
    """Observe native turn/completed independently, without activation lock."""
    from ccx.codex import delivery as codex_delivery
    from ccx.store.root import NativeEffectIndeterminate
    owned = codex_delivery.read_receipt(handle, delivery_id)
    if isinstance(owned, dict) and codex_delivery.route(owned) == "steer":
        raise NativeEffectIndeterminate(
            "Codex steer acceptance is placement, not a completed-turn receipt"
        )
    original = observe_completion(handle, codex_delivery.turn_ref_for(turn_id))
    if (not isinstance(original, dict)
            or original.get("workerId") != worker_id
            or original.get("deliveryId") != delivery_id
            or original.get("nativeSource") != expected_source
            or original.get("threadId") != native["threadId"]
            or original.get("turnId") != turn_id
            or original.get("status") != "completed"):
        raise NativeEffectIndeterminate(
            "Codex turn completion could not be verified; inspect its status"
        )
    return codex_delivery.mark_settled(
        handle, delivery_id, turn_id=turn_id,
        original_positive_completion=original,
    )
