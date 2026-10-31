"""Codex configuration, CLI reconciliation, compact, telemetry and account reads."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from ccx.codex import runtime as runtime
from ccx.codex.telemetry import read_telemetry
from ccx.errors import DeliveryRejected, InvalidRequest, SessionNotFound, SessionUnavailable, worker_not_found
from ccx.store.worker_state import RUNTIME_ABSENT, RUNTIME_INCONCLUSIVE, RUNTIME_SERVED

_CONFIG_UNSET = object()

def _deadline(timeout: float | None) -> float | None:
    if timeout is not None and timeout < 0:
        raise InvalidRequest("timeout must be zero or greater")
    return None if timeout is None else time.monotonic() + timeout


def deliver_machine_wake(
    owner, payload: dict[str, Any], *, namespace: str, name: str,
    reconcile: bool, timeout: float = 65,
    expected_thread_id: str | None = None,
    expected_generation: str | None = None,
) -> dict[str, Any]:
    """Private original Codex machine output: one owned writer, never a worker send."""
    from ccx import constants
    from ccx.store.root import GenerationError, require_ccx_active
    from ccx.store.identity import WorkerOwner, IdentityError
    from ccx.ops.send import ensure_ready

    if (not isinstance(owner, WorkerOwner) or owner.engine != "codex"
            or not isinstance(payload, dict)
            or not isinstance(payload.get("eventId"), str) or not payload["eventId"]
            or not isinstance(payload.get("deliveryId"), str) or not payload["deliveryId"]
            or not isinstance(namespace, str) or not namespace or namespace == "ccx"
            or not isinstance(name, str) or not name
            or type(reconcile) is not bool
            or type(timeout) not in (int, float) or not 0 < timeout <= 65):
        raise InvalidRequest("Invalid Codex notification; check the recipient")
    event_id, delivery_id = payload["eventId"], payload["deliveryId"]
    deadline = time.monotonic() + timeout

    def no_effect(reason: str) -> dict[str, Any]:
        return {"ok": False, "definitive": True,
                **({"notAttempted": True} if not reconcile else {}),
                "error": reason}

    try:
        store = require_ccx_active(
            constants.CCX_ROOT, expected_generation=expected_generation,
        )
        current = store.resolve(owner.worker)
        if current != owner:
            return no_effect("Codex machine wake owner changed")
        source = store.codex_source(owner.worker)
        record = store.record(owner.worker)
        native = record.get("native") or {}
        thread_id = native.get("threadId")
        if (not isinstance(thread_id, str) or not thread_id
                or native.get("threadSource") != source
                or (expected_thread_id is not None
                    and thread_id != expected_thread_id)):
            return no_effect("Codex machine wake original thread/source differs")
        state = (record.get("lifecycle") or {}).get("state")
        if state in {"dead", "budget-stopped"}:
            return no_effect("explicitly stopped original Codex parent stays pending")
        if state not in {"idle", "idle-killed", "crashed", "busy", "waiting"}:
            return no_effect("original Codex parent is not ready for machine wake")
        reload = state in {"idle-killed", "crashed"}
        if state == "idle":
            observed = runtime.observe_worker(owner.worker_id, timeout=min(1., max(.01, deadline-time.monotonic())))
            if observed["presence"] == RUNTIME_INCONCLUSIVE:
                return no_effect("original Codex parent presence inconclusive")
            reload = observed["presence"] == RUNTIME_ABSENT
        if reload:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return no_effect("original Codex parent load budget expired")
            ready = ensure_ready(
                owner.worker, expected_generation=store.generation,
                timeout=min(60., remaining),
            )
            if (ready.get("worker_id") != owner.worker_id
                    or ready.get("thread_id") != thread_id
                    or ready.get("state") != "idle"):
                return no_effect("original Codex parent reload thread/owner unproven")
        store.revalidate()
    except (GenerationError, IdentityError, SessionUnavailable, DeliveryRejected,
            OSError, ValueError) as error:
        return no_effect(f"original Codex machine wake preflight unavailable: {error}")

    remaining = deadline - time.monotonic()
    if remaining <= 0:
        return no_effect("original Codex machine wake RPC budget expired")
    try:
        reply = runtime.rpc(
            owner.worker_id,
            {"op": "machine-wake", "eventId": event_id,
             "deliveryId": delivery_id, "payload": payload,
             "namespace": namespace, "name": name, "threadId": thread_id,
             "reconcile": reconcile},
            timeout=remaining,
        )
    except Exception as error:
        return {"ok": False, "pending": True,
                "error": f"original Codex machine wake RPC uncertain: {error}"}
    if reply.get("transport") != RUNTIME_SERVED:
        # Absent can follow a written request (daemon died before replying).
        return {"ok": False, "pending": True,
                "error": "original Codex machine wake RPC not served"}
    if reply.get("ok") is not True:
        refused = {key: value for key, value in reply.items() if key != "notAttempted"}
        if not reconcile and reply.get("definitive") is True and not reply.get("pending"):
            refused["notAttempted"] = True
        return refused
    if (reply.get("workerId") != owner.worker_id
            or reply.get("threadId") != thread_id
            or reply.get("deliveryId") != delivery_id
            or not (reply.get("reconciled") is True
                    or (isinstance(reply.get("turnId"), str) and reply["turnId"]))):
        return {"ok": False, "pending": True,
                "error": "original Codex machine wake lacks positive native placement proof"}
    try:
        store.revalidate()
    except GenerationError as error:
        return {"ok": False, "pending": True,
                "error": f"Codex machine wake generation changed after native effect: {error}"}
    return reply






def configure_worker(
    target: str,
    *,
    model: str | None | object = _CONFIG_UNSET,
    effort: str | None | object = _CONFIG_UNSET,
    timeout: float = 5,
    expected_generation: str | None = None,
) -> dict[str, Any]:
    """Set only *future* turns on one original Codex full-UUID worker.

    A serving daemon always owns mutation, even if this worker is unloaded.
    A proven absent daemon permits one fresh dotted CAS while holding the
    existing singleton spawn lock. Neither path loads a thread or sends text.
    """
    from math import isfinite
    from ccx import constants
    from ccx.codex.runtime import daemon_call, daemon_spawn_lock, observe_worker
    from ccx.store.root import GenerationError, RecordConflict, require_ccx_active
    from ccx.store.identity import EngineMismatch, IdentityError, UnknownWorker

    if model is _CONFIG_UNSET and effort is _CONFIG_UNSET:
        raise InvalidRequest("configure_worker requires model and/or effort")
    updates: dict[str, str | None] = {}
    for key, value in (("model", model), ("effort", effort)):
        if value is _CONFIG_UNSET:
            continue
        if value is not None and (not isinstance(value, str) or not value.strip()):
            raise InvalidRequest(f"{key} must be a non-empty string or None")
        normalized = value.strip() if isinstance(value, str) else None
        if key == "effort" and normalized not in (None, "low", "medium", "high", "xhigh", "max"):
            raise InvalidRequest("Codex effort must be low/medium/high/xhigh/max or None")
        updates[key] = normalized
    if type(timeout) not in (int, float) or not isfinite(timeout) or timeout <= 0:
        raise InvalidRequest("Codex configuration timeout must be positive and finite")
    try:
        store = require_ccx_active(constants.CCX_ROOT, expected_generation=expected_generation)
        owner = store.resolve(target)
        if owner.engine != "codex":
            raise EngineMismatch("Configuring Codex needs a worker handle")
        handle, worker_id = owner.worker, owner.worker_id
        record = store.record(handle)  # original owner/record validation
        if updates.get("model") is not None:
            from ccx.codex.runtime import validate_model
            validate_model(updates["model"], cwd=record["cwd"])
        # All persistence paths, including daemon set-config, follow preflight.
        deadline = time.monotonic() + timeout

        def time_left() -> float:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise SessionUnavailable("Codex configuration deadline expired", short=owner.short)
            return remaining

        def observe():
            return observe_worker(worker_id, timeout=min(1., time_left()))

        def daemon_mutation(probe: dict) -> dict | None:
            presence = probe["presence"]
            if presence == RUNTIME_INCONCLUSIVE:
                raise SessionUnavailable("Codex loadedness is inconclusive; no config written", short=owner.short)
            if not probe["daemon_served"]:
                return None
            # A loaded non-idle native turn can never receive a future-turn
            # configuration. codexd checks again under its worker.lock.
            if presence == RUNTIME_SERVED and probe["reply"].get("state") != "idle":
                raise DeliveryRejected(
                    f"Worker is {probe['reply'].get('state')}; wait until idle to change settings.",
                    short=owner.short, state=probe["reply"].get("state"),
                    refusal="busy",
                )
            reply = daemon_call(
                {"op": "set-config", "short": worker_id, "updates": updates},
                timeout=time_left(), short=owner.short,
            )
            if reply.get("transport") != RUNTIME_SERVED:
                raise SessionUnavailable(
                    "Codex settings may not have saved; check current settings before retrying",
                    short=owner.short,
                )
            if reply.get("ok") is not True:
                raise SessionUnavailable(
                    reply.get("error") or "Codex configuration refused",
                    short=owner.short,
                )
            store.revalidate()
            return store.record(handle)

        applied = daemon_mutation(observe())
        if applied is not None:
            return applied
        with daemon_spawn_lock(deadline):
            # No daemon may boot/load while this lock is held. It is not a
            # new configuration lock: this is the original daemon spawn fence.
            second = observe()
            applied = daemon_mutation(second)
            if applied is not None:
                return applied
            if second["presence"] != RUNTIME_ABSENT:
                raise SessionUnavailable(
                    "Codex runtime absentness unproven; no config written",
                    short=owner.short,
                )
            recorded = store.record(handle)
            state = (recorded.get("lifecycle") or {}).get("state")
            if state in {"busy", "waiting", "booting", "resuming"}:
                raise SessionUnavailable(
                    f"Codex worker is {state}; configuration requires idle or unloaded",
                    short=owner.short,
                )
            # Re-validate the durable lifecycle *under* the record CAS, too.
            return store.mutate_record(
                handle,
                patches={f"createArgs.{key}": value for key, value in updates.items()},
                expect={"lifecycle.state": state},
            )
    except UnknownWorker as exc:
        raise worker_not_found(target) from exc
    except IdentityError as exc:
        raise InvalidRequest(f"Invalid Codex worker handle: {exc}") from exc
    except RecordConflict as exc:
        raise SessionUnavailable("Codex state changed before saving settings; retry while idle", short=owner.short) from exc
    except GenerationError as exc:
        raise SessionUnavailable(f"Codex ACTIVE owner/config unavailable: {exc}") from exc


def caller_status(result: dict[str, Any]) -> tuple[str, bool]:
    """CLI/Python verdict for a reconcile result: ``(status, pending)``.

    A delivery CCX refused before submission (`prepared`, or `owned` with
    nothing written natively) is pending only in the receipt sense that the
    same id may still be sent. Nothing is in flight and nothing will land
    without a retry, so these adapters report it as ``not_delivered`` rather
    than leave a caller waiting on it. The public protocol keeps its documented
    ``pending`` + ``retry_same_id`` + ``last_refusal`` shape.
    """
    delivery = result.get("delivery")
    if (
        result.get("status") == "pending"
        and isinstance(delivery, dict)
        and delivery.get("recommended_action") == "retry_same_id"
    ):
        return "not_delivered", False
    return result["status"], bool(result.get("pending"))


def _semantic_status(public: dict[str, Any] | None) -> str:
    if not isinstance(public, dict):
        return "not_found"
    raw = public.get("status")
    if raw == "accepted":
        return "accepted"
    if raw == "rejected":
        return "rejected"
    return "pending"














def compact_intent_digest(
    worker_id: str, generation: str, thread_id: str,
    thread_source: str, operation_id: str,
) -> str:
    """One immutable full-UUID compact identity; budgets are not inputs."""
    from ccx.store.store import _digest

    return _digest({
        "workerId": worker_id, "engine": "codex", "generation": generation,
        "threadId": thread_id, "threadSource": thread_source,
        "operationId": operation_id, "action": "compact",
    })


def _compact_marker(value: dict[str, Any], operation_id: str):
    active = value.get("activeCompact")
    if isinstance(active, dict) and active.get("operationId") == operation_id:
        return "active", active
    last = value.get("lastCompact")
    if isinstance(last, dict) and last.get("operationId") == operation_id:
        return "last", last
    return None, None


def _compact_view(store, owner, operation_id: str, receipt: dict | None) -> dict:
    """Project exact-ID compaction status without native reads."""
    record = store.record(owner.worker)
    kind, marker = _compact_marker(record, operation_id)
    state = receipt.get("state") if receipt else None
    native = record.get("native") or {}
    digest = compact_intent_digest(
        owner.worker_id, store.generation, native.get("threadId"),
        native.get("threadSource"), operation_id,
    )
    if receipt is not None and (
        receipt.get("workerId") != owner.worker_id
        or receipt.get("engine") != "codex"
        or receipt.get("generation") != store.generation
        or receipt.get("family") != "codex-compact"
        or receipt.get("action") != "compact"
        or receipt.get("intentSha256") != digest
    ):
        raise SessionUnavailable("Codex compaction record changed; inspect its operation",
                                 short=owner.short)

    result = "not_found" if state is None else "unknown"
    if state == "prepared" and kind is None:
        result = "prepared"
    elif state == "rejected" and kind is None:
        result = "rejected"
    elif state in ("submitted-unknown", "accepted"):
        if kind == "active":
            result = "submitted"
        elif kind == "last":
            settled = (
                marker.get("nativeTurnStarted") is True
                and isinstance(marker.get("turnId"), str) and bool(marker["turnId"])
                and isinstance(marker.get("contextItemId"), str)
                and bool(marker["contextItemId"])
                and marker.get("nativeTurnSettled") is True
                and marker.get("nativeTurnStatus") == "completed"
                and marker.get("status") == "succeeded"
            )
            if settled:
                result = "succeeded"
            elif (marker.get("nativeTurnSettled") is True
                  and marker.get("nativeTurnStatus") in (
                      "failed", "interrupted", "cancelled"
                  )):
                result = "failed"
            else:
                result = "unknown"
    elif kind is not None:
        # A marker without an authenticated original effect is never proof.
        result = "unknown"

    before = marker.get("contextBefore") if isinstance(marker, dict) else None
    after = marker.get("contextAfter") if isinstance(marker, dict) else None
    completed = marker.get("completedAtMs") if isinstance(marker, dict) else None
    if (result != "succeeded" or not isinstance(after, dict)
            or type(completed) is not int
            or type(after.get("as_of_ms")) is not int
            or after["as_of_ms"] <= completed
            or (isinstance(before, dict)
                and type(before.get("as_of_ms")) is int
                and after["as_of_ms"] <= before["as_of_ms"])):
        after = None
    return {
        "worker": {"worker": owner.worker, "worker_id": owner.worker_id,
                   "short": owner.short, "engine": "codex"},
        "operationId": operation_id, "operation_id": operation_id,
        "status": result, "compacted": result == "succeeded",
        "receipt_status": state,
        "recommendedAction": ("wait" if result in ("submitted", "prepared")
                              else "inspect" if result == "unknown" else "none"),
        "contextBefore": before if isinstance(before, dict) else None,
        "contextAfter": after,
        "compactTurnId": marker.get("turnId") if isinstance(marker, dict) else None,
        "contextItemId": marker.get("contextItemId") if isinstance(marker, dict) else None,
        "nativeTurnStatus": (marker.get("nativeTurnStatus")
                             if isinstance(marker, dict) else None),
        "reason": (marker.get("reason") if isinstance(marker, dict) else None),
        "receipt": ({"operation_id": operation_id, "state": state,
                     "native_evidence_sha256": receipt.get("nativeEvidenceSha256")}
                    if receipt else None),
    }


def compact_observation(target: str, operation_id: str) -> dict[str, Any]:
    """Read only a generation-native effect and exact worker markers."""
    from ccx import constants
    from ccx.store.root import require_ccx_active, NativeActionDenied

    if (not isinstance(operation_id, str) or not operation_id
            or len(operation_id) > 128 or "\x00" in operation_id):
        raise InvalidRequest("Codex compact operation_id must be 1..128 non-NUL characters")
    store = require_ccx_active(constants.CCX_ROOT)
    owner = store.resolve(target)
    if owner.engine != "codex":
        raise InvalidRequest("Codex compaction needs a worker handle")
    rec = store.record(owner.worker)
    native = rec.get("native") or {}
    binding = store.native_activation(owner.worker)
    source = store.codex_source(owner.worker)
    if (binding.get("state") != "confirmed"
            or binding.get("source") != source
            or (binding.get("nativeBinding") or {}).get("threadId") != native.get("threadId")
            or not isinstance(native.get("threadId"), str)
            or not native["threadId"]
            or native.get("threadSource") != source):
        raise NativeActionDenied("Codex conversation could not be verified; inspect worker status")
    receipt = store.read_receipt(owner.worker, "codex-compact", operation_id)
    row = _compact_view(store, owner, operation_id, receipt)
    store.revalidate()
    return row


def compact_worker(
    target: str, *, operation_id: str, timeout: float | None = 90,
    wait: bool = True,
) -> dict[str, Any]:
    """One original Codex native compaction, never a legacy short-keyed RPC."""
    from ccx import constants
    from ccx.store.root import NativeEffectIndeterminate, NativeActionDenied, require_ccx_active
    from ccx.store.store import _digest
    from ccx.ops.send import ensure_ready
    from ccx.codex.runtime import daemon_call

    if (not isinstance(operation_id, str) or not operation_id
            or len(operation_id) > 128 or "\x00" in operation_id):
        raise InvalidRequest("Codex compact requires a valid operation_id")
    if timeout is not None and (type(timeout) not in (int, float)
                                or timeout < 0 or timeout != timeout
                                or timeout == float("inf")):
        raise InvalidRequest("Codex compact timeout must be finite nonnegative seconds")
    deadline = _deadline(timeout)
    store = require_ccx_active(constants.CCX_ROOT)
    owner = store.resolve(target)
    if owner.engine != "codex":
        raise InvalidRequest("Codex compact requires an exact Codex owner")
    handle = owner.worker
    rec = store.record(handle)
    native = rec.get("native") or {}
    source = store.codex_source(owner.worker)
    thread_id = native.get("threadId")
    binding = store.native_activation(handle)
    if (binding.get("state") != "confirmed"
            or binding.get("source") != source
            or (binding.get("nativeBinding") or {}).get("threadId") != thread_id
            or not isinstance(thread_id, str) or not thread_id
            or native.get("threadSource") != source):
        raise NativeActionDenied("Codex conversation is not ready for compaction; check status")
    digest = compact_intent_digest(
        owner.worker_id, store.generation, thread_id, source, operation_id,
    )
    prior = store.read_receipt(handle, "codex-compact", operation_id)
    row = _compact_view(store, owner, operation_id, prior)
    if prior is None:
        # An exact native marker without an original global receipt is
        # untrusted possible work, never permission for a fresh submit.
        if _compact_marker(store.record(handle), operation_id)[0] is not None:
            return _wait_for_compact(row, handle, operation_id, deadline, wait)
    elif prior["state"] != "prepared" or row["status"] == "unknown":
        return _wait_for_compact(row, handle, operation_id, deadline, wait)

    # Genuine fresh/prepared action may promptlessly load its original thread.
    # Submitted/replayed actions are reconciliation-only, never a load.
    store.authorize_native_action(handle, "compact")
    left = None if deadline is None else deadline - time.monotonic()
    if left is not None and left <= 0:
        raise SessionUnavailable("Codex compaction did not start before timeout; retry when ready",
                                 short=owner.short)
    ensure_ready(
        handle, expected_generation=store.generation,
        timeout=90 if left is None else min(90, left),
    )
    store.revalidate()
    intent = store.begin_native_intent(
        handle, family="codex-compact", action="compact",
        operation_id=operation_id, intent_sha256=digest,
    )
    if intent.replayed:
        row = compact_observation(handle, operation_id)
        return _wait_for_compact(row, handle, operation_id, deadline, wait)

    try:
        answer = store.dispatch_native_intent_once(
            intent,
            bounded_submit=lambda budget: daemon_call({
                "op": "compact", "short": owner.worker_id,
                "operationId": operation_id, "threadId": thread_id,
            }, timeout=budget, short=owner.short),
            dispatch_budget_s=2.,
        )
    except NativeEffectIndeterminate:
        # IPC may have landed despite caller-side loss of its positive ACK.
        answer = None

    if (isinstance(answer, dict) and answer.get("transport") == "served"
            and (answer.get("ok") is True or
                 (answer.get("noEffect") is True and answer.get("definitive") is True))):
        # Ambiguous served errors carry no positive native admission proof.
        # Read the exact original markers instead of treating them as an ACK.
        identity = {
            "workerId": owner.worker_id, "engine": "codex",
            "generation": store.generation, "nativeSource": source,
            "threadId": thread_id, "operationId": operation_id,
        }
        if (any(answer.get(key) != value for key, value in identity.items())
                or type(answer.get("startedAtMs")) is not int
                or answer.get("nativeEvidenceSha256") != _digest({
                    **identity, "startedAtMs": answer.get("startedAtMs"),
                    "admission": "thread/compact/start"
                })):
            raise NativeEffectIndeterminate("Codex compaction response does not match this worker; inspect status")
        if answer.get("ok") is True and answer.get("nativeStatus") == "accepted":
            store.settle_native_intent(
                intent, state="accepted",
                native_evidence_sha256=answer["nativeEvidenceSha256"],
            )
        elif (answer.get("ok") is False
              and answer.get("noEffect") is True and answer.get("definitive") is True):
            record = store.record(handle)
            if _compact_marker(record, operation_id)[0] is not None:
                raise NativeEffectIndeterminate("Codex compaction records disagree; inspect operation status")
            store.settle_native_intent(
                intent, state="rejected",
                native_evidence_sha256=answer["nativeEvidenceSha256"],
            )

    row = compact_observation(handle, operation_id)
    return _wait_for_compact(row, handle, operation_id, deadline, wait)


def _wait_for_compact(
    row: dict, handle: str, operation_id: str, deadline: float | None,
    wait: bool,
) -> dict:
    if not wait:
        # The exact-ID observation is already truthful, including a settled replay.
        return row
    while row["status"] in ("submitted", "prepared") and (
        deadline is None or time.monotonic() < deadline
    ):
        time.sleep(.1 if deadline is None else min(.1, max(0, deadline-time.monotonic())))
        row = compact_observation(handle, operation_id)
    return row






def telemetry_worker(
    target: str,
    *,
    after_cursor: str | None = None,
    limit: int = 200,
    timeout: float = 120,
) -> dict[str, Any]:
    return read_telemetry(
        target,
        after_cursor=after_cursor,
        limit=limit,
        timeout=timeout,
    )








def _iso_reset(value: Any) -> str | None:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return None
    return datetime.fromtimestamp(value, timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def account_usage_read(*, timeout: float = 30) -> dict[str, Any]:
    if timeout <= 0:
        raise InvalidRequest("usage timeout must be greater than zero")
    raw = runtime.read_account_usage(tokens=False, timeout=timeout)
    by_id = raw.get("rateLimitsByLimitId")
    if isinstance(by_id, dict) and by_id:
        buckets = sorted(
            by_id.items(), key=lambda item: str(item[0]).casefold()
        )
    else:
        buckets = [("codex", raw.get("rateLimits"))]

    windows: list[dict[str, Any]] = []
    for limit_id, bucket in buckets:
        if not isinstance(bucket, dict):
            continue
        for kind in ("primary", "secondary"):
            window = bucket.get(kind)
            if not isinstance(window, dict):
                continue
            used = window.get("usedPercent")
            used_percent = (
                float(used)
                if isinstance(used, (int, float)) and not isinstance(used, bool)
                else None
            )
            minutes = window.get("windowDurationMins")
            duration_seconds = (
                int(minutes) * 60
                if isinstance(minutes, int)
                and not isinstance(minutes, bool)
                and minutes > 0
                else None
            )
            windows.append({
                "provider": "codex",
                "window_id": f"{limit_id or 'codex'}:{kind}",
                "limit_id": str(limit_id or "codex"),
                "kind": kind,
                "window_duration_seconds": duration_seconds,
                "used_percent": used_percent,
                "remaining_percent": (
                    max(0.0, 100.0 - used_percent)
                    if used_percent is not None else None
                ),
                "resets_at": _iso_reset(window.get("resetsAt")),
            })
    return {
        "provider": "codex",
        "observed_at_ms": int(time.time() * 1000),
        "windows": windows,
    }


__all__ = [
    "account_usage_read",
    "compact_worker",
    "telemetry_worker",
]
