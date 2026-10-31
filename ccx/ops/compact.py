from __future__ import annotations
import time
from typing import Any
from ccx.client import _compact_failed
from ccx.constants import COMPACT_WAIT_DEFAULT_S
from ccx.errors import (
    CompactIndeterminate,
    DeliveryRejected,
    InvalidRequest,
    SessionNotFound,
    SessionUnavailable,
)
from ccx.ops.common import split_target
from ccx.ops.send import ensure_ready


def _original_claude_compact_view(store, owner, rec: dict, operation_id: str,
                                  *, publish: bool = True) -> dict | None:
    """One source-verified projection for both mutating and pure observations."""
    from ccx.ops.compact_ops import observe_original_operation

    operation = observe_original_operation(
        store, owner, rec, operation_id, publish=publish,
    )
    if operation is None:
        return None
    result = operation.get("result") or {}
    status = operation.get("status") or "unknown"
    latest = store.record(owner.worker)
    return {
        "short": owner.short, "worker_id": owner.worker_id,
        "state": (latest.get("lifecycle") or {}).get("state"),
        "compacted": status == "succeeded",
        "message": result.get("message"),
        "pre_tokens": result.get("pre_tokens"),
        "post_tokens": result.get("post_tokens"),
        "duration_ms": result.get("duration_ms"),
        "operation_id": operation_id, "operation_status": status,
        "evidence_uuid": operation.get("evidence_uuid"),
        "reason": operation.get("reason"),
    }


def compact_status(target: str, *, operation_id: str, timeout: float = 0.75,
                   publish: bool = True) -> dict[str, Any]:
    """Read original Claude compact; pure if publish=False, never native."""
    from ccx import constants
    from ccx.ops.compact_ops import get_operation, compact_intent_digest, CompactJournalError
    from ccx.store.root import require_ccx_active
    from ccx.store.root import GenerationError

    handle, engine, worker_id = split_target(target)
    if engine != "claude":
        raise SessionUnavailable("Codex compaction is unavailable for this worker")
    if (not isinstance(operation_id, str) or not operation_id
            or len(operation_id) > 128 or "\x00" in operation_id):
        raise InvalidRequest("compact_status needs a nonempty operation_id of at most 128 characters")
    store = require_ccx_active(constants.CCX_ROOT)
    owner = store.resolve(handle)
    rec = store.record(handle)
    native = rec.get("native") or {}
    session_id = native.get("sessionId") if isinstance(native, dict) else None
    try:
        prior = get_operation(owner.worker, operation_id)
        receipt = store.read_receipt(owner.worker, "compact", operation_id)
        if prior is None:
            if receipt is None:
                raise SessionNotFound(f"No compaction found for operation {operation_id!r}", short=owner.short)
            raise CompactIndeterminate(
                "Compaction result cannot be verified; inspect its operation",
                short=owner.short, operation_id=operation_id, operation_status="unknown",
            )
        digest = compact_intent_digest(
            owner.worker_id, generation=store.generation,
            native_session_id=session_id, operation_id=operation_id,
        )
        if (not receipt or receipt.get("state") not in (
                "submitted-unknown", "accepted", "rejected")
                or receipt.get("intentSha256") != digest
                or receipt.get("workerId") != owner.worker_id
                or receipt.get("engine") != "claude"
                or receipt.get("generation") != store.generation
                or receipt.get("family") != "compact"
                or receipt.get("action") != "compact"):
            raise CompactIndeterminate(
                "Compaction confirmation is missing; inspect its operation",
                short=owner.short, operation_id=operation_id, operation_status="unknown",
            )
        viewed = _original_claude_compact_view(
            store, owner, rec, operation_id, publish=publish,
        )
        assert viewed is not None
        return viewed
    except (GenerationError, CompactJournalError, OSError) as exc:
        raise CompactIndeterminate(
            f"Cannot read compaction status; retry: {exc}",
            short=owner.short, operation_id=operation_id, operation_status="unknown",
        ) from exc


def compact_worker(
    target: str,
    *,
    operation_id: str,
    timeout: float | None = 90,
    wait: bool = True,
    execution_timeout: float | None = None,
) -> dict[str, Any]:
    """One receipt-fenced native Claude /compact, then same-ID journal observation."""
    from ccx import constants
    from ccx.ops.compact_ops import compact_intent_digest, get_operation, CompactJournalError
    from ccx.constants import COMPACT_POLL_S
    import math
    from ccx.store.root import GenerationError, NativeEffectIndeterminate, require_ccx_active
    from ccx.store.store import _digest
    from ccx.rpc.rpc import _rpc

    handle, engine, worker_id = split_target(target)
    if engine != "claude":
        raise SessionUnavailable("Codex compaction is unavailable for this worker")
    if (not isinstance(operation_id, str) or not operation_id
            or len(operation_id) > 128 or "\x00" in operation_id):
        raise InvalidRequest("compact operation_id must be a nonempty string of at most 128 characters")
    if timeout is not None and (type(timeout) not in (float, int)
                                or not math.isfinite(timeout) or timeout < 0):
        raise InvalidRequest("compact observation timeout must be finite, nonnegative or None")
    if (execution_timeout is not None
            and (type(execution_timeout) not in (int, float)
                 or not math.isfinite(execution_timeout) or execution_timeout <= 0)):
        raise InvalidRequest("compact execution_timeout must be finite and positive or None")
    deadline = None if timeout is None else time.monotonic() + timeout
    store = require_ccx_active(constants.CCX_ROOT)
    owner = store.resolve(handle)
    rec = store.record(handle)
    native = rec.get("native") or {}
    session_id = native.get("sessionId") if isinstance(native, dict) else None
    if not isinstance(session_id, str) or not session_id:
        raise SessionUnavailable("original Claude native session ID is unavailable", short=owner.short)
    digest = compact_intent_digest(
        owner.worker_id, generation=store.generation,
        native_session_id=session_id, operation_id=operation_id,
    )

    def indeterminate(reason: str, *, status: str = "unknown") -> CompactIndeterminate:
        return CompactIndeterminate(
            reason, short=owner.short,
            operation_id=operation_id, operation_status=status,
        )

    # Original operation reconciliation precedes any host revival, claim or IPC.
    try:
        prior = get_operation(handle, operation_id)
        effect = store.read_receipt(handle, "compact", operation_id)
    except (GenerationError, CompactJournalError, OSError) as exc:
        raise indeterminate(f"cannot inspect original compact receipt/journal: {exc}") from exc
    if effect is not None and (
        effect.get("intentSha256") != digest
        or effect.get("workerId") != owner.worker_id
        or effect.get("engine") != "claude"
        or effect.get("generation") != store.generation
        or effect.get("action") != "compact"
        or effect.get("family") != "compact"
    ):
        raise indeterminate("original compact receipt identity does not match")
    if prior is not None and (effect is None or effect.get("state") == "prepared"):
        raise indeterminate("original compact journal lacks a submitted native effect receipt")
    if effect and effect.get("state") == "rejected":
        raise DeliveryRejected(
            "native compact was rejected; request a new operation_id to try again",
            short=owner.short, operation_id=operation_id,
            refusal="compact-rejected",
        )
    submitted = effect is not None and effect.get("state") in ("submitted-unknown", "accepted")
    if submitted and prior is None:
        raise indeterminate("submitted native compact has no original journal; never resubmit")
    if effect is not None and effect.get("state") not in (
        "prepared", "submitted-unknown", "accepted",
    ):
        raise indeterminate("unsupported original compact intent state")
    if not submitted:
        left = None if deadline is None else max(0., deadline - time.monotonic())
        if left is not None and left <= 0:
            raise indeterminate("compact observation budget expired before native admission")
        ensure_ready(
            handle,
            # Revive has a separate bounded original-native timeout; a
            # longer user observation window must remain legal.
            timeout=min(600., left) if left is not None else 180,
            expected_generation=store.generation,
        )
        rec = store.record(handle)
        store.authorize_native_action(handle, "compact")
        attempt = store.begin_native_intent(
            handle, action="compact", family="compact",
            operation_id=operation_id, intent_sha256=digest,
        )
        if attempt.replayed:
            raise indeterminate("original compact receipt already submitted; no replay")
        budget = 2. if deadline is None else min(2., max(.001, deadline - time.monotonic()))
        if deadline is not None and deadline - time.monotonic() <= 0:
            raise indeterminate("compact observation budget expired before native dispatch")
        native_budget = (
            execution_timeout if execution_timeout is not None
            else COMPACT_WAIT_DEFAULT_S
        )
        try:
            reply = store.dispatch_native_intent_once(
                attempt, dispatch_budget_s=budget,
                bounded_submit=lambda seconds: _rpc(
                    handle,
                    {"op": "compact", "operation_id": operation_id,
                     "timeout": native_budget},
                    timeout=seconds,
                ),
            )
        except NativeEffectIndeterminate:
            # A lost reply is NOT a proved rejection, and this token must
            # never be sent to the host a second time.
            reply = None
        if isinstance(reply, dict) and reply.get("ok") is True:
            journal_reply = reply.get("operation")
            if (reply.get("workerId") != owner.worker_id
                    or reply.get("engine") != "claude"
                    or reply.get("generation") != store.generation
                    or reply.get("nativeSessionId") != session_id
                    or reply.get("operationId") != operation_id
                    or not isinstance(journal_reply, dict)
                    or journal_reply.get("operation_id") != operation_id
                    or journal_reply.get("status") not in (
                        "accepted", "pasted", "submitted", "succeeded", "noop", "failed", "unknown",
                    )):
                raise indeterminate("original compact host ACK identity or journal differs")
            store.settle_native_intent(
                attempt, state="accepted",
                native_evidence_sha256=_digest({
                    "workerId": owner.worker_id, "generation": store.generation,
                    "operationId": operation_id, "journal": journal_reply,
                }),
            )
        else:
            # Only an authenticated host refusal (its own state field)
            # PLUS a fresh trusted absent journal proves nothing reached Ink.
            try:
                prior = get_operation(handle, operation_id)
            except (GenerationError, CompactJournalError, OSError) as exc:
                raise indeterminate("compact journal unreadable after native IPC") from exc
            if (isinstance(reply, dict) and reply.get("ok") is False
                    and isinstance(reply.get("state"), str)
                    and prior is None):
                store.settle_native_intent(
                    attempt, state="rejected",
                    native_evidence_sha256=_digest({
                        "workerId": owner.worker_id, "operationId": operation_id,
                        "nativeRefusal": reply.get("error"),
                        "nativeState": reply["state"],
                    }),
                )
                raise DeliveryRejected(
                    "native compact refused: " + str(reply.get("error"))
                    + "; use a new operation_id to retry",
                    short=owner.short, state=reply["state"],
                    operation_id=operation_id,
                )
            if prior is None:
                raise indeterminate("native compact ACK unavailable; submitted token fenced")
    # Journal presence is physical acceptance, *never* terminal success.
    store.revalidate()
    if not wait:
        # The compaction journal proves durable native acceptance even before the
        # provider has written a readable transcript. It is NOT a completion.
        accepted = get_operation(handle, operation_id)
        if accepted is None:
            raise indeterminate("original native compact acceptance journal missing")
        latest = store.record(handle)
        return {
            "short": owner.short, "worker_id": owner.worker_id,
            "state": (latest.get("lifecycle") or {}).get("state"),
            "operation_id": operation_id,
            "operation_status": accepted.get("status") or "unknown",
            "compacted": False, "pre_tokens": None, "post_tokens": None,
            "duration_ms": None, "message": None,
            "evidence_uuid": None, "reason": accepted.get("reason"),
        }

    def admission_released(row: dict) -> bool:
        # A native compact boundary can precede the host's IDLE transition (or
        # the Stop for a native turn queued behind /compact). The synchronous
        # operation must include that lifecycle settlement, not just the journal.
        if row.get("state") not in {
            "idle", "idle-killed", "crashed", "dead", "budget-stopped",
        }:
            return False
        current = store.record(handle)
        lc = current.get("lifecycle") or {}
        return (
            lc.get("state") in {
                "idle", "idle-killed", "crashed", "dead", "budget-stopped",
            }
            and not lc.get("activeTurnId")
            and not current.get("activeCompact")
            and not current.get("pendingSend")
            and not current.get("waitingFor")
            and not current.get("pendingApprovals")
        )

    while True:
        row = compact_status(handle, operation_id=operation_id)
        status = row.get("operation_status")
        if status in ("succeeded", "noop") and admission_released(row):
            return row
        if status == "failed":
            raise _compact_failed(
                get_operation(handle, operation_id),
                short=owner.short, state=row.get("state"),
                operation_id=operation_id,
            )
        if status == "unknown":
            raise indeterminate(
                row.get("reason") or "native compact outcome indeterminate",
                status="unknown",
            )
        if deadline is not None and time.monotonic() >= deadline:
            # Final evidence at the deadline wins over a simultaneous timeout.
            last = compact_status(handle, operation_id=operation_id)
            if (last["operation_status"] in ("succeeded", "noop")
                    and admission_released(last)):
                return last
            if last["operation_status"] == "failed":
                raise _compact_failed(
                    get_operation(handle, operation_id), short=owner.short,
                    state=last.get("state"), operation_id=operation_id,
                )
            raise indeterminate(
                "compact outcome or worker input settlement remains pending at observation timeout",
                status=last.get("operation_status") or "unknown",
            )
        time.sleep(COMPACT_POLL_S)
