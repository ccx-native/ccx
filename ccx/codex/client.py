"""Python client for CCX-owned Codex workers.

Operations require an active owner store and exact worker UUIDs. Native
thread IDs are provider bindings, not worker targets. Construction verifies
the store without launching a provider process.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

from ccx.codex.ops import configure_worker, telemetry_worker
from ccx.codex.paths import ensure_dirs
from ccx.codex.runtime import (
    record,
    remaining,
    read_account_usage,
    require_workspace,
    rpc,
    validate_model,
    wait as wait_for_stable,
)
from ccx.codex.settle import (
    DEFAULT_WAKE_GRACE_S,
    wait_for_settlement,
)
from ccx.codex.types import (
    CodexApprovalResult,
    CodexCompactResult,
    CodexContext,
    CodexControlResult,
    CodexReport,
    CodexReconcileResult,
    CodexResumeResult,
    CodexSessionInfo,
    CodexTelemetryPage,
    CodexTurnResult,
    CodexUsage,
    CodexWaitResult,
)
from ccx.constants import (
    COMPACT_WAIT_DEFAULT_S,
    DEFAULT_IDLE_TIMEOUT_S,
)
from ccx.errors import (
    DeliveryRejected,
    InvalidRequest,
    SessionUnavailable,
    SessionWaiting,
    SendIndeterminate,
    TurnIncomplete,
    worker_not_found,
)

_SANDBOXES = {"read-only", "workspace-write", "danger-full-access"}
_APPROVAL_POLICIES = {"untrusted", "on-request", "never"}


class CodexClient:
    """Codex operations over the same owner store and receipts as the CLI."""

    def __init__(self) -> None:
        """Require the active owner store before any operation can write paths."""
        from ccx.constants import CCX_ROOT
        from ccx.store.root import GenerationError, open_or_initialize
        try:
            self._uuid_store, _created = open_or_initialize(CCX_ROOT)
        except (GenerationError, OSError) as exc:
            raise SessionUnavailable(
                f"CCX worker records are unavailable; check CCX_ROOT: {exc}"
            ) from exc
    def _owned_codex(self, target: str) -> tuple[str, dict]:
        """Positive global full UUID owner, never native thread/name/short."""
        from ccx.store.identity import IdentityError,UnknownWorker
        from ccx.store.root import GenerationError
        from ccx.errors import SessionNotFound
        try:
            self._uuid_store.revalidate()
            owner=self._uuid_store.resolve(target)
            if owner.engine!="codex":
                raise InvalidRequest("CodexClient requires Codex full UUID")
            return owner.worker,self._uuid_store.record(owner.worker)
        except UnknownWorker as exc:
            raise worker_not_found(target) from exc
        except IdentityError as exc:
            raise InvalidRequest(f"Invalid Codex worker handle: {exc}") from exc
        except GenerationError as exc:
            raise SessionUnavailable(f"CCX records changed; reopen the worker: {exc}") from exc


    def create(
        self, prompt: str | None = None, *, cwd: str | None = None,
        name: str | None = None, worker_id: str | None = None,
        model: str | None = None, effort: str | None = None,
        idle_timeout: int = DEFAULT_IDLE_TIMEOUT_S,
        sandbox: str = "danger-full-access", approval_policy: str = "never",
        wait: bool = False, timeout: float | None = None,
        delivery_id: str | None = None,
        start_timeout: float | None = None,
        caps: dict | None = None,
    ) -> CodexSessionInfo | CodexTurnResult:
        """Create one verified original Codex thread, never a speculative resume."""
        from ccx.ops.creation import create
        result = create(
            engine="codex",worker_id=worker_id,cwd=cwd or os.getcwd(),
            prompt=prompt,delivery_id=delivery_id,wait=wait,caps=caps,
            name=name,model=model,effort=effort,
            idle_timeout=idle_timeout,sandbox=sandbox,
            approval_policy=approval_policy,created_via="python-api",
            timeout=timeout,start_timeout=start_timeout,
            expected_generation=self._uuid_store.generation,
        )
        if wait:
            report = result["report"]
            turn_id = report["turn_id"]
            return CodexTurnResult.from_record(
                result["record"], turn_id=turn_id,
                report={"turnId": turn_id, "phase": "completed",
                        "text": report["text"],
                        "completedAt": report.get("settled_at_ms")},
                last_turn={"turnId": turn_id, "status": "completed"},
                client_user_message_id=result["delivery"].get("client_user_message_id"),
                delivery_id=result["delivery_id"],
            )
        # This result was verified by the shared application operation.
        # Avoid a second fallible store read after first-turn submission.
        if prompt is None:
            self._uuid_store.revalidate()
        return CodexSessionInfo.from_record(result["record"])

    def fork(
        self, target: str, prompt: str | None = None, *,
        name: str | None = None, worker_id: str | None = None,
        model: str | None = None, effort: str | None = None,
        wait: bool = False, timeout: float | None = None,
        delivery_id: str | None = None,
        caps: dict | None = None,
    ) -> CodexSessionInfo | CodexTurnResult:
        """Fork a Codex worker into a new worker with its history.

        Native ``thread/fork`` through the source's last settled turn; the
        fork keeps the source's cwd, sandbox, approval policy, model and
        effort unless overridden. Returns like :meth:`create`. Retrying with
        the same ``worker_id`` reuses the original fork point.
        """
        from math import isfinite
        from ccx.ops.forking import fork
        from ccx.store.root import GenerationError
        from ccx.store.identity import IdentityError, UnknownWorker, parse_managed_target
        from ccx.errors import SessionNotFound

        if timeout is not None and (
            type(timeout) not in (int, float) or not isfinite(timeout) or timeout <= 0
        ):
            raise InvalidRequest("fork timeout must be positive finite seconds or None")
        deadline = time.monotonic() + timeout if timeout is not None else None
        try:
            source = parse_managed_target(target).qualified("codex")
            result = fork(
                source, worker_id=worker_id, name=name, model=model, effort=effort,
                created_via="python-api", expected_generation=self._uuid_store.generation,
                deadline=deadline, prompt=prompt, wait=wait, delivery_id=delivery_id,
                caps=caps,
            )
            if wait:
                report = result["report"]
                turn_id = report["turn_id"]
                return CodexTurnResult.from_record(
                    result["record"], turn_id=turn_id,
                    report={"turnId": turn_id, "phase": "completed",
                            "text": report["text"],
                            "completedAt": report.get("settled_at_ms")},
                    last_turn={"turnId": turn_id, "status": "completed"},
                    client_user_message_id=result["delivery"].get("client_user_message_id"),
                    delivery_id=result["delivery_id"],
                )
            if prompt is None:
                self._uuid_store.revalidate()
            return CodexSessionInfo.from_record(result["record"])
        except UnknownWorker as exc:
            raise worker_not_found(target) from exc
        except IdentityError as exc:
            raise InvalidRequest(f"fork: Invalid Codex worker handle: {exc}") from exc
        except GenerationError as exc:
            raise SessionUnavailable(f"fork: Codex ACTIVE generation unavailable: {exc}") from exc

    def resume(
        self,
        target: str,
        *,
        timeout: float | None = 90,
    ) -> CodexResumeResult:
        """Re-subscribe only the original full-UUID native Codex thread.

        No model turn is started by explicit resume. A confirmed idle native
        subscription, not read-only thread history, is the success condition.
        """
        from ccx.ops.send import ensure_ready
        handle, _record = self._owned_codex(target)
        ready = ensure_ready(
            handle, timeout=timeout,
            expected_generation=self._uuid_store.generation,
        )
        return CodexResumeResult.from_record(
            self._uuid_store.record(handle), resumed=ready["resumed"],
        )

    def set_name(self, target: str, name: str | None) -> CodexSessionInfo:
        """Rename an exact Codex full UUID with one fresh semantic record CAS."""
        from ccx.store.identity import EngineMismatch, IdentityError, UnknownWorker
        from ccx.store.root import GenerationError
        try:
            owner = self._uuid_store.resolve(target)
            if owner.engine != "codex":
                raise EngineMismatch("CodexClient requires a Codex full-UUID owner")
            rec = self._uuid_store.rename_worker(
                owner.worker, engine="codex", name=name,
            )
            return CodexSessionInfo.from_record(rec)
        except UnknownWorker as exc:
            from ccx.errors import SessionNotFound
            raise worker_not_found(target) from exc
        except IdentityError as exc:
            raise InvalidRequest(f"invalid Codex worker full UUID: {exc}") from exc
        except GenerationError as exc:
            raise SessionUnavailable(f"Worker records changed; retry with the saved handle: {exc}") from exc

    def set_model(
        self,
        target: str,
        model: str | None,
        *,
        timeout: float = 5,
    ) -> CodexSessionInfo:
        """Set the model used by future turns without starting a turn."""
        worker, _record = self._owned_codex(target)
        value = configure_worker(
            worker, model=model, timeout=timeout,
            expected_generation=self._uuid_store.generation,
        )
        self._uuid_store.revalidate()
        return CodexSessionInfo.from_record(value)

    def set_effort(
        self,
        target: str,
        effort: str | None,
        *,
        timeout: float = 5,
    ) -> CodexSessionInfo:
        """Set future-turn reasoning effort without starting a turn."""
        worker, _record = self._owned_codex(target)
        value = configure_worker(
            worker, effort=effort, timeout=timeout,
            expected_generation=self._uuid_store.generation,
        )
        self._uuid_store.revalidate()
        return CodexSessionInfo.from_record(value)

    def send(
        self, target: str, text: str, *, delivery_id: str | None = None,
        wait: bool = True, timeout: float | None = None, caps: dict | None = None,
    ) -> CodexTurnResult:
        """One original native Codex app-server turn, full-UUID only.

        The original delivery_id and clientUserMessageId are immutable.
        Timeout or a lost app-server reply retains the exact native effect
        fence; never guess a missing turn/start was not submitted.
        """
        from ccx.ops.send import send_turn as _send
        from ccx.ops.wait import reconcile_turn
        from ccx.ops.report import report_worker
        from ccx.store.root import NativeEffectIndeterminate, GenerationError
        from ccx.codex.delivery import DeliveryReceiptConflict
        from ccx.errors import SendIndeterminate,OperationConflict,DeliveryRejected
        from uuid import uuid4
        if timeout is not None and (not isinstance(timeout,(int,float))
                                    or timeout<=0):
            raise InvalidRequest("Codex send timeout must be positive or None")
        worker,record=self._owned_codex(target)
        token=delivery_id if delivery_id is not None else str(uuid4())
        deadline=None if timeout is None else time.monotonic()+timeout
        try:
            accepted=_send(worker,text,delivery_id=token,mode="new_turn_only",cap=caps)
        except DeliveryReceiptConflict as exc:
            raise OperationConflict(
                f"Codex original delivery {token!r} conflicts: {exc}",
                short=worker[6:14],worker_id=worker.split(":",1)[1],
            ) from exc
        except NativeEffectIndeterminate as exc:
            raise SendIndeterminate(
                "native Codex turn effect unknown; original delivery must "
                f"be reconciled without replay: {exc}",
                short=worker.split(":",1)[1][:8],
                delivery_id=token,delivery_status="submitted-unknown",
            ) from exc
        except GenerationError as exc:
            raise SessionUnavailable(
                f"full-UUID Codex native send unavailable: {exc}"
            ) from exc
        result=accepted
        if wait:
            while True:
                self._uuid_store.revalidate()
                result=reconcile_turn(
                    worker, delivery_id=token,
                    timeout=(0.5 if deadline is None else
                             max(0.001, min(0.5, deadline-time.monotonic()))),
                )
                if result["status"] in ("settled","rejected"):
                    break
                from ccx.codex.settle import send_settle_failure
                failure = send_settle_failure(worker, result)
                if failure is not None:
                    detail = failure.get("error") or failure["status"]
                    if failure["status"] == "budget-stopped":
                        from ccx.errors import BudgetStopped
                        raise BudgetStopped(
                            f"budget stop: {detail}", short=worker.split(":", 1)[1][:8],
                            reason=str(detail), phase="turn",
                        )
                    if failure["status"] == "waiting":
                        preview = failure.get("pending_input")
                        raise SessionWaiting(
                            str(detail), short=worker.split(":", 1)[1][:8],
                            waiting_for=({**preview, "type": preview.get("kind"),
                                          "id": preview.get("request_id")}
                                         if preview is not None else None),
                        )
                    delivery = result.get("delivery") or {}
                    raise TurnIncomplete(
                        str(detail), short=worker.split(":", 1)[1][:8],
                        state=failure["status"], turn_id=delivery.get("turn_id"),
                        turn_status=(failure["status"]
                                     if failure["status"] in {"failed", "interrupted", "cancelled"}
                                     else result.get("native_status")),
                        turn_error=failure.get("error"),
                    )
                if deadline is not None and time.monotonic()>=deadline:
                    raise SendIndeterminate(
                        "Codex turn not positively completed within caller "
                        f"deadline; reconcile original delivery {token!r}",
                        short=worker.split(":",1)[1][:8],
                        delivery_id=token,delivery_status=result["status"],
                    )
                time.sleep(.1)
        delivery=result.get("delivery") or {}
        if result["status"]=="rejected":
            raise DeliveryRejected(
                delivery.get("rejection_message") or "native Codex refused",
                short=worker.split(":",1)[1][:8],
                refusal=delivery.get("rejection_code"),
                delivery_id=token,delivery_status="rejected",
            )
        turn_id=delivery.get("turn_id")
        client_id=delivery.get("client_user_message_id")
        report=None
        final_turn=None
        already_completed=(bool(accepted.get("settled")) if not wait
                           else result["status"]=="settled")
        if already_completed:
            if wait:
                native_report=report_worker(
                    worker,turn_ref=delivery.get("turn_ref"))
                positive_text=native_report["text"]
            else:
                original=delivery.get("settlement")
                if (not isinstance(original,dict)
                        or original.get("status")!="completed"
                        or original.get("turnId")!=turn_id
                        or not isinstance(original.get("text"),str)):
                    raise SessionUnavailable(
                        "original Codex accepted receipt lacks positive "
                        "completed-turn evidence"
                    )
                positive_text=original["text"]
            report={"turnId":turn_id,"phase":"completed",
                    "text":positive_text}
            final_turn={"turnId":turn_id,"status":"completed"}
        rec=self._uuid_store.record(worker)
        return CodexTurnResult.from_record(
            rec,turn_id=turn_id,report=report,last_turn=final_turn,
            client_user_message_id=client_id,delivery_id=token,
        )

    def reconcile(
        self, target: str, *, delivery_id: str | None = None,
        turn_id: str | None = None, timeout: float = 30,
    ) -> CodexReconcileResult:
        """Observe original native thread/turn, no second native mutation."""
        from ccx.ops.wait import reconcile_turn
        from ccx.store.root import GenerationError
        if timeout<=0:
            raise InvalidRequest("Codex reconciliation timeout must be positive")
        worker,record=self._owned_codex(target)
        try:
            result=reconcile_turn(
                worker,delivery_id=delivery_id,turn_id=turn_id,timeout=timeout
            )
        except GenerationError as exc:
            raise SessionUnavailable(
                f"Codex delivery record unavailable; inspect the worker: {exc}"
            ) from exc
        return CodexReconcileResult.from_operation(
            self._uuid_store.record(worker),
            result.get("delivery"),
            result["status"],
        )

    def telemetry(
        self,
        target: str,
        *,
        after_cursor: str | None = None,
        limit: int = 200,
        timeout: float = 120,
    ) -> CodexTelemetryPage:
        """Read original source-bound Codex telemetry without starting a turn."""
        from ccx.store.root import GenerationError

        worker, _record = self._owned_codex(target)
        try:
            result = telemetry_worker(
                worker, after_cursor=after_cursor, limit=limit, timeout=timeout,
            )
        except GenerationError as exc:
            raise SessionUnavailable(
                f"Cannot read Codex activity; check worker status: {exc}"
            ) from exc
        return CodexTelemetryPage.from_operation(result)

    def steer(
        self, target: str, text: str, *, delivery_id: str | None = None,
        caps: dict | None = None,
    ) -> CodexControlResult:
        """Place one input in the exact active Codex turn, never start or wait."""
        from uuid import uuid4
        from ccx.codex import delivery as codex_delivery
        from ccx.codex.runtime import settled_daemon_status
        from ccx.ops.send import send_turn
        from ccx.store.root import GenerationError
        from ccx.store.worker_state import RUNTIME_ABSENT

        handle, record = self._owned_codex(target)
        if not isinstance(text, str) or not text.strip():
            raise InvalidRequest("steer: prompt text is required")
        token = delivery_id if delivery_id is not None else str(uuid4())
        if not codex_delivery.valid_delivery_id(token):
            raise InvalidRequest("steer: invalid delivery_id")
        try:
            original = codex_delivery.read_receipt(handle, token)
            if original is None:
                # Never turn a replayed/uncertain original token into a fresh
                # absent-daemon refusal: the shared sender owns every replay.
                self._uuid_store.authorize_native_action(handle, "send")
                status = settled_daemon_status(time.monotonic() + 1.0)
                if status.get("transport") == RUNTIME_ABSENT:
                    raise DeliveryRejected(
                        "Codex worker has no running turn (daemon absent); "
                        "no delivery was recorded, so this token is unclaimed "
                        "and may be used when the worker is running",
                        short=handle.split(":", 1)[1][:8],
                        state=(record.get("lifecycle") or {}).get("state"),
                        refusal="worker-state", delivery_id=token,
                    )
        except (codex_delivery.DeliveryReceiptError, GenerationError, OSError) as exc:
            raise SessionUnavailable(f"Codex message record is unavailable; inspect worker status: {exc}") from exc

        try:
            accepted = send_turn(handle, text, delivery_id=token,
                                 mode="steer_only", cap=caps)
        except (codex_delivery.DeliveryReceiptError, GenerationError, OSError) as exc:
            raise SessionUnavailable(f"Cannot add this message to the running turn; check status: {exc}") from exc
        delivery = accepted["delivery"]
        if accepted["status"] == "rejected":
            raise DeliveryRejected(
                delivery.get("rejection_message") or "Codex steer refused",
                short=handle.split(":", 1)[1][:8],
                refusal=delivery.get("rejection_code"),
                delivery_id=token, delivery_status="rejected",
            )
        if (accepted["status"] != "accepted"
                or delivery.get("route") != "steer"
                or delivery.get("placement") != "current_turn"):
            raise SendIndeterminate(
                "Steering was not confirmed; "
                "reconcile the delivery instead of resending",
                short=handle.split(":", 1)[1][:8],
                delivery_id=token, delivery_status=accepted["status"],
            )
        return CodexControlResult.from_record(
            record, action="steer", turn_id=delivery["turn_id"],
            client_user_message_id=delivery["client_user_message_id"],
            delivery_id=token,
        )

    def interrupt(
        self,
        target: str,
        *,
        expected_turn_id: str | None = None,
    ) -> CodexControlResult:
        """Request an exact full-UUID native interrupt; return ACK, not completion."""
        from ccx.ops.cancel import stop_worker
        handle, value = self._owned_codex(target)
        expected_ref = None
        if expected_turn_id is not None:
            if not isinstance(expected_turn_id, str) or not expected_turn_id:
                raise InvalidRequest("expected_turn_id must be a nonempty native turn ID")
            expected_ref = "ccx-turn:codex:v1:" + expected_turn_id
        result = stop_worker(
            handle, expected_turn_ref=expected_ref,
            expected_generation=self._uuid_store.generation,
        )
        # Existing CodexControlResult is an acceptance-only type. An unknown
        # native reply is raised by stop_worker, never accepted=True here.
        return CodexControlResult.from_record(
            value, action="interrupt", turn_id=result["turn_ref"].rsplit(":", 1)[-1],
        )

    def wait(
        self,
        target: str,
        *,
        timeout: float | None = None,
        settle: bool = False,
        wake_grace: float = DEFAULT_WAKE_GRACE_S,
    ) -> CodexWaitResult:
        """Wait for original Codex stability or recursive notification quiescence."""
        from ccx.ops.wait import wait_worker
        handle, record = self._owned_codex(target)
        result = (wait_for_settlement(
            handle, timeout=timeout, wake_grace_s=wake_grace,
            expected_generation=self._uuid_store.generation)
            if settle else wait_worker(
                handle, timeout=timeout,
                expected_generation=self._uuid_store.generation))
        return CodexWaitResult.from_result(record, result)

    def report(
        self, target: str, *, timeout: float | None = 90,
    ) -> CodexReport:
        """Read Codex's latest turn reply or last confirmed reply while busy.

        An empty final answer stays empty; note explains newer uncertain
        messages, running work, failed workers or native history failures.
        """
        from ccx.ops.report import report_worker
        from ccx.store.root import GenerationError
        if timeout is not None and timeout<=0:
            raise InvalidRequest("Codex report timeout must be positive")
        worker,rec=self._owned_codex(target)
        try:
            report=report_worker(
                worker,timeout=90 if timeout is None else timeout,
                max_chars=None,
            )
        except GenerationError as exc:
            raise SessionUnavailable(
                f"Cannot read Codex reply; inspect worker status: {exc}"
            ) from exc
        return CodexReport.from_record(self._uuid_store.record(worker),{
            "turnId":report.get("turn_id"),
            "itemId":None,"phase":report.get("phase"),
            "text":report["text"],
            "completedAt":report.get("settled_at_ms"),
            "note":report.get("note"),
        })

    def approve(
        self,
        target: str,
        *,
        request_id: str,
        timeout: float | None = None,
    ) -> CodexApprovalResult:
        """Accept one pending command or ordinary file-change request.

        ``timeout`` bounds only the decision RPC itself (the host resolves the
        approval synchronously); pass it when the decision is one step of a
        larger budgeted operation. ``None`` keeps the default host timeout.
        """
        return self._decision(target, "approve", request_id, timeout)

    def reject(
        self,
        target: str,
        *,
        request_id: str,
        timeout: float | None = None,
    ) -> CodexApprovalResult:
        """Decline one pending command or ordinary file-change request.

        Codex approvals carry no denial rationale, so there is no ``reason``
        here: the app-server accepts a decision, not a message.
        """
        return self._decision(target, "reject", request_id, timeout)

    def _decision(
        self,
        target: str,
        operation: str,
        request_id: str,
        timeout: float | None = None,
        *, response_id: str | None = None,
        expected_turn_id: str | None = None,
    ) -> CodexApprovalResult:
        # Native requestIds can repeat after an app-server restart. The caller
        # must supply the unique CCX gateId shown by recorded status/peek.
        if not isinstance(request_id, str) or not request_id or len(request_id) > 128:
            raise InvalidRequest("Codex approval requires an exact bounded CCX gateId")
        if timeout is not None and (type(timeout) not in (int, float) or timeout <= 0):
            raise InvalidRequest("Codex approval timeout must be positive")
        _owner, value = self._owned_codex(target)
        worker = value["workerId"]  # Canonical bare global UUID, not qualified handle.
        message = {"op": operation, "gateId": request_id}
        if response_id is not None:
            message["responseId"] = response_id
        reply = rpc(worker, message, timeout=10 if timeout is None else timeout)
        # The request could have reached codexd even if its ACK was lost.
        # Codex app-server server-request responses have no provider ACK.
        transport = reply.get("transport")
        if reply.get("ok"):
            native = value.get("native") or {}
            expected_decision = "accept" if operation == "approve" else "decline"
            if (reply.get("gateId") != request_id
                    or reply.get("workerId") != worker
                    or reply.get("threadId") != native.get("threadId")
                    or not isinstance(reply.get("turnId"), str)
                    or not reply["turnId"]
                    or (expected_turn_id is not None
                        and reply["turnId"] != expected_turn_id)
                    or reply.get("decision") != expected_decision):
                raise SendIndeterminate(
                    f"Codex {operation} ACK mismatched original worker/gate; "
                    f"worker={worker}, gateId={request_id}", short=worker)
            return CodexApprovalResult.from_record(
                value, request_id=request_id, decision=expected_decision)
        if transport == "absent" or reply.get("loaded") is False:
            raise SessionUnavailable(f"Codex {operation}: worker is stopped; resume it before this action",
                                     short=worker)
        if transport == "inconclusive" or not reply.get("definitive"):
            raise SendIndeterminate(
                f"Codex {operation}: approval response indeterminate; "
                f"worker={worker}, gateId={request_id}", short=worker)
        raise DeliveryRejected(
            str(reply.get("error") or f"Codex {operation} refused original gate"),
            short=worker, reason=reply.get("error"), state=reply.get("state"))

    def compact(
        self,
        target: str,
        *,
        timeout: float | None = COMPACT_WAIT_DEFAULT_S,
    ) -> CodexCompactResult:
        """Wait for one original, source-proven native Codex compaction."""
        from uuid import uuid4
        from ccx.codex.ops import compact_worker
        from ccx.errors import CompactFailed, CompactIndeterminate

        from ccx.store.root import GenerationError, NativeEffectIndeterminate

        handle, _record = self._owned_codex(target)
        operation_id = str(uuid4())  # Internal only; public Codex has no ID option.
        try:
            result = compact_worker(
                handle, operation_id=operation_id, wait=True, timeout=timeout,
            )
        except NativeEffectIndeterminate as exc:
            raise CompactIndeterminate(
                f"Codex compaction outcome is unknown; check status before retrying: {exc}",
                short=handle.split(":", 1)[1][:8], operation_id=operation_id,
                operation_status="unknown",
            ) from exc
        except GenerationError as exc:
            raise SessionUnavailable(
                f"full-UUID Codex native compact unavailable: {exc}"
            ) from exc
        current = self._uuid_store.record(handle)
        native_state = (current.get("lifecycle") or {}).get("state")
        status = result["status"]
        if status == "failed":
            raise CompactFailed(
                result.get("reason") or "Codex native compact turn failed",
                short=current["workerId"][:8], state=native_state,
                operation_id=operation_id,
            )
        if status == "rejected":
            raise DeliveryRejected(
                result.get("reason") or "Codex compact refused before native admission",
                short=current["workerId"][:8], state=native_state,
                refusal="native-rejected", operation_id=operation_id,
            )
        if status != "succeeded" or result.get("compacted") is not True:
            raise CompactIndeterminate(
                f"Codex compaction result {status!r} is unconfirmed; inspect its operation",
                short=current["workerId"][:8], state=native_state,
                operation_id=operation_id, operation_status=status,
            )
        return CodexCompactResult.from_record(
            current, compacted=True,
            context_before=result.get("contextBefore"),
            context_after=result.get("contextAfter"),
            operation_id=operation_id, operation_status=status,
            compact_turn_id=result.get("compactTurnId"),
            reason=result.get("reason"),
        )

    def context(self, target: str) -> CodexContext:
        """Return the latest cached carried-context measurement."""
        _worker, rec = self._owned_codex(target)
        return CodexContext.from_record(rec)

    def usage(
        self,
        *,
        tokens: bool = False,
        timeout: float = 30,
    ) -> CodexUsage:
        """Return live Codex account quota utilization or token activity."""
        if timeout <= 0:
            raise InvalidRequest("timeout must be greater than zero")
        data = read_account_usage(tokens=tokens, timeout=timeout)
        return CodexUsage(
            raw=data,
            kind="token_activity" if tokens else "rate_limits",
        )


__all__ = ["CodexClient"]
