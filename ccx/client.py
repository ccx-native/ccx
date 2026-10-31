"""Direct Python Claude client bound to a sealed active generation.

Managed operations use full CCX worker UUIDs and durable delivery identities."""

from __future__ import annotations
import ccx.cli.commands.read as commands_read
import ccx.cli.commands.turns as commands_turns

import os
import socket
import time
from types import SimpleNamespace
from typing import Iterator

from ccx.rpc import rpc as host_rpc
from ccx.ops import event_feed
from ccx.claude.delivery import (
    public_receipt as public_semantic_receipt,
    read_receipt as read_semantic_receipt,
    valid_delivery_id as valid_semantic_delivery_id,
)
from ccx.ops.compact_ops import (
    get_operation as get_compact_operation,
    new_operation_id as new_compact_operation_id,
    observe_operation as observe_compact_operation,
)
from .constants import (
    COMPACT_POLL_S,
    COMPACT_WAIT_DEFAULT_S,
    DEFAULT_IDLE_TIMEOUT_S,
    S,
    STEER_OBSERVE_DEFAULT_S,
    STEER_OBSERVE_POLL_S,
)
from .errors import (
    BudgetStopped,
    CompactFailed,
    CompactIndeterminate,
    DeliveryRejected,
    InvalidRequest,
    PayloadTooLarge,
    SessionNotFound,
    SessionUnavailable,
    SessionWaiting,
    SendIndeterminate,
    SteerIndeterminate,
    TurnIncomplete,
    WorkerApiError,
    WorkerRateLimited,
    api_error_from_lifecycle,
    build_worker_api_error,
    worker_not_found,
)
from ccx.store.paths import ensure_dirs, events_ndjson_path, sock_path
from ccx.ops.steer_ops import (
    PUBLIC_ACCEPTED_PENDING as STEER_PENDING,
    PUBLIC_CONSUMED as STEER_CONSUMED,
    PUBLIC_STARTED as STEER_STARTED,
    PUBLIC_UNKNOWN as STEER_UNKNOWN,
    get_delivery as get_steer_delivery,
    new_delivery_id as new_steer_delivery_id,
    observe_delivery as observe_steer_delivery,
    public_delivery as public_steer_delivery,
    valid_delivery_id as valid_steer_delivery_id,
)
from ccx.store.records import read_record
from ccx.ops.terminate import KILL_TIMEOUT_DEFAULT_S
from ccx.claude.managed_turns import latest_settled_turn, settlement_for_turn
from ccx.rpc.rpc import (
    _rpc,
)
from ccx.ops.semantics import (
    payload_guidance,
    KIND_PERMISSION,
    answer_illegality,
    answer_request_fence,
    elicitation_illegality,
    elicitation_response_problem,
    filter_events_after,
    pending_input_kind,
    wrong_verb_hint,
    question_answer_map,
    question_answer_message,
)
from ccx.claude.transcript import (
    render_transcript_json,
)
from .types import (
    ClaudeContext,
    ClaudeTask,
    CompactOperationResult,
    Event,
    FiveHourReset,
    Report,
    PendingInput,
    SemanticDeliveryResult,
    SendResult,
    SessionInfo,
    SessionStatus,
    SteerResult,
    Usage,
    WaitResult,
)
from ccx.claude.usage import fetch_usage, reset_five_hour_limit, session_reset_status, usage_summary


def _compact_failed(
    operation: dict | None, *, short: str, state: str | None,
    operation_id: str | None,
) -> CompactFailed:
    """Build the typed error for a proven compaction failure.

    The journal already carries the classification, so this only re-types it.
    ``api_error`` stays ``None`` when the conversation itself was the problem —
    a caller retrying on an API blip must not also retry a compaction that can
    never succeed.
    """
    result = (operation or {}).get("result") or {}
    payload = result.get("api_error")
    api_error = None
    if isinstance(payload, dict):
        api_error = build_worker_api_error(
            (operation or {}).get("reason") or "",
            short=short,
            kind=payload.get("error"),
            status=payload.get("status"),
            transient=bool(payload.get("transient")),
            rate_limited=bool(payload.get("rate_limited")),
            request_id=payload.get("request_id"),
            detail=payload.get("text"),
            reset_text=payload.get("reset_text"),
        )
    return CompactFailed(
        "compact failed after acceptance: "
        f"{(operation or {}).get('reason') or 'unknown reason'}",
        short=short, state=state, operation_id=operation_id,
        api_error=api_error,
    )


def _managed_completed_turns(short: str) -> int:
    # Historical private name retained; this is now the managed lifecycle ordinal.
    record = read_record(short) or {}
    return latest_settled_turn(
        short,
        fallback=int((record.get("stats") or {}).get("turns") or 0),
        record_last_settled_at_ms=record.get("lastSettledAt") or record.get("lastTurnAt"),
    )


def _original_input_ack(reply: dict, *, op: str, worker_id: str,
                        request_id: str, response_id: str | None) -> None:
    """Classify the existing verified host RPC; a lost reply is not a refusal."""
    if reply.get("ok") is True:
        if reply.get("request_id") == request_id and (
                response_id is None or reply.get("response_id") == response_id):
            return
        raise SendIndeterminate(
            f"{op}: original input ACK has wrong request identity "
            f"(request_id={request_id!r}, response_id={response_id!r})",
            short=worker_id,
        )
    error = str(reply.get("error") or "unknown host refusal")
    if error.startswith(("unreachable:", "unverified host:")):
        raise SessionUnavailable(f"{op}: original host unavailable: {error}",
                                 short=worker_id)
    if error in ("timeout", "no reply", "bad reply", "reply too large",
                 "non-object reply"):
        raise SendIndeterminate(
            f"{op}: native decision reply indeterminate ({error}); "
            f"request_id={request_id!r}, response_id={response_id!r}",
            short=worker_id,
        )
    if error.startswith(("no pending ", "pending input is not ",
                         "AskUserQuestion requires ", "MCP elicitation requires ")):
        raise SessionUnavailable(f"{op}: {error}", short=worker_id)
    raise DeliveryRejected(f"{op} rejected: {error}", short=worker_id,
                           reason=error, state=reply.get("state"))


class CcxClient:
    """In-process client for ccx-managed Claude Code sessions.

    Stateless and cheap to construct; all session state lives on disk under
    ``CCX_ROOT`` exactly as the CLI leaves it, so a ``CcxClient`` and the ``ccx``
    command interoperate freely on the same sessions.
    """

    def _uuid_session_info(self, owner, rec: dict, *,
                           inventory_row: dict | None = None) -> SessionInfo:
        """Typed managed-only public fields, never an external/native alias."""
        native = rec.get("native")
        native = native if isinstance(native, dict) else {}
        args = rec.get("createArgs")
        args = args if isinstance(args, dict) else {}
        stats = rec.get("stats")
        stats = stats if isinstance(stats, dict) else {}
        lifecycle = rec.get("lifecycle")
        lifecycle = lifecycle if isinstance(lifecycle, dict) else {}
        return SessionInfo(
            short=owner.short, worker_id=owner.worker_id,
            session_id=native.get("sessionId"), name=rec.get("name"),
            cwd=rec.get("cwd"), state=lifecycle.get("state"),
            model=args.get("model"), effort=args.get("effort"),
            last_turn_at=(inventory_row["last_turn_at"] if inventory_row is not None
                          else rec.get("lastTurnAt")),
            created_at=(inventory_row["created_at"] if inventory_row is not None
                        else rec.get("createdAt")),
            turns=stats.get("turns"),
            warning=inventory_row.get("warning") if inventory_row is not None else None,
        )

    def __init__(self) -> None:
        """One fail-closed full-UUID bootstrap; never legacy ensure_dirs."""
        from .constants import CCX_ROOT
        from ccx.store.root import GenerationError, open_or_initialize
        try:
            self._uuid_store, _created = open_or_initialize(CCX_ROOT)
        except (GenerationError, OSError) as exc:
            raise SessionUnavailable(
                f"CCX worker records are unavailable; check CCX_ROOT: {exc}"
            ) from exc

    # -- internal helpers ---------------------------------------------------

    def _resolve_record(self, identifier: str, *, live: bool = False) -> dict:
        """Exact full-UUID CCX-owned record; unmanaged Claude inaccessible."""
        if live:
            raise SessionUnavailable(
                "Only CCX-managed Claude workers are supported; use a worker handle"
            )
        return self._resolve_managed(identifier,"inspect")

    def _resolve_managed(self, identifier: str, op: str) -> dict:
        """Current global owned full UUID only, no native/name/short fallback."""
        from ccx.store.root import GenerationError
        from ccx.store.identity import IdentityError,UnknownWorker
        try:
            self._uuid_store.revalidate()
            owner=self._uuid_store.resolve(identifier)
            if owner.engine!="claude":
                raise InvalidRequest(f"{op}: Claude full-UUID owner required")
            return self._uuid_store.record(owner.worker)
        except UnknownWorker as exc:
            raise worker_not_found(identifier) from exc
        except IdentityError as exc:
            raise InvalidRequest(f"{op}: invalid Claude full UUID: {exc}") from exc
        except GenerationError as exc:
            raise SessionUnavailable(f"{op}: ACTIVE generation changed: {exc}") from exc

    # -- write paths --------------------------------------------------------

    def new(
        self, prompt: str | None = None, *, cwd: str | None = None,
        name: str | None = None, worker_id: str | None = None,
        model: str | None = None, effort: str | None = None,
        agent: str | None = None, append_system_prompt: str | None = None,
        idle_timeout: int = DEFAULT_IDLE_TIMEOUT_S,
        allowed_tools: str | None = None, disallowed_tools: str | None = None,
        add_dir: list[str] | None = None, permission_mode: str | None = None,
        skip_permissions: bool = True, caps: dict | None = None,
        wait: bool = False, timeout: float | None = None,
        start_timeout: float | None = None,
    ) -> SessionInfo | SendResult:
        """Create one verified native Claude session under its original UUID."""
        from ccx.ops.creation import create
        result = create(
            engine="claude",worker_id=worker_id,cwd=cwd or os.getcwd(),
            prompt=prompt,wait=wait,caps=caps,
            name=name,model=model,effort=effort,agent=agent,
            append_system_prompt=append_system_prompt,
            idle_timeout=idle_timeout,allowed_tools=allowed_tools,
            disallowed_tools=disallowed_tools,add_dir=add_dir,
            permission_mode=permission_mode,
            permissions="full" if skip_permissions else "restricted",
            created_via="python-api",timeout=timeout,
            start_timeout=start_timeout,
            expected_generation=self._uuid_store.generation,
        )
        if wait:
            report = result["report"]
            return SendResult(
                short=result["short"], worker_id=result["worker_id"],
                engine="claude", reply=report["text"], state="idle",
                queued=False, delivery_id=result["delivery_id"],
                delivery_status="settled", settled=True,
                turn_ref=result["turn_ref"],
            )
        if prompt is not None:
            # After a first send, reconstruct the typed public identity from
            # the already-verified create result; a second mutable store read
            # could otherwise lose a freshly minted original worker UUID.
            from ccx.store.identity import WorkerOwner
            return self._uuid_session_info(
                WorkerOwner(result["worker_id"], "claude"), result["record"],
            )
        self._uuid_store.revalidate()
        return self._uuid_session_info(
            self._uuid_store.resolve(result["worker"]), result["record"],
        )

    def fork(
        self, target: str, prompt: str | None = None, *,
        name: str | None = None, worker_id: str | None = None,
        model: str | None = None, effort: str | None = None,
        caps: dict | None = None, wait: bool = False,
        timeout: float | None = None,
    ) -> SessionInfo | SendResult:
        """Fork one owned Claude history with an optional original first turn."""
        from math import isfinite
        from ccx.ops.forking import fork
        from ccx.store.root import GenerationError
        from ccx.store.identity import IdentityError, UnknownWorker, parse_managed_target

        if timeout is not None and (
            type(timeout) not in (int, float) or not isfinite(timeout) or timeout <= 0
        ):
            raise InvalidRequest("fork timeout must be positive finite seconds or None")
        deadline = time.monotonic() + timeout if timeout is not None else None
        try:
            source = parse_managed_target(target).qualified("claude")
            result = fork(
                source, worker_id=worker_id, name=name, model=model, effort=effort,
                created_via="python-api", expected_generation=self._uuid_store.generation,
                deadline=deadline, prompt=prompt, caps=caps, wait=wait,
            )
            if wait:
                report = result["report"]
                return SendResult(
                    short=result["short"], worker_id=result["worker_id"],
                    engine="claude", reply=report["text"], state="idle",
                    queued=False, delivery_id=result["delivery_id"],
                    delivery_status="settled", settled=True,
                    turn_ref=result["turn_ref"],
                )
            if prompt is not None:
                from ccx.store.identity import WorkerOwner
                return self._uuid_session_info(
                    WorkerOwner(result["worker_id"], "claude"), result["record"],
                )
            self._uuid_store.revalidate()
            return self._uuid_session_info(
                self._uuid_store.resolve(result["worker"]), result["record"],
            )
        except UnknownWorker as exc:
            raise worker_not_found(target) from exc
        except IdentityError as exc:
            raise InvalidRequest(f"fork: invalid Claude full UUID: {exc}") from exc
        except GenerationError as exc:
            raise SessionUnavailable(f"fork: Claude ACTIVE generation unavailable: {exc}") from exc

    def send(
        self, short: str, text: str, *, wait: bool | None = None,
        timeout: float | None = None, caps: dict | None = None,
        delivery_id: str | None = None,
    ) -> SendResult:
        """One original full-UUID Claude native delivery, never short-routed.

        Caller-known delivery_id is immutable. Unknown native effect and
        waited completion timeouts retain this original token; they never
        authorize a second provider effect.
        """
        from uuid import uuid4
        from ccx.ops.send import send_turn
        from ccx.ops.wait import wait_delivery
        from ccx.ops.report import report_worker
        from ccx.store.root import NativeEffectIndeterminate, GenerationError
        from ccx.claude.delivery import ClaudeDeliveryConflict
        from .errors import SendIndeterminate,OperationConflict
        if not isinstance(text,str) or not text.strip():
            raise InvalidRequest("send requires nonempty prompt")
        if timeout is not None and (not isinstance(timeout,(int,float))
                                    or timeout <= 0):
            raise InvalidRequest("send timeout must be positive or None")
        rec=self._resolve_managed(short,"send")
        worker_id=rec["workerId"]
        handle=f"claude:{worker_id}"
        token=delivery_id if delivery_id is not None else str(uuid4())
        deadline=time.monotonic()+timeout if timeout is not None else None
        try:
            first=send_turn(handle,text,delivery_id=token,mode="new_turn_only",
                            cap=caps,expected_generation=self._uuid_store.generation)
        except ClaudeDeliveryConflict as exc:
            raise OperationConflict(
                f"original delivery {token!r} content conflict: {exc}",
                short=worker_id[:8],worker_id=worker_id,
            ) from exc
        except NativeEffectIndeterminate as exc:
            raise SendIndeterminate(
                "native Claude effect unknown; reconcile original token, never "
                f"resend: {exc}",short=worker_id[:8],delivery_id=token,
                delivery_status="submitted-unknown",
            ) from exc
        except GenerationError as exc:
            raise SessionUnavailable(f"Claude native send unavailable: {exc}") from exc
        observed=None
        if wait is not False:
            completed = wait_delivery(
                handle, delivery_id=token,
                timeout=None if deadline is None else max(0., deadline-time.monotonic()),
                expected_generation=self._uuid_store.generation,
            )
            observed = {"status": "settled", "delivery": completed["delivery"]}
        result=observed or first
        delivery=result.get("delivery") or {}
        status=result.get("status") or delivery.get("status")
        if status=="rejected":
            raise DeliveryRejected(
                delivery.get("rejection_message") or "native Claude refused",
                short=worker_id[:8],refusal=delivery.get("rejection_code"),
                delivery_id=token,delivery_status="rejected",
            )
        settled=(bool(first.get("settled")) if observed is None
                 else status=="settled")
        reply=None
        if settled:
            reply=(completed["report"]["text"] if observed is not None else
                   report_worker(handle,turn_ref=delivery.get("turn_ref"))["text"])
        return SendResult(
            short=worker_id[:8],worker_id=worker_id,engine="claude",
            reply=reply,state=("idle" if settled else "busy"),queued=False,
            delivery_id=token,
            delivery_status=("settled" if settled else status),
            settled=settled,
            turn_ref=delivery.get("turn_ref"),
        )

    def _semantic_result(
        self, short: str, delivery_id: str, delivery: dict | None, *,
        state: str | None = None,
    ) -> SemanticDeliveryResult:
        """Typed original semantic Claude receipt, full UUID globally owned."""
        owner=self._uuid_store.resolve(short)
        value=delivery or {}
        status=value.get("status") or "pending"
        refusal=value.get("last_refusal") or {}
        return SemanticDeliveryResult(
            short=owner.short,worker_id=owner.worker_id,engine="claude",
            delivery_id=value.get("delivery_id") or delivery_id,
            status=status,accepted=status in ("accepted","settled"),
            owned=bool(value.get("owned")),
            settled=bool(value.get("settlement")),
            recommended_action=value.get("recommended_action"),
            route=value.get("route"),placement=value.get("placement"),
            target_turn_ref=value.get("target_turn_ref"),
            turn_ref=value.get("turn_ref"),
            managed_turn_ordinal=value.get("managed_turn_ordinal"),
            state=state,attempts=int(value.get("attempts") or 0),
            maybe_written=bool(value.get("maybe_written")),
            evidence=value.get("evidence"),
            evidence_uuid=value.get("evidence_uuid"),
            reason=(value.get("rejection_message") or refusal.get("message")
                    or value.get("evidence_status")),
        )

    def deliver(
        self,
        short: str,
        text: str,
        *,
        delivery_id: str,
        mode: str = "auto",
        timeout: float | None = None,
        caps: dict | None = None,
    ) -> SemanticDeliveryResult:
        """Atomically deliver one caller-identified semantic message."""
        from ccx.ops.send import send_turn
        from ccx.claude.delivery import ClaudeDeliveryConflict
        from ccx.store.root import NativeEffectIndeterminate
        from .errors import OperationConflict
        if mode not in {"auto", "new_turn_only"}:
            raise InvalidRequest(f"deliver: unsupported mode {mode!r}")
        if not isinstance(text, str) or not text.strip():
            raise InvalidRequest("deliver requires nonempty prompt")
        violation = commands_turns._send_payload_violation(text)
        if violation is not None:
            size, limit = violation
            raise PayloadTooLarge(
                payload_guidance(short, size, limit),
                short=short, size=size, limit=limit,
            )
        rec = self._resolve_managed(short, "deliver")
        worker_id = rec["workerId"]
        try:
            result = send_turn(
                f"claude:{worker_id}", text, delivery_id=delivery_id,
                mode=mode, cap=caps, timeout=timeout,
                expected_generation=self._uuid_store.generation,
            )
        except ClaudeDeliveryConflict as exc:
            raise OperationConflict(str(exc), short=worker_id[:8]) from exc
        except NativeEffectIndeterminate as exc:
            raise SendIndeterminate(
                f"Claude delivery uncertain: {exc}",
                short=worker_id[:8], delivery_id=delivery_id,
                delivery_status="submitted-unknown",
            ) from exc
        current = self._uuid_store.record(worker_id)
        return self._semantic_result(
            worker_id, delivery_id, result.get("delivery"),
            state=(current.get("lifecycle") or {}).get("state"),
        )

    def delivery_status(
        self, short: str, delivery_id: str,
    ) -> SemanticDeliveryResult:
        """Read original Claude receipt without re-entering the provider."""
        from ccx.ops.wait import reconcile_turn
        from ccx.store.root import GenerationError
        rec=self._resolve_managed(short,"delivery_status")
        wid=rec["workerId"]
        self._uuid_store.revalidate()
        try:
            verdict=reconcile_turn(f"claude:{wid}",delivery_id=delivery_id)
        except GenerationError as exc:
            raise SessionUnavailable(
                f"Claude delivery unavailable: {exc}"
            ) from exc
        original=verdict.get("delivery")
        if original is None:
            raise SessionNotFound(
                f"no original delivery {delivery_id!r} for Claude {wid}"
            )
        latest=self._uuid_store.record(wid)
        return self._semantic_result(
            wid,delivery_id,original,
            state=(latest.get("lifecycle") or {}).get("state"),
        )

    @staticmethod
    def _input_deadline(timeout: float | None) -> float | None:
        from math import isfinite
        if timeout is not None and (
            type(timeout) not in (int, float) or not isfinite(timeout) or timeout <= 0
        ):
            raise InvalidRequest("input timeout must be positive and finite or None")
        return None if timeout is None else time.monotonic() + timeout

    @staticmethod
    def _input_remaining(deadline: float | None, cap: float | None = None) -> float | None:
        if deadline is None:
            return cap
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TurnIncomplete("input deadline exhausted")
        return remaining if cap is None else min(cap, remaining)

    def _input_turn_fence(
        self, worker_id: str, record: dict, request_id: str,
        kind: str, deadline: float | None,
    ) -> tuple[str, str | None]:
        """Bind the already-running original turn while its native gate is open."""
        from ccx.store.root import GenerationError

        sid = (record.get("native") or {}).get("sessionId")
        try:
            activation = self._uuid_store.native_activation(worker_id)
        except GenerationError as exc:
            raise SessionUnavailable("input original native claim unavailable") from exc
        if (not isinstance(sid, str) or not sid
                or activation.get("state") != "confirmed"
                or activation.get("source") != f"claude-session:{sid}"
                or (activation.get("nativeBinding") or {}).get("sessionId") != sid):
            raise SessionUnavailable("Claude input source could not be verified; check the worker",
                                     short=worker_id[:8])
        status = _rpc(worker_id, {"op": "status"},
                      timeout=self._input_remaining(deadline, 5.0))
        waiting = status.get("waitingFor")
        ref = status.get("activeTurnRef")
        if (status.get("ok") is not True or status.get("state") != "waiting"
                or not isinstance(waiting, dict)
                or waiting.get("id") != request_id
                or pending_input_kind(waiting) != kind):
            raise SessionUnavailable("Claude is not accepting input; check its pending request",
                                     short=worker_id[:8])
        if (not isinstance(ref, str)
                or not ref.startswith("ccx-turn:claude:v1:")
                or len(ref) <= len("ccx-turn:claude:v1:")):
            raise SessionUnavailable("Claude turn cannot be identified; inspect the worker",
                                     short=worker_id[:8])
        prior = self._uuid_store.record(worker_id).get("lastSettledTurn") or {}
        prior_ref = prior.get("turnRef")
        if not (isinstance(prior_ref, str)
                and prior_ref.startswith("ccx-turn:claude:v1:")):
            prior_ref = None
        self._uuid_store.revalidate()
        return ref, prior_ref

    def _input_wait_reply(
        self, worker_id: str, request_id: str, response_id: str | None,
        turn_ref: str, prior_ref: str | None, deadline: float | None,
    ) -> SendResult:
        """Observe only the original native R; never send another input."""
        from ccx.ops.wait import wait_worker
        from ccx.ops.report import report_worker
        from .errors import CcxError
        from ccx.store.root import GenerationError

        def incomplete(why: str, *, state: str | None = None) -> TurnIncomplete:
            err = TurnIncomplete(
                f"{why}; input accepted, never retry: request_id={request_id!r}, "
                f"response_id={response_id!r}, turn_ref={turn_ref!r}",
                short=worker_id[:8], state=state, turn_id=turn_ref,
            )
            err.input_accepted = True
            err.request_id = request_id
            err.response_id = response_id
            err.turn_ref = turn_ref
            return err

        current = self._uuid_store.record(worker_id)
        settled = current.get("lastSettledTurn") or {}
        if settled.get("turnRef") != turn_ref:
            budget = None if deadline is None else max(0.0, deadline - time.monotonic())
            try:
                verdict = wait_worker(
                    f"claude:{worker_id}", next_turn=True, after_turn=prior_ref,
                    timeout=budget, expected_generation=self._uuid_store.generation,
                )
            except (CcxError, GenerationError) as exc:
                raise incomplete("original resumed turn observation unavailable") from exc
            latest = verdict.get("last_settled_turn") or {}
            if latest.get("turn_ref") != turn_ref:
                if verdict.get("status") == "waiting":
                    raise SessionWaiting(
                        "new input gate opened before the original turn settled",
                        short=worker_id[:8], waiting_for=verdict.get("waiting_for"),
                    )
                if verdict.get("status") == "budget-stopped":
                    raise BudgetStopped("original resumed turn stopped on budget",
                                        short=worker_id[:8], phase="turn")
                if verdict.get("status") in ("dead", "crashed", "host-dead"):
                    error = api_error_from_lifecycle(
                        (self._uuid_store.record(worker_id).get("lifecycle") or {}),
                        short=worker_id[:8],
                    )
                    if error is not None:
                        raise error
                raise incomplete(
                    "original resumed turn not proven settled",
                    state=verdict.get("state"),
                )
            settled = {"turnRef": latest["turn_ref"], "outcome": latest.get("outcome")}
        if settled.get("outcome") != "completed":
            raise incomplete(
                "The resumed turn did not finish "
                f"({settled.get('outcome') or 'unknown'})"
            )
        try:
            report = report_worker(f"claude:{worker_id}", turn_ref=turn_ref)
        except (CcxError, GenerationError) as exc:
            raise incomplete(
                "original input turn completed but its exact reply report is unavailable"
            ) from exc
        if (report.get("turn_ref") != turn_ref
                or report.get("status") != "completed"
                or not isinstance(report.get("text"), str)):
            raise incomplete("The reply is not confirmed complete; retry or inspect the turn")
        return SendResult(
            short=worker_id[:8], worker_id=worker_id, engine="claude",
            reply=report["text"], transcript=None, state=S.IDLE,
            queued=False, settled=True, turn_ref=turn_ref,
        )

    def _require_dead_original_question_host(
        self, real: str, rec: dict, *, response_id: str | None,
    ) -> None:
        """Prove original QUESTION host died before any native answer effect."""
        from ccx.store.liveness import owned_process
        from ccx.store.root import GenerationError
        lc = rec.get("lifecycle") or {}
        if (not lc.get("hostPid") or owned_process(
                lc.get("hostPid"), lc.get("hostStart")) != "dead"):
            raise SessionUnavailable(
                "Cannot confirm Claude stopped; check its status before sending another answer",
                short=real,
            )
        native = rec.get("native") or {}
        try:
            activation = self._uuid_store.native_activation(real)
        except GenerationError as exc:
            raise SessionUnavailable("Cannot verify this question belongs to the worker; check its status") from exc
        sid = native.get("sessionId")
        if (not isinstance(sid, str) or not sid
                or activation.get("state") != "confirmed"
                or activation.get("source") != f"claude-session:{sid}"
                or (activation.get("nativeBinding") or {}).get("sessionId") != sid):
            raise SessionUnavailable("Cannot verify this question belongs to the worker; check its status", short=real)
        if response_id is not None and self._uuid_store.read_receipt(
            real, "answer", response_id,
        ) is not None:
            raise SessionUnavailable(
                "This response ID was already used; "
                "reconcile it, never convert to another message", short=real,
            )
        self._uuid_store.revalidate()

    def answer(
        self,
        short: str,
        text: str | None = None,
        *,
        request_id: str | None = None,
        options: list[tuple[int, str]] | None = None,
        text_answers: list[tuple[int, str]] | None = None,
        wait: bool = False,
        timeout: float | None = None,
        fallback_to_send: bool = False,
        response_id: str | None = None,
    ) -> SendResult:
        """Answer the exact current original Claude question, optionally wait.

        A missing request_id binds the currently brokered question once.
        A dead original host may become a NEW strict message only with
        fallback_to_send=True and positive pre-response no-effect evidence.
        """
        deadline = self._input_deadline(timeout)
        if request_id is not None and (
            not isinstance(request_id, str) or not request_id or len(request_id) > 256
        ):
            raise InvalidRequest("answer: request_id must be a nonempty bounded string")
        rec = self._resolve_managed(short, "answer")
        real = rec["workerId"]
        question = rec.get("waitingFor")
        illegality = answer_illegality(question, real)
        if illegality:
            error_type = DeliveryRejected if pending_input_kind(question) is None else SessionUnavailable
            raise error_type(illegality, short=real)
        if not isinstance(question, dict) or not question.get("id"):
            raise SessionUnavailable("answer: no brokered question request ID", short=real)
        if request_id is None:
            request_id = question["id"]
        if not isinstance(request_id, str) or len(request_id) > 256:
            raise InvalidRequest("answer: original request_id must be bounded")
        fence = answer_request_fence(question, request_id)
        if fence:
            raise DeliveryRejected(
                f"answer rejected: {fence} (expected {request_id!r}, "
                f"pending {question.get('id')!r})",
                short=real, reason=fence,
                state=(rec.get("lifecycle") or {}).get("state"),
            )

        answers, err = question_answer_map(
            (question.get("input") or {}).get("questions") or [],
            text=text, options=options, text_answers=text_answers,
        )
        if err:
            raise InvalidRequest(f"answer: {err}", short=real)
        if not answers:
            raise InvalidRequest("answer: answer text is empty", short=real)

        if not host_rpc.host_socket_live(real, rec):
            if not fallback_to_send:
                raise SessionUnavailable(
                    "answer: original host unavailable; fallback_to_send=True "
                    "only when its owned process is confirmed dead", short=real,
                )
            self._require_dead_original_question_host(
                real, rec, response_id=response_id,
            )
            return self.send(
                real, question_answer_message(answers), wait=wait,
                timeout=self._input_remaining(deadline), caps=None,
            )
        turn_fence = self._input_turn_fence(
            real, rec, request_id, "question", deadline,
        ) if wait else None
        msg = {"op": "answer-permission", "answers": answers, "request_id": request_id}
        if response_id is not None:
            msg["response_id"] = response_id
        reply = _rpc(real, msg, timeout=self._input_remaining(deadline, 5.0))
        _original_input_ack(reply, op="answer", worker_id=real,
                            request_id=request_id, response_id=response_id)
        if turn_fence is not None:
            return self._input_wait_reply(real, request_id, response_id,
                                          *turn_fence, deadline)
        return SendResult(short=real[:8], worker_id=real, engine="claude",
                          queued=True, state=S.BUSY)

    def respond_elicitation(
        self,
        short: str,
        action: str,
        content: dict | None = None,
        *,
        request_id: str | None = None,
        response_id: str | None = None,
        wait: bool = False,
        timeout: float | None = None,
    ) -> SendResult:
        """Resolve the exact original MCP elicitation, optionally wait for R.

        Omitted request_id binds the current brokered elicitation once.
        The response is schema-fenced; there is no new-message fallback.
        """
        deadline = self._input_deadline(timeout)
        if request_id is not None and (
            not isinstance(request_id, str) or not request_id or len(request_id) > 256
        ):
            raise InvalidRequest("respond: request_id must be a nonempty bounded string")
        rec = self._resolve_managed(short, "respond")
        real = rec["workerId"]
        pending = rec.get("waitingFor")
        illegality = elicitation_illegality(pending, real)
        if illegality:
            error_type = DeliveryRejected if pending_input_kind(pending) is None else SessionUnavailable
            raise error_type(illegality, short=real)
        if not isinstance(pending, dict) or not pending.get("id"):
            raise SessionUnavailable("respond: no brokered elicitation request ID", short=real)
        if request_id is None:
            request_id = pending["id"]
        if not isinstance(request_id, str) or len(request_id) > 256:
            raise InvalidRequest("respond: original request_id must be bounded")
        fence = answer_request_fence(pending, request_id)
        if fence:
            raise DeliveryRejected(
                f"respond rejected: {fence} (expected {request_id!r}, "
                f"pending {pending.get('id')!r})",
                short=real, reason=fence,
                state=(rec.get("lifecycle") or {}).get("state"),
            )
        problem = elicitation_response_problem(pending, action, content)
        if problem:
            raise InvalidRequest(f"respond: {problem}", short=real)
        if not host_rpc.host_socket_live(real, rec):
            raise SessionUnavailable(
                f"respond: session host not alive "
                f"(state={(rec.get('lifecycle') or {}).get('state')}); the MCP "
                "request died with it and cannot be answered",
                short=real,
            )

        turn_fence = self._input_turn_fence(
            real, rec, request_id, "elicitation", deadline,
        ) if wait else None
        msg: dict = {"op": "elicitation-response", "action": action,
                     "request_id": request_id}
        if content is not None:
            msg["content"] = content
        if response_id is not None:
            msg["response_id"] = response_id
        reply = _rpc(real, msg, timeout=self._input_remaining(deadline, 5.0))
        _original_input_ack(reply, op="respond", worker_id=real,
                            request_id=request_id, response_id=response_id)
        if turn_fence is not None:
            return self._input_wait_reply(real, request_id, response_id,
                                          *turn_fence, deadline)
        return SendResult(short=real[:8], worker_id=real, engine="claude",
                          queued=True, state=S.BUSY)

    def approve(
        self,
        short: str,
        *,
        request_id: str | None = None,
        reason: str | None = None,
        wait: bool = False,
        timeout: float | None = None,
        response_id: str | None = None,
    ) -> SendResult:
        """Allow one original Claude permission, optionally await its turn.

        The current brokered gate binds an omitted request_id once.
        """
        return self._permission_decision(
            short, "allow", reason=reason, request_id=request_id,
            wait=wait, timeout=timeout, response_id=response_id,
        )

    def reject(
        self,
        short: str,
        *,
        request_id: str | None = None,
        reason: str | None = None,
        wait: bool = False,
        timeout: float | None = None,
        response_id: str | None = None,
    ) -> SendResult:
        """Deny one original Claude permission, optionally await its turn.

        A reason is passed to the original host gate unchanged.
        """
        return self._permission_decision(
            short, "deny", reason=reason, request_id=request_id,
            wait=wait, timeout=timeout, response_id=response_id,
        )

    def _permission_decision(
        self,
        short: str,
        decision: str,
        *,
        reason: str | None,
        request_id: str | None,
        wait: bool,
        timeout: float | None,
        response_id: str | None = None,
    ) -> SendResult:
        """Resolve exactly one native permission and optionally observe its turn."""
        op = "approve" if decision == "allow" else "reject"
        deadline = self._input_deadline(timeout)
        if request_id is not None and (
            not isinstance(request_id, str) or not request_id or len(request_id) > 256
        ):
            raise InvalidRequest(f"{op}: request_id must be a nonempty bounded string")
        rec = self._resolve_managed(short, op)
        real = rec["workerId"]
        # A decision against an AskUserQuestion or an MCP elicitation is the
        # wrong verb; the host would reject it. Fail early with the same
        # guidance the CLI prints.
        wf = rec.get("waitingFor")
        kind = pending_input_kind(wf)
        if kind not in (None, KIND_PERMISSION):
            what = ("an AskUserQuestion answer" if kind == "question"
                    else "an MCP elicitation response")
            verb = "respond_elicitation" if kind == "elicitation" else wrong_verb_hint(kind)
            raise SessionUnavailable(
                f"session {real} is waiting for {what}; use {verb} instead",
                short=real,
            )

        if not isinstance(wf, dict) or not wf.get("id"):
            raise DeliveryRejected(f"{op}: no brokered permission request", short=real)
        if not host_rpc.host_socket_live(real, rec):
            raise SessionUnavailable(
                f"{op}: session host not alive (state={(rec.get('lifecycle') or {}).get('state')})",
                short=real,
            )
        if request_id is None:
            request_id = wf["id"]
        if not isinstance(request_id, str) or len(request_id) > 256:
            raise InvalidRequest(f"{op}: original request_id must be bounded")
        fence = answer_request_fence(wf, request_id)
        if fence:
            raise DeliveryRejected(f"{op}: {fence}", short=real, reason=fence)
        turn_fence = self._input_turn_fence(
            real, rec, request_id, "permission", deadline,
        ) if wait else None
        msg: dict = {"op": "permission-decision", "decision": decision,
                     "request_id": request_id}
        if reason:
            msg["reason"] = reason
        if response_id is not None:
            msg["response_id"] = response_id
        reply = _rpc(real, msg, timeout=self._input_remaining(deadline, 5.0))
        _original_input_ack(reply, op=op, worker_id=real,
                            request_id=request_id, response_id=response_id)
        if turn_fence is not None:
            return self._input_wait_reply(real, request_id, response_id,
                                          *turn_fence, deadline)
        return SendResult(short=real[:8], worker_id=real, engine="claude",
                          queued=True, state=S.BUSY)

    def _steer_result(self, short: str, delivery_id: str, delivery: dict | None,
                      *, state: str | None, timed_out: bool) -> SteerResult:
        owner = self._uuid_store.resolve(short)
        value = delivery or {}
        state_key = value.get("status")
        placement = value.get("placement")
        if state_key == "accepted":
            status = ("consumed-current-turn" if placement == "current_turn"
                      else "started-next-turn" if placement == "new_turn"
                      else "unknown")
        elif state_key == "pending" and value.get("owned"):
            status = ("unknown" if value.get("evidence_status") == "unknown"
                      else "accepted-pending")
        else:
            status = state_key or "unknown"
        owned = bool(value.get("owned"))
        refusal = value.get("last_refusal") or {}
        return SteerResult(
            short=owner.short, worker_id=owner.worker_id, engine="claude",
            delivery_id=value.get("delivery_id") or delivery_id,
            accepted=owned, status=status,
            timed_out=timed_out, state=state,
            pending=owned and status == "accepted-pending",
            evidence=value.get("evidence"),
            evidence_uuid=value.get("evidence_uuid"),
            attempts=value.get("attempts"),
            reason=(value.get("rejection_message") or refusal.get("message")
                    or value.get("evidence_status")),
            recommended_action=("wait" if timed_out else
                                value.get("recommended_action")),
        )

    def steer(
        self,
        short: str,
        text: str,
        *,
        delivery_id: str | None = None,
        wait: bool = True,
        timeout: float | None = STEER_OBSERVE_DEFAULT_S,
        caps: dict | None = None,
    ) -> SteerResult:
        """Steer a BUSY managed worker with a durably-delivered prompt.

        This is deliberately distinct from :meth:`send` (which starts a strict
        new turn on an idle worker) and from :meth:`interrupt`: it aims a
        prompt at a worker that is already working, with exactly-once delivery.

        The prompt and its transcript fence are journaled atomically before the
        host acknowledges ownership, so ``accepted=True`` means CCX owns the
        delivery even if this process dies immediately afterwards. Retrying
        with the same ``delivery_id`` reconciles the original delivery and can
        never paste the prompt a second time — that is the correct response to
        a lost RPC reply.

        With ``wait=True`` the call observes for up to ``timeout`` seconds for
        stronger placement evidence. ``status`` speaks one vocabulary:
        ``accepted-pending`` (durably owned, not visibly placed yet),
        ``consumed-current-turn`` (the running turn saw it),
        ``started-next-turn`` (it runs next), or ``unknown``. Observation
        elapsing is NOT a failure: the result has ``accepted=True``,
        ``status="accepted-pending"``, ``timed_out=True``, ``pending=True`` and
        ``recommended_action="wait"``. Never resend on that outcome; use
        :meth:`steer_status` (or ``ccx steer-status``) for placement.

        ``caps`` is a per-message Claude quota cap, exactly as in :meth:`send`.
        It travels with this delivery — it is journaled with the prompt and
        armed only for the turn the steered text joins — so it survives a host
        restart without authorizing a second paste.

        Raises :class:`~ccx.errors.DeliveryRejected` if the host refuses the
        steer (for example the worker is idle, waiting, or compacting), and
        :class:`~ccx.errors.SteerIndeterminate` when durable acceptance cannot
        be established or an accepted delivery resolves to ``unknown``.
        """
        from math import isfinite
        from uuid import uuid4
        from ccx.ops.send import send_turn
        from ccx.ops.wait import reconcile_turn
        from ccx.claude.delivery import ClaudeDeliveryConflict, valid_delivery_id
        from ccx.store.root import NativeEffectIndeterminate
        from .errors import OperationConflict

        if timeout is not None and (
            type(timeout) not in (int, float) or not isfinite(timeout) or timeout < 0
        ):
            raise InvalidRequest("steer observation timeout must be finite and nonnegative")
        if not isinstance(text, str) or not text.strip():
            raise InvalidRequest("steer: prompt text is required")
        violation = commands_turns._send_payload_violation(text)
        if violation is not None:
            size, limit = violation
            raise PayloadTooLarge(
                payload_guidance(short, size, limit), short=short,
                size=size, limit=limit,
            )
        token = delivery_id if delivery_id is not None else str(uuid4())
        if not valid_delivery_id(token):
            raise InvalidRequest("steer requires a nonempty delivery_id of at most 128 characters")
        rec = self._resolve_managed(short, "steer")
        worker_id = rec["workerId"]
        handle = f"claude:{worker_id}"
        try:
            first = send_turn(
                handle, text, delivery_id=token, mode="steer_only", cap=caps,
                expected_generation=self._uuid_store.generation,
            )
        except ClaudeDeliveryConflict as exc:
            raise OperationConflict(str(exc), short=worker_id[:8]) from exc
        except NativeEffectIndeterminate as exc:
            raise SteerIndeterminate(
                f"steer native effect uncertain; reconcile original delivery {token}: {exc}",
                short=worker_id[:8], delivery_id=token,
                delivery_status="unknown",
            ) from exc

        def project(original: dict, *, timed_out: bool) -> SteerResult:
            delivery = original.get("delivery") or {}
            lifecycle = self._uuid_store.record(worker_id).get("lifecycle") or {}
            value = self._steer_result(
                handle, token, delivery,
                state=lifecycle.get("state"), timed_out=timed_out,
            )
            if value.status == "rejected":
                raise DeliveryRejected(
                    delivery.get("rejection_message") or "Claude refused steer",
                    short=worker_id[:8], delivery_id=token,
                    delivery_status="rejected",
                    refusal=delivery.get("rejection_code"),
                    state=value.state,
                )
            if not value.accepted:
                raise SteerIndeterminate(
                    f"steer {token} lacks durable native ownership",
                    short=worker_id[:8], delivery_id=token,
                    delivery_status=value.status,
                )
            if value.status == "unknown":
                raise SteerIndeterminate(
                    f"steer {token} placement remains unknown; do not re-send",
                    short=worker_id[:8], delivery_id=token,
                    delivery_status="unknown",
                )
            return value

        if wait is False:
            return project(first, timed_out=False)
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            self._uuid_store.revalidate()
            observed = reconcile_turn(
                handle, delivery_id=token, include_worker=False,
                expected_generation=self._uuid_store.generation,
            )
            item = project(observed, timed_out=False)
            if not item.pending:
                return item
            if deadline is not None and time.monotonic() >= deadline:
                return project(observed, timed_out=True)
            time.sleep(STEER_OBSERVE_POLL_S if deadline is None else
                       min(STEER_OBSERVE_POLL_S, max(0, deadline - time.monotonic())))

    def steer_status(self, short: str, delivery_id: str) -> SteerResult:
        """Reconcile one durable steering delivery without sending anything."""
        from ccx.ops.wait import reconcile_turn
        from ccx.claude.delivery import read_receipt, valid_delivery_id
        if not valid_delivery_id(delivery_id):
            raise InvalidRequest("steer status requires a valid delivery_id")
        rec = self._resolve_managed(short, "steer status")
        worker_id = rec["workerId"]
        handle = f"claude:{worker_id}"
        original = read_receipt(handle, delivery_id)
        if original is None or original.get("requestedMode") != "steer_only":
            raise SessionNotFound(
                f"no exact Claude steer delivery {delivery_id!r} for this worker",
                short=worker_id[:8],
            )
        probe = reconcile_turn(
            handle, delivery_id=delivery_id, include_worker=False,
            expected_generation=self._uuid_store.generation,
        )
        current = self._uuid_store.record(worker_id).get("lifecycle") or {}
        return self._steer_result(
            handle, delivery_id, probe.get("delivery"),
            state=current.get("state"), timed_out=False,
        )

    def compact(
        self,
        short: str,
        *,
        wait: bool = True,
        timeout: float | None = COMPACT_WAIT_DEFAULT_S,
        operation_id: str | None = None,
        execution_timeout: float | None = None,
    ) -> CompactOperationResult:
        """Compact one exact original Claude worker, with native terminal proof.

        The caller's observation budget never changes the immutable native
        effect identity; retry the same operation_id to reconcile, not resend.
        """
        from ccx.ops.compact import compact_worker

        rec = self._resolve_managed(short, "compact")
        return CompactOperationResult(**compact_worker(
            f"claude:{rec['workerId']}",
            operation_id=operation_id if operation_id is not None
            else new_compact_operation_id(),
            wait=wait, timeout=timeout,
            execution_timeout=execution_timeout,
        ))

    def compact_status(self, short: str, operation_id: str) -> CompactOperationResult:
        """Inspect one original compact journal without native submission/revival."""
        from ccx.ops.compact import compact_status

        rec = self._resolve_managed(short, "compact status")
        return CompactOperationResult(**compact_status(
            f"claude:{rec['workerId']}", operation_id=operation_id,
        ))

    def kill(self, short: str, *,
             timeout: float | None = KILL_TIMEOUT_DEFAULT_S) -> SessionInfo:
        """Confirm exact owned Claude host and native child are gone.

        Durable session history is retained; automatic full-UUID resume is
        currently unavailable. An unproven teardown raises, never succeeds.
        """
        from ccx.ops.wait import park_worker
        from ccx.store.identity import IdentityError, UnknownWorker
        try:
            owner = self._uuid_store.resolve(short)
        except UnknownWorker as exc:
            raise worker_not_found(short) from exc
        except IdentityError as exc:
            raise InvalidRequest(f"kill: invalid full-UUID worker target: {exc}") from exc
        if owner.engine != "claude":
            raise InvalidRequest("kill requires a Claude full-UUID owner")
        park_worker(owner.worker, timeout=timeout,
                    expected_generation=self._uuid_store.generation)
        return self._uuid_session_info(owner, self._uuid_store.record(owner.worker))

    def interrupt(
        self,
        short: str,
        *,
        expected_turn_ref: str | None = None,
    ) -> dict:
        """Request cancellation of one exact active full-UUID Claude turn.

        A successful return proves native acceptance, NOT interruption.
        ``confirmed`` and ``outcome`` expose separately observed terminality.
        """
        from ccx.ops.cancel import stop_worker
        rec = self._resolve_managed(short, "interrupt")
        result = stop_worker(
            f"claude:{rec['workerId']}", expected_turn_ref=expected_turn_ref,
            expected_generation=self._uuid_store.generation,
        )
        return {
            **result, "ok": True, "short": rec["workerId"][:8],
            "sent": True,
            "cancelled": (result["confirmed"] and result["outcome"] == "interrupted"),
        }

    def set_model(self, short: str, model: str | None, *, restart: bool = False) -> SessionInfo:
        """Edit the next Claude spawn model, never an already running turn.

        Plain edits require no warm owned host or native child. An explicit
        restart requires a proven settled-idle original and confirms teardown.
        A later send/resume reuses the original session with the saved model.
        """
        info, _restarted = self._set_spawn_config(
            short, {"model": model}, restart=restart,
        )
        return info

    def set_effort(self, short: str, effort: str | None, *, restart: bool = False) -> SessionInfo:
        """Edit future Claude spawn effort (low/medium/high/xhigh/max).

        Changing effort invalidates the model prompt cache on the next turn.
        None removes the override; no current native turn is reconfigured.
        """
        info, _restarted = self._set_spawn_config(
            short, {"effort": effort}, restart=restart,
        )
        return info

    def _set_spawn_config(
        self, target: str, updates: dict[str, str | None], *,
        restart: bool | None, timeout: float = KILL_TIMEOUT_DEFAULT_S,
    ) -> tuple[SessionInfo, bool]:
        """The ONE original full-UUID Claude spawn-config admission and CAS.

        restart=None means WorkerClient's settled-IDLE convenience selection;
        explicit public setters keep their existing restart=False signature.
        Only authenticated read-only host probes are used for admission, never
        ensure_ready or an unproven short-record status projection.
        """
        from math import isfinite
        from ccx.store.liveness import owned_process
        from ccx.store.root import GenerationError, RecordConflict
        from ccx.ops.terminate import confirmed_kill_uuid

        if not updates or set(updates) - {"model", "effort", "permissions"}:
            raise InvalidRequest("Claude configuration requires model, effort and/or permissions")
        validated: dict[str, str | bool | None] = {}
        for field, value in updates.items():
            if field == "permissions":
                if type(value) is not str or value not in ("full", "restricted"):
                    raise InvalidRequest("Claude permissions must be full or restricted")
                validated["skip_permissions"] = value == "full"
                validated["permission_mode"] = None
                if value == "full":
                    # Full cannot retain a hidden explicit tool denial.
                    validated["disallowed_tools"] = None
                continue
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise InvalidRequest(f"{field} must be nonblank text or None")
            normalized = value.strip() if isinstance(value, str) else None
            if field == "effort" and normalized not in (
                None, "low", "medium", "high", "xhigh", "max"
            ):
                raise InvalidRequest("Claude effort must be low/medium/high/xhigh/max or None")
            validated[field] = normalized
        if (type(timeout) not in (int, float)
                or not isfinite(timeout) or timeout <= 0):
            raise InvalidRequest("Claude config timeout must be positive and finite")
        if restart not in (True, False, None):
            raise InvalidRequest("Claude config restart must be true, false, or automatic")
        deadline = time.monotonic() + timeout
        rec = self._resolve_managed(target, "set config")
        owner = self._uuid_store.resolve(f"claude:{rec['workerId']}")
        handle = owner.worker
        native = rec.get("native") or {}
        session_id = native.get("sessionId")
        if not isinstance(session_id, str) or not session_id:
            raise SessionUnavailable("Claude session is missing; inspect the worker", short=owner.short)

        def remaining() -> float:
            left = deadline - time.monotonic()
            if left <= 0:
                raise SessionUnavailable("Claude config observation deadline expired", short=owner.short)
            return left

        # _rpc verifies the original generation, peer credentials and host
        # process START token on its actual socket, before any status request.
        attested = _rpc(handle, {"op": "startup-status"}, timeout=min(.5, remaining()))
        self._uuid_store.revalidate()
        current = self._uuid_store.record(handle)
        lc = current.get("lifecycle") or {}

        host_presence = owned_process(lc.get("hostPid"), lc.get("hostStart"))
        child_presence = owned_process(lc.get("workerPid"), lc.get("workerStart"))
        warm = False
        if attested.get("ok") is True:
            proof = lc.get("nativeStartProof")
            if (not isinstance(proof, dict)
                    or attested.get("workerId") != owner.worker_id
                    or attested.get("engine") != "claude"
                    or attested.get("generation") != self._uuid_store.generation
                    or attested.get("nativeStartProof") != proof
                    or proof.get("sessionId") != session_id
                    or any(proof.get(field) != lc.get(field) for field in (
                        "hostPid", "hostStart", "workerPid", "workerStart"
                    ))
                    or host_presence != "alive" or child_presence != "alive"
                    or not lc.get("hostStart") or not lc.get("workerStart")):
                raise SessionUnavailable(
                    "Claude process could not be verified; check its status before changing settings",
                    short=owner.short,
                )
            status = _rpc(handle, {"op": "status"}, timeout=min(.5, remaining()))
            if status.get("ok") is not True:
                raise SessionUnavailable(
                    "Claude status is unavailable; check it before changing settings",
                    short=owner.short,
                )
            # Refuse every potentially active, queued or undecidable control:
            # no native model swap may land while any of these remain.
            if (status.get("state") != "idle" or lc.get("state") != S.IDLE
                    or status.get("activeTurnRef") is not None
                    or status.get("pendingDeliveries") != []
                    or status.get("queuedInputs") != 0
                    or status.get("waitingFor") is not None
                    or status.get("compacting") is not False
                    or (status.get("compactOperation") is not None
                        and (not isinstance(status["compactOperation"], dict)
                             or status["compactOperation"].get("status")
                             not in {"succeeded", "noop", "failed"}))
                    or status.get("steerJournalError")
                    or status.get("compactJournalError")):
                raise DeliveryRejected(
                    "Claude is busy or waiting for input; finish the turn before changing settings.",
                    short=owner.short, state=status.get("state"), refusal="busy",
                )
            warm = True
        else:
            # A timeout, an unverified socket peer, or a live owned PID is
            # NOT proof of absence. Even a physically missing socket must
            # agree with both original host AND child process being gone.
            error = attested.get("error", "")
            if (not isinstance(error, str) or not error.startswith("unreachable:")
                    or host_presence != "dead" or child_presence != "dead"
                    or lc.get("state") in (S.BOOTING, S.RESUMING, S.BUSY, S.WAITING)):
                if lc.get("state") in (S.BOOTING, S.RESUMING, S.BUSY, S.WAITING):
                    raise DeliveryRejected(
                        "Claude is starting or busy; finish the turn before changing settings.",
                        short=owner.short, state=lc.get("state"), refusal="busy",
                    )
                raise SessionUnavailable(
                    "Cannot verify whether Claude is running; check its status and retry.",
                    short=owner.short,
                )
        if restart is False and warm:
            raise DeliveryRejected(
                "Claude is running; use --restart while idle or stop it first.",
                short=owner.short, state=lc.get("state"), refusal="busy",
            )
        restart_now = warm and restart is not False
        observed = {
            "lifecycle.state": lc.get("state"),
            "lifecycle.hostPid": lc.get("hostPid"),
            "lifecycle.hostStart": lc.get("hostStart"),
            "lifecycle.workerPid": lc.get("workerPid"),
            "lifecycle.workerStart": lc.get("workerStart"),
            "lifecycle.nativeStartProof": lc.get("nativeStartProof"),
        }
        try:
            patched = self._uuid_store.mutate_record(
                handle, patches={f"createArgs.{field}": value
                                 for field, value in validated.items()},
                expect=observed,
            )
        except (RecordConflict, GenerationError) as exc:
            raise SessionUnavailable(
                "Claude changed while saving; retry settings when idle",
                short=owner.short,
            ) from exc
        if not restart_now:
            return self._uuid_session_info(owner, patched), False

        # The config is already durably saved. A failing/unconfirmed teardown
        # is NOT success or a reason to undo the CAS; the old host may still
        # run old settings. The caller may inspect/stop it separately.
        try:
            stopped = confirmed_kill_uuid(
                self._uuid_store, handle, timeout=remaining(),
            )
            if not stopped.get("killed"):
                raise SessionUnavailable("Claude did not confirm stopping; check its status")
        except Exception as exc:
            raise SessionUnavailable(
                "Settings were saved, but Claude may still be running; "
                "the updated model/effort/permissions are for a later spawn only",
                short=owner.short,
            ) from exc
        self._uuid_store.revalidate()
        return self._uuid_session_info(owner, self._uuid_store.record(handle)), True

    def set_name(self, short: str, name: str | None) -> SessionInfo:
        """Rename exact managed full UUID using fresh-record semantic CAS."""
        from ccx.store.identity import IdentityError
        from ccx.store.root import GenerationError
        from ccx.store.identity import UnknownWorker
        try:
            owner = self._uuid_store.resolve(short)
            if owner.engine != "claude":
                raise InvalidRequest("CcxClient.set_name requires a Claude full-UUID worker")
            updated = self._uuid_store.rename_worker(
                owner.worker, engine="claude", name=name,
            )
            return self._uuid_session_info(owner, updated)
        except UnknownWorker as exc:
            raise worker_not_found(short) from exc
        except IdentityError as exc:
            raise InvalidRequest(f"Invalid Claude worker handle: {exc}") from exc
        except GenerationError as exc:
            raise SessionUnavailable(f"Worker records changed; reopen CCX: {exc}") from exc

    # -- read paths ---------------------------------------------------------

    def peek(self, short: str, *, live: bool = False) -> SessionStatus:
        """Recorded-only forensic status; native/liveness queries are fenced."""
        from ccx.store.identity import UnknownWorker, IdentityError
        from ccx.store.root import GenerationError
        if live:
            raise SessionUnavailable(
                "CcxClient.peek(live=True) requires full-UUID host adoption"
            )
        try:
            owner = self._uuid_store.resolve(short)
            if owner.engine != "claude":
                raise InvalidRequest("CcxClient.peek requires a Claude full-UUID target")
            rec = self._uuid_store.record(owner.worker)
            lc = rec.get("lifecycle")
            lc = lc if isinstance(lc, dict) else {}
            stats = rec.get("stats")
            stats = stats if isinstance(stats, dict) else {}
            import time as _time
            last = rec.get("lastTurnAt")
            age = max(0,int(_time.time()-last/1000)) if type(last) is int and last>0 else None
            return SessionStatus(
                short=owner.short, worker_id=owner.worker_id,
                engine="claude", state=lc.get("state"),
                turn=stats.get("turns"), last_turn_age_s=age,
                host_status="unobserved",
                mode="managed",
                waiting_for=PendingInput.from_waiting_for(rec.get("waitingFor")),
                context=commands_read._status_context_for_record(owner.worker, rec),
                background_task_summary=rec.get("backgroundTaskSummary"),
            )
        except UnknownWorker as exc:
            raise worker_not_found(short) from exc
        except IdentityError as exc:
            raise InvalidRequest(f"invalid Claude full-UUID target: {exc}") from exc
        except GenerationError as exc:
            raise SessionUnavailable(f"Worker record unavailable; inspect worker status: {exc}") from exc

    def status(self, shorts: list[str], *, live: bool = False) -> list[SessionStatus]:
        """Batch exact-owned forensic status; never native host polling."""
        if live:
            raise SessionUnavailable("Live batch status is not supported; use recorded status")
        return [self.peek(value, live=False) for value in shorts]

    def report(
        self, short, *, max_chars: int | None = None,
        live: bool = False,
    ) -> Report | dict:
        """Read Claude's latest turn reply; note an earlier confirmed reply if busy.

        A list of targets returns ``{"reports": [...], "errors": {...}}``.

        Turns without a final answer have an empty text and has_final_message=False.
        The report's note describes a newer active turn or a failed worker.
        """
        from ccx.ops.report import report_worker
        from ccx.store.root import GenerationError
        if live:
            raise SessionUnavailable(
                "Only CCX-managed Claude workers are supported; use a worker handle"
            )
        if max_chars is not None and (type(max_chars) is not int or max_chars<0):
            raise InvalidRequest("max_chars must be nonnegative integer")
        if isinstance(short,(list,tuple)):
            from .errors import CcxError
            out=[]
            errors={}
            for target in short:
                try:
                    out.append(self.report(target,max_chars=max_chars))
                except CcxError as exc:
                    errors[str(target)] = {"error": type(exc).__name__,
                                           "message": str(exc), "exit_code": exc.exit_code}
            return {"reports": out, "errors": errors}
        rec=self._resolve_managed(short,"report")
        wid=rec["workerId"]
        try:
            native=report_worker(f"claude:{wid}",max_chars=max_chars)
        except GenerationError as exc:
            raise SessionUnavailable(
                f"Claude report unavailable; inspect the worker: {exc}"
            ) from exc
        return Report(
            short=wid[:8],worker_id=wid,engine="claude",turn=None,
            text=native["text"],turn_ref=native.get("turn_ref"),
            delivery_id=native.get("delivery_id"),transcript=None,
            note=native.get("note"),
        )

    def context(self, short: str, *, live: bool = False) -> ClaudeContext:
        """Recorded carried context for one owned full UUID.

        `short` is a legacy parameter name, not a short-key target.
        `live=True` and external discovery are refused. No host or
        transcript rendering is performed; absent evidence is no-signal.
        """
        from ccx.claude.context import read_claude_observation

        rec = self._resolve_record(short, live=live)
        owner = self._uuid_store.resolve(rec["workerId"])
        observed = read_claude_observation(self._uuid_store, owner, rec)
        return ClaudeContext.from_record(
            rec, observed.get("context") if observed else None,
            note=observed.get("note") if observed else None,
        )

    def tasks(self, short: str, *, live: bool = False) -> list[ClaudeTask]:
        """Read this owned Claude worker's native TaskCreate/TaskUpdate list.

        Task files belong to the distinct original native session UUID, never
        to the public full worker UUID or its eight-character display prefix.
        """
        from ccx.claude.tasks import read_task_list
        from ccx.store.root import GenerationError
        from ccx.store.identity import InvalidTarget, canonical_worker_id

        if live:
            raise SessionUnavailable("tasks(live=True): external/native Claude discovery unavailable")
        rec = self._resolve_managed(short, "tasks")
        try:
            owner = self._uuid_store.resolve("claude:" + rec["workerId"])
            native = rec.get("native")
            sid = native.get("sessionId") if isinstance(native, dict) else None
            try:
                canonical_worker_id(sid)
            except InvalidTarget as exc:
                raise SessionUnavailable(
                    "tasks: original Claude native session ID is unproven",
                    short=owner.short,
                ) from exc
            fingerprint = rec.get("createSha256")
            if not isinstance(fingerprint, str) or not fingerprint:
                raise SessionUnavailable(
                    "tasks: original Claude create identity is unavailable",
                    short=owner.short,
                )
            bound = self._uuid_store.creation_binding(
                owner.worker, engine="claude", fingerprint=fingerprint,
            )
            if not isinstance(bound, dict) or bound.get("sessionId") != sid:
                raise SessionUnavailable(
                    "tasks: native session differs from original Claude creation",
                    short=owner.short,
                )
            tasks = read_task_list(sid)
            self._uuid_store.revalidate()
            return tasks
        except GenerationError as exc:
            raise SessionUnavailable(f"tasks: original Claude source unavailable: {exc}") from exc

    def _uuid_background_task_facts(self, short: str) -> dict:
        """Resolve an exact original Claude owner for the shared read."""
        from ccx.store.root import GenerationError

        rec = self._resolve_managed(short, "background_tasks")
        try:
            owner = self._uuid_store.resolve("claude:" + rec["workerId"])
        except GenerationError as exc:
            raise SessionUnavailable(
                f"background_tasks: original Claude owner unavailable: {exc}",
            ) from exc
        return self._uuid_background_task_facts_owned(self._uuid_store, owner, rec)

    @staticmethod
    def _uuid_background_task_facts_owned(store, owner, rec: dict) -> dict:
        """Read the verified original JSONL using the caller's pinned owner."""
        from ccx.claude.transcript import open_original_claude_source
        from ccx.store.root import GenerationError

        try:
            with open_original_claude_source(store, owner, rec) as source:
                pinned = source[0] if source is not None else None
                view = {**rec, "transcript": pinned}
                facts = commands_read._background_task_facts(view)
            store.revalidate()
            return facts
        except (GenerationError, OSError) as exc:
            raise SessionUnavailable(
                f"background_tasks: original Claude native source unavailable: {exc}",
            ) from exc

    def background_tasks(self, short: str, *, live: bool = False) -> list[dict]:
        """Transcript-derived Bash, subagent and Monitor facts for one owner.

        An open task is advisory; its liveness never proves turn completion or
        changes the worker lifecycle. A missing original JSONL yields [].
        """
        if live:
            raise SessionUnavailable(
                "background_tasks(live=True): external/native Claude discovery unavailable",
            )
        return self._uuid_background_task_facts(short)["background_tasks"]

    def logs(
        self,
        short: str,
        *,
        tail: int | None = None,
        jsonl: bool = False,
        live: bool = False,
    ) -> dict | str:
        """Read the original owned Claude transcript without native activity.

        Cleaned mode returns the existing rendered document (optionally its
        last ``tail`` turns). ``jsonl=True`` returns the entire original raw
        transcript, including an unfinished final line. Missing transcripts
        are not replaced by a stale derived render.
        """
        from ccx.claude.transcript import open_original_claude_source
        from ccx.store.root import GenerationError

        if live:
            raise InvalidRequest("logs(live=True): unmanaged Claude discovery unavailable")
        rec = self._resolve_managed(short, "logs")
        try:
            owner = self._uuid_store.resolve("claude:" + rec["workerId"])
            if jsonl:
                with open_original_claude_source(self._uuid_store, owner, rec) as source:
                    if source is None:
                        raise SessionNotFound(
                            f"transcript not found for {owner.worker}", short=owner.short,
                        )
                    with source[0].open_reader() as stream:
                        result = stream.read().decode("utf-8", "replace")
            else:
                result = render_transcript_json(owner.worker, store=self._uuid_store)
                if result is None:
                    raise SessionNotFound(
                        f"transcript not found for {owner.worker}", short=owner.short,
                    )
                if tail:
                    result["turns"] = result.get("turns", [])[-tail:]
            self._uuid_store.revalidate()
            return result
        except (GenerationError, OSError) as exc:
            raise SessionUnavailable(
                f"logs: original Claude transcript unavailable: {exc}",
                short=rec["workerId"][:8],
            ) from exc

    def list(
        self,
        *,
        cwd: str | None = None,
        cwd_prefix: str | None = None,
        state: str | None = None,
        recent_hours: float | None = None,
    ) -> list[SessionInfo]:
        """List only managed Claude full UUIDs; no external/native discovery."""
        from ccx.system.cwd_paths import canonical_cwd
        from ccx.store.root import GenerationError
        if cwd is not None and cwd_prefix is not None:
            raise InvalidRequest("cwd and cwd_prefix are mutually exclusive")
        try:
            owned: dict = {}
            rows = self._uuid_store.list_workers(
                engine="claude", cwd=canonical_cwd(cwd) if cwd else None,
                state=state, records=owned,
            )
        except GenerationError as exc:
            raise SessionUnavailable(f"full-UUID worker list unavailable: {exc}") from exc
        if cwd_prefix:
            prefix=canonical_cwd(cwd_prefix)
            rows=[r for r in rows if isinstance(r.get("cwd"),str) and
                  (r["cwd"]==prefix or r["cwd"].startswith(prefix.rstrip(os.sep)+os.sep))]
        if recent_hours is not None:
            import time as _time
            if (not isinstance(recent_hours,(int,float))
                    or not 0<recent_hours<=1000000):
                raise InvalidRequest("recent_hours must be positive")
            floor=int((_time.time()-recent_hours*3600)*1000)
            rows=[r for r in rows if max(r.get("last_turn_at") or 0,
                                         r.get("created_at") or 0)>=floor]
        return [self._uuid_session_info(*owned[row["worker"]], inventory_row=row)
                for row in rows]

    def wait(
        self,
        short: str,
        *,
        live: bool = False,
        next_turn: bool = False,
        timeout: float | None = None,
        after_turn: str | None = None,
        known_input: str | None = None,
    ) -> WaitResult:
        """Observe a full-UUID managed Claude worker without native input.

        A numeric timeout returns a timeout verdict, not an exception. Next-turn
        checkpoints describe exact turn boundaries and input-gate changes; they
        never by themselves prove a completed assistant reply.
        """
        from ccx.ops.wait import wait_worker
        if live:
            raise InvalidRequest("external/live Claude wait is not supported for full UUID")
        rec = self._resolve_managed(short, "wait")
        result = wait_worker(
            "claude:" + rec["workerId"], timeout=timeout, next_turn=next_turn,
            after_turn=after_turn, known_input=known_input,
            expected_generation=self._uuid_store.generation,
        )
        return WaitResult.from_verdict(result)

    def events(
        self,
        short: str,
        *,
        since_epoch: str | None = None,
        since_seq: int | None = None,
        limit: int | None = None,
    ) -> list[Event]:
        """Read original stamped Claude events from the owned full-UUID journal.

        The typed (epoch, seq) cursor spans host restarts; a bare since_seq is
        only a legacy within-epoch filter. Newest limit applies after filtering.
        """
        from ccx.ops.worker_events import MAX_PAGE_EVENTS, read_full_uuid_event_page
        from ccx.store.root import GenerationError
        from ccx.store.identity import UnknownWorker

        record = self._resolve_managed(short, "events")
        worker = f"claude:{record['workerId']}"
        parsed: list[Event] = []
        cursor = None
        try:
            while True:
                page = read_full_uuid_event_page(
                    self._uuid_store, worker, after=cursor,
                    limit=MAX_PAGE_EVENTS, native=True,
                )
                parsed.extend(Event.from_dict(obj) for obj in page["events"])
                if not page["has_more"]:
                    break
                cursor = page["next_cursor"]
        except UnknownWorker as exc:
            raise worker_not_found(worker) from exc
        except GenerationError as exc:
            raise SessionUnavailable(f"events: original source unavailable: {exc}") from exc

        out = filter_events_after(parsed, since_epoch, since_seq)
        if limit is not None and limit >= 0:
            out = out[-limit:]
        return out

    def subscribe(
        self,
        short: str,
        *,
        since_epoch: str | None = None,
        since_seq: int | None = None,
        timeout: float | None = None,
    ) -> Iterator[Event]:
        """Replay typed original Claude events, then follow its owned journal.

        Capture the ev2 source watermark before replay and dedupe overlapping
        historical/live events per epoch, not by a global sequence counter.
        """
        from math import isfinite
        from ccx.ops.worker_events import read_full_uuid_event_page, follow_full_uuid_events
        from ccx.store.root import GenerationError
        from ccx.store.identity import UnknownWorker

        if timeout is not None and (
            type(timeout) not in (int, float) or not isfinite(timeout) or timeout < 0
        ):
            raise InvalidRequest("subscribe timeout must be nonnegative finite or None")
        deadline = None if timeout is None else time.monotonic() + timeout
        record = self._resolve_managed(short, "subscribe")
        worker = f"claude:{record['workerId']}"
        try:
            watermark = read_full_uuid_event_page(
                self._uuid_store, worker, tail=0, native=True,
            )["next_cursor"]
        except UnknownWorker as exc:
            raise worker_not_found(worker) from exc
        except GenerationError as exc:
            raise SessionUnavailable(
                f"subscribe: original source unavailable: {exc}"
            ) from exc

        yielded_max: dict[str, int] = {}

        def fresh(epoch, seq) -> bool:
            if epoch is None or seq is None:
                return True
            if epoch in yielded_max and seq <= yielded_max[epoch]:
                return False
            yielded_max[epoch] = seq
            return True

        for event in self.events(
            worker, since_epoch=since_epoch, since_seq=since_seq,
        ):
            if fresh(event.epoch, event.seq):
                yield event
        remaining = (None if deadline is None
                     else max(0.0, deadline - time.monotonic()))
        try:
            for raw in follow_full_uuid_events(
                self._uuid_store, worker, after=watermark, native=True,
                timeout=remaining, heartbeat=None,
            ):
                # The synthetic heartbeat is not a stamped source event.
                if raw.get("kind") == "heartbeat" and "seq" not in raw:
                    continue
                event = Event.from_dict(raw)
                if fresh(event.epoch, event.seq):
                    yield event
        except UnknownWorker as exc:
            raise worker_not_found(worker) from exc
        except GenerationError as exc:
            raise SessionUnavailable(
                f"subscribe: original source unavailable: {exc}"
            ) from exc

    def usage(self, *, cached: bool = False) -> Usage:
        """Live quota utilization (the signal usage caps enforce). With
        ``cached=True`` serve a recent cached value instead of forcing a fresh
        fetch. Raises :class:`InvalidRequest` if no signal is available; plan data
        comes from native Claude Code unless the machine selected OAuth."""
        data = fetch_usage(force=not cached)
        if data is None:
            raise InvalidRequest("usage: no signal (Claude usage unavailable or held; see ccx usage)")
        return Usage(
            summary=usage_summary(data),
            raw=data,
            five_hour_reset=session_reset_status(data),
        )

    def reset_five_hour_limit(self) -> FiveHourReset:
        """Claim Anthropic's weekly session-limit reset when provider-claimable.

        The provider helper performs a mandatory live eligibility preflight, at
        most one mutation, and a read-only reconciliation. No local utilization
        threshold or entitlement calculation participates in authorization.

        This is Claude Code's own reset offer (a limited rollout), claimed through
        its account endpoint, which isn't a public API and can change. Off by
        default: raises :class:`InvalidRequest` unless
        ``CCX_ENABLE_UNOFFICIAL_RESET=1`` is set.
        """
        outcome = reset_five_hour_limit()
        return FiveHourReset(
            result=str(outcome.get("result") or "error"),
            status=(
                outcome.get("status")
                if isinstance(outcome.get("status"), dict)
                else None
            ),
            next_available_at=(
                outcome.get("next_available_at")
                if isinstance(outcome.get("next_available_at"), str)
                else None
            ),
            weekly_resets_at=(
                outcome.get("weekly_resets_at")
                if isinstance(outcome.get("weekly_resets_at"), str)
                else None
            ),
            refreshed=bool(outcome.get("refreshed")),
        )


__all__ = ["CcxClient"]
