"""Typed exceptions for the public CCX Python clients.

The CLI signals failure by writing to stderr and calling ``sys.exit`` with one
of the integer codes named in :mod:`ccx.constants`. A library cannot do that —
exiting the host process from inside a function call is hostile to anything that
imports it. So every public client method that its CLI would have exited from
raises one of the exceptions below instead.

Each exception carries the CLI ``exit_code`` it corresponds to, so an
orchestrator that wants the old numeric semantics (or a thin CLI adapter) can
recover them without re-deriving the mapping:

    try:
        client.send(short, "go")
    except CcxError as e:
        sys.exit(e.exit_code)

The mapping mirrors the exit-code table in ``ccx/constants.py`` exactly; see
that file for what each code means.
"""

from __future__ import annotations

from .constants import (
    BUDGET_EXIT,
    EXIT_ERROR,
    EXIT_NOT_FOUND,
    EXIT_API_TRANSIENT,
    EXIT_RATE_LIMITED,
    EXIT_REJECTED,
    EXIT_TURN_INCOMPLETE,
    EXIT_UNAVAILABLE,
    EXIT_WAITING,
)


class CcxError(Exception):
    """Base class for every error the public client raises.

    ``exit_code`` is the CLI exit code this condition maps to (see
    ``ccx/constants.py``). ``short`` is the session id involved, when known.
    Subclasses may attach extra structured context (e.g. ``waiting_for`` on
    :class:`SessionWaiting`) so callers never have to parse the message string.
    """

    exit_code: int = EXIT_ERROR

    def __init__(self, message: str = "", *, short: str | None = None):
        super().__init__(message)
        self.short = short


class InvalidRequest(CcxError):
    """A call's arguments were invalid or unusable (bad prompt source, a usage
    cap with no prompt to attach to, an unreadable cwd, …). Maps to the CLI's
    general-failure code."""

    exit_code = EXIT_ERROR


class AmbiguousWorker(InvalidRequest):
    """Discovery matched multiple exact managed owners; no target was chosen."""

    def __init__(self, query: str, candidates: tuple[str, ...] | list[str]):
        self.candidates = tuple(sorted(candidates))
        assert len(self.candidates) >= 2
        super().__init__(
            f"ambiguous managed worker prefix {query!r}; candidates: "
            + ", ".join(self.candidates)
            + "; use a longer canonical prefix or full UUID"
        )


class OperationConflict(InvalidRequest):
    """An immutable caller-owned identity conflicts with durable ownership."""

    exit_code = EXIT_REJECTED

    def __init__(self, message="", *, short=None, reason=None, worker_id=None):
        super().__init__(message, short=short)
        self.reason = reason
        self.worker_id = worker_id


class CreatePartialFailure(CcxError):
    """A durable worker identity is claimed, but creation is incomplete.

    In phase="record", the claim may exist without a usable worker record
    or native activation. Later phases may follow a live native worker. In
    either case the caller must retain and reconcile the full worker_id.
    """

    exit_code = EXIT_UNAVAILABLE

    def __init__(self, message="", *, info, phase, delivery_id=None,
                 delivery_status=None, cause=None, notification_intent=False):
        super().__init__(message, short=info["short"])
        self.worker_id = info["worker_id"]
        self.worker = info["worker"]
        self.engine = info["engine"]
        self.phase = phase
        self.delivery_id = delivery_id
        self.delivery_status = delivery_status
        self.cause = cause
        self.notification_intent = notification_intent
        self.info = info
        if isinstance(cause, CcxError):
            self.exit_code = cause.exit_code


class CursorInvalidated(InvalidRequest):
    """An opaque paging cursor no longer belongs to the current native source."""

    exit_code = EXIT_REJECTED


class SessionNotFound(CcxError):
    """No session (managed or discoverable external) matches the identifier, or
    a requested transcript/reply does not exist yet."""

    exit_code = EXIT_NOT_FOUND


def worker_not_found(target: str) -> SessionNotFound:
    """One public error for an absent owned worker or missing CLI prefix."""
    return SessionNotFound(f"no such worker: {target}")


class PayloadTooLarge(InvalidRequest):
    """A send()'s prompt text exceeds the paste-channel payload bound.

    Managed delivery drains the complete bracketed paste through a bounded PTY
    write path. Operators may still configure ``CCX_SEND_MAX_BYTES`` (unlimited
    by default) to reject payloads above their own resource policy.

    ``size`` is the offending UTF-8 byte length; ``limit`` the enforced bound.
    """

    def __init__(self, message: str = "", *, short: str | None = None,
                 size: int | None = None, limit: int | None = None):
        super().__init__(message, short=short)
        self.size = size
        self.limit = limit


class CapabilityUnsupported(InvalidRequest):
    """An engine cannot honor a requested option, and the request is refused
    rather than silently downgraded.

    Raised only by :class:`ccx.WorkerClient`, whose contract is that an
    unsupported delivery mode, quota budget, or usage projection is an error —
    never a quiet no-op that runs with different semantics than asked for.
    ``engine`` and ``capability`` name the offending pair so a caller can branch
    on them without parsing the message. See ``WorkerClient.capabilities()``.
    """

    def __init__(self, message: str = "", *, short: str | None = None,
                 engine: str | None = None, capability: str | None = None):
        super().__init__(message, short=short)
        self.engine = engine
        self.capability = capability


class ProviderProtocolError(CcxError):
    """A provider/runtime response violated a contract CCX needs for correctness.

    Retrying the same request/cursor is not expected to repair the response.
    Protocol V1 maps this to ``provider_protocol_error`` rather than presenting
    the condition as caller input or transient runtime availability.
    """

    exit_code = EXIT_ERROR


class OperationTimeout(CcxError):
    """A bounded operation exhausted its caller-owned deadline.

    Unlike :class:`TurnIncomplete`, this does not imply that a model turn was
    running or failed to settle; it is for read/control operations whose own
    end-to-end deadline elapsed. V1 maps it to the stable ``timeout`` code.
    """

    exit_code = EXIT_TURN_INCOMPLETE


class SessionUnavailable(CcxError):
    """The session exists but cannot service this operation in its current state
    — wrong lifecycle state, host not alive, an external session that is
    read-only for this verb, or a failed respawn. The most overloaded code."""

    exit_code = EXIT_UNAVAILABLE


class EngineCliNotFound(SessionUnavailable):
    """An engine's configured native executable cannot run; exit 3."""


class SpawnFailed(SessionUnavailable):
    "The detached host launcher failed hard — it never handed off to a host\n    process (could not run, crashed, or produced no verdict). Distinct from a\n    slow boot: a host whose socket is merely late does NOT raise this. ``phase``\n    is the launcher verdict phase (see :mod:`ccx.claude.spawn`) and ``detail`` carries\n    the launcher's error text when present."

    def __init__(self, message: str = "", *, short: str | None = None,
                 phase: str | None = None, detail: str | None = None):
        super().__init__(message, short=short)
        self.phase = phase
        self.detail = detail


class SessionWaiting(CcxError):
    """A turn ended (or a wait resolved) with the session blocked on input — an
    AskUserQuestion dialog or a permission prompt. ``waiting_for`` carries the
    live pending-input preview (the raw host dict) when one is available;
    ``pending_input`` is the same thing as a typed
    :class:`~ccx.types.PendingInput`."""

    exit_code = EXIT_WAITING

    def __init__(self, message: str = "", *, short: str | None = None,
                 waiting_for: dict | None = None):
        super().__init__(message, short=short)
        self.waiting_for = waiting_for

    @property
    def pending_input(self):
        """Typed view of ``waiting_for`` (:class:`~ccx.types.PendingInput`),
        or None when no preview was available."""
        from .types import PendingInput  # lazy: keep errors dependency-free
        return PendingInput.from_waiting_for(self.waiting_for)


class DeliveryRejected(CcxError):
    """The host/daemon refused to accept the message or decision (queue full,
    wrong state, unreachable socket). ``reason`` and ``state`` echo the host's
    rejection when present.

    ``refusal`` is the host's STRUCTURED refusal code when it sent one
    (``busy``, ``waiting``, ``input-surface``, ``steer-pending``). Branch on it
    rather than on the prose in ``reason``: only the code distinguishes a
    worker blocked on a question (answer/approve it) from one holding an
    unresolved durable steer (reconcile it, never resend).

    ``native_refusal`` is the engine's own code. It equals ``refusal`` except
    where :class:`~ccx.workers.WorkerClient` normalized a busy refusal (Codex
    ``worker-state``) to ``busy``."""

    exit_code = EXIT_REJECTED

    def __init__(self, message: str = "", *, short: str | None = None,
                 reason: str | None = None, state: str | None = None,
                 refusal: str | None = None, delivery_id: str | None = None,
                 delivery_status: str | None = None, operation_id: str | None = None,
                 native_refusal: str | None = None):
        super().__init__(message, short=short)
        self.reason = reason
        self.state = state
        self.refusal = refusal
        self.native_refusal = native_refusal if native_refusal is not None else refusal
        self.delivery_id = delivery_id
        self.delivery_status = delivery_status
        self.operation_id = operation_id


class TurnIncomplete(CcxError):
    """A blocking call's turn did not finish cleanly — it never reached IDLE
    before the timeout, or the session moved to a crashed/dead/idle-killed
    state mid-turn. ``state`` is the final observed state. Codex turn failures
    also carry the native turn identity, status, and structured error when
    those facts are available."""

    exit_code = EXIT_TURN_INCOMPLETE

    def __init__(self, message: str = "", *, short: str | None = None,
                 state: str | None = None, turn_id: str | None = None,
                 turn_status: str | None = None, turn_error=None):
        super().__init__(message, short=short)
        self.state = state
        self.turn_id = turn_id
        self.turn_status = turn_status
        self.turn_error = turn_error


class SendIndeterminate(TurnIncomplete):
    """A Codex turn may have been accepted and must be reconciled, never resent.

    The caller-known delivery_id remains the durable key. delivery_status is
    the last durable CCX receipt state, normally prepared or submitted.
    """

    def __init__(
        self,
        message: str = "",
        *,
        short: str | None = None,
        state: str | None = None,
        delivery_id: str | None = None,
        delivery_status: str | None = None,
    ):
        super().__init__(message, short=short, state=state)
        self.delivery_id = delivery_id
        self.delivery_status = delivery_status


class CompactFailed(TurnIncomplete):
    """Claude reported that a submitted compaction did not happen.

    A *proven* terminal failure, and the deliberate opposite of
    :class:`CompactIndeterminate`: there the evidence was missing, here Claude
    wrote the verdict itself. The session is unharmed — compaction owns no turn
    of its own, so the worker stays idle and healthy and the conversation is
    simply un-compacted. Claude Code does not retry compaction, so re-issuing
    ``compact()`` is the caller's move.

    ``api_error`` carries the :class:`WorkerApiError` when the cause was an API
    failure, and is ``None`` for failures owned by the conversation itself (it
    could not be reduced below the context limit, unstrippable media).
    """

    def __init__(self, message: str = "", *, short: str | None = None,
                 state: str | None = None, operation_id: str | None = None,
                 api_error: "WorkerApiError | None" = None):
        super().__init__(message, short=short, state=state)
        self.operation_id = operation_id
        self.api_error = api_error


class CompactIndeterminate(TurnIncomplete):
    """A native compact was accepted but CCX cannot prove its terminal result.

    This is deliberately distinct from a confirmed compact failure.  The
    operation may still reconcile to success from durable transcript evidence;
    ``operation_id`` lets an orchestrator retain and diagnose that identity
    without parsing text.
    """

    def __init__(self, message: str = "", *, short: str | None = None,
                 state: str | None = None, operation_id: str | None = None,
                 operation_status: str | None = None):
        super().__init__(message, short=short, state=state)
        self.operation_id = operation_id
        self.operation_status = operation_status


class SteerIndeterminate(TurnIncomplete):
    """A steer's delivery cannot be established or proven.

    Raised when durable acceptance itself could not be confirmed (so the caller
    must reconcile with the SAME ``delivery_id`` rather than retry with a fresh
    one), or when an accepted delivery resolved to durable ``unknown``. It is
    never raised merely because an observation window elapsed: a still-pending
    delivery is a successful, honest outcome.
    """

    def __init__(self, message: str = "", *, short: str | None = None,
                 state: str | None = None, delivery_id: str | None = None,
                 delivery_status: str | None = None):
        super().__init__(message, short=short, state=state)
        self.delivery_id = delivery_id
        self.delivery_status = delivery_status


class BudgetStopped(CcxError):
    """A usage cap hard-stopped the run. ``reason`` is the recorded stop reason.
    The session is still inspectable and a fresh (re-capped or uncapped) send
    will revive it."""

    exit_code = BUDGET_EXIT

    def __init__(self, message: str = "", *, short: str | None = None,
                 reason: str | None = None, phase: str | None = None):
        super().__init__(message, short=short)
        self.reason = reason
        self.phase = phase


class WorkerApiError(SessionUnavailable):
    """A turn ended because the Claude API returned a terminal error.

    **The catch-all parent, and never raised on its own.** Every failure is one
    of the three concrete subclasses below, each with the exit code its remedy
    deserves. Catch this class when you only need "the API killed this"; catch a
    subclass when you will act differently. If you ever *do* see this exact type,
    it came from hand-construction, not from CCX.

    CCX does **not** retry on your behalf, and neither should you immediately:
    Claude Code has already spent its own retry budget — roughly ten attempts
    with exponential backoff, honoring ``retry-after`` — before the error is
    ever written to the transcript. An instant retry re-runs a call that just
    failed ten times.

    The fields are Claude's own classification, so a caller never has to match
    on prose:

    ``kind``
        Claude's error class: ``server_error``, ``authentication_failed``,
        ``invalid_request``, ``rate_limit``, ...
    ``status``
        HTTP status, or ``None`` for failures that have none — notably a stream
        that dies after a successful 200.
    ``transient``
        The failure was a property of the server or connection at that instant
        rather than of the request, so the identical call could succeed later.
        Informational: it says whether coming back later is meaningful at all,
        not that CCX will do it for you.
    ``request_id``
        Anthropic's request id, for support tickets.
    ``message``
        Claude's own message, verbatim. ``str(exc)`` is the composed CCX line
        (``Claude API <label> (<status>): <that message>``); this attribute is
        the upstream half of it on its own.
    """

    def __init__(self, message: str = "", *, short: str | None = None,
                 kind: str | None = None, status: int | None = None,
                 transient: bool = False, request_id: str | None = None,
                 detail: str | None = None):
        super().__init__(message, short=short)
        self.kind = kind
        self.status = status
        self.transient = transient
        self.request_id = request_id
        self.message = detail


class WorkerRateLimited(WorkerApiError):
    """Anthropic rate-limited the account (HTTP 429).

    Distinct from :class:`BudgetStopped`, which is CCX's *own* usage cap: this
    limit is imposed upstream and clears on a reset the caller does not control.
    Conflating them would hide the one condition no orchestrator should miss.

    It is also deliberately not folded into ``transient``. Both flags can be
    set at once — Claude Code marks a 429 that is a capacity throttle as
    transient, while a quota wall is not — so ``transient and rate_limited``
    means "throttled, worth retrying soon" and a bare rate limit means "wait
    for the reset".

    ``reset_text`` is the reset wording lifted verbatim from Claude's message
    (e.g. ``"resets 8:40pm (Europe/Madrid)"``), or ``None`` when it carried
    none. It is deliberately NOT parsed into a timestamp: the format is human
    prose, not a contract, and a parser would fail silently when it drifts. Use
    ``usage()`` when you need structured quota data.
    """

    exit_code = EXIT_RATE_LIMITED

    def __init__(self, message: str = "", *, short: str | None = None,
                 kind: str | None = None, status: int | None = None,
                 transient: bool = False, request_id: str | None = None,
                 detail: str | None = None, reset_text: str | None = None):
        super().__init__(message, short=short, kind=kind, status=status,
                         transient=transient, request_id=request_id,
                         detail=detail)
        self.reset_text = reset_text


class WorkerApiPermanent(WorkerApiError):
    """The request itself, the account, or the configuration is wrong.

    Expired auth, a billing problem, a model you cannot reach, a malformed
    request: the identical call will fail identically, so the remedy is to fix
    something or stop — never to retry. Exit 3, the session-unavailable code,
    because that is exactly the situation.

    Spelled out as its own class rather than left as "the bare parent" so that
    ``type(e) is WorkerApiError`` never has to silently mean "permanent".
    """


class WorkerApiTransient(WorkerApiError):
    """Anthropic could not serve the request right now, but could later.

    Covers everything CCX classifies as transient: a 529 (the canonical case —
    Anthropic at capacity), a 5xx, or a stream that died mid-response. They
    share the only property that matters to a caller — the identical call may
    succeed later, unchanged.

    A 529 is capacity, **not** your quota, and costs nothing against it. That is
    what separates this from :class:`WorkerRateLimited`: here you wait minutes
    and retry, there you wait for a reset you do not control.

    CCX still does not retry for you. Claude Code has already spent its own
    retry budget before this reaches you, so back off meaningfully rather than
    looping immediately.
    """

    exit_code = EXIT_API_TRANSIENT


def build_worker_api_error(
    message: str, *, short: str | None = None, kind: str | None = None,
    status: int | None = None, transient: bool = False,
    rate_limited: bool = False, request_id: str | None = None,
    detail: str | None = None, reset_text: str | None = None,
) -> WorkerApiError:
    """Construct the most specific API-error class the classification supports.

    Single source of the class-selection rule. Every surface that rebuilds one
    of these — lifecycle records, compact journals, controller replay — goes
    through here, so they cannot drift into disagreeing about which condition
    maps to which class and exit code.

    Rate limiting wins over transience when both are set: Claude Code marks a
    capacity-throttle 429 as transient, but it is still a rate limit, and that
    is the one an orchestrator must not miss.

    Always returns a concrete subclass; :class:`WorkerApiError` itself is only
    ever the catch-all, never a verdict.
    """
    common = {
        "short": short, "kind": kind, "status": status,
        "transient": transient, "request_id": request_id, "detail": detail,
    }
    if rate_limited:
        return WorkerRateLimited(message, reset_text=reset_text, **common)
    if transient:
        return WorkerApiTransient(message, **common)
    return WorkerApiPermanent(message, **common)


def api_error_from_lifecycle(
    lifecycle: dict | None, *, short: str | None = None,
) -> WorkerApiError | None:
    """Rebuild the typed error from a record's ``lifecycle.apiError`` block.

    Returns ``None`` when the session did not stop on an API error, so callers
    can fall through to their existing generic handling.
    """
    payload = (lifecycle or {}).get("apiError")
    if not isinstance(payload, dict):
        return None
    reason = (lifecycle or {}).get("lastError") or payload.get("message") or ""
    return build_worker_api_error(
        reason,
        short=short,
        kind=payload.get("kind"),
        status=payload.get("status"),
        transient=bool(payload.get("transient")),
        rate_limited=bool(payload.get("rate_limited")),
        request_id=payload.get("request_id"),
        detail=payload.get("message"),
        reset_text=payload.get("reset_text"),
    )


class ControllerError(CcxError):
    """Base error for the durable Python controller runtime."""


class ControllerRunNotFound(ControllerError):
    """No controller run matches the name or run-id prefix."""

    exit_code = EXIT_NOT_FOUND


class ControllerLaunchRequired(ControllerError):
    """A decorated controller was invoked outside ``ccx controller run``."""


class ControllerReplayMismatch(ControllerError):
    """Persisted replay identity no longer matches the controller invocation."""


class ControllerLiveStateLost(ControllerError):
    """An accepted worker operation mutated live Python state from a prior attempt.

    CCX will not redeliver the already-accepted worker turn, and arbitrary live
    object identity is intentionally not reconstructed across controller process
    death. The controller must complete any branch that consumes live variables
    in the same successful task/attempt and return a durable JSON-compatible
    result for later replay.
    """

    def __init__(self, message: str = "", *, operation_id: str | None = None):
        super().__init__(message)
        self.operation_id = operation_id


class ControllerOperationIndeterminate(ControllerError):
    """A worker mutation may have landed but cannot be safely replayed."""

    def __init__(self, message: str = "", *, operation_id: str | None = None):
        super().__init__(message)
        self.operation_id = operation_id




# Exit code → exception class, for a thin CLI adapter or tests. Only the codes a
# library call can raise are listed; EXIT_ERROR is the CcxError default.
EXIT_CODE_TO_ERROR: dict[int, type[CcxError]] = {
    EXIT_ERROR: InvalidRequest,
    EXIT_NOT_FOUND: SessionNotFound,
    EXIT_UNAVAILABLE: SessionUnavailable,
    EXIT_WAITING: SessionWaiting,
    EXIT_REJECTED: DeliveryRejected,
    EXIT_TURN_INCOMPLETE: TurnIncomplete,
    BUDGET_EXIT: BudgetStopped,
    EXIT_RATE_LIMITED: WorkerRateLimited,
    EXIT_API_TRANSIENT: WorkerApiTransient,
}


__all__ = [
    "CcxError",
    "InvalidRequest",
    "OperationConflict",
    "CursorInvalidated",
    "ProviderProtocolError",
    "OperationTimeout",
    "SessionNotFound",
    "PayloadTooLarge",
    "CapabilityUnsupported",
    "SessionUnavailable",
    "SpawnFailed",
    "SessionWaiting",
    "DeliveryRejected",
    "TurnIncomplete",
    "CompactFailed",
    "CompactIndeterminate",
    "SteerIndeterminate",
    "SendIndeterminate",
    "BudgetStopped",
    "WorkerApiError",
    "WorkerApiPermanent",
    "WorkerApiTransient",
    "WorkerRateLimited",
    "api_error_from_lifecycle",
    "build_worker_api_error",
    "ControllerError",
    "ControllerLaunchRequired",
    "ControllerReplayMismatch",
    "ControllerLiveStateLost",
    "ControllerOperationIndeterminate",
    "EXIT_CODE_TO_ERROR",
]
