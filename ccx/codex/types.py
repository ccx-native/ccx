"""Frozen return types for the public :class:`ccx.CodexClient`.

The Codex client is intentionally separate from the Claude ``CcxClient``:
Codex has native thread/turn ids, in-turn steering, and no transcript surface.
Every result still follows the same library contract: attribute access plus an
``as_dict()`` projection containing only JSON-friendly values.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any
import time


WINDOW_MINUTES = {"5h": 300, "7d": 10080}


def rate_limit_windows(data: dict):
    """Yield (limit ID, native window) by duration, not primary/secondary rank."""
    by_id = data.get("rateLimitsByLimitId")
    buckets = (by_id.items() if isinstance(by_id, dict) and by_id
               else (("codex", data.get("rateLimits")),))
    for limit_id, bucket in buckets:
        if not isinstance(bucket, dict):
            continue
        for field in ("primary", "secondary"):
            window = bucket.get(field)
            if isinstance(window, dict):
                yield str(limit_id), window


def window_label(window: dict) -> str:
    minutes = window.get("windowDurationMins")
    if type(minutes) is not int or minutes <= 0:
        return "limit"
    if minutes % 1440 == 0:
        return f"{minutes // 1440}d"
    if minutes % 60 == 0:
        return f"{minutes // 60}h"
    return f"{minutes}m"


def rate_limit_window(data: dict, window: str, *, limit_id: str = "codex") -> dict | None:
    """Find an exact-duration window in a named bucket, even if weekly is primary."""
    minutes = WINDOW_MINUTES[window]
    by_id = data.get("rateLimitsByLimitId")
    bucket = by_id.get(limit_id) if isinstance(by_id, dict) else None
    for source in (bucket, data.get("rateLimits")):
        if not isinstance(source, dict):
            continue
        if source.get("limitId") not in (None, limit_id):
            continue
        for field in ("primary", "secondary"):
            candidate = source.get(field)
            if (isinstance(candidate, dict)
                    and candidate.get("windowDurationMins") == minutes):
                return candidate
    return None


def codex_cap_tripped(data: dict | None, caps: dict | None):
    """The first known reached window; missing, malformed or stale data fails open."""
    if not isinstance(data, dict) or not caps:
        return None
    for name, cap in caps.items():
        if name not in WINDOW_MINUTES:
            continue
        value = rate_limit_window(data, name)
        used = value.get("usedPercent") if isinstance(value, dict) else None
        reset = value.get("resetsAt") if isinstance(value, dict) else None
        if type(reset) in (int, float) and reset <= time.time():
            continue
        if type(used) in (int, float) and 0 <= used <= 100 and used >= cap:
            return name, float(used), float(cap)
    return None


def _identity(record: dict) -> dict[str, Any]:
    lifecycle = record.get("lifecycle") or {}
    create_args = record.get("createArgs") or {}
    last_turn = record.get("lastTurn")
    if not isinstance(last_turn, dict):
        last_turn = None
    native = record.get("native")
    native = native if isinstance(native, dict) else {}
    worker_id = record.get("workerId")
    return {
        # The short is for display; derive it from the global full UUID.
        # Never resolve this prefix to a worker authority.
        "short": (worker_id[:8] if isinstance(worker_id, str)
                  else record.get("short")),
        "worker_id": worker_id,
        "thread_id": native.get("threadId") or record.get("threadId"),
        "name": record.get("name"),
        "cwd": record.get("cwd"),
        "state": lifecycle.get("state"),
        "active_turn_id": lifecycle.get("activeTurnId"),
        "model": create_args.get("model"),
        "effort": create_args.get("effort"),
        "sandbox": create_args.get("sandbox"),
        "approval_policy": create_args.get("approvalPolicy"),
        "idle_timeout": create_args.get("idleTimeout"),
        "created_at": record.get("createdAt"),
        "last_turn_at": record.get("lastTurnAt"),
        "last_turn": dict(last_turn) if last_turn is not None else None,
    }


@dataclass(frozen=True)
class CodexSessionInfo:
    """Identity and persisted creation settings for one CCX-owned Codex worker."""

    short: str
    worker_id: str | None = None
    thread_id: str | None = None
    name: str | None = None
    cwd: str | None = None
    state: str | None = None
    active_turn_id: str | None = None
    model: str | None = None
    effort: str | None = None
    sandbox: str | None = None
    approval_policy: str | None = None
    idle_timeout: int | None = None
    created_at: int | None = None
    last_turn_at: int | None = None
    last_turn: dict | None = None
    engine: str = "codex"

    def as_dict(self) -> dict[str, Any]:
        out = asdict(self)
        if self.worker_id is not None:
            out["worker"] = f"codex:{self.worker_id}"
        return out

    @classmethod
    def from_record(cls, record: dict) -> "CodexSessionInfo":
        return cls(**_identity(record))


@dataclass(frozen=True)
class CodexResumeResult(CodexSessionInfo):
    """Result of explicitly ensuring that a durable Codex worker is loaded."""

    resumed: bool = False

    @classmethod
    def from_record(cls, record: dict, *, resumed: bool) -> "CodexResumeResult":
        return cls(**_identity(record), resumed=bool(resumed))


@dataclass(frozen=True)
class CodexTurnResult(CodexSessionInfo):
    """A started or completed Codex turn.

    ``accepted`` means app-server accepted ``turn/start``. With ``wait=False``,
    ``reply`` is ``None`` and the turn normally remains busy. With a waited
    turn, ``reply`` comes from the last completed agent message cached by the
    host. ``status``, ``error``, and the timing fields are the matching native
    turn's outcome and remain ``None`` for an acknowledged but not-yet-settled
    turn. ``item_id`` and ``phase`` describe the selected report; the item ID
    is native to the observation surface that supplied it and may differ from
    a corresponding ``thread/read`` item ID. Use ``turn_id`` for turn
    association across observation surfaces.

    ``client_user_message_id`` is the CCX-owned id sent with this turn's user
    message. Native Codex persists it on the ``userMessage`` item, so it is how
    a cold reader identifies the input CCX started this turn with. It is
    ``None`` for a result that did not start a turn.
    """

    accepted: bool = True
    turn_id: str | None = None
    reply: str | None = None
    item_id: str | None = None
    phase: str | None = None
    status: str | None = None
    error: Any = None
    started_at: int | None = None
    completed_at: int | None = None
    duration_ms: int | float | None = None
    client_user_message_id: str | None = None
    delivery_id: str | None = None

    @classmethod
    def from_record(
        cls,
        record: dict,
        *,
        accepted: bool = True,
        turn_id: str | None = None,
        report: dict | None = None,
        last_turn: dict | None = None,
        client_user_message_id: str | None = None,
        delivery_id: str | None = None,
    ) -> "CodexTurnResult":
        report = report or {}
        raw_last_turn = last_turn
        if not isinstance(raw_last_turn, dict):
            raw_last_turn = record.get("lastTurn")
        if not isinstance(raw_last_turn, dict):
            raw_last_turn = None
        selected_turn_id = turn_id or report.get("turnId")
        if selected_turn_id is None and raw_last_turn is not None:
            selected_turn_id = raw_last_turn.get("turnId")
        outcome = raw_last_turn
        if (
            selected_turn_id is not None
            and outcome is not None
            and outcome.get("turnId") != selected_turn_id
        ):
            # The record may still expose a previous turn while a no-wait
            # result identifies a newly accepted one. Do not attach that old
            # outcome to the new result.
            outcome = None
        report_matches = (
            selected_turn_id is None
            or report.get("turnId") == selected_turn_id
        )
        selected_report = report if report_matches else {}
        identity_record = {**record, "lastTurn": raw_last_turn}
        return cls(
            **_identity(identity_record),
            accepted=accepted,
            turn_id=selected_turn_id,
            reply=selected_report.get("text"),
            item_id=selected_report.get("itemId"),
            phase=selected_report.get("phase"),
            status=outcome.get("status") if outcome else None,
            error=outcome.get("error") if outcome else None,
            started_at=outcome.get("startedAtMs") if outcome else None,
            completed_at=(
                outcome.get("completedAtMs")
                if outcome
                else selected_report.get("completedAt")
            ),
            duration_ms=outcome.get("durationMs") if outcome else None,
            client_user_message_id=client_user_message_id,
            delivery_id=delivery_id,
        )


@dataclass(frozen=True)
class CodexReconcileResult(CodexSessionInfo):
    """Read-only verdict for one caller-known delivery or native turn."""

    delivery_id: str | None = None
    status: str = "not_found"
    pending: bool = False
    turn_id: str | None = None
    client_user_message_id: str | None = None
    rejection_code: str | None = None
    rejection_message: str | None = None
    # Set with status="not_delivered": why CCX refused the delivery before
    # submitting it. Sending again under the same delivery id is safe.
    refusal_code: str | None = None
    refusal_message: str | None = None

    @property
    def retryable(self) -> bool:
        return self.status == "not_delivered"

    @classmethod
    def from_operation(cls, worker: dict, delivery: dict | None, status: str) -> "CodexReconcileResult":
        delivery = delivery or {}
        refusal = delivery.get("last_refusal") or {}
        return cls(
            **_identity(worker),
            delivery_id=delivery.get("delivery_id"),
            status=status,
            pending=bool(delivery.get("pending")) and status != "not_delivered",
            turn_id=delivery.get("turn_id"),
            client_user_message_id=delivery.get("client_user_message_id"),
            rejection_code=delivery.get("rejection_code"),
            rejection_message=delivery.get("rejection_message"),
            refusal_code=refusal.get("code") if status == "not_delivered" else None,
            refusal_message=(
                refusal.get("message") if status == "not_delivered" else None
            ),
        )


@dataclass(frozen=True)
class CodexTelemetryPage:
    """Normalized replay page for one Codex worker's captured telemetry."""

    short: str
    thread_id: str | None = None
    name: str | None = None
    turns: list[dict] = field(default_factory=list)
    compactions: list[dict] = field(default_factory=list)
    coverage: dict = field(default_factory=dict)
    next_cursor: str | None = None
    has_more: bool = False
    engine: str = "codex"

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_operation(cls, result: dict[str, Any]) -> "CodexTelemetryPage":
        worker = result.get("worker")
        worker = worker if isinstance(worker, dict) else {}
        return cls(
            short=str(worker.get("short") or ""),
            thread_id=worker.get("thread_id"),
            name=worker.get("name"),
            turns=list(result.get("turns") or []),
            compactions=list(result.get("compactions") or []),
            coverage=dict(result.get("coverage") or {}),
            next_cursor=result.get("next_cursor"),
            has_more=bool(result.get("has_more")),
        )


@dataclass(frozen=True)
class CodexControlResult(CodexSessionInfo):
    """Acknowledgement for ``steer`` or ``interrupt``.

    ``client_user_message_id`` is set for ``steer``, whose input becomes its own
    native user message, and stays ``None`` for ``interrupt``, which sends no
    user message at all. A steer id is never a start id: they are minted
    separately and carry different origins.
    """

    action: str = ""
    accepted: bool = True
    turn_id: str | None = None
    client_user_message_id: str | None = None
    delivery_id: str | None = None

    @classmethod
    def from_record(
        cls,
        record: dict,
        *,
        action: str,
        turn_id: str | None = None,
        client_user_message_id: str | None = None,
        delivery_id: str | None = None,
    ) -> "CodexControlResult":
        return cls(
            **_identity(record),
            action=action,
            accepted=True,
            turn_id=turn_id,
            client_user_message_id=client_user_message_id,
            delivery_id=delivery_id,
        )


@dataclass(frozen=True)
class CodexWaitResult(CodexSessionInfo):
    """Stable-state verdict from :meth:`CodexClient.wait`.

    ``evidence`` is the worker's raw evidence-journal health block, or ``None``
    for a worker that has produced no evidence. It is deliberately independent
    of ``status``: a healthy idle worker whose journal degraded stays idle, and
    a poller sees the degradation without the worker looking broken.
    """

    status: str = ""
    timed_out: bool = False
    last_settled_turn: dict | None = None
    active_turn_ref: str | None = None
    pending_approvals: list[dict] = field(default_factory=list)
    report: dict | None = None
    active_turn_error: dict | None = None
    error: str | None = None
    turn_id: str | None = None
    turn_status: str | None = None
    turn_error: Any = None
    turn_started_at: int | None = None
    turn_completed_at: int | None = None
    turn_duration_ms: int | float | None = None
    evidence: dict | None = None
    settle: bool = False
    settlement_verdict: str | None = None
    settled: bool | None = None
    outstanding_count: int = 0
    outstanding: list[dict] = field(default_factory=list)
    outstanding_truncated: bool = False
    pending_wake_count: int = 0
    pending_wake: list[dict] = field(default_factory=list)
    pending_wake_truncated: bool = False
    registration_count: int = 0
    wake_grace_seconds: float | None = None
    last_message: str | None = None

    @property
    def is_waiting(self) -> bool:
        return self.status == "waiting"

    @classmethod
    def from_result(cls, record: dict, result: dict) -> "CodexWaitResult":
        state = result.get("state") or (record.get("lifecycle") or {}).get("state")
        timed_out = result.get("timed_out") is True or (
            not result.get("ok", False) and result.get("error") == "wait timed out"
        )
        last_turn = result.get("lastTurn")
        if not isinstance(last_turn, dict):
            last_turn = record.get("lastTurn")
        if not isinstance(last_turn, dict):
            last_turn = None
        if "activeTurnError" in result:
            active_turn_error = result["activeTurnError"]
        elif "active_turn_error" in result:
            active_turn_error = result["active_turn_error"]
        else:
            active_turn_error = record.get("activeTurnError")
        active_turn = (record.get("lifecycle") or {}).get("activeTurnId")
        for key in ("activeTurnId", "active_turn_id", "active_turn_ref"):
            if key in result:
                active_turn = result[key]
                if key == "active_turn_ref" and active_turn is not None:
                    active_turn = active_turn.removeprefix("ccx-turn:codex:v1:")
                break
        approvals = record.get("pendingApprovals")
        for key in ("pendingApprovals", "pending_approvals"):
            if key in result:
                approvals = result[key]
                break
        identity_record = {**record, "lastTurn": last_turn}
        return cls(
            **_identity({**identity_record, "lifecycle": {
                **(record.get("lifecycle") or {}),
                "state": state,
                "activeTurnId": active_turn,
            }}),
            status=str(result.get("status") or ("timeout" if timed_out else state or "unknown")),
            timed_out=timed_out,
            last_settled_turn=result.get("last_settled_turn"),
            active_turn_ref=result.get("active_turn_ref"),
            pending_approvals=list(approvals or []),
            # A general native wait is worker stability, not a reply request.
            # Do not lift an older cached report into this wait's answer.
            report=(None if "worker" in result else
                    result.get("lastReport") or result.get("report")
                    or record.get("lastReport")),
            active_turn_error=active_turn_error,
            error=result.get("error"),
            turn_id=last_turn.get("turnId") if last_turn else None,
            turn_status=last_turn.get("status") if last_turn else None,
            turn_error=last_turn.get("error") if last_turn else None,
            turn_started_at=last_turn.get("startedAtMs") if last_turn else None,
            turn_completed_at=last_turn.get("completedAtMs") if last_turn else None,
            turn_duration_ms=last_turn.get("durationMs") if last_turn else None,
            evidence=(
                result.get("evidence")
                if isinstance(result.get("evidence"), dict)
                else (
                    record.get("evidence")
                    if isinstance(record.get("evidence"), dict) else None
                )
            ),
            settle=bool(result.get("settle")),
            settlement_verdict=(
                result.get("settlementVerdict")
                or result.get("settlement_verdict")
            ),
            settled=(result.get("settled") if result.get("settle") else None),
            outstanding_count=int(
                result.get("outstandingCount")
                or result.get("outstanding_count")
                or 0
            ),
            outstanding=list(result.get("outstanding") or []),
            outstanding_truncated=bool(
                result.get("outstandingTruncated")
                or result.get("outstanding_truncated")
            ),
            pending_wake_count=int(
                result.get("pendingWakeCount")
                or result.get("pending_wake_count")
                or 0
            ),
            pending_wake=list(
                result.get("pendingWake")
                or result.get("pending_wake")
                or []
            ),
            pending_wake_truncated=bool(
                result.get("pendingWakeTruncated")
                or result.get("pending_wake_truncated")
            ),
            registration_count=int(
                result.get("registrationCount")
                or result.get("registration_count")
                or 0
            ),
            wake_grace_seconds=(
                result.get("wakeGraceSeconds")
                if result.get("wakeGraceSeconds") is not None
                else result.get("wake_grace_seconds")
            ),
            last_message=(
                result.get("lastMessage")
                if result.get("lastMessage") is not None
                else result.get("last_message")
            ),
        )


@dataclass(frozen=True)
class CodexReport(CodexSessionInfo):
    """The latest completed Codex agent message.

    ``item_id`` is the native ID from the observation surface that supplied
    this report. It is evidence about that source item, not a durable join key
    across live notifications and reconstructed ``thread/read`` history;
    ``turn_id`` is the authoritative cross-surface identity.
    """

    turn_id: str | None = None
    item_id: str | None = None
    phase: str | None = None
    text: str = ""
    completed_at: int | None = None
    # Set when a newer turn is running or ended without a reply.
    note: str | None = None

    @classmethod
    def from_record(cls, record: dict, report: dict) -> "CodexReport":
        return cls(
            **_identity(record),
            turn_id=report.get("turnId"),
            item_id=report.get("itemId"),
            phase=report.get("phase"),
            text=str(report.get("text") or ""),
            completed_at=report.get("completedAt"),
            note=report.get("note"),
        )


@dataclass(frozen=True)
class CodexApprovalResult(CodexSessionInfo):
    """Acknowledgement for one connection-scoped approval decision."""

    request_id: str | int | None = None
    decision: str = ""

    @classmethod
    def from_record(
        cls, record: dict, *, request_id: str | int | None, decision: str
    ) -> "CodexApprovalResult":
        return cls(
            **_identity(record),
            request_id=request_id,
            decision=decision,
        )


@dataclass(frozen=True)
class CodexContext(CodexSessionInfo):
    """Latest carried input tokens and model context-window size, the shape
    ``ccx context codex:TARGET --json`` emits. ``state`` is the worker lifecycle;
    ``reason`` is ``None`` when ``tokens_used`` is a measurement,
    ``"compacting"`` while a compaction runs (the older measurement is
    withheld) and ``"no-signal"`` otherwise. ``as_of_turn_ref`` identifies settled CCX turn context;
    live context has a null turn reference."""

    tokens_used: int | None = None
    window_size: int | None = None
    percent_used: float | None = None
    as_of_turn_ref: str | None = None
    as_of_ms: int | None = None
    reason: str | None = None

    @classmethod
    def from_record(cls, record: dict) -> "CodexContext":
        from ccx.claude.context import current_context, public_context
        return cls(
            **_identity(record),
            **public_context(
                current_context(record, engine="codex", live=record.get("context")), engine="codex",
                compacting=isinstance(record.get("activeCompact"), dict),
            ),
        )


@dataclass(frozen=True)
class CodexUsage:
    """Live Codex account quota utilization or token-activity payload."""

    raw: dict = field(default_factory=dict)
    kind: str = "rate_limits"
    engine: str = "codex"

    def as_dict(self) -> dict[str, Any]:
        return {
            "engine": self.engine,
            "kind": self.kind,
            "raw": dict(self.raw),
        }

    def utilization(
        self,
        *,
        limit_id: str = "codex",
        window: str = "5h",
    ) -> int | None:
        """Return a quota window's used percentage when present."""
        if window not in WINDOW_MINUTES:
            raise ValueError("Codex window must be '5h' or '7d'")
        value = rate_limit_window(self.raw, window, limit_id=limit_id)
        used = value.get("usedPercent") if isinstance(value, dict) else None
        return used if type(used) in (int, float) and 0 <= used <= 100 else None


@dataclass(frozen=True)
class CodexCompactResult(CodexSessionInfo):
    """One evidenced native Codex compact, not an idle or an admission ACK."""

    compacted: bool = False
    context_before: dict | None = None
    context_after: dict | None = None
    operation_id: str | None = None
    operation_status: str | None = None
    compact_turn_id: str | None = None
    reason: str | None = None

    @classmethod
    def from_record(
        cls,
        record: dict,
        *,
        compacted: bool,
        context_before: dict | None,
        context_after: dict | None,
        operation_id: str | None = None,
        operation_status: str | None = None,
        compact_turn_id: str | None = None,
        reason: str | None = None,
    ) -> "CodexCompactResult":
        return cls(
            **_identity(record),
            compacted=compacted,
            context_before=context_before,
            context_after=context_after,
            operation_id=operation_id,
            operation_status=operation_status,
            compact_turn_id=compact_turn_id,
            reason=reason,
        )


__all__ = [
    "CodexApprovalResult",
    "CodexCompactResult",
    "CodexContext",
    "CodexControlResult",
    "CodexReport",
    "CodexSessionInfo",
    "CodexTelemetryPage",
    "CodexTurnResult",
    "CodexUsage",
    "CodexWaitResult",
]
