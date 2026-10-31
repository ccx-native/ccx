"""Typed return shapes for the public :mod:`ccx.client` API.

Every value the client hands back is a frozen dataclass with an ``as_dict()``
method, so callers get attribute access (``status.state``) *and* a plain-``dict``
projection that round-trips through ``json.dumps`` or feeds a Pydantic model
unchanged. The dict shapes intentionally mirror the JSON the corresponding
``--json`` CLI surfaces already emit, so existing consumers keep working when
they move from shelling out to importing.

Construct them with the ``from_*`` classmethods, which accept the loosely-typed
dicts the underlying record/RPC/transcript helpers produce and pick out the
fields each shape promises. Unknown extra keys are dropped rather than carried,
so the public shape stays stable even as internal records gain fields.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class SessionInfo:
    """Recorded identity and location returned by client operations.

    State and transcript may be unknown before startup. Listing reads records;
    use peek or context for live observations.
    """

    short: str
    worker_id: str | None = None
    session_id: str | None = None
    name: str | None = None
    cwd: str | None = None
    state: str | None = None
    transcript: str | None = None
    model: str | None = None
    effort: str | None = None
    last_turn_at: int | None = None
    created_at: int | None = None
    # Cumulative managed-turn accounting from the record (None when the record
    # carries none, e.g. an external session).
    turns: int | None = None
    warning: str | None = None

    def as_dict(self) -> dict[str, Any]:
        out = asdict(self)
        if out["warning"] is None:
            out.pop("warning")
        if self.worker_id is not None:
            # Full UUID is the only managed target. Do NOT project a short
            # fallback when a historical record lacks verified ownership.
            out["worker"] = f"claude:{self.worker_id}"
            out["engine"] = "claude"
        return out



@dataclass(frozen=True)
class ClaudeContext:
    """Carried context of one Claude session, the shape ``ccx context --json``
    emits. ``state`` is the session's own lifecycle state; whether a token
    measurement exists is ``reason`` (``None`` when ``tokens_used`` is current,
    else ``pending`` / ``compacting`` / ``just_compacted`` / ``no-signal``).
    Claude reports no window capacity, so ``window_size`` / ``percent_used`` are
    always ``None``; ``as_of_turn_ref`` identifies settled turn context."""

    short: str
    worker_id: str | None = None
    name: str | None = None
    cwd: str | None = None
    state: str | None = None
    tokens_used: int | None = None
    window_size: int | None = None
    percent_used: float | None = None
    as_of_turn_ref: str | None = None
    as_of_ms: int | None = None
    reason: str | None = None
    engine: str = "claude"
    note: str | None = None

    def as_dict(self) -> dict[str, Any]:
        out = asdict(self)
        if out.get("note") is None:
            out.pop("note", None)
        if self.worker_id is not None:
            out["worker"] = f"{self.engine}:{self.worker_id}"
        return out

    @classmethod
    def from_record(cls, rec: dict, block: dict | None,
                    *, note: str | None = None) -> "ClaudeContext":
        """Project a session record plus its merged context block."""
        from ccx.claude.context import public_context
        worker_id = rec.get("workerId")
        return cls(
            short=worker_id[:8] if isinstance(worker_id, str) else rec.get("short"),
            worker_id=worker_id,
            name=rec.get("name"),
            cwd=rec.get("cwd"),
            state=(rec.get("lifecycle") or {}).get("state"),
            note=note,
            **public_context(block, engine="claude"),
        )


@dataclass(frozen=True)
class ClaudeTask:
    """One read-only Claude Code task from the native structured task list.

    The on-disk Claude schema is intentionally normalized here. Additive native
    fields stay private to Claude Code; consumers get a small stable shape that
    can survive upstream implementation changes without learning its file layout.
    """

    id: str
    subject: str
    status: str
    active_form: str | None = None
    description: str | None = None
    owner: str | None = None
    blocks: list[str] = field(default_factory=list)
    blocked_by: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, obj: dict) -> "ClaudeTask":
        def text(key: str) -> str | None:
            value = obj.get(key)
            return value if isinstance(value, str) else None

        def strings(key: str) -> list[str]:
            value = obj.get(key)
            if not isinstance(value, list):
                return []
            return [item for item in value if isinstance(item, str)]

        ident = obj.get("id")
        return cls(
            id=str(ident) if ident is not None else "",
            subject=text("subject") or "",
            status=text("status") or "unknown",
            active_form=text("activeForm"),
            description=text("description"),
            owner=text("owner"),
            blocks=strings("blocks"),
            blocked_by=strings("blockedBy"),
        )


@dataclass(frozen=True)
class Question:
    """One question inside a pending AskUserQuestion. ``text`` is the exact
    question string (the key `answer()` matches on), ``options`` are the raw
    option dicts (``{label, description}``), and ``labels`` is the convenience
    list of pickable option labels. ``raw`` round-trips the original dict."""

    text: str | None = None
    header: str | None = None
    multi_select: bool = False
    options: list = field(default_factory=list)
    raw: dict = field(default_factory=dict)

    @property
    def labels(self) -> list[str]:
        return [o.get("label") for o in self.options
                if isinstance(o, dict) and o.get("label")]

    def as_dict(self) -> dict[str, Any]:
        return dict(self.raw)

    @classmethod
    def from_dict(cls, obj: dict) -> "Question":
        return cls(
            text=obj.get("question"),
            header=obj.get("header"),
            multi_select=bool(obj.get("multiSelect")),
            options=list(obj.get("options") or []),
            raw=dict(obj),
        )


@dataclass(frozen=True)
class PendingInput:
    """Typed view of what a WAITING session is blocked on.

    ``kind`` is ``"question"`` for an AskUserQuestion (resolve with
    :meth:`CcxClient.answer`), ``"elicitation"`` for an MCP server's input
    request (resolve with :meth:`CcxClient.respond_elicitation`), or
    ``"permission"`` for a tool permission prompt (resolve with
    :meth:`CcxClient.approve` / :meth:`CcxClient.reject`).
    ``questions`` is populated for the question kind; ``server``, ``mode``
    (``"form"``/``"url"``), ``url`` and ``schema`` (the server's
    ``requestedSchema``) for the elicitation kind. ``raw`` carries the
    host's original ``waitingFor`` preview verbatim, and ``as_dict()`` returns
    it unchanged — so the JSON projection stays byte-compatible with the CLI's
    ``--json`` surfaces while attribute access is typed.
    """

    kind: str
    tool: str | None = None
    request_id: str | None = None
    message: str | None = None
    requested_at: int | None = None
    questions: list = field(default_factory=list)
    raw: dict = field(default_factory=dict)
    server: str | None = None
    mode: str | None = None
    url: str | None = None
    schema: dict | None = None

    def as_dict(self) -> dict[str, Any]:
        return dict(self.raw)

    @classmethod
    def from_waiting_for(cls, wf) -> "PendingInput | None":
        """Build from a host ``waitingFor`` preview dict (None-safe; a
        PendingInput passes through unchanged)."""
        if wf is None or isinstance(wf, PendingInput):
            return wf
        if not isinstance(wf, dict):
            return None
        from ccx.ops.semantics import pending_input_kind  # single-sourced kind rule
        tool = wf.get("tool")
        qs = ((wf.get("input") or {}).get("questions")
              if isinstance(wf.get("input"), dict) else None) or []
        schema = wf.get("schema")
        return cls(
            kind=pending_input_kind(wf) or "permission",
            tool=tool,
            request_id=wf.get("id"),
            message=wf.get("message"),
            requested_at=wf.get("requested_at"),
            questions=[Question.from_dict(q) for q in qs if isinstance(q, dict)],
            raw=dict(wf),
            server=wf.get("server"),
            mode=wf.get("mode"),
            url=wf.get("url"),
            schema=schema if isinstance(schema, dict) else None,
        )


@dataclass(frozen=True)
class SessionStatus:
    """Bounded, orchestration-only status projection — the shape ``peek
    --status`` emits. Scalars only: no transcript text. ``error`` is set (e.g.
    ``"not_found"``) instead of raising for a vanished session, so a poller never
    has to special-case one."""

    short: str
    worker_id: str | None = None
    engine: str = "claude"
    state: str | None = None
    turn: int | None = None
    last_turn_age_s: int | None = None
    host_status: str | None = None
    # Typed pending-input view (PendingInput) when the session is waiting;
    # as_dict() projects it back to the raw host preview for CLI parity.
    waiting_for: PendingInput | None = None
    mode: str | None = None
    needs: str | None = None
    context: dict | None = None
    # Stop-time transcript summary, recorded without a read-time task scan.
    background_task_summary: dict | None = None
    error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        # Drop keys the projection didn't include, matching the CLI's output
        # (mode/needs only appear for externals; error only on failure).
        out = asdict(self)
        if self.worker_id is not None:
            out["worker"] = f"{self.engine}:{self.worker_id}"
        else:
            out.pop("engine", None)
            out.pop("worker_id", None)
        if isinstance(self.waiting_for, PendingInput):
            # asdict() recursed into the dataclass; the public JSON shape is
            # the raw host preview, byte-compatible with `peek --status --json`.
            out["waiting_for"] = self.waiting_for.as_dict()
        for k in ("mode", "needs", "error", "context", "background_task_summary"):
            if out.get(k) is None:
                out.pop(k, None)
        return out

    @property
    def is_waiting(self) -> bool:
        return bool(self.waiting_for)

    @classmethod
    def from_projection(cls, proj: dict) -> "SessionStatus":
        return cls(
            short=proj.get("short"),
            worker_id=proj.get("worker_id"),
            engine=proj.get("engine") or "claude",
            state=proj.get("state"),
            turn=proj.get("turn"),
            last_turn_age_s=proj.get("last_turn_age_s"),
            host_status=proj.get("host_status"),
            waiting_for=PendingInput.from_waiting_for(proj.get("waiting_for")),
            mode=proj.get("mode"),
            needs=proj.get("needs"),
            context=proj.get("context"),
            background_task_summary=proj.get("background_task_summary"),
            error=proj.get("error"),
        )


@dataclass(frozen=True)
class Report:
    """The last assistant turn's text for a session — the shape ``report
    --json`` emits per session."""

    short: str
    turn: int | None
    text: str
    transcript: str | None = None
    context: dict | None = None
    # Transcript-derived background tasks for the session (see
    # ``ccx.transcript.extract_background_tasks``). Additive and read-only: an
    # open task means a continuation may still fire, not that the worker is
    # unfinished. Empty list / zero when none.
    background_tasks: list = field(default_factory=list)
    background_task_count: int = 0
    open_background_task_count: int = 0
    active_background_task_count: int = 0
    stale_background_task_count: int = 0
    running_background_task_count: int = 0
    background_tasks_truncated: bool = False
    background_task_limit: int | None = None
    # Advisory string surfaced when active/stale background task state needs a
    # caller-facing hint (see ccx.commands._background_task_facts).
    background_task_hint: str | None = None
    # Full identity is authoritative; legacy short remains display-only.
    worker_id: str | None = None
    engine: str = "claude"
    turn_ref: str | None = None
    delivery_id: str | None = None
    # Set when a newer turn is running or ended without a reply.
    note: str | None = None

    def as_dict(self) -> dict[str, Any]:
        out = asdict(self)
        if self.worker_id is not None:
            out["worker"] = f"{self.engine}:{self.worker_id}"
        return out


@dataclass(frozen=True)
class SendResult:
    """Outcome of a ``send`` / ``answer`` / ``new(wait=True)`` call.

    For a synchronous (waited) delivery ``reply`` holds the assistant's last
    turn and ``queued`` is False. An accepted strict send with ``wait=False``
    started its turn: ``queued`` is False and ``reply`` is None.
    Brokered input replies may still set ``queued`` while a native turn resumes.
    """

    short: str
    reply: str | None = None
    transcript: str | None = None
    state: str | None = None
    queued: bool = False
    # Original full worker/delivery identity, additive to original result.
    # A caller can reconcile an unknown send without guessing native source.
    worker_id: str | None = None
    engine: str = "claude"
    delivery_id: str | None = None
    delivery_status: str | None = None
    settled: bool = False
    turn_ref: str | None = None

    def as_dict(self) -> dict[str, Any]:
        out = asdict(self)
        if self.worker_id is not None:
            out["worker"] = f"{self.engine}:{self.worker_id}"
        return out


@dataclass(frozen=True)
class CompactResult:
    """Outcome of a native Claude Code ``/compact`` request."""

    short: str
    state: str | None = None
    compacted: bool = False
    message: str | None = None
    pre_tokens: int | None = None
    post_tokens: int | None = None
    duration_ms: int | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CompactOperationResult(CompactResult):
    """Durable compact result with operation identity and native evidence.

    Subclassing preserves the frozen legacy :class:`CompactResult` schema for
    consumers that construct or validate it directly, while new compact calls
    can expose the production operation protocol additively.
    """

    operation_id: str | None = None
    operation_status: str | None = None
    evidence_uuid: str | None = None
    reason: str | None = None
    worker_id: str | None = None

    def as_dict(self) -> dict[str, Any]:
        out = asdict(self)
        if self.worker_id is not None:
            out["worker"] = f"claude:{self.worker_id}"
        return out


@dataclass(frozen=True)
class SemanticDeliveryResult:
    """Outcome of caller-identified semantic delivery.

    Route may be known before final placement. turn_ref is non-null only after
    placement/start identity is proven.
    """

    short: str
    delivery_id: str
    status: str
    accepted: bool = False
    owned: bool = False
    recommended_action: str | None = None
    route: str | None = None
    placement: str | None = None
    target_turn_ref: str | None = None
    turn_ref: str | None = None
    managed_turn_ordinal: int | None = None
    state: str | None = None
    attempts: int = 0
    maybe_written: bool = False
    evidence: str | None = None
    evidence_uuid: str | None = None
    reason: str | None = None
    worker_id: str | None = None
    engine: str = "claude"
    settled: bool = False

    def as_dict(self) -> dict[str, Any]:
        out = asdict(self)
        if self.worker_id is not None:
            out["worker"] = f"{self.engine}:{self.worker_id}"
        return out


@dataclass(frozen=True)
class SteerResult:
    """Outcome of a durable ``steer`` on a busy managed worker.

    ``accepted`` is True once the prompt is durably owned by CCX — that is the
    delivery guarantee, and it does not weaken when the observation window
    elapses. ``status`` speaks one placement vocabulary:

    ``"accepted-pending"``
        CCX owns the prompt; Claude has not visibly placed it in a turn yet.
        Never a reason to resend.
    ``"consumed-current-turn"``
        The running turn saw it (native ``queued_command`` attachment).
    ``"started-next-turn"``
        It runs as the next turn (ordinary user event / dequeue).
    ``"unknown"``
        Genuinely indeterminate transport or evidence.

    ``timed_out`` means only that stronger evidence had not appeared yet;
    ``recommended_action`` is then ``"wait"``. Retrying with the same
    ``delivery_id`` is idempotent and never pastes twice.
    """

    short: str
    delivery_id: str
    accepted: bool = False
    status: str | None = None
    timed_out: bool = False
    state: str | None = None
    pending: bool = False
    evidence: str | None = None
    evidence_uuid: str | None = None
    attempts: int | None = None
    reason: str | None = None
    recommended_action: str | None = None
    worker_id: str | None = None
    engine: str = "claude"

    def as_dict(self) -> dict[str, Any]:
        result = asdict(self)
        if self.worker_id is not None:
            result["worker"] = f"{self.engine}:{self.worker_id}"
        return result


@dataclass(frozen=True)
class WaitResult:
    """Resolution of a ``wait`` call: the stable state a session settled into.

    ``status`` is the coarse verdict (``idle``/``waiting``/``dead``/
    ``budget-stopped``/``gone``/``timeout``/…), ``mode`` distinguishes managed vs
    external, and ``reason`` is a human sentence. ``wait_kind`` is set when
    ``status`` is ``waiting`` (``permission`` or an AskUserQuestion).
    ``timeout`` means a numeric ``timeout=`` elapsed before the session
    stabilized; ``state`` then carries the last observed lifecycle state.
    """

    status: str
    mode: str | None = None
    state: str | None = None
    reason: str | None = None
    wait_kind: str | None = None
    # Typed pending-input view when status is "waiting" (managed sessions);
    # None when the verdict carries no preview. as_dict() projects it raw.
    waiting_for: PendingInput | None = None
    # `--next-turn` waits: ``wait_mode`` is "next-turn" when the call blocked for
    # a future turn/revival (else None for a plain wait). ``next_turn_observed``
    # is True once that future turn — or, for managed sessions, a new input
    # gate or a settlement newer than ``after_turn`` — was seen.
    # ``next_turn_event`` carries the managed host event when known: a terminal
    # (`turn_complete` or additive `turn_settled`, liveness-only settlement
    # preserving `outcome="unknown"`) or a gate event (`state`→waiting,
    # `permission_requested`, `elicitation_requested`), which is NOT
    # settlement. None on the `after_turn` comparison fast path and for
    # native-bg, which has no such event.
    wait_mode: str | None = None
    next_turn_observed: bool = False
    next_turn_event: dict | None = None
    # Managed sessions: the exact latest settled turn (``turn_ref``,
    # ``turn_id`` (None for Claude), ``outcome``, ``completed_at``) — the
    # checkpoint to pass back as ``after_turn`` — and the host's active turn
    # ref when known. None when unknown or not a managed session.
    last_settled_turn: dict | None = None
    active_turn_ref: str | None = None
    # Background-task facts for the session `wait` reported on. `wait` reports
    # them but never blocks on them — an open task does not extend the wait.
    background_tasks: list = field(default_factory=list)
    background_task_count: int = 0
    open_background_task_count: int = 0
    active_background_task_count: int = 0
    stale_background_task_count: int = 0
    running_background_task_count: int = 0
    background_tasks_truncated: bool = False
    background_task_limit: int | None = None
    # Advisory string surfaced when active/stale background task state needs a hint.
    background_task_hint: str | None = None
    # Claude's classification when the session stopped on an API error, else
    # None. `reason` states the same thing in prose; this is the half a caller
    # can branch on — notably `rate_limited`, which must never be indistinguish-
    # able from an ordinary crash. Managed Claude sessions only: Codex and
    # native-bg carry no equivalent, and inventing one would be a lie.
    api_error: dict | None = None

    def as_dict(self) -> dict[str, Any]:
        out = asdict(self)
        if isinstance(self.waiting_for, PendingInput):
            out["waiting_for"] = self.waiting_for.as_dict()
        elif out.get("waiting_for") is None:
            out.pop("waiting_for", None)
        return out

    @classmethod
    def from_verdict(cls, verdict: dict) -> "WaitResult":
        return cls(
            status=verdict.get("status"),
            mode=verdict.get("mode"),
            state=verdict.get("state"),
            reason=verdict.get("reason"),
            wait_kind=verdict.get("wait_kind"),
            waiting_for=PendingInput.from_waiting_for(verdict.get("waiting_for")),
            wait_mode=verdict.get("wait_mode"),
            next_turn_observed=verdict.get("next_turn_observed", False),
            next_turn_event=verdict.get("next_turn_event"),
            last_settled_turn=verdict.get("last_settled_turn"),
            active_turn_ref=verdict.get("active_turn_ref"),
            background_tasks=verdict.get("background_tasks") or [],
            background_task_count=verdict.get("background_task_count", 0),
            open_background_task_count=verdict.get("open_background_task_count", 0),
            active_background_task_count=verdict.get("active_background_task_count", 0),
            stale_background_task_count=verdict.get("stale_background_task_count", 0),
            running_background_task_count=verdict.get("running_background_task_count", 0),
            background_tasks_truncated=verdict.get("background_tasks_truncated", False),
            background_task_limit=verdict.get("background_task_limit"),
            background_task_hint=verdict.get("background_task_hint"),
            api_error=verdict.get("apiError"),
        )


@dataclass(frozen=True)
class Event:
    """One stamped event from a session's broadcast stream.

    Every host broadcast carries an envelope: ``type`` (the event kind, e.g.
    ``state`` / ``tool_started`` / ``subagent_finished``), ``seq`` (a per-host
    monotonic counter), ``short``, ``epoch`` (identifies the host life — changes
    on respawn, so ``seq`` numbering restarts with it), ``ts`` (broadcast
    wall-clock seconds), and ``v`` (schema version). The full stamped object —
    envelope plus any event-specific fields — is kept in ``raw``; ``as_dict()``
    returns it verbatim so it round-trips through ``json.dumps``.
    """

    type: str | None
    seq: int | None = None
    short: str | None = None
    epoch: str | None = None
    ts: float | None = None
    v: int | None = None
    raw: dict = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return dict(self.raw)

    @property
    def cursor(self) -> tuple[str | None, int | None]:
        """The ``(epoch, seq)`` pair identifying this event's position. Pass it
        back as ``events(since_epoch=…, since_seq=…)`` to page forward across
        host respawns (``seq`` resets per epoch, so epoch is required)."""
        return (self.epoch, self.seq)

    @classmethod
    def from_dict(cls, obj: dict) -> "Event":
        return cls(
            type=obj.get("type"),
            seq=obj.get("seq"),
            short=obj.get("short"),
            epoch=obj.get("epoch"),
            ts=obj.get("ts"),
            v=obj.get("v"),
            raw=dict(obj),
        )


@dataclass(frozen=True)
class Usage:
    """Live quota utilization plus provider-normalized session-reset status.

    ``summary`` is the compact quota-only projection; ``raw`` is the full OAuth
    payload. ``five_hour_reset`` is deliberately normalized by ccx so callers do
    not need to know Anthropic's private experiment/program names.
    """

    summary: dict = field(default_factory=dict)
    raw: dict = field(default_factory=dict)
    five_hour_reset: dict | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "summary": dict(self.summary),
            "raw": dict(self.raw),
            "five_hour_reset": (
                dict(self.five_hour_reset) if self.five_hour_reset is not None else None
            ),
        }

    def utilization(self, metric: str) -> float | None:
        """Convenience for a directly named quota such as ``five_hour`` or
        ``seven_day``. Dynamic per-model quotas live in ``model_quotas``."""
        block = self.summary.get(metric) or self.raw.get(metric)
        if isinstance(block, dict):
            return block.get("utilization")
        return None


@dataclass(frozen=True)
class FiveHourReset:
    """Outcome of one provider-gated 5-hour session-limit reset attempt."""

    result: str
    status: dict | None = None
    next_available_at: str | None = None
    weekly_resets_at: str | None = None
    refreshed: bool = False

    @property
    def succeeded(self) -> bool:
        return self.result == "reset"

    def as_dict(self) -> dict[str, Any]:
        return {
            "result": self.result,
            "status": dict(self.status) if self.status is not None else None,
            "next_available_at": self.next_available_at,
            "weekly_resets_at": self.weekly_resets_at,
            "refreshed": self.refreshed,
        }


__all__ = [
    "SessionInfo",
    "ClaudeContext",
    "SessionStatus",
    "PendingInput",
    "Question",
    "Report",
    "SendResult",
    "SteerResult",
    "WaitResult",
    "Event",
    "Usage",
    "FiveHourReset",
]
