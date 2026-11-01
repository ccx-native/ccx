"""Engine-neutral client for managed workers.

Both engine clients pin the same sealed active generation and resolve full
CCX worker UUIDs independently of provider-native identifiers."""

from __future__ import annotations
import ccx.cli.commands.read as commands_read

import json
import os
import signal
import time
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
from functools import wraps
from typing import Callable, Iterator

from ccx.codex import evidence as codex_evidence
from ccx.codex import records as codex_records
from ccx.ops import event_feed
from ccx.ops import turn_reports
from ccx.ops import terminate
from ccx.system import oslayer
from ccx.ops import conversation as _conv
from .client import CcxClient
from ccx.codex.client import CodexClient
from ccx.codex.types import rate_limit_windows, window_label
from .constants import (
    DEFAULT_IDLE_TIMEOUT_S,
    STEER_OBSERVE_DEFAULT_S,
    S,
)
from ccx.codex.read import pending_input as _codex_pending
from ccx.claude.context import current_context, public_context, read_claude_observation
from .errors import (
    CapabilityUnsupported,
    CcxError,
    DeliveryRejected,
    InvalidRequest,
    SessionNotFound,
    SessionUnavailable,
    SessionWaiting,
    TurnIncomplete,
    worker_not_found,
)
from ccx.store.liveness import pid_alive, recorded_host_loaded
from ccx.store.paths import sock_path, statusline_json_path
from ccx.store.records import merge_lifecycle
from ccx.rpc.rpc import _respawn, _respawn_failure_message, _rpc
from ccx.ops.semantics import KIND_ELICITATION, pending_input_kind
from ccx.claude.usage import fetch_usage, usage_summary
from ccx.store.worker_state import (
    RUNTIME_INCONCLUSIVE,
    normalize_native_state,
)

_ENGINES = ("claude", "codex")

# Failures a per-engine step may raise that must NOT take down the other engine
# in a cross-engine list/usage: typed ccx errors plus the ordinary ways a
# corrupt record or an unexpected provider payload blows up. Deliberately not
# BaseException — a KeyboardInterrupt is not a provider failure.
_PROVIDER_FAILURES = (CcxError, OSError, RuntimeError, ValueError, KeyError, TypeError,
                      AttributeError)


def _engine_error(error: BaseException) -> dict:
    return {"error": type(error).__name__, "message": str(error),
            "exit_code": getattr(error, "exit_code", 1)}

_CAP_KEYS = ("5h", "7d", "opus")

# States in which a worker is settled enough to stop without forcing.
_SETTLED = ("idle", "stopped", "failed", "unavailable")
# WaitResult background-task fields carried through ``wait`` for Claude.
_BACKGROUND_FACT_FIELDS = (
    "background_tasks", "background_task_count",
    "running_background_task_count", "open_background_task_count",
    "active_background_task_count", "stale_background_task_count",
    "background_tasks_truncated", "background_task_limit",
    "background_task_hint",
)

# Normalized states in the order a recap summary counts them: lifecycle order,
# so the one-line header reads the same way every time.
_RECAP_STATES = ("starting", "idle", "busy", "waiting", "stopped", "failed",
                 "unavailable")

# Sentinel: "read the state yourself" vs an explicitly supplied one (which may
# legitimately be None).
_UNSET = object()


# --- handles, records, budgets ----------------------------------------------

def _handle(worker) -> str:
    """Coerce a handle string or any facade result dict to a handle string."""
    if isinstance(worker, dict):
        worker = worker.get("worker")
    if not isinstance(worker, str):
        raise InvalidRequest(
            "worker must be a handle string like 'claude:966a8a1b' "
            "(or a result dict carrying one)"
        )
    return worker


def _split(worker) -> tuple[str, str, str]:
    """``"claude:966a8a1b"`` -> ``("claude:966a8a1b", "claude", "966a8a1b")``."""
    handle = _handle(worker)
    engine, sep, short = handle.partition(":")
    if not sep or engine not in _ENGINES or not short:
        raise InvalidRequest(
            f"invalid worker handle {handle!r}; expected "
            "'claude:<short>' or 'codex:<short>'"
        )
    return handle, engine, short


def _state(native) -> str:
    return normalize_native_state(native)


def _public_created_result(store, raw: dict, *, fork: bool = False) -> dict:
    """Main's neutral create/fork shape from the child's latest recorded lifecycle."""
    try:
        record = store.record(raw["worker"])
    except Exception as error:
        # The original native effect may already have landed. Keep its full UUID
        # and cause rather than returning a stale activation snapshot or retrying.
        from .errors import CreatePartialFailure
        raise CreatePartialFailure(
            "worker created but public projection is incomplete; retain worker_id",
            info=raw, phase="inspect", cause=error,
        ) from error
    native = (record.get("lifecycle") or {}).get("state")
    report = raw.get("report")
    reply = (report.get("text") if isinstance(report, dict)
             and report.get("status") == "completed"
             and isinstance(report.get("text"), str) else None)
    public = {
        "worker": raw["worker"], "worker_id": raw["worker_id"],
        "engine": raw["engine"], "short": raw["short"],
        "state": _state(native), "native_state": native,
        "report": report, "reply": reply,
        "created": raw["created"], "reconciled": raw["reconciled"],
        "delivery_id": raw.get("delivery_id"),
        "delivery_status": raw.get("delivery_status"),
        "notification": raw.get("notification"),
    }
    if fork:
        public["forked_from"] = raw["forked_from"]
    return public


def _host_from_loaded(loaded: bool | None) -> str:
    if loaded is True:
        return "ok"
    if loaded is False:
        return "unreachable"
    return "timeout"


# Budget arithmetic is shared with ccx.terminate, which owns the confirmed
# teardown these same budgets are spent on.
_deadline = terminate.deadline_for
_left = terminate.left




def _codex_record(short: str) -> dict:
    rec = codex_records.read_record(short)
    if rec is None:
        raise SessionNotFound(f"no such Codex worker: {short}", short=short)
    return rec




def _background_task_block_for_worker(store, owner) -> dict:
    """Bound the original background-task facts for facade wait and report."""
    from ccx.ops.common import background_task_block, recorded_background_facts

    rec = store.record(owner.worker)  # mandatory owner/record evidence stays strict
    recorded = rec.get("backgroundTaskSummary")
    if isinstance(recorded, dict):
        return background_task_block(recorded_background_facts(recorded), supported=True)
    if owner.engine != "claude":
        return background_task_block(None, supported=False)
    try:
        facts = CcxClient._uuid_background_task_facts_owned(store, owner, rec)
    except (SessionUnavailable, OSError) as exc:
        store.revalidate()  # never hide a generation change as missing task facts
        return {"background_tasks_supported": True,
                "background_tasks_available": False,
                "background_task_note": f"background tasks unknown: {exc}",
                "background_task_summary": rec.get("backgroundTaskSummary")}
    if "backgroundTaskSummary" in rec:
        facts["background_task_summary"] = rec["backgroundTaskSummary"]
    return background_task_block(facts, supported=owner.engine == "claude")


def _check_caps(caps, *, engine: str | None = None) -> None:
    """Validate a per-turn quota cap before anything is delivered.

    ``{"5h"|"7d"|"opus": 0-100}``. Booleans are rejected explicitly: ``True``
    is an ``int`` in Python and would silently arm a 1% cap.
    """
    if not isinstance(caps, dict) or not caps:
        raise InvalidRequest("caps must be a non-empty dict")
    unknown = set(caps) - set(_CAP_KEYS)
    if unknown:
        raise InvalidRequest(
            f"unknown caps keys {sorted(unknown)}; expected {_CAP_KEYS}"
        )
    if engine == "codex" and "opus" in caps:
        raise InvalidRequest("Codex does not support cap-opus (Claude-only)")
    for key, value in caps.items():
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise InvalidRequest(
                f"caps[{key!r}] must be a number between 0 and 100, "
                f"got {value!r}"
            )
        if not 0 <= value <= 100:
            raise InvalidRequest(
                f"caps[{key!r}]={value!r} is outside 0..100"
            )


def evaluate_quota(usage: dict, limits: dict) -> dict:
    """Evaluate account quota windows against exact normalized window ids.

    Unknown or malformed requested windows make ``allowed`` false.  The
    function is deliberately policy-free: callers may inspect ``unknown`` and
    choose to fail open, report a checkpoint, or retry later themselves.
    """
    if not isinstance(usage, dict):
        raise InvalidRequest("usage must be a WorkerClient.usage result dict")
    if not isinstance(limits, dict) or not limits:
        raise InvalidRequest("quota limits must be a non-empty dict")
    normalized_limits: dict[str, float] = {}
    for window_id, limit in limits.items():
        if not isinstance(window_id, str) or not window_id:
            raise InvalidRequest("quota limit ids must be non-empty strings")
        if isinstance(limit, bool) or not isinstance(limit, (int, float)):
            raise InvalidRequest(
                f"quota limit {window_id!r} must be a number between 0 and 100"
            )
        if not 0 <= limit <= 100:
            raise InvalidRequest(
                f"quota limit {window_id!r}={limit!r} is outside 0..100"
            )
        normalized_limits[window_id] = float(limit)

    raw_windows = usage.get("windows")
    if not isinstance(raw_windows, list):
        raw_windows = []
    by_id: dict[str, list[dict]] = {}
    for item in raw_windows:
        if isinstance(item, dict) and isinstance(item.get("id"), str):
            by_id.setdefault(item["id"], []).append(item)

    reached: list[str] = []
    unknown: list[str] = []
    windows: list[dict] = []
    reached_resets: list[tuple[datetime, str]] = []
    retry_known = True
    for window_id, limit in normalized_limits.items():
        candidates = by_id.get(window_id, [])
        item = candidates[0] if len(candidates) == 1 else None
        used = item.get("used_percent") if item is not None else None
        if (
            item is None
            or isinstance(used, bool)
            or not isinstance(used, (int, float))
            or not 0 <= used <= 100
        ):
            unknown.append(window_id)
            windows.append({
                "id": window_id, "limit_percent": limit,
                "used_percent": None, "resets_at": None, "state": "unknown",
            })
            continue
        used = float(used)
        is_reached = used >= limit
        resets_at = item.get("resets_at")
        reset_time = None
        if isinstance(resets_at, str):
            try:
                reset_time = datetime.fromisoformat(resets_at.replace("Z", "+00:00"))
                if reset_time.tzinfo is None:
                    reset_time = None
            except ValueError:
                reset_time = None
        else:
            resets_at = None
        windows.append({
            "id": window_id, "limit_percent": limit,
            "used_percent": used, "resets_at": resets_at,
            "state": "reached" if is_reached else "allowed",
        })
        if is_reached:
            reached.append(window_id)
            if reset_time is None:
                retry_known = False
            else:
                reached_resets.append((reset_time, resets_at))
    retry_at = (
        max(reached_resets, key=lambda item: item[0])[1] if reached and retry_known
        and len(reached_resets) == len(reached) else None
    )
    return {
        "allowed": not reached and not unknown,
        "reached": reached,
        "unknown": unknown,
        "retry_at": retry_at,
        "windows": windows,
    }


def _unsupported(engine: str, capability: str, detail: str) -> CapabilityUnsupported:
    return CapabilityUnsupported(
        f"{engine} does not support {detail}", engine=engine, capability=capability
    )


def _claude_observed_model(rec: dict) -> str | None:
    """The model Claude actually answered with, from the rendered transcript.

    Never falls back to the requested model, and never accepts ``<synthetic>``
    (the pseudo-model on API-error replies). Reads the already-rendered document
    so this stays cheap enough for ``status``.
    """
    path = rec.get("transcriptJson")
    if not path:
        return None
    try:
        with open(path, encoding="utf-8") as handle:
            doc = json.load(handle)
    except (OSError, ValueError):
        return None
    for turn in reversed(doc.get("turns") or []):
        model = turn.get("model")
        if isinstance(model, str) and model and model != "<synthetic>":
            return model
    return None


def _claude_statusline_model(short: str) -> str | None:
    """Last model Claude reported through the compact statusline sidecar.

    Unlike context freshness, a settled worker's last observed model does not
    expire.  This is a read-only fallback for inventory summaries when the
    cleaned transcript cache has not been rendered yet.
    """
    try:
        doc = json.loads(statusline_json_path(short).read_text())
    except (OSError, ValueError):
        return None
    model = doc.get("model") if isinstance(doc, dict) else None
    value = model.get("id") if isinstance(model, dict) else None
    return value if isinstance(value, str) and value and value != "<synthetic>" else None


def _summary_context(block: dict | None, *, has_turn: bool, codex: bool) -> dict:
    """Normalize persisted context without rendering either engine transcript."""
    if not has_turn:
        state = "not_started"
        block = None
    elif not isinstance(block, dict):
        state = "unavailable"
        block = None
    else:
        native_state = block.get("state")
        state = native_state if native_state in {
            "known", "unknown", "pending", "unavailable"
        } else ("known" if block.get("tokens_used") is not None else "unavailable")
    block = block or {}
    return {
        "state": state,
        "tokens_used": block.get("tokens_used"),
        "window_size": block.get("window_size") if codex else None,
        "percent_used": block.get("percent_used") if codex else None,
        "as_of_turn_ref": block.get("as_of_turn_ref"),
        "as_of_ms": block.get("as_of_ms"),
        "reason": block.get("reason"),
    }


def _claude_pending(waiting_for) -> dict | None:
    if not isinstance(waiting_for, dict) or not waiting_for:
        return None
    tool = waiting_for.get("tool")
    questions = waiting_for.get("input") or {}
    questions = (questions.get("questions") or []) if isinstance(questions, dict) else []
    out = {
        "kind": pending_input_kind(waiting_for),
        "request_id": waiting_for.get("id"),
        "tool": tool,
        "message": waiting_for.get("message"),
        "questions": list(questions),
        "native": dict(waiting_for),
    }
    if out["kind"] == KIND_ELICITATION:
        # Only the fields a responder needs; `native` keeps the full preview.
        out["elicitation"] = {
            key: waiting_for.get(key) for key in ("server", "mode", "url", "schema")
        }
    return out


def _codex_approval_turn_id(record: dict, request_id) -> str | None:
    """Return the native turn fenced by one pending approval request."""
    approvals = [value for value in record.get("pendingApprovals") or []
                 if isinstance(value, dict)]
    selected = None
    if request_id is None and len(approvals) == 1:
        selected = approvals[0]
    elif request_id is not None:
        selected = next(
            (
                value for value in approvals
                if str(request_id) in {
                    str(value.get("requestId")), str(value.get("gateId")),
                }
            ),
            None,
        )
    turn_id = selected.get("turnId") if selected else None
    if not isinstance(turn_id, str) or not turn_id:
        turn_id = (record.get("lifecycle") or {}).get("activeTurnId")
    return turn_id if isinstance(turn_id, str) and turn_id else None


def _recap_row(handle: str, engine: str, short: str) -> dict:
    """A recap row before anything is observed.

    Every row carries the same keys whatever happened to the worker, so a script
    can index them unconditionally. ``unavailable`` is the honest starting point:
    a row only claims a better state once one has actually been read.
    """
    return {"worker": handle, "engine": engine, "short": short, "name": None,
            "state": "unavailable", "native_state": None,
            "requested_model": None, "observed_model": None,
            "requested_effort": None, "turns": None,
            "context_tokens": None, "context_as_of_turn_ref": None,
            "context_as_of_ms": None, "error": None}


def _recap_counts(rows: list[dict]) -> str:
    counts: dict[str, int] = {}
    for row in rows:
        counts[row["state"]] = counts.get(row["state"], 0) + 1
    order = [s for s in _RECAP_STATES if s in counts]
    order += sorted(s for s in counts if s not in _RECAP_STATES)
    return ", ".join(f"{state} {counts[state]}" for state in order)


def _recap_line(row: dict, width: int) -> str:
    """One worker as one line. Unobservable fields are omitted, never guessed."""
    fields = [row["worker"].ljust(width)]
    if row["name"]:
        fields.append(str(row["name"]))
    state, native = row["state"], row["native_state"]
    fields.append(f"{state}({native})" if native and native != state else state)
    model, observed = row["requested_model"], row["observed_model"]
    if observed and observed != model:
        # Requested and observed never merge, so a divergence is shown as one.
        model = f"{model or '?'}->{observed}"
    if model:
        fields.append(f"model={model}")
    if row["requested_effort"]:
        fields.append(f"effort={row['requested_effort']}")
    if row["turns"] is not None:
        fields.append(f"turns={row['turns']}")
    if row["context_tokens"] is not None:
        marker = row["context_as_of_turn_ref"]
        fields.append(f"context={row['context_tokens']}"
                      + ("" if marker is None else f"@{marker}"))
    if row["error"]:
        fields.append(f"error={row['error']}")
    return "  ".join(fields)


def _recap_text(rows: list[dict]) -> str:
    """The printable recap: a count header plus one line per worker, in order.

    Deterministic by construction — no timestamps, no elapsed time, no ordering
    by anything but the caller's input. Task-specific facts (result, phase,
    workspace, report paths, continuation commands) belong to the script, which
    is the only thing that knows them.
    """
    if not rows:
        return "0 workers"
    width = max(len(row["worker"]) for row in rows)
    header = (f"{len(rows)} worker{'' if len(rows) == 1 else 's'}: "
              f"{_recap_counts(rows)}")
    return "\n".join([header] + [_recap_line(row, width) for row in rows])


# Process termination primitives live in ccx.terminate, which both this facade
# and the two CLIs use for confirmed kills.
_signal = terminate.signal_pid
_escalate_at = terminate.escalate_at
_await_exit = terminate.await_exit
_reap = terminate.reap


def _claude_windows(summary: dict) -> list[dict]:
    windows: list[dict] = []
    for key, label in (("five_hour", "5h"), ("seven_day", "7d")):
        block = summary.get(key)
        if isinstance(block, dict):
            used = block.get("utilization")
            windows.append({
                "id": key,
                "label": label,
                "used_percent": float(used) if isinstance(used, (int, float)) else None,
                "resets_at": block.get("resets_at"),
            })
    for quota in summary.get("model_quotas") or []:
        if not isinstance(quota, dict):
            continue
        used = quota.get("utilization")
        windows.append({
            "id": f"model:{quota.get('model_id') or quota.get('model')}",
            "label": str(quota.get("model") or "model"),
            "used_percent": float(used) if isinstance(used, (int, float)) else None,
            "resets_at": quota.get("resets_at"),
        })
    return windows


def _codex_windows(data: dict) -> list[dict]:
    """Normalize Codex windows with duration-based IDs."""
    windows: list[dict] = []
    for limit_id, window in rate_limit_windows(data):
        used, resets = window.get("usedPercent"), window.get("resetsAt")
        label = window_label(window)
        windows.append({
            "id": f"{limit_id}:{label}",
            "label": label,
            "used_percent": float(used) if type(used) in (int, float) else None,
            "resets_at": (
                datetime.fromtimestamp(resets, timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ) if type(resets) in (int, float) else None
            ),
        })
    return windows


class WorkerClient:
    """One lifecycle for ccx-managed Claude workers and CCX-owned Codex workers.

    Stateless and cheap to construct; it holds only the two stateless concrete
    clients. There is no "current engine" anywhere, so concurrent calls against
    distinct workers are safe by construction. Never shells out to the ``ccx``
    CLI and never calls ``os.fork()`` in your process.
    """

    def _uuid_owner(self, worker):
        from ccx.store.identity import UnknownWorker, IdentityError
        from ccx.store.root import GenerationError
        target=worker.get("worker") if isinstance(worker,dict) else worker
        try:
            return self._uuid_store.resolve(target)
        except UnknownWorker as exc:
            raise worker_not_found(target) from exc
        except IdentityError as exc:
            raise InvalidRequest(f"Invalid worker handle: {exc}") from exc
        except GenerationError as exc:
            raise SessionUnavailable(f"Worker record unavailable; check its status: {exc}") from exc

    def __init__(self) -> None:
        """One pinned global generation for both engine views, no ensure_dirs."""
        self._claude = CcxClient()
        self._codex = CodexClient()
        self._uuid_store = self._claude._uuid_store
        if self._codex._uuid_store.pointer != self._uuid_store.pointer:
            raise SessionUnavailable("CCX worker records changed during startup; retry")
        self._uuid_store.revalidate()

    # -- introspection ------------------------------------------------------

    def capabilities(self, engine) -> dict:
        """Truthful implemented UUID capabilities, not old native promises."""
        if isinstance(engine,dict):
            engine=engine.get("worker")
        if engine not in _ENGINES:
            engine=self._uuid_owner(engine).engine
        # The dashboard's busy-turn router uses steer_current_turn.
        # All other published flags below describe actual engine differences.
        return {
            "engine": engine,
            "steer_current_turn": True,
            "auto_delivery": engine == "claude",
            "wait_next_turn": engine == "claude",
            "wait_after_turn": engine == "claude",
            "wait_known_input": engine == "claude",
            "answer_question": engine == "claude",
            "respond_elicitation": engine == "claude",
        }

    def variables(self, worker, name: str | None = None) -> dict:
        """Full-UUID durable variable read, no live object resurrection."""
        owner=self._uuid_owner(worker)
        from ccx.store.root import GenerationError
        try:
            return self._uuid_store.variables_info(owner.worker,name=name)
        except GenerationError as exc:
            raise SessionUnavailable(f"full-UUID variables unavailable: {exc}") from exc

    # -- create -------------------------------------------------------------

    def create(
        self, *, engine: str, cwd: str, worker_id: str | None = None,
        model: str | None = None, effort: str | None = None,
        permissions: str = "full", name: str | None = None,
        prompt: str | None = None, idle_timeout: int | None = None,
        wait: bool = False, timeout: float | None = None,
        start_timeout: float | None = None,
        caps: dict | None = None, variables: dict | None = None,
        variables_from=None,
    ) -> dict:
        """One engine-neutral original native-ready UUID creation operation."""
        from ccx.ops.creation import create
        if engine not in _ENGINES:
            raise InvalidRequest(f"unknown worker engine: {engine!r}")
        if variables is not None and variables_from is not None:
            raise InvalidRequest("variables and variables_from are mutually exclusive")
        attachment = None
        if variables is not None or variables_from is not None:
            from ccx.controllers import live_vars
            if variables_from is not None:
                source = self._uuid_owner(variables_from)
                attachment = live_vars.prepare_shared_durable_attachment(
                    source.engine, source.worker_id,
                )
            else:
                attachment = live_vars.prepare_attachment(variables)
        raw = create(
            engine=engine,cwd=cwd,worker_id=worker_id,prompt=prompt,wait=wait,
            caps=caps,
            name=name,model=model,effort=effort,permissions=permissions,
            idle_timeout=(DEFAULT_IDLE_TIMEOUT_S
                          if idle_timeout is None else idle_timeout),
            created_via="python-api",timeout=timeout,
            start_timeout=start_timeout, attachment=attachment,
            expected_generation=self._uuid_store.generation,
        )
        return _public_created_result(self._uuid_store, raw)

    def fork(
        self, worker, *, worker_id: str | None = None, name: str | None = None,
        model: str | None = None, effort: str | None = None,
        prompt: str | None = None, wait: bool = False,
        timeout: float | None = None, caps: dict | None = None,
        variables: dict | None = None, variables_from=None,
    ) -> dict:
        """Fork the CCX-owned ``worker`` into a new worker with its history.

        The result has :meth:`create`'s keys plus ``forked_from``. Retrying
        with the same ``worker_id`` reuses the original fork point.
        """
        from math import isfinite
        from ccx.ops.forking import fork
        from ccx.store.root import GenerationError
        from ccx.store.identity import IdentityError, UnknownWorker

        if variables is not None and variables_from is not None:
            raise InvalidRequest("variables and variables_from are mutually exclusive")
        attachment = None
        if variables is not None or variables_from is not None:
            from ccx.controllers import live_vars
            if variables_from is not None:
                source = self._uuid_owner(variables_from)
                attachment = live_vars.prepare_shared_durable_attachment(
                    source.engine, source.worker_id,
                )
            else:
                attachment = live_vars.prepare_attachment(variables)
        if timeout is not None and (
            type(timeout) not in (int, float) or not isfinite(timeout) or timeout <= 0
        ):
            raise InvalidRequest("fork timeout must be positive finite seconds or None")
        deadline = time.monotonic() + timeout if timeout is not None else None
        try:
            raw = fork(
                _handle(worker), worker_id=worker_id, name=name, model=model,
                effort=effort, created_via="python-api",
                expected_generation=self._uuid_store.generation, deadline=deadline,
                prompt=prompt, wait=wait, caps=caps, attachment=attachment,
            )
        except UnknownWorker as exc:
            raise worker_not_found(_handle(worker)) from exc
        except IdentityError as exc:
            raise InvalidRequest(f"fork: invalid full-UUID worker: {exc}") from exc
        except GenerationError as exc:
            raise SessionUnavailable(f"fork: ACTIVE generation unavailable: {exc}") from exc
        return _public_created_result(self._uuid_store, raw, fork=True)

    # -- state readers ------------------------------------------------------



    def status(self, worker) -> dict:
        """Recorded-only worker status, with native identity distinct."""
        owner=self._uuid_owner(worker)
        rec=self._uuid_store.record(owner.worker)
        lc=rec.get("lifecycle")
        lc=lc if isinstance(lc,dict) else {}
        native=rec.get("native")
        native=native if isinstance(native,dict) else {}
        stats=rec.get("stats")
        stats=stats if isinstance(stats,dict) else {}
        args=rec.get("createArgs")
        args=args if isinstance(args,dict) else {}
        if owner.engine == "claude":
            observation = read_claude_observation(self._uuid_store, owner, rec)
            observed = observation.get("observedModel") if observation else None
        else:
            observed = rec.get("resolvedModel")
        return {
            "worker": owner.worker,
            "worker_id":owner.worker_id, "engine":owner.engine,
            "short":owner.short, "name":rec.get("name"),
            "cwd":rec.get("cwd"), "state":normalize_native_state(lc.get("state")),
            "native_state": lc.get("state"),
            "native_id": (native.get("sessionId") if owner.engine=="claude"
                          else native.get("threadId")),
            "terminal_only":bool(rec.get("terminalOnly")),
            "state_source":"recorded",
            "host":"unobserved",
            "loaded":recorded_host_loaded(owner.engine, lc),
            # Recorded gate previews, like peek: the request_id is what
            # approve/reject/answer require; the host stays the final fence.
            "pending_input": (_claude_pending(rec.get("waitingFor"))
                              if owner.engine == "claude"
                              else _codex_pending(rec.get("pendingApprovals"))
                              if lc.get("state") == "waiting" else None),
            "last_turn_at":rec.get("lastTurnAt"),
            "managed_turns":stats.get("turns"),
            "requested_model":args.get("model"),
            "requested_effort":args.get("effort"),
            "observed_model":observed,
            "record_revision":rec.get("recordRevision"),
            "context": commands_read._status_context_for_record(owner.worker, rec),
            **({"background_task_summary": rec["backgroundTaskSummary"]}
               if "backgroundTaskSummary" in rec else {}),
        }

    def list(self, *, engine: str | None = None, cwd: str | None = None,
             state: str | None = None, parent: str | None = None,
             limit: int | None = None) -> dict:
        """Recorded owned workers, optionally exact direct children of a full UUID."""
        if engine is not None and engine not in _ENGINES:
            raise InvalidRequest(f"unknown engine: {engine!r}")
        if cwd is not None and not isinstance(cwd,str):
            raise InvalidRequest("cwd filter must be a path string")
        parent_owner = self._uuid_owner(parent) if parent is not None else None
        from ccx.system.cwd_paths import canonical_cwd
        from ccx.store.root import GenerationError
        try:
            rows=self._uuid_store.list_workers(
                engine=engine, cwd=canonical_cwd(cwd) if cwd else None,
                state=state, parent=parent_owner.worker if parent_owner else None,
                limit=limit,
            )
        except GenerationError as exc:
            raise SessionUnavailable(f"full-UUID list unavailable: {exc}") from exc
        return {"workers": rows, "errors": {}}


    def summaries(self, handles) -> dict:
        """Bounded recorded-only enrichment of exact owned full-UUID workers.

        Preserve caller order and isolate only a genuinely absent full ID.
        Neither the display prefix nor a native session/thread id is a target;
        no host, daemon, transcript rendering or inferred liveness is queried.
        Claude's already-rendered observation is a small owned sidecar; this
        never starts a provider, renders a transcript, or guesses liveness.
        """
        from ccx.store.identity import IdentityError, parse_managed_target

        if isinstance(handles, (str, bytes, dict)):
            raise InvalidRequest("summaries expects an iterable of worker handles")
        try:
            items = iter(handles)
        except TypeError as exc:
            raise InvalidRequest("summaries expects an iterable of worker handles") from exc
        self._uuid_store.revalidate()
        rows: list[dict] = []
        errors: dict[str, dict] = {}
        seen: set[str] = set()
        for value in items:
            target = value.get("worker") if isinstance(value, dict) else value
            try:
                parsed = parse_managed_target(target)
            except IdentityError as exc:
                raise InvalidRequest(f"Invalid worker handle: {exc}") from exc
            try:
                owner = self._uuid_owner(target)
            except SessionNotFound as exc:
                # Missing is a per-row consumer result; invalid/contradictory
                # authority, stale generations and corrupt records are NOT.
                missing = (f"{parsed.qualified_engine}:{parsed.worker_id}"
                           if parsed.qualified_engine else parsed.worker_id)
                if missing in seen:
                    continue
                seen.add(missing)
                failure = _engine_error(exc)
                errors[missing] = failure
                rows.append({
                    "worker": missing, "worker_id": parsed.worker_id,
                    "engine": parsed.qualified_engine, "short": parsed.worker_id[:8],
                    "observed_model": None,
                    "context": _summary_context(
                        None, has_turn=True, codex=parsed.qualified_engine == "codex",
                    ),
                    "error": failure["error"],
                })
                continue
            if owner.worker in seen:
                continue
            seen.add(owner.worker)
            rec = self._uuid_store.record(owner.worker)
            if owner.engine == "claude":
                observation = read_claude_observation(self._uuid_store, owner, rec)
                block = observation.get("context") if observation else None
                observed = observation.get("observedModel") if observation else None
            else:
                block = current_context(rec, engine="codex", live=rec.get("context"))
                observed = rec.get("resolvedModel")
            rows.append({
                "worker": owner.worker, "worker_id": owner.worker_id,
                "engine": owner.engine, "short": owner.short,
                "observed_model": observed,
                "context": _summary_context(
                    block, has_turn=rec.get("lastTurnAt") is not None or block is not None,
                    codex=owner.engine == "codex",
                ),
                "error": None,
            })
        return {"count": len(rows), "workers": rows, "errors": errors}

    def report(
        self, worker, *, max_chars: int | None = None,
        turn_ref: str | None = None, timeout: float = 30,
    ) -> dict:
        """Read the latest turn's reply, or an explicitly selected completed turn.

        ``has_final_message`` says whether the selected turn produced a final
        assistant message, independent of ``max_chars`` clipping of ``text``.
        ``note`` explains when a newer turn is running, an earlier reply was
        selected, native history could not be read, or the worker failed.
        """
        from ccx.ops.report import report_worker
        owner=self._uuid_owner(worker)
        if timeout<=0:
            raise InvalidRequest("native report timeout must be positive")
        if max_chars is not None and (type(max_chars) is not int or max_chars<0):
            raise InvalidRequest("report max_chars must be nonnegative integer")
        self._uuid_store.revalidate()
        result=report_worker(
            owner.worker,max_chars=max_chars,turn_ref=turn_ref,
            timeout=timeout,
        )
        background = _background_task_block_for_worker(self._uuid_store, owner)
        return {
            "worker":owner.worker,"worker_id":owner.worker_id,
            "engine":owner.engine,"short":owner.short,
            "text":result["text"],"truncated":result["truncated"],
            "has_final_message":result["has_final_message"],
            "turn":None,"turn_id":result.get("turn_id"),
            "turn_ref":result.get("turn_ref"),
            "delivery_id":result.get("delivery_id"),
            "status":result["status"],
            "source":result["source"],
            "native_evidence_sha256":result.get("native_evidence_sha256"),
            "note":result.get("note"),
            **background,
        }

    def telemetry(self, worker, *, after_cursor: str | None = None,
                  limit: int = 200, timeout: float = 120) -> dict:
        """Read the same source-bound telemetry page as worker.telemetry RPC."""
        from ccx.ops.report import telemetry_worker
        owner = self._uuid_owner(worker)
        return telemetry_worker(
            owner.worker, after_cursor=after_cursor, limit=limit, timeout=timeout,
        )

    def context(self, worker) -> dict:
        """Read recorded carried-context evidence, never native liveness."""
        owner = self._uuid_owner(worker)
        rec = self._uuid_store.record(owner.worker)
        lifecycle = rec.get("lifecycle") or {}
        native = rec.get("native") or {}
        if owner.engine == "claude":
            observation = read_claude_observation(self._uuid_store, owner, rec)
            block = observation.get("context") if observation else None
        else:
            block = current_context(rec, engine="codex", live=rec.get("context"))
        return {
            "worker": owner.worker, "worker_id": owner.worker_id,
            "engine": owner.engine, "short": owner.short,
            "state": lifecycle.get("state"),
            "native_state": native.get("nativeState"),
            **public_context(
                block, engine=owner.engine,
                compacting=(owner.engine == "codex"
                            and isinstance(rec.get("activeCompact"), dict)),
            ),
            **({"note": observation["note"]} if owner.engine == "claude"
               and observation and observation.get("note") else {}),
        }

    def recap(self, handles) -> dict:
        """Deterministic recorded-only recap of exact full-UUID workers.

        Reuse the authoritative summaries, owner records and carried-context
        reader. Only valid absent UUIDs become unavailable rows; bad identity,
        generation and observation integrity fail closed. No native probes,
        transcript rendering or lifecycle changes occur.
        """
        summary = self.summaries(handles)
        rows: list[dict] = []
        for item in summary["workers"]:
            handle = item["worker"]
            row = _recap_row(handle, item["engine"], item["short"])
            row["worker_id"] = item["worker_id"]
            row["error"] = item["error"]
            if item["error"] is None:
                rec = self._uuid_store.record(handle)
                lifecycle = rec.get("lifecycle") or {}
                stats = rec.get("stats") or {}
                args = rec.get("createArgs")
                args = args if isinstance(args, dict) else {}
                context = self.context(handle)
                row.update(
                    name=rec.get("name"),
                    state=_state(lifecycle.get("state")),
                    native_state=lifecycle.get("state"),
                    turns=stats.get("turns"),
                    requested_model=args.get("model"),
                    observed_model=item["observed_model"],
                    requested_effort=args.get("effort"),
                    context_tokens=context["tokens_used"],
                    context_as_of_turn_ref=context["as_of_turn_ref"],
                    context_as_of_ms=context["as_of_ms"],
                )
            rows.append(row)
        return {
            "count": summary["count"], "workers": rows,
            "errors": summary["errors"], "text": _recap_text(rows),
        }

    # -- delivery -----------------------------------------------------------

    def send(
        self, worker, text: str, *, wait: bool = True,
        timeout: float | None = None,
        caps: dict | None = None, variables: dict | None = None,
        variables_from=None, delivery_id: str | None = None,
    ) -> dict:
        """Full UUID facade routes to original engine native send, no alias.

        Native delivery_id is caller-owned and may be retried only for the
        exact same prompt. Indeterminate original effects are returned as
        typed errors, never changed into a fresh token/provider request.
        """
        owner=self._uuid_owner(worker)
        handle=owner.worker
        if caps is not None:
            _check_caps(caps, engine=owner.engine)
        if variables is not None and variables_from is not None:
            raise InvalidRequest("variables and variables_from are mutually exclusive")
        if variables is not None or variables_from is not None:
            from ccx.controllers import live_vars
            record = self._uuid_store.record(handle)
            live_vars.require_send_variable_permission(owner, record)
            if variables_from is not None:
                source = self._uuid_owner(variables_from)
                attachment = live_vars.prepare_shared_durable_attachment(
                    source.engine, source.worker_id,
                )
            else:
                attachment = live_vars.prepare_attachment(variables)
            if attachment is not None and (
                attachment.names or attachment.state_id is not None
            ):
                if live_vars.should_publish_send_attachment(
                    self._uuid_store, owner, delivery_id, caps=caps,
                ):
                    live_vars.publish_attachment(
                        attachment, owner.engine, owner.worker_id,
                    )
                names = self._uuid_store.variables_info(handle)["names"]
                text = live_vars.with_variable_hint(text, names)
        self._uuid_store.revalidate()
        try:
            if owner.engine=="claude":
                native=self._claude.send(
                    handle,text,wait=wait,timeout=timeout,
                    delivery_id=delivery_id,caps=caps,
                )
            else:
                native=self._codex.send(
                    handle,text,wait=wait,timeout=timeout,delivery_id=delivery_id,
                    caps=caps,
                )
        except DeliveryRejected as exc:
            if exc.state=="waiting":
                # Only a positive native refusal triggers waiting. Recorded
                # status supplies the pending gate preview, never authority.
                status=self.status(handle)
                raise SessionWaiting(
                    f"{owner.engine} worker {owner.short} is waiting on input",
                    short=owner.short,
                    waiting_for=(status.get("pending_input") or {}).get("native"),
                ) from exc
            if (owner.engine=="codex" and exc.refusal=="worker-state"
                    and exc.state=="busy"):
                exc.refusal="busy"
            raise
        if owner.engine=="claude":
            token=native.delivery_id
            return {
                "worker":handle,"worker_id":owner.worker_id,
                "engine":"claude","short":owner.short,
                "delivery_id":token,
                "delivery_status":native.delivery_status,
                "waited":bool(wait),"queued":False,
                "reply":native.reply,"turn_id":None,
                "turn_ref":native.turn_ref,
                "client_user_message_id":None,
                "settled":native.settled,
                "state":native.state,
            }
        return {
            "worker":handle,"worker_id":owner.worker_id,
            "engine":"codex","short":owner.short,
            "delivery_id":native.delivery_id,
            "delivery_status":("settled" if native.reply is not None
                               else "accepted"),
            "waited":bool(wait),"queued":False,
            "reply":native.reply,"turn_id":native.turn_id,
            "turn_ref":("ccx-turn:codex:v1:"+native.turn_id
                        if native.turn_id else None),
            "client_user_message_id":native.client_user_message_id,
            "settled":native.reply is not None,
            "state":native.state,
        }

    def steer(self, worker, text: str, *, wait: bool = True,
              timeout: float | None = STEER_OBSERVE_DEFAULT_S,
              caps: dict | None = None,
              delivery_id: str | None = None) -> dict:
        """Place one busy-turn instruction; Codex returns on native placement."""
        owner = self._uuid_owner(worker)
        if owner.engine == "codex":
            if caps is not None:
                _check_caps(caps, engine="codex")
            try:
                native = self._codex.steer(
                    owner.worker, text, delivery_id=delivery_id, caps=caps,
                )
            except DeliveryRejected as exc:
                status = self.status(owner.worker)
                if status["state"] == "waiting":
                    raise SessionWaiting(
                        f"Codex worker {owner.worker} is waiting on input",
                        short=owner.short,
                        waiting_for=(status.get("pending_input") or {}).get("native"),
                    ) from exc
                raise
            return {
                "worker": owner.worker, "worker_id": owner.worker_id,
                "engine": "codex", "short": owner.short,
                "accepted": True, "delivery_id": native.delivery_id,
                "status": "accepted-current-turn", "pending": False,
                "timed_out": False, "turn_id": native.turn_id,
                "client_user_message_id": native.client_user_message_id,
                "recommended_action": None, "state": native.state,
            }
        if caps is not None:
            _check_caps(caps, engine="claude")
        value = self._claude.steer(
            owner.worker, text, delivery_id=delivery_id,
            wait=wait, timeout=timeout, caps=caps,
        ).as_dict()
        self._uuid_store.revalidate()
        return {**value, "turn_id": None, "client_user_message_id": None}

    def _codex_settle(
        self, owner, deadline: float | None, operation: str, *,
        gate_id: str, turn_id: str,
    ) -> str:
        """Observe the one approved original Codex turn; never decide again."""
        turn_ref = f"ccx-turn:codex:v1:{turn_id}"

        def incomplete(
            reason: str, *, state: str | None = None,
            native_status: str | None = None, native_error=None,
        ) -> TurnIncomplete:
            failure = TurnIncomplete(
                f"{reason}; approval input accepted for gateId={gate_id!r}, "
                f"turn_ref={turn_ref!r}; never retry the decision",
                short=owner.short, state=state, turn_id=turn_id,
                turn_status=native_status, turn_error=native_error,
            )
            failure.input_accepted = True
            failure.request_id = gate_id
            failure.turn_ref = turn_ref
            return failure

        from ccx.store.root import GenerationError

        try:
            observed = self.wait(owner.worker, timeout=_left(deadline))
        except (CcxError, GenerationError) as exc:
            raise incomplete("original approved turn observation unavailable") from exc
        state = observed.get("native_state") or observed.get("state")
        status = observed.get("status")
        if status == "waiting" or state == "waiting":
            gate_error = SessionWaiting(
                f"Codex worker {owner.short} opened another input gate after "
                f"{operation}; original {gate_id!r} already accepted, never retry",
                short=owner.short,
                waiting_for=(observed.get("pending_input") or {}).get("native"),
            )
            gate_error.input_accepted = True
            gate_error.request_id = gate_id
            gate_error.turn_ref = turn_ref
            raise gate_error
        if observed.get("timed_out") or status == "timeout":
            raise incomplete("The approved turn did not finish in time; inspect its status", state=state)
        last = observed.get("last_settled_turn") or {}
        actual = observed.get("last_turn") or observed.get("lastTurn") or {}
        if (state != "idle" or status not in ("idle", None)
                or last.get("turn_ref") != turn_ref
                or last.get("turn_id") != turn_id
                or last.get("outcome") != "completed"
                or actual.get("turnId") != turn_id
                or actual.get("status") != "completed"):
            raise incomplete(
                "Approved turn did not complete as expected; inspect its status",
                state=state, native_status=actual.get("status"),
                native_error=actual.get("error"),
            )
        remaining = _left(deadline, 30)
        if remaining is not None and remaining <= 0:
            raise incomplete("The reply was not available before timeout; inspect the turn",
                             state=state, native_status="completed")
        try:
            report = self.report(owner.worker, turn_ref=turn_ref,
                                 timeout=remaining if remaining is not None else 30)
        except (CcxError, GenerationError) as exc:
            raise incomplete(
                "original approved turn completed but its exact report is unavailable",
                state=state, native_status="completed",
            ) from exc
        if (report.get("turn_ref") != turn_ref
                or report.get("turn_id") != turn_id
                or report.get("status") != "completed"
                or not isinstance(report.get("text"), str)):
            raise incomplete(
                "The approved reply could not be verified; inspect the turn",
                state=state,
            )
        self._uuid_store.revalidate()
        return report["text"]

    def wait(self, worker, *, timeout: float | None = None,
             next_turn: bool = False, after_turn: str | None = None,
             known_input: str | None = None) -> dict:
        """Read-only full-UUID stability/next-turn verdict; timeout is data.

        Both engines share one native-evidence observer. Claude additionally
        accepts next-turn/checkpoint/input selectors; Codex rejects them.
        """
        from ccx.ops.wait import wait_worker

        owner = self._uuid_owner(worker)
        result = wait_worker(
            owner.worker, timeout=timeout, next_turn=next_turn,
            after_turn=after_turn, known_input=known_input,
            expected_generation=self._uuid_store.generation,
        )
        background = _background_task_block_for_worker(self._uuid_store, owner)
        # Keep the engine-neutral snake_case views without changing the
        # canonical worker/ref or interpreting a terminal as a completed reply.
        state = result.get("state")
        if isinstance(state, str):
            result = {**result, "native_state": result.get("native_state", state),
                      "state": normalize_native_state(state)}
        public = {key: value for key, value in result.items()
                  if key not in {"apiError", "lastTurn", "pendingApprovals"}}
        return {**public,
                "api_error": result.get("apiError"),
                "last_turn": result.get("lastTurn"),
                "pending_approvals": result.get("pendingApprovals"), **background}

    def answer(
        self, worker, text: str | None = None, *,
        request_id: str | None = None,
        options: list[tuple[int, str]] | None = None,
        text_answers: list[tuple[int, str]] | None = None,
        wait: bool = False, timeout: float | None = None,
        fallback_to_send: bool = False,
        response_id: str | None = None,
    ) -> dict:
        """Answer a Claude question using its existing exact input fence."""
        owner = self._uuid_owner(worker)
        if owner.engine != "claude":
            raise _unsupported(owner.engine, "answer_question", "questions")
        result = self._claude.answer(
            owner.worker, text, request_id=request_id, options=options,
            text_answers=text_answers, wait=wait, timeout=timeout,
            fallback_to_send=fallback_to_send, response_id=response_id,
        )
        return result.as_dict()

    def respond_elicitation(
        self, worker, action: str, content: dict | None = None, *,
        request_id: str | None = None, response_id: str | None = None,
        wait: bool = False, timeout: float | None = None,
    ) -> dict:
        """Resolve a Claude MCP form through its existing native handler."""
        owner = self._uuid_owner(worker)
        if owner.engine != "claude":
            raise _unsupported(owner.engine, "respond_elicitation", "MCP forms")
        result = self._claude.respond_elicitation(
            owner.worker, action, content, request_id=request_id,
            response_id=response_id, wait=wait, timeout=timeout,
        )
        return result.as_dict()

    def approve(self, worker, *, request_id: str | None = None,
                wait: bool = False, timeout: float | None = None) -> dict:
        """Allow the original current permission gate, optionally await its turn."""
        return self._decision(worker, "allow", request_id, None, wait, timeout)

    def reject(self, worker, *, request_id: str | None = None,
               reason: str | None = None, wait: bool = False,
               timeout: float | None = None) -> dict:
        """Deny the original permission gate without guessing a successor."""
        return self._decision(worker, "deny", request_id, reason, wait, timeout)

    def _decision(self, worker, decision, request_id, reason, wait, timeout) -> dict:
        from math import isfinite
        from ccx.store.root import GenerationError

        if timeout is not None and (
            type(timeout) not in (int, float) or not isfinite(timeout) or timeout <= 0
        ):
            raise InvalidRequest("permission timeout must be positive finite or None")
        if request_id is not None and (
            not isinstance(request_id, str) or not request_id
            or len(request_id) > 256 or "\x00" in request_id
        ):
            raise InvalidRequest("Invalid request ID; use the one shown in ccx peek")
        deadline = _deadline(timeout)
        owner = self._uuid_owner(worker)
        if owner.engine == "codex" and reason is not None:
            raise _unsupported(
                "codex", "decision_reason",
                "Codex native approval does not take a rationale",
            )

        record = self._uuid_store.record(owner.worker)
        if owner.engine == "claude":
            waiting = record.get("waitingFor")
            if (not isinstance(waiting, dict)
                    or pending_input_kind(waiting) != "permission"
                    or not isinstance(waiting.get("id"), str)
                    or not waiting["id"]):
                raise SessionUnavailable(
                    "no original Claude permission gate is currently brokered",
                    short=owner.short,
                )
            actual_id = waiting["id"]
            if request_id is None:
                request_id = actual_id
            if request_id != actual_id:
                raise DeliveryRejected(
                    "permission request_id differs from the current original gate",
                    short=owner.short, refusal="request-id-mismatch",
                )
            verb = self._claude.approve if decision == "allow" else self._claude.reject
            remaining = _left(deadline)
            if remaining is not None and remaining <= 0:
                raise TurnIncomplete("permission budget elapsed before response",
                                     short=owner.short)
            result = verb(
                owner.worker, request_id=request_id, reason=reason,
                wait=wait, timeout=remaining,
            )
            reply = result.reply
        else:
            native = record.get("native") or {}
            original_thread = native.get("threadId")
            try:
                source = self._uuid_store.codex_source(owner.worker)
                activation = self._uuid_store.native_activation(owner.worker)
            except GenerationError as exc:
                raise SessionUnavailable(
                    "original Codex approval source unavailable", short=owner.short,
                ) from exc
            if (not isinstance(original_thread, str) or not original_thread
                    or native.get("threadSource") != source
                    or activation.get("state") != "confirmed"
                    or activation.get("source") != source
                    or (activation.get("nativeBinding") or {}).get("threadId")
                    != original_thread):
                raise SessionUnavailable(
                    "original Codex approval thread/source unconfirmed",
                    short=owner.short,
                )
            gates = [
                gate for gate in record.get("pendingApprovals") or []
                if isinstance(gate, dict)
            ]
            if request_id is None:
                if len(gates) != 1:
                    raise InvalidRequest(
                        "Codex approval requires request_id when zero or multiple "
                        "original gates are pending; inspect pending_input"
                    )
                request_id = gates[0].get("gateId")
            matches = [gate for gate in gates if gate.get("gateId") == request_id]
            if len(matches) != 1:
                raise DeliveryRejected(
                    "no unique original Codex permission for this CCX gateId; "
                    "inspect pending_input before responding",
                    short=owner.short, refusal="request-id-mismatch",
                )
            native_turn_id = matches[0].get("turnId")
            if (not isinstance(request_id, str) or not request_id
                    or len(request_id) > 128 or "\x00" in request_id
                    or not isinstance(native_turn_id, str) or not native_turn_id):
                raise SessionUnavailable(
                    "original Codex approval gateId/turnId is untrusted",
                    short=owner.short,
                )
            remaining = _left(deadline, 10)
            if remaining is not None and remaining <= 0:
                raise TurnIncomplete("approval budget elapsed before native decision",
                                     short=owner.short, turn_id=native_turn_id)
            op = "approve" if decision == "allow" else "reject"
            if wait:
                # Reuse the concrete client's original ACK check including the
                # same native turn; no raw daemon response or second decision.
                result = self._codex._decision(
                    owner.worker, op, request_id, remaining,
                    expected_turn_id=native_turn_id,
                )
            else:
                verb = self._codex.approve if decision == "allow" else self._codex.reject
                result = verb(owner.worker, request_id=request_id, timeout=remaining)
            request_id = result.request_id or request_id
            reply = (self._codex_settle(
                owner, deadline, decision, gate_id=request_id,
                turn_id=native_turn_id,
            ) if wait else None)
        self._uuid_store.revalidate()
        return {
            "worker": owner.worker, "worker_id": owner.worker_id,
            "short": owner.short, "engine": owner.engine,
            "decision": decision, "request_id": request_id, "reply": reply,
        }

    # -- context management -------------------------------------------------

    @staticmethod
    def _compact_result_row(owner, result, *, wait: bool = True,
                            before_tokens: int | None = None) -> dict:
        """Project one original Claude compact result, never an inferred turn."""
        proven = result.operation_status == "succeeded" and bool(result.compacted)
        pre = result.pre_tokens if proven else None
        post = result.post_tokens if proven else None
        comparable = type(pre) is int and type(post) is int
        return {
            "worker": owner.worker, "worker_id": owner.worker_id,
            "engine": owner.engine, "short": owner.short,
            "state": normalize_native_state(result.state),
            "native_state": result.state,
            "compacted": bool(result.compacted) if wait else False,
            "changed": (pre != post) if wait and comparable else None,
            "tokens_before": pre if wait else before_tokens,
            "tokens_after": post if wait else None,
            "message": result.message,
            "operation_id": result.operation_id,
            "operation_status": result.operation_status,
            "evidence_id": result.evidence_uuid,
            "reason": result.reason,
        }

    def compact(self, worker, *, wait: bool = True,
                timeout: float | None = None,
                operation_id: str | None = None) -> dict:
        """Run one native compact through the exact-owner engine core.

        The one pre-read is recorded, not a fabricated post-compact measurement.
        """
        owner = self._uuid_owner(worker)
        if owner.engine == "codex":
            from uuid import uuid4
            from ccx.codex.ops import compact_worker
            from .errors import CompactFailed, CompactIndeterminate
            from ccx.store.root import GenerationError, NativeEffectIndeterminate

            before = self.context(owner.worker)  # One fenced recorded pre-read.
            token = str(uuid4()) if operation_id is None else operation_id
            try:
                result = compact_worker(
                    owner.worker, operation_id=token, wait=wait, timeout=timeout,
                )
            except NativeEffectIndeterminate as exc:
                raise CompactIndeterminate(
                    f"Codex compact effect unknown; reconcile, never resubmit: {exc}",
                    short=owner.short, operation_id=token, operation_status="unknown",
                ) from exc
            except GenerationError as exc:
                raise SessionUnavailable(
                    f"full-UUID Codex native compact unavailable: {exc}",
                    short=owner.short,
                ) from exc
            native_state = (
                self._uuid_store.record(owner.worker).get("lifecycle") or {}
            ).get("state")
            status = result["status"]
            if wait and status == "failed":
                raise CompactFailed(
                    result.get("reason") or "Codex native compact turn failed",
                    short=owner.short, state=native_state, operation_id=token,
                )
            if status == "rejected":
                raise DeliveryRejected(
                    result.get("reason") or "Codex compact refused before native admission",
                    short=owner.short, state=native_state,
                    refusal="native-rejected", operation_id=token,
                )
            proven = status == "succeeded" and result.get("compacted") is True
            if wait and not proven:
                # Main contract: a returned wait=True compact is a settled one.
                raise CompactIndeterminate(
                    f"Codex compact result {status!r} has no proven native completion",
                    short=owner.short, state=native_state,
                    operation_id=token, operation_status=status,
                )
            native_before = result.get("contextBefore")
            native_after = result.get("contextAfter") if proven else None
            baseline = native_before if isinstance(native_before, dict) else before
            pre = baseline.get("tokens_used") if isinstance(baseline, dict) else None
            post = (native_after.get("tokens_used")
                    if isinstance(native_after, dict) else None)
            comparable = (proven and type(pre) is int and type(post) is int)
            return {
                "worker": owner.worker, "worker_id": owner.worker_id,
                "engine": "codex", "short": owner.short,
                "state": normalize_native_state(native_state),
                "native_state": native_state,
                "operation_id": token, "operation_status": status,
                "compacted": proven,
                "tokens_before": pre if type(pre) is int else None,
                "tokens_after": post if type(post) is int else None,
                "changed": (pre != post) if comparable else None,
                "message": result.get("reason") or f"Codex compact {status}",
                "reason": result.get("reason"),
                "evidence_id": result.get("contextItemId") if proven else None,
                "compact_turn_id": result.get("compactTurnId"),
            }

        before = self.context(owner.worker)
        result = self._claude.compact(
            owner.worker, wait=wait, timeout=timeout, operation_id=operation_id,
        )
        return self._compact_result_row(
            owner, result, wait=wait,
            before_tokens=before.get("tokens_used") if not wait else None,
        )

    def compact_status(self, worker, operation_id: str) -> dict:
        """Read an existing native compaction result without submitting work."""
        owner = self._uuid_owner(worker)
        if owner.engine == "claude":
            result = self._claude.compact_status(owner.worker, operation_id)
            return self._compact_result_row(owner, result)

        from ccx.codex.ops import compact_observation
        row = compact_observation(owner.worker, operation_id)
        self._uuid_store.revalidate()
        native_state = (self._uuid_store.record(owner.worker).get("lifecycle") or {}).get("state")
        status = row["status"]
        proven = status == "succeeded" and row.get("compacted") is True
        before = row.get("contextBefore")
        after = row.get("contextAfter") if proven else None
        pre = before.get("tokens_used") if isinstance(before, dict) else None
        post = after.get("tokens_used") if isinstance(after, dict) else None
        comparable = type(pre) is int and type(post) is int
        return {
            "worker": owner.worker, "worker_id": owner.worker_id,
            "engine": owner.engine, "short": owner.short,
            "state": normalize_native_state(native_state),
            "native_state": native_state,
            "operation_id": operation_id, "operation_status": status,
            "compacted": proven,
            "tokens_before": pre if type(pre) is int else None,
            "tokens_after": post if type(post) is int else None,
            "changed": pre != post if proven and comparable else None,
            "message": row.get("reason") or f"Codex compact {status}",
            "reason": row.get("reason"),
            "evidence_id": row.get("contextItemId") if proven else None,
            "compact_turn_id": row.get("compactTurnId"),
        }

    # -- next-turn configuration -------------------------------------------

    def set_model(self, worker, model: str | None, *, timeout: float = 5) -> dict:
        """Persist the model for future turns without starting a native turn.

        Claude automatically confirms a settled-idle host stop if warm;
        Codex updates the next original turn in-place without restarting.
        """
        return self._set_next_turn_config(worker, "model", model, timeout=timeout)

    def set_effort(self, worker, effort: str | None, *, timeout: float = 5) -> dict:
        """Persist future-turn effort without changing current model work."""
        return self._set_next_turn_config(worker, "effort", effort, timeout=timeout)

    def set_name(self, worker, name: str | None) -> dict:
        """Rename a full-UUID worker with the same CAS used by the CLI."""
        owner=self._uuid_owner(worker)
        try:
            rec=self._uuid_store.rename_worker(
                owner.worker,engine=owner.engine,name=name,
            )
        except Exception as exc:
            from ccx.store.root import GenerationError
            if isinstance(exc, GenerationError):
                raise SessionUnavailable(f"full-UUID rename unavailable: {exc}") from exc
            raise
        return {
            "worker":owner.worker, "worker_id":owner.worker_id,
            "engine":owner.engine, "short":owner.short,
            "name":rec["name"], "action":"rename",
            "record_revision":rec["recordRevision"],
        }

    def _set_next_turn_config(self, worker, key: str, value: str | None,
                              *, timeout: float) -> dict:
        """Project one verified owner from its engine's existing config core."""
        owner = self._uuid_owner(worker)
        if owner.engine == "claude":
            configured, restarted = self._claude._set_spawn_config(
                owner.worker, {key: value}, restart=None, timeout=timeout,
            )
        else:
            configured = (
                self._codex.set_model(owner.worker, value, timeout=timeout)
                if key == "model" else
                self._codex.set_effort(owner.worker, value, timeout=timeout)
            )
            restarted = False
        self._uuid_store.revalidate()
        return {**configured.as_dict(), "restarted": restarted}

    # -- lifecycle ----------------------------------------------------------

    def resume(self, worker, *, timeout: float | None = 90) -> dict:
        """Explicit promptless revival of the original engine runtime.

        Claude restarts its proven original session; Codex re-subscribes the
        original thread. No synthetic model message is created.
        """
        owner = self._uuid_owner(worker)
        from ccx.ops.send import ensure_ready
        result = ensure_ready(
            owner.worker, timeout=timeout,
            expected_generation=self._uuid_store.generation,
        )
        if owner.engine == "claude":
            return {"worker": owner.worker, "worker_id": owner.worker_id,
                    "engine": "claude", "short": owner.short,
                    "session_id": result["session_id"], "state": result["state"],
                    "resumed": result["resumed"]}
        return {"worker": owner.worker, "worker_id": owner.worker_id,
                "engine": "codex", "short": owner.short,
                "thread_id": result["thread_id"], "state": result["state"],
                "resumed": result["resumed"]}

    def stop(self, worker) -> dict:
        """Request one exact active turn cancellation; keep the runtime alive.

        ``accepted`` means the native interrupt was requested, not that the
        original turn ended. ``confirmed``/``outcome`` are separate proof.
        """
        from ccx.ops.cancel import stop_worker
        owner = self._uuid_owner(worker)
        result = stop_worker(
            owner.worker, expected_generation=self._uuid_store.generation,
        )
        return {
            **result, "worker": owner.worker, "short": owner.worker_id[:8],
            "worker_id": owner.worker_id, "engine": owner.engine,
            "requested": True,
            "cancelled": result["confirmed"] and result["outcome"] == "interrupted",
            "turn_id": (result["turn_ref"].rsplit(":", 1)[-1]
                        if owner.engine == "codex" else None),
        }

    def kill(self, worker, *,
             timeout: float | None = terminate.KILL_TIMEOUT_DEFAULT_S) -> dict:
        """Confirm only the exact owned worker runtime is terminated.

        History survives; callable full-UUID resume is not yet integrated.
        """
        from ccx.ops.wait import park_worker
        owner = self._uuid_owner(worker)
        result = park_worker(owner.worker, timeout=timeout,
                             expected_generation=self._uuid_store.generation)
        return {**result, "stopped": True}

    # -- event feed ---------------------------------------------------------

    def events(self, worker, *, after: str | None = None,
               tail: int | None = None, limit: int = event_feed.LIMIT_DEFAULT,
               kinds=None, wait: float = 0.0) -> dict:
        """One original full-UUID lifecycle page, optionally held for an append."""
        from ccx.ops.worker_events import EventCursorInvalid, wait_full_uuid_event_page
        from ccx.store.root import GenerationError
        from .errors import CursorInvalidated
        from ccx.store.identity import UnknownWorker

        owner = self._uuid_owner(worker)
        try:
            page = wait_full_uuid_event_page(
                self._uuid_store, owner.worker, after=after, tail=tail,
                limit=limit, kinds=list(kinds) if kinds is not None else None,
                wait=wait,
            )
            # Internal semantic bookkeeping has no public lifecycle kind.
            # Retain the native cursor so subsequent pages remain resumable.
            if owner.engine == "claude":
                page["events"] = [event for event in page["events"]
                                  if event.get("kind") is not None]
            return page
        except EventCursorInvalid as exc:
            raise CursorInvalidated(f"Event history changed; restart reading without --after: {exc}") from exc
        except UnknownWorker as exc:
            raise worker_not_found(owner.worker) from exc
        except GenerationError as exc:
            raise SessionUnavailable(f"Events are unavailable; check the worker: {exc}") from exc

    def follow(self, worker, *, after: str | None = None,
               tail: int | None = None, kinds=None,
               timeout: float | None = None,
               heartbeat: float | None = event_feed.HEARTBEAT_DEFAULT_S,
               ) -> Iterator[dict]:
        """Replay then follow one owned original event source, even while stopped."""
        from ccx.ops.worker_events import EventCursorInvalid, follow_full_uuid_events
        from ccx.store.root import GenerationError
        from .errors import CursorInvalidated
        from ccx.store.identity import UnknownWorker

        owner = self._uuid_owner(worker)

        def observations() -> Iterator[dict]:
            try:
                for event in follow_full_uuid_events(
                    self._uuid_store, owner.worker, after=after, tail=tail,
                    kinds=list(kinds) if kinds is not None else None,
                    timeout=timeout, heartbeat=heartbeat,
                ):
                    if owner.engine != "claude" or event.get("kind") is not None:
                        yield event
            except EventCursorInvalid as exc:
                raise CursorInvalidated(f"Event history changed; restart reading without --after: {exc}") from exc
            except UnknownWorker as exc:
                raise worker_not_found(owner.worker) from exc
            except GenerationError as exc:
                raise SessionUnavailable(f"Cannot follow events; check the worker: {exc}") from exc

        return observations()

    # -- conversation -------------------------------------------------------

    def conversation_page(
        self,
        worker,
        *,
        cursor: str | None = None,
        source_record_target: int = 40,
        max_tool_result_bytes: int | None = 16_384,
        timeout: float = 30,
    ) -> dict:
        """Read one bounded normalized native-source conversation page.

        The first call returns the newest source records; older_cursor pages
        backward. Page sizing is a target over provider/native conversation
        records, never over rendered messages or normalized children.
        """
        from ccx.ops import conversation_paging as paging
        from ccx.claude.transcript import open_original_claude_source
        from ccx.store.root import GenerationError
        from ccx.store.identity import UnknownWorker
        from .errors import CursorInvalidated

        paging._validate_options(source_record_target, max_tool_result_bytes, timeout)
        owner = self._uuid_owner(worker)
        if owner.engine == "codex":
            from ccx.codex import telemetry as codex_telemetry
            from .errors import OperationTimeout
            from ccx.store.identity import UnknownWorker

            # The original journal fold and every budget refetch share one
            # caller deadline. The transport receives only the time remaining.
            deadline = time.monotonic() + float(timeout)
            store = self._uuid_store
            try:
                rec, thread_id, _journal, foreign, evidence = (
                    codex_telemetry._original_source(store, owner)
                )
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise OperationTimeout(
                        "Codex conversation page deadline expired", short=owner.short,
                    )
                page = paging.read_codex_page(
                    rec, owner.worker, root=store.root,
                    generation=store.generation, worker_id=owner.worker_id,
                    thread_id=thread_id, expected_source=store.codex_source(owner.worker),
                    evidence_index=evidence,
                    foreign_evidence=bool(foreign), cursor=cursor,
                    source_record_target=source_record_target,
                    max_tool_result_bytes=max_tool_result_bytes,
                    timeout=remaining,
                )
                store.revalidate()
                return page
            except UnknownWorker as exc:
                raise worker_not_found(owner.worker) from exc
            except (GenerationError, OSError) as exc:
                raise SessionUnavailable(
                    f"original Codex conversation page source unavailable: {exc}",
                    short=owner.short,
                ) from exc
        store = self._uuid_store
        try:
            rec = store.record(owner.worker)
            with open_original_claude_source(store, owner, rec) as source:
                if source is None:
                    if cursor is not None:
                        raise CursorInvalidated(
                            "original Claude transcript disappeared; restart paging",
                            short=owner.short,
                        )
                    raise SessionNotFound(
                        f"no original Claude transcript for {owner.worker}",
                        short=owner.short,
                    )
                pinned, source_stat, end_offset, _original_path, _directory = source
                page = paging.read_claude_page(
                    pinned, owner.worker, root=store.root,
                    generation=store.generation,
                    session_id=rec["native"]["sessionId"],
                    source_stat=source_stat, end_offset=end_offset,
                    cursor=cursor, source_record_target=source_record_target,
                    max_tool_result_bytes=max_tool_result_bytes, timeout=timeout,
                )
                store.revalidate()
                return page
        except UnknownWorker as exc:
            raise worker_not_found(owner.worker) from exc
        except (GenerationError, OSError) as exc:
            raise SessionUnavailable(
                f"original Claude conversation page source unavailable: {exc}",
                short=owner.short,
            ) from exc

    @contextmanager
    def _open_full_conversation(self, worker, *, max_tool_result_bytes,
                                timeout, include_native=False,
                                codex_recent_turns: int | None = None):
        """Build once from the original owner, retaining its native source until exit."""
        from ccx.codex import runtime as codex_runtime
        from ccx.codex import telemetry as codex_telemetry
        from ccx.claude.transcript import open_original_claude_source, PinnedNativeSource
        from ccx.store.root import GenerationError
        from ccx.store.identity import EngineMismatch, UnknownWorker
        from .errors import OperationTimeout

        _conv._requested_label(max_tool_result_bytes)
        if (timeout is not None and (isinstance(timeout, bool)
                or not isinstance(timeout, (int, float))
                or not 0 < timeout < float("inf"))):
            raise InvalidRequest("conversation timeout must be positive and finite")
        deadline = time.monotonic() + timeout if timeout is not None else None

        def read_budget() -> float:
            remaining = 120 if deadline is None else deadline - time.monotonic()
            if remaining <= 0:
                raise OperationTimeout("conversation read deadline expired")
            return remaining

        if deadline is not None:
            read_budget()
        owner = self._uuid_owner(worker)
        store = self._uuid_store
        with ExitStack() as held:
            # Catch only original source/read failures. The yield (and export's
            # destination writes) must remain outside this exception mapping.
            try:
                native_files = ()
                if owner.engine == "claude":
                    rec = store.record(owner.worker)
                    source = held.enter_context(
                        open_original_claude_source(store, owner, rec)
                    )
                    if source is None:
                        raise SessionNotFound(
                            f"no original Claude transcript available for {owner.worker}",
                            short=owner.short,
                        )
                    pinned, _stat, end_offset, original_path, _directory = source
                    native = rec.get("native") or {}
                    view = {**rec, "short": owner.short,
                            "sessionId": native.get("sessionId"),
                            "transcript": pinned}
                    document = _conv.build_claude(
                        view, state=normalize_native_state(
                            (rec.get("lifecycle") or {}).get("state")),
                        worker=owner.worker,
                        max_tool_result_bytes=max_tool_result_bytes,
                        before_offset=end_offset,
                    )
                    # Public reference names the verified original pathname;
                    # export uses the still-open native descriptor, not that path.
                    document["nativeArtifacts"][0]["path"] = str(original_path)
                    native_source = pinned
                else:
                    rec, thread_id, journal, foreign, evidence = (
                        codex_telemetry._original_source(store, owner)
                    )
                    if foreign:
                        # Foreign events cannot prove original-thread coverage.
                        evidence = {**evidence, "status": "degraded", "complete": False,
                                    "warnings": [*(evidence.get("warnings") or []),
                                                 "excluded native evidence from a foreign thread"]}
                    known_empty = rec.get("historyKnownEmpty") is True
                    if known_empty:
                        thread = {"id": thread_id, "threadSource": store.codex_source(owner.worker),
                                  "turns": []}
                    else:
                        # Only rich Codex peek requests a bounded recent window.
                        # Ordinary full conversation, logs and export retain
                        # their unmodified source-read argument contract.
                        recent = ({"limit": codex_recent_turns, "newest_first": True}
                                  if codex_recent_turns is not None else {})
                        thread = codex_runtime.read_thread(
                            thread_id, cwd=rec["cwd"], timeout=read_budget(),
                            worker_id=owner.worker_id,
                            expected_source=store.codex_source(owner.worker),
                            **recent,
                        )
                    document = _conv.build_codex(
                        rec, thread, state=normalize_native_state(
                            (rec.get("lifecycle") or {}).get("state")),
                        worker=owner.worker,
                        max_tool_result_bytes=max_tool_result_bytes, evidence=evidence,
                    )
                    if (codex_recent_turns is not None
                            and len(thread.get("turns") or []) >= codex_recent_turns):
                        # A bounded tail cannot claim global 1-based turn numbers,
                        # nor pretend to describe the complete native history.
                        for turn in document["turns"]:
                            turn["sequence"] = None
                        complete = document["completeness"]
                        complete["sourceLimited"] = True
                        complete["status"] = "source-limited"
                        complete["warnings"] = [
                            *(complete.get("warnings") or []),
                            f"showing only the most recent {codex_recent_turns} native turns",
                        ]
                        document["peekHistoryWindow"] = {
                            "kind": "recent", "limit": codex_recent_turns,
                        }
                    # A proven empty history is a synthetic normalization view,
                    # never evidence that a native thread/read actually occurred.
                    native_source = None if known_empty else thread
                    if include_native and journal.exists:
                        try:
                            fd = codex_evidence._owned_evidence_fd(
                                owner.worker, os.O_RDONLY, store=store,
                            )
                        except FileNotFoundError:
                            # Secondary evidence vanished; do not claim bytes.
                            pass
                        else:
                            held.callback(os.close, fd)
                            native_files = ((
                                "codex-evidence-jsonl", "codex-evidence.jsonl",
                                PinnedNativeSource(fd),
                            ),)
                store.revalidate()
                if deadline is not None:
                    read_budget()
            except UnknownWorker as exc:
                raise worker_not_found(owner.worker) from exc
            except (GenerationError, OSError, EngineMismatch) as exc:
                raise SessionUnavailable(
                    f"original {owner.engine} conversation source unavailable: {exc}",
                    short=owner.short,
                ) from exc
            yield owner, document, native_source, native_files

    def conversation(self, worker, *, max_tool_result_bytes: int | None = None,
                     timeout: float | None = None,
                     _recent_codex_turns: int | None = None) -> dict:
        """Read a normalized full history from this original full-UUID owner.

        Claude reads a pinned native JSONL complete-line snapshot; Codex reads
        its proven native thread without loading a worker or starting a turn.
        Neither engine uses lifecycle events or a short-keyed transcript cache.
        """
        with self._open_full_conversation(
            worker, max_tool_result_bytes=max_tool_result_bytes,
            timeout=timeout, codex_recent_turns=_recent_codex_turns,
        ) as (_owner, document, _native, _files):
            return document

    def export_conversation(self, worker, destination, *, include_native: bool = True,
                            max_tool_result_bytes: int | None = None) -> dict:
        """Write ``conversation.json`` (+ hashed native evidence) atomically."""
        from ccx.store.root import GenerationError

        with self._open_full_conversation(
            worker, max_tool_result_bytes=max_tool_result_bytes,
            timeout=None, include_native=include_native,
        ) as (owner, document, native, native_files):
            written = _conv.write_export(
                document, destination, engine=owner.engine, native_source=native,
                include_native=include_native, native_files=native_files,
            )
            try:
                self._uuid_store.revalidate()
            except GenerationError as exc:
                raise SessionUnavailable(
                    f"original {owner.engine} export generation unavailable: {exc}",
                    short=owner.short,
                ) from exc
            return {"worker": owner.worker, "engine": owner.engine,
                    "short": owner.short,
                    "destination": written["destination"],
                    "conversation_path": written["conversation_path"],
                    "native_path": written["native_path"],
                    "native_paths": written["native_paths"],
                    "turn_count": len(document["turns"]),
                    "completeness": document["completeness"],
                    "evidence": document["evidence"]}

    # -- account usage ------------------------------------------------------

    def usage(self, engine: str | None = None, *, include_tokens: bool = False,
              timeout: float = 30) -> dict:
        """Account-scoped quota utilization for one or both providers.

        The normalized ``windows`` list is a convenience, not a claim of
        equivalence: Claude and Codex quota buckets are provider-specific, so
        ``raw`` always carries the untouched provider payload.

        ``timeout`` is one end-to-end budget for the whole call, so querying
        both providers cannot take twice as long as querying one. Neither
        provider supports unbounded account reads, so it must be a positive
        number — there is no ``None``.
        """
        if engine is not None and engine not in _ENGINES:
            raise InvalidRequest(f"unknown engine: {engine!r}")
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
            raise InvalidRequest(
                f"usage timeout must be a positive number of seconds, "
                f"got {timeout!r}"
            )
        if timeout <= 0:
            raise InvalidRequest("usage timeout must be greater than zero")
        deadline = _deadline(timeout)
        if engine is not None:
            return self._usage_one(engine, include_tokens, deadline)
        from ccx.system.engine_binary import resolve_engine_binary, account_signed_out
        from .errors import EngineCliNotFound
        out: dict[str, dict] = {}
        errors: dict[str, dict] = {}
        skipped: dict[str, dict] = {}
        for eng in _ENGINES:
            try:
                binary = resolve_engine_binary(eng)
            except EngineCliNotFound as error:
                skipped[eng] = {"reason": "not_installed", "message": str(error)}
                continue
            try:
                out[eng] = self._usage_one(eng, include_tokens and eng == "codex", deadline)
            except _PROVIDER_FAILURES as error:
                if account_signed_out(eng, binary):
                    skipped[eng] = {"reason": "signed_out", "message": f"{eng} is not signed in"}
                else:
                    errors[eng] = _engine_error(error)
        return {"usage": out, "errors": errors, "skipped": skipped}

    def _usage_one(self, engine: str, include_tokens: bool,
                   deadline: float | None) -> dict:
        if engine == "claude" and include_tokens:
            raise _unsupported(engine, "account_token_activity",
                               "account token-activity reporting")
        left = _left(deadline)
        if left is not None and left <= 0:
            # The other provider already spent the budget. Say so as a typed
            # per-engine failure rather than borrowing more time.
            raise SessionUnavailable(
                f"usage budget exhausted before {engine} was queried"
            )
        if engine == "claude":
            from ccx.system.engine_binary import resolve_engine_binary
            resolve_engine_binary("claude")
            data = fetch_usage(timeout=left, max_age=0)
            if not data:
                raise SessionUnavailable("could not read Claude account usage")
            return {"engine": "claude", "kind": "rate_limits",
                    "windows": _claude_windows(usage_summary(data)),
                    "raw": data}
        result = self._codex.usage(tokens=include_tokens, timeout=left)
        return {"engine": "codex", "kind": result.kind,
                "windows": [] if include_tokens else _codex_windows(result.raw),
                "raw": dict(result.raw)}


def _controller_observed(method_name, function):
    """Journal original full-UUID mutations when a controller is active."""
    @wraps(function)
    def observed(self, *args, **kwargs):
        from sys import modules
        controller = modules.get('ccx.controllers.controller')
        context = getattr(controller, "_active_runtime", None)
        if context is None or context.get() is None:
            return function(self, *args, **kwargs)
        # Static creation refusals remain before any controller ordinal.
        if method_name == "create":
            if kwargs.get("engine") not in _ENGINES:
                raise InvalidRequest(f"unknown worker engine: {kwargs.get('engine')!r}")
        from ccx.controllers.controller import observe_worker_call
        return observe_worker_call(self, method_name, function, args, kwargs)
    return observed


for _controller_method in (
    "create", "fork", "send", "steer", "resume", "compact", "answer", "respond_elicitation", "approve",
    "reject", "stop", "kill",
):
    setattr(
        WorkerClient,
        _controller_method,
        _controller_observed(_controller_method, getattr(WorkerClient, _controller_method)),
    )


__all__ = ["WorkerClient", "evaluate_quota"]
