"""Local JSON protocol validation and dispatch.

Pure validation and capabilities precede the active-generation gate.
One-shot and persistent transports share the same full-UUID dispatcher."""

from __future__ import annotations

import json
import math
import sys
import time
from typing import Any, Callable

from ccx.ops.report import account_usage_read, conversation_page, read_transcript, report_worker, telemetry_worker
from ccx.ops.input import answer_input, decide_input, respond_input
from ccx.ops.cancel import cancel_turn
from ccx.ops.compact import compact_worker, compact_status
from ccx.ops.create import fork_worker
from ccx.ops.wait import inspect_notification, park_worker, reconcile_turn, wait_worker
from ccx.ops.common import recorded_inspector
from ccx.ops.send import send_turn
from ccx.constants import DEFAULT_IDLE_TIMEOUT_S
from ccx.errors import (
    CcxError,
    BudgetStopped,
    CapabilityUnsupported,
    CompactFailed,
    DeliveryRejected,
    InvalidRequest,
    OperationConflict,
    CursorInvalidated,
    CreatePartialFailure,
    OperationTimeout,
    ProviderProtocolError,
    SendIndeterminate,
    SessionNotFound,
    SessionUnavailable,
    SessionWaiting,
    TurnIncomplete,
    WorkerApiError,
    WorkerRateLimited,
)
from ccx.store.worker_state import normalize_native_state
from ccx.version import ccx_version

PROTOCOL_VERSION = 2
SUPPORTED_PROTOCOL_VERSIONS = (2,)
MAX_MESSAGE_BYTES = 4 * 1024 * 1024
# Budget a wait sets aside for the snapshot it was asked to include.
_INCLUDE_RESERVE_S = 1.0
_DEFAULT_REPORT_MAX_CHARS = 64 * 1024
# Re-read cadence of a reconcile held while its receipt is pending.
_HOLD_POLL_S = 0.25
MAX_REQUEST_ID_CHARS = 256
# Budget of a request that sends no `timeout_ms`, in either transport.
DEFAULT_TIMEOUT_S = 90.0

# Answered by the `ccx rpc --serve` parent itself; one-shot `ccx rpc` has no
# other request in flight and reports it as an unsupported operation.
CANCEL_OP = "request.cancel"

_ALLOWED_PARAMS: dict[str, frozenset[str]] = {
    "system.info": frozenset(),
    "worker.create": frozenset({
        "worker_id",
        "name",
        "cwd",
        "engine",
        "model",
        "effort",
        "idle_timeout",
        "permissions",
        "sandbox",
        "approval_policy",
    }),
    "worker.fork": frozenset({"target", "worker_id", "name", "model", "effort"}),
    "worker.inspect": frozenset({"target"}),
    "worker.wait": frozenset({
        "target", "next_turn", "after_turn", "known_input", "include_report", "report_max_chars",
    }),
    "worker.kill": frozenset({"target"}),
    "worker.compact": frozenset({"target", "operation_id"}),
    "worker.compact.status": frozenset({"target", "operation_id"}),
    "turn.send": frozenset({
        "target",
        "text",
        "delivery_id",
        "mode",
        "notify_parent",
        "caller_metadata",
    }),
    "turn.reconcile": frozenset({
        "target", "delivery_id", "turn_id", "hold_while_pending"
    }),
    "turn.cancel": frozenset({
        "target", "cancellation_id", "turn_ref"
    }),
    "input.answer": frozenset({
        "target", "request_id", "response_id", "answers", "notify_parent"
    }),
    "input.decide": frozenset({
        "target", "request_id", "response_id", "decision", "reason"
    }),
    "input.respond": frozenset({
        "target", "request_id", "response_id", "action", "content"
    }),
    "notification.inspect": frozenset({"event_id"}),
    "worker.report": frozenset({"target", "max_chars", "turn_ref"}),
    "worker.telemetry": frozenset({"target", "after_cursor", "limit"}),
    "worker.events": frozenset({
        "target", "after_cursor", "tail", "limit", "kinds", "wait_ms",
    }),
    "account.usage.read": frozenset({"engine"}),
    "transcript.read": frozenset({
        "locator",
        "cursor",
        "limit",
        "max_tool_result_bytes",
    }),
    "conversation.page": frozenset({
        "target",
        "cursor",
        "source_record_target",
        "max_tool_result_bytes",
    }),
    CANCEL_OP: frozenset({"cancel_request_id"}),
}


def _error(
    request_id: str | None,
    code: str,
    message: str,
    *,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    # One code per existing CLI exit category; the original precise reason
    # remains in details without adding a second public error vocabulary.
    legacy = {
        "unsupported_protocol_version": "invalid_request",
        "unsupported_operation": "invalid_request",
        "unsupported_capability": "unsupported",
        "state_unavailable": "unavailable",
        "runtime_unavailable": "unavailable",
        "partial_failure": "unavailable",
        "local_refused": "refused",
        "native_rejected": "refused",
        "busy": "refused",
        "conflict": "refused",
        "cursor_invalidated": "refused",
        "definitive_rejection": "refused",
        "approval_waiting": "waiting",
        "reconcile_required": "incomplete",
        "timeout": "incomplete",
        "provider_protocol_error": "invalid_request",
        "response_too_large": "invalid_request",
        "internal_error": "invalid_request",
        "ccx_error": "invalid_request",
    }
    public_code = legacy.get(code, code)
    public_details = dict(details or {})
    if public_code != code:
        public_details.setdefault("reason", code)
    if public_code == "unsupported":
        public_details.setdefault("engine", "unknown")
        public_details["operation"] = (
            public_details.get("operation")
            or public_details.pop("capability", "unknown")
        )
    return {
        "protocol_version": PROTOCOL_VERSION,
        "request_id": request_id,
        "ok": False,
        "error": {
            "code": public_code,
            "message": str(message)[:4000],
            "details": public_details,
        },
    }


def _success(request_id: str, result: dict[str, Any]) -> dict[str, Any]:
    return {
        "protocol_version": PROTOCOL_VERSION,
        "request_id": request_id,
        "ok": True,
        "result": result,
    }


def _request_id(value: Any) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > MAX_REQUEST_ID_CHARS
        or "\x00" in value
    ):
        raise InvalidRequest(
            f"request_id must be a non-empty string of at most "
            f"{MAX_REQUEST_ID_CHARS} characters"
        )
    return value


def _params(request: dict[str, Any]) -> dict[str, Any]:
    value = request.get("params")
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise InvalidRequest("params must be a JSON object")
    return value


def _validate_operation_params(
    op: str,
    params: dict[str, Any],
) -> None:
    allowed = _ALLOWED_PARAMS.get(op)
    if allowed is None:
        return
    unknown = sorted(set(params) - allowed)
    if not unknown:
        return
    allowed_names = sorted(allowed)
    raise InvalidRequest(
        f"{op} received unsupported params {unknown}; "
        f"allowed params are {allowed_names}"
    )


def _timeout_seconds(request: dict[str, Any], default: float) -> float:
    raw = request.get("timeout_ms")
    if raw is None:
        return default
    if not isinstance(raw, (int, float)) or isinstance(raw, bool):
        raise InvalidRequest("timeout_ms must be a positive number")
    try:
        timeout_ms = float(raw)
    except (OverflowError, ValueError) as error:
        raise InvalidRequest("timeout_ms must be a finite positive number") from error
    if not math.isfinite(timeout_ms) or timeout_ms <= 0:
        raise InvalidRequest("timeout_ms must be a finite positive number")
    return timeout_ms / 1000.0


def _required_text(params: dict[str, Any], key: str) -> str:
    value = params.get(key)
    if not isinstance(value, str) or not value:
        raise InvalidRequest(f"{key} is required")
    return value


def _optional_text(
    params: dict[str, Any],
    key: str,
    default: str | None = None,
    *,
    allow_null: bool = True,
) -> str | None:
    if key not in params:
        return default
    value = params.get(key)
    if value is None and allow_null:
        return default
    if not isinstance(value, str) or not value:
        raise InvalidRequest(f"{key} must be a non-empty string")
    return value


def _optional_nullable_int(
    params: dict[str, Any], key: str, default: int | None
) -> int | None:
    if key not in params:
        return default
    value = params.get(key)
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        raise InvalidRequest(f"{key} must be an integer or null")
    return value


def _optional_int(
    params: dict[str, Any], key: str, default: int
) -> int:
    value = params.get(key, default)
    if not isinstance(value, int) or isinstance(value, bool):
        raise InvalidRequest(f"{key} must be an integer")
    return value


def _optional_bool(
    params: dict[str, Any], key: str, default: bool = False
) -> bool:
    value = params.get(key, default)
    if not isinstance(value, bool):
        raise InvalidRequest(f"{key} must be a boolean")
    return value


def _optional_object(
    params: dict[str, Any], key: str
) -> dict[str, Any] | None:
    if key not in params:
        return None
    value = params.get(key)
    if not isinstance(value, dict):
        raise InvalidRequest(f"{key} must be a JSON object")
    return value


def _rpc_handle(value: Any, target: str | None = None) -> str | None:
    """One qualified identity from an original operation's return value."""
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        candidate = value.get("worker")
        if isinstance(candidate, str):
            return candidate
    return target


def _rpc_worker(store, source: dict, *, context: bool = True,
                settled: dict | None = None) -> dict:
    """Build V2 recorded-only worker without a new native observation."""
    from ccx.rpc.view import context_view, project_worker_view
    handle = _rpc_handle(source)
    if not isinstance(handle, str) or ":" not in handle:
        raise InvalidRequest("Request has no valid worker handle; include a target")
    try:
        record = store.record(handle)
    except Exception:
        # An accepted original native effect must not become a false refusal
        # merely because its subsequent recorded view was unavailable.
        state = source.get("state")
        state = state.get("state") if isinstance(state, dict) else None
        native = source.get("native") or {}
        engine = handle.split(":", 1)[0]
        record = {
            "lifecycle": {"state": state},
            "native": {"sessionId": native.get("session_id"),
                       "threadId": native.get("thread_id")},
            "name": source.get("name"), "cwd": source.get("cwd"),
            "createArgs": {"model": source.get("model"),
                           "effort": source.get("effort")},
            "createdAt": source.get("created_at"),
            "lastTurnAt": source.get("last_turn_at"),
            "stats": {"turns": source.get("managed_turns")},
        }
    context_data = None
    if context:
        from ccx.cli.commands.read import _status_context_for_record
        try:
            context_data = context_view(_status_context_for_record(handle, record))
        except Exception:
            context_data = context_view(None)
    if "background_tasks_supported" in source:
        background = source
    else:
        # Once, after the native operation/wait has returned. This is the same
        # recorded/owner-pinned background read used by report; never poll it.
        from ccx.workers import _background_task_block_for_worker
        try:
            background = _background_task_block_for_worker(store, store.resolve(handle))
        except Exception:
            background = {"background_tasks_supported": False}
    return project_worker_view(
        handle, record, context=context_data, background=background,
        last_settled_turn=settled,
    )


REPORT_KEYS = (
    "delivery_id", "turn_ref", "turn_id", "phase", "status", "outcome", "text",
    "truncated", "has_final_message", "settled_at_ms", "native_evidence_sha256",
    "delivery", "source",
)


def _report_view(handle: str | None, report: dict) -> dict:
    """One V2 report shape for worker.report and the report bundled in wait."""
    return {"worker": handle, **{key: report.get(key) for key in REPORT_KEYS}}


def _rpc_result(op: str, result: dict, store, params: dict) -> dict:
    """Only the Local JSON boundary is V2; never mutate shared operation output."""
    if not isinstance(result, dict):
        return result
    source = result
    original = source.get("worker")
    target = params.get("target")
    handle = _rpc_handle(original, target) or _rpc_handle(source, target)
    # Telemetry's original worker is only {engine, short, native_id}; a
    # source caller may also supply the bare full UUID. Resolve to the
    # generation-owned canonical qualified handle before publication.
    if isinstance(handle, str) and ":" not in handle:
        handle = store.resolve(handle).worker

    if op in {"worker.create", "worker.fork", "worker.inspect"}:
        view = _rpc_worker(store, source)
        if op == "worker.inspect":
            return {"worker": handle, "worker_view": view,
                    "transcript_locator": source.get("transcript_locator")}
        return {"worker": handle, "worker_view": view,
                "created": source.get("created"),
                "reconciled": source.get("reconciled")}

    if op == "worker.wait":
        view = _rpc_worker(store, {"worker": handle},
                           settled=source.get("last_settled_turn"))
        raw = source.get("status")
        from ccx.store.worker_state import normalize_native_state
        status = ("timeout" if raw == "timeout" else
                  "unavailable" if raw == "host-dead" else
                  "stopped" if raw == "killed" else
                  raw if raw in {"starting", "idle", "busy", "waiting", "stopped",
                                 "failed", "unavailable"} else
                  normalize_native_state(raw))
        cause = (raw if raw == "host-dead" or
                 raw not in {status, "timeout"} and isinstance(raw, str)
                 else source.get("reason"))
        return {
            "worker": handle, "worker_view": view,
            "status": status,
            "timed_out": bool(source.get("timed_out")),
            "reason": cause,
            "wait_mode": source.get("wait_mode", "stable"),
            "next_turn_observed": source.get("next_turn_observed", False),
            "pending_input": source.get("pending_input"),
            "active_turn_ref": source.get("active_turn_ref"),
            "report": (_report_view(handle, source["report"])
                       if isinstance(source.get("report"), dict) else None),
            "report_error": source.get("report_error"),
        }

    if op == "worker.kill":
        return {
            "worker": handle,
            "killed": source.get("killed"),
            "changed": source.get("changed"),
            "interrupted": source.get("interrupted"),
            # Existing confirmed teardown returns resume_available, not resumable.
            "resumable": source.get("resume_available"),
            "state": source.get("state"),
        }

    if op in {"worker.compact", "worker.compact.status"}:
        keys = ("operation_id", "status", "receipt_status", "compacted",
                "recommended_action", "context_before", "context_after",
                "compact_turn_id", "receipt", "reason", "evidence", "input_surface")
        out = {"worker": handle, **{key: source.get(key) for key in keys}}
        if isinstance(out["evidence"], dict):
            out["evidence"] = {key: value for key, value in out["evidence"].items()
                               if key != "compact_turn_id"}
        return out

    if op in {"turn.send", "turn.reconcile"}:
        receipt = dict(source.get("delivery") or {})
        for field in ("delivery_id", "status", "pending", "owned", "settled",
                      "accepted", "route", "placement", "turn_ref", "turn_id",
                      "caller_metadata", "recommended_action"):
            if (field in source
                    and (op == "turn.reconcile" or field not in receipt)
                    and not (op == "turn.send" and field == "settled"
                             and "settlement" in receipt)):
                receipt[field] = source[field]
        if "status" not in receipt:
            receipt["status"] = "unknown"
        out = {"worker": handle, "delivery": receipt}
        if op == "turn.send":
            out["notification"] = source.get("notification")
        return out

    if op == "turn.cancel":
        receipt = dict(source.get("receipt") or {})
        for key in ("cancellation_id", "turn_ref", "status", "confirmed",
                    "owned", "accepted", "recommended_action", "outcome", "reason"):
            if key in source:
                receipt[key] = source[key]
        return {"worker": handle, "cancellation": receipt}

    if op in {"input.answer", "input.decide", "input.respond"}:
        receipt = dict(source.get("receipt") or {})
        for key in ("response_id", "request_id", "kind", "status", "owned",
                    "recommended_action", "reason", "evidence"):
            if key in source:
                receipt[key] = source[key]
        return {"worker": handle, "input": receipt,
                "notification": source.get("notification")}

    if op == "notification.inspect":
        return {"notification": dict(source)}

    if op == "worker.report":
        return _report_view(handle, source)

    if op == "worker.telemetry":
        # Its native per-engine identity blocks are intentionally not WorkerView.
        keys = ("telemetry_kind", "turns", "compactions", "totals", "coverage",
                "next_cursor", "has_more")
        return {"worker": handle,
                **{key: source.get(key) for key in keys}}

    if op == "worker.events":
        keys = ("events", "next_cursor", "has_more", "gap", "source", "timed_out",
                "runtime")
        return {"worker": handle,
                **{key: source.get(key) for key in keys}}

    if op == "account.usage.read":
        return {key: source.get(key) for key in
                ("provider", "observed_at_ms", "windows")}

    if op == "transcript.read":
        keys = ("transcript_locator", "conversation_schema_version", "turns",
                "completeness", "evidence", "source_revision", "source_changed",
                "next_cursor", "has_more")
        return {"worker": handle,
                **{key: source.get(key) for key in keys}}

    if op == "conversation.page":
        keys = ("records", "older_cursor", "completeness")
        return {"worker": handle,
                **{key: source.get(key) for key in keys}}

    return dict(source)


def _op_worker_fork(
    params: dict[str, Any], timeout: float, *,
    expected_generation: str | None = None,
    deadline: float | None = None,
) -> dict[str, Any]:
    return fork_worker(
        target=_required_text(params, "target"),
        worker_id=_optional_text(params, "worker_id", allow_null=False),
        name=_optional_text(params, "name"),
        model=_optional_text(params, "model"),
        effort=_optional_text(params, "effort"),
        timeout=timeout, expected_generation=expected_generation,
        deadline=deadline, created_via="local-json-v1",
    )


def _op_worker_wait(
    params: dict[str, Any], timeout: float
) -> dict[str, Any]:
    selectors: dict[str, Any] = {}
    if "next_turn" in params:
        selectors["next_turn"] = _optional_bool(params, "next_turn", False)
    if "after_turn" in params:
        selectors["after_turn"] = _optional_text(params, "after_turn")
    if "known_input" in params:
        selectors["known_input"] = _optional_text(params, "known_input")
    if selectors.get("after_turn") is not None and not selectors.get("next_turn"):
        raise InvalidRequest("after_turn requires next_turn=true")
    if selectors.get("known_input") is not None and not selectors.get("next_turn"):
        raise InvalidRequest("known_input requires next_turn=true")
    target = _required_text(params, "target")
    include_report = _optional_bool(params, "include_report", False)
    if "report_max_chars" in params and not include_report:
        raise InvalidRequest("report_max_chars requires include_report=true")
    report_max_chars = _optional_nullable_int(
        params, "report_max_chars", _DEFAULT_REPORT_MAX_CHARS
    )
    if not include_report:
        return wait_worker(target, timeout=timeout, **selectors)
    # Inclusions are read after the verdict, inside the same envelope budget,
    # so the caller needs no further process to learn them.
    deadline = time.monotonic() + timeout
    result = wait_worker(
        target,
        timeout=timeout - min(_INCLUDE_RESERVE_S, timeout / 4),
        **selectors,
    )
    if include_report:
        settled = result.get("last_settled_turn")
        turn_ref = (settled.get("turn_ref") if isinstance(settled, dict)
                    and settled.get("outcome") == "completed" else None)
        if turn_ref is None:
            result["report"] = None
            result["report_error"] = (
                "no positively completed exact turn" if settled else None
            )
        else:
            _include(result, "report", deadline, lambda remaining: report_worker(
                target,
                turn_ref=turn_ref,
                max_chars=report_max_chars,
                timeout=remaining,
            ))
    return result


def _include(
    result: dict[str, Any], key: str, deadline: float,
    read: Callable[[float], Any],
) -> None:
    """Attach one inclusion; the wait's verdict stands even when it fails.

    ``read`` gets what is left of the envelope budget; none left is a
    ``timeout`` inclusion error rather than a read past the caller's deadline.
    """
    try:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise OperationTimeout(f"no time left in the request budget for {key}")
        result[key] = read(remaining)
        result[f"{key}_error"] = None
    except Exception as error:
        result[key] = None
        result[f"{key}_error"] = _error_from_exception(None, error)["error"]


def _op_worker_park(
    params: dict[str, Any], timeout: float
) -> dict[str, Any]:
    return park_worker(_required_text(params, "target"), timeout=timeout)


def _rpc_compact_result(store, owner, operation_id: str, row: dict | None,
                       *, observing: bool) -> dict[str, Any]:
    """Project verified Claude compaction evidence."""
    from ccx.ops.compact_ops import get_operation, public_operation

    row = row or {}
    retained = get_operation(owner.worker, operation_id)
    raw = (retained or {}).get("status")
    proven = row.get("operation_status") or "unknown"
    if observing:
        status = "pending" if proven in ("accepted", "pasted") else proven
        compacted = (status == "succeeded"
                     if status in ("succeeded", "noop", "failed") else None)
        action = ("wait" if status in ("pending", "submitted")
                  else "inspect" if status in ("unknown", "not_found")
                  else "none")
    else:
        # Indeterminate compaction returns pending or unknown data,
        # and a native noop is settled succeeded without compacting.
        status = ("submitted" if proven in ("accepted", "pasted", "submitted")
                  else "succeeded" if proven == "noop" else proven)
        # Compaction reports a plain boolean; tri-state is status-only.
        compacted = proven == "succeeded"
        action = "wait" if status == "submitted" else "none"

    def native_tokens(name: str) -> dict | None:
        value = row.get(name)
        return {"tokens_used": value} if type(value) is int else None

    return {
        "worker": recorded_inspector(store, owner.worker),
        "operation_id": operation_id,
        "status": status,
        "receipt_status": raw,
        "compacted": compacted,
        "recommended_action": action,
        "context_before": native_tokens("pre_tokens"),
        "context_after": native_tokens("post_tokens"),
        "compact_turn_id": None,
        "receipt": public_operation(retained),
        "reason": row.get("reason"),
        "evidence": {
            "native_marker": "manual" if row.get("evidence_uuid") else "absent",
            "compact_turn_id": None, "event_id": row.get("evidence_uuid"),
        },
        # Without optional read-only host observation, never assert
        # current input-surface release, loaded state or native termination.
        "input_surface": {"compaction_ownership": "unknown",
                          "operation_id": None, "runtime_id": None},
    }


def _rpc_codex_compact_result(store, owner, operation_id: str,
                             row: dict, *, observing: bool) -> dict[str, Any]:
    """Project original Codex compaction evidence."""
    native_status = row.get("status") or "unknown"
    status = ("pending" if native_status == "prepared"
              else "failed" if native_status == "rejected"
              else native_status)
    if not observing and status == "pending":
        # Prepared means no proven native admission, not completed.
        status = "unknown"
    compacted = (status == "succeeded" if not observing else
                 True if status == "succeeded" else
                 False if status == "failed" else None)
    action = ("wait" if status in ("pending", "submitted")
              else "inspect" if observing and status in ("unknown", "not_found")
              else "none")
    before = row.get("contextBefore")
    after = row.get("contextAfter") if status == "succeeded" else None
    turn_id = row.get("compactTurnId")
    item_id = row.get("contextItemId")
    return {
        "worker": recorded_inspector(store, owner.worker),
        "operation_id": operation_id,
        "status": status,
        "receipt_status": row.get("receipt_status"),
        "compacted": compacted,
        "recommended_action": action,
        "context_before": before if isinstance(before, dict) else None,
        "context_after": after if isinstance(after, dict) else None,
        "compact_turn_id": turn_id,
        "receipt": row.get("receipt"),
        "reason": row.get("reason"),
        "evidence": {
            "native_marker": "contextCompaction" if isinstance(item_id, str)
                            and bool(item_id) else "unproven",
            "compact_turn_id": turn_id,
            "event_id": item_id,
        },
        "input_surface": {
            "compaction_ownership": "unknown",
            "operation_id": None, "runtime_id": None,
        },
    }


def _rpc_codex_compact(store, owner, operation_id: str, timeout: float) -> dict:
    """One core wait and at most ONE pure original-ID observation on loss."""
    from ccx.codex.ops import compact_worker as codex_compact_worker
    from ccx.codex.ops import compact_observation
    from ccx.store.root import NativeEffectIndeterminate

    try:
        row = codex_compact_worker(
            owner.worker, operation_id=operation_id, wait=True, timeout=timeout,
        )
    except NativeEffectIndeterminate:
        row = compact_observation(owner.worker, operation_id)
        if row["status"] == "not_found":
            row = {**row, "status": "unknown"}
    if row["status"] == "failed":
        cause = CompactFailed(
            row.get("reason") or "Codex native compact turn failed",
            short=owner.short, operation_id=operation_id,
        )
        raise DeliveryRejected(
            str(cause), short=owner.short, reason=str(cause),
            refusal="compact-failed", operation_id=operation_id,
        ) from cause
    if row["status"] == "rejected":
        raise DeliveryRejected(
            row.get("reason") or "Codex compact was refused before native admission",
            short=owner.short, refusal="native-rejected",
            operation_id=operation_id,
        )
    return _rpc_codex_compact_result(
        store, owner, operation_id, row, observing=False,
    )


def _op_worker_compact(
    params: dict[str, Any], timeout: float, *, store=None,
) -> dict[str, Any]:
    """Dispatch Claude compaction through its durable operation."""
    from ccx.constants import COMPACT_WAIT_DEFAULT_S
    from ccx.ops.compact_ops import get_operation
    from ccx.errors import CompactIndeterminate

    if store is None:
        raise SessionUnavailable("V1 compact requires the exact ACTIVE UUID dispatcher")
    owner = store.resolve(_required_text(params, "target"))
    token = _required_text(params, "operation_id")
    if owner.engine == "codex":
        return _rpc_codex_compact(store, owner, token, timeout)
    try:
        try:
            row = compact_worker(
                owner.worker, operation_id=token, timeout=timeout,
                execution_timeout=max(timeout or 0, COMPACT_WAIT_DEFAULT_S),
            )
        except CompactIndeterminate:
            # Exactly ONE existing original-UUID status reconciliation, not
            # a second paste, retry loop or new status evaluator.
            try:
                row = compact_status(owner.worker, operation_id=token)
            except (CompactIndeterminate, SessionNotFound):
                row = {"operation_status": "unknown", "reason":
                       "original compact terminal evidence unavailable"}
        # A boundary/failure race can be confirmed by that final status read.
        if row.get("operation_status") == "failed":
            from ccx.client import _compact_failed
            failure = _compact_failed(
                get_operation(owner.worker, token),
                short=owner.short, state=row.get("state"), operation_id=token,
            )
            raise failure
    except CompactFailed as failed:
        raise DeliveryRejected(
            str(failed), short=owner.short, reason=str(failed),
            state=getattr(failed, "state", None),
            refusal="compact-failed", operation_id=token,
        ) from failed
    return _rpc_compact_result(store, owner, token, row, observing=False)


def _op_turn_send(
    params: dict[str, Any], timeout: float
) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "delivery_id": _required_text(params, "delivery_id"),
        "timeout": timeout,
    }
    if "mode" in params:
        mode = _optional_text(params, "mode", allow_null=False)
        if mode not in {"new_turn_only", "auto"}:
            raise InvalidRequest(
                "turn.send cannot steer a running turn; use turn.steer instead"
            )
        kwargs["mode"] = mode
    if "notify_parent" in params:
        kwargs["notify_parent"] = _optional_bool(
            params, "notify_parent", False
        )
    if "caller_metadata" in params:
        kwargs["caller_metadata"] = _optional_object(
            params, "caller_metadata"
        )
    return send_turn(
        _required_text(params, "target"),
        _required_text(params, "text"),
        **kwargs,
    )


def _op_turn_reconcile(
    params: dict[str, Any], timeout: float
) -> dict[str, Any]:
    target = _required_text(params, "target")
    identity = {
        "delivery_id": _optional_text(params, "delivery_id"),
        "turn_id": _optional_text(params, "turn_id"),
    }
    if not _optional_bool(params, "hold_while_pending", False):
        return reconcile_turn(target, **identity, timeout=timeout)
    # Reconciliation is read-only, so holding is a bounded re-read of the
    # receipt; only the final answer pays for the worker snapshot.
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= _HOLD_POLL_S:
            return reconcile_turn(
                target, **identity, timeout=max(remaining, _HOLD_POLL_S)
            )
        probe = reconcile_turn(
            target, **identity, timeout=remaining, include_worker=False
        )
        # Only an in-flight delivery (`wait`) can progress on its own; a
        # `retry_same_id` receipt needs the caller, so holding it is pointless.
        if probe.get("pending") is not True or probe.get("recommended_action") != "wait":
            return reconcile_turn(
                target, **identity,
                timeout=max(deadline - time.monotonic(), _HOLD_POLL_S),
            )
        time.sleep(_HOLD_POLL_S)


def _op_turn_cancel(
    params: dict[str, Any], timeout: float
) -> dict[str, Any]:
    return cancel_turn(
        _required_text(params, "target"),
        cancellation_id=_required_text(params, "cancellation_id"),
        turn_ref=_required_text(params, "turn_ref"),
        timeout=timeout,
    )


def _op_input_answer(
    params: dict[str, Any], timeout: float
) -> dict[str, Any]:
    answers = params.get("answers")
    if not isinstance(answers, dict):
        raise InvalidRequest("answers must be a JSON object")
    return answer_input(
        _required_text(params, "target"),
        request_id=_required_text(params, "request_id"),
        response_id=_required_text(params, "response_id"),
        answers=answers,
        timeout=timeout,
        notify_parent=_optional_bool(params, "notify_parent", False),
    )


def _op_input_decide(
    params: dict[str, Any], timeout: float
) -> dict[str, Any]:
    return decide_input(
        _required_text(params, "target"),
        request_id=_required_text(params, "request_id"),
        response_id=_required_text(params, "response_id"),
        decision=_required_text(params, "decision"),
        reason=_optional_text(params, "reason"),
        timeout=timeout,
    )


def _op_input_respond(
    params: dict[str, Any], timeout: float
) -> dict[str, Any]:
    return respond_input(
        _required_text(params, "target"),
        request_id=_required_text(params, "request_id"),
        response_id=_required_text(params, "response_id"),
        action=_required_text(params, "action"),
        content=_optional_object(params, "content"),
        timeout=timeout,
    )


def _op_notification_inspect(
    params: dict[str, Any], timeout: float
) -> dict[str, Any]:
    del timeout
    return inspect_notification(_required_text(params, "event_id"))


def _op_worker_report(
    params: dict[str, Any], timeout: float
) -> dict[str, Any]:
    max_chars = _optional_nullable_int(params, "max_chars", _DEFAULT_REPORT_MAX_CHARS)
    return report_worker(
        _required_text(params, "target"),
        timeout=timeout,
        max_chars=max_chars,
        turn_ref=_optional_text(params, "turn_ref"),
    )


def _op_worker_telemetry(
    params: dict[str, Any], timeout: float
) -> dict[str, Any]:
    return telemetry_worker(
        _required_text(params, "target"),
        after_cursor=_optional_text(params, "after_cursor"),
        limit=_optional_int(params, "limit", 200),
        timeout=timeout,
    )


def _op_account_usage(
    params: dict[str, Any], timeout: float
) -> dict[str, Any]:
    return account_usage_read(
        engine=_required_text(params, "engine"),
        timeout=timeout,
    )


def _op_transcript_read(
    params: dict[str, Any], timeout: float
) -> dict[str, Any]:
    max_tool = _optional_nullable_int(
        params, "max_tool_result_bytes", 64 * 1024
    )
    return read_transcript(
        _required_text(params, "locator"),
        cursor=_optional_text(params, "cursor"),
        limit=_optional_int(params, "limit", 20),
        max_tool_result_bytes=max_tool,
        timeout=timeout,
    )


def _op_conversation_page(
    params: dict[str, Any], timeout: float
) -> dict[str, Any]:
    max_tool = _optional_nullable_int(
        params, "max_tool_result_bytes", 16 * 1024
    )
    return conversation_page(
        _required_text(params, "target"),
        cursor=_optional_text(params, "cursor"),
        source_record_target=_optional_int(
            params, "source_record_target", 40
        ),
        max_tool_result_bytes=max_tool,
        timeout=timeout,
    )


def _op_compact_status(params: dict[str, Any], timeout: float,
                       *, store=None) -> dict[str, Any]:
    """Strictly observational protocol status: original evidence, no journal write."""
    from ccx.errors import CompactIndeterminate

    if store is None:
        raise SessionUnavailable("V1 compact status requires the exact ACTIVE UUID dispatcher")
    owner = store.resolve(_required_text(params, "target"))
    token = _required_text(params, "operation_id")
    if owner.engine == "codex":
        from ccx.codex.ops import compact_observation
        from ccx.codex.runtime import daemon_rpc
        from ccx.constants import CODEXD_RPC_GENERATION

        row = compact_observation(owner.worker, token)
        result = _rpc_codex_compact_result(
            store, owner, token, row, observing=True,
        )
        # The existing codexd compact-observe is read-only, never starts
        # a daemon or changes native outcome. It may describe only current
        # input-surface ownership, and an unavailable daemon proves nothing.
        budget = min(.75, timeout)
        if budget > 0:
            try:
                live = daemon_rpc(
                    {"op": "compact-observe", "short": owner.worker_id},
                    timeout=budget,
                )
            except Exception:
                live = {}
            if (isinstance(live, dict) and live.get("ok") is True
                    and live.get("transport") == "served"
                    and live.get("rpcGeneration") == CODEXD_RPC_GENERATION
                    and isinstance(live.get("input_surface"), dict)):
                result["input_surface"] = {
                    **result["input_surface"],
                    **live["input_surface"], "source": "live",
                }
        return result
    try:
        row = compact_status(owner.worker, operation_id=token,
                             timeout=timeout, publish=False)
    except SessionNotFound:
        # A missing OPERATION is DATA; the OWNER was already proven above.
        row = {"operation_status": "not_found"}
    except CompactIndeterminate:
        row = {"operation_status": "unknown"}
    result = _rpc_compact_result(store, owner, token, row, observing=True)
    # Main's bounded read-only snapshot of a host that is already running:
    # compact-observe never reconciles, settles or writes. A cold or
    # unreachable host leaves the conservative unknown surface.
    from ccx.store.liveness import lifecycle_host_alive
    from ccx.rpc.rpc import _rpc

    lifecycle = store.record(owner.worker).get("lifecycle") or {}
    budget = min(.75, timeout)
    if budget > 0 and lifecycle.get("hostPid") and lifecycle_host_alive(lifecycle):
        try:
            live = _rpc(owner.worker, {"op": "compact-observe"}, timeout=budget)
        except Exception:
            live = {}
        if (isinstance(live, dict) and live.get("ok")
                and isinstance(live.get("input_surface"), dict)):
            result["input_surface"] = {**result["input_surface"],
                                       **live["input_surface"], "source": "live"}
    return result


def _api_error_details(error: WorkerApiError) -> dict[str, Any]:
    """The same snake_case shape ``worker.wait`` reports as ``api_error``."""
    return {
        "kind": error.kind,
        "status": error.status,
        "transient": error.transient,
        "rate_limited": isinstance(error, WorkerRateLimited),
        "request_id": error.request_id,
        "message": error.message,
        "reset_text": getattr(error, "reset_text", None),
    }


def _error_from_exception(
    request_id: str | None, error: BaseException, *,
    operation: str | None = None, engine: str | None = None,
) -> dict[str, Any]:
    details: dict[str, Any] = {}
    short = getattr(error, "short", None)
    if short is not None:
        details["short"] = short

    if isinstance(error, SendIndeterminate):
        details.update({
            "delivery_id": getattr(error, "delivery_id", None),
            "delivery_status": getattr(error, "delivery_status", None),
        })
        return _error(
            request_id, "reconcile_required", str(error), details=details
        )
    if isinstance(error, CursorInvalidated):
        return _error(
            request_id, "cursor_invalidated", str(error), details=details
        )
    if isinstance(error, OperationConflict):
        if error.reason is not None:
            details["reason"] = error.reason
        if error.worker_id is not None:
            details["worker_id"] = error.worker_id
        return _error(request_id, "conflict", str(error), details=details)
    if isinstance(error, CreatePartialFailure):
        details.update(worker_id=error.worker_id, worker=error.worker,
                       engine=error.engine, phase=error.phase,
                       delivery_id=error.delivery_id,
                       delivery_status=error.delivery_status,
                       notification_intent=error.notification_intent)
        # The claim flags are committed before post-create inspection; their
        # meaning is new/reused claim, not whether native activation ran.
        code = "partial_failure"
        if isinstance(error.info, dict):
            for key in ("created", "reconciled", "activation_state", "native_start_error"):
                if key in error.info:
                    details[key] = error.info[key]
            rejected = error.info.get("native_rejection")
            local = error.info.get("native_local_refusal")
            if isinstance(rejected, dict) and error.info.get("activation_state") == "rejected":
                details["native_rejection"] = dict(rejected)
                code = "native_rejected"
            elif (isinstance(local, dict) and local.get("proven") is True
                    and error.info.get("activation_state") == "local-refused"):
                details["native_local_refusal"] = dict(local)
                code = "local_refused"
        if error.cause is not None:
            details["cause_type"] = type(error.cause).__name__
        if isinstance(error.info, dict) and error.info.get("integrity_error") is True:
            details["integrity_error"] = True
        return _error(request_id, code, str(error), details=details)
    if isinstance(error, CapabilityUnsupported):
        engine = getattr(error, "engine", None)
        capability = getattr(error, "capability", None)
        if engine is not None:
            details["engine"] = engine
        if capability is not None:
            details["capability"] = capability
        details["engine"] = details.get("engine") or engine or "unknown"
        details["operation"] = operation or capability or "unknown"
        return _error(
            request_id,
            "unsupported_capability",
            str(error),
            details=details,
        )
    if isinstance(error, SessionNotFound):
        return _error(request_id, "not_found", str(error), details=details)
    if isinstance(error, SessionWaiting):
        waiting = getattr(error, "waiting_for", None)
        if waiting is not None:
            details["waiting_for"] = waiting
        return _error(
            request_id, "approval_waiting", str(error), details=details
        )
    if isinstance(error, DeliveryRejected):
        state = getattr(error, "state", None)
        if state is not None:
            details["state"] = normalize_native_state(state)
            details["native_state"] = state
        refusal = getattr(error, "refusal", None)
        if refusal is not None:
            details["refusal"] = refusal
        cause = error.__cause__
        if isinstance(cause, CompactFailed) and cause.api_error is not None:
            # A proven compaction failure caused by the API keeps its
            # classification, so a caller can tell a rate limit from a
            # conversation that can never compact.
            details["api_error"] = _api_error_details(cause.api_error)
        if refusal == "compaction-busy":
            details.update(retryable=True, recommended_action="wait")
            for key in ("delivery_id", "delivery_status", "operation_id"):
                if getattr(error, key, None) is not None:
                    details[key] = getattr(error, key)
        code = (
            "busy" if state == "busy" or refusal == "compaction-busy"
            else "approval_waiting" if state == "waiting"
            else "definitive_rejection"
        )
        return _error(request_id, code, str(error), details=details)
    if isinstance(error, ProviderProtocolError):
        return _error(
            request_id, "provider_protocol_error", str(error), details=details
        )
    if isinstance(error, (OperationTimeout, TurnIncomplete)):
        return _error(request_id, "timeout", str(error), details=details)
    if isinstance(error, WorkerApiError):
        details["api_error"] = _api_error_details(error)
        code = ("rate_limited" if isinstance(error, WorkerRateLimited)
                else "transient_api_error" if error.transient
                else "runtime_unavailable")
        return _error(request_id, code, str(error), details=details)
    if isinstance(error, BudgetStopped):
        return _error(request_id, "budget_stopped", str(error), details=details)
    if isinstance(error, SessionUnavailable):
        return _error(
            request_id, "runtime_unavailable", str(error), details=details
        )
    if isinstance(error, InvalidRequest):
        return _error(
            request_id, "invalid_request", str(error), details=details
        )
    if isinstance(error, CcxError):
        return _error(request_id, "ccx_error", str(error), details=details)
    return _error(
        request_id,
        "internal_error",
        f"{error.__class__.__name__}: {error}",
    )


def dispatch(request: Any, *, expected_generation: str | None = None) -> dict[str, Any]:
    """Direct Python Local protocol uses the shared full-UUID backend.

    Only the exact-owned operations advertised by dispatch_full_uuid run;
    no short-keyed or external-provider fallback is permitted.
    """
    from ccx.constants import CCX_ROOT
    from ccx.store.root import GenerationError, require_ccx_active, open_or_initialize
    # No filesystem access or legacy handler before a pure, typed protocol reply.
    pure = _full_uuid_pure_reply(request)
    if pure is not None:
        return pure
    request_id = request["request_id"]  # _full_uuid_pure_reply validated it
    deadline = time.monotonic() + _timeout_seconds(request, DEFAULT_TIMEOUT_S)
    try:
        store = (require_ccx_active(CCX_ROOT, expected_generation=expected_generation)
                 if expected_generation is not None
                 else open_or_initialize(CCX_ROOT)[0])
    except (GenerationError, OSError) as exc:
        return _error(
            request_id, "state_unavailable",
            f"full-UUID ACTIVE generation required: {exc}",
        )
    return dispatch_full_uuid(request, store, deadline=deadline)


def _serialized(response: dict[str, Any]) -> bytes:
    try:
        raw = (
            json.dumps(
                response,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError):
        request_id = response.get("request_id")
        response = _error(
            request_id if isinstance(request_id, str) else None,
            "internal_error",
            "protocol response contains a value that cannot be encoded as JSON",
        )
        raw = (
            json.dumps(
                response, allow_nan=False, separators=(",", ":"), sort_keys=True
            )
            + "\n"
        ).encode("utf-8")
    if len(raw) <= MAX_MESSAGE_BYTES:
        return raw
    request_id = response.get("request_id")
    smaller = _error(
        request_id if isinstance(request_id, str) else None,
        "response_too_large",
        "protocol response exceeds the 4 MiB protocol limit",
        details={"limit_bytes": MAX_MESSAGE_BYTES},
    )
    return (
        json.dumps(
            smaller, allow_nan=False, separators=(",", ":"), sort_keys=True
        ) + "\n"
    ).encode("utf-8")


def _read_request() -> dict[str, Any]:
    stream = getattr(sys.stdin, "buffer", sys.stdin)
    raw = stream.read(MAX_MESSAGE_BYTES + 1)
    if isinstance(raw, str):
        raw = raw.encode("utf-8")
    return parse_request(raw)


def parse_request(raw: bytes) -> dict[str, Any]:
    """Decode one request envelope with the protocol size and JSON rules."""
    if len(raw) > MAX_MESSAGE_BYTES:
        raise InvalidRequest(
            "protocol request exceeds the 4 MiB protocol limit"
        )
    if not raw.strip():
        raise InvalidRequest("protocol request is empty")
    try:
        value = json.loads(
            raw,
            parse_constant=lambda value: (_ for _ in ()).throw(
                ValueError(f"non-standard JSON constant: {value}")
            ),
        )
    except (ValueError, UnicodeError) as error:
        raise InvalidRequest("protocol request is not valid JSON") from error
    if not isinstance(value, dict):
        raise InvalidRequest("request must be a JSON object")
    return value


def respond(
    read: Callable[[], dict[str, Any]], *, expected_generation: str | None = None,
) -> bytes:
    """One serialized response line for the request ``read`` yields."""
    request_id = None
    try:
        request = read()
        raw_id = request.get("request_id")
        if isinstance(raw_id, str):
            request_id = raw_id
        response = dispatch(request, expected_generation=expected_generation)
    except Exception as error:
        response = _error_from_exception(request_id, error)
    return _serialized(response)


def cmd_rpc(args) -> None:
    """CLI adapter for Local protocol requests on stdin."""
    if getattr(args, "serve", False):
        from ccx.rpc.serve import serve

        raise SystemExit(serve())
    out = getattr(sys.stdout, "buffer", sys.stdout)
    payload = respond(_read_request)
    if hasattr(out, "write"):
        try:
            out.write(payload)
        except TypeError:
            out.write(payload.decode("utf-8"))
    if hasattr(out, "flush"):
        out.flush()


__all__ = [
    "MAX_MESSAGE_BYTES",
    "PROTOCOL_VERSION",
    "SUPPORTED_PROTOCOL_VERSIONS",
    "cmd_rpc",
    "dispatch",
    "parse_request",
    "respond",
]


# --- Full-UUID Local protocol operations and native receipt dispatch -----------
FULL_UUID_OBSERVATIONAL_OPS = frozenset({
    "system.info", "worker.inspect", "worker.events", "worker.wait",
    "worker.compact.status", "account.usage.read", "worker.telemetry",
    "conversation.page", "transcript.read", "notification.inspect",
})
# A create request is permitted to reserve one original identity and report
# an explicit PARTIAL native-activation outcome, never native success.
FULL_UUID_PREPARATION_OPS = frozenset({"worker.create", "worker.fork"})
# Reused handlers for the full-UUID native/receipt operations.
FULL_UUID_NATIVE_OPS = frozenset({
    "turn.send", "turn.reconcile", "turn.cancel", "worker.report", "worker.kill",
    "worker.compact", "input.answer", "input.decide", "input.respond",
})


_FULL_UUID_SUPPORTED_OPS = (
    FULL_UUID_OBSERVATIONAL_OPS | FULL_UUID_PREPARATION_OPS | FULL_UUID_NATIVE_OPS
)


def _full_uuid_pure_reply(request: Any) -> dict[str, Any] | None:
    """protocol envelope/error precedence and capabilities, without root I/O.

    Returns None only for validated, supported STATEFUL operations. The existing
    dispatch_full_uuid remains their sole native/receipt dispatcher. Preserve
    the old order: ID, protocol version, op, params shape, supported-op check,
    allowed params, timeout. An unsupported op is not a legacy fallback.
    """
    request_id = None
    try:
        if not isinstance(request, dict):
            raise InvalidRequest("request must be a JSON object")
        request_id = _request_id(request.get("request_id"))
        version = request.get("protocol_version")
        if version != PROTOCOL_VERSION:
            return _error(
                request_id, "unsupported_protocol_version",
                f"unsupported protocol_version {version!r}",
                details={"supported_protocol_versions": [2]},
            )
        op = request.get("op")
        if not isinstance(op, str) or not op:
            raise InvalidRequest("op is required")
        params = _params(request)
        if op not in _FULL_UUID_SUPPORTED_OPS:
            return _error(
                request_id, "unsupported_operation",
                f"operation {op!r} is not supported",
                details={"operation": op},
            )
        _validate_operation_params(op, params)
        if op in ("worker.compact", "worker.compact.status"):
            # The protocol owner needs the caller's exact operation identity even
            # before an ACTIVE store exists; never silently mint one.
            _required_text(params, "target")
            _required_text(params, "operation_id")
        if op in ("input.answer", "input.decide", "input.respond"):
            # The original gate and caller-known response identity precede
            # any ACTIVE root or native input side effect.
            _required_text(params, "target")
            _required_text(params, "request_id")
            _required_text(params, "response_id")
        _timeout_seconds(request, DEFAULT_TIMEOUT_S)
        if op == "system.info":
            return _success(request_id, {"protocol_version": 2, "ccx_version": ccx_version()})
        return None
    except InvalidRequest as error:
        return _error(request_id, "invalid_request", str(error))


def dispatch_full_uuid(request: Any, store, *, deadline: float | None = None) -> dict[str, Any]:
    """Exact-owned Local protocol native receipt subset with truthful capabilities.

    Reuses the existing protocol turn.send/reconcile/worker.report operation
    parsers with the globally owned full-UUID host/codexd native receipt
    implementation. No external Claude or short/name alias fallback.
    """
    from ccx.store.root import GenerationError, StaleGeneration, StorageIntegrityError
    from ccx.store.store import GenerationStore
    from ccx.store.identity import (
        EngineMismatch, InvalidTarget, UnknownWorker,
    )
    from ccx.ops.worker_events import EventCursorInvalid, wait_full_uuid_event_page

    request_id = None
    if deadline is None and isinstance(request, dict):
        # Direct one-shot protocol calls use the same validated end-to-end budget.
        try:
            deadline = time.monotonic() + _timeout_seconds(request, DEFAULT_TIMEOUT_S)
        except InvalidRequest as exc:
            return _error(request.get("request_id"), "invalid_request", str(exc))
    try:
        if not isinstance(store, GenerationStore):
            raise StorageIntegrityError("CCX worker records are unavailable; check CCX_ROOT")
        store.revalidate()
        pure = _full_uuid_pure_reply(request)
        if pure is not None:
            return pure
        # A typed/complete stateful protocol envelope was validated above. Runtime
        # generation/owner/source and receipt checks remain required below.
        request_id = request["request_id"]
        op = request["op"]
        params = _params(request)
        if op == "worker.create":
            from ccx.ops.create import create_worker
            from ccx.store.identity import InvalidTarget, canonical_worker_id
            from ccx.errors import CreatePartialFailure, OperationConflict
            if "worker_id" in params:
                try:
                    canonical_worker_id(params["worker_id"])
                except InvalidTarget as exc:
                    raise InvalidRequest(
                        f"worker_id must be complete canonical UUIDv4: {exc}"
                    ) from exc
            engine = params.get("engine")
            cwd = params.get("cwd")
            if engine not in ("claude", "codex"):
                raise InvalidRequest("worker.create engine must be claude or codex")
            if not isinstance(cwd, str) or not cwd:
                raise InvalidRequest("worker.create cwd must be explicit")
            # For a newly minted ID the durable returned partial error must
            # carry the exact chosen worker_id, not a display-name retry.
            budget = (deadline - time.monotonic()) if deadline is not None else DEFAULT_TIMEOUT_S
            if budget <= 0:
                raise OperationTimeout("worker.create request deadline elapsed before native creation")
            result = create_worker(
                engine=engine, cwd=cwd, worker_id=params.get("worker_id"),
                name=params.get("name"), model=params.get("model"),
                effort=params.get("effort"),
                idle_timeout=params.get("idle_timeout", DEFAULT_IDLE_TIMEOUT_S),
                permissions=params.get("permissions"),
                sandbox=params.get("sandbox"),
                approval_policy=params.get("approval_policy"),
                created_via="local-json-v1", timeout=budget,
                expected_generation=store.generation, deadline=deadline,
            )
        elif op == "worker.fork":
            from ccx.store.identity import InvalidTarget, canonical_worker_id
            if "worker_id" in params:
                try:
                    canonical_worker_id(params["worker_id"])
                except InvalidTarget as exc:
                    raise InvalidRequest(
                        f"worker_id must be complete canonical UUIDv4: {exc}"
                    ) from exc
            budget = (deadline - time.monotonic()) if deadline is not None else DEFAULT_TIMEOUT_S
            if budget <= 0:
                raise OperationTimeout("worker.fork request deadline elapsed before native creation")
            result = _op_worker_fork(
                params, budget, expected_generation=store.generation, deadline=deadline,
            )
        elif op in FULL_UUID_NATIVE_OPS:
            # Exact original protocol schema validation ran above; the same old
            # operation functions now invoke protocol_ops' real UUID-native
            # host/codexd and semantic receipt implementations.
            original_handlers = {
                "turn.send": _op_turn_send,
                "turn.reconcile": _op_turn_reconcile,
                "turn.cancel": _op_turn_cancel,
                "worker.kill": _op_worker_park,
                "worker.report": _op_worker_report,
                "input.answer": _op_input_answer,
                "input.decide": _op_input_decide,
                "input.respond": _op_input_respond,
            }
            budget = _timeout_seconds(request, DEFAULT_TIMEOUT_S)
            if op == "worker.compact":
                result = _op_worker_compact(params, budget, store=store)
            else:
                result = original_handlers[op](params, budget)
        elif op == "worker.compact.status":
            result = _op_compact_status(
                params, _timeout_seconds(request, DEFAULT_TIMEOUT_S), store=store,
            )
        elif op == "notification.inspect":
            result = _op_notification_inspect(
                params, _timeout_seconds(request, DEFAULT_TIMEOUT_S),
            )
        elif op == "account.usage.read":
            result = _op_account_usage(
                params, _timeout_seconds(request, DEFAULT_TIMEOUT_S),
            )
        elif op == "worker.telemetry":
            result = _op_worker_telemetry(
                params, _timeout_seconds(request, DEFAULT_TIMEOUT_S),
            )
        elif op == "worker.wait":
            budget = max(0.0, deadline - time.monotonic())
            result = _op_worker_wait(params, budget)
        elif op == "conversation.page":
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise OperationTimeout("conversation page request deadline expired")
            result = _op_conversation_page(params, remaining)
        elif op == "transcript.read":
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise OperationTimeout("transcript read request deadline expired")
            result = _op_transcript_read(params, remaining)
        elif op == "worker.inspect":
            target = params.get("target")
            if not isinstance(target, str) or not target:
                raise InvalidRequest("Target must be a complete CCX worker handle")
            result = recorded_inspector(store, target)
        else:
            target = params.get("target")
            if not isinstance(target, str) or not target:
                raise InvalidRequest("Target must be a complete CCX worker handle")
            wait_ms = params.get("wait_ms", 0)
            if type(wait_ms) is not int or wait_ms < 0:
                raise InvalidRequest("worker.events wait_ms must be a nonnegative integer")
            remaining = max(0.0, deadline - time.monotonic())
            reserve = min(1.0, remaining / 4)
            result = wait_full_uuid_event_page(
                store, target,
                after=params.get("after_cursor"), tail=params.get("tail"),
                limit=params.get("limit", 200), kinds=params.get("kinds"),
                wait=min(wait_ms / 1000.0, max(0.0, remaining - reserve)),
            )
        store.revalidate()
        return _success(request_id, _rpc_result(op, result, store, params))
    except EventCursorInvalid as error:
        return _error(request_id, "cursor_invalidated", str(error))
    except UnknownWorker as error:
        return _error(request_id, "not_found", str(error))
    except Exception as error:
        from ccx.store.root import NativeEffectIndeterminate
        from ccx.claude.delivery import ClaudeDeliveryConflict
        from ccx.codex.delivery import DeliveryReceiptConflict
        from ccx.errors import CcxError
        if isinstance(error, NativeEffectIndeterminate):
            params = request.get("params") if isinstance(request,dict) else {}
            params = params if isinstance(params,dict) else {}
            return _error(
                request_id, "reconcile_required", str(error),
                details={
                    "delivery_id": params.get("delivery_id"),
                    "delivery_status": "submitted-unknown",
                    "target": params.get("target"),
                    "recommended_action": "reconcile_original_never_resend",
                },
            )
        if isinstance(error,(ClaudeDeliveryConflict,DeliveryReceiptConflict)):
            return _error(request_id,"conflict",str(error))
        if isinstance(error,CcxError):
            try:
                engine = (getattr(error, "engine", None)
                          or params.get("engine")
                          or store.resolve(params["target"]).engine
                          if isinstance(params, dict) and params.get("target")
                          else getattr(error, "engine", None)
                          or (params.get("engine") if isinstance(params, dict) else None))
            except Exception:
                engine = getattr(error, "engine", None)
            return _error_from_exception(request_id,error,
                                         operation=op, engine=engine)
        if isinstance(error, (InvalidTarget, EngineMismatch, InvalidRequest)):
            return _error(request_id,"invalid_request",str(error))
        if isinstance(error,(StaleGeneration,GenerationError)):
            return _error(request_id,"state_unavailable",str(error))
        return _error(
            request_id,"internal_error",
            f"{error.__class__.__name__}: {error}",
        )
