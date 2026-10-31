"""Controller-attempt observations for the shared parent notification daemon."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ccx.controllers import store as store
from ccx.errors import ControllerError


def _attempt_for_registration(
    record: dict[str, Any], reg: dict[str, Any]
) -> dict[str, Any] | None:
    operation = reg.get("operation") if isinstance(reg.get("operation"), dict) else {}
    number = operation.get("attempt")
    token = operation.get("attemptToken")
    if not isinstance(token, str) or not token:
        return None
    for attempt in record.get("attempts") or []:
        if not isinstance(attempt, dict) or attempt.get("number") != number:
            continue
        if attempt.get("attemptToken") != token:
            continue
        return attempt
    return None


def _startup_deadline(
    reg: dict[str, Any],
    attempt: dict[str, Any] | None = None,
) -> int:
    operation = (
        reg.get("operation")
        if isinstance(reg.get("operation"), dict)
        else {}
    )
    deadline = int(operation.get("startupDeadlineMs") or 0)
    if isinstance(attempt, dict):
        deadline = max(
            deadline,
            int(attempt.get("startedAt") or 0) + 45_000,
        )
    return deadline


def _record(reg: dict[str, Any]) -> dict[str, Any] | None:
    run_id = reg["worker"]["workerId"]
    try:
        bootstrap = store.bootstrap_root() / run_id
        run_dir = (bootstrap if bootstrap.is_dir() else next(
            (path for path in store._all_run_dirs() if path.name == run_id), None
        ))
        if run_dir is None:
            raise ControllerError("original controller run is unavailable")
        record = store.read_json(store.run_record_path(run_dir))
        if record.get("runId") != run_id:
            raise ControllerError("controller run identity differs from notification")
        attempt = _attempt_for_registration(record, reg)
        if (
            isinstance(attempt, dict)
            and attempt.get("lifecycle") in store.TERMINAL_LIFECYCLES
        ):
            return record

        # A registration can deliberately precede attempt publication. While
        # that bounded startup window is open, missing attempt state is pending.
        # After the window, settlement/progress will turn a missing or token-
        # mismatched attempt into a structural unavailable result. Never inspect
        # a later attempt's liveness on behalf of this registration.
        if attempt is None:
            return record

        # An earlier unfinished attempt must never sample a newer host's PID.
        if (record.get("attempts") or [])[-1] is not attempt:
            return record

        lifecycle = record.get("lifecycle") or {}
        deadline = _startup_deadline(reg, attempt)
        if (
            lifecycle.get("state") == "starting"
            and not lifecycle.get("pid")
            and store.now_ms() < deadline
        ):
            return record
        return store.refresh_liveness(run_dir)
    except (ControllerError, OSError):
        # Bootstrap publication can temporarily hide the directory. After the
        # already recorded deadline, keep this event visible as unavailable.
        if store.now_ms() < _startup_deadline(reg):
            return None
        return {"runId": run_id, "controller": reg["worker"].get("name"),
                "attempts": [], "reports": []}


def settlement(reg: dict[str, Any]) -> dict[str, Any] | None:
    record = _record(reg)
    if record is None:
        return None
    number = reg["operation"]["attempt"]
    store.snapshot_attempt_outcome(record)
    attempt = _attempt_for_registration(record, reg)
    if attempt is None:
        if store.now_ms() < _startup_deadline(reg):
            return None
        token = reg.get("operation", {}).get("attemptToken")
        return {
            "status": "unavailable",
            "workerState": "unavailable",
            "operationType": "controller-attempt",
            "operationId": f"{record['runId']}:{number}",
            "controller": record.get("controller"),
            "runId": record["runId"],
            "attempt": number,
            "controllerStatus": None,
            "checkpoint": None,
            "summary": None,
            "report": None,
            "reportTruncated": False,
            "reportPath": None,
            "error": {
                "type": "ControllerAttemptUnavailable",
                "message": (
                    "controller attempt was not durably published"
                    if token
                    else "controller attempt is unavailable"
                ),
            },
        }
    if attempt.get("lifecycle") not in store.TERMINAL_LIFECYCLES:
        return None
    outcome = attempt.get("outcome") or {}
    metadata = outcome.get("report") or {}
    report = None
    path = metadata.get("path")
    error = outcome.get("error")
    if path:
        try:
            report = Path(path).read_text(encoding="utf-8")
        except OSError as exc:
            error = error or {"type": "ControllerReportUnavailable", "message": str(exc)}
    from ccx.notify.settle import _truncate_report
    report, truncated = _truncate_report(report)
    return {
        "status": attempt["lifecycle"],
        "workerState": attempt["lifecycle"],
        "operationType": "controller-attempt",
        "operationId": f"{record['runId']}:{number}",
        "controller": record.get("controller"),
        "runId": record["runId"],
        "attempt": number,
        "controllerStatus": outcome.get("status"),
        "checkpoint": outcome.get("checkpoint"),
        "summary": outcome.get("summary"),
        "report": report,
        "reportTruncated": truncated,
        "reportPath": path,
        "error": error,
    }


def progress(reg: dict[str, Any]) -> dict[str, Any]:
    record = _record(reg)
    result: dict[str, Any] = {
        "elapsedSeconds": max(0, int((store.now_ms() - reg["createdAtMs"]) / 1000)),
        "attempt": reg["operation"]["attempt"],
    }
    if record is None:
        return {**result, "workerState": "unknown", "health": "record-unavailable"}
    store.snapshot_attempt_outcome(record)
    attempt = _attempt_for_registration(record, reg)
    if attempt is None:
        if store.now_ms() >= _startup_deadline(reg):
            return {
                **result,
                "workerState": "unavailable",
                "health": "attempt-unavailable",
                "controller": record.get("controller"),
                "runId": record["runId"],
            }
        return {
            **result,
            "workerState": "starting",
            "health": "attempt-pending-publication",
            "controller": record.get("controller"),
            "runId": record["runId"],
        }
    if attempt.get("outcome") is not None:
        outcome = attempt["outcome"]
        return {
            **result, "workerState": attempt["lifecycle"],
            "controller": record.get("controller"), "runId": record["runId"],
            "controllerStatus": outcome.get("status"),
            "checkpoint": outcome.get("checkpoint"),
        }
    if (record.get("attempts") or [])[-1] is not attempt:
        return {
            **result, "workerState": attempt.get("lifecycle") or "unavailable",
            "controller": record.get("controller"), "runId": record["runId"],
        }
    reports = (record.get("reports") or [])[int(attempt.get("reportStartIndex", 0)):]
    latest = reports[-1] if reports else {}
    tasks = list((record.get("tasks") or {}).values())
    return {
        **result,
        "workerState": (record.get("lifecycle") or {}).get("state"),
        "controller": record.get("controller"),
        "runId": record["runId"],
        "controllerStatus": latest.get("status"),
        "checkpoint": latest.get("checkpoint"),
        "taskCount": len(tasks),
        "activeTasks": [t.get("name") or t.get("id") for t in tasks if t.get("state") == "running"][:4],
    }
