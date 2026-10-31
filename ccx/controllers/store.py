"""Durable storage for task-specific Python controller runs.

Controller records are deliberately independent from Claude and Codex worker
records.  They retain qualified worker handles, never duplicate or own worker
lifecycle state.
"""

from __future__ import annotations

from contextlib import contextmanager
import copy
import fcntl
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time
import uuid
from typing import Any, Callable, Iterator

from ccx import constants
from ccx.system import oslayer
from ccx.errors import ControllerError, ControllerRunNotFound


CONTROLLER_SCHEMA_VERSION = 1
# Operation ordinals changed when WorkerClient.steer/kill became observable.
# Missing/unknown layout markers MUST NOT silently adopt the new slot order.
CONTROLLER_OPERATION_JOURNAL_LAYOUT = 3
TERMINAL_LIFECYCLES = frozenset({"succeeded", "failed", "stopped", "unavailable"})

#: Mechanical form each execution attempt began with.  These describe only the
#: CLI shape that started the attempt, never why an orchestrator started it.
ATTEMPT_START_INITIAL = "initial"
ATTEMPT_START_RESUME = "resume"
ATTEMPT_START_RESUME_NOTE = "resume-note"
ATTEMPT_START_RESUME_WORKER_COMMENT = "resume-worker-comment"
ATTEMPT_START_FORMS = frozenset({
    ATTEMPT_START_INITIAL,
    ATTEMPT_START_RESUME,
    ATTEMPT_START_RESUME_NOTE,
    ATTEMPT_START_RESUME_WORKER_COMMENT,
})


def now_ms() -> int:
    return int(time.time() * 1000)


def controllers_root() -> Path:
    return constants.CCX_ROOT / "controllers"


def bootstrap_root() -> Path:
    return controllers_root() / ".bootstrap"


def launch_root() -> Path:
    return controllers_root() / ".launch"


def stop_marker_path(run_id: str) -> Path:
    return launch_root() / f"{run_id}.stopped"


def clear_stop_marker(run_id: str) -> None:
    try:
        stop_marker_path(run_id).unlink()
    except FileNotFoundError:
        pass


def ensure_controller_dirs() -> None:
    from ccx.store.paths import private_mkdir

    for path in (controllers_root(), bootstrap_root(), launch_root()):
        private_mkdir(path)


def validate_controller_name(name: str) -> str:
    if not isinstance(name, str) or not name.strip():
        raise ControllerError("controller name must be a non-empty string")
    value = name.strip()
    if len(value) > 80:
        raise ControllerError("controller name must be at most 80 characters")
    if value.startswith(".") or os.sep in value or (os.altsep and os.altsep in value):
        raise ControllerError(
            f"controller name must be a single path segment, got {name!r}"
        )
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ControllerError("controller name cannot contain control characters")
    return value


def source_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


@contextmanager
def file_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise ControllerError(f"controller state not found: {path}") from error
    except (OSError, json.JSONDecodeError) as error:
        raise ControllerError(f"cannot read controller state {path}: {error}") from error
    if not isinstance(value, dict):
        raise ControllerError(f"controller state is not a JSON object: {path}")
    return value


def write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(
        dir=path.parent, prefix=f".{path.name}.", suffix=".tmp"
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def append_ndjson(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
    with path.open("ab", buffering=0) as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            handle.write(line)
            os.fsync(handle.fileno())
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def run_record_path(run_dir: Path) -> Path:
    return run_dir / "run.json"


def require_operation_journal_layout(
    run_dir: Path, record: dict[str, Any] | None = None,
) -> None:
    """Refuse replay across incompatible positional WorkerClient journals.

    Never repair, infer or auto-upgrade old runs: operations absent from their
    journals (notably steer/kill) may already have mutated a real worker.
    """
    if record is None:
        record = read_json(run_record_path(run_dir))
    found = record.get("operationJournalLayout")
    if type(found) is int and found == CONTROLLER_OPERATION_JOURNAL_LAYOUT:
        return
    label = "missing" if found is None else repr(found)
    raise ControllerError(
        f"controller run {record.get('runId') or run_dir.name} uses "
        f"unsupported operation journal layout {label} "
        f"(required {CONTROLLER_OPERATION_JOURNAL_LAYOUT}); start a new run."
    )


def mutate_run(run_dir: Path, update: Callable[[dict[str, Any]], None]) -> dict[str, Any]:
    with file_lock(run_dir / "state.lock"):
        record = read_json(run_record_path(run_dir))
        snapshot_attempt_outcome(record)
        update(record)
        snapshot_attempt_outcome(record)
        record["updatedAt"] = now_ms()
        write_json_atomic(run_record_path(run_dir), record)
        return copy.deepcopy(record)


def snapshot_attempt_outcome(record: dict[str, Any]) -> None:
    """Keep a settled attempt's report stable across later resumes."""
    attempts = record.get("attempts") or []
    if not attempts:
        return
    attempt = attempts[-1]
    state = attempt.get("lifecycle")
    if state not in TERMINAL_LIFECYCLES or "outcome" in attempt:
        return
    reports = record.get("reports") or []
    reports = reports[int(attempt.get("reportStartIndex", 0)):]
    report = reports[-1] if reports else None
    attempt["outcome"] = copy.deepcopy({
        "status": report.get("status") if report else ("completed" if state == "succeeded" else None),
        "checkpoint": report.get("checkpoint") if report else None,
        "summary": report.get("summary") if report else None,
        "report": report,
        "error": record.get("error"),
    })


def append_event(run_dir: Path, event_type: str, **data: Any) -> None:
    append_ndjson(
        run_dir / "events.ndjson",
        {
            "schemaVersion": CONTROLLER_SCHEMA_VERSION,
            "type": event_type,
            "ts": now_ms(),
            **data,
        },
    )


def create_bootstrap(
    script: Path,
    launch_cwd: Path,
    argv: list[str],
    *,
    controller_name: str | None = None,
) -> tuple[str, Path]:
    requested_name = (
        validate_controller_name(controller_name)
        if controller_name is not None
        else None
    )
    ensure_controller_dirs()
    run_id = str(uuid.uuid4())
    directory = bootstrap_root() / run_id
    directory.mkdir(parents=True)
    for child in ("reports", "tasks", "worktrees", "operations"):
        (directory / child).mkdir()
    created = now_ms()
    record = {
        "schemaVersion": CONTROLLER_SCHEMA_VERSION,
        "operationJournalLayout": CONTROLLER_OPERATION_JOURNAL_LAYOUT,
        "runId": run_id,
        "controller": None,
        "requestedControllerName": requested_name,
        "declaredControllerName": None,
        "scriptPath": str(script),
        "sourceSha256": source_sha256(script),
        "launchCwd": str(launch_cwd),
        "workspace": str(launch_cwd),
        "createdAt": created,
        "updatedAt": created,
        "lifecycle": {
            "state": "starting", "since": created, "pid": None,
            "pidStart": None,
        },
        "status": None,
        "checkpoint": None,
        "summary": None,
        "latestReport": None,
        "reports": [],
        "result": None,
        "error": None,
        "attempts": [{
            "number": 1,
            "attemptToken": str(uuid.uuid4()),
            "reportStartIndex": 0,
            "startedAt": created,
            "argv": list(argv),
            "startForm": ATTEMPT_START_INITIAL,
            "startInputId": None,
        }],
        "tasks": {},
        "workers": [],
        "inputs": 0,
        "worktrees": {},
    }
    write_json_atomic(run_record_path(directory), record)
    append_event(directory, "run-created", runId=run_id, scriptPath=str(script))
    return run_id, directory


def _controller_dirs() -> list[Path]:
    root = controllers_root()
    if not root.is_dir():
        return []
    return sorted(
        (
            path
            for path in root.iterdir()
            if path.is_dir() and not path.name.startswith(".")
        ),
        key=lambda path: path.name.casefold(),
    )


def _controller_entry_by_name(name: str) -> tuple[Path, dict[str, Any]] | None:
    folded = name.casefold()
    matches: list[tuple[Path, dict[str, Any]]] = []
    for directory in _controller_dirs():
        try:
            record = read_json(directory / "controller.json")
        except ControllerError:
            continue
        if str(record.get("name", "")).casefold() == folded:
            matches.append((directory, record))
    if len(matches) > 1:
        raise ControllerError(f"ambiguous controller name: {name!r}")
    return matches[0] if matches else None


def publish_bootstrap(
    bootstrap: Path,
    *,
    name: str,
    declared_name: str | None = None,
    script: Path,
    workspace: Path,
) -> Path:
    name = validate_controller_name(name)
    if declared_name is not None:
        declared_name = validate_controller_name(declared_name)
    script = script.resolve()
    workspace = workspace.resolve()
    if not workspace.is_dir():
        raise ControllerError(f"controller workspace is not a directory: {workspace}")
    with file_lock(controllers_root() / ".controllers.lock"):
        existing = _controller_entry_by_name(name)
        if existing is not None:
            directory, controller_record = existing
            bound = Path(str(controller_record.get("scriptPath", ""))).resolve()
            if bound != script:
                raise ControllerError(
                    f"controller name {name!r} is already bound to {bound}; "
                    f"choose a different controller name for {script}"
                )
            name = str(controller_record["name"])
        else:
            directory = controllers_root() / name
            directory.mkdir(parents=True)
            created = now_ms()
            controller_record = {
                "schemaVersion": CONTROLLER_SCHEMA_VERSION,
                "name": name,
                "scriptPath": str(script),
                "createdAt": created,
                "updatedAt": created,
            }
            write_json_atomic(directory / "controller.json", controller_record)
        (directory / "runs").mkdir(exist_ok=True)
        record = read_json(run_record_path(bootstrap))
        run_id = str(record["runId"])
        destination = directory / "runs" / run_id
        if destination.exists():
            raise ControllerError(f"controller run already exists: {run_id}")
        record["controller"] = name
        record["declaredControllerName"] = declared_name
        record["workspace"] = str(workspace)
        write_json_atomic(run_record_path(bootstrap), record)
        os.replace(bootstrap, destination)
        controller_record["updatedAt"] = now_ms()
        controller_record["latestRunId"] = run_id
        write_json_atomic(directory / "controller.json", controller_record)
    append_event(
        destination, "run-published", controller=name,
        declaredControllerName=declared_name, workspace=str(workspace),
    )
    return destination


def launch_verdict_path(run_id: str) -> Path:
    return launch_root() / f"{run_id}.json"


def write_launch_verdict(run_id: str, value: dict[str, Any]) -> None:
    ensure_controller_dirs()
    write_json_atomic(launch_verdict_path(run_id), value)


def read_launch_verdict(run_id: str) -> dict[str, Any] | None:
    path = launch_verdict_path(run_id)
    if not path.exists():
        return None
    try:
        return read_json(path)
    except ControllerError:
        return None


def controller_inventory() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for directory in _controller_dirs():
        try:
            controller_record = read_json(directory / "controller.json")
        except ControllerError:
            continue
        runs = run_inventory(str(controller_record.get("name", directory.name)))
        latest = runs[0] if runs else None
        rows.append({
            "name": controller_record.get("name", directory.name),
            "scriptPath": controller_record.get("scriptPath"),
            "runs": len(runs),
            "latest": latest,
        })
    return sorted(rows, key=lambda row: str(row["name"]).casefold())


def run_inventory(name: str) -> list[dict[str, Any]]:
    entry = _controller_entry_by_name(name)
    if entry is None:
        raise ControllerError(f"controller not found: {name}")
    directory, _record = entry
    rows: list[dict[str, Any]] = []
    runs = directory / "runs"
    if runs.is_dir():
        for run_dir in runs.iterdir():
            if not run_dir.is_dir():
                continue
            try:
                run = refresh_liveness(run_dir)
            except ControllerError:
                continue
            rows.append(run_summary(run, run_dir))
    return sorted(rows, key=lambda row: int(row.get("createdAt") or 0), reverse=True)


def latest_attempt_start(record: dict[str, Any]) -> dict[str, Any]:
    """Project the recorded mechanical start form of the newest attempt.

    Attempts written before this field existed stay valid and are reported as
    unrecorded rather than guessed at or backfilled.
    """
    attempts = [item for item in (record.get("attempts") or []) if isinstance(item, dict)]
    if not attempts:
        return {"number": None, "form": None, "inputId": None, "recorded": False}
    latest = attempts[-1]
    form = latest.get("startForm")
    if form not in ATTEMPT_START_FORMS:
        form = None
    input_id = latest.get("startInputId")
    return {
        "number": latest.get("number"),
        "form": form,
        "inputId": str(input_id) if isinstance(input_id, str) and input_id else None,
        "recorded": form is not None,
    }


def run_summary(record: dict[str, Any], run_dir: Path | None = None) -> dict[str, Any]:
    lifecycle = record.get("lifecycle") or {}
    tasks = record.get("tasks") or {}
    complete = sum(
        1 for task_record in tasks.values()
        if isinstance(task_record, dict) and task_record.get("state") == "succeeded"
    )
    summary = {
        "runId": record.get("runId"),
        "controller": record.get("controller"),
        "declaredControllerName": record.get("declaredControllerName"),
        "requestedControllerName": record.get("requestedControllerName"),
        "lifecycle": lifecycle.get("state"),
        "pid": lifecycle.get("pid"),
        "status": record.get("status"),
        "checkpoint": record.get("checkpoint"),
        "summary": record.get("summary"),
        "createdAt": record.get("createdAt"),
        "updatedAt": record.get("updatedAt"),
        "tasks": len(tasks),
        "tasksSucceeded": complete,
        "workers": len(record.get("workers") or []),
        "attempts": len([
            item for item in (record.get("attempts") or []) if isinstance(item, dict)
        ]),
        "latestAttemptStart": latest_attempt_start(record),
    }
    if run_dir is not None:
        summary["runDir"] = str(run_dir)
    return summary


def _all_run_dirs() -> Iterator[Path]:
    for controller_dir in _controller_dirs():
        runs = controller_dir / "runs"
        if not runs.is_dir():
            continue
        yield from (path for path in runs.iterdir() if path.is_dir())


def resolve_run(target: str) -> Path:
    exact_controller = _controller_entry_by_name(target)
    if exact_controller is not None:
        runs = run_inventory(target)
        if not runs:
            raise ControllerRunNotFound(f"controller has no runs: {target}")
        return Path(str(runs[0]["runDir"]))

    candidates: list[Path] = []
    for run_dir in _all_run_dirs():
        run_id = run_dir.name
        if run_id == target:
            return run_dir
        if run_id.startswith(target):
            candidates.append(run_dir)
    if not candidates:
        raise ControllerRunNotFound(f"controller run not found: {target}")
    if len(candidates) > 1:
        raise ControllerError(f"ambiguous controller run prefix: {target}")
    return candidates[0]


def append_controller_input(
    run_dir: Path,
    *,
    worker_target: str | None,
    comment: str,
) -> dict[str, Any]:
    if not isinstance(comment, str) or not comment.strip():
        raise ControllerError("controller input comment must be non-empty")
    record = read_json(run_record_path(run_dir))
    worker = None
    if worker_target is not None:
        from ccx.store.identity import IdentityError, parse_managed_target
        try:
            target = parse_managed_target(worker_target)
            if target.qualified_engine is None:
                raise IdentityError("controller worker input needs a qualified full UUID")
        except IdentityError as error:
            raise ControllerError(f"controller worker must be an exact qualified full UUID: {error}") from error
        matches = [
            item for item in record.get("workers", [])
            if isinstance(item, dict)
            and item.get("worker") == worker_target
            and item.get("worker_id") == target.worker_id
            and item.get("engine") == target.qualified_engine
        ]
        if len(matches) != 1:
            raise ControllerError(
                f"worker {worker_target!r} is not uniquely recorded by controller run {record['runId']}"
            )
        worker = matches[0]
    entry = {
        "id": str(uuid.uuid4()),
        "createdAt": now_ms(),
        "worker": worker["worker"] if worker is not None else None,
        "workerName": worker.get("name") if worker is not None else None,
        "comment": comment,
    }
    append_ndjson(run_dir / "inputs.ndjson", entry)
    mutate_run(run_dir, lambda value: value.__setitem__("inputs", int(value.get("inputs") or 0) + 1))
    append_event(run_dir, "input-appended", inputId=entry["id"], worker=entry["worker"])
    return entry


def read_inputs(run_dir: Path) -> list[dict[str, Any]]:
    path = run_dir / "inputs.ndjson"
    if not path.exists():
        return []
    values: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            fcntl.flock(handle, fcntl.LOCK_SH)
            try:
                lines = handle.read().splitlines()
            finally:
                fcntl.flock(handle, fcntl.LOCK_UN)
        for line in lines:
            if not line.strip():
                continue
            parsed = json.loads(line)
            if isinstance(parsed, dict):
                values.append(parsed)
    except (OSError, json.JSONDecodeError) as error:
        raise ControllerError(f"cannot read controller inputs: {error}") from error
    return values


def process_presence(pid: Any, start: Any) -> oslayer.Presence:
    """Controller PID/start identity, never an argv-only signal authority."""
    from ccx.store.liveness import owned_process

    if pid is None:
        return "dead"
    if type(pid) is not int or pid <= 0 or not isinstance(start, str) or not start:
        return "unprovable"
    return owned_process(pid, start)


def refresh_liveness(run_dir: Path) -> dict[str, Any]:
    record = read_json(run_record_path(run_dir))
    lifecycle = record.get("lifecycle") or {}
    state = lifecycle.get("state")
    run_id = str(record.get("runId") or "")
    if state in {"starting", "running"} and process_presence(
        lifecycle.get("pid"), lifecycle.get("pidStart")
    ) == "dead":
        stopped = stop_marker_path(run_id).exists()
        changed = False

        def terminal(value: dict[str, Any]) -> None:
            nonlocal changed
            current = value.setdefault("lifecycle", {})
            # A resume or host startup may land while liveness is checked.
            # Never apply a dead-PID observation to that newer lifecycle.
            if (current.get("state") in {"starting", "running"}
                    and current.get("pid") == lifecycle.get("pid")
                    and current.get("pidStart") == lifecycle.get("pidStart")
                    and current.get("since") == lifecycle.get("since")
                    and len(value.get("attempts") or []) == len(record.get("attempts") or [])):
                changed = True
                ended_at = now_ms()
                current.update(
                    state="stopped" if stopped else "unavailable",
                    since=ended_at,
                    pid=None,
                    pidStart=None,
                )
                attempts = value.setdefault("attempts", [])
                if attempts:
                    attempts[-1].update(
                        completedAt=ended_at,
                        lifecycle="stopped" if stopped else "unavailable",
                    )
                if stopped:
                    value["error"] = None
                else:
                    value["error"] = value.get("error") or {
                        "type": "ControllerHostUnavailable",
                        "message": "controller host exited without publishing a terminal state",
                    }

        record = mutate_run(run_dir, terminal)
        if changed:
            append_event(run_dir, "run-stopped" if stopped else "host-unavailable")
    return record


__all__ = [
    "ATTEMPT_START_FORMS",
    "ATTEMPT_START_INITIAL",
    "ATTEMPT_START_RESUME",
    "ATTEMPT_START_RESUME_NOTE",
    "ATTEMPT_START_RESUME_WORKER_COMMENT",
    "CONTROLLER_SCHEMA_VERSION",
    "TERMINAL_LIFECYCLES",
    "latest_attempt_start",
    "append_controller_input",
    "append_event",
    "append_ndjson",
    "bootstrap_root",
    "clear_stop_marker",
    "controller_inventory",
    "controllers_root",
    "create_bootstrap",
    "ensure_controller_dirs",
    "file_lock",
    "launch_verdict_path",
    "mutate_run",
    "now_ms",
    "process_presence",
    "publish_bootstrap",
    "read_inputs",
    "read_json",
    "read_launch_verdict",
    "refresh_liveness",
    "resolve_run",
    "run_inventory",
    "run_record_path",
    "run_summary",
    "source_sha256",
    "stop_marker_path",
    "validate_controller_name",
    "write_json_atomic",
    "write_launch_verdict",
]
