"""Durable, task-specific Python controllers over real CCX workers."""

from __future__ import annotations

from collections.abc import Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from contextvars import ContextVar, copy_context
from dataclasses import asdict, dataclass, is_dataclass
from functools import wraps
import hashlib
import json
import os
from pathlib import Path
import re
import runpy
import subprocess
import sys
import threading
import traceback
from typing import Any, Callable, Generic, Literal, ParamSpec, TypeVar, overload
import uuid

from ccx.controllers import store as store
from ccx.errors import (
    CapabilityUnsupported,
    BudgetStopped,
    CcxError,
    CreatePartialFailure,
    CompactFailed,
    ControllerError,
    ControllerLaunchRequired,
    ControllerOperationIndeterminate,
    ControllerReplayMismatch,
    ControllerLiveStateLost,
    DeliveryRejected,
    InvalidRequest,
    SessionNotFound,
    WorkerApiError,
    WorkerRateLimited,
    build_worker_api_error,
)


P = ParamSpec("P")
R = TypeVar("R")

_MUTATING_WORKER_METHODS = frozenset({
    "create", "fork", "send", "steer", "resume", "compact", "answer",
    "respond_elicitation", "approve", "reject", "stop", "kill",
})
# Worker-creating calls: the journal owns their UUID across lost returns.
_CREATING_WORKER_METHODS = frozenset({"create", "fork"})
_SAFE_REJECTIONS = (
    InvalidRequest,
    CapabilityUnsupported,
    DeliveryRejected,
    SessionNotFound,
)
_SLUG_RE = re.compile(r"[^A-Za-z0-9._-]+")


@dataclass(frozen=True)
class TaskOutput:
    value: Any = None
    report: str | None = None


@dataclass(frozen=True)
class ControllerInput:
    id: str
    created_at: int
    worker: str | None
    worker_name: str | None
    comment: str

    @classmethod
    def from_record(cls, value: dict[str, Any]) -> "ControllerInput":
        return cls(
            id=str(value["id"]),
            created_at=int(value["createdAt"]),
            worker=str(value["worker"]) if value.get("worker") is not None else None,
            worker_name=value.get("workerName"),
            comment=str(value["comment"]),
        )


def task_output(*, value: Any = None, report: str | None = None) -> TaskOutput:
    if report is not None and not isinstance(report, str):
        raise InvalidRequest("task_output report must be a string or None")
    _canonical_json(value, "task output value")
    return TaskOutput(value=value, report=report)


def _canonical_json(value: Any, label: str) -> str:
    try:
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError) as error:
        raise InvalidRequest(f"{label} must be JSON-compatible: {error}") from error


def _fingerprint(value: Any, label: str) -> str:
    return hashlib.sha256(_canonical_json(value, label).encode("utf-8")).hexdigest()


def _slug(value: str, fallback: str = "item") -> str:
    rendered = _SLUG_RE.sub("-", value.strip()).strip("-._")
    return (rendered or fallback)[:60]


def _write_text_atomic(path: Path, text: str) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        temporary.write_text(text, encoding="utf-8")
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


@dataclass
class _LaunchContext:
    run_id: str
    run_dir: Path
    script: Path
    resume: bool
    name_override: str | None = None
    registered: bool = False


@dataclass
class _TaskContext:
    task_id: str
    label: str
    workspace: Path
    operation_count: int = 0


_launch_context: ContextVar[_LaunchContext | None] = ContextVar(
    "ccx_controller_launch", default=None
)
_active_runtime: ContextVar["_ControllerRuntime | None"] = ContextVar(
    "ccx_controller_runtime", default=None
)
_active_task: ContextVar[_TaskContext | None] = ContextVar(
    "ccx_controller_task", default=None
)
_reserved_task_invocation: ContextVar[tuple[object, int] | None] = ContextVar(
    "ccx_controller_reserved_task_invocation", default=None
)
_suspend_worker_hook: ContextVar[bool] = ContextVar(
    "ccx_controller_suspend_worker_hook", default=False
)


class _ControllerRuntime:
    def __init__(self, run_dir: Path):
        self.run_dir = run_dir
        record = store.read_json(store.run_record_path(run_dir))
        store.require_operation_journal_layout(run_dir, record)
        self.run_id = str(record["runId"])
        self.name = str(record["controller"])
        self.base_workspace = Path(str(record["workspace"]))
        self._lock = threading.RLock()
        self._task_counts: dict[str, int] = {}
        self._controller_operation_count = 0
        self._executor = ThreadPoolExecutor(thread_name_prefix=f"ccx-{self.run_id[:8]}")
        self._futures: list[Future[Any]] = []
        self._observed_futures: set[Future[Any]] = set()
        self._live_vars_owner = None

    def live_vars_owner(self):
        with self._lock:
            if self._live_vars_owner is None:
                from ccx.controllers.live_vars import LiveOwner
                self._live_vars_owner = LiveOwner()
            return self._live_vars_owner

    def _close_live_vars(self) -> None:
        with self._lock:
            owner = self._live_vars_owner
            self._live_vars_owner = None
        if owner is not None:
            owner.close()

    def next_task_invocation(self, key: str) -> int:
        with self._lock:
            value = self._task_counts.get(key, 0) + 1
            self._task_counts[key] = value
            return value

    def submit(self, call: Callable[[], R]) -> Future[R]:
        context = copy_context()
        future = self._executor.submit(context.run, call)
        with self._lock:
            self._futures.append(future)
        return future

    def observe_future(self, future: Future[Any]) -> None:
        with self._lock:
            self._observed_futures.add(future)

    def finish_futures(self) -> None:
        checked = 0
        first_error: BaseException | None = None
        while True:
            with self._lock:
                futures = self._futures[checked:]
            if not futures:
                break
            checked += len(futures)
            for future in futures:
                try:
                    future.result()
                except BaseException as error:
                    with self._lock:
                        observed = future in self._observed_futures
                    if not observed:
                        first_error = first_error or error
        self._executor.shutdown(wait=True)
        self._close_live_vars()
        if first_error is not None:
            raise first_error

    def shutdown_after_error(self) -> None:
        with self._lock:
            futures = list(self._futures)
        for future in futures:
            future.cancel()
        self._executor.shutdown(wait=True, cancel_futures=True)
        self._close_live_vars()

    def next_operation(self, task_context: _TaskContext | None) -> tuple[str, int, Path]:
        with self._lock:
            if task_context is None:
                self._controller_operation_count += 1
                number = self._controller_operation_count
                scope = "controller"
                directory = self.run_dir / "operations"
            else:
                task_context.operation_count += 1
                number = task_context.operation_count
                scope = task_context.task_id
                directory = self.run_dir / "tasks" / task_context.task_id / "operations"
        directory.mkdir(parents=True, exist_ok=True)
        return scope, number, directory


class _Current:
    def _runtime(self) -> _ControllerRuntime:
        runtime = _active_runtime.get()
        if runtime is None:
            raise ControllerLaunchRequired("current is only available inside a running CCX controller")
        return runtime

    @property
    def run_id(self) -> str:
        return self._runtime().run_id

    @property
    def controller_name(self) -> str:
        return self._runtime().name

    @property
    def run_dir(self) -> Path:
        return self._runtime().run_dir

    @property
    def workspace(self) -> Path:
        task_context = _active_task.get()
        return task_context.workspace if task_context is not None else self._runtime().base_workspace

    @property
    def workers(self) -> tuple[dict[str, Any], ...]:
        record = store.read_json(store.run_record_path(self._runtime().run_dir))
        return tuple(dict(item) for item in record.get("workers", []) if isinstance(item, dict))

    @property
    def inputs(self) -> tuple[ControllerInput, ...]:
        return tuple(
            ControllerInput.from_record(value)
            for value in store.read_inputs(self._runtime().run_dir)
        )

    @property
    def latest_input(self) -> ControllerInput | None:
        values = self.inputs
        return values[-1] if values else None

    def report(
        self,
        *,
        status: str,
        checkpoint: str | None = None,
        summary: str | None = None,
        report: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        runtime = self._runtime()
        if not isinstance(status, str) or not status.strip():
            raise InvalidRequest("controller report status must be non-empty")
        if data is not None:
            _canonical_json(data, "controller report data")
        report_path: Path | None = None
        with runtime._lock:
            if report is not None:
                if not isinstance(report, str):
                    raise InvalidRequest("controller report text must be a string or None")
                record = store.read_json(store.run_record_path(runtime.run_dir))
                sequence = len(record.get("reports") or []) + 1
                report_path = runtime.run_dir / "reports" / f"controller-{sequence:04d}.md"
                _write_text_atomic(report_path, report)
            report_record = {
                "createdAt": store.now_ms(),
                "status": status.strip(),
                "checkpoint": checkpoint,
                "summary": summary,
                "path": str(report_path) if report_path is not None else None,
                "data": data,
            }

            def update(value: dict[str, Any]) -> None:
                value["status"] = report_record["status"]
                value["checkpoint"] = checkpoint
                value["summary"] = summary
                value["latestReport"] = report_record
                value.setdefault("reports", []).append(report_record)

            store.mutate_run(runtime.run_dir, update)
        store.append_event(
            runtime.run_dir, "controller-report", status=status.strip(),
            checkpoint=checkpoint, reportPath=report_record["path"],
        )
        return dict(report_record)


current = _Current()


def _update_task_projection(runtime: _ControllerRuntime, task_record: dict[str, Any]) -> None:
    def update(value: dict[str, Any]) -> None:
        value.setdefault("tasks", {})[str(task_record["id"])] = {
            key: task_record.get(key)
            for key in (
                "id", "name", "invocation", "state", "workspaceMode", "workspace",
                "startedAt", "completedAt", "attempts", "reportPath", "git", "error",
            )
        }
    store.mutate_run(runtime.run_dir, update)


def _git(workspace: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(
        ["git", "-C", str(workspace), *args],
        text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    if check and proc.returncode != 0:
        detail = (proc.stderr or proc.stdout).strip()
        raise ControllerError(f"git {' '.join(args)} failed in {workspace}: {detail}")
    return proc


def _prepare_worktree(
    runtime: _ControllerRuntime, task_id: str, label: str,
) -> tuple[Path, dict[str, Any]]:
    persisted = (
        store.read_json(store.run_record_path(runtime.run_dir)).get("worktrees") or {}
    ).get(task_id)
    if isinstance(persisted, dict):
        repository = Path(str(persisted["repository"])).resolve()
        base = str(persisted["base"])
        workspace = Path(str(persisted["path"])).resolve()
        branch = str(persisted["branch"])
    else:
        repo_text = _git(runtime.base_workspace, "rev-parse", "--show-toplevel").stdout.strip()
        repository = Path(repo_text).resolve()
        base = _git(repository, "rev-parse", "HEAD").stdout.strip()
        workspace = runtime.run_dir / "worktrees" / task_id
        branch = f"ccx/{_slug(runtime.name)}/{runtime.run_id[:8]}/{task_id}"
    with store.file_lock(runtime.run_dir / "git.lock"):
        if workspace.exists():
            _git(workspace, "rev-parse", "--is-inside-work-tree")
        else:
            branch_exists = _git(repository, "show-ref", "--verify", "--quiet", f"refs/heads/{branch}", check=False).returncode == 0
            command = ["worktree", "add"]
            if not branch_exists:
                command += ["-b", branch]
            command += [str(workspace), branch if branch_exists else base]
            _git(repository, *command)
    metadata = {
        "repository": str(repository), "path": str(workspace),
        "branch": branch, "base": base, "head": _git(workspace, "rev-parse", "HEAD").stdout.strip(),
        "status": _git(workspace, "status", "--porcelain=v1").stdout,
        "diffStat": _git(workspace, "diff", "--stat", "HEAD").stdout,
        "commit": None,
    }
    store.mutate_run(
        runtime.run_dir,
        lambda value: value.setdefault("worktrees", {}).__setitem__(task_id, metadata),
    )
    return workspace, metadata


def _finish_git(
    runtime: _ControllerRuntime,
    task_id: str,
    label: str,
    workspace: Path,
    metadata: dict[str, Any],
    commit: bool | str,
) -> dict[str, Any]:
    dirty = _git(workspace, "status", "--porcelain=v1").stdout
    metadata = dict(metadata)
    metadata["status"] = dirty
    metadata["diffStat"] = _git(workspace, "diff", "--stat", "HEAD").stdout
    if commit and dirty:
        _git(workspace, "add", "-A")
        message = commit if isinstance(commit, str) else f"ccx({runtime.name}): {label}"
        _git(workspace, "commit", "-m", message)
        metadata["commit"] = _git(workspace, "rev-parse", "HEAD").stdout.strip()
        metadata["status"] = _git(workspace, "status", "--porcelain=v1").stdout
        metadata["diffStat"] = _git(workspace, "show", "--stat", "--oneline", "HEAD").stdout
    metadata["head"] = _git(workspace, "rev-parse", "HEAD").stdout.strip()
    store.mutate_run(
        runtime.run_dir,
        lambda value: value.setdefault("worktrees", {}).__setitem__(task_id, metadata),
    )
    return metadata


class TaskFuture(Generic[R]):
    def __init__(self, future: Future[R], runtime: _ControllerRuntime):
        self._future = future
        self._runtime = runtime

    def result(self, timeout: float | None = None) -> R:
        try:
            return self._future.result(timeout=timeout)
        finally:
            if self._future.done():
                self._runtime.observe_future(self._future)

    def done(self) -> bool:
        return self._future.done()


class Task(Generic[P, R]):
    def __init__(
        self,
        function: Callable[P, R],
        *,
        name: str | None = None,
        workspace: Literal["shared", "worktree"] = "shared",
        commit: bool | str = False,
    ):
        if workspace not in {"shared", "worktree"}:
            raise InvalidRequest("task workspace must be 'shared' or 'worktree'")
        if commit and workspace != "worktree":
            raise InvalidRequest("task commits require workspace='worktree'")
        self.function = function
        self.name = name or function.__name__
        self.workspace_mode = workspace
        self.commit = commit
        wraps(function)(self)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        runtime = _active_runtime.get()
        if runtime is None:
            raise ControllerLaunchRequired("tasks can only run inside a CCX controller")
        key = _slug(self.name)
        reserved = _reserved_task_invocation.get()
        invocation = (
            reserved[1] if reserved is not None and reserved[0] is self
            else runtime.next_task_invocation(key)
        )
        task_id = f"{_slug(self.name)}--{invocation:04d}"
        input_value = {"args": args, "kwargs": kwargs}
        input_fingerprint = _fingerprint(input_value, f"arguments for task {self.name}")
        task_dir = runtime.run_dir / "tasks" / task_id
        task_dir.mkdir(parents=True, exist_ok=True)
        task_path = task_dir / "task.json"
        with store.file_lock(task_dir / "task.lock"):
            if task_path.exists():
                record = store.read_json(task_path)
                if record.get("function") != self.function.__qualname__:
                    raise ControllerReplayMismatch(
                        f"task {task_id} resolved to {self.function.__qualname__!r}, but the "
                        f"stored invocation belongs to {record.get('function')!r}; controller "
                        "resumes must make the same task calls in the same order"
                    )
                if record.get("inputSha256") != input_fingerprint:
                    raise ControllerReplayMismatch(
                        f"task {task_id} input changed while replaying controller {runtime.run_id}; "
                        "controllers resume from the top and must make the same task calls with "
                        "the same arguments in the same order"
                    )
                if record.get("state") == "succeeded":
                    if record.get("outputKind") == "task-output":
                        report_path = record.get("reportPath")
                        report = Path(report_path).read_text(encoding="utf-8") if report_path else None
                        return TaskOutput(value=record.get("value"), report=report)  # type: ignore[return-value]
                    return record.get("value")  # type: ignore[return-value]
                attempts = int(record.get("attempts") or 0) + 1
            else:
                attempts = 1
                record = {
                    "schemaVersion": 1, "id": task_id, "name": self.name,
                    "function": self.function.__qualname__, "invocation": invocation,
                    "inputSha256": input_fingerprint, "workspaceMode": self.workspace_mode,
                    "workspace": None, "state": "pending", "attempts": 0,
                    "createdAt": store.now_ms(), "startedAt": None, "completedAt": None,
                    "outputKind": None, "value": None, "reportPath": None,
                    "git": None, "error": None,
                }
            record.update(state="running", attempts=attempts, startedAt=store.now_ms(), error=None)
            store.write_json_atomic(task_path, record)
        _update_task_projection(runtime, record)
        store.append_event(runtime.run_dir, "task-started", taskId=task_id, attempt=attempts)

        git_metadata: dict[str, Any] | None = None
        workspace = runtime.base_workspace
        try:
            if self.workspace_mode == "worktree":
                workspace, git_metadata = _prepare_worktree(runtime, task_id, self.name)
            record["workspace"] = str(workspace)
            task_context = _TaskContext(task_id=task_id, label=self.name, workspace=workspace)
            token = _active_task.set(task_context)
            try:
                result = self.function(*args, **kwargs)
            finally:
                _active_task.reset(token)
            if isinstance(result, TaskOutput):
                value = result.value
                report_text = result.report
                output_kind = "task-output"
            else:
                value = result
                report_text = None
                output_kind = "value"
            _canonical_json(value, f"return value for task {self.name}")
            report_path: Path | None = None
            if report_text is not None:
                report_path = task_dir / "report.md"
                _write_text_atomic(report_path, report_text)
            if git_metadata is not None:
                git_metadata = _finish_git(
                    runtime, task_id, self.name, workspace, git_metadata, self.commit
                )
            record.update(
                state="succeeded", completedAt=store.now_ms(), outputKind=output_kind,
                value=value, reportPath=str(report_path) if report_path else None,
                git=git_metadata, error=None,
            )
            store.write_json_atomic(task_path, record)
            _update_task_projection(runtime, record)
            store.append_event(runtime.run_dir, "task-succeeded", taskId=task_id)
            return result
        except BaseException as error:
            if git_metadata is not None and workspace.exists():
                try:
                    git_metadata = _finish_git(
                        runtime, task_id, self.name, workspace, git_metadata, False
                    )
                except Exception:
                    pass
            record.update(
                state="failed", completedAt=store.now_ms(), git=git_metadata,
                error={"type": type(error).__name__, "message": str(error)},
            )
            store.write_json_atomic(task_path, record)
            _update_task_projection(runtime, record)
            store.append_event(
                runtime.run_dir, "task-failed", taskId=task_id,
                errorType=type(error).__name__, message=str(error),
            )
            raise

    def submit(self, *args: P.args, **kwargs: P.kwargs) -> TaskFuture[R]:
        runtime = _active_runtime.get()
        if runtime is None:
            raise ControllerLaunchRequired("tasks can only run inside a CCX controller")
        invocation = runtime.next_task_invocation(_slug(self.name))
        def invoke():
            token = _reserved_task_invocation.set((self, invocation))
            try:
                return self(*args, **kwargs)
            finally:
                _reserved_task_invocation.reset(token)
        return TaskFuture(runtime.submit(invoke), runtime)


@overload
def task(function: Callable[P, R]) -> Task[P, R]: ...


@overload
def task(
    function: None = None, *, name: str | None = None,
    workspace: Literal["shared", "worktree"] = "shared", commit: bool | str = False,
) -> Callable[[Callable[P, R]], Task[P, R]]: ...


def task(function=None, *, name=None, workspace="shared", commit=False):
    def decorate(value):
        return Task(value, name=name, workspace=workspace, commit=commit)
    return decorate(function) if function is not None else decorate


def _original_controller_start() -> str:
    from ccx.store.liveness import proc_start_token
    start = proc_start_token(os.getpid())
    if start is None:
        raise ControllerError("controller cannot prove its original PID/start")
    return start


def _mark_lifecycle(run_dir: Path, state: str, *, error: BaseException | None = None) -> None:
    start = _original_controller_start() if state == "running" else None

    def update(value: dict[str, Any]) -> None:
        value.setdefault("lifecycle", {}).update(
            state=state, since=store.now_ms(), pid=os.getpid() if state == "running" else None,
            pidStart=start,
        )
        attempts = value.setdefault("attempts", [])
        if attempts:
            attempts[-1]["pid"] = os.getpid()
            if start is not None:
                attempts[-1]["pidStart"] = start
            if state != "running":
                attempts[-1]["completedAt"] = store.now_ms()
                attempts[-1]["lifecycle"] = state
        if error is not None:
            value["error"] = {
                "type": type(error).__name__, "message": str(error),
                "traceback": "".join(traceback.format_exception(error)),
            }
        elif state == "succeeded":
            value["error"] = None
            if value.get("status") is None:
                value["status"] = "completed"
    store.mutate_run(run_dir, update)
    store.append_event(run_dir, f"run-{state}", pid=os.getpid())


def _register_controller(
    launch: _LaunchContext,
    *,
    name: str | None,
    workspace: str | os.PathLike[str] | None,
) -> _ControllerRuntime:
    if launch.registered:
        raise ControllerError("a controller script may invoke only one @controller entrypoint")
    # Fence direct entry invocation before publishing a bootstrap.
    store.require_operation_journal_layout(launch.run_dir)
    decorated_name = store.validate_controller_name(name or launch.script.stem)
    effective_name = store.validate_controller_name(
        launch.name_override or decorated_name
    )
    if launch.resume:
        record = store.read_json(store.run_record_path(launch.run_dir))
        recorded_declared = record.get("declaredControllerName")
        if (
            not isinstance(recorded_declared, str)
            or recorded_declared.casefold() != decorated_name.casefold()
        ):
            raise ControllerReplayMismatch(
                f"declared controller name changed from {recorded_declared!r} "
                f"to {decorated_name!r}"
            )
        effective_workspace = (
            Path(workspace).expanduser().resolve()
            if workspace is not None
            else Path(str(record.get("workspace"))).resolve()
        )
        if str(record.get("controller", "")).casefold() != effective_name.casefold():
            raise ControllerReplayMismatch(
                f"effective controller name changed from {record.get('controller')!r} "
                f"to {effective_name!r}"
            )
        if Path(str(record.get("workspace"))).resolve() != effective_workspace:
            raise ControllerReplayMismatch("decorated controller workspace changed during resume")
    else:
        effective_workspace = (
            Path(workspace).expanduser().resolve()
            if workspace is not None else Path.cwd().resolve()
        )
        launch.run_dir = store.publish_bootstrap(
            launch.run_dir, name=effective_name, script=launch.script,
            declared_name=decorated_name, workspace=effective_workspace,
        )
    launch.registered = True
    runtime = _ControllerRuntime(launch.run_dir)
    _mark_lifecycle(launch.run_dir, "running")
    store.write_launch_verdict(
        launch.run_id,
        {"ok": True, "runId": launch.run_id, "controller": runtime.name,
         "runDir": str(launch.run_dir), "pid": os.getpid()},
    )
    return runtime


@overload
def controller(function: Callable[P, R]) -> Callable[P, R]: ...


@overload
def controller(
    function: None = None, *, name: str | None = None,
    workspace: str | os.PathLike[str] | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]: ...


def controller(function=None, *, name=None, workspace=None):
    def decorate(value):
        @wraps(value)
        def wrapped(*args, **kwargs):
            launch = _launch_context.get()
            if launch is None:
                raise ControllerLaunchRequired(
                    f"{launch_command_for_current_script()} must launch decorated controllers; "
                    "direct Python execution is intentionally refused"
                )
            runtime = _register_controller(launch, name=name, workspace=workspace)
            token = _active_runtime.set(runtime)
            try:
                result = value(*args, **kwargs)
                runtime.finish_futures()
                if isinstance(result, TaskOutput):
                    _canonical_json(result.value, "controller return value")
                    if result.report is not None:
                        current.report(status="completed", report=result.report)
                    persisted_result = result.value
                else:
                    _canonical_json(result, "controller return value")
                    persisted_result = result
                store.mutate_run(
                    runtime.run_dir,
                    lambda record: record.__setitem__("result", persisted_result),
                )
                _mark_lifecycle(runtime.run_dir, "succeeded")
                return result
            except BaseException as error:
                try:
                    runtime.shutdown_after_error()
                except BaseException:
                    pass
                try:
                    existing = store.read_json(store.run_record_path(runtime.run_dir))
                    if (existing.get("lifecycle") or {}).get("state") != "stopped":
                        _mark_lifecycle(runtime.run_dir, "failed", error=error)
                except Exception:
                    pass
                raise
            finally:
                _active_runtime.reset(token)
        return wrapped
    return decorate(function) if function is not None else decorate


def launch_command_for_current_script() -> str:
    script = Path(sys.argv[0]).resolve() if sys.argv else Path("controller.py")
    return f"ccx controller run {script}"


def _host_execute(
    *, run_id: str, run_dir: Path, script: Path, resume: bool, script_args: list[str],
) -> int:
    from ccx.controllers.live_vars import sanitize_inherited_shell_bridge_env
    sanitize_inherited_shell_bridge_env()

    launch = _LaunchContext(
        run_id=run_id,
        run_dir=run_dir,
        script=script,
        resume=resume,
    )
    launch_token = _launch_context.set(launch)
    old_argv = sys.argv[:]
    try:
        record = store.read_json(store.run_record_path(run_dir))
        # This precedes lifecycle writes and, crucially, runpy.run_path: top-level
        # controller source can perform arbitrary side effects before @controller.
        try:
            store.require_operation_journal_layout(run_dir, record)
        except ControllerError as error:
            # Direct _host entry is independently fenced. A layout refusal is
            # not an execution failure: do not overwrite the legacy lifecycle,
            # error, attempt, launch verdict or operation journals.
            sys.stderr.write(f"[ccx controller] {error}\n")
            return 1
        requested_name = record.get("requestedControllerName")
        launch.name_override = (
            str(requested_name) if requested_name is not None else None
        )
        start = _original_controller_start()
        store.mutate_run(
            run_dir,
            lambda value: value.setdefault("lifecycle", {}).update(
                state="starting", since=store.now_ms(), pid=os.getpid(),
                pidStart=start,
            ),
        )
        sys.argv = [str(script), *script_args]
        runpy.run_path(str(script), run_name="__main__")
        if not launch.registered:
            raise ControllerError(
                "controller script did not invoke an @controller-decorated entrypoint"
            )
        return 0
    except SystemExit as error:
        code = error.code if isinstance(error.code, int) else 1
        if not launch.registered:
            try:
                _mark_lifecycle(run_dir, "failed", error=error)
            except Exception:
                pass
            store.write_launch_verdict(
                run_id, {"ok": False, "runId": run_id, "error": f"script exited with {code} before controller startup"},
            )
        return code
    except BaseException as error:
        if launch.registered:
            try:
                _mark_lifecycle(launch.run_dir, "failed", error=error)
            except Exception:
                pass
        else:
            try:
                _mark_lifecycle(run_dir, "failed", error=error)
            except Exception:
                pass
            store.write_launch_verdict(
                run_id,
                {"ok": False, "runId": run_id, "error": f"{type(error).__name__}: {error}"},
            )
        traceback.print_exc()
        return 1
    finally:
        sys.argv = old_argv
        _launch_context.reset(launch_token)


def _operation_metadata(method: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> dict[str, Any]:
    metadata: dict[str, Any] = {"method": method}
    if method == "create":
        metadata.update(
            engine=kwargs.get("engine"), name=kwargs.get("name"), cwd=kwargs.get("cwd"),
            workerId=kwargs.get("worker_id"),
            requestedEngine=kwargs.get("engine"), requestedModel=kwargs.get("model"),
            requestedEffort=kwargs.get("effort"),
            promptBytes=len(str(kwargs.get("prompt") or "").encode("utf-8")),
        )
    elif method == "fork":
        source = args[0] if args else kwargs.get("worker")
        metadata.update(
            source=_worker_handle(source), name=kwargs.get("name"),
            workerId=kwargs.get("worker_id"),
            requestedModel=kwargs.get("model"), requestedEffort=kwargs.get("effort"),
            promptBytes=len(str(kwargs.get("prompt") or "").encode("utf-8")),
        )
    elif method == "send":
        worker = args[0] if args else kwargs.get("worker")
        text = args[1] if len(args) > 1 else kwargs.get("text")
        metadata.update(worker=_worker_handle(worker), deliveryId=kwargs.get("delivery_id"),
                        textBytes=len(str(text or "").encode("utf-8")))
    else:
        worker = args[0] if args else kwargs.get("worker")
        if worker is not None:
            metadata["worker"] = _worker_handle(worker)
        if method == "steer":
            metadata["deliveryId"] = kwargs.get("delivery_id")
        if method == "compact":
            metadata["operationId"] = kwargs.get("operation_id")
    variables = kwargs.get("variables")
    if isinstance(variables, Mapping):
        metadata["variableMode"] = "live"
        metadata["liveVariables"] = sorted(str(name) for name in variables)
    variable_source = kwargs.get("variables_from")
    if variable_source is not None:
        metadata["variableMode"] = "durable-shared"
        metadata["variableSource"] = _worker_handle(variable_source)
    return metadata


def _stable_call_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Path):
        return {"path": str(value)}
    if isinstance(value, (list, tuple)):
        return [_stable_call_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _stable_call_value(item) for key, item in sorted(value.items(), key=lambda item: str(item[0]))}
    if is_dataclass(value):
        return _stable_call_value(asdict(value))
    return {"type": f"{type(value).__module__}.{type(value).__qualname__}", "repr": repr(value)}


def _worker_handle(value: Any) -> str | None:
    if isinstance(value, str):
        return value
    if isinstance(value, dict) and isinstance(value.get("worker"), str):
        return value["worker"]
    return None


def _verified_created_owner(client: Any, result: Any, kwargs: dict[str, Any],
                            operation_id: str, *, method: str = "create"):
    """Require the journaled full UUID and its ACTIVE original owner."""
    worker_id = kwargs.get("worker_id")
    engine = kwargs.get("engine")
    if method == "fork":
        try:
            from ccx.store.identity import canonical_worker_id
            canonical_worker_id(worker_id)
            client._uuid_store.revalidate()
            owner = client._uuid_store.resolve(worker_id)
            engine = owner.engine
        except Exception as error:
            raise ControllerOperationIndeterminate(
                f"fork child has no confirmed original ACTIVE owner: {operation_id}",
                operation_id=operation_id,
            ) from error
    handle = f"{engine}:{worker_id}"
    if (not isinstance(result, dict) or result.get("worker") != handle
            or result.get("worker_id") != worker_id
            or result.get("engine") != engine):
        raise ControllerOperationIndeterminate(
            f"create result disagrees with its original full-UUID intent: {operation_id}",
            operation_id=operation_id,
        )
    if method != "fork":
        try:
            from ccx.store.identity import canonical_worker_id
            canonical_worker_id(worker_id)
            client._uuid_store.revalidate()
            owner = client._uuid_store.resolve(handle)
        except Exception as error:
            raise ControllerOperationIndeterminate(
                f"created worker has no confirmed original ACTIVE owner: {operation_id}",
                operation_id=operation_id,
            ) from error
    if owner.worker != handle or owner.worker_id != worker_id or owner.engine != engine:
        raise ControllerOperationIndeterminate(
            f"created owner differs from committed journal identity: {operation_id}",
            operation_id=operation_id,
        )
    return owner


def _registered_cwd(client: Any, handle: str, cwd: Any) -> str | None:
    """Use the exact created owner's record, never the old short-keyed store."""
    if cwd:
        return str(Path(str(cwd)).resolve())
    record = client._uuid_store.record(handle)
    return record.get("cwd")


def _register_worker(
    runtime: _ControllerRuntime,
    client: Any,
    owner: Any,
    result: dict[str, Any],
    kwargs: dict[str, Any],
    task_context: _TaskContext | None,
    operation_id: str,
) -> None:
    handle = owner.worker
    name = kwargs.get("name")
    worker_record = {
        "worker": handle,
        "engine": owner.engine,
        "short": owner.short,
        "worker_id": owner.worker_id,
        "name": name,
        "cwd": _registered_cwd(client, handle, kwargs.get("cwd") or result.get("cwd")),
        "taskId": task_context.task_id if task_context is not None else None,
        "createdAt": store.now_ms(),
    }

    def update(value: dict[str, Any]) -> None:
        workers = value.setdefault("workers", [])
        if not any(item.get("worker") == handle for item in workers):
            workers.append(worker_record)
    store.mutate_run(runtime.run_dir, update)
    store.append_event(runtime.run_dir, "worker-recorded", worker=handle, name=name)


def _budget_stopped_from_record(record: dict[str, Any]) -> BudgetStopped:
    error = record.get("error") or {}
    return BudgetStopped(
        str(error.get("message") or "budget stopped"),
        short=error.get("short"), reason=error.get("reason"),
        phase=error.get("phase"),
    )


# Journal `error.type` values that mean "a WorkerApiError of some subclass".
# Listed by name because a journal written by an older ccx must still replay;
# the exact subclass is then rebuilt from the stored flags, not from the name.
_API_ERROR_TYPES = frozenset({
    "WorkerApiError", "WorkerRateLimited", "WorkerApiTransient",
    "WorkerApiPermanent", "WorkerOverloaded",
})


def _api_error_fields(error: WorkerApiError | None) -> dict[str, Any] | None:
    """Flatten a typed API error for the journal, or ``None`` when absent."""
    if error is None:
        return None
    return {
        "type": type(error).__name__, "message": str(error),
        "short": error.short, "kind": error.kind, "status": error.status,
        "transient": error.transient,
        "rateLimited": isinstance(error, WorkerRateLimited),
        "requestId": error.request_id, "detail": error.message,
        **({"resetText": error.reset_text}
           if isinstance(error, WorkerRateLimited) else {}),
    }


def _api_error_from_fields(fields: Any) -> WorkerApiError | None:
    """Rebuild the exception from its durable classification flags."""
    if not isinstance(fields, dict):
        return None
    rate_limited = bool(fields.get("rateLimited"))
    return build_worker_api_error(
        str(fields.get("message") or "Claude API error"),
        short=fields.get("short"), kind=fields.get("kind"),
        status=fields.get("status"),
        transient=bool(fields.get("transient")),
        rate_limited=rate_limited,
        request_id=fields.get("requestId"), detail=fields.get("detail"),
        reset_text=fields.get("resetText"),
    )


def _stopped_error_from_record(record: dict[str, Any]) -> CcxError:
    """Rebuild the error a `stopped` operation died on, for replay.

    Resume must re-raise the same condition it stopped on, typed as it was
    originally: a controller that branches on ``WorkerRateLimited`` has to see
    it again on the second run, not a flattened stand-in.
    """
    error = record.get("error") or {}
    kind = error.get("type")
    message = str(error.get("message") or "Claude API error")
    if kind == "CompactFailed":
        return CompactFailed(
            message, short=error.get("short"), state=error.get("state"),
            operation_id=error.get("operationId"),
            api_error=_api_error_from_fields(error.get("apiError")),
        )
    if kind == "CreatePartialFailure":
        result = record.get("result") or {}
        cause = error.get("causeType")
        typed_cause = (
            _budget_stopped_from_record(record) if cause == "BudgetStopped" else
            _api_error_from_fields(error.get("apiError"))
            if cause in _API_ERROR_TYPES else None
        )
        return CreatePartialFailure(
            message, info={
                "short": result.get("short") or error.get("short"),
                "worker_id": result.get("worker_id") or error.get("workerId"),
                "worker": result.get("worker") or error.get("worker"),
                "engine": result.get("engine") or error.get("engine") or "claude",
            },
            phase=error.get("phase") or "initial-delivery",
            delivery_id=error.get("deliveryId"),
            delivery_status=error.get("deliveryStatus"),
            notification_intent=bool(error.get("notificationIntent")),
            cause=typed_cause,
        )
    if kind not in _API_ERROR_TYPES:
        return _budget_stopped_from_record(record)
    return _api_error_from_fields(error) or _budget_stopped_from_record(record)


def _stopped_create_result(client: Any, error: CcxError,
                           kwargs: dict[str, Any], operation_id: str, *,
                           method: str = "create") -> dict[str, Any] | None:
    """Record only a positively ACTIVE original worker, never a short alias.

    A partial create may preserve an unconfirmed claimed UUID. Its typed error
    is durable, but that claim is NOT evidence of native activation or success.
    """
    if not isinstance(error, CreatePartialFailure):
        return None
    if (error.info.get("activation_state") != "confirmed"
            or error.phase not in {"initial-delivery", "wait-report", "notification-registration"}):
        return None
    result = {"worker": error.worker, "worker_id": error.worker_id,
              "engine": error.engine, "short": error.worker_id[:8],
              "cwd": error.info.get("cwd"), "report": None}
    owner = _verified_created_owner(client, result, kwargs, operation_id, method=method)
    record = client._uuid_store.record(owner.worker)
    lifecycle = record.get("lifecycle") or {}
    result["state"] = lifecycle.get("state")
    result["native_state"] = (record.get("native") or {}).get("nativeState")
    return result


def _worker_call_fingerprint_value(
    args: tuple[Any, ...], kwargs: dict[str, Any],
) -> dict[str, Any]:
    stable_kwargs = dict(kwargs)
    variables = stable_kwargs.get("variables")
    if isinstance(variables, Mapping):
        stable_kwargs["variables"] = {
            "mode": "live",
            "names": sorted(str(name) for name in variables),
        }
    variable_source = stable_kwargs.get("variables_from")
    if variable_source is not None:
        stable_kwargs["variables_from"] = _worker_handle(variable_source)
    return {
        "args": _stable_call_value(args),
        "kwargs": _stable_call_value(stable_kwargs),
    }


def _raise_live_state_lost(record: dict[str, Any], operation_id: str) -> None:
    metadata = record.get("metadata") or {}
    names = metadata.get("liveVariables")
    if not isinstance(names, list) or not names:
        return
    rendered = ", ".join(str(name) for name in names)
    raise ControllerLiveStateLost(
        "worker operation bound live Python variables "
        f"({rendered}) in a prior controller attempt; CCX will not redeliver "
        "the worker turn or reconstruct that object graph. Complete the branch "
        "that consumes live state inside the same successful task/attempt and "
        "return the JSON-compatible result needed for replay.",
        operation_id=operation_id,
    )


def _first_live_binding_committed(client: Any, record: dict, worker_id: str) -> bool:
    names = (record.get("metadata") or {}).get("liveVariables")
    if not isinstance(names, list) or not names:
        return False
    from ccx.store.identity import UnknownWorker
    from ccx.store.root import _read_json
    ctx = client._uuid_store
    try:
        owner = ctx.resolve(worker_id)
    except UnknownWorker:
        return False
    try:
        with ctx.lease():
            with ctx._gen("variables", "bindings", owner.engine) as directory:
                prior = _read_json(directory, owner.worker_id + ".json")
    except FileNotFoundError:
        return False
    return (isinstance(prior, dict) and prior.get("schemaVersion") == 1
            and prior.get("names") == sorted(names))


def observe_worker_call(
    client: Any,
    method: str,
    function: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> Any:
    runtime = _active_runtime.get()
    if runtime is None or _suspend_worker_hook.get():
        return function(client, *args, **kwargs)
    if method not in _MUTATING_WORKER_METHODS:
        return function(client, *args, **kwargs)
    kwargs = dict(kwargs)
    task_context = _active_task.get()
    scope, number, directory = runtime.next_operation(task_context)
    operation_id = f"{scope}:{number:04d}:{method}"
    path = directory / f"{number:04d}-{method}.json"
    existing_record = store.read_json(path) if path.exists() else None
    # A positional journal, not a display name, owns the UUID. Use it on
    # every lost-return retry even when the caller did not specify an ID.
    supplied_create_id = kwargs.get("worker_id") if method in _CREATING_WORKER_METHODS else None
    committed_create_id = (
        (existing_record.get("metadata") or {}).get("workerId")
        if method in _CREATING_WORKER_METHODS and existing_record is not None else None
    )
    if method not in _CREATING_WORKER_METHODS:
        # Reserve the ordinal before resolving a changing worker target: a
        # caught lookup refusal must not displace a later accepted operation.
        worker = args[0] if args else kwargs.get("worker")
        owner = client._uuid_owner(worker)
        if args:
            args = (owner.worker, *args[1:])
        else:
            kwargs["worker"] = owner.worker
        if existing_record is not None and (
            (existing_record.get("metadata") or {}).get("worker") != owner.worker
        ):
            raise ControllerOperationIndeterminate(
                f"{method} intent has no proven original worker: {operation_id}",
                operation_id=operation_id,
            )
    if method in {"send", "steer"}:
        caps = kwargs.get("caps")
        if caps is not None:
            from ccx.workers import _check_caps
            _check_caps(caps, engine=owner.engine)
        supplied_delivery_id = kwargs.get("delivery_id")
        stored_delivery_id = ((existing_record.get("metadata") or {}).get("deliveryId")
                              if existing_record is not None else None)
        if stored_delivery_id is not None and supplied_delivery_id is not None and supplied_delivery_id != stored_delivery_id:
            raise ControllerReplayMismatch(f"original {method} delivery token changed during replay: {operation_id}")
        kwargs["delivery_id"] = stored_delivery_id or supplied_delivery_id or str(uuid.uuid4())
        from ccx.claude import delivery as claude_delivery
        from ccx.codex import delivery as codex_delivery
        validator = claude_delivery.valid_delivery_id if owner.engine == "claude" else codex_delivery.valid_delivery_id
        if not validator(kwargs["delivery_id"]):
            raise InvalidRequest(f"controller {method} delivery_id must be a bounded native token")
        if existing_record is not None and stored_delivery_id != kwargs["delivery_id"]:
            raise ControllerOperationIndeterminate(
                f"{method} intent has no proven original delivery token: {operation_id}",
                operation_id=operation_id,
            )
    if method == "compact" and owner.engine == "claude" and not kwargs.get("operation_id"):
        persisted_operation_id = (
            (existing_record.get("metadata") or {}).get("operationId")
            if existing_record is not None else None
        )
        kwargs["operation_id"] = persisted_operation_id or str(uuid.uuid4())

    def require_original_result(value: Any) -> None:
        # The native methods own receipt proof; the controller must not
        # publish/replay a result for a different worker or operation token.
        if (not isinstance(value, dict) or value.get("worker") != owner.worker
                or value.get("worker_id", owner.worker_id) != owner.worker_id
                or value.get("engine", owner.engine) != owner.engine
                or (method in {"send", "steer"}
                    and value.get("delivery_id", kwargs["delivery_id"]) != kwargs["delivery_id"])
                or (method == "compact" and owner.engine == "claude"
                    and value.get("operation_id") != kwargs["operation_id"])):
            raise ControllerOperationIndeterminate(
                f"{method} result disagrees with its original worker/token: {operation_id}",
                operation_id=operation_id,
            )
    call_fingerprint = _fingerprint(
        _worker_call_fingerprint_value(args, kwargs),
        f"WorkerClient.{method} call",
    )
    if method in _CREATING_WORKER_METHODS:
        from ccx.ops.creation import new_worker_id
        kwargs["worker_id"] = committed_create_id or supplied_create_id or new_worker_id()
    if existing_record is not None:
        record = existing_record
        if record.get("callSha256") != call_fingerprint:
            raise ControllerReplayMismatch(f"worker operation changed during replay: {operation_id}")
        state = record.get("state")
        if (state in {"intent", "indeterminate", "rejected"}
                and method in _CREATING_WORKER_METHODS
                and committed_create_id
                and _first_live_binding_committed(client, record, committed_create_id)):
            _raise_live_state_lost(record, operation_id)
        if state == "accepted":
            _raise_live_state_lost(record, operation_id)
            result = record.get("result")
            if method in _CREATING_WORKER_METHODS:
                owner = _verified_created_owner(
                    client, result, kwargs, operation_id, method=method,
                )
                _register_worker(runtime, client, owner, result, kwargs, task_context, operation_id)
            else:
                require_original_result(result)
            return result
        if state == "stopped":
            _raise_live_state_lost(record, operation_id)
            result = record.get("result")
            if method in _CREATING_WORKER_METHODS and isinstance(result, dict):
                owner = _verified_created_owner(
                    client, result, kwargs, operation_id, method=method,
                )
                _register_worker(runtime, client, owner, result, kwargs, task_context, operation_id)
            raise _stopped_error_from_record(record)
        if state == "rejected":
            pass
        elif state not in {"intent", "indeterminate"}:
            raise ControllerOperationIndeterminate(
                f"worker operation has unknown journal state {state!r}: {operation_id}",
                operation_id=operation_id,
            )
        elif method in _CREATING_WORKER_METHODS:
            if not committed_create_id:
                raise ControllerOperationIndeterminate(
                    f"old worker create intent has no replayable UUID: {operation_id}",
                    operation_id=operation_id,
                )
            # Fall through: same worker UUID and create:initial receipt reconcile
            # both activation and first delivery without matching any names.
        if state in {"intent", "indeterminate"} and method == "compact" and kwargs.get("operation_id"):
            token = _suspend_worker_hook.set(True)
            try:
                status = client.compact_status(
                    args[0] if args else kwargs.get("worker"), kwargs["operation_id"]
                )
            finally:
                _suspend_worker_hook.reset(token)
            if status.get("operation_status") == "succeeded":
                require_original_result(status)
                record.update(state="accepted", acceptedAt=store.now_ms(), result=status, reconciled=True)
                store.write_json_atomic(path, record)
                return status
        if state != "rejected" and method not in _CREATING_WORKER_METHODS:
            raise ControllerOperationIndeterminate(
                f"worker operation may have landed and will not be retried: {operation_id}",
                operation_id=operation_id,
            )

    record = {
        "schemaVersion": 1, "id": operation_id, "method": method,
        "state": "intent", "createdAt": store.now_ms(),
        "callSha256": call_fingerprint, "metadata": _operation_metadata(method, args, kwargs),
        "result": None, "error": None,
    }
    if existing_record is None or existing_record.get("state") == "rejected":
        # A safe rejection permits another dispatch, not reuse of its durable
        # rejection. Fence the retry before it can land and lose its reply.
        if existing_record is not None:
            record["createdAt"] = existing_record["createdAt"]
        store.write_json_atomic(path, record)
        store.append_event(runtime.run_dir, "worker-operation-intent", operationId=operation_id, method=method)
    else:
        record = existing_record
    try:
        # One public mutation owns one replay slot. Nested calls should not
        # create another slot when restoring an accepted outer result.
        token = _suspend_worker_hook.set(True)
        try:
            result = function(client, *args, **kwargs)
        finally:
            _suspend_worker_hook.reset(token)
        # Never persist accepted before proving its original journaled worker.
        if method in _CREATING_WORKER_METHODS:
            created_owner = _verified_created_owner(
                client, result, kwargs, operation_id, method=method,
            )
        else:
            require_original_result(result)
    except BaseException as error:
        budget_stopped = isinstance(error, BudgetStopped)
        partial = isinstance(error, CreatePartialFailure)
        partial_cause = error.cause if partial else None
        # An API error journals like a budget stop rather than as a generic
        # failure. Generic failures become `indeterminate`, which on resume
        # raises ControllerOperationIndeterminate and loses the real reason —
        # exactly the wrong answer for a run halted by capacity or a rate limit.
        # `stopped` keeps the typed error and re-raises it on resume instead.
        #
        # Replay stays off the table on purpose: a turn killed by an API error
        # had already been delivered, so re-running the call would double-send
        # the prompt. A controller that wants to wait out a rate limit catches
        # the exception INSIDE its task, where it knows that is safe.
        stopped = budget_stopped or partial or isinstance(
            error, (WorkerApiError, CompactFailed),
        )
        # The worker may exist even though the call raised — recover it for an
        # API error exactly as for a budget stop, or the run leaks a live
        # worker while freeing its name.
        expected_engine = kwargs.get("engine") if method == "create" else (
            error.engine if partial else None
        )
        if partial and (error.worker_id != kwargs.get("worker_id")
                        or error.engine != expected_engine
                        or error.worker != f"{expected_engine}:{kwargs.get('worker_id')}"):
            record.update(state="indeterminate", completedAt=store.now_ms(),
                          error={"type": type(error).__name__, "message": str(error),
                                 "workerId": error.worker_id, "worker": error.worker})
            store.write_json_atomic(path, record)
            raise ControllerOperationIndeterminate(
                f"partial create returned foreign original worker identity: {operation_id}",
                operation_id=operation_id,
            ) from error
        stopped_result = (
            _stopped_create_result(client, error, kwargs, operation_id, method=method)
            if stopped and method in _CREATING_WORKER_METHODS else None
        )
        record.update(
            state=("stopped" if stopped else
                   "rejected" if isinstance(error, _SAFE_REJECTIONS) else "indeterminate"),
            completedAt=store.now_ms(),
            error={
                "type": type(error).__name__, "message": str(error),
                **({
                    "short": error.short, "reason": error.reason,
                    "phase": error.phase,
                } if budget_stopped else {}),
                **(_api_error_fields(error)
                   if isinstance(error, WorkerApiError) else {}),
                **({
                    "short": error.short, "state": error.state,
                    "operationId": error.operation_id,
                    "apiError": _api_error_fields(error.api_error),
                } if isinstance(error, CompactFailed) else {}),
                **({
                    "workerId": error.worker_id, "worker": error.worker,
                    "engine": error.engine, "phase": error.phase,
                    "deliveryId": error.delivery_id,
                    "deliveryStatus": error.delivery_status,
                    "notificationIntent": error.notification_intent,
                    "causeType": type(partial_cause).__name__ if partial_cause else None,
                    "apiError": _api_error_fields(partial_cause)
                    if isinstance(partial_cause, WorkerApiError) else None,
                    "reason": partial_cause.reason if isinstance(partial_cause, BudgetStopped) else None,
                    "short": error.short,
                } if partial else {}),
            },
            result=stopped_result,
        )
        store.write_json_atomic(path, record)
        if method in _CREATING_WORKER_METHODS and stopped and isinstance(stopped_result, dict):
            stopped_owner = _verified_created_owner(
                client, stopped_result, kwargs, operation_id, method=method,
            )
            _register_worker(runtime, client, stopped_owner, stopped_result, kwargs, task_context, operation_id)
        store.append_event(
            runtime.run_dir, "worker-operation-error", operationId=operation_id,
            state=record["state"], errorType=type(error).__name__,
        )
        raise
    record.update(state="accepted", acceptedAt=store.now_ms(), result=result)
    store.write_json_atomic(path, record)
    if method in _CREATING_WORKER_METHODS:
        _register_worker(runtime, client, created_owner, result, kwargs, task_context, operation_id)
    store.append_event(runtime.run_dir, "worker-operation-accepted", operationId=operation_id, method=method)
    return result


__all__ = [
    "ControllerInput",
    "Task",
    "TaskFuture",
    "TaskOutput",
    "controller",
    "current",
    "observe_worker_call",
    "task",
    "task_output",
]
