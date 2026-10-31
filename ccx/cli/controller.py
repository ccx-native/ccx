"""CLI presentation and detached process lifecycle for Python controllers."""

from __future__ import annotations
import ccx.notify.daemon as notify_daemon

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid
from typing import Any

from ccx.controllers import store as store
from ccx.constants import EXIT_ERROR, EXIT_TURN_INCOMPLETE, EXIT_UNAVAILABLE
from ccx.controllers.controller import _host_execute
from ccx.errors import CcxError, ControllerError
from ccx.system.executable import current_executable_path
from ccx.controllers.orchestrator import codex_blocking_override_enabled, current_caller, require_notify_parent
from ccx.notify import registry as parent_notify
from ccx.workers import WorkerClient


STARTUP_TIMEOUT_S = 30.0


def _prepare_completion(args, *, notify: bool = False) -> dict[str, Any] | None:
    if notify or getattr(args, "notify_parent", False):
        try:
            return notify_daemon.prepare_notify_parent(require_notify_parent())
        except CcxError as error:
            # The shared hint names worker waits; controllers have their own.
            _fail(str(error).replace("use --wait / ccx wait TARGET instead",
                                     "use ccx controller wait TARGET instead"), error.exit_code)
    if getattr(args, "detach", False):
        return None
    try:
        caller = current_caller()
    except CcxError as error:
        _fail("caller worker identity is not proven in this CCX_ROOT; use the "
              f"worker's original root: {error}", error.exit_code)
    if caller.supports_parent_notify and not codex_blocking_override_enabled():
        _fail(
            "blocking controller waits are disabled for Codex orchestrators; "
            "use --detach for run/resume and inspect with "
            "`ccx controller status TARGET`, `ccx controller report TARGET`, "
            "or `ccx controller logs TARGET` "
            "(without --follow)"
        )
    return None


def _arm_notification(
    parent,
    record,
    *,
    attempt: int,
    source: str,
    attempt_token: str | None = None,
):
    if attempt_token is None:
        existing = next(
            (
                item
                for item in (record.get("attempts") or [])
                if isinstance(item, dict) and item.get("number") == attempt
            ),
            None,
        )
        token = existing.get("attemptToken") if isinstance(existing, dict) else None
        attempt_token = token if isinstance(token, str) and token else None
    if not attempt_token:
        raise ControllerError("selected controller attempt lacks its original token")
    operation = {
        "kind": "controller-attempt",
        "attempt": attempt,
        "attemptToken": attempt_token,
        "startupDeadlineMs": store.now_ms() + int((STARTUP_TIMEOUT_S + 15) * 1000),
    }
    return parent_notify.create_registration(
        parent=parent,
        worker_engine="controller",
        worker_id=record["runId"],
        worker_name=record.get("controller") or record.get("requestedControllerName"),
        operation=operation,
        source=source,
    )


def _emit_notification(args, verdict, registration) -> None:
    if verdict.get("controller"):
        registration = parent_notify.mutate_registration(
            registration["eventId"],
            lambda value: value["worker"].update(name=verdict["controller"]),
        ) or registration
    public = parent_notify.public_registration(registration)
    supervisor = ("" if public.get("notifier_available", True)
                  else " (supervisor currently unavailable)")
    _emit(
        {**verdict, "notify": public}, as_json=args.json,
        text=(f"{verdict.get('controller') or registration['worker'].get('name')} "
              f"{verdict['runId']}\nnotify: {public['state']} {public['event_id']}"
              f"{supervisor}\n{public['instruction']}"),
    )


def _script_args(values: list[str] | None) -> list[str]:
    result = list(values or [])
    if result[:1] == ["--"]:
        result.pop(0)
    return result


def _emit(value: dict[str, Any], *, as_json: bool, text: str | None = None) -> None:
    if as_json:
        print(json.dumps(value, indent=2, sort_keys=True))
    elif text is not None:
        print(text)


def _fail(message: str, code: int = EXIT_ERROR) -> None:
    sys.stderr.write(f"[ccx controller] {message}\n")
    raise SystemExit(code)


def _parse_verdict(stdout: bytes | None) -> dict[str, Any] | None:
    if not stdout:
        return None
    for raw in reversed(stdout.decode("utf-8", "replace").splitlines()):
        try:
            value = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            return value
    return None


def _spawn_controller(
    *, run_id: str, run_dir: Path, script: Path, resume: bool,
    script_args: list[str], startup_timeout: float = STARTUP_TIMEOUT_S,
) -> dict[str, Any]:
    verdict_path = store.launch_verdict_path(run_id)
    store.clear_stop_marker(run_id)
    try:
        verdict_path.unlink()
    except FileNotFoundError:
        pass
    argv = [
        sys.executable, current_executable_path(), "controller", "_launch",
        "--run-id", run_id, "--run-dir", str(run_dir), "--script", str(script),
        "--startup-timeout", f"{startup_timeout:g}",
        "--argv-json", json.dumps(script_args),
    ]
    if resume:
        argv.append("--resume")
    try:
        proc = subprocess.run(
            argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, start_new_session=True,
            timeout=startup_timeout + 10,
        )
    except (OSError, subprocess.SubprocessError) as error:
        return {"ok": False, "runId": run_id, "error": f"{type(error).__name__}: {error}"}
    verdict = _parse_verdict(proc.stdout)
    try:
        if verdict is None:
            detail = (proc.stderr or b"").decode("utf-8", "replace").strip()
            return {
                "ok": False, "runId": run_id,
                "error": detail or f"controller launcher exited {proc.returncode} without a verdict",
            }
        return verdict
    finally:
        try:
            verdict_path.unlink()
        except FileNotFoundError:
            pass


def cmd_internal_launch(args) -> None:
    run_dir = Path(args.run_dir)
    try:
        store.require_operation_journal_layout(run_dir)
    except ControllerError as error:
        _fail(str(error), getattr(error, "exit_code", EXIT_ERROR))
    log_path = run_dir / "controller.log"
    pid = os.fork()
    if pid == 0:
        os.setsid()
        child = os.fork()
        if child > 0:
            os._exit(0)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log = log_path.open("ab", buffering=0)
        devnull = os.open(os.devnull, os.O_RDONLY)
        os.dup2(devnull, 0)
        os.close(devnull)
        os.dup2(log.fileno(), 1)
        os.dup2(log.fileno(), 2)
        try:
            log.close()
        except Exception:
            pass
        argv = [
            sys.executable, current_executable_path(), "controller", "_host",
            "--run-id", args.run_id, "--run-dir", args.run_dir,
            "--script", args.script, "--argv-json", args.argv_json,
        ]
        if args.resume:
            argv.append("--resume")
        try:
            os.execv(sys.executable, argv)
        finally:
            os._exit(127)

    os.waitpid(pid, 0)
    deadline = time.monotonic() + args.startup_timeout
    verdict = None
    while time.monotonic() < deadline:
        verdict = store.read_launch_verdict(args.run_id)
        if verdict is not None:
            break
        time.sleep(0.05)
    if verdict is None:
        try:
            record = store.read_json(store.run_record_path(run_dir))
            lifecycle = record.get("lifecycle") or {}
            host_pid = lifecycle.get("pid")
            # A PID can exit or be reused between observation and signalling.
            # The single check immediately before SIGTERM proves the recorded
            # original identity; a second adjacent read would not close the
            # unavoidable kernel scheduling window.
            if store.process_presence(host_pid, lifecycle.get("pidStart")) == "alive":
                os.kill(int(host_pid), signal.SIGTERM)
        except (ControllerError, OSError, TypeError, ValueError):
            pass
        verdict = {
            "ok": False, "runId": args.run_id,
            "error": f"controller did not finish startup within {args.startup_timeout:g}s",
        }
    print(json.dumps(verdict, separators=(",", ":")))
    raise SystemExit(0 if verdict.get("ok") else 1)


def cmd_internal_host(args) -> None:
    run_dir = Path(args.run_dir)
    script = Path(args.script).resolve()
    try:
        script_args = json.loads(args.argv_json)
        if not isinstance(script_args, list) or not all(isinstance(item, str) for item in script_args):
            raise ValueError("argv is not a string list")
    except (json.JSONDecodeError, ValueError) as error:
        store.write_launch_verdict(
            args.run_id, {"ok": False, "runId": args.run_id, "error": str(error)}
        )
        raise SystemExit(1)

    marker_path = os.fspath(store.stop_marker_path(args.run_id))

    def stopped(_signum, _frame):
        try:
            fd = os.open(marker_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            try:
                os.write(fd, b"SIGTERM\n")
            finally:
                os.close(fd)
        finally:
            os._exit(143)

    signal.signal(signal.SIGTERM, stopped)
    raise SystemExit(
        _host_execute(
            run_id=args.run_id, run_dir=run_dir, script=script,
            resume=bool(args.resume), script_args=script_args,
        )
    )


def _identity_line(value: dict[str, Any]) -> str:
    declared = value.get("declaredControllerName")
    requested = value.get("requestedControllerName")
    return (
        f"names: declared={declared or 'unrecorded'} "
        f"requested={requested or '-'} effective={value.get('controller')}"
    )


def _attempt_line(value: dict[str, Any]) -> str:
    attempts = value.get("attempts") or 0
    start = value.get("latestAttemptStart") or {}
    if not start.get("recorded"):
        return f"attempts: {attempts} (latest start: unrecorded)"
    detail = f"latest start: {start['form']}"
    if start.get("inputId"):
        detail += f" input={str(start['inputId'])[:8]}"
    return f"attempts: {attempts} ({detail})"


def _print_summary(record: dict[str, Any], run_dir: Path) -> None:
    value = store.run_summary(record, run_dir)
    print(f"controller: {value.get('controller')}")
    print(_identity_line(value))
    print(f"run: {value.get('runId')}")
    print(f"lifecycle: {value.get('lifecycle')}")
    print(f"script-reported status: {value.get('status') or '-'}")
    print(f"checkpoint: {value.get('checkpoint') or '-'}")
    print(f"tasks: {value.get('tasksSucceeded')}/{value.get('tasks')}")
    print(f"workers: {value.get('workers')}")
    print(_attempt_line(value))
    if value.get("summary"):
        print(f"summary: {value['summary']}")


def _latest_report(record: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    metadata = record.get("latestReport")
    if not isinstance(metadata, dict):
        return None, None
    path = metadata.get("path")
    if not path:
        return metadata, None
    try:
        return metadata, Path(str(path)).read_text(encoding="utf-8")
    except OSError:
        return metadata, None


def _wait_and_follow(
    run_dir: Path, *, start_offset: int, timeout: float | None,
    as_json: bool, include_final: bool = True,
) -> int:
    started = time.monotonic()
    log_path = run_dir / "controller.log"
    offset = start_offset
    while True:
        if not as_json and log_path.exists():
            try:
                with log_path.open("rb") as handle:
                    handle.seek(offset)
                    data = handle.read()
                    offset = handle.tell()
                if data:
                    sys.stdout.write(data.decode("utf-8", "replace"))
                    sys.stdout.flush()
            except OSError:
                pass
        record = store.refresh_liveness(run_dir)
        state = (record.get("lifecycle") or {}).get("state")
        if state in store.TERMINAL_LIFECYCLES:
            break
        if timeout is not None and time.monotonic() - started >= timeout:
            if as_json:
                print(json.dumps({"timedOut": True, **store.run_summary(record, run_dir)}, indent=2))
            else:
                print(f"[ccx controller] wait timed out; controller remains {state}")
            # Same code as every other timed-out CCX wait; 4 means blocked on input.
            return EXIT_TURN_INCOMPLETE
        time.sleep(0.1)
    if include_final:
        metadata, report_text = _latest_report(record)
        if as_json:
            print(json.dumps({
                **store.run_summary(record, run_dir),
                "report": {**(metadata or {}), "text": report_text} if metadata else None,
                "error": record.get("error"),
            }, indent=2, sort_keys=True))
        else:
            if offset and not str(log_path.read_text(encoding="utf-8", errors="replace") if log_path.exists() else "").endswith("\n"):
                print()
            _print_summary(record, run_dir)
            if report_text is not None:
                print("\nreport:\n")
                print(report_text)
    return 0 if state == "succeeded" else EXIT_ERROR


def cmd_run(args) -> None:
    parent = _prepare_completion(args)
    script = Path(args.script).expanduser().resolve()
    if not script.is_file():
        _fail(f"controller script is not a file: {script}")
    launch_cwd = Path.cwd().resolve()
    script_args = _script_args(args.script_args)
    try:
        run_id, bootstrap = store.create_bootstrap(
            script, launch_cwd, script_args, controller_name=args.name
        )
        registration = _arm_notification(
            parent, store.read_json(store.run_record_path(bootstrap)),
            attempt=1, source="controller.run",
        ) if parent else None
        verdict = _spawn_controller(
            run_id=run_id, run_dir=bootstrap, script=script, resume=False,
            script_args=script_args,
        )
    except CcxError as error:
        _fail(str(error), getattr(error, "exit_code", EXIT_ERROR))
    if not verdict.get("ok"):
        if registration:
            _emit_notification(args, {**verdict, "runId": run_id}, registration)
        _fail(str(verdict.get("error") or "controller startup failed"))
    run_dir = Path(str(verdict["runDir"]))
    if registration:
        _emit_notification(args, verdict, registration)
        return
    if args.detach:
        _emit(verdict, as_json=args.json, text=f"{verdict['controller']} {run_id[:8]}")
        return
    try:
        code = _wait_and_follow(run_dir, start_offset=0, timeout=None, as_json=args.json)
    except KeyboardInterrupt:
        sys.stderr.write("\n[ccx controller] detached; controller is still running\n")
        raise SystemExit(130)
    if code:
        raise SystemExit(code)


def cmd_list(args) -> None:
    rows = store.controller_inventory()
    if args.json:
        _emit({"controllers": rows}, as_json=True)
        return
    if not rows:
        print("0 controller(s)")
        return
    print(f"{'NAME':28} {'RUNS':>4} {'LIFECYCLE':12} {'SCRIPT-REPORTED STATUS':22} LATEST")
    for row in rows:
        latest = row.get("latest") or {}
        print(
            f"{str(row['name'])[:28]:28} {row['runs']:>4} "
            f"{str(latest.get('lifecycle') or '-')[:12]:12} "
            f"{str(latest.get('status') or '-')[:22]:22} "
            f"{str(latest.get('runId') or '-')[:8]}"
        )


def cmd_runs(args) -> None:
    try:
        rows = store.run_inventory(args.name)
    except ControllerError as error:
        _fail(str(error), getattr(error, "exit_code", EXIT_ERROR))
    if args.json:
        _emit({"controller": args.name, "runs": rows}, as_json=True)
        return
    if not rows:
        print(f"0 run(s) for {args.name}")
        return
    print(f"{'RUN':8} {'LIFECYCLE':12} {'SCRIPT-REPORTED STATUS':22} {'TASKS':>7} {'WORKERS':>7} UPDATED")
    for row in rows:
        print(
            f"{str(row['runId'])[:8]:8} {str(row.get('lifecycle') or '-')[:12]:12} "
            f"{str(row.get('status') or '-')[:22]:22} "
            f"{row.get('tasksSucceeded', 0)}/{row.get('tasks', 0):<5} "
            f"{row.get('workers', 0):>7} {row.get('updatedAt')}"
        )


def _status_payload(run_dir: Path) -> dict[str, Any]:
    record = store.refresh_liveness(run_dir)
    client = WorkerClient()
    live_workers: list[dict[str, Any]] = []
    for worker in record.get("workers", []):
        if not isinstance(worker, dict) or not worker.get("worker"):
            continue
        try:
            live = client.status(worker["worker"])
        except Exception as error:
            live = {"worker": worker["worker"], "state": "unavailable", "error": type(error).__name__}
        live_workers.append({**worker, "live": live})
    return {
        **store.run_summary(record, run_dir),
        "scriptPath": record.get("scriptPath"),
        "sourceSha256": record.get("sourceSha256"),
        "attemptsDetail": [
            item for item in (record.get("attempts") or []) if isinstance(item, dict)
        ],
        "workspace": record.get("workspace"),
        "tasksDetail": record.get("tasks") or {},
        "workersDetail": live_workers,
        "worktrees": record.get("worktrees") or {},
        "latestReport": record.get("latestReport"),
        "error": record.get("error"),
    }


def cmd_status(args) -> None:
    try:
        run_dir = store.resolve_run(args.target)
        payload = _status_payload(run_dir)
    except ControllerError as error:
        _fail(str(error), getattr(error, "exit_code", EXIT_ERROR))
    if args.json:
        _emit(payload, as_json=True)
        return
    _print_summary(store.read_json(store.run_record_path(run_dir)), run_dir)
    if payload["workersDetail"]:
        print("\nworkers:")
        for worker in payload["workersDetail"]:
            live = worker["live"]
            print(
                f"  {worker.get('name')}: {worker.get('worker')} "
                f"state={live.get('state')} cwd={worker.get('cwd')}"
            )
    if payload["worktrees"]:
        print("\nworktrees:")
        for task_id, worktree in payload["worktrees"].items():
            print(
                f"  {task_id}: {worktree.get('path')} "
                f"branch={worktree.get('branch')} commit={worktree.get('commit') or '-'}"
            )


def cmd_report(args) -> None:
    try:
        run_dir = store.resolve_run(args.target)
        record = store.refresh_liveness(run_dir)
    except ControllerError as error:
        _fail(str(error), getattr(error, "exit_code", EXIT_ERROR))
    metadata, text = _latest_report(record)
    if args.json:
        _emit({
            "runId": record.get("runId"), "controller": record.get("controller"),
            "report": {**(metadata or {}), "text": text} if metadata else None,
        }, as_json=True)
    elif metadata is None:
        _fail("controller has not published a report", EXIT_UNAVAILABLE)
    elif text is not None:
        print(text)
    else:
        print(metadata.get("summary") or "controller report has no Markdown body")


def cmd_wait(args) -> None:
    _prepare_completion(args)
    try:
        run_dir = store.resolve_run(args.target)
        log = run_dir / "controller.log"
        offset = log.stat().st_size if log.exists() else 0
        code = _wait_and_follow(
            run_dir, start_offset=offset, timeout=args.timeout, as_json=args.json
        )
    except ControllerError as error:
        _fail(str(error), getattr(error, "exit_code", EXIT_ERROR))
    except KeyboardInterrupt:
        sys.stderr.write("\n[ccx controller] detached; controller is still running\n")
        raise SystemExit(130)
    if code:
        raise SystemExit(code)


def cmd_logs(args) -> None:
    if args.follow:
        _prepare_completion(args)
    try:
        run_dir = store.resolve_run(args.target)
    except ControllerError as error:
        _fail(str(error), getattr(error, "exit_code", EXIT_ERROR))
    log = run_dir / "controller.log"
    text = log.read_text(encoding="utf-8", errors="replace") if log.exists() else ""
    if args.json:
        if args.follow:
            _fail("--json and --follow cannot be combined")
        _emit({"runDir": str(run_dir), "text": text}, as_json=True)
        return
    sys.stdout.write(text)
    sys.stdout.flush()
    if args.follow:
        code = _wait_and_follow(
            run_dir, start_offset=log.stat().st_size if log.exists() else 0, timeout=None,
            as_json=False, include_final=False,
        )
        if code:
            raise SystemExit(code)


def cmd_input(args) -> None:
    try:
        run_dir = store.resolve_run(args.target)
        if args.note is not None:
            if args.worker is not None or args.comment is not None:
                raise ControllerError("--note cannot be combined with --worker or --comment")
            worker_target = None
            comment = args.note
        else:
            if not args.worker or args.comment is None:
                raise ControllerError("supply --note or both --worker and --comment")
            worker_target = args.worker
            comment = args.comment
        entry = store.append_controller_input(
            run_dir, worker_target=worker_target, comment=comment
        )
    except ControllerError as error:
        _fail(str(error), getattr(error, "exit_code", EXIT_ERROR))
    _emit(entry, as_json=args.json, text=entry["id"])


def cmd_notify(args) -> None:
    parent = _prepare_completion(args, notify=True)
    try:
        run_dir = store.resolve_run(args.target)
        # The resume lock prevents adopting a partially prepared attempt.
        with store.file_lock(run_dir / "resume.lock"):
            record = store.read_json(store.run_record_path(run_dir))
            registration = _arm_notification(
                parent, record, attempt=record["attempts"][-1]["number"],
                source="controller.notify",
            )
    except CcxError as error:
        _fail(str(error), getattr(error, "exit_code", EXIT_ERROR))
    _emit_notification(args, {"runId": record["runId"], "controller": record["controller"]}, registration)


def cmd_resume(args) -> None:
    parent = _prepare_completion(args)
    registration = None
    dispatch_started = False
    try:
        run_dir = store.resolve_run(args.target)
        if args.note is not None and (args.worker is not None or args.comment is not None):
            raise ControllerError("--note cannot be combined with --worker or --comment")
        if bool(args.worker) != bool(args.comment):
            raise ControllerError("--worker and --comment must be supplied together")
        with store.file_lock(run_dir / "resume.lock"):
            # Refuse old/missing/unknown positional journals before recording
            # inputs, attempts, notifications, or starting any controller code.
            store.require_operation_journal_layout(run_dir)
            record = store.refresh_liveness(run_dir)
            state = (record.get("lifecycle") or {}).get("state")
            if state in {"starting", "running"}:
                raise ControllerError(f"controller run is already {state}")
            script = Path(str(record["scriptPath"]))
            if not script.is_file():
                raise ControllerError(f"controller script no longer exists: {script}")
            actual_hash = store.source_sha256(script)
            if actual_hash != record.get("sourceSha256"):
                raise ControllerError(
                    "controller source changed since this run; start a new run instead of replaying"
                )
            script_args = _script_args(args.script_args)
            attempt = len(record.get("attempts") or []) + 1
            attempt_token = str(uuid.uuid4())
            if parent:
                registration = _arm_notification(
                    parent,
                    record,
                    attempt=attempt,
                    source="controller.resume",
                    attempt_token=attempt_token,
                )
            start_input_id: str | None = None
            if args.worker:
                start_form = store.ATTEMPT_START_RESUME_WORKER_COMMENT
                start_input_id = store.append_controller_input(
                    run_dir, worker_target=args.worker, comment=args.comment
                )["id"]
            elif args.note is not None:
                start_form = store.ATTEMPT_START_RESUME_NOTE
                start_input_id = store.append_controller_input(
                    run_dir, worker_target=None, comment=args.note
                )["id"]
            else:
                start_form = store.ATTEMPT_START_RESUME
            started = store.now_ms()
            store.clear_stop_marker(str(record["runId"]))

            def update(value: dict[str, Any]) -> None:
                value.setdefault("attempts", []).append({
                    "number": attempt,
                    "attemptToken": attempt_token,
                    "startedAt": started,
                    "argv": script_args,
                    "reportStartIndex": len(value.get("reports") or []),
                    "startForm": start_form, "startInputId": start_input_id,
                })
                value.setdefault("lifecycle", {}).update(state="starting", since=started, pid=None)
                value["error"] = None

            store.mutate_run(run_dir, update)
            store.append_event(
                run_dir, "run-resuming", attempt=attempt,
                startForm=start_form, startInputId=start_input_id,
            )
        log = run_dir / "controller.log"
        attach_offset = log.stat().st_size if log.exists() else 0
        dispatch_started = True
        verdict = _spawn_controller(
            run_id=str(record["runId"]), run_dir=run_dir, script=script,
            resume=True, script_args=script_args,
        )
    except (CcxError, OSError) as error:
        if registration and not dispatch_started:
            parent_notify.cancel_registration(registration["eventId"], str(error))
            _fail(f"{error}; notification {registration['eventId']} cancelled before dispatch")
        _fail(str(error), getattr(error, "exit_code", EXIT_ERROR))
    if not verdict.get("ok"):
        if registration:
            _emit_notification(args, {**verdict, "runId": record["runId"]}, registration)
        _fail(str(verdict.get("error") or "controller resume failed"))
    if registration:
        _emit_notification(args, verdict, registration)
        return
    if args.detach:
        _emit(verdict, as_json=args.json, text=f"{record['controller']} {str(record['runId'])[:8]}")
        return
    code = _wait_and_follow(
        run_dir, start_offset=attach_offset, timeout=None, as_json=args.json
    )
    if code:
        raise SystemExit(code)


def cmd_stop(args) -> None:
    try:
        run_dir = store.resolve_run(args.target)
        record = store.refresh_liveness(run_dir)
        lifecycle = record.get("lifecycle") or {}
        pid, start = lifecycle.get("pid"), lifecycle.get("pidStart")
        if lifecycle.get("state") not in {"starting", "running"}:
            raise ControllerError(f"controller is not running (state={lifecycle.get('state')})")
        if store.process_presence(pid, start) != "alive":
            raise ControllerError("controller original PID/start not proven alive; no signal")
        os.kill(int(pid), signal.SIGTERM)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and store.process_presence(pid, start) == "alive":
            time.sleep(0.05)
        presence = store.process_presence(pid, start)
        if presence == "unprovable":
            raise ControllerError("controller exit not proven after SIGTERM")
        if presence == "alive":
            if not args.force:
                raise ControllerError("controller did not stop within 5s; retry with --force")
            if store.process_presence(pid, start) != "alive":
                raise ControllerError("controller original PID/start changed before SIGKILL")
            os.kill(int(pid), signal.SIGKILL)
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline and store.process_presence(pid, start) == "alive":
                time.sleep(0.05)
            if store.process_presence(pid, start) != "dead":
                raise ControllerError("controller host remained alive after SIGKILL")
        def stopped(value: dict[str, Any]) -> None:
            stopped_at = store.now_ms()
            value.setdefault("lifecycle", {}).update(
                state="stopped", since=stopped_at, pid=None, pidStart=None,
            )
            attempts = value.setdefault("attempts", [])
            if attempts:
                attempts[-1].update(completedAt=stopped_at, lifecycle="stopped")
        record = store.mutate_run(run_dir, stopped)
        store.append_event(run_dir, "run-stopped", forced=bool(args.force))
    except (ControllerError, OSError) as error:
        _fail(str(error), getattr(error, "exit_code", EXIT_ERROR))
    _emit(store.run_summary(record, run_dir), as_json=args.json, text=str(record["runId"])[:8])


__all__ = [name for name in globals() if name.startswith("cmd_")]
