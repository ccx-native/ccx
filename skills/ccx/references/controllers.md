# ccx controllers

A controller is a Python script that drives ccx workers. Its `@task` results
are recorded, so a run can resume and skip finished steps.

- [A complete controller](#a-complete-controller)
- [Replay rules](#replay-rules)
- [Structured results through variables](#structured-results-through-variables)
- [Checkpoints and input](#checkpoints-and-input)
- [Loops and failures](#loops-and-failures)
- [Before you finish](#before-you-finish)
- [Python API](#python-api)
- [CLI](#cli)

## A complete controller

Review two areas in parallel, stop for a decision, then apply what the user
approves in a worktree:

```python
from ccx import WorkerClient, controller, current, task

workers = WorkerClient()

@task
def review(area):
    shared = {"result": {"done": False, "findings": []}}
    workers.create(
        engine="codex", cwd=str(current.workspace), name=f"review-{area}",
        prompt=f"Review {area} without editing. With python3, append findings "
               "to result['findings'], then set result['done'] = True last.",
        variables=shared, wait=True)
    result = shared["result"]
    if not result["done"]:
        raise RuntimeError("Review result is incomplete")
    return result

@task(workspace="worktree", commit="Apply approved fixes")
def implement(input_id, instruction, findings):
    created = workers.create(
        engine="codex", cwd=str(current.workspace), name=f"fix-{input_id[:8]}",
        prompt=f"Apply only this: {instruction}\nFindings:\n{findings}",
        wait=True)
    return {"worker": created["worker"]}

@controller(name="review-fix", workspace="/repo")
def main():
    areas = ("api", "storage")
    futures = [review.submit(area) for area in areas]     # start all, fixed order
    findings = {area: f.result() for area, f in zip(areas, futures)}

    if not current.inputs:                                # first run: ask
        current.report(status="needs-review", checkpoint="findings", data={
            "findings": findings,
            "resume": f"ccx controller resume {current.run_id} --note '...'"})
        return
    for item in current.inputs:                           # after resume: act
        implement(item.id, item.comment, findings)
    current.report(status="done", data=findings)

if __name__ == "__main__":
    main()
```

On `ccx controller resume RUN --note "Fix the SQL injection only"`, both
`review` tasks return their recorded results at once, and the note arrives as
`item.comment`.

## Replay rules

Code outside tasks runs again on every resume; a task that finished runs once.

- Outside tasks, don't read the clock, random values, files, Git state or
  workers. Do it in a task and use the returned value.
- Outside tasks, branch only on task results, script arguments and
  `current.inputs`.
- Call tasks in the same order with the same arguments every time. A
  different call raises `ControllerReplayMismatch`.
- A task's result never changes. To act on new input, call a new task with
  `item.id`; don't expect a recorded task to return something else.

```python
files = subprocess.check_output(["git", "ls-files", "src"], text=True).split()
for f in files: review.submit(f)       # wrong: a different list on resume breaks replay

@task
def list_files():
    return subprocess.check_output(["git", "ls-files", "src"], text=True).split()
for f in list_files(): review.submit(f)   # right: the list is recorded once
```

Resume needs the script unchanged and the same script arguments. After editing
the script, start a new run; it starts from the top. Workers keep running when
a controller stops, and ordinary `ccx` commands work on them.

## Structured results through variables

Pass `variables=` instead of parsing worker prose. Inside the worker, plain
`python3` or `python` reads and updates those names; `command python3` runs
normal Python.

- Read results from the dict you passed: a worker can rebind a top-level name.
- Set a sentinel such as `done: False` and ask the worker to set it last, so an
  untouched value can't pass as an empty result.
- Live variables last for one attempt. Keep the worker call and the JSON you
  return from it in the same task; otherwise a replay raises
  `ControllerLiveStateLost`.
- Return small JSON values (verdicts, paths, worker handles), not transcripts.

## Checkpoints and input

`current.report(...)` publishes status; it doesn't stop the run. To stop for a
decision, report a stable `checkpoint` id with what's needed to decide
(findings, options, the resume command), then return from the controller.
Workers stay alive meanwhile, so resuming days later is fine.

```bash
ccx controller resume RUN --note "..."                      # resume with a note
ccx controller resume RUN --worker codex:<uuid> --comment "..."
ccx controller input RUN --note "..."                       # record without resuming
```

Input is recorded for the controller; it isn't sent to any worker. Loop over
every `current.inputs` item in order and pass `item.id` to the task that
handles it.

## Loops and failures

Bound every loop, and let code decide when it's done rather than a worker's
claim:

```python
@task
def fix_round(n, failing):
    workers.send(fixer, f"Fix these failures:\n{failing}", wait=True)
    run = subprocess.run(["pytest", "-q"], cwd=str(current.workspace),
                         capture_output=True, text=True)
    return {"passed": run.returncode == 0, "failing": run.stdout[-2000:]}

result = {"passed": False, "failing": initial}
for n in range(1, 4):
    result = fix_round(n, result["failing"])
    if result["passed"]:
        break
else:
    current.report(status="stuck", checkpoint="tests-failing", data=result)
    return
```

- A task that raised or was interrupted runs again from its start on resume;
  finished tasks, including parallel siblings, stay recorded. A worktree task
  reuses its worktree, so write prompts that check the current state first.
- A worker call can raise `ControllerOperationIndeterminate` (acceptance
  unknown), `SessionWaiting` (input needed) or `TurnIncomplete`. `create` wraps
  first-turn failures in `CreatePartialFailure`; retain its worker and delivery
  ids. Catch failures inside the task, inspect the worker and return a result
  for a checkpoint before deciding whether to send again.
- Don't wrap task calls in a broad `except Exception`: it would also swallow
  replay errors.
- Quota is a reading, not a reservation. Check it right before a wave with
  `evaluate_quota(workers.usage("claude"), {"five_hour": 70})` and checkpoint
  if `allowed` is false; `caps={"5h": 80}` on `create` or `send` stops a turn.

## Before you finish

- `python -m py_compile SCRIPT` passes.
- Worker calls, and anything time-, file- or Git-dependent, happen inside tasks.
- Every task returns small JSON; every loop has a bound and a checkpoint for
  hitting it.
- Irreversible actions (push, deploy, messages) follow a checkpoint, or the
  task checks whether they already happened.

## Python API

Everything below is importable from `ccx`. Worker arguments are the handle a
create returns (`claude:<uuid>` / `codex:<uuid>`) or the full UUID; prefixes
and names are not accepted. Results are plain dicts.

**Controller building blocks**

| Name | Use |
|---|---|
| `@controller(name=None, workspace=None)` | The entry point. `workspace` is the base folder (default: where `ccx controller run` started). |
| `@task(name=None, workspace="shared", commit=False)` | A recorded step. `workspace="worktree"` runs it in its own Git worktree; `commit="message"` commits its changes there. |
| `task.submit(*args)` | Start a task in parallel; `.result()` waits for its value. |
| `task_output(value=..., report=...)` | Return JSON `value` plus prose `report`; on replay the task returns the same `.value` and `.report`. |
| `current.workspace` | The task's folder inside a task, else the controller's. |
| `current.run_id`, `current.controller_name`, `current.run_dir` | Run identity and its storage folder. |
| `current.inputs`, `current.latest_input` | Recorded input: `id`, `created_at`, `worker`, `worker_name`, `comment` (a `--note` has `worker=None`). |
| `current.workers` | Workers this run created. |
| `current.report(status, checkpoint=None, summary=None, report=None, data=None)` | Publish the run's status; `data` must be JSON. |
| `evaluate_quota(usage, limits)` | Compare a `workers.usage(...)` reading with limits such as `{"five_hour": 70}`; returns `allowed` and details. |

**`WorkerClient()` methods**

| Method | Use |
|---|---|
| `create(engine, cwd, prompt=None, name=None, model=None, effort=None, permissions="full", wait=False, timeout=None, idle_timeout=None, caps=None, variables=None, variables_from=None, worker_id=None)` | New worker. Returns `worker`, `worker_id`, `engine`, `state`, and with `prompt` + `wait=True` `reply` (the reply text) and `report`. |
| `send(worker, text, wait=True, timeout=None, caps=None, variables=None, delivery_id=None)` | New turn on an idle or stopped worker. `reply` holds the reply text when there is one. |
| `steer(worker, text, wait=True, timeout=120, delivery_id=None)` | Add to a busy worker's running turn. |
| `wait(worker, timeout=None)` | Until idle, waiting for input or stopped. A timeout is `status="timeout"`, not an exception. |
| `report(worker, max_chars=None, turn_ref=None)` | The latest finished reply: `text`, `truncated`, `turn_ref`. Raises `SessionUnavailable` when no turn has finished. |
| `status(worker)`, `list(engine=None, cwd=None, state=None, parent=None, limit=None)` | Recorded state; nothing is sent. |
| `context(worker)` | Carried context: `tokens_used`, `window_size`, `percent_used`. |
| `stop(worker)`, `kill(worker)`, `resume(worker)` | Cancel the turn; stop the runtime; reload without a turn. |
| `fork(worker, prompt=None, name=None, wait=False, ...)` | New worker from a copy of a settled worker's history. |
| `compact(worker, wait=True)` | The engine's own compaction. |
| `approve(worker, request_id=None)`, `reject(worker, request_id=None, reason=None)` | Resolve a permission request. |
| `set_model(worker, model)`, `set_effort(worker, effort)`, `set_name(worker, name)` | Settings for future turns. |
| `variables(worker, name=None)` | Read a worker's durable JSON variables. |
| `usage(engine=None)` | Account quota for one or both engines. |
| `conversation_page(worker, cursor=None)`, `events(worker, after=None)` | Bounded history and lifecycle events. |

`caps` takes `{"5h": PCT, "7d": PCT}` (Claude also `"opus"`).

**Exceptions** (all subclass `CcxError`): `ControllerReplayMismatch` (task calls
changed on resume), `ControllerLiveStateLost` (live variables from a failed
attempt), `ControllerOperationIndeterminate` (acceptance unknown),
`SessionWaiting` (input needed), `TurnIncomplete` (the turn didn't finish),
`CreatePartialFailure` (creation didn't finish; it carries the worker id),
`ControllerLaunchRequired` (the script wasn't started by `ccx controller run`).

Full reference: [Python API](https://github.com/oneferdev/ccx/blob/main/docs/python/README.md)
and [controllers guide](https://github.com/oneferdev/ccx/blob/main/docs/controllers.md).

## CLI

Options go before the script; arguments after it go to the script.

```bash
ccx controller run [--notify-parent] [--name NAME] SCRIPT [ARGS...]
ccx controller resume [--notify-parent] RUN [ARGS...]
ccx controller status RUN                  # tasks, worktrees and workers
ccx controller report RUN [--json]         # --json includes the reported data
ccx controller logs RUN
ccx controller stop RUN                    # workers keep running
ccx controller list
```

A Codex parent adds `--notify-parent` and ends its turn (`ccx controller
notify RUN` follows a run already going); Claude or a person waits by default.
`RUN` is a controller name (its newest run) or a unique run-id prefix. Launch
through `ccx controller run`, never directly with Python. ccx never merges,
pushes or removes worktrees: review and integrate the results yourself.
