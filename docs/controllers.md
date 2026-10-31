# Controllers

A controller is a Python script that drives several workers and survives being
interrupted. You write the plan as ordinary code (calls, branches, loops), and
ccx records each finished step. If the script stops halfway, `resume` runs it
again from the top and skips the steps that already finished.

Use one when a flow has several mechanical steps between decisions. For a single
create, wait and report, the CLI or `WorkerClient` is enough.

## An example

Run two investigations in parallel and publish their reports:

```python
# review.py
from pathlib import Path
from ccx import WorkerClient, controller, current, task, task_output

workers = WorkerClient()

@task
def investigate(area: str):
    created = workers.create(
        engine="codex", cwd=str(current.workspace),
        model="gpt-6.1-sol", effort="medium", name=f"investigator-{area}",
        prompt=f"Inspect the {area} code in this repo and report the risks.",
        wait=True,
    )
    return task_output(value={"worker": created["worker"], "area": area},
                       report=created["reply"])

@controller(name="review", workspace=Path("/repo"))
def main():
    pending = [investigate.submit(area) for area in ("auth", "billing")]  # run in parallel
    findings = [future.result() for future in pending]
    current.report(status="completed", summary="Both areas reviewed.",
                   report="\n\n".join(finding.report for finding in findings))

if __name__ == "__main__":
    main()
```

The workers keep running after the controller ends. Find their handles in
`ccx list` to `peek`, `send` or `kill` them; names such as `investigator-auth`
are labels, not command targets.

## Running it

```bash
ccx controller run review.py                 # run and wait for it
ccx controller run --detach review.py        # return once it has started
ccx controller run --notify-parent review.py # from a Codex parent: wake it when done
ccx controller run --name staging review.py  # a separate run of the same script
ccx controller run review.py --arg value     # arguments after SCRIPT go to the script
```

Running `python review.py` directly is refused: the script needs the context
`ccx controller run` gives it.

```bash
ccx controller list            # every controller and its newest run
ccx controller status review   # tasks, worktrees and workers of the newest run
ccx controller report review   # the latest report the script published
ccx controller logs review     # the script's stdout and stderr (--follow to keep reading)
ccx controller wait review     # wait for the run to finish
ccx controller stop review     # stop the script; its workers keep running
```

The example's controller name is `review`. A name selects the newest run;
a unique run-id prefix selects one run
(`ccx controller runs review` lists them).

A Codex parent can't block on long commands (see [Codex workers](codex.md)), so
it uses `--notify-parent` on `run` and `resume`, or `ccx controller notify` for a
run that's already going. ccx wakes it with a new turn when the run finishes.
`wait` and `logs --follow` are refused from a Codex parent.

## How resume works

`ccx controller resume` runs the script **from the top again**. Each `@task`
call that already finished returns its recorded result without running again.
Everything outside a task runs again.

Code outside tasks must reach the same task calls, in the same order, with
the same arguments, or ccx raises `ControllerReplayMismatch`:

- Do anything that could change the plan (reading a worker, the clock, random
  values, a directory listing) **inside** a task.
- Return from a task what later steps need. Its return value is what survives.
- The script must be unchanged since the run started. After editing it, start a
  new run.

Worker calls inside tasks are saved too. If a call's outcome can't be verified
on resume, ccx raises `ControllerOperationIndeterminate` without repeating it.

## Reports and your input

`current.report(status=..., checkpoint=..., summary=..., report=..., data=...)`
publishes the run's latest report, which `ccx controller report` prints. It
doesn't stop the script. The run's lifecycle (running, succeeded, failed,
stopped) is tracked separately from the status you report.

To continue a run with direction from you, resume it with input:

```bash
ccx controller resume review --note "Quota reset; continue"
ccx controller resume review --worker "$W" --comment "Use option B for auth"
ccx controller input review --note "Staging is down until 14:00"   # record without waking
```

ccx doesn't pass your input to any worker. The script reads it from
`current.inputs`, oldest first, and decides what to do:

```python
for item in current.inputs:
    follow_up(item.id, item.worker, item.comment)
```

Loop over all of `current.inputs` rather than only the latest one, so earlier
task calls keep the same arguments on the next resume. A `resume` without input
just continues after a crash or stop.

## Worktrees

`@task(workspace="worktree")` runs a task in its own Git worktree of the
controller's repository (`current.workspace`), so parallel tasks can edit the
same repo without colliding. `commit=True` commits the task's changes when it
succeeds; a string sets the commit message.

ccx never merges, pushes or deletes these worktrees and branches. Integrating
the results is up to you.

## Variables

Pass live Python objects with `variables=` inside a task, retain the mapping,
and read it after the worker finishes. Objects last only while the controller
process is alive; resume raises `ControllerLiveStateLost` for calls that relied
on lost objects. See [Python and controllers](variables/python.md) for an
example, sharing and failure behavior.

## Recovering a run

A run can end early: the script raised, you ran `controller stop`, or its
process died (a reboot, a killed terminal). Its workers keep running either way.

```bash
ccx controller status review    # lifecycle: failed, stopped, or unavailable (process gone)
ccx controller logs review      # the traceback, if the script raised
ccx controller resume review    # run again; finished tasks are skipped
```

- **Resume is refused while the run is still running** and after the script has
  changed. To recover from a bug in the script, fix it and start a new run with
  `ccx controller run review.py` (the old one stays in `ccx controller runs review`).
  Tasks the old run finished run again in the new one.
- **To steer the retry**, resume with `--note` or `--worker ... --comment`
  (see [Reports and your input](#reports-and-your-input)).

## When something goes wrong

- **`ControllerOperationIndeterminate`:** a worker call may or may not have
  landed. Catch it inside the task, look at the worker (`peek`, `report`), and
  publish a report for a human to decide. Repeating the call could run it twice.
- **`ControllerReplayMismatch`:** the task calls changed between runs. Fix the
  script, or start a new run with `--name`.
- **`ControllerLiveStateLost`:** see [Variables](#variables).
- **A usage cap stop** raises `BudgetStopped` from `send`; `create` wraps it in
  `CreatePartialFailure`. Catch it inside the task and publish a report.

Handling has to be in the script before the run starts: a task that should
survive these needs to catch them in its own body.

Runs, inputs, logs and reports are kept under `~/.ccx/controllers/<name>/`.
Each controller name stays bound to one script path.

## Known limitations

- Code outside tasks runs again on resume; worker calls with unknown outcomes
  aren't retried automatically. See [How resume works](#how-resume-works).
- **Live variables don't survive** the controller process, and changes aren't
  undone on failure. See [variable limitations](variables/python.md#known-limitations)
  and [supported shell calls](variables/troubleshooting.md#which-calls-see-variables).
- **Worktrees accumulate.** ccx never removes them; prune them yourself.
- **`controller status` lists a worker only once its `create` returns.** While
  a `create(..., wait=True)` runs, or if the controller stops during it, find
  the worker with `ccx list`.
