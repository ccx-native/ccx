# ccx controllers

A controller script drives workers and records finished steps. Run an existing
script through the CLI; see the [controllers guide](https://github.com/oneferdev/ccx/blob/main/docs/controllers.md)
for writing one.

## Run and inspect

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

## Resume with input

```bash
ccx controller resume RUN --note "Fix only the approved findings"
ccx controller resume RUN --worker "$W" --comment "Use option B"
ccx controller input RUN --note "Waiting for staging"   # record without resuming
```

Input goes to the controller, not directly to a worker. Resume uses the
unchanged script and the same script arguments: finished steps return their
recorded results, while failed or interrupted steps run again. After editing
the script, start a new run.
