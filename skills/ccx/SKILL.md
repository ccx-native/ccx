---
name: ccx
description: Run persistent Claude Code and Codex workers with the ccx CLI. Start workers, send follow-ups, wait or get notified, read replies, and coordinate work with Python controllers.
---

# ccx workers

If `ccx` is missing, install it with `uv tool install ccx-native` (or
`pipx install ccx-native`). The engine you pick must be installed and signed in.

Workers keep their own history and stay reachable after your conversation ends.
Use `ccx help COMMAND` for flags.

## The core loop

For Claude Code or a person:

```bash
W=$(ccx new --engine codex --cwd ~/repo -n auth-tests "Fix the failing tests in tests/auth.")
ccx wait "$W"                  # until it finishes or needs input
ccx report "$W"                # its last reply
ccx send "$W" "Now add a regression test."
```

- Keep the handle from `new`. Worker targets accept it, the full UUID or a
  unique UUID prefix of at least 8 lowercase hex characters. Names are labels
  for `ccx list`, never targets.
- Give a self-contained task and the right `--cwd`. Project instructions,
  skills, MCP servers and config come from that folder; your conversation
  isn't inherited.
- For long or literal prompts use `--file task.md`, or `--stdin <<'EOF'` with
  the delimiter quoted so the shell leaves backticks and `$VARS` alone.
- Model and effort default to the engine's settings. Claude accepts aliases
  (`opus`, `sonnet`, `haiku`); Codex needs a full model id from its model list.

## Waiting depends on who you are

- **Claude Code or a person:** use `--wait` or `ccx wait`; `send` waits by
  default. Claude can background a long wait and let its harness wake it.
- **Codex parent:** add `--notify-parent` to `new`, `send` or `compact`, or arm a
  running worker with `ccx notify TARGET`. Then carry on or end your turn: ccx
  resumes you when it settles. Don't wait, sleep or poll. This works from a ccx
  Codex worker, the Codex app, or a default `codex` terminal. A `codex` started
  with `-c`, `--profile` or `--no-daemon`, and `codex exec`, can't be woken; ccx
  refuses and says to wait instead. In the notification, trust the turn's
  `status`; `workerState` is only a hint.
  Run `ccx` with full access: it starts processes and writes outside the workspace.
- `--no-wait` returns at once and wakes nobody.

## Talking to a worker

| The worker is | Do |
|---|---|
| idle or stopped | `send`; stopped workers revive with their history. |
| busy | `steer TARGET "..."`, or let it settle before `send`. |
| waiting for input | `peek`, then `approve`/`reject` a permission request; on Claude, `answer` a question or `respond` to an MCP form. `send` is refused. |
| going the wrong way | `stop` cancels the turn, not background work it started. `kill` stops the runtime; `send` revives it with its history. |

## Reading results

- `ccx peek TARGET`: state, latest messages, tools and errors. Start here.
  `ccx peek A B --status` checks several workers at once.
- `ccx report TARGET`: the last reply.
- `ccx logs TARGET`: conversation JSON when the report isn't enough.
- `ccx list`: find workers by name, state or folder.

## Permissions

Workers default to full permissions: they can read, write and run anything your
user can, without permission prompts. Put read-only or "no edits" boundaries in
the task. Restrict permissions only if the user asks.

## Reuse, compact or start fresh

Treat a worker like a teammate with working memory: reuse it when that memory
helps the next task, compact it when a smaller context will repay the cost of
summarizing it, and start fresh when the next task doesn't benefit from it.

`ccx peek TARGET --status --json` shows both what you need:
`context.tokens_used` (what the worker carries now, not total usage) and
`last_turn_age_s`.

- **Reuse** for the same task when the history helps. Design and strategy
  conversations deserve long contexts; don't cut them for size alone.
- **Start fresh** for unrelated work or a different working set, even in the
  same repository.
- **Compact** with `ccx compact TARGET` at a clean boundary above about 150k
  tokens when most context is implementation residue (tool output) and the
  next task uses the same files or domain. Summarizing costs tokens; when in
  doubt, continue. Engines also compact automatically.

**Claude only:** treat the prompt cache as warm for one hour after the last
turn. Changing model or effort (`ccx set TARGET --model M --effort E`)
invalidates it. Above about 100k tokens with a warm cache, compact before
switching. With a cold cache, switch directly or start fresh if the history
is dispensable. These cache rules don't apply to Codex.

## Variables: structured state between workers

Workers can share JSON state without files or parsing prose. Attach it with
`--var NAME=JSON` on `new`, `send` or `fork`; inside the worker, plain
`python3` sees `NAME` as a variable it can read and change, and changes
persist. Read it back with `ccx vars TARGET NAME --json`, and give another
worker, on either engine, the same shared state with `--vars-from TARGET`.

```bash
A=$(ccx new --engine claude --cwd ~/repo --var 'plan={"steps": [], "done": false}' \
      "Draft an implementation plan. With python3, append steps to plan['steps'], then set plan['done'] = True last.")
ccx wait "$A"
ccx vars "$A" plan --json
```

Read [references/variables.md](references/variables.md) when sharing state
between workers or when `python3` doesn't see the variables.

## Controllers: programmable workflows that run unattended

A controller is a Python script that drives many workers on its own, so you
don't have to start, wait on and collect each one by hand. It's ordinary
Python: it can branch on results, loop or fan out as the work requires. Its
steps are recorded, so a run can stop for a decision and resume later without
redoing finished steps, and workers hand back structured results through
shared variables instead of prose. A few lines from one:

```python
futures = [review.submit(area) for area in ("api", "storage")]  # workers in parallel
findings = [f.result() for f in futures]                         # structured results
if not current.inputs:                                           # stop for a decision
    current.report(status="needs-review", checkpoint="findings", data=findings)
    return
```

Start one with `ccx controller run SCRIPT` (a Codex parent adds
`--notify-parent` before the script). Read
[references/controllers.md](references/controllers.md) before writing one.

## When something looks off

A wait timeout doesn't mean the work failed. Check `peek` or `report`;
for uncertain delivery, use `ccx reconcile TARGET --delivery-id ID`.
A server or watcher left running doesn't mean the task is unfinished.

Exit codes: 0 success · 1 bad input or usage · 2 not found · 3 unavailable ·
4 waiting for input · 5 refused in this state · 6 incomplete (timeout, failed
turn or uncertain delivery) · 7 usage cap · 8 Anthropic rate limit ·
9 transient API error.

More in [references/troubleshooting.md](references/troubleshooting.md).

## Also useful

- `ccx fork TARGET ["prompt"]` copies a settled conversation into a new worker.
- `ccx attach TARGET` opens the worker in a terminal; Ctrl-Z detaches.
- `--cap-5h PCT` or `--cap-7d PCT` on `new` or `send` stops that turn when
  account usage reaches PCT%. `ccx usage` shows the current levels.
