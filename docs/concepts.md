# Concepts

Workers, turns and replies are shared across the CLI, Python and RPC.
Use `ccx help COMMAND` for flags.

## Workers and engines

A **worker** is a Claude Code session or a Codex thread that ccx runs for you.
The **engine** is which of the two it is. Both are the real native tools, logged
in with your own accounts: ccx starts them, sends them prompts and reads their
replies.

- A worker's **handle** is `claude:<uuid>` or `codex:<uuid>`, printed by
  `ccx new`. Worker commands take the handle or the bare UUID. The CLI also
  accepts a unique prefix of at least 8 lowercase hex characters.
- A worker's **name** (`ccx new -n`, `ccx set --name`) is a label for lists and the
  dashboard. It is never used to pick a worker.
- `ccx attach` opens a worker in the engine's own interface so you can work in
  it directly. ccx keeps running the worker, and other commands keep working
  while you're attached.
- `ccx import` brings in a session you started outside ccx, keeping its
  history. A Claude Code session moves into ccx, so stop using it in the old
  terminal. A Codex thread is copied into a new worker, and the original stays
  as it was.

## States

| State | Meaning | What you can do |
|---|---|---|
| `starting` | The engine is starting or reloading the session. | Wait. |
| `idle` | Ready for a new turn. | `send`, `compact`, `set`, `fork`. |
| `busy` | A turn is running. | `steer` to add to it, `stop` to cancel it, `wait`. |
| `waiting` | The turn needs input: a question, a permission request or an MCP form. | `answer`, `approve`/`reject`, `respond`. |
| `stopped` | Not running. The history is kept. | `send` revives it; so does `resume`, without a prompt. |
| `failed` | The engine process ended unexpectedly. | `send` usually revives it; `peek` shows what happened. |
| `unavailable` | ccx can't tell the state right now. | Check again; `peek` explains why. |

JSON output also has `native_state`, the engine-level reason behind the state.
For example `stopped` covers `idle-killed` (unloaded after idling), `dead`
(killed) and `budget-stopped` (stopped by a usage cap).

## Talking to a worker

- `send` starts a new turn on an idle worker. A busy worker or one waiting for
  input is refused, never queued.
- `steer` adds a message to the turn that is running.
- `answer`, `approve`/`reject` and `respond` reply to whatever the turn is waiting
  for.
- `stop` cancels the running turn; the worker stays up. It doesn't end
  [background work](#background-work) the turn started.

Each `send` or `steer` has a **delivery id**, generated unless you pass
`--delivery-id`. Repeating the same message with that id never delivers twice.
`ccx reconcile` looks up what happened to an id, for example after a lost
connection.

`stop`, `kill` and `shutdown` differ in how much they stop: the current turn, one
worker, or all of ccx's runtimes. None of them deletes a conversation, and the
next `send` continues where the worker left off.

For structured results shared between workers, see [Variables](variables/README.md).

## Waiting for results

How you wait depends on who you are:

- **A person, a script or a Claude Code parent:** `ccx wait` blocks until the
  worker is idle, needs input or stops; `send` waits by default.
- **A Codex parent:** Codex moves a long command to the background after about
  a minute, so it can't block on `ccx wait`. Use `--notify-parent` on `send` (or
  `ccx notify`); ccx wakes the parent with a new turn when the worker finishes.

Then read the reply with `ccx report`, or check on a worker with `ccx peek`.

## Background work

A worker can leave work running after its turn ends: a background shell
command, a Claude Monitor or async subagent, or a notification to its parent
that hasn't been delivered yet. ccx keeps a summary of it on the worker, shown
by `ccx peek` and returned as `background_task_summary` by Python
`status()` and RPC `worker.inspect`:

- `running_count` counts running commands, Monitors and subagents;
  `pending_count` counts undelivered notifications; `by_kind` splits both by
  kind.
- `items` lists up to 100 of them, oldest first, each with an `id`, `kind`,
  `status` and short `label`. `items_truncated` says how many were left out.
- `available` is false when ccx couldn't read the background state; the counts
  are then null.

When background work finishes, Claude starts a new turn to handle it. Codex
doesn't: a command still running when its turn ends shows here until it
exits. Either way, a worker isn't stopped for idling while something in it is
still running.

## Following changes

Two events report changes as they happen, so you don't need to poll:

- `background.changed` has `transition` (`started`, `event`, `finished`,
  `failed`, `stopped` or `reopened`), the `item` that changed, and the new
  `summary`. `item` is null when several things changed at once or ccx
  corrected the summary.
- `context.changed` has `context`, with the same fields as `ccx context`:
  `tokens_used`, `window_size`, `percent_used`, `as_of_turn_ref`, `as_of_ms`
  and `reason`. It comes at most every 5 seconds during a turn, and always at
  the end of a turn and around a compaction (`reason` is `compacting`, then
  `just_compacted` with `tokens_used` null until the next reading).

Each event carries the full current value, so apply it as the new state;
seeing one twice is harmless. To start a view without missing anything, read
the current event cursor first, then the worker's status, then follow events
from that cursor. After a `gap`, read the status again. Once you're following
events, prefer them over periodic status reads, which can be older.

## Engine differences

Most commands work the same on both engines. The differences:

| | Claude | Codex |
|---|---|---|
| Answer questions (`answer`) and MCP forms (`respond`) | yes | no |
| Approve or reject a permission request | yes | yes |
| `steer` can return `accepted-pending` | yes | no: placement is confirmed before success |
| `wait --next-turn` (a turn started by a background task) | yes | no |
| `--settle` (also wait for the workers it started) | no | yes |
| Context window percentage in `peek` | no: tokens only | yes |
| Usage caps, compaction, fork, attach | yes | yes |

Permissions are covered in [Permissions](permissions.md), and caps and account
usage in [Usage and caps](usage.md). For Python, [`WorkerClient.capabilities()`](python/workers.md#account)
lists what differs between the engines.

## Known limitations

- **No queue behind a busy worker.** `send` refuses; use `steer`, or wait and send.
- **Import needs an idle session.** Claude Code background agents, Claude
  sessions with a question pending, and Codex threads with a turn running
  can't be imported.
- **Requested and actual model are kept apart.** Worker status in Python and RPC
  has `requested_model` and `observed_model`; they can differ, and the second
  may be empty until the engine reports it.
- **Pending notifications can show late.** While a Codex parent isn't loaded,
  the pending notifications in its background summary can stay out of date
  until it loads again. Delivery itself isn't affected.
- **No cache verdict.** ccx doesn't report whether a worker's prompt cache is
  still warm.
