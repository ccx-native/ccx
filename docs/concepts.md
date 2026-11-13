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
  while you're attached. Several terminals can attach to the same worker.
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
- `stop` requests cancellation of the running turn; the worker stays up.
  [Background work](#background-work) can keep running.

**Images.** To send images, put each image's absolute path on a line of its own
in the message of `new`, `send`, `steer` or `fork`:

```bash
ccx send "$W" $'What is wrong in this screenshot?\n/home/me/shots/error.png'
```

A line becomes an image when it is only the absolute path of an existing .png,
.jpg, .jpeg, .gif or .webp file. Other lines stay text, including a path inside
a sentence, a `~/` or relative path, and a path to a missing file. You can send
several images, with or without text; the worker gets them in order, before the
text. Keep the files until ccx confirms delivery. On Claude, a message whose
image doesn't load within 15 seconds is rejected.

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

A worker's turn can end while a shell command, Claude Monitor or async
subagent keeps running. `ccx peek` summarizes this work and, for a Codex parent,
notifications waiting to reach it. Python `status()` returns the summary as
`background_task_summary`; RPC `worker.inspect` puts it in
`result.worker_view.background_tasks.summary`.

- `running_count` counts running commands, Monitors and subagents; `pending_count`
  counts notifications. `by_kind` breaks down the counts.
- `items` lists up to 100 active items, oldest first, with `id`, `kind`,
  `status` and `label`. `items_truncated` counts omitted items.
- `available: false` means the state couldn't be read; counts are then null.

Claude can start another turn when background work reports back. Codex commands
don't start follow-up turns; they stay in the summary until they exit. Running
background work prevents idle shutdown on both engines.

## Following changes

Follow `background.changed` and `context.changed` with `ccx events TARGET --follow`,
Python `events()` / `follow()`, or RPC `worker.events`:

- `background.changed` has `transition` (`started`, `event`, `finished`,
  `failed`, `stopped` or `reopened`), the `item` that changed, and the new
  `summary`. `item` is null for a summary-only update.
- `context.changed` has `context`, with the same fields as `ccx context`:
  `tokens_used`, `window_size`, `percent_used`, `as_of_turn_ref`, `as_of_ms`
  and `reason`. Unchanged values are skipped. During a turn, updates are at
  least 5 seconds apart; turn end and compaction publish changes immediately.
  During compaction, `reason` is `compacting`, then `just_compacted`, with
  `tokens_used` null until the next reading.

Replace the displayed summary or context with each event's full value; replaying
an event is harmless. To start a live view, get an event cursor with `tail=0`,
read the worker's status, then follow from that cursor. After a `gap`, read
status again. Prefer events to periodic status reads, which can be older.

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
- **Pending notifications can show late.** An unloaded Codex parent's summary
  can be stale until it loads again; delivery isn't affected.
- **No cache verdict.** ccx doesn't report whether a worker's prompt cache is
  still warm.
