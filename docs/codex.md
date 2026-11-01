# Codex workers

Codex workers use the same commands as Claude workers: create one with
`ccx new --engine codex`, then `send`, `wait`, `report`, `attach` and the rest.
This page covers what's different.

## Creating one

```bash
C=$(ccx new --engine codex --cwd ~/repo --model gpt-6-luna --effort low \
      "Inspect the failing tests")
```

- **Models** use their full ids (`gpt-6-luna`, `gpt-6.1-sol`, ...). An unknown
  model is refused before anything is created, with the list of valid ones.
  Without `--model` and `--effort`, Codex's own configuration decides.
- **Project context** works as it does in Codex: the worker reads `AGENTS.md`,
  skills and configuration from its folder. ccx doesn't change your Codex login
  or configuration.
- **Permissions** default to sandbox `danger-full-access` and approval policy
  `never`. `--permissions restricted`, or `--sandbox` and `--approval-policy`,
  gate it. See [Permissions](permissions.md).
- `ccx set` changes the name any time, and the model and effort while the worker
  is idle or stopped. They apply from its next turn.

## The shared daemon

All Codex workers in a store share one daemon (`ccx.codex.daemon`) and one
`codex app-server` process.

- It starts with the first Codex command that needs it, and stops 15 minutes
  after its last worker is unloaded (`CCX_CODEXD_LINGER`, in seconds).
- A worker is unloaded after 30 minutes idle (`--idle-timeout`; `0` keeps it
  loaded). Its thread stays in Codex, and the next `send` loads it again.
- `ccx shutdown --engine codex` unloads every worker and stops the daemon. Its
  log is `runtime-metadata/codexd.log` inside the store.

## Codex as the orchestrator

A Codex session can't block on long commands: Codex moves a command to the
background after about a minute. So a Codex parent uses notifications instead of
`ccx wait`:

```bash
ccx send "$W" --notify-parent "Run the full test suite and report failures"
```

The command returns once the message is accepted, and ccx wakes the parent with
a new turn when the worker finishes. `ccx notify "$W"` does the same for a worker
that's already running. `--settle` also waits for the workers *that* worker
started with `--notify-parent`, so the parent hears back once the whole subtree
is done. ccx refuses blocking waits from a Codex parent with a message saying
what to use instead.

**Which sessions can be woken:** a ccx Codex worker, or your own Codex app or
terminal session whose thread is loaded on Codex's shared app-server. ccx
checks this before accepting `--notify-parent`; if refused, use `--wait` or
`ccx wait` instead. For a shared-server session, set `CCX_ROOT` and `PATH` in
the server's environment; setting them only in the terminal won't reach its
commands.

Keep your own Codex thread open to receive notifications. ccx won't resume it
in the background; notifications expire an hour after becoming ready for
delivery. Read missed results with `ccx report`. Idle-unloaded ccx parents
reload automatically; killed or capped parents receive notifications after you
revive them. Trust the notification's `status`; `workerState` can still say busy.

**Sandbox:** ccx starts processes and writes outside the workspace, so it can't
run inside Codex's sandbox. Inside the sandbox it refuses at once and says so.
Run ccx commands with escalated permissions, or start the parent Codex with full
access.

## Attach

`ccx attach "$C"` runs the real Codex TUI against the worker's live thread
(`codex resume --remote`), with the worker's model and effort and ccx's status
bar at the bottom. Ctrl-Z detaches and leaves the worker running; `/quit` also
exits. Turns you type there run through the same daemon, so `ccx report`
shows their replies.

## Differences from Claude

- **Steering:** success confirms placement in the running turn, without waiting
  for its reply. If delivery is uncertain, use `ccx reconcile` with its delivery id.
- **Input:** Codex asks for approval of commands and file changes (on a gated
  worker), which you answer with `approve`/`reject`. There is no `answer` or
  `respond`, and `reject --reason` is Claude-only.
- **Context:** `peek --status` shows tokens used and the window percentage.
- **History** comes from the Codex app-server; ccx doesn't parse Codex's own
  session files. `ccx logs --jsonl` and `--format rendered` are Claude-only.

## Known limitations

- **The app-server protocol can change.** See
  [Troubleshooting](troubleshooting.md#after-upgrading-claude-code-or-codex).
- **An import is a fork.** `ccx import` copies a Codex thread into a new worker;
  its latest turn must have completed. Later turns in either one don't reach
  the other. Like any fork, it reads the original thread's history: archiving
  the original is fine, but deleting it breaks the worker.
- **Commands can outlive `stop` and `kill`.** Use
  `ccx shutdown --engine codex` to stop their app-server.
- **Questions and MCP forms are unsupported; permissions are fixed at creation.**
  See [Permissions](permissions.md#known-limitations).
- **Attach:** Codex may show its own startup prompts, and model or permission
  changes made in the TUI don't change what ccx uses for its own sends.
