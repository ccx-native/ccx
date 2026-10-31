# Architecture

A map of processes, packages and stored state for contributors.
The code and tests give the details.

## What runs

```text
ccx CLI / Python API / ccx rpc
   │
   ├── Claude worker ──► ccx host <worker-id>          one detached process per worker
   │                        └── claude (real interactive Claude Code, in a PTY)
   │                              hooks + status line ──► ccx hooks-relay / statusline-relay
   │                                                       └── back to the host's socket
   │
   ├── Codex workers ──► codexd (python -m ccx.codex.daemon)   one per store
   │                        └── codex app-server --stdio       many native threads
   │
   └── notifications ─► notifyd (python -m ccx.notify)         one per store
                           wakes Codex parents when a worker's turn settles
```

- **Claude:** ccx starts the real `claude` TUI in a pseudo-terminal, owned by a
  per-worker host process. Prompts are pasted into the TUI; Claude Code's own
  hooks and status line report back to the host through small relay commands.
  Replies are read from Claude Code's own transcript. `ccx attach` joins the same
  PTY through the host, so you and ccx share one live session.
- **Codex:** one `codexd` per store drives one `codex app-server --stdio` and
  multiplexes every ccx-owned thread over it. `ccx attach` runs the real Codex
  TUI against that live thread (`codex resume --remote`).
- **Controllers:** `ccx controller run SCRIPT` starts a separate Python process
  that drives workers through the Python API and saves each `@task` result.
  `ccx shutdown` leaves controllers running; `ccx controller stop` ends one.
- Nothing runs between commands except these processes; each starts on demand.
  [Background processes](machine-changes.md#background-processes) says when each
  stops.

## Package map

| Folder | Owns |
|---|---|
| `ccx/cli/` | Argument parsing (`parser.py`), the entry point (`main.py`), and command handlers in `commands/`. Codex command variants are in `codex.py`, controller commands in `controller.py`, skills install in `skills.py`. |
| `ccx/ops/` | Engine-neutral operations that every surface (CLI, Python, RPC) shares: create, send, wait, report, fork, compact, cancel, input answers, conversation paging, event feeds. |
| `ccx/claude/` | Everything Claude-specific: spawning the host (`spawn.py`), the host itself (`host/`: PTY, hooks, events, subagents), delivery receipts, transcript parsing, context, usage, attach. |
| `ccx/codex/` | Everything Codex-specific: the daemon (`daemon/`), the app-server protocol, deliveries and evidence, reports, settle, attach, usage reads. |
| `ccx/store/` | The on-disk store: the sealed root and generation (`root.py`), records and locks (`store.py`), worker identity and prefix discovery, paths, liveness. |
| `ccx/notify/` | Parent notifications: registry, selection, settlement and delivery, and the daemon. |
| `ccx/controllers/` | Python controllers (`controller.py`, `store.py`) and worker variables (`live_vars.py`, `live_vars_runner.py`). |
| `ccx/rpc/` | The local JSON protocol: validation and dispatch (`local_rpc.py`), the `--serve` transport, socket checks, result views. |
| `ccx/system/` | Platform and process helpers (Linux and macOS in `oslayer.py`), bounded previews, sanitizing. |
| `ccx/workers.py`, `client.py`, `codex/client.py` | The public Python clients: engine-neutral `WorkerClient`, and the Claude and Codex clients under it. Types and errors are in `types.py`, `codex/types.py` and `errors.py`. |

Where to add things:

- **A command or flag:** the parser entry in `cli/parser.py`, a handler in
  `cli/commands/`, and the behavior in `ops/` so Python and RPC can share it.
  Engine-only flags are refused in `cli/main.py` (`_native_handler`).
- **An RPC operation:** `rpc/local_rpc.py`, calling the same `ops/` function.
- **Engine behavior:** `claude/` or `codex/`, behind an `ops/` function.

## State on disk

Everything lives in one store, `~/.ccx` by default (`CCX_ROOT` selects
another). The first command that uses it creates it; daemons and hosts never do.
Stores never share state.

```text
$CCX_ROOT/
  identity/current.json            points at the active generation
  identity/generations/g-<id>/     the store: everything about workers
    owners/  claims/               who owns each worker id
    workers/claude/<worker-id>/    record, settings, host events, delivery receipts, host log
    workers/codex/<worker-id>/     record, turn evidence
    receipts/  semantic-deliveries/  delivery receipts by id
    variables/                     worker variables and the shell bridge files
    runtime-metadata/              daemon pid files, codexd.log, native-effect journals
  run/                             live sockets: one per Claude host, codexd's
  notify/                          notifyd registrations, lock and log
  controllers/<name>/runs/<id>/    controller journal, task records, reports, worktrees
  trust-ledger.json                folder trust ccx added to Claude's config
  usage-cache.json, usage-source.json
```

The native engines keep their own data as usual: Claude Code under
`~/.claude/` (ccx reads the transcripts there), Codex under `~/.codex/`.
[What ccx changes on your machine](machine-changes.md) lists every file ccx
writes outside its root.

## Rules that hold everywhere

- **A worker is its full UUID.** Handles are `claude:<uuid>` or `codex:<uuid>`.
  The CLI also accepts a unique prefix of at least 8 hex characters, resolved
  once, before any effect (`store/discovery.py`). Names are labels, never targets.
- **One active generation.** Every process pins the generation it started with
  and refuses to continue if it changes (`StaleGeneration`), so it never writes
  to a store it didn't start in.
- **Retries keep their ids.** Sends and steers use a delivery id; input responses
  use a response id. Reusing an id with the same content avoids duplicate
  delivery. See [retry identities](rpc/protocol.md#retry-identities).
- **Reads don't wait.** `report`, `peek`, `logs` and `vars` return the best
  available answer, with a short note when it's partial; `wait` is the only
  read that waits for a turn. A read fails when its source is unavailable.
- **The native engine is the truth.** ccx records what it sent and what it saw,
  but turn state, replies and context come from Claude Code's transcript and
  hooks or from Codex's app-server, not from ccx's guesses.
- **Shared operations.** CLI, Python and RPC use `ops/` for worker operations;
  each surface defines its own arguments and result format.

## Known limitations

- **Claude depends on the interactive TUI.** ccx drives the real Claude Code
  UI through a PTY and its hooks, so a Claude Code release that changes
  pasting, slash commands or hook payloads can break ccx until it adapts.
- **Codex depends on the app-server protocol**, which is still changing.
  `scripts/check_codex_protocol.py` compares the installed Codex's schema with
  the one ccx expects.
- **Local and single-user.** Workers run on this machine under your user; there
  is no remote mode. Linux and macOS are supported; on Windows use WSL.
- **One store at a time.** A process works in one store; workers of different
  stores can't share variables, notifications or controllers.
