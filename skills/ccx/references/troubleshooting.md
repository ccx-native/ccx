# ccx troubleshooting

## Delivery and steering

`send` and `steer` have delivery ids (`--json`, or supply `--delivery-id`).
Retry the same message with the same id to avoid duplicate delivery.
`ccx reconcile TARGET --delivery-id ID` checks without sending; Codex also
accepts `--turn-id`.

If a `new` reply was lost, `ccx list` may find the worker. For retryable
creation, choose a UUIDv4 first and reuse it with `--worker-id`; bare `new`
always creates another worker.

A Claude `steer` watches for where the message landed:

| Status | Meaning |
|---|---|
| `consumed-current-turn` | the running turn took it |
| `started-next-turn` | it runs as the next turn |
| `accepted-pending` | ccx holds it; placement is unproven. Wait or reconcile; don't resend. |
| `unknown` | reconcile before retrying |

Pending Claude steering blocks other sends and steers; the refusal names its
delivery id. Codex steering accepts or rejects at once.

## Stopped, failed and revived workers

Idle workers stop after `--idle-timeout`. Stopped or failed workers revive on
`send`, keeping their history. Interrupted turns aren't resumed or resent:
check `report`, then send what remains.

`ccx resume TARGET` reloads without starting a turn. `ccx shutdown` stops
runtimes, preserving histories; `--engine` limits it to one engine.

## Workers waiting on background work

A Claude worker can finish a turn while a background task or monitor keeps
running, then wake for another turn. `peek` and `report` show background tasks.

To follow finite work the worker is waiting on (tests or a build):

- Claude or a person: `ccx wait TARGET --next-turn`
- Codex: `ccx notify TARGET --next-turn`

A server or watcher left running doesn't mean the work is unfinished.

## Input requests

`ccx peek TARGET` shows pending input and its request id.

- **Question (Claude):** `ccx answer TARGET "Option label"`; see help for
  multiple questions or selections.
- **Permission:** `ccx approve TARGET` or `ccx reject TARGET`.
- **MCP server form (Claude):** `ccx respond TARGET --accept --field
  name=value`, `--decline` or `--cancel`.

To follow the resumed turn, Claude/human parents add `--wait` where supported;
Codex parents use `--notify-parent`.

## Context and compaction

Right after compaction, `ccx context TARGET --json` may show `tokens_used: null`
until the next reading; `reason` explains why.

Claude compaction revives stopped workers; Codex compaction needs an idle
worker. `ccx notify TARGET` follows a compaction already running.

`ccx set TARGET --model M --effort E` needs an idle or stopped worker. Codex
uses the settings next turn. For a running, idle Claude worker, add `--restart`
to stop its runtime; the next send revives it with the new settings. A stopped
Claude worker uses them on revival. Let compaction settle before `set`.

## Usage caps

Caps stop a turn at the given account usage percentage (exit 7). Check
`ccx usage` first. Caps apply only to that message; later sends need their own.
If usage can't be read, the turn keeps running.

## Deeper inspection

- `ccx peek TARGET --full`: untruncated messages and tool errors.
- `ccx logs TARGET --full`: conversation JSON without tool-result truncation,
  including stopped workers. Claude's `--jsonl` gives the raw transcript.
- `ccx events TARGET --follow`: lifecycle events for diagnostics.
- `ccx import ID`: bring an idle Claude session into ccx, or fork a Codex
  thread whose latest turn completed.

## Engine differences

- `answer`, `respond`, `--cap-opus`, `--next-turn` and `logs --jsonl` are
  Claude-only. `--settle` (also wait for workers a Codex worker started) and
  `reconcile --turn-id` are Codex-only.
- Codex workers share one ccx-managed Codex daemon. `kill` unloads a single
  worker and leaves the others running.
