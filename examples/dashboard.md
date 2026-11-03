# Dashboard example

`examples/dashboard.py` is a terminal dashboard for local Claude Code and Codex
workers. It uses the `ccx` CLI to show workers, context usage, pending input,
conversations, plan usage and live events.

## Run

It needs only the installed `ccx` command and Python 3.11+. Run it straight from
GitHub with uv, or download `dashboard.py` and run it with `python3`:

```bash
uv run https://raw.githubusercontent.com/ccx-native/ccx/main/examples/dashboard.py
python3 dashboard.py                 # Interactive: active scope
python3 dashboard.py --scope all     # Include stopped/historical workers
python3 dashboard.py --dir ~/project # Only workers in that directory or below it
python3 dashboard.py --once          # One text snapshot, no terminal needed
python3 dashboard.py --once --json   # One JSON object
```

`--snapshot` is an alias for `--once`. The available scopes are `active` (default,
`ccx list --active`) and `all` (`ccx list`). Use `--ccx-bin /path/to/ccx` or `CCX_BIN` to select
a CLI executable; otherwise it searches `PATH` and then the source-tree `ccx.py`. `--json`
requires `--once` (otherwise exit code 2).

## What it shows

The table contains **engine, state, short ID, name, turns, context tokens, last activity, and
working directory**. The default state sort places workers waiting for input first, followed by
live workers and then other states; the header shows live and attention counts. The `CONTEXT`
column comes from a bounded `ccx peek` status batch for up to 40 visible workers; unavailable
counters display as unknown.

The detail pane shows the selected worker's state, turn, context usage, and pending input, above
a scrollable, turn-by-turn conversation: user/assistant text, tool calls/results, and errors
from the last 30 logged turns. Press `e` to switch the pane to a live `ccx events --follow` feed
for a worker in a live state.

Quota gauges are shown when data is available. Claude usage is read via
`ccx usage --engine claude --summary --cached` (about every 12 seconds); Codex rate limits use
`ccx usage --engine codex --json` (at most once every five minutes). `r` refreshes the
inventory, status and conversation and requests a Claude usage refresh, but does not bypass the
Codex minimum interval.

## Keys

| Key | Action |
|---|---|
| `↑` / `↓`, `k` / `j` | Previous / next worker |
| `g` / `G` | First / last worker |
| `Tab` | Toggle `active` / `all` scope |
| `o` | Cycle sort: state, name, turns, context, age |
| `/`, `Esc` | Search name, short ID, working directory, state or engine / clear search |
| `d`, `e` | Toggle detail pane / live event feed |
| `PgUp` / `PgDn`, `Home` / `End`, mouse wheel | Scroll the conversation; jump to beginning / live bottom |
| `c` | Copy the selected worker's full id |
| `r`, `?`, `q` | Refresh / show help (any key closes it) / quit |
| `Enter` | Attach to a non-terminal-only worker's **native UI**, then return on detach/exit |
| `s` | Send a prompt without waiting (idle/stopped, non-terminal-only worker) |
| `A` / `X` | Approve / reject a pending permission request |
| `a` | Answer the first pending **Claude** question (numbered choice or text) |
| `K` | Kill selected worker, after confirmation |

Enter, `s`, `A`, `X`, `a` and `K` control workers as listed above.

## `--once --json` output

A successful snapshot with no workers and no detected provider executables has this exact shape:

```json
{
  "scope": "active",
  "live_count": 0,
  "attention_count": 0,
  "sessions": [],
  "usage": {},
  "usage_errors": {}
}
```

`live_count` counts `busy`, `starting`, and `waiting` workers; `attention_count` counts
`waiting` workers. Each session entry contains the full `worker` handle, `worker_id`, `engine`,
`short`, `name`, `state`, `terminal_only`, `cwd`, `turns`, `input_tokens`, `output_tokens`,
`tokens`, `context` and `last_turn_at`. `tokens` is the input/output sum only when both are
known; `context` comes from the status batch and may be `null`.

`usage` is keyed by provider: `claude` may contain `five_hour`, `seven_day`, `model_quotas` and
overage data; `codex` contains a `windows` list of `label` / `utilization` entries. Unavailable
usage for a detected engine appears in `usage_errors`; an undetected engine is omitted. On a
session-list failure, the JSON adds `error` and returns an empty `sessions` array with exit code
1; `--once` prints an `ERROR:` line and also exits 1 (for example, an unsafe store). An
ordinary successful snapshot exits 0.

## Known limitations

- The interactive dashboard needs a real terminal. Both snapshot modes work without `curses`
  interaction; `--once` captures one moment, not a stream.
- Inventory, status, usage and conversation are sampled separately, so values can differ
  momentarily. Status enrichment covers at most 40 workers per batch (visible rows in the TUI or
  the first listed workers in a snapshot); conversation fetches are bounded to 30 turns and
  happen on focus/record changes, explicit refresh, or retry after an error.
- Quota gauges need a discoverable provider executable and available usage data. Claude's
  summary is cached. Codex quota retrieval may use a short-lived native app-server; headless
  mode does not itself start worker turns.
- The `a` shortcut handles only the first pending Claude question. For a multi-question dialog,
  use `ccx answer` with the appropriate per-question options.
