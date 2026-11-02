# Troubleshooting

Start with the exit code and `ccx peek`.

## Installing

- **`ccx: command not found` after installing:** the install folder, usually
  `~/.local/bin`, isn't on your `PATH`. Run `uv tool update-shell` or
  `pipx ensurepath`, then open a new terminal.
- **npm says `Python 3.11+ is required`:** the npm package runs on your own
  Python, and it found only an older one (macOS ships Python 3.9 as `python3`).
  Install a newer Python, or point to one with `PYTHON=/path/to/python3.12`.
  `uv tool install ccx-native` avoids this, since uv downloads a Python if needed.
- **The wrong `claude` or `codex` runs:** ccx uses `~/.local/bin/claude` if it
  exists, otherwise the `claude` on your `PATH`. For Codex it's the reverse:
  `PATH` first, then `~/.local/bin/codex`. Set `CCX_CLAUDE_BIN` or
  `CCX_CODEX_BIN` to choose a specific one.
- **`ccx new` says Claude Code is not signed in:** run `claude` once and sign
  in. If you are signed in, see
  [Claude workers say "Not logged in"](#claude-workers-say-not-logged-in).
  Codex workers that aren't signed in fail their first turn with
  `401 Unauthorized`; run `codex login`.

## What the exit code means

Every command uses the same codes, on both engines. Python errors carry the same
number as `exit_code`.

| Code | Meaning | Typical cause |
|---|---|---|
| 0 | success | |
| 1 | bad input | unknown flag, missing prompt, invalid value |
| 2 | not found | no such worker or requested turn |
| 3 | unavailable | unsafe or unreadable store, worker not running, no completed reply yet, inside the Codex sandbox |
| 4 | waiting for input | a question, permission request or MCP form is pending |
| 5 | refused in this state | `send` to a busy worker, `stop` with no turn running; nothing changed |
| 6 | incomplete | a `--timeout` ran out, the turn failed or was interrupted, or delivery is uncertain |
| 7 | usage cap | a `--cap-*` limit refused or stopped the turn |
| 8 | rate limited | Anthropic returned 429; wait for the reset |
| 9 | transient API error | 529, 5xx or a dropped stream; retry in a few minutes |

A timeout is never success: the worker may still be running. Check with `peek`
or wait again rather than resending.

## A message seems lost

Every message has a delivery id (`--json` shows it, or pass `--delivery-id`).
Before sending again, look it up:

```bash
ccx reconcile "$W" --delivery-id ID
```

A retry must keep the same text and delivery id; it never delivers twice.
A Claude `steer` can report `accepted-pending`: the message is still pending,
so wait or reconcile it.

If the output of `ccx new` was lost, `ccx list` shows the worker. For creation
you can retry safely, pick a UUID yourself and pass it with `--worker-id`.

## A worker seems stuck

```bash
ccx peek "$W"            # last messages, tool calls, anything pending
ccx peek "$W" --status   # just the state
```

- **`waiting`:** it needs input. `peek` shows what kind; see
  [Permissions](permissions.md).
- **`busy` for a long time:** look at its recent tool calls in `peek`. Add to the
  turn with `steer`, or cancel it with `ccx stop`.
- **Idle, but waiting on background work:** a Claude worker can end its turn
  while a background task keeps running, and wake up for another turn when it
  finishes. `peek` lists background tasks. `ccx wait "$W" --next-turn` (or
  `ccx notify "$W" --next-turn` from a Codex parent) waits for that turn. A server
  or watcher left running doesn't mean the work is unfinished.

## Claude workers say "Not logged in"

- **Started from a Claude worker, with a token login.** With
  `claude setup-token`, your login is only the `CLAUDE_CODE_OAUTH_TOKEN`
  variable, and Claude Code removes it from the shell its tools run in. A Claude
  worker started by another Claude worker then comes up not logged in. Log in
  with `/login` instead, or set `CLAUDE_ENV_FILE`, in the environment you run
  ccx from, to a file containing
  `export CLAUDE_CODE_OAUTH_TOKEN="$(cat ~/.config/claude-token)"`. Every command
  the model runs can then read your token, so only do this on a machine you
  control. Codex parents aren't affected, unless your Codex shell policy
  excludes `*TOKEN*` variables; ccx doesn't override that policy.
- **On macOS over SSH.** The login Keychain is locked for SSH sessions, even
  while you're logged in at the desktop. Run `security unlock-keychain` in the
  SSH session, and start or revive Claude workers only from that session, or use
  `claude setup-token`.

## A worker stopped or failed

`ccx send` revives a stopped or failed worker with its history. A turn that was
interrupted isn't resumed or resent: check `ccx report`, then send what's left.
`ccx peek` shows why a worker failed.

## After upgrading ccx

- **Running workers keep the old code** until their process restarts. Run
  `ccx shutdown`; the next `send` revives each worker on the new version.
- **Installed skills** refresh by themselves on the next `ccx` command.

## After upgrading Claude Code or Codex

ccx drives parts of both tools that aren't stable APIs, so an upgrade can break
it. Typical signs:

- **Claude:** sends fail or land late, slash commands like `/compact` don't run,
  or prompts show up wrapped in `<pasted_content>` tags. These usually mean Claude
  Code changed how its interface handles pasted text or timing.
- **Codex:** commands fail with protocol errors. Compare the installed version's
  protocol with the one ccx expects:

  ```bash
  codex app-server generate-json-schema --out /tmp/codex-schema
  python scripts/check_codex_protocol.py --generated /tmp/codex-schema   # from a ccx checkout
  ```

Check the [issues](https://github.com/ccx-native/ccx/issues), or open one with the
two versions (`claude --version`, `codex --version`, `ccx --version`).

## Looking deeper

- `ccx peek "$W" --full`: messages and tool errors without truncation.
- `ccx logs "$W"`: the whole conversation as JSON. For Claude, `--jsonl` gives
  the raw transcript.
- `ccx events "$W" --follow`: lifecycle events as they happen.
- Logs: each Claude worker's `host.log` is in its folder in the store; the Codex
  daemon writes `runtime-metadata/codexd.log` there, and the notification daemon
  writes `~/.ccx/notify/notifyd.log`.

## Known limitations

- Status can lag behind a crashed process; use `wait` to check whether a turn
  has finished.
- A timeout doesn't cancel a worker's turn. Use `ccx stop` if you want to cancel it.
- Claude hooks that block Stop or UserPromptSubmit aren't supported. ccx treats
  the first Stop as the turn's end: waits and notifications can return while a
  hook keeps Claude working, without the turn's usage cap. A blocking
  UserPromptSubmit hook can leave the worker busy; use `ccx stop`.
- On macOS, select a store with the same spelling it was created with (case and
  accents).
