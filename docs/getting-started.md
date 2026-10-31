# Getting started

Install ccx, start a worker and send a follow-up. The examples run real turns
on your plan.

## Install

You need Python 3.11+, Linux, macOS or WSL2, and Claude Code or Codex (or both)
installed and logged in. Check that `claude` or `codex` starts on its own first.

```bash
uv tool install ccx-native     # or: pipx install ccx-native
```

The first `ccx` command that needs the store creates it in `~/.ccx` (it prints
`ccx: created store at ...`). To keep test workers in a separate store, set
`export CCX_ROOT=$(mktemp -d)/ccx`; its parent directory must exist. Workers
still use your provider accounts and can access your files.

## Your first worker

```bash
W=$(ccx new --engine claude --model haiku --cwd ~/myproject -n first \
      "Read the README and summarize it in three bullets")
ccx wait "$W"          # until the turn finishes
ccx report "$W"        # the reply
```

`ccx new` prints the handle (`claude:<uuid>`) and returns while the first turn
runs. Keep the handle: worker commands take it, or a unique prefix of at least
8 lowercase hex characters.

Follow up, and look around:

```bash
ccx send "$W" "Which section is the weakest?"   # waits and prints the reply
ccx peek "$W"          # last messages, recent tool calls, anything pending
ccx list               # all workers with their states
```

Open the same live session in your terminal with `ccx attach "$W"`. Type as
usual; Ctrl-Z detaches and leaves the worker running. ccx commands keep working
while you're attached.

Workers stop after 30 minutes idle. Their history is kept, and the next
`ccx send` brings them back.

## A Codex worker

Same commands, `--engine codex`. Codex model names are the full ids:

```bash
C=$(ccx new --engine codex --model gpt-6-luna --effort low --cwd ~/myproject \
      "List the test files and what each one covers")
ccx wait "$C" && ccx report "$C"
```

[Codex workers](codex.md) covers what's different.

## When a worker needs you

Workers skip permission prompts by default. Use `--permissions restricted` to
receive permission requests; see [Permissions](permissions.md) for each engine's
behavior:

```bash
R=$(ccx new --engine claude --model haiku --permissions restricted --cwd ~/myproject \
      "Run the test suite")
ccx wait "$R"          # exits 4 when the worker is waiting for input
ccx peek "$R"          # shows the pending request
ccx approve "$R"       # or: ccx reject "$R"
```

Questions a Claude worker asks (its multiple-choice question tool) are answered
with `ccx answer`. [Permissions](permissions.md) has the details.

## Let your agent drive it

```bash
ccx skills install
```

This installs the ccx skill for Claude Code and Codex. Then ask your agent in
plain words, for example "use ccx to start two workers that review the API
and the CLI in parallel, then summarize what they found". The skill tells it
how to wait correctly: a Claude Code session uses `ccx wait`; a Codex session uses
`--notify-parent`, which wakes it when a worker finishes.

## Cleaning up

```bash
ccx kill "$W"          # stop one worker; its history is kept
ccx shutdown           # stop every worker and ccx's background processes
```

Worker histories are kept: a later `ccx send` revives a worker.
[What ccx changes on your machine](machine-changes.md) explains how to remove
ccx completely.

## Next

- [Concepts](concepts.md): states, messages, waiting, engine differences.
- [Controllers](controllers.md): Python workflows that run unattended.
- [Usage and caps](usage.md): see your plan limits and stop work at a cap.
- [Troubleshooting](troubleshooting.md): when something looks off.

## Known limitations

- `ccx send` refuses a busy worker. Use `ccx steer` to add to the running turn,
  or wait first.
- `ccx send` prints the handle and a `delivery_id:` line before the reply, and
  `ccx new --wait` a `worker:` line. In scripts, read replies with `ccx report`
  or use `--json`.
- Every turn spends your Claude or Codex quota, including turns nobody watches.
  `ccx usage` shows where you stand.
