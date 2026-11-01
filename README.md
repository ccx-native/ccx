# ccx

Run Claude Code and Codex as persistent local workers. Start them in any folder,
send follow-ups and read their replies. Coordinate several workers from the
terminal, a script or another agent.

Workers can be driven by your main agent, by a script, or by you: `ccx attach`
opens any worker in the engine's own interface, and `ccx import` brings in a
session you started yourself. [`examples/dashboard.py`](examples/dashboard.md)
is a small terminal dashboard for watching and joining workers.

- **What it is:** a CLI and Python library that drives the real `claude` and
  `codex` tools, logged in with your own accounts. A worker keeps its folder,
  history and settings across turns and restarts.
- **Who it's for:** developers who run several agents locally, or want an agent
  to delegate to other agents.
- **What it isn't:** a hosted service or an API client. For large-scale or
  multi-tenant automation, use the Claude Agent SDK or the OpenAI APIs.

> [!IMPORTANT]
> **Before you use it**
>
> - **Unofficial.** ccx is an independent project, not affiliated with or
>   endorsed by Anthropic or OpenAI. Using Claude Code and Codex through it is
>   still subject to their terms.
> - **It can break on updates.** ccx drives Claude Code's interactive interface
>   and hooks, and Codex's app-server protocol. Neither is a stable public API,
>   so a new release of either can break ccx until it adapts.
> - **It spends your quota.** Workers run unattended on your Claude and Codex
>   plans. Usage caps (`--cap-5h`, `--cap-7d`) help, but they're not exact.
> - **Workers have your full user privileges by default**: they can read, write
>   and run anything you can, not only inside their folder. Use
>   `--permissions restricted` for permission prompts and Codex sandboxing.
> - **It changes a few things on your machine:** it marks worker folders as
>   trusted in `~/.claude.json` (`ccx untrust` undoes it), leaves background
>   processes running (`ccx shutdown` stops them), and adds `python`/`python3`
>   shell functions inside worker shells for [variables](docs/variables/README.md).
>   [What ccx changes on your machine](docs/machine-changes.md) has the full list.

## Install

You need Python 3.11 or newer on Linux, macOS or WSL2, and at least one of
[Claude Code](https://docs.claude.com/en/docs/claude-code) or
[Codex](https://github.com/openai/codex), installed and logged in. Tested with
Claude Code 2.1.316 and Codex 0.166, on WSL2, macOS 12 (Intel) and
macOS 27 (Apple silicon).

```bash
uv tool install ccx-native     # or: pipx install ccx-native / npm install -g ccx-native
ccx skills install             # optional: teach Claude Code and Codex to use ccx
```

The package is `ccx-native`; the command is `ccx`. The first command that
needs the store creates it in `~/.ccx`. `ccx skills install` copies
the agent skill into `~/.claude/skills` and `~/.agents/skills` and keeps it
current when you upgrade. To use [skills.sh](https://skills.sh) instead:
`npx skills add oneferdev/ccx -g`.

## Quick start

```bash
W=$(ccx new --engine claude --cwd ~/myproject "List the .py files here")
ccx wait "$W"                           # until the first turn finishes
ccx report "$W"                         # the reply
ccx send "$W" "Now count their lines"   # a follow-up; waits and prints the reply
ccx attach "$W"                         # open it in your terminal; Ctrl-Z to detach
```

`ccx new` prints the worker's handle, `claude:<uuid>`. Use `--engine codex` for a
Codex worker; every command works the same. Add `--model haiku` (Claude) or
`--model gpt-6-luna --effort low` (Codex) to keep experiments cheap.

From Python (install `ccx-native` in your project's environment):

```python
from ccx import WorkerClient

workers = WorkerClient()
created = workers.create(engine="claude", cwd="/path/to/project",
                         prompt="List the .py files here", wait=True)
worker = created["worker"]
print(created["reply"])
print(workers.send(worker, "Now count their lines")["reply"])
```

## What else it does

- **Steer and answer:** add to a running turn (`steer`), answer questions and
  permission requests (`answer`, `approve`, `reject`), cancel a turn (`stop`).
- **Orchestration:** a Codex parent gets woken when a worker finishes
  (`--notify-parent`); Claude parents and scripts use `ccx wait`.
  [Waiting for results](docs/concepts.md#waiting-for-results)
- **Controllers:** Python workflows that run unattended, branch on results, and
  resume after an interruption without redoing finished steps.
  [Controllers](docs/controllers.md)
- **Variables:** structured JSON state that workers read and update with plain
  `python3`, shared across engines. [Variables](docs/variables/README.md)
- **Context management:** `ccx compact`, `ccx fork`, and `peek --status` to see
  how full a worker's context is.
- **Usage:** `ccx usage` shows your Claude and Codex plan limits, and caps
  stop a turn at a limit you set. [Usage and caps](docs/usage.md)
- **Local JSON protocol:** `ccx rpc` for callers in other languages.
  [RPC](docs/rpc/README.md)

## Docs

Start with [Getting started](docs/getting-started.md) and
[Concepts](docs/concepts.md). The rest:
[Permissions](docs/permissions.md) ·
[Usage and caps](docs/usage.md) ·
[Codex workers](docs/codex.md) ·
[Controllers](docs/controllers.md) ·
[Troubleshooting](docs/troubleshooting.md) ·
[What ccx changes on your machine](docs/machine-changes.md) ·
[Python API](docs/python/README.md) ·
[RPC](docs/rpc/README.md) ·
[Architecture](docs/architecture.md).
`ccx help COMMAND` has the details for each command.

## Contributing, security, license

See [CONTRIBUTING.md](CONTRIBUTING.md) and [SECURITY.md](SECURITY.md).
Released under the [MIT License](LICENSE).

## Known limitations

- Runs locally under one user on Linux, macOS or WSL2.
- Claude Code or Codex updates can break ccx. See
  [Troubleshooting](docs/troubleshooting.md#after-upgrading-claude-code-or-codex).
- Usage caps are sampled and can overshoot. See [Usage and caps](docs/usage.md).
