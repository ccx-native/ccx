# What ccx changes on your machine

ccx runs as your user and adds no privileges of its own. This page lists
everything it writes, starts or sends, and how to undo each one. The security
view of the same facts is in [SECURITY.md](../SECURITY.md).

## Files

| Where | What | Undo |
|---|---|---|
| `~/.ccx/` (or `$CCX_ROOT`) | Everything ccx keeps: worker records and settings, delivery ids, event logs, variables, controller runs (including their Git worktrees), sockets and daemon logs. The store is owner-only (mode 0700). | `rm -rf ~/.ccx` after the steps in [Uninstall](#uninstall). |
| `~/.claude.json` (or `$CLAUDE_CONFIG_DIR/.claude.json`) | Each Claude worker's folder, and every `--add-dir`, is marked as trusted (`hasTrustDialogAccepted`), so Claude Code doesn't stop to ask. ccx records each change it makes in `~/.ccx/trust-ledger.json`. | `ccx untrust` |
| `~/.claude/skills/ccx`, `~/.agents/skills/ccx` | The agent skill, only if you ran `ccx skills install`. Copies carry a `.ccx-skill.json` marker; ordinary `ccx` commands refresh them after an upgrade. | `ccx skills uninstall` |
| `$XDG_RUNTIME_DIR/ccx-live-vars-<uid>/` (or under `/tmp`) | Short-lived sockets for variables passed from a running controller. | Removed when unused; safe to delete. |

ccx doesn't change your `~/.claude/settings.json` or Codex's configuration.
Claude Code and Codex keep their own sessions in `~/.claude` and `~/.codex` as
usual; ccx reads Claude Code's transcripts there.

## Inside a worker

- **Permissions.** Workers have your full user privileges by default.
  [Permissions](permissions.md) explains restricted mode.
- **Claude settings.** Each Claude worker gets its own `--settings` file in the
  store. It registers ccx's hook commands (`ccx hooks-relay`) and status line
  (`ccx statusline-relay`; `CCX_STATUSLINE_BRIDGE=0` turns it off). Workers with
  full permissions also skip Claude's one-time "Bypass Permissions mode" warning.
  The file also selects Claude's classic renderer, even if your settings choose
  fullscreen.
- **Your hooks.** Your own and plugins' Claude hooks still run in every worker:
  Notification hooks (sounds, desktop alerts) fire for unattended workers too,
  and PreToolUse or PermissionRequest hooks that decide still apply. See the Stop-hook
  limitation in [Troubleshooting](troubleshooting.md#known-limitations).
- **Shell.** Worker shells get `python` and `python3` shell functions, through
  `CLAUDE_ENV_FILE` for Claude, and `BASH_ENV` and `ZDOTDIR` for Codex. They
  make [variables](variables/README.md) available, and run
  your normal Python when a worker has none. If `python` behaves differently
  inside a worker than in your own shell, this is why.
- **Prompts.** When a worker has variables, ccx adds a one-line note naming
  them to the prompts it sends.

## Background processes

| Process | Its command line contains | Stops |
|---|---|---|
| One host per running Claude worker, plus its `claude` child | `ccx host` | On `ccx kill`, or after 30 minutes idle (not while a background task in the worker is still running). |
| One Codex daemon per store, plus one `codex app-server` child | `ccx.codex.daemon` | 15 minutes after its last worker is unloaded (`CCX_CODEXD_LINGER`, in seconds). |
| One notification daemon per store | `ccx.notify` | After no `--notify-parent` / `ccx notify` notifications remain pending. |
| Controllers you started | `ccx controller` | When the script ends, or `ccx controller stop`. |

`ccx shutdown` stops the first three; `--dry-run` lists what it would stop. It
leaves controllers and Claude sessions outside ccx alone.

## Network and credentials

All model traffic comes from `claude` and `codex` themselves, billed to your
plan. ccx makes no network calls of its own, except where noted:

- **Claude usage (default).** `ccx usage` and caps ask Claude Code for usage
  through a promptless `claude -p` probe. ccx reads no credentials.
- **Claude usage, OAuth source (opt-in, Linux only).** After
  `ccx usage --engine claude --set-source oauth`, ccx reads
  `~/.claude/.credentials.json` and calls Anthropic's undocumented usage
  endpoint directly. It may run `claude auth login --claudeai` to refresh an
  expired token. `--set-source get_usage` switches back.
- **Codex usage** comes from the Codex app-server, which handles its own login.
- **Five-hour reset (Python only, off by default).**
  `CcxClient.reset_five_hour_limit()` claims the five-hour limit reset that
  Claude Code offers to eligible accounts (a limited rollout), through the same
  account endpoint Claude Code uses. It needs both the OAuth source and
  `CCX_ENABLE_UNOFFICIAL_RESET=1`; otherwise it refuses before reading anything.

## Uninstall

```bash
ccx shutdown            # stop workers and background processes
ccx untrust             # undo the folder trust in ~/.claude.json (--dry-run to preview)
ccx skills uninstall    # if you installed the skill
rm -rf ~/.ccx           # all ccx state, including worker histories it keeps
uv tool uninstall ccx-native    # or: pipx uninstall ccx-native
```

Stop running controllers first (`ccx controller stop NAME`). Run `ccx untrust`
before deleting `~/.ccx`, since its record of what to undo lives there, and
with Claude Code closed if you can: a running `claude` may write the file back.
It removes only the trust ccx added and restores any value ccx overrode.

Controller worktrees are registered in their Git repositories. After deleting
`~/.ccx`, run `git worktree prune` in those repositories, and delete the
`ccx/...` branches you don't need.

Claude Code and Codex conversations stay in `~/.claude` and `~/.codex`.

## Known limitations

- **Worktrees are never cleaned up by ccx.** Controller tasks that use
  `workspace="worktree"` keep their checkouts until you remove them.
- **Daemon logs** (`codexd.log`, `notifyd.log`) are rotated only when the
  daemon starts.
