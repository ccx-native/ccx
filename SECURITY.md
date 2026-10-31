# Security

## Reporting a vulnerability

Report it privately through GitHub: on the repository's **Security** tab, choose
**Report a vulnerability**. Please don't open a public issue for it.

Include what's affected, how to reproduce it (with credentials removed) and any
mitigation you know of. ccx is maintained on a best-effort basis; reports that
affect local code execution, control of workers or the `~/.ccx` store come
first.

## What ccx can do on your machine

ccx runs as your user and adds no privileges of its own. The full list of what it
writes, starts and sends is in
[What ccx changes on your machine](docs/machine-changes.md), with cleanup steps.

- **Workers have your full user privileges by default.** Claude workers run with
  `--dangerously-skip-permissions`, Codex workers with `danger-full-access` and
  approval policy `never`. They can read, write and run anything you can, not
  only inside their folder. See [Permissions](docs/permissions.md) for
  restricted mode.
- **Folder trust.** ccx marks Claude worker folders as trusted in
  `~/.claude.json`; `ccx untrust` undoes its changes.
- **Credentials.** By default ccx reads no credentials: Claude Code and Codex
  handle their own logins. Only the opt-in OAuth usage source (Linux) reads
  Claude Code's token from `~/.claude/.credentials.json` to call Anthropic's
  usage endpoint.
- **Network.** Model traffic comes from `claude` and `codex` themselves. ccx's
  own calls are the opt-in OAuth usage read and, off unless you set
  `CCX_ENABLE_UNOFFICIAL_RESET=1`, the Python-only five-hour reset.
- **Worker shells** get `python` and `python3` functions so workers can use
  shared variables.

## How local state is protected

- ccx uses mode 0700 for its state directories and 0600 for state files and
  sockets, under `~/.ccx` (or `$CCX_ROOT`).
- Worker hosts, the Codex daemon and variable sockets check that each
  connection comes from the same user, using the operating system's peer
  credentials.
- Variable sockets live in a per-user 0700 folder, which ccx refuses to use if
  another user owns it or it's a symlink.

The main attack surface is the boundary between ccx and the `claude` and `codex`
processes (terminal, hooks, app-server), local sockets and the store.

## Known limitations

- Anyone who can run code as your user can control your workers.
- Only the latest release receives security fixes.
