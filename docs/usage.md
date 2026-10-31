# Usage and caps

Workers spend your Claude and Codex plan quota. `ccx usage` shows where you
stand, and caps stop work at a limit you choose.

## Seeing usage

```bash
ccx usage                      # both accounts: 5-hour and 7-day windows, reset times
ccx usage --engine claude      # also per-model windows, such as Opus
ccx usage --engine codex --tokens   # Codex token activity
ccx usage --json               # everything, as JSON
```

Percentages are of your plan's window, shared by everything that uses the
account, not only ccx. Reset times are in your local time zone.

**Where the numbers come from:**

- **Claude (default):** ccx asks Claude Code itself, through a short `claude -p`
  probe that sends no prompt, with hooks, plugins, MCP servers and tools off.
  Claude Code handles its own login; ccx reads no credentials.
- **Claude, OAuth source (Linux only, opt-in):**
  `ccx usage --engine claude --set-source oauth` makes ccx read Claude Code's
  stored token and call Anthropic's undocumented usage endpoint directly.
  `--set-source get_usage` switches back. The setting applies to every ccx process
  using the same store.
- **Codex:** the Codex app-server, from the running daemon or a short
  connection. It needs no worker and starts no turn.

Claude usage is cached across ccx processes and read at most once a minute.
Workers with caps check every two minutes; OAuth reads may be less frequent.
If Anthropic holds usage data back, ccx marks the last values as held and shows
when it will retry.

## Caps

```bash
ccx new --engine claude --cwd ~/proj --cap-7d 80 "Refactor the parser"
ccx send "$W" --cap-5h 60 "Keep going"
```

A cap is an absolute percentage of the window (check `ccx usage` first, and set
it above the current reading), and applies to that one message:

- **Already over the cap:** the message is refused before it's delivered. The
  worker is left as it was. Exit code 7.
- **Reached during the turn:** the turn is interrupted, and a waiting `send` or
  `new --wait` exits 7 with the reason. A Claude worker becomes `stopped`
  (`native_state` `budget-stopped`); a Codex worker stays loaded and idle. The
  next `send` continues either one.

A later message without a cap runs uncapped. A cap on a `steer` applies to the
running turn the message joins. Both engines support caps; `--cap-opus` limits
Claude's 7-day Opus window.

## Known limitations

- **Caps can overshoot.** Usage is sampled, so a turn can pass its cap between
  two samples before it's stopped.
- **Caps fail open.** If usage can't be read (unavailable, held, stale or
  unparseable), the cap isn't checked and ccx warns. It never stops a worker on
  old numbers.
- **The Claude source is unofficial either way.** Claude Code's usage reply and
  Anthropic's usage endpoint are both undocumented and can change.
- **No OAuth source on macOS:** Claude Code keeps its token in the Keychain
  there.
- **The five-hour reset** (`CcxClient.reset_five_hour_limit()`, Python only)
  claims the reset Claude Code offers to eligible accounts (a limited rollout).
  It only works when your account is offered one, and it is off unless you use
  the OAuth source and set `CCX_ENABLE_UNOFFICIAL_RESET=1`.
