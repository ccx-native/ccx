# Changelog

All notable changes to ccx are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Changed

- Claude hosts no longer re-read a worker's whole transcript after every turn.
  Context and model readings update from what Claude wrote since the last
  reading, so long-lived workers stay fast and use little memory.
- Steering a busy Claude worker no longer rescans its whole transcript while
  waiting for Claude to accept the message.
- `ccx peek` on a Claude worker reads only recent turns, as Codex peek
  already does: the last two prompts, or more when they hold less than peek
  shows. Turns in that window show no turn number. `--since-turn` still
  reads the whole history.
- `ccx report` and `ccx logs --tail` read long transcripts from the end much
  faster.
- On Claude workers, a steer or task notification that arrives while Claude is
  working now stays in that turn, as it already did on Codex. Claude turn numbers
  in `ccx logs` and `ccx peek --since-turn` change once.

### Fixed

- `ccx stop` on a Claude worker no longer refuses while a steer is waiting
  for the next tool boundary. The turn stops and the steer runs as the next
  turn. It still asks you to retry while a message is being handed to Claude.
- `ccx logs --tail` on a Claude worker no longer numbers turns from the start
  of the tail. As with Codex, tail turns carry no turn number and the output
  is marked source-limited.
- On a busy machine, Codex commands no longer fail with "codexd presence is
  inconclusive" after one slow reply from codexd. CCX keeps asking until the
  command's time budget runs out, and the error says how long it waited.
  It still never starts a second codexd while it cannot tell whether one is
  running.
- `ccx stop` now stops a Codex turn that a notification started. CCX checks
  that it is the worker's live turn, as it already did for Claude, instead
  of refusing because no `ccx send` started it. A damaged record of an older
  message no longer blocks it either.
- Reporting one exact Codex turn (`report(turn_ref=...)` in Python) no
  longer refuses with "not confirmed complete" when the turn was steered
  and no `ccx send` started it. It also no longer refuses for a gap in CCX's
  local record of the worker: Codex's own history of that exact turn is
  enough, unless the record says the turn ended differently.
- `ccx logs` no longer shows an empty first turn on recent Claude workers, and
  no longer includes Claude Code's session records, which held account and
  organization IDs.

### Added

- `ccx logs` items on Claude workers carry their own time (`startedAtMs`, and
  `completedAtMs` for tool calls), injected Claude rows are marked `meta`, and
  Codex notification results keep their `namespace`.

### Removed

- The per-worker `transcript.json` file and the worker record's
  `transcriptJson` field. `ccx logs` still renders the conversation on demand.

## [0.1.0] - 2026-11-03

First public release.

- `ccx` CLI: create, send, steer, wait, report, fork, import, attach, stop and kill
  persistent Claude Code and Codex workers, on Linux, macOS and WSL2.
- Python API (`WorkerClient`, `CcxClient`, `CodexClient`) and durable controllers.
- Local JSON protocol (`ccx rpc`, `ccx rpc --serve`) for other languages.
- Shared worker variables, parent notifications, permission and question
  brokering, usage readings and caps.
- The `ccx` agent skill (`ccx skills install`).
