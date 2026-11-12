# Changelog

All notable changes to ccx are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.2.0] - 2026-11-12

### Added

- `ccx models` lists the models Claude and Codex offer: the id to pass to
  `--model`, the display name, the model a Claude alias such as `opus` points
  to now, and the efforts each model supports. `--json` prints the lists as
  JSON, and `WorkerClient.models()` returns them in Python. Lists are kept for
  24 hours, and Claude's is fetched again after a Claude Code update;
  `--refresh` fetches them now.
- `ccx history` reads past conversations of the workers you name as text, or
  searches them. With no options it shows recent turns as an outline;
  `--turns` reads exact turns or one message; `--grep`, `--cmd` (the commands
  a tool call ran) and `--errors` list matching messages, newest first, each
  with its address (`#TURN.N`, the same numbers as `ccx logs`). `--role`,
  `--since`/`--until`, `-C` and the `chat`, `trace` and `map` views narrow
  and shape the output. Reasoning is never shown. Codex histories are read
  from Codex's local history database when possible, which takes
  milliseconds instead of minutes on long threads.
- Up to 8 terminals can use `ccx attach` on the same Claude worker. They
  share its screen and can all type, and Ctrl-Z detaches only that terminal.
  The screen takes the smallest size among them.
- `ccx logs` items on Claude workers carry their own time (`startedAtMs`, and
  `completedAtMs` for tool calls), injected Claude rows are marked `meta`, and
  Codex notification results keep their `namespace`.

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

### Deprecated

- `ccx logs --format rendered` prints a deprecation note and will be removed.
  Read a conversation as text with `ccx history`; programs keep the default
  JSON. `CcxClient.logs()` in Python is unchanged.

### Removed

- The per-worker `transcript.json` file and the worker record's
  `transcriptJson` field. `ccx logs` still renders the conversation on demand.

### Fixed

- A Claude worker now gets a fresh idle timeout when its last background
  task finishes. Before, if the timeout had run out while the task was
  running, the worker stopped right away, before Claude could act on the
  result.
- A Claude worker no longer lists another worker's background task as its
  own when a command such as `ccx peek other-worker` prints that task.
- Reading a Claude conversation no longer fails when a message queued during
  a turn carries an image.
- A crashed Codex worker's `lastError` now includes Codex's own error
  message, such as a rate limit, when there is one. Before, it said only
  "Codex thread system error".
- `ccx events --follow` no longer waits for the next heartbeat to report a
  worker whose process stopped.
- A message to a Claude worker with an image file name on its own line (for
  example `image.png` where a screenshot was) no longer arrives out of order
  and leaves the worker refusing every later send until `ccx kill`. Such a
  name is now sent as plain text; only a message that is just an image path
  is still attached as the image. When Claude starts a turn whose text
  doesn't match the message, the refusal now says so.

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

## [0.1.0] - 2026-11-03

First public release.

- `ccx` CLI: create, send, steer, wait, report, fork, import, attach, stop and kill
  persistent Claude Code and Codex workers, on Linux, macOS and WSL2.
- Python API (`WorkerClient`, `CcxClient`, `CodexClient`) and durable controllers.
- Local JSON protocol (`ccx rpc`, `ccx rpc --serve`) for other languages.
- Shared worker variables, parent notifications, permission and question
  brokering, usage readings and caps.
- The `ccx` agent skill (`ccx skills install`).
