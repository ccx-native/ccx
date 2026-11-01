# WorkerClient

`WorkerClient` drives both engines and returns dicts. This page lists the
main arguments and result fields; see [Python API](README.md) for handles,
timeouts and errors.

```python
from ccx import WorkerClient
workers = WorkerClient()
```

## Creating workers

### `create(*, engine, cwd, ...)`

| Argument | Default | |
|---|---|---|
| `engine` | required | `"claude"` or `"codex"` |
| `cwd` | required | The folder it works in. |
| `prompt` | `None` | First message. Without one the worker starts idle. |
| `wait` | `False` | Wait for the first turn and include its reply. |
| `timeout` | `None` | Seconds to wait for that turn; `None` is unbounded. |
| `model`, `effort` | engine default | Claude: `sonnet`, `opus`, `haiku` or a full id; Codex: full ids such as `gpt-6.1-sol`. |
| `permissions` | `"full"` | `"restricted"` enables approval requests; see [Permissions](../permissions.md). |
| `name` | `ccx-<short>` / `codex-<short>` | Display name. |
| `worker_id` | random | A UUID you chose, for example from `new_worker_id()`. Retrying with the same one never creates a second worker. |
| `idle_timeout` | 1800 | Seconds idle before the worker is unloaded; `0` keeps it loaded. |
| `start_timeout` | engine default | Seconds to wait for the engine to start. |
| `caps` | `None` | Usage caps for the first turn, such as `{"7d": 80}`; see [Usage and caps](../usage.md). |
| `variables`, `variables_from` | `None` | Shared Python variables (see [Variables](#variables)). |

Returns `worker` (the handle), `state`, `native_state`, `created`, `delivery_id`,
`delivery_status`, `reply` and `report`. With `wait=True` and a prompt, `reply`
is the first turn's final text and `report` is what `report()` would return for
that turn; otherwise both are `None`.

### `fork(worker, *, prompt=None, ...)`

Creates a new worker of the same engine that continues from a copy of the
source's history. It takes `prompt`, `wait`, `timeout`, `name`, `model`,
`effort`, `worker_id`, `caps` and the variable arguments, as `create` does;
engine, folder and permissions come from the source. The result is `create`'s
plus `forked_from`. The source isn't changed. A Codex source must have a completed turn.

## Sending messages

### `send(worker, text, *, wait=True, timeout=None, caps=None, delivery_id=None, variables=None, variables_from=None)`

Starts a new turn on an idle worker, reviving it first if stopped or failed.
A busy worker raises `DeliveryRejected`; one waiting for input raises
`SessionWaiting`. Neither receives the message.

Returns `reply` (the final text, with `wait=True`), `delivery_id`,
`delivery_status`, `turn_ref`, `settled` and `state`. With `wait=False` it
returns once the message is accepted; read the reply later with `wait` and
`report`.

Pass a `delivery_id` you saved before calling to retry the same message safely.

### `steer(worker, text, *, wait=True, timeout=120, caps=None, delivery_id=None)`

Adds a message to the turn that's running now. It refuses an idle worker. A
Claude steer can come back `accepted-pending` (held by ccx until it lands);
that isn't a failure, so don't resend it. On Codex, success confirms placement
without waiting for the turn's reply, regardless of `wait`.

## Waiting and reading

### `wait(worker, *, timeout=None)`

Waits until the worker is no longer busy and returns a verdict: `status` (the
state, `timeout`, or `budget-stopped`), `state`, `timed_out`, `pending_input`
(what it's waiting for, if anything) and `last_settled_turn` (`turn_ref`,
`outcome`, `completed_at`), plus `last_turn`, `api_error` and
`pending_approvals` when they apply. Running out of time is a result here, not
an exception.

Claude also accepts `next_turn=True`, which waits for the worker's *next* turn
to finish, such as the one a background task starts when it ends. `after_turn`
(a `turn_ref`) and `known_input` (a pending request you've already seen) narrow
it. Results also list the worker's background work as `background_tasks`.

### `report(worker, *, max_chars=None, turn_ref=None, timeout=30)`

The final reply of the latest finished turn: `text`, `has_final_message`,
`truncated`, `turn_ref`, `delivery_id` and `status`. `turn_ref` (from `wait` or
`send`) reads exactly that turn instead. A turn that ended with tool calls only
returns `text=""` and `has_final_message=False`. While a newer turn is running,
waiting for input or has failed, it returns the last confirmed reply with a
`note` saying so. With no finished turn yet it raises `SessionUnavailable`.

### `status(worker)` and `list(*, engine=None, cwd=None, state=None, parent=None, limit=None)`

What ccx has recorded, without asking the engine. `status` returns `name`,
`cwd`, `state`, `pending_input`, `managed_turns`, `requested_model`,
`observed_model`, `context` and `background_task_summary` (see
[Background work](../concepts.md#background-work)). `list` returns `{"workers": [...]}`.
`parent` lists the workers another worker created.

`recap(handles)` summarizes several workers at once, including a printable
`text`.

### `context(worker)`

How full the context window was after the last turn: `tokens_used`,
`window_size`, `percent_used`, `as_of_turn_ref`.

### `conversation(worker)`, `conversation_page(worker, *, cursor=None)`, `export_conversation(worker, destination)`

The conversation in one format for both engines: messages, tool calls and their
results.

- `conversation` returns the whole history as one document.
- `conversation_page` returns the newest part (`records`) and an `older_cursor`
  for the page before it. Use it for long histories and live views.
- `export_conversation` writes `conversation.json` to a folder, with the engine's
  own transcript next to it unless `include_native=False`.

`max_tool_result_bytes` cuts long tool outputs. `completeness` says if anything
was cut or couldn't be read.

### `events(worker, ...)` and `follow(worker, ...)`

Lifecycle events: state changes, turns starting and ending, tool calls, input
requests, compactions, errors. They work for stopped workers too.

```python
page = workers.events(w, tail=0)               # no history, just a cursor
cursor = page["next_cursor"]
while True:
    page = workers.events(w, after=cursor, wait=60)   # hold up to 60 s for news
    for event in page["events"]:
        print(event["kind"], event.get("turn_ref"))
    cursor = page["next_cursor"]
```

`follow(w, after=cursor, timeout=3600, heartbeat=30)` is the same as an
iterator, with a `heartbeat` item after each quiet period. Filter with
`kinds=[...]`; `limit` caps a page (default 200).

Every event has `kind`, `type` (the engine's own name), `ts`, `cursor`, and
`turn_ref` when it belongs to a turn:

| `kind` | Extra fields |
|---|---|
| `state` | `from`, `to` |
| `turn.started` | |
| `turn.ended` | `outcome`: `completed`, `interrupted`, `failed` (Codex) or `unknown` |
| `tool.started` | `tool`, `tool_use_id`, `input_preview` |
| `tool.finished` | `tool`, `tool_use_id`, `ok`, `duration_ms`, `result_preview` |
| `input.requested` / `input.resolved` | `request`, `request_id`, Codex `decision` |
| `compact.started` / `compact.ended` | `outcome` on the end event |
| `delivery.changed` | Codex: `delivery_id`, `status`; a cue to check that delivery again |
| `background.changed` | `transition`, `item`, `summary` |
| `context.changed` | `context` |
| `error` | the error's details |

See [Following changes](../concepts.md#following-changes) for background and
context event fields and how to start a live view.

Engine-specific events also come through, with `kind=None`. Cursors are opaque
strings; keep the last one and pass it as `after`. If a cursor can't be placed
any more, the page replays everything kept and `gap` says why, so handle events
idempotently.

## Answering input

### `approve(worker, *, request_id=None, wait=False, timeout=None)` and `reject(worker, *, request_id=None, reason=None, ...)`

Decide one pending permission request on a restricted worker. `reason` (Claude
only) is shown to the model. `wait=True` waits for the turn that continues. With
several requests pending, pass `request_id` from `status(w)["pending_input"]`.

### `answer(worker, text=None, *, options=None, text_answers=None, request_id=None, ...)` and `respond_elicitation(worker, action, content=None, *, request_id=None, ...)`

Claude only. `answer` replies to a pending question: `text` for a single free-text
answer, or `options`/`text_answers` as `(question_index, value)` pairs.
`respond_elicitation` resolves an MCP form with `action` `accept`, `decline` or
`cancel` (and `content` for `accept`). Both take `wait`, `timeout` and a
`response_id` you saved to retry safely. For typed results, see
[Engine clients](engines.md#ccxclient-claude).

## Controlling workers

| Method | Does |
|---|---|
| `stop(worker)` | Cancels the running turn. The worker stays loaded. Refuses when nothing is running. |
| `kill(worker, *, timeout=90)` | Ends the worker's process and confirms it's gone. The next `send` revives it. |
| `resume(worker, *, timeout=90)` | Loads a stopped worker without starting a turn. |
| `compact(worker, *, wait=True, timeout=None, operation_id=None)` | Runs the engine's own compaction. Save an `operation_id` to look it up later with `compact_status(worker, operation_id)`; that never starts another one. |
| `set_name(worker, name)` | Renames it; `None` resets the default name. |
| `set_model(worker, model)`, `set_effort(worker, effort)` | Applies from the next turn; refused while busy. A loaded idle Claude worker is stopped so that `send` revives it with the change (`restarted=True`). |

## Account

- `usage(engine=None, *, include_tokens=False)`: plan usage for one or both
  accounts, as `ccx usage` shows it. An engine that isn't installed or signed in
  is skipped.
- `capabilities(engine)`: the features that differ between engines, as booleans:
  `auto_delivery`, `wait_next_turn`, `wait_after_turn`, `wait_known_input`,
  `answer_question`, `respond_elicitation` (all Claude-only), plus
  `steer_current_turn`.
- `telemetry(worker, *, after_cursor=None, limit=200, timeout=120)`: per-turn token usage,
  one page at a time (`turns`, `totals`, `next_cursor`, `has_more`), as RPC
  [`worker.telemetry`](../rpc/reading.md#usage) returns it.

## Variables

`create`, `fork` and `send` accept `variables={...}` or
`variables_from=other`, never both. Plain scripts attach saved JSON; controller
tasks attach live Python objects. Both need full permissions.
`variables(worker)["names"]` lists names;
`variables(worker, "name")["value"]` reads saved JSON without waiting.
Read live objects from the controller's retained mapping.

See [Python and controllers](../variables/python.md) for examples and
[sharing and saved state](../variables/sharing.md) for persistence and conflicts.

## Known limitations

- **Questions and MCP forms are Claude-only**; Codex declines its own questions.
- **`next_turn`, `after_turn` and `known_input` are Claude-only**; Codex raises
  `CapabilityUnsupported`.
- **`status` and `list` are records, not live checks.** A worker whose process
  died shows its last recorded state until something touches it.
