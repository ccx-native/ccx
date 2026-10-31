# History, events and usage

Read replies, page through conversations, observe changes and collect usage.
All cursors, transcript locators and turn references are opaque strings.

## Conversation pages

Use `conversation.page` for a history view that opens at the recent end:

```json
{"protocol_version":2,"request_id":"history-1","op":"conversation.page","params":{"target":"claude:12345678-1234-4abc-8def-123456789abc","source_record_target":40,"max_tool_result_bytes":16384}}
```

Read `records` and pass `older_cursor` as the next request's `cursor` to move
backward. Stop when it is null. The first page covers the newest records;
records within a page are in chronological order. `source_record_target`
is approximate, accepts 1–100 and defaults to 40.

For a forward read from the start, obtain `transcript_locator` from the
`worker.inspect` result, then call `transcript.read` with it as `locator`.
Pass `next_cursor` back as `cursor` while `has_more` is true. `limit` defaults
to 20. If `source_changed` is true, restart if you need a consistent copy.

Both reads accept `max_tool_result_bytes` to bound tool output; defaults are
16384 for conversation pages and 65536 for transcripts. Inspect `completeness`
and record truncations instead of assuming each tool result is complete.

## Events

`worker.events` returns `events`, `next_cursor`, `has_more`, `gap`, `timed_out`
and `runtime`. To observe changes from now, request `tail: 0`, save the cursor,
then long poll:

```json
{"protocol_version":2,"request_id":"events-1","op":"worker.events","timeout_ms":32000,"params":{"target":"claude:12345678-1234-4abc-8def-123456789abc","after_cursor":"<saved next_cursor>","wait_ms":30000,"limit":200}}
```

Always advance to `next_cursor`, including on empty pages. Drain pages while
`has_more` is true. `wait_ms` holds an empty read until an event or the wait
limit; keep `timeout_ms` larger to leave time for the final response.

`limit` accepts 1–1000 (default 200); `tail` accepts 0–1000. Do not combine
`tail` with `after_cursor`. Without either, the read starts at retained history.
Optional `kinds` filters events:

```json
["state","turn.started","turn.ended","tool.started","tool.finished","input.requested","input.resolved","compact.started","compact.ended","error","delivery.changed"]
```

A nonnull `gap` means some history could not be continued from the cursor.
Refresh live worker state and reconcile affected deliveries; retained events
may be replayed. Make event processing tolerant of duplicates.
`runtime` is `live`, `gone` or `unknown`; an empty page or an unknown runtime
does not prove the worker has stopped. Events can trigger a fresh `worker.wait`
or `turn.reconcile`; they do not replace those results.

## Usage

`worker.telemetry` accepts `after_cursor` and `limit` (default 200). Read
`turns`, `compactions`, `totals`, `coverage`, `next_cursor` and `has_more`.

- Codex reports native turns with input, output, cached and reasoning tokens
  and context readings. Later pages can update an earlier turn; merge by id.
- Claude reports per-prompt entries, token/model totals, tools and errors;
  `turn_ref` is null.

`account.usage.read` takes `engine: "claude"` or `"codex"` and returns
`provider`, `observed_at_ms` and plan usage `windows`. This is account usage,
separate from a worker's token telemetry.

## Parent notifications

When called from a supported Codex parent context, `turn.send` with
`notify_parent: true` schedules a new parent turn when the child turn settles.
It requires `mode: "new_turn_only"`. Optional `caller_metadata` is a JSON
object limited to 4096 serialized bytes and requires `notify_parent: true`.
Use it to correlate the notification with your task.

Retain `event_id` from `result.notification` and read `notification.inspect`
with that id to inspect delivery state (`result.notification`).
`input.answer` also accepts `notify_parent` for the continued turn.

## Known limitations

- RPC does not stream tokens or push events. Long poll `worker.events` or
  wait for the worker instead.
- History may be bounded, truncated or incomplete. Use paging and inspect
  completeness fields; larger requests still face the 4 MiB response limit.
- Claude telemetry cannot be joined to managed turns using `turn_ref`.
- Parent notifications require a Codex parent context. A standalone RPC
  client should observe completion through wait or events.
