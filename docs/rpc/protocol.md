# Requests, errors and retries

This page covers the JSON envelope and how to recover when a request fails or
its response is lost. For subprocess framing, see [persistent mode](persistent.md).

## Envelope

```json
{"protocol_version":2,"request_id":"read-1","op":"system.info","timeout_ms":5000,"params":{}}
```

`request_id` is a caller-chosen, nonempty string of at most 256 characters,
without NUL. It correlates a response with a request; it does not prevent a
mutation from running twice. Use a fresh request id for each attempt.
`params` is an object; omitting it is equivalent to `{}`. Unknown operation
parameters return `invalid_request`.

`timeout_ms` is a finite positive number, defaulting to 90000. Set it per
operation: a wait may need minutes, while an inspection should finish quickly.
Give your subprocess timeout more time than the RPC budget. In serve mode,
allow for the server's additional five-second deadline grace plus transport
latency. A client timeout does not prove a mutation failed.

Success and failure use the same envelope:

```json
{"protocol_version":2,"request_id":"read-1","ok":true,"result":{}}
{"protocol_version":2,"request_id":"read-1","ok":false,"error":{"code":"refused","message":"...","details":{"reason":"busy"}}}
```

Validate `protocol_version`, `request_id` and `ok` before consuming the result.
Malformed requests may receive `request_id: null`. A one-shot process exits 0
when it writes a protocol response, including `ok: false`; check the JSON.
A nonzero process exit or an unusable response is a transport failure.

Requests and responses are limited to 4 MiB each (encoded bytes). A response
that exceeds the limit becomes an `invalid_request` error with
`details.reason: "response_too_large"`. Use bounded reads and paging.

## Retry identities

Save the mutation's id and parameters before writing to stdin. The outer
`request_id` can change on retry; the mutation's id must stay the same.

| Mutation | Save before the first attempt | Recover a lost response |
|---|---|---|
| `worker.create`, `worker.fork` | Optional `worker_id`, a lowercase UUIDv4 | Repeat with the same id and parameters. Supply an id if you need reliable recovery. |
| `turn.send` | Required `delivery_id` and send parameters | Call `turn.reconcile` with that id; follow its `recommended_action`. |
| `input.answer`, `input.decide`, `input.respond` | Required `response_id`, input `request_id` and answer | Repeat the same operation with the same ids and content. |
| `turn.cancel` | Required `cancellation_id` and `turn_ref` | Repeat with the same id and turn reference. |
| `worker.compact` | Required `operation_id` | Read `worker.compact.status` with that id. |
| `worker.kill` | Worker target | Repeat if needed. |

Reusing an id with conflicting parameters is refused. A new id means a new
action. Do not generate a new delivery id just because a send timed out.

For sends, a lost response, `incomplete`, or uncertain delivery data calls for
reconciliation. `recommended_action: "wait"` means wait or reconcile again;
`"retry_same_id"` permits repeating the saved send with the same delivery id.
`"inspect"` calls for checking the worker before deciding what to do.
`"none"` asks for no delivery retry. An `accepted` delivery means the message
was placed, not that the turn completed. See [workers and turns](workers.md).

A reconciliation can return `delivery.status: "not_found"` as successful data.
That alone does not prove an earlier request still running elsewhere cannot
arrive. Resolve outstanding attempts before deciding to submit again.

## Errors

Branch on `error.code`; use `details.reason` and other details for context.
Do not match the prose in `message`.

| Code | Meaning and next step |
|---|---|
| `invalid_request` | Bad JSON, envelope, operation or parameters; fix the request. Can also carry `internal_error` or `response_too_large` in `details.reason`. |
| `unsupported` | Engine cannot perform the operation; `details.engine` and `details.operation` identify it. |
| `not_found` | Requested worker or resource does not exist. |
| `unavailable` | Store or runtime cannot serve the request now. Creation may have partly completed; retain `worker_id`. |
| `waiting` | Worker needs input; use `worker.wait` and answer the pending request. |
| `refused` | State, id or cursor conflict; inspect details. For `cursor_invalidated`, restart the paged read. |
| `incomplete` | Deadline expired or outcome is uncertain; recover using the saved mutation id. |
| `budget_stopped` | A usage cap stopped or refused work. |
| `rate_limited` | Provider rate limit; wait before trying further work. |
| `transient_api_error` | Temporary provider failure; retry later after resolving any uncertain mutation. |
| `cancelled` | A serve-mode read was stopped by `request.cancel`. |

When present, `details.api_error` includes `kind`, `status`, `transient`,
`rate_limited` and `message`. A provider failure can occur after a send was
accepted; do not treat every error as permission to send a new message.

## Known limitations

- An RPC deadline bounds the request, not the model turn. Use `turn.cancel`
  to stop a particular turn.
- A valid `ok: true` response can describe pending or unknown work. Check the
  operation's status and recommended action before advancing your workflow.
- No response means no confirmed outcome. Keep mutation ids across client
  restarts so you can recover instead of duplicating work.
