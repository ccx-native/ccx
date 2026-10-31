# RPC operations

Accepted parameters and result summaries for protocol version 2.
See [requests and retries](protocol.md) for the envelope and error handling.

Use full worker handles as `target`; full bare UUIDs are also accepted.
Names and short prefixes are not accepted. In the tables, **bold parameters**
are required; all other parameters are optional. Every operation accepts
outer `timeout_ms`, not a timeout inside `params`.

## System and workers

| Operation | Parameters | Result |
|---|---|---|
| `system.info` | None | `protocol_version`, `ccx_version`. |
| `worker.create` | **`engine`**, **`cwd`**, `worker_id`, `name`, `model`, `effort`, `permissions`, `idle_timeout`, `sandbox`, `approval_policy` | `worker` (handle), `worker_view` (status), `created`, `reconciled`. |
| `worker.fork` | **`target`**, `worker_id`, `name`, `model`, `effort` | `worker`, `worker_view`, `created`, `reconciled`. |
| `worker.inspect` | **`target`** | `worker`, `worker_view` (recorded status) and `transcript_locator`. |
| `worker.wait` | **`target`**, `include_report`, `report_max_chars`, `next_turn`, `after_turn`, `known_input` | Live `status`, `reason`, `timed_out`, `pending_input`, `active_turn_ref`, `worker`, `worker_view`, optional report. |
| `worker.kill` | **`target`** | `killed`, `changed`, `interrupted`, `resumable`, `state`. |
| `worker.compact` | **`target`**, **`operation_id`** | Compaction `status`, `compacted`, `recommended_action`, context before/after. |
| `worker.compact.status` | **`target`**, **`operation_id`** | Status of the saved compaction; does not start another one. |

`engine` is `claude` or `codex`. `worker_id`, when supplied, must be a canonical
lowercase UUIDv4. `cwd` must identify an existing directory. `permissions` is
`full` (default) or `restricted`. `idle_timeout` is nonnegative seconds.

Codex-only `sandbox` accepts `read-only`, `workspace-write` or
`danger-full-access`; `approval_policy` accepts `untrusted`, `on-request` or
`never`. Do not combine those options with `permissions`.

Wait's `next_turn`, `after_turn` and `known_input` are Claude-only.
`after_turn` and `known_input` require `next_turn: true`.
`report_max_chars` requires `include_report: true`.
See [workers and turns](workers.md) for live versus recorded state.

## Turns and input

| Operation | Parameters | Result |
|---|---|---|
| `turn.send` | **`target`**, **`text`**, **`delivery_id`**, `mode`, `notify_parent`, `caller_metadata` | `delivery` and optional `notification`. |
| `turn.reconcile` | **`target`**, `delivery_id`, `turn_id`, `hold_while_pending` | `delivery`; sends no message. Supply exactly one delivery or turn id. |
| `turn.cancel` | **`target`**, **`cancellation_id`**, **`turn_ref`** | `cancellation` with status and recommended action. |
| `input.answer` | **`target`**, **`request_id`**, **`response_id`**, **`answers`**, `notify_parent` | `input` and optional `notification`. Claude questions only. |
| `input.decide` | **`target`**, **`request_id`**, **`response_id`**, **`decision`**, `reason` | `input`. Permissions on both engines; `reason` is Claude-only. |
| `input.respond` | **`target`**, **`request_id`**, **`response_id`**, **`action`**, `content` | `input`. Claude MCP forms only. |

`mode` is `new_turn_only` (default) or `auto`. Claude reconciliation requires
`delivery_id`; `turn_id` lookup is Codex-only. Use the saved delivery id for
recovery on either engine. Input's `params.request_id` identifies the pending
input, separately from the outer RPC request id.

See [retry identities](protocol.md#retry-identities) and
[answering input](input.md) for safe recovery and payload examples.

## Reads and transport

| Operation | Parameters | Result |
|---|---|---|
| `worker.report` | **`target`**, `max_chars`, `turn_ref` | Report fields directly in `result`, including `text`, `outcome`, `truncated`, `has_final_message`. |
| `conversation.page` | **`target`**, `cursor`, `source_record_target`, `max_tool_result_bytes` | `records`, `older_cursor`, `completeness`. |
| `transcript.read` | **`locator`**, `cursor`, `limit`, `max_tool_result_bytes` | `turns`, `next_cursor`, `has_more`, `source_changed`, `completeness`. |
| `worker.events` | **`target`**, `after_cursor`, `tail`, `limit`, `kinds`, `wait_ms` | `events`, `next_cursor`, `has_more`, `gap`, `timed_out`, `runtime`. |
| `worker.telemetry` | **`target`**, `after_cursor`, `limit` | `turns`, `compactions`, `totals`, `coverage`, `next_cursor`, `has_more`. |
| `account.usage.read` | **`engine`** | `provider`, `observed_at_ms`, `windows`. |
| `notification.inspect` | **`event_id`** | `notification` state. |
| `request.cancel` | **`cancel_request_id`** | `cancelled`, `reason`, `cancel_request_id`; serve mode only. |

See [history, events and usage](reading.md) for paging and bounds, and
[persistent mode](persistent.md) for read cancellation.

## Known limitations

- No `system.capabilities` operation is available. Handle `unsupported` and
  use the documented engine restrictions above.
- RPC does not expose every CLI or Python feature. Unknown operations and
  unknown parameters are rejected.
