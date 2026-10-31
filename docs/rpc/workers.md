# Workers and turns

Create an idle worker, send a task, wait for live state and read its reply.
Save ids before sending requests so [retries](protocol.md#retry-identities) remain safe.

## Create

The directory must exist. This example uses a caller-chosen UUIDv4 and starts
an idle Claude worker in `~/repo`:

```sh
printf '%s\n' '{"protocol_version":2,"request_id":"create-1","op":"worker.create","params":{"engine":"claude","cwd":"~/repo","worker_id":"12345678-1234-4abc-8def-123456789abc","permissions":"restricted"}}' | ccx rpc
```

Use `engine: "codex"` for Codex. Both accept `model`, `effort`, `name` and
`idle_timeout` in seconds. `permissions: "restricted"` enables permission
checks; `"full"` is the default. Codex also accepts `sandbox` and
`approval_policy` instead of `permissions`. See the [reference](operations.md).

For create, fork, inspect and wait, `result.worker` is the handle and
`result.worker_view` is the recorded worker status.

Use full handles such as `claude:12345678-1234-4abc-8def-123456789abc` as
`target`. A full bare UUID is also accepted. Names and short prefixes are not
targets. Keep native session/thread ids out of RPC targets. Treat turn
references as opaque strings and pass them back unchanged.

`worker.fork` copies a worker's history into a separate worker. Supply its own
`worker_id` for retryable creation. `created` and `reconciled` describe whether
the create/fork request reserved a new worker or reused the supplied id; they
are not proof that a task completed.

## Send and recover

```json
{"protocol_version":2,"request_id":"send-1","op":"turn.send","params":{"target":"claude:12345678-1234-4abc-8def-123456789abc","delivery_id":"task-1","text":"Summarize the repository README.","mode":"new_turn_only"}}
```

Read `result.delivery`, including `status`, `recommended_action`, `placement`
and `turn_ref` when available. Retain the turn reference for later reads.

- `new_turn_only` is the default: start a turn when idle and refuse a busy worker.
- `auto` starts a turn when idle or adds the message to the running turn.
  It refuses a worker waiting for input.
- Acceptance and `placement: "current_turn"` do not mean completion.
  Pending work with `recommended_action: "wait"` needs observation, not another send.

If the reply is lost or uncertain, read the saved delivery:

```json
{"protocol_version":2,"request_id":"recover-1","op":"turn.reconcile","timeout_ms":30000,"params":{"target":"claude:12345678-1234-4abc-8def-123456789abc","delivery_id":"task-1","hold_while_pending":true}}
```

This sends nothing. With `hold_while_pending: true`, it waits while the delivery
is pending and its recommended action is `wait`, within the request budget.
Follow the [retry rules](protocol.md#retry-identities) rather than inferring a
retry from a status name alone.

## Wait and read the reply

```json
{"protocol_version":2,"request_id":"wait-1","op":"worker.wait","timeout_ms":120000,"params":{"target":"claude:12345678-1234-4abc-8def-123456789abc","include_report":true}}
```

Use the wait result's top-level `status`, `pending_input` and `active_turn_ref`
for live decisions. Status is `starting`, `idle`, `busy`, `waiting`, `stopped`,
`failed`, `unavailable` or `timeout`. A timeout returns `timed_out: true`;
continue observing if you still need the result. `waiting` means the worker
needs an [input response](input.md).

`include_report: true` adds `report` for a positively completed turn when it
can read one within the budget. Check `report_error` if it is absent. Read
`report.text`, `outcome`, `has_final_message` and `truncated`; an idle worker
alone does not establish that your task produced an answer.

For a particular task, call `worker.report` with the `turn_ref` from its
delivery. Without it, the report is for the latest finished turn. The result
itself is the report (so its text is `result.text`). `max_chars` defaults to
65536; `null` removes that text bound, subject to the overall response limit.

`worker.inspect` returns promptly from recorded state. Its status has
`state_source: "recorded"`, `loaded: null` and `pending_input_observed: false`.
A null pending input there does not mean no input is waiting. Use `worker.wait`
for a live decision, even when a recorded state looks idle.

Claude supports `next_turn: true` to wait for a later turn, including one
started by a background task. `after_turn` supplies a turn reference and
`known_input` identifies an already-seen input request; both require
`next_turn: true`. These selectors are not supported for Codex.

## Cancel, stop or compact

`turn.cancel` takes the running turn's `turn_ref` and your `cancellation_id`.
Read `result.cancellation`: only `status: "confirmed"` confirms cancellation
of that turn. Keep observing on an uncertain result. The turn reference prevents
a delayed cancellation from stopping a later turn.

`worker.kill` ends the worker runtime. A later `turn.send` can revive it with
its saved history; check the kill result's `resumable` field.

`worker.compact` runs native context compaction on either engine. Save an
`operation_id`, then use `worker.compact.status` to recover or poll it.
Check `status`, `compacted` and `recommended_action`; a submitted operation
is not finished, and `compacted: false` alone does not explain the outcome.

## Known limitations

- Create has no initial prompt. Send the task separately.
- Wait's included report can be null or fail to read while the live wait
  result remains valid. Read the specific turn with `worker.report` if needed.
- Worker status is a snapshot. Another caller can change the worker before
  your next request; handle refusals and keep the same ids for recovery.
