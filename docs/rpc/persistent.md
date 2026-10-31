# Persistent mode

Keep one `ccx rpc --serve` subprocess open to handle repeated requests.
Use the same operations and envelopes as [one-shot RPC](protocol.md).

## Transport loop

Start the child with piped stdin, stdout and stderr, and set `CCX_ROOT` before
launching it (the store is created if missing). Write one UTF-8 JSON
object per line and flush; encode newlines inside text as JSON escapes.

```sh
ccx rpc --serve
```

A client should:

1. Register a pending response by a fresh `request_id` before writing the line.
2. Read stdout continuously, buffer partial lines and decode each complete line.
3. Match each response by `request_id`, validate the envelope and resolve that request.
4. Drain stderr separately for diagnostics.
5. On process exit or broken stdin, fail unresolved calls as lost responses.
   Restart the transport as needed and recover mutations using their saved ids.

Requests run concurrently and responses arrive in completion order. Sequence
dependent requests yourself: await create before send, and retain the delivery
and turn ids before waiting or reading a report. At most 256 requests can run
at once; excess requests return `unavailable`. Reusing an in-flight request id
returns `refused` and makes correlation ambiguous, so avoid it.

Each request runs in a separate child process. The server ends a request still
running at its `timeout_ms` budget plus five seconds and returns `incomplete`.
This does not establish that a mutation had no effect. Give client deadlines
enough grace to receive that response.

## Cancel an abandoned read

For an outstanding read with outer id `wait-1`, write a separate request:

```json
{"protocol_version":2,"request_id":"cancel-wait-1","op":"request.cancel","params":{"cancel_request_id":"wait-1"}}
```

If cancelled, the read receives `ok: false` with code `cancelled`, followed by
the cancel request's own response:

```json
{"protocol_version":2,"request_id":"cancel-wait-1","ok":true,"result":{"cancel_request_id":"wait-1","cancelled":true,"reason":null}}
```

If the read already finished, its response wins and cancellation returns
`cancelled: false`, `reason: "not_in_flight"`. A mutation returns
`cancelled: false`, `reason: "not_cancellable"` and keeps running.

Cancellable reads are `system.info`, `worker.inspect`, `worker.wait`,
`turn.reconcile`, `worker.report`, `worker.telemetry`, `worker.events`,
`worker.compact.status`, `transcript.read`, `conversation.page`,
`account.usage.read` and `notification.inspect`.

Closing stdin lets requests in flight finish before the server exits.
If the server dies, its observing requests stop; mutations may continue to
finish within their own budgets. Recover their outcomes before retrying.

## Known limitations

- `request.cancel` is available only in serve mode and only for reads on
  that server. Use `turn.cancel` to cancel a worker turn.
- There are no unsolicited response lines for events or notifications;
  poll the corresponding operations.
- A server uses one store and cannot switch stores after startup. If that
  store is reset or replaced, restart the server.
