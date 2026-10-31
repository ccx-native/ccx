# JSON protocol (RPC)

Drive local Claude Code and Codex workers from any language with JSON requests.
Use RPC when you want the same workers as the CLI without importing Python.

`ccx rpc` reads one JSON object from stdin, waits for EOF, writes one JSON
response line to stdout and exits. `ccx rpc --serve` keeps the process open for
[line-delimited requests](persistent.md). Diagnostics go to stderr.

## First request

With `ccx` installed (package: `ccx-native`), check the protocol without starting
any workers:

```sh
printf '%s\n' '{"protocol_version":2,"request_id":"info-1","op":"system.info","params":{}}' | ccx rpc
```

A successful response has this shape; the installed version fills `ccx_version`:

```json
{"protocol_version":2,"request_id":"info-1","ok":true,"result":{"protocol_version":2,"ccx_version":"..."}}
```

The first request creates the store if it doesn't exist. Set `CCX_ROOT` in the
subprocess environment if you need a separate store; every request in that
process uses it. See [getting started](../getting-started.md) for setup.

## Build a client

- [Requests, errors and retries](protocol.md): envelopes, deadlines and recovery after a lost response.
- [Workers and turns](workers.md): create → send → wait → report, cancellation and compaction.
- [Answering input](input.md): approvals, questions and forms.
- [History, events and usage](reading.md): paging, long polling and parent notifications.
- [Persistent mode](persistent.md): concurrent requests over one subprocess.
- [Operations reference](operations.md): accepted parameters and result summaries.

## Known limitations

- Only `protocol_version: 2` is accepted; there is no version negotiation.
- RPC is local and uses the current user's ccx store.
- RPC has no operations for shared variables or usage caps. Use the CLI or
  [Python API](../python/README.md) for those features.
