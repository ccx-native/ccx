# Python API

The Python clients use the same workers and store as the CLI. Start with
`WorkerClient` for both engines.
Install `ccx-native` in the environment your code runs in
(`pip install ccx-native` or `uv add ccx-native`).

```python
from ccx import WorkerClient

workers = WorkerClient()
created = workers.create(engine="claude", cwd="/repo", model="sonnet",
                         prompt="Summarize the open TODOs", wait=True)
w = created["worker"]                    # "claude:6f1c…" (engine and full UUID)
print(created["reply"])

reply = workers.send(w, "Now group them by file")
print(reply["reply"])
workers.kill(w)
```

## Which client

| Client | Use it for |
|---|---|
| `WorkerClient` | Almost everything. One set of methods for both engines; results are plain dicts. See [WorkerClient](workers.md). |
| `CcxClient` | Claude-specific options and typed results, including task lists and background tasks. See [Engine clients](engines.md). |
| `CodexClient` | Codex-only extras, such as reconciling a delivery or waiting with `settle`. See [Engine clients](engines.md). |
| `controller`, `task`, `current` | Scripts that drive several workers and survive interruption. See [Controllers](../controllers.md). |

Other helpers: `new_worker_id()` makes a UUID you can pass as `worker_id=` (and
save before creating, so a lost reply can't leave you guessing), and
`resolve_worker("6f1c2a9b")` turns a unique prefix into a full handle.

## Workers and results

- **Handles.** Methods take a worker handle (`"claude:<uuid>"`,
  `"codex:<uuid>"`), a bare full UUID, or any result dict that has a `"worker"`
  key, so calls chain. Prefixes and display names aren't accepted; use
  `resolve_worker` first.
- **Results** from `WorkerClient` are JSON-friendly dicts. Worker operations
  usually carry `worker`, `worker_id`, `engine` and `short`; account and list
  results have their own shapes. Engine clients return typed objects.
- **States** are the same as in the CLI: `starting`, `idle`, `busy`, `waiting`,
  `stopped`, `failed`, `unavailable`. `native_state` keeps the engine's own
  value, such as `budget-stopped`.
- **Engines are explicit.** `create` always takes `engine=`; ccx never guesses it
  from a model name.

## Waiting

`send` and `create(..., wait=True)` block until the turn ends. `timeout=None`
(the default) waits as long as it takes; a number of seconds bounds it.

- `wait(w)` returns a verdict dict. Running out of time is data there
  (`status="timeout"`), not an exception.
- In `send`, running out of time raises `TurnIncomplete` (exit 6).
- In `create`, a first-turn failure is wrapped in `CreatePartialFailure`,
  retaining the worker and delivery ids for recovery.

The worker may still be running after a timeout. Check with `wait` or look up
the delivery id before retrying.

## Errors

Every error is a `CcxError` with an `exit_code` that matches the CLI's (see
[the exit codes](../troubleshooting.md#what-the-exit-code-means)). Catch the
specific ones you handle:

| Exception | Exit | Means |
|---|---|---|
| `InvalidRequest` (and `CapabilityUnsupported`, `AmbiguousWorker`, `PayloadTooLarge`) | 1 | Bad input, or the engine doesn't support it. |
| `SessionNotFound` | 2 | No such worker or requested turn. |
| `SessionUnavailable` (and `SpawnFailed`, `EngineCliNotFound`) | 3 | Runtime or store unavailable, no completed reply yet, or the engine CLI is missing. |
| `CreatePartialFailure` | usually 3 | Creation didn't finish; retain `worker` and `worker_id` to recover it. A known cause can set another exit code. |
| `SessionWaiting` | 4 | The worker is waiting for input; `waiting_for` says what. |
| `DeliveryRejected`, `OperationConflict` | 5 | Refused in the current state (busy, nothing to stop). Nothing changed. |
| `TurnIncomplete` (and `SendIndeterminate`, `SteerIndeterminate`, `CompactFailed`) | 6 | The timeout ran out, the turn failed or was interrupted, or delivery is uncertain. Check before resending. |
| `OperationTimeout` | 6 | A read (conversation, report) ran out of time. |
| `BudgetStopped` | 7 | A usage cap refused or stopped the turn. |
| `WorkerRateLimited` | 8 | The provider rate-limited the account. |
| `WorkerApiTransient` | 9 | A temporary provider error; retry later. |

Errors carry context as attributes where it applies, such as `short`,
`delivery_id`, `state` and `refusal`.

## Known limitations

- **Questions and MCP forms are Claude-only.** `WorkerClient.answer` and
  `respond_elicitation` refuse Codex workers.
- **Calls block.** To drive several workers at once, use threads (one client can
  be shared) or a controller's `task.submit()`.
- **The store is per process.** `CCX_ROOT` is read when `ccx` is imported; set it
  before importing to use another store.
