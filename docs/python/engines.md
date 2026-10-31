# Engine clients

`CcxClient` (Claude) and `CodexClient` (Codex) work on the same workers as
[`WorkerClient`](workers.md), with the options and features only one engine
has. They return typed, frozen result objects (from `ccx.types` and
`ccx.codex.types`) instead of dicts, and take a full handle or UUID as the
worker.

Use `WorkerClient` unless you need something on this page. Both kinds of client
can be used in the same program.

## CcxClient (Claude)

```python
from ccx import CcxClient
claude = CcxClient()
```

**Answering questions and forms:**

```python
claude.answer(w, "Use PostgreSQL", wait=True)            # one question
claude.answer(w, options=[(1, "Alpha"), (2, "Beta")])    # several, by option label
claude.answer(w, text_answers=[(1, "a custom answer")])  # free text per question
claude.respond_elicitation(w, "accept", {"name": "value"}, wait=True)  # MCP form
claude.respond_elicitation(w, "decline")                 # or "cancel"
```

Without `request_id`, they answer the request pending now. Pass a `response_id`
to make a retry safe: ccx applies each one once.

**Creating with Claude options.** `new(prompt, *, cwd, ...)` accepts Claude
Code's own options besides the usual ones: `agent`, `append_system_prompt`,
`allowed_tools`, `disallowed_tools`, `add_dir` and `permission_mode`.
`skip_permissions=False` is the restricted mode.

**Reading.**

| Method | Returns |
|---|---|
| `peek(w)` / `status([w, ...])` | One `SessionStatus` / a list of them: state, pending input, last activity. |
| `report(w)` | `Report` with the latest reply. |
| `logs(w, *, tail=None, jsonl=False)` | The conversation, or with `jsonl=True` Claude Code's raw transcript lines. |
| `tasks(w)` | The worker's own task list (Claude Code's TaskCreate/TaskUpdate items). |
| `background_tasks(w)` | Background shell commands, subagents and monitors it started, and whether they're still running. |
| `context(w)` | `ClaudeContext`: tokens used; window size and percentage are `None`. |
| `events(w)` / `subscribe(w)` | Claude's own event records, as typed `Event` objects. |
| `usage()` | Claude plan usage as a `Usage` object. |

**Delivery.** `steer_status(w, delivery_id)` and `delivery_status(w,
delivery_id)` look up an earlier steer or send without sending anything.
`deliver(w, text, delivery_id=...)` sends if the worker is idle and steers if
it's busy (`mode="new_turn_only"` only sends).

**Settings.** `set_model` and `set_effort` take `restart=True` to restart an
idle worker right away.

## CodexClient (Codex)

```python
from ccx import CodexClient
codex = CodexClient()
```

| Method | Does |
|---|---|
| `create(prompt, *, cwd, sandbox=..., approval_policy=..., delivery_id=None, ...)` | Creates with Codex's own sandbox and approval settings instead of `permissions`. |
| `reconcile(w, *, delivery_id=None, turn_id=None)` | Looks up what happened to a delivery or turn, without sending anything. |
| `wait(w, *, timeout=None, settle=False)` | With `settle=True`, also waits for the workers *that* worker started with `--notify-parent`. |
| `steer(w, text, *, delivery_id=None, caps=None)` | Adds to the running turn and returns when Codex confirms placement. Reuse the delivery id for retries. |
| `telemetry(w, *, after_cursor=None)` | Token use and tool activity per turn, in pages. |
| `approve(w, *, request_id)` / `reject(w, *, request_id)` | Decide one request; here `request_id` is required. |
| `usage(*, tokens=False)` | Codex plan usage, or with `tokens=True` token activity. |

## Known limitations

- Field names can differ from `WorkerClient` results.
- **Some names differ from `WorkerClient`**: Claude creates with `new`, and
  cancels with `interrupt` (it takes an `expected_turn_ref`, so a late call can't
  cancel a later turn).
