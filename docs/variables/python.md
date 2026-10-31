# Python and controllers

`WorkerClient` can attach saved JSON values from a plain script or live Python
objects inside a controller. The two modes have different lifetimes.

## Saved JSON from a script

```python
from pathlib import Path
from ccx import WorkerClient

workers = WorkerClient()
created = workers.create(
    engine="codex", cwd=str(Path("~/repo").expanduser()), wait=True,
    variables={"result": {"done": False, "paths": []}},
    prompt="With python3, list paths containing TODOs in result['paths']; set result['done'] last.",
)
result = workers.variables(created, "result")["value"]
if not result["done"]:
    raise RuntimeError("Worker did not finish the result")
print(result["paths"])
```

`create`, `fork` and `send` accept `variables={...}` or
`variables_from=other_worker`, never both. Handles and result dicts work as
worker arguments; see [WorkerClient](../python/workers.md).
`variables(worker)["names"]` lists names;
`variables(worker, "result")["value"]` reads saved JSON without waiting.
The mapping you supplied is copied into saved state; read results through
`variables()`, not from that original mapping.

## Live controller objects

Inside an active [controller](../controllers.md) task, `variables=` retains
your mutable mapping and its objects. Worker Python code executes in the
controller process, so it can use custom Python objects without JSON conversion.

The following task belongs in a controller script with a `WorkerClient`
instance named `workers`:

```python
from ccx import current, task

@task
def classify(items: list[str]):
    bindings = {"items": items, "result": None}
    workers.create(
        engine="claude", cwd=str(current.workspace), wait=True,
        variables=bindings,
        prompt="With python3, assign result to a dict mapping each item in items to a category.",
    )
    return bindings["result"]
```

Keep the mapping and read its entries after waiting. If a worker rebinds
`result`, `bindings["result"]` holds the replacement; a separate local alias
to the old object doesn't change. Deleting a bound name removes its mapping
entry. New unrelated names aren't attached automatically.

To share live bindings, pass the same mapping to each worker in that task.
Worker Python calls within the controller run one at a time. Controller code
that accesses objects directly isn't protected by that serialization; wait for
worker calls before inspecting or changing them.

## Known limitations

- Live objects last only while their controller process is alive. Worker
  `kill`/revive doesn't save them to disk. On controller resume, replaying a
  call that depended on lost live objects raises `ControllerLiveStateLost`
  instead of running it again. Use live objects within one task and return
  plain data; use saved JSON or files for persistent state.
- Live changes, including rebinding and deletion, remain even if Python raises
  an exception or the model turn fails. Validate the result; there is no rollback.
- `variables()` and `ccx vars` can list live names, but cannot read live object
  values; metadata reports `available: false`. Read the retained mapping.
- `variables_from` cannot share live objects, and a worker cannot mix live and
  saved bindings. Use a separate worker for the other mode.
- Live calls require a compatible shell Python and give `-c`, `-m` and script
  code an empty stdin. See [troubleshooting](troubleshooting.md).
