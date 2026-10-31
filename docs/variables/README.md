# Variables

Variables are named values attached to a worker. Its direct `python3` calls
see ordinary Python variables; ccx saves changes for later calls.

Use them for structured results such as findings, paths and completion flags.
For large data, save a file and pass its path.

## A first result

```bash
W=$(ccx new --engine codex --cwd ~/repo \
  --var 'result={"done":false,"paths":[]}' \
  "Find files containing TODOs. With python3, put their paths in result['paths']; set result['done'] = True last.")
ccx wait "$W"
ccx vars "$W" result --json
```

The last command returns metadata and a `value` field containing `result`.
Check `value.done` before treating an empty list as a completed result.
`ccx vars "$W" --json` lists names and metadata without their values.
Neither read waits for Python to finish; it shows the last saved data.

## Attach and change values

`new`, `fork` and `send` accept repeatable `--var NAME=JSON` arguments.
Quote JSON for your shell; a string value needs JSON double quotes inside:
`--var 'label="pending"'`. Names must be Python identifiers, cannot be keywords
and cannot start with `__`. Don't repeat a name in the same command.

On `send`, `--var` adds names or replaces their values, keeping other names:

```bash
ccx send "$W" --var 'result={"done":false,"paths":[]}' \
  "Check again with python3; update result and set result['done'] last."
```

ccx tells the worker which names are attached. Within a Python call, both
mutation (`result['done'] = True`) and rebinding (`result = {...}`) write back.
`del result` removes the binding. Only attached names are saved: assigning a
new unrelated Python name doesn't attach it. Add names with `--var` or put
new fields inside an attached dict.

- [Sharing and saved state](sharing.md): multiple workers, conflicts, types and lifetime.
- [Python and controllers](python.md): saved JSON and live Python objects.
- [Troubleshooting](troubleshooting.md): shell calls, Python selection and errors.

## Known limitations

- Variables require [full permissions](../permissions.md), the default.
  Restricted or sandboxed workers cannot attach them. Use files or replies
  when you need restricted workers.
- Only direct `python3` or `python` calls use the attached values. See
  [supported calls](troubleshooting.md#which-calls-see-variables).
- Saved state is limited to 4 MiB including its JSON metadata. Use paths to
  files for larger results.
- `--var` and `--vars-from` cannot be used together. Attach values first,
  then share them in a separate command.
