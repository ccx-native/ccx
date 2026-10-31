# Sharing and saved state

Share saved JSON variables between Claude and Codex workers with
`--vars-from`. Workers must use the same ccx store.

## Share a result

After creating `W` in the [quick example](README.md#a-first-result):

```bash
B=$(ccx new --engine claude --cwd ~/repo --vars-from "$W" \
  "With python3, read result['paths'] and add a 'summary' field to result.")
ccx wait "$B"
ccx vars "$W" result --json
```

Both workers now use the same saved state. This is sharing, not a copy.
`ccx send "$B" --vars-from "$W" "..."` also switches an existing worker
to that state, replacing its attachment rather than merging two sets of values.
Updating a value through `--var` on either worker updates their shared state.

A `fork` copies conversation history without copying variables. Use
`ccx fork "$W" --vars-from "$W"` to share them, or pass fresh `--var` values
for independent state. A worker created by another worker never inherits its
variables; pass `--vars-from` explicitly.

## Conflicting writes

ccx runs entire Python calls against the same saved state one at a time.
A read, modification and write in one call therefore uses the latest saved
value. Separate calls can interleave: keep an increment or other dependent
update in one call. If two calls assign different replacements, the last
successful call wins; ccx doesn't merge their values or promise call order.

Changes are saved only when the Python call exits successfully and all attached
values can be saved within the size limit. A Python exception, nonzero exit,
unsupported value or oversized result leaves the previous saved state intact.
This applies to one Python call, not a whole model turn: earlier successful
calls stay saved if a later call or the turn fails. File writes and other
external effects aren't rolled back.

`ccx vars` returns the last saved data immediately, including while a call is
running. Wait for the worker, then check your completion flag before using it.

## Types and lifetime

Saved values are JSON: dicts, lists, strings, numbers, booleans and `None`
(`null` in CLI JSON). Tuples become lists and numeric dict keys become strings.
Object identity and aliases aren't preserved. Sets, bytes, custom objects,
NaN and infinity cannot be saved. Convert them to JSON data, or use
[live controller objects](python.md#live-controller-objects).

Saved values survive worker idle unload, `ccx kill`, and revival with `send` or
`resume`. Killing a worker doesn't clear its variables or undo saved updates.
An interrupted Python call may not have saved its changes; inspect the result
before continuing. Shared state remains usable by the other attached workers.

## Known limitations

- Attaching values on `send` happens before delivery. A later delivery failure
  can leave the values attached; inspect them before retrying or resetting.
- The 4 MiB limit covers the whole shared JSON document, not each variable.
  Store large data in files and attach their paths.
- `--vars-from` shares saved JSON only. For live objects, attach the same
  mutable mapping to workers inside one controller task.
- Serialization covers worker Python calls, not your own unrelated file or
  database writes. Use the relevant storage system's concurrency controls.
