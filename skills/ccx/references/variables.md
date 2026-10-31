# ccx variables

Variables are named JSON values attached to a worker. Inside the worker,
plain `python3` (or `python`) sees them as ordinary Python variables; changes
are saved and visible to every worker sharing the same state.

## Attach, update, read

```bash
W=$(ccx new --engine codex --cwd ~/repo --var 'result={"done": false, "issues": []}' \
      "List the TODOs in src/. With python3, append each to result['issues'], then set result['done'] = True last.")
ccx wait "$W"
ccx vars "$W" result --json          # the value
ccx vars "$W" --json                 # names and metadata only
```

- `--var NAME=JSON` works on `new`, `send` and `fork`, and is repeatable. On
  `send` it attaches or replaces that variable.
- ccx tells the worker which names are available; you don't need to explain
  the mechanism, just say what to put where.
- Inside the worker, mutating a value or rebinding the name both write back;
  `del NAME` removes it.
- Start with a sentinel (`done: false`) and ask the worker to set it last, so an
  untouched value can't pass as an empty result.

## Sharing between workers

`--vars-from TARGET` gives a new worker (or a `send`) the same state as
`TARGET`, across Claude and Codex. Both see each other's changes at once, and
ccx runs their Python calls one at a time.

```bash
B=$(ccx new --engine claude --cwd ~/repo --vars-from "$W" \
      "With python3, read result['issues'] and add a 'priority' key to each.")
```

`--var` and `--vars-from` can't be combined in one command. A `fork` doesn't
copy the source's variables (pass `--vars-from SOURCE` to share them), and
workers a worker starts never inherit its variables.

## Rules

- Variables need full permissions (the default).
- Values attached from the CLI are JSON. A controller can attach live Python
  objects instead; see [controllers.md](controllers.md).
- Only direct `python`/`python3` sees them. `command python3`, `env python3`,
  a full interpreter path and scripts run through a shebang get normal Python.
  In Codex workers, nested bash and zsh keep them; in Claude workers, call
  `python3` directly from the tool shell.
- Keep values small and structured: verdicts, lists, paths, handles.

## When python3 doesn't see them

Check the worker's exact command and its selected Python with
`command -v python3` and `command python3 --version`. Leading interpreter flags
such as `python3 -u` bypass variables. On stock macOS, a non-login shell can
find Apple's older `/usr/bin/python3`, which can't run them.

Live controller objects require Python to match the controller's version and
platform; otherwise ccx runs ordinary Python with a warning. Saved JSON has no
matching-version check, but needs Python capable of running ccx's variable
support. See [variable troubleshooting](https://github.com/oneferdev/ccx/blob/main/docs/variables/troubleshooting.md).
