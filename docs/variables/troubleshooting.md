# Troubleshooting variables

Start by checking the attached names with `ccx vars "$W" --json`.
Then check the worker's exact Python command and which interpreter its shell selects.

## Which calls see variables

| Worker command | Attached variables |
|---|---|
| `python3 -c '...'`, `python3 script.py` | Yes |
| `python3 -m module`, `python3 -` or a heredoc | Yes |
| Direct `python ...` | Yes, if a native `python` exists |
| `command python3`, `env python3`, an explicit interpreter path | No |
| An executable script launched through its shebang | No |
| Python started by another program (`timeout`, `nohup`, `xargs`, `make`) | No |
| `python3 -u ...`, `python3 -I ...` and other leading interpreter options | No |
| `python3 -m pip`, `python3 -m venv`, `python3 -m ensurepip` | No |

Use a direct `python3 -c`, heredoc or `python3 script.py` when you need variables.
Flags and the three modules listed above deliberately use ordinary Python.
A Python subprocess started by your Python code also needs explicit data passing;
these names aren't environment variables or builtins available in every module.

ccx preserves existing shell aliases and functions named `python` or `python3`.
If one overrides the command, use a shell configuration without that override.

Codex installs variable support for Bash and zsh, including nested shells in
the same worker. Disabling startup files (for example `zsh -f`) or clearing
the shell environment can bypass it. Claude installs support in its tool shell;
don't rely on a separately started Bash or zsh loading it. Use a direct call in
the worker's tool shell. Neither engine passes variables implicitly to a new
ccx worker; [share them explicitly](sharing.md).

If your Codex configuration sets `shell_environment_policy.include_only`, include
`BASH_ENV`, `ZDOTDIR`, `CCX_ROOT`, `CCX_SELF_WORKER_ID` and `CCX_SELF_ENGINE`.
Otherwise Codex workers lose their variables, and ccx can't tell which worker a
command came from.

## Selecting Python for live objects

For live controller objects, the shell's selected Python must match the
controller Python's major/minor version, platform and implementation cache tag.
If ccx cannot verify that match, or it differs, the call prints a warning and
runs ordinary Python without the attached variables.

Inside the worker shell, inspect `command -v python3` and
`command python3 --version`; compare with the controller's Python. Adjust
`PATH` so a compatible Python is selected, then use direct `python3` again.
An explicit interpreter path bypasses variables even if that interpreter matches.

On macOS, a shell can select Apple's older system `python3`, including when
login startup changes `PATH`. Check the actual shell rather than assuming its
Python matches your terminal. A system may have no `python` command at all;
ccx doesn't create one or redirect it to `python3`. Prefer `python3`.

The compatibility check is for live objects. Saved JSON runs with the selected
shell Python and doesn't use this check; it still needs a Python capable of
running the ccx variable runner. Both engines support saved JSON and live
objects on Linux and macOS.

## Failed saves and lost live objects

- `CcxStateValueUnsupported`: convert the value to JSON types. No saved JSON
  changes from that call were committed.
- `CcxStateTooLarge`: shrink the attached data and retry. The shared document
  must fit within 4 MiB; use file paths for large results.
- `CcxStateUnavailable`: inspect the worker and its attachment before retrying;
  don't treat missing state as an empty result.
- A dead live controller can cause Python to run normally without variables;
  controller resume can raise `ControllerLiveStateLost`. Rebuild the objects
  in a fresh task/run or use saved JSON. A worker restart alone cannot recover
  controller memory.

## Known limitations

- Live `-c`, `-m` and script calls receive empty stdin. Pass inputs in attached
  values or files. `python3 -` reads stdin as program source; use a heredoc for it.
- A successful model turn doesn't prove it wrote a variable. Initialize a
  completion flag to false, ask the worker to set it last and check it yourself.
- Shell startup can change Python selection on either OS. Check the command
  inside the affected worker; don't dump its environment to diagnose this.
