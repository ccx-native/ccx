"""Execution helpers for CCX worker variables.

This module intentionally depends only on the Python standard library. The
standalone durable path executes this file with the worker-selected Python,
which may not have the ccx package importable.
"""

from __future__ import annotations

import argparse
import json
import linecache
import os
from pathlib import Path
import runpy
import sys
import traceback
from typing import Any


SCHEMA_VERSION = 1
STATE_FILE_LIMIT = 4 * 1024 * 1024


def strict_json(value: Any) -> str:
    return json.dumps(
        value,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _system_exit_code(error: SystemExit) -> int:
    if error.code is None:
        return 0
    if isinstance(error.code, int):
        return error.code
    if error.code:
        print(error.code, file=sys.stderr)
    return 1


def execute_target(args: list[str], globals_seed: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    """Execute one supported Python invocation against a fresh globals mapping."""
    ns = dict(globals_seed)
    ns.setdefault("__name__", "__main__")
    try:
        if not args or args[0] == "-":
            if sys.path:
                sys.path[0] = ""
            sys.argv = ["-", *args[1:]]
            ns["__file__"] = "<stdin>"
            source = sys.stdin.read()
            exec(compile(source, "<stdin>", "exec"), ns, ns)
            return 0, ns

        if args[0] == "-c":
            if sys.path:
                sys.path[0] = ""
            if len(args) < 2:
                print("argument expected for -c", file=sys.stderr)
                return 2, ns
            sys.argv = ["-c", *args[2:]]
            ns["__file__"] = "<string>"
            # The bootstrap itself ran as -c; give traceback the user's source.
            linecache.cache["<string>"] = (
                len(args[1]), None, args[1].splitlines(keepends=True), "<string>"
            )
            exec(compile(args[1], "<string>", "exec"), ns, ns)
            return 0, ns

        if args[0] == "-m":
            if sys.path:
                sys.path[0] = ""
            if len(args) < 2:
                print("argument expected for -m", file=sys.stderr)
                return 2, ns
            sys.argv = [args[1], *args[2:]]
            # Resolve packages through runpy, but retain the executing globals
            # even when the module exits or raises before runpy can return them.
            _, spec, code = runpy._get_module_details(args[1])
            with runpy._TempModule("__main__") as temporary, runpy._ModifiedArgv0(spec.origin):
                ns = temporary.module.__dict__
                ns.update(globals_seed)
                runpy._run_code(code, ns, mod_name="__main__", mod_spec=spec)
            return 0, ns

        if args[0].startswith("-"):
            print(
                f"ccx shared python: unsupported interpreter option {args[0]!r}; "
                "use an explicit interpreter path for native Python",
                file=sys.stderr,
            )
            return 2, ns

        script = Path(args[0]).resolve()
        if sys.path:
            sys.path[0] = str(script.parent)
        sys.argv = [str(script), *args[1:]]
        ns["__file__"] = str(script)
        source = script.read_bytes()
        exec(compile(source, str(script), "exec"), ns, ns)
        return 0, ns
    except SystemExit as error:
        return _system_exit_code(error), ns
    except KeyboardInterrupt as error:
        tb = error.__traceback__
        traceback.print_exception(type(error), error, tb.tb_next if tb else None)
        return 130, ns
    except BaseException as error:
        tb = error.__traceback__
        traceback.print_exception(type(error), error, tb.tb_next if tb else None)
        return 1, ns


def _atomic_write(path: Path, text: str) -> None:
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(text, encoding="utf-8")
    try:
        os.chmod(tmp, 0o600)
    except OSError:
        pass
    os.replace(tmp, path)


def _parse_runner_args(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--state", required=True)
    parser.add_argument("--lock", required=True)
    parser.add_argument("--cwd", required=True)
    parser.add_argument("--real-python", required=True)
    parser.add_argument("target_args", nargs=argparse.REMAINDER)
    value = parser.parse_args(argv)
    if value.target_args and value.target_args[0] == "--":
        value.target_args = value.target_args[1:]
    return value


def durable_main(argv: list[str] | None = None) -> int:
    """Run one durable invocation under a whole-invocation file lock."""
    import fcntl

    args = _parse_runner_args(argv)
    state_path = Path(args.state)
    lock_path = Path(args.lock)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        try:
            document = json.loads(state_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            print("ccx: worker variable state is no longer available", file=sys.stderr)
            return 69

        if not isinstance(document.get("stateId"), str):
            print("ccx: durable variable state is malformed", file=sys.stderr)
            return 69

        names = {
            str(name)
            for name in document.get("names", [])
            if isinstance(name, str)
        }
        variables = document.get("variables")
        if not isinstance(variables, dict):
            variables = {}

        seed = {name: variables[name] for name in names if name in variables}
        old_cwd = os.getcwd()
        old_argv = sys.argv[:]
        try:
            os.chdir(args.cwd)
            exit_code, final_ns = execute_target(args.target_args, seed)
        finally:
            sys.argv = old_argv
            os.chdir(old_cwd)

        if exit_code != 0:
            return exit_code

        final_names = sorted(name for name in names if name in final_ns)
        final_variables = {name: final_ns[name] for name in final_names}
        try:
            encoded_variables = strict_json(final_variables)
        except (TypeError, ValueError) as error:
            bad_name = "<unknown>"
            for name, value in final_variables.items():
                try:
                    strict_json(value)
                except (TypeError, ValueError):
                    bad_name = name
                    break
            print(
                f"CcxStateValueUnsupported: variable {bad_name!r} can't be saved: "
                f"{error}; use JSON dicts, lists, strings, numbers, booleans or null",
                file=sys.stderr,
            )
            return 1

        updated = dict(document)
        updated["schemaVersion"] = SCHEMA_VERSION
        updated["names"] = final_names
        updated["variables"] = json.loads(encoded_variables)
        text = strict_json(updated)
        if len(text.encode("utf-8")) > STATE_FILE_LIMIT:
            print("CcxStateTooLarge: worker variables exceed the 4 MiB state limit; "
                  "shrink a bound variable and retry", file=sys.stderr)
            return 1
        _atomic_write(state_path, text)
        return 0



if __name__ == "__main__":
    raise SystemExit(durable_main())
