from __future__ import annotations

import os
import sys

from ccx.controllers.policy import reject_codex_blocking_call
from ccx.cli.parser import build_parser, ccx_version
from ccx.system.pipes import quiet_broken_stdout


# Only ALREADY-ENABLED user-facing worker-reference arguments. Never infer
# from a generic "target" name: controller run IDs, delivery/turn/request IDs,
# creation worker_id, native hook IDs and protocol rpc are different domains.
_USER_WORKER_FIELDS = {
    "new": ("vars_from",),
    "fork": ("target", "vars_from"),
    "resume": ("target",),
    "send": ("target", "vars_from"),
    "report": ("targets",),
    "context": ("target",),
    "peek": ("target", "extra_shorts"),
    "set": ("target",),
    "vars": ("target",),
    "events": ("target",),
    "logs": ("target",),
    "history": ("targets",),
    "wait": ("target",),
    "answer": ("target",),
    "approve": ("target",),
    "reject": ("target",),
    "respond": ("target",),
    "stop": ("target",),
    "kill": ("target",),
    "attach": ("target",),
    "steer": ("target",),
    "reconcile": ("target",),
    "compact": ("target",),
    "notify": ("target",),
}
def _preflight_worker_references(args, store) -> None:
    """Canonicalize all user prefix fields before any handler/effect starts."""
    if (args.cmd in {"new", "fork", "send"}
            and getattr(args, "var", None) is not None
            and getattr(args, "vars_from", None) is not None):
        # Existing mutual-exclusion refusal must win before discovery of an
        # unrelated --vars-from owner; no handler may claim a child or send.
        return
    if args.cmd == "controller":
        # Controller target/name/run IDs are a separate namespace. Only an
        # explicit worker input is CLI-discovered, before any handler effect.
        if (getattr(args, "controller_cmd", None) in {"input", "resume"}
                and getattr(args, "worker", None) is not None):
            from ccx.store.discovery import resolve_worker
            args.worker = resolve_worker(args.worker)
        return

    if (args.cmd == "peek" and args.status
            and getattr(args, "extra_shorts", None)):
        # Read-only multi-target status isolates failures in its row reader.
        # Single-target and effectful verbs still preflight atomically.
        return

    from ccx.store.discovery import resolve_cli_fields

    fields = _USER_WORKER_FIELDS.get(args.cmd, ())
    if fields:
        # Preserve the report command's multi-target per-row error results.
        multi_report = (args.cmd == "report" and len(args.targets) > 1)
        resolve_cli_fields(args, store, fields, check_exact=not multi_report)


def _native_handler(args, store):
    """Choose only existing native CLI handlers, after complete prefix preflight."""
    from ccx.cli import codex as codex_commands
    from ccx.constants import EXIT_ERROR
    from ccx.claude.usage import _caps_from_args

    def refuse(message):
        sys.stderr.write(f"ccx {args.cmd}: {message}; no native effect submitted\n")
        raise SystemExit(EXIT_ERROR)

    if args.cmd == "usage":
        if args.engine == "claude":
            if args.tokens:
                refuse("--tokens is Codex-only")
            if args.set_source and args.cached:
                refuse("--set-source cannot combine with --cached")
        elif args.engine == "codex" and (args.cached or args.summary or args.set_source):
            refuse("--cached/--summary/--set-source are Claude-only")
        if args.engine is None and args.set_source and (args.cached or args.tokens):
            refuse("--set-source cannot combine with --cached or --tokens")
        return codex_commands.cmd_usage if args.engine == "codex" else args.func

    if args.cmd in {"answer", "respond"}:
        for target in [args.target]:
            if store.resolve(target).engine != "claude":
                refuse("this operation is Claude-only")
        return args.func

    target_verbs = {
        "fork", "send", "steer", "reconcile", "wait", "notify",
        "approve", "reject", "set", "compact", "logs",
    }
    if args.cmd not in target_verbs:
        return args.func
    # Ordinary normalized history has its own typed owner/error handling.
    if args.cmd == "logs" and args.format == "conversation" and not args.jsonl:
        return args.func
    from ccx.store.identity import IdentityError, UnknownWorker
    try:
        owner = store.resolve(args.target)
    except IdentityError as error:
        if args.cmd == "logs":
            return args.func
        sys.stderr.write(f"ccx {args.cmd}: original CCX owner unavailable: {error}\n")
        raise SystemExit(2 if isinstance(error, UnknownWorker) else EXIT_ERROR) from error
    codex = owner.engine == "codex"
    settle = bool(getattr(args, "settle", False))
    notify = bool(getattr(args, "notify_parent", False))
    if args.cmd in {"fork", "send", "steer"}:
        if codex and "opus" in (_caps_from_args(args) or {}):
            refuse("--cap-opus is Claude-only")
    if args.cmd in {"fork", "send", "compact", "approve", "reject"}:
        if settle and (not codex or not notify):
            refuse("--settle requires a Codex worker and --notify-parent")
    if args.cmd == "fork":
        return args.func  # Fork derives native engine from the original source.
    if args.cmd == "steer" and codex and args.timeout is not None:
        refuse("--timeout on steer is Claude-only")
    if args.cmd == "reconcile" and not codex and args.turn_id is not None:
        refuse("--turn-id is Codex-only")
    if args.cmd == "wait":
        if codex and (args.next_turn or args.after_turn is not None or args.known_input is not None):
            refuse("--next-turn/--after-turn/--known-input are Claude-only")
        if not codex and (settle or args.wake_grace is not None):
            refuse("--settle/--wake-grace are Codex-only")
        if codex and not settle and args.wake_grace is not None:
            refuse("--wake-grace requires --settle")
        if codex:
            from ccx.codex.settle import DEFAULT_WAKE_GRACE_S
            args.wake_grace = (DEFAULT_WAKE_GRACE_S if args.wake_grace is None
                               else args.wake_grace)
    if args.cmd == "notify":
        if codex and args.next_turn:
            refuse("--next-turn is Claude-only")
        if not codex and settle:
            refuse("--settle is Codex-only")
    if args.cmd in {"approve", "reject"}:
        if codex and (args.wait or args.timeout is not None):
            refuse("--wait/--timeout on a native Codex gate are unsupported; use ccx wait")
        if not codex and settle:
            refuse("--settle is Codex-only")
        if codex and args.cmd == "reject" and args.reason is not None:
            refuse("--reason is Claude-only")
    if args.cmd == "set" and codex and (args.permissions is not None or args.restart):
        refuse("--permissions/--restart are Claude-only")
    if args.cmd == "compact" and codex and args.operation_id is not None:
        refuse("--operation-id is Claude-only")
    if args.cmd == "logs" and codex and (args.format != "conversation" or args.jsonl):
        refuse("--format rendered/--jsonl are Claude-only")
    codex_handlers = {
        "send": codex_commands.cmd_send,
        "steer": codex_commands.cmd_steer,
        "reconcile": codex_commands.cmd_reconcile,
        "wait": codex_commands.cmd_wait,
        "notify": codex_commands.cmd_notify,
        "approve": codex_commands.cmd_approve,
        "reject": codex_commands.cmd_reject,
        "set": codex_commands.cmd_set,
        "compact": codex_commands.cmd_compact,
    }
    return codex_handlers[args.cmd] if codex and args.cmd in codex_handlers else args.func


def _deepest_parser(parser, argv):
    """The (sub)command parser the argv selects, so errors show its usage."""
    import argparse
    for token in argv:
        actions = [a for a in parser._actions if isinstance(a, argparse._SubParsersAction)]
        if actions and token in actions[0].choices:
            parser = actions[0].choices[token]
    return parser


def main(argv=None):
    with quiet_broken_stdout():
        _main(argv)


# Internal relays run inside workers CCX launched; they must keep working there.
_SANDBOX_EXEMPT = {"skills", "host", "statusline-relay", "hooks-relay", "_live-vars-exec"}

# Explicit top-level user commands. Internal relays and the controller's
# private detached-host verbs keep a strict existing-store requirement.
_AUTO_INIT_COMMANDS = frozenset({
    "new", "import", "fork", "resume", "send", "steer", "reconcile",
    "wait", "notify", "answer", "approve", "reject", "respond",
    "set", "compact", "stop", "kill", "attach", "shutdown", "untrust",
    "vars", "list", "logs", "history", "report", "context", "peek", "events",
    "usage", "controller",
})


def _refuse_codex_sandbox(cmd: str) -> None:
    """Fail once, clearly, when a Codex parent runs ccx inside its own sandbox.

    CCX starts native Claude/Codex processes and uses ~/.claude, ~/.codex and
    its root, so inside the sandbox it would fail later in scattered ways.
    Codex sets CODEX_SANDBOX only on sandboxed commands (seatbelt on macOS).
    CODEX_SANDBOX_NETWORK_DISABLED is no signal: Codex also sets it on commands
    the user approved to run outside the sandbox.
    """
    if not os.environ.get("CODEX_SANDBOX") or cmd in _SANDBOX_EXEMPT:
        return
    sys.stderr.write(
        "ccx unavailable: running inside the Codex sandbox (CODEX_SANDBOX is set). "
        "CCX starts Claude/Codex processes and writes outside the workspace, which the "
        "sandbox blocks. Rerun this ccx command with escalated permissions (outside the "
        "sandbox), or start Codex with full access.\n"
    )
    raise SystemExit(3)


def _main(argv=None):
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    # Answer before touching state so `ccx --version` never creates ~/.ccx.
    if raw_argv in (["--version"], ["-V"]):
        print(f"ccx {ccx_version()}")
        return
    from ccx.errors import SessionUnavailable
    try:
        reject_codex_blocking_call(raw_argv)
    except SessionUnavailable as exc:
        sys.stderr.write(
            "ccx unavailable: caller worker identity is not proven in this "
            f"CCX_ROOT; use the worker's original root: {exc}\n"
        )
        raise SystemExit(3) from None
    if raw_argv[:1] == ["help"]:
        raw_argv = raw_argv[1:] + ["--help"]   # `ccx help [command]`
    p = build_parser()
    # Parse first: --help and usage errors exit here without creating ~/.ccx.
    args, extra = p.parse_known_args(raw_argv)
    if extra:
        _deepest_parser(p, raw_argv).error(f"unrecognized arguments: {' '.join(extra)}")
    _refuse_codex_sandbox(args.cmd)
    if args.cmd not in _SANDBOX_EXEMPT:
        # Keep `ccx skills install` copies current after an upgrade; cheap
        # when nothing changed, and never fatal.
        from ccx.cli.skills import refresh_installed
        refresh_installed()
    if args.cmd == "init":
        # Fully wired explicit empty-root bootstrap, not a legacy migration.
        args.func(args)
        return
    if args.cmd == "skills":
        # Setup discovery must work before any root exists or is initialized.
        args.func(args)
        return
    if args.cmd == "rpc":
        # RPC validates capabilities before opening an active store.
        args.func(args)
        return
    # A worker-owned Python shim is pinned to its creator root, even when
    # the command has an unrelated CCX_ROOT in its environment. The native
    # Python child still inherits that caller environment unchanged.
    if args.cmd == "_live-vars-exec" and args.root:
        # Shell functions generated before --root existed still call without
        # it; they keep resolving through the caller's CCX_ROOT.
        from ccx import constants
        # Native Python remains the caller's program: save its exact environment
        # before pinning CCX internals to this worker's (different) root.
        args._bridge_native_env = os.environ.copy()
        constants.select_root(args.root)
    # Validate the sealed store before handlers perform side effects.
    from ccx.constants import CCX_ROOT
    from ccx.store.root import GenerationError, require_ccx_active, open_or_initialize
    try:
        user_facing = (args.cmd in _AUTO_INIT_COMMANDS
                       and not (args.cmd == "controller"
                                and args.controller_cmd in {"_launch", "_host"}))
        if user_facing:
            active_store, created = open_or_initialize(CCX_ROOT)
            if created:
                sys.stderr.write(f"ccx: created store at {active_store.root}\n")
        else:
            active_store = require_ccx_active(CCX_ROOT)
    except (GenerationError, OSError) as exc:
        sys.stderr.write(f"ccx unavailable: {exc}\n")
        raise SystemExit(3) from None
    from ccx.errors import CcxError, SessionNotFound
    try:
        _preflight_worker_references(args, active_store)
    except CcxError as exc:
        sys.stderr.write(
            f"{exc}\n" if isinstance(exc, SessionNotFound)
            else f"ccx worker discovery: {exc}\n"
        )
        raise SystemExit(exc.exit_code) from exc
    handler = _native_handler(args, active_store)
    handler(args)
    return
