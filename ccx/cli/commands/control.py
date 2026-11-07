from __future__ import annotations
import json
import sys
from ccx.constants import (
    EXIT_UNAVAILABLE,
)
from ccx.errors import (
    CcxError,
    InvalidRequest,
    SessionUnavailable,
)
from ccx.ops.semantics import (
    payload_guidance as _payload_guidance,  # noqa: F401
    )


def cmd_kill(args):
    """Confirm only the exact full-UUID Claude runtime exits."""
    from ccx.ops.wait import park_worker
    try:
        result = park_worker(args.target, timeout=getattr(args, "timeout", 90))
    except CcxError as error:
        sys.stderr.write(f"[ccx] kill: {error}\n")
        raise SystemExit(error.exit_code) from error
    if getattr(args, "json", False):
        print(json.dumps(result, indent=2))
    else:
        print(result["worker"])


def cmd_stop(args):
    """Request one exact full-UUID Claude turn interrupt, not a runtime kill."""
    from ccx.ops.cancel import stop_worker
    from ccx.errors import CcxError, DeliveryRejected
    try:
        result = stop_worker(args.target)
    except CcxError as error:
        if getattr(args, "json", False):
            print(json.dumps({
                "ok": False, "error": str(error),
                "status": getattr(error, "turn_status", "rejected"
                                  if isinstance(error, DeliveryRejected) else "unavailable"),
                "cancellation_id": getattr(error, "cancellation_id", None),
                "turn_ref": getattr(error, "turn_ref", None),
            }))
        print(f"ccx stop: {error}", file=sys.stderr)
        raise SystemExit(error.exit_code) from error
    value = {
        **result, "ok": True, "short": result["worker"]["short"],
        "requested": True,
        "cancelled": result["confirmed"] and result["outcome"] == "interrupted",
    }
    print(json.dumps(value) if getattr(args, "json", False)
          else (f"{result['worker']['worker']}: stop confirmed; the turn is no longer running, outcome unknown"
                if result["confirmed"] and result["outcome"] == "unknown"
                else f"{result['worker']['worker']}: stop {result['status']}; turn outcome {result['outcome']}"))


def cmd_attach(args):
    """Attach the user's terminal to the original native worker."""
    from ccx.store.root import require_ccx_active
    from ccx.constants import CCX_ROOT
    from ccx.client import CcxClient
    from ccx.claude.attach import attach as run_attach
    from ccx.ops.send import ensure_ready
    from ccx.store.root import GenerationError
    from ccx.store.liveness import owned_pid_alive

    try:
        owner = require_ccx_active(CCX_ROOT).resolve(args.target)
        if owner.engine == "codex":
            from ccx.cli.codex import cmd_attach as codex_attach
            return codex_attach(args)
        if not sys.stdin.isatty() or not sys.stdout.isatty():
            raise InvalidRequest("attach requires terminal stdin and stdout")
        client = CcxClient()
        record = client._resolve_managed(args.target, "attach")
        if record.get("engine") != "claude":
            raise InvalidRequest("shared attach supports managed Claude workers only")
        worker_id = record["workerId"]
        handle = f"claude:{worker_id}"
        native = record.get("native") or {}
        if not native.get("sessionId") or not isinstance(native.get("sessionId"), str):
            raise SessionUnavailable("Claude session is unavailable; check worker status")
        client._uuid_store.authorize_native_action(handle, "resume")

        # The normal ready helper intentionally refuses BUSY/WAITING deliveries.
        # A live, exact, attested host is already ready for attach regardless of
        # its turn state. Only stopped/unavailable hosts require normal revival.
        from ccx.rpc.rpc import _rpc
        status = _rpc(handle, {"op": "status"}, timeout=1.0)
        startup = _rpc(handle, {"op": "startup-status"}, timeout=1.0)
        current = client._uuid_store.record(handle)
        life = current.get("lifecycle") or {}
        proof = life.get("nativeStartProof") or {}
        live = (
            status.get("ok") is True
            and status.get("state") in ("idle", "busy", "waiting")
            and startup.get("ok") is True
            and startup.get("workerId") == worker_id
            and startup.get("generation") == client._uuid_store.generation
            and startup.get("nativeStartProof") == proof
            and proof.get("sessionId") == native["sessionId"]
            and proof.get("hostPid") == life.get("hostPid")
            and str(proof.get("hostStart")) == str(life.get("hostStart"))
            and proof.get("workerPid") == life.get("workerPid")
            and str(proof.get("workerStart")) == str(life.get("workerStart"))
            and owned_pid_alive(life.get("hostPid"), life.get("hostStart"))
            and owned_pid_alive(life.get("workerPid"), life.get("workerStart"))
        )
        if not live:
            ensure_ready(handle, timeout=90,
                         expected_generation=client._uuid_store.generation)
        reason = run_attach(handle, status=not getattr(args, "no_status", False))
        if reason == "connection-ended":
            raise SessionUnavailable("managed host connection lost; worker may have exited")
        if reason != "detached":
            print(f"[ccx] attach ended: {reason}", file=sys.stderr)
    except CcxError as error:
        print(f"[ccx] attach: {error}", file=sys.stderr)
        raise SystemExit(error.exit_code) from error
    except (GenerationError, OSError, ValueError, RuntimeError) as error:
        print(f"[ccx] attach: {error}", file=sys.stderr)
        raise SystemExit(EXIT_UNAVAILABLE) from error

from ccx.ops.maintenance import shutdown
from ccx.constants import EXIT_UNAVAILABLE

def cmd_shutdown(args) -> None:
    result = shutdown(dry_run=args.dry_run, timeout=args.timeout,
                      engine=getattr(args, "engine", None))
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        for engine in ("claude", "codex"):
            for row in result[engine]:
                label = f" ({row['name']})" if row.get("name") else ""
                print(f"{row['worker']}{label}: {row['action']}")
        print(f"codexd: {result['codexd']}")
        print(f"notifyd: {result['notifyd']}")
        for error in result["errors"]:
            sys.stderr.write(f"[ccx] shutdown: {error.get('worker', error['short'])}: {error['error']}\n")
    if not result["ok"]:
        sys.exit(EXIT_UNAVAILABLE)


def cmd_untrust(args) -> None:
    from ccx.constants import CLAUDE_DOTFILE
    from ccx.claude.settings import untrust_paths

    rows = untrust_paths(args.paths or None, dry_run=args.dry_run)
    if args.json:
        print(json.dumps({"config": str(CLAUDE_DOTFILE), "dry_run": args.dry_run,
                          "paths": rows}, indent=2))
        return
    if not rows:
        print(f"no trust entries recorded for {CLAUDE_DOTFILE}")
        return
    suffix = " (dry run)" if args.dry_run else ""
    for row in rows:
        print(f"{row['path']}: {row['action']}{suffix}")
