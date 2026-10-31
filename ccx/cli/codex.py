from __future__ import annotations
import ccx.ops.input as ops_input

import json
import os
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any

from ccx.codex.ops import caller_status, configure_worker
from ccx.codex.client import CodexClient
from ccx.codex.types import rate_limit_windows, window_label
from ccx.claude.usage import _caps_from_args
from ccx.codex import runtime as runtime
from ccx.controllers import live_vars
from ccx.codex.settle import (
    DEFAULT_WAKE_GRACE_S,
    wait_for_settlement,
)
from ccx.constants import (
    EXIT_ERROR,
    EXIT_NOT_FOUND,
    EXIT_REJECTED,
    EXIT_TURN_INCOMPLETE,
    EXIT_UNAVAILABLE,
    EXIT_WAITING,
)
from ccx.errors import BudgetStopped, CcxError, DeliveryRejected, EngineCliNotFound, InvalidRequest, SendIndeterminate
from ccx.controllers.orchestrator import require_notify_parent
from ccx.notify.daemon import prepare_notify_parent
from ccx.notify.registry import public_registration as public_parent_notification, set_codex_turn_id

STABLE_STATES = {"idle", "waiting", "idle-killed", "crashed", "dead"}


def cmd_attach(args) -> None:
    """Run the real Codex remote TUI on codexd's live native thread."""
    from ccx.system.engine_binary import resolve_engine_binary
    from ccx.store.root import GenerationError

    try:
        if not sys.stdin.isatty() or not sys.stdout.isatty():
            raise InvalidRequest("attach requires terminal stdin and stdout")
        with runtime.attached_worker(args.target) as session:
            argv = [resolve_engine_binary("codex"), "resume", session["threadId"]]
            # Codex's startup update prompt defaults to running an installer.
            argv += ["-c", "check_for_update_on_startup=false"]
            # Turns typed in the TUI use the worker's settings, as CCX sends do.
            if session.get("model"):
                argv += ["-m", session["model"]]
            if session.get("effort"):
                argv += ["-c", f'model_reasoning_effort="{session["effort"]}"']
            argv += ["--remote", "unix://" + session["socket"]]
            from ccx.codex.attach_tty import run_status_tui
            exit_code = run_status_tui(
                argv, cwd=session["cwd"], worker_id=session["workerId"],
                status=not getattr(args, "no_status", False),
            )
        if exit_code:
            if not session.get("hasTurn"):
                # Codex writes a thread's rollout with its first turn.
                print("[ccx] attach: if Codex could not resume this thread, it may "
                      "have no turn yet; send it a first message with `ccx send`, "
                      "then attach", file=sys.stderr)
            raise SystemExit(exit_code)
    except (CcxError, GenerationError, OSError, RuntimeError) as error:
        print(f"[ccx] attach: {error}", file=sys.stderr)
        raise SystemExit(getattr(error, "exit_code", EXIT_UNAVAILABLE)) from error


def _fail(message: str, code: int = EXIT_ERROR) -> None:
    detail = message.removeprefix("ccx: ")
    if detail.startswith("ccx ") and ": " in detail:
        heading, body = detail.split(": ", 1)
        verb = heading.removeprefix("ccx ")
        while body.startswith((f"{verb}: ", f"{heading}: ")):
            body = body.split(": ", 1)[1]
        detail = f"{heading}: {body}"
    else:
        detail = f"ccx: {detail}"
    print(detail, file=sys.stderr)
    raise SystemExit(code)


def _fail_send_indeterminate(error, args) -> None:
    delivery_id = getattr(error, "delivery_id", None)
    status = getattr(error, "delivery_status", None)
    if getattr(args, "json", False):
        print(json.dumps({
            "accepted": False,
            "reconcile_required": True,
            "delivery_id": delivery_id,
            "delivery_status": status,
            "error": str(error),
        }, indent=2))
        raise SystemExit(EXIT_TURN_INCOMPLETE)
    suffix = f"; delivery_id={delivery_id}" if delivery_id else ""
    _fail(
        f"{error}{suffix}; reconcile this delivery and do not resend it",
        EXIT_TURN_INCOMPLETE,
    )


def _fail_send_rejected(error, short: str) -> None:
    """A definite send refusal: nothing started, so exit with its own code
    (EXIT_REJECTED for busy/waiting) and say how to deliver the text later."""
    message = _send_rejection_guidance(short, {
        "error": getattr(error, "reason", None) or str(error),
        "state": getattr(error, "state", None),
    })
    delivery_id = getattr(error, "delivery_id", None)
    if delivery_id and getattr(error, "delivery_status", None) == "prepared":
        # Refused before submission: the same id is safe to retry later.
        message = (
            f"{message}; nothing was submitted; delivery_id={delivery_id}. To "
            f"send this text once the worker is idle (`ccx wait {short}`), "
            f"retry it with `--delivery-id {delivery_id}`"
        )
    _fail(message, error.exit_code)


def _prompt(args, *, required: bool) -> str | None:
    sources = sum([
        getattr(args, "text", None) is not None
        or getattr(args, "prompt", None) is not None,
        bool(getattr(args, "stdin", False)),
        getattr(args, "file", None) is not None,
    ])
    if sources > 1:
        _fail("use exactly one of positional text, --stdin, or --file")
    positional = getattr(args, "text", None)
    if positional is None:
        positional = getattr(args, "prompt", None)
    if getattr(args, "stdin", False) or getattr(args, "file", None) == "-":
        value = sys.stdin.read()
    elif getattr(args, "file", None):
        try:
            value = Path(args.file).read_text()
        except OSError as error:
            _fail(f"cannot read prompt file: {error}")
    else:
        value = positional
    if required and (value is None or not value.strip()):
        _fail("prompt text is required")
    return value


def _rpc(short: str, message: dict[str, Any], timeout: float = 10) -> dict[str, Any]:
    return runtime.rpc(short, message, timeout)


def _record(target: str) -> dict[str, Any]:
    try:
        return runtime.record(target)
    except CcxError as error:
        _fail(str(error), error.exit_code)


def _require_workspace(record: dict[str, Any]) -> None:
    """Refuse to start project work in a workspace that no longer exists."""
    try:
        runtime.require_workspace(record)
    except CcxError as error:
        _fail(str(error), error.exit_code)


def _identity(record: dict[str, Any]) -> dict[str, Any]:
    return runtime.identity(record)


def _emit(value: dict[str, Any], args, *, plain: str | None = None) -> None:
    if getattr(args, "json", False):
        print(json.dumps(value, indent=2))
    elif plain is not None:
        print(plain)
    else:
        print(value.get("short", ""))


def _wait(short: str, timeout: float | None) -> dict[str, Any]:
    return runtime.wait(short, timeout)


def _emit_wait(
    result: dict[str, Any],
    args,
    *,
    include_report: bool = True,
    expected_turn_id: str | None = None,
) -> None:
    state = result.get("state")
    last_turn = result.get("lastTurn") or result.get("last_turn")
    if not isinstance(last_turn, dict):
        last_turn = None
    # A non-success turn is this command's failure only when the command was
    # waiting for that turn. Bare `ccx wait` asks about worker health, so
    # an older failed or interrupted turn stays visible without making a
    # healthy idle worker exit nonzero.
    outcome_matches = expected_turn_id is not None and (
        last_turn is not None and last_turn.get("turnId") == expected_turn_id
    )
    settled_non_success = (
        last_turn is not None
        and last_turn.get("status") in {"failed", "interrupted"}
    )
    # A wait that ran out of budget, or found an active record the daemon no
    # longer serves, is not a stable worker, whatever the recorded state says.
    timed_out = result.get("ok") is False and result.get("error") == "wait timed out"
    orphaned = result.get("ok") is False and result.get("loaded") is False
    output = {
        "engine": "codex",
        "short": result.get("short"),
        "thread_id": result.get("threadId") or result.get("thread_id"),
        "name": result.get("name"),
        "cwd": result.get("cwd"),
        "state": state,
        "active_turn_id": result.get("activeTurnId")
        or result.get("active_turn_id"),
        "pending_approvals": result.get("pendingApprovals") or [],
        "last_turn": last_turn,
        "active_turn_error": (
            result.get("activeTurnError") or result.get("active_turn_error")
        ),
        # Evidence health, beside worker health rather than inside it: a
        # degraded journal never changes the state or the exit code.
        "evidence": result.get("evidence"),
        # The status of the turn this command waited for. Null when no
        # specific turn was awaited (bare `wait`) or when the settled outcome
        # belongs to another turn; `last_turn` stays visible either way.
        "turn_status": (
            last_turn.get("status") if outcome_matches else None
        ),
        "timed_out": timed_out,
    }
    if timed_out or orphaned:
        output["error"] = result.get("error")
    report = result.get("lastReport")
    if not isinstance(report, dict):
        report = None
    # Scopes 02/04 deliberately preserve an older valid report when a newer
    # turn produces none, so the worker's history keeps it. That history is not
    # this operation's answer: a command that waited for one turn may only
    # present a report that came from that turn and settled with it.
    if expected_turn_id is not None and not (
        outcome_matches and report is not None
        and report.get("turnId") == expected_turn_id
    ):
        report = None
    if include_report and report:
        output["report"] = report
    if result.get("settle"):
        output.update({
            "worker": result.get("worker"), "settle": True,
            "settlement_verdict": result.get("settlementVerdict"),
            "settled": bool(result.get("settled")),
            "outstanding_count": int(result.get("outstandingCount") or 0),
            "outstanding": list(result.get("outstanding") or []),
            "outstanding_truncated": bool(result.get("outstandingTruncated")),
            "pending_wake_count": int(result.get("pendingWakeCount") or 0),
            "pending_wake": list(result.get("pendingWake") or []),
            "pending_wake_truncated": bool(result.get("pendingWakeTruncated")),
            "registration_count": int(result.get("registrationCount") or 0),
            "wake_grace_seconds": result.get("wakeGraceSeconds"),
            "last_message": result.get("lastMessage"),
            "error": result.get("error"),
        })
    if getattr(args, "json", False):
        print(json.dumps(output, indent=2))
    elif result.get("settle"):
        verdict = result.get("settlementVerdict") or "unknown"
        print(f"{output['worker']}  {verdict}")
    elif timed_out or orphaned:
        print(f"{output['short']}  {state} ({result.get('error')})")
    elif state == "waiting":
        print(json.dumps(output["pending_approvals"], indent=2))
    elif settled_non_success:
        # Never print the preserved older report here: it did not come from the
        # turn that failed or was interrupted.
        print(f"{output['short']}  {state} (last turn {last_turn['status']})")
    elif report and include_report:
        print(report.get("text", ""))
    else:
        print(f"{output['short']}  {state}")
    if result.get("settle"):
        verdict = result.get("settlementVerdict")
        if verdict == "settled":
            return
        if verdict == "approval_waiting":
            raise SystemExit(EXIT_WAITING)
        if verdict in {"crashed", "dead"}:
            raise SystemExit(EXIT_UNAVAILABLE)
        raise SystemExit(EXIT_TURN_INCOMPLETE)
    if timed_out:
        raise SystemExit(EXIT_TURN_INCOMPLETE)
    if orphaned:
        raise SystemExit(EXIT_UNAVAILABLE)
    if state == "waiting":
        raise SystemExit(EXIT_WAITING)
    if state in {"crashed", "dead"}:
        raise SystemExit(EXIT_UNAVAILABLE)
    if expected_turn_id is None:
        return
    # The waited turn owns this exit status: it must be the settled turn and it
    # must have completed. An unknown native status is never success.
    if not outcome_matches or last_turn.get("status") != "completed":
        raise SystemExit(EXIT_TURN_INCOMPLETE)


def _require_parent_notification():
    try:
        parent = require_notify_parent()
        prepare_notify_parent(parent)
        return parent
    except CcxError as error:
        _fail(f"notify: {error}", error.exit_code)


def _prepare_parent_notification(args):
    settle = bool(getattr(args, "settle", False))
    if settle and not getattr(args, "notify_parent", False):
        _fail("--settle requires --notify-parent")
    if not getattr(args, "notify_parent", False):
        return None
    return _require_parent_notification()


def _emit_parent_notification_armed(
    record: dict[str, Any],
    registration: dict[str, Any],
    args,
    *,
    extra: dict[str, Any] | None = None,
) -> None:
    output = {**_identity(record), **(extra or {})}
    output["notify_parent"] = public_parent_notification(registration)
    _emit(output, args, plain=record["short"] if not getattr(args, "json", False) else None)
    if not getattr(args, "json", False):
        public = output["notify_parent"]
        print(f"notify: armed {public['event_id']} -> Codex parent {public['parent']}")
        print(f"notify: {public['instruction']}")


def cmd_set(args) -> None:
    """Edit exact original Codex future-turn config; rename remains metadata."""
    name_given = args.name is not None
    model_given = args.model is not None
    effort_given = args.effort is not None
    if not (name_given or model_given or effort_given):
        _fail("set: nothing to change (pass --name, --model and/or --effort)")
    try:
        client = CodexClient()
        if model_given or effort_given:
            updates = {}
            if model_given:
                updates["model"] = None if args.model == "default" else args.model
            if effort_given:
                updates["effort"] = None if args.effort == "default" else args.effort
            # one shared core patch for both settings, before a rename;
            # a native busy refusal must not leave a new display name.
            record = configure_worker(
                args.target, expected_generation=client._uuid_store.generation,
                **updates,
            )
            from ccx.codex.types import CodexSessionInfo
            info = CodexSessionInfo.from_record(record)
        else:
            # No daemon config RPC for pure display metadata.
            info = None
        if name_given:
            info = client.set_name(args.target, args.name)
        assert info is not None
    except CcxError as error:
        _fail(str(error), error.exit_code)
    result = {
        "worker": f"codex:{info.worker_id}", "worker_id": info.worker_id,
        "short": info.short, "engine": "codex",
        "action": ("set+rename" if name_given else "set")
                  if (model_given or effort_given) else "rename",
    }
    if name_given:
        result["name"] = info.name
    if model_given:
        result["model"] = info.model
    if effort_given:
        result["effort"] = info.effort
    _emit(result, args, plain=result["worker"])


def cmd_send(args) -> None:
    """Original Codex CLI sends one full-ID native turn through codexd.

    The one globally owned full UUID and immutable delivery ID select the
    original semantic receipt; Codex native threadId remains independently
    bound. No implicit thread/start, no native-busy steering, and no old
    short/name or external source is authorized.
    """
    from ccx import constants
    import time
    from ccx.ops.send import send_turn as full_send
    from ccx.ops.wait import reconcile_turn
    from ccx.ops.report import report_worker
    from ccx.store.root import GenerationError, require_ccx_active
    from ccx.store.identity import IdentityError, EngineMismatch
    from ccx.codex.delivery import DeliveryReceiptError
    from uuid import uuid4
    if getattr(args, "settle", False) and not args.notify_parent:
        _fail("--settle requires --notify-parent")
    if args.timeout is not None and args.timeout <= 0:
        _fail("positive timeout required")
    prompt = _prompt(args, required=True)
    token = args.delivery_id or str(uuid4())
    try:
        store=require_ccx_active(constants.CCX_ROOT)
        owner=store.resolve(args.target)
        if owner.engine != "codex":
            raise EngineMismatch("Send requires a Codex worker handle")
        if args.var is not None or args.vars_from is not None:
            from ccx.cli.commands.turns import _publish_cli_send_variables
            prompt = _publish_cli_send_variables(store, owner, args, prompt, token)
        first=full_send(owner.worker,prompt,delivery_id=token,
                        cap=_caps_from_args(args),
                        mode="new_turn_only", notify_parent=bool(args.notify_parent),
                        settle=bool(getattr(args, "settle", False)))
        # Parser makes --notify-parent / --wait / --no-wait exclusive;
        # the parent gets its durable event, then yields rather than polling.
        should_wait=not (args.no_wait or args.notify_parent)
        started=time.monotonic()
        observation=None
        failure=None
        if should_wait:
            from ccx.codex.settle import send_settle_failure
            while True:
                # native-observe can hold for its entire budget: poll in short
                # slices so a failed turn cannot consume a 120s send timeout.
                observation=reconcile_turn(
                    owner.worker, delivery_id=token,
                    timeout=(0.5 if args.timeout is None else
                             max(0.001, min(0.5, args.timeout-(time.monotonic()-started)))),
                )
                if observation["status"] in ("settled","rejected"):
                    break
                failure=send_settle_failure(owner.worker, observation)
                if failure is not None:
                    break
                if args.timeout is not None and time.monotonic()-started>=args.timeout:
                    break
                time.sleep(.1)
        complete = (first.get("settled") if observation is None
                    else observation["status"]=="settled")
        result={
            "worker":owner.worker, "worker_id":owner.worker_id,
            "engine":"codex","short":owner.short,
            "delivery_id":token,
            "status":(failure["status"] if failure else
                      observation["status"] if observation else first["status"]),
            "settled":bool(complete),
            "delivery":(observation.get("delivery") if observation
                        else first.get("delivery")),
        }
        if failure is not None:
            result["error"]=failure.get("error")
        if args.notify_parent:
            result["notify_parent"]=first.get("notification")
        if complete and not args.notify_parent:
            # Report this delivery's original R, never the worker's latest.
            turn_ref=(result["delivery"] or {}).get("turn_ref")
            if not turn_ref:
                from ccx.errors import SessionUnavailable
                raise SessionUnavailable(
                    "Completed delivery has no turn ID; "
                    "reconcile its delivery ID"
                )
            result["report"]=report_worker(owner.worker,turn_ref=turn_ref)
        if args.json:
            _emit(result,args)
        else:
            print(owner.worker)
            print(f"delivery_id: {token}")
            if args.notify_parent:
                notification = result["notify_parent"] or {}
                print(f"notification: {notification.get('event_id')} ({notification.get('state')})")
            elif complete:
                print(result["report"]["text"])
            else:
                print(f"status: {result['status']}" +
                      (f"; error: {result['error']}" if result.get("error") else ""))
        if result["status"]=="rejected":
            raise SystemExit(EXIT_REJECTED)
        if should_wait and not complete:
            from ccx.cli.commands._common import _nbwait_exit_code
            raise SystemExit(
                (EXIT_UNAVAILABLE if failure["status"] == "unavailable" else
                 _nbwait_exit_code(failure["status"], failure.get("apiError")))
                if failure is not None else EXIT_TURN_INCOMPLETE
            )
    except SendIndeterminate as error:
        _fail_send_indeterminate(error, args)
    except BudgetStopped as error:
        if args.json:
            _emit({
                "worker": owner.worker, "worker_id": owner.worker_id,
                "engine": "codex", "short": owner.short,
                "delivery_id": token, "status": "budget-stopped",
                "code": "budget_stopped", "reason": error.reason or str(error),
                "settled": False,
            }, args)
            raise SystemExit(error.exit_code) from None
        _fail("ccx send: " + str(error), error.exit_code)
    except CcxError as error:
        # Definite busy refusal is exit 5; immutable input/parent conflicts
        # remain typed, and no uncertain native write is reported as unavailable.
        _fail("ccx send: "+str(error), error.exit_code)
    except (GenerationError, IdentityError, DeliveryReceiptError) as error:
        _fail("ccx send: native delivery unavailable: "+str(error),3)


def cmd_reconcile(args) -> None:
    """Read-only reconciliation of one original Codex delivery or turn receipt.

    The exact owned full UUID's semantic receipt is the only authority; a
    native turn CCX holds no receipt for is not_found, never accepted.
    """
    from ccx import constants
    from ccx.codex.delivery import DeliveryReceiptError
    from ccx.codex.ops import _semantic_status
    from ccx.ops.wait import reconcile_turn
    from ccx.store.root import GenerationError, require_ccx_active
    from ccx.store.identity import EngineMismatch, IdentityError
    try:
        store = require_ccx_active(constants.CCX_ROOT)
        owner = store.resolve(args.target)
        if owner.engine != "codex":
            raise EngineMismatch("Reconcile requires a Codex worker handle")
        result = reconcile_turn(
            owner.worker,
            delivery_id=getattr(args, "delivery_id", None),
            turn_id=getattr(args, "turn_id", None),
        )
        record = store.record(owner.worker)
    except CcxError as error:
        _fail(str(error), error.exit_code)
    except (GenerationError, IdentityError, DeliveryReceiptError) as error:
        _fail("ccx reconcile unavailable: " + str(error), 3)

    # Project from the semantic receipt, not the shared raw status: a prepared
    # receipt whose send effect advanced recommends "wait" (pending, never
    # retryable), and accepted stays delivered (exit 0) whether settled or not.
    delivery = result.get("delivery")
    semantic = _semantic_status(delivery)
    status, pending = caller_status({
        "status": semantic, "pending": semantic == "pending", "delivery": delivery,
    })
    output = {
        **_identity(record), "worker": owner.worker, "worker_id": owner.worker_id,
        "engine": "codex", "thread_id": (record.get("native") or {}).get("threadId"),
        "reconcile_status": status,
        "pending": pending,
        "retryable": status == "not_delivered",
        "delivery": delivery,
    }
    turn = (delivery or {}).get("turn_id") or getattr(args, "turn_id", None) or "-"
    delivery_id = (delivery or {}).get("delivery_id") or getattr(args, "delivery_id", None) or "-"
    _emit(output, args, plain=(
        f"{owner.short}  {status}  delivery_id: {delivery_id}  turn: {turn}"
    ))

    if status == "not_found":
        raise SystemExit(EXIT_NOT_FOUND)
    # Definitively not delivered shares the refused send's exit code.
    if status in {"rejected", "not_delivered"}:
        raise SystemExit(EXIT_REJECTED)
    if pending:
        raise SystemExit(EXIT_TURN_INCOMPLETE)


def _send_rejection_guidance(short: str, reply: dict[str, Any]) -> str:
    """A refused strict send, named with the verb the caller actually wanted."""
    error = str(reply.get("error") or "send rejected")
    state = reply.get("state")
    if state == "waiting":
        return (f"{error}; the worker is blocked on an approval — resolve it "
                f"with `ccx approve {short}` or `ccx reject {short}`")
    if state == "busy":
        return (f"{error}; `send` starts a new turn on an idle worker. To add "
                f"input to the turn already running, use "
                f"`ccx steer {short} \"...\"`")
    return error


def cmd_notify(args) -> None:
    """Arm an independent read-only observation of one original Codex turn."""
    from ccx import constants
    from ccx.errors import CcxError, InvalidRequest, SessionNotFound, SessionUnavailable
    from ccx.controllers.orchestrator import require_notify_parent
    from ccx.notify.registry import create_registration, public_registration
    from ccx.notify.daemon import ensure_notify_daemon
    from ccx.notify.select import select_current_turn
    from ccx.store.root import GenerationError, require_ccx_active
    from ccx.store.identity import IdentityError, UnknownWorker

    try:
        try:
            store = require_ccx_active(constants.CCX_ROOT)
            owner = store.resolve(args.target)
            if owner.engine != "codex":
                raise InvalidRequest("Notification requires a Codex worker handle")
            parent = require_notify_parent()
            selection = select_current_turn(owner.worker)
            reg = create_registration(
                parent=parent, worker_engine="codex", worker_id=owner.worker_id,
                worker_name=selection["name"], operation=selection["operation"],
                source="notify", settle=bool(getattr(args, "settle", False)),
            )
        except UnknownWorker as exc:
            raise SessionNotFound(f"ccx notify: no such managed worker {args.target}") from exc
        except IdentityError as exc:
            raise InvalidRequest(f"Invalid worker handle for notification: {exc}") from exc
        except (GenerationError, OSError) as exc:
            # Selection or publication failed; never reported as an armed event.
            raise SessionUnavailable(f"Cannot select a Codex turn to notify about; check status: {exc}") from exc
        available = True
        try:
            ensure_notify_daemon()
        except (CcxError, GenerationError, OSError) as exc:
            available = False
            print(f"ccx notify: event durably armed; supervisor unavailable: {exc}",
                  file=sys.stderr)
        result = {**public_registration(reg), "armed": True,
                  "selected_state": selection["state"],
                  "notifier_available": available}
    except CcxError as exc:
        _fail(str(exc), exc.exit_code)
    _emit(result, args, plain=f"{result['worker']} event_id={result['event_id']}")



def cmd_steer(args) -> None:
    """One original full-UUID Codex native steer, accepted placement only."""
    from uuid import uuid4

    text = _prompt(args, required=True)
    token = getattr(args, "delivery_id", None)
    if token is None:
        token = str(uuid4())
    try:
        accepted = CodexClient().steer(
            args.target, text, delivery_id=token,
            caps=_caps_from_args(args),
        )
    except SendIndeterminate as error:
        _fail_send_indeterminate(error, args)
    except DeliveryRejected as error:
        if getattr(error, "delivery_status", None) is None:
            _fail(f"steer: {error}; delivery_id={token}; nothing recorded; "
                  "this ID may be used when a turn is running", error.exit_code)
        _fail(f"steer: {error}; delivery_id={token}; "
              "this rejected delivery is terminal; choose a new ID for a "
              "new instruction", error.exit_code)
    except CcxError as error:
        _fail(f"steer: {error}; delivery_id={token}", error.exit_code)
    value = accepted.as_dict()
    _emit(value, args, plain=f"{value['worker']}  steer accepted-current-turn  "
                             f"delivery_id={accepted.delivery_id}")


def cmd_stop(args) -> None:
    """Request only this full-UUID Codex worker's active native turn interrupt."""
    from ccx.ops.cancel import stop_worker
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
        _fail(f"stop: {error}", error.exit_code)
    value = {
        **result, "ok": True, "short": result["worker"]["short"],
        "requested": True,
        "cancelled": result["confirmed"] and result["outcome"] == "interrupted",
        "interrupted": result["confirmed"] and result["outcome"] == "interrupted",
        "turn_id": result["turn_ref"].rsplit(":", 1)[-1],
    }
    _emit(value, args, plain=result["worker"]["worker"])

def cmd_kill(args) -> None:
    """Unload only the exact full-UUID Codex worker, never the shared daemon."""
    from ccx.ops.wait import park_worker
    try:
        result = park_worker(args.target, timeout=getattr(args, "timeout", 90))
    except CcxError as error:
        _fail(f"kill: {error}", error.exit_code)
    _emit(result, args, plain=result["worker"])

def cmd_wait(args) -> None:
    from ccx.cli.commands.turns import _emit_full_uuid_wait
    from ccx.ops.wait import wait_worker
    try:
        if getattr(args, "settle", False):
            verdict = wait_for_settlement(
                args.target, timeout=getattr(args, "timeout", None),
                wake_grace_s=getattr(args, "wake_grace", DEFAULT_WAKE_GRACE_S))
        else:
            verdict = wait_worker(args.target, timeout=getattr(args, "timeout", None))
    except CcxError as error:
        _fail(f"wait: {error}", error.exit_code)
    if verdict.get("settle"):
        _emit_wait(verdict, args)
    else:
        _emit_full_uuid_wait(args, verdict)

def _decision(args, operation: str) -> None:
    # Freeze one current CCX gateId before parent preparation or native effect.
    # Supplied IDs pass through unchanged; the backend fences the original gate.
    if getattr(args, "settle", False) and not getattr(args, "notify_parent", False):
        _fail("--settle requires --notify-parent")
    try:
        request_id = args.request_id
        record = None
        if request_id is None:
            record = _record(args.target)
            gates = [gate for gate in record.get("pendingApprovals") or []
                     if isinstance(gate, dict)]
            if not gates:
                raise DeliveryRejected(f"Codex {operation}: no pending approval; check ccx peek.")
            if len(gates) != 1:
                raise InvalidRequest(
                    "Several approvals are pending; specify --request-id from ccx peek."
                )
            request_id = gates[0].get("gateId")
            if not isinstance(request_id, str) or not request_id:
                raise DeliveryRejected("Cannot identify a pending approval; check ccx peek.")
        _prepare_parent_notification(args)
        if getattr(args, "notify_parent", False):
            from ccx import constants
            from ccx.ops import common as protocol_ops
            from ccx.store.root import require_ccx_active
            if record is None:
                record = _record(args.target)
            store = require_ccx_active(constants.CCX_ROOT)
            owner = store.resolve(f"codex:{record['workerId']}")
            # Deterministic gate response token, never the native requestId.
            token = str(uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"ccx-input-decision:{store.generation}:{owner.worker_id}:{request_id}",
            ))
            result = ops_input.decide_input(
                owner.worker, request_id=request_id, response_id=token,
                decision=operation, notify_parent=True,
                settle=bool(getattr(args, "settle", False)),
            )
            public = {
                "worker": owner.worker, "worker_id": owner.worker_id,
                "request_id": request_id, "response_id": token,
                "decision": "accept" if operation == "approve" else "decline",
                "status": result["status"], "owned": result["owned"],
                "recommended_action": result["recommended_action"],
                "decision_sent": result["status"] == "accepted",
                "notify_parent": result["notification"],
            }
            _emit(public, args, plain=f"{operation} {result['status']} for Codex request "
                                      f"{request_id}")
            if result["status"] in {"submitted", "unknown"}:
                raise SystemExit(EXIT_TURN_INCOMPLETE)
            if result["status"] == "rejected":
                raise SystemExit(EXIT_REJECTED)
            return
        client = CodexClient()
        native = (client.approve if operation == "approve" else client.reject)(
            args.target, request_id=request_id)
    except CcxError as error:
        _fail(str(error), error.exit_code)
    result = native.as_dict()
    result["decision_sent"] = True  # Native gate response; no model completion.
    _emit(result, args, plain=f"{operation} accepted for Codex request {request_id}")

def cmd_approve(args) -> None:
    _decision(args, "approve")


def cmd_reject(args) -> None:
    _decision(args, "reject")


def cmd_compact(args) -> None:
    """Compact the exact owned Codex thread through the shared operation core."""
    from ccx.codex.ops import compact_worker
    from ccx.constants import CCX_ROOT
    from ccx.errors import (
        CompactFailed, CompactIndeterminate,
    )
    from ccx.store.root import GenerationError, NativeEffectIndeterminate, require_ccx_active
    from ccx.store.identity import EngineMismatch, IdentityError, UnknownWorker

    operation_id = str(uuid.uuid4())  # Internal; Codex CLI has no ID flag.
    if getattr(args, "notify_parent", False) or getattr(args, "settle", False):
        from ccx.notify.registry import compact_notification_failure, create_registration, public_registration
        notification = None
        try:
            if not getattr(args, "notify_parent", False):
                raise InvalidRequest("--settle requires --notify-parent")
            store = require_ccx_active(CCX_ROOT)
            owner = store.resolve(args.target)
            if owner.engine != "codex":
                raise InvalidRequest("Codex compact notification needs a Codex worker")
            record = store.record(owner.worker)
            parent = require_notify_parent()
            prepare_notify_parent(parent)
            notification = create_registration(
                parent=parent, worker_engine="codex", worker_id=owner.worker_id,
                worker_name=record.get("name"),
                operation={"kind": "codex-compact", "operationId": operation_id,
                           "nativeSourceId": (record.get("native") or {}).get("threadId")},
                source="compact", settle=bool(getattr(args, "settle", False)),
            )
            # One native call after the durable parent event.
            result = compact_worker(
                owner.worker, operation_id=operation_id, wait=False,
                timeout=args.timeout)
            if result["status"] in {"prepared", "rejected", "not_found"}:
                raise DeliveryRejected(
                    result.get("reason") or "Codex did not start compaction; check worker status",
                    refusal="native-rejected", operation_id=operation_id)
            if result["status"] == "failed":
                raise CompactFailed(result.get("reason") or
                                    "Codex compaction failed; check worker status",
                                    operation_id=operation_id)
            if (result["status"] == "unknown"
                    or (result["status"] == "submitted"
                        and result.get("receipt_status") != "accepted")):
                raise CompactIndeterminate(
                    "Codex compaction outcome is unknown; "
                    "keep the notification and operation ID to check later",
                    operation_id=operation_id, operation_status=result["status"])
        except (CcxError, GenerationError, NativeEffectIndeterminate,
                IdentityError, EngineMismatch, UnknownWorker, OSError) as error:
            if notification is not None:
                try:
                    uncertain, state = compact_notification_failure(notification, str(error))
                except (ValueError, OSError) as failure:
                    uncertain, state = True, f"unreadable ({failure})"
                code = EXIT_TURN_INCOMPLETE if uncertain else getattr(
                    error, "exit_code", EXIT_UNAVAILABLE)
                _fail(f"{error}; operation_id={operation_id}; "
                      f"event_id={notification['eventId']}; notification_state={state}"
                      + ("; effect uncertain, no resubmission" if uncertain
                         else "; no native compact was submitted"), code)
            _fail(f"{error}; operation_id={operation_id}",
                  getattr(error, "exit_code", EXIT_UNAVAILABLE))
        record = store.record(owner.worker)
        status = result["status"]
        proven = status == "succeeded" and result.get("compacted") is True
        output = {
            **_identity(record), "worker": owner.worker, "worker_id": owner.worker_id,
            "engine": "codex", "thread_id": (record.get("native") or {}).get("threadId"),
            "operation_id": operation_id, "operation_status": status,
            "accepted": result.get("receipt_status") == "accepted" or proven,
            "compacted": proven, "settled": False,
            "compact_turn_id": result.get("compactTurnId"),
            "context_item_id": result.get("contextItemId"),
            "native_turn_status": result.get("nativeTurnStatus"),
            "context_before": result.get("contextBefore"),
            "context_after": result.get("contextAfter") if proven else None,
            "notify_parent": public_registration(notification),
        }
        _emit(output, args, plain=(f"Codex compact {status} (parent not yet notified): "
                                  f"{owner.worker}; operation_id={operation_id}; "
                                  f"event_id={notification['eventId']}"))
        return
    try:
        result = compact_worker(
            args.target, operation_id=operation_id,
            wait=not args.no_wait, timeout=args.timeout,
        )
        status = result["status"]
        if status == "failed":
            raise CompactFailed(
                result.get("reason") or "Codex compaction failed; check worker status",
                operation_id=operation_id,
            )
        if status == "rejected":
            raise DeliveryRejected(
                result.get("reason") or "Codex refused compaction; check worker status",
                refusal="native-rejected", operation_id=operation_id,
            )
        if not args.no_wait and status != "succeeded":
            raise CompactIndeterminate(
                f"Codex compaction outcome is unknown; inspect its operation ({status})",
                operation_id=operation_id, operation_status=status,
            )
        store = require_ccx_active(CCX_ROOT)
        owner = store.resolve(args.target)
        record = store.record(owner.worker)
        identity = _identity(record)
        native = record.get("native") or {}
        proven = status == "succeeded" and result.get("compacted") is True
        output = {
            **identity, "worker": owner.worker, "worker_id": owner.worker_id,
            "engine": "codex", "thread_id": native.get("threadId"),
            "operation_id": operation_id, "operation_status": status,
            "accepted": result.get("receipt_status") == "accepted" or proven,
            "compacted": proven,
            "context_before": result.get("contextBefore"),
            "context_after": result.get("contextAfter") if proven else None,
            "compact_turn_id": result.get("compactTurnId"),
            "context_item_id": result.get("contextItemId"),
            "native_turn_status": result.get("nativeTurnStatus"),
            "reason": result.get("reason"),
        }
    except NativeEffectIndeterminate as error:
        error = CompactIndeterminate(
            f"Codex native compact admission unknown: {error}",
            operation_id=operation_id, operation_status="unknown",
        )
        _fail(f"{error}; operation_id={operation_id}", error.exit_code)
    except UnknownWorker as error:
        _fail(f"{error}; operation_id={operation_id}", EXIT_NOT_FOUND)
    except (IdentityError, EngineMismatch) as error:
        _fail(f"{error}; operation_id={operation_id}", EXIT_ERROR)
    except GenerationError as error:
        _fail(f"Cannot compact Codex; inspect its status: {error}; "
              f"operation_id={operation_id}", EXIT_UNAVAILABLE)
    except CcxError as error:
        _fail(f"{error}; operation_id={operation_id}", error.exit_code)

    if output["compacted"]:
        plain = f"Codex compact succeeded: {output['worker']}; operation_id={operation_id}"
    else:
        plain = (f"Codex compact {status} (not proven complete): "
                 f"{output['worker']}; operation_id={operation_id}")
    _emit(output, args, plain=plain)


def _reset_time(value: Any) -> str:
    from ccx.cli.commands._common import local_reset_time
    return local_reset_time(value) if isinstance(value, (int, float)) else ""


def _quota_rows(data: dict[str, Any]) -> list[tuple[str, int, str]]:
    rows: list[tuple[str, int, str]] = []
    for limit_id, window in sorted(
        rate_limit_windows(data),
        key=lambda item: (item[0].casefold() != "codex", item[0].casefold()),
    ):
        by_id = data.get("rateLimitsByLimitId") or {}
        bucket = (by_id.get(limit_id) if isinstance(by_id, dict) and by_id
                  else data.get("rateLimits"))
        bucket_name = (bucket.get("limitName") if isinstance(bucket, dict) else None) or limit_id
        suffix = (
            ""
            if not bucket_name or str(bucket_name).casefold() == "codex"
            else f" {bucket_name}"
        )
        used = window.get("usedPercent")
        if type(used) not in (int, float):
            continue
        rows.append((
            window_label(window) + suffix,
            int(used),
            _reset_time(window.get("resetsAt")),
        ))
    return rows


def cmd_usage(args) -> None:
    """Print live Codex account quota utilization or token activity."""
    tokens = getattr(args, "tokens", False)
    try:
        data = runtime.read_account_usage(tokens=tokens)
    except CcxError as error:
        if args.json and isinstance(error, EngineCliNotFound):
            print(json.dumps({"ok": False, "code": "runtime_unavailable",
                              "message": str(error)}, sort_keys=True))
            raise SystemExit(error.exit_code) from error
        _fail(str(error), error.exit_code)
    if args.json:
        print(json.dumps(data, indent=2))
        return
    if tokens:
        summary = data.get("summary") or {}
        print("usage (tokens):")
        fields = (
            ("lifetime", "lifetimeTokens"),
            ("peak/day", "peakDailyTokens"),
            ("streak", "currentStreakDays"),
        )
        emitted = False
        for label, field in fields:
            value = summary.get(field)
            if isinstance(value, int):
                print(f"  {label:<10} {value:>12,}")
                emitted = True
        if not emitted:
            print("  unavailable")
        return
    print("usage (live):")
    rows = _quota_rows(data)
    if not rows:
        print("  unavailable")
        return
    for label, used, resets in rows:
        reset_text = f"   resets {resets}" if resets else ""
        print(f"  {label:<10} {used:>5.1f}%{reset_text}")
