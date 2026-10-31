from __future__ import annotations
import json
import os
import pathlib
import sys
import time
from ccx import constants
from ccx.constants import (
    EXIT_ERROR,
    EXIT_UNAVAILABLE,
    USAGE_STALE_LIMIT_S,
)
from ccx.claude.external import import_eligibility
from ccx.errors import (
    CcxError,
    EngineCliNotFound,
    InvalidRequest,
    SessionUnavailable,
    SessionWaiting,
)
from ccx.ops.semantics import (
    payload_guidance as _payload_guidance,  # noqa: F401
    pending_input_kind,
)
from ccx.claude.usage import (
    _caps_from_args,
    cap_tripped,
    fetch_usage,
    unknown_cap_metrics,
)
from ccx.cli.commands._common import _prepare_parent_notification, _resolve_prompt_source, emit_waiting


def _initial_caps(args, prompt: str | None, *, engine: str = "claude") -> dict | None:
    """Per-turn caps for a create's first prompt, refused if already tripped."""
    from ccx.errors import BudgetStopped
    caps = _caps_from_args(args)
    if caps:
        from ccx.workers import _check_caps
        _check_caps(caps, engine=engine)
        if not prompt:
            raise InvalidRequest("initial cap requires a prompt")
        if engine == "codex":
            # codexd checks the fresh cached window immediately before turn/start.
            return caps
        usage = fetch_usage(stale_limit=USAGE_STALE_LIMIT_S)
        missing = unknown_cap_metrics(usage, caps)
        if missing:
            sys.stderr.write(f"[ccx] usage cap preflight skipped unknown/stale metrics: {', '.join(missing)}\n")
        tripped = cap_tripped(usage, caps)
        if tripped:
            key, current, cap = tripped
            raise BudgetStopped(
                f"usage cap {key} already exceeded ({current:.0f}% >= {cap:g}%); no worker created",
                reason=f"{key} {current:.0f}% >= {cap:g}%", phase="preflight",
            )
    return caps


def _emit_create_partial(op: str, error, args) -> None:
    if args.json:
        print(json.dumps({
            "worker": error.worker, "worker_id": error.worker_id,
            "short": error.short, "phase": error.phase,
            "delivery_id": error.delivery_id,
            "delivery_status": error.delivery_status,
            "partial": True, "error": str(error),
            **({"waiting_for": error.cause.waiting_for,
                "request_id": (error.cause.waiting_for or {}).get("id"),
                "pending_kind": pending_input_kind(error.cause.waiting_for)}
               if isinstance(error.cause, SessionWaiting) else {}),
        }, indent=2))
    elif isinstance(error.cause, SessionWaiting):
        # The first turn is parked on a gate: say which and how to clear it.
        emit_waiting(error.worker_id, header=f"Short: {error.short}  worker_id={error.worker_id}")
    else:
        print(f"Short: {error.short}  worker_id={error.worker_id}")
    cause = f": {error.cause}" if error.cause is not None and str(error.cause) else ""
    sys.stderr.write(
        f"[ccx] {op}: {error} ({error.phase}{cause}); existing worker={error.worker}; "
        f"inspect it with `ccx peek {error.short}`\n")
    raise SystemExit(error.exit_code) from error


def cmd_new(args):
    return _prepare_full_uuid_new(args, engine=args.engine)


def cmd_resume(args):
    """Restore the one original native worker without sending a new turn."""
    from ccx.workers import WorkerClient
    try:
        result = WorkerClient().resume(args.target)
    except CcxError as error:
        print(f"ccx resume: {error}", file=sys.stderr)
        raise SystemExit(error.exit_code) from error
    print(json.dumps(result, ensure_ascii=False, sort_keys=True)
          if args.json else result["worker"])


def cmd_fork(args):
    """Original full-UUID fork and optional first turn through creation."""
    from math import isfinite
    from ccx.errors import CreatePartialFailure, OperationConflict
    from ccx.ops.forking import fork
    from ccx.store.root import GenerationError
    from ccx.store.identity import IdentityError, UnknownWorker

    parent = _prepare_parent_notification(args)
    prompt = _resolve_prompt_source(args, "prompt", "fork", required=False)
    if args.timeout is not None and not (isfinite(args.timeout) and args.timeout > 0):
        print("ccx fork: --timeout must be positive finite seconds; "
              "no child identity claimed", file=sys.stderr)
        raise SystemExit(1)

    try:
        from ccx.controllers import live_vars
        if args.var is not None and args.vars_from is not None:
            raise InvalidRequest("--var and --vars-from are mutually exclusive")
        attachment = None
        if args.vars_from is not None:
            from ccx.store.discovery import resolve_worker
            source_engine, source_id = resolve_worker(args.vars_from).split(":", 1)
            attachment = live_vars.prepare_shared_durable_attachment(source_engine, source_id)
        elif args.var is not None:
            attachment = live_vars.prepare_durable_attachment(
                live_vars.parse_cli_vars(args.var)
            )
        caps = _initial_caps(args, prompt, engine=(
            "codex" if args.target.startswith("codex:") else "claude"))
        deadline = time.monotonic() + args.timeout if args.timeout is not None else None
        result = fork(
            args.target, worker_id=args.worker_id, name=args.name,
            model=args.model, effort=args.effort, created_via="cli",
            prompt=prompt, caps=caps, wait=args.wait,
            attachment=attachment, delivery_id=args.delivery_id,
            parent=parent, settle=bool(args.settle), deadline=deadline,
        )
    except CreatePartialFailure as error:
        output = {
            "ok": False, "code": "partial_failure", "phase": error.phase,
            "worker": error.worker, "worker_id": error.worker_id,
            "engine": error.engine, "short": error.short,
            "created": error.info.get("created"),
            "reconciled": error.info.get("reconciled"),
            "native_ready": error.info.get("native_ready") is True,
            "activation_state": error.info.get("activation_state"),
            "native_start_error": error.info.get("native_start_error"),
            "cause_type": type(error.cause).__name__ if error.cause else None,
            "integrity_error": bool(error.info.get("integrity_error")),
            "message": str(error),
            "delivery_id": error.delivery_id,
            "delivery_status": error.delivery_status,
            **({"waiting_for": error.cause.waiting_for,
                "request_id": (error.cause.waiting_for or {}).get("id"),
                "pending_kind": pending_input_kind(error.cause.waiting_for)}
               if isinstance(error.cause, SessionWaiting) else {}),
        }
        refusal = error.info.get("native_rejection")
        if isinstance(refusal, dict):
            output.update(
                code="native_rejected", activation_state="rejected",
                message=refusal["message"], native_start_error=refusal["message"],
                native_rejection=dict(refusal),
            )
        local = error.info.get("native_local_refusal")
        if isinstance(local, dict):
            if local.get("proven") is True and output["activation_state"] == "local-refused":
                output.update(
                    code="local_refused", message=local["reason"],
                    native_start_error=local["reason"],
                )
            output["native_local_refusal"] = dict(local)
        if error.info.get("initial_delivery_conflict") is True:
            output["code"] = "conflict"
            error.exit_code = OperationConflict.exit_code
        elif error.info.get("initial_delivery_rejected") is True:
            output["code"] = "delivery_rejected"
        if args.json:
            print(json.dumps(output, sort_keys=True, ensure_ascii=False))
        else:
            cause = f": {error.cause}" if error.cause is not None and str(error.cause) else ""
            print(
                f"ccx fork: {error.worker} identity claimed; phase {error.phase}{cause} "
                f"remains unavailable. Retain exact --worker-id {error.worker_id}, "
                f"retry with the same full source {args.target}; never remint",
                file=sys.stderr,
            )
        raise SystemExit(error.exit_code) from error
    except CcxError as error:
        if args.json and isinstance(error, EngineCliNotFound):
            print(json.dumps({"ok": False, "code": "runtime_unavailable",
                              "message": str(error)}, sort_keys=True))
        else:
            print(f"ccx fork rejected: {error}", file=sys.stderr)
        raise SystemExit(error.exit_code) from error
    except IdentityError as error:
        print(f"ccx fork: invalid or unowned full UUID: {error}", file=sys.stderr)
        raise SystemExit(2 if isinstance(error, UnknownWorker) else 1) from error
    except (GenerationError, OSError) as error:
        print(f"ccx fork unavailable: {error}", file=sys.stderr)
        raise SystemExit(3) from error

    if (result.get("native_ready") is not True
            or result.get("activation_state") != "confirmed"
            or not isinstance(result.get("forked_from"), dict)):
        print(
            f"ccx fork: native activation unproven for {result['worker']}; "
            f"retain exact --worker-id {result['worker_id']}; never remint",
            file=sys.stderr,
        )
        raise SystemExit(3)
    if args.json:
        public = {key: result[key] for key in (
            "worker", "worker_id", "engine", "short", "native_source",
            "native_ready", "created", "reconciled",
        )}
        public.update(
            ok=True, name=result["record"].get("name"),
            cwd=result["record"].get("cwd"),
            forked_from=result["forked_from"],
        )
        if prompt is not None:
            public.update(delivery_id=result["delivery_id"],
                          delivery_status=result["delivery_status"],
                          delivery=result["delivery"])
        if args.wait:
            public.update(waited=True, turn_ref=result["turn_ref"],
                          report=result["report"])
        if parent is not None and result.get("notification"):
            public["notify_parent"] = result["notification"]
        print(json.dumps(public, ensure_ascii=False, sort_keys=True))
    else:
        if args.wait:
            print(f"worker: {result['worker']}")
            print(result["report"]["text"])
        else:
            print(result["worker"])
        if parent is not None and result.get("notification"):
            notice = result["notification"]
            print(f"notify: armed {notice['event_id']} -> Codex parent {notice['parent']}")
            print(f"notify: {notice['instruction']}")


def cmd_import(args):
    """Adopt one exact Claude native SID, not an unmanaged CCX record/alias."""
    from uuid import UUID
    from ccx.claude.external import exact_import_candidate
    from ccx.store.root import require_ccx_active
    from ccx.store.identity import mint_worker_id
    from ccx.ops.creation import create as create_worker
    from ccx.errors import CreatePartialFailure, OperationConflict

    sid = args.session
    try:
        if not isinstance(sid, str) or str(UUID(sid)) != sid:
            raise InvalidRequest("Import needs a complete Claude session ID; copy it from Claude")
        store = require_ccx_active(constants.CCX_ROOT)

        def previous_owner():
            owned = store.import_native_owner(sid)
            if owned is None:
                return None
            if owned["provenance"] == "ccx-import" and owned["confirmed"]:
                print(owned["worker"])
                sys.stderr.write("[ccx] import: already managed; using original "
                                 + owned["worker"] + " (no new resume)\n")
                return owned["worker"]
            if owned["provenance"] == "ccx-import":
                raise SessionUnavailable("Claude import is still "
                                         + owned["activation_state"] + "; retain "
                                         + owned["worker"] + ", never resume twice")
            raise OperationConflict("Claude session is already managed by "
                                    + owned["worker"], worker_id=owned["worker_id"])

        if previous_owner():
            return
        source = exact_import_candidate(sid)
        eligible, _code, reason = import_eligibility(source)
        if not eligible:
            raise SessionUnavailable(reason)
        transcript = source["transcript"]
        st = os.stat(transcript)
        original = {
            "sessionId": sid, "cwd": source["cwd"],
            "transcriptJsonl": transcript,
            "importedAt": int(time.time() * 1000),
            "preImportTranscriptSize": st.st_size,
            "preImportTranscriptMtime": int(st.st_mtime * 1000),
            "wasLive": bool(source["live"]),
        }
        try:
            result = create_worker(
                engine="claude", prompt=None, cwd=source["cwd"],
                worker_id=mint_worker_id(), name=source.get("name"),
                permissions="full", created_via="import",
                import_session_id=sid, imported_from=original,
                start_timeout=15,
            )
        except OperationConflict:
            # Another caller may have won the same native-SID lock after our
            # first read; reconcile its original owner, never mint a fallback.
            if previous_owner():
                return
            raise
        print(result["worker"])
        sys.stderr.write(
            "[ccx] imported " + result["worker"] + ": original native Claude "
            "history resumed under ccx hooks (--dangerously-skip-permissions).\n"
            "[ccx] Do NOT continue using the old Claude terminal for this session.\n"
        )
    except CreatePartialFailure as error:
        # Identity is already durable. Retain/print the same full UUID even
        # when the first resume or genuine SessionStart was inconclusive.
        print(error.worker)
        underlying = (error.info.get("native_start_error") or
                      str(error.cause or ""))
        sys.stderr.write(f"[ccx] import: {error} ({error.phase}); "
                         f"worker={error.worker}; {underlying}; "
                         "do not retry a native resume\n")
        raise SystemExit(error.exit_code) from error
    except CcxError as error:
        sys.stderr.write(f"[ccx] import: {error}\n")
        raise SystemExit(error.exit_code) from error
    except (OSError, ValueError) as error:
        sys.stderr.write(f"[ccx] import: source or generation unavailable: {error}\n")
        raise SystemExit(EXIT_UNAVAILABLE) from error


def cmd_init(args) -> None:
    """Explicit fresh install on an empty directory or a missing root leaf."""
    from ccx.store.root import GenerationError, create_missing_root_leaf, initialize_empty_root, root_status, root_status_message
    original_env_root = constants._ORIGINAL_CCX_ROOT_ENV
    if args.root is not None:
        if not pathlib.Path(args.root).expanduser().is_absolute():
            print("ccx init: --root must be absolute", file=sys.stderr)
            raise SystemExit(1)
        root = constants.select_root(args.root)
    else:
        root = constants.CCX_ROOT
    try:
        status = root_status(root)
        if status.kind == "initialized":
            print(f"ccx init: CCX is already initialized in {root}", file=sys.stderr)
            raise SystemExit(1)
        if status.kind == "unsafe":
            raise GenerationError(root_status_message(status))
        # First run: hold and verify the existing parent, create only its leaf;
        # a concurrent replacement is caught by initialize_empty_root's fd.
        expected_inode = create_missing_root_leaf(root) if status.kind == "missing" else None
        store = initialize_empty_root(root, _expected_inode=expected_inode)
    except (OSError, GenerationError) as exc:
        print(f"ccx init refused: {exc}", file=sys.stderr)
        raise SystemExit(3) from None
    result = {
        "layout": 2,
        "root": str(store.root),
        "generation": store.generation,
        "phase": "ACTIVE",
        "workerCount": 0,
        "newEmptyInstall": True,
    }
    print(json.dumps(result, sort_keys=True) if args.json else
          f"initialized empty CCX generation {store.generation} in {store.root}")
    if original_env_root and args.root is not None:
        from ccx.store.root import canonical_root
        previous = canonical_root(original_env_root)
        if previous != store.root:
            print(
                f"ccx init: initialized {store.root}; later commands will use "
                f"CCX_ROOT={previous} unless you change or unset CCX_ROOT",
                file=sys.stderr,
            )


def _prepare_full_uuid_new(args, *, engine: str) -> None:
    """CLI preclaim intent and presentation; native creation belongs to creation.create."""
    from math import isfinite
    from ccx.ops.creation import create
    from ccx.ops.creation_steps import validated_create_spec
    from ccx.errors import BudgetStopped, CcxError, CreatePartialFailure, InvalidRequest, OperationConflict
    from ccx.store.root import GenerationError
    from ccx.store.identity import IdentityError, canonical_worker_id

    notify_parent = bool(getattr(args, "notify_parent", False))
    settle = bool(getattr(args, "settle", False))
    permissions = getattr(args, "permissions", None)
    sandbox = getattr(args, "sandbox", None)
    approval_policy = getattr(args, "approval_policy", None)
    start_timeout = getattr(args, "start_timeout", None)
    try:
        if engine == "claude":
            if start_timeout is not None:
                raise InvalidRequest("--start-timeout is Codex-only")
            if settle:
                raise InvalidRequest("--settle is Codex-only")
            if args.permission_mode is not None and permissions != "restricted":
                raise InvalidRequest(
                    "--permission-mode requires explicit --permissions restricted"
                )
        else:
            if settle and not notify_parent:
                raise InvalidRequest("--settle requires --notify-parent")
            from ccx.workers import _check_caps
            if _caps_from_args(args):
                _check_caps(_caps_from_args(args), engine="codex")
            if start_timeout is not None and not (
                isfinite(start_timeout) and 0 < start_timeout <= 90
            ):
                raise InvalidRequest("--start-timeout must be finite, positive and at most 90")
        # The existing native-spec validator already knows which provider flags
        # and generic/granular permission combinations are legal. Run it before
        # preparing variables, notifying a parent or claiming the worker UUID.
        validated_create_spec(
            engine=engine, cwd=args.cwd, model=args.model, effort=args.effort,
            idle_timeout=args.idle_timeout, permissions=permissions,
            sandbox=sandbox, approval_policy=approval_policy,
            agent=getattr(args, "agent", None),
            append_system_prompt=getattr(args, "append_system_prompt", None),
            allowed_tools=getattr(args, "allowed_tools", None),
            disallowed_tools=getattr(args, "disallowed_tools", None),
            add_dir=getattr(args, "add_dir", None),
            permission_mode=getattr(args, "permission_mode", None),
        )
        if args.worker_id is not None:
            canonical_worker_id(args.worker_id)
        if args.var is not None and args.vars_from is not None:
            raise InvalidRequest("--var and --vars-from are mutually exclusive")
    except (InvalidRequest, IdentityError) as error:
        print(f"ccx new: {error}; no worker identity claimed", file=sys.stderr)
        raise SystemExit(EXIT_ERROR) from error

    prompt = _resolve_prompt_source(args, "prompt", "new", required=False)
    if prompt is not None and (not isinstance(prompt, str) or not prompt.strip()):
        print("ccx new: the initial prompt must not be blank; "
              "no worker identity claimed", file=sys.stderr)
        raise SystemExit(EXIT_ERROR)
    if args.wait and prompt is None:
        print("ccx new: --wait requires an initial prompt; no worker identity claimed",
              file=sys.stderr)
        raise SystemExit(EXIT_ERROR)
    if args.timeout is not None and (not args.wait or not (
        isfinite(args.timeout) and args.timeout > 0
    )):
        print("ccx new: --timeout requires --wait and positive finite seconds; "
              "no worker identity claimed", file=sys.stderr)
        raise SystemExit(EXIT_ERROR)
    invoke = {
        "engine": engine, "cwd": args.cwd, "worker_id": args.worker_id,
        "prompt": prompt,
        "wait": args.wait,
        "name": args.name, "model": args.model, "effort": args.effort,
        "idle_timeout": args.idle_timeout, "timeout": args.timeout,
        "created_via": "cli",
    }
    if engine == "claude":
        invoke.update(
            agent=getattr(args, "agent", None),
            append_system_prompt=getattr(args, "append_system_prompt", None),
            allowed_tools=getattr(args, "allowed_tools", None),
            disallowed_tools=getattr(args, "disallowed_tools", None),
            add_dir=getattr(args, "add_dir", None),
            permission_mode=getattr(args, "permission_mode", None),
            permissions=permissions,
        )
    else:
        invoke.update(permissions=permissions, sandbox=sandbox,
                      approval_policy=approval_policy)
        invoke["settle"] = settle

    try:
        from ccx.controllers import live_vars
        attachment = None
        if args.vars_from is not None:
            from ccx.store.discovery import resolve_worker
            source_engine, source_id = resolve_worker(args.vars_from).split(":", 1)
            attachment = live_vars.prepare_shared_durable_attachment(source_engine, source_id)
        elif args.var is not None:
            attachment = live_vars.prepare_durable_attachment(
                live_vars.parse_cli_vars(args.var)
            )
        invoke["attachment"] = attachment
        if engine == "claude":
            invoke["caps"] = _initial_caps(args, prompt)
            parent = _prepare_parent_notification(args)
        else:
            invoke["caps"] = _initial_caps(args, prompt, engine="codex")
            # Reuse the authenticated Codex parent gate; do not claim first.
            from ccx.cli.codex import _prepare_parent_notification as _prepare_codex_parent_notification
            parent = _prepare_codex_parent_notification(args)
        invoke["parent"] = parent
        result = create(
            **invoke, start_timeout=start_timeout if engine == "codex" else None,
        )
    except CreatePartialFailure as error:
        output = {
            "ok": False, "code": "partial_failure", "phase": error.phase,
            "worker_id": error.worker_id, "worker": error.worker,
            "engine": error.engine, "short": error.short,
            "created": error.info.get("created"),
            "reconciled": error.info.get("reconciled"),
            "native_ready": error.info.get("native_ready") is True,
            "delivery_id": error.delivery_id,
            "delivery_status": error.delivery_status,
            "turn_ref": error.info.get("turn_ref"),
            "message": str(error),
            "native_start_error": error.info.get("native_start_error"),
            "cause_type": type(error.cause).__name__ if error.cause else None,
            "integrity_error": bool(error.info.get("integrity_error")),
            "activation_state": error.info.get("activation_state"),
        }
        if isinstance(error.cause, BudgetStopped):
            # An original worker was claimed, but a usage cap stopped its
            # first turn. Preserve the handle and report the typed outcome.
            reason = error.cause.reason or str(error.cause)
            output.update(code="budget_stopped", status="budget-stopped",
                          reason=reason)
            if args.json:
                print(json.dumps(output, sort_keys=True, ensure_ascii=False))
            else:
                print(error.worker)
                print("status: budget-stopped")
                print(f"reason: {reason}")
            raise SystemExit(BudgetStopped.exit_code) from error
        if isinstance(error.info.get("native_rejection"), dict):
            refusal = error.info["native_rejection"]
            output.update(
                code="native_rejected", activation_state="rejected",
                message=refusal["message"], native_start_error=refusal["message"],
                native_rejection=dict(refusal),
            )
        if isinstance(error.info.get("native_local_refusal"), dict):
            diagnostic = error.info["native_local_refusal"]
            if diagnostic.get("proven") is True and output["activation_state"] == "local-refused":
                output.update(
                    code="local_refused", message=diagnostic["reason"],
                    native_start_error=diagnostic["reason"],
                )
            output["native_local_refusal"] = dict(diagnostic)
        if error.info.get("initial_delivery_conflict") is True:
            output["code"] = "conflict"
            error.exit_code = OperationConflict.exit_code
        elif error.info.get("initial_delivery_rejected") is True:
            output["code"] = "delivery_rejected"
        if error.phase == "initial-delivery" and error.cause is not None:
            output["initial_delivery_error"] = str(error.cause)
        if error.phase == "wait-report" and error.cause is not None:
            cause = error.cause
            output["turn_status"] = getattr(cause, "turn_status", None)
            output["turn_error"] = getattr(cause, "turn_error", None)
            output["waiting_for"] = getattr(cause, "waiting_for", None)
        if args.json:
            print(json.dumps(output, sort_keys=True, ensure_ascii=False))
        elif output["code"] in {"local_refused", "native_rejected"}:
            print(f"ccx {engine} new: {output['message']} "
                  f"(worker {error.worker}; no native replay)", file=sys.stderr)
        else:
            print(
                f"ccx new: {error.worker} identity claimed; creation phase "
                f"{error.phase} remains unavailable. Retain exact worker_id "
                f"{error.worker_id}, never remint.", file=sys.stderr,
            )
        raise SystemExit(error.exit_code)
    except BudgetStopped as exc:
        print(f"ccx new rejected: {exc}", file=sys.stderr)
        raise SystemExit(exc.exit_code) from exc
    except (InvalidRequest, OperationConflict, GenerationError, CcxError) as exc:
        model_error = (
            engine == "codex" and args.model is not None
            and isinstance(exc, InvalidRequest)
            and str(exc).startswith(("unsupported Codex model ", "Codex model must "))
        )
        if args.json and (isinstance(exc, EngineCliNotFound) or model_error):
            print(json.dumps({"ok": False,
                              "code": ("runtime_unavailable" if isinstance(exc, EngineCliNotFound)
                                       else "invalid_request"),
                              "message": str(exc)}, sort_keys=True))
        else:
            print(f"ccx new rejected: {exc}", file=sys.stderr)
        raise SystemExit(exc.exit_code if isinstance(exc, CcxError) else 3) from None
    if result.get("native_ready") is not True or result.get("activation_state") != "confirmed":
        raise RuntimeError("unproven native activation must never exit as a successful ccx new")
    if args.json:
        public = {key: result[key] for key in (
            "worker", "worker_id", "engine", "short", "native_source",
            "native_ready", "created", "reconciled",
        )}
        if prompt is not None:
            public.update(delivery_id=result["delivery_id"],
                          delivery_status=result["delivery_status"],
                          delivery=result["delivery"])
        if args.wait:
            public.update(waited=True, turn_ref=result["turn_ref"],
                          report=result["report"])
        if notify_parent and result.get("notification"):
            public["notify_parent"] = result["notification"]
        print(json.dumps({**public, "ok": True}, sort_keys=True))
    else:
        if args.wait:
            print(f"worker: {result['worker']}")
            print(result["report"]["text"])
        else:
            print(result["worker"])
        if notify_parent and result.get("notification"):
            notice = result["notification"]
            print(f"notify: armed {notice['event_id']} -> Codex parent {notice['parent']}")
            print(f"notify: {notice['instruction']}")
