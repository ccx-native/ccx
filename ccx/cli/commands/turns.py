from __future__ import annotations
import json
import sys
import time
from ccx.ops.compact_ops import (
    new_operation_id as new_compact_operation_id,
)
from ccx.constants import (
    EXIT_ERROR,
    EXIT_NOT_FOUND,
    EXIT_REJECTED,
    EXIT_TURN_INCOMPLETE,
    EXIT_UNAVAILABLE,
    SEND_TEXT_MAX_BYTES,
    STEER_OBSERVE_DEFAULT_S,
)
from ccx.errors import (
    CcxError,
    InvalidRequest,
    SessionUnavailable,
)
from ccx.controllers.orchestrator import require_notify_parent
from ccx.notify.daemon import prepare_notify_parent
from ccx.notify.registry import public_registration as public_parent_notification
from ccx.rpc import rpc as host_rpc
from ccx.ops.semantics import (
    KIND_ELICITATION,
    KIND_PERMISSION,
    KIND_QUESTION,
    elicitation_field_content as _elicitation_field_content,
    payload_guidance as _payload_guidance,  # noqa: F401
    pending_input_kind,
    question_answer_map as _pure_question_answer_map,
    question_answer_message as _question_answer_message,
    send_payload_violation as _pure_send_payload_violation,
)
from ccx.claude.usage import (
    _caps_from_args,
)
from ccx.codex.settle import send_settle_failure as _send_settle_failure
from ccx.cli.commands._common import _has_structured_answer_args, _nbwait_exit_code, _prepare_parent_notification, _resolve_prompt_source, _uuid_input_error, _uuid_input_guard, _uuid_input_id, _uuid_input_print


def _validate_after_turn(after_turn: str | None, next_turn: bool,
                         known_input: str | None = None) -> None:
    """Reject a malformed ``after_turn`` / ``known_input`` selector before
    any observation."""
    if known_input is not None:
        if not isinstance(known_input, str) or not known_input:
            raise InvalidRequest("known_input must be a non-empty request id")
        if not next_turn:
            raise InvalidRequest("known_input requires next_turn")
    if after_turn is None:
        return
    if not next_turn:
        raise InvalidRequest("after_turn requires next_turn")
    from ccx.ops.turn_reports import _ref_id
    _ref_id(after_turn, engine="claude")


_AFTER_TURN_UNSUPPORTED = (
    "--after-turn is supported only for managed Claude sessions: other session "
    "modes have no settled-turn identity to compare"
)


_KNOWN_INPUT_UNSUPPORTED = (
    "--known-input is supported only for managed Claude sessions: other session "
    "modes have no input-gate identity to compare"
)


def _checkpoint_unsupported(after_turn: str | None) -> str:
    return _AFTER_TURN_UNSUPPORTED if after_turn is not None else _KNOWN_INPUT_UNSUPPORTED


def _emit_full_uuid_wait(args, result: dict) -> None:
    """One display/exit mapping for Claude and Codex native wait verdicts."""
    state = result["status"]
    ok = state in {"idle", "stopped"}
    if getattr(args, "json", False):
        print(json.dumps({"ok": ok, **result}, indent=2))
    else:
        print(f"wait: {state}  {result['worker']}")
        if result.get("last_settled_turn"):
            print(f"  last_settled_turn: {result['last_settled_turn']['turn_ref']}")
        if result.get("active_turn_ref"):
            print(f"  active_turn_ref: {result['active_turn_ref']}")
        if result.get("waiting_for"):
            print(f"  waiting_for: {json.dumps(result['waiting_for'])}")
        if result.get("reason"):
            print(f"  reason: {result['reason']}")
        if result.get("error"):
            print(f"  error: {result['error']}")
    if not ok:
        raise SystemExit(
            EXIT_UNAVAILABLE if state == "unavailable"
            else _nbwait_exit_code(state, result.get("apiError"))
        )


def cmd_wait(args):
    from ccx.ops.wait import wait_worker
    try:
        verdict = wait_worker(
            args.target, timeout=getattr(args, "timeout", None),
            next_turn=bool(getattr(args, "next_turn", False)),
            after_turn=getattr(args, "after_turn", None),
            known_input=getattr(args, "known_input", None),
        )
    except CcxError as error:
        print(f"[ccx] wait: {error}", file=sys.stderr)
        raise SystemExit(error.exit_code) from error
    _emit_full_uuid_wait(args, verdict)


def cmd_answer(args):
    """Answer the original QUESTION or notify a separate, positively dead-host SEND."""
    from ccx.client import CcxClient

    try:
        notified = bool(getattr(args, "notify_parent", False))
        if not notified:
            _uuid_input_guard(args, "answer")
            text = _resolve_prompt_source(
                args, "text", "answer", required=not _has_structured_answer_args(args),
            )
        client = CcxClient()
        request_id = getattr(args, "request_id", None)
        if notified:
            from uuid import uuid5, NAMESPACE_URL
            from ccx.claude.delivery import (
                ClaudeDeliveryError, read_receipt as read_semantic_receipt,
            )
            from ccx.errors import OperationConflict
            from ccx.notify.registry import read_registration as read_parent_notification
            from ccx.ops.input import answer_input
            from ccx.ops.wait import reconcile_turn
            from ccx.ops.send import send_turn
            from ccx.store.root import GenerationError, NativeEffectIndeterminate

            record = client._resolve_managed(args.target, "answer")
            request_id = _uuid_input_id(args, record.get("waitingFor"), KIND_QUESTION)
            parent = _prepare_parent_notification(args)
            worker_id = record["workerId"]
            generation = client._uuid_store.generation
            handle = f"claude:{worker_id}"
            token = str(uuid5(
                NAMESPACE_URL,
                f"ccx-input-decision:{generation}:{worker_id}:{request_id}",
            ))
            event_id = str(uuid5(
                NAMESPACE_URL,
                "ccx-notify:" +
                f"full-uuid-claude-input:{generation}:{worker_id}:{request_id}",
            ))
            # F is a different original SEND, never an original native answer.
            fallback_id = str(uuid5(
                NAMESPACE_URL,
                f"ccx-question-deadhost-new-send:{generation}:{worker_id}:{request_id}",
            ))
            fallback_event = str(uuid5(
                NAMESPACE_URL,
                f"ccx-notify:full-uuid-claude-send:{generation}:{worker_id}:{fallback_id}",
            ))
            guidance = (
                f"inspect delivery_id={fallback_id} and "
                f"notification_event_id={fallback_event}; never remap or resend "
                "an uncertain native effect"
            )
            f_path = False
            try:
                # FIRST: original F receipt, before any current-question mapping
                # or host-liveness decision. SEND core owns all replay states.
                try:
                    prior_fallback = read_semantic_receipt(handle, fallback_id)
                except (GenerationError, ClaudeDeliveryError) as error:
                    raise SessionUnavailable(
                        f"Cannot verify the earlier answer delivery; inspect it: {error}; {guidance}",
                        short=worker_id[:8],
                    ) from error
                f_path = prior_fallback is not None
                # SECOND: original native Q receipt. Q core already replays it
                # before checking a now-dead host; never convert Q to F.
                prior_question = (client._uuid_store.read_receipt(handle, "answer", token)
                                  if prior_fallback is None else None)
                pending = record.get("waitingFor")
                same_question = (
                    isinstance(pending, dict)
                    and pending_input_kind(pending) == KIND_QUESTION
                    and pending.get("id") == request_id
                )
                if prior_fallback is not None and not same_question:
                    # A revived/successor host has no original formatted preview.
                    # Read-only reconciliation, never reuse the stored SEND text.
                    original_event = read_parent_notification(fallback_event)
                    if original_event is None or (
                        original_event.get("source") != "send"
                        or (original_event.get("operation") or {}).get("kind") != "claude-turn"
                        or (original_event.get("operation") or {}).get("deliveryId") != fallback_id
                        or original_event.get("parent") != parent
                    ):
                        raise OperationConflict(
                            f"Answer delivery belongs to another request; {guidance}",
                            short=worker_id[:8],
                        )
                    notification = public_parent_notification(original_event)
                    observed = reconcile_turn(
                        handle, delivery_id=fallback_id, expected_generation=generation,
                    )
                    if observed.get("delivery") is None:
                        raise OperationConflict(
                            f"Cannot read the earlier answer delivery; {guidance}",
                            short=worker_id[:8],
                        )
                    delivery = observed["delivery"]
                    send_status = observed["status"]
                    settled = send_status == "settled"
                    preview_gone = True
                else:
                    if not same_question:
                        raise SessionUnavailable(
                            f"The pending question {request_id!r} is no longer current; "
                            f"inspect response_id={token} and notification_event_id={event_id}; "
                            "never remap to a later question", short=worker_id[:8],
                        )
                    text = _resolve_prompt_source(
                        args, "text", "answer",
                        required=not _has_structured_answer_args(args),
                    )
                    answers, problem = _pure_question_answer_map(
                        (pending.get("input") or {}).get("questions") or [],
                        text=text, options=getattr(args, "option", None),
                        text_answers=getattr(args, "text_answer", None),
                    )
                    if problem:
                        raise InvalidRequest(f"answer: {problem}", short=worker_id[:8])
                    if not answers:
                        raise InvalidRequest("answer: answer text is empty", short=worker_id[:8])

                    if prior_fallback is None and (
                        prior_question is not None or host_rpc.host_socket_live(worker_id, record)
                    ):
                        # Q's ordinary receipt-first native path, including replay
                        # on a dead host; never catch its errors and convert to F.
                        accepted = answer_input(
                            handle, request_id=request_id, response_id=token,
                            answers=answers, notify_parent=True,
                            timeout=(getattr(args, "timeout", None)
                                     if getattr(args, "timeout", None) is not None else 90),
                        )
                        notification = accepted["notification"]
                        public = {
                            "ok": accepted["status"] == "accepted",
                            "worker": handle, "worker_id": worker_id,
                            "short": worker_id[:8], "request_id": request_id,
                            "response_id": token, "decision": "answer",
                            "status": accepted["status"], "owned": accepted["owned"],
                            "recommended_action": accepted["recommended_action"],
                            "input_mode": "native-input",
                            "original_gate_answered": accepted["status"] == "accepted",
                            "notify_parent": notification,
                        }
                        if args.json:
                            print(json.dumps(public, ensure_ascii=False, indent=2))
                        else:
                            print(f"answer input {accepted['status']} for original request {request_id}")
                            print(f"Worker: {handle}")
                            print(f"notify: armed {notification['event_id']} -> Codex parent {notification['parent']}")
                            print(f"notify: {notification['instruction']}")
                        if accepted["status"] in {"submitted", "unknown", "prepared"}:
                            raise SystemExit(EXIT_TURN_INCOMPLETE)
                        if accepted["status"] == "rejected":
                            raise SystemExit(EXIT_REJECTED)
                        return

                    if prior_fallback is None:
                        f_path = True
                        # Fresh F only. Positive owned-PID death, native SID
                        # proof and no Q answer receipt are ONE shared guard.
                        # Never call the original answer input first.
                        client._require_dead_original_question_host(
                            worker_id, record, response_id=token,
                        )
                    first = send_turn(
                        handle, _question_answer_message(answers),
                        delivery_id=fallback_id, mode="new_turn_only",
                        cap=None, notify_parent=True, settle=False,
                        expected_generation=generation,
                    )
                    notification = first["notification"]
                    delivery = first["delivery"]
                    send_status = first["status"]
                    settled = bool(first["settled"])
                    preview_gone = False
            except OperationConflict as error:
                if not f_path:
                    raise  # Preserve the original native Q error path.
                raise OperationConflict(
                    f"{error}; {guidance}", short=worker_id[:8],
                ) from error
            except NativeEffectIndeterminate as error:
                if not f_path:
                    raise  # The original Q path never degrades to SEND.
                print(f"[ccx] Answer delivery outcome is unknown; reconcile it: {error}; {guidance}",
                      file=sys.stderr)
                raise SystemExit(EXIT_TURN_INCOMPLETE) from None
            except (GenerationError, ClaudeDeliveryError) as error:
                if not f_path:
                    raise
                raise SessionUnavailable(
                    f"Answer delivery unavailable; check the worker: {error}; {guidance}",
                    short=worker_id[:8],
                ) from error

            # Both the fresh/same-preview SEND and preview-gone read-only
            # projection describe the original NEW delivery, never native Q.
            pending_send = not settled and send_status != "rejected"
            public = {
                "ok": send_status in {"accepted", "settled"},
                "worker": handle, "worker_id": worker_id, "short": worker_id[:8],
                "request_id": request_id, "original_gate": request_id,
                "decision": "answer", "input_mode": "new-message-fallback",
                "original_gate_answered": False,
                "delivery_id": fallback_id, "delivery": delivery,
                "status": send_status, "owned": bool(delivery.get("owned")),
                "pending": pending_send, "settled": settled,
                "completed_model_turn": settled,
                "turn_ref": delivery.get("turn_ref"),
                "recommended_action": ("inspect" if preview_gone and pending_send
                                       else delivery.get("recommended_action")),
                "notify_parent": notification,
            }
            if preview_gone:
                public["inspection_guidance"] = guidance
            if args.json:
                print(json.dumps(public, ensure_ascii=False, indent=2))
            else:
                print(f"Claude question {request_id} was NOT answered; "
                      "a separate NEW user message delivery was attempted/recorded")
                print(f"Worker: {handle}")
                print(f"delivery_id: {fallback_id}; status: {send_status}; "
                      f"owned: {public['owned']}; model turn completed: {settled}")
                print(f"notify: source=send event {notification['event_id']} "
                      f"({notification['state']}); parent machine placement "
                      "does NOT imply parent model completion")
                if preview_gone or pending_send:
                    print(guidance)
            if send_status == "rejected":
                raise SystemExit(EXIT_REJECTED)
            if pending_send and not public["ok"]:
                raise SystemExit(EXIT_TURN_INCOMPLETE)
            return

        if request_id is None:
            pending = client._resolve_managed(args.target, "answer").get("waitingFor")
            request_id = _uuid_input_id(args, pending, KIND_QUESTION)
        result = client.answer(
            args.target, text, request_id=request_id,
            options=getattr(args, "option", None),
            text_answers=getattr(args, "text_answer", None),
            wait=getattr(args, "wait", False),
            timeout=getattr(args, "timeout", None),
            fallback_to_send=True,  # Main CLI only; Python stays opt-in.
        )
        _uuid_input_print(args, "answer", request_id, result)
    except CcxError as error:
        _uuid_input_error("answer", error)


def _send_payload_violation(text: str) -> tuple[int, int] | None:
    "Adapter over the pure :func:`ccx.ops.semantics.send_payload_violation`,\n    reading the limit from this module's ``SEND_TEXT_MAX_BYTES`` global so\n    tests (and callers) that patch ``C.SEND_TEXT_MAX_BYTES`` keep working."
    return _pure_send_payload_violation(text, limit=SEND_TEXT_MAX_BYTES)


def _publish_cli_send_variables(store, owner, args, text, token, *, cap=None) -> str:
    """Publish CLI durable/share variables before the original native SEND.

    Caller already owns an exact full UUID; this helper neither submits a turn
    nor allocates a delivery token or registers a parent notification.
    """
    from ccx.controllers import live_vars
    from ccx.claude.delivery import ClaudeDeliveryError, valid_delivery_id, validate_cap
    from ccx.errors import InvalidRequest
    from ccx.controllers.orchestrator import require_notify_parent

    if args.var is not None and args.vars_from is not None:
        raise InvalidRequest("--var and --vars-from are mutually exclusive")
    values = live_vars.parse_cli_vars(args.var) if args.var is not None else None

    # Original protocol SEND independently checks these again. Do not publish
    # variables for invalid *options*; native busy/receipt fate is later.
    if not isinstance(text, str) or not text.strip():
        raise InvalidRequest("send: prompt text is required")
    if not valid_delivery_id(token):
        raise InvalidRequest("send: delivery_id is missing or unsafe")
    if owner.engine == "claude":
        try:
            validate_cap(cap)
        except ClaudeDeliveryError as error:
            raise InvalidRequest(str(error)) from error
    if args.notify_parent:
        require_notify_parent()

    record = store.record(owner.worker)
    live_vars.require_send_variable_permission(owner, record)

    if args.vars_from is not None:
        source = store.resolve(args.vars_from)
        attachment = live_vars.prepare_shared_durable_attachment(
            source.engine, source.worker_id,
        )
    else:
        attachment = live_vars.prepare_durable_attachment(values)

    if attachment is not None and (attachment.names or attachment.state_id is not None):
        if live_vars.should_publish_send_attachment(
            store, owner, token, caps=cap,
        ):
            live_vars.publish_attachment(attachment, owner.engine, owner.worker_id)
        names = store.variables_info(owner.worker)["names"]
        text = live_vars.with_variable_hint(text, names)
    store.revalidate()
    return text


def cmd_send(args):
    """One globally owned full-UUID Claude native send through original receipts.

    Existing host IPC authenticates the root/generation/full worker/PID/start
    on the same connection before PTY input. Original native-effect intent is
    fsynced submitted-unknown before dispatch. The host's accepted ownership
    is not a completed turn: --wait polls only positively evidenced original
    Stop, never a model-wait while holding an activation lock.
    """
    from ccx import constants
    from ccx.ops.send import send_turn
    from ccx.ops.wait import reconcile_turn
    from ccx.ops.report import report_worker
    from ccx.store.root import require_ccx_active, GenerationError, NativeEffectIndeterminate
    from ccx.store.identity import IdentityError, EngineMismatch
    from ccx.claude.delivery import ClaudeDeliveryError
    from ccx.errors import BudgetStopped, DeliveryRejected, OperationConflict, SessionUnavailable
    from uuid import uuid4
    if args.timeout is not None and args.timeout <= 0:
        print("ccx send: positive --timeout required", file=sys.stderr)
        raise SystemExit(EXIT_ERROR)
    text = _resolve_prompt_source(args, "text", "send", required=True)
    target = args.target
    token = getattr(args, "delivery_id", None) or str(uuid4())
    try:
        store = require_ccx_active(constants.CCX_ROOT)
        owner = store.resolve(target)
        if owner.engine != "claude":
            raise EngineMismatch("Send requires a Claude worker handle")
        caps = _caps_from_args(args)
        if args.var is not None or args.vars_from is not None:
            text = _publish_cli_send_variables(
                store, owner, args, text, token, cap=caps,
            )
        first = send_turn(
            owner.worker, text, delivery_id=token,
            mode="new_turn_only", cap=caps,
            notify_parent=bool(args.notify_parent),
        )
        should_wait = not (args.no_wait or args.notify_parent)
        begin = time.monotonic()
        observation = None
        failure = None
        if should_wait:
            while True:
                observation = reconcile_turn(
                    owner.worker, delivery_id=token,
                    timeout=(0.5 if args.timeout is None else
                             max(0.001, min(0.5, args.timeout-(time.monotonic()-begin)))),
                )
                if observation["status"] in ("settled", "rejected"):
                    break
                failure = _send_settle_failure(owner.worker, observation)
                if failure is not None:
                    break
                if (args.timeout is not None
                        and time.monotonic() - begin >= args.timeout):
                    break
                time.sleep(.10)
        delivery = (observation.get("delivery")
                    if observation is not None else first.get("delivery"))
        settled = bool(
            first.get("settled") if observation is None
            else observation["status"] == "settled"
        )
        summary = {
            "worker": owner.worker, "worker_id": owner.worker_id,
            "engine": owner.engine, "short": owner.short,
            "delivery_id": token,
            "status": (
                failure["status"] if failure is not None else
                observation["status"] if observation is not None
                else first["status"]
            ),
            "settled": settled,
            "delivery": delivery,
        }
        if failure is not None:
            summary["error"] = failure.get("error")
        if args.notify_parent:
            summary["notify_parent"] = first.get("notification")
        if settled and not args.notify_parent:
            # Report THIS delivery's original R, never the worker's latest.
            turn_ref = (delivery or {}).get("turn_ref")
            if not turn_ref:
                raise SessionUnavailable(
                    "Completed delivery has no turn ID; "
                    "reconcile its delivery ID"
                )
            summary["report"] = report_worker(owner.worker, turn_ref=turn_ref)
        if args.json:
            print(json.dumps(summary, ensure_ascii=False))
        else:
            print(owner.worker)
            print(f"delivery_id: {token}")
            if args.notify_parent:
                notification = summary["notify_parent"] or {}
                print(f"notification: {notification.get('event_id')} ({notification.get('state')})")
            elif settled:
                print(summary["report"]["text"])
            else:
                if args.no_wait and summary["status"] == "owned":
                    print("queued for delivery (status: owned); "
                          f"check with ccx reconcile {owner.worker} --delivery-id {token}")
                else:
                    print(f"status: {summary['status']}" +
                          (f"; error: {summary['error']}" if summary.get("error") else ""))
        if summary["status"] == "rejected":
            print(
                "ccx send rejected: " +
                str((delivery or {}).get("rejection_message") or "the worker refused the message"),
                file=sys.stderr,
            )
            raise SystemExit(EXIT_REJECTED)
        if should_wait and not settled:
            # A proven failed/terminal state differs from an unknown timeout.
            raise SystemExit(
                (EXIT_UNAVAILABLE if failure["status"] == "unavailable" else
                 _nbwait_exit_code(failure["status"], failure.get("apiError")))
                if failure is not None else EXIT_TURN_INCOMPLETE
            )
    except NativeEffectIndeterminate as error:
        # Original global submitted-unknown is not a definite refusal and is
        # never permission to resend this caller-owned Claude delivery token.
        print(f"ccx send: delivery {token!r} is indeterminate; "
              f"reconcile that same token, never resend: {error}", file=sys.stderr)
        raise SystemExit(EXIT_TURN_INCOMPLETE) from None
    except OperationConflict as error:
        print(f"ccx send conflict: {error}", file=sys.stderr)
        raise SystemExit(error.exit_code) from None
    except BudgetStopped as error:
        if args.json:
            print(json.dumps({
                "worker": owner.worker, "worker_id": owner.worker_id,
                "engine": "claude", "short": owner.short,
                "delivery_id": token, "status": "budget-stopped",
                "code": "budget_stopped", "reason": error.reason or str(error),
                "settled": False,
            }, ensure_ascii=False))
        else:
            print(f"ccx send: {error}", file=sys.stderr)
        raise SystemExit(error.exit_code) from None
    except DeliveryRejected as error:
        # Only the typed host-busy proof from the original readiness probe is
        # a definite refusal: it is raised before any receipt or native input.
        if ((error.refusal == "busy" and error.state == "busy"
             or error.refusal in {"waiting", "state_unavailable"}
             and error.state == "waiting")
                and error.delivery_status == "rejected"):
            print(f"ccx send rejected: {error}", file=sys.stderr)
            raise SystemExit(EXIT_REJECTED) from None
        print(f"ccx send unavailable: {error}", file=sys.stderr)
        raise SystemExit(3) from None
    except CcxError as error:
        print(f"ccx send unavailable: {error}", file=sys.stderr)
        raise SystemExit(error.exit_code) from None
    except (GenerationError, IdentityError, ClaudeDeliveryError) as error:
        print(f"ccx send unavailable: {error}", file=sys.stderr)
        raise SystemExit(3) from None


def cmd_vars(args):
    """Read only full-owned CCX variable state (not live object execution)."""
    from ccx import constants
    from ccx.store.root import GenerationError, require_ccx_active
    from ccx.store.identity import IdentityError
    store = require_ccx_active(constants.CCX_ROOT)
    try:
        value = store.variables_info(args.target, name=getattr(args, "name", None))
    except IdentityError as exc:
        print(f"ccx vars: {exc}", file=sys.stderr)
        raise SystemExit(2) from None
    except GenerationError as exc:
        print(f"ccx vars: durable state unavailable: {exc}", file=sys.stderr)
        raise SystemExit(3) from None
    if getattr(args, "json", False):
        print(json.dumps(value, indent=2, ensure_ascii=False))
    elif getattr(args, "name", None) is not None:
        print(json.dumps(value["value"], indent=2, ensure_ascii=False))
    elif value["names"]:
        print("\n".join(value["names"]))
    else:
        print("no variables")


def cmd_notify(args):
    """Arm an independent read-only observation of one original Claude turn."""
    from ccx import constants
    from ccx.errors import CcxError, InvalidRequest, SessionNotFound, SessionUnavailable
    from ccx.controllers.orchestrator import require_notify_parent
    from ccx.notify.registry import create_registration, public_registration
    from ccx.notify.daemon import ensure_notify_daemon
    from ccx.notify.select import select_current_turn, select_future_turn
    from ccx.store.root import GenerationError, require_ccx_active
    from ccx.store.identity import IdentityError, UnknownWorker

    future = bool(getattr(args, "next_turn", False))
    try:
        try:
            store = require_ccx_active(constants.CCX_ROOT)
            owner = store.resolve(args.target)
            if owner.engine != "claude":
                raise InvalidRequest("Notification requires a Claude worker handle")
            parent = require_notify_parent()
            selection = (select_future_turn(owner.worker) if future
                         else select_current_turn(owner.worker))
            reg = create_registration(
                parent=parent, worker_engine="claude", worker_id=owner.worker_id,
                worker_name=selection["name"], operation=selection["operation"],
                source="notify",
            )
        except UnknownWorker as exc:
            raise SessionNotFound(f"Worker not found: {args.target}") from exc
        except IdentityError as exc:
            raise InvalidRequest(f"Invalid worker handle for notification: {exc}") from exc
        except (GenerationError, OSError) as exc:
            # Selection or publication failed; never reported as an armed event.
            raise SessionUnavailable(f"Cannot select a worker turn to notify about; inspect the worker: {exc}") from exc
        available = True
        try:
            ensure_notify_daemon()
        except (CcxError, GenerationError, OSError) as exc:
            # Durable publication already happened. Re-arming would allocate
            # another independent event, so report this one as still armed.
            available = False
            print(f"ccx notify: event durably armed; supervisor unavailable: {exc}",
                  file=sys.stderr)
        result = {**public_registration(reg), "armed": True,
                  "selected_state": selection["state"],
                  "notifier_available": available,
                  **({"wait_mode": "next-turn"} if future else {})}
    except CcxError as exc:
        print(f"ccx notify: {str(exc).removeprefix('ccx notify: ')}", file=sys.stderr)
        raise SystemExit(exc.exit_code) from None
    if args.json:
        print(json.dumps(result, ensure_ascii=False))
    else:
        print(f"{result['worker']} event_id={result['event_id']} "
              f"selection={result['selection_mode']} state={result['state']}")


def _cmd_permission_decision(args, decision: str):
    """One current native permission decision, optionally its exact reply."""
    from ccx.client import CcxClient

    verb = "approve" if decision == "allow" else "reject"
    try:
        if getattr(args, "notify_parent", False):
            # Reuse the same original prefix/broker lookup as ordinary Claude.
            # The parent is captured before any child native input effect.
            _prepare_parent_notification(args)
            if getattr(args, "settle", False):
                raise InvalidRequest("Claude permission --notify-parent does not support --settle")
            from uuid import uuid5, NAMESPACE_URL
            from ccx import constants
            from ccx.ops.input import decide_input
            from ccx.store.root import require_ccx_active
            client = CcxClient()
            record = client._resolve_managed(args.target, verb)
            request_id = _uuid_input_id(args, record.get("waitingFor"), KIND_PERMISSION)
            worker_id = record["workerId"]
            store = require_ccx_active(constants.CCX_ROOT)
            token = str(uuid5(
                NAMESPACE_URL,
                f"ccx-input-decision:{store.generation}:{worker_id}:{request_id}",
            ))
            reason = getattr(args, "reason", None)
            if reason == "":  # CLI-only empty reason is native absence.
                reason = None
            accepted = decide_input(
                f"claude:{worker_id}", request_id=request_id, response_id=token,
                decision="approve" if decision == "allow" else "reject",
                reason=reason, notify_parent=True,
                timeout=(getattr(args, "timeout", None) if getattr(args, "timeout", None)
                         is not None else 90),
            )
            notification = accepted["notification"]
            public = {
                "ok": accepted["status"] == "accepted",
                "worker": f"claude:{worker_id}", "worker_id": worker_id,
                "short": worker_id[:8], "request_id": request_id,
                "response_id": token, "decision": decision,
                "status": accepted["status"], "owned": accepted["owned"],
                "recommended_action": accepted["recommended_action"],
                "input_mode": "native-input",
                "original_gate_answered": accepted["status"] == "accepted",
                "notify_parent": notification,
            }
            if args.json:
                print(json.dumps(public, ensure_ascii=False, indent=2))
            else:
                print(f"{verb} permission input {accepted['status']} for original request {request_id}")
                print(f"Worker: claude:{worker_id}")
                print(f"notify: armed {notification['event_id']} -> Codex parent {notification['parent']}")
                print(f"notify: {notification['instruction']}")
            if accepted["status"] in {"submitted", "unknown"}:
                raise SystemExit(EXIT_TURN_INCOMPLETE)
            if accepted["status"] == "rejected":
                raise SystemExit(EXIT_REJECTED)
            return
        _uuid_input_guard(args, verb)
        client = CcxClient()
        request_id = getattr(args, "request_id", None)
        if request_id is None:
            pending = client._resolve_managed(args.target, verb).get("waitingFor")
            request_id = _uuid_input_id(args, pending, KIND_PERMISSION)
        method = client.approve if decision == "allow" else client.reject
        result = method(
            args.target, request_id=request_id,
            reason=getattr(args, "reason", None),
            wait=getattr(args, "wait", False),
            timeout=getattr(args, "timeout", None),
        )
        _uuid_input_print(args, verb, request_id, result)
    except CcxError as error:
        _uuid_input_error(verb, error)


def cmd_respond(args):
    """Resolve one original MCP elicitation with the existing client."""
    from ccx.client import CcxClient

    try:
        notified = bool(getattr(args, "notify_parent", False))
        if notified:
            _prepare_parent_notification(args)  # own Codex caller before input effect
        else:
            _uuid_input_guard(args, "respond")
        action = "accept" if args.accept else "decline" if args.decline else "cancel"
        client = CcxClient()
        record = client._resolve_managed(args.target, "respond")
        pending = record.get("waitingFor")
        request_id = _uuid_input_id(args, pending, KIND_ELICITATION)
        if notified:
            from uuid import uuid5, NAMESPACE_URL
            from ccx.ops.input import respond_input
            worker_id = record["workerId"]
            generation = client._uuid_store.generation
            token = str(uuid5(
                NAMESPACE_URL, f"ccx-input-decision:{generation}:{worker_id}:{request_id}",
            ))
            event_id = str(uuid5(
                NAMESPACE_URL,
                "ccx-notify:" +
                f"full-uuid-claude-input:{generation}:{worker_id}:{request_id}",
            ))
            if (args.field and (
                    not isinstance(pending, dict)
                    or pending_input_kind(pending) != KIND_ELICITATION
                    or pending.get("id") != request_id)):
                raise SessionUnavailable(
                    f"The requested input form {request_id!r} is no longer "
                    f"current; inspect response_id={token} and "
                    f"notification_event_id={event_id}; never remap fields to a later gate",
                    short=worker_id[:8],
                )
        content = None
        if args.content is not None or args.field:
            if action != "accept":
                raise InvalidRequest("respond: only --accept takes --content or --field")
            content = {}
            if args.content is not None:
                try:
                    content = json.loads(args.content)
                except ValueError as error:
                    raise InvalidRequest(f"respond: invalid content JSON: {error}") from error
                if not isinstance(content, dict):
                    raise InvalidRequest("respond: --content must be a JSON object")
            pairs = []
            for raw in args.field or []:
                name, sep, value = raw.partition("=")
                if not sep or not name:
                    raise InvalidRequest(f"respond: --field expects NAME=VALUE, got {raw!r}")
                pairs.append((name, value))
            typed, error = _elicitation_field_content(pending, pairs)
            if error:
                raise InvalidRequest(f"respond: {error}")
            content.update(typed)
        if notified:
            accepted = respond_input(
                f"claude:{worker_id}", request_id=request_id, response_id=token,
                action=action, content=content, notify_parent=True,
                timeout=(getattr(args, "timeout", None)
                         if getattr(args, "timeout", None) is not None else 90),
            )
            notification = accepted["notification"]
            public = {
                "ok": accepted["status"] == "accepted",
                "worker": f"claude:{worker_id}", "worker_id": worker_id,
                "short": worker_id[:8], "request_id": request_id,
                "response_id": token, "action": action,
                "status": accepted["status"], "owned": accepted["owned"],
                "recommended_action": accepted["recommended_action"],
                "input_mode": "native-input",
                "original_gate_answered": accepted["status"] == "accepted",
                "notify_parent": notification,
            }
            if args.json:
                print(json.dumps(public, ensure_ascii=False, indent=2))
            else:
                print(f"{action} MCP input {accepted['status']} for original request {request_id}")
                print(f"Worker: claude:{worker_id}")
                print(f"notify: {notification['state']} {notification['event_id']} "
                      f"-> Codex parent {notification['parent']}")
                print(f"notify: {notification['instruction']}")
            if accepted["status"] in {"submitted", "unknown", "prepared"}:
                raise SystemExit(EXIT_TURN_INCOMPLETE)
            if accepted["status"] == "rejected":
                raise SystemExit(EXIT_REJECTED)
            return
        result = client.respond_elicitation(
            args.target, action, content, request_id=request_id,
            wait=getattr(args, "wait", False),
            timeout=getattr(args, "timeout", None),
        )
        _uuid_input_print(args, action, request_id, result)
    except CcxError as error:
        _uuid_input_error("respond", error)


def cmd_approve(args):
    _cmd_permission_decision(args, "allow")


def cmd_reject(args):
    _cmd_permission_decision(args, "deny")


def cmd_steer(args):
    """One exact owned Claude steer through the native semantic receipt."""
    from uuid import uuid4
    from ccx.client import CcxClient
    from ccx.claude.delivery import valid_delivery_id
    from ccx.errors import CcxError
    text = _resolve_prompt_source(args, "text", "steer", required=True)
    token = getattr(args, "delivery_id", None) or str(uuid4())
    if not valid_delivery_id(token):
        print("ccx steer: invalid --delivery-id (nonempty, at most 128 characters)",
              file=sys.stderr)
        raise SystemExit(2)
    try:
        result = CcxClient().steer(
            args.target, text, delivery_id=token,
            wait=not args.no_wait,
            timeout=STEER_OBSERVE_DEFAULT_S if args.timeout is None else args.timeout,
            caps=_caps_from_args(args),
        )
    except CcxError as error:
        print(f"ccx steer: {error} (delivery_id={token})", file=sys.stderr)
        raise SystemExit(error.exit_code) from error
    out = result.as_dict()
    if args.json:
        print(json.dumps(out, indent=2))
    else:
        print(f"steer: {out['status']}  {out['worker']}")
        print(f"delivery_id: {token}")
        if result.timed_out:
            print("Placement still pending; the prompt was NOT re-sent.")
            print(f"Inspect with ccx reconcile {out['worker']} --delivery-id {token}.")
        elif result.evidence:
            print(f"evidence: {result.evidence}")


def cmd_reconcile(args):
    """Read the exact Claude receipt; native steer placement is not turn completion."""
    from ccx.claude import delivery as claude_delivery
    from ccx.client import CcxClient
    from ccx.ops.wait import reconcile_turn
    from ccx.store.root import GenerationError

    try:
        receipt = claude_delivery.read_receipt(args.target, args.delivery_id)
        if isinstance(receipt, dict) and receipt.get("requestedMode") == "steer_only":
            result = CcxClient().steer_status(args.target, args.delivery_id)
            output, status = result.as_dict(), result.status
        else:
            output = reconcile_turn(args.target, delivery_id=args.delivery_id)
            status = output["status"]
    except (CcxError, GenerationError, claude_delivery.ClaudeDeliveryError) as error:
        print(f"ccx reconcile: {error}", file=sys.stderr)
        raise SystemExit(getattr(error, "exit_code", EXIT_UNAVAILABLE)) from error
    if args.json:
        print(json.dumps(output, ensure_ascii=False, indent=2))
    else:
        print(f"{args.target}  delivery {args.delivery_id}: {status}")
        if output.get("evidence"):
            print(f"evidence: {output['evidence']}")
    if status == "not_found":
        raise SystemExit(EXIT_NOT_FOUND)
    if status in {"rejected", "not_delivered"}:
        raise SystemExit(EXIT_REJECTED)
    if status in {"unknown", "prepared", "owned", "submitted", "pending"}:
        raise SystemExit(EXIT_TURN_INCOMPLETE)


def cmd_compact(args):
    """One full-UUID Claude compact through the existing typed original core."""
    from ccx.client import CcxClient
    from ccx.errors import CapabilityUnsupported, CcxError

    token = getattr(args, "operation_id", None)
    if token is None:
        token = new_compact_operation_id()
    notification = None
    if getattr(args, "notify_parent", False):
        from ccx import constants
        from ccx.errors import InvalidRequest, OperationConflict
        from ccx.notify.registry import compact_notification_failure, create_registration, public_registration
        from ccx.store.root import GenerationError, require_ccx_active
        try:
            if getattr(args, "settle", False):
                raise InvalidRequest("Claude compact does not support --settle")
            store = require_ccx_active(constants.CCX_ROOT)
            owner = store.resolve(args.target)
            if owner.engine != "claude":
                raise InvalidRequest("compact notification needs a managed Claude worker")
            record = store.record(owner.worker)
            parent = require_notify_parent()
            prepare_notify_parent(parent)
            notification = create_registration(
                parent=parent, worker_engine="claude", worker_id=owner.worker_id,
                worker_name=record.get("name"),
                operation={"kind": "claude-compact", "operationId": token,
                           "nativeSourceId": (record.get("native") or {}).get("sessionId")},
                source="compact",
            )
            # A cancelled event is sticky; no same-ID retry may turn its
            # original no-effect refusal into a new unnotified native compact.
            if notification["state"] in {"cancelled", "undeliverable"}:
                raise OperationConflict("Compaction notification already finished; start a new request")
            if notification["state"] in {"ready", "delivered"}:
                prior = store.read_receipt(owner.worker, "compact", token)
                if prior is None or prior.get("state") not in {"submitted-unknown", "accepted"}:
                    raise OperationConflict("Compaction result could not be verified; inspect its operation")
            # Publication was fsynced before the existing native compact core.
            result = CcxClient().compact(
                owner.worker, wait=False, timeout=args.timeout, operation_id=token)
        except (CcxError, GenerationError, OSError) as exc:
            if notification is not None:
                try:
                    uncertain, state = compact_notification_failure(notification, str(exc))
                except (ValueError, OSError) as failure:
                    uncertain, state = True, f"unreadable ({failure})"
                code = EXIT_TURN_INCOMPLETE if uncertain else getattr(exc, "exit_code", EXIT_UNAVAILABLE)
                print(f"ccx compact: {exc}; operation_id={token}; "
                      f"event_id={notification['eventId']}; notification_state={state}"
                      + ("; native effect uncertain, do not retry with a new ID"
                         if uncertain else "; no native compact was submitted"),
                      file=sys.stderr)
                raise SystemExit(code) from exc
            print(f"ccx compact: {exc} (operation_id={token})", file=sys.stderr)
            raise SystemExit(getattr(exc, "exit_code", EXIT_UNAVAILABLE)) from exc
        out = {**result.as_dict(), "notify_parent": public_registration(notification),
               "accepted": True, "settled": False}
        if args.json:
            print(json.dumps(out, indent=2))
        else:
            print(f"compact {result.operation_status}: {out['worker']}")
            print(f"operation_id: {token}")
            print(f"notify: {out['notify_parent']['state']} {notification['eventId']} (parent model completion unproven)")
        return
    try:
        if getattr(args, "settle", False):
            raise CapabilityUnsupported("Claude compact does not support --settle",
                                        engine="claude", capability="compact_settle")
        result = CcxClient().compact(
            args.target, wait=not args.no_wait, timeout=args.timeout,
            operation_id=token,
        )
    except CcxError as exc:
        # A caller can only safely reconcile the *same* native operation ID.
        print(f"ccx compact: {exc} (operation_id={token})", file=sys.stderr)
        raise SystemExit(exc.exit_code) from exc

    out = result.as_dict()
    if args.json:
        print(json.dumps(out, indent=2))
    elif args.no_wait:
        print(f"compact {result.operation_status or 'accepted'} (not yet proven): "
              f"{out['worker']}")
        print(f"operation_id: {token}")
    elif result.compacted:
        if type(result.pre_tokens) is int and type(result.post_tokens) is int:
            print(f"compacted {result.pre_tokens} -> {result.post_tokens} tokens")
        else:
            print("compacted")
        print(f"operation_id: {token}")
    else:
        print(result.message or "nothing to compact")
        print(f"operation_id: {token}")


def cmd_set(args):
    """Configure future Claude spawns with the existing exact public core."""
    from ccx.client import CcxClient
    from ccx.errors import CcxError

    name_given = args.name is not None
    model_given = args.model is not None
    effort_given = args.effort is not None
    permissions_given = args.permissions is not None
    config_given = model_given or effort_given or permissions_given
    if not (name_given or config_given):
        print("ccx set: nothing to change (use --name, --model, --effort or --permissions)",
              file=sys.stderr)
        raise SystemExit(1)
    if args.restart and not config_given:
        print("ccx set: --restart requires --model, --effort and/or --permissions; "
              "no rename performed", file=sys.stderr)
        raise SystemExit(1)
    try:
        client = CcxClient()
        restarted = False
        if config_given:
            changes = {}
            if model_given:
                # Preserve the established Claude CLI meaning: unlike Codex,
                # --model default is a literal provider model name.
                changes["model"] = args.model
            if effort_given:
                changes["effort"] = None if args.effort == "default" else args.effort
            if permissions_given:
                changes["permissions"] = args.permissions
            # One original owner CAS, and at most ONE confirmed host stop,
            # even when all three config switches are specified together.
            info, restarted = client._set_spawn_config(
                args.target, changes, restart=args.restart,
            )
        else:
            info = None
        if name_given:
            # A refused config must not have already renamed this worker.
            info = client.set_name(args.target, args.name)
        assert info is not None
    except CcxError as error:
        print(f"ccx set: {error}", file=sys.stderr)
        raise SystemExit(error.exit_code) from error
    result = {
        "worker": f"claude:{info.worker_id}", "worker_id": info.worker_id,
        "engine": "claude", "short": info.short,
        "action": ("restart" if restarted else "set")
                  if config_given else "rename",
    }
    if name_given:
        result["name"] = info.name
    if model_given:
        result["model"] = info.model
    if effort_given:
        result["effort"] = info.effort
    if permissions_given:
        result["permissions"] = args.permissions
    if args.json:
        print(json.dumps(result, sort_keys=True, ensure_ascii=False))
    else:
        print(result["worker"])
