from __future__ import annotations
from typing import Any
from ccx.rpc import rpc as host_rpc
from ccx.codex import evidence as codex_evidence
from ccx.controllers.orchestrator import require_notify_parent
from ccx.notify.registry import cancel_registration as cancel_parent_notification, create_registration as create_parent_notification, public_registration as public_parent_notification, read_registration as read_parent_notification
from ccx.notify.daemon import prepare_notify_parent
from ccx.errors import (
    CapabilityUnsupported,
    DeliveryRejected,
    InvalidRequest,
    OperationConflict,
    SessionUnavailable,
)
from ccx.ops.semantics import (
    ELICITATION_ACTIONS,
    KIND_ELICITATION,
    KIND_PERMISSION,
    KIND_QUESTION,
    elicitation_response_problem,
    pending_input_kind,
)
from ccx.store.worker_state import RUNTIME_SERVED
from ccx.ops.common import MAX_RESPONSE_ID, _CLIENT, _CODEX_CLIENT, recorded_inspector


def _validate_answer_map(value: object) -> dict[str, str]:
    if not isinstance(value, dict) or not value:
        raise InvalidRequest("answers must be a non-empty object")
    result: dict[str, str] = {}
    for question, answer in value.items():
        if not isinstance(question, str) or not question:
            raise InvalidRequest("answer question keys must be non-empty strings")
        if not isinstance(answer, str) or not answer:
            raise InvalidRequest("answer values must be non-empty strings")
        result[question] = answer
    return result


def _answer_text_pairs(
    waiting_for: dict[str, Any], answers: dict[str, str]
) -> list[tuple[int, str]]:
    source = waiting_for.get("input")
    questions = (
        source.get("questions")
        if isinstance(source, dict)
        else None
    )
    questions = list(questions or [])
    expected: list[str] = []
    for question in questions:
        text = question.get("question") if isinstance(question, dict) else None
        if isinstance(text, str) and text:
            expected.append(text)
    missing = [text for text in expected if text not in answers]
    extra = [text for text in answers if text not in set(expected)]
    if missing:
        raise InvalidRequest(
            "answers is missing question(s): " + "; ".join(missing)
        )
    if extra:
        raise InvalidRequest(
            "answers contains unknown question(s): " + "; ".join(extra)
        )
    return [
        (index + 1, answers[text])
        for index, text in enumerate(expected)
    ]


def _original_input_evidence(store, owner, kind: str, request_id: str,
                             response_id: str, payload: dict, native_id: str):
    """Bounded read of the original native response event, never a short alias."""
    from ccx.store.store import _digest

    if owner.engine == "claude":
        from ccx.ops.worker_events import read_full_uuid_event_page
        event_type = ("question_answered" if kind == "answer"
                      else "elicitation_resolved" if kind == "respond"
                      else ("permission_approved"
                            if payload["decision"] == "approve"
                            else "permission_rejected"))
        page = read_full_uuid_event_page(
            store, owner.worker, tail=1000, kinds=["input.resolved"],
        )
        for event in reversed(page["events"]):
            if (event.get("type") == event_type
                    and event.get("request_id") == request_id
                    and event.get("response_id") == response_id
                    and (kind != "respond"
                         or event.get("action") == payload["action"])):
                label = f"host-event:{event.get('epoch')}:{event.get('seq')}"
                sha = _digest({
                    "workerId": owner.worker_id, "requestId": request_id,
                    "responseId": response_id, "cursor": event["cursor"],
                })
                return label, sha
        return None

    journal = codex_evidence.read_journal(owner.worker)
    expected = "accept" if payload["decision"] == "approve" else "decline"
    for event in reversed(journal.events):
        params = event.get("params")
        if (event.get("method") == codex_evidence.INPUT_RESOLVED_METHOD
                and event.get("threadId") == native_id
                and isinstance(params, dict)
                and params.get("gateId") == request_id
                and params.get("responseId") == response_id
                and params.get("decision") == expected):
            label = f"codex-evidence:{event.get('sequence')}"
            sha = _digest({
                "workerId": owner.worker_id, "threadId": native_id,
                "requestId": request_id, "responseId": response_id,
                "journalInstanceId": journal.journal_instance_id,
                "sequence": event.get("sequence"), "decision": expected,
            })
            return label, sha
    return None


def _original_input_public(store, owner, kind: str, request_id: str,
                           response_id: str, receipt: dict, *,
                           evidence: str | None = None,
                           unresolved: bool = False,
                           notification: dict | None = None) -> dict:
    """Legacy protocol shape, read exclusively from the global native receipt."""

    state = receipt["state"]
    status = ("unknown" if unresolved else "submitted"
              if state == "submitted-unknown" else state)
    action = ("retry_same_id" if status == "prepared"
              else "wait" if status == "submitted"
              else "inspect" if status == "unknown" else "none")
    proof = receipt.get("nativeEvidenceSha256")
    row = {
        "response_id": response_id, "request_id": request_id, "kind": kind,
        "status": status, "owned": status in ("submitted", "accepted", "unknown"),
        "recommended_action": action,
        "notification_event_id": notification["eventId"] if notification else None,
        "created_at_ms": None, "submitted_at_ms": None, "completed_at_ms": None,
        "evidence": evidence or ("native-sha256:" + proof if proof else None),
        "reason": ("original input fate remains unproven; inspect without resubmitting"
                   if unresolved else None),
    }
    return {"worker": recorded_inspector(store, owner.worker),
            **row, "receipt": dict(row),
            "notification": (public_parent_notification(notification)
                             if notification is not None else None)}


def _input_intent_digest(
    *, worker_id: str, engine: str, generation: str, native_id: str,
    native_source: str, kind: str, request_id: str, response_id: str,
    payload: dict | None = None, payload_sha256: str | None = None,
) -> str:
    """One native input-intent digest, also verifying frozen notification hashes."""
    from ccx.store.store import _digest
    from ccx.store.root import _HASH
    if (payload is None) == (payload_sha256 is None):
        raise InvalidRequest("input digest needs exactly one payload or frozen hash")
    if payload_sha256 is not None and (
        not isinstance(payload_sha256, str) or not _HASH.fullmatch(payload_sha256)
    ):
        raise InvalidRequest("input payload hash must be 64 lowercase hex characters")
    return _digest({
        "workerId": worker_id, "engine": engine,
        "generation": generation, "nativeId": native_id,
        "nativeSource": native_source, "kind": kind, "requestId": request_id,
        "responseId": response_id,
        "payloadSha256": _digest(payload) if payload_sha256 is None else payload_sha256,
        "action": "answer",
    })


def valid_response_id(value: object) -> bool:
    return (
        isinstance(value, str)
        and bool(value)
        and len(value) <= MAX_RESPONSE_ID
        and "\x00" not in value
    )


def _original_input(
    target: str, *, kind: str, request_id: str, response_id: str,
    payload: dict, timeout: float | None,
    notify_parent: bool = False, settle: bool = False,
) -> dict[str, Any]:
    """One full-UUID response receipt and <=2s original native effect."""
    from ccx import constants
    from ccx.codex import runtime as codex_runtime
    from ccx.store.root import NativeEffectIndeterminate, require_ccx_active
    from ccx.store.store import OperationIntent, _digest

    if (not isinstance(request_id, str) or not request_id
            or len(request_id) > 256 or "\x00" in request_id):
        raise InvalidRequest("request_id must be a nonempty bounded string")
    if not valid_response_id(response_id):
        raise InvalidRequest("response_id must be a nonempty string of at most 128 characters")
    store = require_ccx_active(constants.CCX_ROOT)
    owner = store.resolve(target)
    if settle and not notify_parent:
        raise InvalidRequest("--settle requires --notify-parent")
    if notify_parent and kind not in ("decide", "answer", "respond"):
        raise CapabilityUnsupported("input notification requires original Claude input",
                                    engine=owner.engine, capability="decision_notify_parent")
    if notify_parent and settle and owner.engine != "codex":
        raise InvalidRequest("Claude permission notification does not support --settle")
    parent = None
    if notify_parent:
        parent = require_notify_parent()
        prepare_notify_parent(parent)  # existing original Codex return-address gate
    if kind in ("answer", "respond") and owner.engine != "claude":
        raise CapabilityUnsupported(
            f"{kind} requires an original Claude input gate",
            engine=owner.engine,
            capability="answer_question" if kind == "answer" else "respond_elicitation",
        )
    if owner.engine == "codex" and len(request_id) > 128:
        raise InvalidRequest("Codex approval requires a bounded original gateId")
    rec = store.record(owner.worker)
    native = rec.get("native") or {}
    activation = store.native_activation(owner.worker)
    binding = activation.get("nativeBinding") or {}
    name = "sessionId" if owner.engine == "claude" else "threadId"
    native_id = native.get(name)
    source = (f"claude-session:{native_id}" if owner.engine == "claude"
              else store.codex_source(owner.worker))
    if (not isinstance(native_id, str) or not native_id
            or activation.get("state") != "confirmed"
            or activation.get("source") != source
            or binding.get(name) != native_id
            or (owner.engine == "codex"
                and native.get("threadSource") != source)):
        raise SessionUnavailable("original input native source is unconfirmed",
                                 short=owner.short)
    digest = _input_intent_digest(
        worker_id=owner.worker_id, engine=owner.engine,
        generation=store.generation, native_id=native_id, native_source=source,
        kind=kind, request_id=request_id, response_id=response_id, payload=payload,
    )
    prior = store.read_receipt(owner.worker, "answer", response_id)
    if prior is not None and (
        prior.get("intentSha256") != digest
        or prior.get("family") != "answer"
        or prior.get("action") != "answer"
    ):
        raise OperationConflict(
            "response_id is already bound to another gate/kind/payload",
            short=owner.short,
        )
    def previous_notification() -> dict:
        # Read only: never arm a new parent event after any possible native
        # response, nor re-select a later active turn/gate on replay.
        from uuid import uuid5, NAMESPACE_URL
        event_id = str(uuid5(NAMESPACE_URL,
            "ccx-notify:" +
            f"full-uuid-{owner.engine}-input:{store.generation}:{owner.worker_id}:{request_id}"))
        row = read_parent_notification(event_id)
        if row is None:
            raise OperationConflict(
                f"original input {response_id} was submitted without a parent event; "
                "cannot arm after native effect", short=owner.short)
        operation = row.get("operation") or {}
        expected_kind = (f"claude-input-{kind}" if kind in ("answer", "respond")
                         else f"{owner.engine}-input-decision")
        if (row.get("source") != "input"
                or operation.get("kind") != expected_kind
                or operation.get("gateId") != request_id
                or operation.get("responseId") != response_id
                or (kind in ("answer", "respond")
                    and operation.get("payloadSha256") != _digest(payload))
                or (kind == "decide"
                    and (operation.get("decision") != payload["decision"]
                         or (owner.engine == "claude"
                             and operation.get("reason") != payload["reason"])))
                or operation.get("intentSha256") != digest
                or row.get("parent") != parent
                or row.get("settlementMode") != ("orchestration" if settle else None)):
            raise OperationConflict(
                "original input event is owned by another input, parent or settlement mode",
                short=owner.short)
        public_parent_notification(row)  # validates the frozen native proof and digest
        return row

    proof = None
    if prior is not None:
        if prior["state"] == "submitted-unknown":
            proof = _original_input_evidence(
                store, owner, kind, request_id, response_id, payload, native_id,
            )
            if proof:
                original = OperationIntent(
                    owner.worker_id, owner.engine, "answer", response_id,
                    digest, "submitted-unknown", store.generation,
                )
                prior = store.settle_native_intent(
                    original, state="accepted", native_evidence_sha256=proof[1],
                )
        if prior["state"] != "prepared":
            current = store.record(owner.worker)
            if owner.engine == "claude":
                waiting = current.get("waitingFor")
                still_original = (isinstance(waiting, dict)
                                  and waiting.get("id") == request_id)
            else:
                still_original = any(
                    isinstance(gate, dict) and gate.get("gateId") == request_id
                    for gate in current.get("pendingApprovals") or []
                )
            store.revalidate()
            return _original_input_public(
                store, owner, kind, request_id, response_id, prior,
                evidence=proof[0] if proof else None,
                unresolved=prior["state"] == "submitted-unknown"
                and not still_original,
                notification=previous_notification() if notify_parent else None,
            )

    # Fresh/PREPARED only: any prior effect was handled before native gate selection.
    input_turn_ref = None
    if owner.engine == "claude":
        current = rec.get("waitingFor")
        expected = {"answer": KIND_QUESTION, "decide": KIND_PERMISSION,
                    "respond": KIND_ELICITATION}[kind]
        if (not isinstance(current, dict)
                or pending_input_kind(current) != expected
                or current.get("id") != request_id):
            raise DeliveryRejected(
                "the exact original Claude input gate is not pending",
                short=owner.short, refusal="request-id-mismatch",
            )
        if kind == "answer":
            pairs = _answer_text_pairs(current, payload)
            if not pairs:
                raise InvalidRequest("no pending original question")
        elif kind == "respond":
            problem = elicitation_response_problem(
                current, payload["action"], payload["content"],
            )
            if problem:
                raise InvalidRequest(f"respond: {problem}", short=owner.short)
        if not host_rpc.host_socket_live(owner.worker_id, rec):
            raise SessionUnavailable("Claude cannot receive input; check worker status",
                                     short=owner.short)
        if notify_parent:
            input_turn_ref, _prior_ref = _CLIENT._input_turn_fence(
                owner.worker_id, rec, request_id, expected,
                _CLIENT._input_deadline(timeout),
            )
    else:
        gate = next((
            value for value in rec.get("pendingApprovals") or []
            if isinstance(value, dict) and value.get("gateId") == request_id
        ), None)
        if (gate is None or not isinstance(gate.get("turnId"), str)
                or not gate["turnId"]):
            raise DeliveryRejected("no original Codex approval for this gateId",
                                   short=owner.short, refusal="request-id-mismatch")
        runtime = codex_runtime.daemon_rpc({
            "op": "status", "short": owner.worker_id,
            "rpcGeneration": constants.CODEXD_RPC_GENERATION,
        }, timeout=min(0.75, timeout if timeout is not None else 0.75))
        if (runtime.get("ok") is not True
                or runtime.get("transport") != RUNTIME_SERVED
                or runtime.get("loaded") is not True):
            raise SessionUnavailable("Codex approval is unavailable; check worker status",
                                     short=owner.short)
    store.authorize_native_action(owner.worker, "answer")
    intent = store.begin_native_intent(
        owner.worker, action="answer", family="answer",
        operation_id=response_id, intent_sha256=digest,
    )
    if intent.replayed:
        return _original_input_public(
            store, owner, kind, request_id, response_id,
            store.read_receipt(owner.worker, "answer", response_id),
            notification=previous_notification() if notify_parent else None,
        )
    notification = None
    if notify_parent:
        # First original input intent is PREPARED; use its unchanged digest.
        from ccx.notify.daemon import ensure_notify_daemon
        operation = {
            "kind": (f"claude-input-{kind}" if kind in ("answer", "respond")
                     else f"{owner.engine}-input-decision"),
            "gateId": request_id, "responseId": response_id,
            "nativeSourceId": native_id, "intentSha256": digest,
            "selectedTurnRef": (input_turn_ref if owner.engine == "claude"
                                else f"ccx-turn:codex:v1:{gate['turnId']}"),
        }
        if kind in ("answer", "respond"):
            operation["payloadSha256"] = _digest(payload)
        else:
            operation["decision"] = payload["decision"]
            if owner.engine == "claude":
                operation["reason"] = payload["reason"]
            else:
                operation["turnId"] = gate["turnId"]
        notification = create_parent_notification(
            parent=parent, worker_engine=owner.engine, worker_id=owner.worker_id,
            worker_name=rec.get("name"), source="input", settle=settle,
            operation=operation,
        )
        ensure_notify_daemon()

    def native_submit(budget: float) -> dict:
        try:
            if owner.engine == "claude":
                if kind == "answer":
                    _CLIENT.answer(
                        owner.worker, text_answers=pairs, request_id=request_id,
                        response_id=response_id, wait=False,
                        fallback_to_send=False, timeout=budget,
                    )
                elif kind == "decide":
                    verb = (_CLIENT.approve if payload["decision"] == "approve"
                            else _CLIENT.reject)
                    verb(
                        owner.worker, request_id=request_id,
                        response_id=response_id, reason=payload["reason"],
                        wait=False, timeout=budget,
                    )
                else:
                    _CLIENT.respond_elicitation(
                        owner.worker, payload["action"], payload["content"],
                        request_id=request_id, response_id=response_id,
                        wait=False, timeout=budget,
                    )
            else:
                verb = "approve" if payload["decision"] == "approve" else "reject"
                _CODEX_CLIENT._decision(
                    owner.worker, verb, request_id, budget,
                    response_id=response_id, expected_turn_id=gate["turnId"],
                )
        except InvalidRequest as error:
            # Accepted Python validators raise this before native IPC.
            return {"noEffect": True, "definitive": True, "reason": str(error)}
        except DeliveryRejected as error:
            # Codex's private validator only rejects after definitive served
            # refusal; Claude's exact request-id guard is pre-request.
            if (owner.engine == "codex"
                    or getattr(error, "reason", None) == "request id mismatch"):
                return {"noEffect": True, "definitive": True, "reason": str(error)}
            return {"ambiguous": True}
        except Exception:
            # Connection reset, unreachable, timeout and SessionUnavailable
            # may occur after sendall. Never call those definite no-effect.
            return {"ambiguous": True}
        return {"accepted": True}

    reply = None
    try:
        reply = store.dispatch_native_intent_once(
            intent, bounded_submit=native_submit,
            dispatch_budget_s=min(2.0, timeout if timeout is not None else 2.0),
        )
    except NativeEffectIndeterminate:
        pass
    if reply and reply.get("accepted") is True:
        sha = _digest({
            "workerId": owner.worker_id, "kind": kind,
            "nativeSource": source, "requestId": request_id,
            "responseId": response_id, "positive": "original-native-ack",
        })
        receipt = store.settle_native_intent(
            intent, state="accepted", native_evidence_sha256=sha,
        )
    elif reply and (reply.get("noEffect") is True
                    and reply.get("definitive") is True):
        sha = _digest({
            "workerId": owner.worker_id, "kind": kind,
            "nativeSource": source, "requestId": request_id,
            "responseId": response_id, "noEffect": reply.get("reason"),
        })
        receipt = store.settle_native_intent(
            intent, state="rejected", native_evidence_sha256=sha,
        )
        if notification is not None:
            cancel_parent_notification(
                notification["eventId"],
                str(reply.get("reason") or "original native input definitively refused"),
            )
            notification = read_parent_notification(notification["eventId"]) or notification
    else:
        receipt = store.read_receipt(owner.worker, "answer", response_id)
        proof = _original_input_evidence(
            store, owner, kind, request_id, response_id, payload, native_id,
        )
        if proof and receipt["state"] == "submitted-unknown":
            receipt = store.settle_native_intent(
                intent, state="accepted", native_evidence_sha256=proof[1],
            )
    store.revalidate()
    return _original_input_public(
        store, owner, kind, request_id, response_id, receipt,
        evidence=proof[0] if proof else None, notification=notification,
    )


def answer_input(
    target: str, *, request_id: str, response_id: str,
    answers: dict[str, str], timeout: float | None = 90,
    notify_parent: bool = False,
) -> dict[str, Any]:
    return _original_input(
        target, kind="answer", request_id=request_id, response_id=response_id,
        payload=_validate_answer_map(answers), timeout=timeout,
        notify_parent=notify_parent,
    )


def decide_input(
    target: str, *, request_id: str, response_id: str,
    decision: str, reason: str | None = None,
    timeout: float | None = 90,
    notify_parent: bool = False, settle: bool = False,
) -> dict[str, Any]:
    if decision not in ("approve", "reject"):
        raise InvalidRequest("decision must be approve or reject")
    if reason is not None and (not isinstance(reason, str) or not reason):
        raise InvalidRequest("reason must be a nonempty string")
    from ccx import constants
    from ccx.store.root import require_ccx_active

    owner = require_ccx_active(constants.CCX_ROOT).resolve(target)
    if settle and not notify_parent:
        raise InvalidRequest("--settle requires --notify-parent")
    if notify_parent and settle and owner.engine != "codex":
        raise InvalidRequest("Claude permission notification does not support --settle")
    if owner.engine == "codex" and reason is not None:
        raise CapabilityUnsupported(
            "Codex approvals do not accept a rationale",
            engine="codex", capability="decision_reason",
        )
    return _original_input(
        owner.worker, kind="decide", request_id=request_id,
        response_id=response_id, payload={"decision": decision, "reason": reason},
        timeout=timeout, notify_parent=notify_parent, settle=settle,
    )


def respond_input(
    target: str, *, request_id: str, response_id: str,
    action: str, content: dict[str, Any] | None = None,
    timeout: float | None = 90, notify_parent: bool = False,
) -> dict[str, Any]:
    if action not in ELICITATION_ACTIONS:
        raise InvalidRequest("action must be accept, decline or cancel")
    if content is not None and (not isinstance(content, dict) or action != "accept"):
        raise InvalidRequest("content is only valid as an object with action=accept")
    return _original_input(
        target, kind="respond", request_id=request_id, response_id=response_id,
        payload={"action": action, "content": content}, timeout=timeout,
        notify_parent=notify_parent,
    )
