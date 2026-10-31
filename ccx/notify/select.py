from __future__ import annotations
import hashlib
import time
import uuid
from typing import Any
from ccx.errors import InvalidRequest, SessionUnavailable
from ccx.ops.turn_reports import codex_native_turn_evidence as _codex_native_turn_evidence


def _controller_notification_shape(
    value: dict[str, Any], worker: dict[str, Any], operation: dict[str, Any],
) -> None:
    """Static v3 controller subject, independent of any later run state."""
    from ccx.store.identity import IdentityError, canonical_worker_id

    try:
        canonical_worker_id(worker.get("workerId"))
        canonical_worker_id(operation.get("attemptToken"))
        event = uuid.UUID(value.get("eventId"))
    except (IdentityError, TypeError, ValueError, AttributeError):
        raise SessionUnavailable("controller notification identity invalid") from None
    if (set(worker) != {"engine", "workerId", "generation", "name"}
            or set(operation) != {"kind", "attempt", "attemptToken", "startupDeadlineMs"}
            or operation.get("kind") != "controller-attempt"
            or type(operation.get("attempt")) is not int or operation["attempt"] < 1
            or type(operation.get("startupDeadlineMs")) is not int
            or operation["startupDeadlineMs"] <= 0
            or str(event) != value.get("eventId") or event.version != 4
            or value.get("registrationKeySha256") is not None
            or value.get("settlementMode") is not None):
        raise SessionUnavailable("controller notification identity invalid")


def _controller_attempt_at_arm(run_id: str, operation: dict[str, Any], source: str) -> None:
    """Check controller run/layout/slot once, before publishing an event."""
    from ccx.controllers import store as controller_store
    from ccx.errors import ControllerError

    bootstrap = controller_store.bootstrap_root() / run_id
    run_dir = (bootstrap if bootstrap.is_dir() else next(
        (path for path in controller_store._all_run_dirs() if path.name == run_id), None
    ))
    if run_dir is None:
        raise SessionUnavailable("controller run not found for notification")
    try:
        record = controller_store.read_json(controller_store.run_record_path(run_dir))
        controller_store.require_operation_journal_layout(run_dir, record)
    except (ControllerError, OSError) as exc:
        raise SessionUnavailable(f"controller notification run unavailable: {exc}") from exc
    if record.get("runId") != run_id:
        raise SessionUnavailable("controller notification run identity changed")
    number, token = operation["attempt"], operation["attemptToken"]
    attempts = record.get("attempts") or []
    selected = next((item for item in attempts
                     if isinstance(item, dict) and item.get("number") == number), None)
    if selected is not None and selected.get("attemptToken") == token:
        if source != "controller.run" or number == 1:
            return
    elif (selected is None and source == "controller.resume" and number == len(attempts) + 1
          and attempts and isinstance(attempts[-1], dict)
          and attempts[-1].get("number") == len(attempts)
          and (record.get("lifecycle") or {}).get("state")
          in controller_store.TERMINAL_LIFECYCLES):
        # The caller holds resume.lock and publishes this exact token next.
        return
    raise InvalidRequest("controller notification attempt or token is not the selected start")


def _original_notification_parent(store, parent: dict[str, Any], worker_id: str | None):
    """The existing exact Codex return address; never infer one from a child."""
    if parent["kind"] == "codex-native-thread":
        native_thread = parent.get("threadId")
        if (set(parent) != {"kind", "threadId"}
                or not isinstance(native_thread, str) or not native_thread
                or native_thread != native_thread.strip() or "\x00" in native_thread
                or len(native_thread.encode("utf-8")) > 1024):
            raise SessionUnavailable("interactive parent return address malformed")
        # Managed-native inventory exclusion is proven when the caller arms.
        return dict(parent)
    parent_id = parent.get("workerId")
    parent_owner = store.resolve(f"codex:{parent_id}")
    if (parent_owner.worker_id != parent_id or parent_id == worker_id
            or parent.get("generation") != store.generation):
        raise SessionUnavailable("Notification recipient is invalid; verify the parent worker")
    store.authorize_native_action(parent_owner.worker, "send")
    parent_native = (store.record(parent_owner.worker).get("native") or {})
    if (parent.get("threadId") != parent_native.get("threadId")
            or parent_native.get("threadSource") != store.codex_source(parent_owner.worker)):
        raise SessionUnavailable("Notification recipient changed; create a new notification")
    return parent_owner.worker


def _compact_effect_receipt(store, child: str, engine: str, worker_id: str,
                            operation: dict[str, Any]) -> dict[str, Any] | None:
    """Bind a compact event to its original full-UUID native source and intent."""
    from ccx.ops.compact_ops import compact_intent_digest as claude_digest
    from ccx.codex.ops import compact_intent_digest as codex_digest

    original = operation["nativeSourceId"]
    operation_id = operation["operationId"]
    record = store.record(child)
    native = record.get("native") or {}
    activation = store.native_activation(child)
    name = "sessionId" if engine == "claude" else "threadId"
    source = (f"claude-session:{original}" if engine == "claude"
              else store.codex_source(child))
    if (native.get(name) != original
            or activation.get("state") != "confirmed"
            or activation.get("source") != source
            or (activation.get("nativeBinding") or {}).get(name) != original
            or (engine == "codex" and native.get("threadSource") != source)):
        raise SessionUnavailable("Compaction worker changed; inspect its operation")
    family = "compact" if engine == "claude" else "codex-compact"
    digest = (claude_digest(worker_id, generation=store.generation,
                            native_session_id=original, operation_id=operation_id)
              if engine == "claude" else codex_digest(
                  worker_id, store.generation, original, source, operation_id))
    receipt = store.read_receipt(child, family, operation_id)
    if receipt is not None and (
            receipt.get("workerId") != worker_id
            or receipt.get("engine") != engine
            or receipt.get("generation") != store.generation
            or receipt.get("operationId") != operation_id
            or receipt.get("family") != family
            or receipt.get("action") != "compact"
            or receipt.get("intentSha256") != digest):
        raise SessionUnavailable("Compaction delivery does not match the worker; inspect the operation")
    return receipt


def _original_notification_registration(value: dict[str, Any]) -> tuple[Any, str | dict[str, Any], str]:
    """Verify the typed notification subject and original Codex parent.

    The old short-keyed registration layout is not a routing fallback; an
    interactive native return address is never a managed worker target.
    """
    from ccx import constants
    from ccx.store.root import require_ccx_active

    store = require_ccx_active(constants.CCX_ROOT)
    parent = value.get("parent") if isinstance(value.get("parent"), dict) else {}
    worker = value.get("worker") if isinstance(value.get("worker"), dict) else {}
    operation = value.get("operation") if isinstance(value.get("operation"), dict) else {}
    worker_id = worker.get("workerId")
    engine = worker.get("engine")
    source = value.get("source")
    if (value.get("version") != 3 or value.get("generation") != store.generation
            or parent.get("kind") not in {"ccx-codex", "codex-native-thread"}
            or engine not in {"codex", "claude", "controller"}
            or worker.get("generation") != store.generation
            or source not in ({"controller.run", "controller.resume", "controller.notify"}
                              if engine == "controller" else {"send", "notify", "new", "input", "compact"})
            or value.get("settlementMode") not in {None, "orchestration"}
            or (value.get("settlementMode") == "orchestration" and engine != "codex")):
        raise SessionUnavailable("Notification record is incompatible; create a new notification")
    if engine == "controller":
        _controller_notification_shape(value, worker, operation)
        # The independently minted controller run UUID is not a worker owner.
        return store, _original_notification_parent(store, parent, None), f"controller:{worker_id}"
    if source == "send":
        from ccx.claude.delivery import valid_delivery_id
        if (operation.get("kind") != f"{engine}-turn"
                or not valid_delivery_id(operation.get("deliveryId"))):
            raise SessionUnavailable("original send notification has no owned delivery ID")
        key = f"full-uuid-{engine}-send:{store.generation}:{worker_id}:{operation['deliveryId']}"
        if (value.get("eventId") != str(uuid.uuid5(uuid.NAMESPACE_URL, "ccx-notify:" + key))
                or value.get("registrationKeySha256") != hashlib.sha256(key.encode()).hexdigest()):
            raise SessionUnavailable("notification event ID differs from original semantic key")
    elif source == "compact":
        operation_id = operation.get("operationId")
        original = operation.get("nativeSourceId")
        if (set(operation) != {"kind", "operationId", "nativeSourceId"}
                or operation.get("kind") != f"{engine}-compact"
                or not isinstance(operation_id, str) or not 0 < len(operation_id) <= 128
                or "\x00" in operation_id
                or not isinstance(original, str) or not original):
            raise SessionUnavailable("original compact notification operation malformed")
        key = f"full-uuid-{engine}-compact:{store.generation}:{worker_id}:{operation_id}"
        if (value.get("eventId") != str(uuid.uuid5(
                uuid.NAMESPACE_URL, "ccx-notify:" + key))
                or value.get("registrationKeySha256") != hashlib.sha256(
                    key.encode()).hexdigest()):
            raise SessionUnavailable("original compact notification event differs from operation")
    elif source == "input":
        from ccx.store.root import _HASH
        key = f"full-uuid-{engine}-input:{store.generation}:{worker_id}:{operation.get('gateId')}"
        hashed_input = (engine == "claude"
                        and operation.get("kind") in {"claude-input-answer", "claude-input-respond"})
        fields = {"kind", "gateId", "responseId", "nativeSourceId",
                  "intentSha256", "selectedTurnRef"}
        fields |= ({"payloadSha256"} if hashed_input
                   else {"decision", "reason" if engine == "claude" else "turnId"})
        reason = operation.get("reason")
        ref = operation.get("selectedTurnRef")
        if (set(operation) != fields
                or (not hashed_input and (
                    operation.get("kind") != f"{engine}-input-decision"
                    or operation.get("decision") not in {"approve", "reject"}))
                or (hashed_input and (
                    not isinstance(operation.get("payloadSha256"), str)
                    or not _HASH.fullmatch(operation["payloadSha256"])))
                or any(not isinstance(operation.get(field), str) or not operation[field]
                       for field in ("gateId", "responseId", "nativeSourceId",
                                     "intentSha256"))
                or (engine == "claude"
                    and (reason is not None and (not isinstance(reason, str) or not reason)
                         or not isinstance(ref, str)
                         or not ref.startswith("ccx-turn:claude:v1:")
                         or len(ref) <= len("ccx-turn:claude:v1:")))
                or (engine == "codex"
                    and (not isinstance(operation.get("turnId"), str)
                         or not operation["turnId"]
                         or ref != f"ccx-turn:codex:v1:{operation['turnId']}"))
                or value.get("eventId") != str(uuid.uuid5(
                    uuid.NAMESPACE_URL, "ccx-notify:" + key))
                or value.get("registrationKeySha256") != hashlib.sha256(key.encode()).hexdigest()):
            raise SessionUnavailable("original input notification identity invalid")
    elif source == "new":
        # One original positive native creation, no model send or latest-turn
        # selection. The event is keyed to the child, not the parent/mode.
        key = f"full-uuid-{engine}-new:{store.generation}:{worker_id}:promptless"
        if (engine not in {"claude", "codex"} or type(operation.get("expectTurn")) is not bool
                or operation != {"kind": f"{engine}-stable", "expectTurn": False,
                                 "selectionMode": "status-only", "stableState": "idle"}
                or value.get("eventId") != str(uuid.uuid5(
                    uuid.NAMESPACE_URL, "ccx-notify:" + key))
                or value.get("registrationKeySha256") != hashlib.sha256(key.encode()).hexdigest()):
            raise SessionUnavailable("original promptless creation identity invalid")
    else:
        # Independent observation arms use random event identity. They are not
        # fresh send attempts and cannot rekey an earlier delivery's event.
        try:
            event = uuid.UUID(value.get("eventId"))
        except (ValueError, TypeError, AttributeError):
            raise SessionUnavailable("standalone notification event UUID malformed") from None
        if (str(event) != value.get("eventId") or event.version != 4
                or value.get("registrationKeySha256") is not None):
            raise SessionUnavailable("standalone observation needs its own UUIDv4 event")
        kind = operation.get("kind")
        selection = operation.get("selectionMode")
        if kind == f"{engine}-compact" and selection == "active-compact":
            token = operation.get("operationId")
            original = operation.get("nativeSourceId")
            if (set(operation) != {"kind", "selectionMode", "operationId",
                                   "nativeSourceId"}
                    or not isinstance(token, str) or not 0 < len(token) <= 128
                    or "\x00" in token or not isinstance(original, str) or not original):
                raise SessionUnavailable("standalone compact observation identity invalid")
        elif kind == f"{engine}-stable":
            if (selection != "status-only"
                    or operation.get("stableState") not in {
                        "idle", "idle-killed", "dead", "crashed", "budget-stopped"
                    } or operation.get("selectedTurnRef") is not None
                    or operation.get("deliveryId") is not None):
                raise SessionUnavailable("standalone stable observation identity invalid")
        elif (kind == "claude-next-turn" and engine == "claude"
              and selection == "future"):
            from ccx.ops.event_feed import decode_cursor
            cursor = operation.get("armCursor")
            ref = operation.get("selectedTurnRef")
            if (set(operation) != {"kind", "selectionMode", "nativeSourceId",
                                   "armCursor", "selectedTurnRef"}
                    or not isinstance(cursor, str)
                    or (ref is not None and (not isinstance(ref, str)
                        or not ref.startswith("ccx-turn:claude:v1:")
                        or len(ref) <= len("ccx-turn:claude:v1:")))):
                raise SessionUnavailable("future Claude notification identity invalid")
            decode_cursor(cursor, engine="claude", short=worker_id)
        elif (kind == f"{engine}-observed-turn"
              and selection in {"active", "last-settled"}):
            from ccx.claude.delivery import valid_delivery_id
            ref = operation.get("selectedTurnRef")
            if (not isinstance(ref, str)
                    or not ref.startswith(f"ccx-turn:{engine}:v1:")
                    or len(ref) <= len(f"ccx-turn:{engine}:v1:")):
                raise SessionUnavailable("standalone selected original turn identity invalid")
            if operation.get("nativeProof") == "original-native-turn":
                fields = {"kind", "selectionMode", "selectedTurnRef",
                          "nativeProof", "nativeSourceId"}
                if engine == "codex":
                    fields.add("turnId")
                if (set(operation) != fields
                        or not isinstance(operation.get("nativeSourceId"), str)
                        or not operation["nativeSourceId"]
                        or (engine == "codex" and operation.get("turnId") !=
                            ref[len("ccx-turn:codex:v1:"):])):
                    raise SessionUnavailable("receiptless native turn identity invalid")
            elif (operation.get("nativeProof") is not None
                  or not valid_delivery_id(operation.get("deliveryId"))):
                raise SessionUnavailable("standalone selected original delivery invalid")
        else:
            raise SessionUnavailable("standalone observation operation unsupported")
    # The child is always an exactly owned original UUID; only the caller's
    # return-address kind varies. No native parent ID is a worker target.
    worker_owner = store.resolve(f"{engine}:{worker_id}")
    if worker_owner.worker_id != worker_id:
        raise SessionUnavailable("Notification worker is invalid; verify its handle")
    # This is a read-only original source/binding gate. It is lifecycle
    # independent and permits stopped workers, but never terminalOnly or
    # pending provider bindings; no model native action is performed.
    store.authorize_native_action(worker_owner.worker, "send")
    if source == "compact" or (source == "notify"
            and operation.get("kind") == f"{engine}-compact"):
        receipt = _compact_effect_receipt(
            store, worker_owner.worker, engine, worker_id, operation)
        if source == "notify" and (receipt is None or receipt.get("state") not in {
                "submitted-unknown", "accepted"}):
            raise SessionUnavailable("standalone compact has no authenticated original effect")
    if source == "input":
        # Lazy import avoids a module-load cycle, and one original input
        # digest layout is shared by Claude, Codex and the public protocol.
        from ccx.ops.input import _input_intent_digest
        record = store.record(worker_owner.worker)
        native = record.get("native") or {}
        source_id = operation["nativeSourceId"]
        expected_source = (f"claude-session:{source_id}" if engine == "claude"
                           else store.codex_source(worker_owner.worker))
        hashed_input = (engine == "claude"
                        and operation["kind"] in {"claude-input-answer", "claude-input-respond"})
        expected_sha = _input_intent_digest(
            worker_id=worker_id, engine=engine, generation=store.generation,
            native_id=source_id, native_source=expected_source,
            kind=(operation["kind"].removeprefix("claude-input-") if hashed_input else "decide"),
            request_id=operation["gateId"], response_id=operation["responseId"],
            **({"payload_sha256": operation["payloadSha256"]} if hashed_input else
               {"payload": {"decision": operation["decision"],
                            "reason": operation["reason"] if engine == "claude" else None}}),
        )
        intent = store.read_receipt(worker_owner.worker, "answer", operation["responseId"])
        if (native.get("sessionId" if engine == "claude" else "threadId") != source_id
                or (engine == "codex" and native.get("threadSource") != expected_source)
                or not isinstance(intent, dict)
                or intent.get("workerId") != worker_id
                or intent.get("engine") != engine
                or intent.get("generation") != store.generation
                or intent.get("family") != "answer"
                or intent.get("action") != "answer"
                or intent.get("operationId") != operation["responseId"]
                or operation["intentSha256"] != expected_sha
                or intent.get("intentSha256") != expected_sha):
            raise SessionUnavailable("original input receipt/source/decision proof invalid")
        if engine == "claude":
            from ccx.claude.managed_turns import report_window_for_turn_ref
            window, available = report_window_for_turn_ref(
                worker_id, operation["selectedTurnRef"])
            offset = window.get("startOffset") if isinstance(window, dict) else None
            if not available or type(offset) is not int or offset < 0:
                raise SessionUnavailable("original Claude input R lacks native start/window proof")
    if source == "notify" and operation["kind"] == "claude-next-turn":
        native = store.record(worker_owner.worker).get("native") or {}
        if (not isinstance(operation.get("nativeSourceId"), str)
                or not operation["nativeSourceId"]
                or operation["nativeSourceId"] != native.get("sessionId")):
            raise SessionUnavailable("future Claude native source changed")
        ref = operation.get("selectedTurnRef")
        if ref is not None and not _future_first_ref_matches(
                worker_id, operation["armCursor"], ref):
            raise SessionUnavailable("future Claude selected R is not first post-arm start")
    elif (source == "notify" and operation["kind"] == f"{engine}-observed-turn"
          and operation.get("nativeProof") == "original-native-turn"):
        record = store.record(worker_owner.worker)
        native = record.get("native") or {}
        original = operation["nativeSourceId"]
        if engine == "claude":
            from ccx.claude.managed_turns import report_window_for_turn_ref
            window, available = report_window_for_turn_ref(
                worker_id, operation["selectedTurnRef"])
            start = window.get("startOffset") if isinstance(window, dict) else None
            valid = (native.get("sessionId") == original and available
                     and type(start) is int and start >= 0)
        else:
            valid = (native.get("threadId") == original and
                     native.get("threadSource") == store.codex_source(worker_owner.worker))
            if valid:
                valid = _codex_native_turn_evidence(
                    worker_owner.worker, record, original, operation["turnId"]) is not None
        if not valid:
            raise SessionUnavailable("receiptless selected original native source or turn proof changed")
    elif source == "notify" and operation["kind"] == f"{engine}-observed-turn":
        token = operation["deliveryId"]
        if engine == "codex":
            from ccx.codex import delivery as codex_delivery
            receipt = codex_delivery.read_receipt(worker_owner.worker, token)
            valid = (isinstance(receipt, dict) and receipt.get("state") == "accepted"
                     and codex_delivery.route(receipt) == "new_turn"
                     and codex_delivery.placement(receipt) == "new_turn"
                     and receipt.get("turnId") == operation.get("turnId")
                     and operation["selectedTurnRef"] == codex_delivery.turn_ref_for(receipt["turnId"])
                     and all(receipt.get(field) == operation.get(field)
                             for field in ("requestedMode", "promptSha256",
                                           "callerMetadata", "clientUserMessageId")))
        else:
            from ccx.claude import delivery as claude_delivery
            receipt = claude_delivery.read_receipt(worker_owner.worker, token)
            valid = (isinstance(receipt, dict) and receipt.get("state") == "accepted"
                     and receipt.get("route") == "new_turn"
                     and receipt.get("placement") == "new_turn"
                     and receipt.get("turnRef") == operation["selectedTurnRef"]
                     and all(receipt.get(field) == operation.get(field)
                             for field in ("requestedMode", "promptSha256",
                                           "promptByteCount", "callerMetadata", "cap")))
        if not valid or receipt.get("requestedMode") not in {"new_turn_only", "auto"}:
            raise SessionUnavailable("selected turn differs from its original opener receipt")
    if engine == "claude":
        native = store.record(worker_owner.worker).get("native") or {}
        if (not isinstance(native.get("sessionId"), str)
                or not native["sessionId"]):
            raise SessionUnavailable("original Claude notification child native session is unbound")
    return store, _original_notification_parent(store, parent, worker_id), worker_owner.worker


def _active_compact_selection(store, owner, record: dict[str, Any]) -> dict[str, Any] | None:
    """Observe only an already-active, original-effect-backed native compact."""
    from ccx.errors import CcxError, OperationConflict
    from ccx.store.root import GenerationError

    engine, child, wid = owner.engine, owner.worker, owner.worker_id
    if engine == "claude":
        from ccx.ops.compact_ops import active_operation
        try:
            active = active_operation(wid)
        except (OSError, ValueError, RuntimeError) as error:
            raise SessionUnavailable(f"Claude compact journal unavailable: {error}") from error
        if active is None:
            return None
        token = active.get("operation_id")
    else:
        active = record.get("activeCompact")
        if active is None:
            return None
        token = active.get("operationId") if isinstance(active, dict) else None
    native = record.get("native") or {}
    native_id = native.get("sessionId" if engine == "claude" else "threadId")
    if (not isinstance(token, str) or not 0 < len(token) <= 128 or "\x00" in token
            or not isinstance(native_id, str) or not native_id):
        raise OperationConflict("Compaction operation cannot be verified; inspect the worker")
    operation = {"kind": f"{engine}-compact", "selectionMode": "active-compact",
                 "operationId": token, "nativeSourceId": native_id}
    receipt = _compact_effect_receipt(store, child, engine, wid, operation)
    if not isinstance(receipt, dict) or receipt.get("state") not in {
            "submitted-unknown", "accepted"}:
        raise OperationConflict("Compaction has no recorded submission; inspect its operation")
    try:
        if engine == "claude":
            from ccx.ops.compact import compact_status
            status = compact_status(child, operation_id=token, publish=False).get("operation_status")
            valid = status in {"accepted", "pasted", "submitted"}
        else:
            from ccx.codex.ops import compact_observation
            valid = compact_observation(child, token).get("status") == "submitted"
    except (CcxError, GenerationError, OSError, ValueError):
        valid = False
    if not valid:
        raise OperationConflict("Compaction outcome is unknown; inspect the operation")
    # Freeze exactly the original ID, never a later compact or a model-turn R.
    latest = store.record(child)
    if engine == "claude":
        from ccx.ops.compact_ops import active_operation
        try:
            current = active_operation(wid)
        except (OSError, ValueError, RuntimeError) as error:
            raise SessionUnavailable(f"Claude compact changed during selection: {error}") from error
        same = isinstance(current, dict) and current.get("operation_id") == token
    else:
        current = latest.get("activeCompact")
        same = isinstance(current, dict) and current.get("operationId") == token
    if (not same or (latest.get("native") or {}).get(
            "sessionId" if engine == "claude" else "threadId") != native_id):
        raise OperationConflict("Compaction changed during selection; retry notification")
    store.revalidate()
    return operation


def _claude_prompt_opening(child: str) -> bool:
    """A Claude send is still being pasted (owned, not yet accepted)."""
    from ccx.claude import delivery as claude_delivery
    try:
        return any(row.get("state") == "owned"
                   for row in claude_delivery.iter_receipts(child))
    except (claude_delivery.ClaudeDeliveryError, OSError):
        return False


def select_current_turn(target: str) -> dict[str, Any]:
    """Read one original managed turn or positively proven stable no-turn."""
    from ccx.claude import delivery as claude_delivery
    from ccx.codex import delivery as codex_delivery
    from ccx import constants
    from ccx.errors import OperationConflict
    from ccx.ops.wait import wait_worker
    from ccx.store.root import require_ccx_active

    store = require_ccx_active(constants.CCX_ROOT)
    owner = store.resolve(target)
    child = owner.worker
    store.authorize_native_action(child, "send")  # original binding check only
    record = store.record(child)
    compact = _active_compact_selection(store, owner, record)
    if compact is not None:
        return {"worker": child, "worker_id": owner.worker_id,
                "engine": owner.engine, "name": record.get("name"),
                "state": "compacting", "operation": compact}
    # One bounded read: a zero-second call gives the live daemon barely .01s
    # and may falsely label a healthy just-settled worker inconclusive.
    snapshot = wait_worker(child, timeout=.75, expected_generation=store.generation)
    # Right after `new/send --no-wait` the worker is idle while the prompt is
    # still being delivered; wait briefly for that turn to open instead of
    # selecting the previous one.
    settle_by = time.monotonic() + 15
    while (snapshot.get("state") == "idle" and snapshot.get("active_turn_ref") is None
           and (snapshot.get("status") == "timeout"
                or owner.engine == "claude" and _claude_prompt_opening(child))
           and time.monotonic() < settle_by):
        time.sleep(.25)
        snapshot = wait_worker(child, timeout=.75, expected_generation=store.generation)
    if (owner.engine == "claude" and snapshot.get("active_turn_ref") is None
            and _claude_prompt_opening(child)):
        raise SessionUnavailable(
            "the last message to this worker is still being delivered; "
            "retry notify once its turn has started")
    state = snapshot.get("state")
    active = snapshot.get("active_turn_ref")
    live_idle = (snapshot.get("status") == "idle"
                 and snapshot.get("reason") == "worker is idle")
    if owner.engine == "claude" and (active is not None or live_idle):
        # The host can be compacting even before the journal is visible.
        from ccx.rpc.rpc import _rpc
        try:
            status = _rpc(child, {"op": "status"}, timeout=.5)
        except (OSError, ValueError, TimeoutError) as error:
            raise SessionUnavailable(f"Claude compact status read unavailable: {error}") from error
        if (status.get("ok") is not True or status.get("compacting") is not False
                or status.get("compactJournalError")):
            raise OperationConflict("Claude compact state unavailable or active")
        if status.get("activeTurnRef") != active:
            raise SessionUnavailable("original Claude active turn changed while selecting")
    if state in {"busy", "waiting"}:
        if not isinstance(active, str) or not active:
            raise SessionUnavailable("busy/waiting worker has no proven original active turn")
        ref, selection = active, "active"
    elif state in {"idle", "idle-killed", "dead", "crashed", "budget-stopped"}:
        if state == "idle" and active is not None:
            raise SessionUnavailable("idle worker retains an untrusted active turn")
        if state != "idle" and (record.get("lifecycle") or {}).get("state") != state:
            raise SessionUnavailable("terminal worker state differs from durable source")
        settled = snapshot.get("last_settled_turn")
        ref = settled.get("turn_ref") if isinstance(settled, dict) else None
        selection = "last-settled"
    else:
        raise SessionUnavailable("worker state cannot select an original model turn")

    receipt = None
    if isinstance(ref, str) and ref.startswith(f"ccx-turn:{owner.engine}:v1:"):
        if owner.engine == "codex":
            turn_id = ref[len("ccx-turn:codex:v1:"):]
            try:
                receipt = codex_delivery.receipt_for_turn(child, turn_id)
            except (codex_delivery.DeliveryReceiptError, OSError) as error:
                raise SessionUnavailable(f"Codex original opener receipt untrusted: {error}") from error
            if (not isinstance(receipt, dict) or receipt.get("state") != "accepted"
                    or codex_delivery.route(receipt) != "new_turn"
                    or codex_delivery.placement(receipt) != "new_turn"
                    or receipt.get("turnId") != turn_id):
                receipt = None
        else:
            # Claude's existing receipt inventory is best effort. Match only
            # unique eligible valid openers, then strictly reread that token.
            try:
                candidates = [row for row in claude_delivery.iter_receipts(child)
                              if row.get("state") == "accepted"
                              and row.get("route") == "new_turn"
                              and row.get("placement") == "new_turn"
                              and row.get("turnRef") == ref]
                if len(candidates) == 1:
                    receipt = claude_delivery.read_receipt(child, candidates[0]["deliveryId"])
            except (claude_delivery.ClaudeDeliveryError, OSError) as error:
                raise SessionUnavailable(f"Claude original opener receipt untrusted: {error}") from error
            if receipt is not None:
                if (receipt.get("turnRef") != ref
                        or receipt.get("state") != "accepted"
                        or receipt.get("route") != "new_turn"
                        or receipt.get("placement") != "new_turn"):
                    receipt = None
    if receipt is None:
        native = record.get("native") or {}
        original = native.get("sessionId" if owner.engine == "claude" else "threadId")
        proven = False
        if isinstance(ref, str) and isinstance(original, str) and original:
            if owner.engine == "claude":
                from ccx.claude.managed_turns import report_window_for_turn_ref
                window, available = report_window_for_turn_ref(owner.worker_id, ref)
                start = window.get("startOffset") if isinstance(window, dict) else None
                proven = (available and type(start) is int and start >= 0)
            elif native.get("threadSource") == store.codex_source(owner.worker):
                proven = _codex_native_turn_evidence(child, record, original,
                    ref[len("ccx-turn:codex:v1:"):]) is not None
                if proven and state in {"busy", "waiting"}:
                    from ccx.codex import runtime as codex_runtime
                    served = codex_runtime.observe_worker(owner.worker_id, timeout=.5)
                    reply = served.get("reply") or {}
                    proven = (served.get("daemon_served") is True and
                              reply.get("ok") is True and reply.get("loaded") is True
                              and reply.get("activeTurnId") == ref[len("ccx-turn:codex:v1:"):])
        if proven:
            operation = {"kind": f"{owner.engine}-observed-turn",
                         "selectionMode": selection, "selectedTurnRef": ref,
                         "nativeProof": "original-native-turn", "nativeSourceId": original}
            if owner.engine == "codex":
                operation["turnId"] = ref[len("ccx-turn:codex:v1:"):]
        else:
            if state in {"busy", "waiting"}:
                raise SessionUnavailable("current original turn lacks an eligible opening receipt or native proof")
            if state == "idle" and not live_idle:
                raise SessionUnavailable(
                    "Cannot notify yet: the worker's idle state could not be confirmed. "
                    "Check its status and retry."
                )
            operation = {"kind": f"{owner.engine}-stable",
                         "selectionMode": "status-only", "selectedTurnRef": None,
                         "stableState": state}
    else:
        operation = {"kind": f"{owner.engine}-observed-turn",
                     "selectionMode": selection, "selectedTurnRef": ref,
                     "deliveryId": receipt["deliveryId"],
                     "requestedMode": receipt.get("requestedMode"),
                     "promptSha256": receipt.get("promptSha256"),
                     "callerMetadata": receipt.get("callerMetadata")}
        if owner.engine == "codex":
            operation.update(turnId=receipt["turnId"],
                             clientUserMessageId=receipt.get("clientUserMessageId"))
        else:
            operation.update(promptByteCount=receipt.get("promptByteCount"),
                             cap=receipt.get("cap"))
    store.revalidate()
    return {"worker": child, "worker_id": owner.worker_id,
            "engine": owner.engine, "name": record.get("name"),
            "state": state, "operation": operation}


def select_future_turn(target: str) -> dict[str, Any]:
    """Arm one future Claude turn only from a positively served idle host."""
    from ccx import constants
    from ccx.ops.compact_ops import active_operation
    from ccx.ops.event_feed import read_page, decode_cursor
    from ccx.store.paths import events_ndjson_path
    from ccx.ops.wait import wait_worker
    from ccx.rpc.rpc import _rpc
    from ccx.store.root import require_ccx_active

    store = require_ccx_active(constants.CCX_ROOT)
    owner = store.resolve(target)
    if owner.engine != "claude":
        raise SessionUnavailable("notify --next-turn requires managed Claude")
    store.authorize_native_action(owner.worker, "send")  # binding only
    record = store.record(owner.worker)
    native = record.get("native") or {}
    sid = native.get("sessionId")
    if (not isinstance(sid, str) or not sid
            or (record.get("lifecycle") or {}).get("state") != "idle"):
        raise SessionUnavailable("notify --next-turn requires a live idle managed worker")
    if active_operation(owner.worker_id) is not None:
        raise SessionUnavailable("notify --next-turn requires a live idle managed worker (compact active)")
    if not events_ndjson_path(owner.worker_id).is_file():
        raise SessionUnavailable("Cannot find an upcoming Claude turn; retry notification")
    cursor = read_page("claude", owner.worker_id, tail=0)["next_cursor"]
    if decode_cursor(cursor, engine="claude", short=owner.worker_id).get("i") is None:
        raise SessionUnavailable("future Claude event baseline not retained")
    snapshot = wait_worker(owner.worker, timeout=.75,
                           expected_generation=store.generation)
    try:
        live = _rpc(owner.worker, {"op": "status"}, timeout=.5)
    except (OSError, ValueError, TimeoutError) as error:
        raise SessionUnavailable(f"future Claude original host unavailable: {error}") from error
    if (snapshot.get("worker") != owner.worker
            or snapshot.get("state") != "idle"
            or snapshot.get("status") != "idle"
            or snapshot.get("reason") != "worker is idle"
            or snapshot.get("active_turn_ref") is not None
            or live.get("ok") is not True or live.get("state") != "idle"
            or live.get("activeTurnRef") is not None
            or live.get("waitingFor") is not None
            or live.get("compacting") is not False or live.get("compactJournalError")
            or active_operation(owner.worker_id) is not None
            or (store.record(owner.worker).get("native") or {}).get("sessionId") != sid):
        raise SessionUnavailable("notify --next-turn requires a live idle managed worker")
    changed = read_page("claude", owner.worker_id, after=cursor, native=True)
    if changed["gap"] or changed["has_more"] or changed["events"]:
        raise SessionUnavailable("future Claude baseline changed during idle admission")
    store.revalidate()
    return {"worker": owner.worker, "worker_id": owner.worker_id,
            "engine": "claude", "name": record.get("name"), "state": "idle",
            "operation": {"kind": "claude-next-turn", "selectionMode": "future",
                          "nativeSourceId": sid, "armCursor": cursor,
                          "selectedTurnRef": None}}


def _future_events(worker_id: str, cursor: str | None) -> list[dict] | None:
    """One existing physical-order event-feed walk, or None on a cursor gap."""
    from ccx.ops.event_feed import read_page
    rows = []
    while True:
        page = read_page("claude", worker_id, after=cursor, native=True,
                         limit=1000)
        if page["gap"]:
            return None
        rows.extend(page["events"])
        if not page["has_more"]:
            return rows
        cursor = page["next_cursor"]


def _future_ref_before(worker_id: str, cursor: str, ref: str) -> bool:
    """Missing baseline or older evidence disqualifies a first future start."""
    from ccx.ops.event_feed import decode_cursor, read_page
    at = decode_cursor(cursor, engine="claude", short=worker_id)
    if at["p"] is None and at["q"] == 0:
        return False
    next_cursor = None
    while True:
        page = read_page("claude", worker_id, after=next_cursor,
                         native=True, limit=1000)
        if page["gap"]:
            return True
        for row in page["events"]:
            if row.get("turn_ref") == ref:
                return True
            if row.get("epoch") == at["p"] and row.get("seq") == at["q"]:
                return False
        if not page["has_more"]:
            return True  # baseline rotated out, never adopt later old R
        next_cursor = page["next_cursor"]


def _future_first_ref_matches(worker_id: str, cursor: str, ref: str) -> bool:
    """Select the first eligible future turn, so later starts cannot replace it."""
    events = _future_events(worker_id, cursor)
    if events is None:
        return False
    seen: set[str] = set()
    for row in events:
        candidate = row.get("turn_ref")
        if not isinstance(candidate, str) or not candidate.startswith("ccx-turn:claude:v1:"):
            continue
        if candidate in seen:
            continue
        seen.add(candidate)
        if (row.get("type") in {"turn_start", "turn_identity_recovered"}
                and not _future_ref_before(worker_id, cursor, candidate)):
            return candidate == ref
    return False
