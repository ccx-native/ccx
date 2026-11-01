from __future__ import annotations
import fcntl
import hashlib
import json
import os
from pathlib import Path
import time
import uuid
from typing import Any, Callable
from ccx.errors import InvalidRequest, SessionUnavailable
from ccx.notify.paths import (
    ensure_dirs,
    registration_path,
    registrations_dir,
)


NOTIFY_REPORT_MAX_CHARS = int(os.environ.get("CCX_NOTIFY_REPORT_MAX_CHARS", "12000"))


TERMINAL_REGISTRATION_STATES = {"delivered", "cancelled", "undeliverable"}


def now_ms() -> int:
    return int(time.time() * 1000)


def _delivery_id(event_id: str) -> str:
    return f"{event_id}:settled"


def _current_worker_name(worker: dict[str, Any]) -> str | None:
    """Prefer current durable display metadata over registration-time snapshots."""
    engine = worker.get("engine")
    short = worker.get("workerId") or worker.get("short")
    if not isinstance(short, str) or not short:
        return worker.get("name")
    try:
        if engine == "codex":
            from ccx.codex.records import read_record as read_codex_record
            record = read_codex_record(short)
        elif engine == "claude":
            from ccx.store.records import read_record as read_claude_record
            record = read_claude_record(short)
        else:
            record = None
    except Exception:
        record = None
    current = record.get("name") if isinstance(record, dict) else None
    return current if isinstance(current, str) and current else worker.get("name")


def _atomic_write(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(value, handle, indent=2)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
    # The event ID is not durably armed until the rename's directory entry
    # also survives a crash. This is the existing registry, not another log.
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _lock_path(event_id: str) -> Path:
    return registration_path(event_id).with_suffix(".lock")


def read_registration(event_id: str) -> dict[str, Any] | None:
    try:
        value = json.loads(registration_path(event_id).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def _hint_managed_parent_background(reg: dict[str, Any]) -> None:
    """Best-effort projection hint to an already-running managed codexd.

    The registry write precedes this hint. No native wake, daemon startup,
    journal append from notifyd, or retry ledger is involved.
    """
    parent = reg.get("parent") or {}
    if parent.get("kind") != "ccx-codex":
        return
    worker_id = parent.get("workerId")
    if not isinstance(worker_id, str) or not worker_id:
        return
    try:
        from ccx.codex.runtime import daemon_rpc
        from ccx.constants import CODEXD_RPC_GENERATION
        daemon_rpc({
            "op": "background-refresh",
            "short": worker_id,
            "rpcGeneration": CODEXD_RPC_GENERATION,
        }, timeout=0.25)
    except Exception:
        pass  # Notifications do not depend on observer availability.


def mutate_registration(
    event_id: str, mutate: Callable[[dict[str, Any]], None]
) -> dict[str, Any] | None:
    from ccx.notify.select import _original_notification_registration
    ensure_dirs()
    with open(_lock_path(event_id), "a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        value = read_registration(event_id)
        if value is None:
            return None
        _original_notification_registration(value)
        previous = value.get("state")
        mutate(value)
        value["updatedAtMs"] = now_ms()
        _atomic_write(registration_path(event_id), value)
    # Never call codexd while the registration file is locked.
    if value.get("state") != previous and value.get("state") in {
        "cancelled", "undeliverable", "delivered",
    }:
        _hint_managed_parent_background(value)
    return value


def _settlement_child_thread(reg: dict[str, Any], store) -> str | None:
    worker = reg.get("worker") or {}
    if worker.get("engine") != "codex" or worker.get("generation") != store.generation:
        return None
    owner = store.resolve(f"codex:{worker['workerId']}")
    native = (store.record(owner.worker).get("native") or {})
    thread = native.get("threadId")
    return (thread if isinstance(thread, str) and thread
            and native.get("threadSource") == store.codex_source(owner.worker)
            else None)


def _assert_no_settlement_cycle(parent: dict[str, Any], child_id: str, store) -> None:
    """One original managed parent→child graph under its publication lock."""
    child = _settlement_child_thread(
        {"worker": {"engine": "codex", "workerId": child_id,
                    "generation": store.generation}}, store)
    if child is None:
        raise InvalidRequest("Child Codex worker cannot be verified; inspect its status")
    if parent["kind"] == "codex-native-thread":
        return  # A captured interactive return address is no managed graph node.
    parent_thread = parent["threadId"]
    if child == parent_thread:
        raise InvalidRequest("recursive settlement cannot wait on the parent thread itself")
    edges: dict[str, set[str]] = {}
    for reg in iter_registrations(strict=True):
        if reg.get("settlementMode") != "orchestration":
            continue
        edge_parent = reg.get("parent") or {}
        if (edge_parent.get("kind") != "ccx-codex"
                or edge_parent.get("generation") != store.generation):
            continue
        edge_child = _settlement_child_thread(reg, store)
        if edge_child:
            edges.setdefault(edge_parent["threadId"], set()).add(edge_child)
    stack = [child]
    visited = set()
    while stack:
        thread = stack.pop()
        if thread == parent_thread:
            raise InvalidRequest("recursive settlement would create a notification cycle")
        if thread not in visited:
            visited.add(thread)
            stack.extend(edges.get(thread, ()))


def create_registration(
    *, parent: dict[str, Any], worker_engine: str, worker_id: str,
    worker_name: str | None, operation: dict[str, Any],
    source: str, settle: bool = False,
    registration_key: str | None = None,
) -> dict[str, Any]:
    """Stage one strict v3 send receipt or independent read-only notify arm."""
    from ccx.notify.select import _active_compact_selection, _compact_effect_receipt, _controller_attempt_at_arm, _original_notification_registration
    from ccx.notify.daemon import _retire_incompatible_notifyd, ensure_notify_daemon
    from ccx import constants
    from ccx.store.root import require_ccx_active
    from ccx.claude.delivery import valid_delivery_id

    hashed_input = (source == "input" and worker_engine == "claude"
                    and operation.get("kind") in {"claude-input-answer", "claude-input-respond"}
                    if isinstance(operation, dict) else False)
    if (settle and worker_engine != "codex"
            or worker_engine not in {"codex", "claude", "controller"}
            or not isinstance(operation, dict)
            or source not in ({"controller.run", "controller.resume", "controller.notify"}
                              if worker_engine == "controller" else {"send", "notify", "new", "input", "compact"})
            or (source == "input" and (
                set(operation) != ({"kind", "gateId", "responseId",
                                    "nativeSourceId", "intentSha256", "selectedTurnRef"}
                                   | ({"payloadSha256"} if hashed_input
                                      else ({"decision", "reason"} if worker_engine == "claude"
                                            else {"decision", "turnId"})))
                or (not hashed_input and (
                    operation.get("kind") != f"{worker_engine}-input-decision"
                    or operation.get("decision") not in {"approve", "reject"}))))
            or (source == "send" and operation.get("kind") != f"{worker_engine}-turn")
            or (source == "compact" and operation.get("kind") != f"{worker_engine}-compact")
            or (source == "new" and (worker_engine not in {"claude", "codex"}
                or type(operation.get("expectTurn")) is not bool
                or operation != {"kind": f"{worker_engine}-stable", "expectTurn": False,
                                 "selectionMode": "status-only", "stableState": "idle"}))
            or (source == "notify" and operation.get("kind") not in {
                f"{worker_engine}-observed-turn", f"{worker_engine}-stable",
                f"{worker_engine}-compact",
                *({"claude-next-turn"} if worker_engine == "claude" else set()),
            })):
        raise InvalidRequest("This notification type is unsupported; choose a valid source")
    if (source == "notify" and operation.get("kind") == f"{worker_engine}-compact"):
        if (set(operation) != {"kind", "selectionMode", "operationId", "nativeSourceId"}
                or operation.get("selectionMode") != "active-compact"):
            raise InvalidRequest("standalone compact needs one original active operation")
    if source == "compact":
        token = operation.get("operationId")
        native_id = operation.get("nativeSourceId")
        if (set(operation) != {"kind", "operationId", "nativeSourceId"}
                or not isinstance(token, str) or not 0 < len(token) <= 128
                or "\x00" in token or not isinstance(native_id, str) or not native_id):
            raise InvalidRequest("compact notification needs an exact native operation and source")
    delivery_id = operation.get("deliveryId")
    if (source == "notify" and operation.get("kind") == "claude-next-turn"
            and operation.get("selectedTurnRef") is not None):
        raise InvalidRequest("future turn must arm before its first R is selected")
    if source == "send" and not valid_delivery_id(delivery_id):
        raise InvalidRequest("notify_parent requires a valid delivery_id")
    store = require_ccx_active(constants.CCX_ROOT)
    # No caller-provided registration key can create an alternate event for the
    # same semantic operation. It also excludes parent intentionally: a foreign
    # parent retry must hit the same event and fail its identity comparison.
    from ccx.store.identity import canonical_worker_id
    wid = canonical_worker_id(worker_id)
    key = (f"full-uuid-{worker_engine}-send:{store.generation}:{wid}:{delivery_id}"
           if source == "send" else
           f"full-uuid-{worker_engine}-new:{store.generation}:{wid}:promptless"
           if source == "new" else
           f"full-uuid-{worker_engine}-input:{store.generation}:{wid}:{operation['gateId']}"
           if source == "input" else
           f"full-uuid-{worker_engine}-compact:{store.generation}:{wid}:{operation['operationId']}"
           if source == "compact" else None)
    if registration_key is not None and registration_key != key:
        raise InvalidRequest("Notification ID does not match its delivery; inspect the request")
    if not isinstance(parent, dict):
        raise InvalidRequest("notification needs a captured parent return address")
    if parent.get("kind") == "ccx-codex":
        parent_id = canonical_worker_id(parent.get("workerId"))
        if parent.get("generation") != store.generation:
            raise InvalidRequest("notification parent original generation changed")
        parent_value = {
            "kind": "ccx-codex", "workerId": parent_id,
            "threadId": parent.get("threadId"), "generation": store.generation,
        }
    elif parent.get("kind") == "codex-native-thread":
        parent_value = dict(parent)  # captured once, no env reread on retry
    else:
        raise InvalidRequest("unsupported notification parent return-address kind")
    if worker_engine == "controller" or source in {"notify", "new", "input", "compact"}:
        # Input reuses the original global answer intent (not a new receipt).
        # Notify has independent UUIDv4; new/input have keyed UUIDv5.
        receipt = None
        receipt_fields = dict(operation)
        immutable = tuple(receipt_fields)
    elif worker_engine == "codex":
        from ccx.codex.delivery import read_receipt
        receipt = read_receipt(f"codex:{wid}", delivery_id)
        receipt_fields = {
            "kind": "codex-turn", "deliveryId": delivery_id,
            "requestedMode": (receipt or {}).get("requestedMode"),
            "promptSha256": (receipt or {}).get("promptSha256"),
            "callerMetadata": (receipt or {}).get("callerMetadata"),
            "clientUserMessageId": (receipt or {}).get("clientUserMessageId"),
            "turnId": None,
        }
        immutable = ("kind", "deliveryId", "requestedMode", "promptSha256",
                     "callerMetadata", "clientUserMessageId")
    else:
        from ccx.claude.delivery import read_receipt
        receipt = read_receipt(f"claude:{wid}", delivery_id)
        receipt_fields = {
            "kind": "claude-turn", "deliveryId": delivery_id,
            "requestedMode": (receipt or {}).get("requestedMode"),
            "promptSha256": (receipt or {}).get("promptSha256"),
            "promptByteCount": (receipt or {}).get("promptByteCount"),
            "callerMetadata": (receipt or {}).get("callerMetadata"),
            "cap": (receipt or {}).get("cap"),
        }
        immutable = tuple(receipt_fields)
    if source == "send":
        if receipt is None:
            raise InvalidRequest("prepare original child receipt before notification registration")
        if (receipt_fields["requestedMode"] != "new_turn_only"
                or any(k in operation and operation[k] != receipt_fields[k]
                       for k in immutable)):
            raise InvalidRequest("notification operation conflicts with original child receipt")
    event_id = (str(uuid.uuid5(uuid.NAMESPACE_URL, "ccx-notify:" + key))
                if key is not None else str(uuid.uuid4()))
    timestamp = now_ms()

    value = {
        "version": 3, "generation": store.generation,
        "eventId": event_id, "state": "armed", "source": source,
        "parent": parent_value,
        "worker": {"engine": worker_engine, "workerId": wid,
                   "generation": store.generation, "name": worker_name},
        "operation": receipt_fields,
        "settlementMode": "orchestration" if settle else None,
        "registrationKeySha256": (hashlib.sha256(key.encode("utf-8")).hexdigest()
                                  if key is not None else None),
        "createdAtMs": timestamp, "updatedAtMs": timestamp,
        "attempts": 0, "nextAttemptAtMs": 0,
    }
    _original_notification_registration(value)  # no file or daemon effect on refusal
    if worker_engine == "controller":
        _controller_attempt_at_arm(wid, receipt_fields, source)
    ensure_dirs()
    # An incompatible supervisor cannot safely interpret this registration.
    # Retire it before publishing; arming does not start a replacement.
    _retire_incompatible_notifyd(timeout=5.0)
    def publish() -> dict[str, Any]:
        with open(_lock_path(event_id), "a+") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            prior = read_registration(event_id)
            if prior is not None:
                if source == "notify":
                    raise SessionUnavailable("independent notification event UUID collision")
                # Do not reset progressed/ready/cancelled/delivered registration.
                _original_notification_registration(prior)
                if (prior.get("parent") != value["parent"]
                        or any((prior.get("worker") or {}).get(field) != value["worker"][field]
                               for field in ("engine", "workerId", "generation"))
                        or prior.get("registrationKeySha256") != value["registrationKeySha256"]
                        or prior.get("settlementMode") != value["settlementMode"]
                        or any((prior.get("operation") or {}).get(k) != receipt_fields[k]
                               for k in immutable)):
                    raise InvalidRequest("notification operation is owned by a different parent or input")
                return prior
            if source == "notify" and operation.get("kind") == f"{worker_engine}-compact":
                owner = store.resolve(f"{worker_engine}:{wid}")
                if _active_compact_selection(store, owner, store.record(owner.worker)) != operation:
                    raise SessionUnavailable("original active compact changed before publication")
            if source == "compact":
                # An ordinary compact may already own this operation ID. It
                # cannot acquire a new "pre-effect" parent event retroactively.
                effect = _compact_effect_receipt(
                    store, f"{worker_engine}:{wid}", worker_engine, wid, operation)
                if effect is not None and effect.get("state") != "prepared":
                    raise InvalidRequest("cannot retroactively arm an existing native compact")
                if worker_engine == "claude":
                    from ccx.ops.compact_ops import get_operation
                    marker = get_operation(wid, operation["operationId"])
                else:
                    record = store.record(f"codex:{wid}")
                    marker = next((
                        row for field in ("activeCompact", "lastCompact")
                        if isinstance(row := record.get(field), dict)
                        and row.get("operationId") == operation["operationId"]
                    ), None)
                if marker is not None:
                    raise InvalidRequest("cannot retroactively arm a recorded native compact")
            if source == "send" and receipt.get("state") != "prepared":
                raise InvalidRequest("cannot retroactively arm an accepted original Codex delivery")
            if source == "input":
                input_intent = store.read_receipt(f"{worker_engine}:{wid}", "answer", operation["responseId"])
                if input_intent is None or input_intent.get("state") != "prepared":
                    raise InvalidRequest("cannot retroactively arm an already submitted original input")
            store.revalidate()
            _atomic_write(registration_path(event_id), value)  # fsync before return
        return value
    if settle:
        graph_lock = registrations_dir() / ".settlement-graph.lock"
        with graph_lock.open("a+") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            if read_registration(event_id) is None:
                _assert_no_settlement_cycle(parent_value, wid, store)
            published = publish()
        _hint_managed_parent_background(published)
        return published
    published = publish()
    _hint_managed_parent_background(published)
    if worker_engine == "controller":
        # A preflighted, empty supervisor may have retired before this arm.
        # The disk registration is authoritative even if restart is unavailable.
        try:
            ensure_notify_daemon()
        except (SessionUnavailable, OSError):
            pass
    return published


def compact_notification_failure(reg: dict[str, Any], reason: str) -> tuple[bool, str]:
    """On core failure, cancel only when its original receipt proves no submission."""
    from ccx import constants
    from ccx.store.root import require_ccx_active

    try:
        store = require_ccx_active(constants.CCX_ROOT)
        worker = reg["worker"]
        family = "compact" if worker["engine"] == "claude" else "codex-compact"
        receipt = store.read_receipt(
            f"{worker['engine']}:{worker['workerId']}", family,
            reg["operation"]["operationId"])
    except (ValueError, OSError):
        receipt = {"state": "unreadable"}  # Never cancel an ambiguous effect.
    if receipt is None or receipt.get("state") in {"prepared", "rejected"}:
        cancel_registration(reg["eventId"], reason)
        uncertain = False
    else:
        uncertain = True
    current = read_registration(reg["eventId"])
    return uncertain, (current or reg)["state"]


def cancel_registration(event_id: str, reason: str) -> None:
    def mutate(value: dict[str, Any]) -> None:
        if value.get("state") in TERMINAL_REGISTRATION_STATES:
            return
        value["state"] = "cancelled"
        value["cancelReason"] = reason
    mutate_registration(event_id, mutate)


def set_codex_turn_id(event_id: str, turn_id: str | None) -> None:
    """Bind only this registration's original accepted semantic turn."""
    if not isinstance(turn_id, str) or not turn_id:
        return
    def mutate(value: dict[str, Any]) -> None:
        from ccx.codex.delivery import read_receipt
        worker = value["worker"]["workerId"]
        delivery_id = value["operation"]["deliveryId"]
        receipt = read_receipt(f"codex:{worker}", delivery_id)
        if (receipt is None or receipt.get("state") != "accepted"
                or receipt.get("turnId") != turn_id):
            raise InvalidRequest("native turn differs from original Codex delivery receipt")
        operation = value["operation"]
        if operation.get("turnId") not in (None, turn_id):
            raise InvalidRequest("original notification turn identity already bound")
        operation["turnId"] = turn_id
    mutate_registration(event_id, mutate)


def public_registration(value: dict[str, Any]) -> dict[str, Any]:
    """Project the original event and native wake status, not parent completion."""
    from ccx.notify.select import _original_notification_registration
    from ccx.notify.daemon import _notifyd_alive
    _store, parent, child = _original_notification_registration(value)
    operation = value["operation"]
    state = value["state"]
    instruction = (
        "Parent machine notification was placed; parent model completion is unproven."
        if state == "delivered" else
        ("Notification cancelled; the controller run is unaffected."
         if value["worker"]["engine"] == "controller" else
         "Notification cancelled; the child delivery is unaffected.")
        if state == "cancelled" else
        ("Parent notification undeliverable; the controller attempt remains recorded."
         if value["worker"]["engine"] == "controller" else
         "Parent notification undeliverable; the child receipt remains recorded.")
        if state == "undeliverable" else
        ("Controller parent notification is durable; a machine wake can follow "
         "this attempt's outcome."
         if value["worker"]["engine"] == "controller" else
         "Notification saved; CCX will try to notify the calling Codex worker "
         "once this worker's turn outcome is confirmed.")
    )
    turn_ref = operation.get("selectedTurnRef")
    if (value["worker"]["engine"] == "claude" and turn_ref is None
            and operation.get("deliveryId")):
        from ccx.claude.delivery import read_receipt
        original = read_receipt(child, operation["deliveryId"])
        turn_ref = (original.get("turnRef") if isinstance(original, dict)
                    and original.get("deliveryId") == operation["deliveryId"] else None)
        turn_ref = turn_ref or (value.get("settlement") or {}).get("turnRef")
    result = {
        "event_id": value["eventId"], "state": value["state"],
        "parent": parent, "parent_kind": value["parent"]["kind"],
        "worker": child, "worker_id": value["worker"]["workerId"],
        "worker_name": value["worker"].get("name"),
        "short": value["worker"]["workerId"][:8],
        "delivery_id": operation.get("deliveryId"),
        "turn_id": operation.get("turnId") or (value.get("settlement") or {}).get("turnId"),
        **({"selection_mode": operation["selectionMode"],
            "selected_turn_ref": operation.get("selectedTurnRef"),
            "selected_original_delivery_id": operation.get("deliveryId")}
           if value["source"] in {"notify", "new"} else {}),
        **({"selected_turn_ref": operation["selectedTurnRef"]}
           if value["source"] == "input" else {}),
        **({"turn_ref": turn_ref} if value["worker"]["engine"] == "claude" else {}),
        "source": value["source"],
        "settle": value.get("settlementMode") == "orchestration",
        "resume_automatically": False, "polling_required": False,
        "delivery_available": state in {"armed", "ready", "delivered"},
        "instruction": instruction,
        "caller_metadata": operation.get("callerMetadata"),
        "created_at_ms": value.get("createdAtMs"),
        "updated_at_ms": value.get("updatedAtMs"),
        "ready_at_ms": value.get("readyAtMs"),
        "delivered_at_ms": value.get("deliveredAtMs"),
        "attempts": int(value.get("attempts") or 0),
        "last_attempt_at_ms": value.get("lastAttemptAtMs"),
        "next_attempt_at_ms": value.get("nextAttemptAtMs"),
        "last_error": value.get("lastError"),
        "operation_kind": operation["kind"],
        "settlement": value.get("settlement"),
    }
    if operation.get("kind") in {"claude-compact", "codex-compact"}:
        result.update(operation_type="compact",
                      operation_id=operation["operationId"],
                      operation_status=(value.get("operationSettlement") or
                                        value.get("settlement") or {}).get("operationStatus"))
    if value["worker"]["engine"] == "controller":
        result.update(operation_type="controller-attempt",
                      run_id=value["worker"]["workerId"], attempt=operation["attempt"],
                      notifier_available=_notifyd_alive())
    return result


_VALID_TERMINAL_ROWS: set[tuple[str, int, int, int]] = set()


def iter_registrations(*, include_terminal: bool = False,
                       strict: bool = False) -> list[dict[str, Any]]:
    """Read current notification rows without hiding damaged inventory."""
    from ccx.notify.select import _original_notification_registration
    ensure_dirs()
    values: list[dict[str, Any]] = []
    if strict:
        from ccx import constants
        from ccx.store.root import require_ccx_active
        generation = require_ccx_active(constants.CCX_ROOT).generation

    directory = registrations_dir().absolute()  # entry paths, never symlink targets
    invalid: list[str] = []
    for path in sorted(directory.glob("*.json")):
        try:
            st = path.stat()
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            invalid.append(f"{path} (unreadable JSON)")
            continue
        version_key = (str(path), st.st_ino, st.st_mtime_ns, st.st_size)
        if not isinstance(value, dict):
            invalid.append(f"{path} (invalid row)")
            continue
        version, recorded = value.get("version"), value.get("generation")
        if strict and ((type(version) is int and version in (1, 2)) or
                       (version == 3 and isinstance(recorded, str) and
                        recorded != generation)):
            continue  # Recognizable pre-migration or foreign generation: inert.
        terminal = value.get("state") in TERMINAL_REGISTRATION_STATES
        if terminal and not include_terminal and not strict:
            continue  # strict callers must still see damaged settled rows
        # A settled row never changes again, so one successful validation per
        # file version is enough; notifyd re-reads every row each 0.35 s tick.
        if not (terminal and version_key in _VALID_TERMINAL_ROWS):
            try:
                _original_notification_registration(value)
            except Exception:
                # Normal notify supervision remains permissive to legacy rows.
                invalid.append(f"{path} (invalid active row)")
                continue
            if terminal:
                if len(_VALID_TERMINAL_ROWS) > 4096:
                    _VALID_TERMINAL_ROWS.clear()
                _VALID_TERMINAL_ROWS.add(version_key)
        if terminal and not include_terminal:
            continue
        values.append(value)
    if strict and invalid:
        raise SessionUnavailable(
            "notification inventory incomplete: " + "; ".join(invalid)
            + f". Move these files out of {directory} to unblock settle.")
    return values


def has_pending_registrations() -> bool:
    return bool(iter_registrations())


def terminal_retention_s() -> float:
    """How long completed notification receipts remain on disk.

    The receipts are useful for debugging and ambiguous-delivery reconciliation,
    but they are one file per awaited operation and must not accumulate forever.
    Invalid configuration falls back to seven days; zero prunes terminal receipts
    on the next notifyd start.
    """
    raw = os.environ.get("CCX_NOTIFY_TERMINAL_RETENTION_S", str(7 * 24 * 60 * 60))
    try:
        value = float(raw)
    except (TypeError, ValueError):
        value = float(7 * 24 * 60 * 60)
    return max(0.0, value)


def prune_terminal_registrations(*, now_ms_value: int | None = None) -> int:
    """Remove terminal receipts older than the configured retention window.

    Each candidate is re-read while holding its per-event flock so a concurrent
    mutation cannot be pruned from a stale snapshot. Lock sidecars are removed
    with the terminal JSON while the old inode remains locked by this process.
    """
    ensure_dirs()
    current = now_ms() if now_ms_value is None else int(now_ms_value)
    cutoff = current - int(terminal_retention_s() * 1000)
    removed = 0
    for path in sorted(registrations_dir().glob("*.json")):
        event_id = path.stem
        lock_path = _lock_path(event_id)
        try:
            lock = open(lock_path, "a+")
        except OSError:
            continue
        try:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                continue
            value = read_registration(event_id)
            if not isinstance(value, dict):
                continue
            if value.get("state") not in TERMINAL_REGISTRATION_STATES:
                continue
            terminal_at = (
                value.get("deliveredAtMs")
                or value.get("updatedAtMs")
                or value.get("createdAtMs")
            )
            if not isinstance(terminal_at, (int, float)) or terminal_at > cutoff:
                continue
            try:
                registration_path(event_id).unlink()
            except FileNotFoundError:
                pass
            try:
                lock_path.unlink()
            except FileNotFoundError:
                pass
            removed += 1
        finally:
            lock.close()
    return removed
