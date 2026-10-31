"""Durable operation journal for managed Claude ``/compact`` requests.

Compaction is unusual among CCX operations: Claude records its result in the
transcript, while the host also has to release the PTY input surface.  A client
must never infer failure merely because the host stopped saying "compacting";
the result may have landed between two observations.  This module gives the
host and all clients one durable, monotonic operation record to coordinate on.

The journal is a bounded sidecar rather than a field in the session record.
Session records have several independent writers (analytics, lifecycle,
dashboard projections); keeping operations separate prevents a stale
read-modify-write from erasing a terminal compact result.

The journal itself has writers in more than one process too: the host, and a
CLI/Python caller that reconciles from the transcript when the host does not
answer in time. Every mutation therefore holds a bounded exclusive ``flock``
on ``<short>.compact.lock`` across its read -> mutate -> replace cycle, the
same discipline as the steering journal. Reads stay lock-free because
publication is one atomic ``os.replace``.
"""

from __future__ import annotations

import os
import pathlib
import time
import uuid
from typing import Any

from ccx.store.journal_io import journal_lock as _journal_lock, write_json
from ccx.store.paths import compact_operations_path
from ccx.claude.transcript import PinnedNativeSource, api_error_reason, compact_result_since

JOURNAL_VERSION = 1
JOURNAL_KEEP = 32

PENDING_STATUSES = frozenset({"accepted", "pasted", "submitted"})
TERMINAL_STATUSES = frozenset({"succeeded", "noop", "failed", "unknown"})
ALL_STATUSES = PENDING_STATUSES | TERMINAL_STATUSES

def compact_intent_digest(
    worker_id: str, *, generation: str, native_session_id: str,
    operation_id: str,
) -> str:
    """Immutable original Claude /compact intent shared by caller and host.

    Observation wait and host execution budgets are deliberately not part of
    native effect identity: changing either cannot authorize a second paste.
    """
    from ccx.store.store import _digest

    return _digest({
        "workerId": worker_id,
        "engine": "claude",
        "generation": generation,
        "nativeSessionId": native_session_id,
        "operationId": operation_id,
        "action": "compact",
    })


_ALLOWED_TRANSITIONS = {
    "accepted": frozenset({"accepted", "pasted", "submitted", "failed", "unknown",
                           "succeeded", "noop"}),
    "pasted": frozenset({"pasted", "submitted", "failed", "unknown",
                         "succeeded", "noop"}),
    # A submitted compact may reach `failed` only on positive transcript
    # evidence — Claude's own stderr verdict — never on a bare timeout. Absence
    # of a result stays `unknown`, because not hearing back is not proof of
    # failure; that distinction is the whole point of this journal.
    "submitted": frozenset({"submitted", "unknown", "succeeded", "noop", "failed"}),
    "succeeded": frozenset({"succeeded"}),
    "noop": frozenset({"noop"}),
    "failed": frozenset({"failed"}),
    # A late transcript boundary is stronger evidence than a prior inability to
    # decide, so unknown alone is allowed to resolve to success/no-op — or to a
    # failure verdict that only appeared in the transcript after we gave up.
    "unknown": frozenset({"unknown", "succeeded", "noop", "failed"}),
}


class CompactJournalError(RuntimeError):
    """The durable compact journal exists but cannot be trusted."""


class CompactJournalBusy(CompactJournalError):
    """Another writer holds the journal lock; nothing was written."""


#: The host's select loop is single-threaded: it must never park behind a
#: wedged peer. Out-of-host callers may wait longer, never forever.
HOST_LOCK_TIMEOUT_S = 2.0
CLIENT_LOCK_TIMEOUT_S = 15.0


def lock_path(short: str) -> pathlib.Path:
    return compact_operations_path(short).with_suffix(".lock")


def journal_lock(short: str, *, timeout: float | None = None):
    return _journal_lock(
        lock_path(short),
        timeout=CLIENT_LOCK_TIMEOUT_S if timeout is None else timeout,
        busy_error=CompactJournalBusy, label=f"compact journal for {short}",
    )


def now_ms() -> int:
    return int(time.time() * 1000)


def new_operation_id() -> str:
    return str(uuid.uuid4())


def transcript_offset(path: str | pathlib.Path | PinnedNativeSource) -> int:
    try:
        return (path.size() if isinstance(path, PinnedNativeSource)
                else pathlib.Path(path).stat().st_size)
    except OSError:
        return 0


def transcript_identity(
    path: str | pathlib.Path | PinnedNativeSource,
) -> tuple[int | None, int | None]:
    try:
        stat = (os.fstat(path.fd) if isinstance(path, PinnedNativeSource)
                else pathlib.Path(path).stat())
    except OSError:
        return None, None
    return stat.st_dev, stat.st_ino


def empty_journal() -> dict[str, Any]:
    return {"version": JOURNAL_VERSION, "operations": []}


def read_journal(short: str) -> dict[str, Any]:
    """Read the exact generation-owned compact sidecar through verified dir fds."""
    from ccx import constants
    from ccx.store.root import require_ccx_active
    store = require_ccx_active(constants.CCX_ROOT)
    value = store.read_worker_sidecar(short, "compact.json")
    if value is None:
        return empty_journal()
    if not isinstance(value, dict) or not isinstance(value.get("operations"), list):
        raise CompactJournalError("compact journal has an invalid shape")
    if value.get("version") != JOURNAL_VERSION or any(
        not isinstance(op, dict) or op.get("status") not in ALL_STATUSES
        or not isinstance(op.get("operation_id"), str) or not op["operation_id"]
        for op in value["operations"]
    ):
        raise CompactJournalError("compact journal has invalid operation evidence")
    return value


def write_journal(short: str, journal: dict[str, Any]) -> None:
    path = compact_operations_path(short)
    path.parent.mkdir(parents=True, exist_ok=True)
    operations = list(journal.get("operations") or [])
    # Never evict pending operations: each retains unresolved native evidence.
    pending = [op for op in operations if op.get("status") in PENDING_STATUSES]
    terminal = [op for op in operations if op.get("status") not in PENDING_STATUSES]
    kept = terminal[-max(0, JOURNAL_KEEP - len(pending)):] + pending
    value = {
        "version": JOURNAL_VERSION,
        "updated_at": now_ms(),
        "operations": kept,
    }
    write_json(path, value)


def get_operation(short: str, operation_id: str | None) -> dict | None:
    if not operation_id:
        return None
    for operation in reversed(read_journal(short).get("operations") or []):
        if operation.get("operation_id") == operation_id:
            return dict(operation)
    return None


def latest_operation(short: str) -> dict | None:
    operations = read_journal(short).get("operations") or []
    return dict(operations[-1]) if operations else None


def active_operation(short: str) -> dict | None:
    for operation in reversed(read_journal(short).get("operations") or []):
        if operation.get("status") in PENDING_STATUSES:
            return dict(operation)
    return None


def begin_operation(
    short: str,
    *,
    operation_id: str,
    transcript: str,
    timeout_s: float,
    host_epoch: str,
    lock_timeout: float | None = None,
) -> tuple[dict, bool]:
    """Create an accepted operation, or return the idempotent prior operation.

    A different pending operation is a conflict.
    """
    with journal_lock(short, timeout=lock_timeout):
        return _begin_operation_locked(
            short, operation_id=operation_id, transcript=transcript,
            timeout_s=timeout_s, host_epoch=host_epoch)


def _begin_operation_locked(
    short: str,
    *,
    operation_id: str,
    transcript: str,
    timeout_s: float,
    host_epoch: str,
) -> tuple[dict, bool]:
    journal = read_journal(short)
    operations = journal.get("operations") or []
    for operation in operations:
        if operation.get("operation_id") == operation_id:
            return dict(operation), False
    for operation in operations:
        if operation.get("status") in PENDING_STATUSES:
            raise RuntimeError(
                f"compact operation already active: {operation.get('operation_id')}"
            )
    accepted_at = now_ms()
    fence_dev, fence_inode = transcript_identity(transcript)
    operation = {
        "operation_id": operation_id,
        "status": "accepted",
        "accepted_at": accepted_at,
        "updated_at": accepted_at,
        "timeout_s": float(timeout_s),
        "host_epoch": host_epoch,
        "transcript": transcript,
        "fence_offset": transcript_offset(transcript),
        "fence_dev": fence_dev,
        "fence_inode": fence_inode,
        "submitted": False,
        "result": None,
        "reason": None,
        "evidence_uuid": None,
    }
    operations.append(operation)
    journal["operations"] = operations
    write_journal(short, journal)
    return dict(operation), True


def transition_operation(
    short: str,
    operation_id: str,
    status: str,
    *,
    lock_timeout: float | None = None,
    **patch: Any,
) -> dict:
    """Move one operation forward, under the journal lock.

    Raises ``ValueError`` for a non-monotonic move (another writer already
    advanced it), ``KeyError`` for an unknown id, and
    :class:`CompactJournalBusy` when the lock is not acquired in time.
    """
    if status not in ALL_STATUSES:
        raise ValueError(f"invalid compact operation status: {status}")
    with journal_lock(short, timeout=lock_timeout):
        return _transition_operation_locked(short, operation_id, status, **patch)


def _transition_operation_locked(
    short: str,
    operation_id: str,
    status: str,
    **patch: Any,
) -> dict:
    journal = read_journal(short)
    operations = journal.get("operations") or []
    for index, operation in enumerate(operations):
        if operation.get("operation_id") != operation_id:
            continue
        previous = operation.get("status")
        if status not in _ALLOWED_TRANSITIONS.get(previous, frozenset()):
            raise ValueError(
                f"non-monotonic compact transition {previous!r} -> {status!r}"
            )
        updated = {**operation, **patch, "status": status, "updated_at": now_ms()}
        if status == "submitted":
            updated["submitted"] = True
        operations[index] = updated
        journal["operations"] = operations
        write_journal(short, journal)
        return dict(updated)
    raise KeyError(f"unknown compact operation: {operation_id}")


def result_status(result: dict) -> str:
    """Map a transcript verdict onto a terminal journal status.

    ``ok=False`` must come first: a failure is also ``compacted=False``, so
    testing only that would file Claude's explicit failure as a benign no-op.
    """
    if result.get("ok") is False:
        return "failed"
    return "succeeded" if result.get("compacted") else "noop"


def failure_reason(result: dict) -> str | None:
    """The stored ``reason`` for a failed verdict, or ``None`` when it succeeded.

    An API failure is described by its classification; everything else (a
    conversation that cannot be reduced, unstrippable media) is already a
    complete sentence from Claude, so it is passed through untouched.
    """
    if result.get("ok") is not False:
        return None
    api_error = result.get("api_error")
    if isinstance(api_error, dict):
        return api_error_reason(api_error)
    return result.get("message") or "compaction failed"


def evaluate_operation(
    short: str,
    operation_id: str,
    *,
    transcript: str | None = None,
    journal: dict | None = None,
    before_offset: int | None = None,
) -> dict | None:
    """Bounded projection of one original compact operation's native evidence."""
    journal = read_journal(short) if journal is None else journal
    operations = journal.get("operations") or []
    operation = next((dict(op) for op in reversed(operations)
                      if op.get("operation_id") == operation_id), None)
    if operation is None:
        return None
    if operation.get("status") in {"succeeded", "noop", "failed"}:
        return operation
    transcript_path = transcript or operation.get("transcript")
    if not transcript_path:
        return operation
    current_dev, current_inode = transcript_identity(transcript_path)
    fence_dev = operation.get("fence_dev")
    fence_inode = operation.get("fence_inode")
    if fence_dev is not None and fence_inode is not None and (
        current_dev != fence_dev or current_inode != fence_inode
    ):
        # The append cursor belongs to a different file generation.  Scanning
        # the replacement from zero could turn an unrelated old boundary into
        # false success, so preserve the operation's current evidence state.
        return operation
    offset = int(operation.get("fence_offset") or 0)
    if transcript_offset(transcript_path) < offset:
        return operation
    # A later journal operation is an upper bound, even when its native echo
    # has not arrived. Never borrow its boundary to resolve this operation.
    index = next(i for i, op in enumerate(operations)
                 if op.get("operation_id") == operation_id)
    upper = [int(op.get("fence_offset") or 0) for op in operations[index + 1:]
             if op.get("transcript") == operation.get("transcript")
             and op.get("fence_dev") == fence_dev
             and op.get("fence_inode") == fence_inode]
    if before_offset is not None:
        upper.append(before_offset)
    result = compact_result_since(
        transcript_path,
        after_offset=offset,
        before_offset=min(upper) if upper else None,
        bounded_manual=True,
        require_command=not bool(operation.get("submitted")),
    )
    if result is None:
        return operation
    return {**operation, "status": result_status(result), "result": result,
            "reason": failure_reason(result), "evidence_uuid": result.get("event_uuid")}


def reconcile_operation(
    short: str, operation_id: str, *, transcript: str | None = None,
    before_offset: int | None = None,
    lock_timeout: float | None = None,
) -> dict | None:
    """Publish a stronger verdict using the same evaluator as strict reads.

    The transcript scan runs holding no lock; the write re-validates against
    the live journal. If another writer advanced the operation meanwhile, its
    current state is returned rather than raising.
    """
    original = get_operation(short, operation_id)
    evaluated = evaluate_operation(
        short, operation_id, transcript=transcript, before_offset=before_offset,
    )
    if evaluated is None or original == evaluated:
        return evaluated
    try:
        return transition_operation(
            short, operation_id, evaluated["status"], result=evaluated.get("result"),
            reason=evaluated.get("reason"), evidence_uuid=evaluated.get("evidence_uuid"),
            lock_timeout=lock_timeout,
        )
    except ValueError:
        return get_operation(short, operation_id)


def observe_original_operation(store, owner, rec: dict, operation_id: str,
                               *, publish: bool = True) -> dict | None:
    """Evaluate original Claude evidence; optionally publish a proven verdict.

    A missing/unverifiable native source cannot prove native success, failure,
    or noop, even if a cached journal claims terminality. The journal remains
    unchanged in that case; a later verified source can still prove the event.
    """
    from ccx.claude.transcript import open_original_claude_source
    from ccx.store.root import StorageIntegrityError

    store.revalidate()
    operation = get_operation(owner.worker, operation_id)
    if operation is None:
        return None
    try:
        with open_original_claude_source(store, owner, rec) as source:
            if source is None:
                raise FileNotFoundError("original Claude native JSONL absent")
            pinned, stat, end_offset, original_path, _dir_fd = source
            fence = operation.get("fence_offset")
            if (operation.get("transcript") != str(original_path)
                    or type(operation.get("fence_dev")) is not int
                    or type(operation.get("fence_inode")) is not int
                    or (operation["fence_dev"], operation["fence_inode"])
                    != (stat.st_dev, stat.st_ino)
                    or type(fence) is not int or not 0 <= fence <= end_offset):
                raise StorageIntegrityError("original compact transcript fence differs")
            # The V1 observer uses the same bounded native evaluator but
            # never publishes. Python status retains the original journal
            # reconciliation/lock under this one verified source fd.
            store.revalidate()
            if publish:
                result = reconcile_operation(
                    owner.worker_id, operation_id, transcript=pinned,
                    before_offset=end_offset,
                )
            else:
                result = evaluate_operation(
                    owner.worker_id, operation_id, transcript=pinned,
                    before_offset=end_offset,
                )
            store.revalidate()
            return result
    except (FileNotFoundError, OSError, StorageIntegrityError) as exc:
        return {
            **operation, "status": "unknown", "result": None,
            "evidence_uuid": None,
            "reason": f"original Claude compact source unavailable: {exc}",
        }


def reconcile_stale_operations(
    short: str,
    *,
    transcript: str,
    current_host_epoch: str,
    lock_timeout: float | None = None,
) -> list[dict]:
    """Reconcile operations inherited from a previous host process.

    If the transcript proves success, preserve it.  Otherwise the old host and
    PTY are gone, so the truthful terminal state is unknown—not failed.
    """
    resolved: list[dict] = []
    journal = read_journal(short)
    for operation in list(journal.get("operations") or []):
        if operation.get("status") not in PENDING_STATUSES:
            continue
        if operation.get("host_epoch") == current_host_epoch:
            continue
        current = reconcile_operation(
            short, operation.get("operation_id"), transcript=transcript,
            lock_timeout=lock_timeout,
        )
        if current and current.get("status") in PENDING_STATUSES:
            try:
                current = transition_operation(
                    short,
                    current["operation_id"],
                    "unknown",
                    reason="host restarted before terminal compact evidence",
                    lock_timeout=lock_timeout,
                )
            except ValueError:
                current = get_operation(short, current["operation_id"])
        if current:
            resolved.append(current)
    return resolved


def public_operation(operation: dict | None) -> dict | None:
    """Bounded RPC-safe projection of a journal operation."""
    if operation is None:
        return None
    result = operation.get("result") if isinstance(operation.get("result"), dict) else None
    return {
        "operation_id": operation.get("operation_id"),
        "status": operation.get("status"),
        "accepted_at": operation.get("accepted_at"),
        "updated_at": operation.get("updated_at"),
        "submitted": bool(operation.get("submitted")),
        "reason": operation.get("reason"),
        "evidence_uuid": operation.get("evidence_uuid"),
        "result": dict(result) if result else None,
    }


def observe_operation(
    short: str,
    operation_id: str,
    *,
    transcript: str | None,
    status_reader=None,
) -> tuple[dict | None, str | None]:
    """Read one reconciled operation snapshot for CLI and Python callers.

    ``status_reader`` is an optional zero-argument callback returning the live
    host's RPC envelope.  Any unavailable/bad host reply falls back to the
    durable journal and transcript.  Journal corruption deliberately propagates
    so public surfaces can report an indeterminate protocol condition rather
    than silently treating the operation as absent.
    """
    current = None
    native_state = None
    if status_reader is not None:
        try:
            reply = status_reader() or {}
        except Exception:
            reply = {}
        native_state = reply.get("state")
        if reply.get("ok"):
            current = reply.get("operation")
    if current is None:
        current = get_operation(short, operation_id)
        if current is not None:
            current = reconcile_operation(
                short, operation_id, transcript=transcript
            ) or current
    return current, native_state
