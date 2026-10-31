"""Read-only recursive settlement of original CCX-owned Codex work."""
from __future__ import annotations

import time
from typing import Any

from ccx.codex import evidence as codex_evidence
from ccx.notify.registry import TERMINAL_REGISTRATION_STATES, iter_registrations

DEFAULT_WAKE_GRACE_S = 120.0
SETTLE_POLL_S = 0.35
_PRESENTATION_LIMIT = 20
_TERMINAL_TURNS = frozenset({"completed", "failed", "interrupted", "cancelled"})


def _record(worker: str, store) -> dict[str, Any]:
    """One original full owner; the provider thread is a separate identity."""
    owner = store.resolve(worker)
    if owner.engine != "codex":
        from ccx.errors import InvalidRequest
        raise InvalidRequest("recursive settle requires a managed Codex UUID")
    result = store.record(owner.worker)
    native = result.get("native") or {}
    thread = native.get("threadId")
    if (not isinstance(thread, str) or not thread
            or native.get("threadSource") != store.codex_source(owner.worker)):
        thread = None
    return {**result, "threadId": thread, "generation": store.generation}


def _parent_registrations(record: dict[str, Any], source: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """A native thread alone never confers parent ownership."""
    thread = record.get("threadId")
    worker_id = record.get("workerId")
    generation = record.get("generation")
    if not thread:
        return []
    return [row for row in source if (
        (row.get("parent") or {}).get("kind") == "ccx-codex"
        and (row["parent"]).get("workerId") == worker_id
        and row["parent"].get("generation") == generation
        and row["parent"].get("threadId") == thread)]


def _worker_summary(reg: dict[str, Any]) -> dict[str, Any]:
    worker = reg.get("worker") or {}
    return {"eventId": reg.get("eventId"),
            "worker": (f"{worker.get('engine')}:{worker['workerId']}"
                       if worker.get("engine") and worker.get("workerId") else None),
            "workerName": worker.get("name"), "state": reg.get("state"),
            "updatedAtMs": reg.get("updatedAtMs"),
            "deliveredAtMs": reg.get("deliveredAtMs")}


def _completed_turn_ids(record: dict[str, Any]) -> set[str]:
    """Only source-complete native terminal events on the original parent thread."""
    thread = record.get("threadId")
    worker_id = record.get("workerId")
    if not thread or not worker_id:
        return set()
    try:
        journal = codex_evidence.read_journal(f"codex:{worker_id}")
        index = codex_evidence.build_index(journal, health=record.get("evidence"))
    except (OSError, ValueError):
        return set()
    if not index["complete"] or not journal.exists:
        return set()
    completed: set[str] = set()
    for event in journal.events:
        if (event.get("method") != "turn/completed" or
                event.get("threadId") != thread):
            continue
        turn_id = event.get("turnId")
        turn = (event.get("params") or {}).get("turn")
        facts = index["turns"].get(turn_id)
        if (isinstance(turn_id, str) and isinstance(turn, dict)
                and turn.get("id") == turn_id
                and turn.get("status") in _TERMINAL_TURNS
                and facts and facts["sawTurnStarted"] and facts["sawTurnCompleted"]):
            completed.add(turn_id)
    return completed


def _pending_wake(registrations: list[dict[str, Any]], record: dict[str, Any]) -> list[dict[str, Any]]:
    active = (record.get("lifecycle") or {}).get("activeTurnId")
    last = record.get("lastTurn") or {}
    done = None
    pending = []
    for row in registrations:
        if row.get("state") != "delivered":
            continue
        wake = (row.get("delivery") or {}).get("turnId")
        if not isinstance(wake, str) or not wake or wake == active:
            pending.append(row)
            continue
        if last.get("turnId") == wake and last.get("status") in _TERMINAL_TURNS:
            continue
        if done is None:
            done = _completed_turn_ids(record)
        if wake not in done:
            pending.append(row)
    return pending


def _deadline_after(value: float | None) -> float | None:
    if value is None:
        return None
    return time.monotonic() + value


def _expired(deadline: float | None) -> bool:
    return deadline is not None and time.monotonic() >= deadline


def _result(record: dict[str, Any], *, verdict: str,
            registrations: list[dict[str, Any]] = (),
            live: list[dict[str, Any]] = (), pending_wake: list[dict[str, Any]] = (),
            wake_grace_s: float = DEFAULT_WAKE_GRACE_S,
            error: str | None = None, observation: dict[str, Any] | None = None) -> dict[str, Any]:
    lc = record.get("lifecycle") or {}
    last = record.get("lastTurn") or {}
    report = record.get("lastReport") or {}
    # A cached report about another native turn is not this worker's answer.
    message = (report.get("text") if last.get("status") == "completed"
               and report.get("turnId") == last.get("turnId")
               and isinstance(report.get("text"), str) else None)
    worker_id = record.get("workerId")
    observed = observation or {}
    return {"ok": verdict == "settled", "worker": f"codex:{worker_id}",
            "worker_id": worker_id, "short": worker_id[:8] if worker_id else None,
            "threadId": record.get("threadId"), "name": record.get("name"),
            "cwd": record.get("cwd"), "state": observed.get("state") or lc.get("state"),
            "activeTurnId": lc.get("activeTurnId"),
            "pendingApprovals": observed.get("pendingApprovals") or [],
            "lastTurn": record.get("lastTurn"), "lastReport": None,
            "activeTurnError": record.get("activeTurnError"),
            "evidence": record.get("evidence"),
            "settle": True, "settlementVerdict": verdict,
            "settled": verdict == "settled", "outstandingCount": len(live),
            "outstanding": [_worker_summary(r) for r in live[:_PRESENTATION_LIMIT]],
            "outstandingTruncated": len(live) > _PRESENTATION_LIMIT,
            "pendingWakeCount": len(pending_wake),
            "pendingWake": [_worker_summary(r) for r in pending_wake[:_PRESENTATION_LIMIT]],
            "pendingWakeTruncated": len(pending_wake) > _PRESENTATION_LIMIT,
            "registrationCount": len(registrations),
            "wakeGraceSeconds": wake_grace_s,
            "lastMessage": message, "error": error}


def _settlement_cycle(record: dict[str, Any], registrations: list[dict[str, Any]], store) -> list[str] | None:
    root = record.get("threadId")
    if not root:
        return None
    generation = store.generation
    edges: dict[str, set[str]] = {}
    for row in registrations:
        if row.get("state") in TERMINAL_REGISTRATION_STATES or row.get("settlementMode") != "orchestration":
            continue
        parent, child = row.get("parent") or {}, row.get("worker") or {}
        if (parent.get("kind") != "ccx-codex" or parent.get("generation") != generation
                or child.get("engine") != "codex" or child.get("generation") != generation):
            continue
        target = _record(f"codex:{child['workerId']}", store)
        if target.get("threadId"):
            edges.setdefault(parent["threadId"], set()).add(target["threadId"])
    stack = [(root, [root])]
    seen = set()
    while stack:
        node, path = stack.pop()
        if node in seen:
            continue
        seen.add(node)
        for next_thread in edges.get(node, ()):
            if next_thread == root:
                return [*path, root]
            if next_thread not in path:
                stack.append((next_thread, [*path, next_thread]))
    return None


def settlement_progress(worker: str, *,
                        wake_grace_s: float = DEFAULT_WAKE_GRACE_S,
                        registrations: list[dict[str, Any]] | None = None,
                        store=None, observation: dict[str, Any] | None = None) -> dict[str, Any]:
    """One original full-ID registry census and nonblocking recursive verdict."""
    from ccx import constants
    from ccx.errors import SessionUnavailable
    from ccx.ops.wait import wait_worker
    from ccx.store.root import require_ccx_active

    store = store or require_ccx_active(constants.CCX_ROOT)
    store.revalidate()
    record = _record(worker, store)
    if observation is None:
        observation = wait_worker(worker, timeout=.25,
                                  expected_generation=store.generation)
    # A supplied non-strict snapshot is not evidence of a complete inventory.
    try:
        rows = iter_registrations(include_terminal=True, strict=True)
    except SessionUnavailable as error:
        if not str(error).startswith("notification inventory incomplete:"):
            raise
        return _result(record, verdict="inventory_incomplete", error=str(error),
                       observation=observation, wake_grace_s=wake_grace_s)
    parent_rows = _parent_registrations(record, rows)
    live = [r for r in parent_rows if r.get("state") not in TERMINAL_REGISTRATION_STATES]
    pending = _pending_wake(parent_rows, record)
    broken = next((r for r in parent_rows if r.get("state") == "undeliverable"), None)
    cycle = _settlement_cycle(record, rows, store)
    status = observation.get("status")
    state = observation.get("state")
    lc = record.get("lifecycle") or {}
    if status in {"dead", "crashed"} and lc.get("state") == status:
        verdict = status
    elif status == "waiting" and observation.get("pendingApprovals"):
        verdict = "approval_waiting"
    elif broken:
        verdict = "undeliverable_child"
    elif cycle:
        verdict = "cycle"
    else:
        active = observation.get("active_turn_ref")
        idle = (status == "idle" and state == "idle"
                and observation.get("reason") == "worker is idle" and active is None)
        unloaded = (status == "stopped" and state in {"idle", "idle-killed"}
                    and lc.get("state") in {"idle", "idle-killed"}
                    and observation.get("reason", "").startswith("Codex runtime is absent")
                    and active is None)
        killed = (status == "idle-killed" and lc.get("state") == "idle-killed")
        verdict = ("settled" if record.get("threadId") and (idle or unloaded or killed)
                   and not live and not pending else "pending")
    error = (f"child notification {broken['eventId']} is undeliverable" if broken else
             "recursive settlement cycle: " + " -> ".join(cycle) if cycle else None)
    store.revalidate()
    return _result(record, verdict=verdict, registrations=parent_rows,
                   live=live, pending_wake=pending, observation=observation,
                   wake_grace_s=wake_grace_s, error=error)


def settlement_snapshot(worker: str, *,
                        wake_grace_s: float = DEFAULT_WAKE_GRACE_S,
                        registrations: list[dict[str, Any]] | None = None) -> dict[str, Any] | None:
    result = settlement_progress(worker, wake_grace_s=wake_grace_s,
                                 registrations=registrations)
    return None if result["settlementVerdict"] == "pending" else result


def wait_for_settlement(worker: str, *, timeout: float | None = None,
                        wake_grace_s: float = DEFAULT_WAKE_GRACE_S,
                        expected_generation: str | None = None) -> dict[str, Any]:
    """The same graph reader, held until true completion or a bounded verdict."""
    from math import isfinite
    from ccx import constants
    from ccx.ops.wait import wait_worker
    from ccx.store.root import require_ccx_active
    from ccx.errors import InvalidRequest

    if any(type(value) not in {int, float} or not isfinite(value) or value < 0
           for value in (timeout, wake_grace_s) if value is not None):
        raise InvalidRequest("settle timeout and wake grace must be nonnegative finite seconds")
    store = require_ccx_active(constants.CCX_ROOT,
                               expected_generation=expected_generation)
    owner = store.resolve(worker)
    if owner.engine != "codex":
        raise InvalidRequest("recursive settle requires a Codex target")
    worker = owner.worker
    timeout_at = _deadline_after(timeout)
    wake_since = None
    last_turn = None
    while True:
        budget = (SETTLE_POLL_S if timeout_at is None
                  else min(SETTLE_POLL_S, max(0.0, timeout_at - time.monotonic())))
        observed = wait_worker(worker, timeout=budget, expected_generation=store.generation)
        result = settlement_progress(worker, store=store, observation=observed,
                                     wake_grace_s=wake_grace_s)
        if result["settlementVerdict"] != "pending":
            return result
        # As main: missing-wake grace runs only while the parent is quiet. An
        # observed active turn, or a newly completed one, restarts the clock.
        running = bool(observed.get("active_turn_ref") or result.get("activeTurnId"))
        settled_turn = (result.get("lastTurn") or {}).get("turnId")
        if running or settled_turn != last_turn:
            wake_since = None
        last_turn = settled_turn
        wake_only = (result["pendingWakeCount"] and not result["outstandingCount"]
                     and not running)
        wake_since = (wake_since or time.monotonic()) if wake_only else None
        if wake_since is not None and time.monotonic() - wake_since >= wake_grace_s:
            return {**result, "ok": False, "settled": False,
                    "settlementVerdict": "delivery_gap",
                    "error": "original final child delivery lacks its exact completed parent wake within grace"}
        if _expired(timeout_at):
            return {**result, "ok": False, "settled": False,
                    "settlementVerdict": "timeout", "error": "settle timed out"}
        time.sleep(SETTLE_POLL_S if timeout_at is None else
                   min(SETTLE_POLL_S, max(0.0, timeout_at - time.monotonic())))


def send_settle_failure(worker: str, observation: dict) -> dict | None:
    """An exact delivery can fail while its receipt still reads 'accepted'.

    Consult the same stable-state observer as `wait`, without treating an
    ordinary idle (including an owned-but-unpasted Claude delivery) as done.
    """
    receipt = observation.get("delivery") or {}
    if receipt.get("evidence_status") in {"pty-write-failed", "pty-submit-failed"}:
        # A stalled PTY write is not a crashed worker, and partial input may
        # still exist. Do not block --wait indefinitely or invite a new send.
        return {"status": "unknown", "error":
                "Claude PTY input stalled; this original delivery may be partial. "
                "Worker left running; reconcile this delivery ID, do not resend"}
    from ccx.ops.wait import wait_worker
    verdict = wait_worker(worker, timeout=0)
    state = verdict["status"]
    original = observation.get("turn_ref") or receipt.get("turn_ref")
    last = verdict.get("lastTurn") or {}
    failures = {"failed", "interrupted", "cancelled"}
    proven_failure = (
        observation.get("native_status") in failures
        or (original == f"ccx-turn:codex:v1:{last.get('turnId')}"
            and last.get("status") in failures)
    )
    if worker.startswith("codex:") and isinstance(original, str) and proven_failure:
        from ccx.codex.records import read_record
        from ccx.store.root import GenerationError
        try:
            cap_stop = read_record(worker).get("quotaStop") or {}
        except (GenerationError, OSError, ValueError):
            cap_stop = {}  # Recorded quota stop absent/unreadable: ordinary failure.
        if (original == f"ccx-turn:codex:v1:{cap_stop.get('turnId')}"
                and cap_stop.get("turnId")):
            return {"status": "budget-stopped", "error": cap_stop.get("reason")}
    if (original == f"ccx-turn:codex:v1:{last.get('turnId')}"
            and last.get("status") in failures):
        return {"status": last["status"], "error": last.get("error")}
    if (worker.startswith("codex:") and isinstance(original, str)
            and original.startswith("ccx-turn:codex:v1:")
            and original != f"ccx-turn:codex:v1:{last.get('turnId')}"):
        from ccx.codex.records import read_record
        from ccx.ops.turn_reports import codex_native_turn_evidence

        # A later turn replaces the latest projection, but cannot replace
        # the original delivery's retained terminal evidence.
        record = read_record(worker)
        thread_id = (record.get("native") or {}).get("threadId")
        proof = codex_native_turn_evidence(
            worker, record, thread_id, original.removeprefix("ccx-turn:codex:v1:"),
        ) if thread_id else None
        if proof is not None and proof["status"] in failures:
            return proof
    settled = verdict.get("last_settled_turn") or {}
    if (original and original == settled.get("turn_ref")
            and settled.get("outcome") in failures):
        return {"status": settled["outcome"], "error": settled.get("error")}
    if state in {"crashed", "dead", "idle-killed", "host-dead",
                 "budget-stopped", "unavailable", "waiting"}:
        return {"status": state, "error": verdict.get("error") or verdict.get("reason"),
                "apiError": verdict.get("apiError"),
                "pending_input": verdict.get("pending_input")}
    # A native thread/read failure can be a transient resume snapshot.
    # Only the original recorded terminal turn or settled receipt may end
    # --wait; a later turn/completed can still prove this delivery succeeded.
    return None
