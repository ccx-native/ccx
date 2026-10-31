from __future__ import annotations

import math
import os
import threading
from typing import Any
from ccx.codex.protocol import ProtocolError, ProtocolTransportError, error_payload
from ccx.codex.records import now_ms


_LAUNCH_SETTLE_CAP_S = 10.0


def _native_history_budget(message: dict[str, Any]) -> float:
    """One caller-provided native read budget, not a fixed daemon timeout."""
    budget = message.get("observeSeconds", 30.0)
    if (type(budget) not in (int, float)
            or not math.isfinite(budget) or budget <= 0):
        raise ValueError("native history requires positive observation seconds")
    return float(budget)


def linger_seconds() -> float:
    try:
        return float(os.environ.get("CCX_CODEXD_LINGER", "900"))
    except ValueError:
        return 900.0


def _bounded(value: Any, limit: int = 1000) -> Any:
    if not isinstance(value, str) or len(value) <= limit:
        return value
    return value[: limit - 1] + "…"


def _compact_native_refusal(error: ProtocolError) -> bool:
    if isinstance(error, ProtocolTransportError) or error.request_id is None:
        return False
    data = error.data if isinstance(error.data, dict) else {}
    info = data.get("codexErrorInfo", data)
    kind = info.get("activeTurnNotSteerable") if isinstance(info, dict) else None
    if isinstance(kind, dict):
        return kind.get("turnKind") == "compact"
    # Codex 0.159.3 preserves structured error data on steer, but turn/start
    # wraps the same native refusal as an internal-error string without data.
    # Match only the observed method/code/literal; never all -32603 failures.
    if error.method == "turn/start" and error.code == -32603:
        return error.message == "failed to submit turn input: ActiveTurnNotSteerable { turn_kind: Compact }"
    return error.message == "cannot steer a compact turn"


def _protocol_failure(error: ProtocolError) -> dict[str, Any]:
    """One daemon-global failure envelope.

    ``error`` stays the human-readable string every existing caller reads;
    ``protocolError`` carries the machine-readable fields that would otherwise
    be flattened away at this process boundary.
    """
    return {
        "ok": False,
        "error": str(error),
        "protocolError": error_payload(error),
        **({"reason": "compaction-busy", "retryable": True}
           if _compact_native_refusal(error) else {}),
    }


def _abandon_active_compact(record: dict[str, Any], reason: str) -> bool:
    """Close a persisted compact that can no longer yield native proof.

    The compact count does not advance: only native compaction evidence proves
    success. Recording the exact operation as unknown prevents a daemon
    restart/kill from poisoning a later generic ``notify`` (or a compact status
    read) with a stale active marker.
    """
    active = record.get("activeCompact")
    if not isinstance(active, dict):
        return False
    record["lastCompact"] = {
        "operationId": active.get("operationId"),
        "baselineCompactCount": active.get("baselineCompactCount"),
        "targetCompactCount": active.get("targetCompactCount"),
        "startedAtMs": active.get("startedAtMs"),
        "completedAtMs": now_ms(),
        "contextBefore": active.get("contextBefore"),
        "turnId": active.get("turnId"),
        "contextItemId": active.get("contextItemId"),
        "nativeTurnStarted": active.get("nativeTurnStarted") is True,
        "nativeTurnSettled": False,
        "status": "unknown",
        "reason": reason,
    }
    # Original fullUUID effects live exclusively in GenerationStore receipts.
    # Do not create or update a second short-keyed compact receipt.
    record["activeCompact"] = None
    return True

def _retire_runtime_markers(record: dict[str, Any], reason: str) -> bool:
    """Drop markers only a vanished app-server connection could resolve."""
    changed = _abandon_active_compact(record, reason)
    if record.get("nativeTurnStart") is not None:
        record["nativeTurnStart"] = None
        changed = True
    return changed


class _TurnLaunch:
    """A ``turn/start`` that is irrevocably in flight.

    Published under the worker lock in the same critical section that checks the
    cancellation marker, so a concurrent ``kill`` either wins (the loader never
    starts the turn) or sees this and takes responsibility for cancelling it.
    """

    __slots__ = ("done", "turn_id")

    def __init__(self) -> None:
        self.done = threading.Event()
        self.turn_id: str | None = None


class ReleasePending(ProtocolError):
    """Codex has not answered an earlier `thread/unsubscribe` of this thread."""


class _ThreadRelease:
    """One `thread/unsubscribe`, fenced from teardown until Codex answers it."""

    def __init__(self, thread_id: str):
        self.thread_id = thread_id
        self.settled = threading.Event()


class WorkerKilled(Exception):
    """A load was abandoned because the worker was killed while it ran.

    Internal control flow, never surfaced to a caller as-is: the loader
    turns it into an ordinary refused-load reply. It is deliberately not a
    `ProtocolError` — nothing went wrong with app-server.
    """
