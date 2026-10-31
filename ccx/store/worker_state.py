"""Pure worker lifecycle projection shared by normalized public surfaces."""

from __future__ import annotations

from typing import Any

RUNTIME_SERVED = "served"
RUNTIME_ABSENT = "absent"
RUNTIME_INCONCLUSIVE = "inconclusive"
RUNTIME_PRESENCE = frozenset(
    {RUNTIME_SERVED, RUNTIME_ABSENT, RUNTIME_INCONCLUSIVE}
)

_STATE = {
    "booting": "starting",
    "resuming": "starting",
    "idle": "idle",
    "busy": "busy",
    "waiting": "waiting",
    "dead": "stopped",
    "idle-killed": "stopped",
    "budget-stopped": "stopped",
    "crashed": "failed",
}


def normalize_native_state(native: Any) -> str:
    return _STATE.get(native, "unavailable")


def project_runtime_state(
    recorded_native: str | None,
    live_state: str | None,
    runtime_presence: str,
) -> dict[str, Any]:
    """Project one truthful normalized lifecycle from durable and live facts."""
    if runtime_presence not in RUNTIME_PRESENCE:
        raise ValueError(f"invalid runtime presence: {runtime_presence!r}")

    if runtime_presence == RUNTIME_SERVED:
        native = (
            live_state
            if isinstance(live_state, str) and live_state
            else recorded_native
        )
        return {
            "state": normalize_native_state(native),
            "native_state": native,
            "loaded": True,
        }

    native = recorded_native
    if runtime_presence == RUNTIME_ABSENT:
        if native == "idle":
            normalized = "stopped"
        elif native in {"busy", "waiting", "resuming"}:
            normalized = "failed"
        else:
            normalized = normalize_native_state(native)
        return {
            "state": normalized,
            "native_state": native,
            "loaded": False,
        }

    return {
        "state": normalize_native_state(native),
        "native_state": native,
        "loaded": None,
    }


def claude_recorded_delivery_busy(record: dict, receipts) -> bool:
    """Overlay an owned unsettled new turn on a lagging recorded IDLE state."""
    if (record.get("lifecycle") or {}).get("state") != "idle":
        return False
    last = record.get("lastSettledTurn") or {}
    floor = max(record.get("lastSettledAt") or 0, last.get("settledAtMs") or 0)
    return any(
        receipt.get("requestedMode") == "new_turn_only"
        and receipt.get("state") in {"owned", "accepted"}
        and not isinstance(receipt.get("settlement"), dict)
        and (receipt.get("ownedAtMs") or receipt.get("acceptedAtMs")
             or receipt.get("createdAtMs") or 0) > floor
        for receipt in receipts
    )
