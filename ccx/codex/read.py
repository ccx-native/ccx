"""Shared native Codex pending approval projection for CLI, WorkerClient and the public protocol."""
from __future__ import annotations
from typing import Any


def pending_input(approvals: Any) -> dict[str, Any] | None:
    """The cross-engine normalized pending-input object for Codex approvals.

    The one projection ``peek``, ``WorkerClient`` and the public protocol share. A
    Codex approval is never dressed up as a user question: Claude's
    AskUserQuestion is a separate capability, so ``questions`` stays empty.
    """
    approvals = [value for value in approvals or [] if isinstance(value, dict)]
    if not approvals:
        return None
    first = approvals[0]
    message = first.get("reason") or first.get("command")
    return {
        "kind": "permission",
        # Native request IDs may repeat after app-server restart. Only a real
        # CCX gateId is actionable; historical previews cannot be decided.
        "request_id": first.get("gateId") if isinstance(first.get("gateId"), str) else None,
        "tool": first.get("kind"),
        "message": message if isinstance(message, str) else (
            None if message is None else str(message)
        ),
        "questions": [],
        "native": {"pendingApprovals": approvals},
    }
