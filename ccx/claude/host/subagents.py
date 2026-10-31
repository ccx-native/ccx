from __future__ import annotations
from ccx.system import oslayer
from ccx.claude.host.events import _event_preview, _hook_tool_result, _hook_tool_use_id, _tool_ok


def _hook_subagent_id(data) -> str | None:
    """The originating subagent of a hook payload, or None for the parent."""
    if not isinstance(data, dict):
        return None
    for key in ("agent_id", "subagent_id", "agentId", "subagentId"):
        value = data.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _hook_subagent_type(data: dict) -> str | None:
    for key in ("agent_type", "subagent_type", "agentType", "subagentType"):
        value = data.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _subagent_tool_started_event(data: dict) -> dict:
    """A subagent's tool start. Distinct from parent `tool_started`, and carries
    no parent `turn`: subagent work belongs to no parent turn."""
    return {
        "type": "subagent_tool_started",
        "agent_id": _hook_subagent_id(data),
        "agent_type": _hook_subagent_type(data),
        "tool_use_id": _hook_tool_use_id(data),
        "tool": data.get("tool_name"),
        "input_preview": _event_preview(data.get("tool_input")),
    }


def _subagent_tool_finished_event(data: dict, duration_ms: int | None) -> dict:
    return {
        "type": "subagent_tool_finished",
        "agent_id": _hook_subagent_id(data),
        "agent_type": _hook_subagent_type(data),
        "tool_use_id": _hook_tool_use_id(data),
        "tool": data.get("tool_name"),
        "duration_ms": duration_ms,
        "ok": _tool_ok(data),
        "result_preview": _event_preview(_hook_tool_result(data)),
    }


def subagent_boundary_reply(data) -> dict | None:
    """Hook-event reply for a subagent PostToolUse/SubagentStop, else None.

    Steering is pasted only at the parent's own tool boundaries, where Claude's
    main loop is paused on the hook."""
    if _hook_subagent_id(data) is None:
        return None
    return {"ok": True, "delivered": False, "reason": "subagent hook"}


def notification_sets_waiting(data) -> bool:
    """Whether a Notification hook means the parent input surface is blocked.

    Only the parent `permission_prompt` does. `worker_permission_prompt` is a
    worker's prompt; its payload carries no `agent_id`, so it cannot be owned by
    the parent and stays informational."""
    return isinstance(data, dict) and data.get("notification_type") == "permission_prompt"


ELICITATION_DIALOG_NOTIFICATIONS = ("elicitation_dialog", "elicitation_url_dialog")


ELICITATION_SETTLED_NOTIFICATIONS = ("elicitation_response", "elicitation_complete")


def notification_elicitation_dialog(data) -> bool:
    """Whether a Notification hook reports an unbrokered elicitation dialog."""
    return (isinstance(data, dict)
            and data.get("notification_type") in ELICITATION_DIALOG_NOTIFICATIONS)


def notification_elicitation_settled(data) -> bool:
    """Whether a Notification hook reports that an elicitation was answered."""
    return (isinstance(data, dict)
            and data.get("notification_type") in ELICITATION_SETTLED_NOTIFICATIONS)


def hook_fired_ms(data, received_ms: int) -> int:
    """When the hook fired: the relay's stamp, else the host's receipt time."""
    stamp = data.get("_ccx_relay_ms") if isinstance(data, dict) else None
    if isinstance(stamp, int) and not isinstance(stamp, bool) and 0 < stamp <= received_ms:
        return stamp
    return received_ms


def note_subagent_taint(taint: dict, event_name: str, data, now_ms: int) -> bool:
    """Record evidence that a subagent may be running; True if taint began.

    Claude's native presence (`sessions/<pid>.json` status) stays `busy` while
    any subagent runs, so it cannot attribute activity to the parent. Every
    subagent hook — including SubagentStop, since the agent was live until
    then and presence lags — taints native evidence, as does a running
    subagent in a parent Stop's `background_tasks` snapshot. Taint is never
    removed by hook order (relays can reorder); only fresh native quiescence
    clears it (clear_subagent_taint). `now_ms` is when the hook fired
    (hook_fired_ms), so a late-delivered hook cannot outdate a later native
    idle transition."""
    was_tainted = bool(taint)
    data = data if isinstance(data, dict) else {}
    agent_id = _hook_subagent_id(data)
    if agent_id is not None:
        taint[agent_id] = max(taint.get(agent_id, 0), now_ms)
    elif event_name == "Stop" and isinstance(data.get("background_tasks"), list):
        for t in data["background_tasks"]:
            if (
                isinstance(t, dict) and t.get("type") == "subagent"
                and t.get("status") == "running"
                and isinstance(t.get("id"), str) and t.get("id")
            ):
                # Reordered delivery must never lower a fence.
                taint[t["id"]] = max(taint.get(t["id"], 0), now_ms)
    return bool(taint) and not was_tainted


def clear_subagent_taint(taint: dict, value) -> bool:
    """Clear the taint on native quiescence newer than every taint; True if cleared.

    `idle`/`shell` means nothing — parent or subagent — is running, but only a
    status written strictly after the latest subagent evidence proves it: an
    older sample may predate an agent that has started since, and a
    same-millisecond one cannot be ordered against it (the next native
    transition will clear)."""
    if not taint or value is None or value.status not in {"idle", "shell"}:
        return False
    updated = value.status_updated_at
    if updated is None or updated <= max(taint.values()):
        return False
    taint.clear()
    return True


def _native_hook_descendant(peer_pid: int, worker_pid: int | None,
                            worker_start: str | int | None) -> bool:
    """Linux SO_PEERCRED peer must be the original native child or its relay.

    Native hooks run either in the Claude process or a process it spawned.
    Resolve at most 16 current /proc parent links with an immutable original
    worker PID+start-time anchor, rejecting recycled PIDs and detached peers.
    No owner/name/short/native-id inference is made from arbitrary processes.
    """
    from ccx.store.liveness import proc_start_token
    if (not isinstance(peer_pid, int) or peer_pid < 2
            or not isinstance(worker_pid, int) or worker_pid < 2
            or not worker_start
            or str(proc_start_token(worker_pid)) != str(worker_start)):
        return False
    seen = set()
    current = peer_pid
    for _ in range(16):
        if current in seen or current < 2 or not proc_start_token(current):
            return False
        if current == worker_pid:
            return str(proc_start_token(worker_pid)) == str(worker_start)
        seen.add(current)
        parent = oslayer.process_info(current).ppid  # kernel field 4
        if parent is None:
            return False
        current = parent
    return False
