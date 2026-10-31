from __future__ import annotations
from ccx.constants import (
    S,
)
from ccx.claude.transcript import (
    is_compact_command_text,
)


def input_ready_pty_read(last_pty_read: float, subagent_tainted: bool,
                         now: float, quiescence_s: float) -> float:
    """The PTY-read time the Ink-ready quiescence gates should see.

    A live subagent keeps Claude's TUI redrawing its progress while the parent
    is idle, so PTY output stops proving Ink is mid-render and the gates would
    starve every write until the agent ends. While tainted, the post-IDLE
    `earliest_send_at` settle alone gates readiness; the paste ack/retry path
    still catches a dropped paste."""
    if subagent_tainted and last_pty_read > 0:
        return min(last_pty_read, now - quiescence_s)
    return last_pty_read


def settling_grace_elapsed(state: dict, obs: dict | None, *,
                           now: float, grace_s: float) -> bool:
    """After draining ready hooks, only the same still-settling turn may commit.

    A parent hook may reset the idle candidate while ingress is being drained.
    Its missing timestamp must never be interpreted as a grace beginning at 0.
    """
    return bool(
        obs is not None and obs is state.get("interrupt_observation")
        and obs.get("settling") is True
        and type(obs.get("settling_at")) in (float, int)
        and obs["settling_at"] > 0
        and state.get("settling_turn_ref") == obs.get("turn_ref")
        and state.get("active_turn_ref") == obs.get("turn_ref")
        and now - obs["settling_at"] >= grace_s
    )


def interrupt_native_idle_after_escape(state: dict, obs: dict | None, value) -> bool:
    """An exact active turn is inactive on original native IDLE after ESC.

    Claude's interactive session status describes all work, including agents:
    native BUSY is ambiguous under subagent taint, but IDLE/SHELL means nothing
    is running. No prior BUSY observation or taint-clear is required. Preserve
    exact target identity and the actual ESC-write timestamp as the only fence.
    """
    return bool(
        obs and not obs.get("settling")
        and (state.get("current") in (S.BUSY, S.WAITING)
             if obs.get("passive") else state.get("current") == S.BUSY)
        and isinstance(obs.get("turn_ref"), str) and obs["turn_ref"]
        and state.get("active_turn_ref") == obs["turn_ref"]
        and type(obs.get("interrupt_at_ms")) is int
        and obs["interrupt_at_ms"] > 0
        and value is not None
        and (value.status == "idle" if obs.get("passive")
             else value.status in {"idle", "shell"})
        and (not obs.get("passive") or not getattr(value, "waiting_for", None))
        and type(value.status_updated_at) is int
        and value.status_updated_at > obs["interrupt_at_ms"]
    )


def bind_accepted_original_new_turn(
    state: dict, receipt: dict, *, turn_ref: str,
    native_user_event: dict | None,
    delivery_host_epoch: str | None, current_host_epoch: str,
) -> bool:
    """Bind a late-accepted original to its already started generic turn.

    The caller matched this event against the pinned original Claude native
    transcript. Never attribute a steer, old-host recovery or second original
    to an active turn: the actual session-bound Stop still must settle it.
    """
    if (
        state.get("current") != S.BUSY
        or state.get("active_turn_ref") != turn_ref
        or state.get("active_turn_delivery_id") is not None
        or not isinstance(native_user_event, dict)
        or type(native_user_event.get("offset")) is not int
        or native_user_event["offset"] <= 0
        or not current_host_epoch
        or delivery_host_epoch != current_host_epoch
        or receipt.get("state") != "accepted"
        or receipt.get("route") != "new_turn"
        or receipt.get("placement") != "new_turn"
        or receipt.get("turnRef") != turn_ref
        or receipt.get("evidence") not in {"user-event", "started"}
        # Hot `started` includes queued/dequeue hints. Only a separately
        # re-read original ordinary USER row may bind the active turn.
        or (receipt.get("evidence") == "started"
            and native_user_event.get("kind") != "native-started-user")
        or receipt.get("evidenceOffset") != native_user_event["offset"]
        or receipt.get("evidenceUuid") != native_user_event.get("uuid")
    ):
        return False
    delivery_id = receipt.get("deliveryId")
    if not isinstance(delivery_id, str) or not delivery_id:
        return False
    state["active_turn_delivery_id"] = delivery_id
    return True


def hook_prompt_id(data: dict) -> str | None:
    """Claude's per-user-prompt id carried by parent turn hooks, if any."""
    value = data.get("prompt_id")
    return value if isinstance(value, str) and value else None


def guarded_hook_is_late_while_tainted(
    event_name: str,
    active_turn_ref: str | None,
    hook_prompt_id: str | None,
    settled_prompt_ids,
) -> bool:
    """Late-hook classification while native presence is subagent-tainted.

    Native busy edges cannot be attributed, so the hook's own `prompt_id`
    decides when both it and the settled target's are known: a hook of a
    prompt the settled turn carried is late, any other is genuine (e.g. a fast
    successor's Stop). Without that correlation the guard stays conservative for its short
    backstop window: with a successor ref an old Stop is suppressed; without
    one, a Stop/PreToolUse is treated as the settled target's delayed hook."""
    if hook_prompt_id and settled_prompt_ids:
        return hook_prompt_id in settled_prompt_ids
    if active_turn_ref:
        return event_name == "Stop"
    return True


def compact_settle_fence(operation: dict) -> int:
    """Transcript offset past which a user prompt belongs to a new turn.

    A settled compact operation records ``end_offset``: the byte just past its
    own boundary record. Anything after that is not compaction's doing. Fall
    back to the pre-paste ``fence_offset`` when a verdict carries no end offset
    (a no-op has no boundary to end), which is conservative in the right
    direction — a wider window can only include more of compaction's own
    bookkeeping, and `foreign_prompt_since` already discounts all of it.
    """
    result = operation.get("result") or {}
    for candidate in (result.get("end_offset"), operation.get("fence_offset")):
        if isinstance(candidate, int) and candidate >= 0:
            return candidate
    return 0


def successor_hold_should_release(
    successor_pending: dict | None, active_turn_ref: str | None, native_status: str | None
) -> bool:
    """Should a BUSY/no-ref successor hold fall back to IDLE?"""
    return bool(
        successor_pending is not None
        and not active_turn_ref
        and native_status in {"idle", "shell"}
    )


def queued_steer_needs_successor_hold(slot: dict | None, turn_ref: str | None) -> bool:
    """A steer queued before Stop may start without a second prompt hook."""
    return bool(
        slot is not None
        and slot.get("route") == "steer"
        and slot.get("target_turn_ref") == turn_ref
        and slot.get("maybe_written")
        and slot.get("phase") in {"steer_boundary", "await_evidence"}
    )


def successor_hold_quiescence_is_fresh(
    successor_pending: dict, value, taint: dict[str, int],
) -> bool:
    """Is an inactive native sample fresh enough to release a successor hold?

    Untainted holds keep the baseline rule (any inactive sample). A hold that is
    subagent-ambiguous, or any hold while a subagent is live (including one
    created before taint began), needs quiescence ordered strictly after both
    the subagent evidence and the hold's own busy edge.
    """
    if not taint and not successor_pending.get("subagent_ambiguous"):
        return True
    updated = value.status_updated_at if value is not None else None
    if updated is None:
        return False
    if taint and updated <= max(taint.values()):
        return False
    busy_token = successor_pending.get("native_busy_status_updated_at")
    return busy_token is None or updated > busy_token


def input_surface_write_blocked(state: dict) -> bool:
    """Whether CCX-owned cleanup currently forbids prompt bytes reaching Ink.

    Admission stays open: durable semantic sends may be owned while this is true,
    but every PTY write pump/boundary writer must wait until cleanup releases.
    """
    return state.get("compact_clear") is not None or state.get("cancel_clear") is not None


def post_compact_hold_may_release(
    now: float, last_activity: float, last_pty_read: float, tainted: bool,
    grace_s: float,
) -> bool:
    """May the post-compact BUSY hold fall back to IDLE on silence?

    The hold is backed by evidence of a parent turn. Untainted, combined hook
    and PTY silence past the grace proves no turn runs. While a subagent is
    live neither signal is parent-specific: the child keeps Ink redrawing, and
    a parent in a long tool or model wait is silent in hooks and transcript.
    The hold then waits for parent terminal evidence or fresh quiescence
    (which clears the taint and restores the silence rule).
    """
    if tainted:
        return False
    return now - max(last_activity, last_pty_read) > grace_s


def successor_attribution(inactive_token: int | None, value) -> tuple[bool, int | None]:
    """Classify a native busy edge seen after a settling target went inactive.

    Returns ``(successor, successor_token)``: a busy token newer than the
    target's inactive edge may be queued parent work. With a subagent live the
    aggregate presence cannot tell parent from child, and neither IDLE nor a
    timeout is safe, so the caller still holds BUSY (marked ambiguous) until a
    parent UPS attributes it or fresh native quiescence releases it.
    """
    if (
        value is None or value.status != "busy"
        or value.status_updated_at is None
        or value.status_updated_at <= int(inactive_token or 0)
    ):
        return False, None
    return True, value.status_updated_at


def compact_displaced_by_prompt(state: dict, prompt) -> bool:
    """Did an ordinary user turn just take the input surface from our paste?

    ``/compact`` is pasted without its Enter and submitted by a delayed,
    retried keypress (Ink's autocomplete menu eats a same-write ``\\r``).  That
    leaves a window in which Claude can flush a prompt it had queued from an
    earlier steer: the queued prompt runs as an ordinary turn and our command
    text is left sitting in the editor, never to be submitted.

    A ``UserPromptSubmit`` carrying non-``/compact`` text during that window is
    positive proof of exactly that.  It is the only signal that distinguishes
    the case from native compaction (which also emits hook traffic while it
    summarizes), and without it the operation can only die at its timeout —
    twenty minutes of a caller blocking on a command that already failed.

    Deliberately narrow.  Once submission is acknowledged the native command
    owns the surface and later hook traffic is compaction's own, so an
    acknowledged operation is never displaced.
    """
    compacting = state.get("compacting")
    if compacting is None:
        return False
    if compacting.get("submitted"):
        return False
    if compacting.get("pasted_at") is None:
        # Nothing has been written to the editor yet, so nothing was displaced;
        # the pump's own gate still owns the decision to paste.
        return False
    return not is_compact_command_text(prompt)
