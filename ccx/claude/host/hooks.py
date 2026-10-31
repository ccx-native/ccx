from __future__ import annotations
import json
import os
import pathlib
import socket
import sys
import time
from ccx.constants import (
    BOUNDARY_SEND_ACK_S,
    HOOK_RELAY_RETRY_POLL_S,
    HOOK_RELAY_RETRY_S,
    PERMISSION_HOOK_TIMEOUT_S,
    PERMISSION_POLL_INTERVAL_S,
)
from ccx.store.liveness import lifecycle_host_alive
from ccx.store.paths import sock_path
from ccx.store.records import (
    read_record,
)
from ccx.claude.transcript import (
    api_error_from_event,
)
from ccx.claude.host.subagents import _hook_subagent_id


def _read_new_api_error(transcript_path: str | pathlib.Path, start_pos: int = 0) -> tuple[dict | None, int]:
    """Scan newly-appended JSONL transcript bytes for a terminal API error.

    Returns ``(summary, new_pos)``. If the file is truncated or recreated, the
    scan restarts at byte 0. A partial final line is left unread so the next poll
    can parse it after Claude finishes writing.
    """
    path = pathlib.Path(transcript_path)
    try:
        size = path.stat().st_size
    except OSError:
        return None, start_pos
    pos = max(0, int(start_pos or 0))
    if pos > size:
        pos = 0
    if pos == size:
        return None, size
    try:
        with path.open("rb") as f:
            f.seek(pos)
            while True:
                line_start = f.tell()
                raw = f.readline()
                if not raw:
                    return None, f.tell()
                try:
                    evt = json.loads(raw.decode("utf-8"))
                except (UnicodeDecodeError, ValueError):
                    if not raw.endswith(b"\n"):
                        return None, line_start
                    continue
                if isinstance(evt, dict) and evt.get("isSidechain") is True:
                    continue  # a subagent's error is not the session's
                api_error = api_error_from_event(evt)
                if api_error:
                    return api_error, f.tell()
    except OSError:
        return None, pos


def _native_interrupt_row(
    evt: dict, prompt_ids: set[str] | None = None, *, strict: bool = False,
) -> bool:
    """A native cancellation marker, not user/assistant text quoting its words.

    Claude's plain interruption row carries interruptedMessageId. Its tool
    variant is an error tool_result. Sidechains are never parent cancellation.
    Where Claude supplies promptId, it must match the current turn's UPS IDs.
    """
    if (not isinstance(evt, dict) or evt.get("type") != "user"
            or evt.get("isSidechain") is True):
        return False
    pid = evt.get("promptId")
    if prompt_ids and pid is not None and pid not in prompt_ids:
        return False
    content = (evt.get("message") or {}).get("content")
    markers = {"[Request interrupted by user]",
               "[Request interrupted by user for tool use]"}
    if isinstance(content, str):
        return content in markers and (not strict or bool(evt.get("interruptedMessageId")))
    if isinstance(content, list):
        for block in content:
            if not isinstance(block, dict):
                continue
            if (block.get("type") == "tool_result" and block.get("is_error") is True
                    and isinstance(block.get("content"), str)
                    and block["content"] in markers):
                return True
            if (block.get("type") == "text" and block.get("text") in markers
                    and evt.get("interruptedMessageId")):
                return True
    return False


def _read_interrupt_marker(
    transcript_path: str | pathlib.Path, start_pos: int = 0,
    *, prompt_ids: set[str] | None = None, strict: bool = False,
) -> tuple[bool, int]:
    """Passively scan only complete, current parent JSONL rows for a marker.

    An earlier interrupt, a quoted marker, a partial line, and a sidechain row
    cannot confirm this turn. The scan offset is caller-owned and bounded by
    the start of the current real turn; on truncated transcripts restart at 0.
    """
    path = pathlib.Path(transcript_path)
    try:
        size = path.stat().st_size
    except OSError:
        return False, start_pos
    pos = max(0, int(start_pos or 0))
    if pos > size:
        pos = 0
    if pos == size:
        return False, size
    try:
        with path.open("rb") as f:
            f.seek(pos)
            while True:
                line_start = f.tell()
                raw = f.readline()
                if not raw:
                    return False, f.tell()
                if not raw.endswith(b"\n"):
                    return False, line_start
                if b"[Request interrupted by user" not in raw:
                    continue
                try:
                    evt = json.loads(raw.decode("utf-8"))
                except (UnicodeDecodeError, ValueError):
                    continue
                if _native_interrupt_row(evt, prompt_ids, strict=strict):
                    return True, f.tell()
    except OSError:
        return False, pos


def _permission_response(
    decision: str,
    tool_input: dict | None = None,
    reason: str | None = None,
    *,
    updated_input: dict | None = None,
    user_modified: bool = False,
) -> dict:
    behavior = "allow" if decision == "allow" else "deny"
    default_reason = "ccx approved" if behavior == "allow" else "ccx rejected"
    dec = {
        "behavior": behavior,
        "decisionReason": reason or default_reason,
    }
    if behavior == "allow":
        dec["updatedInput"] = updated_input if updated_input is not None else (tool_input or {})
        dec["userModified"] = user_modified
    else:
        # Claude surfaces a denied tool's message to the model from
        # `decision.message`, not from `decisionReason`. Mirror the reason there
        # so `ccx reject --reason` is visible in the interrupted turn.
        dec["message"] = reason or default_reason
    return {"hookSpecificOutput": {"hookEventName": "PermissionRequest", "decision": dec}}


def _recv_json_line(sock: socket.socket, timeout: float) -> dict:
    sock.settimeout(timeout)
    buf = bytearray()
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            return {"ok": False, "error": "no reply"}
        buf.extend(chunk)
        if b"\n" in buf:
            line = bytes(buf).split(b"\n", 1)[0]
            try:
                return json.loads(line)
            except ValueError:
                return {"ok": False, "error": "bad reply"}


def _relay_connect(short: str, *, timeout: float = 2.0,
                   retry_s: float = 0.0) -> socket.socket:
    """Connect to the host socket, retrying a busy host for up to ``retry_s``.

    The host serves its socket from one select loop. While that loop is busy
    the listen backlog fills, and a further ``connect`` then fails at once with
    EAGAIN (or times out). That is a host that is alive but behind, not a
    host that is gone, so it is retried within the budget. A missing socket or
    a refused connection means no host at all and is raised immediately.
    """
    deadline = time.monotonic() + max(0.0, retry_s)
    while True:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(timeout)
        try:
            s.connect(str(sock_path(short)))
            return s
        except (BlockingIOError, socket.timeout):
            s.close()
            if time.monotonic() >= deadline:
                raise
            time.sleep(HOOK_RELAY_RETRY_POLL_S)
        except BaseException:
            s.close()
            raise


def _host_rpc(short: str, msg: dict, timeout: float = 2.0, *,
              connect_retry_s: float = 0.0) -> dict:
    if msg.get("op") in ("permission-request", "elicitation-request",
                         "permission-poll", "permission-timeout"):
        # Unlike native hook-event frames, these require the host's existing
        # same-connection full-owner handshake; do not exempt them server-side.
        from ccx.rpc.rpc import _rpc
        return _rpc(short, msg, timeout=timeout)
    with _relay_connect(short, timeout=timeout, retry_s=connect_retry_s) as s:
        s.sendall((json.dumps(msg) + "\n").encode())
        return _recv_json_line(s, timeout)


def hook_relay_retry_s(data) -> float:
    """Connect budget for one hook's delivery to the host.

    Parent hooks are lifecycle evidence (a lost Stop leaves the worker busy
    forever), so they wait out a busy host. Subagent hooks only feed
    attributed tool events and keep the single attempt."""
    return 0.0 if _hook_subagent_id(data) is not None else HOOK_RELAY_RETRY_S


def _relay_broker_permission(short: str, data: dict, *,
                             register_op: str = "permission-request",
                             gone: dict | None = None,
                             timed_out: dict | None = None) -> dict | None:
    """Register a no-skip permission request with the host and poll until a
    decision arrives, the host dies, or the hook-timeout budget elapses.

    Returns the decision dict ({"decision": "allow"|"deny", "reason": str}) or
    None if the request could not even be registered (host unreachable).

    Transient poll failures leave the request pending: the host may be busy
    servicing the PTY. Polling ends only on a decision, a missing socket with
    a dead host PID, or the hook-timeout deadline. The caller denies on timeout
    so the blocked turn can proceed.

    The Elicitation hook reuses the same poll loop with ``register_op=
    "elicitation-request"`` and cancel-shaped ``gone``/``timed_out`` results."""
    try:
        reply = _host_rpc(short, {"op": register_op, "data": data}, timeout=2.0,
                          connect_retry_s=HOOK_RELAY_RETRY_S)
    except Exception:
        return None
    request_id = reply.get("request_id")
    if not (reply.get("ok") and request_id):
        return None
    host_lifecycle = (read_record(short) or {}).get("lifecycle") or {}
    deadline = time.time() + max(1.0, PERMISSION_HOOK_TIMEOUT_S - 1.0)
    while time.time() < deadline:
        time.sleep(PERMISSION_POLL_INTERVAL_S)
        try:
            poll = _host_rpc(short, {"op": "permission-poll", "request_id": request_id}, timeout=2.0)
        except Exception:
            poll = None
        if poll and poll.get("ok"):
            if poll.get("decision"):
                return poll["decision"]
            if poll.get("unknown"):
                # A restarted host that never saw this request: nobody can
                # decide it any more.
                return gone or {"decision": "deny",
                                "reason": "ccx: session host restarted before approval"}
            continue  # host answered, still no decision — keep waiting
        # Transient/garbled reply. Only give up if the host is genuinely gone;
        # otherwise keep polling until the real deadline.
        if not sock_path(short).exists() and not lifecycle_host_alive(host_lifecycle):
            return gone or {"decision": "deny", "reason": "ccx: session ended before approval"}
    # Hook-timeout: notify the host (it clears the prompt and stays usable) and
    # auto-deny so Claude unblocks instead of falling back to an interactive
    # prompt that would hang this head-less PTY forever.
    try:
        _host_rpc(short, {"op": "permission-timeout", "request_id": request_id}, timeout=2.0)
    except Exception:
        pass
    return timed_out or {"decision": "deny", "reason": "ccx: no approval within timeout, auto-denied"}


def _elicitation_response(action: str, content: dict | None = None) -> dict:
    """Claude `Elicitation` hook output: answer the MCP server directly.

    Returning an action suppresses Claude's own dialog, which would otherwise
    block a head-less PTY forever. ``cancel`` is the safe default for every
    failure: it means "dismissed", so the server records no decision on the
    user's behalf (a ``decline`` is an explicit no that servers may persist)."""
    out: dict = {"hookEventName": "Elicitation", "action": action}
    if action == "accept" and content is not None:
        out["content"] = content
    return {"hookSpecificOutput": out}


def _relay_elicitation(short: str, data: dict) -> dict:
    """Broker one Elicitation hook through the host; always returns hook output."""
    decision = _relay_broker_permission(
        short, data,
        register_op="elicitation-request",
        gone={"action": "cancel", "reason": "ccx: session ended before a response"},
        timed_out={"action": "cancel", "reason": "ccx: no response within timeout"},
    )
    if not decision:
        return _elicitation_response("cancel")
    action = decision.get("action")
    if action not in ("accept", "decline", "cancel"):
        action = "cancel"
    return _elicitation_response(action, decision.get("content"))


def cmd_hooks_relay(args):
    """Read JSON hook payload from stdin, forward to the host socket as a hook-event op,
    print the hook's response to stdout.

    For most events: respond with "{}" (no-op continue).
    For PermissionRequest on skip-permissions sessions: auto-allow ordinary tool
    permissions with the original tool_input. AskUserQuestion is not a permission:
    always broker it so the thread blocks for a real answer instead of silently
    producing an empty/non-answer. For --no-skip-permissions sessions, never emit
    an allow decision, even if an older frozen settings file still contains the hook."""
    from ccx.constants import SELF_ENGINE_ENV, SELF_WORKER_ID_ENV
    from ccx.claude.settings import HOOKS_WE_CARE_ABOUT

    raw = sys.stdin.read()
    try:
        data = json.loads(raw) if raw.strip() else {}
    except ValueError:
        data = {}
    if not isinstance(data, dict):
        data = {}
    # In V2 read_record raises for missing/foreign/short/corrupt workers.
    # Missing proof can never authorize a permission (or a native hook event).
    try:
        rec = read_record(args.short)
    except Exception:
        rec = None
    native = rec.get("native") if isinstance(rec, dict) else None
    native_sid = native.get("sessionId") if isinstance(native, dict) else None
    original = (
        isinstance(rec, dict)
        and rec.get("engine") == "claude"
        and rec.get("workerId") == args.short
        and isinstance(native_sid, str) and bool(native_sid)
        and data.get("session_id") == native_sid
        and os.environ.get(SELF_WORKER_ID_ENV) == args.short
        and os.environ.get(SELF_ENGINE_ENV) == "claude"
        and args.event in HOOKS_WE_CARE_ABOUT
    )
    if not original:
        if args.event == "PermissionRequest":
            sys.stdout.write(json.dumps(_permission_response(
                "deny", data.get("tool_input"),
                "ccx: unverified original managed worker hook",
            )) + "\n")
        elif args.event == "Elicitation":
            sys.stdout.write(json.dumps(_elicitation_response("cancel")) + "\n")
        else:
            sys.stdout.write("{}\n")
        return
    if args.event == "UserPromptSubmit":
        # Capture before the host ACK wait. Native paths can have hashed/long
        # project slugs; this is only a byte-offset hint, never read authority.
        transcript_path = data.get("transcript_path")
        if (isinstance(transcript_path, str)
                and os.path.isabs(transcript_path)
                and os.path.basename(transcript_path) == native_sid + ".jsonl"):
            try:
                data["_ccx_report_start_offset"] = os.path.getsize(transcript_path)
            except FileNotFoundError:
                data["_ccx_report_start_offset"] = 0
            except OSError:
                pass
    dargs = rec.get("createArgs") or {}
    auto_allow_permissions = dargs.get("skip_permissions", True)
    if args.event == "Elicitation":
        # An MCP server asked the user for input mid-tool. Like AskUserQuestion
        # it is a real question, brokered even on skip-permissions sessions.
        sys.stdout.write(json.dumps(_relay_elicitation(args.short, data)) + "\n")
        return
    if args.event == "PermissionRequest":
        tool_name = data.get("tool_name")
        if auto_allow_permissions and tool_name != "AskUserQuestion":
            sys.stdout.write(json.dumps(_permission_response(
                "allow",
                data.get("tool_input", {}),
                "ccx auto-allow (--dangerously-skip-permissions session)",
            )) + "\n")
            return
        # Broker through the host and wait for a CLI decision. Every failure mode
        # resolves to a *deny* rather than a bare "{}": a no-op "{}" lets Claude
        # fall back to its own interactive permission prompt, which in a head-less
        # ccx PTY hangs forever. Denying keeps the session responsive and is the
        # safe default whenever we cannot obtain an explicit allow.
        tool_input = data.get("tool_input", {})
        decision = _relay_broker_permission(args.short, data)
        if decision is None:
            sys.stdout.write(json.dumps(_permission_response(
                "deny", tool_input, "ccx: session host unreachable, cannot broker approval")) + "\n")
            return
        if decision.get("external") is True:
            # The native PostToolUse(/Failure) already resolved this exact
            # original input. Empty hook output makes no second decision.
            sys.stdout.write("{}\n")
            return
        sys.stdout.write(json.dumps(_permission_response(
            decision.get("decision", "deny"),
            tool_input,
            decision.get("reason"),
            updated_input=decision.get("updatedInput"),
            user_modified=bool(decision.get("userModified")),
        )) + "\n")
        return

    # When Claude fired this hook, not when the host reads it: relays run as
    # separate processes and can deliver late or reordered.
    data["_ccx_relay_ms"] = int(time.time() * 1000)
    msg = {"op": "hook-event", "event": args.event, "data": data}
    retry_s = hook_relay_retry_s(data)
    try:
        if args.event in ("SessionStart", "UserPromptSubmit", "PreToolUse",
                          "Stop", "PostToolUse", "PostToolUseFailure", "SubagentStop"):
            # Keep the relay connected until the host has consumed the
            # complete event. Darwin may drop a just-closed PreToolUse relay
            # before the host loop processes it. The bounded acknowledgement
            # is not a reason to replay any hook after an unknown result.
            _host_rpc(args.short, msg, timeout=BOUNDARY_SEND_ACK_S + 2.0,
                      connect_retry_s=retry_s)
        else:
            with _relay_connect(args.short, retry_s=retry_s) as s:
                s.sendall((json.dumps(msg) + "\n").encode())
    except Exception:
        # Hook must not fail Claude. Swallow errors silently.
        pass
    sys.stdout.write("{}\n")
