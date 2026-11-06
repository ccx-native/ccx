from __future__ import annotations
import fcntl
import json
import os
import pathlib
import pty
import queue
import select
import signal
import socket
import stat
import sys
import threading
import time
import traceback
from ccx.system import oslayer
from ccx.claude.attach import HostAttachment, MAX_ATTACH_LINE
from ccx.claude.delivery import (
    HOST_LOCK_TIMEOUT_S as SEMANTIC_LOCK_TIMEOUT_S,
    ClaudeDeliveryBusy,
    ClaudeDeliveryConflict,
    ClaudeDeliveryError,
    iter_receipts as iter_semantic_receipts,
    mark_accepted as mark_semantic_accepted,
    mark_attempt as mark_semantic_attempt,
    mark_evidence as mark_semantic_evidence,
    mark_owned as mark_semantic_owned,
    mark_rejected as mark_semantic_rejected,
    mark_settled as mark_semantic_settled,
    mark_retryable_refusal as mark_semantic_refusal,
    prepare as prepare_semantic_delivery,
    prune_receipts as prune_semantic_receipts,
    public_receipt as public_semantic_receipt,
    read_receipt as read_semantic_receipt,
    valid_delivery_id as valid_semantic_delivery_id,
    validate_cap as validate_semantic_cap,
    spec_digest as semantic_spec_digest,
)
from ccx.ops.background import (
    claude_task_item, diff_items, materially_changed,
    summary as background_summary,
)
from ccx.ops.compact_ops import (
    HOST_LOCK_TIMEOUT_S as COMPACT_LOCK_TIMEOUT_S,
    PENDING_STATUSES as COMPACT_PENDING_STATUSES,
    CompactJournalBusy,
    begin_operation as begin_compact_operation,
    get_operation as get_compact_operation,
    latest_operation as latest_compact_operation,
    compact_intent_digest,
    public_operation as public_compact_operation,
    reconcile_operation as reconcile_compact_operation,
    reconcile_stale_operations as reconcile_stale_compact_operations,
    transition_operation as transition_compact_operation,
    transcript_identity,
    transcript_offset,
)
from ccx.ops.steer_ops import (
    HOST_LOCK_TIMEOUT_S as STEER_LOCK_TIMEOUT_S,
    PENDING_STATUSES as STEER_PENDING_STATUSES,
    begin_delivery as begin_steer_delivery,
    fail_semantic_delivery_unwritten as fail_semantic_hot_unwritten,
    get_delivery as get_steer_delivery,
    mark_pasted as mark_steer_pasted,
    pending_deliveries as pending_steer_deliveries,
    public_delivery as public_steer_delivery,
    read_journal as read_steer_journal,
    reconcile_delivery as reconcile_steer_delivery,
    reset_semantic_delivery_for_retry as reset_semantic_hot_for_retry,
    transition_delivery as transition_steer_delivery,
    unproven_deliveries as unproven_steer_deliveries,
)
from ccx.ops.context_events import ContextChanges
from ccx.ops.semantics import KIND_ELICITATION, KIND_PERMISSION, elicitation_response_problem
from ccx.constants import (
    BOOT_TIMEOUT_S,
    INTERRUPT_CONFIRM_S,
    INTERRUPT_CONTINUED_ACTIVITY_GRACE_S,
    INTERRUPT_NATIVE_POLL_S,
    LATE_HOOK_BACKSTOP_S,
    LATE_HOOK_PROMPT_ID_S,
    SEMANTIC_EVIDENCE_GRACE_S,
    PASSIVE_NATIVE_IDLE_GRACE_S,
    BOUNDARY_SEND_POLL_S,
    CAP_POLL_INTERVAL_S,
    CCX_EVENTS_KEEP,
    CCX_EVENTS_MAX,
    CCX_EVENTS_ROTATE_CHECK_INTERVAL,
    CLEAR_INPUT_MIN_GAP_S,
    COMPACT_POLL_S,
    COMPACT_SUBMIT_DELAY_S,
    COMPACT_SUBMIT_RETRY_S,
    COMPACT_WAIT_DEFAULT_S,
    DEFAULT_IDLE_TIMEOUT_S,
    DEFAULT_PTY_COLS,
    DEFAULT_PTY_ROWS,
    POST_CLEAR_SETTLE_S,
    POST_COMPACT_HOLD_GRACE_S,
    PASTE_SUBMIT_DELAY_S,
    PTY_QUIESCENCE_S,
    PTY_WRITE_TIMEOUT_S,
    S,
    SEND_FIRST_ACK_S,
    SEND_RETRY_ACK_S,
    STEER_BOUNDARY_ACK_S,
    USAGE_STALE_LIMIT_S,
    WARMUP_S,
)
from ccx.store.liveness import proc_start_token, read_managed_claude_native_state
from ccx.claude.managed_turns import latest_settled_turn, new_turn_ref
from ccx.store.paths import accept_same_user, bind_private_unix_socket, events_ndjson_path, settings_path, sock_path
from ccx.store.records import (
    commit_host_turn_settlement, default_claude_name,
    host_live_transition, host_terminal_transition, merge_lifecycle,
    read_record, restore_host_turn_count, take_host_boot_fields,
)
from ccx.claude.transcript import (
    api_error_reason,
    compact_command_seen_since,
    exact_user_prompt_event,
    original_started_user_event,
    BackgroundTaskReducer,
    read_background_append,
    background_task_current,
    native_transcript_start,
    foreign_prompt_since,
    is_compact_command_text,
    normalize_semantic_prompt_text,
    prompt_matches,
    queued_prompt_accepted,
    read_ai_title,
    refresh_context_observation,
)
from ccx.claude.usage import _merge_caps, cap_tripped, fetch_usage, read_usage_cached, unknown_cap_metrics
from ccx.claude.host.events import _count_lines, _elicitation_request, _hook_tool_use_id, _oldest_native_gate, _open_event_journal, _permission_preview, _rotate_events_file, _stamp_event, _subagent_finished_event, _tool_finished_event, _tool_started_event
from ccx.claude.host.hooks import _read_interrupt_marker, _read_new_api_error
from ccx.claude.host.pty import clear_input_bytes, drain_bytes, get_winsize_rows, inline_paste_newline_cap, paste_bytes, paste_chunks, prompt_paste, pty_tail_reason, set_winsize, stalled_paste_clear_action, unstick_bytes, write_all
from ccx.claude.host.subagents import _hook_subagent_id, _native_hook_descendant, _subagent_tool_finished_event, _subagent_tool_started_event, clear_subagent_taint, hook_fired_ms, note_subagent_taint, notification_elicitation_dialog, notification_elicitation_settled, notification_sets_waiting, subagent_boundary_reply
from ccx.claude.host.successor import bind_accepted_original_new_turn, compact_displaced_by_prompt, compact_settle_fence, guarded_hook_is_late_while_tainted, hook_prompt_id, input_ready_pty_read, input_surface_write_blocked, interrupt_native_idle_after_escape, post_compact_hold_may_release, queued_steer_needs_successor_hold, settling_grace_elapsed, successor_attribution, successor_hold_quiescence_is_fresh, successor_hold_should_release


def should_fail_boot(state: dict, now: float, timeout: float) -> str | None:
    """Decide whether a stalled boot should be failed into a terminal state.

    A worker leaves S.BOOTING only via a SessionStart hook event. If Claude
    blocks before SessionStart (e.g. an un-pre-seeded --add-dir trust dialog or
    an onboarding gate), no hook ever fires; the idle-killer is gated on S.IDLE
    so it never reaps the stalled boot and the host hangs indefinitely. This is
    the watchdog decision, factored out as a pure helper for direct testing.

    `state["since"]` is pinned at boot time while BOOTING (set_state updates it
    on every transition, but a stalled boot never transitions), so it doubles as
    the elapsed-since-boot clock. Returns a reason string when the boot has
    exceeded `timeout`, else None. A non-positive `timeout` disables the check.
    """
    if timeout <= 0:
        return None
    if state.get("current") != S.BOOTING:
        return None
    if now - state.get("since", now) > timeout:
        return (f"boot stalled: no SessionStart within {timeout:.0f}s "
                f"(trust dialog or onboarding gate?)")
    return None


INHERITED_CLAUDE_IDENTITY_VARS = (
    "CLAUDE_CODE_CHILD_SESSION",
    "CLAUDE_CODE_SESSION_ID",
    "CLAUDE_CODE_ENTRYPOINT",
)


def worker_env(
    base: dict | None = None, *,
    worker_id: str | None = None,
    engine: str = "claude", short: str | None = None,
) -> dict:
    """Build a Claude subprocess environment with attested full-ID stamp."""
    from ccx.constants import SELF_ENGINE_ENV, SELF_WORKER_ID_ENV
    from ccx.errors import SessionUnavailable
    from ccx.store.identity import IdentityError
    from ccx.store.root import GenerationError, require_ccx_active
    from ccx import constants
    if short is not None:
        raise SessionUnavailable(
            "legacy short worker_env cannot launch full-UUID host"
        )
    env = dict(os.environ if base is None else base)
    from ccx.controllers.live_vars import sanitize_inherited_shell_bridge_env
    sanitize_inherited_shell_bridge_env(env)
    # This is a new/revived full-UUID owner, never a child shell of the
    # submitting worker. In particular no inherited BASH_ENV function may
    # mask the child's own CLAUDE_ENV_FILE python3 bridge.
    for key in tuple(env):
        if key.startswith("CCX_SELF_") or key in INHERITED_CLAUDE_IDENTITY_VARS:
            env.pop(key, None)
    env["CLAUDE_CODE_FORCE_SESSION_PERSISTENCE"] = "1"
    # Claude runs in ccx's own terminal, which `ccx attach` shows. A worker
    # started from an agent's shell inherits TERM=dumb (often with NO_COLOR)
    # and would otherwise draw without colors.
    if env.get("TERM", "dumb") == "dumb":
        env["TERM"] = "xterm-256color"
        env.pop("NO_COLOR", None)
    if worker_id is not None:
        if engine != "claude":
            raise SessionUnavailable("Claude host requires Claude worker engine")
        try:
            owner = require_ccx_active(constants.CCX_ROOT).resolve(
                f"claude:{worker_id}"
            )
        except (GenerationError, IdentityError, OSError) as exc:
            raise SessionUnavailable(
                f"full-UUID Claude host identity unproven: {exc}"
            ) from exc
        env[SELF_WORKER_ID_ENV] = owner.worker_id
        env[SELF_ENGINE_ENV] = owner.engine
    return env


def adopt_ai_title(short: str, transcript_path) -> bool | None:
    """Adopt the ai-generated session title as the record name if still default.

    Returns True if the name was updated, False if the name was already custom
    (no update needed), or None if no ai-title is present in the transcript yet
    (caller should retry later).

    Only overwrites the synthesized default ``ccx-<short>`` name — user-set
    custom names are never touched.  Called at PreToolUse (the first reliable
    hook where the ai-title is guaranteed to be in the JSONL, since Claude Code
    writes it before the first assistant event) and at Stop as a fallback for
    tool-free first turns.
    """
    title = read_ai_title(transcript_path)
    if title is None:
        return None
    from ccx.store.root import RecordConflict, require_ccx_active
    from ccx.store.records import sanitize_name
    from ccx import constants
    normalized = sanitize_name(title)
    if not normalized:
        return None
    try:
        require_ccx_active(constants.CCX_ROOT).mutate_record(
            f"claude:{short}", patches={"name": normalized},
            expect={"name": default_claude_name(short)},
        )
    except RecordConflict:
        # A concurrent explicit rename wins over transcript title adoption.
        return False
    return True


def _semantic_slot_from_receipt(receipt: dict, hot: dict | None = None) -> dict:
    """Recover the original immutable cap even if the hot journal was evicted."""
    hot = hot or {}
    if "cap" in hot and hot["cap"] != receipt.get("cap"):
        raise ClaudeDeliveryConflict("hot steer cap contradicts semantic receipt")
    attempts = max(int(receipt.get("attempts") or 0), int(hot.get("attempts") or 0))
    hot_status = hot.get("status")
    maybe_written = bool(receipt.get("maybeWritten") or hot.get("maybe_written")
                         or hot_status in {"pasted", "enqueued", "consumed", "started", "unknown"})
    route = receipt.get("route")
    phase = ("await_evidence" if maybe_written else
             "new_turn" if route == "new_turn" else "steer_boundary")
    if route == "steer" and not maybe_written:
        phase = "new_turn_fallback"
    return {
        "delivery_id": receipt["deliveryId"],
        "text": receipt.get("promptText") or hot.get("text") or "",
        "mode": receipt.get("requestedMode"), "route": route,
        "target_turn_ref": receipt.get("targetTurnRef"),
        "cap": receipt.get("cap"),
        "phase": phase, "writes": [], "maybe_written": maybe_written,
        "attempts": attempts,
    }


REQUIRE_IDLE_BUSY = "busy"


REQUIRE_IDLE_WAITING = "waiting"


def reap_worker(child_pid: int) -> bool:
    """Reap the `claude` worker if it has exited; True once it is gone.

    Asks about the worker's own pid only. SIGCHLD coalesces, but a by-pid
    WNOHANG still sees the worker's exit whatever other child raised the
    signal; and a ``waitpid(-1)`` drain would also reap children that belong
    to their own waiters (``subprocess.run`` in the usage refresher thread),
    whose exit status then reads as 0."""
    try:
        wpid, _status = os.waitpid(child_pid, os.WNOHANG)
    except ChildProcessError:
        return True  # already reaped
    return wpid == child_pid


def host_socket_answers(short: str) -> bool:
    """Is something listening on this session's host socket?

    A full listen backlog (EAGAIN) or a connect timeout is a live but busy
    host; only a missing or refused socket is stale."""
    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    probe.settimeout(1.0)
    try:
        probe.connect(str(sock_path(short)))
        return True
    except (BlockingIOError, socket.timeout):
        return True
    except OSError:
        return False
    finally:
        probe.close()


def permission_args(dargs: dict) -> list[str]:
    """Claude permission flags for a managed worker's spawn args.

    A restricted worker with no explicit mode is launched in Claude's
    ``default`` mode: otherwise a user ``defaultMode`` such as ``auto`` or
    ``acceptEdits`` in Claude's settings answers the permission prompts itself
    and CCX's approve/reject gate never sees one. An explicit mode wins.
    """
    args = []
    if dargs.get("skip_permissions", True):
        args.append("--dangerously-skip-permissions")
    if dargs.get("permission_mode"):
        args += ["--permission-mode", dargs["permission_mode"]]
    elif not dargs.get("skip_permissions", True):
        args += ["--permission-mode", "default"]
    return args


def cmd_host(args):
    'Internal: run the session host process. Owns the PTY, listens on a Unix\n    socket, drives the lifecycle FSM from hook events arriving over that socket.\n\n    With ``--detach`` this process is only the *launcher*: it daemonizes the\n    real host (which re-enters this command without --detach), reports a JSON\n    verdict on stdout, and exits. See :mod:`ccx.claude.spawn`.'
    # Real internal host starts must prove the exact original submitted
    # activation on the current global full-ID owner. This gate precedes
    # every logfile, socket, pidfile or native Claude process side effect,
    # including the detached launcher fork.
    from ccx.claude.spawn import _verified_full_uuid_host_start
    args.short = _verified_full_uuid_host_start(args.short)
    if getattr(args, "detach", False):
        from ccx.claude.spawn import run_detached_launcher
        sys.exit(run_detached_launcher(
            args.short, getattr(args, "sock_wait", None) or 5.0))
    from ccx import constants
    from ccx.store.root import require_ccx_active
    store = require_ccx_active(constants.CCX_ROOT)
    with _host_process_lease(store, args.short):
        return _run_host(args)


def _host_process_lease(store, worker_id):
    """Stable full-ID lifetime flock; never holds activation across a turn."""
    from contextlib import contextmanager
    from ccx.store.root import StorageIntegrityError
    @contextmanager
    def lifetime():
        # Source and owner were validated by _verified_full_uuid_host_start.
        with store.lease():
            with store._gen("runtime-metadata", "host-locks", create=True) as directory:
                filename = worker_id + ".lease.lock"
                fd = os.open(
                    filename, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW
                    | os.O_CLOEXEC, 0o600, dir_fd=directory,
                )
                try:
                    st = os.fstat(fd)
                    if (not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid()
                            or st.st_mode & 0o077):
                        raise StorageIntegrityError(
                            "Claude host lifetime lock is not owner-private"
                        )
                    try:
                        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except BlockingIOError as exc:
                        raise StorageIntegrityError(
                            "another Claude host owns this full worker UUID"
                        ) from exc
                    current = os.stat(filename, dir_fd=directory,
                                      follow_symlinks=False)
                    if (current.st_dev, current.st_ino) != (st.st_dev, st.st_ino):
                        raise StorageIntegrityError("host lifetime lock inode replaced")
                except BaseException:
                    os.close(fd)
                    raise
        # ACTIVE lease is now released; OS lock stays held through the loop.
        # CLOEXEC keeps the native provider child from inheriting the lock.
        try:
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)
    return lifetime()


def _run_host(args):
    """Original host select/PTY loop under verified full-ID lifetime lock."""
    short = args.short
    rec = read_record(short)
    if rec is None:
        sys.stderr.write(f"[host {short}] no record\n")
        sys.exit(1)
    if rec.get("managed") is False and rec.get("origin") == "external":
        sys.stderr.write(f"[host {short}] refusing to host external read-only record\n")
        sys.exit(1)
    if host_socket_answers(short):
        # Another host owns this session. Rebinding would orphan it behind an
        # unlinked socket and put two `claude` processes on one session id.
        sys.stderr.write(f"[host {short}] refusing to start: a live host owns the socket\n")
        sys.exit(1)
    from ccx.system.engine_binary import resolve_engine_binary
    from ccx.errors import SessionUnavailable
    try:
        claude_bin = resolve_engine_binary("claude")
    except SessionUnavailable as error:
        # Refuse before the PTY child can hide an exec failure.
        sys.stderr.write(f"[host {short}] {error}\n")
        merge_lifecycle(short, state=S.CRASHED, lastError=str(error))
        sys.exit(error.exit_code)

    # First fork copy is deliberately deferred until this host has listened.
    # A copied fork must still prove its history before the native PTY starts.
    is_fork = rec.get("forkAnchor") is not None

    cwd = rec["cwd"]
    native = rec.get("native")
    session_id = native.get("sessionId") if isinstance(native, dict) else None
    if not isinstance(session_id, str):
        raise RuntimeError("full-UUID host has no original provider-native sessionId")
    # Consume only host-owned run cap and stale waiting preview, atomically
    # under the per-full-UUID semantic lock, with absent-key pop semantics.
    # Never replay stale name/config/lifecycle across a concurrent CLI update.
    active_cap, rec = take_host_boot_fields(short)
    active_cap = active_cap or None
    dargs = rec.get("createArgs", {})

    os.chdir(cwd)

    # Write our pid for diagnostics
    host_pid = os.getpid()
    # The exact worker record lifecycle is authoritative; never create a
    # second untrusted 0644 pidfile from an original short alias.
    # The start token lets readers tell this host from a later process that
    # reuses its pid (see liveness.owned_pid_alive).
    merge_lifecycle(short, hostPid=host_pid, hostStart=proc_start_token(host_pid))

    from ccx import constants
    from ccx.store.root import require_ccx_active
    store = require_ccx_active(constants.CCX_ROOT)
    # Event sidecar: every broadcast is stamped with an envelope and tee'd to
    # <short>.events.ndjson so dashboards/orchestrators can read history for a
    # dead session and replay-then-subscribe for a live one. `event_epoch`
    # identifies this host life (changes on respawn → subscribers know `seq`
    # restarted). Fail-open: if the file can't be opened, broadcasts still go to
    # live subscribers and the host runs normally without persistence.
    event_epoch = f"{host_pid}-{int(time.time() * 1000)}"
    try:
        event_file = _open_event_journal(store, short)
    except OSError:
        event_file = None
    # Live line-count of the sidecar, used as a cheap pre-gate so the rotation
    # check only reads/rewrites the whole file when we are actually over the
    # cap. Seeded once from the file a resume inherits (fast newline count, no
    # JSON parsing), then maintained incrementally per appended line.
    event_lines = _count_lines(events_ndjson_path(short)) if event_file is not None else 0

    # Confirmed owner+resuming is a new host *lifetime* of the same native
    # conversation. It must never fall through to fresh --session-id.
    original_activation = store.native_activation(short)
    reviving = (original_activation.get("state") == "confirmed"
                and (rec.get("lifecycle") or {}).get("state") == S.RESUMING)
    tr = pathlib.Path(rec["transcript"])
    transcript_size = tr.stat().st_size if tr.exists() else 0
    is_resume = transcript_size > 0
    if (reviving or rec.get("origin") == "ccx-import") and not is_resume:
        raise RuntimeError("original Claude resume has no native transcript; "
                           "refusing --session-id replacement")
    # Mode is spliced in just before pty.fork: a first fork starts with no
    # child transcript, and only the copy after listen can make it resumable.
    argv = [claude_bin]
    from ccx.claude.settings import write_settings
    write_settings(short)
    argv += ["--settings", str(settings_path(short))]
    # Only pass an explicit display name when the user (or a previously adopted
    # ai-title) actually set one. Passing the synthesized ``ccx-<short>`` fallback
    # via ``-n`` sets a custom title, which suppresses Claude Code's own ai-title
    # generation — defeating adopt_ai_title, so the worker would keep the fallback
    # name forever. Omitting ``-n`` here lets Claude generate the ai-title, which
    # adopt_ai_title then picks up at PreToolUse / Stop.
    if rec.get("name") and rec["name"] != default_claude_name(short):
        argv += ["-n", rec["name"]]
    argv += permission_args(dargs)
    if dargs.get("model"):
        argv += ["--model", dargs["model"]]
    if dargs.get("effort"):
        argv += ["--effort", dargs["effort"]]
    if dargs.get("agent"):
        argv += ["--agent", dargs["agent"]]
    if dargs.get("append_system_prompt"):
        argv += ["--append-system-prompt", dargs["append_system_prompt"]]
    if dargs.get("allowed_tools"):
        argv += ["--allowed-tools", dargs["allowed_tools"]]
    if dargs.get("disallowed_tools"):
        argv += ["--disallowed-tools", dargs["disallowed_tools"]]
    for d in (dargs.get("add_dir") or []):
        argv += ["--add-dir", d]
    # A ccx worker is a standalone top-level agent, not a sub-session of the
    # Claude Code instance that launched it. Strip the inherited child-session
    # identity (and force persistence) so the worker writes its own transcript;
    # otherwise CLAUDE_CODE_CHILD_SESSION makes Claude skip persistence and
    # report/peek/logs go blind. See worker_env().
    child_env = worker_env(worker_id=short)
    from ccx.controllers.live_vars import configure_claude_worker_env
    child_env = configure_claude_worker_env(
        child_env, short, host_pid=os.getpid()
    )

    # Set up the Unix socket before the worker exists, so its first hooks
    # (SessionStart) always find a listener; they queue in the backlog until
    # the loop starts. A stale socket from a dead host is replaced; a live one
    # was refused above.
    from ccx.rpc.transport import plan_transport, ensure_transport_parent
    plan = plan_transport(store, target=f"claude:{short}")
    ensure_transport_parent(store, plan)
    sp = plan.final
    try:
        sp.unlink()
    except FileNotFoundError:
        pass
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    bind_private_unix_socket(server, sp)
    # Hooks from parallel tools/subagents arrive in bursts while the loop may
    # be busy for seconds; a small backlog makes their relays fail fast.
    server.listen(socket.SOMAXCONN)
    server.setblocking(False)

    if is_fork:
        from ccx.ops.forking import _claude_copy_proof, copy_original_claude_fork
        try:
            current = store.record(f"claude:{short}")
            copy_proof = _claude_copy_proof(current)
            original_activation = store.native_activation(f"claude:{short}")
            if (current.get("forkAnchor") != rec.get("forkAnchor")
                    or current.get("forkedFrom") != rec.get("forkedFrom")
                    or (current.get("native") or {}).get("sessionId") != session_id):
                raise RuntimeError("original Claude fork identity/anchor changed")
            if copy_proof is None:
                if original_activation.get("state") != "submitted-unknown":
                    raise RuntimeError("unproven Claude fork cannot copy during revival")
                current = copy_original_claude_fork(
                    store, current,
                    submission_id=original_activation.get("submissionId"),
                    host_pid=host_pid,
                    host_start=(current.get("lifecycle") or {}).get("hostStart"),
                )
                if _claude_copy_proof(current) is None:
                    raise RuntimeError("original Claude fork copy never became proven")
            elif original_activation.get("state") == "submitted-unknown":
                # This is a different internal host invocation with an already
                # admitted first native lifetime. Even a proven copy cannot
                # authorize another unknown native PTY start.
                raise RuntimeError("original Claude fork native startup is still unknown")
            tr = pathlib.Path(current["transcript"])
            if not tr.is_file() or tr.stat().st_size <= 0:
                raise RuntimeError("proven Claude fork has no native transcript to resume")
            is_resume = True
        except Exception as error:
            # No PTY exists yet: close only this just-bound socket and retain
            # the original copy-attempt/unknown marker for later inspection.
            reason = f"Claude fork {short} native copy/start refused: {error}"
            server.close()
            try:
                sp.unlink()
            except FileNotFoundError:
                pass
            merge_lifecycle(short, state=S.CRASHED, lastError=reason)
            sys.stderr.write(f"[host {short}] {reason}\n")
            sys.exit(1)
    argv[1:1] = ["--resume" if is_resume else "--session-id", session_id]

    # The first fork's transcript copy happens above; fence only native work
    # written after this Claude process starts, not the inherited history.
    native_start = tr.stat().st_size if tr.exists() else 0
    # Fork PTY
    child_pid, master_fd = pty.fork()
    if child_pid == 0:
        try:
            os.execvpe(claude_bin, argv, child_env)
        finally:
            os._exit(127)

    # A fresh worker owns no prior failure: clear the previous run's API-error
    # diagnosis so a later crash cannot inherit it.
    merge_lifecycle(short, workerPid=child_pid, workerStart=proc_start_token(child_pid),
                    nativeTranscriptStart=native_start,
                    state=S.RESUMING if reviving else S.BOOTING, apiError=None)
    set_winsize(master_fd, DEFAULT_PTY_ROWS, DEFAULT_PTY_COLS)
    # Non-blocking
    flags = fcntl.fcntl(master_fd, fcntl.F_GETFL)
    fcntl.fcntl(master_fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)


    # SIGCHLD handler
    child_exited = [False]
    def on_sigchld(signum, frame):
        # Signal-safe: flag only, no logging.
        if reap_worker(child_pid):
            child_exited[0] = True
    signal.signal(signal.SIGCHLD, on_sigchld)

    # Set when the PTY master read hits EOF/OSError (worker closed its end or is
    # gone). A backstop for the case where SIGCHLD is missed/coalesced or the
    # worker dies without us reaping it: the read loop notices the dead PTY and
    # breaks instead of spinning on a master_fd that select() keeps reporting
    # readable but that only ever yields b''.
    pty_gone = [False]

    sigterm_pending = [False]
    def on_sigterm(signum, frame):
        sigterm_pending[0] = True
    signal.signal(signal.SIGTERM, on_sigterm)
    signal.signal(signal.SIGINT, on_sigterm)

    # The host event sidecar is the turn-count authority across restarts.
    # Reconcile the cached record count before accepting work.
    recorded_turns = int(rec.get("stats", {}).get("turns", 0) or 0)
    persisted_turns = latest_settled_turn(
        short,
        fallback=recorded_turns,
        record_last_settled_at_ms=rec.get("lastSettledAt") or rec.get("lastTurnAt"),
    )
    # The sidecar repair cannot decrement a newer durable ordinal or
    # erase independent CLI config. The returned count drives hot state.
    rec = restore_host_turn_count(short, persisted_turns)
    persisted_turns = rec["stats"]["turns"]
    state = {
        "current": S.BOOTING,
        "since": time.time(),
        "last_activity": time.time(),
        "turns": persisted_turns,
        # Start-unique identity for the real model turn currently in flight.
        # Independent from the completion counter: a crashed turn may never
        # consume state["turns"], but its identity must never be reused.
        "active_turn_ref": None,
        "active_turn_delivery_id": None,
        "semantic_delivery": None,
        "subscribers": set(),
        "clients": {},
        "handshaken_clients": set(),
        "pty_buf": bytearray(),
        # Gate new turns until the idle warmup and PTY quiescence finish.
        "earliest_send_at": 0.0,
        # Wall-clock of the most recent PTY-output byte. Used together with
        # PTY_QUIESCENCE_S to detect "Ink finished rendering and is input-ready."
        "last_pty_read": 0.0,
        # Set when a compact settled while a real turn was already running: BUSY
        # is being held for that turn, not for the (finished) compact.
        "post_compact_hold": None,
        # Set by UserPromptSubmit when an ordinary turn starts while a compact
        # operation holds BUSY. Armed at compact-start, consumed by the settle.
        "compact_turn_started": False,
        "pending_permission": None,
        # Brokered requests that arrived while another one held the gate, in
        # arrival order. Parallel tool calls and subagents can each block on a
        # PermissionRequest/Elicitation hook at once; the head of this FIFO is
        # promoted to `pending_permission` when the published gate resolves.
        "permission_queue": [],
        # A permission prompt or elicitation dialog seen only via Claude's
        # Notification hook (no brokered request to release). Shown with no
        # request id: only the worker terminal (ccx attach) can answer it.
        "observed_input": None,
        "permission_decisions": {},
        "permission_counter": 0,
        # Native /compact is a local slash-command, not a normal user prompt.
        # While active, the host watches the transcript for compact_boundary or
        # a local-command no-op and returns to IDLE without expecting Stop.
        "compacting": None,
        # Armed when a compact is aborted with its `/compact` text still in the
        # editor: {"unstuck_at": float|None, "cleared_at": float|None}. Drains
        # that residue once the worker is idle, so the next send cannot paste
        # onto it and submit "/compact<prompt>".
        "compact_clear": None,
        # Early user-cancel can restore the cancelled prompt into
        # Ink. This cleanup uses the same separated unstick/clear ticks as the
        # send retry (the restored prompt can span many lines, so it is emptied
        # with Ctrl-C) and blocks PTY write pumps, not semantic admission.
        "cancel_clear": None,
        # Monotonic time of the host's last input-clearing Ctrl-C; see
        # write_clear_input().
        "clear_input_at": 0.0,
        # Exact-turn native liveness observer, separate from the transcript marker
        # probe.
        "interrupt_observation": None,
        "settling_turn_ref": None,
        "late_hook_guard": None,
        "successor_pending": None,
        # Last native sample is diagnostic/cache only; every decision still checks
        # identity and exact status/token.
        "native_sample": None,
        # Subagent evidence (agent_id -> ms). While non-empty, Claude's
        # aggregate native presence cannot be read as parent activity.
        "subagent_taint": {},
        "subagent_taint_sampled_at": 0.0,
        "subagent_tool_starts": {},
        # Parent `prompt_id`s seen during the current turn (a steer adds one);
        # handed to the late-hook guard at settlement.
        "turn_prompt_ids": [],
        # Armed by the `interrupt` op after writing Escape to the PTY:
        #   {"at": float, "scan_pos": int}
        # A cancelled turn fires no Stop hook, so while armed the main loop
        # watches the transcript for Claude's user-interrupt marker and settles
        # the turn itself (see _read_interrupt_marker). Cleared on confirm,
        # on a normal Stop (Escape raced a turn that was ending anyway or did
        # nothing), or after INTERRUPT_CONFIRM_S without a marker.
        "interrupt_probe": None,
        # Parent-only transcript marker observer, fenced to its start offset;
        # unlike interrupt_probe it is armed by UPS, not by our Escape RPC.
        "passive_interrupt": None,
        # Reason text for terminal-state transitions, propagated to record on
        # shutdown as lifecycle.lastError.
        "pending_last_error": None,
        # True when shutdown was triggered by an explicit `ccx kill` (op:kill).
        # The shutdown path writes S.DEAD in that case, vs IDLE_KILLED for the
        # idle-timeout path.
        "killed_explicit": False,
        # Per-message usage cap armed for the current turn only: {"5h": pct, ...}
        # or None. Armed from an explicitly capped revival or semantic
        # delivery; cleared at each turn's Stop. When a capped metric reaches its threshold the worker
        # is hard-stopped (S.BUDGET_STOPPED). last_cap_poll throttles usage fetch.
        "active_cap": active_cap,
        "last_cap_poll": 0.0,
        # Byte offset into the raw transcript for the API-error fallback scan.
        # Resume starts at EOF so historical synthetic errors do not poison a
        # fresh host; new sessions start at 0 because the file is empty/missing.
        "api_error_scan_pos": transcript_size if is_resume else 0,
        # Per-host event sequence (monotonic, stamped on every broadcast) and the
        # amortization counter for sidecar rotation checks.
        "event_seq": 0,
        "events_since_rotate_check": 0,
        "context_changes": ContextChanges(),
        # tool_use_id → PreToolUse wall-clock, used to compute tool durations.
        "tool_starts": {},
        # Set to True once the record name has been updated from the ai-title
        # (or confirmed already custom). Guards against re-scanning the JSONL on
        # every PreToolUse/Stop after the name is settled.
        "ai_titled": False,
    }

    # `or` would treat 0 as "use default" — but 0 explicitly means "disable
    # idle-kill" per the CLI contract. Distinguish None (unset → default) from 0.
    _it = dargs.get("idle_timeout_s")
    idle_timeout_s = DEFAULT_IDLE_TIMEOUT_S if _it is None else int(_it)

    TERMINAL_STATES = (S.IDLE_KILLED, S.DEAD, S.CRASHED, S.BUDGET_STOPPED)

    def set_state(new_state, reason=None, settle_s=None):
        """Transition state. When entering IDLE, also gate sends behind a settle window.

        Terminal states (IDLE_KILLED/DEAD/CRASHED) are not written to the record
        here — the shutdown block writes them after sock/pidfile cleanup, so a
        concurrent `ccx send` reading the terminal state is guaranteed to find no
        live sock and respawn cleanly. We do stash `reason` for the shutdown to
        propagate as lastError, and broadcast the state event so subscribers see
        it in real time."""
        if new_state == state["current"]:
            return
        # Leaving WAITING for any reason other than an explicit approve/reject
        # (those clear pending_permission *before* calling set_state): the request
        # is being abandoned — Stop, crash, idle-kill.
        # Auto-deny it so the still-blocked relay poller unblocks Claude, and clear
        # the persisted indicator so peek/list don't show a phantom prompt after.
        if new_state != S.WAITING and state.get("pending_permission"):
            release_pending_gate(f"auto-released: session left waiting ({new_state})", auto=True)
        if new_state != S.WAITING and state.get("permission_queue"):
            release_queued_gates(f"auto-released: session left waiting ({new_state})", auto=True)
        if new_state != S.WAITING and state.get("observed_input"):
            state["observed_input"] = None
            persist_waiting_for(None)
        prev = state["current"]
        if new_state not in TERMINAL_STATES:
            accepted = host_live_transition(
                short, new_state=new_state, host_pid=host_pid,
            )
            if accepted is None:
                # The CLI's DEAD (or another external terminal) wins. Do not
                # broadcast a new idle/working status or accept another send.
                state["current"] = (read_record(short).get("lifecycle") or {}).get("state", S.DEAD)
                sigterm_pending[0] = True
                return
        now = time.time()
        state["current"] = new_state
        state["since"] = now
        state["last_activity"] = now
        # Any real transition breaks the idle-overdue condition; drop the bg
        # idle-grace latch so a later idle period re-emits idle_grace_bg once.
        state["bg_idle_grace"] = False
        if new_state == S.IDLE:
            state["earliest_send_at"] = now + (WARMUP_S if settle_s is None else settle_s)
        if new_state in TERMINAL_STATES:
            if reason is not None:
                state["pending_last_error"] = reason
        # Nonterminal state/since and stale-error cleanup were already fsynced
        # together by host_live_transition, under a fresh terminal-state CAS.
        broadcast({"type": "state", "from": prev, "to": new_state, "ts": now})

    def _tee_event(line: bytes):
        """Append one stamped event line to the sidecar, with amortized bounded
        rotation. Fail-open: any write/rotation error is swallowed so a full disk
        or vanished file never disrupts the session."""
        nonlocal event_file, event_lines
        if event_file is None:
            return
        try:
            event_file.write(line)
            event_file.flush()
            event_lines += 1
            state["events_since_rotate_check"] += 1
            if state["events_since_rotate_check"] >= CCX_EVENTS_ROTATE_CHECK_INTERVAL:
                state["events_since_rotate_check"] = 0
                # Cheap pre-gate: only read/rewrite the whole file when the live
                # counter says we are actually over the cap. Avoids touching the
                # file on every rotation check for a session under the limit.
                if event_lines > CCX_EVENTS_MAX and _rotate_events_file(events_ndjson_path(short)):
                    # Rotation kept the last CCX_EVENTS_KEEP lines.
                    event_lines = CCX_EVENTS_KEEP
                    # The append handle still points at the unlinked pre-rotation
                    # inode — reopen it onto the freshly rotated file. If the
                    # reopen fails, drop to None so we stop writing to a closed
                    # handle (which would raise ValueError, not OSError).
                    try:
                        event_file.close()
                    except OSError:
                        pass
                    try:
                        event_file = _open_event_journal(store, short)
                    except Exception:  # fail-open, including a busy store lease
                        event_file = None
        except (OSError, ValueError):
            pass

    def drop_client(conn) -> None:
        """Forget and close one client connection.

        Closing is what tells the reader: a subscriber that is merely removed
        from the broadcast set keeps an open socket that never yields another
        event, so its reader waits forever with no EOF."""
        hub.release(conn, "client-eof")
        state["subscribers"].discard(conn)
        state["handshaken_clients"].discard(conn)
        for fd, (known, _buf) in list(state["clients"].items()):
            if known is conn:
                state["clients"].pop(fd, None)
        try:
            conn.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            conn.close()
        except OSError:
            pass

    def broadcast(evt):
        state["event_seq"] += 1
        stamped = _stamp_event(
            evt, short=short, seq=state["event_seq"],
            epoch=event_epoch, ts=time.time(),
        )
        line = (json.dumps(stamped) + "\n").encode()
        dead = []
        for c in list(state["subscribers"]):
            try:
                c.sendall(line)
            except OSError:
                # Includes BlockingIOError: the subscriber fell a full socket
                # buffer behind (and may now hold a torn line). It is closed
                # so it sees EOF and can resume from the sidecar by cursor.
                dead.append(c)
        for c in dead:
            drop_client(c)
        _tee_event(line)
        return stamped

    # Cold reconstruction runs off the PTY/hook loop. Only this host later
    # advances the fold or emits events; hooks themselves contain no task facts.
    bg_ready: queue.SimpleQueue = queue.SimpleQueue()
    bg = {
        "reducer": None, "offset": 0, "items": [],
        "pending": set(), "pending_anonymous": False,
        "last": rec.get("backgroundTaskSummary"), "poll_at": 0.0,
        "settle_changes": [],
    }
    from ccx.ops.forking import inherited_task_filter

    def bootstrap_background() -> None:
        fold = BackgroundTaskReducer()
        offset = 0
        try:
            while True:
                rows, next_offset = read_background_append(rec["transcript"], offset)
                if next_offset == offset:
                    break
                for row in rows:
                    fold.feed(row)
                offset = next_offset
        except FileNotFoundError:
            pass  # brand-new transcript
        except Exception as error:
            bg_ready.put((None, offset, str(error)))
            return
        bg_ready.put((fold, offset, None))

    threading.Thread(target=bootstrap_background, name="ccx-bg-bootstrap",
                     daemon=True).start()

    def current_bg_items() -> list[dict]:
        fold = bg["reducer"]
        if fold is None:
            return []
        boundary = native_transcript_start(native_start, rec["transcript"])
        tasks = inherited_task_filter(rec, fold.tasks())
        result = []
        for task in tasks:
            item = claude_task_item(task)
            if item is None:
                continue
            if item["status"] == "running" and not background_task_current(task, boundary):
                continue
            result.append(item)
        return result

    def publish_bg(new_items: list[dict], changes: list[tuple[str, dict]] | None = None,
                   *, available: bool = True) -> dict:
        snapshot = background_summary(new_items, available=available)
        # Even an item outside the 100-item preview can change; only a
        # snapshot-only refresh with no material change stays silent.
        if not changes and not materially_changed(bg["last"], snapshot):
            return snapshot
        # Only the host may write this worker's record/journal. A publication
        # failure does not invent a successful prior observation.
        store.mutate_record(f"claude:{short}",
                            patches={"backgroundTaskSummary": snapshot})
        bg["last"] = snapshot
        transitions = changes or []
        transition, item = (transitions[0] if len(transitions) == 1
                            else ("event", None))
        broadcast({"type": "background_changed", "transition": transition,
                   "item": item, "summary": snapshot})
        return snapshot

    def poll_background(*, force: bool = False, publish: bool = True) -> dict | None:
        if bg["reducer"] is None:
            try:
                fold, offset, error = bg_ready.get_nowait()
            except queue.Empty:
                return None
            if error:
                bg["failed"] = True
                return publish_bg([], available=False) if publish else background_summary([], available=False)
            bg["reducer"], bg["offset"] = fold, offset
            bg["items"] = current_bg_items()
            # Establish a recorded baseline without replaying old native
            # launches as fresh `started` items. A non-empty first baseline is
            # announced once as a snapshot-only event (item null) so a
            # subscriber that read the earlier empty state learns about it.
            baseline = background_summary(bg["items"])
            if publish and (bg["last"] is not None or bg["items"]):
                publish_bg(bg["items"])
            elif bg["last"] is None:
                store.mutate_record(f"claude:{short}",
                                    patches={"backgroundTaskSummary": baseline})
                bg["last"] = baseline

        if bg["reducer"] is None:
            return None
        active = any(item["status"] == "running" for item in bg["items"])
        if not (force or active or bg["pending"] or bg["pending_anonymous"]):
            return background_summary(bg["items"])
        now = time.monotonic()
        if not force and now - bg["poll_at"] < 0.95:
            return background_summary(bg["items"])
        bg["poll_at"] = now
        try:
            rows, offset = read_background_append(rec["transcript"], bg["offset"])
        except FileNotFoundError:
            return background_summary(bg["items"])
        except (OSError, ValueError):
            if publish:
                return publish_bg([], available=False)
            return background_summary([], available=False)
        for row in rows:
            relevant = bg["reducer"].feed(row)
            if row.get("type") == "user":
                for part in (row.get("message") or {}).get("content") or []:
                    if isinstance(part, dict) and part.get("type") == "tool_result":
                        bg["pending"].discard(part.get("tool_use_id"))
            if not relevant:
                continue
            new_items = current_bg_items()
            changes = diff_items(bg["items"], new_items)
            bg["items"] = new_items
            if publish and changes:
                publish_bg(new_items, changes)
            elif not publish and changes:
                bg["settle_changes"].extend(changes)
        bg["offset"] = offset
        if force:
            bg["pending"].clear()
            bg["pending_anonymous"] = False
        snapshot = background_summary(bg["items"])
        if publish and materially_changed(bg["last"], snapshot):
            publish_bg(bg["items"])
        return snapshot

    def arm_background_hook(event_name: str, data: dict) -> None:
        if event_name == "SubagentStop" or (
            event_name in {"PostToolUse", "PostToolUseFailure"}
            and data.get("tool_name") in {
                "Bash", "Monitor", "Agent", "Task", "TaskStop", "SendMessage"
            }
        ):
            tool_id = _hook_tool_use_id(data)
            if tool_id:
                bg["pending"].add(tool_id)
            else:
                bg["pending_anonymous"] = True

    def _broadcast_context(snapshot: dict) -> None:
        broadcast({"type": "context_changed", "context": snapshot,
                   "turn_ref": snapshot.get("as_of_turn_ref")})

    def publish_context(snapshot: dict | None, *, force: bool = False) -> None:
        changed = state["context_changes"].observe(snapshot, force=force)
        if changed is not None:
            _broadcast_context(changed)

    def observe_statusline_context() -> None:
        # The relay owns only a bound statusline sidecar, not the event journal.
        from ccx.claude.context import read_claude_observation, public_context
        owner = store.resolve(f"claude:{short}")
        record = store.record(owner.worker)
        observation = read_claude_observation(store, owner, record)
        block = observation.get("context") if observation else None
        publish_context(public_context(block, engine="claude"))

    def compact_context_fence(reason: str | None) -> None:
        from ccx.claude.context import compaction_unknown_context, public_context
        block = (compaction_unknown_context(reason, as_of_ms=int(time.time() * 1000))
                 if reason is not None else None)
        try:
            store.mutate_record(f"claude:{short}", patches={"context": block})
        except Exception:
            return  # an advisory context event cannot break the native compact
        if reason is not None:
            publish_context(public_context(block, engine="claude"), force=True)
        else:
            try:
                observe_statusline_context()
            except Exception:
                pass

    # The select loop alone owns both the PTY and the attachment's stream.
    hub = HostAttachment(
        master_fd, lambda rows, cols: set_winsize(master_fd, rows, cols),
        broadcast, drop_client,
        lambda: state.__setitem__("last_activity", time.time()),
    )

    def ensure_turn_instance(
        source: str,
        *,
        delivery_id: str | None = None,
        report_start_offset: int | None = None,
    ) -> str:
        """Return/mint the start-unique identity for the real in-flight turn.

        A liveness-proven target enters ``settling`` before its event is finalized;
        that ref is already retired from attribution and must never be reused by a
        successor UPS/tool hook.
        """
        current = state.get("active_turn_ref")
        if (
            isinstance(current, str)
            and current
            and current != state.get("settling_turn_ref")
        ):
            return current
        turn_ref = new_turn_ref()
        state["active_turn_ref"] = turn_ref
        state["active_turn_delivery_id"] = delivery_id
        # The UPS relay may provide a pre-transcript offset. Otherwise fence
        # from current EOF: old markers in a resumed history are never ours.
        try:
            scan_pos = os.path.getsize(rec["transcript"])
        except OSError:
            scan_pos = native_start
        if isinstance(report_start_offset, int) and report_start_offset >= 0:
            scan_pos = max(native_start, report_start_offset)
        state["passive_interrupt"] = {"turn_ref": turn_ref, "scan_pos": scan_pos}
        state["context_turn_start"] = (turn_ref, scan_pos)
        # The same exact native observer also services an unsolicited human
        # Escape. It is armed at turn start, not from terminal bytes. A native
        # IDLE transition must have a token strictly newer than this
        # fence and remain stable for the passive Stop-upgrade grace.
        state["interrupt_observation"] = {
            "turn_ref": turn_ref, "passive": True, "settling": False,
            "interrupt_at_ms": int(time.time() * 1000),
        }
        state["successor_pending"] = None
        event = {
            "type": "turn_start",
            "turn_ref": turn_ref,
            "turn": state["turns"] + 1,
            "source": source,
        }
        if delivery_id:
            event["delivery_id"] = delivery_id
        if isinstance(report_start_offset, int) and report_start_offset >= 0:
            event["report_start_offset"] = report_start_offset
        broadcast(event)
        return turn_ref

    def semantic_public(delivery_id: str) -> dict | None:
        try:
            return public_semantic_receipt(read_semantic_receipt(short, delivery_id))
        except Exception as exc:
            try:
                broadcast({"type": "semantic_public_failed", "delivery_id": delivery_id,
                           "error": f"{type(exc).__name__}: {exc}"[:300]})
            except Exception:
                pass
            return None

    def semantic_reply(delivery_id: str) -> dict:
        return {
            "ok": True,
            "state": state["current"],
            "delivery": semantic_public(delivery_id),
        }

    def semantic_hot(delivery_id: str) -> dict | None:
        try:
            return get_steer_delivery(short, delivery_id)
        except Exception:
            return None

    def semantic_pending_blocker() -> str | None:
        slot = state.get("semantic_delivery")
        if slot is not None:
            delivery_id = slot.get("delivery_id")
            if not delivery_id:
                # A failed recovery leaves a placeholder without a token; it
                # still owns the input surface until the worker is reconciled.
                return ("An earlier message is unresolved; inspect the worker "
                        "and reconcile its delivery before sending again.")
            prior = read_semantic_receipt(short, delivery_id)
            if prior is not None and prior.get("state") in ("accepted", "rejected", "delivered", "abandoned"):
                state["semantic_delivery"] = None
            else:
                return (f"An earlier message (delivery {delivery_id}) is still unresolved; "
                        "wait for it, or if it is stuck, `ccx kill` the worker to clear it "
                        "(the conversation is kept).")
        if state.get("compacting") is not None or state.get("compact_clear") is not None:
            return "Compaction is in progress; try again after it finishes."
        if state.get("pending_permission"):
            return "worker is waiting on input"
        try:
            if unproven_steer_deliveries(short):
                return "a prior durable delivery may still surface"
            if pending_steer_deliveries(short):
                return "a prior durable steer is unresolved"
        except Exception as exc:
            return f"delivery journal unavailable: {exc}"
        return None

    def semantic_slot_from_receipt(receipt: dict) -> dict:
        return _semantic_slot_from_receipt(receipt, semantic_hot(receipt["deliveryId"]))

    permission_preview = _permission_preview

    def persist_waiting_for(req: dict | None):
        from ccx.store.root import RecordConflict
        preview = permission_preview(req) if req else None
        for _ in range(6):
            latest = read_record(short)
            lifecycle = latest.get("lifecycle") or {}
            if lifecycle.get("state") in TERMINAL_STATES:
                return  # CLI kill owns terminal; no phantom waiting gate
            try:
                store.mutate_record(
                    short,
                    patches={"waitingFor": preview} if req else {},
                    removals=() if req else ("waitingFor",),
                    expect={"lifecycle.state": lifecycle.get("state")},
                )
                return
            except RecordConflict:
                continue
        raise RecordConflict("host waiting preview changed during bounded rebase")

    def current_waiting_preview() -> dict | None:
        """The pending input a status reader should see: the brokered gate, or
        else an observed-only elicitation dialog."""
        return permission_preview(
            state.get("pending_permission") or state.get("observed_input"))

    def release_gate_request(req: dict, reason: str, event_type: str | None = None,
                             **extra) -> None:
        """Answer one brokered request with its kind's no-decision default.

        A permission/AskUserQuestion is denied; an elicitation is cancelled
        (dismissed, so the MCP server records no decision on the user's
        behalf). The still-blocked relay poller picks the decision up."""
        if req.get("kind") == KIND_ELICITATION:
            state["permission_decisions"][req["id"]] = {"action": "cancel", "reason": reason}
            event = {"type": event_type or "elicitation_resolved", "request_id": req["id"],
                     "server": req.get("server"), "action": "cancel"}
        else:
            state["permission_decisions"][req["id"]] = {"decision": "deny", "reason": reason}
            event = {"type": event_type or "permission_rejected", "request_id": req["id"],
                     "tool": req.get("tool_name")}
        event.update(extra)
        broadcast(event)

    def release_pending_gate(reason: str, event_type: str | None = None, **extra) -> dict | None:
        """Resolve the published gate without an explicit answer, kind-aware.

        Queued requests are untouched; the caller owns the follow-up: promote
        the next one (``promote_next_gate``) or transition state."""
        req = state.get("pending_permission")
        if not req:
            return None
        release_gate_request(req, reason, event_type, **extra)
        state["pending_permission"] = None
        persist_waiting_for(None)
        return req

    def release_queued_gates(reason: str, **extra) -> None:
        """Release every queued (not yet published) request: the turn they
        belong to is over, so none of them can be answered any more."""
        queued, state["permission_queue"] = state["permission_queue"], []
        for req in queued:
            release_gate_request(req, reason, **extra)

    def publish_gate(req: dict) -> None:
        """Make ``req`` the gate a status reader and the answer verbs see."""
        state["observed_input"] = None
        state["pending_permission"] = req
        persist_waiting_for(req)
        set_state(S.WAITING)
        kind = "elicitation_requested" if req.get("kind") == KIND_ELICITATION \
            else "permission_requested"
        broadcast({"type": kind, "request": permission_preview(req)})

    def promote_next_gate() -> bool:
        """After the published gate resolved, publish the oldest queued request.

        Returns False when the queue is empty: the caller then clears the
        persisted preview and leaves WAITING as before."""
        if state.get("pending_permission") is not None or not state["permission_queue"]:
            return False
        publish_gate(state["permission_queue"].pop(0))
        return True

    def resolve_published_gate(next_state: str = S.BUSY) -> None:
        """The published gate got its decision: show the next one, or move on."""
        state["pending_permission"] = None
        if not promote_next_gate():
            persist_waiting_for(None)
            set_state(next_state)

    def enqueue_or_publish_gate(req: dict) -> None:
        """One gate is published at a time; later requests wait their turn."""
        if state.get("pending_permission") is not None:
            state["permission_queue"].append(req)
            broadcast({"type": "input_request_queued", "request_id": req["id"],
                       "kind": req.get("kind") or "permission",
                       "position": len(state["permission_queue"])})
            return
        publish_gate(req)

    def take_queued_gate(request_id) -> dict | None:
        for index, queued in enumerate(state["permission_queue"]):
            if queued.get("id") == request_id:
                return state["permission_queue"].pop(index)
        return None

    # Receipts are permanent per delivery id; drop long-settled ones before the
    # scan below so it stays bounded. An unreadable hot journal means we cannot
    # tell which ids it still references, so nothing is pruned.
    try:
        hot_ids = {entry.get("delivery_id")
                   for entry in read_steer_journal(short).get("deliveries") or []}
        prune_semantic_receipts(short, keep_ids=hot_ids)
    except Exception as exc:
        broadcast({"type": "semantic_prune_error", "reason": str(exc)})

    try:
        owned_semantic = [
            value for value in iter_semantic_receipts(short)
            if value.get("state") == "owned"
        ]
        if len(owned_semantic) > 1:
            state["semantic_delivery"] = {
                "recovery_error": "multiple owned semantic deliveries",
            }
            broadcast({
                "type": "semantic_recovery_error",
                "reason": "multiple owned semantic deliveries",
                "delivery_ids": [v.get("deliveryId") for v in owned_semantic],
            })
        elif owned_semantic:
            state["semantic_delivery"] = semantic_slot_from_receipt(owned_semantic[0])
            broadcast({
                "type": "semantic_recovered",
                "delivery_id": owned_semantic[0].get("deliveryId"),
                "route": owned_semantic[0].get("route"),
                "maybe_written": bool(owned_semantic[0].get("maybeWritten")),
            })
    except Exception as exc:
        state["semantic_delivery"] = {"recovery_error": str(exc)}
        broadcast({"type": "semantic_recovery_error", "reason": str(exc)})

    def compact_event_pairs() -> tuple[set[str], set[str]]:
        # The bounded host sidecar is the only event source for a managed
        # Claude compact. Scan once on startup/shutdown, never on a select tick.
        started: set[str] = set()
        ended: set[str] = set()
        try:
            with events_ndjson_path(short).open("rb") as stream:
                for line in stream:
                    try:
                        value = json.loads(line)
                    except (ValueError, UnicodeError):
                        continue
                    if not isinstance(value, dict):
                        continue
                    operation_id = value.get("operation_id")
                    if not isinstance(operation_id, str) or not operation_id:
                        continue
                    if value.get("type") == "compact_started":
                        started.add(operation_id)
                    elif value.get("type") in (
                        "compact_complete", "compact_failed", "compact_unknown"
                    ):
                        ended.add(operation_id)
        except OSError:
            pass
        return started, ended

    def broadcast_missing_compact_end(operation_id: str, operation: dict) -> None:
        started, ended = compact_event_pairs()
        if operation_id not in started or operation_id in ended:
            return
        status = operation.get("status")
        event_type = (
            "compact_complete" if status in ("succeeded", "noop")
            else "compact_failed" if status == "failed" else "compact_unknown"
        )
        broadcast({
            "type": event_type, "operation_id": operation_id,
            "reason": operation.get("reason"),
            "evidence_uuid": operation.get("evidence_uuid"),
            "source": "compact-terminal-reconciled",
        })

    # A previous host may have died after accepting or submitting /compact.
    # Reconcile from the transcript before accepting any new operation.  A
    # proven boundary becomes success; absence of proof becomes durable unknown,
    # never a fabricated failure.  Do this only after broadcast is available so
    # operators get a structured recovery trace.
    try:
        for recovered in reconcile_stale_compact_operations(
            short,
            transcript=rec["transcript"],
            current_host_epoch=event_epoch,
            lock_timeout=COMPACT_LOCK_TIMEOUT_S,
        ):
            broadcast({
                "type": "compact_recovered",
                "operation_id": recovered.get("operation_id"),
                "status": recovered.get("status"),
                "reason": recovered.get("reason"),
                "evidence_uuid": recovered.get("evidence_uuid"),
            })
    except Exception as exc:
        broadcast({"type": "compact_recovery_error", "reason": str(exc)})
    # The preceding host can crash after its compact status journal commits but
    # before its terminal event is broadcast; such settled statuses are not
    # returned by reconcile_stale_compact_operations.
    from ccx.ops.compact_ops import get_operation as _get_compact_operation
    pending_starts, published_ends = compact_event_pairs()
    for stale_operation_id in sorted(pending_starts - published_ends):
        operation = _get_compact_operation(short, stale_operation_id)
        if operation and operation.get("status") in (
            "succeeded", "noop", "failed", "unknown"
        ):
            broadcast_missing_compact_end(stale_operation_id, operation)

    def native_snapshot():
        value = read_managed_claude_native_state(child_pid, session_id)
        clear_subagent_taint(state["subagent_taint"], value)
        state["native_sample"] = value
        return value

    def native_inactive_after_escape(value=None) -> bool:
        """Start one exact-turn native-inactive candidate on this observer."""
        obs = state.get("interrupt_observation")
        value = value or native_snapshot()
        if not interrupt_native_idle_after_escape(state, obs, value):
            return False
        obs["settling"] = True
        obs["inactive_status"] = value.status
        obs["inactive_status_updated_at"] = value.status_updated_at
        obs["settling_at"] = time.monotonic()
        state["settling_turn_ref"] = obs["turn_ref"]
        return True

    def native_candidate_still_valid(obs: dict, value) -> bool:
        """Require continuous verified idle on the *same* original turn."""
        return bool(
            state.get("active_turn_ref") == obs.get("turn_ref")
            and value is not None
            and (value.status == "idle" if obs.get("passive")
                 else value.status in {"idle", "shell"})
            and (not obs.get("passive") or not getattr(value, "waiting_for", None))
            and type(value.status_updated_at) is int
            and value.status_updated_at >= obs.get("inactive_status_updated_at", 0)
            and value.status_updated_at > obs.get("interrupt_at_ms", 0)
        )

    def reset_inactive_candidate(obs: dict) -> None:
        """Contradictory parent work/native busy/unreadable voids grace."""
        obs["settling"] = False
        obs.pop("inactive_status", None)
        obs.pop("inactive_status_updated_at", None)
        obs.pop("settling_at", None)
        if state.get("settling_turn_ref") == obs.get("turn_ref"):
            state["settling_turn_ref"] = None

    def clear_matching_interrupt(turn_ref: str | None) -> None:
        probe = state.get("interrupt_probe")
        if probe is not None and probe.get("turn_ref") == turn_ref:
            state["interrupt_probe"] = None
        obs = state.get("interrupt_observation")
        if obs is not None and obs.get("turn_ref") == turn_ref:
            state["interrupt_observation"] = None
        if state.get("settling_turn_ref") == turn_ref:
            state["settling_turn_ref"] = None

    def finalize_managed_turn(
        turn_ref: str,
        *,
        outcome: str,
        interrupted: bool = False,
        native_status: str | None = None,
        successor_running: bool = False,
        successor_native_token: int | None = None,
        inactive_native_token: int | None = None,
    ) -> dict | None:
        """One bookkeeping tail for Stop, marker and liveness settlement."""
        if not turn_ref:
            return None
        if turn_ref not in {state.get("active_turn_ref"), state.get("settling_turn_ref")}:
            return None
        settled_ms = int(time.time() * 1000)
        title = None
        if outcome in {"completed", "interrupted"} and not state["ai_titled"]:
            try:
                title = read_ai_title(rec["transcript"])
            except Exception:
                pass
        # Exact start-unique turnRef, monotonic counter and settlement times
        # are one durable semantic CAS, not a stale whole-record write. Keep
        # hot state/semantic receipts unchanged until the record commits.
        from ccx.claude.context import read_claude_settlement_context
        start = state.get("context_turn_start")
        try:
            context = read_claude_settlement_context(
                rec["transcript"], start_offset=start[1] if start and start[0] == turn_ref else None,
                turn_ref=turn_ref, settled_ms=settled_ms,
            )
        except Exception:
            context = None  # Telemetry is never allowed to fail settlement.
        # One force read of the appended native rows; no whole-history parse.
        bg_summary = poll_background(force=True, publish=False)
        if bg_summary is None:
            bg_summary = background_summary([], available=False)
        committed = commit_host_turn_settlement(
            short, turn_ref=turn_ref, prior_turns=state["turns"],
            outcome=outcome, settled_at_ms=settled_ms, proposed_title=title,
            context=context,
            background_task_summary=bg_summary,
        )
        state["turns"] = committed["lastSettledTurn"]["turn"]
        state["active_cap"] = None
        if title is not None:
            state["ai_titled"] = True  # also if explicit user name won CAS
        reconcile_semantic_delivery()
        semantic_target_completed(turn_ref)
        clear_matching_interrupt(turn_ref)
        if (state.get("passive_interrupt") or {}).get("turn_ref") == turn_ref:
            state["passive_interrupt"] = None
        state["active_turn_ref"] = None
        state["context_turn_start"] = None
        settled_prompt_ids = state["turn_prompt_ids"]
        state["turn_prompt_ids"] = []
        state["active_turn_delivery_id"] = None
        if successor_running:
            # A successor exists natively but may not have emitted UPS yet. Do not
            # lie IDLE; retain a bounded BUSY/no-ref hold until it is attributed or
            # native state returns inactive again.
            state["successor_pending"] = {
                "since": time.time(),
                "native_busy_status_updated_at": successor_native_token,
                # A live subagent makes the busy edge unattributable.
                "subagent_ambiguous": bool(state["subagent_taint"]),
            }
            if state["current"] != S.BUSY:
                set_state(S.BUSY)
        else:
            state["successor_pending"] = None
            set_state(S.IDLE)
        event: dict = {"turn": state["turns"], "turn_ref": turn_ref}
        if outcome == "unknown":
            event.update({
                "type": "turn_settled",
                "outcome": "unknown",
                "evidence": "native-session-inactive",
                "native_status": native_status or "idle",
            })
        if outcome in {"unknown", "interrupted"}:
            # Native cancellation can restore its draft without a Stop hook;
            # clear the editor before the next semantic paste. The exact
            # original turn and provider-idle token fence the late-hook guard.
            state["cancel_clear"] = {
                "unstuck_at": None,
                "cleared_at": None,
                "turn_ref": turn_ref,
            }
            obs_token = inactive_native_token
            if obs_token is None:
                sample = state.get("native_sample")
                if sample is not None and sample.status in {"idle", "shell"}:
                    obs_token = sample.status_updated_at
            old_guard = state.get("late_hook_guard") or {}
            # Retain exact old identities across a successor's UPS/Stop. The
            # existing heuristic (missing ID) stays short; known prompt IDs
            # survive for a bounded window even after a new parent starts.
            old_ids = (old_guard.get("prompt_ids") or []
                       if time.time() < old_guard.get("prompt_until", 0) else [])
            prompt_ids = list(dict.fromkeys([*old_ids, *settled_prompt_ids]))[-32:]
            state["late_hook_guard"] = {
                "turn_ref": turn_ref,
                "settled_at": time.time(),
                "native_inactive_status_updated_at": obs_token,
                "post_settle_busy_seen": bool(
                    successor_native_token is not None
                    and obs_token is not None
                    and successor_native_token > obs_token
                ),
                "expires_at": time.time() + LATE_HOOK_BACKSTOP_S,
                "prompt_until": time.time() + LATE_HOOK_PROMPT_ID_S if prompt_ids else 0,
                "prompt_ids": prompt_ids,
            }
        if outcome != "unknown":
            event["type"] = "turn_complete"
            if outcome == "interrupted" or interrupted:
                event["interrupted"] = True
        stamped = broadcast(event)
        from ccx.claude.context import public_context
        publish_context(public_context(context, engine="claude"), force=True)
        changes = bg["settle_changes"]
        bg["settle_changes"] = []
        if changes or materially_changed(bg["last"], bg_summary):
            publish_bg(bg["items"], changes, available=bg_summary["available"])
        # A pending parent wake must survive a notifyd crash. Checking the
        # registration inventory and restarting notifyd can block on disk/PIDs,
        # so only schedule the work here, never run it in the PTY/select loop.
        from ccx.notify.daemon import revive_notify_daemon_after_turn
        revive_notify_daemon_after_turn("claude", short)
        # Lifecycle settlement is the event contract. The eager rendered
        # transcript remains a legacy/export cache, but its whole-history work
        # must not delay state/turn observers — or stall this loop for seconds
        # on a large transcript — so it runs on the refresher thread.
        request_refresh()
        return stamped

    def settle_original_semantic_stop(
        original_delivery_id: str | None, original_turn_ref: str,
        stamped_completion: dict | None, native_hook: dict,
    ) -> None:
        """Publish actual Stop-native proof for an accepted original delivery.

        A synthetic model-wait deadline, a queued PTY paste, or a generic
        turn_settled/idle transition cannot prove completion. Only an original
        Stop hook carrying the exact bound provider session and its actual
        last assistant message, plus the host's own stamped completed turn,
        can close the semantic and native-effect receipts. No user/provider
        turn wait happens under the activation lock.
        """
        if not original_delivery_id or not isinstance(stamped_completion, dict):
            return
        session = (rec.get("native") or {}).get("sessionId")
        text = native_hook.get("last_assistant_message")
        if (not isinstance(session, str)
                or native_hook.get("session_id") != session
                or not isinstance(text, str)
                or len(text.encode("utf-8")) > 262144
                or stamped_completion.get("type") != "turn_complete"
                or stamped_completion.get("turn_ref") != original_turn_ref):
            return
        from ccx.store.store import OperationIntent, _digest
        original_semantic = read_semantic_receipt(short, original_delivery_id)
        global_effect = store.read_receipt(
            f"claude:{short}", "claude-delivery", original_delivery_id,
        )
        if (not isinstance(original_semantic, dict)
                or original_semantic.get("state") != "accepted"
                or original_semantic.get("turnRef") != original_turn_ref
                or not isinstance(global_effect, dict)
                or (global_effect.get("state") != "accepted"
                    if original_semantic.get("requestedMode") == "queued"
                    else global_effect.get("state") not in ("submitted-unknown", "accepted"))
                or global_effect.get("action") != "send"
                or global_effect.get("engine") != "claude"
                or global_effect.get("workerId") != short
                or global_effect.get("generation") != store.generation):
            return
        # The source is the original provider-native session in the pinned
        # first worker record, not an eight-character managed CCX display ID.
        # This digest names exact native evidence; it does not turn an
        # unverified external hook/process into production attestation.
        proof_digest = _digest({
            "event": "Stop", "workerId": short, "engine": "claude",
            "generation": store.generation, "sessionId": session,
            "deliveryId": original_delivery_id,
            "turnRef": original_turn_ref,
            "hostEpoch": stamped_completion.get("epoch"),
            "hostSeq": stamped_completion.get("seq"),
            "text": text,
        })
        proof = {
            "workerId": short, "engine": "claude",
            "nativeSource": f"claude-session:{session}",
            "sessionId": session,
            "deliveryId": original_delivery_id,
            "turnRef": original_turn_ref,
            "status": "completed",
            "text": text,
            "nativeEvidenceSha256": proof_digest,
        }
        mark_semantic_settled(
            short, original_delivery_id, turn_ref=original_turn_ref,
            original_positive_completion=proof,
        )
        if original_semantic.get("requestedMode") == "queued":
            # Stop settles each queued member separately. The global native
            # intent already sealed placement; never overwrite that proof.
            broadcast({
                "type": "semantic_settled", "delivery_id": original_delivery_id,
                "turn_ref": original_turn_ref, "source": "native-stop",
            })
            return
        original_intent = OperationIntent(
            short, "claude", "claude-delivery", original_delivery_id,
            global_effect["intentSha256"], "submitted-unknown",
            store.generation,
        )
        store.settle_native_intent(
            original_intent, state="accepted",
            native_evidence_sha256=proof_digest,
        )
        broadcast({
            "type": "semantic_settled",
            "delivery_id": original_delivery_id,
            "turn_ref": original_turn_ref,
            "source": "native-stop",
        })


    def note_interrupt_unconfirmed(reason: str) -> None:
        obs = state.get("interrupt_observation")
        if obs is None or obs.get("settling"):
            return
        turn_ref = obs.get("turn_ref")
        probe = state.get("interrupt_probe")
        if (probe is None or probe.get("turn_ref") != turn_ref
                or probe.get("unconfirmed")):
            return
        probe["unconfirmed"] = True
        # A continued native BUSY or a return to WAITING does not prove
        # cancellation failed. Report uncertainty, but keep exact observation
        # until original native IDLE or a real Stop/marker settles the turn.
        broadcast({
            "type": "interrupt_unconfirmed",
            "turn_ref": turn_ref,
            "reason": reason,
        })

    def note_interrupt_activity(event_name: str) -> None:
        obs = state.get("interrupt_observation")
        if (
            obs is None
            or obs.get("passive")
            or obs.get("settling")
            or state.get("active_turn_ref") != obs.get("turn_ref")
            or event_name not in {"PreToolUse", "PostToolUse", "SubagentStop"}
        ):
            return
        if time.time() - float(obs.get("interrupt_at") or 0.0) < INTERRUPT_CONTINUED_ACTIVITY_GRACE_S:
            return
        if state["subagent_taint"]:
            return  # native busy may be a subagent's, not continued parent work
        value = native_snapshot()
        if value is not None and value.status == "busy":
            note_interrupt_unconfirmed(f"continued {event_name} activity after interrupt")

    def classify_guarded_hook(event_name: str, data: dict) -> bool:
        """Return True when a Stop/PreToolUse is a late hook for settled target."""
        guard = state.get("late_hook_guard")
        if guard is None or event_name not in {"Stop", "PreToolUse"}:
            return False
        now = time.time()
        pid = hook_prompt_id(data)
        # Exact known old prompt IDs beat any native BUSY sample and remain
        # authoritative while B is active; a STOP(A) may arrive after UPS(B).
        if (pid and pid in (guard.get("prompt_ids") or [])
                and now < float(guard.get("prompt_until") or 0)):
            return True
        if now >= float(guard.get("expires_at") or 0.0):
            if now >= float(guard.get("prompt_until") or 0.0):
                state["late_hook_guard"] = None
            return False
        if pid and guard.get("prompt_ids"):
            return False  # known distinct successor ID, never the old Stop
        if state["subagent_taint"]:
            # Native busy edges may be a subagent's: never read them here.
            return guarded_hook_is_late_while_tainted(
                event_name, state.get("active_turn_ref"),
                hook_prompt_id(data), guard.get("prompt_ids"))
        value = native_snapshot()
        inactive_token = int(guard.get("native_inactive_status_updated_at") or 0)
        if (
            value is not None
            and value.status == "busy"
            and value.status_updated_at is not None
            and value.status_updated_at > inactive_token
        ):
            guard["post_settle_busy_seen"] = True
        active = state.get("active_turn_ref")
        if active:
            # A successor UPS already owns a ref. A late old Stop while that
            # successor is still natively busy is suppressed; its real Stop will
            # arrive after native foreground inactivity.
            return event_name == "Stop" and value is not None and value.status == "busy"
        if guard.get("post_settle_busy_seen"):
            return False
        # Unchanged inactive token, or shell->idle without any busy episode, is
        # still the old target's delayed hook.
        return True

    def reclaim_missed_turn() -> None:
        """Re-assert BUSY when a tool proves a turn we never saw start.

        Lifecycle normally begins at ``UserPromptSubmit``, but that hook is a
        no-op whenever the host already holds BUSY for something else — so a
        turn Claude starts on its own (a queued prompt or background-task
        notification flushed the instant a compact boundary lands) can leave no
        lifecycle trace at all, and the worker reports IDLE for its entire
        duration.  ``PreToolUse`` is unambiguous: a tool is about to run, so a
        turn is in flight, whatever we believed.

        Restricted to ``PreToolUse`` deliberately.  It is the first hook of any
        turn that uses tools, and — unlike ``PostToolUse`` — it can never be
        the tail of a turn that has already stopped.
        """
        successor_pending = (
            state.get("successor_pending") is not None
            and state["current"] == S.BUSY
            and not state.get("active_turn_ref")
        )
        if (state["current"] != S.IDLE and not successor_pending) or state.get("compacting") is not None:
            return
        state["post_compact_hold"] = None
        semantic_foreign_turn_start("pre-tool-use-missed")
        turn_ref = ensure_turn_instance("pre-tool-use-missed")
        set_state(S.BUSY)
        broadcast({"type": "turn_start_missed", "turn": state["turns"] + 1,
                   "turn_ref": turn_ref})

    def release_observed_input() -> None:
        """A parent tool boundary means Claude is running again, so an
        observed-only prompt or dialog has been answered, denied or timed out."""
        if (state["current"] == S.WAITING and state.get("observed_input")
                and not state.get("pending_permission") and not state["permission_queue"]):
            set_state(S.BUSY)

    def pty_read_for_input_ready(now: float) -> float:
        """`last_pty_read` for the Ink-ready gates, judged at the caller's `now`."""
        return input_ready_pty_read(
            state.get("last_pty_read", 0.0), bool(state["subagent_taint"]),
            now, PTY_QUIESCENCE_S,
        )

    def on_subagent_hook_event(event_name, data):
        """A subagent's own hook: attributed events only, never parent state."""
        arm_background_hook(event_name, data)
        if event_name == "PreToolUse":
            tuid = _hook_tool_use_id(data)
            if tuid is not None:
                if len(state["subagent_tool_starts"]) > 256:
                    state["subagent_tool_starts"].clear()
                state["subagent_tool_starts"][tuid] = time.time()
            broadcast(_subagent_tool_started_event(data))
        elif event_name in ("PostToolUse", "PostToolUseFailure"):
            tuid = _hook_tool_use_id(data)
            started = (state["subagent_tool_starts"].pop(tuid, None)
                       if tuid is not None else None)
            duration_ms = int((time.time() - started) * 1000) if started else None
            evt = _subagent_tool_finished_event(data, duration_ms)
            if event_name == "PostToolUseFailure":
                evt["ok"] = False
            broadcast(evt)
        elif event_name == "SubagentStop":
            broadcast(_subagent_finished_event(data, state["turns"] + 1))
        # SubagentStart and any other subagent-tagged hook: tracking only.

    def on_hook_event(event_name, data):
        data = data or {}
        note_subagent_taint(
            state["subagent_taint"], event_name, data,
            hook_fired_ms(data, int(time.time() * 1000)))
        # Subagent taint remains meaningful for busy/successor attribution,
        # but never retires an exact cancel observer: aggregate IDLE is
        # sufficient even while the taint fence itself has not cleared.
        if _hook_subagent_id(data) is not None:
            on_subagent_hook_event(event_name, data)
            return
        state["last_activity"] = time.time()
        if classify_guarded_hook(event_name, data):
            broadcast({
                "type": "late_hook_ignored",
                "event": event_name,
                "turn_ref": (state.get("late_hook_guard") or {}).get("turn_ref"),
            })
            return
        # CCX's own /compact command is not a model prompt, even if native
        # Claude emits UserPromptSubmit while that compact owns the surface.
        if (event_name == "UserPromptSubmit" and state.get("compacting") is not None
                and is_compact_command_text(data.get("prompt"))):
            return
        arm_background_hook(event_name, data)
        note_interrupt_activity(event_name)
        obs = state.get("interrupt_observation")
        if (obs is not None and obs.get("passive") and obs.get("settling")
                and event_name in {"PreToolUse", "PostToolUse", "PostToolUseFailure"}
                and state.get("active_turn_ref") == obs.get("turn_ref")):
            reset_inactive_candidate(obs)
        if event_name == "UserPromptSubmit" and state.get("settling_turn_ref"):
            old_ref = state.get("settling_turn_ref")
            obs = state.get("interrupt_observation") or {}
            # The old target's native idle edge was already observed. Retire
            # its exact ref before adding B's prompt ID or new turn, never
            # classify this as a later no-evidence unknown outcome.
            successor, successor_token = successor_attribution(
                obs.get("inactive_status_updated_at"), native_snapshot())
            finalize_managed_turn(
                old_ref,
                outcome="interrupted",
                interrupted=True,
                native_status=obs.get("inactive_status"),
                successor_running=True,
                successor_native_token=successor_token if successor else None,
                inactive_native_token=obs.get("inactive_status_updated_at"),
            )
        # After any successor-forced settlement above, so a successor's id
        # never joins the settled turn's guard set.
        pid = hook_prompt_id(data)
        if pid is not None and pid not in state["turn_prompt_ids"]:
            state["turn_prompt_ids"].append(pid)
        if event_name == "SessionStart":
            request_refresh()
            if state.get("compacting") is None:
                set_state(S.IDLE)
        elif event_name == "UserPromptSubmit":
            prompt = (data or {}).get("prompt")
            # The relay captured this fence inside the hook process before
            # returning control to Claude. Never replace a missing relay fence
            # with a later host-side stat: a busy host loop can observe the file
            # only after the user row or even model activity has already landed.
            raw_report_start = (data or {}).get("_ccx_report_start_offset")
            report_start_offset = (
                raw_report_start
                if isinstance(raw_report_start, int)
                and not isinstance(raw_report_start, bool)
                and raw_report_start >= 0
                else None
            )
            if compact_displaced_by_prompt(state, prompt):
                # Ink flushed a prompt (typically one queued by an earlier
                # steer) on top of our un-submitted `/compact`. The command is
                # dead the moment this turn owns the surface, so fail it now
                # instead of letting the caller block until the deadline.
                abort_compact(
                    "input surface taken by a concurrent user turn "
                    "before /compact was submitted",
                    evidence="user-prompt-submit",
                )
            if (state.get("compacting") is not None
                    and not is_compact_command_text(prompt)):
                # Claude flushes whatever it queued during compaction the
                # instant the boundary lands, so this is a real turn starting.
                # `set_state(S.BUSY)` below cannot record it — the compact
                # already holds BUSY, making it a no-op — and the transcript
                # write lags the hook (live-measured: still not on disk 136ms
                # later, when the settle read it). So the hook is the only
                # exact, race-free evidence this turn exists, and the compact's
                # settle must not release IDLE over it.
                state["compact_turn_started"] = True
            # Attribute semantic delivery before minting a generic turn start.
            # A mismatched prompt is foreign and may preempt an unwritten slot.
            semantic_attributed = semantic_user_prompt(
                prompt, report_start_offset=report_start_offset
            )
            if not semantic_attributed:
                ensure_turn_instance(
                    "user-prompt-submit",
                    report_start_offset=report_start_offset,
                )
            set_state(S.BUSY)
        elif event_name == "Stop":
            # The turn that owned the per-message cap is over — disarm it.
            state["active_cap"] = None
            # A normal Stop supersedes any armed interrupt probe: either the
            # Escape raced a turn that was ending anyway, or it did nothing.
            state["interrupt_probe"] = None
            if state.get("compacting") is not None:
                # Native /compact may emit hook traffic while summarizing.  It
                # is not a user/model turn and its Stop does not settle the
                # operation; only the transcript boundary/no-op does.
                broadcast({
                    "type": "compact_hook_stop",
                    "operation_id": state["compacting"].get("operation_id"),
                })
                return
            # Stop is exact semantic terminal evidence. If a liveness observer
            # already saw the old target inactive, this upgrades the candidate
            # to semantic completion before the grace expires.
            turn_ref = state.get("active_turn_ref") or state.get("settling_turn_ref")
            if not turn_ref:
                turn_ref = ensure_turn_instance("stop-fallback")
            # A fresh native busy token after the old target's inactive edge
            # means queued successor work already started. Complete the old
            # target but do not project a false IDLE over that successor.
            successor = False
            successor_token = None
            obs = state.get("interrupt_observation")
            if obs is not None and obs.get("inactive_status_updated_at") is not None:
                successor, successor_token = successor_attribution(
                    obs.get("inactive_status_updated_at"), native_snapshot())
            slot = current_semantic_slot()
            if queued_steer_needs_successor_hold(slot, turn_ref):
                # A queued steer can start its next turn without another hook.
                successor = True
            # Capture the original delivery before finalization clears
            # active_turn_delivery_id. The Stop hook must positively bind
            # its native session to this same completed turn.
            original_delivery_id = state.get("active_turn_delivery_id")
            # An Escape request is not native cancellation evidence. A
            # genuine parent Stop wins as completed even if ccx stop wrote
            # Escape while the reply was already finishing. Preserve the
            # source-proven semantic send completion and notification.
            completed_event = finalize_managed_turn(
                turn_ref, outcome="completed", successor_running=successor,
                successor_native_token=successor_token,
            )
            try:
                settle_original_semantic_stop(
                    original_delivery_id, turn_ref, completed_event, data,
                )
            except Exception as exc:
                # Never invent a success on incomplete source proof. Keep
                # the original accepted semantic receipt awaiting positive
                # reconciliation; do not replay the native prompt.
                broadcast({
                    "type": "semantic_settlement_error",
                    "delivery_id": original_delivery_id,
                    "turn_ref": turn_ref,
                    "reason": str(exc),
                })
        elif event_name == "Notification":
            # Real schema: payload.notification_type is the discriminator.
            # Values observed from Claude:
            #   permission_prompt, worker_permission_prompt → tool wants approval
            #   idle_prompt                                  → 60s idle nag (not blocking)
            #   elicitation_dialog, elicitation_url_dialog   → MCP input dialog (blocking)
            #   elicitation_response, elicitation_complete   → that dialog settled
            #   auth_success, computer_use_*, push_notification → not blocking
            # Elicitations are normally brokered by the Elicitation hook and never
            # reach a dialog; the dialog values are the unbrokered fallback.
            # If we also wire PermissionRequest auto-allow these *should* never fire,
            # but the matcher is still belt-and-suspenders.
            # worker_permission_prompt belongs to a subagent/worker, not the
            # parent's input surface; see notification_sets_waiting.
            if notification_sets_waiting(data):
                if not state.get("pending_permission") and not state.get("observed_input"):
                    # No PermissionRequest hook was brokered for it (Claude's
                    # own safety checks prompt in the terminal only), so
                    # record what is known; the next parent tool boundary
                    # proves the prompt has gone.
                    observed = {"kind": KIND_PERMISSION, "id": None,
                                "message": data.get("message"),
                                "requested_at": time.time()}
                    state["observed_input"] = observed
                    persist_waiting_for(observed)
                set_state(S.WAITING)
            elif notification_elicitation_dialog(data) and not state.get("pending_permission"):
                # Claude fell back to its own elicitation dialog (the brokering
                # hook did not answer). The tool is blocked on it: surface the
                # wait, but with no request id — only the terminal can answer.
                url_mode = data.get("notification_type") == "elicitation_url_dialog"
                observed = {
                    "kind": KIND_ELICITATION,
                    "id": None,
                    "mode": "url" if url_mode else "form",
                    "message": data.get("message"),
                    "requested_at": time.time(),
                }
                state["observed_input"] = observed
                persist_waiting_for(observed)
                set_state(S.WAITING)
                broadcast({"type": "elicitation_requested", "request": permission_preview(observed)})
            elif (notification_elicitation_settled(data) and state.get("observed_input")
                    and not state.get("pending_permission")):
                # set_state clears the observed dialog on leaving WAITING.
                set_state(S.BUSY)
        elif event_name == "PreToolUse":
            # Tool boundary events. `turn` is the in-flight turn number (the value
            # the matching turn_complete will report), so consumers can group a
            # turn's tool activity even before it settles. These do not drive the
            # lifecycle — the host is normally already BUSY — except for the one
            # case where a tool proves a turn we never observed starting.
            reclaim_missed_turn()
            release_observed_input()
            data = data or {}
            turn = state["turns"] + 1
            tuid = _hook_tool_use_id(data)
            if tuid is not None:
                # Bound the in-flight map so a stream of unmatched starts (missing
                # PostToolUse) can't grow it without limit.
                if len(state["tool_starts"]) > 256:
                    state["tool_starts"].clear()
                state["tool_starts"][tuid] = time.time()
            # Earliest reliable hook where the ai-title is guaranteed to be in the
            # transcript (Claude writes it before the first assistant event, which
            # precedes any tool execution). One-shot: adopt_ai_title returns None
            # while no title exists yet, False once the name is confirmed custom,
            # and True after the update — all three cases set ai_titled on non-None.
            if not state["ai_titled"]:
                try:
                    result = adopt_ai_title(short, rec["transcript"])
                    if result is not None:
                        state["ai_titled"] = True
                except Exception:
                    pass
            broadcast(_tool_started_event(data, turn))
        elif event_name in ("PostToolUse", "PostToolUseFailure"):
            release_observed_input()
            data = data or {}
            turn = state["turns"] + 1
            tuid = _hook_tool_use_id(data)
            started = state["tool_starts"].pop(tuid, None) if tuid is not None else None
            duration_ms = int((time.time() - started) * 1000) if started else None
            # Both the published and queued permission gates participate. A
            # completed tool's *exact* original input wins, never a preview or
            # unrelated tool-use ID. FIFO resolves accepted identical ambiguity.
            req = _oldest_native_gate(
                state.get("pending_permission"), state["permission_queue"], data)
            if req is not None:
                failed = event_name == "PostToolUseFailure"
                # The native operation already ended. A special completion
                # sentinel tells the blocked PermissionRequest relay to return
                # without issuing a second allow/deny or updated input. The
                # failure is observed, not interpreted as a human rejection.
                state["permission_decisions"][req["id"]] = {
                    "external": True,
                    "outcome": "failure" if failed else "completed",
                }
                if req is state.get("pending_permission"):
                    resolve_published_gate()
                else:
                    take_queued_gate(req["id"])
                broadcast({"type": "question_answered_external",
                           "request_id": req["id"],
                           "tool": req.get("tool_name"),
                           "tool_use_id": tuid,
                           "outcome": "failure" if failed else "completed"})
            event = _tool_finished_event(data, turn, duration_ms)
            if event_name == "PostToolUseFailure":
                event["ok"] = False
            broadcast(event)
        elif event_name == "SubagentStop":
            data = data or {}
            broadcast(_subagent_finished_event(data, state["turns"] + 1))
        # PreToolUse/PostToolUse/SubagentStop: stay BUSY, just refresh activity

    def read_pty_output() -> bool:
        """Drain one PTY output block, including during a long input write."""
        eof = False
        try:
            data = os.read(master_fd, 4096)
            if data == b"":
                eof = True
        except BlockingIOError:
            data = b""
        except OSError:
            data = b""
            eof = True
        if eof:
            pty_gone[0] = True
            return False
        if data:
            hub.feed_output(data)
            state["pty_buf"].extend(data)
            if len(state["pty_buf"]) > 65536:
                del state["pty_buf"][:len(state["pty_buf"]) - 65536]
            state["last_pty_read"] = time.time()
        return True

    def write_pty(payload: bytes, *, timeout_s: float = PTY_WRITE_TIMEOUT_S,
                  absolute_timeout_s: float | None = None) -> bool:
        """The single byte-safe path for every write to Claude's PTY."""
        return write_all(
            master_fd,
            payload,
            timeout_s=timeout_s,
            absolute_timeout_s=absolute_timeout_s,
            on_read=read_pty_output,
            on_partial=lambda written, sent, total: broadcast({
                "type": "pty_write_partial",
                "wrote": written,
                "sent": sent,
                "of": total,
            }),
        )

    def write_unstick() -> bool:
        """Phase 1 of the retry clear: force Claude's decoder out of any stuck
        paste-decode state so the buffered residue lands in the visible editor.
        See unstick_bytes()."""
        return write_pty(unstick_bytes())

    def write_drain() -> bool:
        """Phase 2 of the /compact residue clear: drain the (now visible)
        single-line input with a Ctrl-U burst. Run a tick after write_unstick()
        so the unstick's async editor update has applied. See drain_bytes()."""
        return write_pty(drain_bytes())

    def write_clear_input() -> bool:
        """Phase 2 of a prompt residue clear: empty the (now visible) editor
        with one Ctrl-C. Run a tick after write_unstick(). Presses are spaced
        so two can never land inside Claude's double-press exit window; the
        wait is bounded by CLEAR_INPUT_MIN_GAP_S. See clear_input_bytes()."""
        wait = state["clear_input_at"] + CLEAR_INPUT_MIN_GAP_S - time.monotonic()
        if wait > 0:
            time.sleep(min(wait, CLEAR_INPUT_MIN_GAP_S))
        state["clear_input_at"] = time.monotonic()
        return write_pty(clear_input_bytes())

    def pty_rows() -> int:
        try:
            return get_winsize_rows(master_fd)
        except OSError:
            return DEFAULT_PTY_ROWS

    def stage_paste(text: str) -> tuple[float, int] | None:
        """Write whole inline paste frames while letting Ink consume each one."""
        chunks = paste_chunks(text, inline_paste_newline_cap(pty_rows()))
        if chunks is None:
            payload, delay, pastes = prompt_paste(text, pty_rows())
            if not write_pty(payload):
                return None
            return delay, pastes
        chunks = chunks or [text]
        # Keep tiny prompts as one syscall, including multiple inline frames:
        # that is the historical native first-turn/variable-binding contract.
        # Large inputs need frame-by-frame delivery to avoid starving Ink.
        if len(text.encode()) <= 256:
            return (PASTE_SUBMIT_DELAY_S, len(chunks)) if write_pty(
                b"".join(paste_bytes(chunk) for chunk in chunks)) else None
        for chunk in chunks:
            if not write_pty(paste_bytes(chunk)):
                return None
            # Give the terminal decoder a complete UTF-8 frame per read cycle;
            # without pacing, hundreds queue before the editor can consume them.
            until = time.monotonic() + 0.01
            while (wait := until - time.monotonic()) > 0:
                try:
                    readable, _, _ = select.select([master_fd], [], [], wait)
                except InterruptedError:
                    continue
                if readable and not read_pty_output():
                    return None
        return PASTE_SUBMIT_DELAY_S, len(chunks)

    def write_paste(text: str, submit: bool = True) -> bool:
        """Write a prompt to the worker's PTY. Returns True on success.
        ``submit=False`` omits the trailing \\r (see paste_bytes)."""
        if stage_paste(text) is None:
            return False
        if not submit:
            return True
        time.sleep(PASTE_SUBMIT_DELAY_S)
        return write_pty(b"\r")

    def write_enter() -> bool:
        """Press Enter on the worker's PTY. Returns True on success."""
        return write_pty(b"\r")

    def write_escape() -> bool:
        """Press Escape on the worker's PTY (the TUI's chat:cancel binding).
        Best-effort by design: the TUI decides what — if anything — it cancels.
        Returns True on success."""
        return write_pty(b"\x1b")

    def current_semantic_slot() -> dict | None:
        slot = state.get("semantic_delivery")
        if not isinstance(slot, dict) or not slot.get("delivery_id"):
            return None
        return slot

    def ensure_semantic_hot(slot: dict) -> dict | None:
        delivery_id = slot["delivery_id"]
        hot = semantic_hot(delivery_id)
        if hot is not None:
            return hot
        try:
            hot, _created = begin_steer_delivery(
                short,
                delivery_id=delivery_id,
                text=slot["text"],
                transcript=rec["transcript"],
                host_epoch=event_epoch,
                state=state["current"],
                cap=slot.get("cap"),
                requested_mode=slot.get("mode"),
                route=slot.get("route"),
                target_turn_ref=slot.get("target_turn_ref"),
                timeout=STEER_LOCK_TIMEOUT_S,
            )
            return hot
        except Exception as exc:
            broadcast({
                "type": "semantic_hot_journal_error",
                "delivery_id": delivery_id,
                "reason": str(exc),
            })
            return None

    def semantic_accept(
        slot: dict,
        *,
        placement: str,
        turn_ref: str,
        evidence: str,
        evidence_uuid: str | None = None,
        evidence_offset: int | None = None,
        native_user_event: dict | None = None,
    ) -> bool:
        delivery_id = slot["delivery_id"]
        hot = semantic_hot(delivery_id)
        if hot is not None:
            desired = "consumed" if placement == "current_turn" else "started"
            current_status = hot.get("status")
            if current_status != desired:
                try:
                    transition_steer_delivery(
                        short,
                        delivery_id,
                        desired,
                        evidence=evidence,
                        evidence_uuid=evidence_uuid,
                        evidence_offset=evidence_offset,
                        maybe_written=False,
                        timeout=STEER_LOCK_TIMEOUT_S,
                    )
                except Exception as exc:
                    # A terminal conflicting placement is a correctness error;
                    # leave the semantic receipt nonterminal rather than hiding it.
                    broadcast({
                        "type": "semantic_hot_journal_error",
                        "delivery_id": delivery_id,
                        "operation": "accept-transition",
                        "reason": str(exc),
                    })
                    return False
        try:
            receipt = mark_semantic_accepted(
                short,
                delivery_id,
                placement=placement,
                turn_ref=turn_ref,
                managed_turn_ordinal=state["turns"] + 1,
                evidence=evidence,
                evidence_uuid=evidence_uuid,
                evidence_offset=evidence_offset,
                timeout=SEMANTIC_LOCK_TIMEOUT_S,
            )
        except Exception as exc:
            broadcast({
                "type": "semantic_receipt_error",
                "delivery_id": delivery_id,
                "operation": "accept",
                "reason": str(exc),
            })
            return False
        # A native UPS can mint the turn before Claude writes its exact user
        # transcript row. Bind only a *positively accepted*, current-host
        # original event when that same turn still has no delivery owner.
        if native_user_event is not None and slot.get("route") == "new_turn":
            bind_accepted_original_new_turn(
                state, receipt, turn_ref=turn_ref,
                native_user_event=native_user_event,
                delivery_host_epoch=(hot or {}).get("host_epoch"),
                current_host_epoch=event_epoch,
            )
        if state.get("semantic_delivery") is slot:
            state["semantic_delivery"] = None
        broadcast({
            "type": "semantic_accepted",
            "delivery_id": delivery_id,
            "route": receipt.get("route"),
            "placement": placement,
            "turn_ref": turn_ref,
            "evidence": evidence,
            "evidence_uuid": evidence_uuid,
        })
        return True

    def semantic_reject_unwritten(slot: dict, *, code: str, message: str) -> bool:
        delivery_id = slot["delivery_id"]
        hot = semantic_hot(delivery_id)
        if hot is not None:
            try:
                fail_semantic_hot_unwritten(
                    short,
                    delivery_id,
                    reason=message,
                    timeout=STEER_LOCK_TIMEOUT_S,
                )
            except Exception as exc:
                broadcast({
                    "type": "semantic_hot_journal_error",
                    "delivery_id": delivery_id,
                    "operation": "reject",
                    "reason": str(exc),
                })
                return False
        try:
            mark_semantic_rejected(
                short,
                delivery_id,
                code=code,
                message=message,
                timeout=SEMANTIC_LOCK_TIMEOUT_S,
            )
        except Exception as exc:
            broadcast({
                "type": "semantic_receipt_error",
                "delivery_id": delivery_id,
                "operation": "reject",
                "reason": str(exc),
            })
            return False
        if state.get("semantic_delivery") is slot:
            state["semantic_delivery"] = None
        broadcast({
            "type": "semantic_rejected",
            "delivery_id": delivery_id,
            "code": code,
            "reason": message,
        })
        return True

    def semantic_exact_event(slot: dict) -> dict | None:
        hot = semantic_hot(slot["delivery_id"]) or {}
        offset = int(hot.get("paste_offset") or hot.get("fence_offset") or 0)
        try:
            return exact_user_prompt_event(
                rec["transcript"],
                slot["text"],
                after_offset=offset,
            )
        except Exception:
            return None

    def semantic_new_turn_ref(slot: dict, *, source: str, event: dict | None = None) -> str:
        current = state.get("active_turn_ref")
        if (
            isinstance(current, str)
            and current
            and state.get("active_turn_delivery_id") == slot.get("delivery_id")
        ):
            return current
        hot = semantic_hot(slot["delivery_id"]) or {}
        # Transcript persistence can win the select-loop race against the UPS
        # hook. If this delivery was attempted by the current host, the matching
        # native user event is authoritative start evidence for the live turn:
        # establish the host turn instance now so the later hook and completion
        # reuse exactly this identity.
        if (
            event is not None
            and hot.get("host_epoch") == event_epoch
            and state["current"] in (S.IDLE, S.BUSY)
        ):
            return ensure_turn_instance(
                source,
                delivery_id=slot["delivery_id"],
                report_start_offset=(event or {}).get("offset"),
            )
        # Recovery may discover transcript evidence after the originating host
        # or turn is gone. No caller observed a provisional id, so mint a durable
        # recovery identity without claiming a live active turn.
        turn_ref = new_turn_ref()
        recovered = {
            "type": "turn_identity_recovered",
            "turn_ref": turn_ref,
            "turn": state["turns"],
            "source": source,
            "delivery_id": slot["delivery_id"],
            "evidence_uuid": (event or {}).get("uuid"),
        }
        report_start_offset = (event or {}).get("offset")
        if isinstance(report_start_offset, int) and report_start_offset >= 0:
            recovered["report_start_offset"] = report_start_offset
        broadcast(recovered)
        return turn_ref

    def reconcile_semantic_delivery() -> None:
        slot = current_semantic_slot()
        if slot is None:
            return
        delivery_id = slot["delivery_id"]
        try:
            receipt = read_semantic_receipt(short, delivery_id)
        except Exception as exc:
            broadcast({
                "type": "semantic_receipt_error",
                "delivery_id": delivery_id,
                "operation": "reconcile",
                "reason": str(exc),
            })
            return
        if receipt is None:
            return
        if receipt.get("state") in {"accepted", "rejected", "delivered", "abandoned"}:
            if state.get("semantic_delivery") is slot:
                state["semantic_delivery"] = None
            return

        hot = semantic_hot(delivery_id)
        if hot is not None and hot.get("status") == "failed":
            semantic_reject_unwritten(
                slot,
                code="proven_undelivered",
                message=hot.get("reason") or "semantic delivery proven undelivered",
            )
            return

        if hot is not None:
            try:
                hot = reconcile_steer_delivery(
                    short,
                    delivery_id,
                    transcript=rec["transcript"],
                    timeout=STEER_LOCK_TIMEOUT_S,
                ) or hot
            except Exception:
                pass

        # Exact ordinary-user evidence is the strongest new-turn correlation.
        event = semantic_exact_event(slot) if slot.get("maybe_written") else None
        if event is not None:
            turn_ref = semantic_new_turn_ref(
                slot, source="semantic-transcript-user", event=event
            )
            semantic_accept(
                slot,
                placement="new_turn",
                turn_ref=turn_ref,
                evidence="user-event",
                evidence_uuid=event.get("uuid"),
                evidence_offset=event.get("offset"),
                native_user_event=event,
            )
            return

        if hot is None:
            return
        status = hot.get("status")
        if status == "consumed" and slot.get("target_turn_ref"):
            semantic_accept(
                slot,
                placement="current_turn",
                turn_ref=slot["target_turn_ref"],
                evidence=hot.get("evidence") or "queued-command",
                evidence_uuid=hot.get("evidence_uuid"),
                evidence_offset=hot.get("evidence_offset"),
            )
            return
        if status == "started":
            # Recheck the hot journal's UUID/offset against the original
            # current-epoch user row before binding it to Stop settlement.
            # Placement evidence alone may be a dequeue or an attachment.
            started_user = None
            if (slot.get("route") == "new_turn"
                    and hot.get("host_epoch") == event_epoch
                    and hot.get("transcript") == rec["transcript"]
                    and hot.get("evidence") == "started"):
                try:
                    started_user = original_started_user_event(
                        rec["transcript"], slot["text"],
                        after_offset=max(int(hot.get("paste_offset") or 0),
                                         int(hot.get("fence_offset") or 0)),
                        evidence_uuid=hot.get("evidence_uuid"),
                        evidence_offset=hot.get("evidence_offset"),
                    )
                except (OSError, ValueError, TypeError):
                    pass
            turn_ref = semantic_new_turn_ref(
                slot, source="semantic-hot-started",
                event={"uuid": hot.get("evidence_uuid")},
            )
            semantic_accept(
                slot,
                placement="new_turn",
                turn_ref=turn_ref,
                evidence=hot.get("evidence") or "started",
                evidence_uuid=hot.get("evidence_uuid"),
                evidence_offset=hot.get("evidence_offset"),
                native_user_event=started_user,
            )
            return
        if status == "unknown":
            try:
                mark_semantic_evidence(
                    short,
                    delivery_id,
                    evidence=hot.get("evidence") or "unknown",
                    evidence_uuid=hot.get("evidence_uuid"),
                    evidence_offset=hot.get("evidence_offset"),
                    evidence_status="unknown",
                    maybe_written=True,
                    timeout=SEMANTIC_LOCK_TIMEOUT_S,
                )
            except Exception:
                pass
            slot["phase"] = "await_evidence"
            slot["maybe_written"] = True

    def semantic_foreign_turn_start(source: str) -> None:
        slot = current_semantic_slot()
        if slot is None:
            return
        if (slot.get("paste_staged") and slot.get("submit_due") is not None
                and not slot.get("writes")):
            # Staged in the editor but not submitted: the foreign turn cannot
            # have consumed it. Keep the slot as is; the idle pump presses the
            # pending Enter once that turn ends, and re-arms the cap then.
            state["active_cap"] = None
            state["last_cap_poll"] = 0.0
            return
        if slot.get("maybe_written"):
            slot["phase"] = "await_evidence"
            try:
                mark_semantic_evidence(
                    short,
                    slot["delivery_id"],
                    evidence=source,
                    evidence_status="foreign-start-after-possible-write",
                    maybe_written=True,
                    timeout=SEMANTIC_LOCK_TIMEOUT_S,
                )
            except Exception:
                pass
            return
        semantic_reject_unwritten(
            slot,
            code="preempted",
            message=f"foreign turn started before semantic delivery ({source})",
        )

    def semantic_user_prompt(prompt, *, report_start_offset: int | None = None) -> bool:
        """Attribute one UPS to the semantic slot when exact prompt bytes match."""
        slot = current_semantic_slot()
        if slot is None:
            return False
        expected = normalize_semantic_prompt_text(slot.get("text") or "").strip()
        matches = prompt_matches(expected, prompt)
        if matches and slot.get("maybe_written"):
            # For a boundary steer, Claude can fire UserPromptSubmit when it
            # accepts/enqueues the queued input while the target turn is still
            # running. live evidence shows that this is not a new-turn boundary:
            # the actual placement is later proven by queued_command attachment
            # (current turn) or ordinary user/dequeue evidence (next turn).
            if (
                slot.get("phase") in {"steer_boundary", "await_evidence"}
                and not slot.get("target_settled")
            ):
                try:
                    mark_semantic_evidence(
                        short,
                        slot["delivery_id"],
                        evidence="user-prompt-submit",
                        evidence_status="accepted-placement-pending",
                        maybe_written=True,
                        timeout=SEMANTIC_LOCK_TIMEOUT_S,
                    )
                except Exception:
                    pass
                return True
            if slot.get("target_settled"):
                slot["phase"] = "new_turn_fallback"

            active_ref = state.get("active_turn_ref")
            active_delivery = state.get("active_turn_delivery_id")
            if active_ref and active_delivery != slot.get("delivery_id"):
                semantic_foreign_turn_start("user-prompt-submit-active-conflict")
                return False
            turn_ref = ensure_turn_instance(
                "user-prompt-submit",
                delivery_id=slot["delivery_id"],
                report_start_offset=report_start_offset,
            )
            hot = semantic_hot(slot["delivery_id"])
            if hot is not None and hot.get("status") in STEER_PENDING_STATUSES | {"unknown"}:
                try:
                    transition_steer_delivery(
                        short,
                        slot["delivery_id"],
                        "started",
                        evidence="user-prompt-submit",
                        timeout=STEER_LOCK_TIMEOUT_S,
                    )
                except Exception:
                    pass
            semantic_accept(
                slot,
                placement="new_turn",
                turn_ref=turn_ref,
                evidence="user-prompt-submit",
            )
            return True

        # A mismatched prompt is foreign. If our write may already exist, this
        # only makes the semantic placement ambiguous; it never permits resend.
        semantic_foreign_turn_start("user-prompt-submit")
        return False

    def semantic_target_completed(turn_ref: str | None) -> None:
        slot = current_semantic_slot()
        if slot is None or slot.get("route") != "steer":
            return
        if not turn_ref or slot.get("target_turn_ref") != turn_ref:
            return
        if slot.get("maybe_written"):
            slot["phase"] = "await_evidence"
            slot["target_settled"] = True
            return
        slot["phase"] = "new_turn_fallback"
        broadcast({
            "type": "semantic_boundary_fallback",
            "delivery_id": slot["delivery_id"],
            "target_turn_ref": turn_ref,
        })

    def prepare_semantic_write(slot: dict) -> bool:
        """Durably mark possible PTY delivery before any prompt bytes are written."""
        delivery_id = slot["delivery_id"]
        hot = ensure_semantic_hot(slot)
        if hot is None:
            return False

        # Repair a cross-store gap where the hot attempt was published but the
        # receipt update was interrupted. Do not create another hot attempt.
        try:
            receipt = read_semantic_receipt(short, delivery_id) or {}
        except Exception:
            return False
        hot_attempts = int(hot.get("attempts") or 0)
        receipt_attempts = int(receipt.get("attempts") or 0)
        if hot.get("status") == "pasted" and hot_attempts > receipt_attempts:
            try:
                receipt = mark_semantic_attempt(
                    short,
                    delivery_id,
                    maybe_written=True,
                    timeout=SEMANTIC_LOCK_TIMEOUT_S,
                )
            except Exception:
                return False
            slot["attempts"] = int(receipt.get("attempts") or 0)
            slot["maybe_written"] = True
            return True

        offset = transcript_offset(rec["transcript"])
        try:
            hot = mark_steer_pasted(
                short,
                delivery_id,
                offset=offset,
                timeout=STEER_LOCK_TIMEOUT_S,
            )
        except Exception as exc:
            broadcast({
                "type": "semantic_hot_journal_error",
                "delivery_id": delivery_id,
                "operation": "mark-pasted",
                "reason": str(exc),
            })
            return False
        try:
            receipt = mark_semantic_attempt(
                short,
                delivery_id,
                maybe_written=True,
                timeout=SEMANTIC_LOCK_TIMEOUT_S,
            )
        except Exception as exc:
            # Hot status now conservatively blocks any resend. The next pass
            # repairs the receipt before writing bytes.
            broadcast({
                "type": "semantic_receipt_error",
                "delivery_id": delivery_id,
                "operation": "mark-attempt",
                "reason": str(exc),
            })
            return False
        slot["attempts"] = int(receipt.get("attempts") or hot.get("attempts") or 0)
        slot["maybe_written"] = True
        slot["paste_offset"] = offset
        return True

    def boundary_deliver_semantic(data: dict) -> dict | None:
        slot = current_semantic_slot()
        if slot is None or slot.get("phase") != "steer_boundary":
            return None
        if state["current"] != S.BUSY:
            return None
        target = slot.get("target_turn_ref")
        if target and target == state.get("settling_turn_ref"):
            return {"ok": True, "delivered": False, "pending": True,
                    "reason": "target turn is settling"}
        if input_surface_write_blocked(state):
            return {"ok": True, "delivered": False, "pending": True,
                    "reason": "local input residue clear pending"}
        if not target or state.get("active_turn_ref") != target:
            return None
        if slot.get("maybe_written"):
            return {"ok": True, "delivered": False, "pending": True}

        transcript_path = data.get("transcript_path")
        if not transcript_path:
            return {"ok": True, "delivered": False, "reason": "hook has no transcript_path"}

        if not prepare_semantic_write(slot):
            return {"ok": True, "delivered": False, "pending": True,
                    "reason": "semantic write intent not yet durable"}

        before_ms = int((time.time() - 0.1) * 1000)
        if not write_paste(slot["text"]):
            slot["phase"] = "await_evidence"
            try:
                mark_semantic_evidence(
                    short,
                    slot["delivery_id"],
                    evidence_status="pty-write-failed",
                    maybe_written=True,
                    timeout=SEMANTIC_LOCK_TIMEOUT_S,
                )
            except Exception:
                pass
            return {"ok": False, "delivered": False, "pending": True,
                    "error": "PTY write failed"}

        now = time.time()
        slot["writes"].append(now)
        state["last_activity"] = now
        state["active_cap"] = _merge_caps([state.get("active_cap"), slot.get("cap")])
        state["last_cap_poll"] = 0.0
        deadline = now + STEER_BOUNDARY_ACK_S
        while time.time() < deadline:
            if queued_prompt_accepted(transcript_path, slot["text"], after_ms=before_ms):
                reconcile_semantic_delivery()
                return {"ok": True, "delivered": True,
                        "delivery_id": slot["delivery_id"]}
            time.sleep(BOUNDARY_SEND_POLL_S)

        slot["phase"] = "await_evidence"
        broadcast({
            "type": "semantic_awaiting_evidence",
            "delivery_id": slot["delivery_id"],
            "route": "steer",
        })
        return {"ok": True, "delivered": False, "pending": True,
                "reason": "awaiting native delivery evidence"}

    def semantic_paste_write_failed(slot: dict, code: str) -> None:
        # The worker can be healthy while its input/output PTY buffers are
        # backpressured. The original input may be partial: never blindly retry
        # or claim rejection until a quiet Ctrl-C clear has been attempted.
        slot["phase"] = "paste_write_failed"
        slot["maybe_written"] = True
        slot["write_failure_code"] = code
        try:
            mark_semantic_evidence(
                short, slot["delivery_id"],
                evidence_status=code, maybe_written=True,
                timeout=SEMANTIC_LOCK_TIMEOUT_S,
            )
        except Exception:
            pass
        broadcast({"type": "semantic_paste_write_failed",
                   "delivery_id": slot["delivery_id"], "reason": code})

    def stage_semantic_idle_paste(slot: dict, *, retry: bool) -> bool:
        if not prepare_semantic_write(slot):
            return False
        now = time.time()
        staged = stage_paste(slot["text"])
        if staged is None:
            semantic_paste_write_failed(slot, "pty-write-failed")
            return False
        delay, pastes = staged
        slot["submit_due"] = now + delay
        slot["submit_retry"] = retry
        slot["paste_staged"] = True
        state["active_cap"] = slot.get("cap")
        state["last_cap_poll"] = 0.0
        state["last_activity"] = now
        broadcast({
            "type": "semantic_paste_staged",
            "delivery_id": slot["delivery_id"],
            "retry": retry,
            "text_len": len(slot["text"]),
            "pastes": pastes,
        })
        return True

    def semantic_idle_pump() -> None:
        slot = current_semantic_slot()
        if slot is None:
            return
        if slot.get("phase") == "paste_write_failed":
            # An Enter write can report a stall after its first byte reaches
            # Claude. Native user/queue evidence always wins over clearing.
            reconcile_semantic_delivery()
            slot = current_semantic_slot()
            if slot is None or slot.get("phase") != "paste_write_failed":
                return
            if state["current"] != S.IDLE or input_surface_write_blocked(state):
                return
            now = time.time()
            action = stalled_paste_clear_action(
                slot, now, state.get("last_pty_read", 0.0))
            if action == "unstick" and write_unstick():
                slot["stalled_unstuck_at"] = now
            elif action == "clear" and write_clear_input():
                slot["stalled_cleared_at"] = now
            elif action == "reject":
                semantic_reject_unwritten(
                    slot, code=slot.get("write_failure_code") or "pty-write-failed",
                    message="Claude PTY paste made no progress; partial editor input "
                            "was cleared without submission (worker still running)",
                )
            return
        if slot.get("phase") not in {"new_turn", "new_turn_fallback"}:
            return
        if state["current"] != S.IDLE or state.get("active_turn_ref"):
            return
        if input_surface_write_blocked(state):
            return

        reconcile_semantic_delivery()
        slot = current_semantic_slot()
        if slot is None or slot.get("phase") not in {"new_turn", "new_turn_fallback"}:
            return

        now = time.time()
        submit_due = slot.get("submit_due")
        if submit_due is not None:
            if now < submit_due:
                return
            slot.pop("submit_due", None)
            retry = bool(slot.pop("submit_retry", False))
            if not write_enter():
                semantic_paste_write_failed(slot, "pty-submit-failed")
                return
            slot["paste_staged"] = False
            slot["writes"].append(now)
            state["active_cap"] = slot.get("cap")
            state["last_cap_poll"] = 0.0
            state["last_activity"] = now
            broadcast({
                "type": "semantic_paste_submitted",
                "delivery_id": slot["delivery_id"],
                "retry": retry,
            })
            return

        writes = slot.get("writes") or []
        last_pty = state.get("last_pty_read", 0.0)
        pty_quiet = last_pty > 0 and (now - last_pty) >= PTY_QUIESCENCE_S

        if not writes and not slot.get("paste_staged"):
            # Only the first staging may discount subagent rendering; retry
            # decisions below keep raw PTY activity (input_ready_pty_read).
            first_pty = pty_read_for_input_ready(now)
            if (now < state.get("earliest_send_at", 0.0)
                    or not (first_pty > 0 and (now - first_pty) >= PTY_QUIESCENCE_S)):
                return
            stage_semantic_idle_paste(slot, retry=False)
            return

        event = semantic_exact_event(slot)
        if event is not None:
            turn_ref = semantic_new_turn_ref(
                slot, source="semantic-idle-transcript", event=event
            )
            semantic_accept(
                slot,
                placement="new_turn",
                turn_ref=turn_ref,
                evidence="user-event",
                evidence_uuid=event.get("uuid"),
                evidence_offset=event.get("offset"),
                native_user_event=event,
            )
            return

        if len(writes) == 1:
            if now - writes[0] <= SEND_FIRST_ACK_S:
                return
            if not slot.get("unstuck_at"):
                if not pty_quiet:
                    return
                write_unstick()
                slot["unstuck_at"] = now
                state["last_activity"] = now
                return
            if not slot.get("cleared_at"):
                if now - slot["unstuck_at"] < POST_CLEAR_SETTLE_S:
                    return
                write_clear_input()
                slot["cleared_at"] = now
                state["last_activity"] = now
                return
            settled = now - slot["cleared_at"] >= POST_CLEAR_SETTLE_S
            if not settled or not pty_quiet:
                return
            try:
                reset_semantic_hot_for_retry(
                    short,
                    slot["delivery_id"],
                    timeout=STEER_LOCK_TIMEOUT_S,
                )
                mark_semantic_evidence(
                    short,
                    slot["delivery_id"],
                    evidence="cleared-undelivered",
                    evidence_status="retry-safe",
                    maybe_written=False,
                    timeout=SEMANTIC_LOCK_TIMEOUT_S,
                )
            except Exception as exc:
                broadcast({
                    "type": "semantic_retry_reset_error",
                    "delivery_id": slot["delivery_id"],
                    "reason": str(exc),
                })
                slot["phase"] = "await_evidence"
                return
            slot["maybe_written"] = False
            slot.pop("unstuck_at", None)
            slot.pop("cleared_at", None)
            stage_semantic_idle_paste(slot, retry=True)
            return

        if len(writes) >= 2 and now - writes[-1] > SEND_RETRY_ACK_S:
            # Before terminal rejection, clear the second possibly-stranded
            # editor paste too. This is positive non-delivery proof, not a blind
            # inference from silence.
            if not slot.get("final_unstuck_at"):
                if not pty_quiet:
                    return
                write_unstick()
                slot["final_unstuck_at"] = now
                state["last_activity"] = now
                return
            if not slot.get("final_cleared_at"):
                if now - slot["final_unstuck_at"] < POST_CLEAR_SETTLE_S:
                    return
                write_clear_input()
                slot["final_cleared_at"] = now
                state["last_activity"] = now
                return
            if now - slot["final_cleared_at"] < POST_CLEAR_SETTLE_S or not pty_quiet:
                return
            event = semantic_exact_event(slot)
            if event is not None:
                turn_ref = semantic_new_turn_ref(
                    slot, source="semantic-final-transcript", event=event
                )
                semantic_accept(
                    slot,
                    placement="new_turn",
                    turn_ref=turn_ref,
                    evidence="user-event",
                    evidence_uuid=event.get("uuid"),
                    evidence_offset=event.get("offset"),
                    native_user_event=event,
                )
                return
            semantic_reject_unwritten(
                slot,
                code="not_submitted",
                message="Claude did not acknowledge either semantic paste; editor cleared",
            )

    def foreign_turn_since_compact(operation: dict) -> bool:
        """Is an ordinary turn already running past this compact's boundary?

        Fail-closed: an unreadable transcript answers "no", preserving the
        historic release-to-IDLE.  Guessing "yes" here would pin a healthy
        worker BUSY on a question we could not actually answer.
        """
        try:
            return foreign_prompt_since(
                rec["transcript"],
                after_offset=compact_settle_fence(operation),
            )
        except Exception:
            return False

    def settle_compact_terminal(operation: dict) -> bool:
        """Release the input surface after any durable terminal record.

        Both the main pump and ``compact-status`` can be the first observer of
        the verdict.  Whichever wins performs the same finalization exactly
        once.  This keeps a blocking client from returning a result while the
        host still reports BUSY for the just-completed operation.

        Failure settles exactly like success, and deliberately does not crash
        the session: Claude reported that compaction did not happen, which says
        nothing bad about the worker — it is idle and healthy, and only the
        operation died.  Crashing here would throw away a live session over a
        recoverable server-side hiccup.
        """
        active = state.get("compacting")
        operation_id = operation.get("operation_id")
        if not active or active.get("operation_id") != operation_id:
            return False
        status = operation.get("status")
        if status not in {"succeeded", "noop", "failed"}:
            return False
        result = operation.get("result") or {}
        state["compacting"] = None
        # Publish input-surface release immediately after the durable result.
        # Transcript rendering is best-effort and may be slow for very large
        # runs; it must not make a settled compact look BUSY to a caller whose
        # status RPC deadline is shorter than the render.
        #
        # BUSY is shared, though, and releasing it is not ours to do
        # unconditionally.  Claude flushes whatever it queued during compaction
        # the instant the boundary lands, so a real turn is often already
        # running by the time we observe the verdict — and that turn's
        # UserPromptSubmit was a no-op, because we still held BUSY when it
        # fired.  Forcing IDLE there reports an idle worker for the whole turn:
        # queued sends stall against a PTY that never quiets, `--require-idle`
        # accepts into a live turn, and the idle-killer eyes a working session.
        # The transcript settles it — live-measured, Claude journals the turn's
        # user record ~200ms before firing its hook, so by the time we can look
        # the evidence is already on disk.
        turn_in_flight = state["compact_turn_started"]
        state["compact_turn_started"] = False
        if turn_in_flight or foreign_turn_since_compact(operation):
            state["post_compact_hold"] = {
                "at": time.time(), "operation_id": operation_id,
            }
            broadcast({"type": "compact_turn_in_flight",
                       "operation_id": operation_id,
                       "evidence": ("user-prompt-submit" if turn_in_flight
                                    else "transcript-foreign-prompt")})
        else:
            state["post_compact_hold"] = None
            set_state(S.IDLE)
        request_refresh()
        compact_context_fence("just_compacted" if status == "succeeded" else None)
        if status == "failed":
            broadcast({
                "type": "compact_failed",
                "operation_id": operation_id,
                "reason": operation.get("reason"),
                "evidence": "transcript-verdict",
                "evidence_uuid": operation.get("evidence_uuid"),
                "api_error": result.get("api_error"),
            })
        else:
            broadcast({
                "type": "compact_complete",
                "operation_id": operation_id,
                "evidence_uuid": operation.get("evidence_uuid"),
                **result,
            })
        return True

    def compact_transition(operation_id: str, status: str, **patch) -> dict | None:
        """Journal one host-side compact transition; never raise into the loop.

        Returns the updated operation, or None when nothing was written: the
        journal lock was busy (the caller retries on a later tick) or another
        writer already advanced the operation. The latter is a CLI/Python
        caller that reconciled from the transcript while this loop was slow;
        its terminal verdict is adopted here instead of being fought.
        """
        try:
            return transition_compact_operation(
                short, operation_id, status,
                lock_timeout=COMPACT_LOCK_TIMEOUT_S, **patch)
        except CompactJournalBusy as busy:
            broadcast({"type": "compact_journal_busy", "operation_id": operation_id,
                       "status": status, "reason": str(busy)})
            return None
        except ValueError:
            current = get_compact_operation(short, operation_id)
            if current is not None and current.get("status") in {"succeeded", "noop", "failed"}:
                settle_compact_terminal(current)
            return None

    def compact_reconcile(operation_id: str) -> dict | None:
        """Host-side reconcile; a busy journal skips this pass."""
        try:
            return reconcile_compact_operation(
                short, operation_id, transcript=rec["transcript"],
                lock_timeout=COMPACT_LOCK_TIMEOUT_S)
        except CompactJournalBusy:
            return None

    def abort_compact(reason: str, *, evidence: str, settle_idle: bool = False) -> bool:
        """Fail an unsubmitted compact whose paste provably never ran.

        The mirror image of ``settle_compact_terminal``: same single-owner
        finalization, opposite verdict.  Only ever called with positive proof
        that the native command did not start, which is what makes ``failed``
        honest here — a bare timeout stays ``unknown``, because absence of an
        acknowledgement is not absence of the command.

        The worker itself is untouched by any of this: it is running, or has
        just run, an ordinary turn.  So the session stays live and only the
        operation dies.  The stranded ``/compact`` text still sitting in Ink's
        editor is the one piece of real damage, and it is disarmed by the
        deferred clear rather than by taking the host down.
        """
        active = state.get("compacting")
        if active is None:
            return False
        operation_id = active.get("operation_id")
        if compact_transition(operation_id, "failed", reason=reason) is None:
            # Not recorded (busy journal, or someone else already settled
            # it). Leave the operation active: the deadline path re-reaches
            # the same verdict from the transcript.
            return False
        state["compacting"] = None
        # Residue disarm. The command text was pasted but never submitted, so
        # a later send would paste onto it and submit "/compact<prompt>". The
        # clear runs from the pump once the worker is idle and the PTY quiets,
        # in the same two separated ticks the send-retry path uses.
        state["compact_clear"] = {"unstuck_at": None, "cleared_at": None}
        # Who releases BUSY depends on which discriminator fired. The hook path
        # aborts while the displacing turn is still running, so that turn's own
        # Stop settles the state and counts the turn. The transcript path
        # only runs when the hook never arrived, by which point
        # the turn is long over and nothing else will ever release the state.
        if settle_idle:
            set_state(S.IDLE)
            request_refresh()
        broadcast({
            "type": "compact_failed",
            "operation_id": operation_id,
            "reason": reason,
            "evidence": evidence,
        })
        return True

    def handle_client_msg(client_sock, msg):
        op = msg.get("op")
        try:
            store.revalidate()
        except Exception as exc:
            client_sock.sendall((json.dumps({
                "ok": False, "error": "full-UUID generation changed: " + str(exc),
            }) + "\n").encode())
            return
        # A same-UID socket is not native authority. A non-hook mutator or
        # status request must first pass the exact root/gen/worker handshake
        # on this same kernel-authenticated connection. Native hook frames
        # have their own stronger original child PID/start/source check.
        if op == "statusline-updated":
            # Same-UID advisory wake only; source validation is performed on
            # the original bound statusline sidecar. No reply and no native effect.
            if (msg.get("worker_id") == short
                    and msg.get("session_id") == session_id):
                try:
                    observe_statusline_context()
                except Exception:
                    pass
            return
        if (op not in ("handshake", "hook-event")
                and client_sock not in state["handshaken_clients"]):
            client_sock.sendall((json.dumps({
                "ok": False, "error": "full-UUID transport handshake required",
            }) + "\n").encode())
            return
        if op == "attach-open":
            result = hub.open(client_sock, msg, epoch=event_epoch)
            client_sock.sendall((json.dumps(result) + "\n").encode())
            return
        elif op in ("attach-input", "attach-resize", "attach-close"):
            try:
                if not hub.owns(client_sock, msg):
                    raise ValueError("not the active attachment")
                if op == "attach-input":
                    hub.accept_input(client_sock, msg)
                elif op == "attach-resize":
                    hub.accept_resize(client_sock, msg)
                else:
                    hub.release(client_sock, "detached")
                    drop_client(client_sock)
            except ValueError:
                hub.release(client_sock, "invalid-attachment-frame")
                drop_client(client_sock)
            return
        elif op == "deliver":
            delivery_id = msg.get("delivery_id")
            text = msg.get("text") or ""
            mode = msg.get("mode") or "auto"
            try:
                cap = validate_semantic_cap(msg.get("cap"))
            except ClaudeDeliveryError as exc:
                client_sock.sendall((json.dumps({
                    "ok": False, "error": str(exc), "state": state["current"],
                }) + "\n").encode())
                return
            # The host's original durable semantic route can write a native
            # PTY prompt. The caller must have fsynced the separate exact
            # full-owner submitted-unknown effect fence before this RPC.
            # Reading an old "owned" journal or trusting a socket path does
            # not authorize a new send after a lost response.
            from ccx.store.store import _digest
            try:
                if mode not in {"new_turn_only", "auto", "steer_only"}:
                    raise ValueError("invalid semantic delivery mode")
                effect = store.read_receipt(
                    f"claude:{short}", "claude-delivery", delivery_id,
                )
                if not effect or effect.get("state") not in (
                    "submitted-unknown", "accepted", "rejected",
                ):
                    raise ValueError("original full-UUID native effect fence absent")
                import hashlib
                raw_prompt = text.encode("utf-8")
                wanted = semantic_spec_digest({
                    "deliveryId": delivery_id,
                    "promptSha256": hashlib.sha256(raw_prompt).hexdigest(),
                    "promptByteCount": len(raw_prompt),
                    "requestedMode": mode, "callerMetadata": None, "cap": cap,
                })
                original = read_semantic_receipt(short, delivery_id)
                if (not isinstance(original, dict)
                        or semantic_spec_digest(original) != wanted):
                    raise ValueError("original semantic mode/cap/prompt receipt differs")
                if (effect.get("intentSha256") != wanted
                        or effect.get("action") != "send"
                        or effect.get("workerId") != short
                        or effect.get("engine") != "claude"
                        or effect.get("generation") != store.generation):
                    raise ValueError("original semantic prompt/owner differs")
                existing = read_semantic_receipt(short, delivery_id)
                if effect["state"] in ("accepted", "rejected"):
                    if existing is None or existing.get("state") not in (
                        "accepted", "rejected",
                    ):
                        raise ValueError("native terminal effect lacks original semantic receipt")
                    client_sock.sendall(
                        (json.dumps(semantic_reply(delivery_id)) + "\n").encode()
                    )
                    return
                store.authorize_native_action(f"claude:{short}", "send")
            except Exception as exc:
                client_sock.sendall((json.dumps({
                    "ok": False, "error": "full-UUID native admission denied: " + str(exc),
                    "state": state["current"],
                }) + "\n").encode())
                return
            if not valid_semantic_delivery_id(delivery_id) or not text:
                client_sock.sendall((json.dumps({
                    "ok": False,
                    "error": "deliver requires non-empty text and a valid delivery_id",
                    "state": state["current"],
                }) + "\n").encode())
                return
            try:
                receipt, _created = prepare_semantic_delivery(
                    short, delivery_id, text, mode=mode,
                    cap=cap, timeout=SEMANTIC_LOCK_TIMEOUT_S,
                )
            except (ClaudeDeliveryConflict, ClaudeDeliveryError, ClaudeDeliveryBusy) as exc:
                client_sock.sendall((json.dumps({
                    "ok": False, "error": str(exc), "state": state["current"],
                }) + "\n").encode())
                return

            if receipt.get("state") != "prepared":
                client_sock.sendall((json.dumps(semantic_reply(delivery_id)) + "\n").encode())
                return

            blocker = semantic_pending_blocker()
            current_state = state["current"]
            route = None
            target_turn_ref = None
            refusal_code = None
            refusal_message = None

            if blocker is not None:
                if mode == "new_turn_only":
                    # A strict send cannot borrow another delivery's queued
                    # input surface, even while native state still reads IDLE.
                    refusal_code = "busy"
                    refusal_message = f"worker busy: {blocker}"
                else:
                    refusal_code = ("compaction-busy" if state.get("compacting") is not None
                                    or state.get("compact_clear") is not None else "input_surface_busy")
                    refusal_message = blocker
            elif current_state == S.IDLE and mode in {"new_turn_only", "auto"}:
                route = "new_turn"
            elif mode in {"auto", "steer_only"} and current_state == S.BUSY:
                target_turn_ref = state.get("active_turn_ref")
                if target_turn_ref:
                    route = "steer"
                else:
                    refusal_code = "turn_identity_unavailable"
                    refusal_message = "busy turn has no stable turn_ref yet"
            else:
                refusal_code = "busy" if current_state == S.BUSY else "state_unavailable"
                action = ("steer the running turn" if mode == "steer_only"
                          else "send a new message")
                refusal_message = (
                    f"Worker is {current_state}; cannot {action} in this state."
                )

            if route is None:
                if (mode in {"new_turn_only", "steer_only"}
                        or (blocker and blocker.startswith("queued work pending"))):
                    # The host owns the sole native input decision: this new
                    # token has no route and wrote no bytes. Close its receipt
                    # with an exact source-bound refusal, not a generic pending
                    # reply that strands the global submitted-unknown effect.
                    try:
                        mark_semantic_rejected(
                            short, delivery_id, code=refusal_code or "refused",
                            message=refusal_message or "delivery refused",
                            timeout=SEMANTIC_LOCK_TIMEOUT_S,
                        )
                    except Exception as exc:
                        client_sock.sendall((json.dumps({
                            "ok": False, "error": str(exc), "state": current_state,
                        }) + "\n").encode())
                        return
                    proof = _digest({
                        "event": "steer-preownership-refusal",
                        "workerId": short, "generation": store.generation,
                        "deliveryId": delivery_id, "intentSha256": wanted,
                        "rejectionCode": refusal_code or "refused",
                        "state": current_state,
                    })
                    client_sock.sendall((json.dumps({
                        "accepted": False, "nativeStatus": "rejected",
                        "workerId": short, "engine": "claude",
                        "deliveryId": delivery_id,
                        "nativeSource": f"claude-session:{session_id}",
                        "sessionId": session_id,
                        "nativeEvidenceSha256": proof,
                        "rejectionCode": refusal_code or "refused",
                        "rejectionMessage": refusal_message or "delivery refused",
                        "state": current_state,
                    }) + "\n").encode())
                    return
                try:
                    mark_semantic_refusal(
                        short, delivery_id,
                        code=refusal_code or "refused",
                        message=refusal_message or "delivery refused",
                        state=current_state,
                        timeout=SEMANTIC_LOCK_TIMEOUT_S,
                    )
                except Exception:
                    pass
                client_sock.sendall((json.dumps(semantic_reply(delivery_id)) + "\n").encode())
                return

            try:
                receipt = mark_semantic_owned(
                    short, delivery_id,
                    route=route,
                    target_turn_ref=target_turn_ref,
                    managed_turn_ordinal=state["turns"] + 1,
                    timeout=SEMANTIC_LOCK_TIMEOUT_S,
                )
            except Exception as exc:
                client_sock.sendall((json.dumps({
                    "ok": False, "error": str(exc), "state": current_state,
                }) + "\n").encode())
                return

            try:
                hot, _ = begin_steer_delivery(
                    short,
                    delivery_id=delivery_id,
                    text=text,
                    transcript=rec["transcript"],
                    host_epoch=event_epoch,
                    state=current_state,
                    cap=cap,
                    requested_mode=mode,
                    route=route,
                    target_turn_ref=target_turn_ref,
                    timeout=STEER_LOCK_TIMEOUT_S,
                )
            except Exception as exc:
                broadcast({
                    "type": "semantic_hot_journal_deferred",
                    "delivery_id": delivery_id,
                    "reason": str(exc),
                })
                hot = None

            if hot is not None and (
                hot.get("text") != text
                or hot.get("route") not in (None, route)
                or hot.get("cap") != cap
                or hot.get("requested_mode") != mode
                or hot.get("target_turn_ref") != target_turn_ref
            ):
                mark_semantic_rejected(
                    short, delivery_id,
                    code="hot_journal_conflict",
                    message="hot delivery journal conflicts with semantic receipt",
                    timeout=SEMANTIC_LOCK_TIMEOUT_S,
                )
                client_sock.sendall((json.dumps(semantic_reply(delivery_id)) + "\n").encode())
                return

            state["semantic_delivery"] = {
                "delivery_id": delivery_id,
                "text": text,
                "mode": mode,
                "route": route,
                "target_turn_ref": target_turn_ref,
                "cap": cap,
                "phase": "new_turn" if route == "new_turn" else "steer_boundary",
                "writes": [],
                "maybe_written": False,
                "attempts": 0,
                "queued_at": time.time(),
            }
            broadcast({
                "type": "semantic_owned",
                "delivery_id": delivery_id,
                "mode": mode,
                "route": route,
                "target_turn_ref": target_turn_ref,
            })
            client_sock.sendall((json.dumps(semantic_reply(delivery_id)) + "\n").encode())
        elif op == "semantic-status":
            delivery_id = msg.get("delivery_id")
            reconcile_semantic_delivery()
            client_sock.sendall((json.dumps(semantic_reply(delivery_id)) + "\n").encode())
        elif op == "send":
            # Old unkeyed PTY send could bypass the native-effect fence and
            # duplicate a lost original. Refuse before any native queue/write.
            client_sock.sendall((json.dumps({
                "ok": False,
                "error": "legacy unkeyed send is forbidden; require original full-UUID delivery receipt",
                "state": state["current"],
            }) + "\n").encode())
            return
        elif op == "compact":
            timeout = float(msg.get("timeout") or COMPACT_WAIT_DEFAULT_S)
            operation_id = msg.get("operation_id")
            if (not isinstance(operation_id, str) or not operation_id
                    or len(operation_id) > 128 or "\x00" in operation_id):
                client_sock.sendall((json.dumps({
                    "ok": False,
                    "error": "invalid compact operation_id",
                    "state": state["current"],
                }) + "\n").encode())
                return
            # Parallel to deliver's original effect gate, not a second
            # authorization framework. A handshaken socket or journal alone
            # never permits native /compact to touch the PTY.
            try:
                handle = f"claude:{short}"
                current = store.record(handle)
                native = current.get("native")
                native_session_id = (
                    native.get("sessionId") if isinstance(native, dict) else None
                )
                if (not isinstance(native_session_id, str)
                        or native_session_id != session_id):
                    raise ValueError("original Claude native session differs from host")
                effect = store.read_receipt(handle, "compact", operation_id)
                if not effect or effect.get("state") not in (
                    "submitted-unknown", "accepted", "rejected",
                ):
                    raise ValueError("original full-UUID compact effect fence absent")
                wanted = compact_intent_digest(
                    short, generation=store.generation,
                    native_session_id=native_session_id,
                    operation_id=operation_id,
                )
                if (effect.get("intentSha256") != wanted
                        or effect.get("action") != "compact"
                        or effect.get("family") != "compact"
                        or effect.get("operationId") != operation_id
                        or effect.get("workerId") != short
                        or effect.get("engine") != "claude"
                        or effect.get("generation") != store.generation):
                    raise ValueError("original compact intent/owner receipt differs")
                identity = {
                    "workerId": short, "engine": "claude",
                    "generation": store.generation,
                    "nativeSessionId": native_session_id,
                    "operationId": operation_id,
                }
                # Check the original journal only after the global effect
                # receipt. Terminal native receipts never schedule a new paste.
                prior = get_compact_operation(short, operation_id)
                if effect["state"] in ("accepted", "rejected"):
                    if prior is None:
                        raise ValueError(
                            "terminal native compact receipt lacks original journal"
                        )
                    client_sock.sendall((json.dumps({
                        "ok": effect["state"] == "accepted",
                        "state": state["current"],
                        "operation": public_compact_operation(prior),
                        **identity,
                    }) + "\n").encode())
                    return
                if prior is not None:
                    client_sock.sendall((json.dumps({
                        "ok": True, "state": state["current"],
                        "operation": public_compact_operation(prior),
                        **identity,
                    }) + "\n").encode())
                    return
                store.authorize_native_action(handle, "compact")
            except Exception as exc:
                client_sock.sendall((json.dumps({
                    "ok": False,
                    "error": "full-UUID native compact admission denied: " + str(exc),
                    "state": state["current"],
                }) + "\n").encode())
                return
            if state.get("semantic_delivery") is not None:
                client_sock.sendall((json.dumps({
                    "ok": False,
                    "error": "compact requires no semantic delivery in flight",
                    "state": state["current"],
                }) + "\n").encode())
                return
            if state["current"] != S.IDLE:
                client_sock.sendall((json.dumps({
                    "ok": False,
                    "error": "compact requires an idle session",
                    "state": state["current"],
                }) + "\n").encode())
                return
            if state.get("compacting") is not None:
                client_sock.sendall((json.dumps({
                    "ok": False,
                    "error": "compact already in progress",
                    "state": state["current"],
                }) + "\n").encode())
                return
            # An IDLE worker is not proof that Ink's input surface is free.
            # A durable steer that gave up as
            # `unknown` was written into the editor and never explained: Claude
            # may be holding it as a queued prompt, which can flush into a turn
            # at any moment — including the moment after we paste `/compact`,
            # which is exactly how the command gets stranded un-submitted. The
            # original journal remains authoritative until native evidence appears.
            try:
                unproven = unproven_steer_deliveries(short)
            except Exception:
                unproven = []
            if unproven:
                client_sock.sendall((json.dumps({
                    "ok": False,
                    "error": (
                        "compact requires a provably free input surface; "
                        f"{len(unproven)} steer delivery(ies) were written to "
                        "Ink and never resolved, so a queued prompt may still "
                        "be pending"
                    ),
                    "state": state["current"],
                    "unproven_deliveries": [
                        d.get("delivery_id") for d in unproven
                    ],
                }) + "\n").encode())
                return
            # Defer the actual `/compact` paste to the compact pump (main loop) so it
            # can wait for Ink to be input-ready before writing. Pasting here let a
            # mid-render / mid-subscribe Ink silently swallow the bracketed sequence
            # (the steering-paste flake). The pump also owns submission: the paste
            # goes in without a trailing \r (the slash-command autocomplete menu
            # swallows a same-write Enter) and a separate Enter is pressed and
            # retried until the transcript acks the command. set_state(BUSY) now so
            # no send or second compact races in during the brief gate wait.
            now = time.time()
            try:
                operation, _created = begin_compact_operation(
                    short,
                    operation_id=operation_id,
                    transcript=rec["transcript"],
                    timeout_s=timeout,
                    host_epoch=event_epoch,
                    lock_timeout=COMPACT_LOCK_TIMEOUT_S,
                )
            except RuntimeError as exc:
                client_sock.sendall((json.dumps({
                    "ok": False,
                    "error": str(exc),
                    "state": state["current"],
                }) + "\n").encode())
                return
            # Arm the turn-start latch for this operation only: a flag left
            # over from an earlier compact would hold BUSY for a turn that
            # finished long ago.
            state["compact_turn_started"] = False
            state["compacting"] = {
                "operation_id": operation_id,
                "timeout": timeout,
                "queued_at": now,
                "pasted_at": None,
                "fence_offset": operation.get("fence_offset", 0),
                "deadline": None,
                "last_poll": 0.0,
                # Slash-command submission is a second, separate Enter after the
                # paste (the autocomplete menu swallows a same-write \r). The
                # pump presses Enter at submit_due and re-presses on a cadence
                # until the transcript acks the command as submitted.
                "submit_due": None,
                "submitted": False,
            }
            set_state(S.BUSY)
            broadcast({
                "type": "compact_started",
                "operation_id": operation_id,
                "fence_offset": operation.get("fence_offset", 0),
            })
            compact_context_fence("compacting")
            client_sock.sendall((json.dumps({
                "ok": True,
                "state": S.BUSY,
                "operation": public_compact_operation(operation),
                **identity,
            }) + "\n").encode())
        elif op == "compact-observe":
            # Strict observation: do not reconcile, settle, render or broadcast.
            active = state.get("compacting") or state.get("compact_clear")
            try:
                latest = latest_compact_operation(short)
            except Exception as exc:
                client_sock.sendall((json.dumps({"ok": False,
                    "error": f"compact journal unavailable: {exc}"}) + "\n").encode())
                return
            owner = "held" if active else (
                "unknown" if (latest or {}).get("status") == "unknown"
                or state["current"] in TERMINAL_STATES else "released"
            )
            client_sock.sendall((json.dumps({
                "ok": True, "state": state["current"],
                "input_surface": {"compaction_ownership": owner,
                    "operation_id": (active or {}).get("operation_id"),
                    "runtime_id": event_epoch},
            }) + "\n").encode())
        elif op == "compact-status":
            operation_id = msg.get("operation_id")
            operation = get_compact_operation(short, operation_id)
            if operation is None:
                client_sock.sendall((json.dumps({
                    "ok": False,
                    "error": "compact operation not found",
                    "state": state["current"],
                }) + "\n").encode())
                return
            # This closes every observer race: status first asks the transcript
            # for stronger evidence, then returns the durable operation.
            operation = compact_reconcile(operation_id) or operation
            settle_compact_terminal(operation)
            client_sock.sendall((json.dumps({
                "ok": True,
                "state": state["current"],
                "operation": public_compact_operation(operation),
            }) + "\n").encode())
        elif op == "permission-request":
            data = msg.get("data") or {}
            state["permission_counter"] += 1
            request_id = f"{int(time.time() * 1000)}-{state['permission_counter']}"
            tool_use_id = _hook_tool_use_id(data)
            req = {
                "id": request_id,
                "tool_name": data.get("tool_name"),
                "tool_input": data.get("tool_input", {}),
                "tool_use_id": tool_use_id,
                "requested_at": time.time(),
                "message": data.get("permission_message"),
                "raw": data,
            }
            enqueue_or_publish_gate(req)
            client_sock.sendall((json.dumps({"ok": True, "request_id": request_id}) + "\n").encode())
        elif op == "elicitation-request":
            # Claude's Elicitation hook: an MCP server needs user input inside
            # the running tool call. Same single gate slot as a permission, so
            # WAITING, waitingFor, wait/notify and every release path apply.
            data = msg.get("data") or {}
            state["permission_counter"] += 1
            request_id = f"{int(time.time() * 1000)}-{state['permission_counter']}"
            # One gate at a time: a request arriving while another is
            # published waits in the FIFO instead of orphaning either relay.
            req = _elicitation_request(data, request_id)
            enqueue_or_publish_gate(req)
            client_sock.sendall((json.dumps({"ok": True, "request_id": request_id}) + "\n").encode())
        elif op == "elicitation-response":
            req = state.get("pending_permission")
            request_id = msg.get("request_id")
            if not req or req.get("kind") != KIND_ELICITATION:
                error = "no pending elicitation" if not req else "pending input is not an MCP elicitation"
                client_sock.sendall((json.dumps({"ok": False, "error": error, "state": state["current"]}) + "\n").encode())
                return
            if request_id and request_id != req.get("id"):
                client_sock.sendall((json.dumps({"ok": False, "error": "request id mismatch", "state": state["current"]}) + "\n").encode())
                return
            action = msg.get("action")
            content = msg.get("content")
            problem = elicitation_response_problem(permission_preview(req), action, content)
            if problem:
                client_sock.sendall((json.dumps({"ok": False, "error": problem, "state": state["current"]}) + "\n").encode())
                return
            resolved = {"action": action, "reason": msg.get("reason") or f"{action} by ccx"}
            if action == "accept" and req.get("mode") != "url":
                resolved["content"] = content or {}
            state["permission_decisions"][req["id"]] = resolved
            event = {"type": "elicitation_resolved", "request_id": req["id"],
                     "server": req.get("server"), "action": action}
            response_id = msg.get("response_id")
            if response_id is not None:
                event["response_id"] = response_id
            broadcast(event)
            resolve_published_gate()
            reply = {"ok": True, "request_id": req["id"], "action": action}
            if response_id is not None:
                reply["response_id"] = response_id
            client_sock.sendall((json.dumps(reply) + "\n").encode())
        elif op == "permission-poll":
            request_id = msg.get("request_id")
            decision = state["permission_decisions"].pop(request_id, None)
            reply = {"ok": True, "request_id": request_id, "decision": decision}
            pending = state.get("pending_permission") or {}
            if decision is None and pending.get("id") != request_id and not any(
                queued.get("id") == request_id for queued in state["permission_queue"]
            ):
                # Not decided, published or queued: this host never registered
                # it (a relay orphaned by a host restart). Say so, so the relay
                # stops polling instead of waiting out the hook timeout.
                reply["unknown"] = True
            client_sock.sendall((json.dumps(reply) + "\n").encode())
        elif op == "permission-decision":
            req = state.get("pending_permission")
            request_id = msg.get("request_id")
            if not req:
                client_sock.sendall((json.dumps({"ok": False, "error": "no pending permission", "state": state["current"]}) + "\n").encode())
                return
            if request_id and request_id != req.get("id"):
                client_sock.sendall((json.dumps({"ok": False, "error": "request id mismatch", "state": state["current"]}) + "\n").encode())
                return
            decision = msg.get("decision")
            if decision not in ("allow", "deny"):
                client_sock.sendall((json.dumps({"ok": False, "error": "decision must be allow or deny", "state": state["current"]}) + "\n").encode())
                return
            if req.get("kind") == KIND_ELICITATION:
                client_sock.sendall((json.dumps({"ok": False, "error": "MCP elicitation requires ccx respond, not approve/reject", "state": state["current"]}) + "\n").encode())
                return
            if req.get("tool_name") == "AskUserQuestion":
                client_sock.sendall((json.dumps({"ok": False, "error": "AskUserQuestion requires ccx answer, not approve/reject", "state": state["current"]}) + "\n").encode())
                return
            resolved = {
                "decision": decision,
                "reason": msg.get("reason") or ("approved by ccx" if decision == "allow" else "rejected by ccx"),
            }
            state["permission_decisions"][req["id"]] = resolved
            event = {"type": "permission_approved" if decision == "allow" else "permission_rejected",
                     "request_id": req["id"], "tool": req.get("tool_name")}
            reply = {"ok": True, "request_id": req["id"], "decision": decision}
            response_id = msg.get("response_id")
            if response_id is not None:
                event["response_id"] = response_id
                reply["response_id"] = response_id
            broadcast(event)
            resolve_published_gate()
            client_sock.sendall((json.dumps(reply) + "\n").encode())
        elif op == "answer-permission":
            req = state.get("pending_permission")
            request_id = msg.get("request_id")
            if not req:
                client_sock.sendall((json.dumps({"ok": False, "error": "no pending permission", "state": state["current"]}) + "\n").encode())
                return
            if request_id and request_id != req.get("id"):
                client_sock.sendall((json.dumps({"ok": False, "error": "request id mismatch", "state": state["current"]}) + "\n").encode())
                return
            if req.get("tool_name") != "AskUserQuestion":
                error = ("pending input is an MCP elicitation; use ccx respond"
                         if req.get("kind") == KIND_ELICITATION
                         else "pending permission is not AskUserQuestion")
                client_sock.sendall((json.dumps({"ok": False, "error": error, "state": state["current"], "tool": req.get("tool_name")}) + "\n").encode())
                return
            answers = msg.get("answers")
            response = msg.get("response")
            response_id = msg.get("response_id")
            annotations = msg.get("annotations")
            if answers is not None and not isinstance(answers, dict):
                client_sock.sendall((json.dumps({"ok": False, "error": "answers must be an object", "state": state["current"]}) + "\n").encode())
                return
            if not answers and not response:
                client_sock.sendall((json.dumps({"ok": False, "error": "answer text is empty", "state": state["current"]}) + "\n").encode())
                return
            updated_input = dict(req.get("tool_input") or {})
            if answers:
                updated_input["answers"] = answers
            if response:
                updated_input["response"] = response
            if isinstance(annotations, dict) and annotations:
                updated_input["annotations"] = annotations
            resolved = {
                "decision": "allow",
                "reason": msg.get("reason") or "answered by ccx",
                "updatedInput": updated_input,
                "userModified": True,
            }
            state["permission_decisions"][req["id"]] = resolved
            answer_event = {
                "type": "question_answered",
                "request_id": req["id"],
                "tool": req.get("tool_name"),
            }
            if response_id is not None:
                answer_event["response_id"] = response_id
            broadcast(answer_event)
            resolve_published_gate()
            answer_reply = {
                "ok": True,
                "request_id": req["id"],
                "decision": "answer",
            }
            if response_id is not None:
                answer_reply["response_id"] = response_id
            client_sock.sendall((json.dumps(answer_reply) + "\n").encode())
        elif op == "permission-timeout":
            # The relay's hook budget elapsed with no decision and it is auto-
            # denying the tool so Claude unblocks. A timeout is not a crash — the
            # worker is healthy, the operator just never answered. Clear the prompt
            # and go BUSY (Claude is processing the denied tool and will Stop →
            # IDLE), mirroring the explicit-reject path and leaving the session
            # fully usable for the next send instead of stranding it in CRASHED.
            req = state.get("pending_permission")
            request_id = msg.get("request_id")
            if req and (not request_id or request_id == req.get("id")):
                if req.get("kind") == KIND_ELICITATION:
                    release_pending_gate("no response within timeout", timeout=True)
                else:
                    release_pending_gate("no approval within timeout", event_type="permission_timeout")
                if not promote_next_gate():
                    set_state(S.BUSY)
            elif request_id and (queued := take_queued_gate(request_id)) is not None:
                # Its relay gave up while still queued behind the published gate.
                if queued.get("kind") == KIND_ELICITATION:
                    release_gate_request(queued, "no response within timeout", timeout=True)
                else:
                    release_gate_request(queued, "no approval within timeout",
                                         event_type="permission_timeout")
            client_sock.sendall((json.dumps({"ok": True}) + "\n").encode())
        elif op == "handshake":
            # Initial protocol frame; authenticated socket peer credentials
            # are then verified by the client on this same connection before
            # the subsequent message may do anything native-mutating.
            from ccx.rpc.transport import handshake_payload
            try:
                store.revalidate()
                expected = handshake_payload(plan)
                if msg.get("identity") != expected:
                    raise ValueError(
                        "wrong root/generation/exact Claude full worker owner"
                    )
                state["handshaken_clients"].add(client_sock)
                result = {
                    "ok": True, "identity": expected,
                    "nativeStartProof": (
                        (read_record(short).get("lifecycle") or {})
                        .get("nativeStartProof")
                    ),
                }
            except Exception as exc:
                result = {"ok": False, "error": str(exc)}
            client_sock.sendall((json.dumps(result) + "\n").encode())
            return
        elif op == "startup-status":
            # Read-only original hook evidence; never infer readiness solely
            # from the presence of a socket/owner/PID/short path.
            current = read_record(short)
            result = {
                "ok": True,
                "workerId": short,
                "engine": "claude",
                "generation": store.generation,
                "nativeStartProof": (
                    (current.get("lifecycle") or {}).get("nativeStartProof")
                ),
            }
            client_sock.sendall((json.dumps(result) + "\n").encode())
            return
        elif op == "status":
            r = read_record(short) or {}
            # Durable steers that are journaled but not yet resolved. Bounded
            # projections only — identity and evidence, never the full prompt.
            steer_journal_error = None
            try:
                pending_steers = [public_steer_delivery(d)
                                  for d in pending_steer_deliveries(short)]
            except Exception as exc:
                pending_steers = []
                steer_journal_error = str(exc)
            compact_journal_error = None
            try:
                compact_operation = public_compact_operation(
                    get_compact_operation(
                        short,
                        (state.get("compacting") or {}).get("operation_id"),
                    )
                )
            except Exception as exc:
                # A corrupt compact sidecar must block new compact operations,
                # but it must not blind ordinary lifecycle/status inspection.
                compact_operation = None
                compact_journal_error = str(exc)
            client_sock.sendall((json.dumps({
                "ok": True,
                "state": state["current"],
                "since": state["since"],
                "turns": state["turns"],
                "activeTurnRef": state.get("active_turn_ref"),
                "lastTurnAt": r.get("lastTurnAt"),
                "pendingDeliveries": pending_steers,
                "steerJournalError": steer_journal_error,
                "waitingFor": current_waiting_preview(),
                # Brokered requests waiting behind `waitingFor`, oldest first.
                "queuedInputs": len(state["permission_queue"]),
                "attached": hub.attached,
                "attachedTerminals": len(hub.viewers),
                # Is the local /compact pump still running? The pump is bounded
                # by its own deadline and can give up without writing a
                # boundary, so a client waiting for one needs to know when to
                # stop waiting instead of polling forever.
                "compacting": state.get("compacting") is not None,
                "compactOperation": compact_operation,
                "compactJournalError": compact_journal_error,
            }) + "\n").encode())
        elif op == "subscribe":
            state["subscribers"].add(client_sock)
            client_sock.sendall((json.dumps({"ok": True, "subscribed": True, "state": state["current"]}) + "\n").encode())
        elif op == "interrupt":
            expected_turn_ref = msg.get("expected_turn_ref")
            if (
                expected_turn_ref is not None
                and expected_turn_ref != state.get("active_turn_ref")
            ):
                client_sock.sendall((json.dumps({
                    "ok": False,
                    "definitive": True,
                    "error": "active turn changed",
                    "state": state["current"],
                    "activeTurnRef": state.get("active_turn_ref"),
                }) + "\n").encode())
                return
            existing_obs = state.get("interrupt_observation")
            current_ref = state.get("active_turn_ref") or state.get("settling_turn_ref")
            if (
                existing_obs is not None
                and existing_obs.get("turn_ref") == current_ref
                and existing_obs.get("interrupt_at") is not None
                and (existing_obs.get("settling") or existing_obs.get("inactive_status"))
            ):
                client_sock.sendall((json.dumps({
                    "ok": True, "sent": False, "already_observing": True,
                    "state": state["current"], "turnRef": current_ref,
                }) + "\n").encode())
                return
            if state.get("semantic_delivery") is not None:
                client_sock.sendall((json.dumps({
                    "ok": False,
                    "definitive": True,
                    "error": (
                        "queued steer is still pending; ccx kill <worker> discards "
                        "the queue after checking the transcript (delivered or abandoned)"
                        if (state["semantic_delivery"] or {}).get("route") == "steer"
                        else "semantic delivery in flight; retry after it settles"
                    ),
                    "state": state["current"],
                }) + "\n").encode())
                return
            # Best-effort turn interrupt: deliver the Escape keypress the TUI
            # binds to chat:cancel. A turn blocked in the managed permission /
            # AskUserQuestion broker is still the same active turn. Validate the
            # pending gate now, but mutate it only after Escape is actually
            # written so a PTY write failure remains a true pre-mutation refusal.
            waiting_req = None
            if state["current"] == S.WAITING:
                waiting_req = state.get("pending_permission")
                if not isinstance(waiting_req, dict) or not waiting_req.get("id"):
                    client_sock.sendall((json.dumps({
                        "ok": False,
                        "definitive": True,
                        "error": "waiting turn has no cancellable pending input",
                        "state": state["current"],
                    }) + "\n").encode())
                    return
            elif state["current"] != S.BUSY:
                client_sock.sendall((json.dumps({
                    "ok": False,
                    "definitive": True,
                    "error": f"nothing to interrupt (state={state['current']})",
                    "state": state["current"],
                }) + "\n").encode())
                return
            target_ref = state.get("active_turn_ref")
            # Keep the exact target observation even while subagent-tainted:
            # native BUSY is ambiguous but native IDLE/SHELL proves that all
            # original-session work has ended, regardless of taint.
            can_native_observe = bool(
                target_ref and state.get("compacting") is None
            )
            if not write_escape():
                client_sock.sendall((json.dumps({
                    "ok": False,
                    "definitive": True,
                    "error": "PTY write failed (worker dead?)",
                    "state": state["current"],
                }) + "\n").encode())
                return
            if can_native_observe:
                obs = state.get("interrupt_observation")
                if obs is None or obs.get("turn_ref") != target_ref:
                    state["interrupt_observation"] = {
                        "turn_ref": target_ref,
                        "settling": False,
                    }
            if waiting_req is not None:
                release_pending_gate("turn cancelled by ccx", cancelled=True)
                release_queued_gates("turn cancelled by ccx", cancelled=True)
                set_state(S.BUSY)
            obs = state.get("interrupt_observation")
            if obs is not None and obs.get("turn_ref") == state.get("active_turn_ref"):
                obs["interrupt_at"] = time.time()
                obs["interrupt_at_ms"] = int(obs["interrupt_at"] * 1000)
                obs["passive"] = False
                obs["settling"] = False
                obs.pop("inactive_status", None)
                obs.pop("inactive_status_updated_at", None)
                obs.pop("settling_at", None)
            probe = state.get("interrupt_probe")
            try:
                tsize = os.path.getsize(rec["transcript"])
            except OSError:
                tsize = 0
            if probe:
                # Repeated press: refresh the deadline, keep the earliest
                # scan position so the marker can't be missed between presses.
                probe["at"] = time.time()
            else:
                state["interrupt_probe"] = {
                    "at": time.time(),
                    "scan_pos": tsize,
                    "turn_ref": state.get("active_turn_ref"),
                }
            state["last_activity"] = time.time()
            interrupt_event = {
                "type": "interrupt_sent",
                "turn_ref": state.get("active_turn_ref"),
            }
            broadcast(interrupt_event)
            client_sock.sendall((json.dumps({
                "ok": True,
                "sent": True,
                "state": state["current"],
                "turnRef": state.get("active_turn_ref"),
            }) + "\n").encode())
        elif op == "resize":
            if hub.attached:
                client_sock.sendall(b'{"ok":false,"error":"PTY size owned by attached terminal"}\n')
                return
            try:
                set_winsize(master_fd, int(msg.get("rows", DEFAULT_PTY_ROWS)), int(msg.get("cols", DEFAULT_PTY_COLS)))
                client_sock.sendall((json.dumps({"ok": True}) + "\n").encode())
            except Exception as e:
                client_sock.sendall((json.dumps({"ok": False, "error": str(e)}) + "\n").encode())
        elif op == "kill":
            client_sock.sendall((json.dumps({"ok": True, "killed": True}) + "\n").encode())
            try:
                client_sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            state["killed_explicit"] = True
            sigterm_pending[0] = True
        elif op == "hook-event":
            event = msg.get("event")
            data = msg.get("data") or {}
            # New-v2 worker native hooks must originate from the original
            # Claude process (or a living descendant relay) and, for boot,
            # name the exact independently minted provider session. Same UID
            # alone never proves native session ownership. Provider callbacks
            # can mutate records/settle receipts, so check before on_hook_event.
            if rec.get("fingerprintVersion") == 2 and event in (
                "SessionStart", "UserPromptSubmit", "Stop",
            ):
                try:
                    from ccx.store.liveness import proc_start_token
                    credentials = oslayer.peer_credentials(client_sock)
                    actual_pid, actual_uid = credentials.pid, credentials.uid
                    native_state = read_record(short).get("lifecycle") or {}
                    native_pid = native_state.get("workerPid")
                    native_start = native_state.get("workerStart")
                    if (actual_uid != os.getuid()
                            or not isinstance(actual_pid, int)
                            or not _native_hook_descendant(
                                actual_pid, native_pid, native_start,
                            )):
                        raise PermissionError(
                            "native hook peer is not this original worker's "
                            "PID/start-bound descendant"
                        )
                    if event == "SessionStart":
                        if data.get("session_id") != session_id:
                            raise PermissionError(
                                "native SessionStart provider sessionId "
                                "differs from immutable original binding"
                            )
                        prior_proof = native_state.get("nativeStartProof")
                        from ccx.store.store import _digest
                        original = store.native_activation(short)
                        source = f"claude-session:{session_id}"
                        current_host = native_state.get("hostPid")
                        current_host_start = native_state.get("hostStart")
                        # An offline-converted worker's confirmed activation
                        # has no submissionId by construction; every lifetime
                        # under this generation is a resume of that confirmed
                        # history, re-verified by the store's converted-binding
                        # gate. Later resumes still authenticate the prior proof.
                        converted_resume = (
                            original.get("state") == "confirmed"
                            and original.get("submissionId") is None
                            and native_state.get("state") == S.RESUMING
                            and data.get("source") in (None, "resume"))
                        if converted_resume:
                            store.authorize_native_action(f"claude:{short}", "resume")
                        if (original.get("source") != source
                                or (not original.get("submissionId")
                                    and not converted_resume)
                                or current_host != os.getpid()
                                or not current_host_start
                                or str(proc_start_token(current_host)) != str(current_host_start)):
                            raise PermissionError("SessionStart has no original/current host source proof")
                        first = prior_proof is None
                        if not first:
                            if (not isinstance(prior_proof, dict)
                                    or prior_proof.get("sessionId") != session_id
                                    or prior_proof.get("nativeSource") != source
                                    or prior_proof.get("submissionId") != original["submissionId"]
                                    or prior_proof.get("generation") != store.generation
                                    or prior_proof.get("nativeEvidenceSha256") != _digest({
                                        key: value for key, value in prior_proof.items()
                                        if key != "nativeEvidenceSha256"
                                    })):
                                raise PermissionError(
                                    "prior native SessionStart source/claim/proof was not authentic"
                                )
                        old_lifetime = (isinstance(prior_proof, dict)
                                        and (prior_proof.get("workerPid") != native_pid
                                             or str(prior_proof.get("workerStart")) != str(native_start)
                                             or prior_proof.get("hostPid") != current_host
                                             or str(prior_proof.get("hostStart")) != str(current_host_start)))
                        resumed_lifetime = (old_lifetime
                            and original.get("state") == "confirmed"
                            and native_state.get("state") == S.RESUMING
                            and data.get("source") in (None, "resume"))
                        if (first and original.get("state") != "submitted-unknown"
                                and not converted_resume):
                            raise PermissionError("SessionStart lacks original submitted native activation")
                        if not first and not resumed_lifetime and old_lifetime:
                            raise PermissionError("native SessionStart contradicts prior proof")
                        if not first and not resumed_lifetime and not isinstance(prior_proof, dict):
                            raise PermissionError("native SessionStart has malformed prior proof")
                        if first or resumed_lifetime:
                            proof = {
                                "workerId": short,
                                "engine": "claude",
                                "generation": store.generation,
                                "nativeSource": source,
                                "sessionId": session_id,
                                "submissionId": original["submissionId"],
                                "hostPid": current_host,
                                "hostStart": str(current_host_start),
                                "workerPid": native_pid,
                                "workerStart": str(native_start),
                                "peerPid": native_pid,
                                "peerStart": str(native_start),
                                "relayPeerPid": actual_pid,
                                "relayPeerStart": proc_start_token(actual_pid),
                            }
                            if not proof["relayPeerStart"]:
                                raise PermissionError("native provider relay peer start time unavailable")
                            proof["nativeEvidenceSha256"] = _digest(proof)
                            if first:
                                store.patch_lifecycle(
                                    f"claude:{short}", changes={"nativeStartProof": proof})
                            else:
                                store.mutate_record(
                                    f"claude:{short}",
                                    patches={"lifecycle.nativeStartProof": proof},
                                    expect={
                                        "lifecycle.nativeStartProof": prior_proof,
                                        "lifecycle.state": S.RESUMING,
                                        "lifecycle.hostPid": current_host,
                                        "lifecycle.hostStart": current_host_start,
                                        "lifecycle.workerPid": native_pid,
                                        "lifecycle.workerStart": native_start,
                                    },
                                )

                except Exception as exc:
                    client_sock.sendall((json.dumps({
                        "ok": False,
                        "error": "native hook owner/source attestation failed: "
                        + str(exc),
                    }) + "\n").encode())
                    return
            on_hook_event(event, data)
            if event in ("PostToolUse", "PostToolUseFailure", "SubagentStop"):
                # Existing boundary machinery is unchanged for success. Failed
                # tools still require a same-stream hook acknowledgement.
                reply = (subagent_boundary_reply(data)
                         if event != "PostToolUseFailure" else None)
                if reply is None and event != "PostToolUseFailure":
                    reply = boundary_deliver_semantic(data)
                if reply is None:
                    reply = {"ok": True, "delivered": False}
                client_sock.sendall((json.dumps(reply) + "\n").encode())
            else:
                # The event is handled before this same-stream acknowledgement;
                # owner-sensitive hooks have already proved PID/start ancestry.
                # Every relay waits for it: Darwin can drop a just-closed
                # connection before the host reads it.
                client_sock.sendall(b'{"ok":true}\n')
        else:
            client_sock.sendall((json.dumps({"ok": False, "error": f"unknown op {op!r}"}) + "\n").encode())

    def drain_ready_ingress(max_frames: int = 64) -> int:
        """Boundedly accept/read already-ready host RPC/hook traffic.

        Used only while a liveness target is settling so an already-queued Stop
        can upgrade the outcome before ``turn_settled`` is committed. It never
        waits: readiness is sampled with timeout=0 and ordinary handler semantics
        are reused for every complete frame.
        """
        frames = 0
        progress = True
        while progress and frames < max_frames:
            progress = False
            while frames < max_frames:
                try:
                    conn = accept_same_user(server)
                except BlockingIOError:
                    break
                except OSError:
                    break
                if conn is None:
                    continue
                conn.setblocking(False)
                state["clients"][conn.fileno()] = (conn, bytearray())
                progress = True
            fds = list(state["clients"].keys())
            if not fds:
                break
            try:
                ready, _, _ = select.select(fds, [], [], 0)
            except (OSError, ValueError):
                break
            for fd in ready:
                conn, buf = state["clients"].get(fd, (None, None))
                if conn is None:
                    continue
                try:
                    data = conn.recv(4096)
                except BlockingIOError:
                    continue
                except OSError:
                    data = b""
                if not data:
                    drop_client(conn)
                    progress = True
                    continue
                progress = True
                buf.extend(data)
                if (hub.has(conn) and len(buf) > MAX_ATTACH_LINE
                        and b"\n" not in buf):
                    hub.release(conn, "oversized-attachment-frame")
                    drop_client(conn)
                    continue
                while b"\n" in buf and frames < max_frames:
                    if fd not in state["clients"]:
                        break
                    line, rest = buf.split(b"\n", 1)
                    del buf[:]
                    buf.extend(rest)
                    if not line.strip():
                        continue
                    if hub.has(conn) and len(line) > MAX_ATTACH_LINE:
                        hub.release(conn, "oversized-attachment-frame")
                        drop_client(conn)
                        break
                    try:
                        msg = json.loads(line)
                    except (UnicodeError, ValueError):
                        if hub.has(conn):
                            hub.release(conn, "malformed-attachment-frame")
                            drop_client(conn)
                            break
                        continue
                    if not isinstance(msg, dict):
                        if hub.has(conn):
                            hub.release(conn, "malformed-attachment-frame")
                            drop_client(conn)
                            break
                        continue
                    try:
                        handle_client_msg(conn, msg)
                    except Exception as exc:
                        try:
                            conn.sendall((json.dumps({"ok": False, "error": f"handler: {exc}"}) + "\n").encode())
                        except OSError:
                            pass
                    frames += 1
        return frames

    # Off-loop usage-cache refresher. fetch_usage() can block up to ~8s on the
    # network; running it inline in the select loop
    # stalls PTY/RPC servicing for that whole window. So the budget check reads
    # cache-only and this daemon thread does the network refresh out of band.
    # It only fetches while a cap is armed and the worker is consuming
    # (BUSY/BOOTING) — the same gate the inline check uses — so an idle or
    # uncapped host never touches the network. fetch_usage() (no force) honours
    # the shared-cache throttle, so N workers + the statusline still amortise to
    # roughly one network call per CAP_POLL_INTERVAL_S; the cache is written
    # atomically (os.replace), so a concurrent writer is safe. daemon=True so it
    # dies with the process; the Event lets shutdown wake it out of its sleep.
    usage_refresh_stop = threading.Event()
    def _usage_refresher():
        while not usage_refresh_stop.is_set():
            if state["active_cap"] and state["current"] in (S.BUSY, S.BOOTING):
                try:
                    fetch_usage()  # refreshes the shared cache when it's stale
                except Exception:
                    pass  # fail-open: the inline cache-only read handles gaps
            usage_refresh_stop.wait(CAP_POLL_INTERVAL_S)
    threading.Thread(target=_usage_refresher, name="ccx-usage-refresh",
                     daemon=True).start()

    # Keep the context/model observation refresh off the host's event loop.
    refresh_wanted = threading.Event()
    refresh_stop = threading.Event()

    def request_refresh() -> None:
        refresh_wanted.set()

    def _transcript_refresher():
        while True:
            refresh_wanted.wait()
            refresh_wanted.clear()
            try:
                refresh_context_observation(short)
            except Exception:
                pass
            if refresh_stop.is_set() and not refresh_wanted.is_set():
                return

    refresher = threading.Thread(target=_transcript_refresher,
                                 name="ccx-transcript-refresh", daemon=True)
    refresher.start()

    # Main loop
    while True:
        try:
            if sigterm_pending[0]:
                break
            if child_exited[0]:
                break
            if pty_gone[0]:
                break
            # CRASHED is terminal — host can't deliver further work. Tear down so the
            # sock is unlinked and the next `ccx send` starts a clean respawn instead
            # of fighting a zombie host. (Set by the pending-send pump when delivery
            # ack never arrives, or by PTY write failures.)
            if state["current"] == S.CRASHED:
                break

            poll_background()

            # Claude Code can emit a synthetic terminal API-error assistant message
            # and a turn_duration event without firing the Stop hook. Hooks remain
            # the primary lifecycle signal; this raw-transcript tail is only the
            # fallback for that missing-Stop path.
            if state["current"] in (S.BUSY, S.BOOTING):
                api_error, new_pos = _read_new_api_error(
                    rec["transcript"],
                    int(state.get("api_error_scan_pos") or 0),
                )
                state["api_error_scan_pos"] = new_pos
                if api_error:
                    request_refresh()
                    reason = api_error_reason(api_error)
                    # Persist the classification, not just the sentence. The state
                    # stays CRASHED — the worker really is gone either way — but a
                    # library caller needs to tell "Anthropic capacity, come back
                    # later" from "your credentials are wrong" from "rate limited
                    # until the reset" without matching on prose.
                    merge_lifecycle(short, apiError={
                        "status": api_error.get("status"),
                        "kind": api_error.get("error"),
                        "transient": api_error.get("transient"),
                        "rate_limited": api_error.get("rate_limited"),
                        "reset_text": api_error.get("reset_text"),
                        "request_id": api_error.get("request_id"),
                        "message": api_error.get("text"),
                    })
                    set_state(S.CRASHED, reason=reason)
                    broadcast({
                        "type": "api_error",
                        "status": api_error.get("status"),
                        "error": api_error.get("error"),
                        "request_id": api_error.get("request_id"),
                        "transient": api_error.get("transient"),
                        "rate_limited": api_error.get("rate_limited"),
                        "reset_text": api_error.get("reset_text"),
                        "reason": reason,
                    })
                    break

            # Passive parent interrupt observation: a human Escape sends no
            # Stop hook. Never infer it from terminal bytes or a native idle
            # sample, including for a turn whose broker is WAITING.
            passive = state.get("passive_interrupt")
            if (passive and passive.get("turn_ref") == state.get("active_turn_ref")
                    and state["current"] in (S.BUSY, S.WAITING)):
                prompt_ids = set(state["turn_prompt_ids"])
                found, passive["scan_pos"] = _read_interrupt_marker(
                    rec["transcript"], passive["scan_pos"], prompt_ids=prompt_ids,
                    strict=True)
                if found:
                    ref = passive["turn_ref"]
                    if state["current"] == S.WAITING:
                        release_pending_gate("turn cancelled natively", cancelled=True)
                        release_queued_gates("turn cancelled natively", cancelled=True)
                    finalize_managed_turn(ref, outcome="interrupted", interrupted=True)

            # Cancellation reconciliation. Exact semantic evidence
            # always wins. Native liveness is a fallback only after a fresh target
            # busy token was observed; elapsed time alone never proves terminality.
            probe = state.get("interrupt_probe")
            obs = state.get("interrupt_observation")
            if probe is not None:
                if state["current"] == S.WAITING and state.get("active_turn_ref") == probe.get("turn_ref"):
                    note_interrupt_unconfirmed("turn returned to waiting after interrupt")
                elif state["current"] == S.BUSY:
                    found, new_pos = _read_interrupt_marker(
                        rec["transcript"], int(probe.get("scan_pos") or 0))
                    probe["scan_pos"] = new_pos
                    if found:
                        turn_ref = probe.get("turn_ref") or state.get("active_turn_ref")
                        finalize_managed_turn(turn_ref, outcome="interrupted", interrupted=True)
                        probe = None
                        obs = None
                    elif time.time() - probe["at"] > INTERRUPT_CONFIRM_S:
                        # Honest verdict without native evidence. Retain the
                        # exact observation and marker scan beyond 12s: any
                        # later owned IDLE newer than ESC can still settle it.
                        if not probe.get("unconfirmed"):
                            probe["unconfirmed"] = True
                            broadcast({"type": "interrupt_unconfirmed",
                                       "turn_ref": probe.get("turn_ref")})
                        if obs is None or obs.get("turn_ref") != probe.get("turn_ref"):
                            state["interrupt_probe"] = None
                            probe = None

            obs = state.get("interrupt_observation")
            if obs is not None:
                value = native_snapshot()
                if (obs.get("passive") and obs.get("settling")
                        and not native_candidate_still_valid(obs, value)):
                    # Passive fallback requires continuous, verified native
                    # idle. Explicit stop is different: once original A was
                    # seen inactive, a fresh busy B may supersede its native
                    # sample before B's UPS arrives. Keep A's inactive token
                    # and settling ref for successor attribution.
                    reset_inactive_candidate(obs)
                if not obs.get("settling"):
                    native_inactive_after_escape(value)
                if obs.get("settling"):
                    # Give queued Stop/marker hooks a chance to take priority.
                    drain_ready_ingress()
                    obs = state.get("interrupt_observation")
                    # An ingress hook may reset A's idle candidate or start B.
                    # No grace/evidence is a defer, not permission to skip the
                    # rest of the host loop (PTY, attach and semantic pumps).
                    grace = (PASSIVE_NATIVE_IDLE_GRACE_S if (obs or {}).get("passive")
                             else SEMANTIC_EVIDENCE_GRACE_S)
                    if settling_grace_elapsed(
                            state, obs, now=time.monotonic(), grace_s=grace):
                        # Recheck the owned native session after draining Stop.
                        # Explicit stop may have seen A idle before native B
                        # went busy; its observed inactive edge survives.
                        value = native_snapshot()
                        successor, successor_token = successor_attribution(
                            obs.get("inactive_status_updated_at"), value)
                        eligible = (native_candidate_still_valid(obs, value)
                                    or (not obs.get("passive") and successor))
                        if not eligible and obs.get("passive"):
                            reset_inactive_candidate(obs)
                        if eligible:
                            # An exact interruption marker wins. Other evidence
                            # keeps the same turn, token and grace constraints.
                            marker_settled = False
                            probe = state.get("interrupt_probe")
                            if probe is not None:
                                found, probe["scan_pos"] = _read_interrupt_marker(
                                    rec["transcript"], int(probe.get("scan_pos") or 0))
                                if found:
                                    finalize_managed_turn(
                                        obs.get("turn_ref"), outcome="interrupted", interrupted=True)
                                    marker_settled = True
                            if not marker_settled:
                                passive = state.get("passive_interrupt")
                                if (passive is not None
                                        and passive.get("turn_ref") == obs.get("turn_ref")):
                                    found, passive["scan_pos"] = _read_interrupt_marker(
                                        rec["transcript"], passive["scan_pos"],
                                        prompt_ids=set(state["turn_prompt_ids"]), strict=True)
                                    if found:
                                        finalize_managed_turn(
                                            obs.get("turn_ref"), outcome="interrupted", interrupted=True)
                                        marker_settled = True
                            if not marker_settled:
                                if state["current"] == S.WAITING:
                                    release_pending_gate(
                                        "turn cancelled on native idle", cancelled=True)
                                    release_queued_gates(
                                        "turn cancelled on native idle", cancelled=True)
                                finalize_managed_turn(
                                    obs.get("turn_ref"), outcome="interrupted", interrupted=True,
                                    native_status=obs.get("inactive_status"),
                                    inactive_native_token=obs.get("inactive_status_updated_at"),
                                    successor_running=successor,
                                    successor_native_token=successor_token,
                                )

            # Subagent taint clears only on fresh native quiescence, which nothing
            # else samples while no interrupt is in flight.
            if state["subagent_taint"] and time.time() - state["subagent_taint_sampled_at"] >= 1.0:
                state["subagent_taint_sampled_at"] = time.time()
                native_snapshot()

            # Release a pending successor only after late-hook discrimination.
            guard = state.get("late_hook_guard")
            if guard is not None:
                if (time.time() >= float(guard.get("expires_at") or 0.0)
                        and time.time() >= float(guard.get("prompt_until") or 0.0)):
                    state["late_hook_guard"] = None
                elif state["subagent_taint"]:
                    pass  # a subagent's busy must not count as post-settle parent work
                else:
                    value = native_snapshot()
                    inactive_token = int(guard.get("native_inactive_status_updated_at") or 0)
                    if (
                        value is not None and value.status == "busy"
                        and value.status_updated_at is not None
                        and value.status_updated_at > inactive_token
                    ):
                        guard["post_settle_busy_seen"] = True

            pending_successor = state.get("successor_pending")
            if pending_successor is not None and not state.get("active_turn_ref"):
                value = native_snapshot()
                if successor_hold_should_release(
                    pending_successor, state.get("active_turn_ref"), value.status if value is not None else None
                ) and successor_hold_quiescence_is_fresh(
                    pending_successor, value, state["subagent_taint"]
                ):
                    state["successor_pending"] = None
                    set_state(S.IDLE)
                    broadcast({
                        "type": "successor_hold_released",
                        "reason": "native foreground became inactive before successor attribution",
                        "subagent_ambiguous": bool(pending_successor.get("subagent_ambiguous")),
                    })

            # Bounded release for a post-compact BUSY hold. The hold is an
            # inference from the transcript, not a lifecycle event of its own, so
            # it must not be able to pin the worker forever if that inference was
            # wrong. A live turn keeps Ink rendering and fires tool hooks; combined
            # silence across both for the grace window means no turn is running.
            hold = state.get("post_compact_hold")
            if hold is not None:
                if state["current"] != S.BUSY:
                    state["post_compact_hold"] = None
                else:
                    if post_compact_hold_may_release(
                            time.time(), state.get("last_activity", 0.0),
                            state.get("last_pty_read", 0.0),
                            bool(state["subagent_taint"]), POST_COMPACT_HOLD_GRACE_S):
                        state["post_compact_hold"] = None
                        set_state(S.IDLE)
                        broadcast({
                            "type": "compact_hold_released",
                            "operation_id": hold.get("operation_id"),
                            "reason": "no turn activity after the compact boundary",
                        })

            compacting = state.get("compacting")
            if compacting is not None:
                now = time.time()
                operation_id = compacting["operation_id"]
                if compacting["pasted_at"] is None:
                    # Gated first paste. Wait until Ink is input-ready (the same two
                    # signals the send pump uses for its first write) so the bracketed
                    # `/compact` isn't dropped into a mid-render editor. Force it through
                    # if the PTY never quiets, rather than wedging BUSY forever with
                    # nothing pasted. The result-since window and deadline are anchored
                    # to the actual paste, not the (possibly earlier) RPC.
                    ink_settled = now >= state.get("earliest_send_at", 0.0)
                    last_pty = pty_read_for_input_ready(now)
                    pty_quiet = last_pty > 0 and (now - last_pty) >= PTY_QUIESCENCE_S
                    force = now - compacting["queued_at"] > SEND_RETRY_ACK_S
                    if (ink_settled and pty_quiet) or force:
                        fence_offset = transcript_offset(rec["transcript"])
                        fence_dev, fence_inode = transcript_identity(rec["transcript"])
                        # Persist intent and the exact append fence before touching
                        # the PTY.  If the host dies during the write, recovery can
                        # still distinguish prior boundaries from this operation.
                        # Not journaled (busy lock, or already settled elsewhere)
                        # means not written: a later tick tries again.
                        pasted = compact_transition(
                            operation_id,
                            "pasted",
                            fence_offset=fence_offset,
                            fence_dev=fence_dev,
                            fence_inode=fence_inode,
                        )
                        if pasted is None:
                            pass
                        # Paste without the trailing \r: a pasted slash command opens
                        # Ink's autocomplete menu, which swallows a same-write Enter
                        # (live-verified on 2.1.207 — the command sat in the editor
                        # forever and the compact timed out). Submission is a separate
                        # delayed Enter below, retried until the transcript acks it.
                        elif write_paste("/compact", submit=False):
                            compacting["fence_offset"] = fence_offset
                            compacting["pasted_at"] = now
                            compacting["deadline"] = now + compacting["timeout"]
                            compacting["submit_due"] = now + COMPACT_SUBMIT_DELAY_S
                            state["last_activity"] = now
                        else:
                            compact_transition(
                                operation_id,
                                "unknown",
                                reason="PTY write failed while pasting compact command",
                            )
                            state["compacting"] = None
                            set_state(S.CRASHED, reason="PTY write failed (worker dead?)")
                            broadcast({
                                "type": "compact_unknown",
                                "operation_id": operation_id,
                                "reason": "pty write failed",
                            })
                            break
                elif now - compacting.get("last_poll", 0.0) >= COMPACT_POLL_S:
                    compacting["last_poll"] = now
                    operation = compact_reconcile(operation_id)
                    if operation and operation.get("status") in {"succeeded", "noop",
                                                                 "failed"}:
                        # The terminal result is durable before the active marker is
                        # cleared.  A client can therefore never observe
                        # compacting=false without also being able to retrieve it.
                        settle_compact_terminal(operation)
                        continue
                    if not compacting["submitted"]:
                        if compact_command_seen_since(
                            rec["transcript"],
                            after_offset=int(compacting.get("fence_offset") or 0),
                        ):
                            if compact_transition(operation_id, "submitted",
                                                  submitted=True) is None:
                                continue  # retried next poll, or settled meanwhile
                            compacting["submitted"] = True
                            broadcast({
                                "type": "compact_submitted",
                                "operation_id": operation_id,
                            })
                        elif now >= compacting["submit_due"]:
                            if not write_enter():
                                compact_transition(
                                    operation_id,
                                    "unknown",
                                    reason="PTY write failed while submitting compact command",
                                )
                                state["compacting"] = None
                                set_state(S.CRASHED, reason="PTY write failed (worker dead?)")
                                broadcast({
                                    "type": "compact_unknown",
                                    "operation_id": operation_id,
                                    "reason": "pty write failed",
                                })
                                break
                            compacting["submit_due"] = now + COMPACT_SUBMIT_RETRY_S
                            state["last_activity"] = now
                    if now >= compacting.get("deadline", now):
                        if not compacting["submitted"]:
                            # Did an ordinary turn take the surface while we waited?
                            # The hook-side discriminator normally catches this the
                            # moment it happens; re-checking the transcript covers
                            # the host that missed the hook (relay hiccup, restart
                            # mid-operation) and reaches the same verdict from the
                            # file. Positive proof, so the operation can fail
                            # honestly and the healthy worker keeps running.
                            try:
                                displaced = foreign_prompt_since(
                                    rec["transcript"],
                                    after_offset=int(compacting.get("fence_offset") or 0),
                                )
                            except Exception:
                                displaced = False
                            if displaced:
                                abort_compact(
                                    "input surface taken by a concurrent user turn "
                                    "before /compact was submitted",
                                    evidence="transcript-foreign-prompt",
                                    settle_idle=True,
                                )
                                continue
                            state["compacting"] = None
                            # No evidence either way.  Enter was attempted
                            # repeatedly and absence of an ack is not proof the
                            # native command never started (the transcript shape may
                            # have drifted), so do not clear the editor or release
                            # the input surface as if failure were certain.
                            compact_transition(
                                operation_id,
                                "unknown",
                                reason="compact submission was not acknowledged",
                            )
                            set_state(
                                S.CRASHED,
                                reason="compact submission was not acknowledged",
                            )
                            broadcast({
                                "type": "compact_unknown",
                                "operation_id": operation_id,
                                "reason": "submission not acknowledged",
                            })
                            break
                        # Submission was acknowledged, so absence of a boundary at
                        # our deadline is not proof of native failure: the command
                        # is running and owns the surface. Make the ambiguity
                        # durable before taking the host down.
                        state["compacting"] = None
                        compact_transition(
                            operation_id,
                            "unknown",
                            reason="compact deadline elapsed after submission",
                        )
                        set_state(S.CRASHED, reason="compact did not complete before timeout")
                        broadcast({
                            "type": "compact_unknown",
                            "operation_id": operation_id,
                            "reason": "timeout after submission",
                        })
                        break

            # Compact residue clear. An aborted compact leaves its un-submitted
            # `/compact` in Ink's editor; drain it before anything else may paste.
            # Two separated ticks, like the semantic send residue clear: the unstick
            # marker triggers an async (React) editor update and a Ctrl-U burst in
            # the same write races ahead of it. Semantic writes remain blocked
            # until this finishes (see input_surface_write_blocked).
            clear = state.get("compact_clear")
            if clear is not None and state["current"] == S.IDLE:
                now = time.time()
                last_pty = pty_read_for_input_ready(now)
                if clear["unstuck_at"] is None:
                    if (last_pty > 0 and (now - last_pty) >= PTY_QUIESCENCE_S
                            and now >= state.get("earliest_send_at", 0.0)):
                        write_unstick()
                        clear["unstuck_at"] = now
                        state["last_activity"] = now
                elif now - clear["unstuck_at"] >= POST_CLEAR_SETTLE_S:
                    write_drain()
                    state["compact_clear"] = None
                    state["last_activity"] = now
                    broadcast({"type": "compact_residue_cleared"})

            # Claude can restore an early-cancelled prompt into the editor.
            # Hold semantic prompt writes until that residue is drained.
            clear = state.get("cancel_clear")
            if clear is not None and state["current"] == S.IDLE:
                now = time.time()
                last_pty = pty_read_for_input_ready(now)
                if clear["unstuck_at"] is None:
                    if (last_pty > 0 and (now - last_pty) >= PTY_QUIESCENCE_S
                            and now >= state.get("earliest_send_at", 0.0)):
                        write_unstick()
                        clear["unstuck_at"] = now
                        state["last_activity"] = now
                elif now - clear["unstuck_at"] >= POST_CLEAR_SETTLE_S:
                    write_clear_input()
                    state["cancel_clear"] = None
                    state["last_activity"] = now
                    broadcast({"type": "cancel_residue_cleared", "turn_ref": clear.get("turn_ref")})

            # Flush the most recent statusline nudge once the 5 s window opens.
            pending_context = state["context_changes"].flush()
            if pending_context is not None:
                _broadcast_context(pending_context)

            # Reconcile the one original semantic delivery; only native evidence
            # can advance placement, and reconciliation never re-pastes.
            reconcile_semantic_delivery()
            semantic_idle_pump()

            # A singleton semantic delivery is real work, not user idleness.
            # idle_timeout_s=0 disables the idle-killer entirely.
            if (idle_timeout_s > 0 and state["current"] == S.IDLE
                    and state["semantic_delivery"] is None and not hub.attached):
                idle_for = time.time() - state["last_activity"]
                if idle_for > idle_timeout_s:
                    # A worker may sit IDLE while a `Bash(run_in_background)` process
                    # or Monitor keeps running on its own — `last_activity` is
                    # decoupled from bg tasks, so the killer would otherwise reap a
                    # worker with live child work / live monitor subscriptions.
                    poll_background(force=True)
                    state["bg_idle_running"] = (
                        (bg["reducer"] is None and not bg.get("failed"))
                        or any(item["status"] == "running"
                               for item in bg["items"])
                        or bool(bg["pending"]) or bg["pending_anonymous"]
                    )
                    if state.get("bg_idle_running"):
                        if not state.get("bg_idle_grace"):
                            state["bg_idle_grace"] = True
                            broadcast({"type": "idle_grace_bg", "idle_for_s": idle_for})
                    else:
                        set_state(S.IDLE_KILLED, reason=f"idle for {idle_for:.0f}s > timeout {idle_timeout_s}s")
                        broadcast({"type": "idle_killed", "idle_for_s": idle_for})
                        break

            # Boot watchdog. BOOTING only ends on a SessionStart hook; if Claude
            # blocks before that (un-pre-seeded --add-dir trust dialog, onboarding
            # gate) no hook fires and the idle-killer above never runs. Fail the
            # stalled boot into the existing terminal CRASHED state so ccx list / the
            # dashboard need no new label; the boot-stall reason surfaces via the
            # pending_last_error → lifecycle.lastError plumbing on shutdown.
            _boot_fail = should_fail_boot(state, time.time(), BOOT_TIMEOUT_S)
            if _boot_fail:
                set_state(S.CRASHED, reason=_boot_fail)
                broadcast({"type": "boot_stalled", "reason": _boot_fail})
                break

            # Budget-cap check. Runs whenever the worker is consuming or about to —
            # BUSY (a turn) or BOOTING (a capped revival, which
            # never enters BUSY). Throttled to CAP_POLL_INTERVAL_S. The
            # read is cache-only (read_usage_cached) and never blocks: a synchronous
            # fetch_usage here could stall PTY/RPC servicing for the fetch timeout
            # (~8s). The network refresh that keeps that cache warm runs off-loop in
            # the daemon refresher below. The first tick checks immediately
            # (last_cap_poll resets to 0 whenever the cap is set). Fail-open: a
            # missing/stale usage signal (older than USAGE_STALE_LIMIT_S) never stops
            # the run.
            if state["active_cap"] and state["current"] in (S.BUSY, S.BOOTING):
                now = time.time()
                if now - state["last_cap_poll"] >= CAP_POLL_INTERVAL_S:
                    state["last_cap_poll"] = now
                    quota = read_usage_cached(stale_limit=USAGE_STALE_LIMIT_S)
                    missing = unknown_cap_metrics(quota, state["active_cap"])
                    if missing:
                        sys.stderr.write(
                            f"[ccx] usage cap check skipped unknown/stale metrics: {', '.join(missing)}\n"
                        )
                    trip = cap_tripped(quota, state["active_cap"])
                    if trip:
                        key, util, cap = trip
                        set_state(S.BUDGET_STOPPED,
                                  reason=f"usage cap {key} reached: {util:.0f}% ≥ {cap:.0f}%")
                        broadcast({"type": "budget_stopped", "metric": key,
                                   "utilization": util, "cap": cap})
                        break

            # Shared raw terminal input is a bounded, fair PTY write; never an
            # admission fence for semantic operations or brokered hook replies.
            # A slow native child must not make a raw attacher's input wait
            # through the semantic write timeout. Disconnect it on a stalled
            # 4 KiB slice; keep ordinary CCX writes and hooks responsive.
            hub.tick(lambda part: write_pty(
                part, timeout_s=.01, absolute_timeout_s=.01))
            # Build select set
            rlist = [server, master_fd] + list(state["clients"].keys())
            wlist = hub.pending()
            try:
                fast_observe = (
                    state.get("interrupt_observation") is not None
                    or state.get("late_hook_guard") is not None
                    or state.get("successor_pending") is not None
                )
                select_timeout = INTERRUPT_NATIVE_POLL_S if fast_observe else 1.0
                if hub.busy():
                    select_timeout = min(select_timeout, .05)
                r, writable, _ = select.select(rlist, wlist, [], select_timeout)
            except (InterruptedError, OSError):
                continue

            for conn in writable:
                hub.flush(conn)
            for src in r:
                if src is server:
                    try:
                        conn = accept_same_user(server)
                        if conn is not None:
                            conn.setblocking(False)
                            state["clients"][conn.fileno()] = (conn, bytearray())
                    except OSError:
                        pass
                elif src is master_fd:
                    # Same output path while sending and in the ordinary loop:
                    # both update the PTY tail and Ink-ready quiescence clock.
                    if not read_pty_output():
                        break
                else:
                    # client fd
                    fd = src
                    conn, buf = state["clients"].get(fd, (None, None))
                    if conn is None:
                        continue
                    try:
                        data = conn.recv(4096)
                    except BlockingIOError:
                        # Not readable after all (e.g. the fd was closed by a
                        # broadcast this pass and reused by a new connection).
                        continue
                    except OSError:
                        data = b""
                    if not data:
                        drop_client(conn)
                        continue
                    buf.extend(data)
                    if (hub.has(conn) and len(buf) > MAX_ATTACH_LINE
                            and b"\n" not in buf):
                        hub.release(conn, "oversized-attachment-frame")
                        drop_client(conn)
                        continue
                    attach_frames = 0
                    while b"\n" in buf:
                        if fd not in state["clients"]:
                            break  # attach-close or overflow already removed this fd
                        if hub.has(conn) and attach_frames >= 64:
                            hub.release(conn, "input-frame-flood")
                            drop_client(conn)
                            break
                        attach_frames += 1
                        line, rest = buf.split(b"\n", 1)
                        del buf[:]
                        buf.extend(rest)
                        if not line.strip():
                            continue
                        if hub.has(conn) and len(line) > MAX_ATTACH_LINE:
                            hub.release(conn, "oversized-attachment-frame")
                            drop_client(conn)
                            break
                        try:
                            msg = json.loads(line)
                        except (UnicodeError, ValueError):
                            if hub.has(conn):
                                hub.release(conn, "malformed-attachment-frame")
                                drop_client(conn)
                                break
                            continue
                        if not isinstance(msg, dict):
                            if hub.has(conn):
                                hub.release(conn, "malformed-attachment-frame")
                                drop_client(conn)
                                break
                            continue
                        try:
                            handle_client_msg(conn, msg)
                        except Exception as e:
                            try:
                                conn.sendall((json.dumps({"ok": False, "error": f"handler: {e}"}) + "\n").encode())
                            except OSError:
                                pass

        except Exception as exc:
            # Any unexpected failure (an unreadable journal, a full disk, a bug)
            # must still take the ordinary shutdown below: SIGTERM the worker,
            # remove the socket and pidfile, publish a terminal record. Escaping
            # here instead leaves a live-looking record for a dead host.
            traceback.print_exc()
            reason = f"host error: {exc.__class__.__name__}: {exc}"
            try:
                set_state(S.CRASHED, reason=reason)
                broadcast({"type": "host_error", "reason": reason})
            except Exception:
                state["current"] = S.CRASHED
                state["pending_last_error"] = reason
            break

    hub.release_all("host-exit")

    # Shutdown.  Reconcile an active compact before tearing down its host/PTY.
    # A boundary may have landed on the final loop tick; if not, preserve the
    # operation as unknown so a caller can distinguish interrupted evidence
    # from confirmed native failure.
    active_compact = state.get("compacting")
    if active_compact is not None:
        operation_id = active_compact.get("operation_id")
        try:
            operation = reconcile_compact_operation(
                short, operation_id, transcript=rec["transcript"],
                lock_timeout=COMPACT_LOCK_TIMEOUT_S,
            )
            if operation and operation.get("status") in COMPACT_PENDING_STATUSES:
                operation = transition_compact_operation(
                    short,
                    operation_id,
                    "unknown",
                    reason="host stopped before terminal compact evidence",
                    lock_timeout=COMPACT_LOCK_TIMEOUT_S,
                )
            broadcast({
                "type": "compact_shutdown",
                "operation_id": operation_id,
                "status": (operation or {}).get("status"),
                "reason": (operation or {}).get("reason"),
                "evidence_uuid": (operation or {}).get("evidence_uuid"),
            })
            if operation is not None:
                broadcast_missing_compact_end(operation_id, operation)
        except Exception as exc:
            broadcast({
                "type": "compact_recovery_error",
                "operation_id": operation_id,
                "reason": str(exc),
            })

    usage_refresh_stop.set()  # wake the refresher out of its sleep so it exits
    # Let a pending transcript pass finish so the cache describes the
    # final turn; bounded, because a pathological transcript must not keep a
    # dying host around.
    refresh_stop.set()
    refresh_wanted.set()
    refresher.join(timeout=15.0)
    def await_child(grace_s: float) -> None:
        t_end = time.time() + grace_s
        while time.time() < t_end and not child_exited[0]:
            time.sleep(0.1)
            try:
                wpid, status = os.waitpid(child_pid, os.WNOHANG)
                if wpid == child_pid:
                    child_exited[0] = True
            except ChildProcessError:
                child_exited[0] = True
                break

    try:
        # If the child is still alive, SIGTERM it (idle-killed or kill op), and
        # SIGKILL it if it outlives the grace period: a surviving `claude` would
        # keep writing the session JSONL beside the one a revive starts.
        for sig, grace_s in ((signal.SIGTERM, 5.0), (signal.SIGKILL, 2.0)):
            if child_exited[0]:
                break
            try:
                os.kill(child_pid, sig)
            except ProcessLookupError:
                pass
            await_child(grace_s)
    finally:
        cur = state["current"]
        if state.get("killed_explicit"):
            # User typed `ccx kill`. Terminal: never auto-respawn from `ccx send`.
            cur = S.DEAD
        elif cur not in TERMINAL_STATES:
            cur = S.IDLE_KILLED if state["turns"] > 0 else S.CRASHED
        # A worker that just *exits* (PTY EOF) trips no set_state, so it leaves no
        # pending_last_error — the crash reason exists only as the dying output
        # still sitting in pty_buf. Recover it, so a boot failure explains itself
        # instead of surfacing as a bare `crashed`. Never overwrite a reason a
        # set_state already recorded.
        last_error = state.get("pending_last_error")
        if last_error is None and cur == S.CRASHED:
            last_error = pty_tail_reason(state["pty_buf"])
        # The event feed must end on the terminal state too. `kill`, SIGTERM
        # (confirmed kill, `ccx shutdown`) and a worker that simply exits never
        # went through set_state, and an explicit kill overrides an earlier
        # idle-kill; journal the final transition before the sidecar closes.
        # Claude's background commands, Monitors and subagents end with it, so
        # a stopped worker must not keep showing them as running.
        if child_exited[0] and bg["items"]:
            try:
                publish_bg([], [("stopped", {**item, "status": "stopped"})
                                for item in bg["items"]])
                bg["items"] = []
            except Exception:
                pass
        if cur != state["current"]:
            try:
                broadcast({"type": "state", "from": state["current"], "to": cur,
                           "ts": time.time(), "reason": last_error})
            except Exception:
                pass
            state["current"] = cur
        # Order matters: tear down the sock + pidfile before flipping the record
        # to a terminal state. Otherwise a concurrent `ccx send` that reads
        # state=idle-killed during the worker SIGTERM wait will _respawn a new
        # host that binds the same sock path — and then our `sp.unlink()` below
        # would erase the new host's path entry. (Sock file path is the contract
        # for ccx send → host RPC; deleting it under a live new host is fatal.)
        try: server.close()
        except OSError: pass
        try: os.close(master_fd)
        except OSError: pass
        if event_file is not None:
            try: event_file.close()
            except OSError: pass
        try: sp.unlink()
        except (OSError, FileNotFoundError): pass
        # Host PID is stored only in the fresh lifecycle record; no legacy
        # path accessor is allowed to remove a sibling/new host's pidfile.
        # Now publish the terminal state — ccx send seeing this is guaranteed to
        # find no live sock and will respawn cleanly (IDLE_KILLED/CRASHED) or
        # refuse to revive (DEAD).
        # Keep the worker pid of a child we could not reap (stuck in
        # uninterruptible I/O): `kill`'s orphan sweep still needs to find it.
        host_terminal_transition(
            short, state=cur, host_pid=host_pid, last_error=last_error,
            clear_child=bool(child_exited[0]),
        )
