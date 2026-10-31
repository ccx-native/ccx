from __future__ import annotations

import fcntl
import json
import os
import signal
import socket
import time

from ccx.constants import (
    BUDGET_EXIT,
    HOST_SOCK_WAIT_RESPAWN_S,
    RESPAWN_IDLE_DEADLINE_S,
    S,
)
from ccx.store.liveness import lifecycle_host_alive, owned_pid_alive, owned_process
from ccx.store.paths import respawn_lock_path, sock_path
from ccx.store.records import merge_lifecycle, read_record
from ccx.claude.settings import ensure_trusted_paths, write_settings
from ccx.claude.spawn import PHASE_SOCK_TIMEOUT, spawn_host_detached


def host_socket_live(short: str, rec: dict) -> bool:
    """Is the session's managed host actually reachable over its Unix socket?

    True only when the socket file is present AND, if we recorded a hostPid,
    that process is still alive. A host killed ungracefully (host crash, SIGKILL,
    machine reset) can leave its socket on disk while the lifecycle state is
    still non-terminal — dialing that stale socket would otherwise refuse the
    connection. Detecting the dead hostPid lets `send` revive transparently and
    `answer`/`approve` fail with a clear "host not alive" instead of a confusing
    connection-refused rejection. A None hostPid (host mid-boot, pid not yet
    recorded) is treated as live to avoid racing a legitimate startup."""
    if not sock_path(short).exists():
        return False
    lifecycle = rec.get("lifecycle") or {}
    return not (lifecycle.get("hostPid") and not lifecycle_host_alive(lifecycle))


def _rpc(short: str, msg: dict, timeout: float = 5.0) -> dict:
    """One existing host RPC with same-connection Linux PID/start handshake.

    First read the SERVER's exact full-root/generation/worker handshake on
    the SAME socket used for the subsequent native request. Verify its
    kernel SO_PEERCRED PID/start against the currently owned worker record
    and revalidate the generation before writing an operation. Never infer
    ownership from an AF_UNIX pathname or same UID alone.
    """
    from ccx import constants
    from ccx.store.root import require_ccx_active
    from ccx.rpc.transport import (
        plan_transport, handshake_payload, verify_handshake,
    )
    from ccx.store.identity import EngineMismatch

    store = require_ccx_active(constants.CCX_ROOT)
    owner = store.resolve(short)
    if owner.engine != "claude":
        raise EngineMismatch("Claude host RPC requires full Claude worker UUID")
    plan = plan_transport(store, target=owner.worker)
    if type(timeout) not in (int, float) or timeout <= 0 or timeout > 600:
        return {"ok": False, "error": "invalid bounded RPC timeout"}

    def one_line(sock: socket.socket) -> dict:
        buf = bytearray()
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                return {"ok": False, "error": "no reply"}
            buf.extend(chunk)
            if len(buf) > 1024 * 1024:
                return {"ok": False, "error": "reply too large"}
            if b"\n" in buf:
                try:
                    value = json.loads(bytes(buf).split(b"\n", 1)[0])
                except (UnicodeError, ValueError):
                    return {"ok": False, "error": "bad reply"}
                return value if isinstance(value, dict) else {
                    "ok": False, "error": "non-object reply"
                }

    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.settimeout(timeout)
            sock.connect(str(plan.final))
            sock.sendall((json.dumps({
                "op": "handshake",
                "identity": handshake_payload(plan),
            }) + "\n").encode())
            hello = one_line(sock)
            if not hello.get("ok"):
                return {"ok": False, "error": "unverified host: " + str(
                    hello.get("error", "no handshake reply")
                )}
            verify_handshake(
                store, plan, received=hello.get("identity"),
                connected_socket=sock,
            )
            store.revalidate()  # closing gate BEFORE native request
            sock.sendall((json.dumps(msg) + "\n").encode())
            return one_line(sock)
    except socket.timeout:
        return {"ok": False, "error": "timeout"}
    except OSError as exc:
        return {"ok": False, "error": f"unreachable: {exc.__class__.__name__}"}

def _drain_old_host(
    short: str,
    rec: dict,
    timeout: float = 8.0,
    *,
    force_on_timeout: bool = True,
) -> bool:
    """Block until any prior host for `short` is fully gone before we fork a new
    one. `ccx kill` only *signals* the host; its shutdown (worker SIGTERM + an
    up-to-5s wait, then sock unlink, then a final lifecycle write) can still be in
    flight when a revive `send` arrives right after. Forking into that window
    causes two failures:
      - our sock-wait latches the dying host's stale sock and reads its terminal
        state, so the IDLE-wait bails with a spurious rc=1; and
      - the old host's *post-unlink* final `merge_lifecycle` clobbers the new
        host's `booting`/`hostPid` straight back to the terminal state.
    Both are only safe once the old host's PID is dead (past that last write), so
    we wait on the PID, not just the sock. A host still alive at the normal
    drain timeout is wedged — force-kill it and drop its sock so revive can
    proceed. When an outer operation deadline shortens the wait,
    ``force_on_timeout=False`` returns ``False`` instead: a caller's small
    timeout must not make an otherwise healthy shutdown look wedged."""
    sp = sock_path(short)
    lifecycle = rec.get("lifecycle") or {}
    hp = lifecycle.get("hostPid")
    host_start = lifecycle.get("hostStart")
    deadline = time.time() + timeout
    while time.time() < deadline:
        presence = owned_process(hp, host_start)
        if presence == "unprovable":
            return False  # cannot remove a potentially live owner's socket
        if presence == "dead":
            # Old host is proved gone (or none was recorded).
            try:
                sp.unlink()
            except (OSError, FileNotFoundError):
                pass
            return True
        time.sleep(min(0.1, max(0.0, deadline - time.time())))
    if not force_on_timeout:
        return False
    # Wedged: the prior host outlived the drain budget. Force it down — but
    # only the recorded host itself, never a process that reused its pid.
    presence = owned_process(hp, host_start)
    if presence == "unprovable":
        return False
    if presence == "alive":
        try:
            os.kill(hp, signal.SIGKILL)
        except OSError:
            pass
    try:
        sp.unlink()
    except (OSError, FileNotFoundError):
        pass
    return True


def _missing_claude_bin() -> str | None:
    """Why the configured Claude binary cannot run, or None when it can."""
    from ccx.system.engine_binary import resolve_engine_binary
    from ccx.errors import SessionUnavailable

    try:
        resolve_engine_binary("claude")
        return None
    except SessionUnavailable as error:
        return str(error)


def _record_respawn_failure(short: str, detail: str) -> int:
    """Make a failed revive retryable and diagnostically honest.

    ``RESUMING`` is transitional: it is only valid while a newly launched host
    is expected to take ownership.  If no host was handed off, leaving that
    state on disk makes UIs show an endless resume and obscures the launch
    error.  ``CRASHED`` is both truthful and already part of the transparent
    revive set, so the next send can retry normally.
    """
    try:
        merge_lifecycle(
            short,
            state=S.CRASHED,
            hostPid=None,
            hostStart=None,
            workerPid=None,
            workerStart=None,
            lastError=f"respawn: {detail}",
            # Invariant: whoever writes a new lastError also clears apiError.
            # The RESUMING write already cleared it on this path, but keeping
            # the rule local means a future reordering cannot resurrect a stale
            # classification against a fresh, unrelated reason.
            apiError=None,
        )
    except OSError:
        pass
    return 1


def _respawn_failure_message(short: str, rc: int) -> str:
    """Return the actionable recorded reason for a failed managed respawn.

    ``_respawn`` keeps its historic integer return contract because the CLI and
    Python clients share it.  The lifecycle record is the durable diagnostic
    channel, so every caller should surface ``lastError`` instead of collapsing
    all failures to an opaque ``rc=1``.
    """
    last_error = (
        ((read_record(short) or {}).get("lifecycle") or {}).get("lastError")
    )
    if isinstance(last_error, str) and last_error.strip():
        detail = last_error.strip()
        if detail.startswith("respawn:"):
            detail = detail.removeprefix("respawn:").strip()
        return f"respawn failed (rc={rc}): {detail}"
    return f"respawn failed (rc={rc})"


def _acquire_respawn_lock(handle, deadline: float | None) -> bool:
    """Acquire one session's revive lock, respecting an optional deadline."""
    while True:
        if deadline is not None and time.time() >= deadline:
            return False
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except BlockingIOError:
            wait_s = 0.05
            if deadline is not None:
                wait_s = min(wait_s, max(0.0, deadline - time.time()))
            time.sleep(wait_s)


def _responsive_nonterminal_host(
    short: str, deadline: float | None = None
) -> bool:
    """Whether another caller already established a usable managed host."""
    # A terminal record can briefly coexist with the old host's still-responsive
    # socket while `kill` is draining. Never reuse that dying host; the normal
    # locked revive path waits for its PID to exit before launching a successor.
    rec_state = ((read_record(short) or {}).get("lifecycle") or {}).get("state")
    if rec_state in (S.IDLE_KILLED, S.CRASHED, S.DEAD, S.BUDGET_STOPPED):
        return False
    if not sock_path(short).exists():
        return False
    timeout = 1.0
    if deadline is not None:
        remaining = deadline - time.time()
        if remaining <= 0:
            return False
        timeout = min(timeout, remaining)
    status = _rpc(short, {"op": "status"}, timeout=timeout)
    return bool(
        status.get("ok")
        and status.get("state")
        not in (S.IDLE_KILLED, S.CRASHED, S.DEAD, S.BUDGET_STOPPED)
    )


def _finalize_killed_steers(
    store, handle: str, *, child_pid: int | None, child_start: str | None,
    lock_held: bool = False,
) -> None:
    """Fence kill cleanup against a newer Claude lifetime on respawn.lock.

    A concurrent revive can already have launched a new child and accepted its
    own steer. Its current nativeStartProof MUST still match the old dead child
    when the finalizer runs, or nothing in the newer hot journal is touched.
    """
    def finish() -> None:
        lifecycle = (store.record(handle).get("lifecycle") or {})
        proof = lifecycle.get("nativeStartProof") or {}
        if (type(child_pid) is not int or not child_start
                or proof.get("workerPid") != child_pid
                or str(proof.get("workerStart")) != str(child_start)
                or owned_process(child_pid, child_start) != "dead"):
            return
        from ccx.ops.steer_ops import finalize_dead_claude_steers
        finalize_dead_claude_steers(
            store, handle, child_pid=child_pid, child_start=child_start)

    if lock_held:
        # _respawn_locked already owns this same flock; reacquiring would
        # deadlock when its park_worker path confirms an old child kill.
        finish()
        return
    short = store.resolve(handle).worker_id
    lock_path = respawn_lock_path(short)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return  # native kill succeeded; a locked revive repairs old receipts
        finish()


def _respawn(short: str, caps: dict | None = None,
             deadline: float | None = None, *,
             report_launch: bool = False) -> int | tuple[int, bool]:
    'Serialize and respawn a dead host for an existing session.\n\n    The per-session file lock spans the full revive-to-IDLE transition. After\n    acquiring it, liveness is re-checked: a concurrent ``send`` or ``compact``\n    may already have revived the worker while this caller was waiting, in which\n    case the existing host is reused instead of launching a duplicate.\n\n    Returns 0 on success. With report_launch=True, returns (rc, launched)\n    so concurrent revivers cannot both claim to have started a new host.\n\n    Usage caps are run-scoped: a respawn always starts uncapped unless `caps` is\n    passed (i.e. the reviving `send` explicitly re-specifies a cap). This is what\n    lets you "break the cap" — send to a budget-stopped session and keep going.\n\n    The host is launched via :func:`ccx.claude.spawn.spawn_host_detached` — never an\n    in-process ``os.fork()`` — so revive is safe from threaded library callers\n    and the revived host survives the caller exiting. `deadline` (epoch seconds,\n    optional) caps the whole revive: the sock wait and the post-spawn IDLE wait\n    are clamped to it so a caller\'s send `timeout` truthfully bounds the revive\n    too. ``None`` keeps the historic bounds (sock 10s, IDLE\n    ``RESPAWN_IDLE_DEADLINE_S``).'
    lp = respawn_lock_path(short)
    try:
        lp.parent.mkdir(parents=True, exist_ok=True)
        with lp.open("a") as lock:
            if not _acquire_respawn_lock(lock, deadline):
                return (1, False) if report_launch else 1
            if _responsive_nonterminal_host(short, deadline):
                return (0, False) if report_launch else 0
            if deadline is not None and time.time() >= deadline:
                return (1, False) if report_launch else 1
            rc = _respawn_locked(short, caps=caps, deadline=deadline)
            return (rc, rc == 0) if report_launch else rc
    except OSError as e:
        try:
            # Clear the old diagnosis with the old reason: this failure is the
            # lock's, and it happens BEFORE the RESUMING write that would
            # otherwise have cleared it.
            merge_lifecycle(short, lastError=f"respawn-lock: {e}", apiError=None)
        except OSError:
            pass
        return (1, False) if report_launch else 1


def _respawn_locked(short: str, caps: dict | None = None,
                    deadline: float | None = None) -> int:
    """Perform a managed-host revive while the caller holds its session lock."""
    rec = read_record(short)
    if rec is None:
        return 2
    dargs = rec.get("createArgs", {})
    cwd = rec["cwd"]

    # A managed session can outlive a temporary/project directory. Never
    # recreate it implicitly: the caller may need to restore a specific
    # worktree, and silently resuming in an empty directory would give the model
    # a materially different environment. Fail before launching and preserve an
    # actionable reason for CLI, Python, and dashboard callers.
    if not os.path.isdir(cwd):
        return _record_respawn_failure(
            short,
            f"recorded cwd does not exist or is not a directory: {cwd}; "
            "restore that directory before resuming this session",
        )

    # Reuse the accepted K host+native-child teardown (same exact UUID,
    # PID/start pair and terminal CAS) rather than adding a second orphan
    # sweep. _respawn holds only respawn.lock; park_worker never takes it.
    # A running prior BOOTING/RESUMING host is an outstanding original launch,
    # NOT permission to start a second provider process.
    from ccx import constants
    from ccx.store.root import require_ccx_active
    from ccx.ops.wait import park_worker
    store = require_ccx_active(constants.CCX_ROOT)
    handle = store.resolve(short).worker
    store.authorize_native_action(handle, "resume")
    lifecycle = rec.get("lifecycle") or {}
    if lifecycle.get("state") in (S.BOOTING, S.RESUMING):
        hp, hs = lifecycle.get("hostPid"), lifecycle.get("hostStart")
        if hp and owned_process(hp, hs) != "dead":
            return 1  # observe the original in-flight startup, no new launch
        if lifecycle.get("state") == S.RESUMING:
            return 1  # indeterminate last launcher, no speculative replay
    if lifecycle.get("state") in (S.BUSY, S.WAITING):
        hp, hs = lifecycle.get("hostPid"), lifecycle.get("hostStart")
        if hp and owned_process(hp, hs) != "dead":
            return 1  # live owned turn/gate: a missed status probe is no teardown proof
    recorded_process = (lifecycle.get("hostPid") is not None
                        or lifecycle.get("workerPid") is not None)
    if (lifecycle.get("state") not in
            (S.DEAD, S.CRASHED, S.IDLE_KILLED, S.BUDGET_STOPPED) or recorded_process):
        budget = (90.0 if deadline is None else max(0.0, deadline - time.time()))
        if budget <= 0:
            return 1
        park_worker(handle, timeout=budget,
                    expected_generation=store.generation,
                    _respawn_lock_held=True)
        rec = read_record(short)  # only the now-confirmed terminal original
    # Drain of the now-confirmed gone host is purely for its stale socket.
    # Make sure any prior host for this short has fully exited before we spawn —
    # otherwise its stale sock / late lifecycle write races the new host (see
    # _drain_old_host). This is what makes `ccx kill` immediately followed by a
    # revive `ccx send` reliable instead of a ~rc=1 coin-flip.
    drain_timeout = 8.0
    force_on_timeout = True
    if deadline is not None:
        remaining = max(0.0, deadline - time.time())
        if remaining < drain_timeout:
            drain_timeout = remaining
            force_on_timeout = False
    if not _drain_old_host(
        short,
        rec,
        timeout=drain_timeout,
        force_on_timeout=force_on_timeout,
    ):
        return 1

    # Reconcile the same old child's steers only if its PID/start is positively
    # dead. Never infer death from a stale socket or a recorded DEAD string.
    proof = (rec.get("lifecycle") or {}).get("nativeStartProof") or {}
    child_pid, child_start = proof.get("workerPid"), proof.get("workerStart")
    if (isinstance(child_pid, int) and child_start
            and owned_process(child_pid, child_start) == "dead"):
        try:
            from ccx.ops.steer_ops import finalize_dead_claude_steers
            finalize_dead_claude_steers(
                store, handle, child_pid=child_pid, child_start=child_start)
        except Exception:
            pass  # keep unresolved receipt; later proven kill/revive can repair

    # A revived host without a runnable claude could only crash; refuse before
    # trusting folders or rewriting settings for it.
    missing_bin = _missing_claude_bin()
    if missing_bin:
        return _record_respawn_failure(short, missing_bin)

    # Re-seed trust for cwd AND every --add-dir on revive too: the added dirs are
    # persisted in createArgs, and a missing trust would hang the resumed boot.
    # IO failure = no revivable host; keep the CLI's int contract (rc=1) and
    # record the reason instead of leaking a raw OSError to library callers.
    try:
        ensure_trusted_paths([cwd, *(dargs.get("add_dir") or [])])
        write_settings(short)
    except OSError as e:
        try:
            merge_lifecycle(short, lastError=f"respawn: settings-io: {e}",
                            apiError=None)
        except OSError:
            pass
        return 1

    if deadline is not None and time.time() >= deadline:
        return 1

    # A fork whose first activation died before its transcript existed is
    # copied here, under the same lock and with the old host drained.
    from ccx.errors import CcxError
    from ccx.ops.forking import claude_fork_pending, ensure_claude_fork_materialized
    if claude_fork_pending(rec):
        try:
            rec = ensure_claude_fork_materialized(short, deadline=deadline)
        except (CcxError, OSError) as e:
            return _record_respawn_failure(short, f"fork copy: {e}")
        if deadline is not None and time.time() >= deadline:
            return _record_respawn_failure(short, "deadline expired after the fork copy")

    # The revived host detects resume itself: cmd_host passes `--resume <uuid>`
    # to claude whenever the session transcript already has content, so the
    # relaunch needs no resume flag or env hint here.
    #
    # Publish lifecycle/transients in one fresh field CAS. The old
    # snapshot must never erase another writer's unrelated accepted turn.
    patches = {
        "lifecycle.state": S.RESUMING,
        "lifecycle.since": int(time.time() * 1000),
        "lifecycle.lastError": None,
        "lifecycle.apiError": None,
    }
    if caps:
        patches["activeCap"] = caps
    try:
        store.mutate_record(
            handle, patches=patches,
            removals=() if caps else ("activeCap",),
            expect={"lifecycle.state": (rec.get("lifecycle") or {}).get("state")},
        )
    except OSError:
        return 1

    # Launch the detached host through the subprocess launcher — no in-process
    # fork. The launcher waits for the sock within the caller's remaining
    # budget and the host reparents to init, so it outlives whoever called this.
    sock_wait = HOST_SOCK_WAIT_RESPAWN_S
    if deadline is not None:
        remaining = deadline - time.time()
        if remaining <= 0:
            return _record_respawn_failure(
                short, "deadline expired before host launch"
            )
        sock_wait = min(sock_wait, remaining)
    verdict = spawn_host_detached(short, sock_wait_s=sock_wait)
    if not verdict.get("ok"):
        if verdict.get("phase") != PHASE_SOCK_TIMEOUT:
            # Hard launcher failure: no host was handed off. A transitional
            # RESUMING record must not survive this path.
            return _record_respawn_failure(
                short, verdict.get("error") or "host launcher failed"
            )
        if not sock_path(short).exists():
            # A detached child could still be starting. Preserve RESUMING as
            # unresolved; do not mark dead and relaunch on the next call.
            return 1
    # Wait for state=IDLE. Claude --resume can be slow loading the prior transcript
    # and remounting Ink. Older builds may also run a synthetic auto-continue turn
    # (bounded at AUTO_CONTINUE_MAX_S=25s host-side). On slow disk/system we have
    # observed up to ~150s end-to-end; deadline 180s leaves headroom. A caller
    # `deadline` clamps this so a send timeout truthfully bounds the revive.
    state_deadline = time.time() + RESPAWN_IDLE_DEADLINE_S
    if deadline is not None:
        state_deadline = min(state_deadline, deadline)
    while time.time() < state_deadline:
        cur = (read_record(short) or {}).get("lifecycle", {}).get("state")
        if cur == S.IDLE:
            return 0
        if cur == S.BUDGET_STOPPED:
            # A re-capped resume tripped its cap during the auto-continue turn.
            # Return promptly instead of waiting out the IDLE deadline.
            return BUDGET_EXIT
        if cur in (S.CRASHED, S.DEAD):
            return 1
        sleep_s = 0.25
        if deadline is not None:
            sleep_s = min(sleep_s, max(0.0, state_deadline - time.time()))
        time.sleep(sleep_s)
    # If the host is absent/unresponsive, convert the abandoned transition to a
    # retryable terminal state. If a live host merely exceeded the caller's
    # deadline, preserve its authoritative state and let it finish/watchdog.
    status_timeout = 1.0
    if deadline is not None:
        remaining = deadline - time.time()
        if remaining <= 0:
            rec_now = read_record(short) or {}
            lifecycle_now = rec_now.get("lifecycle") or {}
            if owned_process(lifecycle_now.get("hostPid"),
                             lifecycle_now.get("hostStart")) == "dead":
                return _record_respawn_failure(
                    short, "host did not reach idle before deadline"
                )
            try:
                merge_lifecycle(
                    short,
                    lastError="respawn: host did not reach idle before deadline",
                    apiError=None,
                )
            except OSError:
                pass
            return 1
        status_timeout = min(status_timeout, remaining)
    status = _rpc(short, {"op": "status"}, timeout=status_timeout)
    if not status.get("ok"):
        reason = status.get("error") or "host unreachable"
        return _record_respawn_failure(
            short, f"did not reach idle before deadline ({reason})"
        )
    try:
        merge_lifecycle(
            short, lastError="respawn: host did not reach idle before deadline",
            apiError=None,
        )
    except OSError:
        pass
    return 1
