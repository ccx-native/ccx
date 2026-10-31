"""Confirmed forced termination of a CCX-owned runtime, shared by both engines.

``kill`` is the strong lifecycle verb, but what it must confirm differs by
engine, because the two runtimes are shaped differently:

* **Claude** owns a process tree per worker: a ``ccx host`` and the ``claude``
  PTY child it spawns, which reparents to init if the host is SIGKILLed. Kill
  confirms both are gone; ignoring the child would report a stopped worker while
  a real engine kept burning quota.
* **Codex** owns no per-worker process. Every worker is one thread multiplexed
  on the single shared ``codex app-server`` child of the single shared ``codexd``
  daemon — and each worker record stamps that device-wide daemon PID and
  app-server PID into its own lifecycle. Signalling or reaping them to "kill a
  worker" would take down the daemon and every sibling worker with it. Codex
  kill therefore confirms a per-worker **unload** from the shared daemon, and
  never sends a signal to any process.

This is deliberately not `stop`. `stop` cancels the active turn and leaves the
runtime up. `kill` tears the runtime down while preserving resumability: the
durable Claude session id / Codex thread id survives, and a later `send` revives
the same conversation.

A busy worker is never refused: kill best-effort cancels the running turn,
then terminates. It has
to work for idle, busy, waiting, already-stopped, and partially dead process
trees, because that is exactly when a caller reaches for it.

Unconfirmable termination raises :class:`~ccx.errors.SessionUnavailable` rather
than lying.
"""

from __future__ import annotations

import os
import signal
import time

from ccx.constants import S
from ccx.errors import InvalidRequest, SessionUnavailable
from ccx.store.liveness import owned_pid_alive, owned_process, pid_alive

# How long an unbounded kill lets a process exit on SIGTERM before SIGKILL.
# Only relevant when the caller passed timeout=None; a numeric budget is
# divided instead (see :func:`escalate_at`).
TERM_GRACE_S = 5.0

# Default confirmation budget for one kill, shared by both CLIs and
# ``WorkerClient.kill``.
KILL_TIMEOUT_DEFAULT_S = 90

# Per-step caps for the Codex path. `left()` returns the cap only when the
# caller passed no deadline at all; with a deadline it returns the exact
# remaining budget, including a genuine 0.0. Never coalesce that 0.0 away — a
# `or cap` there silently turns "no budget" into a fresh multi-second wait.
_CODEX_KILL_RPC_CAP_S = 30.0
_CODEX_PROBE_CAP_S = 3.0
# Slice of the kill RPC budget reserved for the daemon to answer. The daemon may
# spend the rest observing an in-flight turn launch, so the caller's budget
# bounds that wait too — it is never silently expanded by a fixed daemon window.
_CODEX_REPLY_MARGIN_S = 1.0

_TERMINAL_CLAUDE = (S.DEAD, S.IDLE_KILLED, S.CRASHED, S.BUDGET_STOPPED)
_TERMINAL_CODEX = ("dead", "idle-killed", "crashed", "budget-stopped")


# --- budgets and process primitives -----------------------------------------

def deadline_for(timeout: float | None) -> float | None:
    if timeout is not None and timeout < 0:
        raise InvalidRequest("timeout must be zero or greater")
    return None if timeout is None else time.monotonic() + timeout


def left(deadline: float | None, cap: float | None = None) -> float | None:
    """Remaining budget, optionally capped by a per-step maximum."""
    if deadline is None:
        return cap
    rest = max(0.0, deadline - time.monotonic())
    return rest if cap is None else min(cap, rest)


def signal_pid(pid, sig) -> None:
    if not pid:
        return
    try:
        os.kill(pid, sig)
    except OSError:
        pass


def escalate_at(deadline: float | None) -> float:
    """When to give up on a graceful exit and SIGKILL.

    Half of whatever the caller's budget still allows, or — for an unbounded
    kill, where there is no budget to divide — a fixed grace period. Escalation
    timing is not a deadline extension: it only decides *when* SIGKILL happens
    inside the wait the caller already asked for.
    """
    if deadline is None:
        return time.monotonic() + TERM_GRACE_S
    return time.monotonic() + max(0.0, deadline - time.monotonic()) / 2


def await_exit(pid, deadline: float | None, *, alive=pid_alive) -> bool:
    """Poll until this original process is gone, never past caller deadline."""
    while alive(pid):
        if deadline is not None and time.monotonic() >= deadline:
            return False
        time.sleep(0.05)
    return True


def reap(pid, deadline: float | None, *, alive=pid_alive) -> bool:
    """SIGTERM then SIGKILL only the recorded orphanable child."""
    if not pid or not alive(pid):
        return True
    signal_pid(pid, signal.SIGTERM)
    if await_exit(pid, escalate_at(deadline), alive=alive):
        return True
    if alive(pid):
        signal_pid(pid, signal.SIGKILL)
    return await_exit(pid, deadline, alive=alive)


def _claude_confirmed_kill(
    short: str, deadline: float | None, *, view: dict,
    rpc_call, terminal, host_alive, child_alive,
) -> dict:
    """Confirm the original UUID owner's host and child using scoped PID guards."""
    host_pid, child_pid = view["host_pid"], view["child_pid"]
    was_terminal = view["state"] in _TERMINAL_CLAUDE
    if not view["live"] and not host_alive(host_pid) and not child_alive(child_pid):
        state = terminal() if not was_terminal else view["state"]
        return {"engine": "claude", "short": short, "killed": True,
                "changed": not was_terminal,
                "interrupted": False, "resumable": view["resumable"], "state": state}

    rpc_budget = left(deadline, 3.0)
    reply: dict = {}
    if view["live"] and (rpc_budget is None or rpc_budget > 0):
        try:
            reply = rpc_call(rpc_budget)
        except Exception:
            reply = {}
    if not reply.get("ok") and host_alive(host_pid):
        signal_pid(host_pid, signal.SIGTERM)

    if not await_exit(host_pid, escalate_at(deadline), alive=host_alive):
        if host_alive(host_pid):
            signal_pid(host_pid, signal.SIGKILL)
        await_exit(host_pid, deadline, alive=host_alive)

    child_gone = reap(child_pid, deadline, alive=child_alive)
    if host_alive(host_pid) or not child_gone:
        raise SessionUnavailable(
            f"kill could not confirm worker processes stopped for {short}",
            short=short,
        )
    state = terminal()
    return {"engine": "claude", "short": short, "killed": True,
            "changed": True, "interrupted": False,
            "resumable": view["resumable"], "state": state}


def confirmed_kill_uuid(
    store, target: str, *, timeout: float | None = KILL_TIMEOUT_DEFAULT_S,
    _respawn_lock_held: bool = False,
) -> dict:
    """Exact full-UUID kill: worker owner only; never a short-record fallback."""
    from ccx.constants import CODEXD_RPC_GENERATION
    from ccx.store.root import RecordConflict
    from ccx.store.identity import UnknownWorker

    deadline = deadline_for(timeout)
    store.revalidate()
    owner = store.resolve(target)
    handle = owner.worker
    record = store.record(handle)
    native = record.get("native") or {}
    worker_id = owner.worker_id
    original = record.get("lifecycle") or {}
    identity = {key: original.get(key) for key in (
        "hostPid", "hostStart", "workerPid", "workerStart",
    )} if owner.engine == "claude" else {}
    if owner.engine == "claude":
        from ccx.store.root import NativeActionDenied
        session_id = native.get("sessionId")
        if not isinstance(session_id, str) or not session_id:
            raise SessionUnavailable("kill: Claude session is missing; inspect the worker", short=worker_id)
        host_pid = identity["hostPid"]
        child_pid = identity["workerPid"]
        host_start = identity["hostStart"]
        child_start = identity["workerStart"]
        # Neither signals nor claims of absence may depend on an unproved PID.
        for pid, start, role in ((host_pid,host_start,"host"),
                                 (child_pid,child_start,"child")):
            if owned_process(pid, start) == "unprovable":
                raise SessionUnavailable(
                    f"kill: worker {role} process could not be verified; check status", short=worker_id)
        def host_alive(pid):
            return bool(pid and host_start and owned_pid_alive(pid,host_start))
        def child_alive(pid):
            return bool(pid and child_start and owned_pid_alive(pid,child_start))
        from ccx.rpc.rpc import _rpc
        status = _rpc(handle, {"op":"status"}, timeout=max(.05, left(deadline, .5)))
        live = status.get("ok") is True
        if live and not host_alive(host_pid):
            raise SessionUnavailable("kill: Claude process changed; check worker status", short=worker_id)
        if (original.get("state") not in _TERMINAL_CLAUDE and not live
                and not (host_alive(host_pid) or child_alive(child_pid))
                and not host_pid and not child_pid):
            raise SessionUnavailable("kill: Claude process could not be verified; check worker status", short=worker_id)
        def commit():
            # A clean host exit may clear its own PID/start fields. Accept only
            # that disappearance, never a replacement process or owner.
            current = store.record(handle).get("lifecycle") or {}
            for key, original_value in identity.items():
                if current.get(key) not in (original_value, None):
                    raise SessionUnavailable("kill: worker host identity replaced", short=worker_id)
            patches = {"lifecycle.state": S.DEAD, "lifecycle.since": int(time.time()*1000),
                       "lifecycle.apiError": None}
            expect = {"lifecycle."+key: current.get(key) for key in identity}
            try:
                return store.mutate_record(handle, patches=patches, expect=expect)["lifecycle"]["state"]
            except RecordConflict as error:
                raise SessionUnavailable("kill: worker host identity changed; not marked terminal",short=worker_id) from error
        interrupted = False
        if live and original.get("state") == S.BUSY and not _respawn_lock_held:
            # Interrupt first: Claude ends the running tool and its processes,
            # which a plain SIGTERM of claude leaves running on macOS.
            try:
                from ccx.ops.cancel import stop_worker
                budget = 10.0 if deadline is None else min(
                    10.0, (deadline - time.monotonic()) / 2)
                stopped = stop_worker(handle, timeout=max(.05, budget),
                                      expected_generation=store.generation)
                interrupted = stopped["confirmed"] and stopped["outcome"] == "interrupted"
            except Exception:
                pass
        view = {"state": original.get("state"),"host_pid": host_pid,
                "child_pid": child_pid, "live": live, "resumable": True}
        result = _claude_confirmed_kill(
            worker_id, deadline, view=view,
            rpc_call=lambda budget: _rpc(handle,{"op":"kill"},timeout=budget),
            terminal=commit,host_alive=host_alive,child_alive=child_alive,
        )
        result["interrupted"] = interrupted
        # Dead-child proof is independent of the kill's existing CLI result.
        # An unreadable/source-swapped transcript fails closed, never masking
        # the confirmed native death or changing kill's exit status.
        try:
            from ccx.rpc.rpc import _finalize_killed_steers
            _finalize_killed_steers(
                store, handle, child_pid=child_pid, child_start=child_start,
                lock_held=_respawn_lock_held,
            )
        except Exception:
            pass  # uncertain journal/death/lock: keep pending for revive repair
    else:
        # A creation refused locally never reached Codex, so there is no
        # provider thread to unload. (A prepared one may still be dispatched.)
        if (native.get("nativeState") == "not-started-proven"
                and native.get("threadId") is None
                and store.native_activation(handle).get("state") == "local-refused"):
            changed = original.get("state") != "dead"
            if changed:
                store.mutate_record(
                    handle,
                    patches={
                        "lifecycle.state": "dead",
                        "lifecycle.since": int(time.time() * 1000),
                        "lifecycle.activeTurnId": None,
                    },
                    expect={"lifecycle.state": original.get("state")},
                )
            store.revalidate()
            return {
                "engine": "codex", "short": worker_id[:8], "worker": handle,
                "worker_id": worker_id, "killed": True, "changed": changed,
                "interrupted": False, "native_state": "dead", "state": "stopped",
                "parked": True, "history_retained": True,
                "resume_available": False, "resumable": False,
            }
        from ccx.store.root import NativeActionDenied
        try:
            source = store.codex_source(handle)
        except NativeActionDenied as error:
            raise SessionUnavailable(
                "kill: this Codex worker's start is not confirmed yet; wait for "
                "`ccx new` to finish, then retry", short=worker_id) from error
        thread_id = native.get("threadId")
        if (not isinstance(thread_id,str) or not thread_id
                or native.get("threadSource") != source):
            raise SessionUnavailable("kill: Codex conversation cannot be verified; check worker status",short=worker_id)
        from ccx.codex.runtime import daemon_rpc
        from ccx.constants import CODEXD_RPC_GENERATION
        probe = daemon_rpc({"op":"status","short":worker_id,
                            "rpcGeneration":CODEXD_RPC_GENERATION},timeout=max(.05,left(deadline,1)))
        if probe.get("transport") == "absent":
            # No original singleton can still hold an app-server subscription.
            # An explicit kill always leaves `dead`, even over an unloaded
            # idle-killed/crashed record, so nothing auto-reloads it later.
            changed = original.get("state") != "dead"
            if changed:
                store.mutate_record(handle,patches={"lifecycle.state":"dead",
                    "lifecycle.since":int(time.time()*1000),"lifecycle.activeTurnId":None},
                    expect={"lifecycle.state":original.get("state")})
            result = {"engine":"codex","short":worker_id,"killed":True,
                      "changed":changed,"interrupted":False,"state":"dead"}
        elif probe.get("transport") != "served":
            raise SessionUnavailable("kill: Codex status is unknown; retry after checking the worker",short=worker_id)
        elif probe.get("loaded") is False and original.get("state") in _TERMINAL_CODEX:
            changed = original.get("state") != "dead"
            if changed:
                store.mutate_record(handle,patches={"lifecycle.state":"dead",
                    "lifecycle.since":int(time.time()*1000),"lifecycle.activeTurnId":None},
                    expect={"lifecycle.state":original.get("state")})
            result = {"engine":"codex","short":worker_id,"killed":True,
                      "changed":changed,"interrupted":False,"state":"dead"}
        else:
            budget = left(deadline, _CODEX_KILL_RPC_CAP_S)
            if budget == 0:
                raise SessionUnavailable("kill: no remaining runtime confirmation budget",short=worker_id)
            answer = daemon_rpc({"op":"kill","short":worker_id,
                    "observeSeconds":max(0.0,budget-_CODEX_REPLY_MARGIN_S),
                    "rpcGeneration":CODEXD_RPC_GENERATION},timeout=budget)
            if answer.get("ok") is True and answer.get("unloaded") is True:
                # Existing daemon Worker.kill synchronously terminalizes and
                # deregisters. Native thread/unsubscribe completion is async.
                result={"engine":"codex","short":worker_id,"killed":True,
                        "changed":True,"interrupted":bool(answer.get("interrupted")),
                        "state":"dead"}
            else:
                remaining=left(deadline,_CODEX_PROBE_CAP_S)
                if remaining == 0:
                    raise SessionUnavailable("kill: Codex did not confirm stopping; check worker status",short=worker_id)
                observed=daemon_rpc({"op":"status","short":worker_id,
                    "rpcGeneration":CODEXD_RPC_GENERATION},timeout=remaining)
                if (observed.get("transport") != "served"
                        or observed.get("loaded") is not False
                        or store.record(handle)["lifecycle"].get("state") not in _TERMINAL_CODEX):
                    raise SessionUnavailable("kill: worker may still be running; check its status before retrying",short=worker_id)
                result={"engine":"codex","short":worker_id,"killed":True,
                        "changed":True,"interrupted":False,"state":"dead"}
    store.revalidate()
    # History is retained for both engines, but only verified original Codex
    # threads with an actual prior rollout have a supported resume path.
    # This is eligibility to try native resume, not proof it will succeed.
    from ccx.store.root import NativeActionDenied, CreationIndeterminate
    last = record.get("lastTurn")
    turns = (record.get("stats") or {}).get("turns")
    eligible_claude = False
    if (owner.engine == "claude"
            and record.get("terminalOnly") is not True
            and native.get("nativeState") == "session-bound"
            and isinstance(native.get("sessionId"), str)):
        import os
        import stat
        original_transcript = record.get("transcript")
        if isinstance(original_transcript, str):
            try:
                transcript_stat = os.stat(original_transcript)
                eligible_claude = (stat.S_ISREG(transcript_stat.st_mode)
                                   and transcript_stat.st_size > 0)
            except OSError:
                pass
    eligible = (owner.engine == "codex"
                and record.get("terminalOnly") is not True
                and native.get("nativeState") == "thread-bound"
                and isinstance(native.get("threadId"), str)
                and native.get("threadSource") == store.codex_source(handle)
                and (isinstance(turns, int) and turns > 0
                     or isinstance(last, dict) and isinstance(last.get("turnId"), str)))
    eligible = eligible or eligible_claude
    if eligible:
        try:
            store.authorize_native_action(handle, "resume")
        except (NativeActionDenied, CreationIndeterminate):
            eligible = False
    return {**result,"worker":handle,"worker_id":worker_id,"engine":owner.engine,
            "short":worker_id[:8],"history_retained":True,
            "resume_available":eligible,"resumable":eligible,
            "native_state":result["state"],"state":"stopped","parked":True}
