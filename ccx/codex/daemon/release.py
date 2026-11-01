from __future__ import annotations

import sys
import time
from typing import Any
from ccx.codex.records import now_ms
from ccx.codex.daemon.models import ReleasePending, _ThreadRelease, _retire_runtime_markers
import ccx.codex.daemon.models as daemon_models
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ccx.codex.daemon.worker import Worker


_RELEASE_FENCE_WAIT_S = 5.0


def _abandon_killed_load(daemon, worker: Worker) -> None:
    """Finish a kill the loader was still racing, durably.

    This is the completion path for a kill that could not confirm — an
    unresolved `turn/start` — as well as the tidy-up for one that could.
    Deregistering alone is not enough: leaving a nonterminal record behind
    would show a killed worker as `busy` forever. So the record is written
    terminal and its transient runtime fields cleared here, exactly
    as a confirmed kill does, before the worker leaves the registries.

    Idempotent, and it never touches another worker's bindings.
    """
    worker.loaded = False
    # `_state` then `_unload` is the same durable order `kill` uses:
    # terminal state first, then turn/approval fields cleared and saved.
    worker._state("dead")
    daemon._unload(worker, owns_lifecycle=True, terminal_state="dead")


def _unload(
    daemon,
    worker: Worker,
    *,
    terminal_state: str,
    owns_lifecycle: bool = False,
) -> None:
    """Take one Worker object out of the daemon and release its threads.

    Exactly one teardown runs per object. Only the registered object is
    torn down: the janitor's snapshot can outlive a kill and a reload, and
    a stale object must not save its record over the fresh worker's, drop
    the fresh worker's bindings or release the thread the fresh worker
    resumed. A second unload of an object already being torn down returns
    at once. The exception is the loader (`owns_lifecycle`): it holds this
    short's lifecycle lock, so no fresh object can exist, and a kill that
    raced its resume released the thread before that resume subscribed
    it, so the loader must release it again.

    The release never waits for Codex. Each thread gets a fence that a
    load of that thread waits on, and that only Codex's answer (or the
    loss of the connection) clears; see `_release_native_threads`.
    """
    # Sequential, never nested inside daemon.lock: lock order is
    # daemon.lock -> worker.lock everywhere else.
    with daemon.lock:
        current = daemon.workers.get(worker.short)
        if owns_lifecycle:
            if current is not None and current is not worker:
                return
        elif current is not worker or worker.teardown_claimed:
            return
        # Lock order is daemon.lock -> worker.lock. A native callback must
        # not change the active-command set between the idle-only check
        # and the final teardown claim.
        with worker.lock:
            if terminal_state == "idle-killed" and not owns_lifecycle:
                if worker.attach_count or worker._bg_open:
                    return
            worker.teardown_claimed = True
        releases = [
            _fence_release_locked(daemon, thread_id)
            for thread_id in sorted(worker.native_threads)
            if daemon.by_thread.get(thread_id, worker.short) == worker.short
        ]
        # Unrouted before the final record write, so no notification or
        # server request looked up from now on can reach this worker; one
        # routed earlier sees `teardown_claimed` under `worker.lock` and
        # changes nothing (see `on_notification`).
        daemon._drop_thread_bindings_locked(worker.short)
    # Once claimed, the teardown always finishes: even if the record write
    # fails, the worker leaves the registry and every fence is sent or
    # settled, since no later unload may claim it again.
    try:
        # The journal is closed next: an unloaded worker observes nothing
        # more, and the close is where its last events reach the disk — so
        # its outcome, including a failed final flush, belongs in the
        # record write below rather than being discovered by nobody.
        with worker.lock:
            # Its last lifecycle facts go in before the journal closes;
            # the save below then finds nothing new to journal.
            worker._journal_lifecycle_locked(
                state=terminal_state, approvals=[]
            )
        worker.evidence.close()
        with worker.lock:
            lifecycle = worker.record["lifecycle"]
            # A notification handled after kill wrote its terminal state
            # but before the claim may have moved the record; the teardown
            # owns it.
            if lifecycle["state"] != terminal_state:
                lifecycle["since"] = now_ms()
            lifecycle["state"] = terminal_state
            lifecycle["activeTurnId"] = None
            worker.record["activeTurnError"] = None
            # A server request cannot survive its JSON-RPC connection scope
            # once the worker leaves the daemon.
            worker.record["pendingApprovals"] = []
            if worker.evidence.observed:
                worker.record["evidence"] = worker.evidence.state()
            worker._save()
    finally:
        with daemon.lock:
            if daemon.workers.get(worker.short) is worker:
                del daemon.workers[worker.short]
            if not daemon.workers:
                daemon.empty_since = time.monotonic()
        daemon._release_native_threads(worker, releases)


def _retire_workers(daemon) -> None:
    """Write every still-registered worker's record for a daemon exit."""
    with daemon.lock:
        remaining = list(daemon.workers.values())
        daemon.workers.clear()
        daemon.by_thread.clear()
    for worker in remaining:
        with worker.lock:
            lifecycle = worker.record["lifecycle"]
            lifecycle["activeTurnId"] = None
            state = lifecycle["state"]
            if state in {"booting", "resuming", "busy", "waiting"}:
                # Work was cut, not finished: the same verdict the next
                # daemon's orphan repair gives a record left this way.
                lifecycle["state"] = "crashed"
                lifecycle["lastError"] = (
                    "codexd stopped while the worker was active")
                lifecycle["since"] = now_ms()
            elif state == "idle":
                # A daemon exit merely unloads an idle worker; `dead` is
                # reserved for an explicit kill.
                lifecycle["state"] = "idle-killed"
                lifecycle["since"] = now_ms()
            elif state not in {"idle-killed", "crashed", "dead", "budget-stopped"}:
                lifecycle["state"] = "dead"
                lifecycle["since"] = now_ms()
            _retire_runtime_markers(
                worker.record,
                "codexd stopped before the compaction was observed")
            # This native app-server is gone. Neither its open command IDs
            # nor foreground candidates can pin the next runtime's janitor.
            worker._lose_native_background_locked()
            # A server request cannot survive the process that owns its
            # JSON-RPC connection.
            worker.record["pendingApprovals"] = []
            worker._save()


def _fence_release_locked(daemon, thread_id: str) -> "_ThreadRelease":
    release = _ThreadRelease(thread_id)
    daemon.releases.setdefault(thread_id, []).append(release)
    return release


def _settle_release(daemon, release: "_ThreadRelease") -> None:
    with daemon.lock:
        fences = daemon.releases.get(release.thread_id) or []
        if release in fences:
            fences.remove(release)
        if not fences:
            daemon.releases.pop(release.thread_id, None)
    release.settled.set()


def _release_native_threads(
    daemon, worker: Worker, releases: list["_ThreadRelease"]
) -> None:
    """Send `thread/unsubscribe` for each fenced thread; never wait for it.

    Forgetting a worker locally is not enough: Codex keeps a thread, and
    any stdio MCP servers it started, loaded for as long as this connection
    stays subscribed, which on a long-lived daemon means forever. Once the
    last subscriber leaves, Codex unloads an idle thread after its own
    `thread_unload_delay_secs`; a later resume reloads it from its rollout.

    Each fence stays until Codex answers or the connection is lost, never
    on a local timeout: a load of the thread waits on it, so a late
    unsubscribe can never land after that load's resume and leave a live
    worker deaf. A refusal only costs memory until the app-server exits,
    so it is logged.

    Sending still writes to the app-server's stdin under the client's
    write lock, so it can wait briefly behind another request's write. It
    blocks for long only if Codex stops reading its input, and then every
    other request, including kill's own `turn/interrupt`, hangs the same
    way, so the send is not moved to another thread.
    """
    client = daemon.client
    for release in releases:
        if client is None:
            _settle_release(daemon, release)
            continue
        try:
            pending = client.begin_request(
                "thread/unsubscribe", {"threadId": release.thread_id}
            )
        except Exception as error:  # noqa: BLE001 - must not block unload
            _log_release_failure(daemon, worker, release.thread_id, error)
            _settle_release(daemon, release)
            continue
        pending.add_done_callback(
            lambda answered, release=release: _on_release_answered(daemon, worker, release, answered
            )
        )


def _on_release_answered(
    daemon, worker: Worker, release: "_ThreadRelease", answered: Any
) -> None:
    # Runs on the reader thread: no app-server request may be made here.
    try:
        answered.result()
    except Exception as error:  # noqa: BLE001
        _log_release_failure(daemon, worker, release.thread_id, error)
    finally:
        _settle_release(daemon, release)


def _log_release_failure(daemon, worker: Worker, thread_id: str, error: Any) -> None:
    print(
        f"ccx codexd: could not release Codex thread {thread_id} "
        f"of worker {worker.short}: {error}",
        file=sys.stderr, flush=True,
    )


def _await_thread_release(daemon, thread_id: str | None) -> None:
    """Wait briefly for Codex to answer an earlier release of `thread_id`.

    Raises `ReleasePending` rather than let a resume of the thread race an
    unsubscribe Codex has not answered yet.
    """
    if not thread_id:
        return
    deadline = time.monotonic() + _RELEASE_FENCE_WAIT_S
    while True:
        with daemon.lock:
            fences = list(daemon.releases.get(thread_id) or [])
        if not fences:
            return
        for fence in fences:
            if not fence.settled.wait(max(0.0, deadline - time.monotonic())):
                raise ReleasePending(
                    f"Codex has not yet confirmed releasing thread "
                    f"{thread_id} from an earlier unload; retry shortly"
                )


def _janitor(daemon) -> None:
    while not daemon.stop.wait(1):
        # The janitor is the only thing that idle-kills workers and ends
        # the linger. An exception escaping it would end both silently and
        # keep codexd and its app-server alive forever, so every step is
        # isolated and logged.
        try:
            with daemon.lock:
                snapshot = list(daemon.workers.values())
            for worker in snapshot:
                try:
                    daemon._janitor_worker(worker)
                except Exception as error:  # noqa: BLE001
                    _log_janitor_failure(daemon, f"worker {worker.short}", error)
            daemon._janitor_native_start_pass()
            if daemon._janitor_daemon_pass():
                return
        except Exception as error:  # noqa: BLE001
            _log_janitor_failure(daemon, "pass", error)


def _log_janitor_failure(daemon, what: str, error: BaseException) -> None:
    print(f"ccx codexd: janitor {what} failed: {error!r}",
          file=sys.stderr, flush=True)


def _janitor_worker(daemon, worker: Worker) -> None:
    if worker.load_lock.locked():
        return
    worker.flush_context()
    with worker.lock:
        state = worker.record["lifecycle"]["state"]
        timeout = int(
            worker.record["createArgs"].get("idleTimeout", 1800)
        )
        expired = (
            state == "idle"
            and timeout > 0
            and worker.attach_count == 0
            and not worker._bg_open
            and time.monotonic() - worker.last_activity >= timeout
        )
    if expired:
        if worker.kill_intent.is_set() or worker.killed.is_set():
            # An explicit kill owns this worker's terminal record;
            # idle-kill must not write `idle-killed` over it.
            return
        if not worker.operation_lock.acquire(blocking=False):
            return
        try:
            if (worker.kill_intent.is_set() or worker.killed.is_set()
                    or not daemon._still_registered(worker)):
                return
            with worker.lock:
                state = worker.record["lifecycle"]["state"]
                timeout = int(
                    worker.record["createArgs"].get(
                        "idleTimeout", 1800
                    )
                )
                expired = (
                    state == "idle"
                    and timeout > 0
                    and worker.attach_count == 0
                    and not worker._bg_open
                    and time.monotonic() - worker.last_activity >= timeout
                )
            if expired:
                daemon._idle_kill(worker)
                return
        finally:
            worker.operation_lock.release()
    if state in {"idle-killed", "crashed", "dead"}:
        daemon._unload(worker, terminal_state=state)


def _janitor_daemon_pass(daemon) -> bool:
    """Linger; True once the daemon should stop."""
    with daemon.lock:
        with daemon._native_start_hold_lock:
            # In-flight originals and safely held positive replies keep
            # the daemon alive until a definitive provider result or fsync.
            # Unsafe diagnostic history does not pin the singleton.
            native_pending = bool(daemon._native_start_held)
        if daemon.workers or native_pending:
            daemon.empty_since = None
            expired = False
        else:
            if daemon.empty_since is None:
                daemon.empty_since = time.monotonic()
            expired = (
                time.monotonic() - daemon.empty_since >= daemon_models.linger_seconds()
            )
            if expired:
                daemon.stopping = True
    if expired:
        daemon.stop.set()
    return expired
