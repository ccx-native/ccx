from __future__ import annotations

from ccx.version import ccx_version

import argparse
import json
import os
import queue
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid
from copy import deepcopy
from typing import Any
from ccx.codex.evidence import DELIVERY_CHANGED_METHOD, INPUT_RESOLVED_METHOD, LIFECYCLE_METHOD, EvidenceJournal, journal_path
from ccx.codex.paths import codexd_sock_path
from ccx.codex.protocol import AppServerClient, ProtocolError
from ccx.codex.records import (
    iter_records,
    now_ms,
    read_record,
)
from ccx.constants import CODEXD_RECOVERY_OPS, CODEXD_RPC_GENERATION
from ccx.store.paths import accept_same_user, bind_private_unix_socket
from ccx.codex.daemon.models import ReleasePending, _LAUNCH_SETTLE_CAP_S, _ThreadRelease, _bounded, _native_history_budget, _protocol_failure, _retire_runtime_markers
from ccx.codex.daemon.worker import Worker
import ccx.codex.daemon.native as daemon_native
import ccx.codex.daemon.release as daemon_release
import ccx.codex.daemon.models as daemon_models


STABLE_STATES = {"idle", "waiting", "idle-killed", "crashed", "dead"}


LIVE_STATES = {"booting", "resuming", "idle", "busy", "waiting"}


RATE_LIMIT_CACHE_TTL_S = 30.0


CAP_ADMISSION_READ_TIMEOUT_S = 1.0  # Preserve the caller's short native-send RPC budget.


DAEMON_DIAGNOSTIC_NOTIFICATIONS = {
    "deprecationNotice",
    "warning",
    "guardianWarning",
    "configWarning",
    "model/rerouted",
    "model/verification",
}


def _merge_non_null(old: Any, incoming: Any) -> Any:
    """Merge one sparse native update without letting null clear known data."""

    if incoming is None:
        return deepcopy(old)
    if isinstance(old, dict) and isinstance(incoming, dict):
        merged = deepcopy(old)
        for key, value in incoming.items():
            if value is None:
                continue
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key] = _merge_non_null(merged[key], value)
            else:
                merged[key] = deepcopy(value)
        return merged
    return deepcopy(incoming)


def _rate_limit_reset_passed(response: Any) -> bool:
    """Return whether a known primary or secondary reset boundary is past."""

    rate_limits = response.get("rateLimits") if isinstance(response, dict) else None
    if not isinstance(rate_limits, dict):
        return False
    now = time.time()
    for name in ("primary", "secondary"):
        window = rate_limits.get(name)
        if not isinstance(window, dict):
            continue
        resets_at = window.get("resetsAt")
        if (
            isinstance(resets_at, (int, float))
            and not isinstance(resets_at, bool)
            and resets_at <= now
        ):
            return True
    return False


def _probe_codex_version(codex_bin: str, *, timeout: float = 5) -> str | None:
    """Return the raw Codex version when the optional probe succeeds."""

    try:
        completed = subprocess.run(
            [codex_bin, "--version"],
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if completed.returncode != 0:
        return None
    output = completed.stdout.strip()
    return output or None


def _thread_id(params: dict[str, Any]) -> str | None:
    """Extract the routing thread id from any stable-API server message."""
    value = params.get("threadId")
    if isinstance(value, str) and value:
        return value
    thread = params.get("thread")
    if isinstance(thread, dict) and isinstance(thread.get("id"), str):
        return thread["id"]
    return None


def _journal_orphan_repair(
    record: dict[str, Any],
    previous: str | None,
    codex_version: str | None,
) -> None:
    """Journal the transition an orphan repair wrote, as a loaded worker would.

    The dead daemon never journaled it, so without this the event feed would
    keep showing the worker in its last live state. The lines are the ones
    ``Worker._journal_lifecycle_locked`` writes: one ``input/resolved`` per
    approval the repair dropped, then one ``lifecycle`` line. A worker with no
    journal yet gets none, as it would not have been loaded with one.
    """
    short = record["workerId"]
    if not journal_path(short).exists():
        return
    lifecycle = record.get("lifecycle") or {}
    thread_id = (record.get("native") or {}).get("threadId")
    journal = EvidenceJournal(
        short, codex_version=codex_version, health=record.get("evidence"),
    )
    try:
        for approval in record.get("pendingApprovals") or []:
            if isinstance(approval, dict):
                journal.record(INPUT_RESOLVED_METHOD, {
                    "requestId": approval.get("requestId"),
                    "gateId": approval.get("gateId"),
                    "kind": approval.get("kind"),
                    "decision": None,
                }, thread_id=thread_id)
        journal.record(LIFECYCLE_METHOD, {
            "from": previous,
            "to": lifecycle.get("state"),
            "reason": lifecycle.get("lastError"),
        }, thread_id=thread_id)
    finally:
        journal.close()
    record["evidence"] = journal.state()


class CodexDaemon:
    def __init__(self):
        self.lock = threading.RLock()
        # Durable create markers use this nonce to distinguish "the native
        # request may still complete later" from "the owning app-server is gone".
        self.instance_id = uuid.uuid4().hex
        # Per-daemon bounded one-shot pending/positive Codex creation proofs.
        # Lock only this map briefly; never hold it while acquiring the store
        # generation lease/effect lock or waiting for app-server responses.
        self._native_start_hold_lock = threading.Lock()
        self._native_start_held: dict[tuple[str, str, str], dict[str, Any]] = {}
        # Unsafe original evidence is diagnostic only: it cannot be repaired,
        # but must not permanently consume capacity reserved for recoverable
        # responses. Old diagnostics roll off at the bounded cap; never evict a
        # safe unsaved positive reply or an unanswered original native start.
        self._native_start_unsafe: dict[tuple[str, str, str], str] = {}
        self._native_start_unsafe_total = 0
        self.workers: dict[str, Worker] = {}
        self.by_thread: dict[str, str] = {}
        self.client: AppServerClient | None = None
        self.app_server_path = None
        self.app_server_inode = None
        self.codex_version: str | None = None
        self.diagnostics: list[dict[str, Any]] = []
        self._seen_diagnostics: set[tuple[str, str, str | None]] = set()
        self.ready = threading.Event()
        self.stop = threading.Event()
        # Set under `self.lock` in the same critical section that decides to
        # stop, and never cleared. From then on no worker is registered, so a
        # load cannot start on a runtime about to close; callers see
        # `stopping` and wait for the next daemon instead.
        self.stopping = False
        self.started_at = time.monotonic()
        self.empty_since: float | None = time.monotonic()
        # Native thread id -> unsubscribes Codex has not answered yet.
        self.releases: dict[str, list[_ThreadRelease]] = {}
        self.lifecycle_locks: dict[str, threading.Lock] = {}
        self.server: socket.socket | None = None
        self.accepter: threading.Thread | None = None
        self.rate_limits_cache: dict[str, Any] | None = None
        self.rate_limits_read_at: float | None = None
        # Lost-acknowledgment recovery from journaled user messages. Its
        # receipt and native-intent locks can be held by a dispatching caller
        # waiting on this app-server, so it never runs on the reader thread.
        self._recovery_queue: "queue.Queue[tuple[Worker, str]]" = queue.Queue()
        self._recovery_thread: threading.Thread | None = None
        self._recovery_done: set[tuple[str, str]] = set()

    def queue_delivery_recovery(self, worker: "Worker", client_id: str) -> None:
        with self.lock:
            if self._recovery_thread is None:
                self._recovery_thread = threading.Thread(
                    target=self._delivery_recovery_loop, daemon=True,
                    name="ccx-codex-delivery-recovery",
                )
                self._recovery_thread.start()
        self._recovery_queue.put((worker, client_id))

    def _delivery_recovery_loop(self) -> None:
        while not self.stop.is_set():
            try:
                worker, client_id = self._recovery_queue.get(timeout=.5)
            except queue.Empty:
                continue
            try:
                self._recover_delivery(worker, client_id)
            except Exception as error:  # noqa: BLE001 - one delivery, never the loop
                print("ccx codexd: delivery recovery deferred: "
                      + type(error).__name__ + ": " + str(error)[:200],
                      file=sys.stderr, flush=True)

    def _recover_delivery(self, worker: "Worker", client_id: str) -> None:
        """Recover one journaled user message's delivery, then wake readers.

        Retries only while a lock is held (a dispatching caller owns the
        native intent for at most its bounded dispatch), never on missing or
        ambiguous evidence. The wake line is written strictly after the
        receipt is accepted, so a reader that sees it reconciles a final
        answer; a reader that read earlier sees this later line.
        """
        from ccx.codex.delivery_recovery import delivery_for_client, recover
        from ccx.store.root import GenerationBusy
        key = (worker.worker_id, client_id)
        if key in self._recovery_done:
            return
        handle = f"codex:{worker.worker_id}"
        receipt = None
        for _attempt in range(40):
            store = self._uuid_store
            try:
                store.revalidate()
                delivery = delivery_for_client(handle, client_id)
                if delivery is None:
                    return
                receipt = recover(store, handle, delivery)
                effect = store.read_receipt(handle, "codex-send", delivery)
            except GenerationBusy:
                receipt = None
            else:
                if not isinstance(receipt, dict) or receipt.get("state") != "accepted":
                    return  # Unknown stays unknown; a later item may retry.
                if not (isinstance(effect, dict)
                        and effect.get("state") == "submitted-unknown"):
                    break
            if self.stop.wait(.25):
                return
        if not isinstance(receipt, dict) or receipt.get("state") != "accepted":
            return
        self._recovery_done.add(key)
        with worker.lock:
            if worker.teardown_claimed or worker.evidence.closed:
                return
            worker.evidence.record(DELIVERY_CHANGED_METHOD, {
                "deliveryId": receipt.get("deliveryId"),
                "state": receipt.get("state"),
                "route": receipt.get("route"),
                "placement": receipt.get("placement"),
                # Deliberately no top-level turnId: feed lines never claim
                # native turn evidence (build_index and settlement skip them).
                "turnRef": receipt.get("turnRef"),
            }, thread_id=(worker.record.get("native") or {}).get("threadId"))

    def _record_diagnostic(
        self, method: str, params: dict[str, Any]
    ) -> None:
        """Keep a bounded daemon-global diagnostic and log its useful text."""
        summary = params.get("summary") or params.get("message")
        if not isinstance(summary, str) or not summary:
            if method == "model/rerouted":
                summary = (
                    f"model rerouted from {params.get('fromModel')!s} "
                    f"to {params.get('toModel')!s}"
                )
            elif method == "model/verification":
                summary = "Codex model verification update"
            else:
                summary = f"Codex {method} notification"
        details = params.get("details")
        if not isinstance(details, str) or not details:
            details = None
        entry = {
            "method": method,
            "summary": _bounded(summary),
            "details": _bounded(details),
            "codexVersion": self.codex_version,
            "observedAtMs": now_ms(),
        }
        with self.lock:
            key = (method, entry["summary"], entry["details"])
            if key in self._seen_diagnostics:
                return
            self._seen_diagnostics.add(key)
            self.diagnostics.append(entry)
            del self.diagnostics[:-20]
        log = f"ccx codexd: {method}: {entry['summary']}"
        if entry["details"]:
            log += f" — {entry['details']}"
        print(log, file=sys.stderr, flush=True)

    def bind_thread(self, thread_id: str, short: str) -> None:
        with self.lock:
            self.by_thread[thread_id] = short

    def claim_thread(self, thread_id: str, short: str) -> None:
        """Bind a thread a load is about to resume, or refuse it.

        The subscription belongs to the shared connection, so one native
        thread has at most one live owner. The fence check and the binding are
        one step under `self.lock`: a teardown that fences the thread later
        can only be the owner's own, and an unanswered release from an earlier
        owner refuses the claim instead of racing it.
        """
        with self.lock:
            if self.releases.get(thread_id):
                raise ReleasePending(
                    f"Codex has not yet confirmed releasing thread "
                    f"{thread_id} from an earlier unload; retry shortly"
                )
            owner = self.by_thread.get(thread_id)
            if owner is not None and owner != short:
                raise ProtocolError(
                    f"native Codex thread {thread_id} is already loaded by "
                    f"worker {owner}"
                )
            self.by_thread[thread_id] = short

    def unbind_thread(self, thread_id: str) -> None:
        with self.lock:
            self.by_thread.pop(thread_id, None)

    def _drop_thread_bindings_locked(self, short: str) -> None:
        """Remove every thread binding owned by one worker. Caller holds the lock."""
        for thread_id, owner in list(self.by_thread.items()):
            if owner == short:
                del self.by_thread[thread_id]

    def _lifecycle_lock(self, short: str) -> threading.Lock:
        with self.lock:
            lock = self.lifecycle_locks.get(short)
            if lock is None:
                lock = threading.Lock()
                self.lifecycle_locks[short] = lock
            return lock

    def _idle_kill(self, worker: Worker) -> None:
        # Unload only. The native thread stays unarchived so Codex clients and
        # native ops such as thread/fork can still use it.
        self._unload(worker, terminal_state="idle-killed")

    def _worker_for(self, params: dict[str, Any]) -> Worker | None:
        thread_id = _thread_id(params)
        if not thread_id:
            return None
        with self.lock:
            short = self.by_thread.get(thread_id)
            return self.workers.get(short) if short else None

    def _cached_rate_limits(self) -> dict[str, Any] | None:
        with self.lock:
            if self.rate_limits_cache is None or self.rate_limits_read_at is None:
                return None
            if time.monotonic() - self.rate_limits_read_at >= RATE_LIMIT_CACHE_TTL_S:
                return None
            if _rate_limit_reset_passed(self.rate_limits_cache):
                return None
            return deepcopy(self.rate_limits_cache)

    def _cap_data_from_cache(self) -> dict | None:
        """Prefer the push-merged general bucket over an older named snapshot."""
        cached = self._cached_rate_limits()
        if (isinstance(cached, dict)
                and isinstance(cached.get("rateLimits"), dict)
                and cached["rateLimits"].get("limitId") in (None, "codex")):
            return {"rateLimits": cached["rateLimits"]}
        return cached

    def _cap_at_admission(self, caps: dict | None):
        """Preflight known quota; one short native read on a cold cache.

        Called only by the native-send request handler, not the app-server
        notification reader. An unavailable quota signal must never block a turn.
        """
        from ccx.codex.types import codex_cap_tripped
        data = self._cap_data_from_cache()
        if data is None and caps and self.client is not None:
            try:
                self._read_rate_limits(timeout=CAP_ADMISSION_READ_TIMEOUT_S)
                data = self._cap_data_from_cache()
            except Exception:
                # Account quota observations are advisory; fail open.
                pass
        return codex_cap_tripped(data, caps)

    def _merge_rate_limits_update(self, params: dict[str, Any]) -> None:
        incoming = params.get("rateLimits")
        if not isinstance(incoming, dict) or incoming.get("limitId") not in (None, "codex"):
            return
        with self.lock:
            if self.rate_limits_cache is None:
                self.rate_limits_cache = {"rateLimits": deepcopy(incoming)}
                self.rate_limits_read_at = time.monotonic()
                return
            self.rate_limits_cache["rateLimits"] = _merge_non_null(
                self.rate_limits_cache.get("rateLimits"), incoming
            )

    def _read_rate_limits(self, *, timeout: float = 30.0) -> Any:
        cached = self._cached_rate_limits()
        if cached is not None:
            return cached
        result = self.client.request("account/rateLimits/read", timeout=timeout)
        if isinstance(result, dict):
            with self.lock:
                self.rate_limits_cache = deepcopy(result)
                self.rate_limits_read_at = time.monotonic()
            return deepcopy(result)
        return result

    @staticmethod
    def _persistence_failed(worker: Worker, method: str, error: BaseException) -> None:
        """Degrade one worker, never mistake a store fault for wire failure."""
        worker.persistence_error = {
            "method": method,
            "error": str(error),
            "kind": type(error).__name__,
            "observedAtMs": now_ms(),
        }
        print(
            f"ccx codexd: could not persist worker {worker.short} after "
            f"{method}: {error}",
            file=sys.stderr, flush=True,
        )

    def _contain_store_failure(
        self, worker: Worker, method: str, error: BaseException
    ) -> None:
        from ccx.store.root import StaleGeneration, GenerationUnavailable
        self._persistence_failed(worker, method, error)
        if isinstance(error, (StaleGeneration, GenerationUnavailable)):
            # Not a per-worker rebase: the pinned generation is no longer
            # usable. Close global admission rather than trying a new root.
            with self.lock:
                self.stopping = True
            self.stop.set()

    def _on_notification(self, method: str, params: dict[str, Any]) -> None:
        # Global diagnostics have no reliable worker ownership (notably
        # deprecationNotice), so capture them before attempting thread routing.
        if method in DAEMON_DIAGNOSTIC_NOTIFICATIONS:
            self._record_diagnostic(method, params)
            return
        if method == "account/rateLimits/updated":
            self._merge_rate_limits_update(params)
            merged = self._cap_data_from_cache()
            with self.lock:
                workers = list(self.workers.values())
            for active in workers:
                active.check_active_caps(params)
                if merged is not None:
                    active.check_active_caps(merged)
            return
        worker = self._worker_for(params)
        if worker is not None:
            from ccx.store.root import RecordConflict, GenerationBusy, StaleGeneration, GenerationUnavailable, StorageIntegrityError
            try:
                # A native notification is not allowed to write even its
                # journal after a generation flip.
                self._uuid_store.revalidate()
                worker.on_notification(method, params)
            except (OSError, RecordConflict, GenerationBusy, StaleGeneration,
                    GenerationUnavailable, StorageIntegrityError) as error:
                self._contain_store_failure(worker, method, error)
        # Unknown and intentionally unused notifications are additive data.
        # They do not belong in worker records and are intentionally a no-op.

    def _on_server_request(
        self, request_id: Any, method: str, params: dict[str, Any]
    ) -> None:
        worker = self._worker_for(params)
        if worker is not None:
            from ccx.store.root import RecordConflict, GenerationBusy, StaleGeneration, GenerationUnavailable, StorageIntegrityError
            try:
                self._uuid_store.revalidate()
                worker.on_server_request(request_id, method, params)
            except (OSError, RecordConflict, GenerationBusy, StaleGeneration,
                    GenerationUnavailable, StorageIntegrityError) as error:
                # A failed pending-approval CAS never becomes an in-memory
                # authorization. The native request is explicitly rejected;
                # silence would leave a hanging approval or a false gate.
                with worker.lock:
                    worker.record = deepcopy(worker._disk_baseline)
                self._contain_store_failure(worker, method, error)
                if self.client is not None:
                    self.client.reject_request(
                        request_id, "CCX could not durably admit this approval"
                    )
        elif self.client is not None:
            self.client.reject_request(
                request_id, f"CCX has no handler for app-server request {method}"
            )

    def _start_client(self) -> None:
        from ccx.system.engine_binary import resolve_engine_binary
        codex_bin = resolve_engine_binary("codex")
        # Version is useful factual metadata, but it is not a production
        # permission check.  In particular, an unfamiliar or failing probe
        # must not prevent a working app-server from being attempted.
        self.codex_version = _probe_codex_version(codex_bin)
        import stat
        app_path = codexd_sock_path().with_name("app.sock")
        # The existing codexd transport plan supplies the short private
        # directory (including Darwin's 104-byte Unix socket limit).
        try:
            prior = os.lstat(app_path)
        except FileNotFoundError:
            prior = None
        if prior is not None:
            if (prior.st_uid != os.getuid()
                    or not (stat.S_ISSOCK(prior.st_mode)
                            or stat.S_ISLNK(prior.st_mode))):
                raise RuntimeError("untrusted old Codex app-server socket")
            # A macOS Codex listener is a symlink; unlink the pathname
            # itself, never its target, under the existing singleton lease.
            os.unlink(app_path)
        self.app_server_path = app_path
        self.client = AppServerClient(
            [codex_bin, "app-server", "--listen", f"unix://{app_path}"],
            socket_path=str(app_path),
            cwd=str(app_path.parent),
            on_notification=self._on_notification,
            on_server_request=self._on_server_request,
            stderr=None,
        )
        bound = os.lstat(app_path)
        if (bound.st_uid != os.getuid()
                or not (stat.S_ISSOCK(bound.st_mode) or stat.S_ISLNK(bound.st_mode))):
            raise RuntimeError("untrusted Codex app-server endpoint")
        # Codex 0.165 on macOS exposes a symlink to its actual socket. Its
        # pathname is in our existing private 0700 runtime directory; do not
        # follow that link to mutate the native target.
        if stat.S_ISSOCK(bound.st_mode):
            os.chmod(app_path, 0o600)
        self.app_server_inode = (bound.st_dev, bound.st_ino)
        result = self.client.request("initialize", {
            "clientInfo": {
                "name": "ccx",
                "title": "CCX shared Codex daemon",
                "version": ccx_version(),
            },
            "capabilities": {"experimentalApi": False},
        })
        if result.get("userAgent") is None:
            # The field is not semantically required, but an empty result usually
            # means a mismatched/non-Codex endpoint.
            result.setdefault("userAgent", "codex")
        self.client.notify("initialized")

    def _reconcile_orphan_records(self) -> None:
        """Repair durable nonterminal states left by a previous daemon lifetime.

        The singleton lease proves no other codexd serves this root, and the
        old app-server died with its stdio pipe, so a recorded active turn
        cannot still be running: end it as a crash, the way a graceful stop
        ends an active worker.
        """
        for candidate in iter_records():
            short = candidate.get("workerId")
            if not isinstance(short, str) or not short:
                continue
            try:
                self._reconcile_orphan_record(short)
            except Exception as error:
                # One unreadable record must not keep codexd from starting.
                print(f"ccx codexd: orphan repair skipped {short}: {error}",
                      file=sys.stderr, flush=True)

    def _reconcile_orphan_record(self, short: str) -> None:
        diagnostic = "codexd exited without unloading this worker"
        with self._lifecycle_lock(short):
            record = read_record(short)
            if record is None:
                return
            before = deepcopy(record)
            lifecycle = record.setdefault("lifecycle", {})
            state = lifecycle.get("state")
            # Whatever the state (a graceful stop may already have written
            # a terminal one), the old app-server can no longer settle a
            # compact or an ambiguous turn/start.
            markers = _retire_runtime_markers(
                record, "codexd exited before the compaction was observed")
            # An idle record needs nothing: an unloaded idle worker resumes.
            if state in {"busy", "waiting", "resuming"}:
                lifecycle["state"] = "crashed"
                lifecycle["lastError"] = diagnostic
            elif not markers:
                return
            if lifecycle["state"] != state:
                lifecycle["since"] = now_ms()
                lifecycle["activeTurnId"] = None
                record["activeTurnError"] = None
                _journal_orphan_repair(
                    record, state, getattr(self, "codex_version", None))
                record["pendingApprovals"] = []
            # Field CAS, as a loaded Worker saves: the expected state fences
            # any writer that moved the record since it was read.
            patches = {
                f"lifecycle.{key}": value for key, value in lifecycle.items()
                if (before.get("lifecycle") or {}).get(key) != value
            }
            patches.update({
                key: value for key, value in record.items()
                if key != "lifecycle" and before.get(key) != value
            })
            self._uuid_store.mutate_record(
                f"codex:{short}", patches=patches,
                expect={"lifecycle.state": state},
            )

    def set_config(self, target: str, message: dict[str, Any]) -> dict[str, Any]:
        """Durable full owner config patch, no stale loaded/unloaded snapshot."""
        from ccx.store.identity import EngineMismatch
        from ccx.store.root import GenerationError
        try:
            ctx=self._uuid_store
            owner=ctx.resolve(target)
            if owner.engine!="codex":
                raise EngineMismatch("Codex config requires full Codex UUID")
            changes=message.get("updates")
            if not isinstance(changes,dict) or not changes:
                return {"ok":False,"error":"set-config requires updates"}
            invalid=set(changes)-{"model","effort"}
            if invalid:
                return {"ok":False,"error":
                        f"unsupported config fields: {sorted(invalid)}"}
            patches={}
            for field,value in changes.items():
                if value is not None and (not isinstance(value,str)
                                          or not value.strip()):
                    return {"ok":False,"error":
                            f"{field} must be nonempty text or null"}
                patches[f"createArgs.{field}"]=(
                    value.strip() if isinstance(value,str) else None
                )
            with self._lifecycle_lock(owner.worker_id):
                with self.lock:
                    loaded=self.workers.get(owner.worker_id)
                if loaded is not None:
                    # Native turn/start owns this same worker.lock when it
                    # publishes busy/launch. Hold it through fresh CAS and
                    # mirror; never overwrite an unsaved in-memory turn.
                    with loaded.lock:
                        live_state=(loaded.record.get("lifecycle") or {}).get("state")
                        if (live_state!="idle" or not loaded.loaded
                                or loaded.launch is not None
                                or loaded.killed.is_set()
                                or loaded.kill_intent.is_set()):
                            return {"ok":False, "error":
                                    f"next-turn configuration requires an idle or unloaded worker; "
                                    f"worker is {live_state}", "state":live_state}
                        fresh=ctx.mutate_record(owner.worker,patches=patches)
                        loaded.record=deepcopy(fresh)
                        loaded._disk_baseline=deepcopy(fresh)
                else:
                    # Daemon owns config even for unloaded workers. Reject
                    # unresolved native states, never change a busy record
                    # simply because the Worker object is momentarily absent.
                    current=ctx.record(owner.worker)
                    state=(current.get("lifecycle") or {}).get("state")
                    if state in ("busy", "waiting", "booting", "resuming"):
                        return {"ok":False,"error":
                                f"next-turn configuration requires an idle or unloaded worker; "
                                f"worker is {state}", "state":state}
                    fresh=ctx.mutate_record(owner.worker,patches=patches)
                args=fresh.get("createArgs") or {}
                return {"ok":True,
                        "state":(fresh.get("lifecycle") or {}).get("state"),
                        "model":args.get("model"),"effort":args.get("effort")}
        except (GenerationError,ValueError) as error:
            return {"ok":False,"error":str(error)}

    def set_name(self, target: str, message: dict[str, Any]) -> dict[str, Any]:
        """One canonical full-owner rename, independent lifecycle CAS."""
        from ccx.store.root import GenerationError
        from ccx.store.identity import EngineMismatch
        name=message.get("name")
        if not isinstance(name,str) or not name.strip():
            return {"ok":False,"error":"set-name requires nonempty name"}
        try:
            ctx=self._uuid_store
            owner=ctx.resolve(target)
            if owner.engine!="codex":
                raise EngineMismatch("Codex rename requires full Codex UUID")
            with self._lifecycle_lock(owner.worker_id):
                updated=ctx.rename_worker(owner.worker,engine="codex",name=name)
                with self.lock:
                    loaded=self.workers.get(owner.worker_id)
                if loaded is not None:
                    with loaded.lock:
                        loaded.record=deepcopy(updated)
                        loaded._disk_baseline=deepcopy(updated)
                return {"ok":True,"loaded":loaded is not None,
                        "name":updated["name"]}
        except (GenerationError,ValueError) as error:
            return {"ok":False,"error":str(error)}

















    def runtime_failure(self) -> str | None:
        """Why the shared Codex runtime is unusable, if it is.

        The app-server subprocess outliving its protocol channel is the
        dangerous case: the reader latches a transport failure and stops
        consuming notifications, so every loaded worker silently stops being
        updated while ``process.poll()`` still reports a healthy child. Both
        conditions are fatal to the shared runtime and must reach the
        supervisor.
        """
        client = self.client
        if client is None:
            return None
        if client.process.poll() is not None:
            return "Codex app-server exited"
        failure = client.failure
        return str(failure) if failure is not None else None















    def handle(self, message: dict[str, Any]) -> dict[str, Any]:
        op = message.get("op")
        if op == "daemon-status":
            with self.lock:
                workers = {
                    short: worker.state()
                    for short, worker in self.workers.items()
                }
                empty_for = (
                    time.monotonic() - self.empty_since
                    if self.empty_since is not None
                    else None
                )
            return {
                "ok": True,
                "state": (
                    "stopping" if self.stopping
                    else "ready" if self.ready.is_set() else "booting"
                ),
                "runtimeId": self.instance_id,
                "pid": os.getpid(),
                "appServerPid": (
                    self.client.process.pid if self.client else None
                ),
                "codexVersion": self.codex_version,
                "diagnostics": list(self.diagnostics),
                "workers": workers,
                "lingerSeconds": daemon_models.linger_seconds(),
                "emptyForSeconds": empty_for,
                "nativeStartEvidence": self._native_start_counts(),
            }
        if op == "daemon-stop":
            with self.lock:
                states = {
                    short: worker.state()
                    for short, worker in self.workers.items()
                }
                active = {
                    short: state for short, state in states.items()
                    if state in {"booting", "resuming", "busy", "waiting"}
                }
                # Admission reserves its slot under this same lock, so a new
                # original start is either counted here or sees `stopping`.
                # Unsafe-only diagnostics are unrecoverable and do not pin.
                evidence = self._native_start_counts()
                native_unsettled = evidence["pending"] + evidence["held"]
                if (active or native_unsettled) and not message.get("force"):
                    reasons = []
                    if active:
                        reasons.append("workers are still active: " + ", ".join(
                            f"{short} ({state})"
                            for short, state in sorted(active.items())
                        ))
                    if native_unsettled:
                        reasons.append(
                            "original Codex native starts are unsettled "
                            f"({evidence['pending']} pending, {evidence['held']} "
                            "held unsaved); stopping would lose their only "
                            "recovery evidence"
                        )
                    return {
                        "ok": False,
                        "error": "; ".join(reasons),
                        "workers": states,
                        "nativeStartEvidence": evidence,
                    }
                # One decision with the activity check: a load can no longer
                # register behind it.
                self.stopping = True
            self.stop.set()
            return {"ok": True, "stopped": True, "workers": states,
                    "nativeStartEvidence": evidence}
        if op == "thread-history":
            from ccx.codex.history import read_original_thread
            from ccx.store.root import GenerationError
            from ccx.store.identity import IdentityError
            try:
                owner, tid, source = daemon_native._original_history_binding(self, message)
                if not self.ready.is_set() or self.client is None:
                    raise RuntimeError("Codex app-server not ready")
                deadline = time.monotonic() + _native_history_budget(message)
                thread = read_original_thread(
                    self.client.request, tid, deadline=deadline,
                    exact_turn_id=message.get("turnId"), expected_source=source,
                    limit=message.get("limit"), newest_first=message.get("newestFirst") is True,
                )
                if thread.get("threadSource") != source:
                    raise ValueError("native source mismatch, no provider thread data")
                self._uuid_store.revalidate()
                return {"ok": True, "workerId": owner.worker_id,
                        "threadId": tid, "thread": thread}
            except (GenerationError, IdentityError, ValueError, RuntimeError,
                    ProtocolError, TimeoutError) as error:
                return {"ok": False, "error": str(error)}
        if op == "thread-read":
            # Shared immutable original-owner proof; thread/read additionally
            # checks the returned native thread object before exposing turns.
            from ccx.store.root import GenerationError
            from ccx.store.identity import IdentityError
            try:
                owner, tid, source = daemon_native._original_history_binding(self, message)
                if not self.ready.is_set() or self.client is None:
                    raise RuntimeError("Codex app-server not ready")
                deadline = time.monotonic() + _native_history_budget(message)
                reply = self.client.request("thread/read", {
                    "threadId": tid,
                    "includeTurns": False,
                }, timeout=max(0.001, deadline - time.monotonic()))
                thread = reply.get("thread") if isinstance(reply, dict) else None
                if (not isinstance(thread, dict)
                        or thread.get("id") != tid
                        or thread.get("threadSource") != source):
                    raise ValueError("native source mismatch, no provider thread data")
                # Older editable-install clients still use thread-read and
                # default includeTurns to true. Reconstruct those turns from
                # pages without issuing deprecated full-history reads.
                if message.get("includeTurns", True):
                    from ccx.codex.history import fill_thread
                    thread = fill_thread(thread, self.client.request, deadline=deadline)
                self._uuid_store.revalidate()
                return {"ok": True, "workerId": owner.worker_id,
                        "threadId": tid, "thread": thread}
            except (GenerationError, IdentityError, ValueError,
                    RuntimeError, ProtocolError, TimeoutError) as error:
                return {"ok": False, "error": str(error)}
        if op in {"native-activate", "native-send", "native-observe",
                  "native-activation-status"}:
            try:
                if op == "native-activation-status":
                    return self._observe_original_codex_thread(message)
                return self._original_full_uuid_native(message)
            except Exception as error:
                return {"ok": False, "error": str(error)}
        if op == "thread-list":
            # A provider-native thread catalogue is never a CCX ownership index.
            return {"ok": False, "error":
                    "unscoped native Codex thread discovery forbidden; "
                    "use original full UUID workerId"}
        if op in {"thread-items-list", "thread-turns-list"}:
            from ccx.store.root import GenerationError
            from ccx.store.identity import IdentityError, UnknownWorker
            try:
                params = message.get("params")
                if not isinstance(params, dict):
                    raise ValueError("thread-items-list requires native item params")
                turns = op == "thread-turns-list"
                allowed = ({"threadId", "limit", "sortDirection", "cursor", "itemsView"}
                           if turns else {"threadId", "limit", "sortDirection", "cursor", "turnId"})
                if (set(params) - allowed
                        or type(params.get("limit")) is not int
                        or not 1 <= params["limit"] <= 100
                        or (not isinstance(params.get("sortDirection"), str)
                            or params["sortDirection"] not in {"asc", "desc"})
                        or ("cursor" in params and
                            (not isinstance(params["cursor"], str) or not params["cursor"]))
                        or (turns and (not isinstance(params.get("itemsView"), str)
                            or params["itemsView"] not in {"notLoaded", "summary", "full"}))
                        or (not turns and "turnId" in params and
                            (not isinstance(params["turnId"], str) or not params["turnId"]))):
                    raise ValueError("invalid bounded Codex native item parameters")
                owner, tid, source = daemon_native._original_history_binding(self, {
                    "workerId": message.get("workerId"),
                    "threadId": params.get("threadId"),
                })
                if not self.ready.is_set() or self.client is None:
                    raise RuntimeError("Codex app-server not ready")
                page = self.client.request(
                    "thread/turns/list" if turns else "thread/items/list",
                    params, timeout=2,
                )
                if (not isinstance(page, dict)
                        or not isinstance(page.get("data"), list)
                        or (page.get("nextCursor") is not None
                            and not isinstance(page.get("nextCursor"), str))):
                    raise ValueError("Codex native item page malformed")
                self._uuid_store.revalidate()
                return {"ok": True, "workerId": owner.worker_id,
                        "threadId": tid, "threadSource": source, "data": page}
            except UnknownWorker as error:
                return {"ok": False, "reason": "worker-not-found", "error": str(error)}
            except ProtocolError as error:
                return _protocol_failure(error)
            except (GenerationError, IdentityError, ValueError, RuntimeError) as error:
                return {"ok": False, "error": str(error)}
        if op == "account-read":
            if not self.ready.is_set() or self.client is None:
                return {"ok": False, "error": "codexd is booting"}
            try:
                if message.get("tokens"):
                    result = self.client.request("account/usage/read", timeout=30)
                else:
                    result = self._read_rate_limits()
                return {"ok": True, "data": result}
            except ProtocolError as error:
                return _protocol_failure(error)
        short = message.get("short")
        from ccx.store.identity import IdentityError
        try:
            owner = self._uuid_store.resolve(short)
            if owner.engine != "codex":
                raise ValueError("Codex daemon operation needs Codex full UUID")
            short = owner.worker_id
        except (IdentityError, ValueError, TypeError) as error:
            return {"ok":False,"error":
                    f"globally owned full-UUID Codex worker required: {error}"}
        if op == "load":
            return self._load_original_native_worker(short, message.get("timeout"))
        if op == "set-config":
            return self.set_config(short, message)
        if op == "set-name":
            return self.set_name(short, message)
        with self.lock:
            worker = self.workers.get(short)
        if worker is None and op == "kill":
            # Promptless original native creation has a live thread but no
            # daemon Worker until its first semantic send. Read-only adoption
            # proves exact native threadSource; no start or resume.
            try:
                worker = self._adopt_original_native_worker(f"codex:{short}")
            except Exception as error:
                return {"ok": False, "error": f"original native kill adoption unconfirmed: {error}"}
        if worker is None:
            if op in {"parent-notify", "machine-wake"}:
                return {"ok": False, "definitive": True,
                        "error": "original parent is unloaded; notification did not reach native"}
            return self._not_loaded(short)
        if op == "parent-notify":
            from ccx.notify.registry import read_registration
            from ccx.notify.select import _original_notification_registration
            from ccx.notify.deliver import payload_for_registration
            event = message.get("eventId")
            delivery = message.get("deliveryId")
            payload = message.get("payload")
            if not isinstance(event, str) or not event:
                return {"ok": False, "error": "original parent notification needs event ID"}
            reg = read_registration(event)
            if not isinstance(reg, dict):
                return {"ok": False, "error": "original notification registration missing"}
            try:
                store, parent, _child = _original_notification_registration(reg)
                store.revalidate()
                final = (reg.get("state") == "ready" and payload == reg.get("payload")
                         and isinstance(reg.get("settlement"), dict)
                         and payload == payload_for_registration(reg, reg["settlement"])
                         and delivery == f"{event}:settled"
                         and isinstance(payload, dict) and payload.get("parent") == parent)
                if (parent != f"codex:{short}" or not final
                        or not isinstance(payload, dict)
                        or payload.get("eventId") != event
                        or payload.get("deliveryId") != delivery):
                    raise ValueError("original notification parent/event/payload mismatch")
                with worker.lock:
                    if (worker.killed.is_set() or worker.kill_intent.is_set()
                            or not worker.loaded
                            or worker.record.get("lifecycle", {}).get("state")
                            not in ("idle", "busy", "waiting")
                            or worker._original_notification_thread()
                            != reg["parent"]["threadId"]):
                        raise ValueError("original parent not live, subscribed or source verified")
            except Exception as error:
                return {"ok": False, "definitive": True,
                        "error": f"original notification admission refused: {error}"}
        if op == "machine-wake":
            payload = message.get("payload")
            event, delivery = message.get("eventId"), message.get("deliveryId")
            namespace, name = message.get("namespace"), message.get("name")
            if (not isinstance(event, str) or not event or len(event) > 256
                    or not isinstance(delivery, str) or not delivery or len(delivery) > 512
                    or not isinstance(payload, dict)
                    or payload.get("eventId") != event
                    or payload.get("deliveryId") != delivery
                    or not isinstance(namespace, str) or not 0 < len(namespace) <= 128
                    or namespace == "ccx"
                    or not isinstance(name, str) or not 0 < len(name) <= 128
                    or type(message.get("reconcile")) is not bool):
                return {"ok": False, "definitive": True,
                        "error": "invalid original machine wake identity/output"}
            try:
                with worker.lock:
                    native = worker.record.get("native") or {}
                    if (worker.killed.is_set() or worker.kill_intent.is_set()
                            or not worker.loaded
                            or worker.record.get("lifecycle", {}).get("state")
                            not in ("idle", "busy", "waiting")
                            or message.get("threadId") != worker._original_notification_thread()
                            or native.get("threadId") != message["threadId"]):
                        raise ValueError("original machine wake not subscribed/owned/active")
            except Exception as error:
                return {"ok": False, "definitive": True,
                        "error": f"original machine wake admission refused: {error}"}
        if op == "status":
            return {"ok": True, "loaded": True, **worker.public_status()}
        if op == "compact-observe":
            # No load, settlement, native request or operation execution lock.
            with worker.lock:
                active = worker.record.get("activeCompact")
                last = worker.record.get("lastCompact") or {}
                owner = "held" if active else (
                    "unknown" if (last.get("status") == "unknown"
                    and not last.get("nativeTurnSettled"))
                    or worker.record["lifecycle"]["state"] not in {"idle", "busy", "waiting"}
                    else "released"
                )
                return json.loads(json.dumps({
                    "ok": True, "state": worker.record["lifecycle"]["state"],
                    "activeCompact": active, "lastCompact": last,
                    "input_surface": {"compaction_ownership": owner,
                        "operation_id": (active or {}).get("operationId"),
                        "runtime_id": self.instance_id},
                }))
        # Native control/parent receipt families below still lack the
        # generation-scoped prepared/submitted native-effect fence. They
        # must not descend into the old short-keyed handler just because
        # this original worker is now correctly indexed. `interrupt` is fenced
        # by the caller's GenerationStore cancel intent and expectedTurnId.
        if op not in ("status", "interrupt", "approve", "reject", "kill",
                      "parent-notify", "machine-wake", "compact"):
            return {"ok":False,"error":
                    f"{op}: original full-UUID Codex effect receipt unavailable; "
                    "no native call performed"}
        if op == "kill":
            return self._kill_worker(worker, message.get("observeSeconds"))
        history = None
        if (
            op in {"parent-notify", "machine-wake"}
            and message.get("reconcile", True)
            and isinstance(message.get("payload"), dict)
            and isinstance(message.get("deliveryId") or message.get("eventId"), str)
            and not (worker.kill_intent.is_set() or worker.killed.is_set())
        ):
            # The read-only history walk can page a long parent thread; it runs
            # before the operation lock so the parent's own sends and kills do
            # not queue behind it.
            try:
                history = (worker.find_parent_notification(
                    message.get("deliveryId") or message["eventId"],
                    message["payload"],
                    name=(message["name"] if op == "machine-wake" else "worker_notification"),
                ),)
            except ProtocolError as error:
                return _protocol_failure(error)
            except RuntimeError as error:
                if op != "machine-wake":
                    raise
                return {"ok": False, "pending": True, "error": str(error)}
        for _ in range(3):
            if worker.kill_intent.is_set() or worker.killed.is_set():
                # Refused without waiting for `operation_lock`: a kill may be
                # sitting on that lock waiting for an earlier operation to
                # settle, and queueing new work behind it is exactly what a
                # kill intent must stop.
                return {"ok": False, "definitive": True,
                        "error": f"worker {short} is being killed", "state": worker.state()}
            with worker.operation_lock:
                if self._still_registered(worker):
                    return worker.handle_operation(message, history=history)
            # The object was unloaded (idle-kill, kill, crash) while this
            # request waited for its lock. Acting on it would write a stale
            # record and drive a thread nothing listens to; follow the worker
            # to the object that is registered now, if any.
            with self.lock:
                current = self.workers.get(short)
            if current is None or current is worker:
                break
            worker = current
        return self._not_loaded(short)

    def _not_loaded(self, short: str) -> dict[str, Any]:
        return {
            "ok": False,
            "loaded": False,
            "runtime_id": self.instance_id,
            "error": f"worker {short} is not loaded",
        }

    def _still_registered(self, worker: "Worker") -> bool:
        """Is this object still the daemon's live worker for its short?"""
        with self.lock:
            return (
                self.workers.get(worker.short) is worker
                and not worker.teardown_claimed
            )

    def _kill_worker(self, worker: "Worker",
                     observe_seconds: Any = None) -> dict[str, Any]:
        """Kill one worker without either blocking indefinitely or racing.

        `kill` must be serialized against an in-flight steer/approve — bypassing
        `operation_lock` would let it interrupt a turn while another thread is
        mid-`turn/steer`, and the two would write the same record from opposite
        directions. But taking that lock unconditionally is how a kill silently
        outlives its confirmation budget.

        So the intent is published first (new operations are refused
        immediately, above), and the lock is then awaited only inside the
        caller's own budget. If the earlier operation cannot settle in time, the
        kill is reported explicitly unresolved and the worker is deliberately
        left registered and nonterminal — `confirmed_kill` reconciles that into
        a raise rather than a false success, and a retry after the operation
        settles completes normally. The budget is never extended: the wait for
        the lock and `kill`'s own launch-settle window come out of the same
        allowance.
        """
        budget = (_LAUNCH_SETTLE_CAP_S if observe_seconds is None
                  else max(0.0, min(float(observe_seconds),
                                    _LAUNCH_SETTLE_CAP_S)))
        worker.kill_intent.set()
        started = time.monotonic()
        if budget > 0:
            acquired = worker.operation_lock.acquire(timeout=budget)
        else:
            acquired = worker.operation_lock.acquire(blocking=False)
        if not acquired:
            # unresolved, not killed: another operation still owns this worker,
            # so nothing has been interrupted or terminalized. Saying "killed"
            # here would be the lie this whole path exists to avoid.
            #
            # Nor was anything accepted. No kill ran, and nobody is guaranteed
            # to retry, so leaving the intent set would strand a perfectly live
            # worker refusing every operation forever. Withdraw it — unless a
            # real kill (`killed`) is already in progress, whose sticky marker
            # is the one that must survive.
            if not worker.killed.is_set():
                worker.kill_intent.clear()
            return {
                "ok": False,
                "pending": True,
                "error": (f"worker {worker.short} has an operation in flight "
                          f"that did not settle within {budget:g}s; the kill "
                          f"was NOT completed and is not confirmed — retry "
                          f"kill once that operation settles"),
                "state": worker.state(),
            }
        remaining = max(0.0, budget - (time.monotonic() - started))
        if self._still_registered(worker):
            try:
                return worker.kill(remaining)
            finally:
                worker.operation_lock.release()
        worker.operation_lock.release()
        # The operation we waited behind unloaded this object. Killing it now
        # would confirm a kill against whatever has since been loaded under the
        # same short and write `dead` over its record; kill that one instead.
        with self.lock:
            current = self.workers.get(worker.short)
        if current is not None and current is not worker:
            return self._kill_worker(current, remaining)
        return self._not_loaded(worker.short)

    def dispatch(self, message: dict[str, Any]) -> dict[str, Any]:
        """Fence one request on the client's declared RPC generation.

        Recovery and control operations answer any client: they are how an
        operator inspects and retires a daemon whose generation no longer
        matches theirs. Every other operation must declare this generation
        before its handler runs, so a client from an older install cannot
        mutate a current daemon and discover the mismatch only in the reply.
        """
        if not isinstance(message, dict):
            raise TypeError("codexd request must be a JSON object")
        if message.get("op") in CODEXD_RECOVERY_OPS:
            return self.handle(message)
        declared = message.get("rpcGeneration")
        if isinstance(declared, bool) or not isinstance(declared, int):
            declared = None
        if declared != CODEXD_RPC_GENERATION:
            return {
                "ok": False,
                "incompatible": True,
                "error": (
                    f"codexd speaks RPC generation {CODEXD_RPC_GENERATION}, but "
                    f"the client declared "
                    f"{declared if declared is not None else 'none (legacy)'}. "
                    f"Upgrade the caller, or run `ccx shutdown --engine codex` to "
                    f"replace this daemon."
                ),
            }
        return self.handle(message)

    def _open_attach(self, message: dict[str, Any]) -> tuple[dict, Worker | None]:
        """Load the original native thread and hold codexd's subscription."""
        from ccx.store.identity import IdentityError
        try:
            owner = self._uuid_store.resolve(message.get("short"))
            if owner.engine != "codex":
                raise ValueError("attach requires an original Codex worker")
            result = self._load_original_native_worker(owner.worker_id, 90)
            if result.get("ok") is not True or result.get("loaded") is not True:
                return {**result, "ok": False,
                        "error": result.get("error") or "original thread not loaded"}, None
            with self.lock:
                worker = self.workers.get(owner.worker_id)
                if worker is None or worker.teardown_claimed or self.stopping:
                    raise RuntimeError("Codex worker unloaded during attach")
                with worker.lock:
                    if (not worker.loaded
                            or worker.record["lifecycle"]["state"] not in {"idle", "busy", "waiting"}):
                        raise RuntimeError("Codex worker no longer attachable")
                    worker.attach_count += 1
                    native = worker.record["native"]
                    reply = {"ok": True, "loaded": True,
                             "threadId": native["threadId"], "cwd": worker.record["cwd"],
                             "socket": str(self.app_server_path)}
            return reply, worker
        except (IdentityError, ValueError, RuntimeError, OSError) as error:
            return {"ok": False, "error": str(error)}, None

    def _serve_connection(self, connection: socket.socket) -> None:
        """Require same-connection full-root/gen/role handshake before RPC.

        The existing original daemon dispatch stays authoritative. No legacy
        singleton socket is accepted merely because the caller shares a UID.
        """
        from ccx.rpc.transport import handshake_payload
        store = self._uuid_store
        plan = self._uuid_plan
        expected = handshake_payload(
            plan, daemon_nonce=self._uuid_daemon_lease.nonce,
        )
        with connection:
            held_worker: Worker | None = None
            connection.settimeout(30)
            data = bytearray()

            def read_one() -> dict:
                while b"\n" not in data:
                    chunk = connection.recv(65536)
                    if not chunk:
                        raise EOFError("native daemon request ended before frame")
                    data.extend(chunk)
                    if len(data) > 1024 * 1024:
                        raise ValueError("Codex daemon RPC frame too large")
                line, rest = data.split(b"\n", 1)
                data[:] = rest
                parsed = json.loads(line)
                if not isinstance(parsed, dict):
                    raise ValueError("Codex daemon request must be JSON object")
                return parsed

            try:
                first = read_one()
                store.revalidate()
                if (first.get("op") != "handshake"
                        or first.get("identity") != {
                            "protocolVersion": 2, **plan.evidence(),
                        }):
                    raise ValueError(
                        "missing exact full-root/generation/Codex role handshake"
                    )
                connection.sendall((json.dumps({
                    "ok": True, "identity": expected,
                }) + "\n").encode())
                request = read_one()
                store.revalidate()
                if request.get("op") == "handshake":
                    raise ValueError("duplicate Codex singleton handshake")
                if (request.get("op") == "attach"
                        and request.get("rpcGeneration") == CODEXD_RPC_GENERATION):
                    reply, held_worker = self._open_attach(request)
                else:
                    reply = self.dispatch(request)
            except Exception as error:
                reply = {"ok": False, "error": str(error)}
            reply.setdefault("rpcGeneration", CODEXD_RPC_GENERATION)
            try:
                connection.settimeout(None)
                connection.sendall((json.dumps(reply) + "\n").encode())
                if held_worker is not None:
                    # This is the attach lifetime: EOF releases the hold.
                    # No further protocol messages, tokens or heartbeat needed.
                    while connection.recv(4096):
                        pass
            except OSError:
                pass
            finally:
                if held_worker is not None:
                    with held_worker.lock:
                        held_worker.attach_count -= 1
                        held_worker.last_activity = time.monotonic()

    def _accept_loop(self) -> None:
        server = self.server
        assert server is not None
        # Serve until the socket itself is closed, not merely until `stop`:
        # between the two a client would otherwise connect into an unserved
        # backlog and time out, instead of being told `stopping`.
        while True:
            try:
                connection = accept_same_user(server)
            except socket.timeout:
                if server.fileno() == -1:
                    return
                continue
            except OSError:
                if server.fileno() == -1:
                    return
                continue
            if connection is None:
                continue
            threading.Thread(
                target=self._serve_connection,
                args=(connection,),
                name="ccx-codexd-rpc",
                daemon=True,
            ).start()

    def run(self) -> None:
        """Own the singleton socket and app-server under the active generation.

        The transport and role lease authenticate the full root and generation.
        """
        from ccx import constants
        from ccx.store.root import require_ccx_active
        from ccx.rpc.transport import (
            daemon_process_lease, ensure_transport_parent, plan_transport,
        )
        store = require_ccx_active(constants.CCX_ROOT)
        plan = plan_transport(store, role="codexd")
        ensure_transport_parent(store, plan)
        self._uuid_store = store
        self._uuid_plan = plan
        with daemon_process_lease(store, plan) as lease:
            self._uuid_daemon_lease = lease
            path = plan.final
            try:
                # A crash can leave a stale socket inode. The singleton role
                # flock plus private root bind allows removing only an owned
                # socket; never unlink an arbitrary filesystem entry.
                try:
                    old = os.lstat(path)
                except FileNotFoundError:
                    old = None
                if old is not None:
                    import stat
                    if (not stat.S_ISSOCK(old.st_mode)
                            or old.st_uid != os.getuid()):
                        raise RuntimeError("untrusted old Codex daemon socket")
                    path.unlink()
                with store.lease():
                    self.server = socket.socket(
                        socket.AF_UNIX, socket.SOCK_STREAM,
                    )
                    bind_private_unix_socket(self.server, path)
                    self.server.listen(32)
                    self.server.settimeout(1)
                self.accepter = threading.Thread(
                    target=self._accept_loop,
                    name="ccx-codexd-accept", daemon=True,
                )
                self.accepter.start()
                self._start_client()
                self._reconcile_orphan_records()
                self.ready.set()
                threading.Thread(
                    target=self._janitor, name="ccx-codexd-janitor",
                    daemon=True,
                ).start()
                while not self.stop.wait(1):
                    failure = self.runtime_failure()
                    if failure is not None:
                        raise RuntimeError(failure)
            except BaseException as error:
                if not isinstance(error, (KeyboardInterrupt, SystemExit)):
                    with self.lock:
                        crashed = list(self.workers.values())
                    for worker in crashed:
                        if worker.state() in LIVE_STATES:
                            worker._state("crashed", error=str(error))
                    print(f"ccx codexd: {error}", file=sys.stderr, flush=True)
            finally:
                with self.lock:
                    self.stopping = True
                self.stop.set()
                if self.server:
                    self.server.close()
                if self.accepter:
                    self.accepter.join(timeout=2)
                try:
                    self._retire_workers()
                finally:
                    try:
                        if self.client:
                            self.client.close()
                    finally:
                        if self.app_server_path and self.app_server_inode:
                            try:
                                inode = os.lstat(self.app_server_path)
                                if (inode.st_dev, inode.st_ino) == self.app_server_inode:
                                    os.unlink(self.app_server_path)
                            except FileNotFoundError:
                                pass
                        try:
                            # Do not unlink a replacement socket belonging to a
                            # different inode/process lifetime.
                            if path.exists() and path.is_socket():
                                path.unlink()
                        except OSError:
                            pass

    def _adopt_original_native_worker(self, target: str, *, read_timeout: float=1.0) -> 'Worker':
        return daemon_native._adopt_original_native_worker(self, target, read_timeout=read_timeout)

    def _await_thread_release(self, thread_id: str | None) -> None:
        return daemon_release._await_thread_release(self, thread_id)

    def _janitor(self) -> None:
        return daemon_release._janitor(self)

    def _janitor_daemon_pass(self) -> bool:
        return daemon_release._janitor_daemon_pass(self)

    def _janitor_native_start_pass(self) -> None:
        return daemon_native._janitor_native_start_pass(self)

    def _janitor_worker(self, worker: Worker) -> None:
        return daemon_release._janitor_worker(self, worker)

    def _load_original_native_worker(self, worker_id: str, timeout: float | None) -> dict[str, Any]:
        return daemon_native._load_original_native_worker(self, worker_id, timeout)

    def _native_start_counts(self) -> dict[str, int]:
        return daemon_native._native_start_counts(self)

    def _observe_original_codex_thread(self, message: dict[str, Any]) -> dict[str, Any]:
        return daemon_native._observe_original_codex_thread(self, message)

    def _original_full_uuid_native(self, message: dict[str, Any]) -> dict[str, Any]:
        return daemon_native._original_full_uuid_native(self, message)

    def _release_native_threads(self, worker: Worker, releases: list['_ThreadRelease']) -> None:
        return daemon_release._release_native_threads(self, worker, releases)

    def _retire_workers(self) -> None:
        return daemon_release._retire_workers(self)

    def _save_held_codex_candidate(self, hold_key: tuple[str, str, str]) -> dict[str, Any]:
        return daemon_native._save_held_codex_candidate(self, hold_key)

    def _submit_original_codex_thread(self, worker_id: str, record: dict, source: str, token: str, *, admission_deadline: float | None=None) -> dict[str, Any]:
        return daemon_native._submit_original_codex_thread(self, worker_id, record, source, token, admission_deadline=admission_deadline)

    def _unload(self, worker: Worker, *, terminal_state: str, owns_lifecycle: bool=False) -> None:
        return daemon_release._unload(self, worker, terminal_state=terminal_state, owns_lifecycle=owns_lifecycle)


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        description="CCX shared Codex app-server daemon (codexd)."
    )
    parser.parse_args(argv)
    from ccx.system.engine_binary import resolve_engine_binary
    from ccx.errors import SessionUnavailable
    try:
        resolve_engine_binary("codex")
    except SessionUnavailable as error:
        parser.exit(error.exit_code, f"ccx: {error}\n")
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    CodexDaemon().run()
