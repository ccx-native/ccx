from __future__ import annotations

import json
import os
import sys
import time
from typing import Any
from ccx.prompt_images import split_prompt_images
from ccx.codex.delivery import (
    route as send_receipt_route,
)
from ccx.codex.protocol import AppServerClient, ProtocolError, ProtocolNotSent, ProtocolTransportError
from ccx.codex.reports import select_report_for_turn
from ccx.codex.records import now_ms
from ccx.codex.daemon.models import WorkerKilled, _TurnLaunch, _native_history_budget
import ccx.codex.daemon.worker as daemon_worker
import ccx.codex.daemon.release as daemon_release
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ccx.codex.daemon.worker import Worker


_CODEX_NATIVE_START_HELD_LIMIT = 16


_CODEX_NATIVE_START_UNSAFE_LIMIT = 32


_CODEX_NATIVE_START_RETRY_PER_PASS = 2


_CODEX_NATIVE_START_ID_MAX_BYTES = 2048


_CODEX_NATIVE_START_REPLY_MAX_BYTES = 64 * 1024


def _original_caller_config(worker_id: str) -> dict[str, Any]:
    """Per-thread shell env naming the exact full-UUID caller.

    Codex applies it per start/resume request, so an original thread gets it
    again on every resume, not only at its first thread/start.
    """
    from ccx.controllers.live_vars import codex_thread_config
    return codex_thread_config(worker_id)


def _with_default_effort(config: dict[str, Any], args: dict[str, Any]) -> dict[str, Any]:
    """Make the worker's effort the thread default, not only a per-turn value.

    Turns typed in an attached TUI carry no effort, so a thread whose CCX turn
    has not run yet (fresh or just revived) would otherwise use Codex's default.
    """
    if args.get("effort"):
        return {**config, "model_reasoning_effort": args["effort"]}
    return config


def _original_subscription_in_this_runtime(
    daemon, target: str, thread_id: str, record: dict[str, Any],
) -> bool:
    """Original activation's current daemon/app-server lifetime, not PID alone."""
    from ccx.store.root import _read_json
    import hashlib
    store = daemon._uuid_store
    client = daemon.client
    if (daemon.stopping or not daemon.ready.is_set() or client is None
            or client.process.poll() is not None):
        return False
    original = store.native_activation(target)
    owner = store.resolve(target)
    token = original.get("submissionId")
    source = f"ccx-worker:{owner.worker_id}"
    if (original.get("state") != "confirmed"
            or original.get("source") != source
            or (original.get("nativeBinding") or {}).get("threadId") != thread_id
            or not isinstance(token, str) or not token):
        return False
    key = hashlib.sha256(("activate\x00" + token).encode()).hexdigest()
    try:
        with store.lease():
            with store._gen("runtime-metadata", "codex-native-effects",
                            owner.worker_id) as directory:
                effect = _read_json(directory, key + ".json")
    except FileNotFoundError:
        return False
    if not isinstance(effect, dict):
        return False
    candidate = effect.get("nativeCandidate") or {}
    return bool(
        effect.get("kind") == "activate"
        and effect.get("workerId") == owner.worker_id
        and effect.get("engine") == "codex"
        and effect.get("generation") == store.generation
        and effect.get("token") == token
        and effect.get("source") == source
        and effect.get("createSha256") == record.get("createSha256")
        and effect.get("daemonInstanceId") == daemon.instance_id
        and effect.get("appServerPid") == client.process.pid
        and isinstance(candidate, dict)
        and candidate.get("threadId") == thread_id
    )


def _load_original_native_worker(
    daemon, worker_id: str, timeout: float | None,
) -> dict[str, Any]:
    """Ensure one source-proven full-UUID thread is natively subscribed.

    Thread/read alone is never native loadedness after park. Existing
    per-worker load/kill locks and release fences own the race.
    """
    from ccx.store.root import NativeActionDenied
    if timeout is not None and (not isinstance(timeout, (int, float)) or timeout <= 0):
        return {"ok": False, "error": "invalid full-UUID load timeout"}
    budget = min(float(timeout) if timeout is not None else 90., 90.)
    handle = "codex:" + worker_id
    try:
        daemon._uuid_store.authorize_native_action(handle, "resume")
        worker = daemon._adopt_original_native_worker(handle, read_timeout=budget)
        original_native = (worker.record.get("native") or {}).get("threadId")
        with worker.load_lock:
            with daemon.lock:
                if daemon.workers.get(worker_id) is not worker or daemon.stopping:
                    raise WorkerKilled("original Codex Worker unloaded during resume")
            with worker.lock:
                if worker.killed.is_set() or worker.kill_intent.is_set():
                    raise WorkerKilled("original Codex Worker is being killed")
                lifecycle = worker.record["lifecycle"]
                if isinstance(worker.record.get("nativeTurnStart"), dict):
                    raise RuntimeError("original native turn/start is unresolved; no resume")
                if lifecycle["state"] in ("busy", "waiting"):
                    # Exact source-verified read-only adoption remains a
                    # valid low-level load result for an old active gate,
                    # but it is not an idle/subscribed resume. The public
                    # ensure_ready insists on loaded=True and state=idle.
                    return {"ok": True, "changed": False,
                            "loaded": bool(worker.loaded),
                            **worker.public_status()}
                if worker.loaded:
                    if lifecycle["state"] == "prepared":
                        # Original promptless native activation only wrote
                        # the initial prepared lifecycle. Matching the
                        # original current app-server effect+thread/read
                        # proves the subscribed zero-turn thread is ready.
                        worker._commit_state("idle", activeTurnId=None)
                    if worker.record["lifecycle"]["state"] != "idle":
                        raise RuntimeError("original Codex worker is not idle")
                    return {"ok": True, "changed": False, "loaded": True,
                            **worker.public_status()}
                # An earlier resume may have timed out while Codex kept
                # loading the thread. Resuming again is safe: Codex attaches
                # to a thread it already has loaded.
                if not isinstance(original_native, str) or not original_native:
                    raise NativeActionDenied("original native Codex threadId missing")
                if not os.path.isdir(worker.record["cwd"]):
                    raise RuntimeError("original Codex workspace no longer exists")
            args = worker.record.get("createArgs") or {}
            params = {"cwd": worker.record["cwd"],
                      "sandbox": args.get("sandbox"),
                      "approvalPolicy": args.get("approvalPolicy"),
                      "serviceName": "ccx",
                      "config": _with_default_effort(
                          _original_caller_config(worker.worker_id), args)}
            if args.get("model"):
                params["model"] = args["model"]
            def publish_resume():
                with worker.lock:
                    if worker.killed.is_set() or worker.kill_intent.is_set():
                        raise WorkerKilled("original Codex Worker killed before native resume")
                    current = worker.record["lifecycle"]
                    current["state"] = "resuming"
                    current["since"] = now_ms()
                    current["activeTurnId"] = None
                    worker.record["pendingApprovals"] = []
                    worker._save()
            try:
                reply = worker._resume_exact_thread(
                    original_native, params, timeout=budget,
                    before_submit=publish_resume,
                )
            except ProtocolError as error:
                # An answered JSON-RPC rejection is not an uncertain
                # subscription. A local timeout/transport loss is: its
                # request_id is absent, so preserve the no-replay fence.
                received = (error.request_id is not None or
                            (isinstance(error.__cause__, ProtocolError)
                             and error.__cause__.request_id is not None))
                if received and not isinstance(error, ProtocolTransportError):
                    with worker.lock:
                        worker._commit_state(
                            "crashed", lastError=f"original native Codex resume rejected: {error}",
                        )
                raise
            native_thread = reply.get("thread") if isinstance(reply, dict) else None
            native_status = (native_thread.get("status") if isinstance(native_thread, dict)
                             else None)
            # The independent source-fenced thread/read proved the source
            # even if the reply omits its optional threadSource field.
            if not isinstance(native_thread, dict):
                error_message = "original native Codex resume reply missing thread"
            elif native_thread.get("id") != original_native:
                error_message = "Codex resumed the wrong thread; inspect the worker"
            elif native_thread.get("threadSource") not in (
                None, daemon._uuid_store.codex_source(handle)
            ):
                error_message = "Codex thread belongs to another worker; inspect its handle"
            elif not isinstance(native_status, dict):
                error_message = "original native Codex resume reply missing thread status"
            elif native_status.get("type") not in ("idle", "systemError"):
                error_message = (
                    "original native Codex resume reply not ready: "
                    f"status {native_status.get('type')!r}"
                )
            else:
                error_message = None
            if error_message is not None:
                with worker.lock:
                    if worker.killed.is_set() or worker.kill_intent.is_set():
                        raise WorkerKilled("original Codex Worker killed during resume")
                    worker._commit_state("crashed", lastError=error_message)
                raise RuntimeError(error_message)
            with worker.lock:
                if worker.killed.is_set() or worker.kill_intent.is_set():
                    raise WorkerKilled("original Codex Worker killed during resume")
                worker.record["pendingApprovals"] = []
                worker.record["activeTurnError"] = None
                worker.record["lifecycle"]["lastError"] = None
                worker.record["lifecycle"]["activeTurnId"] = None
                worker._commit_state("idle")
                worker._resumed_system_error_pending = (
                    native_status.get("type") == "systemError"
                )
                worker.loaded = True
            return {"ok": True, "changed": True, "loaded": True,
                    **worker.public_status()}
    except WorkerKilled as error:
        if "worker" in locals():
            daemon_release._abandon_killed_load(daemon, worker)
        return {"ok": False, "error": str(error)}
    except Exception as error:
        # On a lost native reply the resuming record is deliberately held
        # and native_threads is a possible-subscription cleanup obligation.
        return {"ok": False, "error": str(error)}


def _adopt_original_native_worker(
    daemon, target: str, *, read_timeout: float = 1.,
) -> "Worker":
    """Register one immutable full-UUID thread in the original daemon index.

    App-server notifications, lifecycle locks and native send/observe
    share one Worker instance. Merely
    knowing a threadId is never authority: compare exact durable native
    source and confirmed creation claim before constructing anything.
    This never calls thread/start or thread/resume.
    """
    from ccx.store.root import NativeActionDenied
    store = daemon._uuid_store
    owner = store.authorize_native_action(target, "send")
    if owner.engine != "codex":
        raise NativeActionDenied("daemon adoption requires Codex full UUID")
    rec = store.record(owner.worker)
    native = rec.get("native") or {}
    thread_id = native.get("threadId")
    source = store.codex_source(owner.worker)
    if (not isinstance(thread_id, str) or not thread_id
            or native.get("threadSource") != source):
        raise NativeActionDenied("native threadId/source was not confirmed")
    # A live Worker is a cache of the immutable original global owner;
    # never key it by a short/native thread ID. Construct before taking
    # the daemon-global lock to avoid serializing journal inspections.
    with daemon._lifecycle_lock(owner.worker_id):
        # After a daemon restart the original native thread may no
        # longer exist. A recorded threadId alone is not proof this
        # app-server owns it. Read the exact original native source
        # before adopting; never call thread/start or thread/resume
        # as a fallback for a missing/different native thread.
        if daemon.client is None:
            raise RuntimeError("Codex app-server unavailable for adoption")
        observed=daemon.client.request(
            "thread/read",
            {"threadId":thread_id,"includeTurns":False},
            timeout=read_timeout,
        )
        native_proof=(
            observed.get("thread") if isinstance(observed,dict) else None
        )
        if (not isinstance(native_proof,dict)
                or native_proof.get("id")!=thread_id
                or native_proof.get("threadSource") != source):
            raise RuntimeError(
                "original Codex native thread/read source absent or foreign; "
                "never recreate an original thread"
            )
        with daemon.lock:
            existing = daemon.workers.get(owner.worker_id)
            if existing is not None:
                if (existing.worker_id != owner.worker_id
                        or daemon.by_thread.get(thread_id) != owner.worker_id):
                    raise RuntimeError(
                        "Codex registry contradicts original full UUID"
                    )
                return existing
            if daemon.stopping or not daemon.ready.is_set():
                raise RuntimeError("Codex singleton not accepting new workers")
            bound = daemon.by_thread.get(thread_id)
            if bound is not None and bound != owner.worker_id:
                raise RuntimeError("native thread already owned by another UUID")
        worker = daemon_worker.Worker(daemon, rec)
        # Native membership is an unsubscribe obligation, not readiness.
        # A parked or old-daemon thread/read can see history without a
        # current app-server subscription. Only original current-epoch
        # activation with an unparked record proves the first subscription.
        worker.native_threads.add(thread_id)
        worker.loaded = (rec["lifecycle"].get("state") in ("prepared", "idle", "busy", "waiting")
                         and _original_subscription_in_this_runtime(daemon, owner.worker, thread_id, rec))
        with daemon.lock:
            prior = daemon.workers.get(owner.worker_id)
            if prior is not None:
                return prior
            if daemon.by_thread.get(thread_id) not in (None, owner.worker_id):
                raise RuntimeError("native thread ownership changed")
            daemon.workers[owner.worker_id] = worker
            daemon.by_thread[thread_id] = owner.worker_id
            daemon.empty_since = None
        # A newly adopted app-server is not evidence that a command from a
        # previous native runtime still runs. Repair a surviving stale record.
        with worker.lock:
            if worker._lose_native_background_locked():
                worker._save()
        worker.refresh_parent_notifications()
        return worker


def _native_start_retryable_save_error(daemon, error: BaseException) -> bool:
    """Retry only recognized transient persistence failures, not corruption."""
    import errno
    from ccx.store.root import GenerationBusy
    return isinstance(error, GenerationBusy) or (
        isinstance(error, OSError)
        and getattr(error, "errno", None) in {
            errno.ENOSPC, errno.EIO, errno.EAGAIN, errno.EINTR, errno.EBUSY,
        }
    )


def _mark_unsafe_native_start_locked(
    daemon, hold_key: tuple[str, str, str], held: dict[str, Any],
    detail: str,
) -> None:
    """Record a bounded unsafe diagnostic without pinning native capacity.

    Caller holds only `_native_start_hold_lock`. Original one-shot effect
    marker/claim stay untouched, and this rejected evidence is never used
    to write a candidate or to justify a new native dispatch.
    """
    if daemon._native_start_held.get(hold_key) is not held:
        return
    held["unsafe"] = True
    held["error"] = detail
    del daemon._native_start_held[hold_key]
    daemon._native_start_unsafe[hold_key] = detail[:250]
    daemon._native_start_unsafe_total += 1
    if len(daemon._native_start_unsafe) > _CODEX_NATIVE_START_UNSAFE_LIMIT:
        del daemon._native_start_unsafe[next(iter(daemon._native_start_unsafe))]


def _native_start_counts(daemon) -> dict[str, int]:
    """Bounded daemon health, no exposure of native IDs or source tokens."""
    with daemon._native_start_hold_lock:
        pending = sum(value["candidate"] is None
                      and value.get("rejection") is None
                      for value in daemon._native_start_held.values())
        held = len(daemon._native_start_held) - pending
        return {
            "pending": pending, "held": held,
            "unsafe": len(daemon._native_start_unsafe),
            "unsafeTotal": daemon._native_start_unsafe_total,
            "capacity": _CODEX_NATIVE_START_HELD_LIMIT,
        }


def _janitor_native_start_pass(daemon) -> None:
    """Retry a bounded number of transient original saves every janitor tick.

    No native requests or extra threads. Retained evidence is never evicted
    on age; timed-out still-in-flight PendingRequest must continue to pin
    the daemon. Backoff bounds repeated disk/lock work. Explicit daemon
    stop/cutover wins immediately and is never blocked by retry loops.
    """
    if daemon.stop.is_set():
        return
    now = time.monotonic()
    with daemon._native_start_hold_lock:
        attempts = [key for key, held in daemon._native_start_held.items()
                    if (held["candidate"] is not None
                         or held.get("rejection") is not None)
                    and held["error"] is not None
                    and held.get("retryAfter", float("inf")) <= now]
        attempts = attempts[:_CODEX_NATIVE_START_RETRY_PER_PASS]
    for key in attempts:
        if daemon.stop.is_set():
            return
        try:
            daemon._save_held_codex_candidate(key)
        except Exception as error:  # noqa: BLE001
            # Do not let a janitor exception disable idle shutdown or
            # notifications for unrelated workers.
            daemon_release._log_janitor_failure(daemon, "original native candidate", error)


def _save_held_codex_candidate(
    daemon, hold_key: tuple[str, str, str],
) -> dict[str, Any]:
    """Retry fsync of only the original native reply outcome, never dispatch.

    Called once from the app-server reader callback, or later from a
    native-activation-status socket handler. Both use the same pinned
    generation and original effect lock; a concurrent successful fsync is
    idempotent. Unsafe disk identities and a changed generation stay
    blocked in memory, not overwritten on any later status request.
    """
    from ccx.store.root import StorageIntegrityError, _read_json, _write_json, _flock_at
    import hashlib
    with daemon._native_start_hold_lock:
        held = daemon._native_start_held.get(hold_key)
        if held is None:
            unsafe = daemon._native_start_unsafe.get(hold_key)
            return ({"status": "unsafe", "error": unsafe}
                    if unsafe is not None else {"status": "not-held"})
        if held["unsafe"]:
            return {"status": "unsafe", "error": held["error"]}
        if held["candidate"] is None and held.get("rejection") is None:
            return {"status": "not-held"}
        original = dict(held["original"])
        candidate = dict(held["candidate"]) if held["candidate"] is not None else None
        rejection = dict(held["rejection"]) if held.get("rejection") is not None else None
    generation, worker_id, token = hold_key
    key = hashlib.sha256(("activate" + "\x00" + token).encode()).hexdigest()
    store = daemon._uuid_store
    try:
        if generation != store.generation:
            raise StorageIntegrityError("held native reply belongs to another generation")
        with store.lease():
            with store._gen("runtime-metadata", "codex-native-effects",
                            worker_id) as directory:
                with _flock_at(directory, key + ".lock", exclusive=True,
                               create=False, timeout=.5):
                    saved = _read_json(directory, key + ".json")
                    if (not isinstance(saved, dict) or any(
                            saved.get(k) != v for k, v in original.items())):
                        raise StorageIntegrityError(
                            "original Codex effect marker absent/changed; no evidence rewrite"
                        )
                    if (candidate is None) == (rejection is None):
                        raise StorageIntegrityError(
                            "original Codex native response outcome is contradictory"
                        )
                    field, intended, opposite = (
                        ("nativeCandidate", candidate, "nativeRejected")
                        if candidate is not None
                        else ("nativeRejected", rejection, "nativeCandidate")
                    )
                    if (saved.get(opposite) is not None
                            or saved.get("nativeNotSubmitted") is not None):
                        raise StorageIntegrityError(
                            "original Codex native outcome conflicts with local non-submission"
                        )
                    previous = saved.get(field)
                    if previous is not None and previous != intended:
                        raise StorageIntegrityError(
                            "original Codex native outcome changed; no overwrite"
                        )
                    if previous is None:
                        _write_json(directory, key + ".json", {
                            **saved, field: intended,
                        })
        if rejection is not None:
            # A marker is always fsynced before terminal claim transition.
            # Failure to seal the claim keeps this exact rejection held for
            # bounded maintenance/status retry; the durable marker also
            # permits a new CLI to reconcile entirely offline.
            sealed = store.reconcile_codex_native_rejection(
                f"codex:{worker_id}"
            )
            if (not isinstance(sealed, dict)
                    or sealed.get("code") != rejection["code"]
                    or sealed.get("message") != rejection["message"]):
                raise StorageIntegrityError("original native rejection not sealed")
    except Exception as error:
        retryable = _native_start_retryable_save_error(daemon, error)
        # Do not silently discard original response evidence after one
        # ENOSPC/lease-contention failure. Unsafe identity/generation
        # failures are visible and *never* reattempted by status.
        detail = f"{type(error).__name__}: {error}"[:250]
        with daemon._native_start_hold_lock:
            if daemon._native_start_held.get(hold_key) is held:
                held["error"] = detail
                if not retryable:
                    _mark_unsafe_native_start_locked(daemon, hold_key, held, detail)
                else:
                    # First retry is no sooner than one janitor tick; each
                    # failed retry backs off, capped at 30 seconds.
                    failures = min(6, held.get("retryFailures", 0) + 1)
                    held["retryFailures"] = failures
                    held["retryAfter"] = time.monotonic() + min(
                        30.0, float(2 ** (failures - 1))
                    )
        print(
            f"ccx codexd: original Codex start reply pending disk save "
            f"for {worker_id} ({'transient' if retryable else 'unsafe'}): {detail}",
            file=sys.stderr, flush=True,
        )
        return {"status": "transient" if retryable else "unsafe",
                "error": detail}
    with daemon._native_start_hold_lock:
        if daemon._native_start_held.get(hold_key) is held:
            del daemon._native_start_held[hold_key]
    return {"status": "saved"}


def _native_codex_local_proof(daemon, original: dict, reason: str,
                              stage: str) -> dict[str, Any]:
    """Bounded local attribution, not a cryptographically signed proof."""
    from ccx.store.store import _digest
    import hashlib
    message = reason.encode("utf-8")
    if not message or len(message) > 1024:
        raise ValueError("local Codex refusal reason must be <=1024 bytes")
    fields = {
        "stage": stage, "reason": reason,
        "reasonSha256": hashlib.sha256(message).hexdigest(),
        "nativeRequestWritten": False,
        "appServerPid": original["appServerPid"],
    }
    return {**fields, "localEvidenceSha256": _digest({
        "workerId": original["workerId"],
        "generation": original["generation"],
        "createSha256": original["createSha256"],
        "source": original["source"],
        "submissionId": original["token"],
        "daemonInstanceId": original["daemonInstanceId"],
        "daemonPid": original["daemonPid"],
        "nativeNotSubmitted": fields,
    })}


def _record_original_codex_local_refusal(daemon, worker_id: str, record: dict,
                                        source: str, token: str, *,
                                        stage: str, reason: str) -> dict[str, Any]:
    """fsync a pre-IPC local refusal under the original one-shot effect lock.

    Recheck the existing same-ID held/unsafe reservations under self.lock,
    in the same order as _submit_original_codex_thread. No competitor can
    register a native writer between this check and the immutable marker
    write. Never infer refusal from a missing marker or a timeout; only
    this controlled pre-dispatch branch has positive zero-native-write proof.
    """
    from ccx.store.root import _read_json, _write_json, _flock_at
    import hashlib
    store = daemon._uuid_store
    hold_key = (store.generation, worker_id, token)
    original = {
        "workerId": worker_id, "engine": "codex",
        "generation": store.generation, "kind": "activate",
        "token": token, "state": "submitted-unknown",
        "source": source, "createSha256": record["createSha256"],
        "daemonInstanceId": daemon.instance_id,
        "daemonPid": os.getpid(),
        "appServerPid": (daemon.client.process.pid if daemon.client else None),
        **({"nativeMethod": "thread/fork", "forkAnchor": record["forkAnchor"]}
           if record.get("forkAnchor") is not None else {}),
    }
    result = {
        "ok": False, "error": reason, "workerId": worker_id,
        "localRefusalReason": reason, "localRefusalProven": False,
    }
    key = hashlib.sha256(("activate\x00" + token).encode()).hexdigest()
    try:
        # Reentrant lock is intentionally held while writing the one-shot
        # marker: new same-ID reservations need this exact lock first.
        with daemon.lock:
            with daemon._native_start_hold_lock:
                if (hold_key in daemon._native_start_held
                        or hold_key in daemon._native_start_unsafe):
                    result["error"] = (
                        reason + "; existing same-ID native attempt unresolved"
                    )
                    return result
            with store.lease():
                with store._gen("runtime-metadata", "codex-native-effects",
                                worker_id, create=True) as directory:
                    with _flock_at(directory, key + ".lock", exclusive=True,
                                   create=True, timeout=.5):
                        if _read_json(directory, key + ".json") is not None:
                            result["error"] = (
                                reason + "; original effect already fenced; "
                                "cannot prove non-submission"
                            )
                            return result
                        proof = _native_codex_local_proof(daemon, original, reason, stage)
                        _write_json(directory, key + ".json", {
                            **original, "nativeNotSubmitted": proof,
                        })
        # From here onward the original marker alone permits offline
        # reconciliation after crash or any transient claim-write failure.
        result["localRefusalProven"] = True
    except Exception as error:
        result["error"] = (
            f"{reason}; original not-submitted proof unavailable: "
            f"{type(error).__name__}: {error}"
        )[:400]
        return result
    try:
        sealed = store.reconcile_codex_local_refusal(f"codex:{worker_id}")
        if not isinstance(sealed, dict) or sealed.get("reason") != reason:
            raise RuntimeError("local refusal terminal claim not sealed")
    except Exception as error:
        # Never lose the fsynced marker merely because the CLI's original
        # claim lock is busy or its terminal claim fsync transiently fails.
        result["error"] = (
            f"{reason}; original marker durable, terminal claim pending: "
            f"{type(error).__name__}: {error}"
        )[:400]
    return result


def _submit_original_codex_thread(daemon, worker_id: str, record: dict,
                                  source: str, token: str, *,
                                  admission_deadline: float | None = None) -> dict[str, Any]:
    """Fence and write one native thread/start, not its slow completion.

    Reserve bounded daemon memory before native admission. Original
    positive provider evidence stays held until its marker is fsynced,
    including after a transient callback failure. No native request is
    ever submitted from status or the completion callback.
    """
    from ccx.store.root import NativeEffectIndeterminate, _read_json, _write_json, _flock_at
    import hashlib
    store = daemon._uuid_store
    client = daemon.client
    # Compatibility for explicit internal callers predating the deadline
    # argument; regular CLI always supplies the original shared budget.
    if admission_deadline is None:
        admission_deadline = time.monotonic() + 2.0
    if time.monotonic() >= admission_deadline:
        # A generic elapsed deadline is not a proof of local non-write.
        # Without an exact original ProtocolNotSent or pre-IPC gate,
        # retain the original submitted-unknown fence.
        raise NativeEffectIndeterminate("original Codex admission deadline expired")
    if client is None or not daemon.ready.is_set():
        return _record_original_codex_local_refusal(daemon, worker_id, record, source, token,
            stage="not-ready", reason="Codex app-server not ready",
        )
    try:
        caller_config = _original_caller_config(worker_id)
    except Exception as error:
        return _record_original_codex_local_refusal(daemon, worker_id, record, source, token, stage="shell-env",
            reason=f"managed shell environment unavailable: {type(error).__name__}",
        )
    key = hashlib.sha256(("activate" + "\x00" + token).encode()).hexdigest()
    args = record.get("createArgs") or {}
    start_params = {
        "cwd": record["cwd"],
        "sandbox": args.get("sandbox"),
        "approvalPolicy": args.get("approvalPolicy"),
        "serviceName": "ccx", "threadSource": source,
        "config": _with_default_effort(caller_config, args),
    }
    if args.get("model"):
        start_params["model"] = args["model"]
    fork = record.get("forkAnchor")
    method = "thread/fork" if fork is not None else "thread/start"
    request_params = dict(start_params)
    if fork is not None:
        request_params.pop("serviceName")  # Not in pinned ThreadForkParams.
        request_params["threadId"] = fork["sourceNativeThreadId"]
        request_params["lastTurnId"] = fork["lastTurnId"]
        request_params["excludeTurns"] = True
    original = {
        "workerId": worker_id, "engine": "codex",
        "generation": store.generation,
        "kind": "activate", "token": token,
        "state": "submitted-unknown", "source": source,
        "createSha256": record["createSha256"],
        "daemonInstanceId": daemon.instance_id,
        "daemonPid": os.getpid(),
        "appServerPid": client.process.pid,
        **({"nativeMethod": method, "forkAnchor": fork} if fork is not None else {}),
    }
    hold_key = (store.generation, worker_id, token)
    held: dict[str, Any] = {
        "original": original, "candidate": None, "rejection": None,
        "unsafe": False, "error": None,
    }
    # Total order against the janitor's idle-exit decision. Reset linger
    # at admission, not when (possibly slow) provider reply arrives.
    with daemon.lock:
        if daemon.stopping or daemon.stop.is_set():
            return _record_original_codex_local_refusal(daemon, worker_id, record, source, token,
                stage="daemon-stopping", reason="Codex daemon is stopping; no native start",
            )
        with daemon._native_start_hold_lock:
            if hold_key in daemon._native_start_held or hold_key in daemon._native_start_unsafe:
                raise NativeEffectIndeterminate(
                    "same original Codex native start already recorded; no replay"
                )
            full = len(daemon._native_start_held) >= _CODEX_NATIVE_START_HELD_LIMIT
            if not full:
                daemon._native_start_held[hold_key] = held
        if full:
            return _record_original_codex_local_refusal(daemon, worker_id, record, source, token,
                stage="capacity", reason=(
                    "Codex native start retention full; new provider submission refused"
                ),
            )
        daemon.empty_since = None

    def capture_native_result(answer) -> None:
        # Runs on the app-server reader (or immediately upon callback
        # registration for a fast reply). Do not call any provider RPC.
        try:
            provider_rejection = answer.original_provider_rejection()
            if provider_rejection is not None:
                message = provider_rejection["message"]
                from ccx.store.store import _digest
                rejected = {
                    **provider_rejection,
                    "messageSha256": hashlib.sha256(message.encode("utf-8")).hexdigest(),
                    "appServerPid": original["appServerPid"],
                    "nativeErrorSha256": _digest({
                        "code": provider_rejection["code"],
                        "message": message,
                        "requestId": provider_rejection["requestId"],
                        "appServerPid": original["appServerPid"],
                    }),
                }
                # Retain exact original coded provider reply before any
                # claim/marker fsync, just as for an accepted candidate.
                # A numeric code on a local transport exception is not
                # proof of provider rejection.
                with daemon._native_start_hold_lock:
                    if daemon._native_start_held.get(hold_key) is not held:
                        raise NativeEffectIndeterminate(
                            "original provider rejection retention replaced"
                        )
                    held["rejection"] = rejected
                daemon._save_held_codex_candidate(hold_key)
                return
            native = answer.result()
            thread = native.get("thread") if isinstance(native, dict) else None
            candidate_id = thread.get("id") if isinstance(thread, dict) else None
            if not isinstance(candidate_id, str) or not candidate_id:
                with daemon._native_start_hold_lock:
                    if daemon._native_start_held.get(hold_key) is held:
                        del daemon._native_start_held[hold_key]
                return  # no positive candidate; provider rejection is handled above
            if fork is not None and candidate_id == fork["sourceNativeThreadId"]:
                raise ValueError("native fork response reused its source thread ID")
            if len(candidate_id.encode("utf-8")) > _CODEX_NATIVE_START_ID_MAX_BYTES:
                raise ValueError("provider native thread id exceeds retained bound")
            # Fork responses may include enormous copied history. Hash and
            # bound a shallow response without serializing those turns;
            # independent thread/read is the only history/point proof.
            reply = (dict(native, thread={k: v for k, v in thread.items()
                                          if k != "turns"})
                     if fork is not None else native)
            raw = json.dumps(reply, sort_keys=True,
                             separators=(",", ":"), allow_nan=False).encode()
            if len(raw) > _CODEX_NATIVE_START_REPLY_MAX_BYTES:
                raise ValueError("provider native reply exceeds retained bound")
            candidate = {
                "threadId": candidate_id,
                # Source is the original immutable request, not provider
                # attribution. Independent thread/read is still mandatory.
                "submittedSource": source,
                "appServerPid": original["appServerPid"],
                "nativeReplySha256": hashlib.sha256(raw).hexdigest(),
            }
            with daemon._native_start_hold_lock:
                if daemon._native_start_held.get(hold_key) is not held:
                    raise NativeEffectIndeterminate(
                        "original reply retention was replaced before disk save"
                    )
                held["candidate"] = candidate  # before any filesystem call
            daemon._save_held_codex_candidate(hold_key)
        except (ProtocolError, ProtocolTransportError) as error:
            # No confirmed positive reply. Never infer rejection from
            # a timeout, missing code or transport loss.
            with daemon._native_start_hold_lock:
                if daemon._native_start_held.get(hold_key) is held:
                    del daemon._native_start_held[hold_key]
            print(
                f"ccx codexd: Codex original start outcome unconfirmed "
                f"for {worker_id}: {error}",
                file=sys.stderr, flush=True,
            )
        except Exception as error:
            detail = f"{type(error).__name__}: {error}"[:250]
            with daemon._native_start_hold_lock:
                _mark_unsafe_native_start_locked(daemon, hold_key, held, detail)
            print(
                f"ccx codexd: cannot retain original native reply safely "
                f"for {worker_id}: {detail}",
                file=sys.stderr, flush=True,
            )

    pending = None
    local_reason = None
    local_marker_saved = False
    local_marker_error = None
    try:
        with store.lease():
            with store._gen("runtime-metadata", "codex-native-effects",
                            worker_id, create=True) as directory:
                with _flock_at(directory, key + ".lock", exclusive=True,
                               create=True, timeout=.5):
                    if _read_json(directory, key + ".json") is not None:
                        raise NativeEffectIndeterminate(
                            "original Codex native start already submitted; "
                            "no replay after uncertain provider response"
                        )
                    _write_json(directory, key + ".json", original)
                    store.revalidate()
                    # sole native IPC write, after both original fences.
                    try:
                        pending = client.begin_request(
                            method, request_params,
                            write_deadline=admission_deadline,
                        )
                    except ProtocolNotSent as transport_error:
                        # AppServerClient knows that this exact
                        # request wrote zero bytes. Unlike a generic
                        # ProtocolError, numeric errno, timeout or partial
                        # ProtocolTransportError, it is attributable to
                        # the current thread/start attempt. The original
                        # per-ID effect lock is still held; no second
                        # writer can overtake this terminal marker.
                        if (type(client) is not AppServerClient
                                or type(transport_error) is not ProtocolNotSent
                                or transport_error.method != method):
                            raise
                        local_reason = (
                            f"Codex original {method} not submitted: "
                            + transport_error.message
                        )
                        proof = _native_codex_local_proof(daemon, original, local_reason, "transport-zero-bytes",
                        )
                        try:
                            _write_json(directory, key + ".json", {
                                **original, "nativeNotSubmitted": proof,
                            })
                            local_marker_saved = True
                        except Exception as persistence_error:
                            local_marker_error = (
                                f"{type(persistence_error).__name__}: "
                                f"{persistence_error}"
                            )
    finally:
        if pending is not None:
            # Also register after a post-dispatch generation/lease failure:
            # a delivered provider reply must remain capturable.
            pending.add_done_callback(capture_native_result)
        else:
            # No positive original PendingRequest exists to retain.
            with daemon._native_start_hold_lock:
                if daemon._native_start_held.get(hold_key) is held:
                    del daemon._native_start_held[hold_key]
    if local_reason is not None:
        result = {
            "ok": False, "workerId": worker_id,
            "localRefusalReason": local_reason,
            "localRefusalProven": local_marker_saved,
            "error": (local_reason if local_marker_saved
                      else local_reason + "; original refusal marker unavailable: "
                      + str(local_marker_error)),
        }
        if local_marker_saved:
            try:
                sealed = store.reconcile_codex_local_refusal(
                    f"codex:{worker_id}"
                )
                if sealed is None or sealed.get("reason") != local_reason:
                    raise RuntimeError("native zero-byte refusal did not seal")
            except Exception as error:
                result["error"] = (
                    local_reason + "; durable marker retained, claim pending: "
                    + str(error)
                )
        return result
    return {
        "ok": True, "queued": True,
        "workerId": worker_id, "engine": "codex",
        "source": source, "submissionId": token,
        # Transport admission, never native thread binding.
    }


def _observe_original_codex_thread(daemon, message: dict[str, Any]) -> dict[str, Any]:
    """Observe an exact original candidate or replay its coded refusal.

    A provider thread with the same source tag but no local one-shot reply
    candidate is not attributable to this dispatch. No thread/list scans,
    native resume, or speculative recreation are allowed.
    """
    from ccx.store.root import NativeEffectIndeterminate, _read_json, _flock_at, _HASH
    from ccx.store.store import _digest
    import hashlib
    store = daemon._uuid_store
    owner = store.resolve(message.get("workerId"))
    if owner.engine != "codex":
        raise NativeEffectIndeterminate("Codex original full worker UUID required")
    wid = owner.worker_id
    source = f"ccx-worker:{wid}"
    activation = store.native_activation(owner.worker)
    token = message.get("submissionId")
    if (activation.get("source") != source
            or activation.get("submissionId") != token
            or not isinstance(token, str) or not token):
        raise NativeEffectIndeterminate("original submitted Codex activation unproven")
    if activation.get("state") == "rejected":
        sealed = store.reconcile_codex_native_rejection(owner.worker)
        if sealed is None:
            raise NativeEffectIndeterminate("original rejected claim lost provider proof")
        return {"ok": True, **sealed}
    if activation.get("state") == "local-refused":
        local = store.reconcile_codex_local_refusal(owner.worker)
        if local is None:
            raise NativeEffectIndeterminate("original local-refused claim lost proof")
        return {"ok": True, **local}
    # For submitted-unknown first allow the held reply to persist. An
    # unconditional effect read here could contend with the callback
    # before its original result was saved, or change original tamper
    # diagnostics. Marker-to-claim recovery follows the held step below.
    if activation.get("state") != "submitted-unknown":
        raise NativeEffectIndeterminate("original submitted Codex activation unproven")
    record = store.record(owner.worker)
    native = record.get("native") or {}
    if (record.get("fingerprintVersion") != 2
            or record.get("terminalOnly") or record.get("nativeCreate")
            or native.get("threadId") is not None
            or native.get("nativeState") != "not-started-proven"):
        raise NativeEffectIndeterminate("original Codex first native record not eligible")
    # Compare the immutable source/preimage, not mutable current display
    # name, short, provider thread id, or reconstructed operation hash.
    with store.lease():
        original_owner = store._owner(wid)
        claim = store._claim(wid)
        if not isinstance(original_owner, dict) or not isinstance(claim, dict):
            raise NativeEffectIndeterminate("Codex owner/create claim missing")
        fork = record.get("forkAnchor")
        if fork is not None:
            from ccx.store.store import fingerprint_v2, codex_fork_lineage
            lineage = record.get("forkedFrom")
            if (not isinstance(fork, dict) or fork.get("engine") != "codex"
                    or fork.get("cwd") != record.get("cwd")
                    or not isinstance(fork.get("createArgs"), dict)
                    or fingerprint_v2(
                        engine="codex", cwd=fork["cwd"],
                        create_args=fork["createArgs"], fork_anchor=fork,
                    ) != record["createSha256"]
                    or not isinstance(lineage, dict)
                    or lineage != codex_fork_lineage(fork, child_id=wid)):
                raise NativeEffectIndeterminate("Codex original fork lineage/point changed")
        store._check_original_create_intent(
            original_owner, engine="codex", worker_id=wid,
            fingerprint=record["createSha256"], native={}, anchor=fork,
        )
        if (claim.get("createSha256") != record["createSha256"]
                or claim.get("fingerprintVersion") != 2
                or claim.get("nativeBinding") != {}
                or claim.get("forkAnchor") != fork):
            raise NativeEffectIndeterminate("Codex original create hash mismatch")
        key = hashlib.sha256(("activate" + "\x00" + token).encode()).hexdigest()
    # Status handler may retry only an in-memory original positive native
    # reply whose callback met a recognized transient disk/lock failure.
    # Outside the owner lease: never nest a second lease across a native
    # observation, and never invoke thread/start, resume or list here.
    held = daemon._save_held_codex_candidate((store.generation, wid, token))
    if held["status"] == "saved":
        # A status handler may have just sealed the original rejection.
        # The same exact provider proof is also available offline from
        # its marker/claim; no native thread/read or native start needed.
        sealed = store.reconcile_codex_native_rejection(owner.worker)
        if sealed is not None:
            return {"ok": True, **sealed}
    if held["status"] == "unsafe":
        raise NativeEffectIndeterminate(
            "original Codex candidate persistence unsafe: " + held["error"]
        )
    if held["status"] == "transient":
        return {"ok": True, "status": "indeterminate", "workerId": wid,
                "candidatePersistence": "transient", "error": held["error"]}
    with store.lease():
        try:
            with store._gen("runtime-metadata", "codex-native-effects",
                            wid) as directory:
                with _flock_at(directory, key + ".lock", exclusive=False,
                               create=False, timeout=.5):
                    saved = _read_json(directory, key + ".json")
        except FileNotFoundError:
            return {"ok": True, "status": "indeterminate", "workerId": wid}
    if saved is None:
        return {"ok": True, "status": "indeterminate", "workerId": wid}
    if saved.get("nativeNotSubmitted") is not None:
        local = store.reconcile_codex_local_refusal(owner.worker)
        if local is None:
            raise NativeEffectIndeterminate("original local refusal marker unproven")
        return {"ok": True, **local}
    if saved.get("nativeRejected") is not None:
        sealed = store.reconcile_codex_native_rejection(owner.worker)
        if sealed is None:
            raise NativeEffectIndeterminate("original rejected marker not reconciled")
        return {"ok": True, **sealed}
    if (not isinstance(saved, dict) or any(saved.get(k) != v for k, v in {
            "workerId": wid, "engine": "codex", "generation": store.generation,
            "kind": "activate", "token": token, "state": "submitted-unknown",
            "source": source, "createSha256": record["createSha256"],
    }.items()) or (
        (saved.get("nativeMethod") != "thread/fork"
         or saved.get("forkAnchor") != fork)
        if fork is not None else
        ("nativeMethod" in saved or "forkAnchor" in saved)
    ) or not isinstance(saved.get("daemonInstanceId"), str)
            or not saved["daemonInstanceId"]
            or type(saved.get("daemonPid")) is not int
            or saved["daemonPid"] <= 0
            or type(saved.get("appServerPid")) is not int
            or saved["appServerPid"] <= 0):
        raise NativeEffectIndeterminate("original Codex one-shot marker invalid")
    if saved.get("nativeRejected") is not None:
        raise NativeEffectIndeterminate("native rejected marker contradicts candidate")
    candidate = saved.get("nativeCandidate")
    if candidate is None:
        return {"ok": True, "status": "indeterminate", "workerId": wid}
    if (not isinstance(candidate, dict)
            or not isinstance(candidate.get("threadId"), str)
            or not candidate["threadId"]
            or (fork is not None and candidate["threadId"] == fork["sourceNativeThreadId"])
            or candidate.get("submittedSource") != source
            or candidate.get("appServerPid") != saved["appServerPid"]
            or not isinstance(candidate.get("nativeReplySha256"), str)
            or not _HASH.fullmatch(candidate["nativeReplySha256"])):
        raise NativeEffectIndeterminate("Codex candidate lacks original effect proof")
    client = daemon.client
    if client is None or not daemon.ready.is_set():
        raise NativeEffectIndeterminate("Codex app-server not available to inspect")
    if fork is None:
        observed = client.request("thread/read", {
            "threadId": candidate["threadId"], "includeTurns": False,
        }, timeout=2)
        thread = observed.get("thread") if isinstance(observed, dict) else None
    else:
        from ccx.codex.history import read_original_thread
        try:
            thread = read_original_thread(
                client.request, candidate["threadId"],
                deadline=time.monotonic() + 2, limit=1, newest_first=True,
                expected_source=source,
            )
        except ValueError as error:
            raise NativeEffectIndeterminate(
                "independent Codex fork read did not prove original source"
            ) from error
    if (not isinstance(thread, dict)
            or thread.get("id") != candidate["threadId"]
            or thread.get("threadSource") != source):
        raise NativeEffectIndeterminate(
            "independent Codex thread/read did not prove original candidate source"
        )
    if fork is not None:
        turns = thread.get("turns")
        last = turns[-1] if isinstance(turns, list) and turns else None
        if (thread.get("forkedFromId") != fork["sourceNativeThreadId"]
                or not isinstance(last, dict)
                or last.get("id") != fork["lastTurnId"]
                or last.get("status") != "completed"):
            raise NativeEffectIndeterminate(
                "independent Codex fork thread/read lacks original source/point proof"
            )
    evidence = {
        "workerId": wid, "threadSource": source,
        "threadId": candidate["threadId"], "submissionId": token,
        "daemonPid": saved["daemonPid"],
        "appServerPid": saved["appServerPid"],
    }
    if fork is not None:
        # These facts come from one independent native read; the original
        # candidate stays the ordinary response/thread/PID/hash shape.
        evidence.update(
            nativeMethod="thread/fork", forkAnchor=fork,
            forkedFromId=fork["sourceNativeThreadId"],
            lastTurnId=fork["lastTurnId"], lastTurnStatus="completed",
            nativeReplySha256=candidate["nativeReplySha256"],
        )
    store.revalidate()
    return {
        "accepted": True, "workerId": wid, "engine": "codex",
        "source": source, "submissionId": token,
        "nativeEvidenceSha256": _digest(evidence),
        "nativeBinding": {"threadId": candidate["threadId"]},
    }


def _original_full_uuid_native(daemon, message: dict[str, Any]) -> dict[str, Any]:
    """One generation-bound Codex native action through the owning daemon.

    A client must have durably fsynced the original generation-bound
    native intent before this daemon can touch the provider. The daemon
    independently gates exact full owner/source/thread/receipt and
    publishes a second per-operation local single-dispatch marker before
    the app-server write: a lost socket response never replays native.
    """
    from ccx.store.root import NativeEffectIndeterminate, _read_json, _write_json, _flock_at
    from ccx.store.store import _digest
    from ccx.codex.delivery import read_receipt as semantic_receipt
    from ccx.store.identity import EngineMismatch
    import hashlib
    store = daemon._uuid_store
    store.revalidate()
    owner = store.resolve(message.get("workerId"))
    if owner.engine != "codex":
        raise EngineMismatch("Codex daemon needs exact owned Codex full UUID")
    worker_id = owner.worker_id
    action = message.get("op")
    record = store.record(owner.worker)
    native = record.get("native") or {}
    if not isinstance(native, dict):
        raise RuntimeError("Codex original native binding malformed")
    # First native activation still issues only a freshly minted UUID tag.
    source = (f"ccx-worker:{worker_id}" if action == "native-activate"
              else store.codex_source(owner.worker))
    if action == "native-observe":
        if native.get("threadSource") != source:
            raise RuntimeError("native Codex thread source not bound")
        thread_id = native.get("threadId")
        turn_id = message.get("turnId")
        delivery_id = message.get("deliveryId")
        if (not isinstance(thread_id,str) or not isinstance(turn_id,str)
                or not turn_id or not isinstance(delivery_id,str)
                or not delivery_id):
            raise RuntimeError("native completion requires original thread, turn and delivery")
        semantic = semantic_receipt(owner.worker, delivery_id)
        original_effect = store.read_receipt(
            owner.worker, "codex-send", delivery_id,
        )
        if (not isinstance(semantic,dict)
                or semantic.get("state") != "accepted"
                or semantic.get("turnId") != turn_id
                or not isinstance(original_effect,dict)
                or original_effect.get("state") != "accepted"
                or original_effect.get("workerId") != worker_id):
            raise RuntimeError(
                "native turn completion has no exact original accepted"
                " delivery and effect receipt"
            )
        deadline = time.monotonic() + _native_history_budget(message)
        from ccx.codex.history import read_original_thread
        thread = read_original_thread(
            daemon.client.request, thread_id, deadline=deadline,
            exact_turn_id=turn_id, expected_source=source,
        )
        if (not isinstance(thread,dict)
                or thread.get("id") != thread_id
                or thread.get("threadSource") != source):
            raise RuntimeError("native thread/read has no original full UUID source")
        matches = [
            turn for turn in (thread.get("turns") or [])
            if isinstance(turn,dict) and turn.get("id") == turn_id
        ]
        if len(matches) != 1:
            return {"ok":True, "status":"pending", "workerId":worker_id}
        native_status = matches[0].get("status")
        if native_status in ("failed", "interrupted", "cancelled"):
            # thread/read can report a still-running turn as interrupted;
            # trust it once this runtime no longer runs that turn.
            with daemon.lock:
                live = daemon.workers.get(owner.worker_id)
            if (live is not None and live.loaded and
                    live.record["lifecycle"].get("activeTurnId") == turn_id):
                return {"ok":True, "status":"pending", "workerId":worker_id}
            return {"ok":True, "status":native_status,
                    "workerId":worker_id, "turnId":turn_id,
                    "deliveryId":delivery_id,
                    "error":matches[0].get("error")}
        if native_status != "completed":
            return {"ok":True, "status":"pending", "workerId":worker_id}
        # A completed turn without an agent message is still completed.
        # Phase-aware final_answer wins over later phase-less text;
        # commentary alone is not a final reply.
        selected = select_report_for_turn(
            matches[0], live_report=record.get("lastReport"))
        text = selected["text"] if selected is not None else ""
        return {
            "ok":True, "status":"completed", "workerId":worker_id,
            "engine":"codex", "nativeSource":source,
            "threadId":thread_id, "turnId":turn_id,
            "deliveryId":delivery_id, "text":text,
            "nativeEvidenceSha256":_digest({
                "source":source, "threadId":thread_id,
                "turnId":turn_id,"status":"completed",
                "text":text,
            }),
        }

    if action == "native-activate":
        activation = store.native_activation(owner.worker)
        if (activation.get("state") != "submitted-unknown"
                or activation.get("source") != source
                or activation.get("submissionId") != message.get("submissionId")
                or native.get("threadId") is not None):
            raise RuntimeError("original Codex thread/create submitted claim unproven")
        token = activation["submissionId"]
        if (record.get("fingerprintVersion") != 2
                or record.get("terminalOnly") or record.get("nativeCreate")
                or native.get("nativeState") != "not-started-proven"):
            raise NativeEffectIndeterminate("original first Codex native record invalid")
        with store.lease():
            owner_entry, creation_claim = store._owner(worker_id), store._claim(worker_id)
            if not isinstance(owner_entry, dict) or not isinstance(creation_claim, dict):
                raise NativeEffectIndeterminate("original Codex create claim missing")
            fork = record.get("forkAnchor")
            if fork is not None:
                from ccx.store.store import fingerprint_v2, codex_fork_lineage
                lineage = record.get("forkedFrom")
                if (not isinstance(fork, dict) or fork.get("engine") != "codex"
                        or fork.get("cwd") != record.get("cwd")
                        or not isinstance(fork.get("createArgs"), dict)
                        or fingerprint_v2(
                            engine="codex", cwd=fork["cwd"],
                            create_args=fork["createArgs"], fork_anchor=fork,
                        ) != record["createSha256"]
                        or not isinstance(lineage, dict)
                        or lineage != codex_fork_lineage(fork, child_id=worker_id)):
                    raise NativeEffectIndeterminate("original Codex frozen fork claim invalid")
            store._check_original_create_intent(
                owner_entry, engine="codex", worker_id=worker_id,
                fingerprint=record["createSha256"], native={}, anchor=fork,
            )
            if (creation_claim.get("createSha256") != record["createSha256"]
                    or creation_claim.get("nativeBinding") != {}
                    or creation_claim.get("fingerprintVersion") != 2
                    or creation_claim.get("forkAnchor") != fork):
                raise NativeEffectIndeterminate("original Codex create hash changed")
        # Absolute elapsed budget from the CLI's original <=2-second
        # admission. Never reset it after this daemon's generation/effect
        # lease work. Older internal callers receive a bounded local
        # budget; no proof of no-submission is inferred from a timeout.
        admission_deadline = message.get("nativeAdmissionDeadline")
        if admission_deadline is not None:
            import math
            if (type(admission_deadline) not in (int, float)
                    or not math.isfinite(admission_deadline)
                    or admission_deadline - time.monotonic() > 2.0):
                raise NativeEffectIndeterminate("invalid Codex admission deadline")
        return daemon._submit_original_codex_thread(
            worker_id, record, source, token,
            admission_deadline=admission_deadline,
        )
    elif action == "native-send":
        from ccx.codex.delivery import intent_spec, requested_mode, mark_owned, mark_submitted
        from ccx.codex.provenance import parse_client_message_id
        from ccx.claude.usage import _merge_caps
        from ccx.workers import _check_caps

        caps = message.get("cap")
        if caps is not None:
            _check_caps(caps, engine="codex")

        store.authorize_native_action(owner.worker, "send")
        thread_id = native.get("threadId")
        if (not isinstance(thread_id, str) or not thread_id
                or native.get("threadSource") != source
                or thread_id != message.get("threadId")):
            raise RuntimeError("original Codex native thread differs")
        token = message.get("deliveryId")
        if not isinstance(token, str) or not token:
            raise RuntimeError("original delivery token required")
        receipt = semantic_receipt(owner.worker, token)
        effect = store.read_receipt(owner.worker, "codex-send", token)
        if not isinstance(receipt, dict):
            raise RuntimeError("original Codex semantic receipt absent")
        if receipt.get("cap") != caps:
            raise RuntimeError("original Codex cap differs from immutable delivery")
        mode = requested_mode(receipt)
        if (receipt.get("state") != (
                "submitted" if mode == "new_turn_only" else "prepared")
                or receipt.get("clientUserMessageId")
                != message.get("clientUserMessageId")
                or not isinstance(effect, dict)
                or effect.get("state") != "submitted-unknown"
                or effect.get("action") != "send"
                or effect.get("workerId") != worker_id
                or effect.get("generation") != store.generation
                or effect.get("intentSha256") != _digest(intent_spec(receipt))):
            raise RuntimeError("original Codex send effect/semantic receipt unproven")
        if (not isinstance(message.get("text"), str)
                or hashlib.sha256(message["text"].encode()).hexdigest()
                != receipt.get("promptSha256")):
            raise RuntimeError("original Codex send payload changed")
        original_worker = daemon._adopt_original_native_worker(owner.worker)
        if daemon.client is None or not daemon.ready.is_set():
            raise RuntimeError("Codex app-server not ready")

        def refused(code: str, detail: str, *,
                    current: dict | None = None) -> dict:
            frozen = current if current is not None else receipt
            chosen = send_receipt_route(frozen)
            target = frozen.get("targetTurnId")
            client_id = frozen.get("clientUserMessageId")
            return {
                "accepted": False, "nativeStatus": "rejected",
                "rejectionCode": code, "rejectionMessage": detail,
                "workerId": worker_id, "engine": "codex",
                "nativeSource": source, "threadId": thread_id,
                "deliveryId": token, "route": chosen,
                "targetTurnId": target, "clientUserMessageId": client_id,
                "nativeEvidenceSha256": _digest({
                    "source": source, "threadId": thread_id,
                    "deliveryId": token, "code": code,
                    "route": chosen, "targetTurnId": target,
                    "clientUserMessageId": client_id,
                }),
            }

        # Same operation lock as cancel/kill/compact, before store.lease.
        if original_worker.killed.is_set() or original_worker.kill_intent.is_set():
            return refused("worker-killed", "original worker is being killed")
        with original_worker.operation_lock:
            if not daemon._still_registered(original_worker):
                return refused("not-registered", "original worker is no longer registered")
            store.revalidate()
            latest = store.record(owner.worker)
            bound = latest.get("native") or {}
            if (bound.get("threadId") != thread_id
                    or bound.get("threadSource") != source):
                raise RuntimeError("original Codex native binding changed")
            with original_worker.lock:
                lifecycle = original_worker.record.get("lifecycle") or {}
                state = lifecycle.get("state")
                live = (original_worker.loaded
                        and not original_worker.killed.is_set()
                        and not original_worker.kill_intent.is_set()
                        and not original_worker.teardown_claimed)
                busy_compact = bool(original_worker.record.get("activeCompact"))
                current_turn = lifecycle.get("activeTurnId")
                launch_pending = original_worker.launch is not None
            route_kind, target_turn = None, None
            if live and not busy_compact and not launch_pending:
                if state == "idle" and mode != "steer_only":
                    route_kind = "new_turn"
                elif state == "busy" and mode != "new_turn_only":
                    if isinstance(current_turn, str) and current_turn:
                        route_kind, target_turn = "steer", current_turn
            if route_kind is None:
                if (live and not busy_compact and not launch_pending
                        and state == "busy" and mode == "new_turn_only"):
                    # Positive, lock-held proof: the live original owns a
                    # turn, so a strict new turn is refused before any RPC.
                    return refused(
                        "busy", f"original worker cannot accept {mode} in state=busy",
                    )
                return refused(
                    "compaction-busy" if busy_compact else "worker-state",
                    ("compaction owns the input surface" if busy_compact else
                     f"original worker cannot accept {mode} in state={state}"),
                )

            # Preflight before the provider turn, using a cached window or
            # one bounded account read when no fresh signal exists.
            if caps:
                trip = daemon._cap_at_admission(caps)
                if trip:
                    key, used, cap = trip
                    return refused(
                        "budget-stopped", f"usage cap {key} already exceeded "
                        f"({used:g}% >= {cap:g}%); no "
                        f"{'message' if route_kind == 'steer' else 'turn'} delivered",
                    )

            if mode != "new_turn_only":
                receipt = mark_owned(
                    owner.worker, token, route_kind=route_kind,
                    target_turn_id=target_turn,
                )
                receipt = mark_submitted(
                    owner.worker, token, runtime_id=daemon.instance_id,
                )
            else:
                receipt = semantic_receipt(owner.worker, token)
            if (receipt.get("state") != "submitted"
                    or send_receipt_route(receipt) != route_kind
                    or receipt.get("targetTurnId") != target_turn):
                raise RuntimeError("native Codex route not frozen in original receipt")
            client_id = receipt.get("clientUserMessageId")
            identity = parse_client_message_id(client_id)
            if (not isinstance(identity, dict)
                    or identity.get("workerId") != worker_id
                    or identity.get("kind") != (
                        "steer" if route_kind == "steer" else "start")):
                raise RuntimeError("original Codex native client message provenance invalid")

            # A later asynchronous turn notification can still move R;
            # expectedTurnId makes such a native steer a positive refusal.
            with original_worker.lock:
                current = original_worker.record.get("lifecycle") or {}
                if (original_worker.killed.is_set()
                        or original_worker.kill_intent.is_set()
                        or not original_worker.loaded
                        or original_worker.record.get("activeCompact")
                        or current.get("state") != (
                            "busy" if route_kind == "steer" else "idle")
                        or (route_kind == "steer"
                            and current.get("activeTurnId") != target_turn)):
                    return refused(
                        "worker-changed",
                        "original native turn or input owner changed before request",
                        current=receipt,
                    )

            key = hashlib.sha256(("send" + "\x00" + token).encode()).hexdigest()
            with store.lease():
                with store._gen(
                    "runtime-metadata", "codex-native-effects", worker_id,
                    create=True,
                ) as directory:
                    with _flock_at(
                        directory, key+".lock", exclusive=True,
                        create=True, timeout=.5,
                    ):
                        if _read_json(directory, key+".json") is not None:
                            raise NativeEffectIndeterminate(
                                "this original native Codex request was already "
                                "submitted, provider result unknown; no replay"
                            )
                        _write_json(directory, key+".json", {
                            "workerId": worker_id, "engine": "codex",
                            "generation": store.generation, "kind": "send",
                            "token": token, "requestedMode": mode,
                            "route": route_kind, "targetTurnId": target_turn,
                            "clientUserMessageId": client_id,
                            "intentSha256": effect["intentSha256"],
                            "state": "submitted-unknown",
                        })
                        store.revalidate()
                        images, rest = split_prompt_images(message["text"])
                        native_input = [{"type": "localImage", "path": path} for path in images]
                        if rest:
                            native_input.append({"type": "text", "text": rest})
                        if route_kind == "steer":
                            try:
                                daemon.client.request("turn/steer", {
                                    "threadId": thread_id,
                                    "expectedTurnId": target_turn,
                                    "input": native_input,
                                    "clientUserMessageId": client_id,
                                }, timeout=2)
                            except ProtocolError as error:
                                if (not isinstance(error, ProtocolTransportError)
                                        and error.request_id is not None):
                                    return refused(
                                        "native-rejected", str(error), current=receipt,
                                    )
                                raise
                            turn_id = target_turn
                        else:
                            turn_params = {
                                "threadId": thread_id,
                                "input": native_input,
                                "clientUserMessageId": client_id,
                            }
                            for setting in ("model", "effort"):
                                value = (latest.get("createArgs") or {}).get(setting)
                                if value:
                                    turn_params[setting] = value
                            with original_worker.lock:
                                if (original_worker.killed.is_set()
                                        or original_worker.kill_intent.is_set()):
                                    raise RuntimeError(
                                        "original worker is being killed; no native turn/start"
                                    )
                                launch = _TurnLaunch()
                                original_worker.launch = launch
                                lifecycle = original_worker.record["lifecycle"]
                                pre_launch_lifecycle = dict(lifecycle)
                                lifecycle["state"] = "busy"
                                lifecycle["since"] = now_ms()
                                lifecycle["activeTurnId"] = None
                                original_worker.record["nativeTurnStart"] = (
                                    original_worker._native_turn_start_marker(
                                        kind="send", client_message_id=client_id,
                                        delivery_id=token,
                                    )
                                )
                                original_worker._save()
                            turn_id = None
                            try:
                                response = daemon.client.request(
                                    "turn/start", turn_params, timeout=2,
                                )
                                turn = response.get("turn") if isinstance(response, dict) else None
                                turn_id = turn.get("id") if isinstance(turn, dict) else None
                                if not isinstance(turn_id, str) or not turn_id:
                                    raise NativeEffectIndeterminate(
                                        "Codex turn/start reply lacks exact native turn"
                                    )
                            except Exception as error:
                                with original_worker.lock:
                                    if (isinstance(error, ProtocolError)
                                            and not isinstance(error, ProtocolTransportError)
                                            and error.request_id is not None):
                                        original_worker.record["nativeTurnStart"] = None
                                        original_worker.record["lifecycle"] = pre_launch_lifecycle
                                        return refused(
                                            "native-rejected", str(error), current=receipt,
                                        )
                                    original_worker._mark_native_turn_start_unknown_locked(error)
                                raise
                            finally:
                                with original_worker.lock:
                                    if original_worker.launch is launch:
                                        original_worker.launch = None
                                    launch.turn_id = turn_id
                                    if turn_id:
                                        original_worker.record["nativeTurnStart"] = None
                                        original_worker.record["historyKnownEmpty"] = False
                                        if original_worker.record["lifecycle"].get("state") == "busy":
                                            original_worker.record["lifecycle"]["activeTurnId"] = (
                                                original_worker.record["lifecycle"].get("activeTurnId")
                                                or turn_id
                                            )
                                    try:
                                        original_worker._save()
                                    finally:
                                        launch.done.set()
                            if original_worker.killed.is_set():
                                original_worker._interrupt_turn(turn_id)
                                daemon_release._abandon_killed_load(daemon, original_worker)
                                raise RuntimeError(
                                    "original worker killed during native turn/start"
                                )
                        if caps:
                            with original_worker.lock:
                                if (original_worker.record["lifecycle"].get("activeTurnId")
                                        == turn_id
                                        and (original_worker.record.get("quotaStop") or {}).get("turnId")
                                        != turn_id):
                                    original_worker.cap_turn_id = turn_id
                                    original_worker.active_caps = _merge_caps([
                                        original_worker.active_caps
                                        if route_kind == "steer" else None,
                                        caps,
                                    ])
                            original_worker.check_active_caps(
                                daemon._cap_data_from_cache())
                        return {
                            "accepted": True, "workerId": worker_id,
                            "engine": "codex", "nativeSource": source,
                            "threadId": thread_id, "deliveryId": token,
                            "clientUserMessageId": client_id,
                            "route": route_kind, "targetTurnId": target_turn,
                            "turnId": turn_id,
                            "nativeEvidenceSha256": _digest({
                                "source": source, "threadId": thread_id,
                                "turnId": turn_id, "deliveryId": token,
                                "route": route_kind, "targetTurnId": target_turn,
                                "clientUserMessageId": client_id,
                            }),
                        }
    else:
        raise RuntimeError("unknown native Codex effect")


def _original_history_binding(daemon, message: dict[str, Any]):
    """Resolve one immutable original Codex worker/thread for read-only RPC."""
    ctx = daemon._uuid_store
    owner = ctx.resolve(message.get("workerId"))
    if owner.engine != "codex":
        raise ValueError("Codex history needs original full Codex worker UUID")
    original = ctx.record(owner.worker)
    native = original.get("native") or {}
    tid = native.get("threadId") if isinstance(native, dict) else None
    source = ctx.codex_source(owner.worker)
    claim = ctx.native_activation(owner.worker)
    binding = claim.get("nativeBinding")
    if (not isinstance(tid, str) or not tid
            or native.get("threadSource") != source
            or claim.get("state") != "confirmed"
            or claim.get("source") != source
            or not isinstance(binding, dict)
            or binding.get("threadId") != tid
            or message.get("threadId", tid) != tid):
        raise ValueError("native thread not positively bound to original full UUID")
    return owner, tid, source
