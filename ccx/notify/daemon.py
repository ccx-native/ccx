from __future__ import annotations
import fcntl
import os
import signal
from pathlib import Path
import subprocess
import sys
import threading
import time
from typing import Any
from ccx.system import oslayer
from ccx.constants import NOTIFYD_GENERATION
from ccx.errors import SessionUnavailable
from ccx.notify.paths import (
    daemon_generation_path,
    daemon_log_path,
    daemon_spawn_lock_path,
    ensure_dirs,
)
from ccx.notify.registry import has_pending_registrations, iter_registrations


def _read_notifyd_stamp() -> tuple[int, int, str, str] | None:
    """Read the sole published PID/protocol/ACTIVE/start identity.

    The daemon publishes all four fields after acquiring the role flock.
    Readers must not compete for that flock: even a momentary probe could
    prevent a starting daemon from acquiring its nonblocking role lease.
    """
    try:
        fields = daemon_generation_path().read_text(encoding="utf-8").split()
        if len(fields) == 4:
            pid, version = int(fields[0]), int(fields[1])
            generation, start = fields[2:]
            if (pid > 0 and version > 0 and generation
                    and oslayer.valid_kernel_start_token(start)):
                return pid, version, generation, start
    except (OSError, UnicodeError, ValueError):
        pass
    return None


def _notifyd_alive() -> bool:
    from ccx import constants
    from ccx.store.root import GenerationError, require_ccx_active
    from ccx.store.liveness import owned_process

    stamp = _read_notifyd_stamp()
    if stamp is None or stamp[1] != NOTIFYD_GENERATION:
        return False
    try:
        return (stamp[2] == require_ccx_active(constants.CCX_ROOT).generation
                and owned_process(stamp[0], stamp[3]) == "alive")
    except (GenerationError, OSError):
        return False


def _retire_incompatible_notifyd(*, timeout: float = 5.0) -> bool:
    """Three-state original identity: dead is stale, uncertain refuses signal.

    Return True if the current live daemon must be retained by the spawner.

    Registrations are durable authorities: restarting between a Codex write
    and receipt update is safe because the next supervisor reconciles the
    delivery ID against native parent history. A live old-protocol daemon
    may be retired only on full PID/start proof in the current ACTIVE
    generation; an unprovable process must never receive SIGTERM.
    """
    from ccx import constants
    from ccx.store.root import GenerationError, require_ccx_active
    from ccx.store.liveness import owned_process

    stamp = _read_notifyd_stamp()
    if stamp is None:
        return False  # no published identity, including a half-written start
    pid, version, generation, start = stamp
    presence = owned_process(pid, start)
    if presence == "dead":
        return False
    if presence != "alive":
        raise SessionUnavailable("notifyd PID/start presence unprovable")
    try:
        if generation != require_ccx_active(constants.CCX_ROOT).generation:
            raise SessionUnavailable("live notifyd belongs to another ACTIVE generation")
    except (GenerationError, OSError) as exc:
        raise SessionUnavailable("notifyd ACTIVE generation not proven") from exc
    if version == NOTIFYD_GENERATION:
        return True  # current live owner must never have its stamp unlinked
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return False
    except OSError as error:
        raise SessionUnavailable(f"cannot retire incompatible notifyd {pid}: {error}") from error
    deadline = time.monotonic() + max(0.0, timeout)
    while time.monotonic() < deadline:
        presence = owned_process(pid, start)
        if presence == "dead":
            return False
        if presence == "unprovable":
            raise SessionUnavailable(f"notifyd {pid} exit is unprovable after SIGTERM")
        time.sleep(0.05)
    raise SessionUnavailable(f"incompatible CCX notification daemon {pid} did not stop")


def _daemon_env() -> dict[str, str]:
    env = os.environ.copy()
    # A detached notifyd can start from an arbitrary caller cwd. Pin the CCX
    # package that is starting it, just like codexd, so a worktree never imports
    # a different installed/adjacent checkout after detaching.
    package_parent = str(Path(__file__).resolve().parents[2])
    inherited = env.get("PYTHONPATH")
    env["PYTHONPATH"] = (
        package_parent
        if not inherited
        else package_parent + os.pathsep + inherited
    )
    # A singleton notifyd must never inherit any managed/short/native caller
    # identity; the exact original parent is established by durable receipts.
    for key in (
        "CCX_SELF_SHORT", "CCX_SELF_WORKER_ID", "CCX_SELF_ENGINE",
        "CLAUDE_CODE_SESSION_ID", "CODEX_THREAD_ID",
    ):
        env.pop(key, None)
    return env


def ensure_notify_daemon(*, timeout: float = 5.0) -> None:
    """Ensure the one shared notification daemon for this CCX_ROOT is alive."""
    ensure_dirs()
    if _notifyd_alive():
        return
    with open(daemon_spawn_lock_path(), "a+") as spawn_lock:
        fcntl.flock(spawn_lock, fcntl.LOCK_EX)
        if _notifyd_alive():
            return
        if _retire_incompatible_notifyd(timeout=min(timeout, 5.0)):
            # It became current between the outer readiness check and the
            # spawn lock; preserve that daemon's identity rather than spawn.
            return
        daemon_generation_path().unlink(missing_ok=True)
        log_path = daemon_log_path()
        if log_path.exists() and log_path.stat().st_size > 10 * 1024 * 1024:
            log_path.replace(log_path.with_name(log_path.name + ".1"))
        log = open(log_path, "a", buffering=1)
        try:
            process = subprocess.Popen(
                [sys.executable, "-m", "ccx.notify"],
                cwd=str(Path(__file__).resolve().parents[2]),
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=log,
                start_new_session=True,
                close_fds=True,
                env=_daemon_env(),
            )
        except OSError as error:
            raise SessionUnavailable(f"could not start CCX notification daemon: {error}") from error
        finally:
            log.close()
        threading.Thread(
            target=process.wait, name="ccx-notifyd-reaper", daemon=True
        ).start()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if _notifyd_alive():
                return
            if process.poll() is not None:
                break
            time.sleep(0.05)
    raise SessionUnavailable("CCX notification daemon did not become ready")


def ensure_pending_notify_daemon() -> None:
    """Best-effort self-heal used by ordinary CLI invocations."""
    try:
        if has_pending_registrations():
            ensure_notify_daemon()
    except Exception:
        # A normal unrelated CCX command must not fail because a pending wake's
        # supervisor couldn't restart. The registration itself remains durable.
        pass


_revive_warning_lock = threading.Lock()


_revive_warned = False


def _log_revive_failure(error: Exception) -> None:
    """One bounded diagnostic per process, never an exception in a turn path."""
    global _revive_warned
    with _revive_warning_lock:
        if _revive_warned:
            return
        _revive_warned = True
    try:
        detail = f"{type(error).__name__}: {error}".replace("\n", " ")[:200]
        print(f"[ccx notifyd] turn-end revival failed: {detail}",
              file=sys.stderr, flush=True)
    except Exception:
        pass


def _revive_for_worker(engine: str, worker_id: str) -> None:
    try:
        # Terminal rows do not need delivery. Match the original full worker
        # identity, never a display name or prefix, before starting notifyd.
        if any((reg.get("worker") or {}).get("engine") == engine
               and (reg.get("worker") or {}).get("workerId") == worker_id
               for reg in iter_registrations()):
            ensure_notify_daemon()
    except Exception as error:
        _log_revive_failure(error)


def revive_notify_daemon_after_turn(engine: str, worker_id: str) -> None:
    """Recheck this worker's pending wakes off the host/provider event loop."""
    try:
        threading.Thread(target=_revive_for_worker, args=(engine, worker_id),
                         name="ccx-notifyd-revive", daemon=True).start()
    except Exception as error:
        _log_revive_failure(error)


def prepare_notify_parent(parent: dict[str, Any]) -> dict[str, Any]:
    """Start the durable delivery supervisor before a worker operation begins."""
    ensure_notify_daemon()
    return parent

import fcntl
import os
import signal
import time
from typing import Any
from ccx.constants import NOTIFYD_GENERATION
from ccx.store.liveness import proc_start_token
from ccx.notify.paths import (
    daemon_generation_path,
    daemon_lock_path,
    ensure_dirs,
)
from ccx.notify.deliver import DeliveryDeferred, deliver_to_parent, outstanding_for_parent, payload_for_registration
from ccx.notify.registry import TERMINAL_REGISTRATION_STATES, _current_worker_name, iter_registrations, mutate_registration, now_ms, prune_terminal_registrations
from ccx.notify.settle import settlement_for_registration


POLL_S = 0.35


IDLE_EXIT_S = 30.0


MAX_BACKOFF_S = 30.0


EXTERNAL_PARENT_EXPIRY_MS = 60 * 60 * 1000


def _retry_delay(attempts: int) -> float:
    return min(MAX_BACKOFF_S, max(1.0, 2 ** min(max(attempts - 1, 0), 5)))


def _mark_ready(event_id: str, settlement: dict[str, Any]) -> dict[str, Any] | None:
    def mutate(value: dict[str, Any]) -> None:
        if value.get("state") in TERMINAL_REGISTRATION_STATES:
            return
        if value.get("payload") is None:
            ready_settlement = settlement
            if value["worker"]["engine"] == "controller":
                from ccx.controllers.notify import progress as controller_progress
                ready_settlement = {
                    **settlement,
                    "activity": controller_progress(value),
                    "outstanding": outstanding_for_parent(value),
                    "workerName": _current_worker_name(value["worker"]),
                }
            value["settlement"] = ready_settlement
            value["payload"] = payload_for_registration(value, ready_settlement)
            value["readyAtMs"] = now_ms()
        value["state"] = "ready"
    return mutate_registration(event_id, mutate)


def _mark_delivered(event_id: str, result: dict[str, Any]) -> None:
    def mutate(value: dict[str, Any]) -> None:
        if value.get("state") in TERMINAL_REGISTRATION_STATES:
            return
        value["state"] = "delivered"
        value["deliveredAtMs"] = now_ms()
        value["delivery"] = result
        value["lastError"] = None
        value["nextAttemptAtMs"] = 0
    mutate_registration(event_id, mutate)


def _mark_deferred(event_id: str, error: BaseException) -> None:
    def mutate(value: dict[str, Any]) -> None:
        if value.get("state") in TERMINAL_REGISTRATION_STATES:
            return
        attempts = int(value.get("attempts") or 0) + 1
        value["state"] = "ready"
        if getattr(error, "no_effect", False):
            # Positive no-write refusal: same event can be attempted after the
            # original parent becomes ready. An ambiguous write keeps this
            # marker and can only reconcile the original machine delivery.
            value["deliveryAttemptedAtMs"] = None
            value["lastDeliveryAttemptStartedAtMs"] = None
        value["attempts"] = attempts
        value["lastAttemptAtMs"] = now_ms()
        value["lastError"] = f"{error.__class__.__name__}: {error}"[:1000]
        # A user's own Codex UI may close for good (or its daemon go away), so
        # an undeliverable external notification must not retry forever and
        # hold up every other one. Managed parents still retry.
        ready_at = int(value.get("readyAtMs") or value.get("createdAtMs") or now_ms())
        if (value["parent"]["kind"] == "codex-native-thread"
                and now_ms() - ready_at >= EXTERNAL_PARENT_EXPIRY_MS):
            value["state"] = "undeliverable"
            value["nextAttemptAtMs"] = 0
            return
        value["nextAttemptAtMs"] = now_ms() + int(_retry_delay(attempts) * 1000)
    mutate_registration(event_id, mutate)


def _mark_final_attempt_started(event_id: str, delivery_id: str) -> None:
    def mutate(value: dict[str, Any]) -> None:
        payload = value.get("payload")
        if not isinstance(payload, dict):
            return
        current_delivery = payload.get("deliveryId") or payload.get("eventId")
        if current_delivery != delivery_id:
            return
        current = now_ms()
        if value.get("deliveryAttemptedAtMs") is None:
            value["deliveryAttemptedAtMs"] = current
        value["lastDeliveryAttemptStartedAtMs"] = current
    mutate_registration(event_id, mutate)


def _deliver_final(reg: dict[str, Any], payload: dict[str, Any]) -> None:
    event_id = str(reg["eventId"])
    if int(reg.get("nextAttemptAtMs") or 0) > now_ms():
        return
    delivery_id = str(payload.get("deliveryId") or payload.get("eventId") or event_id)
    # Persist intent before the network call. The in-memory ``reg`` remains the
    # pre-attempt snapshot, so the first delivery goes straight to turn/start.
    # A retry/restart reloads the marker and reconciles native Codex history
    # before attempting the same delivery id again.
    _mark_final_attempt_started(event_id, delivery_id)
    try:
        result = deliver_to_parent(reg, payload)
    except DeliveryDeferred as error:
        _mark_deferred(event_id, error)
        return
    except Exception as error:
        # Unknown transport/runtime failures remain retryable. A completion
        # event is more valuable duplicated-and-reconciled than silently lost.
        _mark_deferred(event_id, error)
        return
    _mark_delivered(event_id, result)


def process_registration(
    reg: dict[str, Any],
    registration_snapshot: list[dict[str, Any]] | None = None,
) -> None:
    event_id = reg.get("eventId")
    if not isinstance(event_id, str) or not event_id:
        return
    if reg.get("state") in TERMINAL_REGISTRATION_STATES:
        return
    from ccx.notify.select import _original_notification_registration
    try:
        _original_notification_registration(reg)
    except Exception:
        # Never run an old short/generation registration through native paths.
        return

    # A frozen final payload is the only permissible parent delivery.
    payload = reg.get("payload")
    if isinstance(payload, dict):
        _deliver_final(reg, payload)
        return

    # An unresolved operation stays armed without periodic parent wakes.
    # Freeze its first actionable outcome for durable final delivery.
    from ccx.errors import CcxError
    from ccx.store.root import GenerationError
    from ccx.store.identity import IdentityError
    try:
        settlement = (
            settlement_for_registration(reg)
            if registration_snapshot is None
            else settlement_for_registration(
                reg, registration_snapshot=registration_snapshot
            )
        )
    except (CcxError, GenerationError, IdentityError, OSError):
        # One unreadable or inconsistent row stays armed for the next poll;
        # it must not stop the supervisor loop for every other registration.
        return
    if settlement is not None:
        reg = _mark_ready(event_id, settlement) or reg
        payload = reg.get("payload")
        if isinstance(payload, dict):
            _deliver_final(reg, payload)
        return


def run() -> int:
    from ccx import constants
    from ccx.store.root import require_ccx_active
    # One immutable ACTIVE generation for the entire daemon lifetime. An
    # offline pointer switch must end this notifier, never let it process a
    # new generation's registrations with an old process identity.
    store = require_ccx_active(constants.CCX_ROOT)
    ensure_dirs()
    lock = open(daemon_lock_path(), "a+")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return 0
    # Publish the complete identity only after claiming the singleton lock.
    # Readers do not take that flock: a temporary reader lease could make a
    # new daemon's nonblocking lock acquisition fail and suppress delivery.
    start = proc_start_token(os.getpid())
    if start is None:
        lock.close()
        raise RuntimeError("notification supervisor cannot prove its original PID start")
    identity_stamp = f"{os.getpid()} {NOTIFYD_GENERATION} {store.generation} {start}"
    daemon_generation_path().write_text(identity_stamp + "\n", encoding="utf-8")
    # Best-effort housekeeping under the singleton daemon lock. A malformed or
    # unreadable historical receipt must never prevent current notifications.
    try:
        prune_terminal_registrations()
    except Exception:
        pass
    stopping = False

    def stop(_signum, _frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    empty_since: float | None = None
    try:
        while not stopping:
            store.revalidate()
            registration_snapshot = iter_registrations(
                include_terminal=True
            )
            registrations = [
                reg for reg in registration_snapshot
                if reg.get("state") not in TERMINAL_REGISTRATION_STATES
            ]
            if registrations:
                empty_since = None
                for reg in registrations:
                    store.revalidate()
                    process_registration(
                        reg, registration_snapshot=registration_snapshot
                    )
            else:
                if empty_since is None:
                    empty_since = time.monotonic()
                elif time.monotonic() - empty_since >= IDLE_EXIT_S:
                    return 0
            time.sleep(POLL_S)
        return 0
    finally:
        try:
            current_stamp = daemon_generation_path().read_text(encoding="utf-8").strip()
            # A replaced daemon must never unlink another original's stamp.
            if current_stamp == identity_stamp:
                daemon_generation_path().unlink()
        except OSError:
            pass
        lock.close()
