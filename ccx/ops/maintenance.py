"""Machine-level maintenance: stop CCX runtimes, undo CCX's trust writes.

Both commands act on what CCX itself owns and nothing else: global ``shutdown``
stops managed Claude hosts, Codex workers loaded in the shared codexd (then
codexd) and the parent-notification supervisor. An engine-scoped shutdown
leaves the other engine and the notification supervisor untouched. ``untrust``
reverts only the ``~/.claude.json`` trust entries recorded in CCX's ledger. External
Claude sessions (native background agents, interactive terminals) and running
controllers are never touched.
"""

from __future__ import annotations

import json
import os
import signal
import sys
import time

from ccx.constants import EXIT_UNAVAILABLE
from ccx.errors import CcxError
from ccx.ops.terminate import KILL_TIMEOUT_DEFAULT_S

_NOTIFYD_STOP_S = 5.0
_CODEXD_EXIT_S = 10.0


# --- shutdown ----------------------------------------------------------------

def _live_claude_workers(store) -> tuple[list[dict], list[dict]]:
    from ccx.store.liveness import owned_process
    from ccx.store.root import GenerationError, StorageIntegrityError
    from ccx.store.identity import IdentityError

    rows, errors = [], []
    # The ACTIVE generation's full-ID owners, never legacy sessions/shorts.
    for owner_row in store.list_workers(engine="claude"):
        row = {key: owner_row[key] for key in (
            "engine", "worker", "worker_id", "short", "name", "state")}
        try:
            record = store.record(row["worker"])
            lifecycle = record.get("lifecycle") or {}
            if not isinstance(lifecycle, dict):
                raise StorageIntegrityError("original Claude lifecycle is malformed")
            # Tokenless/unprovable PIDs remain visible so kill must refuse
            # rather than letting shutdown mistake uncertainty for absence.
            def present(pid, start):
                return owned_process(pid, start) != "dead"

            if present(lifecycle.get("hostPid"), lifecycle.get("hostStart")) or present(
                lifecycle.get("workerPid"), lifecycle.get("workerStart")
            ):
                rows.append(row)
        except (GenerationError, IdentityError, OSError) as error:
            row["action"] = "failed"
            rows.append(row)
            errors.append({"engine": "claude", "worker": row["worker"],
                           "short": row["short"], "error": str(error)})
    return rows, errors


def _loaded_codex_workers(store) -> tuple[str, list[dict], list[dict], int | None]:
    from ccx.codex.runtime import observe_registry
    from ccx.constants import CODEXD_RPC_GENERATION
    from ccx.store.root import GenerationError
    from ccx.store.identity import IdentityError

    try:
        registry = observe_registry(timeout=2.0)
    except (CcxError, OSError, ValueError) as error:
        registry = {"presence": "inconclusive", "reply": {"error": str(error)}}
    presence = registry.get("presence")
    if presence == "absent":
        return presence, [], [], None

    rows, errors = [], []
    raw = registry.get("reply")
    # A positively served response from an authenticated daemon of another
    # RPC generation is not a readable worker inventory. Only its generation
    # and the generation-agnostic daemon-stop operation can be used.
    version = raw.get("rpcGeneration") if isinstance(raw, dict) else None
    if (isinstance(raw, dict) and raw.get("transport") == "served"
            and raw.get("ok") is True and type(version) is int
            and version != CODEXD_RPC_GENERATION):
        return "older-generation", rows, errors, version
    workers = raw.get("workers") if isinstance(raw, dict) else None
    if presence != "served" or not isinstance(workers, dict):
        reason = raw.get("error") if isinstance(raw, dict) else presence
        errors.append({"engine": "codex", "worker": "codexd", "short": "codexd",
                       "error": f"codexd original registry unavailable or malformed: {reason}"})
        return "inconclusive", rows, errors, None
    for worker_id, state in sorted(workers.items()):
        handle = f"codex:{worker_id}"
        try:
            owner = store.resolve(handle)
            record = store.record(owner.worker)
            if not isinstance(state, str) or not state:
                raise ValueError("original Codex worker registry state malformed")
            rows.append({"engine": "codex", "worker": owner.worker,
                         "worker_id": owner.worker_id, "short": owner.short,
                         "name": record.get("name"), "state": state})
        except (GenerationError, IdentityError, OSError, ValueError) as error:
            errors.append({"engine": "codex", "worker": handle,
                           "short": str(worker_id), "error": str(error)})
    return presence, rows, errors, None


def _codexd_generation_note(version: int) -> str:
    from ccx.constants import CODEXD_RPC_GENERATION
    direction = "older" if version < CODEXD_RPC_GENERATION else "newer"
    return f"{direction} generation {version}"


def _stop_codexd(deadline: float | None, *, older_generation: int | None = None) -> str:
    from ccx.codex.runtime import daemon_generation, daemon_stop, observe_registry
    from ccx.constants import CODEXD_RPC_GENERATION

    # Older registry layouts cannot be enumerated/unloaded individually:
    # daemon teardown is the one cross-generation recovery operation. It
    # unloads its own workers as it exits and retains their native histories.
    reply = daemon_stop(force=older_generation is not None)
    if not reply.get("ok"):
        if reply.get("transport") == "absent":
            return "not-running"
        # Workers this shutdown did not kill are still active: ones a codexd
        # from another ccx version holds (its registry is unreadable here, so
        # they were never listed) or ones loaded after the kills.
        why = ("it is a codexd from another ccx version, whose workers this "
               "ccx cannot list or kill"
               if daemon_generation(reply) != CODEXD_RPC_GENERATION
               else "workers were loaded after the kills")
        raise CcxError(
            f"codexd stop rejected: {reply.get('error') or reply} ({why}); "
            "codexd remains running to protect those workers; resolve the "
            "loaded-worker ownership before retrying `ccx shutdown --engine codex`")
    wait_until = time.monotonic() + _CODEXD_EXIT_S
    if deadline is not None:
        wait_until = min(wait_until, deadline)
    while time.monotonic() < wait_until:
        if observe_registry(timeout=0.5)["presence"] == "absent":
            return (f"stopped ({_codexd_generation_note(older_generation)})"
                    if older_generation is not None else "stopped")
        time.sleep(0.1)
    raise CcxError("codexd accepted stop but is still serving")


def _bound_notifyd_pid(store) -> tuple[int, str] | None:
    """Only a positively owned, current-generation notifier can be stopped.

    A missing/malformed stamp denotes a daemon not yet serving. A proven
    dead original is stale metadata, not a shutdown blocker; uncertain
    PID/start observations must never authorize signalling.
    """
    from ccx.constants import NOTIFYD_GENERATION
    from ccx.store.liveness import owned_process
    from ccx.notify.daemon import _read_notifyd_stamp

    stamp = _read_notifyd_stamp()
    if stamp is None:
        return None  # no published serving identity
    pid, version, generation, start = stamp
    presence = owned_process(pid, start)
    if presence == "dead":
        return None
    if presence != "alive":
        raise CcxError("notification supervisor PID/start unprovable")
    if version != NOTIFYD_GENERATION or generation != store.generation:
        raise CcxError("notification supervisor version/ACTIVE generation incompatible")
    return pid, start


def _stop_notifyd(bound: tuple[int, str] | None) -> str:
    from ccx.constants import CCX_ROOT
    from ccx.store.root import require_ccx_active
    from ccx.store.liveness import owned_process

    if bound is None:
        return "not-running"
    pid, start = bound
    # Reprove the exact four-field stamp and PID/start once, at the signal.
    current = _bound_notifyd_pid(require_ccx_active(CCX_ROOT))
    if current is None:
        return "not-running"
    if current != bound:
        raise CcxError("notification supervisor identity changed before SIGTERM")
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return "not-running"
    except OSError as error:
        raise CcxError(f"cannot stop notification supervisor {pid}: {error}") from error
    wait_until = time.monotonic() + _NOTIFYD_STOP_S
    while time.monotonic() < wait_until:
        presence = owned_process(pid, start)
        if presence == "dead":
            return "stopped"
        if presence == "unprovable":
            raise CcxError(f"notification supervisor {pid} exit unprovable")
        time.sleep(0.05)
    raise CcxError(f"notification supervisor {pid} did not stop")


def shutdown(*, dry_run: bool = False, timeout: float | None = KILL_TIMEOUT_DEFAULT_S,
             engine: str | None = None) -> dict:
    """Confirmed CCX-owned shutdown, optionally engine-scoped; timeout per kill."""
    from ccx.constants import CCX_ROOT
    from ccx.store.root import GenerationError, require_ccx_active
    from ccx.store.identity import IdentityError

    if engine not in (None, "claude", "codex"):
        raise ValueError(f"unsupported shutdown engine: {engine}")

    errors: list[dict] = []
    claude_rows, codex_rows = [], []
    try:
        store = require_ccx_active(CCX_ROOT)
        if engine != "codex":
            claude_rows, claude_errors = _live_claude_workers(store)
            errors.extend(claude_errors)
    except (GenerationError, IdentityError, OSError) as error:
        # An unreadable/non-ACTIVE global owner index never means an empty
        # machine. No runtime control is safe before it is readable.
        return {"dry_run": dry_run, "claude": [], "codex": [],
                "codexd": "untouched" if engine == "claude" else "failed",
                "notifyd": "failed" if engine is None else "untouched",
                "errors": [{"engine": None, "worker": "inventory",
                            "short": "inventory", "error": str(error)}], "ok": False}
    codexd = "untouched"
    older_generation = None
    if engine != "claude":
        presence, codex_rows, codex_errors, older_generation = _loaded_codex_workers(store)
        errors.extend(codex_errors)
        codexd = ("not-running" if presence == "absent" else
                  "failed" if codex_errors else
                  f"would-stop ({_codexd_generation_note(older_generation)})"
                  if older_generation is not None else "would-stop")

    notifyd_pid = None
    notifyd = "untouched"
    if engine is None:
        try:
            notifyd_pid = _bound_notifyd_pid(store)
            notifyd = "would-stop" if notifyd_pid is not None else "not-running"
        except (CcxError, GenerationError, IdentityError, OSError) as error:
            notifyd = "failed"
            errors.append({"engine": None, "worker": "notifyd",
                           "short": "notifyd", "error": str(error)})

    if dry_run:
        for row in claude_rows + codex_rows:
            row.setdefault("action", "would-kill")
        return {"dry_run": True, "claude": claude_rows, "codex": codex_rows,
                "codexd": codexd, "notifyd": notifyd,
                "errors": errors, "ok": not errors}

    from ccx.ops.terminate import confirmed_kill_uuid

    for engine, rows in (("claude", claude_rows), ("codex", codex_rows)):
        for row in rows:
            if row.get("action") == "failed":
                continue
            try:
                result = confirmed_kill_uuid(store, row["worker"], timeout=timeout)
            except (CcxError, GenerationError, IdentityError, OSError) as error:
                row["action"] = "failed"
                errors.append({"engine": engine, "worker": row["worker"],
                               "short": row["short"], "error": str(error)})
                continue
            row["action"] = "killed" if result["changed"] else "already-down"
            row["state"] = result["state"]

    if codexd.startswith("would-stop"):
        try:
            deadline = None if timeout is None else time.monotonic() + max(0.0, timeout)
            codexd = (_stop_codexd(deadline, older_generation=older_generation)
                      if older_generation is not None else _stop_codexd(deadline))
        except CcxError as error:
            codexd = "failed"
            errors.append({"engine": "codex", "worker": "codexd",
                           "short": "codexd", "error": str(error)})
    if notifyd == "would-stop":
        try:
            notifyd = _stop_notifyd(notifyd_pid)
        except CcxError as error:
            notifyd = "failed"
            errors.append({"engine": None, "worker": "notifyd",
                           "short": "notifyd", "error": str(error)})
    return {"dry_run": False, "claude": claude_rows, "codex": codex_rows,
            "codexd": codexd, "notifyd": notifyd,
            "errors": errors, "ok": not errors}




# --- untrust -----------------------------------------------------------------
