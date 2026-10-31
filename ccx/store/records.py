from __future__ import annotations

import json
import os
import re
import tempfile
import time

from ccx.constants import SELF_SHORT_ENV, S
from ccx.store.paths import record_path, sessions_dir

NAME_MAX_LEN = 80

_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]+")
_WS_RE = re.compile(r"\s+")


def sanitize_name(name: str | None) -> str:
    """Normalize an operator-set display name to a safe single line.

    Control characters and newlines collapse to a single space, surrounding
    whitespace is stripped, and the result is capped at :data:`NAME_MAX_LEN`
    characters. Returns ``""`` for ``None`` / empty / whitespace-only input — the
    caller decides what default to substitute (the auto-name ``ccx-<short>``)."""
    if not name:
        return ""
    collapsed = _WS_RE.sub(" ", _CONTROL_RE.sub(" ", name)).strip()
    return collapsed[:NAME_MAX_LEN].strip()


def iter_session_records():
    """Read only global full-ID managed Claude owners for observational search."""
    store = _uuid_claude_store()
    for row in store.list_workers(engine="claude"):
        yield store.record(row["worker"])


def _uuid_claude_store():
    """One operational ACTIVE-generation gate; never initialize a legacy root."""
    from ccx.constants import CCX_ROOT
    from ccx.store.root import require_ccx_active
    return require_ccx_active(CCX_ROOT)


def read_record(target: str) -> dict:
    """Exact CCX-owned Claude full UUID only, no short/name/path fallback."""
    from ccx.store.identity import EngineMismatch
    store = _uuid_claude_store()
    owner = store.resolve(target)
    if owner.engine != "claude":
        raise EngineMismatch("Claude record lookup requires Claude full worker UUID")
    return store.record(owner.worker)


def write_record(rec: dict) -> None:
    """Explicit version-CAS only for existing CCX-owned Claude worker.

    Historical host code that passes a stale whole-record snapshot will
    fail RecordConflict. A missing revision, native session change, original
    fork/owner change or short-keyed record fails closed before publication.
    Prefer merge_lifecycle / GenerationStore.mutate_record for new writers.
    """
    from ccx.store.root import RecordConflict, StorageIntegrityError
    if not isinstance(rec, dict) or rec.get("engine") != "claude":
        raise StorageIntegrityError("Claude full-UUID record needs exact engine")
    worker_id = rec.get("workerId")
    revision = rec.get("recordRevision")
    if type(revision) is not int or revision < 0:
        raise RecordConflict(
            "Claude whole-record update requires fresh original recordRevision"
        )
    store = _uuid_claude_store()
    updated = store.replace_record_if_revision(
        f"claude:{worker_id}", rec, expected_revision=revision,
    )
    rec.clear()
    rec.update(updated)


def merge_lifecycle(target: str, **patch) -> dict:
    """One locked fresh-disk semantic patch; no stale whole-record write."""
    store = _uuid_claude_store()
    owner = store.resolve(target)
    if owner.engine != "claude":
        from ccx.store.identity import EngineMismatch
        raise EngineMismatch("Claude lifecycle update requires Claude full worker UUID")
    return store.patch_lifecycle(owner.worker, changes=patch)


def default_claude_name(worker_id: str) -> str:
    """Original v2 fallback display from a validated full UUID's short field."""
    from ccx.store.identity import WorkerOwner
    return "ccx-" + WorkerOwner(worker_id, "claude").short


def take_host_boot_fields(target: str) -> tuple[dict | None, dict]:
    """Atomically consume only host-owned run cap and stale waiting preview."""
    from ccx.store.root import RecordConflict
    store = _uuid_claude_store()
    for _ in range(6):
        current = store.record(target)
        cap = current.get("activeCap")
        if "activeCap" not in current and "waitingFor" not in current:
            return None, current
        try:
            committed = store.mutate_record(
                target, removals=("activeCap", "waitingFor"),
                expect={"activeCap": cap, "waitingFor": current.get("waitingFor")},
            )
            return cap, committed
        except RecordConflict:
            continue  # changed host transient: re-read its actual value
    raise RecordConflict("host boot transients changed during bounded consumption")


def restore_host_turn_count(target: str, minimum: int) -> dict:
    """Repair a sidecar-authoritative settled turn count, never decrease it."""
    from ccx.store.root import RecordConflict, StorageIntegrityError
    if type(minimum) is not int or minimum < 0:
        raise StorageIntegrityError("invalid host settled turn floor")
    store = _uuid_claude_store()
    for _ in range(6):
        current = store.record(target)
        value = (current.get("stats") or {}).get("turns")
        if type(value) is not int or value < 0:
            raise StorageIntegrityError("host turn counter malformed")
        if value >= minimum:
            return current
        try:
            return store.mutate_record(
                target, patches={"stats.turns": minimum},
                expect={"stats.turns": value},
            )
        except RecordConflict:
            continue
    raise RecordConflict("host turn count changed during bounded recovery")


def commit_host_turn_settlement(
    target: str, *, turn_ref: str, prior_turns: int, outcome: str,
    settled_at_ms: int, proposed_title: str | None = None,
    context: dict | None = None, background_task_summary: dict | None = None,
) -> dict:
    """Atomic/idempotent settlement, rebased only over fields this host owns.

    No lifecycle state is written here: an external terminal kill owns it.
    Exact turn ref and counter/time fields are one CAS, including after a lost
    acknowledgement. An independent CLI name/config patch is never lost.
    """
    from ccx.store.root import RecordConflict, StorageIntegrityError
    if (not isinstance(turn_ref, str) or not turn_ref
            or type(prior_turns) is not int or prior_turns < 0
            or outcome not in ("completed", "interrupted", "unknown")
            or type(settled_at_ms) is not int or settled_at_ms <= 0):
        raise StorageIntegrityError("invalid exact host settlement")
    store = _uuid_claude_store()
    for _ in range(8):
        current = store.record(target)
        turns = (current.get("stats") or {}).get("turns")
        prior = current.get("lastSettledTurn")
        if (type(turns) is not int or turns < 0
                or prior is not None and not isinstance(prior, dict)):
            raise StorageIntegrityError("host settlement state malformed")
        if prior and prior.get("turnRef") == turn_ref:
            if (prior.get("outcome") != outcome
                    or type(prior.get("turn")) is not int
                    or turns < prior["turn"]):
                raise RecordConflict("replayed host settlement contradicts original")
            return current
        if prior and type(prior.get("turn")) is int and prior["turn"] > prior_turns:
            raise RecordConflict("a newer distinct host turn is already settled")
        ordinal = max(turns, prior_turns) + 1
        settled_time = max(settled_at_ms, int(current.get("lastSettledAt") or 0))
        patches = {
            "stats.turns": ordinal,
            "lastSettledAt": settled_time,
            "lastSettledTurn": {
                "turn": ordinal, "turnRef": turn_ref, "outcome": outcome,
                "settledAtMs": settled_time, "context": context,
            },
        }
        if background_task_summary is not None:
            patches["backgroundTaskSummary"] = background_task_summary
        if outcome in ("completed", "interrupted"):
            patches["lastTurnAt"] = max(settled_time, int(current.get("lastTurnAt") or 0))
        expected = {"stats.turns": turns, "lastSettledTurn": prior}
        fallback = default_claude_name(target)
        if proposed_title and current.get("name") == fallback:
            adopted = sanitize_name(proposed_title)
            if adopted and adopted != fallback:
                patches["name"] = adopted
                expected["name"] = fallback
        try:
            return store.mutate_record(target, patches=patches, expect=expected)
        except RecordConflict:
            continue
    raise RecordConflict("host settlement changed during bounded rebase")


_HOST_TERMINAL_STATES = frozenset({S.DEAD, S.CRASHED, S.IDLE_KILLED, S.BUDGET_STOPPED})


def host_live_transition(target: str, *, new_state: str, host_pid: int) -> dict | None:
    """Only this host PID may publish a nonterminal state, never over a kill."""
    from ccx.store.root import RecordConflict
    if new_state in _HOST_TERMINAL_STATES:
        raise ValueError("terminal host state must be published after socket teardown")
    store = _uuid_claude_store()
    for _ in range(6):
        current = store.record(target)
        lifecycle = current.get("lifecycle") or {}
        if (lifecycle.get("state") in _HOST_TERMINAL_STATES
                or lifecycle.get("hostPid") != host_pid):
            return None
        try:
            return store.mutate_record(
                target, patches={
                    "lifecycle.state": new_state,
                    "lifecycle.since": int(time.time() * 1000),
                    "lifecycle.lastError": None,
                    "lifecycle.apiError": None,
                }, expect={"lifecycle.state": lifecycle.get("state"),
                           "lifecycle.hostPid": host_pid},
            )
        except RecordConflict:
            continue
    raise RecordConflict("host live state changed during bounded transition")


def host_terminal_transition(
    target: str, *, state: str, host_pid: int, last_error: str | None,
    clear_child: bool,
) -> dict:
    """Publish shutdown for this PID only; preserve another terminal verdict."""
    from ccx.store.root import RecordConflict
    if state not in _HOST_TERMINAL_STATES:
        raise ValueError("host final state must be terminal")
    store = _uuid_claude_store()
    for _ in range(6):
        current = store.record(target)
        lifecycle = current.get("lifecycle") or {}
        current_state = lifecycle.get("state")
        if lifecycle.get("hostPid") != host_pid:
            return current  # other/cleared host owner always wins
        patches = {"lifecycle.hostPid": None, "lifecycle.hostStart": None}
        if clear_child:
            patches.update({"lifecycle.workerPid": None, "lifecycle.workerStart": None})
        if current_state not in _HOST_TERMINAL_STATES:
            patches.update({
                "lifecycle.state": state,
                "lifecycle.since": int(time.time() * 1000),
                "lifecycle.lastError": last_error,
            })
        try:
            return store.mutate_record(
                target, patches=patches,
                expect={"lifecycle.state": current_state,
                        "lifecycle.hostPid": host_pid},
            )
        except RecordConflict:
            continue
    raise RecordConflict("terminal host state changed during bounded shutdown")


def spawned_by_from_env() -> dict | None:
    """Return attested full-UUID worker origin, or untrusted native metadata."""
    from ccx.constants import SELF_ENGINE_ENV, SELF_WORKER_ID_ENV
    from ccx.errors import SessionUnavailable
    from ccx.store.identity import InvalidTarget, canonical_worker_id
    old = os.environ.get(SELF_SHORT_ENV, "").strip()
    if old:
        raise SessionUnavailable(
            "stale CCX_SELF_SHORT cannot identify a full-UUID parent"
        )
    full = os.environ.get(SELF_WORKER_ID_ENV, "").strip()
    engine = os.environ.get(SELF_ENGINE_ENV, "").strip()
    if full or engine:
        if engine not in ("claude", "codex"):
            raise SessionUnavailable("CCX_SELF_ENGINE must be explicit")
        try:
            wid = canonical_worker_id(full)
        except InvalidTarget as exc:
            raise SessionUnavailable("CCX_SELF_WORKER_ID is not full UUIDv4") from exc
        try:
            store = _uuid_claude_store()
            owner = store.resolve(f"{engine}:{wid}")
        except Exception as exc:
            from ccx.store.root import GenerationError
            from ccx.store.identity import IdentityError
            if isinstance(exc, (GenerationError, IdentityError, OSError)):
                raise SessionUnavailable(
                    f"unproven full UUID caller identity/generation: {exc}"
                ) from exc
            raise
        return {"kind": "ccx-worker", "workerId": owner.worker_id,
                "engine": owner.engine}
    # Native provider IDs are not managed CCX worker routing authority.
    claude = os.environ.get("CLAUDE_CODE_SESSION_ID", "").strip()
    codex = os.environ.get("CODEX_THREAD_ID", "").strip()
    if claude and codex:
        return {"kind": "provider-native-unknown",
                "facts": {"claudeSessionId": claude, "codexThreadId": codex}}
    if claude:
        return {"kind": "provider-native-unknown", "engine": "claude",
                "nativeId": claude}
    if codex:
        return {"kind": "provider-native-unknown", "engine": "codex",
                "nativeId": codex}
    return None
