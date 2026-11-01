from __future__ import annotations

import json
import sys
import threading
import time
import uuid
from copy import deepcopy
from typing import Any
from ccx.codex.evidence import CONTEXT_CHANGED_METHOD, BACKGROUND_CHANGED_METHOD, COMPACTION_CONTEXT_METHOD, COMPACT_STARTED_METHOD, COMPACT_ENDED_METHOD, INPUT_REQUESTED_METHOD, INPUT_RESOLVED_METHOD, JOURNALED_METHODS, LIFECYCLE_METHOD, EvidenceJournal, read_index as read_evidence_index, read_journal as read_compact_journal
from ccx.codex.notify_history import notification_delivery_match
from ccx.ops.background import label as background_label, summary as background_summary, materially_changed
from ccx.ops.context_events import ContextChanges
from ccx.codex.protocol import AppServerClient, ProtocolError, ProtocolNotSent, ProtocolTransportError
from ccx.codex.reports import native_timestamp_ms, report_from_item, should_replace_live_report
from ccx.codex.records import now_ms
from ccx.codex.daemon.models import _abandon_active_compact, WorkerKilled, _LAUNCH_SETTLE_CAP_S, _TurnLaunch, _bounded, _protocol_failure
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ccx.codex.daemon.daemon import CodexDaemon


_TERMINAL_STATES = {"dead", "idle-killed", "crashed", "budget-stopped"}


_PARENT_NOTIFY_FENCES = 32


TURN_OUTCOME_STATUSES = {"completed", "interrupted", "failed"}


APPROVAL_METHODS: dict[str, str] = {
    "item/commandExecution/requestApproval": "command",
    "item/fileChange/requestApproval": "fileChange",
}


def _turn_id(value: dict[str, Any]) -> str | None:
    turn = value.get("turn")
    return turn.get("id") if isinstance(turn, dict) else None


def _last_turn_from_native(turn: dict[str, Any]) -> dict[str, Any]:
    """Normalize one native ``Turn`` into the durable outcome projection.

    ``lastTurn`` deliberately contains only the terminal turn facts. In
    particular, timestamps come from Codex's Turn object; a missing native
    timestamp remains ``None`` instead of being replaced with CCX wall-clock
    time. Unknown native statuses are not promoted into the public outcome
    vocabulary.
    """
    status = turn.get("status")
    return {
        "turnId": turn.get("id"),
        "status": status if status in TURN_OUTCOME_STATUSES else None,
        "error": turn.get("error"),
        "startedAtMs": native_timestamp_ms(turn.get("startedAt")),
        "completedAtMs": native_timestamp_ms(turn.get("completedAt")),
        "durationMs": (
            turn.get("durationMs")
            if isinstance(turn.get("durationMs"), (int, float))
            and not isinstance(turn.get("durationMs"), bool)
            else None
        ),
    }




def _approval_key(approval: dict[str, Any]) -> str:
    """One pending approval's identity: its JSON-RPC request id, typed."""
    return json.dumps(approval.get("requestId"), sort_keys=True, default=str)


class Worker:
    """One CCX-owned Codex worker, driven over the daemon's shared client."""

    def __init__(self, daemon: "CodexDaemon", record: dict[str, Any]):
        self.daemon = daemon
        # Original local short variable is internally a canonical full owner.
        # Eight-character display prefixes never identify a worker here.
        from ccx.store.identity import canonical_worker_id
        self.worker_id = canonical_worker_id(record.get("workerId"))
        if record.get("engine") != "codex":
            raise ValueError("daemon Worker requires globally owned Codex ID")
        self.record = deepcopy(record)
        self._disk_baseline = deepcopy(record)
        self.short = self.worker_id
        self.lock = threading.RLock()
        self.operation_lock = threading.Lock()
        self.load_lock = threading.Lock()
        self.loaded = False
        # Set by `kill` before it unloads, and never cleared: a Worker object
        # that has been killed is dead forever, and a fresh `load` builds a new
        # one. This is the whole kill/load race protocol — see `_abort_if_killed`.
        self.killed = threading.Event()
        # Published before kill waits for `operation_lock`, and never cleared.
        # A kill that has to wait behind an in-flight steer/approval must not
        # meanwhile let new operations in — but it also must not bypass the
        # lock and race the operation that is already running. The sticky
        # intent separates the two: new work is refused immediately, the
        # running op is allowed to settle, and only then does the kill act.
        self.kill_intent = threading.Event()
        # Non-None exactly while a `turn/start` request is in flight. Written
        # only under `self.lock`, which is what makes kill-vs-launch atomic.
        self.launch: _TurnLaunch | None = None
        # Caps belong to one original native turn, never to the worker's next turn.
        self.cap_turn_id: str | None = None
        self.active_caps: dict | None = None
        # A source-proven resume may return the preceding failed turn's
        # persisted systemError. Until the next native turn/status transition,
        # another systemError edge is that old state, not a new crash.
        self._resumed_system_error_pending = False
        # Native threads this object's load may have subscribed the shared
        # connection to. Codex keeps a thread loaded (with its per-thread MCP
        # servers) until its last subscriber leaves, so `_unload` releases
        # these. Kept rather than cleared: unsubscribing twice is harmless, and
        # a kill that lands mid-resume unloads before the subscription exists.
        self.native_threads: set[str] = set()
        # Parent-notification deliveries whose `turn/start` this object saw
        # answered, so a retry walking history outside the operation lock
        # cannot launch one twice. delivery id -> native turn id.
        self.launched_notifications: dict[str, str | None] = {}
        # Claimed under `daemon.lock` by the one `_unload` that tears this
        # object down; see there.
        self.teardown_claimed = False
        self.last_activity = time.monotonic()
        self.attach_count = 0  # live attach RPC connections, never persisted
        # Native commands become background only if still open at turn/completed.
        # The journal is the source; these are this app-server's owned live IDs.
        # Public context emission is one sparse stream, independent of raw native
        # usage notifications. Reuse the existing daemon janitor for pending flush.
        self._context_changes = ContextChanges()
        self._bg_candidates: dict[str, dict] = {}
        self._bg_open: dict[str, dict] = {}
        # No replay ledger: a persisted snapshot is enough to compare current
        # registry truth when this worker is adopted or notified again.
        prior_items = (record.get("backgroundTaskSummary") or {}).get("items") or []
        self._bg_notifications = {
            item["id"]: dict(item) for item in prior_items
            if isinstance(item, dict) and item.get("kind") == "notification"
            and item.get("status") == "pending"
            and isinstance(item.get("id"), str)
        }
        # The last record write a notification handler could not make (disk
        # full, permissions). The in-memory record stays authoritative.
        self.persistence_error: dict[str, Any] | None = None
        # An external terminal CAS wins over this cached Worker forever.
        self._externally_terminalized = False
        # Sparse durable evidence for this worker's turns. Opened lazily on
        # the first journaled notification, so a worker that never runs one
        # leaves no file — which is itself the truthful answer to "was CCX
        # watching?".
        # `health` rehydrates a degradation an earlier process already
        # recorded: a fresh writer must not present a worker whose evidence is
        # known to be incomplete as complete again.
        self.evidence = EvidenceJournal(
            self.short,
            codex_version=getattr(daemon, "codex_version", None),
            health=record.get("evidence"),
        )
        try:
            evidence_index = read_evidence_index(self.short)
        except Exception:
            evidence_index = {}
        self.completed_turn_ids = {
            str(turn_id)
            for turn_id, facts in (evidence_index.get("turns") or {}).items()
            if isinstance(facts, dict) and facts.get("sawTurnCompleted")
        }
        # Journal-derived identities keep compaction end publication idempotent
        # when native notifications repeat or codexd reloads an owned worker.
        self._compact_started: dict[str, set[str]] = {}
        self._compact_finished: dict[str, set[str]] = {}
        self._compact_ended: set[str] = set()
        self._compact_legacy: set[str] = set()
        try:
            compact_history = read_compact_journal(self.short).events
        except Exception:
            compact_history = []
        for row in compact_history:
            tid, item_id, method = row.get("turnId"), row.get("itemId"), row.get("method")
            if method == "thread/compacted" and isinstance(tid, str):
                self._compact_legacy.add(tid)
            if not isinstance(tid, str) or not tid:
                continue
            # Historical native items are still forensic proof, not feed
            # events. Only CCX-owned sparse lines rehydrate publisher dedupe.
            # Older journals lacking them intentionally gain no backfill.
            if (row.get("itemType") == "contextCompaction"
                    and isinstance(item_id, str)
                    and method == "item/completed"):
                self._compact_finished.setdefault(tid, set()).add(item_id)
            if method == COMPACT_STARTED_METHOD:
                key = (row.get("params") or {}).get("compactId")
                if isinstance(key, str):
                    self._compact_started.setdefault(tid, set()).add(key)
            if method == COMPACT_ENDED_METHOD:
                key = (row.get("params") or {}).get("compactId")
                if isinstance(key, str):
                    self._compact_ended.add(key)
        # What the journal last said about this worker's state and approvals.
        # `_journal_lifecycle_locked` diffs the record against it on every
        # save, so each transition is journaled exactly once whichever of the
        # many record-mutating paths made it.
        self.journaled_state = (record.get("lifecycle") or {}).get("state")
        self.journaled_approvals = {
            _approval_key(approval): approval
            for approval in record.get("pendingApprovals") or []
            if isinstance(approval, dict)
        }
        # Decisions CCX itself sent, keyed like `journaled_approvals`, so the
        # resolution line can say how a request was answered.
        self.approval_decisions: dict[str, str] = {}
        # The caller's response id for a decision, journaled with it so a
        # caller that lost the reply can prove its decision landed.
        self.approval_response_ids: dict[str, str] = {}
        last_turn = record.get("lastTurn")
        if (
            isinstance(last_turn, dict)
            and isinstance(last_turn.get("turnId"), str)
            and last_turn.get("turnId")
        ):
            self.completed_turn_ids.add(str(last_turn["turnId"]))

    @property
    def client(self) -> AppServerClient:
        client = self.daemon.client
        if client is None:
            raise RuntimeError("app-server client is not running")
        return client

    def _save(self) -> None:
        """Save once with field CAS; retry only a proved safe fresh-record rebase.

        A failed CAS is not authority to drop expectations. In particular a
        concurrent CLI kill owns the terminal lifecycle, even if an app-server
        completion races it. The original native event remains in the evidence
        journal when a conflict cannot be reconciled.
        """
        from ccx.store.root import RecordConflict
        with self.lock:
            if self._externally_terminalized and (
                self.record.get("lifecycle") != self._disk_baseline.get("lifecycle")
                or self.record.get("pendingApprovals")
                != self._disk_baseline.get("pendingApprovals")
            ):
                # This object saw an external terminal CAS. Even another
                # daemon-local _state/approval writer cannot bring it back;
                # restore the current persisted terminal view before refusing.
                latest = self.daemon._uuid_store.record(f"codex:{self.worker_id}")
                self.record = deepcopy(latest)
                self._disk_baseline = deepcopy(latest)
                raise RecordConflict("externally terminalized Codex Worker cannot revive")
            self._journal_lifecycle_locked()
            try:
                self._save_current_locked()
            except RecordConflict:
                self._rebase_conflicting_save_locked()
                self._journal_lifecycle_locked()
                # One bounded retry against the newly observed exact fields.
                # A second conflict is surfaced per worker, never overwritten.
                self._save_current_locked()

    def _save_current_locked(self) -> None:
        """One semantic CAS publication; caller holds self.lock."""
        from ccx.store.root import StorageIntegrityError, RecordConflict
        if (self._externally_terminalized
                and self.record["lifecycle"].get("state") not in _TERMINAL_STATES):
            raise RecordConflict("external terminal state cannot be resurrected")
        before = self._disk_baseline
        desired = self.record
        patches: dict[str, object] = {}
        expect: dict[str, object] = {}
        increments: dict[str, int] = {}
        containers = ("lifecycle", "stats", "createArgs")
        top_allowed = {
            "name", "waitingFor", "lastTurnAt", "lastSettledAt",
            "status", "lastTurn", "lastReport", "activeTurnError",
            "pendingApprovals", "context", "activeCompact", "lastCompact",
            "compactCount", "evidence", "tokenUsage", "historyKnownEmpty",
            "nativeArchive", "resolvedModel", "parentNotifyFences",
            "lastTurnError", "pendingSend", "lastConversation", "nativeTurnStart",
            "quotaStop", "backgroundTaskSummary",
        }
        for key in set(before) | set(desired):
            if key in ("recordRevision", "updatedAt"):
                continue
            old = before.get(key)
            new = desired.get(key)
            if new == old:
                continue
            if key not in desired:
                raise StorageIntegrityError(
                    "Codex Worker cannot delete a durable record field"
                )
            if key in containers:
                if not isinstance(old, dict) or not isinstance(new, dict):
                    raise StorageIntegrityError(
                        "Codex Worker cannot replace structured record"
                    )
                if not set(old).issubset(new):
                    raise StorageIntegrityError(
                        "Codex Worker cannot remove structured fields"
                    )
                for member in new:
                    if member in old and old[member] == new[member]:
                        continue
                    field = f"{key}.{member}"
                    if (key == "stats"
                            and type(new[member]) is int
                            and type(old.get(member)) is int
                            and new[member] >= old[member]):
                        increments[field] = new[member] - old[member]
                    else:
                        patches[field] = new[member]
                        expect[field] = old.get(member)
            elif key in top_allowed:
                patches[key] = new
                expect[key] = old
            else:
                raise StorageIntegrityError(
                    f"Codex Worker cannot mutate immutable native/owner field {key}"
                )
        ctx = self.daemon._uuid_store
        if patches or increments:
            durable = ctx.mutate_record(
                f"codex:{self.worker_id}",
                patches=patches, increments=increments,
                expect=expect,
            )
        else:
            durable = ctx.record(f"codex:{self.worker_id}")
        # A nonconflicting metadata/evidence patch (or even a pure read) can
        # absorb another process's terminal kill. Recognize its authority on
        # every fresh durable observation, not only in the conflict rebase.
        old_state = (before.get("lifecycle") or {}).get("state")
        committed_state = (durable.get("lifecycle") or {}).get("state")
        proposed_state = (desired.get("lifecycle") or {}).get("state")
        if (committed_state in _TERMINAL_STATES
                and old_state not in _TERMINAL_STATES
                and proposed_state != committed_state):
            self._externally_terminalized = True
            self.killed.set()
        self.record = durable
        self._disk_baseline = deepcopy(durable)


    def _rebase_conflicting_save_locked(self) -> None:
        """Reconcile one same-worker CAS conflict, never ignore its expectations.

        Fresh latest data is the base. Replay a local edit only if its exact
        previous value is still present (or the edit is already durable). A
        positive integer stat increment is commutative, except a completion
        already recorded for the same native turn must not be counted twice.
        A concurrent terminal state wins over *all* cached lifecycle and
        approval projections. Unprovable overlaps remain visibly unresolved.
        """
        from ccx.store.root import RecordConflict, StorageIntegrityError
        before, desired = self._disk_baseline, self.record
        latest = self.daemon._uuid_store.record(f"codex:{self.worker_id}")
        if (latest.get("workerId") != self.worker_id
                or latest.get("engine") != "codex"
                or latest.get("native") != before.get("native")):
            raise StorageIntegrityError("Codex rebase changed immutable native owner")
        state = (latest.get("lifecycle") or {}).get("state")
        terminal_won = (state in _TERMINAL_STATES
                        and (before.get("lifecycle") or {}).get("state")
                        not in _TERMINAL_STATES)
        if terminal_won:
            self._externally_terminalized = True
            self.killed.set()  # also fences this cached worker's future launches

        def refuse(message: str) -> None:
            # Do not retain stale state that will conflict on every later event,
            # or an approval that was never durably admitted. Native evidence
            # was journaled independently; the handler exposes the lost patch.
            self.record = deepcopy(latest)
            self._disk_baseline = deepcopy(latest)
            raise RecordConflict(message)

        old_turn = (before.get("lastTurn") or {}).get("turnId")
        wanted_turn = (desired.get("lastTurn") or {}).get("turnId")
        disk_turn = (latest.get("lastTurn") or {}).get("turnId")
        if terminal_won:
            old_approvals = before.get("pendingApprovals") or []
            new_approvals = desired.get("pendingApprovals") or []
            if any(approval not in old_approvals for approval in new_approvals):
                refuse("external kill won before Codex approval was durable")
            if wanted_turn != old_turn and (not isinstance(wanted_turn, str)
                    or (before.get("lifecycle") or {}).get("activeTurnId")
                    != wanted_turn or disk_turn not in (old_turn, wanted_turn)):
                refuse("terminal worker has no positive original turn to reconcile")

        rebased = deepcopy(latest)
        containers = ("lifecycle", "stats", "createArgs")
        for key in set(before) | set(desired):
            if key in ("recordRevision", "updatedAt") or before.get(key) == desired.get(key):
                continue
            old, new = before.get(key), desired.get(key)
            if key not in desired:
                raise StorageIntegrityError("Codex cannot rebase a deleted field")
            if key in containers:
                if (not isinstance(old, dict) or not isinstance(new, dict)
                        or not isinstance(rebased.get(key), dict)
                        or not set(old).issubset(new)):
                    raise StorageIntegrityError("Codex cannot rebase malformed structured fields")
                changes = ((f"{key}.{member}", old.get(member), value)
                           for member, value in new.items()
                           if member not in old or old[member] != value)
            else:
                changes = ((key, old, new),)
            for field, previous, value in changes:
                # Terminal status/cleared active turn and approvals are not
                # native completion effects. Never revive them via a stale save.
                if terminal_won and (field.startswith("lifecycle.") or field in {
                        "pendingApprovals", "waitingFor", "pendingSend",
                        "activeTurnError", "name"} or field.startswith("createArgs.")):
                    continue
                parts = field.split(".")
                container = rebased if len(parts) == 1 else rebased[parts[0]]
                current = container.get(parts[-1])
                if (field.startswith("stats.")
                        and type(previous) is int and type(value) is int
                        and value >= previous):
                    if type(current) is not int:
                        refuse(f"{field} cannot be merged as a counter")
                    delta = value - previous
                    if field == "stats.turns" and wanted_turn != old_turn:
                        if disk_turn == wanted_turn:
                            delta = 0  # this same turn was already committed
                        elif disk_turn != old_turn:
                            refuse("newer native turn prevents counter reconciliation")
                    container[parts[-1]] = current + delta
                elif current == previous:
                    container[parts[-1]] = deepcopy(value)
                elif current != value:
                    refuse(f"record semantic field changed before CAS: {field}")
        self.record = rebased
        self._disk_baseline = deepcopy(latest)

    def _journal_lifecycle_locked(
        self,
        *,
        state: str | None = None,
        approvals: list[dict[str, Any]] | None = None,
    ) -> None:
        """Journal state and approval transitions not yet in the journal.

        Caller holds ``self.lock``. ``state``/``approvals`` override the
        record's, for a teardown that journals its terminal facts before it
        closes the journal. Never raises (``EvidenceJournal.record`` does
        not), and writes nothing once the journal is closed: reopening would
        start a new header for a worker that is already gone.
        """
        if self.evidence.closed:
            return
        lifecycle = self.record.get("lifecycle") or {}
        current_state = state if state is not None else lifecycle.get("state")
        current = {
            _approval_key(approval): approval
            for approval in (
                approvals if approvals is not None
                else self.record.get("pendingApprovals") or []
            )
            if isinstance(approval, dict)
        }
        wrote = False
        thread_id = (self.record.get("native") or {}).get("threadId")
        for key, approval in self.journaled_approvals.items():
            if key not in current:
                params = {
                    "requestId": approval.get("requestId"),
                    "gateId": approval.get("gateId"),
                    "kind": approval.get("kind"),
                    "decision": self.approval_decisions.pop(key, None),
                }
                response_id = self.approval_response_ids.pop(key, None)
                if response_id is not None:
                    params["responseId"] = response_id
                self.evidence.record(
                    INPUT_RESOLVED_METHOD, params, thread_id=thread_id,
                )
                wrote = True
        for key, approval in current.items():
            if key not in self.journaled_approvals:
                self.evidence.record(
                    INPUT_REQUESTED_METHOD, {"request": approval},
                    thread_id=thread_id,
                )
                wrote = True
        self.journaled_approvals = current
        if current_state and current_state != self.journaled_state:
            params: dict[str, Any] = {
                "from": self.journaled_state,
                "to": current_state,
            }
            if current_state in {"crashed", "dead", "idle-killed"}:
                params["reason"] = lifecycle.get("lastError")
            self.evidence.record(LIFECYCLE_METHOD, params, thread_id=thread_id)
            self.journaled_state = current_state
            wrote = True
        if wrote:
            self.record["evidence"] = self.evidence.state()


    def _abort_if_killed(self) -> None:
        """Unwind a load that a concurrent ``kill`` has already invalidated.

        Original resume spends most of its time inside app-server RPCs holding
        no daemon or worker lock — deliberately, so the daemon stays responsive.
        That means a `kill` can complete (record ``dead``, worker removed from
        `workers`/`by_thread`) while this load is still in flight, and every
        step after the RPC would then resurrect it: re-binding the thread,
        rewriting the record and finally reporting ``ok``.

        Rather than have `kill` block on `load_lock` — which would stall it
        behind a 90s app-server call — the loader checks this event at each of
        those points and unwinds itself. Reading an Event takes no lock, so it
        cannot deadlock against the killer.
        """
        if self.killed.is_set():
            raise WorkerKilled(f"worker {self.short} was killed during load")

    def _commit_state(self, state: str, **lifecycle_patch) -> None:
        """Write a loader state transition atomically against ``kill``.

        A bare `_abort_if_killed()` before `_state()` is not enough: a kill
        landing in the gap writes ``dead`` and then this write puts the record
        back to ``idle``/``busy``, leaving a killed worker looking alive. The
        check and the write therefore happen inside one hold of ``self.lock``,
        which is the same lock ``kill`` sets its marker under — so the two are
        totally ordered and the loser is the one that does nothing.
        """
        with self.lock:
            if self.killed.is_set():
                raise WorkerKilled(
                    f"worker {self.short} was killed during load")
            lifecycle = self.record["lifecycle"]
            lifecycle.update(lifecycle_patch)
            lifecycle["state"] = state
            lifecycle["since"] = now_ms()
            self.last_activity = time.monotonic()
            self._save()

    def _interrupt_turn(self, turn_id: str | None) -> bool:
        """Best-effort native cancellation of one turn. Safe to call twice."""
        thread_id = (self.record.get("native") or {}).get("threadId") or self.record.get("threadId")
        if not turn_id or not thread_id:
            return False
        try:
            self.client.request(
                "turn/interrupt", {"threadId": thread_id, "turnId": turn_id})
            return True
        except Exception:
            return False

    def check_active_caps(self, rate_limits: dict | None) -> None:
        """Check a pushed quota update without RPC on the notification reader."""
        from ccx.codex.types import codex_cap_tripped
        with self.lock:
            turn_id = self.cap_turn_id
            if (not turn_id or not self.active_caps
                    or self.record["lifecycle"].get("activeTurnId") != turn_id):
                return
            trip = codex_cap_tripped(rate_limits, self.active_caps)
            if not trip:
                return
            name, used, cap = trip
            reason = f"usage cap {name} reached: {used:g}% >= {cap:g}%"
            self.record["quotaStop"] = {"turnId": turn_id, "reason": reason}
            self.active_caps = None  # at most one interrupt for this native turn
            self._save()
        threading.Thread(target=self._interrupt_turn, args=(turn_id,),
                         daemon=True, name="ccx-codex-cap-interrupt").start()

    def _abandon_active_compact_locked(self, reason: str) -> bool:
        """Close a persisted compact that can no longer yield native proof.

        Caller holds ``self.lock``. See `_abandon_active_compact`.
        """
        return _abandon_active_compact(self.record, reason)

    def _state(self, state: str, *, error: str | None = None) -> None:
        with self.lock:
            lifecycle = self.record["lifecycle"]
            lifecycle["state"] = state
            lifecycle["since"] = now_ms()
            if error is not None:
                lifecycle["lastError"] = error
            self.last_activity = time.monotonic()
            self._save()

    def state(self) -> str:
        with self.lock:
            return self.record["lifecycle"]["state"]


    def _native_create_daemon_id(self) -> str:
        return str(getattr(self.daemon, "instance_id", "legacy-daemon"))






    def _resume_exact_thread(
        self, thread_id: str, params: dict[str, Any], *,
        archived: bool = False, timeout: float = 90,
        before_submit=None,
    ) -> dict[str, Any]:
        """One original native thread/resume, never a second thread/start.

        The pre-write release fence and native_threads registration also cover
        a lost reply: a concurrent kill must still unsubscribe this thread.
        """
        self.daemon._await_thread_release(thread_id)
        self.daemon.claim_thread(thread_id, self.short)
        self.native_threads.add(thread_id)
        if archived:
            self.client.request(
                "thread/unarchive", {"threadId": thread_id}, timeout=timeout,
            )
        resume_params = dict(params)
        resume_params["threadId"] = thread_id
        # Kill can win while this load waits for a prior unsubscribe. The
        # already-published killed marker is the existing no-late-load fence.
        self._abort_if_killed()
        if before_submit is not None:
            before_submit()  # persist the current native attempt only after release preflight
        try:
            return self.client.request(
                "thread/resume", resume_params, timeout=timeout,
            )
        except ProtocolError as error:
            if "no rollout found" in error.message:
                raise ProtocolError(
                    "original Codex thread has no persisted rollout; "
                    "refusing native thread/start replacement",
                    method="thread/resume",
                ) from error
            raise



    def _native_turn_start_marker(
        self,
        *,
        kind: str,
        client_message_id: str | None = None,
        delivery_id: str | None = None,
    ) -> dict[str, Any]:
        return {
            "schemaVersion": 1,
            "kind": kind,
            "state": "submitted",
            "daemonInstanceId": self._native_create_daemon_id(),
            "clientUserMessageId": client_message_id,
            "deliveryId": delivery_id,
            "submittedAtMs": now_ms(),
            "observedAtMs": None,
            "lastError": None,
        }

    def _mark_native_turn_start_unknown_locked(self, error: BaseException) -> None:
        marker = self.record.get("nativeTurnStart")
        if not isinstance(marker, dict):
            return
        marker["state"] = "unknown"
        marker["observedAtMs"] = now_ms()
        marker["lastError"] = str(error)
        self._save()

    def _forget_native_turn_start_locked(self) -> None:
        # Full UUID GenStore CAS does not erase persistent top-level fields;
        # clear the existing launch marker to null under the same CAS instead.
        if self.record.get("fingerprintVersion") == 2:
            if "nativeTurnStart" in self.record:
                self.record["nativeTurnStart"] = None
        else:
            self.record.pop("nativeTurnStart", None)

    def _clear_native_turn_start_locked(self) -> None:
        if self.record.get("nativeTurnStart") is not None:
            self._forget_native_turn_start_locked()
            self._save()

    @staticmethod
    def _native_mutation_error_is_ambiguous(error: ProtocolError) -> bool:
        if isinstance(error, ProtocolNotSent):
            return False
        return isinstance(error, ProtocolTransportError) or error.request_id is None

    def _journal(self, method: str, params: dict[str, Any]) -> bool:
        """Record one authoritative native event before it is reduced.

        Returns whether this notification is journaled at all, so the caller
        can persist the journal's health in the record write it is already
        doing. `EvidenceJournal.record` swallows its own failures on purpose:
        a handler that raised here would fail the shared app-server client
        (`AppServerClient._read_loop` treats a handler exception as fatal),
        killing every worker's live turn over a file that could not be
        appended to.
        """
        if method not in JOURNALED_METHODS:
            return False
        durable = None
        if method == "thread/tokenUsage/updated":
            usage_turn_id = params.get("turnId") or _turn_id(params)
            durable = usage_turn_id in self.completed_turn_ids
        self.evidence.record(
            method,
            params,
            thread_id=self.record.get("threadId"),
            durable=durable,
        )
        return True

    def _capture_compaction_context_locked(
        self, compact: dict[str, Any]
    ) -> None:
        """Persist one bounded compaction before/after telemetry enrichment."""
        context_after = self.record.get("context")
        earlier = compact.get("contextBefore")
        completed = compact.get("completedAtMs")
        valid = (
            isinstance(context_after, dict)
            and type(context_after.get("as_of_ms")) is int
            and type(completed) is int
            and context_after["as_of_ms"] > completed
            and (not isinstance(earlier, dict)
                 or type(earlier.get("as_of_ms")) is not int
                 or context_after["as_of_ms"] > earlier["as_of_ms"])
            and (self.record.get("tokenUsage") or {}).get("asOfTurnId") != compact.get("turnId")
        )
        compact["contextAfter"] = dict(context_after) if valid else None
        params = {
            "turnId": compact.get("turnId"),
            "item": {
                "id": compact.get("contextItemId"),
                "type": "contextCompaction",
            },
            "operationId": compact.get("operationId"),
            "compactCount": self.record.get("compactCount"),
            "contextBefore": compact.get("contextBefore"),
            "contextAfter": compact.get("contextAfter"),
            "completedAtMs": compact.get("completedAtMs"),
        }
        self.evidence.record(
            COMPACTION_CONTEXT_METHOD,
            params,
            thread_id=self.record.get("threadId"),
        )
        self.record["evidence"] = self.evidence.state()

    def _complete_active_compact_locked(
        self, *, turn_id: str | None = None, completed_at_ms: int | None = None
    ) -> bool:
        """Publish one successful requested compact. Caller holds ``self.lock``."""
        active = self.record.get("activeCompact")
        if (not isinstance(active, dict) or not isinstance(turn_id, str)
                or not turn_id or active.get("turnId") != turn_id
                or active.get("nativeTurnStarted") is not True
                or not isinstance(active.get("contextItemId"), str)
                or not active["contextItemId"]
                or active.get("nativeItemCompletedAtMs") is None
                or turn_id not in self.completed_turn_ids):
            return False
        compact_count = int(self.record.get("compactCount") or 0) + 1
        self.record["compactCount"] = compact_count
        self.record["lastCompact"] = {
            "operationId": active.get("operationId"),
            "baselineCompactCount": active.get("baselineCompactCount"),
            "targetCompactCount": active.get("targetCompactCount") or compact_count,
            "startedAtMs": active.get("startedAtMs"),
            "completedAtMs": completed_at_ms or now_ms(),
            "contextBefore": active.get("contextBefore"),
            "contextAfter": None,
            "turnId": turn_id or active.get("turnId"),
            "contextItemId": active.get("contextItemId"),
            "nativeTurnStarted": True,
            "nativeTurnSettled": True,
            "nativeTurnStatus": "completed",
            "status": "succeeded",
        }
        # Older carried tokens are invalid immediately after a native
        # compaction; retain no synthetic newer context in the public record.
        self.record["context"] = None
        self._capture_compaction_context_locked(self.record["lastCompact"])
        self.record["activeCompact"] = None
        return True

    def _observe_context_compaction_locked(
        self, *, turn_id: str | None, item_id: str | None, completed_at_ms: int | None
    ) -> None:
        """Bind modern ``contextCompaction`` item evidence to compact state."""
        active = self.record.get("activeCompact")
        if isinstance(active, dict):
            # Requested compact item is evidence only for the internal turn
            # positively pinned by turn/started, never an unrelated item.
            if (not isinstance(turn_id, str) or not turn_id
                    or active.get("turnId") != turn_id
                    or active.get("nativeTurnStarted") is not True
                    or not isinstance(item_id, str) or not item_id):
                return
            active["contextItemId"] = item_id
            active["nativeItemCompletedAtMs"] = completed_at_ms or now_ms()
            return
        # Automatic/native compaction outside an explicit CCX compact request is
        # still worth counting for context/history consumers. Do not double-count
        # the deprecated thread/compacted notification for the same turn.
        last = self.record.get("lastCompact")
        if isinstance(last, dict) and turn_id and last.get("turnId") == turn_id:
            return
        compact_count = int(self.record.get("compactCount") or 0) + 1
        self.record["compactCount"] = compact_count
        self.record["lastCompact"] = {
            "operationId": None,
            "baselineCompactCount": compact_count - 1,
            "targetCompactCount": compact_count,
            "startedAtMs": None,
            "completedAtMs": completed_at_ms or now_ms(),
            "contextBefore": None,
            "contextAfter": None,
            "turnId": turn_id,
            "contextItemId": item_id,
            "status": "succeeded",
        }

    def _emit_compact_feed_locked(
        self, method: str, turn_id: str, compact_id: str, *,
        outcome: str | None = None, operation_id: str | None = None,
        item_id: str | None = None, reason: str | None = None,
    ) -> None:
        if method == COMPACT_STARTED_METHOD:
            if compact_id in self._compact_started.get(turn_id, set()):
                return
        elif method == COMPACT_ENDED_METHOD and compact_id in self._compact_ended:
            return
        params = {
            "turnId": turn_id, "compactId": compact_id,
            "operationId": operation_id, "itemId": item_id,
        }
        if outcome is not None:
            params["outcome"] = outcome
        if reason is not None:
            params["reason"] = reason[:500]
        wrote = self.evidence.record(
            method, params, thread_id=self.record.get("threadId"),
        )
        if wrote is None:
            return
        if method == COMPACT_STARTED_METHOD:
            self._compact_started.setdefault(turn_id, set()).add(compact_id)
        else:
            self._compact_ended.add(compact_id)
        self.record["evidence"] = self.evidence.state()

    def _close_unfinished_compacts_locked(
        self, turn_id: str, *, outcome: str, operation_id: str | None,
        reason: str | None,
    ) -> None:
        starts = self._compact_started.get(turn_id, set())
        # A requested compact can have an admitted turn but no item started.
        # Emit its own sparse start before writing the terminal.
        if not starts and isinstance(operation_id, str) and operation_id:
            self._emit_compact_feed_locked(
                COMPACT_STARTED_METHOD, turn_id, f"operation:{operation_id}",
                operation_id=operation_id,
            )
            starts = self._compact_started.get(turn_id, set())
        for compact_id in sorted(starts):
            item_id = None
            prefix = f"{turn_id}:"
            if compact_id.startswith(prefix):
                item_id = compact_id[len(prefix):]
            self._emit_compact_feed_locked(
                COMPACT_ENDED_METHOD, turn_id, compact_id,
                outcome=outcome, operation_id=operation_id,
                item_id=item_id, reason=reason,
            )

    def _context_snapshot_locked(self) -> dict:
        from ccx.claude.context import current_context, public_context
        return public_context(
            current_context(self.record, engine="codex", live=self.record.get("context")),
            engine="codex", compacting=isinstance(self.record.get("activeCompact"), dict),
        )

    def _append_context_locked(self, value: dict, *, save: bool = True) -> None:
        turn_id = ((self.record.get("lifecycle") or {}).get("activeTurnId")
                   or (self.record.get("lastTurn") or {}).get("turnId"))
        self.evidence.record(
            CONTEXT_CHANGED_METHOD,
            {"context": value, "turnId": turn_id},
            thread_id=(self.record.get("native") or {}).get("threadId"),
        )
        self.record["evidence"] = self.evidence.state()
        if save:
            self._save()

    def _publish_context_locked(self, *, force: bool = False,
                                save: bool = True) -> None:
        view = self._context_snapshot_locked()
        previous = self._context_changes.last
        fenced = (view["reason"] in {"compacting", "just_compacted"}
                  and (previous or {}).get("reason") != view["reason"])
        active = (self.record.get("lifecycle") or {}).get("activeTurnId")
        result = self._context_changes.observe(
            view, force=bool(force or fenced or not active),
        )
        if result is not None:
            self._append_context_locked(result, save=save)

    def flush_context(self) -> None:
        """Publish the last native usage sample on the existing janitor tick."""
        with self.lock:
            if self.teardown_claimed or self._externally_terminalized:
                return
            # The terminal notification itself flushes context synchronously.
            # Never let the janitor publish an old queued mid-turn sample after
            # turn/compact settlement; a read-only compact-status must stay pure.
            if (not (self.record.get("lifecycle") or {}).get("activeTurnId")
                    or isinstance(self.record.get("activeCompact"), dict)):
                self._context_changes.pending = None
                return
            value = self._context_changes.flush()
            if value is not None:
                self._append_context_locked(value)

    def _publish_background_locked(self, transition: str, item: dict | None) -> None:
        """Journal one actual native change; the caller holds worker.lock."""
        snapshot = background_summary([
            *self._bg_open.values(), *self._bg_notifications.values(),
        ])
        if item is None and not materially_changed(
            self.record.get("backgroundTaskSummary"), snapshot
        ):
            return
        self.record["backgroundTaskSummary"] = snapshot
        self.evidence.record(
            BACKGROUND_CHANGED_METHOD,
            {"transition": transition, "item": item, "summary": snapshot},
            thread_id=(self.record.get("native") or {}).get("threadId"),
        )

    def _lose_native_background_locked(self) -> bool:
        """Drop old-runtime command guards; never assert a native exit.

        Called under worker.lock when this app-server goes away or a new
        owner adopts a recorded worker. A lost command is no longer tracked
        (its outcome is unknown, so no ``finished`` item is published); the
        correction keeps the registry-backed pending notifications.
        """
        previous = self.record.get("backgroundTaskSummary")
        had_native = bool(self._bg_open or self._bg_candidates)
        self._bg_open.clear()
        self._bg_candidates.clear()
        stale_running = (isinstance(previous, dict)
                         and type(previous.get("running_count")) is int
                         and previous["running_count"] > 0)
        if not (had_native or stale_running):
            return False
        # Only native command tracking is lost. Pending parent notifications
        # come from the durable registry, so they stay in the summary.
        corrected = background_summary(list(self._bg_notifications.values()))
        if not materially_changed(previous, corrected):
            return False
        self.record["backgroundTaskSummary"] = corrected
        self.evidence.record(
            BACKGROUND_CHANGED_METHOD,
            {"transition": "event", "item": None, "summary": corrected},
            thread_id=(self.record.get("native") or {}).get("threadId"),
        )
        self.record["evidence"] = self.evidence.state()
        return True

    def refresh_parent_notifications(self) -> bool:
        """Read the current registry once; publish only a changed snapshot.

        Only codexd owns this worker's evidence journal. Best-effort IPC hints
        do not contain facts, and an absent owner never starts a daemon.
        """
        from ccx.notify.registry import iter_registrations, _delivery_id
        rows = iter_registrations(include_terminal=True)
        with self.lock:
            if self.teardown_claimed or self._externally_terminalized:
                return False
            updated = {}
            outcomes = {}
            for reg in rows:
                parent = reg.get("parent") or {}
                if (parent.get("kind") != "ccx-codex"
                        or parent.get("workerId") != self.worker_id
                        or parent.get("generation") != self.daemon._uuid_store.generation):
                    continue
                event_id = reg.get("eventId")
                if not isinstance(event_id, str):
                    continue
                delivery_id = _delivery_id(event_id)
                state = reg.get("state")
                if state in {"armed", "ready"}:
                    updated[delivery_id] = {
                        "id": delivery_id, "kind": "notification",
                        "status": "pending", "label": "child result pending",
                        "notification_id": event_id, "delivery_id": delivery_id,
                        "started_at_ms": reg.get("createdAtMs"),
                    }
                else:
                    outcomes[delivery_id] = state
            previous = self._bg_notifications
            if previous == updated:
                return False
            self._bg_notifications = updated
            additions = [id for id in updated if id not in previous]
            removals = [id for id in previous if id not in updated]
            if len(additions) == 1 and not removals:
                transition, item = "started", updated[additions[0]]
            elif len(removals) == 1 and not additions:
                item = dict(previous[removals[0]])
                result = outcomes.get(removals[0])
                outcome = {
                    "delivered": ("finished", "completed"),
                    "cancelled": ("stopped", "stopped"),
                    "undeliverable": ("failed", "failed"),
                }.get(result)
                if outcome is None:
                    # Missing, unreadable or unknown terminal registry row
                    # proves only a snapshot correction, not delivery.
                    transition, item = "event", None
                else:
                    transition, item["status"] = outcome
            else:
                transition, item = "event", None
            snapshot = background_summary([
                *self._bg_open.values(), *updated.values(),
            ])
            if not materially_changed(self.record.get("backgroundTaskSummary"), snapshot):
                return False
            self.record["backgroundTaskSummary"] = snapshot
            self.evidence.record(
                BACKGROUND_CHANGED_METHOD,
                {"transition": transition, "item": item, "summary": snapshot},
                thread_id=(self.record.get("native") or {}).get("threadId"),
            )
            self.record["evidence"] = self.evidence.state()
            self._save()
            return True

    def _background_native_locked(
        self, method: str, params: dict, completed_turn: bool,
    ) -> None:
        native = self.record.get("native") or {}
        if params.get("threadId") != native.get("threadId"):
            return
        if method == "item/started":
            item = params.get("item") or {}
            tid = params.get("turnId")
            item_id = item.get("id") if isinstance(item, dict) else None
            if (item.get("type") == "commandExecution"
                    and isinstance(item_id, str) and item_id
                    and isinstance(tid, str) and tid
                    and tid == self.record["lifecycle"].get("activeTurnId")):
                self._bg_candidates[item_id] = {
                    "turn": tid, "id": "command:" + item_id,
                    "kind": "command", "status": "running",
                    # Deliberately no raw native command, paths or output.
                    "label": "background shell", "tool_use_id": item_id,
                    "started_at_ms": params.get("startedAtMs"),
                }
        elif method == "turn/completed" and completed_turn:
            turn_id = _turn_id(params)
            for item_id, item in list(self._bg_candidates.items()):
                if item["turn"] != turn_id:
                    continue
                del self._bg_candidates[item_id]
                if item_id not in self._bg_open:
                    self._bg_open[item_id] = item
                    self._publish_background_locked("started", item)
        elif method == "item/completed":
            item = params.get("item") or {}
            item_id = item.get("id") if isinstance(item, dict) else None
            if not isinstance(item_id, str):
                return
            candidate = self._bg_candidates.get(item_id)
            if candidate is not None and candidate["turn"] == params.get("turnId"):
                del self._bg_candidates[item_id]  # normal foreground command
            active = self._bg_open.get(item_id)
            if active is None or active["turn"] != params.get("turnId"):
                return
            exit_code = item.get("exitCode")
            if type(exit_code) is int:
                status = "completed" if exit_code == 0 else "failed"
            elif item.get("status") == "failed":
                status = "failed"
            else:
                status = "unknown"
            terminal = {key: value for key, value in active.items() if key != "turn"}
            terminal["status"] = status
            if type(exit_code) is int:
                terminal["exit_code"] = exit_code
            del self._bg_open[item_id]
            self._publish_background_locked(
                {"completed": "finished", "failed": "failed"}.get(status, "event"),
                terminal,
            )

    def on_notification(self, method: str, params: dict[str, Any]) -> None:
        # Journal the authoritative native event first, while it is still the
        # thing the app-server sent, rather than after it has been folded into
        # lifecycle state, `lastTurn`, and `lastReport`. Nothing below reads
        # the journal, so a journal fault cannot change worker health or turn
        # outcome — it can only make the evidence explicitly degraded.
        if self.teardown_claimed:
            # Teardown owns/closed this worker's evidence journal.
            return
        # A separate terminal writer only revoked native *mutations*. Its
        # still-open journal can record late provider facts without resurrecting
        # this worker. This is forensic evidence, not outcome authorization.
        # A valid item/completed can arrive without item/started. Admit only
        # the live exact turn, then publish its missing sparse start before
        # journaling the native item. This is source ownership, not a reader
        # inference or a restatement of native item lines.
        if method == "item/completed":
            native_item = params.get("item")
            turn_id = params.get("turnId")
            item_id = native_item.get("id") if isinstance(native_item, dict) else None
            if (isinstance(native_item, dict)
                    and native_item.get("type") == "contextCompaction"
                    and isinstance(turn_id, str) and turn_id
                    and isinstance(item_id, str) and item_id):
                with self.lock:
                    if self.record["lifecycle"].get("activeTurnId") == turn_id:
                        active = self.record.get("activeCompact")
                        operation_id = (
                            active.get("operationId")
                            if isinstance(active, dict)
                            and active.get("turnId") == turn_id else None
                        )
                        self._emit_compact_feed_locked(
                            COMPACT_STARTED_METHOD, turn_id,
                            f"{turn_id}:{item_id}", item_id=item_id,
                            operation_id=operation_id,
                        )
        journaled = self._journal(method, params)
        if method == "item/completed":
            # The completed user-message item is native's final statement of
            # that input; by then every copy of it has been journaled.
            from ccx.codex.delivery_recovery import ccx_client_id
            client_id = ccx_client_id(params.get("item"), self.worker_id)
            if client_id is not None:
                # Journaled above; resolved off this shared reader thread.
                self.daemon.queue_delivery_recovery(self, client_id)
        if self._externally_terminalized:
            return
        changed = False
        completed_turn = False
        with self.lock:
            if self.teardown_claimed or self._externally_terminalized:
                return
            started_id = _turn_id(params) if method == "turn/started" else None
            if started_id and (
                    started_id in self.completed_turn_ids
                    or started_id == (self.record.get("lastTurn") or {}).get("turnId")):
                # A re-delivered start of a turn already known completed (also
                # after a restart, from the durable evidence index) must not
                # reopen it: its duplicate completion is ignored, so the
                # worker would stay busy forever. Journaled above only.
                pass
            elif method == "turn/started":
                changed = True
                self._resumed_system_error_pending = False
                self.record["historyKnownEmpty"] = False
                active = self.record.get("activeCompact")
                if (isinstance(active, dict) and active.get("awaitingTurn")
                        and not active.get("turnId") and started_id
                        and started_id not in self.completed_turn_ids
                        and not self.record.get("nativeTurnStart")
                        and self.launch is None
                        and not self.record["lifecycle"].get("activeTurnId")):
                    active["turnId"] = started_id
                    active["awaitingTurn"] = False
                    active["nativeTurnStarted"] = True
                self._forget_native_turn_start_locked()
                self.record["lifecycle"]["state"] = "busy"
                self.record["lifecycle"]["activeTurnId"] = _turn_id(params)
                self.record["lifecycle"]["since"] = now_ms()
            elif method == "item/started":
                item = params.get("item")
                turn_id = params.get("turnId")
                item_id = item.get("id") if isinstance(item, dict) else None
                if (isinstance(item, dict)
                        and item.get("type") == "contextCompaction"
                        and isinstance(turn_id, str) and turn_id
                        and isinstance(item_id, str) and item_id
                        and self.record["lifecycle"].get("activeTurnId") == turn_id):
                    active = self.record.get("activeCompact")
                    operation_id = (
                        active.get("operationId")
                        if isinstance(active, dict)
                        and active.get("turnId") == turn_id else None
                    )
                    # Only this producer writes neutral compaction starts.
                    # Native item/started remains raw forensic evidence.
                    self._emit_compact_feed_locked(
                        COMPACT_STARTED_METHOD, turn_id,
                        f"{turn_id}:{item_id}", operation_id=operation_id,
                        item_id=item_id,
                    )
            elif method == "item/completed":
                changed = True
                item = params.get("item") or {}
                if item.get("type") == "contextCompaction":
                    item_turn, item_id = params.get("turnId"), item.get("id")
                    if (isinstance(item_turn, str) and item_turn
                            and isinstance(item_id, str) and item_id
                            and self.record["lifecycle"].get("activeTurnId") == item_turn):
                        self._compact_finished.setdefault(item_turn, set()).add(item_id)
                        active = self.record.get("activeCompact")
                        operation_id = (
                            active.get("operationId")
                            if isinstance(active, dict)
                            and active.get("turnId") == item_turn else None
                        )
                        # Journaled after the original native item: exactly
                        # one admitted sparse terminal for this compactId.
                        self._emit_compact_feed_locked(
                            COMPACT_ENDED_METHOD, item_turn,
                            f"{item_turn}:{item_id}", outcome="completed",
                            operation_id=operation_id, item_id=item_id,
                        )
                        self._observe_context_compaction_locked(
                            turn_id=item_turn,
                            item_id=item_id,
                            completed_at_ms=params.get("completedAtMs"),
                        )
                report = report_from_item(
                    item,
                    turn_id=params.get("turnId"),
                    completed_at=params.get("completedAtMs"),
                )
                if report and should_replace_live_report(
                    self.record.get("lastReport"), report
                ):
                    self.record["lastReport"] = report
            elif method == "thread/tokenUsage/updated":
                changed = True
                usage = params.get("tokenUsage") or {}
                last = usage.get("last") or {}
                total = usage.get("total") or {}
                previous_usage = (
                    self.record.get("tokenUsage")
                    if isinstance(self.record.get("tokenUsage"), dict)
                    else {}
                )
                previous_total = (
                    previous_usage.get("total")
                    if isinstance(previous_usage.get("total"), dict)
                    else None
                )
                reset = (
                    all(
                        int(last.get(key) or 0) == 0
                        for key in (
                            "inputTokens",
                            "cachedInputTokens",
                            "outputTokens",
                            "reasoningOutputTokens",
                        )
                    )
                    and isinstance(previous_total, dict)
                    and dict(total) == dict(previous_total)
                )
                tokens = last.get("inputTokens")
                window = usage.get("modelContextWindow")
                percent = (
                    tokens / window * 100
                    if (
                        not reset
                        and isinstance(tokens, int)
                        and isinstance(window, int)
                        and window > 0
                    )
                    else None
                )
                if reset:
                    self.record["context"] = None
                else:
                    self.record["context"] = {
                        "state": self.record["lifecycle"]["state"],
                        "tokens_used": tokens,
                        "window_size": window,
                        "percent_used": percent,
                        "as_of_turn_ref": None,
                        "as_of_ms": now_ms(),
                    }
                self.record["tokenUsage"] = {
                    "last": dict(last),
                    "total": dict(total),
                    "modelContextWindow": window,
                    "asOfTurnId": params.get("turnId"),
                    "asOfMs": now_ms(),
                }

                last_compact = self.record.get("lastCompact")
                if (
                    not reset
                    and isinstance(last_compact, dict)
                    and last_compact.get("status") == "succeeded"
                    and last_compact.get("contextAfter") is None
                    and last_compact.get("turnId") != params.get("turnId")
                    and isinstance(self.record.get("context"), dict)
                ):
                    self._capture_compaction_context_locked(last_compact)
            elif method == "thread/status/changed":
                changed = True
                status = params.get("status") or {}
                kind = status.get("type")
                flags = status.get("activeFlags") or []
                if kind in {"active", "idle"}:
                    self._resumed_system_error_pending = False
                # The status flag can precede the approval request itself;
                # the request handler sets waiting once there is a gate.
                if kind == "active" and self.record.get("pendingApprovals") and any(
                    flag in {"waitingOnApproval", "waitingOnUserInput"}
                    for flag in flags
                ):
                    self.record["lifecycle"]["state"] = "waiting"
                elif kind == "active":
                    self.record["lifecycle"]["state"] = "busy"
                elif kind == "idle" and not self.record.get("pendingApprovals"):
                    lifecycle = self.record["lifecycle"]
                    if (
                        lifecycle.get("activeTurnId")
                        or self.launch is not None
                        or isinstance(self.record.get("nativeTurnStart"), dict)
                        or isinstance(self.record.get("activeCompact"), dict)
                    ):
                        # Codex publishes idle before the authoritative terminal
                        # notification for both model turns and native compact.
                        # Keep the operation unsettled so waiters cannot mistake
                        # this status edge for turn/completion evidence.
                        if lifecycle.get("state") != "crashed":
                            lifecycle["state"] = "busy"
                    else:
                        lifecycle["state"] = "idle"
                        lifecycle["activeTurnId"] = None
                elif kind == "systemError":
                    if self._resumed_system_error_pending:
                        # The resumed thread still carries the prior failed
                        # turn's systemError. A replayed edge before any new
                        # turn/started or active status cannot fail that turn.
                        changed = False
                    else:
                        active_compact = self.record.get("activeCompact")
                        compact_turn = (
                            active_compact.get("turnId")
                            if isinstance(active_compact, dict) else None
                        )
                        compact_operation = (
                            active_compact.get("operationId")
                            if isinstance(active_compact, dict) else None
                        )
                        compact_turn = compact_turn or self.record["lifecycle"].get("activeTurnId")
                        if isinstance(compact_turn, str) and compact_turn:
                            if compact_operation or self._compact_started.get(compact_turn):
                                self._close_unfinished_compacts_locked(
                                    compact_turn, outcome="unknown",
                                    operation_id=compact_operation,
                                    reason="Codex thread system error",
                                )
                        self._abandon_active_compact_locked("Codex thread system error")
                        self.record["lifecycle"]["state"] = "crashed"
                        self.record["lifecycle"]["lastError"] = "Codex thread system error"
            elif method == "turn/completed":
                changed = True
                turn = params.get("turn") or {}
                if not isinstance(turn, dict):
                    turn = {}
                completed_turn_id = turn.get("id")
                active_turn_id = self.record["lifecycle"].get("activeTurnId")
                last_turn_id = (self.record.get("lastTurn") or {}).get("turnId")
                if (not isinstance(completed_turn_id, str) or not completed_turn_id
                        or completed_turn_id == last_turn_id
                        or completed_turn_id in self.completed_turn_ids
                        or active_turn_id != completed_turn_id):
                    # A completion is a new outcome only for the exact native
                    # turn already admitted as active by turn/started or its
                    # original launch fence. In particular, idle does not prove
                    # an unseen turn: a delayed A after B would roll lastTurn
                    # backwards and count A again. The observed notification
                    # is still journaled, including after a daemon restart.
                    # A duplicate of the current pending save can retry its
                    # original CAS without incrementing again.
                    if journaled:
                        self.record["evidence"] = self.evidence.state()
                    self._save()
                    return
                self.completed_turn_ids.add(completed_turn_id)
                if self.cap_turn_id == completed_turn_id:
                    self.cap_turn_id = None
                    self.active_caps = None
                completed_turn = True
                previous_state = self.record["lifecycle"].get("state")
                active_compact = self.record.get("activeCompact")
                requested_op = (
                    active_compact.get("operationId")
                    if isinstance(active_compact, dict)
                    and active_compact.get("turnId") == completed_turn_id
                    else None
                )
                native_compact_ended = self._compact_finished.get(completed_turn_id, set())
                if turn.get("status") in ("failed", "interrupted", "cancelled"):
                    self._close_unfinished_compacts_locked(
                        completed_turn_id, outcome="failed",
                        operation_id=requested_op,
                        reason=str(turn.get("error") or turn.get("status")),
                    )
                elif (not native_compact_ended and
                      (requested_op or self._compact_started.get(completed_turn_id))):
                    # Completion of the native turn without the native
                    # compaction item does not prove the compact itself.
                    # Cover CCX-requested and automatic/native compactions.
                    self._close_unfinished_compacts_locked(
                        completed_turn_id, outcome="unknown",
                        operation_id=requested_op,
                        reason="native compact item completion not observed",
                    )
                elif (completed_turn_id in self._compact_legacy
                      and not self._compact_started.get(completed_turn_id)
                      and not native_compact_ended
                      and requested_op is None):
                    # Legacy-only automatic compaction: defer until the turn's
                    # accepted completion so modern item evidence wins.
                    legacy_id = f"legacy:{completed_turn_id}"
                    self._emit_compact_feed_locked(
                        COMPACT_STARTED_METHOD, completed_turn_id, legacy_id,
                    )
                    self._emit_compact_feed_locked(
                        COMPACT_ENDED_METHOD, completed_turn_id, legacy_id,
                        outcome="completed",
                    )
                if (
                    isinstance(active_compact, dict)
                    and active_compact.get("turnId") == completed_turn_id
                    and active_compact.get("nativeTurnStarted") is True
                    and isinstance(active_compact.get("contextItemId"), str)
                    and bool(active_compact["contextItemId"])
                    and active_compact.get("nativeItemCompletedAtMs") is not None
                    and turn.get("status") == "completed"
                ):
                    self._complete_active_compact_locked(
                        turn_id=completed_turn_id,
                        completed_at_ms=(
                            int(turn.get("completedAt") * 1000)
                            if isinstance(turn.get("completedAt"), (int, float))
                            else None
                        ),
                    )
                elif (isinstance(active_compact, dict)
                      and active_compact.get("turnId") == completed_turn_id):
                    self._abandon_active_compact_locked(
                        f"compact turn settled without success evidence ({turn.get('status')})"
                    )
                    self.record["lastCompact"].update(
                        turnId=completed_turn_id, nativeTurnSettled=True,
                        nativeTurnStatus=turn.get("status"), nativeTurnError=turn.get("error"),
                    )
                last_compact = self.record.get("lastCompact")
                if (
                    isinstance(last_compact, dict)
                    and last_compact.get("turnId") == completed_turn_id
                    and "contextAfter" in last_compact
                    and last_compact.get("contextAfter") is None
                ):
                    self._capture_compaction_context_locked(last_compact)
                current_turn_id = self.record["lifecycle"].get("activeTurnId")
                if current_turn_id and current_turn_id != completed_turn_id:
                    # An old completion cannot clear or settle a newer turn.
                    if changed:
                        self._save()
                    return
                self._forget_native_turn_start_locked()
                self.record["lifecycle"]["activeTurnId"] = None
                stats = self.record.setdefault("stats", {})
                stats["turns"] = stats.get("turns", 0) + 1
                # Native total is cumulative for the thread. Apply at most
                # its positive advance (not per-turn last), so the existing
                # stats counter-CAS path remains safe on a replay/rebase.
                usage = self.record.get("tokenUsage")
                total = usage.get("total") if isinstance(usage, dict) else None
                if isinstance(total, dict):
                    for native_key, field in (("inputTokens", "inputTokens"),
                                              ("outputTokens", "outputTokens")):
                        value = total.get(native_key)
                        previous = stats.get(field)
                        if (type(value) is int and value >= 0 and
                                (previous is None or value >= previous)):
                            stats[field] = value
                self.record["lastTurnAt"] = now_ms()
                self.record["lastTurn"] = _last_turn_from_native(turn)
                usage = self.record.get("tokenUsage")
                observed = self.record.get("context")
                if (isinstance(usage, dict)
                        and usage.get("asOfTurnId") == completed_turn_id
                        and isinstance(observed, dict)):
                    self.record["lastTurn"]["context"] = {
                        **observed,
                        "as_of_turn_ref": "ccx-turn:codex:v1:" + completed_turn_id,
                    }
                else:
                    self.record["lastTurn"]["context"] = None
                active_error = self.record.get("activeTurnError")
                if (
                    isinstance(active_error, dict)
                    and active_error.get("turnId") == completed_turn_id
                ):
                    self.record["activeTurnError"] = None
                if self.record.get("pendingApprovals"):
                    self.record["lifecycle"]["state"] = "waiting"
                elif previous_state == "crashed":
                    # A completed turn can arrive after the independent
                    # thread/status systemError. Do not let its settlement
                    # demote a worker whose runtime health is already known to
                    # be bad.
                    self.record["lifecycle"]["state"] = "crashed"
                elif isinstance(self.record.get("activeCompact"), dict):
                    # A foreign/native turn can settle while compaction still
                    # owns its own operation fence. Do not publish idle until
                    # thread/compacted (or an explicit compact failure) closes it.
                    self.record["lifecycle"]["state"] = "busy"
                else:
                    self.record["lifecycle"]["state"] = "idle"
                self.record["lifecycle"]["since"] = now_ms()
            elif method == "serverRequest/resolved":
                changed = True
                request_id = params.get("requestId")
                self.record["pendingApprovals"] = [
                    approval
                    for approval in self.record.get("pendingApprovals") or []
                    if str(approval.get("requestId")) != str(request_id)
                ]
                if self.record["lifecycle"]["state"] == "waiting":
                    self.record["lifecycle"]["state"] = (
                        "waiting"
                        if self.record["pendingApprovals"]
                        else (
                            "busy"
                            if (
                                self.record["lifecycle"].get("activeTurnId")
                                or isinstance(self.record.get("nativeTurnStart"), dict)
                                or isinstance(self.record.get("activeCompact"), dict)
                            )
                            else "idle"
                        )
                    )
                    self.record["lifecycle"]["since"] = now_ms()
            elif method == "thread/compacted":
                changed = True
                # Legacy Codex signal. Modern releases expose compaction as a
                # contextCompaction item inside an internal turn. If both arrive,
                # the turn id dedupes the deprecated notification.
                legacy_turn_id = params.get("turnId")
                if isinstance(legacy_turn_id, str) and legacy_turn_id:
                    self._compact_legacy.add(legacy_turn_id)
                active_compact = self.record.get("activeCompact")
                last = self.record.get("lastCompact") or {}
                if legacy_turn_id and last.get("turnId") == legacy_turn_id:
                    return
                if isinstance(active_compact, dict):
                    # Deprecated thread/compacted is not a modern item and
                    # never settles the requested operation on its own.
                    pass
                else:
                    last = self.record.get("lastCompact")
                    already_seen = (
                        isinstance(last, dict)
                        and legacy_turn_id
                        and last.get("turnId") == legacy_turn_id
                    )
                    if not already_seen:
                        compact_count = int(self.record.get("compactCount") or 0) + 1
                        self.record["compactCount"] = compact_count
                        self.record["lastCompact"] = {
                            "operationId": None,
                            "baselineCompactCount": compact_count - 1,
                            "targetCompactCount": compact_count,
                            "startedAtMs": None,
                            "completedAtMs": now_ms(),
                            "contextBefore": None,
                            "contextAfter": None,
                            "turnId": legacy_turn_id,
                            "contextItemId": None,
                            "status": "succeeded",
                        }
                        self._capture_compaction_context_locked(
                            self.record["lastCompact"]
                        )
                # Legacy item evidence is not settlement or an input lease.
            elif method == "error":
                changed = True
                # ``error`` is a turn-scoped diagnostic. It is not evidence of
                # worker/runtime failure: Codex may recover and retry the same
                # turn, and the authoritative settled error arrives in
                # turn/completed.turn.error. Keep the native structured value
                # intact, but only for the currently active turn.
                turn_id = params.get("turnId")
                active_turn_id = self.record["lifecycle"].get("activeTurnId")
                if (
                    isinstance(turn_id, str)
                    and turn_id
                    and turn_id == active_turn_id
                ):
                    self.record["activeTurnError"] = {
                        "turnId": turn_id,
                        "error": params.get("error"),
                        "willRetry": bool(params.get("willRetry", False)),
                        "observedAtMs": now_ms(),
                    }
            if method in {"item/started", "item/completed", "turn/completed"}:
                self._background_native_locked(method, params, completed_turn)
            if not journaled and not changed:
                return
            if journaled:
                self.record["evidence"] = self.evidence.state()
            self.last_activity = time.monotonic()
            # Journal the context view before the same record save that
            # publishes a new native terminal/compact state. A later ordinary
            # read-only compact-status call must not race a second record write.
            self._publish_context_locked(force=completed_turn, save=False)
            self._save()

        if completed_turn:
            # The native notification was admitted as this worker's exact
            # active turn, and its record has been persisted. It is already
            # the original completion evidence: do not hydrate thread/read.
            # A turn that finished before the caller persisted acceptance is
            # caught by the receipt writer's journal check or reconciliation.
            if turn.get("status") == "completed":
                from ccx.codex.completion import settle_completed
                try:
                    settle_completed(
                        self.daemon._uuid_store, f"codex:{self.worker_id}",
                        thread_id=(self.record.get("native") or {}).get("threadId"),
                        turn=turn,
                        live_report=self.record.get("lastReport"),
                    )
                except Exception as error:
                    # A receipt-side fault cannot take down the shared native
                    # notification reader or change the completed turn. The
                    # original journal and recovery path remain available.
                    print("ccx codexd: original completion settlement pending: "
                          + type(error).__name__, file=sys.stderr, flush=True)
            from ccx.notify.daemon import revive_notify_daemon_after_turn
            revive_notify_daemon_after_turn("codex", self.worker_id)

    def on_server_request(
        self, request_id: Any, method: str, params: dict[str, Any]
    ) -> None:
        kind = APPROVAL_METHODS.get(method)
        if not kind or (
            method == "item/fileChange/requestApproval"
            and params.get("grantRoot")
        ):
            self.client.reject_request(
                request_id,
                f"CCX does not broker app-server request {method}",
            )
            return
        preview = {
            "requestId": request_id,
            # App-server request ids restart with the app-server, so a stale
            # id could name a newer request; this one never repeats.
            "gateId": uuid.uuid4().hex,
            # Native request IDs are connection-scoped. An abruptly killed
            # daemon can leave old approvals on disk; they cannot be replayed.
            "runtimeId": self.daemon.instance_id,
            "kind": kind,
            "turnId": params.get("turnId"),
            "itemId": params.get("itemId"),
            "command": _bounded(params.get("command")),
            "cwd": _bounded(params.get("cwd"), 500),
            "reason": _bounded(params.get("reason")),
            "createdAt": now_ms(),
        }
        with self.lock:
            torn_down = (self.teardown_claimed or self._externally_terminalized
                         or self.record["lifecycle"].get("state") in _TERMINAL_STATES)
            if not torn_down:
                self.record["pendingApprovals"].append(preview)
                self.record["lifecycle"]["state"] = "waiting"
                self.record["lifecycle"]["since"] = now_ms()
                self._save()
        if torn_down:
            self.client.reject_request(
                request_id, f"CCX worker {self.short} is being unloaded"
            )

    def _select_approval(self, gate_id: str) -> dict[str, Any] | None:
        """Call under self.lock; only this runtime's exact CCX gateId may act."""
        if not isinstance(gate_id, str) or not gate_id or len(gate_id) > 128:
            return None
        for approval in self.record.get("pendingApprovals") or []:
            if (approval.get("gateId") == gate_id
                    and approval.get("runtimeId") == self.daemon.instance_id):
                return approval
        return None

    def kill(self, observe_seconds: float | None = None) -> dict[str, Any]:  # noqa: D401
        """Terminate this worker and unload it from the shared daemon.

        Codex has no per-worker process to signal: every loaded worker is a
        thread multiplexed on the one shared ``codex app-server`` child, and the
        daemon PID and app-server PID stamped into each record are device-wide,
        not worker-owned. Killing a worker therefore means unloading exactly this
        worker — never signalling either shared process, and never disturbing a
        sibling.

        The active turn is cancelled best-effort first; a refusal or a transport
        failure there does not stop the unload, because the unload is what the
        caller asked for. The native ``threadId`` is preserved and the thread is
        deliberately not archived, so a later ``send``/``resume`` reloads the
        same conversation immediately.
        """
        # one critical section decides the race. `self.lock` totally orders this
        # against the loader's two commit points (the native turn launch
        # publication and `_commit_state`), so exactly one of us wins:
        #   * we go first  -> the loader sees `killed` and does nothing at all;
        #   * loader first -> we see its `launch` here and own the cancellation.
        # It deliberately does not take `load_lock`: waiting there would stall
        # kill behind a 90s `thread/resume`.
        with self.lock:
            launch = self.launch
            unresolved_start = self.record.get("nativeTurnStart")
            if launch is None and isinstance(unresolved_start, dict):
                # The local JSON-RPC request has already settled without exact
                # native evidence, so the in-memory launch fence can no longer
                # hand us a turn id. The native request may still land later;
                # unloading now would falsely confirm a kill.
                self.kill_intent.clear()
                return {
                    "ok": False,
                    "pending": True,
                    "error": (
                        f"worker {self.short} has an indeterminate native "
                        "turn/start; kill was NOT completed and is not confirmed "
                        "— retry after turn evidence arrives or the Codex "
                        "app-server is replaced"
                    ),
                    "state": self.record["lifecycle"]["state"],
                }
            self.kill_intent.set()
            self.killed.set()
            state = self.record["lifecycle"]["state"]
            turn_id = self.record["lifecycle"].get("activeTurnId")
            thread_id = (self.record.get("native") or {}).get("threadId") or self.record.get("threadId")
            active_compact = self.record.get("activeCompact")
            compact_turn = (
                active_compact.get("turnId")
                if isinstance(active_compact, dict) else None
            )
            compact_operation = (
                active_compact.get("operationId")
                if isinstance(active_compact, dict) else None
            )
            compact_turn = compact_turn or turn_id
            if isinstance(compact_turn, str) and compact_turn:
                if compact_operation or self._compact_started.get(compact_turn):
                    self._close_unfinished_compacts_locked(
                        compact_turn, outcome="unknown",
                        operation_id=compact_operation,
                        reason="worker killed during compaction",
                    )
            self._abandon_active_compact_locked("worker killed during compaction")

        interrupted = False
        launch_cancelled = None
        if launch is not None:
            # A `turn/start` was already irrevocably in flight when we won. It
            # is not cancellable until it returns a turn id, so wait for that —
            # bounded by the caller's own remaining budget, capped.
            window = (_LAUNCH_SETTLE_CAP_S if observe_seconds is None
                      else max(0.0, min(float(observe_seconds),
                                        _LAUNCH_SETTLE_CAP_S)))
            if not launch.done.wait(window):
                # unresolved. The turn may be accepted the moment after we
                # answer, so reporting a confirmed kill here would be exactly
                # the lie this operation must not tell. Stay loaded and
                # nonterminal so the caller's reconciliation sees a target that
                # is still pending; the `killed` marker guarantees the loader
                # cancels as soon as the turn id becomes knowable and then
                # terminalizes and unloads. A later kill is idempotent.
                return {
                    "ok": False,
                    "pending": True,
                    "error": (f"worker {self.short} has a turn launch still in "
                              f"flight after {window:g}s; it will be cancelled "
                              f"and unloaded when the request returns — retry "
                              f"kill to confirm"),
                    "state": self.state(),
                }
            if not isinstance(launch.turn_id, str) or not launch.turn_id:
                # A native start returned without an exact turn ID. The
                # original mutation may still land; no unload or success.
                return {"ok": False, "pending": True,
                        "error": "original native turn/start is unresolved; kill not confirmed",
                        "state": self.state()}
            launch_cancelled = self._interrupt_turn(launch.turn_id)
            interrupted = launch_cancelled
        elif state in {"busy", "waiting"} and turn_id and thread_id:
            interrupted = self._interrupt_turn(turn_id)
        self._state("dead")
        # Clears activeTurnId/pendingApprovals, removes this worker alone from
        # `workers` and `by_thread`, and releases its native thread without
        # waiting for Codex to answer.
        self.daemon._unload(self, terminal_state="dead")
        return {
            "ok": True,
            "killed": True,
            "unloaded": True,
            "interrupted": interrupted,
            # Only present when this kill raced a turn launch, so a caller can
            # tell "nothing was running" from "we cancelled what was starting".
            "launchCancelled": launch_cancelled,
            "state": "dead",
            "threadId": thread_id,
        }

    def _original_notification_thread(self) -> str:
        """The one globally claimed parent thread for both read and wake."""
        native = self.record.get("native") or {}
        thread_id = native.get("threadId")
        if (not isinstance(thread_id, str) or not thread_id
                or native.get("threadSource") != self.daemon._uuid_store.codex_source(
                    f"codex:{self.worker_id}"
                )):
            raise RuntimeError("original parent Codex native thread/source mismatch")
        self.daemon._uuid_store.authorize_native_action(
            f"codex:{self.worker_id}", "send",
        )
        return thread_id

    def _parent_notification_match(
        self, delivery_id: str, *, legacy_event_id: str | None = None,
        name: str = "worker_notification",
    ) -> dict[str, Any] | None:
        with self.lock:
            fences = self.record.get("parentNotifyFences")
            fence = fences.get(delivery_id) if isinstance(fences, dict) else None
        return notification_delivery_match(
            self.client.request,
            self._original_notification_thread(),
            delivery_id,
            legacy_event_id=legacy_event_id,
            stop_at_turn_id=fence if isinstance(fence, str) else None,
            name=name,
        )

    def find_parent_notification(
        self, delivery_id: str, payload: dict[str, Any], *,
        name: str = "worker_notification",
    ) -> dict[str, Any] | None:
        """Read-only: has this notification already landed in the thread?

        Runs without `operation_lock` (a long parent history must not stall
        the parent's own sends and kills); `_start_parent_notification` then
        covers an attempt this daemon launched meanwhile.
        """
        legacy_event_id = (
            payload.get("eventId") if payload.get("deliveryId") is None else None
        )
        return self._parent_notification_match(
            delivery_id,
            legacy_event_id=str(legacy_event_id) if legacy_event_id else None,
            name=name,
        )

    def _fence_parent_notification_locked(self, delivery_id: str) -> None:
        """Remember the last turn settled before a delivery's first launch.

        The notification can only land in a later turn, so a retry's history
        walk stops there. Only the first launch sets it: a retry's baseline may
        already be newer than the turn an earlier ambiguous attempt joined.
        """
        fences = self.record.get("parentNotifyFences")
        if not isinstance(fences, dict):
            fences = self.record["parentNotifyFences"] = {}
        if delivery_id in fences:
            return
        last_turn = self.record.get("lastTurn")
        fences[delivery_id] = (
            last_turn.get("turnId") if isinstance(last_turn, dict) else None
        )
        for stale in list(fences)[:-_PARENT_NOTIFY_FENCES]:
            del fences[stale]

    def _compact_refusal(self) -> dict[str, Any] | None:
        with self.lock:
            active = self.record.get("activeCompact")
            if not isinstance(active, dict):
                return None
            return {"ok": False, "state": self.record["lifecycle"]["state"],
                    "error": "Compaction owns the input surface",
                    "reason": "compaction-busy", "retryable": True,
                    "definitive": True, "operationId": active.get("operationId")}

    def _start_parent_notification(
        self,
        delivery_id: str,
        payload: dict[str, Any],
        *,
        reconcile: bool = True,
        history: tuple[dict[str, Any] | None] | None = None,
        namespace: str = "ccx",
        name: str = "worker_notification",
    ) -> dict[str, Any]:
        """Start-or-steer one machine continuation with kill-safe admission.

        Unlike a normal send this intentionally accepts an idle or active parent
        turn. Codex's turn/start semantics start a new turn when idle and steer
        the tool output into the active turn when busy. The launch marker keeps
        kill-vs-turn/start ordering as strong as ordinary user turns.
        """
        if history is not None:
            # Walked by the caller before it took the operation lock.
            match = history[0]
        elif reconcile:
            match = self.find_parent_notification(
                delivery_id, payload, name=name,
            )
        else:
            match = None
        if match is None and delivery_id in self.launched_notifications:
            # An attempt that held the lock while the walk ran outside it.
            match = {"turnId": self.launched_notifications[delivery_id]}
        if match is not None:
            with self.lock:
                marker = self.record.get("nativeTurnStart")
                if (
                    isinstance(marker, dict)
                    and marker.get("kind") == (
                        "parent-notification" if namespace == "ccx" else "machine-wake"
                    )
                    and marker.get("deliveryId") == delivery_id
                ):
                    self._forget_native_turn_start_locked()
                    self._save()
            return {
                "ok": True,
                "state": self.state(),
                "reconciled": True,
                "workerId": self.worker_id,
                "threadId": self._original_notification_thread(),
                "eventId": payload.get("eventId"),
                "deliveryId": delivery_id,
                "turnId": match.get("turnId"),
            }
        # A previous attempt can still land after the network reply is lost.
        # Reconcile-only retries may ACK positive history, never retransmit
        # merely because the history endpoint finds no matching item.
        if reconcile:
            return {"ok": False, "pending": True, "state": self.state(),
                    "error": "original machine wake was attempted; native placement remains unknown"}
        refusal = self._compact_refusal()
        if refusal:
            return refusal
        params = {
            "threadId": self._original_notification_thread(),
            "input": [],
            "turnTrigger": "ccx-worker-notification",
            "toolOutput": {
                "namespace": namespace,
                "name": name,
                "output": json.dumps(payload, separators=(",", ":")),
            },
        }
        with self.lock:
            if self.killed.is_set() or self.kill_intent.is_set():
                return {
                    "ok": False,
                    **({"definitive": True} if namespace != "ccx" else {}),
                    "error": f"worker {self.short} is being killed",
                    "state": self.record["lifecycle"]["state"],
                }
            if (not self.loaded or self.record["lifecycle"]["state"]
                    not in {"idle", "busy", "waiting"}):
                return {"ok": False, "definitive": True,
                        "error": "original native parent not subscribed/active",
                        "state": self.record["lifecycle"]["state"]}
            if self.launch is not None:
                return {"ok": False, "definitive": True,
                        "error": "another original native turn/start is in flight"}
            if isinstance(self.record.get("nativeTurnStart"), dict):
                return {"ok": False, "pending": True,
                        "error": "another original native turn/start remains unresolved"}
            previous_state = self.record["lifecycle"]["state"]
            previous_turn_id = self.record["lifecycle"].get("activeTurnId")
            launch = _TurnLaunch()
            self.launch = launch
            # A turn blocked on an approval stays `waiting`: the notification
            # is steered into that same turn, and publishing `busy` would hide
            # the pending approval from every waiter.
            if previous_state != "waiting":
                self.record["lifecycle"]["state"] = "busy"
                self.record["lifecycle"]["since"] = now_ms()
            self.record["nativeTurnStart"] = self._native_turn_start_marker(
                kind="parent-notification" if namespace == "ccx" else "machine-wake",
                delivery_id=delivery_id,
            )
            self._fence_parent_notification_locked(delivery_id)
            self.last_activity = time.monotonic()
            self._save()
        turn_id = None
        request_returned = False
        try:
            result = self.client.request("turn/start", params, timeout=60)
            turn_id = _turn_id(result)
            request_returned = True
        except ProtocolError as error:
            with self.lock:
                if self._native_mutation_error_is_ambiguous(error):
                    self._mark_native_turn_start_unknown_locked(error)
                else:
                    self._clear_native_turn_start_locked()
                    # Only an explicit native refusal proves the notification
                    # did not mutate the thread, so only then restore what we
                    # owned before the request.
                    if (
                        self.record["lifecycle"].get("activeTurnId")
                        == previous_turn_id
                    ):
                        self.record["lifecycle"]["state"] = previous_state
                        self.record["lifecycle"]["since"] = now_ms()
                        self._save()
            raise
        except Exception as error:
            with self.lock:
                self._mark_native_turn_start_unknown_locked(error)
            raise
        finally:
            with self.lock:
                if self.launch is launch:
                    self.launch = None
                launch.turn_id = turn_id
                if turn_id is not None:
                    self._forget_native_turn_start_locked()
                if request_returned and isinstance(turn_id, str) and turn_id:
                    self.launched_notifications[delivery_id] = turn_id
                lifecycle = self.record["lifecycle"]
                if request_returned and lifecycle["state"] in {"busy", "waiting"}:
                    lifecycle["activeTurnId"] = (
                        lifecycle.get("activeTurnId") or turn_id
                    )
                    if self.record.get("pendingApprovals"):
                        # An approval that arrived while the request was in
                        # flight still gates the turn.
                        lifecycle["state"] = "waiting"
                    self._save()
                launch.done.set()
        if self.killed.is_set():
            self._interrupt_turn(turn_id)
            return {
                "ok": False,
                "error": f"worker {self.short} was killed while starting notification",
                "state": self.state(),
            }
        if not isinstance(turn_id, str) or not turn_id:
            return {"ok": False, "pending": True, "state": self.state(),
                    "error": "original machine wake has no positive native turn ID"}
        return {
            "ok": True,
            "state": self.state(),
            "turnId": turn_id,
            "reconciled": False,
            "workerId": self.worker_id,
            "threadId": self._original_notification_thread(),
            "eventId": payload.get("eventId"),
            "deliveryId": delivery_id,
        }

    def handle_operation(
        self,
        message: dict[str, Any],
        *,
        history: tuple[dict[str, Any] | None] | None = None,
    ) -> dict[str, Any]:
        op = message.get("op")
        with self.lock:
            state = self.record["lifecycle"]["state"]
        if op == "kill":
            # Ahead of the booting/resuming guard on purpose: a worker stuck
            # mid-load is exactly one a caller needs to be able to kill.
            # `CodexDaemon.handle` routes kills through `_kill_worker` (which
            # owns the intent/budget protocol); this branch keeps a direct
            # caller correct.
            return self.kill(message.get("observeSeconds"))
        if self.killed.is_set() or self.kill_intent.is_set():
            # A kill was accepted but could not be confirmed yet (an unresolved
            # turn launch, or a prior operation that has not settled). The
            # worker stays registered so reconciliation can see it, but it must
            # accept no new work. `handle` refuses before taking the operation
            # lock; this repeats the check for an op that passed that gate and
            # only then found the intent published.
            return {"ok": False, "definitive": True,
                    "error": f"worker {self.short} is being killed", "state": state}
        if state in {"booting", "resuming"}:
            return {"ok": False, "error": f"worker is {state}", "state": state,
                    **({"definitive": True} if op == "machine-wake" else {})}

        if op in {"parent-notify", "machine-wake"}:
            event_id = message.get("eventId")
            delivery_id = message.get("deliveryId") or event_id
            payload = message.get("payload")
            if not isinstance(event_id, str) or not event_id:
                return {"ok": False, "error": "parent-notify requires eventId", "state": state}
            if not isinstance(delivery_id, str) or not delivery_id:
                return {"ok": False, "error": "parent-notify requires deliveryId", "state": state}
            if not isinstance(payload, dict):
                return {"ok": False, "error": "parent-notify requires payload", "state": state}
            try:
                return self._start_parent_notification(
                    delivery_id, payload,
                    reconcile=bool(message.get("reconcile", True)),
                    history=history,
                    namespace=(message["namespace"] if op == "machine-wake" else "ccx"),
                    name=(message["name"] if op == "machine-wake" else "worker_notification"),
                )
            except ProtocolError as error:
                failure = _protocol_failure(error)
                failure["definitive"] = not self._native_mutation_error_is_ambiguous(error)
                return failure

        if op == "interrupt":
            expected_turn_id = message.get("expectedTurnId")
            current_turn_id = self.record["lifecycle"].get("activeTurnId")
            if (
                expected_turn_id is not None
                and expected_turn_id != current_turn_id
            ):
                return {
                    "ok": False,
                    "error": "active turn changed",
                    "state": state,
                    "turnId": current_turn_id,
                    "definitive": True,
                }
            if state not in {"busy", "waiting"}:
                return {"ok": False, "definitive": True,
                        "error": "no active turn", "state": state}
            turn_id = self.record["lifecycle"].get("activeTurnId")
            if not turn_id:
                return {"ok": False, "definitive": True,
                        "error": "active turn id is unavailable", "state": state}
            # Full-UUID records keep the original thread under native.threadId.
            native = self.record.get("native") or {}
            self.client.request("turn/interrupt", {
                "threadId": native.get("threadId") or self.record["threadId"],
                "turnId": turn_id,
            })
            return {"ok": True, "state": state, "turnId": turn_id}
        if op in {"approve", "reject"}:
            # Existing operation_lock serializes decisions and unload. The
            # reader can still publish/resolve gates: select under worker lock,
            # then release it before the native server-request response.
            with self.lock:
                approval = self._select_approval(message.get("gateId"))
                gate_state = self.record["lifecycle"]["state"]
                native = self.record.get("native") or {}
                turn = self.record["lifecycle"].get("activeTurnId")
                thread = native.get("threadId")
                if (approval is None or gate_state != "waiting"
                        or not isinstance(turn, str) or not turn
                        or approval.get("turnId") != turn
                        or not isinstance(thread, str) or not thread
                        or native.get("threadSource") != self.daemon._uuid_store.codex_source(
                            f"codex:{self.worker_id}"
                        )
                        or self.daemon.by_thread.get(thread) != self.worker_id):
                    return {"ok": False, "definitive": True,
                            "error": "no current original Codex approval for gateId",
                            "state": gate_state}
                self.approval_decisions[_approval_key(approval)] = (
                    "accept" if op == "approve" else "decline"
                )
                if isinstance(message.get("responseId"), str):
                    self.approval_response_ids[_approval_key(approval)] = (
                        message["responseId"]
                    )
            self.client.respond(
                approval["requestId"],
                {"decision": "accept" if op == "approve" else "decline"},
            )
            with self.lock:
                self.record["pendingApprovals"] = [
                    value for value in self.record["pendingApprovals"]
                    if value is not approval
                ]
                # Same resolution as the serverRequest/resolved notification:
                # without an active turn the worker is idle, not busy — forcing
                # busy here would wedge a worker whose turn already completed.
                self.record["lifecycle"]["state"] = (
                    "waiting"
                    if self.record["pendingApprovals"]
                    else (
                        "busy"
                        if self.record["lifecycle"].get("activeTurnId")
                        else "idle"
                    )
                )
                self.record["lifecycle"]["since"] = now_ms()
                self._save()
            return {
                "ok": True,
                "state": self.record["lifecycle"]["state"],
                "requestId": approval["requestId"],
                "gateId": approval.get("gateId"),
                "workerId": self.worker_id,
                "threadId": thread,
                "turnId": approval.get("turnId"),
                "decision": "accept" if op == "approve" else "decline",
            }
        if op == "compact":
            from ccx.codex.ops import compact_intent_digest
            from ccx.store.store import _digest

            operation_id = message.get("operationId")
            if (not isinstance(operation_id, str) or not operation_id
                    or len(operation_id) > 128 or "\x00" in operation_id):
                return {"ok": False, "definitive": True,
                        "error": "compact requires caller operationId; no native request"}
            ctx = self.daemon._uuid_store
            try:
                owner = ctx.resolve(f"codex:{self.worker_id}")
                original = ctx.record(owner.worker)
                native = original.get("native") or {}
                source = ctx.codex_source(owner.worker)
                thread_id = native.get("threadId")
                activation = ctx.native_activation(owner.worker)
                if (owner.engine != "codex"
                        or activation.get("state") != "confirmed"
                        or activation.get("source") != source
                        or not isinstance(thread_id, str) or not thread_id
                        or native.get("threadSource") != source
                        or (self.record.get("native") or {}).get("threadId") != thread_id
                        or self.daemon.by_thread.get(thread_id) != self.worker_id
                        or message.get("threadId") != thread_id):
                    raise ValueError("original Codex native thread/source unverified")
                effect = ctx.read_receipt(owner.worker, "codex-compact", operation_id)
                digest = compact_intent_digest(
                    owner.worker_id, ctx.generation, thread_id, source, operation_id,
                )
                if (not isinstance(effect, dict)
                        or effect.get("intentSha256") != digest
                        or effect.get("workerId") != owner.worker_id
                        or effect.get("engine") != "codex"
                        or effect.get("generation") != ctx.generation
                        or effect.get("family") != "codex-compact"
                        or effect.get("action") != "compact"
                        or effect.get("state") not in (
                            "submitted-unknown", "accepted", "rejected"
                        )):
                    raise ValueError("original full-UUID submitted compact receipt absent")
                ctx.revalidate()
            except Exception as error:
                return {"ok": False, "definitive": True,
                        "error": f"Codex compact original admission denied: {error}"}

            identity = {
                "workerId": owner.worker_id, "engine": "codex",
                "generation": ctx.generation, "nativeSource": source,
                "threadId": thread_id, "operationId": operation_id,
            }

            def acknowledged(started_at: int, *, accepted: bool,
                             reason: str | None = None, replayed: bool = False) -> dict:
                data = {
                    **identity, "startedAtMs": started_at,
                    "nativeEvidenceSha256": _digest({
                        **identity, "startedAtMs": started_at,
                        "admission": "thread/compact/start",
                    }),
                }
                if accepted:
                    return {**data, "ok": True, "nativeStatus": "accepted",
                            "replayed": replayed, "state": self.record["lifecycle"]["state"]}
                return {**data, "ok": False, "definitive": True, "noEffect": True,
                        "error": reason or "compact refused before native request",
                        "state": self.record["lifecycle"]["state"]}

            # Same-ID replay must precede the idle gate: a live compact owns
            # busy and may have outlived a lost caller ACK. Never paste twice.
            with self.lock:
                active = self.record.get("activeCompact")
                last = self.record.get("lastCompact")
                match = next((
                    marker for marker in (active, last)
                    if isinstance(marker, dict)
                    and marker.get("operationId") == operation_id
                ), None)
                if match is not None:
                    if effect["state"] == "rejected":
                        return {"ok": False, "error": "rejected compact has a marker",
                                "reconcileRequired": True}
                    started_at = match.get("startedAtMs")
                    if type(started_at) is not int:
                        return {"ok": False, "reconcileRequired": True,
                                "error": "existing compact marker has no original start"}
                    return acknowledged(started_at, accepted=True, replayed=True)
                if effect["state"] != "submitted-unknown":
                    return acknowledged(now_ms(), accepted=False,
                                        reason="compact effect already settled; no resubmit")
                state = self.record["lifecycle"]["state"]
                if (state != "idle" or isinstance(active, dict)
                        or self.launch is not None
                        or self.record.get("pendingApprovals")):
                    return acknowledged(now_ms(), accepted=False,
                                        reason=f"compact requires idle worker, state={state}")
                before = self.record.get("context")
                if isinstance(before, dict):
                    before = {**before, "state": state}
                baseline = int(self.record.get("compactCount") or 0)
                started_at = now_ms()
                self.record["activeCompact"] = {
                    "operationId": operation_id,
                    "awaitingTurn": True,
                    "daemonInstanceId": self._native_create_daemon_id(),
                    "baselineCompactCount": baseline,
                    "targetCompactCount": baseline + 1,
                    "startedAtMs": started_at,
                    "contextBefore": before,
                }
                self.record["lifecycle"]["state"] = "busy"
                self.record["lifecycle"]["since"] = started_at
                # Native submission is impossible until its exact durable
                # marker and earlier global submitted intent both exist.
                self._publish_context_locked(force=True, save=False)
                self._save()
            try:
                self.client.request(
                    "thread/compact/start", {"threadId": thread_id}, timeout=1.5,
                )
            except ProtocolError as error:
                definite = isinstance(error, ProtocolNotSent) or (
                    error.request_id is not None
                    and not isinstance(error, ProtocolTransportError)
                )
                if definite:
                    with self.lock:
                        active = self.record.get("activeCompact")
                        if (isinstance(active, dict)
                                and active.get("operationId") == operation_id):
                            self.record["activeCompact"] = None
                            self.record["lifecycle"]["state"] = "idle"
                            self.record["lifecycle"]["since"] = now_ms()
                            self._publish_context_locked(force=True, save=False)
                            self._save()
                    return acknowledged(
                        started_at, accepted=False, reason=str(error),
                    )
                # Transport ambiguity: persisted activeCompact stays busy,
                # fenced by this exact global receipt for same-ID inspection.
                return {**identity, **_protocol_failure(error),
                        "state": "busy", "reconcileRequired": True}
            # Native admission ACK is not contextCompaction completion.
            return acknowledged(started_at, accepted=True)
        return {"ok": False, "error": f"unknown worker operation {op!r}"}

    def public_status(self) -> dict[str, Any]:
        with self.lock:
            lifecycle = dict(self.record["lifecycle"])
            context = self.record.get("context")
            if context:
                context = {**context, "state": lifecycle.get("state")}
            return {
                "short": self.short,
                "threadId": (self.record.get("native") or {}).get("threadId"),
                "name": self.record.get("name"),
                "cwd": self.record.get("cwd"),
                "state": lifecycle.get("state"),
                "activeTurnId": lifecycle.get("activeTurnId"),
                "pendingApprovals": list(self.record.get("pendingApprovals") or []),
                "lastReport": self.record.get("lastReport"),
                "lastTurn": self.record.get("lastTurn"),
                "context": context,
                "lastError": lifecycle.get("lastError"),
                "activeTurnError": self.record.get("activeTurnError"),
                # Evidence health travels beside worker health, never as part
                # of it: a worker whose journal degraded is still idle. Null
                # for a worker that has produced no evidence at all.
                "evidence": self.record.get("evidence"),
                "persistenceError": self.persistence_error,
            }
