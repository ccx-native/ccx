"""Sparse durable evidence for what a native Codex turn actually did.

``thread/read`` reconstructs a thread's content. It does not carry the moments
CCX observed while the turn ran: when an item started, when it completed, the
exact native millisecond timestamps on those notifications, or a structured
``error`` the turn recovered from. Those exist only as live notifications, and
once they are reduced into ``lastTurn``/``lastReport``/``lifecycle`` the detail
is gone. This journal is where the authoritative event survives the reduction.

It is deliberately sparse. Only lifecycle evidence is written —
``turn/started``, ``item/started``, ``item/completed``, ``error``,
``turn/completed`` — never the token, output, or reasoning delta streams, which
would turn a small durable record into a firehose that costs a worker its
throughput. One file per worker, owner-only, outside the lifecycle record so
that record stays compact.

Ordering: ``sequence`` is CCX's own per-worker observation order, and nothing
else. It is not the content order of the normalized document (that is
``ordinal``), and it is not native time (that is ``nativeTimestampMs``, present
only where the native notification actually carried one).

Reading is one simple pass over the file, and :func:`build_index` materializes
the folded result — the same memory tradeoff the conversation document already
accepts, and the reason the journal stays sparse rather than becoming a delta
log nobody can afford to read back.

Durability: every selected event is flushed to the OS before ``record``
returns, so a CCX crash loses nothing, and turn settlement additionally
``fsync``s — the barrier that bounds what a machine crash can take to the tail
of one unsettled turn. Nothing here waits on the network or the model.

Integrity is stated, never repaired. ``sequence`` is part of it: a gap, a
duplicate, a backwards number, a malformed line, and a line that parses but is
not an envelope all mean evidence was lost, and each is reported. A partial
tail's number is reserved rather than reused, so the gap it leaves is the
durable record that one event may be missing. And degradation is sticky: a
writer rehydrates what the worker's record already knew, a reader merges the
file's integrity with that record, and no later success anywhere presents a
worker's evidence as complete once CCX has recorded that it is not.
"""

from __future__ import annotations

import hashlib
import json
import os
import pathlib
import threading
import uuid
from typing import Any

from ccx.codex.paths import ensure_dirs, evidence_path
from ccx.codex.provenance import parse_client_message_id
from ccx.codex.records import now_ms, read_record
from ccx.system.sanitize import safe_native

SCHEMA_VERSION = 1

# The header CCX writes when it begins journaling. It is what separates "no
# such event ever happened" from "CCX was not yet watching", which a consumer
# otherwise cannot tell apart from an empty stretch of journal.
HEADER_METHOD = "ccx/evidence-started"

# Authoritative lifecycle evidence only. Deltas are excluded by construction:
# adding one here is how this file stops being sparse.
TOKEN_USAGE_METHOD = "thread/tokenUsage/updated"
COMPACTION_CONTEXT_METHOD = "ccx/compaction/context"

# CCX's own lifecycle facts, which no native notification carries: the
# worker's state transitions and its approval requests coming and going. They
# exist for the event feed (`ccx/event_feed.py`) and are deliberately kept out
# of turn and item evidence: their params never carry a top-level `turnId`, and
# `build_index` skips them.
LIFECYCLE_METHOD = "ccx/lifecycle"
INPUT_REQUESTED_METHOD = "ccx/input/requested"
INPUT_RESOLVED_METHOD = "ccx/input/resolved"
COMPACT_STARTED_METHOD = "ccx/compact/started"
COMPACT_ENDED_METHOD = "ccx/compact/ended"
# A content-free wake, journaled only after a delivery receipt became
# accepted. It is never evidence: readers reconcile the delivery id.
DELIVERY_CHANGED_METHOD = "ccx/delivery/changed"
BACKGROUND_CHANGED_METHOD = "ccx/background/changed"
FEED_METHODS = frozenset({
    DELIVERY_CHANGED_METHOD,
    BACKGROUND_CHANGED_METHOD,
    LIFECYCLE_METHOD,
    INPUT_REQUESTED_METHOD,
    INPUT_RESOLVED_METHOD,
    COMPACT_STARTED_METHOD,
    COMPACT_ENDED_METHOD,
})

JOURNALED_METHODS = frozenset({
    "turn/started",
    "item/started",
    "item/completed",
    "error",
    "turn/completed",
    TOKEN_USAGE_METHOD,
    COMPACTION_CONTEXT_METHOD,
}) | FEED_METHODS

# Settlement and bounded telemetry enrichments get a real durability barrier.
_DURABLE_METHODS = frozenset({
    "turn/completed",
    COMPACTION_CONTEXT_METHOD,
})

_ITEM_TIMESTAMP = {
    "item/started": "startedAtMs",
    "item/completed": "completedAtMs",
}


def journal_path(short: str) -> pathlib.Path:
    return evidence_path(short)


def _owned_evidence_fd(target: str, flags: int, *, store=None) -> int:
    """Pinned nofollow actual journal inode, never a pathname authority.

    The worker record's full global UUID is the only owner index. A symlink,
    replaced file inode, wrong uid or too-open file is refused, not chmodded
    into trust. The ACTIVE lease bounds opening, not native turn completion.
    """
    from ccx import constants
    from ccx.store.root import require_ccx_active
    from ccx.store.identity import EngineMismatch
    import stat
    ctx = store if store is not None else require_ccx_active(constants.CCX_ROOT)
    owner = ctx.resolve(target)
    if owner.engine != "codex":
        raise EngineMismatch("Codex journal requires Codex full UUID")
    with ctx.lease():
        with ctx._gen("workers", "codex", owner.worker_id) as directory:
            fd = os.open(
                "evidence.jsonl",
                flags | os.O_NOFOLLOW | os.O_CLOEXEC,
                0o600, dir_fd=directory,
            )
            try:
                actual = os.fstat(fd)
                named = os.stat(
                    "evidence.jsonl", dir_fd=directory,
                    follow_symlinks=False,
                )
                if (not stat.S_ISREG(actual.st_mode)
                        or actual.st_uid != os.getuid()
                        or actual.st_mode & 0o077
                        or (actual.st_dev, actual.st_ino)
                        != (named.st_dev, named.st_ino)):
                    raise PermissionError(
                        "Codex full-UUID journal inode not owned/private"
                    )
            except BaseException:
                os.close(fd)
                raise
    return fd


def _int_or_none(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _text_or_none(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _is_envelope(value: Any) -> bool:
    """Is this parsed line actually one of our journal envelopes?

    Deliberately the minimum this schema needs and nothing more — an integer
    ``sequence`` alone does not make a JSON object evidence, and a line that
    parses but cannot be interpreted is lost evidence, not a valid record. This
    is not, and must not grow into, a general schema validator.
    """
    if not isinstance(value, dict):
        return False
    if _int_or_none(value.get("schemaVersion")) is None:
        return False
    if _int_or_none(value.get("sequence")) is None:
        return False
    if _int_or_none(value.get("observedAtMs")) is None:
        return False
    if value.get("method") not in JOURNALED_METHODS | {HEADER_METHOD}:
        return False
    return isinstance(value.get("params"), dict)


def envelope(
    method: str,
    params: dict[str, Any],
    *,
    sequence: int,
    thread_id: str | None = None,
) -> dict[str, Any]:
    """One journal line: sanitized native params plus CCX's own observation.

    The convenience ids are read out of ``params`` rather than supplied
    alongside it, so an envelope cannot disagree with the event it carries.
    """
    safe_params = safe_native(params)
    if not isinstance(safe_params, dict):
        safe_params = {"value": safe_params}
    turn = safe_params.get("turn")
    turn = turn if isinstance(turn, dict) else {}
    item = safe_params.get("item")
    item = item if isinstance(item, dict) else {}
    return {
        "schemaVersion": SCHEMA_VERSION,
        "sequence": sequence,
        # CCX receipt time. Never presented as native time.
        "observedAtMs": now_ms(),
        "method": method,
        "threadId": _text_or_none(safe_params.get("threadId")) or thread_id,
        "turnId": (
            _text_or_none(safe_params.get("turnId"))
            or _text_or_none(turn.get("id"))
        ),
        "itemId": _text_or_none(item.get("id")),
        "itemType": _text_or_none(item.get("type")),
        # Exactly the native timestamp the notification carried, or nothing.
        "nativeTimestampMs": _int_or_none(
            safe_params.get(_ITEM_TIMESTAMP.get(method, ""))
        ),
        "params": safe_params,
    }


class JournalRead:
    """The result of one pass over a worker's journal."""

    __slots__ = (
        "short", "path", "exists", "events", "last_sequence",
        "partial_tail", "malformed_lines", "sequence_issues",
        "journal_instance_id",
    )

    def __init__(
        self,
        short: str,
        path: pathlib.Path,
        *,
        exists: bool,
        events: list[dict[str, Any]],
        last_sequence: int,
        partial_tail: bool,
        malformed_lines: list[int],
        sequence_issues: list[str] | None = None,
        journal_instance_id: str | None = None,
    ):
        self.short = short
        self.path = path
        self.exists = exists
        self.events = events
        self.last_sequence = last_sequence
        self.partial_tail = partial_tail
        self.malformed_lines = malformed_lines
        self.sequence_issues = sequence_issues or []
        self.journal_instance_id = journal_instance_id

    @property
    def complete(self) -> bool:
        """Is every line this journal holds intact?

        A partial tail (a write interrupted mid-line), a malformed complete
        line, and a break in the sequence chain all mean evidence was lost.
        None of them is skipped quietly, and none of them is repaired.
        """
        return not (
            self.partial_tail or self.malformed_lines or self.sequence_issues
        )

    @property
    def warnings(self) -> list[str]:
        notes = []
        if self.partial_tail:
            notes.append(
                "the evidence journal ends in a partial line: a write was "
                "interrupted, so its last event is incomplete"
            )
        if self.malformed_lines:
            shown = ", ".join(str(n) for n in self.malformed_lines[:10])
            notes.append(
                f"{len(self.malformed_lines)} malformed evidence line(s) "
                f"(line {shown})"
            )
        notes.extend(self.sequence_issues[:10])
        if len(self.sequence_issues) > 10:
            notes.append(
                f"{len(self.sequence_issues) - 10} further sequence problem(s)"
            )
        return notes


def _explicit_instance_id(header: dict[str, Any]) -> str | None:
    params = header.get("params")
    params = params if isinstance(params, dict) else {}
    return _text_or_none(params.get("journalInstanceId"))


def header_instance_id(header: dict[str, Any]) -> str:
    """The journal instance a valid evidence-start envelope names.

    Pre-instance-ID journals still need a stable epoch: it is derived from
    their first valid header, and the writer persists the same value on its
    next open.
    """
    explicit = _explicit_instance_id(header)
    if explicit is not None:
        return explicit
    seed = json.dumps(
        header,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "legacy-" + hashlib.sha256(seed).hexdigest()[:32]


def read_journal(short: str, *, events: bool = True) -> JournalRead:
    """Scan a worker's journal once, for its events and/or its sequence.

    Recovery reads the file itself rather than trusting a counter in the
    lifecycle record: the record and the journal are written separately, and
    only the journal knows what actually reached the disk.
    """
    path = journal_path(short)
    collected: list[dict[str, Any]] = []
    last_sequence = 0
    previous = 0
    partial_tail = False
    malformed: list[int] = []
    issues: list[str] = []
    journal_instance_id: str | None = None
    try:
        with os.fdopen(_owned_evidence_fd(short, os.O_RDONLY), "rb") as handle:
            for number, raw in enumerate(handle, 1):
                if not raw.endswith(b"\n"):
                    # An unterminated fragment is not an event. It is never
                    # parsed and its sequence is never claimed.
                    partial_tail = True
                    break
                line = raw.strip()
                if not line:
                    continue
                try:
                    value = json.loads(line)
                except ValueError:
                    malformed.append(number)
                    continue
                # A parseable line can still be unusable. Its sequence is read
                # anyway when it is safe to: recovery must never hand the same
                # number to a future event just because this line was damaged.
                sequence = (
                    _int_or_none(value.get("sequence"))
                    if isinstance(value, dict) else None
                )
                if not _is_envelope(value):
                    malformed.append(number)
                else:
                    if value.get("method") == HEADER_METHOD:
                        explicit = _explicit_instance_id(value)
                        if explicit is not None:
                            if (
                                journal_instance_id is not None
                                and journal_instance_id != explicit
                            ):
                                issues.append(
                                    f"line {number}: evidence journal instance changed "
                                    f"from {journal_instance_id} to {explicit}"
                                )
                            else:
                                journal_instance_id = explicit
                        elif journal_instance_id is None:
                            journal_instance_id = header_instance_id(value)
                    if events:
                        collected.append(value)
                if sequence is None:
                    continue
                if sequence == previous:
                    issues.append(
                        f"line {number}: duplicate evidence sequence {sequence}"
                    )
                elif sequence < previous:
                    issues.append(
                        f"line {number}: evidence sequence {sequence} goes "
                        f"backwards after {previous}"
                    )
                elif sequence > previous + 1:
                    # The writer reserves a number before it writes, so a gap
                    # is positive evidence that an event was lost.
                    missing = (
                        f"{previous + 1}" if sequence == previous + 2
                        else f"{previous + 1}-{sequence - 1}"
                    )
                    issues.append(
                        f"line {number}: evidence sequence gap ({missing} "
                        f"never landed)"
                    )
                previous = max(previous, sequence)
                last_sequence = max(last_sequence, sequence)
    except FileNotFoundError:
        return JournalRead(
            short, path, exists=False, events=[], last_sequence=0,
            partial_tail=False, malformed_lines=[], journal_instance_id=None,
        )
    return JournalRead(
        short, path, exists=True, events=collected,
        last_sequence=last_sequence, partial_tail=partial_tail,
        malformed_lines=malformed, sequence_issues=issues,
        journal_instance_id=journal_instance_id,
    )


class EvidenceJournal:
    """The append-only writer for one worker's journal.

    Opened lazily, so a worker that never produces evidence never creates a
    file — which is itself the honest answer to "was CCX journaling?".
    """

    def __init__(
        self,
        short: str,
        *,
        codex_version: str | None = None,
        health: dict[str, Any] | None = None,
    ):
        self.short = short
        self.path = journal_path(short)
        self.codex_version = codex_version
        self._lock = threading.Lock()
        self._handle = None
        self._closed = False
        self._sequence = 0
        self._partial_tail = False
        self._journal_instance_id: str | None = None
        self._degraded: dict[str, Any] | None = None
        # A worker outlives the process that watched it. Rehydrating the
        # durable health block is what makes degradation survive a daemon
        # restart: once CCX knows an event was lost, no later successful append
        # may present that worker's evidence as complete again. Only an
        # explicit repair could ever clear it, and none exists.
        if isinstance(health, dict) and health.get("status") == "degraded":
            self._degraded = {
                "reason": str(
                    health.get("degradedReason")
                    or "evidence was already recorded as degraded"
                )[:300],
                "atMs": _int_or_none(health.get("degradedAtMs")) or now_ms(),
            }

    def state(self) -> dict[str, Any]:
        """The smallest durable description of this journal's health."""
        with self._lock:
            return self._state_locked()

    def _state_locked(self) -> dict[str, Any]:
        degraded = self._degraded
        return {
            "schemaVersion": SCHEMA_VERSION,
            "status": "degraded" if degraded else "complete",
            "sequence": self._sequence,
            "journalInstanceId": self._journal_instance_id,
            "degradedReason": degraded["reason"] if degraded else None,
            "degradedAtMs": degraded["atMs"] if degraded else None,
        }

    @property
    def degraded(self) -> dict[str, Any] | None:
        return self._degraded

    @property
    def closed(self) -> bool:
        """Has a teardown released this journal? A later write would reopen
        it with a fresh header, so CCX's own lifecycle lines stop here."""
        return self._closed

    @property
    def observed(self) -> bool:
        """Has this worker any evidence story to persist at all?

        False for a worker that never journaled anything, so a pre-journal
        worker keeps a null evidence block rather than being credited with
        complete evidence it never produced.
        """
        return self._sequence > 0 or self._degraded is not None

    def _degrade(self, reason: str) -> None:
        # Once evidence is known to be incomplete it stays that way: a later
        # successful append does not make the lost event reappear.
        if self._degraded is None:
            self._degraded = {"reason": reason[:300], "atMs": now_ms()}

    def _open_locked(self) -> None:
        if self._handle is not None:
            return
        ensure_dirs()
        scan = read_journal(self.short, events=False)
        self._sequence = scan.last_sequence
        self._partial_tail = scan.partial_tail
        self._journal_instance_id = (
            scan.journal_instance_id or str(uuid.uuid4())
        )
        if scan.partial_tail:
            # The interrupted line was mid-write, so it had already been given
            # the next number. Reserve it rather than handing it to a new
            # event: the resulting gap is the durable record that one event may
            # have been lost here.
            self._sequence += 1
        if not scan.complete:
            self._degrade(
                "the existing journal is incomplete: "
                + "; ".join(scan.warnings)
            )
        descriptor = _owned_evidence_fd(
            self.short, os.O_WRONLY | os.O_CREAT | os.O_APPEND,
        )
        self._handle = os.fdopen(descriptor, "a", encoding="utf-8")
        if scan.partial_tail:
            # Close the interrupted fragment on its own line. Appending
            # straight onto it would fuse a broken record and a good one into
            # a single unreadable line, losing the new event as well.
            self._handle.write("\n")
        self._write_locked(HEADER_METHOD, {
            "schemaVersion": SCHEMA_VERSION,
            "short": self.short,
            "journalInstanceId": self._journal_instance_id,
            "codexVersion": self.codex_version,
            "resumedFromSequence": scan.last_sequence,
            "partialTail": scan.partial_tail,
        }, durable=True)

    def _write_locked(
        self,
        method: str,
        params: dict[str, Any],
        *,
        thread_id: str | None = None,
        durable: bool = False,
    ) -> int:
        # A failed write still consumes its number rather than handing it to
        # the next event: sequence is strictly increasing, and a gap is honest
        # evidence that something was lost.
        self._sequence += 1
        line = envelope(
            method, params, sequence=self._sequence, thread_id=thread_id
        )
        self._handle.write(json.dumps(line, separators=(",", ":")) + "\n")
        # Always past CCX's own buffers, so a CCX crash cannot lose an event.
        self._handle.flush()
        if durable:
            os.fsync(self._handle.fileno())
        return self._sequence

    def record(
        self,
        method: str,
        params: dict[str, Any],
        *,
        thread_id: str | None = None,
        durable: bool | None = None,
    ) -> int | None:
        """Journal one selected event. Never raises.

        A journal that cannot be written is a degraded observation, not a
        reason to lose a running native turn — so the failure is latched into
        this journal's state (which the caller persists) instead of
        propagating into the notification path that drives worker health.
        """
        if method not in JOURNALED_METHODS:
            return None
        try:
            with self._lock:
                self._open_locked()
                return self._write_locked(
                    method,
                    params,
                    thread_id=thread_id,
                    durable=(
                        method in _DURABLE_METHODS
                        if durable is None else bool(durable)
                    ),
                )
        except Exception as error:  # noqa: BLE001 - see docstring
            with self._lock:
                self._degrade(f"{error.__class__.__name__}: {error}")
            return None

    def close(self) -> None:
        """Flush and release the journal. Never raises.

        The final durability barrier failing is exactly the case where the tail
        of the evidence may not be on disk, so it degrades the journal's health
        rather than vanishing — but it still must not take down the worker
        teardown that called it.
        """
        with self._lock:
            self._closed = True
            handle, self._handle = self._handle, None
            if handle is None:
                return
            try:
                handle.flush()
                os.fsync(handle.fileno())
            except Exception as error:  # noqa: BLE001 - see docstring
                self._degrade(
                    f"the journal's final flush failed: "
                    f"{error.__class__.__name__}: {error}"
                )
            try:
                handle.close()
            except Exception as error:  # noqa: BLE001 - see docstring
                self._degrade(
                    f"the journal could not be closed cleanly: "
                    f"{error.__class__.__name__}: {error}"
                )


# One CCX client id seen on two live items names neither of them.
_UNSET = object()


def _merged_health(
    read: JournalRead, health: dict[str, Any] | None
) -> tuple[str, list[str]]:
    """Fold the file's own integrity together with the durable record health.

    The two can disagree in both directions, and only one of those directions
    is evidence loss. A file ahead of the record is ordinary: the journal is
    flushed before the record is written, so a crash in between leaves exactly
    that. A record ahead of the file, or a record that remembers evidence where
    no file remains, means something the worker durably knew about is gone.
    """
    warnings = list(read.warnings)
    recorded_sequence = 0
    if isinstance(health, dict):
        recorded_sequence = _int_or_none(health.get("sequence")) or 0
        if health.get("status") == "degraded":
            reason = _text_or_none(health.get("degradedReason"))
            warnings.append(
                "the worker recorded its evidence as degraded"
                + (f": {reason}" if reason else "")
            )
    if not read.exists and recorded_sequence > 0:
        warnings.append(
            f"the worker recorded {recorded_sequence} evidence event(s) but "
            f"the journal file is gone"
        )
    elif read.exists and recorded_sequence > read.last_sequence:
        warnings.append(
            f"the worker recorded evidence through sequence "
            f"{recorded_sequence} but the journal ends at "
            f"{read.last_sequence}"
        )
    if warnings:
        return "degraded", warnings
    return ("complete" if read.exists else "absent"), warnings


def build_index(
    read: JournalRead, *, health: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Fold a journal into the evidence a normalized document can use.

    Identity is exact and native: an item is only ever keyed by the
    ``(turnId, itemId)`` the event stream itself carried. Live and
    reconstructed ``thread/read`` item ids may legitimately differ (Scope 02),
    so an unmatched historical item stays unmatched — nothing here joins two
    source-native ids because they look similar.

    The one exception is not a heuristic at all: a native ``userMessage``
    carries the ``clientId`` CCX itself minted, on both surfaces. That is a
    real shared identity for the semantic message, so it is indexed separately
    — and only when the id parses as one CCX owns and names exactly one live
    item in its turn.

    ``health`` is the worker's durable evidence block. Degradation recorded
    there outranks a file whose surviving lines happen to parse cleanly.
    """
    turns: dict[str, dict[str, Any]] = {}
    items: dict[str, dict[str, dict[str, Any]]] = {}
    clients: dict[str, dict[str, str | None]] = {}
    began_at: int | None = None
    codex_version: str | None = None
    counted = 0

    for event in read.events:
        method = event.get("method")
        sequence = _int_or_none(event.get("sequence"))
        observed = _int_or_none(event.get("observedAtMs"))
        if method == HEADER_METHOD:
            if began_at is None:
                began_at = observed
                params = event.get("params")
                if isinstance(params, dict):
                    codex_version = _text_or_none(params.get("codexVersion"))
            continue
        if method not in JOURNALED_METHODS or method in FEED_METHODS:
            continue
        counted += 1
        turn_id = _text_or_none(event.get("turnId"))
        if turn_id is None:
            continue
        turn = turns.setdefault(turn_id, {
            "startedAtMs": None,
            "completedAtMs": None,
            "sequences": [],
            "errorCount": 0,
            # Turn-level coverage: whether CCX watched this turn from its start
            # through its settlement, and how many distinct item lifecycles it
            # saw while it did.
            "sawTurnStarted": False,
            "sawTurnCompleted": False,
            "distinctItemCount": 0,
        })
        if sequence is not None:
            turn["sequences"].append(sequence)
        if method == "turn/started":
            turn["sawTurnStarted"] = True
            turn["startedAtMs"] = turn["startedAtMs"] or observed
        elif method == "turn/completed":
            turn["sawTurnCompleted"] = True
            turn["completedAtMs"] = observed
        elif method == "error":
            turn["errorCount"] += 1

        item_id = _text_or_none(event.get("itemId"))
        if item_id is None:
            continue
        turn_items = items.setdefault(turn_id, {})
        new_item = item_id not in turn_items
        entry = turn_items.setdefault(item_id, {
            "itemType": None,
            "clientId": None,
            "startedAtMs": None,
            "completedAtMs": None,
            "sequences": [],
            "completed": None,
        })
        if new_item:
            turn["distinctItemCount"] += 1
        entry["itemType"] = entry["itemType"] or _text_or_none(
            event.get("itemType")
        )
        if sequence is not None:
            entry["sequences"].append(sequence)
        params = event.get("params")
        item = params.get("item") if isinstance(params, dict) else None
        if isinstance(item, dict):
            client_id = _text_or_none(item.get("clientId"))
            if client_id and parse_client_message_id(client_id):
                entry["clientId"] = entry["clientId"] or client_id
                turn_clients = clients.setdefault(turn_id, {})
                known = turn_clients.get(client_id, _UNSET)
                if known is _UNSET:
                    turn_clients[client_id] = item_id
                elif known != item_id:
                    # The same CCX id on two different live items is not an
                    # identity any more. Refuse it rather than pick one.
                    turn_clients[client_id] = None
        native = _int_or_none(event.get("nativeTimestampMs"))
        if method == "item/started":
            entry["startedAtMs"] = entry["startedAtMs"] or native
        elif method == "item/completed":
            entry["completedAtMs"] = native
            if isinstance(item, dict):
                entry["completed"] = item

    status, warnings = _merged_health(read, health)
    return {
        "available": read.exists,
        # File integrity alone; `status` is the answer that also accounts for
        # what the worker durably recorded.
        "complete": status == "complete",
        "status": status,
        "beganAtMs": began_at,
        "codexVersion": codex_version,
        "eventCount": counted,
        "turns": turns,
        "items": items,
        "clients": clients,
        "warnings": warnings,
    }


def read_index(short: str) -> dict[str, Any]:
    """One-call read + fold for a worker's journal, health included."""
    record = read_record(short) or {}
    health = record.get("evidence")
    return build_index(
        read_journal(short),
        health=health if isinstance(health, dict) else None,
    )


def export_artifacts(short: str) -> list[tuple[str, str, str]]:
    """``(kind, filename, source path)`` for whatever evidence exists.

    Empty when a worker has no journal, so an export never advertises evidence
    it does not have.
    """
    path = journal_path(short)
    if not path.exists():
        return []
    return [("codex-evidence-jsonl", "codex-evidence.jsonl", str(path))]


__all__ = [
    "COMPACTION_CONTEXT_METHOD",
    "HEADER_METHOD",
    "JOURNALED_METHODS",
    "TOKEN_USAGE_METHOD",
    "SCHEMA_VERSION",
    "EvidenceJournal",
    "JournalRead",
    "build_index",
    "envelope",
    "export_artifacts",
    "journal_path",
    "read_index",
    "read_journal",
]
