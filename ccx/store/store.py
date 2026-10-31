from __future__ import annotations
import hashlib
import json
import os
import re
import stat
import time
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterator, Mapping
from uuid import uuid4, uuid5, NAMESPACE_URL
from ccx.store.liveness import recorded_host_loaded
from ccx.store.worker_state import normalize_native_state
from ccx.store.identity import (
    ENGINES, EngineMismatch, InvalidTarget, OwnerConflict, UnknownWorker,
    WorkerOwner, canonical_worker_id, parse_managed_target,
)
from ccx.store.root import (
    CreationConflict, CreationIndeterminate, GenerationError, GenerationPointer, GenerationUnavailable, NativeActionDenied, NativeEffectIndeterminate, RecordConflict, StaleGeneration, StorageIntegrityError, _CLAIM_SCHEMA, _FAMILIES, _FILE_FLAGS, _HASH, _NATIVE_WRITES, _OWNER_SCHEMA, _RECEIPT_SCHEMA, _flock_at, _open_child_dir, _open_dir, _read_json, _write_json, inspect_generation
)


@dataclass(frozen=True)
class CreationReservation:
    worker_id: str
    engine: str
    create_sha256: str
    reconciled: bool
    record_committed: bool
    # Original native activation fate, never inferred from reconciled/record_committed.
    activation_state: str = "unproven"
    submission_id: str | None = None
    # Immutable managed caller from the first owner intent, not the retry env.
    spawned_by: dict | None = None


@dataclass(frozen=True)
class NativeActivation:
    """Exact original activation intent (not permission to replay a native call)."""
    worker_id: str
    engine: str
    source: str
    submission_id: str
    state: str
    generation: str


@dataclass(frozen=True)
class OperationIntent:
    worker_id: str
    engine: str
    family: str
    operation_id: str
    intent_sha256: str
    state: str
    generation: str
    dispatch_token: str | None = None
    replayed: bool = False
    native_evidence_sha256: str | None = None


def _digest(obj: object) -> str:
    try:
        raw = json.dumps(obj, sort_keys=True, separators=(",", ":"),
                         ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (ValueError, TypeError) as exc:
        raise StorageIntegrityError("noncanonical immutable input") from exc
    return hashlib.sha256(raw).hexdigest()


def converted_binding_digest(worker_id: str, engine: str, native_binding: Mapping,
                             converted_from: Mapping) -> str:
    """One domain-separated offline-conversion digest, NOT a native create hash."""
    return _digest({
        "kind": "ccx-convert", "workerId": canonical_worker_id(worker_id),
        "engine": engine, "nativeBinding": dict(native_binding),
        "convertedFrom": dict(converted_from),
    })


def _converted_binding(owner: Mapping, claim: Mapping, record: Mapping) -> str:
    """Pure proof of one current-format converted native identity; never an ACK."""
    intent = owner.get("createIntent")
    cf = intent.get("convertedFrom") if isinstance(intent, dict) else None
    native = record.get("native")
    activation = claim.get("nativeActivation")
    wid, engine = owner.get("workerId"), owner.get("engine")
    if (owner.get("schemaVersion") != _OWNER_SCHEMA
            or owner.get("status") != "worker" or owner.get("provenance") != "ccx-convert"
            or not isinstance(intent, dict) or not isinstance(cf, dict)
            or not isinstance(native, dict) or not isinstance(activation, dict)
            or engine not in ENGINES or record.get("workerId") != wid
            or record.get("engine") != engine
            or claim.get("workerId") != wid or claim.get("engine") != engine
            or owner.get("generation") != claim.get("generation")
            or claim.get("schemaVersion") != _CLAIM_SCHEMA
            or record.get("schemaVersion") != 2
            or any(doc.get("fingerprintVersion") != 2
                   for doc in (intent, claim, record))
            or intent.get("forkAnchor") is not None or claim.get("forkAnchor") is not None
            or record.get("forkAnchor") is not None
            or intent.get("activationStateAtReservation") != "converted"
            or activation.get("state") != "confirmed"
            or activation.get("submissionId") is not None
            or claim.get("activatedAtMs") is not None
            or record.get("terminalOnly") is not False
            or record.get("nativeCreate") is not None
            or record.get("spawnedBy") != intent.get("spawnedBy")
            or record.get("importedFrom") != cf.get("importedFrom")):
        raise StorageIntegrityError("converted owner/claim/record provenance differs")
    if (not isinstance(cf.get("legacyShort"), str)
            or not re.fullmatch(r"[0-9a-f]{8}", cf["legacyShort"])
            or not isinstance(cf.get("sourcePath"), str) or not cf["sourcePath"]
            or "\x00" in cf["sourcePath"]
            or any(not isinstance(cf.get(key), str)
                   or not _HASH.fullmatch(cf[key])
                   for key in ("sourceRecordSha256", "planSha256"))
            or any(cf.get(key) is not None
                   and (not isinstance(cf[key], str) or not _HASH.fullmatch(cf[key]))
                   for key in ("originalClaimSha256", "originalCreateSha256"))
            or (cf.get("importedFrom") is not None
                and not isinstance(cf.get("importedFrom"), dict))):
        raise StorageIntegrityError("converted frozen source/plan proof malformed")
    if cf.get("importedFrom") is None:
        if record.get("origin") == "ccx-import" or "importedFrom" in record:
            raise StorageIntegrityError("converted import history has no original proof")
    elif record.get("origin") != "ccx-import":
        raise StorageIntegrityError("converted import history lost original origin")
    nb = intent.get("nativeBinding")
    if not isinstance(nb, dict) or nb != claim.get("nativeBinding"):
        raise StorageIntegrityError("converted original native binding differs")
    if engine == "claude":
        sid = nb.get("sessionId")
        if (set(nb) != {"sessionId"} or not isinstance(sid, str) or not sid
                or cf.get("nativeProof") is not None
                or (cf.get("importedFrom") is not None
                    and cf["importedFrom"].get("sessionId") != sid)
                or native.get("sessionId") != sid
                or native.get("nativeState") != "session-bound"):
            raise StorageIntegrityError("converted Claude SID/proof differs")
        source = f"claude-session:{sid}"
    else:
        tid = nb.get("threadId")
        proof = cf.get("nativeProof")
        if (set(nb) != {"threadId"} or not isinstance(tid, str) or not tid
                or not isinstance(proof, dict)
                or set(proof) != {"kind", "threadId", "threadSource",
                                  "headerPath", "headerSha256"}
                or proof.get("kind") != "codex-session-meta"
                or proof.get("threadId") != tid
                or not isinstance(proof.get("headerPath"), str)
                or not proof["headerPath"] or "\x00" in proof["headerPath"]
                or not isinstance(proof.get("headerSha256"), str)
                or not _HASH.fullmatch(proof["headerSha256"])):
            raise StorageIntegrityError("converted Codex session-meta proof malformed")
        source = proof.get("threadSource")
        original = source.removeprefix("ccx-worker:") if isinstance(source, str) else ""
        if (not isinstance(source, str) or not source.startswith("ccx-worker:")
                or original[:8] != cf["legacyShort"]
                or (original != cf["legacyShort"]
                    and not _is_canonical_worker_uuid(original))
                or native.get("threadId") != tid
                or native.get("threadSource") != source
                or native.get("nativeState") != "thread-bound"):
            raise StorageIntegrityError("converted Codex original source tag differs")
    digest = converted_binding_digest(wid, engine, nb, cf)
    if (any(doc.get("createSha256") != digest
            for doc in (intent, claim, record))
            or activation.get("nativeEvidenceSha256") != digest
            or activation.get("nativeBinding") != nb
            or activation.get("source") != source):
        raise StorageIntegrityError("converted native binding digest/source differs")
    return source


def _is_canonical_worker_uuid(value: str) -> bool:
    try:
        canonical_worker_id(value)
        return True
    except InvalidTarget:
        return False


def fingerprint_v2(*, engine: str, cwd: str, create_args: Mapping,
                   fork_anchor: Mapping | None = None) -> str:
    """Immutable creation spec with a frozen fork anchor.

    An exact fork anchor must already be frozen by the caller. No display name
    or mutable lifecycle field enters the immutable preimage.
    """
    if engine not in ENGINES or not isinstance(cwd, str) or not cwd.startswith("/"):
        raise CreationConflict("create engine/cwd must be explicit and canonical")
    if not isinstance(create_args, Mapping):
        raise CreationConflict("createArgs must be a mapping")
    if fork_anchor is not None and not isinstance(fork_anchor, Mapping):
        raise CreationConflict("forkAnchor must be a frozen mapping")
    return _digest({
        "version": 2, "engine": engine, "cwd": cwd,
        "createArgs": dict(create_args),
        "forkAnchor": dict(fork_anchor) if fork_anchor is not None else None,
    })


class GenerationStore:
    """ACTIVE backend pinned to one sealed generation; never hot-switches."""

    def __init__(self, pointer: GenerationPointer):
        if pointer.phase != "ACTIVE":
            raise GenerationUnavailable("only ACTIVE generation is operational")
        # Validate again: caller must not forge a dataclass to bypass loader.
        current = inspect_generation(pointer.root)
        if current != pointer:
            raise StaleGeneration("stale or forged generation context")
        # Direct GenerationStore(pointer) must be equally fail-closed and
        # typed as require_ccx_active, not leak raw FileNotFoundError.
        try:
            with _open_dir(pointer.root, "identity") as identity:
                try:
                    lock_fd = os.open("activation.lock", _FILE_FLAGS, dir_fd=identity)
                except OSError as exc:
                    raise GenerationUnavailable("activation lock missing/unsafe; offline stage required") from exc
                try:
                    st = os.fstat(lock_fd)
                    named = os.stat("activation.lock", dir_fd=identity,
                                    follow_symlinks=False)
                    if ((st.st_dev, st.st_ino) != (named.st_dev, named.st_ino)
                            or not stat.S_ISREG(st.st_mode)
                            or st.st_uid != os.getuid() or st.st_mode & 0o077):
                        raise GenerationUnavailable("activation lock inode is not safe/owner-private")
                finally:
                    os.close(lock_fd)
        except FileNotFoundError as exc:
            raise GenerationUnavailable("activation lock not installed by offline stage") from exc
        self.pointer = pointer
        self.root = pointer.root
        self.generation = pointer.generation
        self._gen_parts = ("identity", "generations", self.generation)

    def revalidate(self) -> None:
        current = inspect_generation(self.root)
        if current != self.pointer:
            raise StaleGeneration("generation changed; refuse and restart on a new context")

    @contextmanager
    def lease(self, *, timeout: float = .5) -> Iterator[None]:
        """Stable shared activation lock for bounded local intent/dispatch only.

        DO NOT hold across native provider turn waits, --wait, streaming or
        arbitrary user callbacks. Every public store mutation uses this lease;
        a later cutover runner needs LOCK_EX on the same never-replaced inode.
        """
        self.revalidate()
        with _open_dir(self.root, "identity") as identity:
            with _flock_at(identity, "activation.lock", exclusive=False, timeout=timeout):
                self.revalidate()
                yield
                self.revalidate()

    @contextmanager
    def _gen(self, *parts: str, create: bool = False) -> Iterator[int]:
        with _open_dir(self.root, *self._gen_parts, *parts, create=create) as fd:
            yield fd

    @contextmanager
    def _claim_lock(self, worker_id: str) -> Iterator[None]:
        with self._gen("locks", "creation", create=True) as fd:
            with _flock_at(fd, worker_id + ".lock", exclusive=True, create=True):
                yield

    def _owner(self, worker_id: str) -> dict | None:
        try:
            with self._gen("owners") as fd:
                return _read_json(fd, worker_id + ".json")
        except FileNotFoundError:
            return None

    def _claim(self, worker_id: str) -> dict | None:
        try:
            with self._gen("claims") as fd:
                return _read_json(fd, worker_id + ".json")
        except FileNotFoundError:
            return None

    def _record(self, engine: str, worker_id: str) -> dict | None:
        try:
            with self._gen("workers", engine, worker_id) as fd:
                return _read_json(fd, "record.json")
        except FileNotFoundError:
            return None

    def _write(self, parts: tuple[str, ...], basename: str, data: Mapping) -> None:
        with self._gen(*parts, create=True) as fd:
            _write_json(fd, basename, data)

    def _owned(self, target: str, *, require_record: bool = True) -> tuple[WorkerOwner, dict | None]:
        parsed = parse_managed_target(target)
        record = self._owner(parsed.worker_id)
        if record is None:
            raise UnknownWorker("full UUID is not present in global owner index")
        if (record.get("schemaVersion") != _OWNER_SCHEMA
                or record.get("workerId") != parsed.worker_id
                or record.get("generation") != self.generation
                or record.get("engine") not in ENGINES):
            raise StorageIntegrityError("global full-ID owner record disagrees")
        if parsed.qualified_engine is not None and parsed.qualified_engine != record["engine"]:
            raise EngineMismatch(
                f"{parsed.worker_id} is a {record['engine'].capitalize()} worker; use "
                f"{record['engine']}:{parsed.worker_id} or the bare UUID")
        if record.get("status") != "worker":
            raise UnknownWorker("full UUID is reserved/quarantined, not an active worker")
        source = self._record(record["engine"], parsed.worker_id)
        if require_record and source is None:
            raise StorageIntegrityError("active owner has no matching full-ID worker record")
        if source is not None and (source.get("workerId") != parsed.worker_id
                                   or source.get("engine") != record["engine"]):
            raise StorageIntegrityError("record and unique global worker owner disagree")
        return WorkerOwner(parsed.worker_id, record["engine"]), source

    def resolve(self, target: str) -> WorkerOwner:
        with self.lease():
            owner, _rec = self._owned(target)
            return owner

    def list_workers(self, *, engine: str | None = None, cwd: str | None = None,
                     state: str | None = None, parent: str | None = None,
                     limit: int | None = None,
                     max_owners: int = 60000,
                     records: dict | None = None) -> list[dict]:
        """Pure durable managed-worker inventory, no native discovery/spawn.

        Every row is selected by its one global full-UUID owner file and a
        separate exact engine/full-ID record. No short, display name or native
        session/thread ID ever authorizes a row. Status is RECORDED, not a
        guess of host liveness; callers inspect live host status separately.
        """
        if engine is not None and engine not in ENGINES:
            raise StorageIntegrityError("unsupported inventory engine")
        if cwd is not None and not isinstance(cwd, str):
            raise StorageIntegrityError("invalid explicit cwd filter")
        if state is not None and (not isinstance(state, str) or not state):
            raise StorageIntegrityError("invalid lifecycle-state filter")
        if limit is not None and (type(limit) is not int or limit < 0):
            raise StorageIntegrityError("worker list limit must be nonnegative")
        if type(max_owners) is not int or not (0 < max_owners <= 1000000):
            raise StorageIntegrityError("invalid bounded owner scan limit")
        # Exact global owner, not a name, short, native ID or fork source.
        if parent is not None:
            with self.lease():
                parent_owner, _ = self._owned(parent)
                parent = parent_owner.worker
        rows: list[dict] = []
        # ONE shared lease and ONE pinned fd per directory for the whole bounded
        # scan. Per-row leases and root-anchored path walks cost ~30 open()
        # calls and three generation inspections per owner (seconds per list
        # on a 2.8k-owner fleet that the dashboard polls). Pure local reads.
        with self.lease(), ExitStack() as dirs:
            try:
                owners_fd = dirs.enter_context(self._gen("owners"))
            except FileNotFoundError:
                return []  # new empty generation has no owners directory yet
            with os.scandir(owners_fd) as iterator:
                names = sorted(entry.name for entry in iterator
                               if entry.name.endswith(".json"))
            if len(names) > max_owners:
                raise StorageIntegrityError("owner inventory exceeds explicit scan budget")
            engine_fds: dict[str, int] = {}
            for name in names:
                worker_id = canonical_worker_id(name[:-5])
                owner = _read_json(owners_fd, name)
                if (owner is None or owner.get("schemaVersion") != _OWNER_SCHEMA
                        or owner.get("workerId") != worker_id
                        or owner.get("generation") != self.generation
                        or owner.get("engine") not in ENGINES
                        or owner.get("status") not in ("worker", "indeterminate-reserved",
                                                       "quarantined")):
                    raise StorageIntegrityError("malformed global owner inventory entry")
                if owner["status"] != "worker":
                    continue  # known reservation, not an addressable worker
                if engine is not None and owner["engine"] != engine:
                    continue
                try:
                    if owner["engine"] not in engine_fds:
                        engine_fds[owner["engine"]] = dirs.enter_context(
                            self._gen("workers", owner["engine"]))
                    with _open_child_dir(engine_fds[owner["engine"]], worker_id) as wfd:
                        rec = _read_json(wfd, "record.json")
                except FileNotFoundError:
                    rec = None
                # Same owner/record agreement `_owned` enforces, on the fds above.
                if rec is None:
                    raise StorageIntegrityError("active owner has no matching full-ID worker record")
                if rec.get("workerId") != worker_id or rec.get("engine") != owner["engine"]:
                    raise StorageIntegrityError("record and unique global worker owner disagree")
                proven_owner = WorkerOwner(worker_id, owner["engine"])
                spawned = rec.get("spawnedBy")
                intent = owner.get("createIntent")
                if spawned != (intent.get("spawnedBy") if isinstance(intent, dict) else None):
                    raise StorageIntegrityError("unproven managed parent edge in worker record")
                if spawned is not None:
                    if (not isinstance(spawned, dict)
                            or spawned.get("kind") != "ccx-worker"
                            or spawned.get("engine") not in ENGINES):
                        raise StorageIntegrityError("unproven managed parent edge in worker record")
                    try:
                        canonical_worker_id(spawned.get("workerId"))
                    except InvalidTarget as exc:
                        raise StorageIntegrityError("invalid owned parent full UUID") from exc
                if parent is not None and (
                    spawned is None or f"{spawned['engine']}:{spawned['workerId']}" != parent
                ):
                    continue
                if cwd is not None and rec.get("cwd") != cwd:
                    continue
                lifecycle = rec.get("lifecycle")
                lc = lifecycle if isinstance(lifecycle, dict) else {}
                durable_state = lc.get("state")
                normalized_state = normalize_native_state(durable_state)
                if owner["engine"] == "claude" and normalized_state == "idle":
                    from ccx.store.worker_state import claude_recorded_delivery_busy
                    # Stream owner-matched receipts and stop at the first
                    # actual pending new turn. File age is not a safe proxy
                    # for receipt timestamps (wall clock can move backwards).
                    def eligible_receipts():
                        try:
                            with self._gen("semantic-deliveries", "claude", worker_id) as rfd:
                                with os.scandir(rfd) as entries:
                                    for entry in entries:
                                        if not entry.name.endswith(".json"):
                                            continue
                                        try:
                                            receipt = _read_json(rfd, entry.name)
                                        except StorageIntegrityError:
                                            continue
                                        if (isinstance(receipt, dict)
                                                and receipt.get("workerId") == worker_id
                                                and receipt.get("engine") == "claude"
                                                and receipt.get("generation") == self.generation):
                                            yield receipt
                        except FileNotFoundError:
                            return
                    if claude_recorded_delivery_busy(rec, eligible_receipts()):
                        normalized_state = "busy"
                if state is not None and state != normalized_state:
                    continue
                born = rec.get("createdAt")
                last = rec.get("lastTurnAt")
                bad_stamps = []
                if born is not None and (type(born) is not int or born < 0):
                    born = None
                    bad_stamps.append("createdAt")
                if last is not None and (type(last) is not int or last < 0):
                    last = None
                    bad_stamps.append("lastTurnAt")
                stats = rec.get("stats")
                stats = stats if isinstance(stats, dict) else {}
                args = rec.get("createArgs")
                args = args if isinstance(args, dict) else {}
                row = {
                    "worker": proven_owner.worker,
                    "worker_id": proven_owner.worker_id,
                    "engine": proven_owner.engine,
                    "short": proven_owner.short,
                    "name": rec.get("name"),
                    "cwd": rec.get("cwd"),
                    "spawned_by": spawned,
                    "state": normalized_state,
                    "state_source": "recorded",  # no live/native guesses
                    "native_state": durable_state,
                    "terminal_only": bool(rec.get("terminalOnly")),
                    "created_at": born,
                    "last_turn_at": last,
                    "turns": stats.get("turns"),
                    **({"input_tokens": stats.get("inputTokens"),
                        "output_tokens": stats.get("outputTokens")}
                       if proven_owner.engine == "codex" else {}),
                    "requested_model": args.get("model"),
                    "requested_effort": args.get("effort"),
                    "loaded": recorded_host_loaded(proven_owner.engine, lc),
                }
                if bad_stamps:
                    row["warning"] = ("invalid optional timestamp: "
                                      + ", ".join(bad_stamps) + "; treated as unknown")
                rows.append(row)
                if records is not None:
                    records[proven_owner.worker] = (proven_owner, rec)
        # Presentation order never grants authority: two same-prefix/full
        # distinct IDs remain separate independent rows, even if names match.
        rows.sort(key=lambda r: (
            -max(r.get("last_turn_at") or 0, r.get("created_at") or 0),
            r["engine"], r["worker_id"],
        ))
        prefixes: dict[str, int] = {}
        for row in rows:
            prefixes[row["short"]] = prefixes.get(row["short"], 0) + 1
        for row in rows:
            row["ambiguous_short"] = prefixes[row["short"]] > 1
        return rows[:limit] if limit is not None else rows

    def owner_ids(self, *, prefix: str = "") -> list[str]:
        """Sorted full UUIDs whose owner file name starts with ``prefix``.

        Names only: callers must still ``resolve`` each candidate, which
        validates the owner, its status and the worker record.
        """
        with self.lease():
            try:
                with self._gen("owners") as fd, os.scandir(fd) as iterator:
                    return sorted(entry.name[:-5] for entry in iterator
                                  if entry.name.endswith(".json")
                                  and entry.name.startswith(prefix))
            except FileNotFoundError:
                return []

    def record(self, target: str) -> dict:
        with self.lease():
            _owner, record = self._owned(target)
            assert record is not None
            return record

    def variables_info(self, target: str, name: str | None = None) -> dict:
        """Read full-ID durable variable bindings; no runtime Python/live-owner revival.

        Variable *stateId* is an independent original identity and remains
        unchanged when two managed full-UUID owners intentionally share state.
        Reads are fd-relative bounded snapshots of atomic state replacements.
        Writers serialize separately; readers never wait on their locks.
        """
        import keyword

        if name is not None and (
            not isinstance(name, str) or not name.isidentifier()
            or keyword.iskeyword(name)
        ):
            raise StorageIntegrityError("variable name is not a Python identifier")
        with self.lease():
            owner, _source = self._owned(target)
            root_result = {
                "worker": owner.worker, "worker_id": owner.worker_id,
                "engine": owner.engine, "short": owner.short,
                "mode": None, "state_id": None, "names": [],
                "available": False,
            }
            try:
                with self._gen("variables", owner.engine) as fd:
                    sidecar = _read_json(fd, owner.worker_id + ".json")
            except FileNotFoundError:
                sidecar = None
            if sidecar is None:
                if name is not None:
                    raise UnknownWorker(
                        f"worker {owner.worker} has no variable {name!r} (has: none)"
                    )
                return root_result
            if (sidecar.get("schemaVersion") != 2
                    or sidecar.get("engine") != owner.engine
                    or sidecar.get("workerId") != owner.worker_id):
                raise StorageIntegrityError("variable sidecar lacks full-ID owner provenance")
            mode = sidecar.get("mode")
            if mode not in ("durable", "live"):
                raise StorageIntegrityError("unknown worker variable binding mode")
            if mode == "live":
                names = sidecar.get("names")
                if not isinstance(names, list) or any(
                    not isinstance(item, str) or not item.isidentifier()
                    or keyword.iskeyword(item) for item in names
                ):
                    raise StorageIntegrityError("malformed live variable names")
                result = {**root_result, "mode": "live", "names": sorted(names)}
                if name is not None:
                    if name not in names:
                        raise UnknownWorker(
                            f"worker {owner.worker} has no variable {name!r} "
                            f"(has: {', '.join(sorted(names)) or 'none'})"
                        )
                    raise NativeActionDenied(
                        "live Python variable cannot be materialized; original "
                        "in-memory controller owner may be unavailable"
                    )
                return result
            state_id = sidecar.get("stateId")
            if (not isinstance(state_id, str) or len(state_id) != 32
                    or any(c not in "0123456789abcdef" for c in state_id)):
                raise StorageIntegrityError("durable variable stateId is invalid")
            try:
                with self._gen("variables", "states") as fd:
                    state = _read_json(fd, state_id + ".json")
            except FileNotFoundError as exc:
                raise StorageIntegrityError(
                    "full UUID durable variable binding references missing state"
                ) from exc
            if (state is None or state.get("schemaVersion") != 1
                    or state.get("stateId") != state_id
                    or not isinstance(state.get("names"), list)
                    or not isinstance(state.get("variables"), dict)):
                raise StorageIntegrityError("durable variable state not sealed/owned")
            variables = state["variables"]
            names = state["names"]
            if (len(set(names)) != len(names)
                    or not all(isinstance(item, str) and item.isidentifier()
                               and not keyword.iskeyword(item) for item in names)
                    or set(names) != set(variables)):
                raise StorageIntegrityError(
                    "durable variable names differ from original state entries"
                )
            # The worker sidecar is immutable during a concurrent state
            # access by design. If it did change across this read, refuse
            # rather than returning data belonging to an old/other binding.
            try:
                with self._gen("variables", owner.engine) as fd:
                    after = _read_json(fd, owner.worker_id + ".json")
            except FileNotFoundError:
                after = None
            if after != sidecar:
                raise StorageIntegrityError("worker variable binding changed while read")
            result = {
                **root_result,
                "mode": "durable", "state_id": state_id,
                "names": sorted(names), "available": True,
            }
            if name is not None:
                if name not in variables:
                    raise UnknownWorker(
                        f"worker {owner.worker} has no variable {name!r} "
                        f"(has: {', '.join(sorted(names)) or 'none'})"
                    )
                result["name"] = name
                result["value"] = variables[name]
            return result

    def read_worker_sidecar(self, target: str, filename: str) -> dict | None:
        """Descriptor-relative JSON sidecar read, never a returned pathname open.

        Host/client readers use this rather than opening an unpinned
        pathname returned by worker_path().
        """
        if filename not in ("record.json", "settings.json", "transcript.json",
                            "statusline.json", "compact.json",
                            "context-observation.json"):
            raise StorageIntegrityError("unsupported CCX-owned JSON worker sidecar")
        with self.lease():
            owner, _record = self._owned(target)
            with self._gen("workers", owner.engine, owner.worker_id) as dir_fd:
                return _read_json(dir_fd, filename)

    def worker_path(self, target: str, filename: str = "record.json") -> Path:
        if filename not in ("record.json", "settings.json", "transcript.json",
                            "statusline.json", "events.ndjson",
                            "evidence.jsonl", "compact.json", "context-observation.json"):
            raise StorageIntegrityError("unknown worker sidecar family")
        with self.lease():
            owner, _ = self._owned(target)
            return self.pointer.generation_root / "workers" / owner.engine / owner.worker_id / filename

    def receipt_path(self, target: str, family: str, token: str) -> Path:
        if family not in _FAMILIES or not isinstance(token, str) or not token or len(token) > 512 or "\x00" in token:
            raise StorageIntegrityError("invalid semantic operation family/token")
        with self.lease():
            owner, _rec = self._owned(target)
            return (self.pointer.generation_root / "receipts" / family / owner.engine
                    / owner.worker_id / (hashlib.sha256(token.encode("utf-8")).hexdigest() + ".json"))

    @staticmethod
    def _new_claim(worker_id: str, engine: str, generation: str, fingerprint: str,
                   native: dict, anchor: dict | None) -> dict:
        return {
            "schemaVersion": _CLAIM_SCHEMA, "generation": generation,
            "workerId": worker_id, "engine": engine,
            "createSha256": fingerprint, "fingerprintVersion": 2,
            "forkAnchor": anchor, "nativeBinding": native,
            "activatedAtMs": None,
            "nativeActivation": {
                "state": "unprepared", "source": None,
                "submissionId": None, "nativeEvidenceSha256": None,
            },
        }

    def _check_original_create_intent(self, old: dict, *, engine: str,
                                      worker_id: str, fingerprint: str,
                                      native: dict, anchor: dict | None,
                                      provenance: str | None = None,
                                      imported_from: Mapping | None = None) -> dict:
        if old.get("engine") != engine:
            raise OwnerConflict("full UUID globally belongs to a different engine")
        if (old.get("workerId") != worker_id or old.get("generation") != self.generation
                or old.get("schemaVersion") != _OWNER_SCHEMA):
            raise StorageIntegrityError("existing global owner identity/generation mismatch")
        intent = old.get("createIntent")
        if not isinstance(intent, dict):
            raise CreationIndeterminate("owner predates durable create intent; never infer native absence")
        origin = old.get("provenance")
        source = intent.get("importedFrom")
        if origin == "ccx-convert":
            if provenance is not None and provenance != origin:
                raise CreationConflict("converted UUID is not a fresh creation/import")
            if (intent.get("createSha256") != fingerprint
                    or intent.get("nativeBinding") != native
                    or intent.get("forkAnchor") != anchor):
                raise CreationConflict("converted immutable owner intent changed")
            claim = self._claim(worker_id)
            record = self._record(engine, worker_id)
            if not isinstance(claim, dict) or not isinstance(record, dict):
                raise StorageIntegrityError("converted worker has no attested claim/record")
            _converted_binding(old, claim, record)
            return intent
        if (origin not in {"ccx-new", "ccx-import"}
                or (provenance is not None and provenance != origin)
                or (origin == "ccx-import" and
                    (engine != "claude" or not isinstance(source, dict)
                     or source.get("sessionId") != native.get("sessionId")))
                or (origin == "ccx-new" and source is not None)
                or (provenance is not None and source !=
                    (dict(imported_from) if imported_from is not None else None))
                or intent.get("fingerprintVersion") != 2
                or intent.get("createSha256") != fingerprint
                or intent.get("nativeBinding") != native
                or intent.get("forkAnchor") != anchor):
            raise CreationConflict("same UUID has different original owner create specification")
        if intent.get("activationStateAtReservation") != "unprepared":
            raise CreationIndeterminate("owner does not prove native activation was unprepared")
        return intent

    def creation_binding(self, target: str, *, engine: str,
                         fingerprint: str, provenance: str | None = None,
                         imported_from: Mapping | None = None) -> dict | None:
        """Read immutable new-v2 owner intent, including an indeterminate owner.

        Omitted provenance observes the original owner; creation passes its explicit origin.
        A generated full ID must NOT remint its original Claude native session
        ID after a crash between owner intent and claim publication. Checking
        this owner intent is read-only and never authorizes a native restart.
        """
        from ccx.store.identity import parse_managed_target
        parsed = parse_managed_target(target)
        parsed.qualified(engine)
        with self.lease():
            old = self._owner(parsed.worker_id)
            if old is None:
                return None
            intent = old.get("createIntent")
            if not isinstance(intent, dict):
                raise CreationIndeterminate(
                    "existing worker identity lacks original v2 native binding proof"
                )
            native = intent.get("nativeBinding")
            anchor = intent.get("forkAnchor")
            if not isinstance(native, dict):
                raise CreationIndeterminate("owned create intent has no native binding")
            self._check_original_create_intent(
                old, engine=engine, worker_id=parsed.worker_id,
                fingerprint=fingerprint, native=native, anchor=anchor,
                provenance=provenance, imported_from=imported_from,
            )
            return dict(native)

    @staticmethod
    def _check_record_origin(owner: dict, record: Mapping) -> None:
        """First claimed source and recovered record must describe one origin."""
        intent = owner.get("createIntent") or {}
        if record.get("spawnedBy") != intent.get("spawnedBy"):
            raise CreationConflict("first record changed original managed caller")
        if owner.get("provenance") == "ccx-convert":
            cf = intent.get("convertedFrom") or {}
            if record.get("importedFrom") != cf.get("importedFrom"):
                raise CreationConflict("converted import history differs")
            if cf.get("importedFrom") is not None and record.get("origin") != "ccx-import":
                raise CreationConflict("converted import origin differs")
            return
        if owner.get("provenance") == "ccx-import":
            if (record.get("origin") != "ccx-import"
                    or record.get("importedFrom") != intent.get("importedFrom")
                    or (record.get("native") or {}).get("sessionId") !=
                       (intent.get("nativeBinding") or {}).get("sessionId")):
                raise CreationConflict("original imported source/record differs")
        elif record.get("origin") == "ccx-import" or "importedFrom" in record:
            raise CreationConflict("ordinary create contradicts import provenance")

    def recover_first_record(self, reservation: CreationReservation) -> bool:
        """Promote an exact previously fsynced first record after crash.

        A new record with changing timestamps cannot be reconstructed from
        mutable fields. Instead verify its immutable v2 spec/fork/native source
        against original claim+owner and promote the *existing* bytes only
        while nativeActivation is durably UNPREPARED.
        """
        wid = canonical_worker_id(reservation.worker_id)
        with self.lease():
            with self._claim_lock(wid):
                self.revalidate()
                old, claim = self._owner(wid), self._claim(wid)
                if not old or not claim:
                    raise CreationIndeterminate("original owner/claim unavailable")
                immutable = self._check_original_create_intent(
                    old, engine=reservation.engine, worker_id=wid,
                    fingerprint=reservation.create_sha256,
                    native=claim.get("nativeBinding"),
                    anchor=claim.get("forkAnchor"),
                )
                if claim.get("createSha256") != reservation.create_sha256:
                    raise CreationConflict("original immutable claim changed")
                act = claim.get("nativeActivation")
                if not isinstance(act, dict) or act.get("state") != "unprepared":
                    raise CreationIndeterminate(
                        "first record replay not safe after native activation intent"
                    )
                original = self._record(reservation.engine, wid)
                if original is None:
                    return False
                self._check_record_origin(old, original)
                if (original.get("workerId") != wid
                        or original.get("engine") != reservation.engine
                        or original.get("createSha256") != reservation.create_sha256
                        or original.get("fingerprintVersion") != 2
                        or not isinstance(original.get("createArgs"), dict)
                        or fingerprint_v2(
                            engine=reservation.engine, cwd=original.get("cwd"),
                            create_args=original["createArgs"],
                            fork_anchor=claim.get("forkAnchor"),
                        ) != reservation.create_sha256
                        or original.get("forkAnchor") != claim.get("forkAnchor")
                        or not isinstance(original.get("native"), dict)
                        or any(original["native"].get(k) != v for k, v
                               in claim.get("nativeBinding", {}).items())):
                    raise CreationIndeterminate(
                        "original first-record immutable full-UUID proof differs"
                    )
                if old.get("status") == "indeterminate-reserved":
                    updated = dict(old)
                    updated["status"] = "worker"
                    self._write(("owners",), wid + ".json", updated)
                elif old.get("status") != "worker":
                    raise CreationIndeterminate("original UUID remains quarantined")
                return True

    def _native_sid_owner(self, session_id: str) -> dict | None:
        """Current owner files, including pending/converted records; no index."""
        with self.lease():
            try:
                with self._gen("owners") as fd:
                    with os.scandir(fd) as entries:
                        names = sorted(e.name for e in entries if e.name.endswith(".json"))
            except FileNotFoundError:
                return None
            if len(names) > 60000:
                raise StorageIntegrityError("native owner census exceeds bounded scan")
            found = None
            for filename in names:
                wid = canonical_worker_id(filename[:-5])
                owner = self._owner(wid)
                if (not isinstance(owner, dict) or owner.get("workerId") != wid
                        or owner.get("generation") != self.generation
                        or owner.get("schemaVersion") != _OWNER_SCHEMA
                        or owner.get("engine") not in ENGINES):
                    raise StorageIntegrityError("native owner census contains invalid owner")
                if owner.get("status") not in {"worker", "indeterminate-reserved", "quarantined"}:
                    raise StorageIntegrityError("native owner has unsupported claim state")
                if owner["engine"] != "claude":
                    continue
                intent = owner.get("createIntent")
                if intent is not None and not isinstance(intent, dict):
                    raise StorageIntegrityError("invalid original Claude owner intent")
                binding = intent.get("nativeBinding") if isinstance(intent, dict) else None
                if isinstance(intent, dict) and not isinstance(binding, dict):
                    raise StorageIntegrityError("unverifiable owned Claude native identity")
                record = self._record("claude", wid)
                if record is not None and not isinstance(record.get("native"), dict):
                    raise StorageIntegrityError("invalid owned native Claude session binding")
                recorded = (record["native"].get("sessionId")
                            if isinstance(record, dict) else None)
                bound = binding.get("sessionId") if binding is not None else None
                if (record is not None and (record.get("workerId") != wid
                                            or record.get("engine") != "claude")):
                    raise StorageIntegrityError("native owner record disagrees")
                if bound is not None and recorded is not None and bound != recorded:
                    raise StorageIntegrityError("owner and worker bind different native sessions")
                if bound is None and recorded is None:
                    # An unprovable Claude owner is not evidence that its native
                    # session differs. Never silently permit another owner.
                    raise CreationIndeterminate("unproven historical Claude native owner")
                if (bound or recorded) != session_id:
                    continue
                if found is not None:
                    raise StorageIntegrityError("multiple owners of one native Claude session")
                claim = self._claim(wid)
                activation = claim.get("nativeActivation") if isinstance(claim, dict) else None
                proven = (owner.get("provenance") == "ccx-import"
                          and owner.get("status") == "worker"
                          and isinstance(intent, dict)
                          and isinstance(activation, dict)
                          and activation.get("state") == "confirmed"
                          and isinstance(record, dict)
                          and record.get("origin") == "ccx-import"
                          and (record.get("native") or {}).get("nativeState") == "session-bound"
                          and record.get("importedFrom") == intent.get("importedFrom"))
                found = {"worker": f"claude:{wid}", "worker_id": wid,
                         "provenance": owner.get("provenance"),
                         "confirmed": proven,
                         "activation_state": (activation or {}).get("state") or "unknown"}
            return found

    @staticmethod
    def _import_sid_lock_name(session_id: str) -> str:
        return str(uuid5(NAMESPACE_URL, "ccx-import:" + session_id))

    def import_native_owner(self, session_id: str) -> dict | None:
        """Read one original owner under the same admission lock as import."""
        with self._claim_lock(self._import_sid_lock_name(session_id)):
            return self._native_sid_owner(session_id)

    def reserve_import_creation(self, target: str, *, session_id: str,
                                fingerprint: str, imported_from: Mapping,
                                native_binding: Mapping) -> CreationReservation:
        """One atomically admitted SID→independent worker owner reservation."""
        with self._claim_lock(self._import_sid_lock_name(session_id)):
            claimed = self._native_sid_owner(session_id)
            wid = parse_managed_target(target).worker_id
            if claimed is not None and claimed["worker_id"] != wid:
                raise CreationConflict("native Claude session already owned by " + claimed["worker"])
            return self.reserve_creation(
                target, engine="claude", fingerprint=fingerprint,
                native_binding=native_binding, provenance="ccx-import",
                imported_from=imported_from)

    def reserve_creation(self, target: str, *, engine: str, fingerprint: str,
                         native_binding: Mapping | None = None,
                         fork_anchor: Mapping | None = None,
                         provenance: str = "ccx-new",
                         imported_from: Mapping | None = None,
                         spawned_by: Mapping | None = None,
                         _failpoint: Callable[[str], None] | None = None) -> CreationReservation:
        """Globally exact-ID creation fence. never performs native activation.

        An existing orphan, historical claimless worker or unproven/changed
        create preimage never triggers a second native start. A crash after
        owner publication but before claim requires explicit reconciliation.
        """
        parsed = parse_managed_target(target)
        if engine not in ENGINES:
            raise CreationConflict("invalid engine")
        parsed.qualified(engine)
        if not isinstance(fingerprint, str) or not _HASH.fullmatch(fingerprint):
            raise CreationConflict("invalid immutable create digest")
        if native_binding is not None and not isinstance(native_binding, Mapping):
            raise CreationConflict("native binding must be a mapping")
        if fork_anchor is not None and not isinstance(fork_anchor, Mapping):
            raise CreationConflict("fork anchor must be a mapping")
        native = dict(native_binding or {})
        anchor = dict(fork_anchor) if fork_anchor is not None else None
        if provenance not in {"ccx-new", "ccx-import"}:
            raise CreationConflict("unsupported CCX owner origin")
        frozen_import = dict(imported_from) if imported_from is not None else None
        if (provenance == "ccx-import" and
                (engine != "claude" or not isinstance(frozen_import, dict)
                 or frozen_import.get("sessionId") != native.get("sessionId"))):
            raise CreationConflict("import provenance/native session mismatch")
        if provenance == "ccx-new" and frozen_import is not None:
            raise CreationConflict("non-import claim cannot carry imported history")
        with self.lease():
            with self._claim_lock(parsed.worker_id):
                self.revalidate()
                old = self._owner(parsed.worker_id)
                # The first owner intent wins; later caller environment must
                # neither overwrite its edge nor add an authority requirement
                # to a compatible retry.
                if old is None and spawned_by is not None:
                    if (not isinstance(spawned_by, Mapping)
                            or set(spawned_by) != {"kind", "engine", "workerId"}
                            or spawned_by.get("kind") != "ccx-worker"
                            or spawned_by.get("engine") not in ENGINES):
                        raise CreationConflict("managed caller stamp is invalid")
                    caller, _ = self._owned(
                        f"{spawned_by['engine']}:{spawned_by['workerId']}"
                    )
                    spawned_by = {"kind": "ccx-worker", "engine": caller.engine,
                                  "workerId": caller.worker_id}
                claim = self._claim(parsed.worker_id)
                if old is not None:
                    self._check_original_create_intent(
                        old, engine=engine, worker_id=parsed.worker_id,
                        fingerprint=fingerprint, native=native, anchor=anchor,
                        provenance=provenance, imported_from=frozen_import,
                    )
                    if claim is None:
                        # same immutable intent may finish only the missing
                        # claim in preparation phase. No native dispatch can be
                        # reached without the claim AND a later durable
                        # prepared/submitted fence. A missing record for an
                        # already-active owner is never recreated here.
                        if old.get("status") != "indeterminate-reserved":
                            raise CreationIndeterminate("missing claim under committed owner")
                        if self._record(engine, parsed.worker_id) is not None:
                            raise CreationIndeterminate("record exists without claim; native result unknown")
                        claim = self._new_claim(
                            parsed.worker_id, engine, self.generation, fingerprint,
                            native, anchor,
                        )
                        self._write(("claims",), parsed.worker_id + ".json", claim)
                        if _failpoint:
                            _failpoint("after-claim")
                        return CreationReservation(parsed.worker_id, engine, fingerprint,
                                                   True, False, "unprepared",
                                                   spawned_by=old["createIntent"].get("spawnedBy"))
                    if (claim.get("engine") != engine
                            or claim.get("workerId") != parsed.worker_id
                            or claim.get("createSha256") != fingerprint
                            or claim.get("fingerprintVersion") != 2
                            or claim.get("nativeBinding") != native
                            or claim.get("forkAnchor") != anchor):
                        raise CreationConflict("same UUID has incompatible immutable create intent")
                    if claim.get("generation") != self.generation:
                        raise StorageIntegrityError("claim generation disagrees")
                    activation = claim.get("nativeActivation")
                    if not isinstance(activation, dict) or activation.get("state") not in (
                        "unprepared", "prepared", "submitted-unknown", "confirmed", "rejected", "local-refused",
                    ):
                        # A pre-fence record has NO evidence of never-started.
                        raise CreationIndeterminate("original activation fate is not durably known")
                    if claim.get("activatedAtMs") is not None and activation.get("state") != "confirmed":
                        raise CreationIndeterminate("old activation timestamp without matching proof")
                    existing_record = self._record(engine, parsed.worker_id)
                    if old.get("status") == "worker" and existing_record is None:
                        raise CreationIndeterminate("active claim record missing; never create another native worker")
                    if old.get("status") not in ("worker", "indeterminate-reserved"):
                        raise CreationIndeterminate("historical/quarantined reservation cannot recreate")
                    return CreationReservation(
                        parsed.worker_id, engine, fingerprint, True,
                        existing_record is not None, activation["state"],
                        activation.get("submissionId"),
                        old["createIntent"].get("spawnedBy"),
                    )
                if claim is not None:
                    raise CreationIndeterminate("orphan claim cannot recreate missing global owner")
                # Owner creates an immutable original v2 intent before the
                # claim. If this write crashes, the caller-supplied AND the
                # freshly generated UUID remain owned; no reminting.
                self._write(("owners",), parsed.worker_id + ".json", {
                    "schemaVersion": _OWNER_SCHEMA, "generation": self.generation,
                    "workerId": parsed.worker_id, "engine": engine,
                    "status": "indeterminate-reserved", "provenance": provenance,
                    "createIntent": {
                        "fingerprintVersion": 2, "createSha256": fingerprint,
                        "nativeBinding": native, "forkAnchor": anchor,
                        **({"spawnedBy": spawned_by} if spawned_by is not None else {}),
                        "activationStateAtReservation": "unprepared",
                        **({"importedFrom": frozen_import}
                           if frozen_import is not None else {}),
                    },
                })
                if _failpoint:
                    _failpoint("after-owner")
                self._write(
                    ("claims",), parsed.worker_id + ".json",
                    self._new_claim(parsed.worker_id, engine, self.generation,
                                    fingerprint, native, anchor),
                )
                if _failpoint:
                    _failpoint("after-claim")
                return CreationReservation(parsed.worker_id, engine, fingerprint,
                                           False, False, "unprepared",
                                           spawned_by=dict(spawned_by) if spawned_by is not None else None)

    def publish_first_record(self, reservation: CreationReservation,
                             record: Mapping, *, _failpoint: Callable[[str], None] | None = None) -> None:
        """Commit first record in full-ID space; no native session/thread start."""
        canonical_worker_id(reservation.worker_id)
        if record.get("workerId") != reservation.worker_id or record.get("engine") != reservation.engine:
            raise StorageIntegrityError("initial record owner/native binding mismatch")
        if not isinstance(record.get("native"), dict):
            raise StorageIntegrityError("first record needs separate native binding")
        with self.lease():
            with self._claim_lock(reservation.worker_id):
                self.revalidate()
                old, claim = self._owner(reservation.worker_id), self._claim(reservation.worker_id)
                if not old or not claim or (old.get("engine"), old.get("workerId"), old.get("generation")) != (
                    reservation.engine, reservation.worker_id, self.generation
                ):
                    raise CreationIndeterminate("missing original owner/create claim")
                if (claim.get("createSha256") != reservation.create_sha256
                        or claim.get("fingerprintVersion") != 2
                        or claim.get("workerId") != reservation.worker_id):
                    raise CreationConflict("initial record differs from original immutable claim")
                if claim.get("activatedAtMs") is not None:
                    raise CreationIndeterminate("original native activation may have happened")
                if not isinstance(record.get("native"), dict):
                    raise StorageIntegrityError("native binding requires separate typed identity")
                expected_binding = claim.get("nativeBinding")
                if not isinstance(expected_binding, dict) or any(
                    record["native"].get(k) != v for k, v in expected_binding.items()
                ):
                    raise CreationConflict("initial record contradicts immutable native binding intent")
                self._check_record_origin(old, record)
                original = self._record(reservation.engine, reservation.worker_id)
                if original is not None:
                    self._check_record_origin(old, original)
                if original is None:
                    self._write(("workers", reservation.engine, reservation.worker_id),
                                "record.json", dict(record))
                elif _digest(original) != _digest(dict(record)):
                    # Two processes can each inspect a missing record and
                    # construct different *mutable* name/createdAt fields
                    # before the first one atomically publishes. Different
                    # prepared documents must not overwrite one another,
                    # but a verified identical original immutable v2 spec,
                    # native source and fork-anchor can safely adopt the
                    # first published bytes under this UUID claim lock.
                    if (claim.get("nativeActivation", {}).get("state") != "unprepared"
                            or original.get("workerId") != reservation.worker_id
                            or original.get("engine") != reservation.engine
                            or original.get("fingerprintVersion") != 2
                            or original.get("createSha256") != reservation.create_sha256
                            or not isinstance(original.get("createArgs"), dict)
                            or fingerprint_v2(
                                engine=reservation.engine,
                                cwd=original.get("cwd"),
                                create_args=original["createArgs"],
                                fork_anchor=claim.get("forkAnchor"),
                            ) != reservation.create_sha256
                            or original.get("forkAnchor") != claim.get("forkAnchor")
                            or not isinstance(original.get("native"), dict)
                            or any(original["native"].get(k) != v for k, v
                                   in expected_binding.items())):
                        raise CreationConflict(
                            "existing first record lacks original immutable spec; no overwrite"
                        )
                    # Preserve the first writer's name, timestamp, current
                    # revision and any subsequent separate metadata patches.
                if _failpoint:
                    _failpoint("after-record")
                if old.get("status") == "worker":
                    return  # idempotent record-only retry
                if old.get("status") != "indeterminate-reserved":
                    raise CreationIndeterminate("non-recreatable historical owner state")
                promoted = dict(old)
                promoted["status"] = "worker"
                self._write(("owners",), reservation.worker_id + ".json", promoted)

    def native_activation(self, target: str) -> dict:
        """Read-only original claim activation fate; no synthetic native proof."""
        with self.lease():
            owner, _ = self._owned(target)
            claim = self._claim(owner.worker_id)
            if claim is None:
                raise CreationIndeterminate("historical claimless worker has no activation proof")
            activation = claim.get("nativeActivation")
            if not isinstance(activation, dict) or activation.get("state") not in (
                "unprepared", "prepared", "submitted-unknown", "confirmed", "rejected", "local-refused",
            ):
                raise CreationIndeterminate("activation state missing or unsupported")
            return dict(activation)

    def prepare_native_activation(self, reservation: CreationReservation, *,
                                  native_source: str) -> NativeActivation:
        """Durably bind one original native activation source, no provider call.

        Only a fully committed *first* record with positive
        pre-native provenance may prepare. A no-thread migrated Codex worker,
        historical claimless owner, or an already submitted activation cannot.
        """
        if (not isinstance(native_source, str) or not native_source
                or len(native_source) > 512 or "\x00" in native_source):
            raise CreationConflict("missing/invalid original native activation source")
        wid = canonical_worker_id(reservation.worker_id)
        with self.lease():
            with self._claim_lock(wid):
                old, claim = self._owner(wid), self._claim(wid)
                if not old or not claim or old.get("status") != "worker":
                    raise CreationIndeterminate("native prepare requires original committed first record")
                if old.get("provenance") not in {"ccx-new", "ccx-import"}:
                    raise CreationIndeterminate("conversion is not an original native start")
                self._check_original_create_intent(
                    old, engine=reservation.engine, worker_id=wid,
                    fingerprint=reservation.create_sha256,
                    native=claim.get("nativeBinding"), anchor=claim.get("forkAnchor"),
                )
                if claim.get("createSha256") != reservation.create_sha256:
                    raise CreationConflict("original create claim differs")
                record = self._record(reservation.engine, wid)
                if not isinstance(record, dict):
                    raise CreationIndeterminate("missing exact first worker record")
                self._check_record_origin(old, record)
                if record.get("terminalOnly") or record.get("nativeCreate"):
                    raise CreationIndeterminate("worker has unknown/previous native-start evidence")
                native = record.get("native")
                if not isinstance(native, dict) or native.get("nativeState") in (
                    "unbound-unknown", "submitted-or-unknown",
                ):
                    raise CreationIndeterminate("no positive first-create native binding")
                if reservation.engine == "codex":
                    if native.get("threadId") is not None:
                        raise CreationIndeterminate("Codex record has native thread before first activation")
                    if native_source != f"ccx-worker:{wid}":
                        raise CreationConflict("native Codex source must name full CCX UUID")
                else:
                    sid = native.get("sessionId")
                    if not isinstance(sid, str) or native_source != f"claude-session:{sid}":
                        raise CreationConflict("native Claude source must retain exact original sessionId")
                activation = claim.get("nativeActivation")
                if not isinstance(activation, dict):
                    raise CreationIndeterminate("original activation fence unavailable")
                state = activation.get("state")
                if state == "prepared" and activation.get("source") == native_source:
                    return NativeActivation(wid, reservation.engine, native_source,
                                            activation["submissionId"], state, self.generation)
                if state != "unprepared":
                    raise CreationIndeterminate("native activation already submitted/settled/ambiguous")
                token = str(uuid4())
                updated = dict(claim)
                updated["nativeActivation"] = {
                    "state": "prepared", "source": native_source,
                    "submissionId": token, "nativeEvidenceSha256": None,
                }
                self._write(("claims",), wid + ".json", updated)
                return NativeActivation(wid, reservation.engine, native_source, token,
                                        "prepared", self.generation)

    def native_activation_dispatch(self, prepared: NativeActivation, *,
                                   bounded_submit: Callable[[float], Mapping | None],
                                   dispatch_budget_s: float = 1.0,
                                   _failpoint: Callable[[str], None] | None = None) -> dict:
        """Exactly one original native submission under a *bounded* activation lease.

        The provided transport function MUST send one bounded native request
        and honor the supplied deadline; it must never wait for a model turn.
        This atomic lock+state+transport admission is mandatory: no separate
        unlocked submitted→provider gap where a cutover could flip generation.
        The submitted-unknown fence is fsynced before calling the transport.
        No retry of a lost reply. A completed positive response is reconciled
        with confirm_native_activation, not guessed from idle.
        """
        if not 0 < dispatch_budget_s <= 2.0:
            raise GenerationError("native dispatch must have a bounded deadline <= 2 seconds")
        if prepared.generation != self.generation:
            raise StaleGeneration("native activation prepared for a different generation")
        if prepared.state != "prepared":
            raise CreationIndeterminate("only the original prepared submission can dispatch")
        wid = canonical_worker_id(prepared.worker_id)
        # One absolute admission budget includes the generation lease, claim
        # fsync, IPC admission and the daemon's eventual app-server pipe write.
        deadline = time.monotonic() + dispatch_budget_s
        with self.lease(timeout=min(dispatch_budget_s, 2.0)):
            with self._claim_lock(wid):
                self.revalidate()
                owner, record = self._owned(f"{prepared.engine}:{wid}")
                claim = self._claim(wid)
                if claim is None:
                    raise CreationIndeterminate("original native activation claim missing")
                act = claim.get("nativeActivation")
                if (not isinstance(act, dict) or act.get("state") != "prepared"
                        or act.get("source") != prepared.source
                        or act.get("submissionId") != prepared.submission_id):
                    raise CreationIndeterminate("native activation already submitted or claimed by another caller")
                if _failpoint:
                    _failpoint("before-submitted")
                updated = dict(claim)
                updated["nativeActivation"] = {**act, "state": "submitted-unknown"}
                self._write(("claims",), wid + ".json", updated)
                if _failpoint:
                    _failpoint("after-submitted")
                self.revalidate()  # still inside activation lease and owner lock
                # The transport's timeout applies only to a SINGLE native IPC
                # dispatch. A late/lost ack cannot un-fence this submission.
                try:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise NativeEffectIndeterminate(
                            "original native admission expired after durable submission fence"
                        )
                    evidence = bounded_submit(remaining)
                except Exception as exc:
                    raise NativeEffectIndeterminate(
                        "native transport result unknown; reconcile original submission"
                    ) from exc
                if evidence is None or not isinstance(evidence, Mapping):
                    raise NativeEffectIndeterminate(
                        "no conclusive native acceptance evidence; original submission fenced"
                    )
                # No assertion of success until explicit positive original
                # native evidence + matching ID/source is checked.
                return dict(evidence)

    def confirm_native_activation(self, prepared: NativeActivation, *,
                                  native_evidence_sha256: str,
                                  native_binding: Mapping) -> NativeActivation:
        """Positive original native evidence only, never a retry of thread/start."""
        if (not isinstance(native_evidence_sha256, str)
                or not _HASH.fullmatch(native_evidence_sha256)
                or not isinstance(native_binding, Mapping)):
            raise CreationIndeterminate("native activation needs exact positive proof")
        if prepared.generation != self.generation:
            raise StaleGeneration("activation submitted under different generation")
        wid = canonical_worker_id(prepared.worker_id)
        with self.lease():
            with self._claim_lock(wid):
                owner, record = self._owned(f"{prepared.engine}:{wid}")
                claim = self._claim(wid)
                if not claim:
                    raise CreationIndeterminate("missing original native activation claim")
                act = claim.get("nativeActivation")
                if (not isinstance(act, dict)
                        or act.get("source") != prepared.source
                        or act.get("submissionId") != prepared.submission_id):
                    raise CreationIndeterminate("native submission identity mismatch")
                if act.get("state") == "confirmed":
                    if (act.get("nativeEvidenceSha256") == native_evidence_sha256
                            and act.get("nativeBinding") == dict(native_binding)):
                        return NativeActivation(wid, prepared.engine, prepared.source,
                                                prepared.submission_id, "confirmed", self.generation)
                    raise CreationConflict("native activation confirmation cannot change")
                if act.get("state") == "rejected":
                    raise CreationConflict("native activation is terminal rejected; cannot confirm")
                if act.get("state") == "local-refused":
                    raise CreationConflict("native activation is terminal local-refused; cannot confirm")
                if act.get("state") != "submitted-unknown":
                    raise CreationIndeterminate("native activation never submitted")
                if prepared.engine == "claude":
                    if native_binding.get("sessionId") != record["native"].get("sessionId"):
                        raise CreationConflict("Claude native session does not match original CCX source")
                elif not isinstance(native_binding.get("threadId"), str) or not native_binding.get("threadId"):
                    raise CreationConflict("Codex native confirmation requires exact threadId")
                result = dict(claim)
                result["nativeActivation"] = {
                    **act, "state": "confirmed",
                    "nativeEvidenceSha256": native_evidence_sha256,
                    "nativeBinding": dict(native_binding),
                }
                # Claim confirmation is immutable and monotonically records
                # first native activation; follow-up normal operations may
                # only proceed after a separate record binding by CAS.
                result["activatedAtMs"] = int(time.time() * 1000)
                self._write(("claims",), wid + ".json", result)
                return NativeActivation(wid, prepared.engine, prepared.source,
                                        prepared.submission_id, "confirmed", self.generation)

    def reconcile_codex_native_rejection(self, target: str) -> dict | None:
        """Seal/replay a definite original Codex provider rejection, offline.

        The existing generation-scoped one-shot daemon effect marker must have
        fsynced the *matched coded JSON-RPC error response* before a claim can
        become terminal `rejected`. This method never contacts the app-server,
        never unlocks a native create for the same UUID and never infers a
        provider rejection from missing/timeout/transport evidence. It is the
        original claim's only submitted-unknown -> rejected transition, not a
        second creation receipt/state machine.
        """
        from ccx.store.identity import EngineMismatch
        wid = canonical_worker_id(parse_managed_target(target).worker_id)
        with self.lease():
            with self._claim_lock(wid):
                owner, record = self._owned(target)
                if owner.engine != "codex":
                    raise EngineMismatch("native thread/start rejection requires Codex owner")
                claim = self._claim(wid)
                if not isinstance(claim, dict):
                    raise CreationIndeterminate("missing original Codex claim")
                anchor = record.get("forkAnchor")
                if anchor is not None:
                    lineage = record.get("forkedFrom")
                    if (not isinstance(anchor, dict) or anchor.get("engine") != "codex"
                            or anchor.get("cwd") != record.get("cwd")
                            or not isinstance(anchor.get("createArgs"), dict)
                            or fingerprint_v2(
                                engine="codex", cwd=anchor["cwd"],
                                create_args=anchor["createArgs"], fork_anchor=anchor,
                            ) != record.get("createSha256")
                            or not isinstance(lineage, dict)
                            or any(lineage.get(k) != anchor.get(k) for k in (
                                "engine", "sourceWorkerId", "sourceNativeThreadId",
                                "lastTurnId", "forkedAtMs"))):
                        raise CreationIndeterminate("Codex fork terminal original anchor mismatch")
                self._check_original_create_intent(
                    self._owner(wid), engine="codex", worker_id=wid,
                    fingerprint=record.get("createSha256"), native={}, anchor=anchor,
                )
                activation = claim.get("nativeActivation")
                source = f"ccx-worker:{wid}"
                # The first create is not yet natively submitted. There can
                # be no original provider rejection to recover here; leave
                # the existing one-shot prepare/admission path untouched.
                if (isinstance(activation, dict)
                        and activation.get("state") in (
                            "unprepared", "prepared", "local-refused",
                        )):
                    # Terminal local-refused is validated against its OWN
                    # durable marker by the native-start reconciler, never
                    # treated as a provider-side coded error.
                    return None
                if (not isinstance(activation, dict)
                        or activation.get("state") not in {
                            "submitted-unknown", "confirmed", "rejected",
                        }
                        or activation.get("source") != source
                        or not isinstance(activation.get("submissionId"), str)
                        or not activation["submissionId"]
                        or claim.get("workerId") != wid
                        or claim.get("engine") != "codex"
                        or claim.get("generation") != self.generation
                        or claim.get("fingerprintVersion") != 2
                        or claim.get("createSha256") != record.get("createSha256")
                        or claim.get("nativeBinding") != {}
                        or claim.get("forkAnchor") != anchor
                        or record.get("fingerprintVersion") != 2
                        or record.get("terminalOnly") or record.get("nativeCreate")
                        or not isinstance(record.get("native"), dict)
                        or (activation.get("state") != "confirmed" and (
                            record["native"].get("threadId") is not None
                            or record["native"].get("nativeState") != "not-started-proven"
                        ))):
                    raise CreationIndeterminate("original Codex submitted claim is not eligible")
                token = activation["submissionId"]
                key = hashlib.sha256(("activate\x00" + token).encode()).hexdigest()
                try:
                    with self._gen("runtime-metadata", "codex-native-effects", wid) as fd:
                        with _flock_at(fd, key + ".lock", exclusive=False,
                                       create=False, timeout=.5):
                            effect = _read_json(fd, key + ".json")
                except (FileNotFoundError, GenerationUnavailable):
                    if activation["state"] == "rejected":
                        raise StorageIntegrityError(
                            "sealed Codex rejection lacks its original one-shot effect"
                        )
                    return None
                if effect is None:
                    if activation["state"] == "rejected":
                        raise StorageIntegrityError("sealed Codex rejection marker missing")
                    return None
                expected = {
                    "workerId": wid, "engine": "codex",
                    "generation": self.generation, "kind": "activate",
                    "token": token, "state": "submitted-unknown",
                    "source": source, "createSha256": record["createSha256"],
                }
                if (not isinstance(effect, dict)
                        or any(effect.get(k) != v for k, v in expected.items())
                        or (
                            (effect.get("nativeMethod") != "thread/fork"
                             or effect.get("forkAnchor") != anchor)
                            if anchor is not None else
                            ("nativeMethod" in effect or "forkAnchor" in effect)
                        )
                        or not isinstance(effect.get("daemonInstanceId"), str)
                        or not effect["daemonInstanceId"]
                        or type(effect.get("daemonPid")) is not int
                        or effect["daemonPid"] <= 0
                        or type(effect.get("appServerPid")) is not int
                        or effect["appServerPid"] <= 0):
                    raise StorageIntegrityError(
                        "original Codex rejection effect identity/source/generation changed"
                    )
                rejection = effect.get("nativeRejected")
                if rejection is None:
                    if activation["state"] == "rejected":
                        raise StorageIntegrityError("sealed rejection missing provider error proof")
                    return None
                if (not isinstance(rejection, dict)
                        or effect.get("nativeCandidate") is not None
                        or effect.get("nativeNotSubmitted") is not None
                        or type(rejection.get("code")) is not int
                        or not isinstance(rejection.get("message"), str)
                        or len(rejection["message"].encode("utf-8")) > 8192
                        or type(rejection.get("requestId")) is not int
                        or rejection["requestId"] <= 0
                        or rejection.get("appServerPid") != effect["appServerPid"]
                        or rejection.get("nativeErrorSha256") != _digest({
                            "code": rejection["code"],
                            "message": rejection["message"],
                            "requestId": rejection["requestId"],
                            "appServerPid": rejection["appServerPid"],
                        })
                        or not isinstance(rejection.get("messageSha256"), str)
                        or rejection["messageSha256"] != hashlib.sha256(
                            rejection["message"].encode("utf-8")
                        ).hexdigest()
                        or not isinstance(rejection.get("nativeReplySha256"), str)
                        or not _HASH.fullmatch(rejection["nativeReplySha256"])):
                    raise StorageIntegrityError(
                        "Codex provider rejection marker malformed or contradictory"
                    )
                evidence = _digest({
                    "workerId": wid, "generation": self.generation,
                    "createSha256": record["createSha256"],
                    "source": source, "submissionId": token,
                    "nativeRejected": rejection,
                })
                if activation["state"] == "confirmed":
                    raise CreationConflict("confirmed native activation contradicts provider rejection")
                if activation["state"] == "rejected":
                    if (activation.get("nativeRejection") != rejection
                            or activation.get("nativeEvidenceSha256") != evidence):
                        raise CreationConflict("terminal native rejection proof cannot change")
                else:
                    newer = dict(claim)
                    newer["nativeActivation"] = {
                        **activation, "state": "rejected",
                        "nativeEvidenceSha256": evidence,
                        "nativeRejection": dict(rejection),
                    }
                    self._write(("claims",), wid + ".json", newer)
                return {
                    "rejected": True, "workerId": wid, "engine": "codex",
                    "source": source, "submissionId": token,
                    "code": rejection["code"], "message": rejection["message"],
                    "messageSha256": rejection["messageSha256"],
                    "nativeEvidenceSha256": evidence,
                }

    def reconcile_codex_local_refusal(self, target: str) -> dict | None:
        """Seal/replay only a durably proven local, pre-native Codex refusal.

        The original one-shot daemon effect marker must prove that this exact
        attempt never entered a potentially writing `thread/start`. This is
        LOCAL attribution, not a cryptographic signature or a claim that an
        absent marker proves non-submission. One terminal claim, no new start.
        """
        from ccx.store.identity import EngineMismatch
        wid = canonical_worker_id(parse_managed_target(target).worker_id)
        with self.lease():
            with self._claim_lock(wid):
                owner, record = self._owned(target)
                if owner.engine != "codex":
                    raise EngineMismatch("Codex local refusal requires original Codex owner")
                claim = self._claim(wid)
                if not isinstance(claim, dict):
                    raise CreationIndeterminate("original local refusal claim absent")
                anchor = record.get("forkAnchor")
                if anchor is not None:
                    lineage = record.get("forkedFrom")
                    if (not isinstance(anchor, dict) or anchor.get("engine") != "codex"
                            or anchor.get("cwd") != record.get("cwd")
                            or not isinstance(anchor.get("createArgs"), dict)
                            or fingerprint_v2(
                                engine="codex", cwd=anchor["cwd"],
                                create_args=anchor["createArgs"], fork_anchor=anchor,
                            ) != record.get("createSha256")
                            or not isinstance(lineage, dict)
                            or any(lineage.get(k) != anchor.get(k) for k in (
                                "engine", "sourceWorkerId", "sourceNativeThreadId",
                                "lastTurnId", "forkedAtMs"))):
                        raise CreationIndeterminate("Codex fork terminal original anchor mismatch")
                self._check_original_create_intent(
                    self._owner(wid), engine="codex", worker_id=wid,
                    fingerprint=record.get("createSha256"), native={}, anchor=anchor,
                )
                activation = claim.get("nativeActivation")
                if isinstance(activation, dict) and activation.get("state") in (
                    "unprepared", "prepared",
                ):
                    return None
                source = f"ccx-worker:{wid}"
                if (not isinstance(activation, dict)
                        or activation.get("state") not in {
                            "submitted-unknown", "confirmed", "rejected", "local-refused",
                        }
                        or activation.get("source") != source
                        or not isinstance(activation.get("submissionId"), str)
                        or not activation["submissionId"]
                        or claim.get("workerId") != wid
                        or claim.get("engine") != "codex"
                        or claim.get("generation") != self.generation
                        or claim.get("fingerprintVersion") != 2
                        or claim.get("createSha256") != record.get("createSha256")
                        or claim.get("nativeBinding") != {}
                        or claim.get("forkAnchor") != anchor
                        or record.get("fingerprintVersion") != 2
                        or record.get("terminalOnly") or record.get("nativeCreate")
                        or not isinstance(record.get("native"), dict)
                        or (activation.get("state") not in {"confirmed", "rejected"}
                            and (record["native"].get("threadId") is not None
                                 or record["native"].get("nativeState") != "not-started-proven"))):
                    raise CreationIndeterminate("original local refusal claim/record ineligible")
                token = activation["submissionId"]
                key = hashlib.sha256(("activate\x00" + token).encode()).hexdigest()
                try:
                    with self._gen("runtime-metadata", "codex-native-effects", wid) as fd:
                        with _flock_at(fd, key + ".lock", exclusive=False,
                                       create=False, timeout=.5):
                            effect = _read_json(fd, key + ".json")
                except FileNotFoundError:
                    effect = None
                if effect is None:
                    if activation["state"] == "local-refused":
                        raise StorageIntegrityError("terminal local refusal lacks original effect marker")
                    return None  # absent evidence never establishes not-submitted
                expected = {
                    "workerId": wid, "engine": "codex",
                    "generation": self.generation, "kind": "activate",
                    "token": token, "state": "submitted-unknown",
                    "source": source, "createSha256": record["createSha256"],
                }
                if (not isinstance(effect, dict)
                        or any(effect.get(k) != v for k, v in expected.items())
                        or (
                            (effect.get("nativeMethod") != "thread/fork"
                             or effect.get("forkAnchor") != anchor)
                            if anchor is not None else
                            ("nativeMethod" in effect or "forkAnchor" in effect)
                        )
                        or not isinstance(effect.get("daemonInstanceId"), str)
                        or not effect["daemonInstanceId"]
                        or type(effect.get("daemonPid")) is not int
                        or effect["daemonPid"] <= 0
                        or (effect.get("appServerPid") is not None
                            and (type(effect["appServerPid"]) is not int
                                 or effect["appServerPid"] <= 0))):
                    raise StorageIntegrityError("original local refusal marker identity changed")
                proof = effect.get("nativeNotSubmitted")
                if proof is None:
                    if activation["state"] == "local-refused":
                        raise StorageIntegrityError("terminal local refusal has no original proof")
                    return None
                if (not isinstance(proof, dict)
                        or effect.get("nativeCandidate") is not None
                        or effect.get("nativeRejected") is not None
                        or proof.get("stage") not in {
                            "capacity", "not-ready", "daemon-stopping",
                            "transport-zero-bytes", "shell-env",
                        }
                        or proof.get("nativeRequestWritten") is not False
                        or not isinstance(proof.get("reason"), str)
                        or not proof["reason"]
                        or len(proof["reason"].encode("utf-8")) > 1024
                        or proof.get("reasonSha256") != hashlib.sha256(
                            proof["reason"].encode("utf-8")
                        ).hexdigest()
                        or proof.get("appServerPid") != effect.get("appServerPid")
                        or proof.get("localEvidenceSha256") != _digest({
                            "workerId": wid, "generation": self.generation,
                            "createSha256": record["createSha256"],
                            "source": source, "submissionId": token,
                            "daemonInstanceId": effect["daemonInstanceId"],
                            "daemonPid": effect["daemonPid"],
                            "nativeNotSubmitted": {
                                "stage": proof["stage"], "reason": proof["reason"],
                                "reasonSha256": proof["reasonSha256"],
                                "nativeRequestWritten": False,
                                "appServerPid": proof["appServerPid"],
                            },
                        })):
                    raise StorageIntegrityError("local refusal proof malformed/contradictory")
                evidence = _digest({
                    "workerId": wid, "generation": self.generation,
                    "createSha256": record["createSha256"],
                    "source": source, "submissionId": token,
                    "nativeNotSubmitted": proof,
                })
                if activation["state"] in ("confirmed", "rejected"):
                    raise CreationConflict("provider outcome contradicts local non-submission")
                if activation["state"] == "local-refused":
                    if (activation.get("nativeNotSubmitted") != proof
                            or activation.get("nativeEvidenceSha256") != evidence):
                        raise CreationConflict("terminal local refusal proof cannot change")
                else:
                    update = dict(claim)
                    update["nativeActivation"] = {
                        **activation, "state": "local-refused",
                        "nativeEvidenceSha256": evidence,
                        "nativeNotSubmitted": dict(proof),
                    }
                    self._write(("claims",), wid + ".json", update)
                return {
                    "localRefused": True, "workerId": wid, "engine": "codex",
                    "source": source, "submissionId": token,
                    "reason": proof["reason"], "stage": proof["stage"],
                    "nativeEvidenceSha256": evidence,
                }

    def bind_confirmed_native(self, target: str) -> dict:
        """Publish exact positive native owner binding without stale record write.

        Native creation claim confirmation may have been fsynced just before a
        process crash. This separate under-lock reconciliation is idempotent:
        only the original confirmed session/thread/source may be installed in
        the immutable native record. Never invent a thread from missing IDs,
        and never bind an unconfirmed/unknown submission.
        """
        parsed = parse_managed_target(target)
        with self.lease():
            with self._claim_lock(parsed.worker_id):
                with self._record_lock(parsed.worker_id):
                    owner, record = self._owned(target)
                    claim = self._claim(parsed.worker_id)
                    activation = (claim.get("nativeActivation")
                                  if isinstance(claim, dict) else None)
                    if (not isinstance(activation, dict)
                            or activation.get("state") != "confirmed"
                            or not isinstance(activation.get("nativeEvidenceSha256"), str)
                            or not _HASH.fullmatch(activation["nativeEvidenceSha256"])):
                        raise CreationIndeterminate(
                            "positive immutable original native activation not confirmed"
                        )
                    expected = activation.get("nativeBinding")
                    if not isinstance(expected, dict):
                        raise CreationIndeterminate(
                            "confirmed native binding lacks original source proof"
                        )
                    original = record.get("native")
                    if not isinstance(original, dict):
                        raise StorageIntegrityError("worker has no separate native binding")
                    if owner.engine == "claude":
                        session_id = expected.get("sessionId")
                        if (not isinstance(session_id, str)
                                or original.get("sessionId") != session_id
                                or activation.get("source") != f"claude-session:{session_id}"):
                            raise CreationConflict("confirmed native Claude session differs")
                        name = "session-bound"
                        binding = {"sessionId": session_id}
                    else:
                        thread_id = expected.get("threadId")
                        if (not isinstance(thread_id, str) or not thread_id
                                or original.get("threadId") not in (None, thread_id)
                                or activation.get("source")
                                != f"ccx-worker:{owner.worker_id}"):
                            raise CreationConflict("confirmed native Codex source/thread differs")
                        name = "thread-bound"
                        binding = {"threadId": thread_id,
                                   "threadSource": activation["source"]}
                    # A formerly terminal-only/migrated worker never reaches
                    # this path: it has no nativeActivation originally created
                    # by the new-v2 owner and matching claim.
                    if record.get("terminalOnly"):
                        raise NativeActionDenied(
                            "terminal-only worker may not be rebound through creation"
                        )
                    newer = dict(record)
                    newer_native = dict(original)
                    newer_native.update(binding)
                    newer_native["nativeState"] = name
                    if newer_native == original:
                        return record
                    newer["native"] = newer_native
                    revision = newer.get("recordRevision", 0)
                    if type(revision) is not int or revision < 0:
                        raise StorageIntegrityError("bad record revision during native binding")
                    newer["recordRevision"] = revision + 1
                    self._write(
                        ("workers", owner.engine, owner.worker_id),
                        "record.json", newer,
                    )
                    return newer

    def authorize_native_action(self, target: str, action: str) -> WorkerOwner:
        """Read-only capability gate before any provider/client/daemon call.

        Direct worker methods and durable-intent paths must pass this gate;
        it does not start a provider.
        """
        with self.lease():
            owner, source = self._owned(target)
            assert source is not None
            self._native_guard(source, action)
            return owner

    @contextmanager
    def _record_lock(self, worker_id: str) -> Iterator[None]:
        """Per-full-UUID stable lock; only synchronous local RMW, no native waits."""
        with self._gen("locks", "records", create=True) as fd:
            with _flock_at(fd, worker_id + ".lock", exclusive=True, create=True):
                yield

    @staticmethod
    def _record_field_path(path: str) -> tuple[str, ...]:
        """Restrict semantic mutation to mutable CCX-owned fields.

        Never allow a generic stale record writer to modify immutable
        workerId/engine, native identity, original create/fork binding,
        receipt identity or recordRevision.
        """
        if not isinstance(path, str):
            raise StorageIntegrityError("record patch key must be a string")
        parts = tuple(path.split("."))
        if (len(parts) == 1 and parts[0] in (
                "name", "waitingFor", "activeCap", "lastTurnAt",
                "lastSettledAt", "lastSettledTurn",
                "backgroundTaskSummary",
                "status", "lastTurn", "lastReport", "activeTurnError",
                "pendingApprovals", "context", "activeCompact", "lastCompact",
                "compactCount", "evidence", "tokenUsage", "historyKnownEmpty",
                "nativeArchive", "resolvedModel", "parentNotifyFences",
                "lastTurnError", "pendingSend", "lastConversation", "nativeTurnStart",
                "quotaStop",
            )):
            return parts
        if (len(parts) == 2 and parts[0] in (
                "lifecycle", "createArgs", "stats"
            ) and parts[1].isidentifier() and not parts[1].startswith("_")):
            return parts
        raise StorageIntegrityError("immutable or unsupported worker record patch field")

    @staticmethod
    def _at_field(doc: Mapping, path: tuple[str, ...]) -> object:
        value: object = doc
        for part in path:
            if not isinstance(value, Mapping):
                return None
            value = value.get(part)
        return value

    @staticmethod
    def _set_field(doc: dict, path: tuple[str, ...], value: object) -> None:
        if len(path) == 1:
            doc[path[0]] = value
            return
        branch = doc.get(path[0])
        if branch is None:
            branch = {}
            doc[path[0]] = branch
        if not isinstance(branch, dict):
            raise StorageIntegrityError("record field parent is not an owned JSON object")
        branch[path[1]] = value

    _SNAPSHOT_MUTABLE_FIELDS = frozenset({
        # All are CCX-owned mutable projection fields. Immutable worker
        # identity/native binding/fork/creation evidence and provider-owned
        # transcript fields are excluded intentionally.
        "name", "status", "waitingFor", "updatedAt",
        "lastTurnAt", "lastSettledAt", "lastSettledTurn",
        "backgroundTaskSummary",
        "lastTurn", "lastReport", "activeTurnError",
        "lastTurnError", "pendingApprovals", "context",
        "activeCompact", "lastCompact", "compactCount",
        "lifecycle", "createArgs", "stats",
        "nativeArchive", "lastConversation", "pendingSend",
    })

    def replace_record_if_revision(
        self, target: str, record: Mapping, *,
        expected_revision: int,
    ) -> dict:
        """Exact full-ID fresh on-disk CAS for old host/codexd snapshot writers.

        Unlike locking an arbitrarily stale whole-record write, this refuses
        a snapshot whose explicit monotonic revision is no longer current.
        It never changes immutable provider/native/owner/fork identity, never
        deletes unseen fields, and preserves independent modifications by
        rejecting stale contenders instead of losing data. Prefer the
        semantic field-patch API for liveness/config; this CAS is
        for audited operations that truly update several related fields.
        """
        if (not isinstance(record, Mapping) or type(expected_revision) is not int
                or expected_revision < 0
                or type(record.get("recordRevision")) is not int
                or record["recordRevision"] != expected_revision):
            raise RecordConflict(
                "explicit current whole-record revision proof is required"
            )
        parsed = parse_managed_target(target)
        snapshot = json.loads(json.dumps(dict(record), allow_nan=False))
        with self.lease():
            # Invalid, foreign and quarantined IDs must not leave even an
            # otherwise harmless full-UUID lock file behind.
            self._owned(target)
            with self._record_lock(parsed.worker_id):
                owner, latest = self._owned(target)
                assert latest is not None
                if (snapshot.get("workerId") != owner.worker_id
                        or snapshot.get("engine") != owner.engine):
                    raise StorageIntegrityError("CAS snapshot belongs to foreign full worker")
                actual_revision = latest.get("recordRevision", 0)
                if type(actual_revision) is not int or actual_revision < 0:
                    raise StorageIntegrityError("durable worker revision malformed")
                if actual_revision != expected_revision:
                    raise RecordConflict(
                        "stale whole-record writer would overwrite another process"
                    )
                if not set(latest).issubset(snapshot):
                    raise StorageIntegrityError(
                        "CAS snapshot cannot silently remove existing record fields"
                    )
                modified = {
                    key for key in set(snapshot) | set(latest)
                    if snapshot.get(key) != latest.get(key)
                }
                if modified - self._SNAPSHOT_MUTABLE_FIELDS:
                    raise StorageIntegrityError(
                        "CAS snapshot cannot change native, owner, original "
                        "creation/fork binding or unknown record fields"
                    )
                for parent in ("lifecycle", "createArgs", "stats",
                               "nativeArchive"):
                    previous = latest.get(parent)
                    now = snapshot.get(parent)
                    if previous is not None:
                        if not isinstance(previous, dict) or not isinstance(now, dict):
                            raise StorageIntegrityError(
                                "CAS cannot replace a structured worker record field"
                            )
                        if not set(previous).issubset(now):
                            raise StorageIntegrityError(
                                "CAS cannot drop existing nested record fields"
                            )
                if not modified:
                    return latest
                snapshot["recordRevision"] = expected_revision + 1
                snapshot["updatedAt"] = int(time.time() * 1000)
                self._write(("workers", owner.engine, owner.worker_id),
                            "record.json", snapshot)
                return snapshot

    def patch_lifecycle(self, target: str, *, changes: Mapping) -> dict:
        """Fresh per-worker atomic lifecycle field patch (not stale snapshot)."""
        if not isinstance(changes, Mapping) or not changes:
            raise StorageIntegrityError("nonempty lifecycle changes required")
        if any(not isinstance(k, str) or not k.isidentifier()
               or k.startswith("_") for k in changes):
            raise StorageIntegrityError("invalid lifecycle patch key")
        patches = {f"lifecycle.{k}": value for k, value in changes.items()}
        if "state" in changes:
            patches["lifecycle.since"] = int(time.time() * 1000)
        return self.mutate_record(target, patches=patches)

    def rename_worker(self, target: str, *, engine: str, name: str | None) -> dict:
        """One canonical full-ID semantic rename for CLI and Python callers.

        Never accept a native/session display alias; no host restart and no
        existing stale full-record snapshot write. Only the name field is
        patched under the exact worker's per-UUID fresh-disk CAS lock.
        """
        from ccx.store.records import sanitize_name
        if engine not in ENGINES:
            raise StorageIntegrityError("unsupported engine for managed rename")
        owner = self.resolve(target)
        if owner.engine != engine:
            raise EngineMismatch("target worker belongs to a different engine")
        new_name = sanitize_name(name) or (
            ("ccx-" if engine == "claude" else "codex-") + owner.short
        )
        return self.mutate_record(owner.worker, patches={"name": new_name})

    def mutate_record(
        self, target: str, *, patches: Mapping[str, object] | None = None,
        increments: Mapping[str, int] | None = None,
        expect: Mapping[str, object] | None = None,
        expected_revision: int | None = None,
        removals: tuple[str, ...] = (),
    ) -> dict:
        """Fresh on-disk READ→semantic PATCH/CAS→fsync WRITE under owner lock.

        Never read an old in-memory whole record and then overwrite another
        process's independent changes. A field-specific expectation gives CAS
        without globally serializing unrelated config/lifecycle.
        Monotonic integer increments cannot lose updates. This method must be
        called by host, daemon and CLI writers to preserve field-level CAS.
        """
        changes = dict(patches or {})
        additions = dict(increments or {})
        comparisons = dict(expect or {})
        # Only the two Claude host-owned transient fields may be REMOVED. This
        # is not a general deletion mechanism for owner/claim/native evidence.
        if (not isinstance(removals, tuple)
                or any(type(field) is not str for field in removals)
                or len(removals) != len(set(removals))
                or set(removals) - {"activeCap", "waitingFor"}):
            raise StorageIntegrityError("only Claude host transient fields may be removed")
        if not changes and not additions and not removals:
            raise StorageIntegrityError("record semantic mutation requires field changes")
        if (set(changes) & (set(additions) | set(removals))
                or set(additions) & set(removals)):
            raise StorageIntegrityError("record semantic field mutated more than once")
        if len(changes) + len(additions) > 64:
            raise StorageIntegrityError("record patch exceeds bounded field count")
        for path in (*changes, *additions, *comparisons):
            self._record_field_path(path)
        if expected_revision is not None and (
                type(expected_revision) is not int or expected_revision < 0):
            raise StorageIntegrityError("invalid expected record revision")
        for path, amount in additions.items():
            if (not path.startswith("stats.")
                    or type(amount) is not int or amount < 0):
                raise StorageIntegrityError("record increments are nonnegative stats only")
        parsed = parse_managed_target(target)
        with self.lease():
            # Ownership validation precedes any incidental lock-file mkdir.
            # We repeat it after acquiring the stable per-full-UUID lock.
            self._owned(target)
            with self._record_lock(parsed.worker_id):
                # THIS fresh read under both locks, never a stale cached
                # record from before another CLI/host write.
                owner, current = self._owned(target)
                assert current is not None
                revision = current.get("recordRevision", 0)
                if type(revision) is not int or revision < 0:
                    raise StorageIntegrityError("bad worker recordRevision")
                if expected_revision is not None and revision != expected_revision:
                    raise RecordConflict("stale expected whole-record revision")
                for field, expected in comparisons.items():
                    if self._at_field(current, self._record_field_path(field)) != expected:
                        raise RecordConflict("record semantic field changed before CAS")
                # JSON roundtrip makes a detached copy and rejects objects not
                # serializable in the durable record (no arbitrary Python).
                candidate = json.loads(json.dumps(current, allow_nan=False))
                for field, value in changes.items():
                    self._set_field(candidate, self._record_field_path(field), value)
                for field, delta in additions.items():
                    path = self._record_field_path(field)
                    previous = self._at_field(candidate, path)
                    if type(previous) is not int:
                        raise RecordConflict("stat increment requires existing integer counter")
                    self._set_field(candidate, path, previous + delta)
                if removals:
                    if owner.engine != "claude":
                        raise StorageIntegrityError("host transient removal requires Claude owner")
                    for field in removals:
                        candidate.pop(field, None)
                if candidate == current and removals and not changes and not additions:
                    # Deleting an already-absent host transient is idempotent.
                    # Keep previous revision semantics for ordinary equal-value
                    # patch/increment callers outside this small host slice.
                    return current
                candidate["recordRevision"] = revision + 1
                # immutable worker identity/native fields always survive.
                for frozen in ("workerId", "engine", "native",
                               "nativeCreate", "terminalOnly", "forkedFrom", "spawnedBy"):
                    if candidate.get(frozen) != current.get(frozen):
                        raise StorageIntegrityError("immutable native/owner field changed")
                self._write(("workers", owner.engine, owner.worker_id), "record.json", candidate)
                return candidate

    def _native_guard(self, source: Mapping, action: str) -> None:
        if action not in _NATIVE_WRITES:
            raise NativeActionDenied("unknown native operation action")
        if action == "start":
            raise NativeActionDenied(
                "generic native thread/session start is forbidden;"
                " original create/activation proof must be reconciled separately"
            )
        native = source.get("native")
        if not isinstance(native, dict):
            raise NativeActionDenied("native binding not present; forensic read only")
        # New v2 workers may have a first record and *even a Claude native
        # sessionId* before a single native start was submitted. Neither
        # record existence nor a native UUID proves provider activation.
        # All native mutators must consult the durable original activation
        # receipt. Unknown/missing/temporary states never authorize send.
        if source.get("fingerprintVersion") != 2:
            raise NativeActionDenied("current full-UUID native binding requires v2")
        wid = canonical_worker_id(source.get("workerId"))
        owner, claim = self._owner(wid), self._claim(wid)
        activation = claim.get("nativeActivation") if isinstance(claim, dict) else None
        if (not isinstance(owner, dict) or not isinstance(claim, dict)
                or owner.get("status") != "worker"
                or owner.get("workerId") != wid
                or owner.get("engine") != source.get("engine")
                or owner.get("generation") != self.generation
                or claim.get("workerId") != wid
                or claim.get("engine") != source.get("engine")
                or claim.get("generation") != self.generation
                or claim.get("fingerprintVersion") != 2
                or not isinstance(activation, dict)
                or activation.get("state") != "confirmed"
                or not isinstance(activation.get("nativeEvidenceSha256"), str)
                or not _HASH.fullmatch(activation["nativeEvidenceSha256"])):
            raise NativeActionDenied("full-UUID native activation unconfirmed or unowned")
        if owner.get("provenance") == "ccx-convert":
            _converted_binding(owner, claim, source)
        elif owner.get("provenance") not in {"ccx-new", "ccx-import"}:
            raise NativeActionDenied("unsupported original native owner provenance")
        original_binding = activation.get("nativeBinding")
        if not isinstance(original_binding, dict):
            raise NativeActionDenied("confirmed claim has no original provider binding")
        if source["engine"] == "claude":
            if (native.get("sessionId") != original_binding.get("sessionId")
                    or native.get("nativeState") != "session-bound"
                    or activation.get("source")
                    != f"claude-session:{native.get('sessionId')}"):
                raise NativeActionDenied("Claude provider identity is not bound to original receipt")
        else:
            if (native.get("threadId") != original_binding.get("threadId")
                    or native.get("nativeState") != "thread-bound"
                    or native.get("threadSource") != activation.get("source")
                    or (owner.get("provenance") != "ccx-convert"
                        and activation.get("source") != f"ccx-worker:{wid}")):
                raise NativeActionDenied("Codex provider thread is not bound to original receipt")
        if source.get("terminalOnly") or native.get("nativeState") in (
            "unbound-unknown", "submitted-or-unknown",
        ):
            raise NativeActionDenied("terminal-only or pending native binding: never start/resume/send")
        if source.get("engine") == "codex" and not native.get("threadId"):
            raise NativeActionDenied("Codex has no proven native thread binding")
        if source.get("engine") == "claude" and not native.get("sessionId"):
            raise NativeActionDenied("Claude has no proven native session binding")
        if (isinstance(source.get("nativeCreate"), dict)
                and source["nativeCreate"].get("state") in ("submitted", "unknown")):
            raise NativeActionDenied("native creation pending original receipt/source reconciliation")

    def codex_source(self, target: str) -> str:
        """Privately prove original bound Codex source, never infer it from UUID."""
        with self.lease():
            owner, record = self._owned(target)
            if owner.engine != "codex" or record is None:
                raise NativeActionDenied("Codex full-UUID owner required")
            self._native_guard(record, "send")
            return self._claim(owner.worker_id)["nativeActivation"]["source"]

    def begin_native_intent(self, target: str, *, action: str, family: str,
                            operation_id: str, intent_sha256: str) -> OperationIntent:
        """Persist semantic operation *prepared* before any possible native call.

        A sealed accepted/rejected receipt is replayed as a public terminal
        outcome, NOT dispatched again. An unknown submitted operation refuses.
        An unsubmitted prepared intent may be reclaimed by same spec: replacing
        its random permit invalidates the original caller under the receipt lock.
        Only dispatch_native_intent_once can reach the native transport.
        """
        if (family not in _FAMILIES or not isinstance(operation_id, str)
                or not operation_id or len(operation_id) > 512 or "\\x00" in operation_id):
            raise StorageIntegrityError("invalid semantic operation identity")
        if not isinstance(intent_sha256, str) or not _HASH.fullmatch(intent_sha256):
            raise StorageIntegrityError("invalid operation intent digest")
        with self.lease():
            owner, source = self._owned(target)
            assert source is not None
            key = hashlib.sha256(operation_id.encode("utf-8")).hexdigest()
            parts = ("receipts", family, owner.engine, owner.worker_id)
            # Already terminal receipts are read-only replay evidence even
            # when the worker has since become terminalOnly/unbound. No native
            # capability is required to show an accepted/rejected original
            # outcome, and no receipt directory is created on that path.
            try:
                with self._gen(*parts) as read_fd:
                    prior_terminal = _read_json(read_fd, key + ".json")
            except FileNotFoundError:
                prior_terminal = None
            if prior_terminal is not None and prior_terminal.get("state") in ("accepted", "rejected"):
                if (prior_terminal.get("workerId") != owner.worker_id
                        or prior_terminal.get("engine") != owner.engine
                        or prior_terminal.get("family") != family
                        or prior_terminal.get("action") != action
                        or prior_terminal.get("operationId") != operation_id
                        or prior_terminal.get("intentSha256") != intent_sha256
                        or prior_terminal.get("generation") != self.generation):
                    raise CreationConflict("terminal semantic receipt identity mismatch")
                return OperationIntent(
                    owner.worker_id, owner.engine, family, operation_id,
                    intent_sha256, prior_terminal["state"], self.generation,
                    replayed=True,
                    native_evidence_sha256=prior_terminal.get("nativeEvidenceSha256"),
                )
            self._native_guard(source, action)
            with self._gen(*parts, create=True) as fd:
                with _flock_at(fd, key + ".lock", exclusive=True, create=True):
                    self.revalidate()
                    original = _read_json(fd, key + ".json")
                    if original is not None:
                        if (original.get("workerId") != owner.worker_id
                                or original.get("engine") != owner.engine
                                or original.get("family") != family
                                or original.get("action") != action
                                or original.get("operationId") != operation_id
                                or original.get("intentSha256") != intent_sha256
                                or original.get("generation") != self.generation):
                            raise CreationConflict("operation token already bound to different owner/intent")
                        state = original.get("state")
                        if state in ("accepted", "rejected"):
                            return OperationIntent(
                                owner.worker_id, owner.engine, family, operation_id,
                                intent_sha256, state, self.generation,
                                replayed=True,
                                native_evidence_sha256=original.get("nativeEvidenceSha256"),
                            )
                        if state != "prepared":
                            raise NativeEffectIndeterminate(
                                "submitted native operation has unknown result; no replay"
                            )
                    # Prepared means all new-code native dispatches are
                    # impossible without first durably recording submitted.
                    permit = uuid4().hex
                    pending = {
                        "schemaVersion": _RECEIPT_SCHEMA,
                        "workerId": owner.worker_id, "engine": owner.engine,
                        "generation": self.generation, "family": family,
                        "action": action, "operationId": operation_id,
                        "intentSha256": intent_sha256, "state": "prepared",
                        "dispatchPermitSha256": hashlib.sha256(
                            permit.encode("ascii")).hexdigest(),
                        "nativeEvidenceSha256": None,
                    }
                    _write_json(fd, key + ".json", pending)
                    return OperationIntent(
                        owner.worker_id, owner.engine, family, operation_id,
                        intent_sha256, "prepared", self.generation,
                        dispatch_token=permit,
                    )

    def dispatch_native_intent_once(self, prepared: OperationIntent, *,
                                    bounded_submit: Callable[[float], Mapping | None],
                                    dispatch_budget_s: float = 1.0,
                                    _failpoint: Callable[[str], None] | None = None) -> dict:
        """Same critical section rechecks ACTIVE + marks submitted before native IPC.

        The provided transport MUST honor the supplied single-request timeout,
        cannot wait for a provider turn, and cannot run arbitrary user Python.
        No pointer can flip under a legitimate runner's exclusive lock during
        bounded send; a pointer changed outside that protocol still triggers
        pre-dispatch refusal. Unknown results leave a durable replay fence.
        """
        if not 0 < dispatch_budget_s <= 2:
            raise GenerationError("native receipt dispatch must have deadline <= 2 seconds")
        if prepared.replayed or prepared.state != "prepared" or not prepared.dispatch_token:
            raise NativeEffectIndeterminate("terminal or unknown receipt cannot dispatch natively")
        if prepared.generation != self.generation:
            raise StaleGeneration("receipt prepared under different generation")
        with self.lease(timeout=min(dispatch_budget_s, 2)):
            owner, source = self._owned(f"{prepared.engine}:{prepared.worker_id}")
            assert source is not None
            with self._gen("receipts", prepared.family, owner.engine,
                           owner.worker_id) as fd:
                key = hashlib.sha256(prepared.operation_id.encode("utf-8")).hexdigest()
                with _flock_at(fd, key + ".lock", exclusive=True):
                    self.revalidate()
                    record = _read_json(fd, key + ".json")
                    if record is None:
                        raise StorageIntegrityError("missing original prepared receipt")
                    self._native_guard(source, record.get("action", ""))
                    if (record.get("workerId") != owner.worker_id
                            or record.get("engine") != prepared.engine
                            or record.get("family") != prepared.family
                            or record.get("operationId") != prepared.operation_id
                            or record.get("intentSha256") != prepared.intent_sha256
                            or record.get("generation") != self.generation):
                        raise StorageIntegrityError("native dispatch claim owner/intent mismatch")
                    if record.get("state") != "prepared":
                        raise NativeEffectIndeterminate("native dispatch already submitted or terminal")
                    expected = hashlib.sha256(
                        prepared.dispatch_token.encode("ascii")).hexdigest()
                    if record.get("dispatchPermitSha256") != expected:
                        raise NativeEffectIndeterminate("superseded receipt dispatch permit")
                    if _failpoint:
                        _failpoint("before-submitted")
                    self.revalidate()
                    unknown = dict(record)
                    unknown["state"] = "submitted-unknown"
                    unknown["dispatchPermitSha256"] = None
                    _write_json(fd, key + ".json", unknown)
                    if _failpoint:
                        _failpoint("after-submitted")
                    self.revalidate()
                    try:
                        answer = bounded_submit(dispatch_budget_s)
                    except Exception as exc:
                        raise NativeEffectIndeterminate(
                            "native receipt response unknown; do not replay"
                        ) from exc
                    if not isinstance(answer, Mapping):
                        raise NativeEffectIndeterminate(
                            "native receipt missing exact acceptance evidence"
                        )
                    return dict(answer)

    def settle_native_intent(self, attempt: OperationIntent, *, state: str,
                             native_evidence_sha256: str | None) -> dict:
        """Only terminal with durable positive evidence; changed pointer refuses.

        If the native acknowledgment was lost, leave submitted-unknown fenced
        and use a separately reviewed exact-native-history reconciliation.
        """
        if state not in ("accepted", "rejected", "unknown"):
            raise StorageIntegrityError("invalid native settlement")
        if state != "unknown" and (not isinstance(native_evidence_sha256, str)
                                   or not _HASH.fullmatch(native_evidence_sha256)):
            raise NativeEffectIndeterminate("cannot assert native terminal without exact evidence")
        if attempt.generation != self.generation:
            raise StaleGeneration("attempt from a different generation")
        with self.lease():
            owner, _source = self._owned(f"{attempt.engine}:{attempt.worker_id}")
            if owner.worker_id != attempt.worker_id:
                raise StorageIntegrityError("receipt owner changed")
            key = hashlib.sha256(attempt.operation_id.encode("utf-8")).hexdigest()
            with self._gen("receipts", attempt.family, attempt.engine, attempt.worker_id) as fd:
                with _flock_at(fd, key + ".lock", exclusive=True):
                    prior = _read_json(fd, key + ".json")
                    if not prior or (prior.get("operationId"), prior.get("intentSha256"),
                                     prior.get("workerId"), prior.get("generation")) != (
                        attempt.operation_id, attempt.intent_sha256,
                        attempt.worker_id, self.generation,
                    ):
                        raise StorageIntegrityError("missing/mismatched original receipt fence")
                    if prior.get("state") in ("accepted", "rejected"):
                        if (prior["state"], prior.get("nativeEvidenceSha256")) != (state, native_evidence_sha256):
                            raise CreationConflict("terminal native proof is immutable")
                        return prior
                    if state == "unknown":
                        return prior
                    if prior.get("state") != "submitted-unknown":
                        raise NativeEffectIndeterminate(
                            "cannot assert accepted/rejected before durable native dispatch"
                        )
                    result = dict(prior)
                    result.update(state=state, nativeEvidenceSha256=native_evidence_sha256)
                    _write_json(fd, key + ".json", result)
                    return result

    def read_receipt(self, target: str, family: str, operation_id: str) -> dict | None:
        path = self.receipt_path(target, family, operation_id)
        with self.lease():
            owner, _ = self._owned(target)
            key = path.name
            try:
                with self._gen("receipts", family, owner.engine, owner.worker_id) as fd:
                    value = _read_json(fd, key)
            except FileNotFoundError:
                return None
            if value is not None and (
                value.get("workerId") != owner.worker_id or value.get("engine") != owner.engine
                or value.get("operationId") != operation_id
                or value.get("generation") != self.generation
            ):
                raise StorageIntegrityError("semantic receipt identity mismatch")
            return value
