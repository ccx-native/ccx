"""Bounded AF_UNIX transport plans and exact root/generation/owner peer checks.

Socket filenames and hashes never select workers. This module validates private
paths, proves peer identity, and owns daemon lifetime leases; the actual staged
socket bind is performed by paths.bind_private_unix_socket.
"""
from __future__ import annotations

import fcntl
import hashlib
import os
import socket
import stat
from contextlib import contextmanager
from uuid import uuid4
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from ccx.system import oslayer
from ccx.store.store import GenerationStore
from ccx.store.root import GenerationError, GenerationUnavailable, StorageIntegrityError, _open_dir, _flock_at, _read_json, _write_json
from ccx.store.liveness import proc_start_token

_STAGE_SUFFIX = "/.XXXXXXXX/s"  # existing tempfile.mkdtemp(prefix=".", dir=parent)
_ALLOWED_ROLES = frozenset({"worker", "codexd", "notifyd"})


class TransportError(StorageIntegrityError):
    """Socket transport cannot securely bind or prove full owner identity."""


@dataclass(frozen=True)
class DaemonLease:
    """One process's generation/role singleton socket lifetime fence."""
    role: str
    generation: str
    nonce: str
    pid: int
    start_token: str


@dataclass(frozen=True)
class TransportPlan:
    root: Path
    generation: str
    parent: Path
    final: Path
    worker_id: str | None
    engine: str | None
    role: str
    fallback: bool

    def evidence(self) -> dict:
        return {
            "rootRealpath": str(self.root), "rootHash": hashlib.sha256(
                os.fsencode(self.root)).hexdigest(),
            "generation": self.generation, "workerId": self.worker_id,
            "engine": self.engine, "role": self.role,
        }


def _fits(path: Path) -> bool:
    return oslayer.fits_unix_path(path)


def _budget(parent: Path, basename: str) -> tuple[Path, Path]:
    final = parent / basename
    staged = Path(str(parent) + _STAGE_SUFFIX)
    return final, staged


def _trust_dir(path: Path, *, may_be_missing: bool = False) -> None:
    """Reject a symlink in any existing path component; writable base private."""
    absolute = Path(os.path.abspath(path))
    component = Path(absolute.anchor)
    for idx, item in enumerate(absolute.parts[1:]):
        component = component / item
        try:
            st = os.lstat(component)
        except FileNotFoundError:
            if may_be_missing and idx == len(absolute.parts[1:]) - 1:
                return
            raise TransportError("trusted short socket parent does not exist")
        if stat.S_ISLNK(st.st_mode) or not stat.S_ISDIR(st.st_mode):
            raise TransportError("socket root contains symlink/non-directory")
    st = os.lstat(absolute)
    if st.st_uid != os.getuid() or st.st_mode & 0o077:
        raise TransportError("transport directory must be user-owned private 0700")


def _default_runtime_dir(*, suffix_bytes: int = 0) -> oslayer.RuntimeChoice | None:
    """One read-only base choice on both Linux and Darwin."""
    return oslayer.runtime_base(purpose="transport", suffix_bytes=suffix_bytes)


def _transport_suffix(discriminator: str, basename: str) -> int:
    """Worst of the actual final and staged socket suffixes, including NUL."""
    parent = f"/ccx-uuid-{discriminator}"
    return max(len(os.fsencode(parent + "/" + basename)) + 1,
               len(os.fsencode(parent + _STAGE_SUFFIX)) + 1)


def _ensure_private_runtime_directory(
    path: Path, choice: oslayer.RuntimeChoice,
) -> None:
    """The shared checked-create path for transport and live-var sockets.

    An existing XDG base is never created here. A missing short per-UID base
    on Linux or Darwin is created only after the owner has confirmed ACTIVE; every
    write uses a no-follow pinned parent FD, then revalidates the created leaf.
    """
    base = choice.path
    if choice.may_create:
        if base.name != f"ccx-u{os.getuid()}":
            raise TransportError("untrusted short runtime creation candidate")
        with oslayer.open_root_dir(base.parent) as (_parent, fd):
            st = os.fstat(fd)
            if (not stat.S_ISDIR(st.st_mode) or st.st_uid != 0
                    or not st.st_mode & stat.S_ISVTX or not st.st_mode & 0o002):
                raise TransportError("short runtime parent is not trusted sticky root")
            try:
                os.mkdir(base.name, 0o700, dir_fd=fd)
                os.fsync(fd)
            except FileExistsError:
                pass
        _trust_dir(base)
    if path != base:
        if path.parent != base:
            raise TransportError("untrusted runtime child path")
        if choice.may_create:
            with oslayer.open_root_dir(base) as (_, fd):
                try:
                    os.mkdir(path.name, 0o700, dir_fd=fd)
                    os.fsync(fd)
                except FileExistsError:
                    pass
            _trust_dir(path)
        else:
            # Preserve the existing Linux per-UID live-vars mkdir + chmod path.
            path.mkdir(mode=0o700, parents=True, exist_ok=True)
    elif not choice.may_create:
        _trust_dir(base)


def plan_transport(store: GenerationStore, *, target: str | None = None,
                   role: str = "worker",
                   trusted_runtime: Path | str | None = None) -> TransportPlan:
    """No filesystem writes. Select shallow CCX_ROOT/run or private UID fallback.

    Socket basename for worker is the full 36-char worker UUID; a daemon role
    is a separately checked singleton, never a worker alias.
    """
    if role not in _ALLOWED_ROLES:
        raise TransportError("unknown CCX transport role")
    if role == "worker":
        if target is None:
            raise TransportError("worker socket requires an owned full UUID")
        owner = store.resolve(target)
        worker_id, engine = owner.worker_id, owner.engine
        basename = worker_id + ".sock"
    else:
        if target is not None:
            raise TransportError("daemon singleton cannot use worker target alias")
        store.revalidate()
        worker_id, engine = None, None
        basename = role + ".sock"
    primary = store.root / "run"
    final, stage = _budget(primary, basename)
    if _fits(final) and _fits(stage):
        _trust_dir(primary, may_be_missing=True)
        parent, fallback = primary, False
    else:
        discriminator = hashlib.sha256(os.fsencode(store.root)).hexdigest()[:16]
        suffix = _transport_suffix(discriminator, basename)
        if trusted_runtime is None:
            choice = _default_runtime_dir(suffix_bytes=suffix)
        else:
            choice = oslayer.RuntimeChoice(
                Path(os.path.abspath(os.fspath(trusted_runtime))))
        if choice is None:
            raise TransportError(
                f"CCX root {store.root} is too long for Unix socket paths (108 bytes); "
                "use a shorter CCX_ROOT or a private XDG_RUNTIME_DIR")
        runtime = choice.path
        _trust_dir(runtime, may_be_missing=choice.may_create)
        if not _fits(runtime / "x.sock"):
            raise TransportError("trusted runtime root too deep for pathname AF_UNIX")
        parent = runtime / ("ccx-uuid-" + discriminator)
        if not choice.may_create or os.path.lexists(runtime):
            _trust_dir(parent, may_be_missing=True)
        final, stage = _budget(parent, basename)
        fallback = True
        if not _fits(final) or not _fits(stage):
            raise TransportError("even trusted short runtime root exceeds socket budget")
    if store.pointer.generation_root in final.parents:
        raise TransportError("socket path would live under forbidden generation directory")
    return TransportPlan(store.root, store.generation, parent, final,
                         worker_id, engine, role, fallback)


def ensure_transport_parent(store: GenerationStore, plan: TransportPlan,
                            *, trusted_runtime: Path | str | None = None) -> None:
    """Create only post-ACTIVE + exact validated plan (never at bootstrap)."""
    store.revalidate()
    required = plan_transport(
        store, target=(f"{plan.engine}:{plan.worker_id}" if plan.role == "worker" else None),
        role=plan.role, trusted_runtime=trusted_runtime,
    )
    if required != plan:
        raise TransportError("transport root, generation or owner plan changed")
    with store.lease():
        if plan.fallback:
            # The original selector is read-only; the post-ACTIVE writer alone
            # creates a missing short UID leaf through a pinned FD on either OS.
            basename = plan.worker_id + ".sock" if plan.worker_id else plan.role + ".sock"
            discriminator = hashlib.sha256(os.fsencode(store.root)).hexdigest()[:16]
            if trusted_runtime is None:
                choice = _default_runtime_dir(
                    suffix_bytes=_transport_suffix(discriminator, basename))
            else:
                choice = oslayer.RuntimeChoice(
                    Path(os.path.abspath(os.fspath(trusted_runtime))))
            base = plan.parent.parent
            if choice is None or choice.path != base:
                raise TransportError("fallback runtime base changed before creation")
            _ensure_private_runtime_directory(base, choice)
            with _open_dir(base) as fd:
                name = plan.parent.name
                try:
                    os.mkdir(name, 0o700, dir_fd=fd)
                    os.fsync(fd)
                except FileExistsError:
                    pass
            _trust_dir(plan.parent)
        else:
            with _open_dir(store.root) as fd:
                try:
                    os.mkdir("run", 0o700, dir_fd=fd)
                    os.fsync(fd)
                except FileExistsError:
                    pass
            _trust_dir(plan.parent)


@contextmanager
def daemon_process_lease(store: GenerationStore, plan: TransportPlan):
    """Own a singleton daemon role across its actual socket lifetime.

    A stable role .lease.lock is held exclusively until the daemon exits;
    only short local writes take the shared activation lease. Native turn
    or subprocess waits must never run under the activation lock. A crash
    leaves the metadata but releases the role flock, so subsequent
    adoption refuses stale peer info even if PID has been recycled.
    """
    if plan.role not in ("codexd", "notifyd") or plan.worker_id is not None:
        raise TransportError("daemon lifetime lease needs a singleton role")
    if (plan.root, plan.generation) != (store.root, store.generation):
        raise TransportError("daemon lease targets a stale generation")
    if plan_transport(
        store, role=plan.role,
        trusted_runtime=plan.parent.parent if plan.fallback else None,
    ) != plan:
        raise TransportError("daemon lease plan is not its verified root/role transport")
    pid = os.getpid()
    start = proc_start_token(pid)
    if not start:
        raise TransportError("cannot bind daemon lease to positive /proc start time")
    nonce = uuid4().hex
    binding = DaemonLease(plan.role, store.generation, nonce, pid, str(start))
    # All inaugural directory and lock-file creation requires a *short*
    # activation lease; do not create dirs on a stale pointer then attempt
    # to revalidate only when writing metadata. The lifetime role lock is
    # acquired separately so no activation lease spans a daemon's lifetime.
    with store.lease():
        with store._gen("runtime-metadata", "pid", create=True) as fd:
            with _flock_at(fd, plan.role + ".lease.lock", exclusive=True,
                           create=True, timeout=.5):
                pass
    with store._gen("runtime-metadata", "pid") as fd:
        with _flock_at(fd, plan.role + ".lease.lock", exclusive=True,
                       timeout=.5):
            store.revalidate()
            with store.lease():
                _write_json(fd, plan.role + ".json", {
                    "schemaVersion": 2, "state": "active",
                    "role": plan.role, "rootRealpath": str(store.root),
                    "generation": store.generation, "pid": pid,
                    "startToken": str(start), "instanceNonce": nonce,
                })
            try:
                yield binding
            finally:
                # Release the role file lock even if the generation changed.
                # The next verifier checks lock occupancy and fresh /proc
                # start token, not just the lingering active JSON.
                try:
                    with store.lease(timeout=.1):
                        prior = _read_json(fd, plan.role + ".json")
                        if (prior and prior.get("instanceNonce") == nonce
                                and prior.get("pid") == pid
                                and prior.get("generation") == store.generation):
                            released = dict(prior)
                            released["state"] = "released"
                            _write_json(fd, plan.role + ".json", released)
                except (OSError, GenerationError):
                    pass


def handshake_payload(plan: TransportPlan, *,
                      daemon_nonce: str | None = None) -> dict:
    """Exact root/generation/owner payload; no native-ID routing aliases."""
    if plan.role in ("codexd", "notifyd"):
        if (not isinstance(daemon_nonce, str) or len(daemon_nonce) != 32
                or any(x not in "0123456789abcdef" for x in daemon_nonce)):
            raise TransportError("singleton handshake needs exact active daemon lease nonce")
        return {"protocolVersion": 2, **plan.evidence(),
                "daemonLeaseNonce": daemon_nonce}
    if daemon_nonce is not None:
        raise TransportError("worker socket cannot use daemon role lease")
    return {"protocolVersion": 2, **plan.evidence()}


def verify_handshake(store: GenerationStore, plan: TransportPlan, *,
                     received: Mapping, connected_socket: socket.socket,
                     peer_uid: int | None = None) -> None:
    """Exact Linux peer UID/PID/start plus global full owner or daemon lease."""
    if not isinstance(received, Mapping):
        raise TransportError("invalid socket handshake")
    store.revalidate()
    if (plan.root, plan.generation) != (store.root, store.generation):
        raise TransportError("socket plan root/generation does not match active owner store")
    expected_plan = plan_transport(
        store,
        target=(f"{plan.engine}:{plan.worker_id}" if plan.role == "worker" else None),
        role=plan.role,
        trusted_runtime=plan.parent.parent if plan.fallback else None,
    )
    if expected_plan != plan:
        raise TransportError("socket plan is not the verified full-ID/root/role address")
    expected = {"protocolVersion": 2, **plan.evidence()}
    if any(received.get(key) != val for key, val in expected.items()):
        raise TransportError("socket handshake root/generation/full owner mismatch")
    uid = os.getuid() if peer_uid is None else peer_uid
    try:
        credentials = oslayer.peer_credentials(connected_socket)
    except oslayer.PeerIdentityUnavailable as exc:
        raise TransportError("socket peer credentials unavailable") from exc
    pid, actual_uid = credentials.pid, credentials.uid
    if pid <= 0 or actual_uid != uid:
        raise TransportError("socket peer UID mismatch")
    start = proc_start_token(pid)
    if not start:
        raise TransportError("socket peer live start time could not be attested")
    if plan.role == "worker":
        if dict(received) != expected:
            raise TransportError("worker handshake has unexpected unowned fields")
        owner = store.resolve(f"{plan.engine}:{plan.worker_id}")
        if (owner.worker_id, owner.engine) != (plan.worker_id, plan.engine):
            raise TransportError("socket global full-ID worker owner mismatch")
        rec = store.record(owner.worker)
        lifecycle = rec.get("lifecycle")
        if (not isinstance(lifecycle, dict)
                or lifecycle.get("hostPid") != pid
                or str(lifecycle.get("hostStart")) != str(start)):
            raise TransportError("worker socket peer is not current PID/start-token owner")
        return
    if plan.role not in ("codexd", "notifyd"):
        raise TransportError("unknown singleton transport role")
    nonce = received.get("daemonLeaseNonce")
    if not isinstance(nonce, str) or len(nonce) != 32:
        raise TransportError("missing daemon lease nonce in peer handshake")
    if dict(received) != {**expected, "daemonLeaseNonce": nonce}:
        raise TransportError("unexpected daemon handshake identity field")
    try:
        with store._gen("runtime-metadata", "pid") as fd:
            metadata = _read_json(fd, plan.role + ".json")
            if (not metadata or metadata.get("schemaVersion") != 2
                    or metadata.get("state") != "active"
                    or metadata.get("role") != plan.role
                    or metadata.get("rootRealpath") != str(store.root)
                    or metadata.get("generation") != store.generation
                    or metadata.get("instanceNonce") != nonce
                    or metadata.get("pid") != pid
                    or str(metadata.get("startToken")) != str(start)):
                raise TransportError("daemon socket is not bound to current runtime PID/lease")
            # Positive proof another open file description holds the ROLE
            # flock exclusively. Merely having the metadata is insufficient:
            # if the daemon crashed its kernel lock is no longer held.
            guard = os.open(plan.role + ".lease.lock",
                            os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                            dir_fd=fd)
            try:
                st = os.fstat(guard)
                if (not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid()
                        or st.st_mode & 0o077):
                    raise TransportError("daemon singleton lease inode is unsafe")
                try:
                    fcntl.flock(guard, fcntl.LOCK_SH | fcntl.LOCK_NB)
                except BlockingIOError:
                    pass  # active exclusive lease held; required
                else:
                    fcntl.flock(guard, fcntl.LOCK_UN)
                    raise TransportError("daemon metadata is stale (no live role lease)")
            finally:
                os.close(guard)
    except FileNotFoundError as exc:
        raise TransportError("daemon socket lacks generation PID/lease proof") from exc
    store.revalidate()
