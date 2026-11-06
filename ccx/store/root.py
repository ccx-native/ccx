from __future__ import annotations
import errno
import fcntl
import hashlib
import json
import os
import re
import stat
import time
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Iterator, Mapping
from uuid import uuid4
from ccx.system import oslayer
from ccx.store.identity import (
    OwnerConflict,
)

if TYPE_CHECKING:
    from ccx.store.store import GenerationStore


def canonical_root(value: Path | str) -> Path:
    """Absolute canonical root spelling without following a symlink at its leaf.

    The descriptor-pinned generation walk owns the actual trust decision. This
    function only gives every caller (including offline migrations) one stable
    spelling, even for symlinked HOME and macOS /tmp ancestors.
    """
    text = os.path.abspath(os.path.expanduser(os.fspath(value)))
    parent, leaf = os.path.split(os.path.normpath(text))
    return Path(os.path.realpath(parent), leaf)


_GENERATION = re.compile(r"g-[0-9a-f]{32}\Z")


_HASH = re.compile(r"[0-9a-f]{64}\Z")


_FAMILIES = frozenset({"claude-delivery", "codex-send", "answer",
                       "cancel", "codex-compact", "compact", "steer"})


_NATIVE_WRITES = frozenset({"send", "resume", "start", "compact", "steer", "answer", "cancel"})


_POINTER_LAYOUT = 2


_OWNER_SCHEMA = 2


_CLAIM_SCHEMA = 2


_RECEIPT_SCHEMA = 2


_FILE_LIMIT = 4 * 1024 * 1024


_RUNTIME_SEAL_LIMIT = 64 * 1024  # never parse the full migration plan each request


_DIR_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC


_FILE_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC


class GenerationError(ValueError):
    """Unsafe, malformed or inaccessible full-UUID generation."""


class _StrictFileError(ValueError):
    """Unsafe or changing descriptor-pinned strict file."""


class GenerationUnavailable(GenerationError):
    """Absent, non-ACTIVE or otherwise unusable generation pointer."""


class StaleGeneration(GenerationError):
    """Pinned generation/root changed before an operation's effect."""


class GenerationBusy(GenerationError):
    """Stable lock unavailable within a bounded non-blocking attempt."""


class StorageIntegrityError(GenerationError):
    """Owned record/manifest/receipt shape or disk identity disagrees."""


class RecordConflict(GenerationError):
    """Fresh record version/field precondition failed under per-UUID CAS."""


class CreationConflict(OwnerConflict):
    """Caller reused a full UUID for a different immutable specification."""


class CreationIndeterminate(GenerationError):
    """Existing reservation/claim lacks proof for safe initial retry."""


class NativeActionDenied(GenerationError):
    """Unbound or unresolved worker cannot invoke a native operation."""


class NativeEffectIndeterminate(GenerationError):
    """Prior native submission may have reached provider; do not replay."""


@dataclass(frozen=True)
class GenerationPointer:
    root: Path
    generation: str
    manifest_sha256: str
    phase: str
    layout: int = _POINTER_LAYOUT

    @property
    def generation_root(self) -> Path:
        return self.root / "identity" / "generations" / self.generation


def _root_text(root: Path | str) -> Path:
    text = os.path.expanduser(os.fspath(root))
    if not os.path.isabs(text):
        raise GenerationError("explicit CCX root must be absolute")
    return canonical_root(text)


@contextmanager
def _root_dir_fd(path: Path):
    """Pin every explicit root component, preserving errors from with-body.

    Symlink/open errors in the *root traversal* are wrapped in _StrictFileError.
    Errors raised by the caller after yield retain their own domain/errno.
    """
    # Translate only errors from opening ancestors, not with-body OSErrors.
    with ExitStack() as stack:
        try:
            absolute, fd = stack.enter_context(oslayer.open_root_dir(path))
        except oslayer.RootOpenError as exc:
            raise _StrictFileError(str(exc)) from exc.__cause__
        yield absolute, fd


def _read_bounded_at(parent_fd: int, name: str, max_bytes: int) -> tuple[bytes, str]:
    """Read a regular child relative to pinned parent fd, no-follow at both ends."""
    try:
        before = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if not stat.S_ISREG(before.st_mode):
            raise _StrictFileError("not a regular inventory file")
        if before.st_size > max_bytes:
            raise _StrictFileError("file exceeds read-only inventory byte limit")
        fd = os.open(name, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW, dir_fd=parent_fd)
        try:
            inside = os.fstat(fd)
            fields = lambda st: (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns)
            if not stat.S_ISREG(inside.st_mode) or fields(before) != fields(inside):
                raise _StrictFileError("source file replaced during scan")
            parts: list[bytes] = []
            remaining = max_bytes + 1
            while remaining:
                block = os.read(fd, min(65536, remaining))
                if not block:
                    break
                parts.append(block)
                remaining -= len(block)
            data = b"".join(parts)
            if len(data) > max_bytes:
                raise _StrictFileError("file grew beyond inventory byte limit")
            after_fd = os.fstat(fd)
        finally:
            os.close(fd)
        after = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if fields(before) != fields(inside) or fields(inside) != fields(after_fd) or fields(after_fd) != fields(after):
            raise _StrictFileError("source file modified during scan")
        return data, hashlib.sha256(data).hexdigest()
    except OSError as exc:
        raise _StrictFileError("source file unavailable without following links") from exc


@contextmanager
def _open_child_dir(parent_fd: int, part: str) -> Iterator[int]:
    """One nofollow, owner-private child directory relative to a pinned fd."""
    if not part or part in (".", "..") or "/" in part or "\x00" in part:
        raise StorageIntegrityError("unsafe generation path component")
    fd = os.open(part, _DIR_FLAGS, dir_fd=parent_fd)
    try:
        st = os.fstat(fd)
        if st.st_uid != os.getuid() or st.st_mode & 0o077:
            raise StorageIntegrityError("generation directory must be owner-private")
        yield fd
    finally:
        os.close(fd)


@contextmanager
def _open_dir(root: Path, *parts: str, create: bool = False) -> Iterator[int]:
    """OS-owned file descriptor walk; each component is nofollow."""
    for part in parts:
        if not part or part in (".", "..") or "/" in part or "\x00" in part:
            raise StorageIntegrityError("unsafe generation path component")
    with _root_dir_fd(root) as (_pinned_root, root_fd):
        root_st = os.fstat(root_fd)
        if root_st.st_uid != os.getuid() or root_st.st_mode & 0o077:
            raise StorageIntegrityError("CCX generation root must be owned by UID and private")
        fds: list[int] = []
        fd = root_fd
        try:
            for part in parts:
                if create:
                    try:
                        os.mkdir(part, 0o700, dir_fd=fd)
                        os.fsync(fd)
                    except FileExistsError:
                        pass
                next_fd = os.open(part, _DIR_FLAGS, dir_fd=fd)
                st = os.fstat(next_fd)
                if st.st_uid != os.getuid() or st.st_mode & 0o077:
                    os.close(next_fd)
                    raise StorageIntegrityError("generation directory must be owner-private")
                fds.append(next_fd)
                fd = next_fd
            yield fd
        finally:
            for held in reversed(fds):
                os.close(held)


def _read_json(fd: int, name: str, *, max_bytes: int = _FILE_LIMIT) -> dict | None:
    """Read one consistent live-store inode, even during atomic replacement.

    The strict `_read_bounded_at` deliberately rejects a
    *pathname* changed mid-scan. Runtime writers instead fsync a new inode and
    atomically replace that pathname: an already-open old inode is a valid
    immutable snapshot. Verify only that same open fd remains internally
    consistent. Sealed-manifest reads also require the pathname to stay fixed.
    """
    try:
        # O_NONBLOCK prevents a substituted FIFO from blocking *before* fstat
        # can reject its nonregular file type. Never follow a symlink.
        child = os.open(name, _FILE_FLAGS | os.O_NONBLOCK, dir_fd=fd)
    except FileNotFoundError:
        return None
    except OSError as exc:
        # Keep the established security diagnostic for an O_NOFOLLOW symlink
        # or a changed non-directory path component. Never follow either.
        if exc.errno in (errno.ELOOP, errno.ENOTDIR):
            raise StorageIntegrityError(
                "owned JSON path must be owner-private regular file"
            ) from exc
        raise StorageIntegrityError("unreadable or unsafe owned JSON") from exc
    try:
        try:
            before = os.fstat(child)
            if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.getuid()
                    or before.st_mode & 0o077):
                raise StorageIntegrityError("owned JSON path must be owner-private regular file")
            if before.st_size < 0 or before.st_size > max_bytes:
                raise StorageIntegrityError("owned JSON exceeds bounded read size")
            parts: list[bytes] = []
            remaining = max_bytes + 1
            while remaining:
                block = os.read(child, min(65536, remaining))
                if not block:
                    break
                parts.append(block)
                remaining -= len(block)
            raw = b"".join(parts)
            after = os.fstat(child)
        except OSError as exc:
            raise StorageIntegrityError("unreadable or changing owned JSON") from exc
        # ctime and st_nlink can change when os.replace unlinks the OLD
        # still-open inode, without modifying its data. Do not compare them.
        fields = lambda st: (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns,
                             st.st_uid, st.st_mode)
        if (fields(before) != fields(after) or len(raw) != before.st_size
                or len(raw) > max_bytes):
            raise StorageIntegrityError("owned JSON inode changed during read")
    finally:
        try:
            os.close(child)
        except OSError as exc:
            raise StorageIntegrityError("could not close owned JSON descriptor") from exc
    try:
        val = json.loads(raw)
    except (UnicodeError, ValueError) as exc:
        raise StorageIntegrityError("malformed JSON in owned store") from exc
    if not isinstance(val, dict):
        raise StorageIntegrityError("owned store JSON must be an object")
    return val


def _write_json(fd: int, name: str, value: Mapping) -> None:
    """Fsync new 0600 inode, same-directory atomic replace and directory fsync.

    Every owned JSON file is compact and bounded to 4 MiB.
    """
    if not name or "/" in name or name in (".", ".."):
        raise StorageIntegrityError("unsafe owned output basename")
    raw = (json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8")
    if len(raw) > _FILE_LIMIT:
        raise StorageIntegrityError("owned JSON exceeds bounded writer size")
    name_tmp = ".new-" + uuid4().hex
    output_fd = os.open(
        name_tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
        0o600, dir_fd=fd,
    )
    try:
        with os.fdopen(output_fd, "wb") as out:
            out.write(raw)
            out.flush()
            os.fsync(out.fileno())
        # No symlink/unknown owner should be overwritten. Existing targets must
        # already be regular owner-private files on same pinned filesystem.
        try:
            prior = os.stat(name, dir_fd=fd, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            if (not stat.S_ISREG(prior.st_mode) or prior.st_uid != os.getuid()
                    or prior.st_mode & 0o077):
                raise StorageIntegrityError("refuse replacement of unsafe owned JSON target")
        os.replace(name_tmp, name, src_dir_fd=fd, dst_dir_fd=fd)
        os.fsync(fd)
    finally:
        try:
            os.unlink(name_tmp, dir_fd=fd)
        except FileNotFoundError:
            pass


@contextmanager
def _flock_at(fd: int, name: str, *, exclusive: bool, create: bool = False,
              timeout: float = 0.5) -> Iterator[None]:
    if timeout < 0 or timeout > 3:
        raise GenerationError("lock window must be bounded (<= 3 seconds)")
    flags = (_FILE_FLAGS | (os.O_RDWR | os.O_CREAT if create else 0))
    if create:
        flags &= ~os.O_RDONLY
    # On Monterey, concurrent openat(O_CREAT|O_NOFOLLOW) against the same
    # pinned directory can transiently return ENOENT while another process
    # creates the very same lock. This was reproduced on the test Mac with
    # unchanged parent st_dev/st_ino and 3 of 4 opens succeeding. Retry only
    # CREATING a lock, on that one errno, for a short bounded interval; a
    # missing PREEXISTING lock must still fail closed immediately.
    for attempt in range(6):
        try:
            lock_fd = os.open(name, flags, 0o600, dir_fd=fd)
            break
        except FileNotFoundError as exc:
            if not create:
                raise GenerationUnavailable("required preinstalled stable lock is missing") from exc
            if attempt == 5:
                raise GenerationUnavailable(
                    "stable lock creation failed on pinned owner directory"
                ) from exc
            time.sleep(.005)
    try:
        st = os.fstat(lock_fd)
        if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid() or st.st_mode & 0o077:
            raise StorageIntegrityError("activation/creation lock inode is not private regular file")
        deadline = time.monotonic() + timeout
        op = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
        while True:
            try:
                fcntl.flock(lock_fd, op | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise GenerationBusy("bounded generation or owner lock unavailable")
                time.sleep(min(.01, max(0, deadline - time.monotonic())))
        current = os.stat(name, dir_fd=fd, follow_symlinks=False)
        if (current.st_dev, current.st_ino) != (st.st_dev, st.st_ino):
            raise StorageIntegrityError("stable activation/creation lock inode was replaced")
        try:
            yield
        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
    finally:
        os.close(lock_fd)


@dataclass(frozen=True)
class RootStatus:
    """One read-only classification of a canonical CCX root."""
    root: Path
    kind: str  # missing | empty | initialized | unsafe
    pointer: GenerationPointer | None = None
    reason: str | None = None

    @property
    def generation(self) -> str | None:
        return self.pointer.generation if self.pointer else None

    @property
    def phase(self) -> str | None:
        return self.pointer.phase if self.pointer else None


_ROOT_REASONS = {
    "symlink-leaf": "root is a symlink; select a real, owner-private directory (do not follow the link)",
    "dangling-link": "root is a dangling symlink; replace it with a real, owner-private directory",
    "not-dir": "root is not a directory; choose a directory path",
    "foreign-owner": "root has a foreign owner; select a directory owned by this UID",
    "perms": "root must be owner-private (mode 0700); correct its permissions offline",
    "unreadable": "root or ancestor cannot be safely read; check directory traversal permissions and path components",
    "pointer-invalid": "generation pointer is missing, malformed or unsafe; inspect identity/current.json offline",
    "seal-mismatch": "sealed generation manifest is missing, unsafe or does not match its SHA seal; inspect offline",
    "nonempty-uninitialized": "root is nonempty without a valid generation pointer; inspect partial state offline, do not reinitialize",
}


def root_status_message(status: RootStatus) -> str:
    """One actionable wording table, independent of CLI/API error wrappers."""
    if status.kind in ("missing", "empty"):
        return (f"no CCX store at {status.root}; run any ccx command to create it, "
                "or ccx init")
    if status.kind == "unsafe":
        if status.reason and status.reason.startswith("spelling-alias:"):
            spelling = status.reason.removeprefix("spelling-alias:")
            return (f"CCX root {status.root} was initialized as {spelling}; "
                    "select it with that exact spelling")
        if status.reason == "perms":
            import shlex
            return (f"CCX root {status.root}: {_ROOT_REASONS['perms']}; "
                    f"if you own it: chmod 700 {shlex.quote(str(status.root))}")
        return f"CCX root {status.root}: {_ROOT_REASONS[status.reason]}"
    raise ValueError("initialized root has no unavailability message")


def _unsafe_root(root: Path, reason: str) -> RootStatus:
    return RootStatus(root, "unsafe", reason=reason)


def root_status(root: Path | str) -> RootStatus:
    """Inspect one root once: descriptor-pinned, no-follow, read-only and sealed.

    Never infer a safe root from Path.exists() or an exception's English text.
    Symlink leaves (including dangling links) are always refused; checking
    whether their targets exist would follow an untrusted path, so both use
    symlink-leaf rather than claiming to prove a dangling target.
    """
    base = _root_text(root)
    try:
        # Pin the canonical parent before lstat/openat of the untrusted LEAF.
        # Ancestor symlinks were normalized by canonical_root, not the leaf.
        with oslayer.open_root_dir(base.parent) as (_parent, parent_fd):
            name = base.name or "."
            try:
                before = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            except FileNotFoundError:
                return RootStatus(base, "missing")
            if stat.S_ISLNK(before.st_mode):
                return _unsafe_root(base, "symlink-leaf")
            if not stat.S_ISDIR(before.st_mode):
                return _unsafe_root(base, "not-dir")
            if before.st_uid != os.getuid():
                return _unsafe_root(base, "foreign-owner")
            if before.st_mode & 0o077:
                return _unsafe_root(base, "perms")
            try:
                root_fd = os.open(name, _DIR_FLAGS, dir_fd=parent_fd)
            except OSError:
                return _unsafe_root(base, "unreadable")
            try:
                pinned = os.fstat(root_fd)
                if ((pinned.st_dev, pinned.st_ino) != (before.st_dev, before.st_ino)
                        or pinned.st_uid != os.getuid() or pinned.st_mode & 0o077):
                    return _unsafe_root(base, "unreadable")
                try:
                    with _open_child_dir(root_fd, "identity") as identity:
                        try:
                            obj = _read_json(identity, "current.json")
                        except (StorageIntegrityError, OSError, ValueError):
                            return _unsafe_root(base, "pointer-invalid")
                        if obj is None:
                            return _unsafe_root(base, "nonempty-uninitialized")
                        if (not isinstance(obj, dict) or obj.get("layout") != _POINTER_LAYOUT
                                or obj.get("phase") != "ACTIVE"
                                or not isinstance(obj.get("rootRealpath"), str)):
                            return _unsafe_root(base, "pointer-invalid")
                        original_spelling = obj["rootRealpath"]
                        if original_spelling != str(base):
                            # Diagnostic-only inode test: refuse the alias even
                            # when both paths name the same pinned directory.
                            try:
                                alias_stat = os.stat(original_spelling)
                            except OSError:
                                return _unsafe_root(base, "pointer-invalid")
                            if (alias_stat.st_dev, alias_stat.st_ino) != (
                                pinned.st_dev, pinned.st_ino
                            ):
                                return _unsafe_root(base, "pointer-invalid")
                        generation = obj.get("generation")
                        checksum = obj.get("manifestSha256")
                        if (not isinstance(generation, str) or not _GENERATION.fullmatch(generation)
                                or not isinstance(checksum, str) or not _HASH.fullmatch(checksum)):
                            return _unsafe_root(base, "pointer-invalid")
                        try:
                            with _open_child_dir(identity, "generations") as generations:
                                with _open_child_dir(generations, generation) as gfd:
                                    raw, observed = _read_bounded_at(gfd, "manifest.json", _RUNTIME_SEAL_LIMIT)
                        except (OSError, StorageIntegrityError, ValueError):
                            return _unsafe_root(base, "seal-mismatch")
                except FileNotFoundError:
                    # Scandir only for roots without an identity directory;
                    # keep the hot initialized path to bounded pointer + seal.
                    try:
                        with os.scandir(root_fd) as entries:
                            is_empty = not any(True for _ in entries)
                    except OSError:
                        return _unsafe_root(base, "unreadable")
                    return RootStatus(base, "empty") if is_empty else _unsafe_root(base, "nonempty-uninitialized")
                except (OSError, StorageIntegrityError, ValueError):
                    return _unsafe_root(base, "pointer-invalid")
                if observed != checksum:
                    return _unsafe_root(base, "seal-mismatch")
                try:
                    manifest = json.loads(raw)
                except (UnicodeError, ValueError):
                    return _unsafe_root(base, "seal-mismatch")
                if (not isinstance(manifest, dict) or manifest.get("schemaVersion") != 1
                        or manifest.get("generation") != generation
                        or not isinstance(manifest.get("rootRealpath"), str)
                        or manifest.get("sealed") is not True):
                    return _unsafe_root(base, "seal-mismatch")
                if manifest["rootRealpath"] != original_spelling:
                    return _unsafe_root(base, "seal-mismatch")
                if original_spelling != str(base):
                    return _unsafe_root(base, "spelling-alias:" + original_spelling)
                return RootStatus(base, "initialized", GenerationPointer(
                    base, generation, checksum, obj["phase"]
                ))
            finally:
                os.close(root_fd)
    except oslayer.RootOpenError as exc:
        # oslayer deliberately preserves the original errno as __cause__.
        cause = exc.__cause__
        if isinstance(cause, OSError) and cause.errno == errno.ENOENT:
            return RootStatus(base, "missing")
        return _unsafe_root(base, "unreadable")
    except OSError:
        return _unsafe_root(base, "unreadable")


def inspect_generation(root: Path | str) -> GenerationPointer:
    """Read-only pointer and sealed manifest check."""
    status = root_status(root)
    if status.pointer is None:
        message = root_status_message(status)
        # Preserve the existing integrity-exception family for an insecure
        # root owner/mode, while sharing exactly the same diagnosis text.
        if status.reason in ("perms", "foreign-owner"):
            raise StorageIntegrityError(message)
        raise GenerationUnavailable(message)
    return status.pointer


def require_ccx_active(root: Path | str, *, expected_generation: str | None = None) -> "GenerationStore":
    """Validate and pin the active store without incidental writes."""
    pointer = inspect_generation(root)
    if expected_generation is not None and pointer.generation != expected_generation:
        raise StaleGeneration("expected generation no longer active")
    # Crucially never ensure_dirs or create activation.lock here. The
    # constructor performs the same check even if called directly.
    from ccx.store.store import GenerationStore
    return GenerationStore(pointer)


def create_missing_root_leaf(root: Path | str) -> tuple[int, int]:
    """Create only a missing root leaf using a pinned parent; never mkdir -p.

    Return the newly created inode identity for initialize_empty_root's initial
    descriptor check. Existing roots, links and replacement races fail closed.
    """
    base = _root_text(root)
    try:
        with oslayer.open_root_dir(base.parent) as (_parent, parent_fd):
            try:
                os.mkdir(base.name, 0o700, dir_fd=parent_fd)
            except FileNotFoundError:
                raise GenerationUnavailable(f"parent directory of {base} does not exist") from None
            except FileExistsError:
                raise GenerationUnavailable(root_status_message(_unsafe_root(base, "unreadable"))) from None
            os.fsync(parent_fd)
            fd = os.open(base.name, _DIR_FLAGS, dir_fd=parent_fd)
            try:
                st = os.fstat(fd)
                named = os.stat(base.name, dir_fd=parent_fd, follow_symlinks=False)
                if (not stat.S_ISDIR(st.st_mode) or st.st_uid != os.getuid()
                        or st.st_mode & 0o077
                        or (st.st_dev, st.st_ino) != (named.st_dev, named.st_ino)):
                    raise GenerationUnavailable(root_status_message(_unsafe_root(base, "unreadable")))
                return st.st_dev, st.st_ino
            finally:
                os.close(fd)
    except oslayer.RootOpenError as exc:
        if isinstance(exc.__cause__, OSError) and exc.__cause__.errno == errno.ENOENT:
            raise GenerationUnavailable(f"parent directory of {base} does not exist") from None
        raise GenerationUnavailable(root_status_message(_unsafe_root(base, "unreadable"))) from exc


def initialize_empty_root(root: Path | str, *,
                          generation: str | None = None,
                          _failpoint: Callable[[str], None] | None = None,
                          _expected_inode: tuple[int, int] | None = None) -> GenerationStore:
    """Explicit fresh install on an EXISTING EMPTY 0700 owned directory only.

    No implicit legacy CCX initialization, no provider calls, no migration.
    A process crash leaves a protected immutable marker/partial tree:
    a second initializer must NOT repair or adopt it automatically.
    Everything is written relative to the ONE held root inode, avoiding a
    re-opening by path after the initial empty-root eligibility check.
    """
    base = _root_text(root)
    generation = "g-" + uuid4().hex if generation is None else generation
    if not _GENERATION.fullmatch(generation):
        raise GenerationError("invalid fresh-install generation ID")
    try:
        with _root_dir_fd(base) as (_canonical, root_fd):
            st = os.fstat(root_fd)
            if _expected_inode is not None and (st.st_dev, st.st_ino) != _expected_inode:
                raise GenerationUnavailable(root_status_message(_unsafe_root(base, "unreadable")))
            if not stat.S_ISDIR(st.st_mode) or st.st_uid != os.getuid() or st.st_mode & 0o077:
                raise GenerationUnavailable("new installation root must exist, be owner-private 0700")
            with os.scandir(root_fd) as entries:
                if any(True for _ in entries):
                    raise GenerationUnavailable(root_status_message(_unsafe_root(base, "nonempty-uninitialized")))
            try:
                marker_fd = os.open(
                    ".ccx-init.lock",
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                    0o600, dir_fd=root_fd,
                )
            except FileExistsError as exc:
                raise GenerationUnavailable("another fresh initialization already claimed root") from exc
            try:
                os.fsync(marker_fd)
            finally:
                os.close(marker_fd)
            os.fsync(root_fd)
            if _failpoint:
                _failpoint("after-init-marker")
            os.mkdir("identity", 0o700, dir_fd=root_fd)
            os.fsync(root_fd)
            identity = os.open("identity", _DIR_FLAGS, dir_fd=root_fd)
            try:
                with os.fdopen(os.open(
                    "activation.lock",
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                    0o600, dir_fd=identity,
                ), "wb") as lock:
                    lock.flush()
                    os.fsync(lock.fileno())
                os.fsync(identity)
                if _failpoint:
                    _failpoint("after-activation-lock")
                os.mkdir("generations", 0o700, dir_fd=identity)
                os.fsync(identity)
                gens_fd = os.open("generations", _DIR_FLAGS, dir_fd=identity)
                try:
                    os.mkdir(generation, 0o700, dir_fd=gens_fd)
                    os.fsync(gens_fd)
                    gen_fd = os.open(generation, _DIR_FLAGS, dir_fd=gens_fd)
                    try:
                        _write_json(gen_fd, "manifest.json", {
                            "schemaVersion": 1,
                            "generation": generation,
                            "rootRealpath": str(base),
                            "sealed": True,
                            "newEmptyInstall": True,
                        })
                        _raw, seal_sha = _read_bounded_at(
                            gen_fd, "manifest.json", _RUNTIME_SEAL_LIMIT,
                        )
                    finally:
                        os.close(gen_fd)
                finally:
                    os.close(gens_fd)
                if _failpoint:
                    _failpoint("after-sealed-generation")
                _write_json(identity, "current.json", {
                    "layout": 2,
                    "generation": generation,
                    "manifestSha256": seal_sha,
                    "phase": "ACTIVE",
                    "rootRealpath": str(base),
                })
            finally:
                os.close(identity)
    except _StrictFileError as exc:
        raise GenerationUnavailable("explicit empty installation root has an unsafe symlink or component") from exc
    return require_ccx_active(base)


def open_or_initialize(root: Path | str) -> tuple["GenerationStore", bool]:
    """Open a sealed store, initializing only a missing or owner-private empty root.

    A concurrent initializer may own the exclusive init marker. Observe its
    result for at most five seconds; never repair or replace partial state.
    Daemons/hosts use require_ccx_active instead of this user-facing entry gate.
    """
    base = _root_text(root)

    def claimed() -> bool:
        # The marker is only evidence to *wait*, never evidence of a valid
        # generation. An ACTIVE pointer still has to pass require_ccx_active.
        try:
            with _root_dir_fd(base) as (_path, fd):
                marker = os.stat(".ccx-init.lock", dir_fd=fd, follow_symlinks=False)
                return (stat.S_ISREG(marker.st_mode) and marker.st_uid == os.getuid()
                        and marker.st_mode & 0o077 == 0)
        except (OSError, GenerationError, _StrictFileError):
            return False

    def join() -> tuple["GenerationStore", bool]:
        deadline = time.monotonic() + 5.0
        while True:
            current = root_status(base)
            if current.kind == "initialized":
                return require_ccx_active(base), False
            # Only a known transient init marker or the short mkdir/claim
            # gap can precede a winning initializer. No unsafe root is
            # reinitialized, modified or adopted without its sealed pointer.
            waiting = (current.kind == "empty"
                       or (current.kind == "unsafe"
                           and current.reason in {"nonempty-uninitialized", "pointer-invalid"}
                           and claimed()))
            if not waiting or time.monotonic() >= deadline:
                raise GenerationUnavailable(root_status_message(current))
            time.sleep(min(.025, max(0., deadline - time.monotonic())))

    status = root_status(base)
    if status.kind == "initialized":
        return require_ccx_active(base), False
    if status.kind == "unsafe":
        if (status.reason in {"nonempty-uninitialized", "pointer-invalid"}
                and claimed()):
            return join()
        message = root_status_message(status)
        if status.reason in ("perms", "foreign-owner"):
            raise StorageIntegrityError(message)
        raise GenerationUnavailable(message)

    inode = None
    if status.kind == "missing":
        try:
            inode = create_missing_root_leaf(base)
        except GenerationUnavailable:
            # Another initializer may have won mkdir between status and our
            # attempt. Do not wait for a genuinely missing parent or unsafe
            # replacement.
            current = root_status(base)
            if (current.kind == "initialized" or current.kind == "empty"
                    or (current.kind == "unsafe"
                        and current.reason in {"nonempty-uninitialized", "pointer-invalid"}
                        and claimed())):
                return join()
            raise
    try:
        return initialize_empty_root(base, _expected_inode=inode), True
    except GenerationUnavailable:
        current = root_status(base)
        if not (current.kind == "initialized" or current.kind == "empty"
                or (current.kind == "unsafe"
                    and current.reason in {"nonempty-uninitialized", "pointer-invalid"}
                    and claimed())):
            raise
        if inode is not None:
            # Another process can see the leaf we just mkdir'd and claim its
            # exclusive init marker first. Join ONLY that same original inode,
            # never a replaced root (even if the replacement has a good seal).
            try:
                with _root_dir_fd(base) as (_path, fd):
                    st = os.fstat(fd)
                    if (st.st_dev, st.st_ino) != inode:
                        raise GenerationUnavailable("new installation root inode changed")
            except _StrictFileError as exc:
                raise GenerationUnavailable("new installation root inode changed") from exc
        return join()
