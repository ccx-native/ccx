"""Internal Linux and Darwin process, filesystem and socket observations.

The boundary retains kernel /proc and kill(0) facts; callers make lifecycle,
identity and ownership decisions instead of treating PID presence as proof.
Only this module dispatches by OS. A Darwin fact that cannot be verified
remains unprovable; it never falls back to a Linux PID or pathname.
"""

from __future__ import annotations

from contextlib import contextmanager
import ctypes
from dataclasses import dataclass
from datetime import datetime, timezone
import errno
import fcntl
from functools import lru_cache
import os
from pathlib import Path
import re
import select
import signal
import socket
import stat
import struct
import sys
import tempfile
import threading
import time
from typing import Iterator, Literal


Presence = Literal["alive", "dead", "unprovable"]
PidProbe = Literal["exists", "missing", "denied"]


class UnsupportedPlatformError(RuntimeError):
    """No verified implementation of this OS fact is available."""


class PeerIdentityUnavailable(OSError):
    """The kernel did not provide usable credentials on this connection."""


class RootOpenError(OSError):
    """An explicit root ancestor or leaf could not be opened safely."""


if sys.platform not in ("linux", "darwin"):
    raise UnsupportedPlatformError(f"unsupported operating system: {sys.platform}")
UNIX_SUN_PATH_BYTES = 108 if sys.platform == "linux" else 104


def claude_project_cwd_is_nfc() -> bool:
    """Claude normalizes the native project cwd on Darwin, not on Linux."""
    return sys.platform == "darwin"


def direct_oauth_usage_supported() -> bool:
    """Direct credential-file usage is available only on Linux for now.

    Claude Code's own promptless get_usage path supports macOS separately.
    """
    return sys.platform == "linux"


def stdout_reader_gone(fd: int) -> bool:
    """Prove that a pipe's reader has closed, without masking other EPIPEs.

    Linux poll reports POLLERR on the abandoned write end. macOS has no
    select.poll; EVFILT_WRITE reports EV_EOF for the same abandoned pipe.
    A missing/inconclusive kernel event must not hide an unrelated socket
    error or alter the CLI's exit status.
    """
    try:
        if sys.platform == "linux":
            poller = select.poll()
            poller.register(fd, select.POLLOUT)
            return any(event & select.POLLERR for _, event in poller.poll(0))
        queue = select.kqueue()
        try:
            change = select.kevent(
                fd, filter=select.KQ_FILTER_WRITE,
                flags=select.KQ_EV_ADD | select.KQ_EV_ENABLE,
            )
            return any(
                event.flags & select.KQ_EV_EOF
                for event in queue.control([change], 1, 0)
            )
        finally:
            queue.close()
    except (AttributeError, OSError, ValueError):
        return False


@dataclass(frozen=True)
class ProcessInfo:
    pid: int | None
    presence: Presence
    ppid: int | None
    state: str | None
    start_token: str | None


@dataclass(frozen=True)
class PeerCredentials:
    uid: int
    pid: int
    gid: int | None


@dataclass(frozen=True)
class RuntimeChoice:
    """Darwin's read-only base selection; only callers may create a safe leaf."""

    path: Path
    may_create: bool = False


@contextmanager
def open_root_dir(path: Path) -> Iterator[tuple[Path, int]]:
    """Pin each root ancestor without following links, even if execute-only."""
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    path_flags = os.O_PATH | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    absolute = Path(os.path.abspath(os.fspath(path)))
    parts = absolute.parts[1:]

    def pin(name: str, mode: int, reason: str, **kwargs) -> int:
        try:
            return os.open(name, mode, **kwargs)
        except OSError as exc:
            raise RootOpenError(reason) from exc

    fd = pin(absolute.anchor, path_flags if parts else flags, "unsafe explicit root anchor")
    try:
        for index, part in enumerate(parts):
            next_fd = pin(part, flags if index == len(parts) - 1 else path_flags,
                          "unsafe explicit root component (symlink or non-directory)", dir_fd=fd)
            previous, fd = fd, next_fd
            os.close(previous)
        yield absolute, fd
    finally:
        os.close(fd)


def peer_credentials(conn: socket.socket) -> PeerCredentials:
    option = getattr(socket, "SO_PEERCRED", None)
    if option is None:
        raise PeerIdentityUnavailable("Linux SO_PEERCRED unavailable")
    try:
        raw = conn.getsockopt(socket.SOL_SOCKET, option, struct.calcsize("3i"))
        pid, uid, gid = struct.unpack("3i", raw)
    except (OSError, struct.error) as exc:
        raise PeerIdentityUnavailable(str(exc)) from exc
    return PeerCredentials(uid=uid, pid=pid, gid=gid)


def probe_pid(pid: object) -> PidProbe:
    try:
        os.kill(pid, 0)
        return "exists"
    except (ProcessLookupError, OverflowError, ValueError, TypeError):
        return "missing"
    except PermissionError:
        return "denied"


def _linux_parse_stat(pid: int, raw: bytes) -> ProcessInfo:
    try:
        # comm may contain spaces, ')' and non-UTF-8 bytes. Split after the
        # final ')' before interpreting the ASCII fields (including start #22).
        tail = raw.rsplit(b")", 1)[1].split()
        state = tail[0].decode("ascii")
    except (ValueError, IndexError, TypeError):
        return ProcessInfo(pid, "unprovable", None, None, None)
    def optional(index: int, convert):
        try:
            return convert(tail[index])
        except (ValueError, IndexError, TypeError):
            return None

    ppid = optional(1, int)  # field 4
    start = optional(19, lambda text: text.decode("ascii"))  # field 22 clock ticks
    dead = state in ("Z", "X")
    presence = (
        "dead" if dead else
        "alive" if start is not None else "unprovable"
    )
    return ProcessInfo(pid, presence, ppid, state, None if dead else start)


def process_info(pid: int | None) -> ProcessInfo:
    if not pid:
        return ProcessInfo(pid, "dead", None, None, None)
    try:
        raw = Path(f"/proc/{int(pid)}/stat").read_bytes()
    except (OSError, ValueError, TypeError) as exc:
        presence = "dead" if isinstance(exc, FileNotFoundError) else "unprovable"
        return ProcessInfo(pid, presence, None, None, None)
    return _linux_parse_stat(pid, raw)


def read_argv(pid: int | None) -> bytes | None:
    if not pid:
        return None
    try:
        return Path(f"/proc/{pid}/cmdline").read_bytes()
    except OSError:
        return None


PROC_LOCKS = "/proc/locks"


def _linux_held_flock_inodes() -> frozenset[tuple[int, int, int]] | None:
    """Every (major, minor, inode) with a granted flock, read without locking.

    None when the kernel table is unreadable. Never takes a lock itself: a
    probe lock could make a concurrent non-blocking lease acquisition fail.
    """
    try:
        with open(PROC_LOCKS, encoding="ascii", errors="replace") as stream:
            lines = stream.read().splitlines()
    except OSError:
        return None
    held = set()
    for line in lines:
        fields = line.split()
        # A granted lock: "<n>: FLOCK ADVISORY WRITE <pid> MAJ:MIN:INO 0 EOF".
        # A blocked waiter's line has "->" after the index and is skipped.
        if len(fields) < 6 or fields[1] != "FLOCK":
            continue
        parts = fields[5].split(":")
        if len(parts) != 3:
            continue
        try:
            held.add((int(parts[0], 16), int(parts[1], 16), int(parts[2])))
        except ValueError:
            continue
    return frozenset(held)


held_flock_inodes = _linux_held_flock_inodes


def _darwin_held_flock_inodes() -> None:
    """Darwin has no lock-free, dependency-free view of other processes'
    flocks (``lsof`` is an external spawn; ``F_GETLK`` sees POSIX locks only;
    a probe ``flock`` would contend). Lease presence is unobservable here."""
    return None


def fits_unix_path(path: Path | str) -> bool:
    return len(os.fsencode(path)) + 1 <= UNIX_SUN_PATH_BYTES


def pinned_fd_path(fd: int) -> str:
    """Reopen a still-held Linux file descriptor, never its mutable pathname."""
    return f"/proc/self/fd/{fd}"


def arm_parent_death(parent_pid: int) -> None:
    """Fail closed when a read-only child cannot arm Linux parent death."""
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        result = libc.prctl(1, signal.SIGKILL, 0, 0, 0)
    except (OSError, AttributeError, TypeError, ctypes.ArgumentError):
        os._exit(0)
    if result != 0 or os.getppid() != parent_pid:
        os._exit(0)


def runtime_base(
    *, purpose: Literal["transport", "live_vars"], suffix_bytes: int = 0,
) -> RuntimeChoice | None:
    """Choose an existing private XDG base without writing to the filesystem.

    An inherited XDG_RUNTIME_DIR may belong to a different user (for example
    when using su without -); transport refuses it, while live-vars can use
    the per-user directory under the system temporary base.
    """
    private = None
    text = os.environ.get("XDG_RUNTIME_DIR")
    if text and os.path.isabs(text):
        try:
            st = os.lstat(text)
        except OSError:
            pass
        else:
            if (stat.S_ISDIR(st.st_mode) and st.st_uid == os.getuid()
                    and not st.st_mode & 0o077):
                private = Path(text)
    if purpose == "transport":
        if (private is not None
                and len(os.fsencode(private)) + suffix_bytes <= UNIX_SUN_PATH_BYTES):
            return RuntimeChoice(private)
        return _short_private_runtime_base(Path("/tmp"), suffix_bytes)
    if purpose == "live_vars":
        return RuntimeChoice(private or Path(tempfile.gettempdir()))
    raise ValueError(f"unsupported runtime purpose: {purpose}")


def _short_private_runtime_base(parent: Path, suffix_bytes: int) -> RuntimeChoice:
    """Guard the OS's short sticky tmp ancestor and owner-only UID leaf.

    Selection never creates anything. Transport/live-vars create a missing leaf
    later, under an ACTIVE generation and a no-follow parent descriptor.
    """
    st = os.lstat(parent)
    if (not stat.S_ISDIR(st.st_mode) or st.st_uid != 0
            or not st.st_mode & stat.S_ISVTX or not st.st_mode & 0o002):
        raise OSError(f"short runtime ancestor must be root-owned sticky {parent}")
    base = parent / f"ccx-u{os.getuid()}"
    if len(os.fsencode(base)) + suffix_bytes > UNIX_SUN_PATH_BYTES:
        raise OSError("private runtime pathname exceeds AF_UNIX budget")
    try:
        existing = os.lstat(base)
    except FileNotFoundError:
        return RuntimeChoice(base, may_create=True)
    if (not stat.S_ISDIR(existing.st_mode) or existing.st_uid != os.getuid()
            or existing.st_mode & 0o077):
        raise OSError("short runtime base is not private and user-owned")
    return RuntimeChoice(base)


_ENGLISH_WEEKDAYS = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
_ENGLISH_MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun",
                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
_DARWIN_LSTART = re.compile(
    r"^(Mon|Tue|Wed|Thu|Fri|Sat|Sun) "
    r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) "
    r"\s?([0-3]?\d) (\d{2}):(\d{2}):(\d{2}) (\d{4})$",
    re.ASCII,
)


def provider_start_matches(
    provider_start: object, kernel_start: str | None,
    provider_domain: object = None,
) -> bool:
    """Does Claude's provider start denote this kernel-observed process start?

    Linux provider metadata is the exact decimal /proc field-22 token. Darwin
    records a C-locale, UTC one-second lstart timestamp; its kernel token
    retains the finer microseconds, which must not be compared as a string.
    Parse English names explicitly, independent of the host's LC_TIME locale.
    """
    if not isinstance(kernel_start, str):
        return False
    # Claude's Linux pidDomain includes machine and namespace details, e.g.
    # linux:<machine-id>:pid:[<namespace-inode>]. Only its OS prefix is
    # relevant here: kernel PID/start ownership is proved independently.
    if (provider_domain is not None
            and (not isinstance(provider_domain, str)
                 or provider_domain.split(":", 1)[0] != sys.platform)):
        return False
    if sys.platform == "linux":
        return (type(provider_start) in (str, int)
                and str(provider_start).isascii()
                and str(provider_start).isdecimal()
                and str(provider_start) == kernel_start)
    if sys.platform != "darwin" or not isinstance(provider_start, str):
        return False
    match = _DARWIN_LSTART.fullmatch(provider_start)
    token = re.fullmatch(r"darwin:(\d+):(\d{6})", kernel_start, re.ASCII)
    if match is None or token is None:
        return False
    try:
        weekday, month, day, hour, minute, second, year = match.groups()
        parsed = datetime(int(year), _ENGLISH_MONTHS.index(month) + 1,
                          int(day), int(hour), int(minute), int(second),
                          tzinfo=timezone.utc)
        return (parsed.weekday() == _ENGLISH_WEEKDAYS.index(weekday)
                and int(parsed.timestamp()) == int(token[1])
                and int(token[2]) < 1_000_000)
    except (ValueError, OverflowError):
        return False


def valid_kernel_start_token(token: object) -> bool:
    """Recognize only this OS's original process-start identity encoding."""
    if not isinstance(token, str) or not token:
        return False
    if sys.platform == "linux":
        return token.isascii() and token.isdecimal()
    if sys.platform == "darwin":
        match = re.fullmatch(r"darwin:(\d+):(\d{6})", token, re.ASCII)
        return bool(match and int(match[1]) > 0 and int(match[2]) < 1_000_000)
    return False


# Darwin struct proc_bsdinfo from sys/proc_info.h (MAXCOMLEN=16). These
# layouts and the imported constants must be checked against the installed
# Monterey SDK before treating a real Mac run as release validation.
_DARWIN_PROC_PIDTBSDINFO = 3
_DARWIN_SZOMB = 5
_DARWIN_SOL_LOCAL = 0
_DARWIN_LOCAL_PEERCRED = 1
_DARWIN_LOCAL_PEERPID = 2
_DARWIN_XUCRED_VERSION = 0


class _DarwinBSDInfo(ctypes.Structure):
    _fields_ = [
        ("pbi_flags", ctypes.c_uint32), ("pbi_status", ctypes.c_uint32),
        ("pbi_xstatus", ctypes.c_uint32), ("pbi_pid", ctypes.c_uint32),
        ("pbi_ppid", ctypes.c_uint32), ("pbi_uid", ctypes.c_uint32),
        ("pbi_gid", ctypes.c_uint32), ("pbi_ruid", ctypes.c_uint32),
        ("pbi_rgid", ctypes.c_uint32), ("pbi_svuid", ctypes.c_uint32),
        ("pbi_svgid", ctypes.c_uint32), ("rfu_1", ctypes.c_uint32),
        ("pbi_comm", ctypes.c_char * 16), ("pbi_name", ctypes.c_char * 32),
        ("pbi_nfiles", ctypes.c_uint32), ("pbi_pgid", ctypes.c_uint32),
        ("pbi_pjobc", ctypes.c_uint32), ("e_tdev", ctypes.c_uint32),
        ("e_tpgid", ctypes.c_uint32), ("pbi_nice", ctypes.c_int32),
        ("pbi_start_tvsec", ctypes.c_uint64),
        ("pbi_start_tvusec", ctypes.c_uint64),
    ]


class _DarwinXucred(ctypes.Structure):
    _fields_ = [
        ("cr_version", ctypes.c_uint32), ("cr_uid", ctypes.c_uint32),
        ("cr_ngroups", ctypes.c_int16),
        ("cr_groups", ctypes.c_uint32 * 16),
    ]


@lru_cache(maxsize=1)
def _darwin_proc_pidinfo():
    """Load libproc and configure its ABI once, lazily on first Darwin use."""
    libproc = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
    call = libproc.proc_pidinfo
    call.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_uint64,
                     ctypes.c_void_p, ctypes.c_int]
    call.restype = ctypes.c_int
    return call


def _darwin_pidinfo_call(pid: int, info: _DarwinBSDInfo) -> tuple[int, int]:
    """One SDK call; injectable on Linux for exact ABI/error tests."""
    call = _darwin_proc_pidinfo()
    ctypes.set_errno(0)
    result = call(pid, _DARWIN_PROC_PIDTBSDINFO, 0,
                  ctypes.byref(info), ctypes.sizeof(info))
    return result, ctypes.get_errno()


def _darwin_process_info(pid: int | None) -> ProcessInfo:
    if not pid:
        return ProcessInfo(pid, "dead", None, None, None)
    if type(pid) is not int or pid <= 0:
        return ProcessInfo(pid, "unprovable", None, None, None)
    info = _DarwinBSDInfo()
    try:
        size, error = _darwin_pidinfo_call(pid, info)
    except (OSError, AttributeError, TypeError, ValueError):
        return ProcessInfo(pid, "unprovable", None, None, None)
    if size != ctypes.sizeof(info):
        # ESRCH establishes a missing process. A short result, EPERM,
        # race or ambiguous zero must not establish absence/ownership.
        return ProcessInfo(pid, "dead" if size <= 0 and error == errno.ESRCH
                           else "unprovable", None, None, None)
    if (info.pbi_pid != pid or info.pbi_status not in (1, 2, 3, 4, 5)
            or info.pbi_uid == 0xffffffff):
        return ProcessInfo(pid, "unprovable", None, None, None)
    state = {1: "I", 2: "R", 3: "S", 4: "T", 5: "Z"}[info.pbi_status]
    if info.pbi_status == _DARWIN_SZOMB:
        return ProcessInfo(pid, "dead", info.pbi_ppid, state, None)
    sec, usec = info.pbi_start_tvsec, info.pbi_start_tvusec
    if sec <= 0 or usec >= 1_000_000:
        return ProcessInfo(pid, "unprovable", info.pbi_ppid, state, None)
    return ProcessInfo(pid, "alive", info.pbi_ppid, state,
                       f"darwin:{sec}:{usec:06d}")


def _darwin_peer_credentials(conn: socket.socket) -> PeerCredentials:
    """Both credentials are from this same accepted AF_UNIX connection."""
    try:
        credentials = conn.getsockopt(
            _DARWIN_SOL_LOCAL, _DARWIN_LOCAL_PEERCRED, ctypes.sizeof(_DarwinXucred))
        peer_pid = conn.getsockopt(
            _DARWIN_SOL_LOCAL, _DARWIN_LOCAL_PEERPID, ctypes.sizeof(ctypes.c_int32))
        if (not isinstance(credentials, bytes) or
                len(credentials) != ctypes.sizeof(_DarwinXucred)
                or not isinstance(peer_pid, bytes)
                or len(peer_pid) != ctypes.sizeof(ctypes.c_int32)):
            raise ValueError("incomplete Darwin kernel peer credentials")
        identity = _DarwinXucred.from_buffer_copy(credentials)
        pid = ctypes.c_int32.from_buffer_copy(peer_pid).value
        if (identity.cr_version != _DARWIN_XUCRED_VERSION or
                identity.cr_uid == 0xffffffff or
                not 0 <= identity.cr_ngroups <= 16 or pid <= 0):
            raise ValueError("invalid Darwin xucred version, UID, group count or peer PID")
    except (OSError, ValueError, TypeError) as exc:
        raise PeerIdentityUnavailable(f"Darwin socket peer identity unavailable: {exc}") from exc
    gid = identity.cr_groups[0] if identity.cr_ngroups else None
    return PeerCredentials(uid=identity.cr_uid, pid=pid, gid=gid)


@contextmanager
def _darwin_open_root_dir(path: Path) -> Iterator[tuple[Path, int]]:
    """Pin no-follow ancestors and leaf, including search-only ancestors.

    macOS 27 offers O_SEARCH for traverse-only directories. Older Darwin
    kernels may reject it; retain their established O_RDONLY behavior.
    """
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    search = getattr(os, "O_SEARCH", 0)
    absolute = Path(os.path.abspath(os.fspath(path)))
    fd: int | None = None

    def pinned_open(component: str, *, parent: int | None, leaf: bool) -> int:
        options = {} if parent is None else {"dir_fd": parent}
        if not leaf and search:
            try:
                return os.open(component, flags | search, **options)
            except OSError as exc:
                if exc.errno not in (errno.EINVAL, errno.ENOTSUP, errno.EOPNOTSUPP,
                                     errno.ENOSYS):
                    raise
        return os.open(component, flags, **options)

    try:
        parts = absolute.parts[1:]
        fd = pinned_open(absolute.anchor, parent=None, leaf=not parts)
        for index, part in enumerate(parts):
            next_fd = pinned_open(part, parent=fd, leaf=index == len(parts) - 1)
            os.close(fd)
            fd = next_fd
    except OSError as exc:
        if fd is not None:
            os.close(fd)
        raise RootOpenError("unsafe/unreadable Darwin root ancestor or leaf") from exc
    try:
        # Exceptions raised by the context body retain their original type.
        yield absolute, fd
    finally:
        os.close(fd)


def _darwin_runtime_base(
    *, purpose: Literal["transport", "live_vars"], suffix_bytes: int = 0,
) -> RuntimeChoice:
    """Select a short private base without any filesystem writes."""
    if purpose not in ("transport", "live_vars") or suffix_bytes < 0:
        raise ValueError(f"unsupported Darwin runtime selection: {purpose}")
    xdg = os.environ.get("XDG_RUNTIME_DIR")
    if xdg and os.path.isabs(xdg):
        try:
            st = os.lstat(xdg)
        except OSError:
            pass
        else:
            if (stat.S_ISDIR(st.st_mode) and st.st_uid == os.getuid()
                    and not st.st_mode & 0o077
                    and len(os.fsencode(xdg)) + suffix_bytes <= 104):
                return RuntimeChoice(Path(xdg))
    # No reliance on macOS's often very long /var/folders/... TMPDIR.
    return _short_private_runtime_base(Path("/private/tmp"), suffix_bytes)


def _darwin_pinned_fd_path(fd: int) -> str:
    """Probe /dev/fd reopen on this exact held inode, never its pathname."""
    original = os.fstat(fd)
    if not stat.S_ISREG(original.st_mode):
        raise OSError("cannot reopen nonregular Darwin source")
    old_offset = os.lseek(fd, 0, os.SEEK_CUR)
    path = f"/dev/fd/{fd}"
    reopened = os.open(path, os.O_RDONLY | os.O_CLOEXEC)
    try:
        observed = os.fstat(reopened)
        if ((observed.st_dev, observed.st_ino, observed.st_mode) !=
                (original.st_dev, original.st_ino, original.st_mode)
                or observed.st_size != original.st_size):
            raise OSError("Darwin /dev/fd did not reopen the original file")
        saved = os.lseek(reopened, 0, os.SEEK_CUR)
        os.lseek(reopened, 0, os.SEEK_SET)
        if os.lseek(fd, 0, os.SEEK_CUR) != old_offset:
            raise OSError("Darwin /dev/fd reopen shares mutable source offset")
        os.lseek(reopened, saved, os.SEEK_SET)
        if os.lseek(fd, 0, os.SEEK_CUR) != old_offset:
            raise OSError("Darwin /dev/fd verification changed source offset")
    finally:
        os.close(reopened)
        if os.lseek(fd, 0, os.SEEK_CUR) != old_offset:
            os.lseek(fd, old_offset, os.SEEK_SET)
    return path


def _darwin_read_argv(_pid: int | None) -> None:
    """Darwin argv is not an ownership fact; no KERN_PROCARGS2 parser."""
    return None


def physical_directory_spelling(resolved: Path) -> str:
    """Linux's physical symlink resolution keeps Path.resolve() spelling."""
    return str(resolved)


def _darwin_physical_directory_spelling(resolved: Path) -> str:
    """Read the real APFS directory case via a pinned descriptor/F_GETPATH.

    Creation opens the resolved directory (not O_NOFOLLOW: symlinks are valid).
    For an untrusted path that was deleted after its ledger write, read the
    longest existing directory ancestor and append only the missing suffix.
    """
    ancestor = resolved
    suffix: list[str] = []
    while True:
        try:
            fd = os.open(ancestor, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        except OSError as exc:
            if exc.errno not in (errno.ENOENT, errno.ENOTDIR) or ancestor == ancestor.parent:
                raise
            suffix.insert(0, ancestor.name)
            ancestor = ancestor.parent
            continue
        try:
            # F_GETPATH is Darwin fcntl command 50; some Python builds lack
            # the symbolic name even though the kernel supplies the operation.
            raw = fcntl.fcntl(fd, getattr(fcntl, "F_GETPATH", 50), bytes(1024))
        finally:
            os.close(fd)
        if not isinstance(raw, bytes) or b"\0" not in raw or raw.startswith(b"\0"):
            raise OSError(errno.EIO, "Darwin F_GETPATH returned an invalid directory path")
        actual = Path(os.fsdecode(raw.split(b"\0", 1)[0]))
        if not actual.is_absolute():
            raise OSError(errno.EIO, "Darwin F_GETPATH returned a relative directory path")
        return str(actual.joinpath(*suffix))


def _darwin_parent_watch(parent_pid: int) -> None:
    while True:
        try:
            if os.getppid() != parent_pid:
                os._exit(0)
            time.sleep(0.3)
        except Exception:
            os._exit(0)


def _darwin_arm_parent_death(parent_pid: int) -> None:
    """Read-only RPC children die on lost parent even during blocked I/O."""
    try:
        if parent_pid <= 0 or os.getppid() != parent_pid:
            os._exit(0)
        watcher = threading.Thread(target=_darwin_parent_watch,
                                   args=(parent_pid,), daemon=True,
                                   name="ccx-read-only-parent-watch")
        watcher.start()
        if not watcher.is_alive() or os.getppid() != parent_pid:
            os._exit(0)
    except Exception:
        os._exit(0)


# The sole OS dispatch in CCX; callers consume facts, not platform branches.
if sys.platform == "darwin":
    process_info = _darwin_process_info
    read_argv = _darwin_read_argv
    peer_credentials = _darwin_peer_credentials
    open_root_dir = _darwin_open_root_dir
    runtime_base = _darwin_runtime_base
    pinned_fd_path = _darwin_pinned_fd_path
    arm_parent_death = _darwin_arm_parent_death
    physical_directory_spelling = _darwin_physical_directory_spelling
    held_flock_inodes = _darwin_held_flock_inodes
