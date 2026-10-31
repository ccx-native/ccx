from __future__ import annotations

import errno
import os
import pathlib
import re
import socket
import tempfile
import unicodedata

from ccx.system import oslayer
from ccx import constants
from ccx.constants import CLAUDE_HOME

def private_mkdir(path: pathlib.Path | str) -> pathlib.Path:
    """``mkdir -p`` where every directory it creates, and the leaf, is 0700.

    ``Path.mkdir(parents=True, mode=...)`` applies the mode to the leaf only;
    the parents it creates get the umask default, which would leave e.g.
    ``operations/`` listable by other users.
    """
    path = pathlib.Path(path)
    missing = []
    probe = path
    while not probe.exists() and probe != probe.parent:
        missing.append(probe)
        probe = probe.parent
    for directory in reversed(missing):
        try:
            directory.mkdir(mode=0o700)
        except FileExistsError:
            continue
        directory.chmod(0o700)
    path.chmod(0o700)
    return path


def ensure_dirs():
    """New runtime bootstrap is observational; never mkdir/chmod old short tree."""
    from ccx import constants
    from ccx.store.root import require_ccx_active
    return require_ccx_active(constants.CCX_ROOT)



def peer_is_same_user(conn: socket.socket) -> bool:
    """Whether an accepted AF_UNIX peer runs as this user (Linux SO_PEERCRED).

    The 0700 directory and 0600 socket are the primary guard; this is the
    second one, so a socket reachable by mistake still only answers its
    owner. Missing or unreadable kernel credentials always fail closed;
    file permissions alone cannot authenticate a connected peer.
    """
    try:
        credentials = oslayer.peer_credentials(conn)
    except oslayer.PeerIdentityUnavailable:
        return False
    return credentials.uid == os.getuid()


def accept_same_user(server: socket.socket) -> socket.socket | None:
    """``server.accept()`` that closes and drops a peer of another user.

    Returns None for a dropped peer; accept errors propagate as before.
    """
    conn, _ = server.accept()
    if peer_is_same_user(conn):
        return conn
    try:
        conn.close()
    except OSError:
        pass
    return None


def _require_unix_socket_budget(path: pathlib.Path) -> None:
    """Check the final and fixed-width staged bind paths without writing."""
    # mkdtemp(prefix=".") generates exactly eight characters after the dot.
    for candidate, kind in (
        (path, "final"),
        (path.parent / ".XXXXXXXX" / "s", "staged"),
    ):
        if not oslayer.fits_unix_path(candidate):
            raise OSError(errno.ENAMETOOLONG,
                          f"{kind} Unix socket path exceeds AF_UNIX pathname budget",
                          os.fspath(candidate))


def bind_private_unix_socket(server: socket.socket, path: pathlib.Path | str) -> None:
    """Bind ``server`` at ``path`` with mode 0600 and no permissive window.

    Binding a socket creates the inode with umask permissions, so chmod alone
    leaves an exposure window. Instead, bind inside a fresh private 0700
    staging directory, chmod there, and atomically rename the 0600 socket.
    This avoids changing the process-wide umask, which could race threads.
    """
    path = pathlib.Path(path)
    _require_unix_socket_budget(path)  # before mkdtemp creates any directory
    staging = pathlib.Path(tempfile.mkdtemp(prefix=".", dir=path.parent))
    try:
        tmp = staging / "s"
        server.bind(str(tmp))
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)
    finally:
        try:
            (staging / "s").unlink()
        except FileNotFoundError:
            pass
        try:
            os.rmdir(staging)
        except OSError:
            pass

def trust_ledger_path() -> pathlib.Path:
    """Which ``.claude.json`` trust entries ccx itself wrote (see settings)."""
    return constants.CCX_ROOT / "trust-ledger.json"

def _owner_path(target: str, filename: str) -> pathlib.Path:
    """Inert per-owned-worker pathname; actual writers must use pinned fds."""
    from ccx import constants
    from ccx.store.root import require_ccx_active
    from ccx.store.identity import EngineMismatch
    store = require_ccx_active(constants.CCX_ROOT)
    owner = store.resolve(target)
    if owner.engine != "claude":
        raise EngineMismatch("Claude CCX sidecar requires Claude full worker owner")
    # The path is NOT a second authority: the authoritative owner is checked
    # on each call, and native internal sidecar I/O must use the pinned fd.
    return (store.pointer.generation_root / "workers" /
            owner.engine / owner.worker_id / filename)


def sessions_dir() -> pathlib.Path:
    from ccx import constants
    from ccx.store.root import require_ccx_active
    store = require_ccx_active(constants.CCX_ROOT)
    return store.pointer.generation_root / "workers" / "claude"


def record_path(short: str) -> pathlib.Path:
    return _owner_path(short, "record.json")


def transcript_json_path(short: str) -> pathlib.Path:
    return _owner_path(short, "transcript.json")


def statusline_json_path(short: str) -> pathlib.Path:
    return _owner_path(short, "statusline.json")


def settings_path(short: str) -> pathlib.Path:
    return _owner_path(short, "settings.json")


def events_ndjson_path(short: str) -> pathlib.Path:
    return _owner_path(short, "events.ndjson")


def conversation_page_index_path(short: str) -> pathlib.Path:
    return _owner_path(short, "conversation-page-index.json")


def compact_operations_path(short: str) -> pathlib.Path:
    return _owner_path(short, "compact.json")


def steer_deliveries_path(short: str) -> pathlib.Path:
    return _owner_path(short, "deliveries.json")


def steer_deliveries_lock_path(short: str) -> pathlib.Path:
    return _owner_path(short, "deliveries.lock")


def sock_path(short: str) -> pathlib.Path:
    from ccx import constants
    from ccx.store.root import require_ccx_active
    from ccx.rpc.transport import plan_transport
    store = require_ccx_active(constants.CCX_ROOT)
    owner = store.resolve(short)
    if owner.engine != "claude":
        from ccx.store.identity import EngineMismatch
        raise EngineMismatch("Claude socket cannot use Codex worker")
    return plan_transport(store, target=owner.worker).final


def respawn_lock_path(short: str) -> pathlib.Path:
    return _owner_path(short, "respawn.lock")


def host_pid_path(short: str) -> pathlib.Path:
    return _owner_path(short, "host.pid")


def host_log_path(short: str) -> pathlib.Path:
    return _owner_path(short, "host.log")


def claude_project_slug(cwd: str) -> str:
    """Mirror Claude 2.1.311's ``hL`` key (UTF-16 units / 200-unit cap)."""
    # Claude's macOS cwd is NFC before hL; on Linux it retains on-disk NFD.
    if oslayer.claude_project_cwd_is_nfc():
        cwd = unicodedata.normalize("NFC", cwd)
    encoded = cwd.encode("utf-16-le", "surrogatepass")
    units = [encoded[i] | encoded[i + 1] << 8 for i in range(0, len(encoded), 2)]
    # Claude's JS /[^a-zA-Z0-9]/g replaces each astral surrogate separately.
    slug = "".join(
        chr(unit) if (48 <= unit <= 57 or 65 <= unit <= 90 or 97 <= unit <= 122)
        else "-" for unit in units
    )
    if len(slug) <= 200:
        return slug
    h = 0
    for unit in units:
        h = (31 * h + unit) & 0xffffffff
    value = abs(h if h < 0x80000000 else h - 0x100000000)
    digits = "0123456789abcdefghijklmnopqrstuvwxyz"
    suffix = ""
    while value:
        value, remainder = divmod(value, 36)
        suffix = digits[remainder] + suffix
    return f"{slug[:200]}-{suffix or '0'}"


def transcript_path_for(cwd: str, session_id: str) -> pathlib.Path:
    """Mirror of Claude Code's path derivation for ~/.claude/projects/<slug>/<uuid>.jsonl."""
    slug = claude_project_slug(cwd)
    return CLAUDE_HOME / "projects" / slug / f"{session_id}.jsonl"

def task_list_dir_for(session_id: str) -> pathlib.Path | None:
    """Claude Code's structured task-list directory for one native session.

    Claude currently keys ordinary task lists by the native session UUID under
    ``~/.claude/tasks/<session-id>``. Keep that private upstream layout behind
    CCX so consumers never need to derive it themselves. A malformed record id
    degrades to ``None`` instead of letting a path separator escape the task root.
    """
    if not session_id or pathlib.PurePath(session_id).name != session_id:
        return None
    if session_id in {".", ".."}:
        return None
    return CLAUDE_HOME / "tasks" / session_id

def subagent_dir_for(cwd: str, session_id: str) -> pathlib.Path:
    slug = claude_project_slug(cwd)
    return CLAUDE_HOME / "projects" / slug / session_id / "subagents"

def iter_subagent_transcripts(cwd: str, session_id: str):
    root = subagent_dir_for(cwd, session_id)
    if not root.exists():
        return
    yield from sorted(root.rglob("*.jsonl"))
