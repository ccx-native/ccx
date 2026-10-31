from __future__ import annotations

import json
import pathlib
from dataclasses import dataclass

from ccx.system import oslayer
from ccx.constants import CLAUDE_HOME

def pid_alive(pid: int | None) -> bool:
    if not pid:
        return False
    # Keep the historical EPERM-is-present projection, unlike controller runs.
    return oslayer.probe_pid(pid) in ("exists", "denied")


def owned_process(pid: int | None, start_token: str | int | None) -> oslayer.Presence:
    """Compare an original PID/start identity with the current OS observation.

    A PID without a start token cannot prove ownership; kernel-confirmed
    absence still proves the original process is gone.
    """
    if not pid:
        return "dead"
    if not start_token:
        return "dead" if oslayer.probe_pid(pid) == "missing" else "unprovable"
    info = oslayer.process_info(pid)
    if info.presence == "dead":
        return "dead"
    if info.presence != "alive" or info.start_token is None:
        return "unprovable"
    return "alive" if str(info.start_token) == str(start_token) else "dead"


def owned_pid_alive(pid: int | None, start_token: str | int | None = None) -> bool:
    """Is ``pid`` alive and still the process CCX recorded?

    A PID can be reused after the original process exits, crashes or after a
    reboot. A bare ``kill(pid, 0)`` can therefore report a stranger's process,
    including another user's process (EPERM still means that PID exists).
    Treating that as ownership could block a safe revive or signal a stranger.

    Only the original PID plus its kernel start token proves a living owner.
    Without the token, absence can be proven, but ownership cannot.
    """
    return owned_process(pid, start_token) == "alive"


def lifecycle_host_alive(lifecycle: dict | None) -> bool:
    """Whether a record's recorded managed host process is still that host."""
    lifecycle = lifecycle or {}
    return owned_pid_alive(lifecycle.get("hostPid"), lifecycle.get("hostStart"))


def recorded_host_loaded(engine: str, lifecycle: dict | None) -> bool | None:
    """RPC-free host presence, returning True/False only with identity proof.

    Claude's recorded host PID/start identifies the original host as alive or
    gone. Codex is served by the shared codexd: the daemon being alive says
    nothing about whether this particular thread is loaded, so Codex returns
    None (unobserved), not a fabricated True/False.
    """
    lifecycle = lifecycle if isinstance(lifecycle, dict) else {}
    if engine != "claude" or not lifecycle.get("hostPid"):
        return None
    observed = owned_process(lifecycle.get("hostPid"), lifecycle.get("hostStart"))
    return None if observed == "unprovable" else observed == "alive"


def proc_argv(pid: int | None) -> list[str] | None:
    """Full argv of a live pid from /proc/<pid>/cmdline, or None if unreadable.

    Reads the whole cmdline — a Claude session-id passed via `--session-id`/
    `--resume` sits well past the first argv tokens, so a truncated read would
    miss it."""
    raw = oslayer.read_argv(pid)
    if raw is None:
        return None
    parts = [p.decode("utf-8", "replace") for p in raw.split(b"\x00") if p]
    return parts or None


def session_pid_alive(pid: int | None, session_id: str | None = None) -> bool:
    """Is `pid` alive and actually the Claude session it claims to be?

    `~/.claude/sessions/<pid>.json` is pid-keyed and accumulates stale files
    (pids recycle). A bare `pid_alive` check could accept an unrelated process.
    Accept either of these identity checks:

      1. the live process's argv contains the 8-char session short — definitive,
         and present whenever the session was launched with `--session-id` or
         `--resume <id>` (bg agents, ccx-managed, resumed sessions); or
      2. argv0 is the `claude` binary (plain `claude` interactive sessions don't
         carry the id in argv). Claude rewrites its own pid-keyed session-meta on
         startup, so a live claude at this pid means the meta is its own.

    A recycled non-claude pid fails both gates, so its stale meta is rejected."""
    if not pid_alive(pid):
        return False
    argv = proc_argv(pid)
    if not argv:
        return False
    if session_id and session_id[:8] in " ".join(argv):
        return True
    argv0 = argv[0]
    base = argv0.rsplit("/", 1)[-1].lower()
    # `claude`, `~/.local/bin/claude`, or the SEA `.../claude/versions/<v>`.
    return base == "claude" or "/claude/versions/" in argv0 or "/claude/launcher" in argv0


@dataclass(frozen=True)
class NativeClaudeState:
    """One validated snapshot from Claude's pid-keyed interactive registry."""

    pid: int
    session_id: str
    kind: str
    status: str | None
    status_updated_at: int | None
    waiting_for: object | None
    proc_start: str | int | None


def proc_start_token(pid: int | None) -> str | None:
    """Linux /proc start-time token for one live pid, or ``None``.

    Field 22 is process start time in clock ticks since boot. It is stable for a
    process lifetime and rejects a stale Claude pid-file after PID reuse.
    """
    return oslayer.process_info(pid).start_token


def read_managed_claude_native_state(
    pid: int | None,
    session_id: str | None,
    *,
    claude_home: pathlib.Path | None = None,
) -> NativeClaudeState | None:
    """Read an exact managed Claude process's native session snapshot.

    The registry is an upstream-internal compatibility surface, so this helper is
    deliberately strict: any identity mismatch, unsupported kind, dead process,
    stale process-start token, or unreadable JSON is simply ``None`` (no native
    evidence). The caller must fall back to ordinary CCX semantic evidence.
    """
    if not pid or not session_id:
        return None
    root = claude_home or CLAUDE_HOME
    try:
        value = json.loads((root / "sessions" / f"{int(pid)}.json").read_text())
    except (OSError, ValueError, TypeError):
        return None
    if not isinstance(value, dict):
        return None
    meta_pid = value.get("pid")
    if meta_pid is not None:
        try:
            if int(meta_pid) != int(pid):
                return None
        except (TypeError, ValueError, OverflowError):
            return None
    if value.get("sessionId") != session_id:
        return None
    # Managed PTY workers are ordinary interactive Claude sessions. Future or
    # missing kinds are not silently treated as equivalent.
    kind = value.get("kind")
    if kind != "interactive":
        return None
    meta_start = value.get("procStart")
    if meta_start is None:
        meta_start = value.get("procStartFt")
    if meta_start is not None:
        live_start = proc_start_token(pid)
        if not oslayer.provider_start_matches(
            meta_start, live_start, value.get("pidDomain")):
            return None
    elif not session_pid_alive(pid, session_id):
        # Legacy local metadata may lack the provider token. It is not a
        # Darwin authorization; only Linux's historical argv corroboration
        # can supply the optional diagnostic snapshot.
        return None
    status = value.get("status")
    status_updated_at = value.get("statusUpdatedAt")
    if not isinstance(status_updated_at, int) or isinstance(status_updated_at, bool):
        status_updated_at = None
    return NativeClaudeState(
        pid=int(pid),
        session_id=session_id,
        kind=kind,
        status=status if isinstance(status, str) else None,
        status_updated_at=status_updated_at,
        waiting_for=value.get("waitingFor"),
        proc_start=meta_start,
    )
