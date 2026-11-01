from __future__ import annotations

import contextlib
import json
import os
import socket
from pathlib import Path
import signal
import subprocess
import sys
import time

from ccx.constants import SELF_ENGINE_ENV, SELF_WORKER_ID_ENV

SCHEMA_VERSION = 1
_MAX_STDIN = 32768
_USER_COMMAND_TIMEOUT_S = 2.0


def _normalize_payload(short: str, payload: dict) -> dict:
    model = payload.get("model")
    if not isinstance(model, dict):
        model = {}
    cw = payload.get("context_window")
    if not isinstance(cw, dict):
        cw = None
    return {
        "schemaVersion": SCHEMA_VERSION,
        "writtenAtMs": int(time.time() * 1000),
        "short": short,
        "sessionId": payload.get("session_id"),
        "model": {
            "id": model.get("id"),
            "display_name": model.get("display_name"),
        },
        "statuslineVersion": payload.get("version"),
        "contextWindow": cw,
        "exceeds200k": payload.get("exceeds_200k_tokens"),
    }


def write_statusline_sidecar(short: str, payload: dict) -> str | None:
    """Capture only the original owned Claude session's advisory statusline."""
    from ccx import constants
    from ccx.store.root import StorageIntegrityError, _write_json, require_ccx_active

    store = require_ccx_active(constants.CCX_ROOT)
    owner = store.resolve(short)  # Exact global full UUID; never display/short/native.
    if (owner.engine != "claude"
            or os.environ.get(SELF_WORKER_ID_ENV) != owner.worker_id
            or os.environ.get(SELF_ENGINE_ENV) != "claude"):
        raise StorageIntegrityError("statusline worker binding mismatch")
    rec = store.record(owner.worker)
    native = rec.get("native")
    native_sid = native.get("sessionId") if isinstance(native, dict) else None
    fingerprint = rec.get("createSha256")
    if (not isinstance(native_sid, str) or not isinstance(fingerprint, str)
            or payload.get("session_id") != native_sid):
        raise StorageIntegrityError("statusline original native session mismatch")
    original = store.creation_binding(
        owner.worker, engine="claude", fingerprint=fingerprint,
    )
    if not isinstance(original, dict) or original.get("sessionId") != native_sid:
        raise StorageIntegrityError("statusline original native binding unproven")
    doc = _normalize_payload(owner.worker_id, payload)
    with store.lease():
        current_owner, current = store._owned(owner.worker)
        if (current_owner != owner or not isinstance(current.get("native"), dict)
                or current["native"].get("sessionId") != native_sid
                or current.get("createSha256") != fingerprint):
            raise StorageIntegrityError("statusline owner changed during capture")
        with store._gen("workers", "claude", owner.worker_id) as fd:
            _write_json(fd, "statusline.json", doc)
    return rec.get("cwd") if isinstance(rec.get("cwd"), str) else None


def nudge_statusline_host(short: str, session_id: str) -> None:
    """One best-effort local wake. The host, not this relay, reads and journals."""
    from ccx.store.paths import sock_path
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.setblocking(False)
            # Never select/wait for a busy host and never read an ACK.
            if sock.connect_ex(str(sock_path(short))) != 0:
                return
            sock.send((json.dumps({
                "op": "statusline-updated", "worker_id": short,
                "session_id": session_id,
            }) + "\n").encode())
    except (OSError, ValueError):
        pass


def _configured_statusline(cwd: Path) -> str | None:
    """Resolve Claude's user -> project -> local command, omitting our sidecar.

    This is a separate projection of the caller's settings. Do not read CCX's
    generated per-worker ``--settings`` file, which points back at this relay.
    Later layers override earlier ones, including explicit null/disabled values.
    """
    from ccx import constants

    selected = None
    for path in (constants.CLAUDE_HOME / "settings.json",
                 cwd / ".claude" / "settings.json",
                 cwd / ".claude" / "settings.local.json"):
        try:
            cfg = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError, UnicodeError):
            continue
        if isinstance(cfg, dict) and "statusLine" in cfg:
            selected = cfg["statusLine"]
    if not isinstance(selected, dict) or selected.get("type") != "command":
        return None
    command = selected.get("command")
    return command if isinstance(command, str) and command.strip() else None


def cmd_statusline_relay(args) -> None:
    """Capture exact original statusline, then pass stdin to user configuration.

    The user-configured command is unmodified and runs from the owned worker's
    cwd with the original statusline JSON bytes. Its stderr is not displayed or
    logged. Any missing configuration, failed command or timeout is fail-open.
    """
    try:
        raw = sys.stdin.read(_MAX_STDIN + 1)
        if len(raw) > _MAX_STDIN:
            return
        payload = json.loads(raw or "{}")
        if not isinstance(payload, dict):
            return
        cwd_string = write_statusline_sidecar(args.short, payload)
        if not cwd_string:
            return
        nudge_statusline_host(args.short, payload.get("session_id"))
        cwd = Path(cwd_string)
        command = _configured_statusline(cwd)
        if command is None:
            return
        # Shell commands can launch children/pipelines. Own a fresh process
        # group and kill the whole group on timeout, not just the shell, so
        # statusline refreshes never strand a slow background child.
        child = subprocess.Popen(
            command, shell=True, cwd=cwd, text=True,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, start_new_session=True,
        )
        try:
            stdout, _stderr = child.communicate(input=raw, timeout=_USER_COMMAND_TIMEOUT_S)
        except subprocess.TimeoutExpired:
            with contextlib.suppress(OSError):
                os.killpg(child.pid, signal.SIGKILL)
            with contextlib.suppress(OSError, subprocess.TimeoutExpired):
                child.communicate(timeout=1.0)
            return
        if child.returncode == 0:
            sys.stdout.write(stdout)
    except Exception:
        pass
