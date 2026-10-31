from __future__ import annotations

from pathlib import Path


def root() -> Path:
    from ccx import constants
    return constants.CCX_ROOT / "notify"


def ensure_dirs() -> None:
    from ccx.store.paths import private_mkdir

    base = root()
    private_mkdir(base.parent)
    try:
        private_mkdir(base)
    except OSError:
        pass
    registrations_dir().mkdir(exist_ok=True)
    try:
        registrations_dir().chmod(0o700)
    except OSError:
        pass


def registrations_dir() -> Path:
    return root() / "registrations"


def registration_path(event_id: str) -> Path:
    return registrations_dir() / f"{event_id}.json"


def daemon_generation_path() -> Path:
    # One published role stamp: PID, protocol, ACTIVE generation, original
    # kernel start token. A bare PID or obsolete two-field stamp never proves
    # authority to signal the supervisor.
    return root() / "notifyd.generation"


def daemon_lock_path() -> Path:
    return root() / "notifyd.lock"


def daemon_spawn_lock_path() -> Path:
    return root() / "notifyd.spawn.lock"


def daemon_log_path() -> Path:
    return root() / "notifyd.log"
