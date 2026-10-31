from __future__ import annotations

import os
import pathlib


def _store():
    from ccx import constants
    from ccx.store.root import require_ccx_active
    return require_ccx_active(constants.CCX_ROOT)


def root() -> pathlib.Path:
    """Verified generation's Codex namespace, not old CCX_ROOT/codex."""
    store = _store()
    return store.pointer.generation_root / "workers" / "codex"


def ensure_dirs() -> None:
    """Bootstrap verifies ACTIVE; does not mkdir/chmod legacy codex files."""
    _store()


def _worker_path(target: str, leaf: str) -> pathlib.Path:
    from ccx.store.identity import EngineMismatch
    store = _store()
    owner = store.resolve(target)
    if owner.engine != "codex":
        raise EngineMismatch("Codex path requires owned Codex full UUID")
    return root() / owner.worker_id / leaf


def sessions_dir() -> pathlib.Path:
    return root()


def record_path(short: str) -> pathlib.Path:
    return _worker_path(short, "record.json")


def evidence_dir() -> pathlib.Path:
    return root()


def evidence_path(short: str) -> pathlib.Path:
    return _worker_path(short, "evidence.jsonl")


def codexd_sock_path() -> pathlib.Path:
    from ccx.rpc.transport import plan_transport
    return plan_transport(_store(), role="codexd").final


def codexd_pid_path() -> pathlib.Path:
    return (_store().pointer.generation_root /
            "runtime-metadata" / "pid" / "codexd.json")


def codexd_spawn_lock_path() -> pathlib.Path:
    return (_store().pointer.generation_root /
            "runtime-metadata" / "pid" / "codexd.spawn.lock")


def codexd_lifetime_lock_path() -> pathlib.Path:
    return (_store().pointer.generation_root /
            "runtime-metadata" / "pid" / "codexd.lease.lock")


def codexd_log_path() -> pathlib.Path:
    return _store().pointer.generation_root / "runtime-metadata" / "codexd.log"
