"""Read-only presence of the runtime that writes a worker's event journal.

A runtime killed abruptly (SIGKILL, OOM) writes no final event, so a held
event read alone cannot tell a quiet worker from a dead one. Each runtime
already holds a kernel lifetime lease for its whole life: a Claude host
``runtime-metadata/host-locks/<worker>.lease.lock`` (``host._host_process_lease``)
and codexd ``runtime-metadata/pid/codexd.lease.lock``
(``transport.daemon_process_lease``). The kernel drops it with the process.

This module only *looks* at those leases, through the kernel lock table
(``oslayer.held_flock_inodes``: ``/proc/locks`` on Linux; Darwin has none, so
``runtime`` is always ``unknown`` there). It never
takes a lock: even a momentary non-blocking probe could make a starting
Claude host, whose lease acquisition is non-blocking, refuse to start. It
writes nothing, calls no runtime and starts nothing.

``runtime`` values:

- ``live``: the lease of this root/generation/worker runtime is held.
- ``gone``: the lease has been free, and the recorded runtime process is not
  alive, for a whole startup grace (the one ``worker.wait`` uses: 0.35 s, or
  5 s while booting or resuming) within this observation. It prompts an
  authoritative bounded ``worker.wait``; it never proves that a turn ended.
- ``unknown``: anything else (no kernel lock table, an untrusted lease file,
  a free lease whose recorded process is still alive, or a free lease not yet
  observed for the grace).
"""
from __future__ import annotations

import os
import stat
import time
from typing import Callable

from ccx.system import oslayer
from ccx.store.liveness import owned_process

LIVE = "live"
GONE = "gone"
UNKNOWN = "unknown"

HOST_MISSING_GRACE_S = 0.35
BOOTING_GRACE_S = 5.0
_BOOTING = frozenset({"booting", "resuming", "starting"})


def _lease_file(store, engine: str, worker_id: str) -> os.stat_result | None | bool:
    """The lease file's stat, None when it was never created, False if untrusted."""
    parts, name = ((("runtime-metadata", "host-locks"), worker_id + ".lease.lock")
                   if engine == "claude" else
                   (("runtime-metadata", "pid"), "codexd.lease.lock"))
    try:
        with store._gen(*parts) as directory:
            info = os.stat(name, dir_fd=directory, follow_symlinks=False)
    except FileNotFoundError:
        return None
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
            or info.st_mode & 0o077):
        return False
    return info


def _codexd_identity(store) -> tuple[object, object]:
    """The recorded codexd pid/start of this exact root and generation."""
    from ccx.store.root import _read_json
    try:
        with store._gen("runtime-metadata", "pid") as directory:
            meta = _read_json(directory, "codexd.json")
    except (FileNotFoundError, OSError, ValueError):
        return None, None
    if (not isinstance(meta, dict) or meta.get("generation") != store.generation
            or meta.get("rootRealpath") != str(store.root)):
        return None, None
    return meta.get("pid"), meta.get("startToken")


def lease(store, engine: str, worker_id: str, record: dict | None) -> str:
    """One sample: ``held``, ``free`` (and the recorded process is not alive) or ``unknown``."""
    # Without a kernel lock table nothing can be observed, not even a free
    # lease: a never-created lease file must not become "gone" here.
    held = oslayer.held_flock_inodes()
    if held is None:
        return UNKNOWN
    info = _lease_file(store, engine, worker_id)
    if info is False:
        return UNKNOWN
    if (info is not None and
            (os.major(info.st_dev), os.minor(info.st_dev), info.st_ino) in held):
        return "held"
    if engine == "claude":
        lifecycle = (record or {}).get("lifecycle") or {}
        pid, start = lifecycle.get("hostPid"), lifecycle.get("hostStart")
    else:
        pid, start = _codexd_identity(store)
    # A free lease next to a recorded process that is still alive is a start
    # before its lease, or a lease this reader cannot see: never "gone".
    if pid is not None and owned_process(pid, start) != "dead":
        return UNKNOWN
    return "free"


class RuntimeObserver:
    """Presence across one held read; no state outlives it or is shared."""

    def __init__(self, store, target: str,
                 clock: Callable[[], float] = time.monotonic):
        owner = store.resolve(target)
        self._store, self._target = store, target
        self._engine, self._worker_id = owner.engine, owner.worker_id
        self._clock = clock
        self._free_since: float | None = None
        self.value: str = UNKNOWN
        self.seen_live = False

    def sample(self) -> str:
        # The shared activation lease, as every other source read takes it.
        with self._store.lease():
            record = self._store.record(self._target)
            observed = lease(self._store, self._engine, self._worker_id, record)
        now = self._clock()
        if observed == "held":
            self._free_since = None
            self.seen_live = True
            self.value = LIVE
        elif observed == "free":
            if self._free_since is None:
                self._free_since = now
            native = ((record or {}).get("lifecycle") or {}).get("state")
            grace = BOOTING_GRACE_S if native in _BOOTING else HOST_MISSING_GRACE_S
            self.value = GONE if now - self._free_since >= grace else UNKNOWN
        else:
            self._free_since = None
            self.value = UNKNOWN
        return self.value


__all__ = ["GONE", "LIVE", "UNKNOWN", "RuntimeObserver", "lease"]
