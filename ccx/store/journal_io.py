"""Bounded reentrant locks and durable publication for Claude journals."""

import contextlib
import fcntl
import json
import os
from pathlib import Path
import tempfile
import threading
import time

_LOCAL = threading.local()
_THREAD_LOCKS: dict[Path, threading.RLock] = {}
_THREAD_LOCKS_GUARD = threading.Lock()


@contextlib.contextmanager
def journal_lock(path: Path, *, timeout: float, busy_error: type[Exception], label: str):
    # Key by lock path so different journals and roots cannot share ownership.
    path = path.resolve()
    held = getattr(_LOCAL, "held", None)
    if held is None:
        held = _LOCAL.held = set()
    if path in held:
        yield
        return
    budget = max(0.0, float(timeout))
    deadline = time.monotonic() + budget
    path.parent.mkdir(parents=True, exist_ok=True)
    with _THREAD_LOCKS_GUARD:
        thread_lock = _THREAD_LOCKS.setdefault(path, threading.RLock())
    acquired = (thread_lock.acquire(timeout=budget) if budget > 0
                else thread_lock.acquire(blocking=False))
    if not acquired:
        raise busy_error(f"{label} is locked by this process (waited {budget:g}s)")
    try:
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise busy_error(
                            f"{label} is held by another process (waited {budget:g}s)"
                        ) from None
                    time.sleep(min(0.02, max(0.0, deadline - time.monotonic())))
            held.add(path)
            try:
                yield
            finally:
                held.discard(path)
        finally:
            os.close(fd)
    finally:
        thread_lock.release()


def write_json(path: Path, value: dict) -> None:
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as handle:
            json.dump(value, handle, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, path)
        # Persist the rename as well as the contents before acknowledging it.
        dir_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise
