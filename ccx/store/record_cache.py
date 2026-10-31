"""File-stamped caching for expensive derived reads."""
from __future__ import annotations

import threading
import time
from pathlib import Path

# Avoid caching writes within coarse filesystem timestamp granularity.
_MIN_AGE_NS = 50_000_000


class StampedCache:
    """Cache by mtime, size and inode; missing files delegate to the reader."""

    def __init__(self) -> None:
        self._entries: dict[str, tuple[tuple[int, int, int], object]] = {}
        self._lock = threading.Lock()

    def get(self, path, compute):
        key = str(path)
        try:
            st = Path(path).stat()
        except OSError:
            with self._lock:
                self._entries.pop(key, None)
            return compute(path)
        stamp = (st.st_mtime_ns, st.st_size, st.st_ino)
        with self._lock:
            hit = self._entries.get(key)
        if hit is not None and hit[0] == stamp:
            return hit[1]
        value = compute(path)
        if time.time_ns() - st.st_mtime_ns >= _MIN_AGE_NS:
            with self._lock:
                self._entries[key] = (stamp, value)
        return value

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()
