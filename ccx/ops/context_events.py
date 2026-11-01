"""Bound material carried-context changes for the existing worker event feed."""
from __future__ import annotations

import time

FIELDS = ("tokens_used", "window_size", "percent_used", "as_of_turn_ref",
          "as_of_ms", "reason")
INTERVAL_S = 5.0


def snapshot(value: dict | None) -> dict:
    value = value if isinstance(value, dict) else {}
    return {name: value.get(name) for name in FIELDS}


def _material(value: dict) -> tuple:
    return tuple(value.get(key) for key in FIELDS if key != "as_of_ms")


class ContextChanges:
    """One owner's small dedupe/throttle gate. Force bypasses the interval."""

    def __init__(self):
        self.last: dict | None = None
        self.pending: dict | None = None
        self.last_at: float | None = None

    def observe(self, value: dict | None, *, force: bool = False,
                now: float | None = None) -> dict | None:
        current = snapshot(value)
        if current["reason"] == "no-signal" and self.last is None:
            return None
        if self.last is not None and _material(current) == _material(self.last):
            self.pending = None
            return None
        now = time.monotonic() if now is None else now
        if not force and self.last_at is not None and now - self.last_at < INTERVAL_S:
            self.pending = current
            return None
        self.last = current
        self.pending = None
        self.last_at = now
        return current

    def flush(self, *, now: float | None = None) -> dict | None:
        if self.pending is None:
            return None
        return self.observe(self.pending, now=now)
