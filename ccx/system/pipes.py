"""Quiet exit when the reader of stdout goes away (`ccx logs | head`).

Standard CLI tools stop silently when the downstream reader closes the pipe.
Python instead raises ``BrokenPipeError`` on the next write or on the final
flush at shutdown, which prints a traceback. Entry points wrap their work in
:func:`quiet_broken_stdout`; only a broken *stdout* is silenced, so an EPIPE
from a worker socket still surfaces as an error.
"""
from __future__ import annotations

import os
import sys
from contextlib import contextmanager
from typing import Iterator

from ccx.system import oslayer


def stdout_reader_gone() -> bool:
    """True when stdout is a pipe or socket whose reading end has closed."""
    try:
        return oslayer.stdout_reader_gone(sys.stdout.fileno())
    except (AttributeError, OSError, ValueError):
        return False


@contextmanager
def quiet_broken_stdout() -> Iterator[None]:
    """Exit 0 without a traceback when stdout's reader has gone away."""
    try:
        try:
            yield
        finally:
            sys.stdout.flush()
    except BrokenPipeError:
        if not stdout_reader_gone():
            raise
        # Point stdout at /dev/null so the interpreter's own shutdown flush of
        # whatever is still buffered cannot fail again.
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())
        os.close(devnull)
        raise SystemExit(0)


__all__ = ["quiet_broken_stdout", "stdout_reader_gone"]
