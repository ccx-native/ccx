"""Private JSON files shared by account-level caches and settings."""

from __future__ import annotations

import errno
import json
import os
from pathlib import Path
import stat
import tempfile


def _open_private_file(path: Path) -> int:
    """Do not trust a symlink, another owner's file, or a readable private record."""
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    st = os.fstat(fd)
    if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid() or st.st_mode & 0o077:
        os.close(fd)
        raise PermissionError(f"not a private regular file owned by this user: {path}")
    return fd


def _read_private_json(path: Path) -> dict | None:
    with os.fdopen(_open_private_file(path), "rb") as handle:
        value = json.load(handle)
    return value if isinstance(value, dict) else None


def _atomic_private_json(path: Path, value: dict) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    tmp = None
    try:
        fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        with os.fdopen(fd, "w", encoding="utf-8") as out:
            json.dump(value, out, separators=(",", ":"), allow_nan=False)
            out.flush()
            os.fsync(out.fileno())
        # Never overwrite a planted symlink/foreign file at the final path.
        try:
            existing = _open_private_file(path)
        except FileNotFoundError:
            pass
        except OSError as exc:
            # Replacing a planted symlink itself is safe: os.replace never
            # follows the destination. Its target is neither read nor written.
            if exc.errno != errno.ELOOP:
                raise
        else:
            os.close(existing)
        os.replace(tmp, path)
        tmp = None
    finally:
        if tmp is not None:
            os.unlink(tmp)
