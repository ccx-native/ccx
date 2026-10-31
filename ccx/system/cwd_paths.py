"""Canonical directory spelling shared by original create and targeted untrust.

``Path.resolve`` follows symlinks on both platforms, but on case-insensitive
APFS it can retain the *caller's* case rather than the directory's actual
on-disk case. Darwin F_GETPATH returns the path of the opened directory inode.
For a directory that was removed after trust, resolve the longest existing
prefix and preserve the missing suffix for an exact ledger lookup.
"""
from __future__ import annotations

from pathlib import Path

from ccx.system import oslayer


def canonical_cwd(path: str | Path) -> str:
    """Return the symlink-resolved, physical directory spelling if available.

    The requested path may traverse legitimate symlinks, so the resolved
    directory is opened without O_NOFOLLOW. On Linux, keep Path.resolve's
    existing behavior. Darwin's pathname is read from the pinned directory
    descriptor rather than reconstructed from the request spelling.
    """
    resolved = Path(path).expanduser().resolve(strict=False)
    return oslayer.physical_directory_spelling(resolved)
