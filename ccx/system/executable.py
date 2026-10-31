from __future__ import annotations

import os
import pathlib
import shutil
import sys


def _normalize_invoked_path(path: str) -> str:
    # Python can preserve an invoked path like //./ccx.py in __main__.__file__.
    # Treat that as cwd-relative; otherwise os.path.abspath preserves the
    # leading double slash and hooks later try to execute /ccx.py.
    if path.startswith("//./"):
        path = "." + path[3:]
    return os.path.abspath(path)


def current_executable_path() -> str:
    """Absolute script path suitable for re-exec and Claude hook commands."""
    main_file = getattr(sys.modules.get("__main__"), "__file__", None)
    if main_file and _looks_like_ccx_entrypoint(main_file):
        return _normalize_invoked_path(main_file)
    package_main = _package_main_path()
    if package_main is not None:
        return str(package_main)
    # Fallback for unusual source layouts where the package file is not
    # discoverable: keep the historic behavior.
    if main_file:
        return _normalize_invoked_path(main_file)
    arg0 = sys.argv[0] if sys.argv else "ccx.py"
    if os.path.isabs(arg0) or os.sep in arg0:
        return _normalize_invoked_path(arg0)
    found = shutil.which(arg0)
    return _normalize_invoked_path(found or arg0)


def _looks_like_ccx_entrypoint(path: str) -> bool:
    p = pathlib.Path(_normalize_invoked_path(path))
    if p.name == "ccx.py":
        return True
    return p.name == "__main__.py" and p.parent.name == "ccx"


def _package_main_path() -> pathlib.Path | None:
    pkg = sys.modules.get("ccx")
    pkg_file = getattr(pkg, "__file__", None)
    if not pkg_file:
        return None
    candidate = pathlib.Path(pkg_file).resolve().parent / "__main__.py"
    return candidate if candidate.exists() else None
