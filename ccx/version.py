"""The running CCX version, without importing the CLI parser."""

from __future__ import annotations


def ccx_version() -> str:
    """The running ccx's version.

    A source checkout (including an editable install) answers from its own
    pyproject.toml, so a version bump shows up without reinstalling and an
    older installed ccx-native in the same interpreter cannot mask it; an
    installed wheel answers from its metadata.
    """
    import pathlib
    import tomllib
    from importlib import metadata

    pyproject = pathlib.Path(__file__).resolve().parent.parent / "pyproject.toml"
    try:
        with pyproject.open("rb") as f:
            project = tomllib.load(f).get("project") or {}
        if project.get("name") == "ccx-native" and isinstance(project.get("version"), str):
            return project["version"]
    except (OSError, tomllib.TOMLDecodeError):
        pass
    try:
        return metadata.version("ccx-native")
    except metadata.PackageNotFoundError:
        return "unknown"
