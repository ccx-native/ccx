"""Install the bundled agent skills into agent skill folders.

A wheel carries the skills as package data in ``ccx/_skills``; a source
checkout or editable install uses the repository's ``skills/`` directory.
Installs are copies with a marker file, so CCX only ever replaces or removes
folders it created, and refreshes them after an upgrade.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import uuid

MARKER = ".ccx-skill.json"
_IGNORE = shutil.ignore_patterns("__pycache__", "*.pyc", MARKER)


def skills_dir() -> Path:
    package = Path(__file__).resolve().parents[1]
    bundled = package / "_skills"
    if bundled.is_dir():
        return bundled
    return package.parent / "skills"


def bundled_skills() -> dict[str, Path]:
    root = skills_dir()
    if not root.is_dir():
        return {}
    return {p.name: p for p in sorted(root.iterdir()) if (p / "SKILL.md").is_file()}


def _digest(folder: Path) -> str:
    h = hashlib.sha256()
    for path in sorted(folder.rglob("*")):
        rel = path.relative_to(folder)
        if not path.is_file() or rel.name == MARKER or "__pycache__" in rel.parts \
                or rel.suffix == ".pyc":
            continue
        h.update(str(rel).encode() + b"\0" + path.read_bytes() + b"\0")
    return h.hexdigest()


def _claude_skills_dir() -> Path:
    config = os.environ.get("CLAUDE_CONFIG_DIR")
    return (Path(config).expanduser() if config else Path.home() / ".claude") / "skills"


def _agents_skills_dir() -> Path:
    return Path.home() / ".agents" / "skills"


def _installed(engine: str) -> bool:
    from ccx.errors import CcxError
    from ccx.system.engine_binary import resolve_engine_binary
    try:
        resolve_engine_binary(engine)
    except CcxError:
        return False
    return True


def default_targets() -> list[Path]:
    """Claude Code's folder, and the shared ~/.agents/skills (Codex and others)."""
    targets = []
    if _installed("claude"):
        targets.append(_claude_skills_dir())
    agents = _agents_skills_dir()
    if _installed("codex") or agents.is_dir():
        targets.append(agents)
    return targets


def _marker(folder: Path) -> dict | None:
    if folder.is_symlink() or not folder.is_dir():
        return None
    try:
        data = json.loads((folder / MARKER).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) and data.get("installedBy") == "ccx" else None


def _remove(folder: Path) -> None:
    if folder.is_symlink() or folder.is_file():
        folder.unlink()
    elif folder.exists():
        shutil.rmtree(folder)


def _copy(source: Path, dest: Path, digest: str) -> None:
    """Copy next to dest, then swap it in, so readers never see half a skill."""
    from ccx.version import ccx_version
    dest.parent.mkdir(parents=True, exist_ok=True)
    staging = dest.parent / f".{dest.name}.ccx-{uuid.uuid4().hex[:8]}"
    shutil.copytree(source, staging, ignore=_IGNORE)
    (staging / MARKER).write_text(json.dumps(
        {"installedBy": "ccx", "version": ccx_version(), "digest": digest},
        indent=2) + "\n", encoding="utf-8")
    old = None
    if dest.exists() or dest.is_symlink():
        old = dest.parent / f".{dest.name}.ccx-old-{uuid.uuid4().hex[:8]}"
        os.rename(dest, old)
    os.rename(staging, dest)
    if old is not None:
        _remove(old)


def install(targets: list[Path], *, force: bool = False) -> list[tuple[str, Path, str]]:
    rows = []
    for target in targets:
        for name, source in bundled_skills().items():
            dest = target / name
            digest = _digest(source)
            marker = _marker(dest)
            if marker is None and (dest.exists() or dest.is_symlink()) and not force:
                rows.append(("skipped", dest, "not created by ccx; use --force to replace"))
                continue
            if marker is not None and marker.get("digest") == digest:
                rows.append(("unchanged", dest, ""))
                continue
            action = ("updated" if marker is not None
                      else "replaced" if dest.exists() or dest.is_symlink() else "installed")
            _copy(source, dest, digest)
            rows.append((action, dest, ""))
    return rows


def uninstall(targets: list[Path]) -> list[tuple[str, Path, str]]:
    rows = []
    for target in targets:
        for name in bundled_skills():
            dest = target / name
            if _marker(dest) is not None:
                _remove(dest)
                rows.append(("removed", dest, ""))
            elif dest.exists() or dest.is_symlink():
                rows.append(("skipped", dest, "not created by ccx"))
    return rows


def refresh_installed() -> None:
    """Bring CCX-installed copies in the default folders up to date.

    Runs on ordinary commands after an upgrade. Only marked folders are
    touched, nothing is created, and any failure leaves things as they were.
    """
    try:
        skills = bundled_skills()
        stale = []
        for target in (_claude_skills_dir(), _agents_skills_dir()):
            for name, source in skills.items():
                marker = _marker(target / name)
                if marker is not None and marker.get("digest") != _digest(source):
                    stale.append((source, target / name))
        if not stale:
            return
        import fcntl
        lock_path = Path(tempfile.gettempdir()) / f"ccx-skills-{os.getuid()}.lock"
        with open(lock_path, "a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            for source, dest in stale:
                digest = _digest(source)
                marker = _marker(dest)
                if marker is not None and marker.get("digest") != digest:
                    _copy(source, dest, digest)
                    sys.stderr.write(f"[ccx] refreshed the {dest.name} skill in {dest.parent}\n")
    except Exception:
        return


def _print(rows) -> None:
    home = str(Path.home())
    for action, path, note in rows:
        shown = str(path)
        if shown == home or shown.startswith(home + os.sep):
            shown = "~" + shown[len(home):]
        print(f"{action:<10} {shown}" + (f"   ({note})" if note else ""))


def _targets(args) -> list[Path]:
    if getattr(args, "dir", None):
        return [Path(args.dir).expanduser().resolve()]
    targets = default_targets()
    if not targets:
        sys.stderr.write(
            "[ccx] skills: neither Claude Code nor Codex was found and ~/.agents/skills "
            "doesn't exist; pass --dir with your agent's skills folder\n")
        raise SystemExit(1)
    return targets


def cmd_skills(args) -> None:
    if not bundled_skills():
        sys.stderr.write(f"[ccx] skills: no bundled skills found in {skills_dir()}\n")
        raise SystemExit(1)
    command = getattr(args, "skills_cmd", None)
    if command == "install":
        _print(install(_targets(args), force=args.force))
    elif command == "uninstall":
        targets = ([Path(args.dir).expanduser().resolve()] if args.dir
                   else [_claude_skills_dir(), _agents_skills_dir()])
        rows = uninstall(targets)
        if rows:
            _print(rows)
        else:
            print("no ccx-installed skills found")
    else:
        args.skills_parser.print_help()
