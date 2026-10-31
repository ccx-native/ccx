"""`ccx skills install/uninstall` and the automatic refresh after an upgrade."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

REPO = Path(__file__).resolve().parent.parent


def _fake_cli(folder: Path, name: str) -> str:
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / name
    path.write_text("#!/bin/sh\nexit 0\n")
    path.chmod(0o755)
    return str(path)


def _env(tmp_path: Path, *, claude: bool = True, codex: bool = True, **extra) -> dict:
    home = tmp_path / "home"
    home.mkdir(parents=True, exist_ok=True)
    bins = tmp_path / "bin"
    env = {
        "HOME": str(home), "PATH": "/usr/bin:/bin", "LANG": "C.UTF-8",
        "PYTHONPATH": str(REPO), "CCX_ROOT": str(tmp_path / "no-store"),
        "XDG_RUNTIME_DIR": str(tmp_path),
        "CCX_CLAUDE_BIN": _fake_cli(bins, "claude") if claude else str(tmp_path / "none"),
        "CCX_CODEX_BIN": _fake_cli(bins, "codex") if codex else str(tmp_path / "none"),
    }
    env.update(extra)
    return env


def _ccx(env: dict, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run([sys.executable, "-B", "-m", "ccx", *args], env=env,
                          capture_output=True, text=True, timeout=30, cwd="/")


def _home(env) -> Path:
    return Path(env["HOME"])


def test_install_goes_to_claude_and_shared_agents_folders(tmp_path):
    env = _env(tmp_path)
    result = _ccx(env, "skills", "install")
    assert result.returncode == 0, result.stderr
    for folder in (".claude/skills/ccx", ".agents/skills/ccx"):
        skill = _home(env) / folder
        assert (skill / "SKILL.md").read_text() == (REPO / "skills/ccx/SKILL.md").read_text()
        assert (skill / "references" / "controllers.md").is_file()
        assert json.loads((skill / ".ccx-skill.json").read_text())["installedBy"] == "ccx"
        assert f"installed  ~/{folder}" in result.stdout

    again = _ccx(env, "skills", "install")
    assert again.returncode == 0
    assert again.stdout.count("unchanged") == 2


def test_install_picks_folders_from_what_is_installed(tmp_path):
    env = _env(tmp_path, codex=False)
    assert _ccx(env, "skills", "install").returncode == 0
    assert (_home(env) / ".claude/skills/ccx").is_dir()
    assert not (_home(env) / ".agents").exists()

    (_home(env) / ".agents/skills").mkdir(parents=True)   # another agent uses it
    assert _ccx(env, "skills", "install").returncode == 0
    assert (_home(env) / ".agents/skills/ccx/SKILL.md").is_file()

    nothing = _env(tmp_path / "other", claude=False, codex=False)
    result = _ccx(nothing, "skills", "install")
    assert result.returncode == 1
    assert "--dir" in result.stderr
    assert "Traceback" not in result.stderr


def test_claude_config_dir_is_respected(tmp_path):
    config = tmp_path / "claude-config"
    env = _env(tmp_path, codex=False, CLAUDE_CONFIG_DIR=str(config))
    assert _ccx(env, "skills", "install").returncode == 0
    assert (config / "skills/ccx/SKILL.md").is_file()
    assert not (_home(env) / ".claude").exists()


def test_folders_ccx_did_not_create_are_kept_unless_forced(tmp_path):
    env = _env(tmp_path)
    own = _home(env) / ".claude/skills/ccx"
    own.mkdir(parents=True)
    (own / "SKILL.md").write_text("my own copy\n")
    checkout = tmp_path / "checkout-skill"
    checkout.mkdir()
    (checkout / "SKILL.md").write_text("linked\n")
    (_home(env) / ".agents/skills").mkdir(parents=True)
    (_home(env) / ".agents/skills/ccx").symlink_to(checkout)

    result = _ccx(env, "skills", "install")
    assert result.returncode == 0
    assert result.stdout.count("skipped") == 2 and "--force" in result.stdout
    assert (own / "SKILL.md").read_text() == "my own copy\n"
    assert (_home(env) / ".agents/skills/ccx").is_symlink()

    assert _ccx(env, "skills", "uninstall").stdout.count("skipped") == 2
    assert (own / "SKILL.md").read_text() == "my own copy\n"

    forced = _ccx(env, "skills", "install", "--force")
    assert forced.returncode == 0 and forced.stdout.count("replaced") == 2
    assert not (_home(env) / ".agents/skills/ccx").is_symlink()
    assert (checkout / "SKILL.md").read_text() == "linked\n"   # the link target is untouched
    assert "my own copy" not in (own / "SKILL.md").read_text()


def test_dir_install_and_uninstall_touch_only_that_folder(tmp_path):
    env = _env(tmp_path)
    target = tmp_path / "myagent" / "skills"
    assert _ccx(env, "skills", "install", "--dir", str(target)).returncode == 0
    assert (target / "ccx/SKILL.md").is_file()
    assert not (_home(env) / ".claude").exists()

    removed = _ccx(env, "skills", "uninstall", "--dir", str(target))
    assert removed.returncode == 0 and "removed" in removed.stdout
    assert not (target / "ccx").exists()
    assert _ccx(env, "skills", "uninstall", "--dir", str(target)).stdout.strip() == \
        "no ccx-installed skills found"


def test_ordinary_commands_refresh_stale_installed_copies(tmp_path):
    env = _env(tmp_path)
    assert _ccx(env, "skills", "install").returncode == 0
    skill = _home(env) / ".claude/skills/ccx"
    marker = skill / ".ccx-skill.json"
    data = json.loads(marker.read_text())
    marker.write_text(json.dumps({**data, "digest": "from-an-older-ccx"}))
    (skill / "SKILL.md").write_text("old text\n")
    (skill / "stale-file.md").write_text("gone after refresh\n")

    result = _ccx(env, "list")          # fails without a store, refreshes anyway
    assert f"refreshed the ccx skill in {skill.parent}" in result.stderr
    assert (skill / "SKILL.md").read_text() == (REPO / "skills/ccx/SKILL.md").read_text()
    assert not (skill / "stale-file.md").exists()
    assert json.loads(marker.read_text())["digest"] == data["digest"]
    assert "refreshed" not in _ccx(env, "list").stderr      # nothing left to do


def test_refresh_never_creates_or_replaces_unmarked_folders(tmp_path):
    env = _env(tmp_path)
    own = _home(env) / ".claude/skills/ccx"
    own.mkdir(parents=True)
    (own / "SKILL.md").write_text("my own copy\n")
    result = _ccx(env, "list")
    assert "refreshed" not in result.stderr
    assert (own / "SKILL.md").read_text() == "my own copy\n"
    assert not (_home(env) / ".agents").exists()
    assert not [p for p in os.listdir(own.parent) if p.startswith(".")]
