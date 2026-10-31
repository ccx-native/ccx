"""Offline CLI regressions for symlinked ancestors (macOS /tmp and $TMPDIR)."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from ccx.store.store import fingerprint_v2
from ccx.store.root import initialize_empty_root, require_ccx_active

REPO = Path(__file__).resolve().parent.parent
WORKER_ID = "cccccccc-1234-4000-8000-000000000008"


def _run(env: dict[str, str], *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-B", "-m", "ccx", *args],
        env=env, capture_output=True, text=True, timeout=20,
    )


def _paths(tmp_path: Path) -> tuple[Path, Path]:
    physical = tmp_path / "physical"
    physical.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(physical, target_is_directory=True)
    return physical, alias


def _env(tmp_path: Path, root: Path) -> dict[str, str]:
    profile = tmp_path / "native-profile"
    profile.mkdir(exist_ok=True)
    return {
        **os.environ, "CCX_ROOT": str(root),
        "CLAUDE_CONFIG_DIR": str(profile),
        "PYTHONPATH": str(REPO),
        "CCX_CLAUDE_BIN": str(tmp_path / "no-claude"),
        "CCX_CODEX_BIN": str(tmp_path / "no-codex"),
    }


@pytest.mark.parametrize("env_alias,arg_alias", [
    (True, True), (False, False), (True, False), (False, True),
])
def test_init_accepts_both_spellings_of_symlinked_ancestor(
    tmp_path, env_alias, arg_alias,
):
    physical, alias = _paths(tmp_path)
    root = physical / "empty-root"
    root.mkdir(mode=0o700)
    env_root = (alias if env_alias else physical) / root.name
    arg_root = (alias if arg_alias else physical) / root.name

    result = _run(_env(tmp_path, env_root), "init", "--root", str(arg_root), "--json")
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["root"] == str(root)
    assert require_ccx_active(arg_root).root == root


def test_init_does_not_follow_symlinked_root_itself(tmp_path):
    physical, _alias = _paths(tmp_path)
    root = physical / "real-root"
    root.mkdir(mode=0o700)
    linked_root = tmp_path / "linked-root"
    linked_root.symlink_to(root, target_is_directory=True)

    result = _run(_env(tmp_path, linked_root), "init", "--root", str(linked_root))
    assert result.returncode != 0
    assert not list(root.iterdir())


def test_skills_version_and_help_work_without_store_or_root(tmp_path):
    _physical, alias = _paths(tmp_path)
    missing_root = alias / "not-created"
    env = _env(tmp_path, missing_root)

    target = tmp_path / "agent-skills"
    skills = _run(env, "skills", "install", "--dir", str(target))
    assert skills.returncode == 0, skills.stderr
    assert (target / "ccx" / "SKILL.md").is_file()

    for argv in (("--version",), ("-V",), ("--help",), ("skills", "--help"),
                 ("init", "--help")):
        proc = _run(env, *argv)
        assert proc.returncode == 0, (argv, proc.stderr)
        assert proc.stdout
    assert not missing_root.exists()


def test_list_cwd_and_prefix_filters_resolve_symlinked_ancestors(tmp_path, monkeypatch):
    physical, alias = _paths(tmp_path)
    real_root = physical / "ccx"
    real_root.mkdir(mode=0o700)
    initialize_empty_root(real_root)
    project = physical / "project"
    cwd = project / "child"
    cwd.mkdir(parents=True)
    store = require_ccx_active(real_root)
    claim = store.reserve_creation(
        WORKER_ID, engine="claude",
        fingerprint=fingerprint_v2(engine="claude", cwd=str(cwd), create_args={}),
    )
    store.publish_first_record(claim, {
        "workerId": WORKER_ID, "engine": "claude", "name": "fake no host",
        "native": {"sessionId": WORKER_ID}, "terminalOnly": False, "cwd": str(cwd),
    })
    env = _env(tmp_path, alias / "ccx")

    for spelling in (alias, physical):
        for flag, target in (("--cwd", spelling / "project" / "child"),
                             ("--cwd-prefix", spelling / "project")):
            result = _run(env, "list", flag, str(target), "--json")
            assert result.returncode == 0, result.stderr
            rows = json.loads(result.stdout)
            assert len(rows) == 1 and rows[0]["worker_id"] == WORKER_ID
            assert rows[0]["cwd"] == str(cwd)

    # The direct Python clients must use the same filter spelling as the CLI.
    from ccx import constants
    from ccx.client import CcxClient
    from ccx.workers import WorkerClient
    monkeypatch.setattr(constants, "CCX_ROOT", real_root)
    assert [r.worker_id for r in CcxClient().list(
        cwd=str(alias / "project" / "child"),
    )] == [WORKER_ID]
    assert [r.worker_id for r in CcxClient().list(
        cwd_prefix=str(alias / "project"),
    )] == [WORKER_ID]
    assert [r["worker_id"] for r in WorkerClient().list(
        cwd=str(alias / "project" / "child"),
    )["workers"]] == [WORKER_ID]


def test_notify_root_resolves_ancestor_spelling(tmp_path, monkeypatch):
    from ccx.notify import paths as notify_paths
    physical, alias = _paths(tmp_path)
    from ccx import constants
    monkeypatch.setattr(constants, "CCX_ROOT", constants.canonical_root(alias / "ccx"))
    assert notify_paths.root() == physical / "ccx" / "notify"


def test_untrust_clears_exact_physical_ledger_path_with_symlink_alias(tmp_path):
    physical, alias = _paths(tmp_path)
    real_root = physical / "ccx"
    real_root.mkdir(mode=0o700)
    initialize_empty_root(real_root)
    real_cwd = physical / "project" / "child"
    real_cwd.mkdir(parents=True)
    env = _env(tmp_path, alias / "ccx")
    cfg = tmp_path / "native-profile" / ".claude.json"

    for spelling in (alias, physical):
        requested = str(spelling / "project" / "child")
        # Exercise the setup writer with a fake/native-free caller, never Claude.
        trusted = subprocess.run(
            [sys.executable, "-B", "-c",
             'import sys; from ccx.claude.settings import ensure_trusted_paths; assert ensure_trusted_paths([sys.argv[1]])',
             requested],
            env=env, text=True, capture_output=True, timeout=20,
        )
        assert trusted.returncode == 0, trusted.stderr
        assert set(json.loads(cfg.read_text())["projects"]) == {str(real_cwd)}
        result = _run(env, "untrust", requested, "--json")
        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout)["paths"] == [
            {"path": str(real_cwd), "action": "removed-entry"},
        ]
        assert str(real_cwd) not in json.loads(cfg.read_text())["projects"]


def test_missing_root_list_initializes_without_unsafe_diagnostic(tmp_path):
    missing_root = tmp_path / "not-created"
    env = _env(tmp_path, missing_root)
    proc = _run(env, "list", "--json")
    assert proc.returncode == 0 and json.loads(proc.stdout) == []
    assert proc.stderr == f"ccx: created store at {missing_root}\n"
    assert missing_root.stat().st_mode & 0o777 == 0o700
    assert _run(env, "list", "--json").stderr == ""
    assert _run(env, "init").returncode == 1


def test_init_creates_missing_private_root_leaf_once(tmp_path):
    root = tmp_path / "fresh-root"
    env = _env(tmp_path, root)
    first = _run(env, "init", "--root", str(root))
    assert first.returncode == 0, first.stderr
    assert root.stat().st_mode & 0o777 == 0o700
    assert _run(env, "list").returncode == 0

    again = _run(env, "init", "--root", str(root))
    assert again.returncode == 1
    assert "already initialized" in again.stderr


def test_init_never_creates_missing_parents(tmp_path):
    root = tmp_path / "missing-parent" / "root"
    proc = _run(_env(tmp_path, root), "init", "--root", str(root))
    assert proc.returncode == 3
    assert "parent directory" in proc.stderr
    assert not root.parent.exists()
