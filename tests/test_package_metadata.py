"""Sanity checks for package metadata consistency."""

import json
import tomllib
from pathlib import Path

REPO = Path(__file__).parent.parent

EXPECTED_SCRIPTS = {"ccx"}


def _pyproject() -> dict:
    with open(REPO / "pyproject.toml", "rb") as f:
        return tomllib.load(f)


def _package_json() -> dict:
    with open(REPO / "package.json") as f:
        return json.load(f)


def test_versions_match():
    py_ver = _pyproject()["project"]["version"]
    npm_ver = _package_json()["version"]
    assert py_ver == npm_ver, (
        f"pyproject.toml version ({py_ver!r}) != package.json version ({npm_ver!r})"
    )


def test_pyproject_scripts_present():
    scripts = set(_pyproject()["project"]["scripts"])
    missing = EXPECTED_SCRIPTS - scripts
    assert not missing, f"pyproject.toml [project.scripts] missing: {sorted(missing)}"


def test_pyproject_no_runtime_deps():
    deps = _pyproject()["project"].get("dependencies", [])
    assert deps == [], f"pyproject.toml should have no runtime dependencies, got: {deps}"


def test_npm_package_ships_every_subpackage_and_every_skill():
    files = _package_json()["files"]
    assert "ccx/**/*.py" in files
    assert "skills/" in files


def test_skills_dir_prefers_bundled_package_data(tmp_path, monkeypatch):
    from ccx.cli import skills

    assert skills.skills_dir() == REPO / "skills"
    package = tmp_path / "site" / "ccx"
    (package / "_skills" / "ccx").mkdir(parents=True)
    # skills.py lives in ccx/cli/ after the package move, while wheel data
    # remains at ccx/_skills (the public package root).
    monkeypatch.setattr(skills, "__file__", str(package / "cli" / "skills.py"))
    assert skills.skills_dir() == package / "_skills"
