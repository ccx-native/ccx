"""Offline structural/root-matrix regressions. No provider, daemon or Mac."""
from __future__ import annotations

import ast
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from ccx.store.root import root_status, root_status_message
from ccx.store.root import canonical_root

REPO = Path(__file__).resolve().parent.parent


def _env(tmp_path: Path, root: Path | None) -> dict[str, str]:
    env = {**os.environ, "PYTHONPATH": str(REPO),
           "CCX_CLAUDE_BIN": "/bin/false", "CCX_CODEX_BIN": "/bin/false",
           "CLAUDE_CONFIG_DIR": str(tmp_path / "fake-native-home")}
    if root is None:
        env.pop("CCX_ROOT", None)
    else:
        env["CCX_ROOT"] = str(root)
    return env


def _cli(env: dict[str, str], *args: str):
    return subprocess.run([sys.executable, "-B", "-m", "ccx", *args],
                          cwd=REPO, env=env, capture_output=True, text=True,
                          timeout=20)


_CLIENT_PROBE = '\nimport json, os\nfrom ccx import CcxClient; from ccx import CodexClient; from ccx import WorkerClient; from ccx import constants; import ccx.notify.paths as notify_paths\nfrom ccx.store.root import root_status; from ccx.store.root import root_status_message\nstatus = root_status(constants.CCX_ROOT)\nresult = {"kind": status.kind, "reason": status.reason,\n          "phase": status.phase, "generation": status.generation,\n          "root": str(constants.CCX_ROOT), "env": os.environ["CCX_ROOT"],\n          "notify": str(notify_paths.root()),\n          "message": None if status.kind == "initialized" and status.phase == "ACTIVE"\n                   else root_status_message(status),\n          "clients": {}}\nfor cls in (CcxClient, CodexClient, WorkerClient):\n    try:\n        cls()\n    except Exception as err:\n        result["clients"][cls.__name__] = {"type": type(err).__name__,\n                                            "exit_code": getattr(err, "exit_code", None),\n                                            "message": str(err)}\n    else:\n        result["clients"][cls.__name__] = {"ok": True}\nprint(json.dumps(result))\n'


def _probe(env: dict[str, str]) -> dict:
    proc = subprocess.run([sys.executable, "-B", "-c", _CLIENT_PROBE],
                          env=env, cwd=REPO, capture_output=True, text=True,
                          timeout=20)
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout)


def _tree(root: Path) -> list[str] | None:
    if root.is_symlink() or not root.is_dir():
        return None
    return sorted(str(path.relative_to(root)) for path in root.rglob("*"))


@pytest.mark.parametrize("case,kind,reason,init_rc", [
    ("missing", "missing", None, 0),
    ("missing-parent", "missing", None, 3),
    ("empty", "empty", None, 0),
    ("symlink-leaf", "unsafe", "symlink-leaf", 3),
    ("dangling-leaf", "unsafe", "symlink-leaf", 3),
    ("wide", "unsafe", "perms", 3),
    ("file", "unsafe", "not-dir", 3),
    ("nonempty", "unsafe", "nonempty-uninitialized", 3),
    ("seal-mismatch", "unsafe", "seal-mismatch", 3),
    ("verify-only", "unsafe", "pointer-invalid", 3),
])
def test_fresh_machine_root_matrix(tmp_path, case, kind, reason, init_rc):
    root = (tmp_path / "no-parent" / "ccx") if case == "missing-parent" else tmp_path / "ccx"
    if case in ("empty", "wide", "nonempty", "seal-mismatch", "verify-only"):
        root.mkdir(mode=0o700)
    if case == "wide":
        root.chmod(0o755)
    if case == "nonempty":
        (root / "existing").write_text("not an empty root")
    if case in ("symlink-leaf", "dangling-leaf"):
        target = tmp_path / "target"
        if case == "symlink-leaf":
            target.mkdir(mode=0o700)
        root.symlink_to(target, target_is_directory=True)
    if case == "file":
        root.write_text("not a directory")
    env = _env(tmp_path, root)
    if case in ("seal-mismatch", "verify-only"):
        created = _cli(env, "init")
        assert created.returncode == 0, created.stderr
        if case == "seal-mismatch":
            pointer = json.loads((root / "identity" / "current.json").read_text())
            manifest = root / "identity" / "generations" / pointer["generation"] / "manifest.json"
            manifest.write_text('{"tampered":true}')
        else:
            current = root / "identity" / "current.json"
            data = json.loads(current.read_text())
            data["phase"] = "VERIFY_ONLY"
            current.write_text(json.dumps(data))
    before = _tree(root)
    status = root_status(root)
    assert (status.kind, status.reason) == (kind, reason)
    message = root_status_message(status)
    listed = _cli(env, "list", "--json")
    if case in {"missing", "empty"}:
        assert listed.returncode == 0 and json.loads(listed.stdout) == []
        assert listed.stderr == f"ccx: created store at {root}\n"
        assert root.stat().st_mode & 0o777 == 0o700
        assert root_status(root).kind == "initialized"
        python = _probe(env)
        assert all(result == {"ok": True} for result in python["clients"].values())
        assert python["kind"] == "initialized"
        assert _cli(env, "init").returncode == 1  # explicit init still refuses duplicate
        assert not (root / "run").exists()
        return
    assert listed.returncode == 3, listed.stderr
    assert (("parent directory" in listed.stderr) if case == "missing-parent"
            else message in listed.stderr)
    python = _probe(env)
    assert (python["kind"], python["reason"]) == (kind, reason)
    assert python["root"] == python["env"] == str(canonical_root(root))
    assert python["notify"] == str(canonical_root(root) / "notify")
    assert message == python["message"]
    for result in python["clients"].values():
        assert result["type"] == "SessionUnavailable", result
        assert result["exit_code"] == 3
        assert (("parent directory" in result["message"]) if case == "missing-parent"
                else message in result["message"])
    assert _tree(root) == before

    initialized = _cli(env, "init")
    assert initialized.returncode == init_rc == 3, initialized.stderr
    assert _tree(root) == before
    if case == "missing-parent":
        assert "parent directory" in initialized.stderr and not root.parent.exists()
    else:
        assert message in initialized.stderr
    if case == "symlink-leaf":
        assert list((tmp_path / "target").iterdir()) == []
    if case == "dangling-leaf":
        assert not (tmp_path / "target").exists()


def test_symlink_ancestor_spellings_are_one_identity(tmp_path):
    physical = tmp_path / "physical"
    physical.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(physical, target_is_directory=True)
    root = physical / "ccx"
    # Explicit physical beats the selected env alias. The reverse works too.
    env_alias = _env(tmp_path, alias / "ccx")
    created = _cli(env_alias, "init", "--root", str(root), "--json")
    assert created.returncode == 0, created.stderr
    assert json.loads(created.stdout)["root"] == str(root)
    for spelling in (physical / "ccx", alias / "ccx"):
        env = _env(tmp_path, spelling)
        result = _cli(env, "list", "--json")
        assert result.returncode == 0 and json.loads(result.stdout) == []
        probe = _probe(env)
        assert probe["root"] == probe["env"] == str(root)
        assert all(v == {"ok": True} for v in probe["clients"].values())
        again = _cli(env, "init", "--root", str(alias / "ccx" if spelling == root else root))
        assert again.returncode == 1 and "already initialized" in again.stderr


def test_init_explicit_precedes_conflicting_env_and_default_is_pinned(tmp_path):
    parent = tmp_path / "selected-env"
    chosen = tmp_path / "explicit-root"
    env = _env(tmp_path, parent)
    made = _cli(env, "init", "--root", str(chosen), "--json")
    assert made.returncode == 0, made.stderr
    assert json.loads(made.stdout)["root"] == str(chosen)
    assert not parent.exists()
    home = tmp_path / "home"
    home.mkdir()
    no_root = _env(tmp_path, None)
    no_root["HOME"] = str(home)
    probe = _probe(no_root)
    assert probe["root"] == probe["env"] == str(home / ".ccx")
    assert (home / ".ccx").is_dir()
    assert root_status(home / ".ccx").kind == "initialized"


_BRIDGE_PROBE = '\nimport json, os, sys\nimport ccx.cli.main as cli; import ccx.controllers.live_vars as live_vars; from ccx import constants; import ccx.notify.paths as notify_paths\nfrom ccx.store.root import root_status\nfrom ccx.store.paths import trust_ledger_path\nfrom ccx.constants import usage_source_path\n\ndef fake(args):\n    print(json.dumps({"root": str(constants.CCX_ROOT), "env": os.environ["CCX_ROOT"],\n                     "notify": str(notify_paths.root()), "trust": str(trust_ledger_path()),\n                     "usage": str(usage_source_path()), "status": root_status(constants.CCX_ROOT).kind,\n                     "arg": args.root, "invoked_as": args.invoked_as}))\nlive_vars.cmd_worker_exec = fake\ncli.main(sys.argv[1:])\n'


def test_old_python_bridge_with_and_without_root_under_conflicting_parent(tmp_path):
    env_root = tmp_path / "parent"
    worker_root = tmp_path / "worker"
    for root in (env_root, worker_root):
        made = _cli(_env(tmp_path, root), "init")
        assert made.returncode == 0, made.stderr
    env = _env(tmp_path, env_root)
    for override, expected, name in (([], env_root, "python"),
                                     (["--root", str(worker_root)], worker_root, "python3")):
        proc = subprocess.run([sys.executable, "-B", "-c", _BRIDGE_PROBE,
                               "_live-vars-exec", *override, "--engine", "claude",
                               "--short", "aaaaaaaa-1234-4000-8000-000000000008",
                               "--invoked-as", name, "--", "-c", "print(1)"],
                              env=env, cwd=REPO, capture_output=True, text=True, timeout=20)
        assert proc.returncode == 0, proc.stderr
        result = json.loads(proc.stdout)
        assert result["root"] == result["env"] == str(expected)
        assert result["notify"] == str(expected / "notify")
        assert result["trust"] == str(expected / "trust-ledger.json")
        assert result["usage"] == str(expected / "usage-source.json")
        assert result["status"] == "initialized"
        assert result["invoked_as"] == name
    assert env["CCX_ROOT"] == str(env_root)  # child pin never changes parent env


def test_read_only_commands_without_a_store_create_nothing(tmp_path):
    root = tmp_path / "missing-store"
    env = _env(tmp_path, root)
    for argv in (("--version",), ("-V",), ("--help",), ("skills",),
                 ("skills", "--help"), ("init", "--help")):
        cmd = _cli(env, *argv)
        assert cmd.returncode == 0, (argv, cmd.stderr)
        assert not root.exists()
    invalid = _cli(env, "init", "--root", "relative-root")
    assert invalid.returncode == 1 and not root.exists()
    # Protocol V1 pure validation is intentionally independent of store status.
    raw = subprocess.run([sys.executable, "-B", "-m", "ccx", "rpc"],
                         env=env, cwd=REPO, input="", capture_output=True,
                         text=True, timeout=20)
    assert raw.returncode == 0 and json.loads(raw.stdout)["ok"] is False
    assert not root.exists()


# These are deliberate structural guards, not tests that count current call sites.
def _source_tree():
    for path in sorted((REPO / "ccx").glob("*.py")):
        yield path, ast.parse(path.read_text())


def test_no_module_scope_ccx_root_import_snapshots():
    for path, tree in _source_tree():
        for node in tree.body:
            if isinstance(node, ast.ImportFrom) and node.module in ("constants", "ccx.constants"):
                assert "CCX_ROOT" not in [item.name for item in node.names], (path, node.lineno)


def test_only_constants_reads_ccx_root_environment():
    for path, tree in _source_tree():
        if path.name == "constants.py":
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Subscript) and ast.unparse(node.value) == "os.environ":
                assert ast.unparse(node.slice) != "'CCX_ROOT'", (path, node.lineno)
            if isinstance(node, ast.Call) and ast.unparse(node.func) in (
                    "os.getenv", "os.environ.get", "os.environ.setdefault", "os.environ.pop"):
                if node.args:
                    assert not isinstance(node.args[0], ast.Constant) or node.args[0].value != "CCX_ROOT", (path, node.lineno)


def test_root_realpath_only_in_canonical_root():
    for path, tree in _source_tree():
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and ast.unparse(node.func) == "os.path.realpath":
                argument = ast.unparse(node.args[0]).lower()
                if "root" in argument or "ccx_root" in argument:
                    assert path.name == "root.py", (path, node.lineno)
    assert canonical_root(Path("~/ccx-root-for-structural-check")).is_absolute()
