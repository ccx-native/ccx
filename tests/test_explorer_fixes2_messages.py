"""Explorer rerun message and file-mode regressions; fake roots only."""
from __future__ import annotations

import hashlib
import json
import os
import stat

from ccx.claude import spawn
from ccx.store.root import root_status, root_status_message
from test_generation_store import A, fixture_root, put
from test_root_model import _cli, _env


def test_different_root_spelling_same_inode_is_actionable_but_refused(tmp_path):
    root = fixture_root(tmp_path)
    alias = tmp_path / "another-spelling"
    alias.symlink_to(root, target_is_directory=True)
    identity = root / "identity"
    pointer = identity / "current.json"
    original = json.loads(pointer.read_text())
    manifest_path = identity / "generations" / original["generation"] / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["rootRealpath"] = str(alias)
    put(manifest_path, manifest)
    original["rootRealpath"] = str(alias)
    original["manifestSha256"] = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    put(pointer, original)
    before = pointer.read_bytes(), manifest_path.read_bytes()

    status = root_status(root)
    assert status.kind == "unsafe"
    assert status.reason == "spelling-alias:" + str(alias)
    assert root_status_message(status) == (
        f"CCX root {root} was initialized as {alias}; "
        "select it with that exact spelling"
    )
    assert (pointer.read_bytes(), manifest_path.read_bytes()) == before


def test_init_warns_conflicting_environment_root_and_keeps_json_stdout(tmp_path):
    selected = tmp_path / "initialized-here"
    environment = tmp_path / "next-command-will-use-this"
    proc = _cli(_env(tmp_path, environment), "init", "--root", str(selected), "--json")
    assert proc.returncode == 0, proc.stderr
    assert json.loads(proc.stdout)["root"] == str(selected)
    assert str(environment) in proc.stderr and str(selected) in proc.stderr
    assert "later commands" in proc.stderr and "CCX_ROOT" in proc.stderr
    assert not environment.exists()


def test_invalid_root_mode_prints_chmod_hint_without_changing_permissions(tmp_path):
    root = tmp_path / "root with space"
    root.mkdir(mode=0o700)
    root.chmod(0o755)
    before = stat.S_IMODE(root.stat().st_mode)
    status = root_status(root)
    message = root_status_message(status)
    assert status.reason == "perms"
    assert "chmod 700 " in message and repr(str(root)) in message
    assert stat.S_IMODE(root.stat().st_mode) == before


def test_worker_with_foreign_root_gets_typed_unavailable_not_traceback(tmp_path):
    root = fixture_root(tmp_path)
    env = _env(tmp_path, root)
    env["CCX_SELF_ENGINE"] = "codex"
    env["CCX_SELF_WORKER_ID"] = A
    proc = _cli(env, "new", "--engine", "codex", "--cwd", str(tmp_path), "--wait")
    assert proc.returncode == 3
    assert "ccx unavailable:" in proc.stderr
    assert "worker identity is not proven in this CCX_ROOT" in proc.stderr
    assert "Traceback" not in proc.stderr


def test_host_log_creation_is_private_even_with_permissive_umask(tmp_path, monkeypatch):
    path = tmp_path / "host.log"
    monkeypatch.setattr(spawn, "host_log_path", lambda _short: path)
    old = os.umask(0o002)
    try:
        with spawn._open_private_host_log(A) as log:
            log.write(b"one\n")
    finally:
        os.umask(old)
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    with spawn._open_private_host_log(A) as log:
        log.write(b"two\n")
    assert path.read_bytes() == b"one\ntwo\n"
    path.chmod(0o664)  # a log an older host created world-readable
    with spawn._open_private_host_log(A) as log:
        log.write(b"three\n")
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
