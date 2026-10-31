"""Full-UUID worker self environment, no legacy short or native alias."""
from __future__ import annotations
import ccx.codex.daemon.native as daemon_native

import os
from pathlib import Path

import pytest

from test_generation_store import A, B, C, fixture_root
from ccx import constants
from ccx.claude.host import session as host
from ccx.store import records
from ccx.errors import SessionUnavailable
from ccx.ops.creation_steps import prepare_worker
from ccx.store.root import require_ccx_active


@pytest.fixture
def managed(tmp_path, monkeypatch):
    root = fixture_root(tmp_path)
    store = require_ccx_active(root)
    prepare_worker(store, engine="claude", cwd=str(tmp_path), worker_id=A)
    prepare_worker(store, engine="codex", cwd=str(tmp_path), worker_id=B)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    for field in (
        constants.SELF_SHORT_ENV, constants.SELF_WORKER_ID_ENV,
        constants.SELF_ENGINE_ENV, "CLAUDE_CODE_SESSION_ID",
        "CODEX_THREAD_ID",
    ):
        monkeypatch.delenv(field, raising=False)
    return root, store


def test_exact_parent_full_worker_env_disambiguates_cross_engine_prefix(managed, monkeypatch):
    root, store = managed
    monkeypatch.setenv(constants.SELF_WORKER_ID_ENV, A)
    monkeypatch.setenv(constants.SELF_ENGINE_ENV, "claude")
    assert records.spawned_by_from_env() == {
        "kind": "ccx-worker", "engine": "claude", "workerId": A,
    }
    monkeypatch.setenv(constants.SELF_WORKER_ID_ENV, B)
    monkeypatch.setenv(constants.SELF_ENGINE_ENV, "codex")
    assert records.spawned_by_from_env() == {
        "kind": "ccx-worker", "engine": "codex", "workerId": B,
    }
    assert A[:8] == B[:8] == "aaaaaaaa"
    assert not (root / "sessions").exists()


@pytest.mark.parametrize("env", [
    {"CCX_SELF_SHORT": "aaaaaaaa"},
    {"CCX_SELF_SHORT": "aaaaaaaa",
     "CCX_SELF_WORKER_ID": A, "CCX_SELF_ENGINE": "claude"},
    {"CCX_SELF_WORKER_ID": A},
    {"CCX_SELF_ENGINE": "codex"},
    {"CCX_SELF_WORKER_ID": "aaaaaaaa", "CCX_SELF_ENGINE": "claude"},
    {"CCX_SELF_WORKER_ID": A, "CCX_SELF_ENGINE": "codex"},
    {"CCX_SELF_WORKER_ID": C, "CCX_SELF_ENGINE": "claude"},
])
def test_stale_or_unowned_self_environment_refuses_without_writes(managed, monkeypatch, env):
    root, store = managed
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    before = sorted(str(p.relative_to(root)) for p in root.rglob("*"))
    with pytest.raises(SessionUnavailable):
        records.spawned_by_from_env()
    assert before == sorted(str(p.relative_to(root)) for p in root.rglob("*"))
    assert not (root / "sessions").exists()


def test_native_parent_only_is_forensic_not_ccx_target(managed, monkeypatch):
    root, store = managed
    monkeypatch.setenv("CLAUDE_CODE_SESSION_ID", "provider-session-unverified")
    assert records.spawned_by_from_env() == {
        "kind": "provider-native-unknown", "engine": "claude",
        "nativeId": "provider-session-unverified",
    }
    monkeypatch.delenv("CLAUDE_CODE_SESSION_ID")
    monkeypatch.setenv("CODEX_THREAD_ID", "other-thread-unverified")
    assert records.spawned_by_from_env() == {
        "kind": "provider-native-unknown", "engine": "codex",
        "nativeId": "other-thread-unverified",
    }
    monkeypatch.delenv("CODEX_THREAD_ID")
    assert records.spawned_by_from_env() is None
    assert not (root / "codex").exists()


def test_claude_host_env_scrubs_inherited_native_and_short_stamp(managed):
    root, store = managed
    raw = {
        "CCX_SELF_SHORT": "aaaaaaaa",
        "CCX_SELF_WORKER_ID": B,
        "CCX_SELF_ENGINE": "codex",
        "CLAUDE_CODE_SESSION_ID": "other-native",
        "CLAUDE_CODE_ENTRYPOINT": "not-canonical",
        "CLEAN": "retained",
    }
    result = host.worker_env(raw, worker_id=A, engine="claude")
    assert result[constants.SELF_WORKER_ID_ENV] == A
    assert result[constants.SELF_ENGINE_ENV] == "claude"
    assert constants.SELF_SHORT_ENV not in result
    assert "CLAUDE_CODE_SESSION_ID" not in result
    assert "CLAUDE_CODE_ENTRYPOINT" not in result
    assert result["CLEAN"] == "retained"
    assert result["CLAUDE_CODE_FORCE_SESSION_PERSISTENCE"] == "1"
    assert not (root / "sock").exists()


def test_claude_host_env_rejects_legacy_short_unowned_or_cross_engine(managed):
    root, store = managed
    for kwargs in (
        {"short": "aaaaaaaa"},
        {"worker_id": "aaaaaaaa"},
        {"worker_id": C},
        {"worker_id": B, "engine": "codex"},
        {"worker_id": B, "engine": "claude"},
    ):
        with pytest.raises(SessionUnavailable):
            host.worker_env({"CLEAN": "ok"}, **kwargs)
    assert not (root / "sock").exists()
    assert not (root / "sessions").exists()


def test_claude_host_env_unstamped_scrubs_old_ids_without_writes(managed):
    root, _ = managed
    result = host.worker_env({
        "CCX_SELF_SHORT": "stale",
        "CCX_SELF_WORKER_ID": "stale",
        "CCX_SELF_ENGINE": "stale",
        "CLAUDE_CODE_SESSION_ID": "native",
    })
    assert constants.SELF_SHORT_ENV not in result
    assert constants.SELF_WORKER_ID_ENV not in result
    assert constants.SELF_ENGINE_ENV not in result
    assert "CLAUDE_CODE_SESSION_ID" not in result
    assert result["CLAUDE_CODE_FORCE_SESSION_PERSISTENCE"] == "1"
    assert not (root / "run").exists()


def test_host_full_id_env_missing_pointer_no_write(tmp_path, monkeypatch):
    root = tmp_path / "empty"
    root.mkdir(mode=0o700)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    with pytest.raises(SessionUnavailable):
        host.worker_env({"CLEAN": "ok"}, worker_id=A)
    assert not list(root.iterdir())



def test_orchestrator_managed_claude_attested_full_UUID_and_native_provenance(managed, monkeypatch):
    from ccx.controllers.orchestrator import current_caller, require_notify_parent
    from ccx.errors import InvalidRequest
    root, store = managed
    sid = store.record(A)["native"]["sessionId"]
    monkeypatch.setenv(constants.SELF_WORKER_ID_ENV, A)
    monkeypatch.setenv(constants.SELF_ENGINE_ENV, "claude")
    monkeypatch.setenv("CLAUDE_CODE_SESSION_ID", sid)
    caller = current_caller()
    assert caller.kind == "claude"
    assert caller.worker_id == A and caller.managed
    assert caller.native_id == sid
    assert caller.supports_parent_notify is False
    with pytest.raises(InvalidRequest, match="Claude and mixed origins are not supported"):
        require_notify_parent()


def test_orchestrator_managed_codex_no_thread_never_infers_provider_id(managed, monkeypatch):
    from ccx.controllers.orchestrator import current_caller
    root, store = managed
    monkeypatch.setenv(constants.SELF_WORKER_ID_ENV, B)
    monkeypatch.setenv(constants.SELF_ENGINE_ENV, "codex")
    caller = current_caller()
    assert caller.kind == "codex" and caller.worker_id == B
    assert caller.native_id is None
    assert caller.managed and not caller.supports_parent_notify
    monkeypatch.setenv("CODEX_THREAD_ID", "unrelated-native-thread")
    with pytest.raises(SessionUnavailable, match="Codex caller does not match"):
        current_caller()


def test_orchestrator_stale_short_refuses_even_if_native_id_looks_valid(managed, monkeypatch):
    from ccx.controllers.orchestrator import current_caller
    monkeypatch.setenv(constants.SELF_SHORT_ENV, "aaaaaaaa")
    monkeypatch.setenv("CODEX_THREAD_ID", "native-other-engine")
    with pytest.raises(SessionUnavailable, match="CCX_SELF_SHORT"):
        current_caller()



def test_shared_codexd_and_notifyd_drop_all_managed_caller_stamps(managed, monkeypatch):
    from ccx.codex import runtime as codex_runtime
    import importlib
    notify_module = importlib.import_module('ccx.notify.daemon')
    root, store = managed
    for k, v in (
        ("CCX_SELF_SHORT", "aaaaaaaa"),
        ("CCX_SELF_WORKER_ID", A),
        ("CCX_SELF_ENGINE", "claude"),
        ("CLAUDE_CODE_SESSION_ID", "forged"),
        ("CODEX_THREAD_ID", "forged-thread"),
    ):
        monkeypatch.setenv(k, v)
    for build in (codex_runtime.daemon_env, notify_module._daemon_env):
        built = build()
        assert not any(k in built for k in (
            "CCX_SELF_SHORT", "CCX_SELF_WORKER_ID", "CCX_SELF_ENGINE",
            "CLAUDE_CODE_SESSION_ID", "CODEX_THREAD_ID",
        ))
        assert "PYTHONPATH" in built
    assert not (root / "pid").exists()
    assert not (root / "sock").exists()


def test_codex_shared_appserver_scrubs_parent_identity_not_user_environment(
    managed, monkeypatch,
):
    from ccx.codex import runtime as codex_runtime
    from ccx.codex import protocol as codex_protocol

    for name in (
        "CCX_SELF_SHORT", "CCX_SELF_WORKER_ID", "CCX_SELF_ENGINE",
        "CCX_SELF_FOREIGN", "CCX_SPAWN_SCOPE", "CLAUDECODE",
        "CLAUDE_CODE_SESSION_ID", "CLAUDE_CODE_CHILD_SESSION",
        "CLAUDE_CODE_ENTRYPOINT", "CLAUDE_CODE_FORCE_SESSION_PERSISTENCE",
        "CODEX_THREAD_ID",
    ):
        monkeypatch.setenv(name, "synthetic-stale-parent")
    monkeypatch.setenv("BASH_ENV", "/tmp/synthetic-parent-bash-env")
    monkeypatch.setenv("ZDOTDIR", "/tmp/synthetic-user-zdotdir")
    monkeypatch.setenv("HTTP_PROXY", "synthetic-proxy")
    monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", "synthetic-not-real")
    monkeypatch.setenv("PATH", os.environ["PATH"] + os.pathsep + "/synthetic-tool")

    daemon = codex_runtime.daemon_env()
    with monkeypatch.context() as temporary:
        temporary.setattr(os, "environ", daemon)
        appserver = codex_protocol._user_command_env()

    for name in (
        "CCX_SELF_SHORT", "CCX_SELF_WORKER_ID", "CCX_SELF_ENGINE",
        "CCX_SELF_FOREIGN", "CCX_SPAWN_SCOPE", "CLAUDECODE",
        "CLAUDE_CODE_SESSION_ID", "CLAUDE_CODE_CHILD_SESSION",
        "CLAUDE_CODE_ENTRYPOINT", "CLAUDE_CODE_FORCE_SESSION_PERSISTENCE",
        "CODEX_THREAD_ID",
    ):
        assert name not in daemon
        assert name not in appserver
    assert "BASH_ENV" not in appserver
    for env in (daemon, appserver):
        assert env["ZDOTDIR"] == "/tmp/synthetic-user-zdotdir"
        assert "CLAUDE_CODE_OAUTH_TOKEN" in env
        assert "HTTP_PROXY" in env
        assert env["PATH"].endswith("/synthetic-tool")


def test_codex_original_thread_policy_preserves_external_user_shell_not_short(managed, monkeypatch, tmp_path):
    from ccx.codex.daemon import daemon as codexd
    from ccx.controllers import live_vars
    root, store = managed
    external = tmp_path / "user-shell-script.sh"
    external.write_text("export USER_SHELL_MARKER=1\n")
    monkeypatch.setenv("BASH_ENV", str(external))
    user_zdotdir = tmp_path / "user zsh dotfiles"
    user_zdotdir.mkdir()
    monkeypatch.setenv("ZDOTDIR", str(user_zdotdir))
    original = external.read_bytes()
    config = daemon_native._original_caller_config(B)["shell_environment_policy"]
    expected = (store.pointer.generation_root / "variables" / "env" / "codex" /
                (B + ".sh"))
    expected_zdotdir = expected.parent / "zsh" / B
    assert "inherit" not in config
    assert live_vars.codex_thread_config(B) == {"shell_environment_policy": config}
    assert config["set"] == {
        "CCX_SELF_WORKER_ID": B, "CCX_SELF_ENGINE": "codex",
        "CCX_ROOT": str(root),
        "BASH_ENV": str(expected),
        "ZDOTDIR": str(expected_zdotdir),
    }
    source = expected.read_text()
    assert str(external) in source and "--engine codex --short " + B in source
    assert "CCX_SELF_SHORT" not in source and external.read_bytes() == original
    assert expected_zdotdir.is_dir()
    assert (expected_zdotdir / ".zshenv").is_file()
    assert str(user_zdotdir) in (expected_zdotdir / ".zshenv").read_text()
    # An inherited CCX-generated script must never chain its source worker into
    # an independent per-thread caller, unlike a genuine external user file.
    prepare_worker(store, engine="codex", cwd=str(tmp_path), worker_id=C)
    monkeypatch.setenv("BASH_ENV", str(expected))
    monkeypatch.setenv("ZDOTDIR", str(expected_zdotdir))
    child = daemon_native._original_caller_config(C)["shell_environment_policy"]["set"]["BASH_ENV"]
    child_source = Path(child).read_text()
    child_zdotdir = expected.parent / "zsh" / C
    assert child_zdotdir != expected_zdotdir
    assert str(expected_zdotdir) not in (child_zdotdir / ".zshenv").read_text()
    assert child != str(expected) and str(expected) not in child_source
    assert "--engine codex --short " + C in child_source
    # A generated full-UUID bridge from an unrelated root must not be sourced.
    foreign = (tmp_path / "foreign-root" / "variables" / "env" /
               "codex" / (B + ".sh"))
    foreign.parent.mkdir(parents=True)
    foreign.write_text("export USER_SHELL_MARKER=foreign\n")
    monkeypatch.setenv("BASH_ENV", str(foreign))
    foreign_zsh = foreign.parent / "zsh" / B
    monkeypatch.setenv("ZDOTDIR", str(foreign_zsh))
    daemon_native._original_caller_config(C)
    assert str(foreign) not in Path(child).read_text()
    assert str(foreign_zsh) not in (child_zdotdir / ".zshenv").read_text()

    with pytest.raises(Exception):
        daemon_native._original_caller_config(B[:8])
    assert not (root / "variables").exists()
    assert not (root / "run").exists()


def test_codex_shim_zdotdir_scrub_is_scoped_to_managed_paths(managed, monkeypatch, tmp_path):
    from ccx.codex import runtime as codex_runtime
    from ccx.codex import protocol as codex_protocol
    from ccx.controllers import live_vars
    from ccx.codex.worker_environment import scrub_shared_codex_identity
    root, store = managed
    shim = live_vars.codex_thread_config(B)["shell_environment_policy"]["set"]["ZDOTDIR"]
    foreign = str(tmp_path / "retired" / "variables" / "env" / "codex" / "zsh" / B)
    genuine = str(tmp_path / "genuine zsh configuration")
    for value in (shim, foreign):
        assert live_vars._managed_zdotdir(value)
        assert "ZDOTDIR" not in live_vars.sanitize_inherited_shell_bridge_env({"ZDOTDIR": value})
        assert "ZDOTDIR" not in scrub_shared_codex_identity({"ZDOTDIR": value})
        monkeypatch.setenv("ZDOTDIR", value)
        assert "ZDOTDIR" not in codex_runtime.daemon_env()
        assert "ZDOTDIR" not in codex_protocol._user_command_env()
    assert live_vars.sanitize_inherited_shell_bridge_env({"ZDOTDIR": genuine}) == {
        "ZDOTDIR": genuine,
    }
    monkeypatch.setenv("ZDOTDIR", genuine)
    assert codex_runtime.daemon_env()["ZDOTDIR"] == genuine
    assert codex_protocol._user_command_env()["ZDOTDIR"] == genuine


def test_codex_original_policy_absent_active_generation_denies_without_dirs(tmp_path, monkeypatch):
    from ccx.codex.daemon import daemon as codexd
    from ccx.store.root import GenerationUnavailable
    root = tmp_path / "empty-root"
    root.mkdir(mode=0o700)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    with pytest.raises(GenerationUnavailable):
        daemon_native._original_caller_config(B)
    assert not list(root.iterdir())
