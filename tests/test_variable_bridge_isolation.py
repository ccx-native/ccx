"""A nested worker owns its Python shell function, never the parent worker's."""
from __future__ import annotations

import json
import os
from pathlib import Path
import shlex
import subprocess
import sys

import pytest

from ccx.codex import protocol as codex_protocol
from ccx.codex import runtime as codex_runtime
from ccx import constants
from ccx.claude.host import session as host
from ccx.controllers import live_vars
from ccx.ops.creation_steps import prepare_worker
from ccx.store.root import require_ccx_active
from test_generation_store import A, B, C, short_root  # noqa: F401

SECOND = "aaaaaaaa-9876-4000-8000-000000000099"
CAPTURE_KEYS = (
    "BASH_ENV", "ZDOTDIR", "CLAUDE_ENV_FILE", "CCX_SELF_WORKER_ID",
    "CCX_SELF_ENGINE", "CCX_SELF_SHORT", "CCX_SELF_EXTRA",
    "CCX_BRIDGE_ORIGINAL_BASH_ENV", "CCX_BRIDGE_ORIGINAL_ZDOTDIR",
    "CCX_BRIDGE_ORIGINAL_CLAUDE_ENV_FILE",
)


def _parent_shell_env(env: dict[str, str], engine: str, cwd: Path):
    """Emulate the real parent tool's Bash shell and capture only test-owned keys."""
    script = (
        ('source "$CLAUDE_ENV_FILE"; ' if engine == "claude" else "")
        + shlex.quote(sys.executable) + " -c "
        + shlex.quote("import os,json; keys=" + repr(CAPTURE_KEYS)
                        + ";print(json.dumps({k:os.environ[k] for k in keys if k in os.environ}))")
    )
    result = subprocess.run(["bash", "-c", script], env=env, cwd=cwd,
                            text=True, capture_output=True, timeout=20)
    assert result.returncode == 0, (result.returncode, result.stderr[-260:])
    return {**env, **json.loads(result.stdout)}


@pytest.mark.parametrize("parent_engine,child_engine", [
    ("claude", "claude"), ("claude", "codex"),
    ("codex", "claude"), ("codex", "codex"),
])
@pytest.mark.parametrize("child_has_variables", [True, False], ids=["bound", "no-vars"])
def test_nested_child_bridge_owns_exact_variables_and_native_fallback(
    short_root, monkeypatch, tmp_path, parent_engine, child_engine, child_has_variables,
):
    # The only originals are explicit, inert test files. Do not introspect the
    # workstation's credential environment or actual shell startup content.
    user_bash = tmp_path / "real-user-bash.sh"
    user_bash.write_text("export CCX_TEST_USER_BASH=present\n")
    user_claude = tmp_path / "real-user-claude.sh"
    user_claude.write_text("export CCX_TEST_USER_CLAUDE=present\n")
    user_zdotdir = tmp_path / "real-user-zdotdir"
    user_zdotdir.mkdir()
    (user_zdotdir / ".zshenv").write_text("export CCX_TEST_USER_ZSH=present\n")
    user_env = {"CCX_ROOT": str(short_root), "HOME": str(tmp_path),
                "PATH": os.pathsep.join([str(Path(sys.executable).parent), os.defpath]),
                "BASH_ENV": str(user_bash), "CLAUDE_ENV_FILE": str(user_claude),
                "ZDOTDIR": str(user_zdotdir), "PYTHONPATH": str(Path(__file__).resolve().parent.parent),
                "LC_ALL": "C.UTF-8", "CCX_SELF_EXTRA": "forged-parent-extra"}
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    monkeypatch.setenv("CCX_ROOT", str(short_root))
    store = require_ccx_active(short_root)
    parent = A if parent_engine == "claude" else B
    child = C if child_engine == "claude" else SECOND
    prepare_worker(store, engine=parent_engine, cwd=str(tmp_path), worker_id=parent)
    prepare_worker(store, engine=child_engine, cwd=str(tmp_path), worker_id=child)
    live_vars.publish_attachment(live_vars.prepare_durable_attachment({"r": 100}),
                                 parent_engine, parent)
    if child_has_variables:
        live_vars.publish_attachment(live_vars.prepare_durable_attachment({"r": 0}),
                                     child_engine, child)

    # Parent's actual generated script is sourced by the real local bash
    # subprocess, exactly as it would be by native provider tool shells.
    with monkeypatch.context() as context:
        context.setattr(os, "environ", user_env)
        if parent_engine == "claude":
            source_env = host.worker_env(user_env, worker_id=parent)
            source_env = live_vars.configure_claude_worker_env(
                source_env, parent, host_pid=os.getpid())
        else:
            source_env = {**user_env,
                          **live_vars.codex_thread_config(parent)["shell_environment_policy"]["set"]}
    inherited = _parent_shell_env(source_env, parent_engine, tmp_path)
    assert inherited["CCX_SELF_WORKER_ID"] == parent

    # Perform precisely the native launcher boundaries the child uses:
    # Claude host worker_env + generated CLAUDE_ENV_FILE, or the shared Codex
    # daemon + native per-full-UUID policy (thread/start and resume alike).
    with monkeypatch.context() as context:
        context.setattr(os, "environ", inherited)
        if child_engine == "claude":
            child_env = host.worker_env(inherited, worker_id=child)
            child_env = live_vars.configure_claude_worker_env(
                child_env, child, host_pid=os.getpid())
        else:
            daemon_env = codex_runtime.daemon_env()
            context.setattr(os, "environ", daemon_env)
            appserver_env = codex_protocol._user_command_env()
            policy = live_vars.codex_thread_config(child)["shell_environment_policy"]["set"]
            child_env = {**appserver_env, **policy}
    assert child_env["CCX_SELF_WORKER_ID"] == child
    assert child_env["CCX_SELF_ENGINE"] == child_engine
    assert "CCX_SELF_SHORT" not in child_env
    assert "CCX_SELF_EXTRA" not in child_env
    if child_engine == "codex":
        # A Claude parent provided a managed CLAUDE_ENV_FILE; the shared
        # Codex app-server must see the original user file, not that shim.
        assert child_env.get("CLAUDE_ENV_FILE") == str(user_claude)
    for name in CAPTURE_KEYS:
        if name.startswith("CCX_BRIDGE_ORIGINAL_"):
            assert name not in child_env, name
    assert child_env.get("ZDOTDIR") != inherited.get("ZDOTDIR") or inherited["ZDOTDIR"] == str(user_zdotdir)
    assert user_bash.read_text() == "export CCX_TEST_USER_BASH=present\n"

    # The child reads a function installed by its OWN shim. With no variables,
    # that function must execute native Python and must not expose parent's r.
    source = (
        "r = 31" if child_has_variables
        else "assert 'r' not in globals(), 'inherited-parent-variable'; print('NATIVE_ONLY')"
    )
    prefix = ('source "$CLAUDE_ENV_FILE"; ' if child_engine == "claude" else "")
    command = prefix + "python3 -c " + shlex.quote(source)
    shell_env = {**child_env, "PATH": os.pathsep.join([
        str(Path(sys.executable).parent), child_env.get("PATH", os.defpath),
    ])}
    result = subprocess.run(["bash", "-c", command], cwd=tmp_path, env=shell_env,
                            text=True, capture_output=True, timeout=30)
    assert result.returncode == 0, (parent_engine, child_engine, result.returncode, result.stderr[-350:])
    assert live_vars.variables_info(parent_engine, parent, "r")["value"] == 100
    if child_has_variables:
        assert live_vars.variables_info(child_engine, child, "r")["value"] == 31
    else:
        assert result.stdout.strip() == "NATIVE_ONLY"
        assert live_vars.variables_info(child_engine, child)["mode"] is None

    # Genuine user shell file is retained; no original parent's generated
    # script may be sourced by this new worker's independent tool shell.
    if child_engine == "claude":
        script = Path(child_env["CLAUDE_ENV_FILE"]).read_text()
        assert str(user_claude) in script
        assert str(user_bash) in child_env.get("BASH_ENV", "")
    else:
        script = Path(child_env["BASH_ENV"]).read_text()
        assert str(user_bash) in script
        zsh = Path(child_env["ZDOTDIR"]) / ".zshenv"
        assert str(user_zdotdir) in zsh.read_text()
    assert "--short " + child in script
    assert "--short " + parent not in script
