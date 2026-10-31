"""Real nested zsh policy: one worker's shell retains its own bridge."""
from __future__ import annotations

import os
from pathlib import Path
import shlex
import subprocess
import sys

import pytest

from ccx.controllers import live_vars
from test_codex_zsh_vars import _policy, _run_zsh, worker_root, zsh  # noqa: F401
from test_generation_store import B


def _nested(zsh: str, option: str, code: str) -> str:
    return f"{shlex.quote(zsh)} {option} {shlex.quote(code)}"


@pytest.mark.parametrize("option", ["-c", "-lc"])
def test_nested_login_and_nonlogin_zsh_update_same_durable_owner(
    worker_root, tmp_path, zsh, option,
):
    live_vars.publish_attachment(
        live_vars.prepare_durable_attachment({"r": 10, "proof": []}), "codex", B,
    )
    policy = _policy()
    code = "python3 -c " + shlex.quote("r += 3; proof.append('zsh-' + " + repr(option) + ")")
    result = _run_zsh(zsh, policy, _nested(zsh, option, code),
                      home=tmp_path, cwd=tmp_path, options="-c")
    assert result.returncode == 0, (result.stdout, result.stderr)
    assert live_vars.variables_info("codex", B, "r")["value"] == 13
    assert live_vars.variables_info("codex", B, "proof")["value"] == ["zsh-" + option]


@pytest.mark.parametrize("option", ["-c", "-lc"])
def test_nested_login_and_nonlogin_zsh_update_controller_live_objects(
    worker_root, tmp_path, zsh, option,
):
    box = {"box": {"n": 1}, "result": None}
    owner = live_vars.LiveOwner()
    try:
        live_vars.publish_attachment(
            live_vars.PreparedAttachment("live", sorted(box), mapping=box, owner=owner),
            "codex", B,
        )
        code = "python3 -c " + shlex.quote(
            "box['n'] += 5; result = {'verified': box['n']}")
        result = _run_zsh(zsh, _policy(), _nested(zsh, option, code),
                          home=tmp_path, cwd=tmp_path, options="-c")
        assert result.returncode == 0, (result.stdout, result.stderr)
        assert box == {"box": {"n": 6}, "result": {"verified": 6}}
    finally:
        owner.close()


def test_nested_zsh_keeps_bridge_and_native_user_startup_stages_once(
    worker_root, tmp_path, monkeypatch, zsh,
):
    user = tmp_path / "user's private zsh files"
    user.mkdir()
    home = tmp_path / "synthetic-home"
    home.mkdir()
    marker = tmp_path / "user-startup-stages.txt"
    for suffix in (".zshenv", ".zprofile", ".zshrc", ".zlogin", ".zlogout"):
        (user / suffix).write_text(
            f"print -r -- {shlex.quote(suffix)} >> {shlex.quote(str(marker))}\n")
    # All five genuine user startup files are immutable inputs to CCX.
    # The *user* files may write their normal stage marker, but CCX only
    # publishes new shim files inside its own private generation directory.
    originals = {item.name: item.read_bytes() for item in user.iterdir()}
    monkeypatch.setenv("ZDOTDIR", str(user))
    policy = _policy()
    managed = policy["set"]["ZDOTDIR"]
    inner_login = ("[[ $ZDOTDIR == " + shlex.quote(managed) + " ]] && "
                   "typeset -f python3 >/dev/null && print -r -- NESTED_LOGIN_BRIDGED")
    inner_plain = ("[[ $ZDOTDIR == " + shlex.quote(managed) + " ]] && "
                   "typeset -f python3 >/dev/null && print -r -- NESTED_PLAIN_BRIDGED")
    command = (_nested(zsh, "-lc", inner_login) + "; "
               + _nested(zsh, "-c", inner_plain))
    result = _run_zsh(zsh, policy, command, home=home, cwd=tmp_path, options="-c")
    assert result.returncode == 0, (result.stdout, result.stderr)
    assert result.stdout.splitlines() == ["NESTED_LOGIN_BRIDGED", "NESTED_PLAIN_BRIDGED"]
    # Outer -c reads .zshenv; the nested -lc has env/profile/login/logout;
    # nested -c has env alone. Each requested native user startup file runs
    # exactly once per shell, with no duplicate sourcing by the CCX shim.
    assert marker.read_text().splitlines() == [
        ".zshenv", ".zshenv", ".zprofile", ".zlogin", ".zshenv",
    ]
    assert sorted(p.name for p in Path(managed).iterdir()) == [
        ".zlogin", ".zlogout", ".zprofile", ".zshenv", ".zshrc",
    ]
    assert {item.name: item.read_bytes() for item in user.iterdir()} == originals
    assert not list(home.iterdir()), "CCX unexpectedly wrote a user HOME file"


def test_missing_native_python_does_not_install_a_broken_python_bridge(
    worker_root, tmp_path, zsh,
):
    binary = tmp_path / "only-python3"
    binary.mkdir()
    (binary / "python3").symlink_to(sys.executable)
    policy = _policy()
    env = {"HOME": str(tmp_path), "PATH": str(binary), "CCX_ROOT": str(worker_root),
           "PYTHONPATH": str(Path(__file__).resolve().parent.parent),
           **policy["set"]}
    code = "if typeset -f python >/dev/null 2>&1; then print BROKEN; exit 1; fi; "
    code += "if whence -p python >/dev/null 2>&1; then print PRESENT; exit 2; fi; "
    code += "if typeset -f python3 >/dev/null 2>&1; then print PYTHON3_BRIDGED; fi"
    result = subprocess.run([zsh, "-c", code], env=env, cwd=tmp_path,
                            text=True, capture_output=True, timeout=20)
    assert result.returncode == 0, (result.stdout, result.stderr)
    assert result.stdout.strip() == "PYTHON3_BRIDGED"
    # When no native executable named `python` exists, CCX must NOT invent
    # one that aliases python3. The shell's ordinary command-not-found is 127.
    native= subprocess.run([zsh, "-c", "python -c 'pass'"], env=env,
                           cwd=tmp_path, text=True, capture_output=True,
                           timeout=20)
    assert native.returncode == 127


def test_nested_zsh_parent_cannot_give_its_bridge_to_new_claude_child(
    worker_root, tmp_path, zsh,
):
    from ccx.claude.host import session as host
    from test_generation_store import C, add_worker

    add_worker(worker_root, C, "claude")
    live_vars.publish_attachment(
        live_vars.prepare_durable_attachment({"r": 100}), "codex", B)
    live_vars.publish_attachment(
        live_vars.prepare_durable_attachment({"r": 0}), "claude", C)
    # In the nested original Codex tool zsh, let its native Python launch
    # an independently stamped original Claude host tool shell. Using
    # command python3 for the harness is deliberate; the child's own direct
    # Python performs the *actual* state mutation.
    script = (
        "import os,subprocess; import ccx.claude.host.session as host; import ccx.controllers.live_vars as live_vars; "
        + f"env=host.worker_env(worker_id={C!r}); "
        + f"env=live_vars.configure_claude_worker_env(env,{C!r},host_pid=os.getpid()); "
        + "p=subprocess.run(['bash','-c',\"source \\\"$CLAUDE_ENV_FILE\\\"; declare -f python3; python3 -c 'r=77'\"],"
        + "env=env,capture_output=True,text=True,timeout=20); "
        + "assert p.returncode==0,p.stderr; "
        + f"assert {C!r} in p.stdout,('actual child function does not contain child ID',p.stdout[:180])"
    )
    inner = "command python3 -c " + shlex.quote(script)
    # Use an explicit provider-free shell environment: no unrelated, live
    # desktop/CLI agent CLAUDE_ENV_FILE may preload a foreign Python function.
    policy = _policy()
    env = {"HOME": str(tmp_path), "CCX_ROOT": str(worker_root),
           "PYTHONPATH": str(Path(__file__).resolve().parent.parent),
           "PATH": os.pathsep.join([str(Path(sys.executable).parent), os.defpath]),
           "LC_ALL": "C.UTF-8", **policy["set"]}
    result = subprocess.run([zsh, "-c", _nested(zsh, "-lc", inner)],
                            env=env, cwd=tmp_path, text=True,
                            capture_output=True, timeout=30)
    assert result.returncode == 0, (result.stdout, result.stderr)
    assert live_vars.variables_info("claude", C, "r")["value"] == 77
    assert live_vars.variables_info("codex", B, "r")["value"] == 100
