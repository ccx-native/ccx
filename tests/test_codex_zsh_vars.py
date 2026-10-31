"""Real zsh startup and captured fake Codex policy for shared Python variables."""
from __future__ import annotations

import ast
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import shlex
import shutil
import stat
import subprocess
import sys
import sysconfig

import pytest

from ccx import CodexClient
from ccx import WorkerClient
from ccx import constants
from ccx.controllers import live_vars
from ccx.store.root import StorageIntegrityError
from test_generation_store import B, add_worker, fixture_root, short_root  # noqa: F401
from test_send_variable_attachment import _controller_attempt, _controller_dir
import test_codex_native_effects as native_fake
import test_reactivation_codex as reactivation


SECOND = "aaaaaaaa-9876-4000-8000-000000000099"
ZSH_STARTUP_FILES = (".zshenv", ".zprofile", ".zshrc", ".zlogin", ".zlogout")


@pytest.fixture
def zsh():
    binary = os.environ.get("CCX_TEST_ZSH") or shutil.which("zsh")
    if not binary:
        pytest.skip("real zsh unavailable (set CCX_TEST_ZSH to a zsh executable)")
    if not Path(binary).is_file():
        pytest.skip(f"real zsh executable missing: {binary}")
    return str(Path(binary).resolve())


@pytest.fixture
def worker_root(tmp_path, monkeypatch):
    root = fixture_root(tmp_path)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    monkeypatch.setenv("CCX_ROOT", str(root))
    monkeypatch.delenv("ZDOTDIR", raising=False)
    monkeypatch.delenv("BASH_ENV", raising=False)
    add_worker(root, B, "codex")
    add_worker(root, SECOND, "codex")
    return root


def _policy(worker: str = B):
    return live_vars.codex_thread_config(worker)["shell_environment_policy"]


def _run_zsh(zsh, policy, command, *, home: Path, cwd: Path, options="-c"):
    env = os.environ.copy()
    env.pop("BASH_ENV", None)
    env.pop("ZDOTDIR", None)
    env.update(policy["set"])
    env["HOME"] = str(home)
    # A real provider tool shell must resolve python/python3 to the test
    # interpreter. Stock macOS offers only /usr/bin/python3 (3.9) and may
    # have no 'python' at all; it cannot share this pytest process's ABI.
    env["PATH"] = os.pathsep.join([os.path.dirname(sys.executable),
                                   env.get("PATH", os.defpath)])
    return subprocess.run(
        [zsh, options, command],
        cwd=cwd, env=env, text=True, capture_output=True, timeout=25,
    )


def _startup_files(directory: Path, marker: Path, *, only: tuple[str, ...] = ()):
    directory.mkdir(parents=True, exist_ok=True)
    files = only or (".zshenv", ".zprofile", ".zshrc", ".zlogin", ".zlogout")
    for file in files:
        (directory / file).write_text(
            f"print -r -- {shlex.quote(file)} >> {shlex.quote(str(marker))}\n",
        )


@pytest.mark.parametrize("original", ["unset", "empty", "relative", "spaced", "quoted"])
@pytest.mark.parametrize("options,expected", [
    ("-c", [".zshenv"]),
    ("-lc", [".zshenv", ".zprofile", ".zlogin"]),
    ("-ic", [".zshenv", ".zshrc"]),
    ("-lic", [".zshenv", ".zprofile", ".zshrc", ".zlogin", ".zlogout"]),
])
def test_real_zsh_keeps_managed_zdotdir_and_runs_original_startup_files(
    worker_root, tmp_path, monkeypatch, zsh, original, options, expected,
):
    home = tmp_path / "synthetic-home"
    home.mkdir()
    marker = tmp_path / "startup-order.txt"
    user = {
        "unset": home,
        "empty": home,
        "relative": tmp_path / "relative user",
        "spaced": tmp_path / "user dotfiles and spaces",
        "quoted": tmp_path / "user's dotfiles",
    }[original]
    _startup_files(user, marker)
    if original == "unset":
        monkeypatch.delenv("ZDOTDIR", raising=False)
        restored = "__unset__"
    elif original == "empty":
        monkeypatch.setenv("ZDOTDIR", "")
        restored = ""
    elif original == "relative":
        monkeypatch.setenv("ZDOTDIR", "relative user")
        restored = "relative user"
    else:
        monkeypatch.setenv("ZDOTDIR", str(user))
        restored = str(user)
    policy = _policy()
    shim = Path(policy["set"]["ZDOTDIR"])
    assert shim != user
    assert sorted(p.name for p in shim.iterdir()) == sorted(ZSH_STARTUP_FILES)
    result = _run_zsh(
        zsh, policy, 'print -r -- "COMMAND:${ZDOTDIR-__unset__}"',
        home=home, cwd=tmp_path, options=options,
    )
    assert result.returncode == 0, result.stderr
    # The genuine user files execute, while this worker keeps the effective
    # managed directory for subsequent nested zsh processes.
    assert result.stdout == f"COMMAND:{shim}\n"
    # Native zsh with a defined-but-empty ZDOTDIR loads no HOME dotfiles.
    observed = marker.read_text().splitlines() if marker.exists() else []
    assert observed == ([] if original == "empty" else expected)


def test_real_zsh_original_zshenv_can_change_later_lookup_directory(
    worker_root, tmp_path, monkeypatch, zsh,
):
    home = tmp_path / "synthetic-home"
    home.mkdir()
    marker = tmp_path / "order.txt"
    first, next_dir = tmp_path / "initial user dir", tmp_path / "next user dir"
    _startup_files(first, marker)
    _startup_files(next_dir, marker)
    with (first / ".zshenv").open("a") as f:
        f.write(f"export ZDOTDIR={shlex.quote(str(next_dir))}\n")
    monkeypatch.setenv("ZDOTDIR", str(first))
    result = _run_zsh(
        zsh, _policy(), 'print -r -- "COMMAND:$ZDOTDIR"',
        home=home, cwd=tmp_path, options="-lc",
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == f"COMMAND:{_policy()['set']['ZDOTDIR']}\n"
    assert marker.read_text().splitlines() == [
        ".zshenv", ".zprofile", ".zlogin",
    ]


@pytest.mark.parametrize("kind", ["alias", "function"])
def test_real_zsh_preserves_user_python_precedence(
    worker_root, tmp_path, monkeypatch, zsh, kind,
):
    home = tmp_path / "synthetic-home"
    home.mkdir()
    if kind == "alias":
        (home / ".zshenv").write_text(
            "alias python3='print -r -- user-alias'\n"
        )
    else:
        (home / ".zshenv").write_text(
            "function python3 { print -r -- user-function; }\n"
        )
    result = _run_zsh(zsh, _policy(), "python3", home=home, cwd=tmp_path)
    assert result.returncode == 0, result.stderr
    assert result.stdout == ("user-alias\n" if kind == "alias" else "user-function\n")


def test_real_zsh_fast_start_does_not_promise_bridge(worker_root, tmp_path, zsh):
    home = tmp_path / "synthetic-home"
    home.mkdir()
    result = _run_zsh(
        zsh, _policy(), 'if typeset -f python3 >/dev/null; then print BRIDGE; else print NATIVE; fi',
        home=home, cwd=tmp_path, options="-fc",
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == "NATIVE\n"


def test_private_generation_zsh_trampoline_atomic_idempotent_and_owned(worker_root):
    policy = _policy()
    shim = Path(policy["set"]["ZDOTDIR"])
    file = shim / ".zshenv"
    assert shim.is_absolute()
    assert shim == Path(policy["set"]["BASH_ENV"]).parent / "zsh" / B
    assert shim.stat().st_uid == os.getuid()
    assert stat.S_IMODE(shim.stat().st_mode) == 0o700
    assert file.stat().st_uid == os.getuid()
    assert stat.S_IMODE(file.stat().st_mode) == 0o600
    assert file.is_file()
    files = [shim / name for name in ZSH_STARTUP_FILES]
    original = [(item.stat().st_ino, item.read_bytes()) for item in files]
    assert all(item.stat().st_uid == os.getuid()
               and stat.S_IMODE(item.stat().st_mode) == 0o600 for item in files)
    with ThreadPoolExecutor(max_workers=6) as pool:
        configs = list(pool.map(lambda _n: _policy(), range(12)))
    assert all(config == policy for config in configs)
    assert [(item.stat().st_ino, item.read_bytes()) for item in files] == original
    assert sorted(p.name for p in shim.iterdir()) == sorted(ZSH_STARTUP_FILES)
    assert not (worker_root / "variables").exists()


def test_warm_codex_shell_files_do_not_write_or_fsync(worker_root, monkeypatch):
    policy = _policy()
    bash = Path(policy["set"]["BASH_ENV"])
    zsh = Path(policy["set"]["ZDOTDIR"]) / ".zshenv"
    all_files = [bash, *(zsh.parent / name for name in ZSH_STARTUP_FILES)]
    before = [(file.stat().st_ino, file.stat().st_mtime_ns, file.read_bytes())
              for file in all_files]

    def forbidden(*_args, **_kwargs):
        pytest.fail("unchanged generated shell file was rewritten or fsynced")

    with monkeypatch.context() as no_writes:
        no_writes.setattr(live_vars.os, "replace", forbidden)
        no_writes.setattr(live_vars.os, "fsync", forbidden)
        assert _policy() == policy
        assert live_vars.ensure_shell_env("codex", B) == bash
        assert live_vars.ensure_codex_zshenv(B, original_zdotdir=None) == zsh.parent

    assert before == [(file.stat().st_ino, file.stat().st_mtime_ns,
                       file.read_bytes()) for file in all_files]
    assert sorted(p.name for p in zsh.parent.iterdir()) == sorted(ZSH_STARTUP_FILES)


def test_original_user_shell_snapshots_replace_only_changed_content(
    worker_root, monkeypatch, tmp_path,
):
    policy = _policy()
    bash = Path(policy["set"]["BASH_ENV"])
    zsh = Path(policy["set"]["ZDOTDIR"]) / ".zshenv"
    first_bash, first_zsh = bash.stat().st_ino, zsh.stat().st_ino
    user_bash = tmp_path / "own bash startup.sh"
    user_bash.write_text("export USER_BASH_MARKER=present\n")
    monkeypatch.setenv("BASH_ENV", str(user_bash))
    assert _policy() == policy
    assert bash.stat().st_ino != first_bash
    # Both Bash and zsh need the caller's *original* BASH_ENV snapshot when
    # one of their native shells later launches a separate CCX-owned child.
    assert zsh.stat().st_ino != first_zsh
    assert str(user_bash) in bash.read_text()
    assert str(user_bash) in zsh.read_text()
    rewritten_bash = bash.stat().st_ino
    rewritten_zsh = zsh.stat().st_ino

    user_zsh = tmp_path / "own zsh dotfiles"
    user_zsh.mkdir()
    monkeypatch.setenv("ZDOTDIR", str(user_zsh))
    assert _policy() == policy
    assert bash.stat().st_ino != rewritten_bash
    assert zsh.stat().st_ino != rewritten_zsh
    assert str(user_zsh) in bash.read_text()
    assert str(user_zsh) in zsh.read_text()
    rewritten_bash = bash.stat().st_ino
    rewritten_zsh = zsh.stat().st_ino
    assert _policy() == policy
    assert (bash.stat().st_ino, zsh.stat().st_ino) == (
        rewritten_bash, rewritten_zsh,
    )

    # A valid private file with incorrect bytes is replaced, not reused.
    bash.write_bytes(b"corrupt")
    zsh.write_bytes(b"corrupt")
    assert _policy() == policy
    assert bash.read_bytes() != b"corrupt"
    assert zsh.read_bytes() != b"corrupt"
    assert "USER_BASH_MARKER" not in bash.read_text()
    assert str(user_bash) in bash.read_text()
    assert str(user_zsh) in zsh.read_text()

    # Missing output files are regenerated, then subsequent calls are no-ops.
    bash.unlink()
    zsh.unlink()
    assert _policy() == policy
    assert stat.S_IMODE(bash.stat().st_mode) == 0o600
    assert stat.S_IMODE(zsh.stat().st_mode) == 0o600
    assert _policy() == policy


def test_bash_shell_file_rejects_unsafe_existing_inode(
    worker_root, tmp_path,
):
    path = Path(_policy()["set"]["BASH_ENV"])
    path.chmod(0o644)
    with pytest.raises(StorageIntegrityError, match="private regular file"):
        _policy()
    path.chmod(0o600)
    path.unlink()
    victim = tmp_path / "untouched-user-file"
    victim.write_text("do not alter")
    path.symlink_to(victim)
    with pytest.raises(StorageIntegrityError, match="private regular file"):
        _policy()
    assert victim.read_text() == "do not alter"


def test_private_generation_zsh_rejects_symlink_and_nonprivate_components(
    worker_root, tmp_path,
):
    shim = Path(_policy()["set"]["ZDOTDIR"])
    file = shim / ".zshenv"
    file.unlink()
    victim = tmp_path / "outside-file"
    victim.write_text("safe")
    file.symlink_to(victim)
    with pytest.raises(StorageIntegrityError, match="private regular file"):
        _policy()
    assert victim.read_text() == "safe"
    file.unlink()
    for stage in ZSH_STARTUP_FILES[1:]:
        other = shim / stage
        other.unlink()
        other.symlink_to(victim)
        with pytest.raises(StorageIntegrityError, match="private regular file"):
            _policy()
        other.unlink()
        _policy()
    assert victim.read_text() == "safe"
    shim.chmod(0o755)
    with pytest.raises(StorageIntegrityError, match="owner-private"):
        _policy()
    shim.chmod(0o700)
    shim.chmod(0o500)
    with pytest.raises(StorageIntegrityError, match="owner-owned mode 0700"):
        _policy()
    shim.chmod(0o700)
    for stage in ZSH_STARTUP_FILES:
        (shim / stage).unlink()
    shim.rmdir()
    shim.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises((StorageIntegrityError, OSError)):
        _policy()
    assert victim.read_text() == "safe"


def test_shared_appserver_non_ccx_zsh_thread_does_not_inherit_bridge(
    worker_root, tmp_path, monkeypatch, zsh,
):
    from ccx.codex import protocol as codex_protocol
    from ccx.codex import runtime as codex_runtime
    home = tmp_path / "unmanaged-home"
    home.mkdir()
    policy = _policy()
    monkeypatch.setenv("ZDOTDIR", policy["set"]["ZDOTDIR"])
    monkeypatch.setenv("BASH_ENV", policy["set"]["BASH_ENV"])
    for shared in (codex_runtime.daemon_env(), codex_protocol._user_command_env()):
        assert "ZDOTDIR" not in shared
        assert "CCX_SELF_WORKER_ID" not in shared
    appserver = codex_protocol._user_command_env()
    assert "BASH_ENV" not in appserver
    appserver["HOME"] = str(home)
    result = subprocess.run(
        [zsh, "-lc", "if typeset -f python3 >/dev/null; then print BRIDGE; "
                      "else print NATIVE; fi"],
        env=appserver, cwd=tmp_path, capture_output=True, text=True, timeout=15,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == "NATIVE\n"


def _captured_policies(trace):
    starts = [
        ast.literal_eval(chunk.split("\\n", 1)[0])
        for chunk in trace.read_text().split("RPC thread/start ")[1:]
    ]
    assert len(starts) == 2
    assert {start["threadSource"] for start in starts} == {
        "ccx-worker:" + B, "ccx-worker:" + SECOND,
    }
    return {
        start["threadSource"].split(":", 1)[1]: start["config"]["shell_environment_policy"]
        for start in starts
    }


def _python_command(name: str, script: str):
    return f"{name} -c {shlex.quote(script)}"


def _assert_run_zsh(zsh, policy, code, tmp_path, *, options="-c", output=None):
    result = _run_zsh(
        zsh, policy, code, home=tmp_path / "synthetic-home",
        cwd=tmp_path, options=options,
    )
    assert result.returncode == 0, (code, result.stdout, result.stderr)
    if output is not None:
        assert result.stdout == output
    return result


def test_real_zsh_incompatible_python3_falls_back_to_native_with_warning(
    worker_root, tmp_path, zsh,
):
    """An incompatible interpreter must execute natively, never mutate live state.

    This fake python3 deliberately advertises a different ABI to the guard's
    metadata probe, then delegates ordinary -c execution to the test Python.
    It is independent of whether the host has stock /usr/bin/python3.
    """
    directory = tmp_path / "incompatible-bin"
    directory.mkdir()
    stub = directory / "python3"
    stub.write_text(
        "#!/bin/sh\n"
        'case "$2" in\n'
        "  *cacheTag*) printf '%s\\n' "
        + shlex.quote('{"version":[0,0,0],"cacheTag":"incompatible",'
                      '"platform":"incompatible"}')
        + ";;\n"
        "  *) exec " + shlex.quote(sys.executable) + ' "$@";;\n'
        "esac\n"
    )
    stub.chmod(0o700)
    metadata = live_vars._native_python_metadata(str(stub))
    assert metadata is not None and metadata["version"] == [0, 0, 0]

    box = {"box": {"count": 1}}
    owner = live_vars.LiveOwner()
    try:
        live_vars.publish_attachment(
            live_vars.PreparedAttachment("live", sorted(box), mapping=box, owner=owner),
            "codex", B,
        )
        env = os.environ.copy()
        env.pop("BASH_ENV", None)
        env.pop("ZDOTDIR", None)
        env.update(_policy()["set"])
        env["HOME"] = str(tmp_path)
        env["PATH"] = os.pathsep.join([
            str(directory), os.path.dirname(sys.executable), env.get("PATH", os.defpath),
        ])
        code = _python_command(
            "python3", "assert 'box' not in globals(); print('NATIVE_FALLBACK')",
        )
        result = subprocess.run(
            [zsh, "-c", "typeset -f python3 >/dev/null && " + code],
            env=env, cwd=tmp_path, capture_output=True, text=True, timeout=20,
        )
        assert result.returncode == 0, (result.stdout, result.stderr)
        assert result.stdout == "NATIVE_FALLBACK\n"
        assert ("ccx: selected Python is incompatible with the live variable owner; "
                "running native Python") in result.stderr
        assert box == {"box": {"count": 1}}
    finally:
        owner.close()


def test_fake_codex_captured_policy_real_zsh_live_rebind_and_isolation(
    short_root, tmp_path, monkeypatch, zsh,
):
    (tmp_path / "synthetic-home").mkdir()
    reactivation._subscription_fenced_provider(monkeypatch)
    with native_fake._f2_fake_processes(
        short_root, tmp_path, monkeypatch,
    ) as (_cli, _restart, trace, _store, _daemon_rpc):
        monkeypatch.setenv("CCX_CODEX_BIN", str(tmp_path / "fake-codex"))
        client = CodexClient()
        client.create(cwd=str(tmp_path), worker_id=B)
        client.create(cwd=str(tmp_path), worker_id=SECOND)
        policies = _captured_policies(trace)
        assert policies[B]["set"]["ZDOTDIR"] != policies[SECOND]["set"]["ZDOTDIR"]
        assert policies[B]["set"]["BASH_ENV"] != policies[SECOND]["set"]["BASH_ENV"]
        assert all("inherit" not in policy for policy in policies.values())

        directory = _controller_dir(tmp_path)
        one = {"box": {"count": 1}, "result": None}
        two = {"box": {"count": 90}, "result": None}
        with _controller_attempt(directory, "codex-zsh-live-0001"):
            WorkerClient().send(
                "codex:" + B, "use live variables", variables=one,
                delivery_id="zsh-live-one", wait=True,
            )
            WorkerClient().send(
                "codex:" + SECOND, "use sibling variables", variables=two,
                delivery_id="zsh-live-two", wait=True,
            )
            _assert_run_zsh(
                zsh, policies[B],
                _python_command("python3", "box['count'] += 2; result = {'seen': box['count']}"),
                tmp_path, options="-lc",
            )
            assert one == {"box": {"count": 3}, "result": {"seen": 3}}
            assert two == {"box": {"count": 90}, "result": None}
            _assert_run_zsh(
                zsh, policies[B],
                _python_command("python", "box['count'] += 4; result = {'bound': box['count']}"),
                tmp_path,
            )
            assert one == {"box": {"count": 7}, "result": {"bound": 7}}
            _assert_run_zsh(
                zsh, policies[SECOND],
                _python_command("python3", "box['count'] += 5; result = {'other': box['count']}"),
                tmp_path,
            )
            assert two == {"box": {"count": 95}, "result": {"other": 95}}
            assert one["box"]["count"] == 7

            native = "assert 'box' not in globals() and 'result' not in globals()"
            for prefix in ("command python3", "env python3", shlex.quote(sys.executable)):
                _assert_run_zsh(
                    zsh, policies[B], _python_command(prefix, native), tmp_path,
                )
            shebang = tmp_path / "native-shebang.py"
            shebang.write_text(f"#!{sys.executable}\n{native}\n")
            shebang.chmod(0o700)
            _assert_run_zsh(zsh, policies[B], shlex.quote(str(shebang)), tmp_path)
            assert one == {"box": {"count": 7}, "result": {"bound": 7}}

            # Nested zsh is another shell of the SAME managed Codex worker.
            probe = "print('BRIDGED' if 'box' in globals() else 'NATIVE')"
            child_zsh = f"{shlex.quote(zsh)} -c {shlex.quote(_python_command('python3', probe))}"
            # An isolated macOS Bash subprocess may not auto-load the fake
            # provider's BASH_ENV. Source the same captured policy only if its
            # bridge is missing; normal Bash auto-startup is not re-sourced.
            child_bash_script = (
                'if [ "$(type -t python3)" != function ]; then . "$BASH_ENV"; fi; '
                + _python_command('python3', probe)
            )
            child_bash = f"bash -c {shlex.quote(child_bash_script)}"
            _assert_run_zsh(zsh, policies[B], child_zsh, tmp_path, output="BRIDGED\n")
            _assert_run_zsh(zsh, policies[B], child_bash, tmp_path, output="BRIDGED\n")
            assert one["box"]["count"] == 7
            # Force a compatible native interpreter for a mutating login
            # test; macOS login path_helper can otherwise pick stock 3.9.
            compatible_path = os.path.dirname(sys.executable) + os.pathsep + "/usr/bin:/bin"
            _assert_run_zsh(
                zsh, policies[B],
                "PATH=" + shlex.quote(compatible_path) + "; "
                + _python_command("python3", "box['count'] += 1"),
                tmp_path, options="-lc",
            )
            assert one["box"]["count"] == 8
            # The macOS system-only PATH may select an incompatible stock
            # interpreter. In that case native Python MUST NOT see live box.
            selected = shutil.which("python3", path="/usr/bin:/bin")
            if selected:
                owner_info = {"major": sys.version_info.major,
                              "minor": sys.version_info.minor,
                              "platform": sysconfig.get_platform(),
                              "cacheTag": sys.implementation.cache_tag}
                native_info = live_vars._native_python_metadata(selected)
                compatible = native_info is not None and live_vars._compatible(owner_info, native_info)
                check = ("assert 'box' in globals()" if compatible
                         else "assert 'box' not in globals()")
                _assert_run_zsh(
                    zsh, policies[B],
                    "PATH=/usr/bin:/bin; " + _python_command("python3", check),
                    tmp_path, options="-lc",
                )
                assert one["box"]["count"] == 8


def test_fake_codex_captured_policy_real_zsh_durable_workers_isolated(
    short_root, tmp_path, monkeypatch, zsh,
):
    (tmp_path / "synthetic-home").mkdir()
    reactivation._subscription_fenced_provider(monkeypatch)
    with native_fake._f2_fake_processes(
        short_root, tmp_path, monkeypatch,
    ) as (_cli, _restart, trace, _store, _daemon_rpc):
        monkeypatch.setenv("CCX_CODEX_BIN", str(tmp_path / "fake-codex"))
        client = CodexClient()
        client.create(cwd=str(tmp_path), worker_id=B)
        client.create(cwd=str(tmp_path), worker_id=SECOND)
        policies = _captured_policies(trace)
        workers = WorkerClient()
        workers.send("codex:" + B, "bind durable", variables={"counter": 1, "proof": []},
                     delivery_id="zsh-durable-one", wait=True)
        workers.send("codex:" + SECOND, "bind own durable", variables={"counter": 100},
                     delivery_id="zsh-durable-two", wait=True)
        _assert_run_zsh(
            zsh, policies[B], _python_command(
                "python3", "counter += 2; proof.append('zsh-c')",
            ), tmp_path,
        )
        _assert_run_zsh(
            zsh, policies[B], _python_command(
                "python", "counter = 15; proof.append('zsh-login')",
            ), tmp_path, options="-lc",
        )
        assert workers.variables("codex:" + B, "counter")["value"] == 15
        assert workers.variables("codex:" + B, "proof")["value"] == [
            "zsh-c", "zsh-login",
        ]
        assert workers.variables("codex:" + SECOND, "counter")["value"] == 100
        _assert_run_zsh(
            zsh, policies[SECOND],
            _python_command("python3", "counter += 8"),
            tmp_path,
        )
        assert workers.variables("codex:" + SECOND, "counter")["value"] == 108
        assert workers.variables("codex:" + B, "counter")["value"] == 15
