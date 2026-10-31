"""Original Codex per-shell full-UUID variable SEND integration, fake-only."""
from __future__ import annotations

from contextlib import contextmanager
import ast
import importlib
import json
import os
import shlex
import shutil
import subprocess
import sys
from tempfile import TemporaryDirectory
import time
from types import SimpleNamespace

import pytest

from ccx import CodexClient
from ccx import WorkerClient
from ccx import constants
from ccx.controllers import store as controller_store
from ccx.controllers import live_vars
from ccx.errors import (
    CapabilityUnsupported, ControllerLiveStateLost, ControllerOperationIndeterminate,
    ControllerReplayMismatch, DeliveryRejected, InvalidRequest, SessionUnavailable, WorkerApiError,
)
from ccx.ops.creation_steps import prepare_worker
from ccx.store.root import require_ccx_active
from test_generation_store import A, B, C, short_root  # noqa: F401
import test_codex_native_effects as native_fake
import test_reactivation_codex as reactivation
import test_worker_variable_storage as backend_fake

SECOND = "aaaaaaaa-9876-4000-8000-000000000099"
RESTRICTED = "dddddddd-1234-4000-8000-000000000004"
controller = importlib.import_module('ccx.controllers.controller')


@pytest.fixture
def owned_workers(short_root, monkeypatch, tmp_path):
    with TemporaryDirectory(prefix="vsend-", dir="/tmp") as runtime:
        monkeypatch.setattr(constants, "CCX_ROOT", short_root)
        monkeypatch.setenv("CCX_ROOT", str(short_root))
        monkeypatch.setenv("XDG_RUNTIME_DIR", runtime)
        store = require_ccx_active(short_root)
        for wid, engine, permissions in (
            (A, "claude", "full"), (B, "codex", "full"),
            (C, "claude", "full"), (RESTRICTED, "codex", "restricted"),
        ):
            prepare_worker(store, engine=engine, cwd=str(tmp_path),
                           worker_id=wid, permissions=permissions)
        yield store


def _claude_ack(token, reply="fake native"):
    return SimpleNamespace(
        delivery_id=token, delivery_status="settled" if reply else "accepted",
        reply=reply, turn_ref="ccx-turn:claude:v1:fake-original",
        settled=reply is not None, state="idle",
    )


@contextmanager
def _controller_attempt(directory, task_id):
    runtime = controller._ControllerRuntime(directory)
    active = controller._active_runtime.set(runtime)
    task = controller._active_task.set(
        controller._TaskContext(task_id, task_id, directory))
    try:
        yield
    finally:
        controller._active_task.reset(task)
        controller._active_runtime.reset(active)
        runtime._executor.shutdown(wait=True)
        runtime._close_live_vars()


def _controller_dir(tmp_path):
    script = tmp_path / "controller_source.py"
    script.write_text("# inert source\n")
    _, bootstrap = controller_store.create_bootstrap(script, tmp_path, [])
    return controller_store.publish_bootstrap(
        bootstrap, name="uuid-variable-send", script=script, workspace=tmp_path)


def _operation(directory, task):
    return directory / "tasks" / task / "operations" / "0001-send.json"


# Group 1: the ORIGINAL app-server thread/start/resume, not legacy load_thread.
def test_original_codex_start_resume_per_worker_shell_and_no_sibling_leak(
    short_root, tmp_path, monkeypatch,
):
    inherited = tmp_path / "user-bash-env.sh"
    inherited.write_text("export USER_BASH_ENV_MARKER=present\n")
    monkeypatch.setenv("BASH_ENV", str(inherited))
    before = inherited.read_bytes()
    reactivation._subscription_fenced_provider(monkeypatch)
    with native_fake._f2_fake_processes(
        short_root, tmp_path, monkeypatch,
    ) as (_cli, _restart, trace, store, _daemon_rpc):
        monkeypatch.setenv("CCX_CODEX_BIN", str(tmp_path / "fake-codex"))
        client = CodexClient()
        assert client.create(cwd=str(tmp_path), worker_id=B).worker_id == B
        first = client.send(B, "first real fake-native turn",
                            delivery_id="vs-first", wait=True)
        assert first.thread_id and first.turn_id
        assert client.create(cwd=str(tmp_path), worker_id=SECOND).worker_id == SECOND
        assert WorkerClient().kill("codex:" + B)["killed"]
        resumed = client.resume(B)
        assert resumed.thread_id == first.thread_id and resumed.resumed
        trace_text = trace.read_text()
        assert trace_text.count("RPC thread/start") == 2
        assert trace_text.count("RPC thread/resume") == 1
        assert "RPC thread/fork" not in trace_text
        starts = [part.split("RPC ", 1)[0]
                  for part in trace_text.split("RPC thread/start ")[1:]]
        resume = trace_text.split("RPC thread/resume ", 1)[1].split("RPC ", 1)[0]
        scripts = store.pointer.generation_root / "variables" / "env" / "codex"
        for wid, request in ((B, starts[0]), (SECOND, starts[1]), (B, resume)):
            script = scripts / (wid + ".sh")
            zsh_dir = scripts / "zsh" / wid
            assert "'shell_environment_policy': {'set': " in request
            assert "'CCX_SELF_WORKER_ID': '" + wid + "'" in request
            assert "'CCX_SELF_ENGINE': 'codex'" in request
            assert "'BASH_ENV': '" + str(script) + "'" in request
            assert "'ZDOTDIR': '" + str(zsh_dir) + "'" in request
            contents = script.read_text()
            zsh_source = (zsh_dir / ".zshenv").read_text()
            assert "--engine codex --short " + wid in contents
            assert "--engine codex --short " + wid in zsh_source
            assert "ZDOTDIR" in zsh_source
            assert "--short " + (SECOND if wid == B else B) not in zsh_source
            assert str(inherited) in contents
            assert "--short " + (SECOND if wid == B else B) not in contents
        assert "'threadId': '" + first.thread_id + "'" in resume
        assert inherited.read_bytes() == before
        assert not (short_root / "variables").exists()


def test_fake_codex_provider_effective_bash_shell_mutates_live_controller_mapping(
    short_root, tmp_path, monkeypatch,
):
    reactivation._subscription_fenced_provider(monkeypatch)
    with native_fake._f2_fake_processes(
        short_root, tmp_path, monkeypatch,
    ) as (_cli, _restart, trace, _store, _daemon_rpc):
        monkeypatch.setenv("CCX_CODEX_BIN", str(tmp_path / "fake-codex"))
        CodexClient().create(cwd=str(tmp_path), worker_id=B)
        # The physical fake app-server records literal backslash-n delimiters.
        starts = [
            ast.literal_eval(chunk.split("\\n", 1)[0])
            for chunk in trace.read_text().split("RPC thread/start ")[1:]
        ]
        assert len(starts) == 1
        # Use the config actually sent to the fake app-server, not an
        # independently constructed test policy.
        policy = starts[0]["config"]["shell_environment_policy"]
        assert "inherit" not in policy
        directory = _controller_dir(tmp_path)
        binding = {"box": {"count": 1}, "result": None}
        with _controller_attempt(directory, "codex-live--0001"):
            WorkerClient().send(
                "codex:" + B, "run the bound python",
                variables=binding, delivery_id="fake-codex-live", wait=True,
            )
            env = os.environ.copy()
            env.pop("BASH_ENV", None)
            env.update(policy["set"])
            # The bash command invokes python3 by name; do not let a system
            # interpreter with a different ABI shadow this pytest venv.
            env["PATH"] = os.pathsep.join([os.path.dirname(sys.executable),
                                           env.get("PATH", os.defpath)])
            command = (
                # Stock macOS Bash may not auto-load BASH_ENV in this synthetic
                # tool-shell launch. Source the captured, test-owned policy
                # only when it has not already installed the Python function.
                'if [ "$(type -t python3)" != function ]; then . "$BASH_ENV"; fi; '
                'test "$(type -t python3)" = function && python3 -c ' +
                shlex.quote("box['count'] += 2; result = {'seen':box['count']}")
            )
            shell = subprocess.run(
                ["bash", "-c", command],
                cwd=tmp_path, env=env, capture_output=True, text=True, timeout=20,
            )
            assert shell.returncode == 0, (shell.stdout, shell.stderr)
            assert binding["box"]["count"] == 3
            assert binding["result"] == {"seen": 3}


def test_original_codex_first_bind_nonprivate_storage_refusal_preserves_uuid_without_native_start(
    short_root, tmp_path, monkeypatch,
):
    with native_fake._f2_fake_processes(
        short_root, tmp_path, monkeypatch,
    ) as (cli, _restart, trace, store, _daemon_rpc):
        # Deliberately non-private variables storage inside the ACTIVE generation.
        variables = store.pointer.generation_root / "variables"
        variables.mkdir()
        variables.chmod(0o755)  # Exact refusal condition regardless of umask.
        assert variables.stat().st_mode & 0o777 == 0o755
        reply = native_fake._f2_new(cli, tmp_path, B, "--start-timeout", "2")
        assert reply.returncode == 3, (reply.stdout, reply.stderr)
        original = json.loads(reply.stdout)
        # The initial variables binding fails *before* native dispatch. This
        # is a postclaim partial create, not a durable no-byte IPC refusal.
        assert original["code"] == "partial_failure", original
        assert original["phase"] == "initial-variables"
        assert original["cause_type"] == "StorageIntegrityError"
        assert original["worker_id"] == B and original["created"] is True
        assert original["activation_state"] == "unprepared"
        assert original["native_ready"] is False
        assert not original.get("native_local_refusal")
        assert store.native_activation(B)["state"] == "unprepared"
        retained = store.record(B)
        assert "RPC thread/start" not in trace.read_text()
        repeat = native_fake._f2_new(cli, tmp_path, B, "--start-timeout", "2")
        assert repeat.returncode == 3, (repeat.stdout, repeat.stderr)
        typed = json.loads(repeat.stdout)
        assert typed["code"] == "partial_failure" and typed["worker_id"] == B
        assert typed["activation_state"] == "unprepared"
        assert store.record(B) == retained
        assert "RPC thread/start" not in trace.read_text()

# Group 2: facade uses the accepted backend and original fake native SEND.
def test_send_durable_merge_exact_cross_engine_share_and_no_native_rollback(
    owned_workers, monkeypatch,
):
    client = WorkerClient()
    seen = []

    def claude_native(target, text, **options):
        assert target == "claude:" + A
        assert client.variables(A)["mode"] == "durable"  # Publish BEFORE native.
        seen.append((target, text, options))
        if "native-refuse" in text:
            raise DeliveryRejected("original native busy")
        return _claude_ack(options["delivery_id"])

    def codex_native(target, text, **options):
        assert target == "codex:" + B
        assert client.variables(B)["mode"] == "durable"
        seen.append((target, text, options))
        return SimpleNamespace(
            delivery_id=options["delivery_id"], reply="Codex original final",
            turn_id="native-fake-turn", client_user_message_id="native-message",
            state="idle",
        )

    monkeypatch.setattr(client._claude, "send", claude_native)
    monkeypatch.setattr(client._codex, "send", codex_native)
    with pytest.raises(TypeError, match="delivery"):
        client.send(A, "bad", delivery="queue-next-turn", variables={"not_published": 1})
    with pytest.raises(InvalidRequest, match="cap-opus"):
        client.send(B, "bad", caps={"opus": 40}, variables={"not_published": 1})
    assert client.variables(A)["mode"] is None
    assert client.variables(B)["mode"] is None  # C2: no invalid-option publication.

    first = client.send(
        A, "prepare evidence", variables={"counter": 1},
        caps={"5h": 35}, delivery_id="public-original-A", wait=False,
    )
    assert first["worker"] == "claude:" + A
    assert first["delivery_id"] == "public-original-A"
    assert first["queued"] is False and first["turn_ref"] == "ccx-turn:claude:v1:fake-original"
    assert seen[-1][2]["caps"] == {"5h": 35}
    assert "CCX variables available" in seen[-1][1] and "counter" in seen[-1][1]
    original_state = client.variables(A)["state_id"]

    shared = client.send(
        B, "inspect", variables_from={"worker": "claude:" + A},
        delivery_id="public-original-B",
    )
    assert shared["delivery_id"] == "public-original-B"
    assert shared["turn_ref"] == "ccx-turn:codex:v1:native-fake-turn"
    assert shared["client_user_message_id"] == "native-message"
    assert client.variables(B)["state_id"] == original_state
    edited = backend_fake._exec_python(
        constants.CCX_ROOT, "codex", B, "-c", "counter += 2; print(counter)",
    )
    assert edited.returncode == 0, (edited.stdout, edited.stderr)
    assert edited.stdout == "3\n"
    assert client.variables(A, "counter")["value"] == 3

    with pytest.raises(InvalidRequest, match="mutually exclusive"):
        client.send(A, "invalid", variables={}, variables_from=B)
    with pytest.raises(InvalidRequest):
        client.send(C, "bad source", variables_from="claude:" + A[:8])
    with pytest.raises(InvalidRequest, match="no durable variables"):
        client.send(C, "source unbound", variables_from="claude:" + C)
    with pytest.raises(InvalidRequest, match="JSON-compatible"):
        client.send(C, "not serializable", variables={"box": object()})
    assert client.variables(C)["mode"] is None

    with pytest.raises(DeliveryRejected, match="original native"):
        client.send(A, "native-refuse", variables={"evidence": [7]})
    assert client.variables(A, "evidence")["value"] == [7]
    assert client.variables(B)["state_id"] == original_state
    assert len(seen) == 3
    assert not (constants.CCX_ROOT / "variables").exists()


def test_send_restricted_and_live_claude_prebridge_refuse_before_publication(
    owned_workers, monkeypatch,
):
    store = owned_workers
    client = WorkerClient()
    native_calls = []
    monkeypatch.setattr(
        client._claude, "send", lambda *_args, **kw: (
            native_calls.append("claude") or _claude_ack(kw["delivery_id"])
        ),
    )
    monkeypatch.setattr(
        client._codex, "send", lambda *_args, **kw: native_calls.append("codex"),
    )
    store.mutate_record("claude:" + A, patches={
        "lifecycle.state": "idle", "lifecycle.hostPid": 77113,
    })
    with pytest.raises(SessionUnavailable, match="predates"):
        client.send(A, "prebridge", variables={"blocked": 1})
    assert client.variables(A)["mode"] is None

    store.mutate_record("claude:" + A, patches={"lifecycle.state": "dead"})
    resumed = client.send(A, "revival-compatible", variables={"ok": True},
                          delivery_id="prebridge-revived")
    assert resumed["delivery_id"] == "prebridge-revived"
    assert client.variables(A, "ok")["value"] is True

    with pytest.raises(CapabilityUnsupported, match="full permissions"):
        client.send(RESTRICTED, "restricted Codex", variables={"blocked": 1})
    store.mutate_record("claude:" + C, patches={"createArgs.skip_permissions": False})
    with pytest.raises(CapabilityUnsupported, match="full permissions"):
        client.send(C, "restricted Claude", variables={"blocked": 1})
    assert client.variables(RESTRICTED)["mode"] is None
    assert client.variables(C)["mode"] is None
    assert native_calls == ["claude"]

# Group 3: controller owns its existing positional journal and original token.
def test_controller_live_send_mutates_retained_objects_and_lost_owner_never_replays(
    owned_workers, tmp_path, monkeypatch,
):
    client = WorkerClient()
    directory = _controller_dir(tmp_path)
    calls = []

    class Box:
        def __init__(self):
            self.value = 2

    bindings = {"box": Box(), "result": None}

    def native(target, text, **options):
        intent = controller_store.read_json(_operation(directory, "live--0001"))
        assert intent["state"] == "intent"
        assert intent["metadata"]["worker"] == "claude:" + A
        assert intent["metadata"]["deliveryId"] == options["delivery_id"]
        assert "CCX variables available" in text
        calls.append(options["delivery_id"])
        return _claude_ack(options["delivery_id"], reply=None)

    monkeypatch.setattr(client._claude, "send", native)
    with _controller_attempt(directory, "live--0001"):
        accepted = client.send(
            A, "compute", variables=bindings,
            delivery_id="live-original-token", wait=False,
        )
        assert accepted["delivery_id"] == "live-original-token"
        info = client.variables(A)
        assert info["mode"] == "live" and info["names"] == ["box", "result"]
        assert info["available"] is False
        performed = backend_fake._exec_python(
            constants.CCX_ROOT, "claude", A, "-c",
            "box.value += 3; result={'proof':box.value}; print(box.value)",
        )
        assert performed.returncode == 0, (performed.stdout, performed.stderr)
        assert performed.stdout == "5\n"
        assert bindings["box"].value == 5 and bindings["result"] == {"proof": 5}

    journal = controller_store.read_json(_operation(directory, "live--0001"))
    assert journal["state"] == "accepted"
    assert journal["metadata"]["liveVariables"] == ["box", "result"]
    assert journal["metadata"]["deliveryId"] == calls[0] == "live-original-token"
    with _controller_attempt(directory, "live--0001"), pytest.raises(
        ControllerLiveStateLost,
    ) as lost:
        client.send(
            A, "compute", variables={"result": None, "box": Box()},
            delivery_id="live-original-token", wait=False,
        )
    assert lost.value.operation_id == "live--0001:0001:send"
    assert len(calls) == 1
    # No serialization/alias promotion of another controller's live source.
    with pytest.raises(InvalidRequest, match="no durable variables"):
        client.send(B, "copy live?", variables_from="claude:" + A)
    assert client.variables(B)["mode"] is None


def test_controller_durable_send_accepted_stopped_unknown_replay_one_token(
    owned_workers, tmp_path, monkeypatch,
):
    client = WorkerClient()
    directory = _controller_dir(tmp_path)
    live_vars.publish_attachment(
        live_vars.prepare_durable_attachment({"evidence": {"count": 1}}),
        "codex", B,
    )
    state = client.variables(B)["state_id"]
    tokens = []

    def accepted(_target, text, **options):
        assert "evidence" in text
        tokens.append(options["delivery_id"])
        return _claude_ack(options["delivery_id"], reply=None)

    monkeypatch.setattr(client._claude, "send", accepted)
    with _controller_attempt(directory, "durable--0001"):
        first = client.send(C, "use evidence", variables_from="codex:" + B, wait=False)
    assert client.variables(C)["state_id"] == state
    assert first["delivery_id"] == tokens[0]
    with _controller_attempt(directory, "durable--0001"):
        assert client.send(C, "use evidence",
                           variables_from={"worker": "codex:" + B}, wait=False) == first
    with _controller_attempt(directory, "durable--0001"), pytest.raises(ControllerReplayMismatch):
        client.send(C, "use evidence", variables_from="codex:" + B,
                    delivery_id="foreign-replacement-token", wait=False)
    with _controller_attempt(directory, "durable--0001"), pytest.raises(
        ControllerOperationIndeterminate,
    ):
        client.send(A, "use evidence", variables_from="codex:" + B, wait=False)
    assert tokens == [first["delivery_id"]]

    def stopped(_target, _text, **options):
        tokens.append(options["delivery_id"])
        raise WorkerApiError("original native API stopped", status=529, transient=True)

    monkeypatch.setattr(client._claude, "send", stopped)
    for _ in range(2):
        with _controller_attempt(directory, "stopped--0001"), pytest.raises(WorkerApiError):
            client.send(C, "stopped", variables_from="codex:" + B, wait=False)
    assert len(tokens) == 2
    assert controller_store.read_json(_operation(directory, "stopped--0001"))["state"] == "stopped"

    def uncertain(_target, _text, **options):
        tokens.append(options["delivery_id"])
        raise RuntimeError("original native acknowledgment lost")

    monkeypatch.setattr(client._claude, "send", uncertain)
    with _controller_attempt(directory, "uncertain--0001"), pytest.raises(RuntimeError):
        client.send(C, "uncertain", variables_from="codex:" + B, wait=False)
    with _controller_attempt(directory, "uncertain--0001"), pytest.raises(
        ControllerOperationIndeterminate,
    ):
        client.send(C, "uncertain", variables_from="codex:" + B, wait=False)
    assert len(tokens) == 3  # no extra original native send or delivery ID
    journal = controller_store.read_json(_operation(directory, "uncertain--0001"))
    assert journal["state"] == "indeterminate"
    assert journal["metadata"]["deliveryId"] == tokens[-1]
    assert journal["metadata"]["worker"] == "claude:" + C

# One small interpreter/native-escape/75 transfer; existing 69+runner nodes reused.
def test_worker_exec_selected_interpreter_native_escape_and_busy(
    owned_workers, monkeypatch, tmp_path,
):
    native = shutil.which("python3")
    assert native
    observed = []
    with monkeypatch.context() as scoped:
        scoped.setattr(live_vars, "_exec_native",
                       lambda selected, args, env=None: observed.append((selected, args)) or 71)
        assert live_vars.worker_exec("codex", B, "python3", ["-c", "pass"]) == 71
        assert observed[-1] == (native, ["-c", "pass"])

    owner = live_vars.LiveOwner()
    try:
        mapping = {"result": 0}
        live_vars.publish_attachment(
            live_vars.PreparedAttachment(
                "live", ["result"], mapping=mapping, owner=owner,
            ), "claude", A,
        )
        with monkeypatch.context() as scoped:
            scoped.setattr(live_vars, "_exec_native",
                           lambda selected, args, env=None: observed.append((selected, args)) or 71)
            assert live_vars.worker_exec(
                "claude", A, "python3", ["-m", "pip", "--version"],
            ) == 71
            assert observed[-1] == (native, ["-m", "pip", "--version"])
            for argv in (["--version"], ["-V"], ["-u", "script.py"]):
                assert live_vars.worker_exec("claude", A, "python3", argv) == 71
                assert observed[-1] == (native, argv)
            scoped.setattr(live_vars, "_native_python_metadata", lambda _: {
                "version": [0, 0, 0], "cacheTag": "incompatible",
                "platform": "incompatible",
            })
            assert live_vars.worker_exec(
                "claude", A, "python3", ["-c", "result=900"],
            ) == 71
            assert observed[-1] == (native, ["-c", "result=900"])
        marker = tmp_path / "live-entered"
        first = subprocess.Popen(
            backend_fake._python_argv(
                "claude", A, "-c",
                f"from pathlib import Path; Path({str(marker)!r}).write_text('in'); "
                "import time; time.sleep(1.1); result=1",
            ),
            env=backend_fake._python_env(constants.CCX_ROOT),
            cwd=constants.CCX_ROOT, stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        try:
            deadline = time.monotonic() + 5
            while not marker.exists() and time.monotonic() < deadline:
                time.sleep(.015)
            assert marker.exists()
            busy = backend_fake._exec_python(
                constants.CCX_ROOT, "claude", A, "-c", "result=777",
            )
            assert busy.returncode == 0, (busy.stdout, busy.stderr)
            stdout, stderr = first.communicate(timeout=15)
            assert first.returncode == 0, (stdout, stderr)
            assert mapping["result"] == 777
        finally:
            if first.poll() is None:
                first.kill()
                first.communicate(timeout=5)
    finally:
        owner.close()


@pytest.mark.parametrize("method", ["create", "fork"])
def test_interrupted_live_initial_binding_replays_typed_loss(
    owned_workers, tmp_path, method,
):
    from ccx.store.root import _write_json
    client = WorkerClient()
    directory = _controller_dir(tmp_path)
    calls = []

    class Box:
        pass

    def interrupted(_client, *args, **kwargs):
        worker_id = kwargs["worker_id"]
        calls.append(worker_id)
        prepare_worker(
            owned_workers, engine="claude", cwd=str(tmp_path),
            worker_id=worker_id, permissions="full",
        )
        with owned_workers.lease():
            with owned_workers._gen(
                "variables", "bindings", "claude", create=True,
            ) as fd:
                _write_json(fd, worker_id + ".json", {
                    "schemaVersion": 1, "names": ["box"], "sha256": "a" * 64,
                })
        raise RuntimeError("controller stopped after binding live state")

    options = {"variables": {"box": Box()}, "wait": True}
    if method == "create":
        options.update(engine="claude", cwd=str(tmp_path))
    args = () if method == "create" else ("claude:" + A,)
    with _controller_attempt(directory, "lost--0001"):
        with pytest.raises(RuntimeError, match="stopped after binding"):
            controller.observe_worker_call(client, method, interrupted, args, options)
    with _controller_attempt(directory, "lost--0001"):
        with pytest.raises(ControllerLiveStateLost) as error:
            controller.observe_worker_call(
                client, method, interrupted, args, {**options, "variables": {"box": Box()}},
            )
    assert error.value.operation_id == f"lost--0001:0001:{method}"
    assert len(calls) == 1


def test_send_preflight_busy_waiting_caps_and_complete_attached_names(
    owned_workers, monkeypatch,
):
    from ccx.errors import BudgetStopped
    client = WorkerClient()
    sent = []

    def native(_target, text, **options):
        sent.append((text, options["delivery_id"]))
        return _claude_ack(options["delivery_id"], reply=None)

    monkeypatch.setattr(client._claude, "send", native)
    client.send(A, "first", variables={"result": 10}, wait=False)
    client.send(A, "more", variables={"counter": 2}, wait=False)
    assert "counter, result" in sent[-1][0]
    assert client.variables(A)["names"] == ["counter", "result"]
    before = client.variables(A, "counter")["value"]

    # A dead host's stale busy record doesn't refuse; the send revives it.
    owned_workers.mutate_record("claude:" + A, patches={"lifecycle.state": "busy"})
    monkeypatch.setattr(live_vars, "_runtime_state", lambda _owner: None)
    client.send(A, "stale busy", variables={"stale": 1}, wait=False)
    assert len(sent) == 3
    before = client.variables(A, "counter")["value"]

    for state in ("busy", "waiting"):
        owned_workers.mutate_record("claude:" + A, patches={
            "lifecycle.state": state,
        })
        monkeypatch.setattr(live_vars, "_runtime_state", lambda _owner, s=state: s)
        with pytest.raises(DeliveryRejected, match=state):
            client.send(A, "refuse", variables={"counter": -1}, wait=False)
        assert client.variables(A, "counter")["value"] == before
        assert len(sent) == 3

    owned_workers.mutate_record("claude:" + A, patches={
        "lifecycle.state": "idle",
    })
    monkeypatch.setattr(
        "ccx.claude.usage.read_usage_cached",
        lambda **_kwargs: {"five_hour": {"utilization": 99}},
    )
    with pytest.raises(BudgetStopped, match="cap"):
        client.send(A, "cap refuse", variables={"counter": -5},
                    caps={"5h": 30}, wait=False)
    assert client.variables(A, "counter")["value"] == before
    assert len(sent) == 3
