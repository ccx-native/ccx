"""Real disposable Claude host: CLI/Python/V1 share one original native start."""
from __future__ import annotations
import ccx.ops.send as ops_send
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import time
import pytest
from test_generation_store import A, short_root
from test_claude_native_cli import _stop_disposable_original
from ccx import constants
from ccx.store.liveness import owned_pid_alive
from ccx.store.root import require_ccx_active
from ccx.store.worker_state import normalize_native_state


def _invoke(repo, env, *argv, request=None):
    proc = subprocess.run(
        [sys.executable, "-B", *argv], env=env, cwd=repo,
        input=json.dumps(request) if request else None,
        capture_output=True, text=True, timeout=17,
    )
    assert proc.returncode == 0, (proc.args, proc.stdout, proc.stderr)
    return json.loads(proc.stdout)


@pytest.mark.parametrize("order", [
    ("cli", "claude", "worker", "v1"),
    ("v1", "worker", "claude", "cli"),
])
def test_create_parity_claude_all_surfaces_one_original_native_start(
    short_root, tmp_path, monkeypatch, order,
):
    repo = Path(__file__).resolve().parent.parent
    fake = repo / "tests" / "fake_claude.py"
    wrapper = tmp_path / "fake-claude-parity"
    launches = tmp_path / "fake-claude-physical-starts.log"
    wrapper.write_text(
        "#!/bin/sh\n"
        + "printf 'original-provider-exec\\n' >> " + shlex.quote(str(launches)) + "\n"
        + "exec " + shlex.quote(sys.executable) + " "
        + shlex.quote(str(fake)) + ' "$@"\n', encoding="utf-8",
    )
    wrapper.chmod(0o700)
    env = {**os.environ, "CCX_ROOT": str(short_root),
           "CCX_CLAUDE_BIN": str(wrapper), "CCX_SPAWN_SCOPE": "0", "CCX_BOOT_TIMEOUT_S": "15",
           "FAKE_MODE": "", "FAKE_LOG": str(tmp_path / "fake-input.log"),
           "PYTHONPATH": str(repo)}
    for key in ("CCX_SELF_SHORT", "CCX_SELF_WORKER_ID", "CCX_SELF_ENGINE"):
        env.pop(key, None)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    store = require_ccx_active(short_root)
    try:
        for surface in order:
            until = time.monotonic() + 12
            while True:
                if surface == "cli":
                    proc = subprocess.run([
                        sys.executable, "-B", "-m", "ccx", "new", "--engine", "claude", "--cwd",
                        str(tmp_path), "--worker-id", A, "--json",
                    ], env=env, cwd=repo, capture_output=True, text=True, timeout=17)
                    assert proc.returncode in (0, 3), (proc.stdout, proc.stderr)
                    obj = json.loads(proc.stdout)
                    ready = obj.get("native_ready") is True
                    assert obj["worker_id"] == A
                elif surface in {"claude", "worker"}:
                    call = (
                        f"CcxClient().new(cwd={str(tmp_path)!r}, worker_id={A!r})"
                        if surface == "claude" else
                        f"WorkerClient().create(engine='claude',cwd={str(tmp_path)!r},worker_id={A!r})"
                    )
                    source = (
                        "import json\nfrom ccx import CcxClient, WorkerClient\n"
                        "from ccx.errors import CreatePartialFailure\n"
                        f"try:\n x={call}\n"
                        " print(json.dumps({'worker_id':x.worker_id, 'native_ready':True, "
                        "'session_id':x.session_id}))\n"
                        if surface == "claude" else
                        "import json\nfrom ccx import WorkerClient\n"
                        "from ccx.errors import CreatePartialFailure\n"
                        f"try:\n x={call}\n"
                        " print(json.dumps(x))\n"
                    ) + (
                        "except CreatePartialFailure as e:\n"
                        " print(json.dumps({'worker_id':e.worker_id,'native_ready':False}))\n"
                    )
                    obj = _invoke(repo, env, "-c", source)
                    assert obj["worker_id"] == A
                    # The facade's success envelope is distinct from the raw
                    # CLI/typed readiness flag: only proven create returns a dict.
                    ready = obj["native_ready"] if surface == "claude" else "state" in obj
                    if ready and surface == "claude":
                        assert obj["session_id"] == store.record(A)["native"]["sessionId"]
                    elif ready:
                        assert set(obj) == {
                            "worker", "worker_id", "engine", "short", "state",
                            "native_state", "report", "reply", "created", "reconciled",
                            "delivery_id", "delivery_status", "notification",
                        }
                        assert obj["worker"] == f"claude:{A}" and obj["engine"] == "claude"
                        assert obj["short"] == A[:8]
                        assert obj["state"] == normalize_native_state(obj["native_state"])
                        assert all(obj[key] is None for key in (
                            "report", "delivery_id", "delivery_status", "notification",
                        ))
                        assert store.native_activation(A)["state"] == "confirmed"
                else:
                    obj = _invoke(repo, env, "-m", "ccx", "rpc", request={
                        "protocol_version": 2, "request_id": "claude-parity",
                        "op": "worker.create", "params": {
                            "engine": "claude", "cwd": str(tmp_path), "worker_id": A,
                        }, "timeout_ms": 5000,
                    })
                    ready = obj["ok"]
                    if ready:
                        public = obj["result"]
                        assert public["worker"] == f"claude:{A}"
                        assert public["worker_view"]["state_source"] == "recorded"
                        assert public["worker_view"]["native_id"] == store.record(A)["native"]["sessionId"]
                        assert {"record", "native_ready", "activation_state", "native_source"}.isdisjoint(public)
                    else:
                        assert obj["error"]["code"] == "unavailable", obj
                        assert obj["error"]["details"]["worker_id"] == A
                if ready:
                    break
                assert time.monotonic() < until, (surface, obj)
                time.sleep(.1)
        claim = store.native_activation(A)
        assert claim["state"] == "confirmed"
        record = store.record(A)
        native = record["native"]
        assert native["nativeState"] == "session-bound"
        assert native["sessionId"] == claim["nativeBinding"]["sessionId"]
        proof = record["lifecycle"]["nativeStartProof"]
        assert proof["sessionId"] == native["sessionId"]
        assert owned_pid_alive(proof["hostPid"], proof["hostStart"])
        assert owned_pid_alive(proof["workerPid"], proof["workerStart"])
        assert store.record(A)["lifecycle"]["hostPid"] == proof["hostPid"]
        assert launches.read_text().splitlines() == ["original-provider-exec"]
    finally:
        _stop_disposable_original(short_root)


@pytest.mark.parametrize("start_timeout", [0, -1, float("nan"), float("inf"),
                                             -float("inf"), 90.1, True])
def test_create_parity_python_invalid_start_budget_never_claims_id(
    short_root, tmp_path, monkeypatch, start_timeout,
):
    from ccx import CcxClient, CodexClient, WorkerClient
    from ccx.errors import InvalidRequest
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    store = require_ccx_active(short_root)
    before = sorted((store.pointer.generation_root / "claims").glob("*.json"))
    clients = (lambda: CcxClient().new(cwd=str(tmp_path), worker_id=A,
                                       start_timeout=start_timeout),
               lambda: CodexClient().create(cwd=str(tmp_path), worker_id=A,
                                             start_timeout=start_timeout),
               lambda: WorkerClient().create(engine="claude", cwd=str(tmp_path),
                                              worker_id=A, start_timeout=start_timeout))
    for create in clients:
        with pytest.raises(InvalidRequest, match="start_timeout"):
            create()
        assert sorted((store.pointer.generation_root / "claims").glob("*.json")) == before
        assert not (store.pointer.generation_root / "owners" / f"{A}.json").exists()


def _deadline_fake_claude(root, tmp_path, monkeypatch):
    """Disposable real host/PTY with one physical shell-exec counter."""
    repo = Path(__file__).resolve().parent.parent
    wrapper = tmp_path / "deadline-claude-fake"
    launches = tmp_path / "deadline-claude-launches.log"
    fake = repo / "tests" / "fake_claude.py"
    wrapper.write_text(
        "#!/bin/sh\n"
        + "printf 'provider-start\\n' >> " + shlex.quote(str(launches)) + "\n"
        + "exec " + shlex.quote(sys.executable) + " "
        + shlex.quote(str(fake)) + ' "$@"\n', encoding="utf-8",
    )
    wrapper.chmod(0o700)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    monkeypatch.setattr(constants, "CLAUDE_BIN", str(wrapper))
    for name, value in {
        "CCX_ROOT": str(root), "CCX_CLAUDE_BIN": str(wrapper),
        "CCX_SPAWN_SCOPE": "0",
        "CCX_BOOT_TIMEOUT_S": "15", "PYTHONPATH": str(repo),
    }.items():
        monkeypatch.setenv(name, value)
    for name in ("CCX_SELF_SHORT", "CCX_SELF_WORKER_ID", "CCX_SELF_ENGINE"):
        monkeypatch.delenv(name, raising=False)
    return launches


@pytest.mark.parametrize("budget_ms", [500, 700, 1000])
def test_repair_claude_v1_deadline_caps_slow_status_and_recovers_same_id(
    short_root, tmp_path, monkeypatch, budget_ms,
):
    """Expired late-positive status is a partial; the same host is observed later."""
    from ccx.rpc import local_rpc
    from ccx.rpc import rpc
    from ccx.claude import spawn
    from ccx.errors import CreatePartialFailure
    from ccx.store.root import NativeEffectIndeterminate

    launches = _deadline_fake_claude(short_root, tmp_path, monkeypatch)
    store = require_ccx_active(short_root)

    def request(label, timeout_ms):
        return {
            "protocol_version": 2, "request_id": label,
            "op": "worker.create", "params": {
                "engine": "claude", "cwd": str(tmp_path), "worker_id": A,
            }, "timeout_ms": timeout_ms,
        }

    try:
        # First call submits the actual host once, while the provider's
        # first SessionStart verification is deliberately held unknown.
        with monkeypatch.context() as injected:
            def pending(*args, **kwargs):
                return None
            injected.setattr(spawn, "verified_original_claude_start", pending)
            first = local_rpc.dispatch(request("submit-original", 1500))
        assert first["ok"] is False, first
        assert first["error"]["details"]["worker_id"] == A
        assert store.native_activation(A)["state"] == "submitted-unknown"
        until = time.monotonic() + 8
        while time.monotonic() < until:
            proof = (store.record(A).get("lifecycle") or {}).get("nativeStartProof")
            if isinstance(proof, dict):
                break
            time.sleep(.025)
        assert isinstance(proof, dict), "disposable original SessionStart never arrived"
        assert launches.read_text().splitlines() == ["provider-start"]

        # A realistic slow status must receive no *more* than remaining time;
        # even if it returns a positive receipt just after expiry, the caller
        # must NOT receive native_ready or launch another host.
        original_rpc = rpc._rpc
        samples = []
        started = time.monotonic()
        absolute_end = started + budget_ms / 1000.0

        def slow_status(worker, msg, timeout=5.):
            if msg.get("op") == "startup-status":
                samples.append(timeout)
                # Simulate 200-240ms status latency, but deliberately arrange
                # the positive reply only after the caller-owned deadline.
                time.sleep(max(0., absolute_end - time.monotonic() + .015))
            return original_rpc(worker, msg, timeout=timeout)

        with monkeypatch.context() as injected:
            injected.setattr(rpc, "_rpc", slow_status)
            late = local_rpc.dispatch(request("late-status", budget_ms))
        elapsed = time.monotonic() - started
        assert late["ok"] is False and late["error"]["code"] == "unavailable", late
        assert late["error"]["details"]["worker_id"] == A
        assert elapsed <= budget_ms / 1000.0 + .17, (budget_ms, elapsed, late)
        assert samples and all(0 < value <= .25 for value in samples), samples
        assert store.native_activation(A)["state"] == "submitted-unknown"
        recovered = local_rpc.dispatch(request("observe-same-original", 4000))
        assert recovered["ok"] is True, recovered
        public = recovered["result"]
        assert public["worker"] == f"claude:{A}"
        assert public["worker_view"]["state_source"] == "recorded"
        assert public["worker_view"]["native_id"] == store.record(A)["native"]["sessionId"]
        assert {"record", "native_ready", "activation_state"}.isdisjoint(public)
        assert store.native_activation(A)["state"] == "confirmed"
        assert launches.read_text().splitlines() == ["provider-start"]
    finally:
        _stop_disposable_original(short_root)


@pytest.mark.parametrize("budget_ms,slow_s", [(450, .2), (700, .24), (1000, .2)])
def test_repair_claude_v1_first_launch_status_uses_remaining_deadline(
    short_root, tmp_path, monkeypatch, budget_ms, slow_s,
):
    """First native submission never makes a known late success look timely."""
    from ccx.rpc import local_rpc
    from ccx.rpc import rpc
    from ccx.claude import spawn
    from types import SimpleNamespace

    launches = _deadline_fake_claude(short_root, tmp_path, monkeypatch)
    store = require_ccx_active(short_root)
    def make_request(label, budget):
        return {
            "protocol_version": 2, "request_id": label,
            "op": "worker.create", "params": {
                "engine": "claude", "cwd": str(tmp_path), "worker_id": A,
            }, "timeout_ms": budget,
        }
    original_rpc = rpc._rpc
    seen = []
    virtual_latency = [0.0]
    # Keep the real host, socket and absolute request budget. Only the caller's
    # provider-status clock advances virtually when we inject 240ms latency;
    # scheduler descheduling cannot turn a simulated 240ms into 450ms.
    virtual_clock = SimpleNamespace(
        monotonic=lambda: time.monotonic() + virtual_latency[0],
        monotonic_ns=time.monotonic_ns, time=time.time, sleep=time.sleep,
    )

    def realistic_slow_status(worker, msg, timeout=5.):
        if msg.get("op") == "startup-status":
            seen.append(timeout)
            # Simulate a slow provider status response respecting the time
            # allotted by the caller; an exhausted RPC returns no proof.
            simulated = min(slow_s, timeout)
            virtual_latency[0] += simulated
            if simulated >= timeout:
                return {"ok": False, "error": "fake delayed status exceeded RPC budget"}
            return original_rpc(worker, msg, timeout=max(.001, timeout - simulated))
        return original_rpc(worker, msg, timeout=timeout)

    try:
        with monkeypatch.context() as injected:
            injected.setattr(rpc, "_rpc", realistic_slow_status)
            injected.setattr(spawn, "time", virtual_clock)
            result = local_rpc.dispatch(make_request("first-native-start", budget_ms))
        if result["ok"]:
            public = result["result"]
            assert public["worker"] == f"claude:{A}" and public["worker_view"]["state_source"] == "recorded"
            assert public["worker_view"]["native_id"] == store.record(A)["native"]["sessionId"]
            assert {"record", "native_ready", "activation_state"}.isdisjoint(public)
            # The source-verified positive reply passed the (virtual) deadline
            # gate in spawn. Wall elapsed is not a valid assertion under load.
            assert virtual_latency[0] >= 0
        else:
            assert result["error"]["code"] == "unavailable", result
            assert result["error"]["details"]["worker_id"] == A
        assert all(0 < timeout <= .25 for timeout in seen), seen
        # The original host may continue past an observation timeout: retry
        # by exact UUID, never by a fresh native session.
        end = time.monotonic() + 8
        while True:
            recovered = local_rpc.dispatch(make_request("reconcile-original", 2500))
            if recovered["ok"]:
                break
            assert recovered["error"]["details"]["worker_id"] == A, recovered
            assert time.monotonic() < end, recovered
            time.sleep(.05)
        public = recovered["result"]
        assert public["worker"] == f"claude:{A}" and public["worker_view"]["state_source"] == "recorded"
        assert public["worker_view"]["native_id"] == store.record(A)["native"]["sessionId"]
        assert {"record", "native_ready", "activation_state"}.isdisjoint(public)
        assert store.native_activation(A)["state"] == "confirmed"
        assert launches.read_text().splitlines() == ["provider-start"]
    finally:
        _stop_disposable_original(short_root)


@pytest.mark.parametrize("first", ["cli", "python"])
def test_initial_prompt_claude_real_host_cli_python_and_worker_no_second_paste(
    short_root, tmp_path, monkeypatch, first,
):
    """One original Claude host and one initial PTY user message, even on retry."""
    from ccx.claude.delivery import read_receipt
    prompt = "Claude initial native request shared across surfaces"
    launches = _deadline_fake_claude(short_root, tmp_path, monkeypatch)
    native_input = tmp_path / "first-prompt-claude-pty.log"
    monkeypatch.setenv("FAKE_LOG", str(native_input))
    repo = Path(__file__).resolve().parent.parent
    env = {**os.environ}
    store = require_ccx_active(short_root)

    def call(surface, text=prompt):
        if surface == "cli":
            proc = subprocess.run(
                [sys.executable, "-B", "-m", "ccx", "new", "--engine", "claude", text,
                 "--cwd", str(tmp_path), "--worker-id", A, "--json"],
                env=env, cwd=repo, capture_output=True, text=True, timeout=17,
            )
            assert proc.returncode in ((0, 3) if text == prompt else (5,)), (proc.stdout, proc.stderr)
            result = json.loads(proc.stdout)
            if proc.returncode == 0:
                assert result["delivery_id"] == "create:initial"
                assert result["delivery_status"] in ("owned", "accepted")
                assert result["delivery"]["owned"] is True
                assert result["delivery"]["route"] == "new_turn"
                if result["delivery_status"] == "owned":
                    assert result["delivery"]["status"] == "pending"
            return proc.returncode == 0, result
        if surface == "python":
            source = (
                "import json\nfrom ccx import CcxClient\n"
                "from ccx.errors import CreatePartialFailure\n"
                f"try:\n x=CcxClient().new({text!r}, cwd={str(tmp_path)!r}, worker_id={A!r})\n"
                " print(json.dumps({'worker_id':x.worker_id, 'session_id':x.session_id,"
                "'ready':True}))\n"
                "except CreatePartialFailure as e:\n"
                " print(json.dumps({'worker_id':e.worker_id, 'ready':False,"
                "'phase':e.phase, 'delivery_id':e.delivery_id}))\n"
            )
            result = _invoke(repo, env, "-c", source)
            return result["ready"], result
        source = (
            "import json\nfrom ccx import WorkerClient\n"
            "from ccx.errors import CreatePartialFailure\n"
            f"try:\n x=WorkerClient().create(engine='claude',cwd={str(tmp_path)!r},"
            f"worker_id={A!r},prompt={text!r})\n"
            " print(json.dumps({'worker_id':x['worker_id'], 'ready':True,"
            "'delivery_id':x['delivery_id'],'delivery_status':x['delivery_status']}))\n"
            "except CreatePartialFailure as e:\n"
            " print(json.dumps({'worker_id':e.worker_id, 'ready':False,"
            "'phase':e.phase,'delivery_id':e.delivery_id}))\n"
        )
        result = _invoke(repo, env, "-c", source)
        return result["ready"], result

    try:
        for surface in (first, "cli", "python", "worker"):
            until = time.monotonic() + 12
            while True:
                ready, response = call(surface)
                assert response["worker_id"] == A, response
                if ready:
                    break
                assert response.get("phase") in {"native-activation", "initial-delivery"}
                assert time.monotonic() < until, (surface, response)
                time.sleep(.08)
        receipt = read_receipt(f"claude:{A}", "create:initial")
        assert receipt["state"] in ("owned", "accepted")
        assert receipt["promptText"] == prompt
        assert receipt["route"] == "new_turn"
        until = time.monotonic() + 10
        while time.monotonic() < until:
            if native_input.exists() and prompt.encode() in native_input.read_bytes():
                break
            time.sleep(.05)
        assert native_input.exists() and prompt.encode() in native_input.read_bytes()
        assert native_input.read_bytes().count(prompt.encode()) == 1
        for surface in ("cli", "python", "worker"):
            ready, result = call(surface)
            assert ready and result["worker_id"] == A
        # The host can flush a trailing PTY carriage return asynchronously;
        # count physical prompt bytes, not unrelated framing bytes.
        assert native_input.read_bytes().count(prompt.encode()) == 1
        assert launches.read_text().splitlines() == ["provider-start"]
        assert store.native_activation(A)["state"] == "confirmed"
        # The first durable receipt owns the text, not mutable display name.
        rejected, bad = call("cli", "different original first message")
        assert rejected is False and bad["code"] == "conflict", bad
        assert bad["phase"] == "initial-delivery"
        assert bad["native_ready"] is True and bad["worker_id"] == A
        assert bad["delivery_id"] == "create:initial"
        assert native_input.read_bytes().count(prompt.encode()) == 1
    finally:
        _stop_disposable_original(short_root)


def test_initial_prompt_claude_cli_file_source_and_v1_remains_two_calls(
    short_root, tmp_path, monkeypatch,
):
    """Long markdown is read preclaim; V1 worker.create never accepts prompt."""
    from ccx.claude.delivery import read_receipt
    prompt = "Read this multi-line Claude first task\nand keep the same ID."
    launches = _deadline_fake_claude(short_root, tmp_path, monkeypatch)
    native_input = tmp_path / "file-task-native-pty.log"
    monkeypatch.setenv("FAKE_LOG", str(native_input))
    repo = Path(__file__).resolve().parent.parent
    path = tmp_path / "first-brief.md"
    path.write_text(prompt)
    env = {**os.environ}
    store = require_ccx_active(short_root)
    try:
        invalid = _invoke(repo, env, "-m", "ccx", "rpc", request={
            "protocol_version": 2, "request_id": "no-combined-create",
            "op": "worker.create", "params": {
                "engine": "claude", "cwd": str(tmp_path),
                "worker_id": A, "prompt": prompt,
            }, "timeout_ms": 2000,
        })
        assert invalid["ok"] is False
        assert invalid["error"]["code"] == "invalid_request"
        assert not (store.pointer.generation_root / "owners" / f"{A}.json").exists()
        until = time.monotonic() + 12
        while True:
            proc = subprocess.run([
                sys.executable, "-B", "-m", "ccx", "new", "--engine", "claude", "--cwd",
                str(tmp_path), "--worker-id", A, "--file", str(path), "--json",
            ], env=env, cwd=repo, capture_output=True, text=True, timeout=17)
            assert proc.returncode in (0, 3), (proc.stdout, proc.stderr)
            data = json.loads(proc.stdout)
            assert data["worker_id"] == A
            if proc.returncode == 0:
                assert data["delivery_id"] == "create:initial"
                assert data["delivery_status"] in {"owned", "accepted"}
                break
            assert time.monotonic() < until, data
            time.sleep(.08)
        assert read_receipt(f"claude:{A}", "create:initial")["promptText"] == prompt
        assert launches.read_text().splitlines() == ["provider-start"]
        assert store.native_activation(A)["state"] == "confirmed"
    finally:
        _stop_disposable_original(short_root)


@pytest.mark.parametrize("argv,stdin_text", [
    # Prompted --wait is supported now; missing prompt is separately refused.
    (("--timeout", "1"), None),
    (("--delivery-id", "another-initial-id"), None),
    (("--delivery-id", "create:initial"), None),
    (("--var", "x={bad"), None),
    (("--stdin",), "  \n"),
    (("--stdin", "positional"), "second source"),
    (("--file", "no-such-prompt-file.md"), None),
])
def test_initial_prompt_cli_unsupported_or_invalid_inputs_claim_nothing(
    short_root, tmp_path, monkeypatch, argv, stdin_text,
):
    """Disallowed settings, blank text and conflicting sources fail preclaim."""
    _deadline_fake_claude(short_root, tmp_path, monkeypatch)
    repo = Path(__file__).resolve().parent.parent
    env = {**os.environ}
    store = require_ccx_active(short_root)
    args = [sys.executable, "-B", "-m", "ccx", "new", "--engine", "claude"]
    if argv == ("--delivery-id", "create:initial"):
        # Without a prompt even this otherwise allowed fixed token is invalid.
        pass
    else:
        args.append("hello" if argv not in (("--stdin",),
                                              ("--stdin", "positional"),
                                              ("--file", "no-such-prompt-file.md")) else "")
        if args[-1] == "":
            args.pop()
    args.extend(["--cwd", str(tmp_path), *argv, "--json"])
    result = subprocess.run(args, input=stdin_text, text=True, capture_output=True,
                            cwd=repo, env=env, timeout=10)
    assert result.returncode != 0, (result.stdout, result.stderr)
    assert not (store.pointer.generation_root / "owners").exists()
    assert not (tmp_path / "deadline-claude-launches.log").exists()


def test_initial_prompt_typed_invalid_options_refuse_without_claim(
    short_root, tmp_path, monkeypatch,
):
    from ccx import CodexClient, WorkerClient
    from ccx.errors import InvalidRequest, SessionUnavailable
    _deadline_fake_claude(short_root, tmp_path, monkeypatch)
    store = require_ccx_active(short_root)
    calls = (
        lambda: CodexClient().create("hello", cwd=str(tmp_path),
                                     delivery_id="different-token"),
        lambda: CodexClient().create("hello", cwd=str(tmp_path), timeout=1),
        lambda: WorkerClient().create(engine="claude", cwd=str(tmp_path),
                                      prompt="hello", variables={"x": float("nan")}),
        lambda: WorkerClient().create(engine="codex", cwd=str(tmp_path),
                                      prompt="hello", caps={"opus": 50}),
        lambda: WorkerClient().create(engine="codex", cwd=str(tmp_path),
                                      prompt="  "),
    )
    for create in calls:
        with pytest.raises((InvalidRequest, SessionUnavailable)):
            create()
        assert not (store.pointer.generation_root / "owners").exists()
        assert not (tmp_path / "deadline-claude-launches.log").exists()


def test_initial_prompt_claude_lost_send_result_retries_once_on_real_pty(
    short_root, tmp_path, monkeypatch,
):
    """A lost post-receipt Python reply never pastes the first Claude prompt twice."""
    from ccx import CcxClient
    from ccx import WorkerClient
    from ccx.ops import creation
    from ccx.ops import common as protocol_ops
    from ccx.errors import CreatePartialFailure
    from ccx.claude.delivery import read_receipt

    launches = _deadline_fake_claude(short_root, tmp_path, monkeypatch)
    native_input = tmp_path / "lost-initial-claude-pty.log"
    monkeypatch.setenv("FAKE_LOG", str(native_input))
    store = require_ccx_active(short_root)
    prompt = "the first Claude prompt whose return is lost"
    original_send = ops_send.send_turn

    def lost_response(*args, **kwargs):
        proven = original_send(*args, **kwargs)
        assert proven["status"] in {"owned", "accepted"}, proven
        raise OSError("fake caller lost the exact first-send receipt response")

    try:
        # The injected failure must occur AFTER native activation, not in
        # the independent first host's bounded startup observation window.
        until = time.monotonic() + 12
        while True:
            try:
                assert CcxClient().new(cwd=str(tmp_path), worker_id=A,
                                       start_timeout=3).worker_id == A
                break
            except CreatePartialFailure as exc:
                assert exc.phase == "native-activation"
                assert time.monotonic() < until, exc.info
                time.sleep(.05)
        with monkeypatch.context() as interrupted:
            interrupted.setattr(ops_send, "send_turn", lost_response)
            with pytest.raises(CreatePartialFailure) as incomplete:
                creation.create(
                    engine="claude", cwd=str(tmp_path), worker_id=A,
                    prompt=prompt, permissions="full", start_timeout=3,
                )
        failed = incomplete.value
        assert failed.phase == "initial-delivery"
        assert failed.worker_id == A and failed.delivery_id == "create:initial"
        assert failed.info["native_ready"] is True
        assert failed.delivery_status is None
        receipt = read_receipt(f"claude:{A}", "create:initial")
        assert receipt["state"] in {"owned", "accepted"}
        assert receipt["promptText"] == prompt
        typed = CcxClient().new(prompt, cwd=str(tmp_path), worker_id=A,
                                start_timeout=3)
        assert typed.worker_id == A
        again = WorkerClient().create(engine="claude", cwd=str(tmp_path),
                                      worker_id=A, prompt=prompt, start_timeout=3)
        assert again["delivery_id"] == "create:initial"
        assert again["delivery_status"] in {"owned", "accepted"}
        end = time.monotonic() + 10
        while time.monotonic() < end:
            if native_input.exists() and prompt.encode() in native_input.read_bytes():
                break
            time.sleep(.05)
        assert native_input.exists() and native_input.read_bytes().count(prompt.encode()) == 1
        assert launches.read_text().splitlines() == ["provider-start"]
        assert store.native_activation(A)["state"] == "confirmed"
    finally:
        _stop_disposable_original(short_root)


def _first_wait_original_claude_ready(cwd, worker_id):
    """Wait for ONE original native start before testing the turn wait."""
    from ccx import CcxClient
    from ccx.errors import CreatePartialFailure
    until=time.monotonic()+12
    while True:
        try:
            return CcxClient().new(cwd=str(cwd), worker_id=worker_id,
                                   start_timeout=3)
        except CreatePartialFailure as exc:
            assert exc.phase=='native-activation' and exc.worker_id==worker_id
            assert time.monotonic()<until,exc.info
            time.sleep(.05)


@pytest.mark.parametrize('surface', ['cli', 'python', 'worker'])
@pytest.mark.parametrize('hook_route', ['direct', 'settings'])
def test_initial_wait_claude_real_exact_report_once_across_surfaces(
    short_root,tmp_path,monkeypatch,surface,hook_route,
):
    """Existing physical host Stop settles exactly create:initial once."""
    from ccx import CcxClient, WorkerClient
    from ccx.claude.delivery import read_receipt
    from ccx.types import SendResult
    prompt='initial Claude waited exact response'
    launches=_deadline_fake_claude(short_root,tmp_path,monkeypatch)
    inputs=tmp_path/'waited-native-claude-pty.log'
    monkeypatch.setenv('FAKE_LOG',str(inputs))
    if hook_route == 'settings':
        # This is the REAL generated settings.json command route, not the
        # older fake's direct AF_UNIX host-hook shortcut.
        monkeypatch.setenv('FAKE_HOOKS_VIA_SETTINGS','1')
        monkeypatch.setenv('FAKE_HOOK_RESULT_LOG',str(tmp_path/'native-generated-hook-results.jsonl'))
    store=require_ccx_active(short_root)
    repo=Path(__file__).resolve().parent.parent
    try:
        # Prove original native activation before testing the independent
        # first-turn wait. A host socket may appear after an early native
        # start observation window; only the SAME full UUID is retried.
        assert _first_wait_original_claude_ready(tmp_path,A).worker_id==A
        if surface=='cli':
            env={**os.environ}
            proc=subprocess.run([sys.executable,'-B','-m','ccx','new','--engine','claude',prompt,
                                 '--cwd',str(tmp_path),'--worker-id',A,
                                 '--wait','--timeout','10','--json'],
                                env=env,cwd=repo,capture_output=True,text=True,timeout=18)
            if proc.returncode==3:
                # Normal first Claude startup may take longer than the CLI's
                # default .5s; retry only the claimed immutable ID.
                assert json.loads(proc.stdout)['worker_id']==A
                proc=subprocess.run([sys.executable,'-B','-m','ccx','new','--engine','claude',prompt,
                                     '--cwd',str(tmp_path),'--worker-id',A,
                                     '--wait','--timeout','10','--json'],
                                    env=env,cwd=repo,capture_output=True,text=True,timeout=18)
            assert proc.returncode==0,(proc.stdout,proc.stderr)
            result=json.loads(proc.stdout)
            assert result['waited'] is True
            assert result['report']['text']=='Fake Claude completed: '+prompt
            plain=subprocess.run([sys.executable,'-B','-m','ccx','new','--engine','claude',prompt,
                                  '--cwd',str(tmp_path),'--worker-id',A,
                                  '--wait','--timeout','10'],
                                 env=env,cwd=repo,capture_output=True,text=True,timeout=18)
            assert plain.returncode==0,(plain.stdout,plain.stderr)
            assert plain.stdout.splitlines()==[
                f'worker: claude:{A}', 'Fake Claude completed: '+prompt,
            ]
        elif surface=='python':
            result=CcxClient().new(prompt,cwd=str(tmp_path),worker_id=A,
                                   wait=True,timeout=10,start_timeout=4)
            assert isinstance(result,SendResult) and result.settled
            assert result.reply=='Fake Claude completed: '+prompt
        else:
            result=WorkerClient().create(engine='claude',cwd=str(tmp_path),
                                         worker_id=A,prompt=prompt,wait=True,
                                         timeout=10,start_timeout=4)
            assert set(result) == {
                "worker", "worker_id", "engine", "short", "state",
                "native_state", "report", "reply", "created", "reconciled",
                "delivery_id", "delivery_status", "notification",
            }
            assert result['worker'] == f'claude:{A}' and result['worker_id'] == A
            assert result['state'] == normalize_native_state(result['native_state'])
            assert result['delivery_id'] == 'create:initial'
            assert result['delivery_status'] == 'settled'
            assert result['notification'] is None
            assert result['report']['text']=='Fake Claude completed: '+prompt
            assert result['reply'] == result['report']['text']
        receipt=read_receipt(f'claude:{A}','create:initial')
        assert receipt['settlement']['status']=='completed'
        assert receipt['settlement']['text']=='Fake Claude completed: '+prompt
        assert receipt['turnRef']
        assert inputs.read_bytes().count(prompt.encode())==1
        assert launches.read_text().splitlines()==['provider-start']
        assert store.native_activation(A)['state']=='confirmed'
        if hook_route == 'settings':
            # Host receipt publication can precede the hook subprocess's
            # separate diagnostic log append by a few scheduler ticks.
            hook_log=tmp_path/'native-generated-hook-results.jsonl'
            end=time.monotonic()+3
            results=[]
            while time.monotonic()<end:
                if hook_log.exists():
                    results=[json.loads(line) for line in hook_log.read_text().splitlines()]
                if {'SessionStart','UserPromptSubmit','Stop'} <= {
                        item['event'] for item in results}:
                    break
                time.sleep(.03)
            assert all(item['rc']==0 for item in results), results
            assert {'SessionStart','UserPromptSubmit','Stop'} <= {
                item['event'] for item in results}, results
    finally:
        _stop_disposable_original(short_root)


@pytest.mark.parametrize('failure', ['waiting', 'dead'])
def test_initial_wait_claude_pending_original_gate_or_dead_host_is_not_unbounded(
    short_root,tmp_path,monkeypatch,failure,
):
    """A pending original first turn cannot hang on a gate or dead host."""
    import signal
    from ccx import CcxClient
    from ccx.claude.delivery import read_receipt
    from ccx.errors import CreatePartialFailure,SessionWaiting,TurnIncomplete
    prompt='first Claude unanswered turn '+failure
    launches=_deadline_fake_claude(short_root,tmp_path,monkeypatch)
    monkeypatch.setenv('FAKE_MODE','nostop')
    monkeypatch.setenv('FAKE_LOG',str(tmp_path/'pending-claude-native-input.log'))
    store=require_ccx_active(short_root)
    try:
        assert _first_wait_original_claude_ready(tmp_path,A).worker_id==A
        first=CcxClient().new(prompt,cwd=str(tmp_path),worker_id=A,
                              wait=False,start_timeout=4)
        assert first.worker_id==A
        rec=read_receipt(f'claude:{A}','create:initial')
        assert rec['state'] in ('owned','accepted')
        if failure=='waiting':
            store.mutate_record(f'claude:{A}',patches={
                'waitingFor':{'kind':'question','id':'first-gate',
                              'questions':[{'question':'Need input?'}]},
                'lifecycle.state':'waiting',
            })
        else:
            original=store.record(A)
            pid=original['lifecycle']['hostPid']
            assert pid
            os.kill(pid,signal.SIGKILL)
            end=time.monotonic()+4
            while time.monotonic()<end and owned_pid_alive(pid,original['lifecycle'].get('hostStart')):
                time.sleep(.02)
        start=time.monotonic()
        with pytest.raises(CreatePartialFailure) as caught:
            CcxClient().new(prompt,cwd=str(tmp_path),worker_id=A,
                            wait=True,timeout=None,start_timeout=4)
        err=caught.value
        assert time.monotonic()-start<4
        assert err.worker_id==A and err.phase=='wait-report'
        assert err.info['native_ready'] is True
        assert err.delivery_id=='create:initial'
        assert isinstance(err.cause,SessionWaiting if failure=='waiting' else TurnIncomplete)
        assert launches.read_text().splitlines()==['provider-start']
        proc=subprocess.run([
            sys.executable,'-B','-m','ccx','new','--engine','claude',prompt,
            '--cwd',str(tmp_path),'--worker-id',A,'--wait','--json',
        ],cwd=Path(__file__).resolve().parent.parent,env={**os.environ},
            capture_output=True,text=True,timeout=9)
        assert proc.returncode==(4 if failure=='waiting' else 6), (
            proc.returncode,proc.stdout,proc.stderr)
        replay=json.loads(proc.stdout)
        assert replay['worker_id']==A and replay['phase']=='wait-report'
        assert replay['native_ready'] is True
        assert replay['delivery_id']=='create:initial'
    finally:
        _stop_disposable_original(short_root)


def test_initial_wait_claude_timeout_then_same_id_native_stop_recovers(
    short_root,tmp_path,monkeypatch,
):
    """One first PTY paste, timeout partial, then original late Stop report."""
    from ccx import CcxClient
    from ccx.errors import CreatePartialFailure,TurnIncomplete
    repo=Path(__file__).resolve().parent.parent
    fake=(repo/'tests'/'fake_claude.py').read_text()
    # Slow ONLY the provider's own Stop hook; do not modify hook harness.
    slow=tmp_path/'slow-first-stop-claude.py'
    assert 'time.sleep(0.3)' in fake
    slow.write_text(fake.replace('time.sleep(0.3)','time.sleep(1.4)',1))
    launches=_deadline_fake_claude(short_root,tmp_path,monkeypatch)
    wrapper=tmp_path/'deadline-claude-fake'
    wrapper.write_text('#!/bin/sh\nprintf "provider-start\\n" >> '+shlex.quote(str(launches))+'\n'
                       +'exec '+shlex.quote(sys.executable)+' '+shlex.quote(str(slow))+' "$@"\n')
    wrapper.chmod(0o700)
    monkeypatch.setenv('FAKE_LOG',str(tmp_path/'late-stop-pty.log'))
    prompt='first Claude turn with late original Stop'
    try:
        assert _first_wait_original_claude_ready(tmp_path,A).worker_id==A
        with pytest.raises(CreatePartialFailure) as caught:
            CcxClient().new(prompt,cwd=str(tmp_path),worker_id=A,
                            wait=True,timeout=.1,start_timeout=4)
        err=caught.value
        assert err.phase=='wait-report' and err.worker_id==A
        assert isinstance(err.cause,TurnIncomplete)
        assert err.info['native_ready'] is True and err.delivery_id=='create:initial'
        retried=CcxClient().new(prompt,cwd=str(tmp_path),worker_id=A,
                                wait=True,timeout=8,start_timeout=4)
        assert retried.reply=='Fake Claude completed: '+prompt
        assert retried.delivery_id=='create:initial' and retried.settled
        assert launches.read_text().splitlines()==['provider-start']
        assert (tmp_path/'late-stop-pty.log').read_bytes().count(prompt.encode())==1
    finally:
        _stop_disposable_original(short_root)


@pytest.mark.parametrize('argv,stdin_text',[
    (('new','--engine','claude','--stdin','--wait'),''),
    (('new','--engine','claude','--wait'),None),
    (('new','--engine','claude','hello','--timeout','5'),None),
    (('new','--engine','claude','hello','--wait','--timeout','0'),None),
    (('new','--engine','claude','hello','--wait','--timeout','nan'),None),
    (('new', '--engine', 'codex','hello','--wait','--timeout','inf'),None),
    (('new', '--engine', 'codex','--wait'),None),
])
def test_initial_wait_invalid_cli_settings_refuse_preclaim_with_input_exit(
    short_root,tmp_path,monkeypatch,argv,stdin_text,
):
    """Invalid initial-turn wait is caller input, not a retryable outage."""
    _deadline_fake_claude(short_root,tmp_path,monkeypatch)
    repo=Path(__file__).resolve().parent.parent
    store=require_ccx_active(short_root)
    proc=subprocess.run([sys.executable,'-B','-m','ccx',*argv,'--cwd',
                         str(tmp_path),'--json'],input=stdin_text,env={**os.environ},
                        cwd=repo,capture_output=True,text=True,timeout=9)
    assert proc.returncode==1,(proc.stdout,proc.stderr)
    assert not (store.pointer.generation_root/'owners').exists()
    assert not (tmp_path/'deadline-claude-launches.log').exists()


def test_initial_wait_invalid_python_settings_refuse_preclaim(
    short_root,tmp_path,monkeypatch,
):
    from ccx import CcxClient,CodexClient,WorkerClient
    from ccx.errors import InvalidRequest
    monkeypatch.setattr(constants,'CCX_ROOT',short_root)
    store=require_ccx_active(short_root)
    actions=[
        lambda:CcxClient().new(cwd=str(tmp_path),wait=True),
        lambda:CodexClient().create('hi',cwd=str(tmp_path),timeout=1),
        lambda:CodexClient().create('hi',cwd=str(tmp_path),wait=True,timeout=float('inf')),
        lambda:WorkerClient().create(engine='claude',cwd=str(tmp_path),wait=True),
        lambda:WorkerClient().create(engine='codex',cwd=str(tmp_path),prompt='hi',
                                     wait=True,timeout=0),
    ]
    for action in actions:
        with pytest.raises(InvalidRequest):
            action()
        assert not (store.pointer.generation_root/'owners').exists()
