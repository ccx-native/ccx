"""Codex creation across CLI, Python and V1, including initial turns."""
from __future__ import annotations
import ccx.store.root as store_root
import ccx.ops.send as ops_send
import ccx.ops.wait as ops_wait
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time

import pytest

pytestmark = pytest.mark.usefixtures("executable_engine_stubs")
from test_generation_store import short_root, A, B
from ccx import constants
from ccx.store.root import require_ccx_active
from ccx.store.worker_state import normalize_native_state


import hashlib
from contextlib import contextmanager
from test_codex_native_effects import (
    _fake_codex_executable, _f2_fake_processes, _f2_new, _f2_marker, _f2_until_candidate,
)
from test_codex_daemon_evidence import _r1_pending_original_codex
from test_codex_creation_receipts import _l3_ready_original, _l3_original_dispatch

def _m1_start_cli(root, tmp_path, hook, *args):
    repo = Path(__file__).resolve().parent.parent
    env = {**os.environ, "CCX_ROOT": str(root),
           "CCX_CODEX_BIN": str(tmp_path / "fake-codex"),
           "CCX_CODEXD_LINGER": "120", "PYTHONPATH": str(repo)}
    for key in ("CCX_SELF_SHORT", "CCX_SELF_WORKER_ID", "CCX_SELF_ENGINE"):
        env.pop(key, None)
    return subprocess.Popen(
        [sys.executable, "-B", "-c", hook + '\nfrom ccx.cli.main import main\nmain(sys.argv[1:])', *args],
        env=env, cwd=str(repo), stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, text=True,
    )


def _m1_wait_for(path, process=None):
    until = time.monotonic() + 9
    while time.monotonic() < until:
        if path.exists():
            return
        if process is not None and process.poll() is not None:
            break
        time.sleep(.025)
    rc = process.poll() if process is not None else None
    if rc is not None:
        stdout, stderr = process.communicate(timeout=3)
        pytest.fail(f"CLI did not reach {path.name}; process rc={rc}; "
                    f"stdout={stdout!r}; stderr={stderr!r}")
    pytest.fail(f"CLI did not reach {path.name}; process rc={rc}")


def _m1_wire(trace, method):
    import ast
    prefix = f"RPC {method} "
    return [ast.literal_eval(line[len(prefix):])
            for line in trace.read_text().replace("\\n", "\n").splitlines()
            if line.startswith(prefix)]


def test_m1_same_id_status_error_after_peer_confirms_converges_without_replay(
    short_root, tmp_path, monkeypatch,
):
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch, start_delay=.4,
        persisted_threads=tmp_path / "m1-status-history.json",
    ) as (cli, _restart, trace, store, _rpc):
        first = _f2_new(cli, tmp_path, B, "--start-timeout", ".1")
        assert first.returncode == 3, (first.stdout, first.stderr)
        candidate = _f2_until_candidate(store, B)
        assert candidate and candidate.get("nativeCandidate"), candidate
        assert store.native_activation(B)["state"] == "submitted-unknown"
        entered, release = tmp_path / "m1-at-status", tmp_path / "m1-release-status"
        hook = f'''
import sys, time
from pathlib import Path
import ccx.codex.runtime as codex_runtime
original_rpc = codex_runtime.daemon_rpc
def held_status(message, timeout=10):
    if message.get("op") == "native-activation-status":
        Path({str(entered)!r}).touch()
        until = time.monotonic() + 12
        while not Path({str(release)!r}).exists() and time.monotonic() < until:
            time.sleep(.01)
        assert Path({str(release)!r}).exists()
    return original_rpc(message, timeout=timeout)
codex_runtime.daemon_rpc = held_status
'''
        waiting = _m1_start_cli(
            short_root, tmp_path, hook, "new", "--engine", "codex", "--cwd", str(tmp_path),
            "--worker-id", B, "--start-timeout", "6", "--json",
        )
        try:
            _m1_wait_for(entered, waiting)
            peer = _f2_new(cli, tmp_path, B, "--start-timeout", "6")
            assert peer.returncode == 0, (peer.stdout, peer.stderr)
        finally:
            release.touch()
        out, err = waiting.communicate(timeout=18)
        assert waiting.returncode == 0, (out, err)
        assert json.loads(out)["native_ready"] is True
        assert json.loads(peer.stdout)["worker"] == json.loads(out)["worker"] == f"codex:{B}"
        assert store.record(B)["native"]["threadId"] == candidate["nativeCandidate"]["threadId"]
        assert trace.read_text().count("RPC thread/start") == 1
        assert "RPC thread/list" not in trace.read_text()


def test_m1_two_prepared_cli_callers_join_one_original_dispatch(
    short_root, tmp_path, monkeypatch,
):
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch, start_delay=.3,
        persisted_threads=tmp_path / "m1-dispatch-history.json",
    ) as (_cli, _restart, trace, store, _rpc):
        def launch(index):
            entered = tmp_path / f"m1-dispatch-entered-{index}"
            release = tmp_path / f"m1-dispatch-release-{index}"
            observed = tmp_path / f"m1-status-entered-{index}"
            contested = tmp_path / f"m1-dispatch-contested-{index}"
            hook = f'''
import sys, time
from pathlib import Path
import ccx.codex.runtime as codex_runtime
from ccx.store.store import GenerationStore; from ccx.store.root import CreationIndeterminate
native_dispatch = GenerationStore.native_activation_dispatch
native_status = codex_runtime.daemon_rpc
def held_dispatch(self, prepared, **kwargs):
    Path({str(entered)!r}).touch()
    until = time.monotonic() + 12
    while not Path({str(release)!r}).exists() and time.monotonic() < until:
        time.sleep(.01)
    assert Path({str(release)!r}).exists()
    try:
        return native_dispatch(self, prepared, **kwargs)
    except CreationIndeterminate:
        Path({str(contested)!r}).touch()
        raise
GenerationStore.native_activation_dispatch = held_dispatch
def note_status(message, timeout=10):
    if message.get("op") == "native-activation-status":
        Path({str(observed)!r}).touch()
    return native_status(message, timeout=timeout)
codex_runtime.daemon_rpc = note_status
'''
            proc = _m1_start_cli(
                short_root, tmp_path, hook, "new", "--engine", "codex", "--cwd", str(tmp_path),
                "--worker-id", B, "--start-timeout", "8", "--json",
            )
            return proc, entered, release, observed, contested
        first = launch(1)
        second = None
        try:
            # This test needs two ORIGINAL PREPARED callers at native dispatch.
            # Start caller 2 only after caller 1 reaches its dispatch hook,
            # proving the first immutable worker record is fully committed.
            # Starting both immediately also tests a separate, intentionally
            # fail-closed incomplete-first-record window (not this M1 test).
            _m1_wait_for(first[1], first[0])
            second = launch(2)
            _m1_wait_for(second[1], second[0])
            # Both children reached dispatch with the SAME original prepared
            # token. Finish caller 1's original bounded admission before
            # releasing the rival: releasing on mere entry to a status RPC
            # lets a competing record/claim lock preempt caller 1 under load.
            first[2].touch()
            _m1_wait_for(first[3], first[0])
            out_a, err_a = first[0].communicate(timeout=20)
            assert first[0].returncode == 0, (out_a, err_a)
            second[2].touch()
            out_b, err_b = second[0].communicate(timeout=20)
        except BaseException as error:
            # Original process failures can occur before their hook touches
            # the entered marker. Preserve BOTH CLI stdout/stderr streams and
            # the originating assertion, not just an opaque rc=3. Release the
            # exact test-owned gates before collecting the original children.
            first[2].touch()
            if second is not None:
                second[2].touch()
            diagnostics = []
            launched = [("first", first[0])]
            if second is not None:
                launched.append(("second", second[0]))
            for name, proc in launched:
                try:
                    stdout, stderr = proc.communicate(timeout=8)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    stdout, stderr = proc.communicate(timeout=5)
                diagnostics.append((name, proc.returncode, stdout, stderr))
            pytest.fail(f"{error}; CLI original caller outputs={diagnostics!r}")
        finally:
            first[2].touch()
            if second is not None:
                second[2].touch()
            processes = (first[0],) + ((second[0],) if second is not None else ())
            for proc in processes:
                if proc.poll() is None:
                    proc.kill()
                    proc.communicate(timeout=5)
        assert first[0].returncode == 0, (out_a, err_a)
        first_result = json.loads(out_a)
        assert first_result["ok"] is True and first_result["native_ready"] is True
        assert first_result["worker"] == f"codex:{B}"
        assert first_result["worker_id"] == B
        second_result = json.loads(out_b)
        assert second_result["worker"] == f"codex:{B}"
        assert second_result["worker_id"] == B
        assert second_result["engine"] == "codex"
        if second[0].returncode == 0:
            assert second_result["ok"] is True and second_result["native_ready"] is True
            assert second[4].exists(), "loser must encounter competing native dispatch"
        else:
            # Bounded exclusive creation-lock contention is a documented,
            # retryable NO-DISPATCH result. No other partial failure qualifies.
            assert second[0].returncode == 3, (out_b, err_b)
            assert second_result["ok"] is False
            assert second_result["code"] == "partial_failure"
            assert second_result["cause_type"] == "GenerationBusy"
        assert store.native_activation(B)["state"] == "confirmed"
        assert store.record(B)["native"]["threadId"] == _f2_until_candidate(store, B)[
            "nativeCandidate"]["threadId"]
        assert trace.read_text().count("RPC thread/start") == 1
        assert "RPC thread/list" not in trace.read_text()
        if second[0].returncode == 3:
            # One intentional SAME CLI retry, AFTER caller 1 has finished and
            # the competing original lock was released. No native re-submit.
            retried = _f2_new(_cli, tmp_path, B, "--start-timeout", "8")
            assert retried.returncode == 0, (retried.stdout, retried.stderr)
            retry_result = json.loads(retried.stdout)
            assert retry_result["ok"] is True and retry_result["native_ready"] is True
            assert retry_result["worker"] == f"codex:{B}"
            assert retry_result["worker_id"] == B
            assert trace.read_text().count("RPC thread/start") == 1
            assert "RPC thread/list" not in trace.read_text()


def test_m1_confirmed_same_uuid_join_under_original_creation_lock_contention(
    short_root, tmp_path, monkeypatch,
):
    """Compatible same-ID CLI refuses typed busy, then joins after lock release.

    This is separate from the prepared-dispatch ordering fixture. The first
    fake native start has already completed and is durably confirmed. A thread
    holds only THIS worker's original exclusive creation lock while a second
    normal CLI attempts to join the known exact-ID worker. Real fcntl locks and the
    normal CLI path are used; no provider is contacted or any effect replayed.
    """
    with _f2_fake_processes(short_root, tmp_path, monkeypatch) as (
        cli, _restart, trace, store, _rpc,
    ):
        first = _f2_new(cli, tmp_path, B, "--start-timeout", "8")
        assert first.returncode == 0, (first.stdout, first.stderr)
        assert store.native_activation(B)["state"] == "confirmed"
        assert trace.read_text().count("RPC thread/start") == 1

        held = threading.Event()
        release = threading.Event()
        errors = []

        def hold_original_creation_lock():
            try:
                with store._claim_lock(B):
                    held.set()
                    assert release.wait(timeout=8), "test failed to release original creation lock"
            except BaseException as exc:
                errors.append(exc)
                held.set()

        holder = threading.Thread(target=hold_original_creation_lock, daemon=True)
        holder.start()
        try:
            assert held.wait(timeout=4) and not errors, errors
            joined = _f2_new(cli, tmp_path, B, "--start-timeout", "8")
        finally:
            release.set()
            holder.join(timeout=8)

        assert not holder.is_alive() and not errors, errors
        assert store.native_activation(B)["state"] == "confirmed"
        assert trace.read_text().count("RPC thread/start") == 1
        assert joined.returncode == 3, (joined.stdout, joined.stderr)
        refused = json.loads(joined.stdout)
        assert refused["ok"] is False
        assert refused["code"] == "partial_failure"
        assert refused["cause_type"] == "GenerationBusy"
        assert refused["worker"] == f"codex:{B}"
        assert refused["worker_id"] == B
        # The SAME exact-ID CLI call after release joins the positive original
        # rather than minting another UUID or submitting another thread/start.
        retried = _f2_new(cli, tmp_path, B, "--start-timeout", "8")
        assert retried.returncode == 0, (retried.stdout, retried.stderr)
        retry_result = json.loads(retried.stdout)
        assert retry_result["ok"] is True and retry_result["native_ready"] is True
        assert retry_result["worker"] == f"codex:{B}"
        assert retry_result["worker_id"] == B
        assert store.native_activation(B)["state"] == "confirmed"
        assert trace.read_text().count("RPC thread/start") == 1
        assert "RPC thread/list" not in trace.read_text()


def test_m1_original_claim_confirmation_survives_cli_crash_before_record_binding(
    short_root, tmp_path, monkeypatch,
):
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch,
        persisted_threads=tmp_path / "m1-crash-history.json",
    ) as (cli, _restart, trace, store, _rpc):
        stopped = tmp_path / "m1-crashed-after-confirm"
        hook = f'''
import os, sys
from pathlib import Path
from ccx.store.store import GenerationStore
original_bind = GenerationStore.bind_confirmed_native
def crash_before_bind(self, target):
    if target == "codex:{B}" and self.native_activation(target)["state"] == "confirmed":
        Path({str(stopped)!r}).touch()
        os._exit(47)
    return original_bind(self, target)
GenerationStore.bind_confirmed_native = crash_before_bind
'''
        process = _m1_start_cli(
            short_root, tmp_path, hook, "new", "--engine", "codex", "--cwd", str(tmp_path),
            "--worker-id", B, "--start-timeout", "5", "--json",
        )
        out, err = process.communicate(timeout=15)
        assert process.returncode == 47 and stopped.exists(), (out, err)
        assert store.native_activation(B)["state"] == "confirmed"
        assert store.record(B)["native"]["threadId"] is None
        retried = _f2_new(cli, tmp_path, B, "--start-timeout", "5")
        assert retried.returncode == 0, (retried.stdout, retried.stderr)
        assert json.loads(retried.stdout)["worker_id"] == B
        assert store.record(B)["native"]["threadId"] == _f2_until_candidate(store, B)[
            "nativeCandidate"]["threadId"]
        assert trace.read_text().count("RPC thread/start") == 1


@pytest.mark.parametrize("initial_model,initial_effort", [
    (None, None), ("gpt-m1-wire", None), (None, "high"), ("gpt-m1-wire", "high"),
])
def test_m1_native_model_and_effort_wire_uses_current_durable_settings_not_original_hash(
    short_root, tmp_path, monkeypatch, initial_model, initial_effort,
):
    from ccx.codex.delivery import read_receipt
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch,
        persisted_threads=tmp_path / "m1-config-history.json",
    ) as (cli, _restart, trace, store, _rpc):
        overrides = (["--model", initial_model] if initial_model else []) + (
            ["--effort", initial_effort] if initial_effort else [])
        created = _f2_new(cli, tmp_path, B, "--start-timeout", "5", *overrides)
        assert created.returncode == 0, (created.stdout, created.stderr)
        starts = _m1_wire(trace, "thread/start")
        assert len(starts) == 1 and starts[0]["threadSource"] == f"ccx-worker:{B}"
        assert starts[0].get("model") == initial_model
        assert ("model" in starts[0]) is (initial_model is not None)
        assert "effort" not in starts[0]  # absent from ThreadStartParams
        original_sha = store.record(B)["createSha256"]
        token = "m1-first-semantic-delivery"
        first = cli("send", B, "m1 first turn", "--no-wait",
                    "--delivery-id", token, "--json")
        assert first.returncode == 0, (first.stdout, first.stderr)
        turns = _m1_wire(trace, "turn/start")
        assert len(turns) == 1, turns
        assert turns[0].get("model") == initial_model
        assert turns[0].get("effort") == initial_effort
        assert ("model" in turns[0]) is (initial_model is not None)
        assert ("effort" in turns[0]) is (initial_effort is not None)
        assert turns[0]["clientUserMessageId"] == read_receipt(B, token)[
            "clientUserMessageId"]
        repeated = cli("send", B, "m1 first turn", "--no-wait",
                       "--delivery-id", token, "--json")
        assert repeated.returncode == 0, (repeated.stdout, repeated.stderr)
        assert len(_m1_wire(trace, "turn/start")) == 1
        until = time.monotonic() + 8
        while time.monotonic() < until:
            if store.record(B)["lifecycle"]["state"] == "idle":
                break
            time.sleep(.04)
        else:
            pytest.fail("first fake native turn did not settle")
        store.mutate_record(B, patches={
            "createArgs.model": "gpt-m1-changed", "createArgs.effort": "medium",
        })
        second = cli("send", B, "m1 changed configuration", "--no-wait",
                     "--delivery-id", "m1-second-semantic-delivery", "--json")
        assert second.returncode == 0, (second.stdout, second.stderr)
        turns = _m1_wire(trace, "turn/start")
        assert len(turns) == 2, turns
        assert turns[1]["model"] == "gpt-m1-changed"
        assert turns[1]["effort"] == "medium"
        assert turns[1]["clientUserMessageId"] == read_receipt(
            B, "m1-second-semantic-delivery")["clientUserMessageId"]
        assert store.record(B)["createSha256"] == original_sha
        replay = _f2_new(cli, tmp_path, B, "--start-timeout", "5", *overrides)
        assert replay.returncode == 0, (replay.stdout, replay.stderr)
        assert store.record(B)["createArgs"]["model"] == "gpt-m1-changed"
        assert store.record(B)["createArgs"]["effort"] == "medium"
        assert trace.read_text().count("RPC thread/start") == 1
        for mismatch in (("--model", "different-original"),
                         ("--effort", "xhigh"),
                         ("--sandbox", "read-only")):
            conflict = _f2_new(cli, tmp_path, B, "--start-timeout", "5", *mismatch)
            assert conflict.returncode != 0, (conflict.stdout, conflict.stderr)
        other_cwd = tmp_path / "different-project"
        other_cwd.mkdir()
        conflict = _f2_new(cli, other_cwd, B, "--start-timeout", "5")
        assert conflict.returncode != 0
        assert trace.read_text().count("RPC thread/start") == 1


@pytest.mark.parametrize("flag,value,exit_code", [
    ("--start-timeout", "0", 1),
    ("--start-timeout", "nan", 1),
    ("--timeout", "0.2", 1),
])
def test_m2_invalid_or_unsupported_create_timeout_claims_nothing(
    short_root, tmp_path, monkeypatch, flag, value, exit_code,
):
    """Bad observation budget / premature --wait flag never mints an owner."""
    with _f2_fake_processes(short_root, tmp_path, monkeypatch) as (
        cli, _restart, trace, store, _rpc,
    ):
        claims = store.pointer.generation_root / "claims"
        before = sorted(path.name for path in claims.glob("*.json"))
        for include_id in (True, False):
            args = ["new", "--engine", "codex", "--cwd", str(tmp_path)]
            if include_id:
                args += ["--worker-id", B]
            invalid = cli(*args, f"{flag}={value}", "--json")
            assert invalid.returncode == exit_code, (
                flag, value, invalid.stdout, invalid.stderr,
            )
            assert not invalid.stdout
            if flag == "--timeout":
                assert "--timeout requires --wait" in invalid.stderr
            else:
                assert flag in invalid.stderr
            assert "no worker identity claimed" in invalid.stderr
            assert sorted(path.name for path in claims.glob("*.json")) == before
            assert not (claims / f"{B}.json").exists()
            assert "RPC thread/start" not in trace.read_text()


def test_m2_subsecond_observation_is_only_status_budget_and_same_id_recovers(
    short_root, tmp_path, monkeypatch,
):
    """A slow status IPC cannot use the old >=.2s floor or poll past expiry."""
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch, start_delay=1.0,
        persisted_threads=tmp_path / "m2-original-threads.json",
    ) as (cli, _restart, trace, store, _rpc):
        captures = tmp_path / "m2-status-timeouts"
        hook = f'''
import sys, time
from pathlib import Path
import ccx.codex.runtime as codex_runtime
original_rpc = codex_runtime.daemon_rpc
def held_status(message, timeout=10):
    if message.get("op") == "native-activation-status":
        with Path({str(captures)!r}).open("a") as out:
            out.write(str(timeout) + "\\n")
        time.sleep(.30)  # exceed ONLY the observation budget
        return {{"ok": True, "status": "indeterminate"}}
    return original_rpc(message, timeout=timeout)
codex_runtime.daemon_rpc = held_status
'''
        process = _m1_start_cli(
            short_root, tmp_path, hook, "new", "--engine", "codex", "--cwd", str(tmp_path),
            "--worker-id", B, "--start-timeout", ".15", "--json",
        )
        try:
            stdout, stderr = process.communicate(timeout=15)
            assert process.returncode == 3, (stdout, stderr)
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate(timeout=5)
        partial = json.loads(stdout)
        assert partial["worker_id"] == B and not partial["native_ready"]
        assert partial["activation_state"] == "submitted-unknown"
        assert "submitted-unknown" in (partial["native_start_error"] or "")
        samples = [float(value) for value in captures.read_text().splitlines()]
        assert len(samples) == 1, samples  # no second status after expiry
        assert 0 < samples[0] <= .15 and samples[0] < .2, samples
        candidate = _f2_until_candidate(store, B)
        assert candidate and candidate.get("nativeCandidate"), candidate
        assert trace.read_text().count("RPC thread/start") == 1
        assert store.native_activation(B)["state"] == "submitted-unknown"
        recovered = _f2_new(cli, tmp_path, B, "--start-timeout", "5")
        assert recovered.returncode == 0, (recovered.stdout, recovered.stderr)
        assert json.loads(recovered.stdout)["native_ready"] is True
        assert store.record(B)["native"]["threadId"] == candidate[
            "nativeCandidate"]["threadId"]
        assert trace.read_text().count("RPC thread/start") == 1
        assert "RPC thread/list" not in trace.read_text()


def _parity_process(root, fake_dir, *, python=None, request=None):
    """Real separate Python or V1 client against one disposable fake daemon."""
    repo = Path(__file__).resolve().parent.parent
    env = {**os.environ, "CCX_ROOT": str(root),
           "CCX_CODEX_BIN": str(fake_dir / "fake-codex"),
           "CCX_CODEXD_LINGER": "120", "PYTHONPATH": str(repo)}
    for name in ("CCX_SELF_SHORT", "CCX_SELF_WORKER_ID", "CCX_SELF_ENGINE"):
        env.pop(name, None)
    assert (python is None) != (request is None)
    command = ([sys.executable, "-B", "-c", python] if python is not None
               else [sys.executable, "-B", "-m", "ccx", "rpc"])
    proc = subprocess.run(command, env=env, cwd=repo,
                          input=json.dumps(request) if request else None,
                          capture_output=True, text=True, timeout=20)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)
    return json.loads(proc.stdout)


@pytest.mark.parametrize("order", [
    ("cli", "codex-client", "worker-client", "v1"),
    ("v1", "worker-client", "codex-client", "cli"),
    ("codex-client", "v1", "cli", "worker-client"),
])
def test_create_parity_codex_cli_python_worker_and_v1_same_native_effect(
    short_root, tmp_path, monkeypatch, order,
):
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch,
        persisted_threads=tmp_path / "parity-codex-threads.json",
    ) as (cli, _restart, trace, store, _rpc):
        outputs = {}
        for surface in order:
            if surface == "cli":
                proc = _f2_new(cli, tmp_path, B, "--start-timeout", "5")
                assert proc.returncode == 0, (proc.stdout, proc.stderr)
                response = json.loads(proc.stdout)
                assert response["native_ready"] is True
                outputs[surface] = response["worker_id"]
            elif surface == "codex-client":
                response = _parity_process(short_root, tmp_path, python=(
                    "import json\nfrom ccx import CodexClient\n"
                    f"x=CodexClient().create(cwd={str(tmp_path)!r},worker_id={B!r})\n"
                    "print(json.dumps(x.as_dict()))"
                ))
                assert response["thread_id"] and response["worker_id"] == B
                outputs[surface] = response["worker_id"]
            elif surface == "worker-client":
                response = _parity_process(short_root, tmp_path, python=(
                    "import json\nfrom ccx import WorkerClient\n"
                    f"x=WorkerClient().create(engine='codex',cwd={str(tmp_path)!r},worker_id={B!r})\n"
                    "print(json.dumps(x))"
                ))
                assert set(response) == {
                    "worker", "worker_id", "engine", "short", "state",
                    "native_state", "report", "reply", "created", "reconciled",
                    "delivery_id", "delivery_status", "notification",
                }
                assert response["worker"] == f"codex:{B}" and response["engine"] == "codex"
                assert response["worker_id"] == B and response["short"] == B[:8]
                assert response["state"] == normalize_native_state(response["native_state"])
                assert all(response[key] is None for key in (
                    "report", "reply", "delivery_id", "delivery_status", "notification",
                ))
                # Original native proof is independent of the neutral facade.
                assert store.native_activation(B)["state"] == "confirmed"
                assert store.record(B)["native"]["threadId"] not in (None, B)
                outputs[surface] = response["worker_id"]
            else:
                response = _parity_process(short_root, tmp_path, request={
                    "protocol_version": 2, "request_id": "native-parity",
                    "op": "worker.create", "params": {
                        "engine": "codex", "cwd": str(tmp_path), "worker_id": B,
                    }, "timeout_ms": 8000,
                })
                assert response["ok"] is True, response
                public = response["result"]
                assert public["worker"] == f"codex:{B}"
                assert public["worker_view"]["state_source"] == "recorded"
                assert public["worker_view"]["native_id"] == store.record(B)["native"]["threadId"]
                assert {"record", "native_ready", "activation_state", "native_source"}.isdisjoint(public)
                outputs[surface] = public["worker"].split(":", 1)[1]
        assert outputs == {surface: B for surface in order}
        claimed = store.native_activation(B)
        assert claimed["state"] == "confirmed"
        assert store.record(B)["native"]["threadId"] == claimed[
            "nativeBinding"]["threadId"]
        assert trace.read_text().count("RPC thread/start") == 1
        assert "RPC thread/list" not in trace.read_text()
        # Both engines advertise native-ready (not preparation-only) create.
        caps = _parity_process(short_root, tmp_path, request={
            "protocol_version": 2, "request_id": "caps", "op": "system.info",
            "params": {},
        })["result"]
        assert set(caps) == {"protocol_version", "ccx_version"}
        assert caps["protocol_version"] == 2


def test_create_parity_codex_lost_original_reply_then_v1_and_python_converge(
    short_root, tmp_path, monkeypatch,
):
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch, start_delay=1.0,
        persisted_threads=tmp_path / "parity-lost-reply.json",
    ) as (cli, _restart, trace, store, _rpc):
        first = _f2_new(cli, tmp_path, B, "--start-timeout", ".12")
        assert first.returncode == 3, (first.stdout, first.stderr)
        assert json.loads(first.stdout)["activation_state"] == "submitted-unknown"
        candidate = _f2_until_candidate(store, B)
        assert candidate and candidate.get("nativeCandidate")
        v1 = _parity_process(short_root, tmp_path, request={
            "protocol_version": 2, "request_id": "recovered",
            "op": "worker.create", "params": {
                "engine": "codex", "cwd": str(tmp_path), "worker_id": B,
            }, "timeout_ms": 5000,
        })
        assert v1["ok"] is True, v1
        python = _parity_process(short_root, tmp_path, python=(
            "import json\nfrom ccx import CodexClient\n"
            f"x=CodexClient().create(cwd={str(tmp_path)!r},worker_id={B!r})\n"
            "print(json.dumps(x.as_dict()))"
        ))
        assert python["thread_id"] == candidate["nativeCandidate"]["threadId"]
        assert v1["result"]["worker_view"]["native_id"] == python["thread_id"]
        assert v1["result"]["worker_view"]["state_source"] == "recorded"
        assert trace.read_text().count("RPC thread/start") == 1


def test_create_parity_terminal_codex_provider_refusal_replays_to_v1_and_python(
    short_root, tmp_path, monkeypatch,
):
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch,
        start_error_kind="coded", start_error_code=-32133,
        start_error_message="original scripted provider refusal",
    ) as (cli, _restart, trace, store, _rpc):
        first = _f2_new(cli, tmp_path, B, "--start-timeout", "2")
        assert first.returncode == 3
        verdict = json.loads(first.stdout)
        assert verdict["code"] == "native_rejected", verdict
        result = _parity_process(short_root, tmp_path, request={
            "protocol_version": 2, "request_id": "offline-provider-refusal",
            "op": "worker.create", "params": {
                "engine": "codex", "cwd": str(tmp_path), "worker_id": B,
            }, "timeout_ms": 3000,
        })
        assert result["ok"] is False and result["error"]["code"] == "refused"
        assert result["error"]["details"]["native_rejection"] == verdict["native_rejection"]
        answer = _parity_process(short_root, tmp_path, python=(
            "import json\nfrom ccx import CodexClient\nfrom ccx.errors import CreatePartialFailure\n"
            f"try: CodexClient().create(cwd={str(tmp_path)!r},worker_id={B!r})\n"
            "except CreatePartialFailure as e: print(json.dumps(e.info['native_rejection']))\n"
        ))
        assert answer == verdict["native_rejection"]
        assert trace.read_text().count("RPC thread/start") == 1


def test_create_parity_durable_local_refusal_replays_offline_across_v1_and_python(
    short_root, tmp_path, monkeypatch,
):
    store, daemon, client, _admit, _answer, _status = _r1_pending_original_codex(
        short_root, tmp_path, monkeypatch,
    )
    prepared, activation = _l3_ready_original(store, B, tmp_path)
    daemon.ready.clear()
    proof = _l3_original_dispatch(store, daemon, prepared, activation)
    assert proof["localRefusalProven"] is True
    assert store.reconcile_codex_local_refusal(B)["reason"] == "Codex app-server not ready"
    result = _parity_process(short_root, tmp_path, request={
        "protocol_version": 2, "request_id": "offline-local-refusal",
        "op": "worker.create", "params": {
            "engine": "codex", "cwd": str(tmp_path), "worker_id": B,
            "idle_timeout": 900,  # fixture's exact frozen original createArgs
        }, "timeout_ms": 3000,
    })
    assert result["ok"] is False and result["error"]["code"] == "refused", result
    local = result["error"]["details"]["native_local_refusal"]
    assert local["proven"] is True and local["reason"] == "Codex app-server not ready"
    answer = _parity_process(short_root, tmp_path, python=(
        "import json\nfrom ccx import CodexClient\nfrom ccx.errors import CreatePartialFailure\n"
        f"try: CodexClient().create(cwd={str(tmp_path)!r},worker_id={B!r},idle_timeout=900)\n"
        "except CreatePartialFailure as e: print(json.dumps(e.info['native_local_refusal']))\n"
    ))
    assert answer == local
    assert client.native_writes == []


def test_create_parity_v1_wins_original_dispatch_against_held_cli_without_second_start(
    short_root, tmp_path, monkeypatch,
):
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch, start_delay=.25,
        persisted_threads=tmp_path / "parity-contended-native.json",
    ) as (_cli, _restart, trace, store, _rpc):
        entered, release = tmp_path / "parity-cli-admit-entered", tmp_path / "parity-cli-release"
        hook = f'''
import sys, time
from pathlib import Path
from ccx.store.store import GenerationStore
original = GenerationStore.native_activation_dispatch
def hold(self, prepared, **kwargs):
    Path({str(entered)!r}).touch()
    until = time.monotonic() + 12
    while not Path({str(release)!r}).exists() and time.monotonic() < until:
        time.sleep(.01)
    assert Path({str(release)!r}).exists()
    return original(self, prepared, **kwargs)
GenerationStore.native_activation_dispatch = hold
'''
        waiting = _m1_start_cli(
            short_root, tmp_path, hook, "new", "--engine", "codex", "--cwd", str(tmp_path),
            "--worker-id", B, "--start-timeout", "6", "--json",
        )
        try:
            _m1_wait_for(entered, waiting)
            winner = _parity_process(short_root, tmp_path, request={
                "protocol_version": 2, "request_id": "v1-physical-race",
                "op": "worker.create", "params": {
                    "engine": "codex", "cwd": str(tmp_path), "worker_id": B,
                }, "timeout_ms": 6000,
            })
            assert winner["ok"], winner
            assert winner["result"]["worker"].split(":", 1)[1] == B
            assert winner["result"]["worker_view"]["state_source"] == "recorded"
            assert winner["result"]["worker_view"]["native_id"]
        finally:
            release.touch()
        stdout, stderr = waiting.communicate(timeout=18)
        assert waiting.returncode == 0, (stdout, stderr)
        assert json.loads(stdout)["native_ready"] is True
        assert store.native_activation(B)["state"] == "confirmed"
        assert trace.read_text().count("RPC thread/start") == 1
        assert store.record(B)["native"]["threadId"] == winner["result"]["worker_view"]["native_id"]


def test_create_parity_v1_short_total_budget_keeps_original_and_reconciles(
    short_root, tmp_path, monkeypatch,
):
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch, start_delay=1.,
        persisted_threads=tmp_path / "parity-v1-short-budget.json",
    ) as (_cli, _restart, trace, store, _rpc):
        started = time.monotonic()
        first = _parity_process(short_root, tmp_path, request={
            "protocol_version": 2, "request_id": "deadline-150ms",
            "op": "worker.create", "params": {
                "engine": "codex", "cwd": str(tmp_path), "worker_id": B,
            }, "timeout_ms": 150,
        })
        assert time.monotonic() - started < 2., first  # subprocess startup is external
        assert not first["ok"], first
        assert first["error"]["details"]["worker_id"] == B
        first_code = first["error"]["code"]
        assert first_code in {"unavailable", "refused"}, first
        # A bounded admission may prove local non-submission before the tiny
        # deadline. That exact L3 terminal proof MUST replay, not restart.
        later = _parity_process(short_root, tmp_path, request={
            "protocol_version": 2, "request_id": "same-original-recover",
            "op": "worker.create", "params": {
                "engine": "codex", "cwd": str(tmp_path), "worker_id": B,
            }, "timeout_ms": 5000,
        })
        if first_code == "refused":
            assert first["error"]["details"]["native_local_refusal"]["proven"] is True
            assert not later["ok"] and later["error"]["code"] == "refused", later
            assert later["error"]["details"]["native_local_refusal"] == (first[
                "error"]["details"]["native_local_refusal"])
            assert trace.read_text().count("RPC thread/start") == 0
        elif store.native_activation(B)["state"] == "submitted-unknown":
            if _f2_until_candidate(store, B) and store.native_activation(B)["state"] == "submitted-unknown":
                # A physical start did happen: one original candidate is the
                # only admissible thread on this exact full UUID.
                if not later["ok"]:
                    later = _parity_process(short_root, tmp_path, request={
                        "protocol_version": 2, "request_id": "same-original-observe",
                        "op": "worker.create", "params": {
                            "engine": "codex", "cwd": str(tmp_path), "worker_id": B,
                        }, "timeout_ms": 5000,
                    })
                assert later["ok"], later
                assert later["result"]["worker"].split(":", 1)[1] == B
                assert later["result"]["worker_view"]["native_id"] == store.record(B)["native"]["threadId"]
                assert later["result"]["worker_view"]["state_source"] == "recorded"
                assert trace.read_text().count("RPC thread/start") == 1
            else:
                assert not later["ok"] and trace.read_text().count("RPC thread/start") <= 1
        else:
            assert not later["ok"] or (
                later["result"]["worker"].split(":", 1)[1] == B
                and later["result"]["worker_view"]["native_id"] == store.record(B)["native"]["threadId"]
            )
            assert trace.read_text().count("RPC thread/start") <= 1


@pytest.mark.parametrize("failure", [
    "StaleGeneration", "StorageIntegrityError", "CreationIndeterminate", "CreationConflict",
])
@pytest.mark.parametrize("surface", ["cli", "python", "v1"])
def test_repair_postclaim_minted_uuid_survives_native_bind_error_on_every_surface(
    short_root, tmp_path, monkeypatch, surface, failure,
):
    """One real thread/start, failed postclaim bind, recover exactly minted ID."""
    import uuid
    from ccx import CodexClient
    from ccx.rpc import local_rpc
    from ccx.errors import CreatePartialFailure
    from ccx.store import store as generation

    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch,
        persisted_threads=tmp_path / f"minted-{surface}-{failure}.json",
    ) as (cli, _restart, trace, store, _rpc):
        if surface == "cli":
            # The wrapper changes ONLY the CLI process, never codexd or the
            # physical fake app-server, and omits --worker-id entirely.
            hook = f'''
import sys
from ccx.store.store import GenerationStore, {failure}
original_bind = GenerationStore.bind_confirmed_native
def fail_one_postclaim_bind(self, target):
    if target.startswith('codex:') and self.native_activation(target)['state'] == 'confirmed':
        raise {failure}('injected original native binding failure')
    return original_bind(self, target)
GenerationStore.bind_confirmed_native = fail_one_postclaim_bind
'''
            proc = _m1_start_cli(
                short_root, tmp_path, hook, "new", "--engine", "codex", "--cwd", str(tmp_path),
                "--start-timeout", "5", "--json",
            )
            stdout, stderr = proc.communicate(timeout=18)
            assert proc.returncode == 3, (stdout, stderr)
            partial = json.loads(stdout)
            worker_id = partial["worker_id"]
            assert partial["cause_type"] == failure, partial
            assert partial["activation_state"] == "confirmed", partial
            assert partial["native_ready"] is False
            if failure == "StorageIntegrityError":
                assert partial["integrity_error"] is True
        else:
            monkeypatch.setenv("CCX_CODEX_BIN", str(tmp_path / "fake-codex"))
            original_bind = generation.GenerationStore.bind_confirmed_native
            failure_type = getattr(generation, failure)

            def fail_one_postclaim_bind(self, target):
                if (target.startswith("codex:")
                        and self.native_activation(target)["state"] == "confirmed"):
                    raise failure_type("injected original native binding failure")
                return original_bind(self, target)

            with monkeypatch.context() as patch:
                patch.setattr(generation.GenerationStore, "bind_confirmed_native",
                              fail_one_postclaim_bind)
                if surface == "python":
                    with pytest.raises(CreatePartialFailure) as caught:
                        CodexClient().create(cwd=str(tmp_path), start_timeout=5)
                    partial = caught.value
                    worker_id = partial.worker_id
                    assert isinstance(partial.cause, failure_type)
                    assert partial.info["cause_type"] == failure
                    assert partial.info["activation_state"] == "confirmed"
                    if failure == "StorageIntegrityError":
                        assert partial.info["integrity_error"] is True
                else:
                    reply = local_rpc.dispatch({
                        "protocol_version": 2,
                        "request_id": f"minted-{failure}", "op": "worker.create",
                        "params": {"engine": "codex", "cwd": str(tmp_path)},
                        "timeout_ms": 7000,
                    })
                    assert reply["ok"] is False, reply
                    assert reply["error"]["code"] == "unavailable", reply
                    details = reply["error"]["details"]
                    worker_id = details["worker_id"]
                    assert details["cause_type"] == failure, reply
                    assert details["activation_state"] == "confirmed", reply
                    if failure == "StorageIntegrityError":
                        assert details["integrity_error"] is True
        assert uuid.UUID(worker_id).version == 4
        assert store.native_activation(worker_id)["state"] == "confirmed"
        assert store.record(worker_id)["native"]["threadId"] is None
        assert trace.read_text().count("RPC thread/start") == 1
        recovered = _f2_new(cli, tmp_path, worker_id, "--start-timeout", "5")
        assert recovered.returncode == 0, (recovered.stdout, recovered.stderr)
        assert json.loads(recovered.stdout)["worker_id"] == worker_id
        assert store.record(worker_id)["native"]["threadId"]
        assert trace.read_text().count("RPC thread/start") == 1


def test_repair_secondary_original_status_failure_cannot_mask_minted_uuid_or_cause(
    short_root, tmp_path, monkeypatch,
):
    """If postclaim proof reads also break, preserve the FIRST native-bind error."""
    import uuid
    from ccx import CodexClient
    from ccx.store import store as generation
    from ccx.errors import CreatePartialFailure

    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch,
        persisted_threads=tmp_path / "minted-secondary-proof-failure.json",
    ) as (cli, _restart, trace, store, _rpc):
        monkeypatch.setenv("CCX_CODEX_BIN", str(tmp_path / "fake-codex"))
        original_bind = generation.GenerationStore.bind_confirmed_native
        original_read = generation.GenerationStore.native_activation
        failed = [False]

        def fail_bind(self, target):
            if (target.startswith("codex:")
                    and original_read(self, target)["state"] == "confirmed"):
                failed[0] = True
                raise store_root.StaleGeneration("original bind was stale")
            return original_bind(self, target)

        def fail_secondary_read(self, target):
            if failed[0]:
                raise store_root.StorageIntegrityError("independent proof read also failed")
            return original_read(self, target)

        with monkeypatch.context() as injected:
            injected.setattr(generation.GenerationStore, "bind_confirmed_native", fail_bind)
            injected.setattr(generation.GenerationStore, "native_activation", fail_secondary_read)
            with pytest.raises(CreatePartialFailure) as caught:
                CodexClient().create(cwd=str(tmp_path), start_timeout=5)
            outcome = caught.value
            worker_id = outcome.worker_id
            assert isinstance(outcome.cause, store_root.StaleGeneration)
            assert outcome.info["cause_type"] == "StaleGeneration"
            assert "original bind was stale" in outcome.info["native_start_error"]
            assert "independent proof read also failed" not in outcome.info["native_start_error"]
        assert uuid.UUID(worker_id).version == 4
        assert store.native_activation(worker_id)["state"] == "confirmed"
        result = _f2_new(cli, tmp_path, worker_id, "--start-timeout", "5")
        assert result.returncode == 0, (result.stdout, result.stderr)
        assert json.loads(result.stdout)["worker_id"] == worker_id
        assert trace.read_text().count("RPC thread/start") == 1


@pytest.mark.parametrize("first", ["cli", "python", "worker"])
def test_initial_prompt_codex_real_cli_python_same_id_one_native_turn(
    short_root, tmp_path, monkeypatch, first,
):
    """One full-UUID worker and one first-turn receipt across public surfaces."""
    from ccx.codex.delivery import read_receipt
    prompt = "codex initial single native turn"
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch,
        persisted_threads=tmp_path / "initial-native-thread-history.json",
    ) as (cli, _restart, trace, store, _rpc):
        def run(surface):
            if surface == "cli":
                p = cli("new", "--engine", "codex", prompt, "--cwd", str(tmp_path),
                        "--worker-id", B, "--json")
                assert p.returncode == 0, (p.stdout, p.stderr)
                value = json.loads(p.stdout)
                assert value["delivery_id"] == "create:initial"
                assert value["delivery_status"] == "accepted"
                assert value["delivery"]["turn_id"]
                return value
            if surface == "python":
                value = _parity_process(short_root, tmp_path, python=(
                    "import json\nfrom ccx import CodexClient\n"
                    f"x=CodexClient().create({prompt!r},cwd={str(tmp_path)!r},worker_id={B!r})\n"
                    "print(json.dumps(x.as_dict()))"
                ))
                assert value["thread_id"]
                return value
            value = _parity_process(short_root, tmp_path, python=(
                "import json\nfrom ccx import WorkerClient\n"
                f"x=WorkerClient().create(engine='codex',cwd={str(tmp_path)!r},"
                f"worker_id={B!r},prompt={prompt!r})\n"
                "print(json.dumps({'worker_id':x['worker_id'],"
                "'delivery_id':x['delivery_id'], 'delivery_status':x['delivery_status']}))"
            ))
            assert value["delivery_id"] == "create:initial"
            assert value["delivery_status"] == "accepted"
            return value
        for surface in (first, "cli", "python", "worker"):
            assert run(surface)["worker_id"] == B
        receipt = read_receipt(f"codex:{B}", "create:initial")
        assert receipt["state"] == "accepted"
        assert receipt["promptByteCount"] == len(prompt.encode())
        assert receipt["turnId"]
        wire = _m1_wire(trace, "turn/start")
        assert len(wire) == 1
        assert wire[0]["clientUserMessageId"] == receipt["clientUserMessageId"]
        assert wire[0]["input"][0]["text"] == prompt
        assert store.native_activation(B)["state"] == "confirmed"
        assert trace.read_text().count("RPC thread/start") == 1
        assert trace.read_text().count("RPC turn/start") == 1
        conflict = cli("new", "--engine", "codex", "different first turn", "--cwd",
                       str(tmp_path), "--worker-id", B, "--json")
        assert conflict.returncode == 5, (conflict.stdout, conflict.stderr)
        refused = json.loads(conflict.stdout)
        assert refused["code"] == "conflict" and refused["phase"] == "initial-delivery"
        assert refused["native_ready"] is True and refused["worker_id"] == B
        assert refused["delivery_id"] == "create:initial"
        assert trace.read_text().count("RPC turn/start") == 1


def test_initial_prompt_codex_real_lost_send_reply_reconciles_same_uuid(
    short_root, tmp_path, monkeypatch,
):
    """Caller dies AFTER receipt; retry never emits a second native turn/start."""
    from ccx.codex.delivery import read_receipt
    prompt = "codex original lost response"
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch,
        persisted_threads=tmp_path / "initial-lost-ack-history.json",
    ) as (cli, _restart, trace, store, _rpc):
        hook = '''
import sys
import ccx.ops.send as protocol_ops
original_send = protocol_ops.send_turn
def lose_reply(*args, **kwargs):
    original_send(*args, **kwargs)
    raise OSError('simulated response lost after original native send receipt')
protocol_ops.send_turn = lose_reply
'''
        crashed = _m1_start_cli(
            short_root, tmp_path, hook, "new", "--engine", "codex", prompt,
            "--cwd", str(tmp_path), "--worker-id", B, "--json",
        )
        out, err = crashed.communicate(timeout=18)
        assert crashed.returncode == 3, (out, err)
        result = json.loads(out)
        assert result["worker_id"] == B and result["phase"] == "initial-delivery"
        assert result["native_ready"] is True
        assert result["delivery_id"] == "create:initial"
        original = read_receipt(f"codex:{B}", "create:initial")
        assert original["state"] == "accepted", original
        retried = cli("new", "--engine", "codex", prompt, "--cwd", str(tmp_path),
                      "--worker-id", B, "--json")
        assert retried.returncode == 0, (retried.stdout, retried.stderr)
        recovered = json.loads(retried.stdout)
        assert recovered["delivery_status"] == "accepted"
        assert recovered["delivery"]["turn_id"] == original["turnId"]
        assert trace.read_text().count("RPC thread/start") == 1
        assert trace.read_text().count("RPC turn/start") == 1


@pytest.mark.parametrize("source", ["file", "stdin"])
def test_initial_prompt_codex_real_source_file_or_stdin_once(
    short_root, tmp_path, monkeypatch, source,
):
    prompt = f"Codex first input from {source}\nwith a second line."
    with _f2_fake_processes(short_root, tmp_path, monkeypatch) as (
        cli, _restart, trace, store, _rpc,
    ):
        if source == "file":
            path = tmp_path / "first-task.md"
            path.write_text(prompt)
            proc = cli("new", "--engine", "codex", "--cwd", str(tmp_path),
                       "--worker-id", B, "--file", str(path), "--json")
        else:
            repo = Path(__file__).resolve().parent.parent
            env = {**os.environ, "CCX_ROOT": str(short_root),
                   "CCX_CODEX_BIN": str(tmp_path / "fake-codex"),
                   "CCX_CODEXD_LINGER": "120", "PYTHONPATH": str(repo)}
            for key in ("CCX_SELF_SHORT", "CCX_SELF_WORKER_ID", "CCX_SELF_ENGINE"):
                env.pop(key, None)
            proc = subprocess.run(
                [sys.executable, "-B", "-m", "ccx", "new", "--engine", "codex",
                 "--cwd", str(tmp_path), "--worker-id", B, "--stdin", "--json"],
                input=prompt, env=env, cwd=repo, capture_output=True,
                text=True, timeout=18,
            )
        assert proc.returncode == 0, (proc.stdout, proc.stderr)
        data = json.loads(proc.stdout)
        assert data["delivery_id"] == "create:initial"
        assert data["delivery_status"] == "accepted"
        assert trace.read_text().count("RPC thread/start") == 1
        assert trace.read_text().count("RPC turn/start") == 1
        if source == "file":
            path.write_text("edited original first prompt")
            conflict = cli("new", "--engine", "codex", "--cwd", str(tmp_path),
                           "--worker-id", B, "--file", str(path), "--json")
            assert conflict.returncode == 5
            assert json.loads(conflict.stdout)["code"] == "conflict"
            assert trace.read_text().count("RPC turn/start") == 1


def test_initial_prompt_codex_v1_stays_explicit_two_call_create_then_send(
    short_root, tmp_path, monkeypatch,
):
    """No prompt parameter on worker.create; the next V1 call owns create:initial."""
    with _f2_fake_processes(short_root, tmp_path, monkeypatch) as (
        _cli, _restart, trace, store, _rpc,
    ):
        def request(operation, params, rid):
            return _parity_process(short_root, tmp_path, request={
                "protocol_version": 2, "request_id": rid, "op": operation,
                "params": params, "timeout_ms": 7000,
            })
        identity = {"engine": "codex", "cwd": str(tmp_path), "worker_id": B}
        refused = request("worker.create", {**identity, "prompt": "forbidden"},
                          "before-any-claim")
        assert not refused["ok"] and refused["error"]["code"] == "invalid_request"
        assert not (store.pointer.generation_root / "owners" / f"{B}.json").exists()
        created = request("worker.create", identity, "create-only")
        assert created["ok"], created
        public = created["result"]
        assert public["worker"] == f"codex:{B}"
        assert public["created"] is True and public["reconciled"] is False
        assert public["worker_view"]["native_id"] == store.record(B)["native"]["threadId"]
        assert public["worker_view"]["state_source"] == "recorded"
        assert {"record", "native_ready", "activation_state", "native_source"}.isdisjoint(public)
        inspected = request("worker.inspect", {"target": B}, "inspect-created")
        assert inspected["ok"], inspected
        assert public["worker_view"] == inspected["result"]["worker_view"]
        assert trace.read_text().count("RPC thread/start") == 1
        assert trace.read_text().count("RPC turn/start") == 0
        submitted = request("turn.send", {
            "target": f"codex:{B}", "text": "explicit V1 first prompt",
            "delivery_id": "create:initial", "mode": "new_turn_only",
        }, "explicit-first-send")
        assert submitted["ok"] and submitted["result"]["delivery"]["status"] == "accepted", submitted
        assert submitted["result"]["delivery"]["delivery_id"] == "create:initial"
        again = request("turn.send", {
            "target": f"codex:{B}", "text": "explicit V1 first prompt",
            "delivery_id": "create:initial", "mode": "new_turn_only",
        }, "explicit-first-retry")
        assert again["ok"] and again["result"]["delivery"] == submitted["result"]["delivery"]
        reconciled = request("turn.reconcile", {
            "target": f"codex:{B}", "delivery_id": "create:initial",
        }, "exact-original-reconcile")
        assert reconciled["ok"], reconciled
        assert reconciled["result"]["delivery"]["delivery_id"] == "create:initial"
        assert trace.read_text().count("RPC turn/start") == 1
        assert trace.read_text().count("RPC thread/start") == 1


def test_initial_prompt_codex_minted_uuid_survives_first_send_refusal_and_retry(
    short_root, tmp_path, monkeypatch,
):
    """Prompted create can mint an ID, lose send, then retry the same worker."""
    import uuid
    from ccx import CodexClient
    from ccx.ops import common as protocol_ops
    from ccx.errors import CreatePartialFailure
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch,
        persisted_threads=tmp_path / "initial-minted-uuid.json",
    ) as (cli, _restart, trace, store, _rpc):
        monkeypatch.setenv("CCX_CODEX_BIN", str(tmp_path / "fake-codex"))
        original_send = ops_send.send_turn
        seen = []

        def fail_before_first_input(target, text, **kw):
            seen.append((target, text, kw))
            raise OSError("fake first-send transport failed before receipt")

        with monkeypatch.context() as patch:
            patch.setattr(ops_send, "send_turn", fail_before_first_input)
            with pytest.raises(CreatePartialFailure) as incomplete:
                CodexClient().create("first original prompt", cwd=str(tmp_path),
                                     start_timeout=5)
        original = incomplete.value
        minted = original.worker_id
        assert uuid.UUID(minted).version == 4
        assert original.phase == "initial-delivery"
        assert original.delivery_id == "create:initial"
        assert original.info["native_ready"] is True
        assert isinstance(original.cause, OSError)
        assert seen == [(f"codex:{minted}", "first original prompt", {
            "delivery_id": "create:initial", "mode": "new_turn_only",
            "expected_generation": store.generation,
            "notify_parent": False, "settle": False, "cap": None,
        })]
        assert trace.read_text().count("RPC thread/start") == 1
        assert trace.read_text().count("RPC turn/start") == 0
        again = cli("new", "--engine", "codex", "first original prompt", "--cwd",
                    str(tmp_path), "--worker-id", minted, "--json")
        assert again.returncode == 0, (again.stdout, again.stderr)
        value = json.loads(again.stdout)
        assert value["worker_id"] == minted
        assert value["delivery_id"] == "create:initial"
        assert value["delivery_status"] == "accepted"
        assert trace.read_text().count("RPC thread/start") == 1
        assert trace.read_text().count("RPC turn/start") == 1


def test_initial_prompt_codex_proven_rejection_never_returns_create_success(
    short_root, tmp_path, monkeypatch,
):
    """A durable rejected first receipt is not a successful prompted create."""
    from ccx import CodexClient
    from ccx.ops import common as protocol_ops
    from ccx.errors import CreatePartialFailure, DeliveryRejected
    with _f2_fake_processes(short_root, tmp_path, monkeypatch) as (
        _cli, _restart, trace, store, _rpc,
    ):
        monkeypatch.setenv("CCX_CODEX_BIN", str(tmp_path / "fake-codex"))
        with monkeypatch.context() as patch:
            patch.setattr(ops_send, "send_turn", lambda *args, **kwargs: {
                "status": "rejected", "delivery": {
                    "delivery_id": "create:initial", "status": "rejected",
                    "rejection_code": "fake-first-refusal",
                    "rejection_message": "initial native input was refused",
                },
            })
            with pytest.raises(CreatePartialFailure) as caught:
                CodexClient().create("refused original prompt", cwd=str(tmp_path),
                                     worker_id=B, start_timeout=5)
        failure = caught.value
        assert failure.phase == "initial-delivery"
        assert failure.worker_id == B and failure.delivery_id == "create:initial"
        assert failure.delivery_status == "rejected"
        assert failure.info["native_ready"] is True
        assert failure.info["initial_delivery_rejected"] is True
        assert isinstance(failure.cause, DeliveryRejected)
        assert trace.read_text().count("RPC thread/start") == 1
        assert trace.read_text().count("RPC turn/start") == 0


@pytest.mark.parametrize("first", ["cli", "typed", "worker"])
def test_initial_wait_codex_physical_first_exact_reply_after_later_turn(
    short_root, tmp_path, monkeypatch, first,
):
    """Original create:initial report survives later native turns, no duplicate."""
    from ccx import CodexClient, WorkerClient
    from ccx.codex.delivery import read_receipt
    first_prompt = 'Codex first exact waited reply'
    with _f2_fake_processes(short_root, tmp_path, monkeypatch) as (
        cli, _restart, trace, store, _rpc,
    ):
        monkeypatch.setenv('CCX_CODEX_BIN', str(tmp_path/'fake-codex'))

        freshness_checked = False

        def waited(surface):
            nonlocal freshness_checked
            if surface == 'cli':
                proc = cli('new', '--engine', 'codex',first_prompt,'--cwd',str(tmp_path),
                           '--worker-id',B,'--wait','--timeout','5','--json')
                assert proc.returncode == 0,(proc.stdout,proc.stderr)
                data=json.loads(proc.stdout)
                assert data['waited'] is True and data['report']['text'] == (
                    'Fake Codex completed: ' + first_prompt)
                plain = cli('new', '--engine', 'codex', first_prompt, '--cwd', str(tmp_path),
                            '--worker-id', B, '--wait', '--timeout', '5')
                assert plain.returncode == 0, (plain.stdout, plain.stderr)
                assert plain.stdout.splitlines() == [
                    f'worker: codex:{B}', 'Fake Codex completed: ' + first_prompt,
                ]
                return data['report']
            if surface == 'typed':
                value=CodexClient().create(first_prompt,cwd=str(tmp_path),
                                            worker_id=B,wait=True,timeout=5)
                assert value.worker_id == B and value.status == 'completed'
                assert value.delivery_id == 'create:initial'
                assert value.reply == 'Fake Codex completed: ' + first_prompt
                return {'turn_id':value.turn_id, 'text':value.reply}
            if first == 'worker' and not freshness_checked:
                # The real first-turn wait completes, then a later turn becomes
                # busy before WorkerClient projects the recorded child state.
                from ccx.ops import common as protocol_ops
                real_wait = ops_wait.wait_delivery
                previous_state = []

                def later_turn_started(*args, **kwargs):
                    observed = real_wait(*args, **kwargs)
                    before = store.record(B)['lifecycle']['state']
                    store.mutate_record(B, patches={"lifecycle.state": "busy"})
                    previous_state.append(before)
                    return observed

                with monkeypatch.context() as after_wait:
                    after_wait.setattr(ops_wait, 'wait_delivery', later_turn_started)
                    try:
                        value=WorkerClient().create(
                            engine='codex',cwd=str(tmp_path),worker_id=B,
                            prompt=first_prompt,wait=True,timeout=5,
                        )
                        assert previous_state
                        assert (value['native_state'], value['state']) == ('busy', 'busy')
                    finally:
                        if previous_state:
                            store.mutate_record(
                                B, patches={"lifecycle.state": previous_state[0]},
                            )
                freshness_checked = True
            else:
                value=WorkerClient().create(
                    engine='codex',cwd=str(tmp_path),worker_id=B,
                    prompt=first_prompt,wait=True,timeout=5,
                )
            assert set(value) == {
                "worker", "worker_id", "engine", "short", "state",
                "native_state", "report", "reply", "created", "reconciled",
                "delivery_id", "delivery_status", "notification",
            }
            assert value['worker'] == f'codex:{B}' and value['worker_id'] == B
            assert value['delivery_id'] == 'create:initial'
            assert value['delivery_status'] == 'settled'
            assert value['state'] == normalize_native_state(value['native_state'])
            assert value['notification'] is None
            assert value['report']['text'] == 'Fake Codex completed: '+first_prompt
            assert value['reply'] == value['report']['text']
            return value['report']

        original = waited(first)
        delivery=read_receipt(f'codex:{B}','create:initial')
        assert delivery['state'] == 'accepted'
        assert delivery['settlement']['status'] == 'completed'
        assert original['turn_id'] == delivery['turnId']
        # A different subsequent user turn may overwrite the latest record;
        # a retry must still return the first receipt's actual answer.
        second=CodexClient().send(B,'unrelated second Codex turn',wait=False,
                                  delivery_id='other-turn-after-initial')
        assert second.delivery_id == 'other-turn-after-initial'
        for surface in ('cli','typed','worker'):
            value=waited(surface)
            assert value['turn_id'] == delivery['turnId']
            assert value['text'] == 'Fake Codex completed: '+first_prompt
        assert trace.read_text().count('RPC thread/start') == 1
        assert trace.read_text().count('RPC turn/start') == 2


def test_initial_wait_codex_timeout_then_exact_retry_without_second_turn(
    short_root, tmp_path, monkeypatch,
):
    """Native read outage does not un-fence the exact first native turn."""
    from ccx import CodexClient
    from ccx.errors import CreatePartialFailure, TurnIncomplete
    read_mode=tmp_path/'initial-wait-read-mode'
    read_mode.write_text('normal')
    with _f2_fake_processes(short_root,tmp_path,monkeypatch,read_mode=read_mode,
                             omit_completion=True) as (
        cli,_restart,trace,store,_rpc,
    ):
        monkeypatch.setenv('CCX_CODEX_BIN',str(tmp_path/'fake-codex'))
        # Allow original thread/read to prove native creation first. Simulate
        # a missed completion notification, then make its recovery read fail
        # only AFTER the initial input has been accepted.
        first = CodexClient().create('timeout first Codex turn',
                                     cwd=str(tmp_path), worker_id=B, wait=False)
        assert first.worker_id == B
        read_mode.write_text('fail')
        with pytest.raises(CreatePartialFailure) as caught:
            CodexClient().create('timeout first Codex turn',cwd=str(tmp_path),
                                 worker_id=B,wait=True,timeout=.2)
        failure=caught.value
        assert failure.phase == 'wait-report' and failure.worker_id == B
        assert failure.delivery_id == 'create:initial'
        assert failure.info['native_ready'] is True
        assert isinstance(failure.cause,TurnIncomplete)
        assert trace.read_text().count('RPC thread/start') == 1
        assert trace.read_text().count('RPC turn/start') == 1
        read_mode.write_text('normal')
        retry=cli('new', '--engine', 'codex','timeout first Codex turn','--cwd',str(tmp_path),
                  '--worker-id',B,'--wait','--timeout','5','--json')
        assert retry.returncode == 0,(retry.stdout,retry.stderr)
        info=json.loads(retry.stdout)
        assert info['report']['text'] == 'Fake Codex completed: timeout first Codex turn'
        assert info['delivery_id'] == 'create:initial'
        assert trace.read_text().count('RPC thread/start') == 1
        assert trace.read_text().count('RPC turn/start') == 1


@pytest.mark.parametrize('suffix,expected',[
    ('failed','failed'),('interrupted','interrupted'),('empty','completed'),
])
def test_initial_wait_codex_terminal_failures_and_empty_answer(
    short_root,tmp_path,monkeypatch,suffix,expected,
):
    """Exact failed native turn is terminal; completed no-agent turn is empty."""
    from ccx.codex.delivery import read_receipt
    text='initial-wait-'+suffix+' first message'
    with _f2_fake_processes(short_root,tmp_path,monkeypatch) as (
        cli,_restart,trace,store,_rpc,
    ):
        proc=cli('new', '--engine', 'codex',text,'--cwd',str(tmp_path),'--worker-id',B,
                 '--wait','--timeout','5','--json')
        info=json.loads(proc.stdout)
        receipt=read_receipt(f'codex:{B}','create:initial')
        assert info['worker_id'] == B
        if expected == 'completed':
            assert proc.returncode == 0,(proc.stdout,proc.stderr)
            assert info['report']['text'] == ''
            assert receipt['settlement']['text'] == ''
        else:
            assert proc.returncode == 6,(proc.stdout,proc.stderr)
            assert info['phase'] == 'wait-report'
            assert info['native_ready'] is True
            assert info['delivery_id'] == 'create:initial'
            assert info['turn_status'] == expected
            assert info['turn_error']['message'] == 'fake '+expected+' original turn'
            assert receipt.get('settlement') is None
        assert trace.read_text().count('RPC turn/start') == 1
        assert trace.read_text().count('RPC thread/start') == 1


def test_generated_native_codex_full_uuid_approval_gate_id_python_cli_and_sibling(
    short_root, tmp_path, monkeypatch,
):
    """Native serverRequest/response through actual daemon and public adapters."""
    from ccx.codex.runtime import daemon_rpc
    from ccx.codex.client import CodexClient
    from ccx.workers import WorkerClient
    from ccx.errors import DeliveryRejected, CapabilityUnsupported, SessionUnavailable

    root = short_root
    store = require_ccx_active(root)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    trace = tmp_path / "codex-native-approval-trace"
    fake = _fake_codex_executable(tmp_path, trace, approval_mode=True)
    repo = Path(__file__).resolve().parent.parent
    env = {**os.environ, "CCX_ROOT": str(root), "CCX_CODEX_BIN": str(fake),
           "PYTHONPATH": str(repo), "CCX_CODEXD_LINGER": "120"}
    for name in ("CCX_SELF_WORKER_ID", "CCX_SELF_ENGINE", "CCX_SELF_SHORT"):
        env.pop(name, None)

    def cli(*args):
        return subprocess.run([sys.executable, "-B", "-m", "ccx", *args],
                              env=env, cwd=repo, capture_output=True,
                              text=True, timeout=15)

    def pending(wait_old=None):
        until = time.monotonic() + 12
        while time.monotonic() < until:
            rec = store.record(B)
            gates = rec.get("pendingApprovals") or []
            if (gates and isinstance(gates[0].get("gateId"), str)
                    and gates[0]["gateId"] != wait_old):
                return rec, gates[0]
            time.sleep(.05)
        pytest.fail("genuine native approval not published")

    sibling = "aaaaaaaa-9876-4000-8000-000000000099"
    log = tmp_path / "codex-approval-daemon.log"
    with log.open("wb") as out:
        process = subprocess.Popen([sys.executable, "-B", "-m", 'ccx.codex.daemon'],
                                   env=env, cwd=repo, stdout=out, stderr=out,
                                   start_new_session=True)
        try:
            until = time.monotonic() + 10
            while time.monotonic() < until:
                daemon = daemon_rpc({"op": "daemon-status"}, timeout=.5)
                if daemon.get("ok") and daemon.get("state") == "ready":
                    break
                time.sleep(.05)
            assert daemon["state"] == "ready", log.read_text(errors="replace")[-3500:]
            for wid in (B, sibling):
                made = cli("new", "--engine", "codex", "--cwd", str(tmp_path),
                           "--worker-id", wid, "--json")
                assert made.returncode == 0, (made.stdout, made.stderr)
            start = cli("send", B, "codex-approve-first", "--no-wait",
                        "--delivery-id", "codex-native-gate-1", "--json")
            assert start.returncode == 0, (start.stdout, start.stderr)
            rec, first = pending()
            gate = first["gateId"]
            assert first["runtimeId"] == daemon["runtimeId"]
            assert first["turnId"] == rec["lifecycle"]["activeTurnId"]
            assert first["requestId"] == "reused-native-request"
            worker = WorkerClient()
            assert worker.capabilities("codex")["answer_question"] is False
            projection = worker.status(B)
            assert projection["host"] == "unobserved" and projection["loaded"] is None
            assert projection["pending_input"]["request_id"] == gate
            assert projection["pending_input"]["native"]["pendingApprovals"][0]["gateId"] == gate
            observed = cli("peek", f"codex:{B}", "--status", "--json")
            assert observed.returncode == 0, (observed.stdout, observed.stderr)
            codex_status = json.loads(observed.stdout)["statuses"][0]
            assert codex_status["pending_input"]["request_id"] == gate
            assert codex_status["pending_approvals"][0]["gateId"] == gate
            for bad in ("reused-native-request", "stale-original-gate"):
                with pytest.raises(DeliveryRejected):
                    CodexClient().approve(B, request_id=bad)
            with pytest.raises(CapabilityUnsupported):
                worker.reject(B, request_id=gate, reason="Codex has no rationale")
            with pytest.raises((DeliveryRejected, SessionUnavailable)):
                worker.approve(sibling, request_id=gate)
            assert store.record(B)["pendingApprovals"][0]["gateId"] == gate
            granted = worker.approve(B, request_id=gate)
            assert granted["request_id"] == gate and granted["decision"] == "allow"
            until = time.monotonic() + 8
            while time.monotonic() < until:
                if "'id': 'reused-native-request'" in trace.read_text() and "'decision': 'accept'" in trace.read_text():
                    break
                time.sleep(.05)
            assert "'decision': 'accept'" in trace.read_text(), trace.read_text()[-2500:]
            until = time.monotonic() + 10
            while time.monotonic() < until:
                if store.record(B)["lifecycle"]["state"] == "idle":
                    break
                time.sleep(.05)
            assert store.record(B)["lifecycle"]["state"] == "idle", store.record(B)
            second = cli("send", B, "codex-approve-second", "--no-wait",
                         "--delivery-id", "codex-native-gate-2", "--json")
            assert second.returncode == 0, (second.stdout, second.stderr)
            _, next_gate = pending(wait_old=gate)
            assert next_gate["requestId"] == first["requestId"]
            assert next_gate["gateId"] != gate
            with pytest.raises(DeliveryRejected):
                CodexClient().reject(B, request_id=gate)
            assert store.record(B)["pendingApprovals"][0]["gateId"] == next_gate["gateId"]
            declined = cli("reject", B, "--request-id", next_gate["gateId"], "--json")
            assert declined.returncode == 0, (declined.stdout, declined.stderr)
            result = json.loads(declined.stdout)
            assert result["request_id"] == next_gate["gateId"]
            assert result["decision"] == "decline" and result["decision_sent"] is True
            until = time.monotonic() + 8
            while time.monotonic() < until:
                if "'decision': 'decline'" in trace.read_text():
                    break
                time.sleep(.05)
            assert "'decision': 'decline'" in trace.read_text()
            assert store.record(sibling)["pendingApprovals"] == []
            assert daemon_rpc({"op": "daemon-status"}, timeout=1)["pid"] == process.pid
            assert not (root / "codex").exists()
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=4)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=4)


def test_generated_codex_gate_dies_with_abrupt_daemon_even_when_native_id_recycles(
    short_root, tmp_path, monkeypatch,
):
    """A SIGKILL leaves the old gate on disk; a new daemon cannot answer it."""
    from ccx.codex.runtime import daemon_rpc, rpc
    from ccx.codex.client import CodexClient
    from ccx.errors import DeliveryRejected, SessionUnavailable

    root = short_root
    store = require_ccx_active(root)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    trace = tmp_path / "abrupt-approval-trace"
    fake = _fake_codex_executable(
        tmp_path, trace, persisted_threads=tmp_path / "native-threads.json",
        approval_mode=True,
    )
    before = "        elif method=='thread/unsubscribe':\n"
    fake.write_text(fake.read_text().replace(before, (
        "        elif method=='thread/resume':\n"
        "            tid=params.get('threadId')\n"
        "            result={'thread':dict(threads[tid],status={'type':'idle'}),"
        "'model':threads[tid].get('model')}\n" + before), 1))
    repo = Path(__file__).resolve().parent.parent
    env = {**os.environ, "CCX_ROOT": str(root), "CCX_CODEX_BIN": str(fake),
           "PYTHONPATH": str(repo), "CCX_CODEXD_LINGER": "120"}

    def cli(*args):
        return subprocess.run([sys.executable, "-B", "-m", "ccx", *args],
                              cwd=repo, env=env, capture_output=True, text=True,
                              timeout=15)

    def daemon_process(log_path):
        log = log_path.open("wb")
        process = subprocess.Popen([sys.executable, "-B", "-m", 'ccx.codex.daemon'],
                                   env=env, cwd=repo, stdout=log, stderr=log,
                                   start_new_session=True)
        log.close()
        until = time.monotonic() + 10
        while time.monotonic() < until:
            observed = daemon_rpc({"op": "daemon-status"}, timeout=.5)
            if (observed.get("ok") and observed.get("pid") == process.pid
                    and observed.get("state") == "ready"):
                return process, observed
            time.sleep(.05)
        pytest.fail(f"fake daemon startup failed: {observed}")

    process = None
    try:
        process, first = daemon_process(tmp_path / "abrupt-daemon-1.log")
        made = cli("new", "--engine", "codex", "--cwd", str(tmp_path), "--worker-id", B, "--json")
        assert made.returncode == 0, (made.stdout, made.stderr)
        sent = cli("send", B, "codex-approve-before-abrupt-death",
                   "--no-wait", "--delivery-id", "abrupt-native-approval", "--json")
        assert sent.returncode == 0, (sent.stdout, sent.stderr)
        until = time.monotonic() + 10
        old = None
        while time.monotonic() < until:
            approvals = store.record(B).get("pendingApprovals") or []
            if approvals:
                old = approvals[0]
                break
            time.sleep(.04)
        assert old and old["runtimeId"] == first["runtimeId"]
        assert old["requestId"] == "reused-native-request"
        # Abrupt termination: no clean daemon-stop and no native gate cleanup.
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=5)
        process = None
        assert store.record(B)["pendingApprovals"][0]["gateId"] == old["gateId"]
        process, second = daemon_process(tmp_path / "abrupt-daemon-2.log")
        assert second["runtimeId"] != first["runtimeId"]
        # The new daemon ends the dead daemon's open turn and drops its gate.
        repaired = store.record(B)
        assert repaired["lifecycle"]["state"] == "crashed"
        assert repaired["pendingApprovals"] == []
        # No new thread/start, turn/start, or load through the approval path;
        # an explicit load resumes the thread on the new app-server channel.
        adopted = rpc(B, {"op": "load"}, timeout=5)
        assert adopted.get("ok"), adopted
        with pytest.raises((DeliveryRejected, SessionUnavailable)):
            CodexClient().approve(B, request_id=old["gateId"])
        assert not any("'decision': 'accept'" in line or "'decision': 'decline'" in line
                       for line in trace.read_text().splitlines()
                       if line.startswith("APPROVAL_REPLY"))
        assert store.record(B).get("pendingApprovals") is not None
    finally:
        if process is not None and process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=5)
