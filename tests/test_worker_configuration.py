"""Public Codex next-turn config against real disposable app-server/daemon.

The only native effects are physical fake Codex JSON-RPC requests.  No real
provider, external thread adoption, or legacy short-keyed state is used.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from ccx import CcxClient, CodexClient, WorkerClient, constants
from ccx.errors import DeliveryRejected, InvalidRequest, SessionUnavailable
from ccx.store.root import require_ccx_active
from test_generation_store import A, B, short_root
from test_codex_native_effects import _f2_fake_processes, _f2_new
from test_codex_creation_surfaces import _m1_wire


@pytest.fixture(autouse=True)
def _fake_codex_model_catalog(monkeypatch):
    """These configuration tests use fake IDs, never the user's provider."""
    from ccx.codex import runtime as codex_runtime
    monkeypatch.setattr(codex_runtime, "list_models", lambda **_: [
        "gpt-config-new", "gpt-config-cli", "gpt-no-busy", "gpt-offline",
        "gpt-one", "gpt-forbidden-waiting",
    ])


def _eventually(read, predicate, *, seconds=9):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        last = read()
        if predicate(last):
            return last
        time.sleep(.04)
    raise AssertionError(f"native evidence not received: {last!r}")


def test_loaded_idle_public_model_effort_and_next_real_turn_wire(
    short_root, tmp_path, monkeypatch,
):
    from ccx.ops.creation_steps import prepare_worker
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch,
        persisted_threads=tmp_path / "config-fake-native-threads.json",
    ) as (cli, _restart, trace, store, _rpc):
        # Two canonical workers collide on their first eight hex digits.
        prepare_worker(store, engine="codex", cwd=str(tmp_path), worker_id=A)
        before_sibling = store.record(A)
        created = _f2_new(cli, tmp_path, B, "--start-timeout", "5")
        assert created.returncode == 0, (created.stdout, created.stderr)
        native_id = store.record(B)["native"]["threadId"]
        original_sha = store.record(B)["createSha256"]

        first = cli("send", B, "original model one",
                    "--delivery-id", "first-model-config", "--no-wait", "--json")
        assert first.returncode == 0, (first.stdout, first.stderr)
        _eventually(lambda: store.record(B)["lifecycle"]["state"],
                    lambda state: state == "idle")
        assert len(_m1_wire(trace, "thread/start")) == 1
        assert len(_m1_wire(trace, "turn/start")) == 1

        # Setters cause no native turn/thread; data is requested future config.
        model = CodexClient().set_model("codex:"+B, "  gpt-config-new  ")
        assert model.worker_id == B and model.model == "gpt-config-new"
        assert model.thread_id == native_id
        effort = WorkerClient().set_effort("codex:"+B, "medium")
        assert effort["worker"] == "codex:"+B and effort["worker_id"] == B
        assert effort["effort"] == "medium" and effort["restarted"] is False

        # Existing CLI parser and central prefix discovery: qualified full
        # handle, combined one config mutation then rename.
        cli_set = cli("set", "codex:"+B, "--model", "gpt-config-cli",
                      "--effort", "high", "--name", "codex config owner", "--json")
        assert cli_set.returncode == 0, (cli_set.stdout, cli_set.stderr)
        projected = json.loads(cli_set.stdout)
        assert projected["worker"] == "codex:"+B
        assert projected["worker_id"] == B and projected["engine"] == "codex"
        assert projected["model"] == "gpt-config-cli"
        assert projected["effort"] == "high" and projected["name"] == "codex config owner"
        assert projected["action"] == "set+rename"
        assert len(_m1_wire(trace, "thread/start")) == 1
        assert len(_m1_wire(trace, "turn/start")) == 1
        assert store.record(A) == before_sibling

        second = cli("send", B, "native config second",
                     "--delivery-id", "second-model-config", "--no-wait", "--json")
        assert second.returncode == 0, (second.stdout, second.stderr)
        turns = _m1_wire(trace, "turn/start")
        assert len(turns) == 2
        assert turns[1]["model"] == "gpt-config-cli"
        assert turns[1]["effort"] == "high"
        assert turns[0]["threadId"] == turns[1]["threadId"] == native_id
        assert len(_m1_wire(trace, "thread/start")) == 1
        assert store.record(B)["createSha256"] == original_sha
        assert store.record(A) == before_sibling
        _eventually(lambda: store.record(B)["lifecycle"]["state"],
                    lambda state: state == "idle")
        default = cli("set", B, "--model", "default",
                      "--effort", "default", "--json")
        assert default.returncode == 0, (default.stdout, default.stderr)
        cleared = json.loads(default.stdout)
        assert cleared["model"] is None and cleared["effort"] is None
        assert store.record(B)["createArgs"]["model"] is None
        assert store.record(B)["createArgs"]["effort"] is None
        assert len(_m1_wire(trace, "thread/start")) == 1
        assert len(_m1_wire(trace, "turn/start")) == 2


def test_busy_loaded_real_native_turn_refuses_config_and_name_before_mutation(
    short_root, tmp_path, monkeypatch,
):
    completion_gate = tmp_path / "allow-fake-native-completion"
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch,
        completion_gate=completion_gate,
        persisted_threads=tmp_path / "config-busy-native-threads.json",
    ) as (cli, _restart, trace, store, _rpc):
        created = _f2_new(cli, tmp_path, B, "--start-timeout", "5")
        assert created.returncode == 0, (created.stdout, created.stderr)
        first = cli("send", B, "f1-race-hold-config",
                    "--delivery-id", "busy-config-original",
                    "--no-wait", "--json")
        assert first.returncode == 0, (first.stdout, first.stderr)
        _eventually(lambda: store.record(B)["lifecycle"]["state"],
                    lambda state: state == "busy")
        before = store.record(B)
        try:
            with pytest.raises(DeliveryRejected, match="wait until idle"):
                CodexClient().set_effort(B, "max")
            # The CLI config must refuse before applying a requested name.
            refused = cli("set", B, "--model", "gpt-no-busy",
                          "--name", "should not rename", "--json")
            assert refused.returncode == 5
            assert store.record(B)["createArgs"] == before["createArgs"]
            assert store.record(B)["name"] == before["name"]
            assert len(_m1_wire(trace, "turn/start")) == 1
        finally:
            completion_gate.touch()
        _eventually(lambda: store.record(B)["lifecycle"]["state"],
                    lambda state: state == "idle")
        assert store.record(B)["createArgs"] == before["createArgs"]


def test_original_unloaded_served_then_daemon_absent_config_never_loads(
    short_root, tmp_path, monkeypatch,
):
    # Physically real daemon is running but has no registered Worker for the
    # just-started original thread. It still MUST own all config writes.
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch,
        persisted_threads=tmp_path / "config-unloaded-threads.json",
    ) as (cli, _restart, trace, store, rpc):
        created = _f2_new(cli, tmp_path, B, "--start-timeout", "5")
        assert created.returncode == 0, (created.stdout, created.stderr)
        original_id = store.record(B)["native"]["threadId"]
        original_sha = store.record(B)["createSha256"]
        before_wire = trace.read_text()
        assert rpc({"op": "status", "short": B}, timeout=1).get("loaded") is False
        info = CodexClient().set_effort(B, "high")
        assert info.effort == "high" and info.thread_id == original_id
        assert rpc({"op": "status", "short": B}, timeout=1).get("loaded") is False
        assert trace.read_text() == before_wire
        assert store.record(B)["createSha256"] == original_sha

    # No serving daemon: spawn lock is held across absence re-probe and CAS.
    state = store.record(B)
    assert state["native"]["threadId"] == original_id
    cleared = WorkerClient().set_effort(B, None)
    assert cleared["effort"] is None and cleared["worker"] == f"codex:{B}"
    changed = CodexClient().set_model(B, "gpt-offline")
    assert changed.model == "gpt-offline"
    assert store.record(B)["native"]["threadId"] == original_id
    assert store.record(B)["createSha256"] == original_sha
    assert len(_m1_wire(trace, "thread/start")) == 1
    assert _m1_wire(trace, "turn/start") == []


@pytest.mark.parametrize("key,value", [
    ("model", ""), ("model", "   "), ("model", 27),
    ("effort", ""), ("effort", "extreme"), ("effort", True),
])
def test_public_invalid_config_refuses_before_any_owned_effect(
    short_root, tmp_path, monkeypatch, key, value,
):
    from ccx.ops.creation_steps import prepare_worker
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    store = require_ccx_active(short_root)
    prepare_worker(store, engine="codex", cwd=str(tmp_path), worker_id=B)
    before = store.record(B)
    with pytest.raises(InvalidRequest):
        getattr(CodexClient(), "set_"+key)(B, value)
    assert store.record(B) == before


def test_public_none_cli_default_and_claude_prepared_plain_no_native(
    short_root, tmp_path, monkeypatch,
):
    """Replace the old staging-only Claude refusal with safe original no-host CAS."""
    from ccx.ops.creation_steps import prepare_worker
    from ccx import CcxClient
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    store = require_ccx_active(short_root)
    prepare_worker(store, engine="codex", cwd=str(tmp_path), worker_id=B)
    prepare_worker(store, engine="claude", cwd=str(tmp_path), worker_id=A)
    original = store.record(A)
    assert CodexClient().set_model(B, "gpt-one").model == "gpt-one"
    assert CodexClient().set_model(B, None).model is None
    assert WorkerClient().set_effort(B, "low")["effort"] == "low"
    changed = CcxClient().set_model(A, "  sonnet  ")
    assert changed.worker_id == A and changed.model == "sonnet"
    effort = WorkerClient().set_effort(f"claude:{A}", "high")
    assert effort["worker"] == f"claude:{A}"
    assert effort["effort"] == "high" and effort["restarted"] is False
    assert store.record(A)["createSha256"] == original["createSha256"]
    assert store.record(A)["native"] == original["native"]
    assert store.record(B)["createArgs"]["effort"] == "low"
    cleared = CcxClient().set_model(A, None)
    assert cleared.model is None
    stopped_restart = CcxClient().set_model(A, "opus", restart=True)
    assert stopped_restart.model == "opus"
    assert store.record(A)["createArgs"]["model"] == "opus"



def test_waiting_native_approval_refuses_config_without_touching_record(
    short_root, tmp_path, monkeypatch,
):
    # A fake provider holds a real native input gate open; the daemon's
    # current Worker state, not the earlier disk snapshot, must control CAS.
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch,
        approval_mode=True,
        persisted_threads=tmp_path / "config-waiting-native-threads.json",
    ) as (cli, _restart, trace, store, _rpc):
        assert _f2_new(cli, tmp_path, B, "--start-timeout", "5").returncode == 0
        sent = cli("send", B, "codex-approve-config-hold",
                   "--delivery-id", "waiting-config-original",
                   "--no-wait", "--json")
        assert sent.returncode == 0, (sent.stdout, sent.stderr)
        _eventually(lambda: store.record(B)["lifecycle"]["state"],
                    lambda state: state == "waiting")
        before = store.record(B)
        with pytest.raises(DeliveryRejected, match="wait until idle"):
            WorkerClient().set_model(B, "gpt-forbidden-waiting")
        assert store.record(B)["createArgs"] == before["createArgs"]
        assert store.record(B)["name"] == before["name"]
        assert len(_m1_wire(trace, "thread/start")) == 1
        assert len(_m1_wire(trace, "turn/start")) == 1


def test_claude_real_idle_cli_combined_restart_same_session_and_native_argv(
    short_root, tmp_path, monkeypatch,
):
    from test_reactivation_claude import (
        _setup, _record_fake_native_argv, _cli, SECOND,
    )
    from test_create_cross_surface import _first_wait_original_claude_ready
    from test_claude_native_cli import _stop_disposable_original
    from ccx.store.liveness import owned_pid_alive
    from ccx.ops.creation_steps import prepare_worker

    launches, store = _setup(short_root, tmp_path, monkeypatch)
    argv_log = _record_fake_native_argv(tmp_path)
    original = f"claude:{A}"
    try:
        assert _first_wait_original_claude_ready(tmp_path, A).worker_id == A
        first = CcxClient().new(
            "M2 first actual original Claude turn", cwd=str(tmp_path),
            worker_id=A, wait=True, timeout=16, start_timeout=5,
        )
        assert first.settled, first
        before = store.record(A)
        assert before["lifecycle"]["state"] == "idle"
        sid = before["native"]["sessionId"]
        sha = before["createSha256"]
        first_transcript = Path(before["transcript"]).read_bytes()
        assert first_transcript
        with pytest.raises(DeliveryRejected, match="use --restart"):
            CcxClient().set_model(original, "wrong-plain-model")
        assert store.record(A)["createArgs"] == before["createArgs"]
        # An independent SAME 8-hex display prefix must not be changed.
        prepare_worker(store, engine="claude", cwd=str(tmp_path), worker_id=SECOND)
        neighbor = store.record(SECOND)
        initial_launches = launches.read_text().splitlines()
        assert len(initial_launches) == 1

        # Existing central prefix resolver canonicalizes the unique longer
        # textual prefix BEFORE the CLI handler; it never persists the alias.
        configured = _cli(
            "set", "aaaaaaaa-1234", "--model", "sonnet", "--effort", "high",
            "--restart", "--name", "configured-for-next-spawn", "--json",
        )
        assert configured.returncode == 0, (configured.stdout, configured.stderr)
        result = json.loads(configured.stdout)
        assert result["worker"] == original and result["worker_id"] == A
        assert result["model"] == "sonnet" and result["effort"] == "high"
        assert result["name"] == "configured-for-next-spawn"
        assert result["action"] == "restart"
        saved = store.record(A)
        assert saved["createArgs"]["model"] == "sonnet"
        assert saved["createArgs"]["effort"] == "high"
        assert saved["lifecycle"]["state"] == "dead"
        assert saved["createSha256"] == sha
        assert saved["native"]["sessionId"] == sid
        assert store.record(SECOND) == neighbor
        assert not owned_pid_alive(
            before["lifecycle"]["hostPid"], before["lifecycle"]["hostStart"],
        )
        assert not owned_pid_alive(
            before["lifecycle"]["workerPid"], before["lifecycle"]["workerStart"],
        )
        assert launches.read_text().splitlines() == initial_launches
        assert len(argv_log.read_text().splitlines()) == 1

        # No synthetic turn, new session ID or prompt on explicit resume.
        revived = WorkerClient().resume(original, timeout=18)
        assert revived["resumed"] and revived["session_id"] == sid
        now = store.record(A)
        assert now["native"]["sessionId"] == sid
        assert now["stats"]["turns"] == before["stats"]["turns"]
        assert Path(before["transcript"]).read_bytes() == first_transcript
        assert store.record(SECOND) == neighbor
        argv = argv_log.read_text().splitlines()
        assert len(argv) == 2, argv
        assert "--resume " + sid in argv[1] and "--session-id" not in argv[1]
        assert "--model sonnet" in argv[1] and "--effort high" in argv[1]
        assert len(launches.read_text().splitlines()) == 2
    finally:
        _stop_disposable_original(short_root)


def test_claude_worker_auto_idle_restart_true_then_same_original_resume(
    short_root, tmp_path, monkeypatch,
):
    from test_reactivation_claude import _setup, _record_fake_native_argv
    from test_create_cross_surface import _first_wait_original_claude_ready
    from test_claude_native_cli import _stop_disposable_original

    launches, store = _setup(short_root, tmp_path, monkeypatch)
    argv_log = _record_fake_native_argv(tmp_path)
    handle = f"claude:{A}"
    try:
        assert _first_wait_original_claude_ready(tmp_path, A).worker_id == A
        assert CcxClient().new(
            "M2 Worker original history", cwd=str(tmp_path), worker_id=A,
            wait=True, timeout=16, start_timeout=5,
        ).settled
        original = store.record(A)
        sid = original["native"]["sessionId"]
        result = WorkerClient().set_effort(handle, "xhigh", timeout=15)
        assert result["worker"] == handle and result["worker_id"] == A
        assert result["effort"] == "xhigh" and result["restarted"] is True
        assert store.record(A)["lifecycle"]["state"] == "dead"
        assert len(launches.read_text().splitlines()) == 1
        resumed = WorkerClient().resume(handle, timeout=18)
        assert resumed["session_id"] == sid and resumed["resumed"]
        argv = argv_log.read_text().splitlines()
        assert len(argv) == 2 and "--resume " + sid in argv[-1]
        assert "--effort xhigh" in argv[-1]
        assert "--session-id" not in argv[-1]
        assert store.record(A)["stats"]["turns"] == original["stats"]["turns"]
    finally:
        _stop_disposable_original(short_root)


def test_claude_real_stopped_plain_config_and_cli_default_guards(
    short_root, tmp_path, monkeypatch,
):
    from test_reactivation_claude import _setup, _cli
    from test_create_cross_surface import _first_wait_original_claude_ready
    from test_claude_native_cli import _stop_disposable_original

    launches, store = _setup(short_root, tmp_path, monkeypatch)
    original = f"claude:{A}"
    try:
        assert _first_wait_original_claude_ready(tmp_path, A).worker_id == A
        assert CcxClient().new(
            "M2 stopped worker original history", cwd=str(tmp_path),
            worker_id=A, wait=True, timeout=16, start_timeout=5,
        ).settled
        assert WorkerClient().kill(original, timeout=15)["killed"]
        before = store.record(A)
        assert before["lifecycle"]["state"] == "dead"
        initial_launches = launches.read_text().splitlines()
        updated = CcxClient().set_model(original, "sonnet")
        assert updated.worker_id == A and updated.model == "sonnet"
        worker = WorkerClient().set_effort(original, "low")
        assert worker["restarted"] is False and worker["effort"] == "low"
        assert launches.read_text().splitlines() == initial_launches
        current = store.record(A)

        # The established Claude CLI literal 'default' is a provider model,
        # but its effort 'default' clears that setting (not Codex semantics).
        cleared = _cli(
            "set", original, "--model", "default", "--effort", "default", "--json",
        )
        assert cleared.returncode == 0, (cleared.stdout, cleared.stderr)
        result = json.loads(cleared.stdout)
        assert result["model"] == "default" and result["effort"] is None
        assert store.record(A)["createArgs"]["model"] == "default"
        assert store.record(A)["createArgs"]["effort"] is None

        # No config => --restart must refuse BEFORE any metadata rename.
        saved = store.record(A)
        refused = _cli(
            "set", original, "--restart", "--name", "forbidden", "--json",
        )
        assert refused.returncode != 0
        assert store.record(A) == saved
        assert launches.read_text().splitlines() == initial_launches
        assert store.record(A)["createSha256"] == before["createSha256"]
    finally:
        _stop_disposable_original(short_root)


@pytest.mark.parametrize("refusal", [
    "busy", "waiting", "status-unknown", "startup-timeout", "compact-pending",
])
def test_claude_live_refusals_never_mutate_original_config(
    short_root, tmp_path, monkeypatch, refusal,
):
    from test_reactivation_claude import _setup, _cli
    from test_create_cross_surface import _first_wait_original_claude_ready
    from test_cancel_core import _wait_ref
    from test_claude_native_cli import _stop_disposable_original
    from ccx import client as claude_module
    from ccx.rpc.rpc import _rpc as real_rpc

    launches, store = _setup(short_root, tmp_path, monkeypatch)
    handle = f"claude:{A}"
    try:
        assert _first_wait_original_claude_ready(tmp_path, A).worker_id == A
        if refusal == "busy":
            monkeypatch.setenv("FAKE_MODE", "nostop")
            start = CcxClient().new(
                "M2 native busy original", cwd=str(tmp_path), worker_id=A,
                wait=False, start_timeout=5,
            )
            assert start.worker_id == A
            assert _wait_ref(handle)
        else:
            # These four refusals exercise status preflight, not turn delivery.
            # An unnecessary initial prompt could time out after its durable
            # native send fence under load, although the source is healthy;
            # waiting for the real host's idle state is the needed condition.
            assert _eventually(
                lambda: store.record(A)["lifecycle"]["state"],
                lambda state: state == "idle",
            ) == "idle"

            def status_probe(target, msg, timeout=5):
                result = real_rpc(target, msg, timeout=timeout)
                if refusal == "waiting" and msg.get("op") == "status":
                    return {**result, "state": "waiting"}
                if refusal == "status-unknown" and msg.get("op") == "status":
                    return {"ok": False, "error": "timeout"}
                if refusal == "compact-pending" and msg.get("op") == "status":
                    return {**result, "compacting": False,
                            "compactOperation": {"status": "owned"}}
                if refusal == "startup-timeout" and msg.get("op") == "startup-status":
                    return {"ok": False, "error": "timeout"}
                return result

            monkeypatch.setattr(claude_module, "_rpc", status_probe)
        before = store.record(A)
        expected = (DeliveryRejected if refusal in {"busy", "waiting", "compact-pending"}
                    else SessionUnavailable)
        with pytest.raises(expected):
            CcxClient().set_model(handle, "must-not-save", restart=True)
        with pytest.raises(expected):
            WorkerClient().set_effort(handle, "max", timeout=12)
        assert store.record(A)["createArgs"] == before["createArgs"]
        assert store.record(A)["name"] == before["name"]
        assert store.record(A)["createSha256"] == before["createSha256"]
        assert len(launches.read_text().splitlines()) == 1
    finally:
        # This test's RPC monkeypatch is confined to the client facade, not
        # the cleanup's genuine native socket and owned PID/start protocol.
        _stop_disposable_original(short_root)


def test_worker_effort_is_the_thread_default_on_start_and_resume(
    short_root, tmp_path, monkeypatch,
):
    """Turns typed in an attached TUI carry no effort, so the thread needs it."""
    import test_codex_native_effects as effects
    factory = effects._fake_codex_executable

    def resumable(*args, **kwargs):
        binary = factory(*args, **kwargs)
        before = "        elif method=='thread/unsubscribe':\n"
        binary.write_text(binary.read_text().replace(before, (
            "        elif method=='thread/resume':\n"
            "            tid=params.get('threadId')\n"
            "            result={'thread':dict(threads[tid],status={'type':'idle'}),"
            "'model':threads[tid].get('model')}\n" + before), 1))
        return binary

    monkeypatch.setattr(effects, "_fake_codex_executable", resumable)
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch,
        persisted_threads=tmp_path / "effort-default-threads.json",
    ) as (cli, _restart, trace, store, _rpc):
        created = _f2_new(cli, tmp_path, B, "--effort", "low", "--start-timeout", "5")
        assert created.returncode == 0, (created.stdout, created.stderr)
        assert _m1_wire(trace, "thread/start")[0]["config"]["model_reasoning_effort"] == "low"
        killed = cli("kill", B)
        assert killed.returncode == 0, (killed.stdout, killed.stderr)
        revived = cli("send", B, "after revive", "--no-wait", "--json")
        assert revived.returncode == 0, (revived.stdout, revived.stderr)
        assert _m1_wire(trace, "thread/resume")[-1]["config"]["model_reasoning_effort"] == "low"
