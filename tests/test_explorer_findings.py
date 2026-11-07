"""Regressions from the 2026-10-27 physical exploratory run; provider-free.

All observed workers and provider responses below are sealed fake records or
monkeypatched original RPC results. No actual Claude or Codex process runs.
"""
from __future__ import annotations
import ccx.ops.cancel as ops_cancel
import ccx.ops.report as ops_report
import ccx.ops.send as ops_send
import ccx.ops.wait as ops_wait

import json
from pathlib import Path

import pytest

from ccx.cli import main as cli
from ccx.claude import delivery as claude_delivery
from ccx.codex import runtime as codex_runtime
from ccx import constants
from ccx.ops import creation
from ccx.ops import common as protocol_ops
from ccx.errors import DeliveryRejected, InvalidRequest
from ccx.store.root import require_ccx_active
from test_generation_store import A, B, C, record, spec, short_root
from test_fork_source import sources


@pytest.fixture
def both(short_root, monkeypatch, tmp_path):
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    store = require_ccx_active(short_root)
    for worker, engine in ((A, "claude"), (B, "codex")):
        reserved = store.reserve_creation(worker, engine=engine, fingerprint=spec(engine))
        saved = record(worker, engine)
        saved.update(cwd=str(tmp_path), lifecycle={"state": "idle"})
        store.publish_first_record(reserved, saved)
    return store


def run_cli(capsys, *args, exit_code=0):
    if exit_code:
        with pytest.raises(SystemExit) as stopped:
            cli._main(list(args))
        assert stopped.value.code == exit_code
    else:
        cli._main(list(args))
    return capsys.readouterr()


def test_a_unknown_codex_new_rejected_before_identity_claim(short_root, tmp_path, monkeypatch, executable_engine_stubs):
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    requested = []

    def catalog(*, cwd, timeout=30):
        requested.append(cwd)
        return ["gpt-6-codex", "gpt-5.6-codex"]

    monkeypatch.setattr(codex_runtime, "list_models", catalog)
    with pytest.raises(InvalidRequest, match="Valid models: gpt-6-codex, gpt-5.6-codex"):
        creation.create(engine="codex", cwd=str(tmp_path), model="luna", worker_id=B)
    assert requested == [str(tmp_path)]
    assert require_ccx_active(short_root).list_workers() == []


def test_a_unknown_codex_fork_rejected_before_child_claim(sources, monkeypatch, executable_engine_stubs):
    from ccx.ops.forking import fork
    store, _, _ = sources
    monkeypatch.setattr(codex_runtime, "list_models",
                        lambda **_: ["gpt-6-codex", "gpt-5.6-codex"])
    with pytest.raises(InvalidRequest, match="Valid models: gpt-6-codex"):
        fork(B, worker_id=C, model="luna")
    assert store.record(B)["createArgs"]["model"] == "model-src"
    assert C not in {row["worker_id"] for row in store.list_workers()}


def test_a_unknown_codex_set_does_not_persist_or_call_daemon(both, capsys, monkeypatch):
    from ccx.codex import ops as codex_ops
    before = both.record(B)
    monkeypatch.setattr(codex_runtime, "list_models",
                        lambda **_: ["gpt-6-codex", "gpt-5.6-codex"])
    monkeypatch.setattr(codex_runtime, "observe_worker",
                        lambda *_a, **_kw: pytest.fail("invalid model reached daemon"))
    error = run_cli(capsys, "set", B, "--model", "luna", exit_code=1)
    assert "Valid models: gpt-6-codex" in error.err
    assert both.record(B) == before


@pytest.mark.parametrize("engine,worker", [("claude", A), ("codex", B)])
def test_b_send_detects_crash_without_consuming_timeout(both, capsys, monkeypatch,
                                                       engine, worker):
    full = f"{engine}:{worker}"
    native_ref = f"ccx-turn:{engine}:v1:native-failed"
    timeouts = []

    def send(target, prompt, **kwargs):
        assert target == full
        return {"status": "accepted", "settled": False,
                "delivery": {"turn_ref": native_ref}}

    def reconcile(target, **kwargs):
        timeouts.append(kwargs["timeout"])
        return {"status": "accepted", "delivery": {"turn_ref": native_ref},
                "turn_ref": native_ref}

    def failed_wait(target, *, timeout):
        assert target == full and timeout == 0
        return {"status": "crashed", "error": "model_not_found", "apiError": None}

    monkeypatch.setattr(ops_send, "send_turn", send)
    monkeypatch.setattr(ops_wait, "reconcile_turn", reconcile)
    monkeypatch.setattr(ops_wait, "wait_worker", failed_wait)
    value = run_cli(capsys, "send", worker, "question", "--timeout", "120", "--json",
                    exit_code=3)
    result = json.loads(value.out)
    assert result["status"] == "crashed" and result["error"] == "model_not_found"
    assert len(timeouts) == 1 and 0 < timeouts[0] <= 0.5


def test_b_codex_native_failed_without_crash_is_incomplete(both, capsys, monkeypatch):
    ref = "ccx-turn:codex:v1:t-failed"
    monkeypatch.setattr(ops_send, "send_turn", lambda *_a, **_kw: {
        "status": "accepted", "delivery": {"turn_ref": ref}, "settled": False})
    monkeypatch.setattr(ops_wait, "reconcile_turn", lambda *_a, **_kw: {
        "status": "accepted", "turn_ref": ref, "native_status": "failed",
        "native_error": "model not found", "delivery": {"turn_ref": ref}})
    monkeypatch.setattr(ops_wait, "wait_worker", lambda *_a, **_kw: {
        "status": "idle", "lastTurn": {"turnId": "t-failed", "status": "failed",
                                   "error": "model not found"}})
    result = run_cli(capsys, "send", B, "question", "--timeout", "120", "--json",
                     exit_code=6)
    assert json.loads(result.out)["status"] == "failed"


def test_c_claude_owned_delivery_busy_in_status(both, capsys, monkeypatch):
    both.mutate_record(A, patches={"lastSettledAt": 100})
    entry = {"requestedMode": "new_turn_only", "state": "accepted",
             "ownedAtMs": 150, "settlement": None}
    monkeypatch.setattr(claude_delivery, "iter_receipts", lambda *_: iter([entry]))

    def status():
        return json.loads(run_cli(capsys, "peek", A, "--status", "--json").out)["statuses"][0]

    assert status()["state"] == "busy"
    assert status()["host_status"] == "unobserved"
    # `ccx list` stays a recorded inventory (no per-row receipt scan).
    assert json.loads(run_cli(capsys, "list", "--json").out)[0]["state"] == "idle"
    entry["settlement"] = {"turnRef": "ccx-turn:claude:v1:resolved"}
    assert status()["state"] == "idle"
    entry["settlement"] = None
    both.mutate_record(A, patches={"lastSettledAt": 160})
    assert status()["state"] == "idle"


@pytest.mark.parametrize("engine,worker", [("claude", A), ("codex", B)])
def test_d_notify_parent_still_requires_codex_parent_origin(both, capsys, engine, worker):
    # Omitted child delivery ID is now generated, but no Codex parent is present.
    output = run_cli(capsys, "send", worker, "hi", "--notify-parent", exit_code=1)
    assert "parent notification needs" in output.err
    assert "CODEX_THREAD_ID" in output.err
    assert "--delivery-id" not in output.err


@pytest.mark.parametrize("native_refusal", ["state_unavailable", "waiting"])
def test_d_claude_waiting_send_is_refused_not_unavailable(
    both, capsys, monkeypatch, native_refusal,
):
    def rejected(*_a, **_kw):
        raise DeliveryRejected("pending permission", state="waiting",
                               refusal=native_refusal, delivery_status="rejected")
    monkeypatch.setattr(ops_send, "send_turn", rejected)
    result = run_cli(capsys, "send", A, "hi", "--no-wait", exit_code=5)
    assert "rejected" in result.err


@pytest.mark.parametrize("command", ["send", "new", "notify"])
def test_d_unsupported_claude_parent_has_single_usage_code(both, capsys, monkeypatch,
                                                            command):
    from ccx.cli.commands import _common as commands
    def not_supported(*_a, **_kw):
        raise InvalidRequest("parent notification requires Codex caller")
    monkeypatch.setattr(commands, "_require_parent_notification", not_supported)
    monkeypatch.setattr(ops_send, "send_turn", not_supported)
    if command == "send":
        args = ("send", A, "hi", "--delivery-id", "original", "--notify-parent")
    elif command == "new":
        args = ("new", "--engine", "claude", "--notify-parent", "hi")
    else:
        args = ("notify", A)
        monkeypatch.setattr(__import__('ccx.controllers.orchestrator', fromlist=['require_notify_parent']),
                            "require_notify_parent", not_supported)
    result = run_cli(capsys, *args, exit_code=1)
    assert "Codex" in result.err


@pytest.mark.parametrize("engine,worker", [("claude", A), ("codex", B)])
def test_e_wait_displays_original_crash_error(both, capsys, engine, worker):
    updates = {"lifecycle.state": "crashed"}
    if engine == "claude":
        updates["lifecycle.apiError"] = {"message": "model_not_found", "kind": "invalid_request"}
    else:
        updates["lastTurn"] = {"turnId": "turn-failed", "status": "failed",
                               "error": {"message": "model_not_found"}}
    both.mutate_record(worker, patches=updates)
    obj = json.loads(run_cli(capsys, "wait", worker, "--json", exit_code=3).out)
    assert obj["status"] == "crashed" and obj["error"] == "model_not_found"
    text = run_cli(capsys, "wait", worker, exit_code=3).out
    assert "error: model_not_found" in text


def test_e_interrupted_report_marker_text_and_json(both, capsys, monkeypatch):
    monkeypatch.setattr(ops_report, "report_worker", lambda *_a, **_kw: {
        "worker": f"codex:{B}", "text": "", "status": "interrupted",
        "outcome": "interrupted", "has_final_message": False})
    assert run_cli(capsys, "report", B).out.strip() == "(turn interrupted; no reply)"
    json_record = json.loads(run_cli(capsys, "report", B, "--json").out)
    assert json_record["outcome"] == "interrupted"


def test_e_stop_claude_confirmed_unknown_explained_in_text(both, capsys, monkeypatch):
    monkeypatch.setattr(ops_cancel, "stop_worker", lambda *_a, **_kw: {
        "worker": {"worker": f"claude:{A}", "short": A[:8]},
        "status": "confirmed", "confirmed": True, "outcome": "unknown"})
    assert "the turn is no longer running, outcome unknown" in run_cli(capsys, "stop", A).out
    obj = json.loads(run_cli(capsys, "stop", A, "--json").out)
    assert obj["status"] == "confirmed" and obj["confirmed"] is True
    assert obj["outcome"] == "unknown" and obj["cancelled"] is False


@pytest.mark.parametrize("engine,worker", [("claude", A), ("codex", B)])
def test_f_dead_lifecycle_normalized_like_list(both, capsys, engine, worker):
    both.mutate_record(worker, patches={"lifecycle.state": "dead"})
    peek = json.loads(run_cli(capsys, "peek", worker, "--status", "--json").out)
    assert peek["statuses"][0]["state"] == "stopped"
    rows = json.loads(run_cli(capsys, "list", "--json").out)
    assert next(item for item in rows if item["worker_id"] == worker)["state"] == "stopped"


@pytest.mark.parametrize("engine,worker,shell", [("claude", A, "bash"), ("codex", B, "zsh")])
@pytest.mark.parametrize("attached", [False, True])
def test_g_python_bridge_is_rooted_to_worker_but_preserves_callers_environment(
    both, tmp_path, engine, worker, shell, attached,
):
    import os
    import shlex
    import shutil
    import subprocess
    import sys
    from ccx.controllers import live_vars

    root = both.root
    if attached:
        live_vars.publish_attachment(
            live_vars.prepare_durable_attachment({"counter": 7}), engine, worker,
        )
    env = os.environ.copy()
    foreign_root = str(tmp_path / "absent-foreign-ccx-root")
    env["CCX_ROOT"] = foreign_root
    env["PYTHONPATH"] = str(Path(__file__).resolve().parent.parent)
    env.pop("BASH_ENV", None)
    env.pop("ZDOTDIR", None)
    env.pop("CLAUDE_ENV_FILE", None)

    if shell == "bash":
        binary = shutil.which("bash")
        env_path = live_vars._ensure_shell_env_owned(
            both, engine, worker, inherited_env_file=None, native_env={},
        )
        shell_prefix = f"source {shlex.quote(str(env_path))}; "
        argv = [binary, "--noprofile", "--norc", "-c"]
    else:
        binary = os.environ.get("CCX_TEST_ZSH") or shutil.which("zsh")
        if not binary:
            pytest.skip("zsh required for actual Codex worker shell bridge")
        private_home = tmp_path / "empty-user-home"
        private_home.mkdir(exist_ok=True)
        env["HOME"] = str(private_home)
        env["ZDOTDIR"] = str(live_vars._ensure_codex_zshenv_owned(
            both, worker, original_zdotdir=None, native_env={},
        ))
        shell_prefix = ""
        argv = [binary, "-c"]
    assert str(root) != foreign_root

    code = ("import os; print('native-ok|' + os.environ['CCX_ROOT'])"
            if not attached else
            "import os; counter += 1; print(f'counter:{counter}|' + os.environ['CCX_ROOT'])")
    result = subprocess.run(
        [*argv, shell_prefix + "python3 -c " + shlex.quote(code)],
        env=env, text=True, capture_output=True, timeout=12,
    )
    assert result.returncode == 0, (result.stdout, result.stderr)
    assert result.stdout.strip() == (
        "native-ok|" if not attached else "counter:8|"
    ) + foreign_root
    assert foreign_root not in result.stderr
    if attached:
        assert live_vars.variables_info(engine, worker, name="counter")["value"] == 8
    else:
        assert live_vars.variables_info(engine, worker)["mode"] is None


def test_g_bridge_without_root_from_pre_upgrade_shell_still_parses():
    # Workers started before --root existed keep their generated shell
    # function until their host restarts; their python3 must not break.
    from ccx.cli.parser import build_parser
    args = build_parser().parse_args([
        "_live-vars-exec", "--engine", "claude", "--short", A,
        "--invoked-as", "python3", "--", "-c", "pass",
    ])
    assert args.root is None
