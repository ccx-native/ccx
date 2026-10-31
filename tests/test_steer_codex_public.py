"""Public full-UUID Codex steer over the accepted original fake app-server.

The S1 fixture owns the disposable codexd/app-server, original activation and
exact native R. No real Codex binary, new harness or raw worker steer path.
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from ccx import CodexClient
from ccx import WorkerClient
from ccx.codex import delivery as codex_delivery
from ccx import constants
from ccx.codex.provenance import parse_client_message_id
from ccx.codex.runtime import daemon_rpc, settled_daemon_status
from ccx.errors import (
    CapabilityUnsupported, DeliveryRejected, InvalidRequest, OperationConflict,
    SendIndeterminate, SessionUnavailable, SessionWaiting,
)
from ccx.ops.wait import reconcile_turn
from ccx.ops.send import send_turn
from ccx.ops.creation_steps import prepare_worker
from ccx.store.root import require_ccx_active
from ccx.store.worker_state import RUNTIME_ABSENT
from test_generation_store import A, B, C, short_root
from test_delivery_flow import _activate_fake_codex
from test_steer_codex import original_codex, _hold_busy


def _cli(store, *args, input=None):
    env = {**os.environ, "CCX_ROOT": str(store.root),
           "PYTHONPATH": str(Path(__file__).resolve().parent.parent),
           "PYTHONDONTWRITEBYTECODE": "1", "CCX_REAL_PARITY": "0"}
    return subprocess.run(
        [sys.executable, "-B", "-m", "ccx", *args],
        text=True, input=input, capture_output=True, env=env, timeout=20,
    )


def _stop_disposable_fake(store):
    """Stop only the fixture-owned codexd, not an ambient provider process."""
    probe = daemon_rpc({"op": "daemon-status"}, timeout=1)
    assert probe.get("ok") is True and probe.get("pid")
    pid = probe["pid"]
    os.kill(pid, signal.SIGTERM)
    until = time.monotonic() + 5
    while time.monotonic() < until:
        status = settled_daemon_status(time.monotonic() + .2)
        if status.get("transport") == RUNTIME_ABSENT:
            break
        time.sleep(.04)
    else:
        pytest.fail(f"disposable fixture daemon failed to release its role: {status}")
    assert require_ccx_active(store.root).generation == store.generation


def test_typed_and_worker_steer_exact_R_token_no_completion(original_codex):
    store, trace = original_codex
    r = _hold_busy(store, trace)
    before_starts = trace.read_text().count("RPC turn/start")
    client = CodexClient()
    typed = client.steer(f"codex:{B}", "public typed R", delivery_id="public-typed-R")
    value = typed.as_dict()
    assert typed.action == "steer" and typed.accepted
    assert typed.turn_id == r and typed.delivery_id == "public-typed-R"
    assert value["worker"] == f"codex:{B}" and value["worker_id"] == B
    assert value["thread_id"] == store.record(B)["native"]["threadId"]
    provenance = parse_client_message_id(typed.client_user_message_id)
    assert provenance["workerId"] == B and provenance["kind"] == "steer"
    assert trace.read_text().count("RPC turn/steer") == 1
    assert trace.read_text().count("RPC turn/start") == before_starts

    # Native acceptance is immediate placement, not model completion.
    again = client.steer(B, "public typed R", delivery_id="public-typed-R")
    assert again.turn_id == r and again.delivery_id == typed.delivery_id
    assert again.client_user_message_id == typed.client_user_message_id
    assert trace.read_text().count("RPC turn/steer") == 1
    with pytest.raises(OperationConflict):
        client.steer(B, "changed prompt", delivery_id="public-typed-R")
    assert trace.read_text().count("RPC turn/steer") == 1

    facade = WorkerClient()
    assert facade.capabilities("codex")["steer_current_turn"] is True
    # Both engines now have durable/reconcilable steer IDs; the shared flag is removed.
    assert "durable_steer_delivery" not in facade.capabilities("codex")
    assert facade.capabilities("codex")["auto_delivery"] is False
    result = facade.steer(B, "public Worker R", delivery_id="worker-steer-R", wait=True)
    assert result["worker"] == f"codex:{B}" and result["worker_id"] == B
    assert result["status"] == "accepted-current-turn" and result["accepted"]
    assert not result["pending"] and not result["timed_out"]
    assert result["recommended_action"] is None
    assert result["turn_id"] == r and result["delivery_id"] == "worker-steer-R"
    assert parse_client_message_id(result["client_user_message_id"])["kind"] == "steer"
    assert facade.steer(B, "public Worker R", delivery_id="worker-steer-R", wait=False)[
        "client_user_message_id"] == result["client_user_message_id"]
    assert trace.read_text().count("RPC turn/steer") == 2
    assert trace.read_text().count("RPC turn/start") == before_starts
    saved = codex_delivery.read_receipt(B, "worker-steer-R")
    assert saved["targetTurnId"] == r and saved.get("settlement") is None
    assert reconcile_turn(B, delivery_id="worker-steer-R")["status"] == "accepted"


def test_r1_waiting_gate_and_codex_budget_refusal(original_codex):
    store, trace = original_codex
    gates = [{"gateId": "g-original", "reason": "Approval needed",
              "kind": "command"}]
    store.mutate_record(B, patches={
        "lifecycle.state": "waiting", "pendingApprovals": gates,
    })
    client = WorkerClient()
    before = trace.read_text()
    with pytest.raises(InvalidRequest, match="cap-opus"):
        client.steer(B, "blocked", delivery_id="no-cap", caps={"opus": 40})
    assert codex_delivery.read_receipt(B, "no-cap") is None
    with pytest.raises(SessionWaiting) as waiting:
        client.steer(B, "permission blocked", delivery_id="waiting-steer")
    expected = client.status(B)["pending_input"]["native"]
    assert waiting.value.waiting_for == expected == {"pendingApprovals": gates}
    assert isinstance(waiting.value.__cause__, DeliveryRejected)
    assert waiting.value.__cause__.refusal == "worker-state"
    assert trace.read_text().count("RPC turn/steer") == before.count("RPC turn/steer")
    assert trace.read_text().count("RPC turn/start") == before.count("RPC turn/start")


def test_d1_new_original_without_daemon_and_invalid_source(short_root, tmp_path, monkeypatch):
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    store = require_ccx_active(short_root)
    prepared = prepare_worker(store, engine="codex", cwd=str(tmp_path), worker_id=B)
    # A mere prepared native record is not a confirmed original thread.
    with pytest.raises(SessionUnavailable):
        CodexClient().steer(B, "unbound native", delivery_id="not-activated")
    assert codex_delivery.read_receipt(B, "not-activated") is None
    _activate_fake_codex(store, prepared)  # Original immutable native binding, no daemon.
    client = CodexClient()
    with pytest.raises(InvalidRequest):
        client.steer(B, "  ", delivery_id="invalid-text")
    with pytest.raises(InvalidRequest):
        client.steer(B, "valid text", delivery_id="")
    with pytest.raises(DeliveryRejected) as absent:
        client.steer(B, "no running turn", delivery_id="absent-fresh")
    assert absent.value.refusal == "worker-state"
    assert absent.value.delivery_id == "absent-fresh"
    assert absent.value.delivery_status is None
    assert "no delivery was recorded" in str(absent.value)
    assert codex_delivery.read_receipt(B, "absent-fresh") is None
    assert store.read_receipt(B, "codex-send", "absent-fresh") is None
    refused_cli = _cli(store, "steer", B, "no running turn",
                       "--delivery-id", "absent-cli", "--json")
    assert refused_cli.returncode == 5 and not refused_cli.stdout
    assert "nothing recorded" in refused_cli.stderr
    assert codex_delivery.read_receipt(B, "absent-cli") is None
    assert store.read_receipt(B, "codex-send", "absent-cli") is None
    assert not (store.root / "run").exists()
    with pytest.raises(DeliveryRejected):
        client.steer(B, "no running turn", delivery_id="absent-fresh")
    assert codex_delivery.read_receipt(B, "absent-fresh") is None


def test_d1_accepted_and_rejected_replay_after_disposable_daemon_stops(
    original_codex, monkeypatch,
):
    store, trace = original_codex
    r = _hold_busy(store, trace)
    client = CodexClient()
    accepted = client.steer(B, "accepted before stop", delivery_id="known-steer")
    assert accepted.turn_id == r
    with pytest.raises(DeliveryRejected) as idle:
        client.steer(A, "idle rejected", delivery_id="terminal-rejected")
    assert idle.value.delivery_status == "rejected"
    before = trace.read_text()
    _stop_disposable_fake(store)

    from ccx.codex import runtime as codex_runtime
    def no_probe(*args, **kwargs):
        pytest.fail("an existing original token must never probe daemon presence")
    monkeypatch.setattr(codex_runtime, "settled_daemon_status", no_probe)
    replay = client.steer(B, "accepted before stop", delivery_id="known-steer")
    assert replay.turn_id == accepted.turn_id
    assert replay.delivery_id == accepted.delivery_id
    with pytest.raises(DeliveryRejected) as again:
        client.steer(A, "idle rejected", delivery_id="terminal-rejected")
    assert again.value.delivery_status == "rejected"
    assert again.value.delivery_id == "terminal-rejected"
    with pytest.raises(OperationConflict):
        client.steer(B, "changed text", delivery_id="known-steer")
    assert trace.read_text() == before


def test_d1_pre_route_uncertain_stays_unknown_after_daemon_absent(original_codex):
    store, trace = original_codex
    r = _hold_busy(store, trace)
    token, prompt = "pre-route-lost", "original uncertain steer"
    before = trace.read_text()
    def lost_before_daemon(*args):
        raise TimeoutError("original IPC ACK lost before route selection")
    with pytest.raises(SendIndeterminate) as first:
        send_turn(B, prompt, delivery_id=token, mode="steer_only",
                  _native_submit=lost_before_daemon)
    assert first.value.delivery_id == token
    assert codex_delivery.read_receipt(B, token)["state"] == "prepared"
    assert store.read_receipt(B, "codex-send", token)["state"] == "submitted-unknown"
    _stop_disposable_fake(store)
    with pytest.raises(SendIndeterminate) as after:
        CodexClient().steer(B, prompt, delivery_id=token)
    assert after.value.delivery_id == token
    pending = reconcile_turn(B, delivery_id=token)
    assert pending["status"] == "submitted" and pending["pending"]
    assert pending["recommended_action"] == "wait"
    assert pending["delivery"]["route"] is None
    cli_lost = _cli(store, "steer", B, prompt,
                    "--delivery-id", token, "--json")
    assert cli_lost.returncode == 6
    lost_json = json.loads(cli_lost.stdout)
    assert lost_json["delivery_id"] == token
    assert lost_json["reconcile_required"] is True
    assert trace.read_text() == before


def test_real_cli_exact_prefix_ambiguous_wrong_engine_and_token(original_codex):
    store, trace = original_codex
    r = _hold_busy(store, trace)
    # Stopped workers still count toward ambiguous central CLI prefixes.
    store.mutate_record(A, patches={"lifecycle.state": "dead"})
    before = trace.read_text()
    ambiguous = _cli(store, "steer", "aaaaaaaa", "not placed", "--json")
    assert ambiguous.returncode == 1 and not ambiguous.stdout
    assert "ambiguous" in ambiguous.stderr
    assert f"codex:{A}" in ambiguous.stderr and f"codex:{B}" in ambiguous.stderr
    assert trace.read_text() == before

    prepare_worker(store, engine="claude", cwd=str(store.root), worker_id=C)
    wrong = _cli(store, "steer", f"claude:{C}", "wrong", "--json")
    assert wrong.returncode == 1 and not wrong.stdout
    assert trace.read_text() == before
    token, prompt = "public-cli-1", "CLI steers exact R"
    for target in (f"codex:{B}", B, B[:13]):
        answer = _cli(store, "steer", target, prompt,
                      "--delivery-id", token, "--json")
        assert answer.returncode == 0, (answer.stdout, answer.stderr)
        flat = json.loads(answer.stdout)
        assert flat["worker"] == f"codex:{B}" and flat["worker_id"] == B
        assert flat["delivery_id"] == token and flat["turn_id"] == r
        assert flat["client_user_message_id"]
    assert trace.read_text().count("RPC turn/steer") == 1
    assert trace.read_text().count("RPC turn/start") == 1
    other = _cli(store, "steer", B,
                 "--stdin", "--no-wait", "--delivery-id", "public-cli-stdin",
                 input="from stdin")
    assert other.returncode == 0 and f"codex:{B}" in other.stdout
    assert "public-cli-stdin" in other.stdout
    assert trace.read_text().count("RPC turn/steer") == 2
    conflict = _cli(store, "steer", B, "different text",
                    "--delivery-id", token, "--json")
    assert conflict.returncode == 5
    assert trace.read_text().count("RPC turn/steer") == 2
