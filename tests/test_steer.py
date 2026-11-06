"""Original full-UUID Claude steering core and public placement adapters."""
from __future__ import annotations

import json
import os
from pathlib import Path
import shlex
import sys
import time

import pytest

from test_generation_store import A, short_root
from test_create_cross_surface import (
    _deadline_fake_claude, _first_wait_original_claude_ready,
)
from test_claude_native_cli import _stop_disposable_original
from ccx.claude import delivery as cd
from ccx import constants
from ccx.errors import DeliveryRejected, InvalidRequest
from ccx.claude.host.session import _semantic_slot_from_receipt
from ccx.ops.wait import reconcile_turn
from ccx.ops.send import send_turn
from ccx.rpc.rpc import _rpc
from ccx.store.root import require_ccx_active


def eventually(call, condition, *, timeout=12):
    until = time.monotonic() + timeout
    last = None
    while time.monotonic() < until:
        last = call()
        if condition(last):
            return last
        time.sleep(.035)
    raise AssertionError(f"native evidence did not arrive: {last!r}")


@pytest.fixture
def hosted(short_root, tmp_path, monkeypatch):
    """Real owned host, PTY and disposable fake provider; no network."""
    from ccx.claude import usage as account_usage
    monkeypatch.setattr(account_usage, "fetch_usage", lambda **kw: None)
    launches = _deadline_fake_claude(short_root, tmp_path, monkeypatch)
    input_log = tmp_path / "native-steer-pty.log"
    monkeypatch.setenv("FAKE_LOG", str(input_log))
    monkeypatch.setenv("FAKE_MODE", "nostop")
    monkeypatch.setenv("FAKE_HOOKS_VIA_SETTINGS", "1")
    monkeypatch.setenv("FAKE_HOOK_RESULT_LOG", str(tmp_path / "native-hook-ack.jsonl"))
    store = require_ccx_active(short_root)
    try:
        _first_wait_original_claude_ready(tmp_path, A)
        yield f"claude:{A}", store, input_log, launches
    finally:
        _stop_disposable_original(short_root)


def _original_busy(handle):
    original = send_turn(handle, "turn R", delivery_id="first-turn-R")
    assert original["delivery"]["status"] == "pending"
    return eventually(
        lambda: reconcile_turn(handle, delivery_id="first-turn-R"),
        lambda v: v["status"] in ("accepted", "settled") and v.get("turn_ref"),
    )["turn_ref"]


def test_idle_explicit_steer_refuses_before_ownership_without_native_effect(hosted):
    handle, store, input_log, launches = hosted
    before = input_log.read_bytes() if input_log.exists() else b""
    with pytest.raises(DeliveryRejected):
        send_turn(handle, "should never start idle", delivery_id="idle-steer",
                  mode="steer_only", cap={"5h": 25})
    assert cd.read_receipt(handle, "idle-steer") is None
    assert store.read_receipt(handle, "claude-delivery", "idle-steer") is None
    assert (input_log.read_bytes() if input_log.exists() else b"") == before
    assert launches.read_text().splitlines() == ["provider-start"]
    # Raw host APIs are REMOVED; a handshaken peer cannot bypass effect fencing.
    assert _rpc(handle, {"op": "steer", "text": "raw", "delivery_id": "raw-id"})["ok"] is False
    assert _rpc(handle, {"op": "steer-status", "delivery_id": "raw-id"})["ok"] is False
    assert (input_log.read_bytes() if input_log.exists() else b"") == before


@pytest.mark.parametrize("mode", ["steer_only", "auto"])
def test_busy_steer_is_owned_once_pending_and_cap_text_id_immutable(hosted, mode):
    handle, store, input_log, launches = hosted
    ref = _original_busy(handle)
    first_native = input_log.read_bytes()
    cap = {"5h": 30} if mode == "steer_only" else None
    if mode == "auto":
        from ccx.rpc.local_rpc import dispatch_full_uuid
        wire = dispatch_full_uuid({
            "protocol_version": 2, "request_id": "v1-auto-busy",
            "op": "turn.send", "timeout_ms": 1800,
            "params": {"target": handle, "text": "queued for R",
                       "delivery_id": "steer-one", "mode": "auto"},
        }, store)
        assert wire["ok"], wire
        submitted = wire["result"]
    else:
        submitted = send_turn(handle, "queued for R", delivery_id="steer-one",
                              mode=mode, cap=cap)
    receipt = cd.read_receipt(handle, "steer-one")
    if mode == "auto":
        assert submitted["delivery"]["owned"] is True
    else:
        assert submitted["status"] in {"owned", "accepted"}, submitted
    assert receipt["requestedMode"] == mode
    assert receipt["route"] == "steer" and receipt["targetTurnRef"] == ref
    assert receipt["cap"] == cap
    assert submitted["delivery"]["status"] == "pending"
    assert reconcile_turn(handle, delivery_id="steer-one")["status"] == "pending"
    # Fake has not reached a tool boundary: no second PTY prompt was written.
    assert input_log.read_bytes() == first_native
    repeated = send_turn(handle, "queued for R", delivery_id="steer-one",
                         mode=mode, cap=cap)
    if mode == "auto":
        from ccx import CcxClient
        auto_python = CcxClient().deliver(
            handle, "queued for R", delivery_id="steer-one",
            mode="auto",
        )
        assert auto_python.worker_id == A
        assert auto_python.route == "steer" and auto_python.target_turn_ref == ref
    assert repeated["delivery"] == submitted["delivery"]
    assert input_log.read_bytes() == first_native
    for changed_text, changed_cap in (
        ("different", cap), ("queued for R", {"5h": 20}),
    ):
        with pytest.raises(cd.ClaudeDeliveryConflict):
            send_turn(handle, changed_text, delivery_id="steer-one",
                      mode=mode, cap=changed_cap)
    assert input_log.read_bytes() == first_native
    assert launches.read_text().splitlines() == ["provider-start"]




def test_auto_idle_new_turn_and_v1_original_receipt_live(hosted):
    from ccx.rpc.local_rpc import dispatch_full_uuid
    handle, store, native, _launches = hosted

    request = {
        "protocol_version": 2, "request_id": "s2-auto-new",
        "op": "turn.send", "timeout_ms": 1800,
        "params": {"target": handle, "text": "V1 auto opens one native turn",
                   "mode": "auto", "delivery_id": "v1-auto-one"},
    }
    result = dispatch_full_uuid(request, store)
    assert result["ok"], result
    delivery = result["result"]["delivery"]
    assert result["result"]["worker"] == handle
    assert delivery["route"] == "new_turn" and delivery["target_turn_ref"] is None
    assert cd.read_receipt(handle, "v1-auto-one")["requestedMode"] == "auto"
    eventually(lambda: native.read_bytes().count(b"V1 auto opens one native turn")
               if native.exists() else 0, lambda n: n == 1)
    request["request_id"] = "s2-auto-same-id"
    repeated = dispatch_full_uuid(request, store)
    assert repeated["ok"] and repeated["result"]["delivery"]["delivery_id"] == "v1-auto-one"
    assert native.read_bytes().count(b"V1 auto opens one native turn") == 1

    request["params"]["mode"] = "steer_only"
    request["request_id"] = "s2-internal-mode-blocked"
    blocked = dispatch_full_uuid(request, store)
    assert not blocked["ok"] and blocked["error"]["code"] == "invalid_request"
    assert native.read_bytes().count(b"V1 auto opens one native turn") == 1



def test_native_admission_can_refuse_steer_after_a_stale_busy_probe(
    hosted, monkeypatch,
):
    """Client saw BUSY, but only live native host IDLE is admission authority."""
    from ccx.rpc import rpc as transport

    handle, store, input_log, launches = hosted
    real = transport._rpc
    before = input_log.read_bytes() if input_log.exists() else b""

    def stale_preflight(worker, request, timeout=5):
        if request.get("op") == "status":
            return {"ok": True, "state": "busy", "activeTurnRef":
                    "ccx-turn:claude:v1:already-finished"}
        return real(worker, request, timeout=timeout)

    with monkeypatch.context() as patch:
        patch.setattr(transport, "_rpc", stale_preflight)
        refused = send_turn(
            handle, "must not begin a turn", delivery_id="raced-idle-steer",
            mode="steer_only",
        )
    assert refused["status"] == "rejected"
    assert refused["delivery"]["rejection_code"] == "state_unavailable"
    assert cd.read_receipt(handle, "raced-idle-steer")["state"] == "rejected"
    effect = store.read_receipt(handle, "claude-delivery", "raced-idle-steer")
    assert effect["state"] == "rejected"
    assert (input_log.read_bytes() if input_log.exists() else b"") == before
    assert launches.read_text().splitlines() == ["provider-start"]


def test_receipt_cap_recovery_survives_evicted_hot_journal():
    receipt = {
        "deliveryId": "recover-steer", "requestedMode": "steer_only",
        "route": "steer", "targetTurnRef": "ccx-turn:claude:v1:R",
        "cap": {"5h": 20, "opus": 40}, "promptText": "recover me",
        "attempts": 1, "maybeWritten": True,
    }
    slot = _semantic_slot_from_receipt(receipt, {})
    assert slot["cap"] == receipt["cap"]
    assert slot["maybe_written"] and slot["phase"] == "await_evidence"
    with pytest.raises(cd.ClaudeDeliveryConflict):
        _semantic_slot_from_receipt(receipt, {"cap": {"5h": 99}})


@pytest.mark.parametrize("cap", [
    {"5h": True}, {"7d": -1}, {"opus": 101}, {"5h": float("nan")},
    {"opus": float("inf")}, {"unknown": 20},
])
def test_invalid_caps_refused_before_creating_any_semantic_effect(hosted, cap):
    handle, store, input_log, _launches = hosted
    before = input_log.read_bytes() if input_log.exists() else b""
    with pytest.raises(InvalidRequest):
        send_turn(handle, "invalid cap", delivery_id="invalid-cap",
                  mode="steer_only", cap=cap)
    assert store.read_receipt(handle, "claude-delivery", "invalid-cap") is None
    assert cd.read_receipt(handle, "invalid-cap") is None
    assert (input_log.read_bytes() if input_log.exists() else b"") == before


def test_no_cap_original_digest_is_byte_identical_to_existing_field_spec():
    from ccx.store.store import _digest
    original = {
        "deliveryId": "original", "promptSha256": "c" * 64,
        "promptByteCount": 8, "requestedMode": "new_turn_only",
        "callerMetadata": None, "cap": None,
    }
    assert cd.spec_digest(original) == _digest(original)
    assert cd.spec_digest({**original, "cap": {"5h": 30}}) != cd.spec_digest(original)


# A test-local disposable provider extension: the real fake Claude still runs
# through original generated settings/hook relays, and a held tool boundary is
# released explicitly by the test. The generated queued_command event is
# provider-owned, with the same native transcript path and byte fence.
def _boundary_provider(tmp_path, monkeypatch):
    repo = Path(__file__).resolve().parent
    fake = (repo / "fake_claude.py").read_text(encoding="utf-8")
    trigger = '        if "nostop" not in MODES:\n'
    assert fake.count(trigger) == 1
    extension = '''
        if "steer_boundary" in MODES and text.strip() == "turn R":
            from threading import Thread
            boundary = Path(os.environ["FAKE_BOUNDARY_GATE"])
            steering = os.environ["FAKE_BOUNDARY_TEXT"]
            while not boundary.exists():
                time.sleep(.015)
            def native_queued():
                until = time.monotonic() + 6
                while time.monotonic() < until:
                    if LOG and Path(LOG).exists() and steering.encode() in Path(LOG).read_bytes():
                        native_row({"type": "queue-operation", "operation": "enqueue",
                                    "content": steering, "uuid": "queued-steer"})
                        time.sleep(.06)
                        native_row({"type": "user", "uuid": "joined-steer",
                                    "message": {"content": [
                                        {"type": "queued_command", "text": steering}]}})
                        return
                    time.sleep(.01)
            Thread(target=native_queued, daemon=True).start()
            hook("PostToolUse", {"tool_name": "Read", "tool_use_id": "fake-boundary-R"})
            if "complete_boundary" in MODES:
                hook("Stop", {"session_id": SESSION_ID,
                              "last_assistant_message": "Only turn R completed"})
'''
    provider = tmp_path / "boundary_fake_claude.py"
    provider.write_text(fake.replace(trigger, extension + trigger), encoding="utf-8")
    wrapper = tmp_path / "boundary_fake_wrapper"
    wrapper.write_text(
        "#!/bin/sh\nexec " + shlex.quote(sys.executable) + " "
        + shlex.quote(str(provider)) + ' "$@"\n', encoding="utf-8",
    )
    wrapper.chmod(0o700)
    monkeypatch.setattr(constants, "CLAUDE_BIN", str(wrapper))
    monkeypatch.setenv("CCX_CLAUDE_BIN", str(wrapper))


def test_native_busy_boundary_consumes_once_and_never_borrows_R_completion(
    short_root, tmp_path, monkeypatch,
):
    _deadline_fake_claude(short_root, tmp_path, monkeypatch)
    _boundary_provider(tmp_path, monkeypatch)
    input_log = tmp_path / "boundary-pty.log"
    trigger = tmp_path / "release-boundary"
    steer_text = "joining R only"
    monkeypatch.setenv("FAKE_LOG", str(input_log))
    monkeypatch.setenv("FAKE_MODE", "nostop,steer_boundary,complete_boundary")
    monkeypatch.setenv("FAKE_HOOKS_VIA_SETTINGS", "1")
    monkeypatch.setenv("FAKE_BOUNDARY_GATE", str(trigger))
    monkeypatch.setenv("FAKE_BOUNDARY_TEXT", steer_text)
    handle = f"claude:{A}"
    try:
        _first_wait_original_claude_ready(tmp_path, A)
        ref = _original_busy(handle)
        from ccx import CcxClient
        steer = CcxClient().steer(handle, steer_text, delivery_id="joins-R",
                                  wait=False)
        assert steer.accepted and steer.status == "accepted-pending"
        assert steer.as_dict()["worker"] == handle
        assert cd.read_receipt(handle, "joins-R")["targetTurnRef"] == ref
        trigger.touch()
        joined = eventually(lambda: reconcile_turn(handle, delivery_id="joins-R"),
                            lambda v: v["delivery"].get("placement") == "current_turn")
        assert joined["turn_ref"] == ref and joined["status"] == "accepted"
        assert joined["delivery"]["settlement"] is None
        native = input_log.read_bytes()
        assert native.count(steer_text.encode()) == 1
        after = eventually(lambda: reconcile_turn(handle, delivery_id="first-turn-R"),
                           lambda v: v["status"] == "settled")
        assert after["delivery"]["settlement"]["text"] == "Only turn R completed"
        steer_after = reconcile_turn(handle, delivery_id="joins-R")
        assert steer_after["delivery"]["settlement"] is None
        assert steer_after["status"] == "accepted"
        public = CcxClient().steer_status(handle, "joins-R")
        assert public.status == "consumed-current-turn"
        assert public.worker_id == A and public.accepted
        assert input_log.read_bytes() == native
    finally:
        _stop_disposable_original(short_root)


def test_lost_native_ack_same_steer_id_returns_owned_receipt_without_redelivery(
    hosted, monkeypatch,
):
    """Physical host ACK reaches client RPC, but its return is lost once."""
    from ccx.rpc import rpc as transport
    from ccx.store.root import NativeEffectIndeterminate

    handle, store, input_log, _launches = hosted
    ref = _original_busy(handle)
    before = input_log.read_bytes()
    real_rpc = transport._rpc
    attempts = []

    def lost_ack(worker, message, timeout=5):
        reply = real_rpc(worker, message, timeout=timeout)
        if (message.get("op") == "deliver"
                and message.get("delivery_id") == "lost-steer-ack"):
            attempts.append(reply)
            raise TimeoutError("disposable caller lost acknowledged socket reply")
        return reply

    with monkeypatch.context() as patch:
        patch.setattr(transport, "_rpc", lost_ack)
        with pytest.raises(NativeEffectIndeterminate):
            send_turn(handle, "at most once steering", delivery_id="lost-steer-ack",
                      mode="steer_only")
    assert len(attempts) == 1 and attempts[0]["delivery"]["owned"]
    state = cd.read_receipt(handle, "lost-steer-ack")
    assert state["state"] == "owned" and state["targetTurnRef"] == ref
    assert store.read_receipt(handle, "claude-delivery", "lost-steer-ack")["state"] == "submitted-unknown"
    again = send_turn(handle, "at most once steering", delivery_id="lost-steer-ack",
                      mode="steer_only")
    assert again["delivery"]["owned"] is True
    assert again["delivery"]["target_turn_ref"] == ref
    assert input_log.read_bytes() == before


def test_timed_out_host_reply_reports_the_owned_receipt(hosted, monkeypatch):
    """The host owns the message but its reply times out: no indeterminate."""
    from ccx.rpc import rpc as transport

    handle, _store, _input_log, _launches = hosted
    real_rpc = transport._rpc

    def timed_out(worker, message, timeout=5):
        reply = real_rpc(worker, message, timeout=timeout)
        if message.get("op") == "deliver":
            return {"ok": False, "error": "timeout"}
        return reply

    monkeypatch.setattr(transport, "_rpc", timed_out)
    result = send_turn(handle, "turn after a lost reply", delivery_id="timed-out-reply")
    assert result["delivery"]["owned"] is True
    assert cd.read_receipt(handle, "timed-out-reply")["state"] in ("owned", "accepted")


def test_native_current_turn_steer_merges_cap_and_trips_cached_usage(
    short_root, tmp_path, monkeypatch,
):
    """Real PTY/native hook boundary; injected usage cache, no network."""
    _deadline_fake_claude(short_root, tmp_path, monkeypatch)
    _boundary_provider(tmp_path, monkeypatch)
    input_log = tmp_path / "capped-steer-pty.log"
    trigger = tmp_path / "release-capped-boundary"
    steer_text = "merge this quota cap"
    cache = tmp_path / "usage.json"
    # Capped host now trusts only a fresh, source-qualified CCX cache.
    from ccx.claude import usage as account_usage
    sampled = account_usage._apply_sample("get_usage", None, {
        "kind": "success", "body": {
            "five_hour": {"utilization": 30.0},
            "seven_day": {"utilization": 0.0},
        },
    }, time.time())
    cache.write_text(json.dumps(sampled))
    cache.chmod(0o600)
    monkeypatch.setattr(account_usage, "usage_cache_path", lambda: cache)
    # The host learns the elevated 30% metric after a preflight that saw 10%.
    monkeypatch.setattr(account_usage, "fetch_usage", lambda **kw: {
        "five_hour": {"utilization": 10.0},
    })
    monkeypatch.setenv("CCX_USAGE_CACHE", str(cache))
    monkeypatch.setenv("FAKE_LOG", str(input_log))
    monkeypatch.setenv("FAKE_MODE", "nostop,steer_boundary")
    monkeypatch.setenv("FAKE_HOOKS_VIA_SETTINGS", "1")
    monkeypatch.setenv("FAKE_BOUNDARY_GATE", str(trigger))
    monkeypatch.setenv("FAKE_BOUNDARY_TEXT", steer_text)
    handle = f"claude:{A}"
    try:
        _first_wait_original_claude_ready(tmp_path, A)
        first = send_turn(handle, "turn R", delivery_id="capped-original",
                          cap={"5h": 50})
        assert first["status"] == "owned"
        ref = eventually(
            lambda: reconcile_turn(handle, delivery_id="capped-original"),
            lambda v: bool(v.get("turn_ref")),
        )["turn_ref"]
        cap_steer = send_turn(handle, steer_text, delivery_id="capped-steer",
                              mode="steer_only", cap={"5h": 20, "7d": 70})
        assert cap_steer["delivery"]["route"] == "steer"
        assert cap_steer["delivery"]["target_turn_ref"] == ref
        trigger.touch()
        store = require_ccx_active(short_root)
        stopped = eventually(
            lambda: store.record(A)["lifecycle"],
            lambda v: v["state"] == "budget-stopped",
        )
        # The original 50% cap did not trip at 30%, but the steer's
        # strictest merged 20% cap did; no second provider input is needed.
        # The host merges a steer cap only after writing the steer, so the
        # stop proves the write; the stop may kill the fake before it logs it.
        assert "usage cap 5h reached: 30% ≥ 20%" in str(stopped)
        assert (input_log.read_bytes() if input_log.exists() else b"").count(
            steer_text.encode()) <= 1
        probe = reconcile_turn(handle, delivery_id="capped-steer")
        assert probe["status"] in {"accepted", "pending"}
        if probe["status"] == "pending":
            assert probe["delivery"]["settlement"] is None
        assert cd.read_receipt(handle, "capped-steer")["cap"] == {
            "5h": 20, "7d": 70,
        }
    finally:
        _stop_disposable_original(short_root)


def test_public_python_worker_steer_pending_timeout_replay_and_full_identity(hosted):
    from ccx import CcxClient, WorkerClient
    from ccx.errors import OperationConflict, SessionNotFound

    handle, _store, native, _launches = hosted
    ref = _original_busy(handle)
    before = native.read_bytes()
    py = CcxClient().steer(handle, "public queued for R",
                           delivery_id="public-pending-steer",
                           caps={"5h": 35}, wait=True, timeout=0)
    assert py.accepted and py.pending and py.timed_out
    assert py.status == "accepted-pending" and py.recommended_action == "wait"
    assert py.worker_id == A and py.as_dict()["worker"] == handle
    assert py.short == A[:8]
    observed = CcxClient().steer_status(handle, "public-pending-steer")
    assert observed.accepted and observed.status == "accepted-pending"
    worker = WorkerClient()
    again = worker.steer(handle, "public queued for R",
                         delivery_id="public-pending-steer",
                         caps={"5h": 35}, wait=False)
    assert again["worker"] == handle and again["worker_id"] == A
    assert again["status"] == "accepted-pending" and again["accepted"]
    assert worker.capabilities(handle)["steer_current_turn"]
    assert worker.capabilities("codex")["steer_current_turn"]
    receipt = cd.read_receipt(handle, "public-pending-steer")
    assert receipt["cap"] == {"5h": 35} and receipt["targetTurnRef"] == ref
    with pytest.raises(OperationConflict):
        CcxClient().steer(handle, "public queued for R",
                          delivery_id="public-pending-steer",
                          caps={"5h": 30}, wait=False)
    with pytest.raises(SessionNotFound):
        CcxClient().steer_status(handle, "unknown-steer")
    assert native.read_bytes() == before


@pytest.mark.parametrize("source", ["stdin", "file"])
def test_public_cli_source_status_idempotent_no_repaste(hosted, tmp_path, source):
    import subprocess
    from ccx import CcxClient
    handle, store, native, _launches = hosted
    _original_busy(handle)
    before = native.read_bytes()
    content = "CLI-" + source + " queued for R"
    argv = [sys.executable, "-B", "-m", "ccx", "steer", handle]
    if source == "stdin":
        argv += ["--stdin"]
        source_text = content
    else:
        path = tmp_path / "steer-prompt.txt"
        path.write_text(content)
        argv += ["--file", str(path)]
        source_text = None
    argv += ["--delivery-id", "cli-steer-one", "--timeout", "0",
             "--cap-5h", "40", "--json"]
    proc = subprocess.run(argv, input=source_text, text=True,
                          capture_output=True, env=os.environ.copy(), timeout=15)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)
    result = json.loads(proc.stdout)
    assert result["worker"] == handle and result["worker_id"] == A
    assert result["status"] == "accepted-pending"
    assert result["accepted"] and result["timed_out"] and result["pending"]
    assert cd.read_receipt(handle, "cli-steer-one")["cap"] == {"5h": 40}
    status = subprocess.run(
        [sys.executable, "-B", "-m", "ccx", "reconcile",
         handle, "--delivery-id", "cli-steer-one", "--json"],
        capture_output=True, text=True, env=os.environ.copy(), timeout=15)
    assert status.returncode == 0, (status.stdout, status.stderr)
    observed = json.loads(status.stdout)
    assert observed["worker"] == handle
    assert observed["delivery_id"] == "cli-steer-one"
    assert observed["status"] == "accepted-pending"
    repeat = CcxClient().steer(handle, content, delivery_id="cli-steer-one",
                               caps={"5h": 40}, wait=False)
    assert repeat.delivery_id == "cli-steer-one"
    assert native.read_bytes() == before


def test_public_cli_strict_cap_and_codex_absent_daemon_steer_refusal(hosted, tmp_path):
    import subprocess
    from test_generation_store import B
    from test_delivery_flow import _activate_fake_codex
    from ccx.ops.creation_steps import prepare_worker
    from ccx import WorkerClient
    from ccx.codex import delivery as codex_delivery
    from ccx.errors import CapabilityUnsupported, DeliveryRejected

    handle, store, native, _launches = hosted
    cmd = [sys.executable, "-B", "-m", "ccx", "send", handle,
           "public strict cap", "--no-wait", "--delivery-id",
           "public-strict-cap", "--cap-7d", "95", "--json"]
    result = subprocess.run(cmd, text=True, capture_output=True,
                            env=os.environ.copy(), timeout=15)
    assert result.returncode == 0, (result.stdout, result.stderr)
    sent = json.loads(result.stdout)
    assert sent["worker"] == handle
    assert cd.read_receipt(handle, "public-strict-cap")["cap"] == {"7d": 95}

    prepared = prepare_worker(store, engine="codex", cwd=str(tmp_path), worker_id=B)
    _activate_fake_codex(store, prepared)  # Original native binding, no codexd.
    codex = f"codex:{B}"
    worker = WorkerClient()
    with pytest.raises(DeliveryRejected) as denied:
        worker.steer(codex, "not yet", delivery_id="codex-steer-no-daemon", wait=False)
    assert denied.value.refusal == "worker-state"
    assert denied.value.delivery_id == "codex-steer-no-daemon"
    assert denied.value.delivery_status is None
    assert codex_delivery.read_receipt(codex, "codex-steer-no-daemon") is None
    assert store.read_receipt(codex, "codex-send", "codex-steer-no-daemon") is None
    with pytest.raises(InvalidRequest, match="cap-opus"):
        worker.send(codex, "not capped", wait=False, caps={"opus": 30},
                    delivery_id="codex-send-cap")
    assert codex_delivery.read_receipt(codex, "codex-send-cap") is None
    assert store.read_receipt(codex, "codex-send", "codex-send-cap") is None

def test_direct_injected_auto_ack_requires_true_route_and_never_replays(
    short_root, tmp_path, monkeypatch,
):
    from test_delivery_flow import _activate_fake_claude
    from ccx.ops.creation_steps import prepare_worker
    from ccx.store.root import NativeEffectIndeterminate
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    store = require_ccx_active(short_root)
    prepared = prepare_worker(store, engine="claude", cwd=str(tmp_path), worker_id=A)
    _activate_fake_claude(store, prepared)
    session = store.record(A)["native"]["sessionId"]
    calls = []
    def ambiguous(handle, native, text, token, budget):
        calls.append(token)
        return {"accepted": True, "workerId": A, "engine": "claude",
                "deliveryId": token, "nativeSource": "claude-session:" + session,
                "sessionId": session, "turnRef": "ccx-turn:claude:v1:fake-ref",
                "nativeEvidenceSha256": "a" * 64}
    with pytest.raises(NativeEffectIndeterminate, match="exact auto/steer route"):
        send_turn(f"claude:{A}", "auto injected", delivery_id="unknown-route",
                  mode="auto", _native_submit=ambiguous)
    assert cd.read_receipt(f"claude:{A}", "unknown-route")["state"] == "prepared"
    assert store.read_receipt(f"claude:{A}", "claude-delivery", "unknown-route")["state"] == "submitted-unknown"
    with pytest.raises(NativeEffectIndeterminate):
        send_turn(f"claude:{A}", "auto injected", delivery_id="unknown-route",
                  mode="auto", _native_submit=ambiguous)
    assert calls == ["unknown-route"]


def test_public_capped_worker_and_python_strict_send_same_original_receipt(hosted):
    import subprocess
    from ccx import WorkerClient, CcxClient
    handle, store, native, _launches = hosted
    worker = WorkerClient()
    first = worker.send(
        handle, "worker strict quota", delivery_id="worker-strict-quota",
        caps={"5h": 70}, wait=False,
    )
    assert first["worker"] == handle and first["worker_id"] == A
    assert cd.read_receipt(handle, "worker-strict-quota")["cap"] == {"5h": 70}
    eventually(lambda: native.read_bytes().count(b"worker strict quota")
               if native.exists() else 0, lambda n: n == 1)
    again = CcxClient().send(
        handle, "worker strict quota", delivery_id="worker-strict-quota",
        caps={"5h": 70}, wait=False,
    )
    assert again.delivery_id == "worker-strict-quota"
    assert native.read_bytes().count(b"worker strict quota") == 1
    rejected = subprocess.run(
        [sys.executable, "-B", "-m", "ccx", "send", handle,
         "invalid cap must not paste", "--no-wait",
         "--delivery-id", "cli-invalid-budget", "--cap-5h", "nan"],
        capture_output=True, text=True, env=os.environ.copy(), timeout=12,
    )
    assert rejected.returncode != 0
    assert cd.read_receipt(handle, "cli-invalid-budget") is None
    assert native.read_bytes().count(b"worker strict quota") == 1


def test_interrupt_refusal_only_while_text_may_be_in_claude():
    from ccx.claude.host.session import _interrupt_refusal_for_delivery as refusal
    assert refusal(None) is None
    held = {"delivery_id": "d", "route": "steer", "phase": "steer_boundary",
            "maybe_written": False}
    assert refusal(held) is None  # CCX still holds it: Escape cannot lose it
    assert refusal({**held, "route": "new_turn", "phase": "new_turn"}) is None
    assert "retry in a few seconds" in refusal({**held, "maybe_written": True})
    assert "retry in a few seconds" in refusal(
        {**held, "route": "new_turn", "maybe_written": True})
    assert "reconcile" in refusal({"delivery_id": None})


@pytest.fixture
def interruptible(short_root, tmp_path, monkeypatch):
    """Like `hosted`, but the fake writes Claude's interrupt marker on Escape."""
    from ccx.claude import usage as account_usage
    monkeypatch.setattr(account_usage, "fetch_usage", lambda **kw: None)
    launches = _deadline_fake_claude(short_root, tmp_path, monkeypatch)
    input_log = tmp_path / "native-steer-pty.log"
    monkeypatch.setenv("FAKE_LOG", str(input_log))
    monkeypatch.setenv("FAKE_MODE", "nostop,manual_interrupt_marker")
    monkeypatch.setenv("FAKE_HOOKS_VIA_SETTINGS", "1")
    monkeypatch.setenv("FAKE_HOOK_RESULT_LOG", str(tmp_path / "native-hook-ack.jsonl"))
    store = require_ccx_active(short_root)
    try:
        _first_wait_original_claude_ready(tmp_path, A)
        yield f"claude:{A}", store, input_log, launches
    finally:
        _stop_disposable_original(short_root)


def test_interrupt_with_held_steer_stops_turn_and_steer_runs_next(interruptible):
    handle, store, input_log, launches = interruptible
    ref = _original_busy(handle)
    submitted = send_turn(handle, "steer held through stop", delivery_id="held-steer",
                          mode="steer_only")
    assert submitted["delivery"]["status"] == "pending"
    assert cd.read_receipt(handle, "held-steer")["maybeWritten"] is False
    assert b"steer held through stop" not in input_log.read_bytes()

    from ccx.ops.cancel import stop_worker
    stopped = stop_worker(handle, timeout=8)
    assert stopped["accepted"] is True and stopped["turn_ref"] == ref, stopped
    eventually(lambda: store.record(handle).get("lastSettledTurn") or {},
               lambda v: v.get("turnRef") == ref and v.get("outcome") == "interrupted")
    # The held steer goes out once, as the next turn.
    receipt = eventually(lambda: cd.read_receipt(handle, "held-steer"),
                         lambda r: r["state"] in {"accepted", "delivered"})
    assert receipt["placement"] == "new_turn"
    assert reconcile_turn(handle, delivery_id="held-steer")["status"] in {"accepted", "settled"}
    assert input_log.read_bytes().count(b"steer held through stop") == 1
    assert launches.read_text().splitlines() == ["provider-start"]
