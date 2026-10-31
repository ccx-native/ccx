"""Direct Claude original brokered input wait and strict no-effect fallback."""
from __future__ import annotations
import ccx.ops.report as ops_report
import ccx.ops.wait as ops_wait

import json
import time

import pytest

from ccx import constants
from ccx.ops import common as protocol_ops
from ccx.client import CcxClient
from ccx.store.paths import events_ndjson_path
from ccx.errors import DeliveryRejected, SessionUnavailable, SessionWaiting, TurnIncomplete
from ccx.types import SendResult
from test_generation_store import A, short_root
from test_claude_native_cli import generated_hook_host


def _pending(store, kind):
    until = time.monotonic() + 12
    while time.monotonic() < until:
        value = store.record(A).get("waitingFor")
        if isinstance(value, dict) and value.get("id"):
            if ((kind == "question" and value.get("tool") == "AskUserQuestion")
                    or (kind == "elicitation" and value.get("type") == "elicitation")
                    or (kind == "permission" and value.get("tool") == "Bash")):
                return value
        time.sleep(.04)
    pytest.fail(f"no original {kind} gate: {store.record(A).get('waitingFor')}")


@pytest.mark.parametrize(
    ("kind", "prompt", "generated_hook_host"), (
        ("question", "ask generated question", None),
        ("permission", "request generated permission", "restricted"),
        ("elicitation", "request generated elicitation", None),
    ), indirect=["generated_hook_host"],
)
def test_direct_waited_input_settles_the_original_native_turn(
    generated_hook_host, kind, prompt,
):
    store, call, _env, _hook_log = generated_hook_host
    sent = call("send", A, prompt, "--no-wait", "--delivery-id",
                "d1-input-" + kind, "--json")
    assert sent.returncode == 0, (sent.stdout, sent.stderr)
    gate = _pending(store, kind)
    client = CcxClient()
    prior = store.record(A).get("lastSettledTurn") or {}
    if kind == "question":
        result = client.answer(A, "Yes", wait=True, timeout=12,
                               response_id="d1-response-question")
    elif kind == "permission":
        result = client.approve(A, wait=True, timeout=12,
                                response_id="d1-response-permission")
    else:
        result = client.respond_elicitation(
            A, "accept", {"title": "Text"}, wait=True, timeout=12,
            response_id="d1-response-elicitation",
        )
    assert isinstance(result, SendResult)
    assert result.worker_id == A and result.engine == "claude"
    assert result.short == A[:8]
    assert result.settled is True and result.state == "idle"
    assert result.queued is False
    assert result.turn_ref.startswith("ccx-turn:claude:v1:")
    assert result.turn_ref != prior.get("turnRef")
    assert isinstance(result.reply, str) and result.reply.strip()
    assert result.transcript is None
    assert store.record(A).get("lastSettledTurn", {}).get("turnRef") == result.turn_ref
    assert gate["id"] not in str(store.record(A).get("waitingFor") or "")


def test_waited_original_source_race_and_missing_report_after_accepted_input(
    generated_hook_host, monkeypatch,
):
    store, call, _env, _hook_log = generated_hook_host
    sent = call("send", A, "ask generated question", "--no-wait",
                "--delivery-id", "d1-race-question", "--json")
    assert sent.returncode == 0, (sent.stdout, sent.stderr)
    gate = _pending(store, "question")
    client = CcxClient()
    # Synthetic exact turn completion between host ACK and waiter registration.
    from ccx import client as client_module
    real_rpc = client_module._rpc
    active = {}

    def resolved_ack(worker, message, **kwargs):
        if message.get("op") == "status":
            row = real_rpc(worker, message, **kwargs)
            active["ref"] = row.get("activeTurnRef")
            return row
        if message.get("op") == "answer-permission":
            store.mutate_record(A, patches={
                "lastSettledTurn": {
                    "turnRef": active["ref"], "outcome": "completed",
                    "settledAtMs": 1792500000000,
                }
            })
            return {"ok": True, "request_id": gate["id"],
                    "response_id": "d1-race-id"}
        return real_rpc(worker, message, **kwargs)

    monkeypatch.setattr(client_module, "_rpc", resolved_ack)
    monkeypatch.setattr(
        ops_wait, "wait_worker",
        lambda *_a, **_kw: pytest.fail("R already settled; no second waiter needed"),
    )
    seen = []

    def report(target, *, turn_ref):
        seen.append((target, turn_ref))
        return {"turn_ref": active["ref"], "status": "completed", "text": "exact R reply"}

    monkeypatch.setattr(ops_report, "report_worker", report)
    result = client.answer(A, "Yes", request_id=gate["id"], wait=True,
                           timeout=5, response_id="d1-race-id")
    assert result.turn_ref == active["ref"] and result.reply == "exact R reply"
    assert seen == [(f"claude:{A}", active["ref"])]

    # Accepted native input with no opening-delivery report is a typed
    # POST-effect incomplete result, never a rejection or second input write.
    def report_missing(target, *, turn_ref):
        raise SessionUnavailable("no original opening delivery report")
    monkeypatch.setattr(ops_report, "report_worker", report_missing)
    with pytest.raises(TurnIncomplete, match="input accepted") as caught:
        client._input_wait_reply(A, gate["id"], "d1-race-id",
                                 active["ref"], None, None)
    assert caught.value.input_accepted is True
    assert caught.value.request_id == gate["id"]
    assert caught.value.response_id == "d1-race-id"
    assert caught.value.turn_ref == active["ref"]
    assert "never retry" in str(caught.value)


def test_definite_dead_question_fallback_and_lost_ack_never_resubmits(
    generated_hook_host, monkeypatch,
):
    from ccx import client as client_module
    from ccx.store import liveness
    from ccx.ops.worker_events import read_full_uuid_event_page

    store, call, _env, _hook_log = generated_hook_host
    sent = call("send", A, "ask generated question", "--no-wait",
                "--delivery-id", "d1-dead-question", "--json")
    assert sent.returncode == 0, (sent.stdout, sent.stderr)
    gate = _pending(store, "question")
    client = CcxClient()
    fake_sends = []

    def send_new(target, text, *, wait=None, timeout=None, caps=None,
                 delivery_id=None):
        fake_sends.append({
            "target": target, "text": text, "wait": wait,
            "timeout": timeout, "caps": caps,
        })
        return SendResult(
            short=A[:8], worker_id=A, engine="claude",
            delivery_id="original-new-message", delivery_status="accepted",
            queued=False, state="busy",
        )

    with monkeypatch.context() as patch:
        patch.setattr(client_module.host_rpc, "host_socket_live", lambda *_a: False)
        patch.setattr(liveness, "owned_process", lambda *_a: "dead")
        patch.setattr(client, "send", send_new)
        with pytest.raises(SessionUnavailable, match="fallback_to_send=True"):
            client.answer(A, "Yes", request_id=gate["id"])
        assert fake_sends == []
        # A known same-token original response receipt blocks the new message.
        patch.setattr(
            client._uuid_store, "read_receipt",
            lambda *_a, **_kw: {"state": "submitted-unknown"},
        )
        with pytest.raises(SessionUnavailable, match="already used"):
            client.answer(A, "Yes", response_id="uncertain-original",
                          fallback_to_send=True)
        assert fake_sends == []
    with monkeypatch.context() as patch:
        patch.setattr(client_module.host_rpc, "host_socket_live", lambda *_a: False)
        patch.setattr(liveness, "owned_process", lambda *_a: "dead")
        patch.setattr(client, "send", send_new)
        result = client.answer(A, "Yes", fallback_to_send=True,
                               wait=True, timeout=10)
        assert result.delivery_id == "original-new-message"
    assert len(fake_sends) == 1
    assert fake_sends[0]["target"] == A
    assert fake_sends[0]["text"] == (
        "User provided answer to the previous question:\n"
        "Question: Which option?\nAnswer: Yes"
    )
    assert fake_sends[0]["wait"] is True
    assert fake_sends[0]["caps"] is None
    assert 0 < fake_sends[0]["timeout"] <= 10

    # Lost caller ACK after the actual fake host consumed this original gate
    # is not no-effect and cannot degrade to an ordinary user message.
    real_rpc = client_module._rpc
    with monkeypatch.context() as patch:
        def lost_after_send(worker, message, **kwargs):
            if message.get("op") == "answer-permission":
                actual = real_rpc(worker, message, **kwargs)
                assert actual.get("ok") is True
                return {"ok": False,
                        "error": "unreachable:ConnectionResetError after sendall"}
            return real_rpc(worker, message, **kwargs)
        patch.setattr(client_module, "_rpc", lost_after_send)
        patch.setattr(
            client, "send",
            lambda *_a, **_kw: pytest.fail("lost input ACK cannot fallback"),
        )
        with pytest.raises(SessionUnavailable, match="unavailable"):
            client.answer(A, "Yes", request_id=gate["id"],
                          fallback_to_send=True, response_id="post-write-lost")
    events = read_full_uuid_event_page(
        store, A, tail=1000, kinds=["input.resolved"],
    )["events"]
    assert len([e for e in events if e.get("response_id") == "post-write-lost"]) == 1
    assert len(fake_sends) == 1


def test_terminal_only_permission_prompt_is_shown_then_released(generated_hook_host, tmp_path):
    store, call, _env, _hook_log = generated_hook_host
    sent = call("send", A, "terminal-only permission prompt", "--no-wait", "--json")
    assert sent.returncode == 0, (sent.stdout, sent.stderr)
    until = time.monotonic() + 12
    while time.monotonic() < until:
        rec = store.record(A)
        if (rec.get("lifecycle") or {}).get("state") == "waiting" and rec.get("waitingFor"):
            break
        time.sleep(.04)
    waiting = store.record(A).get("waitingFor")
    assert waiting["type"] == "permission" and waiting["id"] is None
    assert waiting["message"] == "Claude needs your permission to use Bash"
    peek = call("peek", A)
    assert f"in the worker terminal (ccx attach claude:{A})" in peek.stdout, peek.stdout

    (tmp_path / "terminal-prompt-gate").touch()
    until = time.monotonic() + 12
    while time.monotonic() < until:
        rec = store.record(A)
        if (rec.get("lifecycle") or {}).get("state") == "idle":
            break
        time.sleep(.04)
    rec = store.record(A)
    assert (rec.get("lifecycle") or {}).get("state") == "idle"
    assert "waitingFor" not in rec
    # host_live_transition persists the idle record before broadcast tees its
    # matching state event. Wait for that event rather than racing the append.
    until = time.monotonic() + 3
    while True:
        events = [json.loads(line) for line in events_ndjson_path(A).read_text().splitlines()]
        states = [(e.get("from"), e.get("to")) for e in events if e.get("type") == "state"]
        if states[-2:] == [("waiting", "busy"), ("busy", "idle")]:
            break
        assert time.monotonic() < until, states
        time.sleep(.02)
