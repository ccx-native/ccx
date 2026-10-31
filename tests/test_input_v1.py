"""V1 original response IDs over generation-owned fake Claude/Codex gates."""
from __future__ import annotations

import json
import time

import pytest

from ccx import constants
from ccx.codex import client as codex_client
from ccx.codex import evidence as codex_evidence
from ccx.codex import runtime as codex_runtime
from ccx.store import paths
from ccx.ops import common as protocol_ops
from ccx.errors import SessionUnavailable
from ccx.rpc.local_rpc import dispatch_full_uuid
from test_generation_store import A, B, C, short_root
from test_claude_native_cli import generated_hook_host
from test_v1_worker_protocol import request as v1_request
from test_worker_telemetry import original_sources, original_codex


def invoke(store, name, target, *, timeout_ms=5000, **params):
    envelope = v1_request(name, target, **params)
    envelope["timeout_ms"] = timeout_ms
    return dispatch_full_uuid(envelope, store)


def test_v1_input_capabilities_pure_schema_and_pre_effect_rules(short_root):
    from ccx.store.root import require_ccx_active

    store = require_ccx_active(short_root)
    caps = invoke(store, "system.info", None)
    assert caps["ok"] is True, caps
    assert caps["result"]["protocol_version"] == 2
    assert set(caps["result"]) == {"protocol_version", "ccx_version"}

    for name in ("input.answer", "input.decide", "input.respond"):
        for missing in ("target", "request_id", "response_id"):
            row = {"target": A, "request_id": "gate-1", "response_id": "caller-1"}
            row.pop(missing)
            bad = v1_request(name, **row)
            out = dispatch_full_uuid(bad, store)
            assert out["ok"] is False, (name, missing, out)
            assert out["error"]["code"] == "invalid_request"
        bad = v1_request(name, A, request_id="gate", response_id="response",
                         unknown_key=True)
        out = dispatch_full_uuid(bad, store)
        assert out["error"]["code"] == "invalid_request"
    wrong_answer = invoke(
        store, "input.answer", A, request_id="g", response_id="r",
        answers={"Which?": ""})
    assert wrong_answer["error"]["code"] == "invalid_request"
    assert not (short_root / "operations" / "answers").exists()
    assert not (short_root / "sessions").exists()


@pytest.mark.parametrize("scenario", ("question", "elicitation"))
def test_generated_claude_original_gate_v1_answer_or_respond(
    generated_hook_host, scenario, monkeypatch,
):
    from ccx.ops.worker_events import read_full_uuid_event_page
    from ccx.store.root import require_ccx_active

    store, call, env, hook_log = generated_hook_host
    # The disposable child uses a private original Claude source home.
    monkeypatch.setattr(paths, "CLAUDE_HOME", __import__("pathlib").Path(env["CLAUDE_CONFIG_DIR"]))
    monkeypatch.setattr(protocol_ops._CLIENT, "value", None)
    delivery = "v1-fake-" + scenario
    text = ("ask generated question" if scenario == "question"
            else "request generated elicitation")
    sent = call("send", A, text, "--no-wait", "--delivery-id", delivery, "--json")
    assert sent.returncode == 0, (sent.stdout, sent.stderr)
    deadline = time.monotonic() + 12
    waiting = None
    while time.monotonic() < deadline:
        waiting = store.record(A).get("waitingFor")
        if (isinstance(waiting, dict) and isinstance(waiting.get("id"), str)
                and (scenario == "question" or waiting.get("type") == "elicitation")):
            break
        time.sleep(.05)
    assert waiting and waiting.get("id"), store.record(A)

    rid = "v1-original-" + scenario
    args = ({"answers": {"Which option?": "Yes"}}
            if scenario == "question"
            else {"action": "accept", "content": {"title": "Text"}})
    name = "input.answer" if scenario == "question" else "input.respond"
    bad_args = ({"answers": {"Wrong question": "Yes"}} if scenario == "question"
                else {"action": "accept", "content": {"unknown": "bad"}})
    invalid = invoke(
        store, name, A, request_id=waiting["id"],
        response_id=rid, **bad_args)
    assert invalid["ok"] is False and invalid["error"]["code"] == "invalid_request"
    assert store.read_receipt(A, "answer", rid) is None

    first = invoke(store, name, A, request_id=waiting["id"], response_id=rid,
                   **args)
    assert first["ok"] is True, first
    assert first["result"]["worker"] == f"claude:{A}"
    value = first["result"]["input"]
    assert value["status"] == "accepted", first
    assert value["owned"] is True and value["recommended_action"] == "none"
    assert value["response_id"] == rid and value["request_id"] == waiting["id"]
    receipt = store.read_receipt(A, "answer", rid)
    assert receipt["state"] == "accepted"
    with monkeypatch.context() as patch:
        patch.setattr(
            "ccx.rpc.rpc.host_socket_live",
            lambda *_a, **_kw: False,
        )
        same = invoke(store, name, A, request_id=waiting["id"], response_id=rid,
                      **args)
    assert same["ok"] and same["result"]["input"]["status"] == "accepted"
    assert store.read_receipt(A, "answer", rid) == receipt

    events = read_full_uuid_event_page(
        store, A, tail=1000, kinds=["input.resolved"],
    )["events"]
    resolutions = [event for event in events if event.get("response_id") == rid]
    assert len(resolutions) == 1
    assert resolutions[0]["request_id"] == waiting["id"]
    # Replaying original ID never answers a successor, even if its native
    # request counter/shape is similar.
    wrong = invoke(store, name, A, request_id=waiting["id"],
                   response_id=rid, **bad_args)
    assert wrong["ok"] is False and wrong["error"]["code"] in (
        "invalid_request", "refused",
    )
    assert len([e for e in read_full_uuid_event_page(
        store, A, tail=1000, kinds=["input.resolved"],
    )["events"] if e.get("response_id") == rid]) == 1


@pytest.mark.parametrize("transport_kind", ("inconclusive", "postwrite-reset"))
def test_codex_v1_original_gate_response_id_replay_and_uncertainty(
    original_codex, monkeypatch, transport_kind,
):
    store, _cwd, wid, tid = original_codex
    monkeypatch.setattr(protocol_ops._CODEX_CLIENT, "value", None)
    gate_id = "unique-gate-a"
    native_turn = "original-codex-turn-a"
    store.mutate_record(wid, patches={
        "pendingApprovals": [{
            "gateId": gate_id, "turnId": native_turn, "requestId": 0,
            "runtimeId": "original-fake-daemon", "kind": "commandExecution",
        }],
    })
    daemon_reads = []
    monkeypatch.setattr(
        codex_runtime, "daemon_rpc",
        lambda request, **kwargs: daemon_reads.append(dict(request)) or {
            "ok": True, "transport": "served", "loaded": True,
            "workerId": wid, "state": "waiting",
        },
    )
    original_calls = []

    def fake_rpc(worker_id, request, timeout=10):
        original_calls.append((worker_id, dict(request)))
        if transport_kind == "postwrite-reset":
            raise SessionUnavailable("unreachable:ConnectionResetError after sendall")
        return {"ok": False, "transport": "inconclusive", "error": "native reply lost"}

    monkeypatch.setattr(codex_client, "rpc", fake_rpc)
    # R1: a refusal BEFORE effect cannot burn this response ID.
    invalid = invoke(
        store, "input.decide", wid, request_id=gate_id,
        response_id="same-r1-id", decision="approve", reason="unsupported Codex reason")
    assert invalid["error"]["code"] == "unsupported"
    assert store.read_receipt(wid, "answer", "same-r1-id") is None
    first = invoke(
        store, "input.decide", wid, request_id=gate_id,
        response_id="same-r1-id", decision="approve")
    assert first["ok"] is True, first
    assert first["result"]["input"]["status"] == "submitted"
    assert first["result"]["input"]["owned"] is True
    assert first["result"]["input"]["recommended_action"] == "wait"
    receipt = store.read_receipt(wid, "answer", "same-r1-id")
    assert receipt["state"] == "submitted-unknown"
    assert original_calls == [(wid, {
        "op": "approve", "gateId": gate_id, "responseId": "same-r1-id",
    })]
    assert daemon_reads
    replay = invoke(
        store, "input.decide", wid, request_id=gate_id,
        response_id="same-r1-id", decision="approve")
    assert replay["result"]["input"]["status"] == "submitted"
    assert len(original_calls) == 1 and len(daemon_reads) == 1

    changed = invoke(
        store, "input.decide", wid, request_id=gate_id,
        response_id="same-r1-id", decision="reject")
    assert changed["ok"] is False
    assert len(original_calls) == 1

    # Gate disappeared without any original resolution evidence: unknown, NOT
    # accepted or a second submission.
    store.mutate_record(wid, patches={"pendingApprovals": []})
    orphan = invoke(
        store, "input.decide", wid, request_id=gate_id,
        response_id="same-r1-id", decision="approve")
    assert orphan["result"]["input"]["status"] == "unknown"
    assert orphan["result"]["input"]["recommended_action"] == "inspect"
    assert len(original_calls) == 1

    journal = codex_evidence.EvidenceJournal(wid)
    journal.record(
        codex_evidence.INPUT_RESOLVED_METHOD,
        {"gateId": gate_id, "requestId": 0, "responseId": "same-r1-id",
         "decision": "accept", "kind": "commandExecution"},
        thread_id=tid,
    )
    journal.close()
    resolved = invoke(
        store, "input.decide", wid, request_id=gate_id,
        response_id="same-r1-id", decision="approve")
    assert resolved["result"]["input"]["status"] == "accepted"
    assert resolved["result"]["input"]["evidence"].startswith("codex-evidence:")
    assert store.read_receipt(wid, "answer", "same-r1-id")["state"] == "accepted"
    assert len(original_calls) == 1


def test_codex_v1_authenticated_ack_definitive_refusal_and_turn_fence(
    original_codex, monkeypatch,
):
    store, _cwd, wid, tid = original_codex
    monkeypatch.setattr(protocol_ops._CODEX_CLIENT, "value", None)
    gate = {"gateId": "gate-b", "requestId": 1, "turnId": "turn-b",
            "runtimeId": "fake-daemon", "kind": "commandExecution"}
    store.mutate_record(wid, patches={"pendingApprovals": [gate]})
    monkeypatch.setattr(
        codex_runtime, "daemon_rpc",
        lambda *_a, **_kw: {"ok": True, "transport": "served", "loaded": True},
    )
    from ccx.codex import client as codex_module
    sends = []
    def native(worker_id, message, timeout=10):
        sends.append(message)
        return {"ok": False, "transport": "served", "definitive": True,
                "error": "no current original approval for gateId"}
    monkeypatch.setattr(codex_module, "rpc", native)
    refusal = invoke(
        store, "input.decide", wid, request_id="gate-b",
        response_id="response-b", decision="reject")
    assert refusal["ok"] and refusal["result"]["input"]["status"] == "rejected", refusal
    assert store.read_receipt(wid, "answer", "response-b")["state"] == "rejected"
    assert sends == [{"op": "reject", "gateId": "gate-b", "responseId": "response-b"}]
    same = invoke(
        store, "input.decide", wid, request_id="gate-b",
        response_id="response-b", decision="reject")
    assert same["result"]["input"]["status"] == "rejected" and len(sends) == 1

    # Private R3 validator must not accept a mismatched original gate turn.
    def mismatched_positive(worker_id, message, timeout=10):
        sends.append(message)
        return {
            "ok": True, "transport": "served", "workerId": wid,
            "threadId": tid, "gateId": "gate-b",
            "turnId": "successor-turn-not-the-original",
            "decision": "accept",
        }
    monkeypatch.setattr(codex_module, "rpc", mismatched_positive)
    wrong_turn = invoke(
        store, "input.decide", wid, request_id="gate-b",
        response_id="wrong-turn-response", decision="approve")
    assert wrong_turn["ok"] and wrong_turn["result"]["input"]["status"] == "submitted"
    assert store.read_receipt(wid, "answer", "wrong-turn-response")["state"] == "submitted-unknown"
    replay = invoke(
        store, "input.decide", wid, request_id="gate-b",
        response_id="wrong-turn-response", decision="approve")
    assert replay["result"]["input"]["status"] == "submitted"
    assert len(sends) == 2


def test_claude_lost_ack_evidence_requires_exact_response_id(
    generated_hook_host, monkeypatch,
):
    """F1: a resolution of the same gate without OUR response_id never credits R."""
    from ccx.client import CcxClient
    from ccx.errors import SendIndeterminate

    store, call, env, _hook_log = generated_hook_host
    monkeypatch.setattr(paths, "CLAUDE_HOME", __import__("pathlib").Path(env["CLAUDE_CONFIG_DIR"]))
    real = CcxClient()
    disposable = CcxClient()
    monkeypatch.setattr(protocol_ops._CLIENT, "value", disposable)

    def question(previous=None):
        deadline = time.monotonic() + 12
        while (previous is not None and time.monotonic() < deadline
               and (store.record(A).get("lifecycle") or {}).get("state") != "idle"):
            time.sleep(.05)  # The resolved fake turn settles before the next send.
        sent = call("send", A, "ask generated question", "--no-wait",
                    "--delivery-id", f"f1-{previous or 'first'}", "--json")
        assert sent.returncode == 0, (sent.stdout, sent.stderr)
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            waiting = store.record(A).get("waitingFor")
            if (isinstance(waiting, dict) and isinstance(waiting.get("id"), str)
                    and waiting["id"] != previous):
                return waiting
            time.sleep(.05)
        raise AssertionError(store.record(A))

    def lost_ack(native_response_id):
        calls = []

        def answer(target, **kwargs):
            calls.append(kwargs["response_id"])
            real.answer(target, **{**kwargs, "response_id": native_response_id})
            raise SendIndeterminate("caller lost the original input ACK")
        return calls, answer

    args = {"answers": {"Which option?": "Yes"}}
    gate = question()
    calls, foreign = lost_ack(None)  # Another actor resolves the same gate.
    with monkeypatch.context() as patch:
        patch.setattr(disposable, "answer", foreign)
        first = invoke(store, "input.answer", A, request_id=gate["id"],
                       response_id="f1-r", **args)
        retry = invoke(store, "input.answer", A, request_id=gate["id"],
                       response_id="f1-r", **args)
    assert calls == ["f1-r"]  # One dispatch; the retry only reconciles.
    assert first["ok"] and first["result"]["input"]["status"] != "accepted", first
    assert retry["ok"] and retry["result"]["input"]["status"] == "unknown", retry
    assert store.read_receipt(A, "answer", "f1-r")["state"] == "submitted-unknown"

    successor = question(gate["id"])
    calls, landed = lost_ack("f1-exact")
    with monkeypatch.context() as patch:
        patch.setattr(disposable, "answer", landed)
        exact = invoke(store, "input.answer", A, request_id=successor["id"],
                       response_id="f1-exact", **args)
    assert calls == ["f1-exact"]
    assert exact["ok"] and exact["result"]["input"]["status"] == "accepted", exact
    assert store.read_receipt(A, "answer", "f1-exact")["state"] == "accepted"
