"""Fake-time Claude cold-revive steer gate: no native write before live busy."""
from __future__ import annotations
import ccx.ops.send as ops_send

import time as real_time
import pytest

from ccx.claude import delivery as claude_delivery
from ccx.ops import common as protocol_ops
from ccx.rpc import rpc
from ccx.errors import DeliveryRejected, SessionUnavailable
from test_generation_store import A, short_root  # noqa: F401
from test_notify_claude import owners  # noqa: F401


def _ready_stub(owners, monkeypatch):
    _, store = owners
    handle = "claude:" + A
    native_id = store.record(handle)["native"]["sessionId"]
    proof = {"sessionId": native_id, "nativeSource": "claude-session:" + native_id}
    store.mutate_record(handle, patches={
        "lifecycle.state": "idle", "lifecycle.nativeStartProof": proof,
        "stats.turns": 1, "lastSettledAt": 0,
    })
    claude_delivery.prepare(handle, "revived-turn", "new native turn",
                            mode="new_turn_only")
    claude_delivery.mark_owned(handle, "revived-turn", route="new_turn")
    return handle, store, proof


def test_recent_revive_steer_waits_four_fake_seconds_then_old_rc5(owners, monkeypatch):
    handle, store, proof = _ready_stub(owners, monkeypatch)
    clock = [0.0]
    class FakeTime:
        time = staticmethod(real_time.time)
        @staticmethod
        def monotonic():
            return clock[0]
        @staticmethod
        def sleep(seconds):
            clock[0] += seconds

    monkeypatch.setattr(ops_send, "time", FakeTime)
    opcodes = []
    def answered(_worker, request, timeout=.5):
        opcodes.append(request["op"])
        if request["op"] == "startup-status":
            return {"ok": True, "workerId": A, "engine": "claude",
                    "generation": store.generation, "nativeStartProof": proof}
        assert request["op"] == "status"
        return {"ok": True, "state": "idle"}
    monkeypatch.setattr(rpc, "_rpc", answered)
    with pytest.raises(DeliveryRejected, match="currently busy"):
        ops_send.send_turn(handle, "steer text", mode="steer_only",
                               delivery_id="steer-before-busy")
    assert 4.0 <= clock[0] <= 4.15
    assert "deliver" not in opcodes
    assert claude_delivery.read_receipt(handle, "steer-before-busy") is None


@pytest.mark.parametrize("raised", [OSError("disconnected"), ValueError("bad host response")])
def test_steer_status_transport_exceptions_are_typed(owners, monkeypatch, raised):
    handle, store, proof = _ready_stub(owners, monkeypatch)
    def answered(_worker, request, timeout=.5):
        if request["op"] == "startup-status":
            return {"ok": True, "workerId": A, "engine": "claude",
                    "generation": store.generation, "nativeStartProof": proof}
        raise raised
    monkeypatch.setattr(rpc, "_rpc", answered)
    with pytest.raises(SessionUnavailable, match="status unavailable"):
        ops_send.send_turn(handle, "steer", mode="steer_only",
                               delivery_id="steer-raw-error")
    assert claude_delivery.read_receipt(handle, "steer-raw-error") is None


def test_steer_journal_busy_is_typed_before_native_delivery(owners, monkeypatch):
    handle, store, proof = _ready_stub(owners, monkeypatch)
    opcodes = []
    def answered(_worker, request, timeout=.5):
        opcodes.append(request["op"])
        if request["op"] == "startup-status":
            return {"ok": True, "workerId": A, "engine": "claude",
                    "generation": store.generation, "nativeStartProof": proof}
        return {"ok": True, "state": "busy"}
    monkeypatch.setattr(rpc, "_rpc", answered)
    def locked(*_args, **_kw):
        raise claude_delivery.ClaudeDeliveryBusy("fake journal lock busy")
    monkeypatch.setattr(claude_delivery, "prepare", locked)
    with pytest.raises(SessionUnavailable, match="receipt lock contested"):
        ops_send.send_turn(handle, "steer", mode="steer_only",
                               delivery_id="contended-steer")
    assert opcodes == ["startup-status", "status"]
