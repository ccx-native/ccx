"""Private ChatGPT machine wake: original Codex fake transport only."""
from __future__ import annotations
import ccx.ops.send as ops_send

import json

import pytest

from ccx.codex import ops as codex_ops
from ccx.codex import runtime as codex_runtime
from ccx import constants
from ccx.controllers import orchestrator
from ccx.ops import common as protocol_ops
from ccx.codex.protocol import ProtocolError, ProtocolTransportError
from ccx.codex.daemon.daemon import CodexDaemon
from ccx.errors import DeliveryRejected, InvalidRequest, SessionUnavailable
from ccx.store.root import require_ccx_active
from test_converter_runtime_binding import _converted, THREAD, TAG
from test_generation_store import A, B, add_worker, put, short_root as short_root


def _payload(delivery="gptwake-1:settled"):
    return {"kind": "chatgpt-work-notification", "eventId": "gptwake-1",
            "deliveryId": delivery, "phase": "settled"}


class Native:
    def __init__(self, *, lost_ack=False, no_pages=False):
        self.calls = []
        self.lost_ack = lost_ack
        self.no_pages = no_pages
        self.items = []

    def request(self, method, params, timeout=30):
        self.calls.append((method, dict(params)))
        if method == "thread/read":
            return {"thread": {"id": THREAD, "threadSource": TAG,
                               "status": {"type": "idle"}, "turns": []}}
        if method == "thread/resume":
            return {"thread": {"id": THREAD, "status": {"type": "idle"}}}
        if method == "thread/items/list":
            if self.no_pages:
                raise ProtocolError("method not found", code=-32601, method=method)
            return {"data": list(reversed(self.items)), "nextCursor": None}
        if method == "turn/start":
            assert params["threadId"] == THREAD and params["input"] == []
            assert params["turnTrigger"] == "ccx-worker-notification"
            tool = params["toolOutput"]
            assert (tool["namespace"], tool["name"]) == (
                "chatgpt-ops", "chatgpt_work_notification",
            )
            value = json.loads(tool["output"])
            turn = "fake-machine-turn-" + str(len(self.items) + 1)
            self.items.append({"turnId": turn, "item": {
                "type": "functionCallOutput",
                "name": "chatgpt_work_notification",
                "output": json.dumps(value),
            }})
            if self.lost_ack:
                self.lost_ack = False
                raise ProtocolTransportError("reply lost after placement", method=method)
            return {"turn": {"id": turn}}
        raise AssertionError(f"unexpected native method {method}")

    def count(self, method):
        return sum(m == method for m, _ in self.calls)


@pytest.fixture
def managed(short_root, tmp_path, monkeypatch):
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    store = require_ccx_active(short_root)
    _converted(store, tmp_path, A, "codex")
    daemon = CodexDaemon()
    daemon._uuid_store = store
    daemon.client = Native()
    daemon.ready.set()
    return store, daemon


def _message(*, delivery="gptwake-1:settled", reconcile=False, **changes):
    value = {"op": "machine-wake", "short": A, "threadId": THREAD,
             "namespace": "chatgpt-ops", "name": "chatgpt_work_notification",
             "eventId": "gptwake-1", "deliveryId": delivery,
             "payload": _payload(delivery), "reconcile": reconcile}
    return {**value, **changes}


def _loaded(store, daemon):
    loaded = daemon.handle({"op": "load", "short": A, "timeout": 3})
    assert loaded["ok"] and loaded["loaded"] and loaded["state"] == "idle", loaded
    assert store.codex_source(A) == TAG and TAG != f"ccx-worker:{A}"


def _serve(monkeypatch, daemon):
    monkeypatch.setattr(
        codex_ops.runtime, "observe_worker",
        lambda *_a, **_kw: {
            "presence": "served", "daemon_served": True,
            "reply": {"ok": True, "loaded": True, "state": "idle"},
        },
    )
    def rpc(owner, message, timeout):
        assert owner == A and 0 < timeout <= 65
        return {**daemon.handle({**message, "short": owner}), "transport": "served"}
    monkeypatch.setattr(codex_ops.runtime, "rpc", rpc)


def test_managed_idle_original_tool_output_once_then_history_only(managed):
    store, daemon = managed
    _loaded(store, daemon)
    native = daemon.client
    initial = daemon.handle(_message())
    assert initial["ok"] and not initial["reconciled"] and initial["turnId"]
    assert (initial["workerId"], initial["threadId"]) == (A, THREAD)
    assert native.count("turn/start") == 1
    replay = daemon.handle(_message(reconcile=True))
    assert replay["ok"] and replay["reconciled"]
    assert replay["deliveryId"] == initial["deliveryId"]
    assert native.count("turn/start") == 1
    assert native.count("thread/fork") == native.count("thread/start") == 0


def test_busy_waiting_are_single_original_writer_without_idle_loader(managed, monkeypatch):
    store, daemon = managed
    _loaded(store, daemon)
    _serve(monkeypatch, daemon)
    monkeypatch.setattr(
        ops_send, "ensure_ready",
        lambda *_a, **_kw: pytest.fail("busy parent passed through idle-only loader"),
    )
    monkeypatch.setattr(
        codex_runtime, "observe_worker",
        lambda *_a, **_kw: pytest.fail("busy parent observed as idle"),
    )
    for state, delivery in (("busy", "gptwake-1:heartbeat:1"),
                            ("waiting", "gptwake-1:heartbeat:2")):
        store.mutate_record(A, patches={"lifecycle.state": state})
        worker = daemon.workers[A]
        worker.record["lifecycle"]["state"] = state
        result = codex_ops.deliver_machine_wake(
            store.resolve(A), _payload(delivery), namespace="chatgpt-ops",
            name="chatgpt_work_notification", reconcile=False,
            expected_thread_id=THREAD, expected_generation=store.generation,
        )
        assert result["ok"] and result["turnId"]
    assert daemon.client.count("turn/start") == 2
    assert daemon.client.count("thread/start") == 0


def test_unloaded_original_reloads_exact_thread_before_one_machine_wake(
    managed, monkeypatch,
):
    store, daemon = managed
    native = daemon.client
    _serve(monkeypatch, daemon)
    called = []

    def ready(target, *, expected_generation, timeout):
        called.append((target, expected_generation, timeout))
        loaded = daemon.handle({"op": "load", "short": A, "timeout": 3})
        assert loaded["ok"] and loaded["loaded"]
        return {"worker_id": A, "worker": f"codex:{A}", "state": "idle",
                "thread_id": THREAD}

    monkeypatch.setattr(ops_send, "ensure_ready", ready)
    result = codex_ops.deliver_machine_wake(
        store.resolve(A), _payload(), namespace="chatgpt-ops",
        name="chatgpt_work_notification", reconcile=False,
        expected_thread_id=THREAD, expected_generation=store.generation,
    )
    assert result["ok"] and result["turnId"]
    assert called and called[0][0] == f"codex:{A}"
    assert native.count("thread/read") >= 1 and native.count("thread/resume") == 1
    assert native.count("turn/start") == 1
    assert native.count("thread/start") == native.count("thread/fork") == 0


def test_lost_reply_later_definitive_refusal_cannot_clear_old_attempt(
    managed, monkeypatch,
):
    store, daemon = managed
    _loaded(store, daemon)
    native = daemon.client
    native.lost_ack = True
    _serve(monkeypatch, daemon)
    owner = store.resolve(A)
    first = codex_ops.deliver_machine_wake(
        owner, _payload(), namespace="chatgpt-ops", name="chatgpt_work_notification",
        reconcile=False,
    )
    assert not first["ok"] and not first.get("notAttempted")
    assert native.count("turn/start") == 1
    # Even a new pre-effect refusal on the same ID cannot retrospectively
    # prove the earlier ambiguous launch never happened.
    store.mutate_record(A, patches={"lifecycle.state": "dead"})
    later = codex_ops.deliver_machine_wake(
        owner, _payload(), namespace="chatgpt-ops", name="chatgpt_work_notification",
        reconcile=True,
    )
    assert not later["ok"] and not later.get("notAttempted")
    assert native.count("turn/start") == 1
    store.mutate_record(A, patches={"lifecycle.state": "busy"})
    assert daemon.handle(_message(reconcile=True))["reconciled"] is True
    assert native.count("turn/start") == 1


def test_machine_paging_unsupported_defers_without_full_read(managed):
    store, daemon = managed
    _loaded(store, daemon)
    native = daemon.client
    native.calls.clear()
    native.no_pages = True
    pending = daemon.handle(_message(reconcile=True))
    assert not pending["ok"] and native.count("turn/start") == 0
    assert native.count("thread/items/list") == 1
    assert native.count("thread/read") == 0


def test_native_turn_start_without_positive_turn_id_remains_unknown(managed, monkeypatch):
    store, daemon = managed
    _loaded(store, daemon)
    native = daemon.client
    original = native.request

    def missing_turn_id(method, params, timeout=30):
        if method == "turn/start":
            native.calls.append((method, dict(params)))
            return {"turn": {}}
        return original(method, params, timeout)

    monkeypatch.setattr(native, "request", missing_turn_id)
    refused = daemon.handle(_message())
    assert not refused["ok"] and refused["pending"]
    assert not daemon.workers[A].launched_notifications
    marker = store.record(A)["nativeTurnStart"]
    assert marker["kind"] == "machine-wake" and marker["deliveryId"] == _payload()["deliveryId"]
    retry = daemon.handle(_message(reconcile=True))
    assert not retry["ok"] and retry["pending"]
    assert native.count("turn/start") == 1


def test_first_definitive_no_effect_then_unknown_vs_reconcile_never_tagged(
    managed, monkeypatch,
):
    store, daemon = managed
    _loaded(store, daemon)
    _serve(monkeypatch, daemon)
    worker = daemon.workers[A]
    worker.record["activeCompact"] = {"operationId": "fake-compaction"}
    first = codex_ops.deliver_machine_wake(
        store.resolve(A), _payload(), namespace="chatgpt-ops",
        name="chatgpt_work_notification", reconcile=False,
    )
    assert not first["ok"] and first["definitive"] and first["notAttempted"]
    assert daemon.client.count("turn/start") == 0
    worker.record["activeCompact"] = None
    positive = codex_ops.deliver_machine_wake(
        store.resolve(A), _payload(), namespace="chatgpt-ops",
        name="chatgpt_work_notification", reconcile=False,
    )
    assert positive["ok"] and daemon.client.count("turn/start") == 1
    monkeypatch.setattr(codex_ops.runtime, "rpc", lambda *_a, **_kw: {
        "ok": False, "transport": "served", "definitive": True,
        "notAttempted": True, "error": "currently pre-effect refused",
    })
    second = codex_ops.deliver_machine_wake(
        store.resolve(A), _payload(), namespace="chatgpt-ops",
        name="chatgpt_work_notification", reconcile=True,
    )
    assert not second["ok"] and not second.get("notAttempted")


def test_invalid_machine_output_and_nonregistration_parent_refused(managed):
    store, daemon = managed
    _loaded(store, daemon)
    native = daemon.client
    bad = daemon.handle(_message(namespace="ccx"))
    assert not bad["ok"] and bad["definitive"]
    changed = daemon.handle(_message(payload={**_payload(), "deliveryId": "different"}))
    assert not changed["ok"] and changed["definitive"]
    native_ref = daemon.handle(_message(threadId="foreign-original-thread"))
    assert not native_ref["ok"] and native_ref["definitive"]
    # The historical genuine registration/settlement gate must remain strict.
    forged = daemon.handle({**_message(), "op": "parent-notify"})
    assert not forged["ok"] and "registration" in forged["error"]
    assert native.count("turn/start") == 0


def test_complete_native_census_exact_colliding_prefix_and_fail_closed(
    short_root, tmp_path, monkeypatch,
):
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    store = add_worker(short_root, A, "codex", confirmed=True)
    add_worker(short_root, B, "codex", confirmed=True)
    assert A[:8] == B[:8]
    first = store.record(A)["native"]["threadId"]
    second = store.record(B)["native"]["threadId"]
    assert first != second
    assert orchestrator._owned_codex_thread(store, first).worker_id == A
    assert orchestrator._owned_codex_thread(store, second).worker_id == B
    assert orchestrator._owned_codex_thread(store, "external-native-thread") is None
    # Test the exact native-inventory rejection with a fake capable server;
    # the on-disk original could not be armed without a loaded controller.
    monkeypatch.setattr(orchestrator, "_external_codex_unavailable", lambda _thread: None)
    owner = orchestrator.CallerContext("codex", first)
    with pytest.raises(InvalidRequest, match="already a CCX worker"):
        owner.parent_ref()
    path = store.worker_path(B, "record.json")
    row = store.record(B)
    row["native"]["threadId"] = first
    put(path, row)
    with pytest.raises(SessionUnavailable, match="Cannot identify the Codex caller"):
        orchestrator._owned_codex_thread(store, first)
    row["native"] = None
    put(path, row)
    with pytest.raises(SessionUnavailable, match="incomplete|unavailable"):
        orchestrator._owned_codex_thread(store, "external-native-thread")


def test_explicitly_stopped_machine_parent_never_auto_resumes(managed, monkeypatch):
    store, daemon = managed
    store.mutate_record(A, patches={"lifecycle.state": "dead"})
    monkeypatch.setattr(
        ops_send, "ensure_ready",
        lambda *_a, **_kw: pytest.fail("explicitly killed native thread resumed"),
    )
    monkeypatch.setattr(
        codex_ops.runtime, "rpc",
        lambda *_a, **_kw: pytest.fail("machine turn submitted for dead parent"),
    )
    first = codex_ops.deliver_machine_wake(
        store.resolve(A), _payload(), namespace="chatgpt-ops",
        name="chatgpt_work_notification", reconcile=False,
    )
    assert not first["ok"] and first["notAttempted"]
    later = codex_ops.deliver_machine_wake(
        store.resolve(A), _payload(), namespace="chatgpt-ops",
        name="chatgpt_work_notification", reconcile=True,
    )
    assert not later["ok"] and not later.get("notAttempted")
    assert daemon.client.calls == []


def test_absent_rpc_after_preflight_is_unknown_not_unattempted(managed, monkeypatch):
    store, daemon = managed
    _loaded(store, daemon)
    _serve(monkeypatch, daemon)
    # The daemon may die after receiving the request: absent is not proof.
    monkeypatch.setattr(codex_ops.runtime, "rpc", lambda *_a, **_kw: {
        "ok": False, "transport": codex_runtime.RUNTIME_ABSENT,
    })
    first = codex_ops.deliver_machine_wake(
        store.resolve(A), _payload(), namespace="chatgpt-ops",
        name="chatgpt_work_notification", reconcile=False,
    )
    assert not first["ok"] and first["pending"] and not first.get("notAttempted")
    # A busy race inside the pre-wake reload is a proven no-effect refusal.
    store.mutate_record(A, patches={"lifecycle.state": "idle-killed"})
    def busy(*_a, **_kw):
        raise DeliveryRejected("worker is busy", short=A[:8])
    monkeypatch.setattr(ops_send, "ensure_ready", busy)
    monkeypatch.setattr(codex_ops.runtime, "rpc",
                        lambda *_a, **_kw: pytest.fail("wake sent after refused reload"))
    refused = codex_ops.deliver_machine_wake(
        store.resolve(A), _payload(), namespace="chatgpt-ops",
        name="chatgpt_work_notification", reconcile=False,
    )
    assert not refused["ok"] and refused["definitive"] and refused["notAttempted"]
