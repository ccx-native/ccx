"""Full-UUID V1 notification.inspect reads only original local v3 registrations."""
from __future__ import annotations
import ccx.notify.deliver as notify_deliver

import json
from uuid import UUID, uuid4

import pytest

from ccx.codex import delivery as codex_delivery
from ccx.rpc import local_rpc
from ccx.notify import daemon as notifyd
from ccx.notify import registry as parent_notify
from ccx.notify.paths import daemon_generation_path
from ccx.notify.client import ParentNotificationClient
from test_generation_store import A, B, put, short_root as short_root
from test_notify_registration import originals as originals  # real fake-native fixture


def _request(event_id=None, **extra):
    params = {**extra}
    if event_id is not None:
        params["event_id"] = event_id
    return {"protocol_version": 2, "request_id": "v1-notification-read",
            "op": "notification.inspect", "params": params}


def _read(event_id=None, **extra):
    # This is the public, exact full-UUID Protocol V1 path, NOT _OPERATIONS.
    return local_rpc.dispatch(_request(event_id, **extra))


def _error(reply, code):
    assert reply["protocol_version"] == 2
    assert reply["request_id"] == "v1-notification-read"
    assert reply["ok"] is False and reply["error"]["code"] == code
    assert set(reply["error"]) == {"code", "message", "details"}
    assert "Traceback" not in json.dumps(reply)


def _arm_original(store, *, metadata=None, token="inspect-original-child"):
    """Use the pre-existing owned Codex receipt and registration producer."""
    codex_delivery.prepare(
        f"codex:{B}", token, "fake original child content",
        mode="new_turn_only", caller_metadata=metadata,
    )
    parent = {"kind": "ccx-codex", "workerId": A,
              "threadId": store.record(A)["native"]["threadId"],
              "generation": store.generation}
    registration = parent_notify.create_registration(
        parent=parent, worker_engine="codex", worker_id=B,
        worker_name="original child", operation={"kind": "codex-turn", "deliveryId": token},
        source="send",
    )
    return registration, parent


def test_rich_v1_original_registration_armed_then_fake_delivered_no_wake(
    originals, monkeypatch,
):
    store, trace, _cwd = originals
    caller_metadata = {"client": "fake-integration", "correlation": {"attempt": 9}}
    registration, parent = _arm_original(store, metadata=caller_metadata)
    event_id = registration["eventId"]
    assert UUID(event_id).version == 5  # semantic source key, NOT worker UUIDv4
    path = parent_notify.registration_path(event_id)
    pre_read_bytes = path.read_bytes()
    before_parent = store.record(A)
    before_child = store.record(B)
    before_native_trace = trace.read_bytes()
    monkeypatch.setattr(notify_deliver, "deliver_to_parent", lambda *_a, **_k:
                        pytest.fail("a V1 inspection must never wake a parent"))
    monkeypatch.setattr(notifyd, "process_registration", lambda *_a, **_k:
                        pytest.fail("a V1 inspection must not run a notifier"))

    # Existing public V1 one-shot serializer and direct dispatch share a backend.
    response = json.loads(local_rpc.respond(lambda: _request(event_id)))
    assert response["ok"] is True, response
    assert response["request_id"] == "v1-notification-read"
    armed = response["result"]["notification"]
    assert armed["event_id"] == event_id and armed["state"] == "armed"
    assert armed["parent"] == f"codex:{A}" and armed["parent_kind"] == "ccx-codex"
    assert armed["worker"] == f"codex:{B}" and armed["worker_id"] == B
    assert armed["short"] == B[:8] and armed["source"] == "send"
    assert armed["delivery_id"] == "inspect-original-child"
    assert armed["caller_metadata"] == caller_metadata
    assert armed["operation"] == {
        "kind": "codex-turn", "delivery_id": "inspect-original-child",
        "turn_id": None, "resume_waiting_request_id": None,
        "caller_metadata": caller_metadata,
    }
    assert "heartbeat" not in armed
    assert not any(key.startswith("heartbeat_") for key in armed)
    assert armed["attempts"] == 0
    assert armed["settlement"] is None and armed["payload"] is None
    assert armed["delivery"] is None
    assert path.read_bytes() == pre_read_bytes  # V1 read did not mutate receipt

    fake_settlement = {"status": "waiting", "waitingFor": {"id": "q1", "tool": "AskUserQuestion"}}
    fake_payload = {"phase": "settled", "status": "waiting", "report": "original-only"}
    fake_delivery = {"ok": True, "turnId": "fake-parent-wake-evidence"}

    def fake_completed(value):
        # Test only: do not drive notifyd, native sends or parent wake.
        value.update(state="delivered", attempts=2, lastAttemptAtMs=1230,
                     lastError="earlier fake retry", deliveredAtMs=1250,
                     cancelReason=None, settlement=fake_settlement,
                     payload=fake_payload, delivery=fake_delivery)

    parent_notify.mutate_registration(event_id, fake_completed)
    frozen_complete = path.read_bytes()
    result = _read(event_id)
    assert result["ok"] is True, result
    rich = result["result"]["notification"]
    assert rich["source"] == "send" and rich["state"] == "delivered"
    assert rich["attempts"] == 2 and rich["last_attempt_at_ms"] == 1230
    assert rich["last_error"] == "earlier fake retry" and rich["delivered_at_ms"] == 1250
    assert rich["cancel_reason"] is None
    assert rich["caller_metadata"] == caller_metadata
    assert rich["operation"] == armed["operation"]
    assert rich["settlement"] == fake_settlement
    assert rich["settlement"]["waitingFor"]["id"] == "q1"
    assert rich["payload"] == fake_payload and rich["delivery"] == fake_delivery
    assert "heartbeat" not in rich
    assert not any(key.startswith("heartbeat_") for key in rich)
    # The separate convenience client intentionally does NOT have nested V1 fields.
    flat = ParentNotificationClient().get(event_id)
    assert flat["event_id"] == event_id
    assert "operation" not in flat and "heartbeat" not in flat
    assert not any(key.startswith("heartbeat_") for key in flat)
    listed = ParentNotificationClient().list(include_terminal=True)["notifications"]
    assert any(row["event_id"] == event_id for row in listed)
    assert not any(key.startswith("heartbeat_") for row in listed for key in row)
    assert path.read_bytes() == frozen_complete

    # The independently minted NOTIFY observation is a canonical event UUIDv4.
    standalone = parent_notify.create_registration(
        parent=parent, worker_engine="codex", worker_id=B, worker_name="original child",
        operation={"kind": "codex-stable", "selectionMode": "status-only",
                   "stableState": "idle"}, source="notify",
    )
    assert UUID(standalone["eventId"]).version == 4
    direct = _read(standalone["eventId"])
    assert direct["ok"] is True and direct["result"]["notification"]["source"] == "notify"
    assert direct["result"]["notification"]["operation"]["kind"] == "codex-stable"
    assert store.record(A) == before_parent and store.record(B) == before_child
    assert trace.read_bytes() == before_native_trace  # no new native turn/provider RPC
    assert not daemon_generation_path().exists()


def test_v1_event_domain_absent_corrupt_generation_and_source_proof_fail_closed(
    originals, monkeypatch,
):
    store, trace, _cwd = originals
    registration, _parent = _arm_original(store)
    event_id = registration["eventId"]
    path = parent_notify.registration_path(event_id)
    original = parent_notify.read_registration(event_id)
    assert original is not None
    before_parent = store.record(A)
    before_child = store.record(B)
    before_trace = trace.read_bytes()
    monkeypatch.setattr(notify_deliver, "deliver_to_parent", lambda *_a, **_k:
                        pytest.fail("no notification read may send a wake"))

    caps = local_rpc.dispatch({"protocol_version": 2, "request_id": "caps", "op":
                               "system.info", "params": {}})
    assert caps["ok"] is True
    assert set(caps["result"]) == {"protocol_version", "ccx_version"}
    assert caps["result"]["protocol_version"] == 2

    for bad_id in ("../escape", B[:8], event_id.upper(), "", "event-1", 22):
        _error(_read(bad_id), "invalid_request")
    _error(_read(), "invalid_request")
    _error(_read(event_id, target=f"codex:{B}"), "invalid_request")
    _error(_read(event_id, no_such_param=1), "invalid_request")
    nonexistent = str(uuid4())
    assert not parent_notify.registration_path(nonexistent).exists()
    _error(_read(nonexistent), "not_found")
    assert not parent_notify.registration_path(nonexistent).exists()
    # Generation fences precede any read of an otherwise valid registration.
    _error(local_rpc.dispatch(_request(event_id), expected_generation="g-" + "e" * 32),
           "unavailable")

    # The existing local registration reader returns None for damaged/non-object rows;
    # one existence check must distinguish them from an actually absent event.
    path.write_text("{bad json", encoding="utf-8")
    _error(_read(event_id), "unavailable")
    path.write_text('[]', encoding="utf-8")
    _error(_read(event_id), "unavailable")

    for edit in (
        lambda row: row.update(eventId=str(uuid4())),
        lambda row: row.update(generation="g-wrong-original-generation"),
        lambda row: row["parent"].update(threadId="not-the-original-parent-thread"),
        lambda row: row["worker"].update(generation="g-wrong-child-generation"),
    ):
        candidate = json.loads(json.dumps(original))
        edit(candidate)
        put(path, candidate)
        _error(_read(event_id), "unavailable")

    put(path, original)
    correct = _read(event_id)
    assert correct["ok"] is True and correct["result"]["notification"]["event_id"] == event_id
    assert store.record(A) == before_parent and store.record(B) == before_child
    assert trace.read_bytes() == before_trace
    assert not daemon_generation_path().exists()
