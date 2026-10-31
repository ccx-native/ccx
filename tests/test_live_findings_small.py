"""Regressions for live tester's standalone notify and unstarted Codex kill."""
import ccx.notify.deliver as notify_deliver

import pytest

from ccx import constants
from ccx.notify import registry as parent_notify
from ccx.ops import terminate
from ccx.errors import CcxError
from ccx.ops.creation_steps import prepare_worker
from ccx.store.root import require_ccx_active
from ccx.constants import S
from test_generation_store import A, B, add_worker, short_root  # noqa: F401


def test_kill_unactivated_codex_marks_stopped_without_daemon(short_root, tmp_path, monkeypatch):
    store = require_ccx_active(short_root)
    target = prepare_worker(store, engine="codex", worker_id=B, cwd=str(tmp_path)).worker
    store.mutate_record(target, patches={"lifecycle.state": "unavailable"})
    from ccx.codex import runtime as codex_runtime
    monkeypatch.setattr(codex_runtime, "daemon_rpc", lambda *_a, **_k:
                        (_ for _ in ()).throw(AssertionError("must not contact daemon")))

    # A prepared start may still be dispatched by its creator: no shortcut.
    with pytest.raises(CcxError):
        terminate.confirmed_kill_uuid(store, target, timeout=1)
    assert store.record(target)["lifecycle"]["state"] == "unavailable"

    # Live case: `shutdown` refused the start locally, so Codex never saw it.
    monkeypatch.setattr(store, "native_activation", lambda _h: {"state": "local-refused"})
    result = terminate.confirmed_kill_uuid(store, target, timeout=1)
    assert result["state"] == "stopped" and result["killed"]
    assert not result["resumable"] and not result["interrupted"]
    assert store.record(target)["lifecycle"]["state"] == "dead"
    assert terminate.confirmed_kill_uuid(store, target, timeout=1)["changed"] is False


def test_standalone_notify_receipt_report_is_bounded(monkeypatch):
    # The producer may have an exact, long settled receipt, regardless of
    # engine; truncation is applied to the outgoing standalone payload.
    monkeypatch.setattr(notify_deliver, "_original_notification_registration",
                        lambda _reg: (None, {"kind": "ccx-codex"}, "claude:worker"))
    reg = {
        "eventId": "event-1", "source": "notify",
        "worker": {"engine": "claude", "workerId": "worker"},
        "operation": {"selectionMode": "last-accepted", "deliveryId": "receipt"},
    }
    settlement = {"status": "completed", "report": "x" * 13000,
                  "reportTruncated": False}
    payload = notify_deliver.payload_for_registration(reg, settlement)
    assert len(payload["report"]) == 12000
    assert payload["reportTruncated"] is True
    assert payload["selectedOriginalDeliveryId"] == "receipt"
    settlement["report"] = "short"
    payload = notify_deliver.payload_for_registration(reg, settlement)
    assert payload["report"] == "short" and not payload["reportTruncated"]


def test_public_claude_events_hide_internal_semantic_rows(short_root, monkeypatch):
    from ccx.workers import WorkerClient
    from test_worker_events import _append, _event, _journal

    add_worker(short_root, A)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    journal = _journal(short_root)
    _append(journal, _event("E1", 1, "semantic_owned"),
            _event("E1", 2, "turn_start"),
            _event("E1", 3, "semantic_settled"),
            _event("E1", 4, "turn_complete"))
    page = WorkerClient().events(A)
    assert [entry["kind"] for entry in page["events"]] == [
        "turn.started", "turn.ended",
    ]
    after = WorkerClient().events(A, after=page["next_cursor"])
    assert after["events"] == []
