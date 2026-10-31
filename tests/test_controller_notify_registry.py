"""Controller-run notification v3: three focused fake public/source/effect groups.

All provider and parent machine paths use accepted fake Codex fixtures; no
controller source, native CLI, or notification daemon is launched by these tests.
"""
from __future__ import annotations
import ccx.notify.daemon as notify_daemon
import ccx.notify.deliver as notify_deliver
import ccx.notify.select as notify_select
import ccx.notify.settle as notify_settle

import shlex
from pathlib import Path
from uuid import uuid4

import pytest

from ccx.codex import control as codex_control
from ccx.codex import runtime as codex_runtime
from ccx import constants
from ccx.controllers import store as runs
from ccx.controllers import notify as controller_notify
from ccx.notify import daemon as notifyd
from ccx.controllers import orchestrator
from ccx.notify import registry as notify
from ccx.errors import InvalidRequest, SessionUnavailable
from ccx.notify.client import ParentNotificationClient
from test_generation_store import A, short_root as short_root
from test_notify_registration import originals as originals
from test_notify_ext import FakeControl, RETURN_THREAD


@pytest.fixture
def fixture(originals, monkeypatch):
    store, trace, work = originals
    # Durable registrations remain test-local; never spawn a real notifyd.
    monkeypatch.setattr(notify_daemon, "ensure_notify_daemon", lambda **_kw: None)
    return store, trace, work


def _run(work: Path, *, published: bool = True):
    script = work / "flow.py"
    script.write_text("# synthetic controller source, not executed\n")
    run_id, bootstrap = runs.create_bootstrap(script, work, [])
    directory = (runs.publish_bootstrap(bootstrap, name="flow", script=script,
                                        workspace=work) if published else bootstrap)
    return run_id, directory


def _parent(store, monkeypatch):
    for key in ("CCX_SELF_SHORT", "CLAUDE_CODE_SESSION_ID"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("CCX_SELF_ENGINE", "codex")
    monkeypatch.setenv("CCX_SELF_WORKER_ID", A)
    monkeypatch.setenv("CODEX_THREAD_ID", store.record(A)["native"]["threadId"])
    return orchestrator.require_notify_parent()


def _native_parent(monkeypatch):
    for key in ("CCX_SELF_SHORT", "CCX_SELF_ENGINE", "CCX_SELF_WORKER_ID",
                "CLAUDE_CODE_SESSION_ID"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("CODEX_THREAD_ID", RETURN_THREAD)
    monkeypatch.setattr(orchestrator, '_external_codex_unavailable', lambda _thread: None)
    return orchestrator.require_notify_parent()


def _operation(directory: Path, *, attempt: int = 1, token: str | None = None):
    record = runs.read_json(runs.run_record_path(directory))
    return {"kind": "controller-attempt", "attempt": attempt,
            "attemptToken": token or record["attempts"][attempt - 1]["attemptToken"],
            "startupDeadlineMs": runs.now_ms() + 45_000}


def _arm(run_id, directory, parent, *, source="controller.notify", operation=None):
    return notify.create_registration(
        parent=parent, worker_engine="controller", worker_id=run_id,
        worker_name="flow", source=source, operation=operation or _operation(directory))


def _finish(directory: Path, *, text: str, checkpoint: str = "review"):
    original_report = directory / "reports" / "original attempt report.md"
    original_report.write_text(text)

    def complete(value):
        value["reports"].append({
            "status": "awaiting-review", "checkpoint": checkpoint,
            "summary": "Original attempt", "path": str(original_report)})
        value["attempts"][-1].update(lifecycle="succeeded", completedAt=runs.now_ms())
        value["lifecycle"].update(state="succeeded", pid=None)

    runs.mutate_run(directory, complete)
    return original_report


# G1: a controller run is a typed independent subject, never a native worker.
def test_controller_subject_original_parent_and_public_inventory(fixture, monkeypatch):
    store, trace, work = fixture
    run_id, directory = _run(work, published=False)
    parent = _parent(store, monkeypatch)
    before_native = trace.read_text()
    from ccx.store.store import GenerationStore
    original_resolve = GenerationStore.resolve
    seen = []

    def resolve(self, target):
        seen.append(target)
        assert not str(target).startswith("controller:")
        return original_resolve(self, target)

    monkeypatch.setattr(GenerationStore, "resolve", resolve)
    first = _arm(run_id, directory, parent, source="controller.run")
    second = _arm(run_id, directory, parent, source="controller.notify")
    assert first["eventId"] != second["eventId"]
    assert first["worker"] == {
        "engine": "controller", "workerId": run_id,
        "generation": store.generation, "name": "flow"}
    assert set(first["operation"]) == {
        "kind", "attempt", "attemptToken", "startupDeadlineMs"}
    assert first["settlementMode"] is None and first["registrationKeySha256"] is None
    assert notify_select._original_notification_registration(first)[2] == "controller:" + run_id
    assert "codex:" + A in seen and trace.read_text() == before_native
    assert {row["eventId"] for row in notify.iter_registrations(strict=True)} == {
        first["eventId"], second["eventId"]}
    assert notify_deliver.outstanding_for_parent(first)[0]["worker"] == "controller:" + run_id
    public = ParentNotificationClient().get(first["eventId"])
    assert public["worker"] == "controller:" + run_id
    assert public["operation_kind"] == public["operation_type"] == "controller-attempt"
    assert public["run_id"] == run_id and public["attempt"] == 1
    assert public["polling_required"] is False and public["settle"] is False
    assert ParentNotificationClient().list(include_terminal=True)["count"] == 2

    native = _native_parent(monkeypatch)
    third = _arm(run_id, directory, native)
    assert third["parent"] == {"kind": "codex-native-thread", "threadId": RETURN_THREAD}
    assert notify_select._original_notification_registration(third)[1] == native
    assert len(notify.iter_registrations(strict=True)) == 3
    with pytest.raises((InvalidRequest, SessionUnavailable)):
        _arm(run_id, directory, {"kind": "codex-native-thread", "threadId": "bad\x00thread"})
    with pytest.raises((InvalidRequest, SessionUnavailable)):
        _arm(run_id, directory, {**parent, "threadId": "different-parent-thread"})
    with pytest.raises((InvalidRequest, SessionUnavailable)):
        _arm(str(uuid4()), directory, native)
    with pytest.raises((InvalidRequest, SessionUnavailable)):
        _arm(run_id, directory, native, operation=_operation(directory, token=str(uuid4())))
    with pytest.raises((InvalidRequest, SessionUnavailable)):
        notify.create_registration(parent=native, worker_engine="controller", worker_id=run_id,
                                   worker_name="flow", source="controller.notify",
                                   settle=True, operation=_operation(directory))
    assert len(notify.iter_registrations(strict=True)) == 3
    assert trace.read_text() == before_native

    def failed_before_publication(value):
        value["lifecycle"].update(state="failed", pid=None)
        value["attempts"][0].update(lifecycle="failed", completedAt=runs.now_ms())
        value["error"] = {"type": "ControllerError", "message": "before decorator"}

    runs.mutate_run(directory, failed_before_publication)
    failure = notify_settle.settlement_for_registration(first)
    assert failure["status"] == "failed"
    assert failure["error"]["message"] == "before decorator"
    assert failure["report"] is None

    def unavailable_supervisor(**_kwargs):
        raise SessionUnavailable("fake supervisor offline")

    monkeypatch.setattr(notify_daemon, "ensure_notify_daemon", unavailable_supervisor)
    durable = _arm(run_id, directory, parent)
    assert notify.read_registration(durable["eventId"])["state"] == "armed"
    public = notify.public_registration(durable)
    assert public["notifier_available"] is False
    assert public["polling_required"] is False


# G2: once armed, missing/wrong token must remain VISIBLE and settle unavailable.
def test_original_frozen_report_and_later_mismatched_token_remain_visible(
    fixture, monkeypatch,
):
    store, _trace, work = fixture
    run_id, directory = _run(work)
    parent = _parent(store, monkeypatch)
    old = _arm(run_id, directory, parent)
    report = _finish(directory, text="FIRST REPORT ONLY", checkpoint="first-checkpoint")
    original = notify_settle.settlement_for_registration(old)
    assert original["controllerStatus"] == "awaiting-review"
    assert original["checkpoint"] == "first-checkpoint"
    assert original["report"] == "FIRST REPORT ONLY"

    new_token = str(uuid4())
    waiting = _arm(run_id, directory, parent, source="controller.resume",
                   operation=_operation(directory, attempt=2, token=new_token))
    assert notify_settle.settlement_for_registration(waiting) is None
    assert controller_notify.progress(waiting)["workerState"] == "starting"
    assert waiting in notify.iter_registrations(strict=True)

    def newer(value):
        value["attempts"].append({
            "number": 2, "attemptToken": str(uuid4()),
            "startedAt": runs.now_ms(), "reportStartIndex": len(value["reports"]),
            "lifecycle": "succeeded", "completedAt": runs.now_ms()})
        value["reports"].append({"status": "later", "checkpoint": "NOT ORIGINAL",
                                 "path": str(directory / "reports" / "later.md")})
        value["lifecycle"].update(state="succeeded", pid=None)

    (directory / "reports" / "later.md").write_text("LATER REPORT")
    runs.mutate_run(directory, newer)
    assert notify_settle.settlement_for_registration(old) == original
    monkeypatch.setattr(notify_settle, "NOTIFY_REPORT_MAX_CHARS", 5)
    frozen = notify_settle.settlement_for_registration(old)
    payload = notifyd._mark_ready(old["eventId"], frozen)["payload"]
    assert payload["report"] != "LATER REPORT" and payload["reportTruncated"]
    assert payload["reportPath"] == str(report)
    assert payload["reportCommand"] == "cat " + shlex.quote(str(report))
    assert payload["runId"] == run_id and payload["attempt"] == 1
    assert payload["checkpoint"] == "first-checkpoint"

    monkeypatch.setattr(runs, "now_ms", lambda: waiting["operation"]["startupDeadlineMs"] + 1)
    assert waiting in notify.iter_registrations(strict=True)
    assert ParentNotificationClient().get(waiting["eventId"])["state"] == "armed"
    assert any(row["eventId"] == waiting["eventId"] for row in
               notify_deliver.outstanding_for_parent(old))
    failed = notify_settle.settlement_for_registration(waiting)
    assert failed["status"] == "unavailable" and failed["report"] is None
    assert failed["checkpoint"] is None
    assert failed["error"]["type"] == "ControllerAttemptUnavailable"
    # The controller's durable directory vanishing after arm is a settlement
    # issue, not a reason for strict inventory to silently lose this row.
    directory.rename(directory.with_name(run_id + "-unloaded"))
    assert waiting in notify.iter_registrations(strict=True)
    assert notify_settle.settlement_for_registration(waiting)["status"] == "unavailable"


# G3: quiet armed registration, one final native delivery, cancel and retry.
def test_final_only_cancel_and_ambiguous_native_wake(fixture, monkeypatch):
    store, _trace, work = fixture
    run_id, directory = _run(work)
    parent = _native_parent(monkeypatch)
    native = FakeControl(lost_ack=True, history_absent=True)
    monkeypatch.setattr(codex_control, "CodexControlClient", lambda: native)
    arm = _arm(run_id, directory, parent)

    notifyd.process_registration(arm)
    pending = notify.read_registration(arm["eventId"])
    assert pending["state"] == "armed" and pending.get("payload") is None
    assert pending.get("deliveryAttemptedAtMs") is None
    assert native.methods("turn/start") == []

    _finish(directory, text="SETTLED")
    notifyd.process_registration(pending)
    ready = notify.read_registration(arm["eventId"])
    assert ready["state"] == "ready" and ready["deliveryAttemptedAtMs"]
    assert ready["payload"]["phase"] == "settled"
    assert ready["payload"]["report"] == "SETTLED"
    assert ready["payload"]["runId"] == run_id
    assert ready["payload"]["deliveryId"] == arm["eventId"] + ":settled"
    assert len(native.placed) == 1  # lost native ACK after one final placement

    notify.mutate_registration(arm["eventId"], lambda row: row.update(nextAttemptAtMs=0))
    notifyd.process_registration(notify.read_registration(arm["eventId"]))
    assert len(native.placed) == 1  # history absent cannot authorize resend
    assert notify.read_registration(arm["eventId"])["state"] == "ready"
    native.history_absent = False
    notify.mutate_registration(arm["eventId"], lambda row: row.update(nextAttemptAtMs=0))
    notifyd.process_registration(notify.read_registration(arm["eventId"]))
    delivered = notify.read_registration(arm["eventId"])
    assert delivered["state"] == "delivered" and delivered["delivery"]["reconciled"] is True
    assert len(native.placed) == 1
    notifyd.process_registration(delivered)
    assert len(native.placed) == 1

    cancelled = _arm(run_id, directory, parent)
    notify.cancel_registration(cancelled["eventId"], "no more parent wake")
    notifyd.process_registration(notify.read_registration(cancelled["eventId"]))
    assert ParentNotificationClient().get(cancelled["eventId"])["state"] == "cancelled"
    assert len(native.placed) == 1

    # Managed parent explicitly dead: same final event stays ready, no native
    # dispatch until another action makes that exact parent live again.
    managed = _parent(store, monkeypatch)
    killed = _arm(run_id, directory, managed)
    store.mutate_record("codex:" + A, patches={"lifecycle.state": "dead"})
    notifyd.process_registration(notify.read_registration(killed["eventId"]))
    deferred = notify.read_registration(killed["eventId"])
    assert deferred["state"] == "ready" and deferred["payload"]["deliveryId"]
    assert deferred["deliveryAttemptedAtMs"] is None
    assert len(native.placed) == 1

    # The accepted managed delivery route owns original-thread verification;
    # this fake acknowledges ONLY the same run/event/delivery after revival.
    store.mutate_record("codex:" + A, patches={"lifecycle.state": "busy"})
    expected = deferred["payload"]
    native_parent = store.record(A)["native"]["threadId"]
    calls = []

    def machine(message, *, timeout):
        calls.append(message)
        assert message["eventId"] == killed["eventId"]
        assert message["deliveryId"] == expected["deliveryId"]
        assert message["short"] == A and message["reconcile"] is False
        return {"transport": "served", "ok": True,
                "rpcGeneration": constants.CODEXD_RPC_GENERATION,
                "workerId": A, "threadId": native_parent,
                "eventId": killed["eventId"], "deliveryId": expected["deliveryId"],
                "turnId": "same-original-parent-wake"}

    monkeypatch.setattr(codex_runtime, "daemon_rpc", machine)
    notify.mutate_registration(killed["eventId"], lambda row: row.update(nextAttemptAtMs=0))
    notifyd.process_registration(notify.read_registration(killed["eventId"]))
    assert len(calls) == 1
    assert notify.read_registration(killed["eventId"])["state"] == "delivered"
