"""Controller continuation with real local records and a fake parent transport."""

from __future__ import annotations
import ccx.notify.daemon as notify_daemon
import ccx.notify.deliver as notify_deliver
import ccx.notify.settle as notify_settle

import importlib
import json
from pathlib import Path
from uuid import uuid4

import pytest

from ccx import constants
from ccx.cli import controller as C
from ccx.controllers import store as S
from ccx.codex import settle as CS
from ccx.controllers import notify as CN
from ccx.notify import daemon as notifyd
from ccx.notify import registry as N
from ccx.errors import InvalidRequest, SessionUnavailable
from ccx.controllers.orchestrator import CallerContext
from ccx.cli.parser import build_parser
from test_generation_store import fixture_root

runtime = importlib.import_module('ccx.controllers.controller')


@pytest.fixture
def local_notifications(monkeypatch, tmp_path):
    root = fixture_root(tmp_path)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    monkeypatch.setenv("CCX_ROOT", str(root))
    monkeypatch.setattr(notify_daemon, "ensure_notify_daemon", lambda: None)
    # Fake private Codex-native return-address, not a short worker identity.
    monkeypatch.setattr(C, "require_notify_parent", lambda: {
        "kind": "codex-native-thread", "threadId": "test-parent"})
    monkeypatch.setattr(C, "current_caller", lambda: CallerContext("codex", "test-parent"))
    from ccx.controllers import orchestrator
    monkeypatch.setattr(orchestrator, '_external_codex_unavailable', lambda _thread: None)
    monkeypatch.setattr(C, "codex_blocking_override_enabled", lambda: False)

    def launch(**kwargs):
        records = N.iter_registrations()
        reg = max(records, key=lambda r: r["operation"]["attempt"])
        assert reg["worker"]["workerId"] == kwargs["run_id"]
        assert notify_settle.settlement_for_registration(reg) is None
        # Run ordinary controller Python, without spawning any model workers.
        runtime._host_execute(**{k: v for k, v in kwargs.items() if k != "startup_timeout"})
        return S.read_launch_verdict(kwargs["run_id"])

    monkeypatch.setattr(C, "_spawn_controller", launch)
    return tmp_path


def command(*args):
    parsed = build_parser().parse_args(["controller", *map(str, args)])
    parsed.func(parsed)


def write_flow(path: Path, body: str) -> Path:
    script = path / "flow.py"
    script.write_text("from ccx import controller, current\n"
                      "@controller(name='flow')\ndef main():\n" +
                      "\n".join("    " + line for line in body.splitlines()) +
                      "\nmain()\n")
    return script


def latest_registration(attempt):
    return next(r for r in N.iter_registrations() if r["operation"]["attempt"] == attempt)


def test_run_resume_fence_reports_and_checkpoint_lifecycle(local_notifications, capsys):
    script = write_flow(local_notifications, """if not current.inputs:
    current.report(status='waiting-for-review', checkpoint='direction', report='FIRST')
    return
current.report(status='completed', report='SECOND')""")
    command("run", "--notify-parent", "--json", script)
    accepted = json.loads(capsys.readouterr().out)
    assert accepted["notify"]["resume_automatically"] is False
    assert accepted["notify"]["worker"] == f"controller:{accepted['runId']}"
    assert accepted["notify"]["polling_required"] is False
    first = latest_registration(1)
    one = notify_settle.settlement_for_registration(first)
    assert (one["status"], one["controllerStatus"], one["checkpoint"]) == (
        "succeeded", "waiting-for-review", "direction")

    command("resume", "--notify-parent", "--note", "continue", "flow", "--json")
    second = latest_registration(2)
    assert notify_settle.settlement_for_registration(second)["report"] == "SECOND"
    assert notify_settle.settlement_for_registration(first) == one
    assert one["report"] == "FIRST"
    assert Path(one["reportPath"]).read_text() == "FIRST"
    saved = S.read_json(S.run_record_path(S.resolve_run("flow")))
    assert [a["outcome"]["report"]["path"] for a in saved["attempts"]] == [
        one["reportPath"], notify_settle.settlement_for_registration(second)["reportPath"]]


def test_resume_without_new_report_does_not_reuse_checkpoint(local_notifications):
    script = write_flow(local_notifications, """if not current.inputs:
    current.report(status='waiting-for-review', checkpoint='old', report='OLD')""")
    command("run", "--notify-parent", script)
    command("resume", "--notify-parent", "--note", "go", "flow")
    settled = notify_settle.settlement_for_registration(latest_registration(2))
    assert settled["report"] is None
    assert settled["checkpoint"] is None
    assert settled["controllerStatus"] == "completed"


def test_notify_adopts_settled_attempt_without_executing(local_notifications, monkeypatch):
    script = write_flow(local_notifications, "current.report(status='completed', report='DONE')")
    command("run", "--notify-parent", script)
    monkeypatch.setattr(C, "_spawn_controller", lambda **_: pytest.fail("notify must not dispatch"))
    command("notify", "flow", "--json")
    assert len(N.iter_registrations()) == 2
    assert {notify_settle.settlement_for_registration(r)["report"] for r in N.iter_registrations()} == {"DONE"}


def test_delivery_retry_keeps_exact_attempt_report(local_notifications, monkeypatch):
    script = write_flow(local_notifications, "current.report(status='completed', report=str(len(current.inputs)))")
    command("run", "--notify-parent", script)
    first = latest_registration(1)
    delivered = []
    def transport(reg, payload):
        delivered.append(payload)
        if len(delivered) == 1:
            raise notify_deliver.DeliveryDeferred("parent temporarily unavailable")
        return {"ok": True}
    monkeypatch.setattr(notifyd, "deliver_to_parent", transport)
    notifyd.process_registration(first)
    command("resume", "--notify-parent", "--note", "next", "flow")
    N.mutate_registration(first["eventId"], lambda r: r.update(nextAttemptAtMs=0))
    notifyd.process_registration(N.read_registration(first["eventId"]))
    assert delivered[0] == delivered[1]
    assert delivered[1]["report"] == "0"
    assert N.read_registration(first["eventId"])["state"] == "delivered"


def test_ready_controller_final_payload_survives_time_and_sibling_change(
    local_notifications, monkeypatch
):
    script = write_flow(local_notifications, "current.report(status='completed', report='FINAL')")
    command("run", "--notify-parent", script)
    first = latest_registration(1)
    # Freeze a settled registration without sending it to a real Codex parent.
    with monkeypatch.context() as patch:
        patch.setattr(notifyd, "_deliver_final", lambda *_: None)
        notifyd.process_registration(first)
    ready = N.read_registration(first["eventId"])
    assert ready["state"] == "ready" and ready["payload"]["report"] == "FINAL"
    original = ready["payload"]
    assert ready["settlement"]["activity"] == original["activity"]
    assert ready["settlement"]["outstanding"] == original["outstanding"] == []

    # The stored elapsed time and sibling snapshot are now historical, while
    # ordinary progress and outstanding registrations remain live.
    later = S.now_ms() + 60_000
    monkeypatch.setattr(S, "now_ms", lambda: later)
    monkeypatch.setattr(N, "now_ms", lambda: later)
    command("resume", "--notify-parent", "--note", "later", "flow")
    sibling = latest_registration(2)
    assert sibling["eventId"] != first["eventId"]
    assert notify_deliver.outstanding_for_parent(ready)[0]["eventId"] == sibling["eventId"]
    assert CN.progress(ready)["elapsedSeconds"] >= original["activity"]["elapsedSeconds"] + 60

    fresh = N.read_registration(first["eventId"])
    assert fresh["payload"] == original
    assert notify_deliver.payload_for_registration(fresh, fresh["settlement"]) == original
    native_calls = []

    def fake_native_delivery(reg, payload, *, reconcile):
        native_calls.append((reg["eventId"], payload["deliveryId"], reconcile))
        return {"ok": True, "deliveryId": payload["deliveryId"]}

    monkeypatch.setattr(notify_deliver, "_external_codex_deliver", fake_native_delivery)
    with pytest.raises(notify_deliver.DeliveryDeferred):
        notify_deliver.deliver_to_parent(fresh, {**original, "report": "tampered"})
    assert native_calls == []  # Full-payload validation must still run.
    assert notify_deliver.deliver_to_parent(fresh, original)["ok"] is True
    assert native_calls == [(first["eventId"], original["deliveryId"], False)]


@pytest.mark.parametrize("args", [
    ["run", "flow.py"], ["resume", "flow"], ["wait", "flow"], ["logs", "flow", "--follow"],
])
def test_codex_blocking_rejected_before_effects(local_notifications, monkeypatch, args):
    monkeypatch.setattr(S, "create_bootstrap", lambda *a, **k: pytest.fail("must reject before create"))
    monkeypatch.setattr(S, "resolve_run", lambda *a: pytest.fail("must reject before inspecting/mutating"))
    with pytest.raises(SystemExit):
        command(*args)


@pytest.mark.parametrize("failure", [InvalidRequest("unsupported parent"), SessionUnavailable("no supervisor")])
def test_notify_failure_precedes_dispatch(local_notifications, monkeypatch, failure):
    def fail():
        raise failure
    monkeypatch.setattr(C, "require_notify_parent", fail)
    monkeypatch.setattr(S, "create_bootstrap", lambda *a, **k: pytest.fail("must not create"))
    with pytest.raises(SystemExit):
        command("run", "--notify-parent", local_notifications / "flow.py")


def test_completion_options_are_exclusive_and_do_not_consume_script_args():
    parser = build_parser()
    for verb, target in [("run", "flow.py"), ("resume", "flow")]:
        with pytest.raises(SystemExit):
            parser.parse_args(["controller", verb, "--detach", "--notify-parent", target])
    parsed = parser.parse_args(["controller", "run", "--notify-parent", "flow.py", "--", "--detach"])
    assert parsed.notify_parent and not parsed.detach
    assert C._script_args(parsed.script_args) == ["--detach"]


def test_liveness_rechecks_identity_before_marking_dead(local_notifications, monkeypatch):
    script = write_flow(local_notifications, "pass")
    run_id, bootstrap = S.create_bootstrap(script, local_notifications, [])
    run_dir = S.publish_bootstrap(bootstrap, name="flow", script=script, workspace=local_notifications)
    S.mutate_run(run_dir, lambda r: r["lifecycle"].update(
        state="running", pid=111, pidStart="old-start", since=1))
    def stale_probe(pid, start):
        assert (pid, start) == (111, "old-start")
        S.mutate_run(run_dir, lambda r: r["lifecycle"].update(
            state="running", pid=222, pidStart="new-start", since=2))
        return "dead"
    monkeypatch.setattr(S, "process_presence", stale_probe)
    result = S.refresh_liveness(run_dir)
    assert result["lifecycle"] == {
        "state": "running", "pid": 222, "pidStart": "new-start", "since": 2,
    }


def test_resume_registration_token_cannot_adopt_later_attempt(
    local_notifications, monkeypatch
):
    script = write_flow(local_notifications, "pass")
    run_id, bootstrap = S.create_bootstrap(
        script, local_notifications, []
    )
    run_dir = S.publish_bootstrap(
        bootstrap,
        name="flow",
        script=script,
        workspace=local_notifications,
    )
    # Prospective resume arming requires an original terminal first attempt.
    def settle_original(value):
        value["lifecycle"].update(state="succeeded", pid=None)
        value["attempts"][0]["lifecycle"] = "succeeded"

    S.mutate_run(run_dir, settle_original)
    record = S.read_json(S.run_record_path(run_dir))
    parent = {"kind": "codex-native-thread", "threadId": "test-parent"}

    stale = C._arm_notification(
        parent,
        record,
        attempt=2,
        source="controller.resume",
        attempt_token=str(uuid4()),
    )
    deadline = stale["operation"]["startupDeadlineMs"]
    monkeypatch.setattr(S, "now_ms", lambda: deadline + 1)

    first = notify_settle.settlement_for_registration(stale)
    assert first["status"] == "unavailable"
    assert first["error"]["type"] == "ControllerAttemptUnavailable"

    def later_attempt(value):
        value.setdefault("attempts", []).append({
            "number": 2,
            "attemptToken": str(uuid4()),
            "startedAt": deadline + 2,
            "completedAt": deadline + 3,
            "lifecycle": "succeeded",
            "reportStartIndex": len(value.get("reports") or []),
            "argv": [],
            "startForm": S.ATTEMPT_START_RESUME,
            "startInputId": None,
            "outcome": {
                "status": "completed",
                "checkpoint": None,
                "summary": None,
                "report": None,
                "error": None,
            },
        })
        value["lifecycle"] = {
            "state": "succeeded",
            "since": deadline + 3,
            "pid": None,
        }

    S.mutate_run(run_dir, later_attempt)

    second = notify_settle.settlement_for_registration(stale)
    assert second["status"] == "unavailable"
    assert second["error"]["type"] == "ControllerAttemptUnavailable"


def test_controller_resume_registration_and_attempt_share_token(
    local_notifications, monkeypatch, capsys
):
    script = write_flow(
        local_notifications,
        "current.report(status='completed', report='FIRST')",
    )
    command("run", "--notify-parent", script)
    capsys.readouterr()

    # Avoid actually executing the resumed controller; inspect the durable
    # registration + attempt immediately after preparation.
    captured = {}

    def launch(**kwargs):
        run_dir = Path(kwargs["run_dir"])
        record = S.read_json(S.run_record_path(run_dir))
        captured["record"] = record
        return {
            "ok": True,
            "runId": kwargs["run_id"],
            "runDir": str(run_dir),
            "controller": record.get("controller"),
        }

    monkeypatch.setattr(C, "_spawn_controller", launch)
    command(
        "resume",
        "--notify-parent",
        "--note",
        "continue",
        "flow",
        "--json",
    )

    reg = latest_registration(2)
    attempt = captured["record"]["attempts"][-1]
    assert reg["operation"]["attemptToken"] == attempt["attemptToken"]
