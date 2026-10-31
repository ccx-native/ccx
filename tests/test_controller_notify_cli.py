"""Public controller parent notification: exact durable arm before fake effects.

Three bounded source/public/effect groups. Neither native providers nor Python
controller scripts are launched; the accepted v3 registry is exercised for real.
"""
from __future__ import annotations
import ccx.notify.daemon as notify_daemon
import ccx.notify.settle as notify_settle

import json
from pathlib import Path

import pytest

from ccx.cli import main as cli
from ccx import constants
from ccx.cli import controller as commands
from ccx.controllers import store as runs
from ccx.notify import registry as notify
from ccx.errors import ControllerError, SessionUnavailable
from ccx.notify.client import ParentNotificationClient
from ccx.controllers.orchestrator import CallerContext
from test_generation_store import A, B, add_worker, fixture_root


RETURN_THREAD = "original-interactive-codex-controller-return-thread"


@pytest.fixture
def active(tmp_path, monkeypatch):
    root = fixture_root(tmp_path)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    monkeypatch.setenv("CCX_ROOT", str(root))
    for name in ("CCX_SELF_SHORT", "CCX_SELF_WORKER_ID", "CCX_SELF_ENGINE",
                 "CLAUDE_CODE_SESSION_ID", "CODEX_THREAD_ID"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("CODEX_THREAD_ID", RETURN_THREAD)
    from ccx.controllers import orchestrator
    monkeypatch.setattr(orchestrator, '_external_codex_unavailable', lambda _thread: None)
    # The accepted registry runs in this disposable ACTIVE root. Prevent any
    # actual notifyd start; explicit spies below prove preflight ordering.
    monkeypatch.setattr(notify_daemon, "prepare_notify_parent", lambda parent: parent)
    monkeypatch.setattr(notify_daemon, "ensure_notify_daemon", lambda **_kw: None)
    return root


def _existing_run(work: Path, *, name="flow", workers=(), report_text=None):
    script = work / "controller_script.py"
    script.write_text("# original Python source: never executed by tests\n")
    run_id, bootstrap = runs.create_bootstrap(script, work, [])
    directory = runs.publish_bootstrap(bootstrap, name=name, script=script,
                                       workspace=work)
    if report_text is not None:
        (directory / "reports" / "old-report.md").write_text(report_text)

    def finish(record):
        if report_text is not None:
            record["reports"].append({
                "status": "awaiting-review", "checkpoint": "handoff",
                "summary": "Original final",
                "path": str(directory / "reports" / "old-report.md")})
        record["lifecycle"].update(state="succeeded", pid=None)
        record["attempts"][-1]["lifecycle"] = "succeeded"
        for engine, worker_id in workers:
            record["workers"].append({
                "worker": f"{engine}:{worker_id}", "worker_id": worker_id,
                "engine": engine, "name": "reviewer", "short": worker_id[:8],
                "taskId": "review-original", "cwd": str(work),
            })

    runs.mutate_run(directory, finish)
    return script, run_id, directory


def _events():
    return notify.iter_registrations(strict=True, include_terminal=True)


# G1: real caller verification and preflight before bootstrap; registry before
# synthetic host and truthful durable event on an uncertain startup failure.
def test_run_parent_preflight_precedes_bootstrap_event_precedes_fake_launch(
    active, tmp_path, monkeypatch, capsys,
):
    marker = tmp_path / "must-never-run-python"
    script = tmp_path / "notify_flow.py"
    script.write_text(f"from pathlib import Path\nPath({str(marker)!r}).touch()\n")
    calls = []
    original_bootstrap = runs.create_bootstrap

    def preflight(parent):
        assert parent == {"kind": "codex-native-thread", "threadId": RETURN_THREAD}
        calls.append("original-parent-preflight")
        return parent

    def bootstrap(*args, **kwargs):
        assert calls == ["original-parent-preflight"]
        calls.append("bootstrap")
        return original_bootstrap(*args, **kwargs)

    def fake_launch(**kwargs):
        assert calls == ["original-parent-preflight", "bootstrap"]
        assert kwargs["resume"] is False and not marker.exists()
        bootstrap_dir = kwargs["run_dir"]
        record = runs.read_json(runs.run_record_path(bootstrap_dir))
        registrations = _events()
        assert len(registrations) == 1
        event = registrations[0]
        assert event["worker"]["workerId"] == kwargs["run_id"] == record["runId"]
        assert event["parent"]["threadId"] == RETURN_THREAD
        assert event["source"] == "controller.run"
        assert event["operation"]["attempt"] == 1
        assert event["operation"]["attemptToken"] == record["attempts"][0]["attemptToken"]
        assert set(event["operation"]) == {
            "kind", "attempt", "attemptToken", "startupDeadlineMs"}
        assert record["sourceSha256"] == runs.source_sha256(script)
        assert record["operationJournalLayout"] == 3
        assert kwargs["script_args"] == ["--notify-parent"]
        calls.append("fake-host")
        run_dir = runs.publish_bootstrap(
            bootstrap_dir, name="flow", script=script, workspace=tmp_path)
        return {"ok": True, "runId": record["runId"],
                "runDir": str(run_dir), "controller": "flow"}

    monkeypatch.setattr(notify_daemon, "prepare_notify_parent", preflight)
    monkeypatch.setattr(runs, "create_bootstrap", bootstrap)
    monkeypatch.setattr(commands, "_spawn_controller", fake_launch)
    cli._main(["controller", "run", "--notify-parent", "--name", "flow",
               "--json", str(script), "--", "--notify-parent"])
    result = json.loads(capsys.readouterr().out)
    assert calls == ["original-parent-preflight", "bootstrap", "fake-host"]
    assert result["ok"] is True and result["notify"]["state"] == "armed"
    assert result["notify"]["event_id"] == _events()[0]["eventId"]
    assert result["notify"]["notifier_available"] is False
    assert result["notify"]["polling_required"] is False
    assert result["notify"]["resume_automatically"] is False
    assert not marker.exists()

    # An uncertain launcher result is not a controller terminal verdict. A
    # durable event is still shown with its original ID on the failure exit.
    calls.clear()
    monkeypatch.setattr(commands, "_spawn_controller", lambda **kwargs: {
        "ok": False, "runId": kwargs["run_id"],
        "error": "fake launcher did not confirm startup"})
    with pytest.raises(SystemExit) as failed:
        cli._main(["controller", "run", "--notify-parent", "--name", "flow",
                   "--json", str(script)])
    assert failed.value.code == 1
    out = capsys.readouterr()
    partial = json.loads(out.out)
    assert "fake launcher did not confirm startup" in out.err
    assert partial["ok"] is False and partial["notify"]["state"] == "armed"
    assert any(row["eventId"] == partial["notify"]["event_id"] for row in _events())
    assert len(_events()) == 2 and not marker.exists()

    # Unsupported parent and supervisor preflight both fail before even the
    # bootstrap record; neither silently detaches or manufactures a new event.
    monkeypatch.delenv("CODEX_THREAD_ID")
    calls.clear()
    with pytest.raises(SystemExit) as unsupported:
        cli._main(["controller", "run", "--notify-parent", str(script)])
    assert unsupported.value.code == 1
    assert "parent notification needs" in capsys.readouterr().err
    assert calls == [] and len(_events()) == 2
    monkeypatch.setenv("CODEX_THREAD_ID", RETURN_THREAD)

    def unavailable(_parent):
        raise SessionUnavailable("fake notify supervisor unavailable")

    monkeypatch.setattr(notify_daemon, "prepare_notify_parent", unavailable)
    with pytest.raises(SystemExit) as unready:
        cli._main(["controller", "run", "--notify-parent", str(script)])
    assert unready.value.code == 3
    assert "fake notify supervisor unavailable" in capsys.readouterr().err
    assert calls == [] and len(_events()) == 2


# G2: global unique worker resolution before parent preflight, N+1 minted
# under resume.lock, exactly the same token before input, attempt or host.
def test_resume_exact_token_prearmed_and_ambiguous_or_failed_launch_retained(
    active, tmp_path, monkeypatch, capsys,
):
    add_worker(active, A, "claude", terminal=True)
    add_worker(active, B, "codex", terminal=True)
    script, run_id, run_dir = _existing_run(
        tmp_path, workers=(("claude", A),))
    before = runs.read_json(runs.run_record_path(run_dir))
    calls = []

    def preflight(parent):
        calls.append("preflight")
        assert parent["threadId"] == RETURN_THREAD
        return parent

    original_append = runs.append_controller_input

    def append_input(directory, *, worker_target, comment):
        assert calls == ["preflight"]
        assert directory == run_dir
        assert worker_target == f"claude:{A}" and comment == "continue original"
        current = runs.read_json(runs.run_record_path(directory))
        assert len(current["attempts"]) == 1
        reg = _events()[0]
        assert reg["source"] == "controller.resume"
        assert reg["operation"]["attempt"] == 2
        assert reg["operation"]["attemptToken"] != current["attempts"][0]["attemptToken"]
        calls.append("input")
        return original_append(directory, worker_target=worker_target, comment=comment)

    def fake_launch(**kwargs):
        assert calls == ["preflight", "input"]
        reg = _events()[0]
        new = runs.read_json(runs.run_record_path(run_dir))
        assert kwargs["resume"] is True and kwargs["run_id"] == run_id
        assert len(new["attempts"]) == 2
        assert new["attempts"][1]["attemptToken"] == reg["operation"]["attemptToken"]
        assert new["attempts"][1]["reportStartIndex"] == len(before["reports"])
        assert new["attempts"][1]["startForm"] == "resume-worker-comment"
        assert new["sourceSha256"] == before["sourceSha256"]
        assert runs.read_inputs(run_dir)[0]["worker"] == f"claude:{A}"
        calls.append("host")
        return {"ok": True, "runId": run_id, "runDir": str(run_dir),
                "controller": "flow"}

    monkeypatch.setattr(notify_daemon, "prepare_notify_parent", preflight)
    monkeypatch.setattr(runs, "append_controller_input", append_input)
    monkeypatch.setattr(commands, "_spawn_controller", fake_launch)

    snapshot = runs.run_record_path(run_dir).read_bytes()
    with pytest.raises(SystemExit) as ambiguous:
        cli._main(["controller", "resume", "--notify-parent", "--worker", "aaaaaaaa",
                   "--comment", "continue original", run_id[:8]])
    assert ambiguous.value.code == 1
    assert "ambiguous managed worker prefix" in capsys.readouterr().err
    assert calls == [] and _events() == []
    assert runs.run_record_path(run_dir).read_bytes() == snapshot

    cli._main(["controller", "resume", "--notify-parent", "--worker", A,
               "--comment", "continue original", "--json", run_id[:8]])
    out = json.loads(capsys.readouterr().out)
    assert calls == ["preflight", "input", "host"]
    assert out["notify"]["run_id"] == run_id and out["notify"]["attempt"] == 2

    # A changed source is a definite pre-arm refusal, even with a valid parent.
    bad_script, bad_id, bad_dir = _existing_run(tmp_path, name="source-check")
    bad_script.write_text("# changed since durable run\n")
    calls.clear()
    before_bad = runs.run_record_path(bad_dir).read_bytes()
    events_before = len(_events())
    with pytest.raises(SystemExit) as changed:
        cli._main(["controller", "resume", "--notify-parent", bad_id[:8]])
    assert changed.value.code == 1
    assert "source changed" in capsys.readouterr().err
    assert calls == ["preflight"]
    assert runs.run_record_path(bad_dir).read_bytes() == before_bad
    assert len(_events()) == events_before

    # After a durable N+1 arm, definite input refusal cancels THAT event and
    # leaves the run's next attempt uncreated, without dispatch.
    _, cancelled_id, cancelled_dir = _existing_run(tmp_path, name="cancel-before-input",
                                                   workers=(("claude", A),))
    monkeypatch.setattr(runs, "append_controller_input", lambda *_a, **_kw: (
        (_ for _ in ()).throw(ControllerError("fake definite input refusal"))))
    before_cancel = runs.run_record_path(cancelled_dir).read_bytes()
    count_before = len(_events())
    with pytest.raises(SystemExit) as refused:
        cli._main(["controller", "resume", "--notify-parent", "--worker", A,
                   "--comment", "continue original", cancelled_id[:8]])
    assert refused.value.code == 1
    assert "cancelled before dispatch" in capsys.readouterr().err
    assert len(_events()) == count_before + 1
    cancelled = next(row for row in _events() if row["worker"]["workerId"] == cancelled_id)
    assert cancelled["state"] == "cancelled"
    assert runs.run_record_path(cancelled_dir).read_bytes() == before_cancel

    # An unknown launcher ACK is *not* a definite no-dispatch. Keep a single
    # original arm with the N+1 token; do not silently start user code twice.
    _, uncertain_id, uncertain_dir = _existing_run(tmp_path, name="uncertain-launch")
    dispatches = []

    def lost_ack(**kwargs):
        dispatches.append(kwargs)
        candidate = next(row for row in _events()
                         if row["worker"]["workerId"] == uncertain_id)
        assert candidate["operation"]["attemptToken"] == runs.read_json(
            runs.run_record_path(uncertain_dir))["attempts"][1]["attemptToken"]
        return {"ok": False, "runId": uncertain_id,
                "error": "fake uncertain launcher ACK"}

    monkeypatch.setattr(commands, "_spawn_controller", lost_ack)
    with pytest.raises(SystemExit) as incomplete:
        cli._main(["controller", "resume", "--notify-parent", "--json", uncertain_id[:8]])
    assert incomplete.value.code == 1
    outcome = capsys.readouterr()
    durable = json.loads(outcome.out)
    assert "fake uncertain launcher ACK" in outcome.err
    assert durable["notify"]["state"] == "armed"
    assert len(dispatches) == 1
    assert notify.read_registration(durable["notify"]["event_id"])["state"] == "armed"


# G3: read-only standalone observation of exact terminal attempt, cancellation,
# truthful public help and the unchanged Codex unnotified blocking fence.
def test_standalone_notify_terminal_attempt_without_dispatch_or_input(
    active, tmp_path, monkeypatch, capsys,
):
    script, run_id, directory = _existing_run(
        tmp_path, name="read-only-notify", report_text="ORIGINAL controller completion\n")
    report = directory / "reports" / "old-report.md"
    before_record = runs.run_record_path(directory).read_bytes()
    before_events = (directory / "events.ndjson").read_bytes()
    assert runs.read_inputs(directory) == []
    monkeypatch.setattr(commands, "_spawn_controller", lambda **_kw: pytest.fail(
        "standalone notification must never dispatch a controller"))
    monkeypatch.setattr(runs, "append_controller_input", lambda *_a, **_kw: pytest.fail(
        "standalone notification must never append controller input"))

    cli._main(["controller", "notify", "read-only-notify", "--json"])
    first = json.loads(capsys.readouterr().out)
    cli._main(["controller", "notify", run_id[:8], "--json"])
    second = json.loads(capsys.readouterr().out)
    assert first["notify"]["event_id"] != second["notify"]["event_id"]
    assert first["notify"]["operation_kind"] == "controller-attempt"
    assert first["notify"]["run_id"] == second["notify"]["run_id"] == run_id
    assert first["notify"]["attempt"] == second["notify"]["attempt"] == 1
    assert first["notify"]["state"] == "armed"
    assert all(row["source"] == "controller.notify" for row in _events())
    assert runs.run_record_path(directory).read_bytes() == before_record
    assert (directory / "events.ndjson").read_bytes() == before_events
    assert runs.read_inputs(directory) == []
    settlement = notify_settle.settlement_for_registration(_events()[0])
    assert settlement["status"] == "succeeded"
    assert settlement["report"] == "ORIGINAL controller completion\n"
    assert settlement["checkpoint"] == "handoff"
    assert settlement["reportPath"] == str(report)
    notify.cancel_registration(first["notify"]["event_id"], "no wake needed")
    assert ParentNotificationClient().get(first["notify"]["event_id"])["state"] == "cancelled"
    assert runs.run_record_path(directory).read_bytes() == before_record

    # No false controller --settle and no claim that native placement completes
    # the parent model, even in the CLI's public help.
    for cmd in ("run", "resume", "notify"):
        with pytest.raises(SystemExit) as ok:
            cli._main(["controller", cmd, "--help"])
        assert ok.value.code == 0
        help_text = capsys.readouterr().out
        assert "Codex parent" in help_text
        assert "currently unavailable" not in help_text
        assert "--settle" not in help_text

    monkeypatch.setattr(commands, "current_caller", lambda: CallerContext(
        "codex", native_id=RETURN_THREAD))
    with pytest.raises(SystemExit) as blocked:
        cli._main(["controller", "run", str(script)])
    assert blocked.value.code == 1
    guidance = capsys.readouterr().err
    assert "--detach" in guidance and "status TARGET" in guidance
    assert "report TARGET" in guidance and "logs TARGET" in guidance
    assert len(_events()) == 2
