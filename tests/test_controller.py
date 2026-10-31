"""Durable controller runtime and CLI tests; no live model workers."""

from __future__ import annotations

import json
import importlib
import os
from pathlib import Path
import subprocess
import sys
import threading
import time

import pytest

from ccx import (
    BudgetStopped,
    CompactFailed,
    ControllerOperationIndeterminate,
    InvalidRequest,
    SessionNotFound,
    WorkerApiError,
    WorkerApiTransient,
    WorkerRateLimited,
)
from ccx.controllers import store as store
from ccx.workers import WorkerClient
from test_generation_store import A, B, add_worker, fixture_root
from ccx.constants import EXIT_API_TRANSIENT, EXIT_RATE_LIMITED

controller_runtime = importlib.import_module('ccx.controllers.controller')


REPO = Path(__file__).resolve().parent.parent


@pytest.fixture(autouse=True)
def active_controller_generation(tmp_path: Path, monkeypatch):
    # One sealed generation per test; all controller CLI children reuse it.
    root = fixture_root(tmp_path)
    monkeypatch.setattr(store.constants, "CCX_ROOT", root)
    monkeypatch.setenv("CCX_ROOT", str(root))
    return root


def _env(tmp_path: Path) -> dict[str, str]:
    env = os.environ.copy()
    env["CCX_ROOT"] = str(store.constants.CCX_ROOT)
    env["CLAUDE_CONFIG_DIR"] = str(tmp_path / "claude")
    env["PYTHONPATH"] = str(REPO)
    env.pop("CODEX_THREAD_ID", None)
    return env


def _ccx(tmp_path: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "ccx", *args], cwd=REPO, env=_env(tmp_path),
        text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=check,
        timeout=20,
    )


def _write_controller(path: Path, source: str) -> Path:
    path.write_text(source, encoding="utf-8")
    return path


def test_legacy_text_result_parser_is_not_public_api() -> None:
    import ccx

    assert not hasattr(ccx, "parse_ccx_result")
    assert not hasattr(ccx, "CcxResultError")


def test_direct_python_execution_is_refused(tmp_path: Path) -> None:
    script = _write_controller(
        tmp_path / "direct.py",
        """from ccx import controller
@controller(name="direct")
def main():
    raise RuntimeError("body must not run")
if __name__ == "__main__":
    main()
""",
    )
    proc = subprocess.run(
        [sys.executable, str(script)], cwd=REPO, env=_env(tmp_path), text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10,
    )
    assert proc.returncode != 0
    assert "ccx controller run" in proc.stderr
    assert "body must not run" not in proc.stderr


def test_run_report_task_replay_and_inventory(tmp_path: Path) -> None:
    counter = tmp_path / "counter"
    script = _write_controller(
        tmp_path / "flow.py",
        f"""from pathlib import Path
from ccx import controller, current, task, task_output
COUNTER = Path({str(counter)!r})
@task
def phase():
    value = int(COUNTER.read_text()) + 1 if COUNTER.exists() else 1
    COUNTER.write_text(str(value))
    return task_output(value={{"count": value}}, report="phase report")
@controller(name="replay-flow")
def main():
    result = phase()
    current.report(status="waiting-for-review", checkpoint="gate", summary="paused", report=result.report, data=result.value)
if __name__ == "__main__":
    main()
""",
    )
    first = _ccx(tmp_path, "controller", "run", "--json", str(script))
    payload = json.loads(first.stdout)
    assert payload["controller"] == "replay-flow"
    assert payload["lifecycle"] == "succeeded"
    assert payload["status"] == "waiting-for-review"
    assert payload["checkpoint"] == "gate"
    assert payload["report"]["text"] == "phase report"
    assert counter.read_text() == "1"

    run_id = payload["runId"]
    assert not store.launch_verdict_path(run_id).exists()
    resumed = _ccx(tmp_path, "controller", "resume", "--json", run_id[:8])
    resumed_payload = json.loads(resumed.stdout)
    assert resumed_payload["lifecycle"] == "succeeded"
    assert counter.read_text() == "1"

    listed = json.loads(_ccx(tmp_path, "controller", "list", "--json").stdout)
    assert listed["controllers"][0]["name"] == "replay-flow"
    runs = json.loads(_ccx(tmp_path, "controller", "runs", "replay-flow", "--json").stdout)
    assert runs["runs"][0]["runId"] == run_id
    report = json.loads(_ccx(tmp_path, "controller", "report", run_id[:8], "--json").stdout)
    assert report["report"]["text"] == "phase report"


def test_detached_run_and_wait(tmp_path: Path) -> None:
    script = _write_controller(
        tmp_path / "detached.py",
        """import time
from ccx import controller, current
@controller(name="detached-flow")
def main():
    print("controller output", flush=True)
    time.sleep(0.25)
    current.report(status="done", summary="finished")
if __name__ == "__main__":
    main()
""",
    )
    launched = json.loads(
        _ccx(tmp_path, "controller", "run", "--detach", "--json", str(script)).stdout
    )
    waited = json.loads(
        _ccx(tmp_path, "controller", "wait", launched["runId"][:8], "--json").stdout
    )
    assert waited["lifecycle"] == "succeeded"
    assert waited["status"] == "done"
    logs = json.loads(
        _ccx(tmp_path, "controller", "logs", launched["runId"][:8], "--json").stdout
    )
    assert "controller output" in logs["text"]


def test_default_name_and_changed_source_resume_refusal(tmp_path: Path) -> None:
    script = _write_controller(
        tmp_path / "stem_name.py",
        """from ccx import controller
@controller
def main(): pass
if __name__ == "__main__": main()
""",
    )
    payload = json.loads(_ccx(tmp_path, "controller", "run", "--json", str(script)).stdout)
    assert payload["controller"] == "stem_name"
    script.write_text(script.read_text() + "\n# changed\n")
    resumed = _ccx(
        tmp_path, "controller", "resume", payload["runId"][:8], check=False
    )
    assert resumed.returncode != 0
    assert "source changed" in resumed.stderr

def test_task_names_with_same_slug_do_not_share_replay_records(tmp_path: Path) -> None:
    script = _write_controller(
        tmp_path / "slug_identity.py",
        """from ccx import controller, current, task
@task(name="phase one")
def first(): return {"source": "first"}
@task(name="phase-one")
def second(): return {"source": "second"}
@controller(name="slug-identity")
def main(): current.report(status="done", data={"values": [first(), second()]})
if __name__ == "__main__": main()
""",
    )
    first = json.loads(_ccx(tmp_path, "controller", "run", "--json", str(script)).stdout)
    assert first["report"]["data"]["values"] == [
        {"source": "first"}, {"source": "second"},
    ]
    resumed = json.loads(
        _ccx(tmp_path, "controller", "resume", "--json", first["runId"][:8]).stdout
    )
    assert resumed["lifecycle"] == "succeeded"
    status = json.loads(
        _ccx(tmp_path, "controller", "status", first["runId"][:8], "--json").stdout
    )
    assert sorted(status["tasksDetail"]) == ["phase-one--0001", "phase-one--0002"]


def test_controller_failure_waits_for_running_tasks_before_terminal_state(tmp_path: Path) -> None:
    marker = tmp_path / "task-finished"
    script = _write_controller(
        tmp_path / "failure_wait.py",
        f"""import time
from pathlib import Path
from ccx import controller, task
@task
def slow():
    time.sleep(0.25)
    Path({str(marker)!r}).write_text("done")
@controller(name="failure-wait")
def main():
    slow.submit()
    raise RuntimeError("controller failed")
if __name__ == "__main__": main()
""",
    )
    failed = _ccx(tmp_path, "controller", "run", str(script), check=False)
    assert failed.returncode != 0
    assert marker.read_text() == "done"
    run_dir = next((store.constants.CCX_ROOT / "controllers" / "failure-wait" / "runs").iterdir())
    events = [json.loads(line) for line in (run_dir / "events.ndjson").read_text().splitlines()]
    event_types = [event["type"] for event in events]
    assert event_types.index("task-succeeded") < event_types.index("run-failed")


def test_parallel_repeated_task_invocations_have_stable_ordinals(tmp_path: Path) -> None:
    script = _write_controller(
        tmp_path / "parallel.py",
        """import time
from ccx import controller, current, task
@task
def sleeper(value):
    started = time.monotonic()
    time.sleep(0.2)
    return {"value": value, "started": started, "ended": time.monotonic()}
@controller(name="parallel-flow")
def main():
    a = sleeper.submit(1)
    b = sleeper.submit(2)
    first, second = a.result(), b.result()
    assert [first["value"], second["value"]] == [1, 2]
    # Overlapping intervals are what "parallel" means, and unlike an elapsed
    # threshold they say it on a machine of any speed: serial execution can
    # only ever produce a negative overlap.
    current.report(status="done", data={"overlap": (
        min(first["ended"], second["ended"])
        - max(first["started"], second["started"])
    )})
if __name__ == "__main__": main()
""",
    )
    payload = json.loads(_ccx(tmp_path, "controller", "run", "--json", str(script)).stdout)
    assert payload["report"]["data"]["overlap"] > 0
    status = json.loads(
        _ccx(tmp_path, "controller", "status", payload["runId"][:8], "--json").stdout
    )
    assert sorted(status["tasksDetail"]) == ["sleeper--0001", "sleeper--0002"]
    resumed = json.loads(
        _ccx(tmp_path, "controller", "resume", "--json", payload["runId"][:8]).stdout
    )
    assert resumed["lifecycle"] == "succeeded"


def test_append_input_resolves_real_recorded_worker(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(store.constants, "CCX_ROOT", tmp_path / "state")
    script = tmp_path / "flow.py"
    script.write_text("# source")
    run_id, bootstrap = store.create_bootstrap(script, tmp_path, [])
    run_dir = store.publish_bootstrap(
        bootstrap, name="input-flow", script=script, workspace=tmp_path
    )
    store.mutate_run(run_dir, lambda value: value["workers"].append({
        "worker": f"codex:{B}", "worker_id": B, "engine": "codex", "short": B[:8],
        "name": "reviewer", "cwd": str(tmp_path), "taskId": "review--0001",
    }))
    with pytest.raises(Exception, match="qualified full UUID"):
        store.append_controller_input(
            run_dir, worker_target="reviewer", comment="must not target a display name"
        )
    entry = store.append_controller_input(
        run_dir, worker_target=f"codex:{B}", comment="check the boundary again"
    )
    assert entry["worker"] == f"codex:{B}"
    assert store.read_inputs(run_dir) == [entry]
    assert run_id in str(run_dir)


def test_append_controller_note_has_no_worker(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(store.constants, "CCX_ROOT", tmp_path / "state")
    script = tmp_path / "flow.py"
    script.write_text("# source")
    _run_id, bootstrap = store.create_bootstrap(script, tmp_path, [])
    run_dir = store.publish_bootstrap(
        bootstrap, name="note-flow", script=script, workspace=tmp_path
    )
    entry = store.append_controller_input(
        run_dir, worker_target=None, comment="quota reset; continue"
    )
    assert entry["worker"] is None
    parsed = controller_runtime.ControllerInput.from_record(entry)
    assert parsed.worker is None
    assert parsed.comment == "quota reset; continue"


def test_controller_note_can_resume_without_selecting_a_worker(tmp_path: Path) -> None:
    script = _write_controller(
        tmp_path / "note_resume.py",
        """from ccx import controller, current
@controller(name="note-resume")
def main():
    if not current.inputs:
        current.report(status="waiting", checkpoint="quota")
        return
    latest = current.latest_input
    current.report(status="continued", data={"worker": latest.worker, "note": latest.comment})
if __name__ == "__main__": main()
""",
    )
    first = json.loads(_ccx(tmp_path, "controller", "run", "--json", str(script)).stdout)
    stored = json.loads(_ccx(
        tmp_path, "controller", "input", first["runId"][:8],
        "--note", "quota reset observed", "--json",
    ).stdout)
    assert stored["worker"] is None
    resumed = json.loads(_ccx(
        tmp_path, "controller", "resume", first["runId"][:8],
        "--note", "continue after reset", "--json",
    ).stdout)
    assert resumed["status"] == "continued"
    assert resumed["report"]["data"] == {
        "worker": None, "note": "continue after reset",
    }

def _runtime_fixture(tmp_path: Path, monkeypatch):
    # Do not replace ACTIVE between original and replayed worker operations.
    assert store.constants.CCX_ROOT == tmp_path / "disposable-ccx"
    script = tmp_path / "runtime.py"
    script.write_text("# source")
    _run_id, bootstrap = store.create_bootstrap(script, tmp_path, [])
    run_dir = store.publish_bootstrap(
        bootstrap, name="runtime-flow", script=script, workspace=tmp_path
    )
    return run_dir


def _bound_controller_client(fake, *, worker_id=None, engine="claude"):
    """Fake native dispatch, but real sealed ACTIVE full-UUID owner resolution."""
    if worker_id is not None:
        add_worker(store.constants.CCX_ROOT, worker_id, engine)
    active = WorkerClient()
    fake._uuid_store = active._uuid_store
    fake._uuid_owner = active._uuid_owner
    return fake


@pytest.mark.parametrize(
    ("engine", "worker", "short", "model", "effort"),
    [
        ("claude", f"claude:{A}", A[:8], "opus", "high"),
        ("codex", f"codex:{B}", B[:8], "gpt-5.6-sol", "medium"),
    ],
)
def test_worker_create_is_journaled_with_requested_routing_and_replayed(
    tmp_path: Path, monkeypatch, engine: str, worker: str, short: str,
    model: str, effort: str,
) -> None:
    run_dir = _runtime_fixture(tmp_path, monkeypatch)

    class FakeWorkers:
        calls = 0
        def create(self, **kwargs):
            self.calls += 1
            worker_id = kwargs["worker_id"]
            add_worker(store.constants.CCX_ROOT, worker_id, engine)
            return {
                "worker": f"{engine}:{worker_id}", "worker_id": worker_id,
                "engine": engine, "short": worker_id[:8],
                "state": "idle", "native_state": "idle", "report": None,
            }

    fake = _bound_controller_client(FakeWorkers())
    kwargs = {
        "engine": engine, "cwd": str(tmp_path), "name": "reviewer",
        "model": model, "effort": effort,
        "prompt": "TOP SECRET PROMPT", "wait": False,
    }
    runtime = controller_runtime._ControllerRuntime(run_dir)
    runtime_token = controller_runtime._active_runtime.set(runtime)
    task_token = controller_runtime._active_task.set(
        controller_runtime._TaskContext("review--0001", "review", tmp_path)
    )
    try:
        first = controller_runtime.observe_worker_call(
            fake, "create", FakeWorkers.create, (), kwargs
        )
    finally:
        controller_runtime._active_task.reset(task_token)
        controller_runtime._active_runtime.reset(runtime_token)
        runtime._executor.shutdown(wait=True)
    operation = run_dir / "tasks" / "review--0001" / "operations" / "0001-create.json"
    encoded = operation.read_text()
    assert "TOP SECRET PROMPT" not in encoded
    metadata = json.loads(encoded)["metadata"]
    assert metadata["promptBytes"] == len("TOP SECRET PROMPT")
    assert metadata["requestedEngine"] == engine
    assert metadata["requestedModel"] == model
    assert metadata["requestedEffort"] == effort
    assert store.read_json(store.run_record_path(run_dir))["workers"][0]["worker"] == first["worker"]
    assert first["worker_id"] == first["worker"].partition(":")[2]
    assert len(fake._uuid_store.list_workers(limit=None)) == 1

    replay_runtime = controller_runtime._ControllerRuntime(run_dir)
    runtime_token = controller_runtime._active_runtime.set(replay_runtime)
    task_token = controller_runtime._active_task.set(
        controller_runtime._TaskContext("review--0001", "review", tmp_path)
    )
    try:
        replayed = controller_runtime.observe_worker_call(
            fake, "create", FakeWorkers.create, (), kwargs
        )
    finally:
        controller_runtime._active_task.reset(task_token)
        controller_runtime._active_runtime.reset(runtime_token)
        replay_runtime._executor.shutdown(wait=True)
    assert replayed == first
    assert fake.calls == 1
def test_indeterminate_send_is_never_retried(tmp_path: Path, monkeypatch) -> None:
    run_dir = _runtime_fixture(tmp_path, monkeypatch)

    class FakeWorkers:
        calls = 0
        def send(self, worker, text, **kwargs):
            self.calls += 1
            raise RuntimeError("transport vanished after delivery")

    fake = _bound_controller_client(FakeWorkers(), worker_id=B, engine="codex")
    runtime = controller_runtime._ControllerRuntime(run_dir)
    runtime_token = controller_runtime._active_runtime.set(runtime)
    first_task = controller_runtime._TaskContext("send--0001", "send", tmp_path)
    task_token = controller_runtime._active_task.set(first_task)
    try:
        with pytest.raises(RuntimeError):
            controller_runtime.observe_worker_call(
                fake, "send", FakeWorkers.send,
                (f"codex:{B}", "DO NOT STORE THIS PROMPT"), {"wait": True},
            )
    finally:
        controller_runtime._active_task.reset(task_token)
    operation = run_dir / "tasks" / "send--0001" / "operations" / "0001-send.json"
    assert "DO NOT STORE THIS PROMPT" not in operation.read_text()

    task_token = controller_runtime._active_task.set(
        controller_runtime._TaskContext("send--0001", "send", tmp_path)
    )
    try:
        with pytest.raises(ControllerOperationIndeterminate):
            controller_runtime.observe_worker_call(
                fake, "send", FakeWorkers.send,
                (f"codex:{B}", "DO NOT STORE THIS PROMPT"), {"wait": True},
            )
    finally:
        controller_runtime._active_task.reset(task_token)
        controller_runtime._active_runtime.reset(runtime_token)
        runtime._executor.shutdown(wait=True)
    assert fake.calls == 1


def test_interrupted_budgeted_create_replays_exact_stored_uuid(tmp_path: Path, monkeypatch) -> None:
    """An interrupted original create retries with the same journaled UUID."""
    run_dir = _runtime_fixture(tmp_path, monkeypatch)

    class FakeWorkers:
        ids = []

        def create(self, **kwargs):
            self.ids.append(kwargs["worker_id"])
            if len(self.ids) == 1:
                raise RuntimeError("transport vanished")
            add_worker(store.constants.CCX_ROOT, kwargs["worker_id"], "claude")
            return {
                "worker": f"claude:{kwargs['worker_id']}",
                "worker_id": kwargs["worker_id"], "engine": "claude",
                "short": kwargs["worker_id"][:8], "state": "idle",
                "native_state": "idle", "report": None,
            }

    fake = _bound_controller_client(FakeWorkers())
    kwargs = {
        "engine": "claude", "cwd": str(tmp_path), "name": "budgeted-worker",
        "prompt": "bounded work", "wait": True,
    }
    runtime = controller_runtime._ControllerRuntime(run_dir)
    token = controller_runtime._active_runtime.set(runtime)
    try:
        for attempt in range(2):
            task_token = controller_runtime._active_task.set(
                controller_runtime._TaskContext("budget--0001", "budget", tmp_path)
            )
            try:
                if attempt == 0:
                    with pytest.raises(RuntimeError, match="transport vanished"):
                        controller_runtime.observe_worker_call(
                            fake, "create", FakeWorkers.create, (), kwargs
                        )
                else:
                    result = controller_runtime.observe_worker_call(
                        fake, "create", FakeWorkers.create, (), kwargs
                    )
            finally:
                controller_runtime._active_task.reset(task_token)
    finally:
        controller_runtime._active_runtime.reset(token)
        runtime._executor.shutdown(wait=True)
    assert len(fake.ids) == 2
    assert fake.ids[0] == fake.ids[1] == result["worker_id"]
    operation = store.read_json(
        run_dir / "tasks" / "budget--0001" / "operations" / "0001-create.json"
    )
    assert operation["metadata"]["workerId"] == result["worker_id"]
    assert operation["state"] == "accepted"
    assert len(store.read_json(store.run_record_path(run_dir))["workers"]) == 1





def test_budget_stopped_send_replays_same_phase_without_redelivery(
    tmp_path: Path, monkeypatch
) -> None:
    run_dir = _runtime_fixture(tmp_path, monkeypatch)

    class FakeWorkers:
        calls = 0

        def send(self, worker, text, **kwargs):
            self.calls += 1
            raise BudgetStopped(
                "budget stop on revive: weekly cap", short="sendstop",
                reason="weekly cap", phase="revive",
            )

    fake = _bound_controller_client(FakeWorkers(), worker_id=A, engine="claude")
    runtime = controller_runtime._ControllerRuntime(run_dir)
    runtime_token = controller_runtime._active_runtime.set(runtime)
    try:
        for _attempt in range(2):
            task_token = controller_runtime._active_task.set(
                controller_runtime._TaskContext("send--0001", "send", tmp_path)
            )
            try:
                with pytest.raises(BudgetStopped) as stopped:
                    controller_runtime.observe_worker_call(
                        fake, "send", FakeWorkers.send,
                        (f"claude:{A}", "bounded follow-up"),
                        {"budget": {"7d": 60}, "wait": True},
                    )
                assert stopped.value.phase == "revive"
                assert stopped.value.reason == "weekly cap"
            finally:
                controller_runtime._active_task.reset(task_token)
    finally:
        controller_runtime._active_runtime.reset(runtime_token)
        runtime._executor.shutdown(wait=True)
    assert fake.calls == 1
    operation = store.read_json(
        run_dir / "tasks" / "send--0001" / "operations" / "0001-send.json"
    )
    assert operation["state"] == "stopped"
    assert operation["result"] is None


def test_controller_wait_timeout_exits_like_every_other_wait(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    """A timed-out `ccx controller wait` exits 6 (EXIT_TURN_INCOMPLETE), the
    code worker waits use; 4 is reserved for "blocked on input"."""
    from ccx.cli import controller as controller_commands
    from ccx.constants import EXIT_TURN_INCOMPLETE

    monkeypatch.setattr(store, "refresh_liveness",
                        lambda run_dir: {"lifecycle": {"state": "running"}})
    monkeypatch.setattr(store, "run_summary", lambda record, run_dir: {"state": "running"})
    code = controller_commands._wait_and_follow(
        tmp_path, start_offset=0, timeout=0, as_json=True)
    assert code == EXIT_TURN_INCOMPLETE
    assert json.loads(capsys.readouterr().out)["timedOut"] is True


@pytest.mark.parametrize("caller_delivery_id", [None, "original-caller-delivery-token"])
def test_controller_native_send_hook_freezes_original_token_and_claude_budget(
    tmp_path: Path, monkeypatch, caller_delivery_id: str | None,
) -> None:
    """The real WorkerClient hook journals native delivery ID before effect."""
    from types import SimpleNamespace
    run_dir = _runtime_fixture(tmp_path, monkeypatch)
    add_worker(store.constants.CCX_ROOT, A, "claude")
    client = WorkerClient()
    native_calls = []

    def native_send(worker, text, **kwargs):
        assert worker == f"claude:{A}" and text == "original bounded prompt"
        assert kwargs["caps"] == {"7d": 60}  # Existing supported Claude budget.
        token = kwargs["delivery_id"]
        assert token and "\x00" not in token
        intent = store.read_json(run_dir / "tasks" / "send--0001" / "operations" / "0001-send.json")
        assert intent["state"] == "intent"
        assert intent["metadata"]["worker"] == worker
        assert intent["metadata"]["deliveryId"] == token
        native_calls.append((worker, token, kwargs["caps"]))
        return SimpleNamespace(delivery_id=token, delivery_status="accepted",
                               reply=None, turn_ref=None, settled=False, state="idle")

    monkeypatch.setattr(client._claude, "send", native_send)
    returned = []
    for _attempt in range(2):
        runtime = controller_runtime._ControllerRuntime(run_dir)
        token = controller_runtime._active_runtime.set(runtime)
        task_token = controller_runtime._active_task.set(
            controller_runtime._TaskContext("send--0001", "send", tmp_path)
        )
        try:
            returned.append(client.send(
                f"claude:{A}", "original bounded prompt", wait=False,
                caps={"7d": 60}, delivery_id=caller_delivery_id,
            ))
        finally:
            controller_runtime._active_task.reset(task_token)
            controller_runtime._active_runtime.reset(token)
            runtime._executor.shutdown(wait=True)
    assert returned[0] == returned[1]
    assert len(native_calls) == 1
    assert native_calls[0][1] == returned[0]["delivery_id"]
    if caller_delivery_id is not None:
        assert returned[0]["delivery_id"] == caller_delivery_id
    assert returned[0]["worker_id"] == A and returned[0]["worker"] == f"claude:{A}"


@pytest.mark.parametrize("method,options,kind", [
    ("create", {"engine": "nope", "variables": {"x": 1}}, "InvalidRequest"),
    ("send", {"worker": "codex", "caps": {"opus": 65}}, "InvalidRequest"),
    ("send", {"worker": "claude", "caps": {"bad": 10}}, "InvalidRequest"),
])
def test_controller_hook_refuses_unsupported_native_options_before_intent(
    tmp_path: Path, monkeypatch, method: str, options: dict, kind: str,
) -> None:
    from ccx import CapabilityUnsupported
    run_dir = _runtime_fixture(tmp_path, monkeypatch)
    add_worker(store.constants.CCX_ROOT, A, "claude")
    add_worker(store.constants.CCX_ROOT, B, "codex")
    client = WorkerClient()
    expected = {"InvalidRequest": InvalidRequest,
                "CapabilityUnsupported": CapabilityUnsupported}[kind]
    runtime = controller_runtime._ControllerRuntime(run_dir)
    token = controller_runtime._active_runtime.set(runtime)
    try:
        with pytest.raises(expected):
            if method == "create":
                client.create(cwd=str(tmp_path), **options)
            elif method == "send":
                engine = options["worker"]
                kwargs = {k:v for k,v in options.items() if k != "worker"}
                client.send(f"{engine}:{A if engine == 'claude' else B}", "prompt", **kwargs)
            else:
                client.stop(f"claude:{A}")
    finally:
        controller_runtime._active_runtime.reset(token)
        runtime._executor.shutdown(wait=True)
    assert not list((run_dir / "operations").glob("*.json"))
    assert not list((run_dir / "tasks").glob("*/operations/*.json"))

@pytest.mark.parametrize("activation", ["confirmed", "submitted-unknown", "foreign"])
def test_controller_partial_create_retains_typed_original_without_inventing_owner(
    tmp_path: Path, monkeypatch, activation: str,
) -> None:
    from ccx import CreatePartialFailure
    run_dir = _runtime_fixture(tmp_path, monkeypatch)
    client = WorkerClient()
    native_calls = []

    def native_create(_client, **kwargs):
        worker_id = kwargs["worker_id"]
        native_calls.append(worker_id)
        if activation == "confirmed":
            add_worker(store.constants.CCX_ROOT, worker_id, "claude")
        result_id = B if activation == "foreign" else worker_id
        info = {"worker_id": result_id, "worker": f"claude:{result_id}",
                "engine": "claude", "short": result_id[:8],
                "activation_state": activation}
        raise CreatePartialFailure(
            "original creation did not return a successful native turn",
            info=info, phase="initial-delivery" if activation == "confirmed" else "native-activation",
            delivery_id="create:initial", delivery_status="unknown",
        )

    kwargs = {"engine": "claude", "cwd": str(tmp_path), "prompt": "do one thing", "wait": False}
    for attempt in range(2 if activation != "foreign" else 1):
        runtime = controller_runtime._ControllerRuntime(run_dir)
        token = controller_runtime._active_runtime.set(runtime)
        task_token = controller_runtime._active_task.set(
            controller_runtime._TaskContext("create--0001", "create", tmp_path)
        )
        try:
            if activation == "foreign":
                with pytest.raises(ControllerOperationIndeterminate, match="foreign original"):
                    controller_runtime.observe_worker_call(client, "create", native_create, (), kwargs)
            else:
                with pytest.raises(CreatePartialFailure) as err:
                    controller_runtime.observe_worker_call(client, "create", native_create, (), kwargs)
                assert err.value.worker_id == native_calls[0]
                assert err.value.phase == ("initial-delivery" if activation == "confirmed" else "native-activation")
        finally:
            controller_runtime._active_task.reset(task_token)
            controller_runtime._active_runtime.reset(token)
            runtime._executor.shutdown(wait=True)
    assert len(native_calls) == 1
    intent = store.read_json(run_dir / "tasks" / "create--0001" / "operations" / "0001-create.json")
    assert intent["metadata"]["workerId"] == native_calls[0]
    assert intent["state"] == ("indeterminate" if activation == "foreign" else "stopped")
    recorded = store.read_json(store.run_record_path(run_dir))["workers"]
    if activation == "confirmed":
        assert len(recorded) == 1 and recorded[0]["worker"] == f"claude:{native_calls[0]}"
    else:
        assert recorded == []  # Pending or forged identity is NOT a created worker.
