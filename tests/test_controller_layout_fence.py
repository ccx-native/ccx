"""Offline journal-layout compatibility gate and lost-kill-reply regressions."""
import importlib
import json
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from ccx import ControllerOperationIndeterminate
from ccx import SessionUnavailable
from ccx.controllers import store as store
from ccx.cli import controller as commands
from ccx.errors import ControllerError
from ccx.workers import WorkerClient
from test_generation_store import A, add_worker, fixture_root

C = importlib.import_module('ccx.controllers.controller')


def _run(tmp_path, monkeypatch):
    root = fixture_root(tmp_path)
    monkeypatch.setattr(store.constants, "CCX_ROOT", root)
    add_worker(root, A, "claude")
    script = tmp_path / "old_flow.py"
    script.write_text(
        "from pathlib import Path\n"
        f"Path({str(tmp_path / 'user-code-executed')!r}).write_text('unsafe')\n"
        "from ccx import controller\n"
        "@controller(name='old')\n"
        "def flow(): pass\n"
        "flow()\n"
    )
    run_id, bootstrap = store.create_bootstrap(script, tmp_path, [])
    run_dir = store.publish_bootstrap(bootstrap, name="old", script=script, workspace=tmp_path)
    return script, run_id, run_dir


def _legacy(run_dir, marker, method):
    path = store.run_record_path(run_dir)
    record = store.read_json(path)
    if marker is None:
        record.pop("operationJournalLayout")
    else:
        record["operationJournalLayout"] = marker
    store.write_json_atomic(path, record)
    operations = run_dir / "tasks" / "unfinished--0001" / "operations"
    operations.mkdir(parents=True, exist_ok=True)
    (operations / "0001-send.json").write_text('{"state":"accepted"}\n')
    if method == "interrupt":
        (operations / "0001-stop.json").write_text('{"state":"accepted"}\n')
    return {p.name: p.read_bytes() for p in operations.iterdir()}


@pytest.mark.parametrize("marker", [None, 2])
def test_old_journal_rejected_before_public_resume_runtime_and_user_source(
    tmp_path, monkeypatch, capsys, marker,
):
    script, run_id, run_dir = _run(tmp_path, monkeypatch)
    original_ops = _legacy(run_dir, marker, "kill")
    original_run = store.read_json(store.run_record_path(run_dir))
    monkeypatch.setattr(commands, "_prepare_completion", lambda *a, **k: None)
    monkeypatch.setattr(commands, "_spawn_controller", lambda **k: pytest.fail("launched old code"))
    args = SimpleNamespace(
        target=run_id, note=None, worker=None, comment=None,
        script_args=[], json=True, detach=True,
    )

    with pytest.raises(SystemExit) as exc:
        commands.cmd_resume(args)
    assert exc.value.code != 0
    msg = capsys.readouterr().err
    assert "operation journal layout" in msg
    assert "start a new run" in msg
    assert store.read_json(store.run_record_path(run_dir)) == original_run

    # Direct paths are independently guarded, not dependent on CLI checks.
    with pytest.raises(ControllerError, match="operation journal layout"):
        C._ControllerRuntime(run_dir)
    launch = C._LaunchContext(run_id=run_id, run_dir=run_dir, script=script, resume=True)
    with pytest.raises(ControllerError, match="operation journal layout"):
        C._register_controller(launch, name="old", workspace=tmp_path)
    with pytest.raises(ControllerError, match="operation journal layout"):
        store.require_operation_journal_layout(run_dir)

    # _host_execute catches and reports the refusal; runpy must never execute.
    assert C._host_execute(
        run_id=run_id, run_dir=run_dir, script=script, resume=True, script_args=[]
    ) == 1
    assert not (tmp_path / "user-code-executed").exists()
    assert store.read_json(store.run_record_path(run_dir)).get("operationJournalLayout") == marker
    op_dir = run_dir / "tasks" / "unfinished--0001" / "operations"
    assert {p.name: p.read_bytes() for p in op_dir.iterdir()} == original_ops

@contextmanager
def _attempt(run_dir):
    runtime = C._ControllerRuntime(run_dir)
    token = C._active_runtime.set(runtime)
    try:
        yield
    finally:
        C._active_runtime.reset(token)
        runtime._executor.shutdown(wait=True)


def test_kill_lost_reply_then_other_actor_revives_never_blindly_retries(tmp_path, monkeypatch):
    _script, _run_id, run_dir = _run(tmp_path, monkeypatch)
    client = WorkerClient()  # Resolve the existing fake original full-UUID owner.
    live = {"generation": "original", "kill_calls": []}

    def kill(_client, worker):
        live["kill_calls"].append(live["generation"])
        live["generation"] = None   # The teardown really happened.
        raise SessionUnavailable("lost kill reply after teardown")

    with _attempt(run_dir), pytest.raises(SessionUnavailable):
        C.observe_worker_call(client, "kill", kill, (f"claude:{A}",), {})
    journal = run_dir / "operations" / "0001-kill.json"
    assert store.read_json(journal)["state"] == "indeterminate"

    # A new actor starts unrelated work on the same identity.
    live["generation"] = "unrelated-new-turn"
    with _attempt(run_dir), pytest.raises(ControllerOperationIndeterminate):
        C.observe_worker_call(client, "kill", kill, (f"claude:{A}",), {})
    assert live == {"generation": "unrelated-new-turn", "kill_calls": ["original"]}
    assert store.read_json(journal)["state"] == "indeterminate"


def test_new_run_publishes_current_marker_before_any_tasks(tmp_path, monkeypatch):
    _, _, run_dir = _run(tmp_path, monkeypatch)
    record = store.read_json(store.run_record_path(run_dir))
    assert store.CONTROLLER_OPERATION_JOURNAL_LAYOUT == 3
    assert record["operationJournalLayout"] == 3
    store.require_operation_journal_layout(run_dir)
    with _attempt(run_dir):
        assert C.current.run_id == record["runId"]
