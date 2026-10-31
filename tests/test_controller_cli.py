"""Controller CLI full-UUID admission: sealed roots, no launcher/provider process.

Three bounded groups: distinct namespaces and input ownership, startup/source
truth, and unavailable parent completion / Codex blocking before any effect.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from ccx.cli import main as cli
from ccx import constants
from ccx.cli import controller as commands
from ccx.controllers import store as store
from ccx.controllers.orchestrator import CallerContext
from ccx.cli.parser import build_parser
from test_generation_store import A, B, C, add_worker, fixture_root

D = "aaaaaaaa-1235-4000-8000-000000000004"  # same-engine collision with A


@pytest.fixture
def active(tmp_path, monkeypatch):
    root = fixture_root(tmp_path)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    monkeypatch.setattr(commands, "current_caller", lambda: CallerContext("unknown"))
    monkeypatch.setattr(commands, "codex_blocking_override_enabled", lambda: False)
    return root


def _run(tmp_path: Path, *, name: str = "aaaaaaaa", workers=()):
    script = tmp_path / "controller_script.py"
    script.write_text("# controller source, never executed by this test\n")
    run_id, bootstrap = store.create_bootstrap(script, tmp_path, [])
    run_dir = store.publish_bootstrap(
        bootstrap, name=name, script=script, workspace=tmp_path,
    )

    def complete(record):
        record["lifecycle"].update(state="succeeded", pid=None)
        record["attempts"][-1]["lifecycle"] = "succeeded"
        for engine, worker_id in workers:
            record["workers"].append({
                "worker": f"{engine}:{worker_id}", "worker_id": worker_id,
                "engine": engine, "short": worker_id[:8],
                "name": "reviewer", "taskId": "review--0001",
                "cwd": str(tmp_path),
            })

    store.mutate_run(run_dir, complete)
    return script, run_id, run_dir


def _workers(root):
    # No native process: terminal owners MUST still count for uniqueness.
    for worker_id, engine in ((A, "claude"), (B, "codex"), (D, "claude"), (C, "codex")):
        add_worker(root, worker_id, engine, terminal=True)


@pytest.mark.parametrize("start", (None, "1"))
def test_controller_stop_never_signals_unstamped_or_reused_pid(active, tmp_path, monkeypatch, start):
    """An unknown or reused original PID cannot receive TERM or KILL."""
    _script, run_id, run_dir = _run(tmp_path)
    store.mutate_run(run_dir, lambda record: record["lifecycle"].update(
        state="running", pid=os.getpid(), pidStart=start))
    monkeypatch.setattr(commands.os, "kill", lambda *_args: pytest.fail(
        "unproven or reused PID cannot receive TERM or KILL"))
    with pytest.raises(SystemExit) as result:
        commands.cmd_stop(SimpleNamespace(target=run_id, force=True, json=True))
    assert result.value.code != 0
    # The projection may mark a reused PID unavailable; it must never mark
    # the unproved run stopped as a result of an unauthorized signal.
    assert store.read_json(store.run_record_path(run_dir))["lifecycle"]["state"] != "stopped"


# G1: only the explicit --worker query is globally resolved. Controller run
# names and run-ID prefixes never go through worker discovery.
@pytest.mark.parametrize("query,expected", [
    (A, f"claude:{A}"),                 # bare full UUID -> qualified
    (f"claude:{A}", f"claude:{A}"),    # qualified full UUID
    ("aaaaaaaa-1234", f"claude:{A}"),  # uniquely longer prefix
    ("codex:aaaaaaaa", f"codex:{B}"), # engine-qualified unique prefix
])
def test_controller_input_resolves_only_recorded_original_worker(
    active, tmp_path, capsys, query, expected,
):
    _workers(active)
    _script, run_id, run_dir = _run(
        tmp_path, workers=(("claude", A), ("codex", B)),
    )
    cli._main(["controller", "input", run_id[:8], "--worker", query,
               "--comment", "inspect original", "--json"])
    entry = json.loads(capsys.readouterr().out)
    assert (entry["worker"], entry["workerName"], entry["comment"]) == (
        expected, "reviewer", "inspect original",
    )
    assert store.read_inputs(run_dir) == [entry]
    assert len(store.read_json(store.run_record_path(run_dir))["attempts"]) == 1


@pytest.mark.parametrize("query", [A, "aaaaaaaa-1234"])
def test_controller_resume_worker_records_exact_input_before_fake_launch(
    active, tmp_path, capsys, monkeypatch, query,
):
    _workers(active)
    _script, run_id, run_dir = _run(tmp_path, workers=(("claude", A),))
    initial = store.read_json(store.run_record_path(run_dir))
    seen = []

    def fake_launch(**kwargs):
        seen.append(kwargs)
        return {"ok": True, "runId": kwargs["run_id"],
                "runDir": str(kwargs["run_dir"]), "controller": "aaaaaaaa"}

    monkeypatch.setattr(commands, "_spawn_controller", fake_launch)
    cli._main(["controller", "resume", "--detach", "--worker", query,
               "--comment", "continue original", "--json", run_id[:8]])
    assert json.loads(capsys.readouterr().out)["ok"]
    assert len(seen) == 1 and seen[0]["resume"] is True
    entry, = store.read_inputs(run_dir)
    updated = store.read_json(store.run_record_path(run_dir))
    assert entry["worker"] == f"claude:{A}"
    assert entry["comment"] == "continue original"
    assert len(updated["attempts"]) == 2
    assert updated["attempts"][-1]["startForm"] == "resume-worker-comment"
    assert updated["attempts"][-1]["startInputId"] == entry["id"]
    assert updated["attempts"][-1]["attemptToken"] != initial["attempts"][0]["attemptToken"]
    assert updated["sourceSha256"] == initial["sourceSha256"]


@pytest.mark.parametrize("query,exit_code,candidates", [
    ("aaaaaaaa", 1, (f"claude:{A}", f"codex:{B}", f"claude:{D}")),
    ("claude:aaaaaaaa", 1, (f"claude:{A}", f"claude:{D}")),
    ("reviewer", 1, ()),            # a display name has no authority
    ("aaaaaaa", 1, ()),             # less than eight lowercase hex
    ("AAAAAAAA", 1, ()),           # not case-folded
    ("dddddddd", 2, ()),           # no owner
])
def test_controller_worker_preflight_refuses_before_input_or_attempt(
    active, tmp_path, capsys, query, exit_code, candidates, monkeypatch,
):
    _workers(active)
    _script, run_id, run_dir = _run(tmp_path, workers=(("claude", A),))
    before = store.run_record_path(run_dir).read_bytes()
    monkeypatch.setattr(commands, "cmd_input", lambda _args: pytest.fail("handler entered"))
    monkeypatch.setattr(commands, "cmd_resume", lambda _args: pytest.fail("handler entered"))
    # The real CLI parser binds the current handlers, including these sentinels.
    for verb in ("input", "resume"):
        tail = (["--comment", "no effect"] if verb == "input"
                else ["--comment", "no effect", "--detach"])
        with pytest.raises(SystemExit) as ex:
            cli._main(["controller", verb, run_id[:8], "--worker", query, *tail])
        assert ex.value.code == exit_code
        output = capsys.readouterr()
        assert output.out == ""
        for full_candidate in candidates:
            assert full_candidate in output.err
        assert store.run_record_path(run_dir).read_bytes() == before
        assert store.read_inputs(run_dir) == []


def test_controller_names_run_prefix_and_membership_never_retarget(
    active, tmp_path, capsys, monkeypatch,
):
    _workers(active)
    _script, run_id, run_dir = _run(tmp_path, workers=(("claude", A),))
    # Controller *name* is "aaaaaaaa", also an ambiguous worker prefix.
    cli._main(["controller", "input", "aaaaaaaa", "--note", "note by controller name", "--json"])
    first = json.loads(capsys.readouterr().out)
    cli._main(["controller", "input", run_id[:8], "--note", "note by run-ID prefix", "--json"])
    second = json.loads(capsys.readouterr().out)
    assert first["worker"] is second["worker"] is None
    assert [r["comment"] for r in store.read_inputs(run_dir)] == [
        "note by controller name", "note by run-ID prefix",
    ]
    # An owned but unregistered worker must not be written as controller input.
    before = store.run_record_path(run_dir).read_bytes()
    with pytest.raises(SystemExit) as ex:
        cli._main(["controller", "input", "aaaaaaaa", "--worker", C,
                   "--comment", "wrong run"])
    assert ex.value.code == 1
    assert "not uniquely recorded" in capsys.readouterr().err
    assert store.run_record_path(run_dir).read_bytes() == before
    assert len(store.read_inputs(run_dir)) == 2

    # These original read verbs accept controller names, not global workers.
    cli._main(["controller", "list", "--json"])
    assert json.loads(capsys.readouterr().out)["controllers"][0]["name"] == "aaaaaaaa"
    cli._main(["controller", "runs", "aaaaaaaa", "--json"])
    assert json.loads(capsys.readouterr().out)["runs"][0]["runId"] == run_id
    monkeypatch.setattr(commands.WorkerClient, "status", lambda _self, worker: {
        "worker": worker, "state": "unavailable",
    })
    cli._main(["controller", "status", "aaaaaaaa", "--json"])
    status = json.loads(capsys.readouterr().out)
    assert status["runId"] == run_id
    assert status["workersDetail"][0]["worker"] == f"claude:{A}"
    assert status["workersDetail"][0]["live"]["state"] == "unavailable"


# G2: detached startup is a startup verdict, never fabricated completion;
# source mismatch on resume refuses before input and attempt mutation.
def test_controller_detach_is_only_startup_ack(active, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(commands, "current_caller", lambda: CallerContext("codex", "native-thread"))
    marker = tmp_path / "source-was-executed"
    source = tmp_path / "controller_script.py"
    source.write_text(f"from pathlib import Path\nPath({str(marker)!r}).write_text('bad')\n")
    launched = []

    def fake_launch(**kwargs):
        launched.append(kwargs)
        return {"ok": True, "runId": kwargs["run_id"],
                "runDir": str(kwargs["run_dir"]), "controller": "test-flow"}

    monkeypatch.setattr(commands, "_spawn_controller", fake_launch)
    cli._main(["controller", "run", "--detach", "--json", str(source)])
    result = json.loads(capsys.readouterr().out)
    assert result["ok"] and result["runId"] == launched[0]["run_id"]
    assert launched[0]["resume"] is False
    assert "lifecycle" not in result and not marker.exists()
    record = store.read_json(store.run_record_path(Path(result["runDir"])))
    assert record["lifecycle"]["state"] == "starting"
    assert record["sourceSha256"] == store.source_sha256(source)
    assert record["operationJournalLayout"] == 3
    assert record["attempts"][0]["startForm"] == "initial"
    assert record["attempts"][0]["attemptToken"]


def test_controller_resume_source_mismatch_before_input_or_attempt(
    active, tmp_path, monkeypatch, capsys,
):
    _source, run_id, run_dir = _run(tmp_path)
    source = tmp_path / "controller_script.py"
    source.write_text("# changed controller source\n")
    record_before = store.run_record_path(run_dir).read_bytes()
    events_before = (run_dir / "events.ndjson").read_bytes()
    monkeypatch.setattr(commands, "_spawn_controller", lambda **_: pytest.fail("host spawned"))
    for argv in (["--note", "continue"], []):
        with pytest.raises(SystemExit) as ex:
            cli._main(["controller", "resume", "--detach", run_id[:8], *argv])
        assert ex.value.code == 1
        assert "source changed" in capsys.readouterr().err
        assert store.run_record_path(run_dir).read_bytes() == record_before
        assert (run_dir / "events.ndjson").read_bytes() == events_before
        assert store.read_inputs(run_dir) == []


@pytest.mark.parametrize("argv", [
    ["controller", "run", "missing.py"],
    ["controller", "resume", "aaaaaaaa"],
    ["controller", "wait", "aaaaaaaa"],
    ["controller", "logs", "aaaaaaaa", "--follow"],
])
def test_codex_blocking_controller_commands_refuse_with_available_guidance(
    active, monkeypatch, capsys, argv,
):
    monkeypatch.setattr(commands, "current_caller", lambda: CallerContext("codex", "native-thread"))
    from ccx.controllers import orchestrator
    monkeypatch.setattr(orchestrator, "_external_codex_unavailable", lambda _thread: None)
    monkeypatch.setattr(commands, "codex_blocking_override_enabled", lambda: False)

    def forbidden(*_args, **_kwargs):
        pytest.fail("blocking Codex entry must refuse before controller effects")

    for name in ("create_bootstrap", "resolve_run", "append_controller_input", "mutate_run"):
        monkeypatch.setattr(store, name, forbidden)
    monkeypatch.setattr(commands, "_spawn_controller", forbidden)
    with pytest.raises(SystemExit) as ex:
        cli._main(argv)
    assert ex.value.code == 1
    output = capsys.readouterr()
    assert output.out == ""
    for allowed_choice in ("--detach", "status TARGET", "report TARGET", "logs TARGET"):
        assert allowed_choice in output.err
    assert "--notify-parent" not in output.err


def test_codex_nonblocking_reads_and_unchanged_controller_grammar(
    active, tmp_path, monkeypatch, capsys,
):
    _script, run_id, run_dir = _run(tmp_path, name="read-flow")
    monkeypatch.setattr(commands, "current_caller", lambda: CallerContext("codex", "native-thread"))
    report = run_dir / "reports" / "last.md"
    report.write_text("exact frozen report\n")
    store.mutate_run(run_dir, lambda record: record.update(
        latestReport={"path": str(report), "summary": "latest"},
    ))
    (run_dir / "controller.log").write_text("original log\n")
    cli._main(["controller", "report", run_id[:8]])
    assert capsys.readouterr().out == "exact frozen report\n\n"  # print adds a newline
    cli._main(["controller", "logs", "read-flow"])
    assert capsys.readouterr().out == "original log\n"
    cli._main(["controller", "status", "read-flow", "--json"])
    assert json.loads(capsys.readouterr().out)["runId"] == run_id

    parser = build_parser()
    for child in ("run", "resume", "input", "notify"):
        with pytest.raises(SystemExit) as ex:
            parser.parse_args(["controller", child, "--help"])
        assert ex.value.code == 0
        help_text = capsys.readouterr().out
        if child in {"run", "resume", "notify"}:
            assert "Codex parent" in help_text
            assert "currently unavailable" not in help_text
        if child in {"input", "resume"}:
            assert "worker id" in help_text and "not a name" in help_text
    with pytest.raises(SystemExit) as ex:
        parser.parse_args(["controller", "run", "--detach", "--notify-parent", "flow.py"])
    assert ex.value.code == 1
    capsys.readouterr()
    run_args = parser.parse_args(["controller", "run", "--detach", "flow.py", "--", "--notify-parent"])
    assert run_args.detach and not run_args.notify_parent
    assert commands._script_args(run_args.script_args) == ["--notify-parent"]
    for private_cmd, original in (("_launch", commands.cmd_internal_launch),
                                  ("_host", commands.cmd_internal_host)):
        args = parser.parse_args(["controller", private_cmd, "--run-id", run_id,
                                  "--run-dir", str(run_dir), "--script", "flow.py",
                                  "--argv-json", "[]"])
        assert args.func is original
