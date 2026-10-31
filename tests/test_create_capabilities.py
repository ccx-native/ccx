"""First-turn Claude quota caps through existing full-UUID creation/receipt paths.

Three focused groups: public/preclaim, fake-host native effect/no stickiness,
and original receipt retry/typed outcome/controller journal. Disposable fake
Claude only; no production provider, migration, or legacy short-ID fixtures.
"""
from __future__ import annotations
import ccx.cli.commands.create as commands_create
import ccx.ops.send as ops_send

import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

from ccx import CcxClient
from ccx import WorkerClient
from ccx.claude import delivery as claude_delivery
from ccx.cli.commands import _common as commands
from ccx import constants
from ccx.ops import creation
from ccx.claude import usage
from ccx.errors import (
    BudgetStopped, CapabilityUnsupported, ControllerReplayMismatch,
    CreatePartialFailure, InvalidRequest,
)
from ccx.cli.parser import build_parser
from ccx.store.root import require_ccx_active
from test_generation_store import A, B, short_root as short_root
from test_controller_mutators import _attempt, _operation
from test_create_cross_surface import (
    _deadline_fake_claude, _first_wait_original_claude_ready,
)
from test_claude_native_cli import _stop_disposable_original
from test_steer import eventually
from ccx.controllers import store as controller_store


def _write_capped_usage_fixture(cache: Path, **utilizations: float) -> None:
    """Seed a fresh owned source-qualified cache, not an unsigned legacy payload."""
    data = usage._apply_sample(
        "get_usage", None,
        {"kind": "success", "body": {
            key: {"utilization": value} for key, value in utilizations.items()
        }},
        time.time(),
    )
    cache.write_text(json.dumps(data))
    cache.chmod(0o600)


# Group A — public option validation and original main CLI pretripped policy.
def test_initial_caps_preclaim_invalid_promptless_codex_and_cli_pretrip(
    short_root, tmp_path, monkeypatch, capsys,
):
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    store = require_ccx_active(short_root)

    for invalid in (
        {}, {"5h": True}, {"5h": float("nan")},
        {"7d": float("inf")}, {"opus": 101},
        {"not-a-cap": 40}, ["5h", 40],
    ):
        with pytest.raises(InvalidRequest):
            CcxClient().new(
                "A real first message", cwd=str(tmp_path), worker_id=A, caps=invalid,
            )
    with pytest.raises(InvalidRequest, match="requires a prompt"):
        CcxClient().new(cwd=str(tmp_path), worker_id=A, caps={"5h": 40})
    with pytest.raises(InvalidRequest, match="unknown caps keys"):
        creation.create(
            engine="claude", cwd=str(tmp_path), worker_id=A,
            prompt="must not create", caps={"wrong": 10},
        )
    with pytest.raises(InvalidRequest, match="cap-opus"):
        WorkerClient().create(
            engine="codex", cwd=str(tmp_path), worker_id=B,
            prompt="no Codex Opus quota", caps={"opus": 40},
        )
    # Existing CLI sampler checks live/cached quota before reserving a worker.
    args = build_parser().parse_args([
        "new", "--engine", "claude", "pretripped task", "--cwd", str(tmp_path), "--worker-id", A,
        "--cap-5h", "30", "--json",
    ])
    monkeypatch.setattr(
        commands_create, "fetch_usage",
        lambda **kwargs: {"five_hour": {"utilization": 75.0}},
    )
    with pytest.raises(SystemExit) as pretrip:
        commands_create.cmd_new(args)
    assert pretrip.value.code == 7
    assert "no worker created" in capsys.readouterr().err
    assert not (store.pointer.generation_root / "owners").exists()
    assert not (store.pointer.generation_root / "claims").exists()
    assert not (short_root / "run").exists()


@pytest.mark.parametrize("phase", ["initial-delivery", "wait-report"])
@pytest.mark.parametrize("as_json", [False, True])
def test_codex_new_capped_first_turn_reports_budget_stopped(
    monkeypatch, tmp_path, capsys, phase, as_json,
):
    """An original first-turn cap stop reports the worker, never 'remint'."""
    cap_stop = BudgetStopped(
        "budget stop: weekly quota reached", reason="7d 29% >= 20%",
        phase="preflight" if phase == "initial-delivery" else "turn",
    )
    worker = f"codex:{B}"
    calls = []

    def claimed(**kwargs):
        calls.append(kwargs)
        raise CreatePartialFailure(
            "original first turn ended at cap",
            info={"short": B[:8], "worker_id": B, "worker": worker,
                  "engine": "codex", "native_ready": True,
                  "activation_state": "confirmed", "created": True},
            phase=phase, delivery_id="create:initial",
            delivery_status="accepted", cause=cap_stop,
        )

    monkeypatch.setattr(creation, "create", claimed)
    argv = ["new", "--engine", "codex", "--cwd", str(tmp_path), "--worker-id", B,
            "--cap-7d", "20", "--wait", "first original capped instruction"]
    if as_json:
        argv.append("--json")
    args = build_parser().parse_args(argv)
    with pytest.raises(SystemExit) as stopped:
        commands_create.cmd_new(args)
    assert stopped.value.code == 7
    assert calls and calls[0]["caps"] == {"7d": 20.0}
    output = capsys.readouterr()
    assert "never remint" not in output.err
    if as_json:
        data = json.loads(output.out)
        assert data["worker"] == worker
        assert data["status"] == "budget-stopped"
        assert data["reason"] == "7d 29% >= 20%"
        assert data["delivery_id"] == "create:initial"
    else:
        assert output.out.splitlines() == [
            worker, "status: budget-stopped", "reason: 7d 29% >= 20%",
        ]


@pytest.mark.parametrize("admission_state", ["prepared", "idle"])
def test_codex_first_cap_refusal_keeps_empty_thread_peekable(
    short_root, tmp_path, monkeypatch, capsys, admission_state,
    executable_engine_stubs,
):
    """The rejected create:initial has no native turn, like promptless new."""
    from ccx.cli import main as cli
    from ccx.codex import delivery as codex_delivery
    from ccx.codex import runtime as codex_runtime
    from ccx.ops import common as protocol_ops
    from test_delivery_flow import _activate_fake_codex

    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    store = require_ccx_active(short_root)

    def bound(ctx, original, _info, **kwargs):
        return _activate_fake_codex(ctx, original).record

    def refused(target, text, **kwargs):
        assert target == f"codex:{B}"
        assert kwargs["cap"] == {"7d": 20}
        receipt, fresh = codex_delivery.prepare(
            target, kwargs["delivery_id"], text, cap=kwargs["cap"],
        )
        assert fresh and receipt["turnId"] is None
        codex_delivery.mark_submitted(target, kwargs["delivery_id"])
        codex_delivery.mark_rejected(
            target, kwargs["delivery_id"], code="budget-stopped",
            message="weekly limit at 29%, above cap 20%",
        )
        if admission_state == "idle":
            # Native ensure_ready may load the worker before refusing turn/start.
            store.patch_lifecycle(target, changes={"state": "idle"})
        raise BudgetStopped(
            "usage cap 7d already exceeded", reason="7d 29% >= 20%",
            phase="preflight",
        )

    monkeypatch.setattr(creation, "_complete_codex_create", bound)
    monkeypatch.setattr(ops_send, "send_turn", refused)
    with pytest.raises(CreatePartialFailure) as stopped:
        creation.create(
            engine="codex", worker_id=B, cwd=str(tmp_path),
            prompt="Reply X.", caps={"7d": 20}, wait=True,
        )
    assert stopped.value.exit_code == 7
    assert isinstance(stopped.value.cause, BudgetStopped)
    rec = store.record(f"codex:{B}")
    assert rec["lifecycle"]["state"] == "idle"
    assert rec["historyKnownEmpty"] is True
    assert rec["stats"]["turns"] == 0 and rec.get("lastTurn") is None
    assert codex_delivery.read_receipt(B, "create:initial")["state"] == "rejected"

    def cannot_read_native(*args, **kwargs):
        pytest.fail("empty original Codex history must not request native thread/items")

    monkeypatch.setattr(codex_runtime, "read_thread", cannot_read_native)
    cli._main(["peek", f"codex:{B}", "--json"])
    view = json.loads(capsys.readouterr().out)
    assert view["turn_count"] == 0
    assert view["worker"]["handle"] == f"codex:{B}"


# Group B — the REAL original host/PTY, but a disposable fake Claude provider.
def test_capped_create_python_cli_worker_one_native_turn_and_uncapped_followup(
    short_root, tmp_path, monkeypatch,
):
    launches = _deadline_fake_claude(short_root, tmp_path, monkeypatch)
    log = tmp_path / "caps-first-native-pty.log"
    monkeypatch.setenv("FAKE_LOG", str(log))
    cache = tmp_path / "caps-sampled-usage.json"
    _write_capped_usage_fixture(cache, five_hour=0.0)
    monkeypatch.setattr(usage, "usage_cache_path", lambda: cache)
    monkeypatch.setenv("CCX_USAGE_CACHE", str(cache))
    store = require_ccx_active(short_root)
    prompt = "the capped original Claude creation turn"
    cap = {"5h": 60.0}
    handle = f"claude:{A}"
    try:
        _first_wait_original_claude_ready(tmp_path, A)
        first = CcxClient().new(
            prompt, cwd=str(tmp_path), worker_id=A, caps=cap,
            wait=True, timeout=12, start_timeout=4,
        )
        assert first.worker_id == A
        assert first.reply == "Fake Claude completed: " + prompt
        assert first.delivery_id == "create:initial"
        assert first.settled and first.turn_ref
        receipt = claude_delivery.read_receipt(handle, "create:initial")
        assert receipt["cap"] == cap and receipt["requestedMode"] == "new_turn_only"
        assert receipt["workerId"] == A and receipt["route"] == "new_turn"
        assert receipt["settlement"]["status"] == "completed"
        native = store.record(A)["native"]["sessionId"]
        assert native != A and receipt["turnRef"] == first.turn_ref
        assert not any(key.lower().startswith("cap") for key in store.record(A)["createArgs"])

        worker = WorkerClient().create(
            engine="claude", cwd=str(tmp_path), worker_id=A,
            prompt=prompt, caps=cap, start_timeout=4,
        )
        assert worker["worker_id"] == A and worker["delivery_id"] == "create:initial"
        assert worker["delivery_status"] in {"owned", "accepted"}
        # CLI uses the same 60.0 representation; no create-only normalization.
        repo = Path(__file__).resolve().parent.parent
        proc = subprocess.run([
            sys.executable, "-B", "-m", "ccx", "new", "--engine", "claude", prompt,
            "--cwd", str(tmp_path), "--worker-id", A,
            "--cap-5h", "60", "--json",
        ], env=os.environ.copy(), cwd=repo, text=True,
           capture_output=True, timeout=18)
        assert proc.returncode == 0, (proc.stdout, proc.stderr)
        cli = json.loads(proc.stdout)
        assert cli["worker_id"] == A and cli["delivery_id"] == "create:initial"
        assert cli["delivery"]["owned"] is True
        assert cli["delivery"]["route"] == "new_turn"
        assert log.read_bytes().count(prompt.encode()) == 1

        # Raise the sampled quota AFTER its completed cap-bound first turn.
        # The next distinct unbounded send must not inherit that cap.
        _write_capped_usage_fixture(cache, five_hour=95.0)
        followup = "this later message carries no quota cap"
        plain = CcxClient().send(
            handle, followup, wait=True, timeout=12,
            delivery_id="after-capped-create",
        )
        assert plain.reply == "Fake Claude completed: " + followup
        assert claude_delivery.read_receipt(handle, "after-capped-create")["cap"] is None
        assert log.read_bytes().count(followup.encode()) == 1
        assert launches.read_text().splitlines() == ["provider-start"]
        assert store.record(A)["native"]["sessionId"] == native
    finally:
        _stop_disposable_original(short_root)


# Group C — first host ownership and controller original UUID are each at-most-once.
def test_capped_initial_lost_return_same_spec_and_controller_original_journal(
    short_root, tmp_path, monkeypatch,
):
    from ccx.ops import common as protocol_ops

    monkeypatch.setattr(usage, "fetch_usage", lambda **kw: None)
    launches = _deadline_fake_claude(short_root, tmp_path, monkeypatch)
    log = tmp_path / "caps-lost-return-pty.log"
    monkeypatch.setenv("FAKE_LOG", str(log))
    store = require_ccx_active(short_root)
    prompt = "one first prompt with a lost reply"
    cap = {"5h": 75.0}
    handle = f"claude:{A}"
    try:
        _first_wait_original_claude_ready(tmp_path, A)
        real_send = ops_send.send_turn
        calls = []

        def lost_first_return(*args, **kwargs):
            answer = real_send(*args, **kwargs)
            calls.append(answer["delivery"])
            assert answer["status"] in {"owned", "accepted"}
            raise OSError("fake caller lost original host-owned response")

        with monkeypatch.context() as patch:
            patch.setattr(ops_send, "send_turn", lost_first_return)
            with pytest.raises(CreatePartialFailure) as lost:
                CcxClient().new(
                    prompt, cwd=str(tmp_path), worker_id=A,
                    caps=cap, start_timeout=4,
                )
        assert len(calls) == 1
        assert lost.value.phase == "initial-delivery"
        assert lost.value.worker_id == A and lost.value.delivery_id == "create:initial"
        assert lost.value.info["native_ready"] is True
        first_receipt = claude_delivery.read_receipt(handle, "create:initial")
        assert first_receipt["cap"] == cap and first_receipt["state"] in {"owned", "accepted"}

        # Retry after lost first ACK: same immutable original, no new provider input.
        original = WorkerClient().create(
            engine="claude", cwd=str(tmp_path), worker_id=A,
            prompt=prompt, caps=cap, start_timeout=4,
        )
        assert original["delivery_id"] == "create:initial"
        assert original["worker_id"] == A and original["reconciled"]
        chosen = creation.create(
            engine="claude", cwd=str(tmp_path), worker_id=A,
            prompt=prompt, caps=cap,
            permissions="full", start_timeout=4,
        )
        assert chosen["worker_id"] == A
        with pytest.raises(CreatePartialFailure) as mismatch:
            CcxClient().new(
                prompt, cwd=str(tmp_path), worker_id=A,
                caps={"5h": 74.0}, start_timeout=4,
            )
        assert mismatch.value.phase == "initial-delivery"
        assert mismatch.value.info["initial_delivery_conflict"] is True
        eventually(
            lambda: log.read_bytes().count(prompt.encode()) if log.exists() else 0,
            lambda count: count == 1,
        )
        assert launches.read_text().splitlines() == ["provider-start"]

        # Independent controller journal uses exactly the original create path.
        script = tmp_path / "controller.py"
        script.write_text("# disposable controller script\n")
        _, bootstrap = controller_store.create_bootstrap(script, tmp_path, [])
        directory = controller_store.publish_bootstrap(
            bootstrap, name="caps-owner", script=script, workspace=tmp_path,
        )
        controller_prompt = "one capped controller-created message"
        controller_cap = {"7d": 90.0}
        task_id = "caps--0001"
        with _attempt(directory, task_id=task_id):
            registered = WorkerClient().create(
                engine="claude", cwd=str(tmp_path), worker_id=B,
                prompt=controller_prompt, caps=controller_cap, start_timeout=4,
            )
        assert registered["worker_id"] == B
        journal = controller_store.read_json(_operation(directory, task_id, 1, "create"))
        assert journal["state"] == "accepted"
        assert journal["metadata"]["workerId"] == B
        with _attempt(directory, task_id=task_id):
            repeated = WorkerClient().create(
                engine="claude", cwd=str(tmp_path), worker_id=B,
                prompt=controller_prompt, caps=controller_cap, start_timeout=4,
            )
        assert repeated == registered
        with _attempt(directory, task_id=task_id):
            with pytest.raises(ControllerReplayMismatch):
                WorkerClient().create(
                    engine="claude", cwd=str(tmp_path), worker_id=B,
                    prompt=controller_prompt, caps={"7d": 89.0}, start_timeout=4,
                )
        assert claude_delivery.read_receipt(f"claude:{B}", "create:initial")["cap"] == controller_cap
        assert store.record(B)["native"]["sessionId"] != B
        eventually(
            lambda: log.read_bytes().count(controller_prompt.encode()) if log.exists() else 0,
            lambda count: count == 1,
        )
        assert launches.read_text().splitlines() == ["provider-start", "provider-start"]
        # Core validation may consume a controller ordinal, but not an owner.
        with _attempt(directory, task_id="badcap--0001"):
            with pytest.raises(InvalidRequest):
                WorkerClient().create(
                    engine="claude", cwd=str(tmp_path),
                    prompt="invalid first cap", caps={"5h": True},
                )
        bad = controller_store.read_json(_operation(directory, "badcap--0001", 1, "create"))
        assert bad["state"] == "rejected"
        assert len(list((store.pointer.generation_root / "owners").glob("*.json"))) == 2
    finally:
        try:
            # The accepted fixture tears down A only; this one test creates
            # another disposable native host under B's exact owned UUID.
            if (store.pointer.generation_root / "owners" / f"{B}.json").exists():
                CcxClient().kill(f"claude:{B}", timeout=10)
        finally:
            _stop_disposable_original(short_root)


def test_capped_initial_actual_fake_host_budget_stop_waits_with_typed_cause(
    short_root, tmp_path, monkeypatch, capsys,
):
    # Quota was unknown at admission; the fake host sees the exceeded metric.
    monkeypatch.setattr(usage, "fetch_usage", lambda **kw: None)
    launches = _deadline_fake_claude(short_root, tmp_path, monkeypatch)
    log = tmp_path / "caps-stopped-pty.log"
    monkeypatch.setenv("FAKE_LOG", str(log))
    monkeypatch.setenv("FAKE_MODE", "nostop")
    cache = tmp_path / "tripped-usage.json"
    _write_capped_usage_fixture(cache, five_hour=75.0)
    monkeypatch.setattr(usage, "usage_cache_path", lambda: cache)
    monkeypatch.setenv("CCX_USAGE_CACHE", str(cache))
    prompt = "initial Claude model turn hits its own cap"
    cap = {"5h": 30.0}
    handle = f"claude:{A}"
    store = require_ccx_active(short_root)
    try:
        _first_wait_original_claude_ready(tmp_path, A)
        started = WorkerClient().create(
            engine="claude", cwd=str(tmp_path), worker_id=A,
            prompt=prompt, caps=cap, start_timeout=4,
        )
        assert started["delivery_status"] in {"owned", "accepted"}
        stopped = eventually(
            lambda: store.record(A)["lifecycle"],
            lambda lifecycle: lifecycle["state"] == "budget-stopped",
        )
        assert "usage cap 5h reached" in stopped["lastError"]
        receipt = claude_delivery.read_receipt(handle, "create:initial")
        assert receipt["cap"] == cap and receipt.get("settlement") is None
        with pytest.raises(CreatePartialFailure) as caught:
            CcxClient().new(
                prompt, cwd=str(tmp_path), worker_id=A,
                caps=cap, wait=True, timeout=3, start_timeout=4,
            )
        error = caught.value
        assert error.worker_id == A and error.delivery_id == "create:initial"
        assert error.phase == "wait-report" and error.exit_code == 7
        assert isinstance(error.cause, BudgetStopped)
        assert error.cause.phase == "turn" and error.cause.reason == stopped["lastError"]

        # Re-enter existing Claude CLI new with the SAME receipt, and a sampler
        # that happens to have no live quota signal: this is the *wait* exit 7,
        # not the distinct preflight exit 7 tested in group A.
        monkeypatch.setattr(commands_create, "fetch_usage", lambda **kwargs: None)
        args = build_parser().parse_args([
            "new", "--engine", "claude", prompt, "--cwd", str(tmp_path), "--worker-id", A,
            "--cap-5h", "30", "--wait", "--json",
        ])
        with pytest.raises(SystemExit) as stopped_exit:
            commands_create.cmd_new(args)
        assert stopped_exit.value.code == 7
        public = json.loads(capsys.readouterr().out)
        assert public["worker_id"] == A
        assert public["phase"] == "wait-report"
        assert public["cause_type"] == "BudgetStopped"
        assert public["delivery_id"] == "create:initial"
        assert launches.read_text().splitlines() == ["provider-start"]
        eventually(
            lambda: log.read_bytes().count(prompt.encode()) if log.exists() else 0,
            lambda count: count == 1,
        )
    finally:
        _stop_disposable_original(short_root)
