"""C1 CLI-only engine choice, creation preflight and promptless original resume."""
from __future__ import annotations
import ccx.cli.commands.create as commands_create
import ccx.cli.commands.control as commands_control
import ccx.cli.commands.turns as commands_turns
import ccx.ops.report as ops_report
import ccx.ops.send as ops_send
import ccx.ops.wait as ops_wait

import json

import pytest

from ccx.cli import main as cli
from ccx.cli import codex as codex_commands
from ccx.cli.commands import _common as commands
from ccx import constants
from ccx.controllers import policy as orchestrator_policy
from ccx.controllers.orchestrator import CallerContext
from ccx.cli.parser import build_parser
from ccx.workers import WorkerClient
from test_generation_store import A, B, add_worker, short_root
from test_codex_native_effects import _f2_fake_processes
from test_notify_claude import owners  # noqa: F401 - activated original owners


@pytest.mark.parametrize(("engine", "flags"), [
    ("claude", ["--permission-mode", "plan"]),
    ("claude", ["--permissions", "full", "--permission-mode", "plan"]),
    ("claude", ["--sandbox", "read-only"]),
    ("claude", ["--approval-policy", "never"]),
    ("claude", ["--start-timeout", "2"]),
    ("claude", ["--settle"]),
    ("codex", ["--permissions", "restricted", "--sandbox", "workspace-write"]),
    ("codex", ["--permissions", "full", "--approval-policy", "never"]),
    ("codex", ["--cap-opus", "30"]),
    ("codex", ["--agent", "some-agent"]),
    ("codex", ["--allowed-tools", "Read"]),
    ("codex", ["--permission-mode", "plan"]),
    ("codex", ["--start-timeout", "0"]),
    ("codex", ["--start-timeout", "91"]),
])
def test_new_refuses_wrong_engine_or_conflicting_posture_before_any_effect(
    engine, flags, tmp_path, monkeypatch, capsys,
):
    from ccx.ops import creation
    from ccx.controllers import live_vars

    def forbidden(*_args, **_kwargs):
        pytest.fail("wrong-engine options reached claim, variables or parent preparation")

    monkeypatch.setattr(creation, "create", forbidden)
    monkeypatch.setattr(live_vars, "prepare_durable_attachment", forbidden)
    monkeypatch.setattr(commands_create, "_prepare_parent_notification", forbidden)
    monkeypatch.setattr(codex_commands, "_prepare_parent_notification", forbidden)
    args = build_parser().parse_args([
        "new", "--engine", engine, "--cwd", str(tmp_path),
        "--worker-id", A, "--notify-parent", "--var", 'data={"ok":true}',
        *flags, "--json",
    ])
    with pytest.raises(SystemExit) as stopped:
        commands_create.cmd_new(args)
    assert stopped.value.code == 1
    assert "no worker identity claimed" in capsys.readouterr().err


@pytest.mark.parametrize(("engine", "flags", "posture", "sandbox", "policy"), [
    ("claude", [], None, None, None),
    ("claude", ["--permissions", "restricted", "--permission-mode", "plan"],
     "restricted", None, None),
    ("codex", [], None, None, None),
    ("codex", ["--permissions", "restricted"], "restricted", None, None),
    ("codex", ["--sandbox", "read-only", "--approval-policy", "on-request"],
     None, "read-only", "on-request"),
])
def test_new_forwards_original_native_posture_without_normalizing_python_api(
    engine, flags, posture, sandbox, policy, tmp_path, monkeypatch, capsys,
):
    from ccx.ops import creation

    captured = []

    def native_create(**kwargs):
        captured.append(kwargs)
        return {
            "worker": f"{engine}:{A}", "worker_id": A, "engine": engine,
            "short": A[:8], "native_source": f"{engine}-original-source",
            "created": True, "reconciled": False, "native_ready": True,
            "activation_state": "confirmed",
        }

    monkeypatch.setattr(creation, "create", native_create)
    args = build_parser().parse_args([
        "new", "--engine", engine, "--cwd", str(tmp_path),
        "--worker-id", A, "--json", *flags,
    ])
    commands_create.cmd_new(args)
    out = json.loads(capsys.readouterr().out)
    assert out["worker"] == f"{engine}:{A}" and out["native_source"]
    assert len(captured) == 1 and captured[0]["worker_id"] == A
    assert captured[0].get("permissions") == posture
    assert captured[0].get("sandbox") == sandbox
    assert captured[0].get("approval_policy") == policy


def test_resume_preflights_stopped_cross_engine_prefix_and_never_sends(
    short_root, monkeypatch, capsys,
):
    add_worker(short_root, A, "claude", terminal=True)
    add_worker(short_root, B, "codex", terminal=True)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    invoked = []

    def resume(_self, worker, **_kwargs):
        invoked.append(worker)
        return {"worker": worker, "worker_id": worker.split(":", 1)[1],
                "engine": worker.split(":", 1)[0], "resumed": True,
                "state": "idle", "session_id": A}

    monkeypatch.setattr(WorkerClient, "resume", resume)
    with pytest.raises(SystemExit) as ambiguous:
        cli._main(["resume", "aaaaaaaa", "--json"])
    assert ambiguous.value.code == 1 and invoked == []
    errors = capsys.readouterr().err
    assert f"claude:{A}" in errors and f"codex:{B}" in errors
    cli._main(["resume", "claude:aaaaaaaa", "--json"])
    out = json.loads(capsys.readouterr().out)
    assert invoked == [f"claude:{A}"]
    assert out["session_id"] == A and out["worker"] == f"claude:{A}"


@pytest.mark.parametrize("argv", [
    ["send", A, "hello"], ["send", A, "hello", "--wait"],
    ["new", "--engine", "codex", "hello", "--wait"],
    ["new", "--engine", "claude", "hello", "--wait"],
    ["fork", A, "hello", "--wait"], ["wait", A],
    ["respond", A, "--cancel", "--wait"],
    ["answer", A, "yes", "--wait"],
    ["approve", A, "--wait"], ["reject", A, "--wait"],
    ["compact", A],
])
def test_codex_parent_model_wait_refused_before_parser_or_effect(
    argv, monkeypatch, capsys,
):
    monkeypatch.setattr(orchestrator_policy, "current_caller", lambda: CallerContext("codex", "managed-parent", A, True))
    monkeypatch.setattr(orchestrator_policy, "codex_blocking_override_enabled", lambda: False)
    monkeypatch.setattr(cli, "build_parser", lambda: pytest.fail("guard was too late"))
    with pytest.raises(SystemExit) as stopped:
        cli._main(argv)
    assert stopped.value.code == 1
    assert "--notify-parent" in capsys.readouterr().err or argv[0] == "wait"


@pytest.mark.parametrize("argv", [
    ["send", A, "hello", "--no-wait"],
    ["send", A, "hello", "--notify-parent"],
    ["new", "--engine", "codex", "hello"],
    ["new", "--engine", "codex", "hello", "--notify-parent"],
    ["respond", A, "--cancel", "--notify-parent"],
    ["resume", A], ["steer", A, "add"], ["kill", A], ["report", A],
])
def test_codex_parent_nonblocking_and_operational_calls_remain_allowed(argv, monkeypatch):
    monkeypatch.setattr(orchestrator_policy, "current_caller", lambda: CallerContext("codex"))
    monkeypatch.setattr(orchestrator_policy, "codex_blocking_override_enabled", lambda: False)
    orchestrator_policy.reject_codex_blocking_call(argv)


def test_codex_parent_hidden_override_preserves_explicit_wait(monkeypatch):
    monkeypatch.setattr(orchestrator_policy, "current_caller", lambda: CallerContext("codex"))
    monkeypatch.setattr(orchestrator_policy, "codex_blocking_override_enabled", lambda: True)
    orchestrator_policy.reject_codex_blocking_call(["send", A, "hello"])


def test_codex_root_new_partial_retry_and_promptless_resume_use_same_native_source(
    short_root, tmp_path, monkeypatch,
):
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch, start_delay=.4,
        persisted_threads=tmp_path / "cli-root-codex-history.json",
    ) as (call, _restart, trace, store, _rpc):
        argv = ("new", "--engine", "codex", "--cwd", str(tmp_path),
                "--worker-id", B, "--json")
        partial = call(*argv, "--start-timeout", ".1")
        assert partial.returncode == 3, (partial.stdout, partial.stderr)
        pending = json.loads(partial.stdout)
        assert pending["worker_id"] == B and pending["native_ready"] is False

        retry = call(*argv, "--start-timeout", "6")
        assert retry.returncode == 0, (retry.stdout, retry.stderr)
        created = json.loads(retry.stdout)
        assert created["worker"] == f"codex:{B}" and created["reconciled"] is True
        native_thread = store.record(B)["native"]["threadId"]
        assert native_thread and trace.read_text().count("RPC thread/start") == 1
        assert trace.read_text().count("RPC turn/start") == 0

        revived = call("resume", "codex:" + B[:8], "--json")
        assert revived.returncode == 0, (revived.stdout, revived.stderr)
        recovered = json.loads(revived.stdout)
        assert recovered["worker"] == f"codex:{B}"
        assert recovered["thread_id"] == native_thread
        assert trace.read_text().count("RPC thread/start") == 1
        assert trace.read_text().count("RPC turn/start") == 0


def test_c2_root_has_no_codex_aliases_and_usage_selects_one_native_engine():
    choices = build_parser()._subparsers._group_actions[0].choices
    assert "codex" not in choices and "steer-status" not in choices
    for command in ("send", "steer", "reconcile", "wait", "notify", "set", "compact"):
        assert command in choices
    assert build_parser().parse_args(["usage"]).engine is None
    assert build_parser().parse_args(["usage", "--engine", "codex", "--tokens"]).tokens
    for argv in (["peek", A, "--live"], ["peek", A, "--debug"],
                 ["list", "--managed"], ["list", "--live"]):
        with pytest.raises(SystemExit) as removed:
            build_parser().parse_args(argv)
        assert removed.value.code == 1


@pytest.mark.parametrize(("engine", "verb", "flags"), [
    ("codex", "send", ["--cap-opus", "30"]),
    ("claude", "send", ["--settle", "--notify-parent"]),
    ("codex", "steer", ["--timeout", "1"]),
    ("codex", "fork", ["--cap-opus", "30"]),
    ("claude", "fork", ["--settle", "--notify-parent"]),
    ("claude", "reconcile", ["--turn-id", "codex-turn"]),
    ("codex", "wait", ["--next-turn"]),
    ("claude", "wait", ["--settle"]),
    ("codex", "notify", ["--next-turn"]),
    ("claude", "notify", ["--settle"]),
    ("codex", "approve", ["--wait"]),
    ("codex", "reject", ["--reason", "no"]),
    ("codex", "set", ["--permissions", "restricted"]),
    ("codex", "set", ["--restart"]),
    ("codex", "compact", ["--operation-id", "native-original"]),
    ("claude", "compact", ["--settle", "--notify-parent"]),
    ("codex", "logs", ["--jsonl"]),
])
def test_c2_wrong_native_options_refused_before_command_effect(
    engine, verb, flags, short_root, monkeypatch, capsys,
):
    add_worker(short_root, A, "claude", terminal=True)
    add_worker(short_root, B, "codex", terminal=True)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    target = f"{engine}:{B if engine == 'codex' else A}"
    parser = build_parser()
    args = [verb, target]
    if verb in {"send", "steer", "fork"}:
        args.append("one native instruction")
    if verb == "reconcile":
        args.extend(flags)
    else:
        args.extend(flags)
    if verb == "fork":
        from ccx.ops import forking
        monkeypatch.setattr(forking, "fork", lambda *_a, **_kw: pytest.fail("fork effect"))
    args_obj = parser.parse_args(args)
    monkeypatch.setattr(args_obj, "func", lambda _a: pytest.fail("Claude effect"))
    with pytest.raises(SystemExit) as stopped:
        cli._native_handler(args_obj, __import__('ccx.store.root', fromlist=["require_ccx_active"]).require_ccx_active(short_root))(args_obj)
    assert stopped.value.code == 1
    assert "no native effect submitted" in capsys.readouterr().err


def test_c2_root_selects_original_codex_handlers_and_shared_stop_kill(short_root, monkeypatch):
    add_worker(short_root, A, "claude", terminal=True)
    add_worker(short_root, B, "codex", terminal=True)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    store = __import__('ccx.store.root', fromlist=["require_ccx_active"]).require_ccx_active(short_root)
    for verb, source in (("send", "cmd_send"), ("steer", "cmd_steer"),
                         ("reconcile", "cmd_reconcile"), ("wait", "cmd_wait"),
                         ("notify", "cmd_notify"), ("approve", "cmd_approve"),
                         ("reject", "cmd_reject"), ("set", "cmd_set"),
                         ("compact", "cmd_compact")):
        argv = [verb, f"codex:{B}"]
        if verb in {"send", "steer"}:
            argv.append("one input")
        if verb == "reconcile":
            argv.extend(["--delivery-id", "native-token"])
        selected = cli._native_handler(build_parser().parse_args(argv), store)
        assert selected is getattr(codex_commands, source)
    for verb in ("stop", "kill", "resume"):
        args = build_parser().parse_args([verb, f"codex:{B}"])
        owner = (commands_create if verb == "resume"
                 else commands_control)
        assert args.func is getattr(owner, f"cmd_{verb}")


def test_c2_claude_steer_receipt_is_read_only(short_root, monkeypatch, capsys):
    from types import SimpleNamespace
    from ccx.claude import delivery as claude_delivery
    from ccx.client import CcxClient
    token = "existing-original-steer"
    calls = []
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    monkeypatch.setattr(claude_delivery, "read_receipt",
                        lambda target, delivery: {"requestedMode": "steer_only"})
    def placed(_self, target, delivery):
        calls.append((target, delivery))
        return SimpleNamespace(status="accepted-pending",
                               as_dict=lambda: {"worker": target, "delivery_id": delivery,
                                                "status": "accepted-pending"})
    monkeypatch.setattr(CcxClient, "steer_status", placed)
    commands_turns.cmd_reconcile(build_parser().parse_args([
        "reconcile", f"claude:{A}", "--delivery-id", token, "--json"]))
    out = json.loads(capsys.readouterr().out)
    assert out["status"] == "accepted-pending"
    assert calls == [(f"claude:{A}", token)]


@pytest.mark.parametrize(("engine", "no_wait"), [
    ("claude", True), ("claude", False),
    ("codex", True), ("codex", False),
])
def test_c2_definite_rejected_send_original_receipt_exits_five(
    engine, no_wait, short_root, monkeypatch, capsys,
):
    from ccx.ops import common as protocol_ops
    add_worker(short_root, A if engine == "claude" else B, engine, terminal=True)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    token = "original-rejected-token"
    selected = f"{engine}:{A if engine == 'claude' else B}"
    first = {"status": "rejected", "settled": False,
             "delivery": {"delivery_id": token, "status": "rejected"}}
    submitted = []

    def refuse(worker, prompt, **kwargs):
        submitted.append((worker, prompt, kwargs["delivery_id"]))
        return dict(first)

    def observe(worker, **kwargs):
        assert worker == selected and kwargs["delivery_id"] == token
        return dict(first)

    monkeypatch.setattr(ops_send, "send_turn", refuse)
    monkeypatch.setattr(ops_wait, "reconcile_turn", observe)
    monkeypatch.setattr(ops_report, "report_worker",
                        lambda *_args, **_kwargs: pytest.fail("rejected send fabricated a report"))
    argv = ["send", selected, "an original native turn", "--delivery-id", token, "--json"]
    if no_wait:
        argv.append("--no-wait")
    args = build_parser().parse_args(argv)
    from ccx.store.root import require_ccx_active
    with pytest.raises(SystemExit) as result:
        cli._native_handler(args, require_ccx_active(short_root))(args)
    assert result.value.code == 5
    body = json.loads(capsys.readouterr().out)
    assert body["status"] == "rejected" and body["delivery_id"] == token
    assert not body["settled"] and "report" not in body
    assert submitted == [(selected, "an original native turn", token)]


def test_c3_claude_generic_pre_effect_send_exception_retains_exit_three(
    short_root, monkeypatch, capsys,
):
    from ccx.ops import common as protocol_ops
    from ccx.errors import DeliveryRejected

    add_worker(short_root, A, "claude", terminal=True)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    calls = []

    def rejected(worker, text, **kwargs):
        calls.append((worker, text, kwargs["delivery_id"]))
        raise DeliveryRejected("original worker busy", short=A[:8], state="busy")

    monkeypatch.setattr(ops_send, "send_turn", rejected)
    args = build_parser().parse_args([
        "send", f"claude:{A}", "native turn", "--delivery-id", "original-generic",
        "--no-wait", "--json",
    ])
    with pytest.raises(SystemExit) as stopped:
        commands_turns.cmd_send(args)
    assert stopped.value.code == 3  # Only definite returned receipts gain exit 5.
    assert "original worker busy" in capsys.readouterr().err
    assert calls == [(f"claude:{A}", "native turn", "original-generic")]


@pytest.mark.parametrize("no_wait", [True, False])
@pytest.mark.parametrize(("host_state", "exit_code"), [("busy", 5), ("waiting", 5)])
def test_c3_claude_proven_busy_host_refusal_exits_five_before_any_effect(
    owners, monkeypatch, capsys, no_wait, host_state, exit_code,
):
    from ccx.claude import delivery as claude_delivery
    from ccx.ops import common as protocol_ops
    from ccx.rpc import rpc
    from test_worker_permissions import _warm_original

    _root, store = owners
    status, proof, _seen = _warm_original(store, monkeypatch)
    status["state"] = host_state
    ops = []

    def host(target, msg, timeout):
        ops.append(msg["op"])
        if msg["op"] == "startup-status":
            return {"ok": True, "workerId": A, "engine": "claude",
                    "generation": store.generation, "nativeStartProof": proof}
        if msg["op"] == "status":
            return dict(status)
        pytest.fail(f"refused send reached native host op {msg['op']!r}")

    monkeypatch.setattr(rpc, "_rpc", host)
    monkeypatch.setattr(ops_wait, "reconcile_turn",
                        lambda *_a, **_k: pytest.fail("refused send was observed"))
    token = f"busy-proof-{host_state}-{no_wait}"
    argv = ["send", f"claude:{A}", "native turn", "--delivery-id", token, "--json"]
    if no_wait:
        argv.append("--no-wait")
    with pytest.raises(SystemExit) as stopped:
        commands_turns.cmd_send(build_parser().parse_args(argv))
    assert stopped.value.code == exit_code
    captured = capsys.readouterr()
    assert not captured.out
    assert f"Worker is {host_state}" in captured.err
    assert ops == ["startup-status", "status"]  # never deliver
    assert claude_delivery.read_receipt(f"claude:{A}", token) is None
    assert store.read_receipt(f"claude:{A}", "claude-delivery", token) is None


def test_c3_claude_indeterminate_send_exits_six_without_resend(
    short_root, monkeypatch, capsys,
):
    from ccx.ops import common as protocol_ops
    from ccx.store.root import NativeEffectIndeterminate

    add_worker(short_root, A, "claude", terminal=True)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    calls = []

    def uncertain(worker, text, **kwargs):
        calls.append(kwargs["delivery_id"])
        raise NativeEffectIndeterminate("original submitted-unknown")

    monkeypatch.setattr(ops_send, "send_turn", uncertain)
    with pytest.raises(SystemExit) as stopped:
        commands_turns.cmd_send(build_parser().parse_args([
            "send", f"claude:{A}", "native turn", "--delivery-id", "uncertain-id",
        ]))
    assert stopped.value.code == 6
    assert "never resend" in capsys.readouterr().err
    assert calls == ["uncertain-id"]


def test_wait_progress_heartbeat_removed_but_event_stream_option_retained():
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["wait", "some-worker", "--settle", "--heartbeat", "30"])
    wait = parser.parse_args(["wait", "some-worker", "--settle",
                              "--timeout", "3", "--wake-grace", "2"])
    assert (wait.timeout, wait.wake_grace) == (3.0, 2.0)
    events = parser.parse_args(["events", "some-worker", "--follow", "--json",
                                "--heartbeat", "30"])
    assert events.heartbeat == 30.0


@pytest.mark.parametrize(("argv", "field", "value"), [
    (["send", "w", "--wait", "--timeout", "5", "--json", "hi"], "text", "hi"),
    (["send", "w", "hi", "--wait"], "text", "hi"),
    (["send", "w", "--wait", "--", "-dash"], "text", "-dash"),
    (["steer", "w", "--json", "nudge"], "text", "nudge"),
    (["fork", "w", "--name", "x", "go"], "prompt", "go"),
    (["vars", "w", "--json", "counter"], "name", "counter"),
    (["answer", "w", "--json", "yes"], "text", "yes"),
])
def test_trailing_optional_positional_parses_after_options(argv, field, value):
    args = build_parser().parse_args(argv)
    assert (args.target, getattr(args, field)) == ("w", value)


def test_intermixed_parsing_keeps_remainder_and_extra_refusals():
    p = build_parser()
    assert p.parse_args(["controller", "run", "x.py", "--", "--a", "b"]).script_args == ["--a", "b"]
    with pytest.raises(SystemExit) as exc:
        p.parse_args(["send", "w", "a", "b"])
    assert exc.value.code == 1
