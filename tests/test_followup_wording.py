"""Mac live tester H4/H8/H9/H10 CLI presentation regressions.

These are output-only assertions; no native worker needs to be launched.
"""
import ccx.cli.commands.turns as commands_turns
import ccx.ops.send as ops_send
import ccx.ops.wait as ops_wait

from types import SimpleNamespace

import pytest

from ccx.cli import codex as codex_commands
from ccx.codex import ops as codex_ops
from ccx.cli.commands import _common as commands
from ccx import constants
from ccx.cli import controller as controller_commands
from ccx.cli.parser import build_parser
from test_generation_store import A, B, add_worker, short_root  # noqa: F401


def test_send_no_wait_help_says_delivery_not_acceptance(capsys):
    with pytest.raises(SystemExit) as stopped:
        build_parser().parse_args(["send", "--help"])
    assert stopped.value.code == 0
    help_text = capsys.readouterr().out
    assert "Return once CCX has accepted the prompt" not in help_text
    assert "queued for delivery" in help_text


def test_send_no_wait_owned_explains_reconcile(short_root, monkeypatch, capsys):
    from ccx.ops import common as protocol_ops
    add_worker(short_root, A, "claude", confirmed=True)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    monkeypatch.setattr(ops_send, "send_turn", lambda *_a, **_kw: {
        "status": "owned", "settled": False, "delivery": {},
    })
    args = build_parser().parse_args([
        "send", "claude:" + A, "hello", "--no-wait", "--delivery-id", "h4-token",
    ])
    commands_turns.cmd_send(args)
    output = capsys.readouterr().out
    assert "delivery_id: h4-token" in output
    assert "queued for delivery" in output
    assert "ccx reconcile" in output and "h4-token" in output


def test_controller_status_list_explain_script_reported_status(monkeypatch, capsys):
    record = {"controller": "review", "runId": "abcdef0011223344"}
    summary = {"controller": "review", "runId": "abcdef0011223344",
               "lifecycle": "failed", "status": "completed", "tasksSucceeded": 0,
               "tasks": 1, "workers": 0, "attempts": 1}
    monkeypatch.setattr(controller_commands.store, "run_summary", lambda *_a: summary)
    controller_commands._print_summary(record, None)
    status = capsys.readouterr().out
    assert "lifecycle: failed" in status
    assert "script-reported status: completed" in status

    monkeypatch.setattr(controller_commands.store, "controller_inventory", lambda: [{
        "name": "review", "runs": 1, "latest": {"lifecycle": "failed", "status": "completed",
                                          "runId": "abcdef0011223344"},
    }])
    controller_commands.cmd_list(SimpleNamespace(json=False))
    listed = capsys.readouterr().out
    assert "SCRIPT-REPORTED" in listed
    assert "failed" in listed and "completed" in listed


@pytest.mark.parametrize("verb,description", [
    ("list", "Lifecycle tracks execution"),
    ("runs", "List stored runs"),
    ("status", "Lifecycle tracks execution"),
    ("report", "Print the run"),
    ("wait", "Wait for a run"),
    ("logs", "Print a run"),
])
def test_controller_child_help_has_description(verb, description, capsys):
    with pytest.raises(SystemExit) as stopped:
        build_parser().parse_args(["controller", verb, "--help"])
    assert stopped.value.code == 0
    output = capsys.readouterr().out
    assert description in output.split("positional arguments:", 1)[0]


def test_codex_reconcile_turn_id_names_delivery_and_turn(short_root, monkeypatch, capsys):
    from ccx.ops import common as protocol_ops
    add_worker(short_root, B, "codex", confirmed=True)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    monkeypatch.setattr(ops_wait, "reconcile_turn", lambda *_a, **_kw: {
        "status": "accepted", "delivery": {
            "delivery_id": "sent-token", "turn_id": "native-turn-9", "state": "accepted",
        },
    })
    monkeypatch.setattr(codex_commands, "_identity", lambda _record: {"short": B[:8]})
    monkeypatch.setattr(codex_ops, "_semantic_status", lambda _delivery: "accepted")
    args = build_parser().parse_args([
        "reconcile", "codex:" + B, "--turn-id", "native-turn-9",
    ])
    codex_commands.cmd_reconcile(args)
    output = capsys.readouterr().out
    assert "accepted" in output
    assert "delivery_id: sent-token" in output
    assert "turn: native-turn-9" in output


def test_codex_error_prefix_not_repeated(capsys):
    with pytest.raises(SystemExit) as stopped:
        codex_commands._fail("ccx send: send: prompt is too large", 1)
    assert stopped.value.code == 1
    err = capsys.readouterr().err.strip()
    assert err == "ccx send: prompt is too large"


def test_claude_notify_error_prefix_not_repeated(short_root, monkeypatch, capsys):
    missing = "00000000-0000-4000-8000-000000000000"
    add_worker(short_root, A, "claude")
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    args = build_parser().parse_args(["notify", "claude:" + missing])
    with pytest.raises(SystemExit) as stopped:
        commands_turns.cmd_notify(args)
    assert stopped.value.code == 2
    err = capsys.readouterr().err
    assert err.count("ccx notify:") == 1
