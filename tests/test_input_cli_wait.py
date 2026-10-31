"""Full-UUID top-level Claude CLI input waiting over disposable native hooks."""
from __future__ import annotations
import ccx.cli.commands.turns as commands_turns

import json
import time
from types import SimpleNamespace

import pytest

from ccx.cli.commands import _common as commands
from ccx.errors import SessionUnavailable
from ccx.types import SendResult
from test_generation_store import A, short_root
from test_claude_native_cli import generated_hook_host


def _brokered(store, kind: str) -> dict:
    until = time.monotonic() + 12
    while time.monotonic() < until:
        waiting = store.record(A).get("waitingFor")
        if isinstance(waiting, dict) and isinstance(waiting.get("id"), str):
            tool = waiting.get("tool") or waiting.get("tool_name")
            if (kind == "question" and tool == "AskUserQuestion"
                    or kind == "permission" and tool == "Bash"
                    or kind == "elicitation" and waiting.get("type") == "elicitation"):
                return waiting
        time.sleep(.04)
    pytest.fail(f"native {kind} gate not observed: {store.record(A).get('waitingFor')}")


@pytest.mark.parametrize(
    ("kind", "prompt", "generated_hook_host", "as_json"), [
        ("question", "ask generated question", None, False),
        ("permission-approve", "request generated permission", "restricted", True),
        ("permission-reject", "request generated permission", "restricted", False),
        ("elicitation", "request generated elicitation", None, True),
    ],
    indirect=["generated_hook_host"],
)
def test_real_cli_waits_for_exact_original_input_turn(
    generated_hook_host, kind, prompt, as_json,
):
    store, call, _env, _log = generated_hook_host
    delivery_id = "d3-original-" + kind
    sent = call("send", A, prompt, "--no-wait",
                "--delivery-id", delivery_id, "--json")
    assert sent.returncode == 0, (sent.stdout, sent.stderr)
    gate_kind = ("permission" if kind.startswith("permission")
                 else kind)
    gate = _brokered(store, gate_kind)

    if kind == "question":
        # A globally unique eight-character user prefix is discovery ONLY;
        # the D1 backend receives the resolved complete original UUID.
        target = A[:8]
        args = ["answer", target, "Yes"]  # omitted ID binds original Q once
    elif kind == "permission-approve":
        args = ["approve", A]  # no ID: CLI binds this exact current gate once
    elif kind == "permission-reject":
        args = ["reject", A, "--reason", "declined original"]  # source-bound deny
    else:
        args = ["respond", A, "--accept", "--field", "title=Text"]
    if kind == "permission-reject":
        bad = call("reject", A, "--request-id", "old-successor", "--wait")
        assert bad.returncode == 5, (bad.stdout, bad.stderr)
        assert store.record(A)["waitingFor"]["id"] == gate["id"]
    out = call(*args, "--wait", *(["--json"] if as_json else []))
    assert out.returncode == 0, (out.stdout, out.stderr)
    assert not out.stderr or "[ccx] " not in out.stderr
    if as_json:
        value = json.loads(out.stdout)
        assert value["ok"] is True
        assert value["worker_id"] == A and value["worker"] == f"claude:{A}"
        assert value["request_id"] == gate["id"]
        assert value["input_mode"] == "native-input"
        assert value["original_gate_answered"] is True
        assert value["settled"] is True and value["queued"] is False
        assert value["turn_ref"].startswith("ccx-turn:claude:v1:")
        assert value["reply"] == "Fake Claude completed: " + prompt
    else:
        assert "Fake Claude completed: " + prompt in out.stdout
        assert f"Worker: claude:{A}" in out.stdout
        assert "input accepted" in out.stdout
        assert "former gate NOT answered" not in out.stdout
    assert store.record(A)["lastSettledTurn"]["outcome"] == "completed"
    assert store.record(A).get("waitingFor") is None


def test_real_cli_no_wait_json_reports_input_acceptance_not_fake_completion(
    generated_hook_host,
):
    store, call, _env, _log = generated_hook_host
    sent = call("send", A, "ask generated question", "--no-wait",
                "--delivery-id", "d3-question-no-wait", "--json")
    assert sent.returncode == 0, (sent.stdout, sent.stderr)
    gate = _brokered(store, "question")
    result = call("answer", A, "Yes", "--no-wait", "--json")
    assert result.returncode == 0, (result.stdout, result.stderr)
    value = json.loads(result.stdout)
    assert value["worker"] == f"claude:{A}"
    assert value["request_id"] == gate["id"]
    assert value["input_mode"] == "native-input"
    assert value["original_gate_answered"] is True
    assert value["queued"] is True
    assert value["settled"] is False
    assert value["reply"] is None and value["turn_ref"] is None


def test_cli_fallback_new_message_json_and_plain_never_claim_old_gate(
    monkeypatch, capsys,
):
    from ccx import client as client_module
    from ccx.cli.parser import build_parser

    calls = []

    class OriginalClient:
        def answer(self, target, text, *, request_id, options, text_answers,
                   wait, timeout, fallback_to_send):
            calls.append((target, request_id, fallback_to_send, wait, timeout))
            return SendResult(
                short=A[:8], worker_id=A, engine="claude",
                delivery_id="separate-original-send", delivery_status="accepted",
                queued=False, settled=False, state="busy",
            )

    monkeypatch.setattr(client_module, "CcxClient", OriginalClient)
    json_args = build_parser().parse_args([
        "answer", A, "Yes", "--request-id", "original-dead-gate", "--json",
    ])
    commands_turns.cmd_answer(json_args)
    value = json.loads(capsys.readouterr().out)
    assert value["ok"] is True
    assert value["worker"] == f"claude:{A}"
    assert value["request_id"] == "original-dead-gate"
    assert value["delivery_id"] == "separate-original-send"
    assert value["input_mode"] == "new-message-fallback"
    assert value["original_gate_answered"] is False
    assert value["reply"] is None and value["settled"] is False
    plain_args = build_parser().parse_args([
        "answer", A, "Yes", "--request-id", "original-dead-gate",
    ])
    commands_turns.cmd_answer(plain_args)
    plain = capsys.readouterr().out
    assert "NEW user message delivered" in plain
    assert "former gate NOT answered" in plain
    assert "Delivery: separate-original-send" in plain
    assert calls == [
        (A, "original-dead-gate", True, False, None),
        (A, "original-dead-gate", True, False, None),
    ]


def test_cli_lost_ack_never_falls_back(
    monkeypatch, capsys,
):
    from ccx import client as client_module
    from ccx.cli.parser import build_parser

    effect_count = []

    class LostAck:
        def answer(self, target, text, **kwargs):
            effect_count.append((target, kwargs))
            raise SessionUnavailable("unreachable after native answer sendall")
        def send(self, *args, **kwargs):
            pytest.fail("lost native input ACK must never become another send")

    monkeypatch.setattr(client_module, "CcxClient", LostAck)
    args = build_parser().parse_args([
        "answer", A, "Yes", "--request-id", "original-bound-gate", "--wait",
    ])
    with pytest.raises(SystemExit) as error:
        commands_turns.cmd_answer(args)
    assert error.value.code == 3
    assert len(effect_count) == 1
    assert effect_count[0][1]["fallback_to_send"] is True
    assert "unreachable after native answer" in capsys.readouterr().err
