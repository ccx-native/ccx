"""CLI SEND durable variables on ORIGINAL full-UUID native delivery (fake only)."""
from __future__ import annotations
import ccx.cli.commands.turns as commands_turns
import ccx.ops.send as ops_send

from argparse import Namespace
import hashlib
import json

import pytest

from ccx.claude import delivery as claude_delivery
from ccx.cli import main as cli
from ccx.cli import codex as codex_commands
from ccx.codex import delivery as codex_delivery
from ccx.cli.commands import _common as commands
from ccx import constants
from ccx.controllers import live_vars
from ccx.notify import registry as parent_notify
from ccx.ops import common as protocol_ops
from ccx.ops.creation_steps import prepare_worker
from ccx.store.root import require_ccx_active
from test_generation_store import A, B, C, short_root  # noqa: F401 - pytest fixture imported
from test_delivery_flow import _activate_fake_claude
from test_notify_registration import originals as codex_originals  # noqa: F401 - pytest fixture
from test_notify_claude import owners, _fake_original_response  # noqa: F401 - pytest fixture


def _claude_args(wid, token, *, variables=None, source=None, text="ordinary send"):
    return Namespace(
        target="claude:" + wid, text=text, stdin=False, file=None,
        delivery_id=token, notify_parent=False, timeout=9,
        var=variables, vars_from=source, json=True, no_wait=True, wait=False,
        cap_5h=None, cap_7d=None, cap_opus=None,
    )


def test_cli_claude_durable_merge_original_token_mutable_retry_and_native_unknown(
    owners, monkeypatch, capsys,
):
    root, store = owners
    original_send = ops_send.send_turn
    native_calls = []

    def native_submit(handle, sid, text, token, _budget):
        # This is the LOWER original native-effect seam; the semantic receipt
        # and globally fenced dispatch still execute in real protocol_ops.
        assert handle == "claude:" + A
        assert sid == store.record(A)["native"]["sessionId"]
        assert store.variables_info(handle, name="counter")["value"] in (1, 7, 9)
        assert "CCX variables available in direct python/python3: counter" in text
        assert text.count("[CCX variables available") == 1
        assert store.read_receipt(handle, "claude-delivery", token)["state"] == "submitted-unknown"
        native_calls.append((token, text))
        if token == "cli-send-original-unknown":
            raise TimeoutError("fake original native acknowledgement lost")
        return _fake_original_response(store, token, text)

    def original_with_fake_native(*args, **kwargs):
        return original_send(*args, _native_submit=native_submit, **kwargs)

    monkeypatch.setattr(ops_send, "send_turn", original_with_fake_native)
    token = "cli-send-original-once"
    cli._main([
        "send", "claude:" + A, "use counter",
        "--var", "counter=1", "--cap-5h", "35",
        "--delivery-id", token, "--no-wait", "--json",
    ])
    output = json.loads(capsys.readouterr().out)
    assert output["worker"] == "claude:" + A
    assert output["delivery_id"] == token and output["status"] == "accepted"
    assert output["settled"] is False and "notify_parent" not in output
    receipt = claude_delivery.read_receipt("claude:" + A, token)
    assert receipt["cap"] == {"5h": 35}
    assert receipt["promptSha256"] == hashlib.sha256(native_calls[0][1].encode("utf-8")).hexdigest()
    assert receipt["promptText"] is None  # accepted receipt keeps identity, not payload
    before_state = store.variables_info("claude:" + A)["state_id"]
    assert before_state and len(native_calls) == 1

    # A retry of an already-admitted token must not republish mutable values.
    cli._main([
        "send", "claude:" + A, "use counter", "--var", "counter=7",
        "--cap-5h", "35", "--delivery-id", token, "--no-wait", "--json",
    ])
    again = json.loads(capsys.readouterr().out)
    assert again["delivery_id"] == token and again["status"] == output["status"]
    assert store.variables_info("claude:" + A, name="counter")["value"] == 1
    assert store.variables_info("claude:" + A)["state_id"] == before_state
    assert len(native_calls) == 1

    # This new VALID send loses its native acknowledgement. Publication is
    # truthful and retained, while the original token is indeterminate.
    failed = "cli-send-original-unknown"
    with pytest.raises(SystemExit) as uncertain:
        cli._main([
            "send", "claude:" + A, "use counter", "--var", "counter=9",
            "--delivery-id", failed, "--no-wait", "--json",
        ])
    assert uncertain.value.code == 6
    assert failed in capsys.readouterr().err
    assert store.variables_info("claude:" + A, name="counter")["value"] == 9
    assert store.variables_info("claude:" + A)["state_id"] == before_state
    assert len(native_calls) == 2
    assert store.read_receipt("claude:" + A, "claude-delivery", failed)["state"] == "submitted-unknown"
    assert parent_notify.iter_registrations() == []
    assert not (root / "variables").exists()  # only ACTIVE generation storage
    with capsys.disabled():
        print("FAKE_CLAUDE_ORIGINAL_EFFECT_TRACE " + repr(native_calls), flush=True)


def test_cli_codex_vars_from_claude_prefix_reuses_state_and_native_thread(
    codex_originals, monkeypatch, capsys, tmp_path,
):
    store, trace, _native = codex_originals
    # The accepted real fake Codex app-server owns distinct native A/B threads.
    # The source is a globally owned Claude worker, not a native/provider alias.
    prepare_worker(store, engine="claude", cwd=str(tmp_path), worker_id=C)
    live_vars.publish_attachment(
        live_vars.prepare_durable_attachment({"evidence": [1, 2]}), "claude", C,
    )
    source = store.variables_info("claude:" + C)
    own_thread = store.record(B)["native"]["threadId"]
    sibling_thread = store.record(A)["native"]["threadId"]
    assert own_thread != sibling_thread
    before = trace.read_text().count("RPC turn/start")
    token = "cli-codex-cross-engine-share"

    # Real argparse + all-fields prefix preflight → exactly owned source,
    # followed by the ORIGINAL daemon/native Codex SEND, no mocked send_turn.
    cli._main([
        "send", "codex:" + B, "inspect evidence",
        "--vars-from", "claude:" + C[:8],
        "--delivery-id", token, "--no-wait", "--json",
    ])
    result = json.loads(capsys.readouterr().out)
    assert result["worker"] == "codex:" + B
    assert result["delivery_id"] == token and result["status"] == "accepted"
    # A fast native completion may already have settled the accepted opening
    # receipt before the no-wait CLI returns; neither path changes the send.
    if result["settled"]:
        proof = result["delivery"]["settlement"]
        assert proof["status"] == "completed" and proof["deliveryId"] == token
        assert result["report"]["delivery_id"] == token
        assert result["report"]["text"].startswith("Fake Codex completed:")
    else:
        assert "report" not in result
    assert "notify_parent" not in result
    target = store.variables_info("codex:" + B)
    assert target["mode"] == "durable"
    assert target["state_id"] == source["state_id"]
    assert target["names"] == ["evidence"]
    assert store.variables_info("codex:" + B, name="evidence")["value"] == [1, 2]
    receipt = codex_delivery.read_receipt("codex:" + B, token)
    assert receipt["clientUserMessageId"]
    assert receipt["turnId"]
    after = trace.read_text()
    assert after.count("RPC turn/start") == before + 1
    final_turn = after.split("RPC turn/start")[-1].split("RPC ", 1)[0]
    assert own_thread in final_turn and sibling_thread not in final_turn
    assert "inspect evidence" in final_turn
    assert "CCX variables available in direct python/python3: evidence" in final_turn
    assert final_turn.count("[CCX variables available") == 1
    assert store.record(B)["native"]["threadId"] == own_thread
    assert parent_notify.iter_registrations() == []
    with capsys.disabled():
        print("FAKE_CODEX_ORIGINAL_TURN_TRACE " + final_turn.strip(), flush=True)


def test_cli_send_invalid_before_publication_and_terminal_claude_can_attach(
    short_root, monkeypatch, tmp_path, capsys,
):
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    store = require_ccx_active(short_root)
    _activate_fake_claude(
        store, prepare_worker(store, engine="claude", cwd=str(tmp_path), worker_id=A),
    )
    prepare_worker(store, engine="codex", cwd=str(tmp_path), worker_id=B,
                   permissions="restricted")
    prepare_worker(store, engine="claude", cwd=str(tmp_path), worker_id=C)
    original_send = ops_send.send_turn
    native_effects = []

    def native_submit(handle, sid, text, token, _budget):
        assert (handle, sid) == (
            "claude:" + A, store.record(A)["native"]["sessionId"],
        )
        assert "CCX variables available" in text
        native_effects.append(token)
        return _fake_original_response(store, token, text)

    def original_with_fake_native(*args, **kwargs):
        return original_send(*args, _native_submit=native_submit, **kwargs)

    monkeypatch.setattr(ops_send, "send_turn", original_with_fake_native)

    def rejected(call, args, expected_code, phrase, receipt_target, token):
        with pytest.raises(SystemExit) as error:
            call(args)
        assert error.value.code == expected_code
        assert phrase in capsys.readouterr().err
        assert store.variables_info("claude:" + A)["mode"] is None
        assert store.variables_info("claude:" + C)["mode"] is None
        assert store.variables_info("codex:" + B)["mode"] is None
        if "\x00" not in token:
            if receipt_target == "codex:" + B:
                assert codex_delivery.read_receipt(receipt_target, token) is None
            else:
                assert claude_delivery.read_receipt(receipt_target, token) is None
        assert native_effects == []
        assert parent_notify.iter_registrations() == []

    rejected(
        commands_turns.cmd_send,
        _claude_args(A, "cli-xor", variables=[], source="claude:" + C),
        1, "mutually exclusive", "claude:" + A, "cli-xor",
    )
    from test_notify_registration import _n1c_cli_send_args
    bad_token = _n1c_cli_send_args("invalid\x00token", notify=False)
    bad_token.no_wait = True
    bad_token.var = ["evidence=1"]
    rejected(
        codex_commands.cmd_send, bad_token, 1,
        "delivery_id", "codex:" + B, "invalid\x00token",
    )
    restricted = _n1c_cli_send_args("cli-restricted", notify=False)
    restricted.no_wait = True
    restricted.var = ["evidence=1"]
    rejected(
        codex_commands.cmd_send, restricted, 1,
        "full permissions", "codex:" + B, "cli-restricted",
    )

    # Current live Claude host belongs to this UUID but no bridge marker was
    # published for that PID; publication fails before any receipt/native.
    store.mutate_record("claude:" + A, patches={
        "lifecycle.state": "idle", "lifecycle.hostPid": 77113,
    })
    rejected(
        commands_turns.cmd_send,
        _claude_args(A, "cli-no-bridge", variables=["evidence=2"]),
        3, "predates", "claude:" + A, "cli-no-bridge",
    )
    rejected(
        commands_turns.cmd_send,
        _claude_args(C, "cli-no-source", source="codex:" + B),
        1, "no durable variables", "claude:" + C, "cli-no-source",
    )

    # A later explicit SEND for a terminal managed original may attach even
    # without that old live-host marker. Its original native effect still uses
    # the accepted full UUID; the fake lower effect represents resumed delivery.
    store.mutate_record("claude:" + A, patches={"lifecycle.state": "dead"})
    commands_turns.cmd_send(_claude_args(
        A, "cli-terminal-reattach", variables=["evidence=3"],
    ))
    output = json.loads(capsys.readouterr().out)
    assert output["delivery_id"] == native_effects[0] == "cli-terminal-reattach"
    assert output["worker"] == "claude:" + A
    assert store.variables_info("claude:" + A, name="evidence")["value"] == 3
    assert len(native_effects) == 1
    assert parent_notify.iter_registrations() == []
    with capsys.disabled():
        print("FAKE_TERMINAL_CLAUDE_ORIGINAL_EFFECT_TRACE " + repr(native_effects), flush=True)
