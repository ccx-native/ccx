"""Proof only: a CLI same-token SEND retry reports ITS original R (fake native)."""
from __future__ import annotations

import json

import pytest

from test_generation_store import A, B, short_root  # noqa: F401 - pytest fixture
from test_claude_native_cli import generated_hook_host  # noqa: F401 - pytest fixture
from test_codex_native_effects import _f2_fake_processes


@pytest.mark.parametrize("engine", ["claude", "codex"])
def test_cli_same_token_retry_reports_original_turn(
    engine, request, short_root, tmp_path, monkeypatch,
):
    if engine == "claude":
        _store, call, _env, _hooks = request.getfixturevalue("generated_hook_host")
        target, prefix = A, ("send",)
        log = tmp_path / "native-stdin.log"
        effects = lambda: log.read_bytes().count(b"CLI ORIGINAL R ")  # noqa: E731
        _run(call, target, prefix, effects, engine)
        return
    with _f2_fake_processes(short_root, tmp_path, monkeypatch) as (
        call, _restart, trace, _store, _rpc,
    ):
        made = call("new", "--engine", "codex", "--cwd", str(tmp_path), "--worker-id", B, "--json")
        assert made.returncode == 0, (made.stdout, made.stderr)
        assert json.loads(made.stdout)["native_ready"] is True
        effects = lambda: trace.read_text().count("RPC turn/start")  # noqa: E731
        _run(call, B, ("send",), effects, engine)


def _run(call, target, prefix, effects, engine):
    def send(text, token):
        done = call(*prefix, target, text, "--delivery-id", token,
                    "--timeout", "20", "--json")
        assert done.returncode == 0, (done.stdout, done.stderr)
        return json.loads(done.stdout)

    first = send("CLI ORIGINAL R ONE", "cli-original-r1")
    second = send("CLI ORIGINAL R TWO", "cli-original-r2")
    for value, token in ((first, "cli-original-r1"), (second, "cli-original-r2")):
        assert value["settled"] is True and value["delivery_id"] == token, value
        assert value["report"]["delivery_id"] == token, value
    assert first["delivery"]["turn_ref"] != second["delivery"]["turn_ref"]
    assert effects() == 2

    again = send("CLI ORIGINAL R ONE", "cli-original-r1")
    observed = {
        "engine": engine,
        "retry_delivery_id": again["delivery_id"],
        "retry_delivery_turn_ref": again["delivery"]["turn_ref"],
        "first_turn_ref": first["delivery"]["turn_ref"],
        "report_delivery_id": again["report"]["delivery_id"],
        "report_turn_ref": again["report"]["turn_ref"],
        "report_text": again["report"]["text"],
        "first_report_text": first["report"]["text"],
        "second_report_text": second["report"]["text"],
        "native_effects": effects(),
    }
    print("CLI_ORIGINAL_R_OBSERVED " + json.dumps(observed, sort_keys=True), flush=True)
    assert again["settled"] is True and again["delivery_id"] == "cli-original-r1"
    assert again["delivery"]["turn_ref"] == first["delivery"]["turn_ref"]
    assert effects() == 2  # replay: no new native effect
    assert again["report"]["delivery_id"] == "cli-original-r1", observed
    assert again["report"]["turn_ref"] == first["delivery"]["turn_ref"], observed
    assert again["report"]["text"] == first["report"]["text"], observed
