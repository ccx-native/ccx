"""Original CLI promptless full-UUID fork, through physical fake native engines."""
from __future__ import annotations
import ccx.notify.daemon as notify_daemon
import ccx.ops.send as ops_send

from contextlib import nullcontext
import json
import os
from pathlib import Path
import subprocess
import sys
from uuid import UUID

import pytest

from ccx.cli import main as cli_module
from ccx import constants
from ccx.ops import forking
from test_generation_store import A, B, C, short_root as short_root  # noqa: F401
from test_fork_host import _stop_fake
from test_fork_public import _claude_environment, _codex_environment
from test_fork_source import sources as sources  # noqa: F401


def _cli(*argv):
    """Original CLI entrypoint in a disposable fake-only subprocess."""
    repo = Path(__file__).resolve().parent.parent
    return subprocess.run(
        [sys.executable, "-B", "-m", "ccx", *argv],
        cwd=repo, env={**os.environ, "CCX_ROOT": str(constants.CCX_ROOT),
                       "PYTHONPATH": str(repo)},
        capture_output=True, text=True, timeout=25,
    )


@pytest.mark.parametrize("engine", ["claude", "codex"])
def test_cli_promptless_native_fork_lineage_peek_and_source_free_retry(
    sources, tmp_path, monkeypatch, capsys, engine,
):
    store, original_transcript, _native_source_reads = sources
    source = A if engine == "claude" else B
    full = f"{engine}:{source}"
    prefix = source[:10]  # The two owned sources share eight chars, not nine.
    before = store.record(source)
    before_transcript = original_transcript.read_bytes()
    overrides = (("--name", "child fork", "--model", "child-model", "--effort", "low")
                 if engine == "codex" else ())
    initial = prefix if engine == "claude" else full
    alternate = full if engine == "claude" else prefix
    trace = _claude_environment(tmp_path, monkeypatch) if engine == "claude" else None
    runtime = (_codex_environment(store, tmp_path, monkeypatch)
               if engine == "codex" else nullcontext())

    try:
        with runtime as native:
            if engine == "codex":
                trace = native[2]
            first = _cli("fork", initial, "--worker-id", C, "--timeout", "25",
                         "--json", *overrides)
            assert first.returncode == 0, (first.stdout, first.stderr)
            result = json.loads(first.stdout)
            assert set(result) == {
                "worker", "worker_id", "engine", "short", "native_source",
                "native_ready", "created", "reconciled", "ok", "name", "cwd",
                "forked_from",
            }
            assert result["ok"] is True and result["native_ready"] is True
            assert result["created"] is True and result["reconciled"] is False
            assert result["worker"] == f"{engine}:{C}" and result["worker_id"] == C
            assert result["engine"] == engine and result["short"] == C[:8]
            assert result["cwd"] == str(tmp_path)
            child = store.record(C)
            assert result["name"] == ("child fork" if engine == "codex"
                                      else before["name"] + " (fork)")
            assert child["name"] == result["name"]
            assert child["createArgs"]["model"] == (
                "child-model" if engine == "codex" else "sonnet")
            assert child["createArgs"]["effort"] == (
                "low" if engine == "codex" else "medium")
            assert child["forkAnchor"]["sourceWorkerId"] == source
            assert store.native_activation(C)["state"] == "confirmed"
            assert store.record(source)["createArgs"] == before["createArgs"]
            assert store.record(source)["native"] == before["native"]
            assert original_transcript.read_bytes() == before_transcript

            source_native_key = "sessionId" if engine == "claude" else "threadId"
            child_native = child["native"][source_native_key]
            source_native = before["native"][source_native_key]
            assert child_native not in {source_native, C, source}
            assert result["forked_from"] == forking.public_lineage(child)
            assert result["forked_from"]["worker"] == full
            assert result["forked_from"]["worker_id"] == source
            assert result["forked_from"]["session_id" if engine == "claude"
                                         else "thread_id"] == source_native
            assert result["native_source"] == (
                f"claude-session:{child_native}" if engine == "claude"
                else f"ccx-worker:{C}")

            peek = _cli("peek", full, C, "--status", "--json")
            assert peek.returncode == 0, (peek.stdout, peek.stderr)
            source_row, child_row = json.loads(peek.stdout)["statuses"]
            assert source_row["worker"] == full and source_row["forked_from"] is None
            assert child_row["worker"] == f"{engine}:{C}"
            assert child_row["forked_from"] == result["forked_from"]

            # Alternate textual spelling resolves only at CLI preflight, not
            # inside the backend; one immutable child and no native replay.
            plain = _cli("fork", alternate, "--worker-id", C, "--no-wait",
                         "--timeout", "25", *overrides)
            assert plain.returncode == 0, (plain.stdout, plain.stderr)
            assert plain.stdout.strip() == f"{engine}:{C}"

            store.mutate_record(source, patches={
                "lifecycle.state": "dead", "createArgs.model": "moved source",
            })
            # The full-handle retry must NOT even try to select a new native
            # source point; call the real CLI parser in-process to deny it.
            with monkeypatch.context() as denied:
                reader = "_claude_point" if engine == "claude" else "_codex_point"
                denied.setattr(
                    forking, reader,
                    lambda *_args, **_kwargs: pytest.fail("retry reread moving source"),
                )
                capsys.readouterr()
                cli_module._main([
                    "fork", full, "--worker-id", C, "--json", *overrides,
                ])
                replay = json.loads(capsys.readouterr().out)
            assert replay["ok"] and not replay["created"] and replay["reconciled"]
            assert replay["forked_from"] == result["forked_from"]
            assert store.record(C)["native"][source_native_key] == child_native

            assert trace is not None
            evidence = trace.read_text()
            if engine == "claude":
                assert [json.loads(line)["kind"] for line in evidence.splitlines()] == [
                    "copy", "pty",
                ]
                paste = tmp_path / "f3a-pty-input.log"
                assert not paste.exists() or paste.read_bytes() == b""
            else:
                assert evidence.count("RPC thread/fork ") == 1
                assert "RPC thread/start " not in evidence
                assert "RPC turn/start " not in evidence
    finally:
        if engine == "claude":
            _stop_fake(constants.CCX_ROOT, C)


def test_cli_fork_invalid_options_bad_ids_and_stopped_ambiguity_prechild(
    sources, tmp_path, monkeypatch, capsys,
):
    """Real parser/CLI refuses invalid intent without a new owner or native child."""
    from ccx.cli import commands
    from ccx.notify import registry as parent_notify
    from test_notify_ext import native_caller

    store, original_transcript, _ = sources
    before = original_transcript.read_bytes()
    prior = {row["worker"] for row in store.list_workers(limit=None)}
    binary = tmp_path / "present-fake-claude"
    binary.write_text("#!/bin/sh\nexit 1\n")
    binary.chmod(0o700)
    monkeypatch.setattr(constants, "CLAUDE_BIN", str(binary))
    monkeypatch.setenv("CCX_CLAUDE_BIN", str(binary))
    # Every cap CLI path patches usage in this process: never query an OAuth account.
    monkeypatch.setattr("ccx.cli.commands.create.fetch_usage", lambda **_kw: {
        "five_hour": {"utilization": 0.0}, "seven_day": {"utilization": 0.0},
    })
    invalid = (
        (A, ("",), 1, "nonblank"),
        (A, ("start", "--file", str(tmp_path / "unused")), 1, "one prompt source"),
        (A, ("--file", str(tmp_path / "unreadable")), 1, "failed to read"),
        (A, ("--wait",), 1, "needs a prompt"),
        (A, ("--delivery-id", "create:initial"), 1, "with a prompt"),
        (A, ("initial", "--delivery-id", "different"), 1, "create:initial"),
        (A, ("--cap-5h", "0"), 1, "initial cap requires a prompt"),
        (B, ("initial", "--cap-opus", "40"), 1, "Claude-only"),
        (A, ("--settle",), 1, "Codex worker"),
        (A, ("--var", "x=1", "--vars-from", B), 1, "mutually exclusive"),
        (A, ("--var", "x={bad"), 1, "valid JSON"),
        (A, ("--var", "x=1"), 1, "full permissions"),
    )
    for source, flags, exit_code, detail in invalid:
        with pytest.raises(SystemExit) as caught:
            cli_module._main(["fork", source, *flags, "--worker-id", C, "--json"])
        observed = capsys.readouterr()
        assert caught.value.code == exit_code, (flags, observed)
        assert detail in observed.err, (flags, observed)
        assert store._owner(C) is None and store._claim(C) is None

    # A Claude child cannot request recursive settlement, even with a valid
    # caller-owned interactive Codex return address. Refuse before native fork.
    monkeypatch.setattr(notify_daemon, "ensure_notify_daemon", lambda **_kw: None)
    native_caller(monkeypatch, "fork-invalid-recursive-parent")
    with pytest.raises(SystemExit) as blocked:
        cli_module._main(["fork", A, "--worker-id", C, "--notify-parent", "--settle"])
    assert blocked.value.code == 1
    assert "Codex worker" in capsys.readouterr().err
    assert store._owner(C) is None

    # Main/NEW also pre-refuse already tripped Claude caps before claiming a
    # child. This needs the real helper, but never the real account sampler.
    monkeypatch.setattr("ccx.cli.commands.create.fetch_usage", lambda **_kw: {
        "five_hour": {"utilization": 75.0},
    })
    with pytest.raises(SystemExit) as tripped:
        cli_module._main(["fork", A, "initial", "--cap-5h", "30", "--worker-id", C])
    assert tripped.value.code == 7
    assert "no worker created" in capsys.readouterr().err
    assert store._owner(C) is None

    for timeout in ("0", "nan"):
        response = _cli("fork", A, "--worker-id", C, "--timeout", timeout)
        assert response.returncode == 1
        assert "--timeout" in response.stderr
        assert store._owner(C) is None
    bad = _cli("fork", A, "--worker-id", C[:8], "--json")
    assert bad.returncode == 1 and "UUID" in bad.stderr
    assert store._owner(C) is None

    # Both ambiguous fields use the same full, untruncated inventory, including
    # a stopped source. This is never an instruction to create a child.
    store.mutate_record(B, patches={"lifecycle.state": "idle-killed"})
    for options in (("aaaaaaaa",), (A, "--vars-from", "aaaaaaaa")):
        ambiguous = _cli("fork", *options, "--worker-id", C, "--json")
        assert ambiguous.returncode == 1
        assert f"claude:{A}" in ambiguous.stderr
        assert f"codex:{B}" in ambiguous.stderr
        assert "ambiguous" in ambiguous.stderr.lower()
        assert store._owner(C) is None
    assert {row["worker"] for row in store.list_workers(limit=None)} == prior
    assert original_transcript.read_bytes() == before


def test_cli_fork_lost_codex_reply_keeps_original_minted_child(sources, tmp_path, monkeypatch):
    store, _transcript, _source_reads = sources
    with _codex_environment(
        store, tmp_path, monkeypatch, lost_fork_reply=True,
    ) as (cli, _restart, trace, _store, _rpc):
        first = cli("fork", B, "--json", "--timeout", "3")
        assert first.returncode != 0, (first.stdout, first.stderr)
        partial = json.loads(first.stdout)
        assert partial["ok"] is False and partial["code"] == "partial_failure"
        assert partial["phase"] == "native-activation"
        assert partial["native_ready"] is False
        child = partial["worker_id"]
        assert str(UUID(child, version=4)) == child and len(child) == 36
        assert partial["worker"] == f"codex:{child}"
        assert store._owner(child) is not None
        assert trace.read_text().count("RPC thread/fork ") == 1
        assert "RPC thread/start " not in trace.read_text()

        store.mutate_record(B, patches={
            "lifecycle.state": "dead", "createArgs.model": "later source",
        })
        again = cli("fork", f"codex:{B}", "--worker-id", child,
                    "--json", "--timeout", "1")
        assert again.returncode != 0, (again.stdout, again.stderr)
        repeated = json.loads(again.stdout)
        assert repeated["ok"] is False and repeated["worker_id"] == child
        assert trace.read_text().count("RPC thread/fork ") == 1
        assert "RPC thread/start " not in trace.read_text()

        conflict = cli("fork", f"codex:{B}", "--worker-id", child,
                       "--model", "different", "--json")
        assert conflict.returncode == 5, (conflict.stdout, conflict.stderr)
        assert "another original creation/fork" in conflict.stderr
        assert trace.read_text().count("RPC thread/fork ") == 1
        assert "RPC thread/start " not in trace.read_text()


@pytest.mark.parametrize("engine", ["claude", "codex"])
def test_cli_fork_initial_prompt_wait_exact_original_report(
    sources, tmp_path, monkeypatch, capsys, engine,
):
    """Only the CLI first-turn flags are new; accepted backend owns native truth."""
    from ccx.claude import delivery as claude_delivery
    from ccx.codex import delivery as codex_delivery

    store, original_transcript, _ = sources
    source = A if engine == "claude" else B
    before = store.record(source)
    before_bytes = original_transcript.read_bytes()
    prompt = f"CLI first fork delivery {engine} original"
    # The real sampler may contact an OAuth account. No test may call it.
    monkeypatch.setattr("ccx.cli.commands.create.fetch_usage", lambda **_kw: {
        "five_hour": {"utilization": 0.0},
    })
    if engine == "claude":
        options = [prompt, "--cap-5h", "99"]
        trace = _claude_environment(tmp_path, monkeypatch)
        runtime = nullcontext()
    else:
        prompt_file = tmp_path / "fork-original-prompt.txt"
        prompt_file.write_text(prompt)
        options = ["--file", str(prompt_file), "--delivery-id", "create:initial"]
        trace = None
        runtime = _codex_environment(store, tmp_path, monkeypatch)
    try:
        with runtime as native:
            if engine == "codex":
                trace = native[2]
            cli_module._main([
                "fork", f"{engine}:{source}", *options, "--worker-id", C,
                "--wait", "--timeout", "25", "--json",
            ])
            result = json.loads(capsys.readouterr().out)
            assert result["ok"] is True and result["native_ready"] is True
            assert result["worker"] == f"{engine}:{C}"
            assert result["worker_id"] == C and result["created"] is True
            assert result["forked_from"]["worker"] == f"{engine}:{source}"
            assert result["delivery_id"] == "create:initial"
            assert result["waited"] is True and result["turn_ref"]
            assert result["report"]["delivery_id"] == "create:initial"
            assert result["report"]["turn_ref"] == result["turn_ref"]
            assert result["report"]["text"] == f"Fake {engine.title()} completed: {prompt}"
            cli_module._main([
                "fork", f"{engine}:{source}", *options, "--worker-id", C,
                "--wait", "--timeout", "25",
            ])
            assert capsys.readouterr().out == (
                f"worker: {engine}:{C}\nFake {engine.title()} completed: {prompt}\n"
            )
            assert store.record(source) == before
            assert original_transcript.read_bytes() == before_bytes
            child = store.record(C)
            key = "sessionId" if engine == "claude" else "threadId"
            assert child["native"][key] not in {C, source, before["native"][key]}
            assert trace is not None
            if engine == "claude":
                receipt = claude_delivery.read_receipt(f"claude:{C}", "create:initial")
                assert receipt["cap"] == {"5h": 99.0}
                assert [json.loads(line)["kind"] for line in trace.read_text().splitlines()] == [
                    "copy", "pty",
                ]
                assert (tmp_path / "f3a-pty-input.log").read_bytes().count(prompt.encode()) == 1
            else:
                receipt = codex_delivery.read_receipt(f"codex:{C}", "create:initial")
                wire = trace.read_text()
                assert wire.count("RPC thread/fork ") == 1
                assert wire.count("RPC turn/start ") == 1
                assert "RPC thread/start " not in wire
            assert receipt["deliveryId"] == "create:initial"
    finally:
        if engine == "claude":
            _stop_fake(constants.CCX_ROOT, C)


def test_cli_fork_codex_durable_then_prefix_shared_variables(
    sources, tmp_path, monkeypatch, capsys,
):
    """Existing CLI vars preparation + L1b first bind on two real native forks."""
    from ccx.controllers import live_vars
    from test_fork_python_options import (
        FULL_SOURCE, SHARED_CHILD, _full_codex_source,
    )
    from test_create_variable_binding import _receipt

    store, _, _ = sources
    with _full_codex_source(store, tmp_path, monkeypatch) as (_cli, _restart, trace, _, _rpc):
        prompt = "Use the first CLI fork counter"
        cli_module._main([
            "fork", FULL_SOURCE, prompt, "--worker-id", C,
            "--var", "counter=1", "--json",
        ])
        first = json.loads(capsys.readouterr().out)
        assert first["ok"] and first["worker"] == f"codex:{C}"
        assert first["delivery_id"] == "create:initial"
        assert first["forked_from"]["worker"] == f"codex:{FULL_SOURCE}"
        assert _receipt(store, "codex", C)["names"] == ["counter"]
        first_info = store.variables_info(C)
        state_id = first_info["state_id"]
        assert first_info["mode"] == "durable" and state_id
        assert store.variables_info(C, name="counter")["value"] == 1
        assert store.variables_info(FULL_SOURCE)["mode"] is None

        cli_module._main([
            "fork", f"codex:{FULL_SOURCE}", "--worker-id", SHARED_CHILD,
            "--vars-from", C[:8], "--json",
        ])
        second = json.loads(capsys.readouterr().out)
        assert second["ok"] and second["worker"] == f"codex:{SHARED_CHILD}"
        assert "delivery_id" not in second and "report" not in second
        assert store.variables_info(SHARED_CHILD)["state_id"] == state_id
        assert _receipt(store, "codex", SHARED_CHILD)["names"] == ["counter"]
        assert live_vars.read_sidecar("codex", SHARED_CHILD)["stateId"] == state_id

        live_vars.publish_attachment(
            live_vars.prepare_durable_attachment({"counter": 7}), "codex", C,
        )
        assert store.variables_info(SHARED_CHILD, name="counter")["value"] == 7
        cli_module._main([
            "fork", f"codex:{FULL_SOURCE}", "--worker-id", SHARED_CHILD,
            "--vars-from", C[:8], "--json",
        ])
        replay = json.loads(capsys.readouterr().out)
        assert replay["ok"] and replay["reconciled"] and not replay["created"]
        assert store.variables_info(C, name="counter")["value"] == 7
        assert store.variables_info(SHARED_CHILD)["state_id"] == state_id
        wire = trace.read_text()
        assert wire.count("RPC thread/fork ") == 2
        assert wire.count("RPC turn/start ") == 1
        assert "RPC thread/start " not in wire
        assert wire.count("CCX variables available") == 1


@pytest.mark.parametrize("engine", ["codex", "claude"])
def test_cli_fork_existing_original_parent_receipts_not_rearmed(
    sources, tmp_path, monkeypatch, capsys, engine,
):
    """First-turn SEND and promptless stable are separate existing notify families."""
    from ccx.claude import delivery as claude_delivery
    from ccx.codex import delivery as codex_delivery
    from ccx.notify import registry as parent_notify
    from test_notify_registration import _n1c_managed_parent
    from test_notify_ext import native_caller

    store, _, _ = sources
    monkeypatch.setattr(notify_daemon, "ensure_notify_daemon", lambda **_kw: None)
    if engine == "codex":
        _n1c_managed_parent(monkeypatch, store, B)
        prompt = "original fork turn with parent settlement"
        options = [prompt, "--notify-parent", "--settle"]
        trace = None
        runtime = _codex_environment(store, tmp_path, monkeypatch)
        source = B
        parent = {"kind": "ccx-codex", "workerId": B,
                  "threadId": store.record(B)["native"]["threadId"],
                  "generation": store.generation}
    else:
        parent = native_caller(monkeypatch, "original-interactive-fork-return")
        options = ["--notify-parent"]
        trace = _claude_environment(tmp_path, monkeypatch)
        runtime = nullcontext()
        source = A
    try:
        with runtime as native:
            if engine == "codex":
                trace = native[2]
            args = ["fork", f"{engine}:{source}", *options, "--worker-id", C, "--json"]
            cli_module._main(args)
            first = json.loads(capsys.readouterr().out)
            assert first["ok"] and first["worker"] == f"{engine}:{C}"
            event = first["notify_parent"]
            eid = event["event_id"]
            row = parent_notify.read_registration(eid)
            assert row["parent"] == parent and row["worker"]["workerId"] == C
            assert len([r for r in parent_notify.iter_registrations(include_terminal=True)
                        if r["worker"]["workerId"] == C]) == 1
            if engine == "codex":
                assert first["delivery_id"] == "create:initial"
                assert first["delivery_status"] in {"accepted", "owned"}
                assert event["source"] == "send" and event["settle"] is True
                assert row["operation"]["deliveryId"] == "create:initial"
                assert row["settlementMode"] == "orchestration"
                assert codex_delivery.read_receipt(f"codex:{C}", "create:initial") is not None
            else:
                assert "delivery_id" not in first and "report" not in first
                assert event["source"] == "new" and event["selection_mode"] == "status-only"
                assert event["delivery_id"] is None and event["turn_id"] is None
                assert row["operation"] == {"kind": "claude-stable", "expectTurn": False,
                                            "selectionMode": "status-only", "stableState": "idle"}
                assert claude_delivery.read_receipt(f"claude:{C}", "create:initial") is None
            # The same original child/caller never creates an alternate event.
            cli_module._main(args)
            again = json.loads(capsys.readouterr().out)
            assert again["reconciled"] and not again["created"]
            assert again["notify_parent"]["event_id"] == eid
            assert len([r for r in parent_notify.iter_registrations(include_terminal=True)
                        if r["worker"]["workerId"] == C]) == 1
            # Preserve the real durable registration for independent physical
            # evidence; short_root's safe teardown removes its live root.
            original_registration = parent_notify.read_registration(eid)
            assert original_registration["parent"] == parent
            (tmp_path / "fork-original-parent-registration.json").write_text(
                json.dumps(original_registration, indent=2, sort_keys=True) + "\n"
            )
            wire = trace.read_text()
            if engine == "codex":
                assert wire.count("RPC thread/fork ") == 1
                assert wire.count("RPC turn/start ") == 1
                assert "RPC thread/start " not in wire
            else:
                assert [json.loads(line)["kind"] for line in wire.splitlines()] == [
                    "copy", "pty",
                ]
                paste = tmp_path / "f3a-pty-input.log"
                assert not paste.exists() or not paste.read_bytes()
    finally:
        if engine == "claude":
            _stop_fake(constants.CCX_ROOT, C)


def test_cli_fork_initial_delivery_lost_return_and_waiting_partial_format(
    sources, tmp_path, monkeypatch, capsys,
):
    """One real after-effect loss plus one synthetic *format-only* gate preview."""
    import time
    from ccx.claude import delivery as claude_delivery
    from ccx.ops import common as protocol_ops
    from ccx.errors import CreatePartialFailure, SessionWaiting

    store, _, _ = sources
    trace = _claude_environment(tmp_path, monkeypatch)
    prompt = "one genuine original fork first delivery, ACK lost"
    original_send = ops_send.send_turn

    def lost_return(*args, **kwargs):
        delivered = original_send(*args, **kwargs)
        assert delivered["status"] in {"owned", "accepted"}
        raise OSError("first send return disappeared AFTER original receipt")

    try:
        with monkeypatch.context() as interrupted:
            interrupted.setattr(ops_send, "send_turn", lost_return)
            with pytest.raises(SystemExit) as caught:
                cli_module._main(["fork", f"claude:{A}", prompt, "--json"])
        assert caught.value.code == 3
        partial = json.loads(capsys.readouterr().out)
        child = partial["worker_id"]
        assert str(UUID(child, version=4)) == child
        assert partial["worker"] == f"claude:{child}"
        assert partial["phase"] == "initial-delivery" and partial["ok"] is False
        assert partial["native_ready"] is True
        assert partial["delivery_id"] == "create:initial"
        assert "delivery_status" in partial
        assert store._owner(child) is not None
        assert claude_delivery.read_receipt(f"claude:{child}", "create:initial") is not None
        assert [json.loads(line)["kind"] for line in trace.read_text().splitlines()] == [
            "copy", "pty",
        ]
        with pytest.raises(SystemExit) as changed:
            cli_module._main([
                "fork", f"claude:{A}", "DIFFERENT first prompt", "--worker-id", child,
                "--json",
            ])
        assert changed.value.code == 5
        difference = json.loads(capsys.readouterr().out)
        assert difference["worker_id"] == child and difference["phase"] == "initial-delivery"
        assert [json.loads(line)["kind"] for line in trace.read_text().splitlines()] == [
            "copy", "pty",
        ]
        # Original host output is asynchronous even after its positive semantic ACK.
        native_input = tmp_path / "f3a-pty-input.log"
        until = time.monotonic() + 6
        while not native_input.exists() or prompt.encode() not in native_input.read_bytes():
            assert time.monotonic() < until
            time.sleep(.03)
        assert native_input.read_bytes().count(prompt.encode()) == 1

        # Presentation-only pin: no fake host gate or second native message.
        gate = {"id": "fork-original-permission", "kind": "permission"}
        cause = SessionWaiting("original permission decision still needed", waiting_for=gate)
        info = {
            "worker": f"claude:{child}", "worker_id": child, "engine": "claude",
            "short": child[:8], "native_ready": True, "activation_state": "confirmed",
        }
        def format_only(*_args, **_kwargs):
            raise CreatePartialFailure(
                "first original wait lacks settled proof", info=info, phase="wait-report",
                delivery_id="create:initial", delivery_status="owned", cause=cause,
            )
        with monkeypatch.context() as rendered:
            rendered.setattr(forking, "fork", format_only)
            with pytest.raises(SystemExit) as waiting:
                cli_module._main([
                    "fork", f"claude:{A}", prompt, "--worker-id", child, "--json",
                ])
            assert waiting.value.code == 4
            gate_output = json.loads(capsys.readouterr().out)
            assert gate_output["phase"] == "wait-report" and gate_output["worker_id"] == child
            assert gate_output["waiting_for"] == gate
            assert gate_output["request_id"] == gate["id"]
            assert gate_output["pending_kind"] == "permission"
            with pytest.raises(SystemExit) as plain:
                cli_module._main([
                    "fork", f"claude:{A}", prompt, "--worker-id", child,
                ])
            assert plain.value.code == 4
            diagnostic = capsys.readouterr().err
            assert str(cause) in diagnostic and child in diagnostic
            assert "wait-report" in diagnostic and "never remint" in diagnostic
        assert native_input.read_bytes().count(prompt.encode()) == 1
    finally:
        # The minted ID must be retained even when the first response is missing.
        if 'child' in locals():
            _stop_fake(constants.CCX_ROOT, child)
