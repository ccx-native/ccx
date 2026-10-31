"""Existing CLI new flags bind share variabless through the original UUID create."""
from __future__ import annotations

import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import time

import pytest

pytestmark = pytest.mark.usefixtures("executable_engine_stubs")

from ccx import constants
from ccx.controllers import live_vars
from ccx.claude.delivery import read_receipt
from ccx.store.root import require_ccx_active
from test_generation_store import A, B, C, add_worker, short_root  # noqa: F401
from test_create_cross_surface import _deadline_fake_claude
from test_claude_native_cli import _stop_disposable_original
from test_codex_native_effects import _f2_fake_processes


def test_cli_new_durable_claude_prompt_and_codex_shared_prefix_one_original(
    short_root, tmp_path, monkeypatch,
):
    """Both real CLI parsers, one shared stateId, one native first turn/replay."""
    launches = _deadline_fake_claude(short_root, tmp_path, monkeypatch)
    native_input = tmp_path / "cli-new-claude-input.log"
    monkeypatch.setenv("FAKE_LOG", str(native_input))
    prompt = "Original CLI variables task"
    claude_opts = (
        "new", "--engine", "claude", prompt, "--cwd", str(tmp_path), "--worker-id", A,
        "--var", "counter=1", "--var", "items=[1,2]", "--json",
    )
    try:
        with _f2_fake_processes(
            short_root, tmp_path, monkeypatch,
            persisted_threads=tmp_path / "cli-new-codex-history.json",
        ) as (cli, _restart, trace, store, _rpc):
            deadline = time.monotonic() + 12
            while True:
                first = cli(*claude_opts)
                assert first.returncode in (0, 3), (first.stdout, first.stderr)
                result = json.loads(first.stdout)
                assert result["worker_id"] == A and result["worker"] == "claude:" + A
                if first.returncode == 0:
                    break
                assert result["phase"] == "native-activation", result
                assert time.monotonic() < deadline, result
                time.sleep(.08)
            assert result["ok"] and result["native_ready"]
            assert result["delivery_id"] == "create:initial"
            assert result["delivery_status"] in {"owned", "accepted"}
            assert result["created"] or result["reconciled"]
            assert launches.read_text().splitlines() == ["provider-start"]

            receipt = read_receipt("claude:" + A, "create:initial")
            saved_prompt = receipt["promptText"]
            first_digest = receipt["promptSha256"]
            assert saved_prompt.startswith(prompt + "\n\n[CCX variables available")
            assert saved_prompt.count("CCX variables available") == 1
            assert "counter, items" in saved_prompt
            deadline = time.monotonic() + 8
            while time.monotonic() < deadline:
                if native_input.exists() and prompt.encode() in native_input.read_bytes():
                    break
                time.sleep(.05)
            original_bytes = native_input.read_bytes()
            assert original_bytes.count(prompt.encode()) == 1
            assert original_bytes.count(b"CCX variables available") == 1

            source = store.variables_info("claude:" + A)
            assert source["mode"] == "durable"
            assert source["names"] == ["counter", "items"]
            assert store.variables_info(A, name="items")["value"] == [1, 2]

            # The CLI discovers the engine for the unique eight-hex source
            # before the child exists; the backend only receives exact UUIDs.
            created = cli(
                "new", "--engine", "codex", "--cwd", str(tmp_path), "--worker-id", B,
                "--vars-from", A[:8], "--start-timeout", "8", "--json",
            )
            assert created.returncode == 0, (created.stdout, created.stderr)
            child = json.loads(created.stdout)
            assert child["ok"] and child["native_ready"]
            assert child["worker"] == "codex:" + B and child["worker_id"] == B
            assert child["native_source"] != result["native_source"]
            assert store.record(B)["native"]["threadId"] not in {A, B}
            shared = store.variables_info("codex:" + B)
            assert shared["mode"] == "durable"
            assert shared["state_id"] == source["state_id"]
            assert shared["names"] == source["names"]
            assert store.variables_info(B, name="counter")["value"] == 1
            assert trace.read_text().count("RPC thread/start") == 1

            # A later explicit state mutation must survive identical CREATE
            # replay. The first binding receipt and create:initial own the
            # original intent; replay must not re-publish or paste again.
            live_vars.publish_attachment(
                live_vars.prepare_durable_attachment({"counter": 7}),
                "claude", A,
            )
            assert store.variables_info("codex:" + B, name="counter")["value"] == 7
            again = cli(*claude_opts)
            assert again.returncode == 0, (again.stdout, again.stderr)
            retried = json.loads(again.stdout)
            assert retried["worker_id"] == A and retried["reconciled"]
            assert retried["native_ready"] and retried["delivery_id"] == "create:initial"
            # A positively accepted Claude receipt clears its plaintext;
            # its durable prompt digest and semantic token remain immutable.
            replay_receipt = read_receipt("claude:" + A, "create:initial")
            assert replay_receipt["deliveryId"] == "create:initial"
            assert replay_receipt["promptSha256"] == first_digest
            assert store.variables_info(A, name="counter")["value"] == 7
            assert store.variables_info(B, name="counter")["value"] == 7
            assert native_input.read_bytes().count(prompt.encode()) == 1
            assert native_input.read_bytes().count(b"CCX variables available") == 1
            assert launches.read_text().splitlines() == ["provider-start"]
            assert trace.read_text().count("RPC thread/start") == 1
    finally:
        _stop_disposable_original(short_root)


@pytest.mark.parametrize("case,args,reason", [
    ("malformed", ("new", "--engine", "claude", "--var", "x={bad"), "valid JSON"),
    ("xor", ("new", "--engine", "claude", "--var", "x=1", "--vars-from", A), "mutually exclusive"),
    ("claude-restricted", ("new", "--engine", "claude", "--permissions", "restricted", "--var", "x=1"),
     "full permissions"),
    ("codex-restricted", ("new", "--engine", "codex", "--sandbox", "workspace-write", "--var", "x=1"),
     "full permissions"),
    ("nondurable", ("new", "--engine", "codex", "--vars-from", "claude:" + A), "no durable variables"),
    ("ambiguous-stopped", ("new", "--engine", "claude", "--vars-from", A[:8]), "ambiguous managed worker"),
])
def test_cli_new_var_refusals_preclaim_even_with_stopped_source(
    short_root, tmp_path, monkeypatch, case, args, reason,
):
    """Invalid new intent is refused before child claim or any fake native exec."""
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    store = require_ccx_active(short_root)
    if case in {"nondurable", "ambiguous-stopped"}:
        add_worker(short_root, A, "claude", terminal=True)
        assert store.variables_info(A)["mode"] is None
    if case == "ambiguous-stopped":
        add_worker(short_root, B, "codex", terminal=True)
        assert store.list_workers(limit=None)[0]["terminal_only"]

    root = Path(__file__).resolve().parent.parent
    trace = tmp_path / "forbidden-native-exec"
    fake = tmp_path / "fail-closed-provider"
    fake.write_text(
        "#!/bin/sh\nprintf 'UNEXPECTED_NATIVE_EXEC\\n' >> "
        + shlex.quote(str(trace)) + "\nexit 77\n",
        encoding="utf-8",
    )
    fake.chmod(0o700)
    env = {
        **os.environ, "CCX_ROOT": str(short_root),
        "CCX_CLAUDE_BIN": str(fake), "CCX_CODEX_BIN": str(fake), "CCX_SPAWN_SCOPE": "0",
        "PYTHONPATH": str(root),
    }
    source_before = (
        store.record(A) if case in {"nondurable", "ambiguous-stopped"} else None
    )
    command = [
        sys.executable, "-B", "-m", "ccx", *args,
        "--cwd", str(tmp_path), "--worker-id", C, "--json",
    ]
    result = subprocess.run(
        command, env=env, cwd=root, text=True, capture_output=True, timeout=12,
    )
    assert result.returncode != 0, (result.stdout, result.stderr)
    assert reason in result.stderr, (case, result.stderr, result.stdout)
    assert store._owner(C) is None and store._claim(C) is None
    assert not trace.exists(), (case, trace.read_text() if trace.exists() else "")
    if source_before is not None:
        assert store.record(A) == source_before
        assert store.variables_info(A)["mode"] is None
    if case == "ambiguous-stopped":
        assert "claude:" + A in result.stderr
        assert "codex:" + B in result.stderr
