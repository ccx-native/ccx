"""Actual canonical `ccx logs` CLI over sealed, original UUID fake sources."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from ccx.cli import main as cli
from ccx.codex import runtime as codex_runtime
from ccx.store import paths
from ccx.ops.creation_steps import activate_prepared_worker, prepare_worker
from ccx.workers import WorkerClient
from test_generation_store import A, B, C
from test_generation_store import short_root  # noqa: F401 - imported pytest fixture
from test_native_conversation import _append, _entry
from test_worker_telemetry import _codex_turn
from test_worker_telemetry import original_codex, original_sources  # noqa: F401 - pytest fixtures


def _environment(store) -> dict[str, str]:
    """Explicit only: no provider credentials, installed CLI or live state."""
    home = store.root.parent / "fake-cli-home"
    dirs = {key: home / key.lower() for key in (
        "HOME", "XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_RUNTIME_DIR",
    )}
    for directory in dirs.values():
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        directory.chmod(0o700)
    return {
        **{key: str(directory) for key, directory in dirs.items()},
        "CCX_ROOT": str(store.root),
        "CLAUDE_CONFIG_DIR": str(paths.CLAUDE_HOME),
        "PYTHONPATH": str(Path(__file__).resolve().parent.parent),
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
        "PATH": os.defpath,
        "LC_ALL": "C.UTF-8",
    }


def _cli(store, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-B", "-m", "ccx", "logs", *args],
        cwd=str(store.root.parent), env=_environment(store),
        capture_output=True, text=True, encoding="utf-8", timeout=15,
    )


def _tool_result(doc: dict) -> dict:
    return next(item["result"] for turn in doc["turns"]
                for item in turn["output"] if item["kind"] == "tool-call")


def _write_claude_dialogue(store, *, payload: str, second: bool = True) -> tuple[Path, str]:
    rec = store.record(A)
    native = Path(rec["transcript"])
    _append(native,
            _entry("user", "première question", uuid="qa"),
            {"type": "assistant", "uuid": "call", "message": {
                "role": "assistant", "model": "fake", "content": [
                    {"type": "tool_use", "id": "tool1", "name": "Read", "input": {"file_path": "fixture"}},
                ]}},
            {"type": "user", "uuid": "result", "message": {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "tool1", "content": payload},
            ]}},
            _entry("assistant", "résultat café", uuid="answer1"))
    if second:
        _append(native, _entry("user", "deuxième question", uuid="qb"),
                _entry("assistant", "terminé été", uuid="answer2"))
    return native, rec["native"]["sessionId"]


def test_cli_logs_claude_normalized_default_bound_full_zero_prefix_and_no_cache(
    original_sources,
):
    store, _cwd = original_sources
    payload = "é" * 3500   # 7000 UTF-8 bytes, cannot fit the default 4096.
    _native, sid = _write_claude_dialogue(store, payload=payload)
    assert sid != A and A[:8] == B[:8]
    store.mutate_record(A, patches={"lifecycle.state": "dead"})
    before = store.worker_path(A, "record.json").read_bytes()

    result = _cli(store, A[:13])  # One unique CLI textual prefix, never backend alias.
    assert result.returncode == 0 and not result.stderr, result.stderr
    assert result.stdout.startswith('{\n  "schemaVersion": 5,')
    assert result.stdout.endswith("\n") and "résultat café" in result.stdout
    doc = json.loads(result.stdout)
    assert doc["worker"]["handle"] == f"claude:{A}"
    assert doc["worker"]["short"] == A[:8]
    assert doc["worker"]["nativeSessionId"] == sid
    assert doc["state"] == "stopped"
    assert doc["schemaVersion"] == 5 and len(doc["turns"]) == 2
    clipped = _tool_result(doc)
    assert clipped["truncated"] is True
    assert clipped["returnedByteCount"] == 4096
    assert clipped["originalByteCount"] == len(payload.encode())
    assert clipped["contentSha256"] == hashlib.sha256(payload.encode()).hexdigest()

    entire = _cli(store, A, "--full", "--max-tool-result-bytes", "-1")
    assert entire.returncode == 0, entire.stderr
    doc_full = json.loads(entire.stdout)
    assert _tool_result(doc_full)["content"] == payload
    assert _tool_result(doc_full)["truncated"] is False
    assert doc_full["completeness"]["requested"] == "full"

    meta = _cli(store, A, "--max-tool-result-bytes", "0")
    assert meta.returncode == 0, meta.stderr
    result_meta = _tool_result(json.loads(meta.stdout))
    assert result_meta["content"] == "" and result_meta["returnedByteCount"] == 0
    assert result_meta["originalByteCount"] == len(payload.encode())
    assert result_meta["contentSha256"] == clipped["contentSha256"]

    one = _cli(store, A, "--tail", "1")
    zero = _cli(store, A, "--tail", "0")
    minus = _cli(store, A, "--tail", "-1")
    assert all(x.returncode == 0 for x in (one, zero, minus))
    # A Claude tail reads only its own turns, so it cannot number them.
    tail_doc = json.loads(one.stdout)
    tail_turns = tail_doc["turns"]
    assert [t["sequence"] for t in tail_turns] == [None]
    assert tail_doc["completeness"]["sourceLimited"] is True
    assert ([{**t, "sequence": None} for t in tail_turns]
            == [{**t, "sequence": None} for t in doc["turns"][-1:]])
    assert json.loads(zero.stdout)["turns"] == doc["turns"]
    assert json.loads(minus.stdout)["turns"] == doc["turns"][1:]

    invalid_bound = _cli(store, A, "--max-tool-result-bytes", "-1")
    assert invalid_bound.returncode == 1 and not invalid_bound.stdout
    assert "--max-tool-result-bytes must be zero or greater" in invalid_bound.stderr
    assert before == store.worker_path(A, "record.json").read_bytes()
    assert not (store.worker_path(A).parent / "transcript.json").exists()
    assert not store.worker_path(A, "context-observation.json").exists()
    assert not (store.root / "sessions").exists()

    help_result = _cli(store, "--help")
    assert help_result.returncode == 0 and "conversation as JSON" in help_result.stdout
    assert "Reserved" not in help_result.stdout and "--live" not in help_result.stdout


def test_cli_logs_claude_rendered_raw_priority_utf8_and_original_source_errors(
    original_sources,
):
    store, _cwd = original_sources
    native, sid = _write_claude_dialogue(store, payload="tool output", second=True)
    assert sid != A
    with native.open("ab") as stream:
        stream.write(b'{"type":"assistant","message":\xff')
    raw = native.read_bytes().decode("utf-8", errors="replace")
    assert raw.endswith('"message":\ufffd')

    # Raw first: no renderer/cache; --jsonl wins over --format rendered and
    # ignores tail/cap/full, even negative caps.
    raw_call = _cli(store, A, "--jsonl", "--format", "rendered", "--tail", "1",
                    "--max-tool-result-bytes", "-1", "--full")
    assert raw_call.returncode == 0 and not raw_call.stderr
    assert raw_call.stdout == raw and not raw_call.stdout.endswith("\n")
    assert not (store.worker_path(A).parent / "transcript.json").exists()
    assert not store.worker_path(A, "context-observation.json").exists()

    legacy = _cli(store, A, "--format", "rendered", "--tail", "1",
                  "--max-tool-result-bytes", "-1")
    assert legacy.returncode == 0, legacy.stderr
    assert legacy.stdout.endswith("\n") and "\\u00e9" in legacy.stdout
    assert "UNCOMMITTED" not in legacy.stdout
    doc = json.loads(legacy.stdout)
    assert doc["schemaVersion"] == 2 and doc["sessionId"] == sid
    assert doc["worker"] == f"claude:{A}" and "short" not in doc
    assert len(doc["turns"]) == 1 and doc["turns"][-1]["text"] == "terminé été"
    all_turns = _cli(store, A, "--format", "rendered", "--tail", "0")
    assert all_turns.returncode == 0 and len(json.loads(all_turns.stdout)["turns"]) > 1
    # Rendering is read-only: no cache or observation is published.
    assert not (store.worker_path(A).parent / "transcript.json").exists()
    assert not store.worker_path(A, "context-observation.json").exists()

    native.unlink()
    for flags in ((), ("--format", "rendered"), ("--jsonl",)):
        missing = _cli(store, A, *flags)
        assert missing.returncode == 2 and not missing.stdout, (flags, missing)
        assert "transcript" in missing.stderr

    native.symlink_to(Path(store.record(B)["transcript"]))
    for flags in ((), ("--jsonl",), ("--format", "rendered")):
        refused = _cli(store, A, *flags)
        assert refused.returncode == 3 and not refused.stdout, (flags, refused)
        assert "original" in refused.stderr or "native" in refused.stderr


def test_cli_logs_codex_original_inprocess_global_prefix_and_typed_refusals(
    original_codex, monkeypatch, capsys,
):
    store, cwd, wid, tid = original_codex
    # Additional original Codex owner shares the first eight hex digits of
    # Claude A/B: an unqualified ambiguity must include both ENGINES.
    codex_peer = "aaaaaaaa-7890-4000-8000-000000000004"
    prepared = prepare_worker(store, engine="codex", cwd=str(cwd), worker_id=codex_peer)
    def fake_admission(_budget):
        original = store.native_activation(codex_peer)
        return {
            "accepted": True, "workerId": codex_peer, "engine": "codex",
            "source": f"ccx-worker:{codex_peer}",
            "submissionId": original["submissionId"],
            "nativeEvidenceSha256": "a" * 64,
            "nativeBinding": {"threadId": "another-original-thread"},
        }
    assert activate_prepared_worker(
        store, prepared, bounded_native_submit=fake_admission,
    ).activation_state == "confirmed"
    store.mutate_record(A, patches={"lifecycle.state": "dead"})

    thread = _codex_turn(tid, wid)
    thread["turns"][0]["items"].insert(1, {
        "id": "cmd", "type": "commandExecution", "command": "echo okay",
        "status": "completed", "exitCode": 0, "aggregatedOutput": "èxito",
    })
    thread["turns"][0]["items"].insert(2, {
        "id": "thought", "type": "reasoning",
        "content": [{"type": "reasoningText", "text": "INTERNAL_SECRET"}],
    })
    seen = []

    def fake_thread_read(thread_id, *, cwd, timeout, worker_id, expected_source):
        seen.append((thread_id, cwd, timeout, worker_id))
        assert thread_id == tid and worker_id == wid
        assert expected_source == store.codex_source(wid) == f"ccx-worker:{wid}"
        return thread

    monkeypatch.setattr(codex_runtime, "read_thread", fake_thread_read)

    def invoke(*args: str) -> tuple[int, str, str]:
        try:
            cli.main(["logs", *args])
            code = 0
        except SystemExit as exc:
            code = exc.code
        result = capsys.readouterr()
        return code, result.out, result.err

    ambiguous = invoke(A[:8])
    assert ambiguous[0] == 1 and not ambiguous[1] and "ambiguous" in ambiguous[2]
    assert all(value in ambiguous[2] for value in (
        f"claude:{A}", f"claude:{B}", f"codex:{codex_peer}",
    ))
    assert seen == []  # No native history read before global prefix failure.

    for flags in (("--jsonl",), ("--format", "rendered")):
        error = invoke(wid, *flags)
        assert error[0] == 1 and not error[1] and "Claude-only" in error[2]
        assert seen == []

    success = invoke(C[:8])
    assert success[0] == 0 and not success[2], success
    assert len(seen) == 1 and seen[0] == (tid, str(cwd), 120, wid)
    result = json.loads(success[1])
    assert success[1].startswith('{\n  "schemaVersion": 5,')
    assert "èxito" in success[1] and "INTERNAL_SECRET" not in success[1]
    assert result["worker"]["handle"] == f"codex:{C}"
    assert result["worker"]["nativeSessionId"] == tid
    items = sorted(result["turns"][0]["input"] + result["turns"][0]["output"],
                   key=lambda x: x["ordinal"])
    assert [item["id"] for item in items if item["id"] in ("native-user-1", "cmd", "native-final-1")] == [
        "native-user-1", "cmd", "native-final-1",
    ]

    for bad in (A[:7], "display is not a key", "ABCDEF01", "not-a-uuid"):
        code, out, err = invoke(bad)
        assert code == 1 and not out and "worker discovery" in err, (bad, err)
    # The independent provider-native Claude SID is a valid canonical full UUID
    # but not a CCX identity, hence UNKNOWN exact owner (exit 2), NOT invalid 1.
    sid = store.record(A)["native"]["sessionId"]
    assert sid not in (A, B, wid, codex_peer)
    for unknown in (sid, "cccccccc-1234-4000-8000-000000000009"):
        code, out, err = invoke(unknown)
        assert code == 2 and not out and err == f"no such worker: {unknown}\n", (unknown, err)

    # Removed CLI flag never reaches worker discovery or WorkerClient.
    invalid_live = invoke("not-a-uuid", "--live")
    assert invalid_live[0] == 1 and "unrecognized arguments: --live" in invalid_live[2]
    with monkeypatch.context() as patch:
        patch.setattr(WorkerClient, "__init__", lambda _self: pytest.fail(
            "logs --live must refuse before WorkerClient construction"
        ))
        live = invoke(wid, "--live")
    assert live[0] == 1 and not live[1]
    assert "unrecognized arguments: --live" in live[2]
    assert len(seen) == 1
    assert not (store.root / "sessions").exists()


def test_cli_logs_codex_physical_fake_daemon_history_roundtrip(
    short_root, tmp_path, monkeypatch,
):
    """The public CLI must read an actual Codex thread, not only mocked reads."""
    from ccx import constants
    from test_codex_native_effects import _f2_fake_processes, _f2_new

    with _f2_fake_processes(short_root, tmp_path, monkeypatch) as (cli, _restart, trace, store, _rpc):
        created = _f2_new(cli, tmp_path, C, "--start-timeout", "10")
        assert created.returncode == 0, (created.stdout, created.stderr)
        sent = cli("send", C, "codex logs end-to-end original", "--no-wait", "--json")
        assert sent.returncode == 0, (sent.stdout, sent.stderr)
        result = cli("logs", C)
        assert result.returncode == 0, (result.stdout, result.stderr)
        doc = json.loads(result.stdout)
        assert doc["schemaVersion"] == 5
        assert doc["worker"]["handle"] == f"codex:{C}"
        assert any("codex logs end-to-end original" in json.dumps(turn)
                   for turn in doc["turns"])
        assert "RPC thread/read" in trace.read_text()


def test_cli_logs_codex_no_legacy_fallback_when_turn_listing_unsupported(
    short_root, tmp_path, monkeypatch,
):
    """Without turn statuses, do not fabricate a successful conversation."""
    from test_codex_native_effects import _f2_fake_processes, _f2_new

    with _f2_fake_processes(short_root, tmp_path, monkeypatch,
                            read_turns_unsupported=True) as (cli, _restart, trace, store, _rpc):
        created = _f2_new(cli, tmp_path, C, "--start-timeout", "10")
        assert created.returncode == 0, (created.stdout, created.stderr)
        sent = cli("send", C, "fallback-native-items", "--no-wait", "--json")
        assert sent.returncode == 0, (sent.stdout, sent.stderr)
        result = cli("logs", C)
        assert result.returncode != 0
        assert "RPC thread/turns/list" in trace.read_text()
        assert "RPC thread/items/list" not in trace.read_text()
