"""Full-UUID context CLI over original sealed ACTIVE records and source sidecars.

All calls use isolated disposable HOME/native roots and real CLI subprocesses.
No provider, daemon, transcript renderer, or native thread is started.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from ccx import CcxClient
from ccx import CodexClient
from ccx import constants
from ccx.store import paths
from ccx.ops.creation_steps import prepare_worker
from ccx.store.root import require_ccx_active
from test_generation_store import A, B, short_root

# Third full UUID with same eight-character display prefix, owned by Codex.
D = "aaaaaaaa-9abc-4000-8000-000000000004"
UNKNOWN = "bbbbbbbb-1234-4000-8000-000000000005"


def _source_observation(store, home: Path, record: dict, block: dict):
    """Prepare original native source and its real-format owner-bound sidecar."""
    sid = record["native"]["sessionId"]
    src = paths.transcript_path_for(record["cwd"], sid)
    assert src.is_relative_to(home)
    src.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    (home / "projects").chmod(0o700)
    src.parent.chmod(0o700)
    src.write_text(
        json.dumps({"type": "assistant", "message": {"model": "fixture-native",
                    "content": [{"type": "text", "text": "done"}],
                    "usage": {"input_tokens": block.get("tokens_used") or 0}}}) + "\n",
        encoding="utf-8",
    )
    src.chmod(0o600)
    stat = src.stat()
    obs = store.worker_path(record["workerId"], "context-observation.json")
    doc = {
        "schemaVersion": 1,
        "workerId": record["workerId"],
        "nativeSessionId": sid,
        "generation": store.generation,
        "source": {
            "dev": stat.st_dev, "ino": stat.st_ino,
            "endOffset": stat.st_size, "observedAtMs": int(time.time() * 1000),
        },
        "context": block,
        "observedModel": "fixture-native",
    }
    obs.write_text(json.dumps(doc, sort_keys=True), encoding="utf-8")
    obs.chmod(0o600)
    return src, obs


def _snapshot(*roots):
    return {
        path: (path.read_bytes(), path.stat().st_mtime_ns)
        for root in roots for path in root.rglob("*") if path.is_file()
    }


@pytest.fixture
def original_contexts(short_root, tmp_path, monkeypatch):
    home = tmp_path / "original-claude"
    home.mkdir(mode=0o700)
    isolated_home = tmp_path / "isolated-home"
    isolated_home.mkdir(mode=0o700)
    runtime = tmp_path / "runtime"
    runtime.mkdir(mode=0o700)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    monkeypatch.setattr(constants, "CLAUDE_HOME", home)
    monkeypatch.setattr(paths, "CLAUDE_HOME", home)
    store = require_ccx_active(short_root)
    for wid, name in ((A, "alpha"), (B, "beta")):
        prepare_worker(store, engine="claude", worker_id=wid,
                       cwd=str(tmp_path), name=name)
    prepare_worker(store, engine="codex", worker_id=D,
                   cwd=str(tmp_path), name="gamma")
    store.mutate_record(B, patches={"lifecycle.state": "dead"})
    store.mutate_record(D, patches={
        "lifecycle.state": "idle",
        "context": {
            "tokens_used": 1234, "window_size": 9876, "percent_used": 12.345,
            "as_of_turn_ref": None, "as_of_ms": 321,
        },
        "lastTurnAt": 321,
    })
    first = store.record(A)
    second = store.record(B)
    _source_observation(store, home, first, {
        "state": "known", "tokens_used": 14567, "as_of_turn_ref": None,
        "as_of_ms": 123,
    })
    _source_observation(store, home, second, {
        "state": "known", "tokens_used": 98765, "as_of_turn_ref": None,
        "as_of_ms": 456,
    })
    assert first["native"]["sessionId"] != second["native"]["sessionId"]
    assert first["native"]["sessionId"] not in (A, B, D)
    assert A[:8] == B[:8] == D[:8]
    env = dict(os.environ)
    env.update({
        "HOME": str(isolated_home),
        "CCX_ROOT": str(short_root),
        "CLAUDE_CONFIG_DIR": str(home),
        "XDG_RUNTIME_DIR": str(runtime),
        "PYTHONPATH": str(Path(__file__).resolve().parent.parent),
        "PYTHONDONTWRITEBYTECODE": "1",
        "CCX_REAL_PARITY": "0",
    })
    return store, home, env


def _cli(env, *args):
    return subprocess.run(
        [sys.executable, "-B", "-m", "ccx", *args],
        env=env, text=True, capture_output=True, timeout=20, check=False,
    )


def _json(env, *args):
    proc = _cli(env, *args, "--json")
    assert proc.returncode == 0, (args, proc.stdout, proc.stderr)
    return json.loads(proc.stdout)


def test_original_claude_siblings_full_and_unique_prefix_json_plain_no_effects(
    original_contexts,
):
    store, home, env = original_contexts
    before = _snapshot(store.root, home)
    claude = CcxClient()
    for wid, tokens, turn, name in ((A, 14567, 3, "alpha"), (B, 98765, 8, "beta")):
        expected = claude.context(f"claude:{wid}").as_dict()
        assert expected["state"] == ("dead" if wid == B else "prepared")
        assert expected["tokens_used"] == tokens
        assert expected["as_of_turn_ref"] is None
        assert expected["name"] == name
        assert expected["window_size"] is None and expected["percent_used"] is None
        assert _json(env, "context", wid) == expected
        assert _json(env, "context", f"claude:{wid}") == expected
        assert _json(env, "context", wid[:13]) == expected
        assert _json(env, "context", f"claude:{wid[:13]}") == expected
        plain = _cli(env, "context", wid).stdout
        assert plain == f"claude:{wid}  context: {tokens:,} tokens\n"
        assert "claude:" + wid in expected["worker"]
    assert _snapshot(store.root, home) == before
    assert not (store.root / "run").exists()
    assert not (store.root / "sessions").exists()


def test_codex_top_level_context_preserves_flat_typed_and_nullable_percent(
    original_contexts,
):
    store, home, env = original_contexts
    before = _snapshot(store.root, home)
    expected = CodexClient().context(f"codex:{D}").as_dict()
    assert expected["tokens_used"] == 1234
    assert expected["window_size"] == 9876
    assert expected["as_of_turn_ref"] is None
    for args in (("context", D), ("context", f"codex:{D}"),
                 ("context", D[:13]), ("context", f"codex:{D[:13]}")):
        assert _json(env, *args) == expected
    assert _cli(env, "context", D).stdout == (
        f"codex:{D}  1,234 / 9,876 tokens (12.3%)\n"
    )

    # No fabricated percent; keep window and current token measurement.
    store.mutate_record(D, patches={"context": {**store.record(D)["context"], "percent_used": None}})
    expected = CodexClient().context(D).as_dict()
    assert expected["percent_used"] is None
    assert _json(env, "context", D) == expected
    assert _cli(env, "context", D).stdout == f"codex:{D}  1,234 / 9,876 tokens\n"

    store.mutate_record(D, patches={
        "context": {**store.record(D)["context"], "window_size": None},
    })
    assert _cli(env, "context", D).stdout == f"codex:{D}  1,234 tokens\n"

    store.mutate_record(D, patches={"activeCompact": {"operationId": "compact-c"}})
    expected = CodexClient().context(D).as_dict()
    assert expected["reason"] == "compacting" and expected["tokens_used"] is None
    assert expected["percent_used"] is None
    assert _json(env, "context", f"codex:{D}") == expected
    assert _cli(env, "context", D).stdout == f"codex:{D}  context unavailable\n"
    store.mutate_record(D, patches={"activeCompact": None, "context": None})
    no_signal = _json(env, "context", D)
    assert no_signal["tokens_used"] is None and no_signal["reason"] == "no-signal"
    assert _cli(env, "context", D).stdout == f"codex:{D}  context unavailable\n"
    assert not (store.root / "run").exists()
    # Only the explicit fixture mutations changed the Codex record.
    after = _snapshot(store.root, home)
    assert {p: after[p] for p in before if p != store.worker_path(D)} == {
        p: before[p] for p in before if p != store.worker_path(D)
    }


def test_original_context_unknown_reasons_and_missing_data_keep_lifecycle(
    original_contexts,
):
    store, home, env = original_contexts
    obs = store.worker_path(B, "context-observation.json")
    original = json.loads(obs.read_text())
    for reason, expected_line in (
        ("pending", "pending (no completed turn with token usage yet)"),
        ("compacting", "unknown (compacting)"),
        ("just_compacted", "unknown (just compacted)"),
    ):
        doc = dict(original)
        doc["context"] = {
            "state": "pending" if reason == "pending" else "unknown",
            "tokens_used": None, "reason": reason, "as_of_turn_ref": None, "as_of_ms": 456,
        }
        obs.write_text(json.dumps(doc))
        obs.chmod(0o600)
        before = _snapshot(store.root, home)
        result = _json(env, "context", B)
        assert result["state"] == "dead" and result["reason"] == reason
        assert result["tokens_used"] is None and result["worker"] == f"claude:{B}"
        assert _cli(env, "context", B).stdout == (
            f"claude:{B}  context: {expected_line}\n"
        )
        assert _snapshot(store.root, home) == before
    obs.unlink()
    before = _snapshot(store.root, home)
    absent = _json(env, "context", B)
    assert absent["reason"] == "no-signal" and absent["tokens_used"] is None
    assert absent["state"] == "dead"
    assert _cli(env, "context", B).stdout == (
        f"claude:{B}  context: no signal yet (none recorded; updates after the next turn)\n"
    )
    assert _snapshot(store.root, home) == before


def test_exact_prefix_ambiguity_and_live_refuse_before_read(
    original_contexts,
):
    store, home, env = original_contexts
    before = _snapshot(store.root, home)
    refused = _cli(env, "context", "aaaaaaaa", "--json")
    assert refused.returncode == 1 and refused.stdout == ""
    assert "ambiguous" in refused.stderr
    assert all(f"{engine}:{wid}" in refused.stderr for engine, wid in (
        ("claude", A), ("claude", B), ("codex", D)
    ))
    for bad in ("alpha", "claude-session:" + store.record(A)["native"]["sessionId"],
                "aaaaaaa", "AAAAAAAA", "other:" + A):
        refused = _cli(env, "context", bad, "--json")
        assert refused.returncode == 1 and refused.stdout == ""
    for args in (("context", UNKNOWN), ("context", f"codex:{UNKNOWN}")):
        refused = _cli(env, *args, "--json")
        assert refused.returncode == 2 and refused.stdout == ""
    live = _cli(env, "context", A, "--live", "--json")
    assert live.returncode == 1 and live.stdout == ""
    assert "unrecognized arguments: --live" in live.stderr
    assert _snapshot(store.root, home) == before
    assert not (store.root / "run").exists()


def test_cli_context_help_uses_full_uuids_and_removed_alias_is_rejected(
    original_contexts,
):
    _store, _home, env = original_contexts
    help_text = _cli(env, "context", "--help")
    assert help_text.returncode == 0
    assert "handle, full UUID, or a unique UUID prefix" in " ".join(help_text.stdout.split())
    top_help = _cli(env, "codex", "--help")
    assert top_help.returncode == 1 and "invalid choice" in top_help.stderr
    assert "context" in _cli(env, "--help").stdout
    removed = _cli(env, "codex", "context", "--help")
    assert removed.returncode == 1 and removed.stdout == ""
    assert "invalid choice" in removed.stderr
