"""Same owner-written context.changed events through Python, RPC and CLI."""
from __future__ import annotations

import json

import pytest

from ccx import constants
from ccx.claude.context import current_context
from ccx.ops.context_events import ContextChanges, FIELDS, snapshot
from ccx.rpc import local_rpc
from ccx.workers import WorkerClient
from test_codex_native_effects import _f1_observation_worker, _f1_turn_notification
from test_generation_store import A, B, add_worker, short_root
from test_worker_events import _append, _journal
from test_worker_events_cli import _cli


@pytest.fixture
def event_owners(short_root, monkeypatch):
    add_worker(short_root, A)
    add_worker(short_root, B, "codex")
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    return short_root, WorkerClient()


def _context(tokens=120, *, ref=None, stamp=123):
    return dict(tokens_used=tokens, window_size=1000, percent_used=(
        100 * tokens / 1000 if tokens is not None else None),
        as_of_turn_ref=ref, as_of_ms=stamp, reason=None)


def test_owner_context_gate_dedup_throttle_pending_and_compaction():
    gate = ContextChanges()
    assert gate.observe(dict(reason="no-signal"), now=0) is None
    assert gate.observe(_context(10), now=1)["tokens_used"] == 10
    assert gate.observe(_context(10, stamp=555), now=2) is None
    assert gate.observe(_context(20), now=2) is None
    assert gate.observe(_context(33), now=3) is None
    assert gate.flush(now=5.9) is None
    assert gate.flush(now=6)["tokens_used"] == 33
    assert gate.observe(_context(34), now=7) is None
    compact = _context(None, stamp=777)
    compact["reason"] = "just_compacted"
    assert gate.observe(compact, force=True, now=7)["reason"] == "just_compacted"
    assert gate.flush(now=12) is None  # stale pre-fence pending removed
    assert set(snapshot(compact)) == set(FIELDS)


def test_context_projection_python_follow_rpc_and_cli_kind(event_owners):
    root, client = event_owners
    claude = {"type": "context_changed", "epoch": "e", "seq": 1,
              "ts": 123.0, "context": _context(25)}
    codex = {"method": "ccx/context/changed", "sequence": 1,
             "observedAtMs": 123000, "turnId": "native-c",
             "params": {"context": _context(25)}}
    _append(_journal(root, A), claude)
    _append(_journal(root, B, "codex"), codex)
    for handle, engine in ((A, "claude"), (B, "codex")):
        page = client.events(handle, kinds=["context.changed"])
        assert len(page["events"]) == 1, page
        event = page["events"][0]
        assert event["kind"] == "context.changed"
        assert event["engine"] == engine
        assert event["context"] == _context(25)
        assert set(event["context"]) == set(FIELDS)
        assert event["cursor"].startswith("ev2.")
        rpc = local_rpc.dispatch({
            "protocol_version": 2, "request_id": "context-filter-1",
            "op": "worker.events", "params": {"target": handle,
                    "kinds": ["context.changed"]},
        })
        assert rpc["ok"], rpc
        assert rpc["result"]["events"][0]["context"] == event["context"]
        resume = client.events(handle, tail=0)["next_cursor"]
        if engine == "claude":
            _append(_journal(root, A), {"type": "context_changed", "epoch": "e",
                      "seq": 2, "ts": 124.0, "context": _context(33, stamp=124)})
        else:
            _append(_journal(root, B, "codex"), {"method": "ccx/context/changed",
                      "sequence": 2, "observedAtMs": 124000,
                      "params": {"context": _context(33, stamp=124)}})
        seen = list(client.follow(handle, after=resume, kinds=["context.changed"],
                                  timeout=0, heartbeat=0))
        assert [ev["context"]["tokens_used"] for ev in seen
                if ev.get("kind") == "context.changed"] == [33]
        cli = _cli(root, handle, "--tail", "2", "--kind", "context.changed", "--json")
        assert cli.returncode == 0, (cli.stdout, cli.stderr)
        assert [ev["context"]["tokens_used"] for ev in json.loads(cli.stdout)["events"]] == [25, 33]
    help_result = _cli(root, A, "--help")
    assert help_result.returncode == 0
    assert "context.changed" in help_result.stdout


def test_codex_owner_bounded_native_usage_and_settle(short_root, tmp_path, monkeypatch):
    store, daemon, worker = _f1_observation_worker(short_root, tmp_path, monkeypatch)
    client = WorkerClient()
    _f1_turn_notification(daemon, "turn/started", "context-native-1")

    def usage(tokens):
        daemon._on_notification("thread/tokenUsage/updated", {
            "threadId": "native-f1-thread", "turnId": "context-native-1",
            "tokenUsage": {"total": {"inputTokens": tokens + 10},
                           "last": {"inputTokens": tokens, "outputTokens": 1},
                           "modelContextWindow": 1000},
        })

    usage(11)
    page = client.events(B, kinds=["context.changed"])
    assert [ev["context"]["tokens_used"] for ev in page["events"]] == [11]
    usage(15)
    usage(28)
    assert len(client.events(B, kinds=["context.changed"])["events"]) == 1
    worker._context_changes.last_at -= 6
    worker.flush_context()
    assert [ev["context"]["tokens_used"] for ev in
            client.events(B, kinds=["context.changed"])["events"]] == [11, 28]
    _f1_turn_notification(daemon, "turn/completed", "context-native-1")
    events = client.events(B, kinds=["context.changed"])["events"]
    assert len(events) == 3, events
    assert events[-1]["context"]["as_of_turn_ref"] == "ccx-turn:codex:v1:context-native-1"
    assert events[-1]["context"]["tokens_used"] == 28
    assert events[-1]["context"]["percent_used"] == pytest.approx(2.8)
    assert events[-1]["context"] == {key: client.context(B)[key] for key in FIELDS}


def test_claude_compact_fence_beats_refreshed_precompact_sidecar():
    old = {"state": "known", "tokens_used": 900, "as_of_ms": 100}
    record = {"lifecycle": {"state": "busy"},
              "context": {"state": "unknown", "reason": "just_compacted",
                          "tokens_used": None, "as_of_ms": 200,
                          "as_of_turn_ref": None}}
    assert current_context(record, engine="claude", live={**old, "as_of_ms": 300})["reason"] == "just_compacted"
    record["context"]["reason"] = "compacting"
    verified = {"state": "unknown", "reason": "just_compacted", "tokens_used": None,
                "as_of_ms": 150, "as_of_turn_ref": None}
    assert current_context(record, engine="claude", live=verified)["reason"] == "just_compacted"
