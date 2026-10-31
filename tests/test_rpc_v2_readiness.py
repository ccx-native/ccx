"""Turn-correlated V2 context and closed wait/unsupported contracts."""
from __future__ import annotations

import json
import time
from types import SimpleNamespace

import pytest

from ccx.claude.context import (current_context, public_context,
                         read_claude_observation, read_claude_settlement_context)
from ccx.rpc.view import CONTEXT_KEYS


def _settled(engine="claude"):
    ref = "ccx-turn:" + engine + ":v1:turn-1"
    return {
        "turnRef": ref,
        "turnId": "turn-1",
        "context": {"state": "known", "tokens_used": 100,
                    "window_size": 200 if engine == "codex" else None,
                    "percent_used": 50.0 if engine == "codex" else None,
                    "as_of_ms": 1000, "as_of_turn_ref": ref},
    }


@pytest.mark.parametrize("engine", ["claude", "codex"])
def test_idle_after_settlement_keeps_ref_even_if_live_measurement_is_newer(engine):
    last = _settled(engine)
    rec = {"lifecycle": {"state": "idle", "activeTurnId": None},
           ("lastSettledTurn" if engine == "claude" else "lastTurn"): last}
    later_sidecar = {"state": "known", "tokens_used": 120, "as_of_ms": 2000}
    ctx = current_context(rec, engine=engine, live=later_sidecar)
    assert ctx["as_of_turn_ref"] == "ccx-turn:" + engine + ":v1:turn-1"
    assert ctx["tokens_used"] == 100
    assert set(public_context(ctx, engine=engine)) == set(CONTEXT_KEYS)
    rec["lifecycle"] = {"state": "busy", "activeTurnId": "turn-2"}
    fresh = current_context(rec, engine=engine, live=later_sidecar)
    assert fresh["tokens_used"] == 120 and fresh["as_of_turn_ref"] is None
    assert current_context(rec, engine=engine, live=None)["as_of_turn_ref"] == last["turnRef"]


@pytest.mark.parametrize("engine", ["claude", "codex"])
def test_only_newer_compaction_fence_overrides_idle_settlement(engine):
    last = _settled(engine)
    rec = {"lifecycle": {"state": "idle", "activeTurnId": None},
           ("lastSettledTurn" if engine == "claude" else "lastTurn"): last}
    new = {"state": "unknown", "reason": "just_compacted",
           "tokens_used": None, "as_of_ms": 1001}
    old = {**new, "as_of_ms": 999}
    assert current_context(rec, engine=engine, live=new)["reason"] == "just_compacted"
    assert current_context(rec, engine=engine, live=new)["as_of_turn_ref"] is None
    assert current_context(rec, engine=engine, live=old)["as_of_turn_ref"] == last["turnRef"]


def test_observation_sidecar_is_not_a_settlement_after_idle_turn():
    now = int(time.time() * 1000)
    owner = SimpleNamespace(engine="claude", worker="claude:worker",
                            worker_id="worker")
    rec = {"native": {"sessionId": "session"}, "lifecycle": {"state": "idle"},
           "lastSettledTurn": {**_settled(), "settledAtMs": 1000}}
    observation = {"schemaVersion": 1, "workerId": "worker",
                   "nativeSessionId": "session", "generation": "gen",
                   "source": {"dev": 1, "ino": 2, "endOffset": 3,
                              "observedAtMs": now},
                   "context": {"state": "known", "tokens_used": 110,
                               "as_of_ms": now},
                   "observedModel": "haiku"}
    live = {"schemaVersion": 1, "short": "worker", "sessionId": "session",
            "writtenAtMs": now, "model": {"id": "haiku"},
            "contextWindow": {"total_input_tokens": 130}}
    class Store:
        generation = "gen"
        def read_worker_sidecar(self, worker, name):
            assert worker == owner.worker
            return {"context-observation.json": observation,
                    "statusline.json": live}[name]
    result = read_claude_observation(Store(), owner, rec)
    assert result["context"]["tokens_used"] == 100
    assert result["context"]["as_of_turn_ref"] == _settled()["turnRef"]
    rec["lifecycle"]["state"] = "busy"
    active = read_claude_observation(Store(), owner, rec)["context"]
    assert active["tokens_used"] == 130 and active["as_of_turn_ref"] is None


def _write_jsonl(path, rows):
    path.write_bytes(b"".join(json.dumps(value).encode() + bytes([10]) for value in rows))


def _usage(used, cached=0, created=0, **other):
    return {"type": "assistant", "timestamp": "2026-10-29T10:00:00Z",
            "message": {"usage": {"input_tokens": used,
                                  "cache_read_input_tokens": cached,
                                  "cache_creation_input_tokens": created},
                        "model": other.get("model", "haiku")}}


def test_bounded_claude_settlement_tail_is_exact_and_skips_zero(tmp_path, monkeypatch):
    source = tmp_path / "native.jsonl"
    prior = _usage(90000)
    end = _usage(10, cached=30, created=5)
    synthetic = _usage(0, model="<synthetic>")
    _write_jsonl(source, [prior])
    boundary = source.stat().st_size
    _write_jsonl(source, [prior, end, synthetic])
    sizes = []
    # The implementation uses only a single bounded positional read.
    import os
    original = os.pread
    def counted(fd, n, offset):
        sizes.append(n)
        return original(fd, n, offset)
    monkeypatch.setattr(os, "pread", counted)
    ref = "ccx-turn:claude:v1:x"
    ctx = read_claude_settlement_context(str(source), start_offset=boundary,
                                        turn_ref=ref, settled_ms=4000)
    assert (ctx["tokens_used"], ctx["as_of_turn_ref"]) == (45, ref)
    assert sizes and len(sizes) == 1 and sizes[0] <= 256 * 1024
    assert read_claude_settlement_context(str(source), start_offset=None,
                                          turn_ref=ref, settled_ms=4000) is None
    assert read_claude_settlement_context(str(source) + ".absent", start_offset=0,
                                          turn_ref=ref, settled_ms=4000) is None


def test_claude_settlement_compaction_and_no_usage(tmp_path):
    p = tmp_path / "native.jsonl"
    ref = "ccx-turn:claude:v1:now"
    _write_jsonl(p, [_usage(100), {"type": "system", "subtype": "compact_boundary"}])
    ctx = read_claude_settlement_context(str(p), start_offset=0,
                                         turn_ref=ref, settled_ms=3000)
    assert ctx["reason"] == "just_compacted" and ctx["tokens_used"] is None
    assert ctx["as_of_turn_ref"] == ref
    _write_jsonl(p, [{"type": "user", "message": {"content": "hi"}}])
    empty = read_claude_settlement_context(str(p), start_offset=0,
                                           turn_ref=ref, settled_ms=3000)
    assert empty["state"] == "pending" and empty["tokens_used"] is None


@pytest.mark.parametrize("raw,normalized,reason", [
    ("booting", "starting", "booting"), ("resuming", "starting", "resuming"),
    ("idle", "idle", "worker is idle"), ("busy", "busy", None),
    ("waiting", "waiting", None), ("dead", "stopped", "dead"),
    ("idle-killed", "stopped", "idle-killed"),
    ("budget-stopped", "stopped", "budget-stopped"),
    ("crashed", "failed", "crashed"), ("killed", "stopped", "killed"),
    ("host-dead", "unavailable", "host-dead"),
    ("unavailable", "unavailable", None), ("stopped", "stopped", None),
    ("timeout", "timeout", "deadline"), ("prepared", "unavailable", "prepared"),
])
def test_wait_status_closed_only_at_v2_boundary(monkeypatch, raw, normalized, reason):
    from ccx.rpc import local_rpc
    monkeypatch.setattr(local_rpc, "_rpc_worker", lambda *_args, **_kw: {"state": "idle"})
    result = local_rpc._rpc_result(
        "worker.wait",
        {"worker": "claude:owner", "status": raw,
         "reason": ("worker is idle" if raw == "idle" else
                    "deadline" if raw == "timeout" else None)},
        None, {"target": "claude:owner"})
    assert result["status"] == normalized and result["reason"] == reason


def test_wait_context_and_background_computed_once_at_projection(monkeypatch):
    from ccx.rpc import local_rpc
    from ccx import workers
    from ccx.rpc.view import WORKER_KEYS
    owner = SimpleNamespace(engine="codex", worker="codex:worker", worker_id="worker")
    last = {"turnId": "turn-1", "status": "completed",
            "context": {**_settled("codex")["context"]}}
    rec = {"lifecycle": {"state": "idle"}, "lastTurn": last,
           "context": {"tokens_used": 999, "as_of_ms": 2000}}
    class Store:
        def record(self, _worker):
            return rec
        def resolve(self, _worker):
            return owner
    calls = []
    def bg(_store, _owner):
        calls.append("background")
        return {"background_tasks_supported": False}
    monkeypatch.setattr(workers, "_background_task_block_for_worker", bg)
    settled = {"turn_ref": "ccx-turn:codex:v1:turn-1", "turn_id": "turn-1",
               "outcome": "completed", "completed_at": 1200,
               "context": last["context"]}
    result = local_rpc._rpc_result(
        "worker.wait", {"worker": owner.worker, "status": "idle",
                        "last_settled_turn": settled}, Store(),
        {"target": owner.worker})
    view = result["worker_view"]
    assert result["worker"] == owner.worker
    assert tuple(view) == WORKER_KEYS
    assert calls == ["background"]
    assert view["context"]["tokens_used"] == 100
    assert view["context"]["as_of_turn_ref"] == settled["turn_ref"]
    assert view["last_settled_turn"]["context"]["as_of_turn_ref"] == settled["turn_ref"]
    assert view["background_tasks"]["supported"] is False


def test_claude_reconcile_turn_id_is_typed_unsupported(tmp_path, monkeypatch):
    from ccx import constants
    from ccx.rpc import local_rpc
    from ccx.store.root import require_ccx_active
    from ccx.ops.creation_steps import prepare_worker
    from test_generation_store import fixture_root, A
    root = fixture_root(tmp_path)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    store = require_ccx_active(root)
    prepare_worker(store, engine="claude", cwd=str(tmp_path), worker_id=A)
    request = {"protocol_version": 2, "request_id": "r1",
               "op": "turn.reconcile",
               "params": {"target": "claude:" + A, "turn_id": "native-one"}}
    reply = local_rpc.dispatch_full_uuid(request, store)
    assert reply["error"]["code"] == "unsupported"
    assert reply["error"]["details"]["engine"] == "claude"
    assert reply["error"]["details"]["operation"] == "turn.reconcile"


@pytest.mark.parametrize("key,value", [("sandbox", "workspace-write"),
                                      ("approval_policy", "on-request")])
def test_claude_create_rejects_codex_only_params_as_unsupported(tmp_path, key, value):
    from ccx.ops.creation_steps import validated_create_spec
    from ccx.errors import CapabilityUnsupported
    args = dict(engine="claude", cwd=str(tmp_path), model=None, effort=None,
                idle_timeout=900, permissions=None, sandbox=None,
                approval_policy=None, agent=None, append_system_prompt=None,
                allowed_tools=None, disallowed_tools=None, add_dir=None,
                permission_mode=None)
    args[key] = value
    with pytest.raises(CapabilityUnsupported) as error:
        validated_create_spec(**args)
    assert error.value.engine == "claude"



def test_codex_succeeded_compaction_without_followup_usage_is_fenced():
    rec = {"lifecycle": {"state": "idle"}, "lastTurn": {
               "turnId": "compact-turn", "status": "completed", "context": None},
           "context": None,
           "lastCompact": {"status": "succeeded", "completedAtMs": 3000,
                           "contextBefore": {"tokens_used": 12345,
                                             "window_size": 258400,
                                             "as_of_ms": 2000},
                           "contextAfter": None}}
    fenced = current_context(rec, engine="codex", live=None)
    assert fenced["reason"] == "just_compacted"
    assert fenced["tokens_used"] is None and fenced["as_of_turn_ref"] is None
    flat = public_context(fenced, engine="codex")
    assert flat["window_size"] == 258400 and flat["percent_used"] is None
    rec["context"] = {"tokens_used": 300, "as_of_ms": 3001}
    restored = current_context(rec, engine="codex", live=rec["context"])
    assert restored["tokens_used"] == 300
    assert restored["as_of_turn_ref"] is None


def test_v2_settled_pending_has_pending_reason():
    from ccx.rpc.view import project_worker_view
    from test_generation_store import A, record
    row = project_worker_view(
        "claude:" + A, record(A, "claude"),
        last_settled_turn={"turn_ref": "ccx-turn:claude:v1:turn",
                           "context": {"state": "pending", "tokens_used": None,
                                       "as_of_ms": 123,
                                       "as_of_turn_ref": "ccx-turn:claude:v1:turn"}})
    assert row["last_settled_turn"]["context"]["reason"] == "pending"


def test_wait_bundled_report_has_the_worker_report_shape(monkeypatch):
    """worker.wait include_report and worker.report publish one report shape."""
    from ccx.rpc import local_rpc

    monkeypatch.setattr(local_rpc, "_rpc_worker", lambda *a, **k: {})
    handle = "claude:2b6c1d3e-4f5a-4b7c-8d9e-0f1a2b3c4d5e"
    raw = {key: f"v-{key}" for key in local_rpc.REPORT_KEYS}
    raw.update(worker=handle, engine="claude", short="2b6c1d3e",
               name="n", context={"tokens_used": 1}, transcript="/x")
    report = local_rpc._rpc_result("worker.report", dict(raw), None,
                                   {"target": handle})
    waited = local_rpc._rpc_result(
        "worker.wait", {"worker": handle, "status": "idle", "report": dict(raw)},
        None, {"target": handle})
    assert waited["report"] == report
    assert set(report) == {"worker", *local_rpc.REPORT_KEYS}
    empty = local_rpc._rpc_result(
        "worker.wait", {"worker": handle, "status": "idle", "report": None,
                        "report_error": "no positively completed exact turn"},
        None, {"target": handle})
    assert empty["report"] is None and empty["report_error"]
