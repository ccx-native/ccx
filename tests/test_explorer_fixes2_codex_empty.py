"""Source-bound promptless Codex conversation and empty-fork refusal."""
from __future__ import annotations

import pytest

from ccx.codex import runtime as codex_runtime
from ccx.ops import forking
from ccx.errors import InvalidRequest
from ccx.workers import WorkerClient
from test_generation_store import short_root  # noqa: F401
from test_worker_telemetry import original_codex, original_sources  # noqa: F401


def test_known_empty_native_thread_is_a_valid_conversation_and_not_forkable(
    original_codex, monkeypatch,
):
    store, _cwd, wid, _tid = original_codex
    worker = f"codex:{wid}"
    store.mutate_record(worker, patches={
        "lifecycle.state": "idle", "historyKnownEmpty": True,
    })
    monkeypatch.setattr(codex_runtime, "read_thread", lambda *_a, **_k: pytest.fail(
        "known-empty thread must not invoke unsupported native history listing"
    ))
    document = WorkerClient().conversation(worker)
    assert document["turns"] == []
    assert WorkerClient().list(engine="codex")["workers"][0]["state"] == "idle"
    before = {r["worker_id"] for r in store.list_workers()}
    with pytest.raises(InvalidRequest, match="no completed conversation turn"):
        forking.plan(worker, worker_id="dddddddd-1234-4000-8000-000000000004")
    assert {r["worker_id"] for r in store.list_workers()} == before
