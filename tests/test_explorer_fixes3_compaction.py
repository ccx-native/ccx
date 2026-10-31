"""Compaction-only history is skippable; real latest user answers are not."""
from __future__ import annotations
import ccx.ops.report as ops_report

import pytest

from ccx.codex import runtime as codex_runtime
from ccx.ops import common as protocol_ops
from test_generation_store import short_root  # noqa: F401
from test_worker_telemetry import original_codex, original_sources  # noqa: F401


@pytest.mark.parametrize("case", ["compact_id", "context_item", "mixed"])
def test_latest_native_compaction_skips_only_internal_turn(original_codex, monkeypatch, case):
    store, _cwd, wid, tid = original_codex
    handle = "codex:" + wid
    if case != "context_item":
        store.mutate_record(handle, patches={
            "lastCompact": {"turnId": "new-native", "status": "succeeded"},
        })
    store.mutate_record(handle, patches={"lifecycle.state": "idle"})
    items = ([{"type": "contextCompaction", "id": "c"}]
             if case == "context_item" else
             [{"type": "reasoning", "id": "internal"}]
             if case == "compact_id" else
             [{"type": "contextCompaction", "id": "c"},
              {"type": "userMessage", "id": "u", "content": [{"type": "text", "text": "question"}]},
              {"type": "agentMessage", "id": "a", "phase": "final_answer", "text": "NEWEST"}])
    thread = {
        "id": tid, "threadSource": store.codex_source(handle),
        "turns": [
            {"id": "old-native", "status": "completed", "items": [
                {"type": "agentMessage", "phase": "final_answer", "text": "OLDER"}]},
            {"id": "new-native", "status": "completed", "items": items},
        ],
    }
    monkeypatch.setattr(codex_runtime, "read_thread", lambda *_a, **_kw: thread)
    result = ops_report.report_worker(handle, timeout=3)
    if case == "mixed":
        assert (result["turn_id"], result["text"]) == ("new-native", "NEWEST")
    else:
        assert (result["turn_id"], result["text"]) == ("old-native", "OLDER")
    assert result["source"] == "native-transcript-latest"
