"""Engine-neutral ``WorkerClient`` facade.

Deterministic only: no live Claude host, no Codex app-server, no network. Runtime
load/observation seams are patched with semantic fakes while records are written
directly.
"""

from __future__ import annotations

import json

import pytest

import ccx
from ccx import WorkerClient, evaluate_quota
from ccx import workers as W
from ccx.constants import S
from ccx.errors import (
    InvalidRequest,
    SessionNotFound,
)

# Sealed full-UUID ACTIVE-root fixtures and worker IDs from the UUID suites.
from test_python_worker_api import active_api_root, _worker_summaries_file_snapshot
from test_generation_store import A, B, C as UUID_C, add_worker
from ccx.store.root import require_ccx_active


# --- handles -----------------------------------------------------------------

def test_worker_client_is_exported():
    assert ccx.WorkerClient is WorkerClient
    assert ccx.CapabilityUnsupported is W.CapabilityUnsupported


def test_evaluate_quota_uses_exact_window_ids_and_fails_closed() -> None:
    usage = {"windows": [
        {"id": "five_hour", "used_percent": 42, "resets_at": "2026-08-12T12:00:00Z"},
        {"id": "model:Fable", "used_percent": 90, "resets_at": "2026-08-14T12:00:00Z"},
    ]}
    result = evaluate_quota(usage, {"five_hour": 50, "model:Fable": 80})
    assert result == {
        "allowed": False,
        "reached": ["model:Fable"],
        "unknown": [],
        "retry_at": "2026-08-14T12:00:00Z",
        "windows": [
            {"id": "five_hour", "limit_percent": 50.0, "used_percent": 42.0,
             "resets_at": "2026-08-12T12:00:00Z", "state": "allowed"},
            {"id": "model:Fable", "limit_percent": 80.0, "used_percent": 90.0,
             "resets_at": "2026-08-14T12:00:00Z", "state": "reached"},
        ],
    }
    unknown = evaluate_quota(usage, {"model:Other": 80})
    assert unknown["allowed"] is False
    assert unknown["unknown"] == ["model:Other"]
    assert unknown["retry_at"] is None


@pytest.mark.parametrize("limits", [
    {}, {"five_hour": True}, {"five_hour": -1}, {"five_hour": 101}, {"": 10},
])
def test_evaluate_quota_rejects_invalid_limits(limits) -> None:
    with pytest.raises(InvalidRequest):
        evaluate_quota({"windows": []}, limits)


def test_evaluate_quota_treats_duplicate_or_malformed_windows_as_unknown() -> None:
    duplicate = {"windows": [
        {"id": "five_hour", "used_percent": 1},
        {"id": "five_hour", "used_percent": 2},
    ]}
    assert evaluate_quota(duplicate, {"five_hour": 50})["unknown"] == ["five_hour"]
    assert evaluate_quota(
        {"windows": [{"id": "five_hour", "used_percent": None}]},
        {"five_hour": 50},
    )["unknown"] == ["five_hour"]


def test_evaluate_quota_retry_at_is_latest_reached_reset() -> None:
    usage = {"windows": [
        {"id": "a", "used_percent": 90, "resets_at": "2026-08-13T10:00:00+02:00"},
        {"id": "b", "used_percent": 90, "resets_at": "2026-08-13T09:30:00Z"},
    ]}
    assert evaluate_quota(usage, {"a": 50, "b": 50})["retry_at"] == (
        "2026-08-13T09:30:00Z"
    )
    usage["windows"][1]["resets_at"] = "not-a-time"
    assert evaluate_quota(usage, {"a": 50, "b": 50})["retry_at"] is None


def test_summaries_read_existing_artifacts_without_rendering(tmp_path, monkeypatch):
    """Genuine V2 native render restores Claude's useful model/context contract."""
    from test_transcript import _render_owned_source
    from ccx import client as client_module
    from ccx.claude import transcript as T

    src = tmp_path / "native-context.jsonl"
    src.write_text("\n".join(json.dumps(event) for event in [
        {"type": "user", "message": {"content": "hello"}},
        {"type": "assistant", "message": {
            "model": "claude-opus-observed", "content": [
                {"type": "text", "text": "real model response"},
            ], "usage": {"input_tokens": 18422, "output_tokens": 7},
        }},
    ]) + "\n")
    worker_id, _dst, rec = _render_owned_source(tmp_path, monkeypatch, src)
    T.render_transcript_json(worker_id)
    assert rec["native"]["sessionId"] != worker_id

    for renderer_module in (T, client_module):
        monkeypatch.setattr(renderer_module, "render_transcript_json", lambda *_a, **_k:
                            pytest.fail("summaries must never render"))
    row = WorkerClient().summaries([f"claude:{worker_id}"])["workers"][0]
    assert row["worker"] == f"claude:{worker_id}" and row["worker_id"] == worker_id
    assert row["observed_model"] == "claude-opus-observed"
    assert row["context"]["state"] == "known"
    assert row["context"]["tokens_used"] == 18422
    assert row["context"]["as_of_turn_ref"] is None
    assert row["context"]["window_size"] is None
    assert row["error"] is None


def test_summaries_codex_context_and_zero_turn_states(active_api_root):
    store = require_ccx_active(active_api_root)
    store.mutate_record(B, patches={
        "createArgs.model": "requested-model-not-observed",
        "resolvedModel": "recorded-verified-model", "lastTurnAt": 42,
        "context": {"state": "known", "tokens_used": 16607,
                    "window_size": 258400, "percent_used": 6.43,
                    "as_of_turn_ref": None, "as_of_ms": 42},
    })
    rows = WorkerClient().summaries([f"codex:{B}", f"claude:{A}"])["workers"]
    assert rows[0]["worker_id"] == B and rows[0]["observed_model"] == "recorded-verified-model"
    assert rows[0]["context"]["tokens_used"] == 16607
    assert rows[0]["context"]["percent_used"] == 6.43
    assert rows[0]["context"]["as_of_turn_ref"] is None
    assert rows[1]["worker_id"] == A and rows[1]["context"]["state"] == "not_started"
    assert rows[1]["observed_model"] is None


# --- recap: full-UUID recorded-only public read ------------------------------


def test_recap_preserves_input_order_and_deduplicates(active_api_root):
    store = require_ccx_active(active_api_root)
    another = "aaaaaaaa-9876-4000-8000-000000000099"
    add_worker(active_api_root, another, "claude")
    facade = WorkerClient()
    result = facade.recap([
        f"codex:{B}", A, {"worker": f"claude:{another}"},
        f"claude:{A}", {"worker": B}, another,
    ])
    assert result["count"] == 3 and result["errors"] == {}
    rows = result["workers"]
    assert [row["worker"] for row in rows] == [
        f"codex:{B}", f"claude:{A}", f"claude:{another}",
    ]
    assert [row["worker_id"] for row in rows] == [B, A, another]
    assert all(row["short"] == "aaaaaaaa" for row in rows)
    assert all(row["error"] is None for row in rows)
    assert all(line.startswith(row["worker"]) for line, row
               in zip(result["text"].splitlines()[1:], rows))


def test_recap_rejects_a_bare_worker_instead_of_iterating_it(active_api_root):
    facade = WorkerClient()
    for invalid in (
        A, {"worker": f"claude:{A}"}, b"not-handles", None,
        [A[:8]], ["display is not a key"], [f"codex:{A}"],
        [f"claude:{A[:8]}"], [{"worker": None}],
    ):
        with pytest.raises(InvalidRequest):
            facade.recap(invalid)


def test_recap_row_shape_is_identical_for_both_engines_and_for_failures(
    active_api_root,
):
    store = require_ccx_active(active_api_root)
    store.mutate_record(A, patches={
        "lifecycle.state": S.CRASHED, "stats.turns": 2,
    })
    store.mutate_record(B, patches={
        "lifecycle.state": S.IDLE, "stats.turns": 3,
    })
    result = WorkerClient().recap([A, B, f"claude:{UUID_C}"])
    assert result["count"] == 3
    claude, codex, missing = result["workers"]
    keys = {
        "worker", "worker_id", "engine", "short", "name",
        "state", "native_state", "requested_model", "observed_model",
        "requested_effort", "turns", "context_tokens", "context_as_of_turn_ref",
        "context_as_of_ms", "error",
    }
    assert all(set(row) == keys for row in result["workers"])
    assert (claude["state"], claude["native_state"], claude["turns"]) == (
        "failed", "crashed", 2,
    )
    assert (codex["state"], codex["native_state"], codex["turns"]) == (
        "idle", "idle", 3,
    )
    assert missing["worker_id"] == UUID_C and missing["engine"] == "claude"
    assert missing["state"] == "unavailable"
    assert missing["error"] == "SessionNotFound"
    assert result["errors"][f"claude:{UUID_C}"]["exit_code"] == SessionNotFound.exit_code
    assert result["text"].splitlines()[0] == (
        "3 workers: idle 1, failed 1, unavailable 1"
    )
    assert f"codex:{B}  " in result["text"]
    assert "idle" in result["text"]
    assert f"claude:{UUID_C}  unavailable  error=SessionNotFound" in result["text"]
    bare_missing = WorkerClient().recap([UUID_C])["workers"][0]
    assert bare_missing["engine"] is None and bare_missing["worker_id"] == UUID_C


def test_recap_reports_requested_and_observed_model_separately(active_api_root):
    store = require_ccx_active(active_api_root)
    effort_only = "cccccccc-5678-4000-8000-000000000004"
    add_worker(active_api_root, effort_only, "claude")
    store.mutate_record(A, patches={
        "createArgs.model": "requested-claude", "createArgs.effort": "high",
    })
    store.mutate_record(B, patches={
        "createArgs.model": "requested-codex", "createArgs.effort": "medium",
        "resolvedModel": "observed-codex",
    })
    store.mutate_record(effort_only, patches={
        "createArgs.model": None, "createArgs.effort": "low",
    })
    result = WorkerClient().recap([A, B, effort_only])
    claude, codex, effort = result["workers"]
    assert (claude["requested_model"], claude["observed_model"],
            claude["requested_effort"]) == ("requested-claude", None, "high")
    assert (codex["requested_model"], codex["observed_model"],
            codex["requested_effort"]) == (
        "requested-codex", "observed-codex", "medium",
    )
    assert (effort["requested_model"], effort["observed_model"],
            effort["requested_effort"]) == (None, None, "low")
    assert all("observed_effort" not in row for row in result["workers"])
    assert "model=requested-codex->observed-codex" in result["text"]
    assert "effort=low" in result["text"].splitlines()[-1]
    assert "model=" not in result["text"].splitlines()[-1]


def test_recap_carries_context_and_its_as_of_marker(tmp_path, monkeypatch):
    # Genuine trusted producer -> existing observation reader -> recap.
    from test_transcript import _render_owned_source
    from ccx import constants
    from ccx.claude import transcript as transcript_module

    native = tmp_path / "original.jsonl"
    native.write_text(json.dumps({
        "type": "assistant",
        "message": {
            "model": "native-claude-model",
            "usage": {"input_tokens": 1234, "output_tokens": 3},
            "content": [{"type": "text", "text": "answer"}],
        },
    }) + "\n")
    owner, _, _ = _render_owned_source(tmp_path, monkeypatch, native)
    transcript_module.render_transcript_json(owner)
    root = constants.CCX_ROOT
    add_worker(root, B, "codex")
    store = require_ccx_active(root)
    store.mutate_record(B, patches={
        "context": {"tokens_used": 16607, "as_of_turn_ref": None,
                    "as_of_ms": 42, "window_size": 258400},
        "lastTurnAt": 42,
    })
    facade = WorkerClient()
    claude, codex = facade.recap([owner, B])["workers"]
    assert claude["observed_model"] == "native-claude-model"
    assert claude["context_tokens"] == 1234
    assert claude["context_as_of_turn_ref"] == facade.context(owner)["as_of_turn_ref"]
    assert claude["context_as_of_ms"] == facade.context(owner)["as_of_ms"]
    assert codex["context_tokens"] == 16607
    assert (codex["context_as_of_turn_ref"], codex["context_as_of_ms"]) == (None, 42)

    # All readers, including summaries, must honor the same compact fence.
    store.mutate_record(B, patches={"activeCompact": {"id": "compact-1"}})
    assert facade.summaries([B])["workers"][0]["context"]["tokens_used"] is None
    fenced = facade.recap([B])["workers"][0]
    assert fenced["context_tokens"] is None
    assert fenced["context_as_of_turn_ref"] is None
    assert "context=" not in facade.recap([B])["text"]


def test_recap_never_revives_a_worker_or_starts_a_turn(active_api_root, monkeypatch):
    store = require_ccx_active(active_api_root)
    store.mutate_record(A, patches={"lifecycle.state": S.DEAD})
    store.mutate_record(B, patches={"lifecycle.state": S.DEAD})
    before = _worker_summaries_file_snapshot(active_api_root)

    def forbidden(*_a, **_kw):
        pytest.fail("recorded recap must not probe, render, send or revive")

    from ccx import client as client_module
    from ccx.codex import runtime as codex_runtime
    from ccx.claude import transcript as transcript_module
    monkeypatch.setattr(transcript_module, "render_transcript_json", forbidden)
    monkeypatch.setattr(client_module, "render_transcript_json", forbidden)
    monkeypatch.setattr(W, "_respawn", forbidden)
    monkeypatch.setattr(W, "_rpc", forbidden)
    monkeypatch.setattr(codex_runtime, "observe_registry", forbidden)
    monkeypatch.setattr(codex_runtime, "rpc", forbidden)
    monkeypatch.setattr("ccx.rpc.rpc.host_socket_live", forbidden)
    monkeypatch.setattr(ccx.CcxClient, "send", forbidden)
    monkeypatch.setattr(ccx.CodexClient, "send", forbidden)

    result = WorkerClient().recap([A, B])
    assert [row["state"] for row in result["workers"]] == ["stopped", "stopped"]
    assert result["errors"] == {}
    assert _worker_summaries_file_snapshot(active_api_root) == before
    assert not (active_api_root / "sessions").exists()
    assert not (active_api_root / "run").exists()


def test_recap_rejects_foreign_context_observation(tmp_path, monkeypatch):
    """Genuine source provenance failures propagate; never become error rows."""
    from test_transcript import _render_owned_source
    from ccx import constants
    from ccx.claude import transcript as transcript_module
    from ccx.store.root import StorageIntegrityError

    native = tmp_path / "foreign-check.jsonl"
    native.write_text(json.dumps({
        "type": "assistant", "message": {
            "model": "trusted-model",
            "usage": {"input_tokens": 10},
            "content": [{"type": "text", "text": "hello"}],
        },
    }) + "\n")
    owner, _, _ = _render_owned_source(tmp_path, monkeypatch, native)
    transcript_module.render_transcript_json(owner)
    store = require_ccx_active(constants.CCX_ROOT)
    sidecar = store.worker_path(owner, "context-observation.json")
    doc = json.loads(sidecar.read_text())
    doc["workerId"] = B  # wrong owner, same 8-character prefix
    sidecar.write_text(json.dumps(doc))
    sidecar.chmod(0o600)
    before = _worker_summaries_file_snapshot(constants.CCX_ROOT)
    with pytest.raises(StorageIntegrityError, match="identity/provenance"):
        WorkerClient().recap([owner])
    assert _worker_summaries_file_snapshot(constants.CCX_ROOT) == before
