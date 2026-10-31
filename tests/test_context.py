"""Transcript context usage and bounded typed full-UUID adapter regressions.

Pure accounting tests have no disk; adapter tests use disposable sealed workers.
"""

from __future__ import annotations

import json

import pytest

from ccx import CcxClient, CodexClient, WorkerClient, ClaudeContext
from ccx.claude import context as C
from ccx.codex.types import CodexContext
from ccx.errors import InvalidRequest, SessionNotFound, SessionUnavailable
from ccx.store.root import StorageIntegrityError, require_ccx_active
from test_generation_store import A, B, C as UNKNOWN_UUID, add_worker
from test_python_worker_api import active_api_root, _worker_summaries_file_snapshot


def _assistant(model=None, *, input=None, cache_read=None, cache_creation=None,
               output=None, ts=None, text="ok"):
    tokens = {}
    if input is not None:
        tokens["input"] = input
    if output is not None:
        tokens["output"] = output
    if cache_read is not None:
        tokens["cache_read_input"] = cache_read
    if cache_creation is not None:
        tokens["cache_creation_input"] = cache_creation
    turn = {"role": "assistant", "ts": ts, "text": text, "tools": []}
    if model is not None:
        turn["model"] = model
    if tokens:
        turn["tokens"] = tokens
    return turn


def _user(text="hi", ts=None):
    return {"role": "user", "ts": ts, "text": text}


def _compact_boundary(*, ts=None, post_tokens=3000):
    return {
        "role": "system",
        "kind": "compact_boundary",
        "ts": ts,
        "data": {
            "summary": "Conversation compacted",
            "pre_tokens": 12000,
            "post_tokens": post_tokens,
            "duration_ms": 42,
            "trigger": "manual",
        },
    }


# --- compute_context_usage ---------------------------------------------------

def test_no_turns_returns_none():
    assert C.compute_context_usage([]) is None
    assert C.compute_context_usage(None) is None


def test_turns_without_token_data_is_pending():
    turns = [_user(), _assistant(model="claude-opus-4-8")]  # no tokens
    ctx = C.compute_context_usage(turns)
    assert ctx == {
        "state": "pending", "tokens_used": None,
        "as_of_turn_ref": None, "as_of_ms": None,
    }


def test_occupancy_sums_input_and_cache_only():
    turns = [
        _user(),
        _assistant(model="claude-opus-4-8", input=1000, cache_read=5000,
                   cache_creation=4000, output=2000, ts=10),
    ]
    ctx = C.compute_context_usage(turns)
    # Output is excluded from the carried input footprint and public shape.
    assert ctx == {
        "state": "known", "tokens_used": 10_000,
        "as_of_turn_ref": None, "as_of_ms": 10,
    }


def test_latest_token_bearing_turn_sets_carried_context():
    turns = [
        _user(),
        _assistant(model="claude-opus-4-8", input=150_000, ts=10),
        _user(),
        _assistant(model="claude-opus-4-8[1m]", input=300_000, cache_read=50_000,
                   ts=20),
    ]
    ctx = C.compute_context_usage(turns)
    assert ctx["tokens_used"] == 350_000
    assert ctx["as_of_turn_ref"] is None
    assert ctx["as_of_ms"] == 20


def test_compact_boundary_newer_than_usage_returns_explicit_unknown():
    turns = [
        _assistant(input=1, cache_read=12000, ts=10),
        _compact_boundary(ts=20, post_tokens=3000),
    ]
    assert C.compute_context_usage(turns) == {
        "state": "unknown",
        "tokens_used": None,
        "reason": "just_compacted",
        "as_of_turn_ref": None,
        "as_of_ms": 20,
    }


def test_compact_boundary_with_invalid_post_tokens_is_still_unknown_not_zero():
    turns = [
        _assistant(input=12000, ts=10),
        _compact_boundary(ts=20, post_tokens=None),
    ]
    ctx = C.compute_context_usage(turns)
    assert ctx["state"] == "unknown"
    assert ctx["reason"] == "just_compacted"
    assert ctx["tokens_used"] is None


def test_valid_usage_after_compact_boundary_restores_known_context():
    turns = [
        _assistant(input=12000, ts=10),
        _compact_boundary(ts=20),
        _assistant(input=2, cache_read=3000, cache_creation=400, ts=30),
    ]
    assert C.compute_context_usage(turns) == {
        "state": "known",
        "tokens_used": 3402,
        "as_of_turn_ref": None,
        "as_of_ms": 30,
    }


def test_zero_synthetic_usage_after_compact_does_not_clear_unknown_fence():
    turns = [
        _assistant(input=12000, ts=10),
        _compact_boundary(ts=20),
        _assistant(
            model="<synthetic>", input=0, cache_read=0,
            cache_creation=0, output=0, ts=30,
        ),
    ]
    ctx = C.compute_context_usage(turns)
    assert ctx["state"] == "unknown"
    assert ctx["reason"] == "just_compacted"


def test_latest_of_multiple_compact_boundaries_governs():
    turns = [
        _assistant(input=12000, ts=10),
        _compact_boundary(ts=20),
        _assistant(input=4000, ts=30),
        _compact_boundary(ts=40, post_tokens=2000),
    ]
    ctx = C.compute_context_usage(turns)
    assert ctx["state"] == "unknown"
    assert ctx["as_of_turn_ref"] is None
    assert ctx["as_of_ms"] == 40


def test_model_identity_does_not_change_public_shape():
    turns = [_assistant(model="some-other-llm", input=1234)]
    ctx = C.compute_context_usage(turns)
    assert ctx == {
        "state": "known", "tokens_used": 1234,
        "as_of_turn_ref": None, "as_of_ms": None,
    }


def test_prefer_model_is_accepted_for_caller_compatibility():
    turns = [_assistant(input=1000)]  # no model on the turn
    ctx = C.compute_context_usage(turns, prefer_model="claude-opus-4-8")
    assert ctx["tokens_used"] == 1000


def test_latest_token_bearing_turn_wins_over_trailing_user():
    turns = [
        _assistant(model="claude-opus-4-8", input=5000, ts=10),
        _user(text="another prompt", ts=20),  # trailing user, no tokens
    ]
    ctx = C.compute_context_usage(turns)
    assert ctx["tokens_used"] == 5000
    assert ctx["as_of_turn_ref"] is None


def test_all_zero_synthetic_turn_is_skipped():
    # A rate-limit / API-error reply writes an all-zero usage turn with model
    # "<synthetic>". The anchor must fall back to the last real turn instead.
    turns = [
        _user(),
        _assistant(model="claude-opus-4-8", input=1, cache_read=27051,
                   cache_creation=1791, output=1399, ts=10),
        _user(),
        _assistant(model="<synthetic>", input=0, cache_read=0, cache_creation=0,
                   output=0, ts=20),
    ]
    ctx = C.compute_context_usage(turns)
    assert ctx["state"] == "known"
    assert ctx["tokens_used"] == 28843   # 1 + 27051 + 1791
    assert ctx["as_of_turn_ref"] is None


def test_all_zero_synthetic_turns_all_skipped_falls_back_to_pending():
    # When every assistant turn is degenerate (no real signal), state is pending.
    turns = [
        _user(),
        _assistant(model="<synthetic>", input=0, cache_read=0, cache_creation=0,
                   output=0),
    ]
    ctx = C.compute_context_usage(turns)
    assert ctx["state"] == "pending"


def test_tiny_but_nonzero_turn_is_not_skipped():
    # A genuine turn with input=1 (trivially small) still qualifies as the anchor.
    turns = [_assistant(model="claude-opus-4-8", input=1, ts=5)]
    ctx = C.compute_context_usage(turns)
    assert ctx["state"] == "known"
    assert ctx["tokens_used"] == 1


# --- statusline overlay ------------------------------------------------------

def _sidecar_doc(**overrides):
    doc = {
        "schemaVersion": 1,
        "writtenAtMs": 1_000_000,
        "short": "abc12345",
        "sessionId": "sid",
        "model": {"id": "claude-opus-4-8[1m]", "display_name": "Opus"},
        "statuslineVersion": "2.1.132",
        "contextWindow": {
            "used_percentage": 12.34,
            "remaining_percentage": 87.66,
            "total_input_tokens": 123_400,
            "total_output_tokens": 500,
            "context_window_size": 1_000_000,
        },
        "exceeds200k": False,
    }
    doc.update(overrides)
    return doc


def test_statusline_context_normalizes_fresh_sidecar():
    ctx = C.statusline_context_from_sidecar(_sidecar_doc(), now_ms=1_005_000)
    assert ctx == {
        "state": "known", "tokens_used": 123_400,
        "as_of_turn_ref": None, "as_of_ms": 1_000_000,
    }


def test_statusline_context_rejects_stale_or_unusable_sidecar():
    assert C.statusline_context_from_sidecar(
        _sidecar_doc(), now_ms=1_200_001, ttl_s=90,
    ) is None
    assert C.statusline_context_from_sidecar(
        _sidecar_doc(contextWindow={}), now_ms=1_005_000,
    ) is None


def test_merge_context_usage_prefers_statusline_when_present():
    transcript = {"state": "known", "tokens_used": 50_000}
    statusline = {"state": "known", "tokens_used": 51_000}
    assert C.merge_context_usage(transcript, statusline) is statusline
    assert C.merge_context_usage(transcript, None) is transcript


def test_merge_context_usage_compaction_fence_beats_statusline():
    transcript = C.compaction_unknown_context(
        "just_compacted", as_of_ms=1000,
    )
    stale_statusline = {
        "state": "known", "tokens_used": 350_000,
        "as_of_turn_ref": None, "as_of_ms": 1001,
    }
    assert C.merge_context_usage(transcript, stale_statusline) is transcript


def test_merge_context_usage_pending_compact_beats_zero_statusline():
    transcript = C.compaction_unknown_context("compacting", as_of_ms=1000)
    zero_statusline = {
        "state": "known", "tokens_used": 0,
        "as_of_turn_ref": None, "as_of_ms": 1001,
    }
    assert C.merge_context_usage(transcript, zero_statusline) is transcript


def test_merge_context_usage_rejects_statusline_older_than_transcript_usage():
    transcript = {
        "state": "known", "tokens_used": 30_000,
        "as_of_turn_ref": None, "as_of_ms": 2000,
    }
    older_statusline = {
        "state": "known", "tokens_used": 0,
        "as_of_turn_ref": None, "as_of_ms": 1000,
    }
    assert C.merge_context_usage(transcript, older_statusline) is transcript


def test_merge_context_usage_accepts_statusline_at_least_as_new_as_transcript():
    transcript = {
        "state": "known", "tokens_used": 30_000,
        "as_of_turn_ref": None, "as_of_ms": 2000,
    }
    newer_statusline = {
        "state": "known", "tokens_used": 31_000,
        "as_of_turn_ref": None, "as_of_ms": 2001,
    }
    assert C.merge_context_usage(transcript, newer_statusline) is newer_statusline


# --- original full-UUID typed context adapters (no native services on reads) ---


_CONTEXT_FIELDS = (
    "worker", "worker_id", "short", "engine", "state", "tokens_used",
    "window_size", "percent_used", "as_of_turn_ref",
    "as_of_ms", "reason",
)


def test_typed_claude_context_observation_parity(tmp_path, monkeypatch):
    from ccx import constants, client as claude_mod
    from ccx.claude import transcript as transcript_mod
    from test_transcript import _render_owned_source

    native = tmp_path / "real-assistant.jsonl"
    native.write_text(json.dumps({
        "type": "assistant", "message": {
            "model": "observed-native-model",
            "usage": {"input_tokens": 1234, "output_tokens": 7},
            "content": [{"type": "text", "text": "answer"}],
        },
    }) + "\n")
    owner, _rendered, _record = _render_owned_source(tmp_path, monkeypatch, native)
    transcript_mod.render_transcript_json(owner)  # producer/setup only
    root = constants.CCX_ROOT
    add_worker(root, B, "claude")  # same eight-character prefix, no observation
    before = _worker_summaries_file_snapshot(root)

    def forbidden(*_args, **_kwargs):
        pytest.fail("typed context must not render, contact host or run provider")

    monkeypatch.setattr(claude_mod, "render_transcript_json", forbidden)
    monkeypatch.setattr(claude_mod, "_rpc", forbidden)
    client, workers = CcxClient(), WorkerClient()
    for worker_id, tokens in ((owner, 1234), (B, None)):
        typed = client.context(f"claude:{worker_id}")
        facade = workers.context(worker_id)
        assert isinstance(typed, ClaudeContext)
        assert typed.worker_id == worker_id and typed.short == worker_id[:8]
        assert typed.name == require_ccx_active(root).record(worker_id).get("name")
        assert typed.as_dict()["worker"] == f"claude:{worker_id}"
        assert all(typed.as_dict()[key] == facade[key] for key in _CONTEXT_FIELDS)
        assert typed.tokens_used == tokens
        assert typed.window_size is None and typed.percent_used is None
        assert typed.reason == (None if tokens is not None else "no-signal")
    assert owner[:8] == B[:8] and owner != B
    assert _worker_summaries_file_snapshot(root) == before


def test_typed_codex_context_record_and_compact_fence(active_api_root, monkeypatch):
    from ccx.codex import client as codex_mod

    store = require_ccx_active(active_api_root)
    store.mutate_record(B, patches={
        "lifecycle.state": "idle",
        "context": {
            "tokens_used": 91, "window_size": 1234, "percent_used": 7.37,
            "as_of_turn_ref": None, "as_of_ms": 42,
        },
        "lastTurnAt": 42,
    })

    def forbidden(*_args, **_kwargs):
        pytest.fail("typed context must not use legacy/native Codex runtime")

    monkeypatch.setattr(codex_mod, "record", forbidden)
    monkeypatch.setattr(codex_mod, "rpc", forbidden)
    before = _worker_summaries_file_snapshot(active_api_root)
    client, workers = CodexClient(), WorkerClient()
    for compacting in (False, True):
        if compacting:
            store.mutate_record(B, patches={"activeCompact": {"id": "compact-1"}})
            before = _worker_summaries_file_snapshot(active_api_root)
        typed = client.context(f"codex:{B}")
        facade = workers.context(B)
        assert isinstance(typed, CodexContext)
        assert (typed.worker_id, typed.short, typed.thread_id) == (
            B, B[:8], f"native-thread-{B}",
        )
        assert typed.as_dict()["worker"] == f"codex:{B}"
        assert all(typed.as_dict()[key] == facade[key] for key in _CONTEXT_FIELDS)
        assert typed.state == "idle" and typed.as_of_turn_ref is None
        assert typed.window_size == 1234 and typed.as_of_ms == 42
        if compacting:
            assert typed.reason == "compacting"
            assert typed.tokens_used is None and typed.percent_used is None
        else:
            assert typed.reason is None
            assert typed.tokens_used == 91 and typed.percent_used == 7.37
        assert _worker_summaries_file_snapshot(active_api_root) == before


def test_typed_context_full_uuid_preflight_and_no_effects(active_api_root, monkeypatch):
    from ccx import client as claude_mod
    from ccx.codex import client as codex_mod

    def forbidden(*_args, **_kwargs):
        pytest.fail("refused or recorded-only context must not use native routes")

    monkeypatch.setattr(claude_mod, "render_transcript_json", forbidden)
    monkeypatch.setattr(claude_mod, "_rpc", forbidden)
    monkeypatch.setattr(codex_mod, "record", forbidden)
    monkeypatch.setattr(codex_mod, "rpc", forbidden)
    before = _worker_summaries_file_snapshot(active_api_root)
    claude, codex = CcxClient(), CodexClient()
    assert A[:8] == B[:8] and A != B
    assert (claude.context(A).tokens_used, claude.context(A).reason) == (
        None, "no-signal",
    )
    assert (codex.context(B).tokens_used, codex.context(B).reason) == (
        None, "no-signal",
    )
    with pytest.raises(SessionUnavailable, match="Only CCX-managed"):
        claude.context("not-an-owned-uuid", live=True)  # live refuses before target
    for client, target, error in (
        (claude, A[:8], InvalidRequest),
        (claude, "display is not a key", InvalidRequest),
        (claude, B, InvalidRequest),
        (claude, UNKNOWN_UUID, SessionNotFound),
        (codex, B[:8], InvalidRequest),
        (codex, A, InvalidRequest),
        (codex, UNKNOWN_UUID, SessionNotFound),
    ):
        with pytest.raises(error):
            client.context(target)
    assert _worker_summaries_file_snapshot(active_api_root) == before


def test_typed_claude_context_fails_closed_on_foreign_observation(tmp_path, monkeypatch):
    from ccx import constants
    from ccx.claude import transcript as transcript_mod
    from test_transcript import _render_owned_source

    native = tmp_path / "real-sourced.jsonl"
    native.write_text(json.dumps({
        "type": "assistant", "message": {
            "model": "observed-native",
            "usage": {"input_tokens": 11},
            "content": [{"type": "text", "text": "done"}],
        },
    }) + "\n")
    owner, _rendered, _record = _render_owned_source(tmp_path, monkeypatch, native)
    transcript_mod.render_transcript_json(owner)
    root = constants.CCX_ROOT
    store = require_ccx_active(root)
    sidecar = store.worker_path(owner, "context-observation.json")
    document = json.loads(sidecar.read_text())
    document["workerId"] = B  # plausible same-prefix foreign owner
    sidecar.write_text(json.dumps(document))
    sidecar.chmod(0o600)
    before = _worker_summaries_file_snapshot(root)
    with pytest.raises(StorageIntegrityError, match="identity/provenance"):
        CcxClient().context(owner)
    assert _worker_summaries_file_snapshot(root) == before
