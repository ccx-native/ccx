"""Claude Python logs: original full-UUID-owned native source only (no providers)."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from ccx import constants
from ccx.store import paths
from ccx.claude import transcript
from ccx.client import CcxClient
from ccx.errors import InvalidRequest, SessionNotFound, SessionUnavailable
from ccx.ops.creation_steps import prepare_worker
from ccx.store.store import GenerationStore
from ccx.store.root import StorageIntegrityError, require_ccx_active
from test_transcript import _render_owned_source
from test_generation_store import A, B, C, G2, add_worker, privdir, switch_pointer


def _source(tmp_path: Path, *events: dict) -> Path:
    file = tmp_path / "fake-native-source.jsonl"
    file.write_text("".join(json.dumps(e, ensure_ascii=False) + "\n" for e in events),
                    encoding="utf-8")
    file.chmod(0o600)
    return file


def _user(text: str) -> dict:
    return {"type": "user", "message": {"content": text}}


def _assistant(text: str, *, model: str = "fake-claude") -> dict:
    return {"type": "assistant", "message": {
        "model": model, "content": [{"type": "text", "text": text}],
        "usage": {"input_tokens": 123, "output_tokens": 8},
    }}


def test_claude_logs_full_uuid_clean_tail_raw_and_same_short_two_originals(
    tmp_path, monkeypatch,
):
    source = _source(tmp_path, _user("first prompt"), _assistant("first reply"),
                     _user("second prompt"), _assistant("second reply 🎯"))
    owner, rendered, rec_a = _render_owned_source(tmp_path, monkeypatch, source)
    assert owner == A and rec_a["native"]["sessionId"] != A
    store = require_ccx_active(constants.CCX_ROOT)
    prepared = prepare_worker(store, engine="claude", cwd=str(tmp_path), worker_id=B)
    rec_b = store.record(prepared.worker)
    assert A[:8] == B[:8] and rec_b["native"]["sessionId"] != rec_a["native"]["sessionId"]
    original_b = paths.transcript_path_for(rec_b["cwd"], rec_b["native"]["sessionId"])
    privdir(original_b.parent)
    original_b.write_text(json.dumps(_assistant("B ONLY", model="another-native-source")) + "\n")
    original_b.chmod(0o600)
    store.mutate_record(A, patches={"lifecycle.state": "dead"})

    client = CcxClient()
    # A raw read is a full pinned-native view, including unfinished JSONL and
    # malformed UTF-8 replacement, without creating either derived sidecar.
    native_a = Path(rec_a["transcript"])
    with native_a.open("ab") as f:
        f.write(b'{"type":"assistant","message":\xff')
    expected = native_a.read_bytes().decode("utf-8", errors="replace")
    assert client.logs(A, jsonl=True, tail=1) == expected
    assert expected.endswith('"message":\ufffd')
    assert not rendered.exists()
    assert not store.worker_path(A, "context-observation.json").exists()

    doc_a = client.logs(A)
    assert doc_a == json.loads(rendered.read_text())
    assert doc_a["schemaVersion"] == 2 and "short" not in doc_a
    assert doc_a["worker"] == f"claude:{A}" and doc_a["sessionId"] == rec_a["native"]["sessionId"]
    assert doc_a["transcriptJsonl"] == rec_a["transcript"]
    assert [turn["role"] for turn in doc_a["turns"]] == [
        "user", "assistant", "user", "assistant",
    ]
    assert "second reply 🎯" == doc_a["turns"][-1]["text"]
    assert client.logs(A, tail=0)["turns"] == doc_a["turns"]
    assert client.logs(A, tail=1)["turns"] == doc_a["turns"][-1:]
    doc_b = client.logs(f"claude:{B}")
    assert doc_b["worker"] == f"claude:{B}" and doc_b["sessionId"] == rec_b["native"]["sessionId"]
    assert [t["text"] for t in doc_b["turns"]] == ["B ONLY"]
    assert "B ONLY" not in json.dumps(doc_a)
    add_worker(constants.CCX_ROOT, C, "codex")
    for target in (A[:8], "demo"):
        with pytest.raises(InvalidRequest):
            client.logs(target)
    with pytest.raises(SessionNotFound):
        client.logs(rec_a["native"]["sessionId"])
    with pytest.raises(InvalidRequest):
        client.logs(C)
    with pytest.raises(InvalidRequest):
        client.logs(A, live=True)
    assert not (constants.CCX_ROOT / "sessions").exists()
    assert not (constants.CCX_ROOT / "run").exists()


def test_claude_logs_missing_unsafe_original_and_generation_never_uses_stale_render(
    tmp_path, monkeypatch,
):
    source = _source(tmp_path, _user("original"), _assistant("original reply"))
    _, rendered, rec = _render_owned_source(tmp_path, monkeypatch, source)
    client = CcxClient()
    assert client.logs(A)["turns"][-1]["text"] == "original reply"
    cache = rendered.read_bytes()
    native = Path(rec["transcript"])
    native.unlink()
    for kwargs in ({}, {"jsonl": True}):
        with pytest.raises(SessionNotFound, match="transcript not found"):
            client.logs(A, **kwargs)
    assert rendered.read_bytes() == cache  # cache is never used as a fallback

    # Existing native file reopened through an unsafe symlink must be rejected.
    native.symlink_to(source)
    for kwargs in ({}, {"jsonl": True}):
        with pytest.raises(SessionUnavailable, match="original Claude transcript unavailable"):
            client.logs(A, **kwargs)
    native.unlink()
    native.write_bytes(source.read_bytes())
    native.chmod(0o600)

    original_record = GenerationStore.record

    def malformed(self, target):
        row = original_record(self, target)
        row["transcript"] = str(tmp_path / "forged-path.jsonl")
        return row

    with monkeypatch.context() as patch:
        patch.setattr(GenerationStore, "record", malformed)
        with pytest.raises(SessionUnavailable, match="original Claude transcript unavailable"):
            client.logs(A)
        with pytest.raises(SessionUnavailable, match="original Claude transcript unavailable"):
            client.logs(A, jsonl=True)

    def wrong_native_sid(self, target):
        row = original_record(self, target)
        row["native"] = {**row["native"], "sessionId": C}
        return row

    with monkeypatch.context() as patch:
        patch.setattr(GenerationStore, "record", wrong_native_sid)
        with pytest.raises(SessionUnavailable, match="original Claude transcript unavailable"):
            client.logs(A)
    assert rendered.read_bytes() == cache

    # A fresh G2 claim of the *same* full UUID cannot be adopted by old client.
    switch_pointer(constants.CCX_ROOT, G2)
    fresh = require_ccx_active(constants.CCX_ROOT)
    prepare_worker(fresh, engine="claude", cwd=str(tmp_path), worker_id=A)
    for kwargs in ({}, {"jsonl": True}):
        with pytest.raises(SessionUnavailable, match="generation"):
            client.logs(A, **kwargs)


def test_claude_logs_large_clean_history_append_complete_boundary_and_default_renderer(
    tmp_path, monkeypatch,
):
    long_text = "native-" + "Z" * (4 * 1024 * 1024 + 2048)
    source = _source(tmp_path, _user(long_text), _assistant("first answer"))
    _, rendered, rec = _render_owned_source(tmp_path, monkeypatch, source)
    client = CcxClient()
    first = client.logs(A)
    assert first["turns"][0]["text"] == long_text
    assert rendered.stat().st_size > 4 * 1024 * 1024
    assert first["context"]["tokens_used"] == 123
    with pytest.raises(StorageIntegrityError, match="bounded read size"):
        client._uuid_store.read_worker_sidecar(A, "transcript.json")

    native = Path(rec["transcript"])
    with native.open("ab") as f:
        f.write((json.dumps(_user("later prompt")) + "\n").encode())
        f.write((json.dumps(_assistant("later complete ✅"), ensure_ascii=False) + "\n").encode())
        f.write(b'{"type":"assistant","message":{"content": [')
    second = client.logs(A)
    assert len(second["turns"]) == len(first["turns"]) + 2
    assert second["turns"][-1]["text"] == "later complete ✅"
    assert all(turn.get("text") is not None for turn in second["turns"])
    assert client.logs(A, tail=1)["turns"] == second["turns"][-1:]
    assert client.logs(A, jsonl=True).endswith('{"type":"assistant","message":{"content": [')
    # Existing direct callers still use the default ACTIVE renderer and may
    # ignore the returned document. Its return equals the existing published file.
    default = transcript.render_transcript_json(A)
    assert default["turns"] == second["turns"]
    assert default == json.loads(rendered.read_text())
