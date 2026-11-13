"""Live-test findings O1 (unmatched Claude prompts) and O2 (Codex false interrupt)."""
import json
import threading
from types import SimpleNamespace

import pytest

from ccx.codex import delivery as codex_delivery
from ccx.ops import conversation
from ccx.claude import transcript
from ccx.codex.daemon.daemon import CodexDaemon
from test_generation_store import short_root  # noqa: F401
from test_worker_telemetry import original_codex, original_sources  # noqa: F401


def _user_row(tmp_path, content):
    path = tmp_path / "t.jsonl"
    path.write_text(json.dumps({"type": "user", "uuid": "u1",
                                "message": {"content": content}}) + "\n")
    return path


def test_escaped_tag_matches_only_the_pasted_content_tag(tmp_path):
    sent = "Tag test <pasted_content>x</pasted_content>. Reply TAGOK."
    path = _user_row(tmp_path, "Tag test <\\pasted_content>x<\\/pasted_content>. Reply TAGOK.")
    assert transcript.exact_user_prompt_event(path, sent)["uuid"] == "u1"
    path = _user_row(tmp_path, "explain a <\\b")
    assert transcript.exact_user_prompt_event(path, "explain a <b") is None


def test_bare_image_path_matches_its_image_placeholder(tmp_path):
    placeholder = [{"type": "text", "text": "[Image #1]"},
                   {"type": "image", "source": {"type": "base64", "data": "AA=="}}]
    path = _user_row(tmp_path, placeholder)
    for sent in ("/tmp/work/red.png", "/tmp/shots/My Shot.JPEG\n"):
        assert transcript.exact_user_prompt_event(path, sent)["uuid"] == "u1"
    # ccx pastes `~` paths as text, so they never become images.
    for sent in ("look at /tmp/red.png", "/tmp/notes.txt", "~/shots/My Shot.JPEG"):
        assert transcript.exact_user_prompt_event(path, sent) is None
    # A different image path is a different prompt.
    path = _user_row(tmp_path, "/tmp/blue.jpg")
    assert transcript.exact_user_prompt_event(path, "/tmp/red.png") is None
    queue = tmp_path / "q.jsonl"
    queue.write_text(json.dumps({"type": "queue-operation", "operation": "enqueue",
                                 "content": "/tmp/blue.jpg"}) + "\n")
    assert not transcript.queued_prompt_accepted(queue, "/tmp/red.png")


def test_image_meta_row_stays_in_its_turn():
    events = [
        {"type": "user", "uuid": "u1", "timestamp": "2026-10-29T21:36:43Z",
         "message": {"content": [{"type": "text", "text": "[Image #1]"},
                                 {"type": "image", "source": {"type": "base64",
                                  "media_type": "image/png", "data": "AA=="}}]}},
        {"type": "user", "uuid": "u2", "isMeta": True, "timestamp": "2026-10-29T21:36:43Z",
         "message": {"content": [{"type": "text",
                                  "text": "[Image: source: /tmp/red.png]"}]}},
        {"type": "assistant", "uuid": "a1", "timestamp": "2026-10-29T21:36:46Z",
         "message": {"id": "m1", "content": [{"type": "text", "text": "A red square."}]}},
    ]
    turns, _model, _records = conversation._normalize_claude_events(
        events, conversation._Counters(), None)
    assert len(turns) == 1
    assert [item["kind"] for item in turns[0]["output"]] == ["assistant-text"]


@pytest.mark.parametrize("still_running,loaded", [
    (True, True), (False, True), (True, False)])
def test_codex_failure_waits_only_while_this_runtime_runs_the_turn(
        original_codex, monkeypatch, still_running, loaded):
    store, _, wid, tid = original_codex
    target, turn = "codex:" + wid, "original-turn"
    daemon = object.__new__(CodexDaemon)
    daemon._uuid_store = SimpleNamespace(
        revalidate=store.revalidate, resolve=store.resolve, record=store.record,
        codex_source=store.codex_source,
        read_receipt=lambda *a: {"state": "accepted", "workerId": wid})
    monkeypatch.setattr(codex_delivery, "read_receipt",
                        lambda *a: {"state": "accepted", "turnId": turn})
    def original_native(method, params, timeout=2):
        assert params["threadId"] == tid
        if method == "thread/read":
            assert params["includeTurns"] is False
            return {"thread": {"id": tid, "threadSource": store.codex_source(target),
                               "turns": []}}
        assert method == "thread/turns/list"
        return {"data": [{"id": turn, "status": "interrupted", "error": None,
                          "items": [], "itemsView": "full"}], "nextCursor": None}

    daemon.client = SimpleNamespace(request=original_native)
    daemon.lock = threading.RLock()
    # After a codexd restart the worker is not loaded: the native answer stands.
    daemon.workers = {}
    if still_running:
        # An adopted, not yet loaded worker keeps its old active turn id.
        daemon.workers[wid] = SimpleNamespace(
            loaded=loaded, record={"lifecycle": {"activeTurnId": turn}})
    got = daemon._original_full_uuid_native({
        "op": "native-observe", "workerId": wid,
        "turnId": turn, "deliveryId": "original-delivery"})
    assert got["status"] == ("pending" if still_running and loaded else "interrupted")
