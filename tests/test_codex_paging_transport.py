"""Original full-worker Codex bounded item transport: fake native RPC only."""
from __future__ import annotations
import ccx.store.root as store_root

from contextlib import contextmanager

import pytest

from ccx.codex import runtime as codex_runtime
from ccx.codex.paths import root as codex_state_root
from ccx.codex.daemon.daemon import CodexDaemon
from ccx.errors import InvalidRequest, OperationTimeout, SessionNotFound, SessionUnavailable
from ccx.ops.creation_steps import activate_prepared_worker, prepare_worker
from test_generation_store import A
from test_generation_store import short_root  # noqa: F401 - pytest fixture
from test_worker_telemetry import original_codex, original_sources  # noqa: F401 - pytest fixtures


def _ready_original_daemon(store, native):
    # Instantiating a CodexDaemon is inert: no .run(), socket, process or model.
    daemon = CodexDaemon()
    daemon._uuid_store = store
    daemon.client = native
    daemon.ready.set()
    assert daemon.workers == {} and daemon.by_thread == {}
    return daemon


class _NativePages:
    def __init__(self, tid, wid):
        self.thread_id, self.worker_id = tid, wid
        self.calls = []

    def request(self, method, params, timeout=30):
        self.calls.append((method, dict(params), timeout))
        if method == "thread/read":
            assert params["threadId"] == self.thread_id
            return {"thread": {
                "id": self.thread_id,
                "threadSource": f"ccx-worker:{self.worker_id}", "turns": [],
            }}
        if method == "thread/items/list":
            assert params["threadId"] == self.thread_id
            return {
                "data": [
                    {"turnId": "t-new", "item": {"id": "newest", "type": "agentMessage", "text": "n"}},
                    {"turnId": "t-old", "item": {"id": "older", "type": "userMessage", "content": []}},
                ],
                "nextCursor": "opaque-native-next",
            }
        raise AssertionError(f"forbidden native method {method}")


def test_original_items_served_no_metadata_call_and_cold_one_attested_channel(
    original_codex, monkeypatch,
):
    store, cwd, wid, tid = original_codex
    store.mutate_record(wid, patches={"lifecycle.state": "dead"})
    record = store.worker_path(wid, "record.json")
    before = record.read_bytes()
    native = _NativePages(tid, wid)
    daemon = _ready_original_daemon(store, native)
    sent = []

    monkeypatch.setattr(codex_runtime, "observation_daemon", lambda *_a, **_kw: True)

    def served(message, **kw):
        sent.append((message, kw))
        return daemon.handle(message)

    monkeypatch.setattr(codex_runtime, "daemon_call", served)
    page = codex_runtime.list_thread_items(
        tid, worker_id=wid, cwd=str(cwd), expected_source=store.codex_source(wid),
        cursor="opaque-native-input", limit=9, sort_direction="asc", timeout=5,
    )
    assert [row["item"]["id"] for row in page["data"]] == ["newest", "older"]
    assert page["nextCursor"] == "opaque-native-next"
    assert sent[0][0] == {
        "op": "thread-items-list", "workerId": wid, "params": {
            "threadId": tid, "cursor": "opaque-native-input",
            "limit": 9, "sortDirection": "asc",
        },
    }
    assert 0 < sent[0][1]["timeout"] <= 5
    assert [(method, params) for method, params, _ in native.calls] == [
        ("thread/items/list", sent[0][0]["params"]),
    ]  # Served path uses accepted durable binding, NOT a second native read.
    assert native.calls[0][2] == 2
    assert not daemon.workers and not daemon.by_thread
    assert record.read_bytes() == before

    # Actual thread-read daemon branch keeps its original native id/source proof
    # after the same immutable binding check was shared with item listing.
    thread_read = daemon.handle({
        "op": "thread-read", "workerId": wid,
        "threadId": tid, "includeTurns": False,
    })
    assert thread_read["ok"] is True
    assert thread_read["thread"] == {
        "id": tid, "threadSource": f"ccx-worker:{wid}", "turns": [],
    }
    assert native.calls[-1][:2] == ("thread/read", {"threadId": tid, "includeTurns": False})
    assert 29 < native.calls[-1][2] <= 30
    native.calls.clear()

    # Positive no-daemon observation: one read-only app-server connection,
    # metadata proof BEFORE the bounded item request, even if cwd is gone.
    monkeypatch.setattr(codex_runtime, "observation_daemon", lambda *_a, **_kw: False)
    neutral = codex_state_root()
    assert neutral.is_dir()
    cwd.rmdir()
    openings = []

    @contextmanager
    def cold(**kwargs):
        openings.append(kwargs)
        yield native

    monkeypatch.setattr(codex_runtime, "_short_lived_app_server", cold)
    cold_page = codex_runtime.list_thread_items(
        tid, worker_id=wid, cwd=str(cwd), expected_source=store.codex_source(wid),
        cursor="cold-cursor", limit=7, sort_direction="desc", timeout=5,
    )
    assert cold_page["nextCursor"] == "opaque-native-next"
    assert len(openings) == 1 and openings[0]["cwd"] == str(neutral)
    assert [(method, params) for method, params, _ in native.calls] == [
        ("thread/read", {"threadId": tid, "includeTurns": False}),
        ("thread/items/list", {
            "threadId": tid, "cursor": "cold-cursor",
            "limit": 7, "sortDirection": "desc",
        }),
    ]
    assert all(0 < bound <= 5 for _, _, bound in native.calls)
    assert store.record(wid)["cwd"] == str(cwd)  # No project relocation.
    assert record.read_bytes() == before
    assert daemon.workers == {} and daemon.by_thread == {}


def test_old_thread_read_without_include_turns_reconstructs_native_history(original_codex):
    """An older client may omit includeTurns; Codex itself must only see False."""
    store, _cwd, wid, tid = original_codex

    class FakeNative:
        def __init__(self):
            self.calls = []

        def request(self, method, params, timeout=30):
            assert timeout > 0
            assert params["threadId"] == tid
            assert params.get("includeTurns") is not True
            self.calls.append((method, dict(params)))
            if method == "thread/read":
                assert params["includeTurns"] is False
                return {"thread": {
                    "id": tid, "threadSource": store.codex_source(wid),
                    "turns": [],
                }}
            if method == "thread/turns/list":
                assert params["itemsView"] == "full"
                return {"data": [
                    {"id": "native-original-turn", "status": "completed",
                     "items": [{"id": "answer-1", "type": "agentMessage",
                                "text": "original result"}], "itemsView": "full"},
                ], "nextCursor": None}
            raise AssertionError(f"unexpected fake native request {method}")

    native = FakeNative()
    daemon = _ready_original_daemon(store, native)
    # The legacy wire shape has no includeTurns key: old code defaulted True.
    reply = daemon.handle({"op": "thread-read", "workerId": wid, "threadId": tid})
    assert reply["ok"] is True, reply
    assert reply["thread"]["historySource"] == "thread/turns/list"
    assert [t["id"] for t in reply["thread"]["turns"]] == ["native-original-turn"]
    assert reply["thread"]["turns"][0]["items"][0]["text"] == "original result"
    assert [method for method, _ in native.calls] == [
        "thread/read", "thread/turns/list",
    ]

    # Explicit True from the old client also pages; False remains metadata-only.
    native.calls.clear()
    explicit = daemon.handle({"op": "thread-read", "workerId": wid,
                              "threadId": tid, "includeTurns": True})
    assert explicit["thread"] == reply["thread"]
    assert [method for method, _ in native.calls] == [
        "thread/read", "thread/turns/list",
    ]
    native.calls.clear()
    metadata = daemon.handle({"op": "thread-read", "workerId": wid,
                              "threadId": tid, "includeTurns": False})
    assert metadata["ok"] is True and metadata["thread"]["turns"] == []
    assert [method for method, _ in native.calls] == ["thread/read"]


def test_original_items_wrong_owner_source_missing_and_unscoped_refuse_before_native(
    original_codex, monkeypatch,
):
    store, cwd, wid, tid = original_codex
    native = _NativePages(tid, wid)
    daemon = _ready_original_daemon(store, native)

    # The daemon never offers general native thread discovery, with or without
    # a forged full worker identity.
    for message in (
        {"op": "thread-list", "workerId": wid, "params": {"limit": 5}},
        {"op": "thread-items-list", "params": {"threadId": tid, "limit": 3, "sortDirection": "desc"}},
        {"op": "thread-items-list", "workerId": A, "params": {"threadId": tid, "limit": 3, "sortDirection": "desc"}},
        {"op": "thread-items-list", "workerId": wid, "params": {"threadId": "wrong-thread", "limit": 3, "sortDirection": "desc"}},
        {"op": "thread-items-list", "workerId": wid, "params": {"threadId": tid, "limit": 3, "sortDirection": {}}},
    ):
        refused = daemon.handle(message)
        assert refused["ok"] is False and native.calls == []
    unknown = "cccccccc-1234-4000-8000-000000000009"
    unknown_reply = daemon.handle({
        "op": "thread-items-list", "workerId": unknown,
        "params": {"threadId": tid, "limit": 3, "sortDirection": "desc"},
    })
    assert unknown_reply["ok"] is False and unknown_reply["reason"] == "worker-not-found"
    assert native.calls == []

    # Even two Codex workers with a COLLIDING first eight UUID hex digits
    # cannot lend native thread authority to each other.
    other = "bbbbbbbb-5678-4000-8000-000000000004"
    prepared = prepare_worker(store, engine="codex", cwd=str(cwd), worker_id=other)

    def accepted(_budget):
        claim = store.native_activation(other)
        return {"accepted": True, "workerId": other, "engine": "codex",
                "source": f"ccx-worker:{other}", "submissionId": claim["submissionId"],
                "nativeEvidenceSha256": "f" * 64,
                "nativeBinding": {"threadId": "native-thread-other"}}

    assert activate_prepared_worker(store, prepared, bounded_native_submit=accepted).activation_state == "confirmed"
    assert wid[:8] == other[:8]
    conflicting = daemon.handle({
        "op": "thread-items-list", "workerId": other,
        "params": {"threadId": tid, "limit": 3, "sortDirection": "desc"},
    })
    assert conflicting["ok"] is False and native.calls == []

    # Confirmed original activation/source must match nativeBinding itself;
    # source record mutation is an unsafe oracle, never repaired by the pager.
    with monkeypatch.context() as patch:
        original = store.native_activation
        patch.setattr(store, "native_activation", lambda owner: (
            {**original(owner), "nativeBinding": {"threadId": "not-original"}}
            if owner in (wid, f"codex:{wid}") else original(owner)
        ))
        assert daemon.handle({
            "op": "thread-items-list", "workerId": wid,
            "params": {"threadId": tid, "limit": 3, "sortDirection": "desc"},
        })["ok"] is False
    assert native.calls == []

    # The runtime's obsolete unscoped callers cannot contact provider/daemon.
    monkeypatch.setattr(codex_runtime, "observation_daemon",
                        lambda *_a, **_kw: pytest.fail("unscoped transport called daemon"))
    for bad in (None, wid[:8], "display name", tid):
        with pytest.raises(InvalidRequest):
            codex_runtime.list_thread_items(tid, cwd=str(cwd), worker_id=bad)

    # Served failures cannot silently switch to a second app-server; and an
    # unknown canonical CCX owner is NotFound, not a native ID alias.
    monkeypatch.setattr(codex_runtime, "observation_daemon", lambda *_a, **_kw: True)
    monkeypatch.setattr(codex_runtime, "daemon_call", lambda msg, **_kw: daemon.handle(msg))
    monkeypatch.setattr(codex_runtime, "_short_lived_app_server",
                        lambda **_kw: pytest.fail("started a second app-server"))
    with pytest.raises(SessionNotFound):
        codex_runtime.list_thread_items(
            tid, worker_id=unknown, cwd=str(cwd), expected_source=store.codex_source(wid))
    with pytest.raises(SessionUnavailable):
        codex_runtime.list_thread_items(
            "wrong-thread", worker_id=wid, cwd=str(cwd),
            expected_source=store.codex_source(wid))
    assert native.calls == []

    # No callable private cold/served route can restore native thread/list,
    # even if a caller supplies an apparently correct full worker ID.
    with pytest.raises(InvalidRequest, match="Only CCX-managed"):
        codex_runtime._paged_history_request(
            method="thread/list", daemon_op="thread-list",
            params={"threadId": tid, "limit": 5, "sortDirection": "desc"},
            cwd=str(cwd), timeout=5, short=tid, worker_id=wid,
            expected_source=store.codex_source(wid),
        )

    # Cold metadata with a foreign threadSource must refuse before the item RPC.
    monkeypatch.setattr(codex_runtime, "observation_daemon", lambda *_a, **_kw: False)
    cold_calls = []

    @contextmanager
    def foreign(**_kwargs):
        class Observer:
            def request(self, method, params, timeout):
                cold_calls.append(method)
                assert method == "thread/read"
                return {"thread": {"id": tid, "threadSource": "ccx-worker:foreign"}}
        yield Observer()

    monkeypatch.setattr(codex_runtime, "_short_lived_app_server", foreign)
    with pytest.raises(SessionUnavailable, match="belongs to another worker"):
        codex_runtime.list_thread_items(
            tid, worker_id=wid, cwd=str(cwd), expected_source=store.codex_source(wid))
    assert cold_calls == ["thread/read"] and native.calls == []


def test_original_items_cursor_options_single_deadline_and_typed_result_errors(
    original_codex, monkeypatch,
):
    store, cwd, wid, tid = original_codex
    monkeypatch.setattr(codex_runtime, "observation_daemon",
                        lambda *_a, **_kw: pytest.fail("invalid options reached transport"))
    for options in ({"limit": 0}, {"limit": 101}, {"limit": True},
                    {"sort_direction": "latest"}, {"sort_direction": {}}, {"cursor": 1},
                    {"timeout": 0}, {"timeout": -1}):
        with pytest.raises(InvalidRequest):
            codex_runtime.list_thread_items(
                tid, cwd=str(cwd), worker_id=wid,
                expected_source=store.codex_source(wid), **options)

    # Served reply identities and native result shape must BOTH be true.
    monkeypatch.setattr(codex_runtime, "observation_daemon", lambda *_a, **_kw: True)
    reply = {"ok": True, "workerId": wid, "threadId": tid,
             "threadSource": f"ccx-worker:{wid}",
             "data": {"data": [], "nextCursor": "opaque"}}
    monkeypatch.setattr(codex_runtime, "daemon_call", lambda *_a, **_kw: reply)
    assert codex_runtime.list_thread_items(
        tid, cwd=str(cwd), worker_id=wid,
        expected_source=store.codex_source(wid))["nextCursor"] == "opaque"
    for invalid in (
        {**reply, "workerId": "wrong"},
        {**reply, "threadSource": "ccx-worker:foreign"},
        {**reply, "data": {"data": "not-a-list"}},
        {**reply, "data": {"data": [], "nextCursor": 7}},
    ):
        monkeypatch.setattr(codex_runtime, "daemon_call", lambda *_a, _v=invalid, **_kw: _v)
        with pytest.raises(SessionUnavailable):
            codex_runtime.list_thread_items(
                tid, cwd=str(cwd), worker_id=wid, expected_source=store.codex_source(wid))
    for failure, error in (
        ({"ok": False, "reason": "worker-not-found", "error": "owner missing"}, SessionNotFound),
        ({"ok": False, "error": "thread not loaded: native"}, SessionNotFound),
        ({"ok": False, "error": "invalid thread id: broad -32600"}, SessionUnavailable),
    ):
        monkeypatch.setattr(codex_runtime, "daemon_call", lambda *_a, _v=failure, **_kw: _v)
        with pytest.raises(error):
            codex_runtime.list_thread_items(
                tid, cwd=str(cwd), worker_id=wid, expected_source=store.codex_source(wid))

    # A cold observer spends the ONE caller budget on initialization and
    # metadata before the item request. Once expired it must not page or retry.
    monkeypatch.setattr(codex_runtime, "observation_daemon", lambda *_a, **_kw: False)
    timings = iter([2.0, 2.0, 0.0])
    monkeypatch.setattr(codex_runtime, "remaining", lambda *_a, **_k: next(timings))
    cold_calls = []
    openings = []

    @contextmanager
    def cold(**kwargs):
        openings.append(kwargs)
        class Observer:
            def request(self, method, params, timeout):
                cold_calls.append((method, timeout))
                assert method == "thread/read"
                return {"thread": {"id": tid, "threadSource": f"ccx-worker:{wid}"}}
        yield Observer()

    monkeypatch.setattr(codex_runtime, "_short_lived_app_server", cold)
    with pytest.raises(OperationTimeout, match="deadline expired"):
        codex_runtime.list_thread_items(
            tid, cwd=str(cwd), worker_id=wid,
            expected_source=store.codex_source(wid), timeout=2)
    assert len(openings) == 1
    assert cold_calls == [("thread/read", 2.0)]


@pytest.mark.parametrize("op, served", [
    ("thread-read", True), ("thread-items-list", True), ("status", False),
])
def test_history_replies_are_not_held_to_the_control_reply_budget(monkeypatch, op, served):
    """A >1 MiB native history page is served; a control reply stays bounded."""
    import json
    import types
    from ccx.store import store as generation
    from ccx.rpc import transport

    big = json.dumps({"ok": True, "data": "x" * (2 * 1024 * 1024)}).encode() + b"\n"

    class FakeSocket:
        def __init__(self, *_a):
            self.replies = [b'{"ok": true, "identity": {}}\n', big]
            self.pending = b""
        def __enter__(self): return self
        def __exit__(self, *_a): return False
        def settimeout(self, _t): pass
        def connect(self, _p): pass
        def sendall(self, _b):
            self.pending += self.replies.pop(0)
        def recv(self, n):
            chunk, self.pending = self.pending[:n], self.pending[n:]
            return chunk

    store = types.SimpleNamespace(revalidate=lambda: None)
    plan = types.SimpleNamespace(final="/nonexistent.sock", evidence=lambda: {})
    monkeypatch.setattr(store_root, "require_ccx_active", lambda *_a, **_kw: store)
    monkeypatch.setattr(transport, "plan_transport", lambda *_a, **_kw: plan)
    monkeypatch.setattr(transport, "verify_handshake", lambda *_a, **_kw: None)
    monkeypatch.setattr(codex_runtime.socket, "socket", FakeSocket)

    reply = codex_runtime.daemon_rpc({"op": op}, timeout=5)
    if served:
        assert reply["transport"] == codex_runtime.RUNTIME_SERVED
        assert len(reply["data"]) == 2 * 1024 * 1024
    else:
        assert reply["ok"] is False and "ValueError" in reply["error"]
