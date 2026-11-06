"""Python worker clients, exact IDs and native Python passthrough."""
from __future__ import annotations
import ccx.cli.commands.read as cli_read
import ccx.cli.commands.turns as commands_turns
import ccx.ops.create as ops_create
import ccx.ops.send as ops_send
import ccx.ops.wait as ops_wait

import json
import os
from pathlib import Path

import pytest

pytestmark = pytest.mark.usefixtures("executable_engine_stubs")

from test_generation_store import (
    A, B, C, G1, add_worker, fixture_root, put, record, spec, switch_pointer,
)
from ccx import constants
from ccx.errors import InvalidRequest, SessionNotFound, SessionUnavailable
from ccx.store.root import require_ccx_active
from ccx.client import CcxClient
from ccx.codex.client import CodexClient
from ccx.workers import WorkerClient


@pytest.fixture
def active_api_root(tmp_path, monkeypatch):
    root=fixture_root(tmp_path)
    add_worker(root,A,"claude")
    add_worker(root,B,"codex")
    monkeypatch.setattr(constants,"CCX_ROOT",root)
    return root


def test_direct_python_constructor_initializes_only_missing_or_empty(tmp_path, monkeypatch):
    from ccx.store.root import root_status
    for root in (tmp_path / "not-initialized", tmp_path / "missing"):
        if root.name == "not-initialized":
            root.mkdir(mode=0o700)
        monkeypatch.setattr(constants, "CCX_ROOT", root)
        for klass in (CcxClient, CodexClient, WorkerClient):
            client = klass()
            assert client._uuid_store.root == root
            assert client._uuid_store.list_workers() == []
        assert root_status(root).kind == "initialized"
        assert root.stat().st_mode & 0o777 == 0o700
        assert not (root / "run").exists()
    # Unsupported phases cannot authorize a client or provider operation.
    offline = fixture_root(tmp_path / "offline", phase="VERIFY_ONLY")
    monkeypatch.setattr(constants, "CCX_ROOT", offline)
    prior = sorted(str(p.relative_to(offline)) for p in offline.rglob("*"))
    for klass in (CcxClient, CodexClient, WorkerClient):
        with pytest.raises(SessionUnavailable, match="CCX worker records are unavailable"):
            klass()
    assert prior == sorted(str(p.relative_to(offline)) for p in offline.rglob("*"))


def test_ccx_client_list_has_no_retired_external_filter_options():
    import inspect
    from ccx.client import CcxClient

    options = set(inspect.signature(CcxClient.list).parameters)
    assert {"self", "cwd", "cwd_prefix", "state", "recent_hours"} == options
    assert not (options & {"external", "native_bg", "importable", "all",
                           "live", "active", "sendable", "managed"})


def test_direct_ccx_client_real_full_uuid_typed_list_peek_and_cas(active_api_root):
    root=active_api_root
    client=CcxClient()
    result=client.peek(f"claude:{A}")
    assert result.short=="aaaaaaaa" and result.worker_id==A
    assert result.as_dict()["worker"]==f"claude:{A}"
    assert result.host_status=="unobserved"
    batch=client.status([A])
    assert [s.worker_id for s in batch]==[A]
    rows=client.list()
    assert len(rows)==1
    assert rows[0].worker_id==A and rows[0].as_dict()["worker"]==f"claude:{A}"
    assert not ({"mode", "can_send", "can_import", "cost_usd"} & rows[0].as_dict().keys())
    first=client.set_name(A,"direct Python")
    assert first.worker_id==A and first.as_dict()["worker"]==f"claude:{A}"
    assert first.name=="direct Python"
    assert client._uuid_store.record(A)["recordRevision"]==1
    assert client._uuid_store.record(B).get("recordRevision",0)==0
    assert not (root/"sessions").exists()
    assert not (root/"run").exists()


def test_direct_codex_client_real_full_UUID_typed_rename(active_api_root):
    client=CodexClient()
    first=client.set_name(f"codex:{B}","codex Python")
    assert first.worker_id==B and first.thread_id==f"native-thread-{B}"
    assert first.as_dict()["worker"]==f"codex:{B}"
    assert first.name=="codex Python"
    assert client._uuid_store.record(A)["name"]=="display is not a key"
    assert client._uuid_store.record(B)["recordRevision"]==1
    with pytest.raises(InvalidRequest):
        client.set_name(f"claude:{A}","forbidden")


def test_direct_worker_client_real_full_uuid_facade_recorded_reads_and_metadata(active_api_root):
    client=WorkerClient()
    named=client.set_name({"worker":f"codex:{B}"},"via workers")
    assert named["worker"]==f"codex:{B}"
    assert named["record_revision"]==1
    combined=client.list()
    assert [r["worker_id"] for r in combined["workers"]]==[A,B] or {
        r["worker_id"] for r in combined["workers"]}=={A,B}
    assert {r["short"] for r in combined["workers"]}=={"aaaaaaaa"}
    assert all(r["ambiguous_short"] for r in combined["workers"])
    assert combined["errors"]=={}
    cd=client.status(f"codex:{B}")
    assert cd["worker_id"]==B and cd["native_id"]==f"native-thread-{B}"
    assert cd["host"]=="unobserved" and cd["loaded"] is None
    assert cd["state_source"]=="recorded"
    assert "pending_send_count" not in cd
    assert client.list(engine="claude")["workers"][0]["worker_id"]==A
    caps=client.capabilities("claude")
    assert caps == {
        "engine": "claude", "steer_current_turn": True,
        "auto_delivery": True, "wait_next_turn": True,
        "wait_after_turn": True, "wait_known_input": True,
        "answer_question": True, "respond_elicitation": True,
    }
    assert client.capabilities(B) == {
        "engine": "codex", "steer_current_turn": True,
        "auto_delivery": False, "wait_next_turn": False,
        "wait_after_turn": False, "wait_known_input": False,
        "answer_question": False, "respond_elicitation": False,
    }
    assert client.variables(A)["worker"]==f"claude:{A}"
    assert client.variables(A)["available"] is False
    events=client.events(A,tail=0)
    assert events["worker"]==f"claude:{A}" and events["events"]==[]
    held=client.events(A,wait=.1)
    assert held["worker"]==f"claude:{A}" and held["events"]==[]
    assert held["timed_out"] is True
    assert client.list(parent=A)["workers"] == []


@pytest.mark.parametrize("worker_id,native_state", [(A, "budget-stopped"), (B, "dead")])
def test_worker_status_preserves_recorded_lifecycle_alongside_normalized_state(
    active_api_root, worker_id, native_state
):
    store = require_ccx_active(active_api_root)
    store.mutate_record(worker_id, patches={"lifecycle.state": native_state})
    status = WorkerClient().status(worker_id)
    assert status["state"] == "stopped"
    assert status["native_state"] == native_state
    assert status["state_source"] == "recorded"


def test_codex_wait_reports_budget_stop_not_idle_after_capped_interrupt(
    active_api_root, monkeypatch, capsys,
):
    from types import SimpleNamespace
    from ccx.codex import runtime as codex_runtime
    from ccx.ops import common as protocol_ops
    from ccx.cli.commands import _common as commands
    from ccx.constants import BUDGET_EXIT, CODEXD_RPC_GENERATION

    store = require_ccx_active(active_api_root)
    stopped = {"turnId": "codex-cap-turn", "status": "interrupted"}
    store.mutate_record(B, patches={
        "lifecycle.state": "idle", "lastTurn": stopped,
        "quotaStop": {"turnId": "codex-cap-turn", "reason": "usage cap 7d reached"},
    })

    def served(_message, *, timeout):
        return {"ok": True, "loaded": True, "transport": "served",
                "rpcGeneration": CODEXD_RPC_GENERATION, "state": "idle",
                "lastTurn": store.record(B)["lastTurn"]}

    monkeypatch.setattr(codex_runtime, "daemon_rpc", served)
    verdict = ops_wait.wait_worker(f"codex:{B}", timeout=0)
    assert verdict["status"] == "budget-stopped"
    assert verdict["state"] == "idle"
    assert verdict["reason"] == "usage cap 7d reached"
    with pytest.raises(SystemExit) as exited:
        commands_turns._emit_full_uuid_wait(SimpleNamespace(json=True), verdict)
    assert exited.value.code == BUDGET_EXIT
    assert json.loads(capsys.readouterr().out)["status"] == "budget-stopped"

    store.mutate_record(B, patches={"lastTurn": {
        "turnId": "next-uncapped-turn", "status": "completed",
    }})
    assert ops_wait.wait_worker(f"codex:{B}", timeout=0)["status"] == "idle"


def test_worker_status_list_and_recap_agree_on_native_and_normalized_states(active_api_root):
    store = require_ccx_active(active_api_root)
    store.mutate_record(A, patches={"lifecycle.state": "budget-stopped"})
    store.mutate_record(B, patches={"lifecycle.state": "idle-killed"})
    client = WorkerClient()
    listed = {row["worker_id"]: row for row in client.list()["workers"]}
    recapped = {row["worker_id"]: row for row in client.recap([A, B])["workers"]}
    for worker_id, native_state in ((A, "budget-stopped"), (B, "idle-killed")):
        status = client.status(worker_id)
        expected = ("stopped", native_state)
        assert (status["state"], status["native_state"]) == expected
        assert (listed[worker_id]["state"], listed[worker_id]["native_state"]) == expected
        assert (recapped[worker_id]["state"], recapped[worker_id]["native_state"]) == expected
        assert listed[worker_id]["state_source"] == "recorded"
    stopped = client.list(state="stopped")["workers"]
    assert {row["worker_id"] for row in stopped} == {A, B}
    assert client.list(state="idle")["workers"] == []


def test_worker_status_and_list_restore_recorded_model_fields(tmp_path, monkeypatch):
    from ccx.claude import transcript as transcript_module
    from test_transcript import _render_owned_source

    native = tmp_path / "native-models.jsonl"
    native.write_text(json.dumps({
        "type": "assistant", "message": {
            "model": "observed-claude", "usage": {"input_tokens": 24},
            "content": [{"type": "text", "text": "hello"}],
        },
    }) + "\n")
    owner, _, _ = _render_owned_source(tmp_path, monkeypatch, native)
    transcript_module.refresh_context_observation(owner)
    add_worker(constants.CCX_ROOT, B, "codex")
    store = require_ccx_active(constants.CCX_ROOT)
    store.mutate_record(A, patches={
        "createArgs.model": "requested-claude", "createArgs.effort": "high",
    })
    store.mutate_record(B, patches={
        "createArgs.model": "requested-codex", "createArgs.effort": "medium",
        "resolvedModel": "observed-codex",
    })

    client = WorkerClient()
    expected = {
        A: ("requested-claude", "high", "observed-claude"),
        B: ("requested-codex", "medium", "observed-codex"),
    }
    status_keys = None
    for worker_id, values in expected.items():
        status = client.status(worker_id)
        if status_keys is None:
            status_keys = set(status)
        assert set(status) == status_keys  # Claude and Codex share one status shape.
        assert (status["requested_model"], status["requested_effort"],
                status["observed_model"]) == values
        assert "managed_turns" in status and "turns" not in status
    assert client.status(A)["observed_model"] == client.summaries([A])["workers"][0]["observed_model"]

    listed = {row["worker_id"]: row for row in client.list()["workers"]}
    assert set(listed) == set(expected)
    for worker_id, (model, effort, _observed) in expected.items():
        assert (listed[worker_id]["requested_model"],
                listed[worker_id]["requested_effort"]) == (model, effort)
        assert "turns" in listed[worker_id]
        assert "observed_model" not in listed[worker_id]

    store.mutate_record(B, patches={"resolvedModel": None})
    assert client.status(B)["observed_model"] is None  # Never echo the request.


@pytest.mark.parametrize("bad",[
    "aaaaaaaa","claude:aaaaaaaa","same name","claude-session:"+A,
    "codex:"+A,C
])
def test_direct_python_clients_reject_names_prefixes_foreign_or_unknown_ids(active_api_root,bad):
    claude=CcxClient()
    facade=WorkerClient()
    for method in (claude.set_name, facade.status):
        with pytest.raises((InvalidRequest,SessionNotFound)):
            method(bad,"changed") if method==claude.set_name else method(bad)
    assert require_ccx_active(active_api_root).record(A)["name"]=="display is not a key"


def test_missing_full_uuid_raises_for_status_peek_and_batch(active_api_root):
    facade = WorkerClient()
    claude = CcxClient()
    with pytest.raises(SessionNotFound):
        facade.status(C)
    with pytest.raises(SessionNotFound):
        claude.peek(C)
    with pytest.raises(SessionNotFound):
        claude.status([A, C])  # A single missing member fails the whole batch.
    assert [row.worker_id for row in claude.status([A])] == [A]


def test_direct_clients_all_refuse_stale_generation_before_record_mutation(active_api_root):
    c=CcxClient()
    d=CodexClient()
    w=WorkerClient()
    switch_pointer(active_api_root)
    from ccx.store.root import GenerationError
    for method,args in (
        (c.set_name,(A,"stale")),
        (c.peek,(A,)),
        (d.set_name,(B,"stale")),
        (w.set_name,(A,"stale")),
        (w.list,()),
    ):
        with pytest.raises((SessionUnavailable,GenerationError)) as exc:
            method(*args)
        # Domain-specific stale-generation or typed unavailable; no writes to
        # original immutable owner/record after ACTIVE changed.
        assert "generation" in str(exc.value).lower() or "stale" in str(exc.value).lower()
    old=active_api_root/"identity"/"generations"/G1
    assert (old/"workers"/"claude"/A/"record.json").exists()



def test_direct_legacy_local_rpc_dispatch_uses_full_uuid_store_not_short(tmp_path,monkeypatch):
    from ccx.rpc import local_rpc
    root=fixture_root(tmp_path)
    add_worker(root,A)
    add_worker(root,C,"codex")
    monkeypatch.setattr(constants,"CCX_ROOT",root)
    req={"protocol_version":2,"request_id":"direct-1","op":"worker.inspect",
         "params":{"target":f"claude:{A}"}}
    # Claude inspect now pins the original native source for background
    # tasks, which this bare fixture record lacks (covered with real sources
    # in test_worker_background_inspection). The legacy direct entry still
    # resolves through the full-UUID store, shown with a Codex owner here.
    answer=local_rpc.dispatch({**req,"params":{"target":f"codex:{C}"}})
    assert answer["ok"] and answer["result"]["worker"]==f"codex:{C}"
    assert answer["result"]["worker_view"]["native_id"]==require_ccx_active(root).record(C)["native"]["threadId"]
    for bad in ("aaaaaaaa","claude:aaaaaaaa","same name","claude-session:"+A):
        deny=local_rpc.dispatch({**req,"params":{"target":bad}})
        assert not deny["ok"] and deny["error"]["code"]=="invalid_request"
    no_op=local_rpc.dispatch({**req,"op":"turn.send",
                              "params":{"target":f"claude:{A}","text":"forbidden"}})
    assert not no_op["ok"]
    # Native V1 turn.send is now a supported verb, but still requires an
    # ORIGINAL immutable caller delivery_id; no token means no native effect.
    assert no_op["error"]["code"]=="invalid_request"
    gen=root/"identity"/"generations"/G1
    assert not (gen/"receipts").exists()
    assert not (gen/"semantic-deliveries").exists()
    assert require_ccx_active(root).record(A).get("recordRevision",0)==0
    assert require_ccx_active(root).record(A)["name"]=="display is not a key"
    assert not (root/"sessions").exists()


def test_direct_local_rpc_initializes_empty_but_refuses_unsafe(tmp_path, monkeypatch):
    from ccx.rpc import local_rpc
    from ccx.store.root import root_status
    root = tmp_path / "empty"
    root.mkdir(mode=0o700)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    # The pure capabilities response precedes any store access.
    capabilities = local_rpc.dispatch({
        "protocol_version": 2, "request_id": "req", "op": "system.info",
        "params": {},
    })
    assert capabilities["ok"] is True
    assert set(capabilities["result"]) == {"protocol_version", "ccx_version"}
    assert not list(root.iterdir())
    answer = local_rpc.dispatch({
        "protocol_version": 2, "request_id": "req", "op": "worker.inspect",
        "params": {"target": A},
    })
    assert answer["ok"] is False
    assert answer["error"]["code"] == "not_found"
    assert root_status(root).kind == "initialized"
    assert not (root / "run").exists()

    offline = fixture_root(tmp_path / "verify", phase="VERIFY_ONLY")
    monkeypatch.setattr(constants, "CCX_ROOT", offline)
    prior = sorted(str(p.relative_to(offline)) for p in offline.rglob("*"))
    assert local_rpc.dispatch({
        "protocol_version": 2, "request_id": "req", "op": "system.info",
        "params": {},
    })["result"]["protocol_version"] == 2
    assert local_rpc.dispatch({
        "protocol_version": 2, "request_id": "req", "op": "worker.inspect",
        "params": {"target": A},
    })["error"]["code"] == "unavailable"
    assert prior == sorted(str(p.relative_to(offline)) for p in offline.rglob("*"))


def test_direct_rpc_serve_initializes_empty_but_refuses_unsafe_without_fork(
    tmp_path, monkeypatch,
):
    from ccx.rpc import serve as rpc_serve
    called = []
    monkeypatch.setattr(os, "fork", lambda: called.append("fork"))
    with pytest.raises(ValueError, match="original ACTIVE generation"):
        rpc_serve._Server(0, 1, "")

    class ProbeServer:
        def __init__(self, in_fd, out_fd, generation):
            assert generation.startswith("g-")
            called.append("server")
        def run(self):
            return 0
    monkeypatch.setattr(rpc_serve, "_Server", ProbeServer)
    empty = tmp_path / "empty"
    empty.mkdir(mode=0o700)
    monkeypatch.setattr(constants, "CCX_ROOT", empty)
    assert rpc_serve.serve(in_fd=0, out_fd=1) == 0
    assert called == ["server"]
    assert not (empty / "run").exists()

    offline = fixture_root(tmp_path / "offline", phase="VERIFY_ONLY")
    monkeypatch.setattr(constants, "CCX_ROOT", offline)
    prior = sorted(str(path.relative_to(offline)) for path in offline.rglob("*"))
    assert rpc_serve.serve(in_fd=0, out_fd=1) == 3
    assert sorted(str(path.relative_to(offline)) for path in offline.rglob("*")) == prior
    assert called == ["server"]
    assert not (offline / "run").exists()


def test_direct_worker_events_cursors_no_provider(tmp_path,monkeypatch):
    root=fixture_root(tmp_path)
    add_worker(root,A)
    monkeypatch.setattr(constants,"CCX_ROOT",root)
    workers=WorkerClient()
    first=workers.events(A,tail=0)
    assert first["worker"]==f"claude:{A}"
    assert first["events"]==[]
    cursor=first["next_cursor"]
    assert cursor.startswith("ev2.")
    journal=root/"identity"/"generations"/G1/"workers"/"claude"/A/"events.ndjson"
    with journal.open("w",encoding="utf-8") as out:
        import json
        out.write(json.dumps({"epoch":"e","seq":1,"type":"state"})+"\n")
    journal.chmod(0o600)
    next_=workers.events(A,after=cursor)
    assert len(next_["events"])==1
    assert next_["events"][0]["worker_id"]==A
    streamed = list(workers.follow(A, after=cursor, timeout=.1, heartbeat=0))
    assert [item["kind"] for item in streamed] == ["state", "heartbeat"]
    assert streamed[0]["worker_id"] == A
    assert streamed[1]["cursor"] == streamed[0]["cursor"]
    assert not (root/"run").exists()



def test_direct_protocol_ops_full_UUID_owner_split_no_short_or_foreign(tmp_path,monkeypatch):
    from ccx.ops import common as protocol_ops
    root=fixture_root(tmp_path)
    add_worker(root,A,"claude")
    add_worker(root,B,"codex")
    monkeypatch.setattr(constants,"CCX_ROOT",root)
    assert protocol_ops.split_target(A)==(f"claude:{A}","claude",A)
    assert protocol_ops.split_target(f"codex:{B}")==(
        f"codex:{B}","codex",B)
    with pytest.raises(InvalidRequest):
        protocol_ops.split_target("aaaaaaaa")
    with pytest.raises(InvalidRequest):
        protocol_ops.split_target("claude:aaaaaaaa")
    with pytest.raises(InvalidRequest):
        protocol_ops.split_target(f"codex:{A}")
    with pytest.raises(SessionNotFound):
        protocol_ops.split_target(C)
    assert require_ccx_active(root).record(A)["name"]=="display is not a key"
    assert not (root/"sessions").exists()


def test_removed_queued_mode_refuses_before_receipt_or_provider(active_api_root):
    from ccx.claude import delivery as claude_delivery
    from ccx.ops import common as protocol_ops
    assert "queued" not in claude_delivery.MODES
    before = sorted(p.relative_to(active_api_root) for p in active_api_root.rglob("*"))
    with pytest.raises(InvalidRequest, match="unsupported Claude mode"):
        ops_send.send_turn(
            A, "must not join a FIFO", delivery_id="retired-queued",
            mode="queued",
        )
    assert sorted(p.relative_to(active_api_root) for p in active_api_root.rglob("*")) == before
    assert claude_delivery.read_receipt(A, "retired-queued") is None







def test_direct_worker_controller_observer_refuses_before_journal_or_provider(active_api_root,monkeypatch):
    import importlib
    controller_module = importlib.import_module('ccx.controllers.controller')
    events=[]
    monkeypatch.setattr(controller_module,"observe_worker_call",
                        lambda *a,**k:events.append("legacy-controller-observe"))
    w=WorkerClient()
    from ccx.errors import CreatePartialFailure
    # Layout-3 controllers journal full-UUID create/send (covered by the
    # controller hook tests); an older layout never gets a runtime at all.
    with pytest.raises(CreatePartialFailure) as prepared:
        w.create(engine="claude",cwd=str(active_api_root))
    assert prepared.value.phase=="native-activation"
    assert prepared.value.worker_id and prepared.value.worker.startswith("claude:")
    assert events==[]
    assert not (active_api_root/"controllers").exists()

def test_direct_python_creation_claim_is_partial_and_original_id_retained(tmp_path,monkeypatch):
    from ccx.errors import CreatePartialFailure
    from ccx.store.identity import canonical_worker_id
    root=fixture_root(tmp_path)
    monkeypatch.setattr(constants,"CCX_ROOT",root)
    from ccx.ops import creation
    from ccx.store.root import NativeEffectIndeterminate
    def synthetic_failure(*_args, **_kwargs):
        raise NativeEffectIndeterminate("injected pre-submission activation failure")
    monkeypatch.setattr(creation, "_complete_claude_create", synthetic_failure)
    monkeypatch.setattr(creation, "_complete_codex_create", synthetic_failure)
    c=CcxClient();d=CodexClient();w=WorkerClient()
    for fn,engine,wid in (
        (lambda: c.new(cwd=str(tmp_path),worker_id=A,name="display"),"claude",A),
        (lambda: d.create(cwd=str(tmp_path),worker_id=B,name="display"),"codex",B),
        (lambda: w.create(engine="claude",cwd=str(tmp_path),worker_id=C),"claude",C),
    ):
        with pytest.raises(CreatePartialFailure) as error:
            fn()
        assert error.value.phase=="native-activation"
        assert error.value.worker==f"{engine}:{wid}"
        assert error.value.worker_id==wid
        assert require_ccx_active(root).resolve(wid).worker_id==wid
        assert require_ccx_active(root).native_activation(wid)["state"]=="unprepared"
    assert not (root/"sessions").exists()
    assert not (root/"run").exists()

def test_direct_protocol_create_reserves_original_full_UUID_without_native(tmp_path,monkeypatch):
    from ccx.ops import common as protocol_ops
    from ccx.errors import CreatePartialFailure
    root=fixture_root(tmp_path)
    monkeypatch.setattr(constants,"CCX_ROOT",root)
    from ccx.ops import creation
    from ccx.store.root import NativeEffectIndeterminate
    def synthetic_failure(*_args, **_kwargs):
        raise NativeEffectIndeterminate("injected pre-submission activation failure")
    monkeypatch.setattr(creation, "_complete_claude_create", synthetic_failure)
    with pytest.raises(CreatePartialFailure) as captured:
        ops_create.create_worker(engine="claude",cwd=str(tmp_path),
                                   worker_id=A,name="same display")
    err=captured.value
    assert err.phase=="native-activation"
    assert err.worker_id==A and err.worker==f"claude:{A}"
    ctx=require_ccx_active(root)
    assert ctx.resolve(A).worker_id==A
    assert ctx.native_activation(A)["state"]=="unprepared"
    assert ctx.record(A)["name"]=="same display"
    with pytest.raises(CreatePartialFailure) as retry:
        ops_create.create_worker(engine="claude",cwd=str(tmp_path),
                                   worker_id=A,name="different")
    assert retry.value.info["reconciled"] is True
    assert ctx.record(A)["name"]=="same display"
    assert not (root/"sessions").exists() and not (root/"sock").exists()


# WorkerClient.summaries: a real public refusal removed only for recorded-only
# exact full-UUID enrichment. This NEVER implies live status or transcript parity.
def test_worker_summaries_full_uuid_same_prefix_canonical_dedup_and_no_effects(
    active_api_root, monkeypatch,
):
    from ccx import client as client_module
    from ccx.codex import runtime as codex_runtime
    from ccx.claude import transcript as transcript_module
    from ccx import workers as worker_module
    store = require_ccx_active(active_api_root)
    store.mutate_record(B, patches={
        "createArgs.model": "requested-not-observed", "resolvedModel": "proven-codex",
        "lastTurnAt": 42, "context": {
            "state": "known", "tokens_used": 18400,
            "window_size": 258400, "percent_used": 7.12,
            "as_of_turn_ref": None, "as_of_ms": 42,
        },
    })
    client = WorkerClient()
    renamed = client.set_name(f"claude:{A}", "Renamed only this owner")
    assert renamed["worker_id"] == A
    sibling = store.record(B)
    before = _worker_summaries_file_snapshot(active_api_root)

    def forbidden(*_args, **_kwargs):
        pytest.fail("recorded-only summaries/list/status must not read provider/host/derived data")

    monkeypatch.setattr(codex_runtime, "observe_registry", forbidden)
    monkeypatch.setattr(codex_runtime, "observe_worker", forbidden)
    monkeypatch.setattr(transcript_module, "render_transcript", forbidden)
    monkeypatch.setattr(transcript_module, "refresh_context_observation", forbidden)
    monkeypatch.setattr(client_module, "render_transcript", forbidden)
    # The deleted transcript cache must not be consulted by any read.
    assert not hasattr(cli_read, "_context_snapshot_for_record")

    result = client.summaries([
        {"worker": f"codex:{B}"}, A, B,
        f"claude:{A}", {"worker": f"codex:{B}"},
    ])
    assert result["count"] == 2 and result["errors"] == {}
    codex, claude = result["workers"]
    assert (codex["worker"], codex["worker_id"], codex["engine"], codex["short"]) == (
        f"codex:{B}", B, "codex", "aaaaaaaa",
    )
    assert (claude["worker"], claude["worker_id"], claude["engine"], claude["short"]) == (
        f"claude:{A}", A, "claude", "aaaaaaaa",
    )
    assert codex["observed_model"] == "proven-codex"
    assert codex["observed_model"] != sibling["createArgs"]["model"]
    assert codex["context"]["state"] == "known"
    assert codex["context"]["window_size"] == 258400
    assert codex["context"]["percent_used"] == 7.12
    assert codex["context"]["as_of_turn_ref"] is None
    assert claude["observed_model"] is None and claude["context"]["state"] == "not_started"
    assert all(row["error"] is None for row in result["workers"])
    status = client.status(A)
    assert status["worker_id"] == A
    assert status["context"] == {
        "tokens_used": None, "window_size": None, "percent_used": None,
        "reason": "no-signal", "as_of_ms": None, "as_of_turn_ref": None,
    }
    assert {x["worker_id"] for x in client.list()["workers"]} == {A, B}
    assert store.record(A)["name"] == "Renamed only this owner"
    assert store.record(B) == sibling  # including original native thread identity
    assert _worker_summaries_file_snapshot(active_api_root) == before
    assert not (active_api_root / "run").exists()
    assert not (active_api_root / "sessions").exists()


def _worker_summaries_file_snapshot(root):
    """Pure test observation: stable content/mtime and directory membership."""
    return sorted(
        (str(path.relative_to(root)), path.stat().st_mtime_ns,
         path.read_bytes() if path.is_file() else None)
        for path in root.rglob("*")
    )


def test_worker_summaries_missing_valid_original_isolated_without_aliasing(active_api_root):
    client = WorkerClient()
    result = client.summaries([B, f"claude:{C}", f"claude:{C}", A, f"codex:{B}"])
    assert result["count"] == 3
    assert [r["worker"] for r in result["workers"]] == [
        f"codex:{B}", f"claude:{C}", f"claude:{A}",
    ]
    missing = result["workers"][1]
    assert missing["worker_id"] == C and missing["engine"] == "claude"
    assert missing["observed_model"] is None
    assert missing["error"] == "SessionNotFound"
    assert missing["context"]["state"] == "unavailable"
    assert result["errors"][f"claude:{C}"]["error"] == "SessionNotFound"
    assert result["errors"][f"claude:{C}"]["exit_code"] == SessionNotFound.exit_code
    assert result["workers"][0]["error"] is None
    assert result["workers"][2]["error"] is None
    bare_missing = client.summaries([C])["workers"][0]
    assert bare_missing["worker"] == C and bare_missing["worker_id"] == C
    assert bare_missing["engine"] is None


@pytest.mark.parametrize("invalid", [
    "aaaaaaaa", "claude:aaaaaaaa", "display is not a key",
    "native-thread-aaaaaaaa", "claude:" + B, "codex:" + A,
    "gpt-5.6-sol:" + A, {"worker": "claude:aaaaaaaa"},
    {"worker": None}, 42,
])
def test_worker_summaries_invalid_or_wrong_engine_targets_refuse(active_api_root, invalid):
    from ccx.errors import InvalidRequest
    client = WorkerClient()
    with pytest.raises(InvalidRequest):
        client.summaries([A, invalid])
    assert client._uuid_store.record(A)["name"] == "display is not a key"
    assert not (active_api_root / "run").exists()


@pytest.mark.parametrize("invalid_collection", [
    A, {"worker": A}, b"not-an-iterable-of-handles", None,
])
def test_worker_summaries_scalar_collections_fail_typed(active_api_root, invalid_collection):
    from ccx.errors import InvalidRequest
    with pytest.raises(InvalidRequest, match="iterable"):
        WorkerClient().summaries(invalid_collection)


def test_worker_summaries_terminal_unbound_codex_record_is_forensic_only(tmp_path, monkeypatch):
    from ccx.codex import runtime as codex_runtime
    root = fixture_root(tmp_path)
    add_worker(root, A, "claude")
    add_worker(root, B, "codex", native_state="unbound-unknown", terminal=True)
    store = require_ccx_active(root)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    client = WorkerClient()
    before = _worker_summaries_file_snapshot(root)

    def forbidden(*_args, **_kwargs):
        pytest.fail("an unbound terminal-only worker must never reach provider/daemon")

    monkeypatch.setattr(codex_runtime, "observe_registry", forbidden)
    monkeypatch.setattr(codex_runtime, "observe_worker", forbidden)
    monkeypatch.setattr(codex_runtime, "ensure_daemon", forbidden)
    rows = client.summaries([B, A])["workers"]
    assert [r["worker_id"] for r in rows] == [B, A]
    assert rows[0]["observed_model"] is None
    assert rows[0]["context"]["state"] == "not_started"
    assert rows[0]["error"] is None
    state = client.status(B)
    assert state["terminal_only"] is True
    assert state["native_state"] is None
    assert state["native_id"] is None
    assert store.record(B)["native"].get("threadId") is None
    assert _worker_summaries_file_snapshot(root) == before
    assert not (root / "run").exists()


def test_worker_summaries_does_not_import_external_or_native_only_worker(tmp_path, monkeypatch):
    from test_generation_store import put, privdir
    root = fixture_root(tmp_path)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    external = root / "external" / "claude"
    privdir(external)
    put(external / f"{A}.json", {
        "workerId": A, "sessionId": "native-outside-CCX", "managed": False,
        "origin": "external",
    })
    client = WorkerClient()
    before = _worker_summaries_file_snapshot(root)
    result = client.summaries([f"claude:{A}", f"codex:{B}"])
    assert result["count"] == 2
    assert all(row["error"] == "SessionNotFound" for row in result["workers"])
    assert client.list()["workers"] == []
    assert not (root / "identity" / "generations" / G1 / "owners").exists()
    assert _worker_summaries_file_snapshot(root) == before


def test_worker_summaries_stale_active_pointer_fails_closed(active_api_root):
    from ccx.store.root import StaleGeneration
    client = WorkerClient()
    switch_pointer(active_api_root)
    with pytest.raises(StaleGeneration):
        client.summaries([A])
    with pytest.raises(StaleGeneration):
        client.summaries([])


def test_worker_summaries_corrupt_global_owner_does_not_hide_as_missing(
    active_api_root,
):
    import json
    from test_generation_store import put
    root = active_api_root
    client = WorkerClient()
    path = root / "identity" / "generations" / G1 / "owners" / f"{A}.json"
    owner = json.loads(path.read_text())
    owner["generation"] = "g-" + "e" * 32
    put(path, owner)
    with pytest.raises(SessionUnavailable, match="Worker record unavailable"):
        client.summaries([B, A])
    # Even with a good first row, contradictory owner integrity is NOT turned
    # into a per-row missing error or a partially successful public result.


def test_worker_summaries_distinct_same_engine_full_uuids_with_identical_display_prefix(
    active_api_root,
):
    another = "aaaaaaaa-abcd-4000-8000-000000000099"
    add_worker(active_api_root, another, "claude")
    store = require_ccx_active(active_api_root)
    client = WorkerClient()
    before = _worker_summaries_file_snapshot(active_api_root)
    result = client.summaries([A, another, f"claude:{A}", f"claude:{another}"])
    assert result["count"] == 2 and result["errors"] == {}
    assert [r["worker_id"] for r in result["workers"]] == [A, another]
    assert all(r["engine"] == "claude" and r["short"] == "aaaaaaaa"
               for r in result["workers"])
    assert store.record(A)["workerId"] != store.record(another)["workerId"]
    assert _worker_summaries_file_snapshot(active_api_root) == before


def test_worker_context_real_v2_observation_same_prefix_and_no_read_effects(
    tmp_path, monkeypatch,
):
    import json
    from test_transcript import _render_owned_source
    from test_generation_store import privdir
    from ccx import client as client_module
    from ccx.store import paths
    from ccx.claude import transcript as transcript_module
    from ccx import workers as worker_module
    from ccx.ops.creation_steps import prepare_worker

    source = tmp_path / "real-a.jsonl"
    source.write_text(json.dumps({
        "type": "assistant", "message": {
            "model": "native-claude-A", "content": [{"type": "text", "text": "A"}],
            "usage": {"input_tokens": 1234, "output_tokens": 10},
        },
    }) + "\n")
    owner, _dst, rec_a = _render_owned_source(tmp_path, monkeypatch, source)
    store = require_ccx_active(constants.CCX_ROOT)
    second = prepare_worker(store, engine="claude", cwd=str(tmp_path), worker_id=B)
    rec_b = store.record(second.worker)
    path_b = paths.transcript_path_for(rec_b["cwd"], rec_b["native"]["sessionId"])
    privdir(path_b.parent)
    path_b.write_text(json.dumps({
        "type": "assistant", "message": {
            "model": "native-claude-B", "content": [{"type": "text", "text": "B"}],
            "usage": {"input_tokens": 5678, "output_tokens": 5},
        },
    }) + "\n")
    path_b.chmod(0o600)
    transcript_module.refresh_context_observation(A)
    transcript_module.refresh_context_observation(B)
    assert A[:8] == B[:8] and rec_a["native"]["sessionId"] != rec_b["native"]["sessionId"]
    before = _worker_summaries_file_snapshot(constants.CCX_ROOT)

    def forbidden(*_a, **_k):
        pytest.fail("observation must never render or read provider telemetry")

    monkeypatch.setattr(transcript_module, "render_transcript", forbidden)

    monkeypatch.setattr(transcript_module, "refresh_context_observation", forbidden)
    monkeypatch.setattr(client_module, "render_transcript", forbidden)
    assert not hasattr(cli_read, "_context_snapshot_for_record")
    facade = WorkerClient()
    summaries = facade.summaries([A, f"claude:{B}", f"claude:{A}"])
    assert summaries["count"] == 2 and summaries["errors"] == {}
    first, other = summaries["workers"]
    assert (first["worker_id"], first["observed_model"], first["context"]["tokens_used"]) == (
        A, "native-claude-A", 1234,
    )
    assert (other["worker_id"], other["observed_model"], other["context"]["tokens_used"]) == (
        B, "native-claude-B", 5678,
    )
    for wid, tokens in ((A, 1234), (B, 5678)):
        value = facade.context({"worker": f"claude:{wid}"})
        assert value["worker_id"] == wid and value["engine"] == "claude"
        assert value["tokens_used"] == tokens and value["reason"] is None
        assert value["window_size"] is None and value["percent_used"] is None
        assert value["native_state"] == "not-started-proven"
    assert _worker_summaries_file_snapshot(constants.CCX_ROOT) == before
    assert not (constants.CCX_ROOT / "sessions").exists()
    assert not (constants.CCX_ROOT / "run").exists()


def test_worker_context_large_native_history_reads_only_small_sidecar(
    tmp_path, monkeypatch,
):
    import json
    from test_transcript import _render_owned_source
    from ccx import client as client_module
    from ccx.claude import transcript as transcript_module
    from ccx.store.root import StorageIntegrityError

    src = tmp_path / "large-history.jsonl"
    src.write_text("\n".join(json.dumps(event) for event in [
        {"type": "user", "message": {"content": "x" * (20 * 1024 * 1024 + 123)}},
        {"type": "assistant", "message": {
            "model": "native-large-history-model",
            "content": [{"type": "text", "text": "done"}],
            "usage": {"input_tokens": 90123, "output_tokens": 11},
        }},
    ]) + "\n")
    owner, obs, _rec = _render_owned_source(tmp_path, monkeypatch, src)
    transcript_module.refresh_context_observation(owner)
    assert obs.stat().st_size < 2048  # small however large the history
    before = _worker_summaries_file_snapshot(constants.CCX_ROOT)
    for renderer_module in (transcript_module, client_module):
        monkeypatch.setattr(renderer_module, "render_transcript", lambda *_a, **_k:
                            pytest.fail("native render on read is forbidden"))
    facade = WorkerClient()
    assert facade.summaries([owner])["workers"][0]["observed_model"] == "native-large-history-model"
    assert facade.summaries([owner])["workers"][0]["context"]["tokens_used"] == 90123
    assert facade.context(owner)["tokens_used"] == 90123
    assert _worker_summaries_file_snapshot(constants.CCX_ROOT) == before


def test_worker_context_missing_observation_and_codex_compaction(active_api_root):
    store = require_ccx_active(active_api_root)
    facade = WorkerClient()
    missing = facade.summaries([A])["workers"][0]
    assert missing["context"]["state"] == "not_started"
    assert missing["observed_model"] is None
    assert facade.context(A)["reason"] == "no-signal"
    store.mutate_record(B, patches={
        "lifecycle.state": "idle",
        "context": {"tokens_used": 91, "window_size": 1234,
                    "percent_used": 7.37, "as_of_turn_ref": None, "as_of_ms": 42},
        "lastTurnAt": 42,
    })
    present = facade.context(f"codex:{B}")
    assert present["tokens_used"] == 91 and present["window_size"] == 1234
    assert present["percent_used"] == 7.37 and present["as_of_turn_ref"] is None
    assert present["state"] == store.record(B)["lifecycle"]["state"]
    store.mutate_record(B, patches={"activeCompact": {"id": "compact-1"}})
    compacting = facade.context(B)
    assert compacting["reason"] == "compacting" and compacting["tokens_used"] is None


@pytest.mark.parametrize("field,wrong", [
    ("workerId", B),
    ("nativeSessionId", B),
    ("generation", "g-" + "2" * 32),
])
def test_worker_context_sidecar_foreign_identity_fails_closed(tmp_path, monkeypatch, field, wrong):
    import json
    from test_transcript import _render_owned_source
    from ccx.claude import transcript as transcript_module
    from ccx.store.root import StorageIntegrityError

    src = tmp_path / "real.jsonl"
    src.write_text(json.dumps({
        "type": "assistant", "message": {
            "model": "real-model", "usage": {"input_tokens": 11},
            "content": [{"type": "text", "text": "reply"}],
        },
    }) + "\n")
    owner, _dst, _rec = _render_owned_source(tmp_path, monkeypatch, src)
    transcript_module.refresh_context_observation(owner)
    store = require_ccx_active(constants.CCX_ROOT)
    sidecar = store.worker_path(owner, "context-observation.json")
    doc = json.loads(sidecar.read_text())
    doc[field] = wrong
    sidecar.write_text(json.dumps(doc))
    sidecar.chmod(0o600)
    client = WorkerClient()
    with pytest.raises(StorageIntegrityError, match="identity/provenance"):
        client.summaries([owner])
    with pytest.raises(StorageIntegrityError, match="identity/provenance"):
        client.context(owner)


def test_claude_input_response_optional_request_id_signatures():
    import inspect
    for name in ("answer", "approve", "reject", "respond_elicitation"):
        parameter = inspect.signature(getattr(CcxClient, name)).parameters["request_id"]
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
        assert parameter.default is None
    for name in ("approve", "reject"):
        parameter = inspect.signature(getattr(WorkerClient, name)).parameters["request_id"]
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
        assert parameter.default is None


def test_claude_unbrokered_and_codex_approval_requires_loaded_native_gate(active_api_root):

    store = require_ccx_active(active_api_root)
    store.mutate_record(A, patches={"waitingFor": {
        "tool": "AskUserQuestion", "input": {"questions": [
            {"question": "Which option?"}]}}})
    with pytest.raises(SessionUnavailable, match="no brokered"):
        CcxClient().answer(A, "Yes", request_id="some-fabricated-id")
    worker = WorkerClient()
    with pytest.raises(SessionUnavailable):
        worker.approve(B, request_id="native-approvals-not-yet-migrated")
    assert worker.capabilities("codex")["answer_question"] is False
    assert worker.capabilities("codex")["respond_elicitation"] is False


def test_worker_status_projects_recorded_claude_gate_like_peek(active_api_root):
    store = require_ccx_active(active_api_root)
    worker = WorkerClient()
    assert worker.status(A)["pending_input"] is None
    schema = {"type": "object", "properties": {"title": {"type": "string"}}}
    store.mutate_record(A, patches={"waitingFor": {
        "id": "mcp-gate-7", "type": "elicitation", "server": "fake-mcp",
        "mode": "form", "message": "Choose a title", "schema": schema}})
    projected = worker.status(A)["pending_input"]
    assert projected["request_id"] == CcxClient().peek(A).waiting_for.request_id
    assert projected["kind"] == "elicitation" and projected["questions"] == []
    assert projected["elicitation"] == {"server": "fake-mcp", "mode": "form",
                                        "url": None, "schema": schema}
    # An unbrokered dialog is visible but has no actionable ID.
    store.mutate_record(A, patches={"waitingFor": {
        "tool": "AskUserQuestion", "input": {"questions": [{"question": "Q?"}]}}})
    projected = worker.status(A)["pending_input"]
    assert projected["kind"] == "question" and projected["request_id"] is None
    assert projected["questions"] == [{"question": "Q?"}]
    # Codex keeps its own recorded projection: nothing outside waiting.
    assert worker.status(B)["pending_input"] is None


def test_codex_gateid_required_signatures_transport_outcomes_and_no_provider_launch(
    active_api_root, monkeypatch,
):
    import inspect
    from ccx.codex import client as codex_module
    from ccx.errors import (DeliveryRejected, SendIndeterminate, CapabilityUnsupported)
    from ccx.codex.read import pending_input

    for name in ("approve", "reject"):
        parameter = inspect.signature(getattr(CodexClient, name)).parameters["request_id"]
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
        assert parameter.default is inspect.Parameter.empty
    assert pending_input([{"requestId": "recycled-native-id"}])["request_id"] is None
    client = CodexClient()
    real = require_ccx_active(active_api_root).record(B)
    observed = []

    def fake_rpc(worker, msg, timeout=10):
        observed.append((worker, msg))
        return replies.pop(0)

    monkeypatch.setattr(codex_module, "rpc", fake_rpc)
    replies = [
        {"ok": False, "transport": "inconclusive", "error": "timeout"},
        {"ok": False, "transport": "absent", "error": "unreachable"},
        {"ok": False, "transport": "served", "definitive": True,
         "error": "no current original Codex approval for gateId"},
        {"ok": True, "transport": "served", "workerId": B,
         "threadId": real["native"]["threadId"],
         "turnId": "native-turn-1", "gateId": "real-gate-1", "decision": "accept"},
        {"ok": True, "transport": "served", "workerId": B,
         "threadId": real["native"]["threadId"],
         "turnId": "native-turn-2", "gateId": "wrong-gate", "decision": "decline"},
    ]
    with pytest.raises(InvalidRequest):
        client.approve(B, request_id="")
    with pytest.raises(SendIndeterminate, match="real-gate-1"):
        client.approve(B, request_id="real-gate-1")
    with pytest.raises(SessionUnavailable):
        client.approve(B, request_id="real-gate-1")
    with pytest.raises(DeliveryRejected):
        client.approve(B, request_id="real-gate-1")
    accepted = client.approve(B, request_id="real-gate-1")
    assert accepted.worker_id == B and accepted.request_id == "real-gate-1"
    assert accepted.decision == "accept"
    with pytest.raises(SendIndeterminate):
        client.reject(B, request_id="real-gate-1")
    assert len(observed) == 5
    assert all(worker == B and msg == {"op": msg["op"], "gateId": "real-gate-1"}
               for worker, msg in observed)
    with pytest.raises(CapabilityUnsupported):
        WorkerClient().reject(B, request_id="real-gate-1", reason="no native reason")
    assert not replies
    assert not (active_api_root / "sessions").exists()
    assert not (active_api_root / "run").exists()


# Native Python passthrough belongs to the same current Python worker interface.
import json
import subprocess
import sys
from test_generation_store import short_root  # noqa: F401 - pytest fixture
REPO = Path(__file__).resolve().parent.parent
def _hidden_exec(root, engine, worker, marker):
    env = {key: value for key, value in os.environ.items()
           if not key.startswith("CCX_SELF_")}
    env.update(CCX_ROOT=str(root), PYTHONPATH=str(REPO))
    program = ("import pathlib, sys; pathlib.Path(sys.argv[1]).write_text('ran'); "
               "print('native', sys.argv[2])")
    return subprocess.run(
        [sys.executable, "-m", "ccx", "_live-vars-exec", "--root", str(root), "--engine", engine,
         "--short", worker, "--invoked-as", "python3", "--",
         "-c", program, str(marker), "arg"],
        env=env, cwd=str(root), stdin=subprocess.DEVNULL,
        capture_output=True, text=True, timeout=60,
    )


def test_unbound_full_uuid_worker_runs_native_python_ignoring_legacy_sidecar(
    short_root, tmp_path,
):
    add_worker(short_root, A)
    add_worker(short_root, B, "codex")
    # A root-level legacy schema-v1 sidecar must never be read or migrated.
    legacy = short_root / "variables" / "claude" / f"{A}.json"
    legacy.parent.mkdir(parents=True)
    legacy.write_text(json.dumps({
        "schemaVersion": 1, "mode": "durable", "short": A[:8],
        "names": ["x"], "variables": {"x": 1},
    }))
    before = legacy.read_bytes()

    for engine, worker in (("claude", A), ("codex", B)):
        marker = tmp_path / f"{engine}.ran"
        proc = _hidden_exec(short_root, engine, worker, marker)
        assert proc.returncode == 0, (proc.stdout, proc.stderr)
        assert proc.stdout == "native arg\n"
        assert marker.read_text() == "ran"

    assert legacy.read_bytes() == before
    assert sorted(p.name for p in legacy.parent.parent.iterdir()) == ["claude"]


def test_hidden_exec_refuses_non_exact_or_foreign_worker_without_running(
    short_root, tmp_path,
):
    add_worker(short_root, A)
    add_worker(short_root, B, "codex")
    unknown = "cccccccc-1234-4000-8000-000000000009"
    for label, engine, worker in (
        ("short", "claude", A[:8]),
        ("unknown", "claude", unknown),
        ("foreign-engine", "codex", A),
        ("qualified-foreign", "claude", f"codex:{B}"),
    ):
        marker = tmp_path / f"{label}.ran"
        proc = _hidden_exec(short_root, engine, worker, marker)
        assert proc.returncode == 69, (label, proc.stdout, proc.stderr)
        assert "CcxStateUnavailable" in proc.stderr, label
        assert proc.stdout == "" and not marker.exists(), label


def test_worker_send_has_no_delivery_mode_or_interrupt_alias():
    import inspect
    assert "delivery" not in inspect.signature(WorkerClient.send).parameters
    assert not hasattr(WorkerClient, "interrupt")


def test_creation_reply_requires_proven_report(active_api_root):
    from ccx.workers import _public_created_result
    client = WorkerClient()
    raw = {"worker": f"claude:{A}", "worker_id": A, "engine": "claude",
           "short": A[:8], "created": False, "reconciled": True}
    assert _public_created_result(client._uuid_store, raw)["reply"] is None
    pending = {**raw, "report": {"status": "unknown", "text": "unproven"}}
    assert _public_created_result(client._uuid_store, pending)["reply"] is None
    proven = {**raw, "report": {"status": "completed", "text": ""}}
    assert _public_created_result(client._uuid_store, proven)["reply"] == ""


def test_worker_input_wrappers_preserve_claude_native_arguments(active_api_root, monkeypatch):
    from ccx.errors import CapabilityUnsupported
    from ccx.types import SendResult

    client = WorkerClient()
    calls = []
    native_result = SendResult(short=A[:8], worker_id=A, engine="claude", reply="accepted")

    def original_answer(*args, **kwargs):
        calls.append(("answer", args, kwargs))
        return native_result

    def original_respond(*args, **kwargs):
        calls.append(("respond", args, kwargs))
        return native_result

    monkeypatch.setattr(client._claude, "answer", original_answer)
    monkeypatch.setattr(client._claude, "respond_elicitation", original_respond)
    answer = client.answer({"worker": f"claude:{A}"}, "yes", request_id="question-1",
                           response_id="answer-1", wait=True, timeout=12,
                           options=[(1, "yes")], fallback_to_send=False)
    response = client.respond_elicitation(A, "accept", {"name": "value"},
                                          request_id="form-2", response_id="reply-2",
                                          wait=False, timeout=7)
    assert answer == response == native_result.as_dict()
    assert calls[0] == ("answer", (f"claude:{A}", "yes"), {
        "request_id": "question-1", "response_id": "answer-1",
        "wait": True, "timeout": 12, "options": [(1, "yes")],
        "text_answers": None, "fallback_to_send": False,
    })
    assert calls[1] == ("respond", (f"claude:{A}", "accept", {"name": "value"}), {
        "request_id": "form-2", "response_id": "reply-2",
        "wait": False, "timeout": 7,
    })
    with pytest.raises(CapabilityUnsupported) as answer_unsupported:
        client.answer(B, "yes")
    assert answer_unsupported.value.capability == "answer_question"
    with pytest.raises(CapabilityUnsupported) as form_unsupported:
        client.respond_elicitation(B, "decline")
    assert form_unsupported.value.capability == "respond_elicitation"
    assert len(calls) == 2


def test_worker_telemetry_forwards_original_owner_and_cursor(active_api_root, monkeypatch):
    from ccx.ops import report as ops_report
    calls = []
    page = {"telemetry_kind": "source-bound", "turns": [], "next_cursor": "next"}

    def original(target, *, after_cursor, limit, timeout):
        calls.append((target, after_cursor, limit, timeout))
        return page

    monkeypatch.setattr(ops_report, "telemetry_worker", original)
    client = WorkerClient()
    assert client.telemetry({"worker": f"codex:{B}"}, after_cursor="old",
                            limit=7, timeout=12) is page
    assert client.telemetry(A) is page
    assert calls == [(f"codex:{B}", "old", 7, 12),
                     (f"claude:{A}", None, 200, 120)]
