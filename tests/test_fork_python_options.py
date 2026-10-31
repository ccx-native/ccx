"""Existing Python fork options through real fake-native child and first turn.

Only the public boundaries new to this slice: B's backend, L1b binding, F3a
native fakes, VF6 projection and the controller journal remain unmocked.
"""
from __future__ import annotations
import ccx.ops.send as ops_send
import ccx.ops.wait as ops_wait

from contextlib import nullcontext
import importlib
import json

import pytest

from ccx import CcxClient
from ccx import CodexClient
from ccx import WorkerClient
from ccx.codex import runtime as codex_runtime
from ccx import constants
from ccx.controllers import store as controller_store
from ccx.ops import forking
from ccx.controllers import live_vars
from ccx.ops import common as protocol_ops
from ccx.codex.types import CodexTurnResult
from ccx.errors import (
    ControllerLiveStateLost, CreatePartialFailure, InvalidRequest,
)
from ccx.types import SendResult
from ccx.ops.creation_steps import activate_prepared_worker, prepare_worker
from ccx.store.worker_state import normalize_native_state
from test_generation_store import A, B, C, short_root as short_root
from test_fork_host import _stop_fake
from test_fork_public import _claude_environment, _codex_environment
from test_fork_python_controller import (
    _PUBLIC_FORK_KEYS, _controller_dir, _operation,
)
from test_fork_source import sources as sources
from test_create_variable_binding import _receipt
from test_controller_mutators import _attempt


FULL_SOURCE = "dddddddd-1234-4000-8000-000000000004"
SHARED_CHILD = "eeeeeeee-1234-4000-8000-000000000005"
REFUSED_CHILD = "ffffffff-1234-4000-8000-000000000006"
SOURCE_THREAD = "native-full-permission-source"
SOURCE_TURN = "native-full-permission-last-turn"


def _full_codex_source(store, tmp_path, monkeypatch):
    """One extra settled/full-permission source, via F1/F2's original seams."""
    prepared = prepare_worker(
        store, engine="codex", cwd=str(tmp_path), worker_id=FULL_SOURCE,
        model="full-source-model", effort="high",
        sandbox="danger-full-access", approval_policy="never",
    )

    def bind_native(_budget):
        claim = store.native_activation(FULL_SOURCE)
        assert claim["state"] == "submitted-unknown"
        return {
            "accepted": True, "workerId": FULL_SOURCE, "engine": "codex",
            "source": prepared.native_source, "submissionId": claim["submissionId"],
            "nativeEvidenceSha256": "a" * 64,
            "nativeBinding": {"threadId": SOURCE_THREAD},
        }

    activate_prepared_worker(store, prepared, bounded_native_submit=bind_native)
    store.mutate_record(FULL_SOURCE, patches={
        "lifecycle.state": "idle", "stats.turns": 1,
        "lastTurn": {"turnId": SOURCE_TURN, "status": "completed"},
    })
    first_read = codex_runtime.read_thread

    def read_source(tid, *, cwd, worker_id, expected_source, timeout,
                    limit=None, newest_first=False):
        if worker_id != FULL_SOURCE:
            return first_read(
                tid, cwd=cwd, worker_id=worker_id,
                expected_source=expected_source, timeout=timeout,
                limit=limit, newest_first=newest_first,
            )
        assert (tid, cwd) == (SOURCE_THREAD, str(tmp_path))
        assert expected_source == store.codex_source(FULL_SOURCE) == f"ccx-worker:{FULL_SOURCE}"
        return {
            "id": tid, "threadSource": f"ccx-worker:{FULL_SOURCE}",
            "status": {"type": "idle"},
            "turns": [{"id": SOURCE_TURN, "status": "completed", "items": []}],
        }

    monkeypatch.setattr(codex_runtime, "read_thread", read_source)
    context = _codex_environment(store, tmp_path, monkeypatch)
    # F3a's physical fake starts with B only. Add this ONE source before the
    # daemon reads persisted native threads; don't change its implementation.
    threads_file = tmp_path / "f3a-source-native-threads.json"
    threads = json.loads(threads_file.read_text())
    threads[SOURCE_THREAD] = {
        "id": SOURCE_THREAD, "threadSource": f"ccx-worker:{FULL_SOURCE}",
        "model": "full-source-model",
        "turns": [{"id": SOURCE_TURN, "status": "completed", "items": []}],
    }
    threads_file.write_text(json.dumps(threads))
    return context


@pytest.mark.parametrize("engine", ["claude", "codex"])
def test_python_fork_waited_typed_and_neutral_original_child_replay(
    sources, tmp_path, monkeypatch, engine,
):
    store, source_transcript, source_reads = sources
    source = A if engine == "claude" else B
    prompt = f"public fork first report {engine}"
    cap = {"5h": 95.0} if engine == "claude" else None
    original_source = store.record(source)
    original_history = source_transcript.read_bytes()
    trace = _claude_environment(tmp_path, monkeypatch) if engine == "claude" else None
    native = (_codex_environment(store, tmp_path, monkeypatch)
              if engine == "codex" else nullcontext())
    try:
        with native as process:
            if engine == "codex":
                trace = process[2]
            typed_client = CcxClient() if engine == "claude" else CodexClient()
            typed = typed_client.fork(
                source, prompt, worker_id=C, wait=True, timeout=25.0,
                **({"caps": cap} if engine == "claude"
                   else {"delivery_id": "create:initial"}),
            )
            assert isinstance(typed, SendResult if engine == "claude" else CodexTurnResult)
            assert typed.worker_id == C and typed.delivery_id == "create:initial"
            assert typed.reply == f"Fake {engine.title()} completed: {prompt}"
            if engine == "claude":
                assert typed.settled is True and typed.state == "idle"
                assert typed.turn_ref
            else:
                assert typed.turn_id and typed.status == "completed"
                assert typed.thread_id
            original_report = ops_wait.wait_delivery(
                f"{engine}:{C}", delivery_id="create:initial", timeout=5,
            )["report"]
            assert original_report["text"] == typed.reply
            assert original_report["delivery_id"] == "create:initial"

            record = store.record(C)
            native_key = "sessionId" if engine == "claude" else "threadId"
            native_id = record["native"][native_key]
            assert native_id not in {source, C}
            assert record["forkAnchor"]["sourceWorkerId"] == source
            assert store.record(source) == original_source
            assert source_transcript.read_bytes() == original_history
            lineage = forking.public_lineage(record)
            before_reads = list(source_reads)
            store.mutate_record(source, patches={
                "lifecycle.state": "dead", "createArgs.model": "later source model",
            })
            with monkeypatch.context() as frozen:
                point = "_claude_point" if engine == "claude" else "_codex_point"
                frozen.setattr(
                    forking, point,
                    lambda *_a, **_k: pytest.fail("same child reread moving source"),
                )
                again = WorkerClient().fork(
                    {"worker": f"{engine}:{source}"}, worker_id=C, prompt=prompt,
                    wait=True, **({"caps": cap} if cap is not None else {}),
                )
            assert set(again) == _PUBLIC_FORK_KEYS
            assert again["worker"] == f"{engine}:{C}" and again["worker_id"] == C
            assert again["engine"] == engine and again["short"] == C[:8]
            assert (again["created"], again["reconciled"]) == (False, True)
            assert again["report"] == original_report
            assert again["reply"] == original_report["text"]
            assert again["delivery_id"] == "create:initial"
            assert again["delivery_status"] == "settled"
            assert again["notification"] is None
            assert again["forked_from"] == lineage
            assert again["native_state"] == store.record(C)["lifecycle"]["state"]
            assert again["state"] == normalize_native_state(again["native_state"])
            assert store.record(C)["native"][native_key] == native_id
            assert source_reads == before_reads

            if engine == "claude":
                assert [json.loads(row)["kind"] for row in trace.read_text().splitlines()] == [
                    "copy", "pty",
                ]
                assert (tmp_path / "f3a-pty-input.log").read_bytes().count(
                    prompt.encode()
                ) == 1
            else:
                wire = trace.read_text()
                assert wire.count("RPC thread/fork ") == 1
                assert wire.count("RPC turn/start ") == 1
                assert "RPC thread/start " not in wire
    finally:
        if engine == "claude":
            _stop_fake(constants.CCX_ROOT, C)


def test_python_fork_explicit_durable_and_shared_codex_variables(
    sources, tmp_path, monkeypatch,
):
    store, _transcript, _reads = sources
    native = _full_codex_source(store, tmp_path, monkeypatch)
    worker = WorkerClient()
    with native as (_cli, _restart, trace, _native_store, _rpc):
        prompt = "Use the first fork variable counter"
        first = worker.fork(
            FULL_SOURCE, worker_id=C, prompt=prompt, variables={"counter": 1},
        )
        assert set(first) == _PUBLIC_FORK_KEYS
        assert first["worker"] == f"codex:{C}"
        assert first["delivery_id"] == "create:initial"
        assert first["forked_from"]["worker"] == f"codex:{FULL_SOURCE}"
        assert _receipt(store, "codex", C)["names"] == ["counter"]
        sidecar = live_vars.read_sidecar("codex", C)
        assert sidecar["schemaVersion"] == 2 and sidecar["mode"] == "durable"
        state_id = worker.variables(C)["state_id"]
        assert state_id and sidecar["stateId"] == state_id
        assert worker.variables(C, "counter")["value"] == 1

        second = worker.fork(
            FULL_SOURCE, worker_id=SHARED_CHILD,
            variables_from={"worker": f"codex:{C}"},
        )
        assert set(second) == _PUBLIC_FORK_KEYS
        assert second["worker"] == f"codex:{SHARED_CHILD}"
        assert second["delivery_id"] is None and second["report"] is None
        assert second["reply"] is None
        assert worker.variables(SHARED_CHILD)["state_id"] == state_id
        assert worker.variables(SHARED_CHILD, "counter")["value"] == 1
        assert _receipt(store, "codex", SHARED_CHILD)["names"] == ["counter"]
        assert live_vars.read_sidecar("codex", SHARED_CHILD)["stateId"] == state_id

        with pytest.raises(InvalidRequest):
            worker.fork(
                FULL_SOURCE, worker_id=REFUSED_CHILD,
                variables={}, variables_from=C,
            )
        with pytest.raises(InvalidRequest):
            worker.fork(
                FULL_SOURCE, worker_id=REFUSED_CHILD, variables_from=C[:8],
            )
        assert store._owner(REFUSED_CHILD) is None
        wire = trace.read_text()
        assert wire.count("RPC thread/fork ") == 2
        assert wire.count("RPC turn/start ") == 1
        assert "RPC thread/start " not in wire
        assert wire.count("CCX variables available") == 1
        assert "'CCX_SELF_WORKER_ID': '" + C + "'" in wire
        assert store.record(C)["native"]["threadId"] not in {FULL_SOURCE, C}
        assert store.record(SHARED_CHILD)["native"]["threadId"] != (
            store.record(C)["native"]["threadId"]
        )


def test_python_controller_live_codex_fork_lost_original_first_return(
    sources, tmp_path, monkeypatch,
):
    store, _transcript, _reads = sources
    native = _full_codex_source(store, tmp_path, monkeypatch)
    directory = _controller_dir(tmp_path)
    worker = WorkerClient()
    controller = importlib.import_module('ccx.controllers.controller')
    original_send = ops_send.send_turn

    class Box:
        value = 2

    mapping = {"box": Box()}

    def lost_reply(*args, **kwargs):
        sent = original_send(*args, **kwargs)
        assert sent["status"] == "accepted"
        assert sent["delivery"]["owned"] is True
        assert sent["delivery"]["delivery_id"] == "create:initial"
        raise OSError("real native turn accepted, public first ACK lost")

    live_owner = None
    try:
        with native as (_cli, _restart, trace, _native_store, _rpc):
            with monkeypatch.context() as failed_ack:
                failed_ack.setattr(ops_send, "send_turn", lost_reply)
                with _attempt(directory, task_id="fork--0001"):
                    with pytest.raises(CreatePartialFailure) as failed:
                        worker.fork(
                            FULL_SOURCE, prompt="Use the live box",
                            variables=mapping,
                        )
                    runtime = controller._active_runtime.get()
                    live_owner = runtime.live_vars_owner()
                    original_id = failed.value.worker_id
                    assert live_owner._workers[("codex", original_id)]["box"].mapping is mapping
                    assert live_owner._workers[("codex", original_id)]["box"].mapping["box"] is mapping["box"]
            assert failed.value.phase == "initial-delivery"
            assert failed.value.worker == f"codex:{original_id}"
            assert failed.value.delivery_id == "create:initial"
            assert failed.value.info["activation_state"] == "confirmed"
            receipt = _receipt(store, "codex", original_id)
            assert receipt and receipt["names"] == ["box"]
            assert live_vars.read_sidecar("codex", original_id)["mode"] == "live"

            operation = controller_store.read_json(_operation(directory))
            assert operation["state"] == "stopped"
            assert operation["metadata"]["workerId"] == original_id
            assert operation["metadata"]["variableMode"] == "live"
            assert operation["metadata"]["liveVariables"] == ["box"]
            assert "Box" not in json.dumps(operation)
            registered = controller_store.read_json(
                controller_store.run_record_path(directory)
            )["workers"]
            assert [row["worker"] for row in registered] == [f"codex:{original_id}"]
            first_wire = trace.read_text()
            assert first_wire.count("RPC thread/fork ") == 1
            assert first_wire.count("RPC turn/start ") == 1
            assert "RPC thread/start " not in first_wire
            with _attempt(directory, task_id="fork--0001"):
                with pytest.raises(ControllerLiveStateLost):
                    worker.fork(
                        FULL_SOURCE, prompt="Use the live box",
                        variables=mapping,
                    )
            assert _receipt(store, "codex", original_id) == receipt
            assert trace.read_text().count("RPC thread/fork ") == 1
            assert trace.read_text().count("RPC turn/start ") == 1
            assert [row["worker"] for row in controller_store.read_json(
                controller_store.run_record_path(directory)
            )["workers"]] == [f"codex:{original_id}"]
    finally:
        if live_owner is not None:
            live_owner.close()
