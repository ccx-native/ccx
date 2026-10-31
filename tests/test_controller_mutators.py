"""Full-UUID public controller mutations over sealed owners and fake native ACKs.

No provider/daemon launch, legacy short routing, or new operation journal.
"""
from __future__ import annotations
import ccx.ops.cancel as ops_cancel
import ccx.ops.send as ops_send
import ccx.ops.wait as ops_wait

from contextlib import contextmanager
import importlib
from types import SimpleNamespace

import pytest

from ccx import WorkerClient
from ccx import constants
from ccx.controllers import store as store
from ccx import task
from ccx.codex import ops as codex_ops
from ccx.ops import common as protocol_ops
from ccx.errors import (
    CapabilityUnsupported, CompactFailed, CompactIndeterminate,
    ControllerOperationIndeterminate, DeliveryRejected, InvalidRequest,
    WorkerApiError,
)
from ccx.types import SteerResult
from ccx.store.root import NativeEffectIndeterminate
from test_generation_store import A, B, add_worker, fixture_root

controller = importlib.import_module('ccx.controllers.controller')
CLAUDE = f"claude:{A}"
CODEX = f"codex:{B}"


@pytest.fixture
def controlled(tmp_path, monkeypatch):
    root = fixture_root(tmp_path)
    add_worker(root, A, "claude")
    add_worker(root, B, "codex", confirmed=True)  # Exact v2 original Codex source/claim, same-prefix owner.
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    script = tmp_path / "flow.py"
    script.write_text("# inert controller\n")
    _, bootstrap = store.create_bootstrap(script, tmp_path, [])
    directory = store.publish_bootstrap(
        bootstrap, name="full-uuid-mutations", script=script, workspace=tmp_path,
    )
    return WorkerClient(), directory, root


@contextmanager
def _attempt(directory, *, task_id=None):
    runtime = controller._ControllerRuntime(directory)
    active = controller._active_runtime.set(runtime)
    task_token = None
    if task_id:
        task_token = controller._active_task.set(
            controller._TaskContext(task_id, task_id, directory),
        )
    try:
        yield
    finally:
        if task_token is not None:
            controller._active_task.reset(task_token)
        controller._active_runtime.reset(active)
        runtime._executor.shutdown(wait=True)


def _operation(directory, task_id, ordinal, method):
    return directory / "tasks" / task_id / "operations" / f"{ordinal:04d}-{method}.json"


def _send_fake(client, monkeypatch, calls, directory, task_id, ordinal):
    def submit(target, text, **kwargs):
        token = kwargs["delivery_id"]
        intent = store.read_json(_operation(directory, task_id, ordinal, "send"))
        assert intent["state"] == "intent"
        assert intent["metadata"]["worker"] == CLAUDE
        assert intent["metadata"]["deliveryId"] == token
        assert target == CLAUDE and text == "one follow-up"
        calls.append((target, token))
        return SimpleNamespace(
            delivery_id=token, delivery_status="accepted", reply=None,
            turn_ref=None, settled=False, state="idle",
        )
    monkeypatch.setattr(client._claude, "send", submit)


# Group 1: dynamic lookup (new mutation and SEND) consumes its slot BEFORE
# a caught failure. The follow-up original send must never land twice.
@pytest.mark.parametrize("first", ["steer", "send", "send-option", "steer-option"])
def test_pre_intent_owner_refusal_keeps_original_send_ordinal(
    controlled, monkeypatch, first,
):
    client, directory, _root = controlled
    task_id = "ordinal--0001"
    native = []
    _send_fake(client, monkeypatch, native, directory, task_id, 2)

    def rejected_lookup():
        if first == "steer":
            client.steer("aaaaaaaa", "wrong target", wait=False)
        elif first == "send":
            client.send("claude:aaaaaaaa", "wrong target", wait=False)
        elif first == "send-option":
            # Original budget refusal moved after ordinal but before intent.
            client.send(CODEX, "wrong options", caps={"opus": 60})
        else:
            client.steer(CODEX, "wrong options", caps={"opus": 60})

    def attempt():
        with _attempt(directory, task_id=task_id):
            with pytest.raises((InvalidRequest, CapabilityUnsupported)):
                rejected_lookup()
            return client.send(A, "one follow-up", wait=False)

    first_reply = attempt()
    assert first_reply["worker"] == CLAUDE
    assert attempt() == first_reply
    assert len(native) == 1
    assert not _operation(directory, task_id, 1, "steer"
                          if first in {"steer", "steer-option"} else "send").exists()
    assert store.read_json(_operation(directory, task_id, 2, "send"))["state"] == "accepted"

    # Unknown-engine creation still refuses before any ordinal.
    with _attempt(directory, task_id="static--0001"):
        with pytest.raises(InvalidRequest, match="unknown worker engine"):
            client.create(engine="nope", cwd=str(directory), variables={"a": 1})
    assert not (directory / "tasks" / "static--0001" / "operations").exists()


# Group 2: one-effect/follow-up/stop-slot behavior, using the
# real public WorkerClient and full owner resolution on both engines.
@pytest.mark.parametrize("method,engine", [
    ("steer", "claude"), ("steer", "codex"),
    ("resume", "claude"), ("resume", "codex"),
    ("approve", "claude"), ("reject", "codex"),
    ("stop", "claude"), ("stop", "codex"),
    ("kill", "claude"), ("kill", "codex"),
])
def test_public_mutation_is_one_task_slot_and_one_native_effect(
    controlled, monkeypatch, method, engine,
):
    client, directory, root = controlled
    target, wid = (CLAUDE, A) if engine == "claude" else (CODEX, B)
    calls, followups, executions = [], [], []
    assert client.capabilities(engine)["engine"] == engine
    _send_fake(client, monkeypatch, followups, directory, "apply--0001", 2)

    if method == "steer":
        if engine == "claude":
            def native(_target, text, **opts):
                calls.append((_target, text, opts["delivery_id"]))
                return SteerResult(
                    short=wid[:8], worker_id=wid, engine=engine,
                    delivery_id=opts["delivery_id"], accepted=True,
                    status="accepted-pending", pending=True, state="busy",
                )
            monkeypatch.setattr(client._claude, "steer", native)
        else:
            def native(_target, text, **opts):
                calls.append((_target, text, opts["delivery_id"]))
                return SimpleNamespace(
                    state="busy", delivery_id=opts["delivery_id"],
                    turn_id="original-turn-9", client_user_message_id="original-message-9",
                )
            monkeypatch.setattr(client._codex, "steer", native)
    elif method == "resume":
        def ready(worker, **opts):
            calls.append((worker, opts["expected_generation"]))
            return {"session_id" if engine == "claude" else "thread_id":
                    "original-native-id", "state": "idle", "resumed": False}
        monkeypatch.setattr(ops_send, "ensure_ready", ready)
    elif method in ("approve", "reject"):
        if engine == "claude":
            from ccx.store.root import require_ccx_active
            require_ccx_active(root).mutate_record(A, patches={
                "waitingFor": {"id": "permission-7", "type": "permission"},
            })
            def decide(worker, **opts):
                calls.append((worker, opts["request_id"]))
                return SimpleNamespace(reply=None)
            monkeypatch.setattr(client._claude, method, decide)
        else:
            # Fake the already-recorded original native source/gate facts;
            # WorkerClient still validates them before the concrete decision.
            original_record = client._uuid_store.record
            native_thread = original_record(B)["native"]["threadId"]
            source = f"ccx-worker:{B}"
            def original(target):
                row = original_record(target)
                if target == CODEX:
                    return {**row, "native": {**row["native"], "threadSource": source},
                            "pendingApprovals": [{"gateId": "permission-7", "turnId": "turn-7"}]}
                return row
            monkeypatch.setattr(client._uuid_store, "record", original)
            monkeypatch.setattr(client._uuid_store, "native_activation", lambda *_: {
                "state": "confirmed", "source": source,
                "nativeBinding": {"threadId": native_thread},
            })
            def decide(worker, **opts):
                calls.append((worker, opts["request_id"]))
                return SimpleNamespace(request_id=opts["request_id"])
            monkeypatch.setattr(client._codex, method, decide)
    elif method == "stop":
        def stop(worker, **opts):
            calls.append((worker, opts["expected_generation"]))
            return {"accepted": True, "confirmed": False, "outcome": None,
                    "status": "submitted", "cancellation_id": "original-cancel-7",
                    "turn_ref": f"ccx-turn:{engine}:v1:original-turn-7"}
        monkeypatch.setattr(ops_cancel, "stop_worker", stop)
    else:
        def kill(worker, **opts):
            calls.append((worker, opts["expected_generation"]))
            return {"worker": worker, "worker_id": wid, "engine": engine,
                    "short": wid[:8], "killed": True, "changed": True,
                    "history_retained": True, "resumable": True, "state": "stopped"}
        monkeypatch.setattr(ops_wait, "park_worker", kill)

    @task(name="apply")
    def unfinished():
        executions.append(True)
        if method == "steer":
            result = client.steer(target, "correction", wait=False)
        elif method in ("approve", "reject"):
            result = getattr(client, method)(target, request_id="permission-7")
        else:
            result = getattr(client, method)(target)
        client.send(CLAUDE, "one follow-up", wait=False)
        if len(executions) == 1:
            raise RuntimeError("later task failure")
        return result

    with _attempt(directory), pytest.raises(RuntimeError, match="later task failure"):
        unfinished()
    with _attempt(directory):
        result = unfinished()
    assert result["worker"] == target
    assert result["engine"] == engine and result["worker_id"] == wid
    assert len(calls) == 1 and len(followups) == 1 and len(executions) == 2
    recorded = store.read_json(_operation(directory, "apply--0001", 1, method))
    assert recorded["state"] == "accepted" and recorded["result"] == result
    assert recorded["metadata"]["worker"] == target
    if method == "steer":
        assert recorded["metadata"]["deliveryId"] == calls[0][2]
        assert result["delivery_id"] == calls[0][2]
        assert result["accepted"] and not result.get("settled", False)
    if method == "stop":
        assert result["accepted"] and not result["confirmed"] and not result["cancelled"]
    if method == "kill":
        assert result["killed"] and result["stopped"]
    if method == "resume":
        assert result["resumed"] is False
    assert not _operation(directory, "apply--0001", 2, "stop").exists()


# Group 3: claimed original identity/native receipt vs observed completion;
# uncertain writes are never redispatched, typed stopped errors stay typed.
def test_original_compact_receipt_replay_and_uncertain_native_effects(controlled, monkeypatch):
    client, directory, _root = controlled
    monkeypatch.setattr(client, "context", lambda *_: {"tokens_used": 120})
    compact_calls, statuses = [], []

    def claude_compact(worker, **opts):
        token = opts["operation_id"]
        assert worker == CLAUDE
        assert store.read_json(_operation(directory, "compact--0001", 1, "compact"))["metadata"]["operationId"] == token
        compact_calls.append(token)
        raise RuntimeError("lost compact reply after native acceptance")

    def claude_status(worker, token):
        statuses.append(token)
        # A status on some other worker/operation cannot mark this accepted.
        wrong = len(statuses) == 1
        return SimpleNamespace(
            short=A[:8], state="idle", operation_id=token + "-other" if wrong else token,
            operation_status="succeeded", compacted=True, pre_tokens=120,
            post_tokens=70, message="original boundary", evidence_uuid="boundary-7",
            reason=None,
        )

    monkeypatch.setattr(client._claude, "compact", claude_compact)
    monkeypatch.setattr(client._claude, "compact_status", claude_status)
    with _attempt(directory, task_id="compact--0001"), pytest.raises(RuntimeError, match="lost compact"):
        client.compact(CLAUDE)
    operation = _operation(directory, "compact--0001", 1, "compact")
    assert store.read_json(operation)["state"] == "indeterminate"
    with _attempt(directory, task_id="compact--0001"), pytest.raises(ControllerOperationIndeterminate, match="original worker/token"):
        client.compact(CLAUDE)
    assert store.read_json(operation)["state"] == "indeterminate"
    with _attempt(directory, task_id="compact--0001"):
        result = client.compact(CLAUDE)
    with _attempt(directory, task_id="compact--0001"):
        assert client.compact(CLAUDE) == result
    assert result["worker"] == CLAUDE and result["compacted"] is True
    assert result["operation_status"] == "succeeded"
    assert result["operation_id"] == compact_calls[0] == statuses[0] == statuses[1]
    assert len(compact_calls) == 1 and len(statuses) == 2
    assert store.read_json(operation)["state"] == "accepted"

    def failed(_worker, **opts):
        compact_calls.append(opts["operation_id"])
        raise CompactFailed(
            "native compact failed", short=A[:8], operation_id=opts["operation_id"],
            api_error=WorkerApiError("API 529", status=529, transient=True),
        )
    monkeypatch.setattr(client._claude, "compact", failed)
    for _ in range(2):
        with _attempt(directory, task_id="stopped--0001"), pytest.raises(CompactFailed) as error:
            client.compact(CLAUDE)
        assert error.value.api_error.status == 529 and error.value.api_error.transient
    assert len(compact_calls) == 2
    assert store.read_json(_operation(directory, "stopped--0001", 1, "compact"))["state"] == "stopped"

    codex_calls = []
    def uncertain(worker, **opts):
        assert worker == CODEX
        codex_calls.append(opts["operation_id"])
        raise NativeEffectIndeterminate("original codex compact receipt uncertain")
    monkeypatch.setattr(codex_ops, "compact_worker", uncertain)
    with _attempt(directory, task_id="codex--0001"), pytest.raises(CompactIndeterminate, match="Codex compact effect unknown"):
        client.compact(CODEX)
    with _attempt(directory, task_id="codex--0001"), pytest.raises(ControllerOperationIndeterminate):
        client.compact(CODEX)
    assert len(codex_calls) == 1
    stored = store.read_json(_operation(directory, "codex--0001", 1, "compact"))
    assert stored["state"] == "indeterminate" and stored["metadata"]["operationId"] is None

    # The confirmed effect cannot be credited to another original full ID.
    kill_calls = []
    def wrong_kill(worker, **_opts):
        kill_calls.append(worker)
        return {"worker": CODEX, "worker_id": B, "engine": "codex", "killed": True}
    monkeypatch.setattr(ops_wait, "park_worker", wrong_kill)
    with _attempt(directory, task_id="foreign--0001"), pytest.raises(ControllerOperationIndeterminate):
        client.kill(CLAUDE)
    with _attempt(directory, task_id="foreign--0001"), pytest.raises(ControllerOperationIndeterminate):
        client.kill(CLAUDE)
    assert kill_calls == [CLAUDE]
    assert store.read_json(_operation(directory, "foreign--0001", 1, "kill"))["state"] == "indeterminate"

    # A genuinely rejected original native attempt is safe for a new intent;
    # neither refusal is evidence of any successful steer submission.
    refusals = []
    def refuse(_worker, _text, **opts):
        refusals.append(opts["delivery_id"])
        raise DeliveryRejected("original worker not busy")
    monkeypatch.setattr(client._claude, "steer", refuse)
    for _ in range(2):
        with _attempt(directory, task_id="rejected--0001"), pytest.raises(DeliveryRejected):
            client.steer(CLAUDE, "only while busy", wait=False)
    rejected = store.read_json(_operation(directory, "rejected--0001", 1, "steer"))
    assert rejected["state"] == "rejected"
    assert rejected["metadata"]["worker"] == CLAUDE
    assert len(refusals) == 2 and refusals[0] == refusals[1] == rejected["metadata"]["deliveryId"]


@pytest.mark.parametrize("method", ["answer", "respond_elicitation"])
def test_controller_input_wrappers_record_once(controlled, monkeypatch, method):
    from ccx.types import SendResult
    client, directory, _root = controlled
    calls = []

    def native(*args, **kwargs):
        calls.append((args, kwargs))
        return SendResult(short=A[:8], worker_id=A, engine="claude", queued=True)

    monkeypatch.setattr(client._claude, method, native)

    def attempt():
        with _attempt(directory, task_id="input--0001"):
            if method == "answer":
                return client.answer(A, "yes", request_id="question", response_id="response")
            return client.respond_elicitation(A, "decline", request_id="form",
                                              response_id="response")

    first = attempt()
    assert first["worker"] == CLAUDE
    assert attempt() == first
    assert len(calls) == 1
    row = store.read_json(_operation(directory, "input--0001", 1, method))
    assert row["state"] == "accepted" and row["result"] == first
