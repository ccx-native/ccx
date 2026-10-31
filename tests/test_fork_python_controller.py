"""Promptless Python/controller forks through F3a's original fake native effects."""
from __future__ import annotations

from contextlib import nullcontext
import json

import pytest

pytestmark = pytest.mark.usefixtures("executable_engine_stubs")

from ccx import CcxClient
from ccx import CodexClient
from ccx import WorkerClient
from ccx import constants
from ccx.controllers import store as controller_store
from ccx.ops import forking
from ccx.codex.types import CodexSessionInfo
from ccx.errors import (
    CapabilityUnsupported, ControllerOperationIndeterminate, CreatePartialFailure,
    InvalidRequest, OperationConflict, SessionNotFound, SessionUnavailable,
)
from ccx.types import SessionInfo
from ccx.store.root import GenerationError
from ccx.store.worker_state import normalize_native_state
from test_generation_store import A, B, C, short_root as short_root
from test_controller_mutators import _attempt
from test_fork_host import _stop_fake
from test_fork_public import _claude_environment, _codex_environment
from test_fork_source import sources as sources


_PUBLIC_FORK_KEYS = {
    "worker", "worker_id", "engine", "short", "state", "native_state",
    "report", "reply", "created", "reconciled", "delivery_id", "delivery_status",
    "notification", "forked_from",
}


def _operation(directory):
    return directory / "tasks" / "fork--0001" / "operations" / "0001-fork.json"


def _controller_dir(tmp_path):
    script = tmp_path / "fork_controller.py"
    script.write_text("# fake-only fork controller\n")
    _, bootstrap = controller_store.create_bootstrap(script, tmp_path, [])
    return controller_store.publish_bootstrap(
        bootstrap, name="original-fork", script=script, workspace=tmp_path,
    )


@pytest.mark.parametrize("engine", ["claude", "codex"])
def test_python_facades_promptless_native_and_admission(
    sources, tmp_path, monkeypatch, engine,
):
    store, _original_transcript, _source_reads = sources
    source = A if engine == "claude" else B
    model = "opus" if engine == "claude" else "child-model"
    trace = _claude_environment(tmp_path, monkeypatch) if engine == "claude" else None
    native_context = (
        _codex_environment(store, tmp_path, monkeypatch)
        if engine == "codex" else nullcontext()
    )
    try:
        with native_context as native:
            if engine == "codex":
                trace = native[2]
            single = CcxClient() if engine == "claude" else CodexClient()
            workers = WorkerClient()
            assert workers.capabilities(engine)["engine"] == engine
            assert workers.capabilities(f"{engine}:{source}")["engine"] == engine
            other = f"{'codex' if engine == 'claude' else 'claude'}:{B if engine == 'claude' else A}"
            with pytest.raises(InvalidRequest):
                single.fork(other)
            if engine == "claude":
                with pytest.raises(InvalidRequest):
                    single.fork(B)  # Bare UUID: wrong immutable source engine.
                for options in ({"prompt": ""}, {"caps": {}}, {"wait": True}):
                    with pytest.raises(InvalidRequest):
                        single.fork(source, **options)
            else:
                for options in ({"prompt": ""}, {"delivery_id": ""}, {"wait": True}):
                    with pytest.raises(InvalidRequest):
                        single.fork(source, **options)
            with pytest.raises(CapabilityUnsupported):
                workers.fork(source, variables={})  # Inherited restricted permission.
            with pytest.raises(InvalidRequest, match="no durable variables"):
                workers.fork(source, variables_from=source)  # No durable source state.
            for options in ({"caps": {}}, {"prompt": ""}, {"wait": True}):
                with pytest.raises(InvalidRequest):
                    workers.fork(source, **options)
            for bad_timeout in (True, 0, float("nan")):
                with pytest.raises(InvalidRequest):
                    single.fork(source, timeout=bad_timeout)
            with pytest.raises(SessionNotFound):
                workers.fork("dddddddd-1234-4000-8000-000000000004")
            assert store._owner(C) is None

            typed = single.fork(source, worker_id=C, model=model, timeout=25.0)
            assert isinstance(typed, SessionInfo if engine == "claude" else CodexSessionInfo)
            assert typed.worker_id == C and typed.short == C[:8]
            child = store.record(C)
            native_key = "sessionId" if engine == "claude" else "threadId"
            native_id = child["native"][native_key]
            assert native_id and native_id not in {source, C}
            assert getattr(typed, "session_id" if engine == "claude" else "thread_id") == native_id
            assert child["createArgs"]["model"] == model
            assert child["createArgs"]["effort"] == ("medium" if engine == "claude" else "high")
            assert child["stats"]["turns"] == 1
            if engine == "claude":
                assert "inputTokens" not in child["stats"] and "outputTokens" not in child["stats"]
            else:
                assert "inputTokens" not in child["stats"] and "outputTokens" not in child["stats"]
            assert store.native_activation(C)["state"] == "confirmed"

            # Same native-confirmed child and lineage, but the neutral facade
            # projects only MAIN's public shape from the latest recorded state.
            second = workers.fork({"worker": f"{engine}:{source}"}, worker_id=C, model=model)
            assert set(second) == _PUBLIC_FORK_KEYS
            assert second["worker"] == f"{engine}:{C}" and second["worker_id"] == C
            assert second["engine"] == engine and second["short"] == C[:8]
            assert (second["created"], second["reconciled"]) == (False, True)
            assert second["native_state"] == store.record(C)["lifecycle"]["state"]
            assert second["state"] == normalize_native_state(second["native_state"])
            assert all(second[key] is None for key in (
                "report", "delivery_id", "delivery_status", "notification",
            ))
            assert second["forked_from"]["worker"] == f"{engine}:{source}"
            assert second["forked_from"]["worker_id"] == source
            assert second["forked_from"]["engine"] == engine
            assert second["forked_from"]["short"] == source[:8]
            assert second["forked_from"]["session_id" if engine == "claude" else "thread_id"] == (
                store.record(source)["native"][native_key]
            )
            assert second["forked_from"] == forking.public_lineage(store.record(C))
            assert store.record(C)["native"][native_key] == native_id

            if engine == "codex":
                # An inspection failure AFTER native success is an original-ID
                # partial, not a stale raw-success return or another native fork.
                original_forks = trace.read_text().count("RPC thread/fork ")
                original_starts = trace.read_text().count("RPC thread/start ")
                with monkeypatch.context() as failed_projection:
                    def lost_record(_target):
                        raise GenerationError("fake pinned public record read failed")
                    failed_projection.setattr(workers._uuid_store, "record", lost_record)
                    with pytest.raises(CreatePartialFailure) as incomplete:
                        workers.fork(
                            {"worker": f"codex:{source}"}, worker_id=C, model=model,
                        )
                assert incomplete.value.phase == "inspect"
                assert incomplete.value.worker_id == C and incomplete.value.engine == "codex"
                assert incomplete.value.info["native_ready"] is True
                assert incomplete.value.info["record"]["native"]["threadId"] == native_id
                assert isinstance(incomplete.value.cause, GenerationError)
                assert trace.read_text().count("RPC thread/fork ") == original_forks
                assert trace.read_text().count("RPC thread/start ") == original_starts

            with pytest.raises(OperationConflict):
                opposite = CodexClient() if engine == "claude" else CcxClient()
                opposite.fork(source, worker_id=C)  # Child-first opposite engine.

            if engine == "claude":
                assert [json.loads(line)["kind"] for line in trace.read_text().splitlines()] == [
                    "copy", "pty",
                ]
            else:
                assert trace.read_text().count("RPC thread/fork ") == 1
                assert "RPC thread/start " not in trace.read_text()
    finally:
        if engine == "claude":
            _stop_fake(constants.CCX_ROOT, C)


@pytest.mark.parametrize("lost_ack", [False, True])
def test_controller_fork_accepts_or_stops_original_codex_child(
    sources, tmp_path, monkeypatch, lost_ack,
):
    store, _original_transcript, _source_reads = sources
    directory = _controller_dir(tmp_path)
    client = WorkerClient()
    with _codex_environment(
        store, tmp_path, monkeypatch, lost_fork_reply=lost_ack,
    ) as (_cli, _restart, trace, _native_store, _rpc):
        if lost_ack:
            with _attempt(directory, task_id="fork--0001"):
                with pytest.raises(CreatePartialFailure) as failed:
                    # The fake has durably forked the native child but never
                    # sends an ACK. Exercise the original partial/replay path
                    # without consuming the production 90-second budget.
                    client.fork(B, model="child-model", timeout=5)
            first_id = failed.value.worker_id
            assert failed.value.phase == "native-activation"
        else:
            with _attempt(directory, task_id="fork--0001"):
                first = client.fork({"worker": f"codex:{B}"}, model="child-model")
            first_id = first["worker_id"]
            assert set(first) == _PUBLIC_FORK_KEYS
            assert first["worker"] == f"codex:{first_id}"
            assert first["native_state"] == store.record(first_id)["lifecycle"]["state"]
            assert first["state"] == normalize_native_state(first["native_state"])
            assert store.native_activation(first_id)["state"] == "confirmed"
            assert first["forked_from"] == forking.public_lineage(store.record(first_id))
        path = _operation(directory)
        op = controller_store.read_json(path)
        assert op["id"].endswith(":fork")
        assert op["metadata"]["workerId"] == first_id
        assert op["metadata"]["source"] == (B if lost_ack else f"codex:{B}")
        assert op["metadata"]["requestedModel"] == "child-model"
        assert op["state"] == ("stopped" if lost_ack else "accepted")
        registered = controller_store.read_json(controller_store.run_record_path(directory))["workers"]
        assert len(registered) == (0 if lost_ack else 1)
        assert not registered or registered[0]["worker"] == f"codex:{first_id}"
        assert trace.read_text().count("RPC thread/fork ") == 1

        with _attempt(directory, task_id="fork--0001"):
            if lost_ack:
                with pytest.raises(CreatePartialFailure) as replay_error:
                    client.fork(B, model="child-model", timeout=5)
                assert replay_error.value.worker_id == first_id
                assert replay_error.value.phase == "native-activation"
            else:
                assert client.fork({"worker": f"codex:{B}"}, model="child-model") == first
        assert trace.read_text().count("RPC thread/fork ") == 1
        if not lost_ack:
            tampered = controller_store.read_json(path)
            tampered["result"]["engine"] = "claude"
            controller_store.write_json_atomic(path, tampered)
            with _attempt(directory, task_id="fork--0001"):
                with pytest.raises(ControllerOperationIndeterminate):
                    client.fork({"worker": f"codex:{B}"}, model="child-model")
            assert trace.read_text().count("RPC thread/fork ") == 1
            assert len(controller_store.read_json(
                controller_store.run_record_path(directory),
            )["workers"]) == 1


def test_controller_indeterminate_retries_original_child_without_source(
    sources, tmp_path, monkeypatch,
):
    store, _transcript, _reads = sources
    directory = _controller_dir(tmp_path)
    client = WorkerClient()

    class Interrupted(BaseException):
        pass

    with _codex_environment(store, tmp_path, monkeypatch) as (
        _cli, _restart, trace, _native_store, _rpc,
    ):
        original_lineage = forking.public_lineage
        with monkeypatch.context() as patch:
            def after_native(_record):
                raise Interrupted("controller lost the completed fork return")
            patch.setattr(forking, "public_lineage", after_native)
            with _attempt(directory, task_id="fork--0001"):
                with pytest.raises(Interrupted):
                    client.fork(B, model="child-model")
        path = _operation(directory)
        intent = controller_store.read_json(path)
        assert intent["state"] == "indeterminate"
        child_id = intent["metadata"]["workerId"]
        native_id = store.record(child_id)["native"]["threadId"]
        assert trace.read_text().count("RPC thread/fork ") == 1

        store.mutate_record(B, patches={
            "lifecycle.state": "dead", "createArgs.model": "source changed",
        })
        with monkeypatch.context() as patch:
            patch.setattr(
                forking, "_codex_point",
                lambda *_a, **_k: pytest.fail("child-first retry reread source"),
            )
            with _attempt(directory, task_id="fork--0001"):
                result = client.fork(B, model="child-model")
        assert set(result) == _PUBLIC_FORK_KEYS
        assert result["worker_id"] == child_id and result["worker"] == f"codex:{child_id}"
        child = store.record(child_id)
        assert child["native"]["threadId"] == native_id
        assert store.native_activation(child_id)["state"] == "confirmed"
        assert result["native_state"] == child["lifecycle"]["state"]
        assert result["state"] == normalize_native_state(result["native_state"])
        assert result["forked_from"] == original_lineage(child)
        assert all(result[key] is None for key in (
            "report", "delivery_id", "delivery_status", "notification",
        ))
        assert (result["created"], result["reconciled"]) == (False, True)
        assert controller_store.read_json(path)["state"] == "accepted"
        owners = controller_store.read_json(controller_store.run_record_path(directory))["workers"]
        assert [w["worker"] for w in owners] == [f"codex:{child_id}"]
        assert trace.read_text().count("RPC thread/fork ") == 1
        assert "RPC thread/start " not in trace.read_text()
