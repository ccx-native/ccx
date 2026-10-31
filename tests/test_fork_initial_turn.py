"""One original first turn after an original full-UUID native fork, both engines.

F1/F2/F3a accepted physical fakes and creation's existing create:initial receipt;
no additional producer, native harness, source alias or public fork facade.
"""
from __future__ import annotations
import ccx.ops.send as ops_send

from contextlib import nullcontext
import json
import time

import pytest

pytestmark = pytest.mark.usefixtures("executable_engine_stubs")

from ccx.claude import delivery as claude_delivery
from ccx.codex import delivery as codex_delivery
from ccx import constants
from ccx.ops import forking
from ccx.ops import common as protocol_ops
from ccx.errors import CreatePartialFailure, InvalidRequest, TurnIncomplete
from test_generation_store import A, B, C, short_root as short_root
from test_fork_host import _stop_fake
from test_fork_public import _claude_environment, _codex_environment
from test_fork_source import sources as sources


@pytest.mark.parametrize("engine", ["claude", "codex"])
def test_fork_and_one_original_waited_first_turn_both_engines(
    sources, tmp_path, monkeypatch, engine,
):
    store, source_transcript, source_reads = sources
    source = A if engine == "claude" else B
    prompt = f"fork first settled {engine} original turn"
    before_source = store.record(source)
    before_transcript = source_transcript.read_bytes()
    trace = _claude_environment(tmp_path, monkeypatch) if engine == "claude" else None
    native = (_codex_environment(store, tmp_path, monkeypatch)
              if engine == "codex" else nullcontext())
    try:
        with native as process:
            if engine == "codex":
                trace = process[2]
            result = forking.fork(
                f"{engine}:{source}", worker_id=C, prompt=prompt,
                wait=True, deadline=time.monotonic() + 25.0,
            )
            assert result["native_ready"] is True
            assert result["activation_state"] == "confirmed"
            assert (result["created"], result["reconciled"]) == (True, False)
            assert result["worker_id"] == C and result["worker"] == f"{engine}:{C}"
            assert result["forked_from"]["worker"] == f"{engine}:{source}"
            assert result["forked_from"]["worker_id"] == source
            assert result["record"]["forkAnchor"]["sourceWorkerId"] == source
            assert result["delivery_id"] == "create:initial"
            assert result["waited"] is True
            assert result["delivery_status"] == "settled"
            assert isinstance(result["turn_ref"], str) and result["turn_ref"]
            assert result["report"]["worker"] == f"{engine}:{C}"
            assert result["report"]["delivery_id"] == "create:initial"
            assert result["report"]["turn_ref"] == result["turn_ref"]
            assert result["report"]["text"] == f"Fake {engine.title()} completed: {prompt}"
            key = "sessionId" if engine == "claude" else "threadId"
            native_id = store.record(C)["native"][key]
            assert native_id not in {source, C}
            assert store.record(source) == before_source
            assert source_transcript.read_bytes() == before_transcript
            if engine == "claude":
                kinds = [json.loads(line)["kind"] for line in trace.read_text().splitlines()]
                assert kinds == ["copy", "pty"]
                paste = (tmp_path / "f3a-pty-input.log").read_bytes()
                assert paste.count(prompt.encode()) == 1
                receipt = claude_delivery.read_receipt(f"claude:{C}", "create:initial")
                assert receipt["settlement"]["status"] == "completed"
            else:
                wire = trace.read_text()
                assert wire.count("RPC thread/fork ") == 1
                assert wire.count("RPC turn/start ") == 1
                assert "RPC thread/start " not in wire
                receipt = codex_delivery.read_receipt(f"codex:{C}", "create:initial")
                assert receipt["settlement"]["status"] == "completed"
                assert source_reads == [("native-thread-B", B)]
    finally:
        if engine == "claude":
            _stop_fake(constants.CCX_ROOT, C)


def test_claude_fork_lost_first_return_reuses_frozen_child_and_cap(
    sources, tmp_path, monkeypatch,
):
    store, source_transcript, source_reads = sources
    trace = _claude_environment(tmp_path, monkeypatch)
    prompt = "Claude fork first turn original ACK lost"
    caps = {"5h": 95.0}
    original_send = ops_send.send_turn

    def lost_reply(*args, **kwargs):
        delivered = original_send(*args, **kwargs)
        assert delivered["status"] in {"owned", "accepted"}
        assert delivered["delivery"]["owned"] is True
        raise OSError("original first-send reply lost after real host receipt")

    try:
        with monkeypatch.context() as lost:
            lost.setattr(ops_send, "send_turn", lost_reply)
            with pytest.raises(CreatePartialFailure) as caught:
                forking.fork(A, worker_id=C, model="opus", prompt=prompt, caps=caps)
        failure = caught.value
        assert failure.worker_id == C and failure.phase == "initial-delivery"
        assert failure.delivery_id == "create:initial"
        assert failure.info["native_ready"] is True
        original = claude_delivery.read_receipt(f"claude:{C}", "create:initial")
        assert original["promptText"] == prompt
        assert original["cap"] == caps
        assert original["state"] in {"owned", "accepted"}
        before_bytes = source_transcript.read_bytes()
        child_id = store.record(C)["native"]["sessionId"]
        assert [json.loads(line)["kind"] for line in trace.read_text().splitlines()] == [
            "copy", "pty",
        ]
        store.mutate_record(A, patches={
            "lifecycle.state": "dead", "createArgs.model": "new source model",
        })
        store.mutate_record(C, patches={
            "name": "renamed actual child", "createArgs.model": "mutated child config",
        })
        with monkeypatch.context() as frozen:
            frozen.setattr(forking, "_claude_point", lambda *_a, **_k: pytest.fail(
                "matching child must not reread source"))
            replay = forking.fork(A, worker_id=C, model="opus", prompt=prompt, caps=caps)
            assert replay["worker_id"] == C
            assert (replay["created"], replay["reconciled"]) == (False, True)
            assert replay["delivery_id"] == "create:initial"
            assert replay["record"]["native"]["sessionId"] == child_id
            assert replay["record"]["name"] == "renamed actual child"
            assert replay["record"]["createArgs"]["model"] == "mutated child config"
            for changed in ({"prompt": "different original first prompt"},
                            {"caps": {"5h": 96.0}}):
                kwargs = {"prompt": prompt, "caps": caps, **changed}
                with pytest.raises(CreatePartialFailure) as conflict:
                    forking.fork(A, worker_id=C, model="opus", **kwargs)
                assert conflict.value.worker_id == C
                assert conflict.value.delivery_id == "create:initial"
                assert conflict.value.phase == "initial-delivery"
        assert source_transcript.read_bytes() == before_bytes
        assert source_reads == []
        assert [json.loads(line)["kind"] for line in trace.read_text().splitlines()] == [
            "copy", "pty",
        ]
        native_input = tmp_path / "f3a-pty-input.log"
        # Host append is asynchronous, but every original send is already ACKed.
        until = time.monotonic() + 6
        while (not native_input.exists() or prompt.encode() not in native_input.read_bytes()):
            assert time.monotonic() < until
            time.sleep(.03)
        assert native_input.read_bytes().count(prompt.encode()) == 1
    finally:
        _stop_fake(constants.CCX_ROOT, C)


def test_codex_fork_opus_cap_refuses_prechild_and_deadline_clamps_pending_wait(
    sources, tmp_path, monkeypatch,
):
    store, source_transcript, _source_reads = sources
    before_source = store.record(B)
    with pytest.raises(InvalidRequest, match="cap-opus"):
        forking.fork(B, worker_id=C, prompt="Codex invalid Opus quota",
                     caps={"opus": 40})
    assert store._owner(C) is None
    assert store.record(B) == before_source

    gate = tmp_path / "hold-native-first-turn"
    # Feed the existing physical Codex fake's completion gate through the F3a
    # forkable factory; neither the runtime nor the provider seam is mocked.
    import test_codex_native_effects as native_fake
    real_processes = native_fake._f2_fake_processes
    monkeypatch.setattr(native_fake, "_f2_fake_processes", lambda *args, **kwargs:
                        real_processes(*args, completion_gate=gate, **kwargs))
    with _codex_environment(store, tmp_path, monkeypatch) as native:
        trace = native[2]
        # First bind child/native fork on a roomy budget, then isolate the
        # caller's *observation* deadline from provider activation overhead.
        created = forking.fork(B, worker_id=C)
        assert created["native_ready"] is True
        assert trace.read_text().count("RPC thread/fork ") == 1
        assert "RPC turn/start " not in trace.read_text()
        prompt = "f1-race-fork-first-turn-held-before-completion"
        start = time.monotonic()
        try:
            with pytest.raises(CreatePartialFailure) as caught:
                forking.fork(B, worker_id=C, prompt=prompt, wait=True,
                             deadline=start + 2.0)
            failure = caught.value
            assert failure.worker_id == C and failure.phase == "wait-report"
            assert failure.delivery_id == "create:initial"
            assert failure.info["native_ready"] is True
            assert isinstance(failure.cause, TurnIncomplete)
            assert "did not finish before timeout" in str(failure.cause)
            assert not gate.exists()  # Actual native completion was withheld.
            # A new unbounded wait would follow the fake's 12-second gate,
            # rather than the original 2-second creation caller budget.
            assert time.monotonic() - start < 10.0
            original = codex_delivery.read_receipt(f"codex:{C}", "create:initial")
            assert original["state"] == "accepted" and original["turnId"]
            wire = trace.read_text()
            assert wire.count("RPC thread/fork ") == 1
            assert wire.count("RPC turn/start ") == 1
            assert "RPC thread/start " not in wire
            assert store.record(B) == before_source
            assert source_transcript.exists()
        finally:
            gate.touch()  # Release only the disposable provider's held completion.
