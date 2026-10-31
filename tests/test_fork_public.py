"""F3a: public promptless V1 fork, through the original native child once."""
from __future__ import annotations

from contextlib import nullcontext
import json
from pathlib import Path
import time

import pytest

from ccx import constants
from ccx.ops import forking
from ccx.rpc import local_rpc
from ccx.ops import common as protocol_ops
from test_generation_store import A, B, C, short_root as short_root
from test_fork_host import _fake_copy_and_pty_wrapper, _stop_fake
from test_fork_source import sources as sources
import test_codex_native_effects as native_fake


def _v1(op: str, *, target: str | None = None, worker_id: str | None = None,
        timeout_ms: int = 30000, **options) -> dict:
    params = dict(options)
    if target is not None:
        params["target"] = target
    if worker_id is not None:
        params["worker_id"] = worker_id
    return local_rpc.dispatch({
        "protocol_version": 2, "request_id": "f3a-public",
        "op": op, "params": params, "timeout_ms": timeout_ms,
    })


def _forkable_codex_binary(path: Path, trace: Path, *, factory,
                           lost_fork_reply: bool = False, **kwargs) -> Path:
    """Add only thread/fork to the accepted generated physical fake for this node."""
    binary = factory(path, trace, **kwargs)
    script = binary.read_text()
    marker = "        elif method=='thread/read':"
    assert script.count(marker) == 1
    fork = """
        elif method=='thread/fork':
            source=params.get('threadId')
            if source not in threads: raise ValueError('unknown source for native fork')
            point=params.get('lastTurnId')
            original=threads[source]
            selected=[]
            for turn in original.get('turns') or []:
                selected.append(dict(turn))
                if turn.get('id')==point:break
            if not selected or selected[-1].get('id')!=point:
                raise ValueError('fork point must be a real native turn')
            if selected[-1].get('status')!='completed':
                raise ValueError('fork point must be completed')
            counter+=1
            tid='native-fake-thread-'+str(counter)
            threads[tid]={'id':tid,'threadSource':params.get('threadSource'),
                          'model':params.get('model') or original.get('model'),
                          'forkedFromId':source,'turns':selected}
            if PERSIST:
                with open(PERSIST,'w',encoding='utf8') as f:
                    json.dump(threads,f);f.flush();os.fsync(f.fileno())
            if FORK_REPLY_LOST:
                continue  # The original native effect DID happen, ACK was lost.
            result={'thread':{'id':tid,'turns':selected},
                    'model':threads[tid]['model']}
"""
    script = script.replace(marker, fork + marker)
    assert script.count("            observed=dict(threads[tid])") == 1
    script = script.replace(
        "            observed=dict(threads[tid])",
        "            observed=dict(threads[tid])\n"
        "            if not params.get('includeTurns',False): observed.pop('turns',None)",
    )
    assert script.count("HELD_APPROVAL_TURNS={}") == 1
    script = script.replace(
        "HELD_APPROVAL_TURNS={}",
        "FORK_REPLY_LOST=" + repr(lost_fork_reply) + "\nHELD_APPROVAL_TURNS={}",
    )
    binary.write_text(script)
    return binary


def _codex_environment(store, tmp_path: Path, monkeypatch, *,
                       lost_fork_reply: bool = False):
    """One accepted native subprocess fake with only a test-local fork addition."""
    from test_codex_native_effects import _f2_fake_processes

    threads = tmp_path / "f3a-source-native-threads.json"
    threads.write_text(json.dumps({
        "native-thread-B": {
            "id": "native-thread-B", "threadSource": f"ccx-worker:{B}",
            "model": "source-model",
            "turns": [{"id": "native-turn-9", "status": "completed", "items": []}],
        },
    }))
    factory = native_fake._fake_codex_executable
    monkeypatch.setattr(
        native_fake, "_fake_codex_executable",
        lambda path, trace, **kw: _forkable_codex_binary(
            path, trace, factory=factory, lost_fork_reply=lost_fork_reply, **kw,
        ),
    )
    # The V1 direct caller and detached fake daemon must see the same binary.
    monkeypatch.setenv("CCX_CODEX_BIN", str(tmp_path / "fake-codex"))
    return _f2_fake_processes(
        constants.CCX_ROOT, tmp_path, monkeypatch, persisted_threads=threads,
    )


def _claude_environment(tmp_path: Path, monkeypatch) -> Path:
    wrapper = _fake_copy_and_pty_wrapper(tmp_path)
    trace = tmp_path / "f3a-claude-native-argv.ndjson"
    native_home = tmp_path / "native-home"
    fake_pty = Path(__file__).resolve().parent / "fake_claude.py"
    monkeypatch.setattr(constants, "CLAUDE_BIN", str(wrapper))
    for key, value in {
        "CCX_ROOT": str(constants.CCX_ROOT),
        "CCX_CLAUDE_BIN": str(wrapper),
        "CLAUDE_CONFIG_DIR": str(native_home),
        "FORK_NATIVE_ARGV_LOG": str(trace),
        "FORK_FAKE_PTY": str(fake_pty),
        "CCX_SPAWN_SCOPE": "0",
        "CCX_BOOT_TIMEOUT_S": "15", "FAKE_HOOKS_VIA_SETTINGS": "1",
        "FAKE_LOG": str(tmp_path / "f3a-pty-input.log"),
        "FAKE_HOOK_RESULT_LOG": str(tmp_path / "f3a-hook-result.log"),
        "PYTHONPATH": str(Path(__file__).resolve().parent.parent),
        "PYTHONDONTWRITEBYTECODE": "1",
    }.items():
        monkeypatch.setenv(key, value)
    return trace


@pytest.mark.parametrize("engine", ["claude", "codex"])
def test_f3a_v1_promptless_native_fork_and_frozen_inspect_lineage(
    sources, tmp_path, monkeypatch, engine,
):
    store, original_transcript, codex_source_reads = sources
    source = A if engine == "claude" else B
    trace = (_claude_environment(tmp_path, monkeypatch) if engine == "claude"
             else None)
    runtime = (_codex_environment(store, tmp_path, monkeypatch)
               if engine == "codex" else nullcontext())
    try:
        with runtime as native:
            if engine == "codex":
                trace = native[2]
            caps = _v1("system.info")
            assert caps["ok"], caps
            assert set(caps["result"]) == {"protocol_version", "ccx_version"}
            assert caps["result"]["protocol_version"] == 2
            response = _v1(
                "worker.fork", target=f"{engine}:{source}", worker_id=C,
                name="a true original fork",
                model="opus" if engine == "claude" else "child-model",
                timeout_ms=25000,
            )
            assert response["ok"], response
            result = response["result"]
            worker = result["worker_view"]
            assert result["worker"] == f"{engine}:{C}"
            assert worker["worker"] == f"{engine}:{C}"
            assert worker["engine"] == engine
            assert result["created"] is True and result["reconciled"] is False
            lineage = worker["forked_from"]
            assert lineage["worker"] == f"{engine}:{source}"
            assert lineage["native_id"]
            assert isinstance(lineage["forked_at_ms"], int)

            child = store.record(C)
            assert child["forkAnchor"]["sourceWorkerId"] == source
            assert child["stats"]["turns"] == 1
            if engine == "claude":
                assert "inputTokens" not in child["stats"] and "outputTokens" not in child["stats"]
            else:
                assert "inputTokens" not in child["stats"] and "outputTokens" not in child["stats"]
            assert store.native_activation(C)["state"] == "confirmed"
            native_id = child["native"]["sessionId" if engine == "claude" else "threadId"]
            assert native_id not in {source, C}
            assert (child.get("native") or {}).get("threadSource") != f"ccx-worker:{source}"
            observed = _v1("worker.inspect", target=C)
            assert observed["ok"], observed
            assert worker == observed["result"]["worker_view"]
            assert original_transcript.read_text().count("original") == 1

            if engine == "claude":
                actions = [json.loads(line) for line in trace.read_text().splitlines()]
                assert [row["kind"] for row in actions] == ["copy", "pty"]
            else:
                assert trace.read_text().count("RPC thread/fork ") == 1
                assert "RPC thread/start " not in trace.read_text()

            # The supplied original child must NEVER read a moving source.
            store.mutate_record(
                source, patches={"lifecycle.state": "dead",
                                 "createArgs.model": "later source"},
            )
            store.mutate_record(
                C, patches={"name": "later child", "createArgs.model": "later child"},
            )
            with monkeypatch.context() as deny:
                if engine == "claude":
                    deny.setattr(forking, "_claude_point",
                                 lambda *_a, **_k: pytest.fail("moved source reread"))
                else:
                    deny.setattr(forking, "_codex_point",
                                 lambda *_a, **_k: pytest.fail("moved source reread"))
                # An inspection failure AFTER a native-confirmed fork must
                # retain the same child UUID as a partial, not mint again.
                deny.setattr(protocol_ops, "recorded_inspector",
                             lambda *_a, **_k: (_ for _ in ()).throw(
                                 OSError("fake post-fork inspect failed")))
                partial = _v1(
                    "worker.fork", target=f"{engine}:{source}", worker_id=C,
                    model="opus" if engine == "claude" else "child-model",
                )
                assert partial["ok"] is False, partial
                assert partial["error"]["code"] == "unavailable"
                assert partial["error"]["details"]["worker_id"] == C
                assert partial["error"]["details"]["phase"] == "inspect"
            retried = _v1(
                "worker.fork", target=f"{engine}:{source}", worker_id=C,
                model="opus" if engine == "claude" else "child-model",
            )
            assert retried["ok"], retried
            assert retried["result"]["created"] is False
            assert retried["result"]["reconciled"] is True
            assert retried["result"]["worker_view"]["name"] == "later child"
            assert store.record(C)["native"]["sessionId" if engine == "claude" else "threadId"] == native_id
            if engine == "claude":
                assert [json.loads(line)["kind"] for line in trace.read_text().splitlines()] == ["copy", "pty"]
            else:
                assert trace.read_text().count("RPC thread/fork ") == 1
    finally:
        if engine == "claude":
            _stop_fake(constants.CCX_ROOT, C)


def test_f3a_v1_typed_admission_and_unknown_native_keeps_original_id(
    sources, tmp_path, monkeypatch,
):
    store, _source, codex_source_reads = sources
    for target, code in (
        ("aaaaaaaa", "invalid_request"),
        ("same name", "invalid_request"),
        (f"codex:{A}", "invalid_request"),
        (f"claude:{B}", "invalid_request"),
        ("dddddddd-1234-4000-8000-000000000004", "not_found"),
    ):
        reply = _v1("worker.fork", target=target, worker_id=C)
        assert reply["ok"] is False and reply["error"]["code"] == code, (target, reply)
        assert store._owner(C) is None
    for wrong in (C[:8], "not-a-uuid", None):
        params = {"target": B, "worker_id": wrong}
        result = local_rpc.dispatch({
            "protocol_version": 2, "request_id": "invalid-child",
            "op": "worker.fork", "params": params,
        })
        assert result["error"]["code"] == "invalid_request", result
        assert store._owner(C) is None

    # Genuine expired caller's native budget before planning cannot claim C.
    expired = local_rpc.dispatch_full_uuid(
        {"protocol_version": 2, "request_id": "expired", "op": "worker.fork",
         "params": {"target": B, "worker_id": C}},
        store, deadline=time.monotonic() - 1,
    )
    assert expired["error"]["code"] == "incomplete", expired
    assert store._owner(C) is None and codex_source_reads == []

    with _codex_environment(
        store, tmp_path, monkeypatch, lost_fork_reply=True,
    ) as (_cli, _restart, trace, _store, _rpc):
        uncertain = _v1("worker.fork", target=B, worker_id=C, timeout_ms=3000)
        assert uncertain["ok"] is False, uncertain
        assert uncertain["error"]["code"] == "unavailable"
        assert uncertain["error"]["details"]["worker_id"] == C
        assert uncertain["error"]["details"]["phase"] == "native-activation"
        assert store._owner(C) is not None
        assert trace.read_text().count("RPC thread/fork ") == 1
        assert "RPC thread/start " not in trace.read_text()

        store.mutate_record(B, patches={"lifecycle.state": "dead",
                                        "createArgs.model": "later source"})
        with monkeypatch.context() as deny:
            deny.setattr(forking, "_codex_point",
                         lambda *_a, **_k: pytest.fail("uncertain retry reread source"))
            repeated = _v1("worker.fork", target=B, worker_id=C, timeout_ms=1200)
        assert repeated["ok"] is False
        assert repeated["error"]["details"]["worker_id"] == C
        assert trace.read_text().count("RPC thread/fork ") == 1
