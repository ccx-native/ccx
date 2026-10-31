"""F2: frozen full-UUID Codex fork via one original native effect, fake only."""
from __future__ import annotations
import ccx.codex.daemon.native as daemon_native

import hashlib
import json
import os
import time
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.usefixtures("executable_engine_stubs")

from ccx.codex import runtime as codex_runtime
from ccx.codex.daemon import daemon as codexd
from ccx import constants
from ccx.ops import creation
from ccx.ops import forking
from ccx.controllers import live_vars
from ccx.codex.protocol import PendingRequest
from ccx.ops.creation_steps import (
    activate_prepared_worker, prepare_worker, reconcile_submitted_native,
)
from ccx.store.root import CreationIndeterminate, NativeEffectIndeterminate, require_ccx_active
from test_generation_store import A, B, C, short_root as short_root

_SOURCE_THREAD = "provider-source-thread"
_LAST_TURN = "provider-source-turn-2"
_CHILD_THREAD = "provider-independent-child-thread"


@pytest.fixture
def frozen(short_root, tmp_path, monkeypatch):
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    monkeypatch.delenv("BASH_ENV", raising=False)
    store = require_ccx_active(short_root)
    source = prepare_worker(
        store, engine="codex", cwd=str(tmp_path), worker_id=B,
        model="source-model", effort="high", sandbox="workspace-write",
        approval_policy="on-request", idle_timeout=456,
    )

    def native_reply(_budget):
        original = store.native_activation(B)
        assert original["state"] == "submitted-unknown"
        return {
            "accepted": True, "workerId": B, "engine": "codex",
            "source": source.native_source, "submissionId": original["submissionId"],
            "nativeEvidenceSha256": "a" * 64,
            "nativeBinding": {"threadId": _SOURCE_THREAD},
        }

    activate_prepared_worker(store, source, bounded_native_submit=native_reply)
    store.mutate_record(B, patches={
        "lifecycle.state": "idle", "stats.turns": 2,
        "lastTurn": {"turnId": _LAST_TURN, "status": "completed"},
    })
    observations = []

    def native_source_read(tid, *, cwd, worker_id, expected_source, timeout,
                           limit=None, newest_first=False):
        observations.append((tid, cwd, worker_id))
        assert (tid, cwd, worker_id) == (_SOURCE_THREAD, str(tmp_path), B)
        assert expected_source == store.codex_source(B) == f"ccx-worker:{B}"
        return {
            "id": tid, "threadSource": f"ccx-worker:{B}",
            "status": {"type": "idle"},
            "turns": [
                {"id": "provider-source-turn-1", "status": "completed"},
                {"id": _LAST_TURN, "status": "completed"},
            ],
        }

    monkeypatch.setattr(codex_runtime, "read_thread", native_source_read)
    plan = forking.plan(B)  # Both None overrides are valid.
    assert plan["fork_anchor"]["lastTurnId"] == _LAST_TURN
    return store, plan, observations


def _child(store, plan):
    return prepare_worker(
        store, engine="codex", cwd=plan["cwd"], worker_id=C,
        **plan["launch"], fork_anchor=plan["fork_anchor"],
    )


def _arm(store, child):
    reservation = store.reserve_creation(
        C, engine="codex", fingerprint=child.record["createSha256"],
        native_binding={}, fork_anchor=child.record["forkAnchor"],
    )
    activation = store.prepare_native_activation(
        reservation, native_source=child.native_source,
    )
    queued = store.native_activation_dispatch(
        activation, bounded_submit=lambda _budget: {
            "ok": True, "queued": True, "workerId": C, "engine": "codex",
            "source": activation.source, "submissionId": activation.submission_id,
        },
    )
    assert queued["queued"] and store.native_activation(C)["state"] == "submitted-unknown"
    return activation.submission_id


def _marker_path(store, token):
    key = hashlib.sha256(("activate\x00" + token).encode()).hexdigest()
    return (store.pointer.generation_root / "runtime-metadata" /
            "codex-native-effects" / C / (key + ".json"))


def _marker(store, token):
    return json.loads(_marker_path(store, token).read_text())


class _FakeNative:
    """One fake original app-server JSON-RPC reply and independent thread/read."""

    def __init__(self, store, token, anchor, *, outcome="accepted", wrong=None,
                 huge_history=False):
        self.store, self.token, self.anchor = store, token, anchor
        self.outcome, self.wrong = outcome, wrong
        self.huge_history = huge_history
        self.process = SimpleNamespace(pid=os.getpid())
        self.writes = []
        self.reads = []

    def begin_request(self, method, params, *, write_deadline):
        assert time.monotonic() < write_deadline
        marker = _marker(self.store, self.token)  # Must precede SOLE native write.
        assert marker["nativeMethod"] == method == "thread/fork"
        assert marker["forkAnchor"] == self.anchor
        assert marker["source"] == f"ccx-worker:{C}"
        self.writes.append((method, dict(params)))
        pending = PendingRequest(method)
        pending.request_id = 7
        if self.outcome == "unknown":
            return pending
        if self.outcome == "coded":
            pending.put({
                "jsonrpc": "2.0", "id": 7,
                "error": {"code": -32001, "message": "fake fork source rejected"},
            })
            return pending
        turns = [
            {"id": "provider-source-turn-1", "status": "completed"},
            {"id": _LAST_TURN, "status": "completed",
             "items": [{"type": "agentMessage", "text": "x" * 80000}]
             if self.huge_history else []},
        ]
        pending.put({
            "jsonrpc": "2.0", "id": 7,
            "result": {"thread": {"id": _CHILD_THREAD, "turns": turns},
                       "model": "child-resolved-model"},
        })
        return pending

    def request(self, method, params, timeout=2):
        assert method in {"thread/read", "thread/turns/list"}
        assert not (method == "thread/read" and params.get("includeTurns"))
        self.reads.append((method, dict(params)))
        if method == "thread/turns/list":
            return {"data": [
                {"id": _LAST_TURN,
                 "status": "inProgress" if self.wrong == "point" else "completed",
                 "items": [], "itemsView": "full"}], "nextCursor": None}
        thread = {
            "id": _CHILD_THREAD, "threadSource": f"ccx-worker:{C}",
            "forkedFromId": _SOURCE_THREAD, "turns": [
                {"id": "provider-source-turn-1", "status": "completed"},
                {"id": _LAST_TURN, "status": "completed"},
            ],
        }
        if self.wrong == "parent":
            thread["forkedFromId"] = "another-source-thread"
        elif self.wrong == "point":
            thread["turns"][-1]["status"] = "inProgress"
        elif self.wrong == "tag":
            thread["threadSource"] = "ccx-worker:foreign"
        return {"thread": thread}


def _daemon(store, fake):
    daemon = codexd.CodexDaemon()
    daemon._uuid_store = store
    daemon.client = fake
    daemon.ready.set()
    return daemon


def _submit(daemon, token):
    return daemon._original_full_uuid_native({
        "op": "native-activate", "workerId": C,
        "submissionId": token,
        "nativeAdmissionDeadline": time.monotonic() + 1.5,
    })


def test_f2_frozen_claim_child_seed_override_none_and_mutable_retry(frozen, monkeypatch):
    store, plan, observations = frozen
    anchor = plan["fork_anchor"]
    assert anchor["overrides"] == {"model": None, "effort": None}
    assert observations == [(_SOURCE_THREAD, plan["cwd"], B)]
    child = _child(store, plan)
    row = store.record(C)
    assert row["forkAnchor"] == anchor
    assert row["forkedFrom"] == {
        key: anchor[key] for key in ("engine", "sourceWorkerId", "sourceNativeThreadId",
                                      "lastTurnId", "forkedAtMs")
    }
    assert row["stats"] == {"turns": 2}  # No native usage for this fork yet.
    assert row["lastTurn"] == anchor["seed"]["lastTurn"]
    assert row["native"]["threadId"] is None and row["workerId"] != B
    # A malformed point is rejected before claiming another independent owner.
    bad = dict(anchor, lastTurnId="")
    with pytest.raises(CreationIndeterminate, match="fork settings changed"):
        prepare_worker(store, engine="codex", cwd=plan["cwd"], worker_id=A,
                       **plan["launch"], fork_anchor=bad)
    # Current settings become mutable after the original submitted-unknown
    # admission; before it, first-record recovery still checks first bytes.
    _arm(store, child)
    store.mutate_record(C, patches={"name": "child edited", "createArgs.model": "child-current"})
    store.mutate_record(B, patches={"lifecycle.state": "dead",
                                    "createArgs.model": "source-later"})
    monkeypatch.setattr(codex_runtime, "read_thread", lambda *_args, **_kwargs:
                        pytest.fail("child retry reread moving source"))
    same = forking.plan(B, worker_id=C)
    assert same["existing"] and same["fork_anchor"] == anchor
    retry = _child(store, same)
    assert retry.reconciled and retry.record["createArgs"]["model"] == "child-current"
    assert store.record(C)["name"] == "child edited"
    assert len(observations) == 1


def test_f2_single_fork_wire_large_reply_independent_read_and_offline_confirmed(frozen):
    store, plan, _ = frozen
    child = _child(store, plan)
    store.mutate_record(C, patches={"createArgs.model": "child-current"})
    token = _arm(store, child)
    fake = _FakeNative(store, token, plan["fork_anchor"], huge_history=True)
    daemon = _daemon(store, fake)
    response = _submit(daemon, token)
    assert response["ok"] and response["queued"]
    assert len(fake.writes) == 1
    method, payload = fake.writes[0]
    assert method == "thread/fork" and payload["threadId"] == _SOURCE_THREAD
    assert payload["lastTurnId"] == _LAST_TURN
    assert payload["threadSource"] == f"ccx-worker:{C}"
    assert payload["model"] == "child-current"
    assert payload["cwd"] == plan["cwd"]
    assert payload["sandbox"] == "workspace-write"
    assert payload["approvalPolicy"] == "on-request"
    assert payload["config"]["shell_environment_policy"]["set"]["CCX_SELF_WORKER_ID"] == C
    assert payload["config"]["shell_environment_policy"]["set"]["CCX_SELF_ENGINE"] == "codex"
    assert payload["config"]["shell_environment_policy"]["set"]["BASH_ENV"] == str(
        live_vars.shell_env_path("codex", C))
    assert payload["config"]["shell_environment_policy"]["set"]["ZDOTDIR"] == str(
        live_vars.codex_zdotdir_path(C))
    assert (live_vars.codex_zdotdir_path(C) / ".zshenv").is_file()
    assert "serviceName" not in payload and "excludeTurns" not in payload
    saved = _marker(store, token)
    assert saved["nativeMethod"] == "thread/fork" and saved["forkAnchor"] == plan["fork_anchor"]
    assert set(saved["nativeCandidate"]) == {
        "threadId", "submittedSource", "appServerPid", "nativeReplySha256",
    }
    assert saved["nativeCandidate"]["threadId"] == _CHILD_THREAD
    assert saved["nativeCandidate"]["nativeReplySha256"]  # 80KiB turns were omitted.
    observed = daemon._observe_original_codex_thread({"workerId": C, "submissionId": token})
    assert observed["accepted"] and observed["nativeBinding"] == {"threadId": _CHILD_THREAD}
    assert fake.reads[0] == ("thread/read", {"threadId": _CHILD_THREAD, "includeTurns": False})
    assert fake.reads[1][0] == "thread/turns/list"
    confirmed = reconcile_submitted_native(store, child, original_receipt=observed)
    assert confirmed.activation_state == "confirmed"
    assert confirmed.record["native"]["threadId"] == _CHILD_THREAD
    assert "inputTokens" not in confirmed.record["stats"]
    # No runtime/callback proof survives, yet durable original ready remains.
    daemon._native_start_held.clear()
    daemon.client = None
    store.mutate_record(C, patches={"createArgs.effort": "low"})
    retried = creation.create(
        engine="codex", cwd=plan["cwd"], worker_id=C,
        **plan["launch"], _fork_anchor=plan["fork_anchor"],
    )
    assert retried["native_ready"] is True and retried["record"]["native"]["threadId"] == _CHILD_THREAD
    assert store.record(C)["createArgs"]["effort"] == "low"
    assert len(fake.writes) == 1 and len(fake.reads) == 2
    assert store.record(B)["native"]["threadId"] == _SOURCE_THREAD


@pytest.mark.parametrize("case", ["coded", "shell-env", "unknown", "parent", "point", "tag"])
def test_f2_original_rejection_unknown_and_wrong_read_no_replay(frozen, monkeypatch, case):
    store, plan, _ = frozen
    child = _child(store, plan)
    token = _arm(store, child)
    fake = _FakeNative(
        store, token, plan["fork_anchor"],
        outcome=case if case in {"coded", "unknown"} else "accepted",
        wrong=case if case in {"parent", "point", "tag"} else None,
    )
    daemon = _daemon(store, fake)
    shell_config_calls = []
    if case == "shell-env":
        def fail_shell_config(worker_id):
            shell_config_calls.append(worker_id)
            raise OSError("fake shell bridge unavailable")
        monkeypatch.setattr(daemon_native, "_original_caller_config", fail_shell_config)
    response = _submit(daemon, token)
    marker = _marker(store, token)
    assert marker["nativeMethod"] == "thread/fork"
    assert marker["forkAnchor"] == plan["fork_anchor"]
    if case == "coded":
        assert response["ok"] and store.native_activation(C)["state"] == "rejected"
        assert store.reconcile_codex_native_rejection(C)["message"] == "fake fork source rejected"
        assert len(fake.writes) == 1
    elif case == "shell-env":
        assert shell_config_calls == [C]  # Patches native.py's live lookup.
        assert response["localRefusalProven"] is True
        assert marker["nativeNotSubmitted"]["stage"] == "shell-env"
        assert store.reconcile_codex_local_refusal(C)["stage"] == "shell-env"
        assert store.native_activation(C)["state"] == "local-refused"
        assert fake.writes == []
    elif case == "unknown":
        assert response["queued"] and len(fake.writes) == 1
        pending = daemon._observe_original_codex_thread({"workerId": C, "submissionId": token})
        assert pending["status"] == "indeterminate"
    else:
        assert response["queued"] and len(fake.writes) == 1
        with pytest.raises(NativeEffectIndeterminate, match="independent Codex"):
            daemon._observe_original_codex_thread({"workerId": C, "submissionId": token})
    assert store.native_activation(C)["state"] != "confirmed"
    assert store.record(C)["native"]["threadId"] is None
    # Exact original UUID is fenced: refusal, lost result or foreign read
    # NEVER submits another thread/fork or a replacement thread/start.
    with pytest.raises((RuntimeError, NativeEffectIndeterminate)):
        _submit(daemon, token)
    assert len(fake.writes) <= 1
