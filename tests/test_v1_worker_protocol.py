"""V1 worker protocol: exact UUID, typed refusals and recorded projection."""
from __future__ import annotations
import ccx.store.root as store_root

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.usefixtures("executable_engine_stubs")

from test_generation_store import (
    A, B, G1, add_worker, fixture_root, put, record, spec, short_root,
)
from ccx.rpc.local_rpc import dispatch_full_uuid
from ccx.store.root import require_ccx_active


def request(op, target=None, **extra):
    params = extra
    if target is not None:
        params = {"target": target, **extra}
    return {"protocol_version": 2, "request_id": "test-uuid-v1",
            "op": op, "params": params}


@pytest.mark.parametrize("op", ["worker.list", "request.cancel", "not.an.operation"])
def test_unknown_v2_op_refuses_without_capability_list(op):
    from ccx.rpc import local_rpc
    response = local_rpc.dispatch(request(op, f"claude:{A}"))
    assert not response["ok"]
    assert response["error"]["code"] == "invalid_request"
    assert response["error"]["details"]["operation"] == op
    assert "capabilities" not in response["error"]["details"]


def cli_rpc(root, envelope: dict, *args):
    env = {**os.environ, "CCX_ROOT": str(root),
           "PYTHONPATH": str(Path(__file__).parent.parent)}
    return subprocess.run(
        [sys.executable, "-B", "-m", "ccx", "rpc", *args],
        input=json.dumps(envelope), env=env, text=True, capture_output=True,
        timeout=20,
    )


def test_root_free_v2_info_from_real_cli(tmp_path):
    root = fixture_root(tmp_path)
    reply = cli_rpc(root, request("system.info"))
    assert reply.returncode == 0, reply.stderr
    envelope = json.loads(reply.stdout)
    assert envelope["ok"] is True
    assert envelope["protocol_version"] == 2
    from ccx.version import ccx_version
    assert envelope["result"] == {
        "protocol_version": 2, "ccx_version": ccx_version()}
    assert not (root / "sessions").exists()


def test_full_uuid_v1_inspects_both_engine_records_without_native_liveness(short_root, tmp_path):
    from ccx.ops.creation_steps import prepare_worker

    root = short_root
    ctx = require_ccx_active(root)
    prepare_worker(ctx, engine="claude", cwd=str(tmp_path), worker_id=A)
    original_sid = ctx.record(A)["native"]["sessionId"]
    assert original_sid != A
    add_worker(root, B, "codex", native_state="unbound-unknown", terminal=True)
    for target, engine in ((A, "claude"), (B, "codex")):
        full = ctx.pointer.generation_root / "workers" / engine / target / "record.json"
        original = json.loads(full.read_text())
        original.update(name="same name", lifecycle={"state": "idle"},
                        createdAt=1700000000000, stats={"turns": 5})
        put(full, original)
    from ccx.rpc.view import WORKER_KEYS
    for worker in (A, f"codex:{B}"):
        response = cli_rpc(root, request("worker.inspect", worker))
        assert response.returncode == 0, response.stderr
        answer = json.loads(response.stdout)
        assert answer["ok"] is True and answer["request_id"] == "test-uuid-v1"
        row = answer["result"]["worker_view"]
        assert answer["result"]["worker"] == row["worker"]
        assert set(row) == set(WORKER_KEYS) and len(row) == 21
        assert row["worker"] in (f"claude:{A}", f"codex:{B}")
        assert row["loaded"] is None and row["state_source"] == "recorded"
        assert row["state"] == row["native_state"] == "idle"
        assert row["native_id"] == (
            original_sid if row["engine"] == "claude" else None)
        assert row["context"]["reason"] == "no-signal"
        assert row["background_tasks"]["supported"] is (row["engine"] == "claude")
        assert "transcript_locator" in answer["result"]
    assert not (root / "run").exists()
    assert not (root / "sessions").exists()


@pytest.mark.parametrize("target,code", [
    ("aaaaaaaa", "invalid_request"),
    ("claude:aaaaaaaa", "invalid_request"),
    ("same name", "invalid_request"),
    ("claude-session:" + A, "invalid_request"),
    ("codex:" + A, "invalid_request"),
    ("claude:" + B, "invalid_request"),
    ("bbbbbbbb-1234-4000-8000-000000000003", "not_found"),
])
def test_full_uuid_v1_rejects_nonowned_short_native_aliases(tmp_path, target, code):
    root = fixture_root(tmp_path)
    add_worker(root, A)
    add_worker(root, B, "codex")
    ctx = require_ccx_active(root)
    before = ctx.record(A)
    response = cli_rpc(root, request("worker.inspect", target))
    answer = json.loads(response.stdout)
    assert answer["ok"] is False
    assert answer["error"]["code"] == code
    assert ctx.record(A) == before
    assert not (root / "sessions").exists()


@pytest.mark.parametrize("operation", [
    "request.cancel",
])
def test_full_uuid_v1_unconverted_ops_are_rejected_without_effect(tmp_path, operation):
    root = fixture_root(tmp_path)
    add_worker(root, A)
    ctx = require_ccx_active(root)
    before = ctx.record(A)
    response = cli_rpc(root, request(operation, f"claude:{A}"))
    assert response.returncode == 0
    answer = json.loads(response.stdout)
    assert answer["ok"] is False
    assert answer["error"]["code"] == "invalid_request"
    assert ctx.record(A) == before
    assert not (root / "sessions").exists()
    assert not (root / "run").exists()


def test_full_uuid_v1_event_page_resumes_by_ev2_cursor(tmp_path):
    root = fixture_root(tmp_path)
    add_worker(root, A)
    path = (root / "identity" / "generations" / G1
            / "workers" / "claude" / A / "events.ndjson")
    with path.open("w") as f:
        for i, kind in enumerate(("state", "turn_start", "turn_complete"), 1):
            f.write(json.dumps({"type": kind, "seq": i, "epoch": "e"}) + "\n")
    path.chmod(0o600)
    first = cli_rpc(root, request("worker.events", f"claude:{A}", limit=1))
    answer = json.loads(first.stdout)
    assert answer["ok"] is True
    events = answer["result"]
    assert events["has_more"] is True
    assert len(events["events"]) == 1
    next_ = cli_rpc(root, request(
        "worker.events", A, after_cursor=events["next_cursor"], limit=1,
    ))
    second = json.loads(next_.stdout)
    assert second["ok"] is True
    assert second["result"]["events"][0]["kind"] == "turn.started"
    invalid = cli_rpc(root, request(
        "worker.events", A, after_cursor="ev1.legacy", limit=1,
    ))
    assert json.loads(invalid.stdout)["error"]["code"] == "refused"
    now = cli_rpc(root, request("worker.events", A, tail=0))
    from_now = json.loads(now.stdout)["result"]["next_cursor"]
    waiting = cli_rpc(root, request(
        "worker.events", A, after_cursor=from_now, wait_ms=30,
    ))
    held = json.loads(waiting.stdout)
    assert held["ok"] is True
    assert held["result"]["events"] == []
    assert held["result"]["timed_out"] is True
    assert held["result"]["next_cursor"].startswith("ev2.")


def test_full_uuid_v1_uninitialized_root_initializes_and_reports_not_found(tmp_path):
    root = tmp_path / "unmigrated"
    root.mkdir(mode=0o700)
    result = cli_rpc(root, request("worker.inspect", f"claude:{A}"))
    assert result.returncode == 0, result.stderr
    reply = json.loads(result.stdout)
    assert reply["ok"] is False and reply["request_id"] == "test-uuid-v1"
    assert reply["error"]["code"] == "not_found"
    from ccx.store.root import require_ccx_active
    assert require_ccx_active(root).list_workers() == []
    assert not (root / "run").exists()


def test_serve_v2_info_from_original_active_generation(tmp_path):
    root = fixture_root(tmp_path)
    reply = cli_rpc(root, request("system.info"), "--serve")
    assert reply.returncode == 0, reply.stderr
    lines = reply.stdout.splitlines()
    assert len(lines) == 1
    result = json.loads(lines[0])
    assert result["ok"] and result["protocol_version"] == 2
    assert set(result["result"]) == {"protocol_version", "ccx_version"}
    assert result["result"]["protocol_version"] == 2
    assert not (root / "run").exists()


def test_full_uuid_v1_invalid_request_version_and_extra_params(tmp_path):
    root = fixture_root(tmp_path)
    req = request("system.info")
    req["protocol_version"] = 99
    unsupported = cli_rpc(root, req)
    assert json.loads(unsupported.stdout)["error"]["code"] == "invalid_request"
    bad = cli_rpc(root, request("worker.inspect", f"claude:{A}", name="short"))
    assert json.loads(bad.stdout)["error"]["code"] == "invalid_request"
    assert not (root / "sessions").exists()


def test_direct_full_uuid_v1_dispatch_requires_active_generation_context(short_root, tmp_path, monkeypatch):
    from ccx import constants
    from ccx.ops.creation_steps import prepare_worker

    root = short_root
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    ctx = require_ccx_active(root)
    prepare_worker(ctx, engine="claude", cwd=str(tmp_path), worker_id=A)
    accepted = dispatch_full_uuid(request("worker.inspect", A), ctx)
    assert accepted["ok"] and accepted["result"]["worker"] == "claude:" + A
    assert accepted["result"]["worker_view"]["native_id"] == ctx.record(A)["native"]["sessionId"]
    rejected = dispatch_full_uuid(request("worker.inspect", A), object())
    assert rejected["ok"] is False and rejected["error"]["code"] == "unavailable"
    # This partial adapter is explicitly separate from legacy
    # local_rpc.dispatch until all old mutating client methods are migrated.


def test_full_uuid_v1_refusal_receipt_normalizes_state_at_wire_boundary(short_root, monkeypatch):
    """Synthetic receipt through real ACTIVE V1 dispatch: wire shape, not native delivery."""
    from ccx.rpc import local_rpc

    add_worker(short_root, A)
    store = require_ccx_active(short_root)
    raw = {
        "worker": f"claude:{A}",
        "delivery": {"last_refusal": {"state": "crashed", "reason": "provider gone"}},
        "receipt": {"last_refusal": {"state": "waiting", "reason": "input gate"}},
    }

    def fake_reconcile(target, *, delivery_id, turn_id, timeout):
        assert target == A
        assert delivery_id == "synthetic-receipt" and turn_id is None
        assert timeout > 0
        return raw

    monkeypatch.setattr(local_rpc, "reconcile_turn", fake_reconcile)
    wire = dispatch_full_uuid(request(
        "turn.reconcile", A, delivery_id="synthetic-receipt",
    ), store)
    assert wire["ok"] is True, wire
    assert wire["result"]["delivery"]["last_refusal"] == {
        "state": "crashed", "reason": "provider gone",
    }
    assert "receipt" not in wire["result"]
    assert raw["delivery"]["last_refusal"]["state"] == "crashed"
    assert raw["receipt"]["last_refusal"] == {"state": "waiting", "reason": "input gate"}


def test_real_v1_worker_create_missing_binary_has_no_original_identity(tmp_path, monkeypatch):
    root = fixture_root(tmp_path)
    missing = tmp_path / "no-claude-installed"
    monkeypatch.setenv("CCX_CLAUDE_BIN", str(missing))
    first = cli_rpc(root, request("worker.create", engine="claude",
                                  cwd=str(tmp_path), name="same display"))
    assert first.returncode == 0, first.stderr
    result = json.loads(first.stdout)
    assert result["ok"] is False
    assert result["error"]["code"] == "unavailable"
    assert result["error"]["message"] == (
        f"Claude CLI not found (looked for {missing}); "
        "install Claude or set CCX_CLAUDE_BIN"
    )
    ctx = require_ccx_active(root)
    assert ctx.list_workers(limit=None) == []
    assert not (ctx.pointer.generation_root / "owners").exists()
    assert not (root / "sessions").exists()


@pytest.mark.parametrize("supplied", [None, "", "wrong",
    "aaaaaaaa-0000-1000-8000-000000000001"])
def test_real_v1_explicit_invalid_uuid_rejects_without_reservation(tmp_path,supplied):
    root=fixture_root(tmp_path)
    req=request("worker.create",engine="codex",cwd=str(tmp_path),
                worker_id=supplied)
    response=cli_rpc(root,req)
    assert response.returncode == 0,response.stderr
    value=json.loads(response.stdout)
    assert value["ok"] is False and value["error"]["code"] == "invalid_request"
    gen=root/"identity"/"generations"/G1
    assert not (gen/"owners").exists()
    assert not (root/"codex").exists()


def test_full_uuid_v1_native_verbs_need_real_owned_host_and_positive_receipts(tmp_path):
    root=fixture_root(tmp_path)
    add_worker(root,A,"claude")
    ctx=require_ccx_active(root)
    original=ctx.record(A)
    response=cli_rpc(root,request(
        "turn.send",A,text="must not reach external Claude",
        delivery_id="caller-known-token",
    ))
    answer=json.loads(response.stdout)
    assert answer["ok"] is False
    assert answer["error"]["code"] in ("unavailable","unavailable")
    assert answer["error"]["message"]
    # This disposable deep fixture can refuse at the encoded AF_UNIX byte
    # budget before it ever probes native state; that is also a hard no-write.
    missing=cli_rpc(root,request(
        "turn.reconcile",A,delivery_id="caller-known-token",
    ))
    observed=json.loads(missing.stdout)
    assert observed["ok"] is True
    assert observed["result"]["delivery"]["status"]=="not_found"
    report=cli_rpc(root,request("worker.report",A))
    answer=json.loads(report.stdout)
    assert answer["ok"] is False
    assert answer["error"]["code"]=="unavailable"
    assert ctx.record(A)==original
    gen=root/"identity"/"generations"/G1
    assert not (gen/"semantic-deliveries").exists()
    assert not (gen/"receipts").exists()
    assert not (root/"run").exists()
    for target in ("aaaaaaaa","claude:aaaaaaaa","invalid name"):
        refused=cli_rpc(root,request(
            "turn.send",target,text="no native short alias",
            delivery_id="caller-known-token",
        ))
        payload=json.loads(refused.stdout)
        assert payload["ok"] is False
        assert payload["error"]["code"]=="invalid_request"
    assert ctx.record(A)==original


@pytest.mark.parametrize("kind", ["nonexistent", "unmigrated"])
def test_f5_pure_v1_cli_contract_before_active_or_fs_mutation(tmp_path, kind):
    from ccx.rpc.local_rpc import _FULL_UUID_SUPPORTED_OPS
    root = tmp_path / "root-not-ready"
    if kind == "unmigrated":
        root.mkdir(mode=0o700)
    before = sorted(str(p.relative_to(root)) for p in root.rglob("*")) if root.exists() else []

    valid = request("system.info")
    invalid_request_id = dict(valid, request_id="")
    unsupported_version = dict(valid, protocol_version=99)
    missing_op = dict(valid, op="")
    malformed_params = dict(valid, params=[])
    unknown_params = request("worker.inspect", f"claude:{A}", unexpected=True)
    unsupported_op = request("request.cancel", f"claude:{A}")
    invalid_timeout = dict(request("worker.inspect", f"claude:{A}"), timeout_ms=0)
    bad_request = dict(request("worker.inspect", f"claude:{A}"), request_id="\x00bad")
    cases = (
        (valid, None),
        (invalid_request_id, "invalid_request"),
        (unsupported_version, "invalid_request"),
        (missing_op, "invalid_request"),
        (malformed_params, "invalid_request"),
        (unknown_params, "invalid_request"),
        (unsupported_op, "invalid_request"),
        (invalid_timeout, "invalid_request"),
        (bad_request, "invalid_request"),
        (request("worker.inspect", f"claude:{A}"), "not_found"),
    )
    for payload, code in cases:
        proc = cli_rpc(root, payload)
        assert proc.returncode == 0, (payload, proc.stdout, proc.stderr)
        assert len(proc.stdout.splitlines()) == 1, (payload, proc.stdout)
        value = json.loads(proc.stdout)
        assert value["protocol_version"] == 2
        assert value["ok"] is (code is None), (payload, value)
        if code is None:
            assert set(value["result"]) == {"protocol_version", "ccx_version"}
            assert value["result"]["protocol_version"] == 2
        else:
            assert value["error"]["code"] == code, (payload, value)
            assert value["error"]["message"]
            assert "capabilities" not in value["error"]["details"]
    from ccx.store.root import require_ccx_active
    assert require_ccx_active(root).list_workers() == []
    assert (root / "identity" / "current.json").is_file()
    assert not (root / "sessions").exists()
    assert not (root / "run").exists()


@pytest.mark.parametrize("kind", ["nonexistent", "unmigrated"])
def test_f5_direct_rpc_preflight_never_calls_generation_for_pure_requests(
    tmp_path, monkeypatch, kind,
):
    from ccx.rpc import local_rpc as rpc_module
    from ccx import constants
    import ccx.store.store as generation

    root = tmp_path / "unavailable"
    if kind == "unmigrated":
        root.mkdir(mode=0o700)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    generation_calls = []

    def refuse_state_lookup(*args, **kwargs):
        generation_calls.append((args, kwargs))
        raise AssertionError("pure Protocol V1 must never read an ACTIVE root")

    with monkeypatch.context() as patch:
        patch.setattr(store_root, "require_ccx_active", refuse_state_lookup)
        for payload, expected in (
            (request("system.info"), None),
            (dict(request("system.info"), request_id=""), "invalid_request"),
            (dict(request("system.info"), protocol_version=22), "invalid_request"),
            (request("request.cancel", A), "invalid_request"),
            (dict(request("worker.inspect", A), timeout_ms=-1), "invalid_request"),
            (request("worker.inspect", A, surprise="no"), "invalid_request"),
        ):
            got = rpc_module.dispatch(payload)
            assert got["ok"] is (expected is None), (payload, got)
            if expected:
                assert got["error"]["code"] == expected
            else:
                assert set(got["result"]) == {"protocol_version", "ccx_version"}
                assert got["result"]["protocol_version"] == 2
        assert generation_calls == []
    assert root.exists() is (kind == "unmigrated")
    if root.exists():
        assert not list(root.iterdir())


def test_f5_original_error_precedence_version_op_params_timeout_before_active(tmp_path, monkeypatch):
    from ccx.rpc import local_rpc as rpc_module
    from ccx import constants

    monkeypatch.setattr(constants, "CCX_ROOT", tmp_path / "missing")
    original = request("worker.inspect", A)
    assertions = (
        (dict(original, request_id="" , protocol_version=42), "invalid_request"),
        (dict(original, protocol_version=42, op="", params=[]), "invalid_request"),
        (dict(original, op="", params=[]), "invalid_request"),
        (dict(original, op="worker.wait", params=[]), "invalid_request"),
        (dict(original, op="request.cancel", timeout_ms=-9), "invalid_request"),
        (dict(original, params={"target": A, "unexpected": 3}, timeout_ms=0), "invalid_request"),
        (dict(original, timeout_ms=0), "invalid_request"),
        (original, "not_found"),
    )
    for payload, code in assertions:
        response = rpc_module.dispatch(payload)
        assert response["error"]["code"] == code, (payload, response)
    from ccx.store.root import require_ccx_active
    assert require_ccx_active(tmp_path / "missing").list_workers() == []


def test_f5_one_shot_v1_serve_initializes_root(tmp_path):
    root = tmp_path / "uninitialized"
    result = cli_rpc(root, request("system.info"), "--serve")
    assert result.returncode == 0
    assert json.loads(result.stdout)["ok"] is True
    assert result.stderr == ""
    from ccx.store.root import require_ccx_active
    assert require_ccx_active(root).list_workers() == []


# Original worker ID survives a failed post-native V1 recorded inspection.
from uuid import UUID
from ccx.rpc import local_rpc
from ccx.ops import common as protocol_ops
from test_codex_native_effects import _f2_fake_processes
def _v1(op: str, params: dict, request_id: str) -> dict:
    return local_rpc.dispatch({
        "protocol_version": 2, "request_id": request_id,
        "op": op, "params": params, "timeout_ms": 8000,
    })


@pytest.mark.parametrize("supplied", [True, False], ids=["supplied-id", "minted-id"])
def test_v1_post_native_create_inspection_failure_retains_original_uuid(
    short_root, tmp_path, monkeypatch, supplied,
):
    """Real original thread/start is confirmed before the secondary inspector fails."""
    # The V1 direct caller AND the original detached Codex daemon see the same
    # accepted disposable fake binary, not an installed Codex/provider binary.
    monkeypatch.setenv("CCX_CODEX_BIN", str(tmp_path / "fake-codex"))
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch,
        persisted_threads=tmp_path / "vf5-original-threads.json",
    ) as (_cli, _restart, trace, store, _rpc):
        params = {"engine": "codex", "cwd": str(tmp_path), "name": "VF5 projection"}
        if supplied:
            params["worker_id"] = B
        observed = []

        def failed_inspection(active, target):
            # The actual creation coordinator and raw native-readiness gate
            # already ran. We fault only the later public projection.
            assert active.generation == store.generation
            assert active.native_activation(target)["state"] == "confirmed"
            record = active.record(target)
            assert record["native"]["threadId"]
            observed.append(target)
            raise OSError("disposable post-native recorded inspection failure")

        with monkeypatch.context() as fault:
            fault.setattr(protocol_ops, "recorded_inspector", failed_inspection)
            failed = _v1("worker.create", params, "vf5-original-effect")

        assert not failed["ok"], failed
        assert failed["error"]["code"] == "unavailable"
        assert failed["error"]["details"]["reason"] == "partial_failure"
        details = failed["error"]["details"]
        wid = details["worker_id"]
        assert UUID(wid).version == 4
        assert wid == B if supplied else wid != B
        assert details["worker"] == f"codex:{wid}"
        assert details["engine"] == "codex" and details["phase"] == "inspect"
        assert details["cause_type"] == "OSError"
        assert details["created"] is True and details["reconciled"] is False
        assert observed == [f"codex:{wid}"]
        native_id = store.record(wid)["native"]["threadId"]
        assert native_id and store.native_activation(wid)["state"] == "confirmed"
        assert trace.read_text().count("RPC thread/start ") == 1

        # A caller that learned the minted ID from the error retries exactly
        # that ID; it must not start a second native thread or remint.
        repeated = _v1(
            "worker.create", {**params, "worker_id": wid}, "vf5-original-retry",
        )
        assert repeated["ok"], repeated
        public = repeated["result"]
        assert public["worker"] == f"codex:{wid}"
        assert public["created"] is False and public["reconciled"] is True
        assert public["worker_view"]["native_id"] == native_id
        assert public["worker_view"]["state_source"] == "recorded"
        assert {"record", "native_ready", "activation_state", "native_source"}.isdisjoint(public)
        inspected = _v1("worker.inspect", {"target": wid}, "vf5-following-inspection")
        assert inspected["ok"], inspected
        assert public["worker_view"] == inspected["result"]["worker_view"]
        assert store.native_activation(wid)["state"] == "confirmed"
        assert store.record(wid)["native"]["threadId"] == native_id
        assert trace.read_text().count("RPC thread/start ") == 1
