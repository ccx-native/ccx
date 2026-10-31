"""L1b first-record variable receipt and create/native seam, using accepted fakes."""
from __future__ import annotations
import ccx.ops.send as ops_send

import hashlib
import json
from tempfile import TemporaryDirectory

import pytest

pytestmark = pytest.mark.usefixtures("executable_engine_stubs")

from ccx import constants
from ccx.ops import creation
from ccx.controllers import live_vars
from ccx.errors import (CapabilityUnsupported, CreatePartialFailure,
                        OperationConflict, SessionUnavailable)
from ccx.ops.creation_steps import prepare_worker
from ccx.store.root import require_ccx_active
from test_generation_store import A, B, C, short_root  # noqa: F401

D = "dddddddd-1234-4000-8000-000000000004"


@pytest.fixture
def owned(short_root, monkeypatch):
    with TemporaryDirectory(prefix="l1b-owner-", dir="/tmp") as runtime:
        monkeypatch.setattr(constants, "CCX_ROOT", short_root)
        monkeypatch.setenv("CCX_ROOT", str(short_root))
        monkeypatch.setenv("XDG_RUNTIME_DIR", runtime)
        yield require_ccx_active(short_root)


def _receipt(store, engine, worker_id):
    path = (store.pointer.generation_root / "variables" / "bindings" / engine /
            f"{worker_id}.json")
    return json.loads(path.read_text()) if path.exists() else None


def _complete_without_provider(monkeypatch, expected=None):
    """A seam spy: it observes the prepared original, never calls native IPC."""
    observed = []

    def ready(store, original, *args, **kwargs):
        record = store.record(original.worker)
        receipt = _receipt(store, original.engine, original.worker_id)
        assert record["workerId"] == original.worker_id
        assert record["createSha256"] == original.record["createSha256"]
        assert receipt and receipt["schemaVersion"] == 1
        if expected is not None:
            assert receipt["names"] == expected
            sidecar = live_vars.read_sidecar(original.engine, original.worker_id)
            assert sidecar["schemaVersion"] == 2
            assert sidecar["workerId"] == original.worker_id
        observed.append((original.worker, receipt))
        return record

    monkeypatch.setattr(creation, "_complete_claude_create", ready)
    monkeypatch.setattr(creation, "_complete_codex_create", ready)
    return observed


def _create(engine, worker_id, cwd, *, attachment=None, **options):
    return creation.create(
        engine=engine, worker_id=worker_id, cwd=str(cwd),
        attachment=attachment, **options,
    )


# 1. One shared first-record/bind/native seam, not a second native coordinator.
@pytest.mark.parametrize("engine,worker_id", [("claude", A), ("codex", B)])
def test_first_record_receipt_sidecar_before_native_and_one_initial_hint(
    owned, tmp_path, monkeypatch, engine, worker_id,
):
    observations = _complete_without_provider(monkeypatch, expected=["counter"])
    calls = []

    def sent(target, text, **kwargs):
        calls.append((target, text, kwargs))
        token = kwargs["delivery_id"]
        status = "pending" if engine == "claude" else "accepted"
        return {"status": "owned" if engine == "claude" else "accepted",
                "delivery": {"delivery_id": token, "route": "new_turn",
                             "owned": True, "status": status,
                             **({"turn_id": "fake-original-turn"} if engine == "codex" else {})}}

    from ccx.ops import common as protocol_ops
    monkeypatch.setattr(ops_send, "send_turn", sent)
    att = live_vars.prepare_durable_attachment({"counter": 1})
    result = _create(engine, worker_id, tmp_path, attachment=att,
                     prompt="Do original work")
    assert result["worker_id"] == worker_id
    assert observations[0][0] == f"{engine}:{worker_id}"
    assert _receipt(owned, engine, worker_id)["names"] == ["counter"]
    assert len(calls) == 1
    target, prompt, kwargs = calls[0]
    assert target == f"{engine}:{worker_id}"
    assert kwargs["delivery_id"] == "create:initial"
    assert prompt.startswith("Do original work\n\n[CCX variables available")
    assert prompt.count("CCX variables available") == 1
    assert "counter" in prompt
    assert owned.native_activation(worker_id)["state"] == "unprepared"  # No provider called.

    # The same validated launch spec disallows restricted worker variable state
    # before a UUID claim or sidecar can be published, for both engines.
    with pytest.raises(CapabilityUnsupported) as invalid:
        _create(engine, C, tmp_path, permissions="restricted", attachment=att)
    assert invalid.value.capability == "shared_python_variables"
    assert owned._owner(C) is None and _receipt(owned, engine, C) is None
    if engine == "codex":
        with pytest.raises(CapabilityUnsupported):
            _create(engine, D, tmp_path, sandbox="workspace-write",
                    approval_policy="on-request", attachment=att)
        assert owned._owner(D) is None


# 2. First-intent immutability; mutable SEND is never undone by creation replay.
def test_first_values_replay_mismatch_none_empty_crash_and_post_effect_refusal(
    owned, tmp_path, monkeypatch,
):
    completed = _complete_without_provider(monkeypatch)
    first = live_vars.prepare_durable_attachment({"counter": 1})
    _create("claude", A, tmp_path, attachment=first)
    first_receipt = _receipt(owned, "claude", A)
    original_state = live_vars.variables_info("claude", A)["state_id"]
    assert first_receipt["names"] == ["counter"]
    assert first_receipt["sha256"] == hashlib.sha256(json.dumps(
        {"mode": "durable", "values": {"counter": 1}},
        sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode()).hexdigest()

    # Accepted explicit later SEND publication is mutable; create is not.
    live_vars.publish_attachment(live_vars.prepare_durable_attachment({"counter": 7}),
                                 "claude", A)
    original_publish = live_vars.publish_attachment
    republished = []

    def tracked(*args):
        republished.append(args)
        return original_publish(*args)

    monkeypatch.setattr(live_vars, "publish_attachment", tracked)
    _create("claude", A, tmp_path, attachment=first, name="a mutable display name")
    assert live_vars.variables_info("claude", A, "counter")["value"] == 7
    assert live_vars.variables_info("claude", A)["state_id"] == original_state
    assert _receipt(owned, "claude", A) == first_receipt
    assert republished == []
    # Completion helpers may be re-entered to observe an original native effect.
    assert len(completed) == 2

    with pytest.raises(OperationConflict) as mismatch:
        _create("claude", A, tmp_path,
                attachment=live_vars.prepare_durable_attachment({"counter": 2}))
    assert mismatch.value.reason == "initial_variables_mismatch"
    assert mismatch.value.worker_id == A
    assert len(completed) == 2

    _create("codex", B, tmp_path)
    none = _receipt(owned, "codex", B)
    assert none["names"] == []
    with pytest.raises(OperationConflict, match="intent differs"):
        _create("codex", B, tmp_path,
                attachment=live_vars.prepare_durable_attachment({}))
    assert _receipt(owned, "codex", B) == none
    assert live_vars.read_sidecar("codex", B) is None
    _create("codex", C, tmp_path, attachment=live_vars.prepare_durable_attachment({}))
    empty = _receipt(owned, "codex", C)
    assert empty["names"] == [] and empty["sha256"] != none["sha256"]
    assert live_vars.read_sidecar("codex", C) is None
    with pytest.raises(OperationConflict):
        _create("codex", C, tmp_path)
    # Empty first binding invokes the existing (no-op) publisher once. Only the
    # next crash/retry should be measured against any *new* publication.
    republished.clear()

    # Sidecar publication succeeded, receipt fsync did not: no partial rebind.
    def crash(attachment, engine, wid):
        original_publish(attachment, engine, wid)
        raise OSError("simulated post-sidecar crash")

    monkeypatch.setattr(live_vars, "publish_attachment", crash)
    with pytest.raises(CreatePartialFailure) as partial:
        _create("claude", D, tmp_path, attachment=first)
    assert partial.value.phase == "initial-variables"
    assert partial.value.worker_id == D and partial.value.engine == "claude"
    assert partial.value.info["record"]["workerId"] == D
    assert partial.value.info["activation_state"] == "unprepared"
    assert isinstance(partial.value.cause, OSError)
    assert live_vars.read_sidecar("claude", D) is not None
    assert _receipt(owned, "claude", D) is None
    monkeypatch.setattr(live_vars, "publish_attachment", tracked)
    with pytest.raises(CreatePartialFailure) as receiptless:
        _create("claude", D, tmp_path, attachment=first)
    assert receiptless.value.phase == "initial-variables"
    assert isinstance(receiptless.value.cause, SessionUnavailable)
    with pytest.raises(CreatePartialFailure) as receiptless_none:
        _create("claude", D, tmp_path)
    assert receiptless_none.value.phase == "initial-variables"
    assert isinstance(receiptless_none.value.cause, SessionUnavailable)
    assert republished == []

    # N2: after a submitted original native intent, a genuine first attachment
    # refuses; a missing-sidecar *none* intent may still be recorded as metadata.
    E = "eeeeeeee-1234-4000-8000-000000000005"
    # Match create_promptless's normalized defaults, not the backend-only
    # prepare_worker convenience default for idle_timeout.
    prepared = prepare_worker(owned, engine="codex", worker_id=E, cwd=str(tmp_path),
                              idle_timeout=constants.DEFAULT_IDLE_TIMEOUT_S)
    claim = owned.reserve_creation(E, engine="codex",
                                   fingerprint=prepared.record["createSha256"],
                                   native_binding={})
    activation = owned.prepare_native_activation(claim,
                                                  native_source=prepared.native_source)
    owned.native_activation_dispatch(activation, bounded_submit=lambda _budget: {"ok": False},
                                     dispatch_budget_s=.5)
    assert owned.native_activation(E)["state"] == "submitted-unknown"
    with pytest.raises(CreatePartialFailure) as late:
        _create("codex", E, tmp_path, attachment=first)
    assert late.value.phase == "initial-variables"
    assert late.value.info["activation_state"] == "submitted-unknown"
    assert live_vars.read_sidecar("codex", E) is None and _receipt(owned, "codex", E) is None
    _create("codex", E, tmp_path)
    assert _receipt(owned, "codex", E)["sha256"] == none["sha256"]
    assert owned.native_activation(E)["state"] == "submitted-unknown"


# 3. One shared durable identity and one live registration in the original owner.
def test_initial_shared_state_and_live_owner_register_only_once(
    owned, tmp_path, monkeypatch,
):
    _complete_without_provider(monkeypatch)
    _create("claude", A, tmp_path, attachment=live_vars.prepare_durable_attachment(
        {"shared": [1, 2]}))
    original_state = live_vars.variables_info("claude", A)["state_id"]
    shared = live_vars.prepare_shared_durable_attachment("claude", A)
    _create("codex", B, tmp_path, attachment=shared)
    assert live_vars.variables_info("codex", B)["state_id"] == original_state
    assert live_vars.variables_info("codex", B, "shared")["value"] == [1, 2]
    assert live_vars.variables_info("claude", A)["state_id"] == original_state
    assert _receipt(owned, "codex", B)["names"] == ["shared"]

    mapping = {"box": object()}
    live_owner = live_vars.LiveOwner()
    try:
        counts = []
        original_register = live_owner.register_worker

        def register(*args):
            counts.append(args)
            return original_register(*args)

        monkeypatch.setattr(live_owner, "register_worker", register)
        attachment = live_vars.PreparedAttachment(
            mode="live", names=["box"], mapping=mapping, owner=live_owner,
        )
        _create("claude", C, tmp_path, attachment=attachment)
        assert len(counts) == 1 and counts[0][1] == C
        assert live_owner._workers[("claude", C)]["box"].mapping is mapping
        assert live_vars.read_sidecar("claude", C)["owner"]["token"] == live_owner.token
        saved = _receipt(owned, "claude", C)
        _create("claude", C, tmp_path, attachment=attachment)
        assert len(counts) == 1
        assert _receipt(owned, "claude", C) == saved
        assert live_owner._workers[("claude", C)]["box"].mapping is mapping
        empty_live = live_vars.PreparedAttachment(
            mode="live", names=[], mapping={}, owner=live_owner,
        )
        _create("claude", D, tmp_path, attachment=empty_live)
        assert live_vars.read_sidecar("claude", D) is None
        assert _receipt(owned, "claude", D)["sha256"] != saved["sha256"]
        with pytest.raises(OperationConflict):
            _create("claude", D, tmp_path)
    finally:
        live_owner.close()
