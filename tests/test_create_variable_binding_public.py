"""Public full-UUID first variable binding: real WorkerClient/controller, accepted fakes."""
from __future__ import annotations
import ccx.ops.send as ops_send

import json

import pytest

pytestmark = pytest.mark.usefixtures("executable_engine_stubs")

from ccx import WorkerClient
from ccx.ops import creation
from ccx.controllers import live_vars
from ccx.ops import common as protocol_ops
from ccx.controllers import store as controller_store
from ccx.errors import (
    CapabilityUnsupported, ControllerLiveStateLost, ControllerReplayMismatch,
    InvalidRequest, OperationConflict, SessionNotFound, SessionUnavailable,
)
from test_generation_store import A, B, C, short_root  # noqa: F401
from test_create_variable_binding import (
    D, _complete_without_provider, _receipt, owned,  # noqa: F401
)
from test_send_variable_attachment import _controller_attempt, _controller_dir

E = "eeeeeeee-1234-4000-8000-000000000005"


def test_public_create_durable_shared_and_explicit_empty(owned, tmp_path, monkeypatch):
    """Both public engines reach L1b before fake native effect; share one stateId."""
    observed = _complete_without_provider(monkeypatch, expected=["counter"])
    initial = []

    def send_original(target, text, **options):
        initial.append((target, text, options))
        return {
            "status": "owned",
            "delivery": {
                "delivery_id": options["delivery_id"], "route": "new_turn",
                "owned": True, "status": "pending",
            },
        }

    monkeypatch.setattr(ops_send, "send_turn", send_original)
    client = WorkerClient()
    first = client.create(
        engine="claude", cwd=str(tmp_path), worker_id=A,
        variables={"counter": 1}, prompt="Compute the result",
    )
    assert first["worker"] == "claude:" + A and first["delivery_id"] == "create:initial"
    assert len(initial) == 1
    target, text, options = initial[0]
    assert target == "claude:" + A
    assert text.startswith("Compute the result\n\n[CCX variables available")
    assert text.count("CCX variables available") == 1
    assert options["delivery_id"] == "create:initial" and options["mode"] == "new_turn_only"
    state = client.variables(A)["state_id"]
    assert client.variables(A, "counter")["value"] == 1

    second = client.create(
        engine="codex", cwd=str(tmp_path), worker_id=B,
        variables_from={"worker": "claude:" + A},
    )
    assert second["worker"] == "codex:" + B
    assert client.variables(B)["state_id"] == state
    assert client.variables(B, "counter")["value"] == 1
    assert [item[0] for item in observed] == ["claude:" + A, "codex:" + B]

    _complete_without_provider(monkeypatch)
    client.create(engine="claude", cwd=str(tmp_path), worker_id=C, variables={})
    client.create(engine="codex", cwd=str(tmp_path), worker_id=D)
    empty = _receipt(owned, "claude", C)
    absent = _receipt(owned, "codex", D)
    assert empty["names"] == absent["names"] == []
    assert empty["sha256"] != absent["sha256"]
    assert live_vars.read_sidecar("claude", C) is None
    assert live_vars.read_sidecar("codex", D) is None
    assert len(initial) == 1


def test_public_create_invalid_options_refuse_before_claim(owned, tmp_path, monkeypatch):
    """No attachment mutation, claim or native effect for bad public inputs."""
    observed = _complete_without_provider(monkeypatch)
    client = WorkerClient()
    # An explicit empty mapping has an initial receipt but no shareable state.
    client.create(engine="claude", cwd=str(tmp_path), worker_id=C, variables={})
    assert _receipt(owned, "claude", C) is not None
    assert client.variables(C)["mode"] is None
    assert len(observed) == 1

    cases = (
        (InvalidRequest, {"variables": {}, "variables_from": "claude:" + C}),
        (InvalidRequest, {"variables": {"value": float("nan")}}),
        (InvalidRequest, {"variables": {"not a name": 1}}),
        (InvalidRequest, {"variables": {"value": object()}}),
        (InvalidRequest, {"variables": ["not", "a mapping"]}),
        (InvalidRequest, {"variables_from": "claude:" + A[:8]}),
        (SessionNotFound, {"variables_from": "claude:" + E}),
        (InvalidRequest, {"variables_from": "claude:" + C}),
        (CapabilityUnsupported, {"variables": {"value": 1},
                                 "permissions": "restricted"}),
    )
    for error, options in cases:
        with pytest.raises(error) as caught:
            client.create(engine="claude", cwd=str(tmp_path), worker_id=A, **options)
        if error is CapabilityUnsupported:
            assert caught.value.capability == "shared_python_variables"
            assert "full permissions" in str(caught.value)
        assert owned._owner(A) is None and owned._claim(A) is None
        assert _receipt(owned, "claude", A) is None
    with pytest.raises(InvalidRequest, match="unknown worker engine"):
        client.create(engine="wrong", cwd=str(tmp_path), worker_id=A,
                      variables={"value": 1})
    assert owned._owner(A) is None and owned._claim(A) is None
    assert len(observed) == 1  # Only the legitimate empty source reached the native seam.


def test_controller_observed_live_and_post_receipt_source_conflict(
    owned, tmp_path, monkeypatch,
):
    """Real observer owns UUID and journal; it cannot resurrect live state or retarget."""
    observed = _complete_without_provider(monkeypatch)
    client = WorkerClient()
    directory = _controller_dir(tmp_path)
    live_path = directory / "tasks" / "live--0001" / "operations" / "0001-create.json"

    class Box:
        def __init__(self):
            self.value = 2

    mapping = {"box": Box()}
    with _controller_attempt(directory, "live--0001"):
        result = client.create(
            engine="claude", cwd=str(tmp_path), worker_id=A, variables=mapping,
        )
        # Read the real active runtime, not a replacement public create observer.
        import importlib
        runtime = importlib.import_module('ccx.controllers.controller')._active_runtime.get()
        owner = runtime.live_vars_owner()
        assert owner._workers[("claude", A)]["box"].mapping is mapping
        assert owner._workers[("claude", A)]["box"].mapping["box"] is mapping["box"]
    assert result["worker_id"] == A
    journal = controller_store.read_json(live_path)
    assert journal["state"] == "accepted"
    assert journal["metadata"]["workerId"] == A
    assert journal["metadata"]["variableMode"] == "live"
    assert journal["metadata"]["liveVariables"] == ["box"]
    assert "Box" not in json.dumps(journal)
    first_receipt = _receipt(owned, "claude", A)
    assert len(observed) == 1

    with _controller_attempt(directory, "live--0001"), pytest.raises(
        ControllerLiveStateLost,
    ):
        client.create(engine="claude", cwd=str(tmp_path), worker_id=A,
                      variables={"box": Box()})
    with _controller_attempt(directory, "live--0001"), pytest.raises(
        ControllerReplayMismatch,
    ):
        client.create(engine="claude", cwd=str(tmp_path), worker_id=A,
                      variables={"other": Box()})
    assert len(observed) == 1
    assert _receipt(owned, "claude", A) == first_receipt

    # A controller intent interrupted after the first receipt, not a RuntimeError
    # (which creation.create would convert to a *stopped* CreatePartialFailure).
    client.create(engine="codex", cwd=str(tmp_path), worker_id=B,
                  variables={"shared": 1})
    client.create(engine="claude", cwd=str(tmp_path), worker_id=C,
                  variables={"other": 2})
    original_state = client.variables(B)["state_id"]
    replacement_state = client.variables(C)["state_id"]
    assert replacement_state != original_state
    share_path = directory / "tasks" / "share--0001" / "operations" / "0001-create.json"

    class Interrupted(BaseException):
        """Simulated outer attempt interruption, outside creation's Exception catch."""

    original_complete = creation._complete_codex_create
    seen = []

    def interrupt_once(store, original, *args, **kwargs):
        if original.worker_id == D:
            seen.append(original.worker)
            assert _receipt(store, "codex", D) is not None
            raise Interrupted()
        return original_complete(store, original, *args, **kwargs)

    monkeypatch.setattr(creation, "_complete_codex_create", interrupt_once)
    with _controller_attempt(directory, "share--0001"), pytest.raises(Interrupted):
        client.create(engine="codex", cwd=str(tmp_path), worker_id=D,
                      variables_from="codex:" + B)
    assert seen == ["codex:" + D]
    first_shared_receipt = _receipt(owned, "codex", D)
    assert first_shared_receipt is not None
    assert client.variables(D)["state_id"] == original_state
    pending = controller_store.read_json(share_path)
    assert pending["state"] == "indeterminate"
    assert pending["metadata"]["workerId"] == D
    assert pending["metadata"]["variableSource"] == "codex:" + B

    live_vars.publish_attachment(
        live_vars.prepare_shared_durable_attachment("claude", C), "codex", B,
    )
    assert client.variables(B)["state_id"] == replacement_state
    with _controller_attempt(directory, "share--0001"), pytest.raises(
        OperationConflict,
    ) as conflict:
        client.create(engine="codex", cwd=str(tmp_path), worker_id=D,
                      variables_from={"worker": "codex:" + B})
    assert conflict.value.reason == "initial_variables_mismatch"
    assert seen == ["codex:" + D]  # No second native effect.
    assert _receipt(owned, "codex", D) == first_shared_receipt
    assert client.variables(D)["state_id"] == original_state
    # OperationConflict subclasses InvalidRequest: the existing controller
    # records a safe rejection, without changing its original UUID or binding.
    assert controller_store.read_json(share_path)["state"] == "rejected"
