"""A rejected operation must commit a new intent before another effect."""
import importlib
from contextlib import contextmanager

import pytest

from ccx import ControllerOperationIndeterminate
from ccx import DeliveryRejected
from ccx import WorkerClient
from ccx import constants
from ccx.controllers import store as store
from test_generation_store import A, add_worker, fixture_root

C = importlib.import_module('ccx.controllers.controller')


@contextmanager
def _attempt(run_dir):
    runtime = C._ControllerRuntime(run_dir)
    token = C._active_runtime.set(runtime)
    try:
        yield
    finally:
        C._active_runtime.reset(token)
        runtime._executor.shutdown(wait=True)


# Fork is not admitted in this backend slice; the future full-UUID controller
# fork must regain this same fresh-intent/original-UUID contract.
@pytest.mark.parametrize("method", ["send", "create"])
@pytest.mark.parametrize("lost_write", [None, "intent", "accepted"])
def test_rejected_retry_fences_effect_and_preserves_creation_identity(
    tmp_path, monkeypatch, method, lost_write,
):
    root = fixture_root(tmp_path)
    add_worker(root, A, "claude")
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    client = WorkerClient()
    script = tmp_path / "flow.py"
    script.write_text("# inert source\n")
    _, bootstrap = store.create_bootstrap(script, tmp_path, [])
    run_dir = store.publish_bootstrap(bootstrap, name="retry", script=script, workspace=tmp_path)
    journal = run_dir / "operations" / f"0001-{method}.json"
    calls, effects = [], set()
    creating = method == "create"
    args = () if creating else (f"claude:{A}", "one prompt")
    kwargs = {"cwd": str(tmp_path), "engine": "claude"} if creating else {}

    def dispatch(_client, *args, **kwargs):
        durable = store.read_json(journal)
        token = kwargs["worker_id"] if creating else kwargs["delivery_id"]
        calls.append(token)
        assert durable["state"] == "intent"
        assert durable["error"] is None and durable["result"] is None
        assert "completedAt" not in durable
        if creating:
            assert durable["metadata"]["workerId"] == token
        else:
            assert args[0] == f"claude:{A}"
            assert durable["metadata"]["worker"] == f"claude:{A}"
            assert durable["metadata"]["deliveryId"] == token
        if len(calls) == 1:
            raise DeliveryRejected("nothing delivered")
        identity = token if creating else len(calls)
        if creating and identity not in effects:
            # The real create core deduplicates by its committed UUID; the
            # simulated native effect publishes that one ACTIVE original owner.
            add_worker(root, identity, "claude")
        effects.add(identity)
        if creating:
            return {"worker": f"claude:{identity}", "worker_id": identity,
                    "engine": "claude", "cwd": str(tmp_path)}
        return {"worker": args[0], "accepted": True}

    def invoke():
        with _attempt(run_dir):
            return C.observe_worker_call(client, method, dispatch, args, kwargs)

    with pytest.raises(DeliveryRejected):
        invoke()
    assert store.read_json(journal)["state"] == "rejected"
    original_write = store.write_json_atomic

    def fail_write(path, value):
        if path == journal and value.get("state") == lost_write:
            raise OSError("injected journal write loss")
        original_write(path, value)

    with monkeypatch.context() as patch:
        patch.setattr(store, "write_json_atomic", fail_write)
        if lost_write:
            with pytest.raises(OSError, match="injected journal write loss"):
                invoke()
        else:
            invoke()
    if lost_write == "intent":
        assert len(calls) == 1 and not effects
        assert store.read_json(journal)["state"] == "rejected"
    elif lost_write == "accepted":
        assert len(calls) == 2 and len(effects) == 1
        assert store.read_json(journal)["state"] == "intent"

    if lost_write == "accepted" and not creating:
        with pytest.raises(ControllerOperationIndeterminate):
            invoke()
    else:
        invoke()
        assert store.read_json(journal)["state"] == "accepted"
        invoke()  # accepted replay must not dispatch again
    assert len(effects) == 1
    assert len(calls) == (3 if creating and lost_write == "accepted" else 2)
    # One journaled UUID per create; one persisted delivery token per send.
    assert len(set(calls)) == 1
