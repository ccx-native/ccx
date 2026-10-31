from __future__ import annotations

import io
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from ccx.rpc import local_rpc as R
from ccx.errors import DeliveryRejected
from test_generation_store import short_root

def _request(op, params=None, *, request_id="req-1", version=2, timeout_ms=None):
    value = {
        "protocol_version": version,
        "request_id": request_id,
        "op": op,
        "params": params or {},
    }
    if timeout_ms is not None:
        value["timeout_ms"] = timeout_ms
    return value

@pytest.mark.parametrize(
    "payload,code",
    [
        ([], "invalid_request"),
        ({"protocol_version": 2, "request_id": "", "op": "x"}, "invalid_request"),
        (
            {"protocol_version": 2, "request_id": "r", "op": "x", "params": []},
            "invalid_request",
        ),
    ],
)
def test_invalid_envelopes_return_stable_errors(payload, code):
    response = R.dispatch(payload)
    assert response["ok"] is False
    assert response["error"]["code"] == code

@pytest.mark.parametrize(
    "timeout_ms",
    [float("nan"), float("inf"), float("-inf"), 10**400],
)
def test_nonfinite_or_unrepresentable_timeout_is_invalid_request(timeout_ms):
    response = R.dispatch(
        _request(
            "worker.inspect",
            {"target": "claude:abc12345"},
            timeout_ms=timeout_ms,
        )
    )

    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_request"
    assert "finite positive number" in response["error"]["message"]

def test_read_request_rejects_nonstandard_json_constants(monkeypatch):
    raw = b'{"protocol_version":2,"request_id":"x","op":"worker.inspect","params":{"target":"x"},"timeout_ms":NaN}'
    monkeypatch.setattr(R.sys, "stdin", io.BytesIO(raw))

    with pytest.raises(R.InvalidRequest, match="not valid JSON"):
        R._read_request()

def test_serialized_response_never_emits_nonstandard_json_numbers():
    payload = R._serialized(
        {
            "protocol_version": 2,
            "request_id": "x",
            "ok": True,
            "result": {"used_percent": float("nan")},
        }
    )
    response = json.loads(payload)

    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_request"

def test_unknown_operation_is_capability_error():
    response = R.dispatch(_request("worker.teleport"))

    assert response["error"]["code"] == "invalid_request"
    assert response["error"]["details"]["operation"] == "worker.teleport"

def test_response_size_limit_returns_valid_error():
    response = {
        "protocol_version": 2,
        "request_id": "large",
        "ok": True,
        "result": {"text": "x" * (R.MAX_MESSAGE_BYTES + 100)},
    }

    raw = R._serialized(response)
    decoded = json.loads(raw)

    assert len(raw) < R.MAX_MESSAGE_BYTES
    assert decoded["ok"] is False
    assert decoded["error"]["code"] == "invalid_request"

def test_real_process_malformed_json_still_returns_one_protocol_line(tmp_path):
    repo = Path(__file__).resolve().parents[1]
    root = tmp_path / "root"
    proc = subprocess.run(
        [sys.executable, str(repo / "ccx.py"), "rpc"],
        input="{not-json",
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env={**os.environ, "CCX_ROOT": str(root), "PYTHONPATH": str(repo)},
        timeout=20,
    )

    assert proc.returncode == 0
    assert len(proc.stdout.splitlines()) == 1
    result = json.loads(proc.stdout)
    assert result["ok"] is False
    assert result["error"]["code"] == "invalid_request"

@pytest.mark.parametrize("rate_limited, transient, cls", [
    (True, True, "WorkerRateLimited"),
    (False, True, "WorkerApiTransient"),
    (False, False, "WorkerApiPermanent"),
])
def test_worker_api_errors_keep_their_classification(rate_limited, transient, cls):
    from ccx.errors import build_worker_api_error

    error = build_worker_api_error(
        "Claude API rate limit (429): slow down", short="abc12345",
        kind="rate_limit" if rate_limited else "overloaded_error",
        status=429 if rate_limited else 529, transient=transient,
        rate_limited=rate_limited, request_id="req_1", detail="slow down",
        reset_text="resets 8:40pm" if rate_limited else None,
    )
    assert type(error).__name__ == cls

    response = R._error_from_exception("r", error)

    assert response["error"]["code"] == (
        "rate_limited" if rate_limited else
        "transient_api_error" if transient else "unavailable")
    assert response["error"]["details"]["api_error"] == {
        "kind": "rate_limit" if rate_limited else "overloaded_error",
        "status": 429 if rate_limited else 529,
        "transient": transient,
        "rate_limited": rate_limited,
        "request_id": "req_1",
        "message": "slow down",
        "reset_text": "resets 8:40pm" if rate_limited else None,
    }

def test_compact_failed_rejection_carries_its_api_error():
    from ccx.errors import CompactFailed, build_worker_api_error

    api = build_worker_api_error("Claude API overloaded (529)", kind="overloaded_error",
                                 status=529, transient=True)
    try:
        try:
            raise CompactFailed("compact failed", short="abc12345", api_error=api)
        except CompactFailed as cause:
            raise DeliveryRejected("compact failed", short="abc12345",
                                   state="idle", refusal="compact-failed") from cause
    except DeliveryRejected as error:
        response = R._error_from_exception("r", error)

    assert response["error"]["code"] == "refused"
    assert response["error"]["details"]["refusal"] == "compact-failed"
    assert response["error"]["details"]["api_error"]["status"] == 529
    assert response["error"]["details"]["api_error"]["transient"] is True


@pytest.mark.parametrize("accept_during_hold", [True, False], ids=["accepted", "timed-out-pending"])
def test_v1_reconcile_hold_while_pending_is_bounded_and_never_resends(
    short_root, monkeypatch, accept_during_hold,
):
    """A product may hold the *same* delivery receipt, never replay native input."""
    from ccx.store.root import require_ccx_active
    from test_generation_store import A

    store = require_ccx_active(short_root)
    target = f"claude:{A}"
    delivery_id = "product-hold-delivery-1"
    now = [100.0]
    calls = []
    native_submissions = []

    # Real operation, deterministic bounded monotonic clock. No worker/provider.
    monkeypatch.setattr(R.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(R.time, "sleep", lambda seconds: now.__setitem__(0, now[0] + seconds))

    def receipt(worker, *, delivery_id, turn_id, timeout, include_worker=True):
        assert worker == target
        assert delivery_id == "product-hold-delivery-1"
        assert turn_id is None and timeout > 0
        calls.append((include_worker, now[0]))
        # Two cheap probes show a pending receipt. It is either accepted during
        # the hold, or remains pending until the *bounded* last observation.
        accepted = accept_during_hold and len(calls) >= 3
        return {
            "worker": {"worker": target}, "delivery_id": delivery_id,
            "status": "accepted" if accepted else "pending",
            "pending": not accepted,
            "recommended_action": "none" if accepted else "wait",
        }

    monkeypatch.setattr(R, "reconcile_turn", receipt)
    monkeypatch.setattr(R, "send_turn", lambda *a, **k: native_submissions.append((a, k)))
    envelope = _request(
        "turn.reconcile", {"target": target, "delivery_id": delivery_id,
                           "hold_while_pending": True},
        request_id="product-hold-v1", timeout_ms=700,
    )
    result = R.dispatch_full_uuid(envelope, store, deadline=100.7)

    assert result["ok"] is True, result
    assert result["request_id"] == "product-hold-v1"
    assert result["result"]["delivery"]["delivery_id"] == delivery_id
    assert result["result"]["delivery"]["status"] == (
        "accepted" if accept_during_hold else "pending")
    assert len(calls) >= 3
    assert all(not include for include, _ in calls[:-1]), calls
    assert calls[-1][0] is True  # Exactly one final response with the worker snapshot.
    if not accept_during_hold:
        assert now[0] >= 100.45  # Time budget elapsed, rather than polling forever.
    assert now[0] <= 100.7
    assert native_submissions == []
