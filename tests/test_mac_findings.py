"""Live macOS findings: Codex approval-gate status race."""
import ccx.ops.send as ops_send
from test_generation_store import short_root  # noqa: F401
from test_worker_telemetry import original_codex, original_sources  # noqa: F401


def test_codex_waiting_flag_before_its_request_stays_busy(original_codex):
    from ccx.codex.daemon.daemon import CodexDaemon
    from ccx.codex.daemon.worker import Worker

    store, _, owner, tid = original_codex
    daemon = CodexDaemon()
    daemon._uuid_store = store
    worker = Worker(daemon, store.record(owner))
    waiting = {"threadId": tid, "status": {"type": "active",
                                           "activeFlags": ["waitingOnApproval"]}}
    try:
        worker.on_notification("turn/started", {"threadId": tid, "turn": {"id": "t1"}})
        worker.on_notification("thread/status/changed", waiting)
        # No gate CCX could answer yet: waiting here would hide the request id.
        assert worker.record["lifecycle"]["state"] == "busy"
        worker.record["pendingApprovals"].append({"gateId": "g1"})
        worker.on_notification("thread/status/changed", waiting)
        assert worker.record["lifecycle"]["state"] == "waiting"
    finally:
        worker.evidence.close()


def test_codex_prompt_over_the_frame_limit_is_refused_before_any_receipt(monkeypatch):
    import pytest
    from ccx.ops import common as protocol_ops
    from ccx.errors import InvalidRequest

    def never(*a, **k):
        raise AssertionError("no receipt or native call for an oversized prompt")
    monkeypatch.setattr(ops_send, "_send_codex_original", never)
    worker_id = "11111111-1111-4111-8111-111111111111"
    monkeypatch.setattr(ops_send, "split_target",
                        lambda target: ("codex:" + worker_id, "codex", worker_id))
    with pytest.raises(InvalidRequest, match="too large for Codex"):
        ops_send.send_turn("codex:" + worker_id,
                               "x" * (1024 * 1024), delivery_id="big-1",
                               _native_submit=never)
