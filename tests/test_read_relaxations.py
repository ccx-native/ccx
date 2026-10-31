"""Optional read-source failures must not discard independent owned data."""

from __future__ import annotations
import ccx.ops.report as ops_report
import ccx.ops.wait as ops_wait

import json
import time

import pytest

from ccx import CcxClient
from ccx import WorkerClient
from ccx import constants
from ccx.ops import common as protocol_ops
from ccx.cli.main import _main
from ccx.errors import InvalidRequest, SessionUnavailable
from ccx.store.root import StorageIntegrityError, require_ccx_active
from test_generation_store import A, B, C, add_worker, put, short_root
from test_worker_events import _event, _journal
from test_dashboard_example import dash, wait_until


def test_bad_claude_context_measurement_keeps_settlement_and_identity(short_root, monkeypatch):
    add_worker(short_root, A)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    store = require_ccx_active(short_root)
    ref = "ccx-turn:claude:v1:settled"
    store.mutate_record(A, patches={"lastSettledTurn": {
        "turnRef": ref, "context": {
            "state": "known", "tokens_used": 89, "as_of_ms": 10,
        },
    }})
    sidecar = store.worker_path(A, "context-observation.json")
    doc = {
        "schemaVersion": 1, "workerId": A,
        "nativeSessionId": A, "generation": store.generation,
        "source": {"dev": 1, "ino": 2, "endOffset": 3, "observedAtMs": 4},
        "observedModel": "observed-claude", "context": {
            "state": "known", "tokens_used": -8, "as_of_ms": 11,
        },
    }
    put(sidecar, doc)
    assert WorkerClient().status(A)["context"]["tokens_used"] == 89
    assert WorkerClient().context(A)["as_of_turn_ref"] == ref
    assert CcxClient().peek(A).context["tokens_used"] == 89
    assert "invalid" in CcxClient().context(A).note
    assert CcxClient().context(A).tokens_used == 89

    store.mutate_record(A, patches={"lastSettledTurn": None})
    assert WorkerClient().context(A)["tokens_used"] is None
    assert "invalid" in WorkerClient().context(A)["note"]
    assert WorkerClient().status(A)["state"] is not None
    doc["workerId"] = B  # plausible same-prefix foreign observation must still refuse
    put(sidecar, doc)
    with pytest.raises(StorageIntegrityError, match="identity/provenance"):
        WorkerClient().context(A)


def test_malformed_context_keeps_fresh_owned_statusline(short_root, monkeypatch):
    add_worker(short_root, A)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    store = require_ccx_active(short_root)
    put(store.worker_path(A, "context-observation.json"), {
        "schemaVersion": 1, "workerId": A, "nativeSessionId": A,
        "generation": store.generation,
        "source": {"dev": 1, "ino": 2, "endOffset": 3, "observedAtMs": 4},
        "observedModel": "observed-claude",
        "context": {"state": "known", "tokens_used": -1, "as_of_ms": 10},
    })
    put(store.worker_path(A, "statusline.json"), {
        "schemaVersion": 1, "short": A, "sessionId": A,
        "writtenAtMs": int(time.time() * 1000),
        "contextWindow": {"total_input_tokens": 321},
    })
    # No settlement: independent statusline usage must survive a bad sidecar.
    context = WorkerClient().context(A)
    assert context["tokens_used"] == 321
    assert "invalid" in context["note"]
    assert WorkerClient().status(A)["observed_model"] == "observed-claude"
    assert CcxClient().peek(A).context["tokens_used"] == 321

    # When busy, a newer live statusline wins over valid older settlement.
    store.mutate_record(A, patches={
        "lifecycle.state": "busy",
        "lastSettledTurn": {"turnRef": "old-turn", "context": {
            "state": "known", "tokens_used": 88, "as_of_ms": 1,
        }},
    })
    assert WorkerClient().context(A)["tokens_used"] == 321


@pytest.mark.parametrize("state", [[], {}, {"bad": "state"}])
def test_unhashable_context_state_is_discarded(short_root, monkeypatch, state):
    add_worker(short_root, A)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    store = require_ccx_active(short_root)
    put(store.worker_path(A, "context-observation.json"), {
        "schemaVersion": 1, "workerId": A, "nativeSessionId": A,
        "generation": store.generation,
        "source": {"dev": 1, "ino": 2, "endOffset": 3, "observedAtMs": 4},
        "context": {"state": state, "tokens_used": 7},
    })
    status = WorkerClient().status(A)
    assert status["context"]["tokens_used"] is None
    assert "invalid" in status["context"]["note"]
    assert CcxClient().peek(A).context["reason"] == "no-signal"


def test_report_and_wait_keep_primary_result_if_task_enrichment_fails(short_root, monkeypatch):
    add_worker(short_root, A)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)

    def unreadable(*_args):
        raise SessionUnavailable("transcript disappeared")

    monkeypatch.setattr(CcxClient, "_uuid_background_task_facts_owned", staticmethod(unreadable))
    monkeypatch.setattr(ops_report, "report_worker", lambda *_args, **_kw: {
        "text": "verified answer", "truncated": False, "has_final_message": True,
        "status": "completed", "source": "recorded-receipt", "turn_ref": "original-turn",
    })
    monkeypatch.setattr(ops_wait, "wait_worker", lambda *_args, **_kw: {
        "worker": f"claude:{A}", "status": "completed", "state": "idle",
    })
    client = WorkerClient()
    report = client.report(A, turn_ref="original-turn")
    wait = client.wait(A, timeout=0.1)
    assert report["text"] == "verified answer" and wait["status"] == "completed"
    for result in (report, wait):
        assert result["background_tasks_supported"] is True
        assert result["background_tasks_available"] is False
        assert "transcript" in result["background_task_note"]
        assert "background_tasks" not in result
        assert "background_task_count" not in result
    with pytest.raises(SessionUnavailable):
        CcxClient().background_tasks(A)  # explicitly requested task source stays strict


def test_batch_report_returns_good_rows_and_per_target_failures(short_root, monkeypatch):
    add_worker(short_root, A)
    add_worker(short_root, B)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)

    def selected(worker, **_kw):
        if worker == f"claude:{B}":
            raise SessionUnavailable("no completed reply yet")
        return {"text": "answer A", "note": None}

    monkeypatch.setattr(ops_report, "report_worker", selected)
    batch = CcxClient().report([A, B, C])
    assert [r.text for r in batch["reports"]] == ["answer A"]
    assert batch["errors"][B]["error"] == "SessionUnavailable"
    assert batch["errors"][C]["error"] == "SessionNotFound"
    assert CcxClient().report(A).text == "answer A"


def test_batch_report_rejects_shared_invalid_max_chars_once(short_root, monkeypatch):
    add_worker(short_root, A)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    invoked = []
    def should_not_report(*args, **kwargs):
        invoked.append((args, kwargs))
        raise AssertionError("invalid shared arguments must fail before the loop")
    monkeypatch.setattr(ops_report, "report_worker", should_not_report)
    client = CcxClient()
    for targets in ([], [A, "missing"]):
        with pytest.raises(InvalidRequest, match="max_chars"):
            client.report(targets, max_chars=-1)
    assert invoked == []


def test_cli_peek_status_batch_exposes_partial_errors_and_nonzero_exit(short_root, monkeypatch, capsys):
    add_worker(short_root, A)
    add_worker(short_root, B)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    with pytest.raises(SystemExit) as failed:
        _main(["peek", A, C, "aaaaaaaa", "--status", "--json"])
    assert failed.value.code != 0
    output = json.loads(capsys.readouterr().out)
    assert [r["worker_id"] for r in output["statuses"]] == [A]
    assert output["errors"][C]["error"]
    assert output["errors"]["aaaaaaaa"]["error"] == "AmbiguousWorker"


def test_malformed_inventory_timestamps_are_unknown_not_fleet_failure(short_root, monkeypatch):
    add_worker(short_root, A)
    add_worker(short_root, B, "codex")
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    store = require_ccx_active(short_root)
    store.mutate_record(B, patches={"lastTurnAt": -1.2})
    path = store.worker_path(B, "record.json")
    record = json.loads(path.read_text())
    record["createdAt"] = "not-an-int"  # immutable creation field: corrupt fixture bytes
    put(path, record)
    rows = store.list_workers()
    assert {r["worker_id"] for r in rows} == {A, B}
    damaged = next(r for r in rows if r["worker_id"] == B)
    assert damaged["created_at"] is None and damaged["last_turn_at"] is None
    assert "timestamp" in damaged["warning"]


def test_typed_claude_inventory_uses_sanitized_row_timestamps(short_root, monkeypatch):
    add_worker(short_root, A)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    store = require_ccx_active(short_root)
    path = store.worker_path(A, "record.json")
    original = store.record(A)
    original["createdAt"] = "broken"
    original["lastTurnAt"] = -1.2
    put(path, original)

    row = store.list_workers(engine="claude")[0]
    assert row["created_at"] is None and row["last_turn_at"] is None
    typed = CcxClient().list()[0]
    assert typed.created_at is None and typed.last_turn_at is None
    assert typed.as_dict().get("warning") == row["warning"]
    assert store.record(A)["createdAt"] == "broken"  # no in-place sanitizer
    assert store.record(A)["lastTurnAt"] == -1.2


def test_oversized_event_line_has_gap_and_next_events_are_readable(short_root, monkeypatch):
    from ccx.ops import worker_events

    add_worker(short_root, A)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    path = _journal(short_root)
    path.write_bytes((json.dumps(_event("E1", 1)) + "\n").encode()
                     + (json.dumps({**_event("E1", 2),
                                    "padding": "x" * (worker_events.MAX_PAGE_BYTES + 32)}) + "\n").encode()
                     + (json.dumps(_event("E1", 3)) + "\n").encode())
    path.chmod(0o600)
    worker = WorkerClient()
    first = worker.events(A)
    assert [e["seq"] for e in first["events"]] == [1]
    next_page = worker.events(A, after=first["next_cursor"])
    assert [e["seq"] for e in next_page["events"]] == [3]
    assert next_page["gap"]["reason"] == "oversized_event_line"
    assert not next_page["has_more"]
    assert worker.events(A, after=next_page["next_cursor"])["events"] == []


def test_dashboard_partial_statuses_drop_stale_actions_and_retain_good_rows():
    a, b = f"claude:{A}", f"claude:{B}"
    statuses, error = dash.fetch_statuses("ccx", [a, b], lambda *_args, **_kw: ({
        "statuses": [{"worker": a, "state": "idle"}],
        "errors": {b: {"error": "SessionUnavailable", "message": "record unreadable"}},
    }, None))
    assert set(statuses) == {a}
    assert b in error
    store = dash.Store()
    store.update_statuses({b: {"worker": b, "pending_input": {
        "request_id": "old-question", "kind": "question",
    }}}, None)
    store.update_statuses(statuses, error, (a, b))
    assert set(store.snapshot()["statuses"]) == {a}


def test_dashboard_stale_conversation_retries_and_focus_change_clears():
    a, b = f"claude:{A}", f"claude:{B}"
    document = {"schemaVersion": 5, "turns": [{"role": "assistant", "text": "saved"}]}
    outcomes = [(document, None), (None, "temporary timeout"), (document, None)]
    calls = []

    def runner(_binary, argv, **_kw):
        calls.append(argv)
        return outcomes.pop(0) if outcomes else (document, None)

    store = dash.Store()
    worker = dash.ConversationWorker("ccx", store, runner=runner)
    worker.start()
    try:
        worker.set_focus(a, (1,))
        wait_until(lambda: store.snapshot()["conversation"] is document,
                   reason="first successful log read")
        worker.set_focus(a, (2,))
        wait_until(lambda: store.snapshot()["conversation_error"] is not None,
                   reason="failed same-worker read")
        snap = store.snapshot()
        assert snap["conversation"] is document
        assert "timeout" in snap["conversation_error"]
        wait_until(lambda: len(calls) >= 3 and store.snapshot()["conversation_error"] is None,
                   timeout=5, reason="automatic retry")
        worker.set_focus(b, (1,))
        assert store.snapshot()["conversation"] is None
        assert store.snapshot()["conversation_worker"] == b
    finally:
        worker.stop_event.set()
        worker.wake()
        worker.join(timeout=2)
