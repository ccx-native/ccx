"""Original Codex receipt settlement over a disposable fake JSON-RPC server."""

from __future__ import annotations

import json
import time
from types import SimpleNamespace

import pytest

from ccx import WorkerClient
from ccx.codex import delivery as codex_delivery
from ccx.codex import evidence as codex_evidence
from ccx.codex import runtime as codex_runtime
from ccx.codex.completion import settle_completed
from ccx.errors import SessionUnavailable
from ccx.ops.send import ensure_ready
from ccx.ops.wait import reconcile_turn
from test_codex_native_effects import _f2_fake_processes, _f2_new
from test_generation_store import B, short_root  # noqa: F401


def _create(cli, root):
    created = _f2_new(cli, root, B)
    assert created.returncode == 0, (created.stdout, created.stderr)
    assert json.loads(created.stdout)["native_ready"]
    ready = ensure_ready(B)
    assert ready["loaded"] and ready["state"] == "idle"


def _send(cli, prompt, delivery):
    result = cli("send", B, prompt, "--no-wait", "--delivery-id", delivery,
                 "--json")
    assert result.returncode == 0, (result.stdout, result.stderr)
    receipt = json.loads(result.stdout)
    assert receipt["delivery_id"] == delivery
    assert receipt["status"] == "accepted"
    return receipt


def _settled(delivery, seconds=5):
    deadline = time.monotonic() + seconds
    receipt = None
    while time.monotonic() < deadline:
        receipt = codex_delivery.read_receipt(B, delivery)
        if receipt and isinstance(receipt.get("settlement"), dict):
            break
        time.sleep(.03)
    assert receipt and isinstance(receipt.get("settlement"), dict), receipt
    return receipt


def test_live_completion_settles_even_when_history_is_slow_and_large(
    short_root, tmp_path, monkeypatch,
):
    delay = tmp_path / "read-delay"
    padding = tmp_path / "read-padding"
    completion_gate = tmp_path / "native-completion-gate"
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch,
        read_delay_path=delay, read_padding_path=padding, phase_messages=True,
        completion_gate=completion_gate,
    ) as (cli, _restart, trace, store, _rpc):
        _create(cli, tmp_path)
        sent = _send(cli, "f1-race-live slow complete", "live-large-completion")
        delay.write_text("2.35")
        padding.write_text("2200000")
        reads_before = trace.read_text().count("RPC thread/read")
        completion_gate.touch()
        settled = _settled("live-large-completion")
        proof = settled["settlement"]
        assert proof["workerId"] == B
        assert proof["threadId"] == store.record(B)["native"]["threadId"]
        assert proof["deliveryId"] == "live-large-completion"
        assert proof["turnId"] == settled["turnId"]
        assert proof["status"] == "completed"
        assert proof["text"] == "Fake Codex completed: f1-race-live slow complete"
        assert len(proof["nativeEvidenceSha256"]) == 64
        assert sent["delivery"]["turn_id"] == settled["turnId"]
        answer = WorkerClient().report(B, turn_ref=settled["turnRef"], timeout=5)
        assert answer["text"] == proof["text"]
        assert answer["source"] == "original-native-completion-receipt"
        assert trace.read_text().count("RPC thread/read") == reads_before
        assert trace.read_text().count("RPC turn/start") == 1

        # A different/failed outcome cannot settle someone else's receipt;
        # a repeated original completion keeps its immutable first proof.
        native_id = proof["threadId"]
        assert settle_completed(store, B, thread_id=native_id, turn={
            "id": "other-turn", "status": "completed", "items": [],
        }) is None
        assert settle_completed(store, B, thread_id=native_id, turn={
            "id": settled["turnId"], "status": "interrupted", "items": [],
        }) is None
        same = settle_completed(store, B, thread_id=native_id, turn={
            "id": settled["turnId"], "status": "completed", "items": [],
        })
        assert same["settlement"] == proof


def test_preexisting_accepted_without_notification_recovers_slow_history(
    short_root, tmp_path, monkeypatch,
):
    delay = tmp_path / "read-delay"
    read_mode = tmp_path / "read-mode"
    padding = tmp_path / "read-padding"
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch, omit_completion=True,
        read_delay_path=delay, read_padding_path=padding,
        read_mode=read_mode, phase_messages=True,
    ) as (cli, _restart, trace, store, _rpc):
        _create(cli, tmp_path)
        _send(cli, "historical accepted answer", "pre-fix-accepted")
        original = codex_delivery.read_receipt(B, "pre-fix-accepted")
        assert original["state"] == "accepted"
        assert original.get("settlement") is None
        reads_before = trace.read_text().count("RPC thread/read")
        delay.write_text("2.35")
        padding.write_text("2200000")
        read_mode.write_text("foreign")
        with pytest.raises(SessionUnavailable):
            WorkerClient().report(B, turn_ref=original["turnRef"], timeout=6)
        assert codex_delivery.read_receipt(B, "pre-fix-accepted").get("settlement") is None

        read_mode.write_text("normal")
        answer = WorkerClient().report(B, turn_ref=original["turnRef"], timeout=7)
        assert answer["text"] == "Fake Codex completed: historical accepted answer"
        assert answer["source"] == "original-native-completion-receipt"
        assert codex_delivery.read_receipt(B, "pre-fix-accepted")["settlement"]["text"] == answer["text"]
        assert reconcile_turn(B, delivery_id="pre-fix-accepted")["status"] == "settled"
        assert trace.read_text().count("RPC turn/start") == 1
        assert trace.read_text().count("RPC thread/read") >= reads_before + 2

        # The public independent history API must also return a >1 MiB
        # source-proven native result, rather than "codexd unavailable: ValueError".
        native = store.record(B)["native"]
        thread = codex_runtime.read_thread(
            native["threadId"], cwd=str(tmp_path), worker_id=B,
            expected_source=native["threadSource"], timeout=7,
        )
        assert len(thread["largeRollout"]) == 2200000
        assert thread["threadSource"] == native["threadSource"]


def test_preexisting_accepted_recovers_from_admitted_journal_without_native_read(
    short_root, tmp_path, monkeypatch,
):
    read_mode = tmp_path / "read-mode"
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch,
        omit_completion=True, read_mode=read_mode,
    ) as (cli, _restart, trace, store, _rpc):
        _create(cli, tmp_path)
        _send(cli, "before-fix journal", "old-journal-receipt")
        accepted = codex_delivery.read_receipt(B, "old-journal-receipt")
        assert accepted.get("settlement") is None
        native = store.record(B)["native"]
        tid, turn = native["threadId"], accepted["turnId"]
        read_mode.write_text("foreign")  # If journal is ignored, recovery MUST fail.
        terminal = {
            "method": "turn/completed", "threadId": tid, "turnId": turn,
            "params": {"turn": {"id": turn, "status": "completed", "items": [
                {"type": "agentMessage", "phase": "commentary", "text": "not final"},
                {"type": "agentMessage", "text": "phase-less fallback"},
            ]}},
        }
        started = {
            "method": "turn/started", "threadId": tid, "turnId": turn,
            "params": {"turn": {"id": turn}},
        }
        final_item = {
            "method": "item/completed", "threadId": tid, "turnId": turn,
            "params": {"item": {"id": "exact-final", "type": "agentMessage",
                                 "phase": "final_answer", "text": "journal original final"}},
        }
        snapshot = SimpleNamespace(complete=True, events=[terminal])
        with monkeypatch.context() as patch:
            patch.setattr(codex_evidence, "read_journal", lambda _worker: snapshot)
            # A terminal line without a matching start is not an admitted
            # notification. Neither the journal nor a foreign native thread
            # may settle this receipt.
            with pytest.raises(SessionUnavailable):
                WorkerClient().report(B, turn_ref=accepted["turnRef"], timeout=4)
            assert codex_delivery.read_receipt(B, "old-journal-receipt").get("settlement") is None

            before = trace.read_text().count("RPC thread/read")
            snapshot.events = [started, final_item, terminal]
            report = WorkerClient().report(B, turn_ref=accepted["turnRef"], timeout=4)
            assert report["text"] == "journal original final"
            assert report["source"] == "original-native-completion-receipt"
            assert trace.read_text().count("RPC thread/read") == before
            assert codex_delivery.read_receipt(B, "old-journal-receipt")["settlement"]["status"] == "completed"
        assert trace.read_text().count("RPC turn/start") == 1


def _completed_record_without_notification(cli, root, store, *, prompt, delivery):
    """Fake an old codexd that accepted a turn but has no completion handler."""
    _create(cli, root)
    _send(cli, prompt, delivery)
    accepted = codex_delivery.read_receipt(B, delivery)
    assert accepted["state"] == "accepted" and accepted.get("settlement") is None
    turn_id = accepted["turnId"]
    # Old daemons could persist lastTurn while leaving an accepted receipt
    # without a settlement. Emulate that record only in this fake generation.
    store.mutate_record(B, patches={
        "lastTurn": {"turnId": turn_id, "status": "completed"},
        "lifecycle.state": "idle",
        "lifecycle.activeTurnId": None,
    })
    return accepted, store.record(B)["native"]


def test_default_report_recovers_latest_accepted_receipt_from_original_journal(
    short_root, tmp_path, monkeypatch,
):
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch, omit_completion=True,
    ) as (cli, _restart, trace, store, _rpc):
        accepted, native = _completed_record_without_notification(
            cli, tmp_path, store, prompt="latest durable default answer",
            delivery="default-journal-complete",
        )
        thread_id, turn_id = native["threadId"], accepted["turnId"]
        snapshot = SimpleNamespace(complete=True, events=[
            {"method": "turn/started", "threadId": thread_id, "turnId": turn_id,
             "params": {"turn": {"id": turn_id}}},
            {"method": "item/completed", "threadId": thread_id, "turnId": turn_id,
             "params": {"item": {"type": "agentMessage", "phase": "final_answer",
                                 "text": "ORIGINAL journal final answer"}}},
            {"method": "turn/completed", "threadId": thread_id, "turnId": turn_id,
             "params": {"turn": {"id": turn_id, "status": "completed", "items": [
                 {"type": "agentMessage", "phase": "commentary", "text": "not final"},
                 {"type": "agentMessage", "text": "weaker phase-less answer"},
             ]}}},
        ])
        reads = []
        def journal_for_exact_worker(worker_id):
            reads.append(worker_id)
            assert worker_id == B
            return snapshot
        monkeypatch.setattr(codex_evidence, "read_journal", journal_for_exact_worker)
        monkeypatch.setattr(codex_runtime, "read_thread", lambda *_a, **_kw:
                            pytest.fail("default journal settlement must not hydrate history"))
        before = trace.read_text().count("RPC thread/read")
        report = WorkerClient().report(B, timeout=4)
        assert report["text"] == "ORIGINAL journal final answer"
        assert report["source"] == "original-native-completion-receipt"
        assert report["delivery_id"] == "default-journal-complete"
        assert report["turn_ref"] == accepted["turnRef"]
        receipt = codex_delivery.read_receipt(B, "default-journal-complete")
        assert receipt["settlement"]["text"] == report["text"]
        assert receipt["settlement"]["turnId"] == turn_id
        assert len(receipt["settlement"]["nativeEvidenceSha256"]) == 64
        assert reads == [B]
        assert trace.read_text().count("RPC thread/read") == before
        assert trace.read_text().count("RPC turn/start") == 1
        # The same default report is receipt-backed on subsequent calls, with
        # no second journal read or any additional native operation.
        assert WorkerClient().report(B, timeout=4)["text"] == report["text"]
        assert reads == [B]


def test_default_report_missing_journal_completion_falls_through_to_native_history(
    short_root, tmp_path, monkeypatch,
):
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch, omit_completion=True,
    ) as (cli, _restart, trace, store, _rpc):
        accepted, native = _completed_record_without_notification(
            cli, tmp_path, store, prompt="latest pending journal completion",
            delivery="default-journal-absent",
        )
        thread_id, turn_id = native["threadId"], accepted["turnId"]
        journal_reads = []
        def missing_completion(worker_id):
            journal_reads.append(worker_id)
            return SimpleNamespace(complete=True, events=[
                {"method": "turn/started", "threadId": thread_id, "turnId": turn_id,
                 "params": {"turn": {"id": turn_id}}},
            ])
        monkeypatch.setattr(codex_evidence, "read_journal", missing_completion)
        native_reads = []
        def read_history(requested_id, *, cwd, worker_id, expected_source,
                         timeout, include_turns=True):
            native_reads.append(requested_id)
            assert requested_id == thread_id and worker_id == B
            assert expected_source == native["threadSource"]
            assert timeout > 0 and include_turns
            return {"id": thread_id, "threadSource": expected_source, "turns": [
                {"id": turn_id, "status": "completed", "items": [
                    {"type": "agentMessage", "phase": "final_answer",
                     "text": "latest native fallback answer"},
                ]},
            ]}
        monkeypatch.setattr(codex_runtime, "read_thread", read_history)
        report = WorkerClient().report(B, timeout=4)
        assert report["text"] == "latest native fallback answer"
        assert report["source"] == "native-transcript-latest"
        assert report["delivery_id"] is None
        assert report["turn_ref"] == accepted["turnRef"]
        assert journal_reads == [B] and native_reads == [thread_id]
        assert codex_delivery.read_receipt(B, "default-journal-absent").get("settlement") is None
        assert trace.read_text().count("RPC turn/start") == 1


def test_default_report_never_settles_or_reuses_older_accepted_receipt(
    short_root, tmp_path, monkeypatch,
):
    with _f2_fake_processes(
        short_root, tmp_path, monkeypatch, omit_completion=True,
    ) as (cli, _restart, trace, store, _rpc):
        accepted, native = _completed_record_without_notification(
            cli, tmp_path, store, prompt="older unsettled delivery",
            delivery="accepted-older-only",
        )
        newest_id = "newer-native-without-delivery"
        store.mutate_record(B, patches={
            "lastTurn": {"turnId": newest_id, "status": "completed"},
        })
        monkeypatch.setattr(codex_evidence, "read_journal", lambda *_a, **_kw:
                            pytest.fail("older accepted receipt must not start a journal read"))
        native_reads = []
        def read_history(requested_id, *, cwd, worker_id, expected_source,
                         timeout, include_turns=True):
            native_reads.append(requested_id)
            assert requested_id == native["threadId"] and worker_id == B
            assert expected_source == native["threadSource"]
            return {"id": requested_id, "threadSource": expected_source, "turns": [
                {"id": accepted["turnId"], "status": "completed", "items": [
                    {"type": "agentMessage", "phase": "final_answer", "text": "OLD"},
                ]},
                {"id": newest_id, "status": "completed", "items": [
                    {"type": "agentMessage", "phase": "final_answer", "text": "LATEST"},
                ]},
            ]}
        monkeypatch.setattr(codex_runtime, "read_thread", read_history)
        report = WorkerClient().report(B, timeout=4)
        assert report["text"] == "LATEST"
        assert report["turn_id"] == newest_id
        assert report["source"] == "native-transcript-latest"
        assert report["delivery_id"] is None
        assert native_reads == [native["threadId"]]
        older = codex_delivery.read_receipt(B, "accepted-older-only")
        assert older["state"] == "accepted" and older.get("settlement") is None
        assert trace.read_text().count("RPC turn/start") == 1


@pytest.mark.parametrize("prompt", ["initial-wait-failed-error", "initial-wait-interrupted-error"])
def test_failed_or_interrupted_native_turn_does_not_settle_original(
    short_root, tmp_path, monkeypatch, prompt,
):
    with _f2_fake_processes(short_root, tmp_path, monkeypatch) as (cli, _restart, trace, _store, _rpc):
        _create(cli, tmp_path)
        _send(cli, prompt, "non-success-receipt")
        until = time.monotonic() + 4
        outcome = None
        while time.monotonic() < until:
            outcome = reconcile_turn(B, delivery_id="non-success-receipt", timeout=4)
            if outcome.get("native_status") in {"failed", "interrupted"}:
                break
            time.sleep(.03)
        assert outcome["status"] == "accepted"
        assert outcome["native_status"] in {"failed", "interrupted"}
        assert codex_delivery.read_receipt(B, "non-success-receipt").get("settlement") is None
        assert trace.read_text().count("RPC turn/start") == 1
