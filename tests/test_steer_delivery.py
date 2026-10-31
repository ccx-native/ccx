"""Native placement evidence and pure once-only host write decisions."""
from __future__ import annotations

import json

from ccx.ops import steer_ops as SO
from ccx.claude.transcript import queued_prompt_accepted, steer_evidence_events
from test_generation_store import short_root


def _transcript(tmp_path, events):
    path = tmp_path / "transcript.jsonl"
    with path.open("w") as f:
        for evt in events:
            f.write(json.dumps(evt) + "\n")
    return path


def _append(path, events):
    with open(path, "a") as f:
        for evt in events:
            f.write(json.dumps(evt) + "\n")


def _enqueue(text):
    return {"type": "queue-operation", "operation": "enqueue", "content": text,
            "uuid": f"enq-{text}"}


def _user(text, *, queued=False):
    content = ([{"type": "queued_command", "text": text}] if queued
               else [{"type": "text", "text": text}])
    return {"type": "user", "message": {"content": content}, "uuid": f"usr-{text}"}


def test_evidence_kinds_are_ranked_by_placement_strength(tmp_path):
    path = _transcript(tmp_path, [
        _enqueue("do the thing"),
        _user("do the thing", queued=True),
    ])
    kinds = [e["kind"] for e in steer_evidence_events(path, "do the thing")]
    assert kinds == ["enqueued", "consumed"]


def test_attachment_queued_command_is_consumed_evidence(tmp_path):
    text = "live queued instruction"
    path = _transcript(tmp_path, [
        {"type": "attachment",
         "uuid": "att-live",
         "attachment": {
             "type": "queued_command",
             "prompt": (
                 '<pasted_content id="abcd">\\n'
                 + text
                 + '\\n</pasted_content id="abcd">'
             ),
         }},
    ])
    events = list(steer_evidence_events(path, text))
    assert [e["kind"] for e in events] == ["consumed"]
    assert events[0]["uuid"] == "att-live"


def test_plain_user_event_is_started_not_consumed(tmp_path):
    path = _transcript(tmp_path, [_user("next turn please")])
    events = list(steer_evidence_events(path, "next turn please"))
    assert [e["kind"] for e in events] == ["started"]


def test_evidence_matches_claudes_tab_and_crlf_paste_normalization(tmp_path):
    # Claude expands tabs and normalizes CR/CRLF in every paste it inserts.
    path = _transcript(tmp_path, [
        _enqueue("step:\n    indented\nend"),
        _user("step:\n    indented\nend", queued=True),
    ])
    sent = "step:\r\n\tindented\rend\n"
    kinds = [e["kind"] for e in steer_evidence_events(path, sent)]
    assert kinds == ["enqueued", "consumed"]
    assert queued_prompt_accepted(path, sent)


def test_dequeue_is_started_evidence(tmp_path):
    path = _transcript(tmp_path, [
        {"type": "queue-operation", "operation": "dequeue", "content": "later"},
    ])
    assert [e["kind"] for e in steer_evidence_events(path, "later")] == ["started"]


def test_evidence_is_fenced_by_byte_offset_not_text_containment(tmp_path):
    path = _transcript(tmp_path, [_enqueue("same words")])
    fence = path.stat().st_size
    # Nothing new after the fence: the identical older event must not count.
    assert list(steer_evidence_events(path, "same words", after_offset=fence)) == []
    _append(path, [_user("same words")])
    assert [e["kind"] for e in
            steer_evidence_events(path, "same words", after_offset=fence)] == ["started"]


def test_public_projection_speaks_only_the_placement_vocabulary():
    approved = {SO.PUBLIC_ACCEPTED_PENDING, SO.PUBLIC_CONSUMED,
                SO.PUBLIC_STARTED, SO.PUBLIC_UNKNOWN}
    for internal in SO.ALL_STATUSES:
        assert SO.public_status(internal) in approved
    assert SO.public_status("accepted") == SO.PUBLIC_ACCEPTED_PENDING
    assert SO.public_status("pasted") == SO.PUBLIC_ACCEPTED_PENDING
    assert SO.public_status("enqueued") == SO.PUBLIC_ACCEPTED_PENDING
    assert SO.public_status("consumed") == SO.PUBLIC_CONSUMED
    assert SO.public_status("started") == SO.PUBLIC_STARTED
    assert SO.public_status("unknown") == SO.PUBLIC_UNKNOWN


def test_journal_busy_is_a_typed_refusal():
    import ccx
    from ccx.constants import EXIT_REJECTED

    assert ccx.SteerJournalBusy is SO.SteerJournalBusy
    busy = SO.SteerJournalBusy("journal lock busy", short="abcd1234")
    assert isinstance(busy, ccx.CcxError)
    assert isinstance(busy, SO.SteerJournalError)
    assert busy.exit_code == EXIT_REJECTED
    assert busy.short == "abcd1234"
    assert str(busy) == "journal lock busy"


def test_identical_steers_need_distinct_native_events(short_root, tmp_path, monkeypatch):
    from ccx import constants
    from test_generation_store import A, add_worker

    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    add_worker(short_root, A, "claude")
    transcript = _transcript(tmp_path, [])
    for token in ("first", "second"):
        delivery, created = SO.begin_delivery(
            A, delivery_id=token, text="same text", transcript=str(transcript),
            host_epoch="epoch-1", state="busy", requested_mode="steer_only",
            route="steer", target_turn_ref="ccx-turn:claude:v1:original",
        )
        assert created and delivery["status"] == "accepted"
        SO.mark_pasted(A, token, offset=transcript.stat().st_size)

    _append(transcript, [_enqueue("same text")])
    resolved = SO.reconcile_pending(A, transcript=str(transcript))
    assert [d["delivery_id"] for d in resolved] == ["first"]
    assert SO.get_delivery(A, "first")["status"] == "enqueued"
    assert SO.get_delivery(A, "second")["status"] == "pasted"

    _append(transcript, [_user("same text", queued=True)])
    resolved = SO.reconcile_pending(A, transcript=str(transcript))
    assert [d["delivery_id"] for d in resolved] == ["first"]
    assert SO.get_delivery(A, "first")["status"] == "consumed"
    assert SO.get_delivery(A, "second")["status"] == "pasted"

    # Only a new independent event may resolve the second original delivery.
    _append(transcript, [_user("same text")])
    resolved = SO.reconcile_pending(A, transcript=str(transcript))
    assert [d["delivery_id"] for d in resolved] == ["second"]
    assert SO.get_delivery(A, "second")["status"] == "started"


def test_unproven_steer_goes_unknown_without_repaste_and_accepts_late_evidence(
    short_root, tmp_path, monkeypatch,
):
    from ccx import constants
    from test_generation_store import A, add_worker

    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    add_worker(short_root, A, "claude")
    transcript = _transcript(tmp_path, [])
    entry, created = SO.begin_delivery(
        A, delivery_id="unproven", text="maybe delivered",
        transcript=str(transcript), host_epoch="epoch-1",
        requested_mode="steer_only", route="steer",
    )
    assert created and entry["status"] == "accepted"
    pasted = SO.mark_pasted(A, "unproven", offset=transcript.stat().st_size)
    result = SO.reconcile_pending(
        A, transcript=str(transcript), grace_s=2,
        now=pasted["pasted_at"] / 1000 + 3,
    )
    assert [d["status"] for d in result] == ["unknown"]
    assert SO.get_delivery(A, "unproven")["attempts"] == 1

    _append(transcript, [_user("maybe delivered")])
    result = SO.reconcile_pending(A, transcript=str(transcript))
    assert [d["status"] for d in result] == ["started"]
    assert SO.get_delivery(A, "unproven")["attempts"] == 1
