"""Original full-UUID lifecycle event pages, bounded following and V1 cancellation."""
from __future__ import annotations

import json
import os
import threading
import time

import pytest

from ccx import constants
from ccx.rpc import local_rpc
from ccx.ops import worker_events
from ccx.errors import CursorInvalidated, InvalidRequest, SessionNotFound, SessionUnavailable
from ccx.workers import WorkerClient
from ccx.store.root import StaleGeneration, StorageIntegrityError, require_ccx_active
from test_generation_store import (
    A, B, G1, G2, add_worker, short_root, switch_pointer,
)


def _journal(root, wid=A, engine="claude"):
    return root / "identity" / "generations" / (
        G1 if root.joinpath("identity", "generations", G1, "workers", engine, wid).exists()
        else G2
    ) / "workers" / engine / wid / (
        "events.ndjson" if engine == "claude" else "evidence.jsonl"
    )


def _append(path, *events):
    # Create with mode 0600 on the FIRST open: a racing original cursor read
    # must never inspect a chmod-pending 0644/0664 inode under host umask.
    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags, 0o600)
    with os.fdopen(fd, "a", encoding="utf-8") as stream:
        for event in events:
            stream.write(json.dumps(event, sort_keys=True) + "\n")
    path.chmod(0o600)



def test_fixture_append_starts_as_private_inode_before_chmod(owned, monkeypatch):
    """A concurrent read must never see the writer's pre-chmod 0644 inode."""
    from pathlib import Path
    import stat

    root, _store, _workers = owned
    path = _journal(root)
    assert not path.exists()
    native_chmod = Path.chmod
    checked = []

    def verify_before_chmod(self, mode, *args, **kwargs):
        if self == path:
            observed = stat.S_IMODE(path.stat().st_mode)
            checked.append(observed)
            assert observed == 0o600, (
                "new fake journal became readable before its chmod", oct(observed))
        return native_chmod(self, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "chmod", verify_before_chmod)
    previous_umask = os.umask(0o022)
    try:
        _append(path, _event("fixture-private", 1))
    finally:
        os.umask(previous_umask)
    assert checked == [0o600]

def _event(epoch, seq, kind="state"):
    return {"type": kind, "seq": seq, "epoch": epoch, "ts": 1.0}


def _v1(target, *, wait_ms=0, after=None, timeout_ms=1000, **other):
    body = {
        "protocol_version": 2, "request_id": "uuid-events-f1",
        "op": "worker.events", "timeout_ms": timeout_ms,
        "params": {"target": target, "wait_ms": wait_ms, **other},
    }
    if after is not None:
        body["params"]["after_cursor"] = after
    return local_rpc.dispatch(body)


@pytest.fixture
def owned(short_root, monkeypatch):
    add_worker(short_root, A)
    add_worker(short_root, B, "codex")
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    return short_root, require_ccx_active(short_root), WorkerClient()


def test_owned_original_ev2_pages_rotation_gap_and_source_fences(owned, tmp_path):
    root, store, worker = owned
    path = _journal(root)
    # From-now cursor precedes original event file creation.
    before = worker.events(A, tail=0)
    assert before["events"] == []
    assert before["next_cursor"].startswith("ev2.")
    _append(path, _event("E1", 1), _event("E1", 2),
            _event("E1", 3), _event("E2", 1), _event("E2", 2))
    after = worker.events(A, after=before["next_cursor"], limit=2)
    assert [(ev["epoch"], ev["seq"]) for ev in after["events"]] == [
        ("E1", 1), ("E1", 2),
    ]
    assert after["has_more"]
    good = after["next_cursor"]
    next_page = worker.events(A, after=good, kinds=["state"])
    assert [(ev["epoch"], ev["seq"]) for ev in next_page["events"]] == [
        ("E1", 3), ("E2", 1), ("E2", 2),
    ]
    assert next_page["has_more"] is False
    # Complete same owner bytes moved to another inode: still place cursor.
    copy = path.with_suffix(".new")
    copy.write_bytes(path.read_bytes())
    copy.chmod(0o600)
    os.replace(copy, path)
    same = worker.events(A, after=good)
    assert same["gap"] is None
    assert [(ev["epoch"], ev["seq"]) for ev in same["events"]] == [
        ("E1", 3), ("E2", 1), ("E2", 2),
    ]
    # Head-trim with original E1 position still retained: no gap or replay.
    path.write_text("".join(json.dumps(_event(epoch, seq)) + "\n"
                           for epoch, seq in (("E1", 2), ("E1", 3), ("E2", 1))))
    path.chmod(0o600)
    placed = worker.events(A, after=good)
    assert placed["gap"] is None
    assert [(ev["epoch"], ev["seq"]) for ev in placed["events"]] == [
        ("E1", 3), ("E2", 1),
    ]
    # Rotate original epoch away: SAME owner gives honest gap+retained replay.
    path.write_text(json.dumps(_event("E9", 1)) + "\n")
    path.chmod(0o600)
    rotated = worker.events(A, after=good)
    assert rotated["gap"]["reason"] == "cursor_not_retained"
    assert [ev["epoch"] for ev in rotated["events"]] == ["E9"]
    assert rotated["next_cursor"].startswith("ev2.")
    # Lost epoch with more than one replay page: the caught-up heartbeat
    # still reports the gap from the FIRST replay page.
    path.write_text("".join(json.dumps(_event("E9", seq)) + "\n"
                           for seq in range(1, 1003)))
    path.chmod(0o600)
    replay = list(worker.follow(A, after=good, timeout=0, heartbeat=0))
    *replayed, caught_up = replay
    assert [(ev["epoch"], ev["seq"]) for ev in replayed] == [
        ("E9", seq) for seq in range(1, 1003)
    ]
    assert caught_up["kind"] == "heartbeat"
    assert caught_up["gap"]["reason"] == "cursor_not_retained"
    assert caught_up["cursor"] == worker.events(A, tail=0)["next_cursor"]
    with pytest.raises(CursorInvalidated):
        worker.events(B, after=good)
    with pytest.raises(CursorInvalidated):
        worker.events(A, after="ev1.legacy")

    # A Codex journal projects the never-repeating gateId, not native requestId.
    codex = _journal(root, B, "codex")
    _append(codex, {
        "sequence": 1, "method": "ccx/input/requested", "turnId": "native-one",
        "params": {"request": {"gateId": "original-gate", "requestId": 0,
                               "turnId": "native-one", "kind": "command"}},
    })
    codex_page = worker.events(B)
    assert codex_page["events"][0]["request_id"] == "original-gate"
    assert codex_page["events"][0]["native_request_id"] == 0
    assert codex_page["events"][0]["worker_id"] == B

    # Partial final record never gets a cursor until its newline lands.
    old = worker.events(A, tail=0)["next_cursor"]
    with path.open("a", encoding="utf-8") as stream:
        stream.write('{"epoch":"E9","seq":2,"type":"state"')
    partial = worker.events(A, after=old)
    assert partial["events"] == []
    with path.open("a", encoding="utf-8") as stream:
        stream.write("}\n")
    complete = worker.events(A, after=old)
    assert [ev["seq"] for ev in complete["events"]] == [2]
    switch_pointer(root, G2)
    add_worker(root, A)
    with pytest.raises(SessionUnavailable):
        worker.events(A)
    with pytest.raises(worker_events.EventCursorInvalid):
        worker_events.read_full_uuid_event_page(require_ccx_active(root), A,
                                               after=complete["next_cursor"])


def test_append_during_pinned_page_read_is_not_an_integrity_failure(
    owned, monkeypatch,
):
    root, store, worker = owned
    journal = _journal(root)
    _append(journal, _event("E1", 1), _event("E1", 2))
    cursor = worker.events(A, tail=0)["next_cursor"]
    original = os.pread
    did_append = False
    inode = journal.stat().st_ino

    def concurrent(fd, n, offset):
        nonlocal did_append
        result = original(fd, n, offset)
        if not did_append and os.fstat(fd).st_ino == inode:
            did_append = True
            _append(journal, _event("E1", 3))
        return result

    with monkeypatch.context() as patch:
        patch.setattr(worker_events.os, "pread", concurrent)
        first = worker.events(A, after=cursor)
    assert did_append
    assert [e["seq"] for e in first["events"]] == [3] or first["events"] == []
    second = worker.events(A, after=first["next_cursor"])
    seen = [e["seq"] for e in first["events"] + second["events"]]
    assert seen == [3]
    assert second["gap"] is None


def test_event_feed_large_codex_journal_has_bounded_read_cost(owned, monkeypatch):
    root, store, worker = owned
    path = _journal(root, B, "codex")
    # Do not add rotation to this original evidence journal.
    header = {"method": "ccx/evidence-started", "sequence": 1,
              "params": {"journalInstanceId": "large-evidence"}}
    with path.open("wb") as stream:
        stream.write((json.dumps(header) + "\n").encode())
        for i in range(2, 108002):
            stream.write((json.dumps({
                "method": "ccx/lifecycle", "sequence": i,
                "params": {"from": "idle", "to": "busy"}, "padding": "q" * 36,
            }) + "\n").encode())
    path.chmod(0o600)
    assert path.stat().st_size > 8 * 1024 * 1024
    bytes_read = []
    original = os.pread

    def measured(fd, n, offset):
        value = original(fd, n, offset)
        bytes_read.append(len(value))
        return value

    with monkeypatch.context() as patch:
        patch.setattr(worker_events.os, "pread", measured)
        newest = worker.events(B, tail=1)
    assert [e["seq"] for e in newest["events"]] == [108001]
    assert sum(bytes_read) < worker_events.MAX_PAGE_BYTES
    bytes_read.clear()
    with monkeypatch.context() as patch:
        patch.setattr(worker_events.os, "pread", measured)
        first = worker.events(B, limit=20)
    assert [e["seq"] for e in first["events"]] == list(range(2,22))
    assert first["has_more"]
    assert sum(bytes_read) <= worker_events.MAX_PAGE_BYTES + 100_000
    continuation = worker.events(B, after=first["next_cursor"], limit=20)
    assert [e["seq"] for e in continuation["events"]] == list(range(22,42))


def test_filtered_claude_tail_scans_past_two_megabytes_for_retained_matches(owned):
    root, _store, worker = owned
    path = _journal(root)
    with path.open("wb") as stream:
        for seq in (1, 2):
            stream.write((json.dumps(_event("E1", seq, "permission_requested")) + "\n").encode())
        for seq in range(3, 3003):
            stream.write((json.dumps({**_event("E1", seq), "padding": "x" * 1050}) + "\n").encode())
    path.chmod(0o600)
    assert path.stat().st_size > worker_events.MAX_PAGE_BYTES

    page = worker.events(A, tail=2, kinds=["input.requested"])
    assert [(e["seq"], e["kind"]) for e in page["events"]] == [
        (1, "input.requested"), (2, "input.requested")]
    assert page["has_more"] is False and page["gap"] is None


def test_unbounded_codex_filtered_tail_exposes_forward_replay_when_incomplete(owned):
    root, _store, worker = owned
    path = _journal(root, B, "codex")
    with path.open("wb") as stream:
        stream.write((json.dumps({"method": "ccx/evidence-started", "sequence": 1,
                                  "params": {"journalInstanceId": "original-evidence"}}) + "\n").encode())
        stream.write((json.dumps({"method": "ccx/input/requested", "sequence": 2,
                                  "params": {"request": {"gateId": "old-gate", "kind": "command"}}})
                      + "\n").encode())
        for seq in range(3, 3003):
            stream.write((json.dumps({"method": "ccx/lifecycle", "sequence": seq,
                                      "params": {"from": "idle", "to": "busy"},
                                      "padding": "q" * 1050}) + "\n").encode())
    path.chmod(0o600)
    assert path.stat().st_size > worker_events.MAX_PAGE_BYTES

    page = worker.events(B, tail=1, kinds=["input.requested"])
    assert page["events"] == []
    assert page["has_more"] is True and page["gap"] is None
    # The opaque continuation must actually reach unscanned retained bytes;
    # it cannot simply point at EOF while promising more.
    older = worker.events(B, after=page["next_cursor"], kinds=["input.requested"], limit=1)
    assert [(e["seq"], e["request_id"]) for e in older["events"]] == [(2, "old-gate")]


def test_short_tail_follow_does_not_replay_history_from_the_head(owned):
    root, _store, worker = owned
    path = _journal(root, B, "codex")
    with path.open("wb") as stream:
        stream.write((json.dumps({"method": "ccx/evidence-started", "sequence": 1,
                                  "params": {"journalInstanceId": "original-evidence"}}) + "\n").encode())
        stream.write((json.dumps({"method": "ccx/input/requested", "sequence": 2,
                                  "params": {"request": {"gateId": "old-gate", "kind": "command"}}})
                      + "\n").encode())
        for seq in range(3, 3003):
            stream.write((json.dumps({"method": "ccx/lifecycle", "sequence": seq,
                                      "params": {"from": "idle", "to": "busy"},
                                      "padding": "q" * 1050}) + "\n").encode())
    path.chmod(0o600)
    items = list(worker.follow(B, tail=1, kinds=["input.requested"], timeout=0))
    assert [item["kind"] for item in items] == ["heartbeat"]


def test_short_tail_follow_keeps_events_appended_after_its_first_yield(owned):
    root, _store, worker = owned
    path = _journal(root, B, "codex")

    def gate(seq, name):
        return (json.dumps({"method": "ccx/input/requested", "sequence": seq,
                            "params": {"request": {"gateId": name, "kind": "command"}}})
                + "\n")
    with path.open("w") as stream:
        stream.write(json.dumps({"method": "ccx/evidence-started", "sequence": 1,
                                 "params": {"journalInstanceId": "original-evidence"}}) + "\n")
        for seq in range(2, 3003):
            stream.write(json.dumps({"method": "ccx/lifecycle", "sequence": seq,
                                     "params": {"from": "idle", "to": "busy"},
                                     "padding": "q" * 1050}) + "\n")
        stream.write(gate(3003, "initial"))
    path.chmod(0o600)
    follow = worker.follow(B, tail=2, kinds=["input.requested"], timeout=0)
    assert next(follow)["request_id"] == "initial"
    with path.open("a") as stream:
        stream.write(gate(3004, "arrived-after-yield"))
    rest = list(follow)
    assert [item["kind"] for item in rest] == ["heartbeat"]
    after = worker.events(B, after=rest[-1]["cursor"], kinds=["input.requested"])
    assert [event["request_id"] for event in after["events"]] == ["arrived-after-yield"]


@pytest.mark.parametrize("engine,worker_id,previous_seq,new_seq", [
    ("claude", A, 1, 2), ("codex", B, 2, 3),
])
def test_long_unfinished_event_line_does_not_block_tail_or_from_now(
    owned, engine, worker_id, previous_seq, new_seq,
):
    root, _store, workers = owned
    path = _journal(root, worker_id, engine)
    if engine == "claude":
        preceding = [_event("E1", previous_seq)]
        next_event = {**_event("E1", new_seq), "padding": "x" * 75000}
    else:
        preceding = [
            {"method": "ccx/evidence-started", "sequence": 1,
             "params": {"journalInstanceId": "long-line-test"}},
            {"method": "ccx/lifecycle", "sequence": previous_seq,
             "params": {"from": "idle", "to": "busy"}},
        ]
        next_event = {"method": "ccx/lifecycle", "sequence": new_seq,
                      "params": {"from": "busy", "to": "idle"}, "padding": "x" * 75000}
    with path.open("wb") as stream:
        for event in preceding:
            stream.write((json.dumps(event) + "\n").encode())
        stream.write(json.dumps(next_event)[:-1].encode())  # no closing brace/newline
    path.chmod(0o600)

    from_now = workers.events(worker_id, tail=0)
    assert from_now["events"] == []
    tail = workers.events(worker_id, tail=1, kinds=["state"])
    assert [event["seq"] for event in tail["events"]] == [previous_seq]
    assert tail["gap"] is None and tail["has_more"] is False

    with path.open("ab") as stream:
        stream.write(b"}\n")
    completed = workers.events(worker_id, after=from_now["next_cursor"])
    assert [event["seq"] for event in completed["events"]] == [new_seq]
    assert completed["gap"] is None


def test_wait_and_follow_original_stopped_owner_with_no_extra_hashing(owned, monkeypatch):
    root, store, worker = owned
    path = _journal(root)
    initial = worker.events(A, tail=0)["next_cursor"]
    reads = []
    real_pread = os.pread

    def counted(fd, n, offset):
        value = real_pread(fd, n, offset)
        reads.append(len(value))
        return value

    monkeypatch.setattr(worker_events.os, "pread", counted)
    timed = worker.events(A, after=initial, wait=.5)
    assert timed["events"] == [] and timed["timed_out"] is True
    assert sum(reads) < 8192, "quiet wait must not parse journal history"
    append = threading.Timer(.15, lambda: _append(path, _event("E1", 1)))
    append.start()
    found = worker.events(A, after=timed["next_cursor"], wait=2)
    append.join()
    assert found["timed_out"] is False
    assert [(ev["epoch"], ev["seq"]) for ev in found["events"]] == [("E1", 1)]

    store.mutate_record(A, patches={"lifecycle.state": "dead"})
    append2 = threading.Timer(.15, lambda: _append(path, _event("E1", 2)))
    append2.start()
    output = list(worker.follow(A, timeout=.6, heartbeat=.12))
    append2.join()
    assert output[0]["kind"] == "state"
    assert output[1]["kind"] == "heartbeat"
    assert output[1]["cursor"] == output[0]["cursor"]
    assert output[1]["state"] == "stopped"
    assert ("E1", 2) in [(e.get("epoch"), e.get("seq")) for e in output]
    assert len([e for e in output if e["kind"] == "heartbeat"]) >= 2

    with pytest.raises(InvalidRequest):
        worker.events(A, wait=-1)
    with pytest.raises(InvalidRequest):
        next(worker.follow(A, heartbeat=-1))
    following = worker.follow(A, tail=0, timeout=1.5, heartbeat=.1)
    assert next(following)["kind"] == "heartbeat"
    (root / "identity" / "generations" / G1 / "owners" / (A + ".json")).unlink()
    with pytest.raises(SessionNotFound):
        next(following)

    original_codex_follow = worker.follow(B, tail=0, timeout=1.5, heartbeat=.1)
    assert next(original_codex_follow)["kind"] == "heartbeat"
    switch_pointer(root, G2)
    with pytest.raises(SessionUnavailable):
        next(original_codex_follow)


def test_v1_wait_budget_default_page_size_and_real_serve_cancellation(owned):
    from test_rpc_worker_serving import _serve_lines, _line

    root, store, worker = owned
    path = _journal(root)
    _append(path, *(_event("E1", n) for n in range(1, 212)))
    page = _v1(A)
    assert page["ok"] is True, page
    assert len(page["result"]["events"]) == 200
    assert page["result"]["has_more"] is True

    cursor = worker.events(A, tail=0)["next_cursor"]
    start = time.monotonic()
    held = _v1(A, wait_ms=90, after=cursor, timeout_ms=400)
    assert held["ok"] is True
    assert held["result"]["events"] == []
    assert held["result"]["timed_out"] is True
    assert time.monotonic() - start < 1

    # Actual accepted serve transport cancels only the held observational
    # request; no native worker action is attempted.
    responses = _serve_lines(
        _line("held", "worker.events", target=A, after_cursor=cursor,
              wait_ms=30000)
        + _line("drop", "request.cancel", cancel_request_id="held")
    )
    assert len(responses) == 2
    assert responses[0]["request_id"] == "held"
    assert responses[0]["error"]["code"] == "cancelled"
    assert responses[1]["request_id"] == "drop"
    assert responses[1]["result"]["cancelled"] is True
    assert responses[1]["result"]["cancel_request_id"] == "held"
    assert not (root / "sessions").exists()
    assert not (root / "run").exists()


# Typed original Claude event source and normalization; keep its distinct journal fixture.
from ccx.client import CcxClient
from ccx.types import Event
from test_generation_store import C
def _typed_journal(root):
    return root / "identity" / "generations" / G1 / "workers" / "claude" / A / "events.ndjson"


def _typed_event(epoch: str, seq: int, kind: str = "state", **extra):
    return {"epoch": epoch, "seq": seq, "type": kind, "short": "original-host",
            "v": 3, "ts": 1792500000.25, **extra}


def _append_typed(path, *rows):
    with path.open("a", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(row, sort_keys=True) + "\n")
    path.chmod(0o600)


@pytest.fixture
def typed_owned(short_root, monkeypatch):
    add_worker(short_root, A, "claude")
    add_worker(short_root, B, "codex")
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    return short_root, CcxClient()


def test_claude_typed_events_return_original_raw_across_pages_and_epochs(typed_owned):
    root, client = typed_owned
    assert client.events(A) == []  # no original host journal yet; do not create one
    path = _typed_journal(root)
    native = [_typed_event("E1", seq, v_original=seq) for seq in range(1, 1005)]
    unknown = _typed_event("E1", 1005, "unknown_host_diagnostic",
                     nested={"important": ["unchanged", {"a": 9}]},
                     worker="native-not-ccx")
    unknown["kind"] = "heartbeat"
    native.extend([unknown, _typed_event("E2", 1), _typed_event("E2", 2, "turn_complete")])
    _append_typed(path, *native)
    with path.open("a", encoding="utf-8") as stream:
        stream.write("{broken\n")
        stream.write('{"epoch":"E2","seq":3,"type":"partial"')
    full = client.events(A)
    assert len(full) == len(native)
    assert all(isinstance(item, Event) for item in full)
    assert [item.as_dict() for item in full] == native
    assert all(item.raw == expected for item, expected in zip(full, native))
    assert full[-3].type == "unknown_host_diagnostic"
    assert full[-3].cursor == ("E1", 1005)
    assert full[-3].raw["worker"] == "native-not-ccx"
    assert "worker_id" not in full[-3].raw
    assert "cursor" not in full[-3].raw

    # Typed native reads include every source object, without changing F1 default.
    original_page = worker_events.read_full_uuid_event_page(
        client._uuid_store, A, native=True, tail=3, kinds=["state"],
    )
    assert original_page["events"] == native[-3:]
    normalized = worker_events.read_full_uuid_event_page(
        client._uuid_store, A, tail=1,
    )
    assert normalized["events"][0]["worker_id"] == A
    assert normalized["events"][0]["kind"] == "turn.ended"

    filtered = client.events(A, since_epoch="E1", since_seq=1003)
    assert [(event.epoch, event.seq) for event in filtered] == [
        ("E1", 1004), ("E1", 1005), ("E2", 1), ("E2", 2),
    ]
    assert [event.cursor for event in client.events(
        A, since_epoch="E1", since_seq=1003, limit=2,
    )] == [("E2", 1), ("E2", 2)]
    assert [event.cursor for event in client.events(A, since_seq=1003)] == [
        ("E1", 1004), ("E1", 1005),
    ]
    assert len(client.events(A, limit=0)) == len(native)  # shipped main edge case


def test_claude_subscribe_watermark_before_replay_dedups_overlapping_epoch(
    typed_owned, monkeypatch,
):
    root, client = typed_owned
    path = _typed_journal(root)
    originals = [_typed_event("E1", 1), _typed_event("E2", 1)]
    repeated = _typed_event("E2", 2, "custom_host_progress", payload={"id": "only-native"})
    later = _typed_event("E3", 1, "custom_host_progress", payload={"new": "epoch"})
    later["kind"] = "heartbeat"  # Native custom field, NOT a synthetic F1 heartbeat.
    _append_typed(path, *originals)
    client._uuid_store.mutate_record(A, patches={"lifecycle.state": "dead"})

    # The first native page watermark is already captured when events() begins.
    # Insert E2 seq2 into historical replay and subsequently the live iterator.
    real_events = client.events
    appended = []

    def replay(*args, **kwargs):
        if not appended:
            _append_typed(path, repeated)
            appended.append(True)
        return real_events(*args, **kwargs)

    monkeypatch.setattr(client, "events", replay)
    timer = threading.Timer(.15, lambda: _append_typed(path, later))
    timer.start()
    try:
        got = list(client.subscribe(A, timeout=.6))
    finally:
        timer.join(2)
    assert [item.cursor for item in got] == [
        ("E1", 1), ("E2", 1), ("E2", 2), ("E3", 1),
    ]
    assert [item.raw for item in got] == originals + [repeated, later]
    assert all(item.type != "heartbeat" for item in got)
    assert [item.raw["payload"] for item in got[2:]] == [
        {"id": "only-native"}, {"new": "epoch"},
    ]
    assert got[-1].raw["kind"] == "heartbeat"  # Original raw event survived follow.
    # Real native object with a caller-defined heartbeat-like kind is not the
    # follower's synthetic resume cursor/heartbeat and must not be discarded.
    native_heartbeat_kind = _typed_event("E3", 2, "unknown_event")
    native_heartbeat_kind["kind"] = "heartbeat"
    _append_typed(path, native_heartbeat_kind)
    after = client.events(A, since_epoch="E3", since_seq=1)
    assert len(after) == 1 and after[0].raw["kind"] == "heartbeat"


@pytest.mark.parametrize("change", ("removed", "generation"))
def test_claude_typed_original_owner_and_active_generation_are_fenced(typed_owned, change):
    root, client = typed_owned
    with pytest.raises(InvalidRequest):
        client.events(B)   # exact globally-typed_owned but not Claude
    with pytest.raises(InvalidRequest):
        client.events(A[:8])
    with pytest.raises(InvalidRequest):
        client.events("display-name")
    with pytest.raises(SessionNotFound):
        client.events(C)
    with pytest.raises(InvalidRequest):
        next(client.subscribe(B, timeout=.1))

    first = _typed_event("E1", 1)
    _append_typed(_typed_journal(root), first)
    stream = client.subscribe(A, timeout=.6)
    assert next(stream).raw == first
    if change == "removed":
        (root / "identity" / "generations" / G1 / "owners" / (A + ".json")).unlink()
        with pytest.raises(SessionNotFound):
            next(stream)
    else:
        switch_pointer(root, G2)
        add_worker(root, A, "claude")  # same canonical worker ID, another ACTIVE
        with pytest.raises(SessionUnavailable):
            next(stream)
    assert not (root / "sessions").exists()
    assert not (root / "run").exists()


# Generation-bound event pages and CLI no-effect reading.
from pathlib import Path
from test_generation_store import fixture_root, put, privdir, record, spec
def _write_worker_event_journal(root: Path, engine: str, worker_id: str,
                                values: list[dict], *, append: bool = False) -> Path:
    path = (root / "identity" / "generations" / G1 / "workers"
            / engine / worker_id
            / ("events.ndjson" if engine == "claude" else "evidence.jsonl"))
    with path.open("a" if append else "w", encoding="utf-8") as out:
        for value in values:
            out.write(json.dumps(value, sort_keys=True) + "\n")
    path.chmod(0o600)
    return path


def test_uuid_events_both_engines_same_prefix_pages_and_exact_cursors(tmp_path):
    from ccx.ops.worker_events import read_full_uuid_event_page
    root = fixture_root(tmp_path)
    add_worker(root, A, "claude")
    add_worker(root, B, "codex")
    ctx = require_ccx_active(root)
    _write_worker_event_journal(root, "claude", A, [
        {"type": "state", "seq": 1, "epoch": "claude-epoch", "ts": 1},
        {"type": "turn_start", "seq": 2, "epoch": "claude-epoch", "ts": 2},
        {"type": "turn_complete", "seq": 3, "epoch": "claude-epoch", "ts": 3,
         "outcome": "completed"},
    ])
    _write_worker_event_journal(root, "codex", B, [
        {"method": "turn/started", "sequence": 1,
         "turnId": "codex-native-turn-1", "params": {}},
        {"method": "turn/completed", "sequence": 2,
         "turnId": "codex-native-turn-1",
         "params": {"turn": {"status": "completed"}}},
    ])
    claude = read_full_uuid_event_page(ctx, f"claude:{A}", limit=2)
    assert claude["worker"] == f"claude:{A}"
    assert len(claude["events"]) == 2 and claude["has_more"]
    assert {event["worker_id"] for event in claude["events"]} == {A}
    assert claude["events"][0]["kind"] == "state"
    assert claude["events"][1]["kind"] == "turn.started"
    assert all(event["cursor"].startswith("ev2.") for event in claude["events"])
    continuation = read_full_uuid_event_page(ctx, A, after=claude["next_cursor"])
    assert len(continuation["events"]) == 1
    assert continuation["events"][0]["kind"] == "turn.ended"
    assert not continuation["has_more"]
    codex = read_full_uuid_event_page(ctx, B)
    assert codex["worker"] == f"codex:{B}"
    assert [e["kind"] for e in codex["events"]] == [
        "turn.started", "turn.ended",
    ]
    assert codex["events"][-1]["outcome"] == "completed"
    assert codex["events"][-1]["worker_id"] == B
    assert claude["short"] == codex["short"] == "aaaaaaaa"


def test_uuid_events_cursors_bind_full_owner_generation_and_source_prefix(tmp_path):
    from ccx.ops.worker_events import EventCursorInvalid, read_full_uuid_event_page
    root = fixture_root(tmp_path)
    add_worker(root, A, "claude")
    add_worker(root, B, "codex")
    ctx = require_ccx_active(root)
    _write_worker_event_journal(root, "claude", A, [
        {"type": "state", "seq": 1, "epoch": "e", "ts": 1},
    ])
    page = read_full_uuid_event_page(ctx, A)
    after = page["next_cursor"]
    assert after.startswith("ev2.")
    with pytest.raises(EventCursorInvalid):
        read_full_uuid_event_page(ctx, B, after=after)
    with pytest.raises(EventCursorInvalid):
        read_full_uuid_event_page(ctx, A, after="ev1.legacy-short-cursor")
    with pytest.raises(EventCursorInvalid):
        read_full_uuid_event_page(ctx, A, after="ev2.!!!")
    _write_worker_event_journal(root, "claude", A, [
        {"type": "turn_start", "seq": 2, "epoch": "e", "ts": 2},
    ], append=True)
    newly = read_full_uuid_event_page(ctx, A, after=after)
    assert [x["seq"] for x in newly["events"]] == [2]
    switch_pointer(root)
    with pytest.raises(StaleGeneration):
        read_full_uuid_event_page(ctx, A, after=after)


def test_uuid_events_empty_source_cursor_from_now_survives_first_append(tmp_path):
    from ccx.ops.worker_events import read_full_uuid_event_page
    root = fixture_root(tmp_path)
    add_worker(root, A)
    ctx = require_ccx_active(root)
    since = read_full_uuid_event_page(ctx, A, tail=0)
    assert since["events"] == []
    assert since["next_cursor"].startswith("ev2.")
    _write_worker_event_journal(root, "claude", A, [
        {"type": "state", "epoch": "first", "seq": 1, "ts": 1},
    ])
    page = read_full_uuid_event_page(ctx, A, after=since["next_cursor"])
    assert len(page["events"]) == 1
    assert page["events"][0]["seq"] == 1
    assert read_full_uuid_event_page(ctx, A, tail=0)["events"] == []


def test_uuid_events_reject_symlinked_journal_and_invalid_page_params(tmp_path):
    from ccx.ops.worker_events import read_full_uuid_event_page, EventCursorInvalid
    from ccx.errors import InvalidRequest
    root = fixture_root(tmp_path)
    add_worker(root, A)
    ctx = require_ccx_active(root)
    orig = root / "identity" / "generations" / G1 / "workers" / "claude" / A
    outside = tmp_path / "provider-owned"
    outside.write_text('{"type":"state","seq":1,"epoch":"forged"}\n')
    journal = orig / "events.ndjson"
    journal.symlink_to(outside)
    with pytest.raises(StorageIntegrityError, match="unsafe"):
        read_full_uuid_event_page(ctx, A)
    journal.unlink()
    for kwargs in ({"limit": 0}, {"limit": 1001}, {"tail": -1},
                   {"tail": 1001}, {"after": "ev2.bad", "tail": 0},
                   {"kinds": ["not-a-kind"]}):
        with pytest.raises(InvalidRequest):
            read_full_uuid_event_page(ctx, A, **kwargs)
    assert outside.read_text().startswith('{"type":"state"')


def test_uuid_events_real_CLI_both_engines_are_read_only(tmp_path):
    import subprocess, sys
    root = fixture_root(tmp_path)
    add_worker(root, A)
    add_worker(root, B, "codex")
    _write_worker_event_journal(root, "claude", A, [
        {"type": "state", "epoch": "e", "seq": 1, "ts": 1},
        {"type": "turn_start", "epoch": "e", "seq": 2, "ts": 2},
    ])
    _write_worker_event_journal(root, "codex", B, [
        {"method": "turn/started", "sequence": 1, "turnId": "t1"},
    ])
    before = sorted((str(p.relative_to(root)), p.stat().st_mtime_ns)
                    for p in root.rglob("*") if p.is_file())
    env = {**os.environ, "CCX_ROOT": str(root),
           "PYTHONPATH": str(Path(__file__).parent.parent)}
    def cmd(*parts):
        return subprocess.run(
            [sys.executable, "-B", "-m", "ccx", *parts],
            env=env, capture_output=True, text=True, timeout=20,
        )
    page = cmd("events", f"claude:{A}", "--json")
    assert page.returncode == 0, page.stderr
    first = json.loads(page.stdout)
    assert first["worker"] == f"claude:{A}"
    assert [x["kind"] for x in first["events"]] == ["state", "turn.started"]
    nxt = cmd("events", A, "--after", first["events"][0]["cursor"], "--json")
    assert nxt.returncode == 0, nxt.stderr
    assert [x["kind"] for x in json.loads(nxt.stdout)["events"]] == ["turn.started"]
    codex = cmd("events", f"codex:{B}", "--json")
    assert codex.returncode == 0, codex.stderr
    assert json.loads(codex.stdout)["worker"] == f"codex:{B}"
    assert json.loads(codex.stdout)["events"][0]["kind"] == "turn.started"
    assert sorted((str(p.relative_to(root)), p.stat().st_mtime_ns)
                  for p in root.rglob("*") if p.is_file()) == before
    assert not (root / "sessions").exists() and not (root / "run").exists()
    # --follow is now a real indefinite read; never launch it as an invalid CLI.
    for tail in (("events", A, "--timeout", "1"),
                 ("events", "aaaaaaaa"),
                 ("events", f"codex:{B}", "--after", "ev1.abc")):
        bad = cmd(*tail)
        assert bad.returncode != 0


@pytest.mark.parametrize("engine,worker_id", (("claude", A), ("codex", B)))
def test_append_only_page_tail_wait_and_follow_during_native_writes(
    owned, engine, worker_id,
):
    root, store, workers = owned
    path = _journal(root, worker_id, engine)
    if engine == "codex":
        _append(path, {
            "method": "ccx/evidence-started", "sequence": 1,
            "params": {"journalInstanceId": "test-parallel-epoch"},
        })

    def row(seq):
        if engine == "claude":
            return {"type": "state", "epoch": "writer-1", "seq": seq,
                    "from": "idle", "to": "busy", "ts": seq}
        return {"method": "ccx/lifecycle", "sequence": seq+1,
                "observedAtMs": seq * 1000,
                "params": {"from": "idle", "to": "busy"}}

    from_now = workers.events(worker_id, tail=0)["next_cursor"]
    errors = []

    def writer(start, end):
        try:
            for n in range(start, end+1):
                _append(path, row(n))
                time.sleep(.004)
        except Exception as exc:
            errors.append(type(exc).__name__)

    thread = threading.Thread(target=writer, args=(1,45))
    thread.start()
    cursor = from_now
    all_events = []
    seen = set()
    deadline = time.monotonic()+5
    while thread.is_alive() or time.monotonic() < deadline and len(seen) < 45:
        page = workers.events(worker_id, after=cursor, limit=8, wait=.1)
        cursor = page["next_cursor"]
        assert page["gap"] is None
        for ev in page["events"]:
            seq = ev["seq"] if engine == "claude" else ev["seq"]-1
            assert seq not in seen
            seen.add(seq)
            all_events.append(seq)
        # A concurrent reverse tail uses the same pinned-inode safeguards.
        tail = workers.events(worker_id, tail=3)
        assert len(tail["events"]) <= 3
        if len(seen) == 45 and not thread.is_alive():
            break
    thread.join(timeout=3)
    assert not errors
    assert all_events == list(range(1,46))
    # Follow the same public source while another writer appends new rows.
    follow_from = cursor
    second = threading.Thread(target=writer, args=(46,65))
    second.start()
    streamed = list(workers.follow(
        worker_id, after=follow_from, timeout=1.2, heartbeat=.1,
    ))
    second.join(timeout=3)
    delivered = [ev["seq"] if engine == "claude" else ev["seq"]-1
                 for ev in streamed if ev.get("kind") == "state"]
    assert delivered == list(range(46,66))
    assert not errors


def test_neutral_compact_outcomes_python_follow_and_v1(owned):
    root, store, workers = owned
    claude = _journal(root)
    _append(claude,
            {**_event("compact-host", 1, "compact_started"),
             "operation_id": "op-a"},
            {**_event("compact-host", 2, "compact_failed"),
             "operation_id": "op-a"})
    codex = _journal(root, B, "codex")
    _append(codex,
            {"method": "ccx/evidence-started", "sequence": 1,
             "params": {"journalInstanceId": "compact-journal"}},
            {"method": "item/started", "sequence": 2,
             "turnId": "native-a", "itemId": "item-a",
             "params": {"turnId": "native-a",
                        "item": {"id": "item-a", "type": "contextCompaction"}}},
            {"method": "item/completed", "sequence": 3,
             "turnId": "native-a", "itemId": "item-a",
             "params": {"turnId": "native-a",
                        "item": {"id": "item-a", "type": "contextCompaction"}}},
            {"method": "ccx/compact/started", "sequence": 4,
             "turnId": "native-a",
             "params": {"turnId": "native-a", "compactId": "native-a:item-a",
                        "itemId": "item-a"}},
            {"method": "ccx/compact/ended", "sequence": 5,
             "turnId": "native-a",
             "params": {"turnId": "native-a", "compactId": "native-a:item-a",
                        "itemId": "item-a", "outcome": "completed"}},
            {"method": "ccx/compact/started", "sequence": 6,
             "turnId": "native-b",
             "params": {"turnId": "native-b", "compactId": "operation:op-b",
                        "operationId": "op-b"}},
            {"method": "ccx/compact/ended", "sequence": 7,
             "turnId": "native-b",
             "params": {"turnId": "native-b", "compactId": "operation:op-b",
                        "operationId": "op-b", "outcome": "failed"}})
    for target, outcomes in ((A, ["failed"]), (B, ["completed", "failed"])):
        feed = workers.events(
            target, kinds=["compact.started", "compact.ended"],
        )
        started = [e for e in feed["events"] if e["kind"] == "compact.started"]
        ended = [e for e in feed["events"] if e["kind"] == "compact.ended"]
        assert len(started) == len(ended) == len(outcomes)
        assert [e["outcome"] for e in ended] == outcomes
        follow = list(workers.follow(
            target, kinds=["compact.started", "compact.ended"],
            timeout=0, heartbeat=0,
        ))
        assert [e["outcome"] for e in follow
                if e.get("kind") == "compact.ended"] == outcomes
        json_response = _v1(
            target, kinds=["compact.started", "compact.ended"],
        )
        assert json_response["ok"] is True
        assert [e["outcome"] for e in json_response["result"]["events"]
                if e["kind"] == "compact.ended"] == outcomes
    from ccx.client import CcxClient
    # CcxClient.events is an original raw Claude event reader.
    original = CcxClient().events(A)
    assert original and all(
        "outcome" not in ev.as_dict() for ev in original
        if ev.type in {"compact_started", "compact_failed"}
    )


def test_codex_compact_pair_is_stable_across_cursor_pages_duplicates_and_tail(owned):
    root, _store, worker = owned
    journal = _journal(root, B, "codex")

    def native(method, seq, turn="t-c", key="item-c"):
        return {
            "method": method, "sequence": seq, "turnId": turn,
            "itemId": key,
            "params": {"turnId": turn,
                       "item": {"id": key, "type": "contextCompaction"}},
        }

    def sparse(method, seq, turn, compact_id, outcome=None):
        params = {"turnId": turn, "compactId": compact_id}
        if outcome is not None:
            params["outcome"] = outcome
        return {"method": method, "sequence": seq,
                "turnId": turn, "params": params}

    _append(
        journal,
        {"method": "ccx/evidence-started", "sequence": 1,
         "params": {"journalInstanceId": "stable-compact-journal"}},
        native("item/started", 2),
        native("item/started", 3),  # duplicate native, not a neutral event
        sparse("ccx/compact/started", 4, "t-c", "t-c:item-c"),
        native("item/completed", 5),
        native("item/completed", 6),  # duplicate native, not a neutral end
        native("item/completed", 7, "foreign", "foreign-item"),
        sparse("ccx/compact/ended", 8, "t-c", "t-c:item-c", "completed"),
        sparse("ccx/compact/started", 9, "t-d", "operation:op-d"),
        sparse("ccx/compact/ended", 10, "t-d", "operation:op-d", "failed"),
    )
    all_events = []
    cursor = None
    for _ in range(16):
        page = worker.events(
            B, after=cursor, limit=1, kinds=["compact.started", "compact.ended"],
        )
        assert page["gap"] is None
        all_events += page["events"]
        cursor = page["next_cursor"]
        if not page["has_more"]:
            break
    assert [(e["kind"], e.get("outcome")) for e in all_events] == [
        ("compact.started", None), ("compact.ended", "completed"),
        ("compact.started", None), ("compact.ended", "failed"),
    ]
    assert [e["compact_id"] for e in all_events] == [
        "t-c:item-c", "t-c:item-c", "operation:op-d", "operation:op-d",
    ]
    tail = worker.events(
        B, tail=2, kinds=["compact.started", "compact.ended"],
    )
    assert [e["seq"] for e in tail["events"]] == [9, 10]
    assert worker.events(B, after=cursor)["events"] == []
    # A native viewer still receives unchanged duplicate and foreign items.
    native_page = worker.events(B)  # neutral: no native contextCompaction events
    assert not any(e.get("type") in ("item/started", "item/completed")
                   for e in native_page["events"])


def test_codex_compact_terminal_visible_beyond_reader_lookbehind(owned):
    """Sparse compact events are projected without a start in the same window."""
    _root, _store, worker = owned
    journal = _journal(_root, B, "codex")
    header = {"method": "ccx/evidence-started", "sequence": 1,
              "params": {"journalInstanceId": "long-compact-journal"}}
    initial = {"method": "ccx/compact/started", "sequence": 2,
               "turnId": "very-long-compact",
               "params": {"turnId": "very-long-compact",
                          "compactId": "very-long-compact:item"}}
    terminal = {"method": "ccx/compact/ended", "sequence": 9,
                "turnId": "very-long-compact",
                "params": {"turnId": "very-long-compact",
                           "compactId": "very-long-compact:item",
                           "outcome": "completed"}}
    # More than the reader's entire 2 MiB per-page window between the pair.
    # The old reader silently dropped the end because its bounded look-behind
    # could no longer see the native start.
    with journal.open("wb") as stream:
        for row in (header, initial):
            stream.write((json.dumps(row) + "\n").encode())
        for seq in range(3, 9):
            stream.write((json.dumps({
                "method": "thread/tokenUsage/updated", "sequence": seq,
                "params": {}, "padding": "x" * 420_000,
            }) + "\n").encode())
        stream.write((json.dumps(terminal) + "\n").encode())
    journal.chmod(0o600)
    assert journal.stat().st_size > 2 * 1024 * 1024

    kinds = ["compact.started", "compact.ended"]
    cursor = None
    observed = []
    for _ in range(15):
        page = worker.events(B, after=cursor, kinds=kinds, limit=1)
        assert page["gap"] is None
        observed.extend(page["events"])
        cursor = page["next_cursor"]
        if not page["has_more"]:
            break
    assert [(e["kind"], e.get("outcome")) for e in observed] == [
        ("compact.started", None), ("compact.ended", "completed"),
    ]
    assert observed[0]["compact_id"] == observed[1]["compact_id"]
    after_start = worker.events(B, after=observed[0]["cursor"], kinds=kinds)
    # A filtered first forward page can be empty but has_more while it
    # advances over unrelated source bytes. The compact end must be visible
    # through ordinary cursor pagination.
    after = after_start
    for _ in range(12):
        if after["events"]:
            break
        assert after["has_more"]
        after = worker.events(B, after=after["next_cursor"], kinds=kinds)
    assert [e["outcome"] for e in after["events"]] == ["completed"]
    tail = worker.events(B, tail=1, kinds=kinds)
    assert [(e["kind"], e["outcome"]) for e in tail["events"]] == [
        ("compact.ended", "completed"),
    ]


@pytest.mark.parametrize("mutation", ("truncate", "replace"))
def test_pinned_event_source_rejects_in_read_inode_replacement_or_truncation(
    owned, monkeypatch, mutation,
):
    root, store, worker = owned
    path = _journal(root)
    _append(path, _event("stable", 1))
    cursor = worker.events(A, tail=0)["next_cursor"]
    _append(path, _event("stable", 2))
    inode = path.stat().st_ino
    original = os.pread
    mutated = False

    def overwrite_during_read(fd, n, at):
        nonlocal mutated
        data = original(fd, n, at)
        if (not mutated and os.fstat(fd).st_ino == inode
                and at == path.stat().st_size - len(data)
                and n > 4):
            mutated = True
            if mutation == "truncate":
                path.write_bytes(b"")
            else:
                replacement = path.with_suffix(".replacement")
                replacement.write_bytes(path.read_bytes())
                replacement.chmod(0o600)
                os.replace(replacement, path)
        return data

    with monkeypatch.context() as patch:
        patch.setattr(worker_events.os, "pread", overwrite_during_read)
        with pytest.raises(CursorInvalidated):
            worker.events(A, after=cursor)
    assert mutated


def test_old_codex_native_compaction_evidence_is_not_neutral_backfill(owned):
    """No reader shim: sparse CCX lines are the entire neutral compact feed."""
    root, store, worker = owned
    journal = _journal(root, B, "codex")
    _append(journal,
            {"method": "ccx/evidence-started", "sequence": 1,
             "params": {"journalInstanceId": "old-before-ccx-compact"}},
            {"method": "item/started", "sequence": 2,
             "turnId": "old-turn", "itemId": "native-item",
             "params": {"turnId": "old-turn",
                        "item": {"id": "native-item", "type": "contextCompaction"}}},
            {"method": "item/completed", "sequence": 3,
             "turnId": "old-turn", "itemId": "native-item",
             "params": {"turnId": "old-turn",
                        "item": {"id": "native-item", "type": "contextCompaction"}}})
    neutral = worker.events(B, kinds=["compact.started", "compact.ended"])
    assert neutral["events"] == []
    assert neutral["next_cursor"].startswith("ev2.")
    from ccx.ops.worker_events import read_full_uuid_event_page
    native = read_full_uuid_event_page(store, B, native=True)
    assert [row["method"] for row in native["events"]] == [
        "ccx/evidence-started", "item/started", "item/completed",
    ]
