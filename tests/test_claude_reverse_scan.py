"""Synthetic reference-equivalence and cost tests for Claude page discovery.

Run ``PYTHONPATH=. python tests/test_claude_reverse_scan.py --benchmark`` to
measure the same deterministic large source with the current implementation.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys
import time
from contextlib import nullcontext
from unittest.mock import patch

import pytest

from ccx.ops import conversation_paging as P

WORKER_ID = "12345678-1234-4123-8123-123456789abc"
WORKER = f"claude:{WORKER_ID}"


def _user(uid, content, **fields):
    return {"type": "user", "uuid": uid, "message": {"content": content}, **fields}


def _assistant(uid, text):
    return {
        "type": "assistant", "uuid": uid,
        "message": {"id": f"msg-{uid}", "model": "fake-claude",
                    "content": [{"type": "text", "text": text}]},
    }


def _write(path, rows, *, ending=b"\n", partial=b""):
    with path.open("wb") as handle:
        for row in rows:
            if isinstance(row, bytes):
                handle.write(row + ending)
            else:
                handle.write(json.dumps(row, ensure_ascii=False).encode("utf-8") + ending)
        handle.write(partial)
    return path


def _reference_choose(path, source, *, cut_offset, normalization_end_offset,
                      target, max_tool_result_bytes, anchor_hint=None):
    """Previous full forward scan and 4/8/16 adaptive probes, test-only."""
    def normalize_from(boundary):
        records, counters = P._normalize_claude_window(
            path, after_offset=boundary["offset"],
            before_offset=normalization_end_offset,
            max_tool_result_bytes=max_tool_result_bytes,
        )
        return P._bearing_before(records, cut_offset), counters

    if (isinstance(anchor_hint, dict)
            and isinstance(anchor_hint.get("offset"), int)
            and 0 <= anchor_hint["offset"] < cut_offset):
        eligible, counters = normalize_from(anchor_hint)
        if len(eligible) >= target or anchor_hint["offset"] == 0:
            return anchor_hint, eligible, counters

    by_offset = {}
    for evt in P._iter_transcript(path, before_offset=source["eof"]):
        if P._ordinary_user_boundary(evt):
            boundary = P._boundary_from_event(evt)
            if boundary is not None:
                by_offset[boundary["offset"]] = boundary
    roots = [by_offset[offset] for offset in sorted(by_offset)
             if offset < cut_offset]
    count = len(roots)
    if count:
        step = min(4, count)
        while True:
            candidate = roots[count - step]
            eligible, counters = normalize_from(candidate)
            if len(eligible) >= target:
                return candidate, eligible, counters
            if step >= count:
                break
            step = min(count, step * 2)
    zero = {"offset": 0, "source_record_id": None, "fingerprint": None}
    eligible, counters = normalize_from(zero)
    return zero, eligible, counters


def _read(path, *, cursor=None, target=7):
    # Exactly the public reader's pinned-fd path, without any live workers.
    fd = os.open(path, os.O_RDONLY)
    try:
        stat = os.fstat(fd)
        return P.read_claude_page(
            P.PinnedNativeSource(fd), WORKER, root=path.parent,
            generation="synthetic-g1", session_id=WORKER_ID,
            source_stat=stat, end_offset=P._complete_line_end(fd, stat.st_size),
            cursor=cursor, source_record_target=target,
        )
    finally:
        os.close(fd)


def _walk(path, *, target, oracle=False):
    pages = []
    with patch.object(P, "_choose_claude_window", _reference_choose) if oracle else nullcontext():
        cursor = None
        for _ in range(200):
            page = _read(path, cursor=cursor, target=target)
            pages.append(json.dumps(page, sort_keys=True, separators=(",", ":"),
                                    ensure_ascii=False).encode("utf-8"))
            if page["older_cursor"] is None:
                return pages
            payload = P._decode_cursor(page["older_cursor"], engine="claude", worker_id=WORKER_ID)
            assert payload["anchor_offset"] < payload["cut_offset"]
            cursor = page["older_cursor"]
        pytest.fail("synthetic cursor walk did not terminate")


def _fixture_rows(shape):
    if shape == "no-user-boundary":
        rows = [_assistant(f"a{i}", f"message {i}") for i in range(12)]
        rows += [_user("meta", "hidden", isMeta=True),
                 _user("local", "<local-command-stdout>result</local-command-stdout>"),
                 _user("queued", [{"type": "queued_command", "text": "steer"}])]
        return rows
    rows = ([] if shape == "user-at-zero" else
            [b"", b"  ", {"type": "system", "subtype": "init", "uuid": "sys"}])
    for i in range(36 if shape != "few-boundaries" else 3):
        rows.append(_user(f"u{i}", f"ask {i} é" if i % 2 else
                          [{"type": "text", "text": f"ask {i}"}]))
        rows.append(_assistant(f"a{i}", f"reply {i}"))
        if i % 3 == 0:
            rows += [
                _user(f"meta{i}", "private metadata", isMeta=True),
                _user(f"queue{i}", [{"type": "text", "text": "queued_command"},
                                    {"type": "queued_command", "text": "steer"}]),
                _user(f"local{i}", " <local-command-stderr>ignored</local-command-stderr>"),
            ]
        if i % 4 == 0:
            rows.append({**_assistant(f"side{i}", "sidechain"), "isSidechain": True})
            rows.append(b"{malformed-json")
        if i % 5 == 0:
            call = f"call-{i}"
            rows.append({"type": "assistant", "uuid": f"tool{i}",
                         "message": {"id": f"tool-msg{i}", "model": "fake-claude",
                                     "content": [{"type": "tool_use", "id": call,
                                                  "name": "Bash", "input": {"command": "true"}}]}})
            rows.append(_user(f"result{i}", [{"type": "tool_result",
                                               "tool_use_id": call,
                                               "content": "z" * (180_000 if shape == "large-results" else 50)}]))
        if i == 9:
            rows += [{"type": "system", "subtype": "compact_boundary",
                      "uuid": "compact-9", "content": "compacted"},
                     _user("summary-9", "summary", isCompactSummary=True,
                           parentUuid="compact-9")]
        if i == 16:
            rows += [_user("command", "/compact"),
                     {"type": "queue-operation", "uuid": "enqueue",
                      "operation": "enqueue", "content": "steer"}]
    rows += [b"\xff", b"", _assistant("final", "last answer")]
    if shape == "no-native-ids":
        for row in rows:
            if isinstance(row, dict) and row["type"] == "user":
                row.pop("uuid", None)
    return rows


@pytest.mark.parametrize("shape", ["mixed", "large-results", "few-boundaries",
                                    "no-user-boundary", "no-native-ids", "user-at-zero"])
@pytest.mark.parametrize("target", [3, 11, 40])
def test_newest_and_all_older_pages_match_full_scan_byte_for_byte(tmp_path, shape, target):
    path = _write(tmp_path / "original.jsonl", _fixture_rows(shape),
                  ending=b"\r\n" if shape == "mixed" else b"\n",
                  partial=b'{"type":"assistant","uuid":"incomplete"')
    expected = _walk(path, target=target, oracle=True)
    actual = _walk(path, target=target)
    assert actual == expected


def test_bursty_turns_reanchor_before_cut_and_walk_all_pages(tmp_path):
    """Four groups of 20 records cause page 2 to cut exactly at its anchor.

    The original cursor producer emitted that equal anchor/cut, although the
    following cursor validator always rejected it on the unchanged source.
    """
    rows = [b"", {"type": "system", "subtype": "init", "uuid": "header"}]
    for i in range(10):
        rows.append(_user(f"u{i}", f"request {i}"))
        rows.extend(_assistant(f"a{i}-{j}", f"answer {i}/{j}") for j in range(19))
        if i % 2 == 0:
            rows.extend([
                _user(f"meta{i}", "non-page metadata", isMeta=True),
                {**_assistant(f"side{i}", "sidechain"), "isSidechain": True},
            ])
    path = _write(tmp_path / "bursty.jsonl", rows, partial=b'{"type":"user"')
    first = _read(path, target=40)
    second = _read(path, cursor=first["older_cursor"], target=40)
    assert len(first["records"]) == len(second["records"]) == 40
    payload = P._decode_cursor(second["older_cursor"], engine="claude", worker_id=WORKER_ID)
    assert payload["anchor_offset"] < payload["cut_offset"]

    pages = _walk(path, target=40)
    ids = [r["source_record_id"] for raw in pages
           for r in json.loads(raw)["records"]]
    assert len(ids) == 200
    assert len(ids) == len(set(ids))
    assert set(ids) == {f"u{i}" for i in range(10)} | {
        f"a{i}-{j}" for i in range(10) for j in range(19)
    }


def test_reanchor_uses_zero_fallback_before_first_ordinary_user(tmp_path):
    rows = [_assistant("prefix", "before the first ordinary user"),
            _user("meta", "metadata", isMeta=True),
            _user("start", "the first prompt")]
    rows.extend(_assistant(f"a{i}", f"reply {i}") for i in range(39))
    path = _write(tmp_path / "prelude.jsonl", rows)

    newest = _read(path, target=40)
    assert len(newest["records"]) == 40
    payload = P._decode_cursor(newest["older_cursor"], engine="claude", worker_id=WORKER_ID)
    assert payload["anchor_offset"] == 0
    assert payload["anchor_id"] is None and payload["anchor_fingerprint"] is None
    older = _read(path, cursor=newest["older_cursor"], target=40)
    assert [r["source_record_id"] for r in older["records"]] == ["prefix"]
    assert older["older_cursor"] is None


def test_reverse_line_offsets_and_certified_eof_on_pinned_fd(tmp_path):
    body = b"\r\n" + b"long" * 40_000 + b"\n" + b"short\r\n" + b"\n"
    path = tmp_path / "bytes.jsonl"
    path.write_bytes(body + b"partial-line-without-newline")
    offsets = []
    position = 0
    for row in body.split(b"\n")[:-1]:
        if row:
            offsets.append((position, row))
        position += len(row) + 1
    fd = os.open(path, os.O_RDONLY)
    try:
        pinned = P.PinnedNativeSource(fd)
        assert list(P._reverse_lines(pinned, before_offset=len(body))) == list(reversed(offsets))
    finally:
        os.close(fd)


def test_adaptive_probes_choose_exactly_the_old_anchor(tmp_path):
    path = _write(tmp_path / "probes.jsonl", [
        event for i in range(70)
        for event in (_user(f"u{i}", f"prompt {i}"), _assistant(f"a{i}", f"reply {i}"))
    ])
    size = path.stat().st_size
    source = {"eof": size}
    for target in (1, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 99, 100):
        arguments = {"cut_offset": size, "normalization_end_offset": size,
                     "target": target, "max_tool_result_bytes": 16_384}
        expected = _reference_choose(path, source, **arguments)
        actual = P._choose_claude_window(path, source, **arguments)
        assert actual[0] == expected[0], target
        assert P._bearing_before(actual[1], size) == P._bearing_before(expected[1], size)
        assert actual[2].malformed == expected[2].malformed


def test_older_read_does_not_scan_from_transcript_start(tmp_path, monkeypatch):
    path = _write(tmp_path / "many.jsonl", [
        event for i in range(70)
        for event in (_user(f"u{i}", f"prompt {i}"), _assistant(f"a{i}", f"reply {i}"))
    ])
    cursor = _read(path, target=7)["older_cursor"]
    assert cursor
    original = P._iter_transcript
    starts = []

    def tracing_iterator(*args, **kwargs):
        starts.append(kwargs.get("after_offset", 0))
        yield from original(*args, **kwargs)

    monkeypatch.setattr(P, "_iter_transcript", tracing_iterator)
    _read(path, cursor=cursor, target=7)
    assert starts and min(starts) > 0, starts


def test_reverse_discovery_uses_pinned_inode_after_path_replacement(tmp_path):
    path = _write(tmp_path / "original.jsonl", _fixture_rows("mixed"))
    fd = os.open(path, os.O_RDONLY)
    try:
        stat = os.fstat(fd)
        eof = P._complete_line_end(fd, stat.st_size)
        path.rename(tmp_path / "old.jsonl")
        _write(path, [_user("replacement", "different"), _assistant("new", "different")])
        actual = P.read_claude_page(
            P.PinnedNativeSource(fd), WORKER, root=tmp_path,
            generation="synthetic-g1", session_id=WORKER_ID,
            source_stat=stat, end_offset=eof, cursor=None, source_record_target=3)
        assert actual["records"][-1]["source_record_id"] == "final"
    finally:
        os.close(fd)


def _large_transcript(path, *, turns=4_500, payload_bytes=12_000):
    if path.exists():
        return path
    with path.open("wb") as out:
        for i in range(turns):
            for event in (_user(f"u{i}", f"request {i}"),
                          _assistant(f"a{i}", "x" * payload_bytes)):
                out.write(json.dumps(event, separators=(",", ":")).encode() + b"\n")
    return path


def test_large_newest_page_parses_bounded_number_of_lines(tmp_path, monkeypatch):
    path = _large_transcript(tmp_path / "large.jsonl")
    assert path.stat().st_size > 50 * 1024 * 1024
    original = json.loads
    parsed = 0

    def counting_parser(raw, *args, **kwargs):
        nonlocal parsed
        parsed += 1
        return original(raw, *args, **kwargs)

    monkeypatch.setattr(json, "loads", counting_parser)
    page = _read(path, target=7)
    assert page["records"][-1]["source_record_id"] == "a4499"
    # Four source-boundary turns plus one normalized target window, not 9,000 rows.
    assert parsed < 100, parsed


def test_only_returned_anchor_boundary_is_fingerprinted(tmp_path, monkeypatch):
    path = _write(tmp_path / "original.jsonl", _fixture_rows("mixed"))
    original = P._event_fingerprint
    hashed = []

    def tracking_hash(evt):
        hashed.append(evt["_ccx_start_offset"])
        return original(evt)

    monkeypatch.setattr(P, "_event_fingerprint", tracking_hash)
    _read(path, target=11)
    assert len(hashed) == 1


if __name__ == "__main__" and sys.argv[1:] == ["--benchmark"]:
    path = Path("benchmark-claude-synthetic.jsonl")
    _large_transcript(path)
    print(f"synthetic_path={path} bytes={path.stat().st_size} turns=4500")
    for label, reference in (("old-full-scan", True), ("current", False)):
        samples = []
        for _ in range(3):
            started = time.perf_counter()
            with patch.object(P, "_choose_claude_window", _reference_choose) if reference else nullcontext():
                page = _read(path, target=7)
            samples.append((time.perf_counter() - started) * 1000)
        print(f"{label}: samples_ms={[round(x, 1) for x in samples]}, "
              f"median_ms={sorted(samples)[1]:.1f}, "
              f"records={len(page['records'])}, cursor={bool(page['older_cursor'])}")
