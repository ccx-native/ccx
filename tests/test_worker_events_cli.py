"""Top-level events CLI over sealed, original full-UUID fake journals only."""
from __future__ import annotations

from contextlib import contextmanager
import json
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import time
from typing import Iterator

import pytest

from ccx import constants
from ccx.errors import CursorInvalidated
from ccx.workers import WorkerClient
from test_generation_store import A, B, C, G1, G2, add_worker, fixture_root, switch_pointer
from test_worker_events import _append, _event, _journal


def _environment(root: Path) -> dict[str, str]:
    """An allowlisted, provider-free environment for the real CLI subprocess."""
    dirs = {key: root.parent / key.lower() for key in (
        "HOME", "XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_RUNTIME_DIR",
    )}
    for directory in dirs.values():
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        directory.chmod(0o700)
    return {
        **{key: str(directory) for key, directory in dirs.items()},
        "CCX_ROOT": str(root),
        "PYTHONPATH": str(Path(__file__).resolve().parent.parent),
        "PYTHONDONTWRITEBYTECODE": "1",
        "PATH": os.defpath,
        "LC_ALL": "C.UTF-8",
    }


def _cli(root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-B", "-m", "ccx", "events", *args],
        cwd=root.parent, env=_environment(root), capture_output=True,
        text=True, timeout=15,
    )


def _default_sigint() -> None:
    """Do not inherit SIG_IGN from a detached/background pytest launcher."""
    signal.signal(signal.SIGINT, signal.SIG_DFL)


@contextmanager
def _follow_process(root: Path, *args: str) -> Iterator[subprocess.Popen[bytes]]:
    """Own the CLI child's lifetime, including exceptions and read timeouts.

    A Popen context manager calls wait() without a bound on exit. If an
    inherited SIG_IGN makes communicate(timeout=...) fail, that unbounded wait
    would hang pytest indefinitely. Always kill and reap before leaving.
    """
    process = subprocess.Popen(
        [sys.executable, "-B", "-m", "ccx", "events", *args],
        cwd=root.parent, env=_environment(root),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        preexec_fn=_default_sigint,
    )
    try:
        yield process
    finally:
        if process.poll() is None:
            process.kill()
        try:
            process.wait(timeout=5)
        finally:
            if process.stdout is not None:
                process.stdout.close()
            if process.stderr is not None:
                process.stderr.close()


def _initial_lines(process: subprocess.Popen, count: int) -> bytes:
    """Wait for actual flushed CLI output before changing its fake source."""
    output = b""
    deadline = time.monotonic() + 5
    while output.count(b"\n") < count:
        remaining = deadline - time.monotonic()
        assert remaining > 0, (output, process.poll())
        readable, _, _ = select.select([process.stdout], [], [], remaining)
        assert readable, (output, process.poll())
        chunk = os.read(process.stdout.fileno(), 65536)
        assert chunk, (output, process.poll())
        output += chunk
    return output


def test_events_follow_helper_kills_and_reaps_on_communicate_timeout(tmp_path):
    """A stalled test read must fail promptly, never wait forever on Popen exit."""
    root = fixture_root(tmp_path)
    add_worker(root, A)
    with _follow_process(root, A, "--follow", "--tail", "0",
                         "--heartbeat", "0", "--json") as process:
        assert json.loads(_initial_lines(process, 1).splitlines()[0])["kind"] == "heartbeat"
        with pytest.raises(subprocess.TimeoutExpired):
            process.communicate(timeout=0.05)
    assert process.poll() == -signal.SIGKILL


def test_events_snapshot_real_cli_prefix_format_pagination_gap_and_codex(tmp_path):
    root = fixture_root(tmp_path)
    add_worker(root, A)
    add_worker(root, B, "codex")  # Same 8-hex prefix, different full identity.
    journal = _journal(root)
    _append(journal, *(
        {**_event("E1", seq), "from": "idle", "to": "busy"}
        for seq in range(1, 26)
    ))
    codex = _journal(root, B, "codex")
    _append(codex,
            {"method": "turn/started", "sequence": 1, "turnId": "native-turn"},
            {"method": "turn/completed", "sequence": 2, "turnId": "native-turn",
             "params": {"turn": {"status": "completed"}}},
            {"method": "ccx/input/requested", "sequence": 3,
             "params": {"request": {"gateId": "native-gate", "kind": "command"}}})

    ambiguous = _cli(root, A[:8], "--json")
    assert ambiguous.returncode == 1 and not ambiguous.stdout
    assert f"claude:{A}" in ambiguous.stderr and f"codex:{B}" in ambiguous.stderr

    human = _cli(root, A[:13])
    assert human.returncode == 0, human.stderr
    lines = human.stdout.splitlines()
    assert len(lines) == 20, lines
    assert all("state idle -> busy" in line for line in lines)
    assert "#" not in lines[0] and "next_cursor:" not in human.stdout
    assert not human.stderr

    tail = _cli(root, A, "--tail", "1", "--heartbeat", "0", "--json")
    assert tail.returncode == 0, tail.stderr
    one = json.loads(tail.stdout)
    assert [ev["seq"] for ev in one["events"]] == [25]
    assert one["state_source"] == "recorded" and "native_state" in one
    assert one["worker"] == f"claude:{A}" and one["next_cursor"].startswith("ev2.")
    assert one["has_more"] is False

    prior = _cli(root, A, "--tail", "0", "--json")
    assert prior.returncode == 0, prior.stderr
    old_cursor = json.loads(prior.stdout)["next_cursor"]
    journal.write_text("")  # Rotate original epoch: lost cursor, retained replay >1000.
    _append(journal, *(
        {**_event("E9", seq), "from": "idle", "to": "busy"}
        for seq in range(1, 1003)
    ))
    resumed = _cli(root, A[:13], "--after", old_cursor, "--json")
    assert resumed.returncode == 0, resumed.stderr
    page = json.loads(resumed.stdout)  # Exactly ONE JSON object, no NDJSON pages.
    assert len(page["events"]) == 1002 and not page["has_more"]
    assert [page["events"][0]["seq"], page["events"][-1]["seq"]] == [1, 1002]
    assert page["gap"]["reason"] == "cursor_not_retained"
    assert page["next_cursor"] == page["events"][-1]["cursor"]
    assert page["state_source"] == "recorded"
    lost_human = _cli(root, A, "--after", old_cursor)
    assert lost_human.returncode == 0
    assert len(lost_human.stdout.splitlines()) == 1002
    assert "cursor could not be placed (cursor_not_retained)" in lost_human.stderr
    assert "next_cursor:" not in lost_human.stdout

    selected = _cli(root, f"codex:{B}", "--kind", "turn.started",
                    "--kind", "turn.ended", "--json")
    assert selected.returncode == 0, selected.stderr
    codex_page = json.loads(selected.stdout)
    assert [e["kind"] for e in codex_page["events"]] == ["turn.started", "turn.ended"]
    assert {e["worker_id"] for e in codex_page["events"]} == {B}
    assert {e["engine"] for e in codex_page["events"]} == {"codex"}
    assert not (root / "sessions").exists() and not (root / "run").exists()


def test_events_follow_real_cli_stopped_worker_initial_cursor_append_and_human(tmp_path):
    root = fixture_root(tmp_path)
    store = add_worker(root, A)
    store.mutate_record(A, patches={"lifecycle.state": "dead"})
    journal = _journal(root)
    _append(journal, {**_event("E1", 1), "from": "idle", "to": "busy"})

    with _follow_process(root, A, "--follow", "--json", "--timeout", "1.5",
                         "--heartbeat", "0") as process:
        first = _initial_lines(process, 2)
        _append(journal, {**_event("E1", 2), "from": "busy", "to": "idle"})
        rest, err = process.communicate(timeout=8)
        assert process.returncode == 0, (first, rest, err)
    output = [json.loads(line) for line in (first + rest).splitlines()]
    assert [item["kind"] for item in output] == ["state", "heartbeat", "state"]
    assert output[1]["cursor"] == output[0]["cursor"]
    assert output[1]["state"] == "stopped" and output[1]["state_source"] == "recorded"
    assert output[2]["seq"] == 2 and output[2]["cursor"] != output[1]["cursor"]
    assert not err

    # From-now follow emits only its initial cursor heartbeat, even at heartbeat=0.
    now = _cli(root, A, "--tail", "0", "--json")
    from_now = _cli(root, A, "--follow", "--tail", "0", "--heartbeat", "0",
                    "--timeout", "0", "--json")
    assert now.returncode == from_now.returncode == 0
    beats = [json.loads(line) for line in from_now.stdout.splitlines()]
    assert len(beats) == 1 and beats[0]["kind"] == "heartbeat"
    assert beats[0]["cursor"] == json.loads(now.stdout)["next_cursor"]
    human = _cli(root, A, "--follow", "--tail", "1", "--heartbeat", "0",
                 "--timeout", "0")
    assert human.returncode == 0, human.stderr
    assert len(human.stdout.splitlines()) == 1
    assert "state busy -> idle" in human.stdout and "heartbeat" not in human.stdout

    # An interrupt to this disposable CLI process exits 0, without native control.
    with _follow_process(root, A, "--follow", "--tail", "0", "--heartbeat", "0",
                         "--json") as process:
        initial = _initial_lines(process, 1)
        assert json.loads(initial.splitlines()[0])["kind"] == "heartbeat"
        process.send_signal(signal.SIGINT)
        rest, err = process.communicate(timeout=8)
        assert process.returncode == 0 and not err, (initial, rest, err)
    assert not (root / "sessions").exists() and not (root / "run").exists()


def test_events_cli_errors_missing_unsafe_generation_and_backend_cursor_code(tmp_path,
                                                                               monkeypatch):
    root = fixture_root(tmp_path)
    add_worker(root, A)
    add_worker(root, B, "codex")
    _append(_journal(root), _event("E1", 1))
    foreign = json.loads(_cli(root, B, "--tail", "0", "--json").stdout)["next_cursor"]
    for flags, expected in (
        (("--after", foreign), "invalid --after cursor"),
        (("--after", "ev1.legacy"), "invalid --after cursor"),
        (("--after", "ev2.???"), "invalid --after cursor"),
        (("--after", foreign, "--tail", "1"), "cannot be combined"),
        (("--timeout", "1"), "requires --follow"),
        (("--kind", "made-up"), "unknown event feed kind"),
        (("--tail", "-1"), "invalid event page"),
        (("--follow", "--timeout", "-1"), "nonnegative finite"),
    ):
        result = _cli(root, A, *flags)
        assert result.returncode == 1 and expected in result.stderr, (flags, result)
    assert _cli(root, C).returncode == 2  # Full UUID that was never an owner.
    assert _cli(root, "display is not a key").returncode == 1

    monkeypatch.setattr(constants, "CCX_ROOT", root)
    try:
        WorkerClient().events(A, after=foreign)
    except CursorInvalidated as error:
        assert error.exit_code == 5  # Only CLI remaps to main-compatible 1.
    else:
        assert False, "typed backend must reject foreign source cursor"

    unsafe = _journal(root, B, "codex")
    unsafe.symlink_to(tmp_path / "untrusted-journal")
    refused = _cli(root, B, "--json")
    assert refused.returncode == 3 and "unsafe" in refused.stderr
    unsafe.unlink()

    owner = root / "identity" / "generations" / G1 / "owners" / (A + ".json")
    owner.unlink()
    removed = _cli(root, A, "--json")
    assert removed.returncode == 2 and f"no such worker: {A}" in removed.stderr

    # An already-following client is pinned: a new ACTIVE cannot silently
    # reinterpret its original source, even if the same full worker ID returns.
    root2 = fixture_root(tmp_path / "other")
    add_worker(root2, A)
    _append(_journal(root2), _event("E1", 1))
    with _follow_process(root2, A, "--follow", "--json", "--timeout", "2",
                         "--heartbeat", "0") as process:
        first = _initial_lines(process, 2)
        switch_pointer(root2, G2)
        add_worker(root2, A)
        rest, err = process.communicate(timeout=8)
        assert process.returncode == 3, (first, rest, err)
        assert b"Cannot follow events" in err and b"generation" in err


def test_events_cli_compaction_outcome_parity_json_and_follow(tmp_path):
    root = fixture_root(tmp_path)
    add_worker(root, A)
    add_worker(root, B, "codex")
    _append(_journal(root),
            {**_event("host-1", 1, "compact_started"),
             "operation_id": "cli-op-a"},
            {**_event("host-1", 2, "compact_unknown"),
             "operation_id": "cli-op-a"})
    _append(_journal(root, B, "codex"),
            {"method": "ccx/evidence-started", "sequence": 1,
             "params": {"journalInstanceId": "cli-codex-compacts"}},
            {"method": "ccx/compact/started", "sequence": 2,
             "turnId": "compact-codex-turn",
             "params": {"turnId": "compact-codex-turn", "compactId": "operation:cli-b",
                        "operationId": "cli-b"}},
            {"method": "ccx/compact/ended", "sequence": 3,
             "turnId": "compact-codex-turn",
             "params": {"turnId": "compact-codex-turn", "compactId": "operation:cli-b",
                        "operationId": "cli-b", "outcome": "failed"}})
    for target, outcome in ((A, "unknown"), (B, "failed")):
        ordinary = _cli(root, target, "--kind", "compact.started",
                        "--kind", "compact.ended", "--json")
        assert ordinary.returncode == 0, ordinary.stderr
        page = json.loads(ordinary.stdout)
        assert [e["kind"] for e in page["events"]] == [
            "compact.started", "compact.ended",
        ]
        assert page["events"][1]["outcome"] == outcome
        streamed = _cli(root, target, "--follow", "--kind", "compact.started",
                        "--kind", "compact.ended", "--timeout", "0",
                        "--heartbeat", "0", "--json")
        assert streamed.returncode == 0, streamed.stderr
        lines = [json.loads(line) for line in streamed.stdout.splitlines()]
        ended = [e for e in lines if e.get("kind") == "compact.ended"]
        assert len(ended) == 1 and ended[0]["outcome"] == outcome
