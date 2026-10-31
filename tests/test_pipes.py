"""A closed stdout reader ends a ccx command quietly; other EPIPEs still raise."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from ccx.system.pipes import quiet_broken_stdout
from ccx.system import oslayer

REPO = Path(__file__).resolve().parent.parent

FLOOD = (
    "from ccx.system.pipes import quiet_broken_stdout\nwith quiet_broken_stdout():\n    for i in range(200000):\n        print('line', i)\n"
)


def test_reader_closing_the_pipe_exits_zero_without_a_traceback():
    proc = subprocess.run(
        f"{sys.executable} -c \"$FLOOD\" | head -1; echo \"rc=${{PIPESTATUS[0]}}\"",
        shell=True, executable="/bin/bash", cwd=REPO, capture_output=True,
        text=True, env={"FLOOD": FLOOD, "PATH": "/usr/bin:/bin"}, timeout=60,
    )
    assert proc.stdout.splitlines() == ["line 0", "rc=0"]
    assert "BrokenPipeError" not in proc.stderr
    assert proc.stderr == ""


def test_broken_pipe_from_something_other_than_stdout_still_raises():
    with pytest.raises(BrokenPipeError):
        with quiet_broken_stdout():
            raise BrokenPipeError("worker socket went away")


def test_darwin_stdout_EOF_kqueue_distinguishes_closed_reader(monkeypatch):
    """Never swallow an unrelated EPIPE just because kqueue can be opened."""
    class Event:
        def __init__(self, flags):
            self.flags = flags

    class Queue:
        def __init__(self, events):
            self.events = events
            self.closed = False

        def control(self, changes, maximum, timeout):
            assert len(changes) == 1 and maximum == 1 and timeout == 0
            return self.events

        def close(self):
            self.closed = True

    fake_flags = {"EOF": 0x8000}
    queues = []
    monkeypatch.setattr(oslayer.sys, "platform", "darwin")
    monkeypatch.setattr(oslayer.select, "KQ_FILTER_WRITE", -2, raising=False)
    monkeypatch.setattr(oslayer.select, "KQ_EV_ADD", 1, raising=False)
    monkeypatch.setattr(oslayer.select, "KQ_EV_ENABLE", 4, raising=False)
    monkeypatch.setattr(oslayer.select, "KQ_EV_EOF", fake_flags["EOF"], raising=False)
    monkeypatch.setattr(oslayer.select, "kevent", lambda fd, **kwargs:
                        (fd, kwargs), raising=False)

    def create_queue():
        queue = Queue([Event(fake_flags["EOF"])] if not queues else [])
        queues.append(queue)
        return queue

    monkeypatch.setattr(oslayer.select, "kqueue", create_queue, raising=False)
    assert oslayer.stdout_reader_gone(1) is True
    assert oslayer.stdout_reader_gone(1) is False
    assert all(queue.closed for queue in queues)
