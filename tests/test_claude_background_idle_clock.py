"""Background completion begins a new idle period; it is not old idleness.

Uses the real managed host + native-shaped fake transcript, no live provider.
"""
from __future__ import annotations
import json
from pathlib import Path
import time

from ccx import CcxClient
from ccx.errors import CreatePartialFailure
from ccx.store.root import require_ccx_active
from test_generation_store import A, short_root
from test_create_cross_surface import _deadline_fake_claude
from test_claude_native_cli import _stop_disposable_original
from test_transcript import _bg_launch_events, _bg_notification_event


def until(fn, seconds=12):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        value = fn()
        if value:
            return value
        time.sleep(.04)
    raise AssertionError("expected original host observation did not arrive")


def test_last_background_completion_gets_full_configured_idle_window(short_root, tmp_path, monkeypatch):
    _deadline_fake_claude(short_root, tmp_path, monkeypatch)
    monkeypatch.setenv("FAKE_HOOKS_VIA_SETTINGS", "1")
    store = require_ccx_active(short_root)
    try:
        deadline = time.monotonic() + 18
        while True:
            try:
                first = CcxClient().new("fake first settled turn", cwd=str(tmp_path), worker_id=A,
                                        idle_timeout=3, wait=True, timeout=14, start_timeout=4)
                assert first.settled
                break
            except CreatePartialFailure:
                assert time.monotonic() < deadline
        transcript = Path(store.record(A)["transcript"])
        events_path = store.worker_path(A).parent / "events.ndjson"

        def events():
            return [json.loads(x) for x in events_path.read_text().splitlines()]

        with transcript.open("a") as f:
            for row in _bg_launch_events(task_id="late-idle-bash",tool_use_id="late-idle-tool"):
                f.write(json.dumps(row)+"\n")
        until(lambda: any(e.get("type")=="idle_grace_bg" for e in events()))
        # The foreground idle age is already overdue; the background task is
        # the only remaining work keeping the exact original provider alive.
        assert store.record(A)["lifecycle"]["state"] == "idle"
        with transcript.open("a") as f:
            f.write(json.dumps(_bg_notification_event(task_id="late-idle-bash",tool_use_id="late-idle-tool"))+"\n")
        terminal = until(lambda: next((e for e in reversed(events())
                         if e.get("type")=="background_changed"
                         and (e.get("item") or {}).get("status")=="completed"),None))
        assert store.record(A)["lifecycle"]["state"] == "idle", (
            "completion was charged against foreground idle time and killed immediately", events()[-5:])
        time.sleep(.8)
        assert store.record(A)["lifecycle"]["state"] == "idle"
        # No forever extension: with no native follow-up, the configured
        # timeout still eventually parks the worker, measured from completion.
        killed = until(lambda: next((e for e in reversed(events())
                                     if e.get("type")=="idle_killed"),None),seconds=8)
        assert killed["ts"] - terminal["ts"] >= 2.8
    finally:
        _stop_disposable_original(short_root)
