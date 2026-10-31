"""Pure detached-launcher verdict parsing."""

import pytest

import ccx.claude.spawn as SP
from ccx.system import oslayer


def test_parse_verdict_takes_last_json_line():
    out = b'noise\n{"ok": false}\n{"ok": true, "short": "s"}\n'
    assert SP._parse_verdict(out) == {"ok": True, "short": "s"}
    assert SP._parse_verdict(b"") is None
    assert SP._parse_verdict(None) is None


@pytest.mark.parametrize("probe,state,expected", [
    ("missing", None, False), ("denied", None, True), ("exists", "S", True),
    ("exists", "Z", False), ("exists", "X", True), ("exists", None, True),
])
def test_launcher_kernel_presence_and_zombie_boundary(monkeypatch, probe, state, expected):
    # Preserve the launcher's historical EPERM-means-present behavior. Its
    # zombie exclusion is intentionally narrower than exact PID/start proof.
    monkeypatch.setattr(SP.oslayer, "probe_pid", lambda _: probe)

    def observed_process(pid):
        if probe != "exists":
            pytest.fail("no /proc read on ESRCH or EPERM")
        return oslayer.ProcessInfo(pid, "unprovable", None, state, None)

    monkeypatch.setattr(SP.oslayer, "process_info", observed_process)
    assert SP._process_running(123) is expected
