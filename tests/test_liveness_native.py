from __future__ import annotations

import json

import pytest

from ccx.store import liveness as L
from ccx.system import oslayer


def _meta(root, pid, **patch):
    path = root / "sessions" / f"{pid}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    value = {
        "pid": pid,
        "sessionId": "session-1234",
        "kind": "interactive",
        "status": "busy",
        "statusUpdatedAt": 1234,
    }
    value.update(patch)
    path.write_text(json.dumps(value))
    return path


def test_read_managed_native_state_accepts_exact_identity(monkeypatch, tmp_path):
    _meta(tmp_path, 123)
    monkeypatch.setattr(L, "session_pid_alive", lambda pid, sid: (pid, sid) == (123, "session-1234"))
    state = L.read_managed_claude_native_state(123, "session-1234", claude_home=tmp_path)
    assert state is not None
    assert state.status == "busy"
    assert state.status_updated_at == 1234


def test_read_managed_native_state_rejects_session_or_kind(monkeypatch, tmp_path):
    monkeypatch.setattr(L, "session_pid_alive", lambda pid, sid: True)
    _meta(tmp_path, 123, sessionId="other")
    assert L.read_managed_claude_native_state(123, "session-1234", claude_home=tmp_path) is None
    _meta(tmp_path, 123, kind="daemon")
    assert L.read_managed_claude_native_state(123, "session-1234", claude_home=tmp_path) is None


def test_read_managed_native_state_rejects_proc_start_mismatch(monkeypatch, tmp_path):
    monkeypatch.setattr(L, "session_pid_alive", lambda pid, sid: True)
    monkeypatch.setattr(L, "proc_start_token", lambda pid: "222")
    _meta(tmp_path, 123, procStart="111")
    assert L.read_managed_claude_native_state(123, "session-1234", claude_home=tmp_path) is None


def test_read_managed_native_state_accepts_real_linux_pid_domain(monkeypatch, tmp_path):
    monkeypatch.setattr(oslayer.sys, "platform", "linux")
    monkeypatch.setattr(L, "proc_start_token", lambda pid: "123456")
    _meta(tmp_path, 123, procStart="123456",
          pidDomain="linux:9c1c63c55c164e25b6cab9ecf2d91080:pid:[4026532223]")
    state = L.read_managed_claude_native_state(123, "session-1234", claude_home=tmp_path)
    assert state is not None and state.proc_start == "123456"
    _meta(tmp_path, 123, procStart="123456", pidDomain="darwin")
    assert L.read_managed_claude_native_state(123, "session-1234", claude_home=tmp_path) is None


def test_read_managed_native_state_accepts_shell_and_missing_status_token(monkeypatch, tmp_path):
    monkeypatch.setattr(L, "session_pid_alive", lambda pid, sid: True)
    _meta(tmp_path, 123, status="shell", statusUpdatedAt=None)
    state = L.read_managed_claude_native_state(123, "session-1234", claude_home=tmp_path)
    assert state is not None
    assert state.status == "shell"
    assert state.status_updated_at is None


@pytest.mark.skipif(
    oslayer.UNIX_SUN_PATH_BYTES != 108,
    reason="Linux /proc field22 parsing; Darwin libproc tested separately",
)
def test_proc_start_token_parses_proc_stat(monkeypatch, tmp_path):
    class FakePath:
        def __init__(self, value): self.value = value
        def read_bytes(self):
            # fields: pid (comm) state ... field22=999
            return ("123 (claude worker) S " + " ".join(["0"] * 18 + ["999"] + ["0"] * 5)).encode("ascii")
    # The proc stat reader now lives at the OS boundary; its input is unchanged.
    monkeypatch.setattr(oslayer, "Path", FakePath)
    assert L.proc_start_token(123) == "999"


def test_darwin_managed_native_state_accepts_exact_provider_second_without_argv(
    tmp_path, monkeypatch,
):
    monkeypatch.setattr(oslayer.sys, 'platform', 'darwin')
    _meta(tmp_path, 123, procStart='Sun Oct 25 22:02:28 2026', pidDomain='darwin')
    monkeypatch.setattr(L, 'proc_start_token', lambda pid:
                        'darwin:1792965748:005678')
    monkeypatch.setattr(L, 'session_pid_alive', lambda *args:
                        (_ for _ in ()).throw(AssertionError('argv is not identity')))
    state = L.read_managed_claude_native_state(123, 'session-1234', claude_home=tmp_path)
    assert state is not None and state.proc_start == 'Sun Oct 25 22:02:28 2026'
    monkeypatch.setattr(L, 'proc_start_token', lambda pid:
                        'darwin:1792965749:000000')
    assert L.read_managed_claude_native_state(123, 'session-1234', claude_home=tmp_path) is None
    _meta(tmp_path, 123, procStart='Sun Oct 25 22:02:28 2026',
          pidDomain='linux:9c1c63c55c164e25b6cab9ecf2d91080:pid:[4026532223]')
    assert L.read_managed_claude_native_state(123, 'session-1234', claude_home=tmp_path) is None
