"""Only per-root daemon logs rotate on start; no real processes or providers."""
from types import SimpleNamespace

import pytest

from ccx import constants
from ccx.codex import runtime
from ccx.notify import daemon as notifyd
from ccx.notify import paths as notify_paths
from ccx.store.root import require_ccx_active
from test_generation_store import short_root  # noqa: F401

LIMIT = 10 * 1024 * 1024


@pytest.mark.parametrize("size,rotates", [(LIMIT, False), (LIMIT + 1, True)])
def test_codex_daemon_log_rotates_only_when_oversized(short_root, monkeypatch, size, rotates):
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    store = require_ccx_active(short_root)
    with store.lease():
        with store._gen("runtime-metadata", create=True):
            pass
    path = store.pointer.generation_root / "runtime-metadata" / "codexd.log"
    path.touch(mode=0o600)
    path.chmod(0o600)
    path.open("r+b").close()
    with path.open("r+b") as stream:
        stream.truncate(size)
    previous = path.with_name("codexd.log.1")
    previous.write_text("older")
    with runtime._open_daemon_log() as stream:
        stream.write("new line\n")
    assert previous.stat().st_size == (size if rotates else len("older"))
    assert path.stat().st_size == (len("new line\n") if rotates else size + len("new line\n"))
    assert path.stat().st_mode & 0o777 == 0o600
    if rotates:
        assert path.read_bytes() == b"new line\n"



@pytest.mark.parametrize("size,rotates", [(LIMIT, False), (LIMIT + 1, True)])
def test_notifyd_log_rotates_before_mocked_spawn(tmp_path, monkeypatch, size, rotates):
    root = tmp_path / "ccx"
    root.mkdir(mode=0o700)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    notify_paths.ensure_dirs()
    log = notify_paths.daemon_log_path()
    log.touch(mode=0o600)
    with log.open("r+b") as stream:
        stream.truncate(size)
    old = log.with_name("notifyd.log.1")
    old.write_text("old")
    seen = {"started": False}
    monkeypatch.setattr(notifyd, "_notifyd_alive", lambda: seen["started"])
    monkeypatch.setattr(notifyd, "_retire_incompatible_notifyd", lambda **_kw: False)

    def fake_spawn(*_args, **kwargs):
        kwargs["stdout"].write("fake notified\n")
        kwargs["stdout"].flush()
        seen["started"] = True
        return SimpleNamespace(wait=lambda: None, poll=lambda: None)

    monkeypatch.setattr(notifyd.subprocess, "Popen", fake_spawn)
    notifyd.ensure_notify_daemon(timeout=1)
    assert seen["started"]
    assert old.stat().st_size == (size if rotates else len("old"))
    assert log.stat().st_size == (len("fake notified\n") if rotates else size + len("fake notified\n"))
    if rotates:
        assert log.read_text() == "fake notified\n"
