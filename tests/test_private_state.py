"""Private-state permissions: state dirs, control sockets, usage cache, CLAUDE_BIN."""
from __future__ import annotations

import json
import os
import socket
import stat

import pytest

import ccx.constants as C
import ccx.controllers.live_vars as LV
import ccx.store.paths as P
import ccx.claude.usage as U


def _mode(path) -> int:
    return stat.S_IMODE(os.lstat(path).st_mode)


def test_private_mkdir_makes_every_created_parent_private(tmp_path):
    old = os.umask(0o022)
    try:
        leaf = P.private_mkdir(tmp_path / "a" / "b" / "c")
    finally:
        os.umask(old)
    for path in (tmp_path / "a", tmp_path / "a" / "b", leaf):
        assert _mode(path) == 0o700, path
    P.private_mkdir(leaf)  # idempotent


# --- bind_private_unix_socket -------------------------------------------------

def test_bind_private_unix_socket_is_0600_without_umask_window(socket_tmp_path):
    tmp_path = socket_tmp_path
    path = tmp_path / "x.sock"
    old = os.umask(0)
    try:
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            P.bind_private_unix_socket(server, path)
            server.listen(1)
            assert stat.S_ISSOCK(os.lstat(path).st_mode)
            assert _mode(path) == 0o600
            # Only the final socket remains: the staging directory is gone.
            assert sorted(p.name for p in tmp_path.iterdir()) == ["x.sock"]
            client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            client.connect(str(path))
            client.close()
        finally:
            server.close()
    finally:
        os.umask(old)


def test_bind_private_unix_socket_cleans_staging_on_failure(socket_tmp_path):
    tmp_path = socket_tmp_path
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.close()
    with pytest.raises(OSError):
        P.bind_private_unix_socket(server, tmp_path / "x.sock")
    assert list(tmp_path.iterdir()) == []


def test_staged_socket_budget_denied_before_mkdtemp(socket_tmp_path, monkeypatch):
    tmp_path = socket_tmp_path
    # A short final filename can fit while the actual "/.XXXXXXXX/s" cannot.
    from ccx.system import oslayer

    pad = (P.oslayer.UNIX_SUN_PATH_BYTES - 8
           - len(os.fsencode(tmp_path)) - 1)
    if pad < 1:
        pytest.skip("tmp_path is too long to form the staged-only limit")
    parent = tmp_path / ("d" * pad)
    parent.mkdir()
    final = parent / "x.sock"
    assert oslayer.fits_unix_path(final)
    assert not oslayer.fits_unix_path(parent / ".XXXXXXXX" / "s")

    def no_create(*args, **kwargs):
        pytest.fail("mkdtemp must not run for an oversized staging path")

    monkeypatch.setattr(P.tempfile, "mkdtemp", no_create)
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
        with pytest.raises(OSError, match="staged Unix socket path exceeds AF_UNIX"):
            P.bind_private_unix_socket(server, final)
    assert list(parent.iterdir()) == []


# --- live_vars.owners_dir -----------------------------------------------------

def test_owners_dir_is_private(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path))
    path = LV.owners_dir()
    # Darwin may legitimately choose /private/tmp/ccx-uUID when the inherited
    # XDG would exceed the complete final/staged socket pathname budget.
    assert path == LV._owners_path()
    assert _mode(path) == 0o700


def test_live_owner_rejects_multibyte_overlong_runtime_before_owner_dir_write(
    tmp_path, monkeypatch,
):
    if P.oslayer.UNIX_SUN_PATH_BYTES == 104:
        pytest.skip("Darwin uses a short private fallback; Linux rejects this XDG")
    # Unicode bytes, not character count, determine the 108-byte Linux budget.
    private_xdg = tmp_path / ("é" * 60)
    private_xdg.mkdir(mode=0o700)
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(private_xdg))
    owner_path = private_xdg / f"ccx-live-vars-{os.getuid()}"
    with pytest.raises(OSError, match="final Unix socket path exceeds AF_UNIX"):
        LV.LiveOwner()
    assert not owner_path.exists()


def test_owners_dir_rejects_symlink(tmp_path, monkeypatch):
    target = tmp_path / "elsewhere"
    target.mkdir()
    (tmp_path / f"ccx-live-vars-{os.getuid()}").symlink_to(target)
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path))
    with pytest.raises(PermissionError):
        LV.owners_dir(tmp_path / f"ccx-live-vars-{os.getuid()}")


def test_owners_dir_rejects_foreign_owner(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path))
    monkeypatch.setattr(LV.os, "getuid", lambda: os.geteuid() + 4242)
    (tmp_path / f"ccx-live-vars-{os.geteuid() + 4242}").mkdir()
    with pytest.raises(PermissionError):
        LV.owners_dir(tmp_path / f"ccx-live-vars-{os.geteuid() + 4242}")


# --- usage cache --------------------------------------------------------------

def test_usage_cache_defaults_under_ccx_root():
    assert C.usage_cache_path() == C.CCX_ROOT / "usage-cache.json"


def test_write_usage_cache_is_private_and_atomic(tmp_path, monkeypatch):
    cache = tmp_path / "state" / "usage-cache.json"
    monkeypatch.setattr(U, "usage_cache_path", lambda: cache)
    U._write_usage_cache({"five_hour": {"utilization": 12}})
    assert _mode(cache) == 0o600
    assert json.loads(cache.read_text()) == {"five_hour": {"utilization": 12}}
    assert [p.name for p in cache.parent.iterdir()] == ["usage-cache.json"]
    assert U._read_usage_cache() == {"five_hour": {"utilization": 12}}
    # Legacy payload-only files cannot authorize a quota cap without fetchedAt.
    assert U._cache_age() is None


def test_write_usage_cache_replaces_planted_symlink(tmp_path, monkeypatch):
    victim = tmp_path / "victim"
    victim.write_text("untouched")
    cache = tmp_path / "usage-cache.json"
    cache.symlink_to(victim)
    monkeypatch.setattr(U, "usage_cache_path", lambda: cache)
    U._write_usage_cache({"five_hour": {"utilization": 1}})
    assert victim.read_text() == "untouched"
    assert not cache.is_symlink()


def test_read_usage_cache_refuses_symlink(tmp_path, monkeypatch):
    planted = tmp_path / "planted"
    planted.write_text(json.dumps({"five_hour": {"utilization": 100}}))
    cache = tmp_path / "usage-cache.json"
    cache.symlink_to(planted)
    monkeypatch.setattr(U, "usage_cache_path", lambda: cache)
    assert U._read_usage_cache() is None
    assert U._cache_age() is None


def test_read_usage_cache_refuses_foreign_owner(tmp_path, monkeypatch):
    cache = tmp_path / "usage-cache.json"
    cache.write_text(json.dumps({"five_hour": {"utilization": 100}}))
    monkeypatch.setattr(U, "usage_cache_path", lambda: cache)
    monkeypatch.setattr(U.os, "getuid", lambda: os.geteuid() + 4242)
    assert U._read_usage_cache() is None
    assert U._cache_age() is None


def test_read_usage_cache_refuses_a_fifo_without_blocking(tmp_path, monkeypatch):
    cache = tmp_path / "usage-cache.json"
    os.mkfifo(cache)
    monkeypatch.setattr(U, "usage_cache_path", lambda: cache)
    assert U._read_usage_cache() is None
    assert U._cache_age() is None


# --- CLAUDE_BIN ---------------------------------------------------------------

def test_claude_bin_falls_back_to_path(tmp_path, monkeypatch):
    home = tmp_path / "home"
    home.mkdir()
    bindir = tmp_path / "bin"
    bindir.mkdir()
    claude = bindir / "claude"
    claude.write_text("#!/bin/sh\n")
    claude.chmod(0o755)
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("PATH", str(bindir))
    assert C._default_claude_bin() == str(claude)


def test_claude_bin_prefers_native_install(tmp_path, monkeypatch):
    home = tmp_path / "home"
    native = home / ".local" / "bin" / "claude"
    native.parent.mkdir(parents=True)
    native.write_text("#!/bin/sh\n")
    monkeypatch.setenv("HOME", str(home))
    assert C._default_claude_bin() == str(native)


def test_claude_bin_keeps_native_path_when_nothing_found(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("PATH", str(tmp_path / "empty"))
    assert C._default_claude_bin() == str(tmp_path / ".local" / "bin" / "claude")



# --- peer credentials -----------------------------------------------------------

@pytest.mark.skipif(not hasattr(socket, "SO_PEERCRED"), reason="Linux SO_PEERCRED")
def test_peer_check_accepts_this_user_and_drops_another(socket_tmp_path, monkeypatch):
    tmp_path = socket_tmp_path
    path = tmp_path / "p.sock"
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    P.bind_private_unix_socket(server, path)
    server.listen(2)
    try:
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client.connect(str(path))
        conn = P.accept_same_user(server)
        assert conn is not None
        conn.close()
        client.close()

        real_uid = os.getuid()
        monkeypatch.setattr(P.os, "getuid", lambda: real_uid + 1)
        other = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        other.connect(str(path))
        assert P.accept_same_user(server) is None
        other.settimeout(2)
        assert other.recv(1) == b""  # the server closed it
        other.close()
    finally:
        server.close()


def test_accept_same_user_closes_connection_without_kernel_peer_option(socket_tmp_path, monkeypatch):
    tmp_path = socket_tmp_path
    path = tmp_path / "no-creds.sock"
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
        P.bind_private_unix_socket(server, path)
        server.listen(1)
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.connect(str(path))
            monkeypatch.setattr(P.oslayer, "peer_credentials", lambda _conn: (
                _ for _ in ()).throw(P.oslayer.PeerIdentityUnavailable(
                    "kernel did not supply usable credentials")))
            assert P.accept_same_user(server) is None
            client.settimeout(2)
            assert client.recv(1) == b""
