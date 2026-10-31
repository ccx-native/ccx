"""Darwin OS evidence contracts with injected readers; not real-Mac ABI proof."""
from __future__ import annotations
import ccx.cli.commands.control as commands_control

import ctypes
import errno
import os
from pathlib import Path
import socket
import stat
import sys
from types import SimpleNamespace

import pytest

from ccx.store import liveness
from ccx.system import oslayer


def _bsdinfo(pid=123, *, status=2, parent=42, uid=501, sec=1792872000, usec=1234):
    value = oslayer._DarwinBSDInfo()
    value.pbi_pid, value.pbi_ppid, value.pbi_uid = pid, parent, uid
    value.pbi_status = status
    value.pbi_start_tvsec, value.pbi_start_tvusec = sec, usec
    return value


def _inject_proc(monkeypatch, value, *, returned=None, error=0):
    def fake(pid, destination):
        ctypes.memmove(ctypes.byref(destination), ctypes.byref(value), ctypes.sizeof(value))
        return (ctypes.sizeof(value) if returned is None else returned), error
    monkeypatch.setattr(oslayer, "_darwin_pidinfo_call", fake)


def test_darwin_proc_bsdinfo_sdk_shape_injection_and_original_start(monkeypatch):
    assert ctypes.sizeof(oslayer._DarwinBSDInfo) == 136  # SDK validation is CP2
    assert oslayer._DarwinBSDInfo.pbi_start_tvsec.offset == 120
    info = _bsdinfo()
    _inject_proc(monkeypatch, info)
    observed = oslayer._darwin_process_info(123)
    assert observed == oslayer.ProcessInfo(123, "alive", 42, "R", "darwin:1792872000:001234")
    monkeypatch.setattr(oslayer, "process_info", oslayer._darwin_process_info)
    assert liveness.owned_process(123, observed.start_token) == "alive"
    assert liveness.owned_process(123, "darwin:1792872000:001235") == "dead"
    assert liveness.owned_process(123, "1792872000") == "dead"  # no unit conversion


@pytest.mark.parametrize("status,expected", [(1, "alive"), (3, "alive"), (4, "alive"),
                                               (5, "dead"), (0, "unprovable"), (99, "unprovable")])
def test_darwin_process_state_and_zombie(monkeypatch, status, expected):
    _inject_proc(monkeypatch, _bsdinfo(status=status))
    result = oslayer._darwin_process_info(123)
    assert result.presence == expected
    if status == 5:
        assert result.start_token is None and result.state == "Z"


@pytest.mark.parametrize("scenario", ["truncated", "different-pid", "missing-start",
                                      "usec-overflow", "permission", "ambiguous",
                                      "missing", "invalid-uid", "wrong-call"])
def test_darwin_proc_inconclusive_never_authorizes(monkeypatch, scenario):
    record = _bsdinfo()
    if scenario == "different-pid": record.pbi_pid = 124
    if scenario == "missing-start": record.pbi_start_tvsec = 0
    if scenario == "usec-overflow": record.pbi_start_tvusec = 1_000_000
    if scenario == "invalid-uid": record.pbi_uid = 0xffffffff
    if scenario == "wrong-call":
        monkeypatch.setattr(oslayer, "_darwin_pidinfo_call", lambda pid, dest: (_ for _ in ()).throw(OSError("no libproc")))
    else:
        sizes = {"truncated": 40, "permission": -1, "ambiguous": 0, "missing": -1}
        errors = {"permission": errno.EPERM, "missing": errno.ESRCH}
        _inject_proc(monkeypatch, record, returned=sizes.get(scenario), error=errors.get(scenario, 0))
    outcome = oslayer._darwin_process_info(123)
    assert outcome.presence == ("dead" if scenario == "missing" else "unprovable")
    assert outcome.start_token is None


def _xucred(*, version=0, uid=501, ngroups=1):
    identity = oslayer._DarwinXucred()
    identity.cr_version, identity.cr_uid, identity.cr_ngroups = version, uid, ngroups
    identity.cr_groups[0] = 20
    return ctypes.string_at(ctypes.byref(identity), ctypes.sizeof(identity))


@pytest.mark.parametrize("invalid", [None, "short-cred", "short-pid", "wrong-version",
                                           "bad-uid", "bad-group-count", "zero-pid", "socket-error"])
def test_darwin_peer_uses_both_kernel_options_on_same_socket(invalid):
    assert ctypes.sizeof(oslayer._DarwinXucred) == 76  # verify on real Monterey SDK in CP2
    class Connection:
        calls = []
        def getsockopt(self, level, option, length):
            self.calls.append((level, option, length))
            if invalid == "socket-error" or (invalid == "short-pid" and option == 2):
                if invalid == "socket-error": raise OSError("option unavailable")
                return b"\0"
            if option == 1:
                raw = _xucred(version=1 if invalid == "wrong-version" else 0,
                              uid=0xffffffff if invalid == "bad-uid" else 501,
                              ngroups=17 if invalid == "bad-group-count" else 1)
                return raw[:8] if invalid == "short-cred" else raw
            return ctypes.c_int32(0 if invalid == "zero-pid" else 123).value.to_bytes(4, "little", signed=True)
    connected = Connection()
    if invalid is None:
        assert oslayer._darwin_peer_credentials(connected) == oslayer.PeerCredentials(501, 123, 20)
    else:
        with pytest.raises(oslayer.PeerIdentityUnavailable):
            oslayer._darwin_peer_credentials(connected)
    assert connected.calls[0] == (0, 1, 76)
    if invalid not in ("short-cred", "socket-error"):
        assert connected.calls[1] == (0, 2, 4)


def test_darwin_read_argv_none_and_sun_path_budget(monkeypatch):
    assert oslayer._darwin_read_argv(123) is None
    monkeypatch.setattr(oslayer, "UNIX_SUN_PATH_BYTES", 104)
    assert oslayer.fits_unix_path("a" * 103)
    assert not oslayer.fits_unix_path("a" * 104)
    assert oslayer.fits_unix_path("é" * 51)
    assert not oslayer.fits_unix_path("é" * 52)
    assert liveness.proc_argv(None) is None


def test_darwin_root_open_nofollow_and_original_body_error(tmp_path, monkeypatch):
    flags_seen = []
    real_open = os.open
    def checked_open(path, flags, *args, **kwargs):
        assert flags & os.O_NOFOLLOW and flags & os.O_DIRECTORY
        assert not flags & getattr(os, "O_PATH", 0)
        flags_seen.append(flags)
        return real_open(path, flags, *args, **kwargs)
    monkeypatch.setattr(oslayer.os, "open", checked_open)
    parent = tmp_path / "nested"
    parent.mkdir()
    with oslayer._darwin_open_root_dir(parent) as (path, fd):
        assert path == parent and os.fstat(fd).st_ino == parent.stat().st_ino
    with pytest.raises(OSError): os.fstat(fd)
    assert flags_seen
    symlink = tmp_path / "redirect"
    symlink.symlink_to(parent, target_is_directory=True)
    with pytest.raises(oslayer.RootOpenError):
        with oslayer._darwin_open_root_dir(symlink): pytest.fail("followed symlink")
    with pytest.raises(OSError, match="body-error"):
        with oslayer._darwin_open_root_dir(parent): raise OSError("body-error")


@pytest.mark.parametrize('old_kernel', [False, True])
def test_darwin_root_search_flag_for_ancestors_not_leaf(tmp_path, monkeypatch, old_kernel):
    search = 0x20000000  # synthetic distinct bit for Linux test hosts
    monkeypatch.setattr(oslayer.os, 'O_SEARCH', search, raising=False)
    original_open = os.open
    calls = []

    def proxy(path, flags, *args, **kwargs):
        calls.append((str(path), bool(flags & search)))
        if flags & search:
            if old_kernel:
                raise OSError(errno.EINVAL, 'O_SEARCH unavailable on old kernel')
            return original_open(path, flags & ~search, *args, **kwargs)
        return original_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(oslayer.os, 'open', proxy)
    leaf = tmp_path / 'parent' / 'root'
    leaf.mkdir(parents=True)
    with oslayer._darwin_open_root_dir(leaf) as (_, fd):
        assert os.fstat(fd).st_ino == leaf.stat().st_ino
    assert calls[-1] == ('root', False)
    assert any(use_search for _, use_search in calls[:-1])
    if old_kernel:
        assert any(not use_search for _, use_search in calls[:-1])


def test_darwin_short_runtime_choice_no_planner_writes(monkeypatch, socket_tmp_path):
    tmp_path = socket_tmp_path
    fake_private = tmp_path / "private-XDG"
    fake_private.mkdir()
    st_root = SimpleNamespace(st_mode=stat.S_IFDIR | 0o1777, st_uid=0)
    st_private = SimpleNamespace(st_mode=stat.S_IFDIR | 0o700, st_uid=os.getuid())
    real_lstat = os.lstat
    def controlled(path, *args, **kwargs):
        if str(path) == "/private/tmp": return st_root
        if str(path) == str(fake_private): return st_private
        if str(path) == f"/private/tmp/ccx-u{os.getuid()}": raise FileNotFoundError
        return real_lstat(path, *args, **kwargs)
    monkeypatch.setattr(oslayer.os, "lstat", controlled)
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(fake_private))
    assert oslayer._darwin_runtime_base(purpose="transport", suffix_bytes=1).path == fake_private
    before = set(tmp_path.iterdir())
    # Deep XDG becomes too long, but the fixed /private/tmp UID leaf fits.
    selected = oslayer._darwin_runtime_base(
        purpose="transport",
        suffix_bytes=105 - len(os.fsencode(fake_private)),
    )
    assert selected == oslayer.RuntimeChoice(Path(f"/private/tmp/ccx-u{os.getuid()}"), True)
    assert before == set(tmp_path.iterdir())
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path / "missing"))
    assert oslayer._darwin_runtime_base(purpose="live_vars", suffix_bytes=60).may_create
    monkeypatch.setattr(oslayer.os, "lstat", lambda path, *args, **kwargs: SimpleNamespace(
        st_mode=stat.S_IFLNK | 0o777, st_uid=0) if str(path) == "/private/tmp"
        else controlled(path, *args, **kwargs))
    with pytest.raises(OSError, match="sticky"):
        oslayer._darwin_runtime_base(purpose="transport", suffix_bytes=40)


def test_darwin_devfd_reopen_identity_and_offset(tmp_path, monkeypatch):
    path = tmp_path / "original.jsonl"
    path.write_bytes(b"old original bytes\n")
    fd = os.open(path, os.O_RDONLY)
    try:
        os.lseek(fd, 3, os.SEEK_SET)
        if sys.platform == "darwin":
            with pytest.raises(OSError, match="shares mutable source offset"):
                oslayer._darwin_pinned_fd_path(fd)
            from ccx.claude.transcript import PinnedNativeSource
            reader = PinnedNativeSource(fd).open_reader()
        else:
            alias = oslayer._darwin_pinned_fd_path(fd)
            reader = open(alias, "rb")
        assert os.lseek(fd, 0, os.SEEK_CUR) == 3
        path.unlink()
        path.write_bytes(b"replaced bytes\n")
        with reader as stream:
            assert stream.read() == b"old original bytes\n"
        assert os.lseek(fd, 0, os.SEEK_CUR) == 3
        unrelated = tmp_path / "stranger.jsonl"
        unrelated.write_bytes(b"different")
        real_open = os.open
        monkeypatch.setattr(oslayer.os, "open", lambda name, flags: real_open(unrelated, flags))
        with pytest.raises(OSError, match="original"):
            oslayer._darwin_pinned_fd_path(fd)
    finally:
        os.close(fd)


class _ExitNow(BaseException):
    pass


@pytest.mark.parametrize("failure", ["none", "already-lost", "during-start", "start-fails", "not-alive"])
def test_darwin_parent_watcher_setup_fails_closed(monkeypatch, failure):
    parent_ids = iter(([1] if failure == "already-lost" else
                       [123, 1] if failure == "during-start" else [123, 123]))
    monkeypatch.setattr(oslayer.os, "getppid", lambda: next(parent_ids))
    monkeypatch.setattr(oslayer.os, "_exit", lambda code: (_ for _ in ()).throw(_ExitNow()))
    class Thread:
        def __init__(self, *, target, args, daemon, name):
            assert daemon and args == (123,)
        def start(self):
            if failure == "start-fails": raise RuntimeError("not armed")
        def is_alive(self): return failure != "not-alive"
    monkeypatch.setattr(oslayer.threading, "Thread", Thread)
    if failure == "none":
        oslayer._darwin_arm_parent_death(123)
    else:
        with pytest.raises(_ExitNow):
            oslayer._darwin_arm_parent_death(123)


def test_darwin_parent_watcher_exits_on_late_loss(monkeypatch):
    parents = iter([123, 123, 1])
    monkeypatch.setattr(oslayer.os, "getppid", lambda: next(parents))
    monkeypatch.setattr(oslayer.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(oslayer.os, "_exit", lambda code: (_ for _ in ()).throw(_ExitNow()))
    with pytest.raises(_ExitNow):
        oslayer._darwin_parent_watch(123)


@pytest.mark.skipif(
    sys.platform == "darwin",
    reason="synthetic runtime-base paths; real Mac checklist exercises runtime creation",
)
def test_darwin_short_transport_plan_is_read_only_until_active(
    tmp_path, socket_tmp_path, monkeypatch,
):
    from ccx.rpc import transport
    from ccx.store.root import require_ccx_active
    from test_generation_store import A, add_worker, fixture_root

    store_root = fixture_root(tmp_path / ("long-" * 27))
    add_worker(store_root, A)
    store = require_ccx_active(store_root)
    short = socket_tmp_path / "runtime"
    assert not short.exists()
    calls = []
    def selected(*, purpose, suffix_bytes=0):
        assert purpose == "transport" and suffix_bytes > 0
        return oslayer.RuntimeChoice(short, not short.exists())
    def private_create(path, choice):
        calls.append((path, choice))
        path.mkdir(mode=0o700)
    monkeypatch.setattr(oslayer, "runtime_base", selected)
    monkeypatch.setattr(oslayer, "UNIX_SUN_PATH_BYTES", 104)
    monkeypatch.setattr(transport, "_ensure_private_runtime_directory", private_create)
    plan = transport.plan_transport(store, target=A)
    assert plan.fallback and plan.parent.parent == short
    assert not short.exists() and not plan.parent.exists()
    transport.ensure_transport_parent(store, plan)
    assert calls == [(short, oslayer.RuntimeChoice(short, True))]
    assert short.exists() and plan.parent.exists()
    assert oslayer.fits_unix_path(plan.final)
    assert oslayer.fits_unix_path(plan.parent / ".XXXXXXXX" / "s")


@pytest.mark.skipif(
    sys.platform == "darwin",
    reason="synthetic owner runtime path; real Mac checklist exercises runtime creation",
)
def test_darwin_live_owner_selects_short_base_before_writes(
    socket_tmp_path, monkeypatch,
):
    from ccx.controllers import live_vars
    from ccx.rpc import transport
    base = socket_tmp_path / "runtime"
    supplied = []
    def selected(*, purpose, suffix_bytes=0):
        assert purpose == "live_vars" and suffix_bytes >= 40
        supplied.append(suffix_bytes)
        return oslayer.RuntimeChoice(base, not base.exists())
    monkeypatch.setattr(oslayer, "runtime_base", selected)
    monkeypatch.setattr(oslayer, "UNIX_SUN_PATH_BYTES", 104)
    monkeypatch.setattr(transport, "_ensure_private_runtime_directory",
                        lambda path, choice: path.mkdir(mode=0o700, parents=True))
    basename = "123-" + "a" * 32 + ".sock"
    directory, choice = live_vars._owner_runtime(basename)
    candidate = directory / basename
    assert choice.may_create and not base.exists()
    assert oslayer.fits_unix_path(candidate)
    assert oslayer.fits_unix_path(directory / ".XXXXXXXX" / "s")
    assert live_vars.owners_dir(directory, runtime=choice) == directory
    assert directory.exists() and stat.S_IMODE(directory.stat().st_mode) == 0o700
    assert supplied


@pytest.mark.parametrize("base_kind", ("symlink", "wrong-owner", "permissive"))
def test_darwin_existing_short_runtime_refuses_untrusted_leaf(monkeypatch, base_kind):
    parent = SimpleNamespace(st_mode=stat.S_IFDIR | 0o1777, st_uid=0)
    leaf_mode = (stat.S_IFLNK | 0o777 if base_kind == "symlink" else
                 stat.S_IFDIR | 0o755 if base_kind == "permissive" else
                 stat.S_IFDIR | 0o700)
    leaf_uid = (os.getuid() + 1 if base_kind == "wrong-owner" else os.getuid())
    base = f"/private/tmp/ccx-u{os.getuid()}"
    def lstat(path):
        if str(path) == "/private/tmp": return parent
        if str(path) == base:
            return SimpleNamespace(st_mode=leaf_mode, st_uid=leaf_uid)
        raise AssertionError(path)
    monkeypatch.delenv("XDG_RUNTIME_DIR", raising=False)
    monkeypatch.setattr(oslayer.os, "lstat", lstat)
    with pytest.raises(OSError, match="private and user-owned"):
        oslayer._darwin_runtime_base(purpose="transport", suffix_bytes=40)


def test_darwin_short_runtime_creation_requires_trusted_sticky_parent(
    monkeypatch, socket_tmp_path,
):
    from contextlib import contextmanager
    from ccx.rpc import transport

    fd = os.open(socket_tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    @contextmanager
    def fake_pinned(path):
        assert str(path) == "/private/tmp"
        yield path, fd
    monkeypatch.setattr(transport.oslayer, "open_root_dir", fake_pinned)
    try:
        candidate = oslayer.RuntimeChoice(
            Path(f"/private/tmp/ccx-u{os.getuid()}"), may_create=True)
        with pytest.raises(transport.TransportError, match="sticky"):
            transport._ensure_private_runtime_directory(candidate.path, candidate)
        assert not (socket_tmp_path / candidate.path.name).exists()
    finally:
        os.close(fd)


@pytest.mark.parametrize('provider,token,expected', [
    ('Sun Oct 25 22:02:28 2026', 'darwin:1792965748:000000', True),
    ('Sun Oct 25 22:02:28 2026', 'darwin:1792965748:999999', True),
    ('Sun Oct  5 22:02:28 2025', 'darwin:1759701748:001234', True),
    ('Sun Oct 25 22:02:29 2026', 'darwin:1792965748:000000', False),
    ('Sun Oct 25 22:02:28 2026 UTC', 'darwin:1792965748:001234', False),
    ('Mon Oct 25 22:02:28 2026', 'darwin:1792965748:001234', False),
    ('Sun Oct 25 22:02:28 2026', '1792965748', False),
    ('Sun Oct 25 22:02:28 2026', 'darwin:1792965748', False),
    ('Sun Oct 25 22:02:28 2026', 'darwin:1792965748:1000000', False),
    (1792965748, 'darwin:1792965748:001234', False),
    ('garbage', 'darwin:1792965748:001234', False),
    (None, 'darwin:1792965748:001234', False),
])
def test_claude_darwin_procstart_real_C_UTC_sample(
    monkeypatch, provider, token, expected,
):
    # Probe documented by owner on a separate real live-session observation.
    monkeypatch.setattr(oslayer.sys, 'platform', 'darwin')
    assert oslayer.provider_start_matches(provider, token, "darwin") is expected


@pytest.mark.parametrize("platform,provider,kernel,domain,expected", [
    ("linux", "123456", "123456",
     "linux:9c1c63c55c164e25b6cab9ecf2d91080:pid:[4026532223]", True),
    ("linux", "123456", "123456", "darwin", False),
    ("linux", "123456", "123456", None, True),
    ("darwin", "Sun Oct 25 22:02:28 2026",
     "darwin:1792965748:123456", "darwin", True),
    ("darwin", "Sun Oct 25 22:02:28 2026",
     "darwin:1792965748:123456",
     "linux:9c1c63c55c164e25b6cab9ecf2d91080:pid:[4026532223]", False),
    ("darwin", "Sun Oct 25 22:02:28 2026",
     "darwin:1792965748:123456", None, True),
])
def test_provider_pid_domain_matches_only_real_OS_prefix(
    monkeypatch, platform, provider, kernel, domain, expected,
):
    monkeypatch.setattr(oslayer.sys, "platform", platform)
    assert oslayer.provider_start_matches(provider, kernel, domain) is expected


def test_linux_provider_start_still_exact_decimal_tick(monkeypatch):
    monkeypatch.setattr(oslayer.sys, 'platform', 'linux')
    assert oslayer.provider_start_matches('000987654321', '000987654321')
    assert oslayer.provider_start_matches(
        '000987654321', '000987654321',
        'linux:9c1c63c55c164e25b6cab9ecf2d91080:pid:[4026532223]')
    assert not oslayer.provider_start_matches('987654321', '000987654321')
    assert not oslayer.provider_start_matches(True, '1')
    assert not oslayer.provider_start_matches('Sun Oct 25 22:02:28 2026', '000987654321')


@pytest.mark.skipif(sys.platform == "darwin", reason="Linux runtime selection only")
def test_linux_runtime_choice_always_typed_no_write(monkeypatch, socket_tmp_path):
    monkeypatch.setattr(oslayer.sys, 'platform', 'linux')
    monkeypatch.setenv('XDG_RUNTIME_DIR', str(socket_tmp_path))
    before = list(socket_tmp_path.iterdir())
    assert oslayer.runtime_base(purpose='transport') == oslayer.RuntimeChoice(socket_tmp_path)
    assert oslayer.runtime_base(purpose='live_vars') == oslayer.RuntimeChoice(socket_tmp_path)
    assert list(socket_tmp_path.iterdir()) == before
    monkeypatch.delenv('XDG_RUNTIME_DIR')
    runtime = oslayer.runtime_base(purpose='transport')
    assert runtime.path == Path(f'/tmp/ccx-u{os.getuid()}')
    assert isinstance(runtime.may_create, bool)
    assert not oslayer.runtime_base(purpose='live_vars').may_create


@pytest.mark.skipif(sys.platform != 'linux', reason='Linux short runtime fallback')
@pytest.mark.parametrize('invalid', ['symlink', 'permissive', 'other-owner', 'unsticky-parent'])
def test_linux_short_runtime_refuses_untrusted_leaf(monkeypatch, invalid):
    parent = SimpleNamespace(st_mode=stat.S_IFDIR | (0o755 if invalid == 'unsticky-parent' else 0o1777), st_uid=0)
    leaf = SimpleNamespace(
        st_mode=(stat.S_IFLNK | 0o777 if invalid == 'symlink' else
                 stat.S_IFDIR | (0o755 if invalid == 'permissive' else 0o700)),
        st_uid=os.getuid() + (1 if invalid == 'other-owner' else 0),
    )
    def read_stat(path):
        if str(path) == '/tmp':
            return parent
        assert str(path) == f'/tmp/ccx-u{os.getuid()}'
        return leaf

    monkeypatch.delenv('XDG_RUNTIME_DIR', raising=False)
    monkeypatch.setattr(oslayer.os, 'lstat', read_stat)
    with pytest.raises(OSError, match='sticky|private and user-owned'):
        oslayer.runtime_base(purpose='transport', suffix_bytes=65)


@pytest.mark.skipif(sys.platform != 'linux', reason='Linux AF_UNIX transport')
def test_linux_long_root_without_xdg_binds_private_short_socket(tmp_path, monkeypatch):
    from ccx.rpc import transport
    from ccx.store.root import require_ccx_active
    from ccx.store.paths import bind_private_unix_socket
    from test_generation_store import A, add_worker, fixture_root

    monkeypatch.delenv('XDG_RUNTIME_DIR', raising=False)
    root = fixture_root(tmp_path / ('long-root-' * 19))
    add_worker(root, A)
    store = require_ccx_active(root)
    base = Path(f'/tmp/ccx-u{os.getuid()}')
    existed = base.exists()
    plan = transport.plan_transport(store, target='claude:' + A)
    assert not oslayer.fits_unix_path(store.root / 'run' / (A + '.sock'))
    assert plan.fallback and plan.parent.parent == base
    assert not plan.parent.exists()  # planning cannot mkdir a runtime leaf
    try:
        transport.ensure_transport_parent(store, plan)
        assert stat.S_IMODE(base.stat().st_mode) == 0o700
        assert stat.S_IMODE(plan.parent.stat().st_mode) == 0o700
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            bind_private_unix_socket(server, plan.final)
            assert stat.S_IMODE(plan.final.stat().st_mode) == 0o600
            server.listen(1)
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                client.connect(str(plan.final))
                with server.accept()[0] as conn:
                    assert oslayer.peer_credentials(conn).uid == os.getuid()
                    client.sendall(b'ping')
                    assert conn.recv(4) == b'ping'
    finally:
        plan.final.unlink(missing_ok=True)
        if plan.parent.is_dir():
            plan.parent.rmdir()  # only the uniquely hashed test-owned child
        if not existed and base.is_dir():
            try:
                base.rmdir()  # only if still empty; never remove another owner's content
            except OSError:
                pass


def test_darwin_libproc_is_loaded_only_once(monkeypatch):
    records = []
    class Library:
        def __init__(self):
            self.proc_pidinfo = lambda *args: 136
    monkeypatch.setattr(oslayer.ctypes, 'CDLL', lambda *args, **kwargs:
                        records.append((args, kwargs)) or Library())
    oslayer._darwin_proc_pidinfo.cache_clear()
    try:
        assert oslayer._darwin_proc_pidinfo() is oslayer._darwin_proc_pidinfo()
        assert len(records) == 1
        assert records[0][0] == ('/usr/lib/libproc.dylib',)
    finally:
        oslayer._darwin_proc_pidinfo.cache_clear()


def assert_injected_darwin_identity_blocks_signals(tmp_path, monkeypatch, failure):
    """Use real CLI command handlers, disposable records and injected libproc."""
    from types import SimpleNamespace
    from contextlib import redirect_stdout
    from io import StringIO
    import json
    from ccx import constants
    from ccx.cli import controller as controller_commands
    from ccx.controllers import store as controller_store
    from ccx.store.root import require_ccx_active
    from test_generation_store import A, add_worker, fixture_root
    from test_controller_cli import _run

    root = fixture_root(tmp_path)
    monkeypatch.setattr(constants, 'CCX_ROOT', root)
    add_worker(root, A)
    store = require_ccx_active(root)
    pid = os.getpid()  # an actual live PID; never signal it
    start = (oslayer.process_info(pid).start_token if sys.platform == 'darwin'
             else 'darwin:1792965748:000123')
    assert start and start.startswith('darwin:')
    store.mutate_record(A, patches={
        'lifecycle.state': 'idle',
        'lifecycle.hostPid': pid, 'lifecycle.hostStart': start,
        'lifecycle.workerPid': None, 'lifecycle.workerStart': None,
    })
    _script, run_id, run_dir = _run(tmp_path)
    controller_store.mutate_run(run_dir, lambda record: record['lifecycle'].update(
        state='running', pid=pid, pidStart=start))

    def injected_libproc(requested, info):
        if failure == 'eperm':
            return -1, errno.EPERM
        observed = oslayer._DarwinBSDInfo()
        observed.pbi_pid = requested
        observed.pbi_status = 2
        observed.pbi_uid = os.getuid()
        observed.pbi_start_tvsec = int(start.split(':')[1]) + 1
        observed.pbi_start_tvusec = int(start.split(':')[2])
        ctypes.memmove(ctypes.byref(info), ctypes.byref(observed), ctypes.sizeof(info))
        return ctypes.sizeof(info), 0

    signals = []
    def forbid_signal(target, sig):
        signals.append((target, sig))
        pytest.fail('unproven PID must never receive a signal')

    with monkeypatch.context() as controlled:
        controlled.setattr(oslayer, '_darwin_pidinfo_call', injected_libproc)
        if sys.platform != 'darwin':
            controlled.setattr(oslayer, 'process_info', oslayer._darwin_process_info)
        controlled.setattr(os, 'kill', forbid_signal)
        controlled.setattr(controller_commands, '_spawn_controller', lambda **_: pytest.fail(
            'unproven process must never be revived'))
        with pytest.raises(SystemExit) as stopped:
            controller_commands.cmd_stop(SimpleNamespace(target=run_id, force=True, json=True))
        assert stopped.value.code != 0
        if failure == 'eperm':
            with pytest.raises(SystemExit) as killed:
                commands_control.cmd_kill(SimpleNamespace(target='claude:' + A, timeout=0.5, json=True))
            assert killed.value.code != 0
        else:
            # A mismatched PID/start *proves the original is gone*: terminalizing
            # its historical record is safe, but signalling/reviving is not.
            output = StringIO()
            with redirect_stdout(output):
                commands_control.cmd_kill(SimpleNamespace(target='claude:' + A, timeout=0.5, json=True))
            result = json.loads(output.getvalue())
            assert result['state'] == 'stopped'
            assert result['resume_available'] is False

    assert not signals
    assert controller_store.read_json(controller_store.run_record_path(run_dir))[
        'lifecycle']['state'] != 'stopped'
    # No new host activation, PID rewrite or fake resume on a denied identity.
    assert store.record(A)['lifecycle']['hostPid'] == pid
    assert store.record(A)['lifecycle']['hostStart'] == start


@pytest.mark.skipif(sys.platform == 'darwin', reason='real Darwin CLI proof in Mac checklist')
@pytest.mark.parametrize('failure', ['mismatch', 'eperm'])
def test_injected_darwin_libproc_denial_reaches_cli_stop_kill_without_signal(
    tmp_path, monkeypatch, failure,
):
    assert_injected_darwin_identity_blocks_signals(tmp_path, monkeypatch, failure)
