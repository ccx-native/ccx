"""CP2 fake-provider-safe real Mac checks; executed only through mac-tests/run.sh.

No credentials, real provider CLIs, or network are accessed. Child processes and
socket endpoints are test-owned disposable resources only.
"""
from __future__ import annotations

import ctypes
import os
from pathlib import Path
import platform
import shlex
import signal
import socket
import stat
import subprocess
import sys
import time

import pytest

from ccx.system import oslayer
from ccx.store.paths import accept_same_user, bind_private_unix_socket, _require_unix_socket_budget

pytestmark = pytest.mark.skipif(sys.platform != "darwin", reason="real Mac only")


def test_cp2_1_sdk_and_runtime_facts(capsys):
    print('macOS', platform.mac_ver(), 'arch', platform.machine(), 'Python', sys.version)
    print('uname', os.uname(), 'pid', os.getpid(), 'uid', os.getuid())
    print('proc_bsdinfo', ctypes.sizeof(oslayer._DarwinBSDInfo),
          'start_offset', oslayer._DarwinBSDInfo.pbi_start_tvsec.offset)
    print('xucred', ctypes.sizeof(oslayer._DarwinXucred),
          'pid_t', ctypes.sizeof(ctypes.c_int32))
    print('Darwin flags', {name: getattr(os, name, None) for name in
                           ('O_SEARCH', 'O_DIRECTORY', 'O_NOFOLLOW', 'O_CLOEXEC')})
    print('local socket constants', {name: getattr(socket, name, None) for name in
                                     ('SOL_LOCAL', 'LOCAL_PEERCRED', 'LOCAL_PEERPID')})
    print('backend constants', oslayer._DARWIN_PROC_PIDTBSDINFO,
          oslayer._DARWIN_SOL_LOCAL, oslayer._DARWIN_LOCAL_PEERCRED,
          oslayer._DARWIN_LOCAL_PEERPID)
    assert ctypes.sizeof(oslayer._DarwinBSDInfo) == 136
    assert ctypes.sizeof(oslayer._DarwinXucred) == 76
    assert oslayer.UNIX_SUN_PATH_BYTES == 104


def test_cp2_2_live_original_child_start_and_proven_exit():
    current = oslayer.process_info(os.getpid())
    print('SELF', current)
    assert current.presence == 'alive' and current.pid == os.getpid()
    assert current.start_token and current.start_token.startswith('darwin:')
    process = subprocess.Popen([sys.executable, '-c', 'import time;time.sleep(0.5)'])
    try:
        info = oslayer.process_info(process.pid)
        print('CHILD', info)
        assert info.presence == 'alive'
        assert info.ppid == os.getpid()
        assert oslayer.process_info(process.pid).start_token == info.start_token
    finally:
        process.wait(timeout=5)
    departed = oslayer.process_info(process.pid)
    print('EXIT', departed)
    assert departed.presence == 'dead'


def test_cp2_3_real_two_process_same_stream_peer_and_flock(socket_tmp_path):
    path = socket_tmp_path / 'peer.sock'
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(path))
    server.listen(1)
    child = subprocess.Popen([sys.executable, '-c',
                              'import socket,sys,time;s=socket.socket(socket.AF_UNIX);'
                              's.connect(sys.argv[1]);time.sleep(0.4)', str(path)])
    try:
        conn, _ = server.accept()
        with conn:
            peer = oslayer.peer_credentials(conn)
            print('same accepted socket UID/PID', peer)
            assert peer.uid == os.getuid() and peer.pid == child.pid
    finally:
        child.wait(timeout=5)
        server.close()
    # The daemon role lock is kernel flock occupancy, never metadata alone.
    import fcntl
    lock = socket_tmp_path / 'role.lock'
    with open(lock, 'a+') as held, open(lock, 'a+') as contender:
        fcntl.flock(held.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError):
            fcntl.flock(contender.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


@pytest.mark.parametrize('refusal', ['wrong-uid', 'kernel-unavailable'])
def test_cp2_3b_darwin_accept_drops_unproven_peer(socket_tmp_path, monkeypatch, refusal):
    """A real accepted socket cannot pass a denied kernel-identity check."""
    path = socket_tmp_path / 'reject-peer.sock'
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
        bind_private_unix_socket(server, path)
        server.listen(2)
        server.settimeout(3)
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(3)
            client.connect(str(path))
            if refusal == 'wrong-uid':
                fake = lambda conn: oslayer.PeerCredentials(os.getuid() + 1, os.getpid(), None)
            else:
                def fake(conn):
                    raise oslayer.PeerIdentityUnavailable('injected kernel refusal')
            with monkeypatch.context() as context:
                context.setattr(oslayer, 'peer_credentials', fake)
                assert accept_same_user(server) is None
            assert client.recv(1) == b''  # peer was closed, not served
        # Positive control uses actual Darwin LOCAL_PEERCRED/LOCAL_PEERPID.
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(3)
            client.connect(str(path))
            conn = accept_same_user(server)
            assert conn is not None
            with conn:
                proof = oslayer.peer_credentials(conn)
                assert proof.uid == os.getuid() and proof.pid == os.getpid()
                client.sendall(b'ping')
                assert conn.recv(4) == b'ping'


def test_cp2_4_socket_byte_boundary_and_private_runtime(socket_tmp_path):
    assert oslayer.fits_unix_path('x' * 103)
    assert not oslayer.fits_unix_path('x' * 104)
    assert oslayer.fits_unix_path('é' * 51)
    assert not oslayer.fits_unix_path('é' * 52)
    root = socket_tmp_path
    # Construct an actual final path with 103 encoded bytes including its NUL.
    final = root / ('x' * (103 - len(os.fsencode(root)) - 1))
    assert len(os.fsencode(final)) + 1 == 104
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.bind(str(final))
    too_long = root / ('x' * (104 - len(os.fsencode(root)) - 1))
    with pytest.raises(OSError):
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            s.bind(str(too_long))
    # A short final basename can fit while mkdtemp's /.XXXXXXXX/s staging
    # suffix cannot. Construct a deep parent, not a long final basename.
    pad = 94 - len(os.fsencode(root)) - 1
    assert pad > 0
    staged_parent = root / ("d" * pad)
    staged_parent.mkdir()
    staged_final = staged_parent / "x.sock"
    assert oslayer.fits_unix_path(staged_final)
    assert not oslayer.fits_unix_path(staged_parent / ".XXXXXXXX" / "s")
    with pytest.raises(OSError, match='staged'):
        _require_unix_socket_budget(staged_final)
    # Allowed test-account disposable /private/tmp/ccx-uUID is the product
    # short runtime if no shorter private XDG candidate is available.
    choice = oslayer._darwin_runtime_base(purpose='transport', suffix_bytes=60)
    print('runtime choice', choice)
    assert choice.path.name == f'ccx-u{os.getuid()}' or not choice.may_create


def test_cp2_5_nofollow_root_and_original_devfd_reopen(socket_tmp_path):
    root = socket_tmp_path / 'owned-root'
    root.mkdir()
    with oslayer.open_root_dir(root) as (_, fd):
        assert os.fstat(fd).st_ino == root.stat().st_ino
    link = socket_tmp_path / 'redirect'
    link.symlink_to(root, target_is_directory=True)
    with pytest.raises(oslayer.RootOpenError):
        with oslayer.open_root_dir(link):
            pytest.fail('followed a planted root symlink')
    original = socket_tmp_path / 'source.jsonl'
    original.write_bytes(b'original bytes\n')
    held = os.open(original, os.O_RDONLY)
    try:
        os.lseek(held, 3, os.SEEK_SET)
        # Actual Monterey /dev/fd aliases share the original file offset;
        # the old adapter must refuse instead of claiming an independent
        # original source from a pathname alias.
        with pytest.raises(OSError, match="shares mutable source offset"):
            oslayer.pinned_fd_path(held)
        from ccx.claude.transcript import PinnedNativeSource
        pinned = PinnedNativeSource(held)
        original.rename(socket_tmp_path / 'renamed.jsonl')
        original.write_bytes(b'replacement bytes\n')
        with pinned.open_reader() as stream:
            assert stream.read() == b'original bytes\n'
        assert os.lseek(held, 0, os.SEEK_CUR) == 3
    finally:
        os.close(held)


def test_cp2_6_readonly_parent_watcher_real_fork(tmp_path):
    # Isolated launcher forks a read-only child, waits until it has armed its
    # watcher, publishes PID, exits, and leaves the child blocked in I/O.
    script = '''import os,sys,time
from ccx.system import oslayer
rd,wr=os.pipe()
child=os.fork()
if child == 0:
    os.close(rd)
    oslayer.arm_parent_death(os.getppid())
    os.write(wr,b'R')
    time.sleep(8)
    os._exit(0)
os.close(wr)
ready=os.read(rd,1)
print(child,ready.decode(),flush=True)
os._exit(0)
'''
    process = subprocess.Popen([sys.executable, '-c', script], stdout=subprocess.PIPE,
                               text=True)
    try:
        output = process.stdout.readline().strip()
        process.wait(timeout=3)
        pid_text, ready = output.split()
        assert ready == 'R'
        child_pid = int(pid_text)
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            if oslayer.process_info(child_pid).presence == 'dead':
                break
            time.sleep(0.05)
        else:
            os.kill(child_pid, signal.SIGKILL)  # test-owned child only
            pytest.fail('read-only watcher did not exit on lost parent')
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=3)


def test_cp2_7_search_only_ancestor_allows_owned_root(socket_tmp_path):
    """macOS 27 O_SEARCH traverses --x while the root leaf stays readable."""
    major = int(platform.mac_ver()[0].split('.')[0])
    if major < 27:
        pytest.skip('Monterey uses the O_RDONLY ancestor fallback')
    assert getattr(os, 'O_SEARCH', 0), 'macOS 27 must expose O_SEARCH'
    parent = socket_tmp_path / 'search-only'
    parent.mkdir(mode=0o700)
    root = parent / 'owned-root'
    root.mkdir(mode=0o700)
    try:
        parent.chmod(0o100)
        with oslayer.open_root_dir(root) as (_, fd):
            observed = os.fstat(fd)
            assert stat.S_ISDIR(observed.st_mode)
            assert stat.S_IMODE(observed.st_mode) == 0o700
            assert observed.st_ino == root.stat().st_ino
    finally:
        parent.chmod(0o700)


@pytest.mark.parametrize('failure', ['mismatch', 'eperm'])
def test_cp2_8_injected_libproc_failure_cannot_stop_or_kill_foreign_pid(
    tmp_path, monkeypatch, failure,
):
    from test_oslayer_darwin import assert_injected_darwin_identity_blocks_signals
    assert_injected_darwin_identity_blocks_signals(tmp_path, monkeypatch, failure)


def test_cp2_9_real_ccx_list_closed_stdout_is_quiet(socket_tmp_path):
    """Real Darwin kqueue EOF detection, not an injected kevent result."""
    from ccx.store.root import require_ccx_active
    from ccx.system.pipes import quiet_broken_stdout
    from test_generation_store import add_worker, fixture_root

    root = fixture_root(socket_tmp_path)
    for index in range(100):
        worker = f'{index + 1:08x}-1234-4000-8000-000000000001'
        add_worker(root, worker, terminal=True)
    assert len(require_ccx_active(root).list_workers()) == 100

    # One big, pretty-printed native CLI response: head exits after its first
    # line, while CCX still writes the remaining records through stdout.
    source = Path(__file__).resolve().parent.parent
    command = (f'{shlex.quote(sys.executable)} -m ccx list --json | head -1; '
               'printf "CCX_EXIT=%s\\n" "${PIPESTATUS[0]}"')
    completed = subprocess.run(
        ['/bin/bash', '-c', command], cwd=source,
        env={'HOME': str(socket_tmp_path), 'CCX_ROOT': str(root),
             'CLAUDE_CONFIG_DIR': str(socket_tmp_path / 'native'),
             'CCX_CLAUDE_BIN': '/bin/false', 'CCX_CODEX_BIN': '/bin/false',
             'PATH': '/usr/bin:/bin', 'LANG': 'C', 'PYTHONPATH': str(source)},
        capture_output=True, text=True, timeout=60,
    )
    assert completed.returncode == 0
    assert completed.stdout.splitlines() == ['[', 'CCX_EXIT=0']
    assert 'BrokenPipeError' not in completed.stderr
    assert 'Traceback' not in completed.stderr
    assert completed.stderr == ''

    # A broken *non-stdout* socket must still reach the caller, even when
    # Darwin could create a kqueue for process stdout.
    with pytest.raises(BrokenPipeError):
        with quiet_broken_stdout():
            raise BrokenPipeError('unrelated local RPC socket')


def test_cp2_10_apfs_case_alias_uses_physical_directory_spelling(socket_tmp_path):
    """Real case-insensitive APFS case alias must not split native project keys."""
    from ccx.system.cwd_paths import canonical_cwd
    from ccx.ops.creation_steps import validated_create_spec
    from ccx.claude.settings import _requested_trust_path

    original = socket_tmp_path / 'MixedCase'
    original.mkdir(mode=0o700)
    alias = socket_tmp_path / 'mixedcase'
    if not alias.is_dir() or os.stat(alias).st_ino != os.stat(original).st_ino:
        pytest.skip('test temporary volume is case-sensitive')

    expected = str(original.resolve())
    assert canonical_cwd(alias) == expected
    assert _requested_trust_path(str(alias)) == expected
    directory, _ = validated_create_spec(
        engine='claude', cwd=str(alias), model='haiku', effort=None,
        idle_timeout=0, permissions='full', sandbox=None,
        approval_policy=None, agent=None, append_system_prompt=None,
        allowed_tools=None, disallowed_tools=None, add_dir=None,
        permission_mode=None,
    )
    assert directory == expected

    # After deletion of a directory, untrust still canonicalizes the nearest
    # existing parent and must not generate a second lowercase ledger key.
    assert _requested_trust_path(str(alias / 'gone')) == str(original / 'gone')
