import fcntl
import json
import os
import stat
from concurrent.futures import ThreadPoolExecutor

import pytest

from ccx.ops import compact_ops
from ccx.ops import steer_ops


@pytest.fixture(params=[(steer_ops, 'steer_deliveries_lock_path', 'steer_deliveries_path', 'SteerJournalBusy'),
                        (compact_ops, 'lock_path', 'compact_operations_path', 'CompactJournalBusy')])
def journal(request, tmp_path, monkeypatch):
    module, lock_name, journal_name, error_name = request.param
    monkeypatch.setattr(module, lock_name, lambda short: tmp_path / f'{short}.lock')
    monkeypatch.setattr(module, journal_name, lambda short: tmp_path / f'{short}.json')
    return module, getattr(module, error_name)


def test_lock_reentrant_and_bounded_across_threads(journal):
    module, busy = journal
    with ThreadPoolExecutor(max_workers=1) as pool:
        with module.journal_lock('worker', timeout=0):
            with module.journal_lock('worker', timeout=0):
                def contender():
                    with pytest.raises(busy):
                        with module.journal_lock('worker', timeout=0):
                            pytest.fail('contender acquired held lock')
                pool.submit(contender).result(timeout=2)
        with module.journal_lock('worker', timeout=0):
            pass


def test_lock_flock_contention_is_bounded(journal, tmp_path):
    module, busy = journal
    with (tmp_path / 'worker.lock').open('w') as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(busy):
            with module.journal_lock('worker', timeout=0):
                pytest.fail('acquired independently held flock')
    with module.journal_lock('worker', timeout=0):
        pass


def test_journal_publication_fsyncs_file_then_directory(journal, tmp_path, monkeypatch):
    module, _ = journal
    calls = []
    original = os.fsync
    def record(fd):
        calls.append('directory' if stat.S_ISDIR(os.fstat(fd).st_mode) else 'file')
        original(fd)
    monkeypatch.setattr(os, 'fsync', record)
    module.write_journal('worker', module.empty_journal())
    assert calls == ['file', 'directory']
    assert json.loads((tmp_path / 'worker.json').read_text())['version'] == module.JOURNAL_VERSION
    assert not list(tmp_path.glob('*.tmp'))


def test_distinct_journal_paths_do_not_share_reentrancy(tmp_path, monkeypatch):
    monkeypatch.setattr(steer_ops, 'steer_deliveries_lock_path', lambda _: tmp_path / 'steer.lock')
    monkeypatch.setattr(compact_ops, 'lock_path', lambda _: tmp_path / 'compact.lock')
    with (tmp_path / 'compact.lock').open('w') as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with steer_ops.journal_lock('same-worker', timeout=0):
            with pytest.raises(compact_ops.CompactJournalBusy):
                with compact_ops.journal_lock('same-worker', timeout=0):
                    pytest.fail('distinct journal bypassed flock')
