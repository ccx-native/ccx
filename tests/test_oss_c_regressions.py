"""Hostless regressions for wait, controller draining, and list projections."""
import ccx.store.root as store_root
import ccx.ops.report as ops_report
import ccx.ops.send as ops_send
import ccx.ops.wait as ops_wait
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

from ccx.ops import common as protocol_ops
from ccx.store import store as generation
from ccx.client import CcxClient
from ccx.errors import SessionWaiting, TurnIncomplete
from test_generation_store import A


def fake_store(monkeypatch, record):
    store = SimpleNamespace(generation='test', revalidate=lambda: None,
                            resolve=lambda _: SimpleNamespace(worker='claude:' + A, engine='claude', worker_id=A), record=lambda _: record)
    monkeypatch.setattr(store_root, 'require_ccx_active', lambda *a, **kw: store)
    return store


def test_long_wait_caps_probe(monkeypatch):
    fake_store(monkeypatch, {'lifecycle': {'state': 'idle'}})
    def status(*args, timeout):
        assert 0 < timeout <= 600
        return {'ok': True, 'state': 'idle'}
    monkeypatch.setattr(ops_wait, 'claude_host_rpc', status)
    assert ops_wait.wait_worker('claude:' + A, timeout=3600)['status'] == 'idle'


@pytest.mark.parametrize('state,error', [('crashed', TurnIncomplete),
                                         ('waiting', SessionWaiting),
                                         ('dead', TurnIncomplete)])
def test_send_wait_exits_on_worker_failure(monkeypatch, state, error):
    record = {'workerId': A, 'lifecycle': {'state': state}}
    store = fake_store(monkeypatch, record)
    client = object.__new__(CcxClient)
    client._uuid_store = store
    monkeypatch.setattr(client, '_resolve_managed', lambda *a: record)
    receipt = {'status': 'accepted', 'delivery': {'delivery_id': 'original'}}
    monkeypatch.setattr(ops_send, 'send_turn', lambda *a, **kw: receipt)
    monkeypatch.setattr(ops_wait, 'reconcile_turn', lambda *a, **kw: receipt)
    def unexpected_sleep(_):
        pytest.fail('wait ignored worker failure/input')
    monkeypatch.setattr(ops_wait.time, 'sleep', unexpected_sleep)
    with pytest.raises(error) as raised:
        client.send(A, 'hello', wait=True, delivery_id='original')
    assert raised.value.delivery_id == 'original'


def test_finish_futures_checks_nested_failure():
    from ccx.controllers.controller import _ControllerRuntime
    runtime = object.__new__(_ControllerRuntime)
    runtime._lock = threading.RLock()
    runtime._executor = ThreadPoolExecutor(max_workers=2)
    runtime._futures = []
    runtime._observed_futures = set()
    runtime._live_vars_owner = None
    entered = threading.Event()
    release = threading.Event()
    def child():
        raise RuntimeError('nested failure')
    def parent():
        assert release.wait(2)
        runtime.submit(child)
    first = runtime.submit(parent)
    original_result = first.result
    def result(*args, **kwargs):
        entered.set()
        release.set()
        return original_result(*args, **kwargs)
    first.result = result
    with pytest.raises(RuntimeError, match='nested failure'):
        runtime.finish_futures()
    assert entered.is_set()


@pytest.mark.parametrize('flag', ['--cwd-prefix', '--dir'])
def test_list_root_includes_descendants(monkeypatch, capsys, flag):
    from ccx.cli.commands.read import cmd_list
    from ccx.cli.parser import build_parser
    store = fake_store(monkeypatch, {})
    store.list_workers = lambda **kw: [{'cwd': '/tmp/project'}]
    cmd_list(build_parser().parse_args(['list', flag, '/', '--json']))
    assert json.loads(capsys.readouterr().out) == [{'cwd': '/tmp/project'}]


def test_dashboard_normalized_states():
    from test_dashboard_example import dash
    assert dash.state_style('starting') == (dash.C_YELLOW, '◌')
    assert dash.state_style('stopped') == (dash.C_DIM, '·')
    assert dash.state_style('failed') == (dash.C_RED, '✗')
    assert 'starting' in dash.LIVE_STATES


def test_send_wait_selects_exact_receipt_report(monkeypatch):
    record = {'workerId': A, 'lifecycle': {'state': 'busy'}}
    client = object.__new__(CcxClient)
    client._uuid_store = fake_store(monkeypatch, record)
    monkeypatch.setattr(client, '_resolve_managed', lambda *a: record)
    receipt = {'delivery_id': 'original', 'turn_ref': 'exact', 'settlement': {}}
    monkeypatch.setattr(ops_send, 'send_turn', lambda *a, **kw: {'status': 'accepted'})
    def reconcile(*a, **kw):
        assert kw['delivery_id'] == 'original'
        return {'status': 'settled', 'delivery': receipt}
    def report(handle, **kw):
        assert kw['turn_ref'] == 'exact'
        return {'worker': handle, 'delivery_id': 'original', 'turn_ref': 'exact',
                'status': 'completed', 'text': 'original reply'}
    monkeypatch.setattr(ops_wait, 'reconcile_turn', reconcile)
    monkeypatch.setattr(ops_report, 'report_worker', report)
    result = client.send(A, 'hello', wait=True, delivery_id='original')
    assert result.reply == 'original reply'
    assert result.delivery_id == 'original' and result.turn_ref == 'exact'


def test_event_cursor_requires_anchor_bytes():
    from ccx.ops.worker_events import _encode_cursor, _decode_cursor, EventCursorInvalid
    payload = {'v': 2, 'root': 'root', 'generation': 'g', 'engine': 'claude',
               'workerId': A, 'offset': 0, 'p': None, 'q': 0}
    with pytest.raises(EventCursorInvalid, match='anchorBytes'):
        _decode_cursor(_encode_cursor(payload), payload, None, 0, 'claude')
