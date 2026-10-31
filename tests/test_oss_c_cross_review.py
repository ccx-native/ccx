"""Hostless cross-review regressions for slice C."""
import ccx.ops.wait as ops_wait
from types import SimpleNamespace

import pytest

from ccx.client import CcxClient
from ccx.errors import TurnIncomplete
from ccx.controllers import notify as controller_notify
from ccx.ops import common as protocol_ops
from ccx.cli.parser import build_parser
from test_oss_c_regressions import fake_store
from test_generation_store import A


def test_controller_resume_help_matches_argument_parser():
    parser = build_parser()
    controller = parser._subparsers._group_actions[0].choices['controller']
    resume = controller._subparsers._group_actions[0].choices['resume']
    help_text = ' '.join(resume.format_help().split())
    assert 'Arguments after TARGET' in help_text
    assert 'Use -- before option-like script arguments' in help_text
    assert parser.parse_args(['controller', 'resume', 'demo', '--', '--custom']).script_args == ['--custom']
    with pytest.raises(SystemExit):
        parser.parse_args(['controller', 'resume', 'demo', '--custom'])


def test_python_list_root_prefix_includes_descendants(monkeypatch):
    row = {'worker': 'claude:' + A, 'cwd': '/tmp/project'}
    def listing(**kwargs):
        kwargs['records'][row['worker']] = (None, row)
        return [row]
    client = object.__new__(CcxClient)
    client._uuid_store = SimpleNamespace(list_workers=listing)
    # The typed inventory projection may also receive sanitized row metadata;
    # this test only exercises cwd-prefix selection.
    monkeypatch.setattr(client, '_uuid_session_info', lambda owner, record, **_kw: record)
    assert client.list(cwd_prefix='/') == [row]


@pytest.mark.parametrize('status', ['not_found', 'prepared'])
def test_ordinary_delivery_failure_names_original_token(monkeypatch, status):
    fake_store(monkeypatch, {'workerId': A, 'lifecycle': {'state': 'idle'}})
    monkeypatch.setattr(ops_wait, 'reconcile_turn', lambda *args, **kwargs: {'status': status})
    with pytest.raises(TurnIncomplete) as raised:
        ops_wait.wait_delivery('claude:' + A, delivery_id='ordinary-send-token')
    assert raised.value.delivery_id == 'ordinary-send-token'
    assert 'ordinary-send-token' in str(raised.value)
    assert 'create:initial' not in str(raised.value)
    assert 'same delivery ID and message' in str(raised.value)
