import json
import sys

import pytest

from ccx.ops import conversation
from ccx.claude import settings
from ccx.claude import transcript
from ccx.controllers.live_vars_runner import execute_target


def write_events(tmp_path, events):
    path = tmp_path / 'events.jsonl'
    path.write_text(''.join(json.dumps(event) + '\n' for event in events))
    return path


def test_continuation_text_is_an_ordinary_turn(tmp_path):
    prompt = 'Continue from where you left off.'
    path = write_events(tmp_path, [
        {'type': 'user', 'message': {'content': prompt}},
        {'type': 'assistant', 'message': {'content': [{'type': 'text', 'text': 'working'}]}},
    ])
    assert [turn['text'] for turn in transcript._iter_clean_turns(path)] == [prompt, 'working']


@pytest.mark.parametrize('failure', ['unreadable', 'malformed'])
def test_untrust_keeps_retry_ledger(tmp_path, monkeypatch, failure):
    cfg = tmp_path / '.claude.json'
    ledger = tmp_path / 'trust-ledger.json'
    monkeypatch.setattr(settings, 'CLAUDE_DOTFILE', cfg)
    monkeypatch.setattr(settings, 'trust_ledger_path', lambda: ledger)
    path = str(tmp_path / 'project')
    assert settings.ensure_trusted_paths([path])
    before = json.loads(ledger.read_text())
    if failure == 'unreadable':
        monkeypatch.setattr(settings, '_read_config_bytes', lambda: False)
    else:
        cfg.write_text('{broken')
    assert settings.untrust_paths() == [{'path': path, 'action': 'retry'}]
    assert json.loads(ledger.read_text()) == before


@pytest.mark.parametrize('kind', ['user', 'queue-operation', 'attachment'])
def test_delivery_evidence_rejects_substring(tmp_path, kind):
    body = 'do not ship it'
    event = {'type': kind, 'operation': 'enqueue', 'content': body,
             'message': {'content': body},
             'attachment': {'type': 'queued_command', 'prompt': body}}
    path = write_events(tmp_path, [event])
    assert list(transcript.steer_evidence_events(path, 'ship it')) == []


def test_queue_marker_in_prose_is_ordinary_text(tmp_path):
    body = 'explain queued_command'
    path = write_events(tmp_path, [{'type': 'user', 'message': {'content': body}}])
    assert [e['kind'] for e in transcript.steer_evidence_events(path, body)] == ['started']


@pytest.mark.parametrize('ending,code', [('raise SystemExit(0)', 0), ('raise RuntimeError("oops")', 1)])
def test_module_preserves_globals_after_exception(tmp_path, monkeypatch, ending, code):
    package = tmp_path / 'sample_package'
    package.mkdir()
    (package / '__init__.py').write_text('')
    (package / 'value.py').write_text('value = 2')
    (package / '__main__.py').write_text('from .value import value\nx = value\n' + ending)
    monkeypatch.chdir(tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path))
    for name in ('sample_package', 'sample_package.value', 'sample_package.__main__'):
        monkeypatch.delitem(sys.modules, name, raising=False)
    monkeypatch.setattr(sys, 'argv', list(sys.argv))
    status, ns = execute_target(['-m', 'sample_package'], {'x': 1})
    assert status == code
    assert ns['x'] == 2
    assert ns['__package__'] == 'sample_package'


@pytest.mark.parametrize('kind', ['image', 'document'])
@pytest.mark.parametrize('with_text', [True, False])
def test_attachment_opens_current_message_turn(tmp_path, kind, with_text):
    blocks = [{'type': kind, 'source': {'type': 'base64', 'media_type': 'image/png', 'data': 'AA=='}}]
    if with_text:
        blocks.append({'type': 'text', 'text': 'describe'})
    path = write_events(tmp_path, [
        {'type': 'user', 'message': {'content': 'first'}},
        {'type': 'assistant', 'message': {'content': [{'type': 'text', 'text': 'answer'}]}},
        {'type': 'user', 'message': {'content': blocks}},
        {'type': 'assistant', 'message': {'content': [{'type': 'text', 'text': 'description'}]}},
    ])
    result = conversation.build_claude(
        {'short': '966a8a1b', 'sessionId': 'sess', 'transcript': str(path)},
        state='idle', worker='claude:966a8a1b')
    turns = result['turns']
    assert len(turns) == 2
    assert len(turns[0]['input']) == 1
    assert len(turns[1]['input']) == (2 if with_text else 1)
