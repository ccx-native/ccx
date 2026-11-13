"""Exercise actual host closures with a deterministic, fixture-only PTY."""
from __future__ import annotations

import ast
from pathlib import Path
from types import SimpleNamespace

import pytest

from ccx.claude.host import pty as P
from ccx.claude.host import session as H


class FakePTY:
    def __init__(self, monkeypatch, *, convert=True):
        self.now = 100.0
        self.convert = convert
        self.writes = []
        self.pending = []
        self.count = 7
        self.output = bytearray(b'old [Image #7]')
        self.state = {'pty_buf': self.output, 'current': H.S.IDLE,
                      'last_pty_read': self.now, 'active_turn_ref': None}
        self.clock = SimpleNamespace(monotonic=lambda: self.now, time=lambda: self.now,
                                     sleep=self.advance)
        monkeypatch.setattr(P, 'time', self.clock)
        monkeypatch.setattr(P.select, 'select', self.select)

    def advance(self, seconds):
        self.now += seconds

    def select(self, readers, writers, errors, timeout):
        if self.pending and self.pending[0][0] <= self.now + timeout:
            self.now = max(self.now, self.pending[0][0])
            return readers, [], []
        self.advance(timeout)
        return [], [], []

    def read(self):
        _, data = self.pending.pop(0)
        self.output.extend(data)
        self.state['last_pty_read'] = self.now
        return True

    def write(self, data):
        self.writes.append((self.now, data))
        if data.startswith(b'\x1b[200~/'):
            self.pending.append((self.now + .1, b'Pasting... old [Image #7]'))
            if self.convert:
                self.count += 1
                self.pending.extend([
                    (self.now + 1, b'\x1b[32m[Image #'),
                    (self.now + 2, str(self.count).encode() + b']\x1b[0m'),
                ])
        return True


def host_env(fake):
    """Load real closures without spawning a host, engine, socket or worker."""
    tree = ast.parse(Path(H.__file__).read_text())
    runner = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_run_host')
    names = {'stage_paste', 'write_paste', 'stage_semantic_idle_paste',
             'semantic_paste_write_failed', 'semantic_idle_pump',
             'boundary_deliver_semantic', 'semantic_user_prompt', 'semantic_target_completed'}
    bodies = [n for n in runner.body if isinstance(n, ast.FunctionDef) and n.name in names]
    assert len(bodies) == len(names)
    events, rejected, accepted = [], [], []
    slot = {'text': '', 'delivery_id': 'fixture-delivery', 'phase': 'new_turn',
            'writes': [], 'maybe_written': True}
    env = dict(vars(H))
    env.update(state=fake.state, master_fd=42, time=fake.clock, short='fixture-worker',
               write_pty=fake.write, read_pty_output=fake.read, pty_rows=lambda: 40,
               prepare_semantic_write=lambda _: True, broadcast=events.append,
               mark_semantic_evidence=lambda *a, **k: None,
               current_semantic_slot=lambda: slot,
               reconcile_semantic_delivery=lambda: None,
               input_surface_write_blocked=lambda _: False,
               semantic_reject_unwritten=lambda slot, **kw: rejected.append(kw),
               write_unstick=lambda: fake.write(P.unstick_bytes()),
               write_clear_input=lambda: fake.write(P.clear_input_bytes()),
               write_enter=lambda: fake.write(b'\r'),
               ensure_turn_instance=lambda *a, **k: 'fixture-turn',
               semantic_hot=lambda _: None,
               semantic_accept=lambda *a, **k: accepted.append(k),
               semantic_foreign_turn_start=lambda _: pytest.fail('foreign UPS'),
               rec={'transcript': '/fixture/transcript.jsonl'})
    exec(compile(ast.Module(body=bodies, type_ignores=[]), '<real host image staging>', 'exec'), env)
    return env, slot, events, rejected, accepted


@pytest.fixture
def images(tmp_path):
    files = [tmp_path / 'one.PNG', tmp_path / 'two.jpg']
    for path in files:
        path.write_bytes(b'fixture image')
    return [str(path) for path in files]


@pytest.mark.parametrize('route', ['idle', 'boundary-write'])
@pytest.mark.parametrize('with_text', [True, False])
def test_images_finish_before_text_and_enter(monkeypatch, images, route, with_text):
    fake = FakePTY(monkeypatch)
    env, slot, events, _, _ = host_env(fake)
    rest = 'look at /fixture/inline.png' if with_text else ''
    slot['text'] = '\n'.join(images + ([rest] if rest else []))
    if route == 'idle':
        assert env['stage_semantic_idle_paste'](slot, retry=False)
        assert slot['submit_due'] > fake.now
        assert all(data != b'\r' for _, data in fake.writes)
        fake.advance(.02)
        env['semantic_idle_pump']()
    else:
        assert env['write_paste'](slot['text'])
    assert fake.writes[0] == (100, P.paste_bytes(images[0]))
    assert fake.writes[1] == (102, P.paste_bytes(images[1]))
    assert fake.writes[-1][1] == b'\r'
    assert fake.writes[-1][0] >= 104.01
    text_writes = [data for _, data in fake.writes[2:-1]]
    expected = b''.join(P.paste_bytes(c) for c in P.paste_chunks(rest, 2))
    assert b''.join(text_writes) == expected
    assert P.image_placeholders(fake.output) == {7, 8, 9}


def test_single_bare_image_still_waits(monkeypatch, images):
    fake = FakePTY(monkeypatch)
    env, *_ = host_env(fake)
    assert env['write_paste']('  ' + images[0] + '  ')
    assert fake.writes == [(100, P.paste_bytes(images[0])), (102.01, b'\r')]


@pytest.mark.parametrize('route', ['idle', 'boundary'])
def test_timeout_uses_existing_clear_and_reject_path(monkeypatch, images, route):
    fake = FakePTY(monkeypatch, convert=False)
    env, slot, events, rejected, _ = host_env(fake)
    slot['text'] = images[0]
    if route == 'idle':
        assert not env['stage_semantic_idle_paste'](slot, retry=False)
    else:
        fake.state.update(current=H.S.BUSY, active_turn_ref='target')
        slot.update(phase='steer_boundary', route='steer', target_turn_ref='target', maybe_written=False)
        result = env['boundary_deliver_semantic']({'transcript_path': '/fixture/transcript.jsonl'})
        assert not result['ok']
        assert images[0] in result['error']
        env['semantic_target_completed']('target')
        assert slot['target_settled']
        fake.state.update(current=H.S.IDLE, active_turn_ref=None)
    assert fake.now == 115
    assert slot['phase'] == 'paste_write_failed'
    assert len(fake.writes) == 1  # no Enter and no retry
    for _ in range(3):
        fake.advance(2)
        env['semantic_idle_pump']()
    assert [data for _, data in fake.writes] == [
        P.paste_bytes(images[0]), P.unstick_bytes(), P.clear_input_bytes()]
    assert len(rejected) == 1
    assert rejected[0]['code'] == 'pty-write-failed'
    assert images[0] in rejected[0]['message']
    assert 'cleared without submission' in rejected[0]['message']


@pytest.mark.parametrize('with_text', [False, True])
@pytest.mark.parametrize('phase', ['new_turn', 'steer_boundary'])
def test_ups_placeholder_prompt_is_attributed(monkeypatch, phase, with_text):
    fake = FakePTY(monkeypatch)
    env, slot, events, _, accepted = host_env(fake)
    slot.update(phase=phase, text='/fixture/image.png' + ('\nReview' if with_text else ''))
    hook = {'prompt': '[Image #9]' + ('Review' if with_text else '')}
    assert env['semantic_user_prompt'](hook['prompt'])
    assert not events
    assert bool(accepted) == (phase == 'new_turn')


def test_placeholder_detection_strips_ansi_and_requires_number_change(monkeypatch):
    fake = FakePTY(monkeypatch, convert=False)
    fake.output[:] = b'[Image #7]'
    assert not P.paste_image(42, '/fixture/a.png', write=fake.write, on_read=fake.read,
                             output=lambda: fake.output, timeout_s=.3)
    assert P.image_placeholders(b'\x1b[32m[Image #\x1b[0m12] [Image #2]') == {12, 2}


def test_placeholder_detection_survives_cell_diff_cursor_moves():
    # Real Claude 2.1.323 output: the unchanged space cell is skipped with CHA.
    raw = (b"\x1b[0m\x1b8\x1b(B\x0f\x1b[?2004h\x1b[?25l\x1b[2D\x1b[5B\r"
           b"\x1b[2C\x1b[5A[Image\x1b[10G#3]\r\x1b[47C\x1b[4B\x1b[K\r\r\n")
    assert P.image_placeholders(raw) == {3}
