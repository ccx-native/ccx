"""Sanitized image prompt splitting and native Claude evidence shapes."""
from __future__ import annotations

import json

import pytest

from ccx.prompt_images import looks_like_image_line, split_prompt_images
from ccx.claude import transcript as T


@pytest.fixture
def images(tmp_path):
    paths = [tmp_path / 'first.PNG', tmp_path / 'second image.jpeg', tmp_path / 'third.webp']
    for path in paths:
        path.write_bytes(b'fixture image')
    return [str(path) for path in paths]


def test_single_image_and_surrounding_whitespace(images):
    assert split_prompt_images(images[0]) == ([images[0]], '')
    assert split_prompt_images(f' \n {images[0]} \n\n') == ([images[0]], '')


def test_multiple_images_and_text_preserve_other_bytes(images):
    prompt = f' First  \n\n{images[0]}\n\n{images[1]}\n\nLast\t \n\nUnrelated\n'
    assert split_prompt_images(prompt) == (images[:2], ' First  \n\nLast\t \n\nUnrelated\n')
    assert split_prompt_images('\n'.join(images)) == (images, '')
    assert split_prompt_images('\n'.join([images[0], images[0]])) == ([images[0]] * 2, '')


def test_crlf_is_preserved(images):
    prompt = f'{images[0]}\r\n\r\nHello \r\n\r\n{images[1]}\r\n\r\nEnd\r\n'
    assert split_prompt_images(prompt) == (images[:2], 'Hello \r\n\r\nEnd\r\n')


@pytest.mark.parametrize('prompt', [
    'look at /fixture/a.png', 'a.png', './a.png', '~/a.png', '/fixture/missing.png',
    '/fixture/image.svg', 'plain\r\n\r\ntext\t ', '',
])
def test_non_images_stay_identical(prompt):
    assert split_prompt_images(prompt) == ([], prompt)


def test_inline_existing_image_is_text(images):
    prompt = f'look at {images[0]}'
    assert split_prompt_images(prompt) == ([], prompt)


def test_regular_file_checked_at_delivery(images, tmp_path):
    from pathlib import Path
    directory = tmp_path / 'folder.png'
    directory.mkdir()
    assert split_prompt_images(str(directory)) == ([], str(directory))
    Path(images[0]).unlink()
    assert looks_like_image_line(images[0])
    assert split_prompt_images(images[0]) == ([], images[0])


@pytest.mark.parametrize(('line', 'expected'), [
    (' /fixture/space image.GIF \r\n', True), ('/a.jpg\n/b.png', False),
    ('~/a.png', False), ('relative.jpeg', False), ('look at /a.webp', False),
])
def test_syntax_only(line, expected):
    assert looks_like_image_line(line) is expected


@pytest.mark.parametrize(('expected', 'actual', 'matches'), [
    ('Inspect\n/fixture/a.png\n/fixture/b.JPG', '[Image #3] [Image #4]Inspect', True),
    ('/fixture/a.png\n/fixture/b.JPG', '[Image #3] [Image #4]', True),
    ('/fixture/a.png', '[Image #3]', True),
    # b.JPG was missing at staging, so it was pasted as text.
    ('Inspect\n/fixture/a.png\n/fixture/b.JPG', '[Image #3]Inspect\n/fixture/b.JPG', True),
    ('/fixture/missing.png\nInspect\n/fixture/a.png', '[Image #3] /fixture/missing.png\nInspect', True),
    ('Inspect\n/fixture/a.png', '[Image #3]Inspect\nextra', False),
    ('Inspect\n/fixture/a.png', '[Image #3]Other', False),
    ('/fixture/a.png', '', False),
    ('unchanged  text', 'unchanged text', False),
    ('unchanged\ntext', 'unchanged text', False),
    ('look at /fixture/a.png', '[Image #3]', False),
    ('~/a.png', '[Image #3]', False),
    ('/fixture/missing.png', '/fixture/missing.png', True),
    ('unchanged', 'unchanged', True),
])
def test_prompt_matches(expected, actual, matches):
    assert T.prompt_matches(expected, actual) is matches


@pytest.mark.parametrize('text', ['Review these\n/fixture/a.png\n/fixture/b.jpg',
                                  '/fixture/a.png\n/fixture/b.jpg'])
def test_queue_attachment_user_and_original_start_shapes(tmp_path, text):
    rest = 'Review these' if text.startswith('Review') else ''
    actual = f'[Image #3] [Image #4]{rest}'
    rows = [
        {'type': 'queue-operation', 'operation': 'enqueue', 'content': actual},
        {'type': 'queue-operation', 'operation': 'dequeue', 'content': actual},
        {'type': 'attachment', 'attachment': {'type': 'queued_command', 'prompt': actual}},
        {'type': 'user', 'uuid': 'native-user', 'imagePasteIds': [3, 4],
         'message': {'content': [{'type': 'text', 'text': actual}, {'type': 'image'}]}},
        {'type': 'user', 'isMeta': True,
         'message': {'content': [{'type': 'text', 'text': '[Image: source: /fixture/a.png]'}]}},
    ]
    path = tmp_path / 'fixture.jsonl'
    path.write_text(''.join(json.dumps(row) + '\n' for row in rows))
    assert T.queued_prompt_accepted(path, text)
    assert [e['kind'] for e in T.steer_evidence_events(path, text)] == [
        'enqueued', 'started', 'consumed', 'started']
    event = T.exact_user_prompt_event(path, text)
    assert event['uuid'] == 'native-user'
    assert T.original_started_user_event(
        path, text, after_offset=0, evidence_uuid=event['uuid'],
        evidence_offset=event['offset'])['kind'] == 'native-started-user'
