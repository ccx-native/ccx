"""No paste CCX sends trips Claude's asynchronous image-path check."""
from __future__ import annotations

import random
import re

import pytest

from ccx.claude import transcript
from ccx.claude.host.pty import paste_chunks, prompt_paste

# Claude 2.1.319's paste handler, read from the bundle: each pasted line is
# split before a path (`Mr`), and each part that, trimmed, unquoted and with
# backslash escapes removed, ends in an image extension (`R6o`) makes the
# whole paste wait for a file lookup. This port is stricter than Claude: it
# also checks the parts of lines Claude leaves alone.
_CLAUDE_PATH_SPLIT = re.compile(r" (?=[\"']?(?:/|[A-Za-z]:\\))")
_CLAUDE_IMAGE_EXT = re.compile(r"\.(png|jpe?g|gif|webp)$", re.IGNORECASE)


def claude_checks_image(paste: str) -> bool:
    for line in paste.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        for part in _CLAUDE_PATH_SPLIT.split(line):
            s = part.strip()
            if not s:
                continue
            if (s[0] == s[-1] == '"') or (s[0] == s[-1] == "'"):
                s = s[1:-1]
            s = re.sub(r"\\(.)", r"\1", s.replace("\\\\", "\0")).replace("\0", "\\")
            if _CLAUDE_IMAGE_EXT.search(s):
                return True
    return False


def _units(chunk: str) -> int:
    chunk = chunk.replace("\r\n", "\n").replace("\r", "\n")
    return sum(4 if c == "\t" else 2 if ord(c) > 0xFFFF else 1 for c in chunk)


def _assert_safe(text: str, chunks: list[str] | None) -> None:
    assert chunks is not None
    assert "".join(chunks) == text
    for chunk in chunks:
        assert _units(chunk) <= 640
        assert chunk.replace("\r\n", "\n").count("\n") + chunk.count("\r") - chunk.count("\r\n") <= 2
        assert not claude_checks_image(chunk), chunk


INCIDENT = (
    "Hello team,\n\n"
    + "".join(f"{i}. Thanks for the call. Below is the summary we agreed on, with the open "
              "points for the next review; please check the figures before Friday.\n\n"
              for i in range(1, 7))
    + "These are the screens we discussed for the proposals.\nimage.png\n\n\n"
    + "".join(f"{i}. More notes on the plan, the budget and the next steps for the team.\n\n"
              for i in range(7, 14))
    + "Best regards,  \nAlex"
)


def test_the_incident_prompt_keeps_every_image_name_out_of_a_single_paste():
    assert claude_checks_image(INCIDENT)          # the old single-line check trips
    chunks = paste_chunks(INCIDENT, 2)
    assert len(chunks) > 15
    _assert_safe(INCIDENT, chunks)
    assert any(c.endswith("proposals.\nimage.") for c in chunks)
    assert any(c.startswith("png\n") for c in chunks)


@pytest.mark.parametrize("text", [
    "see shot.png",
    "SHOT.PNG\nnext line",
    "a 'shot.webp'\nb",
    "quoted \"my shot.jpeg\"  \nend",
    "escaped x.pn\\g\nend",
    "escaped dot x\\.gif\nend",
    "two a.png b.jpg\nc.gif",
    "look at /tmp/a.png\nthen ~/b.JPEG",
    "tab\tshot.png\r\nnext",
    "emoji 😀 shot.webp\nend",
    "ends with image.png",
    "also bmp.bmp heic.HEIC tiff.tif avif.avif ico.ico svg.svg\nend",
    "C:\\Users\\me\\shot.png\nend",
])
def test_image_names_are_split_in_any_form(text):
    chunks = paste_chunks(text, 2)
    _assert_safe(text, chunks)
    assert len(chunks) > 1


def test_a_split_at_the_unit_limit_still_holds():
    text = "x" * 636 + "a.png\n" + "y" * 700 + ".jpg"
    _assert_safe(text, paste_chunks(text, 2))


@pytest.mark.parametrize("text", ["/tmp/work/red.png", "~/shots/My Shot.JPEG\n", "  /a b/c.gif  "])
def test_text_chunking_breaks_even_bare_paths(text):
    # Existing absolute image files bypass text chunking in stage_paste.
    _assert_safe(text, paste_chunks(text, 2))


@pytest.mark.parametrize("text", ["look:\n/tmp/a.png", "/tmp/a.png\nand this", "shot.png"])
def test_anything_more_than_a_bare_path_is_split(text):
    _assert_safe(text, paste_chunks(text, 2))


def test_text_without_image_names_is_chunked_as_before():
    text = "version 1.2 of file.txt and a png note\nend.\n"
    assert paste_chunks(text, 2) == [text]


def test_random_prompts_never_trip_the_check():
    rng = random.Random(1234)
    pieces = ["word ", "a.png", ".JPG", "\n", "\r\n", "\t", "'q.webp'", " /x/y.gif", "😀",
              "\\", ".", "png", "image", "  ", "\"z.jpeg\"", "b.bmp"]
    for _ in range(400):
        text = "".join(rng.choice(pieces) for _ in range(rng.randint(1, 120)))
        _assert_safe(text, paste_chunks(text, 2))


def test_prompt_paste_frames_the_split_chunks():
    payload, _delay, count = prompt_paste(INCIDENT, rows=40)
    assert count == len(paste_chunks(INCIDENT, 2))
    assert b"image.\x1b[201~\x1b[200~png" in payload


def test_a_mismatch_event_carries_no_text():
    sent = "first line\nimage.png\n\nlast line"
    reordered = "first line\n\nlast line\nimage.png"   # the image paste landed last
    event = transcript.prompt_mismatch_event("d1", sent, reordered)
    assert event == {
        "type": "semantic_prompt_mismatch", "delivery_id": "d1",
        "expected_chars": len(sent), "actual_chars": len(reordered),
        "expected_sha256": event["expected_sha256"], "actual_sha256": event["actual_sha256"],
        "changed_spans": event["changed_spans"], "same_characters": True,
    }
    assert event["changed_spans"] >= 1
    assert "image" not in str(event) and "line" not in str(event)
    assert transcript.prompt_mismatch_event("d1", sent, "something else")["same_characters"] is False


def test_a_send_after_a_mismatched_turn_says_why_it_is_refused():
    from ccx.claude.host.session import unresolved_delivery_refusal
    stuck = {"state": "owned", "evidenceStatus": "foreign-start-after-possible-write"}
    assert unresolved_delivery_refusal("d1", stuck).startswith(
        "Claude started a turn whose text didn't match an earlier message (delivery d1)")
    for receipt in (None, {"state": "owned"}):
        assert unresolved_delivery_refusal("d1", receipt).startswith(
            "An earlier message (delivery d1) is still unresolved")
