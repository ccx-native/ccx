"""`ccx logs --tail` reads a Claude transcript from the end."""
import json

from ccx.ops import conversation as conv
from ccx.ops.conversation_paging import claude_tail_offset


def _transcript(path, turns):
    rows = []
    for n in range(turns):
        rows.append({"type": "user", "uuid": f"u{n}", "timestamp": "2026-11-02T10:00:00Z",
                     "message": {"role": "user", "content": f"question {n}"}})
        rows.append({"type": "assistant", "uuid": f"a{n}", "timestamp": "2026-11-02T10:00:01Z",
                     "message": {"role": "assistant", "model": "m",
                                 "content": [{"type": "text", "text": f"answer {n}"}]}})
    path.write_text("".join(json.dumps(r) + "\n" for r in rows))


def _texts(doc):
    return [[item.get("text") for item in turn["input"] + turn["output"]]
            for turn in doc["turns"]]


def test_tail_reads_only_the_newest_turns(tmp_path):
    path = tmp_path / "t.jsonl"
    _transcript(path, 5)
    size = path.stat().st_size
    rec = {"short": "abcd1234", "transcript": str(path)}
    full = conv.build_claude(rec, state="idle", worker="claude:x")
    start = claude_tail_offset(str(path), before_offset=size, turns=2)
    assert 0 < start < size
    tail = conv.build_claude(rec, state="idle", worker="claude:x", after_offset=start)
    assert _texts(tail) == _texts(full)[-2:]
    assert [t["sequence"] for t in tail["turns"]] == [1, 2]


def test_tail_longer_than_the_conversation_reads_it_all(tmp_path):
    path = tmp_path / "t.jsonl"
    _transcript(path, 3)
    assert claude_tail_offset(str(path), before_offset=path.stat().st_size, turns=10) == 0
