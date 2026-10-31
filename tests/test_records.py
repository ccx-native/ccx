"""Claude's independent native transcript-path formatting contract.

Full-UUID generation record creation, atomicity, and CAS are exercised by the
generation and original record-writer tests, not by retired short-record APIs.
"""
from __future__ import annotations

from ccx.store.paths import transcript_path_for, subagent_dir_for


def test_claude_project_slug_replaces_all_non_alphanumerics():
    sid = "abcd1234-uuid"
    cwd = "/tmp/tmp.8L77T2AeGj/a_b @c"

    transcript = transcript_path_for(cwd, sid)
    subagents = subagent_dir_for(cwd, sid)

    assert transcript.parent.name == "-tmp-tmp-8L77T2AeGj-a-b--c"
    assert subagents.parts[-3] == "-tmp-tmp-8L77T2AeGj-a-b--c"


def test_claude_project_slug_nfc_and_200_utf16_unit_cutoff(monkeypatch):
    from ccx.store import paths
    monkeypatch.setattr(paths.oslayer, "claude_project_cwd_is_nfc", lambda: True)
    claude_project_slug = paths.claude_project_slug

    assert claude_project_slug("/tmp/cafe\u0301") == "-tmp-caf-"
    assert claude_project_slug("/tmp/caf\u00e9") == "-tmp-caf-"
    exact = "/" + "a" * 199
    assert claude_project_slug(exact) == "-" + "a" * 199
    long = "/" + "a" * 200
    assert claude_project_slug(long) == "-" + "a" * 199 + "-b6ymvl"


def test_claude_project_slug_hashes_normalized_utf16_cwd_and_surrogates(monkeypatch):
    from ccx.store import paths
    monkeypatch.setattr(paths.oslayer, "claude_project_cwd_is_nfc", lambda: True)
    claude_project_slug = paths.claude_project_slug

    assert claude_project_slug("/tmp/\U0001f600/project") == "-tmp----project"
    astral = "/tmp/\U0001f600" + "b" * 205
    assert claude_project_slug(astral) == "-tmp---" + "b" * 193 + "-iz0o2g"
    nfd = "/tmp/cafe\u0301/" + "x" * 205
    nfc = "/tmp/caf\u00e9/" + "x" * 205
    assert claude_project_slug(nfd) == claude_project_slug(nfc)
    assert claude_project_slug(nfd) == "-tmp-caf--" + "x" * 190 + "-23dc7j"
    for cwd in (astral, nfd, nfc):
        slug = claude_project_slug(cwd)
        assert transcript_path_for(cwd, "sid").parent.name == slug
        assert subagent_dir_for(cwd, "sid").parts[-3] == slug



def test_claude_project_slug_linux_preserves_nfd_hash_input(monkeypatch):
    from ccx.store import paths
    monkeypatch.setattr(paths.oslayer, "claude_project_cwd_is_nfc", lambda: False)
    cwd = "/tmp/cafe\u0301/"
    assert paths.claude_project_slug(cwd) == "-tmp-cafe--"
    assert paths.claude_project_slug(cwd) != paths.claude_project_slug("/tmp/caf\u00e9/")
    long = cwd + "x" * 205
    result = paths.claude_project_slug(long)
    assert result.startswith("-tmp-cafe--")
    assert result != paths.claude_project_slug("/tmp/caf\u00e9/" + "x" * 205)
    assert paths.transcript_path_for(long, "sid").parent.name == result
