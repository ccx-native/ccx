"""Codex import copies a native source by forking one ordinary managed child."""
from __future__ import annotations

from argparse import Namespace

import pytest

from ccx.errors import SessionNotFound, SessionUnavailable
from ccx.codex.history import read_fork_source_point
from ccx.ops import codex_import
from ccx.store.store import codex_fork_lineage
from ccx.store.root import CreationIndeterminate
from test_generation_store import A, B


def _source(tmp_path, *, status="notLoaded", last_status="completed"):
    return {
        "thread": {
            "id": A, "cwd": str(tmp_path),
            "threadSource": "user", "status": {"type": status},
            "name": "Original Desktop thread", "model": "gpt-6-luna",
            "reasoningEffort": "high",
        },
        "lastTurn": {"id": "original-turn-9", "status": last_status,
                     "completedAt": 1793564634},
    }


def test_native_source_point_reads_only_metadata_and_latest_page():
    calls = []
    def request(method, params, *, timeout):
        calls.append((method, params))
        if method == "thread/read":
            return {"thread": {"id": A, "cwd": "/tmp", "status": {"type": "notLoaded"}}}
        if method == "thread/turns/list":
            return {"data": [{"id": "last-native-turn", "status": "completed",
                              "items": [], "itemsView": "notLoaded"}],
                    "nextCursor": "older-items"}
        pytest.fail(f"unexpected provider call {method}")

    import time
    result = read_fork_source_point(request, A, deadline=time.monotonic()+5)
    assert result["lastTurn"]["id"] == "last-native-turn"
    assert calls == [
        ("thread/read", {"threadId": A, "includeTurns": False}),
        ("thread/turns/list", {"threadId": A, "limit": 1,
                              "sortDirection": "desc", "itemsView": "notLoaded"}),
    ]


def test_native_import_two_forks_independent_without_source_owner(tmp_path, monkeypatch):
    calls = []
    def create(**kwargs):
        calls.append(kwargs)
        return {"worker": "codex:" + kwargs["worker_id"]}

    monkeypatch.setattr(codex_import, "create", create)
    source = _source(tmp_path)
    a = codex_import.import_thread(A, source=source)
    b = codex_import.import_thread(A, source=source, effort="low")
    assert a["worker"] != b["worker"]
    for call in calls:
        anchor = call["_fork_anchor"]
        assert call["engine"] == "codex" and call["created_via"] == "import"
        assert call["permissions"] == "full"
        assert anchor["sourceKind"] == "codex-thread"
        assert anchor["sourceWorkerId"] is None
        assert anchor["sourceNativeThreadId"] == A
        assert anchor["lastTurnId"] == "original-turn-9"
        assert anchor["seed"]["stats"]["turns"] == 0
        assert anchor["seed"]["lastTurn"]["turnId"] == "original-turn-9"
        assert anchor["createArgs"]["sandbox"] == "danger-full-access"
        assert anchor["createArgs"]["approvalPolicy"] == "never"
        assert codex_fork_lineage(anchor, child_id=call["worker_id"])["sourceKind"] == "codex-thread"
        assert call["name"] == "Original Desktop thread"
    assert calls[0]["model"] == "gpt-6-luna"
    assert calls[0]["effort"] == "high"
    assert calls[1]["effort"] == "low"
    assert calls[1]["_fork_anchor"]["overrides"]["effort"] == "low"
    assert source == _source(tmp_path)


@pytest.mark.parametrize("state,latest", [
    ("active", "completed"), ("busy", "completed"),
    ("notLoaded", "inProgress"), ("notLoaded", "failed"),
    ("notLoaded", None),
])
def test_import_refuses_busy_or_unfinished_native_point_without_child(
    tmp_path, monkeypatch, state, latest,
):
    source = _source(tmp_path, status=state, last_status=latest)
    monkeypatch.setattr(codex_import, "create",
                        lambda **_k: pytest.fail("ineligible source claimed child"))
    with pytest.raises(SessionUnavailable):
        codex_import.import_thread(A, source=source)


def test_import_refuses_missing_source_cwd_before_claim(tmp_path, monkeypatch):
    source = _source(tmp_path)
    source["thread"]["cwd"] = str(tmp_path / "gone")
    monkeypatch.setattr(codex_import, "create",
                        lambda **_k: pytest.fail("invalid cwd claimed child"))
    with pytest.raises(Exception, match="cwd is not a directory"):
        codex_import.import_thread(A, source=source)


def test_external_variant_is_exact_and_does_not_accept_placeholder(tmp_path):
    _, args = __import__("ccx.ops.creation_steps", fromlist=["validated_create_spec"]).validated_create_spec(
        engine="codex", cwd=str(tmp_path), model=None, effort=None,
        idle_timeout=1800, permissions="full", sandbox=None,
        approval_policy=None, agent=None, append_system_prompt=None,
        allowed_tools=None, disallowed_tools=None, add_dir=None, permission_mode=None,
    )
    anchor = dict(engine="codex", sourceKind="codex-thread",
                  sourceWorkerId=None, sourceNativeThreadId=A,
                  lastTurnId="last-turn", cwd=str(tmp_path),
                  createArgs=args, seed={"stats": {"turns": 0}, "lastTurnAt": None},
                  overrides={"model": None, "effort": None},
                  forkedAtMs=1793564500000)
    assert codex_fork_lineage(anchor)["sourceWorkerId"] is None
    for mutated in (dict(anchor, sourceWorkerId=B),
                    {k: v for k, v in anchor.items() if k != "sourceKind"},
                    dict(anchor, sourceKind="wrong")):
        with pytest.raises(CreationIndeterminate):
            codex_fork_lineage(mutated)


def test_cli_autodetects_codex_native_id_without_claiming_source(tmp_path, monkeypatch, capsys):
    from ccx.cli.commands import create as cli
    from ccx.claude import external
    from ccx.codex import runtime
    monkeypatch.setattr(external, "exact_import_candidate",
                        lambda _id: (_ for _ in ()).throw(SessionNotFound("not Claude")))
    monkeypatch.setattr(runtime, "read_import_source", lambda _id: _source(tmp_path))
    monkeypatch.setattr(codex_import, "import_thread",
                        lambda tid, **kwargs: {"worker": "codex:"+B})
    cli.cmd_import(Namespace(session=A, engine=None, model=None, effort=None))
    out = capsys.readouterr()
    assert out.out.strip() == "codex:"+B
    assert "copied Codex history" in out.err
    assert "unchanged" in out.err


def test_cli_rejects_ambiguous_exact_native_id(tmp_path, monkeypatch, capsys):
    from ccx.cli.commands import create as cli
    from ccx.claude import external
    from ccx.codex import runtime
    monkeypatch.setattr(external, "exact_import_candidate",
                        lambda _id: {"sessionId": A, "cwd": str(tmp_path)})
    monkeypatch.setattr(runtime, "read_import_source", lambda _id: _source(tmp_path))
    with pytest.raises(SystemExit) as exc:
        cli.cmd_import(Namespace(session=A, engine=None, model=None, effort=None))
    assert exc.value.code == 1
    assert "choose --engine" in capsys.readouterr().err
