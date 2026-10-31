"""An absent live input gate is a state refusal (exit 5) on either engine."""
from __future__ import annotations

import pytest

from ccx.cli import main as cli
from ccx.codex import client as codex_client
from ccx import constants
from ccx.client import CcxClient
from ccx.codex.client import CodexClient
from ccx.errors import DeliveryRejected, SessionUnavailable
from ccx.store.root import require_ccx_active
from test_generation_store import A, B, add_worker, short_root


@pytest.fixture
def idle_fakes(short_root, monkeypatch):
    add_worker(short_root, A, "claude", confirmed=True)
    add_worker(short_root, B, "codex", confirmed=True)
    monkeypatch.setattr(constants, "CCX_ROOT", short_root)
    native_requests = []

    def codex_gate_reply(worker, message, timeout=10):
        native_requests.append((worker, message))
        return {"ok": False, "transport": "served", "definitive": True,
                "error": "no current original Codex approval for gateId"}

    monkeypatch.setattr(codex_client, "rpc", codex_gate_reply)
    return native_requests


@pytest.mark.parametrize("verb,engine,extra", [
    ("approve", "claude", ()),
    ("reject", "claude", ()),
    ("approve", "claude", ("--request-id", "stale-claude-gate")),
    ("reject", "claude", ("--request-id", "stale-claude-gate")),
    ("answer", "claude", ("Yes",)),
    ("answer", "claude", ("Yes", "--request-id", "stale-question")),
    ("respond", "claude", ("--decline",)),
    ("respond", "claude", ("--decline", "--request-id", "stale-elicitation")),
    ("approve", "codex", ()),
    ("reject", "codex", ()),
    ("approve", "codex", ("--request-id", "stale-codex-gate")),
    ("reject", "codex", ("--request-id", "stale-codex-gate")),
])
def test_idle_no_gate_is_exit_5_and_no_duplicate_verb_prefix(
    idle_fakes, verb, engine, extra, capsys,
):
    worker = f"{engine}:{A if engine == 'claude' else B}"
    with pytest.raises(SystemExit) as error:
        cli._main([verb, worker, *extra, "--json"])
    assert error.value.code == 5
    output = capsys.readouterr()
    assert not output.out
    assert "no " in output.err.lower()
    assert f"{verb}: {verb}:" not in output.err
    if engine == "codex" and "--request-id" in extra:
        assert idle_fakes[-1] == (B, {"op": verb, "gateId": "stale-codex-gate"})
    else:
        assert idle_fakes == []


def test_python_clients_preserve_typed_no_gate_refusals(idle_fakes):
    claude = CcxClient()
    for operation in (
        lambda: claude.approve(A),
        lambda: claude.reject(A),
        lambda: claude.answer(A, "Yes"),
        lambda: claude.respond_elicitation(A, "decline"),
    ):
        with pytest.raises(DeliveryRejected) as error:
            operation()
        assert error.value.exit_code == 5
    codex = CodexClient()
    for operation in (codex.approve, codex.reject):
        with pytest.raises(DeliveryRejected) as error:
            operation(B, request_id="stale-codex-gate")
        assert error.value.exit_code == 5


def test_actual_brokered_gate_with_unavailable_claude_host_still_exits_3(
    idle_fakes, short_root,
):
    store = require_ccx_active(short_root)
    store.mutate_record(A, patches={"waitingFor": {"id": "real-gate", "tool": "Bash"}})
    with pytest.raises(SessionUnavailable) as error:
        CcxClient().approve(A, request_id="real-gate")
    assert error.value.exit_code == 3
