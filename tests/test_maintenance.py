"""Scoped shutdown grammar and non-force daemon refusals; fake-only effects."""
import pytest

from ccx.ops import maintenance as M
from ccx import constants
from ccx.cli.parser import build_parser


@pytest.mark.parametrize("generation, why", [
    (1, "a codexd from another ccx version"),
    (None, "a codexd from another ccx version"),
    ("current", "loaded after the kills"),
])
def test_a_refused_codexd_stop_preserves_unowned_workers(monkeypatch, generation, why):
    from ccx.codex import runtime as codex_runtime
    from ccx.constants import CODEXD_RPC_GENERATION

    reply = {"ok": False, "error": "workers are still active: abcd1234 (busy)",
             "transport": "served"}
    if generation is not None:
        reply["rpcGeneration"] = (CODEXD_RPC_GENERATION if generation == "current"
                                  else generation)
    calls = []

    def refuse(*, force):
        calls.append(force)
        return reply

    monkeypatch.setattr(codex_runtime, "daemon_stop", refuse)
    with pytest.raises(M.CcxError) as error:
        M._stop_codexd(None)
    message = str(error.value)
    assert "abcd1234 (busy)" in message and why in message
    assert calls == [False]
    assert "remains running" in message and "ownership" in message
    assert "ccx shutdown --engine codex" in message
    assert "--force" not in message and "ccx codex daemon" not in message


def test_shutdown_scoped_parser_and_truthful_help(capsys):
    parser = build_parser()
    assert parser.parse_args(["shutdown", "--engine", "claude"]).engine == "claude"
    assert parser.parse_args(["shutdown", "--engine", "codex"]).engine == "codex"
    assert parser.parse_args(["shutdown"]).engine is None
    with pytest.raises(SystemExit) as invalid:
        parser.parse_args(["shutdown", "--engine", "unknown"])
    assert invalid.value.code == constants.EXIT_ERROR
    capsys.readouterr()
    with pytest.raises(SystemExit) as help_exit:
        parser.parse_args(["shutdown", "--help"])
    assert help_exit.value.code == 0
    help_text = capsys.readouterr().out
    assert "--engine {claude,codex}" in help_text
    assert "notifications keep running" in help_text
    assert "workers CCX doesn't know about" in help_text
    assert "Without --engine" in help_text


def test_incompatible_codexd_guidance_uses_current_shutdown_command():
    from ccx.codex.daemon.daemon import CodexDaemon
    from ccx.codex.runtime import require_served_generation
    from ccx.errors import SessionUnavailable

    old = constants.CODEXD_RPC_GENERATION - 1
    with pytest.raises(SessionUnavailable) as error:
        require_served_generation({"rpcGeneration": old})
    assert "ccx shutdown --engine codex" in str(error.value)
    assert "ccx codex daemon stop" not in str(error.value)

    response = CodexDaemon().dispatch({
        "op": "native-send", "rpcGeneration": old,
    })
    assert response["ok"] is False and response["incompatible"]
    assert "ccx shutdown --engine codex" in response["error"]
    assert "ccx codex daemon stop" not in response["error"]
