"""A Claude worker draws in ccx's terminal, whatever shell started it."""
from ccx.claude.host.session import worker_env


def test_agent_shell_terminal_gets_colors():
    env = worker_env({"TERM": "dumb", "NO_COLOR": "1", "PATH": "/bin"})
    assert env["TERM"] == "xterm-256color"
    assert "NO_COLOR" not in env


def test_missing_term_gets_a_real_one():
    assert worker_env({"PATH": "/bin"})["TERM"] == "xterm-256color"


def test_real_terminal_settings_are_kept():
    env = worker_env({"TERM": "screen-256color", "NO_COLOR": "1",
                      "COLORTERM": "truecolor"})
    assert env["TERM"] == "screen-256color"
    assert env["NO_COLOR"] == "1"
    assert env["COLORTERM"] == "truecolor"
