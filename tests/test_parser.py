'CLI argument parser wiring (ccx.cli.main.parser.build_parser).\n\nA smoke check that subcommands parse, dispatch to the right handler, and that\nthe usage-cap percentage validator is wired into the flags. No command is\nexecuted — only argument parsing.\n'

from __future__ import annotations
import ccx.cli.commands.control as commands_control
import ccx.cli.commands.create as commands_create
import ccx.cli.commands.read as commands_read
import ccx.cli.commands.turns as commands_turns

import pytest

from ccx.cli.commands import _common as commands
from ccx.cli import codex as codex_commands
from ccx.cli import statusline
from ccx.cli.parser import build_parser


def test_new_with_prompt():
    args = build_parser().parse_args(["new", "--engine", "claude", "hello world"])
    assert args.cmd == "new"
    assert args.engine == "claude"
    assert args.permissions is None  # Omission uses the existing native full default.
    assert args.prompt == "hello world"
    assert args.func is commands_create.cmd_new


def test_new_promptless():
    args = build_parser().parse_args(["new", "--engine", "codex"])
    assert args.cmd == "new"
    assert args.engine == "codex"
    assert args.prompt is None
    assert args.start_timeout is None  # Effective 90s native default.


def test_new_requires_engine_and_has_no_negative_permission_flag():
    for argv in (["new"], ["new", "hi"],
                 ["new", "--engine", "claude", "--no-skip-permissions"]):
        with pytest.raises(SystemExit) as error:
            build_parser().parse_args(argv)
        assert error.value.code == 1


def test_new_supports_posture_and_explicit_codex_raw_flags():
    parser = build_parser()
    claude = parser.parse_args([
        "new", "--engine", "claude", "--permissions", "restricted",
        "--permission-mode", "plan",
    ])
    assert claude.permissions == "restricted" and claude.permission_mode == "plan"
    codex = parser.parse_args([
        "new", "--engine", "codex", "--sandbox", "read-only",
        "--approval-policy", "on-request", "--start-timeout", "5",
        "--notify-parent", "--settle",
    ])
    assert codex.permissions is None
    assert (codex.sandbox, codex.approval_policy, codex.start_timeout) == (
        "read-only", "on-request", 5.0)
    assert codex.settle and codex.notify_parent


def test_resume_is_root_promptless_with_original_native_json():
    args = build_parser().parse_args(["resume", "aaaaaaaa-1234", "--json"])
    assert args.func is commands_create.cmd_resume
    assert args.target == "aaaaaaaa-1234" and args.json is True


def test_send_subcommand():
    args = build_parser().parse_args(["send", "ab12", "a message"])
    assert args.cmd == "send"
    assert args.target == "ab12"
    assert args.text == "a message"
    assert args.func is commands_turns.cmd_send


def test_attach_subcommand():
    args = build_parser().parse_args(["attach", "ab12"])
    assert args.cmd == "attach"
    assert args.target == "ab12"
    assert args.func is commands_control.cmd_attach


def test_set_model_subcommand():
    args = build_parser().parse_args(["set", "ab12", "--model", "opus"])
    assert args.cmd == "set"
    assert args.target == "ab12"
    assert args.model == "opus"
    assert args.json is False
    assert args.restart is False
    assert args.func is commands_turns.cmd_set


def test_compact_subcommand():
    args = build_parser().parse_args(["compact", "ab12", "--json"])
    assert args.cmd == "compact"
    assert args.target == "ab12"
    assert args.json is True
    assert args.no_wait is False
    assert args.notify_parent is False
    assert args.func is commands_turns.cmd_compact


def test_set_model_restart_flag():
    args = build_parser().parse_args(["set", "ab12", "--model", "opus", "--restart"])
    assert args.restart is True
    assert args.model == "opus"


def test_set_model_defaults_to_none():
    args = build_parser().parse_args(["set", "ab12"])
    assert args.model is None
    assert args.permissions is None
    assert args.restart is False
    assert args.name is None


@pytest.mark.parametrize("permissions", ["full", "restricted"])
def test_set_permissions_subcommand(permissions):
    args = build_parser().parse_args(
        ["set", "ab12", "--permissions", permissions, "--restart"]
    )
    assert args.permissions == permissions
    assert args.restart is True


def test_set_permissions_choices_enforced():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["set", "ab12", "--permissions", "loose"])


def test_set_name_subcommand():
    args = build_parser().parse_args(["set", "ab12", "--name", "My Worker"])
    assert args.cmd == "set"
    assert args.target == "ab12"
    assert args.name == "My Worker"
    assert args.model is None
    assert args.func is commands_turns.cmd_set


def test_set_name_and_model_together():
    args = build_parser().parse_args(
        ["set", "ab12", "--name", "My Worker", "--model", "opus"]
    )
    assert args.name == "My Worker"
    assert args.model == "opus"


def test_set_name_reset_empty_string():
    args = build_parser().parse_args(["set", "ab12", "--name", ""])
    assert args.name == ""


def test_list_flags():
    args = build_parser().parse_args(["list", "--json", "--active"])
    assert args.cmd == "list"
    assert args.json is True
    assert args.active is True
    assert args.engine is None


@pytest.mark.parametrize("flag", ["--external", "--native-bg", "--importable", "--all"])
def test_list_retired_external_options_are_parse_errors(flag):
    with pytest.raises(SystemExit) as stopped:
        build_parser().parse_args(["list", flag])
    assert stopped.value.code == 1


def test_unknown_command_exits():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["frobnicate"])


def test_missing_required_subcommand_exits():
    with pytest.raises(SystemExit):
        build_parser().parse_args([])


def test_cap_flag_valid():
    args = build_parser().parse_args(["new", "--engine", "claude", "x", "--cap-5h", "75"])
    assert args.cap_5h == 75.0


def test_cap_flag_out_of_range_exits():
    # _pct validator rejects > 100 at parse time.
    with pytest.raises(SystemExit):
        build_parser().parse_args(["new", "--engine", "claude", "x", "--cap-5h", "150"])


def test_usage_json_and_summary_are_mutually_exclusive():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["usage", "--json", "--summary"])


def test_analytics_command_removed():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["analytics", "ab12"])


def test_permission_mode_choices_enforced():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["new", "--engine", "claude", "x", "--permission-mode", "bogus"])


def _opt_help(parser, subcmd, dest):
    sub = parser._subparsers._group_actions[0].choices[subcmd]
    for action in sub._actions:
        if action.dest == dest:
            return action.help
    raise AssertionError(f"no {dest} option on {subcmd}")


def test_list_cwd_and_state_have_help():
    p = build_parser()
    assert _opt_help(p, "list", "cwd")
    assert _opt_help(p, "list", "state")


def test_wait_plain_defaults_no_next_turn():
    args = build_parser().parse_args(["wait", "ab12"])
    assert args.cmd == "wait"
    assert args.target == "ab12"
    assert args.next_turn is False
    assert args.func is commands_turns.cmd_wait


def test_wait_next_turn_flag_and_timeout():
    args = build_parser().parse_args(["wait", "ab12", "--next-turn", "--timeout", "30"])
    assert args.next_turn is True
    assert args.timeout == 30


def test_wait_after_turn_is_a_scalar_pass_through():
    ref = "ccx-turn:claude:v1:abc"
    args = build_parser().parse_args(
        ["wait", "ab12", "--next-turn", "--after-turn", ref, "--timeout", "5"])
    assert args.after_turn == ref
    assert args.next_turn is True
    assert build_parser().parse_args(["wait", "ab12"]).after_turn is None
    known = build_parser().parse_args(
        ["wait", "ab12", "--next-turn", "--after-turn", ref, "--known-input", "q1"])
    assert known.known_input == "q1"
    assert build_parser().parse_args(["wait", "ab12"]).known_input is None
    assert "last_settled_turn.turn_ref" in _opt_help(build_parser(), "wait", "after_turn")


def test_turn_waits_are_unbounded_by_default():
    """No verb that starts or resumes a model turn carries a default deadline;
    a turn is model work and only an explicit --timeout may bound it."""
    parser = build_parser()
    for argv in (
        ["wait", "ab12", "--next-turn"],
        ["send", "ab12", "hi"],
        ["new", "--engine", "claude", "--wait"],
        ["answer", "ab12", "yes", "--wait"],
        ["approve", "ab12", "--wait"],
        ["reject", "ab12", "--wait"],
        ["send", "ab12", "hi"],
        ["new", "--engine", "codex", "--wait"],
    ):
        assert parser.parse_args(argv).timeout is None, argv


def test_report_subcommand():
    args = build_parser().parse_args(["report", "ab12", "--max-chars", "500", "--json"])
    assert args.cmd == "report"
    assert args.targets == ["ab12"]
    assert args.max_chars == 500
    assert args.json is True
    assert args.func is commands_read.cmd_report


def test_report_defaults():
    args = build_parser().parse_args(["report", "ab12"])
    assert args.targets == ["ab12"]
    assert args.max_chars is None
    assert args.json is False


def test_report_multiple_shorts():
    args = build_parser().parse_args(["report", "ab12", "cd34", "ef56"])
    assert args.targets == ["ab12", "cd34", "ef56"]
    assert args.func is commands_read.cmd_report


def test_report_multiple_shorts_with_flags():
    args = build_parser().parse_args(["report", "ab12", "cd34", "--max-chars", "200", "--json"])
    assert args.targets == ["ab12", "cd34"]
    assert args.max_chars == 200
    assert args.json is True


def test_report_requires_at_least_one_short():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["report"])


def test_peek_max_chars_default_is_deferred_to_command():
    args = build_parser().parse_args(["peek", "ab12"])
    assert args.max_chars is None


def test_peek_brief_flag():
    args = build_parser().parse_args(["peek", "ab12", "--brief"])
    assert args.brief is True
    assert args.func is commands_read.cmd_peek


def test_peek_brief_default_is_false():
    args = build_parser().parse_args(["peek", "ab12"])
    assert args.brief is False


def test_peek_brief_with_tail():
    args = build_parser().parse_args(["peek", "ab12", "--brief", "--tail", "2"])
    assert args.brief is True
    assert args.tail == 2


def test_peek_brief_with_json():
    # --brief and --json are not mutually exclusive at the parser level;
    # --json takes precedence in cmd_peek.
    args = build_parser().parse_args(["peek", "ab12", "--brief", "--json"])
    assert args.brief is True
    assert args.json is True


def test_statusline_relay_internal_subcommand():
    args = build_parser().parse_args(["statusline-relay", "ab12"])
    assert args.cmd == "statusline-relay"
    assert args.short == "ab12"
    assert args.func is statusline.cmd_statusline_relay


def test_peek_status_default_is_false():
    args = build_parser().parse_args(["peek", "ab12"])
    assert args.status is False
    assert args.extra_shorts == []


def test_peek_status_flag():
    args = build_parser().parse_args(["peek", "ab12", "--status", "--json"])
    assert args.status is True
    assert args.json is True
    assert args.func is commands_read.cmd_peek


def test_peek_status_multiple_shorts():
    args = build_parser().parse_args(["peek", "ab12", "cd34", "ef56", "--status"])
    assert args.target == "ab12"
    assert args.extra_shorts == ["cd34", "ef56"]
    assert args.status is True


def test_peek_requires_at_least_one_short():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["peek"])


def test_context_subcommand():
    args = build_parser().parse_args(["context", "ab12", "--json"])
    assert args.cmd == "context"
    assert args.target == "ab12"
    assert args.json is True
    assert not hasattr(args, "live")
    assert args.func is commands_read.cmd_context


def test_context_requires_short():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["context"])


def test_list_limit_flag():
    args = build_parser().parse_args(["list", "-n", "5"])
    assert args.limit == 5


def test_list_limit_default_is_none():
    assert build_parser().parse_args(["list"]).limit is None


def test_notify_completion_modes_are_mutually_exclusive_and_have_standalone_verbs():
    parser = build_parser()
    assert parser.parse_args(["notify", "worker"]).func is commands_turns.cmd_notify
    assert parser.parse_args(["notify", "worker", "--next-turn"]).next_turn is True
    assert parser.parse_args(["notify", "worker"]).func is commands_turns.cmd_notify
    assert parser.parse_args(["new", "--engine", "claude", "hi", "--notify-parent"]).notify_parent is True
    assert parser.parse_args(["new", "--engine", "codex", "hi", "--notify-parent"]).notify_parent is True
    assert parser.parse_args(["send", "w", "hi", "--notify-parent"]).notify_parent is True
    assert parser.parse_args(["send", "w", "hi", "--notify-parent"]).notify_parent is True
    assert parser.parse_args(["compact", "w", "--notify-parent"]).notify_parent is True
    assert parser.parse_args(["compact", "w", "--notify-parent"]).notify_parent is True
    assert parser.parse_args(["compact", "w", "--no-wait"]).no_wait is True
    for argv in (
        ["new", "--engine", "claude", "hi", "--wait", "--notify-parent"],
        ["compact", "w", "--no-wait", "--notify-parent"],
        ["compact", "w", "--no-wait", "--notify-parent"],
        ["send", "w", "hi", "--wait", "--notify-parent"],
        ["new", "--engine", "codex", "hi", "--wait", "--notify-parent"],
        ["send", "w", "hi", "--no-wait", "--notify-parent"],
    ):
        with pytest.raises(SystemExit):
            parser.parse_args(argv)


def test_version_flag_listed_in_help():
    help_text = build_parser().format_help()
    assert "-V, --version" in help_text


def test_version_flag_via_argparse(capsys):
    from ccx.cli.parser import ccx_version
    with pytest.raises(SystemExit) as exc:
        build_parser().parse_args(["--version", "list"])
    assert exc.value.code == 0
    assert capsys.readouterr().out.strip() == f"ccx {ccx_version()}"


# --- exit codes, --no-wait, help coverage ------------------------------------

@pytest.mark.parametrize("argv", [
    ["new", "--engine", "claude", "--bogus"],
    ["wait"],
    ["send"],
    ["wait", "w", "--timeout", "soon"],
    ["controller", "stop"],
])
def test_usage_errors_exit_bad_input_not_not_found(argv, capsys):
    """argparse's own code is 2, which is EXIT_NOT_FOUND; a mistyped command
    must exit EXIT_ERROR instead, on every subcommand."""
    with pytest.raises(SystemExit) as caught:
        build_parser().parse_args(argv)
    assert caught.value.code == commands.EXIT_ERROR
    assert "error:" in capsys.readouterr().err


def test_list_invalid_state_is_a_usage_error(capsys):
    with pytest.raises(SystemExit) as caught:
        build_parser().parse_args(["list", "--state", "not-a-state"])
    assert caught.value.code == 1
    assert "invalid choice" in capsys.readouterr().err


def test_peek_status_missing_target_appears_once(capsys):
    with pytest.raises(SystemExit) as caught:
        build_parser().parse_args(["peek", "--status"])
    assert caught.value.code == 1
    assert "required: target\n" in capsys.readouterr().err


def test_codex_new_invalid_model_json_error(tmp_path, monkeypatch, capsys):
    from ccx.ops import creation
    from ccx.errors import InvalidRequest
    import json

    def invalid_model(**_kwargs):
        raise InvalidRequest("unsupported Codex model 'bad'. Valid models: gpt-test")

    monkeypatch.setattr(creation, "create", invalid_model)
    args = build_parser().parse_args([
        "new", "--engine", "codex", "--cwd", str(tmp_path),
        "--model", "not-an-available-model", "--json",
    ])
    with pytest.raises(SystemExit) as caught:
        args.func(args)
    assert caught.value.code == 1
    output = capsys.readouterr()
    assert not output.err
    value = json.loads(output.out)
    assert value == {
        "ok": False, "code": "invalid_request",
        "message": "unsupported Codex model 'bad'. Valid models: gpt-test",
    }


def test_help_still_exits_zero(capsys):
    with pytest.raises(SystemExit) as caught:
        build_parser().parse_args(["wait", "--help"])
    assert caught.value.code == 0


@pytest.mark.parametrize("argv", [
    ["new", "--engine", "claude", "hi"],
    ["send", "w", "hi"],
    ["steer", "w", "hi"],
    ["answer", "w", "yes"],
    ["approve", "w"],
    ["reject", "w"],
    ["respond", "w", "--cancel"],
    ["compact", "w"],
    ["new", "--engine", "codex", "hi"],
    ["send", "w", "hi"],
    ["steer", "w", "hi"],
    ["approve", "w"],
    ["reject", "w"],
    ["compact", "w"],
])
def test_every_worker_mutation_accepts_no_wait(argv):
    assert build_parser().parse_args([*argv, "--no-wait"]).no_wait is True


@pytest.mark.parametrize("argv", [
    ["new", "--engine", "claude", "hi"], ["answer", "w", "yes"], ["approve", "w"], ["reject", "w"],
    ["respond", "w", "--cancel"], ["new", "--engine", "codex", "hi"],
])
def test_no_wait_and_wait_are_mutually_exclusive(argv, capsys):
    with pytest.raises(SystemExit) as caught:
        build_parser().parse_args([*argv, "--no-wait", "--wait"])
    assert caught.value.code == commands.EXIT_ERROR


def test_plain_wait_timeout_is_documented_as_honoured():
    help_text = _opt_help(build_parser(), "wait", "timeout")
    assert "Ignored" not in help_text
    assert "exits 6" in help_text


def test_compact_timeouts_are_float_seconds_on_both_engines():
    parser = build_parser()
    assert parser.parse_args(["compact", "w", "--timeout", "1.5"]).timeout == 1.5
    assert parser.parse_args(["compact", "w", "--timeout", "1.5"]).timeout == 1.5


_INTERNAL = {"_launch", "_host", "hooks-relay", "statusline-relay", "host",
             "_live-vars-exec"}


def _public_actions(parser, path=("ccx",)):
    import argparse
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            for name, sub in action.choices.items():
                if name not in _INTERNAL:
                    yield from _public_actions(sub, (*path, name))
        else:
            yield " ".join(path), action


def test_every_public_argument_has_help():
    missing = [f"{path} {action.option_strings or action.dest}"
               for path, action in _public_actions(build_parser())
               if action.help is None]
    assert missing == []


def test_internal_commands_are_hidden_but_still_parse():
    parser = build_parser()
    listing = parser.format_help()
    for name in ("host", "hooks-relay", "statusline-relay", "_live-vars-exec"):
        assert f" {name} " not in listing and f",{name}" not in listing, name
    assert parser.parse_args(["host", "ab12"]).func is not None
    assert parser.parse_args(["hooks-relay", "ab12", "Stop"]).func is not None
    assert parser.parse_args(["statusline-relay", "ab12"]).func is statusline.cmd_statusline_relay


@pytest.mark.parametrize("argv", [
    ["interrupt", "ab12"], ["stream", "ab12"],
    ["codex", "interrupt", "ab12"], ["codex", "report", "ab12"],
    ["codex", "context", "ab12"], ["codex", "events", "ab12"],
])
def test_removed_compatibility_aliases_are_usage_errors(argv, capsys):
    with pytest.raises(SystemExit) as exc:
        build_parser().parse_args(argv)
    assert exc.value.code == 1  # EXIT_ERROR usage error, before any handler
    assert "invalid choice" in capsys.readouterr().err


def test_top_level_help_lists_the_exit_codes():
    listing = build_parser().format_help()
    assert "Exit codes" in listing
    for line in ("2  not found", "5  refused", "6  incomplete"):
        assert line in listing


@pytest.mark.parametrize("sub", ["new", "logs", "usage"])
def test_core_commands_have_a_description(sub):
    parser = build_parser()._subparsers._group_actions[0].choices[sub]
    assert parser.description
def test_version_prefers_the_checkout_over_installed_metadata(tmp_path, monkeypatch):
    from importlib import metadata

    from ccx import version as P

    checkout = tmp_path / "checkout"
    (checkout / "ccx").mkdir(parents=True)
    (checkout / "pyproject.toml").write_text('[project]\nname = "ccx-native"\nversion = "9.9.9"\n')
    monkeypatch.setattr(P, "__file__", str(checkout / "ccx" / "version.py"))
    monkeypatch.setattr(metadata, "version", lambda name: "0.0.1")
    assert P.ccx_version() == "9.9.9"


def test_version_uses_metadata_outside_a_checkout(tmp_path, monkeypatch):
    from importlib import metadata

    from ccx import version as P

    site = tmp_path / "site-packages"
    (site / "ccx").mkdir(parents=True)
    (site / "pyproject.toml").write_text('[project]\nname = "something-else"\nversion = "1.0"\n')
    monkeypatch.setattr(P, "__file__", str(site / "ccx" / "version.py"))
    monkeypatch.setattr(metadata, "version", lambda name: "0.1.2")
    assert P.ccx_version() == "0.1.2"


def test_codex_new_start_timeout_is_separate_from_initial_turn_wait_timeout():
    parser = build_parser()
    normal = parser.parse_args(["new", "--engine", "codex"])
    assert normal.start_timeout is None  # Existing backend applies 90s.
    assert normal.timeout is None
    parsed = parser.parse_args([
        "new", "--engine", "codex", "--start-timeout", ".15", "--timeout", "5",
    ])
    assert parsed.start_timeout == .15
    assert parsed.timeout == 5.0  # CLI rejects unsupported initial wait before claim
    assert "thread to start" in _opt_help(parser, "new", "start_timeout")
    root = parser.parse_args([
        "new", "--engine", "codex", "--start-timeout", ".15", "--timeout", "5",
    ])
    assert root.start_timeout == .15 and root.timeout == 5


def test_current_lifecycle_help_describes_owner_receipts_and_engine_neutral_controls():
    """Focused help-text snapshots for live-smoke findings; parser behavior is unchanged."""
    choices = build_parser()._subparsers._group_actions[0].choices
    help_by_command = {name: " ".join(choices[name].format_help().split()) for name in (
        "new", "send", "fork", "stop", "notify", "usage",
    )}
    for name in ("new", "send", "fork"):
        text = help_by_command[name]
        assert "--var" in text and "--vars-from" in text
        assert "not with --vars-from" in _opt_help(build_parser(), name, "var")
        assert "not with --var" in _opt_help(build_parser(), name, "vars_from").lower()
    assert "--delivery-id" not in help_by_command["new"]
    assert "--notify-parent" in help_by_command["new"]
    send = help_by_command["send"]
    for term in ("queued for delivery", "generated if omitted",
                 "never sends twice", "ccx reconcile"):
        assert term in send
    stop = help_by_command["stop"]
    for term in ("Request cancellation", "exit 5", "interrupt was accepted", "confirmed", "outcome"):
        assert term in stop
    notify = help_by_command["notify"]
    for term in ("Codex parent", "Claude or Codex", "full UUID", "--next-turn"):
        assert term in notify
    usage = help_by_command["usage"]
    assert usage.count("Claude only:") == 3
    for term in ("--cached", "--summary", "--set-source"):
        assert term in usage
