from __future__ import annotations

import argparse
import os
import sys
import textwrap

from ccx.cli.commands.control import cmd_attach, cmd_kill, cmd_shutdown, cmd_stop, cmd_untrust
from ccx.cli.commands.create import cmd_fork, cmd_import, cmd_new, cmd_resume
from ccx.cli.commands.read import (
    cmd_context, cmd_events, cmd_history, cmd_list, cmd_logs, cmd_peek,
    cmd_report, cmd_usage,
)
from ccx.cli.commands.models import cmd_models
from ccx.cli.commands.turns import (
    cmd_answer, cmd_approve, cmd_compact, cmd_notify, cmd_reconcile, cmd_reject,
    cmd_respond, cmd_send, cmd_set, cmd_steer, cmd_vars, cmd_wait,
)
from ccx.constants import (
    COMPACT_WAIT_DEFAULT_S,
    DEFAULT_IDLE_TIMEOUT_S,
    EXIT_ERROR,
    HOST_SOCK_WAIT_NEW_S,
)
from ccx.ops.terminate import KILL_TIMEOUT_DEFAULT_S
from ccx.version import ccx_version  # noqa: F401  (re-exported for cli)
from ccx.cli import controller as controller_commands
from ccx.cli import skills
from ccx.controllers import live_vars
from ccx.rpc import local_rpc
from ccx.claude.host.hooks import cmd_hooks_relay
from ccx.claude.host.session import cmd_host
from ccx.cli.statusline import cmd_statusline_relay
from ccx.claude.usage import _add_cap_flags

class _HelpFormatter(argparse.HelpFormatter):
    """Wrap prose paragraphs; keep indented blocks (examples, tables) as written."""

    def _fill_text(self, text, width, indent):
        blocks = []
        for block in textwrap.dedent(text).strip("\n").split("\n\n"):
            if any(line.startswith(" ") for line in block.splitlines()):
                blocks.append(textwrap.indent(block, indent))
            else:
                blocks.append(textwrap.fill(" ".join(block.split()), width,
                                            initial_indent=indent,
                                            subsequent_indent=indent))
        return "\n\n".join(blocks)


class _PeekTargets(argparse.Action):
    """Show one positional in help while retaining peek's existing target fields."""

    def __call__(self, parser, namespace, values, option_string=None):
        namespace.target = values[0]
        namespace.extra_shorts = values[1:]


class _CcxArgumentParser(argparse.ArgumentParser):
    """Usage errors exit EXIT_ERROR (bad input) instead of argparse's 2.

    2 is EXIT_NOT_FOUND, so a mistyped flag must not read as "no such worker".
    Subparsers inherit the class, so every subcommand gets the same code and
    the same help formatter.
    """

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("formatter_class", _HelpFormatter)
        super().__init__(*args, **kwargs)

    def error(self, message):
        if (self.prog == "ccx peek"
                and message == "the following arguments are required: target, more-targets"):
            message = "the following arguments are required: target"
        self.print_usage(sys.stderr)
        self.exit(EXIT_ERROR, f"{self.prog}: error: {message}\n")

    _intermixing = False

    def parse_known_args(self, args=None, namespace=None):
        """Accept a trailing optional positional after options.

        Plain argparse binds `send TARGET [text]` as soon as it sees TARGET, so
        `ccx send W --wait 'text'` failed with "unrecognized arguments".
        Intermixed parsing fixes that, but argparse supports it only on leaf
        parsers without REMAINDER/subparsers; those keep plain parsing.
        """
        if self._intermixing or not self._can_intermix():
            return super().parse_known_args(args, namespace)
        self._intermixing = True
        try:
            return self.parse_known_intermixed_args(args, namespace)
        finally:
            self._intermixing = False

    def _can_intermix(self):
        if any(a.nargs in (argparse.REMAINDER, argparse.PARSER) for a in self._actions):
            return False
        return not any(not action.option_strings
                       for group in self._mutually_exclusive_groups
                       for action in group._group_actions)


def _add_json_flag(parser, help: str = "Print JSON instead of text.") -> None:
    parser.add_argument("--json", action="store_true", help=help)


_WORKER_QUERY_HELP = (
    "Worker: claude:/codex: handle, full UUID, or a unique UUID prefix "
    "(at least 8 lowercase hex characters)."
)


def _examples(*lines: str) -> str:
    return "Examples:\n" + "\n".join(f"  {line}" for line in lines)


OVERVIEW = """\
Run persistent Claude Code and Codex workers from the command line.

Quick start:
  W=$(ccx new --engine claude --cwd ~/repo -n tests "Fix the failing tests")
  ccx wait "$W"               # until it's done or needs input
  ccx report "$W"             # its last reply
  ccx send "$W" "Now add a regression test"

Workers keep their own history. Target them by the handle `ccx new` prints
(claude:<uuid> or codex:<uuid>), the full UUID, or a unique UUID prefix of at
least 8 hex characters. Names are labels for `ccx list`, not targets. Idle workers
stop after a while and revive on the next send. `ccx import` brings in a
Claude Code session or a Codex thread you started yourself.

Waiting for results: a person or a Claude parent uses --wait or `ccx wait`.
A Codex parent uses --notify-parent (or `ccx notify`) and ends its turn; CCX
wakes it when the work settles. That works from a CCX Codex worker, the Codex
app or a default `codex` terminal. A `codex` running its own app-server (for
example one started with -c, --profile or --no-daemon, or `codex exec`) can't
be woken; CCX refuses and says to wait instead. --no-wait returns at once.

From Codex, run ccx outside the sandbox (or with full access): it starts
processes and writes outside the workspace. Codex must be on PATH or
CCX_CODEX_BIN must point to its executable."""

HISTORY_DESCRIPTION = """\
Read past conversations of the workers you name, or search them. Nothing is
sent; it works on idle, busy and stopped workers. Reasoning is never shown.

Each message is labelled: U a user message, U+ a user message sent while the
turn ran (a steer), N[kind] a notification or event (bg, monitor, agent, cron,
system, ccx, ...), / a slash command, ! an interrupt, A assistant text, A* the
turn's final answer, A… the latest text of a turn still running, T a tool call.

Every message has an address #TURN.N, the same numbers as `ccx logs` and
`ccx peek --since-turn`; --turns TURN.N prints that message whole. A compaction
is one line (══ #TURN.N compaction); its address prints the summary when one is
stored (Codex encrypts its summaries).

What you get:
  no options             outline of the last 30 turns (10 per worker if several)
  --since/--until        outline of the turns in that time range
  --turns SPEC           those turns as chat (--turns 1- for all)
  --role ROLES           recent turns as chat, showing only those roles
  --grep/--cmd/--errors  one line per matching message, newest 20 per worker;
                         add -C N (or --view) to read the matching turns instead

Views (--view):
  outline  one block per turn: prompt, start of the final answer, tool summary
  chat     prompts and final answers, each cut to --max-chars
  trace    prompts, all assistant text, and one line per tool call
  map      the whole history in segments split at compactions and idle gaps
           over 6 hours, one title each"""

HISTORY_EPILOG = """\
Roles (--role picks what is searched, and what chat and trace show; search
default: user,assistant,notify,tool):
  user       user messages, steers, slash commands, interrupts
  notify     notifications and events
  assistant  all assistant text; final: final answers only (A*, A…)
  tool       tool calls, shown as commands; output: tool results
  compact    compaction summaries (search only)
Tool output is searched and shown only when output is in --role.

--grep is a regular expression; all lowercase means case-insensitive. --cmd
matches whole command names, in any case: `cd x && timeout 30 .venv/bin/python
-m pytest | tail` runs cd, python, pytest and tail. --since/--until take
2026-11-02, '11-02 14:00', 14:00 (today) or 90m/6h/2d (ago), in local time; a
bare --until date includes that day.

Examples:
  ccx history W                                  recent turns
  ccx history W --view map                       a long history at a glance
  ccx history A B C --grep 'apple silicon' --role user
  ccx history W --turns 184.0                    one message, whole
  ccx history W --turns 180-186                  the exchange around it
  ccx history A B C --turns -1                   each worker's last turn
  ccx history W --grep 'deploy|rollback' -C 1    the turns about a topic
  ccx history W --role notify                    recent notifications
  ccx history W --cmd pytest --role tool,output  test runs and how each ended
  ccx history A B --since '11-07 21:00' --until '11-07 22:00'"""


COMMAND_GROUPS = (
    ("Create", ("new", "fork", "resume", "import")),
    ("Turns", ("send", "steer", "stop", "wait", "notify", "reconcile")),
    ("Input requests", ("answer", "approve", "reject", "respond")),
    ("Inspect", ("list", "peek", "report", "history", "logs", "context", "events", "vars")),
    ("Manage", ("attach", "compact", "set", "kill", "shutdown")),
    ("Account", ("usage", "models")),
    ("Setup and automation", ("init", "skills", "untrust", "controller", "rpc")),
)

ENVIRONMENT = """\
Environment:
  CCX_ROOT            store directory (default ~/.ccx)
  CCX_CLAUDE_BIN      claude executable to run
  CCX_CODEX_BIN       codex executable to run (otherwise codex must be on PATH)
  CCX_SEND_MAX_BYTES  optional size limit for prompts sent to Claude workers
  Inside a worker, CCX sets CCX_SELF_WORKER_ID and CCX_SELF_ENGINE."""

EXIT_CODES = """\
Exit codes (same for Claude and Codex):
  0  success                     5  refused in the current state
  1  bad input or usage          6  incomplete: timed out, turn failed,
  2  not found                      or delivery uncertain
  3  unavailable (no store,      7  a usage cap stopped the turn
     not running, sandboxed)     8  Anthropic rate limit (429)
  4  waiting for input           9  transient API error (529 / 5xx)"""


def _command_overview(subparsers) -> str:
    """The grouped command list, built from each subcommand's own summary."""
    summaries = {action.dest: action.help for action in subparsers._choices_actions
                 if action.help is not argparse.SUPPRESS}
    grouped = [name for _, names in COMMAND_GROUPS for name in names]
    assert sorted(grouped) == sorted(summaries), "COMMAND_GROUPS out of date"
    lines = ["Commands (`ccx <command> --help` for details):"]
    for title, names in COMMAND_GROUPS:
        lines.append(f"  {title}:")
        lines.extend(f"    {name:<11} {summaries[name]}" for name in names)
    return "\n".join(lines)


def _add_controller_parsers(sub) -> None:
    """`ccx controller ...`: durable Python orchestration controllers."""
    pcontroller = sub.add_parser(
        "controller",
        help="Run Python scripts that orchestrate CCX workers.",
        description=(
            "A controller is a Python script with an @controller entry point "
            "that drives ordinary CCX workers. Its runs, state and logs are kept "
            "under $CCX_ROOT/controllers. Controllers are addressed by name or "
            "unique run-id prefix, not by worker id."
        ),
    )
    controller_sub = pcontroller.add_subparsers(
        dest="controller_cmd", required=True,
        metavar="{run,list,runs,status,report,wait,notify,logs,input,resume,stop}",
    )

    crun = controller_sub.add_parser(
        "run", help="Start a run and wait for it (Codex parent: --notify-parent).",
        description="Put CCX options before SCRIPT; everything after it goes to the script.",
    )
    crun_completion = crun.add_mutually_exclusive_group()
    crun_completion.add_argument("--detach", action="store_true", help="Return after startup; check it later with status, report or logs.")
    crun_completion.add_argument("--notify-parent", action="store_true", help="Return after startup; wake the calling Codex parent when the run finishes.")
    _add_json_flag(crun)
    crun.add_argument(
        "--name",
        help=(
            "Run under this controller name instead of the decorator default."
        ),
    )
    crun.add_argument("script", help="Python script with an @controller entry point.")
    crun.add_argument("script_args", nargs=argparse.REMAINDER, help="All arguments after SCRIPT are passed to it; an initial -- is optional.")
    crun.set_defaults(func=controller_commands.cmd_run)

    clist = controller_sub.add_parser(
        "list", help="List controllers and their newest run.",
        description="Lifecycle tracks execution; report status comes from the script.",
    )
    _add_json_flag(clist)
    clist.set_defaults(func=controller_commands.cmd_list)

    cruns = controller_sub.add_parser(
        "runs", help="List stored runs for one controller.",
        description="List stored runs of a named controller.",
    )
    cruns.add_argument("name", help="Controller name.")
    _add_json_flag(cruns)
    cruns.set_defaults(func=controller_commands.cmd_runs)

    cstatus = controller_sub.add_parser(
        "status", help="Show a run's tasks, worktrees and workers.",
        description="Lifecycle tracks execution; report status comes from the script.",
    )
    cstatus.add_argument("target", help="Controller name or unique run-id prefix.")
    _add_json_flag(cstatus)
    cstatus.set_defaults(func=controller_commands.cmd_status)

    creport = controller_sub.add_parser(
        "report", help="Print the run's latest report.",
        description="Print the run's latest published report.",
    )
    creport.add_argument("target", help="Controller name or unique run-id prefix.")
    _add_json_flag(creport)
    creport.set_defaults(func=controller_commands.cmd_report)

    cwait = controller_sub.add_parser(
        "wait", help="Wait for a run to finish (not from a Codex parent).",
        description="Wait for a run to finish; a timeout leaves the run active.",
    )
    cwait.add_argument("target", help="Controller name or unique run-id prefix.")
    cwait.add_argument("--timeout", type=float, default=None,
                       help="Bound the wait to N seconds (default: unbounded); running out exits 6.")
    _add_json_flag(cwait)
    cwait.set_defaults(func=controller_commands.cmd_wait)

    cnotify_controller = controller_sub.add_parser(
        "notify", help="Wake the calling Codex parent when a run finishes.",
        description="Wake the calling Codex parent when the latest attempt of a controller run finishes. Starts nothing.",
    )
    cnotify_controller.add_argument("target", help="Controller name or unique run-id prefix.")
    _add_json_flag(cnotify_controller)
    cnotify_controller.set_defaults(func=controller_commands.cmd_notify)

    clogs = controller_sub.add_parser(
        "logs", help="Print a run's stdout/stderr log.",
        description="Print a run's saved stdout and stderr log.",
    )
    clogs.add_argument("target", help="Controller name or unique run-id prefix.")
    clogs.add_argument("--follow", action="store_true",
                       help="Keep printing until the run finishes (not from a Codex parent). Cannot be combined with --json.")
    _add_json_flag(clogs)
    clogs.set_defaults(func=controller_commands.cmd_logs)

    cinput = controller_sub.add_parser(
        "input", help="Leave a note for a controller without waking it.",
        description="Give either --note, or --worker with --comment.",
    )
    cinput.add_argument("target", help="Controller name or unique run-id prefix.")
    cinput.add_argument("--worker", help="Worker the comment is about (a worker id, not a name).")
    cinput.add_argument("--comment", help="Comment about --worker (both together).")
    cinput.add_argument("--note", help="Note about the whole run; not with --worker/--comment.")
    _add_json_flag(cinput)
    cinput.set_defaults(func=controller_commands.cmd_input)

    cresume_controller = controller_sub.add_parser("resume", help="Continue a finished run, reusing done steps (Codex parent: --notify-parent).",
        description="The script must be unchanged since the run started; after editing it, start a new run.")
    cresume_controller.add_argument("--worker", default=None, help="Worker the comment is about (a worker id, not a name).")
    cresume_controller.add_argument("--comment", default=None, help="Comment about --worker (both together).")
    cresume_controller.add_argument("--note", default=None, help="Note about the whole run; not with --worker/--comment.")
    cresume_completion = cresume_controller.add_mutually_exclusive_group()
    cresume_completion.add_argument("--detach", action="store_true", help="Return after startup; check it later with status, report or logs.")
    cresume_completion.add_argument("--notify-parent", action="store_true", help="Return after startup; wake the calling Codex parent when the run finishes.")
    _add_json_flag(cresume_controller)
    cresume_controller.add_argument("target", help="Controller name or unique run-id prefix.")
    cresume_controller.add_argument(
        "script_args", nargs="*", help="Arguments after TARGET are passed to the script. Use -- before option-like script arguments."
    )
    cresume_controller.set_defaults(func=controller_commands.cmd_resume)

    cstop = controller_sub.add_parser("stop", help="Stop a run's process; its workers keep running.")
    cstop.add_argument("target", help="Controller name or unique run-id prefix.")
    cstop.add_argument("--force", action="store_true",
                       help="SIGKILL the controller if it has not stopped 5s after SIGTERM.")
    _add_json_flag(cstop)
    cstop.set_defaults(func=controller_commands.cmd_stop)

    # Private two-hop detached-host plumbing. These are parsed by a fresh CCX
    # process and intentionally omitted from user-facing help.
    for hidden_name, handler in (
        ("_launch", controller_commands.cmd_internal_launch),
        ("_host", controller_commands.cmd_internal_host),
    ):
        hidden = controller_sub.add_parser(hidden_name, help=argparse.SUPPRESS)
        hidden.add_argument("--run-id", required=True)
        hidden.add_argument("--run-dir", required=True)
        hidden.add_argument("--script", required=True)
        hidden.add_argument("--argv-json", required=True)
        hidden.add_argument("--resume", action="store_true")
        if hidden_name == "_launch":
            hidden.add_argument("--startup-timeout", type=float, default=30.0)
        hidden.set_defaults(func=handler)
    controller_sub._choices_actions = [
        action for action in controller_sub._choices_actions
        if action.dest not in {"_launch", "_host"}
    ]


def _add_events_args(parser) -> None:
    """The flags of `ccx events`."""
    parser.add_argument("--after", metavar="CURSOR", default=None,
                     help="Only events after this cursor (from an earlier event or heartbeat).")
    parser.add_argument("--tail", type=int, default=None, metavar="N",
                     help="Start from the newest N events (default 20 without --after; "
                          "0 = from now).")
    parser.add_argument("--follow", action="store_true",
                     help="Keep printing new events as they happen.")
    parser.add_argument("--timeout", type=float, default=None, metavar="SECONDS",
                     help="With --follow, stop after this many seconds.")
    parser.add_argument("--heartbeat", type=float, default=30.0, metavar="SECONDS",
                     help="Periodic heartbeat interval while following JSON output (default 30; "
                          "0 disables periodic heartbeats). An initial cursor heartbeat is still emitted.")
    parser.add_argument("--kind", action="append", default=None, metavar="KIND",
                     help="Only this event kind (repeatable): state, turn.started, turn.ended, "
                          "tool.started, tool.finished, input.requested, input.resolved, "
                          "compact.started, compact.ended, delivery.changed, background.changed, "
                          "context.changed, error.")
    _add_json_flag(parser, help="NDJSON while following (events and heartbeats); "
                             "otherwise one page object with next_cursor.")


def _add_session_parsers(sub) -> None:
    """Managed worker creation and turn delivery: new, send, vars, steer, wait, notify."""
    pd = sub.add_parser(
        "new",
        help="Create a Claude or Codex worker, with an optional first prompt.",
        description=(
            "Create a persistent worker in --cwd and print its handle "
            "(claude:<uuid> or codex:<uuid>); keep it for follow-ups. With a "
            "prompt, the first turn starts right away; without one, the worker "
            "waits for `ccx send`. Workers stop after --idle-timeout and revive "
            "with their history on the next send."
        ),
        epilog=_examples(
            'ccx new --engine claude --cwd ~/repo -n tests "Fix the tests" --wait',
            'W=$(ccx new --engine codex --cwd ~/repo --file task.md)',
            'ccx new --engine claude "Review the diff" --notify-parent  # Codex parent',
        ),
    )
    pd.add_argument("--engine", required=True, choices=("claude", "codex"),
                    help="Required: which engine runs the worker.")
    pd.add_argument("prompt", nargs="?", help="First prompt (omit to create an idle worker).")
    pd.add_argument("--stdin", action="store_true", help="Read the first prompt from stdin.")
    pd.add_argument("--file", default=None, help="Read the first prompt from a file (`-` means stdin).")
    pd.add_argument("--cwd", default=os.getcwd(), help="Working directory (default: the current one).")
    pd.add_argument("-n", "--name", default=None, help="Display name, shown by `ccx list`.")
    pd.add_argument("--worker-id", default=None,
                    help="Use this UUIDv4 as the worker id, so a retry after a lost "
                         "reply reaches the same worker instead of creating another.")
    pd.add_argument("--model", default=None,
                    help="Model (default: the engine's). Claude: an alias such as opus, "
                         "sonnet or haiku. Codex: a full model id from Codex's model list.")
    pd.add_argument("--effort", default=None, choices=["low","medium","high","xhigh","max"],
                    help="Reasoning effort (default: the engine's).")
    pd.add_argument("--agent", default=None,
                    help="Claude only: run as this Claude Code subagent (a subagent name, not a model).")
    pd.add_argument("--append-system-prompt", default=None,
                    help="Claude only: text appended to Claude Code's system prompt.")
    pd.add_argument("--idle-timeout", type=int, default=DEFAULT_IDLE_TIMEOUT_S, metavar="SECONDS",
                    help="Stop the worker after N idle seconds (default 1800; 0 never). "
                         "The next send revives it.")
    pd.add_argument("--allowed-tools", default=None,
                    help="Claude only: passed to claude --allowedTools (comma-separated).")
    pd.add_argument("--disallowed-tools", default=None,
                    help="Claude only: passed to claude --disallowedTools (comma-separated).")
    pd.add_argument("--add-dir", action="append", default=None,
                    help="Claude only: extra directory Claude may access (repeatable).")
    pd.add_argument("--permission-mode", default=None, choices=["acceptEdits","auto","bypassPermissions","default","dontAsk","plan"],
                    help="Claude only: Claude Code permission mode; needs --permissions restricted.")
    pd.add_argument("--permissions", choices=("full", "restricted"), default=None,
                    help="full (default): no permission prompts (Claude skips them; Codex "
                         "runs with danger-full-access and never asks). restricted: Claude "
                         "asks through `ccx approve`/`reject`; Codex uses workspace-write "
                         "and asks on request. Not with --sandbox/--approval-policy.")
    pd.add_argument("--sandbox", choices=("read-only", "workspace-write", "danger-full-access"),
                    default=None, help="Codex only: sandbox mode (default danger-full-access); not with --permissions.")
    pd.add_argument("--approval-policy", choices=("untrusted", "on-request", "never"),
                    default=None, help="Codex only: approval policy (default never); not with --permissions.")
    pd.add_argument("--start-timeout", type=float, default=None, metavar="SECONDS",
                    help="Codex only: seconds to wait for the thread to start (up to 90, the default).")
    pd.add_argument(
        "--var",
        action="append",
        default=None,
        metavar="NAME=JSON",
        help="Attach a JSON variable to the worker (repeatable); not with --vars-from. Variables require full-permission worker settings.",
    )
    pd.add_argument(
        "--vars-from",
        default=None,
        metavar="TARGET",
        help="Share the source worker's durable variables; live Python attachments cannot be shared this way. Not with --var. Variables require full-permission worker settings.",
    )
    pd_completion = pd.add_mutually_exclusive_group()
    pd_completion.add_argument("--no-wait", action="store_true",
                    help="Return once the worker has started (default).")
    pd_completion.add_argument("--wait", action="store_true",
                    help="Wait for the first turn and print the reply (needs a prompt).")
    pd_completion.add_argument("--notify-parent", action="store_true",
                    help="Return after startup; wake the calling Codex parent when the first turn settles, or when promptless creation completes.")
    pd.add_argument("--settle", action="store_true",
                    help="Codex only, with --notify-parent: also wait for the workers it "
                         "started with --notify-parent to settle.")
    pd.add_argument("--timeout", type=float, default=None,
                    help="Limit --wait to N seconds (default: no limit).")
    pd.add_argument("--json", action="store_true",
                    help="Print a JSON result (worker, worker_id, engine, ...) instead of the handle.")
    _add_cap_flags(pd)
    pd.set_defaults(func=cmd_new)

    pres = sub.add_parser(
        "resume", help="Reload a stopped worker without starting a turn.",
        description="Reload the worker's Claude session or Codex thread without "
                    "sending a prompt. Rarely needed: `ccx send` revives a stopped "
                    "worker by itself. To watch a Claude worker, use `ccx attach`.",
    )
    pres.add_argument("target", help=_WORKER_QUERY_HELP)
    _add_json_flag(pres)
    pres.set_defaults(func=cmd_resume)

    pf = sub.add_parser(
        "fork",
        help="Create a new worker from a copy of another worker's history.",
        description=(
            "The new worker gets the source's history up to its last finished "
            "turn, plus its engine, cwd and settings. The source is unchanged and "
            "must not be mid-turn. Without a prompt nothing is sent and no tokens "
            "are spent. Variables are not copied; use --vars-from to share them."
        ),
        epilog=_examples(
            'ccx fork "$W" -n try-b "Try the other approach" --wait',
        ),
    )
    pf.add_argument("target", help=_WORKER_QUERY_HELP)
    pf.add_argument("prompt", nargs="?", help="First prompt for the fork (omit to only fork).")
    pf.add_argument("--stdin", action="store_true", help="Read the first prompt from stdin.")
    pf.add_argument("--file", default=None, help="Read the first prompt from a file (`-` means stdin).")
    pf.add_argument("-n", "--name", default=None, help="Name for the fork (default: '<source name> (fork)').")
    pf.add_argument("--worker-id", default=None,
                    help="Use this UUIDv4 as the fork's id; a retry reuses the same fork.")
    pf.add_argument("--delivery-id", default=None,
                    help="Only create:initial, the default; to retry a fork, reuse --worker-id.")
    pf.add_argument("--model", default=None, help="Model for the fork (default: the source's).")
    pf.add_argument("--effort", default=None, choices=["low", "medium", "high", "xhigh", "max"],
                    help="Reasoning effort for the fork (default: the source's).")
    pf.add_argument("--var", action="append", default=None, metavar="NAME=JSON",
                    help="Attach a JSON variable (repeatable); not with --vars-from. Variables require full-permission worker settings.")
    pf.add_argument("--vars-from", default=None, metavar="TARGET",
                    help="Share the source worker's durable variables; live Python attachments cannot be shared this way. Not with --var. Variables require full-permission worker settings.")
    pf_completion = pf.add_mutually_exclusive_group()
    pf_completion.add_argument("--no-wait", action="store_true",
                               help="Return once the fork exists (default).")
    pf_completion.add_argument("--wait", action="store_true",
                               help="Wait for the first turn and print the reply (needs a prompt).")
    pf_completion.add_argument("--notify-parent", action="store_true",
                               help="Return after startup; wake the calling Codex parent when the first turn settles, or when promptless creation completes.")
    pf.add_argument("--settle", action="store_true",
                    help="Codex only, with --notify-parent: also wait for the workers it "
                         "started with --notify-parent to settle.")
    pf.add_argument("--timeout", type=float, default=None,
                    help="Limit the whole fork, including --wait, to N seconds.")
    pf.add_argument("--json", action="store_true",
                    help="Print a JSON result instead of the handle.")
    _add_cap_flags(pf)
    pf.set_defaults(func=cmd_fork)

    ps = sub.add_parser(
        "send",
        help="Start a turn on an idle worker (revives a stopped one).",
        description=(
            "Send a prompt as a new turn and, by default, wait for it. Text output "
            "is the worker handle, a delivery_id: line, then the reply; scripts "
            "that only want the reply can use --json or `ccx report`. A stopped "
            "worker is revived first. A busy worker is "
            "refused (use `ccx steer`, or wait); one waiting for input is refused "
            "(use answer, approve, reject or respond)."
        ),
        epilog=_examples(
            'ccx send "$W" "Now add a regression test"',
            'ccx send "$W" --no-wait --file task.md',
            'ccx send "$W" "Continue" --notify-parent   # Codex parent',
        ),
    )
    ps.add_argument("target", help=_WORKER_QUERY_HELP)
    ps.add_argument("text", nargs="?", help="Prompt text (or use --stdin / --file).")
    ps.add_argument("--stdin", action="store_true", help="Read the prompt from stdin.")
    ps.add_argument("--file", default=None, help="Read the prompt from a file (`-` means stdin).")
    ps.add_argument(
        "--var",
        action="append",
        default=None,
        metavar="NAME=JSON",
        help="Attach or replace a JSON variable (repeatable); not with --vars-from. Variables require full-permission worker settings.",
    )
    ps.add_argument(
        "--vars-from",
        default=None,
        metavar="TARGET",
        help="Share the source worker's durable variables; live Python attachments cannot be shared this way. Not with --var. Variables require full-permission worker settings.",
    )
    ps_completion = ps.add_mutually_exclusive_group()
    ps_completion.add_argument("--no-wait", action="store_true",
                    help="Return once the message is queued for delivery, without waiting "
                         "for the turn; follow up with ccx wait, report or reconcile.")
    ps_completion.add_argument("--wait", action="store_true",
                    help="Wait for the turn and print the reply (default).")
    ps_completion.add_argument("--notify-parent", action="store_true",
                    help="Return now; wake the calling Codex parent when the turn settles.")
    ps.add_argument("--settle", action="store_true",
                    help="Codex only, with --notify-parent: also wait for the workers it "
                         "started with --notify-parent to settle.")
    ps.add_argument(
        "--delivery-id", default=None,
        help="Id for this message (generated if omitted). Reusing it never sends "
             "twice; look it up with `ccx reconcile`.",
    )
    ps.add_argument("--timeout", type=float, default=None,
                    help="Stop waiting after N seconds (default: no limit); the turn "
                         "keeps running and the exit code is 6.")
    _add_json_flag(ps)
    _add_cap_flags(ps)
    ps.set_defaults(func=cmd_send)

    pvars = sub.add_parser(
        "vars",
        help="Read a worker's variables.",
        description=(
            "Without NAME, list the worker's variable names and metadata. With "
            "NAME, print that variable's JSON value. Variables are attached with "
            "--var or shared with --vars-from on new, fork and send; inside the "
            "worker, `python3` reads and updates them. Live values from a running "
            "controller aren't readable here."
        ),
    )
    pvars.add_argument("target", help=_WORKER_QUERY_HELP)
    pvars.add_argument("name", nargs="?", help="Variable to print.")
    pvars.add_argument(
        "--json",
        action="store_true",
        help="Print JSON (recommended for scripts).",
    )
    pvars.set_defaults(func=cmd_vars)

    pst = sub.add_parser(
        "steer",
        help="Add a message to a busy worker's running turn.",
        description=(
            "For a worker in the middle of a turn; an idle worker needs `ccx "
            "send`.\n\n"
            "Claude: steer watches up to --timeout seconds for where the message "
            "landed. If it's still pending after that, don't resend: check it "
            "with `ccx reconcile` and the same delivery id.\n\n"
            "Codex: the running turn accepts or rejects the message at once.\n\n"
            "Caps on a steer apply to the running turn the message joins."
        ),
    )
    pst.add_argument("target", help=_WORKER_QUERY_HELP)
    pst.add_argument("text", nargs="?", help="Message text (or use --stdin / --file).")
    pst.add_argument("--stdin", action="store_true", help="Read the message from stdin.")
    pst.add_argument("--file", default=None, help="Read the message from a file (`-` means stdin).")
    pst.add_argument("--no-wait", action="store_true",
                     help="Return as soon as the message is accepted.")
    pst.add_argument("--timeout", type=float, default=None,
                     help="Claude only: seconds to watch for where the message landed "
                          "(default 120). Running out is not a failure.")
    pst.add_argument("--delivery-id", dest="delivery_id", default=None,
                     help="Id for this message (generated if omitted); reusing it "
                          "never sends twice.")
    _add_json_flag(pst)
    # Same per-message caps as `send`: a steer's text runs inside a turn, so it
    # can carry the same quota ceiling. The cap is journaled with the delivery.
    _add_cap_flags(pst)
    pst.set_defaults(func=cmd_steer, short=None)

    prec = sub.add_parser(
        "reconcile",
        help="Look up what happened to an earlier message; never sends.",
        description=(
            "Look up an earlier send or steer without resending it. Use its delivery "
            "ID, or, for Codex, its native turn ID."
        ),
    )
    prec.add_argument("target", help=_WORKER_QUERY_HELP)
    source = prec.add_mutually_exclusive_group(required=True)
    source.add_argument("--delivery-id", metavar="ID", default=None,
                        help="Delivery id from send or steer.")
    source.add_argument("--turn-id", metavar="TURN_ID", default=None,
                        help="Codex only: Codex turn id.")
    _add_json_flag(prec)
    prec.set_defaults(func=cmd_reconcile)

    pw = sub.add_parser(
        "wait",
        help="Wait until a worker is idle, needs input, or stops.",
        description=(
            "Returns at once if the worker is already idle; otherwise waits with "
            "no time limit unless --timeout is given. The exit code tells the "
            "outcome: 0 idle, 3 dead or stopped, 4 waiting for input, "
            "6 timed out or the turn failed, 7 stopped by a usage cap "
            "(see `ccx --help`). For a person or a Claude parent; a Codex parent "
            "should use `ccx notify` or --notify-parent instead."
        ),
        epilog=_examples(
            'ccx wait "$W" --timeout 600',
            'ccx wait "$W" --next-turn   # Claude: the turn a background task starts',
        ),
    )
    pw.add_argument("target", help=_WORKER_QUERY_HELP)
    _add_json_flag(pw)
    pw.add_argument("--next-turn", dest="next_turn", action="store_true",
                    help="Claude only: wait for the next turn that starts after this "
                         "command (for example one woken by a background task), even if "
                         "the worker is idle now. Also returns on a new input request "
                         "(exit 4) or if the worker dies (exit 8/9 for API errors).")
    pw.add_argument("--after-turn", dest="after_turn", default=None, metavar="REF",
                    help="Claude only, with --next-turn: the last turn you saw "
                         "(`last_settled_turn.turn_ref` from `wait --json`). Returns at "
                         "once if a newer turn has finished or input is pending.")
    pw.add_argument("--known-input", dest="known_input", default=None, metavar="ID",
                    help="Claude only, with --next-turn: an input request you already saw "
                         "(`waiting_for.id`); it doesn't count as new.")
    pw.add_argument("--timeout", type=float, default=None,
                    help="Stop waiting after N seconds (default: no limit); exits 6.")
    pw.add_argument("--settle", action="store_true",
                    help="Codex only: also wait for descendant notifications and the parent turns they trigger to finish.")
    pw.add_argument("--wake-grace", type=float, default=None,
                    help="Codex only, with --settle: seconds a delivered child "
                         "notification may take to wake the worker (default 120).")
    pw.set_defaults(func=cmd_wait)

    pn = sub.add_parser(
        "notify",
        help="Wake the calling Codex parent when a worker's work settles.",
        description=(
            "For a Codex parent: arm a notification for a Claude or Codex "
            "worker's current turn (or running compaction), then end your turn. "
            "CCX resumes you when it settles. Starts no turn. A person or a "
            "Claude parent uses `ccx wait` instead."
        ),
    )
    pn.add_argument("target", help=_WORKER_QUERY_HELP)
    pn.add_argument(
        "--next-turn", dest="next_turn", action="store_true",
        help=(
            "Claude only: notify on the worker's next turn instead, for an idle "
            "worker that a background task will wake later."
        ),
    )
    _add_json_flag(pn)
    pn.add_argument("--settle", action="store_true",
                    help="Codex only: also wait for the workers it started with --notify-parent to settle.")
    pn.set_defaults(func=cmd_notify)


def _add_input_parsers(sub) -> None:
    """Answering a worker's input requests: answer, approve, respond, reject."""
    pans = sub.add_parser(
        "answer",
        help="Answer a Claude worker's question (AskUserQuestion).",
        description=(
            "Claude only. `ccx peek TARGET --status --json` shows the pending question "
            "and its options under waiting_for. "
            "Permission requests use approve/reject; MCP server forms use respond."
        ),
        epilog=_examples(
            'ccx answer "$W" "Option label" --wait',
            'ccx answer "$W" --option 1 "Alpha" --option 2 "Beta"',
        ),
    )
    pans.add_argument("target", help=_WORKER_QUERY_HELP)
    pans.add_argument("text", nargs="?", help="Answer to a single question: an option label or free text.")
    pans.add_argument("--request-id", default=None,
                      help="Question id (default: the pending one; see `ccx peek --status`).")
    pans.add_argument("--stdin", action="store_true", help="Read the answer from stdin.")
    pans.add_argument("--file", default=None, help="Read the answer from a file (`-` means stdin).")
    pans.add_argument(
        "--option",
        nargs=2,
        action="append",
        metavar=("N", "LABEL"),
        default=None,
        help=(
            "Pick option LABEL for question N (1-based). Repeat for "
            "multi-select or several questions."
        ),
    )
    pans.add_argument(
        "--text-answer",
        nargs=2,
        action="append",
        metavar=("N", "TEXT"),
        default=None,
        help="Free-text answer for question N (1-based).",
    )
    pans_completion = pans.add_mutually_exclusive_group()
    pans_completion.add_argument("--no-wait", action="store_true", help="Return right away (default).")
    pans_completion.add_argument("--wait", action="store_true", help="Wait for the resumed turn and print the reply.")
    pans_completion.add_argument("--notify-parent", action="store_true", help="Return now; wake the calling Codex parent when the resumed turn settles.")
    pans.add_argument("--timeout", type=float, default=None, help="Limit --wait to N seconds (default: no limit).")
    _add_json_flag(pans)
    pans.set_defaults(func=cmd_answer)

    pa = sub.add_parser(
        "approve",
        help="Allow a worker's pending permission request.",
        description=(
            "Only workers created with --permissions restricted (or a Codex "
            "approval policy that asks) make permission requests. `ccx peek` "
            "shows the pending request."
        ),
    )
    pa.add_argument("target", help=_WORKER_QUERY_HELP)
    pa.add_argument("--request-id", default=None,
                    help="Request id (default: the only pending one; see `ccx peek --status`).")
    pa_completion = pa.add_mutually_exclusive_group()
    pa_completion.add_argument("--no-wait", action="store_true", help="Return right away (default).")
    pa_completion.add_argument("--wait", action="store_true", help="Claude only: wait for the resumed turn and print the reply.")
    pa_completion.add_argument("--notify-parent", action="store_true", help="Return now; wake the calling Codex parent when the resumed turn settles.")
    pa.add_argument("--timeout", type=float, default=None, help="Claude only: limit --wait to N seconds (default: no limit).")
    _add_json_flag(pa)
    pa.add_argument("--settle", action="store_true",
                    help="Codex only, with --notify-parent: also wait for the workers it "
                         "started with --notify-parent to settle.")
    pa.set_defaults(func=cmd_approve)

    pe = sub.add_parser(
        "respond",
        help="Answer a Claude worker's MCP server form (elicitation).",
        description=(
            "Claude only. An MCP server asked for input mid-tool; `ccx peek` "
            "shows its message and fields. --accept submits the form (fields from "
            "--field and/or --content); --decline says no, which the server may "
            "remember; --cancel dismisses it without answering."
        ),
        epilog=_examples(
            'ccx respond "$W" --accept --field name=value --wait',
            'ccx respond "$W" --decline',
        ),
    )
    pe.add_argument("target", help=_WORKER_QUERY_HELP)
    pe_action = pe.add_mutually_exclusive_group(required=True)
    pe_action.add_argument("--accept", action="store_true", help="Submit the form.")
    pe_action.add_argument("--decline", action="store_true", help="Say no.")
    pe_action.add_argument("--cancel", action="store_true", help="Dismiss without answering.")
    pe.add_argument("--field", action="append", default=None, metavar="NAME=VALUE",
                    help="One form field, typed by the server's schema; repeat for more "
                         "(and for multi-select lists).")
    pe.add_argument("--content", default=None, metavar="JSON",
                    help="The whole form as a JSON object (--field values override it).")
    pe.add_argument("--request-id", default=None,
                    help="Form id (default: the pending one; see `ccx peek --status`).")
    pe_completion = pe.add_mutually_exclusive_group()
    pe_completion.add_argument("--no-wait", action="store_true", help="Return right away (default).")
    pe_completion.add_argument("--wait", action="store_true", help="Wait for the resumed turn and print the reply.")
    pe_completion.add_argument("--notify-parent", action="store_true", help="Return now; wake the calling Codex parent when the resumed turn settles.")
    pe.add_argument("--timeout", type=float, default=None, help="Limit --wait to N seconds (default: no limit).")
    _add_json_flag(pe)
    pe.set_defaults(func=cmd_respond)

    pr = sub.add_parser(
        "reject",
        help="Deny a worker's pending permission request.",
        description="The worker's turn continues without the denied action. "
                    "`ccx peek` shows the pending request.",
    )
    pr.add_argument("target", help=_WORKER_QUERY_HELP)
    pr.add_argument("--request-id", default=None,
                    help="Request id (default: the only pending one; see `ccx peek --status`).")
    pr.add_argument("--reason", default=None, help="Claude only: reason shown to Claude.")
    pr_completion = pr.add_mutually_exclusive_group()
    pr_completion.add_argument("--no-wait", action="store_true", help="Return right away (default).")
    pr_completion.add_argument("--wait", action="store_true", help="Claude only: wait for the resumed turn and print the reply.")
    pr_completion.add_argument("--notify-parent", action="store_true", help="Return now; wake the calling Codex parent when the resumed turn settles.")
    pr.add_argument("--timeout", type=float, default=None, help="Claude only: limit --wait to N seconds (default: no limit).")
    _add_json_flag(pr)
    pr.add_argument("--settle", action="store_true",
                    help="Codex only, with --notify-parent: also wait for the workers it "
                         "started with --notify-parent to settle.")
    pr.set_defaults(func=cmd_reject)


def _add_inventory_parsers(sub) -> None:
    """Finding and reading workers: import, attach, list, logs, report, context;
    plus `skills`, which installs the bundled agent skills."""
    pskills = sub.add_parser(
        "skills",
        help="Install the CCX agent skill for Claude Code, Codex and other agents.",
        description=(
            "Copy the CCX agent skill into agent skill folders: Claude Code's "
            "(~/.claude/skills) when Claude Code is installed, and the shared "
            "~/.agents/skills (Codex and other agents) when Codex is installed or "
            "that folder exists. Copies CCX made are refreshed automatically "
            "after a CCX upgrade; folders CCX didn't create are never replaced "
            "without --force."
        ),
        epilog=_examples(
            "ccx skills install",
            "ccx skills install --dir ~/.config/myagent/skills",
            "ccx skills uninstall",
        ),
    )
    skills_sub = pskills.add_subparsers(dest="skills_cmd", metavar="{install,uninstall}")
    sinstall = skills_sub.add_parser(
        "install", help="Install or update the skill.",
        description="Install or update the CCX skill in the default folders, or in --dir.")
    sinstall.add_argument("--dir", metavar="DIR",
                          help="Install into this skills folder instead of the defaults.")
    sinstall.add_argument("--force", action="store_true",
                          help="Replace a skill folder CCX didn't create.")
    suninstall = skills_sub.add_parser(
        "uninstall", help="Remove the skill copies CCX installed.",
        description="Remove the CCX skill from the default folders, or from --dir. "
                    "Only folders CCX installed are removed.")
    suninstall.add_argument("--dir", metavar="DIR",
                            help="Remove from this skills folder instead of the defaults.")
    pskills.set_defaults(func=skills.cmd_skills, skills_parser=pskills)

    pi = sub.add_parser(
        "import",
        help="Import a Claude session or copy a Codex thread into a CCX worker.",
        description=(
            "Claude: resume an idle interactive session as a managed worker; stop "
            "using the old terminal. Codex: fork the last completed turn into a "
            "new full-permission CCX worker, preserving inherited history and "
            "leaving the original Codex thread untouched. Importing the same "
            "Codex thread again creates another independent fork."
        ),
    )
    pi.add_argument("session", help="Exact full native Claude session or Codex thread UUID.")
    pi.add_argument("--engine", choices=["claude", "codex"],
                    help="Select the engine explicitly (default: detect from exact native ID).")
    pi.add_argument("--model", help="Codex child model override.")
    pi.add_argument("--effort", help="Codex child reasoning-effort override.")
    pi.set_defaults(func=cmd_import)

    pat = sub.add_parser(
        "attach",
        help="Open a Claude or Codex worker in your terminal.",
        description=(
            "Needs a terminal. A stopped worker is revived; CCX commands remain "
            "available while attached. For both engines, Ctrl-Z detaches and "
            "leaves the worker running. Several terminals can attach at once; "
            "a Claude worker then uses the smallest one's size. The CCX status "
            "bar is shown unless --no-status. Codex runs its real native UI; "
            "/quit also exits. Codex may first show its own startup prompts. "
            "Model/permission changes made there do not "
            "change CCX's recorded settings for its own sends."
        ),
    )
    pat.add_argument("target", help=_WORKER_QUERY_HELP)
    pat.add_argument("--no-status", action="store_true",
                     help="Hide the CCX status bar for Claude or Codex.")
    pat.set_defaults(func=cmd_attach)

    pl = sub.add_parser(
        "list",
        help="List workers.",
        description=(
            "List Claude and Codex workers with their handles, names and states. "
            "States come from CCX's records, not a live check."
        ),
    )
    _add_json_flag(pl)
    pl.add_argument("--engine", choices=("claude", "codex"), default=None,
                    help="Only workers of this engine.")
    pl.add_argument("--cwd", default=None,
                    help="Only workers whose cwd is exactly this directory.")
    pl.add_argument("--cwd-prefix", "--dir", dest="cwd_prefix", default=None,
                    help="Only workers whose cwd is this directory or inside it.")
    pl.add_argument("--state", default=None,
                    choices=("starting", "idle", "busy", "waiting", "stopped",
                             "failed", "unavailable"),
                    help="Only workers in this state.")
    pl.add_argument("--active", action="store_true",
                    help="Only workers recorded as starting, idle, busy or waiting.")
    pl.add_argument("--sendable", action="store_true",
                    help="Only workers recorded as idle or stopped (send still checks).")
    pl.add_argument("--recent-hours", dest="recent_hours", type=float, default=None,
                    help="Only workers created or active in the last N hours.")
    pl.add_argument("-n", "--limit", type=int, default=None,
                    help="Only the N most recently active workers, newest first.")
    pl.set_defaults(func=cmd_list)

    pg = sub.add_parser(
        "logs",
        help="Print a worker's whole conversation as JSON.",
        description=(
            "Print the conversation as JSON, also for stopped workers; nothing is "
            "sent. Tool results are cut to --max-tool-result-bytes unless --full. "
            "For a quick look, use `ccx peek` or `ccx report`."
        ),
    )
    pg.add_argument("target", help=_WORKER_QUERY_HELP)
    pg.add_argument("--tail", type=int, default=None, help="Only the last N turns. Only those turns are read, so they are unnumbered unless they are the whole conversation.")
    pg.add_argument("--format", choices=("conversation", "rendered"), default="conversation",
                    help="conversation (default) or rendered (deprecated; Claude only: "
                         "per-turn transcript with tool calls paired to their results). "
                         "To read a conversation as text, use `ccx history`.")
    pg.add_argument("--max-tool-result-bytes", dest="max_tool_result_bytes", type=int,
                    default=4096,
                    help="Cut each tool result to N bytes (default 4096).")
    pg.add_argument("--full", action="store_true",
                    help="Don't cut tool results.")
    pg.add_argument("--jsonl", action="store_true",
                    help="Claude only: print Claude's raw JSONL transcript.")
    pg.set_defaults(func=cmd_logs)

    ph = sub.add_parser(
        "history",
        help="Read or search workers' past conversations as text.",
        description=HISTORY_DESCRIPTION,
        epilog=HISTORY_EPILOG,
        formatter_class=_HelpFormatter,
    )
    ph.add_argument("targets", nargs="+", metavar="target",
                    help="One or more workers (find them with `ccx list`).")
    ph.add_argument("--turns", metavar="SPEC",
                    help="12, 10-14, 120-, -5 (the last five) or 184.0 (one message); "
                         "comma-separated.")
    ph.add_argument("--grep", metavar="REGEX",
                    help="Messages matching REGEX in the searched roles.")
    ph.add_argument("--cmd", dest="command_regex", metavar="REGEX",
                    help="Tool calls running a command whose name matches REGEX.")
    ph.add_argument("--errors", action="store_true", help="Failed tool calls only.")
    ph.add_argument("--role", metavar="ROLES",
                    help="Comma-separated roles to search and show; see below.")
    ph.add_argument("--since", metavar="TIME", help="From this time; see below.")
    ph.add_argument("--until", metavar="TIME",
                    help="Before this time; a date includes that whole day.")
    ph.add_argument("-C", "--context", type=int, metavar="N",
                    help="Read the matching turns instead of hit lines, with N turns around each.")
    ph.add_argument("--view", choices=("outline", "chat", "trace", "map"),
                    help="How to show turns; see above.")
    ph.add_argument("--limit", type=int, default=20, metavar="N",
                    help="Hit lines per worker, newest (default 20).")
    ph.add_argument("--max-chars", type=int, metavar="N",
                    help="Cut each message to N characters (default 3000; "
                         "one addressed message prints whole).")
    ph.add_argument("--full", action="store_true",
                    help="Don't cut messages, commands or tool output "
                         "(hit lines stay short).")
    ph.set_defaults(func=cmd_history)

    prep = sub.add_parser(
        "report",
        help="Print the last reply of one or more workers.",
        description=(
            "Print the latest turn's reply. A completed turn with no answer prints "
            "(turn completed; no reply); failed and interrupted turns without "
            "answers have matching markers. When an older confirmed reply is "
            "shown because a newer turn is still running or waiting for input, "
            "native history cannot be read, or a worker failed, a note appears "
            "on stderr (`note` in "
            "JSON). With several workers, "
            "text output has `=== <worker> ===` headers; the exit code is nonzero "
            "if any worker can't be reported, but the others still print."
        ),
    )
    prep.add_argument("targets", nargs="+", metavar="target",
                      help="One or more workers; an ambiguous prefix rejects the whole batch.")
    prep.add_argument("--max-chars", type=int, default=None,
                      help="Cut each reply to N characters.")
    prep.add_argument("--json", action="store_true",
                      help="Print JSON; several workers give {reports, errors, ok}.")
    prep.set_defaults(func=cmd_report)

    pctx = sub.add_parser(
        "context",
        help="Show how much context a worker is carrying.",
        description=(
            "Show the worker's latest context size from CCX's records. Works for "
            "idle, busy and stopped workers and never revives them. Right after "
            "a compaction, tokens_used can be null until the next reading; "
            "`reason` says why."
        ),
    )
    pctx.add_argument("target", help=_WORKER_QUERY_HELP)
    pctx.add_argument("--json", action="store_true",
                      help="Print {engine, worker, short, name, state, tokens_used, "
                           "window_size, percent_used, as_of_turn_ref, as_of_ms, reason}, "
                           "the same for both engines.")
    pctx.set_defaults(func=cmd_context)


def _add_control_parsers(sub) -> None:
    """Session control: kill, compact, set, stop, and machine-level shutdown
    and untrust."""
    pk = sub.add_parser(
        "kill",
        help="Stop a worker's runtime; history is kept and send revives it.",
        description=(
            "Cancel any running turn, then stop the worker and confirm it is "
            "gone: a Claude worker's process exits; a Codex worker is unloaded "
            "from the shared Codex daemon. The history is kept, so the next "
            "`ccx send` continues the same conversation. To cancel only the "
            "current turn, use `ccx stop`."
        ),
    )
    pk.add_argument("target", help=_WORKER_QUERY_HELP)
    pk.add_argument("--timeout", type=float, default=KILL_TIMEOUT_DEFAULT_S,
                    help="Seconds to confirm it stopped (default 90).")
    pk.add_argument("--json", action="store_true",
                    help="Print JSON instead of text.")
    pk.set_defaults(func=cmd_kill)

    pc = sub.add_parser(
        "compact",
        help="Compact a worker's context with the engine's own compaction.",
        description=(
            "Run the engine's native compaction (Claude's /compact) and wait for "
            "it. A stopped worker is revived first."
        ),
    )
    pc.add_argument("target", help=_WORKER_QUERY_HELP)
    pc.add_argument(
        "--operation-id", default=None,
        help="Claude only: id for this compaction (generated if omitted); reusing "
             "it checks the earlier one instead of compacting again.",
    )
    pc.add_argument("--settle", action="store_true",
                    help="Codex only, with --notify-parent: also wait for the workers it "
                         "started with --notify-parent to settle.")
    pc_completion = pc.add_mutually_exclusive_group()
    pc_completion.add_argument("--no-wait", action="store_true",
                    help="Return once compaction has started.")
    pc_completion.add_argument("--notify-parent", action="store_true",
                    help="Return now; wake the calling Codex parent when compaction settles.")
    pc.add_argument("--timeout", type=float, default=COMPACT_WAIT_DEFAULT_S,
                    help=f"Seconds to wait for compaction (default {COMPACT_WAIT_DEFAULT_S:g}).")
    _add_json_flag(pc)
    pc.set_defaults(func=cmd_compact)

    pset = sub.add_parser(
        "set",
        help="Rename a worker or change its model, effort or permissions.",
        description=(
            "A new name applies at once, in any state. Codex: model and effort "
            "need an idle or stopped worker and apply from its next turn. Claude: model, effort and permissions "
            "apply when the worker next starts, so a running worker is refused "
            "unless you add --restart (allowed when it is idle)."
        ),
    )
    pset.add_argument("target", help=_WORKER_QUERY_HELP)
    pset.add_argument(
        "--name",
        default=None,
        help='Display name; automatic naming won\'t replace it. --name "" restores '
             "the default name and automatic naming.",
    )
    pset.add_argument(
        "--model",
        default=None,
        help="Model for future turns; 'default' clears Codex's override and is "
             "passed as the model name for Claude.",
    )
    pset.add_argument(
        "--permissions",
        choices=["full", "restricted"],
        default=None,
        help="Claude only: full (no permission prompts) or restricted (requests "
             "go to `ccx approve`/`reject`).",
    )
    pset.add_argument(
        "--restart",
        action="store_true",
        help="Claude only: apply the changes now by stopping the idle worker; "
             "the next send revives it with them.",
    )
    pset.add_argument(
        "--effort",
        default=None,
        choices=["low", "medium", "high", "xhigh", "max", "default"],
        help="Reasoning effort for future turns ('default' clears it). On Claude, "
             "changing it mid-conversation makes the next turn re-read the whole "
             "context without the prompt cache.",
    )
    pset.add_argument("--json", action="store_true",
                      help="Print JSON instead of text.")
    pset.set_defaults(func=cmd_set)

    pstop = sub.add_parser(
        "stop",
        help="Request cancellation of the current turn; keep the worker running.",
        description=(
            "Request cancellation of the current turn; keep the worker running. Exit 0 "
            "means the interrupt was accepted. Inspect `confirmed` and `outcome` in "
            "--json to see whether cancellation has settled. Refused with exit 5 if the worker is idle, exit 3 if it isn't running."
        ),
    )
    pstop.add_argument("target", help=_WORKER_QUERY_HELP)
    pstop.add_argument("--json", action="store_true",
                       help="Print JSON instead of text.")
    pstop.set_defaults(func=cmd_stop)

    pshutdown = sub.add_parser(
        "shutdown",
        help="Stop all CCX runtimes, or one engine's.",
        description=(
            "Without --engine: kill every Claude worker, unload every Codex "
            "worker and stop the Codex daemon, then stop the notification "
            "daemon. Histories are kept and the next send revives a worker; "
            "pending parent notifications are delivered after CCX starts again. "
            "Claude sessions outside CCX and running controllers are left alone.\n\n"
            "With --engine, only that engine's workers stop and notifications "
            "keep running. For Codex, the daemon stops too unless workers CCX "
            "doesn't know about are still loaded."
        ),
    )
    pshutdown.add_argument("--engine", choices=("claude", "codex"),
                           help="Stop only this engine's workers.")
    pshutdown.add_argument("--dry-run", action="store_true",
                           help="List what would be stopped without stopping it.")
    pshutdown.add_argument("--timeout", type=float, default=KILL_TIMEOUT_DEFAULT_S,
                           help="Seconds each kill may take (default: %(default)s).")
    pshutdown.add_argument("--json", action="store_true",
                           help="Print JSON instead of text.")
    pshutdown.set_defaults(func=cmd_shutdown)

    puntrust = sub.add_parser(
        "untrust",
        help="Remove the folder trust CCX added to Claude's config.",
        description=(
            "Claude asks to trust each new folder; CCX records the trust it adds "
            "for worker cwds in .claude.json. This undoes exactly those changes "
            "(and restores any value CCX overrode). Entries changed since, or "
            "not added by CCX, are left alone. A running claude process can write "
            "the file back; if an entry returns, run it again with Claude closed."
        ),
    )
    puntrust.add_argument("paths", nargs="*", metavar="PATH",
                          help="Only these folders (default: every recorded one).")
    puntrust.add_argument("--dry-run", action="store_true",
                          help="Show what would change without writing.")
    puntrust.add_argument("--json", action="store_true",
                          help="Print JSON instead of text.")
    puntrust.set_defaults(func=cmd_untrust)


def _add_observability_parsers(sub) -> None:
    """Quota and session observation: usage, peek, events."""
    pm = sub.add_parser(
        "models",
        help="List the models Claude and Codex offer.",
        description=(
            "Show the models each engine offers: the id to pass to --model, the "
            "display name, the model a Claude alias points to now, and the efforts "
            "it supports. Lists are kept for 24 hours; Claude's is fetched again "
            "after a Claude Code update. If a list can't be fetched, the last one "
            "is shown, marked stale."
        ),
    )
    pm.add_argument("--engine", choices=("claude", "codex"),
                    help="Only this engine (default: both).")
    pm.add_argument("--json", action="store_true", help="Print the lists as JSON.")
    pm.add_argument("--refresh", action="store_true",
                    help="Fetch the lists now; exit 3 if one can't be fetched.")
    pm.set_defaults(func=cmd_models)

    pu = sub.add_parser(
        "usage",
        help="Show your Claude or Codex account usage.",
        description=(
            "Claude: 5-hour, 7-day and per-model quota use with reset times. "
            "Codex: account quota and, with --tokens, token activity."
        ),
    )
    pu.add_argument("--engine", choices=("claude", "codex"),
                    help="Read only this account (default: both).")
    usage_shape = pu.add_mutually_exclusive_group()
    usage_shape.add_argument("--json", action="store_true", help="Print the full usage data as JSON.")
    usage_shape.add_argument("--summary", action="store_true", help="Claude only: compact quota JSON (five_hour, seven_day, model_quotas, and extra_usage when present).")
    usage_shape.add_argument("--set-source", choices=("get_usage", "oauth"),
                             help="Claude only: save the usage source for all CCX processes sharing this root; fetches nothing. oauth is Linux-only.")
    pu.add_argument("--cached", action="store_true", help="Claude only: reuse a recent shared reading instead of fetching now.")
    pu.add_argument("--tokens", action="store_true",
                    help="Codex account token activity.")
    pu.set_defaults(func=cmd_usage)

    ppk = sub.add_parser(
        "peek",
        help="Show a worker's recent activity (--status: just its state).",
        description=(
            "Show the last user and assistant messages, recent tool calls, any "
            "pending question or permission request, and warnings. Works in any "
            "state and never starts a turn. --status skips the conversation and "
            "reads only CCX's records, for one or many workers."
        ),
        epilog=_examples(
            'ccx peek "$W"',
            'ccx peek "$W1" "$W2" --status --json',
        ),
    )
    ppk.add_argument("target", nargs="+", metavar="target", action=_PeekTargets,
                     help=_WORKER_QUERY_HELP +
                          " More targets are allowed only with --status.")
    ppk.add_argument("--status", action="store_true",
                     help="Only status fields (engine, worker, short, state, turn, "
                          "last_turn_age_s, context with tokens_used, and pending "
                          "input: Claude waiting_for, Codex pending_input), no "
                          "conversation text. --json prints {statuses: [...]}.")
    ppk.add_argument("--tail", type=int, default=4,
                     help="How many recent tool calls to show (default 4; 0 for none).")
    ppk.add_argument("--since-turn", type=int, default=None,
                     help="Only turns from number N on (1-based).")
    ppk.add_argument("--tools-only", action="store_true", help="Show only the recent tool calls.")
    ppk.add_argument("--text-only", action="store_true", help="Show only the last user and assistant messages.")
    ppk.add_argument("--full", action="store_true", help="Don't shorten messages or tool errors.")
    ppk.add_argument("--max-chars", type=int, default=None,
                     help="Character limit for the text view (default 8000; none with --full).")
    ppk.add_argument("--json", action="store_true", help="Print JSON.")
    ppk.add_argument("--brief", action="store_true",
                     help="Short view: state, cwd, a preview of the last reply, tool "
                          "names and any pending input. Ignored with --json.")
    ppk.set_defaults(func=cmd_peek)

    pev = sub.add_parser(
        "events",
        help="Read or follow a worker's lifecycle events.",
        description=(
            "State changes, turn start and end, tool calls, input requests, "
            "compactions and errors, without message content. Each event has a "
            "`cursor`; pass it to --after to continue from there. For "
            "monitoring; to wait for a result, use `ccx wait` or `ccx notify`."
        ),
    )
    pev.add_argument("target", help=_WORKER_QUERY_HELP)
    _add_events_args(pev)
    pev.set_defaults(func=cmd_events)


def _add_internal_parsers(sub) -> None:
    """Internal plumbing re-invoked by CCX itself: hooks/statusline relays, the host, live-vars exec."""
    ph = sub.add_parser("hooks-relay", help=argparse.SUPPRESS)
    ph.add_argument("short")
    ph.add_argument("event")
    ph.set_defaults(func=cmd_hooks_relay)

    psline = sub.add_parser("statusline-relay", help=argparse.SUPPRESS)
    psline.add_argument("short")
    psline.set_defaults(func=cmd_statusline_relay)

    phost = sub.add_parser("host", help=argparse.SUPPRESS)
    phost.add_argument("short")
    phost.add_argument("--detach", action="store_true",
                       help="Daemonize: double-fork the real host (reparented to "
                            "init), wait for its socket, print a one-line JSON "
                            "verdict, and exit. Used by spawn/revive so library "
                            "callers never fork in their own process.")
    phost.add_argument("--sock-wait", dest="sock_wait", type=float,
                       default=HOST_SOCK_WAIT_NEW_S,
                       help="With --detach: max seconds to wait for the host "
                            "socket before reporting sock-timeout.")
    phost.set_defaults(func=cmd_host)

    plivevars = sub.add_parser(
        "_live-vars-exec",
        help=argparse.SUPPRESS,
        add_help=False,
    )
    plivevars.add_argument("--root", default=None)
    plivevars.add_argument("--engine", required=True, choices=["claude", "codex"])
    plivevars.add_argument("--short", required=True)
    plivevars.add_argument("--invoked-as", required=True, choices=["python", "python3"])
    plivevars.add_argument("target_args", nargs=argparse.REMAINDER)
    plivevars.set_defaults(func=live_vars.cmd_worker_exec)


class _VersionAction(argparse.Action):
    """`-V/--version` for --help; resolves the version only when used.

    cli.main answers a bare `ccx --version` before building the parser so it
    never creates ~/.ccx; this covers the remaining argparse paths.
    """

    def __init__(self, option_strings, dest=argparse.SUPPRESS, default=argparse.SUPPRESS, help=None):
        super().__init__(option_strings, dest=dest, default=default, nargs=0, help=help)

    def __call__(self, parser, namespace, values, option_string=None):
        print(f"ccx {ccx_version()}")
        parser.exit()


def build_parser():
    p = _CcxArgumentParser(
        prog="ccx",
        usage="ccx [-h] [-V] <command> ...",
    )
    p.add_argument("-V", "--version", action=_VersionAction, help="Show the ccx version and exit.")
    sub = p.add_subparsers(dest="cmd", required=True, metavar="<command>", prog="ccx")

    # Fresh CCX only; distinct from I3 migration/cutover of an existing root.
    # Parsing --help/--version and failing --root must NEVER mutate CCX state.
    from ccx.cli.commands.create import cmd_init as _cmd_init_full_uuid
    init = sub.add_parser(
        "init",
        help="Create the store up front (optional: the first command creates it).",
        description=(
            "Create an empty store at --root, $CCX_ROOT or ~/.ccx. Optional: "
            "commands that need a store create it when it's missing. Refused if a store "
            "already exists there. Other commands find the store through "
            "$CCX_ROOT, so set it when you use --root."
        ),
    )
    init.add_argument("--root",
                      help="Absolute store directory (default: $CCX_ROOT or ~/.ccx).")
    init.add_argument("--json", action="store_true",
                      help="Print the new store's details as JSON.")
    init.set_defaults(func=_cmd_init_full_uuid)

    prpc = sub.add_parser(
        "rpc",
        help="Local JSON protocol for applications (requests on stdin).",
        description=(
            "Read one JSON request from stdin and write one JSON response line "
            "to stdout; with --serve, handle newline-delimited requests until "
            "stdin closes. Protocol version 2:\n\n"
            '  {"protocol_version": 2, "request_id": "r1", "op": "worker.inspect",\n'
            '   "params": {"target": "<worker>"}}\n\n'
            "Operations: " + ", ".join(sorted(local_rpc._FULL_UUID_SUPPORTED_OPS))
            + "; with --serve also " + local_rpc.CANCEL_OP + "."
        ),
    )
    prpc.add_argument(
        "--serve",
        action="store_true",
        help=(
            "Keep reading requests; responses are written as each completes and "
            "matched by request_id."
        ),
    )
    prpc.set_defaults(func=local_rpc.cmd_rpc)

    _add_controller_parsers(sub)
    _add_session_parsers(sub)
    _add_input_parsers(sub)
    _add_inventory_parsers(sub)
    _add_control_parsers(sub)
    _add_observability_parsers(sub)
    _add_internal_parsers(sub)

    # The grouped overview replaces argparse's flat command list; internal
    # commands (help=SUPPRESS) still parse but are listed nowhere.
    p.description = OVERVIEW + "\n\n" + _command_overview(sub)
    p.epilog = ENVIRONMENT + "\n\n" + EXIT_CODES
    sub._choices_actions = []
    sub.help = argparse.SUPPRESS
    return p
