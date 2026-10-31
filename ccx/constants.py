from __future__ import annotations

import os
import pathlib
import shutil

from ccx.store.root import canonical_root

# Roots are made absolute once, here: the managed host chdirs into the worker
# cwd before its first path write, and the hooks relay runs in that cwd too, so
# a relative root would name a different directory in each process.
# Canonicalize ancestors only; a linked root leaf must fail the no-follow walk.
_ORIGINAL_CCX_ROOT_ENV = os.environ.get("CCX_ROOT")
CCX_ROOT = canonical_root(os.environ.get("CCX_ROOT") or "~/.ccx")


def select_root(path: pathlib.Path | str) -> pathlib.Path:
    """Select an explicit process root and pin the same identity for children."""
    global CCX_ROOT
    CCX_ROOT = canonical_root(path)
    os.environ["CCX_ROOT"] = str(CCX_ROOT)
    return CCX_ROOT

_CLAUDE_CONFIG_OVERRIDE = os.environ.get("CLAUDE_CONFIG_DIR")
CLAUDE_HOME = pathlib.Path(os.path.realpath(os.path.expanduser(_CLAUDE_CONFIG_OVERRIDE or "~/.claude")))
# Always pin the canonical root for detached hosts, daemon and hook children,
# even when the caller used the default rather than exporting CCX_ROOT.
os.environ["CCX_ROOT"] = str(CCX_ROOT)
if _CLAUDE_CONFIG_OVERRIDE:
    os.environ["CLAUDE_CONFIG_DIR"] = str(CLAUDE_HOME)
# Claude 2.1.293 reads global metadata (including per-cwd folder trust) from
# the effective native profile. With a custom config directory, native Claude
# reads <CLAUDE_CONFIG_DIR>/.claude.json, NOT HOME/.claude.json. Both initial
# worker creation and respawn call settings.ensure_trusted_paths.
CLAUDE_DOTFILE = (
    CLAUDE_HOME / ".claude.json"
    if _CLAUDE_CONFIG_OVERRIDE
    else pathlib.Path(os.path.expanduser("~/.claude.json"))
)
def _default_claude_bin() -> str:
    # The native installer's location first; npm/Homebrew installs put claude
    # elsewhere on PATH. Keep the native path when neither exists so error
    # messages name the conventional location.
    native = os.path.expanduser("~/.local/bin/claude")
    if os.path.exists(native):
        return native
    return shutil.which("claude") or native


CLAUDE_BIN = os.environ.get("CCX_CLAUDE_BIN") or _default_claude_bin()

SCHEMA_VERSION = 1
# Long-lived clients and daemons can span package upgrades. Fence requests and
# replies in both directions; bump when required fields or operation meanings change.
CODEXD_RPC_GENERATION = 21
# Inspection and teardown answer any client, whatever generation it speaks:
# they are how an operator sees an incompatible daemon and gets rid of it.
# Every other operation is fenced in both directions before it runs.
CODEXD_RECOVERY_OPS = frozenset({
    "status", "compact-observe", "kill", "interrupt", "daemon-status", "daemon-stop",
})
# Notification supervisors also outlive package upgrades.
NOTIFYD_PROTOCOL = 19
# notifyd is a codexd client, so a codexd RPC bump must also retire a running
# notifyd (otherwise it keeps old code and its Codex parent wakes are deferred).
NOTIFYD_GENERATION = NOTIFYD_PROTOCOL * 100 + CODEXD_RPC_GENERATION
# Exact managed caller full identity replaces old short-routing authority.
SELF_WORKER_ID_ENV = "CCX_SELF_WORKER_ID"
SELF_ENGINE_ENV = "CCX_SELF_ENGINE"
SELF_SHORT_ENV = "CCX_SELF_SHORT"  # historical, refused by new runtime
DEFAULT_IDLE_TIMEOUT_S = 30 * 60      # 30 minutes
# A worker sitting IDLE while a `Bash(run_in_background)` process or Monitor
# keeps running would otherwise be reaped by the idle-killer at
# DEFAULT_IDLE_TIMEOUT_S, taking the process tree down with it. Background work
# and monitors can legitimately run for days, so the killer holds off entirely
# while any bg task is still running (no cap) — this is how often it re-parses
# the transcript to recheck, once past the idle threshold. Cheap because it only
# runs while a worker is already idle-overdue.
BG_IDLE_RECHECK_S = 60.0
# Boot watchdog: a worker leaves S.BOOTING only on a SessionStart hook. If Claude
# blocks before that (e.g. an un-pre-seeded --add-dir trust dialog or onboarding
# gate) no hook ever fires and the idle-killer — gated on S.IDLE — never reaps it,
# so the boot hangs indefinitely. Healthy boots fire SessionStart within seconds;
# 90s is generously above observed boot times. Env-overridable for tests/tuning.
BOOT_TIMEOUT_S = float(os.environ.get("CCX_BOOT_TIMEOUT_S", 90))
DEFAULT_PTY_COLS = 120
DEFAULT_PTY_ROWS = 40
WARMUP_S = 4.0                        # post-IDLE settle before allowing sends (after a turn)
PTY_QUIESCENCE_S = 1.5                # PTY must be quiet this long after IDLE-entry → Ink-ready
PTY_WRITE_TIMEOUT_S = 2.0             # bounded drain for a complete write to the non-blocking PTY
PASTE_SUBMIT_DELAY_S = 0.01           # let Ink apply a completed bracketed paste before Enter
# Ink turns large/multiline bracketed pastes into a collapsed attachment
# asynchronously. An Enter sent during that conversion is silently ignored.
# Only the single-paste fallback (see PASTE_INLINE_*) collapses: it stages large
# IDLE sends and delays submission in proportion to their UTF-8 size while the
# host continues draining PTY output.
PASTE_STAGED_THRESHOLD_BYTES = 8 * 1024
PASTE_STAGED_BYTES_PER_S = 2 * 1024
PASTE_STAGED_MAX_DELAY_S = 30.0
# Claude collapses a bracketed paste longer than 800 UTF-16 units or with more
# than min(rows - 10, 2) line breaks into a `[Pasted text]` attachment, and
# submits attachments wrapped in <pasted_content> tags that its system prompt
# tells the model to treat as possibly not user-authored. Smaller pastes are
# inserted as typed text, so prompts are framed as a run of pastes under both
# limits (2.1.294: FY=800, S8=min(rows-10,2)). The unit cap keeps a margin.
PASTE_INLINE_MAX_UNITS = 640
PASTE_INLINE_ROWS_RESERVED = 10
PASTE_INLINE_MAX_NEWLINES = 2
SEND_FIRST_ACK_S = 5.0                # if no UserPromptSubmit within this of first paste, retry once
SEND_RETRY_ACK_S = 30.0               # if no UPS within this of retry paste, CRASHED
CLEAR_INPUT_MIN_GAP_S = 1.5           # min gap between input-clearing Ctrl-C presses: Claude
                                      # exits on a second Ctrl-C within 800ms of one that
                                      # found the editor empty
CLEAR_KILL_REPEAT = 48                # Ctrl-U presses in the /compact residue clear — drains a multi-line
                                      # input buffer line-by-line (Ctrl-U = delete-to-line-start,
                                      # not whole-buffer); no-op once empty. See drain_bytes().
POST_CLEAR_SETTLE_S = 0.3            # min gap after a retry clear before re-pasting, so the
                                      # buffer-empty has rendered and isn't raced by the paste
COMPACT_WAIT_DEFAULT_S = 5 * 60       # max wait for native /compact completion
COMPACT_POLL_S = 0.2                  # transcript poll cadence while compacting
COMPACT_SUBMIT_DELAY_S = 1.0          # gap between pasting "/compact" and the Enter that runs it —
                                      # pasting a slash command opens Ink's autocomplete menu, and a
                                      # \r in the same write is swallowed by the menu instead of
                                      # submitting (LIVE-verified on Claude 2.1.207)
COMPACT_SUBMIT_RETRY_S = 3.0          # re-press Enter at this cadence until the transcript records
                                      # the /compact command as submitted
POST_COMPACT_HOLD_GRACE_S = 10.0      # a compact that settles while a real turn is already running
                                      # holds BUSY for that turn instead of releasing to IDLE. If the
                                      # inference was wrong the worker would be pinned BUSY forever,
                                      # so the hold is bounded: this much combined hook AND PTY
                                      # silence releases it. A live turn renders its spinner
                                      # continuously, so it never approaches the grace.
BOUNDARY_SEND_ACK_S = 4.0             # hook relay RPC timeout headroom; singleton steer uses its
                                      # own short boundary ACK budget below
BOUNDARY_SEND_POLL_S = 0.02           # native singleton steer enqueue-ack poll cadence
HOOK_RELAY_RETRY_S = 30.0             # a parent hook relay retries a busy host (full listen backlog) this long
HOOK_RELAY_RETRY_POLL_S = 0.05        # pause between those connect attempts
# Durable steering (`ccx steer`). A steer is journaled before it is written to
# the PTY, so absence of quick evidence is never a reason to write it again.
STEER_BOUNDARY_ACK_S = 1.0            # max PostToolUse/SubagentStop hold waiting for the native
                                      # enqueue of a durable steer. Short on purpose: the delivery
                                      # is already journaled, so a slow enqueue is reconciled off
                                      # the hook path instead of blocking Claude's tool loop.
STEER_EVIDENCE_GRACE_S = 300.0        # a delivery pasted with no native evidence at all for this long
                                      # becomes durable `unknown` (never a silent re-paste)
STEER_OBSERVE_DEFAULT_S = 120.0       # caller-side observation window for stronger placement evidence
STEER_OBSERVE_POLL_S = 0.5            # caller-side poll cadence while observing
# Native status is a liveness fallback only; elapsed time never proves terminality.
INTERRUPT_NATIVE_POLL_S = 0.10
INTERRUPT_CONTINUED_ACTIVITY_GRACE_S = 0.60
SEMANTIC_EVIDENCE_GRACE_S = 0.60
PASSIVE_NATIVE_IDLE_GRACE_S = 2.0  # avoid outpacing normal Stop and blocked UPS hooks
LATE_HOOK_BACKSTOP_S = 1.60
LATE_HOOK_PROMPT_ID_S = 60.0  # exact old prompt IDs outlive the heuristic Stop grace

INTERRUPT_CONFIRM_S = 12.0            # after an `interrupt` op, how long the host watches the
                                      # transcript for Claude's "[Request interrupted by user"
                                      # marker before presuming the Escape was a no-op (a
                                      # cancelled turn fires no Stop hook, so this watcher is
                                      # what settles the turn to IDLE)
RESPAWN_IDLE_DEADLINE_S = 180         # _respawn waits this long for state=IDLE post-host-spawn
# There is deliberately no default turn-wait bound. A turn is model work with no
# natural length, so every verb that starts or resumes one (new/send/steer's
# delivery/answer/approve/reject/wait --next-turn) waits unbounded by default and
# ends on the worker's own lifecycle. Callers who want a deadline pass an explicit
# --timeout / timeout=. Operational waits (compact, kill, RPC acks) keep theirs.
# Optional managed `send` payload bound (UTF-8 bytes of the prompt text).
# Unlimited by default: CLI and Python callers must be able to relay complete
# worker output without a CCX-owned length rejection. Operators can opt into a
# defensive PTY-paste guard with CCX_SEND_MAX_BYTES=N; <= 0 stays unlimited.
# The PTY transport drains short writes, but operators may still choose a bound
# for pathological payloads that cannot be delivered within PTY_WRITE_TIMEOUT_S.
SEND_TEXT_MAX_BYTES = int(os.environ.get("CCX_SEND_MAX_BYTES", "0"))
HOST_SOCK_WAIT_NEW_S = 5.0            # detach launcher: max wait for a fresh host's socket
HOST_SOCK_WAIT_RESPAWN_S = 10.0       # detach launcher: max wait for a revived host's socket
SPAWN_LAUNCHER_GRACE_S = 20.0         # headroom over sock-wait for the launcher subprocess itself
                                      # (interpreter start + record read + double-fork)
NB_TERMINAL_CONFIRM_S = 30            # confirm a real user turn lands after terminal-bg queuedPrompt+respawn
PERMISSION_HOOK_TIMEOUT_S = float(os.environ.get("CCX_PERMISSION_HOOK_TIMEOUT_S", 6 * 60 * 60))
PERMISSION_POLL_INTERVAL_S = 0.25

# --- generic CCX event stream ----------------------------------------------
# Every event a host broadcasts (state changes, tool boundaries, subagent
# finishes, …) is stamped with an envelope (v/type/short/seq/epoch/ts) and tee'd
# to a per-session sidecar NDJSON so dashboards/orchestrators can read history
# for dead sessions and replay-then-subscribe for live ones. Event streaming enables this by
# default with no config knobs; persistence is bounded by amortized rotation.
EVENT_SCHEMA_VERSION = 1               # bumped if the envelope/event shapes change
CCX_EVENTS_MAX = 5000                  # rotate the sidecar once it exceeds this many lines
CCX_EVENTS_KEEP = 4000                 # lines retained (most recent) after a rotation
CCX_EVENTS_ROTATE_CHECK_INTERVAL = 256 # events between rotation-size checks (amortization)
EVENT_PREVIEW_LIMIT = 500              # max chars of a bounded input/result/message preview
BACKGROUND_TASK_SURFACE_LIMIT = 10     # max task objects exposed on peek/report/wait surfaces

# --- usage / budget caps ---------------------------------------------------
# Claude plan usage defaults to a promptless Claude Code control request.
# Direct OAuth is an explicit per-CCX_ROOT opt-in, never the default.
USAGE_URL = "https://api.anthropic.com/api/oauth/usage"
USAGE_BETA = "oauth-2025-04-20"
# Per-user and private by default: a fixed /tmp name lets other local users
# read the account's usage or plant data that cap enforcement trusts. Set
# CCX_USAGE_CACHE lets external *readers* consume one CCX-owned cache.
def usage_cache_path() -> pathlib.Path:
    """A current-process root path, not a snapshot captured before select_root."""
    return pathlib.Path(os.path.abspath(os.path.expanduser(
        os.environ.get("CCX_USAGE_CACHE") or str(CCX_ROOT / "usage-cache.json")
    )))


def usage_source_path() -> pathlib.Path:
    return CCX_ROOT / "usage-source.json"
USAGE_MIN_FETCH_INTERVAL_S = 60.0      # shared minimum, even with force/max_age=0
CAP_POLL_INTERVAL_S = 120.0           # capped hosts check/refresh at most every 120s
USAGE_STALE_LIMIT_S = 120.0           # caps fail open when the last good fetch is older
# Direct OAuth measured 2026-10-26: about 5 reads per 10 minutes, then HTTP 429.
# Space its reads evenly instead and let caps accept the correspondingly older sample.
OAUTH_USAGE_MIN_FETCH_INTERVAL_S = 120.0
OAUTH_USAGE_STALE_LIMIT_S = 300.0
BUDGET_EXIT = 7                       # exit code from blocking new/send when a cap hard-stops a run
# Cap flag key → legacy field name in the usage payload. Per-model cap
# lookup also follows the dynamic ``limits`` catalog when the legacy field is
# absent/null.
CAP_METRICS = {"5h": "five_hour", "7d": "seven_day", "opus": "seven_day_opus"}

# --- opt-in gates for unofficial provider internals --------------------------
# Paths that MUTATE undocumented Anthropic/Claude Code state stay off unless the
# operator opts in explicitly. Read-only usage and native-bg discovery are not
# gated. Read at call time (not import time) so tests and callers can toggle them.
UNOFFICIAL_RESET_ENV = "CCX_ENABLE_UNOFFICIAL_RESET"         # 5-hour reset POST


def opt_in_enabled(env_name: str) -> bool:
    """True only for an explicit truthy opt-in (1/true/yes/on)."""
    return os.environ.get(env_name, "").strip().lower() in ("1", "true", "yes", "on")


# Model family aliases understood by `claude --model`. This is NOT an allowlist
# for --model, which stays a free-form passthrough (new model ids ship on
# Claude Code's cadence, not ccx's). Its only job is to catch the `--agent
# <model>` mixup at `ccx new`: `--agent` takes a *subagent* name, and a worker
# launched with `--agent opus` dies at boot with "--agent 'opus' not found",
# which used to surface only as an opaque state=crashed.
MODEL_ALIASES = ("fable", "opus", "sonnet", "haiku")

# --- CLI exit codes --------------------------------------------------------
# The integer codes every `ccx` and `ccx codex` command exits with. Both engines
# use one meaning per code, and each typed Python error carries the same code as
# `exit_code` (see ccx/errors.py and EXIT_CODE_TO_ERROR):
#   1  EXIT_ERROR           bad input or usage (argparse errors included), I/O
#                           error, general failure
#   2  EXIT_NOT_FOUND       no such worker, or no transcript / reply yet
#   3  EXIT_UNAVAILABLE     CCX cannot operate on the worker: host or daemon not
#                           alive, read-only external session, failed respawn,
#                           a recorded active turn nothing serves any more
#   4  EXIT_WAITING         the worker is blocked on input (answer / approve /
#                           reject / respond)
#   5  EXIT_REJECTED        the host or daemon refused the request in the
#                           worker's current state (busy, no active turn,
#                           something in flight); nothing was changed
#   6  EXIT_TURN_INCOMPLETE did not complete cleanly: a wait or turn timed out,
#                           or the awaited turn failed / was interrupted
#   7  BUDGET_EXIT          a usage cap hard-stopped the run (defined above)
#   8  EXIT_RATE_LIMITED    Anthropic rate-limited the account (429). Distinct from
#                           BUDGET_EXIT, which is CCX's OWN cap: this one is imposed
#                           upstream and clears on its own reset, so a caller that
#                           lumps the two together cannot tell "I set this limit"
#                           from "wait until the quota returns"
#   9  EXIT_API_TRANSIENT   Anthropic could not serve the request right now, but
#                           the identical call could succeed later — a 529
#                           (capacity, and free of quota), a 5xx, or a stream that
#                           died mid-response. Split from EXIT_RATE_LIMITED because
#                           the remedy differs: this clears on its own in minutes,
#                           where a rate limit waits for a reset. Both are split
#                           from EXIT_UNAVAILABLE, which stays the permanent-failure
#                           code (auth, billing, bad request) where retrying is
#                           pointless
EXIT_ERROR = 1
EXIT_NOT_FOUND = 2
EXIT_UNAVAILABLE = 3
EXIT_WAITING = 4
EXIT_REJECTED = 5
EXIT_TURN_INCOMPLETE = 6
EXIT_RATE_LIMITED = 8
EXIT_API_TRANSIENT = 9

# Lifecycle states
class S:
    BOOTING       = "booting"
    IDLE          = "idle"
    BUSY          = "busy"
    WAITING       = "waiting"
    IDLE_KILLED   = "idle-killed"
    RESUMING      = "resuming"
    CRASHED       = "crashed"
    DEAD          = "dead"
    BUDGET_STOPPED = "budget-stopped"  # hard-stopped because a usage cap was reached
