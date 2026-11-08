from __future__ import annotations
import json
import os
import sys
import time
from ccx.codex import read as codex_read
from ccx.ops import event_feed
from ccx import constants
from ccx.constants import (
    BACKGROUND_TASK_SURFACE_LIMIT,
    EXIT_ERROR,
    EXIT_NOT_FOUND,
    EXIT_UNAVAILABLE,
    S,
)
from ccx.store.liveness import recorded_host_loaded
from ccx.errors import (
    CcxError,
    EngineCliNotFound,
    InvalidRequest,
    SessionNotFound,
    SessionUnavailable,
)
from ccx.rpc import rpc as host_rpc
from ccx.ops.semantics import (
    payload_guidance as _payload_guidance,  # noqa: F401
    pending_input_kind,
)
from ccx.claude.context import public_context
from ccx.claude.transcript import (
    background_task_current,
    native_transcript_start,
    background_task_summary,
    extract_background_tasks,
)
from ccx.claude.usage import (
    describe_usage_age,
    fetch_usage_for_display,
    set_usage_source,
    usage_summary,
)
from ccx.ops.forking import public_lineage
from ccx.cli.commands._common import _truncate


def _background_task_facts(rec: dict | None) -> dict:
    """Transcript-derived background-task facts for a session: the list plus
    raw/open and host-aware liveness counts.

    A bounded, read-only scan of the raw transcript — never blocks and never
    changes lifecycle state. An *open* background task means a continuation may
    still fire, NOT that the worker is unfinished. `running_background_task_count`
    remains the transcript-open/back-compatible count; `active_*` / `stale_*`
    decorate that with current host liveness. `background_tasks` is capped to the
    most recent BACKGROUND_TASK_SURFACE_LIMIT entries so old completed tasks do
    not bloat every status response; the counts are over the full transcript."""
    src = (rec or {}).get("transcript")
    tasks: list = []
    if src:
        try:
            from ccx.ops.forking import inherited_task_filter
            tasks = inherited_task_filter(rec, extract_background_tasks(src))
        except Exception:
            tasks = []
    summary = background_task_summary(tasks)
    host_live = _background_task_host_live(rec)
    lifecycle = (rec or {}).get("lifecycle") or {}
    native_start = native_transcript_start(
        lifecycle.get("nativeTranscriptStart"), src
    ) if src else None
    decorated = [
        _decorate_background_task_liveness(t, host_live, native_start)
        for t in tasks
    ]
    shown = decorated[-BACKGROUND_TASK_SURFACE_LIMIT:]
    open_count = summary["running_background_task_count"]
    active_count = sum(1 for t in decorated if t.get("active"))
    stale_count = sum(1 for t in decorated if t.get("liveness") == "stale")
    facts = {
        "background_tasks": shown,
        **summary,
        "open_background_task_count": open_count,
        "active_background_task_count": active_count,
        "stale_background_task_count": stale_count,
        "background_tasks_truncated": len(decorated) > len(shown),
        "background_task_limit": BACKGROUND_TASK_SURFACE_LIMIT,
    }
    # Advisory hint (LLM-readable): an active background task can wake the worker
    # for a fresh turn on event/completion, but that turn may just acknowledge
    # and go idle again — `ccx wait --next-turn` blocks for that worker's next
    # actionable boundary (a settled turn or a new input gate).
    if active_count > 0:
        worker = (rec or {}).get("workerId") or "<worker>"
        active_monitors = [
            t for t in decorated
            if t.get("active") and t.get("kind") == "monitor"
        ]
        if active_monitors:
            facts["background_task_hint"] = (
                f"{active_count} background task(s) active; Monitor tasks can stay "
                f"open indefinitely and wake the worker on events. Use `ccx wait "
                f"{worker} --next-turn` only when you want to block until the worker "
                f"next settles a turn or stops on a new input gate."
            )
        else:
            facts["background_task_hint"] = (
                f"{active_count} background task(s) active; each completion wakes "
                f"the worker for a new turn. Use `ccx wait {worker} --next-turn` to "
                f"block until the worker next settles a turn or stops on a new "
                f"input gate."
            )
    elif stale_count > 0:
        if host_live is True:
            facts["background_task_hint"] = (
                f"{stale_count} transcript-open background task(s) are from an "
                "earlier Claude process, not tracked by the current one; their "
                "outcome is unknown, not proven completed."
            )
        else:
            facts["background_task_hint"] = (
                f"{stale_count} transcript-open background task(s) are stale because "
                "the Claude Code host is not live; resume the session to let Claude "
                "reconcile them."
            )
    return facts


def _background_task_host_live(rec: dict | None) -> bool | None:
    """Best-effort host liveness for task decoration.

    True means a transcript-open task is plausibly live. False means it is stale.
    None means we cannot prove either way, so callers should not present it as
    active.
    """
    if not rec:
        return None
    lc = rec.get("lifecycle") or {}
    state = lc.get("state")
    if state in (S.DEAD, S.IDLE_KILLED, S.CRASHED, S.BUDGET_STOPPED):
        return False
    worker_id = rec.get("workerId")
    if worker_id:
        return host_rpc.host_socket_live(worker_id, rec)
    if lc.get("hostPid") or lc.get("workerPid"):
        return True
    if state in (S.BOOTING, S.IDLE, S.BUSY, S.WAITING, S.RESUMING):
        return None
    return None


def _decorate_background_task_liveness(
    task: dict, host_live: bool | None, native_start: int | None
) -> dict:
    t = dict(task)
    t.pop("last_open_offset", None)
    if t.get("status") != "running":
        t["liveness"] = "settled"
        t["active"] = False
    elif host_live is True:
        current = background_task_current(task, native_start)
        t["liveness"] = "live" if current else "stale"
        t["active"] = current
    elif host_live is False:
        t["liveness"] = "stale"
        t["active"] = False
    else:
        t["liveness"] = "unknown"
        t["active"] = False
    return t


def _status_context_for_record(worker: str, rec: dict) -> dict:
    """One provenance-checked context reader across status and context APIs."""
    from ccx import constants
    from ccx.claude.context import current_context, read_claude_observation
    from ccx.store.root import require_ccx_active

    engine = worker.split(":", 1)[0]
    if engine == "claude":
        store = require_ccx_active(constants.CCX_ROOT)
        owner = store.resolve(worker)
        observed = read_claude_observation(store, owner, rec)
        block = observed.get("context") if observed else None
    else:
        block = current_context(rec, engine="codex", live=rec.get("context"))
    result = public_context(block, engine=engine)
    if engine == "claude" and observed and observed.get("note"):
        result["note"] = observed["note"]
    return result


def _claude_recorded_delivery_busy(worker: str, record: dict) -> bool:
    """An owned new turn outranks an idle record until its own settlement.

    The receipt is durable and read-only. Never infer pending work from an
    unowned/prepared intent or a previous turn's already-past settlement.
    """
    from ccx.claude.delivery import iter_receipts
    from ccx.store.worker_state import claude_recorded_delivery_busy
    return claude_recorded_delivery_busy(record, iter_receipts(worker))


def cmd_list(args):
    from ccx import constants
    from ccx.system.cwd_paths import canonical_cwd
    from ccx.store.root import require_ccx_active
    store = require_ccx_active(constants.CCX_ROOT)
    """Public ccx list reads disk-backed full-ID owners; no provider discovery."""
    from ccx.store.root import GenerationError
    cwd = getattr(args, "cwd", None)
    prefix = getattr(args, "cwd_prefix", None)
    if cwd is not None and prefix is not None:
        print("ccx list: --cwd and --cwd-prefix are mutually exclusive", file=sys.stderr)
        raise SystemExit(1)
    target_cwd = canonical_cwd(cwd) if cwd else None
    prefix = canonical_cwd(prefix) if prefix else None
    hours = getattr(args, "recent_hours", None)
    if hours is not None and (
        not isinstance(hours, (float, int)) or not 0 < hours <= 1_000_000
    ):
        print("ccx list: --recent-hours must be positive", file=sys.stderr)
        raise SystemExit(1)
    limit = getattr(args, "limit", None)
    if limit is not None and (type(limit) is not int or limit < 0):
        print("ccx list: --limit must be nonnegative", file=sys.stderr)
        raise SystemExit(1)
    try:
        rows = store.list_workers(
            engine=getattr(args, "engine", None),
            cwd=target_cwd, state=getattr(args, "state", None),
        )
    except GenerationError as exc:
        print(f"ccx list unavailable: {exc}", file=sys.stderr)
        raise SystemExit(3) from None
    if prefix is not None:
        rows = [
            row for row in rows
            if isinstance(row.get("cwd"), str)
            and (row["cwd"] == prefix or row["cwd"].startswith(prefix.rstrip(os.sep) + os.sep))
        ]
    if hours is not None:
        import time
        floor = int((time.time() - hours * 3600) * 1000)
        rows = [
            row for row in rows
            if max(row.get("last_turn_at") or 0, row.get("created_at") or 0) >= floor
        ]
    # Advisory recorded-only predicates, never a probe of native liveness.
    if args.active:
        rows = [row for row in rows if not row.get("terminal_only")
                and row.get("state") in {"starting", "idle", "busy", "waiting"}]
    if args.sendable:
        rows = [row for row in rows if not row.get("terminal_only")
                and row.get("state") in {"idle", "stopped"}]
    if limit is not None:
        rows = rows[:limit]
    if args.json:
        print(json.dumps(rows, sort_keys=True, indent=2, ensure_ascii=False))
        return
    if not rows:
        print("no ccx workers")
        return
    print("WORKER                                        SHORT     ENGINE STATE         NAME")
    for row in rows:
        title = " ".join(str(row.get("name") or "").split())[:80]
        state = str(row.get("state") or "?")[:13]
        print(
            f"{row['worker']:<45} {row['short']:<9} "
            f"{row['engine']:<6} {state:<13} {title}"
            + (" [short ambiguous]" if row["ambiguous_short"] else "")
        )


def cmd_logs(args):
    """Print one original-owned full-UUID worker's native conversation.

    CLI prefix discovery already completed before this handler. Normalized
    output is shared across engines; legacy cleaned and raw JSONL are Claude
    only. Nothing here reopens untrusted record paths or resumes a model turn.
    """
    from ccx.workers import WorkerClient

    try:
        client = WorkerClient()
        owner = client._uuid_owner(args.target)
        if owner.engine == "codex" and (args.jsonl or args.format == "rendered"):
            sys.stderr.write(
                "[ccx] --jsonl and --format rendered are Claude-only; Codex history "
                "is served only as the normalized conversation (the default)\n"
            )
            raise SystemExit(EXIT_ERROR)
        if args.jsonl:
            sys.stdout.write(client._claude.logs(owner.worker, jsonl=True))
            return
        if args.format == "rendered":
            doc = client._claude.logs(owner.worker, tail=args.tail)
            json.dump(doc, sys.stdout, indent=2)
            sys.stdout.write("\n")
            return
        limit = None if args.full else args.max_tool_result_bytes
        if limit is not None and limit < 0:
            sys.stderr.write("[ccx] --max-tool-result-bytes must be zero or greater\n")
            raise SystemExit(EXIT_ERROR)
        # Read only the newest turns: a Claude transcript can be hundreds of
        # MB, and Codex builds every turn it returns.
        tail = args.tail if args.tail and args.tail > 0 else None
        doc = client.conversation(
            owner.worker, max_tool_result_bytes=limit,
            **({"_recent_codex_turns": tail} if owner.engine == "codex"
               else {"_recent_claude_turns": tail}))
        if args.tail:
            doc["turns"] = doc.get("turns", [])[-args.tail:]
        json.dump(doc, sys.stdout, indent=2, ensure_ascii=False)
        sys.stdout.write("\n")
    except CcxError as error:
        sys.stderr.write(f"[ccx] logs: {error}\n")
        raise SystemExit(error.exit_code) from error


def cmd_history(args):
    """Print the named workers' past conversations as text, or search them.

    Read-only: each worker's normalized document is read from its original
    source (Codex from its local history database when it can be), projected
    by ``ccx.ops.history`` and printed. A worker that can't be read is reported
    on stderr while the others still print.
    """
    from ccx import constants
    from ccx.ops import history as hist
    from ccx.store.identity import IdentityError
    from ccx.store.root import GenerationError, require_ccx_active
    from ccx.workers import WorkerClient

    try:
        opts = hist.options(
            view=args.view, turns=args.turns, grep=args.grep, cmd=args.command_regex,
            errors=args.errors, role=args.role, since=args.since, until=args.until,
            context=args.context, limit=args.limit, max_chars=args.max_chars,
            full=args.full)
    except InvalidRequest as error:
        print(f"ccx history: {error}", file=sys.stderr)
        raise SystemExit(error.exit_code) from None
    store = require_ccx_active(constants.CCX_ROOT)
    client = WorkerClient()
    zone = hist.zone_name()
    many = len(args.targets) > 1
    hint = None
    failed = 0
    for target in args.targets:
        try:
            owner = store.resolve(target)
            name = store.record(owner.worker).get("name")
            doc = client.conversation(
                owner.worker,
                max_tool_result_bytes=None if opts.needs_full_results() else 4096,
                _codex_history_db=True)
            store.revalidate()
        except (CcxError, GenerationError, IdentityError, OSError) as exc:
            failed += 1
            print(f"ccx history {target}: cannot read the conversation: {exc}", file=sys.stderr)
            continue
        hint = hist.render_history(hist.project(doc, name=name), opts, sys.stdout,
                                   many=many, zone=zone) or hint
        sys.stdout.flush()
    if hint is not None:
        print("\n" + hist.hint_line(hint))
    if failed:
        raise SystemExit(EXIT_UNAVAILABLE)


class _ReportFailure(Exception):
    """One target that cannot be reported: message plus the CLI exit code."""

    def __init__(self, message: str, exit_code: int = EXIT_NOT_FOUND):
        super().__init__(message)
        self.exit_code = exit_code


def cmd_report(args):
    """Print the latest native final reply, or an exact settled receipt when owned.

    Strict turn-ref and send/wait replies remain separately receipt-authoritative.
    Plain output contains only the selected reply; JSON includes its provenance.
    """
    from ccx.ops.report import report_worker
    from ccx.store.root import GenerationError
    from ccx.store.identity import IdentityError
    if args.max_chars is not None and args.max_chars < 0:
        print("ccx report: --max-chars must be nonnegative",
              file=sys.stderr)
        raise SystemExit(EXIT_ERROR)
    records = []
    failures = []
    for target in args.targets:
        try:
            records.append(report_worker(target, max_chars=args.max_chars))
        except (GenerationError, IdentityError, CcxError) as exc:
            failures.append({"target": target, "error": str(exc)})
    if args.json:
        if len(args.targets) == 1 and len(records) == 1:
            print(json.dumps(records[0], ensure_ascii=False))
        else:
            print(json.dumps({
                "ok": not failures,
                "reports": records,
                "errors": failures,
            }, ensure_ascii=False))
    else:
        for i, report in enumerate(records):
            if len(args.targets) > 1:
                print(f"=== {report['worker']} ===")
            if not report.get("has_final_message"):
                status = report.get("status")
                if status in {"interrupted", "cancelled"}:
                    print("(turn interrupted; no reply)")
                elif status == "failed":
                    print("(turn failed; no reply)")
                elif status == "completed":
                    print("(turn completed; no reply)")
                else:
                    print(report["text"])
            else:
                print(report["text"])
            if report.get("note"):
                where = f" for {report['worker']}" if len(args.targets) > 1 else ""
                print(f"note{where}: {report['note']}", file=sys.stderr)
        for failed in failures:
            print(f"ccx report {failed['target']}: {failed['error']}",
                  file=sys.stderr)
    if failures:
        raise SystemExit(3)


def _format_context_line(worker: str, ctx: dict | None) -> str:
    """Plain Claude context from typed flat fields; state remains lifecycle."""
    reason = (ctx or {}).get("reason")
    if ctx is None or reason == "no-signal":
        return f"{worker}  context: no signal yet (none recorded; updates after the next turn)"
    if reason == "pending":
        return f"{worker}  context: pending (no completed turn with token usage yet)"
    if reason == "compacting":
        return f"{worker}  context: unknown (compacting)"
    if reason == "just_compacted":
        return f"{worker}  context: unknown (just compacted)"
    tokens = ctx.get("tokens_used")
    return (f"{worker}  context: {tokens:,} tokens"
            if tokens is not None else f"{worker}  context: unknown")


def cmd_context(args):
    """Print an exact full-UUID owner's existing typed recorded context."""
    from ccx import constants
    from ccx.client import CcxClient
    from ccx.codex.client import CodexClient
    from ccx.store.root import GenerationError, require_ccx_active
    from ccx.store.identity import IdentityError, UnknownWorker

    try:
        if getattr(args, "live", False):
            raise SessionUnavailable(
                "context --live unavailable: external Claude discovery disabled"
            )
        try:
            store = require_ccx_active(constants.CCX_ROOT)
            owner = store.resolve(args.target)
            store.revalidate()
        except UnknownWorker as exc:
            raise SessionNotFound(f"context: worker not found: {args.target}") from exc
        except IdentityError as exc:
            raise InvalidRequest(f"context: invalid worker handle: {exc}") from exc
        except (GenerationError, OSError) as exc:
            raise SessionUnavailable(f"context: worker record unavailable; check CCX_ROOT: {exc}") from exc
        typed = (CcxClient().context(owner.worker) if owner.engine == "claude"
                 else CodexClient().context(owner.worker))
        output = typed.as_dict()
    except CcxError as exc:
        sys.stderr.write(f"[ccx] context: {exc}\n")
        raise SystemExit(exc.exit_code) from exc

    if args.json:
        print(json.dumps(output, indent=2))
    elif owner.engine == "claude":
        print(_format_context_line(output["worker"], output))
    else:
        worker = output["worker"]
        tokens = output["tokens_used"]
        window = output["window_size"]
        percent = output["percent_used"]
        if tokens is None:
            print(f"{worker}  context unavailable")
        elif window:
            usage = f"{worker}  {tokens:,} / {window:,} tokens"
            if percent is not None:
                usage += f" ({percent:.1f}%)"
            print(usage)
        else:
            print(f"{worker}  {tokens:,} tokens")


def _combined_usage(args):
    """Run each native account reader once; only probe login on read failure."""
    from contextlib import redirect_stdout, redirect_stderr
    from copy import copy
    from io import StringIO
    from ccx.cli import codex as codex_commands
    from ccx.system.engine_binary import resolve_engine_binary, account_signed_out

    responses, answered = {}, 0
    for engine, handler in (("claude", cmd_usage), ("codex", codex_commands.cmd_usage)):
        try:
            binary = resolve_engine_binary(engine)
        except EngineCliNotFound as error:
            responses[engine] = {"skipped": "not_installed", "message": str(error)}
            continue
        stdout, stderr = StringIO(), StringIO()
        error = None
        try:
            selected = copy(args)
            selected.engine = engine
            with redirect_stdout(stdout), redirect_stderr(stderr):
                handler(selected)
        except (Exception, SystemExit) as exc:
            error = exc
        output, warning = stdout.getvalue(), stderr.getvalue()
        if error is None:
            answered += 1
            responses[engine] = (json.loads(output) if args.json else output)
            if warning:
                sys.stderr.write(warning)
        elif account_signed_out(engine, binary):
            responses[engine] = {"skipped": "signed_out", "message": f"{engine} is not signed in"}
        else:
            message = warning.strip() or str(error)
            responses[engine] = {"error": {"type": type(error).__name__, "message": message}}
            if not args.json:
                sys.stderr.write(f"[ccx] usage {engine}: {message}\n")
    if args.json:
        print(json.dumps(responses, indent=2))
    else:
        for engine in ("claude", "codex"):
            row = responses[engine]
            if isinstance(row, dict) and "skipped" in row:
                print(f"{engine.capitalize()}: skipped ({row['skipped']}): {row['message']}")
            elif isinstance(row, dict) and "error" in row:
                print(f"{engine.capitalize()}: error (see stderr)")
            else:
                print(f"{engine.capitalize()}:")
                print(row, end="" if row.endswith("\n") else "\n")
    if not answered:
        raise SystemExit(EXIT_ERROR)


def cmd_usage(args):
    """Read Claude plan usage or persist its machine-wide source."""
    selected = getattr(args, "set_source", None)
    if getattr(args, "engine", None) is None and selected is None:
        return _combined_usage(args)
    if selected is not None:
        if getattr(args, "cached", False):
            raise InvalidRequest("--set-source cannot combine with --cached")
        try:
            print(f"Claude usage source set to {set_usage_source(selected)}")
        except CcxError as error:
            sys.stderr.write(f"[ccx] usage: {error}\n")
            raise SystemExit(error.exit_code) from None
        return

    from ccx.system.engine_binary import resolve_engine_binary
    try:
        resolve_engine_binary("claude")
    except EngineCliNotFound as error:
        if args.json:
            print(json.dumps({"ok": False, "code": "runtime_unavailable",
                              "message": str(error)}, sort_keys=True))
        else:
            sys.stderr.write(f"[ccx] usage: {error}\n")
        raise SystemExit(error.exit_code) from error
    data, origin = fetch_usage_for_display(force=not args.cached)
    error = origin.get("live_error")
    if data is None:
        suffix = ""
        if origin.get("held"):
            suffix = f"; held, retry ~{_usage_clock(origin.get('held_until'))}"
        sys.stderr.write(f"[ccx] usage: no signal: {error or 'Claude usage unavailable'}{suffix}; cache {origin['cache']}\n")
        sys.exit(EXIT_ERROR)
    if origin.get("unparsed"):
        heading = "usage (unparsed provider response):"
    elif origin.get("held"):
        heading = (f"usage (last known {_usage_clock(origin.get('fetched_at'))} "
                   f"(held, retry ~{_usage_clock(origin.get('held_until'))})):" )
    elif origin.get("stale"):
        heading = (f"usage (last known {_usage_clock(origin.get('fetched_at'))} "
                   f"(stale, retry ~{_usage_clock(origin.get('next_attempt_at'))})):" )
    elif origin["source"] == "live":
        heading = "usage (live):"
    else:
        heading = f"usage (cached, {describe_usage_age(origin.get('age_s'))}):"
    if (getattr(args, "json", False) or getattr(args, "summary", False)) and (
        origin.get("stale") or origin.get("unparsed")
    ):
        sys.stderr.write(f"[ccx] usage: {heading} {error or 'last-known data'}\n")
    if getattr(args, "summary", False):
        print(json.dumps(usage_summary(data), indent=2))
        return
    if getattr(args, "json", False):
        print(json.dumps(data, indent=2))
        return
    print(heading)
    if origin.get("unparsed"):
        print(json.dumps((data.get("_ccx") or {}).get("raw"), indent=2))
        return

    summary = usage_summary(data)
    from ccx.cli.commands._common import local_reset_time
    def row(label, field):
        block = summary.get(field)
        if not isinstance(block, dict) or "utilization" not in block:
            return
        util = block["utilization"]
        resets = local_reset_time(block.get("resets_at"))
        print(f"  {label:<10} {util:>5.1f}%   resets {resets}")

    row("5h", "five_hour")
    row("7d", "seven_day")
    model_quotas = summary.get("model_quotas") or []
    if model_quotas:
        for quota in model_quotas:
            resets = local_reset_time(quota.get("resets_at"))
            label = f"7d {quota['model'].lower()}"
            print(f"  {label:<10} {quota['utilization']:>5.1f}%   resets {resets}")
    else:
        row("7d opus", "seven_day_opus")
        row("7d sonnet", "seven_day_sonnet")
    xu = data.get("extra_usage")
    if isinstance(xu, dict) and xu.get("is_enabled"):
        util = xu.get("utilization")
        if isinstance(util, (float, int)) and not isinstance(util, bool):
            print(f"  {'extra':<10} {util:>5.1f}%   "
                  f"{xu.get('used_credits')}/{xu.get('monthly_limit')} {xu.get('currency') or ''}")


def _usage_clock(iso_time):
    """Local wall-clock display of a reported (or estimated) UTC instant."""
    from datetime import datetime
    if not isinstance(iso_time, str):
        return "unknown"
    try:
        return datetime.fromisoformat(iso_time.replace("Z", "+00:00")).astimezone().strftime("%H:%M")
    except ValueError:
        return "unknown"


def _recorded_peek_pending(owner, rec: dict) -> dict:
    """One recorded-only gate projection for status and conversation peek."""
    if owner.engine == "claude":
        waiting = rec.get("waitingFor")
        if not isinstance(waiting, dict):
            return {"pending_input_observed": False}
        return {
            "pending_input_observed": True,
            "waiting_for": waiting,
            "pending_input": {"request_id": waiting.get("id"),
                              "kind": pending_input_kind(waiting)},
        }
    lifecycle = rec.get("lifecycle")
    lifecycle = lifecycle if isinstance(lifecycle, dict) else {}
    approvals = ([value for value in (rec.get("pendingApprovals") or [])
                  if isinstance(value, dict)]
                 if lifecycle.get("state") == "waiting" else [])
    selected = codex_read.pending_input(approvals)
    return {
        "pending_input_observed": bool(approvals),
        **({"pending_input": selected, "pending_approvals": [
            {"gateId": value.get("gateId"), "kind": value.get("kind"),
             "command": value.get("command"), "runtimeId": value.get("runtimeId")}
            for value in approvals]}
           if selected is not None else {}),
    }


def _background_summary_line(summary: dict | None) -> str | None:
    """Brief recorded owner snapshot; never read native background sources."""
    if not isinstance(summary, dict):
        return None
    running = summary.get("running_count", summary.get("runningCount"))
    pending = summary.get("pending_count", 0)
    if running == 0 and pending == 0:
        return None
    if summary.get("available") is False or running is None:
        return "background: unavailable"
    kinds = summary.get("by_kind", summary.get("kinds")) or {}
    return (f"background: running={running} pending={pending} "
            f"command={kinds.get('command', kinds.get('bash', 0))} "
            f"monitor={kinds.get('monitor', 0)} "
            f"subagent={kinds.get('subagent', 0)}")


def _peek_window_enough(doc, args) -> bool:
    """A recent window holds what this peek shows, or there is no older history.

    Tools are counted on their own: the activity view also shows errors, but
    the tool list and --tools-only show only tools, and both use --tail.
    """
    if not isinstance(doc, dict) or "peekHistoryWindow" not in doc:
        return True
    kinds = [item.get("kind") for turn in doc.get("turns") or []
             for item in [*turn.get("input", []), *turn.get("output", [])]
             if isinstance(item, dict)]
    texts = "user-text" in kinds and "assistant-text" in kinds
    tools = sum(kind in {"tool-call", "tool-result"} for kind in kinds) >= args.tail
    if args.text_only:
        return texts
    if args.tools_only:
        return tools
    return texts and tools


def _peek_conversation(args, store):
    """Project one original schema-5 snapshot for the CLI, without native writes."""
    from ccx.store.root import GenerationError
    from ccx.store.identity import IdentityError
    from ccx.workers import WorkerClient

    try:
        owner = store.resolve(args.target)
        options = {}
        # Peek shows only recent items, so it reads a recent tail. An explicit
        # --since-turn keeps the complete history path for original turn numbers.
        if owner.engine == "codex" and args.since_turn is None:
            options["_recent_codex_turns"] = min(64, max(12, args.tail * 2 + 4))
        elif args.since_turn is None:
            # Claude prompts can each span many MB of tool work, so start from
            # the last two and widen only until peek has everything it shows.
            options["_recent_claude_turns"] = 2
        while True:
            doc = WorkerClient().conversation(
                owner.worker, max_tool_result_bytes=None if args.full else 4096,
                **options,
            )
            if "_recent_claude_turns" not in options or _peek_window_enough(doc, args):
                break
            options["_recent_claude_turns"] *= 4
        if (not isinstance(doc, dict) or doc.get("schemaVersion") != 5
                or not isinstance(doc.get("worker"), dict)
                or doc["worker"].get("handle") != owner.worker
                or not isinstance(doc.get("turns"), list)
                or not isinstance(doc.get("completeness"), dict)
                or not isinstance(doc.get("evidence"), dict)):
            raise SessionUnavailable("The conversation is incomplete; retry after the worker finishes writing")
        rec = store.record(owner.worker)
        pending = _recorded_peek_pending(owner, rec)
        bg_summary = rec.get("backgroundTaskSummary")
        store.revalidate()
    except (CcxError, GenerationError, IdentityError, OSError) as exc:
        print(f"ccx peek: cannot read the conversation; check the worker: {exc}", file=sys.stderr)
        raise SystemExit(getattr(exc, "exit_code", EXIT_UNAVAILABLE)) from None

    turns = [t for t in doc["turns"]
             if args.since_turn is None or t["sequence"] >= args.since_turn]
    # The input/output buckets are NOT chronological. The original ordinal is.
    ordered = [
        {**item, "turn_sequence": turn["sequence"], "turn_id": turn.get("turnId")}
        for turn in turns
        for item in sorted(turn["input"] + turn["output"], key=lambda i: i["ordinal"])
    ]
    users = [item for item in ordered if item["kind"] == "user-text"]
    assistants = [item for item in ordered if item["kind"] == "assistant-text"]
    tools = [item for item in ordered if item["kind"] in {"tool-call", "tool-result"}]
    recent_tools = tools[-args.tail:] if args.tail else []
    # Engine/API errors occupy the same bounded chronological activity window
    # as tools, not an independent unbounded failure-history bucket.
    activities = [item for item in ordered
                  if item["kind"] in {"tool-call", "tool-result", "error"}]
    recent_activity = activities[-args.tail:] if args.tail else []
    if args.tools_only:
        displayed = recent_tools
    elif args.text_only:
        displayed = [*users[-1:], *assistants[-1:]]
    else:
        # Show latest user and assistant (including mid-turn steers) IN source
        # order relative to the recent tools, not grouped by bucket.
        selected = {id(item) for item in [*recent_activity,
                                          *users[-1:], *assistants[-1:]]}
        displayed = [item for item in ordered if id(item) in selected]
    projection = {
        "worker": doc["worker"], "state": doc.get("state"),
        **pending,
        "generated_at": doc.get("generatedAt"),
        "completeness": doc["completeness"], "evidence": doc["evidence"],
        "native_artifacts": doc.get("nativeArtifacts"),
        "since_turn": args.since_turn, "turn_count": len(turns),
        **({"history_window": doc["peekHistoryWindow"]}
           if "peekHistoryWindow" in doc else {}),
        "last_user": users[-1] if users and not args.tools_only else None,
        "last_assistant": assistants[-1] if assistants and not args.tools_only else None,
        "tool_count": len(tools), "tools": recent_tools if not args.text_only else [],
        "recent_items": displayed,
        **({"background_task_summary": bg_summary} if bg_summary is not None else {}),
    }
    if args.json:
        print(json.dumps(projection, indent=2, ensure_ascii=False))
        return

    worker = doc["worker"]
    lines = [
        f"{worker['handle']}: {doc.get('state') or '?'}  "
        f"native={worker.get('nativeSessionId') or '?'}",
        f"source: schema-5 completeness={doc['completeness'].get('status')} "
        f"evidence={doc['evidence'].get('status')}",
    ]
    waiting = pending.get("pending_input")
    if waiting:
        if owner.engine == "codex":
            approvals = pending.get("pending_approvals") or []
            first = approvals[0] if approvals else {}
            line = f"awaiting: approval gate_id={waiting.get('request_id') or 'unavailable'}"
            if isinstance(first.get("command"), str) and first["command"]:
                line += " command=" + _truncate(" ".join(first["command"].split()), 120)
            lines.append(line)
        else:
            if waiting.get("request_id"):
                lines.append(f"awaiting: {waiting.get('kind') or 'input'} "
                             f"request_id={waiting['request_id']}")
            else:
                lines.append(f"awaiting: {waiting.get('kind') or 'input'} in the worker terminal "
                             f"(ccx attach {worker['handle']})")
    background_line = _background_summary_line(bg_summary)
    if background_line:
        lines.append(background_line)
    for field in ("completeness", "evidence"):
        for warning in doc[field].get("warnings") or []:
            lines.append(f"{field} warning: {warning}")
    if args.brief:
        if assistants and not args.tools_only:
            lines.append("assistant: " + _truncate(assistants[-1].get("text"), 240))
        if not args.text_only:
            names = [i.get("name") or i.get("nativeType") or "tool"
                     for i in (recent_tools[-2:] if args.tail else [])]
            lines.append(f"tools: {len(tools)} recorded; recent " + ", ".join(names))
    else:
        for item in displayed:
            where = (f"recent turn ordinal {item['ordinal']}"
                     if item["turn_sequence"] is None
                     else f"turn {item['turn_sequence']} ordinal {item['ordinal']}")
            kind = item["kind"]
            if kind in {"user-text", "assistant-text"}:
                text = str(item.get("text") or "")
                lines.append(f"{where} {kind}: " + (text if args.full else _truncate(text, 2000)))
            elif kind in {"tool-call", "tool-result"}:
                label = item.get("name") or item.get("callId") or item.get("id") or "tool"
                lines.append(f"{where} {kind} {label}: {item.get('status') or 'recorded'}")
                if kind == "tool-call" and item.get("arguments"):
                    arguments = json.dumps(item["arguments"], ensure_ascii=False, default=str)
                    lines.append("  args: " + (arguments if args.full else _truncate(arguments, 400)))
                result = item.get("result")
                if isinstance(result, dict):
                    content = str(result.get("content") or "")
                    lines.append("  result" + (" ERROR" if result.get("isError") else "")
                                 + (" [native truncated]" if result.get("truncated") else "")
                                 + ": " + (content if args.full else _truncate(content, 1000)))
            elif kind == "error":
                summary = item.get("summary")
                if isinstance(summary, dict):
                    summary = " ".join(str(summary.get(k) or "") for k in ("verb", "target")).strip()
                message = str(item.get("text") or item.get("message") or summary or item.get("result") or "error")
                lines.append(f"{where} error: " +
                             (message if args.full else _truncate(message, 1000)))
    output = "\n".join(lines) + "\n"
    budget = args.max_chars if args.max_chars is not None else (None if args.full else 8000)
    if budget is not None and len(output) > budget:
        notice = "\n[peek display truncated; use --json or --full]\n"
        output = (output[:max(0, budget-len(notice))] + notice)[:budget]
    sys.stdout.write(output)


def cmd_peek(args):
    from ccx import constants
    from ccx.store.root import require_ccx_active
    store = require_ccx_active(constants.CCX_ROOT)
    """Forensic-only exact-UUID recorded status; no host/native observation."""
    import time
    from ccx.store.root import GenerationError
    from ccx.store.identity import IdentityError

    if (args.tail < 0 or (args.since_turn is not None and args.since_turn < 1)
            or (args.max_chars is not None and args.max_chars < 0)
            or (args.tools_only and args.text_only)):
        print("ccx peek: --tail/--max-chars must be nonnegative, --since-turn >= 1; "
              "choose at most one --tools-only/--text-only",
              file=sys.stderr)
        raise SystemExit(1)
    if args.status and (args.full
            or args.tools_only or args.text_only
            or args.brief or args.since_turn is not None
            or args.max_chars is not None or args.tail != 4):
        print(
            "ccx peek: --status is recorded-only; rich conversation options require "
            "a single target without --status",
            file=sys.stderr,
        )
        raise SystemExit(1)
    if not args.status:
        if args.extra_shorts:
            print("ccx peek: only --status supports multiple targets", file=sys.stderr)
            raise SystemExit(1)
        return _peek_conversation(args, store)
    targets = [args.target, *(getattr(args, "extra_shorts", None) or [])]
    if len(targets) > 128:
        print("ccx peek: too many target UUIDs for one bounded snapshot",
              file=sys.stderr)
        raise SystemExit(1)
    from ccx.store.worker_state import normalize_native_state
    from ccx.store.discovery import _query, _match

    def read_one(target):
        engine, prefix, incomplete = _query(target)
        resolved = _match(target, engine, prefix, store) if incomplete else target
        owner = store.resolve(resolved)
        rec = store.record(owner.worker)
        lifecycle = rec.get("lifecycle")
        lifecycle = lifecycle if isinstance(lifecycle, dict) else {}
        native = rec.get("native")
        native = native if isinstance(native, dict) else {}
        stats = rec.get("stats")
        stats = stats if isinstance(stats, dict) else {}
        timestamp = rec.get("lastTurnAt")
        age = (max(0, time.time() - timestamp / 1000)
               if type(timestamp) is int and timestamp > 0 else None)
        state = normalize_native_state(lifecycle.get("state"))
        if (owner.engine == "claude" and state == "idle"
                and _claude_recorded_delivery_busy(owner.worker, rec)):
            state = "busy"
        return {
            "worker": owner.worker,
            "worker_id": owner.worker_id,
            "engine": owner.engine,
            "short": owner.short,
            "state": state,
            "native_state": native.get("nativeState"),
            "state_source": "recorded",
            "host_status": "unobserved",
            "loaded": recorded_host_loaded(owner.engine, lifecycle),
            **_recorded_peek_pending(owner, rec),
            "turn": stats.get("turns"),
            "last_turn_age_s": age,
            "terminal_only": bool(rec.get("terminalOnly")),
            "record_revision": rec.get("recordRevision"),
            "context": _status_context_for_record(owner.worker, rec),
            "forked_from": public_lineage(rec),
            **({"background_task_summary": rec["backgroundTaskSummary"]}
               if "backgroundTaskSummary" in rec else {}),
        }

    rows, errors = [], {}
    for target in targets:
        try:
            rows.append(read_one(target))
        except (IdentityError, GenerationError, CcxError, OSError) as exc:
            if len(targets) == 1:
                if isinstance(exc, IdentityError):
                    print(f"ccx peek: invalid or unowned full UUID: {exc}", file=sys.stderr)
                    raise SystemExit(2) from None
                print(f"ccx peek: unavailable generation or record: {exc}",
                      file=sys.stderr)
                raise SystemExit(getattr(exc, "exit_code", 3)) from None
            errors[target] = {"error": type(exc).__name__, "message": str(exc),
                              "exit_code": getattr(exc, "exit_code", 3)}
    try:
        store.revalidate()
    except GenerationError as exc:
        print(f"ccx peek: unavailable generation or record: {exc}", file=sys.stderr)
        raise SystemExit(3) from None
    if args.json:
        print(json.dumps({"statuses": rows, **({"errors": errors} if errors else {})},
                         indent=2, sort_keys=True))
    else:
        for r in rows:
            pending = r.get("pending_input") or {}
            label = (f" awaiting={pending.get('kind')} "
                     f"request_id={pending.get('request_id')}" if pending else "")
            if r["engine"] == "codex" and r.get("pending_approvals"):
                gates = [item.get("gateId") for item in r["pending_approvals"]
                         if isinstance(item.get("gateId"), str) and item["gateId"]]
                label = " approval_gate_ids=" + ",".join(gates) if gates else " approval_gate_id=unavailable"
            tokens = r["context"]["tokens_used"]
            ctx = (f"{tokens / 1_000_000:.1f}M" if tokens is not None and tokens >= 1_000_000
                   else f"{tokens / 1_000:.0f}k" if tokens is not None and tokens >= 1_000
                   else str(tokens) if tokens is not None else "—")
            bg_line = _background_summary_line(r.get("background_task_summary"))
            print(f"{r['worker']}: {r['state'] or '?'} ctx={ctx}"
                  + (" terminal-only" if r["terminal_only"] else "") + label
                  + (f" {bg_line}" if bg_line else ""))
    if errors:
        for target, error in errors.items():
            print(f"ccx peek: {target}: {error['message']}", file=sys.stderr)
        raise SystemExit(1)


EVENTS_TAIL_DEFAULT = 20


def _event_summary(event: dict) -> str:
    """One short human line for a feed event (the JSON form has everything)."""
    kind = event.get("kind")
    if kind == "state":
        return f"{event.get('from')} -> {event.get('to')}"
    if kind in {"tool.started", "tool.finished"}:
        detail = (event.get("input_preview") if kind == "tool.started"
                  else ("ok" if event.get("ok") else "failed"))
        return f"{event.get('tool')} {str(detail or '')[:80]}".rstrip()
    if kind == "turn.ended":
        return str(event.get("outcome") or "")
    if kind in {"input.requested", "input.resolved"}:
        request = event.get("request") or {}
        rid = event.get("request_id") or request.get("request_id") or request.get("id")
        return f"request {rid}" if rid is not None else ""
    if kind == "error":
        return str(event.get("message") or event.get("reason") or "")[:120]
    return ""


def _print_event(event: dict, *, as_json: bool) -> None:
    if as_json:
        sys.stdout.write(json.dumps(event) + "\n")
    elif event.get("kind") != event_feed.KIND_HEARTBEAT:
        stamp = time.strftime("%H:%M:%S", time.localtime(event.get("ts") or 0))
        label = event.get("kind") or event.get("type")
        if event.get("kind") and event.get("kind") != event.get("type"):
            label = f"{event['kind']} ({event.get('type')})"
        sys.stdout.write(f"{stamp} {label} {_event_summary(event)}".rstrip() + "\n")
    if event.get("gap"):
        sys.stderr.write(f"[ccx] events: cursor could not be placed "
                         f"({event['gap'].get('reason')}); replayed what is retained\n")
    sys.stdout.flush()


def cmd_events(args):
    """Read or follow the original full-UUID worker's lifecycle events.

    Snapshot JSON is one complete page with a resumable cursor; follow JSON
    is NDJSON (events and cursor heartbeats). Neither path starts a worker.
    """
    from ccx.errors import CursorInvalidated
    from ccx.workers import WorkerClient

    if args.after is not None and args.tail is not None:
        sys.stderr.write("[ccx] --after and --tail cannot be combined\n")
        sys.exit(EXIT_ERROR)
    if args.timeout is not None and not args.follow:
        sys.stderr.write("[ccx] --timeout requires --follow\n")
        sys.exit(EXIT_ERROR)
    tail = args.tail
    if args.after is None and tail is None:
        tail = EVENTS_TAIL_DEFAULT
    kinds = args.kind or None

    try:
        client = WorkerClient()
        if args.follow:
            for item in client.follow(
                args.target, after=args.after, tail=tail, kinds=kinds,
                timeout=args.timeout, heartbeat=args.heartbeat,
            ):
                _print_event(item, as_json=args.json)
            return
        page = client.events(
            args.target, after=args.after, tail=tail,
            limit=event_feed.LIMIT_MAX, kinds=kinds,
        )
        events = list(page["events"])
        gap = page["gap"]
        # A short tail page continues from the journal head; paging on would
        # append the whole history after the newest events.
        while page["has_more"] and tail is None:
            page = client.events(
                args.target, after=page["next_cursor"],
                limit=event_feed.LIMIT_MAX, kinds=kinds,
            )
            events.extend(page["events"])
            gap = gap or page["gap"]
        if args.json:
            print(json.dumps({**page, "events": events,
                              "has_more": page["has_more"] and tail is not None,
                              "gap": gap}))
            return
        for event in events:
            _print_event(event, as_json=False)
        if gap:
            _print_event({"kind": event_feed.KIND_HEARTBEAT, "gap": gap},
                         as_json=False)
    except KeyboardInterrupt:
        pass
    except CursorInvalidated as error:
        sys.stderr.write(f"[ccx] invalid --after cursor: {error}; "
                         "obtain a fresh cursor with ccx events --json\n")
        sys.exit(EXIT_ERROR)
    except CcxError as error:
        sys.stderr.write(f"[ccx] {error}\n")
        sys.exit(error.exit_code)
