"""Settled UUID fork planning, lineage, and original-host Claude copy proof."""
from __future__ import annotations

import contextlib
import json
import os
import pathlib
import subprocess
import stat
import time

from ccx.errors import InvalidRequest, OperationConflict, OperationTimeout, SessionUnavailable

MATERIALIZE_TIMEOUT_S = 180.0


def now_ms() -> int:
    return int(time.time() * 1000)

def _remaining(deadline: float | None) -> float | None:
    if deadline is None:
        return None
    left = deadline - time.monotonic()
    if left <= 0:
        raise OperationTimeout("fork deadline expired")
    return left


# -- Claude -----------------------------------------------------------------

def _rows(path: str | os.PathLike, deadline: float | None = None, *,
          messages: bool = True, before_offset: int | None = None,
          stats: dict | None = None):
    """Main-chain rows of a Claude transcript (only user/assistant messages
    unless ``messages`` is false), checking ``deadline`` as it scans."""
    from ccx.claude.transcript import _iter_transcript
    for index, row in enumerate(_iter_transcript(path, stats=stats, before_offset=before_offset)):
        if deadline is not None and index % 256 == 0:
            _remaining(deadline)
        if not messages or (row.get("type") in ("user", "assistant")
                             and isinstance(row.get("uuid"), str)):
            yield row








def _contains_fork_point(path: pathlib.Path, session_id: str, resume_at: str) -> bool:
    return any(row.get("uuid") == resume_at and row.get("sessionId") == session_id
               for row in _rows(path))


def _claude_copy_proof(record: dict | None) -> dict | None:
    """Only a child/native/anchor-matched immutable-copy fact counts as copied.

    Pure projection: the later F1b host publishes lifecycle.nativeForkProof
    through existing fresh-record CAS. A short, timestamp, or copied file alone
    is not authority to start or recopy a managed native session.
    """
    if not isinstance(record, dict):
        return None
    fork = record.get("forkedFrom")
    anchor = record.get("forkAnchor")
    proof = (record.get("lifecycle") or {}).get("nativeForkProof")
    native = record.get("native")
    if not all(isinstance(part, dict) for part in (fork, anchor, proof, native)):
        return None
    from ccx.store.identity import InvalidTarget, canonical_worker_id
    try:
        worker_id = canonical_worker_id(record.get("workerId"))
        source_id = canonical_worker_id(anchor.get("sourceWorkerId"))
        sid = canonical_worker_id(native.get("sessionId"))
    except InvalidTarget:
        return None
    if (record.get("engine") != "claude" or anchor.get("engine") != "claude"
            or source_id == worker_id or sid == worker_id
            or not isinstance(anchor.get("resumeAt"), str) or not anchor["resumeAt"]
            or not isinstance(anchor.get("sourceNativeSessionId"), str)
            or not anchor["sourceNativeSessionId"]
            or any(fork.get(key) != anchor.get(key) for key in (
                "engine", "sourceWorkerId", "sourceNativeSessionId", "resumeAt",
                "forkedAtMs",
            ))
            or proof.get("workerId") != worker_id
            or proof.get("nativeSessionId") != sid
            or proof.get("forkAnchor") != anchor
            or type(proof.get("copiedAtMs")) is not int
            or proof["copiedAtMs"] <= 0
            or type(proof.get("inheritedBytes")) is not int
            or proof["inheritedBytes"] <= 0):
        return None
    tasks = proof.get("inheritedOpenTaskIds")
    if (not isinstance(tasks, list) or len(tasks) > 1024
            or any(not isinstance(x, str) or not x or len(x) > 256 for x in tasks)):
        return None
    return proof


def claude_fork_pending(record: dict) -> bool:
    """An unproven owned Claude fork is NEVER a recopy authorization."""
    return isinstance(record, dict) and record.get("engine") == "claude" and bool(
        record.get("forkAnchor")
    ) and _claude_copy_proof(record) is None


def ensure_claude_fork_materialized(worker_id: str, *, deadline: float | None = None) -> dict:
    """Refuse unproven UUID fork history; only F1b's original host may copy.

    rpc._respawn and historical creation still call this name. They must not
    enter the retired short-keyed rename/quarantine/kill/recopy operation.
    """
    from ccx.store.records import read_record
    record = read_record(worker_id)
    if claude_fork_pending(record):
        raise SessionUnavailable(
            f"Conversation copy for fork {worker_id} is not verified; "
            "keep its worker ID and inspect status before retrying",
            short=worker_id,
        )
    return record


def _run_materializer(record: dict, fork: dict, *, deadline: float | None = None) -> None:
    import signal
    from ccx.constants import CLAUDE_BIN
    from ccx.claude.host.session import worker_env
    worker_id = record["workerId"]
    session_id = record["native"]["sessionId"]
    budget = MATERIALIZE_TIMEOUT_S
    if deadline is not None:
        budget = min(budget, deadline - time.time())
        if budget <= 0:
            raise OperationTimeout(f"deadline expired before Claude fork {worker_id} was copied",
                                   short=worker_id)
    argv = [
        CLAUDE_BIN, "-p",
        "--session-id", session_id, "--fork-session",
        "--resume", fork.get("transcript") or fork["sourceNativeSessionId"],
        "--resume-session-at", fork["resumeAt"],
        "--output-format", "json",
        # A copy, not a session start: no MCP servers and no user hooks.
        "--strict-mcp-config", "--settings", json.dumps({"disableAllHooks": True}),
        # A local command: appends without a model call and sets the title.
        "/rename " + record.get("name", f"ccx-{worker_id[:8]}"),
    ]
    try:
        child = subprocess.Popen(
            argv, cwd=fork["cwd"], env=worker_env(worker_id=worker_id),
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, start_new_session=True,
        )
    except OSError as error:
        raise SessionUnavailable(f"Claude fork copy failed for {worker_id}: {error}",
                                 short=worker_id) from error
    try:
        stdout, stderr = child.communicate(timeout=budget)
    except subprocess.TimeoutExpired:
        with contextlib.suppress(OSError):
            os.killpg(child.pid, signal.SIGKILL)
        child.communicate()
        raise OperationTimeout(f"Claude fork copy for {worker_id} did not finish in time",
                               short=worker_id) from None
    result = None
    for line in reversed(stdout.strip().splitlines()):
        with contextlib.suppress(ValueError):
            result = json.loads(line)
            break
    if (
        child.returncode != 0 or not isinstance(result, dict)
        or result.get("is_error") or result.get("type") != "result"
        or result.get("session_id") != session_id
        or result.get("num_turns") != 0
        or result.get("local_command") != "rename"
    ):
        detail = (stderr or stdout or "").strip()[-400:]
        raise SessionUnavailable(f"Claude fork copy failed for {worker_id} "
                                 f"(rc={child.returncode}): {detail}", short=worker_id)


def copy_original_claude_fork(store, record: dict, *, submission_id: str,
                              host_pid: int, host_start: str) -> dict:
    """Only the submitted original host may admit ONE copy, after socket.listen.

    This record CAS is copy submission admission, not a new native-create
    mechanism. A crash anywhere after it is indeterminate, never a recopy.
    """
    from ccx.claude.transcript import extract_background_tasks
    from ccx.store.root import CreationIndeterminate
    from ccx.store.store import fingerprint_v2

    worker_id = record["workerId"]
    current = store.record(f"claude:{worker_id}")
    anchor = current.get("forkAnchor")
    native = current.get("native") or {}
    lifecycle = current.get("lifecycle") or {}
    if (not isinstance(anchor, dict) or anchor != record.get("forkAnchor")
            or current.get("forkedFrom") != record.get("forkedFrom")
            or native.get("sessionId") != (record.get("native") or {}).get("sessionId")
            or not claude_fork_pending(current)
            or lifecycle.get("nativeForkCopyAttempt") is not None
            or lifecycle.get("nativeForkProof") is not None
            or lifecycle.get("hostPid") != host_pid
            or str(lifecycle.get("hostStart")) != str(host_start)):
        raise CreationIndeterminate(f"Claude fork {worker_id} copy admission unproven; never recopy")
    with store.lease():
        owner, claim = store._owner(worker_id), store._claim(worker_id)
        if (not isinstance(owner, dict) or not isinstance(claim, dict)
                or (owner.get("createIntent") or {}).get("forkAnchor") != anchor
                or claim.get("forkAnchor") != anchor
                or claim.get("nativeBinding") != {"sessionId": native["sessionId"]}
                or claim.get("createSha256") != fingerprint_v2(
                    engine="claude", cwd=anchor["cwd"],
                    create_args=anchor["createArgs"], fork_anchor=anchor)):
            raise CreationIndeterminate(f"Claude fork {worker_id} original anchor/claim differs")
    activation = store.native_activation(f"claude:{worker_id}")
    if (activation.get("state") != "submitted-unknown"
            or activation.get("source") != f"claude-session:{native['sessionId']}"
            or activation.get("submissionId") != submission_id
            or not isinstance(submission_id, str) or not submission_id):
        raise CreationIndeterminate(f"Claude fork {worker_id} lacks original submitted host")

    child_path = pathlib.Path(current["transcript"])
    from ccx.store.paths import transcript_path_for
    if (current["transcript"] != str(transcript_path_for(anchor["cwd"], native["sessionId"]))
            or child_path.exists() or child_path.is_symlink()):
        raise CreationIndeterminate(
            f"Claude fork {worker_id} already has unproven native bytes; never adopt or recopy")
    store.mutate_record(
        f"claude:{worker_id}",
        patches={"lifecycle.nativeForkCopyAttempt": submission_id},
        expect={"lifecycle.nativeForkCopyAttempt": None,
                "lifecycle.nativeForkProof": None,
                "lifecycle.hostPid": host_pid,
                "lifecycle.hostStart": host_start},
    )
    _run_materializer(current, anchor)
    try:
        st = child_path.lstat()
        if not stat.S_ISREG(st.st_mode) or st.st_size == 0:
            raise ValueError("Forked Claude conversation is missing; inspect the worker")
        with child_path.open("rb") as stream:
            stream.seek(-1, os.SEEK_END)
            if stream.read(1) != b"\n":
                raise ValueError("incomplete child native JSONL")
        malformed = {}
        found = False
        for row in _rows(child_path, messages=False, stats=malformed):
            if row.get("type") in ("user", "assistant") and row.get("uuid"):
                if row.get("sessionId") != native["sessionId"]:
                    raise ValueError("foreign child native SID in copied history")
                if row["uuid"] == anchor["resumeAt"]:
                    found = True
        if malformed.get("malformedLines") or not found:
            raise ValueError("child transcript lacks complete exact original fork message")
        tasks = sorted({task["task_id"] for task in extract_background_tasks(child_path)
                        if task.get("status") in ("running", "unknown")
                        and task.get("task_id")})
        if (len(tasks) > 1024 or any(not isinstance(task, str) or len(task) > 256
                                     for task in tasks)):
            raise ValueError("inherited open task identifiers exceed proof bound")
        if child_path.stat().st_size != st.st_size:
            raise ValueError("Forked Claude conversation changed during copy; retry the fork")
    except (OSError, ValueError, TypeError, KeyError) as exc:
        raise SessionUnavailable(
            f"Claude fork {worker_id} has no exact copied child history: {exc}",
            short=worker_id,
        ) from exc

    proof = {"workerId": worker_id, "nativeSessionId": native["sessionId"],
             "forkAnchor": anchor, "copiedAtMs": now_ms(),
             "inheritedBytes": st.st_size, "inheritedOpenTaskIds": tasks}
    committed = store.mutate_record(
        f"claude:{worker_id}",
        patches={"lifecycle.nativeForkProof": proof},
        expect={"lifecycle.nativeForkProof": None,
                "lifecycle.nativeForkCopyAttempt": submission_id,
                "lifecycle.hostPid": host_pid,
                "lifecycle.hostStart": host_start},
    )
    if _claude_copy_proof(committed) != proof:
        raise CreationIndeterminate(f"Claude fork {worker_id} copy proof was not authenticated")
    return committed


def inherited_task_filter(record: dict | None, tasks: list[dict]) -> list[dict]:
    """Source-owned open tasks do not become the child's live work."""
    proof = _claude_copy_proof(record)
    inherited = set(proof["inheritedOpenTaskIds"]) if proof else set()
    return [task for task in tasks if task.get("task_id") not in inherited] if inherited else tasks


def inherited_offset(record: dict | None) -> int:
    """Token/usage accounting starts after independently proven native copy."""
    proof = _claude_copy_proof(record)
    return proof["inheritedBytes"] if proof else 0


# -- source and original child plan -----------------------------------------


def _conflict(record: dict, reason: str, text: str) -> OperationConflict:
    return OperationConflict(text, short=record.get("short"), reason=reason,
                             worker_id=record.get("workerId"))


def _settled(record: dict) -> None:
    """Refuse an observable turn, input, queue, or compact before choosing R."""
    lifecycle = record.get("lifecycle") or {}
    state = lifecycle.get("state")
    if (state == "waiting" or record.get("waitingFor")
            or record.get("pendingApprovals")):
        raise _conflict(record, "source_waiting", "source has unresolved native input")
    if (state not in {"idle", "idle-killed", "crashed", "dead", "budget-stopped"}
            or lifecycle.get("activeTurnId") or record.get("activeCompact")
            or record.get("pendingSend")):
        raise _conflict(record, "source_busy", "source turn, queue or compact not settled")
    if record.get("terminalOnly"):
        raise SessionUnavailable("Source worker has no verified conversation; inspect it before forking")


def _no_pending_effects(owner, record: dict) -> None:
    """Existing original semantic receipts/compact journal, no new tracker."""
    if owner.engine == "claude":
        from ccx.claude.delivery import iter_receipts
        from ccx.ops.compact_ops import PENDING_STATUSES, read_journal
        if any(op["status"] in PENDING_STATUSES for op in
               read_journal(owner.worker)["operations"]):
            raise _conflict(record, "source_busy", "source has native compact pending")
    else:
        from ccx.codex.delivery import iter_receipts
    # An accepted receipt's settlement is optional observation metadata (steer
    # and unobserved async sends never get one); the native settled point
    # below is the authority for accepted work, so only admission refuses.
    if any(row["state"] in {"prepared", "owned", "submitted"}
           for row in iter_receipts(owner.worker)):
        raise _conflict(record, "source_busy", "source has a pending original send")


def _claude_point(store, owner, record: dict, deadline: float | None) -> dict:
    """Pin accepted original native file; terminal JSONL row is the fork point."""
    from ccx.claude.transcript import open_original_claude_source
    from ccx.claude.managed_turns import turn_settlements
    with open_original_claude_source(store, owner, record) as source:
        if source is None:
            raise InvalidRequest("Source Claude worker has no conversation to fork")
        pinned, before, complete_end, native_path, _dir_fd = source
        if not complete_end or complete_end != before.st_size:
            raise _conflict(record, "source_busy", "source native transcript is incomplete")
        malformed = {}
        last = None
        sid = record["native"]["sessionId"]
        for row in _rows(pinned, deadline, before_offset=complete_end, stats=malformed):
            if row.get("sessionId") != sid:
                raise SessionUnavailable("Claude original native transcript has a foreign session")
            last = row
        if malformed.get("malformedLines"):
            raise SessionUnavailable("Claude history is unreadable; inspect the source worker")
        if last is None:
            raise InvalidRequest("source has no Claude conversation to fork")
        if _turn_open(last):
            raise _conflict(
                record, "source_busy",
                "Source worker has an unfinished turn; wait for it to end or "
                "stop the worker before forking",
            )
        now = os.fstat(pinned.fd)
        if (now.st_size, now.st_mtime_ns) != (before.st_size, before.st_mtime_ns):
            raise _conflict(record, "source_busy", "source native transcript changed during fork planning")
        terminals = turn_settlements(owner.worker)
        if terminals:
            latest = terminals[-1]
            count = (record.get("stats") or {}).get("turns")
            if (type(count) is int and latest["turn"] < count
                    or (type(record.get("lastSettledAt")) is int
                        and latest["settledAtMs"] < record["lastSettledAt"])):
                raise _conflict(record, "source_busy", "Claude original turn settlement is stale")
        return {
            "sourceNativeSessionId": sid, "resumeAt": last["uuid"],
            "transcript": str(native_path),
        }


def _turn_open(row: dict) -> bool:
    """An incomplete assistant/tool continuation is not a settled fork point."""
    if row.get("type") == "assistant":
        return (row.get("message") or {}).get("stop_reason") not in {
            "end_turn", "stop_sequence", "max_tokens", "refusal",
        }
    if row.get("isMeta"):
        return False
    content = (row.get("message") or {}).get("content")
    if isinstance(content, list):
        content = " ".join(x.get("text") or "" for x in content if isinstance(x, dict))
    return not isinstance(content, str) or not content.lstrip().startswith((
        "[Request interrupted by user", "<command-name>", "<local-command-",
        "<command-message>",
    ))


def _codex_point(store, owner, record: dict, deadline: float | None) -> dict:
    """Read the exact owned native thread without starting a model turn."""
    from ccx.codex.runtime import read_thread
    native = record.get("native") or {}
    tid = native.get("threadId")
    source = store.codex_source(owner.worker)
    if (native.get("nativeState") != "thread-bound"
            or native.get("threadSource") != source
            or not isinstance(tid, str) or not tid):
        raise SessionUnavailable("Codex source conversation cannot be verified; inspect it before forking")
    thread = read_thread(
        tid, cwd=record["cwd"], worker_id=owner.worker_id,
        expected_source=source, limit=1, newest_first=True,
        timeout=min(30., _remaining(deadline)) if deadline is not None else 30.,
    )
    if (thread.get("id") != tid
            or thread.get("threadSource") != source):
        raise SessionUnavailable("Codex source conversation changed; inspect it before forking")
    turns = thread.get("turns")
    native_state = thread.get("status")
    if isinstance(native_state, dict):
        native_state = native_state.get("type")
    if native_state not in (None, "idle"):
        raise _conflict(record, "source_busy", "Codex original native thread is not idle")
    if not isinstance(turns, list) or not turns:
        raise InvalidRequest("Source Codex worker has no turn to fork")
    last = turns[-1]
    if (not isinstance(last, dict) or not isinstance(last.get("id"), str)
            or not last["id"] or last.get("status") != "completed"):
        raise _conflict(record, "source_busy", "last native Codex turn is not completed")
    recorded = record.get("lastTurn") or {}
    if recorded.get("turnId") not in (None, last["id"]):
        raise _conflict(record, "source_busy", "Codex source record/last native turn disagree")
    return {"sourceNativeThreadId": tid, "lastTurnId": last["id"]}


def _launch(engine: str, args: dict) -> dict:
    """The original normalized creation arguments, preserved for retry."""
    if engine == "claude":
        return {
            "model": args.get("model"), "effort": args.get("effort"),
            "agent": args.get("agent"),
            "append_system_prompt": args.get("append_system_prompt"),
            "allowed_tools": args.get("allowed_tools"),
            "disallowed_tools": args.get("disallowed_tools"),
            "add_dir": args.get("add_dir") or None,
            "permission_mode": args.get("permission_mode"),
            "permissions": "full" if args.get("skip_permissions") is True else "restricted",
            "idle_timeout": args.get("idle_timeout_s"),
        }
    return {
        "model": args.get("model"), "effort": args.get("effort"),
        "sandbox": args.get("sandbox"), "approval_policy": args.get("approvalPolicy"),
        "idle_timeout": args.get("idleTimeout"),
    }


def _retry(store, target, child_id: str, overrides: dict) -> dict | None:
    """Inspect only existing immutable owner/claim/record; NEVER reread source."""
    from ccx.store.root import CreationIndeterminate, CreationConflict
    from ccx.store.store import fingerprint_v2
    from ccx.store.identity import parse_managed_target
    source = parse_managed_target(target)
    with store.lease():
        owner = store._owner(child_id)
        if owner is None:
            return None
        if not isinstance(owner, dict):
            raise CreationIndeterminate(f"fork {child_id} original owner is unreadable")
        intent = owner.get("createIntent")
        if not isinstance(intent, dict):
            raise CreationIndeterminate(f"fork {child_id} original immutable intent unavailable")
        anchor = intent.get("forkAnchor")
        if (not isinstance(anchor, dict)
                or anchor.get("sourceWorkerId") != source.worker_id
                or (source.qualified_engine is not None
                    and source.qualified_engine != anchor.get("engine"))
                or anchor.get("overrides") != overrides):
            raise OperationConflict(
                f"worker_id {child_id} belongs to another original creation/fork",
                worker_id=child_id, reason="same_worker_id_create_spec_mismatch",
            )
        engine = anchor.get("engine")
        original_args = anchor.get("createArgs")
        cwd = anchor.get("cwd")
        claim = store._claim(child_id)
        record = store._record(engine, child_id) if engine in {"claude", "codex"} else None
        binding = claim.get("nativeBinding") if isinstance(claim, dict) else None
        if (owner.get("status") not in {"worker", "indeterminate-reserved"}
                or not isinstance(binding, dict)
                or not isinstance(claim, dict) or not isinstance(record, dict)
                or not isinstance(original_args, dict) or not isinstance(cwd, str)
                or record.get("forkAnchor") != anchor
                or record.get("createSha256") != claim.get("createSha256")
                or record.get("fingerprintVersion") != 2
                or record.get("workerId") != child_id or record.get("engine") != engine
                or record.get("cwd") != cwd
                or not isinstance(record.get("native"), dict)
                or any(record["native"].get(k) != v for k, v in
                       binding.items())):
            raise CreationIndeterminate(
                f"fork {child_id} lacks original immutable child record proof"
            )
        lineage = record.get("forkedFrom")
        lineage_keys = ("engine", "sourceWorkerId", "forkedAtMs") + (
            ("sourceNativeSessionId", "resumeAt") if engine == "claude"
            else ("sourceNativeThreadId", "lastTurnId")
        )
        if not isinstance(lineage, dict) or any(
            lineage.get(key) != anchor.get(key) for key in lineage_keys
        ):
            raise CreationIndeterminate(
                f"fork {child_id} first-record native point differs from original claim"
            )
        if (claim.get("forkAnchor") != anchor
                or claim.get("workerId") != child_id
                or claim.get("engine") != engine
                or claim.get("generation") != store.generation
                or claim.get("fingerprintVersion") != 2
                or not isinstance(claim.get("createSha256"), str)
                or fingerprint_v2(engine=engine, cwd=cwd, create_args=original_args,
                                  fork_anchor=anchor) != claim["createSha256"]):
            raise CreationIndeterminate(
                f"fork {child_id} immutable first-create fingerprint changed"
            )
        try:
            store._check_original_create_intent(
                owner, engine=engine, worker_id=child_id,
                fingerprint=claim["createSha256"],
                native=claim["nativeBinding"], anchor=anchor,
            )
        except CreationConflict as exc:
            raise CreationIndeterminate(
                f"fork {child_id} has contradictory original identity"
            ) from exc
        return {
            "engine": engine, "source": None, "cwd": cwd,
            "fork_anchor": anchor, "launch": _launch(engine, original_args),
            "name": record.get("name"), "seed": anchor.get("seed") or {}, "existing": True,
        }


def plan(target: str, *, worker_id: str | None = None,
         model: str | None = None, effort: str | None = None,
         deadline: float | None = None,
         expected_generation: str | None = None,
         require_binary: bool = False) -> dict:
    """Prepare one frozen native fork point, without child creation or effects.

    A supplied child UUID is checked FIRST: if it exists, only its original
    full-ID owner/claim/record and frozen anchor can define a retry.
    """
    from ccx import constants
    from ccx.store.root import require_ccx_active
    from ccx.store.identity import canonical_worker_id
    from ccx.ops.creation_steps import validated_create_spec
    store = require_ccx_active(
        constants.CCX_ROOT, expected_generation=expected_generation,
    )
    for value in (model, effort):
        if value is not None and (not isinstance(value, str) or not value.strip()):
            raise InvalidRequest("fork model/effort overrides must be nonempty strings")
    overrides = {"model": model, "effort": effort}
    if worker_id is not None:
        child_id = canonical_worker_id(worker_id)
        previous = _retry(store, target, child_id, overrides)
        if previous is not None:
            store.revalidate()
            return previous
    _remaining(deadline)
    owner = store.resolve(target)
    if require_binary:
        # After child-first conflict checks, but before any Codex native read.
        from ccx.system.engine_binary import resolve_engine_binary
        resolve_engine_binary(owner.engine)
    record = store.record(owner.worker)
    _settled(record)
    store.authorize_native_action(owner.worker, "send")  # original claim, no provider call
    _no_pending_effects(owner, record)
    if owner.engine == "claude":
        point = _claude_point(store, owner, record, deadline)
    else:
        if record.get("historyKnownEmpty") is True:
            raise InvalidRequest(
                "cannot fork: source Codex thread has no completed conversation "
                "turn; send its first prompt and retry"
            )
        point = _codex_point(store, owner, record, deadline)
    _remaining(deadline)
    # Fresh re-read refuses a source that acquired an in-flight turn meanwhile;
    # native point itself is immutable, even if later activity starts.
    current = store.record(owner.worker)
    _settled(current)
    _no_pending_effects(owner, current)
    if (current.get("recordRevision") != record.get("recordRevision")
            or current.get("native") != record.get("native")):
        raise _conflict(record, "source_busy", "source changed while planning its fork")
    original = record.get("createArgs")
    if not isinstance(original, dict):
        raise SessionUnavailable("Source worker settings are missing; inspect it before forking")
    launch = _launch(owner.engine, original)
    launch.update({key: value for key, value in overrides.items() if value is not None})
    cwd, normalized = validated_create_spec(
        engine=owner.engine, cwd=record["cwd"],
        model=launch["model"], effort=launch["effort"],
        idle_timeout=launch["idle_timeout"],
        permissions=launch.get("permissions"),
        sandbox=launch.get("sandbox"), approval_policy=launch.get("approval_policy"),
        agent=launch.get("agent"), append_system_prompt=launch.get("append_system_prompt"),
        allowed_tools=launch.get("allowed_tools"),
        disallowed_tools=launch.get("disallowed_tools"),
        add_dir=launch.get("add_dir"), permission_mode=launch.get("permission_mode"),
    )
    # Only settled/observed source metadata may seed the child's ordinal;
    # subsequent source reconfiguration cannot change this original snapshot.
    stats = record.get("stats") or {}
    turns = stats.get("turns")
    seed = {"stats": {"turns": turns if type(turns) is int and turns >= 0 else 0},
            "lastTurnAt": record.get("lastTurnAt")}
    if owner.engine == "codex":
        last = record.get("lastTurn") or {}
        if last.get("turnId") == point["lastTurnId"]:
            seed["lastTurn"] = dict(last)
    anchor = {
        "engine": owner.engine, "sourceWorkerId": owner.worker_id,
        **point, "cwd": cwd, "createArgs": normalized, "seed": seed,
        "overrides": overrides, "forkedAtMs": now_ms(),
    }
    store.revalidate()
    return {
        "engine": owner.engine, "source": record, "cwd": cwd,
        "fork_anchor": anchor, "launch": _launch(owner.engine, normalized),
        "name": f"{record.get('name') or owner.short} (fork)",
        "seed": seed, "existing": False,
    }


def fork(
    target: str, *, worker_id: str | None = None,
    name: str | None = None, model: str | None = None,
    effort: str | None = None, created_via: str = "local-json-v1",
    prompt: str | None = None,
    caps: dict | None = None, wait: bool = False,
    attachment=None, delivery_id: str | None = None,
    parent: dict | None = None, settle: bool = False,
    deadline: float | None = None, expected_generation: str | None = None,
    start_timeout: float | None = None,
) -> dict:
    """Original-native fork with optional first turn through creation.create.

    A caller-known child is checked by the planner against its original intent
    before any source observation. Native replay belongs solely to creation.
    """
    from ccx.ops.creation import create

    _remaining(deadline)
    planned = plan(
        target, worker_id=worker_id, model=model, effort=effort,
        deadline=deadline, expected_generation=expected_generation,
        require_binary=True,
    )
    # Only an explicit fork --model override requires a new live provider
    # validation. An inherited model is frozen in the original fork spec.
    if planned["engine"] == "codex" and model is not None:
        from ccx.codex.runtime import validate_model
        validate_model(model, cwd=planned["cwd"])
    remaining = _remaining(deadline)
    native_budget = 90.0 if start_timeout is None else start_timeout
    if remaining is not None:
        native_budget = min(native_budget, remaining)
    result = create(
        engine=planned["engine"], cwd=planned["cwd"],
        worker_id=worker_id, name=name if name is not None else planned["name"],
        _fork_anchor=planned["fork_anchor"], **planned["launch"],
        created_via=created_via, start_timeout=native_budget,
        expected_generation=expected_generation, deadline=deadline,
        prompt=prompt, caps=caps, wait=wait,
        attachment=attachment, delivery_id=delivery_id,
        parent=parent, settle=settle,
    )
    return {**result, "forked_from": public_lineage(result["record"])}


def public_lineage(record: dict | None) -> dict | None:
    """Original full-UUID source handle; native session/thread IDs stay separate."""
    if not isinstance(record, dict):
        return None
    fork = record.get("forkedFrom")
    if not isinstance(fork, dict):
        return None
    from ccx.store.identity import InvalidTarget, canonical_worker_id
    engine = fork.get("engine")
    try:
        source_id = canonical_worker_id(fork.get("sourceWorkerId"))
    except InvalidTarget:
        source_id = None
    if engine not in {"claude", "codex"}:
        return None
    return {
        "worker": f"{engine}:{source_id}" if source_id else None,
        "engine": engine, "short": source_id[:8] if source_id else None,
        "worker_id": source_id,
        "session_id": fork.get("sourceNativeSessionId") if engine == "claude" else None,
        "thread_id": fork.get("sourceNativeThreadId") if engine == "codex" else None,
        "forked_at_ms": fork.get("forkedAtMs"),
        **({"source_kind": "codex-thread"}
           if engine == "codex" and fork.get("sourceKind") == "codex-thread"
           and fork.get("sourceWorkerId") is None else {}),
    }
