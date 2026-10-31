"""Identity-first creation: one durable claim and one semantic state machine.

Provider-specific hooks below do only native record preparation and activation.
No display-name search, reservation or compatibility create path lives here.
"""
from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from typing import Any

from ccx import constants
from ccx.constants import DEFAULT_IDLE_TIMEOUT_S
from ccx.errors import (
    BudgetStopped, CreatePartialFailure, InvalidRequest, OperationConflict,
    SessionUnavailable, SessionWaiting, TurnIncomplete,
)
from ccx.controllers import live_vars
from ccx.store.root import CreationConflict
from ccx.store.identity import OwnerConflict, IdentityError

def new_worker_id() -> str:
    """Mint one complete, canonical, caller-persistable UUIDv4 without IO."""
    return str(uuid.uuid4())


def _bind_intent(attachment: live_vars.PreparedAttachment | None) -> dict:
    if attachment is None:
        return {"mode": "none"}
    if attachment.mode == "live":
        return {
            "mode": "live", "names": sorted(attachment.names),
            "owner": attachment.owner.public_identity() if attachment.owner else None,
        }
    if attachment.state_id is not None:
        return {"mode": "durable-shared", "stateId": attachment.state_id}
    return {"mode": "durable", "values": attachment.values or {}}


def _bind_once(store, engine: str, worker_id: str,
               attachment: live_vars.PreparedAttachment | None) -> list[str]:
    """Commit the first variable intent after its worker record, before native effects.

    This receipt is independent of the immutable creation fingerprint. A
    matching create retry must never republish mutable durable or live values.
    """
    from ccx.store.root import _read_json, _write_json

    intent = _bind_intent(attachment)
    digest = hashlib.sha256(json.dumps(
        intent, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")).hexdigest()
    with store.lease():
        with store._claim_lock(worker_id):
            _owner, record = store._owned(f"{engine}:{worker_id}")
            claim = store._claim(worker_id)
            if (claim is None or claim.get("workerId") != worker_id
                    or claim.get("engine") != engine
                    or claim.get("generation") != store.generation
                    or claim.get("createSha256") != record.get("createSha256")):
                raise SessionUnavailable("The worker could not be created; inspect its saved worker ID",
                                         short=worker_id[:8])
            with store._gen("variables", "bindings", engine, create=True) as directory:
                prior = _read_json(directory, worker_id + ".json")
                if prior is not None:
                    if prior.get("schemaVersion") != 1 or prior.get("sha256") != digest:
                        raise OperationConflict(
                            "initial variables intent differs from first binding",
                            short=worker_id[:8], reason="initial_variables_mismatch",
                            worker_id=worker_id,
                        )
                    names = prior.get("names")
                    if (not isinstance(names, list) or any(not isinstance(n, str)
                                                         for n in names)):
                        raise SessionUnavailable("initial variable receipt has invalid names",
                                                 short=worker_id[:8])
                    return list(names)
                if live_vars.read_sidecar(engine, worker_id) is not None:
                    # The first sidecar may have landed before its receipt, or
                    # a later SEND may own it. Never guess/reapply first values.
                    raise SessionUnavailable(
                        f"first variable binding for {engine}:{worker_id} lacks commit proof",
                        short=worker_id[:8],
                    )
                if attachment is not None:
                    activation = claim.get("nativeActivation")
                    if (not isinstance(activation, dict)
                            or activation.get("state") not in {"unprepared", "prepared"}):
                        raise SessionUnavailable(
                            "Worker creation has already been submitted; "
                            "first variables cannot be attached retroactively",
                            short=worker_id[:8],
                        )
                    live_vars.publish_attachment(attachment, engine, worker_id)
                names = sorted(attachment.names) if attachment else []
                _write_json(directory, worker_id + ".json", {
                    "schemaVersion": 1, "sha256": digest, "names": names,
                })
                return names


def _require_codex_fork_ready(store, original, record: dict) -> dict:
    """Offline original fork proof; never require a live daemon/held reply."""
    anchor = original.record.get("forkAnchor")
    if anchor is None:
        return record
    import hashlib
    from ccx.store.root import NativeEffectIndeterminate, _read_json, _flock_at, _HASH
    from ccx.store.store import _digest, fingerprint_v2

    wid = original.worker_id
    native = record.get("native") or {}
    lineage = record.get("forkedFrom")
    activation = store.native_activation(original.worker)
    bound = activation.get("nativeBinding") or {}
    thread_id = bound.get("threadId")
    if (not isinstance(anchor, dict) or anchor.get("engine") != "codex"
            or record.get("forkAnchor") != anchor
            or not isinstance(lineage, dict)
            or any(lineage.get(key) != anchor.get(key) for key in (
                "engine", "sourceWorkerId", "sourceNativeThreadId", "lastTurnId",
                "forkedAtMs"))
            or not isinstance(anchor.get("createArgs"), dict)
            or anchor.get("cwd") != record.get("cwd")
            or fingerprint_v2(engine="codex", cwd=anchor["cwd"],
                              create_args=anchor["createArgs"],
                              fork_anchor=anchor) != record.get("createSha256")
            or activation.get("state") != "confirmed"
            or activation.get("source") != original.native_source
            or not isinstance(activation.get("submissionId"), str)
            or not activation["submissionId"]
            or not isinstance(thread_id, str) or not thread_id
            or thread_id == anchor.get("sourceNativeThreadId")
            or native.get("threadId") != thread_id
            or native.get("threadSource") != original.native_source
            or native.get("nativeState") != "thread-bound"):
        raise NativeEffectIndeterminate(
            f"Codex fork {wid} lacks original child/anchor/activation proof; never re-fork"
        )
    token = activation["submissionId"]
    key = hashlib.sha256(("activate\x00" + token).encode()).hexdigest()
    with store.lease():
        owner, claim = store._owner(wid), store._claim(wid)
        if not isinstance(owner, dict) or not isinstance(claim, dict):
            raise NativeEffectIndeterminate("Codex fork immutable original owner/claim absent")
        store._check_original_create_intent(
            owner, engine="codex", worker_id=wid,
            fingerprint=record["createSha256"], native={}, anchor=anchor,
        )
        if (claim.get("forkAnchor") != anchor
                or claim.get("createSha256") != record["createSha256"]
                or claim.get("fingerprintVersion") != 2
                or claim.get("nativeBinding") != {}
                or claim.get("nativeActivation") != activation):
            raise NativeEffectIndeterminate("Codex fork original immutable claim changed")
        try:
            with store._gen("runtime-metadata", "codex-native-effects", wid) as directory:
                with _flock_at(directory, key + ".lock", exclusive=False,
                               create=False, timeout=.5):
                    effect = _read_json(directory, key + ".json")
        except FileNotFoundError as exc:
            raise NativeEffectIndeterminate("Codex fork original effect marker absent") from exc
    candidate = effect.get("nativeCandidate") if isinstance(effect, dict) else None
    if (not isinstance(effect, dict) or not isinstance(candidate, dict)
            or any(effect.get(k) != v for k, v in {
                "workerId": wid, "engine": "codex", "generation": store.generation,
                "kind": "activate", "token": token, "state": "submitted-unknown",
                "source": original.native_source,
                "createSha256": record["createSha256"],
                "nativeMethod": "thread/fork", "forkAnchor": anchor,
            }.items())
            or effect.get("nativeRejected") is not None
            or effect.get("nativeNotSubmitted") is not None
            or not isinstance(effect.get("daemonInstanceId"), str)
            or not effect["daemonInstanceId"]
            or type(effect.get("daemonPid")) is not int or effect["daemonPid"] <= 0
            or type(effect.get("appServerPid")) is not int or effect["appServerPid"] <= 0
            or candidate.get("threadId") != thread_id
            or candidate.get("submittedSource") != original.native_source
            or candidate.get("appServerPid") != effect["appServerPid"]
            or not isinstance(candidate.get("nativeReplySha256"), str)
            or not _HASH.fullmatch(candidate["nativeReplySha256"])):
        raise NativeEffectIndeterminate("Codex fork records disagree; inspect the worker before retrying")
    # All inputs are stable original marker/claim facts. A successful provider
    # read was mandatory before the claim could receive this precise digest;
    # no current PID, held-slot, native probe or mutable child setting is used.
    evidence = {
        "workerId": wid, "threadSource": original.native_source,
        "threadId": thread_id, "submissionId": token,
        "daemonPid": effect["daemonPid"],
        "appServerPid": effect["appServerPid"],
        "nativeMethod": "thread/fork", "forkAnchor": anchor,
        "forkedFromId": anchor["sourceNativeThreadId"],
        "lastTurnId": anchor["lastTurnId"], "lastTurnStatus": "completed",
        "nativeReplySha256": candidate["nativeReplySha256"],
    }
    if (activation.get("nativeEvidenceSha256") != _digest(evidence)
            or not _HASH.fullmatch(activation["nativeEvidenceSha256"])):
        raise NativeEffectIndeterminate("Codex fork records changed; inspect the worker before retrying")
    return record


def _complete_codex_create(store, original, info: dict, *, start_timeout, deadline):
    """Complete only the one original Codex start; submitted fate is observational."""
    from ccx.codex.runtime import daemon_rpc, ensure_daemon
    from ccx.system.engine_binary import resolve_engine_binary
    from ccx.constants import CODEXD_RPC_GENERATION
    from ccx.ops.creation_steps import dispatch_prepared_codex_native, reconcile_submitted_native
    from ccx.store.root import CreationIndeterminate, NativeEffectIndeterminate

    def terminal_proof():
        # Durable provider errors and pre-IPC refusals are different fates.
        rejected = store.reconcile_codex_native_rejection(original.worker)
        if rejected is not None:
            info["native_rejection"] = {
                "code": rejected["code"], "message": rejected["message"],
            }
            info["activation_state"] = "rejected"
            raise NativeEffectIndeterminate("original Codex provider rejected " + (
                "thread/fork" if original.record.get("forkAnchor") else "thread/start"))
        local = store.reconcile_codex_local_refusal(original.worker)
        if local is not None:
            info["native_local_refusal"] = {
                "reason": local["reason"], "stage": local["stage"], "proven": True,
            }
            info["activation_state"] = "local-refused"
            raise NativeEffectIndeterminate("Codex refused to start; create a new worker ID to retry")

    def remaining():
        return float('inf') if deadline is None else deadline - time.monotonic()

    terminal_proof()  # sealed terminal claims must replay without a daemon/binary
    activation = store.native_activation(original.worker)
    if activation["state"] == "confirmed":
        return _require_codex_fork_ready(
            store, original, store.bind_confirmed_native(original.worker))
    try:
        resolve_engine_binary("codex")
    except SessionUnavailable as error:
        raise NativeEffectIndeterminate(
            "Codex executable not found; set CCX_CODEX_BIN or PATH and retry"
        ) from error
    if remaining() <= 0:
        raise NativeEffectIndeterminate("Codex startup timed out; inspect this worker instead of starting it twice")
    # The shared app-server may need longer than four seconds to initialize on
    # a cold, loaded machine. Both concurrent first users must wait for the
    # *same* codexd within their caller-selected native start observation
    # budget, rather than expiring an unrelated fixed four-second boot timer.
    ensure_daemon(deadline=min(time.monotonic() + start_timeout,
                               deadline if deadline is not None else float('inf')))

    def submit_original(budget):
        admitted_until = min(time.monotonic() + budget,
                             deadline if deadline is not None else float('inf'))
        if time.monotonic() >= admitted_until:
            return {"ok": False, "error": "Codex startup timed out; check worker status"}
        claim = store.native_activation(original.worker)
        answer = daemon_rpc({
            "op": "native-activate", "workerId": original.worker_id,
            "submissionId": claim["submissionId"],
            "nativeAdmissionDeadline": admitted_until,
            "rpcGeneration": CODEXD_RPC_GENERATION,
        }, timeout=max(0., admitted_until - time.monotonic()))
        if (answer.get("transport") == "served"
                and isinstance(answer.get("localRefusalReason"), str)):
            info["native_local_refusal"] = {
                "reason": answer["localRefusalReason"], "proven": False,
            }
        return answer

    if activation["state"] != "submitted-unknown":
        try:
            budget = min(2., remaining())
            if budget <= 0:
                raise NativeEffectIndeterminate("Codex startup timed out; check worker status; no replay")
            dispatch_prepared_codex_native(
                store, original, bounded_native_submit=submit_original,
                dispatch_budget_s=budget,
            )
        except CreationIndeterminate as contention:
            if str(contention) not in {
                "native activation already submitted or claimed by another caller",
                "native activation already submitted/settled/ambiguous",
            }:
                raise
            advanced = store.reserve_creation(
                original.worker_id, engine="codex",
                fingerprint=original.record["createSha256"], native_binding={},
                fork_anchor=original.record.get("forkAnchor"),
            )
            claim = store.native_activation(original.worker)
            if (advanced.activation_state not in {
                    "submitted-unknown", "confirmed", "rejected", "local-refused"
                } or advanced.submission_id != claim.get("submissionId")
                    or claim.get("source") != original.native_source
                    or not isinstance(advanced.submission_id, str)
                    or not advanced.submission_id):
                raise contention

    observe_until = min(time.monotonic() + start_timeout,
                        deadline if deadline is not None else float('inf'))
    while True:
        claim = store.native_activation(original.worker)
        if claim["state"] == "confirmed":
            return _require_codex_fork_ready(
                store, original, store.bind_confirmed_native(original.worker))
        if claim["state"] in {"rejected", "local-refused"}:
            terminal_proof()
        remaining_observation = observe_until - time.monotonic()
        if remaining_observation <= 0:
            raise NativeEffectIndeterminate(
                "original Codex " + (
                    "thread/fork" if original.record.get("forkAnchor") else "thread/start")
                + " is still submitted-unknown; "
                "retain original full worker UUID; never replay"
            )
        observed = daemon_rpc({
            "op": "native-activation-status", "workerId": original.worker_id,
            "submissionId": claim["submissionId"],
            "rpcGeneration": CODEXD_RPC_GENERATION,
        }, timeout=min(3., remaining_observation))
        if observed.get("localRefused") or observed.get("rejected"):
            terminal_proof()
        if observed.get("accepted") is True:
            if time.monotonic() >= observe_until:
                raise NativeEffectIndeterminate("Codex startup timed out; check the worker before retrying")
            return _require_codex_fork_ready(
                store, original, reconcile_submitted_native(
                    store, original, original_receipt=observed,
                ).record)
        if observed.get("ok") is not True:
            # A peer could have confirmed during status; never re-submit.
            state = store.native_activation(original.worker)["state"]
            if state == "confirmed":
                return _require_codex_fork_ready(
                store, original, store.bind_confirmed_native(original.worker))
            if state in {"rejected", "local-refused"}:
                terminal_proof()
            raise NativeEffectIndeterminate(
                "Codex did not confirm starting; check the worker: "
                + str(observed.get("error") or "native evidence unavailable")
            )
        if time.monotonic() >= observe_until:
            raise NativeEffectIndeterminate(
                "original Codex " + (
                    "thread/fork" if original.record.get("forkAnchor") else "thread/start")
                + " is still submitted-unknown; "
                "retain original full worker UUID; never replay"
            )
        time.sleep(min(.1, max(0., observe_until - time.monotonic())))


def _require_claude_fork_ready(store, original, record: dict) -> dict:
    """A local transcript copy and a genuine original SessionStart are separate.

    The original claim is only confirmed from the authenticated host proof;
    retain its durable proof without requiring a still-live host on ID retry.
    """
    if original.record.get("forkAnchor") is None:
        return record
    from ccx.ops.forking import _claude_copy_proof
    from ccx.store.root import NativeEffectIndeterminate
    from ccx.store.store import _digest
    activation = store.native_activation(original.worker)
    start = (record.get("lifecycle") or {}).get("nativeStartProof")
    native_sid = (record.get("native") or {}).get("sessionId")
    source = f"claude-session:{native_sid}"
    if (record.get("forkAnchor") != original.record.get("forkAnchor")
            or _claude_copy_proof(record) is None
            or activation.get("state") != "confirmed"
            or activation.get("source") != source
            or (activation.get("nativeBinding") or {}).get("sessionId") != native_sid
            or not isinstance(start, dict)
            or start.get("workerId") != original.worker_id
            or start.get("engine") != "claude"
            or start.get("generation") != store.generation
            or start.get("nativeSource") != source
            or start.get("sessionId") != native_sid
            or start.get("submissionId") != activation.get("submissionId")
            or start.get("nativeEvidenceSha256") != _digest({
                key: value for key, value in start.items()
                if key != "nativeEvidenceSha256"
            })):
        raise NativeEffectIndeterminate(
            f"Claude fork {original.worker_id} lacks exact copy and native SessionStart proofs; "
            "retain original worker UUID, never recopy or restart"
        )
    return record


def _complete_claude_create(store, original, *, start_timeout, deadline):
    """Admit only the original Claude host, then verify its exact SessionStart."""
    from ccx.claude.spawn import submit_original_claude_start, verified_original_claude_start
    from ccx.ops.creation_steps import activate_prepared_worker, reconcile_submitted_native
    from ccx.store.root import CreationIndeterminate, NativeEffectIndeterminate

    activation = store.native_activation(original.worker)
    if activation["state"] == "confirmed":
        return _require_claude_fork_ready(
            store, original, store.bind_confirmed_native(original.worker))
    if activation["state"] not in {"unprepared", "prepared", "submitted-unknown"}:
        raise NativeEffectIndeterminate("Claude is not ready to start; check worker status")
    if activation["state"] != "submitted-unknown":
        if original.record.get("origin") == "ccx-import":
            from ccx.claude.external import exact_import_candidate, import_eligibility
            from ccx.store.paths import transcript_path_for
            sid = original.record["native"]["sessionId"]
            source = exact_import_candidate(sid)
            ok, _code, why = import_eligibility(source)
            if (not ok or source["cwd"] != original.record["cwd"]
                    or source["transcript"] != original.record["transcript"]
                    or source["transcript"] != str(transcript_path_for(source["cwd"], sid))):
                raise SessionUnavailable("import source changed before first managed resume: "
                                         + (why if not ok else "original transcript/cwd differs"))
        from ccx.system.engine_binary import resolve_engine_binary
        try:
            resolve_engine_binary("claude")
        except SessionUnavailable as error:
            raise NativeEffectIndeterminate(
                "Claude executable not found; set CCX_CLAUDE_BIN or PATH and retry"
            ) from error
        rem = float('inf') if deadline is None else deadline - time.monotonic()
        # A real host proof requires a bounded local status handshake. Do not
        # deliberately enter a >.15s launcher on a .15s protocol deadline.
        if rem < .3:
            raise NativeEffectIndeterminate("Claude could not start before timeout; retry with more time")
        from ccx.claude.settings import ensure_trusted_paths
        from ccx.errors import SpawnFailed
        try:
            ensure_trusted_paths([
                original.record["cwd"],
                *((original.record.get("createArgs") or {}).get("add_dir") or []),
            ])
        except OSError as exc:
            raise SpawnFailed(f"Claude trust settings could not be written: {exc}", short=original.worker_id[:8],
                              phase="settings-io", detail=str(exc)) from exc
        try:
            accepted = activate_prepared_worker(
                store, original,
                bounded_native_submit=lambda budget: submit_original_claude_start(
                    store, original, budget, deadline=deadline),
                dispatch_budget_s=min(2., rem),
            )
        except NativeEffectIndeterminate:
            # Original submitted-unknown may already have produced an
            # authentic host SessionStart after its short admission budget.
            # Observe that same submission below, never dispatch again.
            if store.native_activation(original.worker)["state"] != "submitted-unknown":
                raise
        except CreationIndeterminate as contention:
            if str(contention) not in {
                "native activation already submitted or claimed by another caller",
                "native activation already submitted/settled/ambiguous",
            }:
                raise
            binding = {"sessionId": original.record["native"]["sessionId"]}
            advanced = store.reserve_creation(
                original.worker_id, engine="claude",
                fingerprint=original.record["createSha256"], native_binding=binding,
                fork_anchor=original.record.get("forkAnchor"),
                provenance=("ccx-import" if original.record.get("origin") == "ccx-import"
                            else "ccx-new"),
                imported_from=original.record.get("importedFrom"),
            )
            claim = store.native_activation(original.worker)
            if (advanced.activation_state not in {"submitted-unknown", "confirmed"}
                    or advanced.submission_id != claim.get("submissionId")
                    or claim.get("source") != original.native_source
                    or not isinstance(advanced.submission_id, str)
                    or not advanced.submission_id):
                raise contention
        else:
            if accepted.activation_state == "confirmed":
                if deadline is not None and time.monotonic() >= deadline:
                    raise NativeEffectIndeterminate(
                        "original Claude SessionStart confirmed after request deadline; no replay"
                    )
                return _require_claude_fork_ready(store, original, accepted.record)

    observe_until = min(time.monotonic() + start_timeout,
                        deadline if deadline is not None else float('inf'))
    while True:
        claim = store.native_activation(original.worker)
        if claim["state"] == "confirmed":
            return _require_claude_fork_ready(
                store, original, store.bind_confirmed_native(original.worker))
        remaining = observe_until - time.monotonic()
        if remaining <= 0:
            raise NativeEffectIndeterminate("Claude startup is not confirmed; check worker status before retrying")
        proof = verified_original_claude_start(store, original, deadline=observe_until)
        if proof:
            if time.monotonic() >= observe_until:
                raise NativeEffectIndeterminate("Claude startup timed out; inspect worker status before retrying")
            bound = reconcile_submitted_native(
                store, original, original_receipt=proof,
            ).record
            return _require_claude_fork_ready(store, original, bound)
        time.sleep(min(.025, max(0., observe_until - time.monotonic())))


def _finalize_empty_codex_create(ctx, result: dict, states=("prepared",)) -> None:
    """Apply the promptless Codex empty-history proof to a bound, turnless worker.

    The native admission path may already have loaded the thread into idle
    before refusing its first turn. Both states carry the same original
    thread/start proof; neither is evidence that a turn was delivered.
    """
    latest = ctx.record(result["worker"])
    native = latest.get("native") or {}
    lc = latest.get("lifecycle") or {}
    state = lc.get("state")
    if (ctx.native_activation(result["worker"])["state"] == "confirmed"
            and native.get("nativeState") == "thread-bound"
            and native.get("threadSource") == ctx.codex_source(result["worker"])
            and state in states
            and not lc.get("activeTurnId")
            and not latest.get("pendingSend")
            and not latest.get("activeCompact")
            and not latest.get("nativeTurnStart")):
        inherited = latest.get("forkAnchor") is not None
        turns = (latest.get("stats") or {}).get("turns")
        empty = (not inherited and turns == 0 and not latest.get("lastTurn"))
        from ccx.store.root import RecordConflict
        patches = {"historyKnownEmpty": empty}
        if state == "prepared":
            patches["lifecycle.state"] = "idle"
        try:
            latest = ctx.mutate_record(
                result["worker"], patches=patches,
                expect={"lifecycle.state": state,
                        "lifecycle.activeTurnId": None},
            )
        except RecordConflict:
            # A concurrent original activation may already be idle.
            latest = ctx.record(result["worker"])
            if (latest.get("lifecycle") or {}).get("state") != "idle":
                raise
        result["record"] = latest


def create(*, prompt: str | None = None,
           caps: dict | None = None, wait: bool = False,
           delivery_id: str | None = None,
           parent: dict | None = None, settle: bool = False,
           start_timeout: float | None = None,
           expected_generation: str | None = None,
           deadline: float | None = None,
           **options: Any) -> dict:
    """One proven full-UUID native create for CLI, Python and local protocol."""
    from math import isfinite
    from ccx.store.root import require_ccx_active, GenerationError, NativeEffectIndeterminate
    from ccx.ops.creation_steps import prepare_worker, validated_create_spec
    from ccx.store.identity import InvalidTarget, canonical_worker_id, mint_worker_id

    if prompt is not None and (not isinstance(prompt, str) or not prompt.strip()):
        raise InvalidRequest("initial prompt must be nonblank text; no identity claimed")
    cap = caps
    if cap is not None:
        from ccx.workers import _check_caps
        _check_caps(cap, engine=options.get("engine"))
        if not prompt:
            raise InvalidRequest("initial cap requires a prompt; no identity claimed")
    if delivery_id is not None and (prompt is None or delivery_id != "create:initial"):
        raise InvalidRequest(
            "initial delivery_id must be create:initial with a prompt; no identity claimed"
        )
    if wait and prompt is None:
        raise InvalidRequest("initial-turn wait needs a prompt; no identity claimed")
    imported_sid = options.get("import_session_id")
    imported_from = options.get("imported_from")
    if (imported_sid is not None or imported_from is not None) and (
            options.get("engine") != "claude" or prompt is not None
            or options.get("created_via") != "import"
            or not isinstance(imported_sid, str)
            or not isinstance(imported_from, dict)):
        raise InvalidRequest("Import requires a Claude session without a pending prompt")
    turn_timeout = options.get("timeout")
    if turn_timeout is not None and (
            not wait or type(turn_timeout) not in (int, float)
            or not isfinite(turn_timeout) or turn_timeout <= 0):
        raise InvalidRequest(
            "initial-turn timeout requires wait and positive finite seconds; "
            "no identity claimed"
        )
    if options.get("engine") == "claude" and settle:
        raise InvalidRequest(
            "recursive settlement currently requires a Codex target; no identity claimed"
        )
    if settle and parent is None:
        raise InvalidRequest("--settle requires --notify-parent; no identity claimed")
    if parent is not None and (wait or not isinstance(parent, dict)):
        raise InvalidRequest("Notifications cannot be combined with waiting for creation; choose one")
    if options.get("fork") is not None:
        raise SessionUnavailable(
            "This fork format is not supported; "
            "no identity claimed"
        )
    # Preserve the old explicit preparation signature: never expose lower test hooks.
    unexpected = sorted(set(options) - {
        "engine", "cwd", "worker_id", "name", "model", "effort",
        "idle_timeout", "permissions", "sandbox", "approval_policy", "agent",
        "append_system_prompt", "allowed_tools", "disallowed_tools",
        "add_dir", "permission_mode", "created_via", "import_session_id",
        "imported_from", "fork", "_fork_anchor", "timeout", "attachment",
    })
    if unexpected:
        raise TypeError(f"create() got unexpected keyword arguments: {unexpected}")
    # Attachment is internal to creation; the lower builder accepts only its intent.
    attachment = options.pop("attachment", None)
    engine = options.get("engine")
    if attachment is not None:
        from ccx.errors import CapabilityUnsupported
        if not isinstance(attachment, live_vars.PreparedAttachment):
            raise InvalidRequest("initial attachment must be prepared before creation")
        # Same normalizer and defaults as creation_steps.prepare_worker; a
        # duplicate permission-policy interpretation would admit wrong shells.
        _, create_args = validated_create_spec(
            engine=engine, cwd=options.get("cwd"), model=options.get("model"),
            effort=options.get("effort"),
            idle_timeout=options.get("idle_timeout", DEFAULT_IDLE_TIMEOUT_S),
            permissions=options.get("permissions"), sandbox=options.get("sandbox"),
            approval_policy=options.get("approval_policy"), agent=options.get("agent"),
            append_system_prompt=options.get("append_system_prompt"),
            allowed_tools=options.get("allowed_tools"),
            disallowed_tools=options.get("disallowed_tools"),
            add_dir=options.get("add_dir"), permission_mode=options.get("permission_mode"),
        )
        if not (create_args.get("skip_permissions") is True if engine == "claude"
                else create_args.get("sandbox") == "danger-full-access"
                and create_args.get("approvalPolicy") == "never"):
            raise CapabilityUnsupported(
                'Variables require full permissions (permissions="full").',
                engine=engine, capability="shared_python_variables",
            )
    if start_timeout is None:
        start_timeout = 90.
    if (type(start_timeout) not in (int, float)
            or not isfinite(start_timeout) or not 0 < start_timeout <= 90):
        raise InvalidRequest("start_timeout must be positive, finite and at most 90 seconds")
    if deadline is not None and (type(deadline) not in (float, int)
                                 or not isfinite(deadline)):
        raise InvalidRequest("native creation deadline must be finite")

    # Pin this original ACTIVE generation before minting or touching a native
    # path. A different generation is never an implicit fallback for a retry.
    try:
        ctx = require_ccx_active(constants.CCX_ROOT,
                                 expected_generation=expected_generation)
    except (GenerationError, OSError) as exc:
        raise SessionUnavailable(f"Worker records are unavailable; check CCX_ROOT: {exc}") from exc
    pinned_generation = ctx.generation
    supplied = options.get("worker_id")
    if supplied is not None:
        try:
            supplied = canonical_worker_id(supplied)
        except InvalidTarget as exc:
            raise InvalidRequest(
                f"worker_id must be canonical full UUIDv4: {exc}"
            ) from exc
    # A *fresh* creation cannot claim a UUID without a native executable.
    # Existing caller-owned UUID retries still reconcile their durable claim,
    # including a terminal provider refusal when the CLI has since disappeared.
    existing = False
    if supplied is not None:
        from ccx.store.identity import UnknownWorker
        try:
            existing = ctx.resolve(supplied).engine == engine
        except UnknownWorker:
            pass
    if engine in {"claude", "codex"} and not existing:
        from ccx.system.engine_binary import resolve_engine_binary
        resolve_engine_binary(engine)
    # Public new/fork (CLI, Python and protocol) must validate the explicit
    # native model before minting an identity or writing any claim/record.
    if (engine == "codex" and options.get("model") is not None
            and options.get("_fork_anchor") is None):
        from ccx.codex.runtime import validate_model
        from ccx.system.cwd_paths import canonical_cwd
        validate_model(options["model"], cwd=canonical_cwd(options["cwd"]))
    # Only a CCX-owned full-UUID caller is a delegation edge. Native provider
    # session/thread environment is not parent authority, and malformed/stale
    # self stamps must never be promoted into one.
    from ccx.store.records import spawned_by_from_env
    try:
        caller = spawned_by_from_env()
    except SessionUnavailable:
        caller = None
    spawned_by = caller if caller and caller.get("kind") == "ccx-worker" else None
    chosen = supplied if supplied is not None else mint_worker_id()
    info = {
        "worker_id": chosen, "worker": f"{engine}:{chosen}",
        "engine": engine, "short": chosen[:8], "record": None,
        "created": False, "reconciled": False,
        "activation_state": "unproven", "native_ready": False,
    }
    # Only the recognized private frozen fork anchor is admitted.
    options.pop("timeout", None)  # Already validated; retained as turn_timeout for wait.
    options.pop("fork", None)     # Legacy fork was refused before the ACTIVE pin.
    options["fork_anchor"] = options.pop("_fork_anchor", None)
    options["worker_id"] = chosen
    options.setdefault("idle_timeout", DEFAULT_IDLE_TIMEOUT_S)
    try:
        original = prepare_worker(ctx, spawned_by=spawned_by, **options)
    except (CreationConflict, OwnerConflict) as exc:
        raise OperationConflict(
            f"Worker ID already belongs to another configuration; choose a new ID: {exc}",
            short=chosen[:8], reason="global_worker_uuid_conflict", worker_id=chosen,
        ) from exc
    except (GenerationError, IdentityError) as exc:
        raise CreatePartialFailure(
            "Worker setup is incomplete; check status before retrying; "
            "retain and reconcile exact worker_id",
            info=info, phase="record", cause=exc,
        ) from exc
    info.update(original.as_dict())
    info["record"] = original.record
    try:
        names = _bind_once(ctx, original.engine, original.worker_id, attachment)
    except OperationConflict:
        raise
    except Exception as exc:
        # The first record already belongs to this exact UUID. A failed
        # bind is not a native failure, and could be on an activated retry.
        activation_state = original.activation_state
        try:
            activation_state = ctx.native_activation(original.worker)["state"]
        except Exception:
            pass  # Never mask the original missing/unsafe binding proof.
        raise CreatePartialFailure(
            "Worker record exists but variables could not be attached; "
            "is not proven; retain and reconcile the same worker_id",
            info={**info, "activation_state": activation_state,
                  "cause_type": type(exc).__name__},
            phase="initial-variables", cause=exc,
        ) from exc
    try:
        ctx.revalidate()
        if deadline is not None and time.monotonic() >= deadline:
            raise NativeEffectIndeterminate("native creation request deadline expired; no replay")
        if engine == "codex":
            bound = _complete_codex_create(
                ctx, original, info, start_timeout=start_timeout, deadline=deadline,
            )
        else:
            bound = _complete_claude_create(
                ctx, original, start_timeout=start_timeout, deadline=deadline,
            )
        ctx.revalidate()
        if deadline is not None and time.monotonic() >= deadline:
            raise NativeEffectIndeterminate(
                "original native creation confirmed after request deadline; "
                "retain the exact full worker UUID"
            )
        result = {
            **original.as_dict(), "record": bound,
            "activation_state": "confirmed", "native_ready": True,
            "record_committed": True,
        }
    except Exception as exc:
        # Once the original owner exists, even a stale generation, corrupt
        # marker, or failed claim-CAS cannot lose its minted full UUID.
        # Preserve the exact cause: corruption is NOT retryable unknown.
        info["native_start_error"] = str(exc)
        info["cause_type"] = type(exc).__name__
        from ccx.store.root import StorageIntegrityError
        if isinstance(exc, StorageIntegrityError):
            info["integrity_error"] = True
        # Keep the offline native-start reconciliation for genuinely
        # indeterminate effects or transport failures, but never probe a
        # suspect stale/corrupt store merely to decorate an integrity error.
        if isinstance(exc, (NativeEffectIndeterminate, SessionUnavailable, OSError)):
            try:
                if engine == "codex" and "native_rejection" not in info:
                    rejected = ctx.reconcile_codex_native_rejection(original.worker)
                    if rejected is not None:
                        info["native_rejection"] = {
                            "code": rejected["code"], "message": rejected["message"],
                        }
                if engine == "codex" and "native_rejection" not in info:
                    local = ctx.reconcile_codex_local_refusal(original.worker)
                    if local is not None:
                        info["native_local_refusal"] = {
                            "reason": local["reason"], "stage": local["stage"],
                            "proven": True,
                        }
            except Exception:
                pass  # Optional terminal proof never replaces the cause.
        try:
            # Even after a stale/failed bind, the original immutable claim
            # can sometimes be read safely. An unavailable or suspect
            # store must never hide the original error or its UUID.
            state = ctx.native_activation(original.worker)["state"]
            if state in {"unprepared", "prepared", "submitted-unknown",
                         "confirmed", "rejected", "local-refused"}:
                info["activation_state"] = state
        except Exception:
            pass
        partial = CreatePartialFailure(
            "full worker UUID and original first record are durable, but creation "
            "did not complete cleanly; retain worker_id, do not remint",
            info=info, phase="native-activation",
        )
        partial.cause = exc  # Keep the original native-partial EXIT_UNAVAILABLE.
        raise partial from exc
    if prompt is None:
        if engine == "codex":
            _finalize_empty_codex_create(ctx, result)
        if parent is None:
            return result
        # No first model turn exists. Arm exactly one create-keyed stable
        # outcome after the original native binding is confirmed.
        try:
            # Codex may need an original-thread load to prove idle; a
            # confirmed Claude SessionStart already proves its stable
            # creation outcome without another host readiness action.
            if (engine == "codex" and
                    (result["record"].get("lifecycle") or {}).get("state") == "prepared"):
                from ccx.ops.send import ensure_ready
                ensure_ready(result["worker"], expected_generation=pinned_generation)
            from ccx.notify.registry import create_registration, public_registration
            from ccx.notify.daemon import ensure_notify_daemon
            registered = create_registration(
                parent=parent, worker_engine=engine, worker_id=result["worker_id"],
                worker_name=(result["record"] or {}).get("name"),
                operation={"kind": f"{engine}-stable", "expectTurn": False,
                           "selectionMode": "status-only", "stableState": "idle"},
                source="new", settle=settle,
            )
            ensure_notify_daemon()
            return {**result, "notification": public_registration(registered)}
        except Exception as exc:
            # The original full UUID and native identity were already
            # confirmed; failure to arm must never create a new worker.
            raise CreatePartialFailure(
                "worker created, but original promptless parent notification "
                "is not proven armed; retain the exact worker_id",
                info={**result, "cause_type": type(exc).__name__},
                phase="notification-registration", cause=exc,
            ) from exc
    # Native creation is always promptless. A first message uses precisely
    # the ordinary semantic receipt: the full UUID + create:initial fixes
    # one original prompt across process loss and all public adapters.
    from ccx.ops.send import send_turn
    from ccx.claude.delivery import ClaudeDeliveryConflict
    from ccx.codex.delivery import DeliveryReceiptConflict
    from ccx.store.root import NativeEffectIndeterminate
    initial = "create:initial"
    status = None
    try:
        delivered = send_turn(
            result["worker"], live_vars.with_variable_hint(prompt, names),
            delivery_id=initial,
            mode="new_turn_only", expected_generation=pinned_generation,
            notify_parent=parent is not None, settle=settle, cap=cap,
        )
        status = delivered.get("status")
        receipt = delivered.get("delivery")
        if status == "rejected" and isinstance(receipt, dict):
            from ccx.errors import DeliveryRejected
            raise DeliveryRejected(
                receipt.get("rejection_message") or "original initial prompt rejected",
                short=result["short"], refusal=receipt.get("rejection_code"),
                delivery_id=initial, delivery_status="rejected",
            )
        # The real Claude host ACKs durable ownership before evidence of
        # native turn placement; owned is PENDING, not accepted/completed.
        valid = (
            isinstance(receipt, dict)
            and receipt.get("delivery_id") == initial
            and receipt.get("route") == "new_turn"
            and receipt.get("owned") is True
            and (
                (engine == "claude" and status in {"owned", "accepted"}
                 and receipt.get("status") == (
                     "pending" if status == "owned" else "accepted"))
                or (engine == "codex" and status == "accepted"
                    and receipt.get("status") == "accepted"
                    and isinstance(receipt.get("turn_id"), str)
                    and bool(receipt["turn_id"]))
            )
        )
        if not valid:
            raise NativeEffectIndeterminate(
                "initial prompt has no positive original delivery receipt; "
                "reconcile create:initial, never send a new token"
            )
        ctx.revalidate()
        result = {**result, "delivery_id": initial,
                  "delivery_status": status, "delivery": receipt,
                  **({"notification": delivered["notification"]}
                     if parent is not None and delivered.get("notification") is not None
                     else {})}
    except Exception as exc:
        # Identity/activation were positively bound. A delivery failure
        # cannot roll them back or turn this into a fresh native creation.
        from ccx.errors import DeliveryRejected
        if (engine == "codex" and isinstance(exc, BudgetStopped)
                and exc.phase == "preflight"):
            # Original cap admission refused before any provider turn/start.
            # This is the same promptless bound thread, now ready for peek.
            _finalize_empty_codex_create(ctx, result, ("prepared", "idle"))
        conflict = isinstance(exc, (ClaudeDeliveryConflict, DeliveryReceiptConflict, OperationConflict))
        info = {**result, "delivery_id": initial,
                "delivery_status": status,
                "cause_type": type(exc).__name__,
                "initial_delivery_conflict": conflict,
                "initial_delivery_rejected": isinstance(exc, DeliveryRejected)}
        raise CreatePartialFailure(
            "worker created, but original initial prompt is not proven "
            "delivered; retain worker_id and reconcile create:initial",
            info=info, phase="initial-delivery", delivery_id=initial,
            delivery_status=status, cause=exc,
        ) from exc
    if not wait:
        return result
    from ccx.ops.wait import wait_delivery
    try:
        if deadline is not None:
            left = max(0.0, deadline - time.monotonic())
            turn_timeout = left if turn_timeout is None else min(turn_timeout, left)
        observed = wait_delivery(
            result["worker"], delivery_id=initial, timeout=turn_timeout,
            expected_generation=pinned_generation,
        )
        return {**result, **observed, "waited": True}
    except Exception as exc:
        info = {**result, "cause_type": type(exc).__name__,
                "turn_ref": getattr(exc, "turn_ref", None)}
        status = getattr(exc, "delivery_status", result["delivery_status"])
        info["delivery_status"] = status
        raise CreatePartialFailure(
            "original first turn did not complete with exact proof; "
            "retain worker_id and reconcile create:initial",
            info=info, phase="wait-report", delivery_id=initial,
            delivery_status=status, cause=exc,
        ) from exc
    raise RuntimeError("Worker startup is not confirmed; inspect worker status")
