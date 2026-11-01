"""Store-side creation steps: validate, prepare, activate, dispatch fence, reconcile.

Used by creation.create (also for forks) with caller-supplied native submissions.
Durable fences retain one owned full UUID and independent provider-native binding;
compatible exact-ID retries preserve the original effect without replay.
"""
from __future__ import annotations

import time
from pathlib import Path
from dataclasses import dataclass
from typing import Mapping

from ccx import constants
from ccx.errors import InvalidRequest
from ccx.store.records import sanitize_name
from ccx.system.cwd_paths import canonical_cwd
from ccx.store.root import CreationConflict, CreationIndeterminate
from ccx.store.store import GenerationStore, fingerprint_v2
from ccx.store.identity import canonical_worker_id, mint_worker_id


_ENGINES = {"claude", "codex"}


def _reject_model_as_agent(agent: str | None) -> None:
    """Reject a model alias used instead of a Claude subagent name."""
    if agent and agent.strip().lower() in constants.MODEL_ALIASES:
        raise InvalidRequest(
            f"agent {agent!r} is a model, not an agent — pass it as model={agent!r} "
            f"(CLI: `--model {agent}`). An agent names a subagent, and `claude` "
            f"would exit at boot with \"--agent '{agent}' not found\"."
        )


def validated_create_spec(*, engine: str, cwd: str, model: str | None, effort: str | None,
          idle_timeout: int, permissions: str | None, sandbox: str | None,
          approval_policy: str | None, agent: str | None,
          append_system_prompt: str | None, allowed_tools: str | None,
          disallowed_tools: str | None, add_dir: list[str] | None,
          permission_mode: str | None) -> tuple[str, dict]:
    if engine not in _ENGINES:
        raise InvalidRequest(f"unsupported engine: {engine!r}")
    directory = canonical_cwd(cwd)
    if not Path(directory).is_dir():
        raise InvalidRequest(f"cwd is not a directory: {directory}")
    if type(idle_timeout) is not int or idle_timeout < 0:
        raise InvalidRequest("idle_timeout must be a non-negative integer")
    if permissions is not None and permissions not in {"full", "restricted"}:
        raise InvalidRequest("permissions must be full or restricted")
    if engine == "claude":
        _reject_model_as_agent(agent)
        if sandbox is not None or approval_policy is not None:
            from ccx.errors import CapabilityUnsupported
            raise CapabilityUnsupported(
                "Claude does not accept Codex sandbox/approval policy",
                engine="claude", capability="create_sandbox_or_approval_policy",
            )
        args = {
            "model": model, "effort": effort, "agent": agent,
            "append_system_prompt": append_system_prompt,
            "allowed_tools": allowed_tools, "disallowed_tools": disallowed_tools,
            "add_dir": [canonical_cwd(d) for d in (add_dir or [])],
            "permission_mode": permission_mode,
            "skip_permissions": permissions != "restricted",
            "idle_timeout_s": idle_timeout,
        }
    else:
        if any(value is not None for value in (
            agent, append_system_prompt, allowed_tools, disallowed_tools,
            permission_mode, add_dir,
        )):
            raise InvalidRequest("Codex does not accept Claude launch options")
        if permissions is not None and (sandbox is not None or approval_policy is not None):
            raise InvalidRequest("permissions conflicts with sandbox/approval_policy")
        if permissions == "restricted":
            sandbox, approval_policy = "workspace-write", "on-request"
        elif permissions == "full":
            sandbox, approval_policy = "danger-full-access", "never"
        sandbox = sandbox or "danger-full-access"
        approval_policy = approval_policy or "never"
        if sandbox not in {"read-only", "workspace-write", "danger-full-access"}:
            raise InvalidRequest(f"unsupported Codex sandbox: {sandbox}")
        if approval_policy not in {"untrusted", "on-request", "never"}:
            raise InvalidRequest(f"unsupported Codex approval policy: {approval_policy}")
        args = {
            "model": model, "effort": effort, "idleTimeout": idle_timeout,
            "sandbox": sandbox, "approvalPolicy": approval_policy,
        }
    return directory, args


@dataclass(frozen=True)
class PreparedWorker:
    worker_id: str
    engine: str
    created: bool
    reconciled: bool
    activation_state: str
    native_source: str
    record: dict

    @property
    def worker(self) -> str:
        return f"{self.engine}:{self.worker_id}"

    @property
    def short(self) -> str:
        return self.worker_id[:8]

    def as_dict(self) -> dict:
        return {
            "worker": self.worker, "worker_id": self.worker_id,
            "engine": self.engine, "short": self.short,
            "created": self.created, "reconciled": self.reconciled,
            "record_committed": True, "activation_state": self.activation_state,
            "native_ready": self.activation_state == "confirmed",
            "native_source": self.native_source,
            "created_at": self.record["createdAt"],
        }


def prepare_worker(
    store: GenerationStore,
    *,
    engine: str, cwd: str, worker_id: str | None = None,
    name: str | None = None, model: str | None = None,
    effort: str | None = None, idle_timeout: int = 900,
    permissions: str | None = None, sandbox: str | None = None,
    approval_policy: str | None = None, agent: str | None = None,
    append_system_prompt: str | None = None,
    allowed_tools: str | None = None,
    disallowed_tools: str | None = None,
    add_dir: list[str] | None = None,
    permission_mode: str | None = None,
    created_via: str = "python-api",
    import_session_id: str | None = None,
    imported_from: Mapping | None = None,
    fork_anchor: Mapping | None = None,
    spawned_by: Mapping | None = None,
    _failpoint=None,
) -> PreparedWorker:
    """Durable immutable owner/claim/record, no native start or false success.

    The original first native Claude session UUID is minted independently
    of the CCX full worker ID and fsynced in the owner intent. Exact-ID
    recovery reuses it even after crash before claim/record publication.
    The record's actual timestamps and mutable fields are never overwritten
    by a retry. Unknown original native fate cannot imply new startup.
    """
    from ccx.errors import InvalidRequest
    from uuid import UUID
    importing = import_session_id is not None
    if importing != (imported_from is not None) or (importing and engine != "claude"):
        raise InvalidRequest("Import requires the session ID and its saved source details")
    if importing:
        try:
            if str(UUID(import_session_id)) != import_session_id:
                raise ValueError("noncanonical native session")
        except (ValueError, TypeError, AttributeError) as exc:
            raise InvalidRequest("Import requires a complete Claude session ID") from exc
        if imported_from.get("sessionId") != import_session_id:
            raise InvalidRequest("Claude import session ID does not match its source")
    origin = "ccx-import" if importing else "ccx-new"
    provenance = dict(imported_from) if importing else None
    if not isinstance(created_via, str) or not created_via or len(created_via) > 128:
        raise InvalidRequest("created_via must be bounded, nonempty provenance")
    if name is not None and (not isinstance(name, str) or not sanitize_name(name)):
        raise InvalidRequest("display name must be nonempty or absent")
    if not isinstance(cwd, str):
        raise InvalidRequest("create cwd must be a path")
    directory, args = validated_create_spec(
        engine=engine, cwd=cwd, model=model, effort=effort,
        idle_timeout=idle_timeout, permissions=permissions,
        sandbox=sandbox, approval_policy=approval_policy,
        agent=agent, append_system_prompt=append_system_prompt,
        allowed_tools=allowed_tools, disallowed_tools=disallowed_tools,
        add_dir=add_dir, permission_mode=permission_mode,
    )
    chosen = canonical_worker_id(worker_id) if worker_id is not None else mint_worker_id()
    anchor = dict(fork_anchor) if isinstance(fork_anchor, Mapping) else None
    if fork_anchor is not None and engine == "codex":
        # Freeze the exact native source/terminal turn, whether its source is
        # a CCX worker or an external native Codex thread.
        if importing or anchor is None:
            raise CreationIndeterminate("Codex fork point is not confirmed; inspect source worker")
        from ccx.store.store import codex_fork_lineage
        codex_fork_lineage(anchor, child_id=chosen)
        seed = anchor.get("seed")
        seed_stats = seed.get("stats") if isinstance(seed, dict) else None
        turns = seed_stats.get("turns") if isinstance(seed_stats, dict) else None
        last_at = seed.get("lastTurnAt") if isinstance(seed, dict) else None
        last = seed.get("lastTurn") if isinstance(seed, dict) else None
        overrides = anchor.get("overrides")
        if (anchor.get("engine") != "codex"
                or any(not isinstance(anchor.get(k), str)
                       or not 0 < len(anchor[k]) <= 256 or "\x00" in anchor[k]
                       for k in ("sourceNativeThreadId", "lastTurnId"))
                or anchor.get("cwd") != directory or anchor.get("createArgs") != args
                or type(anchor.get("forkedAtMs")) is not int or anchor["forkedAtMs"] <= 0
                or not isinstance(seed, dict)
                or set(seed) not in ({"stats", "lastTurnAt"},
                                     {"stats", "lastTurnAt", "lastTurn"})
                or not isinstance(seed_stats, dict) or set(seed_stats) != {"turns"}
                or type(turns) is not int or not 0 <= turns <= 1_000_000_000
                or (last_at is not None and (type(last_at) is not int or last_at < 0))
                or ("lastTurn" in seed and (not isinstance(last, dict)
                    or last.get("turnId") != anchor["lastTurnId"]))
                or not isinstance(overrides, dict)
                or set(overrides) != {"model", "effort"}
                or any(value is not None and (not isinstance(value, str)
                                               or not value.strip())
                       for value in overrides.values())
                or any(value is not None and args.get(key) != value
                       for key, value in overrides.items())):
            raise CreationIndeterminate("Codex fork settings changed; inspect source worker")
    if fork_anchor is not None and engine != "codex":
        # A Claude fork requires a complete frozen source point, never an
        # arbitrary unverified native UUID or a caller's changed launch spec.
        from ccx.store.identity import InvalidTarget
        if engine != "claude" or importing or anchor is None:
            raise CreationIndeterminate("Claude fork point is not confirmed; inspect source worker")
        try:
            source_id = canonical_worker_id(anchor.get("sourceWorkerId"))
            source_sid = anchor.get("sourceNativeSessionId")
            # Native Claude SIDs (including exact imported sessions) need a
            # canonical UUID, not CCX's stricter worker-ID UUIDv4 restriction.
            if not isinstance(source_sid, str) or str(UUID(source_sid)) != source_sid:
                raise ValueError("noncanonical native SID")
        except (InvalidTarget, ValueError, TypeError, AttributeError) as exc:
            raise CreationIndeterminate("native Claude fork point has invalid source identity") from exc
        seed = anchor.get("seed")
        overrides = anchor.get("overrides")
        seed_stats = seed.get("stats") if isinstance(seed, dict) else None
        turns = seed_stats.get("turns") if isinstance(seed_stats, dict) else None
        last_at = seed.get("lastTurnAt") if isinstance(seed, dict) else None
        if (set(anchor) != {"engine", "sourceWorkerId", "sourceNativeSessionId",
                            "resumeAt", "transcript", "cwd", "createArgs", "seed",
                            "overrides", "forkedAtMs"}
                or anchor.get("engine") != "claude"
                or source_id in {chosen, source_sid} or source_sid == chosen
                or anchor.get("cwd") != directory or anchor.get("createArgs") != args
                or not isinstance(anchor.get("resumeAt"), str)
                or not 0 < len(anchor["resumeAt"]) <= 256
                or not isinstance(anchor.get("transcript"), str)
                or not anchor["transcript"].startswith("/")
                or type(anchor.get("forkedAtMs")) is not int or anchor["forkedAtMs"] <= 0
                or not isinstance(seed, dict) or set(seed) != {"stats", "lastTurnAt"}
                or not isinstance(seed.get("stats"), dict)
                or set(seed["stats"]) != {"turns"}
                or type(turns) is not int or not 0 <= turns <= 1_000_000_000
                or (last_at is not None and (type(last_at) is not int or last_at < 0))
                or not isinstance(overrides, dict)
                or set(overrides) != {"model", "effort"}
                or any(value is not None and (not isinstance(value, str) or not value.strip())
                       for value in overrides.values())
                or any(value is not None and args.get(key) != value
                       for key, value in overrides.items())):
            raise CreationIndeterminate("Claude fork settings changed; inspect source worker")
    digest = fingerprint_v2(
        engine=engine, cwd=directory, create_args=args, fork_anchor=anchor,
    )
    original = store.creation_binding(
        chosen, engine=engine, fingerprint=digest,
        provenance=origin, imported_from=provenance)
    native = (original if original is not None else
              {"sessionId": import_session_id or mint_worker_id()}
              if engine == "claude" else {})
    if engine == "claude":
        session_id = native.get("sessionId")
        if importing:
            if session_id != import_session_id:
                raise CreationConflict("import retry changed original native SID")
        else:
            canonical_worker_id(session_id)
            if anchor is not None and session_id in {
                    chosen, anchor["sourceWorkerId"], anchor["sourceNativeSessionId"]}:
                raise CreationIndeterminate("fork child native SID must be independent")
        native_source = f"claude-session:{session_id}"
    else:
        if native:
            raise CreationIndeterminate("Codex native thread bound before first activation")
        native_source = f"ccx-worker:{chosen}"
    try:
        if importing:
            reservation = store.reserve_import_creation(
                chosen, session_id=import_session_id, fingerprint=digest,
                native_binding=native, imported_from=provenance)
        else:
            reservation = store.reserve_creation(
                chosen, engine=engine, fingerprint=digest, native_binding=native,
                fork_anchor=anchor, spawned_by=spawned_by, _failpoint=_failpoint,
            )
    except CreationConflict:
        if importing:
            raise  # No replacement CCX worker/native ID after an import conflict.
        # Two independent processes can race on the same caller-provided
        # worker UUID before either sees the first durable owner intent.
        # Claude would otherwise mint TWO distinct native sessionIds. The
        # winner's immutable owner intent is authoritative: reread it under
        # ACTIVE, prove engine+original spec, and retry only that exact
        # native binding. Never mint a substitute CCX UUID.
        previous = store.creation_binding(
            chosen, engine=engine, fingerprint=digest,
            provenance=origin, imported_from=provenance,
        )
        if previous is None or previous == native:
            raise
        native = previous
        if engine == "claude":
            session_id = native.get("sessionId")
            canonical_worker_id(session_id)
            if anchor is not None and session_id in {
                    chosen, anchor["sourceWorkerId"], anchor["sourceNativeSessionId"]}:
                raise CreationIndeterminate("fork retry native SID differs from frozen source")
            native_source = f"claude-session:{session_id}"
        elif native:
            raise CreationIndeterminate("Codex original native identity unexpectedly prebound")
        reservation = store.reserve_creation(
            chosen, engine=engine, fingerprint=digest, native_binding=native,
            fork_anchor=anchor, spawned_by=spawned_by, _failpoint=_failpoint,
        )
    if reservation.activation_state == "unprepared":
        # A visible first record after a crash is NOT necessarily an active
        # owner yet; reconcile immutable bytes and promote the reservation
        # even if reserve_creation reported record_committed=True.
        initial_record_exists = store.recover_first_record(reservation)
    else:
        # After original prepared/submitted/confirmed native intent, NO
        # replay/rebuild of first-record contents is safe. Only a previously
        # promoted complete worker may be read back.
        if not reservation.record_committed:
            raise CreationIndeterminate(
                "native activation intent exists without a complete first record"
            )
        initial_record_exists = True
    if initial_record_exists:
        record = store.record(f"{engine}:{chosen}")
    else:
        ts = int(time.time() * 1000)
        actual_name = sanitize_name(name) or (
            ("ccx-" if engine == "claude" else "codex-") + chosen[:8]
        )
        record = {
            "schemaVersion": 2, "workerId": chosen, "engine": engine,
            "short": chosen[:8], "name": actual_name,
            "cwd": directory, "createdAt": ts,
            "lastTurnAt": anchor["seed"]["lastTurnAt"] if anchor else None,
            "createdVia": created_via,
            "spawnedBy": reservation.spawned_by,
            **({"origin": "ccx-import", "importedFrom": provenance} if importing else {}),
            "createSha256": digest, "fingerprintVersion": 2,
            "forkAnchor": anchor, "createArgs": args,
            **({"forkedFrom": (
                codex_fork_lineage(anchor, child_id=chosen) if engine == "codex"
                else {key: anchor[key] for key in (
                    "engine", "sourceWorkerId", "sourceNativeSessionId",
                    "resumeAt", "forkedAtMs")}
            )} if anchor else {}),
            **({"lastTurn": dict(anchor["seed"]["lastTurn"])}
               if anchor and engine == "codex" and "lastTurn" in anchor["seed"] else {}),
            "native": {
                **native, "nativeState": (
                    "import-resume-pending" if importing else "not-started-proven"),
                **({"threadId": None} if engine == "codex" else {}),
            },
            "terminalOnly": False,
            "lifecycle": {
                "state": "prepared", "since": ts,
                "activeTurnId": None, "hostPid": None,
                "workerPid": None, "lastError": None,
            },
            "stats": {
                "turns": anchor["seed"]["stats"]["turns"] if anchor else 0,
            },
            "recordRevision": 0,
        }
        if engine == "claude":
            from ccx.store.paths import transcript_path_for
            record["transcript"] = str(transcript_path_for(directory, session_id))
            record["transcriptJson"] = str(
                store.pointer.generation_root / "workers" /
                "claude" / chosen / "transcript.json"
            )
        else:
            record.update(
                nativeArchive={"state": "active", "attempts": 0},
                pendingApprovals=[], lastReport=None, context=None,
                activeCompact=None, lastCompact=None,
            )
        store.publish_first_record(reservation, record, _failpoint=_failpoint)
    latest = store.reserve_creation(
        chosen, engine=engine, fingerprint=digest, native_binding=native,
        fork_anchor=anchor, provenance=origin, imported_from=provenance,
    )
    return PreparedWorker(
        worker_id=chosen, engine=engine, created=not reservation.reconciled,
        reconciled=reservation.reconciled,
        activation_state=latest.activation_state,
        native_source=native_source,
        record=record,
    )



def activate_prepared_worker(
    store: GenerationStore, prepared: PreparedWorker, *,
    bounded_native_submit,
    dispatch_budget_s: float = 1.0,
    _failpoint=None,
) -> PreparedWorker:
    """Run one native *submission* through reviewed test/injected adapter.

    This function is INTERNAL: the actual host/codexd adapter still must
    verify SO_PEERCRED, PID/start, owner, native source and real timeout before
    production wiring. A caller-controlled result or hash alone is NOT
    sufficient; the injected low-level transport must return its original
    source/receipt, and unknown native outcomes cannot authorize a retry.
    Tests use fake native adapters only. No default provider is imported.
    """
    from ccx.store.root import CreationIndeterminate, NativeEffectIndeterminate
    if not callable(bounded_native_submit):
        raise CreationIndeterminate("Engine is unavailable; check its executable and retry")
    reservation = store.reserve_creation(
        prepared.worker_id, engine=prepared.engine,
        fingerprint=prepared.record["createSha256"],
        native_binding=(
            {"sessionId": prepared.record["native"]["sessionId"]}
            if prepared.engine == "claude" else {}
        ),
        provenance=("ccx-import" if prepared.record.get("origin") == "ccx-import"
                    else "ccx-new"),
        imported_from=prepared.record.get("importedFrom"),
        fork_anchor=prepared.record.get("forkAnchor"),
    )
    if reservation.activation_state == "confirmed":
        record = store.bind_confirmed_native(prepared.worker)
        return PreparedWorker(
            prepared.worker_id, prepared.engine, False, True, "confirmed",
            prepared.native_source, record,
        )
    if reservation.activation_state == "submitted-unknown":
        raise NativeEffectIndeterminate(
            "Worker startup was submitted but not confirmed; "
            "check the existing worker instead of creating it twice"
        )
    original = store.prepare_native_activation(
        reservation, native_source=prepared.native_source,
    )
    receipt = store.native_activation_dispatch(
        original, bounded_submit=bounded_native_submit,
        dispatch_budget_s=dispatch_budget_s, _failpoint=_failpoint,
    )
    if (receipt.get("accepted") is not True
            or receipt.get("workerId") != prepared.worker_id
            or receipt.get("engine") != prepared.engine
            or receipt.get("source") != original.source
            or receipt.get("submissionId") != original.submission_id
            or not isinstance(receipt.get("nativeEvidenceSha256"), str)):
        raise NativeEffectIndeterminate(
            "Engine did not confirm startup; "
            "check this worker before retrying"
        )
    binding = receipt.get("nativeBinding")
    if not isinstance(binding, dict):
        raise NativeEffectIndeterminate(
            "Engine start response does not match this worker; inspect status"
        )
    # The native adapter must independently attest the source and original
    # immutable client submission ID from the provider's actual event/receipt.
    store.confirm_native_activation(
        original, native_evidence_sha256=receipt["nativeEvidenceSha256"],
        native_binding=binding,
    )
    updated = store.bind_confirmed_native(prepared.worker)
    return PreparedWorker(
        prepared.worker_id, prepared.engine, False, True, "confirmed",
        prepared.native_source, updated,
    )



def dispatch_prepared_codex_native(
    store: GenerationStore, prepared: PreparedWorker, *,
    bounded_native_submit, dispatch_budget_s: float = 2.0,
) -> PreparedWorker:
    """Admit ONE original Codex thread/start; do not await native completion.

    A positive daemon admission is not proof of a provider thread. Retain the
    original submitted-unknown creation fence until the daemon's *observe-only*
    candidate/thread-read evidence is verified by reconcile_submitted_native.
    This is the existing global creation/activation store, not a second owner.
    """
    from ccx.store.root import CreationIndeterminate, NativeEffectIndeterminate
    if prepared.engine != "codex" or not callable(bounded_native_submit):
        raise CreationIndeterminate("Codex startup has not been submitted; retry creation")
    reservation = store.reserve_creation(
        prepared.worker_id, engine="codex",
        fingerprint=prepared.record["createSha256"], native_binding={},
        fork_anchor=prepared.record.get("forkAnchor"),
    )
    if reservation.activation_state == "confirmed":
        bound = store.bind_confirmed_native(prepared.worker)
        return PreparedWorker(prepared.worker_id, "codex", False, True,
                              "confirmed", prepared.native_source, bound)
    if reservation.activation_state == "submitted-unknown":
        # Retrying this path never calls the provider. The caller must observe.
        return PreparedWorker(prepared.worker_id, "codex", False, True,
                              "submitted-unknown", prepared.native_source,
                              store.record(prepared.worker))
    original = store.prepare_native_activation(
        reservation, native_source=prepared.native_source,
    )
    receipt = store.native_activation_dispatch(
        original, bounded_submit=bounded_native_submit,
        dispatch_budget_s=dispatch_budget_s,
    )
    if (receipt.get("ok") is not True or receipt.get("queued") is not True
            or receipt.get("workerId") != prepared.worker_id
            or receipt.get("engine") != "codex"
            or receipt.get("source") != original.source
            or receipt.get("submissionId") != original.submission_id):
        # Preserve daemon's actual refusal reason even when marker fsync or
        # claim CAS failed. The message is a PROVISIONAL diagnostic, never
        # proof that missing evidence allows re-dispatch of this UUID.
        reason = receipt.get("error")
        if not isinstance(reason, str) or not reason:
            reason = "admission unproven"
        raise NativeEffectIndeterminate(
            "Codex startup was refused or not confirmed; check its worker: " + reason[:450]
            + "; submitted-unknown fence retained unless original proof seals"
        )
    return PreparedWorker(prepared.worker_id, "codex", False, True,
                          "submitted-unknown", prepared.native_source,
                          store.record(prepared.worker))


def reconcile_submitted_native(
    store: GenerationStore, prepared: PreparedWorker, *,
    original_receipt: Mapping,
) -> PreparedWorker:
    """Forensic-only reconcile one original submitted native effect.

    Caller must obtain the original positive native source/receipt from a
    reviewed provider-evidence reader; a missing response must NOT be replayed.
    The durable existing submissionId anchors the evidence so another
    thread/session ID cannot be installed opportunistically.
    """
    from ccx.store.root import CreationIndeterminate, NativeEffectIndeterminate
    from ccx.store.store import NativeActivation
    if not isinstance(original_receipt, Mapping):
        raise NativeEffectIndeterminate("No engine response was recorded; inspect worker status")
    activation = store.native_activation(prepared.worker)
    if activation.get("state") not in ("submitted-unknown", "confirmed"):
        raise CreationIndeterminate("Engine start was not submitted; retry creation")
    if (original_receipt.get("accepted") is not True
            or original_receipt.get("workerId") != prepared.worker_id
            or original_receipt.get("engine") != prepared.engine
            or original_receipt.get("source") != prepared.native_source
            or original_receipt.get("submissionId") != activation.get("submissionId")
            or not isinstance(original_receipt.get("nativeBinding"), dict)
            or not isinstance(original_receipt.get("nativeEvidenceSha256"), str)):
        raise NativeEffectIndeterminate(
            "Engine result does not match this worker; inspect status"
        )
    original = NativeActivation(
        prepared.worker_id, prepared.engine, prepared.native_source,
        activation["submissionId"], "submitted-unknown", store.generation,
    )
    store.confirm_native_activation(
        original,
        native_evidence_sha256=original_receipt["nativeEvidenceSha256"],
        native_binding=original_receipt["nativeBinding"],
    )
    bound = store.bind_confirmed_native(prepared.worker)
    return PreparedWorker(
        prepared.worker_id, prepared.engine, False, True, "confirmed",
        prepared.native_source, bound,
    )
