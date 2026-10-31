"""Durable full-UUID first-create path and crash-safe native preparation tests."""
from __future__ import annotations
import json
import os
from pathlib import Path
import pytest
from test_generation_store import A,B,C,G1,fixture_root,put,switch_pointer
from ccx.store.identity import OwnerConflict, canonical_worker_id
from ccx.ops.creation_steps import prepare_worker
from ccx.store.root import CreationConflict, CreationIndeterminate, NativeActionDenied, NativeEffectIndeterminate, StaleGeneration, require_ccx_active


def test_create_spec_case_insensitive_apfs_alias_preserves_one_canonical_original(tmp_path, monkeypatch):
    """Provider-free APFS fake: both spellings address one existing directory.

    Native M1 scenario 30 proved that a valid mixed-case spelling can persist
    lowercase despite the physical original directory spelling being MixedCase.
    This fake isolates create-spec canonicalization without running Claude.
    """
    from ccx.ops import creation_steps
    from ccx.system import cwd_paths
    from ccx.system import oslayer
    from ccx.claude import settings
    from types import SimpleNamespace

    original = tmp_path / "MixedCase"
    original.mkdir()
    alias = tmp_path / "mixedcase"
    # Model a case-insensitive APFS kernel on Linux: Path.resolve still returns
    # the requested lowercase spelling, while open() reaches the same inode
    # and fcntl(F_GETPATH) reports the physical mixed-case spelling.
    real_open = os.open
    opened = []
    def fake_open(path, flags):
        opened.append((str(path), flags))
        return real_open(original if str(path) == str(alias) else path, flags)

    def fake_getpath(fd, command, buffer):
        assert command == 50 and len(buffer) == 1024
        assert os.fstat(fd).st_ino == original.stat().st_ino
        return os.fsencode(original).ljust(1024, b"\0")

    monkeypatch.setattr(oslayer, "physical_directory_spelling",
                        oslayer._darwin_physical_directory_spelling)
    monkeypatch.setattr(oslayer, "os", SimpleNamespace(
        open=fake_open, close=os.close, fsdecode=os.fsdecode,
        O_RDONLY=os.O_RDONLY, O_DIRECTORY=os.O_DIRECTORY, O_CLOEXEC=os.O_CLOEXEC,
    ))
    monkeypatch.setattr(oslayer, "fcntl", SimpleNamespace(F_GETPATH=50, fcntl=fake_getpath))
    def spec(cwd):
        return creation_steps.validated_create_spec(
            engine="claude", cwd=str(cwd), model="haiku", effort=None,
            idle_timeout=0, permissions="full", sandbox=None,
            approval_policy=None, agent=None, append_system_prompt=None,
            allowed_tools=None, disallowed_tools=None, add_dir=None,
            permission_mode=None,
        )[0]

    assert spec(original) == str(original)
    assert spec(alias) == str(original)  # one native transcript namespace
    assert settings._requested_trust_path(str(alias)) == str(original)
    assert opened and all(not flags & os.O_NOFOLLOW for _, flags in opened)

def test_create_same_prefix_two_engines_without_native_calls(tmp_path,monkeypatch):
    root=fixture_root(tmp_path)
    ctx=require_ccx_active(root)
    from ccx.claude import spawn
    calls=[]
    monkeypatch.setattr(spawn,"spawn_host_detached",lambda *a,**k:calls.append("claude"))
    claude=prepare_worker(ctx,engine="claude",cwd=str(tmp_path),worker_id=A,name="same")
    codex=prepare_worker(ctx,engine="codex",cwd=str(tmp_path),worker_id=B,name="same")
    assert claude.worker==f"claude:{A}" and codex.worker==f"codex:{B}"
    assert claude.short==codex.short=="aaaaaaaa"
    assert claude.activation_state==codex.activation_state=="unprepared"
    assert not claude.as_dict()["native_ready"] and not codex.as_dict()["native_ready"]
    provider=claude.record["native"]["sessionId"]
    canonical_worker_id(provider)
    assert provider!=A and provider!=B
    assert claude.native_source=="claude-session:"+provider
    assert codex.record["native"]["threadId"] is None
    assert codex.native_source==f"ccx-worker:{B}"
    assert ctx.resolve(A).worker_id==A and ctx.resolve(B).worker_id==B
    assert calls==[]
    for wid in (A,B):
        with pytest.raises(NativeActionDenied):
            ctx.authorize_native_action(wid,"send")
        with pytest.raises(NativeActionDenied):
            ctx.begin_native_intent(wid,action="send",family="answer",
                                    operation_id="same",intent_sha256="a"*64)
    assert not (root/"sessions").exists()
    assert not (root/"codex").exists()
    assert not (root/"run").exists()

@pytest.mark.parametrize("at",["after-owner","after-claim","after-record"])
def test_create_retries_recover_native_session_and_original_record(tmp_path,at):
    root=fixture_root(tmp_path);ctx=require_ccx_active(root)
    def crash(where):
        if where==at:raise RuntimeError("power loss")
    with pytest.raises(RuntimeError):
        prepare_worker(ctx,engine="claude",cwd=str(tmp_path),worker_id=A,
                       model="sonnet",name="first display",_failpoint=crash)
    g=root/"identity"/"generations"/G1
    owner=json.loads((g/"owners"/f"{A}.json").read_text())
    sid=owner["createIntent"]["nativeBinding"]["sessionId"]
    assert canonical_worker_id(sid)==sid and sid!=A
    before=ctx._record("claude",A)
    recovered=prepare_worker(ctx,engine="claude",cwd=str(tmp_path),
                              worker_id=A,model="sonnet",name="different display")
    assert recovered.reconciled and not recovered.created
    assert recovered.record["native"]["sessionId"]==sid
    assert recovered.activation_state=="unprepared"
    if at=="after-record":
        assert before is not None
        assert recovered.record==before
        assert recovered.record["name"]=="first display"
    assert ctx.resolve(A).worker_id==A
    assert ctx.native_activation(A)["state"]=="unprepared"
    assert len(list((g/"owners").glob("*.json")))==1

def test_generated_original_uuid_after_owner_crash_is_not_reminted(tmp_path):
    root=fixture_root(tmp_path);ctx=require_ccx_active(root)
    with pytest.raises(RuntimeError):
        prepare_worker(ctx,engine="claude",cwd=str(tmp_path),
            _failpoint=lambda at: (_ for _ in ()).throw(RuntimeError("crash"))
            if at=="after-owner" else None)
    owner_path,=list((root/"identity"/"generations"/G1/"owners").glob("*.json"))
    wid=canonical_worker_id(owner_path.stem)
    sid=json.loads(owner_path.read_text())["createIntent"]["nativeBinding"]["sessionId"]
    recovered=prepare_worker(ctx,engine="claude",cwd=str(tmp_path),worker_id=wid)
    assert recovered.worker_id==wid and recovered.record["native"]["sessionId"]==sid
    assert len(list(owner_path.parent.glob("*.json")))==1

def test_same_spec_preserves_mutated_name_and_rejects_other_spec(tmp_path):
    root=fixture_root(tmp_path);ctx=require_ccx_active(root)
    first=prepare_worker(ctx,engine="claude",cwd=str(tmp_path),worker_id=A,model="sonnet")
    ctx.rename_worker(A,engine="claude",name="edited")
    mutated=ctx.record(A)
    again=prepare_worker(ctx,engine="claude",cwd=str(tmp_path),worker_id=A,
                         model="sonnet",name="retry title")
    assert again.record==mutated
    assert again.record["createdAt"]==first.record["createdAt"]
    for options in (
        {"engine":"codex","model":"sonnet"},
        {"engine":"claude","model":"different"},
        {"engine":"claude","model":"sonnet","effort":"high"},
    ):
        with pytest.raises((CreationConflict,CreationIndeterminate,OwnerConflict)):
            prepare_worker(ctx,cwd=str(tmp_path),worker_id=A,**options)
    assert ctx.record(A)==mutated

def test_crash_first_record_corruption_blocks_promotion(tmp_path):
    root=fixture_root(tmp_path);ctx=require_ccx_active(root)
    with pytest.raises(RuntimeError):
        prepare_worker(ctx,engine="codex",cwd=str(tmp_path),worker_id=A,
           _failpoint=lambda at: (_ for _ in ()).throw(RuntimeError("crash"))
           if at=="after-record" else None)
    path=root/"identity"/"generations"/G1/"workers"/"codex"/A/"record.json"
    record=json.loads(path.read_text());record["createArgs"]["sandbox"]="forged"
    put(path,record)
    with pytest.raises(CreationIndeterminate,match="immutable"):
        prepare_worker(ctx,engine="codex",cwd=str(tmp_path),worker_id=A)
    with pytest.raises(Exception):
        ctx.resolve(A)

def test_prepared_codex_native_start_is_durable_and_one_shot(tmp_path):
    root=fixture_root(tmp_path);ctx=require_ccx_active(root)
    worker=prepare_worker(ctx,engine="codex",cwd=str(tmp_path),worker_id=A)
    res=ctx.reserve_creation(A,engine="codex",fingerprint=worker.record["createSha256"])
    start=ctx.prepare_native_activation(res,native_source=f"ccx-worker:{A}")
    calls=[]
    def fake_native(deadline):
        assert ctx.native_activation(A)["state"]=="submitted-unknown"
        calls.append("thread/start")
        return {"threadId":"native-thread-x","source":f"ccx-worker:{A}",
                "submissionId":start.submission_id,"evidenceSha256":"b"*64}
    result=ctx.native_activation_dispatch(start,bounded_submit=fake_native)
    assert result["threadId"]=="native-thread-x"
    assert calls==["thread/start"]
    with pytest.raises(CreationIndeterminate):
        ctx.native_activation_dispatch(start,bounded_submit=fake_native)
    with pytest.raises(NativeActionDenied):
        ctx.authorize_native_action(A,"send")
    assert calls==["thread/start"]

def test_unproven_native_fork_point_cannot_reserve_new_owner(tmp_path):
    root=fixture_root(tmp_path);ctx=require_ccx_active(root)
    prepare_worker(ctx,engine="claude",cwd=str(tmp_path),worker_id=A)
    with pytest.raises(CreationIndeterminate,match="fork point"):
        prepare_worker(ctx,engine="claude",cwd=str(tmp_path),worker_id=B,
                       fork_anchor={"sourceWorkerId":A})
    assert not (root/"identity"/"generations"/G1/"owners"/f"{B}.json").exists()



@pytest.mark.parametrize("engine,wid", [("claude",A),("codex",B)])
def test_fake_native_activation_confirms_original_once_and_binds_provider(tmp_path,engine,wid):
    from ccx.ops.creation_steps import activate_prepared_worker
    root=fixture_root(tmp_path);ctx=require_ccx_active(root)
    prepared=prepare_worker(ctx,engine=engine,cwd=str(tmp_path),worker_id=wid)
    calls=[]
    def fake_adapter(timeout):
        assert 0<timeout<=2
        original=ctx.native_activation(wid)
        assert original["state"]=="submitted-unknown"
        assert original["source"]==prepared.native_source
        calls.append(original["submissionId"])
        native=({"sessionId":prepared.record["native"]["sessionId"]}
                if engine=="claude" else {"threadId":"thread-immutable-123"})
        return {
            "accepted":True,"workerId":wid,"engine":engine,
            "source":prepared.native_source,"submissionId":original["submissionId"],
            "nativeEvidenceSha256":"c"*64,"nativeBinding":native,
        }
    confirmed=activate_prepared_worker(
        ctx,prepared,bounded_native_submit=fake_adapter,
    )
    assert confirmed.worker_id==wid and confirmed.activation_state=="confirmed"
    assert confirmed.as_dict()["native_ready"] is True
    assert len(calls)==1
    assert ctx.native_activation(wid)["state"]=="confirmed"
    assert ctx.record(wid)["native"]["nativeState"] in ("session-bound","thread-bound")
    if engine=="codex":
        assert ctx.record(wid)["native"]["threadId"]=="thread-immutable-123"
        assert ctx.record(wid)["native"]["threadSource"]==f"ccx-worker:{wid}"
    else:
        assert ctx.record(wid)["native"]["sessionId"]==prepared.record["native"]["sessionId"]
    assert ctx.authorize_native_action(wid,"send").worker_id==wid
    # Positive original claim is idempotently returned WITHOUT a second call.
    again=activate_prepared_worker(ctx,prepared,bounded_native_submit=fake_adapter)
    assert again.record==confirmed.record
    assert len(calls)==1
    assert not (root/"sessions").exists() and not (root/"run").exists()


def test_fake_native_lost_response_blocks_retry_but_original_receipt_can_reconcile(tmp_path):
    from ccx.ops.creation_steps import (
        activate_prepared_worker,reconcile_submitted_native,
    )
    root=fixture_root(tmp_path);ctx=require_ccx_active(root)
    prepared=prepare_worker(ctx,engine="codex",cwd=str(tmp_path),worker_id=A)
    calls=[]
    def lost(_budget):
        calls.append("native accepted but reply lost")
        raise TimeoutError("provider response lost")
    with pytest.raises(NativeEffectIndeterminate):
        activate_prepared_worker(ctx,prepared,bounded_native_submit=lost)
    assert calls==["native accepted but reply lost"]
    original=ctx.native_activation(A)
    assert original["state"]=="submitted-unknown"
    with pytest.raises(NativeEffectIndeterminate):
        activate_prepared_worker(ctx,prepared,bounded_native_submit=lost)
    assert len(calls)==1
    fake_proof={
        "accepted":True,"workerId":A,"engine":"codex",
        "source":prepared.native_source,
        "submissionId":original["submissionId"],
        "nativeEvidenceSha256":"b"*64,
        "nativeBinding":{"threadId":"original-provider-thread"},
    }
    for changes in ({"workerId":B},{"submissionId":"stolen"},
                    {"source":f"ccx-worker:{B}"},
                    {"accepted":False}):
        forged={**fake_proof,**changes}
        with pytest.raises(NativeEffectIndeterminate):
            reconcile_submitted_native(ctx,prepared,original_receipt=forged)
        assert ctx.native_activation(A)["state"]=="submitted-unknown"
    recovered=reconcile_submitted_native(
        ctx,prepared,original_receipt=fake_proof,
    )
    assert recovered.activation_state=="confirmed"
    assert ctx.record(A)["native"]["threadId"]=="original-provider-thread"
    assert ctx.record(A)["native"]["threadSource"]==f"ccx-worker:{A}"
    same=reconcile_submitted_native(ctx,prepared,original_receipt=fake_proof)
    assert same.record==recovered.record
    done=activate_prepared_worker(ctx,prepared,bounded_native_submit=lost)
    assert done.activation_state=="confirmed"
    assert calls==["native accepted but reply lost"]


def test_fake_native_result_wrong_source_is_durable_unknown_no_send_retry(tmp_path):
    from ccx.ops.creation_steps import activate_prepared_worker
    root=fixture_root(tmp_path);ctx=require_ccx_active(root)
    prepared=prepare_worker(ctx,engine="claude",cwd=str(tmp_path),worker_id=A)
    calls=[]
    def wrong_source(_budget):
        calls.append("called once")
        original=ctx.native_activation(A)
        return {
            "accepted":True,"workerId":A,"engine":"claude",
            "source":"claude-session:"+B,
            "submissionId":original["submissionId"],
            "nativeBinding":{"sessionId":B},
            "nativeEvidenceSha256":"e"*64,
        }
    with pytest.raises(NativeEffectIndeterminate,match="did not confirm startup"):
        activate_prepared_worker(ctx,prepared,bounded_native_submit=wrong_source)
    assert ctx.native_activation(A)["state"]=="submitted-unknown"
    with pytest.raises(NativeEffectIndeterminate):
        activate_prepared_worker(ctx,prepared,bounded_native_submit=wrong_source)
    assert calls==["called once"]


def test_fake_native_confirm_crash_before_record_bind_uses_original_receipt(tmp_path):
    from ccx.ops.creation_steps import reconcile_submitted_native
    root=fixture_root(tmp_path);ctx=require_ccx_active(root)
    prepared=prepare_worker(ctx,engine="codex",cwd=str(tmp_path),worker_id=A)
    res=ctx.reserve_creation(A,engine="codex",fingerprint=prepared.record["createSha256"])
    start=ctx.prepare_native_activation(res,native_source=prepared.native_source)
    def acknowledge(_budget):
        return {"transport":"accepted"}
    ctx.native_activation_dispatch(start,bounded_submit=acknowledge)
    evidence={"threadId":"native-thread-once"}
    ctx.confirm_native_activation(
        start,native_evidence_sha256="a"*64,native_binding=evidence,
    )
    assert ctx.record(A)["native"]["threadId"] is None
    original=ctx.native_activation(A)
    proof={
        "accepted":True,"workerId":A,"engine":"codex",
        "source":prepared.native_source,"submissionId":original["submissionId"],
        "nativeEvidenceSha256":"a"*64,"nativeBinding":evidence,
    }
    revived=reconcile_submitted_native(ctx,prepared,original_receipt=proof)
    assert revived.record["native"]["threadId"]=="native-thread-once"
    assert revived.record["recordRevision"]==1


def test_created_first_record_stale_generation_prevents_native_submission(tmp_path):
    from ccx.ops.creation_steps import activate_prepared_worker
    root=fixture_root(tmp_path);ctx=require_ccx_active(root)
    prepared=prepare_worker(ctx,engine="claude",cwd=str(tmp_path),worker_id=A)
    switch_pointer(root)
    called=[]
    with pytest.raises(StaleGeneration):
        activate_prepared_worker(
            ctx,prepared,bounded_native_submit=lambda budget: called.append(1),
        )
    assert called==[]
    old=root/"identity"/"generations"/G1/"claims"/f"{A}.json"
    assert json.loads(old.read_text())["nativeActivation"]["state"]=="unprepared"


def _parallel_prepare_same_id(args):
    root,cwd,wid,title=args
    from pathlib import Path
    from ccx.store.root import require_ccx_active
    from ccx.ops.creation_steps import prepare_worker
    ctx=require_ccx_active(Path(root))
    worker=prepare_worker(
        ctx,engine="claude",cwd=cwd,worker_id=wid,name=title,
        model="sonnet",
    )
    return worker.worker_id,worker.record["native"]["sessionId"],worker.created


def test_true_multiprocess_create_same_UUID_keeps_one_original_native_id(tmp_path):
    import multiprocessing
    from ccx.system import oslayer
    root=fixture_root(tmp_path)
    jobs=[(str(root),str(tmp_path),A,f"name {i}") for i in range(8)]
    # macOS uses spawn by default. Forking an already-threaded pytest worker
    # can inherit unrelated locks, so race eight clean child interpreters.
    mp=multiprocessing.get_context(
        "spawn" if oslayer.UNIX_SUN_PATH_BYTES == 104 else "fork")
    with mp.Pool(5) as pool:
        rows=pool.map(_parallel_prepare_same_id,jobs)
    assert {r[0] for r in rows}=={A}
    provider_sessions={r[1] for r in rows}
    assert len(provider_sessions)==1
    assert next(iter(provider_sessions))!=A
    assert sum(bool(r[2]) for r in rows)==1
    ctx=require_ccx_active(root)
    assert ctx.record(A)["native"]["sessionId"]==next(iter(provider_sessions))
    assert ctx.native_activation(A)["state"]=="unprepared"
    g=root/"identity"/"generations"/G1
    assert len(list((g/"owners").glob("*.json")))==1
    assert len(list((g/"claims").glob("*.json")))==1


def test_confirmation_without_record_binding_is_not_native_send_authority(tmp_path):
    root=fixture_root(tmp_path);ctx=require_ccx_active(root)
    original=prepare_worker(ctx,engine="claude",cwd=str(tmp_path),worker_id=A)
    claim=ctx.reserve_creation(
        A,engine="claude",fingerprint=original.record["createSha256"],
        native_binding={"sessionId":original.record["native"]["sessionId"]},
    )
    activation=ctx.prepare_native_activation(claim,native_source=original.native_source)
    ctx.native_activation_dispatch(
        activation,bounded_submit=lambda budget: {"transportAccepted":True},
    )
    ctx.confirm_native_activation(
        activation,native_evidence_sha256="a"*64,
        native_binding={"sessionId":original.record["native"]["sessionId"]},
    )
    assert ctx.native_activation(A)["state"]=="confirmed"
    assert ctx.record(A)["native"]["nativeState"]=="not-started-proven"
    with pytest.raises(NativeActionDenied,match="provider identity"):
        ctx.authorize_native_action(A,"send")
    bound=ctx.bind_confirmed_native(A)
    assert bound["native"]["nativeState"]=="session-bound"
    assert ctx.authorize_native_action(A,"send").worker_id==A



def test_cli_new_missing_engine_does_not_claim_explicit_or_minted_id(tmp_path):
    import subprocess, sys
    root = fixture_root(tmp_path)
    ctx = require_ccx_active(root)
    env = {**os.environ, "CCX_ROOT": str(root),
           "PYTHONPATH": str(Path(__file__).parent.parent)}

    def run(*argv):
        return subprocess.run(
            [sys.executable, "-B", "-m", "ccx", *argv],
            env=env, text=True, capture_output=True, timeout=20,
        )

    for engine, wid in (("claude", A), ("codex", B)):
        for selected in (("--worker-id", wid), ()):
            first = run("new", "--engine", engine, "--cwd", str(tmp_path),
                        "--json", *selected)
            assert first.returncode == 3, (first.stdout, first.stderr)
            error = json.loads(first.stdout)
            assert error == {
                "ok": False, "code": "runtime_unavailable",
                "message": (f"{engine.capitalize()} CLI not found "
                            f"(looked for {env[f'CCX_{engine.upper()}_BIN']}); "
                            f"install {engine.capitalize()} or set CCX_{engine.upper()}_BIN"),
            }
            assert ctx._owner(wid) is None and ctx._claim(wid) is None
            assert ctx.list_workers(limit=None) == []
        listed = run("list", "--json")
        assert listed.returncode == 0, listed.stderr
        assert '"worker"' not in listed.stdout
    assert not (root / "sessions").exists()
    assert not (root / "codex").exists()
    assert not (root / "run").exists()


@pytest.mark.parametrize("arguments",[
    # Nonblank initial text is now supported; blank stdin remains a preclaim refusal.
    ("new","--engine","claude","--stdin"),
    ("new","--engine","claude","--file","/not/needed"),
    ("new","--engine","claude","--var","n={bad"),
    ("new","--engine","claude","--vars-from",A),
    ("new","--engine","claude","--wait"),
    ("new","--engine","claude","--notify-parent"),
    ("new","--engine","claude","--delivery-id","original-turn"),
    ("new","--engine","claude","--cap-5h","5"),
    ("new", "--engine", "codex","--var","noequals"),
    ("new", "--engine", "codex","--notify-parent"),
    ("new", "--engine", "codex","--wait"),
    ("new", "--engine", "codex","--settle"),
])
def test_cli_new_disallows_unmigrated_turn_vars_notification_before_claim(tmp_path,arguments):
    import subprocess,sys
    root=fixture_root(tmp_path)
    env={**os.environ,"CCX_ROOT":str(root),
         "PYTHONPATH":str(Path(__file__).parent.parent)}
    proc=subprocess.run(
        [sys.executable,"-B","-m","ccx",*arguments],
        env=env,text=True,capture_output=True,timeout=20,
    )
    assert proc.returncode!=0
    g=root/"identity"/"generations"/G1
    assert not (g/"owners").exists()
    assert not (g/"claims").exists()
    assert not (root/"sessions").exists()


def test_cli_new_invalid_caller_ID_or_missing_pointer_no_write(tmp_path):
    import subprocess,sys
    root=fixture_root(tmp_path)
    env={**os.environ,"CCX_ROOT":str(root),
         "PYTHONPATH":str(Path(__file__).parent.parent)}
    invalid=subprocess.run(
        [sys.executable,"-B","-m","ccx","new","--engine","claude","--cwd",str(tmp_path),
         "--worker-id","aaaaaaaa","--json"],
        env=env,capture_output=True,text=True,timeout=20,
    )
    assert invalid.returncode!=0
    assert not (root/"identity"/"generations"/G1/"owners").exists()
    old=tmp_path/"unmigrated"
    old.mkdir(mode=0o700)
    env["CCX_ROOT"]=str(old)
    blocked=subprocess.run(
        [sys.executable,"-B","-m","ccx","new","--engine","claude","--cwd",str(tmp_path)],
        env=env,capture_output=True,text=True,timeout=20,
    )
    assert blocked.returncode == 3
    assert blocked.stderr.startswith(f"ccx: created store at {old}\n")
    assert "Claude CLI not found" in blocked.stderr
    assert require_ccx_active(old).list_workers() == []  # no native effect
    assert not (old / "sessions").exists()


def test_create_postclaim_generation_error_keeps_one_original_id(tmp_path, monkeypatch):
    from ccx import constants
    from ccx.ops import creation
    from ccx.errors import CreatePartialFailure
    from ccx.store.root import GenerationError
    from ccx.store.store import GenerationStore

    root = fixture_root(tmp_path)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    binary = tmp_path / "present-fake-claude"
    binary.write_text("#!/bin/sh\nexit 1\n")
    binary.chmod(0o700)
    monkeypatch.setattr(constants, "CLAUDE_BIN", str(binary))
    store = require_ccx_active(root)
    generation = store.pointer.generation_root
    native_completions = []
    original_reserve = GenerationStore.reserve_creation

    def after_claim(stage):
        if stage == "after-claim":
            raise GenerationError("C5 postclaim failure")

    def inject_after_real_claim(self, *args, **kwargs):
        assert kwargs.pop("_failpoint", None) is None
        return original_reserve(self, *args, _failpoint=after_claim, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(GenerationStore, "reserve_creation", inject_after_real_claim)
        patch.setattr(creation, "_complete_claude_create",
                      lambda *a, **k: native_completions.append(True))
        with pytest.raises(CreatePartialFailure) as first:
            creation.create(engine="claude", cwd=str(tmp_path), worker_id=A)
    error = first.value
    assert error.phase == "record" and error.worker_id == A
    assert str(error) == (
        "Worker setup is incomplete; check status before retrying; "
        "retain and reconcile exact worker_id"
    )
    assert isinstance(error.cause, GenerationError) and error.__cause__ is error.cause
    owner_file = generation / "owners" / f"{A}.json"
    claim_file = generation / "claims" / f"{A}.json"
    assert owner_file.is_file() and claim_file.is_file()
    assert not (generation / "workers" / "claude" / A / "record.json").exists()
    native_sid = json.loads(owner_file.read_text())["createIntent"]["nativeBinding"]["sessionId"]
    assert canonical_worker_id(native_sid) == native_sid and native_sid != A
    assert not native_completions

    # Same-ID replay recovers the first record and SID; the fake native
    # activation fails after the executable preflight.
    from ccx.store.root import NativeEffectIndeterminate
    def fail_native(*_args, **_kwargs):
        raise NativeEffectIndeterminate("fake native activation refused")
    monkeypatch.setattr(creation, "_complete_claude_create", fail_native)
    with pytest.raises(CreatePartialFailure) as retry:
        creation.create(engine="claude", cwd=str(tmp_path), worker_id=A)
    assert retry.value.phase == "native-activation" and retry.value.worker_id == A
    assert len(list((generation / "owners").glob("*.json"))) == 1
    assert store.record(A)["native"]["sessionId"] == native_sid
    assert json.loads(claim_file.read_text())["nativeActivation"]["state"] == "unprepared"
    assert not native_completions



def test_create_model_as_agent_refuses_without_claim(tmp_path, monkeypatch):
    from ccx import constants
    from ccx.ops import creation
    from ccx.errors import InvalidRequest

    root = fixture_root(tmp_path)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    binary = tmp_path / "present-fake-claude"
    binary.write_text("#!/bin/sh\nexit 1\n")
    binary.chmod(0o700)
    monkeypatch.setattr(constants, "CLAUDE_BIN", str(binary))
    with pytest.raises(InvalidRequest, match="--model fable"):
        creation.create(engine="claude", cwd=str(tmp_path), worker_id=A,
                        agent="fable", prompt=None)
    generation = root / "identity" / "generations" / G1
    assert not (generation / "owners" / f"{A}.json").exists()
    assert not (generation / "claims" / f"{A}.json").exists()
    assert not (generation / "workers" / "claude" / A / "record.json").exists()


def test_fake_claude_create_trusts_exact_cwd_and_add_dir_before_native_start(
    tmp_path, monkeypatch
):
    from ccx import constants
    from ccx.ops import creation
    from ccx.claude import settings
    from ccx.claude import spawn

    root = fixture_root(tmp_path)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    cwd = tmp_path / "linked-worktree"
    extra = tmp_path / "added-dir"
    cwd.mkdir()
    extra.mkdir()
    native_profile = tmp_path / "private-fake-claude" / ".claude.json"
    native_profile.parent.mkdir()
    native_profile.write_text(json.dumps({"projects": {
        str(tmp_path): {"hasTrustDialogAccepted": True},
        str(cwd): {"hasTrustDialogAccepted": False, "custom": "keep"},
        str(extra): {"hasTrustDialogAccepted": False},
        str(tmp_path / "untouched"): {"hasTrustDialogAccepted": False},
    }}))
    monkeypatch.setattr(settings, "CLAUDE_DOTFILE", native_profile)
    fake_bin = tmp_path / "fake-claude"
    fake_bin.write_text("#!/bin/sh\nexit 99\n")  # Must never execute.
    fake_bin.chmod(0o700)
    monkeypatch.setattr(constants, "CLAUDE_BIN", str(fake_bin))
    native_calls = []

    def fake_start(store, original, budget, *, deadline):
        projects = json.loads(native_profile.read_text())["projects"]
        assert projects[str(cwd)] == {"hasTrustDialogAccepted": True, "custom": "keep"}
        assert projects[str(extra)]["hasTrustDialogAccepted"] is True
        assert projects[str(tmp_path / "untouched")]["hasTrustDialogAccepted"] is False
        receipt = store.native_activation(original.worker)
        assert receipt["state"] == "submitted-unknown"
        native_calls.append(original.worker_id)
        return {
            "accepted": True, "workerId": original.worker_id, "engine": "claude",
            "source": original.native_source, "submissionId": receipt["submissionId"],
            "nativeEvidenceSha256": "d" * 64,
            "nativeBinding": {"sessionId": original.record["native"]["sessionId"]},
        }

    monkeypatch.setattr(spawn, "submit_original_claude_start", fake_start)
    created = creation.create(
        engine="claude", cwd=str(cwd), add_dir=[str(extra)], worker_id=A,
    )
    assert created["native_ready"] is True and native_calls == [A]
    assert require_ccx_active(root).native_activation(A)["state"] == "confirmed"


def test_claude_trust_settings_io_failure_is_typed_and_never_submits(
    tmp_path, monkeypatch
):
    from ccx import constants
    from ccx.ops import creation
    from ccx.claude import settings
    from ccx.claude import spawn
    from ccx.errors import CreatePartialFailure, SpawnFailed

    root = fixture_root(tmp_path)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    fake_bin = tmp_path / "fake-claude"
    fake_bin.write_text("#!/bin/sh\nexit 99\n")
    fake_bin.chmod(0o700)
    monkeypatch.setattr(constants, "CLAUDE_BIN", str(fake_bin))
    monkeypatch.setattr(settings, "ensure_trusted_paths", lambda _: (_ for _ in ()).throw(
        OSError("private fake profile cannot be written")))
    monkeypatch.setattr(spawn, "submit_original_claude_start", lambda *_a, **_k:
                        pytest.fail("no native launch after settings I/O failure"))

    with pytest.raises(CreatePartialFailure) as captured:
        creation.create(engine="claude", cwd=str(tmp_path), worker_id=A)
    assert captured.value.worker_id == A and captured.value.phase == "native-activation"
    assert isinstance(captured.value.cause, SpawnFailed)
    assert captured.value.cause.phase == "settings-io"
    assert require_ccx_active(root).native_activation(A)["state"] == "unprepared"
