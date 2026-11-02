"""A signed-out Claude fails `create` with a plain sign-in message."""
from __future__ import annotations

import sys

import pytest

from test_generation_store import A, short_root  # noqa: F401
from test_claude_native_cli import _stop_disposable_original
from ccx import constants
from ccx.errors import CreatePartialFailure
from ccx.ops import creation


def _never_ready_claude(tmp_path, monkeypatch, root, logged_in: bool):
    """Sits silent like a login screen; `claude auth status` reports the login."""
    fake = tmp_path / "claude-fake"
    fake.write_text(
        "#!/bin/sh\n"
        'if [ "$1" = auth ]; then echo \'{"loggedIn": '
        + ("true" if logged_in else "false") + "}'; exit 0; fi\n"
        "exec sleep 30\n"
    )
    fake.chmod(0o700)
    monkeypatch.setattr(constants, "CCX_ROOT", root)
    monkeypatch.setattr(constants, "CLAUDE_BIN", str(fake))
    for name, value in {"CCX_ROOT": str(root), "CCX_CLAUDE_BIN": str(fake),
                        "CCX_SPAWN_SCOPE": "0", "CCX_BOOT_TIMEOUT_S": "3"}.items():
        monkeypatch.setenv(name, value)
    for name in ("CCX_SELF_SHORT", "CCX_SELF_WORKER_ID", "CCX_SELF_ENGINE"):
        monkeypatch.delenv(name, raising=False)


@pytest.mark.parametrize("logged_in", [False, True])
def test_failed_claude_activation_names_a_signed_out_account(
    short_root, tmp_path, monkeypatch, logged_in,  # noqa: F811
):
    _never_ready_claude(tmp_path, monkeypatch, short_root, logged_in)
    try:
        with pytest.raises(CreatePartialFailure) as failed:
            creation.create(engine="claude", cwd=str(tmp_path), worker_id=A,
                            permissions="full", start_timeout=3)
    finally:
        _stop_disposable_original(short_root)
    error = failed.value
    assert error.phase == "native-activation" and error.worker_id == A
    if logged_in:
        assert "signed_out" not in error.info
        assert "not signed in" not in str(error)
    else:
        assert error.info["signed_out"] is True
        assert "run `claude` once to sign in" in str(error)
