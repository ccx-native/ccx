"""Restricted managed Claude workers launch in Claude's default permission mode."""
from ccx.claude.host.session import permission_args


def test_full_worker_skips_permissions_without_a_mode():
    assert permission_args({}) == ["--dangerously-skip-permissions"]
    assert permission_args({"skip_permissions": True}) == ["--dangerously-skip-permissions"]


def test_restricted_worker_without_a_mode_is_forced_to_default():
    # A user `defaultMode: "auto"` would otherwise answer every prompt itself.
    assert permission_args({"skip_permissions": False}) == ["--permission-mode", "default"]


def test_an_explicit_mode_wins():
    assert permission_args({"skip_permissions": False, "permission_mode": "acceptEdits"}) == [
        "--permission-mode", "acceptEdits"]
    assert permission_args({"permission_mode": "plan"}) == [
        "--dangerously-skip-permissions", "--permission-mode", "plan"]
