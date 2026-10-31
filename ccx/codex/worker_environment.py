"""Prevent one launcher's identity from entering a shared Codex app-server."""

from __future__ import annotations


def scrub_shared_codex_identity(env: dict[str, str]) -> dict[str, str]:
    """Keep user tool/auth settings; remove only worker/session identity."""
    from ccx.claude.host.session import INHERITED_CLAUDE_IDENTITY_VARS

    stale = {
        *INHERITED_CLAUDE_IDENTITY_VARS,
        "CLAUDE_CODE_FORCE_SESSION_PERSISTENCE",
        "CLAUDECODE",
        "CCX_SPAWN_SCOPE",
        "CODEX_THREAD_ID",
    }
    for key in tuple(env):
        if key.startswith("CCX_SELF_") or key in stale:
            env.pop(key, None)
    # Restore the caller's genuine user shell paths while discarding every
    # parent-owned Bash, zsh and Claude Python shim. A shared daemon never
    # carries a worker's generated shell function into its next thread.
    from ccx.controllers.live_vars import sanitize_inherited_shell_bridge_env
    sanitize_inherited_shell_bridge_env(env)
    return env
