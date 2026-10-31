"""Root path constants are absolute whatever the caller's environment says.

The managed host ``chdir``s into the worker cwd before it writes its pid file,
and the hooks relay runs in that cwd too, so a relative ``CCX_ROOT`` used to
crash every host at boot and point the relay at a different root.
"""

from __future__ import annotations

import json
import os
import pathlib
import subprocess
import sys

_REPO = pathlib.Path(__file__).resolve().parent.parent
_PROBE = (
    "import json; from ccx import constants as c; "
    "print(json.dumps([str(c.CCX_ROOT), str(c.CLAUDE_HOME), str(c.CLAUDE_DOTFILE)]))"
)
# What a child process (host, claude, hooks relay) inherits.
_CHILD_ENV_PROBE = (
    "import json, os, subprocess, sys; from ccx import constants; "
    "print(subprocess.run([sys.executable, '-c', 'import json, os; "
    "print(json.dumps([os.environ[\\'CCX_ROOT\\'], os.environ[\\'CLAUDE_CONFIG_DIR\\']]))'], "
    "cwd='/', capture_output=True, text=True, check=True).stdout, end='')"
)


def _constants(cwd, probe=_PROBE, **env):
    full = {**os.environ, "PYTHONPATH": str(_REPO), **env}
    out = subprocess.run([sys.executable, "-c", probe], cwd=cwd, env=full,
                         capture_output=True, text=True, check=True).stdout
    return [pathlib.Path(p) for p in json.loads(out)]


def test_relative_roots_resolve_against_the_launch_cwd(tmp_path):
    root, home, dotfile = _constants(tmp_path, CCX_ROOT="relroot",
                                     CLAUDE_CONFIG_DIR="relclaude")
    assert root == tmp_path / "relroot"
    assert home == tmp_path / "relclaude"
    assert dotfile == tmp_path / "relclaude" / ".claude.json"


def test_tilde_roots_expand(tmp_path):
    root, home, _ = _constants(tmp_path, HOME=str(tmp_path / "h"),
                               CCX_ROOT="~/ccxroot", CLAUDE_CONFIG_DIR="~/cfg")
    assert root == tmp_path / "h" / "ccxroot"
    assert home == tmp_path / "h" / "cfg"


def test_children_inherit_the_absolute_roots(tmp_path):
    # The child runs from another cwd, as the host and relay do.
    root, home = _constants(tmp_path, _CHILD_ENV_PROBE, CCX_ROOT="relroot",
                            CLAUDE_CONFIG_DIR="relclaude")
    assert root == tmp_path / "relroot"
    assert home == tmp_path / "relclaude"
