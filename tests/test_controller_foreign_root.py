"""A worker whose identity belongs to another CCX_ROOT gets a clean refusal."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys

from ccx.store.root import initialize_empty_root

REPO = Path(__file__).resolve().parent.parent


def test_controller_run_from_a_worker_of_another_root_fails_cleanly(tmp_path):
    root = tmp_path / "root"
    root.mkdir(mode=0o700)
    initialize_empty_root(root)
    script = tmp_path / "flow.py"
    script.write_text("raise SystemExit('the script must not start')\n")
    env = {
        **os.environ, "CCX_ROOT": str(root), "PYTHONPATH": str(REPO),
        # A worker identity this root has never seen.
        "CCX_SELF_WORKER_ID": "dddddddd-1234-4000-8000-000000000001",
        "CCX_SELF_ENGINE": "claude",
    }
    env.pop("CCX_SPAWN_SCOPE", None)
    result = subprocess.run(
        [sys.executable, "-B", "-m", "ccx", "controller", "run", str(script)],
        env=env, capture_output=True, text=True, timeout=30, cwd=tmp_path,
    )
    assert result.returncode == 3, result.stderr
    assert "Traceback" not in result.stderr
    assert "caller worker identity is not proven in this CCX_ROOT" in result.stderr
