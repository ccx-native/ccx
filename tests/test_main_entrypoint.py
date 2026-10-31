from __future__ import annotations

import pathlib
import subprocess
import sys


def test_package_main_file_runs_as_direct_script():
    repo = pathlib.Path(__file__).resolve().parent.parent
    proc = subprocess.run(
        [sys.executable, str(repo / "ccx" / "__main__.py"), "--help"],
        cwd=repo,
        text=True,
        capture_output=True,
        timeout=10,
    )
    assert proc.returncode == 0, proc.stderr
    assert "usage: ccx" in proc.stdout
