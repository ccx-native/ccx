"""The npm launcher (bin/ccx) finds a Python 3.11+ even under a versioned name."""
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
NODE = shutil.which("node")
pytestmark = pytest.mark.skipif(NODE is None, reason="node is not installed")


def _run(path_dir: Path, tmp_path: Path, **env):
    return subprocess.run(
        [NODE, str(ROOT / "bin" / "ccx"), "--version"], capture_output=True, text=True,
        env={"PATH": str(path_dir), "HOME": str(tmp_path), **env}, timeout=60,
    )


def _old_python3(bin_dir: Path):
    fake = bin_dir / "python3"
    fake.write_text("#!/bin/sh\necho 3.9\nexit 1\n")
    fake.chmod(0o755)


def test_skips_old_python3_for_a_versioned_python(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _old_python3(bin_dir)
    os.symlink(sys.executable, bin_dir / "python3.12")
    result = _run(bin_dir, tmp_path)
    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith("ccx ")


def test_reports_the_old_python_when_nothing_newer_exists(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _old_python3(bin_dir)
    result = _run(bin_dir, tmp_path)
    assert result.returncode == 127
    assert "Python 3.11+ is required (python3 is Python 3.9)" in result.stderr
