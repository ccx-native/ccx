from __future__ import annotations

import types

from ccx.system.executable import current_executable_path


def test_current_executable_preserves_direct_ccx_entrypoint(monkeypatch, tmp_path):
    direct = tmp_path / "ccx.py"
    direct.write_text("")
    monkeypatch.setattr("__main__.__file__", str(direct), raising=False)

    assert current_executable_path() == str(direct)


def test_current_executable_uses_package_main_when_embedded(monkeypatch):
    monkeypatch.setattr("__main__.__file__", "/some/venv/bin/uvicorn", raising=False)

    path = current_executable_path()

    assert path.endswith("/ccx/__main__.py")


def test_current_executable_falls_back_to_main_file_without_package(monkeypatch):
    monkeypatch.setattr("__main__.__file__", "/some/app/server.py", raising=False)
    monkeypatch.setitem(__import__("sys").modules, "ccx", types.SimpleNamespace(__file__=None))

    assert current_executable_path() == "/some/app/server.py"
