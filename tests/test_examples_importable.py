"""Import examples headlessly without a terminal or live CCX subprocesses."""

from __future__ import annotations

import importlib.util
import pathlib

import pytest

_EXAMPLES = pathlib.Path(__file__).resolve().parent.parent / "examples"


def _load(path: pathlib.Path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.mark.parametrize("path", sorted(_EXAMPLES.glob("*.py")))
def test_example_importable(path):
    mod = _load(path)
    assert callable(getattr(mod, "main", None))
