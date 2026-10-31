"""Unbounded default wait semantics shared by full-UUID API and CLI.

Old short-record, external-discovery and private managed-wait arrangements
are retired; current worker-scoped timeout tests own runtime verdicts.
"""
from __future__ import annotations

import inspect

from ccx import CcxClient


def test_wait_default_timeout_is_none():
    # Only an explicit numeric timeout should bound a worker wait.
    sig = inspect.signature(CcxClient.wait)
    assert sig.parameters["timeout"].default is None


def test_turn_verbs_default_to_an_unbounded_timeout():
    """A turn has no natural length; only an explicit timeout limits it.

    Steer and compact intentionally differ: steer watches placement while
    compact waits for an operational step.
    """
    for verb in ("new", "send", "answer", "approve", "reject", "wait"):
        sig = inspect.signature(getattr(CcxClient, verb))
        assert sig.parameters["timeout"].default is None, verb
