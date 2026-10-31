"""Pure compact result classification."""

from ccx.ops import compact_ops as O


def test_result_status_files_a_failure_as_failed_not_noop():
    """A failure is also compacted=False, so testing only that would file
    Claude's explicit failure as a benign no-op."""
    assert O.result_status({"ok": False, "compacted": False}) == "failed"
    assert O.result_status({"ok": True, "compacted": False}) == "noop"
    assert O.result_status({"ok": True, "compacted": True}) == "succeeded"
