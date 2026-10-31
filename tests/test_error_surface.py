"""Pure classification of Claude API errors from a worker lifecycle record.

`api_error_from_lifecycle` turns a lifecycle `apiError` payload into the
`WorkerApiError` subclass a caller branches on (permanent, transient or rate
limited, each with its own exit code), or None when the failure was not an
API error.
"""

from ccx import errors as E
from ccx.constants import (
    EXIT_API_TRANSIENT,
    EXIT_RATE_LIMITED,
    EXIT_UNAVAILABLE,
)


def _api_lifecycle(**over):
    payload = {
        "status": 529, "kind": "server_error", "transient": True,
        "rate_limited": False, "reset_text": None, "request_id": "req_1",
        "message": "API Error: 529 Overloaded.",
    }
    payload.update(over)
    return {"lastError": "Claude API overloaded (529): 529 Overloaded.",
            "apiError": payload}


def test_worker_api_error_keeps_broad_catch_working():
    """It subclasses SessionUnavailable so existing `except SessionUnavailable`
    callers keep working while new callers can read the classification."""
    e = E.api_error_from_lifecycle(_api_lifecycle(), short="abc12345")
    assert isinstance(e, E.SessionUnavailable) and isinstance(e, E.CcxError)
    assert (e.kind, e.status, e.transient) == ("server_error", 529, True)
    assert e.request_id == "req_1"


def test_api_error_subclasses_split_the_three_remedies():
    """The whole point of the split: three conditions a caller handles
    differently must not share one exit code, and `WorkerApiError` must still
    catch all of them."""
    overloaded = E.api_error_from_lifecycle(_api_lifecycle(), short="s")
    limited = E.api_error_from_lifecycle(_api_lifecycle(
        status=429, kind="rate_limit", transient=False, rate_limited=True,
    ), short="s")
    permanent = E.api_error_from_lifecycle(_api_lifecycle(
        status=None, kind="authentication_failed", transient=False,
    ), short="s")

    assert isinstance(overloaded, E.WorkerApiTransient)
    assert overloaded.exit_code == EXIT_API_TRANSIENT
    assert isinstance(limited, E.WorkerRateLimited)
    assert limited.exit_code == EXIT_RATE_LIMITED
    assert isinstance(permanent, E.WorkerApiPermanent)
    assert permanent.exit_code == EXIT_UNAVAILABLE

    # One catch-all covers every Anthropic-side failure...
    for e in (overloaded, limited, permanent):
        assert isinstance(e, E.WorkerApiError)
        # ...and is never the verdict itself, so `type(e) is WorkerApiError`
        # can never quietly come to mean "permanent".
        assert type(e) is not E.WorkerApiError
    # All four codes stay distinct, or a caller cannot branch on them.
    assert len({EXIT_UNAVAILABLE, EXIT_RATE_LIMITED, EXIT_API_TRANSIENT,
                E.BudgetStopped.exit_code}) == 4


def test_throttled_429_is_rate_limited_not_overloaded():
    """Claude marks a capacity-throttle 429 transient AND rate-limited. The
    rate limit must win — it is the one that must never go unnoticed."""
    e = E.api_error_from_lifecycle(_api_lifecycle(
        status=429, kind="rate_limit", transient=True, rate_limited=True,
    ), short="s")
    assert isinstance(e, E.WorkerRateLimited)
    assert e.exit_code == EXIT_RATE_LIMITED
    assert e.transient is True


def test_rate_limit_gets_its_own_exit_code_and_reset_text():
    """A rate limit must never be indistinguishable from a generic failure —
    that is exactly how it would go unnoticed."""
    e = E.api_error_from_lifecycle(_api_lifecycle(
        status=429, kind="rate_limit", transient=False, rate_limited=True,
        reset_text="resets 8:40pm (Europe/Madrid)",
    ), short="abc12345")
    assert isinstance(e, E.WorkerRateLimited)
    assert isinstance(e, E.WorkerApiError)
    assert e.exit_code == EXIT_RATE_LIMITED
    assert e.reset_text == "resets 8:40pm (Europe/Madrid)"
    # Distinct from CCX's own cap, which is a different condition entirely.
    assert not isinstance(e, E.BudgetStopped)


def test_api_error_absent_returns_none_so_generic_handling_still_runs():
    assert E.api_error_from_lifecycle({"lastError": "crashed"}) is None
    assert E.api_error_from_lifecycle(None) is None
