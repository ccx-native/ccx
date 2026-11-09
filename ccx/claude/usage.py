from __future__ import annotations

import argparse
from contextlib import contextmanager
from email.utils import parsedate_to_datetime
import fcntl
import math
from pathlib import Path
from datetime import datetime, timezone
import json
import os
import stat
import time
import urllib.error
import urllib.parse
import urllib.request

from ccx.system import oslayer
from ccx.system.private_json import (
    _atomic_private_json,
    _open_private_file,
    _read_private_json,
)
from ccx.claude.oauth import oauth_headers, oauth_organization_uuid, oauth_token
from ccx.constants import (
    CAP_METRICS,
    CLAUDE_HOME,
    usage_source_path,
    USAGE_MIN_FETCH_INTERVAL_S,
    OAUTH_USAGE_MIN_FETCH_INTERVAL_S,
    OAUTH_USAGE_STALE_LIMIT_S,
    UNOFFICIAL_RESET_ENV,
    CAP_POLL_INTERVAL_S,
    usage_cache_path,
    USAGE_STALE_LIMIT_S,
    USAGE_URL,
    opt_in_enabled,
)
from ccx.errors import InvalidRequest

_SESSION_RESET_PROGRAM = "juniper_tide"
_SESSION_RESET_RESULTS = frozenset({
    "reset", "already_used", "not_limited", "ineligible", "unavailable",
})
_SESSION_RESET_REASONS = frozenset({
    "tier", "tenure", "surface", "mobile", "cli_version", "not_at_wall",
    "weekly_limit", "no_weekly_limit", "other_experiment", "extra_usage",
    "unavailable", "unknown",
})
_SESSION_RESET_ARMS = frozenset({"control", "reset"})
_SESSION_RESET_TIMEOUT_S = 25.0


# --- machine-wide Claude plan usage source and shared cache -----------------

_SOURCE_CHOICES = frozenset({"get_usage", "oauth"})
_RETRY_ERROR_S = 120.0
_RETRY_HELD_INITIAL_S = 300.0
_RETRY_HELD_MAX_S = 1800.0


def _open_usage_cache() -> int:
    return _open_private_file(usage_cache_path())


def _read_usage_cache() -> dict | None:
    try:
        return _read_private_json(usage_cache_path())
    except (OSError, ValueError, UnicodeError):
        return None


def _utc_timestamp(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_epoch(value: object) -> float | None:
    if not isinstance(value, str):
        return None
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if result.tzinfo is None:
            return None
        return result.timestamp()
    except (ValueError, OverflowError):
        return None


def _cache_age(data: dict | None = None) -> float | None:
    """Age of the last successful provider sample, not the metadata file mtime."""
    if data is None:
        data = _read_usage_cache()
    epoch = _parse_epoch((data.get("_ccx") or {}).get("fetchedAt")) if isinstance(data, dict) else None
    return max(0.0, time.time() - epoch) if epoch is not None else None


def _write_usage_cache(data: dict) -> None:
    _atomic_private_json(usage_cache_path(), data)


def _usage_source() -> str:
    """Persistent per-CCX_ROOT selector, read on every use (including live hosts)."""
    try:
        record = _read_private_json(usage_source_path())
    except FileNotFoundError:
        return "get_usage"
    except (OSError, ValueError, UnicodeError) as exc:
        raise InvalidRequest("invalid private Claude usage-source setting") from exc
    if not isinstance(record, dict) or record.get("version") != 1 or record.get("source") not in _SOURCE_CHOICES:
        raise InvalidRequest("invalid Claude usage-source setting; choose get_usage or oauth")
    if record["source"] == "oauth" and not oslayer.direct_oauth_usage_supported():
        raise InvalidRequest("Claude OAuth usage source is Linux-only; select get_usage")
    return record["source"]


def _lock_file_path() -> Path:
    return Path(str(usage_cache_path()) + ".lock")


@contextmanager
def _usage_lock(timeout: float):
    """Serialize source switches and refreshes across independent CCX processes."""
    path = _lock_file_path()
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid() or st.st_mode & 0o077:
            raise PermissionError("usage cache lock must be private and owned by this user")
        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("Claude usage cache lock busy")
                time.sleep(min(0.025, max(0.0, deadline - time.monotonic())))
        try:
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def set_usage_source(source: str) -> str:
    """Explicit persistent selector update, without a provider read."""
    if source not in _SOURCE_CHOICES:
        raise InvalidRequest("usage source must be get_usage or oauth")
    if source == "oauth" and not oslayer.direct_oauth_usage_supported():
        raise InvalidRequest("direct OAuth usage is Linux-only; use get_usage")
    try:
        with _usage_lock(8.0):
            try:
                previous = _usage_source()
            except InvalidRequest:
                # The atomic writer still checks ownership and private-file mode.
                previous = None
            if previous != source or not usage_source_path().exists():
                _atomic_private_json(usage_source_path(), {"version": 1, "source": source})
                # Retire values from the previous source; never relabel them.
                _write_usage_cache({"_ccx": _new_metadata(source)})
    except (OSError, TimeoutError) as exc:
        raise InvalidRequest(f"cannot set Claude usage source: {exc}") from exc
    return source


_USAGE_RESPONSE_KEYS = frozenset({
    "five_hour", "seven_day", "seven_day_opus", "seven_day_sonnet",
    "limits", "model_scoped", "extra_usage", _SESSION_RESET_PROGRAM,
})


def _is_usage_payload(value: object) -> bool:
    return isinstance(value, dict) and any(key in value for key in _USAGE_RESPONSE_KEYS)


def _usage_request_url(*, at_wall: bool, skip_spend: bool = False) -> str:
    if not at_wall:
        return USAGE_URL
    query = {"at_wall": 1}
    if skip_spend:
        query["skip_spend"] = 1
    return f"{USAGE_URL}?{urllib.parse.urlencode(query)}"


def _oauth_sample(timeout: float = 8.0, *, at_wall: bool = True,
                  skip_spend: bool = False) -> dict:
    """The only read-only path that accesses the private OAuth identity."""
    token = oauth_token()
    if not token:
        return {"kind": "error", "error": "no Claude OAuth token (log in to Claude Code)"}
    request = urllib.request.Request(
        _usage_request_url(at_wall=at_wall, skip_spend=skip_spend),
        headers=oauth_headers(token),
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            if not 200 <= response.status < 300:
                return {"kind": "error", "error": f"usage endpoint returned HTTP {response.status}"}
            body = json.loads(response.read(2 * 1024 * 1024 + 1).decode())
    except urllib.error.HTTPError as exc:
        if exc.code == 429:
            retry_at = None
            headers = exc.headers
            if headers is not None:
                value = headers.get("Retry-After")
                if value:
                    try:
                        retry_at = time.time() + max(0.0, float(value))
                    except ValueError:
                        try:
                            retry_at = parsedate_to_datetime(value).timestamp()
                        except (ValueError, TypeError, OverflowError):
                            pass
            return {"kind": "held", "error": "usage endpoint returned HTTP 429",
                    "retry_at": retry_at}
        return {"kind": "error", "error": f"usage endpoint returned HTTP {exc.code}"}
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        return {"kind": "error", "error": f"usage endpoint unreachable: {exc.reason if isinstance(exc, urllib.error.URLError) else type(exc).__name__}"}
    except (ValueError, UnicodeError):
        return {"kind": "error", "error": "usage endpoint returned unreadable JSON"}
    if _is_usage_payload(body):
        return {"kind": "success", "body": body}
    return {"kind": "unparsed", "raw": body,
            "error": "usage endpoint returned an unrecognized payload"}


def _request_usage_live_detail(
    timeout: float = 8.0, *, at_wall: bool = True, skip_spend: bool = False,
) -> tuple[dict | None, str | None]:
    """Private direct-OAuth read, kept separate for reset's authoritative preflight."""
    result = _oauth_sample(timeout, at_wall=at_wall, skip_spend=skip_spend)
    return (result.get("body"), None) if result["kind"] == "success" else (None, result["error"])


def _request_usage_live(
    timeout: float = 8.0, *, at_wall: bool = True, skip_spend: bool = False,
) -> dict | None:
    return _request_usage_live_detail(timeout, at_wall=at_wall, skip_spend=skip_spend)[0]


def _new_metadata(source: str) -> dict:
    """Only the owner-approved persisted CCX metadata fields."""
    return {"version": 1, "source": source, "fetchedAt": None,
            "lastAttemptAt": None, "nextAttemptAt": None, "held": False,
            "heldUntil": None, "heldUntilEstimated": False, "unparsed": False,
            "raw": None, "error": None}


def _matching_cache(source: str) -> dict | None:
    data = _read_usage_cache()
    meta = data.get("_ccx") if isinstance(data, dict) else None
    if isinstance(meta, dict) and meta.get("version") == 1 and meta.get("source") == source:
        return data
    return None


def _is_stale(data: dict | None, stale_limit: float = USAGE_STALE_LIMIT_S) -> bool:
    if not isinstance(data, dict):
        return True
    meta = data.get("_ccx") or {}
    if not isinstance(meta, dict) or meta.get("held") or meta.get("unparsed") or meta.get("error"):
        return True
    if meta.get("source") == "oauth":
        stale_limit = max(stale_limit, OAUTH_USAGE_STALE_LIMIT_S)
    age = _cache_age(data)
    return age is None or age > stale_limit


def _refresh_due(data: dict | None, *, force: bool, max_age: float, now: float) -> bool:
    if isinstance(data, dict):
        meta = data.get("_ccx") or {}
        deadline = _parse_epoch(meta.get("nextAttemptAt"))
        if deadline is not None and now < deadline:
            return False
        if not force:
            age = _cache_age(data)
            if age is not None and age < max_age and not meta.get("held") and not meta.get("error") and not meta.get("unparsed"):
                return False
    return True


def _next_held_interval(old_meta: dict) -> float:
    if old_meta.get("held"):
        start = _parse_epoch(old_meta.get("lastAttemptAt"))
        end = _parse_epoch(old_meta.get("heldUntil"))
        if start is not None and end is not None and end > start:
            return min(_RETRY_HELD_MAX_S, max(_RETRY_HELD_INITIAL_S, (end - start) * 2))
    return _RETRY_HELD_INITIAL_S


def _apply_sample(source: str, prior: dict | None, sample: dict, now: float) -> dict:
    """Last-known provider values are retained; failed attempts never freshen them."""
    old_meta = (prior or {}).get("_ccx") or {}
    meta = _new_metadata(source)
    meta["fetchedAt"] = old_meta.get("fetchedAt")
    meta["lastAttemptAt"] = _utc_timestamp(now)
    kind = sample["kind"]
    if kind == "success" and (
        not _is_usage_payload(sample.get("body")) or "_ccx" in sample["body"]
    ):
        sample = {"kind": "unparsed", "raw": sample.get("body"),
                  "error": "unrecognized Claude usage response"}
        kind = "unparsed"
    if kind == "success":
        body = dict(sample["body"])
        meta["fetchedAt"] = _utc_timestamp(now)
        floor = OAUTH_USAGE_MIN_FETCH_INTERVAL_S if source == "oauth" else USAGE_MIN_FETCH_INTERVAL_S
        meta["nextAttemptAt"] = _utc_timestamp(now + floor)
    else:
        body = {k: v for k, v in (prior or {}).items() if k != "_ccx"}
        meta["error"] = sample.get("error") or "Claude usage unavailable"
        if kind == "held":
            interval = _next_held_interval(old_meta)
            provider_retry = sample.get("retry_at")
            valid_retry = isinstance(provider_retry, (int, float)) and now < provider_retry <= now + _RETRY_HELD_MAX_S
            until = provider_retry if valid_retry else now + interval
            meta["held"] = True
            meta["heldUntil"] = _utc_timestamp(until)
            meta["heldUntilEstimated"] = not valid_retry
            meta["nextAttemptAt"] = meta["heldUntil"]
        else:
            delay = 300.0 if kind == "unavailable" else _RETRY_ERROR_S
            meta["nextAttemptAt"] = _utc_timestamp(now + delay)
            if kind == "unparsed":
                meta["unparsed"] = True
                meta["raw"] = sample.get("raw")
    body["_ccx"] = meta
    return body


def _origin(data: dict | None, source: str | None, live: bool,
            error: str | None = None) -> dict:
    meta = (data or {}).get("_ccx") or {}
    age = _cache_age(data)
    return {"source": "live" if live else "cache" if data is not None else None,
            "provider": source, "age_s": age,
            "fetched_at": meta.get("fetchedAt"),
            "held": bool(meta.get("held")), "held_until": meta.get("heldUntil"),
            "held_until_estimated": bool(meta.get("heldUntilEstimated")),
            "stale": _is_stale(data), "unparsed": bool(meta.get("unparsed")),
            "next_attempt_at": meta.get("nextAttemptAt"),
            "live_error": error or meta.get("error"), "cache": str(usage_cache_path())}


def _fetch_usage_common(*, timeout: float, max_age: float, force: bool) -> tuple[dict | None, dict]:
    try:
        source = _usage_source()
    except InvalidRequest as exc:
        return None, _origin(None, None, False, str(exc))
    cached = _matching_cache(source)
    now = time.time()
    if not _refresh_due(cached, force=force, max_age=max_age, now=now):
        return cached, _origin(cached, source, False)
    began = time.monotonic()
    try:
        with _usage_lock(timeout):
            # A different process might have fetched or changed the source.
            source = _usage_source()
            cached = _matching_cache(source)
            now = time.time()
            if not _refresh_due(cached, force=force, max_age=max_age, now=now):
                return cached, _origin(cached, source, False)
            left = max(0.0, timeout - (time.monotonic() - began))
            if left <= 0:
                return cached, _origin(cached, source, False, "Claude usage fetch deadline exceeded")
            if source == "oauth":
                sample = _oauth_sample(left)
            else:
                from ccx.claude.usage_control import request_usage
                sample = request_usage(left)
            fresh = _apply_sample(source, cached, sample, time.time())
            _write_usage_cache(fresh)
            return fresh, _origin(fresh, source, sample["kind"] == "success")
    except (OSError, TimeoutError, InvalidRequest) as exc:
        return cached, _origin(cached, source, False, f"Claude usage fetch failed: {exc}")


def fetch_usage(timeout: float = 8.0, max_age: float = CAP_POLL_INTERVAL_S,
                force: bool = False, stale_limit: float | None = None) -> dict | None:
    """One source-aware, process-shared cached Claude account usage read.

    force ignores the caller's age preference, never the 60s global floor or
    provider hold/backoff. stale_limit additionally fails open for caps.
    """
    data, _ = _fetch_usage_common(timeout=8.0 if timeout is None else max(0.0, timeout),
                                  max_age=max(0.0, max_age), force=force)
    if data is None or (stale_limit is not None and _is_stale(data, stale_limit)):
        return None
    if not _is_usage_payload(data) and not (data.get("_ccx") or {}).get("unparsed"):
        return None
    return data


def fetch_usage_for_display(
    *, force: bool, timeout: float = 8.0, max_age: float = CAP_POLL_INTERVAL_S,
) -> tuple[dict | None, dict]:
    data, origin = _fetch_usage_common(timeout=max(0.0, timeout),
                                       max_age=max(0.0, max_age), force=force)
    if data is not None and not _is_usage_payload(data) and not origin["unparsed"]:
        return None, origin
    return data, origin


def describe_usage_age(age_s: float | None) -> str:
    if age_s is None:
        return "age unknown"
    if age_s < 90:
        return f"{max(0, int(age_s))}s old"
    if age_s < 90 * 60:
        return f"{int(age_s // 60)}m old"
    return f"{int(age_s // 3600)}h old"


def _strict_provider_time(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError("provider time must be a non-empty ISO string or null")
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("invalid provider time") from exc
    if parsed.tzinfo is None:
        raise ValueError("provider time must include a timezone")
    return value


def _parse_session_reset_status(data: dict | None) -> dict | None:
    """Strictly parse Anthropic's private session-reset status, else fail closed."""
    if not isinstance(data, dict):
        return None
    block = data.get(_SESSION_RESET_PROGRAM)
    if not isinstance(block, dict):
        return None
    eligible = block.get("eligible")
    available = block.get("available")
    if not isinstance(eligible, bool) or not isinstance(available, bool):
        return None
    reason = block.get("ineligible_reason")
    if reason is not None and (
        not isinstance(reason, str) or reason not in _SESSION_RESET_REASONS
    ):
        return None
    arm = block.get("arm")
    if arm is not None and (not isinstance(arm, str) or arm not in _SESSION_RESET_ARMS):
        return None
    in_experiment = block.get("in_experiment")
    if in_experiment is not None and not isinstance(in_experiment, bool):
        return None
    resets_per_week = block.get("resets_per_week")
    if resets_per_week is not None and (
        isinstance(resets_per_week, bool)
        or not isinstance(resets_per_week, (int, float))
        or not float(resets_per_week).is_integer()
        or resets_per_week < 0
    ):
        return None
    try:
        next_available_at = _strict_provider_time(block.get("next_available_at"))
        weekly_resets_at = _strict_provider_time(block.get("weekly_resets_at"))
    except ValueError:
        return None
    return {
        "eligible": eligible,
        "available": available,
        "claimable": bool(eligible and available and arm == "reset"),
        "ineligible_reason": reason,
        "next_available_at": next_available_at,
        "weekly_resets_at": weekly_resets_at,
        "resets_per_week": int(resets_per_week) if resets_per_week is not None else None,
        # Private protocol detail used only inside ccx's mutation gate.
        "_arm": arm,
    }


def session_reset_status(data: dict | None) -> dict | None:
    """Stable public projection of the private provider reset entitlement."""
    parsed = _parse_session_reset_status(data)
    if parsed is None:
        return None
    return {k: v for k, v in parsed.items() if not k.startswith("_")}


def _parse_reset_result(value: object) -> dict | None:
    if not isinstance(value, dict):
        return None
    result = value.get("result")
    if not isinstance(result, str) or result not in _SESSION_RESET_RESULTS:
        return None
    try:
        next_available_at = _strict_provider_time(value.get("next_available_at"))
        weekly_resets_at = _strict_provider_time(value.get("weekly_resets_at"))
    except ValueError:
        return None
    return {
        "result": result,
        "next_available_at": next_available_at,
        "weekly_resets_at": weekly_resets_at,
    }


def reset_five_hour_limit(*, timeout: float = _SESSION_RESET_TIMEOUT_S) -> dict:
    """Claim Anthropic's provider-reported weekly 5-hour reset exactly once.

    Eligibility is re-read live immediately before the mutation. A stale cache,
    the locally displayed 5-hour percentage, or a locally calculated date can
    never authorize this call. The mutation itself is never retried. After any
    attempted mutation, one read-only usage fetch reconciles current state; an
    ambiguous mutation error stays an error even if that reconciliation succeeds.

    This claims Claude Code's own reset offer (a limited rollout) through its
    account endpoint, which isn't a public API; disabled without
    CCX_ENABLE_UNOFFICIAL_RESET=1. The opt-in check precedes all network I/O.
    """
    if not opt_in_enabled(UNOFFICIAL_RESET_ENV):
        raise InvalidRequest(
            "reset_five_hour_limit: this claims Claude Code's five-hour reset "
            "offer through its account endpoint, which isn't a public API and can "
            f"change without notice; it is disabled by default. Set {UNOFFICIAL_RESET_ENV}=1 to "
            "opt in."
        )
    if _usage_source() != "oauth":
        raise InvalidRequest(
            "reset_five_hour_limit: also select the machine-wide oauth usage "
            "source with ccx usage --engine claude --set-source oauth"
        )
    preflight = _request_usage_live(
        timeout=min(8.0, timeout), at_wall=True, skip_spend=True
    )
    parsed_status = _parse_session_reset_status(preflight)
    public_status = session_reset_status(preflight)
    if parsed_status is None or not parsed_status["claimable"]:
        return {
            "result": "not_available",
            "status": public_status,
            "refreshed": False,
        }

    token = oauth_token()
    organization = oauth_organization_uuid()
    if not token or not organization:
        return {"result": "auth_error", "status": public_status, "refreshed": False}

    encoded_org = urllib.parse.quote(organization, safe="")
    url = f"https://api.anthropic.com/api/organizations/{encoded_org}/reset_rate_limits"
    body = json.dumps({"program": _SESSION_RESET_PROGRAM}, separators=(",", ":")).encode()
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers=oauth_headers(token),
    )
    outcome: dict
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            if not (200 <= response.status < 300):
                outcome = {"result": "error"}
            else:
                try:
                    decoded = json.loads(response.read().decode())
                except ValueError:
                    decoded = None
                outcome = _parse_reset_result(decoded) or {"result": "error"}
    except urllib.error.HTTPError as exc:
        if exc.code == 429:
            outcome = {"result": "rate_limited"}
        elif exc.code in (401, 403):
            outcome = {"result": "auth_error"}
        else:
            outcome = {"result": "error"}
    except (OSError, urllib.error.URLError, TimeoutError):
        outcome = {"result": "error"}

    # Reconcile with one safe read. This is not a mutation retry and never
    # upgrades an ambiguous POST failure into a guessed success.
    refreshed = _request_usage_live(
        timeout=min(8.0, timeout), at_wall=True, skip_spend=False
    )
    outcome["status"] = (
        session_reset_status(refreshed) if refreshed is not None else public_status
    )
    outcome["refreshed"] = refreshed is not None
    return outcome


def read_usage_cached(stale_limit: float | None = None) -> dict | None:
    """Nonblocking, cache-only source-aware read for the host's select loop."""
    try:
        data = _matching_cache(_usage_source())
    except InvalidRequest:
        return None
    if data is None or (stale_limit is not None and _is_stale(data, stale_limit)):
        return None
    return data if _is_usage_payload(data) else None


def _valid_pct(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) and 0.0 <= number <= 100.0 else None


def _metric_util(data: dict, key: str) -> float | None:
    """A missing/null/invalid metric is unknown, never an assumed zero."""
    field = CAP_METRICS.get(key)
    if field is None or not data:
        return None
    block = data.get(field)
    util = _valid_pct(block.get("utilization")) if isinstance(block, dict) else None
    if util is not None:
        return util
    if key == "opus":
        for quota in scoped_weekly_limits(data):
            if quota["model"].casefold() == "opus":
                return quota["utilization"]
    return None


def unknown_cap_metrics(data: dict | None, caps: dict | None) -> list[str]:
    """Explain fail-open omissions without exposing an arbitrary private payload."""
    if not caps:
        return []
    if data is None or (isinstance(data.get("_ccx"), dict) and _is_stale(data)):
        return [key for key, limit in caps.items() if limit is not None]
    return [key for key, limit in caps.items()
            if limit is not None and _metric_util(data, key) is None]


def cap_tripped(data: dict | None, caps: dict | None):
    """Return first known exceeded cap; held/stale/unknown signals fail open."""
    if not data or not caps:
        return None
    if isinstance(data.get("_ccx"), dict) and _is_stale(data):
        return None
    for key, cap in caps.items():
        if cap is None:
            continue
        util = _metric_util(data, key)
        if util is not None and util >= float(cap):
            return (key, util, float(cap))
    return None


_SUMMARY_QUOTA_FIELDS = ("five_hour", "seven_day")
_LEGACY_MODEL_FIELDS = ("seven_day_opus", "seven_day_sonnet")

def _summary_block(block) -> dict | None:
    if block is None:
        return None
    if not isinstance(block, dict):
        return None
    out = {}
    if "utilization" in block:
        util = block.get("utilization")
        validated = _valid_pct(util)
        if validated is not None:
            out["utilization"] = validated
    resets_at = block.get("resets_at")
    if _parse_epoch(resets_at) is not None:
        out["resets_at"] = resets_at
    return out or None


def scoped_weekly_limits(data: dict) -> list[dict]:
    """Normalize the currently relevant per-model weekly quota catalog.

    The ``limits`` catalog describes model-specific quotas. Consumers follow
    that catalog so their UI does not depend on a fixed set of model names.
    """
    out = []
    limits = data.get("limits") if isinstance(data, dict) else None
    for item in limits if isinstance(limits, list) else ():
        if not isinstance(item, dict) or item.get("kind") != "weekly_scoped":
            continue
        scope = item.get("scope")
        model = scope.get("model") if isinstance(scope, dict) else None
        name = model.get("display_name") if isinstance(model, dict) else None
        percent = _valid_pct(item.get("percent"))
        if not isinstance(name, str) or not name or percent is None:
            continue
        quota = {
            "model": name,
            "utilization": percent,
        }
        model_id = model.get("id")
        if isinstance(model_id, str) and model_id:
            quota["model_id"] = model_id
        resets_at = item.get("resets_at")
        if _parse_epoch(resets_at) is not None:
            quota["resets_at"] = resets_at
        if isinstance(item.get("is_active"), bool):
            quota["is_active"] = item["is_active"]
        out.append(quota)
    # The control reply's model_scoped[] is additive. Never synthesize limits[]
    # in the cache; only add missing models to the in-memory presentation.
    scoped = data.get("model_scoped") if isinstance(data, dict) else None
    seen_names = {item["model"].casefold() for item in out}
    if isinstance(scoped, list):
        for item in scoped:
            if not isinstance(item, dict):
                continue
            name, util = item.get("display_name"), _valid_pct(item.get("utilization"))
            if not isinstance(name, str) or not name or util is None or name.casefold() in seen_names:
                continue
            quota = {"model": name, "utilization": util}
            if _parse_epoch(item.get("resets_at")) is not None:
                quota["resets_at"] = item["resets_at"]
            out.append(quota)
            seen_names.add(name.casefold())
    return out


def usage_summary(data: dict) -> dict:
    """Compact public quota summary: stable actionable fields only.

    Includes five_hour, seven_day, provider-announced model_quotas, and
    extra_usage (when enabled). Non-null legacy per-model fields are retained
    for cached payload compatibility. Omits all other raw API keys."""
    meta = data.get("_ccx") or {}
    if isinstance(meta, dict) and meta.get("unparsed"):
        return {"unparsed": True, "raw": meta.get("raw"), "error": meta.get("error")}
    out = {}
    for key in _SUMMARY_QUOTA_FIELDS:
        if key not in data:
            continue
        block = _summary_block(data.get(key))
        if block is not None:
            out[key] = block
    model_quotas = scoped_weekly_limits(data)
    if model_quotas:
        out["model_quotas"] = model_quotas
    else:
        # Old caches may predate the dynamic limits catalog.  Preserve actual
        # legacy quota blocks, but omit null retired fields rather than showing
        # misleading Opus/Sonnet 0% gauges.
        for key in _LEGACY_MODEL_FIELDS:
            block = data.get(key)
            if not isinstance(block, dict):
                continue
            summary = _summary_block(block)
            if summary is not None:
                out[key] = summary
    xu = data.get("extra_usage")
    if isinstance(xu, dict) and xu.get("is_enabled"):
        extra = {"enabled": True}
        for key in ("utilization", "used_credits", "monthly_limit", "currency"):
            value = xu.get(key)
            if value is not None:
                extra[key] = value
        out["extra_usage"] = extra
    return out


def _pct(s: str) -> float:
    """argparse type for a 0–100 utilization percentage."""
    try:
        v = float(s)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{s!r} is not a number")
    if not (0.0 <= v <= 100.0):
        raise argparse.ArgumentTypeError(f"cap must be between 0 and 100 (got {v})")
    return v

def _add_cap_flags(p):
    p.add_argument("--cap-5h", type=_pct, default=None, metavar="PCT",
                   help="Refuse to start when 5-hour usage is already at PCT%% (0–100), and stop "
                        "the turn if it reaches PCT%% while running (exit 7).")
    p.add_argument("--cap-7d", type=_pct, default=None, metavar="PCT",
                   help="The same for 7-day usage.")
    p.add_argument("--cap-opus", type=_pct, default=None, metavar="PCT",
                   help="Claude only: the same for 7-day Opus usage.")

def _caps_from_args(args) -> dict | None:
    caps = {}
    for k, attr in (("5h", "cap_5h"), ("7d", "cap_7d"), ("opus", "cap_opus")):
        v = getattr(args, attr, None)
        if v is not None:
            caps[k] = v
    return caps or None

def _merge_caps(caps_list) -> dict | None:
    """Combine the caps of several sends that batch into one turn — strictest wins
    (lowest threshold per metric, union of metrics)."""
    merged = {}
    for c in caps_list:
        if not c:
            continue
        for k, v in c.items():
            if v is None:
                continue
            merged[k] = min(v, merged[k]) if k in merged else v
    return merged or None
