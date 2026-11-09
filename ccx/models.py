"""Read-only, engine-neutral model catalogs with private per-engine caching."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
import hashlib
from pathlib import Path
import time

from ccx import constants
from ccx.claude.oauth import oauth_organization_uuid
from ccx.claude.usage_control import request_models
from ccx.codex.runtime import list_model_entries
from ccx.errors import InvalidRequest
from ccx.system.private_json import _atomic_private_json, _read_private_json
from ccx.types import ModelCatalog, ModelInfo

_CACHE_TTL_S = 24 * 60 * 60
_CACHE_VERSION = 1
_ENGINES = ("claude", "codex")


def _text(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _efforts(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return list(dict.fromkeys(entry for entry in value
                              if isinstance(entry, str) and entry))


def normalize_claude_models(entries: object) -> list[ModelInfo]:
    """Project Claude initialize's models only, with per-model capabilities."""
    if not isinstance(entries, list):
        return []
    models = []
    for item in entries:
        if not isinstance(item, dict):
            continue
        ident = _text(item.get("value"))
        resolved = _text(item.get("resolvedModel"))
        name = _text(item.get("displayName"))
        if not (ident and resolved and name):
            continue
        models.append(ModelInfo(
            id=ident,
            resolved=resolved,
            display_name=name,
            description=(item.get("description")
                         if isinstance(item.get("description"), str) else None),
            alias=ident != resolved,
            default=ident == "default",
            efforts=(_efforts(item.get("supportedEffortLevels"))
                     if item.get("supportsEffort") is True else []),
            default_effort=None,
            features={
                "fast": item.get("supportsFastMode") is True,
                "auto": item.get("supportsAutoMode") is True,
                "thinkingOff": item.get("supportsThinkingOff") is True,
            },
        ))
    return models


def normalize_codex_models(entries: object) -> list[ModelInfo]:
    """Project model/list items; Codex's model ID is the --model spelling."""
    if not isinstance(entries, list):
        return []
    models = []
    seen = set()
    for item in entries:
        if not isinstance(item, dict) or item.get("hidden") is True:
            continue
        ident = _text(item.get("model")) or _text(item.get("id"))
        if not ident or ident in seen:
            continue
        seen.add(ident)
        efforts = item.get("supportedReasoningEfforts")
        levels = (_efforts([entry.get("reasoningEffort") for entry in efforts
                            if isinstance(entry, dict)])
                  if isinstance(efforts, list) else [])
        models.append(ModelInfo(
            id=ident,
            resolved=ident,
            display_name=_text(item.get("displayName")) or ident,
            description=(item.get("description")
                         if isinstance(item.get("description"), str) else None),
            alias=False,
            default=item.get("isDefault") is True,
            efforts=levels,
            default_effort=_text(item.get("defaultReasoningEffort")),
            features={},
        ))
    return models


def _cache_path(engine: str) -> Path:
    return constants.CCX_ROOT / f"models-{engine}.json"


def _local_claude_version() -> str | None:
    # Bypass oauth.py's process-wide lru_cache: a live CcxClient must notice
    # when the installed Claude Code binary changes beneath it.
    from ccx.claude.oauth import claude_code_version
    return claude_code_version.__wrapped__()


def _cache_key(engine: str) -> dict:
    if engine == "codex":
        # No cheap native Codex version helper; this engine uses the TTL only.
        return {}
    org = oauth_organization_uuid()
    return {
        "version": _local_claude_version(),
        "orgDigest": hashlib.sha256(org.encode()).hexdigest() if org else None,
    }


def _read_cache(engine: str) -> tuple[ModelCatalog | None, dict]:
    try:
        data = _read_private_json(_cache_path(engine))
        if not isinstance(data, dict) or data.get("version") != _CACHE_VERSION:
            return None, {}
        catalog = ModelCatalog.from_dict(data["catalog"])
        if catalog.engine != engine:
            return None, {}
        key = data.get("key")
        return catalog, key if isinstance(key, dict) else {}
    except (OSError, ValueError, KeyError, TypeError, UnicodeError):
        return None, {}


def _fresh(catalog: ModelCatalog, saved_key: dict, current_key: dict) -> bool:
    if catalog.engine == "claude":
        if saved_key.get("orgDigest") != current_key.get("orgDigest"):
            return False
        # If the installed version cannot be read, only TTL/org can be checked.
        local_version = current_key.get("version")
        if local_version is not None and catalog.engine_version != local_version:
            return False
    try:
        stamped = datetime.fromisoformat(catalog.fetched_at.replace("Z", "+00:00"))
        if stamped.tzinfo is None:
            return False
        age = time.time() - stamped.timestamp()
    except (ValueError, OverflowError):
        return False
    return 0 <= age < _CACHE_TTL_S


def _probe(engine: str) -> tuple[str | None, list[ModelInfo]]:
    if engine == "claude":
        reply = request_models()
        if reply.get("kind") != "success":
            raise RuntimeError(reply.get("error") or "initialize unavailable")
        body = reply["body"]
        return _text(body.get("claude_code_version")), normalize_claude_models(body.get("models"))
    # Account-level catalog, independent of the calling project's cwd.
    return None, normalize_codex_models(list_model_entries(cwd=str(Path.home())))


def _builtin(engine: str, note: str) -> ModelCatalog:
    # Only Claude has known aliases; inventing Codex model names would be wrong.
    models = ([ModelInfo(
        id=alias, resolved=None, display_name=alias.title(),
        description=None, alias=True, default=False, efforts=[],
        default_effort=None, features={},
    ) for alias in constants.MODEL_ALIASES] if engine == "claude" else [])
    return ModelCatalog(
        engine=engine, engine_version=None, source="builtin",
        fetched_at=_now(), note=note, models=models,
    )


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _catalog(engine: str, *, refresh: bool) -> ModelCatalog:
    cached, saved_key = _read_cache(engine)
    try:
        current_key = _cache_key(engine)
    except Exception:
        # Broken or unavailable local identity metadata cannot block the other
        # engine's catalog (or the provider read for this engine).
        current_key = {}
    if not refresh and cached is not None and _fresh(cached, saved_key, current_key):
        return replace(cached, source="cache")

    try:
        version, models = _probe(engine)
        if not models:
            raise RuntimeError("no valid model entries")
    except Exception as error:
        note = f"{engine.capitalize()} model list unavailable: {error}"
        if cached is not None:
            return replace(cached, source="stale", note=note)
        return _builtin(engine, note)

    result = ModelCatalog(
        engine=engine, engine_version=version, source="live",
        fetched_at=_now(), note=None, models=models,
    )
    try:
        _atomic_private_json(_cache_path(engine), {
            "version": _CACHE_VERSION,
            "key": ({"version": version, "orgDigest": current_key.get("orgDigest")}
                    if engine == "claude" else {}),
            "catalog": result.as_dict(),
        })
    except OSError:
        # A live listing remains useful if the private cache cannot be saved.
        pass
    return result


def read_models(*, engine: str | None = None, refresh: bool = False) -> list[ModelCatalog]:
    """Return one or both model catalogs; probe failures degrade per engine."""
    if engine is not None and engine not in _ENGINES:
        raise InvalidRequest("engine must be claude or codex")
    return [_catalog(name, refresh=refresh)
            for name in ((engine,) if engine else _ENGINES)]
