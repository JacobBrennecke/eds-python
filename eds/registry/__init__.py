"""PARITY: internal/registry/{registry.go,api.go} — the API-backed schema registry.

A 3-tier read (in-memory cache → tracker → API). The constructor fetches the full schema once (the only
retrying call), re-keys it BY TABLE (Go's data is keyed by API object name), and seeds the tracker +
cache. THE SEED ASYMMETRY: both cache and tracker are seeded with TTL 0, but 0 means *expire-immediately*
for the cache and *persist forever* for the tracker — so every seeded cache entry is dead on arrival and
the first read falls through to the tracker (faithful; see DEVIATIONS notes in the cache/tracker modules).

Kept synchronous (Go's GetSchema blocks on http.DefaultClient). DEVIATIONS: registry-sorttable-collision-order
(deterministic vs Go random map order; behavior-neutral for unique tables); the fetch uses the UN-prefixed
logger for HttpRetry (api.go assigns the "[tracker]" prefix only after a 200 — the C# port diverged here).
"""

from __future__ import annotations

import json
import os
from typing import Any

from eds.schema import Schema, SchemaMap
from eds.tracker import Tracker
from eds.util import gojson
from eds.util.cache import InMemoryCache, new_cache
from eds.util.http import HttpRetry
from eds.util.logger import Logger

_PREFIX = "registry:"
_DEFAULT_CACHE = 86400.0  # 24h
_SWEEP_INTERVAL = 3600.0  # NewCache(ctx, time.Hour)


def _schema_cache_key(table: str, version: str) -> str:
    return _PREFIX + table + "-" + version


def _version_cache_key(table: str) -> str:
    return _PREFIX + table + ":version"


def _sort_table(by_object: dict[str, Schema]) -> tuple[SchemaMap, dict[str, str]]:
    """PARITY: registry.go sortTable — re-key the by-object schema map by table, and build the reverse
    table→object map."""
    kv: SchemaMap = {}
    otm: dict[str, str] = {}
    for obj, d in by_object.items():
        otm[d.table] = obj
        kv[d.table] = d
    return kv, otm


class APIRegistry:
    """PARITY: api.go APIRegistry. Implements the eds.schema.SchemaRegistry protocol."""

    def __init__(
        self,
        logger: Logger,
        api_url: str,
        user_agent: str,
        tracker: Tracker | None,
        cache: InMemoryCache,
        session: Any,
        schema: SchemaMap,
        objects: dict[str, str],
    ) -> None:
        self._logger = logger
        self._api_url = api_url
        self._user_agent = user_agent
        self._tracker = tracker
        self._cache = cache
        self._session = session
        self._schema = schema
        self._objects = objects
        self._closed = False

    def get_latest_schema(self) -> SchemaMap:
        """PARITY: GetLatestSchema."""
        return self._schema

    def get_table_version(self, table: str) -> tuple[bool, str]:
        """PARITY: GetTableVersion — cache → tracker (re-cached for 24h on hit)."""
        key = _version_cache_key(table)
        found, val = self._cache.get(key)
        if found:
            return True, val  # type: ignore[return-value]
        if self._tracker is not None:
            found, version = self._tracker.get_key(key)
            if found:
                self._cache.set(key, version, _DEFAULT_CACHE)
                return True, version
        return False, ""

    def set_table_version(self, table: str, version: str) -> None:
        """PARITY: SetTableVersion — cache (24h) then tracker (persistent)."""
        key = _version_cache_key(table)
        self._cache.set(key, version, _DEFAULT_CACHE)
        if self._tracker is not None:
            self._tracker.set_key(key, version, 0)
        self._logger.trace("set table: %s version: %s", table, version)

    def get_schema(self, table: str, version: str) -> Schema:
        """PARITY: GetSchema — cache → tracker → API fallback (the fallback is NOT retried)."""
        key = _schema_cache_key(table, version)
        found, val = self._cache.get(key)
        if found:
            return val  # type: ignore[return-value]
        if self._tracker is not None:
            found, valstr = self._tracker.get_key(key)
            if found:
                schema = self._decode_schema(valstr, table, version)
                self._cache.set(key, schema, _DEFAULT_CACHE)
                return schema

        # Fall back to the API (bare request, no retry — PARITY: http.DefaultClient.Do).
        obj = self._objects.get(table) or table
        url = self._api_url + "/v3/schema/" + obj + "/" + version
        try:
            resp = self._session.request("GET", url, headers={"User-Agent": self._user_agent})
        except Exception as e:  # PARITY: Go wraps transport errors as "error fetching schema: %s"
            raise ValueError(f"error fetching schema: {e}") from e
        status, body = _read(resp)
        if status != 200:
            raise ValueError(
                f"error fetching schema for table: {table}, modelVersion: {version}. "
                f"status code was: {status}, {body}"
            )
        schema = self._decode_schema(body, table, version)
        self._cache.set(key, schema, _DEFAULT_CACHE)
        if self._tracker is not None:
            self._tracker.set_key(key, gojson.stringify(schema), 0)
        self._logger.trace("get schema returned")
        return schema

    def close(self) -> None:
        """PARITY: Close — close the cache (idempotent)."""
        if self._closed:
            return
        self._closed = True
        self._cache.close()

    @staticmethod
    def _decode_schema(body: str, table: str, version: str) -> Schema:
        # PARITY: Go json.Decode into a Schema struct — null → zero Schema; a wrong-type body errors.
        try:
            parsed = json.loads(body)
        except ValueError as e:
            raise ValueError(f"error decoding schema for table: {table}, modelVersion: {version}: {e}") from e
        if parsed is None:
            return Schema()
        if not isinstance(parsed, dict):
            raise ValueError(
                f"error decoding schema for table: {table}, modelVersion: {version}: "
                f"cannot unmarshal {type(parsed).__name__} into Schema"
            )
        return Schema.from_dict(parsed)


def new_api_registry(
    logger: Logger, api_url: str, eds_version: str, tracker: Tracker | None, *, session: Any = None
) -> APIRegistry:
    """PARITY: NewAPIRegistry."""
    return _new_api_registry_modified(logger, api_url, eds_version, tracker, "", session)


def new_api_registry_private(
    logger: Logger, api_url: str, eds_version: str, tracker: Tracker | None, *, session: Any = None
) -> APIRegistry:
    """PARITY: NewAPIRegistryPrivate — adds the /private?apikey=<env> modifier."""
    apikey = os.environ.get("SM_API_PRIVATE_SCHEMA_KEY", "")
    if not apikey:
        raise ValueError("SM_API_PRIVATE_SCHEMA_KEY is not set; see the backend for details")
    return _new_api_registry_modified(logger, api_url, eds_version, tracker, "/private?apikey=" + apikey, session)


def _new_api_registry_modified(
    logger: Logger, api_url: str, eds_version: str, tracker: Tracker | None, url_modifier: str, session: Any
) -> APIRegistry:
    """PARITY: newAPIRegistryModified."""
    if session is None:
        session = _DefaultSession()
    user_agent = "Shopmonkey EDS Server/" + eds_version
    url = api_url + "/v3/schema" + url_modifier

    # The ONLY retrying network call (unbounded 5xx/429). PARITY: the un-prefixed logger is used here.
    try:
        resp = HttpRetry(
            lambda: session.request("GET", url, headers={"User-Agent": user_agent}),
            method="GET",
            url=url,
            logger=logger,
        ).do()
    except Exception as e:  # PARITY: Go wraps transport errors as "error fetching schema: %s"
        raise ValueError(f"error fetching schema: {e}") from e
    status, body = _read(resp)
    if status != 200:
        raise _fetch_error(status, body)

    by_object = _decode_schema_map(body)
    schema, objects = _sort_table(by_object)

    # PARITY: the cache (and its sweeper) is created only after the 200, so a failed fetch never starts it.
    cache = new_cache(_SWEEP_INTERVAL)
    for s in schema.values():
        key = _schema_cache_key(s.table, s.model_version)
        if tracker is not None:
            tracker.set_key(key, gojson.stringify(s), 0)
            tracker.set_key(_version_cache_key(s.table), s.model_version, 0)
        cache.set(key, s, 0)  # PARITY: TTL 0 = dead-on-arrival in the cache (the seed asymmetry)

    return APIRegistry(
        logger.with_prefix("[tracker]"), api_url, user_agent, tracker, cache, session, schema, objects
    )


def _decode_schema_map(body: str) -> dict[str, Schema]:
    """PARITY: Go json.Decode into SchemaMap — null → empty map; a wrong-type body (or non-object value)
    errors as "error decoding schema". (Go's encoding/json text can't be reproduced verbatim, but the
    "error decoding schema" prefix + the ValueError type match, so callers catching ValueError still catch it.)"""
    try:
        raw = json.loads(body)
    except ValueError as e:
        raise ValueError(f"error decoding schema: {e}") from e
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ValueError(f"error decoding schema: cannot unmarshal {type(raw).__name__} into schema map")
    result: dict[str, Schema] = {}
    for obj, v in raw.items():
        if not isinstance(v, dict):
            raise ValueError(f"error decoding schema: cannot unmarshal {type(v).__name__} into Schema")
        result[obj] = Schema.from_dict(v)
    return result


def _fetch_error(status: int, body: str) -> ValueError:
    message = ""
    try:
        parsed = json.loads(body)
        if isinstance(parsed, dict) and isinstance(parsed.get("message"), str):
            message = parsed["message"]
    except ValueError:
        pass
    if message:
        return ValueError(f"error fetching schema: {message}")
    return ValueError(f"error fetching schema: {status}: {body}")


def _read(resp: Any) -> tuple[int, str]:
    try:
        return resp.status_code, resp.text
    finally:
        close = getattr(resp, "close", None)
        if callable(close):
            close()


class _DefaultSession:
    """Production transport: ``requests`` with no timeout (PARITY: Go http.DefaultClient has none)."""

    def request(self, method: str, url: str, headers: dict | None = None) -> Any:
        import requests

        return requests.request(method, url, headers=headers, timeout=None)
