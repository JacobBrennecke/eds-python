"""PARITY: internal/registry — vectors ported from the C# ApiRegistryTests + Go api.go error strings."""

from __future__ import annotations

from urllib.parse import urlsplit

import pytest

from eds.registry import new_api_registry, new_api_registry_private
from eds.tracker import new_tracker
from eds.util.logger import ConsoleLogger, LogLevel

_ORDER_V1 = (
    '{"properties":{"id":{"type":"string"},"name":{"type":"string"}},'
    '"required":["id"],"primaryKeys":["id"],"table":"order","modelVersion":"v1"}'
)
_ORDER_V2 = (
    '{"properties":{"id":{"type":"string"},"name":{"type":"string"},"note":{"type":"string"}},'
    '"required":["id"],"primaryKeys":["id"],"table":"order","modelVersion":"v2"}'
)
_SCHEMA_MAP = '{"OrderObject":' + _ORDER_V1 + "}"  # keyed by OBJECT name (registry re-keys by table)


class _Resp:
    def __init__(self, status: int, body: str) -> None:
        self.status_code = status
        self.text = body

    def close(self) -> None:
        pass


class _FakeSession:
    def __init__(self, responses: dict[str, tuple[int, str]]) -> None:
        self._responses = responses
        self.paths: list[str] = []
        self.paths_and_queries: list[str] = []
        self.last_user_agent: str | None = None

    def request(self, method: str, url: str, headers: dict | None = None) -> _Resp:
        sp = urlsplit(url)
        path = sp.path
        pq = path + ("?" + sp.query if sp.query else "")
        self.paths.append(path)
        self.paths_and_queries.append(pq)
        if headers:
            self.last_user_agent = headers.get("User-Agent")
        status, body = self._responses.get(pq) or self._responses.get(path) or (404, "not found")
        return _Resp(status, body)


def _silent() -> ConsoleLogger:
    import io

    return ConsoleLogger(LogLevel.ERROR, output=io.StringIO())


def _registry(tmp_path, responses, *, private=False):
    session = _FakeSession(responses)
    tracker = new_tracker(str(tmp_path))
    factory = new_api_registry_private if private else new_api_registry
    return factory(_silent(), "http://api.test", "1.0", tracker, session=session), session, tracker


def test_constructor_rekeys_by_table_and_sets_user_agent(tmp_path) -> None:
    reg, session, tracker = _registry(tmp_path, {"/v3/schema": (200, _SCHEMA_MAP)})
    try:
        latest = reg.get_latest_schema()
        assert "order" in latest
        assert "OrderObject" not in latest
        assert latest["order"].table == "order"
        assert session.last_user_agent == "Shopmonkey EDS Server/1.0"
    finally:
        reg.close()
        tracker.close()


def test_get_schema_from_tracker_no_api_call(tmp_path) -> None:
    reg, session, tracker = _registry(tmp_path, {"/v3/schema": (200, _SCHEMA_MAP)})
    try:
        s = reg.get_schema("order", "v1")
        assert s.table == "order"
        assert s.model_version == "v1"
        assert session.paths == ["/v3/schema"]  # only the constructor fetch; no per-schema call
    finally:
        reg.close()
        tracker.close()


def test_get_schema_api_fallback_uses_object_name(tmp_path) -> None:
    reg, session, tracker = _registry(
        tmp_path, {"/v3/schema": (200, _SCHEMA_MAP), "/v3/schema/OrderObject/v2": (200, _ORDER_V2)}
    )
    try:
        s = reg.get_schema("order", "v2")
        assert s.model_version == "v2"
        assert "/v3/schema/OrderObject/v2" in session.paths  # object name from the reverse map, not "order"
    finally:
        reg.close()
        tracker.close()


def test_get_schema_fallback_non_200_raises(tmp_path) -> None:
    reg, session, tracker = _registry(tmp_path, {"/v3/schema": (200, _SCHEMA_MAP)})
    try:
        with pytest.raises(ValueError, match="status code was: 404, not found"):
            reg.get_schema("order", "v2")
    finally:
        reg.close()
        tracker.close()


def test_get_and_set_table_version(tmp_path) -> None:
    reg, _session, tracker = _registry(tmp_path, {"/v3/schema": (200, _SCHEMA_MAP)})
    try:
        assert reg.get_table_version("order") == (True, "v1")
        assert reg.get_table_version("missing") == (False, "")
        reg.set_table_version("order", "v9")
        assert reg.get_table_version("order") == (True, "v9")
    finally:
        reg.close()
        tracker.close()


def test_constructor_non_200_raises_with_message(tmp_path) -> None:
    session = _FakeSession({"/v3/schema": (500, '{"message":"boom"}')})
    tracker = new_tracker(str(tmp_path))
    try:
        with pytest.raises(ValueError, match="error fetching schema: boom"):
            new_api_registry(_silent(), "http://api.test", "1.0", tracker, session=session)
    finally:
        tracker.close()


def test_constructor_non_200_no_message(tmp_path) -> None:
    # Use 500 (NOT in the retry set) — a retryable status like 503 would loop forever against a fake
    # that always returns it (the §8.8 unbounded-retry path; it would hang in Go too).
    session = _FakeSession({"/v3/schema": (500, "down")})
    tracker = new_tracker(str(tmp_path))
    try:
        with pytest.raises(ValueError, match="error fetching schema: 500: down"):
            new_api_registry(_silent(), "http://api.test", "1.0", tracker, session=session)
    finally:
        tracker.close()


def test_private_uses_apikey_query(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("SM_API_PRIVATE_SCHEMA_KEY", "secret123")
    reg, session, tracker = _registry(
        tmp_path, {"/v3/schema/private?apikey=secret123": (200, _SCHEMA_MAP)}, private=True
    )
    try:
        assert "order" in reg.get_latest_schema()
        assert "/v3/schema/private?apikey=secret123" in session.paths_and_queries
    finally:
        reg.close()
        tracker.close()


class _BoomSession:
    """request() raises a NON-connection error (so HttpRetry does not retry it)."""

    def request(self, method: str, url: str, headers: dict | None = None):
        raise RuntimeError("dns failure")


class _OkThenBoomSession:
    """Constructor fetch succeeds; the get_schema fallback raises a transport error."""

    def __init__(self) -> None:
        self.n = 0

    def request(self, method: str, url: str, headers: dict | None = None):
        self.n += 1
        if self.n == 1:
            return _Resp(200, _SCHEMA_MAP)
        raise OSError("connection refused")


def test_constructor_transport_error_wrapped(tmp_path) -> None:
    tracker = new_tracker(str(tmp_path))
    try:
        with pytest.raises(ValueError, match="error fetching schema"):
            new_api_registry(_silent(), "http://api.test", "1.0", tracker, session=_BoomSession())
    finally:
        tracker.close()


def test_constructor_wrong_shape_body_wrapped(tmp_path) -> None:
    # Valid JSON, wrong shape -> Go's "error decoding schema" (ValueError, not AttributeError).
    session = _FakeSession({"/v3/schema": (200, "[1,2,3]")})
    tracker = new_tracker(str(tmp_path))
    try:
        with pytest.raises(ValueError, match="error decoding schema"):
            new_api_registry(_silent(), "http://api.test", "1.0", tracker, session=session)
    finally:
        tracker.close()


def test_constructor_null_body_is_empty(tmp_path) -> None:
    # PARITY: Go decodes a JSON null into a nil map (no error) -> empty registry.
    session = _FakeSession({"/v3/schema": (200, "null")})
    tracker = new_tracker(str(tmp_path))
    try:
        reg = new_api_registry(_silent(), "http://api.test", "1.0", tracker, session=session)
        assert reg.get_latest_schema() == {}
        reg.close()
    finally:
        tracker.close()


def test_get_schema_fallback_transport_error_wrapped(tmp_path) -> None:
    session = _OkThenBoomSession()
    tracker = new_tracker(str(tmp_path))
    try:
        reg = new_api_registry(_silent(), "http://api.test", "1.0", tracker, session=session)
        with pytest.raises(ValueError, match="error fetching schema"):
            reg.get_schema("order", "v2")  # not seeded -> fallback -> transport error
        reg.close()
    finally:
        tracker.close()


def test_private_without_env_raises(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("SM_API_PRIVATE_SCHEMA_KEY", raising=False)
    tracker = new_tracker(str(tmp_path))
    try:
        with pytest.raises(ValueError, match="SM_API_PRIVATE_SCHEMA_KEY"):
            new_api_registry_private(_silent(), "http://api.test", "1.0", tracker, session=_FakeSession({}))
    finally:
        tracker.close()
