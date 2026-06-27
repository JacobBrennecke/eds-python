"""PARITY: internal/api/api.go — vectors from the C# ApiTests."""

from __future__ import annotations

import pytest

from eds.api import (
    DriverMeta,
    EdsSession,
    EnrollResponse,
    SessionEnd,
    SessionEndResponse,
    SessionStart,
    SessionStartResponse,
    get_api_url,
)
from eds.util.gojson import stringify


@pytest.mark.parametrize(
    ("letter", "url"),
    [
        ("P", "https://api.shopmonkey.cloud"),
        ("S", "https://sandbox-api.shopmonkey.cloud"),
        ("E", "https://edge-api.shopmonkey.cloud"),
        ("L", "http://localhost:3101"),
    ],
)
def test_get_api_url(letter: str, url: str) -> None:
    assert get_api_url(letter) == url


@pytest.mark.parametrize("bad", ["X", "p", "s", ""])  # case-sensitive, unknown, empty
def test_get_api_url_invalid(bad: str) -> None:
    with pytest.raises(ValueError, match="invalid code"):
        get_api_url(bad)


def test_session_start_minimal() -> None:
    s = SessionStart(version="1.0", hostname="h", ip_address="1.2.3.4", machine_id="mid", server_id="sid")
    assert stringify(s) == (
        '{"version":"1.0","hostname":"h","ipAddress":"1.2.3.4","machineId":"mid","osinfo":null,"serverId":"sid"}'
    )


def test_session_start_with_driver_and_company_ids() -> None:
    s = SessionStart(
        version="1.0", hostname="h", ip_address="1.2.3.4", machine_id="mid", server_id="sid",
        driver=DriverMeta(id="d1", name="Postgres", description="pg", url="masked"),
        company_ids=["c1"],
    )
    assert stringify(s) == (
        '{"version":"1.0","hostname":"h","ipAddress":"1.2.3.4","machineId":"mid","osinfo":null,'
        '"driver":{"id":"d1","name":"Postgres","description":"pg","url":"masked"},"serverId":"sid","companyIds":["c1"]}'
    )


def test_eds_session_always_serializes_credential() -> None:
    assert stringify(EdsSession(session_id="s1")) == '{"sessionId":"s1","credential":null}'
    assert stringify(EdsSession(session_id="s1", credential="base64creds")) == (
        '{"sessionId":"s1","credential":"base64creds"}'
    )


def test_session_start_response_deserialize() -> None:
    body = '{"success":true,"message":"ok","data":{"sessionId":"abc","credential":"creds"}}'
    r = SessionStartResponse.from_json(body)
    assert r.success is True
    assert r.message == "ok"
    assert r.data.session_id == "abc"
    assert r.data.credential == "creds"


def test_session_end_serialization() -> None:
    assert stringify(SessionEnd(errored=True)) == '{"errored":true}'
    assert stringify(SessionEnd()) == '{"errored":false}'


def test_session_end_response_deserialize() -> None:
    r = SessionEndResponse.from_json('{"success":true,"message":"ok","data":{"url":"u","errorUrl":"e"}}')
    assert r.data.url == "u"
    assert r.data.error_url == "e"


def test_enroll_response_deserialize() -> None:
    r = EnrollResponse.from_json('{"success":true,"message":"m","data":{"token":"t","serverId":"sid"}}')
    assert r.data.token == "t"
    assert r.data.server_id == "sid"


def test_eds_session_from_dict_missing_credential() -> None:
    assert EdsSession.from_dict({"sessionId": "s1"}).credential is None
