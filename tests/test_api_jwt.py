"""PARITY: internal/util/api.go (api_test.go) — GetAPIURLFromJWT."""

from __future__ import annotations

import base64
import json

import pytest

from eds.util.api import get_api_url_from_jwt

# PARITY: the exact JWT from api_test.go -> issuer http://localhost:3101.
_GO_JWT = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJhdWQiOiJhcGkiLCJjaWQiOiI2Mjg3YTQxNTRkMWE3MmNjNWNlMDkxYmIiLCJpZCI6IjYyODdhNDA0NGQxYTcyM2IxMGUwOTFi"
    "OSIsImxpZCI6IjYyODdhNDA0NGQxYTcyM2IxMGVmZjFiMCIsIm9uIjo2LCJyaWQiOiJ1dzEiLCJzYWQiOjAsInNpZCI6IjM2Mjcz"
    "MzYwZWZkMDA1ZjgiLCJpc3MiOiJodHRwOi8vbG9jYWxob3N0OjMxMDEiLCJpYXQiOjE3MjI0ODY4MzJ9."
    "5fPgQJFBuZWBCaXsPN7uKXKsamfkxP5ssEBI3EECEv0"
)


def _make_jwt(claims: dict) -> str:
    def seg(d: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(d).encode()).rstrip(b"=").decode()

    return f"{seg({'alg': 'HS256', 'typ': 'JWT'})}.{seg(claims)}.sig"


def test_get_api_url_from_jwt() -> None:
    assert get_api_url_from_jwt(_GO_JWT) == "http://localhost:3101"


def test_legacy_issuer_is_remapped() -> None:
    assert get_api_url_from_jwt(_make_jwt({"iss": "https://shopmonkey.io"})) == "https://api.shopmonkey.cloud"


def test_missing_issuer_returns_empty() -> None:
    assert get_api_url_from_jwt(_make_jwt({"sub": "x"})) == ""


@pytest.mark.parametrize("token", ["onlyonesegment", "not.a.jwt!!!"])
def test_invalid_token_raises(token: str) -> None:
    with pytest.raises(ValueError, match="failed to parse jwt"):
        get_api_url_from_jwt(token)
