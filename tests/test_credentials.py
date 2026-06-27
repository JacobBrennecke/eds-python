"""PARITY: internal/consumer/credentials_test.go + get_nats_creds parsing."""

from __future__ import annotations

import base64
import json

import pytest

from eds.consumer.credentials import (
    extract_company_id_from_dbchange_subscription,
    extract_session_id_from_eds_subscription,
    get_nats_creds,
)


def test_extract_company_id() -> None:
    assert (
        extract_company_id_from_dbchange_subscription("dbchange.*.*.6287a4154d1a72cc5ce091bb.*.PUBLIC.>")
        == "6287a4154d1a72cc5ce091bb"
    )
    assert extract_company_id_from_dbchange_subscription("_INBOX.>") == ""
    assert extract_company_id_from_dbchange_subscription("eds.notify.284e8bdb-9c18-45c3-9f18-844ad70610ef.>") == ""
    assert extract_company_id_from_dbchange_subscription("eds.b") == ""


def test_extract_session_id() -> None:
    assert extract_session_id_from_eds_subscription("dbchange.*.*.6287a4154d1a72cc5ce091bb.*.PUBLIC.>") == ""
    assert extract_session_id_from_eds_subscription("_INBOX.>") == ""
    assert (
        extract_session_id_from_eds_subscription("eds.notify.284e8bdb-9c18-45c3-9f18-844ad70610ef.>")
        == "284e8bdb-9c18-45c3-9f18-844ad70610ef"
    )
    assert extract_session_id_from_eds_subscription("eds.b") == ""


def _seg(d: dict) -> str:
    return base64.urlsafe_b64encode(json.dumps(d).encode()).rstrip(b"=").decode()


def _make_creds(claims: dict, tmp_path) -> str:
    jwt = f"{_seg({'typ': 'JWT', 'alg': 'ed25519-nkey'})}.{_seg(claims)}.sig"
    p = tmp_path / "x.creds"
    p.write_text(f"-----BEGIN NATS USER JWT-----\n{jwt}\n------END NATS USER JWT------\n")
    return str(p)


def test_get_nats_creds(tmp_path) -> None:
    claims = {
        "name": "SERVER123",
        "nats": {
            "sub": {
                "allow": [
                    "dbchange.*.*.6287a4154d1a72cc5ce091bb.*.PUBLIC.>",
                    "dbchange.*.*.aabbccddeeff00112233.*.PUBLIC.>",
                    "eds.notify.284e8bdb-9c18-45c3-9f18-844ad70610ef.>",
                    "_INBOX.>",
                ]
            }
        },
    }
    creds_file, info = get_nats_creds(_make_creds(claims, tmp_path))
    assert info.company_ids == ["6287a4154d1a72cc5ce091bb", "aabbccddeeff00112233"]
    assert info.server_id == "SERVER123"
    assert info.session_id == "284e8bdb-9c18-45c3-9f18-844ad70610ef"
    assert creds_file.endswith("x.creds")


def test_get_nats_creds_missing_file() -> None:
    with pytest.raises(ValueError, match="cannot be found"):
        get_nats_creds("definitely-not-a-real.creds")


def test_get_nats_creds_no_company_ids(tmp_path) -> None:
    claims = {"name": "S", "nats": {"sub": {"allow": ["_INBOX.>"]}}}
    with pytest.raises(ValueError, match="company IDs"):
        get_nats_creds(_make_creds(claims, tmp_path))


def test_get_nats_creds_missing_server_id(tmp_path) -> None:
    claims = {"nats": {"sub": {"allow": ["dbchange.*.*.6287a4154d1a72cc5ce091bb.*.PUBLIC.>"]}}}
    with pytest.raises(ValueError, match="missing server id"):
        get_nats_creds(_make_creds(claims, tmp_path))
