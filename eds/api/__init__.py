"""PARITY: internal/api/api.go — Shopmonkey API DTOs + the region URL map.

DTOs (de)serialize with eds.util.gojson (Go json.Marshal byte-parity, declaration order, NOT sorted).
omitempty applies to exactly two fields: SessionStart.driver and SessionStart.companyIds. credential
(*string) and osinfo (any) have NO omitempty → always emitted (null when unset). No typos to preserve here
(message / errorUrl / errored are all spelled correctly). Golden vectors: the C# ApiTests (no Go _test.go).

This module's get_api_url (enroll-code letter → region URL) is DISTINCT from util.get_api_url_from_jwt.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

from eds.util.gostruct import OmitEmpty, gojson_struct

# PARITY: api.GetAPIURL table — case-sensitive, exact match, no default (https for P/S/E, http for L).
_API_URLS: dict[str, str] = {
    "P": "https://api.shopmonkey.cloud",
    "S": "https://sandbox-api.shopmonkey.cloud",
    "E": "https://edge-api.shopmonkey.cloud",
    "L": "http://localhost:3101",
}


def get_api_url(first_letter: str) -> str:
    """PARITY: api.GetAPIURL. DEVIATION: Go returns (*string, error); we return str / raise ValueError.
    The message string "invalid code" is preserved verbatim."""
    url = _API_URLS.get(first_letter)
    if url is None:
        raise ValueError("invalid code")
    return url


@dataclass
class DriverMeta:
    """PARITY: api.DriverMeta. ``url`` is masked upstream (server.go util.MaskURL) — may contain secrets."""

    id: str = field(default="", metadata={"json": "id"})
    name: str = field(default="", metadata={"json": "name"})
    description: str = field(default="", metadata={"json": "description"})
    url: str = field(default="", metadata={"json": "url"})

    def __gojson__(self) -> str:
        return gojson_struct(self)


@dataclass
class SessionStart:
    """PARITY: api.SessionStart (request body)."""

    version: str = field(default="", metadata={"json": "version"})
    hostname: str = field(default="", metadata={"json": "hostname"})
    ip_address: str = field(default="", metadata={"json": "ipAddress"})
    machine_id: str = field(default="", metadata={"json": "machineId"})
    # `any`, NO omitempty → always present (osinfo is lowercase; null when None); stays a __gojson__ struct, not a dict.
    os_info: object | None = field(default=None, metadata={"json": "osinfo"})
    driver: DriverMeta | None = field(default=None, metadata={"json": "driver", "omit": OmitEmpty.IF_NONE})
    server_id: str = field(default="", metadata={"json": "serverId"})
    company_ids: list[str] | None = field(default=None, metadata={"json": "companyIds", "omit": OmitEmpty.IF_FALSY})

    def __gojson__(self) -> str:
        return gojson_struct(self)


@dataclass
class EdsSession:
    """PARITY: api.EdsSession. credential is *string with NO omitempty → always emitted (null when None)."""

    session_id: str = field(default="", metadata={"json": "sessionId"})
    credential: str | None = field(default=None, metadata={"json": "credential"})  # *string, NO omitempty (null)

    def __gojson__(self) -> str:
        return gojson_struct(self)

    @classmethod
    def from_dict(cls, m: dict) -> EdsSession:
        return cls(session_id=m.get("sessionId", ""), credential=m.get("credential"))


@dataclass
class SessionStartResponse:
    """PARITY: api.SessionStartResponse (response body)."""

    success: bool = False
    message: str = ""
    data: EdsSession = field(default_factory=EdsSession)

    @classmethod
    def from_dict(cls, m: dict) -> SessionStartResponse:
        return cls(bool(m.get("success", False)), m.get("message", ""), EdsSession.from_dict(m.get("data") or {}))

    @classmethod
    def from_json(cls, s: str | bytes) -> SessionStartResponse:
        return cls.from_dict(json.loads(s))


@dataclass
class SessionEnd:
    """PARITY: api.SessionEnd (request body)."""

    errored: bool = field(default=False, metadata={"json": "errored"})

    def __gojson__(self) -> str:
        return gojson_struct(self)


@dataclass
class SessionEndURLs:
    """PARITY: api.SessionEndURLs."""

    url: str = ""
    error_url: str = ""

    @classmethod
    def from_dict(cls, m: dict) -> SessionEndURLs:
        return cls(url=m.get("url", ""), error_url=m.get("errorUrl", ""))


@dataclass
class SessionEndResponse:
    """PARITY: api.SessionEndResponse."""

    success: bool = False
    message: str = ""
    data: SessionEndURLs = field(default_factory=SessionEndURLs)

    @classmethod
    def from_dict(cls, m: dict) -> SessionEndResponse:
        return cls(
            bool(m.get("success", False)), m.get("message", ""), SessionEndURLs.from_dict(m.get("data") or {})
        )

    @classmethod
    def from_json(cls, s: str | bytes) -> SessionEndResponse:
        return cls.from_dict(json.loads(s))


@dataclass
class EnrollTokenData:
    """PARITY: api.EnrollTokenData. JSON tags token/serverId; TOML tags token/server_id (used by cmd/enroll)."""

    token: str = ""
    server_id: str = ""

    @classmethod
    def from_dict(cls, m: dict) -> EnrollTokenData:
        return cls(token=m.get("token", ""), server_id=m.get("serverId", ""))


@dataclass
class EnrollResponse:
    """PARITY: api.EnrollResponse."""

    success: bool = False
    message: str = ""
    data: EnrollTokenData = field(default_factory=EnrollTokenData)

    @classmethod
    def from_dict(cls, m: dict) -> EnrollResponse:
        return cls(
            bool(m.get("success", False)), m.get("message", ""), EnrollTokenData.from_dict(m.get("data") or {})
        )

    @classmethod
    def from_json(cls, s: str | bytes) -> EnrollResponse:
        return cls.from_dict(json.loads(s))
