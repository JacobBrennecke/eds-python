"""PARITY: internal/notification/notification.go DTOs (53-129).

Two wire paths: request/reply responses are JSON (m.Respond → __gojson__, byte-parity with Go json.Marshal);
fire-and-forget responses are msgpack (publish → to_msgpack() dict). Faithful quirks preserved: ValidateResponse's
JSON key is the misspelled `messsage`; GenericResponse.message is *string (None omits, "" emits); the `LogPath`
fields are NEVER serialized (json/msgpack `-`) — they only trigger a separate PublishSendLogsResponse.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from eds.driver import DriverConfigurator, FieldError
from eds.util.gojson import marshal


# ---- inbound (built from Notification.data; never serialized) ----
@dataclass
class Notification:
    action: str = ""
    data: dict[str, Any] = field(default_factory=dict)

    def __gojson__(self) -> str:  # for trace logging (Go Notification.String)
        parts = ['"action":' + marshal(self.action)]
        if self.data:  # omitempty
            parts.append('"data":' + marshal(self.data))
        return "{" + ",".join(parts) + "}"


@dataclass
class ImportRequest:
    backfill: bool = False
    job_id: str = ""


@dataclass
class InitBackfillRequest:
    backfill: bool = False


@dataclass
class ConfigureRequest:
    url: str = ""
    backfill: bool = False


# ---- JSON request/reply responses (__gojson__) ----
@dataclass
class InitBackfillResponse:
    success: bool = False
    message: str | None = None  # *string,omitempty
    session_id: str = ""
    job_id: str = ""

    def __gojson__(self) -> str:
        parts = ['"success":' + marshal(self.success)]
        if self.message is not None:
            parts.append('"message":' + marshal(self.message))
        parts.append('"sessionId":' + marshal(self.session_id))
        parts.append('"jobId":' + marshal(self.job_id))
        return "{" + ",".join(parts) + "}"


@dataclass
class ConfigureResponse:
    success: bool = False
    message: str | None = None  # *string,omitempty
    masked_url: str | None = None  # *string,omitempty → maskedURL
    session_id: str = ""
    backfill: bool = False
    log_path: str | None = None  # json:"-" — NOT serialized (triggers a separate sendlogs publish)

    def __gojson__(self) -> str:
        parts = ['"success":' + marshal(self.success)]
        if self.message is not None:
            parts.append('"message":' + marshal(self.message))
        if self.masked_url is not None:
            parts.append('"maskedURL":' + marshal(self.masked_url))
        parts.append('"sessionId":' + marshal(self.session_id))
        parts.append('"backfill":' + marshal(self.backfill))
        return "{" + ",".join(parts) + "}"


@dataclass
class DriverConfigResponse:
    drivers: dict[str, DriverConfigurator] = field(default_factory=dict)
    session_id: str = ""

    def __gojson__(self) -> str:
        return '{"drivers":' + marshal(self.drivers) + ',"sessionId":' + marshal(self.session_id) + "}"


@dataclass
class ValidateResponse:
    success: bool = False
    message: str = ""  # plain string,omitempty — JSON key is the TYPO `messsage`
    field_errors: list[FieldError] = field(default_factory=list)
    session_id: str = ""
    url: str = ""

    def __gojson__(self) -> str:
        parts = ['"success":' + marshal(self.success)]
        if self.message:  # omitempty
            parts.append('"messsage":' + marshal(self.message))  # PARITY: Go json tag typo (3 s's)
        if self.field_errors:  # omitempty
            parts.append('"field_errors":' + marshal(self.field_errors))
        parts.append('"sessionId":' + marshal(self.session_id))
        if self.url:  # omitempty
            parts.append('"url":' + marshal(self.url))
        return "{" + ",".join(parts) + "}"


# ---- msgpack fire-and-forget responses (to_msgpack) ----
@dataclass
class SendLogsResponse:
    path: str = ""
    session_id: str = ""

    def to_msgpack(self) -> dict[str, Any]:
        return {"path": self.path, "sessionId": self.session_id}


@dataclass
class GenericResponse:
    success: bool = False
    message: str | None = None  # *string,omitempty — None omits, "" emits (publishSimpleStatus sends "")
    session_id: str = ""
    action: str = ""

    def to_msgpack(self) -> dict[str, Any]:
        d: dict[str, Any] = {"success": self.success}
        if self.message is not None:
            d["message"] = self.message
        d["sessionId"] = self.session_id
        d["action"] = self.action
        return d


@dataclass
class ImportResponse:
    success: bool = False
    message: str | None = None  # *string,omitempty
    session_id: str = ""
    log_path: str | None = None  # msgpack:"-" — NOT serialized
    job_id: str = ""

    def to_msgpack(self) -> dict[str, Any]:
        d: dict[str, Any] = {"success": self.success}
        if self.message is not None:
            d["message"] = self.message
        d["sessionId"] = self.session_id
        d["jobId"] = self.job_id
        return d


@dataclass
class UpgradeResponse:
    success: bool = False
    message: str = ""  # plain string,omitempty
    session_id: str = ""
    log_path: str | None = None  # msgpack:"-" — NOT serialized
    version: str = ""

    def to_msgpack(self) -> dict[str, Any]:
        d: dict[str, Any] = {"success": self.success}
        if self.message:  # omitempty
            d["message"] = self.message
        d["sessionId"] = self.session_id
        d["version"] = self.version
        return d
