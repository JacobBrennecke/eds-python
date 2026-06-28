"""PARITY: internal/notification/notification.go DTOs (53-129).

Two wire paths: request/reply responses are JSON (m.Respond → __gojson__, byte-parity with Go json.Marshal);
fire-and-forget responses are msgpack (publish → to_msgpack() dict). Faithful quirks preserved: ValidateResponse's
JSON key is the misspelled `messsage`; GenericResponse.message is *string (None omits, "" emits); the `LogPath`
fields are NEVER serialized (json/msgpack `-`) — they only trigger a separate PublishSendLogsResponse.

Stage-2 WS2: serialization is declarative via field(metadata=...) over eds.util.gostruct (see that module); each
field's json key + omit rule reproduces the Go struct tags exactly.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from eds.driver import DriverConfigurator, FieldError
from eds.util.gostruct import OmitEmpty, gojson_struct, msgpack_dict


# ---- inbound (built from Notification.data; never serialized) ----
@dataclass
class Notification:
    action: str = field(default="", metadata={"json": "action"})
    data: dict[str, Any] = field(default_factory=dict, metadata={"json": "data", "omit": OmitEmpty.IF_FALSY})

    def __gojson__(self) -> str:  # for trace logging (Go Notification.String)
        return gojson_struct(self)


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
    success: bool = field(default=False, metadata={"json": "success"})
    message: str | None = field(default=None, metadata={"json": "message", "omit": OmitEmpty.IF_NONE})
    session_id: str = field(default="", metadata={"json": "sessionId"})
    job_id: str = field(default="", metadata={"json": "jobId"})

    def __gojson__(self) -> str:
        return gojson_struct(self)


@dataclass
class ConfigureResponse:
    success: bool = field(default=False, metadata={"json": "success"})
    message: str | None = field(default=None, metadata={"json": "message", "omit": OmitEmpty.IF_NONE})
    masked_url: str | None = field(default=None, metadata={"json": "maskedURL", "omit": OmitEmpty.IF_NONE})
    session_id: str = field(default="", metadata={"json": "sessionId"})
    backfill: bool = field(default=False, metadata={"json": "backfill"})
    log_path: str | None = None  # json:"-" — NOT serialized (triggers a separate sendlogs publish)

    def __gojson__(self) -> str:
        return gojson_struct(self)


@dataclass
class DriverConfigResponse:
    drivers: dict[str, DriverConfigurator] = field(default_factory=dict, metadata={"json": "drivers"})
    session_id: str = field(default="", metadata={"json": "sessionId"})

    def __gojson__(self) -> str:
        return gojson_struct(self)


@dataclass
class ValidateResponse:
    success: bool = field(default=False, metadata={"json": "success"})
    # PARITY: Go json tag typo (3 s's); plain-string omitempty.
    message: str = field(default="", metadata={"json": "messsage", "omit": OmitEmpty.IF_FALSY})
    field_errors: list[FieldError] = field(
        default_factory=list, metadata={"json": "field_errors", "omit": OmitEmpty.IF_FALSY}
    )
    session_id: str = field(default="", metadata={"json": "sessionId"})
    url: str = field(default="", metadata={"json": "url", "omit": OmitEmpty.IF_FALSY})

    def __gojson__(self) -> str:
        return gojson_struct(self)


# ---- msgpack fire-and-forget responses (to_msgpack) ----
@dataclass
class SendLogsResponse:
    path: str = field(default="", metadata={"json": "path"})
    session_id: str = field(default="", metadata={"json": "sessionId"})

    def to_msgpack(self) -> dict[str, Any]:
        return msgpack_dict(self)


@dataclass
class GenericResponse:
    success: bool = field(default=False, metadata={"json": "success"})
    # *string,omitempty — None omits, "" emits (publishSimpleStatus sends "")
    message: str | None = field(default=None, metadata={"json": "message", "omit": OmitEmpty.IF_NONE})
    session_id: str = field(default="", metadata={"json": "sessionId"})
    action: str = field(default="", metadata={"json": "action"})

    def to_msgpack(self) -> dict[str, Any]:
        return msgpack_dict(self)


@dataclass
class ImportResponse:
    success: bool = field(default=False, metadata={"json": "success"})
    message: str | None = field(default=None, metadata={"json": "message", "omit": OmitEmpty.IF_NONE})
    session_id: str = field(default="", metadata={"json": "sessionId"})
    log_path: str | None = None  # msgpack:"-" — NOT serialized
    job_id: str = field(default="", metadata={"json": "jobId"})

    def to_msgpack(self) -> dict[str, Any]:
        return msgpack_dict(self)


@dataclass
class UpgradeResponse:
    success: bool = field(default=False, metadata={"json": "success"})
    message: str = field(default="", metadata={"json": "message", "omit": OmitEmpty.IF_FALSY})  # plain-string omitempty
    session_id: str = field(default="", metadata={"json": "sessionId"})
    log_path: str | None = None  # msgpack:"-" — NOT serialized
    version: str = field(default="", metadata={"json": "version"})

    def to_msgpack(self) -> dict[str, Any]:
        return msgpack_dict(self)
