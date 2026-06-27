"""PARITY: internal/driver.go (+ importer.go, migration.go) — the driver framework.

Capability interfaces become typing.Protocols; the four module-global registries mirror Go's package-level
maps. Config-value getters reproduce Go's exact type-assertion semantics — notably the int getters accept
only int (not float, not bool, not — for the required one — string), so a JSON-decoded numeric config field
(a float once decoded the Go map[string]any way) falls through to the default (DEVIATION: see
DEVIATIONS.md#config-int-float-fallthrough). FieldError's JSON key for the message is "error" (not "message").
"""

from __future__ import annotations

import re
import urllib.parse
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from eds.dbchange import DBChangeEvent
from eds.schema import DatabaseSchema, Schema, SchemaMap, SchemaRegistry, SchemaValidator
from eds.util.gojson import marshal
from eds.util.logger import Logger

# PARITY: driver.go DriverType / DriverFormat string constants.
DRIVER_TYPE_STRING = "string"
DRIVER_TYPE_NUMBER = "number"
DRIVER_TYPE_BOOLEAN = "boolean"
DRIVER_FORMAT_PASSWORD = "password"


class DriverStoppedError(Exception):
    """PARITY: driver.go ErrDriverStopped — message exactly 'driver stopped'."""

    def __init__(self) -> None:
        super().__init__("driver stopped")


@dataclass
class DriverConfig:
    """PARITY: driver.go DriverConfig."""

    url: str = ""
    logger: Logger | None = None
    schema_registry: SchemaRegistry | None = None
    tracker: Any = None
    data_dir: str = ""
    context: Any = None  # cancellation token / None


@dataclass
class ImporterConfig:
    """PARITY: importer.go ImporterConfig."""

    url: str = ""
    logger: Logger | None = None
    schema_registry: SchemaRegistry | None = None
    schema_validator: SchemaValidator | None = None
    max_parallel: int = 0
    job_id: str = ""
    data_dir: str = ""
    dry_run: bool = False
    tables: list[str] = field(default_factory=list)
    single: bool = False
    schema_only: bool = False
    no_delete: bool = False
    context: Any = None


@dataclass
class DriverField:
    """PARITY: driver.go DriverField (JSON order: name, type, format?, default?, description, required)."""

    name: str = ""
    type: str = DRIVER_TYPE_STRING
    format: str | None = None  # omitempty (omit when empty/None)
    default: str | None = None  # *string, omitempty (omit when None)
    description: str = ""
    required: bool = False

    def __gojson__(self) -> str:
        parts = ['"name":' + marshal(self.name), '"type":' + marshal(self.type)]
        if self.format:
            parts.append('"format":' + marshal(self.format))
        if self.default is not None:
            parts.append('"default":' + marshal(self.default))
        parts.append('"description":' + marshal(self.description))
        parts.append('"required":' + marshal(self.required))
        return "{" + ",".join(parts) + "}"


class FieldError(Exception):
    """PARITY: driver.go FieldError — both an error (str() == message) and a JSON struct whose message key
    is 'error'."""

    def __init__(self, field: str, message: str) -> None:
        self.field = field
        self.message = message
        super().__init__(message)

    def __str__(self) -> str:
        return self.message

    def __eq__(self, other: object) -> bool:
        return isinstance(other, FieldError) and other.field == self.field and other.message == self.message

    def __hash__(self) -> int:
        return hash((self.field, self.message))

    def __gojson__(self) -> str:
        return '{"field":' + marshal(self.field) + ',"error":' + marshal(self.message) + "}"


def new_field_error(field: str, message: str) -> FieldError:
    """PARITY: driver.go NewFieldError."""
    return FieldError(field, message)


@dataclass
class DriverMetadata:
    """PARITY: driver.go DriverMetadata."""

    scheme: str = ""
    name: str = ""
    description: str = ""
    example_url: str = ""
    help: str = ""
    supports_import: bool = False
    supports_migration: bool = False

    def __gojson__(self) -> str:
        return (
            '{"scheme":' + marshal(self.scheme)
            + ',"name":' + marshal(self.name)
            + ',"description":' + marshal(self.description)
            + ',"exampleURL":' + marshal(self.example_url)
            + ',"help":' + marshal(self.help)
            + ',"supportsImport":' + marshal(self.supports_import)
            + ',"supportsMigration":' + marshal(self.supports_migration)
            + "}"
        )


@dataclass
class DriverConfigurator:
    """PARITY: driver.go DriverConfigurator."""

    metadata: DriverMetadata = field(default_factory=DriverMetadata)
    fields: list[DriverField] = field(default_factory=list)

    def __gojson__(self) -> str:
        return '{"metadata":' + marshal(self.metadata) + ',"fields":' + marshal(self.fields) + "}"


# ---- capability protocols (runtime_checkable: the registry does isinstance checks) -----------------


@runtime_checkable
class Driver(Protocol):
    def stop(self) -> None: ...
    def max_batch_size(self) -> int: ...
    def process(self, logger: Logger, event: DBChangeEvent) -> bool: ...
    def flush(self, logger: Logger) -> None: ...
    def test(self, ctx: Any, logger: Logger, url: str) -> None: ...
    def configuration(self) -> list[DriverField]: ...
    def validate(self, values: dict[str, Any]) -> tuple[str, list[FieldError]]: ...


@runtime_checkable
class DriverLifecycle(Protocol):
    def start(self, config: DriverConfig) -> None: ...


@runtime_checkable
class DriverSessionHandler(Protocol):
    def set_session_id(self, session_id: str) -> None: ...


@runtime_checkable
class DriverAlias(Protocol):
    def aliases(self) -> list[str]: ...


@runtime_checkable
class DriverHelp(Protocol):
    def name(self) -> str: ...
    def description(self) -> str: ...
    def example_url(self) -> str: ...
    def help(self) -> str: ...


@runtime_checkable
class DriverMigration(Protocol):
    def migrate_new_table(self, ctx: Any, logger: Logger, schema: Schema) -> None: ...
    def migrate_new_columns(self, ctx: Any, logger: Logger, schema: Schema, columns: list[str]) -> None: ...
    def get_destination_schema(self, ctx: Any, logger: Logger) -> DatabaseSchema: ...


@runtime_checkable
class Importer(Protocol):
    def run_import(self, config: ImporterConfig) -> None: ...  # Go Import (keyword in Python)


@runtime_checkable
class ImporterHelp(Protocol):
    def supports_delete(self) -> bool: ...


# ---- field constructors (driver.go) ---------------------------------------------------------------


def string_pointer(val: str) -> str | None:
    """PARITY: StringPointer — nil for empty string."""
    return None if val == "" else val


def int_pointer(val: int) -> int:
    """PARITY: IntPointer."""
    return val


def required_string_field(name: str, description: str, default: str | None) -> DriverField:
    return DriverField(name=name, type=DRIVER_TYPE_STRING, description=description, required=True, default=default)


def optional_string_field(name: str, description: str, default: str | None) -> DriverField:
    return DriverField(name=name, type=DRIVER_TYPE_STRING, description=description, required=False, default=default)


def optional_password_field(name: str, description: str, default: str | None) -> DriverField:
    return DriverField(
        name=name, type=DRIVER_TYPE_STRING, format=DRIVER_FORMAT_PASSWORD, description=description,
        required=False, default=default,
    )


def optional_number_field(name: str, description: str, default: int | None) -> DriverField:
    # PARITY: the numeric default is stored as its decimal string form via %d.
    default_str = str(default) if default is not None else None
    return DriverField(name=name, type=DRIVER_TYPE_NUMBER, description=description, required=False, default=default_str)


# ---- config value getters (driver.go — exact type-assertion semantics) ----------------------------


def get_required_string_value(name: str, values: dict[str, Any]) -> tuple[str, FieldError | None]:
    v = values.get(name)
    if isinstance(v, str):  # PARITY: empty string is valid (no error)
        return v, None
    return "", FieldError(name, f"required field {name} not found")


def get_required_int_value(name: str, values: dict[str, Any]) -> tuple[int, FieldError | None]:
    v = values.get(name)
    if type(v) is int:  # PARITY: Go .(int)/.(int64) — rejects bool and float
        return v, None
    return 0, FieldError(name, f"required field {name} not found")


def get_optional_string_value(name: str, default: str, values: dict[str, Any]) -> str:
    v = values.get(name)
    if isinstance(v, str) and v != "":  # PARITY: empty string treated as absent
        return v
    return default


def get_optional_int_value(name: str, default: int, values: dict[str, Any]) -> int:
    v = values.get(name)
    if type(v) is int:  # PARITY: rejects bool/float; accepts a string via Atoi
        return v
    if isinstance(v, str):
        try:
            return _atoi(v)
        except ValueError:
            return default
    return default


def _atoi(s: str) -> int:
    """PARITY: strconv.Atoi — base-10, optional leading +/-, ASCII digits only (no whitespace/underscore/0x)."""
    body = s[1:] if s[:1] in ("+", "-") else s
    if not body or not all("0" <= c <= "9" for c in body):
        raise ValueError(f"invalid int: {s}")
    return int(s)


def url_from_database_configuration(
    schema: str, defport: int, values: dict[str, Any]
) -> tuple[str, list[FieldError]]:
    """PARITY: URLFromDatabaseConfiguration. DEVIATION: builds the URL string directly (like the C# port)
    rather than Go's url.URL.String()+QueryUnescape round-trip — byte-identical for ASCII inputs (a literal
    '+' in a field would become a space under true Go semantics). Field keys: Hostname/Username/Password/
    Port/Database."""
    errors: list[FieldError] = []
    hostname, err = get_required_string_value("Hostname", values)
    if err is not None:
        errors.append(err)
    username = get_optional_string_value("Username", "", values)
    password = get_optional_string_value("Password", "", values)
    port = get_optional_int_value("Port", defport, values)
    database, err = get_required_string_value("Database", values)
    if err is not None:
        errors.append(err)
    if errors:
        return "", errors

    out = schema + "://"
    if username != "":
        out += username + ":" + password + "@"  # PARITY: empty password keeps the colon
    out += f"{hostname}:{port}" if defport > 0 else hostname
    out += "/" + database
    return out, []


def new_database_configuration(defport: int) -> list[DriverField]:
    """PARITY: NewDatabaseConfiguration — Database, Username, Password, Hostname, [Port if defport>0]."""
    fields = [
        required_string_field("Database", "The database name to use", None),
        optional_string_field("Username", "The username to database", None),
        optional_password_field("Password", "The password to database", None),
        required_string_field("Hostname", "The hostname or ip address to database", None),
    ]
    if defport > 0:
        fields.append(optional_number_field("Port", "The port to database", int_pointer(defport)))
    return fields


# ---- registry (driver.go + importer.go module-level maps) -----------------------------------------

_driver_registry: dict[str, Driver] = {}
_driver_alias_registry: dict[str, str] = {}
_importer_registry: dict[str, Importer] = {}
_importer_alias_registry: dict[str, str] = {}

_ANSI_SGR = re.compile(r"\x1b\[[0-9;]*m")


def ansi_strip(s: str) -> str:
    """PARITY: ansi.Strip (DEVIATION: SGR-only, like the C# port; help text uses only color codes)."""
    return _ANSI_SGR.sub("", s)


def register_driver(protocol: str, driver: Driver) -> None:
    """PARITY: RegisterDriver."""
    _driver_registry[protocol] = driver
    if isinstance(driver, DriverAlias):
        for alias in driver.aliases():
            _driver_alias_registry[alias] = protocol


def register_importer(protocol: str, importer: Importer) -> None:
    """PARITY: RegisterImporter."""
    _importer_registry[protocol] = importer
    if isinstance(importer, DriverAlias):
        for alias in importer.aliases():
            _importer_alias_registry[alias] = protocol


def _resolve_driver(scheme: str) -> Driver | None:
    driver = _driver_registry.get(scheme)
    if driver is None:
        protocol = _driver_alias_registry.get(scheme, "")
        if protocol:
            driver = _driver_registry.get(protocol)
    return driver


def new_driver(
    ctx: Any, logger: Logger, url_string: str, registry: SchemaRegistry, tracker: Any, data_dir: str
) -> Driver:
    """PARITY: NewDriver — resolve by URL scheme (+alias), and Start if the driver is a lifecycle driver."""
    scheme = _scheme(url_string)
    driver = _resolve_driver(scheme)
    if driver is None:
        raise ValueError(f"no driver registered for protocol {scheme}")
    if isinstance(driver, DriverLifecycle):
        driver.start(
            DriverConfig(
                url=url_string,
                logger=logger.with_prefix(f"[{scheme}]") if logger is not None else None,
                schema_registry=registry,
                tracker=tracker,
                data_dir=data_dir,
                context=ctx,
            )
        )
    return driver


def new_driver_for_import(
    ctx: Any, logger: Logger, url_string: str, registry: SchemaRegistry, tracker: Any, data_dir: str
) -> Driver:
    """PARITY: NewDriverForImport — resolve only (does NOT Start)."""
    scheme = _scheme(url_string)
    driver = _resolve_driver(scheme)
    if driver is None:
        raise ValueError(f"no driver registered for protocol {scheme}")
    return driver


def new_importer(ctx: Any, logger: Logger, url_string: str, registry: SchemaRegistry) -> Importer:
    """PARITY: NewImporter."""
    scheme = _scheme(url_string)
    importer = _importer_registry.get(scheme)
    if importer is None:
        protocol = _importer_alias_registry.get(scheme, "")
        if protocol:
            importer = _importer_registry.get(protocol)
    if importer is None:
        supported = ", ".join(_importer_registry.keys())
        raise ValueError(f"no importer registered for protocol {scheme}. the following are supported: {supported}")
    return importer


def validate(schema: str, values: dict[str, Any]) -> tuple[str, list[FieldError]]:
    """PARITY: Validate."""
    driver = _resolve_driver(schema)
    if driver is None:
        raise ValueError(f"no driver registered for protocol {schema}")
    return driver.validate(values)


def driver_supports_migration(driver: Driver) -> bool:
    """PARITY: driverSupportsMigration."""
    return isinstance(driver, DriverMigration)


def get_driver_metadata() -> list[DriverMetadata]:
    """PARITY: GetDriverMetadata — only DriverHelp drivers; Help is RAW (not ANSI-stripped)."""
    res: list[DriverMetadata] = []
    for scheme, driver in _driver_registry.items():
        if isinstance(driver, DriverHelp):
            res.append(
                DriverMetadata(
                    scheme=scheme,
                    name=driver.name(),
                    description=driver.description(),
                    example_url=driver.example_url(),
                    help=driver.help(),
                    supports_import=_importer_registry.get(scheme) is not None,
                    supports_migration=driver_supports_migration(driver),
                )
            )
    return res


def get_driver_configurations() -> dict[str, DriverConfigurator]:
    """PARITY: GetDriverConfigurations — ALL drivers; Help is ANSI-STRIPPED here."""
    res: dict[str, DriverConfigurator] = {}
    for scheme, driver in _driver_registry.items():
        meta = DriverMetadata(scheme=scheme, name=scheme)
        if isinstance(driver, DriverHelp):
            meta.name = driver.name()
            meta.description = driver.description()
            meta.example_url = driver.example_url()
            meta.help = ansi_strip(driver.help())
            meta.supports_import = _importer_registry.get(scheme) is not None
            meta.supports_migration = driver_supports_migration(driver)
        res[scheme] = DriverConfigurator(metadata=meta, fields=driver.configuration())
    return res


def get_driver_metadata_for_url(url_string: str) -> DriverMetadata | None:
    """PARITY: GetDriverMetadataForURL — match scheme or alias; Help RAW; None if no match (no error)."""
    proto = _scheme(url_string)
    for scheme, driver in _driver_registry.items():
        if scheme == proto or _driver_alias_registry.get(proto) == scheme:
            if isinstance(driver, DriverHelp):
                return DriverMetadata(
                    scheme=scheme,
                    name=driver.name(),
                    description=driver.description(),
                    example_url=driver.example_url(),
                    help=driver.help(),
                    supports_import=_importer_registry.get(scheme) is not None,
                    supports_migration=driver_supports_migration(driver),
                )
            return DriverMetadata(scheme=scheme, name=scheme)
    return None


def _scheme(url_string: str) -> str:
    return urllib.parse.urlsplit(url_string).scheme


def reset_registries() -> None:
    """Test helper — clear all four registries."""
    _driver_registry.clear()
    _driver_alias_registry.clear()
    _importer_registry.clear()
    _importer_alias_registry.clear()


# PARITY: SchemaMap re-exported for driver/importer type hints (Handler.create_datasource).
__all__ = ["SchemaMap"]
