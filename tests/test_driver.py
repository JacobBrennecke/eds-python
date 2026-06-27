"""PARITY: internal/driver.go — framework golden vectors + registry behavior."""

from __future__ import annotations

import pytest

from eds import driver as d
from eds.driver import (
    DriverStoppedError,
    FieldError,
    get_optional_int_value,
    get_optional_string_value,
    get_required_int_value,
    get_required_string_value,
    new_database_configuration,
    optional_number_field,
    optional_password_field,
    required_string_field,
    url_from_database_configuration,
)
from eds.util.gojson import stringify


def test_driver_field_gojson() -> None:
    assert stringify(optional_number_field("Port", "The port to database", 5432)) == (
        '{"name":"Port","type":"number","default":"5432","description":"The port to database","required":false}'
    )
    assert stringify(required_string_field("Database", "The database name to use", None)) == (
        '{"name":"Database","type":"string","description":"The database name to use","required":true}'
    )
    assert stringify(optional_password_field("Password", "The password to database", None)) == (
        '{"name":"Password","type":"string","format":"password",'
        '"description":"The password to database","required":false}'
    )


def test_new_database_configuration() -> None:
    fields = new_database_configuration(5432)
    assert [f.name for f in fields] == ["Database", "Username", "Password", "Hostname", "Port"]
    assert fields[4].default == "5432"
    assert [f.name for f in new_database_configuration(-1)] == ["Database", "Username", "Password", "Hostname"]


@pytest.mark.parametrize(
    ("schema", "defport", "values", "expected"),
    [
        ("postgres", 5432, {"Database": "mydb", "Hostname": "localhost"}, "postgres://localhost:5432/mydb"),
        ("postgres", 5432, {"Database": "db", "Hostname": "h", "Username": "u", "Password": "p", "Port": 6543},
         "postgres://u:p@h:6543/db"),
        # Port as a float (JSON-decoded the Go map[string]any way) falls through to the default.
        ("postgres", 5432, {"Database": "db", "Hostname": "h", "Username": "u", "Password": "p", "Port": 6543.0},
         "postgres://u:p@h:5432/db"),
        ("postgres", 5432, {"Database": "db", "Hostname": "h", "Username": "u"}, "postgres://u:@h:5432/db"),
        ("mysql", 3306, {"Database": "db", "Hostname": "h"}, "mysql://h:3306/db"),
        ("snowflake", -1, {"Database": "db", "Hostname": "acct"}, "snowflake://acct/db"),
    ],
)
def test_url_from_database_configuration(schema, defport, values, expected) -> None:
    url, errors = url_from_database_configuration(schema, defport, values)
    assert errors == []
    assert url == expected


def test_url_from_database_configuration_missing_fields() -> None:
    url, errors = url_from_database_configuration("postgres", 5432, {})
    assert url == ""
    assert errors == [
        FieldError("Hostname", "required field Hostname not found"),
        FieldError("Database", "required field Database not found"),
    ]


def test_config_getters() -> None:
    assert get_required_string_value("x", {"x": "v"}) == ("v", None)
    assert get_required_string_value("x", {"x": ""}) == ("", None)  # empty string is valid
    assert get_required_string_value("x", {}) == ("", FieldError("x", "required field x not found"))
    assert get_optional_string_value("x", "d", {"x": ""}) == "d"  # empty treated as absent
    assert get_optional_string_value("x", "d", {"x": "v"}) == "v"
    # int getters reject float + bool; optional accepts a numeric string via atoi.
    assert get_required_int_value("x", {"x": 5}) == (5, None)
    assert get_required_int_value("x", {"x": 5.0})[1] is not None  # float rejected
    assert get_required_int_value("x", {"x": True})[1] is not None  # bool rejected
    assert get_optional_int_value("x", 9, {"x": 5}) == 5
    assert get_optional_int_value("x", 9, {"x": "7"}) == 7  # string via Atoi
    assert get_optional_int_value("x", 9, {"x": 5.0}) == 9  # float -> default
    assert get_optional_int_value("x", 9, {"x": " 7 "}) == 9  # Atoi rejects whitespace


def test_field_error_is_exception_and_serializes() -> None:
    e = FieldError("Port", "bad port")
    assert str(e) == "bad port"
    assert stringify(e) == '{"field":"Port","error":"bad port"}'  # key is "error", not "message"


def test_driver_stopped_error() -> None:
    assert str(DriverStoppedError()) == "driver stopped"


def test_ansi_strip() -> None:
    assert d.ansi_strip("\x1b[1mbold\x1b[0m text") == "bold text"


# ---- registry ----

class _FakeDriver:
    """Implements Driver + DriverHelp + DriverAlias (no migration)."""

    def __init__(self) -> None:
        self.started_with = None

    def start(self, config) -> None:
        self.started_with = config

    def stop(self) -> None: ...
    def max_batch_size(self) -> int:
        return -1

    def process(self, logger, event) -> bool:
        return False

    def flush(self, logger) -> None: ...
    def test(self, ctx, logger, url) -> None: ...
    def configuration(self):
        return []

    def validate(self, values):
        return "ok-url", []

    def name(self) -> str:
        return "Fake"

    def description(self) -> str:
        return "a fake driver"

    def example_url(self) -> str:
        return "fake://folder"

    def help(self) -> str:
        return "\x1b[1mFake help\x1b[0m"

    def aliases(self) -> list[str]:
        return ["fk"]


def test_registry_resolution_and_start() -> None:
    d.reset_registries()
    fake = _FakeDriver()
    d.register_driver("fake", fake)
    # resolve by scheme, and by alias
    drv = d.new_driver(None, None, "fake://x", None, None, "/data")
    assert drv is fake
    assert fake.started_with is not None
    assert fake.started_with.url == "fake://x"
    drv2 = d.new_driver(None, None, "fk://x", None, None, "/data")  # alias
    assert drv2 is fake
    # metadata-for-url
    meta = d.get_driver_metadata_for_url("fake://x")
    assert meta is not None
    assert meta.name == "Fake"
    assert meta.help == "\x1b[1mFake help\x1b[0m"  # RAW (not stripped) in get_driver_metadata_for_url
    assert meta.supports_migration is False
    # configurations: help is ANSI-stripped here
    cfgs = d.get_driver_configurations()
    assert cfgs["fake"].metadata.help == "Fake help"
    # validate routes to the driver
    assert d.validate("fake", {}) == ("ok-url", [])
    d.reset_registries()


def test_registry_unknown_protocol_raises() -> None:
    d.reset_registries()
    with pytest.raises(ValueError, match="no driver registered for protocol nope"):
        d.new_driver(None, None, "nope://x", None, None, "/data")
