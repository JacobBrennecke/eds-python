"""PARITY: internal/drivers/snowflake/snowflake_keypair.go — the Snowflake key-pair driver.

Subclasses SnowflakeDriver, overriding only auth + metadata + configuration/validate. The PKCS#8 key loading
and snowflake-connector connect are unit-untestable (no account) and lazily imported in _connect_to_db.
"""

from __future__ import annotations

import os
from typing import Any

from eds.driver import (
    DriverField,
    FieldError,
    get_optional_string_value,
    get_required_string_value,
    optional_string_field,
    required_string_field,
)
from eds.drivers.snowflake.snowflake import ISnowflakeDb, SnowflakeDriver
from eds.util import gourl
from eds.util.gourl import GoUrl, Userinfo, query_escape

_SECRET_DEFAULT_ENV = "SNOWFLAKE_SECRET_ACCESS_KEY"


def parse_key_pair_url(url: str) -> tuple[str, str, str, str, str]:
    """PARITY: parse a snowflake-keypair URL → (user, account, database, schema, secret_var)."""
    u = gourl.parse(url)
    parts = u.path.removeprefix("/").split("/")  # PARITY: Go TrimPrefix strips exactly one leading slash
    if len(parts) < 2:
        raise ValueError(f"invalid URL path: expected /database/schema, got {u.path}")
    return u.username, u.host, parts[0], parts[1], u.query().get("secret-key")


class SnowflakeKeyPairDriver(SnowflakeDriver):
    """PARITY: snowflakeKeyPairDriver."""

    def log_prefix(self) -> str:
        return "[snowflake-keypair]"

    def name(self) -> str:
        return "Snowflake Key Pair"

    def description(self) -> str:
        return "Temporary driver for migrating to key-pair authentication for Snowflake"

    def example_url(self) -> str:
        return "snowflake-keypair://user@account/database/schema?secret-key=SECRET_ENV_VAR_NAME"

    def configuration(self) -> list[DriverField]:
        return [
            required_string_field(
                "Database", "The database name including the schema, e.g. DBNAME/SCHEMA", "DBNAME/SCHEMA"
            ),
            required_string_field(
                "Username", "The username to use. Note the user must be associated with the public key", None
            ),
            required_string_field(
                "Account",
                "The full Snowflake account identifier including the organization, e.g. abcdefg-ab12345",
                "abcdefg-ab12345",
            ),
            optional_string_field(
                "Secret",
                "Name of environment variable on the EDS server that contains the unencrypted PKCS#8 private key",
                _SECRET_DEFAULT_ENV,
            ),
        ]

    def validate(self, values: dict[str, Any]) -> tuple[str, list[FieldError]]:
        errors: list[FieldError] = []
        account, err = get_required_string_value("Account", values)
        if err is not None:
            errors.append(err)
        database, err = get_required_string_value("Database", values)
        if err is not None:
            errors.append(err)
        username, err = get_required_string_value("Username", values)
        if err is not None:
            errors.append(err)
        secret = get_optional_string_value("Secret", "", values)
        if errors:
            return "", errors
        u = GoUrl(
            scheme="snowflake-keypair",
            user=Userinfo(username, "", False),
            host=account,
            path="/" + database,
            raw_query="secret-key=" + query_escape(secret),
        )
        return str(u), []

    def _connect_to_db(self, ctx: Any, url: str) -> ISnowflakeDb:
        from eds.drivers.snowflake.datadb import SnowflakeDataDb

        assert self._logger is not None
        _user, account, database, schema, secret_var = parse_key_pair_url(url)
        secret = os.environ.get(secret_var) or os.environ.get(_SECRET_DEFAULT_ENV, "")
        db = SnowflakeDataDb.open_with_key_pair(account, _user, database, schema, secret, self._logger)
        try:
            self._refresh_schema(db, fail_if_empty=False)
        except Exception:
            db.close()
            raise
        return db
