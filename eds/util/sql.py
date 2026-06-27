"""PARITY: internal/util/sql.go — SQL identifier quoting + the JSON value-coercion helpers + ToUserPass.

``SQLExecuter`` / ``DropTable`` are DB-connection bound and live in eds.util.db.
"""

from __future__ import annotations

import re

from eds.schema import SchemaProperty
from eds.util.gourl import GoUrl


def to_user_pass(u: GoUrl) -> str:
    """PARITY: util.ToUserPass — decoded ``user[:password]`` ("" when there is no userinfo). The password
    (incl. an empty one) keeps its colon whenever the colon was present in the URL."""
    if not u.has_user_info:
        return ""
    if u.has_password:
        return f"{u.username}:{u.password}"
    return u.username

# PARITY: sql.go scalarValue. The anchoring is ASYMMETRIC (SPEC §8.1): `^` binds only the number
# alternative and `$` only `(true|false)`. With re.search this matches a string that EITHER starts with a
# number OR ends with true/false (so "123abc" and "abctrue" both match, "abc123" does not). `[0-9]` is ASCII
# (fullwidth digits never match); `\Z` is Go's absolute-end `$` (a trailing newline blocks the match — Python
# `$` would not). MUST use re.search, not re.match (re.match would never reach the end-anchored alternative).
_SCALAR_VALUE = re.compile(r"^([+-]?([0-9]*[.])?[0-9]+)|(true|false)\Z")


def quote_identifier(name: str) -> str:
    """PARITY: util.QuoteIdentifier — wrap in double quotes."""
    return '"' + name + '"'


def quote_string_identifiers(vals: list[str]) -> list[str]:
    """PARITY: util.QuoteStringIdentifiers."""
    return [quote_identifier(v) for v in vals]


def is_empty_val(val: str) -> bool:
    """PARITY: util.isEmptyVal."""
    return val in ("''", "", "NULL", "null")


def to_json_string_val(name: str, val: str, prop: SchemaProperty, quote_scalar: bool) -> str:
    """PARITY: util.ToJSONStringVal — coerce empty not-null array/object columns to '[]'/'{}',
    optionally quoting JSON scalar values."""
    if prop.is_array_or_json() and prop.is_not_null() and is_empty_val(val):
        if prop.type == "array":
            return "'[]'"
        if prop.type == "object":
            return "'{}'"
    if quote_scalar:
        return quote_json_scalar(val, prop)
    return val


def quote_json_scalar(val: str, prop: SchemaProperty) -> str:
    """PARITY: util.quoteJSONScalar — for object columns, single-quote a value that looks like a
    number or boolean (per the asymmetric scalarValue regex)."""
    if prop.type == "object" and _SCALAR_VALUE.search(val):
        return "'" + val + "'"
    return val
