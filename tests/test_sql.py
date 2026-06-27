"""PARITY: internal/util/sql.go — value coercion + §8.1 asymmetric scalar regex.
Vectors from the C# SqlValues tests + ParityQuirkTests."""

from __future__ import annotations

import pytest

from eds.schema import SchemaProperty
from eds.util.sql import is_empty_val, quote_identifier, quote_json_scalar, to_json_string_val

_OBJ = SchemaProperty(type="object")
_ARR = SchemaProperty(type="array")
_STR = SchemaProperty(type="string")


def test_quote_identifier() -> None:
    assert quote_identifier("user") == '"user"'


def test_is_empty_val() -> None:
    assert all(is_empty_val(v) for v in ("''", "", "NULL", "null"))
    assert not is_empty_val("x")
    assert not is_empty_val("0")


@pytest.mark.parametrize(
    ("val", "prop", "quote", "expected"),
    [
        ("NULL", _OBJ, True, "'{}'"),  # empty not-null object -> '{}'
        ("", _ARR, True, "'[]'"),  # empty not-null array -> '[]'
        ("42", _OBJ, True, "'42'"),  # object scalar gets quoted
        ("42", _STR, True, "42"),  # string column not quoted
        ("42", _OBJ, False, "42"),  # quote_scalar=False
    ],
)
def test_to_json_string_val(val: str, prop: SchemaProperty, quote: bool, expected: str) -> None:
    assert to_json_string_val("x", val, prop, quote) == expected


@pytest.mark.parametrize(
    ("val", "expected"),
    [
        ("123abc", "'123abc'"),  # ^number is start-anchored only -> matches the "123" prefix
        ("abctrue", "'abctrue'"),  # (true|false)\Z is end-anchored only -> matches the "true" suffix
        ("abc123", "abc123"),  # neither -> NOT quoted
        ("true\n", "true\n"),  # \Z is absolute end -> a trailing newline blocks the match
        ("123", "'123'"),
        ("-3.14", "'-3.14'"),
        ("true", "'true'"),
        ("false", "'false'"),
    ],
)
def test_quote_json_scalar_asymmetric_anchoring(val: str, expected: str) -> None:
    assert quote_json_scalar(val, _OBJ) == expected


def test_quote_json_scalar_fullwidth_digits_not_matched() -> None:
    # [0-9] is ASCII-only (RE2 and Python here) -> fullwidth digits do not match.
    assert quote_json_scalar("１２３", _OBJ) == "１２３"


def test_quote_json_scalar_only_object_columns() -> None:
    assert quote_json_scalar("123", _STR) == "123"
