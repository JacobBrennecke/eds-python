"""Go json.Marshal + float-formatting parity. Vectors verified against Go by the C# port
(JsonTests / GoFloatTests)."""

from __future__ import annotations

import pytest

from eds.util.gofloat import format_f
from eds.util.gojson import RawJson, marshal, stringify


def test_string_escaping_matches_go() -> None:
    # Go json.Marshal("a<b>&\"\\é") -> "a<b>&\"\\é"
    assert stringify("a<b>&\"\\é") == '"a\\u003cb\\u003e\\u0026\\"\\\\é"'


def test_control_chars() -> None:
    # \n \r \t short forms; other controls \u00XX (no \b/\f short forms).
    assert stringify("\n\r\t") == '"\\n\\r\\t"'
    assert stringify("\b\f\x00\x1f") == '"\\u0008\\u000c\\u0000\\u001f"'


def test_scalars() -> None:
    assert stringify(None) == "null"
    assert stringify(True) == "true"
    assert stringify(False) == "false"
    assert stringify(42) == "42"
    assert stringify("x") == '"x"'


def test_dict_sorts_keys_like_go() -> None:
    assert stringify({"zebra": 1.0, "apple": "x", "mango": True}) == '{"apple":"x","mango":true,"zebra":1}'


def test_nesting_and_rawjson_and_lists() -> None:
    assert marshal(["a", 1, True, None]) == '["a",1,true,null]'
    assert marshal({"after": RawJson('{"id":"pk"}'), "key": ["pk"]}) == '{"after":{"id":"pk"},"key":["pk"]}'
    assert marshal([]) == "[]"
    assert marshal({}) == "{}"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (3.0, "3"),
        (0.5, "0.5"),
        (12345.6789, "12345.6789"),
        (-2.5, "-2.5"),
        (0.0, "0"),
        (1e20, "100000000000000000000"),
        (1.7e18, "1700000000000000000"),
        (1e-6, "0.000001"),
        (1e-7, "1e-7"),
        (1e-8, "1e-8"),
        (1e-10, "1e-10"),
        (6.022e23, "6.022e+23"),
        (1e21, "1e+21"),
    ],
)
def test_json_float_matches_go(value: float, expected: str) -> None:
    assert stringify({"v": value}) == '{"v":' + expected + "}"


def test_stringify_empty_on_non_finite() -> None:
    # PARITY: util.JSONStringify ignores json.Marshal's error on NaN/Inf and returns "".
    assert stringify({"v": float("nan")}) == ""
    assert stringify({"v": float("inf")}) == ""
    assert stringify({"v": float("-inf")}) == ""


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (42.0, "42"),
        (9.5, "9.5"),
        (100.0, "100"),
        (0.0001, "0.0001"),
        (1e-7, "0.0000001"),  # FormatF is always plain decimal (unlike the json encoder)
        (-3.25, "-3.25"),
    ],
)
def test_format_f_matches_go(value: float, expected: str) -> None:
    assert format_f(value) == expected
