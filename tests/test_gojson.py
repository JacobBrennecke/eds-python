"""Go json.Marshal + float-formatting parity. Vectors verified against Go by the C# port
(JsonTests / GoFloatTests)."""

from __future__ import annotations

import pytest

from eds.util.gofloat import format_f
from eds.util.gojson import RawJson, compact_raw, marshal, stringify


def test_compact_raw_matches_go_marshal_of_rawmessage() -> None:
    # Go json.Marshal of a RawMessage: drop insignificant whitespace, HTML-escape < > & and U+2028/U+2029,
    # but preserve number formatting + key order.
    assert compact_raw('{"b": 1.50, "a": "x&y<z>"}') == '{"b":1.50,"a":"x\\u0026y\\u003cz\\u003e"}'
    # LS/PS built via chr() so this test file contains no invisible characters.
    ls, ps = chr(0x2028), chr(0x2029)
    assert compact_raw('{"u":"a' + ls + 'b' + ps + 'c"}') == '{"u":"a\\u2028b\\u2029c"}'
    # idempotent on already-compact + escaped input (the streaming "after" path)
    already = '{"a":"x\\u0026y","n":1.5}'
    assert compact_raw(already) == already


def test_rawjson_is_compacted_and_escaped_on_marshal() -> None:
    # A RawJson marshaled via stringify is compacted + HTML-escaped (NOT emitted verbatim).
    assert stringify(RawJson('{"k": "a&b"}')) == '{"k":"a\\u0026b"}'


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
