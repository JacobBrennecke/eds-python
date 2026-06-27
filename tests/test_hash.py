"""PARITY: internal/util/hash_test.go — golden vectors for Hash and Modulo (captured from Go)."""

from __future__ import annotations

import pytest

from eds.util.hash import hash, modulo


@pytest.mark.parametrize(
    ("vals", "want"),
    [
        ((), "ef46db3751d8e999"),  # Empty input
        (("hello",), "26c7827d889f6da3"),  # Single value
        (("hello", 42, True), "d481b75d0fa4abff"),  # Multiple values
        (("hello", 42, True, None), "a668199a6b3fc355"),  # Multiple values with nil
        ((None,), "7c5b4e400f80bf7c"),  # Nil only
    ],
)
def test_hash(vals: tuple[object, ...], want: str) -> None:
    assert hash(*vals) == want


@pytest.mark.parametrize(
    ("val", "num", "want"),
    [
        ("", 10, 1),  # Empty input
        ("1", 1, 0),  # Single
        ("1", 2, 0),  # Double
        ("1", 3, 1),  # Triple
        ("1 2 3 4", 10, 5),  # Multiple
    ],
)
def test_modulo(val: str, num: int, want: int) -> None:
    assert modulo(val, num) == want
