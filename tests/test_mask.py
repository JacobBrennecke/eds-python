"""PARITY: internal/util/mask_test.go — golden vectors for masking (captured from Go)."""

from __future__ import annotations

import pytest

from eds.util.mask import mask_arguments, mask_url


@pytest.mark.parametrize(
    ("url", "want"),
    [
        (
            "http://user:password@localhost:8080/path?query=1",
            "http://us**:pass****@localhost:8080/pa**?query=*",
        ),
        (
            "snowflake://FOO:thisisapassword@TFLXCJY-LU41011/TEST/PUBLIC",
            "snowflake://F**:thisisa********@TFLXCJY-LU41011/TEST/******",
        ),
        (
            "s3://bucket/folder?region=us-west-2&access-key-id=AKIAIOSFODNN7EXAMPLE"
            "&secret-access-key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "s3://bucket/fol***?access-key-id=AKIAIOSFOD**********&region=us-w*****"
            "&secret-access-key=wJalrXUtnFEMI/K7MDEN********************",
        ),
    ],
)
def test_mask_url(url: str, want: str) -> None:
    assert mask_url(url) == want


@pytest.mark.parametrize(
    ("args", "want"),
    [
        (
            [
                "https://alice:bob@example.com/a/b?foo=bar",
                "http://user:password@localhost:8080/path?query=1",
                "s3://bucket/folder?region=us-west-2&access-key-id=AKIAIOSFODNN7EXAMPLE&secret-access",
                "mysql://user:password@localhost:3306/db?query=1",
            ],
            [
                "https://al***:b**@example.com/a**?foo=b**",
                "http://us**:pass****@localhost:8080/pa**?query=*",
                "s3://bucket/fol***?access-key-id=AKIAIOSFOD**********&region=us-w*****&secret-access=",
                "mysql://us**:pass****@localhost:3306/d*?query=*",
            ],
        ),
        (
            ["user@example.com", "another.user@example.com"],
            ["us**@exa****.com", "anothe******@exa****.com"],
        ),
        (
            [
                "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwi"
                "aWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
            ],
            [
                "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6Ikpv"
                "******************************************************************************"
            ],
        ),
        (["hello", "world"], ["hello", "world"]),
        (
            ["http://example.com", "user@example.com", "hello"],
            ["http://example.com", "us**@exa****.com", "hello"],
        ),
    ],
)
def test_mask_arguments(args: list[str], want: list[str]) -> None:
    assert mask_arguments(args) == want
