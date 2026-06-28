"""PARITY: internal/util/util.go (ParseCRDBExportFile/parsePreciseDate) + json.go (NDJSON) — golden vectors."""

from __future__ import annotations

import gzip

import pytest

from eds.util.crdb import parse_crdb_export_file, parse_precise_date
from eds.util.json import NDJSONDecoder

_EXAMPLE = "202407242003015854988560000000000-abc-def-customer-2.ndjson.gz"


def test_parse_precise_date() -> None:
    assert parse_precise_date("202407242003015854988560000000000") == (1721851381585498856, True)


def test_parse_precise_date_too_short() -> None:
    assert parse_precise_date("2024") == (0, False)


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        (_EXAMPLE, ("customer", 1721851381585498856, True)),
        ("202407242003015854988560000000000-abc-def-user-2.ndjson.gz", ("user", 1721851381585498856, True)),
        ("202407242003015854988560000000000-abc-def-user-14a.ndjson.gz", ("user", 1721851381585498856, True)),
        ("202407242003015854988560000000000-abc-def-labor_rate-2.ndjson.gz",
         ("labor_rate", 1721851381585498856, True)),
        ("202407131650522808024600000000000-c7274317e9a4a9cb-1-651-00000000-user-2.ndjson.gz",
         ("user", 1720889452280802460, True)),
        ("not-a-changefeed-file.txt", ("", 0, False)),
    ],
)
def test_parse_crdb_export_file(filename, expected) -> None:
    assert parse_crdb_export_file(filename) == expected


def test_parse_crdb_export_file_strips_dir(tmp_path) -> None:
    # basename is used, so a full path resolves the same.
    assert parse_crdb_export_file(str(tmp_path / _EXAMPLE)) == ("customer", 1721851381585498856, True)


def test_ndjson_decoder_gz(tmp_path) -> None:
    fn = tmp_path / "f.ndjson.gz"
    with gzip.open(fn, "wt", encoding="utf-8") as f:
        f.write('{"id":"c1","companyId":"comp1"}\n{"id":"c2"}\n')
    dec = NDJSONDecoder.open(str(fn))
    try:
        assert dec.more() is True
        assert dec.decode_raw() == '{"id":"c1","companyId":"comp1"}'
        assert dec.decode_raw() == '{"id":"c2"}'
        assert dec.more() is False
        assert dec.count() == 2
    finally:
        dec.close()


def test_ndjson_decoder_plain_and_blank_lines(tmp_path) -> None:
    fn = tmp_path / "f.ndjson"
    fn.write_text('{"a":1}\n\n  \n{"b":2}\n', encoding="utf-8")
    with NDJSONDecoder.open(str(fn)) as dec:
        assert dec.decode_raw() == '{"a":1}'
        assert dec.decode_raw() == '{"b":2}'  # blank/whitespace lines skipped
        assert dec.more() is False
        assert dec.count() == 2


def test_ndjson_decoder_malformed_raises(tmp_path) -> None:
    fn = tmp_path / "f.ndjson"
    fn.write_text("{not valid json\n", encoding="utf-8")
    with NDJSONDecoder.open(str(fn)) as dec, pytest.raises(ValueError):
        dec.decode_raw()
