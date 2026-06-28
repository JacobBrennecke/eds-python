"""PARITY: internal/drivers/file/file.go + util.ToFileURI — golden vectors (Go + C# FileDriver/ParityQuirk)."""

from __future__ import annotations

import os

import pytest

from eds.dbchange import DBChangeEvent
from eds.driver import DriverConfig
from eds.drivers.file import FileDriver
from eds.util.file import to_file_uri
from eds.util.gojson import RawJson, stringify


class _QuietLogger:
    def trace(self, m, *a): ...
    def debug(self, m, *a): ...
    def info(self, m, *a): ...
    def warn(self, m, *a): ...
    def error(self, m, *a): ...
    def fatal(self, m, *a): ...
    def with_prefix(self, p): return self
    def with_fields(self, f): return self


@pytest.mark.parametrize(
    ("directory", "file", "expected"),
    [
        ("/var/folders/60/rf284h4d67g343wcswq6jwmr0000gn/T/eds-import2764310919", "*.ndjson.gz",
         "file:///var/folders/60/rf284h4d67g343wcswq6jwmr0000gn/T/eds-import2764310919/*.ndjson.gz"),
        ("/var/folders/60/rf284h4d67g343wcswq6jwmr0000gn/T/eds-import2764310919/", "*.ndjson.gz",
         "file:///var/folders/60/rf284h4d67g343wcswq6jwmr0000gn/T/eds-import2764310919/*.ndjson.gz"),  # trailing /
        ("c:/foo/bar", "*.ndjson.gz", "file://c:/foo/bar/*.ndjson.gz"),  # 2 slashes, drive preserved
        ("C:/data", "f.json", "file://C:/data/f.json"),
        ("/data", "f.json", "file:///data/f.json"),  # unix-abs stays file:/// even on Windows
    ],
)
def test_to_file_uri(directory, file, expected) -> None:
    assert to_file_uri(directory, file) == expected


def test_get_file_name() -> None:
    assert FileDriver.get_file_name("customer", 1_700_000_000_000, "c1") == "customer/1700000000-c1.json"


def test_get_path_from_url_preserves_drive_roundtrip(tmp_path) -> None:
    # §8.11: the URL carries the drive in the host (file://C:/...); the corrected driver keeps it.
    driver = FileDriver()
    url = "file://" + str(tmp_path).replace("\\", "/")
    result = driver.get_path_from_url(url)
    assert os.path.normcase(result) == os.path.normcase(str(tmp_path))


def test_process_writes_file(tmp_path) -> None:
    driver = FileDriver()
    url = "file://" + str(tmp_path).replace("\\", "/")
    driver.start(DriverConfig(url=url, logger=_QuietLogger()))
    evt = DBChangeEvent(
        operation="INSERT", id="evt1", table="customer", key=["c1"], model_version="v1",
        timestamp=1_700_000_000_000, mvcc_timestamp="m", after=RawJson('{"id":"c1","name":"Bob"}'),
    )
    assert driver.process(_QuietLogger(), evt) is False
    fp = tmp_path / "customer" / "1700000000-c1.json"
    assert fp.exists()
    content = fp.read_text(encoding="utf-8")
    assert content == stringify(evt)  # exact gojson, no trailing newline
    assert '"operation":"INSERT"' in content


def test_validate_valid_dir(tmp_path) -> None:
    url, errors = FileDriver().validate({"Directory": str(tmp_path)})
    assert errors == []
    assert url == "file://" + str(tmp_path).replace("\\", "/")
    assert "\\" not in url


def test_validate_missing_directory() -> None:
    url, errors = FileDriver().validate({"Format": "json"})
    assert url == ""
    assert len(errors) >= 1


def test_validate_root_directory() -> None:
    url, errors = FileDriver().validate({"Directory": "/"})
    assert url == ""
    assert "root directory" in errors[0].message


def test_metadata() -> None:
    d = FileDriver()
    assert d.name() == "File"
    assert d.description() == "Supports streaming EDS messages to local filesystem directory."
    assert d.example_url() == "file://folder"
    assert d.max_batch_size() == -1
    assert d.supports_delete() is False
    assert [f.name for f in d.configuration()] == ["Directory"]
