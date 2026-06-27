"""PARITY: internal/util/zip.go + docker.go + internal/osext."""

from __future__ import annotations

import gzip
import os

from eds.osext import executable, executable_folder
from eds.util.compress import gunzip, gzip_file
from eds.util.docker import is_running_inside_docker


def test_gunzip() -> None:
    assert gunzip(gzip.compress(b"hello world")) == b"hello world"


def test_gzip_file(tmp_path) -> None:
    p = tmp_path / "f.txt"
    p.write_bytes(b"hello world")
    gzip_file(str(p))
    gz = tmp_path / "f.txt.gz"
    assert gz.exists()
    assert gunzip(gz.read_bytes()) == b"hello world"


def test_is_running_inside_docker_returns_bool() -> None:
    assert isinstance(is_running_inside_docker(), bool)


def test_executable() -> None:
    exe = executable()
    assert exe and os.path.exists(exe)
    assert executable_folder() == os.path.dirname(exe)
