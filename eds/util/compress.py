"""PARITY: go-common/compress (Gunzip) + internal/util/zip.go (GzipFile)."""

from __future__ import annotations

import gzip
import shutil


def gunzip(data: bytes) -> bytes:
    """PARITY: compress.Gunzip — decompress gzip bytes."""
    return gzip.decompress(data)


def gzip_file(filepath: str) -> None:
    """PARITY: util.GzipFile — gzip a file to ``<filepath>.gz``.

    DEVIATION: the gzip byte stream is not identical to Go's (different compressor), but it decompresses
    identically; the ``.gz`` output is never byte-compared (it is read back via gunzip)."""
    with open(filepath, "rb") as infile, gzip.open(filepath + ".gz", "wb") as outfile:
        shutil.copyfileobj(infile, outfile)
