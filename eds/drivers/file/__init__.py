"""PARITY: internal/drivers/file/file.go — the File driver (streams each EDS event to an NDJSON-ish .json file).

Not a SQL driver (no SqlDriverBase); it writes one ``<dir>/<table>/<unix>-<id>.json`` per event, content = the
Go-exact gojson.stringify(event) (no trailing newline). §8.11 DECISION: get_path_from_url CORRECTS Go's
Windows drive-letter drop (like the reviewed C#) — the file driver emits JSON not SQL, so no golden SQL vector
is perturbed, and the Go bug makes the Validate→Start round-trip unusable on Windows. The import run-loop
(importer.Run) is M5; run_import sets up then defers (the Handler methods here are import-ready).
"""

from __future__ import annotations

import os
import re
from typing import Any

from eds.dbchange import DBChangeEvent
from eds.driver import (
    DriverConfig,
    DriverField,
    FieldError,
    ImporterConfig,
    get_required_string_value,
    new_field_error,
    required_string_field,
)
from eds.schema import Schema, SchemaMap
from eds.util import gourl
from eds.util.file import exists, is_dir_writable
from eds.util.gojson import stringify
from eds.util.logger import Logger

# DEVIATION: file-uri-windows-drive-letter — distinct from util._WIN_DRIVE (that one requires a trailing slash);
# here Go's url.Parse puts the drive in the host (e.g. "C:"), so match a bare drive-colon host.
_DRIVE_LETTER_HOST = re.compile(r"^[A-Za-z]:$")


class FileDriver:
    """PARITY: fileDriver."""

    def __init__(self) -> None:
        self._config: DriverConfig | None = None
        self._logger: Logger | None = None
        self._dir = ""
        self._import_config: ImporterConfig | None = None

    def get_path_from_url(self, url_string: str) -> str:
        """PARITY: GetPathFromURL (§8.11 corrected). Go only consulted u.Path, dropping the Windows drive in
        u.Host; we keep it. (Go's dead `else: filepath.Abs(p.dir)` branch — using the empty struct field, not
        u.Path — is preserved here as a comment, not reproduced.)"""
        try:
            u = gourl.parse(url_string)
        except ValueError as e:
            raise ValueError(f"unable to parse url: {e}") from e
        if _DRIVE_LETTER_HOST.match(u.host):  # DEVIATION: Go drops the drive letter (§8.11)
            directory = u.host + u.path  # "C:" + "/foo" -> "C:/foo"
        elif u.path:
            directory = u.path
        else:
            raise ValueError("path is required in url which should be the directory to store files")
        directory = os.path.abspath(directory)
        if not exists(directory):
            os.makedirs(directory, mode=0o755, exist_ok=True)
        self._dir = directory
        return directory

    def start(self, config: DriverConfig) -> None:
        """PARITY: Start."""
        assert config.logger is not None
        self._config = config
        self._logger = config.logger.with_prefix("[file]")
        self.get_path_from_url(config.url)

    def stop(self) -> None:
        """PARITY: Stop — no-op."""

    def max_batch_size(self) -> int:
        return -1  # PARITY: no limit

    @staticmethod
    def get_file_name(table: str, timestamp_ms: int, id: str) -> str:
        """PARITY: getFileName — "<table>/<unix-seconds>-<id>.json" (literal '/', joined later)."""
        return f"{table}/{timestamp_ms // 1000}-{id}.json"

    def write_event(self, logger: Logger, event: DBChangeEvent, dry_run: bool) -> None:
        """PARITY: writeEvent — content is gojson.stringify(event) bytes, no trailing newline."""
        key = self.get_file_name(event.table, event.timestamp, event.get_primary_key())
        buf = stringify(event).encode("utf-8")
        fp = os.path.join(self._dir, *key.split("/"))  # PARITY: filepath.Join turns '/' into os.sep
        if not dry_run:
            directory = os.path.dirname(fp)
            if not exists(directory):
                os.makedirs(directory, mode=0o755, exist_ok=True)
            fd = os.open(fp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
            with os.fdopen(fd, "wb") as f:
                f.write(buf)
            logger.trace("stored %s", fp)
        else:
            logger.trace("would have stored %s", fp)

    def process(self, logger: Logger, event: DBChangeEvent) -> bool:
        """PARITY: Process — write the event; never request a flush."""
        self.write_event(logger, event, False)
        return False

    def flush(self, logger: Logger) -> None:
        """PARITY: Flush — no-op."""

    # ---- DriverHelp ----
    def name(self) -> str:
        return "File"

    def description(self) -> str:
        # PARITY: keep the Go string verbatim (the C# port drifted to "...folder on the local filesystem.").
        return "Supports streaming EDS messages to local filesystem directory."

    def example_url(self) -> str:
        return "file://folder"

    def help(self) -> str:
        # PARITY: Go returns this raw string (no GenerateHelpSection wrapper).
        return "Provide a directory in the URL path to store events into this folder.\n"

    # ---- import Handler (driven by the M5 runner) ----
    def create_datasource(self, schema: SchemaMap) -> None:
        """PARITY: CreateDatasource — no-op."""

    def import_event(self, event: DBChangeEvent, schema: Schema) -> None:
        """PARITY: ImportEvent."""
        assert self._logger is not None
        dry_run = self._import_config.dry_run if self._import_config is not None else False
        self.write_event(self._logger, event, dry_run)

    def import_completed(self) -> None:
        """PARITY: ImportCompleted — no-op."""

    def run_import(self, config: ImporterConfig) -> None:
        """PARITY: Import."""
        if config.schema_only:  # PARITY: Go short-circuits before any setup
            return
        assert config.logger is not None
        self._logger = config.logger.with_prefix("[file]")
        self.get_path_from_url(config.url)
        self._import_config = config
        raise NotImplementedError("import run loop (importer.Run) lands at M5")

    def supports_delete(self) -> bool:
        return False

    def test(self, ctx: Any, logger: Logger, url: str) -> None:
        """PARITY: Test — resolve/create the dir only (Go does NOT set the logger here)."""
        self.get_path_from_url(url)

    def configuration(self) -> list[DriverField]:
        return [required_string_field("Directory", "The directory on the server to store files", None)]

    def validate(self, values: dict[str, Any]) -> tuple[str, list[FieldError]]:
        """PARITY: Validate."""
        field_errors: list[FieldError] = []
        directory, field_error = get_required_string_value("Directory", values)
        if field_error is not None:
            field_errors.append(field_error)
        if directory == "/":
            return "", [new_field_error("Directory", "cannot be the root directory")]
        absdir = os.path.abspath(directory)
        if not exists(absdir):
            parent = os.path.dirname(absdir)
            ok, msg = is_dir_writable(parent)
            if not ok:
                return "", [
                    new_field_error(
                        "Directory",
                        f"{parent} directory isn't writable and directory currently does not exist: {msg}",
                    )
                ]
        else:
            ok, _msg = is_dir_writable(absdir)
            if not ok:
                return "", [new_field_error("Directory", f"{absdir} directory isn't writable")]
        if field_errors:
            return "", field_errors
        return "file://" + absdir.replace("\\", "/"), []
