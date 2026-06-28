"""PARITY: internal/importer/importer.go — the import Handler protocol + the replay run-loop.

run() replays CRDB changefeed export files (``*.ndjson.gz``) as synthetic INSERT events into a driver's import
Handler. PARITY: it is single-threaded in Go (config.max_parallel is carried but unused by the loop), so a
sequential Python port is faithful (no threads).
"""

from __future__ import annotations

import os
import time
from typing import Protocol, runtime_checkable

from eds.dbchange import DBChangeEvent
from eds.driver import ImporterConfig
from eds.schema import Schema, SchemaMap, SchemaValidationError
from eds.util.crdb import parse_crdb_export_file
from eds.util.file import list_dir
from eds.util.gojson import RawJson, stringify
from eds.util.hash import hash as eds_hash
from eds.util.json import NDJSONDecoder
from eds.util.logger import Logger


@runtime_checkable
class Handler(Protocol):
    """PARITY: importer.Handler (the import-handler interface)."""

    def create_datasource(self, schema: SchemaMap) -> None: ...
    def import_event(self, event: DBChangeEvent, schema: Schema) -> None: ...
    def import_completed(self) -> None: ...


def _dur(seconds: float) -> str:
    # DEVIATION (duration-format): log-only, not byte-checked; Go renders time.Duration.String().
    if seconds < 1e-3:
        return f"{seconds * 1e6:.3f}µs"
    if seconds < 1.0:
        return f"{seconds * 1e3:.3f}ms"
    return f"{seconds:.3f}s"


def run(logger: Logger, config: ImporterConfig, handler: Handler) -> None:
    """PARITY: importer.Run."""
    started = time.monotonic()
    try:
        schema = config.schema_registry.get_latest_schema()  # type: ignore[union-attr]
    except Exception as e:
        raise RuntimeError(f"unable to get schema: {e}") from e
    if not config.no_delete:
        handler.create_datasource(schema)
    if config.schema_only:  # PARITY: AFTER create_datasource
        return
    total = 0
    try:
        files = list_dir(config.data_dir)
    except OSError as e:
        raise RuntimeError(f"unable to list files in directory: {e}") from e

    for file in files:
        table, unix_nano, ok = parse_crdb_export_file(file)
        if not ok:
            logger.debug("skipping file: %s", file)
            continue
        if table not in config.tables:  # PARITY: silent skip (no log)
            continue
        data = schema.get(table)
        if data is None:
            raise RuntimeError(
                f"unexpected table ({table}) not found in schema but in import directory: {file}"
            )
        logger.debug("processing file: %s, table: %s", file, table)
        event_id = eds_hash(os.path.basename(file))  # PARITY: util.Hash(filepath.Base(file)) — constant per file
        timestamp_ms = unix_nano // 1_000_000  # PARITY: tv.UnixMilli()
        mvcc = str(unix_nano)  # PARITY: fmt.Sprintf("%v", tv.UnixNano())

        try:
            dec = NDJSONDecoder.open(file)
        except OSError as e:
            raise RuntimeError(f"unable to create JSON decoder for {file}: {e}") from e
        count = 0
        tstarted = time.monotonic()
        try:
            while dec.more():
                try:
                    raw = dec.decode_raw()
                except ValueError as e:  # json.JSONDecodeError is a ValueError
                    raise RuntimeError(f"unable to decode JSON: {e}") from e
                event = DBChangeEvent(
                    operation="INSERT", table=table, timestamp=timestamp_ms, mvcc_timestamp=mvcc,
                    id=event_id, model_version=data.model_version, after=RawJson(raw),
                )
                try:
                    event.key = [event.get_primary_key()]
                except ValueError:
                    # DEVIATION (getobject-error-order): Go's GetPrimaryKey swallows the error and sets Key=[""];
                    # the wrapped "unable to get object" surfaces at the explicit get_object() below.
                    event.key = [""]
                try:
                    o = event.get_object()
                except ValueError as e:
                    raise RuntimeError(f"unable to get object: {e}") from e
                if o is not None:
                    lid = o.get("locationId")
                    if isinstance(lid, str):
                        event.location_id = lid
                    cid = o.get("companyId")
                    if isinstance(cid, str):
                        event.location_id = cid  # PARITY: companyId→locationId copy/paste quirk (companyId wins)
                    uid = o.get("userId")
                    if isinstance(uid, str):
                        event.user_id = uid
                event.imported = True

                if config.schema_validator is not None:
                    try:
                        found, valid, path = config.schema_validator.validate(event)
                    except SchemaValidationError as e:
                        logger.debug(
                            "skipping %s, schema did not validate (%s) for event: %s",
                            event.table, str(e).replace("\n", " ").strip(), stringify(event),
                        )
                        continue
                    except Exception as e:
                        raise RuntimeError(f"error validating schema: {e}") from e
                    if not found:
                        logger.trace("skipping %s, no schema found for event: %s", event.table, stringify(event))
                        continue
                    if not valid:  # PARITY: dead branch (mismatch comes via SchemaValidationError)
                        logger.trace(
                            "skipping %s, schema did not validate for event: %s", event.table, stringify(event)
                        )
                        continue
                    if path != "":
                        event.schema_validated_path = path
                        logger.trace("schema validated %s", path)

                count += 1
                handler.import_event(event, data)
        finally:
            dec.close()

        total += count
        logger.debug("imported %d %s records in %s", count, table, _dur(time.monotonic() - tstarted))

    handler.import_completed()
    logger.info("imported %d records from %d files in %s", total, len(files), _dur(time.monotonic() - started))
