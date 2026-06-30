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
from eds.util.duration import format_duration
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


@runtime_checkable
class ImportFlusher(Protocol):
    """FEATURE(import-recovery): a Handler that can flush + reset its pending buffer at a TABLE boundary, so a
    per-table completion marker reflects a durably-committed write (§2.4). SQL drivers implement it via the
    byte-batch flush; handlers without it fall back to the single end-of-run flush + end-of-run markers."""

    def flush_imported(self) -> None: ...


def run(logger: Logger, config: ImporterConfig, handler: Handler) -> None:
    """PARITY: importer.Run.

    FEATURE(import-recovery): when ``config.recovery_enabled`` is set the per-file replay is grouped by table and
    a flush + ``import-progress:{run_id}:{table}`` marker is written at each table boundary (§2.4); the default
    (disabled) path is byte-for-byte the Go single-flush behavior."""
    started = time.monotonic()
    try:
        schema = config.schema_registry.get_latest_schema()  # type: ignore[union-attr]
    except Exception as e:
        raise RuntimeError(f"unable to get schema: {e}") from e
    if not config.no_delete:
        handler.create_datasource(schema)
    if config.schema_only:  # PARITY: AFTER create_datasource
        return
    try:
        files = list_dir(config.data_dir)
    except OSError as e:
        raise RuntimeError(f"unable to list files in directory: {e}") from e

    def process_file(file: str, table: str, unix_nano: int) -> int:
        # PARITY: the per-file synthetic-event replay body (identical for both the legacy and recovery paths).
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

        logger.debug("imported %d %s records in %s", count, table, format_duration(time.monotonic() - tstarted))
        return count

    if config.recovery_enabled:
        total = _run_recovery(logger, config, handler, files, process_file)
    else:
        total = 0
        for file in files:
            table, unix_nano, ok = parse_crdb_export_file(file)
            if not ok:
                logger.debug("skipping file: %s", file)
                continue
            if table not in config.tables:  # PARITY: silent skip (no log)
                continue
            total += process_file(file, table, unix_nano)
        handler.import_completed()

    logger.info(
        "imported %d records from %d files in %s", total, len(files), format_duration(time.monotonic() - started)
    )


def _write_progress_marker(config: ImporterConfig, table: str) -> None:
    # FEATURE(import-recovery): record `import-progress:{run_id}:{table}` after a table durably flushes (§2.4).
    if config.tracker is not None and config.run_id:
        config.tracker.set_key(f"import-progress:{config.run_id}:{table}", "1")


def _run_recovery(logger: Logger, config: ImporterConfig, handler: Handler, files, process_file) -> int:
    """FEATURE(import-recovery): table-grouped replay with a per-table flush boundary + completion markers.

    PARITY(import-log-verbosity): replay tables in the order their files first appear in the (sorted) directory
    listing — Go's single-pass directory order — so the per-file/per-table verbose lines come out in the same
    order across ports; any configured table that exported NO files is still flushed + marked afterwards, so the
    recovery completion-marker invariant (and cross-restart resume) is unchanged.

    FEATURE(import-log-verbosity): each table emits a verbose-only DEBUG detail pair (``importing table <t>`` /
    ``imported <r> records from <f> files for table <t> in <dur>``) with PER-TABLE counts (that table's
    files/records, NOT the all-files batch total). No Go oracle — identical Python↔C# by design (more
    troubleshooting data). See migration/features/import-log-verbosity.md."""
    files_by_table: dict[str, list[tuple[str, int]]] = {}
    for file in files:
        table, unix_nano, ok = parse_crdb_export_file(file)
        if not ok:
            logger.debug("skipping file: %s", file)
            continue
        if table not in config.tables:  # PARITY: silent skip (no log)
            continue
        files_by_table.setdefault(table, []).append((file, unix_nano))

    # dict insertion order == directory (sorted) order; the zero-file configured tables follow so each still
    # gets a flush + completion marker (the recovery invariant is unchanged — only the replay ORDER aligns).
    ordered_tables = list(files_by_table.keys())
    ordered_tables += [t for t in config.tables if t not in files_by_table]

    flusher = handler if isinstance(handler, ImportFlusher) else None
    total = 0
    processed: list[str] = []
    for table in ordered_tables:
        table_files = files_by_table.get(table, [])
        # FEATURE(import-log-verbosity): verbose-only per-table detail (DEBUG; gated by --verbose).
        logger.debug("importing table %s", table)
        t_started = time.monotonic()
        t_total = 0
        for file, unix_nano in table_files:
            t_total += process_file(file, table, unix_nano)
        total += t_total
        # FEATURE(import-log-verbosity): PER-TABLE counts (this table's files/records), NOT the batch total.
        logger.debug(
            "imported %d records from %d files for table %s in %s",
            t_total, len(table_files), table, format_duration(time.monotonic() - t_started),
        )
        if flusher is not None:
            flusher.flush_imported()  # durable per-table flush BEFORE the marker
            _write_progress_marker(config, table)
        processed.append(table)
    handler.import_completed()
    if flusher is None:
        # §1c.9 STREAMING SINKS (kafka / eventhub / s3 / file): no per-table mid-run flush boundary, so resume is
        # per-RUN only — a retry may re-emit already-delivered records (AT-LEAST-ONCE; downstream must tolerate
        # dupes). The single end-of-run flush just committed every table, so the markers are written now.
        for table in processed:
            _write_progress_marker(config, table)
    return total
