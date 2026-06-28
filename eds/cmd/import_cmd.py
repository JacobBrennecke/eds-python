"""PARITY: cmd/import.go importCmd.Run (359-697) — the import command orchestration.

Resolves + tests the driver (no Start), then one of three entry paths (fresh export-job, resume by --job-id, or
reuse an existing --dir), writes the per-table cutoff timestamps to the "table-export" tracker key, and runs the
ported importer replay loop (importer.run_import → eds.importer.run). Exit codes: 0 success/validate/declined,
1 errors, 3 bad driver url / failed connection test (exitCodeIncorrectUsage). Invoked standalone (`eds import`)
and forked by the control plane's runImport (always with --no-confirm).
"""

from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
import time
from datetime import datetime, timezone

from eds.cmd.exit_codes import EXIT_ERROR, EXIT_INCORRECT_USAGE, EXIT_SUCCESS
from eds.cmd.import_client import (
    TRACKER_TABLE_EXPORT_KEY,
    TableExportInfo,
    bulk_download_data,
    create_export_job,
    is_cancelled,
    load_table_export_info,
    marshal_table_export_info,
    poll_until_complete,
    table_names,
)
from eds.driver import (
    DriverMigration,
    ImporterConfig,
    ImporterHelp,
    get_driver_metadata_for_url,
    new_driver_for_import,
    new_importer,
)
from eds.drivers import register_all
from eds.registry import new_api_registry
from eds.tracker import new_tracker
from eds.util.api import get_api_url_from_jwt
from eds.util.crdb import parse_crdb_export_file
from eds.util.file import list_dir
from eds.util.logger import Logger
from eds.util.shutdown import ShutdownSignal


def load_schema_validator(args: argparse.Namespace, logger: Logger):
    # DEVIATION (schema-validator-dir-deferred): the --schema-validator directory loader is not yet ported.
    if getattr(args, "schema_validator", ""):
        logger.warn("schema-validator directory loading is not yet ported; running without validation")
    return None


def _confirm(target: str) -> bool:
    """DEVIATION: Go uses a huh TUI; here a plain stdin prompt (the server fork always passes --no-confirm)."""
    print("\n🚨 WARNING 🚨")
    try:
        answer = input(f"YOU ARE ABOUT TO DELETE EVERYTHING IN {target}. Type 'yes' to confirm: ")
    except EOFError:
        return False
    return answer.strip().lower() in ("yes", "y")


def run_import_command(args: argparse.Namespace) -> int:
    from eds.cmd import root as _root

    logger = _root.new_logger(args).with_prefix("[import]")
    register_all()

    driver_url = args.url
    api_key = args.api_key
    if not driver_url:
        print('error: required flag "url" not set', file=sys.stderr)
        return EXIT_INCORRECT_USAGE
    if not api_key:
        print('error: required flag "api-key" not set', file=sys.stderr)
        return EXIT_INCORRECT_USAGE

    time_offset_ms: int | None = None
    if args.time_offset:
        try:
            dt = datetime.fromisoformat(args.time_offset.replace("Z", "+00:00"))
            time_offset_ms = int(dt.timestamp() * 1000)
        except ValueError as e:
            logger.error("error parsing time offset: %s", e)
            return EXIT_ERROR

    data_dir = _root.get_data_dir(args, logger)
    if args.dry_run:
        logger.info("🚨 Dry run enabled")
    started = time.monotonic()
    cancel = ShutdownSignal()

    if args.api_url is None:  # PARITY: derive the api url from the JWT when --api-url is not changed
        try:
            api_url = get_api_url_from_jwt(api_key)
        except ValueError as e:
            logger.error("invalid API key. %s", e)
            return EXIT_ERROR
    else:
        api_url = args.api_url
        logger.info("using alternative API url: %s", api_url)

    tracker = new_tracker(data_dir, logger)
    try:
        validator = load_schema_validator(args, logger)
        try:
            registry = new_api_registry(logger, api_url, _root.VERSION, tracker)
        except Exception as e:  # noqa: BLE001 — PARITY: registry build failure is Fatal (exit 1)
            logger.error("error creating registry: %s", e)
            return EXIT_ERROR
        try:
            return _do_import(
                args, logger, tracker, registry, data_dir, cancel, api_url, api_key, driver_url,
                time_offset_ms, validator, started, _root.VERSION,
            )
        finally:
            registry.close()
    finally:
        tracker.close()


def _do_import(  # noqa: C901 — faithful to importCmd.Run's single body
    args, logger, tracker, registry, data_dir, cancel, api_url, api_key, driver_url,
    time_offset_ms, validator, started, version,
) -> int:
    try:
        driver = new_driver_for_import(None, logger, driver_url, registry, tracker, data_dir)
    except Exception as e:  # noqa: BLE001 — bad/unsupported driver url → exit 3
        print(str(e), file=sys.stderr)
        return EXIT_INCORRECT_USAGE
    try:
        driver.test(None, logger, driver_url)  # DEVIATION: Go enforces a 15s timeout; not enforced here
    except Exception as e:  # noqa: BLE001 — failed connection test → exit 3 (the control plane "invalid url")
        print(str(e), file=sys.stderr)
        return EXIT_INCORRECT_USAGE
    logger.debug("driver test successful")

    if args.validate_only:  # the control-plane "configure" validate path
        return EXIT_SUCCESS

    importer = new_importer(None, logger, driver_url, registry)
    skip_delete_confirm = isinstance(importer, ImporterHelp) and not importer.supports_delete()

    only = args.only or []
    company_ids = args.company_ids or []
    location_ids = args.location_ids or []

    needs_confirm = not (
        args.dry_run or args.no_confirm or skip_delete_confirm or args.schema_only or args.no_delete
    )
    if needs_confirm:
        meta = get_driver_metadata_for_url(driver_url)
        if not _confirm(meta.name if meta is not None else driver_url):
            return EXIT_SUCCESS

    dir_ = args.dir
    no_cleanup = args.no_cleanup or bool(dir_)  # PARITY: an existing --dir is never deleted
    tables: list[str] = []
    table_export_info: list[TableExportInfo] = []
    job_id = args.job_id or ""
    success = False
    try:
        if not dir_:
            if not args.schema_only:
                if not job_id:
                    logger.info("Requesting Export...")
                    job_id = create_export_job(
                        logger, api_url, api_key, tables=only, company_ids=company_ids,
                        location_ids=location_ids, time_offset_ms=time_offset_ms, version=version,
                    )
                logger.info("Waiting for Export to Complete...")
                job = poll_until_complete(logger, api_url, api_key, job_id, version=version, cancel=cancel)
                if job is None or is_cancelled(cancel):  # cancelled
                    return EXIT_ERROR
                dir_ = tempfile.mkdtemp(prefix=f"import-{job_id}-", dir=data_dir)
                logger.info("Downloading export data...")
                table_export_info = bulk_download_data(logger, job.tables, dir_)
                if is_cancelled(cancel):
                    return EXIT_ERROR
                tables = table_names(table_export_info)
            else:
                logger.debug("schema only, skipping download")
                schema = registry.get_latest_schema()
                now = datetime.now(timezone.utc)
                table_export_info = [TableExportInfo(table=t, timestamp=now) for t in schema]
                tables = table_names(table_export_info)
        else:
            td = load_table_export_info(tracker)
            if td is not None:
                table_export_info = td
                tables = table_names(td)
                logger.debug("reloading tables (%s) from %s", ",".join(tables), dir_)
            else:
                for f in list_dir(dir_):
                    table, _, ok = parse_crdb_export_file(f)
                    if ok and table not in tables:
                        tables.append(table)

        if only:
            tables = [t for t in tables if t in only]

        logger.info("Importing data to tables %s", ", ".join(tables))
        try:
            importer.run_import(
                ImporterConfig(
                    url=driver_url, logger=logger, schema_registry=registry, max_parallel=args.parallel,
                    job_id=job_id, data_dir=dir_, dry_run=args.dry_run, tables=tables, single=args.single,
                    schema_validator=validator, schema_only=args.schema_only, no_delete=args.no_delete,
                )
            )
        except Exception as e:  # noqa: BLE001 — PARITY: importer error → log + exit 1 (success stays false)
            logger.error("error running import: %s", e)
            return EXIT_ERROR

        if isinstance(driver, DriverMigration):  # PARITY: record the versions just migrated
            latest = registry.get_latest_schema()
            for info in table_export_info:
                if info.table in tables:
                    data = latest.get(info.table)
                    if data is not None:
                        registry.set_table_version(info.table, data.model_version)
                        logger.trace("set table %s version to %s", info.table, data.model_version)
        else:
            logger.trace("driver does not support migration, skip setting table versions")

        success = True
        logger.info("👋 Loaded %d tables in %.1fs", len(tables), time.monotonic() - started)
        return EXIT_SUCCESS
    except (RuntimeError, OSError) as e:  # createExportJob/poll/download/mkdir/getLatestSchema failures → exit 1
        logger.error("import failed: %s", e)
        return EXIT_ERROR
    finally:
        files_removed = False
        if success:
            if not no_cleanup and dir_:
                shutil.rmtree(dir_, ignore_errors=True)
                files_removed = True
            try:
                tracker.set_key(TRACKER_TABLE_EXPORT_KEY, marshal_table_export_info(table_export_info))
            except Exception as e:  # noqa: BLE001
                logger.error("error saving table export data to tracker: %s", e)
        if not files_removed and dir_:
            logger.info("downloaded files saved to: %s", dir_)
