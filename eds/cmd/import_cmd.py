"""PARITY: cmd/import.go importCmd.Run (359-697) — the import command orchestration.

Resolves + tests the driver (no Start), then one of three entry paths (fresh export-job, resume by --job-id, or
reuse an existing --dir), writes the per-table cutoff timestamps to the "table-export" tracker key, and runs the
ported importer replay loop (importer.run_import → eds.importer.run). Exit codes: 0 success/validate/declined,
1 errors, 3 bad driver url / failed connection test (exitCodeIncorrectUsage). Invoked standalone (`eds import`)
and forked by the control plane's runImport (always with --no-confirm).
"""

from __future__ import annotations

import argparse
import errno
import json
import shutil
import sys
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from eds.cmd.config import Config, load_config, set_config_value
from eds.cmd.exit_codes import EXIT_ERROR, EXIT_INCORRECT_USAGE, EXIT_SUCCESS
from eds.cmd.import_client import (
    TRACKER_TABLE_EXPORT_KEY,
    DownloadStageError,
    ExportJobResponse,
    RecoveryCancelled,
    TableExportInfo,
    UsageError,
    bulk_download_data,
    create_export_job,
    failed_tables,
    is_cancelled,
    load_table_export_info,
    marshal_table_export_info,
    parse_rfc3339,
    poll_until_complete,
    table_names,
)
from eds.cmd.session import ApiStatusError
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
from eds.schema import SchemaValidationError
from eds.tracker import new_tracker
from eds.util.api import get_api_url_from_jwt
from eds.util.crdb import parse_crdb_export_file
from eds.util.file import list_dir
from eds.util.hash import hash as eds_hash
from eds.util.logger import Logger
from eds.util.shutdown import ShutdownSignal

# FEATURE(import-recovery): the LOCKED backoff ladder + defaults (§2.3 / §4). See features/import-recovery.md.
RECOVERY_BASE_DELAY_SECONDS = 30
RECOVERY_DEFAULT_MAX_RETRIES = 5
_RETRYABLE_STATUS = frozenset({408, 429, 500, 502, 503, 504})  # §1c.4 transient HTTP (adds 500 to Go's set)
# §1c.4 the canonical shared substring fallback set (identical in both ports). Type checks come FIRST; this only
# rescues network errors that arrive as a plain message (e.g. requests' ConnectionError, an OSError ⊄ builtin
# ConnectionError). It deliberately does NOT contain local-IO phrasing, so local OSErrors stay fatal.
_TRANSPORT_MARKERS = (
    "connection reset", "connection refused", "broken pipe", "timed out", "eof",
    "no such host", "tls handshake", "dns",
)
# §1c.4 local (non-network) OSError errnos that are FATAL — disk full, permissions, read-only fs, quota.
_FATAL_ERRNOS = frozenset(
    e for e in (
        getattr(errno, "ENOSPC", None), getattr(errno, "EACCES", None), getattr(errno, "EPERM", None),
        getattr(errno, "EROFS", None), getattr(errno, "EDQUOT", None), getattr(errno, "EISDIR", None),
        getattr(errno, "ENOENT", None), getattr(errno, "ENOTDIR", None), getattr(errno, "EMFILE", None),
        getattr(errno, "ENFILE", None),
    )
    if e is not None
)


def _matches_transport(msg: str) -> bool:
    m = msg.lower()
    return any(s in m for s in _TRANSPORT_MARKERS)


def is_recoverable(err: BaseException) -> bool:
    """FEATURE(import-recovery): the §1c.4 retryable-vs-fatal predicate (one pure function). Classify by EXCEPTION
    TYPE first, then the shared substring fallback.

    Retryable: transport/timeout/socket (builtin ConnectionError/TimeoutError), HTTP 408/429/500/502/503/504,
    download HTTP/network IO (DownloadStageError). FATAL (no retry): malformed URL (ValueError → exit 3),
    PermissionError, disk-full/other local OSErrors, 401/403/400/404/422, schema/config, cancellation, unknown."""
    if isinstance(err, (UsageError, RecoveryCancelled, SchemaValidationError, ValueError, PermissionError)):
        return False  # cancellation / schema / malformed-url (ValueError) / permission → FATAL
    if isinstance(err, DownloadStageError):
        return True  # download HTTP/network IO is retryable (the recall mode depends on .expired)
    if isinstance(err, ApiStatusError):
        return err.status_code in _RETRYABLE_STATUS
    if isinstance(err, (ConnectionError, TimeoutError)):
        return True  # builtin network types (reset/refused/aborted/broken-pipe, socket timeout)
    if isinstance(err, OSError):
        # A genuinely LOCAL OSError (disk-full / ENOENT / ...) is fatal; requests' network errors subclass OSError
        # (not builtin ConnectionError), so route them by the canonical transport substrings.
        if err.errno in _FATAL_ERRNOS:
            return False
        return _matches_transport(str(err))
    return _matches_transport(str(err))


def backoff_ladder(max_retries: int) -> list[int]:
    """FEATURE(import-recovery): the LOCKED ladder [30,60,120,240,480] == 30*2^n (§2.3); truncates from the
    front (N=2 → [30,60]); N<=0 → [] (recovery disabled)."""
    return [RECOVERY_BASE_DELAY_SECONDS * (2**n) for n in range(max(0, max_retries))]


def resolve_max_retries(explicit: int | None, config: Config, data_dir: str) -> int:
    """FEATURE(import-recovery): precedence --max-retries > config.toml import_max_retries > built-in 5, and
    persist via set_config_value — exactly mirroring resolve_ingest_mode (§4)."""
    if explicit is not None:
        retries = max(0, explicit)
        set_config_value(data_dir, "import_max_retries", retries)
        return retries
    if config.has("import_max_retries"):
        return max(0, config.get_int("import_max_retries", RECOVERY_DEFAULT_MAX_RETRIES))
    set_config_value(data_dir, "import_max_retries", RECOVERY_DEFAULT_MAX_RETRIES)
    return RECOVERY_DEFAULT_MAX_RETRIES


def compute_run_id(
    driver_url: str, only: list[str], company_ids: list[str], location_ids: list[str], time_offset_ms: int | None
) -> str:
    """FEATURE(import-recovery): a STABLE run identity for the progress markers — NOT the volatile export jobId
    (§1b OQ-6). The §1c.2 canonical formula (IDENTICAL in both ports, pinned by a golden vector):
    eds_hash("|".join([driver_url, sorted(only), sorted(companyIds), sorted(locationIds), str(timeOffset)])) —
    driver_url FIRST, every list SORTED + comma-joined, "|" field separator. driver_url first means re-pointing
    --url yields a NEW run id (so stale done-markers from another DB are never reused → no silent skip)."""
    return eds_hash(
        "|".join([
            driver_url, ",".join(sorted(only)), ",".join(sorted(company_ids)),
            ",".join(sorted(location_ids)), str(time_offset_ms),
        ])
    )


def _progress_marker_present(tracker: Any, run_id: str, table: str) -> bool:
    found, _ = tracker.get_key(f"import-progress:{run_id}:{table}")
    return found


def _download_urls_expired(err: BaseException) -> bool:
    """FEATURE(import-recovery): §1c.1 — a presigned-URL download error means the URLs EXPIRED (so the recall must
    POST a new job) only on HTTP 403/410; any other download failure is transient (recall GET-re-downloads the
    same job). download_file surfaces the status in its message ("error fetching data: 403 ...")."""
    status = getattr(err, "status_code", None)
    if status in (403, 410):
        return True
    msg = str(err).lower()
    return "403" in msg or "410" in msg


@dataclass
class ImportPlan:
    """FEATURE(import-recovery): the run inputs captured up front and threaded into EVERY retry's POST (§1b)."""

    company_ids: list[str] = field(default_factory=list)
    location_ids: list[str] = field(default_factory=list)
    time_offset_ms: int | None = None
    run_id: str = ""


@dataclass
class RecoveryResult:
    """FEATURE(import-recovery): the outcome of run_with_recovery."""

    imported: list[str] = field(default_factory=list)
    permanently_failed: list[str] = field(default_factory=list)
    table_export_info: list[TableExportInfo] = field(default_factory=list)
    job_id: str = ""
    last_error: str = ""


def run_with_recovery(  # noqa: C901 — the single recovery state machine (export→poll→download→import + ladder)
    logger: Logger, *, plan: ImportPlan, max_retries: int,
    initial_export_tables: list[str], initial_job_id: str,
    export_fn: Callable[[list[str], ImportPlan], str],
    poll_fn: Callable[[str], ExportJobResponse],
    download_fn: Callable[[ExportJobResponse, list[str], str], tuple[list[TableExportInfo], str]],
    import_fn: Callable[[str, list[str], str], None],
    tracker: Any = None, completed_tables: set[str] | None = None, cancel: Any = None,
    sleep: Callable[[float], None] | None = None, now: Any = None,
) -> RecoveryResult:
    """FEATURE(import-recovery): wrap the per-table-set export→poll→download→import unit (§2.1) with the LOCKED
    ladder (§2.3) + the recall (§1c.1). On a recoverable failure: detect the failed set, back off, and re-call —
    export-stage Failed ⇒ POST a NEW job for the failed subset; a transient download / load-stage failure ⇒ GET
    re-download the SAME job; a download URL-EXPIRY (403/410) ⇒ POST a new job. Exactly 5 retries (6 attempts).
    FATAL errors short-circuit (re-raised). §1c.3: when the concrete set was never learned (export/poll failed) on
    a FULL import, the scope is the FULL set — it MUST NOT collapse to "0 tables → success"; exhaustion there is a
    hard failure. Stages are injected so the loop is unit-testable with zero wall time via the sleep seam (§6)."""
    sleep = sleep or time.sleep
    ladder = backoff_ladder(max_retries)
    imported: list[str] = list(completed_tables or [])
    info_all: list[TableExportInfo] = []
    export_tables = list(initial_export_tables)
    job_id = initial_job_id
    need_post = job_id == ""
    # `working` is the concrete remaining set; None means "not yet learned" (export/poll never succeeded). For a
    # FULL import (initial_export_tables == []) None must keep retrying the whole export, NOT collapse to [] (§1c.3).
    working: list[str] | None = None
    attempt = 0
    last_error = ""
    exhausted = False

    while True:
        attempt += 1
        try:
            if need_post:
                job_id = export_fn(export_tables, plan)
                need_post = False
            job = poll_fn(job_id)
            present = [t for t in job.tables.keys() if t not in imported]
            failed = [t for t in failed_tables(job) if t in present]
            ready = [t for t in present if t not in failed]
            working = list(present)  # learned the concrete remaining set (used as the scope if download/import fails)
            if ready:
                info, attempt_dir = download_fn(job, ready, job_id)
                import_fn(job_id, ready, attempt_dir)
                for t in ready:
                    if t not in imported:
                        imported.append(t)
                info_all.extend(info)
                working = [t for t in working if t not in ready]  # drop the just-imported tables
            if not working:
                break  # whole (remaining) set done
            last_error = f"error exporting tables: {', '.join(working)}"
            export_tables = list(working)
            need_post = True  # §1c.1: export-stage Failed ⇒ POST a NEW job for the failed subset
        except Exception as e:  # noqa: BLE001 — classify, then either short-circuit (fatal) or back off
            if not is_recoverable(e):
                raise  # FATAL: re-raise unchanged → caller maps the native exit code (§2.6)
            last_error = str(e)
            if working is not None:
                scope: list[str] | None = list(working)
            elif initial_export_tables:  # filtered/resume run → the concrete requested scope is known
                scope = list(initial_export_tables)
            else:  # §1c.3: FULL import, concrete set never learned ⇒ keep retrying the whole export (NEVER [])
                scope = None
            if scope is not None and tracker is not None and plan.run_id:  # §2.4: drop tables that durably flushed
                flushed = [t for t in scope if _progress_marker_present(tracker, plan.run_id, t)]
                for t in flushed:
                    if t not in imported:
                        imported.append(t)
                scope = [t for t in scope if t not in flushed]
            working = scope
            export_tables = list(working) if working is not None else list(initial_export_tables)
            # §1c.1 recall mode: if the POST itself failed (no live job) re-POST; an EXPIRED download (403/410) ⇒
            # POST a new job; a transient download / load-stage failure ⇒ GET re-download the SAME job.
            if not need_post:
                need_post = isinstance(e, DownloadStageError) and e.expired

        if working is not None and not working:
            break  # learned the set and nothing remains → done
        if attempt > max_retries:
            exhausted = True
            break  # the remaining `working` set is permanently failed
        if is_cancelled(cancel):
            raise RecoveryCancelled()
        logger.error("error running import: %s", last_error)  # §2.2 step 1 — log as today
        logger.info(
            "recovering: retrying tables %s in %ds (retry %d/%d)",
            ", ".join(working or export_tables) or "(all)", ladder[attempt - 1], attempt, max_retries,
        )
        sleep(ladder[attempt - 1])
        if is_cancelled(cancel):  # tripped during the backoff → abort, do NOT consume another retry (§6.8)
            raise RecoveryCancelled()

    # §1c.3: a recoverable failure must NEVER collapse to 0-tables-success. If we exhausted retries having never
    # learned the concrete set (FULL import), the WHOLE request permanently failed (report "*" — all tables).
    if exhausted:
        permanently_failed = list(working) if working is not None else (list(initial_export_tables) or ["*"])
    else:
        permanently_failed = []
    if permanently_failed:
        logger.error(
            "giving up on tables %s after %d retries: %s",
            ", ".join(permanently_failed), max_retries, last_error,
        )
    return RecoveryResult(
        imported=imported, permanently_failed=permanently_failed, table_export_info=info_all,
        job_id=job_id, last_error=last_error,
    )


def load_schema_validator(args: argparse.Namespace, logger: Logger):
    # PARITY: loadSchemaValidator (root.go:245) — None when --schema-validator is empty, else the loaded validator.
    schema_dir = getattr(args, "schema_validator", "")
    if not schema_dir:
        return None
    from eds.util.schema import new_schema_validator

    return new_schema_validator(schema_dir)


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
            # PARITY: Go time.Parse(RFC3339) — accepts arbitrary fractional precision (parse_rfc3339) AND requires
            # a timezone (reject a naive datetime, which fromisoformat would otherwise accept as local time).
            dt = parse_rfc3339(args.time_offset)
            if dt.tzinfo is None:
                raise ValueError(f"missing timezone in {args.time_offset!r}")
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

    try:
        tracker = new_tracker(data_dir, logger)
    except Exception as e:  # noqa: BLE001 — PARITY: tracker create failure is Fatal (exit 1)
        logger.error("error creating tracker: %s", e)
        return EXIT_ERROR
    try:
        try:
            validator = load_schema_validator(args, logger)
        except Exception as e:  # noqa: BLE001 — PARITY: logger.Fatal("error loading validator") (exit 1)
            logger.error("error loading validator: %s", e)
            return EXIT_ERROR
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

    try:
        importer = new_importer(None, logger, driver_url, registry)
    except Exception as e:  # noqa: BLE001 — PARITY: NewImporter failure is Fatal (exit 1)
        logger.error("error creating importer: %s", e)
        return EXIT_ERROR
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

    # FEATURE(import-recovery): the fresh-export full-import path runs through the recovery state machine when
    # enabled (--max-retries>0). --max-retries 0 / --dir / --schema-only keep the exact-Go single-attempt path
    # below byte-for-byte unchanged. See migration/features/import-recovery.md.
    max_retries = resolve_max_retries(getattr(args, "max_retries", None), load_config(data_dir), data_dir)
    if max_retries > 0 and not dir_ and not args.schema_only:
        return _do_import_with_recovery(
            args, logger, tracker, registry, data_dir, cancel, api_url, api_key, driver_url,
            time_offset_ms, validator, importer, driver, only, company_ids, location_ids, max_retries, version,
        )

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
    except Exception as e:  # noqa: BLE001 — PARITY: all run-body failures are Fatal/Error → exit 1 (not panic)
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


def _do_import_with_recovery(  # noqa: C901 — the recovery wiring (build stages + run_with_recovery + summary)
    args, logger, tracker, registry, data_dir, cancel, api_url, api_key, driver_url,
    time_offset_ms, validator, importer, driver, only, company_ids, location_ids, max_retries, version,
) -> int:
    """FEATURE(import-recovery): the fresh-export full-import path with recovery (§1b/§2). Builds the real
    export/poll/download/import stages, threads the captured plan (companyIds/timeOffset) + a stable run id into
    run_with_recovery, then records migration versions, persists the table-export cutoffs, cleans up, and maps
    the outcome to an exit code (0 all-imported; 1 if any table permanently failed — never 3)."""
    try:
        latest = registry.get_latest_schema()
    except Exception as e:  # noqa: BLE001 — PARITY: schema fetch failure → exit 1
        logger.error("import failed: %s", e)
        return EXIT_ERROR

    run_id = compute_run_id(driver_url, only, company_ids, location_ids, time_offset_ms)
    plan = ImportPlan(
        company_ids=company_ids, location_ids=location_ids, time_offset_ms=time_offset_ms, run_id=run_id
    )
    # §2.4/§1b OQ-6: read prior progress markers (cross-restart resume) — skip the already-completed tables.
    candidate = list(only) if only else list(latest.keys())
    completed = {t for t in candidate if _progress_marker_present(tracker, run_id, t)}
    if completed:
        logger.info("resuming import; %d tables already completed: %s", len(completed), ", ".join(sorted(completed)))
    # A fresh run POSTs the original `only` (empty ⇒ all, via omitempty); a resume POSTs only the incomplete set.
    initial_export_tables = [t for t in candidate if t not in completed] if completed else list(only)

    if completed and not initial_export_tables:  # every candidate table already completed on a prior run
        logger.info("👋 all %d tables already imported; nothing to resume", len(completed))
        tracker.delete_keys_with_prefix(f"import-progress:{run_id}:")
        tracker.delete_key(f"import-failed:{run_id}")
        return EXIT_SUCCESS

    temp_dirs: list[str] = []

    def export_fn(tables: list[str], p: ImportPlan) -> str:
        logger.info("Requesting Export...")
        return create_export_job(
            logger, api_url, api_key, tables=tables, company_ids=p.company_ids,
            location_ids=p.location_ids, time_offset_ms=p.time_offset_ms, version=version,
        )

    def poll_fn(job_id: str) -> ExportJobResponse:
        logger.info("Waiting for Export to Complete...")
        job = poll_until_complete(
            logger, api_url, api_key, job_id, version=version, cancel=cancel, raise_on_failed=False
        )
        if job is None or is_cancelled(cancel):
            raise RecoveryCancelled()
        return job

    def download_fn(
        job: ExportJobResponse, tables: list[str], job_id: str
    ) -> tuple[list[TableExportInfo], str]:
        attempt_dir = tempfile.mkdtemp(prefix=f"import-{job_id}-", dir=data_dir)
        temp_dirs.append(attempt_dir)
        logger.info("Downloading export data...")
        subset = {t: td for t, td in job.tables.items() if t in tables}
        try:
            info = bulk_download_data(logger, subset, attempt_dir)
        except Exception as e:  # noqa: BLE001 — tag the download stage; §1c.1: 403/410 ⇒ URLs expired ⇒ re-POST.
            raise DownloadStageError(str(e), expired=_download_urls_expired(e)) from e
        if is_cancelled(cancel):
            raise RecoveryCancelled()
        return info, attempt_dir

    def import_fn(job_id: str, tables: list[str], attempt_dir: str) -> None:
        logger.info("Importing data to tables %s", ", ".join(tables))
        importer.run_import(
            ImporterConfig(
                url=driver_url, logger=logger, schema_registry=registry, max_parallel=args.parallel,
                job_id=job_id, data_dir=attempt_dir, dry_run=args.dry_run, tables=list(tables),
                single=args.single, schema_validator=validator, schema_only=False,
                # §1c.5: re-truncate (create_datasource) is gated on NOT --no-delete — threading no_delete here
                # means a --no-delete (or audit/append) retry RE-APPENDS the in-flight table instead of dropping
                # the audit trail (PK-safe at-least-once for the single in-flight table; force-truncate REJECTED).
                no_delete=args.no_delete,
                recovery_enabled=True, run_id=run_id, tracker=tracker,  # FEATURE(import-recovery): markers + flush
            )
        )

    try:
        result = run_with_recovery(
            logger, plan=plan, max_retries=max_retries, initial_export_tables=initial_export_tables,
            initial_job_id=args.job_id or "", export_fn=export_fn, poll_fn=poll_fn, download_fn=download_fn,
            import_fn=import_fn, tracker=tracker, completed_tables=completed, cancel=cancel,
        )
    except UsageError as e:  # FATAL usage → exit 3 (never converted to a per-table give-up; §2.6)
        print(str(e), file=sys.stderr)
        return e.exit_code
    except RecoveryCancelled:
        return EXIT_ERROR
    except Exception as e:  # noqa: BLE001 — any other FATAL short-circuit → log + exit 1
        logger.error("error running import: %s", e)
        return EXIT_ERROR

    # PARITY: record the versions just migrated (only for tables that actually imported).
    if isinstance(driver, DriverMigration):
        for info in result.table_export_info:
            if info.table in result.imported:
                data = latest.get(info.table)
                if data is not None:
                    registry.set_table_version(info.table, data.model_version)
                    logger.trace("set table %s version to %s", info.table, data.model_version)

    try:  # persist the per-table export cutoffs (consumed by the consumer fork) — PARITY with the legacy finally
        tracker.set_key(TRACKER_TABLE_EXPORT_KEY, marshal_table_export_info(result.table_export_info))
    except Exception as e:  # noqa: BLE001
        logger.error("error saving table export data to tracker: %s", e)

    if not args.no_cleanup:
        for d in temp_dirs:
            shutil.rmtree(d, ignore_errors=True)
    else:
        for d in temp_dirs:
            logger.info("downloaded files saved to: %s", d)

    if result.permanently_failed:
        # §1b OQ-4 (SOFT): record the failed set persistently for a later retry, then exit 1 — do NOT block the
        # rest of the run or the downstream consumer (the server fork treats a non-usage exit-1 as start-enough).
        try:
            tracker.set_key(f"import-failed:{run_id}", json.dumps(sorted(result.permanently_failed)))
        except Exception as e:  # noqa: BLE001
            logger.error("error recording failed tables: %s", e)
        logger.error(
            "imported %d tables; %d tables permanently failed: %s",
            len(result.imported), len(result.permanently_failed), ", ".join(sorted(result.permanently_failed)),
        )
        return EXIT_ERROR

    # whole-run success → reset the progress markers + any prior failed record (§2.4).
    tracker.delete_keys_with_prefix(f"import-progress:{run_id}:")
    tracker.delete_key(f"import-failed:{run_id}")
    logger.info("👋 Loaded %d tables", len(result.imported))
    return EXIT_SUCCESS
