"""FEATURE(import-recovery): the recovery contract tests (the cross-port oracle is
migration/features/import-recovery.md §6, NOT the Go source). Everything runs through the injectable sleep seam
so there is ZERO wall time. Covers: the locked backoff ladder, the recall payload shapes (POST subset vs GET
reuse), recovery-then-success, soft-exhaustion-continue + the consumer-proceeds acceptance criterion, fatal
short-circuit (usage-3 / 401), --max-retries 0 exact-Go, cancellation mid-backoff, cross-restart marker resume,
and the §2.5 classification matrix.
"""

from __future__ import annotations

import gzip
import shutil
import subprocess

import pytest

from eds.cmd.exit_codes import EXIT_ERROR, EXIT_INCORRECT_USAGE, EXIT_SUCCESS
from eds.cmd.import_client import (
    DownloadStageError,
    ExportJobResponse,
    ExportJobTableData,
    RecoveryCancelled,
    UsageError,
    failed_tables,
)
from eds.cmd.import_cmd import (
    ImportPlan,
    backoff_ladder,
    compute_run_id,
    is_recoverable,
    resolve_max_retries,
    run_with_recovery,
)
from eds.cmd.session import ApiStatusError
from eds.schema import SchemaValidationError


class _QuietLogger:
    def trace(self, m, *a): ...
    def debug(self, m, *a): ...
    def info(self, m, *a): ...
    def warn(self, m, *a): ...
    def error(self, m, *a): ...
    def fatal(self, m, *a): ...
    def with_prefix(self, p): return self
    def with_fields(self, f): return self


def _job(statuses: dict[str, str]) -> ExportJobResponse:
    # FEATURE(import-recovery): a completed export-job status map ({table: "Completed"/"Failed"}).
    return ExportJobResponse(
        completed=True,
        tables={t: ExportJobTableData(status=s, urls=[], cursor="0") for t, s in statuses.items()},
    )


# ---- §6.1 the LOCKED backoff ladder ---------------------------------------------------------------


def test_backoff_ladder_is_locked() -> None:
    # FEATURE(import-recovery): pin the magic numbers both ways (explicit list AND == 30*2^n).
    assert backoff_ladder(5) == [30, 60, 120, 240, 480]
    assert backoff_ladder(5) == [30 * 2**n for n in range(5)]
    assert backoff_ladder(2) == [30, 60]  # truncates from the front
    assert backoff_ladder(0) == []


def test_ladder_applied_on_repeated_failure() -> None:
    # FEATURE(import-recovery): a set that fails export every attempt → exactly 5 sleeps (6 attempts), then give up.
    sleeps: list[float] = []
    posts: list[list[str]] = []

    def export_fn(tables, plan):
        posts.append(list(tables))
        return f"job-{len(posts)}"

    def poll_fn(job_id):
        return _job({"a": "Failed", "b": "Failed"})

    res = run_with_recovery(
        _QuietLogger(), plan=ImportPlan(run_id="run1"), max_retries=5,
        initial_export_tables=["a", "b"], initial_job_id="job-0",
        export_fn=export_fn, poll_fn=poll_fn,
        download_fn=lambda *a: (_ for _ in ()).throw(AssertionError("no ready tables")),
        import_fn=lambda *a: (_ for _ in ()).throw(AssertionError("no import")),
        sleep=sleeps.append,
    )
    assert sleeps == [30, 60, 120, 240, 480]  # zero wall time
    assert res.imported == []
    assert res.permanently_failed == ["a", "b"]
    assert len(posts) == 5  # re-POST on each of the 5 retries (export-stage Failed ⇒ POST)


# ---- §6.2 recall payload shapes -------------------------------------------------------------------


def test_recall_export_stage_posts_failed_subset_with_plan() -> None:
    # FEATURE(import-recovery): attempt #1 fails {b,d} of {a,b,c,d}; the recall POSTs {tables:[b,d]} carrying the
    # captured companyIds + timeOffset, minting a NEW jobId (§1b path 2). attempt #2 then succeeds.
    posts: list[tuple[list[str], list[str], int | None]] = []
    polls: list[str] = []
    imps: list[tuple[str, list[str]]] = []
    first = {"v": True}

    def export_fn(tables, plan):
        posts.append((list(tables), list(plan.company_ids), plan.time_offset_ms))
        return "newjob"

    def poll_fn(job_id):
        polls.append(job_id)
        if first["v"]:
            first["v"] = False
            return _job({"a": "Completed", "b": "Failed", "c": "Completed", "d": "Failed"})
        return _job({"b": "Completed", "d": "Completed"})

    def import_fn(job_id, tables, d):
        imps.append((job_id, sorted(tables)))

    res = run_with_recovery(
        _QuietLogger(), plan=ImportPlan(company_ids=["co1"], time_offset_ms=123, run_id="run1"), max_retries=5,
        initial_export_tables=["a", "b", "c", "d"], initial_job_id="job0",
        export_fn=export_fn, poll_fn=poll_fn, download_fn=lambda job, tables, jid: ([], "/tmp/x"),
        import_fn=import_fn, sleep=lambda s: None,
    )
    assert posts == [(["b", "d"], ["co1"], 123)]  # subset + threaded plan; exactly one re-POST
    assert imps == [("job0", ["a", "c"]), ("newjob", ["b", "d"])]  # good first, recall subset second
    assert res.imported == ["a", "c", "b", "d"]
    assert res.permanently_failed == []


def test_recall_load_stage_reuses_job_via_get() -> None:
    # FEATURE(import-recovery): a load-stage (import) transient ⇒ GET re-download the SAME job (no re-POST), §1b path 1.
    posts: list[list[str]] = []
    polls: list[str] = []
    sleeps: list[float] = []
    n = {"v": 0}

    def import_fn(job_id, tables, d):
        n["v"] += 1
        if n["v"] == 1:
            raise RuntimeError("connection reset by peer")  # transient load-stage failure

    res = run_with_recovery(
        _QuietLogger(), plan=ImportPlan(run_id="r"), max_retries=5,
        initial_export_tables=["a"], initial_job_id="job-x",
        export_fn=lambda tables, plan: posts.append(list(tables)) or "job-post",
        poll_fn=lambda job_id: polls.append(job_id) or _job({"a": "Completed"}),
        download_fn=lambda job, tables, jid: ([], "/tmp/x"), import_fn=import_fn, sleep=sleeps.append,
    )
    assert posts == []                 # never re-POSTed
    assert polls == ["job-x", "job-x"]  # GET-reused the same job id
    assert sleeps == [30]              # exactly one 30s backoff
    assert res.imported == ["a"] and res.permanently_failed == []


def test_recall_transient_download_reuses_job_via_get() -> None:
    # FEATURE(import-recovery) §1c.1 path 1: a TRANSIENT download failure (URLs still valid) ⇒ GET re-download the
    # SAME job (no re-POST).
    posts: list[list[str]] = []
    polls: list[str] = []
    n = {"v": 0}

    def download_fn(job, tables, job_id):
        n["v"] += 1
        if n["v"] == 1:
            raise DownloadStageError("error fetching data: 500 Internal Server Error", expired=False)
        return ([], "/t")

    res = run_with_recovery(
        _QuietLogger(), plan=ImportPlan(run_id="r"), max_retries=5,
        initial_export_tables=["a"], initial_job_id="job-x",
        export_fn=lambda t, p: posts.append(list(t)) or "job-post",
        poll_fn=lambda jid: polls.append(jid) or _job({"a": "Completed"}),
        download_fn=download_fn, import_fn=lambda *a: None, sleep=lambda s: None,
    )
    assert posts == []                  # transient download ⇒ NO re-POST
    assert polls == ["job-x", "job-x"]  # GET-reused the same job
    assert res.imported == ["a"] and res.permanently_failed == []


def test_recall_expired_download_posts_new_job() -> None:
    # FEATURE(import-recovery) §1c.1 path 2: an EXPIRED download (HTTP 403/410) ⇒ POST a NEW job (full re-export).
    posts: list[list[str]] = []
    n = {"v": 0}

    def download_fn(job, tables, job_id):
        n["v"] += 1
        if n["v"] == 1:
            raise DownloadStageError("error fetching data: 403 Forbidden", expired=True)
        return ([], "/t")

    res = run_with_recovery(
        _QuietLogger(), plan=ImportPlan(run_id="r"), max_retries=5,
        initial_export_tables=["a"], initial_job_id="job-x",
        export_fn=lambda t, p: posts.append(list(t)) or "job-new",
        poll_fn=lambda jid: _job({"a": "Completed"}),
        download_fn=download_fn, import_fn=lambda *a: None, sleep=lambda s: None,
    )
    assert posts == [["a"]]  # expired URLs ⇒ re-POST a new export of the failed subset
    assert res.imported == ["a"] and res.permanently_failed == []


# ---- §6.3 recovery-then-success -------------------------------------------------------------------


def test_recovery_then_success_one_sleep() -> None:
    sleeps: list[float] = []
    n = {"v": 0}

    def poll_fn(job_id):
        n["v"] += 1
        if n["v"] == 1:
            raise ApiStatusError("import: bad gateway (status code=502)", 502)  # transient
        return _job({"a": "Completed", "b": "Completed"})

    res = run_with_recovery(
        _QuietLogger(), plan=ImportPlan(run_id="r"), max_retries=5,
        initial_export_tables=["a", "b"], initial_job_id="j",
        export_fn=lambda t, p: "j", poll_fn=poll_fn, download_fn=lambda *a: ([], "/t"),
        import_fn=lambda *a: None, sleep=sleeps.append,
    )
    assert sleeps == [30]
    assert sorted(res.imported) == ["a", "b"] and res.permanently_failed == []


# ---- §6.4 classification matrix -------------------------------------------------------------------


@pytest.mark.parametrize(
    ("status", "expected"),
    [(408, True), (429, True), (500, True), (502, True), (503, True), (504, True),
     (400, False), (401, False), (403, False), (404, False), (422, False)],
)
def test_is_recoverable_http_status(status, expected) -> None:
    assert is_recoverable(ApiStatusError("ctx: msg", status)) is expected


@pytest.mark.parametrize(
    "msg",
    [  # §1c.4 canonical substring set (covers requests-style messages that arrive as plain text)
        "connection reset by peer", "connection refused", "read timed out", "broken pipe",
        "unexpected EOF", "no such host", "tls handshake timeout", "dns lookup failed",
    ],
)
def test_is_recoverable_transport_messages(msg) -> None:
    assert is_recoverable(RuntimeError(msg)) is True


def test_is_recoverable_types_and_fatals() -> None:
    import errno as _errno

    # retryable
    assert is_recoverable(ConnectionResetError()) is True
    assert is_recoverable(ConnectionRefusedError()) is True
    assert is_recoverable(TimeoutError()) is True
    assert is_recoverable(DownloadStageError("error fetching data: 500", expired=False)) is True
    assert is_recoverable(DownloadStageError("error fetching data: 403", expired=True)) is True
    # §1c.4 FATAL: malformed URL (ValueError → exit 3), permission, disk-full / other LOCAL OSErrors
    assert is_recoverable(ValueError("unable to parse url: bad")) is False
    assert is_recoverable(PermissionError("Permission denied")) is False
    assert is_recoverable(OSError(_errno.ENOSPC, "No space left on device")) is False
    assert is_recoverable(FileNotFoundError("missing")) is False
    # other fatals
    assert is_recoverable(UsageError("bad flags")) is False
    assert is_recoverable(RecoveryCancelled()) is False
    assert is_recoverable(SchemaValidationError("schema decode")) is False
    assert is_recoverable(RuntimeError("totally unknown internal bug")) is False  # unknown ⇒ fatal


def test_failed_tables_returns_full_set_in_order() -> None:
    job = _job({"a": "Completed", "b": "Failed", "c": "Failed", "d": "Completed"})
    assert failed_tables(job) == ["b", "c"]
    assert failed_tables(_job({"a": "Completed"})) == []


# ---- §6.5 fatal short-circuits --------------------------------------------------------------------


def test_fatal_401_short_circuits_no_sleep() -> None:
    sleeps: list[float] = []

    def poll_fn(job_id):
        raise ApiStatusError("import: unauthorized (requestId=x)", 401)

    with pytest.raises(ApiStatusError):
        run_with_recovery(
            _QuietLogger(), plan=ImportPlan(run_id="r"), max_retries=5,
            initial_export_tables=["a"], initial_job_id="j",
            export_fn=lambda t, p: "j", poll_fn=poll_fn, download_fn=lambda *a: ([], "/t"),
            import_fn=lambda *a: None, sleep=sleeps.append,
        )
    assert sleeps == []  # no retry, no backoff


def test_fatal_usage_short_circuits_no_sleep() -> None:
    sleeps: list[float] = []

    def poll_fn(job_id):
        raise UsageError("driver test failed", exit_code=EXIT_INCORRECT_USAGE)

    with pytest.raises(UsageError) as ei:
        run_with_recovery(
            _QuietLogger(), plan=ImportPlan(run_id="r"), max_retries=5,
            initial_export_tables=["a"], initial_job_id="j",
            export_fn=lambda t, p: "j", poll_fn=poll_fn, download_fn=lambda *a: ([], "/t"),
            import_fn=lambda *a: None, sleep=sleeps.append,
        )
    assert ei.value.exit_code == EXIT_INCORRECT_USAGE
    assert sleeps == []


# ---- §6.6 exhaustion: import the good set, give up the bad set, continue ---------------------------


def test_exhaustion_imports_good_gives_up_bad() -> None:
    sleeps: list[float] = []

    def poll_fn(job_id):
        return _job({"good": "Completed", "bad": "Failed"})

    res = run_with_recovery(
        _QuietLogger(), plan=ImportPlan(run_id="r"), max_retries=5,
        initial_export_tables=["good", "bad"], initial_job_id="j0",
        export_fn=lambda t, p: "j2", poll_fn=poll_fn, download_fn=lambda *a: ([], "/t"),
        import_fn=lambda *a: None, sleep=sleeps.append,
    )
    assert res.imported == ["good"]
    assert res.permanently_failed == ["bad"]
    assert sleeps == [30, 60, 120, 240, 480]


# ---- IR-RECALL-01 (§1c.3): a recoverable failure on a FULL import NEVER collapses to 0-tables-success --------


def test_full_import_attempt1_failure_does_not_collapse_to_success() -> None:
    # FEATURE(import-recovery) §1c.3: a FULL import (no --only ⇒ initial_export_tables == []) whose poll fails
    # recoverably on attempt #1 must RETRY (never collapse the failed scope to [] → "Loaded 0 tables" EXIT_SUCCESS).
    # When it then permanently fails, the result records a NON-EMPTY failed set (so the caller exits 1, not 0).
    sleeps: list[float] = []

    def poll_fn(job_id):
        raise ApiStatusError("import: service unavailable (status code=503)", 503)  # recoverable, every attempt

    res = run_with_recovery(
        _QuietLogger(), plan=ImportPlan(run_id="r"), max_retries=5,
        initial_export_tables=[], initial_job_id="",  # FULL import, no markers
        export_fn=lambda t, p: "job-1", poll_fn=poll_fn, download_fn=lambda *a: ([], "/t"),
        import_fn=lambda *a: None, sleep=sleeps.append,
    )
    assert res.imported == []                       # nothing imported
    assert res.permanently_failed == ["*"]          # NON-EMPTY → caller maps to EXIT_ERROR (NOT silent success)
    assert sleeps == [30, 60, 120, 240, 480]        # it RETRIED the full set across all 6 attempts


def test_full_import_recovers_after_attempt1_failure() -> None:
    # The same FULL-import shape, but attempt #2 succeeds → the concrete set is learned + imported, EXIT_SUCCESS.
    n = {"v": 0}

    def poll_fn(job_id):
        n["v"] += 1
        if n["v"] == 1:
            raise ApiStatusError("import: bad gateway (status code=502)", 502)
        return _job({"a": "Completed", "b": "Completed"})

    res = run_with_recovery(
        _QuietLogger(), plan=ImportPlan(run_id="r"), max_retries=5,
        initial_export_tables=[], initial_job_id="",
        export_fn=lambda t, p: "job-1", poll_fn=poll_fn, download_fn=lambda *a: ([], "/t"),
        import_fn=lambda *a: None, sleep=lambda s: None,
    )
    assert sorted(res.imported) == ["a", "b"]
    assert res.permanently_failed == []


# ---- §6.8 cancellation mid-backoff ----------------------------------------------------------------


class _FakeCancel:
    def __init__(self) -> None:
        self._set = False

    def set(self) -> None:
        self._set = True

    def is_set(self) -> bool:
        return self._set


def test_cancellation_mid_backoff_aborts() -> None:
    cancel = _FakeCancel()
    sleeps: list[float] = []

    def slept(s):
        sleeps.append(s)
        cancel.set()  # trip the cancel DURING the first backoff

    def poll_fn(job_id):
        return _job({"a": "Failed"})

    with pytest.raises(RecoveryCancelled):
        run_with_recovery(
            _QuietLogger(), plan=ImportPlan(run_id="r"), max_retries=5,
            initial_export_tables=["a"], initial_job_id="j",
            export_fn=lambda t, p: "j", poll_fn=poll_fn, download_fn=lambda *a: ([], "/t"),
            import_fn=lambda *a: None, cancel=cancel, sleep=slept,
        )
    assert sleeps == [30]  # only one backoff consumed, then aborted (remaining retries NOT consumed)


# ---- §6.7 disabled escape hatch + flag/config precedence ------------------------------------------


def test_resolve_max_retries_precedence(tmp_path) -> None:
    from eds.cmd.config import Config, load_config

    # explicit flag wins + persists
    assert resolve_max_retries(3, Config(), str(tmp_path)) == 3
    assert load_config(str(tmp_path)).get_int("import_max_retries") == 3
    # explicit 0 (exact-Go escape hatch) wins + persists
    assert resolve_max_retries(0, load_config(str(tmp_path)), str(tmp_path)) == 0
    assert load_config(str(tmp_path)).get_int("import_max_retries") == 0
    # no flag → config value used (no rewrite needed)
    assert resolve_max_retries(None, load_config(str(tmp_path)), str(tmp_path)) == 0


def test_resolve_max_retries_default_when_absent(tmp_path) -> None:
    from eds.cmd.config import Config, load_config

    assert resolve_max_retries(None, Config(), str(tmp_path)) == 5
    assert load_config(str(tmp_path)).get_int("import_max_retries") == 5  # written back, self-documenting


def test_resolve_max_retries_preserves_other_keys(tmp_path) -> None:
    from eds.cmd.config import load_config, set_config_value

    set_config_value(str(tmp_path), "token", "tok")
    set_config_value(str(tmp_path), "url", "postgres://x")
    resolve_max_retries(7, load_config(str(tmp_path)), str(tmp_path))
    c = load_config(str(tmp_path))
    assert c.get_string("token") == "tok" and c.get_string("url") == "postgres://x"
    assert c.get_int("import_max_retries") == 7


def test_max_retries_flag_default_is_none_sentinel() -> None:
    from eds.cmd.root import build_parser

    assert build_parser().parse_args(["import", "--url", "x", "--api-key", "k"]).max_retries is None
    assert build_parser().parse_args(["import", "--url", "x", "--api-key", "k", "--max-retries", "0"]).max_retries == 0


# ---- cross-restart resume via markers (§1b OQ-6 / §2.4) -------------------------------------------


def test_cross_restart_resume_skips_marked_tables() -> None:
    # FEATURE(import-recovery): a table whose progress marker is already present is treated as done — never
    # re-exported, re-downloaded, or re-imported; the loop resumes only the not-yet-completed tables.
    posts: list[list[str]] = []
    imps: list[list[str]] = []

    def poll_fn(job_id):
        return _job({"b": "Completed"})  # only the incomplete table comes back

    res = run_with_recovery(
        _QuietLogger(), plan=ImportPlan(run_id="r"), max_retries=5,
        initial_export_tables=["b"], initial_job_id="resume-job",
        export_fn=lambda t, p: posts.append(list(t)) or "x",
        poll_fn=poll_fn, download_fn=lambda *a: ([], "/t"),
        import_fn=lambda jid, tables, d: imps.append(sorted(tables)),
        completed_tables={"a"},  # 'a' already completed on a prior run
    )
    assert posts == []             # reused resume-job (no re-export)
    assert imps == [["b"]]         # only 'b' re-imported
    assert "a" in res.imported and "b" in res.imported
    assert res.permanently_failed == []


def test_marker_based_detection_drops_flushed_tables_on_load_failure() -> None:
    # FEATURE(import-recovery): when a load-stage failure occurs but a table already wrote its marker (durably
    # flushed), the retry drops it from the working set (it must not be re-truncated/re-imported).
    import tempfile

    from eds.tracker import new_tracker

    d = tempfile.mkdtemp()
    try:
        tracker = new_tracker(d)
        tracker.set_key("import-progress:r:a", "1")  # 'a' durably flushed before the crash
        n = {"v": 0}
        imps: list[list[str]] = []

        def import_fn(job_id, tables, dd):
            n["v"] += 1
            imps.append(sorted(tables))
            if n["v"] == 1:
                raise RuntimeError("connection reset")  # whole {a,b} attempt fails after 'a' flushed

        res = run_with_recovery(
            _QuietLogger(), plan=ImportPlan(run_id="r"), max_retries=5,
            initial_export_tables=["a", "b"], initial_job_id="j",
            export_fn=lambda t, p: "j", poll_fn=lambda jid: _job({"a": "Completed", "b": "Completed"}),
            download_fn=lambda *a: ([], "/t"), import_fn=import_fn, tracker=tracker, sleep=lambda s: None,
        )
        assert imps[0] == ["a", "b"]   # first attempt tried both
        assert imps[1] == ["b"]        # retry dropped the already-flushed 'a'
        assert "a" in res.imported and "b" in res.imported
        tracker.close()
    finally:
        shutil.rmtree(d, ignore_errors=True)


def test_compute_run_id_is_stable_and_distinct() -> None:
    a = compute_run_id("postgres://x", ["t1"], ["c1"], [], 100)
    assert a == compute_run_id("postgres://x", ["t1"], ["c1"], [], 100)  # stable across restarts
    assert a != compute_run_id("postgres://y", ["t1"], ["c1"], [], 100)  # distinct per driver url
    assert a != compute_run_id("postgres://x", ["t2"], ["c1"], [], 100)  # distinct per table filter


def test_compute_run_id_golden_vector_and_formula() -> None:
    # §1c.2 pin: driver_url FIRST, every list SORTED + comma-joined, "|" separator, str(timeOffset). Lists must be
    # order-insensitive (sorted) and the formula must equal the exact string hashed (cross-port byte-identity).
    from eds.util.hash import hash as eds_hash

    expected = eds_hash("postgres://db|t1,t2|c1,c2|l1|1700000000000")
    assert compute_run_id("postgres://db", ["t2", "t1"], ["c2", "c1"], ["l1"], 1700000000000) == expected
    # input list order does not matter (sorted)
    assert compute_run_id("postgres://db", ["t1", "t2"], ["c1", "c2"], ["l1"], 1700000000000) == expected
    # None timeOffset renders as "None"
    assert compute_run_id("postgres://db", [], [], [], None) == eds_hash("postgres://db||||None")


# ---- CRITICAL ACCEPTANCE: a partial soft-exhaustion (exit 1) still starts the consumer ------------


def test_partial_soft_exhaustion_starts_consumer() -> None:
    # FEATURE(import-recovery): the user's hard "if and only if" — after a partial-import soft-exhaustion the
    # import process exits 1 (non-usage); the control-plane import handler MUST still start/signal the consumer
    # (faithful to Go's "non-usage failure ⇒ Success=true ⇒ consumer starts"). Do NOT regress this.
    import threading

    from eds.cmd.notification_wiring import ControlPlaneContext, build_notification_handler
    from eds.notification.dtos import ImportRequest
    from eds.util.process import ForkResult

    calls: list[list[str]] = []

    def soft_exhaustion_forker(args):  # the forked `eds import` soft-exhausted: imported some, some perm-failed
        calls.append(args.args)
        return ForkResult(
            exit_code=EXIT_ERROR,  # exit 1 (non-usage) — the soft-exhaustion exit
            last_error_lines="imported 3 tables; 1 tables permanently failed: orders",
        )

    ctx = ControlPlaneContext(
        logger=_QuietLogger(), port=0, api_url="https://api", api_key="k", version="1.0",
        keep_logs=False, session_id="sess",
    )
    ctx.forker = soft_exhaustion_forker
    ctx.driver_url = "postgres://x"
    ctx.configured = False  # first import after configure → import_action signals the configure channel
    ctx.configure_event = threading.Event()

    resp = build_notification_handler(ctx).import_action(ImportRequest(backfill=True))

    assert resp.success is True                # exit 1 mapped to start-enough (NOT a hard failure)
    assert ctx.configure_event.is_set()        # the consumer is released to start + process real-time data
    assert calls, "the import was actually forked"


def test_partial_soft_exhaustion_restarts_consumer_when_configured() -> None:
    # The already-configured server restarts (not signals) the live consumer fork after a soft-exhaustion exit 1.
    from eds.cmd.loopback import LoopbackServer
    from eds.cmd.notification_wiring import ControlPlaneContext, build_notification_handler
    from eds.notification.dtos import ImportRequest
    from eds.util.process import ForkResult

    hits: list[str] = []
    srv = LoopbackServer(0, {"/control/restart": lambda: (hits.append("restart"), (200, ""))[1]})
    srv.start()
    try:
        ctx = ControlPlaneContext(
            logger=_QuietLogger(), port=srv.port, api_url="https://api", api_key="k", version="1.0",
            keep_logs=False, session_id="sess",
        )
        ctx.forker = lambda args: ForkResult(exit_code=EXIT_ERROR, last_error_lines="1 tables permanently failed: x")
        ctx.driver_url = "postgres://x"
        ctx.configured = True
        ctx.fork_running = True
        resp = build_notification_handler(ctx).import_action(ImportRequest(backfill=True))
        assert resp.success is True
        assert hits == ["restart"]  # the live consumer is restarted to pick up the imported tables
    finally:
        srv.stop()


# ---- integration (no Docker): exact-Go N=0 vs recovery, driven through run_import_command ----------

_GZ = "202407242003015854988560000000000-abc-def-customer-2.ndjson.gz"
_ROWS = '{"id":"c1","companyId":"comp1"}\n{"id":"c2"}\n'


class _FakeRegistry:
    def __init__(self, schema_map):
        self._m = schema_map

    def get_latest_schema(self):
        return self._m

    def set_table_version(self, table, version):
        ...

    def close(self):
        ...


def _customer_schema():
    from eds.schema import Schema, SchemaProperty

    return Schema(
        table="customer", model_version="v1", primary_keys=["id"],
        properties={"id": SchemaProperty(type="string"), "companyId": SchemaProperty(type="string")},
    )


def _import_argv(tmp_path, max_retries: str) -> list[str]:
    out = str(tmp_path / "out")
    return [
        "import", "--url", "file://" + out.replace("\\", "/"), "--api-key", "k",
        "--api-url", "http://localhost", "--no-confirm", "--data-dir", str(tmp_path / "data"),
        "--max-retries", max_retries, "--only", "customer",
    ]


def _wire_fakes(monkeypatch, *, poll_results) -> dict:
    """Patch the export/poll/download client + the registry so run_import_command runs against the real file
    driver + real importer with NO network. poll_results is a list consumed one-per-poll (an Exception is
    raised, an ExportJobResponse is returned)."""
    import eds.cmd.import_cmd as ic

    state = {"polls": 0, "downloads": 0, "exports": 0}

    def fake_create_export_job(*a, **k):
        state["exports"] += 1
        return f"job-{state['exports']}"

    def fake_poll(*a, **k):
        i = state["polls"]
        state["polls"] += 1
        r = poll_results[min(i, len(poll_results) - 1)]
        if isinstance(r, Exception):
            raise r
        return r

    def fake_bulk_download(logger, data, directory, **k):
        state["downloads"] += 1
        with gzip.open(f"{directory}/{_GZ}", "wt", encoding="utf-8") as f:
            f.write(_ROWS)
        from eds.cmd.import_client import TableExportInfo
        return [TableExportInfo(table=t) for t in data.keys()]

    monkeypatch.setattr(ic, "create_export_job", fake_create_export_job)
    monkeypatch.setattr(ic, "poll_until_complete", fake_poll)
    monkeypatch.setattr(ic, "bulk_download_data", fake_bulk_download)
    monkeypatch.setattr(ic, "new_api_registry", lambda *a, **k: _FakeRegistry({"customer": _customer_schema()}))
    return state


def test_import_command_recovery_then_success(tmp_path, monkeypatch) -> None:
    # FEATURE(import-recovery): end-to-end through run_import_command + the real file driver: the first poll fails
    # transiently, the recall succeeds, the table lands, exit 0. No real sleeps (patch the recovery sleep).
    from eds.cmd.root import main

    sleeps: list[float] = []
    import eds.cmd.import_cmd as ic

    real_rwr = ic.run_with_recovery
    monkeypatch.setattr(
        ic, "run_with_recovery",
        lambda *a, **k: real_rwr(*a, **{**k, "sleep": sleeps.append}),
    )
    _wire_fakes(monkeypatch, poll_results=[
        ApiStatusError("import: bad gateway (status code=502)", 502),  # attempt 1: transient
        _job({"customer": "Completed"}),                               # attempt 2: ok
    ])
    rc = main(_import_argv(tmp_path, "5"))
    assert rc == EXIT_SUCCESS
    assert sleeps == [30]
    out = tmp_path / "out" / "customer"
    assert out.exists() and any(out.iterdir())  # the file driver wrote the rows


def test_import_command_max_retries_zero_is_exact_go(tmp_path, monkeypatch) -> None:
    # FEATURE(import-recovery): --max-retries 0 bypasses recovery entirely — the FIRST failure is fatal (exit 1),
    # the export is polled exactly ONCE (no retry, no backoff), exactly like Go.
    from eds.cmd.root import main

    state = _wire_fakes(monkeypatch, poll_results=[
        ApiStatusError("import: bad gateway (status code=502)", 502),  # would be retried if recovery were on
    ])
    rc = main(_import_argv(tmp_path, "0"))
    assert rc == EXIT_ERROR
    assert state["polls"] == 1  # single attempt, no recovery loop


def test_import_command_full_import_permafail_exits_error_not_success(tmp_path, monkeypatch) -> None:
    # IR-RECALL-01 regression (§1c.3) at the COMMAND level: a FULL import (NO --only) whose poll fails recoverably
    # every attempt must NOT collapse to "Loaded 0 tables → EXIT_SUCCESS". It retries the full set, then exits 1
    # and records the failed set for a later retry.
    import eds.cmd.import_cmd as ic
    from eds.cmd.import_cmd import compute_run_id
    from eds.cmd.root import main
    from eds.tracker import new_tracker

    real_rwr = ic.run_with_recovery
    monkeypatch.setattr(ic, "run_with_recovery", lambda *a, **k: real_rwr(*a, **{**k, "sleep": lambda s: None}))
    _wire_fakes(monkeypatch, poll_results=[
        ApiStatusError("import: service unavailable (status code=503)", 503),  # recoverable, every attempt
    ])
    url = "file://" + str(tmp_path / "out").replace("\\", "/")
    argv = [
        "import", "--url", url, "--api-key", "k", "--api-url", "http://localhost",
        "--no-confirm", "--data-dir", str(tmp_path / "data"), "--max-retries", "5",  # NO --only ⇒ full import
    ]
    rc = main(argv)
    assert rc == EXIT_ERROR  # NOT EXIT_SUCCESS — the silent-false-success bug must stay fixed
    # the failed set was recorded persistently for a later retry
    run_id = compute_run_id(url, [], [], [], None)
    tr = new_tracker(str(tmp_path / "data"))
    try:
        ok, recorded = tr.get_key(f"import-failed:{run_id}")
        assert ok and recorded  # a non-empty failed-set record exists
    finally:
        tr.close()


# ---- §6.10 Docker-gated e2e: fake export returns Failed once then Completed → table lands ----------


def _docker_up() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        return subprocess.run(["docker", "info"], capture_output=True, timeout=20).returncode == 0
    except Exception:
        return False


def _e2e_ready() -> bool:
    import importlib.util

    return _docker_up() and importlib.util.find_spec("testcontainers") is not None


@pytest.mark.skipif(not _e2e_ready(), reason="Docker + testcontainers required")
def test_e2e_recovery_lands_table_in_postgres(tmp_path, monkeypatch) -> None:
    # FEATURE(import-recovery): real PostgreSQL via testcontainers; the fake export reports the table Failed once,
    # then Completed on the recall → after recovery the table is truncated + re-imported whole and the rows land.
    from testcontainers.postgres import PostgresContainer

    import eds.cmd.import_cmd as ic
    from eds.cmd.root import main
    from eds.drivers.postgresql.sql import get_connection_string_from_url

    sleeps: list[float] = []
    real_rwr = ic.run_with_recovery
    monkeypatch.setattr(ic, "run_with_recovery", lambda *a, **k: real_rwr(*a, **{**k, "sleep": sleeps.append}))
    _wire_fakes(monkeypatch, poll_results=[
        _job({"customer": "Failed"}),     # attempt 1: export-stage Failed → recall (POST a new job)
        _job({"customer": "Completed"}),  # attempt 2: ok
    ])

    with PostgresContainer("postgres:16-alpine") as pg:
        url = (
            f"postgres://{pg.username}:{pg.password}@"
            f"{pg.get_container_host_ip()}:{pg.get_exposed_port(5432)}/{pg.dbname}"
        )
        argv = [
            "import", "--url", url, "--api-key", "k", "--api-url", "http://localhost", "--no-confirm",
            "--data-dir", str(tmp_path / "data"), "--max-retries", "5", "--only", "customer",
        ]
        rc = main(argv)
        assert rc == EXIT_SUCCESS
        assert sleeps == [30]  # one backoff after the Failed export

        import psycopg

        with psycopg.connect(get_connection_string_from_url(url)) as conn:
            rows = conn.execute('SELECT "id","companyId" FROM "customer" ORDER BY "id"').fetchall()
        assert rows == [("c1", "comp1"), ("c2", None)]
