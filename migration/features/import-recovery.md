# Import Recovery / Retry — Cross-Port Feature Spec & Contract (eds-python + eds-dotnet)

> ============================================================================
> **THIS IS NOT A GO-PARITY TARGET.**
> The Go reference (`edsGolang`) has NO import-level recovery — its only retry is a per-HTTP-request
> transport retry (`util.NewHTTPRetry`); the import command `Fatal`s on the first failed table. This
> feature is an intentional, deliberate divergence (like `audit-mode`). For it, **this document is the
> source of truth, not the Go source.** Both ports MUST behave identically and assert against the
> contract here. Go-parity audits SKIP `FEATURE(import-recovery)`-marked code.
> ============================================================================

Feature name: **import-recovery** → marker `FEATURE(import-recovery)`.
Status: **ACCEPTED** — design decisions resolved by the user (see §1b, which overrides the §2 defaults).
Grounded in the real code (file:line cited throughout); synthesized from four research maps.

---

## 1. Overview + Go-divergence framing

**Goal (user's words):** during an import run, if a recoverable issue occurs (timeout, network error,
etc.), automatically recover — log the error as we do today, DETECT which table(s) failed, RE-CALL the
import for JUST those failed tables using the EXISTING job ID (so the bulk-export endpoint resumes
per-table where it left off), then continue ahead. Standard exponential backoff, exactly 5 retries:
30s, 60s, 120s, 240s, 480s.

**FRAMING VERDICT — NET-NEW FEATURE, NOT a parity gap.** Governed exactly like the existing
`audit-mode`: Go is *not* the oracle, `features/import-recovery.md` is.

- The ONLY retry anywhere in Go's import path is `util.NewHTTPRetry` — a per-single-HTTP-request
  transport retry. Confirmed verbatim at `edsGolang/internal/util/http.go:14-63`: retries on
  `"connection reset"`/`"connection refused"` substrings but ONLY inside a 30s window
  (`http.go:28-29`), or HTTP `408/502/503/504/429` (`http.go:33-38`), with jittered sleep
  `100ms + rand(500*attempt)ms` (`http.go:51`). It is used ONLY by `createExportJob`
  (`cmd/import.go:95-96`) and `checkExportJob` (`cmd/import.go:165-166`).
- It is a DIFFERENT altitude (one HTTP request, not a table set) and a DIFFERENT policy (30s
  jitter-window, not a fixed 5-step ladder).
- At the import-orchestration layer Go has NO recovery: `checkExportJob` raises on the FIRST table
  whose `Status=="Failed"` (`cmd/import.go:177-181`, confirmed), `pollUntilComplete` propagates
  unchanged (`cmd/import.go:194-197`, confirmed), and the import command then `logger.Fatal`s →
  process dies. Downloads (`downloadFile`) and the replay loop (`importer.go`) have ZERO retry.
- Both ports faithfully reproduce only the HTTP-transport retry (`eds-python` `HttpRetry` in
  `import_client.py`; .NET `ExportJobClient`); the Python parity audit calls it "parity-exact"
  (`migration/DEVIATIONS.md`). **Leave that layer alone.**

Therefore: detect-failed-tables → re-call export for JUST those tables on the SAME job id →
exponential 30/60/120/240/480 → continue = intentional divergence. Tag `FEATURE(import-recovery)`;
Go-parity audits SKIP it.

**Two existing primitives the recall builds on (real in Go + both ports — the user's facts check out):**
- tables filter: `exportJobCreateRequest.Tables []string` (`cmd/import.go:57`) → request body `tables`
  (omitempty); surfaced as `--only` (`cmd/import.go:720`) and `ImporterConfig.Tables`.
- job id: `--job-id` "resume an existing job" (`cmd/import.go:707`); `ImporterConfig.JobID`; status via
  `GET /v3/export/bulk/{jobID}` (`cmd/import.go:157`). No NEW endpoint plumbing required.

---

## 1b. RESOLVED DECISIONS (user, 2026-06-29) — these OVERRIDE the §2 defaults + close OQ-2/4/6/7/10

**OQ-2 resume mechanism — CORRECTED (the original "same job id resumes per-table cursor" premise is wrong).**
There is NO per-table cursor resume. The real bulk-export API offers two recovery paths:
1. **`GET /v3/export/bulk/{jobId}`** — re-downloads the **ENTIRE** data set for that job, but ONLY while the
   signed URLs are still active. Use this for a **download/import-stage** failure (export already succeeded).
2. **`POST /v3/export/bulk`** with body `{ "tables": [<failed>], "companyIds": [...], "timeOffset": <ms> }`
   (`locationIds` optional; MINIMUM required = `tables` + `companyIds` + `timeOffset`) — this **mints a NEW
   jobId** and **fully re-exports just those tables**. Use this when an **export-stage** table is `Failed` or
   when the original signed URLs have expired.

So the recall is: re-export the failed tables via POST (full re-export → **NEW jobId**), or re-download the
existing job via GET when its URLs are still valid. Consequences:
- Each retried table is a **FULL re-export of the whole table** ⇒ **TRUNCATE that table, then re-import it
  whole** before replay (closes OQ-7 = re-truncate, OQ-10 = whole-table). No append/cursor logic.
- The recovery MUST thread the original run's **`companyIds` + `timeOffset`** (and `locationIds` if used) into
  the POST — capture them up front so they survive into every retry (and across a restart).
- A POST changes the jobId; recovery must track the **current** jobId per retried set (the per-table
  completion markers below are keyed by the ORIGINAL run identity, not the volatile export jobId).

**OQ-4 after-exhaustion — SOFT (do NOT hard-abort).** When a table set exhausts all 5 retries: log the error +
the exact failed table list, **record the failed set persistently** (so it can be retried later), and
**CONTINUE AHEAD** — import the remaining tables and let the downstream **server run and process incoming
data**. For the server-triggered import this rides the existing "non-usage failure → Success=true → consumer
starts" semantics (do not regress that). The run does not block streaming on permanently-failed tables; a later
import can re-attempt the recorded set.

**OQ-6 crash survival — CROSS-RESTART (yes).** Add a per-table flush boundary and persist a per-table
completion marker (`import-progress:{runId}:{table}`, keyed by a stable run identity, NOT the volatile export
jobId) to the tracker after each table durably flushes. A (re)started import reads the markers and resumes only
the not-yet-completed tables, surviving a full process crash (spec phase P2 is now in-scope, not optional).

**Adopted (spec defaults, unchanged):** OQ-1 scope = built inside the import run (CLI + server fork both);
OQ-3 = use the persisted per-table markers for detection; OQ-5 = always-on, `--max-retries` default 5 / `0` =
exact-Go; OQ-8 = retryable `408/500/502/503/504/429` + transport/timeout + export `Failed` + download IO, fatal
`401/403/400/404/422` + usage-3 + schema/config + cancellation; OQ-9 = fixed `[30,60,120,240,480]` ladder,
count-only tunable.

---

## 1c. REVIEW RECONCILIATION — canonical cross-port decisions (AUTHORITATIVE; supersedes any contradicting prose in §2)

After the per-port adversarial reviews, these are the binding cross-port decisions. Both ports MUST match
byte-for-byte. Where §2's older prose says "same job_id resumes per-table cursor", §1b + §1c override it.

1. **Download-stage recall (canonical).** A transient DOWNLOAD failure (export already succeeded) →
   **GET `/v3/export/bulk/{jobId}`** to re-download the SAME job while its signed URLs are valid; **POST** a new
   job (full re-export of the failed subset) ONLY when the URLs have expired (HTTP **403/410**). Export-stage
   `Failed` still → POST new job. (FIX: Python was always-POSTing on ANY download error; C# was GET-only with no
   expiry fallback.)
2. **`run_id` (one identical formula).**
   `eds_hash("|".join([driver_url, ",".join(sorted(only)), ",".join(sorted(companyIds)), ",".join(sorted(locationIds)), str(timeOffset)]))`
   — driver_url FIRST, every list SORTED, "|" field separator, identical hash + timeOffset rendering in both
   ports. Pin with a golden `run_id` vector. (FIX C#: omitted driver_url, unsorted, no separator → ALSO fixes the
   silent-data-loss where re-pointing `--url` reused stale done-markers and skipped tables on the new DB.)
3. **Empty-scope fallback (Python HIGH).** When there are NO per-table markers AND no `--only`, a recoverable
   failure's candidate set is the FULL table set — NEVER `[]`. A recoverable failure must NEVER collapse to
   "0 tables → EXIT_SUCCESS". (C# already correct via empty-scope→full re-export.)
4. **Retryable classification (canonical, identical both ports).** Retry by EXCEPTION TYPE first (connection
   reset/refused, socket errors, read/write timeout, HTTP transport) + HTTP `408/429/500/502/503/504` + export
   `Failed` + download HTTP/network IO. FATAL (no retry): malformed URL → exit 3; `PermissionError`, disk-full
   (ENOSPC), other non-network local OSErrors; `401/403/400/404/422`; schema/config; cancellation. Shared substring
   fallback set: `connection reset | connection refused | broken pipe | timed out | EOF | no such host | tls
   handshake | dns`. (FIX Python: stop treating ALL OSError as retryable. FIX C#: align the substring set; don't
   blanket-retry `IOException` for local disk errors.)
5. **`--no-delete` / audit-mode → NO re-truncate.** When `--no-delete` is set (or the destination is audit/append
   mode), do NOT re-truncate the retried table on retry — it would drop the audit trail / prior data. Accept the
   at-least-once re-append (PK-safe for the single in-flight table). (Python IR-RECALL-02; force-truncate REJECTED.)
6. **Bad `--max-retries` → clamp to 0** (recovery OFF), both ports (FIX C#: was resetting to 5).
7. **`--max-retries` in help.** List it in the import (and server) command help/usage in BOTH ports (FIX C#:
   missing from the hand-rolled `CliHelp`; Python argparse auto-lists it).
8. **Per-table marker storage.** Both ports WRITE + READ `import-progress:{run_id}:{table}` markers (read on
   restart to skip completed tables); identical resume semantics. (Align C# to read the per-table markers, not only
   a compact index key.)
9. **Streaming sinks (kafka/eventhub/s3/file): per-run resume only.** No per-table mid-run durability; a retry may
   re-emit already-delivered records (at-least-once). DOCUMENTED known property — downstream must tolerate dupes.
10. **`timeOffset` threading.** Capture + reuse the ORIGINAL run's `timeOffset` in every recall POST (avoid
    snapshot skew); if none was set originally, document the caveat. Also remove the dead `ExportFailed` exception.

---

## 2. Recovery behavior contract

### 2.1 The recovery unit (what one "attempt" wraps)
One attempt = the per-table-set slice of the pipeline that can fail transiently (SLICE A §2):
`createExportJob(tables=<set>)` *(skipped when a live job id already exists)* → `pollUntilComplete(jobID)`
→ `bulkDownloadData` → `importer.Import(JobID=jobID, Tables=<set>)`.
Hooks: Go `cmd/import.go:555-670`; Py `eds/cmd/import_cmd.py:_do_import` (185-236); .NET
`ImportService.RunAsync` (104-158) → `ImportRunner.Run`.

### 2.2 The retry loop (per failed table set)
On a recoverable failure inside an attempt:
1. **Log the error as today** — same `logger.Error("error running import: ...")` text, no logging
   behavior change.
2. **Detect the failed table set** (see §2.4).
3. **Re-call for JUST that subset, SAME job id** — next attempt threads `JobID` unchanged and
   `Tables = failed_set` (strict subset). It re-polls `GET /v3/export/bulk/{jobID}` (server resumes
   per-table cursor) and, if the backend needs re-trigger, re-POSTs `/v3/export/bulk` with
   `tables=failed_set`.
4. **Sleep `delay(n)`** (injected clock — §6), then retry.

### 2.3 Backoff schedule (LOCKED)
Exactly **5 retries == 6 total attempts**. Delays in seconds before retries #2..#6:
**`[30, 60, 120, 240, 480]`** = `delay(n) = 30 * 2^n` for `n = 0..4`.
- Attempt #1 (initial) has no preceding sleep. Total wall added by sleeps = 930s (15.5 min).
- Implemented as an explicit list AND asserted `== 30*2^n` in a test, pinning the magic numbers both
  ways.
- Schedule is FIXED/non-configurable in v1; only the *count* is tunable (§4). Truncates from the front
  (`N=2` → `[30,60]`).
- Composes with — does NOT replace — the inner `HttpRetry` jitter: a single attempt may itself do
  jittered HTTP retries; only after those exhaust does the OUTER ladder count one failed attempt.

### 2.4 Failed-table detection method
SLICE C is the load-bearing finding: **no persisted per-table import-progress record exists today**,
and the importer is single-threaded with EXACTLY ONE flush boundary for the whole run
(`importer.go` `ImportCompleted()` at end), with the batching driver buffering ACROSS table boundaries
(`postgresql.go:189-218`). So destination DB state cannot tell you which table finished.

Detection by failure stage:
- **Export-stage failure (poll):** authoritative signal is the status map
  `ExportJobResponse.tables[*].status == "Failed"` (`cmd/import.go:177-181`; Py
  `import_client.py:188-190`; .NET `ExportJob.cs:119-126`). Change `checkExportJob` to additively
  return the FULL failed set instead of raising on the first (keep raise-on-first wrapper for the
  `N=0`/disabled path so Go behavior is preserved).
- **Import/load-stage failure:** the failed set = the `Tables` the attempt was scoped to (the runner
  discards which table it was on — `importer.go:119-121`).
- **Reliable per-table progress (recommended, see OQ-3):** add a per-table flush boundary at each
  table boundary, then write an incremental job-scoped marker `import-progress:{jobID}:{table}` via the
  existing tracker KV (`tracker.go` `SetKey`/`GetKey`) after each table durably flushes. Then
  `incomplete(jobID) = requested_tables − {T : marker present}`. Reset markers via
  `DeleteKeysWithPrefix` on whole-run success.
- **"Don't know yet" case** (failure during create/poll/download, zero markers): fall back to the
  candidate set in priority order — export-job status by jobID → temp-dir scan → the `--only` filter
  (or all tables) — and retry the entire candidate set.

### 2.5 Retryable vs FATAL classification — `is_recoverable(err) -> bool` (one pure predicate)
**Retryable (counts an attempt, backs off):**
- Transport/network: connection reset, connection refused, DNS failure, broken pipe, EOF, read/write
  timeout. (Superset of Go's two substrings — classify by exception/socket TYPE first, message
  substring as fallback.)
- HTTP transient: `408, 500, 502, 503, 504, 429` (mirrors `http.go:33-38`, adds 500).
- Per-table export `Status == "Failed"` (the canonical recoverable case).
- Download failure of a presigned URL (non-200 / IO error).

**Fatal — NO retry (fail fast, native exit code):**
- Usage / `EXIT_INCORRECT_USAGE (3)`: driver `Test` failure, bad flags, unparseable config.
- Auth: `401 / 403`.
- Other 4xx: `400, 404, 422` (malformed / unknown job id / unprocessable).
- Schema/config: genuine schema-decode/registry error. (Row-level `ErrSchemaValidation` is already a
  per-row skip — `importer.go:97-101` — unchanged.)
- User cancellation / context-cancelled / shutdown — abort immediately, do NOT consume a retry.

### 2.6 After-exhaustion behavior
**Give up that table set, continue the rest, then fail the overall run non-zero (no silent success).**
- A permanently-failed set is logged terminally (`"giving up on tables X after 5 retries: <last
  error>"`) and recorded in `failed_tables`; recovery continues to remaining sets.
- End-of-run: if `failed_tables` non-empty → `success=false` → `os.Exit(1)` / `EXIT_GENERIC(1)` /
  `ExitCodes.Error`, matching Go's existing import-error exit. Print summary
  `"imported N tables; M tables permanently failed: ..."`.
- A FATAL error short-circuits the whole run immediately with its native code (3 for usage, 1
  otherwise) — never becomes a per-table give-up. Must NOT convert usage(3) into a retry.

### 2.7 Re-import idempotency PREREQUISITE (SLICE C §5)
The replay writes plain `INSERT` (`postgresql.go:194`), not upsert; full import truncates via
`CreateDatasource` at run start (`importer.go:34-37`). A per-table retry MUST re-truncate just the
retried table before re-importing, or it duplicates rows / violates PKs. This is a hard prerequisite —
detection alone does not make retry safe. (See OQ-7.)

---

## 3. Additive extension points per port

All additions tagged `FEATURE(import-recovery)`; fully additive — no existing symbol changes behavior
when `--max-retries 0` or when no failure occurs.

**eds-python** (`eds/cmd/import_cmd.py`, `eds/cmd/import_client.py`):
- `is_recoverable(err) -> bool`.
- `failed_tables(job: ExportJobResponse) -> list[str]` (additive sibling of raise-on-first
  `check_export_job`, `import_client.py:188-190`).
- `run_with_recovery(...)` wrapping the export→poll→download→import unit (§2.1) with the ladder,
  reusing the EXISTING injectable `sleep`/`now` seam (`poll_until_complete(..., sleep, now)`,
  `import_client.py:198-225`).
- `--max-retries` in `root.py` parser; `import_max_retries` via `set_config_value`.
- Tests: `tests/test_import_recovery.py`.

**eds-dotnet** (`src/Eds.Core/Import/`, `src/Eds.App/ImportService.cs`, `ExportJob.cs`):
- `bool IsRecoverable(Exception)`.
- `IReadOnlyList<string> FailedTables(ExportJobResponse)`.
- `RunWithRecovery(...)` (inject `Func<TimeSpan,Task>` / `ISystemClock` delay) wrapping
  `ImportService.RunAsync` body (104-158).
- `--max-retries` option; `EdsConfig.SetValue(dataDir,"import_max_retries",...)`.
- Tests: `Eds.Tests/Import/ImportRecoveryTests.cs`.

**Server-triggered import — covered transparently.** The control-plane fork (`cmd/server.go`
`runImport` 759-836; Py `notification_wiring.py` `_run_import` 53-119; .NET
`ServerControlPlane.ForkImport`/`ClassifyImportResult` 443-527) just forks a one-shot `eds import` and
maps exit codes (0 ok; 3 invalid-url; default non-zero uploads logs). Because the new retry lives
INSIDE the import run, it transparently benefits BOTH the standalone CLI and the server fork — no
control-plane change needed.

---

## 4. Config / flag surface
**Always-on by default** (user said "automatically recover"), escapable + tunable, precedence/
persistence copied from audit-mode §1.1 (preserve `token`/`server_id`/`url` on read-modify-write):

| Surface | py | .NET | Default | Meaning |
|---|---|---|---|---|
| flag | `--max-retries N` | `--max-retries N` | `5` | `N=0` disables recovery → exact Go behavior |
| config | `import_max_retries` | `import_max_retries` | `5` | persisted in `config.toml` |
| precedence | explicit flag > config.toml > built-in `5` | same | — | identical to `mode` |

The schedule (`30,60,120,240,480`, base 30s, ×2) is fixed/non-configurable in v1. `N=0` is the
explicit "behave exactly like Go" escape hatch, exercised by a test.

---

## 5. Governance — `features/import-recovery.md` cross-port contract
Reuse the audit-mode machinery verbatim:
1. **Marker convention:** every net-new symbol/branch/test tagged `# FEATURE(import-recovery): <note>`
   (py) / `// FEATURE(import-recovery): <note>` (.NET).
2. **Go is NOT the oracle banner** (copy audit-mode.md lines 3-11): "THIS IS NOT A GO-PARITY TARGET …
   this document is the source of truth … both ports MUST behave identically and assert against the
   golden vectors here." Go-parity audits SKIP `FEATURE(import-recovery)` code.
3. **DEVIATIONS.md** in BOTH ports gets a new `import-recovery` anchor pointing at the feature doc (it
   is a FEATURE, not a DEFERRAL).
4. **`features/import-recovery.md` ToC:** §0 markers; §1 behavior contract (§1.1 flag/config/precedence;
   §1.2 recovery unit; §1.3 retryable-vs-fatal; §1.4 exhaustion + exit codes); §2 **golden backoff
   vector** (ladder `[30,60,120,240,480]`, attempt count 6, recall payload shape
   `{tables:[<failed>], job_id:<same>}` — both ports assert byte/shape-identically); §3 additivity
   ledger; §4 cross-port consistency checklist; §5 per-port risk notes.

---

## 6. Test strategy (injectable clock — no real waits)
Reuse the EXISTING `sleep`/`now` seam (`import_client.py:198-225` + .NET twin).
1. **Backoff-ladder (headline):** fake `sleep` records args; force recoverable failure every attempt;
   assert sleeps == `[30,60,120,240,480]` (== `30*2^n`), attempt count == 6, then give-up. Zero wall
   time.
2. **Recall-payload:** attempt #1 fails `{b,d}` of `{a,b,c,d}`; assert attempt #2 body
   `tables == ["b","d"]` (subset) AND `job_id` identical; #2 succeeds.
3. **Recovery-then-success:** one transient failure → exactly one 30s sleep, exit 0, all tables in.
4. **Classification matrix:** parametrized `is_recoverable()` over reset/refused/timeout/500/502/503/
   504/408/429 → True; 400/401/403/404/422/usage-3/schema/cancel → False.
5. **Fatal short-circuits:** inject 401 → zero sleeps, no recall, exit 1; usage-3 → exit 3.
6. **Exhaustion:** one set fails all 6, another succeeds → good set imported, bad set logged terminal,
   process exits non-zero with summary.
7. **Disabled escape hatch:** `--max-retries 0` → exact Go behavior (first failure fatal, no sleeps).
8. **Cancellation mid-backoff:** trip cancel during a sleep → abort, do NOT consume remaining retries.
9. **Cross-port twin:** golden backoff vector + recall shape asserted identically in both ports.
10. **e2e (Docker-gated):** fake export server returns `Failed` for one table then `Completed`, real
    DB driver → table lands after recovery, recall hit same job id.

---

## 7. Phased implementation plan
- **P0 — Contract + governance:** write `features/import-recovery.md` (golden vectors first) + add
  DEVIATIONS.md anchors in both ports. No code yet.
- **P1 — Pure predicate + detection:** `is_recoverable` + `failed_tables` (additive sibling, both
  ports) + their unit tests (matrix). No behavior change to the live path.
- **P2 — Per-table durability + progress markers** (SLICE C §3): add the per-table flush boundary +
  `import-progress:{jobID}:{table}` marker + reset-on-success. Gate behind recovery being enabled.
  (Depends on OQ-3/OQ-7 confirmation.)
- **P3 — Retry wrapper:** `run_with_recovery` / `RunWithRecovery` with injected clock + the ladder,
  wired into the import unit; flag/config plumbing (`--max-retries`, `import_max_retries`). Re-truncate
  retried table (§2.7).
- **P4 — Tests:** ladder, recall-payload, recovery-then-success, exhaustion, fatal short-circuit,
  `N=0`, cancellation, cross-port twin.
- **P5 — e2e (Docker-gated)** + final cross-port consistency audit against the golden vectors.

---

## 8. OPEN QUESTIONS (confirm before build; recommended default in **bold**)

- **OQ-1 — Scope:** standalone `eds import` only, or also server-triggered import? **Default: build it
  INSIDE the import run so BOTH the CLI and the control-plane fork get it for free (no server change).**
- **OQ-2 — External-API resume premise (BLOCKING):** the user states the bulk-export endpoint accepts
  a table subset on the SAME job id and resumes per-table from `cursor`. SLICE B found NO code-level
  evidence: `exportJobCreateRequest` has no jobId field, so POST always mints a NEW job; today's
  "resume" is purely client-side (`--job-id` re-polls/re-downloads, `--only` filters locally). **Default:
  verify against the live Shopmonkey API before P3; if POST does not resume by jobId, the recall
  re-POSTs `tables=failed_set` and relies on the server's per-table `cursor` keyed by job id.**
- **OQ-3 — Failed-table detection method when ambiguous:** rely solely on export-job
  `status=="Failed"` + scoped `Tables`, or also add the persisted per-table progress marker? **Default:
  add `import-progress:{jobID}:{table}` markers (only reliable signal for load-stage failures, since
  the importer has one flush boundary and buffers across tables).**
- **OQ-4 — After-exhaustion behavior:** abort whole run on first exhausted set, or give up that set,
  continue the rest, and exit non-zero at the end? **Default: continue the rest, exit non-zero with a
  summary (salvage max, no silent success).**
- **OQ-5 — Always-on vs flag:** ship recovery on by default, or behind an opt-in flag? **Default:
  always-on (default `--max-retries 5`), escapable via `--max-retries 0` / `import_max_retries`.**
- **OQ-6 — Per-run state survives a process crash:** must recovery resume across a full process
  restart (not just in-process timeout/network errors)? **Default: yes — markers are persisted to the
  tracker (BuntDB `SyncPolicy=EverySecond` / LiteDB) immediately after each table's durable flush, so
  a restarted run with the same job id reads prior progress.**
- **OQ-7 — Re-import idempotency:** replay is plain INSERT, not upsert; a partially-imported table will
  duplicate rows / violate PKs on retry. Re-truncate just the retried table, or switch to upsert?
  **Default: re-truncate the retried table before re-importing (smallest faithful change); upsert is a
  larger driver change deferred.**
- **OQ-8 — Retryable error set exactness:** confirm the matrix — add 500 to Go's `408/502/503/504/429`?
  treat per-table export `Failed` as always-retryable (vs a permanent export error)? **Default:
  retry `408/500/502/503/504/429` + transport/timeout + export `Failed` + download IO; treat
  `401/403/400/404/422`, usage-3, schema/config, cancellation as FATAL.**
- **OQ-9 — Schedule configurability:** keep the `30/60/120/240/480` ladder fixed, exposing only the
  count, or make base/multiplier configurable too? **Default: fixed ladder in v1; only the count is
  tunable (truncates from the front).**
- **OQ-10 — Partial table semantics:** is a table that had mid-stream byte-threshold flushes before
  the crash counted as failed (re-import whole) or attempted-resume? **Default: count partial as
  failed and re-import the whole table — there is no row-level cursor in the importer.**
