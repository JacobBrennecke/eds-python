# Import-Log Parity & Verbosity Contract (cross-port, identical in both ports)

GOAL: `eds import` emits the SAME log MESSAGES (text + level + emit-timing) in Python and C# at BOTH levels.
- DEFAULT (no `--verbose`): matches the **Go oracle** — once per export batch (`PARITY:`). The C# change RESTORES
  this (C# drifted to per-table repetition); Python already matches Go here.
- `--verbose`: adds a **per-table detail layer** = `FEATURE(import-log-verbosity)` — no Go oracle, IDENTICAL in both
  ports. "More troubleshooting data by design" (user decision). This is the only intentional divergence here.

Source of truth for the current per-port lines: the investigation maps in this same scratchpad dir —
`implog_py.md`, `implog_cs.md`, `implog_go.md`, `import_log_parity.md`. Read them first.

## DEFAULT output (INFO) — once per batch, matches Go
1. INFO `Importing data to tables <t1,t2,...>` — ONCE, comma-joined list (NOT once per table).
2. INFO `imported <R> records from <F> files in <dur>` — ONCE, totals across the whole batch.
3. INFO terminal `👋 Loaded <N> tables in <dur>` — ONCE, WITH the duration (C# dropped the seconds in both paths;
   Python dropped them in the recovery path — restore in both).
4. ERROR `error running import: <err>` — on a failed attempt (unchanged).
5. Recovery lines (no Go oracle; align Python↔C# word-for-word; keep at DEFAULT level so recovery is visible
   without `--verbose`). Pick the cleaner existing wording and make BOTH match, e.g.:
   - INFO `recovering: retrying <tables> in <delay>s (attempt <n>/<max>)`
   - WARN `giving up on tables <tables> after <max> retries`  (precedes soft-exhaustion exit 1)
   - INFO `resuming import: skipping already-completed tables <tables>`  (cross-restart)

## `--verbose` output (DEBUG) — everything above PLUS the per-table detail layer, IDENTICAL in both ports
6. `FEATURE(import-log-verbosity)` per-table detail (the C# verbosity the user likes — gated + counts corrected):
   - DEBUG `importing table <t>` (per table), then
   - DEBUG `imported <r> records from <f> files for table <t> in <dur>` (per table) — counts are PER-TABLE
     (`f` = that table's files, `r` = that table's records), NOT the all-files total C# currently prints.
7. Existing DEBUG/TRACE lines — keep as-is, they already match across ports + Go (verify, don't churn):
   - DEBUG `creating table` / `created table` (CreateDatasource, per table)
   - DEBUG `processing file: <f>, table: <t>` (per file)
   - DEBUG `imported <n> <table> records in <dur>` (per file)
   - DEBUG `skipping file: <f>`; schema-validate TRACE/DEBUG skips
   - DEBUG `executing: <sql>` / INFO `[dry-run] <sql>` / ERROR `offending sql: <sql>` (driver-prefixed)

## Rules / non-goals
- SQL-handler lines carry the DRIVER prefix (`[postgres]`/`[postgresql]`/...) not `[import]` — LEGITIMATE, leave it.
- The logger ENVELOPE (timestamp, level tag, line format) is per-port — OUT OF SCOPE. Match the MESSAGE text +
  level + emit-timing, not the envelope.
- Duration rendering is ALIGNED across ports: a compact `µs`/`ms`/`s` format — `<1ms → {µs:.3f}µs`, `<1s →
  {ms:.3f}ms`, else `{s:.3f}s` (InvariantCulture so the decimal sep is `.`). Shared helper both ports: Python
  `format_duration` (`eds/util/duration.py`); C# `ImportDuration.Dur` (`Eds.Core/Import`) — NOT raw
  `TimeSpan.ToString()` which renders `00:00:00.0001234`. Approximates Go's `time.Duration.String()` (`%v`/`%s`),
  used for EVERY import-replay duration INCLUDING the terminal `👋 Loaded N tables in <dur>` line (Go uses `%v`
  there too — `%.1fs` was a Python-only outlier, now removed). The `in <dur>` lines match across ports for the
  same elapsed.
- C#: emit the batch-level INFO lines (#1, #2) ONCE per batch. KEEP the per-table flush boundary + the
  `import-progress:{run_id}:{table}` markers + cross-restart — only DECOUPLE the logging from the per-table loop
  (do not regress recovery behavior or the markers). Fix the once-per-table all-files count.
- Python: ADD the #6 per-table detail layer at DEBUG (it currently emits only the batch-level lines). Align the
  recovery file-iteration to Go's directory order so the per-file verbose lines come out in the same order.
- Do NOT touch the DNS-fix `is_recoverable`/classification edits already in the working tree — build around them.

## Marking & docs
- Default lines: `PARITY:` comments (Go-matching). The verbose per-table layer: `FEATURE(import-log-verbosity)`.
- Add a `migration/DEVIATIONS.md` note (both repos): "verbose-only per-table import detail; no Go oracle; identical
  Python↔C#; intentional — more troubleshooting data."
- Copy THIS file verbatim to `migration/features/import-log-verbosity.md` in your repo (byte-identical cross-port).

## TDD (required)
Write the log-assertion tests FIRST and watch them fail, THEN implement:
- a test capturing log records for a fake/small import at DEFAULT → asserts exactly the #1–#3 (+#4/#5 where
  applicable) lines & levels, ONCE each, with correct totals; asserts the #6/#7 DEBUG lines are ABSENT.
- a test at `--verbose` → asserts the #6 per-table lines appear with correct PER-TABLE counts, plus #7.
- a regression test that the C# batch INFO lines fire ONCE for an N-table batch (not N times).

## Acceptance
- Default `eds import`: message set matches Go's once-per-batch shape AND is identical Python↔C#.
- `eds import --verbose`: adds the per-table detail; identical Python↔C#; per-table counts correct.
- All existing import-recovery tests still green; markers + cross-restart + soft-exhaustion unaffected.
- Python: ruff + mypy clean. C#: build + `dotnet format --verify-no-changes` clean. Do NOT commit.
