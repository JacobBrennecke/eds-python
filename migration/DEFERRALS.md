# DEFERRALS — Go → Python port

Surface from the Go `edsGolang` reference that is **intentionally not yet ported**, with its Go location and
why. These differ from `DEVIATIONS.md` (justified *behavioral* differences in code that IS ported) — a deferral
is missing surface to revisit. Each has an inline `# DEFERRED(parity): <item> — see migration/DEFERRALS.md#<anchor>`
marker at its natural hook point.

Status at this revision: the three streaming drivers (s3, kafka, eventhub) — Driver + Importer + registry
wiring — are now FULLY ported, so the port covers all 9 Go driver schemes. The items below remain.

---

## Self-upgrade closure (RT-01 … RT-05)

The in-place self-upgrade swaps the RUNNING executable, which is only coherent for the M10 packaged single
binary AND once Python release artifacts exist (today's GitHub release assets are Go binaries). The download
MODULE (`eds/upgrade`) and the `eds download` command ARE ported and usable; the orchestration that drives them
is stubbed. The sub-items below are all coupled to that closure landing.

### rt-01-self-upgrade-apply
**Go:** `cmd/server.go:620-707` (the `upgrade` notification closure). **Python:** `eds/cmd/notification_wiring.py`
`upgrade()` refuses inside Docker (ported) then returns a "requires the packaged binary" failure instead of
running download → verify → apply → restart. Also tracked as `DEVIATIONS.md#upgrade-apply-only-for-frozen-binary`.
**Why:** swapping the live interpreter/binary is only meaningful for the frozen single-file build (M10).

### rt-02
**Restart-via-parent.** **Go:** `cmd/server.go:690` — after a successful binary swap the control plane does
`GET http://127.0.0.1:{parentPort}/restart`; `parentPort` comes from the `--parent` flag (server.go:536). **Python:**
the Layer-1 `/restart` endpoint and `--parent` forwarding already exist (`server.py` `_run_wrapper_loop`); what is
missing is capturing `parent_port` onto `ControlPlaneContext` and a `restart_via_parent()` caller in the upgrade
closure. Marker: `eds/cmd/notification_wiring.py` upgrade closure. **Why:** only reachable once RT-01 applies a swap.

### rt-03
**Wrapper upgrade-failure binary reset.** **Go:** `cmd/server.go:327,340-341,410-416` — the wrapper holds an
`inUpgrade` flag; on a FAILED post-upgrade restart it resets the child command back to the original `os.Args[0]`,
increments the failure count, and re-spawns the previous binary. **Python:** `_run_wrapper_loop` treats every
`/restart` identically (re-spawns the on-disk binary) with no `inUpgrade` state. Marker: `eds/cmd/server.py`
`_run_wrapper_loop` restart branch. **Why:** dormant until RT-01 produces upgrade restarts to recover from.

### rt-04
**Downloaded-binary version verification.** **Go:** `cmd/server.go:646-670` — exec the downloaded binary's
`version` subcommand and compare to the requested version BEFORE swapping. **Python:** no equivalent in
`eds/upgrade/`; the caller is part of the stubbed RT-01 orchestration. Marker: `eds/cmd/notification_wiring.py`
upgrade closure. **Why:** only meaningful as part of the apply path.

### fork-update-strategy
**`fork --update-strategy` flag (deprecated, value ignored).** **Go:** `cmd/fork.go:294-296` declares the flag,
marks it deprecated, and ignores the value. **Python:** the flag is absent and `fork` uses `allow_abbrev=False`
strict parsing, so passing `--update-strategy` ERRORS (vs Go's accept-and-ignore). **Why:** purely a deprecated
compatibility shim coupled to the (deferred) Snowflake update-strategy/self-upgrade story; functionally harmless.
If a server is observed forwarding it, add a hidden, ignored `fork --update-strategy` to `root.py`'s fork parser.

---

## integrationtest
**Go:** `cmd/integrationtest.go` + the whole `internal/integrationtest` package (`connection.go`, `random.go`,
`filedata.go`); subcommands `loadtest-random` and `publish-file-data` (a NATS load generator —
`PublishRandomMessages` / `generateRandomCustomer` — and a JSONL.tar.gz replayer — `PublishTestData`). **Python:**
no `integrationtest` subparser in `eds/cmd/root.py` and no `eds/integrationtest` module. Marker: `eds/cmd/root.py`
`build_parser`. **Why:** dev/test harness only (explicitly deferred in SPEC.md lines 71, 385-386); not part of the
production consumer surface.

---

## LOW (cosmetic / dev-tooling)

### e2e-command
**Go:** the build-tagged (`//go:build e2e`) `cmd/e2e.go` + `internal/e2e` multi-driver harness (`RunTests`, ~14
files). **Python:** no `e2e` subparser or `eds/e2e` package — replaced by the pytest `tests/test_*_e2e.py`
Docker-gated suite (now including `test_s3_e2e.py` and `test_kafka_e2e.py`). Marker: `eds/cmd/root.py`
`build_parser`. **Why:** build-tag-gated dev tooling; the C# port made the same substitution (xUnit E2E classes).

### metrics-memory-load-field-subset
**Go:** `internal/metrics.go:51-118` exposes the full gopsutil `VirtualMemoryStat` (~37 fields) + `load.AvgStat`.
**Python:** `eds/metrics.py` `MemoryStat` exposes only 5 fields (total/available/used/usedPercent/free) and
`LoadStat` zeros on Windows. Already documented as `DEVIATIONS.md#metrics-memory-load-partial`. **Why:** a
field-level cosmetic subset of an otherwise-complete subsystem; the C# port uses the SAME 5-field subset.

### sql-snowflake-help-text
**Go:** `internal/util/help.go` `GenerateHelpSection` + the `mysql`/`postgresql`/`sqlserver`/`snowflake`
`Help()` bodies (a color-formatted "Schema" section: *"The database will match the public schema from the
Shopmonkey transactional database."*). **Python:** `generate_help_section` IS now ported (`eds/util/help.py`,
used by the s3/kafka/eventhub drivers), but the SQL/Snowflake `help()` bodies still return `""`
(`sql_base.py::help`, `snowflake.py::help`). Already documented as `DEVIATIONS.md#sql-driver-help-deferred`.
**Why:** cosmetic; `help()` feeds only the CLI driver-metadata commands, not the data path. To close: fill those
`help()` bodies with `generate_help_section("Schema", "…")`.
