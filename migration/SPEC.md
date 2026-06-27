# EDS Consumer — Go → Python Migration Specification (Stage 1)

> **Goal of Stage 1:** a *faithful, fully-functional* Python (3.10) replica of the Go
> `edsGolang` Shopmonkey Enterprise Data Streaming (EDS) consumer. The **logic,
> decisions, and observable functionality must be equal**. Implementation details
> (libraries, idioms, concurrency primitives) may and should differ to be idiomatic
> Python, but **every behavioral decision** — SQL byte output, batching cadence, retry
> policy, NATS subjects, wire formats, exit codes, masking, hashing, defaults, and
> the documented quirks — must match.
>
> This document is the **synthesizing contract**. Per-subsystem behavioral detail
> lives in [`research/`](./research) (9 deep maps, one per subsystem). When this
> spec and a research doc disagree, the Go source in `../edsGolang` is the source
> of truth.

---

## 1. What EDS is

EDS is a long-running consumer that streams Change-Data-Capture (CDC) events from
Shopmonkey's platform (over NATS JetStream) into a customer-chosen destination
(PostgreSQL, MySQL, SQL Server, Snowflake, S3, Kafka, Azure EventHub, or local
files). It also performs a one-time **bulk import/backfill** from the Shopmonkey
export API, handles **schema migrations** on the fly, **self-upgrades**, reports
**health/metrics/logs** back to Shopmonkey, and is remotely controllable through a
NATS control plane (restart / pause / configure / import / upgrade / …).

### 1.1 Top-level runtime topology (must be reproduced)

```
            Shopmonkey API (HTTPS)            NATS (JetStream + control plane)
                  │                                   │
   enroll / sessionStart / sessionEnd / schema /      │ dbchange.*  +  eds.notify.* / eds.client.*
   export-bulk / log-upload                           │
                  ▼                                   ▼
        ┌──────────────────────────────────────────────────────┐
        │  eds server  (WRAPPER process)                        │
        │   • localhost HTTP control (/restart)                 │
        │   • session lifecycle, log upload, upgrade apply      │
        │   • notification (control-plane) consumer             │
        │   • forks ───────────────┐                            │
        └──────────────────────────┼────────────────────────────┘
                                    ▼
        ┌──────────────────────────────────────────────────────┐
        │  eds fork  (CHILD / worker process)                   │
        │   • health + /metrics HTTP server                     │
        │   • localhost HTTP control (/control/pause|unpause|…) │
        │   • Tracker (BuntDB→LiteDB), APIRegistry, Driver      │
        │   • Consumer (JetStream pull + adaptive batching)     │
        └──────────────────────────────────────────────────────┘
```

The **two-process wrapper/fork model is a hard requirement** for faithfulness: it
is how crash detection, restart, and self-upgrade work. (See §6 for the exit-code
contract that glues the two processes together.)

---

## 2. Source inventory & target mapping

Go module: `github.com/shopmonkeyus/eds` (~14.6k non-test LOC, ~18k with tests).

| Go area | Responsibility | C# target (namespace / project) |
|---|---|---|
| `main.go`, `cmd/root.go` | entrypoint, cobra root, logging setup, config (viper/TOML) | `Eds.App` (`Program`, `RootCommand`, `Logging`) |
| `cmd/server.go` | wrapper loop, session lifecycle, log upload, upgrade orchestration | `Eds.App.Commands.ServerCommand`, `Eds.App.Server.*` |
| `cmd/fork.go` | child worker: tracker/registry/driver/consumer wiring, control HTTP | `Eds.App.Commands.ForkCommand`, `Eds.App.Fork.*` |
| `cmd/import.go` | bulk import command + export-job API + concurrent download | `Eds.App.Commands.ImportCommand`, `Eds.App.Import.ExportClient` |
| `cmd/enroll.go`, `download.go`, `publickey.go`, `version.go` | aux commands | `Eds.App.Commands.*` |
| `cmd/driver_*.go` | build-tag driver registration shims | `Eds.Drivers.DriverRegistration` (+ MSBuild `DefineConstants`) |
| `cmd/integrationtest.go`, `internal/integrationtest/*`, `internal/e2e/*` | dev/test harnesses | `Eds.Tests` (deferred; not production path) |
| `internal/driver.go` | `Driver` + capability interfaces, registry, `DriverField`, config helpers | `Eds.Core.Drivers` |
| `internal/dbchange.go` | `DBChangeEvent` CDC model | `Eds.Core.DBChangeEvent` |
| `internal/schema.go` | `Schema`, `SchemaProperty`, `SchemaRegistry`, `SchemaValidator` ifaces, `UpdateDestinationSchema` | `Eds.Core.Schema.*` |
| `internal/migration.go` | `DriverMigration` interface | `Eds.Core.Drivers.IDriverMigration` |
| `internal/importer.go`, `internal/importer/importer.go` | importer interfaces + replay engine | `Eds.Core.Import.*` |
| `internal/metrics.go` | Prometheus instruments + `SystemStats` | `Eds.Core.Metrics` |
| `internal/tracker/tracker.go` | BuntDB key/value state store | `Eds.Core.Tracking.Tracker` (LiteDB or SQLite) |
| `internal/registry/*.go` | API-backed schema registry (3-tier cache) | `Eds.Core.Registry.ApiRegistry` |
| `internal/consumer/*.go` | JetStream consumer, batching, heartbeats, migration-on-the-fly | `Eds.Consumer.*` |
| `internal/notification/*.go` | NATS control plane | `Eds.App.Notification.NotificationConsumer` |
| `internal/api/api.go` | Shopmonkey API DTOs | `Eds.Core.Api.*` |
| `internal/util/*.go` | HTTP retry, mask, hash, JSON, SQL helpers, schema validator, cache, batcher, sysinfo, files | `Eds.Core.Util.*` |
| `internal/upgrade/*.go` | download+PGP-verify+extract, atomic binary swap | `Eds.App.Upgrade.*` |
| `internal/osext/*.go` | exe-path resolution (**dead code**) | dropped → `Environment.ProcessPath` |
| `internal/drivers/{postgresql,mysql,sqlserver,snowflake,s3,kafka,eventhub,file}` | the 8 destination drivers | `Eds.Drivers.<Name>` |
| external `go-common/{logger,nats,command,string,sys,compress,slice}` | shared infra (NOT in repo) | reimplemented in `Eds.Core.Common.*` |

### 2.1 The 8 drivers (capabilities to preserve exactly)

| Driver | Scheme(s) | MaxBatchSize | Import | Migration | Output |
|---|---|---|---|---|---|
| PostgreSQL | `postgres`, alias `postgresql` | 500 | yes | yes | `INSERT … ON CONFLICT DO UPDATE` / `DELETE` (transactional batch) |
| MySQL | `mysql` | 500 | yes | yes | `REPLACE INTO` / `DELETE` (transactional batch) |
| SQL Server | `sqlserver`, alias `mssql` | 500 | yes | yes | `MERGE` / `DELETE` (transactional batch) |
| Snowflake | `snowflake`; `snowflake-keypair` | 200 | yes (stage+COPY) | yes | batched `MERGE`/`DELETE` multi-statement, tracker-gated dedup |
| S3 | `s3` | 1000 (hard) | yes | no | one JSON object per event, worker-pool upload |
| Kafka | `kafka` | -1 | yes | no | one JSON message/event, custom partitioner, 1000 internal cap |
| EventHub | `eventhub` | -1 | yes | no | JSON event-data, partition-key batched |
| File | `file` | -1 | yes | no | one JSON file/event under `<dir>/<table>/<unixSec>-<pk>.json` |

---

## 3. Core domain contracts (the spine)

These types/interfaces are the contract every other component depends on; port
them first and exactly.

### 3.1 `DBChangeEvent` (the CDC record)

Fields with **exact JSON names** (drivers serialize this verbatim for S3/Kafka/
EventHub/File, so naming + `omitempty` + field order matter):

```
operation, id, table, key[], modelVersion,
companyId?, locationId?, userId?,          (omitempty pointers)
before (raw JSON, omitempty), after (raw JSON, omitempty),
diff[] (omitempty), timestamp (int64 epoch MILLIS), mvccTimestamp,
imported (omitempty, import-only)
```
Plus non-serialized: cached `object` (lazy parse of `after` else `before`),
`SchemaValidatedPath`. Methods: `GetPrimaryKey()` (last of `key`, else
`object["id"]`, else `""`), `GetObject()`, `OmitProperties(...)`, `String()`.
`DBChangeEventFromMessage` parses a NATS msg and **requires a non-empty primary
key** (else error).

### 3.2 Driver interfaces

- `IDriver`: `Stop`, `MaxBatchSize`, `Process(logger, event) → (flush bool, err)`,
  `Flush(logger)`, `Test(ctx, logger, url)`, `Configuration() → DriverField[]`,
  `Validate(values) → (url, FieldError[])`.
- Capability interfaces (duck-typed in Go via `interface{}` assertions →
  explicit optional interfaces in C#): `IDriverLifecycle.Start(DriverConfig)`,
  `IDriverMigration` (`MigrateNewTable`, `MigrateNewColumns`,
  `GetDestinationSchema`), `IDriverSessionHandler.SetSessionID`,
  `IDriverAlias.Aliases()`, `IDriverHelp` (`Name/Description/ExampleURL/Help`),
  `IImporter.Import(ImporterConfig)`, `IImporterHelp.SupportsDelete()`.
- Registry: scheme→driver and alias→scheme maps; `NewDriver` parses the URL
  scheme, resolves alias, and calls `Start` if the driver is an `IDriverLifecycle`.
  `Sentinel ErrDriverStopped` returned by Snowflake `Flush` after stop.
- `DriverField` (json: `name,type,format?,default?,description,required`),
  `FieldError` (json: `field`,`error`; **note `error`, not `message`**).
  `URLFromDatabaseConfiguration` / `NewDatabaseConfiguration` build standard
  DB config field sets and `scheme://user:pass@host:port/db` URLs.

### 3.3 Schema model

- `SchemaProperty` (json: `type,format?,nullable?,items?,additionalProperties?,$comment?,deprecated?`);
  `IsNotNull() = !Nullable || Type=="array"`; `IsArrayOrJSON() = Type∈{object,array}`.
- `Schema` (`Properties`, `Required[]`, `PrimaryKeys[]`, `Table`, `ModelVersion`).
  **`Columns()` ordering is load-bearing**: primary keys first (in PK order), then
  the remaining property names sorted ascending; cached. All SQL output depends on
  this order.
- `SchemaRegistry` interface: `GetLatestSchema`, `GetSchema(table,version)`,
  `GetTableVersion`, `SetTableVersion`, `Close`.
- `SchemaValidator` interface: `Validate(event) → (found, valid, path, err)`.
- `DatabaseSchema = map<table, map<column, sqlType>>` with `Columns`/`GetType`.
- `UpdateDestinationSchema`: on startup for migration-capable drivers, diff source
  schema vs destination and create/alter as needed.

---

## 4. Behavioral fidelity requirements (the things that are easy to get wrong)

These are the highest-risk parity items. Each MUST be preserved; each has golden
tests in the plan.

### 4.1 SQL value generation
EDS builds SQL by **string interpolation with custom value quoting** — no
parameter binding. The C# port must reproduce the quoting byte-for-byte per driver.
Key differences across drivers (full matrix in `research/drivers-sql.md`):
- `null` casing: `NULL` (mysql/sqlserver/snowflake) vs `null` (postgres).
- booleans: `1/0` (mysql/sqlserver) vs `true/false` (postgres/snowflake).
- floats: Go `'g'` (mysql/sqlserver) vs `'f'` (postgres/snowflake) — **Go shortest
  round-trip semantics; .NET default `ToString` differs**, must be matched and
  invariant-culture.
- identifier quoting: `` `x` `` (mysql), `[x]` (sqlserver), `"x"` (postgres/snowflake).
- string escaping: backslash table (mysql) vs SQL-standard `''` + backslash hybrid
  (sqlserver) vs `\\`+`''` (snowflake) vs dollar-quoting `$_H_$` (postgres).
- timestamp string coercion (mysql/sqlserver): strings matching the JSON-timestamp
  regex are reparsed/reformatted; **parse failure panics**; years <1970 clamp to
  `1970-01-01 00:00:01 UTC`.
- `ToJSONStringVal`: empty object/array → `'{}'`/`'[]'`; `quoteJSONScalar`'s
  **asymmetric regex** `^([+-]?([0-9]*[.])?[0-9]+)|(true|false)$` (start-anchored
  number OR end-anchored bool) must be ported verbatim.
- type mapping `propTypeToSQLType` per driver (full table in research doc).
- DDL: PK ordering, `NOT NULL` rule (`Required && !Nullable`), per-driver
  `CREATE`/`DROP`/`ALTER` shapes.

### 4.2 JSON serialization
Go `json.Marshal` **sorts map keys**, HTML-escapes `< > &` (→ `<` etc.),
honors `omitempty`, and emits struct fields in declaration order. The streaming
drivers and the schema-validator round-trip depend on this. The C# port uses
`System.Text.Json` with: a custom encoder matching Go's escaping where bytes
matter, **explicit key sorting** for map/dictionary output, declaration-order DTOs,
and raw pass-through of `before`/`after`.

### 4.3 Consumer batching state machine
The adaptive flush logic in `consumer.bufferer()` (research not separately
mapped — read firsthand from `internal/consumer/consumer.go`):
- defaults: `MinPendingLatency=2s`, `MaxPendingLatency=30s`,
  `EmptyBufferPauseTime=10ms`, `MaxAckPending=25_000`, `MaxPendingBuffer=4_096`.
- effective batch size = `min(MaxAckPending, driver.MaxBatchSize())` when the driver
  caps it (>0).
- **strict consumer-sequence ordering check** (`seq == lastSeq+1` else error/NAK).
- flush triggers (exact precedence): driver `Process` returns `flush=true`; OR
  `pending >= max`; OR forced-flush after migration; then in the time-based path:
  if `NumPending > max` and within `2×MaxPendingLatency`, keep accumulating; flush
  when `pending >= max` or `since(pendingStarted) >= MaxPendingLatency`; in the idle
  path flush when `0 < pending < max` and `since(pendingStarted) >= MinPendingLatency`.
- skip rules: per-table export timestamp (`event ts < table ts` → ack+skip), schema
  validator verdicts, and (non-DELETE) drop of properties not in the schema
  (`JSONDiff` + `OmitProperties`).
- migration-on-the-fly: detect table/version mismatch, fetch new schema, call
  `MigrateNewTable`/`MigrateNewColumns`, add new columns to `diff`, force a flush.
- heartbeats: msgpack to `eds.client.<sessionId>.heartbeat` every 1 min with
  `SystemStats`, uptime, offset, paused-at.
- JetStream consumer config: durable `eds-<serverId>[-<suffix>]`, `MaxDeliver=20`,
  `AckWait=5m`, `AckExplicit`, `InactiveThreshold=72h`, `MaxWaiting=1`,
  filter subjects `dbchange.*.*.<companyId>.*.PUBLIC.>`, deliver policy
  (all / by-start-time / new) chosen on first creation.

### 4.4 HTTP retry policy (`util.NewHTTPRetry`)
30s window; retry connection-reset/refused only within window; retry **unbounded**
on 408/429/502/503/504; jitter `100ms + rand(0, 500×attempts)ms`. Preserve the
unbounded-on-5xx behavior (document it). In C#, map socket errors by
`SocketError`, clone the request per attempt, use a loop not recursion.

### 4.5 Masking, hashing, partitioning
- `Mask(s)`: show first `len/2` bytes, replace the rest with `*`. `MaskURL`
  (sorted query, host verbatim), `MaskEmail`, `MaskArguments` (URL→email→JWT→passthrough).
- `Hash(...)` = lowercase 16-hex of XXH64 over `fmt.Sprintf("%+v", v)` of each arg
  (Go format reproduction required).
- `Modulo(value, n)` = `abs(int(FNV1a32(value)) % n)`.
- Kafka/EventHub partition keys & message keys exact formats (see research).

### 4.6 Wire formats & subjects
- NATS subjects: data `dbchange.*.*.<companyId>.*.PUBLIC.>`; control inbound
  `eds.notify.<sessionId>.>`; control outbound `eds.client.<sessionId>.<action>-<status|response>`;
  heartbeat `eds.client.<sessionId>.heartbeat`.
- Control-plane reply duality: `configure`/`import`(init)/`driverconfig`/`validate`
  reply via **request/reply JSON** (`m.Respond`); everything else **publishes
  msgpack** with `Nats-Msg-Id` + `content-encoding: msgpack`.
- DTO quirks to preserve on the wire: `ValidateResponse` JSON key misspelled
  `messsage`; `FieldError` JSON key `error`; `LogPath` never serialized; pointer
  vs value `omitempty` semantics.
- Inbound decode `DecodeNatsMsg`: `gzip/json`→gunzip+json; `msgpack`→msgpack→object
  →json→target; else raw json.
- Shopmonkey API endpoints: `GET /v3/eds/internal/enroll/<code>`,
  `POST /v3/eds/internal` (session start), `POST /v3/eds/internal/<sid>` (end),
  `POST /v3/eds/internal/<sid>/log` (log upload url), `GET/POST /v3/export/bulk[/id]`,
  `GET /v3/schema[/object/version]`. API base URL derived from JWT `iss` (legacy
  `https://shopmonkey.io` → `https://api.shopmonkey.cloud`); enroll-code first
  letter → environment URL (P/S/E/L).

### 4.7 Prometheus metrics (exact)
Names/help/buckets per `research/metrics-registry.md`. Notably the `SystemStats`
snapshot reports histogram **sample counts** (not sums) and `pendingEvents` is
**always 0** (Go gauge/counter-getter quirk) — must be reproduced.

### 4.8 Self-upgrade
`download` → PGP-verify (ProtonMail gopenpgp `crypto.Auto`) → extract (`.zip`→first
`.exe`; else tar.gz→entry named exactly `eds`) → `Apply` atomic swap
(`.<name>.new`/`.old` rename dance, hide-on-Windows, two-tier rollback error). On
Windows (the target host) the rename-aside trick and `FILE_ATTRIBUTE_HIDDEN` are
mandatory. Refused inside Docker.

---

## 5. Configuration, files, and state

- **Data dir** (`--data-dir`/`-d`, default `<cwd>/data`, mode 0700): holds
  `config.toml`, `eds-data.db` (tracker), per-session dirs (`<sessionId>/` with
  `nats.creds`, `logs/`, import temp dirs), and downloaded upgrade binaries.
- **`config.toml`** (written by `enroll`, read at startup): top-level keys `token`,
  `server_id`, plus runtime-written `url`, `keep_logs`. Read via viper → port with
  `Tomlyn` + `Microsoft.Extensions.Configuration`.
- **NATS creds**: base64 in session-start response → decoded to `nats.creds`
  (mode 0600); parsed for `CompanyIDs/ServerID/SessionID` (JWT inside creds).
- **Tracker keys**: `table-export` (JSON `TableExportInfo[]`),
  `registry:<table>-<version>` (schema cache), `registry:<table>:version`,
  `snowflake:<table>:<id>` (24h dedup). BuntDB (single-file, `EverySecond` sync) →
  C# embedded KV (LiteDB recommended; SQLite acceptable) with **TTL semantics**
  (lazy + active eviction) and prefix-scan delete.

---

## 6. Process model & exit-code contract (must match exactly)

Constants: `maxFailures=5`, `defaultMaxAckPending=25_000`,
`defaultMaxPendingBuffer=4_096`.

Exit codes (the wrapper↔fork protocol):
- `0` success / clean shutdown → remove session dir (unless `--keep-logs`).
- `1` generic failure → counts toward `maxFailures` (with required-flag detection
  to exit immediately).
- `3` `exitCodeIncorrectUsage` (bad flags / driver test failure).
- `4` `exitCodeRestart` (intentional restart; do not upload error logs).
- `5` `exitCodeNatsDisconnected` (retry after 5s).
- `2` panic exit (`RecoverPanic` → `Environment.Exit(2)`).

Wrapper loop: forks child with `--wrapper --parent=<port>`; localhost HTTP
`/restart` for upgrades; restarts child up to `maxFailures` with backoff
`failures × 1s`; on upgrade failure resets to original binary. Child (`fork`)
exposes localhost `/control/{pause,unpause,restart,shutdown,logfile}` and
`SIGHUP`/`SIGTERM` handling (on Windows, modeled via the HTTP control channel +
a cross-platform shutdown signal abstraction).

Session lifecycle: `sendStart` (→ creds + sessionId) → write creds → start
notification consumer → (first run) wait for `configure` → fork child with creds →
on exit upload logs (`sendEnd` + gzip log + PUT), renew session every 24h via
restart, hourly log upload.

---

## 7. External dependency mapping (pip)

Full rationale + version pins in [`package-map.md`](./package-map.md). Summary:

| Concern | Go | Python / pip |
|---|---|---|
| CLI framework | spf13/cobra | `argparse` (stdlib) — hand-rolled dispatcher, mirroring the .NET port (no heavyweight framework) |
| Config (TOML) | spf13/viper + BurntSushi/toml | `tomli` (read) + `tomli-w` (write) — 3.10 has no `tomllib` |
| Logging | go-common/logger | custom `eds.util.logger` over stdlib `logging` (console + JSON sink + multi + prefix) |
| NATS + JetStream | nats.go | `nats-py` (asyncio; JetStream + nkeys) |
| MessagePack | vmihailenco/msgpack | `msgpack` |
| Prometheus | client_golang | `prometheus-client` |
| KV store | tidwall/buntdb | `sqlite3` (stdlib) — ordinal/BINARY key ordering |
| JSON schema | santhosh-tekuri/jsonschema | `jsonschema` |
| PGP | ProtonMail/gopenpgp | `PGPy` (pure-Python; no external gpg binary) |
| xxhash | cespare/xxhash | `xxhash` |
| FNV-1a 32 | hash/fnv | hand-rolled |
| PostgreSQL | lib/pq | `psycopg[binary]` (psycopg 3) |
| MySQL | go-sql-driver/mysql | `PyMySQL` (pure-Python) |
| SQL Server | microsoft/go-mssqldb | `pyodbc` (MS ODBC Driver 18) — fallback `pymssql` |
| Snowflake | snowflakedb/gosnowflake | `snowflake-connector-python` |
| S3 | aws-sdk-go-v2 | `boto3` |
| Kafka | segmentio/kafka-go | `confluent-kafka` (librdkafka) |
| EventHub | azure azeventhubs | `azure-eventhub` |
| sysinfo / machine-id | gopsutil / machineid | `psutil` + platform reads + `hmac`/`hashlib` |
| gzip / tar / zip | compress/gzip, archive/* | `gzip`, `tarfile`, `zipfile` (stdlib) |
| JWT (unverified) | golang-jwt | manual base64url + `json` (stdlib); `PyJWT` where a verified path is needed |
| interactive forms | charmbracelet/huh | `questionary` (enroll/config TUI) |
| HTTP client | net/http | `requests` (sync CLI paths) / stdlib for retries |
| testing | testify / go-sqlmock | `pytest` + fakes/seams (`unittest.mock`) |
| containers (tests) | docker-compose | `testcontainers` (Docker-gated) |

---

## 8. Known Go quirks to preserve (do **not** silently "fix")

Faithfulness means reproducing these (each documented in code with a `// PARITY:`
comment and a referencing test). A separate `DEVIATIONS.md` will record any
deliberate, justified divergences (with rationale) for later review.

1. `quoteJSONScalar` regex asymmetric anchoring.
2. MySQL `toSQLFromObject` dead `updateValues` (REPLACE INTO ignores it).
3. SQL Server unchecked `object["id"]` cast; hybrid `''`+backslash escaping.
4. MySQL/SQL Server timestamp-string parse → **panic** on bad input; <1970 clamp.
5. Snowflake update-noise skip (`updatedDate`-only / `updatedDate`+`meta` / empty diff).
6. Snowflake timestamp-gated MERGE; tracker-gated force-delete-before-insert; 24h cache.
7. `SystemStats.pendingEvents` always 0; histogram fields are sample counts.
8. HTTP retry unbounded on 5xx/429.
9. S3 `MaxBatchSize()` hard 1000 (ignores configured); object key uses unix
   **seconds** (sub-second collisions); generic-endpoint bucket-parsing mismatch.
10. Kafka silent drop on 10s `Leader Not Available` flush timeout.
11. File driver Windows file-URI drive-letter loss & relative-path bug (the C#
    port will **correct** the Windows file-URI handling — see DEVIATIONS).
12. `importer.Run` sets `LocationID` from `companyId` (copy/paste bug) — preserve.
13. `download` zip path skips chmod; tar "not found" surfaces as EOF error.
14. `ValidateResponse` JSON key `messsage`; `FieldError` JSON key `error`.
15. `ToFileURI` per-OS leading-slash count.

---

## 9. Scope & phasing of Stage 1

Stage 1 = production data path + import + control plane + self-upgrade, byte-faithful.

**In scope:** all 8 drivers; consumer; import; tracker; registry; schema +
validator; notification control plane; server/fork/wrapper process model; enroll/
download/publickey/version commands; metrics/health HTTP; logging; session
lifecycle; masking/hashing/util.

**Deferred (Stage 1.x / later, explicitly out of the first functional milestone):**
the `e2e`/`integrationtest` dev harnesses (not production path) — replaced by C#
unit + integration tests. The `--schema-validator` path is implemented but its
template engine (`html/template`) is a thin custom renderer for the subset used.

**Definition of done (Stage 1):**
- Package imports clean on CPython 3.10 (64-bit); `ruff`/`mypy` clean.
- Unit tests pass (`pytest`), including **golden SQL output tests** for all DB drivers
  (Python output compared to captured Go output for INSERT/UPDATE/DELETE across every
  schema property type) and parity tests for hashing/masking/quoting/JSON.
- A driver-agnostic **local end-to-end** works against a local NATS + local
  destinations (File + PostgreSQL via Docker) using the same CLI surface.
- All §8 quirks have a `# PARITY:` marker and a covering test or documented note.

See [`PLAN.md`](./PLAN.md) for the ordered milestones and task breakdown.
