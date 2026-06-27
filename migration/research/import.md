I now have all the detail needed. Producing the behavioral specification.

# Behavioral Specification: `import` Subsystem (Bulk Import / Backfill Pipeline)

Source files:
- `D:/Users/kessler/source/Repos/edsGolang/cmd/import.go` — CLI command, export-job API, download, orchestration.
- `D:/Users/kessler/source/Repos/edsGolang/internal/importer.go` — `Importer`/`ImporterConfig`/`ImporterHelp` interfaces + registry.
- `D:/Users/kessler/source/Repos/edsGolang/internal/importer/importer.go` — the per-file event-replay engine (`Run` + `Handler`).

Supporting code read for fidelity: `cmd/root.go`, `internal/util/{util,http,json,hash,errors,api,schema}.go`, `internal/{dbchange,schema,driver,migration}.go`, `internal/tracker/tracker.go`, `internal/drivers/file/file.go`.

---

## 1. Purpose

The `import` subsystem performs a one-time bulk backfill of a Shopmonkey company's data into a destination configured by a driver URL. It (a) requests a server-side "bulk export" job from the Shopmonkey API, (b) polls until the job completes, (c) concurrently downloads the resulting gzipped NDJSON export files (one or more per table), (d) records the latest export timestamp per table into the local tracker, and (e) replays every row of every downloaded file as a synthetic `INSERT` `DBChangeEvent` into the driver's `Importer` implementation (which typically creates the schema/datasource first, then batch-inserts). It is the "seed the database from scratch" counterpart to the streaming consumer; after import, table schema versions are recorded so the streaming consumer can pick up incremental changes.

---

## 2. Public surface

### 2.1 `internal` package (`internal/importer.go`)

```go
type ImporterConfig struct {
    Context         context.Context   // (no tag — not serialized)
    URL             string
    Logger          logger.Logger
    SchemaRegistry  SchemaRegistry
    SchemaValidator SchemaValidator   // nil if not needed
    MaxParallel     int               // max tables imported in parallel (driver-dependent)
    JobID           string
    DataDir         string            // folder containing the downloaded data files
    DryRun          bool
    Tables          []string
    Single          bool              // one insert at a time vs batching
    SchemaOnly      bool
    NoDelete        bool
}

type Importer interface {
    Import(config ImporterConfig) error
}

type ImporterHelp interface {
    SupportsDelete() bool
}

func RegisterImporter(protocol string, importer Importer)
func NewImporter(ctx context.Context, logger logger.Logger, urlString string, registry SchemaRegistry) (Importer, error)
```

Package-private registries: `importerRegistry map[string]Importer`, `importerAliasRegistry map[string]string`. `RegisterImporter` also registers aliases if the importer implements `DriverAlias` (`Aliases() []string`).

### 2.2 `importer` package (`internal/importer/importer.go`)

```go
type Handler interface {
    CreateDatasource(schema internal.SchemaMap) error
    ImportEvent(event internal.DBChangeEvent, schema *internal.Schema) error
    ImportCompleted() error
}

func Run(logger logger.Logger, config internal.ImporterConfig, handler Handler) error
```

### 2.3 `cmd` package (`cmd/import.go`) — all are package-private but define the wire formats and CLI surface that must be reproduced

Types:
```go
type apiResponse[T any] struct {
    Success bool   `json:"success"`
    Message string `json:"message"`
    Data    T      `json:"data"`
}

type exportJobCreateResponse struct { JobID string `json:"jobId"` }

type exportJobCreateRequest struct {
    TimeOffset  *int64   `json:"timeOffset,omitempty"`   // unix-milli
    CompanyIDs  []string `json:"companyIds,omitempty"`
    LocationIDs []string `json:"locationIds,omitempty"`
    Tables      []string `json:"tables,omitempty"`
}

type errorResponse struct { Message string `json:"message"` }

type exportJobTableData struct {
    Error  string   `json:"error"`
    Status string   `json:"status"`   // "Pending" | "Completed" | "Failed"
    URLs   []string `json:"urls"`
    Cursor string   `json:"cursor"`   // nanosecond timestamp string
}

type exportJobResponse struct {
    Completed bool                          `json:"completed"`
    Tables    map[string]exportJobTableData `json:"tables"`   // key = table name
}

type TableExportInfo struct {   // NOTE: no json tags → default Go marshalling: "Table","Timestamp"
    Table     string
    Timestamp time.Time
}

const trackerTableExportKey = "table-export"
```

Functions:
```go
func decodeAPIResponse[T any](resp *http.Response) (*T, error)
func (e *errorResponse) Parse(buf []byte, statusCode int, context string, requestId string) error
func handleAPIError(resp *http.Response, context string) error
func createExportJob(ctx, logger, apiURL, apiKey string, filters exportJobCreateRequest) (string, error)
func (e *exportJobResponse) GetProgress() float64
func (e *exportJobResponse) String() string
func checkExportJob(ctx, logger, apiURL, apiKey, jobID string) (*exportJobResponse, error)
func pollUntilComplete(ctx, logger, apiURL, apiKey, jobID string) (exportJobResponse, error)
func downloadFile(log logger.Logger, dir string, parsedURL *url.URL) (int64, error)
func bulkDownloadData(log logger.Logger, data map[string]exportJobTableData, dir string) ([]TableExportInfo, error)
func isCancelled(ctx context.Context) bool
func tableNames(tableData []TableExportInfo) []string
```

CLI flags (registered in `init()` on `importCmd`, `Use: "import"`, `Args: cobra.NoArgs`):

| Flag | Type | Default | Notes |
|---|---|---|---|
| `--url` | string | `""` | driver connection string; **required** (`mustFlagString(...,true)`) |
| `--api-key` | string | `os.Getenv("SM_APIKEY")` | **required** |
| `--job-id` | string | `""` | resume an existing export job |
| `--dry-run` | bool | `false` | simulate only |
| `--no-confirm` | bool | `false` | skip the delete confirmation prompt |
| `--no-cleanup` | bool | `false` | keep the temp dir |
| `--no-delete` | bool | `false` | skip dropping/recreating tables |
| `--dir` | string | `""` | reuse an existing import directory (skip download) |
| `--schema-only` | bool | `false` | create schema only, skip data |
| `--validate-only` | bool | `false` | test driver connection only, then exit 0 |
| `--timeOffset` | string | `""` | RFC3339 timestamp; export records updated after this time |
| `--parallel` | int | `4` | parallel upload tasks (driver-dependent) |
| `--single` | bool | `false` | one insert at a time |
| `--only` | []string | `nil` | restrict to these tables |
| `--companyIds` | []string | `nil` | restrict to these company ids |
| `--locationIds` | []string | `nil` | restrict to these location ids |
| `--api-url` | string | `"https://api.shopmonkey.cloud"` | hidden; overrides JWT-derived API url |

Persistent (root) flags also consumed: `--data-dir/-d` (default `<cwd>/data`), `--verbose/-v`, `--silent/-s`, `--timestamp/-t`, `--log-label`, `--schema-validator`.

---

## 3. Behavior & algorithms

### 3.1 Top-level command flow (`importCmd.Run`)

1. Build logger via `newLogger(cmd)` then `.WithPrefix("[import]")`.
2. Read flags. Required (exit code **3** if missing): `--url`, `--api-url`, `--api-key`. `mustFlagInt("parallel", false)` is read but not required.
3. `--timeOffset`: if non-empty, parse with `time.Parse(time.RFC3339, …)`; on error `logger.Fatal`. Store `tv.UnixMilli()` into `*int64 timeOffsetUnixMilli`.
4. `defer util.RecoverPanic(logger)` (the outermost recover).
5. `dataDir := getDataDir(cmd, logger)` — abs+clean of `--data-dir`; if missing, verify parent writable then `os.MkdirAll(dataDir, 0700)`; else verify writable. Fatal on failure. (Note: this is the **state** dir, distinct from the per-job download `dir`.)
6. If `--dry-run`, log `"🚨 Dry run enabled"`.
7. Create `ctx, cancel := context.WithCancel(Background())`. Spawn goroutine: on `sys.CreateShutdownChannel()` (SIGINT/SIGTERM) call `cancel()`. Goroutine wraps `defer util.RecoverPanic`.
8. Read `--only`, `--companyIds`, `--locationIds` string slices.
9. **API URL resolution**: if `--api-url` was *explicitly changed*, log "using alternative API url" and keep it. Otherwise call `util.GetAPIURLFromJWT(apiKey)` (parses JWT unverified, reads `iss` claim; legacy `https://shopmonkey.io` → `https://api.shopmonkey.cloud`); fatal on error; assign result to `apiURL`.
10. Create tracker: `tracker.NewTracker({Context, Logger, Dir: dataDir})`; fatal on error; `defer theTracker.Close()`.
11. `loadSchemaValidator(cmd)`: reads `--schema-validator`; if empty returns `(nil,nil)`; else `util.NewSchemaValidator(dir)`. Fatal on error.
12. Create registry: `registry.NewAPIRegistry(ctx, logger, apiURL, Version, theTracker)` (loads schema fresh from API); fatal on error; `defer registry.Close()`.
13. **Driver connection test**: `internal.NewDriverForImport(ctx, logger, driverUrl, registry, theTracker, dataDir)`. On error: print error to stdout and `os.Exit(3)`. Then `driver.Test(timedCtx, logger, driverUrl)` with a **15-second** timeout context; on error print + `os.Exit(3)`. (Exit 3 == "test failed".) Driver is intentionally *not* stopped.
14. If `--validate-only`: `os.Exit(0)` here.
15. Create importer: `internal.NewImporter(ctx, logger, driverUrl, registry)`; fatal on error.
16. **Delete-confirm gate**: `skipDeleteConfirm = false`; if importer implements `ImporterHelp`, `skipDeleteConfirm = !importerHelp.SupportsDelete()`. The interactive warning is shown only if **all** of: `!dryRun && !noconfirm && !skipDeleteConfirm && !schemaOnly && !noDelete`. The prompt uses `huh` form: note title `"\n🚨 WARNING 🚨"`, confirm title `"YOU ARE ABOUT TO DELETE EVERYTHING IN <driver.Name>"`, affirmative `"Confirm"`, negative `"Cancel"`. `meta` comes from `internal.GetDriverMetadataForURL(driverUrl)`. If `form.Run()` errors and it's *not* `huh.ErrUserAborted`: log error, hint "You may use --confirm to skip this prompt", `os.Exit(1)`. If user did not confirm (`!confirmed`), `os.Exit(0)`.
17. Determine `noCleanup`: from `--no-cleanup`, but **forced `true` if `--dir` was provided** (don't delete a user-provided dir).
18. Register the deferred completion handler (see 3.2).
19. **Data acquisition branch** (sets `tables []string` and `tableExportInfo []TableExportInfo`):
    - **`dir == ""` and not `schemaOnly`** (normal path):
      - If `jobID == ""`: log "Requesting Export...", call `createExportJob` with `{Tables: only, CompanyIDs, LocationIDs, TimeOffset}`; fatal on error.
      - Log "Waiting for Export to Complete..."; `pollUntilComplete`. If error and not cancelled → fatal. If cancelled → `return`.
      - Create temp dir: `os.MkdirTemp(dataDir, "import-"+jobID+"-*")`; fatal on error. This becomes `dir`.
      - Log "Downloading export data..."; `bulkDownloadData(logger, job.Tables, dir)`; fatal on error. If cancelled afterward → `return`.
      - `tableExportInfo = tableData`; `tables = tableNames(tableData)`.
    - **`dir == ""` and `schemaOnly`**: skip download. `registry.GetLatestSchema()`; for each schema entry build `TableExportInfo{Table, Timestamp: time.Now()}` (single `time.Now()` captured once). Set `tables`/`tableExportInfo` from it.
    - **`dir != ""`** (reuse): `loadTableExportInfo(theTracker)` reads tracker key `"table-export"` and JSON-unmarshals to `[]TableExportInfo`. If present → use it. If absent → list `dir` via `util.ListDir`, parse each filename with `util.ParseCRDBExportFile`; collect distinct table names that parse successfully.
20. **`--only` filter** (applied again, post-acquisition): if `len(only) > 0`, keep only tables in `only`.
21. Log `"Importing data to tables <comma-joined>"`.
22. Call `importer.Import(ImporterConfig{...})` with all fields populated from flags/state (note `DataDir: dir`, the per-job download dir, NOT `dataDir`). On error: `logger.Error("error running import: %s", err)` then **`return`** (does not set `success`, so deferred handler exits 1).
23. **Schema version recording**: if `driver` implements `internal.DriverMigration`, get `registry.GetLatestSchema()`; for each `info` in `tableExportInfo` whose `info.Table` is in `tables`, call `registry.SetTableVersion(info.Table, latest[info.Table].ModelVersion)`. Log errors and `return` on any failure. If driver does not support migration, log trace and skip.
24. Set `success = true`; log `"👋 Loaded %d tables in %v"`.

### 3.2 Deferred completion / cleanup handler

Registered at step 18 (two defers; LIFO ordering is deliberate — the comment "panic recover needs to happen after the defer above" means an inner `defer util.RecoverPanic(logger)` is registered *after* the cleanup func so it unwinds *first* on panic). Cleanup logic:
- Logs `"exit success: %v"`.
- If `success`:
  - If `!noCleanup`: `os.RemoveAll(dir)`, mark `filesRemoved = true`.
  - Persist tracker: `theTracker.SetKey(trackerTableExportKey, util.JSONStringify(tableExportInfo), 0)` (TTL 0 = no expiry). Log error on failure (non-fatal).
  - `theTracker.Close()`.
- If files were not removed and `dir != ""`: log `"downloaded files saved to: <dir>"`.
- If `!success`: `os.Exit(1)`.

### 3.3 Export-job API calls

All requests set headers via `setHTTPHeader`: `Content-Type: application/json`, `User-Agent: "Shopmonkey EDS Server/"+Version`, and `Authorization: Bearer <apiKey>` (if key non-empty). All use `util.NewHTTPRetry(req, util.WithLogger(logger)).Do()`.

- **`createExportJob`**: `POST <apiURL>/v3/export/bulk` with JSON body = the `exportJobCreateRequest`. Non-200 → `handleAPIError(resp, "import")`. Else `decodeAPIResponse[exportJobCreateResponse]` → return `JobID`.
- **`checkExportJob`**: `GET <apiURL>/v3/export/bulk/<jobID>`. If request-creation error is `context.Canceled` → return `(nil, nil)`. Non-200 → `handleAPIError`. Decode `exportJobResponse`. Then iterate `job.Tables`: if any `Status == "Failed"`, return `(job, error("error exporting table <t>: <data.Error>"))`.
- **`decodeAPIResponse[T]`**: JSON-decode body into `apiResponse[T]`; if `!Success` → `error("api error: <Message>")`; else return `&Data`.
- **`errorResponse.Parse`**: builds optional `requestIdTag = "(requestId=<id>)"` (id from `X-Request-Id` header via `getRequestID`); if body unmarshals to `{message}` → `"<context>: <message> <tag>"`; else `"<context>: <rawbody> (status code=<code>) <tag>"`.

### 3.4 `pollUntilComplete`

Loop:
- Throttled logging: print `"Checking for Export Status (<jobID>)"` at Info level when `lastPrinted` is zero or older than **1 minute**; update `lastPrinted`; set `showProgress=true`.
- `checkExportJob`. On error → return `({}, err)`. If `job == nil` → return `({}, nil)` (cancelled).
- If `job.Completed` → log `"Export Progress: <String()>"` and return `(*job, nil)`.
- Else Debug `"Waiting for Export to Complete: <String()>"`; if `showProgress`, Info `"Export Progress: <String()>"`.
- `select { <-ctx.Done(): return ({},nil); <-time.After(5*time.Second): }` → **5-second** poll interval.

`exportJobResponse.String()`: counts `Pending`/`Completed`/`Failed`; `percent = 100*completed/len(Tables)` only if `completed>0` else 0; returns `"%d/%d (%.2f%%)"` (completed / total / percent). `GetProgress()` returns `completed/total` as a fraction in [0,1] (0 if total==0) — defined but **not used** in this command.

### 3.5 `bulkDownloadData`

For each `(table, tableData)` in the `Tables` map (Go map iteration order — nondeterministic, but only affects `tableExportInfo` ordering which is later JSON-stored):
- **No URLs** (`len(tableData.URLs)==0`): parse `tableData.Cursor` as int64 (`strconv.ParseInt(..,10,64)`); error → fatal-style `error("error parsing timestamp value: <cursor>. <err>")`. Record `TableExportInfo{Table, Timestamp: time.UnixMicro(tv/1000)}`. **The cursor is a nanosecond value**: divide by 1000 → microseconds → `UnixMicro`. Continue (no files for this table).
- **Has URLs**: for each URL string, `url.Parse`; then `util.ParseCRDBExportFile(parsedURL.Path)` → `(table, timestamp, ok)`; if `!ok` → `error("unrecognized file path: <base>")`. Track the max `timestamp` (`finalTimestamp`) across the table's files. Append parsed URL to `downloads`. After the loop, record `TableExportInfo{Table, Timestamp: finalTimestamp}`.
- If `downloads` empty → Debug "no files to download", return `(nil,nil)`.
- **Concurrent download**: `concurrency := 10`. Buffered channel `downloadChan` of size `len(downloads)`. Spawn 10 worker goroutines (each `defer util.RecoverPanic(log)` + `defer wg.Done()`), each pulling URLs and calling `downloadFile(log, dir, url)`. On per-file error: push to buffered `errors` channel (cap = concurrency=10) and `return` (worker stops). Track `downloadBytes` via `atomic.AddInt64` and `completed` via `atomic.AddInt32`; Debug-log `"download completed: %d/%d (%.2f%%)"`.
- Push all URLs, `close(downloadChan)`, `wg.Wait()`.
- Non-blocking `select` on `errors`: if any error present → return it (only the first observed). Else log Info `"Downloaded %d files (%d bytes) in %v"` and return `tables`.

### 3.6 `downloadFile`

- `baseFileName = filepath.Base(parsedURL.Path)`.
- Plain `http.Get(parsedURL.String())` — **no auth header, no retry** (the URL is a pre-signed download link). Non-200 → Trace-log body, return `error("error fetching data: <status>")`.
- Create `<dir>/<baseFileName>`, `io.Copy` body → file. Returns bytes written. Debug-log `"downloaded file %s (%d bytes)"`.

### 3.7 `importer.Run` (the replay engine)

1. `started := time.Now()`. `schema = config.SchemaRegistry.GetLatestSchema()`; error → wrap "unable to get schema".
2. If `!config.NoDelete`: `handler.CreateDatasource(schema)`; propagate error. (So `--no-delete` skips datasource creation entirely.)
3. If `config.SchemaOnly`: return `nil` immediately (schema/datasource already created above; no data replay).
4. `files := util.ListDir(config.DataDir)` — recursive, skips `.DS_Store`, returns full paths.
5. For each `file`:
   - `table, tv, ok := util.ParseCRDBExportFile(file)`. If `!ok` → Debug "skipping file" and continue.
   - If `!util.SliceContains(config.Tables, table)` → continue (silent skip).
   - `data := schema[table]`; if `nil` → `error("unexpected table (%s) not found in schema but in import directory: %s")`.
   - `dec := util.NewNDJSONDecoder(file)` (opens file; if `.gz` extension wraps in `gzip.Reader`; then `json.NewDecoder`). `defer dec.Close()` (note: deferred per-iteration but Run is one call — also explicitly closed at end of loop body, so double-close; `Close` is idempotent/nil-guarded).
   - Loop while `dec.More()`:
     - Construct synthetic event:
       - `Operation = "INSERT"`
       - `Table = table`
       - `Timestamp = tv.UnixMilli()`
       - `MVCCTimestamp = fmt.Sprintf("%v", tv.UnixNano())` (decimal nanoseconds as a string)
       - `ID = util.Hash(filepath.Base(file))` — **xxhash of the filename, hex-formatted; identical for every row in the same file**
       - `ModelVersion = schema[table].ModelVersion`
       - `dec.Decode(&event.After)` decodes one JSON line into `After` (`json.RawMessage`); error → wrap "unable to decode JSON".
     - `event.Key = []string{event.GetPrimaryKey()}` — `GetPrimaryKey` returns last element of `Key` if present, else reads `After`/`Before` object's `"id"` string field, else `""`.
     - `o := event.GetObject()` (lazily unmarshals `After` into `map[string]any`, cached).
     - Owner-id extraction from `o`:
       - `o["locationId"]` (string) → `event.LocationID`
       - `o["companyId"]` (string) → **`event.LocationID`** ⚠️ (see gotchas — this overwrites LocationID, and CompanyID is never set)
       - `o["userId"]` (string) → `event.UserID`
     - `event.Imported = true`.
     - **Schema validation** (only if `config.SchemaValidator != nil`):
       - `found, valid, path, err := Validate(event)`.
       - If `errors.Is(err, util.ErrSchemaValidation)`: Debug-log a single-line joined validation message and `continue` (skip row).
       - If `err != nil` (non-validation): wrap "error validating schema" and return.
       - If `!found`: Trace "no schema found", `continue`.
       - If `!valid`: Trace "schema did not validate", `continue`.
       - If `path != ""`: set `event.SchemaValidatedPath = &path`, Trace "schema validated <path>".
     - `count++`; `handler.ImportEvent(event, data)`; propagate error.
   - After the row loop: `dec.Close()` (propagate error). `total += count`; Debug `"imported %d %s records in %s"`.
6. `handler.ImportCompleted()`; propagate error.
7. Info `"imported %d records from %d files in %s"`.

### 3.8 CRDB export filename parsing (`util.ParseCRDBExportFile`)

Regex (operates on `filepath.Base`):
```
^(\d{33})-\w+-[\w-]+-([a-z0-9_]+)-(\w+)\.ndjson\.gz
```
- Group 1 = 33-digit precise date; Group 2 = **table name**; Group 3 = schema-id. Format reference: CockroachDB changefeed `…/[timestamp]-[uniquer]-[topic]-[schema-id]`.
- `parsePreciseDate`: takes the 33-char string; builds `dateStr[:14] + "." + dateStr[14:23]` and parses with Go layout `"20060102150405.999999999"`. So **first 14 chars = `YYYYMMDDHHMMSS`, next 9 = fractional seconds (nanoseconds)**; the final 10 chars are ignored. Returns `(table, timestamp, true)`; any parse failure → `("", zero, false)`.

### 3.9 HTTP retry (`util.HTTPRetry`)

- `defaultTimeout = 30s`. Retries (recursively, incrementing `attempts`) when:
  - transport error message contains `"connection reset"` or `"connection refused"` AND still within `started+timeout`; OR
  - response status is one of: `408 RequestTimeout`, `502 BadGateway`, `503 ServiceUnavailable`, `504 GatewayTimeout`, `429 TooManyRequests` (drains+closes body first; always retries these regardless of elapsed time).
- Backoff jitter: `100ms + rand.Int63n(500*attempts) ms`, then `time.Sleep`, then recurse. Uses `http.DefaultClient`.

---

## 4. External dependencies

| Go package | Role | .NET / C# equivalent |
|---|---|---|
| `github.com/spf13/cobra` | CLI command + flag parsing | `System.CommandLine`, or `Spectre.Console.Cli` |
| `github.com/spf13/viper` (root) | config file (`config.toml`) | `Microsoft.Extensions.Configuration` (+ TOML provider, e.g. `Tomlyn`) |
| `github.com/charmbracelet/huh` | interactive confirm form (delete warning) | `Spectre.Console` `AnsiConsole.Confirm`, or a plain `Console.ReadLine` prompt |
| `net/http` | HTTP client (export API + file download) | `System.Net.Http.HttpClient` (one shared static instance) |
| `encoding/json` | API + NDJSON + tracker (de)serialization | `System.Text.Json` (`JsonSerializer`, `Utf8JsonReader`/`JsonDocument` for streaming) |
| `compress/gzip` | decompress `.ndjson.gz` exports | `System.IO.Compression.GZipStream` |
| `github.com/golang-jwt/jwt/v5` | parse JWT (unverified) to read `iss` → API URL | `System.IdentityModel.Tokens.Jwt` (`JwtSecurityTokenHandler.ReadJwtToken`) — no signature validation |
| `github.com/cespare/xxhash/v2` + `savsgio/gotils/strconv` | `util.Hash` (event ID from filename) | `System.IO.Hashing.XxHash64` (NuGet `System.IO.Hashing`); format as lowercase hex |
| `github.com/santhosh-tekuri/jsonschema/v5` | optional schema validation of events | `JsonSchema.Net` (NuGet `JsonSchema.Net`) or `NJsonSchema` |
| `html/template` (in schema validator) | render `path` template from event object | `Scriban` / `Handlebars.Net`, or `string.Format`-based templating (note Go template syntax `{{.field}}`) |
| `github.com/tidwall/buntdb` (via `internal/tracker`) | embedded KV store for `table-export` key + table versions | `LiteDB`, SQLite (`Microsoft.Data.Sqlite`), or a simple JSON file with locking |
| `github.com/cockroachdb/errors` (in `RecoverPanic`) | stack-augmented panic errors | `System.Exception` + `Environment.StackTrace` |
| `github.com/shopmonkeyus/go-common/logger` | leveled logger (Trace/Debug/Info/Error/Fatal) | `Microsoft.Extensions.Logging.ILogger` (define a `Fatal` that logs + `Environment.Exit`) |
| `github.com/shopmonkeyus/go-common/sys` | `CreateShutdownChannel()` (SIGINT/SIGTERM) | `Console.CancelKeyPress` / `PosixSignalRegistration` / `CancellationToken` from host |
| `sync`, `sync/atomic`, goroutines | concurrent download workers + counters | `Task` + `Channel<T>` / `Parallel.ForEachAsync`, `Interlocked` |

---

## 5. Edge cases & gotchas

1. **CompanyID bug (must decide: faithful vs fixed).** In `importer.Run`, the `companyId` branch assigns to `event.LocationID`, not `CompanyID`:
   ```go
   if id, ok := o["companyId"].(string); ok { event.LocationID = &id }
   ```
   So after import, `event.CompanyID` is always `nil`, and `LocationID` ends up holding `companyId` if both keys exist (the company branch runs after and overwrites the location branch). A *faithful* port must reproduce this exactly; if you intend to fix it, flag the divergence explicitly. The streaming consumer does set these correctly elsewhere, so this is import-specific behavior.
2. **Event `ID` is the same for every row in a file** (`Hash(base(file))`). Downstream drivers must not rely on `ID` uniqueness during import. The real primary key comes from `Key`/`GetPrimaryKey()` (the row's `id` field).
3. **`time.UnixMicro(tv/1000)`** in the empty-URL branch: `Cursor` is nanoseconds; the `/1000` converts to microseconds before `UnixMicro`. Replicate the integer division (truncation) precisely; do not pass nanoseconds to a micro/milli constructor.
4. **Two timestamp precisions per event**: `Timestamp = UnixMilli` (int64), `MVCCTimestamp = UnixNano as decimal string`. Both derive from the filename's 23-significant-digit timestamp (last 10 of 33 digits dropped), so nanosecond precision is effectively truncated to what the layout `.999999999` preserves.
5. **`--no-delete` skips `CreateDatasource`** entirely (not just the drop) — tables must already exist. **`--schema-only`** runs `CreateDatasource` then returns before any file is read; the command separately pre-builds `TableExportInfo` from the registry's latest schema using a single `time.Now()`.
6. **`--dir` forces `noCleanup=true`** so a user-supplied directory is never deleted. With `--dir`, table list comes from the tracker's saved `table-export` key if present, else from parsing filenames in the dir.
7. **Exit codes are semantically meaningful**: `3` = invalid/missing flag or driver connection/test failure; `2` = panic (from `RecoverPanic`); `1` = generic failure / import error / form error; `0` = success, validate-only, or user-cancelled confirm. The deferred handler calls `os.Exit(1)` whenever `success` never became true.
8. **Map iteration nondeterminism**: `bulkDownloadData` and the schema-only loop iterate Go maps; ordering of `tableExportInfo`/`tables` is not stable. Downstream logic is order-independent, but the JSON persisted under `table-export` will vary in element order between runs. Don't assume a stable order in tests.
9. **Download errors swallow all but the first**: the `errors` channel (cap 10) collects worker errors but only the first is returned via a non-blocking `select`; remaining errors are discarded. A worker that errors stops consuming, so other queued URLs may be left undownloaded — but `wg.Wait()` still completes because each worker's `for range downloadChan` exits when the channel is drained/closed; a returning worker leaves URLs for other workers. Net effect: import proceeds to error out via the returned first error.
10. **`downloadFile` uses bare `http.Get`** (no retry, no auth) since URLs are pre-signed. Don't add the bearer header — it could break signed-URL validation.
11. **Concurrency constants are fixed**: download `concurrency = 10` is hard-coded (independent of `--parallel`, which is passed to the driver as `MaxParallel`). Poll interval 5s, status reprint throttle 1 minute, driver test timeout 15s, HTTP default timeout 30s.
12. **Panic handling**: `util.RecoverPanic` logs all goroutine stacks and `os.Exit(2)` — it does NOT allow normal deferred cleanup (e.g., the success/cleanup defer) to run after recovery. Download workers each recover independently (a worker panic exits the whole process with code 2).
13. **`dec.Close()` is called twice** per file (deferred + explicit). `ndjsonReader.Close` nil-guards `gr`/`in`, so the second call is a no-op. The `defer dec.Close()` inside the loop accumulates one deferred close per file until `Run` returns — acceptable here, but a C# `using`-per-file is cleaner and avoids accumulation.
14. **`TableExportInfo` has no JSON tags**: serialized as `{"Table":...,"Timestamp":...}` with Go's default capitalized field names and RFC3339-ish `time.Time` JSON. The C# DTO must match these exact property names and time format for tracker round-tripping (or you control both ends and can standardize — but be consistent).
15. **`--only` is applied twice** (server-side via export request `Tables`, and client-side post-acquisition). With `--dir` the server filter never happened, so the client-side filter is what enforces it.
16. **`schema[table]` nil check**: a file whose table isn't in the registry schema is a hard error (`unexpected table … not found in schema`), but only after it passed the `Tables` membership check; files for tables not in `config.Tables` are silently skipped earlier.
17. **Validation skip logging**: schema-validation failures are logged at Debug with the multi-line validator error flattened to one line via `strings.TrimSpace(strings.Join(strings.Split(err.Error(),"\n")," "))`; the row is skipped (not fatal).

---

## 6. C# port notes

- **Command/flags**: model `importCmd` with `System.CommandLine`. Reproduce required-flag semantics: missing `--url`/`--api-url`/`--api-key` → exit 3. Keep `--api-url` hidden with default `https://api.shopmonkey.cloud`. Map `SM_APIKEY` env default for `--api-key`. Preserve `mustFlagBool`'s "only honored if explicitly set" nuance (cobra `Changed`).
- **Interfaces**: define `IImporter { Task Import(ImporterConfig cfg); }`, `IImporterHelp { bool SupportsDelete(); }`, `IImportHandler { Task CreateDatasource(SchemaMap); Task ImportEvent(DBChangeEvent, Schema); Task ImportCompleted(); }`. The Go pattern of registering the same driver instance as both `Driver` and `Importer` maps cleanly to a single class implementing both `IDriver` and `IImporter` registered in a `Dictionary<string,...>` keyed by URL scheme, with an alias map. Reproduce `NewImporter`'s error string listing supported protocols.
- **Replay engine** (`Run`): implement as a static method taking `ImporterConfig` + `IImportHandler`. Stream NDJSON with `StreamReader.ReadLine()` over a `GZipStream` (when `.gz`) and parse each line with `JsonSerializer`/`JsonDocument`; this matches Go's line-oriented `json.Decoder.More()`/`Decode`. Keep the synthetic-event field assignments byte-for-byte (operation `"INSERT"`, `Timestamp = UnixMilli`, `MVCCTimestamp = UnixNano.ToString()`, `ID = xxhash64(hex)` of the base filename, `ModelVersion` from schema). **Decide explicitly** whether to keep the companyId→LocationID bug; document the choice.
- **xxhash**: use `System.IO.Hashing.XxHash64`. Go's `Hash` writes `fmt.Sprintf("%+v", v)` of each arg then `fmt.Sprintf("%x", sum)`. For a single string filename, `%+v` is just the string; format the 64-bit digest as lowercase hex with no leading-zero padding beyond natural (`"%x"` of the 8-byte big-endian `Sum(nil)` — confirm endianness against `xxhash.Sum64` byte order to match exactly; `h.Sum(nil)` is big-endian of `Sum64()`).
- **HTTP retry**: implement a helper mirroring `HTTPRetry` (status set {408,502,503,504,429} always retry; transport "connection reset"/"refused" retry within 30s window; jitter `100ms + rand(0..500*attempt)ms`). Use a single static `HttpClient`. For the export API set `Content-Type`, `User-Agent: "Shopmonkey EDS Server/<Version>"`, `Authorization: Bearer <key>`. For file downloads use a plain GET (no auth/retry).
- **Concurrency**: replace the 10-worker goroutine pool with `Channel<Uri>` + N `Task`s or `Parallel.ForEachAsync(downloads, new ParallelOptions{MaxDegreeOfParallelism=10}, …)`. Use `Interlocked.Add` for byte/count totals. Preserve "first error wins" behavior or improve with `AggregateException` (document the divergence if you change it).
- **Generics**: `apiResponse[T]` → a generic `ApiResponse<T>` record with `success`/`message`/`data` JSON property names. `decodeAPIResponse` → deserialize then throw on `!success` with message `"api error: <msg>"`.
- **Filename regex**: port `^(\d{33})-\w+-[\w-]+-([a-z0-9_]+)-(\w+)\.ndjson\.gz` verbatim (`System.Text.RegularExpressions`, `RegexOptions.Compiled`). Reproduce `parsePreciseDate`: take chars [0..14) as `yyyyMMddHHmmss`, append `.` + chars [14..23), parse with `DateTime.ParseExact(s, "yyyyMMddHHmmss.fffffffff", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal|AdjustToUniversal)` — note .NET only supports 7 fractional digits (`fffffff`), so you must truncate the 9-digit fraction to 7 and accept the minor precision loss, or parse the fraction manually and add ticks. Recommend manual parse to avoid silent rounding differences vs Go.
- **Cursor timestamp**: `DateTimeOffset.FromUnixTimeMilliseconds(nanos/1_000/1_000)` is wrong — match Go: `nanos/1000` = micros, then construct from micros (`DateTimeOffset.FromUnixTimeMilliseconds(micros/1000)` loses precision; better build ticks: `new DateTime(1970-epoch + micros*10)` since 1 micro = 10 ticks). Be careful with the integer division order: divide nanos by 1000 first (truncating), then treat as micros.
- **Tracker**: `table-export` key stores `JsonSerializer.Serialize(List<TableExportInfo>)` with TTL 0 (no expiry). Match the no-json-tag Go field names (`Table`, `Timestamp`) or own both serialization ends. On the `--dir` reuse path, read this key first, fall back to directory scan.
- **Exit codes / shutdown**: centralize a `Fatal(msg)` helper = log + `Environment.Exit(3)` for flag/setup errors; map panic-equivalent (unhandled exception) to exit 2 via a top-level handler that also dumps thread/stack info; success path exit 0; failure path exit 1. Wire Ctrl-C via `CancellationTokenSource` (`Console.CancelKeyPress` or `PosixSignalRegistration`) and thread the token through all async API/download calls so cancellation returns cleanly (mirroring the `isCancelled` checkpoints after poll and after download).
- **Risks to watch**: (a) the precise-date 7-vs-9 fractional-digit limitation in .NET; (b) xxhash endianness/format; (c) preserving the deliberate companyId bug; (d) `time.Time` JSON format compatibility in the tracker; (e) ensuring `--schema-only` and `--no-delete` short-circuits land exactly where Go returns; (f) driver-test 15s timeout and exit-3 contract used by external automation.