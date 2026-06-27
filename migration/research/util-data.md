I have all assigned files plus the supporting internal types they depend on. Here is the behavioral specification.

---

# Behavioral Specification — `util-data` subsystem (`internal/util`)

## 1. Purpose

This subsystem is the shared data/utility layer for the Shopmonkey EDS consumer. It provides the pieces every database driver and the import/export pipeline depend on: a JSON-Schema validator that gates incoming change events per table (and computes a routing "path" via a template), a builder that reflects a destination database's `information_schema` into an in-memory `DatabaseSchema`, SQL helper functions (quoting, dry-run execution, the critical `ToJSONStringVal` value-coercion used when writing JSON/array columns), JSON helpers (NDJSON/gzip stream decoder, `JSONStringify`, `JSONDiff`), a record "optimize" pass that collapses multiple events for the same primary key, an in-memory TTL cache, a batcher accumulating change events into `Record`s, file/zip utilities, terminal help formatting, and a panic recovery handler. It does not own business logic; it shapes the data and side effects that the driver layer consumes.

---

## 2. Public surface

All symbols below are in package `util` (path `github.com/shopmonkeyus/eds/internal/util`) unless noted. Cross-package types from `github.com/shopmonkeyus/eds/internal` are listed where load-bearing.

### 2.1 `schema.go`

```go
var ErrSchemaValidation = errors.New("schema validation error")

type SchemaValidator struct {
    compiler *js.Compiler              // unexported
    rules    map[string]*SchemaValidationRule // unexported
}
// compile-time assert: var _ internal.SchemaValidator = (*SchemaValidator)(nil)

type SchemaValidationRule struct {
    Schema string `json:"schema"`
    Path   string `json:"path"`
    // unexported, populated at construction:
    schema   *js.Schema
    template *template.Template   // html/template
}

type SchemaDBChangeEvent struct {
    Operation     string   `json:"operation"`
    ID            string   `json:"id"`
    Table         string   `json:"table"`
    Key           []string `json:"key"`
    ModelVersion  string   `json:"modelVersion"`
    CompanyID     *string  `json:"companyId,omitempty"`
    LocationID    *string  `json:"locationId,omitempty"`
    UserID        *string  `json:"userId,omitempty"`
    Before        any      `json:"before,omitempty"`
    After         any      `json:"after,omitempty"`
    Diff          []string `json:"diff,omitempty"`
    Timestamp     int64    `json:"timestamp"`
    MVCCTimestamp string   `json:"mvccTimestamp"`
}

func (v *SchemaValidator) Validate(event internal.DBChangeEvent) (bool, bool, string, error)
func NewSchemaValidator(schemaDir string) (internal.SchemaValidator, error)
// unexported: func toSchemaDBChangeEvent(event internal.DBChangeEvent) (*SchemaDBChangeEvent, error)
```

### 2.2 `dbschema.go`

```go
func QuerySingleValue(ctx context.Context, db *sql.DB, fn string) (string, error)

type QueryConditions struct {
    Column string
    Value  string
}

func BuildDBSchemaFromInfoSchemaWithConditions(ctx context.Context, logger logger.Logger, db *sql.DB,
    column string, value string, failIfEmpty bool, conditions ...QueryConditions) (internal.DatabaseSchema, error)

func BuildDBSchemaFromInfoSchema(ctx context.Context, logger logger.Logger, db *sql.DB,
    column string, value string, failIfEmpty bool) (internal.DatabaseSchema, error)
```

### 2.3 `sql.go`

```go
func QuoteIdentifier(name string) string
func QuoteStringIdentifiers(vals []string) []string
func SQLExecuter(ctx context.Context, log logger.Logger, db *sql.DB, dryRun bool) func(sql string) error
func ToJSONStringVal(name string, val string, prop internal.SchemaProperty, quoteScalar bool) string
func ToUserPass(u *url.URL) string
func DropTable(ctx context.Context, logger logger.Logger, db *sql.DB, table string) error
// unexported: isEmptyVal(val string) bool
// unexported: var scalarValue = regexp.MustCompile(`^([+-]?([0-9]*[.])?[0-9]+)|(true|false)$`)
// unexported: quoteJSONScalar(val string, prop internal.SchemaProperty) string
```

### 2.4 `json.go`

```go
type JSONDecoder interface {
    Decode(v any) error
    More() bool
    Count() int
    Close() error
}
func NewNDJSONDecoder(fn string) (JSONDecoder, error)
func JSONDiff(obj map[string]any, found []string) []string
// unexported impl: type ndjsonReader struct { in *os.File; gr *gzip.Reader; dec *json.Decoder; count int }
```

### 2.5 `optimize.go`

```go
func CombineRecordsWithSamePrimaryKey(records []*Record) []*Record
func SortRecordsByMVCCTimestamp(records []*Record) []*Record
```

### 2.6 `cache.go`

```go
type Cache interface {
    Get(key string) (bool, any, error)
    Set(key string, val any, expires time.Duration) error
    Close() error
}
func NewCache(parent context.Context, expiryCheck time.Duration) Cache
// unexported: type value struct{object any; expires time.Time}; type inMemoryCache struct{...}
```

### 2.7 `batcher.go`

```go
type Batcher struct {
    records []*Record       // unexported
    pks     map[string]uint // unexported
}

type Record struct {
    Table     string                  `json:"table"`
    Id        string                  `json:"id"`
    Operation string                  `json:"operation"`
    Diff      []string                `json:"diff"`
    Object    map[string]any          `json:"object"`
    Event     *internal.DBChangeEvent `json:"-"`
}

func (r *Record) String() string
func (b *Batcher) Records() []*Record
func (b *Batcher) Add(event *internal.DBChangeEvent)
func (b *Batcher) Clear()
func (b *Batcher) Len() int
func NewBatcher() *Batcher
```

### 2.8 `file.go` / `file_windows.go`

```go
func IsDirWritable(path string) (bool, error)  // two build-tagged implementations
```

### 2.9 `zip.go`

```go
func GzipFile(filepath string) error
```

### 2.10 `help.go`

```go
func GenerateHelpSection(title string, body string) string
// unexported: var green, whiteBold (fatih/color SprintFuncs)
```

### 2.11 `errors.go`

```go
func RecoverPanic(logger logger.Logger)
// unexported: var depth = 3; func panicError(depth int, r interface{}) error
```

### 2.12 Load-bearing helpers from `util.go` (referenced by assigned files)

```go
func JSONStringify(val any) string
func Exists(fn string) bool
func SliceContains(slice []string, val string) bool
func ListDir(dir string) ([]string, error)
```

### 2.13 Cross-package types relied upon (`internal` package)

```go
// internal/schema.go
type SchemaProperty struct {
    Type                 string     `json:"type"`
    Format               string     `json:"format,omitempty"`
    Nullable             bool       `json:"nullable,omitempty"`
    Items                *ItemsType `json:"items,omitempty"`
    AdditionalProperties *bool      `json:"additionalProperties,omitempty"`
    Comment              *string    `json:"$comment,omitempty"`
    Deprecated           *bool      `json:"deprecated,omitempty"`
}
func (p SchemaProperty) IsNotNull() bool   { return !p.Nullable || p.Type == "array" }
func (p SchemaProperty) IsArrayOrJSON() bool { return p.Type == "object" || p.Type == "array" }

type DatabaseSchema map[string]map[string]string  // table -> (column -> dataType)

// internal/dbchange.go
type DBChangeEvent struct {
    Operation, ID, Table, ModelVersion, MVCCTimestamp string
    Key, Diff []string
    CompanyID, LocationID, UserID *string
    Before, After json.RawMessage
    Timestamp int64
    Imported bool
    NatsMsg jetstream.Msg
    object map[string]any        // unexported memoization
    SchemaValidatedPath *string
}
func (c *DBChangeEvent) GetPrimaryKey() string
func (c *DBChangeEvent) GetObject() (map[string]any, error)
```

---

## 3. Behavior & algorithms

### 3.1 Schema validation (`schema.go`)

**`NewSchemaValidator(schemaDir)`** — builds the validator from a directory:

1. `abs = filepath.Abs(schemaDir)`; error-wrap on failure.
2. `config = abs/config.json`. If `!Exists(config)` → error `"config.json not found in schema directory: %s"`.
3. Open `config.json`, JSON-decode into `map[string]*SchemaValidationRule` (key = table name). Each rule has `schema` (filename relative to dir) and optional `path` (a Go template).
4. Create `js.NewCompiler()` (santhosh-tekuri jsonschema/v5).
5. `ListDir(abs)` (recursive; skips `.DS_Store`). For every file **except** the one whose path relative to `abs` equals `"config.json"`:
   - Read bytes.
   - Register the same bytes as a compiler resource under **three** URL spellings (so `$ref`s resolve regardless of how they're written): `"file://"+rel`, `"file:///"+rel`, `"file://"+filename` (filename = absolute path). `rel` is the path relative to `abs`.
6. For each `(table, rule)`:
   - `fn = abs/rule.Schema`. If `!Exists(fn)` → error `"schema file not found: %s for table: %s"`.
   - `compiler.Compile("file://"+fn)` → store in `rule.schema`.
   - If `rule.Path != ""`: `template.New(fn).Parse(rule.Path)` (**`html/template`**, not text/template) → store in `rule.template`.
7. Return `*SchemaValidator{compiler, rules}`.

**`Validate(event)` → `(found bool, valid bool, path string, err error)`**:

1. Look up `rule = v.rules[event.Table]`. If absent → return `(false, false, "", nil)` (no schema for this table; caller treats as "not validated").
2. Convert to `SchemaDBChangeEvent` via `toSchemaDBChangeEvent`: if `event.Before != nil`, JSON-unmarshal raw bytes into `map[string]any`; same for `After`. Copy all scalar fields. On unmarshal error → `(true, false, "", wrapped err)`.
3. Round-trip to a generic map: `JSONStringify(object)` then `json.Unmarshal` into `o map[string]any`. (This normalizes the struct — applying `omitempty`/json tags — into the shape the JSON-Schema validator expects.) On error → `(true, false, "", wrapped err)`.
4. `rule.schema.Validate(o)`:
   - If error is `*js.ValidationError` → return `(true, false, "", errors.Join(ErrSchemaValidation, verr))`. **The sentinel `ErrSchemaValidation` is joined**, so callers do `errors.Is(err, ErrSchemaValidation)`.
   - Any other error → `(true, false, "", err)`.
5. If `rule.template != nil`: execute the template against `o`, accumulating into a `strings.Builder`. On error → `(true, false, "", "error executing template: %w for %s")` (includes `JSONStringify(o)`).
6. Return `(true, true, path.String(), nil)`.

Note: the returned `path` is the rendered template (or `""` when no template). Because it uses `html/template`, characters `& < > " '` in interpolated values are HTML-escaped in the output path.

### 3.2 `dbschema.go`

**`QuerySingleValue`**: executes `"SELECT " + fn` with `QueryRowContext`, scans a single `string`. Returns `("", err)` on failure. `fn` is concatenated raw (no escaping) — caller-controlled SQL fragment.

**`BuildDBSchemaFromInfoSchemaWithConditions`**:
1. `res := make(internal.DatabaseSchema)`; `start := time.Now()`.
2. Build query (string interpolation, **not** parameterized):
   `"SELECT table_name, column_name, data_type FROM information_schema.columns WHERE %s = '%s'"` with `column`,`value`; then for each variadic `QueryConditions`, append `" AND %s = '%s'"`.
3. `QueryContext`; iterate rows scanning `(tableName, columnName, dataType)`; populate `res[tableName][columnName] = dataType` (lazily allocating the inner map).
4. If `failIfEmpty && len(res)==0` → error `"no tables found using %s = %s"`.
5. `logger.Info("refreshed %d tables ddl in %v", len(res), time.Since(start))`.

`BuildDBSchemaFromInfoSchema` simply calls the WithConditions variant with no extra conditions.

### 3.3 SQL helpers (`sql.go`) — includes the critical `ToJSONStringVal`

- **`QuoteIdentifier(name)`** → `"\"" + name + "\""` (wraps in double quotes; no internal escaping of embedded quotes).
- **`QuoteStringIdentifiers(vals)`** → maps `QuoteIdentifier` over the slice (preserving order/length).
- **`SQLExecuter(...)`** returns a closure `func(sql string) error`:
  - If `dryRun`: `log.Info("[dry-run] %s", sql)`, return nil (no execution).
  - Else: `log.Debug("executing: %s", strings.TrimRight(sql, "\n"))` then `db.ExecContext`; return its error.
- **`isEmptyVal(val)`**: true iff `val == "''" || val == "" || val == "NULL" || val == "null"`. (Note: matches the SQL empty-quoted-string literal `''`, the empty Go string, and both cased NULL spellings.)
- **`ToJSONStringVal(name, val, prop, quoteScalar)`** — coerces a value destined for a JSON/array column:
  1. If `prop.IsArrayOrJSON()` (type `"object"` or `"array"`) **AND** `prop.IsNotNull()` (`!Nullable || Type=="array"`) **AND** `isEmptyVal(val)`:
     - `Type == "array"` → return `"'[]'"`
     - `Type == "object"` → return `"'{}'"`
     - (other types fall through to step 2/3 — but only object/array reach here due to the IsArrayOrJSON guard)
  2. If `quoteScalar` → return `quoteJSONScalar(val, prop)`.
  3. Else return `val` unchanged.
- **`quoteJSONScalar(val, prop)`**: if `prop.Type == "object"` and the regex `scalarValue` matches `val` → return `"'" + val + "'"`; else return `val`. This wraps bare numbers/booleans in single quotes so they are valid JSON-string literals in databases that demand quoting of JSON scalars.
- **`scalarValue` regex**: `` ^([+-]?([0-9]*[.])?[0-9]+)|(true|false)$ `` — **CRITICAL, asymmetric anchoring**. Because `|` has lowest precedence, this is `(^([+-]?([0-9]*[.])?[0-9]+))` OR `((true|false)$)`. The numeric alternative is anchored only at the **start**; the boolean alternative only at the **end**. So `"123abc"` matches (starts with a number), and `"abctrue"` matches (ends with `true`). A faithful port MUST reproduce this exact regex including the asymmetry.
- **`ToUserPass(u)`**: builds `user` then, if a password is present, `":"+pass` → returns `"user"` or `"user:pass"`.
- **`DropTable`**: executes `"DROP TABLE IF EXISTS " + table` (table concatenated raw); returns Exec error.

### 3.4 JSON helpers (`json.go` + `util.go`)

- **`ndjsonReader`** implements `JSONDecoder` over a (optionally gzipped) newline-delimited JSON file:
  - `Decode(v)` → `dec.Decode(v)`; on success increments `count`.
  - `More()` → `dec.More()`.
  - `Count()` → `count`.
  - `Close()` → closes gzip reader then file (each only once, nil-guarded).
- **`NewNDJSONDecoder(fn)`**: open file; if `filepath.Ext(fn) == ".gz"` wrap in `gzip.NewReader`; build `json.NewDecoder`. Errors wrapped `"error opening: %s. %w"` / `"gzip: error opening: %s. %w"`.
- **`JSONDiff(obj, found)`**: returns the keys present in `obj` that are **not** in the `found` slice. Result is initialized to non-nil empty slice (`make([]string,0)`); membership via `SliceContains` (linear scan). Iteration order over `obj` is map-random → output order is non-deterministic. Used to detect new/unknown columns in an event versus the known schema.
- **`JSONStringify(val)`** (`util.go`): `json.Marshal(val)`, **error ignored**, returns string (empty string if marshal fails). Go's `encoding/json` escapes `<`,`>`,`&` as `\u003c`,`\u003e`,`\u0026` by default and sorts map keys lexicographically — a faithful port must match key ordering and HTML escaping where this output feeds back into parsing/validation.

### 3.5 Optimize pass (`optimize.go`)

**`CombineRecordsWithSamePrimaryKey(records)`**: collapses multiple change events for the same row.
1. Group records into `recordsMap[key]` where `key = record.Table + record.Id` (string concat, no separator).
2. For each group, run `combine`:
   - If group has 1 record → return as-is.
   - `collapsed = [records[0]]`; `previous = records[0]`.
   - For each subsequent `record`:
     - Determine `batchType` (default `withoutBatch`):
       - `record.Operation == "DELETE"` → `deleteWithBatch`.
       - `record.Operation == "UPDATE"`: inspect `previous.Operation` — `"INSERT"` → `withoutBatch`; `"UPDATE"` → `updateWithBatch`. (Any other prior op, e.g. after a DELETE, leaves `withoutBatch`.)
       - `record.Operation == "INSERT"` → stays `withoutBatch`.
     - Apply:
       - `withoutBatch`: append `record`; `previous = record`.
       - `updateWithBatch` (merge consecutive UPDATEs): union `record.Diff` into `previous.Diff` (append keys not already present, preserving order); `previous.Event = record.Event`; `maps.Copy(previous.Object, record.Object)` (record's fields overwrite previous's, in place).
       - `deleteWithBatch`: `collapsed = collapsed[:0]` (truncate to empty, keep backing array) then append `record`; `previous = record`. **A DELETE discards all earlier records for that key**, leaving only the delete.
   - Return `collapsed`.
3. Concatenate all groups' results into `processedRecords` (declared as nil slice, appended to). **Order across groups is non-deterministic** (map iteration). `processedRecords` may be `nil` if input is empty.

**`SortRecordsByMVCCTimestamp(records)`**: in-place sort (returns the same slice) ascending by `strconv.ParseFloat(record.Event.MVCCTimestamp, 64)`. If `record.Event == nil`, timestamp defaults to `0`. Parse errors ignored (value left at 0). Uses `slices.SortFunc` (**not stable**) with comparator returning −1/1/0. MVCC timestamps are CockroachDB HLC strings like `"1700000000.0000000001"` — float64 parsing is the canonical comparison here (note: large HLC values can lose precision in float64, but this matches Go behavior and must be replicated, e.g. via `double.Parse`).

### 3.6 In-memory TTL cache (`cache.go`)

- `value{object any; expires time.Time}`.
- **`Get(key)`**: RLock; read entry. If found and `val.expires.Before(time.Now())` → upgrade to write lock, `delete`, return `(false, nil, nil)` (lazy eviction). If found and not expired → `(true, val.object, nil)`. If absent → `(false, nil, nil)`. Never returns a non-nil error.
- **`Set(key, val, expires)`**: write lock; store `&value{val, time.Now().Add(expires)}`. Always nil error. (No max size, no eviction count.)
- **`Close()`**: `once.Do` → `cancel()` context, `waitGroup.Wait()` for the sweeper goroutine. Idempotent.
- **`run()` (sweeper goroutine)**: `waitGroup.Add(1)`; `time.NewTicker(expiryCheck)`. Loop: on `ctx.Done()` return (deferred `timer.Stop()` + `waitGroup.Done()`); on tick, write-lock, collect keys whose `expires.Before(now)`, delete them.
- **`NewCache(parent, expiryCheck)`**: creates child cancelable context, empty map, starts `run()` goroutine, returns the interface.

### 3.7 Batcher (`batcher.go`)

- **`Add(event)`**: `object, _ := event.GetObject()` (error deliberately ignored — "error handled in consumer"). Append a `&Record{Table, Id: event.GetPrimaryKey(), Operation, Diff: event.Diff, Object: object, Event: event}`.
  - `GetPrimaryKey()` returns the **last** element of `event.Key` if `len(Key)>=1`; otherwise falls back to `object["id"]` (string); else `""`.
  - `GetObject()` lazily unmarshals `After` (preferred) or `Before` into a memoized `map[string]any`; returns `(nil,nil)` when both empty.
- **`Records()`** → the slice; **`Len()`** → `len(records)`.
- **`Clear()`**: `records = nil`; `pks = make(map[string]uint)`.
- **`NewBatcher()`**: initializes `pks` only.
- **`pks` is effectively dead**: allocated in `NewBatcher`/`Clear` but never read or written in `Add`. A port may omit it (note it for fidelity but it has no behavioral effect).
- **`Record.String()`** → `JSONStringify(r)` (uses the json tags; `Event` excluded via `json:"-"`).

### 3.8 File utilities

- **`IsDirWritable(path)`** — two builds:
  - **Windows** (`file_windows.go`): `os.Stat`; error-wrap `"stat: %w"`. Fail if not dir (`"%s is not a directory"`). Check `info.Mode().Perm() & (1<<7) != 0` (owner-write bit, octal 0200); fail `"write permission bit is not set for this user for %s"`. Returns `true`. (No real ACL check on Windows.)
  - **Non-Windows** (`file.go`): same first three checks, then `syscall.Stat` (`"sysstat: %w"`), then ownership check `uint32(os.Geteuid()) != stat.Uid` → `"user doesn't have permission to write to %s"`. Returns true otherwise.
- **`Exists(fn)`** (`util.go`): `os.Stat`; returns `false` only if `os.IsNotExist(err)`; otherwise `true` (note: other stat errors are treated as "exists").
- **`ListDir(dir)`** (`util.go`): recursive `os.ReadDir`; descends into subdirs; **skips files named `.DS_Store`**; returns full joined paths; result is non-nil empty slice when no files.

### 3.9 `zip.go` — `GzipFile(filepath)`

1. Open `filepath` (`"open: %w"`).
2. Create `filepath + ".gz"` (`"create: %w"`).
3. `gzip.NewWriter(outfile)`, `io.Copy(zr, infile)` (`"copy: %w"`).
4. Deferred closes run LIFO: gzip writer closed (flushes) before output file. **Original file is not deleted.** Close errors are not surfaced.

### 3.10 `help.go` — `GenerateHelpSection(title, body)`

Returns `green(title) + "\n\n" + whiteBold(body)`, where `green = color.New(color.FgGreen).SprintFunc()` and `whiteBold = color.New(color.FgWhite, color.Bold).SprintFunc()` (fatih/color; honors NO_COLOR / non-TTY auto-disable).

### 3.11 `errors.go` — `RecoverPanic(logger)`

Designed to be `defer`-ed. On `recover()`:
1. `v := panicError(depth, r)` with package var `depth = 3` (pops the `panicError`/`RecoverPanic`/`panic` frames).
2. Dump all goroutines: `pprof.Lookup("goroutine").WriteTo(&str, 2)` (debug level 2 = full stacks).
3. `logger.Error("a panic has occurred: %s\ncurrent goroutines:\n\n%s", v, str)`.
4. `os.Exit(2)` (matches Go's panic exit code).

`panicError(depth, r)`: if `r` is an `error` → `errors.WithStackDepth(err, depth+1)`; else `errors.NewWithDepthf(depth+1, "panic: %v", r)` (cockroachdb/errors, captures stack at the given depth).

---

## 4. External dependencies

| Go package | Role | .NET / C# equivalent |
|---|---|---|
| `github.com/santhosh-tekuri/jsonschema/v5` | Compile + validate JSON Schema; resolves `$ref` via registered file resources; distinguishes `*ValidationError`. | `JsonSchema.Net` (NuGet `JsonSchema.Net`) for validation; for `$ref` base-URI/resource registration use its `SchemaRegistry`. Newtonsoft's `Json.Schema` is an alternative (license-limited). |
| `github.com/fatih/color` | ANSI terminal colorization (green title, white-bold body), auto-disable on non-TTY/NO_COLOR. | `Spectre.Console` or `Pastel` (NuGet); or manual ANSI escapes gated on `Console.IsOutputRedirected`/`NO_COLOR`. |
| `github.com/cockroachdb/errors` | Error wrapping with explicit stack-capture depth (`WithStackDepth`, `NewWithDepthf`). | BCL `System.Exception` + `Environment.StackTrace`; or capture via `new StackTrace(skipFrames)`. No exact depth-based equivalent — usually just rethrow with context. |
| `github.com/shopmonkeyus/go-common/logger` | Printf-style structured logger (`Info/Debug/Trace/Error`). | An `ILogger`-style abstraction (`Microsoft.Extensions.Logging`) or the project's own logger port. Preserve printf-format semantics. |
| `github.com/shopmonkeyus/eds/internal` | Domain types `DBChangeEvent`, `SchemaProperty`, `DatabaseSchema`, `SchemaValidator`. | Ported domain model in the C# `Internal` namespace. |
| `github.com/nats-io/nats.go/jetstream` | `jetstream.Msg` field on `DBChangeEvent` (only referenced transitively). | NATS .NET client (`NATS.Client.JetStream`). |
| stdlib `database/sql` | DB handle + `QueryRowContext`/`QueryContext`/`ExecContext`. | `System.Data.Common.DbConnection` / ADO.NET (`DbCommand`, `DbDataReader`). |
| stdlib `encoding/json` | Marshal/unmarshal; streaming `Decoder` (`More`, `Decode`). | `System.Text.Json` (`JsonSerializer`, `Utf8JsonReader`/streaming) — mind escaping/key-ordering differences (see gotchas). |
| stdlib `html/template` | Renders the per-table routing `Path` (**HTML-escaping** applies). | `System.Text` templating; reproduce Go template `{{.Field}}` semantics + HTML escaping. No BCL one-to-one; likely a small custom renderer (e.g. Scriban or hand-rolled) replicating Go's `text/template` actions *with* HTML escaping. |
| stdlib `compress/gzip` | NDJSON gzip read + `GzipFile` write. | `System.IO.Compression.GZipStream`. |
| stdlib `regexp` | `scalarValue`, schema file/URL helpers. | `System.Text.RegularExpressions.Regex` (RE2-vs-.NET differences are negligible for these patterns; keep exact pattern text). |
| stdlib `runtime/pprof` | Goroutine dump on panic. | No equivalent; closest is dumping `Process.Threads` / a managed stack dump (e.g. `ClrMD`) — typically just log the exception + `Environment.StackTrace` and `Environment.Exit(2)`. |
| stdlib `syscall` / `os` (euid, file mode) | POSIX ownership/permission check in `IsDirWritable`. | Windows: attempt a probe write or check ACLs (`System.Security.AccessControl`). The Go behavior is OS-split; port the Windows path (mode bit check only). |
| stdlib `net` (`GetFreePort`) | Bind `localhost:0` to grab a free port. | `TcpListener(IPAddress.Loopback, 0)` then read `LocalEndpoint`. |
| stdlib `maps`/`slices` | `maps.Copy`, `slices.Contains`, `slices.SortFunc`. | `Dictionary` merge loop, `List.Contains`, `List.Sort`/`Array.Sort` with comparer (note: not stable). |

---

## 5. Edge cases & gotchas

1. **`scalarValue` regex asymmetric anchoring** (sql.go): `^...|...$` — start-anchored number OR end-anchored boolean. `"5x"` and `"x true"` both match. Do not "fix" this to a fully-anchored regex; it changes driver output.
2. **`ToJSONStringVal` null/empty mapping**: only object/array+not-null+empty values become `'{}'`/`'[]'`. `IsNotNull()` deems **arrays always not-null** (`Type=="array"` overrides `Nullable`). A nullable object with empty value is left untouched (passes through to scalar quoting / raw). This is the exact source of the JSON column defaults.
3. **`isEmptyVal` matches four spellings**: `''`, `""` (empty), `NULL`, `null`. Case-sensitive (only those two NULL casings).
4. **SQL built by string interpolation** (dbschema.go, DropTable, QuerySingleValue): no parameterization/escaping. Faithful port must concatenate identically (do not "improve" to parameterized queries unless behavior is preserved). Values come from trusted internal config, but the strings/format must match for byte-identical SQL/logs.
5. **`JSONStringify` ignores marshal errors** and returns `""`; Go's json escapes `< > &` as `\uXXXX` and **sorts map keys** — this matters because `Validate` round-trips through `JSONStringify` before schema validation, and `Record.String()` uses it. `System.Text.Json` by default does NOT sort keys and uses different escaping; configure a matching encoder + sorted-key behavior if byte-equality matters, otherwise at least ensure semantic equivalence for validation.
6. **`html/template` HTML-escaping in the routing path** (schema.go): values containing `& < > " '` are escaped in the rendered `Path`. Easy to miss; reproduce escaping.
7. **`ErrSchemaValidation` is `errors.Join`ed** with the underlying `*js.ValidationError`. Callers use `errors.Is(err, ErrSchemaValidation)`. In C#, model with a custom exception type (e.g. `SchemaValidationException`) and surface the inner validation detail.
8. **`Validate` return contract is a 4-tuple** `(found, valid, path, err)`. `found=false` (no rule) is NOT an error and `valid=false` then is meaningless. When a rule exists but validation fails, `found=true, valid=false, err!=nil`. Preserve this tri-state precisely (e.g., a result struct/enum in C#).
9. **`CombineRecordsWithSamePrimaryKey` output order is non-deterministic** (Go map iteration over groups). The DELETE collapse (`collapsed[:0]`) wipes prior records. `processedRecords` can be `nil`. Mutates input `Record`s in place (`previous.Diff`, `.Object`, `.Event`) via `maps.Copy`. A port must (a) decide if order matters downstream and (b) replicate in-place mutation/aliasing semantics — `maps.Copy(dst,src)` overwrites dst keys with src values, keeping dst-only keys.
10. **`SortRecordsByMVCCTimestamp` is not stable** and parses HLC timestamps as `float64` (precision loss possible) with parse errors silently → 0. nil `Event` → 0. Use `double` and an unstable sort to match; ties keep comparator-0 ordering.
11. **Key grouping uses bare concat** `Table + Id` with no separator — theoretically collidable (`"ab"+"c"` vs `"a"+"bc"`); replicate exactly (do not add a delimiter).
12. **Cache lazy + active eviction**: `Get` evicts on read if expired; sweeper evicts on interval. `Get`/`Set` never error. `Close` is idempotent (`sync.Once`) and joins the goroutine. A C# port should use a `CancellationTokenSource` + background `Task` (or `System.Threading.Timer`) and a `ReaderWriterLockSlim`; ensure `Dispose` cancels and awaits.
13. **`Batcher.Add` swallows `GetObject` error** (object may be nil). `GetPrimaryKey` falls back to last `Key` element then `object["id"]`. `pks` field unused.
14. **`Exists` returns true on non-`NotExist` stat errors** (e.g., permission). Don't naively map to `File.Exists` (which returns false on any error) — replicate: only "file not found" → false.
15. **`ListDir` recurses and skips `.DS_Store`** but not other hidden/OS files; returns absolute-ish joined paths in OS-native separators.
16. **`NewSchemaValidator` registers each schema resource under three URL spellings** and the `config.json` skip is by **relative** path equality (`rel == "config.json"`), so a nested file also named `config.json` in a subdir is NOT skipped. Reproduce the exact skip condition.
17. **`RecoverPanic` calls `os.Exit(2)`** — bypasses other defers/cleanup. In C#, the equivalent is logging then `Environment.Exit(2)`; note `Environment.Exit` likewise skips `finally` blocks/`using` disposal.
18. **`GzipFile` leaves the original file** and ignores close errors; output is always `<path>.gz` (double extension if input already `.gz`).
19. **`IsDirWritable` Windows path only checks the owner-write mode bit** (`Perm()&0200`), which on Windows is a coarse heuristic; it does not consult ACLs. The non-Windows path additionally enforces euid==owner. The C# target should port the Windows semantics (mode-bit heuristic) since the consumer runs cross-platform.
20. **`QuoteIdentifier` does not escape embedded double-quotes** — `a"b` → `"a"b"` (malformed). Reproduce as-is.
21. **`ndjsonReader.More()` delegates to the JSON decoder**, which returns true if there's another value in the current array/stream; for NDJSON this drives the read loop. `Count` only increments on successful `Decode`.

---

## 6. C# port notes

- **Project structure**: mirror the Go split into a `Util` static-helper class group plus instance classes: `SchemaValidator`, `DatabaseSchemaBuilder`, `Cache`/`InMemoryCache`, `Batcher`, `NdjsonDecoder`. Keep `DatabaseSchema` as `Dictionary<string, Dictionary<string,string>>` (or a thin wrapper exposing `Columns`/`GetType`). Keep `SchemaProperty.IsNotNull`/`IsArrayOrJSON` as the exact boolean expressions.

- **`Validate` 4-tuple**: return a small `readonly record struct ValidationResult(bool Found, bool Valid, string Path, Exception? Error)` or use `out` params. Do not throw for the "no rule" case (`Found=false`). For schema failure, wrap the validator's error in a `SchemaValidationException` analogous to the joined `ErrSchemaValidation`, and expose an `Is`-style check.

- **JSON Schema**: use `JsonSchema.Net`. Register each schema document into a `SchemaRegistry` under base URIs matching the Go `file://`/`file:///`/`file://<abs>` spellings so `$ref`s resolve. Compile/validate against the round-tripped `JsonNode`/`JsonElement` (replicate the `JSONStringify`-then-parse normalization so `omitempty`/null fields are dropped identically — i.e., serialize the `SchemaDBChangeEvent` DTO with `JsonIgnoreCondition.WhenWritingNull` mapping the `,omitempty` tags, then re-parse).

- **`ToJSONStringVal`**: port verbatim, including the four-way `isEmptyVal`, the `IsArrayOrJSON && IsNotNull && isEmptyVal` guard returning `'[]'`/`'{}'`, and `quoteJSONScalar`. Compile the regex once as `static readonly Regex` with the **exact** pattern string `^([+-]?([0-9]*[.])?[0-9]+)|(true|false)$` (no `RegexOptions` that would change anchoring). The asymmetric anchoring is intentional.

- **`JSONStringify`**: if its output feeds validation/equality, configure `JsonSerializer` to match Go: escape non-ASCII/HTML the same way (custom `JavaScriptEncoder` if byte-equality is needed) and **sort object keys** (Go marshals maps in sorted key order). For `Record.String()`/logging, a default serializer is fine. Honor `[JsonIgnore]` on the `Event` field.

- **Template/path rendering**: Go uses `html/template`. There is no BCL equivalent. Implement a minimal renderer supporting the template features actually used in `config.json` `path` values (likely just `{{.Field}}` lookups into the map) and apply HTML escaping to interpolated values to match Go. If templates are richer, consider Scriban with an HTML-escaping output filter; verify escaping parity.

- **Cache**: `InMemoryCache : IDisposable`. Back with `ConcurrentDictionary<string, (object Value, DateTime Expires)>` or a `Dictionary` + `ReaderWriterLockSlim` to mirror RLock/Lock semantics. Background sweeper via `Task.Run` with a `CancellationToken` and `PeriodicTimer(expiryCheck)`; `Dispose` cancels and awaits (mirror `sync.Once` with a guard flag). `Get` must lazily evict on read. Never throw from `Get`/`Set`.

- **Optimize pass**: port the state machine exactly (`withoutBatch`/`updateWithBatch`/`deleteWithBatch`). Use `List<Record>` per group keyed by `Table + Id`. For `updateWithBatch`, union diffs preserving order and do an in-place dictionary merge (`foreach kv: previous.Object[k]=v`) to match `maps.Copy`. For `deleteWithBatch`, clear the group list and add the delete. If downstream depends on ordering, sort groups deterministically (Go does not — document that you are matching/diverging). For `SortRecordsByMVCCTimestamp`, parse with `double.TryParse(..., CultureInfo.InvariantCulture)` (default 0 on failure / null Event) and `List.Sort` with a comparer returning sign of difference.

- **SQL**: keep raw string concatenation to produce byte-identical SQL and logs (`"SELECT " + fn`, the `information_schema` query with `'%s'` interpolation, `DROP TABLE IF EXISTS ...`). Use ADO.NET `DbCommand.ExecuteReaderAsync`/`ExecuteScalarAsync`. `SQLExecuter` → a delegate `Func<string, Task>` (or `Action<string>`), logging `"[dry-run] {sql}"` or `"executing: {sql.TrimEnd('\n')}"`.

- **Files**: `Exists` → replicate "true unless NotFound" (catch only `FileNotFoundException`/`DirectoryNotFoundException`, treat other errors as exists). `ListDir` → recursive enumeration skipping `.DS_Store`. `IsDirWritable` → port the Windows heuristic (check directory exists/is-dir; a write-permission probe or ACL check; mode-bit check is meaningless on Windows so a probe-write to a temp file is the pragmatic equivalent). `GzipFile` → `GZipStream` writing `<path>.gz`, leave original.

- **Panic handling**: there is no Go-style panic/recover; map to a top-level `AppDomain.CurrentDomain.UnhandledException`/`try-catch` boundary that logs the exception plus a managed stack/thread dump and calls `Environment.Exit(2)`.

- **Risks to watch**: (1) JSON key ordering/escaping divergence breaking schema validation round-trip; (2) the regex anchoring if "normalized"; (3) `html/template` escaping omitted; (4) unstable-sort and float64 timestamp precision differences; (5) in-place mutation/aliasing in the optimize pass (don't accidentally deep-copy and break shared references the caller may rely on); (6) `Environment.Exit` skipping `using`/`finally` just like `os.Exit`; (7) non-deterministic group ordering — confirm whether any consumer assumes order before relying on it.