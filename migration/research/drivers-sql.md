# Behavioral Specification: SQL Drivers Subsystem (`internal/drivers/{mysql,sqlserver,snowflake}`)

## 1. Purpose

This subsystem implements three of the EDS consumer's destination database drivers — MySQL, Microsoft SQL Server, and Snowflake (plus a Snowflake Key-Pair auth variant). Each driver implements the shared `internal.Driver`, `internal.DriverLifecycle`, `internal.Importer`, `internal.DriverHelp`, and `internal.DriverMigration` contracts. They translate `internal.DBChangeEvent` change records (INSERT/UPDATE/DELETE coming from the Shopmonkey transactional DB via NATS JetStream) into vendor-specific SQL text, buffer them, and execute them transactionally. They also support bulk import (loading CockroachDB changefeed `.ndjson.gz` export files) and schema migration (create/drop/alter tables and columns). They are self-registered in `init()` and selected by URL scheme. The PostgreSQL driver (`internal/drivers/postgresql`) is the reference implementation for value quoting; this spec calls out every deviation.

---

## 2. Public surface

All three driver structs are **unexported**; the only thing made public is registration via `init()`. The "public surface" that a port must reproduce is the set of interface methods, the registered schemes, and the exported helper functions.

### Shared interfaces each driver satisfies (from `internal`)
- `internal.Driver`: `Stop() error`, `MaxBatchSize() int`, `Process(logger.Logger, DBChangeEvent) (bool, error)`, `Flush(logger.Logger) error`, `Test(context.Context, logger.Logger, url string) error`, `Configuration() []DriverField`, `Validate(map[string]any) (string, []FieldError)`.
- `internal.DriverLifecycle`: `Start(DriverConfig) error`.
- `internal.Importer`: `Import(ImporterConfig) error`.
- `internal.DriverHelp`: `Name() string`, `Description() string`, `ExampleURL() string`, `Help() string`.
- `internal.DriverMigration`: `MigrateNewTable(ctx, logger, *Schema) error`, `MigrateNewColumns(ctx, logger, *Schema, columns []string) error`, `GetDestinationSchema(ctx, logger) DatabaseSchema`.
- `importer.Handler` (MySQL & SQLServer only): `CreateDatasource(SchemaMap) error`, `ImportEvent(DBChangeEvent, *Schema) error`, `ImportCompleted() error`.
- `internal.DriverSessionHandler` (Snowflake only): `SetSessionID(sessionID string)`.
- `internal.DriverAlias` (SQLServer only): `Aliases() []string` → returns `["mssql"]`.

### Registered schemes (from `init()`)
- MySQL: `internal.RegisterDriver("mysql", …)`, `internal.RegisterImporter("mysql", …)`. No aliases.
- SQLServer: `RegisterDriver("sqlserver", …)`, `RegisterImporter("sqlserver", …)`; alias `mssql` (registered via `Aliases()`).
- Snowflake: `RegisterDriver("snowflake", …)`, `RegisterImporter("snowflake", …)`.
- Snowflake Key-Pair: `RegisterDriver("snowflake-keypair", …)`, `RegisterImporter("snowflake-keypair", …)`.

### Constants (magic numbers)
| Driver | `maxBatchSize` | `maxBytesSizeInsert` | default port |
|---|---|---|---|
| MySQL | `500` | `5_000_000` | `3306` |
| SQLServer | `500` | `5_000_000` | `1433` |
| Snowflake | `200` | (none) | `-1` (no port) |
| Snowflake Key-Pair | `200` (inherited) | (none) | n/a (no port) |

Snowflake additional package-level vars: `var sequence int64` (global flush counter), `var uuidRegexp = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)`, `var snowflakeBulkRecordModifications = []func([]*util.Record)[]*util.Record{ util.SortRecordsByMVCCTimestamp, util.CombineRecordsWithSamePrimaryKey }`. Key-pair: `var secretKey = "secret-key"`.

### Exported functions in these packages
- `mysql.ParseURLToDSN(urlstr string) (string, error)`
- `mysql.EscapeString(s string) string` (copied from TiDB; not used in flow but exported)
- `sqlserver.ParseURLToDSN(urlstr string) (string, error)`
- `sqlserver.EscapeString(s string) string`
- `snowflake.GetConnectionStringFromURL(urlString string) (string, error)`
- `snowflake.OpenSnowflakeWithKeyPair(secret, username, account, database, schema string) (*sql.DB, error)`
- `snowflake.DriverProfilingInfo` struct: fields `recordsProcessedCount int`, `lastExecutionDuration time.Duration`, `averageExecutionDuration time.Duration` (all unexported); method `String() string`.

### Driver struct fields (state to reproduce)
- **MySQL/SQLServer** (identical shape): `ctx context.Context`, `logger logger.Logger`, `db *sql.DB`, `registry internal.SchemaRegistry`, `waitGroup sync.WaitGroup`, `once sync.Once`, `pending strings.Builder`, `count int`, `executor func(string) error`, `importConfig internal.ImporterConfig`, `size int`, `dbname string`, `dbschema internal.DatabaseSchema`.
- **Snowflake**: `config internal.DriverConfig`, `ctx`, `logger`, `db`, `registry`, `waitGroup`, `once`, `batcher *util.Batcher`, `locker sync.Mutex`, `sessionID string`, `dbname string`, `dbSnowflakeSchemaName string`, `dbschema internal.DatabaseSchema`, `profilingInfo DriverProfilingInfo`, `bulkRecordModifications []func([]*util.Record)[]*util.Record`, `connectToDBFunc func(ctx, url) (*sql.DB, error)`.
- **Snowflake Key-Pair**: `type snowflakeKeyPairDriver struct { snowflakeDriver }` (embeds the whole snowflake driver; overrides `Name`, `Description`, `ExampleURL`, `Configuration`, `Validate`, `Start`, `Import`, `Test`).

### Supporting types referenced (must be ported alongside)
- `internal.SchemaProperty` (json tags): `Type string` (`type`), `Format string` (`format,omitempty`), `Nullable bool` (`nullable,omitempty`), `Items *ItemsType` (`items,omitempty`), `AdditionalProperties *bool` (`additionalProperties,omitempty`), `Comment *string` (`$comment,omitempty`), `Deprecated *bool` (`deprecated,omitempty`). Methods: `IsNotNull() bool` → `!Nullable || Type=="array"`; `IsArrayOrJSON() bool` → `Type=="object" || Type=="array"`.
- `internal.ItemsType`: `Type string`, `Enum []string` (`enum,omitempty`), `Format string` (`format,omitempty`).
- `internal.Schema`: `Properties map[string]SchemaProperty`, `Required []string`, `PrimaryKeys []string`, `Table string`, `ModelVersion string`, private cached `columns []string`. Method `Columns()`: returns cached or computes = (non-PK property names **sorted ascending**) appended after `PrimaryKeys` (PKs first, in PK order, then sorted remainder); result is cached.
- `internal.DBChangeEvent`: `Operation string`, `ID string`, `Table string`, `Key []string`, `ModelVersion string`, `CompanyID/LocationID/UserID *string`, `Before/After json.RawMessage`, `Diff []string`, `Timestamp int64`, `MVCCTimestamp string`, `Imported bool`, `NatsMsg jetstream.Msg`, private `object map[string]any`, `SchemaValidatedPath *string`. Methods: `GetObject()` lazily unmarshals `After` (else `Before`) into a `map[string]any` and caches; `GetPrimaryKey()` returns last element of `Key`, else `object["id"]`.
- `util.Record` (used by Snowflake batcher) json tags: `Table` (`table`), `Id` (`id`), `Operation` (`operation`), `Diff []string` (`diff`), `Object map[string]any` (`object`), `Event *internal.DBChangeEvent` (`-`).
- `internal.DatabaseSchema = map[string]map[string]string` (table → column → SQL data type). `GetType(table,col) (bool,string)`, `Columns(table) []string`.

---

## 3. Behavior & algorithms

### 3.0 Shared lifecycle pattern (MySQL & SQLServer are nearly identical)

**`connectToDB(ctx, urlstr)`**
1. `dsn = ParseURLToDSN(urlstr)`.
2. `db = sql.Open(<driverName>, dsn)` (`"mysql"` / `"sqlserver"`).
3. Ping with a **5-second** timeout context (`context.WithTimeout(ctx, 5*time.Second)`); on failure `db.Close()` and return error.
4. `refreshSchema(ctx, db, false)` (failIfEmpty=false). On failure close + return.

**`refreshSchema(ctx, db, failIfEmpty)`**
- If `dbname == ""`, query current DB name via `util.QuerySingleValue(ctx, db, fn)`:
  - MySQL fn = `"DATABASE()"`; SQLServer fn = `"DB_NAME()"`; Snowflake fn = `"CURRENT_DATABASE()"` (and `"CURRENT_SCHEMA()"` for `dbSnowflakeSchemaName`).
  - `QuerySingleValue` runs `SELECT <fn>` and scans one string.
- `BuildDBSchemaFromInfoSchema(ctx, logger, db, column, value, failIfEmpty)` builds `DatabaseSchema`:
  - SQL = `SELECT table_name, column_name, data_type FROM information_schema.columns WHERE <column> = '<value>'`.
  - MySQL filter column = `"table_schema"`; SQLServer = `"table_catalog"`; Snowflake uses `"table_catalog"` plus an **extra AND condition** `table_schema = '<dbSnowflakeSchemaName>'` (via `BuildDBSchemaFromInfoSchemaWithConditions`).
  - **Note: value is string-concatenated, not parameterized.**
  - If `failIfEmpty && len==0` → error `"no tables found using <column> = <value>"`.

**`Start(config)`**: set logger prefix (`[mysql]`/`[sqlserver]`/`[snowflake]`); connect; store `registry`, `db`, `ctx`. Snowflake additionally stores `config`, sets `bulkRecordModifications`, creates `batcher = util.NewBatcher()`.

**`Stop()`** (guarded by `sync.Once`):
- MySQL/SQLServer: `waitGroup.Wait()`, then if `db != nil` close and nil it. **Does NOT flush.**
- Snowflake: **calls `Flush(logger)` first**, then `waitGroup.Wait()`, then lock `locker`, close db, nil it. (Behavioral difference — Snowflake flushes on stop.)

**`Test(ctx, logger, url)`**: set logger prefix, `connectToDB`, then `db.Close()` (returns its error).

**`Configuration()`**: `internal.NewDatabaseConfiguration(port)` → fields `Database`(required), `Username`(optional), `Password`(optional password), `Hostname`(required), and `Port`(optional number, default=port) **only if port>0**. Snowflake passes `-1` so no Port field.

**`Validate(values)`**: `internal.URLFromDatabaseConfiguration(scheme, port, values)` builds a URL `scheme://user:pass@host:port/database` (host without `:port` when port≤0), then `url.QueryUnescape` the whole string and returns it.

**`Help()`**: returns `util.GenerateHelpSection("Schema", "The database will match the public schema from the Shopmonkey transactional database.\n")` — green title + bold body via `fatih/color`.

### 3.1 Process / Flush (streaming)

**MySQL & SQLServer `Process(logger, event)`**:
1. `waitGroup.Add(1)`/`defer Done()`.
2. `_, version = registry.GetTableVersion(event.Table)` (error → wrapped, return false).
3. `schema = registry.GetSchema(event.Table, version)`.
4. `sql = toSQL(event, schema)`.
5. `pending.WriteString(sql)`; `count++`.
6. Always returns `(false, nil)` — never asks the consumer to flush (flush cadence is driven externally by `MaxBatchSize`).

**MySQL & SQLServer `Flush(logger)`**:
1. `waitGroup.Add(1)`/`defer Done()`.
2. If `count > 0`: `db.BeginTx(ctx, nil)`; `defer` rollback unless `success`; `tx.ExecContext(ctx, pending.String())` (on error log `"offending sql: %s"` and return wrapped error); `tx.Commit()`; set `success=true`.
3. Always `pending.Reset()`, `count = 0` at the end (even on the no-op path).

**Snowflake `Process(logger, event)`**: `waitGroup.Add/Done`; `batcher.Add(&event)`; return `(false, nil)`. The batcher (`util.Batcher`) appends a `*util.Record{Table, Id=event.GetPrimaryKey(), Operation, Diff, Object=event.GetObject(), Event}`.

**Snowflake `Flush(logger)`** (the most complex; reproduce exactly):
1. Lock `locker` (defer unlock). If `db == nil` → return `internal.ErrDriverStopped` ("driver stopped").
2. `waitGroup.Add(1)`/`defer Done()`.
3. `records = batcher.Records()`; `count = len(records)`; `batcher.Clear()`.
4. If `count > 0`:
   - `sequence++` (global, non-atomic).
   - `records = runBulkRecordModifications(records)` → applies in order: `SortRecordsByMVCCTimestamp` (stable-ish sort ascending by `strconv.ParseFloat(Event.MVCCTimestamp)`; parse errors treated as 0), then `CombineRecordsWithSamePrimaryKey`.
   - `tag = fmt.Sprintf("eds-%s/%d/%d", sessionID, sequence, count)`; `ctx = sf.WithQueryTag(context.Background(), tag)`.
   - For each record, build per-operation:
     - **INSERT**: `key = fmt.Sprintf("snowflake:%s:%s", record.Table, record.Id)`; `ok,_,_ = config.Tracker.GetKey(key)`; `force = ok`. (If we've previously seen an insert for this PK within 24h, force a delete-before-insert.)
     - **UPDATE**: skip the record entirely (`continue`) if the diff is "noise": `len(Diff)==1 && Diff[0]=="updatedDate"`, OR `len(Diff)==2 && contains "updatedDate" && contains "meta"`, OR `len(Diff)==0`.
     - **DELETE**: `key = "snowflake:<table>:<id>"`; append to `deletekeys`.
   - `version = registry.GetTableVersion`; `schema = registry.GetSchema`.
   - `sql, c = toSQL(record, schema, force)`; `statementCount += c`; append `sql` to query builder; if `key != ""` append to `cachekeys`.
   - If `statementCount > 0`: `execCTX = sf.WithMultiStatement(ctx, statementCount)`; time the `db.ExecContext(execCTX, query.String())`; on error return `"unable to run query: <sql>: <err>"`; read `RowsAffected`; `addProfilingRecord(duration, count)`; if `rows != statementCount` log a **Warn** (known issue per inline TODOs), else Trace.
   - If `cachekeys` non-empty: `config.Tracker.SetKeys(cachekeys, tag, 24h)` (`time.Hour*24`).
   - If `deletekeys` non-empty: `config.Tracker.DeleteKey(deletekeys...)`.
5. Log profiling info.

**`addProfilingRecord(duration, recordsProcessed)`**: running average:
`newAverage = (oldAverage.Nanoseconds()*oldCount + duration.Nanoseconds()) / newCount` (integer ns division), `newCount = oldCount + recordsProcessed`.

**`SetSessionID(sessionID)`** (Snowflake): if non-empty and matches `uuidRegexp`, set `sessionID` and `ctx = sf.WithRequestID(config.Context, sf.ParseUUID(sessionID))`.

### 3.2 SQL generation per driver

#### Identifier quoting
- MySQL: `` `name` `` (backticks). `quoteIdentifier(v) = "`" + v + "`"`. **No internal escaping.**
- SQLServer: `[name]`. `quoteIdentifier(v) = "[" + v + "]"`. **No internal escaping.**
- Snowflake: `util.QuoteIdentifier(v) = `"` + v + `"`` (double quotes). **No internal escaping.**
- Postgres (reference): `pq.QuoteIdentifier` (double quotes with internal `"`→`""` doubling).

#### MySQL `toSQL(c, model)` / `toSQLFromObject`
- **DELETE**: `DELETE FROM <`table`> WHERE <`pk`>=<quoteValue(Key[i])> [AND …];\n` over `model.PrimaryKeys`.
- **Non-delete** → `toSQLFromObject(c.Operation, model, c.Table, c, c.Diff)`:
  - Statement: `REPLACE INTO <`table`> (<cols joined by ",">) VALUES (<insertVals joined by ",">);\n`. Columns = all `model.Columns()` quoted.
  - For each column, value = `o[name]` if present, else literal `"NULL"`, passed through `util.ToJSONStringVal(name, quoteValue(val)|"NULL", prop, true)` (quoteScalar=true).
  - **GOTCHA: `updateValues` is computed but never written** — `REPLACE INTO` does delete+insert, so only `insertVals` ends up in SQL. The `UPDATE`-branch diff handling is effectively dead code for MySQL.

#### SQLServer `toSQL(c, model)` / `toSQLFromObject`
- **DELETE**: same shape as MySQL but with `[ ]` quoting and `quoteValue(c.Key[i])`.
- **Non-delete**: `toSQL` unmarshals `c.After` into a fresh `map[string]any` (not via `GetObject`) then calls `toSQLFromObject(model, table, object, c.Diff)`.
- **`toSQLFromObject` builds a MERGE** (terminated with `;` — comment: "must be terminated for merge to work"):
  ```
  MERGE [table] AS target USING (VALUES('<object["id"]>')) AS source (id) ON target.id=source.id
   [WHEN MATCHED THEN UPDATE SET col=val,...]
   WHEN NOT MATCHED THEN INSERT (cols) VALUES (vals);
  ```
  - `object["id"].(string)` is asserted directly (panics if id missing/non-string).
  - UPDATE SET clause: if `len(diff)>0`, iterate diff (skip names not in `model.Columns()` or `=="id"`); else iterate all `model.Columns()` except `"id"`. Present value → `col=ToJSONStringVal(name, quoteValue(val), prop, false)`; missing → `col=NULL`. Clause omitted entirely if `updateValues` empty.
  - INSERT cols = all `model.Columns()` quoted. INSERT vals: present → `ToJSONStringVal(name, quoteValue(val), prop, false)`, then for non-id columns passed through `handleSchemaProperty(prop, v)`; missing → `handleSchemaProperty(prop, "NULL")`.
- **`handleSchemaProperty(prop, v)`** (SQLServer-only coercion):
  - `object`: if `AdditionalProperties != nil && *AdditionalProperties` → return v (else fall through to return v).
  - `boolean`: if `lower(v)=="true" || v=="1"` → `"1"`; if `(!Nullable && v=="") || lower(v)=="false" || lower(v)=="null"` → `"0"`. (Operator precedence: `!prop.Nullable && v == "" || lower=="false" || lower=="null"`.)
  - `integer`: if `v=="NULL"` → `"0"`.
  - `array`: if `!Nullable && v=="NULL"` → `"''"`.
  - default → v.

#### Snowflake `toSQL(record, model, exists)` → returns `(sql, statementCount)`
- If `exists || Operation=="DELETE"`: emit `toDeleteSQL(record)`, `count++`.
- If `Operation != "DELETE"`: emit `toMergeSQL(record, model)`, `count++`.
- So: forced INSERT → DELETE + MERGE (count 2); normal INSERT/UPDATE → MERGE (count 1); DELETE → DELETE (count 1).
- **`toDeleteSQL`**: `DELETE FROM "table" WHERE "id"=<quoteValue(record.Id, "")>;\n`.
- **`toMergeSQL`**:
  ```
  MERGE INTO "table" AS target USING (SELECT <id> AS "id", <after["updatedDate"]> AS "updatedDate") AS source
   ON target."id" = source."id"
   WHEN MATCHED AND source."updatedDate" > target."updatedDate" THEN UPDATE SET col=val,...
   WHEN NOT MATCHED THEN INSERT (cols) VALUES (vals);\n
  ```
  - `after, _ = record.Event.GetObject()` (error ignored). `quoteValue(after["updatedDate"], "")` and `quoteValue(record.Id, "")`.
  - For each `model.Columns()`: insert column quoted. If `record.Object[name]` present: `fn = generateInsertFunction(prop)`, `v = quoteValue(val, fn)`; this `v` is used for BOTH `updateValues` (`col=v`) and `insertVals`. If missing: insertVal = `nullableValue(prop, true)` (and the column is **omitted from updateValues**).
  - **`generateInsertFunction(prop)`**: `object` → `"PARSE_JSON"`; `array` → if `Items != nil && (Items.Type=="object" || Items.Type=="string")` → `"PARSE_JSON"` else `"TO_VARIANT"`; otherwise `""`.
  - **`nullableValue(prop, wrap)`**: `Nullable` → `"NULL"`; else `object` → `PARSE_JSON('{}')` (wrap) / `'{}'`; `array` → `PARSE_JSON('[]')` / `'[]'`; `number`/`integer` → `"0"`; `boolean` → `"false"`; default → `"''"`.

#### Value quoting (`quoteValue`) — DIFFERENCES FROM POSTGRES

Postgres reference: `nil`→`"null"`; numerics via `strconv`; `float` `'f'` fmt; `bool`→`"true"/"false"`; `[]byte`→`'\x<hex>'`; strings via `quoteString` (strips `\x00`; if empty or matches `safeCharacters` regex → `'str'`, else dollar-quote with `$_H_$`); `time.Time`→`Truncate(microsecond).Format("'2006-01-02 15:04:05.999999999Z07:00:00'")`; `[]string`→`pq.QuoteLiteral` each then `quoteString(JSONStringify)`; ptr/struct fallbacks via reflection + `JSONStringify`.

**MySQL `quoteValue`** (TiDB-derived, backslash escaping):
- `nil` → `"NULL"` (uppercase — differs from Postgres `"null"`).
- numerics: `strconv.AppendInt/AppendUint`; `float32/64` → `'g'` format (differs from Postgres `'f'`).
- `bool` → `'1'`/`'0'` (differs from Postgres `true`/`false`).
- `time.Time`/`*time.Time` → if zero `'0000-00-00'`, else `'2006-01-02 15:04:05.999999'` (6-frac, no timezone — differs from Postgres 9-frac+tz).
- `json.RawMessage`/`map[string]interface{}`/`[]interface{}` → `'` + backslash-escaped JSON + `'`.
- `[]byte` → nil→`NULL`, else `_binary'<escaped>'`.
- **`string`**: if matches `looksLikeJSONTimestamp` regex `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(.\d{1,})?Z$` → `time.Parse(time.RFC3339Nano)`; on parse error **panic**; if `Year()<1970` clamp to `1970-01-01 00:00:01 UTC`; emit `'2006-01-02 15:04:05.999999'`. Otherwise `'<backslash-escaped>'`.
- `[]string`/`[]float32`/`[]float64` → comma-joined quoted/formatted scalars (no surrounding parens).
- default: reflection for ptr-to-scalar kinds; else **panic** `"unsupported argument…"`.
- **Backslash escape table** (`escapeBytesBackslash`): `\x00`→`\0`, `\n`→`\n`, `\r`→`\r`, `\x1a`→`\Z`, `'`→`\'`, `"`→`\"`, `\`→`\\`.

**SQLServer `quoteValue`**: byte-for-byte identical to MySQL's **except** the single-quote escape: `'`→`''` (doubled), not `\'`. All other escapes still use backslash (`\0 \n \r \Z \" \\`). The comment block still says "sqlserver timestamp range…". This is a known oddity — SQL-standard quote doubling mixed with backslash escapes.

**Snowflake `quoteValue(value, fn)`**:
- Signature takes a wrapper function name `fn`.
- `nil`→`"NULL"`; int/`*int32`/`*int64`→`strconv.FormatInt` (nil ptr→`NULL`); `float32/64`/`*float64`→`'f'` format; `bool`/`*bool`→`strconv.FormatBool` (nil ptr→`NULL`); `string`→`quoteString(arg, fn)`; `time.Time`/`*time.Time`→`Truncate(microsecond).Format("'2006-01-02 15:04:05.999999999Z07:00:00'")` (nil ptr→`NULL`); `map[string]interface{}`→`quoteString(util.JSONStringify(arg), fn)`; default reflection (ptr→deref struct→JSONStringify, scalar→`fmt.Sprintf("%v")`; non-ptr→JSONStringify).
- **`quoteString(val, fn)`**: if `val=="NULL"` return `"NULL"` as-is; else escape `\`→`\\` then `'`→`''`; wrap in `'…'`; if `fn != ""` wrap as `fn('…')` (e.g. `PARSE_JSON('…')`). **No `\x00` stripping** (unlike Postgres).

### 3.3 `util.ToJSONStringVal(name, val, prop, quoteScalar)` (shared, applied to non-Snowflake values)
1. If `prop.IsArrayOrJSON()` (object/array) AND `prop.IsNotNull()` (`!Nullable || type=="array"`) AND `isEmptyVal(val)` → return `'[]'` (array) or `'{}'` (object). `isEmptyVal`: `val == "''" || "" || "NULL" || "null"`.
2. If `quoteScalar` → `quoteJSONScalar`: if `prop.Type=="object"` AND `val` matches `scalarValue` regex `^([+-]?([0-9]*[.])?[0-9]+)|(true|false)$` → `'<val>'`; else `val`.
3. Else return `val`. (MySQL & Postgres pass quoteScalar=true; SQLServer passes false.)

### 3.4 Type mapping `propTypeToSQLType`

| schema type | MySQL | SQLServer | Snowflake | Postgres (ref) |
|---|---|---|---|---|
| string (PK) | `VARCHAR(64)` | `VARCHAR(64)` | `STRING` | `TEXT` |
| string + format `date-time` | `TIMESTAMP` | `NVARCHAR(MAX)` | `TIMESTAMP_NTZ` | `TIMESTAMP WITH TIME ZONE` |
| string (other) | `TEXT` | `NVARCHAR(MAX)` | `STRING` | `TEXT` |
| integer | `BIGINT` | `BIGINT` | `INTEGER` | `BIGINT` |
| number | `FLOAT` | `FLOAT` | `FLOAT` | `DOUBLE PRECISION` |
| boolean | `BOOLEAN` | `BIT` | `BOOLEAN` | `BOOLEAN` |
| object | `JSON` | `NVARCHAR(MAX)` | `STRING` | `JSONB` |
| array + `Items.Enum != nil` | `VARCHAR(64)` | `VARCHAR(64)` | `STRING` | `VARCHAR(64)` |
| array (other) | `JSON` | `NVARCHAR(MAX)` | `VARIANT` | `JSONB` |
| default | `TEXT` | `NVARCHAR(MAX)` | `STRING` | `TEXT` |

MySQL/SQLServer/Postgres `propTypeToSQLType(prop, isPrimaryKey)` take the PK flag (only used for the string→VARCHAR(64) override). Snowflake's takes only `prop` (no PK-specific override).

### 3.5 DDL: `createSQL`

Common column ordering: non-PK columns collected, `sort.Strings` ascending, then prepended with `PrimaryKeys`. `NOT NULL` appended when `util.SliceContains(Required, name) && !prop.Nullable`.

- **MySQL**: `DROP TABLE IF EXISTS <`t`>;\n` + `CREATE TABLE <`t`> (\n` + per-col `\t`name` <type>[ NOT NULL],\n` + PK clause: if PKs → `\tPRIMARY KEY (`pk`, …)` else `\tPRIMARY KEY (id)` (literal unquoted `id`) + `\n) CHARACTER SET=utf8mb4;\n`.
- **SQLServer**: `DROP TABLE IF EXISTS [t];\n` + `CREATE TABLE [t] (\n` + cols + PK clause **only if PKs exist** (no fallback) + `\n)` (no trailing `;`).
- **Snowflake**: `CREATE OR REPLACE TABLE "t" (\n` (no DROP) + cols + PK clause only if PKs + `\n);\n`.
- **Postgres** (ref): `DROP TABLE IF EXISTS "t";\n CREATE TABLE "t" (…) \n);\n` PK only if PKs.

### 3.6 DDL: `addNewColumnsSQL(logger, columns, schema, dbschema)`
For each column: if `dbschema.GetType(table, column)` already exists → `logger.Warn("skipping migration for column: %s for table: %s since it already exists")` and skip. Else emit one `ALTER TABLE` statement:
- MySQL: `ALTER TABLE <`t`> ADD COLUMN <`c`> <type>;`
- SQLServer: `ALTER TABLE [t] ADD [c] <type>;` (note: `ADD`, not `ADD COLUMN`)
- Snowflake: `ALTER TABLE "t" ADD COLUMN "c" <type>;`

### 3.7 Migration methods
- **`MigrateNewTable(ctx, logger, schema)`**: `waitGroup.Add/Done`; if table already in `dbschema` → log `"table already exists for: %s, dropping and recreating..."` and `util.DropTable(ctx, logger, db, quoteIdentifier(Table))` (`DROP TABLE IF EXISTS <quoted>`). **Snowflake additionally** deletes tracker cache keys: `config.Tracker.DeleteKeysWithPrefix("snowflake:" + Table + ":")` and logs count. Then `db.ExecContext(ctx, createSQL(schema))` and `refreshSchema(ctx, db, true)` (failIfEmpty=true).
- **`MigrateNewColumns(ctx, logger, schema, columns)`**: `waitGroup.Add/Done`; for each statement from `addNewColumnsSQL` run `db.ExecContext`; then `refreshSchema(…, true)`.
- **`GetDestinationSchema`**: returns cached `dbschema`.

### 3.8 Import paths

**MySQL & SQLServer `Import(config)`** delegate to `importer.Run(logger, config, p)` after: setting logger prefix, `connectToDB` (deferred close), storing `registry`/`importConfig`, `executor = util.SQLExecuter(ctx, logger, db, config.DryRun)` (in dry-run logs `"[dry-run] %s"` and skips execution), resetting `pending`/`count`/`size`.
- `importer.Run`: gets latest schema; if `!NoDelete` → `handler.CreateDatasource(schema)`; if `SchemaOnly` return; lists `DataDir`; for each file matching `ParseCRDBExportFile` (regex `^(\d{33})-\w+-[\w-]+-([a-z0-9_]+)-(\w+)\.ndjson\.gz`) whose table is in `config.Tables`, NDJSON-decodes records into synthetic `DBChangeEvent`s (Operation `"INSERT"`, ID=`util.Hash(filename)`, Timestamp/MVCCTimestamp from the parsed file date, Key from primary key, optional schema validation), and calls `handler.ImportEvent`. Finally `handler.ImportCompleted()`.
- **`CreateDatasource(schema)`**: for each `importConfig.Tables` run `executor(createSQL(schema[table]))`.
- **`ImportEvent`**: build SQL, append to `pending`, `count++`, `size += len(sql)`; **flush when `size >= 5_000_000` OR `importConfig.Single`** via `executor(pending.String())` then reset `pending`/`size`.
  - MySQL: `toSQLFromObject("INSERT", data, event.Table, event, nil)` → a `REPLACE INTO` (the `"INSERT"` op string only matters for the dead update branch).
  - SQLServer: `object = event.GetObject()`; `toSQLFromObject(schema, event.Table, object, nil)` → MERGE.
- **`ImportCompleted`**: if `size > 0` run `executor(pending.String())`.

**Snowflake `Import(config)`** is fully custom (does NOT use `importer.Run`, does NOT implement `importer.Handler`):
1. logger prefix; `getConnectFunc()(ctx, url)` (deferred close); `executeSQL = util.SQLExecuter`.
2. `schema = registry.GetLatestSchema()`; for each `config.Tables` run `executeSQL(createSQL(schema[table]))`.
3. If `SchemaOnly` return.
4. `jobId = config.JobID` or `util.Hash(time.Now().UnixNano())` if empty.
5. `stageName = "eds_import_" + jobId`; `executeSQL("CREATE STAGE " + stageName)`.
6. `parallel = config.MaxParallel`, clamped to `[1, 99]` (`<=0`→1, `>99`→99).
7. `fileURI = util.ToFileURI(config.DataDir, "*.ndjson.gz")` (cross-platform `file://…`; on Windows backslashes→slashes).
8. `executeSQL(fmt.Sprintf("PUT '%s' @%s PARALLEL=%d SOURCE_COMPRESSION=gzip", fileURI, stageName, parallel))`.
9. Concurrent per-table COPY with `golang.org/x/sync/semaphore` weight **4**; each goroutine `defer util.RecoverPanic` (on panic → log + `os.Exit(2)`), acquire, run:
   `COPY INTO "<table>" FROM @<stage> MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = true COMPRESSION = 'GZIP') PATTERN='.*-<table>-.*'`; errors pushed to a buffered channel.
10. `wg.Wait()`; drain error channel; `executeSQL("DROP STAGE " + stageName)`; return `errors.Join(errs...)` if any.

### 3.9 Connection-string construction

**MySQL `ParseURLToDSN`** (go-sql-driver DSN `user:pass@tcp(host)path?params`):
- `url.Parse`; force `multiStatements=true`; if user present `util.ToUserPass(u)+"@"`; then `tcp(<host>)` + `<path>` + `?` + `vals.Encode()`. (`ToUserPass` = `user` or `user:pass`.)

**SQLServer `ParseURLToDSN`** (`sqlserver://user:pass@host?params`):
- `url.Parse`; if `util.IsLocalhost(host)` and `encrypt` unset → `encrypt=disable`; if `app name` unset → `app name=eds`; rebuild `sqlserver://` + userpass + host; if path non-empty move it to query as `database=<path[1:]>`; append `?<encoded>` if any.

**Snowflake `GetConnectionStringFromURL`** (gosnowflake DSN `user:pass@host/path?params`):
- `url.Parse`; `user.String()+"@"` if user; host; ensure path begins with `/`; set `client_session_keep_alive=true` and `application=eds`; append `?<encoded>`.

**Snowflake `connectToDB`**: `GetConnectionStringFromURL`; `sql.Open("snowflake", …)`; `db.QueryRowContext(ctx, "SELECT 1")` (check `row.Err()`); `RefreshSchema(…, false)`.

### 3.10 Snowflake Key-Pair specifics
- `ExampleURL` = `snowflake-keypair://user@account/database/schema?secret-key=SECRET_ENV_VAR_NAME`.
- `Configuration()`: fields `Database`(required, default `"DBNAME/SCHEMA"`), `Username`(required), `Account`(required, default `"abcdefg-ab12345"`), `Secret`(optional, default `"SNOWFLAKE_SECRET_ACCESS_KEY"`).
- `Validate`: requires Account/Database/Username; builds `url.URL{Scheme:"snowflake-keypair", User: url.User(username), Host: account, Path: database, RawQuery: secret-key=<secret>}`; returns errors if any.
- `connectToDBWithKeyPair(ctx, urlString)`: parse; `username = User.Username()`, `account = Host`; split `TrimPrefix(Path,"/")` by `/`, require `len >= 2` (else error "invalid URL path: expected /database/schema…"); `database=parts[0]`, `schema=parts[1]`; `secret = os.Getenv(query.Get("secret-key"))`, fallback `os.Getenv("SNOWFLAKE_SECRET_ACCESS_KEY")`; `OpenSnowflakeWithKeyPair`; `SELECT 1`; `RefreshSchema(…, false)`.
- `OpenSnowflakeWithKeyPair`: `pem.Decode(secret)` (nil → "failed to decode secret"); `x509.ParsePKCS8PrivateKey`; assert `*rsa.PrivateKey` (else "private key is not RSA"); `sf.Config{User, Account, Database, Schema, PrivateKey, Authenticator: sf.AuthTypeJwt}`; `sf.NewConnector` + `sql.OpenDB`.
- `Start`/`Import` inject `connectToDBFunc = connectToDBWithKeyPair` then delegate to embedded `snowflakeDriver`. `Test` sets prefix `[snowflake-keypair]` (comment: "Have to have this or RefreshSchema panics").

---

## 4. External dependencies

| Go package | Role | .NET / C# equivalent |
|---|---|---|
| `database/sql` + `github.com/go-sql-driver/mysql` | MySQL connectivity, DSN `user:pass@tcp(host)/db?multiStatements=true` | `MySqlConnector` NuGet (`MySqlConnection`) — set `AllowUserVariables`/multi-statement via batched command text |
| `github.com/microsoft/go-mssqldb` | SQL Server connectivity (`sqlserver://`) | `Microsoft.Data.SqlClient` (`SqlConnection`) |
| `github.com/snowflakedb/gosnowflake` (`sf`) | Snowflake driver; `WithRequestID`, `WithQueryTag`, `WithMultiStatement`, `Config`, `NewConnector`, `AuthTypeJwt`, `ParseUUID` | `Snowflake.Data` (ADO.NET) NuGet; multi-statement via `MULTI_STATEMENT_COUNT`; JWT key-pair auth supported in connection string |
| `golang.org/x/sync/semaphore` | Weighted semaphore (weight 4) bounding concurrent COPY INTO | `System.Threading.SemaphoreSlim(4,4)` |
| `crypto/rsa`, `crypto/x509`, `encoding/pem` | Parse PKCS#8 RSA private key for Snowflake JWT | `System.Security.Cryptography` (`RSA.ImportPkcs8PrivateKey`, `PemEncoding`) |
| `encoding/json` | (un)marshal `After`/`Before`, JSON values | `System.Text.Json` (`JsonSerializer`); preserve property order & number formatting carefully |
| `regexp` | `looksLikeJSONTimestamp`, `uuidRegexp`, `scalarValue`, CRDB file regex | `System.Text.RegularExpressions.Regex` |
| `strconv` | int/uint/float formatting (`'g'` MySQL/SQLServer, `'f'` Snowflake/Postgres) | `long.ToString`, `double.ToString` — **must replicate Go's `'g'`/`'f'` shortest-round-trip semantics** (see gotchas) |
| `time` | timestamp formatting/truncation; ping timeout | `DateTime`/`DateTimeOffset`, `CancellationTokenSource(TimeSpan)` |
| `reflect`, `unsafe` | reflection fallback in `quoteValue`; zero-copy string→bytes | C# `object` type switch / `Convert`; ignore `unsafe` (use `Encoding.UTF8.GetBytes`) |
| `github.com/shopmonkeyus/go-common/logger` | structured logger with `WithPrefix`, Trace/Debug/Info/Warn/Error | `ILogger`/`ILoggerFactory` with scope or prefix wrapper |
| `github.com/fatih/color` (via `util.help`) | colored help text | `Spectre.Console` or ANSI strings |
| `github.com/cespare/xxhash/v2` (via `util.Hash`) | xxhash for import IDs/job IDs | `System.IO.Hashing.XxHash64` |
| `github.com/lib/pq` (Postgres ref only) | `QuoteIdentifier`, `QuoteLiteral` | Npgsql; not needed for these 3 |
| `internal/tracker` (Snowflake) | local KV store: `GetKey`, `SetKeys(keys, val, 24h)`, `DeleteKey`, `DeleteKeysWithPrefix` | Embedded KV (SQLite/LiteDB) with TTL semantics |

---

## 5. Edge cases & gotchas

- **String-interpolated SQL everywhere.** No parameter binding — every value goes through `quoteValue`. The C# port MUST reproduce the escaping byte-for-byte or risk both injection and behavioral divergence. `information_schema` queries also interpolate `dbname`/schema unparameterized.
- **`nil` casing differs by driver**: MySQL/SQLServer/Snowflake emit `"NULL"`; Postgres emits `"null"`. Don't normalize.
- **Boolean rendering differs**: MySQL/SQLServer → `1`/`0`; Snowflake/Postgres → `true`/`false`. SQLServer additionally coerces booleans in `handleSchemaProperty`.
- **Float format differs**: MySQL/SQLServer use Go `'g'` (shortest, may use exponent); Snowflake/Postgres use `'f'` (no exponent). Replicate exactly — Go's float formatting is shortest-round-trip; naive C# `ToString()` (default `"G17"`/round-trip) will differ. Use the matching .NET format specifier and culture-invariant formatting.
- **SQLServer escape quirk**: single quotes are doubled (`''`) but `\0 \n \r \Z \" \\` are still backslash-escaped — a deliberate hybrid. Reproduce precisely.
- **String timestamp coercion (MySQL/SQLServer)**: any string matching `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(.\d{1,})?Z$` is reparsed and reformatted to `yyyy-MM-dd HH:mm:ss.ffffff`; **parse failure panics**; years <1970 are clamped to `1970-01-01 00:00:01 UTC`. Regex flaw: the `.` before fractional seconds is unescaped (matches any char). Note the format uses 6 fractional digits with Go's trailing-zero trimming (`.999999`).
- **Time formatting**: Go layout `2006-01-02 15:04:05.999999` trims trailing zero fractional seconds (`.999999` is "optional"); Snowflake/Postgres `…999999999Z07:00:00` includes timezone. C# must emulate trailing-zero trimming (Go `.9` semantics ≠ C# `f` which trims, but verify `ffffff` vs `FFFFFF`).
- **MySQL dead update code**: `updateValues` computed in `toSQLFromObject` is never emitted (`REPLACE INTO` is delete+insert). Porting the update branch is unnecessary for output correctness but harmless; the `UPDATE` op produces the exact same `REPLACE INTO` as an INSERT.
- **SQLServer `object["id"].(string)`** is an unchecked type assertion → panics if `id` is missing or non-string. Same risk: Snowflake `quoteValue` `default` and MySQL/SQLServer `quoteValue` `default` **panic** on unsupported types.
- **`Columns()` ordering**: PKs first (in PK slice order), then remaining columns sorted ascending. Cached after first call. `createSQL` re-sorts independently but yields the same order. The C# port must produce identical column ordering for stable SQL.
- **Snowflake update-noise skip**: UPDATE events whose diff is exactly `["updatedDate"]`, exactly `{"updatedDate","meta"}`, or empty are dropped entirely. Easy to miss.
- **Snowflake MERGE is timestamp-gated**: `WHEN MATCHED AND source.updatedDate > target.updatedDate` — stale updates are silently ignored at the DB. Requires `updatedDate` in the source object.
- **Snowflake force-delete-before-insert** depends on the tracker cache (24h TTL keyed `snowflake:<table>:<id>`). Cache keys are set after successful exec; delete keys removed; table migration purges keys by prefix. The cache state materially changes generated SQL (count of statements). The port needs an equivalent persistent tracker.
- **`statementCount` must match `WithMultiStatement`** — Snowflake requires the exact statement count; a mismatch errors. Forced inserts contribute 2. The `RowsAffected != statementCount` warning is expected/known (see inline TODOs) and must not be treated as an error.
- **Global `sequence int64`** is shared across all Snowflake instances and incremented non-atomically under a per-instance mutex — a latent race if multiple Snowflake drivers run in-process. In C# use an instance field or `Interlocked`.
- **Stop() flush asymmetry**: Snowflake flushes on Stop; MySQL/SQLServer do not (rely on the consumer to flush before stop). `sync.Once` guards Stop.
- **`Flush` after Stop**: Snowflake returns `ErrDriverStopped` if `db==nil`. Reproduce this sentinel.
- **`refreshSchema` `failIfEmpty`**: `false` during connect (empty DB allowed), `true` after migrations (must see tables).
- **Ping timeout** is hard-coded 5s (MySQL/SQLServer). Snowflake uses `SELECT 1` with no explicit timeout.
- **`util.RecoverPanic`** (Snowflake import goroutines) logs and calls `os.Exit(2)` — a panic in a COPY goroutine kills the whole process. A C# port should decide whether to mirror process-exit or convert to exceptions.
- **`ToFileURI`** is OS-aware: on Windows it converts backslashes to forward slashes and produces `file://C:/…`. The Snowflake `PUT` depends on this. C# must build the same `file://` URI form.
- **`IsLocalhost`** is a substring check for `localhost`/`127.0.0.1`/`0.0.0.0` (affects MySQL nothing, SQLServer `encrypt=disable`, Postgres `sslmode=disable`).
- **`ToJSONStringVal` empty-object/array coercion** only triggers for object/array props that are not-null and have an empty value, producing `'{}'`/`'[]'`. `IsNotNull` treats arrays as always not-null. Subtle interaction with `prop.Nullable`.
- **`quoteJSONScalar`** only quotes scalars for `object`-typed columns (wraps bare numbers/booleans in quotes for JSON columns). MySQL/Postgres apply it; SQLServer does not (passes `quoteScalar=false`).

---

## 6. C# port notes

- **Structure**: Define an abstract `SqlDriverBase` capturing the shared MySQL/SQLServer lifecycle (`Start`/`Stop`/`Process`/`Flush`/`Import` via an `ImporterRunner`, migration). MySQL and SQLServer differ only in: identifier quoting, `quoteValue` escaping, `propTypeToSQLType`, `createSQL` tail, the per-row statement (`REPLACE INTO` vs `MERGE`), and DSN building. Snowflake is different enough (batcher + MERGE-with-timestamp + tracker cache + custom import via stage/COPY) to warrant its own class; the key-pair variant is a subclass that swaps the connect function.
- **SQL builders**: keep them as pure `static string` functions operating on schema + object/event, exactly mirroring the Go `strings.Builder` concatenation (including `\n`, `\t`, trailing `;`, and the `;` terminator the SQLServer MERGE requires). Write golden-output unit tests comparing C# output to captured Go output for representative events of every operation/type.
- **Value quoting** is the highest-risk area. Implement one `QuoteValue` per driver as an explicit type switch over `object` matching Go's order; replicate: `null`→`NULL`/`null` casing, integer/unsigned handling, float `'g'` vs `'f'`, bool `1/0` vs `true/false`, byte arrays (`_binary'…'` MySQL/SQLServer; `'\x<hex>'` Postgres), `time` formats, JSON marshaling for maps/arrays, and the panic-on-unsupported behavior (throw). Port the backslash escaper (`escapeBytesBackslash`) and the SQLServer single-quote-doubling variant as separate functions. Use `CultureInfo.InvariantCulture` everywhere.
- **Go float `'g'`/`'f'` shortest round-trip**: .NET `double.ToString("R")`/`"G17"` are not equivalent. Use the shortest-round-trippable form: in modern .NET, `double.ToString(CultureInfo.InvariantCulture)` is shortest-round-trip, but you must then decide exponent vs fixed to match `'g'` vs `'f'`. Add focused tests for edge values (very large/small, integers-as-floats, negatives, NaN/Inf — Go would `'g'`-format these).
- **JSON serialization**: `System.Text.Json` must match Go `encoding/json` ordering/escaping for the JSON-column cases. Go marshals maps with **sorted keys**; `System.Text.Json` preserves insertion order for `JsonObject`/dictionaries — sort keys explicitly to match. Watch HTML escaping (`<`,`>`,`&`) — disable it (`JavaScriptEncoder.UnsafeRelaxedJsonEscaping`) to match Go's default non-HTML-escaping inside SQL string literals.
- **Multi-statement execution**: MySQL needs multi-statement enabled (`MySqlConnector` allows multiple statements in one command when the server permits / `AllowUserVariables`); SQLServer executes a batch directly; Snowflake requires setting the statement count (`MULTI_STATEMENT_COUNT` parameter on the command) equal to the computed `statementCount`. Transactions: MySQL/SQLServer use `BeginTransaction`/`Commit`/`Rollback` mirroring the `success` flag pattern (`try/catch`+`finally`).
- **Concurrency**: replace `sync.WaitGroup` with task tracking; `sync.Once` with `Lazy`/a guarded flag; the Snowflake `locker` mutex with `SemaphoreSlim(1,1)` or `lock`; the import semaphore with `SemaphoreSlim(4,4)`. Make `sequence` an instance field (`Interlocked.Increment`).
- **Tracker**: Snowflake correctness depends on a persistent KV cache with TTL and prefix-delete. Port `tracker` (likely SQLite-backed) before/with the Snowflake driver. The `snowflake:<table>:<id>` key format and 24h TTL are load-bearing.
- **Key-pair auth**: use `Snowflake.Data` with `AUTHENTICATOR=SNOWFLAKE_JWT` and `PRIVATE_KEY`; import PKCS#8 via `RSA.ImportPkcs8PrivateKey`. Reproduce the env-var fallback (`secret-key` query param → its value is an **env var name**, then fallback to `SNOWFLAKE_SECRET_ACCESS_KEY`).
- **Risks to flag**: the SQLServer hybrid escaping and unchecked `id` cast; MySQL/SQLServer timestamp-string panic + <1970 clamp; Go float formatting fidelity; JSON key ordering; the Snowflake update-noise skip and timestamp-gated MERGE (changing these silently changes data); and the `os.Exit(2)` panic behavior in Snowflake import goroutines (decide whether to mirror).