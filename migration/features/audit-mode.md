# Audit / Append-Trail Ingest Mode — Cross-Port SQL ORACLE

> ============================================================================
> **THIS IS NOT A GO-PARITY TARGET.**
> Go (`edsGolang`) does NOT have an append/audit mode. This is the FIRST
> intentional, deliberate divergence from the Go oracle. For this feature the
> Go source is **not** the source of truth — **this document is**. The two
> ports (eds-python, eds-dotnet) MUST emit the byte-identical SQL specified
> here, and their unit tests assert against the golden vectors in section 3.
> Do NOT "fix" these strings to match anything in `edsGolang`.
> ============================================================================

## 0. The `FEATURE:` marker convention

Every new symbol, branch, or test introduced for this feature is tagged in
source with a `FEATURE:` marker comment (the deliberate counterpart of the
existing `PARITY:` / `DEVIATION:` markers), naming this feature so the
non-Go-parity code is greppable and auditable:

- Python: `# FEATURE(audit-mode): <note>`
- .NET:   `// FEATURE(audit-mode): <note>`

Rule of thumb: `PARITY:` = faithful Go port; `DEVIATION:` = forced
implementation difference of a Go behavior; `FEATURE:` = net-new behavior
with **no** Go counterpart (this document). A reviewer can `grep -r
"FEATURE(audit-mode)"` to see the entire blast radius in each port.

---

## 1. Behavior contract

### 1.1 The `--mode` flag

| Aspect | Contract |
|---|---|
| Flag | `--mode <value>` on the consumer/run command |
| Values | `upsert` (legacy, the Go behavior) and `append` (this feature) |
| Default | `upsert` |
| Config key | `mode` (top-level basic string in `<data-dir>/config.toml`) |
| Precedence | **explicit `--mode` flag > `mode` in config.toml > built-in default `upsert`** |

Persistence rules (identical in both ports):

1. **Explicit `--mode X` given** → use `X` AND persist it: `set_config_value(data_dir, "mode", X)` (py) / `EdsConfig.SetValue(dataDir, "mode", X)` (.NET). This is read-modify-write of config.toml, preserving `token`/`server_id`/`url`.
2. **No `--mode`, but `mode` present in config.toml** → use the config value; do NOT rewrite config.
3. **No `--mode` and no `mode` key in config.toml** → default to `upsert` AND write it back (`mode = "upsert"`), so the persisted config is self-documenting after first run.
4. Unknown `--mode` value → usage error, exit `EXIT_INCORRECT_USAGE` (3), same code path as other bad-flag exits.

This mirrors the existing viper `BindPFlag` flag-over-config merge already used
for `token`/`url` (`cli_value or config.get_string(key)`); `mode` is just one
more bound key. Resolved mode is threaded into the driver as a single
boolean/enum (`append_mode` / `AppendMode`) — drivers branch on that, never
re-read the flag.

### 1.2 Audit columns (all `_eds_`-prefixed, appended AFTER the object columns)

Fixed order, every driver:

| Column | Role | Per-driver type (PG / MySQL / MSSQL / Snowflake) |
|---|---|---|
| `_eds_seq` | surrogate PRIMARY KEY (DB identity); replaces the object-id PK so many rows per id are allowed; NEVER supplied on INSERT | `BIGINT GENERATED ALWAYS AS IDENTITY` / `BIGINT NOT NULL AUTO_INCREMENT` / `BIGINT IDENTITY(1,1)` / `NUMBER AUTOINCREMENT` |
| `_eds_operation` | `'INSERT'` / `'UPDATE'` / `'DELETE'` | `TEXT NOT NULL` / `VARCHAR(16) NOT NULL` / `NVARCHAR(16) NOT NULL` / `STRING NOT NULL` |
| `_eds_mvcc_timestamp` | PRIMARY order key (CRDB HLC decimal); NULLABLE, NULL sorts LAST | `NUMERIC(38,10)` / `DECIMAL(38,10)` / `DECIMAL(38,10)` / `NUMBER(38,10)` |
| `_eds_timestamp` | tie-break (event epoch) | `BIGINT` / `BIGINT` / `BIGINT` / `NUMBER` |
| `_eds_appended_at` | server wall-clock; omitted from INSERT (DEFAULT) | `TIMESTAMP WITH TIME ZONE ... DEFAULT now()` / `TIMESTAMP(6) ... DEFAULT CURRENT_TIMESTAMP(6)` / `DATETIME2(6) ... DEFAULT SYSUTCDATETIME()` / `TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()` |

Object columns: keep their existing upsert types and `columns()` order, but
become **NULLABLE except the primary key(s)** (so a DELETE tombstone carrying
only the key is accepted; and nullable cols let NULL pass through
`to_json_string_val` instead of being coerced to `'{}'`/`'[]'`).

`columns()` for the worked schema (PK-first, then lexicographically sorted) =
**`id, meta, name, total`** on every driver — reused verbatim in the DDL,
every INSERT column list, and both views.

### 1.3 Two views per table (both plain, non-materialized)

- **`<table>_current`** — latest row per object id, EXCLUDING objects whose latest change is a DELETE; projects ONLY the object columns in `columns()` order, so it is row-for-row equal to the legacy upsert table. Order key: `_eds_mvcc_timestamp DESC (NULLS LAST), _eds_timestamp DESC, _eds_seq DESC`.
- **`<table>_timeline`** — SCD-Type-2 point-in-time: every change row (INCLUDING deletes — a delete is a validity endpoint) plus `valid_from = _eds_mvcc_timestamp` and `valid_to = LEAD(_eds_mvcc_timestamp) OVER (PARTITION BY <pk> ORDER BY mvcc ASC, ts ASC, seq ASC)` (NULL = still valid). Projects object columns + `_eds_operation` + `valid_from` + `valid_to`.
- Point-in-time query: `WHERE id = X AND T >= valid_from AND (valid_to IS NULL OR T < valid_to)`.

### 1.4 Write semantics (all drivers)

Plain INSERT per change — **NO** upsert / ON CONFLICT / REPLACE / MERGE /
delete-before-insert. INSERT and UPDATE both emit the **full after-snapshot**
(only the `_eds_operation` literal differs); DELETE emits a tombstone built
from the before-image: PK value(s) + `'DELETE'`, every other object column
`NULL`. `_eds_seq` (identity) and `_eds_appended_at` (default) are never in the
column list. `_eds_mvcc_timestamp` is emitted as a **bare numeric literal**
(NOT through `quote_value`, which would single-quote it as text);
`_eds_timestamp` via the normal int path.

---

## 2. Reused builder helpers (the byte-identity anchor)

The append SQL is grounded in — and reuses verbatim — the EXISTING per-driver
upsert builders, so it reads as a sibling of the upsert SQL. No new
quoting/typing logic is introduced (the ONE new coercion is the bare-mvcc
literal). NOTE (Snowflake, verified in code): object columns are typed **STRING**
(`prop_type_to_sql_type` returns `STRING` for object — sql.py:147; `VARIANT` is the
ARRAY branch only), and the INSERT value is wrapped with `PARSE_JSON`
(`generate_insert_function`) which Snowflake casts into the STRING column. Append
REUSES BOTH verbatim — column **STRING** + `PARSE_JSON` value via `INSERT … SELECT`
(PARSE_JSON is illegal in a `VALUES` list) — so it matches upsert's stored bytes.
This is "match upsert" (the user's choice); append adds nothing but the audit
columns + views on ALL four drivers.

| Driver | Builders (py / .NET) | Idents | Value quoting | Float |
|---|---|---|---|---|
| PostgreSQL | `drivers/postgresql/sql.py` / `PostgresqlSql.cs` | `"x"`, `"`→`""` | `quote_value` + `to_json_string_val` (empty→`'{}'`/`'[]'`) | `format_f` |
| MySQL | `drivers/mysql/sql.py` / `MysqlSql.cs` | `` `x` ``, no escape | `quote_value` (backslash-escape) + JSON sorted-keys | `format_g` |
| SQL Server | `drivers/sqlserver/sql.py` / `MssqlSql.cs` | `[x]`, no escape | `quote_value` hybrid (`'`→`''`, `"`→`\"`), bool `1/0` | `format_g` |
| Snowflake | `drivers/snowflake/sql.py`+`snowflake.py` / `SnowflakeSql.cs`+`SnowflakeDriver.cs` | `"x"`, no escape | `quote_value` (`\`→`\\`, `'`→`''`) + `generate_insert_function` (PARSE_JSON/TO_VARIANT) | `format_f` |

All four share `Schema.columns()` / `Columns()` (PK-first then sorted) and
`prop_type_to_sql_type` / `PropTypeToSqlType`.

---

## 3. GOLDEN SQL VECTORS (worked `order` example) — assert these byte-for-byte

Schema: table `order`, `primaryKeys=["id"]`, properties `id`(string,pk),
`name`(string), `total`(number/float), `meta`(object/json). `columns()` =
`["id","meta","name","total"]`.

> NOTE: each driver block reproduces its own grounded worked-example literal
> values (they differ per driver because each contract was authored against its
> own value-quoting witnesses); the STRUCTURE is identical. These exact strings
> are the unit-test fixtures for both ports.

### 3.1 PostgreSQL

**(a) Base DDL + index**
```sql
DROP TABLE IF EXISTS "order" CASCADE;
CREATE TABLE "order" (
	"id" TEXT NOT NULL,
	"meta" JSONB,
	"name" TEXT,
	"total" DOUBLE PRECISION,
	"_eds_seq" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	"_eds_operation" TEXT NOT NULL,
	"_eds_mvcc_timestamp" NUMERIC(38,10),
	"_eds_timestamp" BIGINT NOT NULL,
	"_eds_appended_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);
CREATE INDEX "order__eds_history_idx" ON "order" ("id", "_eds_mvcc_timestamp" DESC, "_eds_timestamp" DESC, "_eds_seq" DESC);
```

**(b) Three append INSERTs**
```sql
INSERT INTO "order" ("id","meta","name","total","_eds_operation","_eds_mvcc_timestamp","_eds_timestamp") VALUES ('o_123','{"k":"v"}','Widget',99.5,'INSERT',1717009183239076000.0000000000,1717009183239);
INSERT INTO "order" ("id","meta","name","total","_eds_operation","_eds_mvcc_timestamp","_eds_timestamp") VALUES ('o_123','{"k":"v"}','Widget',129,'UPDATE',1717009190000000000.0000000000,1717009190000);
INSERT INTO "order" ("id","meta","name","total","_eds_operation","_eds_mvcc_timestamp","_eds_timestamp") VALUES ('o_123',NULL,NULL,NULL,'DELETE',1717009200000000000.0000000000,1717009200000);
```

**(c) `_current`**
```sql
CREATE OR REPLACE VIEW "order_current" AS
SELECT "id","meta","name","total"
FROM (
	SELECT DISTINCT ON ("id")
		"id","meta","name","total","_eds_operation"
	FROM "order"
	ORDER BY "id", "_eds_mvcc_timestamp" DESC NULLS LAST, "_eds_timestamp" DESC, "_eds_seq" DESC
) "latest"
WHERE "_eds_operation" <> 'DELETE';
```

**(d) `_timeline`**
```sql
CREATE OR REPLACE VIEW "order_timeline" AS
SELECT
	"id","meta","name","total",
	"_eds_operation",
	"_eds_mvcc_timestamp" AS "valid_from",
	LEAD("_eds_mvcc_timestamp") OVER (
		PARTITION BY "id"
		ORDER BY "_eds_mvcc_timestamp" ASC, "_eds_timestamp" ASC, "_eds_seq" ASC
	) AS "valid_to"
FROM "order";
```

### 3.2 MySQL (8.0.14+)

**(a) Base DDL + index**
```sql
DROP VIEW IF EXISTS `order_timeline`;
DROP VIEW IF EXISTS `order_current`;
DROP TABLE IF EXISTS `order`;
CREATE TABLE `order` (
	`id` VARCHAR(64) NOT NULL,
	`meta` JSON,
	`name` TEXT,
	`total` FLOAT,
	`_eds_seq` BIGINT NOT NULL AUTO_INCREMENT,
	`_eds_operation` VARCHAR(16) NOT NULL,
	`_eds_mvcc_timestamp` DECIMAL(38,10),
	`_eds_timestamp` BIGINT,
	`_eds_appended_at` TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
	PRIMARY KEY (`_eds_seq`),
	KEY `order_eds_history_idx` (`id`, `_eds_mvcc_timestamp` DESC, `_eds_timestamp` DESC, `_eds_seq` DESC)
) CHARACTER SET=utf8mb4;
```

**(b) Three append INSERTs**
```sql
INSERT INTO `order` (`id`,`meta`,`name`,`total`,`_eds_operation`,`_eds_mvcc_timestamp`,`_eds_timestamp`) VALUES ('ord_1','{\"currency\":\"USD\"}','Widget',19.99,'INSERT',1735689600000123000.0000000000,1735689600000123);
INSERT INTO `order` (`id`,`meta`,`name`,`total`,`_eds_operation`,`_eds_mvcc_timestamp`,`_eds_timestamp`) VALUES ('ord_1','{\"currency\":\"USD\",\"tier\":\"pro\"}','Widget Pro',29.5,'UPDATE',1735689700000456000.0000000000,1735689700000456);
INSERT INTO `order` (`id`,`meta`,`name`,`total`,`_eds_operation`,`_eds_mvcc_timestamp`,`_eds_timestamp`) VALUES ('ord_1',NULL,NULL,NULL,'DELETE',1735689800000789000.0000000000,1735689800000789);
```

**(c) `_current`**
```sql
CREATE VIEW `order_current` AS
SELECT `id`, `meta`, `name`, `total`
FROM (
	SELECT `id`, `meta`, `name`, `total`, `_eds_operation`,
		ROW_NUMBER() OVER (
			PARTITION BY `id`
			ORDER BY `_eds_mvcc_timestamp` DESC, `_eds_timestamp` DESC, `_eds_seq` DESC
		) AS `_eds_rn`
	FROM `order`
) AS `_eds_ranked`
WHERE `_eds_rn` = 1 AND `_eds_operation` <> 'DELETE';
```

**(d) `_timeline`**
```sql
CREATE VIEW `order_timeline` AS
SELECT `id`, `meta`, `name`, `total`, `_eds_operation`,
	`_eds_mvcc_timestamp` AS `valid_from`,
	LEAD(`_eds_mvcc_timestamp`) OVER (
		PARTITION BY `id`
		ORDER BY `_eds_mvcc_timestamp` ASC, `_eds_timestamp` ASC, `_eds_seq` ASC
	) AS `valid_to`
FROM `order`;
```

### 3.3 SQL Server (MSSQL) — each statement its own `Exec` (no `GO`)

**(a) Base DDL + index**
```sql
DROP VIEW IF EXISTS [order_timeline];
DROP VIEW IF EXISTS [order_current];
DROP TABLE IF EXISTS [order];
CREATE TABLE [order] (
	[id] VARCHAR(64) NOT NULL,
	[meta] NVARCHAR(MAX),
	[name] NVARCHAR(MAX),
	[total] FLOAT,
	[_eds_seq] BIGINT IDENTITY(1,1) PRIMARY KEY,
	[_eds_operation] NVARCHAR(16) NOT NULL,
	[_eds_mvcc_timestamp] DECIMAL(38,10),
	[_eds_timestamp] BIGINT,
	[_eds_appended_at] DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME()
)
CREATE INDEX [ix_order_id_mvcc] ON [order] ([id], [_eds_mvcc_timestamp] DESC, [_eds_timestamp] DESC, [_eds_seq] DESC)
```

**(b) Three append INSERTs**
```sql
INSERT INTO [order] ([id],[meta],[name],[total],[_eds_operation],[_eds_mvcc_timestamp],[_eds_timestamp]) VALUES ('1234','{\"color\":\"red\"}','Widget',19.99,'INSERT',1719158400000000000.0000000000,1719158400000);
INSERT INTO [order] ([id],[meta],[name],[total],[_eds_operation],[_eds_mvcc_timestamp],[_eds_timestamp]) VALUES ('1234','{\"color\":\"blue\"}','Widget',24.5,'UPDATE',1719162000000000000.0000000000,1719162000000);
INSERT INTO [order] ([id],[meta],[name],[total],[_eds_operation],[_eds_mvcc_timestamp],[_eds_timestamp]) VALUES ('1234',NULL,NULL,NULL,'DELETE',1719165600000000000.0000000000,1719165600000);
```

**(c) `_current`**
```sql
CREATE VIEW [order_current] AS
SELECT [id],[meta],[name],[total]
FROM (
	SELECT [id],[meta],[name],[total],[_eds_operation],
		ROW_NUMBER() OVER (
			PARTITION BY [id]
			ORDER BY [_eds_mvcc_timestamp] DESC, [_eds_timestamp] DESC, [_eds_seq] DESC
		) AS _eds_rn
	FROM [order]
) AS ranked
WHERE _eds_rn = 1 AND [_eds_operation] <> 'DELETE'
```

**(d) `_timeline`**
```sql
CREATE VIEW [order_timeline] AS
SELECT [id],[meta],[name],[total],[_eds_operation],
	[_eds_mvcc_timestamp] AS valid_from,
	LEAD([_eds_mvcc_timestamp]) OVER (
		PARTITION BY [id]
		ORDER BY [_eds_mvcc_timestamp] ASC, [_eds_timestamp] ASC, [_eds_seq] ASC
	) AS valid_to
FROM [order]
```

### 3.4 Snowflake — `INSERT ... SELECT`, no index, object→STRING (matches upsert)

**(a) Base DDL (no secondary index — micro-partition pruning). `meta` is STRING (matches upsert; `PARSE_JSON` value is cast into it).**
```sql
CREATE OR REPLACE TABLE "order" (
	"id" STRING NOT NULL,
	"meta" STRING,
	"name" STRING,
	"total" FLOAT,
	"_eds_seq" NUMBER AUTOINCREMENT,
	"_eds_operation" STRING NOT NULL,
	"_eds_mvcc_timestamp" NUMBER(38,10),
	"_eds_timestamp" NUMBER,
	"_eds_appended_at" TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
	PRIMARY KEY ("_eds_seq")
);
```

**(b) Three append INSERTs (`SELECT`, not `VALUES`)**
```sql
INSERT INTO "order" ("id","meta","name","total","_eds_operation","_eds_mvcc_timestamp","_eds_timestamp")
SELECT 'o-1001',PARSE_JSON('{"region":"emea"}'),'Widget',19.99,'INSERT',1719500000.0000000000,1719500000123;
INSERT INTO "order" ("id","meta","name","total","_eds_operation","_eds_mvcc_timestamp","_eds_timestamp")
SELECT 'o-1001',PARSE_JSON('{"region":"emea"}'),'Widget',24.5,'UPDATE',1719500900.0000000000,1719500900456;
INSERT INTO "order" ("id","meta","name","total","_eds_operation","_eds_mvcc_timestamp","_eds_timestamp")
SELECT 'o-1001',NULL,NULL,NULL,'DELETE',1719501000.0000000000,1719501000789;
```

**(c) `_current` (single-level QUALIFY)**
```sql
CREATE OR REPLACE VIEW "order_current" AS
SELECT
	"id",
	"meta",
	"name",
	"total"
FROM "order"
QUALIFY ROW_NUMBER() OVER (
	PARTITION BY "id"
	ORDER BY "_eds_mvcc_timestamp" DESC NULLS LAST, "_eds_timestamp" DESC, "_eds_seq" DESC
) = 1
	AND "_eds_operation" <> 'DELETE';
```

**(d) `_timeline`**
```sql
CREATE OR REPLACE VIEW "order_timeline" AS
SELECT
	"id",
	"meta",
	"name",
	"total",
	"_eds_operation",
	"_eds_mvcc_timestamp" AS "valid_from",
	LEAD("_eds_mvcc_timestamp") OVER (
		PARTITION BY "id"
		ORDER BY "_eds_mvcc_timestamp" ASC, "_eds_timestamp" ASC, "_eds_seq" ASC
	) AS "valid_to"
FROM "order";
```

### 3.5 Composite-PK variant (e.g. `primaryKeys = ["company_id","id"]`)

Applies to all drivers: `columns()` = `["company_id","id", <sorted rest>]`;
BOTH key columns `NOT NULL`, all other object columns nullable; `_eds_seq`
remains the sole PK. Index/PARTITION BY lead with all PK columns in declared
order. `_current` keys/partitions on all PKs (PG: `DISTINCT ON
("company_id","id")` + matching `ORDER BY` prefix). `_timeline` `PARTITION BY
"company_id","id"`. DELETE tombstone fills every PK column (`quote_value` each,
in `primary_keys` order), all other object columns `NULL`.

**RULE (locked by the cross-port review): PK column lists join with NO space (`,`)** everywhere —
index/KEY, `DISTINCT ON`, `PARTITION BY`, and the `ORDER BY` prefix. The `, ` (comma-space) appears ONLY
between the PK group and the first audit column. (MySQL/MSSQL `_current`/`_timeline` put the PKs in
`PARTITION BY` so their window `ORDER BY` is audit-columns-only.) GOLDEN composite-PK strings (both ports
emit these byte-for-byte; verified by objective generation, not eyeballing):

```sql
-- PostgreSQL
CREATE INDEX "order__eds_history_idx" ON "order" ("company_id","id", "_eds_mvcc_timestamp" DESC, "_eds_timestamp" DESC, "_eds_seq" DESC);
-- _current:  SELECT DISTINCT ON ("company_id","id")  …  ORDER BY "company_id","id", "_eds_mvcc_timestamp" DESC NULLS LAST, "_eds_timestamp" DESC, "_eds_seq" DESC
-- _timeline: PARTITION BY "company_id","id"

-- MySQL
KEY `order_eds_history_idx` (`company_id`,`id`, `_eds_mvcc_timestamp` DESC, `_eds_timestamp` DESC, `_eds_seq` DESC)
-- _current/_timeline: PARTITION BY `company_id`,`id`   (window ORDER BY = audit cols only)

-- SQL Server
CREATE INDEX [ix_order_id_mvcc] ON [order] ([company_id],[id], [_eds_mvcc_timestamp] DESC, [_eds_timestamp] DESC, [_eds_seq] DESC)
-- _current/_timeline: PARTITION BY [company_id],[id]   (window ORDER BY = audit cols only)
```

---

## 4. Additivity ledger — new symbols per project

No existing symbol changes behavior in `upsert` mode; everything below is
additive and gated on the resolved append flag.

### 4.1 Shared / both ports

| Symbol | Purpose |
|---|---|
| `mode` (config.toml key) | persisted ingest mode (`upsert`/`append`) |
| `--mode` (CLI flag) | overrides config; persists per §1.1 |
| `append_mode` / `AppendMode` (resolved bool/enum) | threaded into drivers |
| `_eds_seq`,`_eds_operation`,`_eds_mvcc_timestamp`,`_eds_timestamp`,`_eds_appended_at` | audit columns (literal names) |
| `<table>_current`, `<table>_timeline` | the two views (naming convention) |
| `valid_from`, `valid_to`, `_eds_rn` (MSSQL/MySQL) | view-internal column aliases |

### 4.2 eds-python new symbols

- `eds/cmd/config.py`: `set_config_value(...)` already exists — reused for `mode`.
- Per-driver `sql.py`: `create_append_sql(...)`, `to_append_sql(...)` (INSERT per change), `create_current_view_sql(...)`, `create_timeline_view_sql(...)`, `drop_views_sql(...)`.
- `eds/drivers/snowflake/snowflake.py`: append branch in `plan_flush` (skip `combine_records_with_same_primary_key`, one `INSERT...SELECT` per record, `statement_count == len(records)`); `migrate_new_table` append variant.
- Tests: `tests/test_*_append.py` asserting the §3 golden vectors.

### 4.3 eds-dotnet new symbols

- `EdsConfig.SetValue(dataDir, "mode", ...)` — reused for `mode`.
- Per-driver `*Sql.cs`: `CreateAppendSql`, `ToAppendSql`, `CreateCurrentViewSql`, `CreateTimelineViewSql`, `DropViewsSql`.
- `SnowflakeDriver.cs`: append branch in the `plan_flush` equivalent + `migrate_new_table` append variant.
- Tests: `*AppendTests` / additions to existing `*SqlTests` asserting §3.

---

## 5. Cross-port consistency checklist

Before either port ships append mode, confirm ALL of:

- [ ] `--mode`/config precedence + persistence (§1.1) identical; unknown value → exit 3 in both.
- [ ] `columns()` order `id,meta,name,total` reused in DDL, every INSERT, both views (no re-sorting).
- [ ] Object columns nullable except PK(s); `_eds_seq` is the surrogate PK; trailing object-PK clause dropped.
- [ ] `_eds_mvcc_timestamp` emitted as a BARE numeric literal (empty `""` → `NULL`); `_eds_seq`/`_eds_appended_at` never in the column list.
- [ ] INSERT == UPDATE shape (full after-snapshot, only operation literal differs); DELETE == key(s) + `'DELETE'` + NULLs.
- [ ] Plain INSERT only — no ON CONFLICT/REPLACE/MERGE/delete-before-insert (incl. Snowflake `INSERT...SELECT`).
- [ ] `_current` excludes latest=DELETE and projects ONLY object columns (== upsert table).
- [ ] `_timeline` includes deletes; `valid_to` via `LEAD` ASC-ordered; point-in-time predicate documented.
- [ ] Add-column migration DROPs + recreates both views (PG/MySQL/MSSQL) before re-emitting (a new sorted-position column shifts view output columns).
- [ ] Golden-vector unit tests in BOTH ports assert the §3 strings byte-for-byte (cross-diff the two ports' fixtures).
- [ ] Composite-PK variants (§3.5) covered by tests in both ports.

---

## 6. Per-driver implementer RISK notes (watch these)

1. **PostgreSQL** — `CREATE OR REPLACE VIEW` can only APPEND output columns; a new sorted-position column breaks it. Add-column path MUST `DROP VIEW` both views then recreate. Initial create uses `DROP TABLE ... CASCADE` (auto-drops views); only migrations need explicit `DROP VIEW`. Also: `DESC` defaults to `NULLS FIRST` — the explicit `NULLS LAST` on mvcc is load-bearing.
2. **MySQL** — no `NULLS LAST` syntax; relies on MySQL sorting NULL lowest so NULL mvcc sorts last under `DESC` implicitly (verify on 8.0.14+). Backtick idents have NO escaping. Views freeze their column list at create → DROP+CREATE after add-column. Requires 8.0.14+ (descending index keys, window fns, fractional `CURRENT_TIMESTAMP(6)`).
3. **SQL Server** — each DDL/view stmt is its own `Exec` (no `GO` emitted); ensure the batch ordering (views dropped before table, view created as first stmt in its batch) is preserved by the per-statement path. SQL Server sorts NULLs LAST under `DESC` natively (no clause). Idents `[x]` un-escaped.
4. **Snowflake** — the ONLY non-`SqlDriverBase` driver: wire append into `plan_flush`/`migrate_new_table`, NOT a shared base. object columns are typed **STRING** — REUSE `prop_type_to_sql_type` VERBATIM (it returns `STRING` for object; `VARIANT` is the ARRAY branch only — sql.py:147 vs :151). The INSERT value is still wrapped with `PARSE_JSON` (`generate_insert_function`), which Snowflake casts into the STRING column, so you MUST use `INSERT...SELECT` (PARSE_JSON illegal in `VALUES`). This is "match upsert" (the user's confirmed choice) — do NOT add an object→VARIANT override. Append just replaces the `MERGE` plan with a batched `INSERT...SELECT`. Skip `combine_records_with_same_primary_key`, drop the 24h tracker + update-noise skip (full history is the point), no secondary index.
