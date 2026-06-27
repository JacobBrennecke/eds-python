# EDS Go → Python Migration — Implementation Plan (Stage 1)

Bottom-up, vertically-sliced, **test-first**. Each milestone leaves the package
importing and the suite **green**. Golden-output parity tests are written as soon as
the code that produces the output exists. The Go tree in `../edsGolang` is the
**oracle**; Go `_test.go` vectors are captured/ported to `tests/golden/` and pytest.

This plan mirrors the (completed, validated) C# port's M0–M10 — the architecture,
risks, and quirk list transfer directly; only the language/packages differ. The C#
`migration/research/*.md` (copied here) are the faithful per-subsystem Go analyses.

Legend: ☐ todo · ◐ in progress · ☑ done

---

## Package layout

```
eds-python/
  pyproject.toml                  (3.10+, ruff + mypy + pytest config)
  eds/                            (the package; mirrors Go internal/ + cmd/)
    __main__.py                   = main.go        (python -m eds)
    cmd/                          = cmd/           (root, version, enroll, server, fork, import_cmd, download, publickey, drivers)
    api/  consumer/  drivers/  importer/  notification/  osext/  registry/  tracker/  upgrade/  util/
    dbchange.py  driver.py  metrics.py  migration.py  schema.py   = internal/*.go top-level
  tests/                          (pytest; unit + golden parity + integration [Docker-gated])
    golden/                       (captured Go output)
  migration/                      (SPEC.md, PLAN.md, DEVIATIONS.md, package-map.md, research/)
```

Dependency direction: `cmd → consumer, drivers → core (schema/registry/tracker/util)`. No cycles.
Module-per-Go-file where practical (faithful mirroring of functions/methods). `import.go` →
`import_cmd.py` (Python keyword). Concurrency: **asyncio-first** (nats-py is async) — `context.Context`
→ cancellation/`asyncio.Event`; goroutines → tasks; channels → `asyncio.Queue`; `sync.WaitGroup` →
`asyncio.gather`; `sync.Once` → guard/`functools.cache`. Preserve *ordering and decisions*, not primitives.

---

## M0 — Scaffold & toolchain  ◐
- [x] `.venv` (CPython 3.10.7 64-bit); core deps (pytest, xxhash, msgpack) installed + verified.
- [ ] `pyproject.toml` (package metadata, ruff + mypy + pytest config), `.gitignore`.
- [ ] Package skeleton (`eds/` + subpackages with `__init__.py`); `python -c "import eds"` clean.
- [ ] Smoke test green (`pytest`).
**DoD:** `python -m eds --help` stub runs; `pytest` green; `ruff check` clean.

## M1 — Core domain + util foundation  ☐
Port the spine + byte-critical helpers, each with golden parity tests (vectors from Go `_test.go`).
- [ ] Domain: `DBChangeEvent` (`get_primary_key/get_object/omit_properties/__str__/from_message`),
      `SchemaProperty`, `Schema` (`columns()` ordering), `ItemsType`, `DatabaseSchema`, `SchemaMap`,
      registry/validator protocols.
- [ ] util: `json` (Go-compatible Stringify: **sorted keys, `<>&`/U+2028-9 escaping, decl order, float
      formatting**, JsonDiff, RawJson, omitempty), `hash` (XXH64 of Go `%+v`), `modulo` (FNV1a32),
      `mask`/`mask_url`/`mask_email`/`mask_arguments` (+ Go URL parser), `sql` (ToJsonStringVal + the
      asymmetric scalar regex), `file` (Exists/ListDir/GzipFile/ToFileURI/IsLocalhost), `crdb`
      (ParseExportFile/parsePreciseDate), `jwt` (GetApiUrlFromJwt), `http` (HttpRetry).
- [ ] **Parity tests** frozen from Go: `hash_test.go` (5 vectors), `modulo`, `mask_test.go`,
      `ToJsonStringVal`, `ParseExportFile`, `GetApiUrl`, `json_test.go`.
**DoD:** all parity vectors pass; the §8 quirks touching util (#1, #15) have markers + tests.

## M2 — Common infra + state stores  ☐
- [ ] Logging (`util/logger`): leveled, `with_prefix`, `with(fields)`, printf/composite format, `fatal`→exit.
- [ ] Shutdown signal (SIGINT/SIGTERM → `asyncio.Event`/cancellation), `osext` exe path, docker detection,
      free-port, gzip/gunzip, NATS msg decode (`util/nats`: gzip/json · msgpack→json→target · raw json).
- [ ] creds-file parsing (CompanyIDs/ServerID/SessionID from the user-JWT allow-subjects).
- [ ] Process fork helper (`util/process`: launch child, capture stdout/stderr to `<label>_stdout/stderr`,
      last-error-lines, ctx-cancel kill, exit code).
- [ ] `tracker` (sqlite3, BINARY ordinal keys; BuntDB-faithful): get/set/setKeys/deleteKey/
      deleteKeysWithPrefix + TTL; `cache` (in-memory TTL); `batcher`.
**DoD:** tracker round-trip + TTL + prefix-scan tests; cache eviction; fork captures a child exit code.

## M3 — Registry + metrics + sysinfo  ☐
- [ ] `registry.ApiRegistry` (3-tier memory→tracker→API cache, sortTable re-key, schema-fetch URL,
      version get/set, latest-schema seed asymmetry). HttpRetry injectable for mock-HTTP tests.
- [ ] `metrics` (prometheus-client: 5 instruments w/ exact names/help/buckets incl. the `receving` typo),
      `SystemStats` (pendingEvents always 0; histogram = sample counts), `get_system_stats`.
- [ ] sysinfo (`get_system_info`, `get_machine_id` HMAC-SHA256(MachineGuid,"eds") hex, `get_local_ip`).
- [ ] api DTOs (`api`): Session start/response/end, EdsSession, DriverMeta, Enroll*, `get_api_url`.
**DoD:** registry cache-tier tests (mock API); metrics name/bucket tests; `get_api_url` mapping test.

## M4 — Driver framework + first vertical slice (File + PostgreSQL)  ☐
- [ ] `driver` protocols (lifecycle/migration/session-handler/alias/help; importer/import-handler),
      `DriverField`/`FieldError`, config helpers, `DriverRegistry`, `DriverStoppedError`.
- [ ] **PostgreSQL SQL generation** (pure, golden-tested): quoting, `to_sql`/`to_sql_from_object`,
      prop→sql type, create/alter; Go float `%g`/`%f` formatting.
- [ ] PostgreSQL driver over `psycopg` (start/process/flush transactional batch/stop/test; migration;
      bulk import) + ADO-equivalent helpers (query_single_value / build_db_schema / sql_executer / drop_table).
- [ ] File driver (full) + importer; faithful (or corrected — see DEVIATIONS) Windows file-URI handling.
**DoD:** Postgres driver e2e vs real DB (testcontainers: migrate→insert/update/delete→verify); File e2e.

## M5 — Consumer + importer engine  ☐
- [ ] `importer.run` replay engine + NDJSON(.gz) reader (incl. `LocationID=companyId` quirk + validator hook).
- [ ] `consumer` — batching engine (Go `bufferer`: §4.3 flush precedence incl. `NumPending>max` catch-up,
      sequence check, skip, migration-on-the-fly, schema-diff, ack/nack) + live NATS wiring (`nats-py`):
      creds/dev connect, durable get/create/update w/ `NumWaiting>0`→already-running guard, pump
      (drain/idle/graceful-flush/NAK-on-cancel), pause/unpause, stop, disconnect self-stop, heartbeats (msgpack).
- [ ] `SchemaMigration.update_destination_schema` startup migration.
**DoD:** consumer unit tests w/ a fake driver + mock NATS (mirror `consumer_test.go`): thresholds,
ordering, skip, flush counts; importer replay over a fixture ndjson.gz.

## M6 — Remaining SQL drivers (MySQL, SQL Server, Snowflake[+keypair])  ☐
- [ ] MySQL (`PyMySQL`): REPLACE INTO + dead updateValues quirk; backslash escaping; `g` floats;
      JSON-timestamp coercion (raise on bad + <1970 clamp). Golden tests + e2e.
- [ ] SQL Server (`pyodbc`): MERGE upsert; hybrid `''`+backslash escaping; handleSchemaProperty; alias `mssql`. Golden + e2e.
- [ ] Snowflake (`snowflake-connector-python` behind a seam): batcher + RecordOptimize (sort-by-mvcc +
      combine-by-pk); timestamp-gated MERGE; tracker-gated force-delete; multi-statement count; key-pair. Golden + fake-tracker unit tests.
**DoD:** golden SQL matches Go for all three (MySQL/MSSQL e2e); Snowflake statement-count + dedup unit-tested.

## M7 — Remaining streaming drivers (S3, Kafka, EventHub)  ☐
- [ ] S3 (`boto3`): provider detection, bucket/prefix, `<prefix><table>/<unixSeconds>-<pk>.json` keys,
      hard MaxBatchSize=1000, bounded-concurrency upload. Golden + LocalStack e2e.
- [ ] Kafka (`confluent-kafka`): `dbchange.*` key + `eds-partitionkey` header, Hash/Modulo partitioner
      computed explicitly, 1000 cap, 10s Leader-Not-Available loop. Golden + Redpanda e2e.
- [ ] EventHub (`azure-eventhub`): `sb://` rewrite, consecutive-key coalescing, MessageId/objectId. Unit-tested.
**DoD:** JSON payload + key/partition golden + batch-coalescing tests; per §8 #9/#10.

## M8 — Notification control plane  ☐
- [ ] `notification`: all DTOs (JSON tags incl. `messsage`/`error`/`LogPath`/omitempty quirks), dispatcher
      (routes by `action`; JSON-reply vs msgpack-publish duality; background import; sendlogs), consumer
      (subscribe `eds.notify.<sid>.>`, decode, graceful stop), msgpack publish via stringify→convert.
**DoD:** dispatcher/DTO unit tests (transport+handler faked, per action) + live NATS e2e.

## M9 — CLI + process model + upgrade  ☐
- [ ] CLI core (`cmd`): argparse dispatcher + global flags (`--data-dir/-d`, `--verbose`, `--silent`,
      `--api-url`); `version`, `publickey` (embedded shopmonkey.asc), `enroll` (→ `config.toml`);
      config r/w (token + snake_case `server_id` + url); exit codes (§6: 0/1/2/3/4/5).
- [ ] `SessionManager` (sendStart/sendEnd/getLogUploadUrl/uploadFile via HttpRetry; 409 handling).
- [ ] `ForkWorker` (Layer 3): tracker+registry+driver+consumer; loopback `/` + `/metrics` + `/control/*`;
      lifecycle loop; exit codes (nats-disconnect→5); rotating log sink.
- [ ] `ServerControlPlane` (Layer 2): session loop → fork → on-exit sendEnd+upload → loop; renew ticker;
      the notification handler callbacks driving the fork over `/control/*`.
- [ ] `ServerSupervisor` (Layer 1): `eds server` forks `--wrapper`, crash-restart up to 5× + backoff, `/restart`.
- [ ] Backfill (`ExportJobClient` + `BulkDownload` + `eds import`); upgrade/download (PGP via PGPy).
- [ ] Single-binary packaging (PyInstaller → one `eds` executable), matching Go's single binary.
**DoD:** version/publickey/enroll/import/download work; server/fork/session + control plane + backfill +
upgrade implemented + unit-tested; exit-code contract (§6) golden-tested (mirror the C# `ServerSupervisorTests`).

## M10 — End-to-end + parity sweep  ☐
- [ ] `docker-compose.yaml` (postgres/mysql/mssql/s3/kafka/nats) for manual local integration.
- [ ] E2E: import→stream→verify rows for File (always) + PostgreSQL (Docker-gated).
- [ ] Parity sweep: every §8 quirk has a `# PARITY:` marker + covering test or documented note.
- [ ] `DEVIATIONS.md` finalized; `ruff`/`mypy` clean; full `pytest` green.
**DoD:** green E2E (File + Postgres); parity sweep complete; lints clean; DEVIATIONS finalized.

---

## Cross-cutting conventions
- **Parity markers:** every Go-quirk reproduction gets `# PARITY: <go file:line> — <what/why>`;
  every intentional divergence gets `# DEVIATION: see DEVIATIONS.md#<anchor>` + an entry.
- **asyncio-first:** preserve ordering/decisions, not threading primitives (see layout note).
- **Determinism:** format numbers/dates explicitly (Go float `g`/`f` parity); never rely on locale.
- **Golden files** captured from Go live in `tests/golden/` with a documented capture procedure.

## Risk register (top items — same as the C# port, language-adjusted)
1. Go float `g`/`f` formatting fidelity (Python `repr`/`%g` differ from Go `strconv` — needs care).
2. `json.Marshal` key-sort + `<>&`/U+2028-9 escaping parity (Python `json` does neither by default).
3. `Hash` = XXH64 over Go `%+v` — reproducing `%+v` for the exact hashed types in Python.
4. Snowflake multi-statement count + tracker-gated dedup.
5. Consumer batching state-machine edge timings (asyncio).
6. Process model on Windows (no SIGHUP; control via HTTP + cancellation).
7. PGP verify compatibility with gopenpgp (PGPy detached-sig verify).
8. RE2-vs-`re` regex semantics (Go `\d` ASCII-only, `$` absolute-end) — see the C# regex deviation.
9. 32-bit-int overflow / hashing: Python ints are unbounded — mask to uint32/uint64 explicitly.
