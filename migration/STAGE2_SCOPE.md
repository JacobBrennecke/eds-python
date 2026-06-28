# Stage-2 Idiomatic Refactor — Scope & Plan (eds-python)

Living document (plan + do-not-touch inventory + per-workstream execution log), mirroring the completed
`eds-dotnet/migration/STAGE2_SCOPE.md`. Stage-2 re-expresses the *code* idiomatically **without changing any
observable byte, subject, status code, or exit code** — the 622 tests + the byte-exact JSON/SQL vectors are the net.

## Headline

The Python Stage-1 port is **already at the C# Stage-2 end-state for the structural items** — it was written with
the completed C# port as a secondary reference, so it already has the highest-value C# workstreams baked in:
- **WS7 (C#'s highest value)** — `eds/drivers/sql_base.py` already has the `SqlDb` seam + `SqlDriverBase`
  orchestration; PostgreSQL/MySQL/SQLServer are already thin subclasses, with Docker-free `FakeSqlDb` coverage in
  `tests/test_sql_base.py`. **Done.**
- **WS9 forker seam** — `ControlPlaneContext.forker` is an injectable seam; the exit-code trees are covered by
  `tests/test_runner_*`. **Done.**
- **WS4/WS2/typing** — 0 legacy typing (all `X | None` + `from __future__`), dataclasses + `runtime_checkable`
  Protocols throughout, `IntEnum`. **Done.**

So Stage-2 here is **small and concentrated**: a few low-risk sweeps + one optional high-risk serializer change,
over a large **do-not-touch** core. **The do-not-touch inventory below is the primary deliverable** — it stops a
future contributor from "modernizing" load-bearing code.

## Baseline (taken at Stage-2 start)

- 10,730 LOC / 75 modules; **622 tests** (382 `def test_` × parametrize); ruff (E/F/I/UP/B, line 120) + mypy clean;
  `filterwarnings=error`.
- **212 `# PARITY`** markers, **14 inline `# DEVIATION`** (~40 entries in `migration/DEVIATIONS.md`), 94 `# noqa`.
- **25 hand-rolled `def __gojson__`** + 4 hand-built `to_msgpack`; **0 `NamedTuple`**; only `LogLevel` is an enum.

## The parity contract (every item is byte/wire/exit identical — the gate every workstream must pass)

1. **Byte-exact serialization:** `util/gojson.py` + `util/gofloat.py` (the byte spine); the per-DTO `__gojson__`
   key strings / declaration order / omitempty rules in `dbchange.py`, `notification/dtos.py`, `api/__init__.py`,
   `schema.py`; the per-driver SQL in `drivers/sql_base.py` + `{mysql,postgresql,sqlserver,snowflake}/sql.py`;
   `hash.py`, `mask.py`, `gourl.py`. "Golden" = inline `@parametrize` expected vectors (NOT files — nothing to
   regenerate; a regression is a failing assertion).
2. **Wire/protocol:** NATS subjects (`eds.notify.<sid>.>`, `eds.client.<sid>.<action>-{response,status}`,
   `dbchange.*.*.<cid>.*.PUBLIC.>`, durable `eds-<server_id>[-<suffix>]`, stream `dbchange`, raw `b"pong"`); the
   **msgpack-vs-JSON reply split** (JSON `m.respond` for configure/import-init/driverconfig/validate, msgpack
   publish otherwise); case-insensitive content-encoding; 127.0.0.1-only loopback `/control/*` + status codes.
3. **Exit codes:** `cmd/exit_codes.py` (0–5, `MAX_FAILURES=5`) + the three decision trees (L1 wrapper linear
   backoff, L2 full tree, L3 fork). **Land-mine:** the L2 tree branches on `last_error_lines` substring text
   (`"error: required flag"`, `"Global Flags"`) — do not "clean up" a log string the runner matches on.
4. **PARITY quirks (preserve, do not "fix"):** the `messsage` (3-s) typo key; copy-paste log strings in
   `notification/__init__.py`; gopsutil lowercase `hostid`; RE2 semantics via `re.ASCII`+`\Z`; rawjson
   `marshal(..., sort_keys=False)` for before/after; metrics `_total`/`"10.0"` labels.
5. **Behavior decisions:** the goroutine-analog detached tickers (`NotificationRunner` renew/log-sender,
   disconnect-watcher → exit-5 timing); `SchemaValidationError`-skip vs other-abort; the `(found, T)` cache/tracker
   distinction; HttpRetry policy (unbounded 408/429/5xx, jitter, fresh request/attempt).

## Do-not-touch inventory (the load-bearing core — the core deliverable)

1. `util/gojson.py` engine (`marshal`/`_encode*`/`compact_raw`/`RawJson`/`stringify`) — byte-exact.
2. `util/gofloat.py` (`format_f`/`format_g`/`format_json`) — Go float formatting.
3. Per-DTO `__gojson__` **emitted bytes** (keys + order + omitempty), incl. the `messsage`/`maskedURL` keys,
   `key`-no-omitempty-emits-null, before/after via `compact_raw`. (WS2 may change the *mechanism*, never the bytes.)
4. Per-driver SQL quoting/identifier/float (PG dollar-quote, MySQL/MSSQL 1/0 + escapes, Snowflake, `format_f` vs
   `format_g`, MySQL dead `updateValues`, PG bare-value-on-missing-diff).
5. RE2-faithful regexes (`re.ASCII` + `\Z` + the asymmetric `sql` scalar anchor).
6. `(bool, T)` found-vs-empty distinction (`Cache.get`/`Tracker.get_key`/`get_type`/`get_table_version`).
7. Validator errors-as-data: the 3-tuple `(found, valid, path)` + `(value, FieldError|None)` (the deliberate
   non-raise so all field errors are reported at once).
8. `EXIT_*` values + the L1/L2/L3 decision trees + the `last_error_lines` substring branch + its log strings.
9. printf log format strings + the `[ts ]LEVEL [prefix] msg [k=v]` line (DEVIATION `logger-format`).
10. Hand-rolled `cmd/args.py` flag parsing + `tomli`/`config.py` (flag/exit-3/duration/config asserted).
11. `HttpRetry` policy (`util/http.py`).
12. Goroutine-analog detached tasks (`NotificationRunner`/consumer/loopback) — shutdown + exit-5 timing.
13. Exception-as-control-flow (`SchemaValidationError`, `DriverStoppedError`, `Consumer{Stopped,Fatal}Error`).
14. The already-extracted seams (`SqlDriverBase`/`SqlDb`, the `forker` seam) — keep; don't re-architect.
15. `_atoi` strict parse, the U+2028/2029 escapes in `compact_raw`, `reset_registries()`/metrics reset (test-only).

## Workstreams

### WS0 — Guardrails (S, low) — FIRST
This document (the inventory) + light ruff/mypy carve-out comments pinning the load-bearing sites. No `src` change.

### WS1 — Low-risk idiomatic sweeps (M, LOW risk / MED value) — output-neutral, mechanical
- **NamedTuple the wide returns** (0 today): `snowflake_keypair.parse_key_pair_url` (5-tuple),
  `notification_wiring._run_import` (4-tuple `(success, validated, message, log_path)`), the validator
  `(found, valid, path)` 3-tuple (`schema.py`/`util/schema.py` + the `batch_processor`/`importer` call sites),
  `crdb.parse_crdb_export_file`, `registry._sort_table`, `upgrade.build_release_urls`, etc. Unpacks identically →
  no output touched; adds `.field` access + a type name.
- **`DriverType`/`DriverFormat` → `str, Enum`** (`driver.py:23-26`; `str, Enum` since 3.10 has no `StrEnum`) — used
  as JSON field values, byte-identical via `marshal`'s `isinstance(str)`. Verify with the driver-config goldens.
- **Dead-code:** inline the identity `int_pointer` (`driver.py:214`); drop vestigial `Batcher.pks`.
- **Optional polish:** name/comment the detached tickers so they aren't "optimized away"; rename a few internal
  `new_*` wrappers (keep the `new_driver`/registry family the cmd layer wires).
- Each as its own commit; full suite + lints after each.

### WS2 — Declarative metadata-driven serializer (L, **HIGH risk / HIGH value**) — OPTIONAL, decision required
Collapse the 25 hand-rolled `__gojson__` + 4 `to_msgpack` into a generic `gojson_struct(obj)` that walks
`dataclasses.fields(obj)` (declaration order is guaranteed) reading per-field
`field(metadata={"json": "companyId", "omitempty": OmitEmpty.IF_NONE|IF_FALSY|NEVER|RAWJSON})` — the Python analogue
of C#'s `[JsonPropertyName]`/`[JsonPropertyOrder]` attributes, over the **untouched** byte-exact `marshal` engine.
Unifies the in (`from_dict`) + out paths on one field spec (kills the parallel camelCase hand-mapping). The catch:
omitempty is irregular (the `messsage` typo, `key`-emits-null, before/after via `compact_raw`, `*string` omit-None
vs plain omit-falsy) — every rule must be encoded. **DTO-by-DTO, behind a full byte-diff of every JSON/SQL vector
before/after, with an adversarial parity review.** If risk appetite is low, **DECLINE and keep hand-rolled** — the
C# precedent (JSON internals = do-not-touch #1) fully justifies leaving it; the 25 methods are already byte-locked.

## Declines (each matches an explicit C# Stage-2 decline)

- Validator errors-as-data → raise (WS-cat 1): would change the field-errors-reported-to-HQ semantics. **Decline.**
- printf `%s`/`%v` logging → f-strings (175 sites): parity trace + log-line in the contract. **Decline.**
- Async restructure of `NotificationRunner` loop-in-thread: shutdown/exit-5 timing. **Decline.**
- method→`@property` sweep: Protocol + impls + call-sites churn (C# declined the `IDriver` equivalent). **Decline.**
- Type-hint modernization: already complete (0 legacy typing). **No action.**
- SQL base-class / forker seam / DI: **already done** in Stage-1. **No action.**
- 3rd-party libs (pydantic / dataclasses-json / tomli-w / CLI framework): change float/escape/flag behavior →
  break byte/exit parity. **Decline** (roll the tiny in-house metadata reader if WS2 proceeds).

## Safety net (every workstream must pass)

- Full `pytest` (≥622, count grows ONLY from new tests) **with Docker up** so the 6 e2e files run (they silently
  skip otherwise — the live NATS pull-loop + real driver SQL are only covered there).
- `ruff check` + `mypy` clean; no new warnings (`filterwarnings=error`).
- The §1 byte-exact tests unchanged (the golden gate).
- Adversarial parity review per non-trivial workstream (mandatory for WS2), over the per-WS diff.
- Re-build the PyInstaller one-file binary after any import-graph change (no pytest guard on packaging).
- Any Stage-2 divergence → a `migration/DEVIATIONS.md` entry (Go / Stage-1 / Stage-2 + Why + Risk).

## Execution order

WS0 (guardrails/this doc) → WS1 (mechanical sweeps) → WS2 (only if approved). Mechanical before structural so edits
don't fight. WS1 alone delivers the realistic idiomatic gain; WS2 is the one judgment call.

## Execution log

- **WS0** — scope + do-not-touch inventory authored. **Decision: WS1 + WS2 (full) approved** (do the low-risk
  sweeps, then the declarative serializer DTO-by-DTO behind byte-diffs).
- **WS2** — declarative serializer DONE (632 tests, ruff+mypy clean; byte-identical). New `eds/util/gostruct.py`
  (OmitEmpty NEVER/IF_NONE/IF_FALSY/IF_EMPTY_RAW + gojson_struct + msgpack_dict over the untouched marshal). Added
  `tests/test_ws2_serialization.py` (10 byte-snapshots for the previously-untested DTOs) FIRST as the golden-before.
  Migrated all 24 DTOs to `field(metadata={"json","omit"})` + a `return gojson_struct(self)`/`msgpack_dict(self)`
  body: notification(9), schema(3), driver(3 — DriverField/DriverMetadata/DriverConfigurator), dbchange(1, incl
  IF_EMPTY_RAW before/after + key-null), api(4), metrics(4), sysinfo(2), batcher Record(1). FieldError kept
  hand-rolled (it's an Exception, not a dataclass). Every quirk reproduced via metadata: messsage typo, maskedURL,
  $comment, hostid/num_cpu/go_version, *bool-false-emits-vs-value-false-omits, log_path SKIP, before/after
  compact_raw-via-marshal. The heartbeat's SystemStats.__gojson__() seam preserved. **Review: clean (0 HIGH/MED,
  0 confirmed; 1 LOW informational).** Intentional hand-rolled exclusions (correct, no byte change): `FieldError`
  (an Exception, not a dataclass) and `import_client.TableExportInfo` (has a `datetime` field marshal can't encode).
  msgpack key-order locked by an explicit order test (==dict is order-insensitive but packb isn't). 633 tests.
- **WS1** — DONE (622 tests, ruff+mypy clean). 4 NamedTuples: `ValidationResult` (schema.py + util/schema.py +
  the batch_processor/importer call sites), `ImportRunResult` (notification_wiring `_run_import`), `KeyPairUrl`
  (snowflake_keypair; `secret_var: str` — gourl `Values.get` returns "", not None), `CrdbExportFile` (crdb). All
  unpack positionally → call sites unchanged. `DriverType`/`DriverFormat` → `str, Enum` (byte-identical via
  `marshal` isinstance(str); verified by test_driver + the SQL goldens staying green). Inlined the identity
  `int_pointer`; dropped the write-only `Batcher._pks`. **Review: clean (0 HIGH/MED, 9 LOW, all verified
  behavior-preserving).** Two *latent* (non-current) risks noted: a future `str()`/`%`-format of a `DriverType`
  would coerce to `"DriverType.X"` (guarded by the comment at driver.py:24-25; no current path does it); the
  NamedTuples would marshal as JSON arrays *if ever serialized* (none are). No fix needed.
