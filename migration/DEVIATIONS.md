# DEVIATIONS — Go → Python port

Intentional, justified divergences from the Go `edsGolang` behavior. Every entry is referenced
from code with `# DEVIATION: see DEVIATIONS.md#<anchor>`. Faithfulness is the default; this file
records the exceptions (language/runtime/package differences) for review. Quirks that are
*reproduced* (not deviated) live as `# PARITY:` markers in code, not here.

---

### python-310-64bit
**Decision:** target CPython **3.10 (64-bit)** (`py -3.10` on this machine), not the default
`python` (3.8.1, 32-bit). **Why:** several required packages (`snowflake-connector-python`,
`confluent-kafka`, `azure-eventhub`, `psycopg[binary]`, `pyodbc`) ship 64-bit wheels and target
≥3.8/3.9 with practical support on 64-bit; 32-bit 3.8 (EOL) has scarce/no wheels for them. No
behavioral impact — purely the runtime the faithful port runs on. 3.10 (not 3.11+) because that is
the newest 64-bit interpreter installed; consequence: no `tomllib`/`match`-everywhere reliance, so
TOML uses `tomli`/`tomli-w`.

### asyncio-concurrency-model
**Go:** goroutines + channels + `context.Context` + `sync.*`. **Python:** asyncio is the
concurrency model (the NATS client `nats-py` is async-only). `context.Context`→cancellation +
`asyncio.Event`; goroutines→tasks; channels→`asyncio.Queue`; `sync.WaitGroup`→`asyncio.gather`;
`sync.Once`→guard. **Ordering and decisions are preserved**, not the primitives (same convention the
C# port used). Synchronous one-shot CLI paths (enroll/version/download/import) stay synchronous.

### no-embedded-nats-server
**Go:** imports `nats-server/v2` (can embed a server). **Python:** has no embeddable NATS server;
the consumer connects to an external NATS as a client (`nats-py`). Tests use a real `nats` container
(testcontainers), mirroring how the C# port validated against a real server. To verify: confirm the
Go embedded server is only used by dev/e2e tooling, not the production consumer path (grounding TBD in M5).

### module-naming-keywords
`import.go`→`import_cmd.py` (`import` is a Python keyword). The user-facing CLI command stays
`import`. Other Go files map 1:1 to modules where practical.

### single-binary-pyinstaller
**Go:** `go build` → one static `eds` binary. **Python:** PyInstaller (one-file) → one `eds`
executable bundling the interpreter + deps, matching Go's single-binary distribution (the C# port
used single-file self-contained publish for the same reason). Native deps (librdkafka, ODBC) bundle
where supported; documented at M9.

<!-- Add further deviations below as they arise (carry over the C# port's where they recur:
     regex-re2-vs-python, *-tls-default, *-connstring-params-subset, file-uri-windows-drive-letter,
     download-zip-extract, etc. — re-grounded against the Go source as each subsystem is ported). -->
