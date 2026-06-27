# Package map — Go module → Python package

Grounded in `../edsGolang/go.mod`. Drivers/heavy deps are installed **per-milestone** (not all up
front) so the core lands fast and wheel issues surface in isolation. `[binary]`/wheel-only packages
are preferred to avoid local C toolchain builds. CPython 3.10 64-bit (see DEVIATIONS#python-310-64bit).

## Core / always (M0–M3)
| Go | pip | notes |
|---|---|---|
| cespare/xxhash | `xxhash` | XXH64; ✅ installed |
| vmihailenco/msgpack | `msgpack` | ✅ installed |
| testify, go-sqlmock | `pytest` | + `unittest.mock` fakes/seams; ✅ installed |
| BurntSushi/toml, spf13/viper | `tomli`, `tomli-w` | 3.10 has no `tomllib` |
| client_golang | `prometheus-client` | exact instrument names/help/buckets |
| tidwall/buntdb | stdlib `sqlite3` | BINARY/ordinal key ordering |
| shirou/gopsutil | `psutil` | sysinfo/mem/load |
| golang-jwt | manual base64url; `PyJWT` | unverified decode is manual; PyJWT only where verified |
| net/http | `requests` | sync CLI HTTP; retries hand-rolled (HttpRetry parity) |
| hash/fnv, uuid, gzip/tar/zip, hmac | stdlib | `hashlib`,`uuid`,`gzip`,`tarfile`,`zipfile`,`hmac` |

## Drivers (M4, M6, M7) — install with the milestone
| Go | pip | risk / note |
|---|---|---|
| lib/pq | `psycopg[binary]` | psycopg 3; per-op connections ≈ Go `*sql.DB` pool |
| go-sql-driver/mysql | `PyMySQL` | pure-Python, no build; multi-statement support to confirm |
| microsoft/go-mssqldb | `pyodbc` (+ MS ODBC Driver 18) | fallback `pymssql` if ODBC driver absent; verify in M6 |
| snowflakedb/gosnowflake | `snowflake-connector-python` | heavy; behind a seam (untestable w/o account, like C#) |
| aws-sdk-go-v2/s3 | `boto3` | LocalStack e2e |
| segmentio/kafka-go | `confluent-kafka` | librdkafka wheel; explicit Hash/Modulo partitioner |
| azure azeventhubs | `azure-eventhub` | no emulator → unit-tested |

## Control plane / CLI / upgrade (M8–M9)
| Go | pip | note |
|---|---|---|
| nats.go (+jetstream/nkeys/jwt) | `nats-py` | asyncio; JetStream + nkeys creds |
| santhosh-tekuri/jsonschema | `jsonschema` | schema validator |
| ProtonMail/gopenpgp | `PGPy` | pure-Python detached-sig verify (no gpg binary) |
| spf13/cobra | stdlib `argparse` | hand-rolled dispatcher (mirrors C# port) |
| charmbracelet/huh | `questionary` | interactive enroll/config forms |
| fatih/color, x/ansi | `colorama`/`rich` | terminal styling |
| denisbrodbeck/machineid | manual (Windows MachineGuid) | HMAC-SHA256(MachineGuid,"eds") hex, like C# |
| (packaging) | `pyinstaller` | one-file `eds` executable |

## Tests
| | pip | note |
|---|---|---|
| docker-compose / e2e | `testcontainers` | Docker-gated; mirror C# `DockerGate` skip-when-down |
| lint / types | `ruff`, `mypy` | not yet installed; add in M0 |
