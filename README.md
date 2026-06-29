<!-- markdownlint-disable-file MD024 MD025 MD041 -->

![shopmonkey!](https://www.shopmonkey.io/static/sm-light-logo-2c92d57bf5d188bb44c1b29353579e1f.svg)

> [!IMPORTANT]
> This repository is the Python port of the v3 Enterprise Data Streaming server. If you're looking for the original Go reference implementation, see the [edsGolang](https://github.com/shopmonkeyus/eds) repository.

# Overview

This repository contains a faithful Python port of the reference implementation of the Enterprise Data Streaming server. It mirrors the behavior of the [Go reference implementation](https://github.com/shopmonkeyus/eds) command-for-command. You can find more detailed information at the [Shopmonkey Developer Portal](https://shopmonkey.dev/eds).

## Download Release Binary

You can download a release binary for different operating systems from the Release section. Each release is published as a single self-contained archive named `eds_<OS>_<arch>.zip` (Windows) or `eds_<OS>_<arch>.tar.gz` (Linux/macOS) that contains the `eds` executable (`eds.exe` on Windows). If you do not have access to a prebuilt binary, you can build one locally — see [Building a smaller binary](#building-a-smaller-binary) and [Creating a manual release locally](#creating-a-manual-release-locally).

## Get Help

```
eds --help
```

You can get help for a specific command with:

```
eds <command> --help
```

Running `eds` with no command also prints the top-level help.

## Usage

There are 3 main commands:

- **import** - used for importing data from your Shopmonkey account into a target destination
- **server** - used for running the EDS server to deliver messages from Shopmonkey to the target destination
- **version** - prints the current version of the software

In addition, the following commands are available:

- **publickey** - prints the Shopmonkey GNU PGP public key
- **enroll** - enroll a new server and obtain an API key using an enrollment code
- **download** - download a new version of the software from GitHub releases

All commands require a valid Shopmonkey API key. You can specify the key using the command line `--api-key` option or set the environment variable `SM_APIKEY`. Be careful to safeguard this key as it is like a password and will grant any user access to your protected data.

The target driver is configured using the `--url` option. The driver that is selected is based on the scheme/protocol part of the URL. The following drivers are supported:

- **mysql** - used to stream data into a MySQL database
- **postgres** - used to stream data into a PostgreSQL database (alias: `postgresql`)
- **sqlserver** - used to stream data into a Microsoft SQLServer database (alias: `mssql`)
- **snowflake** - used to stream data into a Snowflake database
- **snowflake-keypair** - used to stream data into a Snowflake database using key-pair authentication
- **s3** - used to stream data into a S3 compatible cloud storage (AWS, Google Cloud, Minio, etc)
- **kafka** - used to stream data into a Kafka topic
- **eventhub** - used to stream data to Microsoft Azure [EventHub](https://azure.microsoft.com/en-us/products/event-hubs)
- **file** - used to stream data into a folder on the local machine. This is useful for bulk export or testing locally.

You can get a list of the flags supported by the server, including driver configuration, by running the following:

```
eds server --help
```

## Importing Data

The import command will take a snapshot of the data in the Shopmonkey Database and import that data to your destination. This is useful to bootstrap or backfill a new destination with existing data. You can then run the server to stream additional incremental changes as they happen.

Example:

```
eds import --api-key 123 --url "postgresql://admin:root@localhost:5432/test?sslmode=disable"
```

This command will import data from the Shopmonkey Database into PostgreSQL database and exit once completed.

> [!CAUTION]
> The import command will remove existing data from the target destination (dependent on the specific driver). Use with caution to not lose data.

The import command will ensure that you have a valid EDS session before running an import. It will ensure that any data that is processed during the import processed will automatically be skipped when the server is started after the import to ensure duplicates aren't processed.

### Automatic Recovery

If an import run hits a recoverable error — a network or transport failure, a timeout, an HTTP `5xx`/`429`, or a failed table export — it automatically recovers: it detects which table(s) failed, re-exports and re-imports just those tables (re-downloading the existing export job while its URLs are still valid, otherwise starting a fresh export of the failed tables), and continues. Retries use exponential backoff over five attempts — `30s, 60s, 120s, 240s, 480s`. Per-table progress is recorded, so a restarted import resumes only the tables that had not finished. If a table still cannot be imported after all retries, the error is logged and that table is recorded as failed, but the remaining tables are still imported so the server can start and begin processing real-time data (the failed tables can be retried later). Recovery is controlled by `--max-retries` (default `5`; `0` disables it).

## Running the Server

> [!IMPORTANT]
> The server should be created using HQ. If you do not have access to HQ or the EDS capabilities within HQ, please contact your account team for further assistance. Running the server outside of HQ is not recommended.

Running the server will start a process which will connect to the Shopmonkey system and stream change data capture (CDC) records in JSON format to the server which will forward them intelligently to the driver for specific handling. The Server will automatically handle logging, crash detection and sending health reports back to Shopmonkey for monitoring.

When the server is started for the first time, it will create a subscription on the Shopmonkey system to register interest in your real-time CDC changes. However, if the server is shutdown after more than 7 days, the subscription will be expired and any pending data will be lost. In this case, you will have to re-import your data and start streaming again.

The server captures data is near real-time as they occur. However, EDS will attempt to intelligent batch data when a large amount of data is pending to speed up data processing. You should expect latencies of around 100-250ms when your system is not under heavy load and around 2-3s when a lot of data is pending processing. EDS server attempts to make a tradeoff of better batching and load during heavy data periods while still providing fast data access during low load periods.

### Ingest Mode (upsert / append)

The server can write change data capture (CDC) records to your destination in one of two modes. In the default **upsert** mode, each object is stored as a single row that is inserted, updated, or deleted in place, so the table always reflects the current state of your data. In **append** mode, every change is instead recorded as a new row, so the destination keeps the full history of every object (an audit trail) rather than only its latest state.

You select the mode with the `--mode` flag on the server command. The flag accepts `upsert` or `append` and defaults to `upsert`:

```
eds server --mode append --data-dir /var/data
```

The mode only needs to be set once. The resolved value is persisted to the `mode` key in the `config.toml` file in your data directory, and the server will continue to use it on subsequent runs without the flag. Resolution follows a fixed precedence: an explicit `--mode` flag wins over the `mode` value in `config.toml`, which in turn wins over the built-in default of `upsert`. An explicit `--mode` is written back to `config.toml`; if no flag is given and no `mode` key is present, the server writes `mode = "upsert"` so the persisted configuration is self-documenting after the first run. An unknown `--mode` value is a usage error and the server exits with a non-zero status.

> [!NOTE]
> Ingest mode applies to the **server** path only. The `import` command is unaffected and always behaves as before. Append mode is supported on the PostgreSQL, MySQL, SQL Server, and Snowflake drivers.

In append mode the full history is recorded in the base table `<table>` itself, with a fixed set of audit columns appended after your object columns:

- `_eds_seq` — surrogate primary key (a database identity) so many rows per object are allowed.
- `_eds_operation` — the change type: `INSERT`, `UPDATE`, or `DELETE`.
- `_eds_mvcc_timestamp` — the change-ordering timestamp used to sequence history.
- `_eds_timestamp` — the event timestamp, used as a tie-break.
- `_eds_appended_at` — the server wall-clock time the row was written.

Because the base table now holds many rows per object, the object columns become nullable except for the primary key(s). This lets a `DELETE` be recorded as a tombstone row that carries only the key plus `_eds_operation = 'DELETE'`, with every other column null.

To make the history easy to query, append mode also creates two views for each table:

- **`<table>_current`** — the latest row per object, excluding objects whose most recent change was a delete. It projects only your object columns, so it has the same shape as the table you would get in upsert mode.
- **`<table>_timeline`** — a slowly-changing-dimension (Type 2) view for point-in-time queries. It includes every change (deletes included, since a delete ends a row's validity) and adds a `valid_from` and `valid_to` range computed with a window function (`valid_to` is null while the row is still current).

For example, to retrieve the state of an object as it existed at a particular point in time `T`, query the timeline view:

```sql
SELECT * FROM order_timeline
WHERE id = 'o_123'
  AND T >= valid_from
  AND (valid_to IS NULL OR T < valid_to);
```

## Data Directory

By default, the server will store log and data files in the current working directory where you start the server. However, you can change the location of this data directory by setting the `--data-dir` to a writable directory. This directory will default to `cwd/data` if not provided and the server attempt to make this directory on startup if it does not exist.

## Monitoring the Server

By default the server runs a HTTP server on port `8080`. This can be changed either with the `--port` command line flag or by setting the `PORT` environment variable.

### Health Checks

You can access the `/` default endpoint to perform a health check. If the server is in shutdown mode, it will return a HTTP status code 503 (Service Unavailable).

### Metrics

You can access the `/metrics` endpoint to retrieve [Prometheus](http://prometheus.io/) metrics. The following metrics are available:

- `eds_pending_events`: Gaugae representing the number of pending events.
- `eds_total_events`: Counter representing the total number of events processed.
- `eds_flush_duration_seconds`: Histogram representing the duration of time in second that it takes for the driver to flush data to the destination.
- `eds_flush_count`: Histogram representing the count of events pending when flushed to the destination.

### Session

The server will automatically renew the EDS session with Shopmonkey every 24 hours. This ensures that your server credentials are short lived.

### Health Monitoring

The server will automatically send a health check event with a few system details about your server every minute. This ensures that Shopmonkey is able to monitor your EDS server and provide information for you in our HQ product.

The following system information is sent to Shopmonkey:

- Unique Machine ID
- Private IP Address
- Number of CPUs
- OS Name
- OS Architecture Name (such as arm64)
- Version of Python that the server is running with
- Version of EDS server

You can audit this information by reviewing the [sysinfo.py](./eds/util/sysinfo.py) and [consumer.py](./eds/consumer/consumer.py) files.

In addition, the server also deliver the metrics mentioned above.

### Session Logging

The server will automatically upload server logs to Shopmonkey to assist in observability, monitoring and remediation during error conditions. In addition, we provide these logs as part of the HQ product. The server logs will be sent periodically while the server is running as well as on shutdown or during a server crash.

### Crash Detection

The server will automatically detect crashes, report them to Shopmonkey and restart the system. In the event the server restarts unexpectedly more than 5 times, it will error and exit with a non-zero exit code.

## Auto Update

The server can be automatically updated from Shopmonkey HQ. This remote update capability is disabled when running inside Docker.

# Deployment

This Python port does not publish a public container image. To run EDS in a container, build the binary locally (see [Building a smaller binary](#building-a-smaller-binary)) and copy the resulting executable into your own image, or run the binary directly on the host.

When configuring EDS in Docker Compose, Kubernetes, or any other supervisor, you pass the arguments without the name of the binary such as:

```
["server", "--data-dir", "/var/data", "--verbose"]
```

> [!IMPORTANT]
> You should enroll the server outside of Docker initially and mount the generated `config.toml` file after setup into your container at runtime. This file should be treated as a secret and should not be committed to your Docker image.

## Building a smaller binary

The release binary is built with [PyInstaller](https://pyinstaller.org/) into a single self-contained executable. From the repository root, run:

```
python packaging/build.py [version]
```

This runs `python -m PyInstaller eds.spec --clean --noconfirm` under the hood and produces a packaged archive in the `dist/` directory:

- Windows: `dist/eds_<OS>_<arch>.zip` containing `eds.exe`
- Linux/macOS: `dist/eds_<OS>_<arch>.tar.gz` containing `eds`

where `<OS>` is `Windows`, `Linux`, or `Darwin` and `<arch>` is `x86_64`, `arm64`, or `i386` (for example, `dist/eds_Windows_x86_64.zip`). The version is resolved from the optional argument, then `$GIT_SHA`, then `git rev-parse --short HEAD`, and finally `dev`; it is baked into a generated `eds/_version.py` for the duration of the build.

The bundle includes only the database libraries that are installed in your environment, so installing only the driver you need (for example, just `psycopg` for PostgreSQL) yields a smaller binary.

# Security

## Verification of Published Software

To verify that the software we release are built by Shopmonkey you can use our [GNU PGP Public Key](./eds/shopmonkey.asc):

```
-----BEGIN PGP PUBLIC KEY BLOCK-----

mDMEZqbX1BYJKwYBBAHaRw8BAQdArMIL32vatKdxrJK2F/aKN+q3hS73CPnUdgpJ
KrxyOYu0LFNob3Btb25rZXksIEluYy4gPGVuZ2luZWVyaW5nQHNob3Btb25rZXku
aW8+iJMEExYKADsWIQTJlG6Z3WVVBa1f6dL+z/SRXnv0kgUCZqbX1AIbAwULCQgH
AgIiAgYVCgkICwIEFgIDAQIeBwIXgAAKCRD+z/SRXnv0kj2FAP9AfaBMaXBGr9OP
vQXHD/dC9DVqu5AWJns98A6OAMxYDAD+IDfjZGf9SsBal9/HE5j6FbuRCcl52Jwx
97f7OrIAhQa4OARmptfUEgorBgEEAZdVAQUBAQdAI7jC9e+tOyLA+k8JWvZu666l
LjXvPznbu9I2dkaLMzcDAQgHiHgEGBYKACAWIQTJlG6Z3WVVBa1f6dL+z/SRXnv0
kgUCZqbX1AIbDAAKCRD+z/SRXnv0kuoWAP91V3SLcNLaXndipxJJ/Z5oQjsyuTDy
3rhqtxmg+EsXVgD/SFc612ihYO2/DFooZ04EU4wwFjj/0u4rxcUdj04u+AI=
=r0eF
-----END PGP PUBLIC KEY BLOCK-----
```

Download this file and name is `shopmonkey.asc` and then import this file into your keyring using GNU GPG:

```
gpg --import shopmonkey.asc
```

Verify any of our released files using the following command:

```
gpg --verify somefile.sig somefile
```

You can also run the command `publickey` to print out the public key:

```
eds publickey
```

## Responsible Disclosure

Shopmonkey, Inc. welcomes feedback from security researchers and the general public to help improve our security. If you believe you have discovered a vulnerability, privacy issue, exposed data, or other security issues in any of our assets, we want to hear from you. This policy outlines steps for reporting vulnerabilities to us, what we expect, what you can expect from us.

### Our Commitment

When working with us, according to this policy, you can expect us to:

- Respond to your report promptly, and work with you to understand and validate your report;
- Strive to keep you informed about the progress of a vulnerability as it is processed;
- Work to remediate discovered vulnerabilities in a timely manner, within our operational constraints; and
- Extend Safe Harbor for your vulnerability research that is related to this policy.

### Our Expectations

In participating in our vulnerability disclosure program in good faith, we ask that you:

- Play by the rules, including following this policy and any other relevant agreements. If there is any inconsistency between this policy and any other applicable terms, the terms of this policy will prevail;
- Report any vulnerability you’ve discovered promptly;
- Avoid violating the privacy of others, disrupting our systems, destroying data, and/or harming user experience;
- Use only the Official Channels to discuss vulnerability information with us;
- Provide us a reasonable amount of time (at least 45 days from the initial report) to resolve the issue before you disclose it publicly;
- Perform testing only on in-scope systems, and respect systems and activities which are out-of-scope;
- If a vulnerability provides unintended access to data: Limit the amount of data you access to the minimum required for effectively demonstrating a Proof of Concept; and cease testing and submit a report immediately if you encounter any user data during testing, such as Personally Identifiable Information (PII), Personal Healthcare Information (PHI), credit card data, or proprietary information;
- You should only interact with test accounts you own or with explicit permission from the account holder; and
- Do not engage in extortion.

### Official Channels

Please report security issues via security@shopmonkey.io, providing all relevant information. The more details you provide, the easier it will be for us to triage and fix the issue.

### Safe Harbor

When conducting vulnerability research, according to this policy, we consider this research conducted under this policy to be:

- Authorized concerning any applicable anti-hacking laws, and we will not initiate or support legal action against you for accidental, good-faith violations of this policy;
- Authorized concerning any relevant anti-circumvention laws, and we will not bring a claim against you for circumvention of technology controls;
- Exempt from restrictions in our Terms of Service (TOS) and/or Acceptable Usage Policy (AUP) that would interfere with conducting security research, and we waive those restrictions on a limited basis; and
- Lawful, helpful to the overall security of the Internet, and conducted in good faith. You are expected, as always, to comply with all applicable laws. If legal action is initiated by a third party against you and you have complied with this policy, we will take steps to make it known that your actions were conducted in compliance with this policy.

If at any time you have concerns or are uncertain whether your security research is consistent with this policy, please submit a report through one of our Official Channels before going any further.

Note that the Safe Harbor applies only to legal claims under the control of the organization participating in this policy, and that the policy does not bind independent third parties.

# Local Development

## Requirements

You will need [Python](https://www.python.org/downloads/) version 3.10 or later (64-bit) to use this package.

Set up a virtual environment and install the package in editable mode with the development tools:

```
python -m venv .venv
pip install -e ".[dev]"
```

You can run EDS directly from source without packaging it:

```
python -m eds <command> [flags]
```

After installation the `eds` console script is also available on your `PATH`, so you can run `eds <command> ...` directly.

The full dependency set is declared in `pyproject.toml`. Runtime dependencies include `xxhash`, `msgpack`, `nats-py[nkeys]`, `requests`, `tomli` (only on Python < 3.11), `pgpy`, `jsonschema`, `prometheus-client`, and `psutil`, plus the streaming-driver libraries `boto3` (s3), `confluent-kafka` (kafka), and `azure-eventhub` (eventhub) — each lazy-imported, so it is only required when that driver is actually used. The development extra (`pip install -e ".[dev]"`) adds `pytest`, `pytest-asyncio`, `ruff`, and `mypy`.

To run the tests, linter, and type checker:

```
pytest
ruff check .
mypy eds
```

## Creating a manual release locally

To produce a release archive locally, run the PyInstaller build from the repository root:

```
python packaging/build.py
```

This produces `dist/eds_<OS>_<arch>.zip` (Windows) or `dist/eds_<OS>_<arch>.tar.gz` (Linux/macOS) containing the self-contained `eds` executable. See [Building a smaller binary](#building-a-smaller-binary) for details on the output naming and version resolution.

# License

The [Go reference implementation](https://github.com/shopmonkeyus/eds) that this port is based on is licensed under the [MIT license](https://opensource.org/licenses/MIT). A LICENSE file is not currently distributed with this port.
