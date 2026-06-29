"""PARITY: internal/drivers/s3/s3.go — the S3 streaming driver + importer.

Each DBChangeEvent is written as a JSON object at ``<prefix><table>/<unixSeconds>-<pk>.json`` (or, when the
event carries a SchemaValidatedPath, ``<prefix>/<validatedPath>``) to an S3-compatible destination — AWS S3,
Google Cloud Storage (S3 interop), or LocalStack. The pure URL/key/Validate logic (provider detect, bucket /
prefix split, object-key naming, the s3:// URL builder) lives here as module functions (the C# port isolates
it as S3Url.cs) and is golden-testable WITHOUT boto3.

LAZY-import boto3: the SDK is imported only inside ``_connect`` (mirrors snowflake's lazy connector), so the
unit/golden tests run with boto3 absent.

DEVIATION: see DEVIATIONS.md#s3-buffered-upload — Go streams each event through a worker-pool channel during
Process; the port buffers per batch and uploads with bounded (uploadTasks) concurrency at Flush. Same objects,
same keys, and the SAME error-surfacing point (Flush). DEVIATION: see DEVIATIONS.md#s3-gcs-resign-not-ported.
"""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from enum import IntEnum
from typing import Any

from eds.dbchange import DBChangeEvent
from eds.driver import (
    DriverConfig,
    DriverField,
    FieldError,
    ImporterConfig,
    get_optional_string_value,
    get_required_string_value,
    optional_password_field,
    optional_string_field,
    required_string_field,
)
from eds.schema import Schema, SchemaMap
from eds.util import gourl
from eds.util.file import is_localhost
from eds.util.gojson import stringify
from eds.util.help import generate_help_section
from eds.util.logger import Logger

# PARITY: maxBatchSize default 1000, uploadTasks default 4.
_MAX_BATCH_SIZE = 1_000
_DEFAULT_UPLOAD_TASKS = 4


class S3Provider(IntEnum):
    """PARITY: s3Provider iota (aws=0, google=1, localstack=2)."""

    AWS = 0
    GOOGLE = 1
    LOCALSTACK = 2


def get_cloud_provider(host: str) -> S3Provider:
    """PARITY: getCloudProvider — localhost → LocalStack; googleapis.com → Google; else AWS."""
    if is_localhost(host):
        return S3Provider.LOCALSTACK
    if "googleapis.com" in host:
        return S3Provider.GOOGLE
    return S3Provider.AWS


def add_final_slash(s: str) -> str:
    """PARITY: addFinalSlash — "" stays ""; otherwise ensure a single trailing '/'."""
    if s == "":
        return ""
    if not s.endswith("/"):
        return s + "/"
    return s


def _trim_leading_slash(s: str) -> str:
    """PARITY: strings.TrimPrefix(s, "/") — strip exactly ONE leading slash (NOT all, so 'host//bucket'
    matches Go's behavior rather than collapsing)."""
    return s[1:] if s.startswith("/") else s


def get_bucket_info(host: str, path: str, provider: S3Provider) -> tuple[str, str, str]:
    """PARITY: getBucketInfo — returns (endpoint, bucket, prefix).

    For AWS the bucket is the host and the prefix is the whole path; for GCS/LocalStack the first path segment
    is the bucket and the rest is the prefix, and the endpoint is the (scheme-prefixed) host."""
    if provider == S3Provider.AWS:
        return "", host, add_final_slash(_trim_leading_slash(path))
    bucket = ""
    prefix = ""
    parts = _trim_leading_slash(path).split("/")
    if parts:
        bucket = parts[0]
        prefix = add_final_slash("/".join(parts[1:]))
    if provider == S3Provider.LOCALSTACK:
        return "http://" + host, bucket, prefix
    return "https://" + host, bucket, prefix


def _path_join(*parts: str) -> str:
    """PARITY: path.Join — join non-empty parts with '/', then collapse duplicate slashes (sufficient for the
    S3 keys EDS builds; matches the C# S3Url.PathJoin)."""
    joined = "/".join(p for p in parts if p)
    while "//" in joined:
        joined = joined.replace("//", "/")
    return joined


def object_key(prefix: str, event: DBChangeEvent) -> str:
    """PARITY: the process() key naming — SchemaValidatedPath wins, else
    ``<prefix><table>/<unixSeconds>-<pk>.json`` (time.UnixMilli(ts).Unix())."""
    if event.schema_validated_path is not None:
        return _path_join(prefix, event.schema_validated_path)
    unix_seconds = event.timestamp // 1000
    return _path_join(prefix, event.table, f"{unix_seconds}-{event.get_primary_key()}.json")


def validate_config(values: dict[str, Any]) -> tuple[str, list[FieldError]]:
    """PARITY: Validate — build an s3:// URL (sorted, escaped query) from the config fields."""
    field_errors: list[FieldError] = []
    bucket, field_error = get_required_string_value("Bucket", values)
    if field_error is not None:
        field_errors.append(field_error)
    prefix = get_optional_string_value("Prefix", "", values)
    region = get_optional_string_value("Region", "", values)
    accesskey = get_optional_string_value("Access Key ID", "", values)
    secret = get_optional_string_value("Secret Access Key", "", values)
    endpoint = get_optional_string_value("Endpoint", "", values)

    u = gourl.GoUrl(scheme="s3")
    if endpoint != "":
        u.host = endpoint
        u.path = bucket
    else:
        u.host = bucket
    if prefix != "":
        if u.path == "":
            u.path = prefix
        else:
            u.path = _path_join(u.path, prefix)
    q = gourl.Values()
    if region != "":
        q.set("region", region)
    if accesskey != "":
        q.set("access-key-id", accesskey)
    if secret != "":
        q.set("secret-access-key", secret)
    u.raw_query = q.encode()  # PARITY: url.Values.Encode SORTS keys

    if field_errors:
        return "", field_errors
    return str(u), []


def _atoi(s: str) -> int:
    """PARITY: strconv.Atoi — base-10, optional sign, ASCII digits only."""
    body = s[1:] if s[:1] in ("+", "-") else s
    if not body or not all("0" <= c <= "9" for c in body):
        raise ValueError(f"invalid int: {s}")
    return int(s)


class S3Driver:
    """PARITY: s3Driver."""

    def __init__(self) -> None:
        self._config: DriverConfig | None = None
        self._logger: Logger | None = None
        self._bucket = ""
        self._prefix = ""
        self._client: Any = None  # boto3 S3 client (lazy)
        self._upload_tasks = _DEFAULT_UPLOAD_TASKS
        self._import_config: ImporterConfig | None = None
        self._pending: list[tuple[str, DBChangeEvent]] = []

    # ---- connection ----
    def _resolve_region(self, q: gourl.Values) -> str:
        """PARITY: region resolution (s3.go:167-182). The query "region" key is the FINAL word — if present it
        wins even when EMPTY (Go's trailing `if u.Query().Has("region") { region = Get("region") }` overrides),
        otherwise $AWS_REGION, then $AWS_DEFAULT_REGION, then "us-west-2"."""
        if q.has("region"):
            return q.get("region")  # PARITY: explicit (possibly empty) ?region= overrides
        env = os.environ.get("AWS_REGION")
        if env:
            return env
        default = os.environ.get("AWS_DEFAULT_REGION")
        return default if default else "us-west-2"

    @staticmethod
    def _query_or_env(q: gourl.Values, key: str, env_var: str) -> str:
        if q.has(key):
            return q.get(key)
        return os.environ.get(env_var, "")

    def _resolve_max_retries(self, q: gourl.Values) -> int:
        if not q.has("max-retries"):
            return 5
        raw = q.get("max-retries")
        try:
            return _atoi(raw)
        except ValueError:
            if self._logger is not None:
                self._logger.warn("skipping max-retires value: %s", raw)
            return 5

    @staticmethod
    def _resolve_positive_int(q: gourl.Values, key: str, default: int) -> int:
        """PARITY: maxBatchSize/uploadTasks — empty → default; unparseable → raise; <=0 → default."""
        raw = q.get(key)
        if raw == "":
            return default
        try:
            n = _atoi(raw)
        except ValueError as e:
            raise ValueError(f"unable to parse {key}: {raw}") from e
        return default if n <= 0 else n

    def _connect(self, url_string: str, testonly: bool) -> None:
        """PARITY: connect — parse the URL, resolve provider/credentials/region, build the boto3 client.

        boto3/botocore are imported HERE so unit/golden tests run without them installed."""
        u = gourl.parse(url_string)
        provider = get_cloud_provider(u.host)
        endpoint, bucket, prefix = get_bucket_info(u.host, u.path, provider)
        self._bucket = bucket
        self._prefix = prefix

        q = u.query()
        region = self._resolve_region(q)
        access_key_id = self._query_or_env(q, "access-key-id", "AWS_ACCESS_KEY_ID")
        secret_access_key = self._query_or_env(q, "secret-access-key", "AWS_SECRET_ACCESS_KEY")
        session_token = self._query_or_env(q, "session-token", "AWS_SESSION_TOKEN")
        max_retries = self._resolve_max_retries(q)
        # PARITY: maxBatchSize only sized Go's worker-pool channel (gone with the buffered design), but Go still
        # aborts startup on a non-empty unparseable value — keep that validation (result unused).
        self._resolve_positive_int(q, "maxBatchSize", _MAX_BATCH_SIZE)
        self._upload_tasks = self._resolve_positive_int(q, "uploadTasks", _DEFAULT_UPLOAD_TASKS)

        import boto3  # noqa: PLC0415 — lazy: keep boto3 off the unit/golden import path
        from botocore.config import Config  # noqa: PLC0415

        # DEVIATION: see DEVIATIONS.md#s3-gcs-resign-not-ported — Go's GCS Accept-Encoding V4 re-sign is an
        # aws-sdk-go-v2-specific workaround and is not reproduced (as in the C# port).
        cfg_kwargs: dict[str, Any] = {
            "retries": {"max_attempts": max_retries, "mode": "standard"},
            "s3": {"addressing_style": "path"},  # PARITY: o.UsePathStyle = true
        }
        # PARITY: s3.go:211 always wires a StaticCredentialsProvider, even when the values are empty — so pass
        # the RAW strings (NOT `... or None`). Empty `or None` would make botocore fall back to its default
        # credential chain (IAM/profile/SSO); Go keeps empty creds STATIC (they fail fast), so we must too.
        client_kwargs: dict[str, Any] = {
            "aws_access_key_id": access_key_id,
            "aws_secret_access_key": secret_access_key,
            "aws_session_token": session_token,
        }
        if provider == S3Provider.GOOGLE:
            client_kwargs["region_name"] = "auto"
            client_kwargs["endpoint_url"] = "https://storage.googleapis.com"
            cfg_kwargs["request_checksum_calculation"] = "when_required"
        else:
            client_kwargs["region_name"] = region
            if endpoint != "":
                client_kwargs["endpoint_url"] = endpoint
        self._client = boto3.client("s3", config=Config(**cfg_kwargs), **client_kwargs)

        if testonly:
            return
        if self._logger is not None:
            self._logger.debug("setting uploadTasks=%d", self._upload_tasks)

    # ---- lifecycle ----
    def start(self, config: DriverConfig) -> None:
        """PARITY: Start."""
        assert config.logger is not None
        self._config = config
        self._logger = config.logger.with_prefix("[s3]")
        self._connect(config.url, testonly=False)

    def stop(self) -> None:
        """PARITY: Stop."""
        if self._logger is not None:
            self._logger.debug("stopping s3 driver")
            self._logger.debug("stopped s3 driver")

    def max_batch_size(self) -> int:
        """PARITY: MaxBatchSize — hard-coded 1000."""
        return _MAX_BATCH_SIZE

    # ---- streaming ----
    def _process(self, logger: Logger, event: DBChangeEvent, dry_run: bool) -> bool:
        key = object_key(self._prefix, event)
        if dry_run:
            logger.trace("would store %s:%s", self._bucket, key)
        else:
            self._pending.append((key, event))
        return False

    def process(self, logger: Logger, event: DBChangeEvent) -> bool:
        """PARITY: Process."""
        return self._process(logger, event, False)

    def flush(self, logger: Logger) -> None:
        """PARITY: Flush — upload all pending objects with bounded concurrency; surface any errors here."""
        logger.debug("flush called")
        if self._pending:
            jobs = self._pending
            self._pending = []
            errors: list[Exception] = []

            def _upload(job: tuple[str, DBChangeEvent]) -> None:
                key, event = job
                buf = stringify(event).encode("utf-8")
                try:
                    self._client.put_object(
                        Bucket=self._bucket, Key=key, ContentType="application/json", Body=buf
                    )
                    logger.trace("uploaded to %s:%s", self._bucket, key)
                except Exception as e:  # noqa: BLE001 — collect + join, mirroring Go's errors channel
                    errors.append(RuntimeError(f"error storing s3 object to {self._bucket}:{key}: {e}"))

            with ThreadPoolExecutor(max_workers=self._upload_tasks) as pool:
                list(pool.map(_upload, jobs))
            if errors:
                logger.debug("flush finished")
                # PARITY: errors.Join(errs...) renders multiple errors newline-separated.
                raise errors[0] if len(errors) == 1 else RuntimeError(
                    "\n".join(str(e) for e in errors)
                )
        logger.debug("flush finished")

    def test(self, ctx: Any, logger: Logger, url: str) -> None:
        """PARITY: Test — connect only (testonly)."""
        self._logger = logger.with_prefix("[s3]")
        self._connect(url, testonly=True)

    # ---- DriverHelp ----
    def name(self) -> str:
        return "AWS S3"

    def description(self) -> str:
        return "Supports streaming EDS messages to a AWS S3 compatible destination."

    def example_url(self) -> str:
        return (
            "s3://bucket/folder?region=us-west-2&access-key-id=AKIAIOSFODNN7EXAMPLE"
            "&secret-access-key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        )

    def help(self) -> str:
        return (
            generate_help_section(
                "AWS",
                "If using AWS, no special configuration is required and you can use the standard AWS "
                "environment variables to configure the access key, secret and region.\n",
            )
            + "\n"
            + generate_help_section(
                "Google Cloud Storage",
                "To use GCS for storage, use the following url pattern: s3://storage.googleapis.com/bucket. "
                "See https://cloud.google.com/storage/docs/interoperability\n",
            )
            + "\n"
            + generate_help_section(
                "LocalStack",
                "To use localstack for testing, use the following url pattern: s3://localhost:4566/bucket.\n",
            )
        )

    # ---- import Handler ----
    def create_datasource(self, schema: SchemaMap) -> None:
        """PARITY: CreateDatasource — no-op."""

    def import_event(self, event: DBChangeEvent, schema: Schema) -> None:
        """PARITY: ImportEvent."""
        assert self._logger is not None
        dry_run = self._import_config.dry_run if self._import_config is not None else False
        self._process(self._logger, event, dry_run)

    def import_completed(self) -> None:
        """PARITY: ImportCompleted — flush."""
        assert self._logger is not None
        self.flush(self._logger)

    def run_import(self, config: ImporterConfig) -> None:
        """PARITY: Import."""
        if config.schema_only:
            return
        assert config.logger is not None
        self._logger = config.logger.with_prefix("[s3]")
        self._import_config = config
        self._connect(config.url, testonly=False)
        from eds.importer import run as importer_run  # noqa: PLC0415

        importer_run(self._logger, config, self)

    def supports_delete(self) -> bool:
        return False

    # ---- config ----
    def configuration(self) -> list[DriverField]:
        return [
            required_string_field("Bucket", "The bucket name", None),
            optional_string_field("Prefix", "The prefix to prepend to the filename", None),
            optional_string_field("Region", "The AWS region to use", None),
            optional_password_field("Access Key ID", "The AWS AWS Key ID", None),
            optional_password_field("Secret Access Key", "The AWS Secret Access Key", None),
            optional_string_field(
                "Endpoint", "The Endpoint hostname to override if using an AWS compatible provider", None
            ),
        ]

    def validate(self, values: dict[str, Any]) -> tuple[str, list[FieldError]]:
        """PARITY: Validate."""
        return validate_config(values)


__all__ = [
    "S3Driver",
    "S3Provider",
    "add_final_slash",
    "get_bucket_info",
    "get_cloud_provider",
    "object_key",
    "validate_config",
]
