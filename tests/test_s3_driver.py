"""PARITY: internal/drivers/s3/s3_test.go — golden vectors (getBucketInfo, object key, Validate) + the
buffered upload/flush and importer Handler. All pure (no boto3): the SDK is lazy-imported only at connect."""

from __future__ import annotations

import pytest

from eds.dbchange import DBChangeEvent
from eds.driver import ImporterConfig
from eds.drivers.s3 import (
    S3Driver,
    S3Provider,
    add_final_slash,
    get_bucket_info,
    get_cloud_provider,
    object_key,
    validate_config,
)
from eds.util.gojson import RawJson, stringify


class _QuietLogger:
    def trace(self, m, *a): ...
    def debug(self, m, *a): ...
    def info(self, m, *a): ...
    def warn(self, m, *a): ...
    def error(self, m, *a): ...
    def fatal(self, m, *a): ...
    def with_prefix(self, p): return self
    def with_fields(self, f): return self


class _FakeS3Client:
    def __init__(self) -> None:
        self.puts: list[dict] = []

    def put_object(self, **kwargs) -> None:
        self.puts.append(kwargs)


# ---- getCloudProvider ----
def test_get_cloud_provider() -> None:
    assert get_cloud_provider("localhost:4566") == S3Provider.LOCALSTACK
    assert get_cloud_provider("127.0.0.1:4566") == S3Provider.LOCALSTACK
    assert get_cloud_provider("storage.googleapis.com") == S3Provider.GOOGLE
    assert get_cloud_provider("my-bucket.s3.amazonaws.com") == S3Provider.AWS
    assert get_cloud_provider("bucket") == S3Provider.AWS


def test_add_final_slash() -> None:
    assert add_final_slash("") == ""
    assert add_final_slash("a") == "a/"
    assert add_final_slash("a/") == "a/"


# ---- getBucketInfo (PARITY: TestGetBucketInfo*) ----
@pytest.mark.parametrize(
    ("host", "path", "provider", "expected"),
    [
        ("bucket", "", S3Provider.AWS, ("", "bucket", "")),
        ("foo-shopmonkey", "/test", S3Provider.AWS, ("", "foo-shopmonkey", "test/")),
        ("localhost:4566", "/foo-shopmonkey/test", S3Provider.LOCALSTACK,
         ("http://localhost:4566", "foo-shopmonkey", "test/")),
        ("localhost:4566", "/foo-shopmonkey/test/", S3Provider.LOCALSTACK,
         ("http://localhost:4566", "foo-shopmonkey", "test/")),
        ("127.0.0.1:4566", "/foo-shopmonkey/test/", S3Provider.LOCALSTACK,
         ("http://127.0.0.1:4566", "foo-shopmonkey", "test/")),
        ("storage.googleapis.com", "/eds-import", S3Provider.GOOGLE,
         ("https://storage.googleapis.com", "eds-import", "")),
        ("storage.googleapis.com", "/eds-import/withprefix", S3Provider.GOOGLE,
         ("https://storage.googleapis.com", "eds-import", "withprefix/")),
        ("storage.googleapis.com", "/eds-import/with/prefix", S3Provider.GOOGLE,
         ("https://storage.googleapis.com", "eds-import", "with/prefix/")),
        ("storage.googleapis.com", "/eds-import/with/prefix/", S3Provider.GOOGLE,
         ("https://storage.googleapis.com", "eds-import", "with/prefix/")),
    ],
)
def test_get_bucket_info(host, path, provider, expected) -> None:
    assert get_bucket_info(host, path, provider) == expected


# ---- object key (PARITY: TestSchemaValidationPath / TestEventPathWithPrefix / TestEventPathNoPrefix) ----
def test_object_key_schema_validated_path() -> None:
    _, _, prefix = get_bucket_info("storage.googleapis.com", "/eds-import/withprefix", S3Provider.GOOGLE)
    e = DBChangeEvent()
    e.schema_validated_path = "a/b/c"
    assert object_key(prefix, e) == "withprefix/a/b/c"


def test_object_key_with_prefix() -> None:
    _, _, prefix = get_bucket_info("storage.googleapis.com", "/eds-import/withprefix", S3Provider.GOOGLE)
    e = DBChangeEvent(table="table", key=["pk"], timestamp=500000000)
    assert object_key(prefix, e) == "withprefix/table/500000-pk.json"


def test_object_key_no_prefix() -> None:
    e = DBChangeEvent(table="table", key=["pk"], timestamp=2000)
    assert object_key("", e) == "table/2-pk.json"


# ---- Validate (PARITY: TestValidate) ----
@pytest.mark.parametrize(
    ("config", "expected_url", "expect_error"),
    [
        ({"Bucket": "bucket"}, "s3://bucket", False),
        ({"Bucket": "bucket", "Prefix": "prefix"}, "s3://bucket/prefix", False),
        ({"Bucket": "bucket", "Prefix": "/prefix"}, "s3://bucket/prefix", False),
        ({"Bucket": "bucket", "Prefix": "/prefix", "Endpoint": "storage.googleapis.com"},
         "s3://storage.googleapis.com/bucket/prefix", False),
        ({"Bucket": "bucket", "Prefix": "prefix", "Endpoint": "storage.googleapis.com"},
         "s3://storage.googleapis.com/bucket/prefix", False),
        ({"Bucket": "bucket", "Endpoint": "storage.googleapis.com"},
         "s3://storage.googleapis.com/bucket", False),
        ({"Bucket": "bucket", "Access Key ID": "AKIAIOSFODNN7EXAMPLE",
          "Secret Access Key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"},
         "s3://bucket?access-key-id=AKIAIOSFODNN7EXAMPLE"
         "&secret-access-key=wJalrXUtnFEMI%2FK7MDENG%2FbPxRfiCYEXAMPLEKEY", False),
        ({"Bucket": "bucket", "Region": "us-east-1", "Access Key ID": "AKIAIOSFODNN7EXAMPLE",
          "Secret Access Key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"},
         "s3://bucket?access-key-id=AKIAIOSFODNN7EXAMPLE&region=us-east-1"
         "&secret-access-key=wJalrXUtnFEMI%2FK7MDENG%2FbPxRfiCYEXAMPLEKEY", False),
        ({"Bucket": "bucket", "Endpoint": "storage.googleapis.com", "Prefix": "/foo", "Region": "us-east-1",
          "Access Key ID": "AKIAIOSFODNN7EXAMPLE", "Secret Access Key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"},
         "s3://storage.googleapis.com/bucket/foo?access-key-id=AKIAIOSFODNN7EXAMPLE&region=us-east-1"
         "&secret-access-key=wJalrXUtnFEMI%2FK7MDENG%2FbPxRfiCYEXAMPLEKEY", False),
        ({"Region": "us-east-1"}, "", True),
    ],
)
def test_validate(config, expected_url, expect_error) -> None:
    url, errs = validate_config(config)
    if expect_error:
        assert len(errs) >= 1
        assert url == ""
    else:
        assert errs == []
        assert url == expected_url


# ---- metadata ----
def test_metadata() -> None:
    d = S3Driver()
    assert d.name() == "AWS S3"
    assert d.description() == "Supports streaming EDS messages to a AWS S3 compatible destination."
    assert d.example_url().startswith("s3://bucket/folder?")
    assert d.max_batch_size() == 1_000
    assert d.supports_delete() is False
    assert [f.name for f in d.configuration()] == [
        "Bucket", "Prefix", "Region", "Access Key ID", "Secret Access Key", "Endpoint",
    ]


def test_help_has_sections() -> None:
    h = S3Driver().help()
    assert "AWS" in h
    assert "Google Cloud Storage" in h
    assert "LocalStack" in h


# ---- buffered upload + flush (fake client, no boto3) ----
def test_process_buffers_and_flush_uploads() -> None:
    d = S3Driver()
    client = _FakeS3Client()
    d._client = client
    d._bucket = "b"
    d._prefix = ""
    d._upload_tasks = 2
    evt = DBChangeEvent(
        operation="INSERT", id="evt1", table="customer", key=["c1"], timestamp=1_700_000_000_000,
        after=RawJson('{"id":"c1","name":"Bob"}'),
    )
    assert d.process(_QuietLogger(), evt) is False
    assert client.puts == []  # buffered, not yet uploaded
    d.flush(_QuietLogger())
    assert len(client.puts) == 1
    put = client.puts[0]
    assert put["Bucket"] == "b"
    assert put["Key"] == "customer/1700000000-c1.json"
    assert put["ContentType"] == "application/json"
    assert put["Body"] == stringify(evt).encode("utf-8")
    # flush drains pending
    d.flush(_QuietLogger())
    assert len(client.puts) == 1


def test_flush_raises_on_upload_error() -> None:
    class _BoomClient:
        def put_object(self, **kwargs):
            raise RuntimeError("boom")

    d = S3Driver()
    d._client = _BoomClient()
    d._bucket = "b"
    d._prefix = ""
    d.process(_QuietLogger(), DBChangeEvent(table="t", key=["k"], timestamp=1000))
    with pytest.raises(Exception, match="error storing s3 object"):
        d.flush(_QuietLogger())


# ---- importer Handler ----
def test_import_schema_only_returns_without_connect() -> None:
    d = S3Driver()
    d.run_import(ImporterConfig(url="s3://bucket", logger=_QuietLogger(), schema_only=True))
    assert d._client is None  # never connected


def test_import_event_dry_run_does_not_buffer() -> None:
    d = S3Driver()
    d._logger = _QuietLogger()
    d._prefix = ""
    d._import_config = ImporterConfig(dry_run=True)
    d.import_event(DBChangeEvent(table="t", key=["k"], timestamp=1000), None)
    assert d._pending == []


def test_import_completed_flushes() -> None:
    d = S3Driver()
    client = _FakeS3Client()
    d._client = client
    d._bucket = "b"
    d._prefix = "p/"
    d._logger = _QuietLogger()
    d._import_config = ImporterConfig(dry_run=False)
    d.import_event(DBChangeEvent(table="t", key=["k"], timestamp=2000), None)
    d.import_completed()
    assert len(client.puts) == 1
    assert client.puts[0]["Key"] == "p/t/2-k.json"
