"""
AWS S3 client wrapper for the cold-tier / object storage layer.

Why S3 (and not R2, which v0.1 used):
    The v0.3 lakehouse arc chose AWS S3 for portfolio authenticity and to align
    with the EMR/Iceberg phases that run natively against S3. R2's free egress
    mattered for a public-dataset goal; the cold tier's primary consumer is now
    Spark/Iceberg in the same AWS region (egress-free), so S3 is the cleaner fit.
    The API is the same boto3 surface, so call sites barely change.

Used for:
    - Daily parquet exports to the cold tier (daily_parquet_export)
    - (Future) Iceberg table data + raw snapshots as the lakehouse arc lands
"""
from __future__ import annotations

from typing import TYPE_CHECKING

import boto3
from botocore.config import Config
from dagster import ConfigurableResource, InitResourceContext
from pydantic import PrivateAttr

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


class S3Resource(ConfigurableResource):
    """Wrapper around a boto3 S3 client pointed at an AWS S3 bucket."""

    access_key_id: str | None = None
    secret_access_key: str | None = None
    bucket: str = ""
    region: str = "us-east-1"

    _client: "S3Client | None" = PrivateAttr(default=None)

    def setup_for_execution(self, context: InitResourceContext) -> None:
        # If credentials/bucket aren't configured (e.g. local dev without S3), defer
        # client init. Methods that try to use S3 raise a clear error at call time,
        # and is_configured() lets assets skip cleanly.
        if not (self.access_key_id and self.secret_access_key and self.bucket):
            return
        self._client = boto3.client(
            "s3",
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            region_name=self.region,
            config=Config(
                signature_version="s3v4",
                retries={"max_attempts": 3, "mode": "adaptive"},
            ),
        )

    def _require_client(self) -> "S3Client":
        if self._client is None:
            raise RuntimeError(
                "S3 client not configured. Set QL_S3_ACCESS_KEY_ID, "
                "QL_S3_SECRET_ACCESS_KEY, QL_S3_BUCKET in the environment."
            )
        return self._client

    def is_configured(self) -> bool:
        """True if S3 is usable. Assets that should skip cleanly when S3 is absent call this."""
        return self._client is not None

    def client(self) -> "S3Client":
        """Return the underlying boto3 S3 client (for callers that need the raw client)."""
        return self._require_client()

    def upload_file(self, local_path: str, key: str) -> None:
        """Upload a local file to s3://{bucket}/{key} (multipart-aware for large files)."""
        client = self._require_client()
        client.upload_file(local_path, self.bucket, key)

    def put_bytes(self, key: str, body: bytes, content_type: str = "application/octet-stream") -> None:
        """Write an in-memory blob to S3. Retained from the R2 interface for small writes."""
        client = self._require_client()
        client.put_object(Bucket=self.bucket, Key=key, Body=body, ContentType=content_type)