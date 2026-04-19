"""
Cloudflare R2 client (S3-compatible).

Why R2 and not S3:
    Free egress is decisive when you intend to publish a public dataset.
    The API is S3-compatible, so the boto3 patterns transfer directly.

Used for:
    - Daily parquet exports (analytics-friendly format, public download)
    - Raw protobuf snapshots (debugging / replay; 7-day retention)
    - Database backups (pg_dump, encrypted)
"""
from __future__ import annotations

from typing import TYPE_CHECKING

import boto3
from botocore.config import Config
from dagster import ConfigurableResource, InitResourceContext
from pydantic import PrivateAttr

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


class R2Resource(ConfigurableResource):
    """Wrapper around a boto3 S3 client pointed at a Cloudflare R2 bucket."""

    endpoint_url: str | None = None
    access_key_id: str | None = None
    secret_access_key: str | None = None
    bucket: str = "quantumlane"
    region: str = "auto"  # R2 ignores region but boto3 requires it

    _client: "S3Client | None" = PrivateAttr(default=None)

    def setup_for_execution(self, context: InitResourceContext) -> None:
        # If credentials aren't configured (e.g. local dev without R2), defer client init.
        # Methods that try to use R2 will raise a clear error at call time.
        if not (self.endpoint_url and self.access_key_id and self.secret_access_key):
            return
        self._client = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            region_name=self.region,
            config=Config(signature_version="s3v4", retries={"max_attempts": 3, "mode": "adaptive"}),
        )

    def _require_client(self) -> "S3Client":
        if self._client is None:
            raise RuntimeError(
                "R2 client not configured. Set QL_R2_ENDPOINT_URL, QL_R2_ACCESS_KEY_ID, "
                "QL_R2_SECRET_ACCESS_KEY in the environment."
            )
        return self._client

    def put_bytes(self, key: str, body: bytes, content_type: str = "application/octet-stream") -> None:
        client = self._require_client()
        client.put_object(Bucket=self.bucket, Key=key, Body=body, ContentType=content_type)

    def is_configured(self) -> bool:
        """Returns True if R2 is usable. Used by assets that should skip cleanly when R2 is absent."""
        return self._client is not None
