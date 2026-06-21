"""
Configuration loaded from environment variables.

Why pydantic-settings:
    Type-checked config at boot time. A typo in a required env var fails fast
    with a clear error rather than silently None'ing through the system.

Env var conventions:
    All vars are prefixed QL_ to namespace cleanly. See .env.example for the full list.
"""
from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Top-level config. Reads from environment and from .env in development."""

    model_config = SettingsConfigDict(
        env_prefix="QL_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- Database ---
    postgres_dsn: str = Field(
        default="postgresql://quantumlane:quantumlane@postgres:5432/quantumlane",
        description="PostgreSQL DSN. Example: postgresql://user:pass@host:5432/db",
    )
    postgres_pool_size: int = Field(default=5, ge=1, le=50)

    # --- TTC GTFS-RT feeds ---
    # The TTC publishes GTFS-RT at a known base URL. Configurable for testing.
    ttc_vehicle_positions_url: str = Field(
        default="https://bustime.ttc.ca/gtfsrt/vehicles",
        description="TTC VehiclePositions feed URL.",
    )
    ttc_trip_updates_url: str = Field(
        default="https://bustime.ttc.ca/gtfsrt/trips",
        description="TTC TripUpdates feed URL.",
    )
    ttc_service_alerts_url: str = Field(
        default="https://bustime.ttc.ca/gtfsrt/alerts",
        description="TTC ServiceAlerts feed URL.",
    )
    ttc_static_gtfs_url: str = Field(
        default="https://opendata.toronto.ca/ttc/routes-and-schedules/OpenData_TTC_Schedules.zip",
        description="TTC static GTFS zip URL.",
    )

    # --- HTTP ---
    http_timeout_seconds: float = Field(default=15.0, ge=1.0, le=120.0)
    http_user_agent: str = Field(
        default="QuantumLane/0.1 (https://quantumlane.io; contact@quantumlane.io)",
        description="UA string sent with every outbound request. Be a polite citizen.",
    )

    # --- S3 / object storage (cold tier) ---
    # AWS S3 replaced Cloudflare R2 in the v0.3 lakehouse arc (S3-native EMR/Iceberg).
    # Credentials are the quantumlane-spark IAM user's keys; bucket is the cold-tier bucket.
    # NOTE: env_prefix is "QL_", so these read QL_S3_ACCESS_KEY_ID, QL_S3_SECRET_ACCESS_KEY,
    # QL_S3_BUCKET, QL_S3_REGION. If your .env currently uses QL_AWS_ACCESS_KEY_ID /
    # QL_AWS_SECRET_ACCESS_KEY (from the V0.3.0 setup), either rename them to the QL_S3_*
    # form or add validation_alias entries — see the comment on each field below.
    s3_access_key_id: str | None = Field(
        default=None,
        description="AWS access key id for the cold-tier bucket (QL_S3_ACCESS_KEY_ID).",
    )
    s3_secret_access_key: str | None = Field(
        default=None,
        description="AWS secret access key for the cold-tier bucket (QL_S3_SECRET_ACCESS_KEY).",
    )
    s3_bucket: str = Field(
        default="",
        description="Cold-tier S3 bucket name (QL_S3_BUCKET).",
    )
    s3_region: str = Field(
        default="us-east-1",
        description="AWS region of the cold-tier bucket (QL_S3_REGION).",
    )

    # --- Operational ---
    environment: str = Field(default="development", pattern="^(development|staging|production)$")
    log_level: str = Field(default="INFO", pattern="^(DEBUG|INFO|WARNING|ERROR)$")


_settings: Settings | None = None


def get_settings() -> Settings:
    """Cached singleton accessor. Use this rather than constructing Settings() directly."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings