"""
Dagster Definitions — the entry point that wires assets, resources, and schedules together.

This is what `dagster dev` and the dagster-webserver/daemon containers load.
The module path is referenced in pyproject.toml under [tool.dagster].
"""
from __future__ import annotations

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)

from quantumlane_ingestion.assets import ops as ops_assets
from quantumlane_ingestion.assets import ttc as ttc_assets
from quantumlane_ingestion.resources import GTFSRTResource, PostgresResource, R2Resource
from quantumlane_ingestion.settings import get_settings

settings = get_settings()

# Resources are constructed once per code-server lifetime.
# Pulling all values from settings (env-driven) keeps the wiring honest:
# the same Definitions module works locally and in production.
resources = {
    "postgres": PostgresResource(
        dsn=settings.postgres_dsn,
        pool_max_size=settings.postgres_pool_size,
    ),
    "gtfs_rt": GTFSRTResource(
        timeout_seconds=settings.http_timeout_seconds,
        user_agent=settings.http_user_agent,
    ),
    "r2": R2Resource(
        endpoint_url=settings.r2_endpoint_url,
        access_key_id=settings.r2_access_key_id,
        secret_access_key=settings.r2_secret_access_key,
        bucket=settings.r2_bucket,
    ),
}

all_assets = [
    *load_assets_from_modules([ttc_assets]),
    *load_assets_from_modules([ops_assets]),
]

# Jobs grouped by cadence. Each job materializes a specific selection of assets.
ttc_realtime_30s_job = define_asset_job(
    name="ttc_realtime_30s_job",
    selection=AssetSelection.assets("ttc_vehicle_positions", "ttc_trip_updates"),
    description="High-frequency TTC realtime feeds (vehicle positions + trip updates).",
)

ttc_alerts_5m_job = define_asset_job(
    name="ttc_alerts_5m_job",
    selection=AssetSelection.assets("ttc_service_alerts"),
    description="Service alerts; lower cadence is sufficient.",
)

freshness_1m_job = define_asset_job(
    name="freshness_1m_job",
    selection=AssetSelection.assets("freshness_check"),
    description="Per-feed freshness snapshot. Drives the public /freshness page.",
)

partition_maintenance_daily_job = define_asset_job(
    name="partition_maintenance_daily_job",
    selection=AssetSelection.assets("daily_partition_maintenance"),
)

parquet_export_daily_job = define_asset_job(
    name="parquet_export_daily_job",
    selection=AssetSelection.assets("daily_parquet_export"),
)

static_gtfs_daily_job = define_asset_job(
    name="static_gtfs_daily_job",
    selection=AssetSelection.assets("ttc_static_gtfs"),
)

# Schedules. All times in America/Toronto for the daily jobs; cron for sub-minute uses minutes only.
# Note: Dagster cron supports "*/N" syntax. For 30-second cadence we use a sensor in v0.2;
# in v0.1 we settle for every-minute scheduling and accept slightly less frequent polling
# than the design ideal. This is a deliberate v0.1 simplification — see ADR-011.
schedules = [
    ScheduleDefinition(
        name="ttc_realtime_every_minute",
        job=ttc_realtime_30s_job,
        cron_schedule="* * * * *",
        default_status=DefaultScheduleStatus.RUNNING,
    ),
    ScheduleDefinition(
        name="ttc_alerts_every_5_minutes",
        job=ttc_alerts_5m_job,
        cron_schedule="*/5 * * * *",
        default_status=DefaultScheduleStatus.RUNNING,
    ),
    ScheduleDefinition(
        name="freshness_every_minute",
        job=freshness_1m_job,
        cron_schedule="* * * * *",
        default_status=DefaultScheduleStatus.RUNNING,
    ),
    ScheduleDefinition(
        name="partition_maintenance_daily",
        job=partition_maintenance_daily_job,
        cron_schedule="0 2 * * *",
        execution_timezone="America/Toronto",
        default_status=DefaultScheduleStatus.RUNNING,
    ),
    ScheduleDefinition(
        name="parquet_export_daily",
        job=parquet_export_daily_job,
        cron_schedule="0 3 * * *",
        execution_timezone="America/Toronto",
        default_status=DefaultScheduleStatus.RUNNING,
    ),
    ScheduleDefinition(
        name="static_gtfs_daily",
        job=static_gtfs_daily_job,
        cron_schedule="0 4 * * *",
        execution_timezone="America/Toronto",
        default_status=DefaultScheduleStatus.RUNNING,
    ),
]

defs = Definitions(
    assets=all_assets,
    resources=resources,
    jobs=[
        ttc_realtime_30s_job,
        ttc_alerts_5m_job,
        freshness_1m_job,
        partition_maintenance_daily_job,
        parquet_export_daily_job,
        static_gtfs_daily_job,
    ],
    schedules=schedules,
)
