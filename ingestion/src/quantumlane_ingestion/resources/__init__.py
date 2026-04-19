"""Dagster resources: long-lived shared dependencies (DB pool, HTTP client, R2 client)."""
from quantumlane_ingestion.resources.gtfs_rt import GTFSRTResource
from quantumlane_ingestion.resources.postgres import PostgresResource
from quantumlane_ingestion.resources.r2 import R2Resource

__all__ = ["GTFSRTResource", "PostgresResource", "R2Resource"]
