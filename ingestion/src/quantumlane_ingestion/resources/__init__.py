"""Dagster resources: long-lived shared dependencies (DB pool, HTTP client, S3 client)."""
from quantumlane_ingestion.resources.gtfs_rt import GTFSRTResource
from quantumlane_ingestion.resources.postgres import PostgresResource
from quantumlane_ingestion.resources.s3 import S3Resource

__all__ = ["GTFSRTResource", "PostgresResource", "S3Resource"]
