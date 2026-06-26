"""
QuantumLane public API.

Read-only. Public. Rate-limited but unauthenticated.
This is what the website calls and what external users can hit directly.

Run locally:
    uvicorn quantumlane_api.main:app --reload --port 8000
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime

import structlog
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from quantumlane_api import db
from quantumlane_api.schemas import (
    Agency,
    DailyStat,
    Envelope,
    FeedFreshness,
    IngestionRun,
    Meta,
    Route,
    VehiclePosition,
)
from quantumlane_api.settings import get_settings

log = structlog.get_logger(__name__)
settings = get_settings()
limiter = Limiter(key_func=get_remote_address, default_limits=[f"{settings.rate_limit_per_minute}/minute"])


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_pool()
    log.info("api_started", env=settings.environment)
    yield
    db.close_pool()
    log.info("api_stopped")


app = FastAPI(
    title="QuantumLane API",
    description=(
        "Public read-only API for GTA transit data. "
        "Rate-limited to 60 req/min/IP. No auth required. "
        "See https://quantumlane.io for the project."
    ),
    version="0.4.0",
    lifespan=lifespan,
    root_path="/api",
)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)


def _meta(data_age_seconds: int | None = None) -> Meta:
    return Meta(fetched_at=datetime.now(UTC), data_age_seconds=data_age_seconds)


# -----------------------------------------------------------------------------
# Health / readiness — no rate limit, no envelope
# -----------------------------------------------------------------------------

@app.api_route("/health", methods=["GET", "HEAD"], tags=["ops"], summary="Liveness probe")
def health() -> dict:
    return {"status": "ok"}


@app.get("/ready", tags=["ops"], summary="Readiness probe (DB-aware)")
def ready() -> JSONResponse:
    if db.ping():
        return JSONResponse(content={"status": "ready"}, status_code=status.HTTP_200_OK)
    return JSONResponse(content={"status": "db_unreachable"}, status_code=status.HTTP_503_SERVICE_UNAVAILABLE)


# -----------------------------------------------------------------------------
# v1 endpoints
# -----------------------------------------------------------------------------

@app.get("/v1/agencies", tags=["catalog"], response_model=Envelope[list[Agency]])
@limiter.limit(f"{settings.rate_limit_per_minute}/minute")
def list_agencies(request: Request) -> Envelope[list[Agency]]:
    """List ingested agencies. v0.1: TTC only."""
    rows = db.fetch_all(
        "SELECT agency_id, agency_name AS name, agency_timezone AS timezone "
        "FROM static_gtfs.agency"
    )
    # If static GTFS hasn't loaded yet, still return TTC as a known agency.
    if not rows:
        rows = [{"agency_id": "ttc", "name": "Toronto Transit Commission", "timezone": "America/Toronto"}]
    return Envelope(data=[Agency(**r) for r in rows], meta=_meta())


@app.get("/v1/freshness", tags=["ops"], response_model=Envelope[list[FeedFreshness]])
@limiter.limit(f"{settings.rate_limit_per_minute}/minute")
def freshness(request: Request) -> Envelope[list[FeedFreshness]]:
    """
    Latest freshness snapshot per feed. This is the endpoint the /freshness page polls.

    Returns one row per feed_key — the most recent snapshot we've taken. The freshness_check
    Dagster asset writes a new snapshot every minute.
    """
    rows = db.fetch_all(
        """
        SELECT DISTINCT ON (feed_key)
            feed_key, last_record_at, record_count_5min, record_count_1h, lag_seconds, status
        FROM ops.freshness_snapshot
        ORDER BY feed_key, snapshot_at DESC
        """
    )
    items = [FeedFreshness(**r) for r in rows]
    return Envelope(data=items, meta=_meta())


@app.get("/v1/vehicle-positions/latest", tags=["realtime"], response_model=Envelope[list[VehiclePosition]])
@limiter.limit(f"{settings.rate_limit_per_minute}/minute")
def vehicle_positions_latest(
    request: Request,
    route_id: str | None = None,
    limit: int = 500,
) -> Envelope[list[VehiclePosition]]:
    """
    Most recent position per vehicle, optionally filtered by route_id.
    Looks back at most 5 minutes (the 'recent' window) — older means the vehicle
    has likely gone out of service or its data dropped.
    """
    if limit < 1 or limit > 5000:
        raise HTTPException(status_code=400, detail="limit must be 1..5000")

    where = "received_at > NOW() - INTERVAL '5 minutes'"
    params: tuple = ()
    if route_id:
        where += " AND route_id = %s"
        params = (route_id,)

    sql = f"""
        SELECT DISTINCT ON (vehicle_id)
            vehicle_id, trip_id, route_id, direction_id,
            ST_Y(location::geometry) AS latitude,
            ST_X(location::geometry) AS longitude,
            bearing, speed_mps, received_at
        FROM realtime.vehicle_positions
        WHERE {where} AND vehicle_id IS NOT NULL
        ORDER BY vehicle_id, received_at DESC
        LIMIT {int(limit)}
    """
    rows = db.fetch_all(sql, params)
    return Envelope(data=[VehiclePosition(**r) for r in rows], meta=_meta())


@app.get("/v1/routes", tags=["catalog"], response_model=Envelope[list[Route]])
@limiter.limit(f"{settings.rate_limit_per_minute}/minute")
def list_routes(request: Request) -> Envelope[list[Route]]:
    """List TTC routes from the static GTFS snapshot."""
    rows = db.fetch_all(
        "SELECT route_id, route_short_name, route_long_name, route_type "
        "FROM static_gtfs.routes ORDER BY route_short_name"
    )
    return Envelope(data=[Route(**r) for r in rows], meta=_meta())


@app.get("/v1/routes/{route_id}/vehicles", tags=["realtime"], response_model=Envelope[list[VehiclePosition]])
@limiter.limit(f"{settings.rate_limit_per_minute}/minute")
def vehicles_on_route(request: Request, route_id: str) -> Envelope[list[VehiclePosition]]:
    """Convenience wrapper over /v1/vehicle-positions/latest with a route filter."""
    return vehicle_positions_latest(request=request, route_id=route_id, limit=500)


@app.get("/v1/stats/daily", tags=["ops"], response_model=Envelope[list[DailyStat]])
@limiter.limit(f"{settings.rate_limit_per_minute}/minute")
def daily_stats(request: Request) -> Envelope[list[DailyStat]]:
    """Per-feed record counts per day for the last 14 days."""
    rows = db.fetch_all(
        """
        SELECT to_char(received_at::date, 'YYYY-MM-DD') AS day,
               'ttc.vehicle_positions' AS feed_key,
               COUNT(*) AS record_count
        FROM realtime.vehicle_positions
        WHERE received_at >= NOW() - INTERVAL '14 days'
        GROUP BY received_at::date
        UNION ALL
        SELECT to_char(received_at::date, 'YYYY-MM-DD') AS day,
               'ttc.trip_updates' AS feed_key,
               COUNT(*) AS record_count
        FROM realtime.trip_updates
        WHERE received_at >= NOW() - INTERVAL '14 days'
        GROUP BY received_at::date
        ORDER BY day DESC, feed_key
        """
    )
    return Envelope(data=[DailyStat(**r) for r in rows], meta=_meta())


@app.get("/v1/ops/runs", tags=["ops"], response_model=Envelope[list[IngestionRun]])
@limiter.limit(f"{settings.rate_limit_per_minute}/minute")
def recent_runs(request: Request, limit: int = 50) -> Envelope[list[IngestionRun]]:
    """Most recent ingestion runs across all assets."""
    if limit < 1 or limit > 200:
        raise HTTPException(status_code=400, detail="limit must be 1..200")
    rows = db.fetch_all(
        """
        SELECT run_id, asset_key, started_at, completed_at, status, records_written
        FROM ops.ingestion_runs
        ORDER BY started_at DESC
        LIMIT %s
        """,
        (limit,),
    )
    return Envelope(data=[IngestionRun(**r) for r in rows], meta=_meta())
