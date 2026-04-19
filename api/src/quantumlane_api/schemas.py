"""
Response models. Every endpoint returns the standard envelope (data + meta).

We define explicit Pydantic models rather than returning dicts so:
    - OpenAPI docs at /docs are accurate
    - Field renaming is centralized
    - Response shape changes break the build, not silently the website
"""
from __future__ import annotations

from datetime import datetime
from typing import Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field

T = TypeVar("T")


class Meta(BaseModel):
    fetched_at: datetime
    data_age_seconds: int | None = None
    next_cursor: str | None = None


class Envelope(BaseModel, Generic[T]):
    data: T
    meta: Meta


class Agency(BaseModel):
    agency_id: str
    name: str
    timezone: str


class FeedFreshness(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    feed_key: str
    last_record_at: datetime | None
    record_count_5min: int
    record_count_1h: int
    lag_seconds: int | None
    status: str = Field(description="One of: healthy, lagging, stale, down")


class VehiclePosition(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    vehicle_id: str | None
    trip_id: str | None
    route_id: str | None
    direction_id: int | None
    latitude: float
    longitude: float
    bearing: float | None
    speed_mps: float | None
    received_at: datetime


class Route(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    route_id: str
    route_short_name: str | None
    route_long_name: str | None
    route_type: int


class DailyStat(BaseModel):
    day: str
    feed_key: str
    record_count: int


class IngestionRun(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    run_id: str
    asset_key: str
    started_at: datetime
    completed_at: datetime | None
    status: str
    records_written: int | None
