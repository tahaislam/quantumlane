"""
Response models. Every endpoint returns the standard envelope (data + meta).

We define explicit Pydantic models rather than returning dicts so:
    - OpenAPI docs at /docs are accurate
    - Field renaming is centralized
    - Response shape changes break the build, not silently the website
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class Meta(BaseModel):
    fetched_at: datetime
    data_age_seconds: int | None = None
    next_cursor: str | None = None


class Envelope[T](BaseModel):
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


class Stop(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    stop_id: str
    stop_code: str | None
    stop_name: str | None
    latitude: float
    longitude: float
    distance_m: float | None = None


class InferredClassification(BaseModel):
    """
    LLM-inferred fields — NOT from the GTFS-RT feed.

    Produced by the alert classifier (see realtime.service_alert_classifications)
    because the TTC rarely populates the native cause and never populates
    severity_level. Kept in a nested object so inferred values can't be mistaken
    for native feed data.
    """

    # protected_namespaces=(): allow the field name "model" (pydantic reserves model_*).
    model_config = ConfigDict(from_attributes=True, protected_namespaces=())

    cause: str = Field(description="GTFS-RT Cause label inferred from the alert text.")
    cause_detail: str | None = Field(
        description="Short phrase from the alert naming the concrete cause, if any."
    )
    severity: str = Field(description="GTFS-RT SeverityLevel label inferred from the alert text.")
    model: str = Field(description="Model id that produced the inference.")
    prompt_version: str = Field(description="Classifier prompt version at inference time.")
    classified_at: datetime


class ServiceAlert(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    agency_id: str
    alert_id: str
    header_text: str | None
    description_text: str | None
    cause: int | None = Field(
        description="Native GTFS-RT Cause enum code; rarely populated by the TTC."
    )
    effect: int | None = Field(description="Native GTFS-RT Effect enum code.")
    severity_level: int | None = Field(
        description="Native GTFS-RT SeverityLevel enum code; never populated by the TTC."
    )
    affected_routes: list[str] | None
    affected_stops: list[str] | None
    active_period_start: datetime | None
    active_period_end: datetime | None
    first_seen_at: datetime
    last_seen_at: datetime
    inferred: InferredClassification | None = Field(
        default=None,
        description=(
            "LLM-inferred classification of the alert text; null when not yet classified. "
            "These values are inferred, not part of the GTFS-RT feed."
        ),
    )
