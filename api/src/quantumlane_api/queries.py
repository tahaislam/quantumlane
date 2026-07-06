"""
Shared data-access layer: domain queries live here, once.

AGENTS.md invariant: the HTTP API and any future consumer (MCP layer, notebooks,
website) import these functions instead of rewriting the SQL, so the surfaces
can't drift apart. db.py stays plumbing-only (pool + fetch helpers); the queries
and their result shaping belong here.
"""

from __future__ import annotations

from typing import Any

from quantumlane_api import db

# Joins realtime.service_alerts with the LATEST classification per alert. The lateral
# join picks the newest row by classified_at because a text edit appends a new
# classification row (keyed by input_hash) rather than updating in place. Inferred
# values live ONLY in service_alert_classifications — never written back into
# service_alerts — so this read-time join is the single place native and inferred
# fields meet.
_ALERTS_WITH_CLASSIFICATIONS_SQL = """
    SELECT
        a.agency_id, a.alert_id,
        a.header_text, a.description_text,
        a.cause, a.effect, a.severity_level,
        a.affected_routes, a.affected_stops,
        a.active_period_start, a.active_period_end,
        a.first_seen_at, a.last_seen_at,
        c.cause_inferred, c.cause_detail, c.severity_inferred,
        c.model AS inferred_model,
        c.prompt_version AS inferred_prompt_version,
        c.classified_at
    FROM realtime.service_alerts a
    LEFT JOIN LATERAL (
        SELECT cause_inferred, cause_detail, severity_inferred,
               model, prompt_version, classified_at
        FROM realtime.service_alert_classifications c
        WHERE c.agency_id = a.agency_id AND c.alert_id = a.alert_id
        ORDER BY c.classified_at DESC
        LIMIT 1
    ) c ON TRUE
    ORDER BY a.last_seen_at DESC
    LIMIT %(limit)s
"""


def list_alerts_with_classifications(limit: int = 100) -> list[dict[str, Any]]:
    """
    Service alerts with their latest LLM classification (if any), newest-seen first.

    Returns one dict per alert: the native GTFS-RT fields at the top level, and the
    LLM-inferred fields nested under "inferred" (None when the alert has no
    classification yet) so callers can't mistake inferred values for feed data.
    """
    rows = db.fetch_all(_ALERTS_WITH_CLASSIFICATIONS_SQL, {"limit": limit})
    alerts: list[dict[str, Any]] = []
    for row in rows:
        inferred = None
        if row["cause_inferred"] is not None:
            inferred = {
                "cause": row["cause_inferred"],
                "cause_detail": row["cause_detail"],
                "severity": row["severity_inferred"],
                "model": row["inferred_model"],
                "prompt_version": row["inferred_prompt_version"],
                "classified_at": row["classified_at"],
            }
        alerts.append(
            {
                "agency_id": row["agency_id"],
                "alert_id": row["alert_id"],
                "header_text": row["header_text"],
                "description_text": row["description_text"],
                "cause": row["cause"],
                "effect": row["effect"],
                "severity_level": row["severity_level"],
                "affected_routes": row["affected_routes"],
                "affected_stops": row["affected_stops"],
                "active_period_start": row["active_period_start"],
                "active_period_end": row["active_period_end"],
                "first_seen_at": row["first_seen_at"],
                "last_seen_at": row["last_seen_at"],
                "inferred": inferred,
            }
        )
    return alerts
