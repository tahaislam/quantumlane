"""
LLM classification of service alerts — cause and severity inferred from alert text.

Why this exists: the live TTC feed almost never populates the native `cause` (~3%) and
never populates `severity_level`, while `header_text` is always present. This module
infers ONLY those two fields (plus a short free-text `cause_detail`). It deliberately
does NOT infer `effect` (natively populated) or routes (informed_entity is reliable).

Design:
    - Pure-ish seam: no DB access in this module. The asset feeds alerts in and writes
      the returned classifications out; provenance stays in the asset/SQL layer.
    - The provider sits behind `ClassificationProvider` so it is swappable (tests use a
      fake; production uses `AnthropicProvider`).
    - One API call per alert. A malformed response or out-of-set label is a PER-ALERT
      failure: logged, skipped, and retried on a later run — never an exception out of
      `classify_alerts`.
    - Validation against the GTFS-RT label sets happens HERE, in Python, before any DB
      write. The CHECK constraints on realtime.service_alert_classifications are the
      backstop, not the validator.
    - `compute_input_hash` defines the classification cache key: an alert is re-classified
      only when its normalized text changes.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol

import anthropic
import structlog

from quantumlane_ingestion.settings import get_settings

log = structlog.get_logger(__name__)

# GTFS-RT Cause / SeverityLevel labels (gtfs_realtime_pb2.Alert). Tuples, not sets, so the
# prompt and the provider-side JSON schema list them in spec order.
CAUSE_LABELS: tuple[str, ...] = (
    "UNKNOWN_CAUSE",
    "OTHER_CAUSE",
    "TECHNICAL_PROBLEM",
    "STRIKE",
    "DEMONSTRATION",
    "ACCIDENT",
    "HOLIDAY",
    "WEATHER",
    "MAINTENANCE",
    "CONSTRUCTION",
    "POLICE_ACTIVITY",
    "MEDICAL_EMERGENCY",
)
SEVERITY_LABELS: tuple[str, ...] = (
    "UNKNOWN_SEVERITY",
    "INFO",
    "WARNING",
    "SEVERE",
)

# CONVENTION: any edit to PROMPT_TEMPLATE requires bumping PROMPT_VERSION. The version is
# stored on every classification row, so a prompt change is distinguishable from a model
# change when analyzing results.
PROMPT_VERSION = "v1"

PROMPT_TEMPLATE = """You classify Toronto TTC transit service alerts.

Alert text:
<alert>
{alert_text}
</alert>

Pick exactly one cause label: UNKNOWN_CAUSE, OTHER_CAUSE, TECHNICAL_PROBLEM, STRIKE, \
DEMONSTRATION, ACCIDENT, HOLIDAY, WEATHER, MAINTENANCE, CONSTRUCTION, POLICE_ACTIVITY, \
MEDICAL_EMERGENCY. Use UNKNOWN_CAUSE only when the text gives no hint at all.

Pick exactly one severity label:
- INFO: routine notices (elevator out of service, accessibility notes, planned-change announcements).
- WARNING: meaningful disruption (delays, detours, diversions, shuttle buses running).
- SEVERE: major disruption (line suspended, no service, station closed).
- UNKNOWN_SEVERITY: only if severity truly cannot be judged from the text.

Set cause_detail to a short phrase taken from the alert naming the concrete cause
(e.g. "sewer replacement", "watermain construction"), or null if the text names none.

Respond with ONLY a JSON object, no other text:
{{"cause": "<label>", "cause_detail": "<phrase or null>", "severity": "<label>"}}"""


class ClassificationError(Exception):
    """A single alert could not be classified (malformed response, out-of-set label, ...)."""


@dataclass(frozen=True)
class AlertForClassification:
    """Input to the seam: the identity and text of one alert from realtime.service_alerts."""

    agency_id: str
    alert_id: str
    header_text: str | None
    description_text: str | None

    @property
    def input_hash(self) -> str:
        return compute_input_hash(self.header_text, self.description_text)


@dataclass(frozen=True)
class AlertClassification:
    """Output of the seam: one validated row for realtime.service_alert_classifications."""

    agency_id: str
    alert_id: str
    input_hash: str
    cause_inferred: str
    cause_detail: str | None
    severity_inferred: str
    model: str
    prompt_version: str
    raw_response: dict[str, Any]


def normalize_alert_text(header_text: str | None, description_text: str | None) -> str:
    """
    Canonical text form the classification (and its cache key) is computed over.

    Whitespace runs collapse to a single space and each part is stripped, so upstream
    formatting jitter doesn't force a re-classification; a wording change does. Case is
    preserved — a case change is a real text change. Returns "" when there is no text.
    """
    parts = []
    for text in (header_text, description_text):
        if text:
            collapsed = re.sub(r"\s+", " ", text).strip()
            if collapsed:
                parts.append(collapsed)
    return "\n".join(parts)


def compute_input_hash(header_text: str | None, description_text: str | None) -> str:
    """sha256 hex over the normalized header+description. The classification cache key."""
    return hashlib.sha256(
        normalize_alert_text(header_text, description_text).encode("utf-8")
    ).hexdigest()


def select_unclassified(
    alerts: Sequence[AlertForClassification],
    existing: set[tuple[str, str, str]],
) -> list[AlertForClassification]:
    """
    Cache diff: alerts whose (agency_id, alert_id, input_hash) has no classification row.

    Everything returned here costs one API call; everything filtered out is a cache hit.
    Kept as a pure function so the zero-call path is unit-testable.
    """
    return [
        alert
        for alert in alerts
        if (alert.agency_id, alert.alert_id, alert.input_hash) not in existing
    ]


class ClassificationProvider(Protocol):
    """Thin provider interface so the concrete LLM backend is swappable."""

    @property
    def model(self) -> str:
        """Identifier of the model answering, stored on each classification row."""
        ...

    def complete(self, prompt: str) -> str:
        """Return the model's raw text response for a single prompt. May raise."""
        ...


# Provider-side JSON schema (structured outputs): guarantees well-formed JSON from the
# live API. parse_classification below remains the actual gate — it also covers swapped
# or misbehaving providers.
_RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "cause": {"type": "string", "enum": list(CAUSE_LABELS)},
        "cause_detail": {"anyOf": [{"type": "string"}, {"type": "null"}]},
        "severity": {"type": "string", "enum": list(SEVERITY_LABELS)},
    },
    "required": ["cause", "cause_detail", "severity"],
    "additionalProperties": False,
}


class AnthropicProvider:
    """
    Anthropic Messages API provider.

    The API key comes from ANTHROPIC_API_KEY in the environment (the SDK reads it
    directly; deliberately not a QL_-prefixed setting). The model id comes from
    QL_CLASSIFIER_MODEL via settings.
    """

    def __init__(self, model: str) -> None:
        self._client = anthropic.Anthropic()
        self._model = model

    @property
    def model(self) -> str:
        return self._model

    def complete(self, prompt: str) -> str:
        response = self._client.messages.create(
            model=self._model,
            max_tokens=300,
            output_config={"format": {"type": "json_schema", "schema": _RESPONSE_SCHEMA}},
            messages=[{"role": "user", "content": prompt}],
        )
        for block in response.content:
            if block.type == "text":
                return block.text
        raise ClassificationError("provider response contained no text block")


def build_prompt(alert: AlertForClassification) -> str:
    """Render the prompt for one alert. Uses the same normalization as the cache key."""
    return PROMPT_TEMPLATE.format(
        alert_text=normalize_alert_text(alert.header_text, alert.description_text)
    )


def parse_classification(raw_text: str) -> tuple[str, str | None, str, dict[str, Any]]:
    """
    Parse and validate one provider response BEFORE any DB write.

    Returns (cause, cause_detail, severity, parsed_payload).
    Raises ClassificationError on malformed JSON, out-of-set labels, or wrong types.
    """
    try:
        payload = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise ClassificationError(f"malformed JSON from provider: {exc}") from exc
    if not isinstance(payload, dict):
        raise ClassificationError(f"provider response is not a JSON object: {payload!r}")

    cause = payload.get("cause")
    cause_detail = payload.get("cause_detail")
    severity = payload.get("severity")

    if cause not in CAUSE_LABELS:
        raise ClassificationError(f"cause {cause!r} is not a GTFS-RT Cause label")
    if severity not in SEVERITY_LABELS:
        raise ClassificationError(f"severity {severity!r} is not a GTFS-RT SeverityLevel label")
    if cause_detail is not None and not isinstance(cause_detail, str):
        raise ClassificationError(f"cause_detail must be a string or null, got {cause_detail!r}")

    return cause, cause_detail, severity, payload


def classify_alerts(
    alerts: Sequence[AlertForClassification],
    provider: ClassificationProvider | None = None,
) -> list[AlertClassification]:
    """
    Classify alerts, one API call per alert. Returns only the successes.

    A failed alert (provider error, malformed JSON, out-of-set label) is logged and
    omitted — never raised — so it stays unclassified and is retried on the next run.
    Callers should diff len(alerts) vs len(result) for the failure count.
    """
    if not alerts:
        return []
    if provider is None:
        provider = AnthropicProvider(model=get_settings().classifier_model)

    results: list[AlertClassification] = []
    for alert in alerts:
        try:
            raw_text = provider.complete(build_prompt(alert))
            cause, cause_detail, severity, payload = parse_classification(raw_text)
        except Exception:  # per-alert isolation is the contract here — log, skip, retry next run
            log.exception(
                "alert_classification_failed",
                agency_id=alert.agency_id,
                alert_id=alert.alert_id,
            )
            continue
        results.append(
            AlertClassification(
                agency_id=alert.agency_id,
                alert_id=alert.alert_id,
                input_hash=alert.input_hash,
                cause_inferred=cause,
                cause_detail=cause_detail,
                severity_inferred=severity,
                model=provider.model,
                prompt_version=PROMPT_VERSION,
                raw_response=payload,
            )
        )
    return results
