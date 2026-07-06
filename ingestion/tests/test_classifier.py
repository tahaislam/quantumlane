"""
Unit tests for the service-alert classifier seam.

Everything runs against FakeProvider — no network, no DB. The load-bearing guarantees:
    - an out-of-set label or malformed JSON is a per-alert failure, never an exception
      (so the Dagster asset can't crash on a bad model response), and
    - the (agency_id, alert_id, input_hash) cache diff makes zero provider calls when
      nothing changed, and re-selects an alert whose text changed.
"""

from __future__ import annotations

import json

from quantumlane_ingestion.classifier import (
    CAUSE_LABELS,
    PROMPT_VERSION,
    SEVERITY_LABELS,
    AlertForClassification,
    classify_alerts,
    compute_input_hash,
    normalize_alert_text,
    select_unclassified,
)


class FakeProvider:
    """Scripted provider: returns canned responses in order and counts calls."""

    def __init__(self, responses: list[str]) -> None:
        self._responses = responses
        self.calls = 0

    @property
    def model(self) -> str:
        return "fake-model"

    def complete(self, prompt: str) -> str:
        response = self._responses[self.calls]
        self.calls += 1
        return response


class ExplodingProvider:
    """Provider whose API call always fails — simulates a provider outage."""

    calls = 0

    @property
    def model(self) -> str:
        return "exploding-model"

    def complete(self, prompt: str) -> str:
        raise ConnectionError("simulated provider outage")


def _alert(
    alert_id: str = "A1", header: str = "504 King detour via Queen"
) -> AlertForClassification:
    return AlertForClassification(
        agency_id="ttc", alert_id=alert_id, header_text=header, description_text=None
    )


GOOD_RESPONSE = json.dumps(
    {"cause": "CONSTRUCTION", "cause_detail": "watermain construction", "severity": "WARNING"}
)


# -----------------------------------------------------------------------------
# Happy path
# -----------------------------------------------------------------------------


def test_classify_alerts_happy_path() -> None:
    alert = _alert()
    provider = FakeProvider([GOOD_RESPONSE])

    results = classify_alerts([alert], provider=provider)

    assert provider.calls == 1
    assert len(results) == 1
    c = results[0]
    assert c.agency_id == "ttc"
    assert c.alert_id == "A1"
    assert c.input_hash == compute_input_hash("504 King detour via Queen", None)
    assert c.cause_inferred == "CONSTRUCTION"
    assert c.cause_detail == "watermain construction"
    assert c.severity_inferred == "WARNING"
    assert c.model == "fake-model"
    assert c.prompt_version == PROMPT_VERSION
    assert c.raw_response["cause"] == "CONSTRUCTION"


def test_null_cause_detail_is_accepted() -> None:
    response = json.dumps({"cause": "UNKNOWN_CAUSE", "cause_detail": None, "severity": "INFO"})
    results = classify_alerts([_alert()], provider=FakeProvider([response]))
    assert len(results) == 1
    assert results[0].cause_detail is None


# -----------------------------------------------------------------------------
# Per-alert failure isolation: bad responses are skipped, never raised
# -----------------------------------------------------------------------------


def test_out_of_set_cause_is_rejected_without_crashing() -> None:
    bad = json.dumps({"cause": "TRAFFIC_JAM", "cause_detail": None, "severity": "WARNING"})
    results = classify_alerts([_alert()], provider=FakeProvider([bad]))
    assert results == []


def test_out_of_set_severity_is_rejected_without_crashing() -> None:
    bad = json.dumps({"cause": "ACCIDENT", "cause_detail": None, "severity": "CATASTROPHIC"})
    results = classify_alerts([_alert()], provider=FakeProvider([bad]))
    assert results == []


def test_malformed_json_is_rejected_without_crashing() -> None:
    results = classify_alerts([_alert()], provider=FakeProvider(['{"cause": "ACCIDENT", oops']))
    assert results == []


def test_non_object_json_is_rejected_without_crashing() -> None:
    results = classify_alerts([_alert()], provider=FakeProvider(['["ACCIDENT"]']))
    assert results == []


def test_wrong_type_cause_detail_is_rejected_without_crashing() -> None:
    bad = json.dumps({"cause": "ACCIDENT", "cause_detail": 7, "severity": "WARNING"})
    results = classify_alerts([_alert()], provider=FakeProvider([bad]))
    assert results == []


def test_one_bad_alert_does_not_poison_the_batch() -> None:
    bad = json.dumps({"cause": "NOT_A_LABEL", "cause_detail": None, "severity": "INFO"})
    alerts = [_alert("A1"), _alert("A2", header="Line 1: no service Bloor to St George")]
    provider = FakeProvider([bad, GOOD_RESPONSE])

    results = classify_alerts(alerts, provider=provider)

    assert provider.calls == 2
    assert [c.alert_id for c in results] == ["A2"]


def test_provider_outage_yields_no_results_and_no_exception() -> None:
    results = classify_alerts([_alert("A1"), _alert("A2")], provider=ExplodingProvider())
    assert results == []


# -----------------------------------------------------------------------------
# Hashing / normalization — defines when a re-classification happens
# -----------------------------------------------------------------------------


def test_whitespace_jitter_does_not_change_the_hash() -> None:
    base = compute_input_hash("504 King detour", "via Queen St")
    assert compute_input_hash("  504  King   detour ", "via Queen St\n") == base


def test_text_change_changes_the_hash() -> None:
    assert compute_input_hash("504 King detour", None) != compute_input_hash(
        "504 King detour ended", None
    )


def test_none_description_hashes_like_absent_description() -> None:
    assert compute_input_hash("elevator out", None) == compute_input_hash("elevator out", "")


def test_normalize_joins_header_and_description() -> None:
    assert normalize_alert_text(" a  b ", "c") == "a b\nc"
    assert normalize_alert_text(None, None) == ""


# -----------------------------------------------------------------------------
# Cache diff — a cache hit must cost zero API calls
# -----------------------------------------------------------------------------


def test_cache_hit_selects_nothing_and_makes_zero_api_calls() -> None:
    alerts = [_alert("A1"), _alert("A2", header="Line 1: no service")]
    existing = {(a.agency_id, a.alert_id, a.input_hash) for a in alerts}
    provider = FakeProvider([])

    pending = select_unclassified(alerts, existing)
    results = classify_alerts(pending, provider=provider)

    assert pending == []
    assert results == []
    assert provider.calls == 0


def test_changed_text_is_selected_for_reclassification() -> None:
    original = _alert("A1", header="504 King detour")
    existing = {(original.agency_id, original.alert_id, original.input_hash)}
    edited = _alert("A1", header="504 King detour has ended")

    assert select_unclassified([original], existing) == []
    assert select_unclassified([edited], existing) == [edited]


def test_label_sets_match_gtfs_rt_spec() -> None:
    assert len(CAUSE_LABELS) == 12
    assert len(SEVERITY_LABELS) == 4
