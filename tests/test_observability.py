"""Tests for pipeline observability, freshness SLAs, and schema drift."""
from datetime import datetime, timezone, timedelta
from data_vault_runtime.observability import PipelineObservability, SuiteReport


def test_freshness_within_sla():
    now = datetime(2026, 11, 26, 12, 0, 0, tzinfo=timezone.utc)
    records = [
        {"id": "rec-1", "ingested_at": (now - timedelta(minutes=10)).isoformat()},
        {"id": "rec-2", "ingested_at": (now - timedelta(minutes=5)).isoformat()},
    ]
    res = PipelineObservability.check_freshness(records, "ingested_at", max_allowed_lag_seconds=600, as_of=now)
    assert res.is_fresh is True
    assert res.lag_seconds == 300.0


def test_freshness_sla_breach():
    now = datetime(2026, 11, 26, 12, 0, 0, tzinfo=timezone.utc)
    records = [
        {"id": "rec-1", "ingested_at": (now - timedelta(hours=2)).isoformat()},
    ]
    res = PipelineObservability.check_freshness(records, "ingested_at", max_allowed_lag_seconds=3600, as_of=now)
    assert res.is_fresh is False
    assert res.lag_seconds == 7200.0
    assert "breached" in res.message


def test_freshness_empty_dataset():
    res = PipelineObservability.check_freshness([], "ingested_at", max_allowed_lag_seconds=300)
    assert res.is_fresh is False
    assert res.latest_timestamp is None


def test_row_count_bounds_and_anomaly():
    data = [{"x": 1}, {"x": 2}, {"x": 3}]
    valid = PipelineObservability.validate_row_count(data, expected_min=1, expected_max=5)
    assert valid.is_valid is True
    assert valid.anomaly_detected is False

    underflow = PipelineObservability.validate_row_count(data, expected_min=10)
    assert underflow.is_valid is False
    assert underflow.anomaly_detected is True

    variance = PipelineObservability.validate_row_count(data, baseline_avg=100.0, max_variance_ratio=0.2)
    assert variance.is_valid is False
    assert "volume deviation" in variance.message


def test_schema_drift_detection():
    expected = {"user_id": str, "amount": (int, float), "active": bool}
    records = [
        {"user_id": "u1", "amount": 99.5, "active": True},
        {"user_id": "u2", "amount": 100, "active": False},
    ]
    clean = PipelineObservability.detect_schema_drift(records, expected)
    assert clean.has_drift is False

    # Drift: missing column and unexpected column
    drifted_records = [
        {"user_id": "u3", "unknown_tag": "val"},
    ]
    drift = PipelineObservability.detect_schema_drift(drifted_records, expected)
    assert drift.has_drift is True
    assert "active" in drift.missing_fields
    assert "unknown_tag" in drift.unexpected_fields


def test_schema_drift_type_mismatch():
    expected = {"user_id": str, "active": bool}
    records = [{"user_id": 12345, "active": "not_a_bool"}]
    drift = PipelineObservability.detect_schema_drift(records, expected)
    assert drift.has_drift is True
    assert "user_id" in drift.type_mismatches
    assert "active" in drift.type_mismatches


def test_full_observability_suite():
    now = datetime(2026, 11, 26, 12, 0, 0, tzinfo=timezone.utc)
    expected = {"event_id": str, "created_at": str}
    records = [
        {"event_id": "e1", "created_at": (now - timedelta(seconds=30)).isoformat()},
        {"event_id": "e2", "created_at": (now - timedelta(seconds=15)).isoformat()},
    ]
    report: SuiteReport = PipelineObservability.run_suite(
        records,
        timestamp_field="created_at",
        max_allowed_lag_seconds=60,
        expected_schema=expected,
        expected_min_rows=2,
        as_of=now,
    )
    assert report.passed is True
    d = report.to_dict()
    assert d["passed"] is True
    assert d["freshness"]["is_fresh"] is True
    assert d["row_count"]["observed_count"] == 2
