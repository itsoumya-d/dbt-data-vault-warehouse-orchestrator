"""Pipeline observability: data freshness checks, row count validation, and schema drift detection."""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Sequence, Dict, Any, Optional, Set, Tuple, List, Type


def _parse_iso_timestamp(ts: Any) -> Optional[datetime]:
    if isinstance(ts, datetime):
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    if isinstance(ts, str):
        try:
            cleaned = ts.replace("Z", "+00:00")
            dt = datetime.fromisoformat(cleaned)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None
    return None


@dataclass(frozen=True)
class FreshnessResult:
    is_fresh: bool
    latest_timestamp: Optional[str]
    lag_seconds: float
    max_allowed_lag_seconds: float
    message: str


@dataclass(frozen=True)
class RowCountResult:
    is_valid: bool
    observed_count: int
    expected_min: Optional[int]
    expected_max: Optional[int]
    anomaly_detected: bool
    message: str


@dataclass(frozen=True)
class SchemaDriftResult:
    has_drift: bool
    missing_fields: List[str]
    unexpected_fields: List[str]
    type_mismatches: Dict[str, Tuple[str, str]]
    message: str


@dataclass(frozen=True)
class SuiteReport:
    passed: bool
    freshness: FreshnessResult
    row_count: RowCountResult
    schema_drift: SchemaDriftResult

    def to_dict(self) -> Dict[str, Any]:
        return {
            "passed": self.passed,
            "freshness": {
                "is_fresh": self.freshness.is_fresh,
                "latest_timestamp": self.freshness.latest_timestamp,
                "lag_seconds": self.freshness.lag_seconds,
                "max_allowed_lag_seconds": self.freshness.max_allowed_lag_seconds,
                "message": self.freshness.message,
            },
            "row_count": {
                "is_valid": self.row_count.is_valid,
                "observed_count": self.row_count.observed_count,
                "expected_min": self.row_count.expected_min,
                "expected_max": self.row_count.expected_max,
                "anomaly_detected": self.row_count.anomaly_detected,
                "message": self.row_count.message,
            },
            "schema_drift": {
                "has_drift": self.schema_drift.has_drift,
                "missing_fields": self.schema_drift.missing_fields,
                "unexpected_fields": self.schema_drift.unexpected_fields,
                "type_mismatches": {k: list(v) for k, v in self.schema_drift.type_mismatches.items()},
                "message": self.schema_drift.message,
            },
        }


class PipelineObservability:
    """Observability suite providing SLA freshness checks, row-count thresholds, and schema drift detection."""

    @staticmethod
    def check_freshness(
        records: Sequence[Dict[str, Any]],
        timestamp_field: str,
        max_allowed_lag_seconds: float,
        as_of: Optional[datetime] = None,
    ) -> FreshnessResult:
        if not records:
            return FreshnessResult(
                is_fresh=False,
                latest_timestamp=None,
                lag_seconds=float("inf"),
                max_allowed_lag_seconds=max_allowed_lag_seconds,
                message="Freshness check failed: record set is empty.",
            )

        now = as_of or datetime.now(timezone.utc)
        if not now.tzinfo:
            now = now.replace(tzinfo=timezone.utc)

        parsed_dates = []
        for r in records:
            dt = _parse_iso_timestamp(r.get(timestamp_field))
            if dt:
                parsed_dates.append(dt)

        if not parsed_dates:
            return FreshnessResult(
                is_fresh=False,
                latest_timestamp=None,
                lag_seconds=float("inf"),
                max_allowed_lag_seconds=max_allowed_lag_seconds,
                message=f"Freshness check failed: no valid timestamps in field '{timestamp_field}'.",
            )

        latest_dt = max(parsed_dates)
        lag = (now - latest_dt).total_seconds()
        is_fresh = lag <= max_allowed_lag_seconds

        msg = (
            f"Freshness OK: lag is {lag:.1f}s (max SLA {max_allowed_lag_seconds:.1f}s)."
            if is_fresh
            else f"Freshness SLA breached: lag is {lag:.1f}s exceeding threshold of {max_allowed_lag_seconds:.1f}s."
        )

        return FreshnessResult(
            is_fresh=is_fresh,
            latest_timestamp=latest_dt.isoformat(),
            lag_seconds=round(lag, 2),
            max_allowed_lag_seconds=max_allowed_lag_seconds,
            message=msg,
        )

    @staticmethod
    def validate_row_count(
        records: Sequence[Any],
        expected_min: Optional[int] = None,
        expected_max: Optional[int] = None,
        baseline_avg: Optional[float] = None,
        max_variance_ratio: float = 0.5,
    ) -> RowCountResult:
        count = len(records)
        anomaly = False
        reasons = []

        if expected_min is not None and count < expected_min:
            anomaly = True
            reasons.append(f"count {count} < expected min {expected_min}")

        if expected_max is not None and count > expected_max:
            anomaly = True
            reasons.append(f"count {count} > expected max {expected_max}")

        if baseline_avg is not None and baseline_avg > 0:
            variance = abs(count - baseline_avg) / baseline_avg
            if variance > max_variance_ratio:
                anomaly = True
                reasons.append(f"volume deviation {variance:.1%} exceeds tolerance {max_variance_ratio:.1%}")

        is_valid = not anomaly
        msg = f"Row count OK ({count} rows)." if is_valid else f"Row count anomaly: {'; '.join(reasons)}."

        return RowCountResult(
            is_valid=is_valid,
            observed_count=count,
            expected_min=expected_min,
            expected_max=expected_max,
            anomaly_detected=anomaly,
            message=msg,
        )

    @staticmethod
    def detect_schema_drift(
        records: Sequence[Dict[str, Any]],
        expected_schema: Dict[str, Type],
        allow_extra_fields: bool = False,
    ) -> SchemaDriftResult:
        if not records:
            return SchemaDriftResult(
                has_drift=False,
                missing_fields=[],
                unexpected_fields=[],
                type_mismatches={},
                message="Schema validation passed: empty dataset.",
            )

        observed_fields: Set[str] = set()
        for r in records:
            observed_fields.update(r.keys())

        expected_fields = set(expected_schema.keys())
        missing = sorted(list(expected_fields - observed_fields))
        unexpected = sorted(list(observed_fields - expected_fields)) if not allow_extra_fields else []

        type_mismatches: Dict[str, Tuple[str, str]] = {}
        for r in records:
            for field_name, expected_type in expected_schema.items():
                val = r.get(field_name)
                if val is not None and not isinstance(val, expected_type):
                    type_mismatches[field_name] = (expected_type.__name__, type(val).__name__)

        has_drift = bool(missing or unexpected or type_mismatches)
        details = []
        if missing:
            details.append(f"missing fields {missing}")
        if unexpected:
            details.append(f"unexpected fields {unexpected}")
        if type_mismatches:
            details.append(f"type mismatches {type_mismatches}")

        msg = "Schema contract satisfied." if not has_drift else f"Schema drift detected: {'; '.join(details)}."

        return SchemaDriftResult(
            has_drift=has_drift,
            missing_fields=missing,
            unexpected_fields=unexpected,
            type_mismatches=type_mismatches,
            message=msg,
        )

    @classmethod
    def run_suite(
        cls,
        records: Sequence[Dict[str, Any]],
        timestamp_field: str,
        max_allowed_lag_seconds: float,
        expected_schema: Dict[str, Type],
        expected_min_rows: Optional[int] = None,
        as_of: Optional[datetime] = None,
    ) -> SuiteReport:
        freshness = cls.check_freshness(records, timestamp_field, max_allowed_lag_seconds, as_of=as_of)
        row_count = cls.validate_row_count(records, expected_min=expected_min_rows)
        drift = cls.detect_schema_drift(records, expected_schema)

        passed = freshness.is_fresh and row_count.is_valid and not drift.has_drift
        return SuiteReport(
            passed=passed,
            freshness=freshness,
            row_count=row_count,
            schema_drift=drift,
        )
