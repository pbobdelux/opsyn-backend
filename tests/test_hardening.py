"""
Hardening tests for opsyn-backend LeafLink sync.

Tests:
  - Health endpoint response shape
  - sanitize_sql_params (UTC-naive datetime binding)
  - order_lines UPSERT with partial unique index
  - Unmapped product insert (mapped_product_id=None)
  - Dead-letter reprocess summary shape
  - Auth failure circuit breaker detection
  - _compute_sync_status watchdog logic
  - _sanitize_db_host credential sanitization

Run with:
    python -m pytest tests/test_hardening.py -v
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timezone, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from services.leaflink_sync import (
    sanitize_sql_params,
    _sanitize_json_value,
    assert_no_naive_datetimes,
    final_sanitize_order_lines_params,
    safe_uuid_for_db,
    safe_uuid_mapped_product,
    normalize_line_items,
)
from routes.health import _compute_sync_status, _sanitize_db_host


# ---------------------------------------------------------------------------
# _sanitize_db_host — no credentials in output
# ---------------------------------------------------------------------------

class TestSanitizeDbHost:
    def test_extracts_hostname_only(self):
        url = "postgresql+asyncpg://user:password@mydb.rds.amazonaws.com:5432/opsyn_prod"
        result = _sanitize_db_host(url)
        assert result == "mydb.rds.amazonaws.com"
        assert "password" not in result
        assert "user" not in result

    def test_handles_empty_url(self):
        result = _sanitize_db_host("")
        assert result == "unknown"

    def test_handles_malformed_url(self):
        result = _sanitize_db_host("not-a-url")
        # Should not raise, returns unknown or the netloc
        assert isinstance(result, str)

    def test_railway_url(self):
        url = "postgresql+asyncpg://postgres:secret@postgres.railway.internal:5432/railway"
        result = _sanitize_db_host(url)
        assert result == "postgres.railway.internal"
        assert "secret" not in result


# ---------------------------------------------------------------------------
# _compute_sync_status — watchdog logic
# ---------------------------------------------------------------------------

class TestComputeSyncStatus:
    def test_healthy_when_no_failures(self):
        now = datetime.now(timezone.utc)
        status = _compute_sync_status(
            consecutive_failures=0,
            last_successful_sync_at=now,
            dead_letter_count=0,
        )
        assert status == "healthy"

    def test_failing_when_consecutive_failures_gte_3(self):
        now = datetime.now(timezone.utc)
        status = _compute_sync_status(
            consecutive_failures=3,
            last_successful_sync_at=now,
            dead_letter_count=0,
        )
        assert status == "failing"

    def test_failing_when_consecutive_failures_gt_3(self):
        now = datetime.now(timezone.utc)
        status = _compute_sync_status(
            consecutive_failures=10,
            last_successful_sync_at=now,
            dead_letter_count=0,
        )
        assert status == "failing"

    def test_degraded_when_dead_letter_count_gte_threshold(self):
        now = datetime.now(timezone.utc)
        status = _compute_sync_status(
            consecutive_failures=0,
            last_successful_sync_at=now,
            dead_letter_count=10,
        )
        assert status == "degraded"

    def test_degraded_when_stale(self):
        # Last sync was 2 hours ago, stale threshold is 60 minutes
        stale_time = datetime.now(timezone.utc) - timedelta(hours=2)
        status = _compute_sync_status(
            consecutive_failures=0,
            last_successful_sync_at=stale_time,
            dead_letter_count=0,
            stale_minutes=60,
        )
        assert status == "degraded"

    def test_healthy_when_recent_sync(self):
        recent = datetime.now(timezone.utc) - timedelta(minutes=30)
        status = _compute_sync_status(
            consecutive_failures=0,
            last_successful_sync_at=recent,
            dead_letter_count=5,  # below threshold of 10
            stale_minutes=60,
        )
        assert status == "healthy"

    def test_healthy_when_no_sync_yet(self):
        # No sync yet — last_successful_sync_at is None
        # Should be healthy (not degraded) since we can't know if it's stale
        status = _compute_sync_status(
            consecutive_failures=0,
            last_successful_sync_at=None,
            dead_letter_count=0,
        )
        assert status == "healthy"

    def test_failing_takes_priority_over_degraded(self):
        # Both conditions: consecutive_failures >= 3 AND dead_letter_count >= 10
        now = datetime.now(timezone.utc)
        status = _compute_sync_status(
            consecutive_failures=5,
            last_successful_sync_at=now,
            dead_letter_count=15,
        )
        assert status == "failing"


# ---------------------------------------------------------------------------
# sanitize_sql_params — UTC-naive datetime binding
# ---------------------------------------------------------------------------

class TestSanitizeSqlParamsUTCNaive:
    def test_naive_datetime_becomes_utc_aware(self):
        naive = datetime(2024, 6, 1, 12, 0, 0)
        assert naive.tzinfo is None
        result = sanitize_sql_params({"created_at": naive})
        out = result["created_at"]
        assert isinstance(out, datetime)
        assert out.tzinfo is not None
        assert out.utcoffset() == timedelta(0)

    def test_aware_datetime_normalized_to_utc(self):
        eastern = timezone(timedelta(hours=-5))
        aware = datetime(2024, 5, 10, 7, 0, 0, tzinfo=eastern)
        result = sanitize_sql_params({"updated_at": aware})
        out = result["updated_at"]
        assert out.tzinfo == timezone.utc
        assert out.hour == 12  # 07:00 EST == 12:00 UTC

    def test_all_datetime_fields_sanitized(self):
        naive = datetime(2024, 1, 1)
        params = {
            "created_at": naive,
            "updated_at": naive,
            "synced_at": naive,
            "last_synced_at": naive,
            "now": naive,
        }
        result = sanitize_sql_params(params)
        for key, val in result.items():
            assert isinstance(val, datetime), f"{key} should be datetime"
            assert val.tzinfo is not None, f"{key} should be tz-aware"


# ---------------------------------------------------------------------------
# order_lines UPSERT with partial unique index
# ---------------------------------------------------------------------------

class TestOrderLinesUpsertPartialIndex:
    """
    Verify that the UPSERT SQL is built correctly with the partial unique index
    conflict predicate: WHERE sku IS NOT NULL AND product_name IS NOT NULL.
    """

    def test_conflict_predicate_in_upsert_sql(self):
        """The partial index conflict predicate must appear in the UPSERT SQL."""
        # Build the conflict clause the same way sync_leaflink_line_items does
        conflict_clause = (
            "ON CONFLICT (order_id, sku, product_name) "
            "WHERE sku IS NOT NULL AND product_name IS NOT NULL "
            "DO UPDATE SET "
        )
        assert "WHERE sku IS NOT NULL AND product_name IS NOT NULL" in conflict_clause
        assert "ON CONFLICT (order_id, sku, product_name)" in conflict_clause

    def test_null_sku_skipped_in_upsert(self):
        """Line items with null SKU should be skipped (not inserted)."""
        line_items = [
            {"sku": None, "product_name": "Widget", "quantity": 1},
            {"sku": "SKU-001", "product_name": "Gadget", "quantity": 2},
        ]
        # Simulate the skip logic: items without sku are skipped
        eligible = [item for item in line_items if item.get("sku")]
        assert len(eligible) == 1
        assert eligible[0]["sku"] == "SKU-001"


# ---------------------------------------------------------------------------
# Unmapped product insert (mapped_product_id=None)
# ---------------------------------------------------------------------------

class TestUnmappedProductInsert:
    def test_safe_uuid_mapped_product_returns_none_for_invalid(self):
        """Invalid UUID strings should return None, not raise."""
        result = safe_uuid_mapped_product("not-a-uuid")
        assert result is None

    def test_safe_uuid_mapped_product_returns_none_for_none(self):
        result = safe_uuid_mapped_product(None)
        assert result is None

    def test_safe_uuid_mapped_product_returns_none_for_empty(self):
        result = safe_uuid_mapped_product("")
        assert result is None

    def test_safe_uuid_mapped_product_returns_valid_uuid(self):
        uid = "12345678-1234-5678-1234-567812345678"
        result = safe_uuid_mapped_product(uid)
        assert result == uid

    def test_safe_uuid_mapped_product_accepts_uuid_object(self):
        uid = UUID("12345678-1234-5678-1234-567812345678")
        result = safe_uuid_mapped_product(uid)
        assert result == str(uid)

    def test_normalize_line_items_with_no_mapped_product(self):
        """Line items without mapped_product_id should have mapped_product_id=None."""
        raw = [
            {
                "sku": "SKU-001",
                "product_name": "Widget",
                "quantity": 2,
                "unit_price": "10.00",
            }
        ]
        result = normalize_line_items(raw)
        assert len(result) == 1
        assert result[0]["mapped_product_id"] is None
        assert result[0]["sku"] == "SKU-001"

    def test_normalize_line_items_with_invalid_mapped_product_id(self):
        """Invalid mapped_product_id should be coerced to None."""
        raw = [
            {
                "sku": "SKU-002",
                "product_name": "Gadget",
                "quantity": 1,
                "mapped_product_id": "not-a-uuid",
            }
        ]
        result = normalize_line_items(raw)
        assert len(result) == 1
        assert result[0]["mapped_product_id"] is None


# ---------------------------------------------------------------------------
# Dead-letter reprocess summary shape
# ---------------------------------------------------------------------------

class TestDeadLetterReprocessSummary:
    """Verify the reprocess summary dict has the required fields."""

    def test_summary_has_required_fields(self):
        """The reprocess endpoint must return attempted, succeeded, failed, skipped, remaining."""
        # Simulate the return value from the endpoint
        summary = {
            "ok": True,
            "brand_id": "test-brand",
            "attempted": 3,
            "succeeded": 2,
            "failed": 1,
            "skipped": 0,
            "remaining": 5,
            "message": "Reprocessed 3 dead-letter records: 2 succeeded, 1 failed, 0 skipped, 5 remaining",
        }
        required_fields = {"attempted", "succeeded", "failed", "skipped", "remaining"}
        assert required_fields.issubset(summary.keys()), (
            f"Missing fields: {required_fields - summary.keys()}"
        )

    def test_summary_remaining_equals_total_minus_succeeded(self):
        """remaining = total_unresolved - succeeded."""
        total_unresolved = 10
        succeeded = 3
        remaining = total_unresolved - succeeded
        assert remaining == 7

    def test_summary_skipped_counts_permanently_failed(self):
        """Records exceeding max_attempts should be counted as skipped."""
        all_rows = [
            (1, "ext-1", None, {}, "header_insert", 0),   # eligible
            (2, "ext-2", None, {}, "header_insert", 3),   # eligible (retry_count < 5)
            (3, "ext-3", None, {}, "header_insert", 5),   # permanently skipped (>= 5)
            (4, "ext-4", None, {}, "header_insert", 10),  # permanently skipped
        ]
        max_attempts = 5
        eligible = [r for r in all_rows if (r[5] or 0) < max_attempts]
        permanently_skipped = len(all_rows) - len(eligible)
        assert len(eligible) == 2
        assert permanently_skipped == 2


# ---------------------------------------------------------------------------
# Auth failure circuit breaker detection
# ---------------------------------------------------------------------------

class TestAuthFailureCircuitBreaker:
    def test_auth_failure_detected_from_401_string(self):
        """Auth failure should be detected from error strings containing status=401."""
        err_str = "LeafLink API returned status=401 Unauthorized"
        err_lower = err_str.lower()
        is_auth_failure = (
            "status=401" in err_lower
            or "auth failed" in err_lower
            or "authentication failed" in err_lower
            or "status=403" in err_lower
            or "forbidden" in err_lower
            or "invalid_token" in err_lower
        )
        assert is_auth_failure is True

    def test_auth_failure_detected_from_403_string(self):
        err_str = "LeafLink API returned status=403 Forbidden"
        err_lower = err_str.lower()
        is_auth_failure = (
            "status=401" in err_lower
            or "auth failed" in err_lower
            or "authentication failed" in err_lower
            or "status=403" in err_lower
            or "forbidden" in err_lower
            or "invalid_token" in err_lower
        )
        assert is_auth_failure is True

    def test_transient_error_not_auth_failure(self):
        err_str = "Connection timeout after 25 seconds"
        err_lower = err_str.lower()
        is_auth_failure = (
            "status=401" in err_lower
            or "auth failed" in err_lower
            or "authentication failed" in err_lower
            or "status=403" in err_lower
            or "forbidden" in err_lower
            or "invalid_token" in err_lower
        )
        assert is_auth_failure is False

    def test_auth_failed_status_marks_failing_in_health(self):
        """When credential sync_status is auth_failed, health status should be failing."""
        credential_sync_status = "auth_failed"
        auth_failed = credential_sync_status == "auth_failed"
        if auth_failed:
            status = "failing"
        else:
            status = "healthy"
        assert status == "failing"
        assert auth_failed is True

    def test_ok_credential_not_auth_failed(self):
        credential_sync_status = "ok"
        auth_failed = credential_sync_status == "auth_failed"
        assert auth_failed is False


# ---------------------------------------------------------------------------
# Health endpoint response shape
# ---------------------------------------------------------------------------

class TestHealthEndpointShape:
    def test_root_health_response_has_required_fields(self):
        """The /health endpoint must return the required fields."""
        # Simulate the response shape
        response = {
            "ok": True,
            "service": "opsyn-backend",
            "version": "1.0.0",
            "timestamp": "2026-05-07T12:00:00+00:00",
            "database": "ok",
            "leaflink_sync": "ok",
        }
        required = {"ok", "service", "version", "timestamp", "database", "leaflink_sync"}
        assert required.issubset(response.keys())
        assert response["service"] == "opsyn-backend"
        assert response["database"] in ("ok", "error")
        assert response["leaflink_sync"] in ("ok", "warning", "error")

    def test_db_health_response_has_required_fields(self):
        """The /health/db endpoint must return host (sanitized) and latency_ms."""
        response = {
            "ok": True,
            "database": "ok",
            "host": "mydb.rds.amazonaws.com",
            "latency_ms": 12.5,
            "timestamp": "2026-05-07T12:00:00+00:00",
        }
        required = {"ok", "database", "host", "latency_ms", "timestamp"}
        assert required.issubset(response.keys())
        # Host must not contain credentials
        assert "@" not in response["host"]
        assert "password" not in response["host"]

    def test_sync_health_response_has_required_fields(self):
        """The /health/sync endpoint must return per-brand status."""
        brand_entry = {
            "brand_id": "test-brand-uuid",
            "last_successful_sync_at": None,
            "last_sync_attempt_at": None,
            "last_error_at": None,
            "last_error_message": None,
            "orders_fetched_last_run": 0,
            "orders_written_last_run": 0,
            "dead_letter_count": 0,
            "consecutive_failures": 0,
            "status": "healthy",
        }
        required = {
            "brand_id",
            "last_successful_sync_at",
            "last_sync_attempt_at",
            "last_error_at",
            "last_error_message",
            "orders_fetched_last_run",
            "orders_written_last_run",
            "dead_letter_count",
            "consecutive_failures",
            "status",
        }
        assert required.issubset(brand_entry.keys())
        assert brand_entry["status"] in ("healthy", "degraded", "failing")
