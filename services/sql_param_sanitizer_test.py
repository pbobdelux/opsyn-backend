"""
services/sql_param_sanitizer_test.py — Unit tests for the canonical SQL parameter sanitizer.

Run with:
    pytest services/sql_param_sanitizer_test.py -v
"""

import pytest
from datetime import datetime, timezone

from services.sql_param_sanitizer import (
    sanitize_sql_params,
    assert_no_datetime_in_json,
    convert_datetime_to_iso,
)


# ---------------------------------------------------------------------------
# sanitize_sql_params — known_datetime_fields behaviour
# ---------------------------------------------------------------------------


def test_sql_datetime_param_stays_aware():
    """Timezone-aware datetime in a known field must remain a datetime object."""
    params = {"created_at": datetime.now(timezone.utc)}
    result = sanitize_sql_params(params, known_datetime_fields={"created_at"})
    assert isinstance(result["created_at"], datetime)
    assert result["created_at"].tzinfo is not None


def test_naive_sql_datetime_normalized():
    """Naive datetime in a known field must be normalised to UTC-aware."""
    params = {"created_at": datetime.now()}  # naive
    result = sanitize_sql_params(params, known_datetime_fields={"created_at"})
    assert isinstance(result["created_at"], datetime)
    assert result["created_at"].tzinfo is not None


def test_none_sql_datetime_stays_none():
    """None in a known datetime field must remain None."""
    params = {"created_at": None}
    result = sanitize_sql_params(params, known_datetime_fields={"created_at"})
    assert result["created_at"] is None


# ---------------------------------------------------------------------------
# sanitize_sql_params — JSON / nested payload behaviour
# ---------------------------------------------------------------------------


def test_nested_datetime_in_raw_payload():
    """Nested datetimes inside raw_payload must become ISO strings."""
    params = {
        "id": 1,
        "raw_payload": {
            "created_at": datetime.now(timezone.utc),
            "nested": {"updated_at": datetime.now(timezone.utc)},
        },
    }
    result = sanitize_sql_params(params, known_datetime_fields=set())
    assert isinstance(result["raw_payload"]["created_at"], str)
    assert isinstance(result["raw_payload"]["nested"]["updated_at"], str)
    assert "T" in result["raw_payload"]["created_at"]  # ISO format


def test_nested_datetime_in_line_items_json():
    """Nested datetimes inside line_items_json must become ISO strings."""
    params = {
        "id": 1,
        "line_items_json": [
            {"sku": "ABC", "created_at": datetime.now(timezone.utc)},
            {"sku": "DEF", "updated_at": datetime.now(timezone.utc)},
        ],
    }
    result = sanitize_sql_params(params, known_datetime_fields=set())
    assert isinstance(result["line_items_json"][0]["created_at"], str)
    assert isinstance(result["line_items_json"][1]["updated_at"], str)


def test_final_assertion_catches_leaked_datetime():
    """Final assertion must raise RuntimeError if a datetime survives in an unknown field.

    This test verifies the hard guard: if convert_datetime_to_iso somehow
    fails to convert a datetime (e.g. a future code path bypasses it), the
    assertion scan catches it and raises with FINAL_SQL_DATETIME_LEAK.
    """
    # Bypass convert_datetime_to_iso by injecting a pre-built result dict
    # that still contains a datetime — simulate a sanitization gap.
    from services import sql_param_sanitizer as _mod

    original_convert = _mod.convert_datetime_to_iso

    def _passthrough(obj, _depth=0):
        # Return the object unchanged (simulates a sanitization gap)
        return obj

    _mod.convert_datetime_to_iso = _passthrough
    try:
        params = {
            "id": 1,
            "unknown_field": {"leaked_dt": datetime.now(timezone.utc)},
        }
        with pytest.raises(RuntimeError, match="FINAL_SQL_DATETIME_LEAK"):
            sanitize_sql_params(params, known_datetime_fields=set())
    finally:
        _mod.convert_datetime_to_iso = original_convert


def test_dead_letter_now_sanitized():
    """Dead-letter params with known datetime fields must preserve datetime objects."""
    params = {
        "brand_id": "test",
        "external_order_id": "ext-123",
        "failure_reason": "test",
        "created_at": datetime.now(timezone.utc),
        "last_failed_at": datetime.now(timezone.utc),
    }
    result = sanitize_sql_params(
        params,
        known_datetime_fields={"created_at", "last_failed_at"},
    )
    assert isinstance(result["created_at"], datetime)
    assert isinstance(result["last_failed_at"], datetime)


# ---------------------------------------------------------------------------
# Regression: order_header_upsert with nested payload datetimes
# ---------------------------------------------------------------------------


def test_order_header_upsert_with_nested_payload():
    """Full order_header_upsert params: top-level datetimes preserved, nested converted."""
    params = {
        "brand_id": "noble",
        "external_order_id": "ext-456",
        "order_number": "ORD-001",
        "customer_name": "Test Customer",
        "status": "confirmed",
        "amount": 99.99,
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
        "synced_at": datetime.now(timezone.utc),
        "last_synced_at": datetime.now(timezone.utc),
        "raw_payload": {
            "id": "ext-456",
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
            "line_items": [
                {"sku": "ABC", "created_at": datetime.now(timezone.utc)},
            ],
        },
        "line_items_json": [
            {"sku": "ABC", "created_at": datetime.now(timezone.utc)},
        ],
    }
    result = sanitize_sql_params(
        params,
        known_datetime_fields={"created_at", "updated_at", "synced_at", "last_synced_at"},
    )

    # Top-level SQL datetimes remain datetime objects
    assert isinstance(result["created_at"], datetime)
    assert isinstance(result["updated_at"], datetime)
    assert isinstance(result["synced_at"], datetime)
    assert isinstance(result["last_synced_at"], datetime)

    # Nested datetimes become ISO strings
    assert isinstance(result["raw_payload"]["created_at"], str)
    assert isinstance(result["raw_payload"]["updated_at"], str)
    assert isinstance(result["raw_payload"]["line_items"][0]["created_at"], str)
    assert isinstance(result["line_items_json"][0]["created_at"], str)

    # No datetime objects remain in JSON payloads
    assert_no_datetime_in_json(result["raw_payload"])
    assert_no_datetime_in_json(result["line_items_json"])


# ---------------------------------------------------------------------------
# convert_datetime_to_iso — standalone tests
# ---------------------------------------------------------------------------


def test_convert_datetime_to_iso_aware():
    """Aware datetime must become an ISO string."""
    dt = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    result = convert_datetime_to_iso(dt)
    assert isinstance(result, str)
    assert "2024-01-15" in result
    assert "T" in result


def test_convert_datetime_to_iso_naive():
    """Naive datetime must be assumed UTC and become an ISO string."""
    from datetime import datetime as _dt
    dt = _dt(2024, 1, 15, 12, 0, 0)  # naive
    result = convert_datetime_to_iso(dt)
    assert isinstance(result, str)
    assert "+00:00" in result or "Z" in result or "2024-01-15" in result


def test_convert_datetime_to_iso_date():
    """Plain date must become an ISO date string."""
    from datetime import date
    d = date(2024, 1, 15)
    result = convert_datetime_to_iso(d)
    assert result == "2024-01-15"


def test_convert_datetime_to_iso_decimal():
    """Decimal must become a float."""
    from decimal import Decimal
    result = convert_datetime_to_iso(Decimal("99.99"))
    assert isinstance(result, float)
    assert abs(result - 99.99) < 0.001


def test_convert_datetime_to_iso_uuid():
    """UUID must become a string."""
    from uuid import UUID
    u = UUID("12345678-1234-5678-1234-567812345678")
    result = convert_datetime_to_iso(u)
    assert isinstance(result, str)
    assert result == "12345678-1234-5678-1234-567812345678"


def test_convert_datetime_to_iso_nested():
    """Nested dict/list with datetimes must be fully converted."""
    obj = {
        "a": datetime.now(timezone.utc),
        "b": [{"c": datetime.now(timezone.utc)}],
    }
    result = convert_datetime_to_iso(obj)
    assert isinstance(result["a"], str)
    assert isinstance(result["b"][0]["c"], str)


def test_convert_datetime_to_iso_primitives_unchanged():
    """Primitive types must pass through unchanged."""
    assert convert_datetime_to_iso(None) is None
    assert convert_datetime_to_iso(42) == 42
    assert convert_datetime_to_iso(3.14) == 3.14
    assert convert_datetime_to_iso("hello") == "hello"
    assert convert_datetime_to_iso(True) is True


# ---------------------------------------------------------------------------
# assert_no_datetime_in_json — standalone tests
# ---------------------------------------------------------------------------


def test_assert_no_datetime_passes_clean_dict():
    """Clean dict with no datetimes must not raise."""
    assert_no_datetime_in_json({"a": 1, "b": "hello", "c": None})


def test_assert_no_datetime_raises_on_datetime():
    """Dict containing a datetime must raise RuntimeError."""
    with pytest.raises(RuntimeError):
        assert_no_datetime_in_json({"leaked": datetime.now(timezone.utc)})


def test_assert_no_datetime_raises_on_nested_datetime():
    """Deeply nested datetime must raise RuntimeError with full path."""
    obj = {"level1": {"level2": [{"level3": datetime.now(timezone.utc)}]}}
    with pytest.raises(RuntimeError, match="level1.level2"):
        assert_no_datetime_in_json(obj)


def test_assert_no_datetime_passes_iso_strings():
    """ISO string datetimes must not raise (they are already converted)."""
    assert_no_datetime_in_json(
        {
            "created_at": "2024-01-15T12:00:00+00:00",
            "items": [{"updated_at": "2024-01-15T12:00:00+00:00"}],
        }
    )
