"""Unit tests for sanitize_sql_params(), _sanitize_json_value(),
assert_no_naive_datetimes(), and final_sanitize_order_lines_params().

Run with:
    python -m pytest tests/test_sanitize_sql_params.py -v

These tests are pure-Python and have no database or network dependencies.
"""
import sys
import os

# Allow running from the repo root without installing the package
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import date, datetime, timezone, timedelta
from decimal import Decimal
from uuid import UUID

import pytest

from services.leaflink_sync import (
    sanitize_sql_params,
    _sanitize_json_value,
    assert_no_naive_datetimes,
    final_sanitize_order_lines_params,
    _sanitize_datetime_params,
)


# ---------------------------------------------------------------------------
# _sanitize_json_value
# ---------------------------------------------------------------------------

class TestSanitizeJsonValue:
    def test_naive_datetime_to_iso_string(self):
        dt = datetime(2024, 1, 15, 10, 30, 0)
        result = _sanitize_json_value(dt)
        assert result == "2024-01-15T10:30:00"

    def test_aware_datetime_to_iso_string(self):
        dt = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        result = _sanitize_json_value(dt)
        assert result == "2024-01-15T10:30:00+00:00"

    def test_date_to_iso_string(self):
        d = date(2024, 6, 1)
        result = _sanitize_json_value(d)
        assert result == "2024-06-01"

    def test_uuid_to_string(self):
        uid = UUID("12345678-1234-5678-1234-567812345678")
        result = _sanitize_json_value(uid)
        assert result == "12345678-1234-5678-1234-567812345678"
        assert isinstance(result, str)

    def test_decimal_to_string(self):
        d = Decimal("123.456")
        result = _sanitize_json_value(d)
        assert result == "123.456"
        assert isinstance(result, str)

    def test_nested_dict_with_datetime(self):
        dt = datetime(2024, 3, 1, tzinfo=timezone.utc)
        payload = {"created_at": dt, "name": "test", "count": 5}
        result = _sanitize_json_value(payload)
        assert result["created_at"] == "2024-03-01T00:00:00+00:00"
        assert result["name"] == "test"
        assert result["count"] == 5

    def test_nested_list_with_uuid(self):
        uid = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        payload = [uid, "hello", 42]
        result = _sanitize_json_value(payload)
        assert result[0] == "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        assert result[1] == "hello"
        assert result[2] == 42

    def test_deeply_nested(self):
        dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
        payload = {"order": {"line_items": [{"created_at": dt, "qty": 3}]}}
        result = _sanitize_json_value(payload)
        assert result["order"]["line_items"][0]["created_at"] == "2024-01-01T00:00:00+00:00"
        assert result["order"]["line_items"][0]["qty"] == 3

    def test_passthrough_primitives(self):
        assert _sanitize_json_value(None) is None
        assert _sanitize_json_value(42) == 42
        assert _sanitize_json_value(3.14) == 3.14
        assert _sanitize_json_value("hello") == "hello"
        assert _sanitize_json_value(True) is True


# ---------------------------------------------------------------------------
# sanitize_sql_params — datetime handling
# ---------------------------------------------------------------------------

class TestSanitizeSqlParamsDatetime:
    def test_naive_datetime_becomes_utc_aware(self):
        naive = datetime(2024, 5, 10, 12, 0, 0)
        assert naive.tzinfo is None

        result = sanitize_sql_params({"created_at": naive})
        out = result["created_at"]

        assert isinstance(out, datetime)
        assert out.tzinfo is not None
        assert out.utcoffset() == timedelta(0)
        assert out.year == 2024
        assert out.month == 5
        assert out.day == 10

    def test_aware_datetime_normalised_to_utc(self):
        eastern = timezone(timedelta(hours=-5))
        aware = datetime(2024, 5, 10, 7, 0, 0, tzinfo=eastern)

        result = sanitize_sql_params({"updated_at": aware})
        out = result["updated_at"]

        assert isinstance(out, datetime)
        assert out.tzinfo == timezone.utc
        # 07:00 EST == 12:00 UTC
        assert out.hour == 12

    def test_utc_aware_datetime_unchanged(self):
        utc_dt = datetime(2024, 5, 10, 12, 0, 0, tzinfo=timezone.utc)
        result = sanitize_sql_params({"synced_at": utc_dt})
        assert result["synced_at"] == utc_dt

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
# sanitize_sql_params — date handling
# ---------------------------------------------------------------------------

class TestSanitizeSqlParamsDate:
    def test_plain_date_becomes_midnight_utc_datetime(self):
        d = date(2024, 6, 15)
        result = sanitize_sql_params({"ship_date": d})
        out = result["ship_date"]

        assert isinstance(out, datetime)
        assert out.tzinfo == timezone.utc
        assert out.year == 2024
        assert out.month == 6
        assert out.day == 15
        assert out.hour == 0
        assert out.minute == 0

    def test_date_in_payload_field_becomes_iso_string(self):
        d = date(2024, 6, 15)
        result = sanitize_sql_params({"raw_payload": {"date_field": d}})
        # raw_payload is a dict — it's a JSON field, so _sanitize_json_value is applied
        out = result["raw_payload"]
        assert isinstance(out, dict)
        assert out["date_field"] == "2024-06-15"


# ---------------------------------------------------------------------------
# sanitize_sql_params — UUID handling
# ---------------------------------------------------------------------------

class TestSanitizeSqlParamsUUID:
    def test_uuid_kept_as_uuid_object_for_sql_params(self):
        uid = UUID("12345678-1234-5678-1234-567812345678")
        result = sanitize_sql_params({"brand_id": uid})
        assert result["brand_id"] == uid
        assert isinstance(result["brand_id"], UUID)

    def test_uuid_in_raw_payload_becomes_string(self):
        uid = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        result = sanitize_sql_params({"raw_payload": {"org_id": uid}})
        out = result["raw_payload"]
        assert isinstance(out, dict)
        assert out["org_id"] == "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        assert isinstance(out["org_id"], str)


# ---------------------------------------------------------------------------
# sanitize_sql_params — raw_payload / JSON field handling
# ---------------------------------------------------------------------------

class TestSanitizeSqlParamsJsonPayload:
    def test_raw_payload_with_nested_datetime_sanitized(self):
        dt = datetime(2024, 3, 1, tzinfo=timezone.utc)
        payload = {"created_on": dt, "order_number": "ORD-001"}
        result = sanitize_sql_params({"raw_payload": payload})
        out = result["raw_payload"]
        assert isinstance(out, dict)
        assert out["created_on"] == "2024-03-01T00:00:00+00:00"
        assert out["order_number"] == "ORD-001"

    def test_raw_payload_with_nested_uuid_sanitized(self):
        uid = UUID("12345678-1234-5678-1234-567812345678")
        payload = {"brand_id": uid, "name": "test"}
        result = sanitize_sql_params({"raw_payload": payload})
        out = result["raw_payload"]
        assert out["brand_id"] == "12345678-1234-5678-1234-567812345678"
        assert isinstance(out["brand_id"], str)

    def test_raw_payload_with_nested_decimal_sanitized(self):
        payload = {"unit_price": Decimal("12.50"), "qty": 3}
        result = sanitize_sql_params({"raw_payload": payload})
        out = result["raw_payload"]
        assert out["unit_price"] == "12.50"
        assert out["qty"] == 3

    def test_raw_payload_string_passed_through(self):
        """Pre-serialized JSON strings should pass through unchanged."""
        json_str = '{"order_number": "ORD-001", "amount": "12.50"}'
        result = sanitize_sql_params({"raw_payload": json_str})
        assert result["raw_payload"] == json_str


# ---------------------------------------------------------------------------
# sanitize_sql_params — passthrough types
# ---------------------------------------------------------------------------

class TestSanitizeSqlParamsPassthrough:
    def test_string_passthrough(self):
        result = sanitize_sql_params({"sku": "ABC-123", "status": "submitted"})
        assert result["sku"] == "ABC-123"
        assert result["status"] == "submitted"

    def test_int_passthrough(self):
        result = sanitize_sql_params({"quantity": 5, "total_cents": 1000})
        assert result["quantity"] == 5
        assert result["total_cents"] == 1000

    def test_none_passthrough(self):
        result = sanitize_sql_params({"mapped_product_id": None, "org_id": None})
        assert result["mapped_product_id"] is None
        assert result["org_id"] is None

    def test_bool_passthrough(self):
        result = sanitize_sql_params({"active": True, "deleted": False})
        assert result["active"] is True
        assert result["deleted"] is False

    def test_decimal_passthrough_for_numeric_columns(self):
        """Decimal values for numeric SQL columns should pass through unchanged."""
        d = Decimal("99.99")
        result = sanitize_sql_params({"unit_price": d, "total_price": d})
        assert result["unit_price"] == d
        assert isinstance(result["unit_price"], Decimal)

    def test_all_keys_preserved(self):
        """sanitize_sql_params must never drop keys."""
        params = {
            "order_id": 42,
            "sku": "TEST",
            "product_name": "Widget",
            "quantity": 3,
            "unit_price": Decimal("10.00"),
            "total_price": Decimal("30.00"),
            "created_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
            "updated_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
            "now": datetime(2024, 1, 1, tzinfo=timezone.utc),
            "brand_id": "some-brand",
            "raw_payload": None,
        }
        result = sanitize_sql_params(params)
        assert set(result.keys()) == set(params.keys()), (
            f"Keys dropped: {set(params.keys()) - set(result.keys())}"
        )

    def test_statement_name_does_not_affect_output(self):
        """The statement= argument is for logging only; output must be identical."""
        params = {"created_at": datetime(2024, 1, 1), "sku": "X"}
        r1 = sanitize_sql_params(params, statement="test_a")
        r2 = sanitize_sql_params(params, statement="test_b")
        assert r1["created_at"] == r2["created_at"]
        assert r1["sku"] == r2["sku"]


# ---------------------------------------------------------------------------
# Integration-style: full order_lines params dict
# ---------------------------------------------------------------------------

class TestOrderLinesParamsIntegration:
    def test_order_lines_params_all_datetimes_aware(self):
        """Simulate the exact params dict built before an order_lines INSERT."""
        naive_now = datetime(2024, 6, 1, 12, 0, 0)  # naive — the bug scenario
        params = {
            "order_id": 123,
            "sku": "SKU-001",
            "product_name": "Test Product",
            "quantity": 2,
            "unit_price": Decimal("15.00"),
            "total_price": Decimal("30.00"),
            "created_at": naive_now,
            "updated_at": naive_now,
            "raw_payload": '{"sku": "SKU-001"}',
            "mapped_product_id": None,
            "mapping_status": "mapped",
            "mapping_issue": None,
        }

        result = sanitize_sql_params(params, statement="order_lines_insert")

        # All keys preserved
        assert set(result.keys()) == set(params.keys())

        # Datetimes are now UTC-aware
        assert result["created_at"].tzinfo is not None
        assert result["created_at"].tzinfo.utcoffset(result["created_at"]) is not None
        assert result["updated_at"].tzinfo is not None
        assert result["updated_at"].tzinfo.utcoffset(result["updated_at"]) is not None

        # Fail-fast assertions that mirror the production code
        ca = result["created_at"]
        ua = result["updated_at"]
        assert isinstance(ca, datetime) and ca.tzinfo is not None and ca.tzinfo.utcoffset(ca) is not None
        assert isinstance(ua, datetime) and ua.tzinfo is not None and ua.tzinfo.utcoffset(ua) is not None


# ---------------------------------------------------------------------------
# assert_no_naive_datetimes
# ---------------------------------------------------------------------------

class TestAssertNoNaiveDatetimes:
    def test_passes_for_aware_datetime(self):
        dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
        # Should not raise
        assert_no_naive_datetimes({"created_at": dt}, "params")

    def test_raises_for_naive_datetime_top_level(self):
        naive = datetime(2024, 1, 1)
        with pytest.raises(AssertionError, match="NAIVE_DATETIME_FOUND"):
            assert_no_naive_datetimes({"created_at": naive}, "params")

    def test_raises_for_naive_datetime_nested_in_dict(self):
        naive = datetime(2024, 5, 7, 16, 3, 51)
        payload = {"line_items": [{"created_at": naive}]}
        with pytest.raises(AssertionError, match="NAIVE_DATETIME_FOUND"):
            assert_no_naive_datetimes({"raw_payload": payload}, "params")

    def test_raises_for_naive_datetime_in_list(self):
        naive = datetime(2024, 3, 1)
        with pytest.raises(AssertionError, match="NAIVE_DATETIME_FOUND"):
            assert_no_naive_datetimes([naive], "params")

    def test_passes_for_non_datetime_values(self):
        # Should not raise for strings, ints, None, Decimal, UUID
        assert_no_naive_datetimes(
            {
                "sku": "ABC",
                "quantity": 3,
                "unit_price": Decimal("9.99"),
                "mapped_product_id": None,
                "brand_id": str(UUID("12345678-1234-5678-1234-567812345678")),
            },
            "params",
        )

    def test_passes_for_empty_dict(self):
        assert_no_naive_datetimes({}, "params")

    def test_passes_for_empty_list(self):
        assert_no_naive_datetimes([], "params")

    def test_path_included_in_error_message(self):
        naive = datetime(2024, 1, 1)
        with pytest.raises(AssertionError) as exc_info:
            assert_no_naive_datetimes({"raw_payload": {"created_at": naive}}, "params")
        assert "params.raw_payload.created_at" in str(exc_info.value)


# ---------------------------------------------------------------------------
# final_sanitize_order_lines_params
# ---------------------------------------------------------------------------

class TestFinalSanitizeOrderLinesParams:
    def test_naive_datetime_becomes_utc_aware(self):
        naive = datetime(2024, 6, 1, 12, 0, 0)
        result = final_sanitize_order_lines_params({"created_at": naive})
        out = result["created_at"]
        assert isinstance(out, datetime)
        assert out.tzinfo is not None
        assert out.tzinfo.utcoffset(out) is not None
        assert out.year == 2024 and out.month == 6 and out.day == 1

    def test_aware_datetime_converted_to_utc(self):
        eastern = timezone(timedelta(hours=-5))
        aware = datetime(2024, 5, 10, 7, 0, 0, tzinfo=eastern)
        result = final_sanitize_order_lines_params({"updated_at": aware})
        out = result["updated_at"]
        assert out.tzinfo == timezone.utc
        assert out.hour == 12  # 07:00 EST == 12:00 UTC

    def test_date_becomes_midnight_utc_datetime(self):
        d = date(2024, 6, 15)
        result = final_sanitize_order_lines_params({"ship_date": d})
        out = result["ship_date"]
        assert isinstance(out, datetime)
        assert out.tzinfo == timezone.utc
        assert out.year == 2024 and out.month == 6 and out.day == 15
        assert out.hour == 0 and out.minute == 0

    def test_dict_value_json_sanitized(self):
        naive = datetime(2024, 1, 1)
        payload = {"created_at": naive, "name": "test"}
        result = final_sanitize_order_lines_params({"raw_payload": payload})
        out = result["raw_payload"]
        # dict values become JSON-safe strings via _sanitize_json_value
        assert isinstance(out, dict)
        assert isinstance(out["created_at"], str)
        assert out["name"] == "test"

    def test_list_value_json_sanitized(self):
        uid = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        result = final_sanitize_order_lines_params({"items": [uid, "hello"]})
        out = result["items"]
        assert isinstance(out, list)
        assert out[0] == "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        assert out[1] == "hello"

    def test_passthrough_primitives(self):
        params = {
            "sku": "ABC",
            "quantity": 3,
            "unit_price": Decimal("9.99"),
            "mapped_product_id": None,
        }
        result = final_sanitize_order_lines_params(params)
        assert result["sku"] == "ABC"
        assert result["quantity"] == 3
        assert result["unit_price"] == Decimal("9.99")
        assert result["mapped_product_id"] is None

    def test_all_keys_preserved(self):
        naive = datetime(2024, 1, 1)
        params = {
            "order_id": 42,
            "sku": "SKU-X",
            "created_at": naive,
            "updated_at": naive,
            "raw_payload": {"note": "test"},
            "mapped_product_id": None,
        }
        result = final_sanitize_order_lines_params(params)
        assert set(result.keys()) == set(params.keys())

    def test_result_passes_assert_no_naive_datetimes(self):
        naive = datetime(2024, 6, 1, 12, 0, 0)
        params = {
            "created_at": naive,
            "updated_at": naive,
            "raw_payload": {"nested_dt": naive},
            "sku": "TEST",
        }
        result = final_sanitize_order_lines_params(params)
        # After final sanitization, assert_no_naive_datetimes must pass
        assert_no_naive_datetimes(result, "params")


# ---------------------------------------------------------------------------
# _sanitize_datetime_params — datetime-to-ISO8601 converter
# ---------------------------------------------------------------------------

class TestSanitizeDatetimeParams:
    """Regression tests for _sanitize_datetime_params().

    Verifies that all datetime/date objects are converted to ISO8601 strings
    before SQL parameter validation, covering all structural variants that
    appear in line-item and dead-letter code paths.
    """

    # 1. Simple dict with a top-level datetime
    def test_simple_dict_naive_datetime_to_iso(self):
        params = {"created_at": datetime(2024, 6, 1, 12, 0, 0)}
        result = _sanitize_datetime_params(params, "test")
        assert isinstance(result["created_at"], str)
        assert "2024-06-01" in result["created_at"]
        assert "+00:00" in result["created_at"]

    def test_simple_dict_aware_datetime_to_iso(self):
        dt = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        params = {"created_at": dt}
        result = _sanitize_datetime_params(params, "test")
        assert isinstance(result["created_at"], str)
        assert result["created_at"] == "2024-06-01T12:00:00+00:00"

    def test_simple_dict_no_datetime_objects_remain(self):
        params = {"created_at": datetime.now(timezone.utc), "sku": "ABC"}
        result = _sanitize_datetime_params(params, "test")
        for v in result.values():
            assert not isinstance(v, datetime), f"datetime object found: {v!r}"

    # 2. Nested dict
    def test_nested_dict_datetime_to_iso(self):
        params = {"payload": {"created_at": datetime(2024, 3, 15, tzinfo=timezone.utc)}}
        result = _sanitize_datetime_params(params, "test")
        assert isinstance(result["payload"]["created_at"], str)
        assert "2024-03-15" in result["payload"]["created_at"]

    def test_nested_dict_no_datetime_objects_remain(self):
        params = {
            "outer": {
                "inner": {"created_at": datetime.now(timezone.utc)},
                "updated_at": datetime.now(timezone.utc),
            }
        }
        result = _sanitize_datetime_params(params, "test")

        def _check_no_dt(obj):
            if isinstance(obj, datetime):
                raise AssertionError(f"datetime found: {obj!r}")
            if isinstance(obj, dict):
                for v in obj.values():
                    _check_no_dt(v)
            if isinstance(obj, list):
                for item in obj:
                    _check_no_dt(item)

        _check_no_dt(result)

    # 3. List of dicts
    def test_list_of_dicts_datetime_to_iso(self):
        params = {
            "items": [
                {"created_at": datetime(2024, 1, 1, tzinfo=timezone.utc)},
                {"created_at": datetime(2024, 2, 1, tzinfo=timezone.utc)},
            ]
        }
        result = _sanitize_datetime_params(params, "test")
        for item in result["items"]:
            assert isinstance(item["created_at"], str)
            assert not isinstance(item["created_at"], datetime)

    # 4. Line item payload
    def test_line_item_payload_datetime_to_iso(self):
        params = {
            "order_id": 42,
            "sku": "SKU-001",
            "created_at": datetime(2024, 5, 10, 8, 0, 0, tzinfo=timezone.utc),
            "updated_at": datetime(2024, 5, 10, 8, 0, 0, tzinfo=timezone.utc),
            "line_items": [
                {"created_at": datetime(2024, 5, 10, tzinfo=timezone.utc), "qty": 3}
            ],
        }
        result = _sanitize_datetime_params(params, "line_item_upsert")
        assert isinstance(result["created_at"], str)
        assert isinstance(result["updated_at"], str)
        assert isinstance(result["line_items"][0]["created_at"], str)
        assert result["line_items"][0]["qty"] == 3

    # 5. Dead-letter payload
    def test_dead_letter_payload_datetime_to_iso(self):
        params = {
            "brand_id": "some-brand",
            "now": datetime(2024, 6, 1, tzinfo=timezone.utc),
            "raw_payload": {"created_at": datetime(2024, 6, 1, tzinfo=timezone.utc)},
        }
        result = _sanitize_datetime_params(params, "dead_letter_write")
        assert isinstance(result["now"], str)
        assert isinstance(result["raw_payload"]["created_at"], str)

    # 6. Naive datetime gets UTC offset in ISO string
    def test_naive_datetime_gets_utc_offset(self):
        params = {"created_at": datetime(2024, 1, 15, 10, 30, 0)}  # naive
        result = _sanitize_datetime_params(params, "test")
        assert "+00:00" in result["created_at"]

    # 7. date object (not datetime) becomes ISO date string
    def test_date_object_to_iso_string(self):
        params = {"ship_date": date(2024, 7, 4)}
        result = _sanitize_datetime_params(params, "test")
        assert isinstance(result["ship_date"], str)
        assert result["ship_date"] == "2024-07-04"

    # 8. Non-datetime values pass through unchanged
    def test_non_datetime_values_pass_through(self):
        params = {
            "sku": "ABC-123",
            "quantity": 5,
            "unit_price": Decimal("9.99"),
            "mapped_product_id": None,
            "active": True,
        }
        result = _sanitize_datetime_params(params, "test")
        assert result["sku"] == "ABC-123"
        assert result["quantity"] == 5
        assert result["unit_price"] == Decimal("9.99")
        assert result["mapped_product_id"] is None
        assert result["active"] is True

    # 9. All keys preserved
    def test_all_keys_preserved(self):
        params = {
            "order_id": 1,
            "sku": "X",
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
        }
        result = _sanitize_datetime_params(params, "test")
        assert set(result.keys()) == set(params.keys())

    # 10. Empty dict returns empty dict
    def test_empty_dict_returns_empty_dict(self):
        result = _sanitize_datetime_params({}, "test")
        assert result == {}

    # 11. Non-dict input returned unchanged
    def test_non_dict_input_returned_unchanged(self):
        assert _sanitize_datetime_params(None, "test") is None
        assert _sanitize_datetime_params("string", "test") == "string"
        assert _sanitize_datetime_params(42, "test") == 42
