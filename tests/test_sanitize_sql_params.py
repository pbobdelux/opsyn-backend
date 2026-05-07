"""Unit tests for sanitize_sql_params() and _sanitize_json_value().

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

from services.leaflink_sync import sanitize_sql_params, _sanitize_json_value


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
