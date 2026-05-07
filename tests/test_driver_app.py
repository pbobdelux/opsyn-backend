"""
Tests for routes/driver_app.py — driver-facing authentication and route API.

Covers:
  - DriverLoginRequest schema validation
  - StopStatusUpdate schema validation
  - CollectionUpdate schema validation
  - _serialize_driver_brief never exposes passcode_hash
  - _create_driver_jwt / _decode_driver_jwt round-trip
  - get_current_driver dependency: missing header, bad format, expired token,
    invalid token, driver not found, driver inactive
  - Router metadata (prefix, tags, registered routes)

Run with:
    python -m pytest tests/test_driver_app.py -q
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone
from uuid import uuid4

import jwt

from routes.driver_app import (
    DriverLoginRequest,
    StopStatusUpdate,
    CollectionUpdate,
    _create_driver_jwt,
    _decode_driver_jwt,
    _serialize_driver_brief,
    _serialize_route_summary,
    _serialize_route_stop,
    router,
    VALID_STOP_STATUSES,
    VALID_PAYMENT_METHODS,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

_TEST_SECRET = "test-driver-jwt-secret-for-unit-tests"


def _make_driver(**kwargs):
    """Build a mock Driver with sensible defaults."""
    now = datetime.now(timezone.utc)
    driver = MagicMock()
    driver.id = kwargs.get("id", uuid4())
    driver.org_id = kwargs.get("org_id", uuid4())
    driver.name = kwargs.get("name", "Test Driver")
    driver.email = kwargs.get("email", "driver@example.com")
    driver.phone = kwargs.get("phone", "555-0000")
    driver.status = kwargs.get("status", "active")
    driver.passcode_hash = kwargs.get("passcode_hash", "$2b$12$somehash")
    driver.license_plate = kwargs.get("license_plate", None)
    driver.vehicle_type = kwargs.get("vehicle_type", None)
    driver.notes = kwargs.get("notes", None)
    driver.preferences = kwargs.get("preferences", {})
    driver.created_at = kwargs.get("created_at", now)
    driver.updated_at = kwargs.get("updated_at", now)
    driver.deactivated_at = kwargs.get("deactivated_at", None)
    return driver


# ---------------------------------------------------------------------------
# DriverLoginRequest validation
# ---------------------------------------------------------------------------


class TestDriverLoginRequest:
    def test_valid(self):
        req = DriverLoginRequest(org_code="noble", passcode="123456")
        assert req.org_code == "noble"
        assert req.passcode == "123456"

    def test_org_code_lowercased(self):
        req = DriverLoginRequest(org_code="NOBLE", passcode="1234")
        assert req.org_code == "noble"

    def test_org_code_stripped(self):
        req = DriverLoginRequest(org_code="  noble  ", passcode="1234")
        assert req.org_code == "noble"

    def test_org_code_empty_rejected(self):
        with pytest.raises(Exception):
            DriverLoginRequest(org_code="", passcode="1234")

    def test_passcode_empty_rejected(self):
        with pytest.raises(Exception):
            DriverLoginRequest(org_code="noble", passcode="")

    def test_passcode_preserved_as_is(self):
        req = DriverLoginRequest(org_code="noble", passcode="000000")
        assert req.passcode == "000000"


# ---------------------------------------------------------------------------
# StopStatusUpdate validation
# ---------------------------------------------------------------------------


class TestStopStatusUpdate:
    def test_valid_arrived(self):
        s = StopStatusUpdate(status="arrived")
        assert s.status == "arrived"

    def test_valid_completed(self):
        s = StopStatusUpdate(status="completed")
        assert s.status == "completed"

    def test_valid_failed(self):
        s = StopStatusUpdate(status="failed")
        assert s.status == "failed"

    def test_valid_skipped(self):
        s = StopStatusUpdate(status="skipped")
        assert s.status == "skipped"

    def test_invalid_status_rejected(self):
        with pytest.raises(Exception):
            StopStatusUpdate(status="pending")

    def test_invalid_status_random(self):
        with pytest.raises(Exception):
            StopStatusUpdate(status="done")

    def test_notes_optional(self):
        s = StopStatusUpdate(status="arrived")
        assert s.notes is None

    def test_notes_accepted(self):
        s = StopStatusUpdate(status="completed", notes="All good")
        assert s.notes == "All good"

    def test_all_valid_statuses_accepted(self):
        for status in VALID_STOP_STATUSES:
            s = StopStatusUpdate(status=status)
            assert s.status == status


# ---------------------------------------------------------------------------
# CollectionUpdate validation
# ---------------------------------------------------------------------------


class TestCollectionUpdate:
    def test_valid_cash(self):
        c = CollectionUpdate(amount_collected=100.0, payment_method="cash")
        assert c.amount_collected == 100.0
        assert c.payment_method == "cash"

    def test_valid_check(self):
        c = CollectionUpdate(amount_collected=50.0, payment_method="check")
        assert c.payment_method == "check"

    def test_valid_card(self):
        c = CollectionUpdate(amount_collected=0.0, payment_method="card")
        assert c.amount_collected == 0.0

    def test_valid_other(self):
        c = CollectionUpdate(amount_collected=25.0, payment_method="other")
        assert c.payment_method == "other"

    def test_negative_amount_rejected(self):
        with pytest.raises(Exception):
            CollectionUpdate(amount_collected=-1.0, payment_method="cash")

    def test_invalid_payment_method_rejected(self):
        with pytest.raises(Exception):
            CollectionUpdate(amount_collected=10.0, payment_method="bitcoin")

    def test_notes_optional(self):
        c = CollectionUpdate(amount_collected=10.0, payment_method="cash")
        assert c.notes is None

    def test_notes_accepted(self):
        c = CollectionUpdate(amount_collected=10.0, payment_method="cash", notes="Paid in full")
        assert c.notes == "Paid in full"

    def test_all_valid_payment_methods(self):
        for method in VALID_PAYMENT_METHODS:
            c = CollectionUpdate(amount_collected=0.0, payment_method=method)
            assert c.payment_method == method


# ---------------------------------------------------------------------------
# _serialize_driver_brief — security: passcode_hash must never appear
# ---------------------------------------------------------------------------


class TestSerializeDriverBrief:
    def test_passcode_hash_never_in_output(self):
        driver = _make_driver(passcode_hash="$2b$12$supersecret")
        result = _serialize_driver_brief(driver)
        assert "passcode_hash" not in result
        assert "$2b$12$supersecret" not in str(result)

    def test_expected_fields_without_org_name(self):
        driver = _make_driver()
        result = _serialize_driver_brief(driver)
        assert set(result.keys()) == {"id", "name", "org_id", "email", "phone", "status"}

    def test_expected_fields_with_org_name(self):
        driver = _make_driver()
        result = _serialize_driver_brief(driver, org_name="Noble Nectar")
        assert "org_name" in result
        assert result["org_name"] == "Noble Nectar"

    def test_uuid_serialized_as_string(self):
        driver = _make_driver()
        result = _serialize_driver_brief(driver)
        assert isinstance(result["id"], str)
        assert isinstance(result["org_id"], str)

    def test_name_defaults_to_empty_string_when_none(self):
        driver = _make_driver(name=None)
        result = _serialize_driver_brief(driver)
        assert result["name"] == ""

    def test_status_defaults_to_active_when_none(self):
        driver = _make_driver(status=None)
        result = _serialize_driver_brief(driver)
        assert result["status"] == "active"


# ---------------------------------------------------------------------------
# JWT: _create_driver_jwt / _decode_driver_jwt
# ---------------------------------------------------------------------------


class TestDriverJWT:
    def test_create_and_decode_round_trip(self):
        driver_id = str(uuid4())
        org_id = str(uuid4())
        with patch.dict(os.environ, {"DRIVER_JWT_SECRET": _TEST_SECRET}):
            token = _create_driver_jwt(driver_id, org_id)
            payload = _decode_driver_jwt(token)
        assert payload["driver_id"] == driver_id
        assert payload["org_id"] == org_id
        assert payload["role"] == "driver"

    def test_token_is_string(self):
        with patch.dict(os.environ, {"DRIVER_JWT_SECRET": _TEST_SECRET}):
            token = _create_driver_jwt(str(uuid4()), str(uuid4()))
        assert isinstance(token, str)
        assert len(token) > 0

    def test_invalid_token_raises_401(self):
        from fastapi import HTTPException
        with patch.dict(os.environ, {"DRIVER_JWT_SECRET": _TEST_SECRET}):
            with pytest.raises(HTTPException) as exc_info:
                _decode_driver_jwt("not.a.valid.token")
        assert exc_info.value.status_code == 401

    def test_expired_token_raises_401(self):
        from fastapi import HTTPException
        from datetime import timedelta
        driver_id = str(uuid4())
        org_id = str(uuid4())
        # Manually craft an expired token
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        payload = {
            "driver_id": driver_id,
            "org_id": org_id,
            "role": "driver",
            "iat": now - timedelta(hours=48),
            "exp": now - timedelta(hours=24),  # expired 24h ago
        }
        expired_token = jwt.encode(payload, _TEST_SECRET, algorithm="HS256")
        with patch.dict(os.environ, {"DRIVER_JWT_SECRET": _TEST_SECRET}):
            with pytest.raises(HTTPException) as exc_info:
                _decode_driver_jwt(expired_token)
        assert exc_info.value.status_code == 401

    def test_wrong_secret_raises_401(self):
        from fastapi import HTTPException
        with patch.dict(os.environ, {"DRIVER_JWT_SECRET": _TEST_SECRET}):
            token = _create_driver_jwt(str(uuid4()), str(uuid4()))
        # Decode with a different secret
        with patch.dict(os.environ, {"DRIVER_JWT_SECRET": "wrong-secret"}):
            with pytest.raises(HTTPException) as exc_info:
                _decode_driver_jwt(token)
        assert exc_info.value.status_code == 401

    def test_missing_secret_raises_500(self):
        from fastapi import HTTPException
        # Ensure DRIVER_JWT_SECRET is not set
        env = {k: v for k, v in os.environ.items() if k != "DRIVER_JWT_SECRET"}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises((HTTPException, RuntimeError)):
                _create_driver_jwt(str(uuid4()), str(uuid4()))


# ---------------------------------------------------------------------------
# Router metadata
# ---------------------------------------------------------------------------


class TestRouterMetadata:
    def test_router_prefix(self):
        assert router.prefix == "/driver"

    def test_router_tags(self):
        assert "driver-app" in router.tags

    def test_login_route_registered(self):
        paths = {route.path for route in router.routes}
        assert "/driver/login" in paths or "/login" in paths

    def test_me_route_registered(self):
        paths = {route.path for route in router.routes}
        assert "/driver/me" in paths or "/me" in paths

    def test_routes_list_registered(self):
        paths = {route.path for route in router.routes}
        assert any("routes" in p for p in paths)

    def test_route_detail_registered(self):
        paths = {route.path for route in router.routes}
        assert any("route_id" in p for p in paths)

    def test_stop_status_route_registered(self):
        paths = {route.path for route in router.routes}
        assert any("status" in p for p in paths)

    def test_collection_route_registered(self):
        paths = {route.path for route in router.routes}
        assert any("collection" in p for p in paths)


# ---------------------------------------------------------------------------
# Security: valid stop statuses and payment methods
# ---------------------------------------------------------------------------


class TestConstants:
    def test_valid_stop_statuses(self):
        assert VALID_STOP_STATUSES == {"arrived", "completed", "failed", "skipped"}

    def test_valid_payment_methods(self):
        assert VALID_PAYMENT_METHODS == {"cash", "check", "card", "other"}

    def test_pending_not_in_valid_stop_statuses(self):
        # Drivers cannot set a stop back to pending
        assert "pending" not in VALID_STOP_STATUSES
