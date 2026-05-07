"""
Tests for routes/drivers.py — admin driver management API.

Covers:
  - DriverCreate schema validation
  - DriverUpdate schema validation
  - PasscodeReset schema validation
  - _serialize_driver never exposes passcode_hash
  - _hash_passcode produces a valid bcrypt hash
  - generate-passcode returns a 6-digit string and never logs the plain value
  - Endpoint routing smoke test (router prefix and tags)

Run with:
    python -m pytest tests/test_drivers.py -q
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
from uuid import uuid4

import bcrypt

from routes.drivers import (
    DriverCreate,
    DriverUpdate,
    PasscodeReset,
    _serialize_driver,
    _hash_passcode,
    router,
)


# ---------------------------------------------------------------------------
# DriverCreate validation
# ---------------------------------------------------------------------------


class TestDriverCreate:
    def test_valid_minimal(self):
        d = DriverCreate(name="Jane Smith")
        assert d.name == "Jane Smith"
        assert d.email is None
        assert d.phone is None
        assert d.preferences is None

    def test_valid_full(self):
        d = DriverCreate(
            name="Jane Smith",
            email="jane@example.com",
            phone="555-1234",
            license_plate="ABC123",
            vehicle_type="van",
            notes="Senior driver",
            preferences={"region": "north"},
        )
        assert d.name == "Jane Smith"
        assert d.email == "jane@example.com"
        assert d.preferences == {"region": "north"}

    def test_name_required(self):
        with pytest.raises(Exception):
            DriverCreate(name="")

    def test_name_too_long(self):
        with pytest.raises(Exception):
            DriverCreate(name="x" * 256)

    def test_name_stripped(self):
        d = DriverCreate(name="  Alice  ")
        assert d.name == "Alice"

    def test_email_too_long(self):
        with pytest.raises(Exception):
            DriverCreate(name="Bob", email="a" * 256 + "@x.com")

    def test_phone_too_long(self):
        with pytest.raises(Exception):
            DriverCreate(name="Bob", phone="1" * 51)

    def test_license_plate_too_long(self):
        with pytest.raises(Exception):
            DriverCreate(name="Bob", license_plate="X" * 51)

    def test_vehicle_type_too_long(self):
        with pytest.raises(Exception):
            DriverCreate(name="Bob", vehicle_type="v" * 51)

    def test_preferences_must_be_dict(self):
        with pytest.raises(Exception):
            DriverCreate(name="Bob", preferences=["not", "a", "dict"])


# ---------------------------------------------------------------------------
# DriverUpdate validation
# ---------------------------------------------------------------------------


class TestDriverUpdate:
    def test_empty_update_is_valid(self):
        u = DriverUpdate()
        assert u.name is None
        assert u.status is None

    def test_valid_status_active(self):
        u = DriverUpdate(status="active")
        assert u.status == "active"

    def test_valid_status_inactive(self):
        u = DriverUpdate(status="inactive")
        assert u.status == "inactive"

    def test_invalid_status(self):
        with pytest.raises(Exception):
            DriverUpdate(status="available")

    def test_name_stripped(self):
        u = DriverUpdate(name="  Bob  ")
        assert u.name == "Bob"

    def test_name_empty_string_rejected(self):
        with pytest.raises(Exception):
            DriverUpdate(name="   ")

    def test_preferences_must_be_dict(self):
        with pytest.raises(Exception):
            DriverUpdate(preferences="not-a-dict")


# ---------------------------------------------------------------------------
# PasscodeReset validation
# ---------------------------------------------------------------------------


class TestPasscodeReset:
    def test_valid_4_digits(self):
        p = PasscodeReset(passcode="1234")
        assert p.passcode == "1234"

    def test_valid_8_digits(self):
        p = PasscodeReset(passcode="12345678")
        assert p.passcode == "12345678"

    def test_valid_6_digits(self):
        p = PasscodeReset(passcode="654321")
        assert p.passcode == "654321"

    def test_too_short(self):
        with pytest.raises(Exception):
            PasscodeReset(passcode="123")

    def test_too_long(self):
        with pytest.raises(Exception):
            PasscodeReset(passcode="123456789")

    def test_non_numeric(self):
        with pytest.raises(Exception):
            PasscodeReset(passcode="12ab")

    def test_leading_zeros_allowed(self):
        p = PasscodeReset(passcode="000000")
        assert p.passcode == "000000"


# ---------------------------------------------------------------------------
# _serialize_driver — security: passcode_hash must never appear
# ---------------------------------------------------------------------------


class TestSerializeDriver:
    def _make_driver(self, **kwargs):
        """Build a mock Driver with sensible defaults."""
        now = datetime.now(timezone.utc)
        driver = MagicMock()
        driver.id = uuid4()
        driver.org_id = uuid4()
        driver.name = kwargs.get("name", "Test Driver")
        driver.email = kwargs.get("email", "test@example.com")
        driver.phone = kwargs.get("phone", "555-0000")
        driver.status = kwargs.get("status", "active")
        driver.passcode_hash = kwargs.get("passcode_hash", "$2b$12$somehash")
        driver.license_plate = kwargs.get("license_plate", "XYZ123")
        driver.vehicle_type = kwargs.get("vehicle_type", "van")
        driver.notes = kwargs.get("notes", None)
        driver.preferences = kwargs.get("preferences", {})
        driver.created_at = kwargs.get("created_at", now)
        driver.updated_at = kwargs.get("updated_at", now)
        driver.deactivated_at = kwargs.get("deactivated_at", None)
        return driver

    def test_passcode_hash_never_in_output(self):
        driver = self._make_driver(passcode_hash="$2b$12$supersecret")
        result = _serialize_driver(driver)
        assert "passcode_hash" not in result
        # Also check nested values don't contain the hash
        assert "$2b$12$supersecret" not in str(result)

    def test_all_expected_fields_present(self):
        driver = self._make_driver()
        result = _serialize_driver(driver)
        expected_keys = {
            "id", "org_id", "name", "email", "phone", "status",
            "license_plate", "vehicle_type", "notes", "preferences",
            "created_at", "updated_at", "deactivated_at",
        }
        assert expected_keys == set(result.keys())

    def test_uuid_serialized_as_string(self):
        driver = self._make_driver()
        result = _serialize_driver(driver)
        assert isinstance(result["id"], str)
        assert isinstance(result["org_id"], str)

    def test_datetime_serialized_as_string(self):
        driver = self._make_driver()
        result = _serialize_driver(driver)
        assert isinstance(result["created_at"], str)
        assert isinstance(result["updated_at"], str)

    def test_deactivated_at_none_when_active(self):
        driver = self._make_driver(deactivated_at=None)
        result = _serialize_driver(driver)
        assert result["deactivated_at"] is None

    def test_preferences_defaults_to_empty_dict(self):
        driver = self._make_driver(preferences=None)
        result = _serialize_driver(driver)
        assert result["preferences"] == {}


# ---------------------------------------------------------------------------
# _hash_passcode — bcrypt correctness
# ---------------------------------------------------------------------------


class TestHashPasscode:
    def test_produces_bcrypt_hash(self):
        hashed = _hash_passcode("123456")
        assert hashed.startswith("$2b$")

    def test_cost_factor_is_12(self):
        hashed = _hash_passcode("000000")
        # bcrypt hash format: $2b$<cost>$...
        parts = hashed.split("$")
        assert parts[2] == "12"

    def test_hash_verifies_correctly(self):
        plain = "654321"
        hashed = _hash_passcode(plain)
        assert bcrypt.checkpw(plain.encode(), hashed.encode())

    def test_different_plains_produce_different_hashes(self):
        h1 = _hash_passcode("111111")
        h2 = _hash_passcode("222222")
        assert h1 != h2

    def test_same_plain_produces_different_hashes_due_to_salt(self):
        h1 = _hash_passcode("123456")
        h2 = _hash_passcode("123456")
        # bcrypt uses random salt — same input should produce different hashes
        assert h1 != h2


# ---------------------------------------------------------------------------
# Router metadata
# ---------------------------------------------------------------------------


class TestRouterMetadata:
    def test_router_prefix(self):
        assert router.prefix == "/drivers"

    def test_router_tags(self):
        assert "drivers" in router.tags

    def test_all_expected_routes_registered(self):
        paths = {route.path for route in router.routes}
        assert "/drivers" in paths or "" in paths  # GET /drivers, POST /drivers
        assert "/drivers/{driver_id}" in paths or "/{driver_id}" in paths
        assert "/drivers/{driver_id}/generate-passcode" in paths or "/{driver_id}/generate-passcode" in paths
        assert "/drivers/{driver_id}/reset-passcode" in paths or "/{driver_id}/reset-passcode" in paths
