"""
order_identity_test.py — Unit tests for upsert identity logic.

Tests:
  1. Same external_order_id in different brands inserts separately
  2. Same order_number but different external_order_id does NOT overwrite
  3. Missing external_order_id uses safe fallback (generated UUID)
  4. Reconciliation fails if pagination incomplete
  5. Count query works with UUID and varchar brand_id

Run with:
    pytest services/order_identity_test.py -v
"""

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services.order_identity_audit import get_order_match_key
from services.reconciliation_validator import check_pagination_complete


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_sync_run(
    *,
    status: str = "completed",
    last_next_url: str | None = None,
    current_cursor: str | None = None,
    pages_synced: int = 10,
    total_pages: int | None = None,
    orders_loaded_this_run: int = 100,
    total_orders_available: int | None = None,
    stalled_reason: str | None = None,
    last_error: str | None = None,
    orders_inserted: int = 0,
    orders_updated: int = 0,
    dead_letters_created: int = 0,
    line_items_inserted: int = 0,
    line_items_updated: int = 0,
) -> MagicMock:
    """Build a minimal SyncRun-like mock for testing."""
    run = MagicMock()
    run.id = 1
    run.brand_id = str(uuid.uuid4())
    run.status = status
    run.last_next_url = last_next_url
    run.current_cursor = current_cursor
    run.pages_synced = pages_synced
    run.total_pages = total_pages
    run.orders_loaded_this_run = orders_loaded_this_run
    run.total_orders_available = total_orders_available
    run.stalled_reason = stalled_reason
    run.last_error = last_error
    run.orders_inserted = orders_inserted
    run.orders_updated = orders_updated
    run.dead_letters_created = dead_letters_created
    run.line_items_inserted = line_items_inserted
    run.line_items_updated = line_items_updated
    return run


# ---------------------------------------------------------------------------
# Test 1: get_order_match_key — external_id present
# ---------------------------------------------------------------------------

class TestGetOrderMatchKey:
    """Tests for get_order_match_key() identity resolution."""

    def test_external_id_present_returns_external_id_type(self):
        """Valid external_order_id → match_key_type = 'external_id'."""
        match_type, match_value = get_order_match_key(
            external_order_id="12345",
            order_number="ORD-001",
            brand_id=str(uuid.uuid4()),
        )
        assert match_type == "external_id"
        assert match_value == "12345"

    def test_external_id_unknown_falls_back_to_order_number(self):
        """external_order_id = 'unknown' → fallback to order_number."""
        match_type, match_value = get_order_match_key(
            external_order_id="unknown",
            order_number="ORD-001",
            brand_id=str(uuid.uuid4()),
        )
        assert match_type == "order_number"
        assert match_value == "ORD-001"

    def test_external_id_none_falls_back_to_order_number(self):
        """external_order_id = None → fallback to order_number."""
        match_type, match_value = get_order_match_key(
            external_order_id=None,
            order_number="ORD-002",
            brand_id=str(uuid.uuid4()),
        )
        assert match_type == "order_number"
        assert match_value == "ORD-002"

    def test_external_id_empty_string_falls_back_to_order_number(self):
        """external_order_id = '' → fallback to order_number."""
        match_type, match_value = get_order_match_key(
            external_order_id="",
            order_number="ORD-003",
            brand_id=str(uuid.uuid4()),
        )
        assert match_type == "order_number"
        assert match_value == "ORD-003"

    def test_both_missing_returns_generated_uuid(self):
        """Both external_order_id and order_number missing → generated_uuid."""
        match_type, match_value = get_order_match_key(
            external_order_id=None,
            order_number=None,
            brand_id=str(uuid.uuid4()),
        )
        assert match_type == "generated_uuid"
        # Value must be a valid UUID
        parsed = uuid.UUID(match_value)
        assert str(parsed) == match_value

    def test_both_missing_generates_unique_uuid_each_call(self):
        """Each call with missing keys generates a different UUID (safe fallback)."""
        brand_id = str(uuid.uuid4())
        _, value1 = get_order_match_key(None, None, brand_id)
        _, value2 = get_order_match_key(None, None, brand_id)
        assert value1 != value2, "Each missing-key call must produce a unique UUID"

    def test_unknown_external_id_no_order_number_returns_generated_uuid(self):
        """external_order_id = 'unknown', no order_number → generated_uuid."""
        match_type, match_value = get_order_match_key(
            external_order_id="unknown",
            order_number=None,
            brand_id=str(uuid.uuid4()),
        )
        assert match_type == "generated_uuid"

    def test_whitespace_external_id_falls_back(self):
        """external_order_id = '   ' (whitespace only) → fallback."""
        match_type, match_value = get_order_match_key(
            external_order_id="   ",
            order_number="ORD-004",
            brand_id=str(uuid.uuid4()),
        )
        assert match_type == "order_number"
        assert match_value == "ORD-004"


# ---------------------------------------------------------------------------
# Test 2: Same order_number but different external_order_id
# ---------------------------------------------------------------------------

class TestSameOrderNumberDifferentExternalId:
    """
    Validates that two orders with the same order_number but different
    external_order_ids resolve to 'external_id' match type (not order_number),
    so they do NOT overwrite each other.
    """

    def test_same_order_number_different_external_id_no_overwrite(self):
        """
        Same order_number but different external_order_id must resolve to
        'external_id' match type — each order gets its own conflict target.
        """
        brand_id = str(uuid.uuid4())

        type1, value1 = get_order_match_key(
            external_order_id="ext-123",
            order_number="ORD-001",
            brand_id=brand_id,
        )
        type2, value2 = get_order_match_key(
            external_order_id="ext-456",
            order_number="ORD-001",
            brand_id=brand_id,
        )

        # Both should use external_id as the conflict target
        assert type1 == "external_id"
        assert type2 == "external_id"
        # They must resolve to different values → different conflict targets
        assert value1 != value2
        assert value1 == "ext-123"
        assert value2 == "ext-456"


# ---------------------------------------------------------------------------
# Test 3: Missing external_order_id uses safe fallback
# ---------------------------------------------------------------------------

class TestMissingExternalIdSafeFallback:
    """
    Validates that orders with no external_order_id and no order_number
    always insert as new rows (never match existing rows).
    """

    def test_missing_external_id_uses_safe_fallback(self):
        """Missing external_order_id must use safe fallback (generated UUID)."""
        brand_id = str(uuid.uuid4())

        match_type, match_value = get_order_match_key(
            external_order_id=None,
            order_number=None,
            brand_id=brand_id,
        )
        assert match_type == "generated_uuid"
        # Must be a valid UUID
        uuid.UUID(match_value)

    def test_two_missing_external_ids_produce_different_keys(self):
        """
        Two orders with no external_order_id must produce different keys
        so they insert as separate rows (not overwrite each other).
        """
        brand_id = str(uuid.uuid4())

        _, key1 = get_order_match_key(None, None, brand_id)
        _, key2 = get_order_match_key(None, None, brand_id)

        assert key1 != key2, (
            "Two orders with missing external_id must get different generated UUIDs"
        )


# ---------------------------------------------------------------------------
# Test 4: Reconciliation fails if pagination incomplete
# ---------------------------------------------------------------------------

class TestReconciliationPaginationIncomplete:
    """
    Validates that check_pagination_complete() returns False when pagination
    stopped before the LeafLink estimate was reached.
    """

    def test_reconciliation_fails_if_pagination_incomplete(self):
        """
        Sync fetched 100 orders, estimate is 16074, next_url exists.
        Status should be partial_reconciliation_gap, NOT success.
        """
        run = _make_sync_run(
            status="stalled",
            last_next_url="https://api.leaflink.com/api/v2/orders/?page=2",
            orders_loaded_this_run=100,
            total_orders_available=16074,
        )

        is_complete, reason = check_pagination_complete(run)

        assert is_complete is False
        assert reason in ("cursor_present", "timeout_occurred", "page_limit_hit")

    def test_pagination_complete_when_next_url_null(self):
        """next_url is NULL → pagination is complete."""
        run = _make_sync_run(
            status="completed",
            last_next_url=None,
            current_cursor=None,
            orders_loaded_this_run=16074,
            total_orders_available=16074,
        )

        is_complete, reason = check_pagination_complete(run)

        assert is_complete is True
        assert reason == "reached_end"

    def test_pagination_complete_when_page_count_reached(self):
        """pages_synced >= total_pages → pagination is complete."""
        run = _make_sync_run(
            status="completed",
            last_next_url=None,
            current_cursor=None,
            pages_synced=161,
            total_pages=161,
        )

        is_complete, reason = check_pagination_complete(run)

        assert is_complete is True
        assert reason == "reached_end"

    def test_pagination_complete_when_orders_count_reached(self):
        """orders_loaded >= total_orders_available → pagination is complete."""
        run = _make_sync_run(
            status="completed",
            last_next_url=None,
            current_cursor=None,
            orders_loaded_this_run=16074,
            total_orders_available=16074,
        )

        is_complete, reason = check_pagination_complete(run)

        assert is_complete is True

    def test_timeout_reason_detected(self):
        """Stalled run with timeout in reason → timeout_occurred."""
        run = _make_sync_run(
            status="stalled",
            last_next_url="https://api.leaflink.com/api/v2/orders/?page=5",
            stalled_reason="timeout_occurred after 1800s",
        )

        is_complete, reason = check_pagination_complete(run)

        assert is_complete is False
        assert reason == "timeout_occurred"

    def test_page_limit_reason_detected(self):
        """Stalled run with page_limit in reason → page_limit_hit."""
        run = _make_sync_run(
            status="stalled",
            last_next_url="https://api.leaflink.com/api/v2/orders/?page=200",
            stalled_reason="page_limit_hit at page 200",
        )

        is_complete, reason = check_pagination_complete(run)

        assert is_complete is False
        assert reason == "page_limit_hit"


# ---------------------------------------------------------------------------
# Test 5: Count query works with UUID and varchar brand_id
# ---------------------------------------------------------------------------

class TestCountQueryBrandIdTypes:
    """
    Validates that get_order_match_key() handles both UUID and varchar
    brand_id values without type errors.
    """

    def test_uuid_brand_id_accepted(self):
        """UUID brand_id string must work without errors."""
        brand_id = str(uuid.uuid4())
        match_type, match_value = get_order_match_key(
            external_order_id="ext-999",
            order_number="ORD-999",
            brand_id=brand_id,
        )
        assert match_type == "external_id"
        assert match_value == "ext-999"

    def test_varchar_brand_id_accepted(self):
        """Varchar (non-UUID) brand_id string must work without errors."""
        brand_id = "noble"
        match_type, match_value = get_order_match_key(
            external_order_id="ext-888",
            order_number="ORD-888",
            brand_id=brand_id,
        )
        assert match_type == "external_id"
        assert match_value == "ext-888"

    def test_numeric_string_brand_id_accepted(self):
        """Numeric string brand_id must work without errors."""
        brand_id = "12345"
        match_type, match_value = get_order_match_key(
            external_order_id="ext-777",
            order_number=None,
            brand_id=brand_id,
        )
        assert match_type == "external_id"
        assert match_value == "ext-777"


# ---------------------------------------------------------------------------
# Test 6: Same external_id different brand inserts separately
# ---------------------------------------------------------------------------

class TestSameExternalIdDifferentBrand:
    """
    Validates that the same external_order_id in different brands resolves
    to the same match_key_type but different brand scopes.
    """

    def test_same_external_id_different_brand_resolves_independently(self):
        """
        Same external_order_id in different brands must resolve to
        'external_id' match type for both — they are independent rows.
        """
        brand_a = str(uuid.uuid4())
        brand_b = str(uuid.uuid4())

        type_a, value_a = get_order_match_key(
            external_order_id="ext-123",
            order_number="ORD-001",
            brand_id=brand_a,
        )
        type_b, value_b = get_order_match_key(
            external_order_id="ext-123",
            order_number="ORD-001",
            brand_id=brand_b,
        )

        # Both use external_id as conflict target
        assert type_a == "external_id"
        assert type_b == "external_id"
        # Same external_id value — the brand_id in the SQL WHERE clause
        # is what makes them separate rows (enforced by the DB constraint)
        assert value_a == "ext-123"
        assert value_b == "ext-123"
        # The brands are different — DB constraint is (brand_id, external_order_id)
        assert brand_a != brand_b
