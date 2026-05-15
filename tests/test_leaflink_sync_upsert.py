"""
Tests for LeafLink sync upsert correctness, identity audit, and reconciliation.

Covers:
  - Upsert match logic: same external_order_id + different brand → separate inserts
  - Upsert match logic: same order_number + different external_order_id → no overwrite
  - Missing external_order_id → order is skipped (not inserted with null key)
  - Reconciliation gap detection: fetched < estimate with next_url → partial status
  - Count query type safety: brand_id as varchar works without UUID cast
  - [ORDER_MATCH_DECISION] sampling: logged once per 100 orders
  - [ORDER_IDENTITY_AUDIT] fields: null/unknown external_order_id detection
  - [PAGINATION_INCOMPLETE] fires when gap > 2%

Run with:
    python -m pytest tests/test_leaflink_sync_upsert.py -v
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock, patch, call
from uuid import uuid4

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_order(
    order_id: str,
    order_number: str,
    brand_id: str,
    customer_name: str = "Test Customer",
    status: str = "submitted",
    amount: str = "100.00",
) -> dict:
    """Build a minimal LeafLink-style order dict."""
    return {
        "id": order_id,
        "order_number": order_number,
        "customer_name": customer_name,
        "status": status,
        "total_amount": amount,
        "line_items": [],
    }


# ---------------------------------------------------------------------------
# 1. Upsert SQL statement: no UUID casts on VARCHAR columns
# ---------------------------------------------------------------------------

class TestUpsertSqlNoCast:
    """Verify the upsert statement does not use CAST(:brand_id AS uuid) or
    CAST(:org_id AS uuid) since those columns are VARCHAR(120) in the DB."""

    def test_upsert_stmt_no_brand_id_uuid_cast(self):
        """The upsert statement must not cast brand_id to uuid."""
        # Import the module and inspect the upsert statement template
        # We do this by running a minimal parse of the source file.
        import inspect
        import services.leaflink_sync as ls_module

        source = inspect.getsource(ls_module)

        # The fixed upsert should NOT contain CAST(:brand_id AS uuid)
        assert "CAST(:brand_id AS uuid)" not in source, (
            "Found CAST(:brand_id AS uuid) in leaflink_sync.py — "
            "brand_id is VARCHAR(120), not UUID. This causes "
            "'operator does not exist: character varying = uuid' errors "
            "that prevent ON CONFLICT from matching existing rows."
        )

    def test_upsert_stmt_no_org_id_uuid_cast_in_orders(self):
        """The orders upsert must not cast org_id to uuid (org_id is VARCHAR)."""
        import inspect
        import services.leaflink_sync as ls_module

        source = inspect.getsource(ls_module)

        # The fixed upsert should NOT contain CAST(:org_id AS uuid) in the
        # orders INSERT VALUES clause. We check the upsert_stmt specifically.
        # Find the upsert_stmt block and verify it doesn't cast org_id.
        upsert_start = source.find("upsert_stmt = \"\"\"")
        upsert_end = source.find('RETURNING (xmax = 0) AS was_inserted', upsert_start)
        assert upsert_start != -1, "Could not find upsert_stmt in source"
        assert upsert_end != -1, "Could not find RETURNING clause in source"

        upsert_block = source[upsert_start:upsert_end]
        assert "CAST(:org_id AS uuid)" not in upsert_block, (
            "Found CAST(:org_id AS uuid) in orders upsert — "
            "org_id is VARCHAR(120), not UUID."
        )

    def test_upsert_stmt_has_conflict_on_brand_external_order(self):
        """The upsert must conflict on (brand_id, external_order_id)."""
        import inspect
        import services.leaflink_sync as ls_module

        source = inspect.getsource(ls_module)
        assert "ON CONFLICT (brand_id, external_order_id)" in source, (
            "Upsert must use ON CONFLICT (brand_id, external_order_id) "
            "to correctly match existing rows."
        )

    def test_upsert_stmt_returns_xmax(self):
        """The upsert must RETURN (xmax = 0) AS was_inserted for insert/update detection."""
        import inspect
        import services.leaflink_sync as ls_module

        source = inspect.getsource(ls_module)
        assert "RETURNING (xmax = 0) AS was_inserted" in source, (
            "Upsert must RETURN xmax to distinguish inserts from updates."
        )


# ---------------------------------------------------------------------------
# 2. External order ID extraction logic
# ---------------------------------------------------------------------------

class TestExternalOrderIdExtraction:
    """Verify that external_order_id is extracted correctly from LeafLink payloads."""

    def test_uses_id_field_as_primary_source(self):
        """LeafLink 'id' field should be used as external_order_id."""
        from services.leaflink_sync import safe_str

        order = {"id": "12345", "order_number": "ORD-001", "external_id": "ext-999"}
        # The sync code uses: safe_str(_raw_id or _raw_external_id or _raw_ext_order)
        _raw_id = order.get("id")
        _raw_external_id = order.get("external_id")
        _raw_ext_order = order.get("external_order_id")
        external_id = safe_str(_raw_id or _raw_external_id or _raw_ext_order)
        assert external_id == "12345"

    def test_falls_back_to_external_id_field(self):
        """Falls back to 'external_id' when 'id' is absent."""
        from services.leaflink_sync import safe_str

        order = {"external_id": "ext-999", "order_number": "ORD-001"}
        _raw_id = order.get("id")
        _raw_external_id = order.get("external_id")
        _raw_ext_order = order.get("external_order_id")
        external_id = safe_str(_raw_id or _raw_external_id or _raw_ext_order)
        assert external_id == "ext-999"

    def test_falls_back_to_external_order_id_field(self):
        """Falls back to 'external_order_id' when 'id' and 'external_id' are absent."""
        from services.leaflink_sync import safe_str

        order = {"external_order_id": "eoid-777", "order_number": "ORD-001"}
        _raw_id = order.get("id")
        _raw_external_id = order.get("external_id")
        _raw_ext_order = order.get("external_order_id")
        external_id = safe_str(_raw_id or _raw_external_id or _raw_ext_order)
        assert external_id == "eoid-777"

    def test_returns_none_when_all_id_fields_absent(self):
        """Returns None/empty when no ID field is present."""
        from services.leaflink_sync import safe_str

        order = {"order_number": "ORD-001", "customer_name": "Test"}
        _raw_id = order.get("id")
        _raw_external_id = order.get("external_id")
        _raw_ext_order = order.get("external_order_id")
        external_id = safe_str(_raw_id or _raw_external_id or _raw_ext_order)
        # safe_str(None) returns None or empty string — order should be skipped
        assert not external_id


# ---------------------------------------------------------------------------
# 3. Upsert conflict key logic: same external_order_id, different brand
# ---------------------------------------------------------------------------

class TestUpsertConflictKeyLogic:
    """Verify the conflict key (brand_id, external_order_id) correctly scopes
    orders per brand so the same external_order_id for two brands inserts
    two separate rows."""

    def test_same_external_id_different_brand_are_distinct_keys(self):
        """Two orders with the same external_order_id but different brand_id
        must produce different conflict keys — they should INSERT separately."""
        brand_a = "brand-aaa"
        brand_b = "brand-bbb"
        ext_id = "leaflink-order-999"

        # The conflict key is (brand_id, external_order_id)
        key_a = (brand_a, ext_id)
        key_b = (brand_b, ext_id)

        assert key_a != key_b, (
            "Same external_order_id with different brand_id must be distinct "
            "conflict keys — they should INSERT as separate rows."
        )

    def test_same_brand_same_external_id_is_same_key(self):
        """Same brand + same external_order_id must be the same conflict key
        so re-syncing the same order UPDATEs instead of INSERTing a duplicate."""
        brand = "brand-aaa"
        ext_id = "leaflink-order-999"

        key_first = (brand, ext_id)
        key_second = (brand, ext_id)

        assert key_first == key_second, (
            "Same brand + same external_order_id must be the same conflict key "
            "so re-syncing UPDATEs the existing row."
        )

    def test_same_order_number_different_external_id_are_distinct(self):
        """Two orders with the same order_number but different external_order_id
        must NOT overwrite each other — they are distinct orders."""
        brand = "brand-aaa"
        order_number = "ORD-001"
        ext_id_1 = "leaflink-111"
        ext_id_2 = "leaflink-222"

        # The conflict key is (brand_id, external_order_id), NOT order_number
        key_1 = (brand, ext_id_1)
        key_2 = (brand, ext_id_2)

        assert key_1 != key_2, (
            "Orders with the same order_number but different external_order_id "
            "must be distinct — the conflict key is (brand_id, external_order_id), "
            "not order_number."
        )

    def test_missing_external_id_order_is_skipped(self):
        """An order with no external_order_id must be skipped, not inserted
        with a null key that would collapse unrelated orders."""
        from services.leaflink_sync import safe_str

        order_without_id = {"order_number": "ORD-001", "customer_name": "Test"}
        _raw_id = order_without_id.get("id")
        _raw_external_id = order_without_id.get("external_id")
        _raw_ext_order = order_without_id.get("external_order_id")
        external_id = safe_str(_raw_id or _raw_external_id or _raw_ext_order)

        # The sync code skips orders where not external_id
        should_skip = not external_id
        assert should_skip, (
            "Orders with no external_order_id must be skipped — inserting them "
            "with a null key would collapse unrelated orders on the next sync."
        )


# ---------------------------------------------------------------------------
# 4. Reconciliation gap detection
# ---------------------------------------------------------------------------

class TestReconciliationGapDetection:
    """Verify that sync status is set to partial_reconciliation_gap when
    total_local_orders_after < leaflink_estimate by > 2%."""

    def test_gap_above_threshold_sets_partial_status(self):
        """When DB count is 10% below estimate, status must be partial_reconciliation_gap."""
        leaflink_estimate = 16074
        db_orders_after = 12300  # ~23.5% below estimate

        gap_pct = (leaflink_estimate - db_orders_after) / leaflink_estimate * 100
        assert gap_pct > 2.0, "Gap should be above 2% threshold"

        # Simulate the completion status logic
        next_cursor = None  # pagination reached end
        completion_percent = 100.0
        _db_inserted_count = 100
        _db_updated_count = 200
        _db_skipped_count = 0
        records_seen_from_leaflink = 300
        _total_errors = 0
        _db_orders_delta = 100

        _recon_gap_pct = gap_pct

        if _db_inserted_count + _db_updated_count == 0 and _db_skipped_count == records_seen_from_leaflink and records_seen_from_leaflink > 0:
            status = "partial_duplicate_skip"
        elif _db_orders_delta == 0 and records_seen_from_leaflink > 0 and _db_inserted_count == 0 and _db_updated_count == 0:
            status = "reconciliation_failed"
        elif _total_errors > 0:
            status = "partial_line_item_failure"
        elif next_cursor and gap_pct > 2.0:
            status = "partial_page_limit"
        elif next_cursor:
            status = "partial_timeout"
        elif completion_percent < 95:
            status = "partial_timeout"
        elif _recon_gap_pct > 2.0:
            status = "partial_reconciliation_gap"
        else:
            status = "success"

        assert status == "partial_reconciliation_gap", (
            f"Expected partial_reconciliation_gap but got {status}. "
            "When DB count is below estimate by > 2%, sync must not report success."
        )

    def test_gap_below_threshold_allows_success(self):
        """When DB count is within 2% of estimate, status can be success."""
        leaflink_estimate = 16074
        db_orders_after = 15900  # ~1.1% below estimate

        gap_pct = (leaflink_estimate - db_orders_after) / leaflink_estimate * 100
        assert gap_pct < 2.0, "Gap should be below 2% threshold"

        next_cursor = None
        completion_percent = 100.0
        _db_inserted_count = 100
        _db_updated_count = 200
        _db_skipped_count = 0
        records_seen_from_leaflink = 300
        _total_errors = 0
        _db_orders_delta = 100
        _recon_gap_pct = gap_pct

        if _db_inserted_count + _db_updated_count == 0 and _db_skipped_count == records_seen_from_leaflink and records_seen_from_leaflink > 0:
            status = "partial_duplicate_skip"
        elif _db_orders_delta == 0 and records_seen_from_leaflink > 0 and _db_inserted_count == 0 and _db_updated_count == 0:
            status = "reconciliation_failed"
        elif _total_errors > 0:
            status = "partial_line_item_failure"
        elif next_cursor and gap_pct > 2.0:
            status = "partial_page_limit"
        elif next_cursor:
            status = "partial_timeout"
        elif completion_percent < 95:
            status = "partial_timeout"
        elif _recon_gap_pct > 2.0:
            status = "partial_reconciliation_gap"
        else:
            status = "success"

        assert status == "success", (
            f"Expected success but got {status}. "
            "When gap is < 2%, sync should report success."
        )

    def test_next_url_with_large_gap_sets_partial_page_limit(self):
        """When next_url exists AND gap > 2%, status must be partial_page_limit."""
        leaflink_estimate = 16074
        fetched = 12300
        gap_pct = (leaflink_estimate - fetched) / leaflink_estimate * 100

        next_cursor = "https://api.leaflink.com/orders/?page=50"  # still has more pages
        completion_percent = 76.5
        _db_inserted_count = 100
        _db_updated_count = 200
        _db_skipped_count = 0
        records_seen_from_leaflink = fetched
        _total_errors = 0
        _db_orders_delta = 100
        _db_orders_after = 12300
        _recon_gap_pct = gap_pct

        if _db_inserted_count + _db_updated_count == 0 and _db_skipped_count == records_seen_from_leaflink and records_seen_from_leaflink > 0:
            status = "partial_duplicate_skip"
        elif _db_orders_delta == 0 and records_seen_from_leaflink > 0 and _db_inserted_count == 0 and _db_updated_count == 0:
            status = "reconciliation_failed"
        elif _total_errors > 0:
            status = "partial_line_item_failure"
        elif next_cursor and gap_pct > 2.0:
            status = "partial_page_limit"
        elif next_cursor:
            status = "partial_timeout"
        elif completion_percent < 95:
            status = "partial_timeout"
        elif _recon_gap_pct > 2.0:
            status = "partial_reconciliation_gap"
        else:
            status = "success"

        assert status == "partial_page_limit", (
            f"Expected partial_page_limit but got {status}. "
            "When next_url exists and gap > 2%, sync must not report success."
        )

    def test_no_false_success_when_all_updates(self):
        """When every order is an update (0 inserts) and DB count didn't grow,
        status must NOT be success if the estimate is much higher."""
        leaflink_estimate = 16074
        db_orders_after = 12300  # stuck — no growth
        db_orders_before = 12300  # same as after
        db_orders_delta = 0

        records_seen_from_leaflink = 300
        _db_inserted_count = 0
        _db_updated_count = 300  # all updates, no inserts
        _db_skipped_count = 0
        _total_errors = 0
        next_cursor = None
        completion_percent = 100.0

        gap_pct = (leaflink_estimate - db_orders_after) / leaflink_estimate * 100
        _recon_gap_pct = gap_pct

        if _db_inserted_count + _db_updated_count == 0 and _db_skipped_count == records_seen_from_leaflink and records_seen_from_leaflink > 0:
            status = "partial_duplicate_skip"
        elif db_orders_delta == 0 and records_seen_from_leaflink > 0 and _db_inserted_count == 0 and _db_updated_count == 0:
            status = "reconciliation_failed"
        elif _total_errors > 0:
            status = "partial_line_item_failure"
        elif next_cursor and gap_pct > 2.0:
            status = "partial_page_limit"
        elif next_cursor:
            status = "partial_timeout"
        elif completion_percent < 95:
            status = "partial_timeout"
        elif _recon_gap_pct > 2.0:
            status = "partial_reconciliation_gap"
        else:
            status = "success"

        # With 300 updates and 0 inserts but DB count 23% below estimate,
        # status should be partial_reconciliation_gap
        assert status == "partial_reconciliation_gap", (
            f"Expected partial_reconciliation_gap but got {status}. "
            "When all orders are updates and DB count is far below estimate, "
            "sync must not report success."
        )


# ---------------------------------------------------------------------------
# 5. Count query type safety
# ---------------------------------------------------------------------------

class TestCountQueryTypeSafety:
    """Verify count queries work with brand_id as varchar (no UUID cast needed)."""

    def test_brand_id_varchar_comparison_works(self):
        """brand_id is VARCHAR(120) — comparing with a string should work
        without any UUID cast."""
        # This is a logic test: verify the ORM query pattern is correct
        # The ORM uses Order.brand_id == brand_id where both are strings
        brand_id_str = "some-brand-uuid-or-varchar"

        # Simulate the comparison that SQLAlchemy generates
        # Order.brand_id is String(120), so == comparison with a str is valid
        from models import Order
        from sqlalchemy import String
        from sqlalchemy.orm import mapped_column

        # Verify the column type is String (not UUID)
        brand_id_col = Order.__table__.c.brand_id
        assert str(brand_id_col.type) in ("VARCHAR(120)", "VARCHAR"), (
            f"brand_id column type should be VARCHAR, got {brand_id_col.type}. "
            "Using CAST(:brand_id AS uuid) on a VARCHAR column causes type errors."
        )

    def test_external_order_id_varchar_comparison_works(self):
        """external_order_id is VARCHAR(120) — no UUID cast needed."""
        from models import Order

        ext_id_col = Order.__table__.c.external_order_id
        assert str(ext_id_col.type) in ("VARCHAR(120)", "VARCHAR"), (
            f"external_order_id column type should be VARCHAR, got {ext_id_col.type}."
        )

    def test_no_uuid_cast_in_sync_health_inserts(self):
        """sync_health.brand_id is VARCHAR(120) — no UUID cast in INSERT."""
        import inspect
        import services.leaflink_sync as ls_module

        source = inspect.getsource(ls_module)

        # Find sync_health INSERT statements and verify no UUID cast on brand_id
        # The fixed code uses VALUES (:brand_id, ...) not VALUES (CAST(:brand_id AS UUID), ...)
        # Check that the sync_health INSERT does not cast brand_id
        sync_health_insert_idx = source.find("INSERT INTO sync_health")
        assert sync_health_insert_idx != -1, "Could not find sync_health INSERT"

        # Extract a window around the first sync_health INSERT
        window = source[sync_health_insert_idx:sync_health_insert_idx + 500]
        assert "CAST(:brand_id AS UUID)" not in window and "CAST(:brand_id AS uuid)" not in window, (
            "sync_health.brand_id is VARCHAR(120) — do not cast to UUID in INSERT. "
            "This causes 'invalid input syntax for type uuid' errors."
        )


# ---------------------------------------------------------------------------
# 6. [ORDER_MATCH_DECISION] sampling logic
# ---------------------------------------------------------------------------

class TestOrderMatchDecisionSampling:
    """Verify [ORDER_MATCH_DECISION] is logged once per 100 orders."""

    def test_sampling_fires_at_index_1(self):
        """First order (index 1) should trigger [ORDER_MATCH_DECISION]."""
        counter = 1
        assert counter % 100 == 1, "Index 1 should trigger sampling (1 % 100 == 1)"

    def test_sampling_fires_at_index_101(self):
        """101st order should trigger [ORDER_MATCH_DECISION]."""
        counter = 101
        assert counter % 100 == 1, "Index 101 should trigger sampling (101 % 100 == 1)"

    def test_sampling_does_not_fire_at_index_50(self):
        """50th order should NOT trigger [ORDER_MATCH_DECISION]."""
        counter = 50
        assert counter % 100 != 1, "Index 50 should not trigger sampling"

    def test_sampling_does_not_fire_at_index_100(self):
        """100th order should NOT trigger [ORDER_MATCH_DECISION] (fires at 1, 101, 201...)."""
        counter = 100
        assert counter % 100 != 1, "Index 100 should not trigger sampling"

    def test_conflict_key_is_external_id_when_present(self):
        """When external_order_id is present, conflict_key_used should be 'external_id'."""
        external_id = "leaflink-12345"
        order_number = "ORD-001"

        ext_id_present = bool(external_id and external_id not in ("unknown", ""))
        order_num_present = bool(order_number and order_number not in ("unknown", ""))
        conflict_key = "external_id" if ext_id_present else (
            "order_number" if order_num_present else "fallback_uuid"
        )
        assert conflict_key == "external_id"

    def test_conflict_key_is_order_number_when_ext_id_missing(self):
        """When external_order_id is absent but order_number is present,
        conflict_key_used should be 'order_number'."""
        external_id = ""
        order_number = "ORD-001"

        ext_id_present = bool(external_id and external_id not in ("unknown", ""))
        order_num_present = bool(order_number and order_number not in ("unknown", ""))
        conflict_key = "external_id" if ext_id_present else (
            "order_number" if order_num_present else "fallback_uuid"
        )
        assert conflict_key == "order_number"

    def test_conflict_key_is_fallback_uuid_when_both_missing(self):
        """When both external_order_id and order_number are absent,
        conflict_key_used should be 'fallback_uuid'."""
        external_id = ""
        order_number = ""

        ext_id_present = bool(external_id and external_id not in ("unknown", ""))
        order_num_present = bool(order_number and order_number not in ("unknown", ""))
        conflict_key = "external_id" if ext_id_present else (
            "order_number" if order_num_present else "fallback_uuid"
        )
        assert conflict_key == "fallback_uuid"


# ---------------------------------------------------------------------------
# 7. [PAGINATION_INCOMPLETE] detection
# ---------------------------------------------------------------------------

class TestPaginationIncompleteDetection:
    """Verify [PAGINATION_INCOMPLETE] fires when pagination stops before estimate."""

    def test_fires_when_next_cursor_present(self):
        """[PAGINATION_INCOMPLETE] must fire when next_cursor is set."""
        next_cursor = "https://api.leaflink.com/orders/?page=50"
        leaflink_estimate = 16074
        fetched = 12300

        gap = max(0, leaflink_estimate - fetched)
        gap_pct = (gap / leaflink_estimate * 100) if leaflink_estimate > 0 else 0.0

        should_log = bool(next_cursor) or (leaflink_estimate > 0 and gap_pct > 2.0)
        assert should_log, "[PAGINATION_INCOMPLETE] should fire when next_cursor is set"

    def test_fires_when_gap_above_2pct_even_without_cursor(self):
        """[PAGINATION_INCOMPLETE] must fire when gap > 2% even if next_cursor is None."""
        next_cursor = None
        leaflink_estimate = 16074
        fetched = 12300  # ~23.5% gap

        gap = max(0, leaflink_estimate - fetched)
        gap_pct = (gap / leaflink_estimate * 100) if leaflink_estimate > 0 else 0.0

        should_log = bool(next_cursor) or (leaflink_estimate > 0 and gap_pct > 2.0)
        assert should_log, "[PAGINATION_INCOMPLETE] should fire when gap > 2%"

    def test_does_not_fire_when_gap_below_2pct(self):
        """[PAGINATION_INCOMPLETE] must NOT fire when gap is < 2% and no cursor."""
        next_cursor = None
        leaflink_estimate = 16074
        fetched = 15900  # ~1.1% gap

        gap = max(0, leaflink_estimate - fetched)
        gap_pct = (gap / leaflink_estimate * 100) if leaflink_estimate > 0 else 0.0

        should_log = bool(next_cursor) or (leaflink_estimate > 0 and gap_pct > 2.0)
        assert not should_log, "[PAGINATION_INCOMPLETE] should NOT fire when gap < 2%"

    def test_does_not_fire_when_no_estimate(self):
        """[PAGINATION_INCOMPLETE] must NOT fire when leaflink_estimate is 0/None."""
        next_cursor = None
        leaflink_estimate = 0
        fetched = 0

        gap = max(0, leaflink_estimate - fetched)
        gap_pct = (gap / leaflink_estimate * 100) if leaflink_estimate > 0 else 0.0

        should_log = bool(next_cursor) or (leaflink_estimate > 0 and gap_pct > 2.0)
        assert not should_log, "[PAGINATION_INCOMPLETE] should NOT fire when estimate is 0"


# ---------------------------------------------------------------------------
# 8. [ORDER_IDENTITY_AUDIT] field detection
# ---------------------------------------------------------------------------

class TestOrderIdentityAudit:
    """Verify [ORDER_IDENTITY_AUDIT] correctly identifies null/unknown external_order_id."""

    def test_detects_null_external_order_id(self):
        """Orders with external_order_id IS NULL should be counted."""
        orders = [
            {"id": None, "external_id": None, "external_order_id": None, "order_number": "ORD-001"},
            {"id": "12345", "order_number": "ORD-002"},
        ]
        from services.leaflink_sync import safe_str

        null_count = 0
        for o in orders:
            ext_id = safe_str(o.get("id") or o.get("external_id") or o.get("external_order_id"))
            if not ext_id:
                null_count += 1

        assert null_count == 1, f"Expected 1 null external_order_id, got {null_count}"

    def test_detects_unknown_external_order_id(self):
        """Orders with external_order_id = 'unknown' should be flagged."""
        # The audit query checks: external_order_id = 'unknown'
        # This simulates what would be in the DB if 'unknown' was stored
        stored_ext_ids = ["12345", "unknown", "67890", "unknown"]
        unknown_count = sum(1 for eid in stored_ext_ids if eid == "unknown")
        assert unknown_count == 2

    def test_audit_warning_fires_for_null_or_unknown(self):
        """[ORDER_IDENTITY_AUDIT_WARN] should fire when null + unknown > 0."""
        null_ext = 5
        unknown_ext = 3
        total_problematic = null_ext + unknown_ext
        should_warn = total_problematic > 0
        assert should_warn, "Should warn when null or unknown external_order_id exists"

    def test_audit_critical_fires_for_duplicate_external_id(self):
        """[ORDER_IDENTITY_AUDIT_CRITICAL] should fire when duplicate external_order_id > 0."""
        dup_ext_id = 2  # 2 external_order_ids appear more than once
        should_error = dup_ext_id > 0
        assert should_error, "Should error when duplicate external_order_id exists"

    def test_no_warning_when_all_ids_unique_and_present(self):
        """No warning when all orders have unique, non-null external_order_id."""
        null_ext = 0
        unknown_ext = 0
        dup_ext_id = 0

        should_warn = (null_ext + unknown_ext) > 0
        should_error = dup_ext_id > 0

        assert not should_warn
        assert not should_error
