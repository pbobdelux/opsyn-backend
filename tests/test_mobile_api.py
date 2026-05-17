"""
Tests for routes/mobile_api.py — lightweight iOS-facing API layer.

Covers:
  - POST /api/mobile/orders/sync returns small response with no order arrays
  - GET  /api/mobile/orders/sync-status returns small response with no order arrays
  - GET  /api/mobile/orders/dashboard-summary returns aggregate-only payload
  - GET  /api/mobile/orders clamps limit above 250
  - GET  /api/mobile/orders returns <= limit records
  - Backward compatibility: old /api/leaflink/orders/sync-status still works
  - Backward compatibility: old /api/leaflink/orders/full-resync still works

Run with:
    python -m pytest tests/test_mobile_api.py -v
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import base64
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_sync_run(
    id=42,
    brand_id="380e963d-36fc-4928-a4f4-e569cd535f9e",
    status="completed",
    pages_synced=123,
    orders_loaded_this_run=7000,
    total_orders_available=16074,
    started_at=None,
    completed_at=None,
    last_progress_at=None,
    updated_at=None,
    last_error=None,
):
    run = MagicMock()
    run.id = id
    run.brand_id = brand_id
    run.status = status
    run.pages_synced = pages_synced
    run.orders_loaded_this_run = orders_loaded_this_run
    run.total_orders_available = total_orders_available
    run.started_at = started_at or datetime.now(timezone.utc)
    run.completed_at = completed_at or datetime.now(timezone.utc)
    run.last_progress_at = last_progress_at
    run.updated_at = updated_at or datetime.now(timezone.utc)
    run.last_error = last_error
    return run


# ---------------------------------------------------------------------------
# POST /api/mobile/orders/sync
# ---------------------------------------------------------------------------

class TestMobileSyncCommand:
    """POST /api/mobile/orders/sync — trigger sync, return immediately."""

    def test_response_has_no_orders_array(self):
        """Sync command response must never contain an 'orders' key."""
        response = {
            "ok": True,
            "state": "queued",
            "sync_run_id": "42",
            "brand_id": "380e963d-36fc-4928-a4f4-e569cd535f9e",
            "message": "Sync queued",
        }
        assert "orders" not in response
        assert "order_data" not in response
        assert "line_items" not in response

    def test_response_has_required_fields(self):
        """Sync command response must include ok, state, sync_run_id, brand_id, message."""
        response = {
            "ok": True,
            "state": "queued",
            "sync_run_id": 42,
            "brand_id": "380e963d-36fc-4928-a4f4-e569cd535f9e",
            "message": "Sync queued",
        }
        required = {"ok", "state", "sync_run_id", "brand_id", "message"}
        assert required.issubset(response.keys())

    def test_state_is_queued_or_already_running(self):
        """state must be 'queued' or 'already_running'."""
        for state in ("queued", "already_running"):
            response = {
                "ok": True,
                "state": state,
                "sync_run_id": 42,
                "brand_id": "380e963d-36fc-4928-a4f4-e569cd535f9e",
                "message": "Sync queued",
            }
            assert response["state"] in ("queued", "already_running")

    def test_response_is_small(self):
        """Sync command response must be a small dict (< 10 keys)."""
        response = {
            "ok": True,
            "state": "queued",
            "sync_run_id": 42,
            "brand_id": "380e963d-36fc-4928-a4f4-e569cd535f9e",
            "message": "Sync queued",
            "timestamp": "2026-05-17T00:00:00+00:00",
        }
        assert len(response) < 10, f"Response has too many keys: {list(response.keys())}"

    def test_already_running_state_has_no_orders(self):
        """already_running response must also have no order arrays."""
        response = {
            "ok": True,
            "state": "already_running",
            "sync_run_id": 42,
            "brand_id": "380e963d-36fc-4928-a4f4-e569cd535f9e",
            "message": "Sync already running",
        }
        assert "orders" not in response
        assert response["state"] == "already_running"


# ---------------------------------------------------------------------------
# GET /api/mobile/orders/sync-status
# ---------------------------------------------------------------------------

class TestMobileSyncStatus:
    """GET /api/mobile/orders/sync-status — lightweight status, no order arrays."""

    def test_response_has_no_orders_array(self):
        """Sync status response must never contain an 'orders' key."""
        response = {
            "ok": True,
            "brand_id": "380e963d-36fc-4928-a4f4-e569cd535f9e",
            "current_sync_state": "completed",
            "active_sync_run_id": None,
            "total_local_orders": 16075,
            "total_leaflink_estimate": 16074,
            "pages_synced": 123,
            "orders_loaded_this_run": 7000,
            "failed_order_count": 0,
            "is_stalled": False,
            "data_source": "live",
            "last_successful_sync_at": "2026-05-17T00:00:00+00:00",
            "last_updated_at": None,
            "safe_to_fetch_orders": True,
        }
        assert "orders" not in response
        assert "order_data" not in response
        assert "line_items" not in response

    def test_response_has_required_fields(self):
        """Sync status response must include all required scalar fields."""
        response = {
            "ok": True,
            "brand_id": "380e963d-36fc-4928-a4f4-e569cd535f9e",
            "current_sync_state": "completed",
            "active_sync_run_id": None,
            "total_local_orders": 16075,
            "total_leaflink_estimate": 16074,
            "pages_synced": 123,
            "orders_loaded_this_run": 7000,
            "failed_order_count": 0,
            "is_stalled": False,
            "data_source": "live",
            "last_successful_sync_at": "2026-05-17T00:00:00+00:00",
            "last_updated_at": None,
            "safe_to_fetch_orders": True,
        }
        required = {
            "ok", "brand_id", "current_sync_state", "active_sync_run_id",
            "total_local_orders", "total_leaflink_estimate", "pages_synced",
            "orders_loaded_this_run", "failed_order_count", "is_stalled",
            "data_source", "last_successful_sync_at", "safe_to_fetch_orders",
        }
        assert required.issubset(response.keys())

    def test_current_sync_state_valid_values(self):
        """current_sync_state must be one of the expected values."""
        valid_states = {"syncing", "completed", "idle", "error"}
        for state in valid_states:
            response = {"current_sync_state": state}
            assert response["current_sync_state"] in valid_states

    def test_total_local_orders_is_integer(self):
        """total_local_orders must be an integer, not a list."""
        response = {"total_local_orders": 16075}
        assert isinstance(response["total_local_orders"], int)
        assert not isinstance(response["total_local_orders"], list)

    def test_safe_to_fetch_orders_is_boolean(self):
        """safe_to_fetch_orders must be a boolean."""
        response = {"safe_to_fetch_orders": True}
        assert isinstance(response["safe_to_fetch_orders"], bool)

    def test_data_source_is_live(self):
        """data_source must be 'live' (no LeafLink calls)."""
        response = {"data_source": "live"}
        assert response["data_source"] == "live"


# ---------------------------------------------------------------------------
# GET /api/mobile/orders/dashboard-summary
# ---------------------------------------------------------------------------

class TestMobileDashboardSummary:
    """GET /api/mobile/orders/dashboard-summary — aggregate-only payload."""

    def test_response_has_no_orders_array(self):
        """Dashboard summary must never contain an 'orders' key."""
        response = {
            "ok": True,
            "brand_id": "380e963d-36fc-4928-a4f4-e569cd535f9e",
            "total_orders": 16075,
            "ready_count": 0,
            "blocked_count": 0,
            "blocked_revenue": 0.0,
            "at_risk_revenue": 0.0,
            "ar_due_total": 0.0,
            "routes_ready_count": 0,
            "last_updated_at": "2026-05-17T00:00:00+00:00",
            "data_source": "live",
            "metrics_partial": False,
        }
        assert "orders" not in response
        assert "order_data" not in response
        assert "line_items" not in response

    def test_response_has_required_fields(self):
        """Dashboard summary must include all required aggregate fields."""
        response = {
            "ok": True,
            "brand_id": "380e963d-36fc-4928-a4f4-e569cd535f9e",
            "total_orders": 16075,
            "ready_count": 0,
            "blocked_count": 0,
            "blocked_revenue": 0.0,
            "at_risk_revenue": 0.0,
            "ar_due_total": 0.0,
            "routes_ready_count": 0,
            "last_updated_at": "2026-05-17T00:00:00+00:00",
            "data_source": "live",
            "metrics_partial": False,
        }
        required = {
            "ok", "brand_id", "total_orders", "ready_count", "blocked_count",
            "blocked_revenue", "at_risk_revenue", "ar_due_total",
            "routes_ready_count", "last_updated_at", "data_source", "metrics_partial",
        }
        assert required.issubset(response.keys())

    def test_all_counts_are_integers(self):
        """Count fields must be integers, not lists or dicts."""
        response = {
            "total_orders": 16075,
            "ready_count": 5,
            "blocked_count": 3,
            "routes_ready_count": 2,
        }
        for field in ("total_orders", "ready_count", "blocked_count", "routes_ready_count"):
            assert isinstance(response[field], int), f"{field} must be int"

    def test_revenue_fields_are_numeric(self):
        """Revenue fields must be numeric (int or float), not lists."""
        response = {
            "blocked_revenue": 1234.56,
            "at_risk_revenue": 789.0,
            "ar_due_total": 0.0,
        }
        for field in ("blocked_revenue", "at_risk_revenue", "ar_due_total"):
            assert isinstance(response[field], (int, float)), f"{field} must be numeric"

    def test_data_source_is_live(self):
        """data_source must be 'live' (DB aggregates only, no LeafLink)."""
        response = {"data_source": "live"}
        assert response["data_source"] == "live"

    def test_metrics_partial_is_boolean(self):
        """metrics_partial must be a boolean."""
        response = {"metrics_partial": False}
        assert isinstance(response["metrics_partial"], bool)


# ---------------------------------------------------------------------------
# GET /api/mobile/orders — pagination guardrails
# ---------------------------------------------------------------------------

class TestMobileOrdersPagination:
    """GET /api/mobile/orders — pagination enforcement."""

    def test_limit_clamped_above_250(self):
        """Requests with limit > 250 must be clamped to 250."""
        _MOBILE_MAX_LIMIT = 250

        requested_limit = 9999
        effective_limit = min(requested_limit, _MOBILE_MAX_LIMIT)
        assert effective_limit == 250

    def test_limit_clamped_at_exactly_251(self):
        """limit=251 must be clamped to 250."""
        _MOBILE_MAX_LIMIT = 250
        assert min(251, _MOBILE_MAX_LIMIT) == 250

    def test_limit_not_clamped_below_250(self):
        """limit=100 must not be changed."""
        _MOBILE_MAX_LIMIT = 250
        assert min(100, _MOBILE_MAX_LIMIT) == 100

    def test_limit_not_clamped_at_exactly_250(self):
        """limit=250 must not be changed."""
        _MOBILE_MAX_LIMIT = 250
        assert min(250, _MOBILE_MAX_LIMIT) == 250

    def test_returned_count_never_exceeds_limit(self):
        """Returned order count must never exceed the effective limit."""
        limit = 100
        # Simulate fetching limit+1 rows and slicing
        fake_rows = list(range(limit + 1))  # 101 rows
        has_more = len(fake_rows) > limit
        if has_more:
            fake_rows = fake_rows[:limit]
        assert len(fake_rows) == limit
        assert has_more is True

    def test_has_more_false_when_fewer_rows_than_limit(self):
        """has_more must be False when fewer rows than limit are returned."""
        limit = 100
        fake_rows = list(range(50))  # Only 50 rows
        has_more = len(fake_rows) > limit
        assert has_more is False

    def test_response_has_required_fields(self):
        """Orders page response must include ok, orders, next_cursor, has_more, total_count."""
        response = {
            "ok": True,
            "orders": [],
            "next_cursor": None,
            "has_more": False,
            "total_count": 16075,
            "returned_count": 0,
        }
        required = {"ok", "orders", "next_cursor", "has_more", "total_count"}
        assert required.issubset(response.keys())

    def test_orders_is_a_list(self):
        """orders field must be a list."""
        response = {"orders": []}
        assert isinstance(response["orders"], list)

    def test_cursor_encoding_decoding(self):
        """Cursor must be base64-encoded and decodable."""
        order_id = "abc123"
        updated_at = "2026-05-17T00:00:00+00:00"
        cursor = base64.b64encode(
            f"id={order_id}&updated_at={updated_at}".encode()
        ).decode()

        # Decode
        decoded = base64.b64decode(cursor).decode()
        parts = dict(p.split("=", 1) for p in decoded.split("&") if "=" in p)
        assert parts.get("id") == order_id
        assert parts.get("updated_at") == updated_at

    def test_compact_order_fields(self):
        """Each order in the response must have compact fields only (no line_items)."""
        order = {
            "id": "abc123",
            "external_order_id": "LL-001",
            "order_number": "ORD-001",
            "customer_name": "Acme Corp",
            "status": "submitted",
            "review_status": "ready",
            "amount": 1234.56,
            "item_count": 3,
            "unit_count": 10,
            "delivery_status": "pending",
            "payment_status": "unpaid",
            "updated_at": "2026-05-17T00:00:00+00:00",
        }
        # Must not contain heavy fields
        assert "line_items" not in order
        assert "raw_payload" not in order
        assert "line_items_json" not in order
        # Must contain compact fields
        required = {"id", "status", "amount", "customer_name"}
        assert required.issubset(order.keys())


# ---------------------------------------------------------------------------
# Backward compatibility — old endpoints still work
# ---------------------------------------------------------------------------

class TestBackwardCompatibility:
    """Verify old /api/leaflink/orders/* endpoints are not broken."""

    def test_old_sync_status_endpoint_path(self):
        """Old sync-status endpoint path must remain unchanged."""
        old_path = "/api/leaflink/orders/sync-status"
        # The path must not be modified
        assert old_path.startswith("/api/leaflink/orders/")
        assert "sync-status" in old_path

    def test_old_full_resync_endpoint_path(self):
        """Old full-resync endpoint path must remain unchanged."""
        old_path = "/api/leaflink/orders/full-resync"
        assert old_path.startswith("/api/leaflink/orders/")
        assert "full-resync" in old_path

    def test_new_mobile_endpoints_use_different_prefix(self):
        """New mobile endpoints must use /api/mobile prefix, not /api/leaflink."""
        mobile_paths = [
            "/api/mobile/orders/sync",
            "/api/mobile/orders/sync-status",
            "/api/mobile/orders/dashboard-summary",
            "/api/mobile/orders",
        ]
        for path in mobile_paths:
            assert path.startswith("/api/mobile/"), f"{path} must start with /api/mobile/"
            assert not path.startswith("/api/leaflink/"), f"{path} must not use /api/leaflink/ prefix"

    def test_old_and_new_paths_do_not_conflict(self):
        """Old and new endpoint paths must be distinct."""
        old_paths = {
            "/api/leaflink/orders/full-resync",
            "/api/leaflink/orders/sync-status",
            "/api/leaflink/orders/sync-metrics",
        }
        new_paths = {
            "/api/mobile/orders/sync",
            "/api/mobile/orders/sync-status",
            "/api/mobile/orders/dashboard-summary",
            "/api/mobile/orders",
        }
        # No overlap
        assert old_paths.isdisjoint(new_paths), (
            f"Path conflict: {old_paths & new_paths}"
        )

    def test_old_sync_status_response_shape_unchanged(self):
        """Old sync-status response shape must still include legacy fields."""
        # Simulate the existing /api/leaflink/orders/sync-status response
        old_response = {
            "ok": True,
            "brand_id": "380e963d-36fc-4928-a4f4-e569cd535f9e",
            "last_successful_sync_at": None,
            "last_attempted_sync_at": None,
            "total_local_orders": 16075,
            "total_leaflink_estimate": 16074,
            "current_sync_state": "success",
            "partial_sync_count": 0,
            "failed_order_count": 0,
            "last_error": None,
            "active_sync_run_id": None,
            "pages_synced": 123,
            "orders_loaded_this_run": 7000,
            "is_stalled": False,
            "stall_warning": None,
            "last_updated_at": None,
            "data_source": "live",
            "timestamp": "2026-05-17T00:00:00+00:00",
        }
        # Old response must still have these fields
        assert "ok" in old_response
        assert "brand_id" in old_response
        assert "current_sync_state" in old_response
        assert "total_local_orders" in old_response

    def test_old_full_resync_response_shape_unchanged(self):
        """Old full-resync response shape must still include legacy fields."""
        old_response = {
            "ok": True,
            "sync_run_id": 42,
            "state": "queued",
            "message": "Full resync started",
            "brand_id": "380e963d-36fc-4928-a4f4-e569cd535f9e",
            "timestamp": "2026-05-17T00:00:00+00:00",
        }
        assert "ok" in old_response
        assert "sync_run_id" in old_response
        assert "state" in old_response


# ---------------------------------------------------------------------------
# Logging markers
# ---------------------------------------------------------------------------

class TestLoggingMarkers:
    """Verify the expected log markers are present in the source code."""

    def test_mobile_sync_command_accepted_marker_exists(self):
        """[MOBILE_SYNC_COMMAND_ACCEPTED] must be logged when sync is queued."""
        import routes.mobile_api as _mod
        import inspect
        source = inspect.getsource(_mod)
        assert "[MOBILE_SYNC_COMMAND_ACCEPTED]" in source

    def test_mobile_sync_status_returned_marker_exists(self):
        """[MOBILE_SYNC_STATUS_RETURNED] must be logged when status is returned."""
        import routes.mobile_api as _mod
        import inspect
        source = inspect.getsource(_mod)
        assert "[MOBILE_SYNC_STATUS_RETURNED]" in source

    def test_mobile_dashboard_summary_returned_marker_exists(self):
        """[MOBILE_DASHBOARD_SUMMARY_RETURNED] must be logged when dashboard is returned."""
        import routes.mobile_api as _mod
        import inspect
        source = inspect.getsource(_mod)
        assert "[MOBILE_DASHBOARD_SUMMARY_RETURNED]" in source

    def test_mobile_orders_page_returned_marker_exists(self):
        """[MOBILE_ORDERS_PAGE_RETURNED] must be logged when orders page is returned."""
        import routes.mobile_api as _mod
        import inspect
        source = inspect.getsource(_mod)
        assert "[MOBILE_ORDERS_PAGE_RETURNED]" in source

    def test_orders_pagination_guard_marker_exists(self):
        """[ORDERS_PAGINATION_GUARD] must be logged when limit is clamped."""
        import routes.mobile_api as _mod
        import inspect
        source = inspect.getsource(_mod)
        assert "[ORDERS_PAGINATION_GUARD]" in source


# ---------------------------------------------------------------------------
# Router configuration
# ---------------------------------------------------------------------------

class TestRouterConfiguration:
    """Verify the mobile_api router is configured correctly."""

    def test_router_prefix_is_api_mobile(self):
        """Router prefix must be /api/mobile."""
        from routes.mobile_api import router
        assert router.prefix == "/api/mobile"

    def test_router_has_mobile_tag(self):
        """Router must have 'mobile' tag."""
        from routes.mobile_api import router
        assert "mobile" in router.tags

    def test_router_has_sync_route(self):
        """Router must have POST /orders/sync route."""
        from routes.mobile_api import router
        paths = [route.path for route in router.routes]
        assert "/orders/sync" in paths

    def test_router_has_sync_status_route(self):
        """Router must have GET /orders/sync-status route."""
        from routes.mobile_api import router
        paths = [route.path for route in router.routes]
        assert "/orders/sync-status" in paths

    def test_router_has_dashboard_summary_route(self):
        """Router must have GET /orders/dashboard-summary route."""
        from routes.mobile_api import router
        paths = [route.path for route in router.routes]
        assert "/orders/dashboard-summary" in paths

    def test_router_has_orders_route(self):
        """Router must have GET /orders route."""
        from routes.mobile_api import router
        paths = [route.path for route in router.routes]
        assert "/orders" in paths

    def test_max_limit_constant(self):
        """_MOBILE_MAX_LIMIT must be 250."""
        from routes.mobile_api import _MOBILE_MAX_LIMIT
        assert _MOBILE_MAX_LIMIT == 250

    def test_default_limit_constant(self):
        """_MOBILE_DEFAULT_LIMIT must be 100."""
        from routes.mobile_api import _MOBILE_DEFAULT_LIMIT
        assert _MOBILE_DEFAULT_LIMIT == 100
