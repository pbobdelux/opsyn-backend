"""
Mobile-specific API endpoints for the iOS app.

All endpoints live under /api/mobile/ and are designed to be lightweight,
safe, and pagination-enforced. They wrap existing sync/order logic without
modifying it.

Endpoints:
  POST /api/mobile/orders/sync?brand_id=...
    — Enqueue a sync for a brand. Returns immediately. Never returns orders.

  GET  /api/mobile/orders/sync-status?brand_id=...
    — Lightweight sync status. Returns only metrics, never order arrays.

  GET  /api/mobile/orders/dashboard-summary?brand_id=...
    — Aggregate dashboard metrics from DB. No LeafLink calls, no order arrays.

  GET  /api/mobile/orders?brand_id=...&limit=100&cursor=...
    — Paginated orders. Max 500 per page. Never returns all orders.

Logging prefixes:
  [MOBILE_SYNC_COMMAND_ACCEPTED]
  [MOBILE_SYNC_STATUS_RETURNED]
  [MOBILE_DASHBOARD_SUMMARY_RETURNED]
  [MOBILE_ORDERS_QUERY]
  [MOBILE_ORDERS_DB_MAX_TIMESTAMPS]
  [MOBILE_ORDER_REAL_ROW]
  [MOBILE_ORDERS_QUERY_RESULT]
  [ORDERS_PAGINATION_GUARD]
"""

import base64
import logging
import time
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import BrandAPICredential, Order, SyncRequest, SyncRun
from models.sync_health import SyncHealth
from utils.json_utils import make_json_safe

logger = logging.getLogger("mobile_api")

router = APIRouter(prefix="/api/mobile", tags=["mobile"])

# ---------------------------------------------------------------------------
# Pagination constants
# ---------------------------------------------------------------------------
MOBILE_DEFAULT_LIMIT = 100
MOBILE_MAX_LIMIT = 500


# ---------------------------------------------------------------------------
# Endpoint A: POST /api/mobile/orders/sync
# ---------------------------------------------------------------------------

@router.post("/orders/sync")
async def mobile_sync_command(
    brand_id: str = Query(..., description="Brand ID (UUID) to trigger sync for"),
    db: AsyncSession = Depends(get_db),
):
    """Enqueue a LeafLink sync for a brand.

    Returns immediately — does NOT run sync inline, does NOT return orders.
    Reuses the same SyncRequest enqueue logic as /orders/sync/leaflink/trigger.

    Response states:
      queued         — a new sync request was enqueued
      already_running — an active sync run already exists for this brand
    """
    logger.info(
        "[MOBILE_SYNC_COMMAND_ACCEPTED] brand_id=%s",
        brand_id,
    )

    # Check for an already-running sync run
    try:
        active_run_result = await db.execute(
            select(SyncRun).where(
                SyncRun.brand_id == brand_id,
                SyncRun.status.in_(["queued", "syncing"]),
            ).order_by(SyncRun.started_at.desc()).limit(1)
        )
        active_run = active_run_result.scalar_one_or_none()
    except Exception as exc:
        logger.error(
            "[MOBILE_SYNC_COMMAND_ACCEPTED] active_run_check_failed brand_id=%s error=%s",
            brand_id,
            str(exc)[:200],
        )
        try:
            await db.rollback()
        except Exception:
            pass
        active_run = None

    if active_run is not None:
        logger.info(
            "[MOBILE_SYNC_COMMAND_ACCEPTED] state=already_running brand_id=%s sync_run_id=%s",
            brand_id,
            active_run.id,
        )
        return {
            "ok": True,
            "state": "already_running",
            "sync_run_id": str(active_run.id),
            "brand_id": brand_id,
            "message": "Sync already in progress",
        }

    # Verify credentials exist before enqueuing (mirrors trigger_leaflink_sync logic)
    try:
        cred_result = await db.execute(
            select(BrandAPICredential).where(
                BrandAPICredential.brand_id == brand_id,
                BrandAPICredential.integration_name == "leaflink",
                BrandAPICredential.is_active == True,
            )
        )
        cred = cred_result.scalar_one_or_none()
    except Exception as exc:
        logger.error(
            "[MOBILE_SYNC_COMMAND_ACCEPTED] credential_lookup_failed brand_id=%s error=%s",
            brand_id,
            str(exc)[:200],
        )
        try:
            await db.rollback()
        except Exception:
            pass
        return {
            "ok": False,
            "error": "Credential lookup failed",
            "brand_id": brand_id,
        }

    if cred is None:
        logger.warning(
            "[MOBILE_SYNC_COMMAND_ACCEPTED] no_credential brand_id=%s",
            brand_id,
        )
        return {
            "ok": False,
            "error": f"No active LeafLink credential found for brand_id={brand_id}",
            "brand_id": brand_id,
        }

    # Enqueue a SyncRequest (same as /orders/sync/leaflink/trigger)
    try:
        sync_req = SyncRequest(
            brand_id=brand_id,
            status="pending",
            start_page=1,
        )
        db.add(sync_req)
        await db.commit()
    except Exception as exc:
        logger.error(
            "[MOBILE_SYNC_COMMAND_ACCEPTED] enqueue_failed brand_id=%s error=%s",
            brand_id,
            str(exc)[:200],
        )
        try:
            await db.rollback()
        except Exception:
            pass
        return {
            "ok": False,
            "error": "Failed to enqueue sync request",
            "brand_id": brand_id,
        }

    logger.info(
        "[MOBILE_SYNC_COMMAND_ACCEPTED] state=queued brand_id=%s sync_run_id=%s",
        brand_id,
        sync_req.id,
    )

    return {
        "ok": True,
        "state": "queued",
        "sync_run_id": str(sync_req.id),
        "brand_id": brand_id,
        "message": "Sync queued",
    }


# ---------------------------------------------------------------------------
# Endpoint B: GET /api/mobile/orders/sync-status
# ---------------------------------------------------------------------------

@router.get("/orders/sync-status")
async def mobile_sync_status(
    brand_id: str = Query(..., description="Brand ID (UUID) to query sync status for"),
    db: AsyncSession = Depends(get_db),
):
    """Lightweight sync status for the iOS app.

    Returns ONLY status metrics — never returns order arrays.
    Uses efficient indexed queries with no expensive joins.
    Response time: < 1 second.
    """
    _t0 = time.monotonic()
    logger.info(
        "[MOBILE_SYNC_STATUS_RETURNED] brand_id=%s",
        brand_id,
    )

    now = datetime.now(timezone.utc)

    # ------------------------------------------------------------------
    # 1. Fetch sync_health + active/completed run + order count in one
    #    efficient batch using raw SQL (no ORM joins, no N+1 queries).
    # ------------------------------------------------------------------
    health_row = None
    active_run_row = None
    completed_run_row = None
    total_local_orders = 0

    try:
        health_res = await db.execute(
            text(
                "SELECT"
                " consecutive_failures,"
                " last_successful_sync_at,"
                " last_attempted_sync_at,"
                " last_error,"
                " updated_at"
                " FROM sync_health"
                " WHERE brand_id = CAST(:brand_id AS UUID)"
                " LIMIT 1"
            ),
            {"brand_id": brand_id},
        )
        health_row = health_res.fetchone()
    except Exception as exc:
        logger.error(
            "[MOBILE_SYNC_STATUS_RETURNED] health_query_failed brand_id=%s error=%s",
            brand_id,
            str(exc)[:200],
        )
        try:
            await db.rollback()
        except Exception:
            pass

    try:
        active_res = await db.execute(
            text(
                "SELECT id, status, started_at, pages_synced,"
                " orders_loaded_this_run, total_orders_available"
                " FROM sync_runs"
                " WHERE brand_id = :brand_id"
                " AND status IN ('queued', 'syncing')"
                " ORDER BY started_at DESC"
                " LIMIT 1"
            ),
            {"brand_id": brand_id},
        )
        active_run_row = active_res.fetchone()
    except Exception as exc:
        logger.error(
            "[MOBILE_SYNC_STATUS_RETURNED] active_run_query_failed brand_id=%s error=%s",
            brand_id,
            str(exc)[:200],
        )
        try:
            await db.rollback()
        except Exception:
            pass

    try:
        completed_res = await db.execute(
            text(
                "SELECT id, completed_at, pages_synced,"
                " orders_loaded_this_run, total_orders_available"
                " FROM sync_runs"
                " WHERE brand_id = :brand_id"
                " AND status = 'completed'"
                " ORDER BY completed_at DESC"
                " LIMIT 1"
            ),
            {"brand_id": brand_id},
        )
        completed_run_row = completed_res.fetchone()
    except Exception as exc:
        logger.error(
            "[MOBILE_SYNC_STATUS_RETURNED] completed_run_query_failed brand_id=%s error=%s",
            brand_id,
            str(exc)[:200],
        )
        try:
            await db.rollback()
        except Exception:
            pass

    try:
        count_res = await db.execute(
            text(
                "SELECT COUNT(*) FROM orders"
                " WHERE brand_id = :brand_id"
            ),
            {"brand_id": brand_id},
        )
        total_local_orders = count_res.scalar() or 0
    except Exception as exc:
        logger.error(
            "[MOBILE_SYNC_STATUS_RETURNED] order_count_failed brand_id=%s error=%s",
            brand_id,
            str(exc)[:200],
        )
        try:
            await db.rollback()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # 2. Derive current_sync_state
    # ------------------------------------------------------------------
    if active_run_row is not None:
        current_sync_state = "syncing"
        active_sync_run_id = str(active_run_row[0])
        pages_synced = active_run_row[3] or 0
        orders_loaded_this_run = active_run_row[4] or 0
        total_leaflink_estimate = active_run_row[5]
        # Stall detection: queued/syncing run started > 90s ago
        started_at = active_run_row[2]
        is_stalled = False
        if started_at is not None:
            try:
                age_s = (now - started_at).total_seconds()
                is_stalled = age_s > 90
            except Exception:
                is_stalled = False
    elif completed_run_row is not None:
        current_sync_state = "completed"
        active_sync_run_id = None
        is_stalled = False
        pages_synced = completed_run_row[2] or 0
        orders_loaded_this_run = completed_run_row[3] or 0
        total_leaflink_estimate = completed_run_row[4]
    else:
        consecutive_failures = (health_row[0] or 0) if health_row else 0
        current_sync_state = "failed" if consecutive_failures > 0 else "idle"
        active_sync_run_id = None
        is_stalled = False
        pages_synced = 0
        orders_loaded_this_run = 0
        total_leaflink_estimate = None

    # ------------------------------------------------------------------
    # 3. Derive safe_to_fetch_orders
    # ------------------------------------------------------------------
    safe_to_fetch_orders = (
        current_sync_state in ("idle", "completed")
        and not is_stalled
    )

    # ------------------------------------------------------------------
    # 4. Build response fields from available data
    # ------------------------------------------------------------------
    failed_order_count = (health_row[0] or 0) if health_row else 0

    last_successful_sync_at = None
    if health_row and health_row[1]:
        _ts = health_row[1]
        last_successful_sync_at = _ts.isoformat() if hasattr(_ts, "isoformat") else str(_ts)
    elif completed_run_row and completed_run_row[1]:
        _ts = completed_run_row[1]
        last_successful_sync_at = _ts.isoformat() if hasattr(_ts, "isoformat") else str(_ts)

    last_updated_at = now.isoformat()
    if health_row and health_row[4]:
        _ts = health_row[4]
        last_updated_at = _ts.isoformat() if hasattr(_ts, "isoformat") else str(_ts)

    elapsed_ms = round((time.monotonic() - _t0) * 1000, 2)
    logger.info(
        "[MOBILE_SYNC_STATUS_RETURNED] brand_id=%s state=%s active_run=%s"
        " total_local=%s pages_synced=%s is_stalled=%s elapsed_ms=%.2f",
        brand_id,
        current_sync_state,
        active_sync_run_id,
        total_local_orders,
        pages_synced,
        is_stalled,
        elapsed_ms,
    )

    # CRITICAL: This response must NEVER include order arrays.
    return {
        "ok": True,
        "brand_id": brand_id,
        "current_sync_state": current_sync_state,
        "active_sync_run_id": active_sync_run_id,
        "total_local_orders": total_local_orders,
        "total_leaflink_estimate": total_leaflink_estimate,
        "pages_synced": pages_synced,
        "orders_loaded_this_run": orders_loaded_this_run,
        "failed_order_count": failed_order_count,
        "is_stalled": is_stalled,
        "data_source": "live",
        "last_successful_sync_at": last_successful_sync_at,
        "last_updated_at": last_updated_at,
        "safe_to_fetch_orders": safe_to_fetch_orders,
    }


# ---------------------------------------------------------------------------
# Endpoint C: GET /api/mobile/orders/dashboard-summary
# ---------------------------------------------------------------------------

@router.get("/orders/dashboard-summary")
async def mobile_dashboard_summary(
    brand_id: str = Query(..., description="Brand ID (UUID) to fetch dashboard metrics for"),
    db: AsyncSession = Depends(get_db),
):
    """Aggregate dashboard metrics for the iOS app.

    Uses a single aggregate SQL query — no raw_payload serialization, no ORM
    joins, no order arrays. Response time: < 2 seconds.

    If some metrics are not reliable (e.g. missing columns), returns 0 with
    metrics_partial=true. Never fakes values.
    """
    _t0 = time.monotonic()
    logger.info(
        "[MOBILE_DASHBOARD_SUMMARY_RETURNED] brand_id=%s",
        brand_id,
    )

    now = datetime.now(timezone.utc)
    metrics_partial = False

    # ------------------------------------------------------------------
    # Single aggregate query — all metrics in one DB round-trip.
    # Uses brand_id plain VARCHAR comparison (no UUID cast on orders)
    # to leverage the idx_orders_brand_updated index.
    # ------------------------------------------------------------------
    total_orders = 0
    ready_count = 0
    blocked_count = 0
    blocked_revenue = 0.0
    at_risk_revenue = 0.0
    ar_due_total = 0.0
    routes_ready_count = 0

    try:
        agg_res = await db.execute(
            text(
                "SELECT"
                "  COUNT(*) AS total_orders,"
                "  COUNT(*) FILTER (WHERE status IN ('approved', 'received', 'open')) AS ready_count,"
                "  COUNT(*) FILTER (WHERE review_status = 'blocked') AS blocked_count,"
                "  COALESCE(SUM(amount) FILTER (WHERE review_status = 'blocked'), 0) AS blocked_revenue,"
                "  COALESCE(SUM(amount) FILTER (WHERE days_overdue > 0), 0) AS at_risk_revenue,"
                "  COALESCE(SUM(balance_due), 0) AS ar_due_total,"
                "  COUNT(*) FILTER (WHERE delivery_status = 'assigned') AS routes_ready_count"
                " FROM orders"
                " WHERE brand_id = :brand_id"
            ),
            {"brand_id": brand_id},
        )
        agg_row = agg_res.fetchone()
        if agg_row:
            total_orders = int(agg_row[0] or 0)
            ready_count = int(agg_row[1] or 0)
            blocked_count = int(agg_row[2] or 0)
            blocked_revenue = float(agg_row[3] or 0)
            at_risk_revenue = float(agg_row[4] or 0)
            ar_due_total = float(agg_row[5] or 0)
            routes_ready_count = int(agg_row[6] or 0)
    except Exception as exc:
        logger.error(
            "[MOBILE_DASHBOARD_SUMMARY_RETURNED] aggregate_query_failed brand_id=%s error=%s",
            brand_id,
            str(exc)[:200],
        )
        metrics_partial = True
        try:
            await db.rollback()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Last updated timestamp from sync_health (single indexed lookup)
    # ------------------------------------------------------------------
    last_updated_at = now.isoformat()
    try:
        health_ts_res = await db.execute(
            text(
                "SELECT updated_at FROM sync_health"
                " WHERE brand_id = CAST(:brand_id AS UUID)"
                " LIMIT 1"
            ),
            {"brand_id": brand_id},
        )
        health_ts_row = health_ts_res.fetchone()
        if health_ts_row and health_ts_row[0]:
            _ts = health_ts_row[0]
            last_updated_at = _ts.isoformat() if hasattr(_ts, "isoformat") else str(_ts)
    except Exception as exc:
        logger.warning(
            "[MOBILE_DASHBOARD_SUMMARY_RETURNED] last_updated_at_failed brand_id=%s error=%s",
            brand_id,
            str(exc)[:200],
        )
        try:
            await db.rollback()
        except Exception:
            pass

    elapsed_ms = round((time.monotonic() - _t0) * 1000, 2)
    logger.info(
        "[MOBILE_DASHBOARD_SUMMARY_RETURNED] brand_id=%s total_orders=%s"
        " ready=%s blocked=%s blocked_revenue=%.2f ar_due=%.2f"
        " routes_ready=%s metrics_partial=%s elapsed_ms=%.2f",
        brand_id,
        total_orders,
        ready_count,
        blocked_count,
        blocked_revenue,
        ar_due_total,
        routes_ready_count,
        metrics_partial,
        elapsed_ms,
    )

    return make_json_safe({
        "ok": True,
        "brand_id": brand_id,
        "total_orders": total_orders,
        "ready_count": ready_count,
        "blocked_count": blocked_count,
        "blocked_revenue": blocked_revenue,
        "at_risk_revenue": at_risk_revenue,
        "ar_due_total": ar_due_total,
        "routes_ready_count": routes_ready_count,
        "last_updated_at": last_updated_at,
        "data_source": "live",
        "metrics_partial": metrics_partial,
    })


# ---------------------------------------------------------------------------
# Endpoint D: GET /api/mobile/orders
# ---------------------------------------------------------------------------

@router.get("/orders")
async def mobile_orders_paginated(
    brand_id: str = Query(..., description="Brand ID (UUID)"),
    limit: int = Query(MOBILE_DEFAULT_LIMIT, ge=1, description="Orders per page (default 100, max 500)"),
    cursor: Optional[str] = Query(None, description="Opaque cursor for cursor-based pagination"),
    fresh: bool = Query(False, description="If true, bypass any upstream caching"),
    include_raw: bool = Query(False, description="If true, include raw_payload in response"),
    db: AsyncSession = Depends(get_db),
):
    """Paginated orders for the iOS app.

    Enforces a hard maximum of 500 orders per page. Never returns all orders.
    Uses cursor-based pagination for efficiency. Supports fresh=true to bypass
    any upstream caching.

    raw_payload is excluded by default (include_raw=true to include it) to
    reduce response size and serialization time.

    Uses raw SQL with plain VARCHAR brand_id comparison (no UUID casts) and sorts
    by COALESCE(external_updated_at, updated_at, created_at) DESC so the newest
    orders always appear first.

    Cursor format: base64("id=<UUID>&sort_ts=<ISO>")
    """
    import json as _json

    _t0 = time.monotonic()

    # ------------------------------------------------------------------
    # Pagination guard — clamp limit to [1, MOBILE_MAX_LIMIT]
    # ------------------------------------------------------------------
    original_limit = limit
    if limit > MOBILE_MAX_LIMIT:
        logger.warning(
            "[ORDERS_PAGINATION_GUARD] limit_clamped brand_id=%s requested=%s clamped_to=%s",
            brand_id,
            original_limit,
            MOBILE_MAX_LIMIT,
        )
        limit = MOBILE_MAX_LIMIT

    logger.info(
        "[MOBILE_ORDERS_QUERY_START] brand_id=%s limit=%s cursor=%s fresh=%s include_raw=%s",
        brand_id,
        limit,
        "present" if cursor else "none",
        fresh,
        include_raw,
    )

    # ------------------------------------------------------------------
    # Decode cursor → (cursor_id, cursor_sort_ts)
    # Cursor format: base64("id=<UUID>&sort_ts=<ISO>")
    # ------------------------------------------------------------------
    cursor_id: Optional[str] = None
    cursor_sort_ts: Optional[str] = None

    if cursor:
        try:
            decoded = base64.b64decode(cursor).decode()
            parts = dict(p.split("=", 1) for p in decoded.split("&") if "=" in p)
            cursor_id = parts.get("id") or None
            cursor_sort_ts = parts.get("sort_ts") or None
        except Exception as cursor_exc:
            logger.error(
                "[MOBILE_ORDERS_QUERY_START] cursor_decode_error brand_id=%s error=%s",
                brand_id,
                cursor_exc,
            )
            return {"ok": False, "error": "Invalid cursor format"}

    # ------------------------------------------------------------------
    # Log max timestamps in DB for this brand (diagnostic)
    # ------------------------------------------------------------------
    try:
        ts_res = await db.execute(
            text(
                "SELECT"
                " MAX(external_updated_at) AS max_ext_upd,"
                " MAX(updated_at) AS max_upd,"
                " MAX(created_at) AS max_cre"
                " FROM orders"
                " WHERE brand_id = :brand_id"
            ),
            {"brand_id": brand_id},
        )
        ts_row = ts_res.fetchone()
        if ts_row:
            logger.info(
                "[MOBILE_ORDERS_DB_MAX_TIMESTAMPS] max_external_updated_at=%s max_updated_at=%s max_created_at=%s",
                ts_row[0],
                ts_row[1],
                ts_row[2],
            )
    except Exception as ts_exc:
        logger.warning(
            "[MOBILE_ORDERS_QUERY_START] max_timestamps_failed brand_id=%s error=%s",
            brand_id,
            str(ts_exc)[:200],
        )
        try:
            await db.rollback()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Build raw SQL query — plain VARCHAR comparison, no UUID casts.
    # Exclude raw_payload by default to reduce response size.
    # Sort by newest COALESCE(external_updated_at, updated_at, created_at) DESC
    # so the most recently updated orders always appear first.
    # ------------------------------------------------------------------
    raw_payload_col = "raw_payload," if include_raw else "NULL AS raw_payload,"

    base_sql = (
        "SELECT"
        " id, org_id, brand_id, external_order_id, order_number, customer_name, status,"
        " total_cents, amount, item_count, unit_count,"
        f" line_items_json, {raw_payload_col} source,"
        " review_status, sync_status, sync_health_status, sync_health_missing_fields,"
        " synced_at, last_synced_at,"
        " created_at, updated_at, external_created_at, external_updated_at"
        " FROM orders"
        " WHERE brand_id = :brand_id"
    )

    params: dict = {"brand_id": brand_id}

    if cursor_id and cursor_sort_ts:
        base_sql += (
            " AND ("
            "  COALESCE(external_updated_at, updated_at, created_at)"
            "    < CAST(:cursor_sort_ts AS TIMESTAMPTZ)"
            "  OR ("
            "    COALESCE(external_updated_at, updated_at, created_at)"
            "      = CAST(:cursor_sort_ts AS TIMESTAMPTZ)"
            "    AND id < :cursor_id"
            "  )"
            " )"
        )
        params["cursor_sort_ts"] = cursor_sort_ts
        params["cursor_id"] = cursor_id

    base_sql += (
        " ORDER BY"
        " COALESCE(external_updated_at, updated_at, created_at) DESC NULLS LAST,"
        " id DESC"
        " LIMIT :fetch_limit"
    )
    params["fetch_limit"] = limit + 1

    _db_t0 = time.monotonic()
    try:
        rows_res = await db.execute(text(base_sql), params)
        raw_rows = rows_res.fetchall()
        col_names = list(rows_res.keys())
        rows = [dict(zip(col_names, row)) for row in raw_rows]
    except Exception as exc:
        logger.error(
            "[MOBILE_ORDERS_QUERY_START] db_query_failed brand_id=%s error=%s",
            brand_id,
            str(exc)[:300],
        )
        try:
            await db.rollback()
        except Exception:
            pass
        return {
            "ok": False,
            "error": "Database query failed",
            "detail": str(exc)[:300],
        }

    _db_elapsed_ms = round((time.monotonic() - _db_t0) * 1000, 2)

    has_more = len(rows) > limit
    if has_more:
        rows = rows[:limit]

    logger.info(
        "[MOBILE_ORDERS_QUERY_DONE] count=%s elapsed_ms=%.2f",
        len(rows),
        _db_elapsed_ms,
    )

    # ------------------------------------------------------------------
    # Total count (cheap indexed COUNT, plain VARCHAR comparison)
    # ------------------------------------------------------------------
    total_count = 0
    try:
        count_res = await db.execute(
            text(
                "SELECT COUNT(*) FROM orders"
                " WHERE brand_id = :brand_id"
            ),
            {"brand_id": brand_id},
        )
        total_count = count_res.scalar() or 0
    except Exception as exc:
        logger.error(
            "[MOBILE_ORDERS_QUERY_START] count_failed brand_id=%s error=%s",
            brand_id,
            str(exc)[:200],
        )
        try:
            await db.rollback()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Field derivation helpers
    # ------------------------------------------------------------------
    def _derive_counts_and_amount(row: dict) -> dict:
        """Derive item_count, unit_count, and amount from JSON payloads if missing."""
        item_count = row.get("item_count") or 0
        unit_count = row.get("unit_count") or 0
        amount = row.get("amount")
        total_cents = row.get("total_cents")

        # Resolve line_items_json — may be a list, dict, or raw JSON string
        line_items = row.get("line_items_json")
        if isinstance(line_items, str):
            try:
                line_items = _json.loads(line_items)
            except Exception:
                line_items = None

        # Resolve raw_payload — may be a dict or raw JSON string (only if included)
        raw_payload = row.get("raw_payload")
        if isinstance(raw_payload, str):
            try:
                raw_payload = _json.loads(raw_payload)
            except Exception:
                raw_payload = None

        # Derive item_count from line_items_json
        if (not item_count) and line_items and isinstance(line_items, list):
            item_count = len(line_items)

        # Derive item_count from raw_payload.line_items
        if (not item_count) and raw_payload and isinstance(raw_payload, dict):
            rp_lines = raw_payload.get("line_items") or raw_payload.get("lines") or []
            if isinstance(rp_lines, list):
                item_count = len(rp_lines)

        # Derive unit_count from line_items_json quantities
        if (not unit_count) and line_items and isinstance(line_items, list):
            total_qty = 0
            for li in line_items:
                if isinstance(li, dict):
                    qty = li.get("quantity") or li.get("qty") or 0
                    try:
                        total_qty += int(qty)
                    except (TypeError, ValueError):
                        pass
            if total_qty:
                unit_count = total_qty

        # Derive unit_count from raw_payload.line_items quantities
        if (not unit_count) and raw_payload and isinstance(raw_payload, dict):
            rp_lines = raw_payload.get("line_items") or raw_payload.get("lines") or []
            if isinstance(rp_lines, list):
                total_qty = 0
                for li in rp_lines:
                    if isinstance(li, dict):
                        qty = li.get("quantity") or li.get("qty") or 0
                        try:
                            total_qty += int(qty)
                        except (TypeError, ValueError):
                            pass
                if total_qty:
                    unit_count = total_qty

        # Derive amount from total_cents
        if amount is None and total_cents is not None:
            try:
                amount = float(total_cents) / 100.0
            except (TypeError, ValueError):
                pass

        # Derive amount from raw_payload.total or raw_payload.amount
        if amount is None and raw_payload and isinstance(raw_payload, dict):
            rp_amount = raw_payload.get("total") or raw_payload.get("amount")
            if rp_amount is not None:
                try:
                    amount = float(rp_amount)
                except (TypeError, ValueError):
                    pass

        return {
            "item_count": item_count,
            "unit_count": unit_count,
            "amount": amount,
            "line_items_json": line_items,
            "raw_payload": raw_payload,
        }

    def _ts_iso(val) -> Optional[str]:
        if val is None:
            return None
        if hasattr(val, "isoformat"):
            return val.isoformat()
        return str(val)

    # ------------------------------------------------------------------
    # Serialize rows — derive missing fields, log first 3 rows only
    # ------------------------------------------------------------------
    _ser_t0 = time.monotonic()
    serialized_orders = []
    for _row_idx, row in enumerate(rows):
        derived = _derive_counts_and_amount(row)

        # Log first 3 rows for diagnostics
        if _row_idx < 3:
            lij = derived["line_items_json"]
            rp = derived["raw_payload"]
            logger.info(
                "[MOBILE_ORDER_REAL_ROW] id=%s customer=%s total_cents=%s amount=%s"
                " item_count=%s unit_count=%s",
                row.get("id"),
                row.get("customer_name"),
                row.get("total_cents"),
                derived["amount"],
                derived["item_count"],
                derived["unit_count"],
            )

        serialized_orders.append({
            "id": str(row["id"]) if row.get("id") else None,
            "org_id": str(row["org_id"]) if row.get("org_id") else None,
            "brand_id": str(row["brand_id"]) if row.get("brand_id") else None,
            "external_order_id": row.get("external_order_id"),
            "order_number": row.get("order_number"),
            "customer_name": row.get("customer_name"),
            "status": row.get("status"),
            "total_cents": row.get("total_cents"),
            "amount": float(derived["amount"]) if derived["amount"] is not None else None,
            "item_count": derived["item_count"],
            "unit_count": derived["unit_count"],
            "line_items_json": derived["line_items_json"],
            "raw_payload": derived["raw_payload"] if include_raw else None,
            "source": row.get("source"),
            "review_status": row.get("review_status"),
            "sync_status": row.get("sync_status"),
            "sync_health_status": row.get("sync_health_status"),
            "sync_health_missing_fields": row.get("sync_health_missing_fields"),
            "synced_at": _ts_iso(row.get("synced_at")),
            "last_synced_at": _ts_iso(row.get("last_synced_at")),
            "created_at": _ts_iso(row.get("created_at")),
            "updated_at": _ts_iso(row.get("updated_at")),
            "external_created_at": _ts_iso(row.get("external_created_at")),
            "external_updated_at": _ts_iso(row.get("external_updated_at")),
        })

    _ser_elapsed_ms = round((time.monotonic() - _ser_t0) * 1000, 2)

    # ------------------------------------------------------------------
    # Build next_cursor from last row's sort key
    # ------------------------------------------------------------------
    next_cursor: Optional[str] = None
    if has_more and serialized_orders:
        last = serialized_orders[-1]
        sort_ts = (
            last.get("external_updated_at")
            or last.get("updated_at")
            or last.get("created_at")
            or ""
        )
        next_cursor = base64.b64encode(
            f"id={last['id']}&sort_ts={sort_ts}".encode()
        ).decode()

    # ------------------------------------------------------------------
    # Summary log
    # ------------------------------------------------------------------
    first = serialized_orders[0] if serialized_orders else {}
    last_item = serialized_orders[-1] if serialized_orders else {}
    newest_ts = (
        first.get("external_updated_at")
        or first.get("updated_at")
        or first.get("created_at")
    )
    oldest_ts = (
        last_item.get("external_updated_at")
        or last_item.get("updated_at")
        or last_item.get("created_at")
    )

    _response_bytes = 0
    try:
        import json as _json2
        _response_bytes = len(_json2.dumps(serialized_orders).encode())
    except Exception:
        pass

    logger.info(
        "[MOBILE_ORDERS_SERIALIZE_DONE] elapsed_ms=%.2f bytes=%d",
        _ser_elapsed_ms,
        _response_bytes,
    )
    logger.info(
        "[MOBILE_ORDERS_QUERY_RESULT] count=%s first_order_id=%s first_customer=%s"
        " first_total_cents=%s first_amount=%s first_item_count=%s first_unit_count=%s"
        " newest_timestamp=%s oldest_timestamp=%s",
        len(serialized_orders),
        first.get("id"),
        first.get("customer_name"),
        first.get("total_cents"),
        first.get("amount"),
        first.get("item_count"),
        first.get("unit_count"),
        newest_ts,
        oldest_ts,
    )

    if original_limit > MOBILE_MAX_LIMIT:
        logger.warning(
            "[ORDERS_PAGINATION_GUARD] enforced brand_id=%s requested=%s served=%s",
            brand_id,
            original_limit,
            limit,
        )

    return make_json_safe({
        "ok": True,
        "orders": serialized_orders,
        "next_cursor": next_cursor,
        "has_more": has_more,
        "total_count": total_count,
    })
