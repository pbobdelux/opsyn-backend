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
    — Paginated orders. Max 250 per page. Never returns all 16k orders.

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
MOBILE_MAX_LIMIT = 250


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
    Wraps sync_health + sync_runs data into a compact mobile-friendly response.
    """
    logger.info(
        "[MOBILE_SYNC_STATUS_RETURNED] brand_id=%s",
        brand_id,
    )

    now = datetime.now(timezone.utc)

    # ------------------------------------------------------------------
    # 1. Fetch sync_health row for this brand
    # ------------------------------------------------------------------
    health: Optional[SyncHealth] = None
    try:
        _health_row = await db.execute(
            text(
                "SELECT id FROM sync_health"
                " WHERE brand_id = CAST(:brand_id AS UUID)"
                " LIMIT 1"
            ),
            {"brand_id": brand_id},
        )
        _health_id_row = _health_row.fetchone()
        if _health_id_row is not None:
            _health_res = await db.execute(
                select(SyncHealth).where(SyncHealth.id == _health_id_row[0])
            )
            health = _health_res.scalar_one_or_none()
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

    # ------------------------------------------------------------------
    # 2. Fetch the most recent active or completed sync run
    # ------------------------------------------------------------------
    active_run: Optional[SyncRun] = None
    last_completed_run: Optional[SyncRun] = None
    try:
        active_run_res = await db.execute(
            select(SyncRun).where(
                SyncRun.brand_id == brand_id,
                SyncRun.status.in_(["queued", "syncing"]),
            ).order_by(SyncRun.started_at.desc()).limit(1)
        )
        active_run = active_run_res.scalar_one_or_none()

        completed_run_res = await db.execute(
            select(SyncRun).where(
                SyncRun.brand_id == brand_id,
                SyncRun.status == "completed",
            ).order_by(SyncRun.completed_at.desc()).limit(1)
        )
        last_completed_run = completed_run_res.scalar_one_or_none()
    except Exception as exc:
        logger.error(
            "[MOBILE_SYNC_STATUS_RETURNED] sync_run_query_failed brand_id=%s error=%s",
            brand_id,
            str(exc)[:200],
        )
        try:
            await db.rollback()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # 3. Fetch total local order count (cheap indexed COUNT)
    # ------------------------------------------------------------------
    total_local_orders = 0
    try:
        count_res = await db.execute(
            text(
                "SELECT COUNT(*) FROM orders"
                " WHERE brand_id::text = :brand_id"
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
    # 4. Derive current_sync_state
    # ------------------------------------------------------------------
    if active_run is not None:
        if active_run.status == "queued":
            current_sync_state = "syncing"
        else:
            current_sync_state = "syncing"
    elif last_completed_run is not None:
        current_sync_state = "completed"
    elif health is not None and health.consecutive_failures > 0:
        current_sync_state = "failed"
    else:
        current_sync_state = "idle"

    # ------------------------------------------------------------------
    # 5. Derive is_stalled
    # ------------------------------------------------------------------
    is_stalled = False
    if active_run is not None:
        try:
            is_stalled = active_run.is_stalled(stall_threshold_seconds=90)
        except Exception:
            is_stalled = False

    # ------------------------------------------------------------------
    # 6. Derive safe_to_fetch_orders
    #    True only when sync is idle/completed and not stalled
    # ------------------------------------------------------------------
    safe_to_fetch_orders = (
        current_sync_state in ("idle", "completed")
        and not is_stalled
    )

    # ------------------------------------------------------------------
    # 7. Build response fields from available data
    # ------------------------------------------------------------------
    active_sync_run_id = str(active_run.id) if active_run else None

    pages_synced = 0
    orders_loaded_this_run = 0
    total_leaflink_estimate = None
    if active_run is not None:
        pages_synced = active_run.pages_synced or 0
        orders_loaded_this_run = active_run.orders_loaded_this_run or 0
        total_leaflink_estimate = active_run.total_orders_available
    elif last_completed_run is not None:
        pages_synced = last_completed_run.pages_synced or 0
        orders_loaded_this_run = last_completed_run.orders_loaded_this_run or 0
        total_leaflink_estimate = last_completed_run.total_orders_available

    failed_order_count = 0
    if health is not None:
        failed_order_count = health.consecutive_failures or 0

    last_successful_sync_at = None
    if health is not None and health.last_successful_sync_at:
        last_successful_sync_at = health.last_successful_sync_at.isoformat()
    elif last_completed_run is not None and last_completed_run.completed_at:
        try:
            last_successful_sync_at = last_completed_run.completed_at.isoformat()
        except Exception:
            pass

    last_updated_at = now.isoformat()
    if health is not None and health.updated_at:
        try:
            last_updated_at = health.updated_at.isoformat()
        except Exception:
            pass

    logger.info(
        "[MOBILE_SYNC_STATUS_RETURNED] brand_id=%s state=%s active_run=%s"
        " total_local=%s pages_synced=%s is_stalled=%s",
        brand_id,
        current_sync_state,
        active_sync_run_id,
        total_local_orders,
        pages_synced,
        is_stalled,
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

    Uses DB aggregate queries only — does NOT call LeafLink, does NOT trigger
    sync, and does NOT return order arrays. Returns small, focused metrics.

    If some metrics are not reliable (e.g. missing columns), returns 0 with
    metrics_partial=true. Never fakes values.
    """
    logger.info(
        "[MOBILE_DASHBOARD_SUMMARY_RETURNED] brand_id=%s",
        brand_id,
    )

    now = datetime.now(timezone.utc)
    metrics_partial = False

    # ------------------------------------------------------------------
    # 1. Total orders for this brand
    # ------------------------------------------------------------------
    total_orders = 0
    try:
        total_res = await db.execute(
            text(
                "SELECT COUNT(*) FROM orders"
                " WHERE brand_id::text = :brand_id"
            ),
            {"brand_id": brand_id},
        )
        total_orders = total_res.scalar() or 0
    except Exception as exc:
        logger.error(
            "[MOBILE_DASHBOARD_SUMMARY_RETURNED] total_orders_failed brand_id=%s error=%s",
            brand_id,
            str(exc)[:200],
        )
        metrics_partial = True
        try:
            await db.rollback()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # 2. Ready count — orders with status = 'approved' or 'received'
    #    (orders that are ready to be dispatched)
    # ------------------------------------------------------------------
    ready_count = 0
    try:
        ready_res = await db.execute(
            text(
                "SELECT COUNT(*) FROM orders"
                " WHERE brand_id::text = :brand_id"
                " AND status IN ('approved', 'received', 'open')"
            ),
            {"brand_id": brand_id},
        )
        ready_count = ready_res.scalar() or 0
    except Exception as exc:
        logger.error(
            "[MOBILE_DASHBOARD_SUMMARY_RETURNED] ready_count_failed brand_id=%s error=%s",
            brand_id,
            str(exc)[:200],
        )
        metrics_partial = True
        try:
            await db.rollback()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # 3. Blocked count — orders with review_status = 'blocked'
    # ------------------------------------------------------------------
    blocked_count = 0
    blocked_revenue = 0.0
    try:
        blocked_res = await db.execute(
            text(
                "SELECT COUNT(*), COALESCE(SUM(amount), 0)"
                " FROM orders"
                " WHERE brand_id::text = :brand_id"
                " AND review_status = 'blocked'"
            ),
            {"brand_id": brand_id},
        )
        blocked_row = blocked_res.fetchone()
        if blocked_row:
            blocked_count = int(blocked_row[0] or 0)
            blocked_revenue = float(blocked_row[1] or 0)
    except Exception as exc:
        logger.error(
            "[MOBILE_DASHBOARD_SUMMARY_RETURNED] blocked_metrics_failed brand_id=%s error=%s",
            brand_id,
            str(exc)[:200],
        )
        metrics_partial = True
        try:
            await db.rollback()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # 4. At-risk revenue — orders overdue > 0 days
    # ------------------------------------------------------------------
    at_risk_revenue = 0.0
    try:
        at_risk_res = await db.execute(
            text(
                "SELECT COALESCE(SUM(amount), 0)"
                " FROM orders"
                " WHERE brand_id::text = :brand_id"
                " AND days_overdue > 0"
            ),
            {"brand_id": brand_id},
        )
        at_risk_revenue = float(at_risk_res.scalar() or 0)
    except Exception as exc:
        logger.error(
            "[MOBILE_DASHBOARD_SUMMARY_RETURNED] at_risk_revenue_failed brand_id=%s error=%s",
            brand_id,
            str(exc)[:200],
        )
        metrics_partial = True
        try:
            await db.rollback()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # 5. AR due total — sum of balance_due across all orders
    # ------------------------------------------------------------------
    ar_due_total = 0.0
    try:
        ar_res = await db.execute(
            text(
                "SELECT COALESCE(SUM(balance_due), 0)"
                " FROM orders"
                " WHERE brand_id::text = :brand_id"
            ),
            {"brand_id": brand_id},
        )
        ar_due_total = float(ar_res.scalar() or 0)
    except Exception as exc:
        logger.error(
            "[MOBILE_DASHBOARD_SUMMARY_RETURNED] ar_due_total_failed brand_id=%s error=%s",
            brand_id,
            str(exc)[:200],
        )
        metrics_partial = True
        try:
            await db.rollback()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # 6. Routes ready count — orders with delivery_status = 'assigned'
    # ------------------------------------------------------------------
    routes_ready_count = 0
    try:
        routes_res = await db.execute(
            text(
                "SELECT COUNT(*) FROM orders"
                " WHERE brand_id::text = :brand_id"
                " AND delivery_status = 'assigned'"
            ),
            {"brand_id": brand_id},
        )
        routes_ready_count = int(routes_res.scalar() or 0)
    except Exception as exc:
        logger.error(
            "[MOBILE_DASHBOARD_SUMMARY_RETURNED] routes_ready_count_failed brand_id=%s error=%s",
            brand_id,
            str(exc)[:200],
        )
        metrics_partial = True
        try:
            await db.rollback()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # 7. Last updated timestamp from sync_health
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

    logger.info(
        "[MOBILE_DASHBOARD_SUMMARY_RETURNED] brand_id=%s total_orders=%s"
        " ready=%s blocked=%s blocked_revenue=%.2f ar_due=%.2f"
        " routes_ready=%s metrics_partial=%s",
        brand_id,
        total_orders,
        ready_count,
        blocked_count,
        blocked_revenue,
        ar_due_total,
        routes_ready_count,
        metrics_partial,
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
    limit: int = Query(MOBILE_DEFAULT_LIMIT, ge=1, description="Orders per page (default 100, max 250)"),
    cursor: Optional[str] = Query(None, description="Opaque cursor for cursor-based pagination"),
    fresh: bool = Query(False, description="If true, bypass any upstream caching"),
    db: AsyncSession = Depends(get_db),
):
    """Paginated orders for the iOS app.

    Enforces a hard maximum of 250 orders per page. Never returns all 16k orders.
    Uses cursor-based pagination for efficiency. Supports fresh=true to bypass
    any upstream caching.

    Uses raw SQL with plain VARCHAR brand_id comparison (no UUID casts) and sorts
    by COALESCE(external_updated_at, updated_at, created_at) DESC so the newest
    orders always appear first.

    Cursor format: base64("id=<UUID>&sort_ts=<ISO>")
    """
    import json as _json

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
        "[MOBILE_ORDERS_QUERY]\nbrand_id=%s\nlimit=%s\ncursor=%s\nfresh=%s\nsource_table=orders",
        brand_id,
        limit,
        "present" if cursor else "none",
        fresh,
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
                "[MOBILE_ORDERS_QUERY] cursor_decode_error brand_id=%s error=%s",
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
                "[MOBILE_ORDERS_DB_MAX_TIMESTAMPS]\n"
                "max_external_updated_at=%s\n"
                "max_updated_at=%s\n"
                "max_created_at=%s",
                ts_row[0],
                ts_row[1],
                ts_row[2],
            )
    except Exception as ts_exc:
        logger.warning(
            "[MOBILE_ORDERS_QUERY] max_timestamps_failed brand_id=%s error=%s",
            brand_id,
            str(ts_exc)[:200],
        )
        try:
            await db.rollback()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Build raw SQL query — plain VARCHAR comparison, no UUID casts.
    # Sort by newest COALESCE(external_updated_at, updated_at, created_at) DESC
    # so the most recently updated orders always appear first.
    # ------------------------------------------------------------------
    base_sql = (
        "SELECT"
        " id, org_id, brand_id, external_order_id, order_number, customer_name, status,"
        " total_cents, amount, item_count, unit_count,"
        " line_items_json, raw_payload, source,"
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

    try:
        rows_res = await db.execute(text(base_sql), params)
        raw_rows = rows_res.fetchall()
        col_names = list(rows_res.keys())
        rows = [dict(zip(col_names, row)) for row in raw_rows]
    except Exception as exc:
        logger.error(
            "[MOBILE_ORDERS_QUERY] db_query_failed brand_id=%s error=%s",
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

    has_more = len(rows) > limit
    if has_more:
        rows = rows[:limit]

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
            "[MOBILE_ORDERS_QUERY] count_failed brand_id=%s error=%s",
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

        # Resolve raw_payload — may be a dict or raw JSON string
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
    # Serialize rows — derive missing fields, log each row
    # ------------------------------------------------------------------
    serialized_orders = []
    for row in rows:
        derived = _derive_counts_and_amount(row)

        lij = derived["line_items_json"]
        rp = derived["raw_payload"]
        logger.info(
            "[MOBILE_ORDER_REAL_ROW]\n"
            "id=%s\n"
            "external_order_id=%s\n"
            "order_number=%s\n"
            "customer_name=%s\n"
            "total_cents=%s\n"
            "amount=%s\n"
            "item_count=%s\n"
            "unit_count=%s\n"
            "line_items_json_len=%s\n"
            "raw_payload_len=%s\n"
            "external_created_at=%s\n"
            "external_updated_at=%s\n"
            "updated_at=%s",
            row.get("id"),
            row.get("external_order_id"),
            row.get("order_number"),
            row.get("customer_name"),
            row.get("total_cents"),
            derived["amount"],
            derived["item_count"],
            derived["unit_count"],
            len(lij) if isinstance(lij, list) else (1 if lij else 0),
            len(rp) if isinstance(rp, dict) else (len(str(rp)) if rp else 0),
            _ts_iso(row.get("external_created_at")),
            _ts_iso(row.get("external_updated_at")),
            _ts_iso(row.get("updated_at")),
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
            "raw_payload": derived["raw_payload"],
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
    logger.info(
        "[MOBILE_ORDERS_QUERY_RESULT]\n"
        "count=%s\n"
        "first_order_id=%s\n"
        "first_customer=%s\n"
        "first_total_cents=%s\n"
        "first_amount=%s\n"
        "first_item_count=%s\n"
        "first_unit_count=%s\n"
        "newest_timestamp=%s\n"
        "oldest_timestamp=%s",
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
