"""
Mobile API — lightweight frontend-facing endpoints for iOS.

Provides:
  - POST /api/mobile/orders/sync          — trigger sync, return immediately
  - GET  /api/mobile/orders/sync-status   — lightweight status (no order arrays)
  - GET  /api/mobile/orders/dashboard-summary — aggregate-only dashboard data
  - GET  /api/mobile/orders               — paginated orders with guardrails

Design principles:
  - Never returns order arrays from sync or status endpoints
  - Pagination is enforced on all order list endpoints (max 250)
  - Dashboard data comes from DB aggregates only (no LeafLink calls)
  - Sync trigger delegates to existing /api/leaflink/orders/full-resync logic
  - Existing /api/leaflink/* endpoints are NOT modified
"""

import base64
import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import SyncRequest, SyncRun
from utils.json_utils import make_json_safe

logger = logging.getLogger("mobile_api")

router = APIRouter(prefix="/api/mobile", tags=["mobile"])

# ---------------------------------------------------------------------------
# Pagination constants
# ---------------------------------------------------------------------------
_MOBILE_DEFAULT_LIMIT = 100
_MOBILE_MAX_LIMIT = 250


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _get_brand_order_count(db: AsyncSession, brand_id: str) -> int:
    """Return total order count for a brand using a safe text query."""
    try:
        result = await db.execute(
            text("SELECT COUNT(*) FROM orders WHERE brand_id::text = :brand_id"),
            {"brand_id": brand_id},
        )
        return result.scalar() or 0
    except Exception as exc:
        logger.error("[MobileAPI] order_count_error brand=%s error=%s", brand_id, exc)
        try:
            await db.rollback()
        except Exception:
            pass
        return 0


# =============================================================================
# POST /api/mobile/orders/sync
# =============================================================================

@router.post("/orders/sync")
async def mobile_sync_command(
    brand_id: str = Query(..., description="Brand ID (UUID)"),
    db: AsyncSession = Depends(get_db),
):
    """
    Trigger a full resync for a brand and return immediately.

    Delegates to the existing full-resync enqueue logic — does NOT call
    LeafLink inline.  Returns a small JSON payload with the sync_run_id so
    the caller can poll /api/mobile/orders/sync-status.

    Response:
        { ok, state, sync_run_id, brand_id, message }
    """
    from services.credential_resolver import resolve_brand_credential
    from services.sync_run_manager import create_sync_run
    import database as _database_module

    timestamp = datetime.now(timezone.utc).isoformat()
    logger.info("[MobileAPI] sync_command brand_id=%s", brand_id)

    # Resolve session factory at call time (not import time)
    try:
        _session_factory = _database_module.get_async_session_local()
    except RuntimeError as _sf_exc:
        logger.error("[MobileAPI] sync_command session_factory_error error=%s", _sf_exc)
        return make_json_safe({
            "ok": False,
            "error": f"Database not ready: {str(_sf_exc)[:200]}",
            "brand_id": brand_id,
            "timestamp": timestamp,
        })

    # Validate credential exists before enqueuing
    cred_row = await resolve_brand_credential(db, brand_id, "leaflink")
    if not cred_row:
        return make_json_safe({
            "ok": False,
            "error": "leaflink_credential_not_found",
            "brand_id": brand_id,
            "timestamp": timestamp,
        })

    resolved_brand_id = cred_row[1]

    # Create SyncRun record
    sync_run_id = None
    state = "queued"
    try:
        async with _session_factory() as _run_db:
            async with _run_db.begin():
                _run = await create_sync_run(_run_db, resolved_brand_id, mode="full")
                sync_run_id = _run.id
        logger.info(
            "[MobileAPI] sync_run_created brand_id=%s run_id=%s",
            resolved_brand_id,
            sync_run_id,
        )
    except ValueError as dup_exc:
        # Active run already exists — return its ID
        logger.warning(
            "[MobileAPI] sync_already_active brand_id=%s detail=%s",
            resolved_brand_id,
            dup_exc,
        )
        state = "already_running"
        # Try to get the existing active run ID
        try:
            active_run = await SyncRun.get_active_run(db, resolved_brand_id)
            if active_run:
                sync_run_id = active_run.id
        except Exception:
            pass
    except Exception as run_exc:
        logger.warning("[MobileAPI] sync_run_create_error error=%s", run_exc)
        # Non-fatal — proceed without a SyncRun record

    # Enqueue background SyncRequest (worker picks this up)
    if state == "queued":
        try:
            async with _session_factory() as _enq_db:
                async with _enq_db.begin():
                    _enq_db.add(SyncRequest(
                        brand_id=resolved_brand_id,
                        status="pending",
                        start_page=1,
                        total_pages=None,
                        total_orders_available=None,
                    ))
            logger.info(
                "[MobileAPI] sync_enqueued brand_id=%s run_id=%s",
                resolved_brand_id,
                sync_run_id,
            )
        except Exception as enq_exc:
            logger.error(
                "[MobileAPI] sync_enqueue_error brand_id=%s error=%s",
                resolved_brand_id,
                enq_exc,
            )
            return make_json_safe({
                "ok": False,
                "error": f"Failed to enqueue sync: {str(enq_exc)[:200]}",
                "brand_id": resolved_brand_id,
                "sync_run_id": sync_run_id,
                "timestamp": timestamp,
            })

    logger.info(
        "[MOBILE_SYNC_COMMAND_ACCEPTED] brand_id=%s sync_run_id=%s state=%s",
        resolved_brand_id,
        sync_run_id,
        state,
    )

    return make_json_safe({
        "ok": True,
        "state": state,
        "sync_run_id": sync_run_id,
        "brand_id": resolved_brand_id,
        "message": "Sync queued" if state == "queued" else "Sync already running",
        "timestamp": timestamp,
    })


# =============================================================================
# GET /api/mobile/orders/sync-status
# =============================================================================

@router.get("/orders/sync-status")
async def mobile_sync_status(
    brand_id: str = Query(..., description="Brand ID (UUID)"),
    db: AsyncSession = Depends(get_db),
):
    """
    Return lightweight sync status for a brand.

    Never returns order arrays — only scalar metrics and state flags.
    Reads from DB only (no LeafLink calls).

    Response:
        { ok, brand_id, current_sync_state, active_sync_run_id,
          total_local_orders, total_leaflink_estimate, pages_synced,
          orders_loaded_this_run, failed_order_count, is_stalled,
          data_source, last_successful_sync_at, last_updated_at,
          safe_to_fetch_orders }
    """
    from services.sync_run_manager import detect_stalled
    from sqlalchemy import text as _text_ss

    _STALL_SECONDS = 1800  # 30 minutes

    timestamp = datetime.now(timezone.utc).isoformat()
    logger.info("[MobileAPI] sync_status brand_id=%s", brand_id)

    # Live order count — always from DB
    live_count = await _get_brand_order_count(db, brand_id)

    # Snapshot cache for partial/failed counts and last sync time
    snapshot = None
    try:
        snap_result = await db.execute(
            _text_ss(
                "SELECT total_local_orders, total_ok, total_partial, total_failed, "
                "dead_letter_count, pages_processed, records_processed, "
                "last_successful_sync_at, updated_at, sync_run_id "
                "FROM sync_metrics_snapshots "
                "WHERE brand_id::text = :brand_id "
                "ORDER BY updated_at DESC NULLS LAST "
                "LIMIT 1"
            ),
            {"brand_id": brand_id},
        )
        snapshot = snap_result.fetchone()
    except Exception as snap_exc:
        logger.info(
            "[MobileAPI] sync_status snapshot_miss brand_id=%s reason=%s",
            brand_id,
            str(snap_exc)[:100],
        )
        try:
            await db.rollback()
        except Exception:
            pass

    # Active and last completed SyncRun
    try:
        active_run = await SyncRun.get_active_run(db, brand_id)
    except Exception:
        await db.rollback()
        active_run = None

    try:
        last_completed = await SyncRun.get_last_completed_run(db, brand_id)
    except Exception:
        await db.rollback()
        last_completed = None

    try:
        last_run_result = await db.execute(
            select(SyncRun)
            .where(SyncRun.brand_id == brand_id)
            .order_by(SyncRun.started_at.desc())
            .limit(1)
        )
        last_run = last_run_result.scalar_one_or_none()
    except Exception:
        await db.rollback()
        last_run = None

    # Stall detection
    is_stalled = detect_stalled(active_run) if active_run else False
    if active_run and not is_stalled:
        _last_updated = active_run.last_progress_at or active_run.started_at
        if _last_updated is not None:
            if _last_updated.tzinfo is None:
                _last_updated = _last_updated.replace(tzinfo=timezone.utc)
            _elapsed = (datetime.now(timezone.utc) - _last_updated).total_seconds()
            if _elapsed > _STALL_SECONDS:
                is_stalled = True

    # Determine sync state
    if active_run and not is_stalled:
        current_sync_state = "syncing"
    elif active_run and is_stalled:
        current_sync_state = "error"
    elif last_run and last_run.status == "failed":
        current_sync_state = "error"
    elif last_completed:
        current_sync_state = "completed"
    elif last_run:
        current_sync_state = "idle"
    else:
        current_sync_state = "idle"

    # Counts from snapshot
    failed_order_count = 0
    snap_last_sync = None
    if snapshot:
        failed_order_count = (snapshot[3] or 0)
        snap_last_sync = snapshot[7]

    # Timestamps
    last_successful_sync_at = None
    if snap_last_sync:
        last_successful_sync_at = (
            snap_last_sync.isoformat()
            if hasattr(snap_last_sync, "isoformat")
            else str(snap_last_sync)
        )
    elif last_completed and last_completed.completed_at:
        last_successful_sync_at = last_completed.completed_at.isoformat()

    _active_run_id = active_run.id if active_run else None
    _pages_synced = last_run.pages_synced if last_run else 0
    _orders_loaded = last_run.orders_loaded_this_run if last_run else 0
    _total_leaflink_estimate = last_run.total_orders_available if last_run else None

    _last_updated_at = None
    if active_run:
        _lu = active_run.last_progress_at or active_run.updated_at
        if _lu:
            _last_updated_at = _lu.isoformat() if hasattr(_lu, "isoformat") else str(_lu)

    # safe_to_fetch_orders: true when sync is not actively running and we have orders
    safe_to_fetch_orders = (
        current_sync_state in ("completed", "idle")
        and live_count > 0
    )

    logger.info(
        "[MOBILE_SYNC_STATUS_RETURNED] brand_id=%s state=%s total_local=%s stalled=%s",
        brand_id,
        current_sync_state,
        live_count,
        is_stalled,
    )

    return make_json_safe({
        "ok": True,
        "brand_id": brand_id,
        "current_sync_state": current_sync_state,
        "active_sync_run_id": _active_run_id,
        "total_local_orders": live_count,
        "total_leaflink_estimate": _total_leaflink_estimate,
        "pages_synced": _pages_synced,
        "orders_loaded_this_run": _orders_loaded,
        "failed_order_count": failed_order_count,
        "is_stalled": is_stalled,
        "data_source": "live",
        "last_successful_sync_at": last_successful_sync_at,
        "last_updated_at": _last_updated_at,
        "safe_to_fetch_orders": safe_to_fetch_orders,
        "timestamp": timestamp,
    })


# =============================================================================
# GET /api/mobile/orders/dashboard-summary
# =============================================================================

@router.get("/orders/dashboard-summary")
async def mobile_dashboard_summary(
    brand_id: str = Query(..., description="Brand ID (UUID)"),
    db: AsyncSession = Depends(get_db),
):
    """
    Return aggregate-only dashboard metrics for a brand.

    Uses ONLY DB aggregate queries — no LeafLink calls, no order arrays.
    All counts are computed via COUNT/SUM aggregates directly in PostgreSQL.

    Response:
        { ok, brand_id, total_orders, ready_count, blocked_count,
          blocked_revenue, at_risk_revenue, ar_due_total,
          routes_ready_count, last_updated_at, data_source, metrics_partial }
    """
    timestamp = datetime.now(timezone.utc).isoformat()
    logger.info("[MobileAPI] dashboard_summary brand_id=%s", brand_id)

    metrics_partial = False

    # ------------------------------------------------------------------
    # Total orders
    # ------------------------------------------------------------------
    total_orders = await _get_brand_order_count(db, brand_id)

    # ------------------------------------------------------------------
    # Ready count: review_status = 'ready'
    # ------------------------------------------------------------------
    ready_count = 0
    try:
        res = await db.execute(
            text(
                "SELECT COUNT(*) FROM orders "
                "WHERE brand_id::text = :brand_id AND review_status = 'ready'"
            ),
            {"brand_id": brand_id},
        )
        ready_count = res.scalar() or 0
    except Exception as exc:
        logger.warning("[MobileAPI] dashboard ready_count error brand=%s error=%s", brand_id, exc)
        metrics_partial = True
        try:
            await db.rollback()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Blocked count: review_status = 'blocked'
    # ------------------------------------------------------------------
    blocked_count = 0
    try:
        res = await db.execute(
            text(
                "SELECT COUNT(*) FROM orders "
                "WHERE brand_id::text = :brand_id AND review_status = 'blocked'"
            ),
            {"brand_id": brand_id},
        )
        blocked_count = res.scalar() or 0
    except Exception as exc:
        logger.warning("[MobileAPI] dashboard blocked_count error brand=%s error=%s", brand_id, exc)
        metrics_partial = True
        try:
            await db.rollback()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Blocked revenue: SUM(amount) for blocked orders
    # ------------------------------------------------------------------
    blocked_revenue = 0.0
    try:
        res = await db.execute(
            text(
                "SELECT COALESCE(SUM(amount), 0) FROM orders "
                "WHERE brand_id::text = :brand_id AND review_status = 'blocked'"
            ),
            {"brand_id": brand_id},
        )
        blocked_revenue = float(res.scalar() or 0)
    except Exception as exc:
        logger.warning("[MobileAPI] dashboard blocked_revenue error brand=%s error=%s", brand_id, exc)
        metrics_partial = True
        try:
            await db.rollback()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # At-risk revenue: SUM(amount) for orders with review_status = 'needs_review'
    # ------------------------------------------------------------------
    at_risk_revenue = 0.0
    try:
        res = await db.execute(
            text(
                "SELECT COALESCE(SUM(amount), 0) FROM orders "
                "WHERE brand_id::text = :brand_id AND review_status = 'needs_review'"
            ),
            {"brand_id": brand_id},
        )
        at_risk_revenue = float(res.scalar() or 0)
    except Exception as exc:
        logger.warning("[MobileAPI] dashboard at_risk_revenue error brand=%s error=%s", brand_id, exc)
        metrics_partial = True
        try:
            await db.rollback()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # AR due total: SUM(balance_due) for orders with balance_due > 0
    # ------------------------------------------------------------------
    ar_due_total = 0.0
    try:
        res = await db.execute(
            text(
                "SELECT COALESCE(SUM(balance_due), 0) FROM orders "
                "WHERE brand_id::text = :brand_id AND balance_due > 0"
            ),
            {"brand_id": brand_id},
        )
        ar_due_total = float(res.scalar() or 0)
    except Exception as exc:
        logger.warning("[MobileAPI] dashboard ar_due_total error brand=%s error=%s", brand_id, exc)
        metrics_partial = True
        try:
            await db.rollback()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Routes ready count: delivery_status = 'pending' AND review_status = 'ready'
    # ------------------------------------------------------------------
    routes_ready_count = 0
    try:
        res = await db.execute(
            text(
                "SELECT COUNT(*) FROM orders "
                "WHERE brand_id::text = :brand_id "
                "AND delivery_status = 'pending' AND review_status = 'ready'"
            ),
            {"brand_id": brand_id},
        )
        routes_ready_count = res.scalar() or 0
    except Exception as exc:
        logger.warning("[MobileAPI] dashboard routes_ready_count error brand=%s error=%s", brand_id, exc)
        metrics_partial = True
        try:
            await db.rollback()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Last updated: most recent updated_at across all orders for brand
    # ------------------------------------------------------------------
    last_updated_at = None
    try:
        res = await db.execute(
            text(
                "SELECT MAX(updated_at) FROM orders "
                "WHERE brand_id::text = :brand_id"
            ),
            {"brand_id": brand_id},
        )
        _max_ts = res.scalar()
        if _max_ts:
            last_updated_at = (
                _max_ts.isoformat()
                if hasattr(_max_ts, "isoformat")
                else str(_max_ts)
            )
    except Exception as exc:
        logger.warning("[MobileAPI] dashboard last_updated_at error brand=%s error=%s", brand_id, exc)
        try:
            await db.rollback()
        except Exception:
            pass

    logger.info(
        "[MOBILE_DASHBOARD_SUMMARY_RETURNED] brand_id=%s total=%s ready=%s blocked=%s partial=%s",
        brand_id,
        total_orders,
        ready_count,
        blocked_count,
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
        "timestamp": timestamp,
    })


# =============================================================================
# GET /api/mobile/orders
# =============================================================================

@router.get("/orders")
async def mobile_orders(
    brand_id: str = Query(..., description="Brand ID (UUID)"),
    limit: int = Query(_MOBILE_DEFAULT_LIMIT, ge=1, description="Orders per page (default 100, max 250)"),
    cursor: Optional[str] = Query(None, description="Opaque cursor for cursor-based pagination"),
    db: AsyncSession = Depends(get_db),
):
    """
    Return a paginated page of compact order records for a brand.

    Enforces a hard maximum of 250 orders per page — never returns all orders.
    Uses cursor-based pagination for efficient traversal of large datasets.

    Cursor format: base64("id=<UUID>&updated_at=<ISO>")

    Response:
        { ok, orders, next_cursor, has_more, total_count }
    """
    from sqlalchemy import text as _text_orders

    timestamp = datetime.now(timezone.utc).isoformat()

    # ------------------------------------------------------------------
    # Pagination guard: clamp limit to MAX
    # ------------------------------------------------------------------
    if limit > _MOBILE_MAX_LIMIT:
        logger.info(
            "[ORDERS_PAGINATION_GUARD] brand_id=%s requested_limit=%s clamped_to=%s",
            brand_id,
            limit,
            _MOBILE_MAX_LIMIT,
        )
        limit = _MOBILE_MAX_LIMIT

    logger.info(
        "[MobileAPI] orders brand_id=%s limit=%s cursor=%s",
        brand_id,
        limit,
        bool(cursor),
    )

    # ------------------------------------------------------------------
    # Decode cursor → (cursor_id, cursor_updated_at)
    # ------------------------------------------------------------------
    cursor_id: Optional[str] = None
    cursor_updated_at: Optional[str] = None

    if cursor:
        try:
            decoded = base64.b64decode(cursor).decode()
            parts = dict(p.split("=", 1) for p in decoded.split("&") if "=" in p)
            cursor_id = parts.get("id") or None
            cursor_updated_at = parts.get("updated_at") or None
        except Exception as cursor_exc:
            logger.error("[MobileAPI] cursor_decode_error error=%s", cursor_exc)
            return make_json_safe({"ok": False, "error": "Invalid cursor format", "timestamp": timestamp})

    # ------------------------------------------------------------------
    # Total count (always live)
    # ------------------------------------------------------------------
    total_count = await _get_brand_order_count(db, brand_id)

    # ------------------------------------------------------------------
    # Fetch limit + 1 rows to detect has_more
    # ------------------------------------------------------------------
    try:
        if cursor_id and cursor_updated_at:
            rows_result = await db.execute(
                _text_orders(
                    "SELECT id, external_order_id, order_number, customer_name, "
                    "status, review_status, amount, item_count, unit_count, "
                    "delivery_status, payment_status, updated_at "
                    "FROM orders "
                    "WHERE brand_id::text = :brand_id "
                    "AND (updated_at < :cursor_updated_at "
                    "     OR (updated_at = :cursor_updated_at AND id < :cursor_id)) "
                    "ORDER BY updated_at DESC, id DESC "
                    "LIMIT :limit"
                ),
                {
                    "brand_id": brand_id,
                    "cursor_updated_at": cursor_updated_at,
                    "cursor_id": cursor_id,
                    "limit": limit + 1,
                },
            )
        else:
            rows_result = await db.execute(
                _text_orders(
                    "SELECT id, external_order_id, order_number, customer_name, "
                    "status, review_status, amount, item_count, unit_count, "
                    "delivery_status, payment_status, updated_at "
                    "FROM orders "
                    "WHERE brand_id::text = :brand_id "
                    "ORDER BY updated_at DESC, id DESC "
                    "LIMIT :limit"
                ),
                {"brand_id": brand_id, "limit": limit + 1},
            )
        rows = rows_result.fetchall()
    except Exception as exc:
        logger.error("[MobileAPI] orders_query_error brand=%s error=%s", brand_id, exc)
        try:
            await db.rollback()
        except Exception:
            pass
        return make_json_safe({
            "ok": False,
            "error": "Database query failed",
            "detail": str(exc)[:300],
            "timestamp": timestamp,
        })

    has_more = len(rows) > limit
    if has_more:
        rows = rows[:limit]

    # ------------------------------------------------------------------
    # Serialize compact order records
    # ------------------------------------------------------------------
    orders_out = []
    for row in rows:
        _updated_at = row[11]
        _updated_at_iso = (
            _updated_at.isoformat()
            if _updated_at and hasattr(_updated_at, "isoformat")
            else str(_updated_at) if _updated_at else None
        )
        orders_out.append({
            "id": str(row[0]) if row[0] else None,
            "external_order_id": row[1],
            "order_number": row[2],
            "customer_name": row[3],
            "status": row[4],
            "review_status": row[5],
            "amount": float(row[6]) if row[6] is not None else 0.0,
            "item_count": row[7],
            "unit_count": row[8],
            "delivery_status": row[9],
            "payment_status": row[10],
            "updated_at": _updated_at_iso,
        })

    # ------------------------------------------------------------------
    # Build next cursor from last row
    # ------------------------------------------------------------------
    next_cursor: Optional[str] = None
    if has_more and orders_out:
        last = orders_out[-1]
        _last_id = last["id"] or ""
        _last_updated = last["updated_at"] or ""
        next_cursor = base64.b64encode(
            f"id={_last_id}&updated_at={_last_updated}".encode()
        ).decode()

    logger.info(
        "[MOBILE_ORDERS_PAGE_RETURNED] brand_id=%s limit=%s returned=%s has_more=%s total=%s",
        brand_id,
        limit,
        len(orders_out),
        has_more,
        total_count,
    )

    return make_json_safe({
        "ok": True,
        "orders": orders_out,
        "next_cursor": next_cursor,
        "has_more": has_more,
        "total_count": total_count,
        "returned_count": len(orders_out),
        "timestamp": timestamp,
    })
