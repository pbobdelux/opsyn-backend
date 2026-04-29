"""
Debug endpoint for order sync diagnostics.
GET /debug/orders-sync?brand=<brand_id>

Returns a comprehensive snapshot of LeafLink credential health, database
order counts, sync status, and actionable recommendations — without
triggering a live sync or modifying any data.
"""
import logging
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import BrandAPICredential, Order

logger = logging.getLogger("debug_orders_sync")

router = APIRouter(tags=["debug"])


@router.get("/orders-sync")
async def debug_orders_sync(
    brand: Optional[str] = Query(None, description="Brand slug or ID (e.g. 'noble-nectar')"),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """
    Comprehensive order-sync diagnostic endpoint.

    Returns LeafLink credential status, database order counts, sync
    metadata, and human-readable recommendations — all read-only.
    Never triggers a sync or modifies data.
    """
    logger.info("[OrdersSync] debug_endpoint brand=%s", brand)

    backend_current_time = datetime.now(timezone.utc).isoformat()
    recommendations: list[str] = []

    # ------------------------------------------------------------------ #
    # LeafLink credential status                                           #
    # ------------------------------------------------------------------ #
    credential_valid = False
    last_pull_at: Optional[str] = None
    last_pull_success: Optional[bool] = None
    last_cred_error: Optional[str] = None
    orders_fetched_in_last_pull: Optional[int] = None

    cred: Optional[BrandAPICredential] = None
    try:
        cred_query = select(BrandAPICredential).where(
            BrandAPICredential.integration_name == "leaflink",
            BrandAPICredential.is_active == True,
        )
        if brand:
            cred_query = cred_query.where(BrandAPICredential.brand_id == brand)
        else:
            cred_query = cred_query.order_by(
                BrandAPICredential.last_sync_at.desc().nullslast()
            ).limit(1)

        cred_result = await db.execute(cred_query)
        cred = cred_result.scalar_one_or_none()

        if cred:
            credential_valid = cred.sync_status == "ok" or (
                cred.sync_status not in ("failed",) and cred.last_error is None
            )
            last_pull_at = cred.last_sync_at.isoformat() if cred.last_sync_at else None
            last_pull_success = cred.sync_status == "ok" if cred.last_sync_at else None
            last_cred_error = cred.last_error
            logger.info(
                "[OrdersSync] debug_endpoint credential_found brand=%s sync_status=%s",
                cred.brand_id,
                cred.sync_status,
            )
        else:
            logger.warning(
                "[OrdersSync] debug_endpoint credential_not_found brand=%s",
                brand,
            )
            recommendations.append(
                "No active LeafLink credential found for this brand. "
                "Add credentials via POST /integrations/leaflink/connect."
            )
    except Exception as cred_exc:
        logger.error(
            "[OrdersSync] debug_endpoint credential_lookup_failed brand=%s error=%s",
            brand,
            cred_exc,
        )
        recommendations.append(f"Credential lookup failed: {cred_exc}")

    leaflink_status: dict[str, Any] = {
        "credential_valid": credential_valid,
        "last_pull_at": last_pull_at,
        "last_pull_success": last_pull_success,
        "last_error": last_cred_error,
        "orders_fetched_in_last_pull": orders_fetched_in_last_pull,
    }

    # ------------------------------------------------------------------ #
    # Database order counts                                                #
    # ------------------------------------------------------------------ #
    total_orders = 0
    newest_order_date: Optional[str] = None
    oldest_order_date: Optional[str] = None
    count_by_status: dict[str, int] = {}
    rejected_order_count = 0
    orders_with_mapping_issues = 0
    orders_with_review_blockers = 0

    try:
        # Total count
        count_q = select(func.count(Order.id))
        if brand:
            count_q = count_q.where(Order.brand_id == brand)
        total_orders = (await db.execute(count_q)).scalar_one() or 0

        if total_orders > 0:
            # Date range
            date_q = select(
                func.max(Order.external_updated_at),
                func.min(Order.external_updated_at),
            )
            if brand:
                date_q = date_q.where(Order.brand_id == brand)
            date_result = await db.execute(date_q)
            date_row = date_result.one()
            newest_dt, oldest_dt = date_row[0], date_row[1]
            newest_order_date = newest_dt.isoformat() if newest_dt else None
            oldest_order_date = oldest_dt.isoformat() if oldest_dt else None

            # Count by status
            status_q = select(Order.status, func.count(Order.id)).group_by(Order.status)
            if brand:
                status_q = status_q.where(Order.brand_id == brand)
            status_result = await db.execute(status_q)
            for row in status_result.all():
                status_val = row[0] or "unknown"
                count_by_status[status_val] = row[1]

            rejected_order_count = count_by_status.get("rejected", 0)

            # Orders with mapping issues (review_status = 'blocked')
            blocked_q = select(func.count(Order.id)).where(Order.review_status == "blocked")
            if brand:
                blocked_q = blocked_q.where(Order.brand_id == brand)
            orders_with_mapping_issues = (await db.execute(blocked_q)).scalar_one() or 0

            # Orders with review blockers (needs_review)
            review_q = select(func.count(Order.id)).where(
                Order.review_status.in_(["blocked", "needs_review"])
            )
            if brand:
                review_q = review_q.where(Order.brand_id == brand)
            orders_with_review_blockers = (await db.execute(review_q)).scalar_one() or 0

        logger.info(
            "[OrdersSync] debug_endpoint db_counts brand=%s total=%s newest=%s",
            brand,
            total_orders,
            newest_order_date,
        )
    except Exception as db_exc:
        logger.error(
            "[OrdersSync] debug_endpoint db_count_failed brand=%s error=%s",
            brand,
            db_exc,
        )
        recommendations.append(f"Database count query failed: {db_exc}")

    database_status: dict[str, Any] = {
        "total_orders": total_orders,
        "newest_order_date": newest_order_date,
        "oldest_order_date": oldest_order_date,
        "count_by_status": count_by_status,
        "rejected_order_count": rejected_order_count,
        "orders_with_mapping_issues": orders_with_mapping_issues,
        "orders_with_review_blockers": orders_with_review_blockers,
    }

    # ------------------------------------------------------------------ #
    # Sync status                                                          #
    # ------------------------------------------------------------------ #
    last_sync_at: Optional[str] = None
    last_sync_success: Optional[bool] = None
    last_sync_error: Optional[str] = None
    sync_duration_seconds: Optional[float] = None
    manual_override_blocking_sync = False  # blockers never block sync in this service

    if cred:
        last_sync_at = cred.last_sync_at.isoformat() if cred.last_sync_at else None
        last_sync_success = cred.sync_status == "ok" if cred.last_sync_at else None
        last_sync_error = cred.last_error

    sync_status: dict[str, Any] = {
        "last_sync_at": last_sync_at,
        "last_sync_success": last_sync_success,
        "last_sync_error": last_sync_error,
        "sync_duration_seconds": sync_duration_seconds,
        "manual_override_blocking_sync": manual_override_blocking_sync,
    }

    # ------------------------------------------------------------------ #
    # Recommendations                                                      #
    # ------------------------------------------------------------------ #
    if not recommendations:
        if not cred:
            recommendations.append(
                "No active LeafLink credential found. "
                "Add credentials via POST /integrations/leaflink/connect."
            )
        elif not credential_valid:
            recommendations.append(
                f"LeafLink credential is marked as failed (last_error={last_cred_error!r}). "
                "Re-validate or re-enter credentials."
            )
        elif total_orders == 0:
            recommendations.append(
                "Credential is valid but no orders found in the database. "
                "Trigger a full sync via GET /orders/sync?brand_id="
                + (brand or "<brand>")
                + "&force_full=true"
            )
        elif not last_sync_at:
            recommendations.append(
                "Orders exist in the database but no sync has been recorded. "
                "Trigger a sync via GET /orders/sync?brand_id=" + (brand or "<brand>")
            )
        elif orders_with_mapping_issues > 0:
            recommendations.append(
                f"{orders_with_mapping_issues} order(s) have mapping issues. "
                "These orders are still synced and visible — mapping issues only affect routing, not fetching."
            )
            recommendations.append("All systems healthy. Orders are current.")
        else:
            recommendations.append("All systems healthy. Orders are current.")

    logger.info(
        "[OrdersSync] debug_endpoint complete brand=%s total_orders=%s credential_valid=%s",
        brand,
        total_orders,
        credential_valid,
    )

    return {
        "ok": True,
        "brand_id": brand,
        "backend_current_time": backend_current_time,
        "leaflink_status": leaflink_status,
        "database_status": database_status,
        "sync_status": sync_status,
        "recommendations": recommendations,
    }
