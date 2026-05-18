"""
Mobile API routes — optimised for iOS app consumption.

Endpoints:
  GET /api/mobile/orders   — paginated order list for a brand.
                             raw_payload excluded by default; pass
                             ?include_raw=true to opt in.

Performance notes:
  - raw_payload is a large JSONB column; excluding it by default cuts
    per-row serialisation cost significantly for large result sets.
  - Sync-status aggregates use raw SQL to avoid loading full ORM objects.
  - Per-endpoint timing is logged with [MOBILE_*] prefix markers.
"""

import logging
import time
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from utils.json_utils import make_json_safe

logger = logging.getLogger("mobile")

router = APIRouter(prefix="/api/mobile", tags=["mobile"])

# Maximum rows the mobile API will return in a single page.
MOBILE_MAX_LIMIT = 500


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_str(value: Any, max_len: int = 500) -> Optional[str]:
    if value is None:
        return None
    return str(value)[:max_len]


# ---------------------------------------------------------------------------
# GET /api/mobile/orders
# ---------------------------------------------------------------------------

@router.get("/orders")
async def mobile_orders(
    brand_id: str = Query(..., description="Brand identifier"),
    limit: int = Query(50, ge=1, le=MOBILE_MAX_LIMIT, description="Max rows to return"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    include_raw: bool = Query(False, description="Include raw_payload field (large — opt-in only)"),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """
    Paginated order list for a brand, optimised for mobile list display.

    Always returns: id, brand_id, external_order_id, order_number,
    customer_name, status, total_cents, amount, item_count, unit_count,
    line_items_json, source, review_status, sync_status, delivery_status,
    payment_status, external_created_at, external_updated_at, synced_at,
    last_synced_at, created_at, updated_at.

    raw_payload is excluded by default to keep response sizes small.
    Pass ?include_raw=true to include it (e.g. for debugging).
    """
    t_start = time.monotonic()
    logger.info(
        "[MOBILE_REQUEST_START] endpoint=orders brand_id=%s limit=%d offset=%d include_raw=%s",
        brand_id,
        limit,
        offset,
        include_raw,
    )

    try:
        # ------------------------------------------------------------------
        # Build column list — conditionally include raw_payload
        # ------------------------------------------------------------------
        raw_col = ", raw_payload" if include_raw else ""

        # ------------------------------------------------------------------
        # Main query — raw SQL for full control over selected columns
        # ------------------------------------------------------------------
        t_query = time.monotonic()
        rows_result = await db.execute(
            text(f"""
                SELECT
                    id,
                    brand_id,
                    external_order_id,
                    order_number,
                    customer_name,
                    status,
                    total_cents,
                    amount,
                    item_count,
                    unit_count,
                    line_items_json,
                    source,
                    review_status,
                    sync_status,
                    delivery_status,
                    payment_status,
                    external_created_at,
                    external_updated_at,
                    synced_at,
                    last_synced_at,
                    created_at,
                    updated_at
                    {raw_col}
                FROM orders
                WHERE brand_id = :brand_id
                ORDER BY created_at DESC
                LIMIT :limit
                OFFSET :offset
            """),
            {"brand_id": brand_id, "limit": limit, "offset": offset},
        )
        rows = rows_result.fetchall()
        query_ms = round((time.monotonic() - t_query) * 1000)
        logger.info(
            "[MOBILE_QUERY_DONE] endpoint=orders brand_id=%s rows=%d query_ms=%d",
            brand_id,
            len(rows),
            query_ms,
        )

        # ------------------------------------------------------------------
        # Total count — separate lightweight query
        # ------------------------------------------------------------------
        t_count = time.monotonic()
        count_result = await db.execute(
            text("SELECT COUNT(*) FROM orders WHERE brand_id = :brand_id"),
            {"brand_id": brand_id},
        )
        total = count_result.scalar() or 0
        count_ms = round((time.monotonic() - t_count) * 1000)
        logger.info(
            "[MOBILE_COUNT_DONE] endpoint=orders brand_id=%s total=%d count_ms=%d",
            brand_id,
            total,
            count_ms,
        )

        # ------------------------------------------------------------------
        # Sync-status summary — raw SQL aggregate, no ORM object loading
        # ------------------------------------------------------------------
        t_sync = time.monotonic()
        try:
            sync_result = await db.execute(
                text("""
                    SELECT sync_status, COUNT(*) AS cnt
                    FROM orders
                    WHERE brand_id = :brand_id
                    GROUP BY sync_status
                """),
                {"brand_id": brand_id},
            )
            sync_summary: dict[str, int] = {
                row[0]: int(row[1]) for row in sync_result.fetchall()
            }
        except Exception as sync_exc:
            logger.warning(
                "[MOBILE_SYNC_SUMMARY_WARN] brand_id=%s error=%s",
                brand_id,
                str(sync_exc)[:200],
            )
            sync_summary = {}
        sync_ms = round((time.monotonic() - t_sync) * 1000)

        # ------------------------------------------------------------------
        # Serialise rows
        # ------------------------------------------------------------------
        t_serial = time.monotonic()
        orders = []
        keys = [
            "id", "brand_id", "external_order_id", "order_number",
            "customer_name", "status", "total_cents", "amount",
            "item_count", "unit_count", "line_items_json", "source",
            "review_status", "sync_status", "delivery_status",
            "payment_status", "external_created_at", "external_updated_at",
            "synced_at", "last_synced_at", "created_at", "updated_at",
        ]
        if include_raw:
            keys.append("raw_payload")

        for row in rows:
            orders.append(make_json_safe(dict(zip(keys, row))))

        serial_ms = round((time.monotonic() - t_serial) * 1000)

        elapsed_ms = round((time.monotonic() - t_start) * 1000)
        logger.info(
            "[MOBILE_RESPONSE_SENT] endpoint=orders brand_id=%s "
            "rows=%d total=%d elapsed_ms=%d "
            "query_ms=%d count_ms=%d sync_ms=%d serial_ms=%d",
            brand_id,
            len(orders),
            total,
            elapsed_ms,
            query_ms,
            count_ms,
            sync_ms,
            serial_ms,
        )

        return {
            "ok": True,
            "brand_id": brand_id,
            "orders": orders,
            "count": len(orders),
            "total": total,
            "limit": limit,
            "offset": offset,
            "include_raw": include_raw,
            "sync_summary": sync_summary,
            "timestamp": _utc_now_iso(),
        }

    except Exception as exc:
        elapsed_ms = round((time.monotonic() - t_start) * 1000)
        logger.error(
            "[MOBILE_ERROR] endpoint=orders brand_id=%s elapsed_ms=%d error=%s",
            brand_id,
            elapsed_ms,
            str(exc)[:300],
            exc_info=True,
        )
        return {
            "ok": False,
            "brand_id": brand_id,
            "orders": [],
            "count": 0,
            "total": 0,
            "limit": limit,
            "offset": offset,
            "include_raw": include_raw,
            "sync_summary": {},
            "error": str(exc)[:300],
            "timestamp": _utc_now_iso(),
        }
