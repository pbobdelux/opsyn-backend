import base64
import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import Order
from utils.json_utils import make_json_safe

logger = logging.getLogger("orders")

router = APIRouter(prefix="/orders", tags=["orders"])


@router.get("")
def get_orders():
    return {
        "items": [],
        "count": 0,
        "message": "Orders endpoint is live",
    }


@router.get("/sync")
async def orders_sync(
    request: Request,
    brand_id: Optional[str] = Query(None, description="Brand slug or ID to filter orders (e.g., 'noble-nectar')"),
    brand_slug: Optional[str] = Query(None, description="Alias for brand_id — brand slug or ID"),
    updated_after: Optional[str] = Query(None, description="ISO timestamp - only return orders updated after this time"),
    cursor: Optional[str] = Query(None, description="Pagination cursor from previous response"),
    limit: int = Query(500, ge=1, le=1000, description="Number of orders to return (default 500, max 1000)"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get orders for incremental sync with cursor-based pagination.

    Supports efficient incremental sync by:
    - Returning only orders updated after a given timestamp
    - Using cursor-based pagination to avoid offset issues
    - Sorting by updated_at for consistent ordering
    - Including stable order ID fields for deduplication

    Query params:
    - brand_id: Brand slug or ID to filter orders (e.g., "noble-nectar")
    - brand_slug: Alias for brand_id
    - updated_after: ISO timestamp (e.g., "2026-04-25T12:30:00Z")
    - cursor: Pagination cursor from previous response
    - limit: Number of orders (1-1000, default 500)

    Response includes:
    - orders: Array of order objects
    - next_cursor: Cursor for next page (if has_more=true)
    - has_more: Whether more orders exist
    - server_time: Current server time
    - sync_version: Sync protocol version
    - last_synced_at: Timestamp of this sync
    """
    try:
        # Resolve brand filter — accept brand_id, brand_slug, or camelCase brandId
        # from query params. brand_slug is an alias; brandId is tolerated for iOS clients.
        brand_filter: Optional[str] = (
            brand_id
            or brand_slug
            or request.query_params.get("brandId")
        )

        logger.info(
            "[OrdersSync] request_start path=%s brand=%s org=%s updated_after=%s cursor=%s limit=%s",
            request.url.path,
            brand_filter,
            request.headers.get("x-opsyn-org") or request.headers.get("x-org-id"),
            updated_after,
            cursor[:8] + "..." if cursor else None,
            limit,
        )

        # Parse updated_after if provided
        updated_after_dt = None
        if updated_after:
            try:
                updated_after_dt = datetime.fromisoformat(updated_after.replace("Z", "+00:00"))
                logger.info("[OrdersSync] filter updated_after=%s", updated_after_dt.isoformat())
            except ValueError as e:
                logger.error("[OrdersSync] validation_error detail=invalid_updated_after error=%s", e)
                return {
                    "ok": False,
                    "error": "Invalid updated_after timestamp format. Use ISO 8601 (e.g., 2026-04-25T12:30:00Z)",
                    "data_source": "error",
                }

        # Decode cursor if provided
        cursor_id = None
        if cursor:
            try:
                cursor_id = int(base64.b64decode(cursor).decode("utf-8"))
                logger.info("[OrdersSync] cursor_decoded cursor_id=%s", cursor_id)
            except Exception as e:
                logger.error("[OrdersSync] validation_error detail=invalid_cursor error=%s", e)
                return {
                    "ok": False,
                    "error": "Invalid cursor format",
                    "data_source": "error",
                }

        # Build query
        query = select(Order)

        # Filter by brand when provided (brand_id column stores the brand slug/ID)
        if brand_filter:
            query = query.where(Order.brand_id == brand_filter)
            logger.info("[OrdersSync] filter brand=%s", brand_filter)

        # Filter by updated_after
        if updated_after_dt:
            query = query.where(Order.updated_at >= updated_after_dt)

        # Filter by cursor (pagination)
        if cursor_id is not None:
            query = query.where(Order.id > cursor_id)

        # Sort by updated_at ascending for consistent incremental sync
        query = query.order_by(Order.updated_at.asc(), Order.id.asc())

        # Fetch limit + 1 to determine if there are more results
        query_with_limit = query.limit(limit + 1)

        result = await db.execute(query_with_limit)
        orders = result.scalars().all()

        # Determine if there are more results
        has_more = len(orders) > limit
        if has_more:
            orders = orders[:limit]

        # Build response
        orders_data = []
        next_cursor = None

        for order in orders:
            order_dict = {
                "id": order.id,
                "external_order_id": order.external_order_id,
                "order_number": order.order_number,
                "customer_name": order.customer_name,
                "amount": float(order.amount) if order.amount else 0,
                "status": order.status,
                "brand_id": order.brand_id,
                "source": order.source,
                "review_status": order.review_status,
                "item_count": order.item_count,
                "unit_count": order.unit_count,
                "external_created_at": order.external_created_at.isoformat() if order.external_created_at else None,
                "external_updated_at": order.external_updated_at.isoformat() if order.external_updated_at else None,
                "created_at": order.created_at.isoformat() if order.created_at else None,
                "updated_at": order.updated_at.isoformat() if order.updated_at else None,
                "last_synced_at": order.last_synced_at.isoformat() if order.last_synced_at else None,
            }
            orders_data.append(order_dict)

            # Update cursor to last order's ID
            if order == orders[-1]:
                next_cursor = base64.b64encode(str(order.id).encode()).decode()

        server_time = datetime.now(timezone.utc).isoformat()

        logger.info(
            "[OrdersSync] response count=%s has_more=%s brand=%s",
            len(orders_data),
            has_more,
            brand_filter,
        )

        return make_json_safe({
            "ok": True,
            "data_source": "live",
            "orders": orders_data,
            "next_cursor": next_cursor if has_more else None,
            "has_more": has_more,
            "server_time": server_time,
            "sync_version": 1,
            "last_synced_at": server_time,
        })

    except Exception as e:
        logger.error("[OrdersSync] validation_error detail=%s", e, exc_info=True)
        return {
            "ok": False,
            "error": str(e),
            "data_source": "error",
        }