import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from database import get_db
from models import BrandAPICredential, Order, OrderLine

logger = logging.getLogger("leaflink_orders")
router = APIRouter()


def money_to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def cents_to_amount(cents: int | None) -> float | None:
    if cents is None:
        return None
    return round(cents / 100.0, 2)


def serialize_line(line: OrderLine) -> dict[str, Any]:
    unit_price = money_to_float(line.unit_price)
    total_price = money_to_float(line.total_price)

    if unit_price is None and line.unit_price_cents is not None:
        unit_price = cents_to_amount(line.unit_price_cents)

    if total_price is None and line.total_price_cents is not None:
        total_price = cents_to_amount(line.total_price_cents)

    return {
        "id": line.id,
        "sku": line.sku,
        "product_name": line.product_name,
        "quantity": line.quantity or 0,
        "unit_price": unit_price,
        "total_price": total_price,
        "mapped_product_id": line.mapped_product_id,
        "mapping_status": line.mapping_status or "unknown",
        "mapping_issue": line.mapping_issue,
    }


def serialize_json_line(item: dict[str, Any]) -> dict[str, Any]:
    quantity = item.get("quantity") or item.get("qty") or 0
    unit_price = item.get("unit_price")
    total_price = item.get("total_price")

    if unit_price is None and item.get("unit_price_cents") is not None:
        unit_price = cents_to_amount(item.get("unit_price_cents"))

    if total_price is None and item.get("total_price_cents") is not None:
        total_price = cents_to_amount(item.get("total_price_cents"))

    return {
        "id": None,
        "sku": item.get("sku"),
        "product_name": item.get("product_name") or item.get("name"),
        "quantity": quantity,
        "unit_price": unit_price,
        "total_price": total_price,
        "mapped_product_id": item.get("mapped_product_id"),
        "mapping_status": item.get("mapping_status", "unknown"),
        "mapping_issue": item.get("mapping_issue"),
    }


def build_line_items(order: Order) -> list[dict[str, Any]]:
    # Try OrderLine table first (most reliable)
    if getattr(order, "lines", None):
        return [serialize_line(line) for line in order.lines]

    # Try line_items_json (normalized data from sync)
    raw = order.line_items_json
    if isinstance(raw, list):
        return [serialize_json_line(item) for item in raw if isinstance(item, dict)]

    if isinstance(raw, dict):
        nested = raw.get("line_items")
        if isinstance(nested, list):
            return [serialize_json_line(item) for item in nested if isinstance(item, dict)]

    # Fallback: extract from raw_payload if line_items_json is empty
    if order.raw_payload and isinstance(order.raw_payload, dict):
        raw_payload = order.raw_payload
        # Try multiple field names
        candidate = (
            raw_payload.get("line_items")
            or raw_payload.get("items")
            or raw_payload.get("order_items")
            or raw_payload.get("products")
            or raw_payload.get("ordered_items")
            or []
        )
        if isinstance(candidate, list):
            items = [serialize_json_line(item) for item in candidate if isinstance(item, dict)]
            if items:
                logger.info(
                    "leaflink: build_line_items fallback_from_raw_payload order_id=%s count=%s",
                    order.id,
                    len(items),
                )
                return items

    return []


def derive_blockers(line_items: list[dict[str, Any]]) -> list[dict[str, str]]:
    blockers: list[dict[str, str]] = []

    unknown_lines = [
        item for item in line_items
        if (
            not item.get("sku")
            or item.get("mapping_status") in {"unknown", "unmapped", None}
            or item.get("mapping_issue")
        )
    ]

    if unknown_lines:
        blockers.append({
            "type": "mapping_issue",
            "message": "Unknown SKU",
        })

    return blockers


def derive_review_status(line_items: list[dict[str, Any]], blockers: list[dict[str, str]], order: Order) -> str:
    if order.review_status:
        return order.review_status
    if blockers:
        return "blocked"
    if not line_items:
        return "needs_review"
    return "ready"


def _serialize_order(order: Order) -> dict[str, Any]:
    """Serialize an Order ORM object to a response dict."""
    line_items = build_line_items(order)

    amount = money_to_float(order.amount)
    if amount is None and order.total_cents is not None:
        amount = cents_to_amount(order.total_cents)

    item_count = order.item_count
    if item_count is None:
        item_count = len(line_items)

    unit_count = order.unit_count
    if unit_count is None:
        unit_count = sum((item.get("quantity") or 0) for item in line_items)

    blockers = derive_blockers(line_items)
    review_status = derive_review_status(line_items, blockers, order)

    return {
        "id": order.id,
        "external_id": order.external_order_id,
        "order_number": order.order_number,
        "customer_name": order.customer_name,
        "status": order.status,
        "amount": amount,
        "item_count": item_count,
        "unit_count": unit_count,
        "line_items": line_items,
        "review_status": review_status,
        "blockers": blockers,
        "sync_status": order.sync_status or "ok",
        "last_synced_at": order.last_synced_at or order.synced_at,
        "source": order.source,
        "brand_id": order.brand_id,
        "external_created_at": order.external_created_at,
        "external_updated_at": order.external_updated_at,
        "created_at": order.created_at,
        "updated_at": order.updated_at,
    }


async def _fetch_orders_from_db(
    db: AsyncSession,
    brand: Optional[str],
) -> tuple[list[Order], int, Optional[datetime]]:
    """Fetch orders from the database, filtered by brand if provided.

    Returns:
        (orders, db_count, newest_order_date)
    """
    query = (
        select(Order)
        .options(selectinload(Order.lines))
        .order_by(Order.external_updated_at.desc().nullslast(), Order.updated_at.desc())
    )
    if brand:
        query = query.where(Order.brand_id == brand)

    result = await db.execute(query)
    orders = result.scalars().all()

    # Count total for this brand (may differ from len(orders) if pagination added later)
    count_query = select(func.count(Order.id))
    if brand:
        count_query = count_query.where(Order.brand_id == brand)
    count_result = await db.execute(count_query)
    db_count = count_result.scalar_one()

    # Newest order date
    newest_order_date: Optional[datetime] = None
    if orders:
        for order in orders:
            candidate = order.external_updated_at or order.updated_at
            if candidate and (newest_order_date is None or candidate > newest_order_date):
                newest_order_date = candidate

    return list(orders), db_count, newest_order_date


async def _attempt_leaflink_refresh(
    db: AsyncSession,
    brand: str,
) -> dict[str, Any]:
    """Attempt to refresh orders from LeafLink for the given brand.

    Returns a dict with keys: ok, error, orders_fetched, pages_fetched.
    Raises asyncio.TimeoutError if the caller's wait_for fires.
    """
    from services.leaflink_client import LeafLinkClient
    from services.leaflink_sync import sync_leaflink_orders

    # Look up credentials for this brand
    cred_result = await db.execute(
        select(BrandAPICredential).where(
            BrandAPICredential.brand_id == brand,
            BrandAPICredential.integration_name == "leaflink",
            BrandAPICredential.is_active == True,
        )
    )
    cred = cred_result.scalar_one_or_none()

    if not cred:
        logger.info("[OrdersAPI] leaflink_refresh_skipped brand=%s reason=no_credentials", brand)
        return {"ok": False, "error": "no_credentials", "orders_fetched": 0, "pages_fetched": 0}

    api_key: str = cred.api_key or ""
    company_id: str = cred.company_id or ""

    client = LeafLinkClient(api_key=api_key, company_id=company_id, brand_id=brand)

    # fetch_recent_orders is synchronous (uses requests) — run in thread pool
    loop = asyncio.get_running_loop()
    fetch_result = await loop.run_in_executor(
        None,
        lambda: client.fetch_recent_orders(max_pages=5, normalize=True, brand=brand),
    )
    orders_from_leaflink = fetch_result["orders"]
    pages_fetched = fetch_result["pages_fetched"]

    logger.info(
        "[OrdersAPI] leaflink_fetch_done brand=%s orders=%s pages=%s",
        brand,
        len(orders_from_leaflink),
        pages_fetched,
    )

    # Upsert into DB
    async with db.begin():
        sync_result = await sync_leaflink_orders(db, brand, orders_from_leaflink, pages_fetched=pages_fetched)

    return {
        "ok": sync_result.get("ok", False),
        "error": sync_result.get("error"),
        "orders_fetched": sync_result.get("orders_fetched", len(orders_from_leaflink)),
        "pages_fetched": pages_fetched,
    }


@router.get("/orders/health")
async def orders_health(
    brand: Optional[str] = Query(None, description="Brand slug to check"),
    db: AsyncSession = Depends(get_db),
):
    """Lightweight health check — reads only from the database, no external calls.

    Returns within 2 seconds. Useful for monitoring and debugging.
    """
    logger.info("[OrdersHealth] request_start brand=%s", brand)

    # Count and newest order date
    count_query = select(func.count(Order.id))
    if brand:
        count_query = count_query.where(Order.brand_id == brand)
    count_result = await db.execute(count_query)
    db_count = count_result.scalar_one()

    newest_order_date: Optional[datetime] = None
    if db_count > 0:
        date_query = select(Order.external_updated_at, Order.updated_at).order_by(
            Order.external_updated_at.desc().nullslast(), Order.updated_at.desc()
        )
        if brand:
            date_query = date_query.where(Order.brand_id == brand)
        date_query = date_query.limit(1)
        date_result = await db.execute(date_query)
        row = date_result.first()
        if row:
            newest_order_date = row[0] or row[1]

    # Check if LeafLink credentials exist for this brand
    leaflink_configured = False
    if brand:
        cred_result = await db.execute(
            select(BrandAPICredential).where(
                BrandAPICredential.brand_id == brand,
                BrandAPICredential.integration_name == "leaflink",
                BrandAPICredential.is_active == True,
            )
        )
        leaflink_configured = cred_result.scalar_one_or_none() is not None

    timestamp = datetime.now(timezone.utc).isoformat()

    logger.info(
        "[OrdersHealth] response_returned brand=%s db_count=%s",
        brand,
        db_count,
    )

    return {
        "ok": True,
        "brand": brand,
        "db_count": db_count,
        "newest_order_date": newest_order_date.isoformat() if newest_order_date else None,
        "leaflink_configured": leaflink_configured,
        "timestamp": timestamp,
    }


@router.get("/orders")
async def get_orders(
    brand: Optional[str] = Query(None, description="Brand slug to filter orders"),
    db: AsyncSession = Depends(get_db),
):
    """Return orders with optional live refresh from LeafLink.

    Always returns within 10 seconds:
    - Fetches orders from the database immediately (no timeout needed).
    - Attempts a LeafLink refresh with a 5-second timeout.
    - If LeafLink times out or fails, returns database orders with
      source="database_fallback" and live_refresh_failed=True.
    """
    logger.info("[OrdersAPI] request_start brand=%s", brand)

    # ------------------------------------------------------------------ #
    # Step 1: Fetch from database (fast, no timeout needed)               #
    # ------------------------------------------------------------------ #
    logger.info("[OrdersAPI] db_fetch_start")
    try:
        db_orders, db_count, newest_order_date = await _fetch_orders_from_db(db, brand)
    except Exception as exc:
        logger.error("[OrdersAPI] db_fetch_error error=%s", exc, exc_info=True)
        raise

    logger.info(
        "[OrdersAPI] db_fetch_done count=%s newest_order_date=%s",
        db_count,
        newest_order_date.isoformat() if newest_order_date else None,
    )

    # ------------------------------------------------------------------ #
    # Step 2: Attempt LeafLink refresh with 5-second timeout              #
    # ------------------------------------------------------------------ #
    live_refresh_failed = False
    refresh_error: Optional[str] = None
    leaflink_timeout = False
    leaflink_configured = False
    source = "database_fallback"

    if brand:
        logger.info("[OrdersAPI] leaflink_refresh_start")
        try:
            refresh_result = await asyncio.wait_for(
                _attempt_leaflink_refresh(db, brand),
                timeout=5.0,
            )

            if refresh_result.get("error") == "no_credentials":
                # No credentials — not a failure, just skip refresh
                logger.info("[OrdersAPI] leaflink_refresh_skipped brand=%s reason=no_credentials", brand)
                leaflink_configured = False
                source = "database_fallback"
                live_refresh_failed = False
            elif refresh_result.get("ok"):
                leaflink_configured = True
                source = "live"
                # Re-fetch from DB to get the freshly upserted orders
                logger.info("[OrdersAPI] db_fetch_start (post-refresh)")
                db_orders, db_count, newest_order_date = await _fetch_orders_from_db(db, brand)
                logger.info(
                    "[OrdersAPI] db_fetch_done (post-refresh) count=%s newest_order_date=%s",
                    db_count,
                    newest_order_date.isoformat() if newest_order_date else None,
                )
            else:
                leaflink_configured = True
                live_refresh_failed = True
                refresh_error = refresh_result.get("error") or "LeafLink refresh failed"
                logger.info(
                    "[OrdersAPI] leaflink_refresh_error error=%s",
                    refresh_error,
                )

        except asyncio.TimeoutError:
            leaflink_timeout = True
            live_refresh_failed = True
            leaflink_configured = True  # credentials likely exist if we tried
            refresh_error = "LeafLink refresh timed out after 5 seconds"
            logger.warning("[OrdersAPI] leaflink_refresh_timeout brand=%s", brand)

        except Exception as exc:
            live_refresh_failed = True
            leaflink_configured = True
            refresh_error = str(exc)
            logger.error("[OrdersAPI] leaflink_refresh_error error=%s", exc, exc_info=True)
    else:
        # No brand filter — skip LeafLink refresh, serve from DB
        source = "database_fallback"

    # ------------------------------------------------------------------ #
    # Step 3: Serialize and return                                         #
    # ------------------------------------------------------------------ #
    results: list[dict[str, Any]] = []
    review_status_counts: dict[str, int] = {}

    for order in db_orders:
        serialized = _serialize_order(order)
        rs = serialized["review_status"]
        review_status_counts[rs] = review_status_counts.get(rs, 0) + 1
        results.append(serialized)

    if not results:
        logger.info("[OrdersAPI] no_orders_found brand=%s (not an error)", brand)
    else:
        logger.info(
            "[OrdersAPI] serialized count=%s review_status_distribution=%s",
            len(results),
            review_status_counts,
        )

    refreshed_at = datetime.now(timezone.utc).isoformat()

    logger.info(
        "[OrdersAPI] response_returned source=%s count=%s",
        source,
        len(results),
    )

    return {
        "ok": True,
        "source": source,
        "live_refresh_failed": live_refresh_failed,
        "error": refresh_error,
        "refreshed_at": refreshed_at,
        "count": len(results),
        "orders": results,
        "sync_metadata": {
            "db_count": db_count,
            "newest_order_date": newest_order_date.isoformat() if newest_order_date else None,
            "leaflink_configured": leaflink_configured,
            "leaflink_timeout": leaflink_timeout,
        },
        # Legacy fields for backward compatibility
        "success": True,
    }


@router.get("/orders/id/{order_id}")
async def get_order_detail(order_id: int, db: AsyncSession = Depends(get_db)):
    logger.info("leaflink: get_order_detail request order_id=%s", order_id)
    result = await db.execute(
        select(Order)
        .options(selectinload(Order.lines))
        .where(Order.id == order_id)
    )
    order = result.scalar_one_or_none()

    if not order:
        logger.info("leaflink: get_order_detail not_found order_id=%s", order_id)
        return {
            "success": False,
            "error": "Order not found",
        }

    logger.info(
        "leaflink: get_order_detail found order_id=%s external_id=%s status=%s",
        order_id,
        order.external_order_id,
        order.status,
    )

    line_items = build_line_items(order)

    amount = money_to_float(order.amount)
    if amount is None and order.total_cents is not None:
        amount = cents_to_amount(order.total_cents)

    item_count = order.item_count if order.item_count is not None else len(line_items)
    unit_count = order.unit_count if order.unit_count is not None else sum((item.get("quantity") or 0) for item in line_items)

    blockers = derive_blockers(line_items)
    review_status = derive_review_status(line_items, blockers, order)

    logger.info(
        "leaflink: get_order_detail serialized order_id=%s line_items=%s review_status=%s",
        order_id,
        len(line_items),
        review_status,
    )

    return {
        "success": True,
        "order": {
            "id": order.id,
            "external_id": order.external_order_id,
            "order_number": order.order_number,
            "customer_name": order.customer_name,
            "status": order.status,
            "amount": amount,
            "item_count": item_count,
            "unit_count": unit_count,
            "line_items": line_items,
            "review_status": review_status,
            "blockers": blockers,
            "sync_status": order.sync_status or "ok",
            "last_synced_at": order.last_synced_at or order.synced_at,
            "source": order.source,
            "external_created_at": order.external_created_at,
            "external_updated_at": order.external_updated_at,
            "created_at": order.created_at,
            "updated_at": order.updated_at,
        }
    }


@router.get("/leaflink/orders")
async def get_orders_legacy(
    brand: Optional[str] = Query(None, description="Brand slug to filter orders"),
    db: AsyncSession = Depends(get_db),
):
    """
    Backward-compatible route so older frontend code that still calls
    /leaflink/orders keeps working while you transition to /orders.
    """
    return await get_orders(brand=brand, db=db)