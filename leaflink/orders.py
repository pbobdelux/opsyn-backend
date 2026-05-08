import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from database import get_db
from models import Order, OrderLine

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


def derive_blockers(line_items: list[dict[str, Any]], order: "Order | None" = None) -> list[dict[str, str]]:
    """Derive real blockers for an order.

    Only returns blockers when there is a genuine issue:
    - Line items with unknown/unmapped SKUs or unresolved mapping issues
    - Missing customer name (if order object provided)
    - Missing totals (if order object provided)

    Does NOT flag every order as blocked. An order with fully mapped line
    items and complete data returns an empty list.
    """
    blockers: list[dict[str, str]] = []

    # Only flag lines that have an actual mapping problem
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
            "message": f"Unknown SKU on {len(unknown_lines)} line item(s)",
        })

    if order is not None:
        # Missing customer is a real blocker
        if not getattr(order, "customer_name", None):
            blockers.append({
                "type": "missing_customer",
                "message": "Missing customer/account name",
            })

        # Missing totals on a non-zero order is a real blocker
        amount = getattr(order, "amount", None)
        total_cents = getattr(order, "total_cents", None)
        if amount is None and total_cents is None:
            blockers.append({
                "type": "missing_total",
                "message": "Missing order total",
            })

        # Sync health failure is a real blocker
        sync_status = getattr(order, "sync_status", None)
        if sync_status in ("partial", "failed", "error"):
            blockers.append({
                "type": "sync_health",
                "message": f"Sync status: {sync_status}",
            })

    return blockers


def derive_review_status(line_items: list[dict[str, Any]], blockers: list[dict[str, str]], order: "Order | None" = None) -> str:
    """Derive review status for an order.

    Rules (in priority order):
    1. If the order has an explicit review_status stored in DB, use it.
    2. If there are real blockers (mapping issues, missing data, sync failures), return "blocked".
    3. If the order has no line items at all AND is not in a terminal status, return "needs_review".
    4. Otherwise return "ready".

    An order is NOT "needs_review" just because it exists — only when it
    genuinely lacks required data.
    """
    if order is not None and getattr(order, "review_status", None):
        return order.review_status

    if blockers:
        return "blocked"

    # Only flag as needs_review when line items are truly missing AND the order
    # is in an active (non-terminal) status that should have line items.
    if not line_items:
        if order is not None:
            status = (getattr(order, "status", None) or "").lower()
            # Terminal statuses don't need line items reviewed
            terminal_statuses = {"cancelled", "canceled", "rejected", "voided", "void"}
            if status not in terminal_statuses:
                return "needs_review"
        else:
            return "needs_review"

    return "ready"


def serialize_order(order: Order) -> dict[str, Any]:
    """Serialize a single Order ORM object to a JSON-safe dict.

    Reusable across endpoints — call this on each order in a paginated
    result set rather than loading all orders into memory at once.
    """
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

    blockers = derive_blockers(line_items, order)
    review_status = derive_review_status(line_items, blockers, order)

    # Build sync_health object per order
    _sync_status = order.sync_status or "ok"
    _last_synced = order.last_synced_at or order.synced_at
    _missing_fields: list[str] = []
    if not order.customer_name:
        _missing_fields.append("customer_name")
    if order.amount is None and order.total_cents is None:
        _missing_fields.append("amount")
    if not line_items:
        _missing_fields.append("line_items")
    if not order.status:
        _missing_fields.append("status")

    # Determine sync_health status: partial if required fields missing, failed if sync failed
    if _sync_status in ("failed", "error"):
        _health_status = "failed"
    elif _missing_fields:
        _health_status = "partial"
    else:
        _health_status = "ok"

    sync_health = {
        "status": _health_status,
        "missing_fields": _missing_fields,
        "last_synced_at": _last_synced,
        "last_error": None,
    }

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
        "sync_status": _sync_status,
        "sync_health": sync_health,
        "last_synced_at": _last_synced,
        "source": order.source,
        "external_created_at": order.external_created_at,
        "external_updated_at": order.external_updated_at,
        "created_at": order.created_at,
        "updated_at": order.updated_at,
        # Dispatch fields
        "assigned_driver_id": getattr(order, "assigned_driver_id", None),
        "assigned_driver_name": getattr(order, "assigned_driver_name", None),
        "delivery_status": getattr(order, "delivery_status", "pending"),
        "delivery_date": getattr(order, "delivery_date", None),
        "route_number": getattr(order, "route_number", None),
        "route_id": getattr(order, "route_id", None),
        "driver_note": getattr(order, "driver_note", None),
        "delivery_instructions": getattr(order, "delivery_instructions", None),
        # AR fields
        "payment_status": getattr(order, "payment_status", "unpaid"),
        "amount_paid": float(order.amount_paid) if getattr(order, "amount_paid", None) is not None else 0,
        "balance_due": float(order.balance_due) if getattr(order, "balance_due", None) is not None else 0,
        "due_date": getattr(order, "due_date", None),
        "days_overdue": getattr(order, "days_overdue", 0),
        "invoice_number": getattr(order, "invoice_number", None),
        "ar_note": getattr(order, "ar_note", None),
    }


@router.get("/orders")
async def get_orders(db: AsyncSession = Depends(get_db)):
    logger.info("[OrdersAPI] request endpoint=/leaflink/orders")
    logger.info("leaflink: get_orders request start")
    try:
        logger.info("leaflink: get_orders db_query_start")
        result = await db.execute(
            select(Order)
            .options(selectinload(Order.lines))
            .order_by(Order.external_updated_at.desc().nullslast(), Order.updated_at.desc())
        )
        orders = result.scalars().all()
    except Exception as exc:
        logger.error("leaflink: get_orders db_query_failed error=%s", exc, exc_info=True)
        raise

    # Count total orders in DB (all brands)
    try:
        total_count_result = await db.execute(select(func.count(Order.id)))
        total_in_database = total_count_result.scalar_one() or 0
    except Exception:
        total_in_database = len(orders)

    results: list[dict[str, Any]] = []
    review_status_counts: dict[str, int] = {}

    # Track freshness dates across all orders
    newest_order_date: datetime | None = None
    oldest_order_date: datetime | None = None

    for order in orders:
        serialized = serialize_order(order)
        review_status = serialized["review_status"]
        review_status_counts[review_status] = review_status_counts.get(review_status, 0) + 1

        # Track newest/oldest order dates
        order_date = order.external_updated_at or order.external_created_at or order.updated_at
        if order_date is not None:
            if newest_order_date is None or order_date > newest_order_date:
                newest_order_date = order_date
            if oldest_order_date is None or order_date < oldest_order_date:
                oldest_order_date = order_date

        results.append(serialized)

    refreshed_at = datetime.now(timezone.utc).isoformat()
    newest_order_date_iso = newest_order_date.isoformat() if newest_order_date else None
    oldest_order_date_iso = oldest_order_date.isoformat() if oldest_order_date else None

    logger.info("[OrdersAPI] db_count=%s", len(results))
    logger.info("[OrdersAPI] newest_order_date=%s", newest_order_date_iso)
    logger.info("[OrdersAPI] refreshed_at=%s", refreshed_at)
    logger.info("[OrdersAPI] returning_mock=false")
    logger.info("[OrdersAPI] returning_cache=false")

    if not results:
        logger.info("leaflink: get_orders no_orders_found (not an error)")
    else:
        logger.info(
            "leaflink: get_orders results count=%s review_status_distribution=%s",
            len(results),
            review_status_counts,
        )

    return {
        "ok": True,
        "success": True,
        "count": len(results),
        "total_in_database": total_in_database,
        "newest_order_date": newest_order_date_iso,
        "oldest_order_date": oldest_order_date_iso,
        "refreshed_at": refreshed_at,
        "source": "database",
        "orders": results,
        "sync_metadata": {
            "returning_mock": False,
            "returning_cache": False,
        },
    }



@router.get("/orders/id/{order_id}")
async def get_order_detail(order_id: int, db: AsyncSession = Depends(get_db)):
    logger.info("[OrdersAPI] request endpoint=/leaflink/orders/id/%s", order_id)
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
            "ok": False,
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

    blockers = derive_blockers(line_items, order)
    review_status = derive_review_status(line_items, blockers, order)

    logger.info(
        "leaflink: get_order_detail serialized order_id=%s line_items=%s review_status=%s",
        order_id,
        len(line_items),
        review_status,
    )

    refreshed_at = datetime.now(timezone.utc).isoformat()
    order_date = order.external_updated_at or order.external_created_at or order.updated_at
    newest_order_date_iso = order_date.isoformat() if order_date else None

    logger.info("[OrdersAPI] db_count=1")
    logger.info("[OrdersAPI] newest_order_date=%s", newest_order_date_iso)
    logger.info("[OrdersAPI] refreshed_at=%s", refreshed_at)
    logger.info("[OrdersAPI] returning_mock=false")
    logger.info("[OrdersAPI] returning_cache=false")

    return {
        "ok": True,
        "success": True,
        "count": 1,
        "total_in_database": 1,
        "newest_order_date": newest_order_date_iso,
        "oldest_order_date": newest_order_date_iso,
        "refreshed_at": refreshed_at,
        "source": "database",
        "sync_metadata": {
            "returning_mock": False,
            "returning_cache": False,
        },
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
async def get_orders_legacy(db: AsyncSession = Depends(get_db)):
    """
    Backward-compatible route so older frontend code that still calls
    /leaflink/orders keeps working while you transition to /orders.
    """
    return await get_orders(db)