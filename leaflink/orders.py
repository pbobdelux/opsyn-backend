import logging
import os
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from database import get_db
from models import Order, OrderLine
from services.leaflink_debug import get_debug_info

logger = logging.getLogger("leaflink_orders")
MOCK_MODE = os.getenv("MOCK_MODE", "false").strip().lower() == "true"

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
    if getattr(order, "lines", None):
        return [serialize_line(line) for line in order.lines]

    raw = order.line_items_json
    if isinstance(raw, list):
        return [serialize_json_line(item) for item in raw if isinstance(item, dict)]

    if isinstance(raw, dict):
        nested = raw.get("line_items")
        if isinstance(nested, list):
            return [serialize_json_line(item) for item in nested if isinstance(item, dict)]

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


@router.get("/debug")
async def get_leaflink_debug(db: AsyncSession = Depends(get_db)):
    logger.info("leaflink_orders: debug endpoint called")
    info = await get_debug_info(db)
    return info


@router.get("/orders")
async def get_orders(
    db: AsyncSession = Depends(get_db),
    mock: bool = Query(default=False, description="Return mock data instead of querying the database"),
):
    use_mock = mock or MOCK_MODE
    logger.info("leaflink_orders: get_orders called mock=%s", use_mock)

    if use_mock:
        logger.info("leaflink_orders: returning mock orders")
        return {
            "success": True,
            "count": 1,
            "data_source": "mock",
            "orders": [
                {
                    "id": 0,
                    "external_id": "MOCK-001",
                    "order_number": "MOCK-001",
                    "customer_name": "Mock Customer",
                    "status": "open",
                    "amount": 100.0,
                    "item_count": 1,
                    "unit_count": 1,
                    "line_items": [],
                    "review_status": "ready",
                    "blockers": [],
                    "sync_status": "ok",
                    "last_synced_at": None,
                    "source": "mock",
                    "external_created_at": None,
                    "external_updated_at": None,
                    "created_at": None,
                    "updated_at": None,
                }
            ],
        }

    try:
        result = await db.execute(
            select(Order)
            .options(selectinload(Order.lines))
            .order_by(Order.external_updated_at.desc().nullslast(), Order.updated_at.desc())
        )
        orders = result.scalars().all()
        logger.info("leaflink_orders: fetched %d orders from database", len(orders))
    except Exception as exc:
        logger.error("leaflink_orders: database error in get_orders: %s", exc)
        return {
            "success": False,
            "count": 0,
            "data_source": "error",
            "error": str(exc),
            "orders": [],
        }

    results: list[dict[str, Any]] = []

    for order in orders:
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

        results.append({
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
        })

    return {
        "success": True,
        "count": len(results),
        "data_source": "database",
        "orders": results,
    }


@router.get("/orders/{order_id}")
async def get_order_detail(order_id: int, db: AsyncSession = Depends(get_db)):
    logger.info("leaflink_orders: get_order_detail called order_id=%s", order_id)

    try:
        result = await db.execute(
            select(Order)
            .options(selectinload(Order.lines))
            .where(Order.id == order_id)
        )
        order = result.scalar_one_or_none()
    except Exception as exc:
        logger.error("leaflink_orders: database error in get_order_detail order_id=%s: %s", order_id, exc)
        return {
            "success": False,
            "data_source": "error",
            "error": str(exc),
        }

    if not order:
        logger.info("leaflink_orders: order not found order_id=%s", order_id)
        return {
            "success": False,
            "data_source": "database",
            "error": "Order not found",
        }

    line_items = build_line_items(order)

    amount = money_to_float(order.amount)
    if amount is None and order.total_cents is not None:
        amount = cents_to_amount(order.total_cents)

    item_count = order.item_count if order.item_count is not None else len(line_items)
    unit_count = order.unit_count if order.unit_count is not None else sum((item.get("quantity") or 0) for item in line_items)

    blockers = derive_blockers(line_items)
    review_status = derive_review_status(line_items, blockers, order)

    return {
        "success": True,
        "data_source": "database",
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