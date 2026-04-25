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

logger = logging.getLogger("leaflink.orders")

MOCK_MODE = os.getenv("LEAFLINK_MOCK_MODE", "false").lower() == "true"

router = APIRouter()


# ---------------------------------------------------------------------------
# Debug helpers
# ---------------------------------------------------------------------------


def get_debug_info() -> dict[str, Any]:
    """Return LeafLink configuration info for debugging (no secrets)."""
    api_key = os.getenv("LEAFLINK_API_KEY", "")
    return {
        "mock_mode": MOCK_MODE,
        "leaflink_base_url": os.getenv("LEAFLINK_BASE_URL", "https://app.leaflink.com/api/v2"),
        "leaflink_company_id": os.getenv("LEAFLINK_COMPANY_ID", ""),
        "leaflink_api_key_set": bool(api_key),
        "leaflink_api_key_prefix": api_key[:6] + "..." if len(api_key) > 6 else "(not set)",
        "environment": os.getenv("ENVIRONMENT", "production"),
    }


@router.get("/leaflink/debug")
def debug_leaflink():
    """Return LeafLink configuration debug info (no secrets exposed)."""
    logger.info("leaflink/debug: returning debug info")
    try:
        info = get_debug_info()
        return {"ok": True, "debug": info}
    except Exception as exc:
        logger.error("leaflink/debug: failed: %s", exc)
        return {"ok": False, "error": str(exc)}


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


def _mock_orders() -> list[dict[str, Any]]:
    """Return a small set of mock orders for local development / testing."""
    return [
        {
            "id": 1,
            "external_id": "mock-001",
            "order_number": "MOCK-001",
            "customer_name": "Mock Dispensary A",
            "status": "received",
            "amount": 1250.00,
            "item_count": 2,
            "unit_count": 10,
            "line_items": [
                {
                    "id": None,
                    "sku": "SKU-001",
                    "product_name": "Mock Product 1",
                    "quantity": 5,
                    "unit_price": 100.00,
                    "total_price": 500.00,
                    "mapped_product_id": None,
                    "mapping_status": "ready",
                    "mapping_issue": None,
                },
                {
                    "id": None,
                    "sku": "SKU-002",
                    "product_name": "Mock Product 2",
                    "quantity": 5,
                    "unit_price": 150.00,
                    "total_price": 750.00,
                    "mapped_product_id": None,
                    "mapping_status": "ready",
                    "mapping_issue": None,
                },
            ],
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
    ]


@router.get("/orders")
async def get_orders(
    db: AsyncSession = Depends(get_db),
    mock: bool = Query(False, description="Return mock data instead of querying the database"),
):
    use_mock = mock or MOCK_MODE
    logger.info("leaflink/orders: fetching orders mock=%s", use_mock)

    if use_mock:
        mock_results = _mock_orders()
        logger.info("leaflink/orders: returning %d mock orders", len(mock_results))
        return {
            "ok": True,
            "success": True,
            "data_source": "mock",
            "count": len(mock_results),
            "orders": mock_results,
        }

    try:
        logger.info("leaflink: get_orders db_query_start")
        result = await db.execute(
            select(Order)
            .options(selectinload(Order.lines))
            .order_by(Order.external_updated_at.desc().nullslast(), Order.updated_at.desc())
        )
        orders = result.scalars().all()
        logger.info("leaflink/orders: found %d orders in database", len(orders))

        results: list[dict[str, Any]] = []
        review_status_counts: dict[str, int] = {}

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
            review_status_counts[review_status] = review_status_counts.get(review_status, 0) + 1

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
            "data_source": "database",
            "count": len(results),
            "orders": results,
        }

    except Exception as exc:
        logger.error("leaflink/orders: database error: %s", exc)
        return {
            "ok": False,
            "success": False,
            "data_source": "error",
            "error": str(exc),
            "orders": [],
        }


@router.get("/orders/{order_id}")
async def get_order_detail(
    order_id: int,
    db: AsyncSession = Depends(get_db),
    mock: bool = Query(False, description="Return mock data instead of querying the database"),
):
    use_mock = mock or MOCK_MODE
    logger.info("leaflink/orders/%d: fetching order mock=%s", order_id, use_mock)

    if use_mock:
        mock_list = _mock_orders()
        mock_order = next((o for o in mock_list if o["id"] == order_id), None)
        if not mock_order:
            return {
                "ok": False,
                "success": False,
                "data_source": "mock",
                "error": "Order not found",
            }
        return {
            "ok": True,
            "success": True,
            "data_source": "mock",
            "order": mock_order,
        }

    try:
        logger.info("leaflink: get_order_detail request order_id=%s", order_id)
        result = await db.execute(
            select(Order)
            .options(selectinload(Order.lines))
            .where(Order.id == order_id)
        )
        order = result.scalar_one_or_none()

        if not order:
            logger.warning("leaflink/orders/%d: not found", order_id)
            return {
                "ok": False,
                "success": False,
                "data_source": "database",
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
            "ok": True,
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
            },
        }

    except Exception as exc:
        logger.error("leaflink/orders/%d: database error: %s", order_id, exc)
        return {
            "ok": False,
            "success": False,
            "data_source": "error",
            "error": str(exc),
        }


@router.get("/leaflink/orders")
async def get_orders_legacy(
    db: AsyncSession = Depends(get_db),
    mock: bool = Query(False, description="Return mock data instead of querying the database"),
):
    """
    Backward-compatible route so older frontend code that still calls
    /leaflink/orders keeps working while you transition to /orders.
    """
    logger.info("leaflink/orders (legacy route): delegating to get_orders mock=%s", mock)
    return await get_orders(db=db, mock=mock)