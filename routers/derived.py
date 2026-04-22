from datetime import datetime, timezone
from typing import Any, Iterable
from collections import defaultdict

from fastapi import APIRouter, Query, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import Order

router = APIRouter(tags=["derived"])


def _iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


def _parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        text = str(value)
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)
    except Exception:
        return None


def _customer_key(order: Order) -> str:
    name = (order.customer_name or "Unknown").strip().lower()
    return name.replace(" ", "-")


def _total_cents(order: Order) -> int:
    return int(order.total_cents or 0)


OPEN_STATUSES = {
    "submitted",
    "pending",
    "accepted",
    "approved",
    "processing",
    "in_progress",
    "shipped",
    "in_transit",
    "unknown",
}


# ======================
# CUSTOMERS
# ======================

@router.get("/customers")
async def list_customers(
    brand_id: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(Order).where(Order.brand_id == brand_id)
    result = await db.execute(stmt)
    orders = result.scalars().all()

    grouped: dict[str, list[Order]] = defaultdict(list)

    for order in orders:
        grouped[_customer_key(order)].append(order)

    customers = []

    for key, group in grouped.items():
        first = group[0]

        total_orders = len(group)
        total_cents = sum(_total_cents(o) for o in group)

        last_dt = max(
            (o.external_created_at or o.external_updated_at or o.synced_at for o in group),
            default=None,
        )

        customers.append({
            "id": key,
            "name": first.customer_name or "Unknown",
            "license": "",
            "address": "",
            "city": "",
            "state": "",
            "contact_name": "",
            "contact_phone": "",
            "contact_email": "",
            "total_orders": total_orders,
            "total_revenue": round(total_cents / 100.0, 2),
            "outstanding_balance": round(total_cents / 100.0, 2),
            "last_order_date": _iso(last_dt),
        })

    customers.sort(key=lambda x: x["last_order_date"] or "", reverse=True)

    return {
        "ok": True,
        "customers": customers,
        "count": len(customers),
        "timestamp": _iso(datetime.now(timezone.utc)),
    }


# ======================
# ROUTES
# ======================

@router.get("/routes")
async def list_routes(
    brand_id: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(Order).where(Order.brand_id == brand_id)
    result = await db.execute(stmt)
    orders = result.scalars().all()

    buckets: dict[str, list[Order]] = defaultdict(list)

    for order in orders:
        status = (order.status or "unknown").lower()
        if status not in OPEN_STATUSES:
            continue

        dt = order.external_created_at or order.external_updated_at or order.synced_at
        if not dt:
            continue

        bucket = dt.date().isoformat()
        buckets[bucket].append(order)

    routes = []

    for bucket, group in buckets.items():
        stops = []

        for order in group:
            order_id = str(order.id or order.external_order_id or "")

            stops.append({
                "id": f"stop-{order_id}",
                "name": order.customer_name or "Unknown",
                "address": "",
                "status": "pending",
                "stop_type": "delivery",
                "latitude": None,
                "longitude": None,
                "items": [],
                "outcome": None,
                "failure_reason": None,
                "proof_of_delivery": None,
            })

        if not stops:
            continue

        routes.append({
            "id": f"route-{bucket}",
            "name": f"Deliveries · {bucket}",
            "status": "pending",
            "created_date": _iso(datetime.fromisoformat(bucket).replace(tzinfo=timezone.utc)),
            "assigned_driver_id": None,
            "assigned_driver_name": None,
            "assignment_status": "unassigned",
            "stops": stops,
        })

    return {
        "ok": True,
        "routes": routes,
        "count": len(routes),
        "timestamp": _iso(datetime.now(timezone.utc)),
    }