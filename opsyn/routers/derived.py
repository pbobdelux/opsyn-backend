from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, Query
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


def _safe_total_cents(value: Any) -> int:
    if value is None:
        return 0
    try:
        return int(value)
    except Exception:
        return 0


def _customer_id_from_name(name: str) -> str:
    cleaned = (name or "unknown").strip().lower()
    cleaned = cleaned.replace("&", "and")
    allowed = []
    for ch in cleaned:
        if ch.isalnum():
            allowed.append(ch)
        elif ch in (" ", "-", "_"):
            allowed.append("-")
    slug = "".join(allowed).strip("-")
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug or "unknown-customer"


def _pretty_date(dt: datetime) -> str:
    try:
        return dt.strftime("%b %-d")
    except Exception:
        return dt.strftime("%b %d").replace(" 0", " ")


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


@router.get("/customers")
async def list_customers(
    brand_id: str = Query(..., description="Tenant brand id"),
    db: AsyncSession = Depends(get_db),
):
    stmt = (
        select(Order)
        .where(Order.brand_id == brand_id)
        .order_by(Order.external_created_at.desc().nullslast(), Order.id.desc())
    )
    result = await db.execute(stmt)
    orders = result.scalars().all()

    grouped: dict[str, dict[str, Any]] = {}

    for order in orders:
        name = (order.customer_name or "Unknown Customer").strip() or "Unknown Customer"
        key = _customer_id_from_name(name)

        order_dt = order.external_created_at or order.external_updated_at or order.synced_at
        total_cents = _safe_total_cents(order.total_cents)

        if key not in grouped:
            grouped[key] = {
                "id": key,
                "name": name,
                "license": "",
                "address": "",
                "city": "",
                "state": "",
                "contact_name": "",
                "contact_phone": "",
                "contact_email": "",
                "total_orders": 0,
                "total_revenue_cents": 0,
                "outstanding_balance_cents": 0,
                "last_order_dt": None,
            }

        grouped[key]["total_orders"] += 1
        grouped[key]["total_revenue_cents"] += total_cents
        grouped[key]["outstanding_balance_cents"] += total_cents

        current_last = grouped[key]["last_order_dt"]
        if order_dt and (current_last is None or order_dt > current_last):
            grouped[key]["last_order_dt"] = order_dt

    customers = []
    for customer in grouped.values():
        customers.append(
            {
                "id": customer["id"],
                "name": customer["name"],
                "license": customer["license"],
                "address": customer["address"],
                "city": customer["city"],
                "state": customer["state"],
                "contact_name": customer["contact_name"],
                "contact_phone": customer["contact_phone"],
                "contact_email": customer["contact_email"],
                "total_orders": customer["total_orders"],
                "total_revenue": round(customer["total_revenue_cents"] / 100.0, 2),
                "outstanding_balance": round(customer["outstanding_balance_cents"] / 100.0, 2),
                "last_order_date": _iso(customer["last_order_dt"]),
            }
        )

    customers.sort(key=lambda c: c["last_order_date"] or "", reverse=True)

    return {
        "ok": True,
        "customers": customers,
        "count": len(customers),
        "timestamp": _iso(datetime.now(timezone.utc)),
    }


@router.get("/routes")
async def list_routes(
    brand_id: str = Query(..., description="Tenant brand id"),
    db: AsyncSession = Depends(get_db),
):
    stmt = (
        select(Order)
        .where(Order.brand_id == brand_id)
        .order_by(Order.external_created_at.desc().nullslast(), Order.id.desc())
    )
    result = await db.execute(stmt)
    orders = result.scalars().all()

    buckets: dict[str, dict[str, Any]] = {}

    for order in orders:
        status = (order.status or "unknown").strip().lower()
        if status not in OPEN_STATUSES:
            continue

        route_dt = order.external_created_at or order.external_updated_at or order.synced_at
        if route_dt is None:
            continue

        bucket = route_dt.date().isoformat()
        if bucket not in buckets:
            route_day = datetime.combine(route_dt.date(), datetime.min.time(), tzinfo=timezone.utc)
            buckets[bucket] = {
                "id": f"route-{bucket}",
                "name": f"Deliveries · {_pretty_date(route_day)}",
                "status": "pending",
                "created_date": _iso(route_day),
                "assigned_driver_id": None,
                "assigned_driver_name": None,
                "assignment_status": "unassigned",
                "stops": [],
            }

        order_id = str(order.id or order.external_order_id or "")
        if not order_id:
            continue

        stop = {
            "id": f"stop-{order_id}",
            "name": (order.customer_name or "Unknown Customer").strip() or "Unknown Customer",
            "address": "",
            "status": "pending",
            "stop_type": "delivery",
            "latitude": None,
            "longitude": None,
            "items": [],
            "outcome": None,
            "failure_reason": None,
            "proof_of_delivery": None,
        }

        buckets[bucket]["stops"].append(stop)

    routes = list(buckets.values())
    routes.sort(key=lambda r: r["created_date"] or "", reverse=True)

    return {
        "ok": True,
        "routes": routes,
        "count": len(routes),
        "timestamp": _iso(datetime.now(timezone.utc)),
    }