"""
routes/routes.py — V1 Atomic Route Creation

Endpoints:
  POST   /routes          — create route + stops atomically (201)
  GET    /routes          — list routes for a brand (200, never 404)
  GET    /routes/{id}     — fetch single route with stops (200 or 404)
  DELETE /routes/{id}     — remove route + stops (200)
"""

import uuid
from datetime import date, datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, field_validator
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from database import get_db
from models import Order, Route, RouteStop

router = APIRouter(prefix="/routes", tags=["routes"])


# =============================================================================
# Request / Response schemas
# =============================================================================

class StopInput(BaseModel):
    order_id: int
    sequence: int


class RouteCreateRequest(BaseModel):
    brand_id: str
    route_date: str          # YYYY-MM-DD
    driver_id: Optional[str] = None
    stops: list[StopInput]

    @field_validator("stops")
    @classmethod
    def stops_not_empty(cls, v: list[StopInput]) -> list[StopInput]:
        if not v:
            raise ValueError("stops must contain at least one entry")
        return v


# =============================================================================
# Helper: serializers
# =============================================================================

def _serialize_order_summary(order: Order) -> dict[str, Any]:
    amount = getattr(order, "amount", None)
    if amount is not None:
        try:
            amount = float(amount)
        except (TypeError, ValueError):
            amount = 0.0
    else:
        total_cents = getattr(order, "total_cents", None)
        amount = round(total_cents / 100.0, 2) if total_cents is not None else 0.0

    return {
        "id": order.id,
        "customer_name": order.customer_name or "Unknown Customer",
        "delivery_city": None,   # not stored in current schema; placeholder
        "amount": amount,
    }


def _serialize_route_stop(stop: RouteStop) -> dict[str, Any]:
    return {
        "id": str(stop.id),
        "sequence": stop.sequence,
        "order_id": stop.order_id,
        "status": stop.status,
        "order": _serialize_order_summary(stop.order) if stop.order else None,
    }


def _serialize_route(route: Route) -> dict[str, Any]:
    route_date = route.route_date
    if isinstance(route_date, date):
        route_date_str = route_date.isoformat()
    else:
        route_date_str = str(route_date)

    stops_sorted = sorted(route.stops, key=lambda s: s.sequence)

    return {
        "id": str(route.id),
        "brand_id": route.brand_id,
        "status": route.status,
        "driver_id": str(route.driver_id) if route.driver_id else None,
        "route_date": route_date_str,
        "stop_count": len(stops_sorted),
        "created_at": route.created_at.isoformat() if route.created_at else None,
        "updated_at": route.updated_at.isoformat() if route.updated_at else None,
        "stops": [_serialize_route_stop(s) for s in stops_sorted],
    }


# =============================================================================
# Helper: validation
# =============================================================================

def _validate_stop_sequence(stops: list[StopInput]) -> bool:
    """Return True if sequences are exactly 1..N with no gaps or duplicates."""
    seqs = sorted(s.sequence for s in stops)
    return seqs == list(range(1, len(stops) + 1))


async def _validate_orders_for_brand(
    order_ids: list[int],
    brand_id: str,
    db: AsyncSession,
) -> dict[int, Order]:
    """
    Fetch all orders by id and verify they belong to brand_id.
    Returns a mapping {order_id: Order}.
    Raises HTTPException 422 if any order is missing or belongs to a different brand.
    """
    stmt = select(Order).where(Order.id.in_(order_ids))
    result = await db.execute(stmt)
    orders = result.scalars().all()

    found: dict[int, Order] = {o.id: o for o in orders}

    missing = [oid for oid in order_ids if oid not in found]
    if missing:
        raise HTTPException(
            status_code=422,
            detail=f"Orders not found: {missing}",
        )

    wrong_brand = [oid for oid, o in found.items() if o.brand_id != brand_id]
    if wrong_brand:
        raise HTTPException(
            status_code=422,
            detail=f"Orders do not belong to brand '{brand_id}': {wrong_brand}",
        )

    return found


# =============================================================================
# POST /routes — atomic creation
# =============================================================================

@router.post("", status_code=201)
async def create_route(
    body: RouteCreateRequest,
    db: AsyncSession = Depends(get_db),
):
    # --- Validate stop sequence ---
    if not _validate_stop_sequence(body.stops):
        raise HTTPException(
            status_code=422,
            detail="Stop sequences must be contiguous integers starting at 1 (e.g. 1, 2, 3).",
        )

    # --- Validate no duplicate order_ids ---
    order_ids = [s.order_id for s in body.stops]
    if len(order_ids) != len(set(order_ids)):
        raise HTTPException(
            status_code=422,
            detail="Duplicate order_ids found in stops. Each order may appear only once.",
        )

    # --- Validate route_date ---
    try:
        parsed_date = date.fromisoformat(body.route_date)
    except ValueError:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid route_date '{body.route_date}'. Expected YYYY-MM-DD.",
        )

    # --- Validate driver_id format (if provided) ---
    driver_uuid: Optional[uuid.UUID] = None
    if body.driver_id:
        try:
            driver_uuid = uuid.UUID(body.driver_id)
        except ValueError:
            raise HTTPException(
                status_code=422,
                detail=f"Invalid driver_id '{body.driver_id}'. Must be a valid UUID.",
            )

    # --- Validate orders exist and belong to brand ---
    orders_map = await _validate_orders_for_brand(order_ids, body.brand_id, db)

    # --- Atomic transaction: create route + stops ---
    async with db.begin():
        route = Route(
            brand_id=body.brand_id,
            route_date=parsed_date,
            driver_id=driver_uuid,
            status="draft",
            version=1,
        )
        db.add(route)
        await db.flush()   # populate route.id

        for stop_input in body.stops:
            stop = RouteStop(
                route_id=route.id,
                order_id=stop_input.order_id,
                sequence=stop_input.sequence,
                status="pending",
            )
            db.add(stop)

        await db.flush()

    # --- Reload with relationships for response ---
    stmt = (
        select(Route)
        .options(selectinload(Route.stops).selectinload(RouteStop.order))
        .where(Route.id == route.id)
    )
    result = await db.execute(stmt)
    route = result.scalar_one()

    return _serialize_route(route)


# =============================================================================
# GET /routes — list routes for a brand
# =============================================================================

@router.get("")
async def list_routes(
    brand_id: str = Query(..., description="Brand ID to filter routes"),
    route_date: Optional[str] = Query(default=None, description="Filter by date YYYY-MM-DD"),
    status: Optional[str] = Query(default=None, description="Filter by status"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    stmt = (
        select(Route)
        .options(selectinload(Route.stops).selectinload(RouteStop.order))
        .where(Route.brand_id == brand_id)
    )

    if route_date:
        try:
            parsed_date = date.fromisoformat(route_date)
            stmt = stmt.where(Route.route_date == parsed_date)
        except ValueError:
            raise HTTPException(
                status_code=422,
                detail=f"Invalid route_date '{route_date}'. Expected YYYY-MM-DD.",
            )

    if status:
        stmt = stmt.where(Route.status == status)

    # Count total before pagination
    count_stmt = select(func.count()).select_from(stmt.subquery())
    total_result = await db.execute(count_stmt)
    total = total_result.scalar_one()

    stmt = stmt.order_by(Route.created_at.desc()).offset(offset).limit(limit)
    result = await db.execute(stmt)
    routes = result.scalars().all()

    return {
        "items": [_serialize_route(r) for r in routes],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


# =============================================================================
# GET /routes/{id} — fetch single route
# =============================================================================

@router.get("/{route_id}")
async def get_route(
    route_id: str,
    db: AsyncSession = Depends(get_db),
):
    try:
        rid = uuid.UUID(route_id)
    except ValueError:
        raise HTTPException(status_code=422, detail=f"Invalid route id '{route_id}'.")

    stmt = (
        select(Route)
        .options(selectinload(Route.stops).selectinload(RouteStop.order))
        .where(Route.id == rid)
    )
    result = await db.execute(stmt)
    route = result.scalar_one_or_none()

    if not route:
        raise HTTPException(status_code=404, detail=f"Route '{route_id}' not found.")

    return _serialize_route(route)


# =============================================================================
# DELETE /routes/{id} — remove route + stops (cascade)
# =============================================================================

@router.delete("/{route_id}")
async def delete_route(
    route_id: str,
    db: AsyncSession = Depends(get_db),
):
    try:
        rid = uuid.UUID(route_id)
    except ValueError:
        raise HTTPException(status_code=422, detail=f"Invalid route id '{route_id}'.")

    stmt = select(Route).where(Route.id == rid)
    result = await db.execute(stmt)
    route = result.scalar_one_or_none()

    if not route:
        raise HTTPException(status_code=404, detail=f"Route '{route_id}' not found.")

    await db.delete(route)
    await db.commit()

    return {"ok": True}
