import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from database import get_db
from models import Driver, Route, RouteStop

router = APIRouter(prefix="/organizations", tags=["routes"])


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class RouteStopIn(BaseModel):
    order_id: str
    stop_index: int
    customer_name: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    status: str = "pending"


class RouteCreate(BaseModel):
    name: str
    status: str = "draft"
    driver_id: Optional[str] = None
    route_date: Optional[str] = None
    total_value: int = 0
    notes: Optional[str] = None
    stops: list[RouteStopIn] = []


class AssignDriver(BaseModel):
    driver_id: Optional[str] = None  # None / null = unassign


# ---------------------------------------------------------------------------
# Serializers
# ---------------------------------------------------------------------------

def serialize_stop(stop: RouteStop) -> dict:
    return {
        "id": stop.id,
        "route_id": stop.route_id,
        "org_id": stop.org_id,
        "order_id": stop.order_id,
        "stop_index": stop.stop_index,
        "customer_name": stop.customer_name,
        "address": stop.address,
        "city": stop.city,
        "state": stop.state,
        "postal_code": stop.postal_code,
        "status": stop.status,
        "created_at": stop.created_at.isoformat() if stop.created_at else None,
        "updated_at": stop.updated_at.isoformat() if stop.updated_at else None,
    }


def serialize_route(route: Route) -> dict:
    stops = sorted(route.stops, key=lambda s: s.stop_index) if route.stops else []
    return {
        "id": route.id,
        "org_id": route.org_id,
        "name": route.name,
        "status": route.status,
        "driver_id": route.driver_id,
        "route_date": route.route_date,
        "total_stops": route.total_stops,
        "total_orders": route.total_orders,
        "total_value": route.total_value,
        "notes": route.notes,
        "stops": [serialize_stop(s) for s in stops],
        "created_at": route.created_at.isoformat() if route.created_at else None,
        "updated_at": route.updated_at.isoformat() if route.updated_at else None,
    }


# ---------------------------------------------------------------------------
# Helper: load route with stops eagerly
# ---------------------------------------------------------------------------

async def _get_route_with_stops(
    db: AsyncSession, route_id: str, org_id: str
) -> Route | None:
    result = await db.execute(
        select(Route)
        .options(selectinload(Route.stops))
        .where(Route.id == route_id, Route.org_id == org_id)
    )
    return result.scalar_one_or_none()


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/{org_id}/routes", status_code=201)
async def create_route(
    org_id: str,
    body: RouteCreate,
    db: AsyncSession = Depends(get_db),
):
    # Validate driver if provided
    if body.driver_id:
        driver_result = await db.execute(
            select(Driver).where(Driver.id == body.driver_id, Driver.org_id == org_id)
        )
        driver = driver_result.scalar_one_or_none()
        if not driver:
            raise HTTPException(status_code=404, detail="Driver not found")
        if not driver.active:
            raise HTTPException(status_code=400, detail="Driver is not active")

    now = datetime.now(timezone.utc)
    route_id = str(uuid.uuid4())

    # Deduplicate order IDs for total_orders count
    unique_order_ids = {s.order_id for s in body.stops}

    route = Route(
        id=route_id,
        org_id=org_id,
        name=body.name,
        status=body.status,
        driver_id=body.driver_id,
        route_date=body.route_date,
        total_stops=len(body.stops),
        total_orders=len(unique_order_ids),
        total_value=body.total_value,
        notes=body.notes,
        created_at=now,
        updated_at=now,
    )
    db.add(route)

    for stop_in in body.stops:
        stop = RouteStop(
            id=str(uuid.uuid4()),
            route_id=route_id,
            org_id=org_id,
            order_id=stop_in.order_id,
            stop_index=stop_in.stop_index,
            customer_name=stop_in.customer_name,
            address=stop_in.address,
            city=stop_in.city,
            state=stop_in.state,
            postal_code=stop_in.postal_code,
            status=stop_in.status,
            created_at=now,
            updated_at=now,
        )
        db.add(stop)

    await db.commit()

    # Reload with stops
    created = await _get_route_with_stops(db, route_id, org_id)
    return {"ok": True, "route": serialize_route(created)}


@router.get("/{org_id}/routes", status_code=200)
async def list_routes(
    org_id: str,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(Route)
        .options(selectinload(Route.stops))
        .where(Route.org_id == org_id)
        .order_by(Route.created_at.desc())
    )
    routes = result.scalars().all()
    return {
        "ok": True,
        "org_id": org_id,
        "count": len(routes),
        "routes": [serialize_route(r) for r in routes],
    }


@router.get("/{org_id}/routes/{route_id}", status_code=200)
async def get_route(
    org_id: str,
    route_id: str,
    db: AsyncSession = Depends(get_db),
):
    route = await _get_route_with_stops(db, route_id, org_id)
    if not route:
        raise HTTPException(status_code=404, detail="Route not found")
    return {"ok": True, "route": serialize_route(route)}


@router.post("/{org_id}/routes/{route_id}/assign", status_code=200)
async def assign_driver(
    org_id: str,
    route_id: str,
    body: AssignDriver,
    db: AsyncSession = Depends(get_db),
):
    route = await _get_route_with_stops(db, route_id, org_id)
    if not route:
        raise HTTPException(status_code=404, detail="Route not found")

    if body.driver_id is not None:
        # Assigning a driver — validate they exist, belong to org, and are active
        driver_result = await db.execute(
            select(Driver).where(Driver.id == body.driver_id, Driver.org_id == org_id)
        )
        driver = driver_result.scalar_one_or_none()
        if not driver:
            raise HTTPException(status_code=404, detail="Driver not found")
        if not driver.active:
            raise HTTPException(status_code=400, detail="Driver is not active")

    route.driver_id = body.driver_id
    route.updated_at = datetime.now(timezone.utc)

    await db.commit()
    await db.refresh(route)

    # Reload with stops after commit
    updated = await _get_route_with_stops(db, route_id, org_id)
    return {"ok": True, "route": serialize_route(updated)}
