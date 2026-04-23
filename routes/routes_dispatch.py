from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from database import get_db
from models import Driver, Route, RouteStop

router = APIRouter(prefix="/routes", tags=["routes"])


# =============================================================================
# Schemas
# =============================================================================

class RouteCreate(BaseModel):
    name: Optional[str] = None
    driver_id: Optional[int] = None
    status: Optional[str] = "pending"
    scheduled_date: Optional[datetime] = None


class StopCreate(BaseModel):
    order_id: Optional[int] = None
    stop_order: Optional[int] = 0
    address: Optional[str] = None
    customer_name: Optional[str] = None
    status: Optional[str] = "pending"
    notes: Optional[str] = None


class AssignDriverBody(BaseModel):
    driver_id: int


def serialize_stop(stop: RouteStop) -> dict:
    return {
        "id": stop.id,
        "route_id": stop.route_id,
        "order_id": stop.order_id,
        "stop_order": stop.stop_order,
        "address": stop.address,
        "customer_name": stop.customer_name,
        "status": stop.status,
        "notes": stop.notes,
        "created_at": stop.created_at.isoformat() if stop.created_at else None,
        "updated_at": stop.updated_at.isoformat() if stop.updated_at else None,
    }


def serialize_route(route: Route, include_stops: bool = True) -> dict:
    data: dict = {
        "id": route.id,
        "org_id": route.org_id,
        "name": route.name,
        "driver_id": route.driver_id,
        "status": route.status,
        "scheduled_date": route.scheduled_date.isoformat() if route.scheduled_date else None,
        "created_at": route.created_at.isoformat() if route.created_at else None,
        "updated_at": route.updated_at.isoformat() if route.updated_at else None,
    }
    if include_stops and route.stops is not None:
        data["stops"] = [serialize_stop(s) for s in route.stops]
    return data


# =============================================================================
# Endpoints
# =============================================================================

@router.post("")
async def create_route(
    org_id: str,
    body: RouteCreate,
    db: AsyncSession = Depends(get_db),
):
    # Validate driver belongs to this org if provided
    if body.driver_id is not None:
        driver_result = await db.execute(
            select(Driver).where(Driver.id == body.driver_id, Driver.org_id == org_id)
        )
        if not driver_result.scalar_one_or_none():
            raise HTTPException(status_code=404, detail="Driver not found")

    route = Route(
        org_id=org_id,
        name=body.name,
        driver_id=body.driver_id,
        status=body.status or "pending",
        scheduled_date=body.scheduled_date,
    )
    db.add(route)
    await db.commit()
    await db.refresh(route)
    return {"ok": True, "route": serialize_route(route, include_stops=False)}


@router.get("")
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


@router.get("/{route_id}")
async def get_route(
    org_id: str,
    route_id: int,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(Route)
        .options(selectinload(Route.stops))
        .where(Route.id == route_id, Route.org_id == org_id)
    )
    route = result.scalar_one_or_none()
    if not route:
        raise HTTPException(status_code=404, detail="Route not found")
    return {"ok": True, "route": serialize_route(route)}


@router.post("/{route_id}/assign-driver")
async def assign_driver(
    org_id: str,
    route_id: int,
    body: AssignDriverBody,
    db: AsyncSession = Depends(get_db),
):
    route_result = await db.execute(
        select(Route).where(Route.id == route_id, Route.org_id == org_id)
    )
    route = route_result.scalar_one_or_none()
    if not route:
        raise HTTPException(status_code=404, detail="Route not found")

    driver_result = await db.execute(
        select(Driver).where(Driver.id == body.driver_id, Driver.org_id == org_id)
    )
    if not driver_result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Driver not found")

    route.driver_id = body.driver_id
    route.updated_at = datetime.now(timezone.utc)
    await db.commit()
    await db.refresh(route)
    return {"ok": True, "route": serialize_route(route, include_stops=False)}


@router.post("/{route_id}/stops")
async def add_stop(
    org_id: str,
    route_id: int,
    body: StopCreate,
    db: AsyncSession = Depends(get_db),
):
    route_result = await db.execute(
        select(Route).where(Route.id == route_id, Route.org_id == org_id)
    )
    if not route_result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Route not found")

    stop = RouteStop(
        route_id=route_id,
        order_id=body.order_id,
        stop_order=body.stop_order or 0,
        address=body.address,
        customer_name=body.customer_name,
        status=body.status or "pending",
        notes=body.notes,
    )
    db.add(stop)
    await db.commit()
    await db.refresh(stop)
    return {"ok": True, "stop": serialize_stop(stop)}


@router.get("/{route_id}/stops")
async def list_stops(
    org_id: str,
    route_id: int,
    db: AsyncSession = Depends(get_db),
):
    route_result = await db.execute(
        select(Route).where(Route.id == route_id, Route.org_id == org_id)
    )
    if not route_result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Route not found")

    result = await db.execute(
        select(RouteStop)
        .where(RouteStop.route_id == route_id)
        .order_by(RouteStop.stop_order)
    )
    stops = result.scalars().all()
    return {
        "ok": True,
        "route_id": route_id,
        "count": len(stops),
        "stops": [serialize_stop(s) for s in stops],
    }
