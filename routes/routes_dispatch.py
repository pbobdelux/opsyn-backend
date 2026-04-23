import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from database import get_db
from models import Driver, Route, RouteStop

router = APIRouter(tags=["routes_dispatch"])


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class RouteStopCreate(BaseModel):
    order_id: str
    stop_index: int
    customer_name: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None


class RouteCreate(BaseModel):
    name: str
    route_date: Optional[str] = None
    driver_id: Optional[str] = None
    notes: Optional[str] = None
    stops: list[RouteStopCreate] = []


class AssignDriverRequest(BaseModel):
    driver_id: Optional[str] = None  # None = unassign


# ---------------------------------------------------------------------------
# Helpers
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


def serialize_route(route: Route, include_stops: bool = True) -> dict:
    data: dict = {
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
        "created_at": route.created_at.isoformat() if route.created_at else None,
        "updated_at": route.updated_at.isoformat() if route.updated_at else None,
    }
    if include_stops:
        stops = getattr(route, "stops", None) or []
        data["stops"] = sorted(
            [serialize_stop(s) for s in stops],
            key=lambda s: s["stop_index"],
        )
    return data


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post(
    "/organizations/{org_id}/routes",
    status_code=status.HTTP_201_CREATED,
)
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

    route_id = str(uuid.uuid4())
    route_status = "assigned" if body.driver_id else "draft"

    route = Route(
        id=route_id,
        org_id=org_id,
        name=body.name,
        status=route_status,
        driver_id=body.driver_id,
        route_date=body.route_date,
        notes=body.notes,
        total_stops=len(body.stops),
        total_orders=len({s.order_id for s in body.stops}),
        total_value=0,
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
            status="pending",
        )
        db.add(stop)

    await db.commit()

    # Reload with stops
    result = await db.execute(
        select(Route)
        .options(selectinload(Route.stops))
        .where(Route.id == route_id)
    )
    route = result.scalar_one()
    return {"ok": True, "route": serialize_route(route)}


@router.get("/organizations/{org_id}/routes")
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


@router.get("/organizations/{org_id}/routes/{route_id}")
async def get_route(
    org_id: str,
    route_id: str,
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


@router.post("/organizations/{org_id}/routes/{route_id}/assign")
async def assign_driver(
    org_id: str,
    route_id: str,
    body: AssignDriverRequest,
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

    if body.driver_id is None:
        # Unassign
        route.driver_id = None
        route.status = "unassigned"
    else:
        driver_result = await db.execute(
            select(Driver).where(Driver.id == body.driver_id, Driver.org_id == org_id)
        )
        driver = driver_result.scalar_one_or_none()
        if not driver:
            raise HTTPException(status_code=404, detail="Driver not found")
        if not driver.active:
            raise HTTPException(status_code=400, detail="Driver is not active")
        route.driver_id = body.driver_id
        route.status = "assigned"

    route.updated_at = datetime.now(timezone.utc)
    await db.commit()
    await db.refresh(route)

    # Reload with stops after refresh
    result = await db.execute(
        select(Route)
        .options(selectinload(Route.stops))
        .where(Route.id == route_id)
    )
    route = result.scalar_one()
    return {"ok": True, "route": serialize_route(route)}
