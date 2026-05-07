"""
Admin route and route-stop management API.

All endpoints are org-scoped via X-OPSYN-ORG + X-OPSYN-SECRET headers.
Uses the Route and RouteStop models from Phase 1.
"""

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel, field_validator
from sqlalchemy import delete, func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import Order
from models.driver import Driver
from models.route import Route
from models.route_stop import RouteStop
from services.route_events import log_route_event
from utils.json_utils import make_json_safe

logger = logging.getLogger("routes")

router = APIRouter(prefix="/routes", tags=["routes"])

# ---------------------------------------------------------------------------
# Valid enum values (mirrors DB check constraints)
# ---------------------------------------------------------------------------
VALID_ROUTE_STATUSES = {"draft", "assigned", "out_for_delivery", "completed", "cancelled"}
VALID_STOP_TYPES = {
    "leaflink_order",
    "manual_stop",
    "bank",
    "processor_pickup",
    "sample_dropoff",
    "supply_pickup",
    "other",
}
VALID_STOP_STATUSES = {"pending", "arrived", "completed", "failed", "skipped"}
VALID_AR_STATUSES = {"unpaid", "partial", "paid", "collection_issue", "not_applicable"}


# =============================================================================
# Pydantic Schemas
# =============================================================================


class RouteCreate(BaseModel):
    route_date: str  # ISO date string, e.g. "2026-05-15"
    route_number: Optional[str] = None
    assigned_driver_id: Optional[str] = None
    notes: Optional[str] = None

    @field_validator("route_date")
    @classmethod
    def validate_route_date(cls, v: str) -> str:
        from datetime import date
        try:
            date.fromisoformat(v)
        except ValueError:
            raise ValueError("route_date must be a valid ISO date (YYYY-MM-DD)")
        return v

    @field_validator("route_number")
    @classmethod
    def validate_route_number(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        if len(v) > 50:
            raise ValueError("route_number must be 50 characters or fewer")
        return v


class RouteUpdate(BaseModel):
    route_number: Optional[str] = None
    route_date: Optional[str] = None
    status: Optional[str] = None
    assigned_driver_id: Optional[str] = None
    notes: Optional[str] = None

    @field_validator("route_date")
    @classmethod
    def validate_route_date(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        from datetime import date
        try:
            date.fromisoformat(v)
        except ValueError:
            raise ValueError("route_date must be a valid ISO date (YYYY-MM-DD)")
        return v

    @field_validator("route_number")
    @classmethod
    def validate_route_number(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        if len(v) > 50:
            raise ValueError("route_number must be 50 characters or fewer")
        return v

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        if v not in VALID_ROUTE_STATUSES:
            raise ValueError(f"status must be one of: {', '.join(sorted(VALID_ROUTE_STATUSES))}")
        return v


class AssignDriverBody(BaseModel):
    driver_id: str


class RouteStopCreate(BaseModel):
    stop_type: str
    stop_order: Optional[int] = None
    source_order_id: Optional[str] = None
    customer_name: Optional[str] = None
    stop_name: Optional[str] = None
    address: Optional[str] = None
    contact_name: Optional[str] = None
    contact_phone: Optional[str] = None
    notes: Optional[str] = None
    time_window: Optional[str] = None
    priority: int = 0
    ar_status: str = "not_applicable"
    amount_due: float = 0.0

    @field_validator("stop_type")
    @classmethod
    def validate_stop_type(cls, v: str) -> str:
        if v not in VALID_STOP_TYPES:
            raise ValueError(f"stop_type must be one of: {', '.join(sorted(VALID_STOP_TYPES))}")
        return v

    @field_validator("ar_status")
    @classmethod
    def validate_ar_status(cls, v: str) -> str:
        if v not in VALID_AR_STATUSES:
            raise ValueError(f"ar_status must be one of: {', '.join(sorted(VALID_AR_STATUSES))}")
        return v


class RouteStopUpdate(BaseModel):
    stop_type: Optional[str] = None
    stop_order: Optional[int] = None
    source_order_id: Optional[str] = None
    customer_name: Optional[str] = None
    stop_name: Optional[str] = None
    address: Optional[str] = None
    contact_name: Optional[str] = None
    contact_phone: Optional[str] = None
    notes: Optional[str] = None
    time_window: Optional[str] = None
    priority: Optional[int] = None
    status: Optional[str] = None
    ar_status: Optional[str] = None
    amount_due: Optional[float] = None
    amount_collected: Optional[float] = None

    @field_validator("stop_type")
    @classmethod
    def validate_stop_type(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        if v not in VALID_STOP_TYPES:
            raise ValueError(f"stop_type must be one of: {', '.join(sorted(VALID_STOP_TYPES))}")
        return v

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        if v not in VALID_STOP_STATUSES:
            raise ValueError(f"status must be one of: {', '.join(sorted(VALID_STOP_STATUSES))}")
        return v

    @field_validator("ar_status")
    @classmethod
    def validate_ar_status(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        if v not in VALID_AR_STATUSES:
            raise ValueError(f"ar_status must be one of: {', '.join(sorted(VALID_AR_STATUSES))}")
        return v


class ReorderStopsBody(BaseModel):
    stop_ids: List[str]


# =============================================================================
# Helpers
# =============================================================================


async def _get_org_from_header(
    x_opsyn_org: Optional[str],
    x_opsyn_secret: Optional[str],
    db: AsyncSession,
) -> str:
    """Resolve and authenticate the org from X-OPSYN-ORG / X-OPSYN-SECRET headers."""
    from services.tenant_auth import get_authenticated_org

    if not x_opsyn_org:
        raise HTTPException(status_code=401, detail="X-OPSYN-ORG header is required")

    return await get_authenticated_org(x_opsyn_org, x_opsyn_secret, x_opsyn_org, db)


async def _get_route_or_404(db: AsyncSession, route_id: str, org_id: str) -> Route:
    """Fetch a route by ID scoped to org_id, or raise 404."""
    result = await db.execute(
        select(Route).where(
            Route.id == route_id,
            Route.org_id == org_id,
        )
    )
    route = result.scalar_one_or_none()
    if route is None:
        raise HTTPException(status_code=404, detail="Route not found")
    return route


async def _get_route_stop_or_404(
    db: AsyncSession, stop_id: str, route_id: str
) -> RouteStop:
    """Fetch a stop by ID scoped to route_id, or raise 404."""
    result = await db.execute(
        select(RouteStop).where(
            RouteStop.id == stop_id,
            RouteStop.route_id == route_id,
        )
    )
    stop = result.scalar_one_or_none()
    if stop is None:
        raise HTTPException(status_code=404, detail="Route stop not found")
    return stop


async def _get_source_order(db: AsyncSession, source_order_id: str) -> Optional[Order]:
    """Fetch an Order by UUID primary key (source_order_id stored as UUID in route_stops)."""
    try:
        result = await db.execute(
            select(Order).where(Order.id == int(source_order_id))
        )
        return result.scalar_one_or_none()
    except (ValueError, TypeError):
        return None


async def _recalculate_route_totals(db: AsyncSession, route: Route) -> None:
    """Recalculate total_stops, total_value, and total_units from current stops."""
    # Count stops
    count_result = await db.execute(
        select(func.count(RouteStop.id)).where(RouteStop.route_id == route.id)
    )
    total_stops = count_result.scalar_one() or 0

    # Sum amount_due
    value_result = await db.execute(
        select(func.coalesce(func.sum(RouteStop.amount_due), 0)).where(
            RouteStop.route_id == route.id
        )
    )
    total_value = value_result.scalar_one() or Decimal("0")

    # For total_units: sum unit_count from source orders where source_order_id is set.
    # We fetch all stops with a source_order_id and look up the orders.
    stops_result = await db.execute(
        select(RouteStop.source_order_id).where(
            RouteStop.route_id == route.id,
            RouteStop.source_order_id.isnot(None),
        )
    )
    source_order_ids = [row[0] for row in stops_result.fetchall()]

    total_units = 0
    if source_order_ids:
        # source_order_id is stored as UUID in route_stops but Order.id is Integer.
        # Attempt integer conversion for each; skip non-integer UUIDs.
        int_ids = []
        for sid in source_order_ids:
            try:
                int_ids.append(int(str(sid)))
            except (ValueError, TypeError):
                pass
        if int_ids:
            units_result = await db.execute(
                select(func.coalesce(func.sum(Order.unit_count), 0)).where(
                    Order.id.in_(int_ids)
                )
            )
            total_units = units_result.scalar_one() or 0

    route.total_stops = total_stops
    route.total_value = Decimal(str(total_value))
    route.total_units = total_units


async def _reorder_stops_sequential(db: AsyncSession, route_id: str) -> None:
    """
    Close gaps in stop_order after a deletion by reassigning sequential
    stop_order values (1, 2, 3, …) ordered by the current stop_order.
    """
    result = await db.execute(
        select(RouteStop)
        .where(RouteStop.route_id == route_id)
        .order_by(RouteStop.stop_order.asc())
    )
    stops = result.scalars().all()
    for idx, stop in enumerate(stops, start=1):
        stop.stop_order = idx


async def _serialize_source_order(db: AsyncSession, source_order_id: Any) -> Optional[dict]:
    """Fetch and serialize a source order for embedding in stop responses."""
    if source_order_id is None:
        return None
    order = await _get_source_order(db, str(source_order_id))
    if order is None:
        return None
    return make_json_safe({
        "id": order.id,
        "customer_name": order.customer_name,
        "total": order.amount,
    })


def _serialize_driver_brief(driver: Optional[Driver]) -> Optional[dict]:
    """Return a compact driver dict for embedding in route responses."""
    if driver is None:
        return None
    return make_json_safe({
        "id": driver.id,
        "name": driver.name,
        "phone": driver.phone,
    })


def _serialize_route_stop(stop: RouteStop, source_order: Optional[dict] = None) -> dict:
    """Convert a RouteStop ORM instance to a JSON-safe dict."""
    return make_json_safe({
        "id": stop.id,
        "route_id": stop.route_id,
        "stop_order": stop.stop_order,
        "stop_type": stop.stop_type,
        "customer_name": stop.customer_name,
        "stop_name": stop.stop_name,
        "address": stop.address,
        "contact_name": stop.contact_name,
        "contact_phone": stop.contact_phone,
        "notes": stop.notes,
        "time_window": stop.time_window,
        "priority": stop.priority,
        "status": stop.status,
        "ar_status": stop.ar_status,
        "amount_due": stop.amount_due,
        "amount_collected": stop.amount_collected,
        "completed_at": stop.completed_at,
        "source_order": source_order,
        "created_at": stop.created_at,
        "updated_at": stop.updated_at,
    })


async def _serialize_route(
    db: AsyncSession,
    route: Route,
    include_stops: bool = False,
) -> dict:
    """Convert a Route ORM instance to a JSON-safe dict, optionally with stops."""
    # Resolve assigned driver
    driver_brief: Optional[dict] = None
    if route.assigned_driver_id is not None:
        driver_result = await db.execute(
            select(Driver).where(Driver.id == route.assigned_driver_id)
        )
        driver = driver_result.scalar_one_or_none()
        driver_brief = _serialize_driver_brief(driver)

    data: dict = make_json_safe({
        "id": route.id,
        "org_id": route.org_id,
        "route_number": route.route_number,
        "status": route.status,
        "route_date": route.route_date,
        "assigned_driver": driver_brief,
        "total_stops": route.total_stops,
        "total_value": route.total_value,
        "total_units": route.total_units,
        "notes": route.notes,
        "version": route.version,
        "created_at": route.created_at,
        "updated_at": route.updated_at,
        "published_at": route.published_at,
    })

    if include_stops:
        stops_result = await db.execute(
            select(RouteStop)
            .where(RouteStop.route_id == route.id)
            .order_by(RouteStop.stop_order.asc())
        )
        stops = stops_result.scalars().all()
        serialized_stops = []
        for stop in stops:
            source_order = await _serialize_source_order(db, stop.source_order_id)
            serialized_stops.append(_serialize_route_stop(stop, source_order))
        data["stops"] = serialized_stops
    else:
        # For list view, include stop_count alias
        data["stop_count"] = route.total_stops

    return data


# =============================================================================
# Route Endpoints
# =============================================================================


@router.get("")
async def list_routes(
    x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
    x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
    status: Optional[str] = Query(default=None, description="Filter by status"),
    route_date: Optional[str] = Query(default=None, description="Filter by route_date (ISO date)"),
    assigned_driver_id: Optional[str] = Query(default=None, description="Filter by assigned driver"),
    limit: int = Query(default=50, ge=1, le=1000, description="Pagination limit"),
    offset: int = Query(default=0, ge=0, description="Pagination offset"),
    db: AsyncSession = Depends(get_db),
):
    """List all routes for the org with filtering and pagination."""
    org_id = await _get_org_from_header(x_opsyn_org, x_opsyn_secret, db)

    query = select(Route).where(Route.org_id == org_id)

    if status:
        if status not in VALID_ROUTE_STATUSES:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid status. Must be one of: {', '.join(sorted(VALID_ROUTE_STATUSES))}",
            )
        query = query.where(Route.status == status)

    if route_date:
        from datetime import date
        try:
            parsed_date = date.fromisoformat(route_date)
        except ValueError:
            raise HTTPException(status_code=400, detail="route_date must be a valid ISO date (YYYY-MM-DD)")
        query = query.where(Route.route_date == parsed_date)

    if assigned_driver_id:
        query = query.where(Route.assigned_driver_id == assigned_driver_id)

    # Total count before pagination
    count_query = select(func.count()).select_from(query.subquery())
    total_result = await db.execute(count_query)
    total = total_result.scalar_one()

    # Paginated results — route_date DESC, then created_at DESC
    query = (
        query
        .order_by(Route.route_date.desc(), Route.created_at.desc())
        .offset(offset)
        .limit(limit)
    )
    result = await db.execute(query)
    routes = result.scalars().all()

    serialized = []
    for route in routes:
        serialized.append(await _serialize_route(db, route, include_stops=False))

    logger.info("[ROUTES_LIST] org_id=%s count=%s", org_id, len(serialized))

    return make_json_safe({
        "ok": True,
        "org_id": org_id,
        "routes": serialized,
        "total": total,
        "limit": limit,
        "offset": offset,
    })


@router.post("")
async def create_route(
    body: RouteCreate,
    x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
    x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
    db: AsyncSession = Depends(get_db),
):
    """Create a new route scoped to the org."""
    org_id = await _get_org_from_header(x_opsyn_org, x_opsyn_secret, db)

    from datetime import date
    route_date = date.fromisoformat(body.route_date)

    # Validate driver if provided
    if body.assigned_driver_id is not None:
        driver_result = await db.execute(
            select(Driver).where(
                Driver.id == body.assigned_driver_id,
                Driver.org_id == org_id,
            )
        )
        if driver_result.scalar_one_or_none() is None:
            raise HTTPException(
                status_code=400,
                detail="assigned_driver_id does not exist or does not belong to this org",
            )

    route = Route(
        org_id=org_id,
        route_number=body.route_number or None,
        route_date=route_date,
        assigned_driver_id=body.assigned_driver_id or None,
        notes=body.notes or None,
        status="draft",
        total_stops=0,
        total_value=Decimal("0"),
        total_units=0,
        version=1,
    )
    db.add(route)

    try:
        await db.commit()
        await db.refresh(route)
    except IntegrityError as exc:
        await db.rollback()
        logger.error("[ROUTE_CREATED] db_integrity_error org_id=%s error=%s", org_id, exc)
        raise HTTPException(status_code=400, detail="Could not create route due to a data conflict")
    except Exception as exc:
        await db.rollback()
        logger.error("[ROUTE_CREATED] unexpected_error org_id=%s error=%s", org_id, exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Unexpected error creating route")

    logger.info(
        "[ROUTE_CREATED] org_id=%s route_id=%s route_date=%s",
        org_id,
        route.id,
        route.route_date,
    )

    return {"ok": True, "route": await _serialize_route(db, route, include_stops=False)}


@router.get("/{route_id}")
async def get_route(
    route_id: str,
    x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
    x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
    db: AsyncSession = Depends(get_db),
):
    """Get a single route with full stops array."""
    org_id = await _get_org_from_header(x_opsyn_org, x_opsyn_secret, db)
    route = await _get_route_or_404(db, route_id, org_id)

    logger.info("[ROUTE_FETCHED] org_id=%s route_id=%s", org_id, route_id)

    return {"ok": True, "route": await _serialize_route(db, route, include_stops=True)}


@router.patch("/{route_id}")
async def update_route(
    route_id: str,
    body: RouteUpdate,
    x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
    x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
    db: AsyncSession = Depends(get_db),
):
    """Update route fields. Only provided fields are changed."""
    org_id = await _get_org_from_header(x_opsyn_org, x_opsyn_secret, db)
    route = await _get_route_or_404(db, route_id, org_id)

    updated_fields: list[str] = []

    if body.route_number is not None:
        route.route_number = body.route_number
        updated_fields.append("route_number")

    if body.route_date is not None:
        from datetime import date
        route.route_date = date.fromisoformat(body.route_date)
        updated_fields.append("route_date")

    if body.status is not None:
        route.status = body.status
        updated_fields.append("status")

    if body.assigned_driver_id is not None:
        # Validate new driver exists in same org
        driver_result = await db.execute(
            select(Driver).where(
                Driver.id == body.assigned_driver_id,
                Driver.org_id == org_id,
            )
        )
        if driver_result.scalar_one_or_none() is None:
            raise HTTPException(
                status_code=400,
                detail="assigned_driver_id does not exist or does not belong to this org",
            )
        route.assigned_driver_id = body.assigned_driver_id
        updated_fields.append("assigned_driver_id")

    if body.notes is not None:
        route.notes = body.notes
        updated_fields.append("notes")

    route.version = (route.version or 1) + 1
    route.updated_at = datetime.now(timezone.utc)

    try:
        await db.commit()
        await db.refresh(route)
    except IntegrityError as exc:
        await db.rollback()
        logger.error("[ROUTE_UPDATED] db_integrity_error org_id=%s route_id=%s error=%s", org_id, route_id, exc)
        raise HTTPException(status_code=400, detail="Could not update route due to a data conflict")
    except Exception as exc:
        await db.rollback()
        logger.error("[ROUTE_UPDATED] unexpected_error org_id=%s route_id=%s error=%s", org_id, route_id, exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Unexpected error updating route")

    logger.info(
        "[ROUTE_UPDATED] org_id=%s route_id=%s fields=%s",
        org_id,
        route_id,
        ",".join(updated_fields) if updated_fields else "none",
    )

    return {"ok": True, "route": await _serialize_route(db, route, include_stops=False)}


@router.post("/{route_id}/assign-driver")
async def assign_driver(
    route_id: str,
    body: AssignDriverBody,
    x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
    x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
    db: AsyncSession = Depends(get_db),
):
    """Assign an active driver to the route."""
    org_id = await _get_org_from_header(x_opsyn_org, x_opsyn_secret, db)
    route = await _get_route_or_404(db, route_id, org_id)

    # Validate driver: must exist, be active, and belong to same org
    driver_result = await db.execute(
        select(Driver).where(
            Driver.id == body.driver_id,
            Driver.org_id == org_id,
        )
    )
    driver = driver_result.scalar_one_or_none()
    if driver is None:
        raise HTTPException(status_code=404, detail="Driver not found or does not belong to this org")
    if driver.status != "active":
        raise HTTPException(status_code=400, detail="Driver is not active")

    route.assigned_driver_id = body.driver_id
    if route.status == "draft":
        route.status = "assigned"
    route.version = (route.version or 1) + 1
    route.updated_at = datetime.now(timezone.utc)

    # Log driver_assigned event (within same transaction)
    try:
        route_uuid = UUID(str(route.id))
        org_uuid = UUID(str(org_id))
        driver_uuid = UUID(str(body.driver_id))
        await log_route_event(
            db=db,
            route_id=route_uuid,
            org_id=org_uuid,
            event_type="driver_assigned",
            actor_type="admin",
            actor_id=org_uuid,
            event_metadata={"assigned_driver_id": str(driver_uuid)},
        )

    except Exception as event_exc:
        logger.warning(
            "[ROUTE_DRIVER_ASSIGNED] event_log_failed route_id=%s error=%s",
            route_id, event_exc,
        )

    try:
        await db.commit()
        await db.refresh(route)
    except Exception as exc:
        await db.rollback()
        logger.error("[ROUTE_DRIVER_ASSIGNED] unexpected_error org_id=%s route_id=%s error=%s", org_id, route_id, exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Unexpected error assigning driver")

    logger.info(
        "[ROUTE_DRIVER_ASSIGNED] org_id=%s route_id=%s driver_id=%s",
        org_id,
        route_id,
        body.driver_id,
    )

    return {"ok": True, "route": await _serialize_route(db, route, include_stops=False)}


@router.post("/{route_id}/publish")
async def publish_route(
    route_id: str,
    x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
    x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
    db: AsyncSession = Depends(get_db),
):
    """Publish the route. Requires an assigned driver."""
    org_id = await _get_org_from_header(x_opsyn_org, x_opsyn_secret, db)
    route = await _get_route_or_404(db, route_id, org_id)

    if route.assigned_driver_id is None:
        raise HTTPException(status_code=400, detail="Cannot publish route without assigned driver")

    now = datetime.now(timezone.utc)
    route.published_at = now
    if route.status == "draft" and route.assigned_driver_id is not None:
        route.status = "assigned"
    route.version = (route.version or 1) + 1
    route.updated_at = now

    try:
        await db.commit()
        await db.refresh(route)
    except Exception as exc:
        await db.rollback()
        logger.error("[ROUTE_PUBLISHED] unexpected_error org_id=%s route_id=%s error=%s", org_id, route_id, exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Unexpected error publishing route")

    logger.info("[ROUTE_PUBLISHED] org_id=%s route_id=%s", org_id, route_id)

    return {"ok": True, "route": await _serialize_route(db, route, include_stops=False)}


# =============================================================================
# Route Stop Endpoints
# =============================================================================


@router.get("/{route_id}/stops")
async def list_route_stops(
    route_id: str,
    x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
    x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
    db: AsyncSession = Depends(get_db),
):
    """Get all stops for the route, ordered by stop_order ASC."""
    org_id = await _get_org_from_header(x_opsyn_org, x_opsyn_secret, db)
    # Verify route exists and belongs to org
    await _get_route_or_404(db, route_id, org_id)

    result = await db.execute(
        select(RouteStop)
        .where(RouteStop.route_id == route_id)
        .order_by(RouteStop.stop_order.asc())
    )
    stops = result.scalars().all()

    serialized = []
    for stop in stops:
        source_order = await _serialize_source_order(db, stop.source_order_id)
        serialized.append(_serialize_route_stop(stop, source_order))

    logger.info(
        "[ROUTE_STOPS_FETCHED] org_id=%s route_id=%s count=%s",
        org_id,
        route_id,
        len(serialized),
    )

    return make_json_safe({
        "ok": True,
        "route_id": route_id,
        "stops": serialized,
    })


@router.post("/{route_id}/stops")
async def create_route_stop(
    route_id: str,
    body: RouteStopCreate,
    x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
    x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
    db: AsyncSession = Depends(get_db),
):
    """Create a new stop in the route."""
    org_id = await _get_org_from_header(x_opsyn_org, x_opsyn_secret, db)
    route = await _get_route_or_404(db, route_id, org_id)

    # Validate stop_type / source_order_id relationship
    if body.stop_type == "manual_stop" and body.source_order_id is not None:
        raise HTTPException(
            status_code=400,
            detail="source_order_id must be null for manual_stop type",
        )

    # Determine stop_order: if not provided, append at end
    if body.stop_order is not None:
        stop_order = body.stop_order
    else:
        max_result = await db.execute(
            select(func.max(RouteStop.stop_order)).where(RouteStop.route_id == route_id)
        )
        current_max = max_result.scalar_one()
        stop_order = (current_max or 0) + 1

    stop = RouteStop(
        route_id=route_id,
        org_id=org_id,
        stop_order=stop_order,
        stop_type=body.stop_type,
        source_order_id=body.source_order_id or None,
        customer_name=body.customer_name or None,
        stop_name=body.stop_name or None,
        address=body.address or None,
        contact_name=body.contact_name or None,
        contact_phone=body.contact_phone or None,
        notes=body.notes or None,
        time_window=body.time_window or None,
        priority=body.priority,
        ar_status=body.ar_status,
        amount_due=Decimal(str(body.amount_due)),
        amount_collected=Decimal("0"),
        status="pending",
    )
    db.add(stop)

    try:
        await db.flush()  # Get stop.id without committing yet
    except IntegrityError as exc:
        await db.rollback()
        logger.error("[ROUTE_STOP_CREATED] db_integrity_error org_id=%s route_id=%s error=%s", org_id, route_id, exc)
        raise HTTPException(status_code=409, detail="Could not create stop due to a data conflict (duplicate stop_order?)")
    except Exception as exc:
        await db.rollback()
        logger.error("[ROUTE_STOP_CREATED] unexpected_error org_id=%s route_id=%s error=%s", org_id, route_id, exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Unexpected error creating route stop")

    # Recalculate route totals and increment version
    await _recalculate_route_totals(db, route)
    route.version = (route.version or 1) + 1
    route.updated_at = datetime.now(timezone.utc)

    try:
        await db.commit()
        await db.refresh(stop)
        await db.refresh(route)
    except Exception as exc:
        await db.rollback()
        logger.error("[ROUTE_STOP_CREATED] commit_error org_id=%s route_id=%s error=%s", org_id, route_id, exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Unexpected error saving route stop")

    logger.info(
        "[ROUTE_STOP_CREATED] org_id=%s route_id=%s stop_id=%s stop_order=%s",
        org_id,
        route_id,
        stop.id,
        stop.stop_order,
    )

    source_order = await _serialize_source_order(db, stop.source_order_id)
    return {"ok": True, "stop": _serialize_route_stop(stop, source_order)}


@router.patch("/{route_id}/stops/{stop_id}")
async def update_route_stop(
    route_id: str,
    stop_id: str,
    body: RouteStopUpdate,
    x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
    x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
    db: AsyncSession = Depends(get_db),
):
    """Update stop fields. Only provided fields are changed."""
    org_id = await _get_org_from_header(x_opsyn_org, x_opsyn_secret, db)
    route = await _get_route_or_404(db, route_id, org_id)
    stop = await _get_route_stop_or_404(db, stop_id, route_id)

    updated_fields: list[str] = []

    if body.stop_type is not None:
        stop.stop_type = body.stop_type
        updated_fields.append("stop_type")

    if body.stop_order is not None:
        stop.stop_order = body.stop_order
        updated_fields.append("stop_order")

    if body.source_order_id is not None:
        stop.source_order_id = body.source_order_id
        updated_fields.append("source_order_id")

    if body.customer_name is not None:
        stop.customer_name = body.customer_name
        updated_fields.append("customer_name")

    if body.stop_name is not None:
        stop.stop_name = body.stop_name
        updated_fields.append("stop_name")

    if body.address is not None:
        stop.address = body.address
        updated_fields.append("address")

    if body.contact_name is not None:
        stop.contact_name = body.contact_name
        updated_fields.append("contact_name")

    if body.contact_phone is not None:
        stop.contact_phone = body.contact_phone
        updated_fields.append("contact_phone")

    if body.notes is not None:
        stop.notes = body.notes
        updated_fields.append("notes")

    if body.time_window is not None:
        stop.time_window = body.time_window
        updated_fields.append("time_window")

    if body.priority is not None:
        stop.priority = body.priority
        updated_fields.append("priority")

    if body.status is not None:
        stop.status = body.status
        updated_fields.append("status")

    if body.ar_status is not None:
        stop.ar_status = body.ar_status
        updated_fields.append("ar_status")

    if body.amount_due is not None:
        stop.amount_due = Decimal(str(body.amount_due))
        updated_fields.append("amount_due")

    if body.amount_collected is not None:
        stop.amount_collected = Decimal(str(body.amount_collected))
        updated_fields.append("amount_collected")

    stop.updated_at = datetime.now(timezone.utc)

    # Recalculate route totals and increment version
    await _recalculate_route_totals(db, route)
    route.version = (route.version or 1) + 1
    route.updated_at = datetime.now(timezone.utc)

    try:
        await db.commit()
        await db.refresh(stop)
        await db.refresh(route)
    except IntegrityError as exc:
        await db.rollback()
        logger.error("[ROUTE_STOP_UPDATED] db_integrity_error org_id=%s route_id=%s stop_id=%s error=%s", org_id, route_id, stop_id, exc)
        raise HTTPException(status_code=409, detail="Could not update stop due to a data conflict")
    except Exception as exc:
        await db.rollback()
        logger.error("[ROUTE_STOP_UPDATED] unexpected_error org_id=%s route_id=%s stop_id=%s error=%s", org_id, route_id, stop_id, exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Unexpected error updating route stop")

    logger.info(
        "[ROUTE_STOP_UPDATED] org_id=%s route_id=%s stop_id=%s fields=%s",
        org_id,
        route_id,
        stop_id,
        ",".join(updated_fields) if updated_fields else "none",
    )

    source_order = await _serialize_source_order(db, stop.source_order_id)
    return {"ok": True, "stop": _serialize_route_stop(stop, source_order)}


@router.delete("/{route_id}/stops/{stop_id}")
async def delete_route_stop(
    route_id: str,
    stop_id: str,
    x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
    x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
    db: AsyncSession = Depends(get_db),
):
    """Delete a stop from the route and reorder remaining stops."""
    org_id = await _get_org_from_header(x_opsyn_org, x_opsyn_secret, db)
    route = await _get_route_or_404(db, route_id, org_id)
    stop = await _get_route_stop_or_404(db, stop_id, route_id)

    await db.delete(stop)
    await db.flush()

    # Reorder remaining stops to close the gap
    await _reorder_stops_sequential(db, route_id)

    # Recalculate route totals and increment version
    await _recalculate_route_totals(db, route)
    route.version = (route.version or 1) + 1
    route.updated_at = datetime.now(timezone.utc)

    try:
        await db.commit()
    except Exception as exc:
        await db.rollback()
        logger.error("[ROUTE_STOP_DELETED] unexpected_error org_id=%s route_id=%s stop_id=%s error=%s", org_id, route_id, stop_id, exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Unexpected error deleting route stop")

    logger.info(
        "[ROUTE_STOP_DELETED] org_id=%s route_id=%s stop_id=%s",
        org_id,
        route_id,
        stop_id,
    )

    return {"ok": True}


@router.post("/{route_id}/reorder-stops")
async def reorder_stops(
    route_id: str,
    body: ReorderStopsBody,
    x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
    x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
    db: AsyncSession = Depends(get_db),
):
    """Reorder stops in the route to match the provided stop_ids array."""
    org_id = await _get_org_from_header(x_opsyn_org, x_opsyn_secret, db)
    route = await _get_route_or_404(db, route_id, org_id)

    # Fetch all current stops for this route
    result = await db.execute(
        select(RouteStop).where(RouteStop.route_id == route_id)
    )
    existing_stops = result.scalars().all()
    existing_ids = {str(s.id) for s in existing_stops}

    # Validate: all provided IDs must belong to this route
    provided_ids = [str(sid) for sid in body.stop_ids]
    unknown = set(provided_ids) - existing_ids
    if unknown:
        raise HTTPException(
            status_code=400,
            detail=f"The following stop_ids do not belong to this route: {', '.join(unknown)}",
        )

    # Validate: array length must match current stop count
    if len(provided_ids) != len(existing_stops):
        raise HTTPException(
            status_code=400,
            detail=f"stop_ids length ({len(provided_ids)}) must match current stop count ({len(existing_stops)})",
        )

    # Build a lookup map and apply new order
    stop_map = {str(s.id): s for s in existing_stops}
    for new_order, stop_id in enumerate(provided_ids, start=1):
        stop_map[stop_id].stop_order = new_order

    route.version = (route.version or 1) + 1
    route.updated_at = datetime.now(timezone.utc)

    # Log stops_reordered event (within same transaction)
    try:
        route_uuid = UUID(str(route.id))
        await log_route_event(
            db=db,
            route_id=route_uuid,
            org_id=org_uuid,
            event_type="stops_reordered",
            actor_type="admin",
            actor_id=org_uuid,
            event_metadata={"stop_ids": provided_ids},
        )
    except Exception as event_exc:
        logger.warning(
            "[ROUTE_STOPS_REORDERED] event_log_failed route_id=%s error=%s",
            route_id, event_exc,
        )


    try:
        await db.commit()
        await db.refresh(route)
    except Exception as exc:
        await db.rollback()
        logger.error("[ROUTE_STOPS_REORDERED] unexpected_error org_id=%s route_id=%s error=%s", org_id, route_id, exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Unexpected error reordering stops")

    # Return updated stops in new order
    result = await db.execute(
        select(RouteStop)
        .where(RouteStop.route_id == route_id)
        .order_by(RouteStop.stop_order.asc())
    )
    stops = result.scalars().all()

    serialized = []
    for stop in stops:
        source_order = await _serialize_source_order(db, stop.source_order_id)
        serialized.append(_serialize_route_stop(stop, source_order))

    logger.info(
        "[ROUTE_STOPS_REORDERED] org_id=%s route_id=%s count=%s",
        org_id,
        route_id,
        len(serialized),
    )

    return make_json_safe({"ok": True, "stops": serialized})


@router.post("/{route_id}/optimize")
async def optimize_route(
    route_id: str,
    x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
    x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
    db: AsyncSession = Depends(get_db),
):
    """
    Suggest an optimized stop order using nearest-neighbor heuristic (MVP).

    Returns a draft suggestion — does NOT save automatically.
    Admin reviews and calls /reorder-stops to apply.
    """
    org_id = await _get_org_from_header(x_opsyn_org, x_opsyn_secret, db)
    await _get_route_or_404(db, route_id, org_id)

    result = await db.execute(
        select(RouteStop)
        .where(RouteStop.route_id == route_id)
        .order_by(RouteStop.stop_order.asc())
    )
    stops = result.scalars().all()

    if not stops:
        return make_json_safe({
            "ok": True,
            "optimized_stops": [],
            "estimated_distance_km": None,
            "estimated_duration_min": None,
        })

    # MVP nearest-neighbor: sort alphabetically by address as a simple proxy.
    # No external mapping APIs are called.
    stops_with_address = [s for s in stops if s.address]
    stops_without_address = [s for s in stops if not s.address]

    sorted_stops = sorted(stops_with_address, key=lambda s: (s.address or "").lower())
    sorted_stops.extend(stops_without_address)

    optimized = [
        make_json_safe({
            "stop_id": str(stop.id),
            "suggested_order": idx,
            "address": stop.address,
        })
        for idx, stop in enumerate(sorted_stops, start=1)
    ]

    logger.info(
        "[ROUTE_OPTIMIZE] org_id=%s route_id=%s suggested_order_count=%s",
        org_id,
        route_id,
        len(optimized),
    )

    return {
        "ok": True,
        "optimized_stops": optimized,
        "estimated_distance_km": None,
        "estimated_duration_min": None,
    }
