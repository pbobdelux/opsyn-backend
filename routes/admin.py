import csv
import io
import logging
import os
from datetime import datetime, date, timezone
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy import func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import Order
from models.driver import Driver
from models.driver_location import DriverLocation
from models.driver_route_history import DriverRouteHistory
from models.route import Route
from models.route_event import RouteEvent
from models.route_stop import RouteStop
from services.route_events import log_route_event
from utils.json_utils import make_json_safe

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/admin", tags=["admin"])

# Register the emergency raw schema check endpoint (no ORM dependencies)
from routes.raw_schema_check import router as _raw_schema_check_router  # noqa: E402
router.include_router(_raw_schema_check_router)

# Admin seed token from environment
ADMIN_SEED_TOKEN = os.getenv("ADMIN_SEED_TOKEN", "opsyn-seed-2026")

# ---------------------------------------------------------------------------
# Bootstrap status — populated by main.py at module level after the
# synchronous bootstrap recovery completes (before any ORM imports).
# ---------------------------------------------------------------------------
_BOOTSTRAP_STATUS: dict = {
    "executed": False,
    "success": False,
    "schema_verified": False,
}


# =============================================================================
# Endpoint: GET /admin/bootstrap-status
# Returns the result of the module-level bootstrap schema recovery.
# =============================================================================


@router.get("/bootstrap-status")
async def get_bootstrap_status():
    """
    Return bootstrap recovery status.

    Used for health checks to confirm schema recovery completed before ORM
    usage.  The status is set at module level in main.py immediately after
    bootstrap_schema_recovery_sync() succeeds.

    Returns:
        {
            "bootstrap_executed": bool,
            "bootstrap_success": bool,
            "schema_verified": bool,
            "critical_columns_present": bool,
        }
    """
    return {
        "bootstrap_executed": _BOOTSTRAP_STATUS["executed"],
        "bootstrap_success": _BOOTSTRAP_STATUS["success"],
        "schema_verified": _BOOTSTRAP_STATUS["schema_verified"],
        "critical_columns_present": _BOOTSTRAP_STATUS["success"],
    }


# =============================================================================
# Endpoint: GET /admin/schema-status
# Internal endpoint — returns live webhook schema health for debugging.
# =============================================================================


@router.get("/schema-status")
async def get_schema_status(
    db: AsyncSession = Depends(get_db),
):
    """
    Return the live webhook schema health status.

    Queries information_schema directly so the result always reflects the
    current database state, not a cached startup snapshot.

    Returns:
        {
          "ok": bool,
          "critical_columns_present": bool,
          "webhook_columns": {col: bool, ...},
          "webhook_events_table_exists": bool,
          "last_recovery_at": str | null,
          "recovery_status": str,
        }
    """
    from services.migration_recovery import (
        _CRITICAL_CREDENTIAL_COLUMNS,
        _get_existing_credential_columns,
        _webhook_events_table_exists,
    )

    try:
        existing_cols = await _get_existing_credential_columns(db)
        webhook_columns = {c: (c in existing_cols) for c in _CRITICAL_CREDENTIAL_COLUMNS}
        critical_columns_present = all(webhook_columns.values())

        events_table_exists = await _webhook_events_table_exists(db)

        ok = critical_columns_present and events_table_exists

        logger.info(
            "[SCHEMA_STATUS] ok=%s critical_columns_present=%s "
            "events_table_exists=%s",
            ok,
            critical_columns_present,
            events_table_exists,
        )

        return {
            "ok": ok,
            "critical_columns_present": critical_columns_present,
            "webhook_columns": webhook_columns,
            "webhook_events_table_exists": events_table_exists,
            "last_recovery_at": None,
            "recovery_status": "verified" if ok else "schema_incomplete",
        }

    except Exception as exc:
        logger.error(
            "[SCHEMA_STATUS] check_failed error=%s", exc, exc_info=True
        )
        raise HTTPException(
            status_code=500,
            detail=f"Schema status check failed: {str(exc)[:300]}",
        )


@router.post("/seed-noble-nectar-credential")
async def seed_noble_nectar_credential(
    x_admin_seed_token: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
):
    """
    Temporary endpoint to seed noble-nectar LeafLink credential.

    Requires X-Admin-Seed-Token header.
    Uses the backend's existing database connection.
    """

    logger.info("[SEED] noble_nectar_seed_start")

    # Validate token
    if not x_admin_seed_token:
        logger.error("[SEED] missing_token")
        raise HTTPException(status_code=403, detail="Missing X-Admin-Seed-Token header")

    if x_admin_seed_token != ADMIN_SEED_TOKEN:
        logger.error("[SEED] invalid_token")
        raise HTTPException(status_code=403, detail="Invalid X-Admin-Seed-Token")

    try:
        # Execute UPSERT with all required NOT NULL fields
        logger.info("[SEED] executing_upsert")

        await db.execute(
            text("""
                INSERT INTO brand_api_credentials (
                    brand_id,
                    integration_name,
                    company_id,
                    api_key,
                    is_active,
                    sync_status,
                    last_synced_page,
                    total_pages_available,
                    total_orders_available,
                    created_at,
                    updated_at
                ) VALUES (
                    LOWER(TRIM(:brand_id)),
                    LOWER(TRIM(:integration_name)),
                    :company_id,
                    :api_key,
                    true,
                    'idle',
                    0,
                    NULL,
                    NULL,
                    NOW(),
                    NOW()
                )
                ON CONFLICT (brand_id, integration_name)
                DO UPDATE SET
                    brand_id = LOWER(TRIM(EXCLUDED.brand_id)),
                    integration_name = LOWER(TRIM(EXCLUDED.integration_name)),
                    company_id = EXCLUDED.company_id,
                    api_key = EXCLUDED.api_key,
                    is_active = true,
                    sync_status = 'idle',
                    last_synced_page = 0,
                    updated_at = NOW()
            """),
            {
                "brand_id": "noble-nectar",
                "integration_name": "leaflink",
                "company_id": "9008",
                "api_key": "daa1586d10978bb5bc104b0fc63685ae47a6308e",
            }
        )

        await db.commit()
        logger.info("[SEED] upsert_committed")

        # Verify
        logger.info("[SEED] verifying_credential")

        result = await db.execute(
            text("""
                SELECT brand_id, integration_name, company_id, is_active, sync_status, last_synced_page, LENGTH(api_key) AS key_len
                FROM brand_api_credentials
                WHERE TRIM(LOWER(brand_id)) = 'noble-nectar'
                AND TRIM(LOWER(integration_name)) = 'leaflink'
            """)
        )

        row = result.fetchone()

        if not row:
            logger.error("[SEED] verification_failed credential_not_found")
            raise RuntimeError("Credential not found after insert")

        brand_id, integration_name, company_id, is_active, sync_status, last_synced_page, key_len = row

        logger.info(
            "[SEED] noble_nectar_seed_success key_len=%s company_id=%s sync_status=%s last_synced_page=%s",
            key_len,
            company_id,
            sync_status,
            last_synced_page,
        )

        return {
            "ok": True,
            "seeded": True,
            "brand_id": brand_id,
            "integration_name": integration_name,
            "company_id": company_id,
            "is_active": is_active,
            "sync_status": sync_status,
            "last_synced_page": last_synced_page,
            "api_key_len": key_len,
        }

    except HTTPException:
        raise

    except Exception as exc:
        logger.error("[SEED] seed_error error=%s", exc, exc_info=True)
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Seed failed: {str(exc)}",
        )


# =============================================================================
# Pydantic Schemas — Route Publish/Update
# =============================================================================


class UpdateAndPublishBody(BaseModel):
    notes: Optional[str] = None
    assigned_driver_id: Optional[str] = None
    stops_reorder: Optional[List[str]] = None


# =============================================================================
# Helper: org auth for route admin endpoints
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


# =============================================================================
# Endpoint: POST /admin/routes/{route_id}/update-and-publish
# =============================================================================


@router.post("/routes/{route_id}/update-and-publish")
async def update_and_publish_route(
    route_id: str,
    body: UpdateAndPublishBody,
    x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
    x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
    db: AsyncSession = Depends(get_db),
):
    """
    Composite endpoint: apply changes and publish the route in a single operation.

    Applies any provided changes (notes, driver assignment, stop reorder),
    sets published_at = now(), increments version, and transitions status
    from 'draft' to 'assigned' when a driver is present.

    Creates route_events for: published, driver_assigned (if changed),
    stops_reordered (if reorder provided).
    """
    org_id = await _get_org_from_header(x_opsyn_org, x_opsyn_secret, db)
    route = await _get_route_or_404(db, route_id, org_id)

    now = datetime.now(timezone.utc)

    # Use org_id as the actor_id for admin actions (tenant-level actor)
    try:
        actor_id = UUID(org_id)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=400, detail="Invalid org_id format")

    try:
        org_uuid = UUID(org_id)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=400, detail="Invalid org_id format")

    try:
        route_uuid = UUID(route_id)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=400, detail="Invalid route_id format")

    driver_changed = False

    # --- Apply notes ---
    if body.notes is not None:
        route.notes = body.notes

    # --- Apply driver assignment ---
    if body.assigned_driver_id is not None:
        driver_result = await db.execute(
            select(Driver).where(
                Driver.id == body.assigned_driver_id,
                Driver.org_id == org_id,
            )
        )
        driver = driver_result.scalar_one_or_none()
        if driver is None:
            raise HTTPException(
                status_code=400,
                detail="assigned_driver_id does not exist or does not belong to this org",
            )
        if driver.status != "active":
            raise HTTPException(status_code=400, detail="Driver is not active")

        old_driver_id = route.assigned_driver_id
        route.assigned_driver_id = body.assigned_driver_id
        driver_changed = str(old_driver_id) != str(body.assigned_driver_id)

    # --- Apply stop reorder ---
    stops_reordered = False
    if body.stops_reorder:
        stops_result = await db.execute(
            select(RouteStop).where(RouteStop.route_id == route_id)
        )
        existing_stops = stops_result.scalars().all()
        existing_ids = {str(s.id) for s in existing_stops}

        provided_ids = [str(sid) for sid in body.stops_reorder]
        unknown = set(provided_ids) - existing_ids
        if unknown:
            raise HTTPException(
                status_code=400,
                detail=f"The following stop_ids do not belong to this route: {', '.join(unknown)}",
            )
        if len(provided_ids) != len(existing_stops):
            raise HTTPException(
                status_code=400,
                detail=(
                    f"stops_reorder length ({len(provided_ids)}) must match "
                    f"current stop count ({len(existing_stops)})"
                ),
            )

        stop_map = {str(s.id): s for s in existing_stops}
        for new_order, sid in enumerate(provided_ids, start=1):
            stop_map[sid].stop_order = new_order

        stops_reordered = True

    # --- Publish ---
    route.published_at = now
    if route.status == "draft" and route.assigned_driver_id is not None:
        route.status = "assigned"
    route.version = (route.version or 1) + 1
    route.updated_at = now

    # --- Log events (within same transaction) ---
    if driver_changed:
        await log_route_event(
            db=db,
            route_id=route_uuid,
            org_id=org_uuid,
            event_type="driver_assigned",
            actor_type="admin",
            actor_id=actor_id,
            event_metadata={"assigned_driver_id": str(body.assigned_driver_id)},
        )

    if stops_reordered:
        await log_route_event(
            db=db,
            route_id=route_uuid,
            org_id=org_uuid,
            event_type="stops_reordered",
            actor_type="admin",
            actor_id=actor_id,
            event_metadata={"stop_ids": body.stops_reorder},
        )

    await log_route_event(
        db=db,
        route_id=route_uuid,
        org_id=org_uuid,
        event_type="published",
        actor_type="admin",
        actor_id=actor_id,
        event_metadata={
            "version": route.version,
            "assigned_driver_id": str(route.assigned_driver_id) if route.assigned_driver_id else None,
            "stops_reordered": stops_reordered,
        },
    )


    try:
        await db.commit()
        await db.refresh(route)
    except Exception as exc:
        await db.rollback()
        logger.error(
            "[ROUTE_PUBLISHED] commit_failed org_id=%s route_id=%s error=%s",
            org_id, route_id, exc, exc_info=True,
        )
        raise HTTPException(status_code=500, detail="Unexpected error publishing route")

    logger.info(
        "[ROUTE_PUBLISHED] route=%s version=%s driver=%s stops_reordered=%s by=%s",
        route_id,
        route.version,
        route.assigned_driver_id,
        stops_reordered,
        org_id,
    )

    return make_json_safe({
        "ok": True,
        "route_id": str(route.id),
        "version": route.version,
        "published_at": route.published_at,
        "assigned_driver_id": str(route.assigned_driver_id) if route.assigned_driver_id else None,
        "message": "Route updated and sent to driver",
    })



# =============================================================================
# Compliance helpers
# =============================================================================


async def _resolve_driver(db: AsyncSession, driver_id) -> Optional[Driver]:
    """Fetch a Driver by id, returning None if not found."""
    if driver_id is None:
        return None
    result = await db.execute(select(Driver).where(Driver.id == driver_id))
    return result.scalar_one_or_none()


async def _get_stop_timestamps(
    db: AsyncSession,
    route_id,
) -> tuple[Optional[datetime], Optional[datetime]]:
    """
    Return (started_at, completed_at) for a route derived from its stops.

    started_at  = MIN(updated_at) where status = 'arrived'
    completed_at = MAX(completed_at) across all stops
    """
    started_result = await db.execute(
        select(func.min(RouteStop.updated_at)).where(
            RouteStop.route_id == route_id,
            RouteStop.status == "arrived",
        )
    )
    started_at: Optional[datetime] = started_result.scalar_one_or_none()

    completed_result = await db.execute(
        select(func.max(RouteStop.completed_at)).where(
            RouteStop.route_id == route_id,
        )
    )
    completed_at: Optional[datetime] = completed_result.scalar_one_or_none()

    return started_at, completed_at


def _duration_minutes(
    started_at: Optional[datetime],
    completed_at: Optional[datetime],
) -> Optional[float]:
    """Return duration in minutes between two timestamps, or None if either is missing."""
    if started_at is None or completed_at is None:
        return None
    delta = completed_at - started_at
    return round(delta.total_seconds() / 60, 1)


async def _get_source_order_number(db: AsyncSession, source_order_id) -> Optional[str]:
    """
    Look up the order_number from the orders table for a given source_order_id (UUID).

    source_order_id is stored as a UUID in route_stops, but Order.id is an integer.
    We attempt integer conversion; if that fails we return None.
    """
    if source_order_id is None:
        return None
    try:
        int_id = int(str(source_order_id))
    except (ValueError, TypeError):
        return None
    result = await db.execute(
        select(Order.order_number).where(Order.id == int_id)
    )
    row = result.fetchone()
    return row[0] if row else None


async def _get_org_info(db: AsyncSession, org_id: str) -> dict:
    """
    Fetch org name and code from the organizations table.
    Falls back gracefully if the table or row is missing.
    """
    try:
        result = await db.execute(
            text(
                "SELECT id, name, org_code FROM organizations WHERE id = :org_id LIMIT 1"
            ),
            {"org_id": org_id},
        )
        row = result.fetchone()
        if row:
            return {"id": str(row[0]), "name": row[1], "code": row[2]}
    except Exception:
        pass
    return {"id": org_id, "name": None, "code": None}


# =============================================================================
# Endpoint: GET /admin/routes/history
# NOTE: Registered before /{route_id} endpoints so FastAPI matches the literal
#       path segment "history" before the parameterized {route_id} pattern.
# =============================================================================


@router.get("/routes/history")
async def get_routes_history(
    x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
    x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
    status: Optional[str] = Query(default="completed"),
    driver_id: Optional[str] = Query(default=None),
    date_from: Optional[str] = Query(default=None, description="ISO date YYYY-MM-DD"),
    date_to: Optional[str] = Query(default=None, description="ISO date YYYY-MM-DD"),
    search: Optional[str] = Query(default=None, description="Search route_number, driver name, or stop customer_name"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """
    Return paginated route history for compliance and the 'Previous Routes' admin tab.

    All results are scoped to the authenticated org. Supports filtering by status,
    driver, date range, and free-text search across route_number, driver name, and
    stop customer_name.
    """
    org_id = await _get_org_from_header(x_opsyn_org, x_opsyn_secret, db)

    # --- Parse and validate date filters ---
    parsed_date_from: Optional[date] = None
    parsed_date_to: Optional[date] = None
    if date_from:
        try:
            parsed_date_from = date.fromisoformat(date_from)
        except ValueError:
            raise HTTPException(status_code=400, detail="date_from must be a valid ISO date (YYYY-MM-DD)")
    if date_to:
        try:
            parsed_date_to = date.fromisoformat(date_to)
        except ValueError:
            raise HTTPException(status_code=400, detail="date_to must be a valid ISO date (YYYY-MM-DD)")

    # --- Build base query ---
    query = select(Route).where(Route.org_id == org_id)

    if status:
        query = query.where(Route.status == status)
    if parsed_date_from:
        query = query.where(Route.route_date >= parsed_date_from)
    if parsed_date_to:
        query = query.where(Route.route_date <= parsed_date_to)
    if driver_id:
        try:
            driver_uuid = UUID(driver_id)
        except ValueError:
            raise HTTPException(status_code=400, detail="driver_id must be a valid UUID")
        query = query.where(Route.assigned_driver_id == driver_uuid)

    # --- Search filter: route_number or driver name or stop customer_name ---
    if search:
        search_term = f"%{search}%"
        # Subquery: route IDs that have a matching stop customer_name
        stop_match_subq = (
            select(RouteStop.route_id)
            .where(
                RouteStop.org_id == org_id,
                RouteStop.customer_name.ilike(search_term),
            )
            .scalar_subquery()
        )
        # Driver name match: join Driver inline
        driver_match_subq = (
            select(Driver.id)
            .where(
                Driver.org_id == org_id,
                Driver.name.ilike(search_term),
            )
            .scalar_subquery()
        )
        query = query.where(
            or_(
                Route.route_number.ilike(search_term),
                Route.assigned_driver_id.in_(driver_match_subq),
                Route.id.in_(stop_match_subq),
            )
        )

    query = query.order_by(Route.route_date.desc(), Route.created_at.desc())
    query = query.offset(offset).limit(limit)

    result = await db.execute(query)
    routes = result.scalars().all()

    # --- Serialize each route with stop counts and financial totals ---
    serialized = []
    for route in routes:
        # Fetch driver
        driver = await _resolve_driver(db, route.assigned_driver_id)

        # Fetch stop aggregates
        stops_result = await db.execute(
            select(RouteStop).where(RouteStop.route_id == route.id)
        )
        stops = stops_result.scalars().all()

        stop_count = len(stops)
        completed_stop_count = sum(1 for s in stops if s.status == "completed")
        failed_stop_count = sum(1 for s in stops if s.status == "failed")
        skipped_stop_count = sum(1 for s in stops if s.status == "skipped")
        total_collected = sum(
            (s.amount_collected or 0) for s in stops
        )

        # Derive timestamps from stops
        started_at, completed_at = await _get_stop_timestamps(db, route.id)
        duration = _duration_minutes(started_at, completed_at)

        serialized.append(make_json_safe({
            "route_id": route.id,
            "route_number": route.route_number,
            "route_date": route.route_date,
            "status": route.status,
            "assigned_driver": {
                "id": driver.id if driver else None,
                "name": driver.name if driver else None,
                "phone": driver.phone if driver else None,
                "license_plate": driver.license_plate if driver else None,
            } if driver else None,
            "stop_count": stop_count,
            "completed_stop_count": completed_stop_count,
            "failed_stop_count": failed_stop_count,
            "skipped_stop_count": skipped_stop_count,
            "total_value": route.total_value,
            "total_collected": total_collected,
            "total_units": route.total_units,
            "started_at": started_at,
            "completed_at": completed_at,
            "published_at": route.published_at,
            "duration_minutes": duration,
        }))

    filters_applied = {
        "status": status,
        "driver_id": driver_id,
        "date_from": date_from,
        "date_to": date_to,
        "search": search,
    }
    logger.info(
        "[ROUTES_HISTORY] org_id=%s count=%s filters=%s",
        org_id,
        len(serialized),
        filters_applied,
    )

    return {"ok": True, "count": len(serialized), "routes": serialized}


# =============================================================================
# Endpoint: GET /admin/routes/history/export
# NOTE: Registered before /{route_id} endpoints so FastAPI matches the literal
#       path segment "history" before the parameterized {route_id} pattern.
# =============================================================================


@router.get("/routes/history/export")
async def export_routes_history(
    x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
    x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
    status: Optional[str] = Query(default=None),
    driver_id: Optional[str] = Query(default=None),
    date_from: Optional[str] = Query(default=None, description="ISO date YYYY-MM-DD"),
    date_to: Optional[str] = Query(default=None, description="ISO date YYYY-MM-DD"),
    db: AsyncSession = Depends(get_db),
):
    """
    Export route history as a CSV file download.

    Applies the same filters as GET /routes/history. Returns a CSV attachment
    suitable for regulatory record-keeping (METRC, OMMA).
    """
    org_id = await _get_org_from_header(x_opsyn_org, x_opsyn_secret, db)

    # --- Parse and validate date filters ---
    parsed_date_from: Optional[date] = None
    parsed_date_to: Optional[date] = None
    if date_from:
        try:
            parsed_date_from = date.fromisoformat(date_from)
        except ValueError:
            raise HTTPException(status_code=400, detail="date_from must be a valid ISO date (YYYY-MM-DD)")
    if date_to:
        try:
            parsed_date_to = date.fromisoformat(date_to)
        except ValueError:
            raise HTTPException(status_code=400, detail="date_to must be a valid ISO date (YYYY-MM-DD)")

    # --- Build query (no pagination — export all matching rows) ---
    query = select(Route).where(Route.org_id == org_id)

    if status:
        query = query.where(Route.status == status)
    if parsed_date_from:
        query = query.where(Route.route_date >= parsed_date_from)
    if parsed_date_to:
        query = query.where(Route.route_date <= parsed_date_to)
    if driver_id:
        try:
            driver_uuid = UUID(driver_id)
        except ValueError:
            raise HTTPException(status_code=400, detail="driver_id must be a valid UUID")
        query = query.where(Route.assigned_driver_id == driver_uuid)

    query = query.order_by(Route.route_date.desc(), Route.created_at.desc())

    result = await db.execute(query)
    routes = result.scalars().all()

    # --- Build CSV in memory ---
    output = io.StringIO()
    writer = csv.writer(output)

    # Header row
    writer.writerow([
        "route_date",
        "route_number",
        "driver_name",
        "driver_license_plate",
        "status",
        "total_stops",
        "completed_stops",
        "failed_stops",
        "total_value",
        "total_collected",
        "started_at",
        "completed_at",
        "duration_minutes",
        "stop_details",
    ])

    for route in routes:
        driver = await _resolve_driver(db, route.assigned_driver_id)

        stops_result = await db.execute(
            select(RouteStop)
            .where(RouteStop.route_id == route.id)
            .order_by(RouteStop.stop_order.asc())
        )
        stops = stops_result.scalars().all()

        completed_stop_count = sum(1 for s in stops if s.status == "completed")
        failed_stop_count = sum(1 for s in stops if s.status == "failed")
        total_collected = float(sum((s.amount_collected or 0) for s in stops))

        started_at, completed_at = await _get_stop_timestamps(db, route.id)
        duration = _duration_minutes(started_at, completed_at)

        # Build semicolon-separated stop summary
        stop_parts = []
        for s in stops:
            stop_parts.append(
                f"{s.stop_order}:{s.customer_name or s.stop_name or 'unknown'}:"
                f"{s.status}:${float(s.amount_collected or 0):.2f}"
            )
        stop_details = "; ".join(stop_parts)

        writer.writerow([
            route.route_date.isoformat() if route.route_date else "",
            route.route_number or "",
            driver.name if driver else "",
            driver.license_plate if driver else "",
            route.status,
            len(stops),
            completed_stop_count,
            failed_stop_count,
            float(route.total_value or 0),
            total_collected,
            started_at.isoformat() if started_at else "",
            completed_at.isoformat() if completed_at else "",
            duration if duration is not None else "",
            stop_details,
        ])

    csv_content = output.getvalue()
    output.close()

    # Build filename
    date_from_str = date_from or "all"
    date_to_str = date_to or "all"
    filename = f"route_history_{org_id}_{date_from_str}_{date_to_str}.csv"

    logger.info(
        "[ROUTES_HISTORY_EXPORT] org_id=%s rows=%s date_range=%s_%s",
        org_id,
        len(routes),
        date_from_str,
        date_to_str,
    )

    return StreamingResponse(
        iter([csv_content]),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
        },
    )


# =============================================================================
# Endpoint: GET /admin/routes/{route_id}/changelog
# =============================================================================


@router.get("/routes/{route_id}/changelog")
async def get_route_changelog(
    route_id: str,
    x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
    x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
    db: AsyncSession = Depends(get_db),
):
    """
    Return the audit log (changelog) for a route.

    Lists all route_events ordered by created_at DESC, providing a full
    history of version changes, driver assignments, stop updates, and
    route completion.
    """
    org_id = await _get_org_from_header(x_opsyn_org, x_opsyn_secret, db)
    route = await _get_route_or_404(db, route_id, org_id)

    events_result = await db.execute(
        select(RouteEvent)
        .where(RouteEvent.route_id == route.id)
        .order_by(RouteEvent.created_at.desc())
    )
    events = events_result.scalars().all()

    changes = [
        make_json_safe({
            "event_id": str(event.id),
            "event_type": event.event_type,
            "actor_type": event.actor_type,
            "actor_id": str(event.actor_id),
            "metadata": event.event_metadata,
            "created_at": event.created_at,
        })
        for event in events
    ]


    logger.info(
        "[ROUTE_CHANGELOG] org_id=%s route_id=%s event_count=%s",
        org_id, route_id, len(changes),
    )

    return make_json_safe({
        "ok": True,
        "route_id": route_id,
        "version": route.version,
        "updated_at": route.updated_at,
        "changes": changes,
    })


# =============================================================================
# Endpoint: GET /admin/routes/{route_id}/live-status
# =============================================================================


@router.get("/routes/{route_id}/live-status")
async def get_route_live_status(
    route_id: str,
    x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
    x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
    db: AsyncSession = Depends(get_db),
):
    """
    Return real-time stop statuses for the route.

    Provides a lightweight snapshot of all stop statuses, collection amounts,
    and AR statuses — suitable for admin dashboards polling for live progress.
    """
    org_id = await _get_org_from_header(x_opsyn_org, x_opsyn_secret, db)
    route = await _get_route_or_404(db, route_id, org_id)

    stops_result = await db.execute(
        select(RouteStop)
        .where(RouteStop.route_id == route.id)
        .order_by(RouteStop.stop_order.asc())
    )
    stops = stops_result.scalars().all()

    serialized_stops = [
        make_json_safe({
            "stop_id": str(stop.id),
            "stop_order": stop.stop_order,
            "status": stop.status,
            "completed_at": stop.completed_at,
            "amount_collected": stop.amount_collected,
            "ar_status": stop.ar_status,
            "updated_at": stop.updated_at,
        })
        for stop in stops
    ]

    logger.info(
        "[ROUTE_LIVE_STATUS] org_id=%s route_id=%s stop_count=%s",
        org_id, route_id, len(serialized_stops),
    )

    return make_json_safe({
        "ok": True,
        "route_id": route_id,
        "version": route.version,
        "status": route.status,
        "stops": serialized_stops,
    })


# =============================================================================
# Endpoint: GET /admin/routes/{route_id}/compliance-report
# =============================================================================


@router.get("/routes/{route_id}/compliance-report")
async def get_compliance_report(
    route_id: str,
    x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
    x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
    db: AsyncSession = Depends(get_db),
):
    """
    Full compliance-ready detail for a single route.

    Returns all stop-level data, financial totals, driver info, and org metadata
    in a format suitable for METRC / OMMA regulatory inspections.
    """
    org_id = await _get_org_from_header(x_opsyn_org, x_opsyn_secret, db)
    route = await _get_route_or_404(db, route_id, org_id)

    now = datetime.now(timezone.utc)

    # Fetch org info
    org_info = await _get_org_info(db, org_id)

    # Fetch driver
    driver = await _resolve_driver(db, route.assigned_driver_id)

    # Fetch stops ordered by stop_order
    stops_result = await db.execute(
        select(RouteStop)
        .where(RouteStop.route_id == route.id)
        .order_by(RouteStop.stop_order.asc())
    )
    stops = stops_result.scalars().all()

    # Derive route-level timestamps from stops
    started_at, completed_at = await _get_stop_timestamps(db, route.id)
    duration = _duration_minutes(started_at, completed_at)

    # Compute totals
    total_stops = len(stops)
    completed_count = sum(1 for s in stops if s.status == "completed")
    failed_count = sum(1 for s in stops if s.status == "failed")
    skipped_count = sum(1 for s in stops if s.status == "skipped")
    pending_count = sum(1 for s in stops if s.status == "pending")
    total_value = float(route.total_value or 0)
    total_collected = float(sum((s.amount_collected or 0) for s in stops))
    collection_rate = (
        round((total_collected / total_value) * 100, 1)
        if total_value > 0
        else 0.0
    )
    total_units = route.total_units or 0

    # Serialize stops with source_order_number lookup
    serialized_stops = []
    for stop in stops:
        source_order_number = await _get_source_order_number(db, stop.source_order_id)
        serialized_stops.append(make_json_safe({
            "stop_order": stop.stop_order,
            "stop_type": stop.stop_type,
            "status": stop.status,
            "customer_name": stop.customer_name,
            "stop_name": stop.stop_name,
            "address": stop.address,
            "contact_name": stop.contact_name,
            "contact_phone": stop.contact_phone,
            "source_order_id": stop.source_order_id,
            "source_order_number": source_order_number,
            "amount_due": stop.amount_due,
            "amount_collected": stop.amount_collected,
            "ar_status": stop.ar_status,
            "completed_at": stop.completed_at,
            "notes": stop.notes,
            "time_window": stop.time_window,
        }))

    logger.info(
        "[COMPLIANCE_REPORT] org_id=%s route_id=%s generated_at=%s",
        org_id,
        route_id,
        now.isoformat(),
    )

    return make_json_safe({
        "ok": True,
        "route_id": route.id,
        "route_number": route.route_number,
        "route_date": route.route_date,
        "status": route.status,
        "org": org_info,
        "driver": {
            "id": driver.id,
            "name": driver.name,
            "phone": driver.phone,
            "license_plate": driver.license_plate,
            "vehicle_type": driver.vehicle_type,
        } if driver else None,
        "published_at": route.published_at,
        "started_at": started_at,
        "completed_at": completed_at,
        "duration_minutes": duration,
        "version": route.version,
        "stops": serialized_stops,
        "totals": {
            "total_stops": total_stops,
            "completed": completed_count,
            "failed": failed_count,
            "skipped": skipped_count,
            "pending": pending_count,
            "total_value": total_value,
            "total_collected": total_collected,
            "collection_rate_percent": collection_rate,
            "total_units": total_units,
        },
        "metadata": {
            "created_at": route.created_at,
            "updated_at": route.updated_at,
            "created_by": route.created_by,
            "version": route.version,
            "report_generated_at": now,
        },
    })


# =============================================================================
# Endpoint: GET /admin/routes/{route_id}/events
# =============================================================================


@router.get("/routes/{route_id}/events")
async def get_route_events(
    route_id: str,
    x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
    x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
    db: AsyncSession = Depends(get_db),
):
    """
    Return all route_events for a route ordered by created_at ASC.

    Used by the admin changelog and compliance audit trail. Provides a
    complete, immutable record of every action taken on the route.
    """
    org_id = await _get_org_from_header(x_opsyn_org, x_opsyn_secret, db)
    route = await _get_route_or_404(db, route_id, org_id)

    events_result = await db.execute(
        select(RouteEvent)
        .where(RouteEvent.route_id == route.id)
        .order_by(RouteEvent.created_at.asc())
    )
    events = events_result.scalars().all()

    serialized_events = [
        make_json_safe({
            "event_id": event.id,
            "event_type": event.event_type,
            "actor_type": event.actor_type,
            "actor_id": event.actor_id,
            "metadata": event.event_metadata,
            "created_at": event.created_at,
        })
        for event in events
    ]

    logger.info(
        "[ROUTE_EVENTS] org_id=%s route_id=%s count=%s",
        org_id,
        route_id,
        len(serialized_events),
    )

    return make_json_safe({
        "ok": True,
        "route_id": route_id,
        "events": serialized_events,
    })


# =============================================================================
# Endpoint: GET /admin/drivers/{driver_id}/location
# =============================================================================


@router.get("/drivers/{driver_id}/location")
async def get_driver_current_location(
    driver_id: str,
    x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
    x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
    db: AsyncSession = Depends(get_db),
):
    """
    Return the most recent GPS location for a driver.

    Returns 404 if no location data exists for this driver.
    """
    org_id = await _get_org_from_header(x_opsyn_org, x_opsyn_secret, db)

    try:
        driver_uuid = UUID(driver_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="driver_id must be a valid UUID")

    result = await db.execute(
        select(DriverLocation)
        .where(
            DriverLocation.driver_id == driver_uuid,
            DriverLocation.org_id == org_id,
        )
        .order_by(DriverLocation.recorded_at.desc())
        .limit(1)
    )
    loc = result.scalar_one_or_none()

    if loc is None:
        raise HTTPException(status_code=404, detail="No location data found for this driver")

    logger.info(
        "[DRIVER_LOCATION_CURRENT] org=%s driver=%s",
        org_id,
        driver_id,
    )

    return make_json_safe({
        "ok": True,
        "location": {
            "driver_id": loc.driver_id,
            "latitude": loc.latitude,
            "longitude": loc.longitude,
            "speed_mph": loc.speed_mph,
            "heading": loc.heading,
            "battery_percent": loc.battery_percent,
            "is_moving": loc.is_moving,
            "recorded_at": loc.recorded_at,
            "route_id": loc.route_id,
        },
    })


# =============================================================================
# Endpoint: GET /admin/drivers/{driver_id}/location/history
# =============================================================================


@router.get("/drivers/{driver_id}/location/history")
async def get_driver_location_history(
    driver_id: str,
    since: str = Query(..., description="ISO 8601 timestamp (required)"),
    until: Optional[str] = Query(default=None, description="ISO 8601 timestamp (default: now)"),
    limit: int = Query(default=500, ge=1, le=5000),
    x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
    x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
    db: AsyncSession = Depends(get_db),
):
    """
    Return GPS location history for a driver between since and until.

    Points are ordered by recorded_at ASC. Maximum 5000 points per request.
    """
    org_id = await _get_org_from_header(x_opsyn_org, x_opsyn_secret, db)

    try:
        driver_uuid = UUID(driver_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="driver_id must be a valid UUID")

    try:
        since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
        if since_dt.tzinfo is None:
            since_dt = since_dt.replace(tzinfo=timezone.utc)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="since must be a valid ISO 8601 timestamp",
        )

    until_dt: datetime
    if until:
        try:
            until_dt = datetime.fromisoformat(until.replace("Z", "+00:00"))
            if until_dt.tzinfo is None:
                until_dt = until_dt.replace(tzinfo=timezone.utc)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="until must be a valid ISO 8601 timestamp",
            )
    else:
        until_dt = datetime.now(timezone.utc)

    result = await db.execute(
        select(DriverLocation)
        .where(
            DriverLocation.driver_id == driver_uuid,
            DriverLocation.org_id == org_id,
            DriverLocation.recorded_at >= since_dt,
            DriverLocation.recorded_at <= until_dt,
        )
        .order_by(DriverLocation.recorded_at.asc())
        .limit(limit)
    )
    locations = result.scalars().all()

    serialized = [
        make_json_safe({
            "latitude": loc.latitude,
            "longitude": loc.longitude,
            "speed_mph": loc.speed_mph,
            "heading": loc.heading,
            "accuracy_meters": loc.accuracy_meters,
            "battery_percent": loc.battery_percent,
            "is_moving": loc.is_moving,
            "source": loc.source,
            "recorded_at": loc.recorded_at,
            "route_id": loc.route_id,
        })
        for loc in locations
    ]

    logger.info(
        "[DRIVER_LOCATION_HISTORY] org=%s driver=%s count=%s",
        org_id,
        driver_id,
        len(serialized),
    )

    return {"ok": True, "count": len(serialized), "locations": serialized}


# =============================================================================
# Endpoint: GET /admin/routes/{route_id}/tracking
# =============================================================================


@router.get("/routes/{route_id}/tracking")
async def get_route_tracking(
    route_id: str,
    x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
    x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
    db: AsyncSession = Depends(get_db),
):
    """
    Return a combined real-time tracking view for a route.

    Includes:
    - current_location: latest GPS ping for the assigned driver
    - history_events: all driver_route_history rows ordered by recorded_at ASC
    - stops: all route_stops with status and timestamps
    """
    org_id = await _get_org_from_header(x_opsyn_org, x_opsyn_secret, db)
    route = await _get_route_or_404(db, route_id, org_id)

    # Current location for assigned driver
    current_location = None
    if route.assigned_driver_id is not None:
        loc_result = await db.execute(
            select(DriverLocation)
            .where(
                DriverLocation.driver_id == route.assigned_driver_id,
                DriverLocation.org_id == org_id,
            )
            .order_by(DriverLocation.recorded_at.desc())
            .limit(1)
        )
        loc = loc_result.scalar_one_or_none()
        if loc is not None:
            current_location = make_json_safe({
                "driver_id": loc.driver_id,
                "latitude": loc.latitude,
                "longitude": loc.longitude,
                "speed_mph": loc.speed_mph,
                "heading": loc.heading,
                "battery_percent": loc.battery_percent,
                "is_moving": loc.is_moving,
                "recorded_at": loc.recorded_at,
            })

    # Route history events
    history_result = await db.execute(
        select(DriverRouteHistory)
        .where(DriverRouteHistory.route_id == route.id)
        .order_by(DriverRouteHistory.recorded_at.asc())
    )
    history_events = history_result.scalars().all()

    serialized_history = [
        make_json_safe({
            "id": event.id,
            "event_type": event.event_type,
            "stop_id": event.stop_id,
            "latitude": event.latitude,
            "longitude": event.longitude,
            "address_snapshot": event.address_snapshot,
            "event_metadata": event.event_metadata,
            "notes": event.notes,
            "recorded_at": event.recorded_at,
        })
        for event in history_events
    ]

    # Stops
    stops_result = await db.execute(
        select(RouteStop)
        .where(RouteStop.route_id == route.id)
        .order_by(RouteStop.stop_order.asc())
    )
    stops = stops_result.scalars().all()

    serialized_stops = [
        make_json_safe({
            "id": stop.id,
            "stop_order": stop.stop_order,
            "customer_name": stop.customer_name,
            "stop_name": stop.stop_name,
            "address": stop.address,
            "status": stop.status,
            "completed_at": stop.completed_at,
            "updated_at": stop.updated_at,
        })
        for stop in stops
    ]

    logger.info(
        "[ROUTE_TRACKING] org=%s route=%s",
        org_id,
        route_id,
    )

    return make_json_safe({
        "ok": True,
        "route_id": route_id,
        "current_location": current_location,
        "history_events": serialized_history,
        "stops": serialized_stops,
    })


# =============================================================================
# Endpoint: GET /admin/routes/{route_id}/timeline
# =============================================================================


@router.get("/routes/{route_id}/timeline")
async def get_route_timeline(
    route_id: str,
    x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
    x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
    db: AsyncSession = Depends(get_db),
):
    """
    Return a human-readable timeline for a route built from driver_route_history.

    Events are ordered chronologically. Times are formatted as "H:MM AM/PM"
    using the UTC timestamp (ISO fallback if no timezone info).
    """
    org_id = await _get_org_from_header(x_opsyn_org, x_opsyn_secret, db)
    route = await _get_route_or_404(db, route_id, org_id)

    history_result = await db.execute(
        select(DriverRouteHistory)
        .where(DriverRouteHistory.route_id == route.id)
        .order_by(DriverRouteHistory.recorded_at.asc())
    )
    history_events = history_result.scalars().all()

    # Build stop name lookup for stop_id → display name
    stops_result = await db.execute(
        select(RouteStop).where(RouteStop.route_id == route.id)
    )
    stops = stops_result.scalars().all()
    stop_name_map = {
        str(s.id): s.customer_name or s.stop_name or s.address or "Stop"
        for s in stops
    }

    # Human-readable event labels
    _event_labels = {
        "route_started": "Route started",
        "stop_arrived": "Arrived at stop",
        "stop_departed": "Departed stop",
        "stop_completed": "Stop completed",
        "stop_failed": "Stop failed",
        "stop_skipped": "Stop skipped",
        "route_completed": "Route completed",
        "route_paused": "Route paused",
        "route_resumed": "Route resumed",
        "break_started": "Break started",
        "break_ended": "Break ended",
        "deviation_detected": "Route deviation detected",
    }

    timeline = []
    for event in history_events:
        # Format time
        ts = event.recorded_at
        if ts is not None:
            try:
                time_str = ts.strftime("%-I:%M %p")
            except ValueError:
                time_str = ts.isoformat()
        else:
            time_str = "Unknown time"

        label = _event_labels.get(event.event_type, event.event_type)

        # Build location string
        location_str = event.address_snapshot
        if location_str is None and event.stop_id is not None:
            location_str = stop_name_map.get(str(event.stop_id))
        if location_str is None and event.latitude is not None and event.longitude is not None:
            location_str = f"{event.latitude}, {event.longitude}"

        timeline.append({
            "time": time_str,
            "event": label,
            "event_type": event.event_type,
            "location": location_str,
            "stop_id": str(event.stop_id) if event.stop_id else None,
            "notes": event.notes,
            "recorded_at": event.recorded_at.isoformat() if event.recorded_at else None,
        })

    logger.info(
        "[ROUTE_TIMELINE] org=%s route=%s events=%s",
        org_id,
        route_id,
        len(timeline),
    )

    return make_json_safe({
        "ok": True,
        "route_id": route_id,
        "timeline": timeline,
    })
