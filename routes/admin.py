import logging
import os
from datetime import datetime, timezone
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models.driver import Driver
from models.route import Route
from models.route_event import RouteEvent
from models.route_stop import RouteStop
from services.route_events import log_route_event
from utils.json_utils import make_json_safe

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/admin", tags=["admin"])

# Admin seed token from environment
ADMIN_SEED_TOKEN = os.getenv("ADMIN_SEED_TOKEN", "opsyn-seed-2026")



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
            metadata={"assigned_driver_id": str(body.assigned_driver_id)},
        )

    if stops_reordered:
        await log_route_event(
            db=db,
            route_id=route_uuid,
            org_id=org_uuid,
            event_type="stops_reordered",
            actor_type="admin",
            actor_id=actor_id,
            metadata={"stop_ids": body.stops_reorder},
        )

    await log_route_event(
        db=db,
        route_id=route_uuid,
        org_id=org_uuid,
        event_type="published",
        actor_type="admin",
        actor_id=actor_id,
        metadata={
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
            "metadata": event.metadata,
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
