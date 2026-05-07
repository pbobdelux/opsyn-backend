"""
Location service helpers for driver GPS tracking.

Provides get_active_route_for_driver() with a simple in-memory cache
(5-minute TTL) to avoid hitting the routes table on every location ping.
The cache is a plain dict passed in by the caller so it can be scoped
to a module-level singleton in the router.
"""

import logging
from datetime import date, datetime, timezone
from typing import Optional
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models.route import Route

logger = logging.getLogger("location_service")


async def get_active_route_for_driver(
    db: AsyncSession,
    driver_id: UUID,
    org_id: UUID,
    cache: Optional[dict] = None,
) -> Optional[UUID]:
    """
    Find the active route for a driver on today's date.

    Returns route_id (UUID) if an active route is found, None otherwise.

    Caches the result for 5 minutes to avoid querying the routes table on
    every location ping. The cache dict is keyed by driver_id and stores
    (route_id, cached_at) tuples. Pass None to skip caching entirely.

    Args:
        db:        Active async database session.
        driver_id: UUID of the driver.
        org_id:    UUID of the organization (for multi-tenant scoping).
        cache:     Optional dict used as a simple in-memory TTL cache.
                   Format: { driver_id: (route_id | None, datetime) }

    Returns:
        UUID of the active route, or None if no active route exists today.
    """
    # Check cache first (5-minute TTL)
    if cache is not None and driver_id in cache:
        cached_route_id, cached_at = cache[driver_id]
        age_seconds = (datetime.now(timezone.utc) - cached_at).total_seconds()
        if age_seconds < 300:
            return cached_route_id

    # Query for active route assigned to this driver today
    today = date.today()
    result = await db.execute(
        select(Route.id).where(
            Route.assigned_driver_id == driver_id,
            Route.org_id == org_id,
            Route.route_date == today,
            Route.status.in_(["assigned", "out_for_delivery"]),
        ).limit(1)
    )
    route_id: Optional[UUID] = result.scalar_one_or_none()

    # Update cache
    if cache is not None:
        cache[driver_id] = (route_id, datetime.now(timezone.utc))

    logger.debug(
        "[ACTIVE_ROUTE_LOOKUP] driver=%s org=%s route=%s",
        driver_id,
        org_id,
        route_id,
    )

    return route_id
