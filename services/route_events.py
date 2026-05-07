"""
Route event logging service.

Provides a single helper function for creating RouteEvent audit records.
All route-modifying operations (publish, driver assignment, stop status
changes, collection recording, stop reordering, route completion) should
call log_route_event() so that a complete audit trail is maintained.

Events are written within the caller's existing database transaction —
the caller is responsible for committing.
"""

import logging
from typing import Optional
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from models.route_event import RouteEvent

logger = logging.getLogger("route_events")

# Valid event types (mirrors DB check constraint)
VALID_EVENT_TYPES = frozenset({
    "published",
    "driver_assigned",
    "stop_status_changed",
    "collection_recorded",
    "stops_reordered",
    "route_completed",
})

# Valid actor types (mirrors DB check constraint)
VALID_ACTOR_TYPES = frozenset({"admin", "driver"})


async def log_route_event(
    db: AsyncSession,
    route_id: UUID,
    org_id: UUID,
    event_type: str,
    actor_type: str,
    actor_id: UUID,
    event_metadata: Optional[dict] = None,
) -> RouteEvent:
    """
    Log a route event for audit trail.

    Creates a RouteEvent record and adds it to the current session.
    The caller must commit the session for the event to be persisted.

    Args:
        db:             Active async database session.
        route_id:       UUID of the route this event belongs to.
        org_id:         UUID of the organization (for multi-tenant scoping).
        event_type:     One of: published, driver_assigned, stop_status_changed,
                        collection_recorded, stops_reordered, route_completed.
        actor_type:     Either 'admin' or 'driver'.
        actor_id:       UUID of the admin user or driver performing the action.
        event_metadata: Optional dict of additional context (stop_id, status, etc.).

    Returns:
        The newly created RouteEvent ORM instance (not yet committed).

    Raises:
        ValueError: If event_type or actor_type is not a recognized value.
    """
    if event_type not in VALID_EVENT_TYPES:
        raise ValueError(
            f"Invalid event_type '{event_type}'. "
            f"Must be one of: {', '.join(sorted(VALID_EVENT_TYPES))}"
        )

    if actor_type not in VALID_ACTOR_TYPES:
        raise ValueError(
            f"Invalid actor_type '{actor_type}'. "
            f"Must be one of: {', '.join(sorted(VALID_ACTOR_TYPES))}"
        )

    event = RouteEvent(
        route_id=route_id,
        org_id=org_id,
        event_type=event_type,
        actor_type=actor_type,
        actor_id=actor_id,
        event_metadata=event_metadata,
    )
    db.add(event)

    logger.info(
        "[ROUTE_EVENT] event_type=%s route_id=%s actor_type=%s actor_id=%s",
        event_type,
        route_id,
        actor_type,
        actor_id,
    )

    return event
