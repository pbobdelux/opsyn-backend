"""
SQLAlchemy ORM model for the route_events table.

RouteEvents provide an immutable audit trail for all significant actions
taken on a route — publishing, driver assignment, stop status changes,
collection recording, stop reordering, and route completion.

Scoped by org_id for multi-tenant isolation.
UUID primary keys are used throughout for distributed-safe ID generation.
"""

from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    VARCHAR,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from database import Base


class RouteEvent(Base):
    __tablename__ = "route_events"

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    route_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("routes.id", ondelete="CASCADE"),
        nullable=False,
    )
    org_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        nullable=False,
    )
    event_type: Mapped[str] = mapped_column(VARCHAR(50), nullable=False)
    actor_type: Mapped[str] = mapped_column(VARCHAR(20), nullable=False)
    actor_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        nullable=False,
    )
    event_metadata: Mapped[dict | None] = mapped_column(JSONB, nullable=True, server_default="{}")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    __table_args__ = (
        CheckConstraint(
            "event_type IN ('published', 'driver_assigned', 'stop_status_changed', "
            "'collection_recorded', 'stops_reordered', 'route_completed')",
            name="ck_event_type",
        ),
        CheckConstraint(
            "actor_type IN ('admin', 'driver')",
            name="ck_actor_type",
        ),
        Index("ix_route_events_route_id", "route_id"),
        Index("ix_route_events_org_id", "org_id"),
        Index("ix_route_events_created_at", "created_at"),
    )
