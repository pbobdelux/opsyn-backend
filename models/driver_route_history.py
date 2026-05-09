"""
SQLAlchemy ORM model for the driver_route_history table.

Stores significant route lifecycle events with timestamps and optional
GPS coordinates. Used for route replay, breadcrumb trails, timeline views,
and break tracking. Retained indefinitely (no archival policy).
"""

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Numeric,
    Text,
    VARCHAR,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base


class DriverRouteHistory(Base):
    __tablename__ = "driver_route_history"

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    driver_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("drivers.id", ondelete="CASCADE"),
        nullable=False,
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
    event_type: Mapped[str] = mapped_column(VARCHAR(30), nullable=False)
    stop_id: Mapped[UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("route_stops.id", ondelete="SET NULL"),
        nullable=True,
    )
    latitude: Mapped[Decimal | None] = mapped_column(Numeric(10, 7), nullable=True)
    longitude: Mapped[Decimal | None] = mapped_column(Numeric(10, 7), nullable=True)
    address_snapshot: Mapped[str | None] = mapped_column(Text, nullable=True)
    event_metadata: Mapped[dict | None] = mapped_column(
        JSONB, nullable=True, server_default="{}"
    )
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    __table_args__ = (
        CheckConstraint(
            "event_type IN ("
            "'route_started','stop_arrived','stop_departed','stop_completed',"
            "'stop_failed','stop_skipped','route_completed','route_paused',"
            "'route_resumed','break_started','break_ended','deviation_detected'"
            ")",
            name="ck_driver_route_history_event_type",
        ),
        Index("ix_driver_route_history_route", "route_id", "recorded_at"),
        Index("ix_driver_route_history_driver", "driver_id", "recorded_at"),
    )
