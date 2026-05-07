"""
SQLAlchemy ORM model for the driver_locations table.

Stores real-time GPS pings from drivers. High-volume table — each driver
sends a location update every few seconds while on a route. Indexed for
fast lookups by driver+time and by route.

NOTE: In production, partition by recorded_at monthly and archive rows
older than 90 days to cold storage.
"""

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    VARCHAR,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from database import Base


class DriverLocation(Base):
    __tablename__ = "driver_locations"

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
    org_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        nullable=False,
    )
    route_id: Mapped[UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("routes.id", ondelete="SET NULL"),
        nullable=True,
    )
    latitude: Mapped[Decimal] = mapped_column(Numeric(10, 7), nullable=False)
    longitude: Mapped[Decimal] = mapped_column(Numeric(10, 7), nullable=False)
    accuracy_meters: Mapped[Decimal | None] = mapped_column(Numeric(8, 2), nullable=True)
    speed_mph: Mapped[Decimal | None] = mapped_column(Numeric(6, 2), nullable=True)
    heading: Mapped[Decimal | None] = mapped_column(Numeric(5, 2), nullable=True)
    altitude_meters: Mapped[Decimal | None] = mapped_column(Numeric(8, 2), nullable=True)
    battery_percent: Mapped[int | None] = mapped_column(Integer, nullable=True)
    is_moving: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    source: Mapped[str] = mapped_column(VARCHAR(20), nullable=False, default="gps")
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
            "source IN ('gps', 'network', 'manual')",
            name="ck_driver_locations_source",
        ),
        Index("ix_driver_locations_driver_recorded", "driver_id", "recorded_at"),
        Index("ix_driver_locations_route", "route_id"),
        Index("ix_driver_locations_org_time", "org_id", "recorded_at"),
    )
