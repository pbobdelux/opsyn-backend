"""
SQLAlchemy ORM model for the routes table.

Routes represent delivery runs scoped by org_id for multi-tenant isolation.
UUID primary keys are used throughout for distributed-safe ID generation.
"""

from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    Text,
    VARCHAR,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base


class Route(Base):
    __tablename__ = "routes"

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    org_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        nullable=False,
        index=True,
    )
    route_number: Mapped[str | None] = mapped_column(VARCHAR(50), nullable=True)
    status: Mapped[str] = mapped_column(VARCHAR(30), nullable=False, default="draft")
    assigned_driver_id: Mapped[UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("drivers.id"),
        nullable=True,
    )
    route_date: Mapped[date] = mapped_column(Date, nullable=False)
    total_stops: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_value: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False, default=0)
    total_units: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_by: Mapped[UUID | None] = mapped_column(PG_UUID(as_uuid=True), nullable=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('draft', 'assigned', 'out_for_delivery', 'completed', 'cancelled')",
            name="ck_routes_status",
        ),
        Index("ix_routes_org_id", "org_id"),
        Index("ix_routes_org_date", "org_id", "route_date"),
        Index("ix_routes_driver", "assigned_driver_id"),
    )
