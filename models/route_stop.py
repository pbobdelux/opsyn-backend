"""
SQLAlchemy ORM model for the route_stops table.

RouteStops represent individual delivery stops within a route.
Scoped by org_id for multi-tenant isolation.
UUID primary keys are used throughout for distributed-safe ID generation.
"""

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    Text,
    UniqueConstraint,
    VARCHAR,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base


class RouteStop(Base):
    __tablename__ = "route_stops"

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
        index=True,
    )
    stop_order: Mapped[int] = mapped_column(Integer, nullable=False)
    stop_type: Mapped[str] = mapped_column(
        VARCHAR(30), nullable=False, default="leaflink_order"
    )
    source_order_id: Mapped[UUID | None] = mapped_column(
        PG_UUID(as_uuid=True), nullable=True
    )
    customer_name: Mapped[str | None] = mapped_column(VARCHAR(255), nullable=True)
    stop_name: Mapped[str | None] = mapped_column(VARCHAR(255), nullable=True)
    address: Mapped[str | None] = mapped_column(Text, nullable=True)
    contact_name: Mapped[str | None] = mapped_column(VARCHAR(255), nullable=True)
    contact_phone: Mapped[str | None] = mapped_column(VARCHAR(50), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    time_window: Mapped[str | None] = mapped_column(VARCHAR(100), nullable=True)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    status: Mapped[str] = mapped_column(VARCHAR(20), nullable=False, default="pending")
    ar_status: Mapped[str] = mapped_column(
        VARCHAR(30), nullable=False, default="not_applicable"
    )
    amount_due: Mapped[Decimal] = mapped_column(
        Numeric(12, 2), nullable=False, default=0
    )
    amount_collected: Mapped[Decimal] = mapped_column(
        Numeric(12, 2), nullable=False, default=0
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
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

    __table_args__ = (
        CheckConstraint(
            "stop_type IN ('leaflink_order', 'manual_stop', 'bank', 'processor_pickup', 'sample_dropoff', 'supply_pickup', 'other')",
            name="ck_route_stops_type",
        ),
        CheckConstraint(
            "status IN ('pending', 'arrived', 'completed', 'failed', 'skipped')",
            name="ck_route_stops_status",
        ),
        CheckConstraint(
            "ar_status IN ('unpaid', 'partial', 'paid', 'collection_issue', 'not_applicable')",
            name="ck_route_stops_ar_status",
        ),
        UniqueConstraint("route_id", "stop_order", name="uq_route_stops_order"),
        Index("ix_route_stops_route_id", "route_id"),
        Index("ix_route_stops_org_id", "org_id"),
    )
