"""
SQLAlchemy ORM model for the drivers table.

Drivers are scoped by org_id for multi-tenant isolation.
UUID primary keys are used throughout for distributed-safe ID generation.
"""

from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Index,
    Text,
    VARCHAR,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSON, UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base


class Driver(Base):
    __tablename__ = "drivers"

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
    name: Mapped[str] = mapped_column(VARCHAR(255), nullable=False)
    email: Mapped[str | None] = mapped_column(VARCHAR(255), nullable=True)
    phone: Mapped[str | None] = mapped_column(VARCHAR(50), nullable=True)
    status: Mapped[str] = mapped_column(VARCHAR(20), nullable=False, default="active")
    passcode_hash: Mapped[str | None] = mapped_column(VARCHAR(255), nullable=True)
    invite_code: Mapped[str | None] = mapped_column(VARCHAR(50), nullable=True)
    license_plate: Mapped[str | None] = mapped_column(VARCHAR(50), nullable=True)
    vehicle_type: Mapped[str | None] = mapped_column(VARCHAR(50), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    preferences: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
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
    deactivated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('active', 'inactive')",
            name="ck_drivers_status",
        ),
        Index("ix_drivers_org_id", "org_id"),
        Index("ix_drivers_org_status", "org_id", "status"),
    )


Index(
    "uq_drivers_org_email",
    Driver.org_id,
    Driver.email,
    unique=True,
    postgresql_where=Driver.email.isnot(None),
)
