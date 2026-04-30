"""
models package — re-exports all ORM models from both the root models.py
(now moved here as _base_models) and the assistant sub-module.

All existing imports of the form `from models import Order` continue to work
because this __init__.py re-exports everything that was previously in models.py.
"""

# ---------------------------------------------------------------------------
# Base / existing models (previously in root models.py)
# ---------------------------------------------------------------------------
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import Mapped, mapped_column, relationship

from database import Base


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class BrandAPICredential(Base):
    __tablename__ = "brand_api_credentials"
    __table_args__ = (
        UniqueConstraint("brand_id", "integration_name", name="uq_brand_integration"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    brand_id: Mapped[str] = mapped_column(String(120), index=True, nullable=False)
    integration_name: Mapped[str] = mapped_column(String(50), nullable=False, default="leaflink")
    base_url: Mapped[str | None] = mapped_column(String(255), nullable=True)
    api_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    vendor_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    company_id: Mapped[str | None] = mapped_column(String(120), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    sync_status: Mapped[str] = mapped_column(String(50), nullable=False, default="idle")
    last_sync_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Progress tracking for paginated background sync
    last_synced_page: Mapped[int | None] = mapped_column(Integer, nullable=True, default=0)
    total_pages_available: Mapped[int | None] = mapped_column(Integer, nullable=True)
    total_orders_available: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # Auto-detected auth scheme for this credential
    auth_scheme: Mapped[Optional[str]] = mapped_column(
        String(20),
        nullable=True,
        default=None,
        comment="Auto-detected auth scheme: Bearer, Token, or Raw",
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now)


class Order(Base):
    __tablename__ = "orders"
    __table_args__ = (
        UniqueConstraint("brand_id", "external_order_id", name="uq_brand_external_order"),
        Index("ix_orders_brand_id", "brand_id"),
        Index("ix_orders_order_number", "order_number"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    brand_id: Mapped[str] = mapped_column(String(120), index=True, nullable=False)
    external_order_id: Mapped[str] = mapped_column(String(120), nullable=False, index=True)

    order_number: Mapped[str | None] = mapped_column(String(120), nullable=True)
    customer_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    status: Mapped[str | None] = mapped_column(String(80), nullable=True)

    total_cents: Mapped[int | None] = mapped_column(Integer, nullable=True)
    amount: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)

    item_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    unit_count: Mapped[int | None] = mapped_column(Integer, nullable=True)

    line_items_json: Mapped[list[dict[str, Any]] | dict[str, Any] | None] = mapped_column(JSONB, nullable=True)

    source: Mapped[str] = mapped_column(String(50), nullable=False, default="leaflink")
    review_status: Mapped[str | None] = mapped_column(String(50), nullable=True)
    sync_status: Mapped[str] = mapped_column(String(50), nullable=False, default="ok")

    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)

    external_created_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    external_updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    synced_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    last_synced_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)

    sync_run_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("sync_runs.id"), nullable=True, index=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now)

    lines: Mapped[list["OrderLine"]] = relationship(
        "OrderLine",
        back_populates="order",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class OrderLine(Base):
    __tablename__ = "order_lines"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id", ondelete="CASCADE"), nullable=False, index=True)

    sku: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    product_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    quantity: Mapped[int | None] = mapped_column(Integer, nullable=True)

    pulled_qty: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    packed_qty: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    unit_price_cents: Mapped[int | None] = mapped_column(Integer, nullable=True)
    total_price_cents: Mapped[int | None] = mapped_column(Integer, nullable=True)

    unit_price: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    total_price: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)

    mapped_product_id: Mapped[str | None] = mapped_column(String(120), nullable=True)
    mapping_status: Mapped[str | None] = mapped_column(String(50), nullable=True, default="unknown")
    mapping_issue: Mapped[str | None] = mapped_column(String(255), nullable=True)

    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now)

    order: Mapped["Order"] = relationship("Order", back_populates="lines")


class OrganizationBrandBinding(Base):
    __tablename__ = "organization_brand_bindings"
    __table_args__ = (
        UniqueConstraint("org_id", "brand_id", name="uq_org_brand_binding"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    org_id: Mapped[str] = mapped_column(String(120), index=True, nullable=False)
    brand_id: Mapped[str] = mapped_column(String(120), index=True, nullable=False)
    brand_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    source: Mapped[str | None] = mapped_column(String(50), nullable=True, default="manual")
    is_default: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now)


# =============================================================================
# Dispatch Models
# =============================================================================

class Driver(Base):
    __tablename__ = "drivers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    org_id: Mapped[str] = mapped_column(String(120), index=True, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    # nullable=True in DB for legacy rows; email is required at the API level for new drivers.
    email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    phone: Mapped[str | None] = mapped_column(String(50), nullable=True)
    license_plate: Mapped[str | None] = mapped_column(String(50), nullable=True)
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="available")
    pin: Mapped[str | None] = mapped_column(String(4), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now)

    routes: Mapped[list["Route"]] = relationship(
        "Route",
        back_populates="driver",
        passive_deletes=True,
    )


class Route(Base):
    __tablename__ = "dispatch_routes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    org_id: Mapped[str] = mapped_column(String(120), index=True, nullable=False)
    driver_id: Mapped[int | None] = mapped_column(ForeignKey("drivers.id", ondelete="SET NULL"), nullable=True, index=True)
    name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="pending")
    scheduled_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now)

    driver: Mapped["Driver | None"] = relationship("Driver", back_populates="routes")
    stops: Mapped[list["RouteStop"]] = relationship(
        "RouteStop",
        back_populates="route",
        cascade="all, delete-orphan",
        passive_deletes=True,
        order_by="RouteStop.stop_order",
    )


class RouteStop(Base):
    __tablename__ = "route_stops"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    route_id: Mapped[int] = mapped_column(ForeignKey("dispatch_routes.id", ondelete="CASCADE"), nullable=False, index=True)
    order_id: Mapped[int | None] = mapped_column(ForeignKey("orders.id", ondelete="SET NULL"), nullable=True, index=True)
    stop_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    address: Mapped[str | None] = mapped_column(String(500), nullable=True)
    customer_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="pending")
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now)

    route: Mapped["Route"] = relationship("Route", back_populates="stops")
    order: Mapped["Order | None"] = relationship("Order")


# =============================================================================
# Tenant Auth Model
# =============================================================================

class TenantCredential(Base):
    __tablename__ = "tenant_credentials"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    org_id: Mapped[str] = mapped_column(String(120), index=True, nullable=False, unique=True)
    api_secret: Mapped[str] = mapped_column(String(255), nullable=False)  # hashed secret
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now)


# =============================================================================
# Sync Run Model
# =============================================================================

class SyncRun(Base):
    """
    Tracks a single sync operation for a brand.

    One active SyncRun per brand at a time.
    Completed/failed runs are archived for audit.
    """
    __tablename__ = "sync_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    brand_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    integration_name: Mapped[str] = mapped_column(String(50), nullable=False, default="leaflink")

    # Sync run state
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="queued",
        comment="idle | queued | syncing | completed | stalled | failed"
    )
    mode: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="incremental",
        comment="full | incremental"
    )

    # Progress tracking
    pages_synced: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_pages: Mapped[int | None] = mapped_column(Integer, nullable=True)
    orders_loaded_this_run: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_orders_available: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Cursor/resumption
    current_cursor: Mapped[str | None] = mapped_column(Text, nullable=True)
    current_page: Mapped[int | None] = mapped_column(Integer, nullable=True, default=1)
    last_successful_cursor: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_successful_page: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Timing
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    last_progress_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Error tracking
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    stalled_reason: Mapped[str | None] = mapped_column(String(255), nullable=True)

    # Worker tracking
    worker_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Metadata
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now)

    # Indexes for efficient queries
    __table_args__ = (
        Index("ix_sync_runs_brand_status", "brand_id", "status"),
        Index("ix_sync_runs_brand_started", "brand_id", "started_at"),
    )

    @classmethod
    async def get_active_run(cls, db: AsyncSession, brand_id: str) -> Optional["SyncRun"]:
        """Get the currently active sync run for a brand."""
        result = await db.execute(
            select(cls).where(
                cls.brand_id == brand_id,
                cls.status.in_(["queued", "syncing"])
            ).order_by(cls.started_at.desc()).limit(1)
        )
        return result.scalar_one_or_none()

    @classmethod
    async def get_last_completed_run(cls, db: AsyncSession, brand_id: str) -> Optional["SyncRun"]:
        """Get the last completed sync run for a brand."""
        result = await db.execute(
            select(cls).where(
                cls.brand_id == brand_id,
                cls.status == "completed"
            ).order_by(cls.completed_at.desc()).limit(1)
        )
        return result.scalar_one_or_none()

    def percent_complete(self) -> float | None:
        """Calculate percent complete from pages_synced / total_pages."""
        if self.total_pages is None or self.total_pages == 0:
            return None
        percent = (self.pages_synced / self.total_pages) * 100
        return min(100.0, max(0.0, percent))  # Clamp 0-100

    def is_stalled(self, stall_threshold_seconds: int = 90) -> bool:
        """Check if sync is stalled (no progress for threshold seconds)."""
        if self.status not in ["syncing", "queued"]:
            return False
        if self.last_progress_at is None:
            return False
        elapsed = (datetime.now(timezone.utc) - self.last_progress_at).total_seconds()
        return elapsed > stall_threshold_seconds

    def estimated_completion_minutes(self) -> float | None:
        """Estimate completion time based on current progress."""
        if self.status != "syncing" or self.is_stalled():
            return None
        if self.total_pages is None or self.total_pages == 0:
            return None
        if self.pages_synced == 0:
            return None

        # Calculate average time per page
        elapsed_seconds = (datetime.now(timezone.utc) - self.started_at).total_seconds()
        if elapsed_seconds < 1:
            return None

        avg_seconds_per_page = elapsed_seconds / self.pages_synced
        remaining_pages = self.total_pages - self.pages_synced
        estimated_seconds = remaining_pages * avg_seconds_per_page

        return estimated_seconds / 60.0  # Convert to minutes


# =============================================================================
# Sync Queue Model
# =============================================================================

class SyncRequest(Base):
    __tablename__ = "sync_requests"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    brand_id: Mapped[str] = mapped_column(String(120), index=True, nullable=False)
    status: Mapped[str] = mapped_column(String(50), default="pending")  # pending, processing, complete, error
    start_page: Mapped[int] = mapped_column(Integer, default=1)
    total_pages: Mapped[int | None] = mapped_column(Integer, nullable=True)
    total_orders_available: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


# ---------------------------------------------------------------------------
# Assistant models
# ---------------------------------------------------------------------------
from models.assistant_models import (  # noqa: F401, E402
    AssistantAuditLog,
    AssistantMessage,
    AssistantPendingAction,
    AssistantSession,
)

