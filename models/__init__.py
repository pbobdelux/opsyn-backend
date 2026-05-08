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
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID
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
    # Webhook support
    webhook_key: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
        default=None,
        comment="LeafLink webhook secret for HMAC-SHA256 signature verification",
    )
    org_id: Mapped[Optional[str]] = mapped_column(
        String(120),
        nullable=True,
        default=None,
        comment="Organization ID for fast tenant resolution from webhook payloads",
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now)


class Order(Base):
    __tablename__ = "orders"
    __table_args__ = (
        UniqueConstraint("brand_id", "external_order_id", name="uq_brand_external_order"),
        Index("ix_orders_brand_id", "brand_id"),
        Index("ix_orders_order_number", "order_number"),
        # Dispatch / AR indexes
        Index("ix_orders_assigned_driver", "assigned_driver_id"),
        Index("ix_orders_delivery_status", "delivery_status"),
        Index("ix_orders_payment_status", "payment_status"),
        Index("ix_orders_route_id", "route_id"),
        # Check constraints for status enums
        CheckConstraint(
            "delivery_status IN ('pending','assigned','out_for_delivery','delivered','failed','needs_reschedule','cancelled')",
            name="ck_orders_delivery_status",
        ),
        CheckConstraint(
            "payment_status IN ('unpaid','partial','paid','overdue','collection_issue','write_off')",
            name="ck_orders_payment_status",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    brand_id: Mapped[str] = mapped_column(String(120), index=True, nullable=False)
    org_id: Mapped[Optional[str]] = mapped_column(String(120), nullable=True, index=True)
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

    # ------------------------------------------------------------------
    # Dispatch fields — connect orders to the driver/route system
    # ------------------------------------------------------------------
    assigned_driver_id: Mapped[Optional[str]] = mapped_column(
        PG_UUID(as_uuid=False),
        ForeignKey("drivers.id"),
        nullable=True,
        default=None,
    )
    assigned_driver_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, default=None)
    delivery_status: Mapped[str] = mapped_column(String(30), nullable=False, default="pending")
    delivery_date: Mapped[Optional[object]] = mapped_column(Date, nullable=True, default=None)
    route_number: Mapped[Optional[str]] = mapped_column(String(50), nullable=True, default=None)
    route_id: Mapped[Optional[str]] = mapped_column(
        PG_UUID(as_uuid=False),
        ForeignKey("routes.id"),
        nullable=True,
        default=None,
    )
    driver_note: Mapped[Optional[str]] = mapped_column(Text, nullable=True, default=None)
    delivery_instructions: Mapped[Optional[str]] = mapped_column(Text, nullable=True, default=None)

    # ------------------------------------------------------------------
    # Accounts-receivable (AR) fields
    # ------------------------------------------------------------------
    payment_status: Mapped[str] = mapped_column(String(30), nullable=False, default="unpaid")
    amount_paid: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False, default=0)
    balance_due: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False, default=0)
    due_date: Mapped[Optional[object]] = mapped_column(Date, nullable=True, default=None)
    days_overdue: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    invoice_number: Mapped[Optional[str]] = mapped_column(String(100), nullable=True, default=None)
    ar_note: Mapped[Optional[str]] = mapped_column(Text, nullable=True, default=None)

    # ------------------------------------------------------------------
    # Sync health fields — per-order sync health tracking
    # ------------------------------------------------------------------
    sync_health_status: Mapped[Optional[str]] = mapped_column(
        String(20),
        nullable=True,
        default=None,
        comment="Per-order sync health: ok | partial | failed",
    )
    sync_health_missing_fields: Mapped[Optional[list]] = mapped_column(
        JSONB,
        nullable=True,
        default=None,
        comment="List of missing required fields for this order",
    )
    sync_health_last_error: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        default=None,
        comment="Last sync error message for this order",
    )

    # ------------------------------------------------------------------
    # Sync health JSONB blob — full per-order sync health snapshot
    # ------------------------------------------------------------------
    sync_health: Mapped[Optional[dict]] = mapped_column(
        JSONB,
        nullable=True,
        default=None,
        comment="JSONB blob: {status, missing_fields, last_error, last_synced_at}",
    )

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

    packed_qty: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    unit_price_cents: Mapped[int | None] = mapped_column(Integer, nullable=True)
    total_price_cents: Mapped[int | None] = mapped_column(Integer, nullable=True)

    unit_price: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    total_price: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)

    mapped_product_id: Mapped[str | None] = mapped_column(
        String(120),
        nullable=True,
    )
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
# Dispatch Models — imported from dedicated sub-modules
# =============================================================================

from models.driver import Driver  # noqa: F401, E402
from models.driver_location import DriverLocation  # noqa: F401, E402
from models.driver_route_history import DriverRouteHistory  # noqa: F401, E402
from models.route import Route  # noqa: F401, E402
from models.route_event import RouteEvent  # noqa: F401, E402
from models.route_stop import RouteStop  # noqa: F401, E402


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

    @staticmethod
    def _ensure_utc(dt: datetime, field_name: str = "unknown") -> datetime:
        """Normalize a datetime to UTC-aware before arithmetic/comparison.

        Handles naive datetimes (assumes UTC) and aware datetimes (converts to UTC).
        This prevents 'can't subtract offset-naive and offset-aware datetimes' errors
        when DB columns return naive datetimes and we compare against UTC-aware now().
        """
        import logging as _logging
        _log = _logging.getLogger("sync_run_model")
        if dt.tzinfo is None:
            normalized = dt.replace(tzinfo=timezone.utc)
            _log.info(
                "[DATETIME_NORMALIZED] field=%s original=%s (naive) normalized=%s (UTC-aware)",
                field_name,
                dt.isoformat(),
                normalized.isoformat(),
            )
            return normalized
        return dt.astimezone(timezone.utc)

    def is_stalled(self, stall_threshold_seconds: int = 90) -> bool:
        """Check if sync is stalled (no progress for threshold seconds)."""
        import logging as _logging
        _log = _logging.getLogger("sync_run_model")
        if self.status not in ["syncing", "queued"]:
            return False
        if self.last_progress_at is None:
            return False
        try:
            a = datetime.now(timezone.utc)
            b = self._ensure_utc(self.last_progress_at, "last_progress_at")
            elapsed = (a - b).total_seconds()
        except TypeError as e:
            _log.error(
                "[DATETIME_COMPARE_ERROR] a=%s tz_a=%s b=%s tz_b=%s error=%s",
                a,
                getattr(a, "tzinfo", None),
                self.last_progress_at,
                getattr(self.last_progress_at, "tzinfo", None),
                str(e),
            )
            raise
        return elapsed > stall_threshold_seconds

    def estimated_completion_minutes(self) -> float | None:
        """Estimate completion time based on current progress."""
        import logging as _logging
        _log = _logging.getLogger("sync_run_model")
        if self.status != "syncing" or self.is_stalled():
            return None
        if self.total_pages is None or self.total_pages == 0:
            return None
        if self.pages_synced == 0:
            return None

        # Calculate average time per page
        try:
            a = datetime.now(timezone.utc)
            b = self._ensure_utc(self.started_at, "started_at")
            elapsed_seconds = (a - b).total_seconds()
        except TypeError as e:
            _log.error(
                "[DATETIME_COMPARE_ERROR] a=%s tz_a=%s b=%s tz_b=%s error=%s",
                a,
                getattr(a, "tzinfo", None),
                self.started_at,
                getattr(self.started_at, "tzinfo", None),
                str(e),
            )
            raise
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

# ---------------------------------------------------------------------------
# Sync health models
# ---------------------------------------------------------------------------
from models.sync_health import DeadLetterLineItem, SyncHealth  # noqa: F401, E402

