"""
SQLAlchemy models for sync health tracking and dead-letter queue.

Tables:
  - sync_health              — per-brand sync health state (one row per brand)
  - dead_letter_line_items   — line items that failed after max retries
"""

from datetime import datetime, timezone

from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from database import Base


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class SyncHealth(Base):
    """
    Tracks per-brand sync health state.

    One row per brand_id. Updated after each sync phase:
      - last_attempted_sync_at: set at the start of every sync
      - last_successful_sync_at: set only after Phase 2 completes cleanly
      - consecutive_failures: incremented on error, reset to 0 on success
      - total_orders_synced / total_line_items_synced: running totals
    """

    __tablename__ = "sync_health"
    __table_args__ = (
        UniqueConstraint("brand_id", name="uq_sync_health_brand_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    brand_id: Mapped[str] = mapped_column(String(120), nullable=False, unique=True, index=True)

    last_successful_sync_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_attempted_sync_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    consecutive_failures: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Resumption tracking
    last_page_synced: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Running totals
    total_orders_synced: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_line_items_synced: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now
    )


class DeadLetterLineItem(Base):
    """
    Line items that failed insertion after MAX_RETRIES attempts.

    These rows are written by sync_leaflink_line_items() when a line item
    cannot be inserted even after retries.  Admin endpoints allow inspection
    and manual replay.
    """

    __tablename__ = "dead_letter_line_items"
    __table_args__ = (
        UniqueConstraint(
            "brand_id", "external_order_id", "sku",
            name="uq_dead_letter_brand_order_sku",
        ),
        Index("ix_dead_letter_brand_external_order", "brand_id", "external_order_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    brand_id: Mapped[str] = mapped_column(String(120), nullable=False, index=True)
    external_order_id: Mapped[str] = mapped_column(String(120), nullable=False)

    # Resolved DB order PK (may be None if the parent order was never found)
    order_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("orders.id", ondelete="SET NULL"), nullable=True
    )

    sku: Mapped[str | None] = mapped_column(String(255), nullable=True)
    product_name: Mapped[str | None] = mapped_column(String(255), nullable=True)

    raw_payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    failure_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)

    last_failed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
