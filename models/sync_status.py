"""
SyncStatus model — tracks per-brand order sync progress for the Watchdog integration.

One row per brand_id (unique constraint).  The row is created when a sync
starts and updated after each batch and at completion/failure.
"""

from datetime import datetime, timezone

from sqlalchemy import DateTime, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from database import Base


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class SyncStatus(Base):
    """Tracks the current sync state for a single brand."""

    __tablename__ = "sync_status"
    __table_args__ = (
        UniqueConstraint("brand_id", name="uq_sync_status_brand_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)

    brand_id: Mapped[str] = mapped_column(String(120), index=True, nullable=False)

    # Lifecycle status: connecting | syncing | processing | complete | failed
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="connecting")

    total_fetched: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_in_database: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    percent_complete: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    latest_order_date: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    oldest_order_date: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    sync_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    last_progress_timestamp: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now, onupdate=_utc_now
    )
