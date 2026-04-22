from datetime import datetime, timezone
from typing import Any

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
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
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now)


class Order(Base):
    __tablename__ = "orders"
    __table_args__ = (
        UniqueConstraint("brand_id", "external_order_id", name="uq_brand_external_order"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    brand_id: Mapped[str] = mapped_column(String(120), index=True, nullable=False)
    external_order_id: Mapped[str] = mapped_column(String(120), nullable=False, index=True)

    # Core order fields used by app/UI
    order_number: Mapped[str | None] = mapped_column(String(120), nullable=True)
    customer_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    status: Mapped[str | None] = mapped_column(String(80), nullable=True)

    # Keep cents for pricing consistency
    total_cents: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Optional convenience/display field if you later want decimal totals too
    amount: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)

    item_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    unit_count: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Backward-compatible JSON snapshot of line items
    line_items_json: Mapped[list[dict[str, Any]] | dict[str, Any] | None] = mapped_column(JSONB, nullable=True)

    source: Mapped[str] = mapped_column(String(50), nullable=False, default="leaflink")
    review_status: Mapped[str | None] = mapped_column(String(50), nullable=True)
    sync_status: Mapped[str] = mapped_column(String(50), nullable=False, default="ok")

    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)

    external_created_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    external_updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    synced_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    last_synced_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now)

    # Relationship to normalized line items
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
    unit_price_cents: Mapped[int | None] = mapped_column(Integer, nullable=True)
    total_price_cents: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Optional decimal mirrors for easier app/API formatting later
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