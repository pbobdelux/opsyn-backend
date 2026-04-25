"""
AI service helpers for the /ai/* endpoints.

These functions are intentionally simple and safe:
- They query the database for real data where available.
- Unimplemented features (inventory, compliance) return 0 as a placeholder.
- No secrets, API keys, or full PII are ever logged or returned.
"""

import logging
import os
from typing import Any, Optional

from sqlalchemy import cast, func, or_, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession

from models import BrandAPICredential, Order, OrderLine

logger = logging.getLogger("ai_service")

# Statuses that are considered "terminal" — not pending.
_TERMINAL_STATUSES = {"completed", "cancelled", "rejected", "voided"}

# Statuses that require human attention.
_REVIEW_STATUSES = {
    "needs_review",
    "blocked",
    "mapping_needed",
    "unmatched",
    "error",
    "pending_review",
}


async def get_pending_orders_count(
    db: AsyncSession,
    org_id: str,
    brand_id: Optional[str] = None,
) -> int:
    """Return the count of orders whose status is not in the terminal set.

    Filters by brand_id when provided; otherwise counts across all brands
    (useful when org_id maps to multiple brands or brand_id is unknown).

    Never raises — returns 0 on any database error so the AI endpoint
    can still return a valid response.
    """
    try:
        stmt = select(func.count(Order.id)).where(
            ~Order.status.in_(_TERMINAL_STATUSES)
        )

        if brand_id:
            stmt = stmt.where(Order.brand_id == brand_id)

        result = await db.execute(stmt)
        count = result.scalar() or 0
        logger.debug(
            "get_pending_orders_count org_id=%s brand_id=%s count=%d",
            org_id,
            brand_id,
            count,
        )
        return int(count)

    except Exception as exc:
        logger.error(
            "get_pending_orders_count failed for org_id=%s: %s", org_id, exc
        )
        return 0


async def get_low_inventory_count(db: AsyncSession, org_id: str) -> int:
    """Return the count of low-inventory items.

    Placeholder — inventory tracking is not yet implemented.
    Returns 0 unconditionally.
    """
    return 0


async def get_compliance_issues_count(db: AsyncSession, org_id: str) -> int:
    """Return the count of open compliance issues.

    Placeholder — compliance tracking is not yet implemented.
    Returns 0 unconditionally.
    """
    return 0


def build_attention_summary(
    pending: int,
    low_inv: int,
    compliance: int,
) -> str:
    """Generate a concise human-readable status summary for the AI agent.

    The summary is intentionally brief so it fits naturally in a voice
    response from ElevenLabs or a short text notification.
    """
    parts: list[str] = []

    if pending == 0:
        parts.append("No pending orders.")
    elif pending == 1:
        parts.append("1 order is pending.")
    else:
        parts.append(f"{pending} orders are pending.")

    if low_inv > 0:
        parts.append(f"{low_inv} low-inventory alert{'s' if low_inv != 1 else ''}.")

    if compliance > 0:
        parts.append(f"{compliance} compliance issue{'s' if compliance != 1 else ''} require attention.")

    if not parts or (pending == 0 and low_inv == 0 and compliance == 0):
        return "All systems are clear. No pending orders or issues."

    return " ".join(parts)


# ---------------------------------------------------------------------------
# Extended helpers used by the attention engine and /ai/get-attention
# ---------------------------------------------------------------------------


async def get_orders_needing_review_count(
    db: AsyncSession,
    org_id: str,
    brand_id: Optional[str] = None,
) -> int:
    """Count orders with review_status in the needs-attention set.

    Statuses counted: needs_review, blocked, mapping_needed, unmatched,
    error, pending_review.
    """
    try:
        stmt = select(func.count(Order.id)).where(
            Order.review_status.in_(list(_REVIEW_STATUSES))
        )
        if brand_id:
            stmt = stmt.where(Order.brand_id == brand_id)
        result = await db.execute(stmt)
        count = int(result.scalar() or 0)
        logger.debug(
            "get_orders_needing_review_count org_id=%s brand_id=%s count=%d",
            org_id, brand_id, count,
        )
        return count
    except Exception as exc:
        logger.error("get_orders_needing_review_count failed org_id=%s: %s", org_id, exc)
        return 0


async def get_orders_ready_to_pack_count(
    db: AsyncSession,
    org_id: str,
    brand_id: Optional[str] = None,
) -> int:
    """Count orders with review_status = 'ready'."""
    try:
        stmt = select(func.count(Order.id)).where(Order.review_status == "ready")
        if brand_id:
            stmt = stmt.where(Order.brand_id == brand_id)
        result = await db.execute(stmt)
        count = int(result.scalar() or 0)
        logger.debug(
            "get_orders_ready_to_pack_count org_id=%s brand_id=%s count=%d",
            org_id, brand_id, count,
        )
        return count
    except Exception as exc:
        logger.error("get_orders_ready_to_pack_count failed org_id=%s: %s", org_id, exc)
        return 0


async def get_blocked_orders_count(
    db: AsyncSession,
    org_id: str,
    brand_id: Optional[str] = None,
) -> int:
    """Count orders with review_status = 'blocked'."""
    try:
        stmt = select(func.count(Order.id)).where(Order.review_status == "blocked")
        if brand_id:
            stmt = stmt.where(Order.brand_id == brand_id)
        result = await db.execute(stmt)
        count = int(result.scalar() or 0)
        logger.debug(
            "get_blocked_orders_count org_id=%s brand_id=%s count=%d",
            org_id, brand_id, count,
        )
        return count
    except Exception as exc:
        logger.error("get_blocked_orders_count failed org_id=%s: %s", org_id, exc)
        return 0


async def get_unmapped_line_items_count(
    db: AsyncSession,
    org_id: str,
    brand_id: Optional[str] = None,
) -> int:
    """Count OrderLine records where mapping_status != 'mapped'."""
    try:
        stmt = (
            select(func.count(OrderLine.id))
            .join(Order, OrderLine.order_id == Order.id)
            .where(
                or_(
                    OrderLine.mapping_status != "mapped",
                    OrderLine.mapping_status.is_(None),
                )
            )
        )
        if brand_id:
            stmt = stmt.where(Order.brand_id == brand_id)
        result = await db.execute(stmt)
        count = int(result.scalar() or 0)
        logger.debug(
            "get_unmapped_line_items_count org_id=%s brand_id=%s count=%d",
            org_id, brand_id, count,
        )
        return count
    except Exception as exc:
        logger.error("get_unmapped_line_items_count failed org_id=%s: %s", org_id, exc)
        return 0


async def get_data_quality_issues(
    db: AsyncSession,
    org_id: str,
    brand_id: Optional[str] = None,
) -> dict[str, int]:
    """Count orders with missing customer_name, amount, or line_items.

    Returns a dict with keys: missing_customer_name, missing_amount,
    missing_line_items.
    """
    result: dict[str, int] = {
        "missing_customer_name": 0,
        "missing_amount": 0,
        "missing_line_items": 0,
    }
    base_filter = []
    if brand_id:
        base_filter.append(Order.brand_id == brand_id)

    try:
        stmt = select(func.count(Order.id)).where(
            Order.customer_name.is_(None), *base_filter
        )
        r = await db.execute(stmt)
        result["missing_customer_name"] = int(r.scalar() or 0)
    except Exception as exc:
        logger.error("missing_customer_name query failed org_id=%s: %s", org_id, exc)

    try:
        stmt = select(func.count(Order.id)).where(
            Order.amount.is_(None),
            Order.total_cents.is_(None),
            *base_filter,
        )
        r = await db.execute(stmt)
        result["missing_amount"] = int(r.scalar() or 0)
    except Exception as exc:
        logger.error("missing_amount query failed org_id=%s: %s", org_id, exc)

    try:
        stmt = select(func.count(Order.id)).where(
            or_(
                Order.line_items_json.is_(None),
                cast(Order.line_items_json, JSONB) == cast("[]", JSONB),
            ),
            *base_filter,
        )
        r = await db.execute(stmt)
        result["missing_line_items"] = int(r.scalar() or 0)
    except Exception as exc:
        logger.error("missing_line_items query failed org_id=%s: %s", org_id, exc)

    return result


async def get_recent_sync_status(
    db: AsyncSession,
    org_id: str,
    brand_id: Optional[str] = None,
) -> dict[str, Any]:
    """Query BrandAPICredential for the most recent sync status.

    Returns a dict with last_sync_at (ISO string or None) and sync_status.
    """
    try:
        lookup_id = brand_id or org_id
        stmt = (
            select(BrandAPICredential)
            .where(BrandAPICredential.brand_id == lookup_id)
            .order_by(BrandAPICredential.last_sync_at.desc().nullslast())
            .limit(1)
        )
        result = await db.execute(stmt)
        cred = result.scalar_one_or_none()
        if cred is None:
            return {"last_sync_at": None, "sync_status": "unknown"}
        return {
            "last_sync_at": cred.last_sync_at.isoformat() if cred.last_sync_at else None,
            "sync_status": cred.sync_status or "unknown",
        }
    except Exception as exc:
        logger.error("get_recent_sync_status failed org_id=%s: %s", org_id, exc)
        return {"last_sync_at": None, "sync_status": "error"}


def is_using_mock_data() -> bool:
    """Return True if the MOCK_MODE environment variable is set to a truthy value."""
    val = os.getenv("MOCK_MODE", "").strip().lower()
    return val in {"1", "true", "yes", "on"}
