"""
AI service helpers for the /ai/* endpoints.

These functions are intentionally simple and safe:
- They query the database for real data where available.
- Unimplemented features (inventory, compliance) return 0 as a placeholder.
- No secrets, API keys, or full PII are ever logged or returned.
"""

import logging
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from models import Order

logger = logging.getLogger("ai_service")

# Statuses that are considered "terminal" — not pending.
_TERMINAL_STATUSES = {"completed", "cancelled", "rejected", "voided"}


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
