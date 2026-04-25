"""
Attention Engine — comprehensive operational priority analysis.

Queries real database data to produce a structured attention report
suitable for voice (ElevenLabs) and screen (Brand App) consumption.

Public API
----------
    async def get_operational_attention(
        db: AsyncSession,
        org_id: str,
        brand_id: Optional[str] = None,
    ) -> dict
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from models import BrandAPICredential, Order, OrderLine

logger = logging.getLogger("attention_engine")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TERMINAL_STATUSES = {"completed", "cancelled", "rejected", "voided", "shipped"}

_REVIEW_STATUSES_NEEDING_ATTENTION = {
    "needs_review",
    "blocked",
    "mapping_needed",
    "unmatched",
    "error",
    "pending_review",
}

# ---------------------------------------------------------------------------
# Score weights
# ---------------------------------------------------------------------------

_SCORE_BLOCKED = 5
_SCORE_NEEDS_REVIEW = 3
_SCORE_UNMAPPED_LINE_ITEM = 2
_SCORE_MISSING_CUSTOMER = 1
_SCORE_MISSING_AMOUNT = 1


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _severity_from_score(score: int) -> str:
    if score == 0:
        return "clear"
    if score <= 20:
        return "low"
    if score <= 50:
        return "medium"
    if score <= 80:
        return "high"
    return "critical"


def _build_spoken_reply(counts: dict, severity: str) -> str:
    """Build a concise, voice-friendly reply for ElevenLabs."""
    parts: list[str] = []

    blocked = counts.get("blocked_orders", 0)
    review = counts.get("orders_needing_review", 0)
    unmapped = counts.get("unmapped_line_items", 0)
    missing_name = counts.get("missing_customer_name", 0)
    missing_amt = counts.get("missing_amount", 0)
    total = counts.get("total_orders", 0)

    if severity == "clear":
        return (
            f"Everything looks good. You have {total} active order{'s' if total != 1 else ''} "
            "and no items requiring immediate attention."
        )

    if blocked:
        parts.append(f"{blocked} blocked order{'s' if blocked != 1 else ''}")
    if review:
        parts.append(f"{review} order{'s' if review != 1 else ''} needing review")
    if unmapped:
        parts.append(f"{unmapped} unmapped line item{'s' if unmapped != 1 else ''}")
    if missing_name:
        parts.append(f"{missing_name} order{'s' if missing_name != 1 else ''} missing customer name")
    if missing_amt:
        parts.append(f"{missing_amt} order{'s' if missing_amt != 1 else ''} missing amount")

    if not parts:
        return f"You have {total} active order{'s' if total != 1 else ''}. No critical issues found."

    summary = ", ".join(parts[:-1])
    if len(parts) > 1:
        summary = summary + ", and " + parts[-1]
    else:
        summary = parts[0]

    recommendation = ""
    if blocked:
        recommendation = " I recommend starting with the blocked orders."
    elif unmapped:
        recommendation = " I recommend mapping the unmapped line items next."
    elif review:
        recommendation = " I recommend reviewing the flagged orders."

    return f"You have {summary}.{recommendation}"


def _build_screen_reply(counts: dict, severity: str) -> str:
    """Build a compact screen-friendly reply for the Brand App."""
    if severity == "clear":
        return f"{counts.get('total_orders', 0)} active orders • All clear"

    parts: list[str] = []
    if counts.get("blocked_orders"):
        parts.append(f"{counts['blocked_orders']} blocked")
    if counts.get("orders_needing_review"):
        parts.append(f"{counts['orders_needing_review']} need review")
    if counts.get("unmapped_line_items"):
        parts.append(f"{counts['unmapped_line_items']} unmapped items")
    if counts.get("missing_customer_name"):
        parts.append(f"{counts['missing_customer_name']} missing name")
    if counts.get("missing_amount"):
        parts.append(f"{counts['missing_amount']} missing amount")

    return " • ".join(parts) if parts else f"{counts.get('total_orders', 0)} active orders"


def _build_summary(severity: str, counts: dict) -> str:
    """Build a one-sentence operational summary."""
    if severity == "clear":
        return "All systems are clear. No pending orders or issues."
    if severity == "low":
        return "Minor attention needed. Review flagged items when convenient."
    if severity == "medium":
        blocked = counts.get("blocked_orders", 0)
        if blocked:
            return f"Moderate attention needed. Focus on {blocked} blocked order{'s' if blocked != 1 else ''} first."
        return "Moderate attention needed. Review flagged orders and unmapped items."
    if severity == "high":
        return "High attention required. Blocked orders and mapping issues need immediate resolution."
    return "Critical attention required. Multiple blocking issues detected — act now."


def _build_top_priorities(counts: dict) -> list[dict]:
    """Build the ranked priority list."""
    priorities: list[dict] = []
    rank = 1

    blocked = counts.get("blocked_orders", 0)
    if blocked:
        priorities.append({
            "rank": rank,
            "severity": "critical" if blocked >= 5 else "high",
            "title": "Blocked Orders",
            "detail": f"{blocked} order{'s are' if blocked != 1 else ' is'} blocked due to mapping issues",
            "recommended_action": "Review and resolve mapping issues",
            "action_key": "show_blocked_orders",
        })
        rank += 1

    unmapped = counts.get("unmapped_line_items", 0)
    if unmapped:
        priorities.append({
            "rank": rank,
            "severity": "high" if unmapped >= 10 else "medium",
            "title": "Unmapped Line Items",
            "detail": f"{unmapped} line item{'s need' if unmapped != 1 else ' needs'} product mapping",
            "recommended_action": "Map products to your catalog",
            "action_key": "show_unmapped_items",
        })
        rank += 1

    review = counts.get("orders_needing_review", 0)
    if review:
        priorities.append({
            "rank": rank,
            "severity": "medium",
            "title": "Orders Needing Review",
            "detail": f"{review} order{'s require' if review != 1 else ' requires'} manual review",
            "recommended_action": "Review and approve or reject flagged orders",
            "action_key": "show_orders_needing_review",
        })
        rank += 1

    missing_name = counts.get("missing_customer_name", 0)
    if missing_name:
        priorities.append({
            "rank": rank,
            "severity": "medium" if missing_name >= 5 else "low",
            "title": "Missing Customer Names",
            "detail": f"{missing_name} order{'s are' if missing_name != 1 else ' is'} missing customer name",
            "recommended_action": "Update customer information for affected orders",
            "action_key": "show_missing_customer_orders",
        })
        rank += 1

    missing_amt = counts.get("missing_amount", 0)
    if missing_amt:
        priorities.append({
            "rank": rank,
            "severity": "low",
            "title": "Missing Order Amounts",
            "detail": f"{missing_amt} order{'s are' if missing_amt != 1 else ' is'} missing amount data",
            "recommended_action": "Verify and update order amounts",
            "action_key": "show_missing_amount_orders",
        })
        rank += 1

    return priorities


def _build_suggested_actions(counts: dict) -> list[dict]:
    """Build the suggested action list for the UI."""
    actions: list[dict] = []

    if counts.get("blocked_orders", 0):
        actions.append({
            "label": "Review blocked orders",
            "assistant_command": "Show me blocked orders",
            "requires_confirmation": False,
        })

    if counts.get("unmapped_line_items", 0):
        actions.append({
            "label": "Map unmapped items",
            "assistant_command": "Show me unmapped line items",
            "requires_confirmation": False,
        })

    if counts.get("orders_needing_review", 0):
        actions.append({
            "label": "Review flagged orders",
            "assistant_command": "Show me orders needing review",
            "requires_confirmation": False,
        })

    if counts.get("orders_ready_to_pack", 0):
        actions.append({
            "label": "Pack ready orders",
            "assistant_command": "Show me orders ready to pack",
            "requires_confirmation": False,
        })

    return actions


# ---------------------------------------------------------------------------
# Main engine function
# ---------------------------------------------------------------------------


async def get_operational_attention(
    db: AsyncSession,
    org_id: str,
    brand_id: Optional[str] = None,
) -> dict:
    """
    Query real database data and return a comprehensive operational attention report.

    Parameters
    ----------
    db:
        Active async SQLAlchemy session.
    org_id:
        The organisation identifier. Used as brand_id filter when brand_id is None.
    brand_id:
        Optional explicit brand_id override. Falls back to org_id when not provided.

    Returns
    -------
    A structured dict with attention_score, severity, spoken_reply, screen_reply,
    top_priorities, counts, suggested_actions, data_source, and errors.
    """
    errors: list[str] = []
    effective_brand_id = brand_id or org_id

    logger.info(
        "attention_request_received org_id=%s user_id=system brand_id=%s",
        org_id,
        effective_brand_id,
    )

    # ------------------------------------------------------------------
    # Initialise all counts to 0 — never crash, always return something
    # ------------------------------------------------------------------
    counts = {
        "total_orders": 0,
        "orders_needing_review": 0,
        "orders_ready_to_pack": 0,
        "blocked_orders": 0,
        "unmapped_line_items": 0,
        "missing_customer_name": 0,
        "missing_amount": 0,
        "missing_line_items": 0,
        "low_inventory": 0,
        "compliance_issues": 0,
    }

    data_source = "database"
    using_mock = False

    # ------------------------------------------------------------------
    # 1. Total active orders
    # ------------------------------------------------------------------
    try:
        stmt = select(func.count(Order.id)).where(
            Order.brand_id == effective_brand_id,
            ~Order.status.in_(_TERMINAL_STATUSES),
        )
        result = await db.execute(stmt)
        counts["total_orders"] = int(result.scalar() or 0)
    except Exception as exc:
        logger.error("attention_engine: total_orders query failed org_id=%s: %s", org_id, exc)
        errors.append(f"total_orders: {exc}")

    # ------------------------------------------------------------------
    # 2. Orders by review_status needing attention
    # ------------------------------------------------------------------
    try:
        stmt = select(func.count(Order.id)).where(
            Order.brand_id == effective_brand_id,
            ~Order.status.in_(_TERMINAL_STATUSES),
            Order.review_status.in_(list(_REVIEW_STATUSES_NEEDING_ATTENTION)),
        )
        result = await db.execute(stmt)
        counts["orders_needing_review"] = int(result.scalar() or 0)
    except Exception as exc:
        logger.error("attention_engine: orders_needing_review query failed org_id=%s: %s", org_id, exc)
        errors.append(f"orders_needing_review: {exc}")

    # ------------------------------------------------------------------
    # 3. Blocked orders specifically
    # ------------------------------------------------------------------
    try:
        stmt = select(func.count(Order.id)).where(
            Order.brand_id == effective_brand_id,
            ~Order.status.in_(_TERMINAL_STATUSES),
            Order.review_status == "blocked",
        )
        result = await db.execute(stmt)
        counts["blocked_orders"] = int(result.scalar() or 0)
    except Exception as exc:
        logger.error("attention_engine: blocked_orders query failed org_id=%s: %s", org_id, exc)
        errors.append(f"blocked_orders: {exc}")

    # ------------------------------------------------------------------
    # 4. Orders ready to pack (review_status = "ready")
    # ------------------------------------------------------------------
    try:
        stmt = select(func.count(Order.id)).where(
            Order.brand_id == effective_brand_id,
            ~Order.status.in_(_TERMINAL_STATUSES),
            Order.review_status == "ready",
        )
        result = await db.execute(stmt)
        counts["orders_ready_to_pack"] = int(result.scalar() or 0)
    except Exception as exc:
        logger.error("attention_engine: orders_ready_to_pack query failed org_id=%s: %s", org_id, exc)
        errors.append(f"orders_ready_to_pack: {exc}")

    # ------------------------------------------------------------------
    # 5. Orders missing customer_name
    # ------------------------------------------------------------------
    try:
        stmt = select(func.count(Order.id)).where(
            Order.brand_id == effective_brand_id,
            ~Order.status.in_(_TERMINAL_STATUSES),
            (Order.customer_name.is_(None)) | (Order.customer_name == ""),
        )
        result = await db.execute(stmt)
        counts["missing_customer_name"] = int(result.scalar() or 0)
    except Exception as exc:
        logger.error("attention_engine: missing_customer_name query failed org_id=%s: %s", org_id, exc)
        errors.append(f"missing_customer_name: {exc}")

    # ------------------------------------------------------------------
    # 6. Orders missing amount
    # ------------------------------------------------------------------
    try:
        stmt = select(func.count(Order.id)).where(
            Order.brand_id == effective_brand_id,
            ~Order.status.in_(_TERMINAL_STATUSES),
            Order.amount.is_(None),
        )
        result = await db.execute(stmt)
        counts["missing_amount"] = int(result.scalar() or 0)
    except Exception as exc:
        logger.error("attention_engine: missing_amount query failed org_id=%s: %s", org_id, exc)
        errors.append(f"missing_amount: {exc}")

    # ------------------------------------------------------------------
    # 7. Orders missing line_items
    # ------------------------------------------------------------------
    try:
        stmt = select(func.count(Order.id)).where(
            Order.brand_id == effective_brand_id,
            ~Order.status.in_(_TERMINAL_STATUSES),
            Order.line_items_json.is_(None),
        )
        result = await db.execute(stmt)
        counts["missing_line_items"] = int(result.scalar() or 0)
    except Exception as exc:
        logger.error("attention_engine: missing_line_items query failed org_id=%s: %s", org_id, exc)
        errors.append(f"missing_line_items: {exc}")

    # ------------------------------------------------------------------
    # 8. Unmapped line items (mapping_status != "mapped")
    # ------------------------------------------------------------------
    try:
        stmt = (
            select(func.count(OrderLine.id))
            .join(Order, OrderLine.order_id == Order.id)
            .where(
                Order.brand_id == effective_brand_id,
                ~Order.status.in_(_TERMINAL_STATUSES),
                (OrderLine.mapping_status != "mapped") | OrderLine.mapping_status.is_(None),
            )
        )
        result = await db.execute(stmt)
        counts["unmapped_line_items"] = int(result.scalar() or 0)
    except Exception as exc:
        logger.error("attention_engine: unmapped_line_items query failed org_id=%s: %s", org_id, exc)
        errors.append(f"unmapped_line_items: {exc}")

    # ------------------------------------------------------------------
    # 9. Inventory / compliance — not yet wired, return 0 with note
    # ------------------------------------------------------------------
    counts["low_inventory"] = 0   # not connected yet
    counts["compliance_issues"] = 0  # not connected yet

    # ------------------------------------------------------------------
    # 10. Recent sync status from BrandAPICredential
    # ------------------------------------------------------------------
    sync_status: Optional[str] = None
    last_sync_at: Optional[str] = None
    try:
        stmt = select(BrandAPICredential).where(
            BrandAPICredential.brand_id == effective_brand_id,
            BrandAPICredential.is_active.is_(True),
        ).limit(1)
        result = await db.execute(stmt)
        cred = result.scalar_one_or_none()
        if cred:
            sync_status = cred.sync_status
            last_sync_at = cred.last_sync_at.isoformat() if cred.last_sync_at else None
    except Exception as exc:
        logger.error("attention_engine: sync_status query failed org_id=%s: %s", org_id, exc)
        errors.append(f"sync_status: {exc}")

    # ------------------------------------------------------------------
    # 11. Determine data_source
    # ------------------------------------------------------------------
    any_real_data = any(
        counts[k] > 0
        for k in ("total_orders", "orders_needing_review", "blocked_orders", "unmapped_line_items")
    )
    data_source = "database" if any_real_data else "database"  # always database; mock flag separate

    # ------------------------------------------------------------------
    # 12. Calculate attention_score
    # ------------------------------------------------------------------
    score = 0
    score += counts["blocked_orders"] * _SCORE_BLOCKED
    score += counts["orders_needing_review"] * _SCORE_NEEDS_REVIEW
    score += counts["unmapped_line_items"] * _SCORE_UNMAPPED_LINE_ITEM
    score += counts["missing_customer_name"] * _SCORE_MISSING_CUSTOMER
    score += counts["missing_amount"] * _SCORE_MISSING_AMOUNT
    attention_score = min(score, 100)

    severity = _severity_from_score(attention_score)

    # ------------------------------------------------------------------
    # 13. Build reply strings and priority list
    # ------------------------------------------------------------------
    spoken_reply = _build_spoken_reply(counts, severity)
    screen_reply = _build_screen_reply(counts, severity)
    summary = _build_summary(severity, counts)
    top_priorities = _build_top_priorities(counts)
    suggested_actions = _build_suggested_actions(counts)

    priority_count = len(top_priorities)

    logger.info(
        "attention_analysis_complete org_id=%s order_count=%d priority_count=%d "
        "severity=%s data_source=%s attention_score=%d",
        org_id,
        counts["total_orders"],
        priority_count,
        severity,
        data_source,
        attention_score,
    )

    return {
        "ok": True,
        "attention_score": attention_score,
        "severity": severity,
        "spoken_reply": spoken_reply,
        "screen_reply": screen_reply,
        "summary": summary,
        "top_priorities": top_priorities,
        "counts": counts,
        "suggested_actions": suggested_actions,
        "sync_status": sync_status,
        "last_sync_at": last_sync_at,
        "data_source": data_source,
        "errors": errors,
    }


# ---------------------------------------------------------------------------
# Alias for backward compatibility
# ---------------------------------------------------------------------------

async def get_attention_report(
    db: AsyncSession,
    org_id: str,
    brand_id: Optional[str] = None,
) -> dict:
    """Alias for get_operational_attention — use that function directly."""
    return await get_operational_attention(db, org_id, brand_id)
