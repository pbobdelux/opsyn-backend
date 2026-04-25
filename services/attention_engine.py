"""
Attention Engine — comprehensive operational priority analysis.

Queries real database data to produce a structured attention report
suitable for voice (ElevenLabs) and screen (Brand App) consumption.

Brand-ID filtering behaviour
-----------------------------
When brand_id is explicitly provided the engine filters all queries to that
brand.  When brand_id is None (the default) the engine queries ALL orders —
matching the behaviour of the /leaflink/orders endpoint — so that the
attention counts always reflect the full picture visible in the UI.

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

from sqlalchemy import func, select, text
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


async def _check_for_unmapped_amount_fields(
    db: AsyncSession,
    filter_by_brand: bool,
    brand_id: Optional[str] = None,
) -> dict[str, bool]:
    """
    Check if raw LeafLink fields exist in order data but aren't mapped to
    Order.amount.

    Queries the raw_payload JSONB column to detect whether LeafLink sends
    pricing fields that are simply not being mapped into the Order model.

    Returns
    -------
    {
        "has_total_field": bool,
        "has_subtotal_field": bool,
        "has_price_field": bool,
        "has_amount_field": bool,
    }
    """
    result: dict[str, bool] = {
        "has_total_field": False,
        "has_subtotal_field": False,
        "has_price_field": False,
        "has_amount_field": False,
    }

    brand_clause = "AND brand_id = :brand_id" if filter_by_brand else ""
    params: dict = {"brand_id": brand_id} if filter_by_brand else {}

    field_map = {
        "has_total_field": "total",
        "has_subtotal_field": "subtotal",
        "has_price_field": "price",
        "has_amount_field": "amount",
    }

    for result_key, json_field in field_map.items():
        try:
            sql = text(
                f"""
                SELECT EXISTS (
                    SELECT 1 FROM orders
                    WHERE raw_payload ? :field
                    {brand_clause}
                    LIMIT 1
                )
                """
            )
            row = await db.execute(sql, {**params, "field": json_field})
            result[result_key] = bool(row.scalar())
        except Exception as exc:
            logger.warning(
                "attention_engine: unmapped_field_check failed field=%s: %s",
                json_field,
                exc,
            )

    return result


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


def _build_spoken_reply(counts: dict, severity: str, is_system_issue: bool = False) -> str:
    """Build a concise, voice-friendly reply for ElevenLabs."""
    parts: list[str] = []

    blocked = counts.get("blocked_orders", 0)
    review = counts.get("orders_needing_review", 0)
    unmapped = counts.get("unmapped_line_items", 0)
    missing_name = counts.get("missing_customer_name", 0)
    missing_amt = counts.get("missing_amount", 0)
    total = counts.get("total_orders", 0)

    # System issue: all orders are missing amount — this is a mapping problem,
    # not a real business issue. Return a diagnostic message instead.
    if is_system_issue:
        return (
            "All orders are missing pricing data, which likely indicates a system "
            "mapping issue rather than actual missing data. "
            "Check the LeafLink sync configuration."
        )

    # Only report "all clear" when there are genuinely zero orders AND no issues.
    if severity == "clear" and total == 0:
        return "No active orders found. Everything is clear."

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
        return (
            f"You have {total} active order{'s' if total != 1 else ''}. "
            "No critical issues found."
        )

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

    return (
        f"You have {total} active order{'s' if total != 1 else ''} in the system. "
        f"{summary[0].upper() + summary[1:]}.{recommendation}"
    )


def _build_screen_reply(counts: dict, severity: str, is_system_issue: bool = False) -> str:
    """Build a compact screen-friendly reply for the Brand App."""
    if is_system_issue:
        return "System mapping issue detected • Check LeafLink sync"

    if severity == "clear":
        total = counts.get("total_orders", 0)
        if total == 0:
            return "No active orders • All clear"
        return f"{total} active order{'s' if total != 1 else ''} • All clear"

    parts: list[str] = []
    if counts.get("blocked_orders"):
        parts.append(f"{counts['blocked_orders']} blocked")
    if counts.get("orders_needing_review"):
        parts.append(f"{counts['orders_needing_review']} need review")
    if counts.get("unmapped_line_items"):
        parts.append(f"{counts['unmapped_line_items']} unmapped items")
    if counts.get("missing_customer_name"):
        parts.append(f"{counts['missing_customer_name']} missing name")
    if counts.get("missing_amount") and not is_system_issue:
        parts.append(f"{counts['missing_amount']} missing amount")

    return " • ".join(parts) if parts else f"{counts.get('total_orders', 0)} active orders"


def _build_summary(severity: str, counts: dict, is_system_issue: bool = False) -> str:
    """Build a one-sentence operational summary."""
    if is_system_issue:
        return (
            "System issue detected: all orders are missing pricing data. "
            "This is likely a LeafLink data mapping problem, not a real business issue. "
            "Verify the LeafLink sync configuration."
        )
    if severity == "clear":
        total = counts.get("total_orders", 0)
        if total == 0:
            return "All systems are clear. No active orders or issues."
        return f"All systems are clear. {total} active order{'s' if total != 1 else ''} with no issues requiring attention."
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


def _build_top_priorities(counts: dict, is_system_issue: bool = False) -> list[dict]:
    """Build the ranked priority list."""
    priorities: list[dict] = []
    rank = 1

    missing_amt = counts.get("missing_amount", 0)
    total = counts.get("total_orders", 0)

    # When all orders are missing amount, surface a system-level diagnostic
    # priority at rank 1 and skip the normal "Missing Order Amounts" entry.
    if is_system_issue:
        priorities.append({
            "rank": rank,
            "severity": "critical",
            "title": "Order Amount Mapping Issue",
            "detail": (
                f"All {total} order{'s are' if total != 1 else ' is'} missing pricing data. "
                "This indicates a data ingestion or mapping issue from LeafLink."
            ),
            "recommended_action": (
                "Verify LeafLink API response fields and ensure amount/total is "
                "correctly mapped during sync."
            ),
            "action_key": "diagnose_order_amount_mapping",
            "is_system_issue": True,
        })
        rank += 1

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

    # Only add the normal "Missing Order Amounts" priority when it is NOT a
    # system-wide mapping issue (i.e. only some orders are missing amount).
    if missing_amt and not is_system_issue:
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
        The organisation identifier.
    brand_id:
        Optional explicit brand_id filter. When provided, only orders with this
        brand_id are counted. When None, ALL orders are queried (matching the
        behaviour of the /leaflink/orders endpoint).

    Returns
    -------
    A structured dict with attention_score, severity, spoken_reply, screen_reply,
    top_priorities, counts, suggested_actions, data_source, and errors.
    """
    errors: list[str] = []

    # Determine whether to apply a brand_id filter.
    # If brand_id is explicitly provided we filter; otherwise we query ALL orders
    # (same as /leaflink/orders which has no brand_id filter).
    filter_by_brand = brand_id is not None

    logger.info(
        "attention_request_received org_id=%s brand_id=%s filter_by_brand=%s",
        org_id,
        brand_id,
        filter_by_brand,
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

    # ------------------------------------------------------------------
    # Helper: build the base Order WHERE clauses
    # ------------------------------------------------------------------
    def _order_base_filters():
        """Return the common filters applied to every Order query."""
        filters = [~Order.status.in_(_TERMINAL_STATUSES)]
        if filter_by_brand:
            filters.append(Order.brand_id == brand_id)
        return filters

    # ------------------------------------------------------------------
    # 1. Total active orders
    # ------------------------------------------------------------------
    try:
        stmt = select(func.count(Order.id)).where(*_order_base_filters())
        result = await db.execute(stmt)
        counts["total_orders"] = int(result.scalar() or 0)
        logger.info(
            "attention_engine: total_orders=%d org_id=%s filter_by_brand=%s",
            counts["total_orders"],
            org_id,
            filter_by_brand,
        )
    except Exception as exc:
        logger.error("attention_engine: total_orders query failed org_id=%s: %s", org_id, exc)
        errors.append(f"total_orders: {exc}")

    # ------------------------------------------------------------------
    # 2. Orders by review_status needing attention
    # ------------------------------------------------------------------
    try:
        stmt = select(func.count(Order.id)).where(
            *_order_base_filters(),
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
            *_order_base_filters(),
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
            *_order_base_filters(),
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
            *_order_base_filters(),
            (Order.customer_name.is_(None)) | (Order.customer_name == ""),
        )
        result = await db.execute(stmt)
        counts["missing_customer_name"] = int(result.scalar() or 0)
    except Exception as exc:
        logger.error("attention_engine: missing_customer_name query failed org_id=%s: %s", org_id, exc)
        errors.append(f"missing_customer_name: {exc}")

    # ------------------------------------------------------------------
    # 6. Orders missing amount (null or zero)
    # ------------------------------------------------------------------
    try:
        stmt = select(func.count(Order.id)).where(
            *_order_base_filters(),
            (Order.amount.is_(None)) | (Order.amount == 0),
        )
        result = await db.execute(stmt)
        counts["missing_amount"] = int(result.scalar() or 0)
    except Exception as exc:
        logger.error("attention_engine: missing_amount query failed org_id=%s: %s", org_id, exc)
        errors.append(f"missing_amount: {exc}")

    # ------------------------------------------------------------------
    # 7. Orders missing line_items (null or empty list)
    # ------------------------------------------------------------------
    try:
        stmt = select(func.count(Order.id)).where(
            *_order_base_filters(),
            (Order.line_items_json.is_(None)) | (Order.line_items_json == []),
        )
        result = await db.execute(stmt)
        counts["missing_line_items"] = int(result.scalar() or 0)
    except Exception as exc:
        logger.error("attention_engine: missing_line_items query failed org_id=%s: %s", org_id, exc)
        errors.append(f"missing_line_items: {exc}")

    # ------------------------------------------------------------------
    # 8. Unmapped line items (mapping_status != "mapped" or null)
    # ------------------------------------------------------------------
    try:
        unmapped_filters = [
            ~Order.status.in_(_TERMINAL_STATUSES),
            (OrderLine.mapping_status != "mapped") | OrderLine.mapping_status.is_(None),
        ]
        if filter_by_brand:
            unmapped_filters.append(Order.brand_id == brand_id)

        stmt = (
            select(func.count(OrderLine.id))
            .join(Order, OrderLine.order_id == Order.id)
            .where(*unmapped_filters)
        )
        result = await db.execute(stmt)
        counts["unmapped_line_items"] = int(result.scalar() or 0)
    except Exception as exc:
        logger.error("attention_engine: unmapped_line_items query failed org_id=%s: %s", org_id, exc)
        errors.append(f"unmapped_line_items: {exc}")

    logger.info(
        "attention_engine: counts_loaded org_id=%s total_orders=%d orders_needing_review=%d "
        "blocked_orders=%d orders_ready_to_pack=%d unmapped_line_items=%d "
        "missing_customer_name=%d missing_amount=%d missing_line_items=%d",
        org_id,
        counts["total_orders"],
        counts["orders_needing_review"],
        counts["blocked_orders"],
        counts["orders_ready_to_pack"],
        counts["unmapped_line_items"],
        counts["missing_customer_name"],
        counts["missing_amount"],
        counts["missing_line_items"],
    )

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
        cred_filters = [BrandAPICredential.is_active.is_(True)]
        if filter_by_brand:
            cred_filters.append(BrandAPICredential.brand_id == brand_id)

        stmt = select(BrandAPICredential).where(*cred_filters).limit(1)
        result = await db.execute(stmt)
        cred = result.scalar_one_or_none()
        if cred:
            sync_status = cred.sync_status
            last_sync_at = cred.last_sync_at.isoformat() if cred.last_sync_at else None
    except Exception as exc:
        logger.error("attention_engine: sync_status query failed org_id=%s: %s", org_id, exc)
        errors.append(f"sync_status: {exc}")

    # data_source is always "database" — we query real data, never mock.

    # ------------------------------------------------------------------
    # 12. Detect system-level issues
    # ------------------------------------------------------------------
    # When every active order is missing an amount this is almost certainly a
    # data mapping problem from LeafLink rather than a real business issue.
    # We classify it separately so the attention system doesn't inflate the
    # score or mislead the operator.
    is_system_issue = (
        counts["total_orders"] > 0
        and counts["missing_amount"] == counts["total_orders"]
    )

    if is_system_issue:
        logger.info(
            "attention_engine: system_issue_detected org_id=%s missing_amount=%d total_orders=%d",
            org_id,
            counts["missing_amount"],
            counts["total_orders"],
        )
        # Run the unmapped-field check to give operators more diagnostic detail.
        unmapped_fields = await _check_for_unmapped_amount_fields(
            db, filter_by_brand, brand_id
        )
        logger.info(
            "attention_engine: unmapped_field_check org_id=%s result=%s",
            org_id,
            unmapped_fields,
        )

    # ------------------------------------------------------------------
    # 13. Calculate attention_score
    # ------------------------------------------------------------------
    score = 0
    score += counts["blocked_orders"] * _SCORE_BLOCKED
    score += counts["orders_needing_review"] * _SCORE_NEEDS_REVIEW
    score += counts["unmapped_line_items"] * _SCORE_UNMAPPED_LINE_ITEM
    score += counts["missing_customer_name"] * _SCORE_MISSING_CUSTOMER
    # When it's a system issue, exclude missing_amount from the score so the
    # severity is not artificially inflated by a mapping problem.
    if not is_system_issue:
        score += counts["missing_amount"] * _SCORE_MISSING_AMOUNT
    attention_score = min(score, 100)

    severity = _severity_from_score(attention_score)
    # Per spec: only return "clear" when total_orders == 0 AND no issues.
    # If there are real orders but no scored issues, use "low" so the reply
    # always reports the actual order count rather than faking "all clear".
    if severity == "clear" and counts["total_orders"] > 0:
        severity = "low"

    # When the only issue is the system mapping problem (no other scored
    # issues), cap severity at "medium" so it doesn't read as "all clear"
    # but also doesn't over-alarm the operator.
    if is_system_issue and score == 0:
        severity = "medium"

    # ------------------------------------------------------------------
    # 14. Build reply strings and priority list
    # ------------------------------------------------------------------
    spoken_reply = _build_spoken_reply(counts, severity, is_system_issue=is_system_issue)
    screen_reply = _build_screen_reply(counts, severity, is_system_issue=is_system_issue)
    summary = _build_summary(severity, counts, is_system_issue=is_system_issue)
    top_priorities = _build_top_priorities(counts, is_system_issue=is_system_issue)
    suggested_actions = _build_suggested_actions(counts)

    priority_count = len(top_priorities)

    logger.info(
        "attention_analysis_complete org_id=%s order_count=%d priority_count=%d "
        "severity=%s data_source=%s attention_score=%d is_system_issue=%s",
        org_id,
        counts["total_orders"],
        priority_count,
        severity,
        data_source,
        attention_score,
        is_system_issue,
    )

    return {
        "ok": True,
        "attention_score": attention_score,
        "severity": severity,
        "is_system_issue": is_system_issue,
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
