"""
Compliance service for Opsyn Assistant.

Provides inspection readiness checks by querying the database for
known compliance risk indicators.

Security rules:
  - Never log full customer names or PII
  - Never raise — always return structured JSON
  - Log action name, org_id, issue counts only
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger("compliance_service")


# ---------------------------------------------------------------------------
# Issue severity constants
# ---------------------------------------------------------------------------

SEVERITY_LOW = "low"
SEVERITY_MEDIUM = "medium"
SEVERITY_HIGH = "high"
SEVERITY_CRITICAL = "critical"

CATEGORY_ORDERS = "orders"
CATEGORY_INVENTORY = "inventory"
CATEGORY_METRC = "metrc"
CATEGORY_RECORDS = "records"
CATEGORY_SECURITY = "security"


def _build_issue(
    severity: str,
    category: str,
    title: str,
    detail: str,
    recommended_action: str,
) -> dict[str, str]:
    return {
        "severity": severity,
        "category": category,
        "title": title,
        "detail": detail,
        "recommended_action": recommended_action,
    }


# ---------------------------------------------------------------------------
# Main check
# ---------------------------------------------------------------------------


async def run_inspection_readiness_check(
    db: AsyncSession,
    org_id: str,
) -> dict[str, Any]:
    """
    Run a compliance inspection readiness check for the organisation.

    Queries the database for known risk indicators and returns a structured
    list of issues with severity, category, and recommended actions.

    Returns
    -------
    {
        "ok": True,
        "status": "ready" | "needs_attention" | "critical",
        "issue_count": int,
        "issues": [...],
        "summary": str,
    }
    """
    issues: list[dict[str, str]] = []
    errors: list[str] = []

    try:
        from sqlalchemy import func, select

        from models import BrandAPICredential, Order

        # ------------------------------------------------------------------
        # 1. Blocked orders
        # ------------------------------------------------------------------
        try:
            result = await db.execute(
                select(func.count(Order.id)).where(
                    Order.brand_id == org_id,
                    Order.review_status == "blocked",
                )
            )
            blocked_count = result.scalar() or 0
            if blocked_count > 0:
                issues.append(_build_issue(
                    severity=SEVERITY_HIGH if blocked_count >= 3 else SEVERITY_MEDIUM,
                    category=CATEGORY_ORDERS,
                    title=f"{blocked_count} blocked order{'s' if blocked_count != 1 else ''}",
                    detail=(
                        f"{blocked_count} order{'s are' if blocked_count != 1 else ' is'} "
                        "blocked due to mapping or compliance issues."
                    ),
                    recommended_action="Review and resolve blocked orders before inspection.",
                ))
        except Exception as exc:
            logger.error("compliance_service: blocked_orders check failed: %s", exc)
            errors.append(f"blocked_orders: {exc}")

        # ------------------------------------------------------------------
        # 2. Orders needing review
        # ------------------------------------------------------------------
        try:
            result = await db.execute(
                select(func.count(Order.id)).where(
                    Order.brand_id == org_id,
                    Order.review_status == "needs_review",
                )
            )
            review_count = result.scalar() or 0
            if review_count > 0:
                issues.append(_build_issue(
                    severity=SEVERITY_MEDIUM,
                    category=CATEGORY_ORDERS,
                    title=f"{review_count} order{'s' if review_count != 1 else ''} need review",
                    detail=(
                        f"{review_count} order{'s' if review_count != 1 else ''} "
                        "have not been reviewed and may have compliance gaps."
                    ),
                    recommended_action="Review all pending orders and resolve any mapping issues.",
                ))
        except Exception as exc:
            logger.error("compliance_service: needs_review check failed: %s", exc)
            errors.append(f"needs_review: {exc}")

        # ------------------------------------------------------------------
        # 3. Orders with missing line items
        # ------------------------------------------------------------------
        try:
            result = await db.execute(
                select(func.count(Order.id)).where(
                    Order.brand_id == org_id,
                    Order.item_count == 0,
                )
            )
            missing_lines_count = result.scalar() or 0
            if missing_lines_count > 0:
                issues.append(_build_issue(
                    severity=SEVERITY_HIGH,
                    category=CATEGORY_RECORDS,
                    title=f"{missing_lines_count} order{'s' if missing_lines_count != 1 else ''} missing line items",
                    detail=(
                        f"{missing_lines_count} order{'s have' if missing_lines_count != 1 else ' has'} "
                        "zero line items, which may indicate incomplete records."
                    ),
                    recommended_action="Sync orders from LeafLink and verify line item data.",
                ))
        except Exception as exc:
            logger.error("compliance_service: missing_line_items check failed: %s", exc)
            errors.append(f"missing_line_items: {exc}")

        # ------------------------------------------------------------------
        # 4. Orders with missing customer name
        # ------------------------------------------------------------------
        try:
            result = await db.execute(
                select(func.count(Order.id)).where(
                    Order.brand_id == org_id,
                    Order.customer_name.is_(None),
                )
            )
            missing_customer_count = result.scalar() or 0
            if missing_customer_count > 0:
                issues.append(_build_issue(
                    severity=SEVERITY_MEDIUM,
                    category=CATEGORY_RECORDS,
                    title=f"{missing_customer_count} order{'s' if missing_customer_count != 1 else ''} missing customer name",
                    detail=(
                        f"{missing_customer_count} order{'s are' if missing_customer_count != 1 else ' is'} "
                        "missing a customer name, which is required for compliance records."
                    ),
                    recommended_action="Re-sync orders from LeafLink to populate customer data.",
                ))
        except Exception as exc:
            logger.error("compliance_service: missing_customer check failed: %s", exc)
            errors.append(f"missing_customer: {exc}")

        # ------------------------------------------------------------------
        # 5. Orders with missing amount
        # ------------------------------------------------------------------
        try:
            result = await db.execute(
                select(func.count(Order.id)).where(
                    Order.brand_id == org_id,
                    Order.amount.is_(None),
                    Order.total_cents.is_(None),
                )
            )
            missing_amount_count = result.scalar() or 0
            if missing_amount_count > 0:
                issues.append(_build_issue(
                    severity=SEVERITY_MEDIUM,
                    category=CATEGORY_RECORDS,
                    title=f"{missing_amount_count} order{'s' if missing_amount_count != 1 else ''} missing amount",
                    detail=(
                        f"{missing_amount_count} order{'s are' if missing_amount_count != 1 else ' is'} "
                        "missing a dollar amount, which is required for financial compliance."
                    ),
                    recommended_action="Re-sync orders from LeafLink to populate amount data.",
                ))
        except Exception as exc:
            logger.error("compliance_service: missing_amount check failed: %s", exc)
            errors.append(f"missing_amount: {exc}")

        # ------------------------------------------------------------------
        # 6. Stale LeafLink sync (> 24 hours)
        # ------------------------------------------------------------------
        try:
            result = await db.execute(
                select(BrandAPICredential).where(
                    BrandAPICredential.brand_id == org_id,
                    BrandAPICredential.integration_name == "leaflink",
                    BrandAPICredential.is_active == True,
                )
            )
            cred = result.scalar_one_or_none()
            if cred and cred.last_sync_at:
                now = datetime.now(timezone.utc)
                age = now - cred.last_sync_at
                if age > timedelta(hours=24):
                    hours_old = int(age.total_seconds() / 3600)
                    issues.append(_build_issue(
                        severity=SEVERITY_HIGH if hours_old > 48 else SEVERITY_MEDIUM,
                        category=CATEGORY_RECORDS,
                        title=f"LeafLink sync is {hours_old} hours old",
                        detail=(
                            f"The last LeafLink sync was {hours_old} hours ago. "
                            "Stale data may cause compliance gaps."
                        ),
                        recommended_action="Run a LeafLink sync to refresh order data.",
                    ))
            elif cred and cred.last_sync_at is None:
                issues.append(_build_issue(
                    severity=SEVERITY_HIGH,
                    category=CATEGORY_RECORDS,
                    title="LeafLink has never been synced",
                    detail="No LeafLink sync has been performed for this organisation.",
                    recommended_action="Run a LeafLink sync to import order data.",
                ))
        except Exception as exc:
            logger.error("compliance_service: stale_sync check failed: %s", exc)
            errors.append(f"stale_sync: {exc}")

    except Exception as exc:
        logger.exception("compliance_service: run_inspection_readiness_check failed org_id=%s", org_id)
        return {
            "ok": False,
            "error": str(exc),
            "status": "critical",
            "issue_count": 0,
            "issues": [],
            "summary": "Compliance check failed due to an internal error.",
            "errors": [str(exc)],
        }

    # ------------------------------------------------------------------
    # Determine overall status
    # ------------------------------------------------------------------
    issue_count = len(issues)

    if issue_count == 0:
        status = "ready"
        summary = "All compliance checks passed. No issues found."
    elif issue_count < 5:
        status = "needs_attention"
        summary = f"{issue_count} compliance issue{'s' if issue_count != 1 else ''} found. Review before inspection."
    else:
        status = "critical"
        summary = f"{issue_count} compliance issues found. Immediate attention required."

    logger.info(
        "compliance_service: org_id=%s status=%s issue_count=%d",
        org_id, status, issue_count,
    )

    return {
        "ok": True,
        "status": status,
        "issue_count": issue_count,
        "issues": issues,
        "summary": summary,
        "errors": errors,
    }
