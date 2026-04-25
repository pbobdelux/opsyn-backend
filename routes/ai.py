"""
AI endpoints for ElevenLabs webhook tools and the Opsyn Brand App.

These endpoints are designed to be:
- Safe: no destructive actions without explicit confirmation.
- Structured: always return JSON, never HTML error pages.
- Logged: org_id, user_id, and action context are logged; secrets and
  full PII (addresses, full customer names) are never logged.
- Resilient: database errors return a 200 with ok=false rather than a 500
  crash, so webhook callers always receive a parseable response.
"""

import logging
from typing import Any, Optional

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from services.ai_service import (
    build_attention_summary,
    get_compliance_issues_count,
    get_low_inventory_count,
    get_pending_orders_count,
)
from services import assistant_action_registry as registry

logger = logging.getLogger("ai_routes")

router = APIRouter(tags=["ai"])


# ---------------------------------------------------------------------------
# POST /ai/get-attention
# ---------------------------------------------------------------------------

@router.post("/get-attention")
async def get_attention(
    body: dict,
    db: AsyncSession = Depends(get_db),
) -> Any:
    """Query pending orders and system status for the AI agent.

    Expected body fields:
      - org_id (required)
      - user_id (optional)
      - role (optional)
      - facility_id (optional)
      - app_context (optional)

    Returns a structured summary suitable for ElevenLabs voice responses
    and Brand App status cards.
    """
    org_id: Optional[str] = body.get("org_id")
    user_id: Optional[str] = body.get("user_id")
    app_context: Optional[str] = body.get("app_context")
    brand_id: Optional[str] = body.get("brand_id")

    # --- Validation ---
    if not org_id:
        logger.warning("get_attention called without org_id")
        return JSONResponse(
            status_code=200,
            content={
                "ok": False,
                "error": "missing_org_id",
                "message": "org_id is required",
            },
        )

    logger.info(
        "get_attention org_id=%s user_id=%s app_context=%s",
        org_id,
        user_id or "unknown",
        app_context or "unknown",
    )

    errors: list[str] = []

    try:
        pending = await get_pending_orders_count(db, org_id, brand_id)
        low_inv = await get_low_inventory_count(db, org_id)
        compliance = await get_compliance_issues_count(db, org_id)

        # Query detailed order breakdowns from the action registry handlers
        review_result = await registry.execute_action(db, "get_orders_needing_review", org_id, {})
        packed_result = await registry.execute_action(db, "get_orders_needing_packed", org_id, {})
        blocked_result = await registry.execute_action(db, "get_blocked_orders", org_id, {})

        orders_needing_review = review_result.get("count", 0) if review_result.get("ok") else 0
        orders_ready_to_pack = packed_result.get("count", 0) if packed_result.get("ok") else 0
        blocked_orders = blocked_result.get("count", 0) if blocked_result.get("ok") else 0

        if not review_result.get("ok") and review_result.get("error"):
            errors.append(f"orders_needing_review: {review_result['error']}")
        if not packed_result.get("ok") and packed_result.get("error"):
            errors.append(f"orders_ready_to_pack: {packed_result['error']}")
        if not blocked_result.get("ok") and blocked_result.get("error"):
            errors.append(f"blocked_orders: {blocked_result['error']}")

        # Determine data source
        has_data = (pending + orders_needing_review + orders_ready_to_pack + blocked_orders) > 0
        data_source = "database" if has_data else "stub"

        summary = build_attention_summary(pending, low_inv, compliance)

        logger.info(
            "get_attention result org_id=%s pending=%d review=%d pack=%d blocked=%d data_source=%s",
            org_id,
            pending,
            orders_needing_review,
            orders_ready_to_pack,
            blocked_orders,
            data_source,
        )

        return {
            "ok": True,
            "pending_orders": pending,
            "orders_needing_review": orders_needing_review,
            "orders_ready_to_pack": orders_ready_to_pack,
            "blocked_orders": blocked_orders,
            "low_inventory": low_inv,
            "compliance_issues": compliance,
            "summary": summary,
            "data_source": data_source,
            "errors": errors,
        }

    except Exception as exc:
        logger.error("get_attention failed for org_id=%s: %s", org_id, exc)
        return JSONResponse(
            status_code=200,
            content={
                "ok": False,
                "error": "server_error",
                "message": "An internal error occurred while fetching attention data.",
                "errors": [str(exc)],
            },
        )


# ---------------------------------------------------------------------------
# POST /ai/approve-orders
# ---------------------------------------------------------------------------

@router.post("/approve-orders")
async def approve_orders(body: dict) -> Any:
    """Safe placeholder for order approval actions.

    Order approvals require explicit backend confirmation before execution.
    This endpoint logs the request and returns a requires_confirmation
    response so the AI agent can prompt the user for confirmation.

    Expected body fields:
      - org_id (required)
      - user_id (optional)
      - role (optional)
      - order_ids (list of order IDs to approve)
    """
    org_id: Optional[str] = body.get("org_id")
    user_id: Optional[str] = body.get("user_id")
    order_ids = body.get("order_ids") or []

    if not org_id:
        logger.warning("approve_orders called without org_id")
        return JSONResponse(
            status_code=200,
            content={
                "ok": False,
                "error": "missing_org_id",
                "message": "org_id is required",
            },
        )

    order_count = len(order_ids) if isinstance(order_ids, list) else 0

    logger.info(
        "approve_orders requested org_id=%s user_id=%s order_count=%d",
        org_id,
        user_id or "unknown",
        order_count,
    )

    return {
        "ok": False,
        "requires_confirmation": True,
        "message": "Order approval requires backend confirmation before execution.",
    }


# ---------------------------------------------------------------------------
# POST /ai/fix-compliance
# ---------------------------------------------------------------------------

@router.post("/fix-compliance")
async def fix_compliance(body: dict) -> Any:
    """Safe placeholder for compliance fix actions.

    Compliance fixes require explicit backend confirmation before execution.
    This endpoint logs the request and returns a requires_confirmation
    response so the AI agent can prompt the user for confirmation.

    Expected body fields:
      - org_id (required)
      - user_id (optional)
      - role (optional)
      - issue_type (string describing the compliance issue)
    """
    org_id: Optional[str] = body.get("org_id")
    user_id: Optional[str] = body.get("user_id")
    issue_type: Optional[str] = body.get("issue_type")

    if not org_id:
        logger.warning("fix_compliance called without org_id")
        return JSONResponse(
            status_code=200,
            content={
                "ok": False,
                "error": "missing_org_id",
                "message": "org_id is required",
            },
        )

    logger.info(
        "fix_compliance requested org_id=%s user_id=%s issue_type=%s",
        org_id,
        user_id or "unknown",
        issue_type or "unknown",
    )

    return {
        "ok": False,
        "requires_confirmation": True,
        "message": "Compliance fix requires backend confirmation before execution.",
    }
