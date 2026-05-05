"""
Webhook endpoints for inbound LeafLink payloads.

Routes:
  POST /webhooks/leaflink          — receive LeafLink webhook events
  GET  /webhooks/leaflink/setup-info   — webhook configuration helper
  GET  /webhooks/leaflink/diagnostic   — webhook + sync diagnostics
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Header, Query, Request
from fastapi.responses import JSONResponse
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import BrandAPICredential, Order, SyncRun
from services.leaflink_webhook import process_leaflink_webhook

logger = logging.getLogger("webhooks")

router = APIRouter(prefix="/webhooks", tags=["webhooks"])

# Production webhook URL — used in setup-info response
_WEBHOOK_BASE_URL = "https://opsyn-backend-production.up.railway.app"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# POST /webhooks/leaflink
# ---------------------------------------------------------------------------

@router.post("/leaflink")
async def receive_leaflink_webhook(
    request: Request,
    ll_signature: Optional[str] = Header(None, alias="LL-Signature"),
    db: AsyncSession = Depends(get_db),
):
    """
    Receive inbound LeafLink webhook payloads.

    Supported events:
    - New & Changed Orders
    - New & Changed Products

    Verifies LL-Signature header using HMAC-SHA256 with the brand's webhook_key.
    Resolves tenant from company_id in the payload.
    Upserts orders idempotently by (brand_id, external_order_id).

    Always returns HTTP 200 — LeafLink requires a 200 response to consider
    the delivery successful. Errors are reported in the response body.
    """
    # Read raw body for signature verification before JSON parsing
    try:
        payload_bytes = await request.body()
    except Exception as exc:
        logger.error("[LeafLinkWebhook] body_read_error error=%s", exc)
        return JSONResponse(
            status_code=200,
            content={
                "ok": False,
                "received_count": 0,
                "upserted_count": 0,
                "errors": ["Failed to read request body"],
            },
        )

    # Parse JSON payload
    try:
        payload = await request.json()
    except Exception as exc:
        logger.error("[LeafLinkWebhook] json_parse_error error=%s", exc)
        return JSONResponse(
            status_code=200,
            content={
                "ok": False,
                "received_count": 0,
                "upserted_count": 0,
                "errors": ["Invalid JSON payload"],
            },
        )

    if not isinstance(payload, dict):
        return JSONResponse(
            status_code=200,
            content={
                "ok": False,
                "received_count": 0,
                "upserted_count": 0,
                "errors": ["Payload must be a JSON object"],
            },
        )

    # Process the webhook
    result = await process_leaflink_webhook(
        payload=payload,
        payload_bytes=payload_bytes,
        signature_header=ll_signature,
        db=db,
    )

    # Always return 200 — LeafLink requires it
    return JSONResponse(
        status_code=200,
        content={
            "ok": result["ok"],
            "received_count": result["received_count"],
            "upserted_count": result["upserted_count"],
            "errors": result["errors"],
            **({"error": result["error"]} if result.get("error") else {}),
        },
    )


# ---------------------------------------------------------------------------
# GET /webhooks/leaflink/setup-info
# ---------------------------------------------------------------------------

@router.get("/leaflink/setup-info")
async def leaflink_webhook_setup_info(
    brand_id: str = Query(..., description="Brand UUID"),
    db: AsyncSession = Depends(get_db),
):
    """
    Return webhook configuration information for a brand.

    Use this to configure the webhook URL in the LeafLink dashboard.
    Shows the webhook URL, supported events, company_id, and whether
    the webhook_key is configured.
    """
    logger.info("[LeafLinkWebhook] setup_info_request brand_id=%s", brand_id)

    try:
        result = await db.execute(
            select(BrandAPICredential).where(
                BrandAPICredential.brand_id == brand_id,
                BrandAPICredential.integration_name == "leaflink",
            )
        )
        cred = result.scalar_one_or_none()
    except Exception as exc:
        logger.error(
            "[LeafLinkWebhook] setup_info_db_error brand_id=%s error=%s",
            brand_id,
            exc,
        )
        return {
            "ok": False,
            "error": "Database error during credential lookup",
            "brand_id": brand_id,
        }

    if not cred:
        return {
            "ok": False,
            "error": "No LeafLink credential found for this brand",
            "brand_id": brand_id,
        }

    webhook_url = f"{_WEBHOOK_BASE_URL}/webhooks/leaflink"

    return {
        "ok": True,
        "webhook_url": webhook_url,
        "events": ["New & Changed Orders", "New & Changed Products"],
        "company_id": cred.company_id,
        "brand_id": brand_id,
        "org_id": cred.org_id,
        "webhook_key_configured": bool(cred.webhook_key),
    }


# ---------------------------------------------------------------------------
# GET /webhooks/leaflink/diagnostic
# ---------------------------------------------------------------------------

@router.get("/leaflink/diagnostic")
async def leaflink_webhook_diagnostic(
    brand_id: str = Query(..., description="Brand UUID"),
    db: AsyncSession = Depends(get_db),
):
    """
    Diagnostic endpoint for LeafLink webhook and API sync health.

    Returns:
    - Credential existence and configuration
    - Last webhook received timestamp and order count
    - Last API sync timestamp and order count
    - Total orders in database
    - Last error information
    """
    logger.info("[LeafLinkWebhook] diagnostic_request brand_id=%s", brand_id)

    timestamp = _utc_now_iso()

    # Load credential
    try:
        cred_result = await db.execute(
            select(BrandAPICredential).where(
                BrandAPICredential.brand_id == brand_id,
                BrandAPICredential.integration_name == "leaflink",
            )
        )
        cred = cred_result.scalar_one_or_none()
    except Exception as exc:
        logger.error(
            "[LeafLinkWebhook] diagnostic_cred_error brand_id=%s error=%s",
            brand_id,
            exc,
        )
        return {
            "ok": False,
            "brand_id": brand_id,
            "error": "Database error during credential lookup",
            "timestamp": timestamp,
        }

    credential_exists = cred is not None
    webhook_key_configured = bool(cred and cred.webhook_key)
    org_id = cred.org_id if cred else None

    # Count total orders in DB for this brand
    try:
        count_result = await db.execute(
            select(func.count(Order.id)).where(Order.brand_id == brand_id)
        )
        total_orders_in_db = count_result.scalar_one() or 0
    except Exception as exc:
        logger.error(
            "[LeafLinkWebhook] diagnostic_order_count_error brand_id=%s error=%s",
            brand_id,
            exc,
        )
        total_orders_in_db = 0

    # Last webhook-sourced order
    last_webhook_received: Optional[str] = None
    last_webhook_order_count: Optional[int] = None
    try:
        webhook_result = await db.execute(
            select(
                func.max(Order.synced_at),
                func.count(Order.id),
            ).where(
                Order.brand_id == brand_id,
                Order.source == "leaflink_webhook",
            )
        )
        wh_row = webhook_result.fetchone()
        if wh_row and wh_row[0]:
            last_webhook_received = wh_row[0].isoformat()
            last_webhook_order_count = wh_row[1]
    except Exception as exc:
        logger.error(
            "[LeafLinkWebhook] diagnostic_webhook_stats_error brand_id=%s error=%s",
            brand_id,
            exc,
        )

    # Last API sync (from SyncRun)
    last_api_sync: Optional[str] = None
    last_api_sync_count: Optional[int] = None
    try:
        sync_result = await db.execute(
            select(SyncRun).where(
                SyncRun.brand_id == brand_id,
                SyncRun.status == "completed",
            ).order_by(SyncRun.completed_at.desc()).limit(1)
        )
        last_run = sync_result.scalar_one_or_none()
        if last_run:
            last_api_sync = last_run.completed_at.isoformat() if last_run.completed_at else None
            last_api_sync_count = last_run.orders_loaded_this_run
    except Exception as exc:
        logger.error(
            "[LeafLinkWebhook] diagnostic_sync_run_error brand_id=%s error=%s",
            brand_id,
            exc,
        )

    # Last error from credential
    last_error = cred.last_error if cred else None
    last_error_at = cred.last_checked_at.isoformat() if (cred and cred.last_checked_at) else None

    return {
        "ok": True,
        "brand_id": brand_id,
        "org_id": org_id,
        "credential_exists": credential_exists,
        "webhook_key_configured": webhook_key_configured,
        "last_webhook_received": last_webhook_received,
        "last_webhook_order_count": last_webhook_order_count,
        "last_api_sync": last_api_sync,
        "last_api_sync_count": last_api_sync_count,
        "total_orders_in_db": total_orders_in_db,
        "last_error": last_error,
        "last_error_at": last_error_at,
        "timestamp": timestamp,
    }
