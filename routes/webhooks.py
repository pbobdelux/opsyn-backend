"""
Webhook endpoints for inbound LeafLink payloads.

Routes:
  POST /webhooks/leaflink                          — receive LeafLink webhook events
  POST /api/leaflink/orders/webhook-config         — configure per-brand webhook settings
  GET  /webhooks/leaflink/setup-info               — webhook configuration helper
  GET  /webhooks/leaflink/diagnostic               — webhook + sync diagnostics
"""

import hashlib
import hmac
import logging
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, Header, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import BrandAPICredential, LeafLinkWebhookEvent, Order, SyncRun
from services.leaflink_webhook import process_leaflink_webhook
from services.webhook_credentials import (
    extract_last4,
    get_webhook_key,
    store_webhook_key,
)
from services.webhook_tenant_resolver import resolve_tenant_from_webhook

logger = logging.getLogger("webhooks")

router = APIRouter(tags=["webhooks"])

# Production webhook URL — used in setup-info response
_WEBHOOK_BASE_URL = "https://opsyn-backend-production.up.railway.app"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Signature verification helper
# ---------------------------------------------------------------------------

def _verify_ll_signature(payload_bytes: bytes, signature_header: str, webhook_key: str) -> bool:
    """Verify LL-Signature header using HMAC-SHA256."""
    if not signature_header or not webhook_key:
        return False
    try:
        expected = hmac.new(
            webhook_key.encode("utf-8"),
            payload_bytes,
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(expected, signature_header.lower())
    except Exception as exc:
        logger.error("[WEBHOOK_SIGNATURE_VERIFY] error=%s", exc)
        return False


# ---------------------------------------------------------------------------
# POST /webhooks/leaflink
# ---------------------------------------------------------------------------

@router.post("/webhooks/leaflink")
async def receive_leaflink_webhook(
    request: Request,
    ll_signature: Optional[str] = Header(None, alias="LL-Signature"),
    x_leaflink_company_id: Optional[str] = Header(None, alias="X-LeafLink-Company-Id"),
    db: AsyncSession = Depends(get_db),
):
    """
    Receive inbound LeafLink webhook payloads.

    Multi-tenant flow:
      1. Resolve brand from company_id (header or payload)
      2. Verify LL-Signature using per-brand key from AWS Secrets Manager
      3. Enqueue sync jobs and return appropriate HTTP status

    HTTP responses:
      200 — resolved, signature valid (or not required), jobs enqueued
      202 — unresolved tenant (stored for later reconciliation)
      401 — signature required but missing or invalid
      409 — ambiguous tenant (multiple brands match same company_id)
    """
    # Read raw body for signature verification before JSON parsing
    try:
        payload_bytes = await request.body()
    except Exception as exc:
        logger.error("[LEAFLINK_WEBHOOK] body_read_error error=%s", exc)
        return JSONResponse(
            status_code=200,
            content={"ok": False, "error": "Failed to read request body"},
        )

    # Parse JSON payload
    try:
        payload = await request.json()
    except Exception as exc:
        logger.error("[LEAFLINK_WEBHOOK] json_parse_error error=%s", exc)
        return JSONResponse(
            status_code=200,
            content={"ok": False, "error": "Invalid JSON payload"},
        )

    if not isinstance(payload, dict):
        return JSONResponse(
            status_code=200,
            content={"ok": False, "error": "Payload must be a JSON object"},
        )

    event_type = str(payload.get("event") or "unknown")
    payload_company_id = str(payload.get("company_id") or "").strip() or None
    payload_seller_id = str(payload.get("seller_id") or "").strip() or None

    # ------------------------------------------------------------------
    # Step 1: Resolve tenant
    # ------------------------------------------------------------------
    tenant = await resolve_tenant_from_webhook(
        db=db,
        payload=payload,
        company_id_header=x_leaflink_company_id,
    )

    if tenant["status"] == "unresolved":
        # Store event for later reconciliation
        await _store_webhook_event(
            db=db,
            event_type=event_type,
            payload=payload,
            resolution_status="unresolved",
            resolution_error=tenant["error"],
            payload_company_id=payload_company_id,
            payload_seller_id=payload_seller_id,
        )
        logger.warning(
            "[WEBHOOK_TENANT_UNRESOLVED] event=%s company_id=%s error=%s",
            event_type,
            payload_company_id,
            tenant["error"],
        )
        return JSONResponse(
            status_code=202,
            content={
                "ok": False,
                "status": "unresolved",
                "error": tenant["error"],
            },
        )

    if tenant["status"] == "ambiguous":
        await _store_webhook_event(
            db=db,
            event_type=event_type,
            payload=payload,
            resolution_status="ambiguous",
            resolution_error=tenant["error"],
            payload_company_id=payload_company_id,
            payload_seller_id=payload_seller_id,
        )
        logger.warning(
            "[WEBHOOK_TENANT_AMBIGUOUS] event=%s company_id=%s candidates=%s",
            event_type,
            payload_company_id,
            tenant["candidates"],
        )
        return JSONResponse(
            status_code=409,
            content={
                "ok": False,
                "status": "ambiguous",
                "error": tenant["error"],
                "candidates": tenant["candidates"],
            },
        )

    # Tenant resolved
    brand_id: str = tenant["brand_id"]
    org_id: Optional[str] = tenant["org_id"]
    cred: BrandAPICredential = tenant["credential"]

    # ------------------------------------------------------------------
    # Step 2: Signature verification (per-brand)
    # ------------------------------------------------------------------
    signature_required: bool = getattr(cred, "webhook_signature_required", True)
    signature_valid: Optional[bool] = None
    signature_error: Optional[str] = None

    if signature_required:
        raw_key = await get_webhook_key(brand_id)

        if not raw_key:
            # Key not configured — treat as signature required but unconfigured
            sig_err = "Webhook key not configured in AWS Secrets Manager"
            await _store_webhook_event(
                db=db,
                event_type=event_type,
                payload=payload,
                resolution_status="resolved",
                resolved_brand_id=brand_id,
                resolved_org_id=org_id,
                payload_company_id=payload_company_id,
                payload_seller_id=payload_seller_id,
                signature_valid=False,
                signature_error=sig_err,
            )
            logger.warning(
                "[WEBHOOK_SIGNATURE_INVALID] brand_id=%s reason=key_not_configured",
                brand_id,
            )
            return JSONResponse(
                status_code=401,
                content={
                    "ok": False,
                    "status": "signature_invalid",
                    "error": sig_err,
                },
            )

        if not ll_signature:
            sig_err = "Missing LL-Signature header"
            await _store_webhook_event(
                db=db,
                event_type=event_type,
                payload=payload,
                resolution_status="resolved",
                resolved_brand_id=brand_id,
                resolved_org_id=org_id,
                payload_company_id=payload_company_id,
                payload_seller_id=payload_seller_id,
                signature_valid=False,
                signature_error=sig_err,
            )
            logger.warning(
                "[WEBHOOK_SIGNATURE_INVALID] brand_id=%s reason=missing_header",
                brand_id,
            )
            return JSONResponse(
                status_code=401,
                content={
                    "ok": False,
                    "status": "signature_invalid",
                    "error": sig_err,
                },
            )

        if not _verify_ll_signature(payload_bytes, ll_signature, raw_key):
            sig_err = "LL-Signature verification failed"
            await _store_webhook_event(
                db=db,
                event_type=event_type,
                payload=payload,
                resolution_status="resolved",
                resolved_brand_id=brand_id,
                resolved_org_id=org_id,
                payload_company_id=payload_company_id,
                payload_seller_id=payload_seller_id,
                signature_valid=False,
                signature_error=sig_err,
            )
            logger.warning(
                "[WEBHOOK_SIGNATURE_INVALID] brand_id=%s reason=hmac_mismatch",
                brand_id,
            )
            return JSONResponse(
                status_code=401,
                content={
                    "ok": False,
                    "status": "signature_invalid",
                    "error": sig_err,
                },
            )

        signature_valid = True
        logger.info("[WEBHOOK_SIGNATURE_VERIFIED] brand_id=%s", brand_id)

    else:
        # Signature check skipped per brand config
        signature_valid = None
        logger.info(
            "[WEBHOOK_SIGNATURE_SKIPPED] brand_id=%s reason=signature_not_required",
            brand_id,
        )

    # ------------------------------------------------------------------
    # Step 3: Store event audit record
    # ------------------------------------------------------------------
    await _store_webhook_event(
        db=db,
        event_type=event_type,
        payload=payload,
        resolution_status="resolved",
        resolved_brand_id=brand_id,
        resolved_org_id=org_id,
        payload_company_id=payload_company_id,
        payload_seller_id=payload_seller_id,
        signature_valid=signature_valid,
        signature_error=signature_error,
    )

    # ------------------------------------------------------------------
    # Step 4: Process webhook (enqueue sync jobs / upsert orders)
    # ------------------------------------------------------------------
    result = await process_leaflink_webhook(
        payload=payload,
        payload_bytes=payload_bytes,
        signature_header=ll_signature,
        db=db,
        resolved_brand_id=brand_id,
        resolved_org_id=org_id,
        skip_signature_check=True,  # Already verified above
    )

    logger.info(
        "[LEAFLINK_WEBHOOK_ENQUEUED] brand_id=%s event=%s received=%s upserted=%s",
        brand_id,
        event_type,
        result.get("received_count", 0),
        result.get("upserted_count", 0),
    )

    return JSONResponse(
        status_code=200,
        content={
            "ok": result["ok"],
            "received_count": result["received_count"],
            "upserted_count": result["upserted_count"],
            "errors": result["errors"],
            **({} if not result.get("error") else {"error": result["error"]}),
        },
    )


# ---------------------------------------------------------------------------
# Webhook event storage helper
# ---------------------------------------------------------------------------

async def _store_webhook_event(
    db: AsyncSession,
    event_type: str,
    payload: dict[str, Any],
    resolution_status: str,
    resolution_error: Optional[str] = None,
    resolved_brand_id: Optional[str] = None,
    resolved_org_id: Optional[str] = None,
    payload_company_id: Optional[str] = None,
    payload_seller_id: Optional[str] = None,
    signature_valid: Optional[bool] = None,
    signature_error: Optional[str] = None,
) -> None:
    """Persist a LeafLinkWebhookEvent audit record. Never raises."""
    try:
        from utils.json_utils import make_json_safe

        event = LeafLinkWebhookEvent(
            event_type=event_type,
            raw_payload=make_json_safe(payload),
            resolution_status=resolution_status,
            resolution_error=resolution_error,
            resolved_brand_id=resolved_brand_id,
            resolved_org_id=resolved_org_id,
            brand_id=resolved_brand_id,  # convenience alias
            payload_company_id=payload_company_id,
            payload_seller_id=payload_seller_id,
            signature_valid=signature_valid,
            signature_error=signature_error,
        )
        db.add(event)
        await db.flush()
    except Exception as exc:
        logger.error(
            "[WEBHOOK_EVENT_STORE] failed resolution_status=%s error=%s",
            resolution_status,
            str(exc)[:300],
        )


# ---------------------------------------------------------------------------
# POST /api/leaflink/orders/webhook-config
# ---------------------------------------------------------------------------

class WebhookConfigRequest(BaseModel):
    brand_id: str
    webhook_key: Optional[str] = None
    webhook_enabled: Optional[bool] = None
    webhook_signature_required: Optional[bool] = None
    leaflink_company_id: Optional[str] = None


@router.post("/api/leaflink/orders/webhook-config")
async def configure_webhook(
    body: WebhookConfigRequest,
    x_opsyn_org: Optional[str] = Header(None, alias="X-OPSYN-ORG"),
    db: AsyncSession = Depends(get_db),
):
    """
    Configure per-brand webhook settings.

    Stores the webhook key in AWS Secrets Manager (never in the DB).
    Updates webhook_enabled, webhook_signature_required, and leaflink_company_id
    on the BrandAPICredential row.

    Requires X-OPSYN-ORG header for org_id validation.
    """
    if not x_opsyn_org:
        return JSONResponse(
            status_code=401,
            content={"ok": False, "error": "X-OPSYN-ORG header is required"},
        )

    org_id = x_opsyn_org.strip()
    brand_id = body.brand_id.strip()

    if not brand_id:
        return JSONResponse(
            status_code=400,
            content={"ok": False, "error": "brand_id is required"},
        )

    # Load credential — must belong to this org
    try:
        result = await db.execute(
            select(BrandAPICredential).where(
                BrandAPICredential.brand_id == brand_id,
                BrandAPICredential.org_id == org_id,
                BrandAPICredential.integration_name == "leaflink",
            )
        )
        cred = result.scalar_one_or_none()
    except Exception as exc:
        logger.error(
            "[WEBHOOK_CONFIG] db_error brand_id=%s org_id=%s error=%s",
            brand_id,
            org_id,
            str(exc)[:300],
        )
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": "Database error during credential lookup"},
        )

    if cred is None:
        return JSONResponse(
            status_code=404,
            content={
                "ok": False,
                "error": f"No LeafLink credential found for brand_id={brand_id} org_id={org_id}",
            },
        )

    # Store webhook key in AWS Secrets Manager if provided
    if body.webhook_key:
        try:
            secret_ref = await store_webhook_key(brand_id, body.webhook_key)
            last4 = extract_last4(body.webhook_key)
            cred.webhook_key_secret_ref = secret_ref
            cred.webhook_key_last4 = last4
        except Exception as exc:
            logger.error(
                "[WEBHOOK_CONFIG] key_store_failed brand_id=%s error=%s",
                brand_id,
                str(exc)[:300],
            )
            return JSONResponse(
                status_code=500,
                content={"ok": False, "error": "Failed to store webhook key in AWS Secrets Manager"},
            )

    # Update per-brand flags
    if body.webhook_enabled is not None:
        cred.webhook_enabled = body.webhook_enabled
    if body.webhook_signature_required is not None:
        cred.webhook_signature_required = body.webhook_signature_required
    if body.leaflink_company_id is not None:
        cred.leaflink_company_id = body.leaflink_company_id.strip() or None

    try:
        await db.commit()
        await db.refresh(cred)
    except Exception as exc:
        await db.rollback()
        logger.error(
            "[WEBHOOK_CONFIG] commit_failed brand_id=%s error=%s",
            brand_id,
            str(exc)[:300],
        )
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": "Failed to save webhook configuration"},
        )

    logger.info(
        "[WEBHOOK_CONFIG_UPDATED] brand_id=%s org_id=%s webhook_enabled=%s"
        " signature_required=%s key_updated=%s leaflink_company_id=%s",
        brand_id,
        org_id,
        cred.webhook_enabled,
        cred.webhook_signature_required,
        bool(body.webhook_key),
        cred.leaflink_company_id,
    )

    return {
        "ok": True,
        "brand_id": brand_id,
        "webhook_enabled": cred.webhook_enabled,
        "webhook_signature_required": cred.webhook_signature_required,
        "webhook_key_configured": bool(cred.webhook_key_secret_ref),
        "webhook_key_last4": cred.webhook_key_last4,
        "leaflink_company_id": cred.leaflink_company_id,
    }


# ---------------------------------------------------------------------------
# GET /webhooks/leaflink/setup-info
# ---------------------------------------------------------------------------

@router.get("/webhooks/leaflink/setup-info")
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
        "leaflink_company_id": cred.leaflink_company_id,
        "brand_id": brand_id,
        "org_id": cred.org_id,
        "webhook_enabled": cred.webhook_enabled,
        "webhook_signature_required": cred.webhook_signature_required,
        "webhook_key_configured": bool(cred.webhook_key_secret_ref),
        "webhook_key_last4": cred.webhook_key_last4,
    }


# ---------------------------------------------------------------------------
# GET /webhooks/leaflink/diagnostic
# ---------------------------------------------------------------------------

@router.get("/webhooks/leaflink/diagnostic")
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
    webhook_key_configured = bool(cred and getattr(cred, "webhook_key_secret_ref", None))
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
        "webhook_enabled": cred.webhook_enabled if cred else False,
        "webhook_signature_required": cred.webhook_signature_required if cred else True,
        "webhook_key_last4": cred.webhook_key_last4 if cred else None,
        "leaflink_company_id": cred.leaflink_company_id if cred else None,
        "last_webhook_received": last_webhook_received,
        "last_webhook_order_count": last_webhook_order_count,
        "last_api_sync": last_api_sync,
        "last_api_sync_count": last_api_sync_count,
        "total_orders_in_db": total_orders_in_db,
        "last_error": last_error,
        "last_error_at": last_error_at,
        "timestamp": timestamp,
    }
