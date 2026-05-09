"""
Webhook endpoints for inbound LeafLink payloads.

Routes:
  POST /webhooks/leaflink          — receive LeafLink webhook events
  GET  /webhooks/leaflink/setup-info   — webhook configuration helper
  GET  /webhooks/leaflink/diagnostic   — webhook + sync diagnostics

Security model:
  - Tenant resolution uses multi-source strategy (no global env-var secrets)
  - Webhook keys are loaded per-brand from AWS Secrets Manager
  - Unresolved events are stored with tenant_resolution_status='unresolved'
  - Ambiguous events are stored with tenant_resolution_status='ambiguous' and return 409
  - Idempotency is tenant-scoped: (brand_id, idempotency_key)
"""

import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Header, Query, Request
from fastapi.responses import JSONResponse
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import BrandAPICredential, LeafLinkWebhookEvent, Order, SyncRun
from services.leaflink_webhook import process_leaflink_webhook, verify_ll_signature
from services.webhook_tenant_resolver import resolve_tenant_multi_source

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
    x_leaflink_brand_id: Optional[str] = Header(None, alias="X-LEAFLINK-BRAND-ID"),
    db: AsyncSession = Depends(get_db),
):
    """
    Receive inbound LeafLink webhook payloads.

    Supported events:
    - New & Changed Orders
    - New & Changed Products

    Multi-tenant security model:
    - Tenant is resolved using multiple strategies (company_id, seller_id, header)
    - Webhook key is loaded per-brand from AWS Secrets Manager (not a global env var)
    - Unresolved events are stored with tenant_resolution_status='unresolved', returns 202
    - Ambiguous events are stored with tenant_resolution_status='ambiguous', returns 409
    - Idempotency is tenant-scoped: (brand_id, idempotency_key)
    - Signature verification uses the brand's own webhook_key_secret_ref
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

    # Build headers dict for tenant resolver
    headers_dict = dict(request.headers)
    if x_leaflink_brand_id:
        headers_dict["x-leaflink-brand-id"] = x_leaflink_brand_id

    event_type = str(payload.get("event") or "unknown")
    # Build a tenant-scoped idempotency key from event metadata
    raw_idempotency = str(
        payload.get("idempotency_key")
        or payload.get("event_id")
        or payload.get("id")
        or ""
    )

    # ------------------------------------------------------------------ #
    # Step 1: Resolve tenant using multi-source strategy                  #
    # ------------------------------------------------------------------ #
    tenant = await resolve_tenant_multi_source(payload, headers_dict, db)

    if not tenant["ok"]:
        status_code = tenant["status_code"]
        resolution_status = "ambiguous" if status_code == 409 else "unresolved"

        # Store the unresolved/ambiguous event for audit and later retry
        event_record = LeafLinkWebhookEvent(
            id=str(uuid.uuid4()),
            brand_id=None,
            org_id=None,
            event_type=event_type,
            idempotency_key=raw_idempotency or None,
            raw_payload=payload,
            tenant_resolution_status=resolution_status,
            signature_verification_status=None,
            processing_error=tenant.get("error"),
        )
        try:
            db.add(event_record)
            await db.commit()
        except Exception as store_exc:
            logger.error(
                "[LeafLinkWebhook] failed_to_store_%s_event error=%s",
                resolution_status,
                store_exc,
            )
            await db.rollback()

        if status_code == 409:
            # Ambiguous — multiple brands match
            logger.error(
                "[LeafLinkWebhook] ambiguous_tenant event_type=%s matching_brands=%s",
                event_type,
                tenant.get("all_matching_brand_ids", []),
            )
            return JSONResponse(
                status_code=409,
                content={
                    "ok": False,
                    "error": tenant["error"],
                    "tenant_resolution_status": "ambiguous",
                    "matching_brand_ids": tenant.get("all_matching_brand_ids", []),
                },
            )
        else:
            # Unresolved — store and return 202 Accepted
            logger.warning(
                "[LeafLinkWebhook] tenant_unresolved event_type=%s resolution_method=%s"
                " payload_keys=%s",
                event_type,
                tenant.get("resolution_method"),
                list(payload.keys())[:10],
            )
            return JSONResponse(
                status_code=202,
                content={
                    "ok": False,
                    "error": tenant["error"],
                    "tenant_resolution_status": "unresolved",
                    "message": "Event stored for later resolution",
                },
            )

    brand_id: str = tenant["brand_id"]
    org_id: Optional[str] = tenant["org_id"]
    cred: BrandAPICredential = tenant["credential"]
    resolution_method: str = tenant["resolution_method"]

    logger.info(
        "[LeafLinkWebhook] tenant_resolved brand_id=%s org_id=%s method=%s",
        brand_id,
        org_id,
        resolution_method,
    )

    # ------------------------------------------------------------------ #
    # Step 2: Tenant-scoped idempotency check                             #
    # ------------------------------------------------------------------ #
    # Idempotency key is scoped to brand: "{brand_id}:{raw_idempotency_key}"
    tenant_idempotency_key = f"{brand_id}:{raw_idempotency}" if raw_idempotency else None

    if tenant_idempotency_key:
        try:
            existing_event = await db.execute(
                select(LeafLinkWebhookEvent).where(
                    LeafLinkWebhookEvent.brand_id == brand_id,
                    LeafLinkWebhookEvent.idempotency_key == tenant_idempotency_key,
                    LeafLinkWebhookEvent.tenant_resolution_status == "resolved",
                )
            )
            duplicate = existing_event.scalar_one_or_none()
            if duplicate:
                logger.info(
                    "[LeafLinkWebhook] duplicate_event brand_id=%s idempotency_key=%s original_id=%s",
                    brand_id,
                    tenant_idempotency_key,
                    duplicate.id,
                )
                # Store duplicate record for audit
                dup_record = LeafLinkWebhookEvent(
                    id=str(uuid.uuid4()),
                    brand_id=brand_id,
                    org_id=org_id,
                    event_type=event_type,
                    idempotency_key=None,  # No unique key to avoid constraint conflict
                    raw_payload=payload,
                    tenant_resolution_status="resolved",
                    signature_verification_status="skipped",
                    duplicate_of_event_id=duplicate.id,
                    processing_error="Duplicate event — idempotency key already processed",
                )
                try:
                    db.add(dup_record)
                    await db.commit()
                except Exception:
                    await db.rollback()
                return JSONResponse(
                    status_code=200,
                    content={
                        "ok": True,
                        "received_count": 0,
                        "upserted_count": 0,
                        "errors": [],
                        "duplicate": True,
                        "original_event_id": duplicate.id,
                    },
                )
        except Exception as idem_exc:
            logger.warning(
                "[LeafLinkWebhook] idempotency_check_error brand_id=%s error=%s",
                brand_id,
                idem_exc,
            )

    # ------------------------------------------------------------------ #
    # Step 3: Load webhook key from AWS Secrets Manager (per-brand)       #
    # ------------------------------------------------------------------ #
    webhook_key = await cred.get_webhook_key_from_aws()

    # ------------------------------------------------------------------ #
    # Step 4: Verify LL-Signature using per-brand webhook key             #
    # ------------------------------------------------------------------ #
    sig_status: str
    sig_error: Optional[str] = None

    if webhook_key:
        if not ll_signature:
            sig_status = "missing"
            sig_error = "LL-Signature header is absent"
            if cred.webhook_signature_required:
                logger.warning(
                    "[LeafLinkWebhook] missing_signature brand_id=%s signature_required=true",
                    brand_id,
                )
                # Store event with invalid sig status before returning 401
                _store_event_sync(
                    db, brand_id, org_id, event_type, tenant_idempotency_key,
                    payload, "resolved", sig_status, sig_error,
                )
                return JSONResponse(
                    status_code=401,
                    content={
                        "ok": False,
                        "error": "Missing LL-Signature header",
                        "signature_verification_status": sig_status,
                    },
                )
            else:
                logger.info(
                    "[LeafLinkWebhook] missing_signature_but_not_required brand_id=%s",
                    brand_id,
                )
        elif not verify_ll_signature(payload_bytes, ll_signature, webhook_key):
            sig_status = "invalid"
            sig_error = "HMAC-SHA256 signature mismatch"
            if cred.webhook_signature_required:
                logger.warning(
                    "[LeafLinkWebhook] invalid_signature brand_id=%s",
                    brand_id,
                )
                _store_event_sync(
                    db, brand_id, org_id, event_type, tenant_idempotency_key,
                    payload, "resolved", sig_status, sig_error,
                )
                return JSONResponse(
                    status_code=401,
                    content={
                        "ok": False,
                        "error": "Invalid LL-Signature",
                        "signature_verification_status": sig_status,
                    },
                )
            else:
                logger.warning(
                    "[LeafLinkWebhook] invalid_signature_but_not_required brand_id=%s",
                    brand_id,
                )
        else:
            sig_status = "verified"
            logger.info("[LeafLinkWebhook] signature_verified brand_id=%s", brand_id)
    else:
        # No webhook key configured — skip signature check
        sig_status = "skipped"
        logger.warning(
            "[LeafLinkWebhook] signature_check_skipped brand_id=%s reason=no_webhook_key",
            brand_id,
        )

    # ------------------------------------------------------------------ #
    # Step 5: Process the webhook payload (upsert orders)                 #
    # ------------------------------------------------------------------ #
    result = await process_leaflink_webhook(
        payload=payload,
        payload_bytes=payload_bytes,
        signature_header=ll_signature,
        db=db,
        # Pass pre-resolved tenant to avoid double lookup
        _resolved_brand_id=brand_id,
        _resolved_org_id=org_id,
        _resolved_cred=cred,
        _skip_signature_check=True,  # Already verified above
    )

    # ------------------------------------------------------------------ #
    # Step 6: Store resolved event record for audit/idempotency           #
    # ------------------------------------------------------------------ #
    event_record = LeafLinkWebhookEvent(
        id=str(uuid.uuid4()),
        brand_id=brand_id,
        org_id=org_id,
        event_type=event_type,
        idempotency_key=tenant_idempotency_key,
        raw_payload=payload,
        tenant_resolution_status="resolved",
        signature_verification_status=sig_status,
        signature_verification_error=sig_error,
        processed_at=datetime.now(timezone.utc),
    )
    try:
        db.add(event_record)
        await db.commit()
    except Exception as store_exc:
        logger.warning(
            "[LeafLinkWebhook] failed_to_store_resolved_event brand_id=%s error=%s",
            brand_id,
            store_exc,
        )
        await db.rollback()

    return JSONResponse(
        status_code=200,
        content={
            "ok": result["ok"],
            "received_count": result["received_count"],
            "upserted_count": result["upserted_count"],
            "errors": result["errors"],
            "tenant_resolution_status": "resolved",
            "signature_verification_status": sig_status,
            "resolution_method": resolution_method,
            **({
                "error": result["error"]
            } if result.get("error") else {}),
        },
    )


def _store_event_sync(
    db: AsyncSession,
    brand_id: str,
    org_id: Optional[str],
    event_type: str,
    idempotency_key: Optional[str],
    payload: dict,
    resolution_status: str,
    sig_status: str,
    sig_error: Optional[str],
) -> None:
    """
    Fire-and-forget helper to add a webhook event record to the session.
    The caller is responsible for committing or rolling back.
    """
    try:
        event_record = LeafLinkWebhookEvent(
            id=str(uuid.uuid4()),
            brand_id=brand_id,
            org_id=org_id,
            event_type=event_type,
            idempotency_key=idempotency_key,
            raw_payload=payload,
            tenant_resolution_status=resolution_status,
            signature_verification_status=sig_status,
            signature_verification_error=sig_error,
        )
        db.add(event_record)
    except Exception as exc:
        logger.error("[LeafLinkWebhook] _store_event_sync_error error=%s", exc)


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
        # Secure webhook configuration status
        "webhook_enabled": getattr(cred, "webhook_enabled", False),
        "webhook_signature_required": getattr(cred, "webhook_signature_required", True),
        "webhook_key_configured": bool(
            getattr(cred, "webhook_key_secret_ref", None) or cred.webhook_key
        ),
        "webhook_key_secure": bool(getattr(cred, "webhook_key_secret_ref", None)),
        "webhook_key_last4": getattr(cred, "webhook_key_last4", None),
        "leaflink_company_id": getattr(cred, "leaflink_company_id", None),
        "config_endpoint": f"POST /api/leaflink/orders/webhook-config",
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
    webhook_key_configured = bool(
        cred and (
            getattr(cred, "webhook_key_secret_ref", None) or cred.webhook_key
        )
    )
    webhook_key_secure = bool(cred and getattr(cred, "webhook_key_secret_ref", None))
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
        "webhook_key_secure": webhook_key_secure,
        "webhook_enabled": getattr(cred, "webhook_enabled", False) if cred else False,
        "webhook_signature_required": getattr(cred, "webhook_signature_required", True) if cred else True,
        "webhook_key_last4": getattr(cred, "webhook_key_last4", None) if cred else None,
        "leaflink_company_id": getattr(cred, "leaflink_company_id", None) if cred else None,
        "last_webhook_received": last_webhook_received,
        "last_webhook_order_count": last_webhook_order_count,
        "last_api_sync": last_api_sync,
        "last_api_sync_count": last_api_sync_count,
        "total_orders_in_db": total_orders_in_db,
        "last_error": last_error,
        "last_error_at": last_error_at,
        "timestamp": timestamp,
    }
