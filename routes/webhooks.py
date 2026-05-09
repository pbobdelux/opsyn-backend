"""
Webhook endpoints for inbound LeafLink payloads.

Routes:
  POST /webhooks/leaflink              — receive LeafLink webhook events
  GET  /webhooks/leaflink/setup-info   — webhook configuration helper
  GET  /webhooks/leaflink/diagnostic   — webhook + sync diagnostics

Security model:
  - Tenant resolution uses multi-source strategy (no global env-var secrets)
  - Webhook keys are loaded per-brand from AWS Secrets Manager with plaintext fallback
  - Unresolved events are stored with tenant_resolution_status='unresolved' and return 202
  - Ambiguous events are stored with tenant_resolution_status='ambiguous' and return 409
  - Idempotency is tenant-scoped: (brand_id, idempotency_key)

Log markers:
  [WEBHOOK_LATENCY_MS]    — full processing time in milliseconds
  [PAYLOAD_SANITIZED]     — payload was truncated/sanitized
  [WEBHOOK_JOB_DEDUPED]   — duplicate webhook job skipped
"""

import json
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, Header, Query, Request
from fastapi.responses import JSONResponse
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import BrandAPICredential, LeafLinkWebhookEvent, Order, SyncRun
from services.leaflink_webhook import process_leaflink_webhook, verify_ll_signature
from services.webhook_tenant_resolver import resolve_tenant_multi_source
from services.aws_secrets_manager import get_webhook_key_for_brand

logger = logging.getLogger("webhooks")

router = APIRouter(prefix="/webhooks", tags=["webhooks"])

# Production webhook URL — used in setup-info response
_WEBHOOK_BASE_URL = "https://opsyn-backend-production.up.railway.app"

# Payload size limits
_MAX_PAYLOAD_BYTES = 1_000_000       # 1MB hard cap
_MAX_STRING_FIELD_BYTES = 100_000    # 100KB per string field
_MAX_ARRAY_ITEMS = 10_000            # max items per array
_MAX_RECURSION_DEPTH = 10            # max nesting depth


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sanitize_webhook_payload(
    payload: dict,
    max_size_bytes: int = _MAX_PAYLOAD_BYTES,
) -> tuple:
    """
    Sanitize a webhook payload to prevent unbounded memory/storage usage.

    Caps:
      - Total payload size: max_size_bytes (default 1MB)
      - String field sizes: _MAX_STRING_FIELD_BYTES (100KB)
      - Array sizes: _MAX_ARRAY_ITEMS (10,000 items)
      - Recursion depth: _MAX_RECURSION_DEPTH (10 levels)

    Returns:
        (sanitized_payload, was_truncated)
    """
    was_truncated = [False]  # Use list for nonlocal mutation in nested function

    def _sanitize_value(value: Any, depth: int = 0) -> Any:
        if depth > _MAX_RECURSION_DEPTH:
            was_truncated[0] = True
            return "[DEPTH_LIMIT_EXCEEDED]"

        if isinstance(value, str):
            encoded = value.encode("utf-8", errors="replace")
            if len(encoded) > _MAX_STRING_FIELD_BYTES:
                was_truncated[0] = True
                return value[: _MAX_STRING_FIELD_BYTES // 4] + "...[TRUNCATED]"
            return value

        if isinstance(value, list):
            if len(value) > _MAX_ARRAY_ITEMS:
                was_truncated[0] = True
                value = value[:_MAX_ARRAY_ITEMS]
            return [_sanitize_value(item, depth + 1) for item in value]

        if isinstance(value, dict):
            return {k: _sanitize_value(v, depth + 1) for k, v in value.items()}

        return value

    sanitized = _sanitize_value(payload)

    # Final size check — if still too large, store truncation marker
    try:
        serialized = json.dumps(sanitized).encode("utf-8")
        original_size = len(serialized)
        if original_size > max_size_bytes:
            was_truncated[0] = True
            sanitized = {
                "_truncated": True,
                "_original_size_bytes": original_size,
                "event": payload.get("event"),
                "company_id": payload.get("company_id"),
            }
    except Exception:
        pass

    return sanitized, was_truncated[0]


async def _persist_webhook_event(
    db: AsyncSession,
    event_id: str,
    brand_id: Optional[str],
    org_id: Optional[str],
    event_type: Optional[str],
    idempotency_key: Optional[str],
    raw_payload: Optional[dict],
    tenant_resolution_status: str,
    signature_verification_status: Optional[str] = None,
    signature_verification_error: Optional[str] = None,
    processing_error: Optional[str] = None,
) -> None:
    """Persist a webhook event to the audit log. Never raises."""
    try:
        event = LeafLinkWebhookEvent(
            id=event_id,
            brand_id=brand_id,
            org_id=org_id,
            event_type=event_type,
            idempotency_key=idempotency_key,
            raw_payload=raw_payload,
            tenant_resolution_status=tenant_resolution_status,
            signature_verification_status=signature_verification_status,
            signature_verification_error=signature_verification_error,
            processing_error=processing_error[:1000] if processing_error else None,
        )
        db.add(event)
        await db.commit()
    except Exception as exc:
        logger.error(
            "[LeafLinkWebhook] persist_event_failed event_id=%s error=%s",
            event_id,
            str(exc)[:200],
        )
        try:
            await db.rollback()
        except Exception:
            pass


def _log_latency(start: float, event_id: str, outcome: str) -> None:
    """Log webhook processing latency with structured markers.

    Markers:
      [WEBHOOK_LATENCY_MS]      -- always logged (INFO)
      [WEBHOOK_LATENCY_WARNING] -- logged when latency >500ms (WARNING)
      [WEBHOOK_LATENCY_ERROR]   -- logged when latency >2000ms (ERROR)
    """
    elapsed_ms = round((time.monotonic() - start) * 1000)
    logger.info(
        "[WEBHOOK_LATENCY_MS] event_id=%s outcome=%s latency_ms=%d target_ms=1000",
        event_id,
        outcome,
        elapsed_ms,
    )
    if elapsed_ms > 2000:
        logger.error(
            "[WEBHOOK_LATENCY_ERROR] event_id=%s latency_ms=%d "
            "threshold_ms=2000 action=investigate_blocking_operations",
            event_id,
            elapsed_ms,
        )
    elif elapsed_ms > 500:
        logger.warning(
            "[WEBHOOK_LATENCY_WARNING] event_id=%s latency_ms=%d "
            "threshold_ms=500 target_ms=1000",
            event_id,
            elapsed_ms,
        )


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

    Multi-source tenant resolution: tries leaflink_company_id, company_id,
    seller_id, and X-LEAFLINK-BRAND-ID header in order.

    Webhook key fallback chain: AWS Secrets Manager → plaintext webhook_key → None.

    Unresolved events return 202 and are stored for later replay.
    Always returns 200 for resolved events — LeafLink requires it.
    """
    _start = time.monotonic()
    event_id = str(uuid.uuid4())

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

    # Sanitize payload before any processing or storage
    sanitized_payload, was_sanitized = _sanitize_webhook_payload(payload)
    if was_sanitized:
        logger.warning(
            "[PAYLOAD_SANITIZED] event_id=%s original_size_bytes=%d",
            event_id,
            len(payload_bytes),
        )

    event_type = str(payload.get("event") or "unknown")
    idempotency_key = str(payload.get("idempotency_key") or event_id)

    # Build headers dict for tenant resolver
    headers = dict(request.headers)
    if x_leaflink_brand_id:
        headers["X-LEAFLINK-BRAND-ID"] = x_leaflink_brand_id

    # ------------------------------------------------------------------ #
    # Multi-source tenant resolution                                       #
    # ------------------------------------------------------------------ #
    tenant = await resolve_tenant_multi_source(
        payload=payload,
        headers=headers,
        db=db,
    )

    if not tenant["ok"]:
        status_code = tenant.get("status_code", 202)

        if status_code == 409:
            # Ambiguous — store event and return 409
            await _persist_webhook_event(
                db=db,
                event_id=event_id,
                brand_id=None,
                org_id=None,
                event_type=event_type,
                idempotency_key=idempotency_key,
                raw_payload=sanitized_payload,
                tenant_resolution_status="ambiguous",
                processing_error=tenant.get("error"),
            )
            _log_latency(_start, event_id, "ambiguous")
            return JSONResponse(
                status_code=409,
                content={
                    "ok": False,
                    "event_id": event_id,
                    "error": tenant.get("error"),
                    "status": "ambiguous",
                    "all_matching_brand_ids": tenant.get("all_matching_brand_ids", []),
                },
            )

        # Unresolved — store event and return 202
        await _persist_webhook_event(
            db=db,
            event_id=event_id,
            brand_id=None,
            org_id=None,
            event_type=event_type,
            idempotency_key=idempotency_key,
            raw_payload=sanitized_payload,
            tenant_resolution_status="unresolved",
            processing_error=tenant.get("error"),
        )
        _log_latency(_start, event_id, "unresolved")
        return JSONResponse(
            status_code=202,
            content={
                "ok": False,
                "event_id": event_id,
                "status": "unresolved",
                "message": "Tenant could not be resolved. Event stored for later replay.",
            },
        )

    brand_id: str = tenant["brand_id"]
    org_id: Optional[str] = tenant["org_id"]
    cred: BrandAPICredential = tenant["credential"]
    resolution_method: str = tenant["resolution_method"]

    # ------------------------------------------------------------------ #
    # Signature verification with AWS fallback chain                      #
    # ------------------------------------------------------------------ #
    secret_ref = getattr(cred, "webhook_key_secret_ref", None)
    plaintext_key = cred.webhook_key
    sig_required = getattr(cred, "webhook_signature_required", True)

    sig_status: Optional[str] = None
    sig_error: Optional[str] = None

    webhook_key: Optional[str] = None
    try:
        if secret_ref:
            # Load webhook key from unified AWS Secrets Manager service
            try:
                from services.integration_secrets import get_integration_secret
                webhook_key = await get_integration_secret(secret_ref)
                logger.info(
                    "[LEAFLINK_WEBHOOK] loaded_webhook_key_from_aws secret_ref=%s brand_id=%s",
                    secret_ref,
                    brand_id,
                )
            except Exception as _aws_exc:
                logger.warning(
                    "[LEAFLINK_WEBHOOK] aws_webhook_key_load_failed brand_id=%s "
                    "secret_ref=%s error=%s — falling back to legacy resolver",
                    brand_id,
                    secret_ref,
                    str(_aws_exc)[:200],
                )
                # Fall back to legacy resolver (handles plaintext fallback)
                webhook_key = await get_webhook_key_for_brand(brand_id, secret_ref, plaintext_key)
        else:
            # No secret ref — use legacy resolver (plaintext fallback chain)
            webhook_key = await get_webhook_key_for_brand(brand_id, None, plaintext_key)
    except Exception as exc:
        logger.error(
            "[LeafLinkWebhook] webhook_key_resolution_error brand_id=%s error=%s",
            brand_id,
            exc,
        )
        webhook_key = plaintext_key  # Final fallback — never crash

    if webhook_key:
        if not ll_signature:
            if sig_required:
                sig_status = "missing"
                sig_error = "LL-Signature header is required but missing"
                logger.warning(
                    "[LeafLinkWebhook] missing_signature brand_id=%s",
                    brand_id,
                )
                await _persist_webhook_event(
                    db=db,
                    event_id=event_id,
                    brand_id=brand_id,
                    org_id=org_id,
                    event_type=event_type,
                    idempotency_key=idempotency_key,
                    raw_payload=sanitized_payload,
                    tenant_resolution_status="resolved",
                    signature_verification_status=sig_status,
                    signature_verification_error=sig_error,
                )
                _log_latency(_start, event_id, "missing_signature")
                return JSONResponse(
                    status_code=200,
                    content={
                        "ok": False,
                        "event_id": event_id,
                        "received_count": 0,
                        "upserted_count": 0,
                        "errors": ["Missing LL-Signature header"],
                    },
                )
            else:
                sig_status = "skipped"
        else:
            valid = verify_ll_signature(payload_bytes, ll_signature, webhook_key)
            if valid:
                sig_status = "verified"
                logger.info("[LeafLinkWebhook] signature_verified brand_id=%s", brand_id)
            else:
                sig_status = "invalid"
                sig_error = "HMAC-SHA256 signature mismatch"
                logger.warning("[LeafLinkWebhook] invalid_signature brand_id=%s", brand_id)
                await _persist_webhook_event(
                    db=db,
                    event_id=event_id,
                    brand_id=brand_id,
                    org_id=org_id,
                    event_type=event_type,
                    idempotency_key=idempotency_key,
                    raw_payload=sanitized_payload,
                    tenant_resolution_status="resolved",
                    signature_verification_status=sig_status,
                    signature_verification_error=sig_error,
                )
                _log_latency(_start, event_id, "invalid_signature")
                return JSONResponse(
                    status_code=200,
                    content={
                        "ok": False,
                        "event_id": event_id,
                        "received_count": 0,
                        "upserted_count": 0,
                        "errors": ["Invalid signature"],
                    },
                )
    else:
        # No webhook key configured — skip signature check
        sig_status = "skipped"
        logger.warning(
            "[LeafLinkWebhook] signature_check_skipped brand_id=%s reason=no_webhook_key_configured",
            brand_id,
        )

    # ------------------------------------------------------------------ #
    # Process the webhook (order upserts)                                 #
    # ------------------------------------------------------------------ #
    result = await process_leaflink_webhook(
        payload=sanitized_payload,
        payload_bytes=payload_bytes,
        signature_header=ll_signature,
        db=db,
        _resolved_brand_id=brand_id,
        _resolved_org_id=org_id,
        _resolved_cred=cred,
        _skip_signature_check=True,  # Already verified above
    )

    # Persist event audit record (errors don't affect response)
    try:
        await _persist_webhook_event(
            db=db,
            event_id=event_id,
            brand_id=brand_id,
            org_id=org_id,
            event_type=event_type,
            idempotency_key=idempotency_key,
            raw_payload=sanitized_payload,
            tenant_resolution_status="resolved",
            signature_verification_status=sig_status,
            signature_verification_error=sig_error,
            processing_error=result.get("errors", [None])[0] if result.get("errors") else None,
        )
    except Exception as exc:
        logger.warning(
            "[LeafLinkWebhook] event_audit_persist_failed event_id=%s error=%s",
            event_id,
            str(exc)[:200],
        )

    _log_latency(_start, event_id, "resolved")

    # Always return 200 — LeafLink requires it
    return JSONResponse(
        status_code=200,
        content={
            "ok": result["ok"],
            "event_id": event_id,
            "received_count": result["received_count"],
            "upserted_count": result["upserted_count"],
            "errors": result["errors"],
            "resolution_method": resolution_method,
            **({("error"): result["error"]} if result.get("error") else {}),
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

    # Check both new and legacy webhook key fields
    secret_ref = getattr(cred, "webhook_key_secret_ref", None)
    webhook_key_configured = bool(secret_ref or cred.webhook_key)

    return {
        "ok": True,
        "webhook_url": webhook_url,
        "events": ["New & Changed Orders", "New & Changed Products"],
        "company_id": cred.company_id,
        "brand_id": brand_id,
        "org_id": cred.org_id,
        "webhook_key_configured": webhook_key_configured,
        "uses_aws_secrets_manager": bool(secret_ref),
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
    secret_ref = getattr(cred, "webhook_key_secret_ref", None) if cred else None
    webhook_key_configured = bool(cred and (secret_ref or cred.webhook_key))
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
        "uses_aws_secrets_manager": bool(secret_ref),
        "last_webhook_received": last_webhook_received,
        "last_webhook_order_count": last_webhook_order_count,
        "last_api_sync": last_api_sync,
        "last_api_sync_count": last_api_sync_count,
        "total_orders_in_db": total_orders_in_db,
        "last_error": last_error,
        "last_error_at": last_error_at,
        "timestamp": timestamp,
    }
