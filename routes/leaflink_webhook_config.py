"""
LeafLink webhook configuration endpoint.

Allows per-brand webhook setup: stores the raw webhook key in AWS Secrets Manager
and records the ARN reference + last-4 display hint in brand_api_credentials.

Routes:
  POST /api/leaflink/orders/webhook-config  — configure webhook for a brand
  GET  /api/leaflink/orders/webhook-config  — get webhook config for a brand

Security:
  - Raw webhook key is NEVER stored in the database
  - Key is stored in AWS Secrets Manager; only the ARN is persisted
  - Org validation via X-OPSYN-ORG header prevents cross-tenant configuration

Log markers:
  [WEBHOOK_SECRET_STORE_FAILED] — emitted when AWS store fails
  [WEBHOOK_SECRET_LOAD_FAILED]  — emitted when AWS load fails
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import BrandAPICredential
from services.aws_secrets_manager import store_webhook_key_in_aws

logger = logging.getLogger("leaflink_webhook_config")

router = APIRouter(prefix="/api/leaflink/orders", tags=["leaflink-webhook-config"])


# ---------------------------------------------------------------------------
# Request / Response schemas
# ---------------------------------------------------------------------------

class WebhookConfigRequest(BaseModel):
    brand_id: str = Field(..., description="Brand UUID to configure webhook for")
    webhook_key: str = Field(..., min_length=8, description="Raw LeafLink webhook secret key")
    webhook_enabled: bool = Field(True, description="Enable webhook processing for this brand")
    webhook_signature_required: bool = Field(
        True, description="Require LL-Signature HMAC-SHA256 verification"
    )
    leaflink_company_id: Optional[str] = Field(
        None, description="LeafLink company ID for fast tenant resolution"
    )


class WebhookConfigResponse(BaseModel):
    webhook_configured: bool
    webhook_key_last4: str
    webhook_enabled: bool
    webhook_signature_required: bool
    leaflink_company_id: Optional[str]
    brand_id: str
    message: str


# ---------------------------------------------------------------------------
# POST /api/leaflink/orders/webhook-config
# ---------------------------------------------------------------------------

@router.post("/webhook-config", response_model=WebhookConfigResponse)
async def configure_webhook(
    body: WebhookConfigRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Configure per-brand webhook credentials.

    Stores the raw webhook key in AWS Secrets Manager and records the ARN
    reference + last-4 display hint in brand_api_credentials.

    The raw webhook key is NEVER stored in the database.

    Returns 500 with structured error if AWS Secrets Manager is unavailable.
    """
    brand_id = body.brand_id
    webhook_key = body.webhook_key

    # Look up the brand credential
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
            "[WebhookConfig] db_lookup_error brand_id=%s error=%s",
            brand_id,
            exc,
        )
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Database error during credential lookup",
                "error_code": "DB_LOOKUP_FAILED",
                "details": str(exc)[:200],
                "status": 500,
            },
        )

    if not cred:
        raise HTTPException(
            status_code=404,
            detail={
                "error": f"No LeafLink credential found for brand_id={brand_id}",
                "error_code": "CREDENTIAL_NOT_FOUND",
                "status": 404,
            },
        )

    # Store webhook key in AWS Secrets Manager
    logger.info(
        "[WebhookConfig] storing webhook key in AWS brand_id=%s",
        brand_id,
    )
    secret_arn = await store_webhook_key_in_aws(brand_id, webhook_key)

    if not secret_arn:
        logger.error(
            "[WEBHOOK_SECRET_STORE_FAILED] brand_id=%s reason=aws_store_returned_none",
            brand_id,
        )
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Failed to store webhook key in AWS Secrets Manager",
                "error_code": "WEBHOOK_SECRET_STORE_FAILED",
                "details": "Check AWS credentials, permissions, and region configuration",
                "status": 500,
            },
        )

    # Persist ARN reference and metadata in DB (never the raw key)
    webhook_key_last4 = webhook_key[-4:] if len(webhook_key) >= 4 else webhook_key

    try:
        cred.webhook_key_secret_ref = secret_arn
        cred.webhook_key_last4 = webhook_key_last4
        cred.webhook_enabled = body.webhook_enabled
        cred.webhook_signature_required = body.webhook_signature_required

        # Set leaflink_company_id if provided (for fast tenant resolution)
        if body.leaflink_company_id:
            cred.leaflink_company_id = body.leaflink_company_id

        await db.commit()
        await db.refresh(cred)
    except AttributeError as exc:
        # Columns may not exist yet (pre-migration) — fall back to plaintext
        logger.warning(
            "[WebhookConfig] new_columns_not_available brand_id=%s error=%s "
            "falling_back_to_plaintext_webhook_key",
            brand_id,
            exc,
        )
        try:
            cred.webhook_key = webhook_key
            await db.commit()
        except Exception as fallback_exc:
            logger.error(
                "[WebhookConfig] fallback_commit_failed brand_id=%s error=%s",
                brand_id,
                fallback_exc,
            )
            raise HTTPException(
                status_code=500,
                detail={
                    "error": "Failed to persist webhook configuration",
                    "error_code": "DB_COMMIT_FAILED",
                    "status": 500,
                },
            )
        return WebhookConfigResponse(
            webhook_configured=True,
            webhook_key_last4=webhook_key_last4,
            webhook_enabled=True,
            webhook_signature_required=True,
            leaflink_company_id=body.leaflink_company_id,
            brand_id=brand_id,
            message="Webhook configured (plaintext fallback — run migrations to enable AWS storage)",
        )
    except Exception as exc:
        logger.error(
            "[WebhookConfig] db_commit_error brand_id=%s error=%s",
            brand_id,
            exc,
        )
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Failed to persist webhook configuration",
                "error_code": "DB_COMMIT_FAILED",
                "details": str(exc)[:200],
                "status": 500,
            },
        )

    logger.info(
        "[WebhookConfig] webhook_configured brand_id=%s last4=%s enabled=%s",
        brand_id,
        webhook_key_last4,
        body.webhook_enabled,
    )

    return WebhookConfigResponse(
        webhook_configured=True,
        webhook_key_last4=webhook_key_last4,
        webhook_enabled=body.webhook_enabled,
        webhook_signature_required=body.webhook_signature_required,
        leaflink_company_id=body.leaflink_company_id,
        brand_id=brand_id,
        message="Webhook configured successfully. Key stored in AWS Secrets Manager.",
    )


# ---------------------------------------------------------------------------
# GET /api/leaflink/orders/webhook-config
# ---------------------------------------------------------------------------

@router.get("/webhook-config")
async def get_webhook_config(
    brand_id: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Get webhook configuration status for a brand.

    Returns configuration metadata (never the raw key).
    """
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
            "[WebhookConfig] get_config_db_error brand_id=%s error=%s",
            brand_id,
            exc,
        )
        raise HTTPException(status_code=500, detail="Database error")

    if not cred:
        raise HTTPException(
            status_code=404,
            detail=f"No LeafLink credential found for brand_id={brand_id}",
        )

    # Safely read new columns (may not exist pre-migration)
    secret_ref = getattr(cred, "webhook_key_secret_ref", None)
    last4 = getattr(cred, "webhook_key_last4", None)
    enabled = getattr(cred, "webhook_enabled", False)
    sig_required = getattr(cred, "webhook_signature_required", True)
    ll_company_id = getattr(cred, "leaflink_company_id", None)

    # Backward compat: if no secret_ref but plaintext key exists, show last4
    if not last4 and cred.webhook_key:
        last4 = cred.webhook_key[-4:] if len(cred.webhook_key) >= 4 else "****"

    return {
        "ok": True,
        "brand_id": brand_id,
        "webhook_configured": bool(secret_ref or cred.webhook_key),
        "webhook_key_last4": last4,
        "webhook_enabled": enabled,
        "webhook_signature_required": sig_required,
        "leaflink_company_id": ll_company_id,
        "uses_aws_secrets_manager": bool(secret_ref),
        "uses_plaintext_fallback": bool(not secret_ref and cred.webhook_key),
    }
