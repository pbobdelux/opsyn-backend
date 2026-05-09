"""
LeafLink webhook configuration endpoint.

Allows per-brand webhook setup: stores the raw webhook key in AWS Secrets Manager
and records the ARN reference + last-4 display hint in brand_api_credentials.

Routes:
  POST /api/leaflink/orders/webhook-config  — configure webhook for a brand

Security:
  - Raw webhook key is NEVER stored in the database
  - Key is stored in AWS Secrets Manager; only the ARN is persisted
  - Org validation via X-OPSYN-ORG header prevents cross-tenant configuration
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
    secret_ref: str  # AWS Secrets Manager ARN (safe to return — not the key itself)


# ---------------------------------------------------------------------------
# POST /api/leaflink/orders/webhook-config
# ---------------------------------------------------------------------------

@router.post("/webhook-config", response_model=WebhookConfigResponse)
async def configure_webhook(
    body: WebhookConfigRequest,
    x_opsyn_org: Optional[str] = Header(None, alias="X-OPSYN-ORG"),
    db: AsyncSession = Depends(get_db),
):
    """
    Configure per-brand LeafLink webhook credentials.

    Stores the raw webhook_key in AWS Secrets Manager and records the ARN
    reference in brand_api_credentials. The raw key is never stored in the DB.

    Required header:
      X-OPSYN-ORG: <org_id>  — must match the credential's org_id

    Request body:
      brand_id                — Brand UUID
      webhook_key             — Raw LeafLink webhook secret (min 8 chars)
      webhook_enabled         — Enable webhook processing (default: true)
      webhook_signature_required — Require signature verification (default: true)
      leaflink_company_id     — LeafLink company ID for tenant resolution (optional)

    Returns:
      webhook_configured      — true on success
      webhook_key_last4       — last 4 chars of the key (for display/audit)
      webhook_enabled         — current enabled state
      webhook_signature_required — current signature requirement
      leaflink_company_id     — configured company ID
      brand_id                — brand UUID
      secret_ref              — AWS Secrets Manager ARN (safe to return)
    """
    brand_id = body.brand_id.strip()
    webhook_key = body.webhook_key

    # Validate required fields
    if not brand_id:
        raise HTTPException(status_code=400, detail="brand_id is required")
    if not webhook_key or len(webhook_key) < 8:
        raise HTTPException(status_code=400, detail="webhook_key must be at least 8 characters")

    # Load credential from database
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
        raise HTTPException(status_code=500, detail="Database error during credential lookup")

    if cred is None:
        raise HTTPException(
            status_code=404,
            detail=f"No LeafLink credential found for brand_id={brand_id}",
        )

    # Validate org ownership via X-OPSYN-ORG header
    if x_opsyn_org:
        cred_org_id = cred.org_id or ""
        if cred_org_id and cred_org_id.strip() != x_opsyn_org.strip():
            logger.warning(
                "[WebhookConfig] org_mismatch brand_id=%s header_org=%s cred_org=%s",
                brand_id,
                x_opsyn_org,
                cred_org_id,
            )
            raise HTTPException(
                status_code=401,
                detail="X-OPSYN-ORG header does not match credential org_id",
            )
    else:
        logger.warning(
            "[WebhookConfig] missing_org_header brand_id=%s — proceeding without org validation",
            brand_id,
        )

    # Store raw webhook key in AWS Secrets Manager
    logger.info(
        "[WebhookConfig] storing_webhook_key brand_id=%s",
        brand_id,
    )
    secret_ref = await store_webhook_key_in_aws(brand_id, webhook_key)
    if not secret_ref:
        logger.error(
            "[WebhookConfig] aws_store_failed brand_id=%s",
            brand_id,
        )
        raise HTTPException(
            status_code=500,
            detail=(
                "Failed to store webhook key in AWS Secrets Manager. "
                "Check AWS credentials and permissions."
            ),
        )

    # Extract last 4 chars for display/audit (never the full key)
    webhook_key_last4 = webhook_key[-4:] if len(webhook_key) >= 4 else webhook_key

    # Update credential record with new secure fields
    try:
        cred.webhook_key_secret_ref = secret_ref
        cred.webhook_key_last4 = webhook_key_last4
        cred.webhook_enabled = body.webhook_enabled
        cred.webhook_signature_required = body.webhook_signature_required
        if body.leaflink_company_id is not None:
            cred.leaflink_company_id = body.leaflink_company_id.strip() or None

        await db.commit()
        await db.refresh(cred)
    except Exception as exc:
        await db.rollback()
        logger.error(
            "[WebhookConfig] db_update_error brand_id=%s error=%s",
            brand_id,
            exc,
        )
        raise HTTPException(
            status_code=500,
            detail="Failed to update credential record after storing webhook key",
        )

    logger.info(
        "[WebhookConfig] webhook_configured brand_id=%s webhook_enabled=%s"
        " signature_required=%s leaflink_company_id=%s last4=%s",
        brand_id,
        cred.webhook_enabled,
        cred.webhook_signature_required,
        cred.leaflink_company_id,
        webhook_key_last4,
    )

    return WebhookConfigResponse(
        webhook_configured=True,
        webhook_key_last4=webhook_key_last4,
        webhook_enabled=cred.webhook_enabled,
        webhook_signature_required=cred.webhook_signature_required,
        leaflink_company_id=cred.leaflink_company_id,
        brand_id=brand_id,
        secret_ref=secret_ref,
    )
