"""
Integration management routes.

Provides endpoints to connect, validate, and check the health of third-party
integrations (LeafLink, etc.) on a per-brand basis.

Endpoints:
  GET    /integrations/leaflink/health                          — Brand-aware health check
  POST   /integrations/leaflink/connect                        — Connect/update LeafLink credentials
  POST   /api/integrations/{integration_name}/credentials      — Configure credentials (AWS-backed)
  GET    /api/integrations/{integration_name}/credentials      — Retrieve credential metadata
  DELETE /api/integrations/{integration_name}/credentials      — Delete credentials
"""

import logging
import os
from datetime import datetime, timezone
from typing import Optional

import requests
from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import BrandAPICredential, OrganizationBrandBinding
from services.integration_credentials import (
    validate_leaflink_credential,
    mark_credential_invalid,
    mark_credential_success,
)

logger = logging.getLogger("integrations")

router = APIRouter(prefix="/integrations", tags=["integrations"])

# ---------------------------------------------------------------------------
# Pydantic schemas for the new credentials endpoints
# ---------------------------------------------------------------------------


class CredentialsConfigRequest(BaseModel):
    brand_id: str = Field(..., description="Brand UUID")
    api_key: Optional[str] = Field(None, description="Raw API key to store in AWS Secrets Manager")
    auth_scheme: Optional[str] = Field(None, description="Auth scheme: Token, Bearer, or Raw")
    base_url: Optional[str] = Field(None, description="Integration base URL")
    leaflink_company_id: Optional[str] = Field(None, description="LeafLink company ID")
    webhook_key: Optional[str] = Field(None, description="Raw webhook secret to store in AWS Secrets Manager")
    webhook_enabled: Optional[bool] = Field(None, description="Enable webhook processing")
    webhook_signature_required: Optional[bool] = Field(None, description="Require HMAC-SHA256 signature verification")


# ---------------------------------------------------------------------------
# Helper: resolve org_id from X-OPSYN-ORG header
# ---------------------------------------------------------------------------


async def _resolve_org_id(
    x_opsyn_org: Optional[str],
    x_opsyn_secret: Optional[str],
    db: AsyncSession,
) -> str:
    """Resolve and authenticate org_id from X-OPSYN-ORG header.

    Raises HTTP 401 if the header is missing or credentials are invalid.
    Returns the authenticated org_id string.
    """
    from services.tenant_auth import get_authenticated_org
    from services.organization_service import lookup_organization

    if not x_opsyn_org:
        raise HTTPException(status_code=401, detail="X-OPSYN-ORG header is required")

    # Authenticate the org
    authenticated_org = await get_authenticated_org(
        x_opsyn_org, x_opsyn_secret, x_opsyn_org, db
    )

    # Resolve to UUID via org_code or UUID lookup
    org_result = await lookup_organization(db, authenticated_org)
    if not org_result["ok"] or not org_result["organization"]:
        raise HTTPException(status_code=404, detail="Organization not found")

    return str(org_result["organization"].id)


async def _verify_brand_belongs_to_org(
    db: AsyncSession,
    org_id: str,
    brand_id: str,
) -> None:
    """Verify that brand_id belongs to org_id via organization_brand_bindings.

    Raises HTTP 403 if the brand does not belong to the org.
    """
    result = await db.execute(
        select(OrganizationBrandBinding).where(
            OrganizationBrandBinding.org_id == org_id,
            OrganizationBrandBinding.brand_id == brand_id,
            OrganizationBrandBinding.is_active == True,
        )
    )
    binding = result.scalar_one_or_none()
    if binding is None:
        logger.warning(
            "[IntegrationCredentials] brand_not_in_org org_id=%s brand_id=%s",
            org_id,
            brand_id,
        )
        raise HTTPException(
            status_code=403,
            detail="Brand does not belong to this organization",
        )


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# GET /integrations/leaflink/health
# ---------------------------------------------------------------------------

@router.get("/leaflink/health")
async def leaflink_health(
    brand_id: Optional[str] = Query(None, description="Brand slug to check (e.g. 'noble-nectar')"),
    db: AsyncSession = Depends(get_db),
):
    """
    Return the current health of the LeafLink credential for a specific brand.

    Loads the credential from the database and performs a lightweight live
    test to confirm it is accepted by the LeafLink API.

    Query params:
      brand_id (required): Brand slug to check

    Response:
      {
        "ok": true | false,
        "provider": "leaflink",
        "brand_id": "noble-nectar",
        "credential_source": "db",
        "connected": bool,
        "status": "active" | "invalid" | "missing" | "degraded",
        "last_success_at": "<ISO timestamp | null>",
        "last_error": "<string | null>",
        "can_fetch_orders": bool
      }

    Never exposes secret values.
    """
    if not brand_id:
        return {
            "ok": False,
            "provider": "leaflink",
            "brand_id": None,
            "credential_source": "db",
            "connected": False,
            "status": "missing",
            "last_success_at": None,
            "last_error": "brand_id query parameter is required",
            "can_fetch_orders": False,
        }

    logger.info("[IntegrationCredentials] health_check brand=%s source=db", brand_id)

    try:
        # Load credential from DB
        cred_result = await db.execute(
            select(BrandAPICredential).where(
                BrandAPICredential.brand_id == brand_id,
                BrandAPICredential.integration_name == "leaflink",
                BrandAPICredential.is_active == True,
            )
        )
        cred = cred_result.scalar_one_or_none()

        if not cred:
            logger.info(
                "[IntegrationCredentials] health_check brand=%s status=missing",
                brand_id,
            )
            return {
                "ok": False,
                "provider": "leaflink",
                "brand_id": brand_id,
                "credential_source": "db",
                "connected": False,
                "status": "missing",
                "last_success_at": None,
                "last_error": "No LeafLink credential found for this brand",
                "can_fetch_orders": False,
            }

        # Test the credential with a live request
        valid = validate_leaflink_credential(cred)

        if valid:
            status = "active"
            connected = True
            can_fetch_orders = True
            last_error = None
        else:
            # Distinguish between invalid key vs degraded (non-auth error)
            status = "invalid"
            connected = False
            can_fetch_orders = False
            last_error = cred.last_error or "Credential validation failed"

        last_success_at = cred.last_sync_at.isoformat() if cred.last_sync_at else None

        logger.info(
            "[IntegrationCredentials] health_check brand=%s status=%s",
            brand_id,
            status,
        )

        return {
            "ok": valid,
            "provider": "leaflink",
            "brand_id": brand_id,
            "credential_source": "db",
            "connected": connected,
            "status": status,
            "last_success_at": last_success_at,
            "last_error": last_error,
            "can_fetch_orders": can_fetch_orders,
        }

    except Exception as exc:
        logger.error(
            "[IntegrationCredentials] health_check_error brand=%s error=%s",
            brand_id,
            exc,
        )
        return {
            "ok": False,
            "provider": "leaflink",
            "brand_id": brand_id,
            "credential_source": "db",
            "connected": False,
            "status": "degraded",
            "last_success_at": None,
            "last_error": "Health check failed — check service logs",
            "can_fetch_orders": False,
        }


# ---------------------------------------------------------------------------
# POST /integrations/leaflink/connect
# ---------------------------------------------------------------------------

@router.post("/leaflink/connect")
async def leaflink_connect(
    body: dict,
    db: AsyncSession = Depends(get_db),
):
    """
    Connect or update LeafLink credentials for a brand.

    Validates the API key with a live LeafLink request before storing.
    Never echoes the API key back in the response.

    Request body:
      {
        "brand_id": "noble-nectar",
        "api_key": "...",
        "company_id": "...",
        "base_url": "https://www.leaflink.com/api/v2"  (optional)
      }

    Response (success):
      {
        "ok": true,
        "provider": "leaflink",
        "brand_id": "noble-nectar",
        "status": "active",
        "can_fetch_orders": true,
        "message": "LeafLink connected successfully."
      }

    Response (failure):
      {
        "ok": false,
        "provider": "leaflink",
        "brand_id": "noble-nectar",
        "status": "invalid",
        "can_fetch_orders": false,
        "message": "Invalid API key or connection failed."
      }
    """
    brand_id: str = (body.get("brand_id") or "").strip()
    api_key: str = (body.get("api_key") or "").strip()
    company_id: str = (body.get("company_id") or "").strip()
    base_url: str = (body.get("base_url") or "https://www.leaflink.com/api/v2").strip().rstrip("/")

    if not brand_id:
        return {
            "ok": False,
            "provider": "leaflink",
            "brand_id": None,
            "status": "invalid",
            "can_fetch_orders": False,
            "message": "brand_id is required.",
        }

    if not api_key:
        return {
            "ok": False,
            "provider": "leaflink",
            "brand_id": brand_id,
            "status": "invalid",
            "can_fetch_orders": False,
            "message": "api_key is required.",
        }

    logger.info("[IntegrationCredentials] connect_start brand=%s", brand_id)

    # Clean the API key — strip "Token " prefix if present
    clean_key = api_key
    if clean_key.startswith("Token "):
        clean_key = clean_key[6:].strip()

    # Validate the key with a live LeafLink request before storing
    test_url = f"{base_url}/orders-received/"
    valid = False
    validation_error: Optional[str] = None

    try:
        resp = requests.get(
            test_url,
            headers={
                "Authorization": f"Token {clean_key}",
                "Content-Type": "application/json",
            },
            params={"page": 1, "page_size": 1},
            timeout=15,
        )
        valid = resp.ok
        if not valid:
            validation_error = f"HTTP {resp.status_code}: {resp.text[:200]}"
    except Exception as exc:
        validation_error = f"Connection error: {exc}"
        logger.error(
            "[IntegrationCredentials] connect_validation_error brand=%s error=%s",
            brand_id,
            exc,
        )

    now = _utc_now()

    try:
        async with db.begin():
            # Upsert the credential inside the transaction
            cred_result = await db.execute(
                select(BrandAPICredential).where(
                    BrandAPICredential.brand_id == brand_id,
                    BrandAPICredential.integration_name == "leaflink",
                )
            )
            cred = cred_result.scalar_one_or_none()

            if cred is None:
                cred = BrandAPICredential(
                    brand_id=brand_id,
                    integration_name="leaflink",
                    api_key=clean_key,
                    company_id=company_id or None,
                    base_url=base_url,
                    is_active=True,
                    sync_status="ok" if valid else "failed",
                    last_error=None if valid else (validation_error or "Invalid credentials"),
                    last_checked_at=now,
                )
                db.add(cred)
            else:
                cred.api_key = clean_key
                cred.company_id = company_id or cred.company_id
                cred.base_url = base_url
                cred.is_active = True
                cred.sync_status = "ok" if valid else "failed"
                cred.last_error = None if valid else (validation_error or "Invalid credentials")
                cred.last_checked_at = now

    except Exception as exc:
        logger.error(
            "[IntegrationCredentials] connect_db_error brand=%s error=%s",
            brand_id,
            exc,
        )
        return {
            "ok": False,
            "provider": "leaflink",
            "brand_id": brand_id,
            "status": "invalid",
            "can_fetch_orders": False,
            "message": "Failed to save credentials. Check service logs.",
        }

    if valid:
        logger.info("[IntegrationCredentials] connect_success brand=%s", brand_id)
        return {
            "ok": True,
            "provider": "leaflink",
            "brand_id": brand_id,
            "status": "active",
            "can_fetch_orders": True,
            "message": "LeafLink connected successfully.",
        }
    else:
        logger.warning(
            "[IntegrationCredentials] connect_failed brand=%s reason=%s",
            brand_id,
            validation_error or "unknown",
        )
        return {
            "ok": False,
            "provider": "leaflink",
            "brand_id": brand_id,
            "status": "invalid",
            "can_fetch_orders": False,
            "message": "Invalid API key or connection failed.",
        }


# =============================================================================
# Secure Credentials Endpoints — AWS Secrets Manager backed
# POST   /api/integrations/{integration_name}/credentials
# GET    /api/integrations/{integration_name}/credentials
# DELETE /api/integrations/{integration_name}/credentials
# =============================================================================

_api_router = APIRouter(prefix="/api/integrations", tags=["integration-credentials"])


@_api_router.post("/{integration_name}/credentials")
async def configure_integration_credentials(
    integration_name: str,
    body: CredentialsConfigRequest,
    x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
    x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
    db: AsyncSession = Depends(get_db),
):
    """Configure integration credentials for a brand.

    Stores api_key and webhook_key in AWS Secrets Manager.
    Only secret references (ARN/path) and last-4 display hints are saved in the DB.
    Raw secrets are never returned in the response.

    Request body:
        brand_id, api_key, auth_scheme, base_url, leaflink_company_id,
        webhook_key, webhook_enabled, webhook_signature_required

    Response (HTTP 200):
        brand_id, integration_name, api_key_configured, api_key_last4,
        webhook_key_configured, webhook_key_last4, webhook_enabled,
        webhook_signature_required, leaflink_company_id, auth_scheme,
        base_url, created_at, updated_at
    """
    import os as _os
    from services.integration_secrets import store_integration_secret

    brand_id = body.brand_id.strip()
    integration_name_lower = integration_name.lower().strip()

    # Resolve and authenticate org
    org_id = await _resolve_org_id(x_opsyn_org, x_opsyn_secret, db)

    # Verify brand belongs to org
    await _verify_brand_belongs_to_org(db, org_id, brand_id)

    logger.info(
        "[IntegrationCredentials] configure_start org_id=%s brand_id=%s integration=%s",
        org_id,
        brand_id,
        integration_name_lower,
    )

    now = _utc_now()

    # Load or create the credential row
    try:
        result = await db.execute(
            select(BrandAPICredential).where(
                BrandAPICredential.brand_id == brand_id,
                BrandAPICredential.integration_name == integration_name_lower,
            )
        )
        cred = result.scalar_one_or_none()

        if cred is None:
            cred = BrandAPICredential(
                brand_id=brand_id,
                integration_name=integration_name_lower,
                is_active=True,
                sync_status="idle",
                created_at=now,
                updated_at=now,
            )
            db.add(cred)
            await db.flush()
    except Exception as exc:
        logger.error(
            "[IntegrationCredentials] configure_db_lookup_error brand_id=%s error=%s",
            brand_id,
            exc,
        )
        raise HTTPException(status_code=500, detail="Database error during credential lookup")

    # Store org_id on the credential for tenant resolution
    cred.org_id = org_id

    # Store API key in AWS Secrets Manager if provided
    api_key_configured = bool(getattr(cred, "api_key_secret_ref", None) or cred.api_key)
    api_key_last4 = getattr(cred, "api_key_last4", None)

    if body.api_key:
        try:
            secret_ref = await store_integration_secret(
                org_id=org_id,
                brand_id=brand_id,
                integration_name=integration_name_lower,
                secret_type="api_key",
                secret_value=body.api_key,
            )
            cred.api_key_secret_ref = secret_ref
            cred.api_key_last4 = body.api_key[-4:] if len(body.api_key) >= 4 else body.api_key
            api_key_configured = True
            api_key_last4 = cred.api_key_last4
            logger.info(
                "[IntegrationCredentials] api_key_stored_in_aws brand_id=%s last4=%s",
                brand_id,
                api_key_last4,
            )
        except RuntimeError as exc:
            allow_fallback = _os.getenv("ALLOW_PLAINTEXT_SECRET_FALLBACK", "false").lower() == "true"
            if allow_fallback:
                logger.warning(
                    "[IntegrationCredentials] api_key_plaintext_fallback brand_id=%s reason=%s",
                    brand_id,
                    str(exc)[:200],
                )
                cred.api_key = body.api_key
                cred.api_key_last4 = body.api_key[-4:] if len(body.api_key) >= 4 else body.api_key
                api_key_configured = True
                api_key_last4 = cred.api_key_last4
            else:
                logger.error(
                    "[IntegrationCredentials] api_key_store_failed brand_id=%s error=%s",
                    brand_id,
                    str(exc)[:200],
                )
                raise HTTPException(
                    status_code=503,
                    detail={
                        "error": "Failed to store API key in AWS Secrets Manager",
                        "error_code": "AWS_SECRETS_STORE_FAILED",
                        "details": str(exc)[:200],
                    },
                )

    # Store webhook key in AWS Secrets Manager if provided
    webhook_key_configured = bool(
        getattr(cred, "webhook_key_secret_ref", None) or cred.webhook_key
    )
    webhook_key_last4 = getattr(cred, "webhook_key_last4", None)

    if body.webhook_key:
        try:
            wh_secret_ref = await store_integration_secret(
                org_id=org_id,
                brand_id=brand_id,
                integration_name=integration_name_lower,
                secret_type="webhook_key",
                secret_value=body.webhook_key,
            )
            cred.webhook_key_secret_ref = wh_secret_ref
            cred.webhook_key_last4 = body.webhook_key[-4:] if len(body.webhook_key) >= 4 else body.webhook_key
            webhook_key_configured = True
            webhook_key_last4 = cred.webhook_key_last4
            logger.info(
                "[IntegrationCredentials] webhook_key_stored_in_aws brand_id=%s last4=%s",
                brand_id,
                webhook_key_last4,
            )
        except RuntimeError as exc:
            allow_fallback = _os.getenv("ALLOW_PLAINTEXT_SECRET_FALLBACK", "false").lower() == "true"
            if allow_fallback:
                logger.warning(
                    "[IntegrationCredentials] webhook_key_plaintext_fallback brand_id=%s reason=%s",
                    brand_id,
                    str(exc)[:200],
                )
                cred.webhook_key = body.webhook_key
                cred.webhook_key_last4 = body.webhook_key[-4:] if len(body.webhook_key) >= 4 else body.webhook_key
                webhook_key_configured = True
                webhook_key_last4 = cred.webhook_key_last4
            else:
                logger.error(
                    "[IntegrationCredentials] webhook_key_store_failed brand_id=%s error=%s",
                    brand_id,
                    str(exc)[:200],
                )
                raise HTTPException(
                    status_code=503,
                    detail={
                        "error": "Failed to store webhook key in AWS Secrets Manager",
                        "error_code": "AWS_SECRETS_STORE_FAILED",
                        "details": str(exc)[:200],
                    },
                )

    # Update non-secret fields
    if body.auth_scheme is not None:
        cred.auth_scheme = body.auth_scheme
    if body.base_url is not None:
        cred.base_url = body.base_url.strip().rstrip("/")
    if body.leaflink_company_id is not None:
        cred.leaflink_company_id = body.leaflink_company_id
        cred.company_id = body.leaflink_company_id
    if body.webhook_enabled is not None:
        cred.webhook_enabled = body.webhook_enabled
    if body.webhook_signature_required is not None:
        cred.webhook_signature_required = body.webhook_signature_required

    cred.is_active = True
    cred.updated_at = now

    try:
        await db.commit()
        await db.refresh(cred)
    except Exception as exc:
        logger.error(
            "[IntegrationCredentials] configure_commit_error brand_id=%s error=%s",
            brand_id,
            exc,
        )
        await db.rollback()
        raise HTTPException(status_code=500, detail="Failed to save credential configuration")

    logger.info(
        "[IntegrationCredentials] configure_success brand_id=%s integration=%s "
        "api_key_configured=%s webhook_key_configured=%s",
        brand_id,
        integration_name_lower,
        api_key_configured,
        webhook_key_configured,
    )

    return {
        "brand_id": brand_id,
        "integration_name": integration_name_lower,
        "api_key_configured": api_key_configured,
        "api_key_last4": api_key_last4,
        "webhook_key_configured": webhook_key_configured,
        "webhook_key_last4": webhook_key_last4,
        "webhook_enabled": getattr(cred, "webhook_enabled", False),
        "webhook_signature_required": getattr(cred, "webhook_signature_required", True),
        "leaflink_company_id": getattr(cred, "leaflink_company_id", None),
        "auth_scheme": cred.auth_scheme,
        "base_url": cred.base_url,
        "created_at": cred.created_at.isoformat() if cred.created_at else None,
        "updated_at": cred.updated_at.isoformat() if cred.updated_at else None,
    }


@_api_router.get("/{integration_name}/credentials")
async def get_integration_credentials(
    integration_name: str,
    brand_id: str = Query(..., description="Brand UUID"),
    x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
    x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
    db: AsyncSession = Depends(get_db),
):
    """Retrieve integration credential metadata for a brand.

    Never returns raw secrets — only configuration metadata and last-4 hints.

    Query params:
        brand_id: Brand UUID

    Response (HTTP 200):
        brand_id, integration_name, api_key_configured, api_key_last4,
        webhook_key_configured, webhook_key_last4, webhook_enabled,
        webhook_signature_required, leaflink_company_id, auth_scheme,
        base_url, last_checked_at, last_error
    """
    integration_name_lower = integration_name.lower().strip()

    # Resolve and authenticate org
    org_id = await _resolve_org_id(x_opsyn_org, x_opsyn_secret, db)

    # Verify brand belongs to org
    await _verify_brand_belongs_to_org(db, org_id, brand_id)

    try:
        result = await db.execute(
            select(BrandAPICredential).where(
                BrandAPICredential.brand_id == brand_id,
                BrandAPICredential.integration_name == integration_name_lower,
                BrandAPICredential.is_active == True,
            )
        )
        cred = result.scalar_one_or_none()
    except Exception as exc:
        logger.error(
            "[IntegrationCredentials] get_db_error brand_id=%s error=%s",
            brand_id,
            exc,
        )
        raise HTTPException(status_code=500, detail="Database error")

    if cred is None:
        raise HTTPException(
            status_code=404,
            detail=f"No active {integration_name_lower} credential found for brand_id={brand_id}",
        )

    # Determine configured state — check both AWS ref and legacy plaintext
    api_key_secret_ref = getattr(cred, "api_key_secret_ref", None)
    api_key_last4 = getattr(cred, "api_key_last4", None)
    api_key_configured = bool(api_key_secret_ref or cred.api_key)

    if not api_key_last4 and cred.api_key:
        api_key_last4 = cred.api_key[-4:] if len(cred.api_key) >= 4 else "****"

    webhook_key_secret_ref = getattr(cred, "webhook_key_secret_ref", None)
    webhook_key_last4 = getattr(cred, "webhook_key_last4", None)
    webhook_key_configured = bool(webhook_key_secret_ref or cred.webhook_key)

    if not webhook_key_last4 and cred.webhook_key:
        webhook_key_last4 = cred.webhook_key[-4:] if len(cred.webhook_key) >= 4 else "****"

    return {
        "brand_id": brand_id,
        "integration_name": integration_name_lower,
        "api_key_configured": api_key_configured,
        "api_key_last4": api_key_last4,
        "webhook_key_configured": webhook_key_configured,
        "webhook_key_last4": webhook_key_last4,
        "webhook_enabled": getattr(cred, "webhook_enabled", False),
        "webhook_signature_required": getattr(cred, "webhook_signature_required", True),
        "leaflink_company_id": getattr(cred, "leaflink_company_id", None),
        "auth_scheme": cred.auth_scheme,
        "base_url": cred.base_url,
        "last_checked_at": cred.last_checked_at.isoformat() if cred.last_checked_at else None,
        "last_error": cred.last_error,
    }


@_api_router.delete("/{integration_name}/credentials", status_code=204)
async def delete_integration_credentials(
    integration_name: str,
    brand_id: str = Query(..., description="Brand UUID"),
    x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
    x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
    db: AsyncSession = Depends(get_db),
):
    """Delete integration credentials for a brand.

    Deletes AWS secrets if they exist, then deactivates the credential row.
    Never leaves orphaned active credentials.

    Response: HTTP 204 No Content
    """
    from services.integration_secrets import delete_integration_secret

    integration_name_lower = integration_name.lower().strip()

    # Resolve and authenticate org
    org_id = await _resolve_org_id(x_opsyn_org, x_opsyn_secret, db)

    # Verify brand belongs to org
    await _verify_brand_belongs_to_org(db, org_id, brand_id)

    try:
        result = await db.execute(
            select(BrandAPICredential).where(
                BrandAPICredential.brand_id == brand_id,
                BrandAPICredential.integration_name == integration_name_lower,
            )
        )
        cred = result.scalar_one_or_none()
    except Exception as exc:
        logger.error(
            "[IntegrationCredentials] delete_db_error brand_id=%s error=%s",
            brand_id,
            exc,
        )
        raise HTTPException(status_code=500, detail="Database error")

    if cred is None:
        # Already gone — return 204 (idempotent)
        return

    # Delete AWS secrets (non-blocking — log failures but don't abort)
    api_key_secret_ref = getattr(cred, "api_key_secret_ref", None)
    if api_key_secret_ref:
        deleted = await delete_integration_secret(api_key_secret_ref)
        logger.info(
            "[IntegrationCredentials] api_key_secret_deleted brand_id=%s ref=%s ok=%s",
            brand_id,
            api_key_secret_ref,
            deleted,
        )

    webhook_key_secret_ref = getattr(cred, "webhook_key_secret_ref", None)
    if webhook_key_secret_ref:
        deleted = await delete_integration_secret(webhook_key_secret_ref)
        logger.info(
            "[IntegrationCredentials] webhook_key_secret_deleted brand_id=%s ref=%s ok=%s",
            brand_id,
            webhook_key_secret_ref,
            deleted,
        )

    # Deactivate the credential row and clear secret refs
    try:
        cred.is_active = False
        cred.api_key_secret_ref = None
        cred.api_key_last4 = None
        cred.webhook_key_secret_ref = None
        cred.webhook_key_last4 = None
        cred.api_key = None
        cred.webhook_key = None
        cred.updated_at = _utc_now()
        await db.commit()
    except Exception as exc:
        logger.error(
            "[IntegrationCredentials] delete_commit_error brand_id=%s error=%s",
            brand_id,
            exc,
        )
        await db.rollback()
        raise HTTPException(status_code=500, detail="Failed to deactivate credential")

    logger.info(
        "[IntegrationCredentials] credentials_deleted brand_id=%s integration=%s",
        brand_id,
        integration_name_lower,
    )
