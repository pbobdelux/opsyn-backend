"""
Integration management routes.

Provides endpoints to connect, validate, and check the health of third-party
integrations (LeafLink, etc.) on a per-brand basis.

Endpoints:
  GET  /integrations/leaflink/health   — Brand-aware health check
  POST /integrations/leaflink/connect  — Connect/update LeafLink credentials
"""

import logging
from datetime import datetime, timezone
from typing import Optional

import requests
from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import BrandAPICredential
from services.integration_credentials import (
    validate_leaflink_credential,
    mark_credential_invalid,
    mark_credential_success,
)

logger = logging.getLogger("integrations")

router = APIRouter(prefix="/integrations", tags=["integrations"])


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
