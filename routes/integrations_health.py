"""
Integration health monitoring endpoints.

Provides visibility into the health of all integrations without exposing
any secret values or internal implementation details.

Endpoints:
  GET  /integrations/health         — Full health report for all integrations
  GET  /integrations/alerts         — Only integrations requiring attention
  POST /integrations/leaflink/test  — Test LeafLink auth (no secrets exposed)
"""

import logging
import os
from datetime import datetime, timezone
from typing import Optional

import requests
from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import BrandAPICredential
from services.integration_credentials import validate_leaflink_credential
from services.integration_health import (
    get_all_integrations_health,
    get_overall_status,
    get_summary,
)

logger = logging.getLogger("integrations_health")

router = APIRouter(prefix="/integrations", tags=["integrations-health"])


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _base_url() -> str:
    port = os.getenv("PORT", "8000")
    return os.getenv("BACKEND_BASE_URL", f"http://localhost:{port}")


# ---------------------------------------------------------------------------
# GET /integrations/health
# ---------------------------------------------------------------------------

@router.get("/health")
async def integrations_health():
    """
    Return a comprehensive health report for all integrations.

    Response:
      {
        "ok": true,
        "overall_status": "healthy" | "degraded" | "broken",
        "checked_at": "<ISO timestamp>",
        "integrations": [...]
      }

    Never exposes secrets. Caches results for 30 seconds.
    """
    logger.info("[IntegrationHealth] health_endpoint_hit")

    try:
        integrations = await get_all_integrations_health()
        overall = get_overall_status(integrations)
        summary = get_summary(integrations)

        return {
            "ok": True,
            "overall_status": overall,
            "checked_at": _utc_now_iso(),
            "summary": summary,
            "integrations": [i.to_dict() for i in integrations],
        }

    except Exception as exc:
        logger.error("[IntegrationHealth] health_endpoint_error error=%s", exc)
        return {
            "ok": False,
            "overall_status": "unknown",
            "checked_at": _utc_now_iso(),
            "summary": "Health check failed. Check service logs.",
            "integrations": [],
        }


# ---------------------------------------------------------------------------
# GET /integrations/alerts
# ---------------------------------------------------------------------------

@router.get("/alerts")
async def integrations_alerts():
    """
    Return only integrations that require attention (broken, stale, degraded).

    Useful for dashboards and alert systems.

    Response:
      {
        "ok": true,
        "alerts": [...],
        "count": N
      }
    """
    logger.info("[IntegrationHealth] alerts_endpoint_hit count=pending")

    try:
        integrations = await get_all_integrations_health()
        alerts = [i for i in integrations if i.requires_attention]

        logger.info("[IntegrationHealth] alerts_endpoint_hit count=%d", len(alerts))

        return {
            "ok": True,
            "alerts": [i.to_dict() for i in alerts],
            "count": len(alerts),
        }

    except Exception as exc:
        logger.error("[IntegrationHealth] alerts_endpoint_error error=%s", exc)
        return {
            "ok": False,
            "alerts": [],
            "count": 0,
        }


# ---------------------------------------------------------------------------
# POST /integrations/leaflink/test
# ---------------------------------------------------------------------------

@router.post("/leaflink/test")
async def leaflink_test(
    brand_id: Optional[str] = Query(None, description="Brand slug to test (e.g. 'noble-nectar')"),
    db: AsyncSession = Depends(get_db),
):
    """
    Test LeafLink authentication and order fetch capability for a specific brand.

    Loads the credential from the database and attempts a minimal order fetch
    (page_size=1) to confirm credentials work.
    Never exposes API keys, tokens, or internal error details.

    Query params:
      brand_id: Brand slug to test

    Response:
      {
        "ok": true,
        "auth_valid": true,
        "can_fetch": true,
        "credential_source": "db",
        "brand_id": "noble-nectar",
        "message": "LeafLink connection is working."
      }
    """
    logger.info("[IntegrationHealth] leaflink_test_start brand=%s", brand_id or "unspecified")

    try:
        if not brand_id:
            return {
                "ok": False,
                "auth_valid": False,
                "can_fetch": False,
                "credential_source": "db",
                "brand_id": None,
                "message": "brand_id query parameter is required.",
            }

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
                "[IntegrationCredentials] health_check brand=%s status=missing source=db",
                brand_id,
            )
            return {
                "ok": False,
                "auth_valid": False,
                "can_fetch": False,
                "credential_source": "db",
                "brand_id": brand_id,
                "message": "No LeafLink credentials found for this brand.",
            }

        valid = validate_leaflink_credential(cred)
        auth_valid = True  # credential exists in DB
        can_fetch = valid

        if not valid:
            last_error = cred.last_error or ""
            if "403" in str(last_error) or "401" in str(last_error):
                message = "LeafLink credentials are invalid or expired. Please reconnect."
            else:
                message = "LeafLink credentials are present but orders cannot be fetched. Check connection."
            ok = False
        else:
            message = "LeafLink connection is working."
            ok = True

        logger.info(
            "[IntegrationHealth] leaflink_test_result brand=%s ok=%s auth_valid=%s can_fetch=%s credential_source=db",
            brand_id,
            ok,
            auth_valid,
            can_fetch,
        )

        return {
            "ok": ok,
            "auth_valid": auth_valid,
            "can_fetch": can_fetch,
            "credential_source": "db",
            "brand_id": brand_id,
            "message": message,
        }

    except Exception as exc:
        logger.error("[IntegrationHealth] leaflink_test_error brand=%s error=%s", brand_id, exc)
        return {
            "ok": False,
            "auth_valid": False,
            "can_fetch": False,
            "credential_source": "db",
            "brand_id": brand_id,
            "message": "LeafLink test failed. Check service logs.",
        }
