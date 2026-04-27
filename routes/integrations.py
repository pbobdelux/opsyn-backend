"""
Integration health-check routes.

Provides visibility into the status of third-party integrations (LeafLink,
etc.) without exposing any secret values.
"""

import logging

from fastapi import APIRouter

from services import leaflink_auth

logger = logging.getLogger("integrations")

router = APIRouter(prefix="/integrations", tags=["integrations"])


@router.get("/leaflink/health")
def leaflink_health():
    """
    Return the current health of the LeafLink authentication subsystem.

    Performs a lightweight live test request to confirm that credentials are
    accepted by the LeafLink API.

    Response:
      {
        "ok": true | false,
        "auth_present": bool,
        "auth_mode": "api_key" | "oauth",
        "last_success_at": "<ISO timestamp | null>",
        "last_error": "<string | null>",
        "can_fetch_orders": bool
      }

    Never exposes secret values.
    """
    logger.info("[LeafLinkAuth] health_check_requested")

    health = leaflink_auth.get_auth_health()

    ok = health["auth_present"] and health["can_fetch_orders"]

    logger.info(
        "[LeafLinkAuth] health_check_result ok=%s auth_present=%s auth_mode=%s can_fetch_orders=%s last_error=%s",
        ok,
        health["auth_present"],
        health["auth_mode"],
        health["can_fetch_orders"],
        health.get("last_error") or "none",
    )

    return {
        "ok": ok,
        "auth_present": health["auth_present"],
        "auth_mode": health["auth_mode"],
        "last_success_at": health["last_success_at"],
        "last_error": health["last_error"],
        "can_fetch_orders": health["can_fetch_orders"],
    }
