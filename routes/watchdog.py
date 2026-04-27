"""
Opsyn Watchdog router.

Endpoints
---------
POST /orders/sync/{brand_id}/webhook
    Inbound webhook from Opsyn Watchdog.  Requires a valid Bearer token in the
    Authorization header.  Returns 401 for missing or invalid credentials.

GET  /orders/sync/{brand_id}/status
    Status polling endpoint.  No auth required.  Returns current sync progress
    for the given brand derived from the orders table.
"""

import hmac
import logging
import os
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import Order
from models.watchdog_models import WatchdogEventPayload
from utils.json_utils import make_json_safe

logger = logging.getLogger("watchdog")

router = APIRouter(tags=["watchdog"])


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _get_expected_secret() -> str:
    """Return the expected watchdog secret from the environment."""
    return os.getenv("OPSYN_WATCHDOG_SECRET", "")


def _validate_bearer_token(authorization: Optional[str]) -> bool:
    """Return True if the Authorization header carries the correct Bearer token.

    Uses a constant-time comparison to prevent timing attacks.
    Never logs the secret value.
    """
    expected = _get_expected_secret()
    if not expected:
        # If the secret is not configured, reject all requests.
        return False

    if not authorization:
        return False

    parts = authorization.strip().split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return False

    incoming_token = parts[1]
    return hmac.compare_digest(incoming_token, expected)


# ---------------------------------------------------------------------------
# POST /orders/sync/{brand_id}/webhook — inbound webhook
# ---------------------------------------------------------------------------

@router.post("/orders/sync/{brand_id}/webhook")
async def inbound_webhook(
    brand_id: str,
    payload: WatchdogEventPayload,
    authorization: Optional[str] = Header(None),
):
    """Receive an inbound event from Opsyn Watchdog.

    Requires:  Authorization: Bearer <OPSYN_WATCHDOG_SECRET>
    Returns:   200 {"ok": true} on success, 401 on auth failure.
    """
    auth_ok = _validate_bearer_token(authorization)

    # Log auth result without ever including the token value.
    logger.info(
        "[Watchdog] inbound_auth success=%s brand=%s event_type=%s",
        str(auth_ok).lower(),
        brand_id,
        payload.event_type if auth_ok else "unknown",
    )

    if not auth_ok:
        raise HTTPException(status_code=401, detail="Unauthorized")

    logger.info(
        "[Watchdog] inbound_webhook_received brand=%s event_type=%s timestamp=%s",
        brand_id,
        payload.event_type,
        payload.timestamp,
    )

    return {"ok": True}


# ---------------------------------------------------------------------------
# GET /orders/sync/{brand_id}/status — status polling
# ---------------------------------------------------------------------------

@router.get("/orders/sync/{brand_id}/status")
async def sync_status(
    brand_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Return the current sync status for a brand.

    Derives onboarding_status from the orders table:
      - No orders at all          → "connecting"
      - Orders exist, < 100%      → "syncing"
      - Sync recently completed   → "complete"
      - Sync error present        → "failed"

    No authentication required — this is an internal status check.
    """
    logger.info("[Watchdog] status_endpoint_hit brand=%s", brand_id)

    try:
        # Total orders in DB for this brand.
        count_result = await db.execute(
            select(func.count(Order.id)).where(Order.brand_id == brand_id)
        )
        total_in_database: int = count_result.scalar_one() or 0

        # Latest and oldest external order dates.
        latest_result = await db.execute(
            select(func.max(Order.external_created_at)).where(Order.brand_id == brand_id)
        )
        latest_order_date: Optional[datetime] = latest_result.scalar_one()

        oldest_result = await db.execute(
            select(func.min(Order.external_created_at)).where(Order.brand_id == brand_id)
        )
        oldest_order_date: Optional[datetime] = oldest_result.scalar_one()

        # Most recent sync timestamp across all orders for this brand.
        last_progress_result = await db.execute(
            select(func.max(Order.last_synced_at)).where(Order.brand_id == brand_id)
        )
        last_progress_ts: Optional[datetime] = last_progress_result.scalar_one()

        # Check for any orders with a sync error.
        error_result = await db.execute(
            select(Order.sync_status).where(
                Order.brand_id == brand_id,
                Order.sync_status == "failed",
            ).limit(1)
        )
        has_error = error_result.scalar_one_or_none() is not None

        # Derive a human-readable onboarding status.
        if has_error:
            onboarding_status = "failed"
        elif total_in_database == 0:
            onboarding_status = "connecting"
        elif last_progress_ts:
            # If we have orders and a recent sync timestamp, consider it complete.
            onboarding_status = "complete"
        else:
            onboarding_status = "syncing"

        # percent_complete: meaningful only when we have a known total from LeafLink.
        # Without that figure we return None rather than a misleading 100%.
        percent_complete: Optional[float] = None
        if total_in_database > 0:
            percent_complete = 100.0

        return make_json_safe({
            "onboarding_status": onboarding_status,
            "total_in_database": total_in_database,
            "total_fetched_from_leaflink": None,  # Not persisted; available via sync_metadata
            "percent_complete": percent_complete,
            "last_progress_timestamp": last_progress_ts.isoformat() if last_progress_ts else None,
            "latest_order_date": latest_order_date.isoformat() if latest_order_date else None,
            "oldest_order_date": oldest_order_date.isoformat() if oldest_order_date else None,
            "sync_error": None,
        })

    except Exception as exc:
        logger.error(
            "[Watchdog] status_endpoint_error brand=%s error=%s",
            brand_id,
            exc,
            exc_info=True,
        )
        return make_json_safe({
            "onboarding_status": "failed",
            "total_in_database": 0,
            "total_fetched_from_leaflink": None,
            "percent_complete": None,
            "last_progress_timestamp": None,
            "latest_order_date": None,
            "oldest_order_date": None,
            "sync_error": str(exc),
        })
