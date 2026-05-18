"""
Instant health endpoints — no DB or LeafLink calls.

Routes registered here:
  GET /health        — instant response (< 50ms). Used by iOS app and load balancers.
  GET /health/deep   — comprehensive checks (DB ping + LeafLink sync health, < 2s).
                       Delegates to the legacy health_deep_legacy() handler in health.py.

This router MUST be registered in main.py BEFORE health_router so that the
instant /health takes precedence over the deep-check handler in health.py.
"""

import logging
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from routes.health import (
    APP_VERSION,
    _compute_sync_status,
    _utc_now_iso,
)

logger = logging.getLogger("health_instant")

router = APIRouter(tags=["health"])


# ---------------------------------------------------------------------------
# GET /health — instant, no DB, no LeafLink
# ---------------------------------------------------------------------------

@router.get("/health")
async def health_instant() -> dict[str, Any]:
    """
    Instant service health check — returns immediately with no DB or
    LeafLink queries.  Response time is always < 50ms.

    Used by the iOS app and load balancers for liveness probes.
    For a comprehensive health check (DB + LeafLink sync), use GET /health/deep.
    """
    return {
        "status": "ok",
        "version": APP_VERSION,
        "timestamp": _utc_now_iso(),
    }


# ---------------------------------------------------------------------------
# GET /health/deep — comprehensive checks (DB ping + LeafLink sync health)
# ---------------------------------------------------------------------------

@router.get("/health/deep")
async def health_deep(db: AsyncSession = Depends(get_db)) -> dict[str, Any]:
    """
    Comprehensive health check — performs a DB ping and LeafLink sync health
    query.  Response time is typically < 2s.

    This contains the same logic as the original /health endpoint, now
    accessible at /health/deep so that /health can remain instant.
    """
    timestamp = _utc_now_iso()
    database_status = "ok"
    leaflink_sync_status = "ok"

    # --- Database ping ---
    try:
        await db.execute(text("SELECT 1"))
        logger.info("[DB_HEALTH_OK] health_check=passed")
    except Exception as db_exc:
        database_status = "error"
        logger.error("[DB_HEALTH_FAIL] health_check=failed error=%s", str(db_exc)[:200])

    # --- LeafLink sync health ---
    try:
        result = await db.execute(
            text("""
                SELECT
                    consecutive_failures,
                    last_successful_sync_at,
                    (SELECT COUNT(*) FROM sync_dead_letters WHERE resolved_at IS NULL) AS dead_letter_count
                FROM sync_health
                ORDER BY updated_at DESC
                LIMIT 1
            """)
        )
        row = result.fetchone()
        if row:
            consecutive_failures = row[0] or 0
            last_successful_sync_at = row[1]
            dead_letter_count = row[2] or 0
            status = _compute_sync_status(
                consecutive_failures, last_successful_sync_at, dead_letter_count
            )
            if status == "failing":
                leaflink_sync_status = "error"
            elif status == "degraded":
                leaflink_sync_status = "warning"
            else:
                leaflink_sync_status = "ok"
        else:
            leaflink_sync_status = "warning"  # no sync health data yet
    except Exception as sync_exc:
        leaflink_sync_status = "error"
        logger.warning("[SYNC_HEALTH_STATUS] check_failed error=%s", str(sync_exc)[:200])

    logger.info(
        "[SYNC_HEALTH_STATUS] database=%s leaflink_sync=%s",
        database_status,
        leaflink_sync_status,
    )

    return {
        "ok": database_status == "ok",
        "service": "opsyn-backend",
        "version": APP_VERSION,
        "timestamp": timestamp,
        "database": database_status,
        "leaflink_sync": leaflink_sync_status,
    }
