"""
Instant health endpoints for opsyn-backend.

GET /health
  — Instant response, no DB or LeafLink checks.
  — Response time: < 50ms.
  — Used by iOS app and load balancers for liveness probes.

GET /health/deep
  — Comprehensive checks: DB ping, LeafLink sync health.
  — Same logic as the original /health implementation.
  — Response time: < 2s.
  — Use for readiness probes and manual diagnostics.
"""

import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db

logger = logging.getLogger("health_instant")

router = APIRouter(tags=["health"])

APP_VERSION = "1.0.0"

_LEAFLINK_SYNC_STALE_MINUTES = int(os.getenv("LEAFLINK_SYNC_STALE_MINUTES", "60"))
_WATCHDOG_CONSECUTIVE_FAILURE_THRESHOLD = 3
_WATCHDOG_DEAD_LETTER_THRESHOLD = 10


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _compute_sync_status(
    consecutive_failures: int,
    last_successful_sync_at: Optional[datetime],
    dead_letter_count: int,
    stale_minutes: int = _LEAFLINK_SYNC_STALE_MINUTES,
) -> str:
    """Compute watchdog status: healthy | degraded | failing."""
    if consecutive_failures >= _WATCHDOG_CONSECUTIVE_FAILURE_THRESHOLD:
        return "failing"
    if dead_letter_count >= _WATCHDOG_DEAD_LETTER_THRESHOLD:
        return "degraded"
    if last_successful_sync_at is not None:
        age_minutes = (datetime.now(timezone.utc) - last_successful_sync_at).total_seconds() / 60
        if age_minutes > stale_minutes:
            return "degraded"
    return "healthy"


# ---------------------------------------------------------------------------
# GET /health — instant liveness probe (no DB, no external checks)
# ---------------------------------------------------------------------------

@router.get("/health")
@router.get("/health/")
async def health_instant() -> dict[str, Any]:
    """
    Instant liveness probe — responds immediately without any DB or
    external service checks.

    iOS app and load balancers should use this endpoint.
    Response time: < 50ms guaranteed.
    """
    return {
        "ok": True,
        "service": "opsyn-backend",
        "version": APP_VERSION,
        "timestamp": _utc_now_iso(),
        "database": "not_checked",
        "leaflink_sync": "not_checked",
    }


# ---------------------------------------------------------------------------
# GET /health/deep — comprehensive health check (DB + sync health)
# ---------------------------------------------------------------------------

@router.get("/health/deep")
async def health_deep(db: AsyncSession = Depends(get_db)) -> dict[str, Any]:
    """
    Comprehensive health check — performs DB ping and LeafLink sync health
    queries. Same logic as the original /health endpoint.

    Use for readiness probes and manual diagnostics.
    Response time: < 2s.
    """
    timestamp = _utc_now_iso()
    database_status = "ok"
    leaflink_sync_status = "ok"

    # --- Database ping ---
    try:
        await db.execute(text("SELECT 1"))
        logger.info("[DB_HEALTH_OK] health_check=passed endpoint=deep")
    except Exception as db_exc:
        database_status = "error"
        logger.error("[DB_HEALTH_FAIL] health_check=failed endpoint=deep error=%s", str(db_exc)[:200])

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
            status = _compute_sync_status(consecutive_failures, last_successful_sync_at, dead_letter_count)
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
        logger.warning("[SYNC_HEALTH_STATUS] check_failed endpoint=deep error=%s", str(sync_exc)[:200])

    logger.info(
        "[SYNC_HEALTH_STATUS] database=%s leaflink_sync=%s endpoint=deep",
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
