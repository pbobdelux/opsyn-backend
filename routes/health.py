import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urlparse

from fastapi import APIRouter, Depends
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import BrandAPICredential, Order
from services.integration_health import (
    get_all_integrations_health,
    get_overall_status,
    get_summary,
    IntegrationStatus,
)
from utils.json_utils import make_json_safe

logger = logging.getLogger("health")

router = APIRouter(prefix="/health", tags=["health"])

# ---------------------------------------------------------------------------
# Feature flags
# ---------------------------------------------------------------------------
_LEAFLINK_SYNC_STALE_MINUTES = int(os.getenv("LEAFLINK_SYNC_STALE_MINUTES", "60"))
_WATCHDOG_CONSECUTIVE_FAILURE_THRESHOLD = 3
_WATCHDOG_DEAD_LETTER_THRESHOLD = 10

APP_VERSION = "1.0.0"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sanitize_db_host(database_url: str) -> str:
    """Return only the hostname from a DATABASE_URL — no credentials."""
    try:
        parsed = urlparse(database_url)
        return parsed.hostname or "unknown"
    except Exception:
        return "unknown"


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
# NOTE: The instant GET /health endpoint is now in routes/health_instant.py.
# It is registered BEFORE this router in main.py so that /health returns
# instantly (< 50ms, no DB calls).  The comprehensive check is available at
# GET /health/deep (also in routes/health_instant.py).
#
# The legacy root handler below is kept as /health/deep-legacy for reference
# but is no longer the primary /health route.
# ---------------------------------------------------------------------------

@router.get("/deep")
async def health_deep_legacy(db: AsyncSession = Depends(get_db)) -> dict[str, Any]:
    """
    Legacy comprehensive health check — DB ping + LeafLink sync health.

    Kept for backward compatibility.  The canonical deep-check endpoint is
    now GET /health/deep (registered in routes/health_instant.py).

    The instant /health endpoint (no DB calls, < 50ms) is in
    routes/health_instant.py and is registered first in main.py.
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


# ---------------------------------------------------------------------------
# GET /health/db — database connectivity check
# ---------------------------------------------------------------------------

@router.get("/db")
async def health_db(db: AsyncSession = Depends(get_db)) -> dict[str, Any]:
    """
    Database connectivity health check.

    Runs SELECT 1, measures latency, returns sanitized host (no credentials).
    """
    database_url = os.getenv("DATABASE_URL", "")
    db_host = _sanitize_db_host(database_url)

    start = time.monotonic()
    try:
        await db.execute(text("SELECT 1"))
        latency_ms = round((time.monotonic() - start) * 1000, 2)
        logger.info("[DB_HEALTH_OK] host=%s latency_ms=%s", db_host, latency_ms)
        return {
            "ok": True,
            "database": "ok",
            "host": db_host,
            "latency_ms": latency_ms,
            "timestamp": _utc_now_iso(),
        }
    except Exception as exc:
        latency_ms = round((time.monotonic() - start) * 1000, 2)
        logger.error("[DB_HEALTH_FAIL] host=%s latency_ms=%s error=%s", db_host, latency_ms, str(exc)[:200])
        return {
            "ok": False,
            "database": "error",
            "host": db_host,
            "latency_ms": latency_ms,
            "error": str(exc)[:200],
            "timestamp": _utc_now_iso(),
        }


# ---------------------------------------------------------------------------
# GET /health/sync — per-brand sync health
# ---------------------------------------------------------------------------

@router.get("/sync")
async def health_sync(db: AsyncSession = Depends(get_db)) -> dict[str, Any]:
    """
    Per-brand LeafLink sync health.

    Returns one entry per brand with:
      - brand_id
      - last_successful_sync_at
      - last_sync_attempt_at
      - last_error_at
      - last_error_message
      - orders_fetched_last_run
      - orders_written_last_run
      - dead_letter_count
      - consecutive_failures
      - status: healthy | degraded | failing
    """
    timestamp = _utc_now_iso()

    try:
        # Fetch all sync_health rows, joined with credential auth status
        result = await db.execute(
            text("""
                SELECT
                    sh.brand_id,
                    sh.last_successful_sync_at,
                    sh.last_attempted_sync_at,
                    sh.last_error,
                    sh.consecutive_failures,
                    COALESCE(sh.orders_fetched_last_run, 0) AS orders_fetched_last_run,
                    COALESCE(sh.orders_written_last_run, 0) AS orders_written_last_run,
                    sh.last_error_at,
                    (
                        SELECT COUNT(*)
                        FROM sync_dead_letters sdl
                        WHERE sdl.brand_id = sh.brand_id
                          AND sdl.resolved_at IS NULL
                    ) AS dead_letter_count,
                    (
                        SELECT bac.sync_status
                        FROM brand_api_credentials bac
                        WHERE bac.brand_id = sh.brand_id
                          AND bac.integration_name = 'leaflink'
                          AND bac.is_active = true
                        LIMIT 1
                    ) AS credential_sync_status
                FROM sync_health sh
                ORDER BY sh.brand_id
            """)
        )
        rows = result.fetchall()

        brands = []
        overall_status = "healthy"

        for row in rows:
            brand_id = row[0]
            last_successful_sync_at = row[1]
            last_sync_attempt_at = row[2]
            last_error_message = row[3]
            consecutive_failures = row[4] or 0
            orders_fetched_last_run = row[5] or 0
            orders_written_last_run = row[6] or 0
            last_error_at = row[7]
            dead_letter_count = int(row[8] or 0)
            credential_sync_status = row[9] if len(row) > 9 else None

            # Auth failure circuit breaker: if credential is auth_failed, mark as failing
            auth_failed = credential_sync_status == "auth_failed"
            if auth_failed:
                status = "failing"
            else:
                status = _compute_sync_status(
                    consecutive_failures,
                    last_successful_sync_at,
                    dead_letter_count,
                )

            if status == "failing" and overall_status != "failing":
                overall_status = "failing"
            elif status == "degraded" and overall_status == "healthy":
                overall_status = "degraded"

            # Emit watchdog warning log if degraded/failing
            if status in ("degraded", "failing"):
                logger.warning(
                    "[SYNC_WATCHDOG_WARNING] brand_id=%s status=%s consecutive_failures=%s"
                    " dead_letter_count=%s last_successful_sync_at=%s auth_failed=%s",
                    brand_id,
                    status,
                    consecutive_failures,
                    dead_letter_count,
                    last_successful_sync_at.isoformat() if last_successful_sync_at else "never",
                    auth_failed,
                )

            brands.append({
                "brand_id": brand_id,
                "last_successful_sync_at": last_successful_sync_at.isoformat() if last_successful_sync_at else None,
                "last_sync_attempt_at": last_sync_attempt_at.isoformat() if last_sync_attempt_at else None,
                "last_error_at": last_error_at.isoformat() if last_error_at else None,
                "last_error_message": last_error_message,
                "orders_fetched_last_run": orders_fetched_last_run,
                "orders_written_last_run": orders_written_last_run,
                "dead_letter_count": dead_letter_count,
                "consecutive_failures": consecutive_failures,
                "auth_failed": auth_failed,
                "auth_failed_message": (
                    "LeafLink API key is invalid or expired. Update credentials in brand_api_credentials."
                    if auth_failed else None
                ),
                "status": status,
            })

        logger.info(
            "[SYNC_HEALTH_STATUS] brands=%s overall=%s timestamp=%s",
            len(brands),
            overall_status,
            timestamp,
        )

        return make_json_safe({
            "ok": True,
            "timestamp": timestamp,
            "overall_status": overall_status,
            "brand_count": len(brands),
            "brands": brands,
        })

    except Exception as exc:
        logger.error("[SYNC_HEALTH_STATUS] check_failed error=%s", str(exc)[:300], exc_info=True)
        return {
            "ok": False,
            "timestamp": timestamp,
            "overall_status": "error",
            "brand_count": 0,
            "brands": [],
            "error": str(exc)[:300],
        }


@router.get("/data")
async def health_data(db: AsyncSession = Depends(get_db)):
    """
    Returns data freshness metrics for the opsyn-backend service.

    Response:
      {
        "ok": true,
        "orders_count": <int>,
        "customers_count": <int>,
        "last_sync": "<ISO timestamp | null>",
        "last_error": "<string | null>",
        "data_source": "live" | "error",
        "synced_at": "<ISO timestamp>"
      }

    Never returns empty — always returns counts even if 0.
    """
    synced_at = _utc_now_iso()

    try:
        # Count total orders in DB
        orders_count_result = await db.execute(
            select(func.count(Order.id)).select_from(Order)
        )
        orders_count: int = orders_count_result.scalar() or 0

        # Count distinct customers (derived from orders)
        customers_count_result = await db.execute(
            select(func.count(func.distinct(Order.customer_name))).select_from(Order)
        )
        customers_count: int = customers_count_result.scalar() or 0

        # Most recent credential sync info
        cred_result = await db.execute(
            select(BrandAPICredential)
            .where(BrandAPICredential.is_active == True)
            .order_by(BrandAPICredential.last_sync_at.desc())
            .limit(1)
        )
        cred = cred_result.scalar_one_or_none()

        last_sync = cred.last_sync_at.isoformat() if (cred and cred.last_sync_at) else None
        last_error = cred.last_error if cred else None

        data_source = "live" if (orders_count > 0 or customers_count > 0) else "empty"

        # LeafLink credential health — load from DB per brand (non-blocking)
        leaflink_auth_ok = False
        leaflink_last_success_at = None
        leaflink_last_error = None
        leaflink_sync_state = "missing"
        leaflink_credential_source = "missing"
        leaflink_connected = False
        leaflink_status = "missing"

        try:
            # Find the most recently synced active LeafLink credential across all brands
            ll_cred_result = await db.execute(
                select(BrandAPICredential)
                .where(
                    BrandAPICredential.integration_name == "leaflink",
                    BrandAPICredential.is_active == True,
                )
                .order_by(BrandAPICredential.last_sync_at.desc().nullslast())
                .limit(1)
            )
            ll_cred = ll_cred_result.scalar_one_or_none()

            if ll_cred:
                leaflink_credential_source = "db"
                leaflink_last_success_at = ll_cred.last_sync_at.isoformat() if ll_cred.last_sync_at else None
                leaflink_last_error = ll_cred.last_error

                if ll_cred.sync_status == "ok":
                    leaflink_auth_ok = True
                    leaflink_connected = True
                    leaflink_status = "active"
                    leaflink_sync_state = "healthy"
                elif ll_cred.sync_status == "failed":
                    leaflink_status = "invalid"
                    leaflink_sync_state = "failed"
                else:
                    leaflink_status = "degraded"
                    leaflink_sync_state = "degraded"
            else:
                leaflink_credential_source = "missing"
                leaflink_status = "missing"
                leaflink_sync_state = "missing"

        except Exception as ll_exc:
            logger.warning("[Health] leaflink_credential_check_failed error=%s", ll_exc)
            leaflink_last_error = str(ll_exc)
            leaflink_sync_state = "failed"

        logger.info(
            "[Health] data_endpoint_hit orders=%s customers=%s last_sync=%s leaflink_auth_ok=%s last_error=%s",
            orders_count,
            customers_count,
            last_sync,
            leaflink_auth_ok,
            leaflink_last_error or "none",
        )

        # Integration health summary (non-blocking — failures don't break the endpoint)
        overall_integration_status = "unknown"
        integrations_broken_count = 0
        integrations_stale_count = 0
        integrations_requiring_attention = 0
        top_issue: str | None = None
        opsyn_summary = "Integration status unavailable."

        try:
            integrations = await get_all_integrations_health()
            overall_integration_status = get_overall_status(integrations)
            integrations_broken_count = sum(
                1 for i in integrations if i.status == IntegrationStatus.BROKEN
            )
            integrations_stale_count = sum(
                1 for i in integrations if i.status == IntegrationStatus.STALE
            )
            integrations_requiring_attention = sum(
                1 for i in integrations if i.requires_attention
            )
            opsyn_summary = get_summary(integrations)

            # Top issue: first broken, then stale, then degraded
            for status in (IntegrationStatus.BROKEN, IntegrationStatus.STALE, IntegrationStatus.DEGRADED):
                match = next((i for i in integrations if i.status == status), None)
                if match:
                    top_issue = match.recommended_action or f"{match.integration_name} needs attention"
                    break

        except Exception as ih_exc:
            logger.warning("[Health] integration_health_check_failed error=%s", ih_exc)

        logger.info(
            "[Health] integration_status=%s broken=%s stale=%s",
            overall_integration_status,
            integrations_broken_count,
            integrations_stale_count,
        )

        return make_json_safe({
            "ok": True,
            "orders_count": orders_count,
            "customers_count": customers_count,
            "last_sync": last_sync,
            "last_error": last_error,
            "data_source": data_source,
            "synced_at": synced_at,
            "leaflink_auth_ok": leaflink_auth_ok,
            "leaflink_last_success_at": leaflink_last_success_at,
            "leaflink_last_error": leaflink_last_error,
            "leaflink_sync_state": leaflink_sync_state,
            "leaflink_credential_source": leaflink_credential_source,
            "leaflink_connected": leaflink_connected,
            "leaflink_status": leaflink_status,
            # Integration health summary
            "overall_integration_status": overall_integration_status,
            "integrations_broken_count": integrations_broken_count,
            "integrations_stale_count": integrations_stale_count,
            "integrations_requiring_attention": integrations_requiring_attention,
            "top_issue": top_issue,
            "opsyn_summary": opsyn_summary,
        })

    except Exception as e:
        logger.error("[Health] data_endpoint_error error=%s", e, exc_info=True)

        # Never return empty — always return counts even on error
        return make_json_safe({
            "ok": False,
            "orders_count": 0,
            "customers_count": 0,
            "last_sync": None,
            "last_error": str(e),
            "data_source": "error",
            "synced_at": synced_at,
            "leaflink_auth_ok": False,
            "leaflink_last_success_at": None,
            "leaflink_last_error": str(e),
            "leaflink_sync_state": "failed",
            "leaflink_credential_source": "missing",
            "leaflink_connected": False,
            "leaflink_status": "missing",
            # Integration health summary (fallback values on error)
            "overall_integration_status": "unknown",
            "integrations_broken_count": 0,
            "integrations_stale_count": 0,
            "integrations_requiring_attention": 0,
            "top_issue": None,
            "opsyn_summary": "Health check failed. Check service logs.",
        })
