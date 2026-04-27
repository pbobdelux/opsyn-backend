"""
Integration health tracking service.

Monitors the health of all integrations (LeafLink, CRM, Database, Backend API)
and provides structured health reports with recommended actions.

Safety:
  - Never exposes secrets in responses or logs
  - Caches results for 30 seconds to avoid hammering endpoints
  - Uses timeouts on all external calls
  - Never crashes — always returns a useful response
"""

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Optional

import requests

logger = logging.getLogger("integration_health")

# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class IntegrationStatus(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    BROKEN = "broken"
    STALE = "stale"
    UNKNOWN = "unknown"


class Severity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class IntegrationHealth:
    integration_name: str
    status: IntegrationStatus
    severity: Severity
    last_success_at: Optional[datetime]
    last_attempt_at: Optional[datetime]
    last_error: Optional[str]
    record_count: Optional[int]
    stale_after_minutes: int
    requires_attention: bool
    recommended_action: Optional[str]

    def to_dict(self) -> dict:
        return {
            "integration_name": self.integration_name,
            "status": self.status.value,
            "severity": self.severity.value,
            "last_success_at": self.last_success_at.isoformat() if self.last_success_at else None,
            "last_attempt_at": self.last_attempt_at.isoformat() if self.last_attempt_at else None,
            "last_error": self.last_error,
            "record_count": self.record_count,
            "stale_after_minutes": self.stale_after_minutes,
            "requires_attention": self.requires_attention,
            "recommended_action": self.recommended_action,
        }


# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------

_last_check_at: Optional[datetime] = None
_cached_results: dict = {}          # {"results": list[IntegrationHealth], "ts": float}
_check_in_progress: bool = False

_CACHE_TTL_SECONDS = 30
_STALE_THRESHOLD_MINUTES = 60
_REQUEST_TIMEOUT_SECONDS = 15


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _base_url() -> str:
    """Resolve the backend's own base URL for internal health calls."""
    port = os.getenv("PORT", "8000")
    return os.getenv("BACKEND_BASE_URL", f"http://localhost:{port}")


def _minutes_since(dt: Optional[datetime]) -> Optional[float]:
    if dt is None:
        return None
    now = _utc_now()
    # Ensure both are timezone-aware
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (now - dt).total_seconds() / 60.0


# ---------------------------------------------------------------------------
# Individual integration checks
# ---------------------------------------------------------------------------

def check_leaflink_health() -> IntegrationHealth:
    """
    Check LeafLink integration health by calling the /integrations/leaflink/health
    endpoint and inspecting auth_present / can_fetch_orders.
    """
    now = _utc_now()
    integration_name = "leaflink"

    try:
        url = f"{_base_url()}/integrations/leaflink/health"
        resp = requests.get(url, timeout=_REQUEST_TIMEOUT_SECONDS)

        if resp.status_code == 403:
            logger.warning(
                "[IntegrationHealth] broken_integration integration=%s reason=auth_rejected_403",
                integration_name,
            )
            result = IntegrationHealth(
                integration_name=integration_name,
                status=IntegrationStatus.BROKEN,
                severity=Severity.CRITICAL,
                last_success_at=None,
                last_attempt_at=now,
                last_error="Authentication rejected (403)",
                record_count=None,
                stale_after_minutes=_STALE_THRESHOLD_MINUTES,
                requires_attention=True,
                recommended_action="Reconnect or update LeafLink credentials",
            )
            logger.info("[IntegrationHealth] leaflink status=%s", result.status.value)
            return result

        if not resp.ok:
            logger.warning(
                "[IntegrationHealth] broken_integration integration=%s reason=http_%s",
                integration_name,
                resp.status_code,
            )
            result = IntegrationHealth(
                integration_name=integration_name,
                status=IntegrationStatus.BROKEN,
                severity=Severity.CRITICAL,
                last_success_at=None,
                last_attempt_at=now,
                last_error=f"Health endpoint returned HTTP {resp.status_code}",
                record_count=None,
                stale_after_minutes=_STALE_THRESHOLD_MINUTES,
                requires_attention=True,
                recommended_action="Check LeafLink service availability and credentials",
            )
            logger.info("[IntegrationHealth] leaflink status=%s", result.status.value)
            return result

        data = resp.json()
        auth_present = data.get("auth_present", False)
        can_fetch_orders = data.get("can_fetch_orders", False)
        last_success_raw = data.get("last_success_at")
        last_error = data.get("last_error")

        last_success_at: Optional[datetime] = None
        if last_success_raw:
            try:
                last_success_at = datetime.fromisoformat(last_success_raw)
            except Exception:
                pass

        # Detect 403 auth failure embedded in last_error
        if last_error and ("403" in str(last_error) or "401" in str(last_error)):
            result = IntegrationHealth(
                integration_name=integration_name,
                status=IntegrationStatus.BROKEN,
                severity=Severity.CRITICAL,
                last_success_at=last_success_at,
                last_attempt_at=now,
                last_error="Authentication rejected",
                record_count=None,
                stale_after_minutes=_STALE_THRESHOLD_MINUTES,
                requires_attention=True,
                recommended_action="Reconnect or update LeafLink credentials",
            )
            logger.warning(
                "[IntegrationHealth] broken_integration integration=%s reason=auth_rejected",
                integration_name,
            )
            logger.info("[IntegrationHealth] leaflink status=%s", result.status.value)
            return result

        if not auth_present:
            result = IntegrationHealth(
                integration_name=integration_name,
                status=IntegrationStatus.BROKEN,
                severity=Severity.CRITICAL,
                last_success_at=last_success_at,
                last_attempt_at=now,
                last_error="No authentication credentials present",
                record_count=None,
                stale_after_minutes=_STALE_THRESHOLD_MINUTES,
                requires_attention=True,
                recommended_action="Configure LeafLink API credentials in environment",
            )
            logger.warning(
                "[IntegrationHealth] broken_integration integration=%s reason=no_auth",
                integration_name,
            )
            logger.info("[IntegrationHealth] leaflink status=%s", result.status.value)
            return result

        if auth_present and not can_fetch_orders:
            # Check staleness
            minutes_since_success = _minutes_since(last_success_at)
            if minutes_since_success is not None and minutes_since_success > _STALE_THRESHOLD_MINUTES:
                status = IntegrationStatus.STALE
                severity = Severity.WARNING
                recommended_action = "Trigger manual sync or check LeafLink connection"
                logger.info(
                    "[IntegrationHealth] stale_data_detected integration=%s minutes_since_success=%.1f",
                    integration_name,
                    minutes_since_success,
                )
            else:
                status = IntegrationStatus.DEGRADED
                severity = Severity.WARNING
                recommended_action = "Check upstream service and retry sync"

            result = IntegrationHealth(
                integration_name=integration_name,
                status=status,
                severity=severity,
                last_success_at=last_success_at,
                last_attempt_at=now,
                last_error=last_error,
                record_count=None,
                stale_after_minutes=_STALE_THRESHOLD_MINUTES,
                requires_attention=True,
                recommended_action=recommended_action,
            )
            logger.info("[IntegrationHealth] leaflink status=%s", result.status.value)
            return result

        # Check for stale data even when auth is working
        minutes_since_success = _minutes_since(last_success_at)
        if last_success_at is not None and minutes_since_success is not None and minutes_since_success > _STALE_THRESHOLD_MINUTES:
            result = IntegrationHealth(
                integration_name=integration_name,
                status=IntegrationStatus.STALE,
                severity=Severity.WARNING,
                last_success_at=last_success_at,
                last_attempt_at=now,
                last_error=None,
                record_count=None,
                stale_after_minutes=_STALE_THRESHOLD_MINUTES,
                requires_attention=True,
                recommended_action="Trigger manual sync or check LeafLink connection",
            )
            logger.info(
                "[IntegrationHealth] stale_data_detected integration=%s minutes_since_success=%.1f",
                integration_name,
                minutes_since_success,
            )
            logger.info("[IntegrationHealth] leaflink status=%s", result.status.value)
            return result

        # All good
        result = IntegrationHealth(
            integration_name=integration_name,
            status=IntegrationStatus.HEALTHY,
            severity=Severity.INFO,
            last_success_at=last_success_at,
            last_attempt_at=now,
            last_error=None,
            record_count=None,
            stale_after_minutes=_STALE_THRESHOLD_MINUTES,
            requires_attention=False,
            recommended_action=None,
        )
        logger.info("[IntegrationHealth] leaflink status=%s", result.status.value)
        return result

    except Exception as exc:
        logger.error(
            "[IntegrationHealth] broken_integration integration=%s reason=check_failed",
            integration_name,
        )
        result = IntegrationHealth(
            integration_name=integration_name,
            status=IntegrationStatus.UNKNOWN,
            severity=Severity.WARNING,
            last_success_at=None,
            last_attempt_at=now,
            last_error="Health check failed — service may be unreachable",
            record_count=None,
            stale_after_minutes=_STALE_THRESHOLD_MINUTES,
            requires_attention=True,
            recommended_action="Verify LeafLink service is reachable and credentials are set",
        )
        logger.info("[IntegrationHealth] leaflink status=%s", result.status.value)
        return result


async def check_database_health() -> IntegrationHealth:
    """
    Check database health by testing the connection and counting records.
    Detects stale data if no sync has occurred in 60+ minutes.
    """
    now = _utc_now()
    integration_name = "database"

    try:
        from database import AsyncSessionLocal
        from models import Order, BrandAPICredential
        from sqlalchemy import func, select

        if AsyncSessionLocal is None:
            result = IntegrationHealth(
                integration_name=integration_name,
                status=IntegrationStatus.BROKEN,
                severity=Severity.CRITICAL,
                last_success_at=None,
                last_attempt_at=now,
                last_error="DATABASE_URL not configured",
                record_count=None,
                stale_after_minutes=_STALE_THRESHOLD_MINUTES,
                requires_attention=True,
                recommended_action="Configure DATABASE_URL environment variable",
            )
            logger.warning(
                "[IntegrationHealth] broken_integration integration=%s reason=no_database_url",
                integration_name,
            )
            logger.info("[IntegrationHealth] db status=%s", result.status.value)
            return result

        async with AsyncSessionLocal() as session:
            # Count orders
            orders_result = await session.execute(
                select(func.count(Order.id)).select_from(Order)
            )
            orders_count: int = orders_result.scalar() or 0

            # Count distinct customers
            customers_result = await session.execute(
                select(func.count(func.distinct(Order.customer_name))).select_from(Order)
            )
            customers_count: int = customers_result.scalar() or 0

            # Most recent sync
            cred_result = await session.execute(
                select(BrandAPICredential)
                .where(BrandAPICredential.is_active == True)
                .order_by(BrandAPICredential.last_sync_at.desc())
                .limit(1)
            )
            cred = cred_result.scalar_one_or_none()

        last_sync_at: Optional[datetime] = None
        if cred and cred.last_sync_at:
            last_sync_at = cred.last_sync_at
            if last_sync_at.tzinfo is None:
                last_sync_at = last_sync_at.replace(tzinfo=timezone.utc)

        minutes_since_sync = _minutes_since(last_sync_at)
        record_count = orders_count

        # Determine staleness
        if last_sync_at is not None and minutes_since_sync is not None and minutes_since_sync > _STALE_THRESHOLD_MINUTES:
            result = IntegrationHealth(
                integration_name=integration_name,
                status=IntegrationStatus.STALE,
                severity=Severity.WARNING,
                last_success_at=last_sync_at,
                last_attempt_at=now,
                last_error=None,
                record_count=record_count,
                stale_after_minutes=_STALE_THRESHOLD_MINUTES,
                requires_attention=True,
                recommended_action="Trigger manual sync or check LeafLink connection",
            )
            logger.info(
                "[IntegrationHealth] stale_data_detected integration=%s minutes_since_sync=%.1f",
                integration_name,
                minutes_since_sync,
            )
            logger.info("[IntegrationHealth] db status=%s", result.status.value)
            return result

        result = IntegrationHealth(
            integration_name=integration_name,
            status=IntegrationStatus.HEALTHY,
            severity=Severity.INFO,
            last_success_at=last_sync_at,
            last_attempt_at=now,
            last_error=None,
            record_count=record_count,
            stale_after_minutes=_STALE_THRESHOLD_MINUTES,
            requires_attention=False,
            recommended_action=None,
        )
        logger.info("[IntegrationHealth] db status=%s", result.status.value)
        return result

    except Exception as exc:
        logger.error(
            "[IntegrationHealth] broken_integration integration=%s reason=connection_failed",
            integration_name,
        )
        result = IntegrationHealth(
            integration_name=integration_name,
            status=IntegrationStatus.BROKEN,
            severity=Severity.CRITICAL,
            last_success_at=None,
            last_attempt_at=now,
            last_error="Database connection failed",
            record_count=None,
            stale_after_minutes=_STALE_THRESHOLD_MINUTES,
            requires_attention=True,
            recommended_action="Check database connection and DATABASE_URL configuration",
        )
        logger.info("[IntegrationHealth] db status=%s", result.status.value)
        return result


def check_crm_health() -> IntegrationHealth:
    """
    Check CRM health by calling the /crm/dashboard endpoint.
    Detects stale data and missing records.
    """
    now = _utc_now()
    integration_name = "crm"

    try:
        url = f"{_base_url()}/crm/dashboard"
        resp = requests.get(url, timeout=_REQUEST_TIMEOUT_SECONDS)

        if not resp.ok:
            result = IntegrationHealth(
                integration_name=integration_name,
                status=IntegrationStatus.BROKEN,
                severity=Severity.CRITICAL,
                last_success_at=None,
                last_attempt_at=now,
                last_error=f"CRM dashboard returned HTTP {resp.status_code}",
                record_count=None,
                stale_after_minutes=_STALE_THRESHOLD_MINUTES,
                requires_attention=True,
                recommended_action="Check CRM service availability",
            )
            logger.warning(
                "[IntegrationHealth] broken_integration integration=%s reason=http_%s",
                integration_name,
                resp.status_code,
            )
            logger.info("[IntegrationHealth] crm status=%s", result.status.value)
            return result

        data = resp.json()
        ok = data.get("ok", False)
        customer_count = data.get("customer_count", 0)
        order_count = data.get("order_count", 0)
        last_synced_raw = data.get("last_synced_at")

        if not ok:
            result = IntegrationHealth(
                integration_name=integration_name,
                status=IntegrationStatus.DEGRADED,
                severity=Severity.WARNING,
                last_success_at=None,
                last_attempt_at=now,
                last_error="CRM dashboard reported an error",
                record_count=order_count,
                stale_after_minutes=_STALE_THRESHOLD_MINUTES,
                requires_attention=True,
                recommended_action="Check upstream service and retry sync",
            )
            logger.info("[IntegrationHealth] crm status=%s", result.status.value)
            return result

        last_synced_at: Optional[datetime] = None
        if last_synced_raw:
            try:
                last_synced_at = datetime.fromisoformat(last_synced_raw)
                if last_synced_at.tzinfo is None:
                    last_synced_at = last_synced_at.replace(tzinfo=timezone.utc)
            except Exception:
                pass

        minutes_since_sync = _minutes_since(last_synced_at)

        if last_synced_at is not None and minutes_since_sync is not None and minutes_since_sync > _STALE_THRESHOLD_MINUTES:
            result = IntegrationHealth(
                integration_name=integration_name,
                status=IntegrationStatus.STALE,
                severity=Severity.WARNING,
                last_success_at=last_synced_at,
                last_attempt_at=now,
                last_error=None,
                record_count=order_count,
                stale_after_minutes=_STALE_THRESHOLD_MINUTES,
                requires_attention=True,
                recommended_action="Trigger manual sync or check LeafLink connection",
            )
            logger.info(
                "[IntegrationHealth] stale_data_detected integration=%s minutes_since_sync=%.1f",
                integration_name,
                minutes_since_sync,
            )
            logger.info("[IntegrationHealth] crm status=%s", result.status.value)
            return result

        result = IntegrationHealth(
            integration_name=integration_name,
            status=IntegrationStatus.HEALTHY,
            severity=Severity.INFO,
            last_success_at=last_synced_at,
            last_attempt_at=now,
            last_error=None,
            record_count=order_count,
            stale_after_minutes=_STALE_THRESHOLD_MINUTES,
            requires_attention=False,
            recommended_action=None,
        )
        logger.info("[IntegrationHealth] crm status=%s", result.status.value)
        return result

    except Exception as exc:
        logger.error(
            "[IntegrationHealth] broken_integration integration=%s reason=check_failed",
            integration_name,
        )
        result = IntegrationHealth(
            integration_name=integration_name,
            status=IntegrationStatus.UNKNOWN,
            severity=Severity.WARNING,
            last_success_at=None,
            last_attempt_at=now,
            last_error="CRM health check failed — service may be unreachable",
            record_count=None,
            stale_after_minutes=_STALE_THRESHOLD_MINUTES,
            requires_attention=True,
            recommended_action="Verify CRM service is running and database is connected",
        )
        logger.info("[IntegrationHealth] crm status=%s", result.status.value)
        return result


def check_backend_health() -> IntegrationHealth:
    """
    Check backend API health by calling the /health endpoint.
    Verifies all critical services are up.
    """
    now = _utc_now()
    integration_name = "backend_api"

    try:
        url = f"{_base_url()}/health"
        resp = requests.get(url, timeout=_REQUEST_TIMEOUT_SECONDS)

        if not resp.ok:
            result = IntegrationHealth(
                integration_name=integration_name,
                status=IntegrationStatus.BROKEN,
                severity=Severity.CRITICAL,
                last_success_at=None,
                last_attempt_at=now,
                last_error=f"Backend health endpoint returned HTTP {resp.status_code}",
                record_count=None,
                stale_after_minutes=_STALE_THRESHOLD_MINUTES,
                requires_attention=True,
                recommended_action="Restart backend service and check logs",
            )
            logger.warning(
                "[IntegrationHealth] broken_integration integration=%s reason=http_%s",
                integration_name,
                resp.status_code,
            )
            logger.info("[IntegrationHealth] backend status=%s", result.status.value)
            return result

        data = resp.json()
        ok = data.get("ok", False)
        status_val = data.get("status", "")

        if not ok or status_val != "healthy":
            result = IntegrationHealth(
                integration_name=integration_name,
                status=IntegrationStatus.DEGRADED,
                severity=Severity.WARNING,
                last_success_at=None,
                last_attempt_at=now,
                last_error="Backend reported unhealthy status",
                record_count=None,
                stale_after_minutes=_STALE_THRESHOLD_MINUTES,
                requires_attention=True,
                recommended_action="Check backend service logs for errors",
            )
            logger.info("[IntegrationHealth] backend status=%s", result.status.value)
            return result

        result = IntegrationHealth(
            integration_name=integration_name,
            status=IntegrationStatus.HEALTHY,
            severity=Severity.INFO,
            last_success_at=now,
            last_attempt_at=now,
            last_error=None,
            record_count=None,
            stale_after_minutes=_STALE_THRESHOLD_MINUTES,
            requires_attention=False,
            recommended_action=None,
        )
        logger.info("[IntegrationHealth] backend status=%s", result.status.value)
        return result

    except Exception as exc:
        logger.error(
            "[IntegrationHealth] broken_integration integration=%s reason=check_failed",
            integration_name,
        )
        result = IntegrationHealth(
            integration_name=integration_name,
            status=IntegrationStatus.UNKNOWN,
            severity=Severity.WARNING,
            last_success_at=None,
            last_attempt_at=now,
            last_error="Backend health check failed — service may be unreachable",
            record_count=None,
            stale_after_minutes=_STALE_THRESHOLD_MINUTES,
            requires_attention=True,
            recommended_action="Verify backend service is running",
        )
        logger.info("[IntegrationHealth] backend status=%s", result.status.value)
        return result


# ---------------------------------------------------------------------------
# Aggregate checks
# ---------------------------------------------------------------------------

async def get_all_integrations_health() -> list:
    """
    Check all integrations in parallel (non-blocking).
    Never crashes if one check fails — always returns a full list.
    Caches results for 30 seconds to avoid hammering endpoints.
    """
    global _last_check_at, _cached_results, _check_in_progress

    now = _utc_now()

    # Return cached results if fresh
    if _cached_results and "ts" in _cached_results:
        age = time.monotonic() - _cached_results["ts"]
        if age < _CACHE_TTL_SECONDS:
            return _cached_results["results"]

    # Prevent concurrent checks
    if _check_in_progress:
        if _cached_results and "results" in _cached_results:
            return _cached_results["results"]
        return []

    _check_in_progress = True
    logger.info("[IntegrationHealth] check_start")
    start_time = time.monotonic()

    try:
        loop = asyncio.get_event_loop()

        # Run sync checks in thread pool to avoid blocking the event loop
        leaflink_future = loop.run_in_executor(None, check_leaflink_health)
        crm_future = loop.run_in_executor(None, check_crm_health)
        backend_future = loop.run_in_executor(None, check_backend_health)

        # Run async DB check directly
        db_future = check_database_health()

        results_raw = await asyncio.gather(
            leaflink_future,
            crm_future,
            backend_future,
            db_future,
            return_exceptions=True,
        )

        results: list = []
        names = ["leaflink", "crm", "backend_api", "database"]
        for name, res in zip(names, results_raw):
            if isinstance(res, Exception):
                logger.error(
                    "[IntegrationHealth] broken_integration integration=%s reason=unexpected_exception",
                    name,
                )
                results.append(IntegrationHealth(
                    integration_name=name,
                    status=IntegrationStatus.UNKNOWN,
                    severity=Severity.WARNING,
                    last_success_at=None,
                    last_attempt_at=now,
                    last_error="Health check raised an unexpected error",
                    record_count=None,
                    stale_after_minutes=_STALE_THRESHOLD_MINUTES,
                    requires_attention=True,
                    recommended_action="Check service logs for details",
                ))
            else:
                results.append(res)

        duration = time.monotonic() - start_time
        _last_check_at = now
        _cached_results = {"results": results, "ts": time.monotonic()}

        logger.info("[IntegrationHealth] check_complete duration=%.2fs", duration)
        return results

    except Exception as exc:
        logger.error("[IntegrationHealth] check_complete with_errors error=%s", exc)
        return _cached_results.get("results", [])

    finally:
        _check_in_progress = False


def get_overall_status(integrations: list) -> str:
    """
    Derive the overall system status from a list of IntegrationHealth objects.

    Returns "broken" if any integration is broken, "degraded" if any are
    degraded or stale, and "healthy" if all are healthy.
    """
    statuses = [i.status for i in integrations]

    if IntegrationStatus.BROKEN in statuses:
        overall = "broken"
    elif IntegrationStatus.DEGRADED in statuses or IntegrationStatus.STALE in statuses:
        overall = "degraded"
    elif IntegrationStatus.UNKNOWN in statuses:
        overall = "degraded"
    else:
        overall = "healthy"

    logger.info("[IntegrationHealth] overall_status=%s", overall)
    return overall


async def detect_stale_data(brand_id: str) -> bool:
    """
    Check if orders for a given brand haven't synced in 60+ minutes.
    Returns True if data is stale.
    """
    try:
        from database import AsyncSessionLocal
        from models import BrandAPICredential
        from sqlalchemy import select

        if AsyncSessionLocal is None:
            return False

        async with AsyncSessionLocal() as session:
            cred_result = await session.execute(
                select(BrandAPICredential)
                .where(
                    BrandAPICredential.brand_id == brand_id,
                    BrandAPICredential.is_active == True,
                )
                .order_by(BrandAPICredential.last_sync_at.desc())
                .limit(1)
            )
            cred = cred_result.scalar_one_or_none()

        if cred is None or cred.last_sync_at is None:
            logger.info(
                "[IntegrationHealth] stale_data_detected integration=leaflink brand_id=%s reason=never_synced",
                brand_id,
            )
            return True

        last_sync = cred.last_sync_at
        if last_sync.tzinfo is None:
            last_sync = last_sync.replace(tzinfo=timezone.utc)

        minutes_since = _minutes_since(last_sync)
        if minutes_since is not None and minutes_since > _STALE_THRESHOLD_MINUTES:
            logger.info(
                "[IntegrationHealth] stale_data_detected integration=leaflink brand_id=%s minutes_since_sync=%.1f",
                brand_id,
                minutes_since,
            )
            return True

        return False

    except Exception as exc:
        logger.error(
            "[IntegrationHealth] stale_data_check_failed brand_id=%s error=%s",
            brand_id,
            exc,
        )
        return False


# ---------------------------------------------------------------------------
# Plain-language summaries
# ---------------------------------------------------------------------------

def get_summary(integrations: list) -> str:
    """
    Generate a human-readable summary of the overall integration health.
    Suitable for display in dashboards and AI context.
    """
    broken = [i for i in integrations if i.status == IntegrationStatus.BROKEN]
    stale = [i for i in integrations if i.status == IntegrationStatus.STALE]
    degraded = [i for i in integrations if i.status == IntegrationStatus.DEGRADED]
    unknown = [i for i in integrations if i.status == IntegrationStatus.UNKNOWN]

    if broken:
        action = broken[0].recommended_action or "Check integration configuration."
        return f"{broken[0].integration_name} is broken. {action}"
    if stale:
        action = stale[0].recommended_action or "Trigger a manual sync."
        return f"{stale[0].integration_name} data is stale. {action}"
    if degraded:
        action = degraded[0].recommended_action or "Check upstream service."
        return f"{degraded[0].integration_name} is degraded. {action}"
    if unknown:
        return f"{unknown[0].integration_name} status is unknown. Check service logs."
    return "All integrations healthy."
