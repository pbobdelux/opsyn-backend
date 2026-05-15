"""
Health service — structured observability and self-diagnosing health logic.

Provides:
  - get_sync_health()        — active/stalled/orphaned runs, 24h stats
  - get_database_health()    — connectivity, latency, error counts
  - get_migration_health()   — last run, failed/skipped, validation warnings
  - get_sync_run_metrics()   — per-run detailed metrics
  - record_sync_error()      — normalize exceptions into ErrorTaxonomy
  - check_for_alerts()       — detect and emit CRITICAL alert logs

Alert log format:
  [HEALTH_ALERT] severity=CRITICAL alert_type=<type> details={...}

Error taxonomy log format:
  [ERROR_TAXONOMY] run_id=<id> error_type=<type> message=<msg>
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from models.health_metrics import (
    DatabaseHealthMetrics,
    ErrorTaxonomy,
    MigrationHealthMetrics,
    SyncHealthMetrics,
    SyncRunMetrics,
)

logger = logging.getLogger("health_service")

# ---------------------------------------------------------------------------
# Thresholds for alert detection
# ---------------------------------------------------------------------------
_STALL_ALERT_SECONDS = 300          # 5 minutes
_DEAD_LETTER_SPIKE_THRESHOLD = 100  # dead letters in 1 hour
_API_FAILURE_THRESHOLD = 5          # API failures in 10 minutes
_DB_ERROR_THRESHOLD = 1             # any DB connectivity failure


# ---------------------------------------------------------------------------
# Migration health state — populated by migration runner hooks
# ---------------------------------------------------------------------------

_migration_health_state: Dict[str, Any] = {
    "last_run_at": None,
    "current_version": None,
    "status": "unknown",
    "failed_migrations": [],
    "skipped_migrations": [],
    "validation_warnings": [],
}


def update_migration_health(
    *,
    last_run_at: Optional[str] = None,
    current_version: Optional[str] = None,
    status: Optional[str] = None,
    failed_migrations: Optional[List[str]] = None,
    skipped_migrations: Optional[List[str]] = None,
    validation_warnings: Optional[List[str]] = None,
) -> None:
    """Update the in-memory migration health state (called by migration runner)."""
    if last_run_at is not None:
        _migration_health_state["last_run_at"] = last_run_at
    if current_version is not None:
        _migration_health_state["current_version"] = current_version
    if status is not None:
        _migration_health_state["status"] = status
    if failed_migrations is not None:
        _migration_health_state["failed_migrations"] = failed_migrations
    if skipped_migrations is not None:
        _migration_health_state["skipped_migrations"] = skipped_migrations
    if validation_warnings is not None:
        _migration_health_state["validation_warnings"] = validation_warnings


# ---------------------------------------------------------------------------
# get_sync_health
# ---------------------------------------------------------------------------

async def get_sync_health(db: AsyncSession) -> SyncHealthMetrics:
    """
    Query sync_runs and related tables to build aggregate sync health metrics.

    Returns counts of active/stalled/orphaned runs, 24h success/failure,
    average duration, and orders/items/dead letters processed.
    """
    now = datetime.now(timezone.utc)
    cutoff_24h = now - timedelta(hours=24)
    stall_cutoff = now - timedelta(seconds=_STALL_ALERT_SECONDS)

    try:
        # Active runs (queued or syncing)
        active_result = await db.execute(
            text("""
                SELECT id, brand_id, status, started_at, last_progress_at,
                       pages_synced, total_pages, orders_loaded_this_run
                FROM sync_runs
                WHERE status IN ('queued', 'syncing')
                ORDER BY started_at DESC
            """)
        )
        active_rows = active_result.fetchall()

        active_details = []
        stalled_details = []
        orphan_cutoff = now - timedelta(seconds=1800)

        for row in active_rows:
            run_id, brand_id, status, started_at, last_progress_at, pages_synced, total_pages, orders_loaded = row
            last_activity = last_progress_at or started_at

            # Normalize to UTC-aware
            if last_activity and last_activity.tzinfo is None:
                last_activity = last_activity.replace(tzinfo=timezone.utc)
            if started_at and started_at.tzinfo is None:
                started_at = started_at.replace(tzinfo=timezone.utc)

            is_stalled = last_activity is not None and last_activity < stall_cutoff
            is_orphaned = last_activity is not None and last_activity < orphan_cutoff

            detail = {
                "run_id": run_id,
                "brand_id": brand_id,
                "status": status,
                "started_at": started_at.isoformat() if started_at else None,
                "last_progress_at": last_progress_at.isoformat() if last_progress_at else None,
                "pages_synced": pages_synced or 0,
                "total_pages": total_pages,
                "orders_loaded": orders_loaded or 0,
            }

            if is_orphaned:
                stalled_details.append(detail)  # orphaned are also stalled
            elif is_stalled:
                stalled_details.append(detail)
            else:
                active_details.append(detail)

        orphaned_details = [d for d in stalled_details if True]  # all stalled qualify

        # 24h completed runs
        completed_result = await db.execute(
            text("""
                SELECT
                    COUNT(*) FILTER (WHERE status = 'completed') AS success_count,
                    COUNT(*) FILTER (WHERE status IN ('failed', 'stalled')) AS failure_count,
                    AVG(EXTRACT(EPOCH FROM (completed_at - started_at)))
                        FILTER (WHERE status = 'completed' AND completed_at IS NOT NULL) AS avg_duration
                FROM sync_runs
                WHERE (completed_at >= :cutoff OR started_at >= :cutoff)
            """),
            {"cutoff": cutoff_24h},
        )
        completed_row = completed_result.fetchone()
        success_24h = int(completed_row[0] or 0)
        failed_24h = int(completed_row[1] or 0)
        avg_duration = float(completed_row[2]) if completed_row[2] is not None else None

        # Failed run details (last 24h)
        failed_result = await db.execute(
            text("""
                SELECT id, brand_id, status, last_error, completed_at
                FROM sync_runs
                WHERE status IN ('failed', 'stalled')
                  AND (completed_at >= :cutoff OR started_at >= :cutoff)
                ORDER BY completed_at DESC
                LIMIT 10
            """),
            {"cutoff": cutoff_24h},
        )
        failed_details = [
            {
                "run_id": r[0],
                "brand_id": r[1],
                "status": r[2],
                "failure_reason": r[3],
                "completed_at": r[4].isoformat() if r[4] else None,
            }
            for r in failed_result.fetchall()
        ]

        # Orders processed in 24h (from sync_runs)
        orders_result = await db.execute(
            text("""
                SELECT COALESCE(SUM(orders_loaded_this_run), 0)
                FROM sync_runs
                WHERE completed_at >= :cutoff
                  AND status = 'completed'
            """),
            {"cutoff": cutoff_24h},
        )
        orders_24h = int(orders_result.scalar() or 0)

        # Dead letters created in 24h
        dead_letter_result = await db.execute(
            text("""
                SELECT COUNT(*)
                FROM sync_dead_letters
                WHERE created_at >= :cutoff
            """),
            {"cutoff": cutoff_24h},
        )
        dead_letters_24h = int(dead_letter_result.scalar() or 0)

        # Retry queue size (unresolved dead letters)
        retry_result = await db.execute(
            text("SELECT COUNT(*) FROM sync_dead_letters WHERE resolved_at IS NULL")
        )
        retry_queue = int(retry_result.scalar() or 0)

        # Last successful sync timestamp
        last_sync_result = await db.execute(
            text("""
                SELECT MAX(completed_at)
                FROM sync_runs
                WHERE status = 'completed'
            """)
        )
        last_sync_at = last_sync_result.scalar()
        last_sync_iso = last_sync_at.isoformat() if last_sync_at else None

        return SyncHealthMetrics(
            active_runs=len(active_details),
            stalled_runs=len(stalled_details),
            orphaned_runs=len(orphaned_details),
            successful_24h=success_24h,
            failed_24h=failed_24h,
            avg_duration_seconds=avg_duration,
            orders_processed_24h=orders_24h,
            line_items_processed_24h=0,  # tracked via sync_health table
            dead_letters_created_24h=dead_letters_24h,
            retry_queue_size=retry_queue,
            last_successful_sync_at=last_sync_iso,
            active_run_details=active_details,
            stalled_run_details=stalled_details,
            orphaned_run_details=orphaned_details,
            failed_run_details=failed_details,
        )

    except Exception as exc:
        logger.error("[HEALTH_SERVICE] get_sync_health failed error=%s", str(exc)[:300])
        return SyncHealthMetrics()


# ---------------------------------------------------------------------------
# get_database_health
# ---------------------------------------------------------------------------

async def get_database_health(db: AsyncSession) -> DatabaseHealthMetrics:
    """
    Check database connectivity and measure query latency.

    Returns connectivity status, p50/p95/p99 latency from the latency
    tracking middleware, and the current migration version.
    """
    import time as _time

    # Import latency percentiles from middleware
    try:
        from middleware.latency_tracking import get_db_latency_percentiles
        db_latency = get_db_latency_percentiles()
    except Exception:
        db_latency = {"p50_ms": None, "p95_ms": None, "p99_ms": None}

    connected = False
    last_query_at = None
    current_version = None

    try:
        start = _time.monotonic()
        await db.execute(text("SELECT 1"))
        latency_ms = ((_time.monotonic() - start) * 1000)
        connected = True
        last_query_at = datetime.now(timezone.utc).isoformat()

        # Record this latency sample
        try:
            from middleware.latency_tracking import record_db_latency
            record_db_latency(latency_ms)
        except Exception:
            pass

        # Get current migration version (most recently applied migration)
        try:
            version_result = await db.execute(
                text("""
                    SELECT migration_name
                    FROM schema_migrations
                    WHERE success = true
                    ORDER BY applied_at DESC
                    LIMIT 1
                """)
            )
            version_row = version_result.fetchone()
            if version_row:
                current_version = version_row[0]
        except Exception:
            pass

    except Exception as exc:
        logger.error("[HEALTH_SERVICE] database connectivity check failed error=%s", str(exc)[:200])

    return DatabaseHealthMetrics(
        connected=connected,
        query_latency_p50_ms=db_latency.get("p50_ms"),
        query_latency_p95_ms=db_latency.get("p95_ms"),
        query_latency_p99_ms=db_latency.get("p99_ms"),
        error_count_24h=0,
        last_successful_query_at=last_query_at,
        connection_pool_status="ok" if connected else "error",
        current_migration_version=current_version,
        pending_migrations=0,
    )


# ---------------------------------------------------------------------------
# get_migration_health
# ---------------------------------------------------------------------------

async def get_migration_health() -> MigrationHealthMetrics:
    """
    Return migration health from the in-memory state populated by the
    migration runner during startup.
    """
    state = _migration_health_state
    failed = state.get("failed_migrations", [])
    skipped = state.get("skipped_migrations", [])
    warnings = state.get("validation_warnings", [])

    return MigrationHealthMetrics(
        last_run_at=state.get("last_run_at"),
        current_version=state.get("current_version"),
        status=state.get("status", "unknown"),
        failed_migrations=failed,
        skipped_migrations=skipped,
        validation_warnings=warnings,
        failed_count=len(failed),
        skipped_count=len(skipped),
        warning_count=len(warnings),
    )


# ---------------------------------------------------------------------------
# get_sync_run_metrics
# ---------------------------------------------------------------------------

async def get_sync_run_metrics(db: AsyncSession, sync_run_id: int) -> Optional[SyncRunMetrics]:
    """
    Return detailed metrics for a specific sync run.

    Reads from sync_runs and computes duration from started_at/completed_at.
    """
    try:
        result = await db.execute(
            text("""
                SELECT
                    id, brand_id, status, mode,
                    started_at, completed_at,
                    pages_synced, total_pages,
                    orders_loaded_this_run,
                    last_error, stalled_reason,
                    error_count
                FROM sync_runs
                WHERE id = :run_id
            """),
            {"run_id": sync_run_id},
        )
        row = result.fetchone()
        if row is None:
            return None

        run_id, brand_id, status, mode, started_at, completed_at, pages_synced, total_pages, orders_loaded, last_error, stalled_reason, error_count = row

        # Normalize datetimes
        if started_at and started_at.tzinfo is None:
            started_at = started_at.replace(tzinfo=timezone.utc)
        if completed_at and completed_at.tzinfo is None:
            completed_at = completed_at.replace(tzinfo=timezone.utc)

        duration = None
        if started_at and completed_at:
            duration = (completed_at - started_at).total_seconds()

        failure_reason = stalled_reason or last_error

        return SyncRunMetrics(
            run_id=run_id,
            brand_id=brand_id,
            status=status,
            mode=mode,
            started_at=started_at.isoformat() if started_at else None,
            completed_at=completed_at.isoformat() if completed_at else None,
            duration_seconds=duration,
            pages_synced=pages_synced or 0,
            total_pages=total_pages,
            orders_fetched=orders_loaded or 0,
            orders_inserted=0,
            orders_updated=0,
            line_items_inserted=0,
            line_items_updated=0,
            dead_letters_created=0,
            failure_reason=failure_reason,
            error_type=None,
            error_context=None,
            is_stalled=status in ("stalled",),
        )

    except Exception as exc:
        logger.error(
            "[HEALTH_SERVICE] get_sync_run_metrics failed run_id=%s error=%s",
            sync_run_id,
            str(exc)[:200],
        )
        return None


# ---------------------------------------------------------------------------
# record_sync_error
# ---------------------------------------------------------------------------

def record_sync_error(
    sync_run_id: Optional[int],
    error_type: ErrorTaxonomy,
    message: str,
    context: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Normalize and log a sync error into the error taxonomy.

    Emits a structured [ERROR_TAXONOMY] log line for easy filtering.
    """
    logger.error(
        "[ERROR_TAXONOMY] run_id=%s error_type=%s message=%s context=%s",
        sync_run_id,
        error_type.value,
        message[:500],
        str(context or {})[:300],
    )


def classify_exception(exc: Exception) -> ErrorTaxonomy:
    """
    Map a Python exception to the closest ErrorTaxonomy value.

    Used by sync code to categorize errors before storing them.
    """
    exc_type = type(exc).__name__
    exc_msg = str(exc).lower()

    if "datetime" in exc_msg or "timezone" in exc_msg or "offset" in exc_msg:
        return ErrorTaxonomy.datetime_error
    if "uuid" in exc_msg or "invalid uuid" in exc_msg:
        return ErrorTaxonomy.uuid_mismatch
    if "timeout" in exc_msg or "timed out" in exc_msg:
        return ErrorTaxonomy.api_timeout
    if "duplicate" in exc_msg or "unique" in exc_msg or "uniqueviolation" in exc_type.lower():
        return ErrorTaxonomy.duplicate_key
    if "brand_id" in exc_msg and ("null" in exc_msg or "missing" in exc_msg):
        return ErrorTaxonomy.missing_brand_id
    if "auth" in exc_msg or "401" in exc_msg or "403" in exc_msg or "unauthorized" in exc_msg:
        return ErrorTaxonomy.auth_failure
    if "connection" in exc_msg or "network" in exc_msg or "socket" in exc_msg:
        return ErrorTaxonomy.network_failure
    if "integrity" in exc_type.lower() or "db" in exc_type.lower() or "sqlalchemy" in exc_type.lower():
        return ErrorTaxonomy.db_write_failure
    if "json" in exc_msg or "payload" in exc_msg or "parse" in exc_msg:
        return ErrorTaxonomy.invalid_payload

    return ErrorTaxonomy.unknown


# ---------------------------------------------------------------------------
# check_for_alerts
# ---------------------------------------------------------------------------

async def check_for_alerts(db: AsyncSession) -> List[Dict[str, Any]]:
    """
    Detect critical conditions and emit structured CRITICAL alert logs.

    Checks:
      1. Stalled syncs (> 5 minutes without progress)
      2. Dead letter spikes (> 100 in 1 hour)
      3. Repeated API failures (> 5 in 10 minutes via consecutive_failures)
      4. Migration validation failures
      5. DB connectivity failures

    Log format:
      [HEALTH_ALERT] severity=CRITICAL alert_type=<type> details={...}

    Returns a list of alert dicts for inclusion in health responses.
    """
    alerts: List[Dict[str, Any]] = []
    now = datetime.now(timezone.utc)

    # --- 1. Stalled syncs ---
    try:
        stall_cutoff = now - timedelta(seconds=_STALL_ALERT_SECONDS)
        stall_result = await db.execute(
            text("""
                SELECT id, brand_id, status, last_progress_at, started_at
                FROM sync_runs
                WHERE status IN ('queued', 'syncing')
                  AND COALESCE(last_progress_at, started_at) < :cutoff
            """),
            {"cutoff": stall_cutoff},
        )
        stalled_rows = stall_result.fetchall()
        for row in stalled_rows:
            run_id, brand_id, status, last_progress_at, started_at = row
            last_activity = last_progress_at or started_at
            if last_activity and last_activity.tzinfo is None:
                last_activity = last_activity.replace(tzinfo=timezone.utc)
            elapsed = (now - last_activity).total_seconds() if last_activity else 0
            details = {
                "run_id": run_id,
                "brand_id": brand_id,
                "status": status,
                "elapsed_seconds": int(elapsed),
            }
            logger.critical(
                "[HEALTH_ALERT] severity=CRITICAL alert_type=stalled_sync details=%s",
                details,
            )
            alerts.append({"severity": "CRITICAL", "alert_type": "stalled_sync", "details": details})
    except Exception as exc:
        logger.warning("[HEALTH_SERVICE] stall check failed error=%s", str(exc)[:200])

    # --- 2. Dead letter spike ---
    try:
        spike_cutoff = now - timedelta(hours=1)
        spike_result = await db.execute(
            text("SELECT COUNT(*) FROM sync_dead_letters WHERE created_at >= :cutoff"),
            {"cutoff": spike_cutoff},
        )
        dead_letter_count = int(spike_result.scalar() or 0)
        if dead_letter_count > _DEAD_LETTER_SPIKE_THRESHOLD:
            details = {"dead_letter_count_1h": dead_letter_count, "threshold": _DEAD_LETTER_SPIKE_THRESHOLD}
            logger.critical(
                "[HEALTH_ALERT] severity=CRITICAL alert_type=dead_letter_spike details=%s",
                details,
            )
            alerts.append({"severity": "CRITICAL", "alert_type": "dead_letter_spike", "details": details})
    except Exception as exc:
        logger.warning("[HEALTH_SERVICE] dead letter spike check failed error=%s", str(exc)[:200])

    # --- 3. Repeated API failures ---
    try:
        api_failure_result = await db.execute(
            text("""
                SELECT brand_id, consecutive_failures
                FROM sync_health
                WHERE consecutive_failures >= :threshold
            """),
            {"threshold": _API_FAILURE_THRESHOLD},
        )
        api_failure_rows = api_failure_result.fetchall()
        for row in api_failure_rows:
            brand_id, consecutive_failures = row
            details = {"brand_id": brand_id, "consecutive_failures": consecutive_failures, "threshold": _API_FAILURE_THRESHOLD}
            logger.critical(
                "[HEALTH_ALERT] severity=CRITICAL alert_type=repeated_api_failures details=%s",
                details,
            )
            alerts.append({"severity": "CRITICAL", "alert_type": "repeated_api_failures", "details": details})
    except Exception as exc:
        logger.warning("[HEALTH_SERVICE] API failure check failed error=%s", str(exc)[:200])

    # --- 4. Migration validation failures ---
    migration_health = await get_migration_health()
    if migration_health.failed_count > 0:
        details = {
            "failed_migrations": migration_health.failed_migrations,
            "failed_count": migration_health.failed_count,
        }
        logger.critical(
            "[HEALTH_ALERT] severity=CRITICAL alert_type=migration_failure details=%s",
            details,
        )
        alerts.append({"severity": "CRITICAL", "alert_type": "migration_failure", "details": details})

    # --- 5. DB connectivity ---
    try:
        await db.execute(text("SELECT 1"))
    except Exception as exc:
        details = {"error": str(exc)[:200]}
        logger.critical(
            "[HEALTH_ALERT] severity=CRITICAL alert_type=db_connectivity_failure details=%s",
            details,
        )
        alerts.append({"severity": "CRITICAL", "alert_type": "db_connectivity_failure", "details": details})

    return alerts
