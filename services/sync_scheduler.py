"""
sync_scheduler.py — SyncRun-based background sync worker.

Polls the `sync_runs` table for queued or syncing jobs, claims them, fetches
the brand's LeafLink credentials, and drives the full page-by-page sync via
sync_leaflink_background_continuous().

Replaces the old HTTP-based scheduler that POSTed to /sync/leaflink/run.

Environment variables:
  DATABASE_URL          — PostgreSQL connection string (required)
  POLL_INTERVAL_SECONDS — seconds to sleep when no jobs are found (default 5)
  WORKER_ID             — unique identifier for this worker instance (default "worker-1")
  STALL_THRESHOLD_SECONDS — seconds without progress before marking a job stalled (default 90)
"""

import asyncio
import logging
import os
import sys
import urllib.parse
from datetime import datetime, timezone

# Force unbuffered output so logs are never lost on crash
os.environ["PYTHONUNBUFFERED"] = "1"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("opsyn-sync-worker")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "5"))
WORKER_ID = os.getenv("WORKER_ID", "worker-1")
STALL_THRESHOLD_SECONDS = int(os.getenv("STALL_THRESHOLD_SECONDS", "90"))

# ---------------------------------------------------------------------------
# Incremental sync configuration
# ---------------------------------------------------------------------------
# LEAFLINK_INCREMENTAL_LOOKBACK_MINUTES: how far back to look for changed orders.
# Default 62 minutes provides a 2-minute buffer over the 60-minute sync interval
# to avoid race conditions where orders updated near the boundary are missed.
# Minimum safe value is 30 minutes (half the typical sync interval).
LEAFLINK_INCREMENTAL_LOOKBACK_MINUTES = int(
    os.getenv("LEAFLINK_INCREMENTAL_LOOKBACK_MINUTES", "62")
)
LEAFLINK_INCREMENTAL_SYNC_INTERVAL_SECONDS = int(
    os.getenv("LEAFLINK_INCREMENTAL_SYNC_INTERVAL_SECONDS", "30")
)

# Safety check: warn if lookback window is dangerously short
if LEAFLINK_INCREMENTAL_LOOKBACK_MINUTES < 30:
    logger.warning(
        "[INCREMENTAL_LOOKBACK_TOO_SHORT] LEAFLINK_INCREMENTAL_LOOKBACK_MINUTES=%d "
        "is below the minimum safe value of 30. Orders updated near sync boundaries "
        "may be missed. Recommend setting to at least 62 minutes.",
        LEAFLINK_INCREMENTAL_LOOKBACK_MINUTES,
    )
else:
    logger.info(
        "[INCREMENTAL_SYNC_ENQUEUED] lookback_minutes=%d interval_seconds=%d "
        "safety=ok (min_safe=30)",
        LEAFLINK_INCREMENTAL_LOOKBACK_MINUTES,
        LEAFLINK_INCREMENTAL_SYNC_INTERVAL_SECONDS,
    )

# ---------------------------------------------------------------------------
# Bootstrap: ensure the repo root is on sys.path so shared modules resolve.
# The scheduler lives in services/ which is one level below the root.
# ---------------------------------------------------------------------------
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


# ============================================================================
# STARTUP DIAGNOSTICS: Log DATABASE_URL before any connection attempt
# ============================================================================
import urllib.parse

logger.info("[STARTUP] Checking environment variables...")
_db_url = os.getenv("DATABASE_URL")
_db_public_url = os.getenv("DATABASE_PUBLIC_URL")
_postgres_url = os.getenv("POSTGRES_URL")
_async_db_url = os.getenv("ASYNC_DATABASE_URL")

logger.info("[STARTUP] DATABASE_URL set: %s", _db_url is not None)
logger.info("[STARTUP] DATABASE_PUBLIC_URL set: %s", _db_public_url is not None)
logger.info("[STARTUP] POSTGRES_URL set: %s", _postgres_url is not None)
logger.info("[STARTUP] ASYNC_DATABASE_URL set: %s", _async_db_url is not None)

_db_url = os.getenv("DATABASE_URL", "")
logger.info("[STARTUP] DATABASE_URL raw length: %d", len(_db_url))

if _db_url:
    try:
        _parsed = urllib.parse.urlparse(_db_url)
        logger.info(
            "[STARTUP] PARSED driver=%s host=%s port=%s database=%s password_exists=%s",
            _parsed.scheme,
            _parsed.hostname,
            _parsed.port,
            _parsed.path.lstrip("/"),
            _parsed.password is not None,
        )
    except Exception as e:
        logger.error("[STARTUP] Failed to parse DATABASE_URL: %s", e)
else:
    logger.error("[STARTUP] DATABASE_URL is empty or not set")

sys.stdout.flush()
# ============================================================================


# =============================================================================
# SYNC WORKER BOOTSTRAP & DATABASE INITIALIZATION
# =============================================================================
# The sync worker runs independently from the backend main.py, so it must
# perform its own bootstrap schema recovery and database initialization.
# This code runs at module import time to ensure the database is ready
# before run_scheduler() is called.
# =============================================================================

async def _sync_worker_bootstrap() -> None:
    """Execute bootstrap schema recovery for the sync worker."""
    logger.info("[SYNC_WORKER_BOOTSTRAP_START] starting bootstrap schema recovery")

    # Validate DATABASE_URL
    database_url = os.getenv("DATABASE_URL", "")
    if not database_url:
        logger.error("[SYNC_WORKER_BOOTSTRAP_START] DATABASE_URL not set — cannot start")
        sys.exit(1)

    logger.info("[SYNC_WORKER_BOOTSTRAP_START] DATABASE_URL validated")

    # Create minimal bootstrap engine
    from sqlalchemy.ext.asyncio import create_async_engine

    def _normalize_url(url: str) -> str:
        """Minimal URL normalizer."""
        url = url.strip()
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql+asyncpg://", 1)
        elif url.startswith("postgresql://"):
            url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
        from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
        parsed = urlparse(url)
        filtered = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True)
                    if k.lower() != "sslmode"]
        return urlunparse(parsed._replace(query=urlencode(filtered)))

    bootstrap_url = _normalize_url(database_url)
    bootstrap_engine = create_async_engine(
        bootstrap_url,
        echo=False,
        pool_pre_ping=True,
        pool_size=20,
        max_overflow=40,
        pool_timeout=60,
        pool_recycle=1800,
        connect_args={"ssl": "require"},
        execution_options={"compiled_cache": None},
    )

    try:
        # Run bootstrap schema recovery
        from services.bootstrap_schema_recovery import bootstrap_schema_recovery
        result = await bootstrap_schema_recovery(bootstrap_engine)

        if not result["ok"]:
            logger.error(
                "[SYNC_WORKER_BOOTSTRAP_FAILED] bootstrap recovery failed errors=%s",
                result["errors"],
            )
            sys.exit(1)

        logger.info(
            "[SYNC_WORKER_BOOTSTRAP_SUCCESS] columns_added=%s tables_created=%s",
            result["columns_added"],
            result["tables_created"],
        )
    finally:
        await bootstrap_engine.dispose()


async def _sync_worker_init_database() -> None:
    """Initialize database engine for the sync worker."""
    logger.info("[SYNC_WORKER_DB_INIT_START] initializing database engine")

    try:
        from database import _initialize_engine
        await _initialize_engine()
        logger.info("[SYNC_WORKER_DB_INIT_SUCCESS] database engine initialized")
    except Exception as e:
        logger.error(
            "[SYNC_WORKER_DB_INIT_FAILED] database initialization failed error=%s",
            str(e)[:500],
        )
        sys.exit(1)


async def _verify_db_connection() -> None:
    """Verify database connection is working."""
    try:
        from database import get_async_session_local
        AsyncSessionLocal = get_async_session_local()
        async with AsyncSessionLocal() as db:
            from sqlalchemy import select
            await db.execute(select(1))
        logger.info("[SYNC_WORKER_DB_VERIFY] connection test passed")
    except Exception as e:
        logger.error("[SYNC_WORKER_DB_VERIFY_FAILED] connection test failed error=%s", str(e)[:200])
        raise


async def worker_main() -> None:
    """Main entry point for sync worker - runs all async operations in one loop."""
    logger.info("[SYNC_WORKER_MAIN_START] sync worker starting")

    try:
        # Phase 1: Bootstrap schema recovery
        logger.info("[SYNC_WORKER_BOOTSTRAP_START] running bootstrap schema recovery")
        await _sync_worker_bootstrap()
        logger.info("[SYNC_WORKER_BOOTSTRAP_COMPLETE] bootstrap schema recovery complete")

        # Phase 2: Initialize database engine
        logger.info("[SYNC_WORKER_DB_INIT_START] initializing database engine")
        await _sync_worker_init_database()
        logger.info("[SYNC_WORKER_DB_INIT_COMPLETE] database engine initialized")

        # Phase 2.5: Validate and log engine pool configuration
        try:
            from database import get_engine, get_engine_id, validate_single_engine
            _eng = get_engine()
            _pool = _eng.pool
            _ps = _pool.size()
            _mo = _pool._max_overflow
            _eid = get_engine_id()
            logger.info(
                "[DB_ENGINE_VALIDATION] engine_id=%s pool_size=%d max_overflow=%d "
                "total_capacity=%d pool_timeout=%s pool_recycle=%s pool_pre_ping=true "
                "database_url_source=DATABASE_URL",
                _eid,
                _ps,
                _mo,
                _ps + _mo,
                getattr(_pool, "_timeout", "unknown"),
                getattr(_pool, "_recycle", "unknown"),
            )
            logger.info(
                "[DB_ENGINE_CONFIG] pool_size=%d max_overflow=%d total_capacity=%d "
                "pool_timeout=%s pool_recycle=%s pool_pre_ping=true "
                "database_url_source=DATABASE_URL",
                _ps,
                _mo,
                _ps + _mo,
                getattr(_pool, "_timeout", "unknown"),
                getattr(_pool, "_recycle", "unknown"),
            )
            logger.info("[DB_POOL_CAPACITY] total_capacity=%d", _ps + _mo)

            assert _ps == 20, (
                f"[DB_ENGINE_CONFIG_INVALID] pool_size={_ps} expected=20 — "
                "engine was created with wrong pool settings. "
                "Ensure initialize_database_after_bootstrap() is the only engine creation path."
            )
            assert _mo == 40, (
                f"[DB_ENGINE_CONFIG_INVALID] max_overflow={_mo} expected=40 — "
                "engine was created with wrong pool settings. "
                "Ensure initialize_database_after_bootstrap() is the only engine creation path."
            )
            logger.info(
                "[DB_ENGINE_CONFIG_VALIDATED] engine_id=%s pool_size=20 max_overflow=40 — "
                "pool configuration is correct",
                _eid,
            )

            # Validate singleton — raises RuntimeError if a second engine was created
            await validate_single_engine()

        except AssertionError:
            raise
        except Exception as _cfg_exc:
            logger.warning("[DB_ENGINE_CONFIG] failed to read pool config: %s", _cfg_exc)

        # Phase 3: Verify database connection
        logger.info("[SYNC_WORKER_DB_VERIFY_START] verifying database connection")
        await _verify_db_connection()
        logger.info("[SYNC_WORKER_DB_VERIFIED] database connection verified")

        # Phase 4: Startup stale job recovery
        logger.info("[SYNC_WORKER_STARTUP_RECOVERY_START] running startup stale job recovery")
        recovered_count = await recover_stale_sync_jobs()
        logger.info(
            "[STARTUP_RECOVERY_COMPLETE] recovered_stale_jobs=%d",
            recovered_count,
        )

        # Phase 5: Start scheduler
        logger.info("[SYNC_WORKER_SCHEDULER_START] starting scheduler")
        await run_scheduler()

    except Exception as e:
        logger.error(
            "[SYNC_WORKER_FATAL] fatal error in worker main error=%s",
            str(e)[:500],
            exc_info=True,
        )
        # Ensure engine is disposed on fatal error
        try:
            from database import _engine
            if _engine is not None:
                await _engine.dispose()
                logger.info("[SYNC_WORKER_FATAL] engine disposed")
        except Exception as dispose_exc:
            logger.error("[SYNC_WORKER_FATAL] failed to dispose engine error=%s", dispose_exc)
        raise


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------


async def validate_schema() -> None:
    """Validate that required columns exist in database schema.

    Checks the orders and order_lines tables for required columns and
    raises ValueError if any are missing or if invalid columns (like
    pulled_qty) are present.
    """
    from database import get_async_session_local
    from sqlalchemy import text
    AsyncSessionLocal = get_async_session_local()

    try:
        async with AsyncSessionLocal() as db:
            # Check orders table
            result = await db.execute(
                text("SELECT column_name FROM information_schema.columns WHERE table_name='orders'")
            )
            orders_columns = {row[0] for row in result.fetchall()}

            # Only require columns that are NOT nullable in the Order model.
            # org_id is optional (nullable=True), so don't require it here.
            required_orders_cols = {
                'id', 'brand_id', 'external_order_id',
                'external_created_at', 'external_updated_at'
            }
            missing_orders = required_orders_cols - orders_columns
            if missing_orders:
                raise ValueError(f"Missing required orders columns: {missing_orders}")

            # Check order_lines table
            result = await db.execute(
                text("SELECT column_name FROM information_schema.columns WHERE table_name='order_lines'")
            )
            lines_columns = {row[0] for row in result.fetchall()}

            required_lines_cols = {'id', 'order_id', 'sku', 'quantity'}
            missing_lines = required_lines_cols - lines_columns
            if missing_lines:
                raise ValueError(f"Missing order_lines columns: {missing_lines}")

            # Check for invalid columns that should have been removed
            invalid_cols = {'pulled_qty'} & lines_columns
            if invalid_cols:
                logger.warning(
                    "[SCHEMA_VALIDATION] WARNING: invalid columns in order_lines (should be removed): %s",
                    invalid_cols,
                )
                # Don't raise — just warn. The sync will fail if pulled_qty is actually used.

            logger.info("[SCHEMA_VALIDATION] passed")
    except Exception as e:
        logger.error("[SCHEMA_VALIDATION] FAILED: %s", e, exc_info=True)
        raise


# ---------------------------------------------------------------------------
# Stale job recovery
# ---------------------------------------------------------------------------

STALE_SYNCING_THRESHOLD_MINUTES = 5


async def recover_stale_sync_jobs() -> int:
    """
    Find sync_jobs stuck in 'syncing' state for more than STALE_SYNCING_THRESHOLD_MINUTES
    and reset them to 'queued' so they can be retried.

    Returns the count of recovered jobs.

    Logs [STALE_SYNC_JOB_RECOVERED] for each recovered job and
    [STARTUP_RECOVERY_COMPLETE] with the total count when called from worker_main().
    """
    from sqlalchemy import select, text
    from database import get_async_session_local
    from models import SyncRun

    AsyncSessionLocal = get_async_session_local()
    recovered = 0

    try:
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                text(
                    """
                    SELECT id, brand_id, updated_at
                    FROM sync_runs
                    WHERE status = 'syncing'
                      AND updated_at < NOW() - INTERVAL '5 minutes'
                    """
                )
            )
            stale_rows = result.fetchall()

            for row in stale_rows:
                sync_run_id, brand_id, updated_at = row[0], row[1], row[2]

                # Calculate how long the job has been stalled
                now_utc = datetime.now(timezone.utc)
                if updated_at is not None:
                    if updated_at.tzinfo is None:
                        updated_at = updated_at.replace(tzinfo=timezone.utc)
                    stalled_seconds = (now_utc - updated_at).total_seconds()
                    stalled_minutes = round(stalled_seconds / 60, 1)
                else:
                    stalled_minutes = "unknown"

                # Reset to queued
                await db.execute(
                    text(
                        """
                        UPDATE sync_runs
                        SET status = 'queued',
                            last_error = 'Recovered stale syncing job after worker crash/pool exhaustion',
                            updated_at = NOW()
                        WHERE id = :sync_run_id
                          AND status = 'syncing'
                        """
                    ),
                    {"sync_run_id": sync_run_id},
                )

                logger.info(
                    "[STALE_SYNC_JOB_RECOVERED] sync_run_id=%s brand_id=%s "
                    "time_stalled_minutes=%s",
                    sync_run_id,
                    brand_id,
                    stalled_minutes,
                )
                recovered += 1

            if recovered > 0:
                await db.commit()

    except Exception as exc:
        logger.error(
            "[STALE_SYNC_JOB_RECOVERY_ERROR] failed to recover stale jobs error=%s",
            str(exc)[:500],
            exc_info=True,
        )

    return recovered


# ---------------------------------------------------------------------------
# Pool saturation check
# ---------------------------------------------------------------------------

# Pool configuration constants (must match engine creation in database.py)
_POOL_SIZE = 20
_POOL_MAX_OVERFLOW = 40
_POOL_TOTAL_CAPACITY = _POOL_SIZE + _POOL_MAX_OVERFLOW  # 60 total connections
_POOL_SATURATION_THRESHOLD = 0.80  # 80%


def _get_pool_saturation() -> float | None:
    """
    Return the current pool saturation as a fraction (0.0–1.0), or None if
    the pool is not available / does not expose checkout stats.

    Uses SQLAlchemy's QueuePool.checkedout() to count active connections.
    """
    try:
        from database import get_engine
        engine = get_engine()
        pool = engine.pool
        # QueuePool exposes checkedout() — number of connections currently in use
        checked_out = pool.checkedout()
        saturation = checked_out / _POOL_TOTAL_CAPACITY
        return saturation
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Pool health telemetry
# ---------------------------------------------------------------------------

# Sentinel used by run_scheduler() to detect idle / sync-running conditions
_POLL_DELAY_NO_JOBS = 15       # seconds to sleep when no queued jobs found
_POLL_DELAY_SYNC_RUNNING = 30  # seconds to sleep when a full sync lock is held


async def _pool_health_monitor() -> None:
    """
    Background task: log DB pool health metrics every 60 seconds.

    Emits [DB_POOL_HEALTH] with:
      checked_out       — connections currently in use
      overflow          — connections beyond pool_size
      pool_size         — configured pool_size (20)
      utilization_pct   — checked_out / total_capacity * 100
    """
    while True:
        try:
            await asyncio.sleep(60)
            from database import get_engine
            eng = get_engine()
            pool = eng.pool
            checked_out = pool.checkedout()
            overflow = max(0, checked_out - _POOL_SIZE)
            utilization_pct = round(checked_out / _POOL_TOTAL_CAPACITY * 100, 1)
            logger.info(
                "[DB_POOL_HEALTH] checked_out=%d overflow=%d pool_size=%d "
                "total_capacity=%d utilization_pct=%.1f",
                checked_out,
                overflow,
                _POOL_SIZE,
                _POOL_TOTAL_CAPACITY,
                utilization_pct,
            )
        except asyncio.CancelledError:
            logger.info("[DB_POOL_HEALTH] monitor task cancelled, exiting")
            return
        except Exception as exc:
            logger.warning(
                "[DB_POOL_HEALTH] failed to read pool stats: %s", str(exc)[:200]
            )


# ---------------------------------------------------------------------------
# Core poll-and-execute function
# ---------------------------------------------------------------------------


async def poll_and_execute() -> int:
    """
    Single poll iteration.  Returns a suggested poll delay (seconds) for the
    caller (run_scheduler) to use as the sleep duration before the next poll:

      * _POLL_DELAY_NO_JOBS      (15 s) — no queued jobs found
      * _POLL_DELAY_SYNC_RUNNING (30 s) — full-sync lock is held, skipping
      * POLL_INTERVAL_SECONDS    ( 5 s) — normal execution path

    Steps:
      1. Query SyncRun for the oldest queued or syncing job.
      2. Claim it (set worker_id, status=syncing, last_progress_at).
      3. Fetch the brand's LeafLink credential.
      4. Execute sync_leaflink_background_continuous().
      5. Handle errors: mark job as failed with last_error.

    Queue priority ordering (for SyncRequest-based jobs, when applicable):
      Priority 1 (highest): webhook_order   -- real-time order updates from webhooks
      Priority 2:           webhook_product -- real-time product updates from webhooks
      Priority 3:           incremental_recent_orders -- 30-second incremental sync
      Priority 4 (lowest):  full_resync     -- manual full resync (never auto-triggered)

    SyncRun jobs (this scheduler) are ordered by started_at ASC (FIFO).
    The [QUEUE_PRIORITY_ORDER] marker is logged when claiming each job.
    """
    from sqlalchemy import select

    from database import get_async_session_local, get_engine_id
    from models import BrandAPICredential, SyncRun
    AsyncSessionLocal = get_async_session_local()

    # Log engine_id at the start of each poll cycle so we can verify
    # all sync operations use the canonical engine throughout the run.
    try:
        _poll_engine_id = get_engine_id()
        logger.debug("[DB_ENGINE_ID] poll_and_execute engine_id=%s", _poll_engine_id)
    except Exception:
        pass

    from services.leaflink_sync import (
        sync_leaflink_background_continuous,
        current_full_sync_brand_id,
        current_full_sync_started_at,
        _full_sync_lock,
    )
    from services.sync_run_manager import detect_stalled, mark_stalled

    # ---------------------------------------------------------------------- #
    # Pool saturation short-circuit: skip this poll cycle entirely if the     #
    # connection pool is >= 80% saturated to prevent DB thrash.               #
    # ---------------------------------------------------------------------- #
    pool_saturation = _get_pool_saturation()
    if pool_saturation is not None and pool_saturation >= _POOL_SATURATION_THRESHOLD:
        logger.warning(
            "[SCHEDULER_SKIPPED_POOL_SATURATED] pool_saturation=%.1f%% "
            "threshold=%.0f%% skipping_poll_cycle sleeping_10s",
            pool_saturation * 100,
            _POOL_SATURATION_THRESHOLD * 100,
        )
        await asyncio.sleep(10)
        return POLL_INTERVAL_SECONDS

    # ---------------------------------------------------------------------- #
    # Stale job recovery: reset any jobs stuck in 'syncing' for > 5 minutes  #
    # back to 'queued' so they can be retried this poll cycle.                #
    # ---------------------------------------------------------------------- #
    await recover_stale_sync_jobs()

    # ---------------------------------------------------------------------- #
    # Step 1: Query for queued or syncing jobs                                #
    # Priority order: oldest queued job first (FIFO by started_at ASC).      #
    # Webhook jobs are enqueued with higher priority by the webhook receiver. #
    # ---------------------------------------------------------------------- #
    logger.info(
        "[QUEUE_PRIORITY_ORDER] polling for next job "
        "priority_1=webhook_order priority_2=webhook_product "
        "priority_3=incremental_recent_orders priority_4=full_resync "
        "order_by=started_at_asc",
    )
    _session_open_at = datetime.now(timezone.utc)
    _no_jobs_found = False
    _job_stalled = False
    try:
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(SyncRun)
                .where(SyncRun.status.in_(["queued", "syncing"]))
                .order_by(SyncRun.started_at.asc())
                .limit(1)
            )
            sync_run = result.scalar_one_or_none()

            if not sync_run:
                _no_jobs_found = True
            else:
                sync_run_id = sync_run.id
                brand_id = sync_run.brand_id
                start_page = sync_run.current_page or 1
                # total_pages may be None for cursor-based pagination (LeafLink).
                # Pass None through so sync_leaflink_background_continuous uses
                # cursor termination rather than a page-count bound.
                total_pages = sync_run.total_pages  # May be None
                total_orders_available = sync_run.total_orders_available
                # Resume cursor: if last_next_url is set, resume from that URL
                # instead of starting from page 1 (worker restart recovery).
                last_next_url = getattr(sync_run, "last_next_url", None)

                # ------------------------------------------------------------------ #
                # Stall detection: only check jobs that have been claimed            #
                # (worker_id is not null). Skip newly queued jobs.                   #
                # ------------------------------------------------------------------ #
                if sync_run.worker_id is not None:
                    # Job was claimed by a worker but made no progress
                    if detect_stalled(sync_run, stall_threshold_seconds=STALL_THRESHOLD_SECONDS):
                        stall_reason = (
                            f"no_progress_for_{STALL_THRESHOLD_SECONDS}s "
                            f"last_progress_at={sync_run.last_progress_at}"
                        )
                        logger.warning(
                            "[SyncWorker] job_stalled id=%s brand=%s reason=%s",
                            sync_run_id,
                            brand_id,
                            stall_reason,
                        )
                        await mark_stalled(db, sync_run_id, stall_reason)
                        await db.commit()
                        _job_stalled = True

                else:
                    # ------------------------------------------------------------------ #
                    # Step 2: Claim the job                                               #
                    # ------------------------------------------------------------------ #
                    sync_run.worker_id = WORKER_ID
                    sync_run.status = "syncing"
                    sync_run.last_progress_at = datetime.now(timezone.utc)
                    await db.commit()

                    logger.info(
                        "[SyncWorker] job_claimed id=%s worker_id=%s brand=%s",
                        sync_run_id,
                        WORKER_ID,
                        brand_id,
                    )
    except Exception as db_exc:
        logger.error(
            "[SyncWorker] database error in job query/claim: %s",
            str(db_exc)[:500],
            exc_info=True,
        )
        sys.stdout.flush()
        raise
    finally:
        _session_duration = (datetime.now(timezone.utc) - _session_open_at).total_seconds()
        logger.info(
            "[DB_SESSION_CLOSED] function=poll_and_execute step=job_query duration_seconds=%.3f",
            _session_duration,
        )
        if _session_duration > 30:
            logger.warning(
                "[LONG_LIVED_DB_SESSION] function=poll_and_execute step=job_query "
                "duration_seconds=%.3f",
                _session_duration,
            )

    if _no_jobs_found:
        logger.info(
            "[SCHEDULER_IDLE_SLEEP] reason=no_jobs_found suggested_delay_seconds=%d",
            _POLL_DELAY_NO_JOBS,
        )
        return _POLL_DELAY_NO_JOBS

    if _job_stalled:
        return POLL_INTERVAL_SECONDS

    # ---------------------------------------------------------------------- #
    # Step 3: Fetch credential                                                #
    # ---------------------------------------------------------------------- #
    _cred_session_open_at = datetime.now(timezone.utc)
    _cred_early_return = False
    try:
        async with AsyncSessionLocal() as db:
            cred_result = await db.execute(
                select(BrandAPICredential).where(
                    BrandAPICredential.brand_id == brand_id,
                    BrandAPICredential.integration_name == "leaflink",
                    BrandAPICredential.is_active == True,
                )
            )
            cred = cred_result.scalar_one_or_none()

            if not cred:
                logger.error(
                    "[SyncWorker] credential_not_found id=%s brand=%s",
                    sync_run_id,
                    brand_id,
                )
                # Mark job as failed
                fail_result = await db.execute(
                    select(SyncRun).where(SyncRun.id == sync_run_id)
                )
                fail_run = fail_result.scalar_one_or_none()
                if fail_run:
                    fail_run.status = "failed"
                    fail_run.last_error = "LeafLink credential not found"
                    fail_run.completed_at = datetime.now(timezone.utc)
                    await db.commit()
                _cred_early_return = True
            else:
                api_key: str = cred.api_key or ""
                company_id: str = cred.company_id or ""
                auth_scheme: str = cred.auth_scheme or "Token"
                base_url: str = cred.base_url or ""
                org_id: str = getattr(cred, "org_id", None) or ""
                logger.info(
                    "[SyncWorker] org_id_from_credential id=%s brand=%s org_id=%s",
                    sync_run_id,
                    brand_id,
                    org_id or "MISSING",
                )

                # Validate api_key
                if not api_key or not api_key.strip():
                    logger.error("[SyncWorker] credential_invalid id=%s api_key_missing", sync_run_id)
                    fail_result = await db.execute(
                        select(SyncRun).where(SyncRun.id == sync_run_id)
                    )
                    fail_run = fail_result.scalar_one_or_none()
                    if fail_run:
                        fail_run.status = "failed"
                        fail_run.last_error = "LeafLink api_key is empty"
                        fail_run.completed_at = datetime.now(timezone.utc)
                        await db.commit()
                    _cred_early_return = True

                # Validate company_id
                elif not company_id or not company_id.strip():
                    logger.error("[SyncWorker] credential_invalid id=%s company_id_missing", sync_run_id)
                    fail_result = await db.execute(
                        select(SyncRun).where(SyncRun.id == sync_run_id)
                    )
                    fail_run = fail_result.scalar_one_or_none()
                    if fail_run:
                        fail_run.status = "failed"
                        fail_run.last_error = "LeafLink company_id is empty"
                        fail_run.completed_at = datetime.now(timezone.utc)
                        await db.commit()
                    _cred_early_return = True

    except Exception as db_exc:
        logger.error(
            "[SyncWorker] database error fetching credentials: %s",
            str(db_exc)[:500],
            exc_info=True,
        )
        sys.stdout.flush()
        raise
    finally:
        _cred_session_duration = (datetime.now(timezone.utc) - _cred_session_open_at).total_seconds()
        logger.info(
            "[DB_SESSION_CLOSED] function=poll_and_execute step=credential_fetch "
            "sync_run_id=%s duration_seconds=%.3f",
            sync_run_id,
            _cred_session_duration,
        )
        if _cred_session_duration > 30:
            logger.warning(
                "[LONG_LIVED_DB_SESSION] function=poll_and_execute step=credential_fetch "
                "sync_run_id=%s duration_seconds=%.3f",
                sync_run_id,
                _cred_session_duration,
            )

    if _cred_early_return:
        return POLL_INTERVAL_SECONDS

    # ---------------------------------------------------------------------- #
    # Step 3.5: Overlapping full-sync guard                                   #
    # If a full backfill is already running (lock held), skip this job rather #
    # than launching a second concurrent sync that would exhaust the DB pool. #
    # ---------------------------------------------------------------------- #
    import services.leaflink_sync as _leaflink_sync_module
    if _full_sync_lock.locked():
        _running_brand = _leaflink_sync_module.current_full_sync_brand_id or "unknown"
        _running_since = (
            _leaflink_sync_module.current_full_sync_started_at.isoformat()
            if _leaflink_sync_module.current_full_sync_started_at
            else "unknown"
        )
        logger.warning(
            "[QUEUE_SCHEDULER_SKIPPED_OVERLAPPING_SYNC] sync_run_id=%s brand_id=%s "
            "already_running_brand=%s running_since=%s — skipping to prevent pool exhaustion",
            sync_run_id,
            brand_id,
            _running_brand,
            _running_since,
        )
        # Leave the job in "syncing" state — it will be retried on the next poll
        # once the active sync completes and releases the lock.
        logger.info(
            "[SCHEDULER_IDLE_SLEEP] reason=sync_lock_held suggested_delay_seconds=%d",
            _POLL_DELAY_SYNC_RUNNING,
        )
        return _POLL_DELAY_SYNC_RUNNING

    # ---------------------------------------------------------------------- #
    # Step 4: Execute sync                                                    #
    # ---------------------------------------------------------------------- #

    # Stamp last_progress_at immediately before the first API call so the
    # stall detector does not fire while we are waiting for the first page.
    _stamp_session_open_at = datetime.now(timezone.utc)
    try:
        async with AsyncSessionLocal() as db:
            pre_run_result = await db.execute(
                select(SyncRun).where(SyncRun.id == sync_run_id)
            )
            pre_run = pre_run_result.scalar_one_or_none()
            if pre_run:
                pre_run.last_progress_at = datetime.now(timezone.utc)
                await db.commit()
    except Exception as db_exc:
        logger.error(
            "[SyncWorker] database error stamping progress: %s",
            str(db_exc)[:500],
            exc_info=True,
        )
        sys.stdout.flush()
        raise
    finally:
        _stamp_duration = (datetime.now(timezone.utc) - _stamp_session_open_at).total_seconds()
        logger.info(
            "[DB_SESSION_CLOSED] function=poll_and_execute step=progress_stamp "
            "sync_run_id=%s duration_seconds=%.3f",
            sync_run_id,
            _stamp_duration,
        )
        if _stamp_duration > 30:
            logger.warning(
                "[LONG_LIVED_DB_SESSION] function=poll_and_execute step=progress_stamp "
                "sync_run_id=%s duration_seconds=%.3f",
                sync_run_id,
                _stamp_duration,
            )

    try:
        if last_next_url:
            logger.info(
                "[SyncWorker] resuming_from_cursor id=%s brand=%s last_next_url_present=true start_page=%s",
                sync_run_id,
                brand_id,
                start_page,
            )
        else:
            logger.info(
                "[SyncWorker] starting_fresh id=%s brand=%s start_page=%s total_pages=%s",
                sync_run_id,
                brand_id,
                start_page,
                total_pages,
            )

        await sync_leaflink_background_continuous(
            brand_id=brand_id,
            api_key=api_key,
            company_id=company_id,
            auth_scheme=auth_scheme,
            start_page=start_page,
            total_pages=total_pages,
            manager=None,  # No in-memory manager needed — DB-only progress tracking
            sync_run_id=sync_run_id,  # Pass SyncRun ID for direct DB updates
            total_orders_available=total_orders_available,
            base_url=base_url,
            org_id=org_id or None,
            last_next_url=last_next_url,
        )

        logger.info("[SyncWorker] sync_complete id=%s brand=%s", sync_run_id, brand_id)

    except asyncio.CancelledError:
        logger.warning(
            "[SyncWorker] sync_cancelled id=%s brand=%s",
            sync_run_id,
            brand_id,
        )
        raise
    except asyncio.TimeoutError as sync_exc:
        logger.error(
            "[SyncWorker] sync_timeout id=%s brand=%s error=%s",
            sync_run_id,
            brand_id,
            str(sync_exc)[:500],
        )
        # Mark as incomplete on timeout
        try:
            async with AsyncSessionLocal() as err_db:
                err_result = await err_db.execute(
                    select(SyncRun).where(SyncRun.id == sync_run_id)
                )
                err_run = err_result.scalar_one_or_none()
                if err_run:
                    err_run.status = "incomplete"
                    err_run.last_error = "sync_timeout"
                    err_run.completed_at = datetime.now(timezone.utc)
                    await err_db.commit()
        except Exception as mark_exc:
            logger.error(
                "[SyncWorker] failed_to_mark_timeout id=%s error=%s",
                sync_run_id,
                str(mark_exc)[:200],
            )
    except Exception as sync_exc:
        logger.error(
            "[SyncWorker] sync_error id=%s brand=%s exception_type=%s error=%s",
            sync_run_id,
            brand_id,
            type(sync_exc).__name__,
            str(sync_exc)[:500],
            exc_info=True,
        )
        # IMMEDIATELY mark as failed with error
        try:
            async with AsyncSessionLocal() as err_db:
                err_result = await err_db.execute(
                    select(SyncRun).where(SyncRun.id == sync_run_id)
                )
                err_run = err_result.scalar_one_or_none()
                if err_run:
                    err_run.status = "failed"
                    err_run.last_error = str(sync_exc)[:500]  # Truncate to 500 chars
                    err_run.completed_at = datetime.now(timezone.utc)
                    await err_db.commit()
                    logger.info("[SyncWorker] marked_failed id=%s error_set=true", sync_run_id)
        except Exception as mark_exc:
            logger.error(
                "[SyncWorker] failed_to_mark_error id=%s error=%s",
                sync_run_id,
                str(mark_exc)[:200],
            )

    return POLL_INTERVAL_SECONDS


# ---------------------------------------------------------------------------
# Scheduler loop
# ---------------------------------------------------------------------------

async def run_scheduler() -> None:
    """
    Main scheduler loop. Polls for SyncRun jobs every POLL_INTERVAL_SECONDS.
    """
    print("=== RUN_SCHEDULER ASYNC FUNCTION ENTERED ===")
    sys.stdout.flush()

    # ---------------------------------------------------------------------- #
    # Hard gate: database MUST be initialized before scheduler can start.     #
    # _sync_worker_bootstrap() and _sync_worker_init_database() run above at  #
    # module load time, so this check should always pass.                     #
    # ---------------------------------------------------------------------- #
    # Verify database was initialized by bootstrap code above
    from database import is_bootstrap_complete, get_async_session_local
    if not is_bootstrap_complete():
        logger.error("[SYNC_WORKER_BLOCKED] database not initialized — cannot start scheduler")
        raise RuntimeError("Database not initialized — cannot start scheduler")

    logger.info("[SYNC_WORKER_READY] database initialized, scheduler starting")

    # ---------------------------------------------------------------------- #
    # Log engine pool configuration for startup diagnostics.                  #
    # ---------------------------------------------------------------------- #
    try:
        from database import get_engine
        _sched_eng = get_engine()
        _sched_pool = _sched_eng.pool
        _sched_ps = _sched_pool.size()
        _sched_mo = _sched_pool._max_overflow
        logger.info(
            "[DB_ENGINE_CONFIG] pool_size=%d max_overflow=%d total_capacity=%d "
            "pool_timeout=%s pool_recycle=%s pool_pre_ping=true "
            "database_url_source=DATABASE_URL",
            _sched_ps,
            _sched_mo,
            _sched_ps + _sched_mo,
            getattr(_sched_pool, "_timeout", "unknown"),
            getattr(_sched_pool, "_recycle", "unknown"),
        )
        logger.info("[DB_POOL_CAPACITY] total_capacity=%d", _sched_ps + _sched_mo)
    except Exception as _sched_cfg_exc:
        logger.warning("[DB_ENGINE_CONFIG] failed to read pool config: %s", _sched_cfg_exc)

    # ---------------------------------------------------------------------- #
    # Startup diagnostics: log env-var presence and parsed DATABASE_URL       #
    # before any database connection attempt.                                 #
    # ---------------------------------------------------------------------- #
    logger.info("[SyncScheduler] Checking environment variables...")
    _db_url = os.getenv("DATABASE_URL")
    _db_public_url = os.getenv("DATABASE_PUBLIC_URL")
    _postgres_url = os.getenv("POSTGRES_URL")
    _async_db_url = os.getenv("ASYNC_DATABASE_URL")

    logger.info("[SyncScheduler] DATABASE_URL set: %s", _db_url is not None)
    logger.info("[SyncScheduler] DATABASE_PUBLIC_URL set: %s", _db_public_url is not None)
    logger.info("[SyncScheduler] POSTGRES_URL set: %s", _postgres_url is not None)
    logger.info("[SyncScheduler] ASYNC_DATABASE_URL set: %s", _async_db_url is not None)

    _db_url = os.getenv("DATABASE_URL", "")
    logger.info("[SyncScheduler] DATABASE_URL raw length: %d", len(_db_url))

    if _db_url:
        try:
            _parsed = urllib.parse.urlparse(_db_url)
            logger.info(
                "[SyncScheduler] PARSED driver=%s host=%s port=%s database=%s password_exists=%s",
                _parsed.scheme,
                _parsed.hostname,
                _parsed.port,
                _parsed.path.lstrip("/"),
                _parsed.password is not None,
            )
        except Exception as e:
            logger.error("[SyncScheduler] Failed to parse DATABASE_URL: %s", e)
    else:
        logger.error("[SyncScheduler] DATABASE_URL is empty or not set")

    sys.stdout.flush()

    logger.info("===================================")
    logger.info("Opsyn Sync Worker Started")
    logger.info("===================================")
    logger.info("Worker ID: %s", WORKER_ID)
    logger.info("Poll interval: %s seconds", POLL_INTERVAL_SECONDS)
    logger.info("Stall threshold: %s seconds", STALL_THRESHOLD_SECONDS)
    logger.info("===================================")
    sys.stdout.flush()

    # Validate database connection at startup
    try:
        logger.info("[SyncWorker] validating database connection")
        sys.stdout.flush()

        from sqlalchemy import select, func
        from database import get_async_session_local
        AsyncSessionLocal = get_async_session_local()

        async with AsyncSessionLocal() as test_db:
            await test_db.execute(select(1))

        logger.info("[SyncWorker] database connection OK")
        sys.stdout.flush()

    except Exception as db_test_exc:
        logger.error(
            "[SyncWorker] FATAL: database connection failed: %s",
            str(db_test_exc)[:500],
            exc_info=True,
        )
        sys.stdout.flush()
        raise

    # Log database identity for verification
    try:
        from sqlalchemy import select, func
        from database import get_async_session_local
        AsyncSessionLocal = get_async_session_local()

        async with AsyncSessionLocal() as identity_db:
            result = await identity_db.execute(
                select(
                    func.current_database(),
                    func.current_user(),
                    func.inet_server_addr(),
                    func.inet_server_port(),
                )
            )
            row = result.first()
            if row:
                db_name, db_user, server_addr, server_port = row
                logger.info(
                    "[DB_IDENTITY] database=%s user=%s host=%s port=%s",
                    db_name,
                    db_user,
                    server_addr,
                    server_port,
                )
    except Exception as identity_exc:
        logger.error(
            "[DB_IDENTITY] failed to query database identity: %s",
            identity_exc,
            exc_info=True,
        )

    # Validate database schema at startup
    try:
        await validate_schema()
    except Exception as schema_exc:
        logger.error(
            "[SyncWorker] FATAL: schema validation failed: %s",
            str(schema_exc)[:500],
            exc_info=True,
        )
        sys.stdout.flush()
        raise

    # Backoff configuration
    _BACKOFF_MAX_SECONDS = 60
    _BACKOFF_MULTIPLIER = 2

    # Spawn pool health monitor as a background task (runs every 60s)
    _pool_monitor_task = asyncio.create_task(
        _pool_health_monitor(), name="db_pool_health_monitor"
    )
    logger.info("[DB_POOL_HEALTH] pool health monitor started (60s interval)")

    # Main polling loop with exponential backoff on QueuePool TimeoutError
    poll_delay = POLL_INTERVAL_SECONDS
    try:
        while True:
            try:
                suggested_delay = await poll_and_execute()

                # Use the delay suggested by poll_and_execute (idle / sync-running
                # conditions return longer delays to reduce DB session churn).
                if suggested_delay != POLL_INTERVAL_SECONDS:
                    # Only log when the delay differs from the normal interval
                    logger.info(
                        "[SCHEDULER_IDLE_SLEEP] delay_seconds=%d "
                        "(poll_and_execute suggested idle delay)",
                        suggested_delay,
                    )
                    # Don't override an elevated backoff delay with a shorter idle delay
                    effective_delay = max(suggested_delay, poll_delay)
                else:
                    # Normal execution — reset backoff if it was elevated
                    if poll_delay != POLL_INTERVAL_SECONDS:
                        logger.info(
                            "[SCHEDULER_BACKOFF_RESET] poll_delay_reset_to=%ds "
                            "previous_delay=%ds",
                            POLL_INTERVAL_SECONDS,
                            poll_delay,
                        )
                        poll_delay = POLL_INTERVAL_SECONDS
                    effective_delay = poll_delay

            except Exception as loop_exc:
                exc_str = str(loop_exc)
                # Detect QueuePool TimeoutError (pool exhaustion)
                is_pool_timeout = (
                    "QueuePool" in exc_str
                    or "TimeoutError" in type(loop_exc).__name__
                    or "pool timeout" in exc_str.lower()
                    or "connection pool" in exc_str.lower()
                )

                if is_pool_timeout:
                    new_delay = min(poll_delay * _BACKOFF_MULTIPLIER, _BACKOFF_MAX_SECONDS)
                    logger.warning(
                        "[SCHEDULER_BACKOFF] pool_timeout_detected "
                        "increasing_poll_delay=%ds previous_delay=%ds "
                        "error=%s",
                        new_delay,
                        poll_delay,
                        exc_str[:200],
                    )
                    poll_delay = new_delay
                else:
                    logger.error(
                        "[SyncWorker] FATAL ERROR in polling loop: %s",
                        exc_str[:500],
                        exc_info=True,
                    )
                sys.stdout.flush()
                effective_delay = poll_delay

            await asyncio.sleep(effective_delay)
    finally:
        _pool_monitor_task.cancel()
        try:
            await _pool_monitor_task
        except asyncio.CancelledError:
            pass
        logger.info("[DB_POOL_HEALTH] pool health monitor stopped")


if __name__ == "__main__":
    try:
        asyncio.run(worker_main())
    except KeyboardInterrupt:
        logger.info("[SYNC_WORKER_SHUTDOWN] received SIGINT, shutting down")
    except Exception as e:
        logger.error("[SYNC_WORKER_SHUTDOWN] fatal error error=%s", str(e)[:500])
        sys.exit(1)
