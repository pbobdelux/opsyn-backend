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
# Bootstrap: ensure the repo root is on sys.path so shared modules resolve.
# The scheduler lives in services/ which is one level below the root.
# ---------------------------------------------------------------------------
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


# ---------------------------------------------------------------------------
# Core poll-and-execute function
# ---------------------------------------------------------------------------

async def poll_and_execute() -> None:
    """
    Single poll iteration:
      1. Query SyncRun for the oldest queued or syncing job.
      2. Claim it (set worker_id, status=syncing, last_progress_at).
      3. Fetch the brand's LeafLink credential.
      4. Execute sync_leaflink_background_continuous().
      5. Handle errors: mark job as failed with last_error.
    """
    from sqlalchemy import select

    from database import AsyncSessionLocal
    from models import BrandAPICredential, SyncRun
    from services.leaflink_sync import sync_leaflink_background_continuous
    from services.sync_run_manager import detect_stalled, mark_stalled

    # ---------------------------------------------------------------------- #
    # Step 1: Query for queued or syncing jobs                                #
    # ---------------------------------------------------------------------- #
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
                return

            sync_run_id = sync_run.id
            brand_id = sync_run.brand_id
            start_page = sync_run.current_page or 1
            # total_pages may be None for cursor-based pagination (LeafLink).
            # Pass None through so sync_leaflink_background_continuous uses
            # cursor termination rather than a page-count bound.
            total_pages = sync_run.total_pages  # May be None
            total_orders_available = sync_run.total_orders_available

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
                    return

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

    # ---------------------------------------------------------------------- #
    # Step 3: Fetch credential                                                #
    # ---------------------------------------------------------------------- #
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
                return

            api_key: str = cred.api_key or ""
            company_id: str = cred.company_id or ""
            auth_scheme: str = cred.auth_scheme or "Token"

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
                return

            # Validate company_id
            if not company_id or not company_id.strip():
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
                return

    except Exception as db_exc:
        logger.error(
            "[SyncWorker] database error fetching credentials: %s",
            str(db_exc)[:500],
            exc_info=True,
        )
        sys.stdout.flush()
        raise

    # ---------------------------------------------------------------------- #
    # Step 4: Execute sync                                                    #
    # ---------------------------------------------------------------------- #

    # Stamp last_progress_at immediately before the first API call so the
    # stall detector does not fire while we are waiting for the first page.
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

    try:
        logger.info(
            "[SyncWorker] sync_start id=%s start_page=%s total_pages=%s",
            sync_run_id,
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
        )

        logger.info("[SyncWorker] sync_complete id=%s brand=%s", sync_run_id, brand_id)

    except Exception as sync_exc:
        logger.error(
            "[SyncWorker] sync_error id=%s brand=%s error=%s",
            sync_run_id,
            brand_id,
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


# ---------------------------------------------------------------------------
# Scheduler loop
# ---------------------------------------------------------------------------

async def run_scheduler() -> None:
    """
    Main scheduler loop. Polls for SyncRun jobs every POLL_INTERVAL_SECONDS.
    """
    print("=== RUN_SCHEDULER ASYNC FUNCTION ENTERED ===")
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

        from sqlalchemy import select
        from database import AsyncSessionLocal

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

    # Main polling loop
    while True:
        try:
            await poll_and_execute()

        except Exception as loop_exc:
            logger.error(
                "[SyncWorker] FATAL ERROR in polling loop: %s",
                str(loop_exc)[:500],
                exc_info=True,
            )
            sys.stdout.flush()

        await asyncio.sleep(POLL_INTERVAL_SECONDS)


def main() -> None:
    print("=== MAIN FUNCTION ENTERED ===")
    sys.stdout.flush()

    try:
        import asyncio as _asyncio
        _asyncio.run(run_scheduler())

    except Exception as main_exc:
        logger.error(
            "[SyncWorker] FATAL ERROR in main: %s",
            str(main_exc)[:500],
            exc_info=True,
        )
        sys.stdout.flush()
        raise


if __name__ == "__main__":
    main()
