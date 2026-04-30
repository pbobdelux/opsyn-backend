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

    logger.info("[SyncWorker] polling for jobs")

    # ---------------------------------------------------------------------- #
    # Step 1: Query for queued or syncing jobs                                #
    # ---------------------------------------------------------------------- #
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(SyncRun)
            .where(SyncRun.status.in_(["queued", "syncing"]))
            .order_by(SyncRun.started_at.asc())
            .limit(1)
        )
        sync_run = result.scalar_one_or_none()

        if not sync_run:
            logger.info("[SyncWorker] polling for jobs - none found")
            return

        sync_run_id = sync_run.id
        brand_id = sync_run.brand_id
        start_page = sync_run.current_page or 1
        total_pages = sync_run.total_pages or 1
        total_orders_available = sync_run.total_orders_available

        logger.info(
            "[SyncWorker] job_found id=%s brand=%s status=%s start_page=%s total_pages=%s",
            sync_run_id,
            brand_id,
            sync_run.status,
            start_page,
            total_pages,
        )

        # ------------------------------------------------------------------ #
        # Stall detection: if the job is already syncing but has made no      #
        # progress for STALL_THRESHOLD_SECONDS, mark it stalled and skip it.  #
        # ------------------------------------------------------------------ #
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

    # ---------------------------------------------------------------------- #
    # Step 3: Fetch credential                                                #
    # ---------------------------------------------------------------------- #
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

        if not api_key.strip() or not company_id.strip():
            logger.error(
                "[SyncWorker] credential_incomplete id=%s brand=%s "
                "api_key_present=%s company_id_present=%s",
                sync_run_id,
                brand_id,
                bool(api_key.strip()),
                bool(company_id.strip()),
            )
            fail_result = await db.execute(
                select(SyncRun).where(SyncRun.id == sync_run_id)
            )
            fail_run = fail_result.scalar_one_or_none()
            if fail_run:
                fail_run.status = "failed"
                fail_run.last_error = "LeafLink api_key or company_id is empty"
                fail_run.completed_at = datetime.now(timezone.utc)
                await db.commit()
            return

    # ---------------------------------------------------------------------- #
    # Step 4: Execute sync                                                    #
    # ---------------------------------------------------------------------- #
    logger.info(
        "[SyncWorker] starting page fetch id=%s start_page=%s total_pages=%s",
        sync_run_id,
        start_page,
        total_pages,
    )

    try:
        await sync_leaflink_background_continuous(
            brand_id=brand_id,
            api_key=api_key,
            company_id=company_id,
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
            sync_exc,
            exc_info=True,
        )
        # Mark job as failed
        try:
            async with AsyncSessionLocal() as err_db:
                err_result = await err_db.execute(
                    select(SyncRun).where(SyncRun.id == sync_run_id)
                )
                err_run = err_result.scalar_one_or_none()
                if err_run:
                    err_run.status = "failed"
                    err_run.last_error = str(sync_exc)
                    err_run.completed_at = datetime.now(timezone.utc)
                    await err_db.commit()
        except Exception as mark_exc:
            logger.error(
                "[SyncWorker] failed_to_mark_error id=%s error=%s",
                sync_run_id,
                mark_exc,
            )


# ---------------------------------------------------------------------------
# Scheduler loop
# ---------------------------------------------------------------------------

async def run_scheduler() -> None:
    """
    Main scheduler loop. Polls for SyncRun jobs every POLL_INTERVAL_SECONDS.
    """
    logger.info("===================================")
    logger.info("Opsyn Sync Worker Started")
    logger.info("===================================")
    logger.info("Worker ID: %s", WORKER_ID)
    logger.info("Poll interval: %s seconds", POLL_INTERVAL_SECONDS)
    logger.info("Stall threshold: %s seconds", STALL_THRESHOLD_SECONDS)
    logger.info("===================================")

    while True:
        try:
            await poll_and_execute()
        except Exception as loop_exc:
            logger.error(
                "[SyncWorker] loop_error error=%s",
                loop_exc,
                exc_info=True,
            )

        await asyncio.sleep(POLL_INTERVAL_SECONDS)


def main() -> None:
    asyncio.run(run_scheduler())


if __name__ == "__main__":
    main()
