"""
opsyn-sync-worker — dedicated background sync worker service.

Polls the `sync_requests` table for pending rows, processes each one by
calling sync_leaflink_background_continuous(), and updates the row status
(pending → processing → complete | error).

Running as a separate service means:
  - The web process is never blocked by background sync I/O.
  - Sync continues even if the web process crashes or restarts.
  - Workers can be scaled independently of the web tier.
  - The persistent queue survives service restarts.

Environment variables (same as the web service):
  DATABASE_URL          — PostgreSQL connection string (required)
  POLL_INTERVAL_SECONDS — seconds to sleep when the queue is empty (default 5)
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, timezone

# Retry configuration for transient LeafLink errors
MAX_RETRIES = 3
INITIAL_BACKOFF_SECONDS = 2
MAX_BACKOFF_SECONDS = 60
BACKOFF_MULTIPLIER = 2.0
TRANSIENT_ERROR_CODES = {429, 502, 503, 504}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("sync-worker")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "5"))

# ---------------------------------------------------------------------------
# Bootstrap: ensure the repo root is on sys.path so shared modules resolve.
# The worker lives in opsyn-sync-worker/ which is one level below the root.
# ---------------------------------------------------------------------------
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


# ---------------------------------------------------------------------------
# Main polling loop
# ---------------------------------------------------------------------------

async def process_sync_requests() -> None:
    """
    Poll the sync_requests table indefinitely.

    For each pending request:
      1. Mark it as 'processing'.
      2. Look up the brand's LeafLink credentials.
      3. Call sync_leaflink_background_continuous().
      4. Mark it as 'complete' or 'error'.
    """
    from sqlalchemy import select

    from database import AsyncSessionLocal
    from models import BrandAPICredential, SyncRequest
    from services.leaflink_sync import sync_leaflink_background_continuous

    logger.info("[SyncWorker] started poll_interval_seconds=%s", POLL_INTERVAL_SECONDS)

    while True:
        try:
            async with AsyncSessionLocal() as db:
                # ---------------------------------------------------------- #
                # Fetch the oldest pending request                            #
                # ---------------------------------------------------------- #
                result = await db.execute(
                    select(SyncRequest)
                    .where(SyncRequest.status == "pending")
                    .order_by(SyncRequest.created_at.asc())
                    .limit(1)
                )
                request = result.scalar_one_or_none()

                if not request:
                    # Queue is empty — sleep and poll again
                    await asyncio.sleep(POLL_INTERVAL_SECONDS)
                    continue

                # ---------------------------------------------------------- #
                # Mark as processing                                          #
                # ---------------------------------------------------------- #
                request.status = "processing"
                request.started_at = datetime.now(timezone.utc)
                await db.commit()

                request_id = request.id
                brand_id = request.brand_id
                start_page = request.start_page
                total_pages = request.total_pages
                total_orders_available = request.total_orders_available

                logger.info(
                    "[SyncWorker] processing_request id=%s brand=%s start_page=%s total_pages=%s",
                    request_id,
                    brand_id,
                    start_page,
                    total_pages,
                )

            # ---------------------------------------------------------------- #
            # Fetch credentials in a fresh session (previous session committed) #
            # ---------------------------------------------------------------- #
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
                        "[SyncWorker] credentials_not_found id=%s brand=%s",
                        request_id,
                        brand_id,
                    )
                    # Mark request as error
                    async with AsyncSessionLocal() as err_db:
                        err_result = await err_db.execute(
                            select(SyncRequest).where(SyncRequest.id == request_id)
                        )
                        err_req = err_result.scalar_one_or_none()
                        if err_req:
                            err_req.status = "error"
                            err_req.error = "Active LeafLink credentials not found for brand"
                            err_req.completed_at = datetime.now(timezone.utc)
                            await err_db.commit()
                    continue

                api_key: str = cred.api_key or ""
                company_id: str = cred.company_id or ""

                if not api_key.strip() or not company_id.strip():
                    logger.error(
                        "[SyncWorker] credentials_incomplete id=%s brand=%s api_key_present=%s company_id_present=%s",
                        request_id,
                        brand_id,
                        bool(api_key.strip()),
                        bool(company_id.strip()),
                    )
                    async with AsyncSessionLocal() as err_db:
                        err_result = await err_db.execute(
                            select(SyncRequest).where(SyncRequest.id == request_id)
                        )
                        err_req = err_result.scalar_one_or_none()
                        if err_req:
                            err_req.status = "error"
                            err_req.error = "LeafLink api_key or company_id is empty"
                            err_req.completed_at = datetime.now(timezone.utc)
                            await err_db.commit()
                    continue

            # ---------------------------------------------------------------- #
            # Process the sync                                                  #
            # ---------------------------------------------------------------- #
            try:
                logger.info(
                    "[SyncWorker] sync_start id=%s brand=%s start_page=%s total_pages=%s",
                    request_id,
                    brand_id,
                    start_page,
                    total_pages,
                )
                logger.info(
                    "[OrdersSyncResume] brand=%s db_count_before=%s starting_page=%s total_pages=%s",
                    brand_id,
                    0,
                    start_page,
                    total_pages,
                )

                await sync_leaflink_background_continuous(
                    brand_id=brand_id,
                    api_key=api_key,
                    company_id=company_id,
                    start_page=start_page,
                    total_pages=total_pages or 1,
                    manager=None,  # Worker uses DB-only progress tracking
                    total_orders_available=total_orders_available,
                )

                # Mark complete
                async with AsyncSessionLocal() as done_db:
                    done_result = await done_db.execute(
                        select(SyncRequest).where(SyncRequest.id == request_id)
                    )
                    done_req = done_result.scalar_one_or_none()
                    if done_req:
                        done_req.status = "complete"
                        done_req.completed_at = datetime.now(timezone.utc)
                        await done_db.commit()

                logger.info(
                    "[SyncWorker] sync_complete id=%s brand=%s",
                    request_id,
                    brand_id,
                )
                logger.info("[OrdersSyncResume] complete brand=%s", brand_id)

            except Exception as sync_exc:
                error_code = getattr(sync_exc, "status_code", None)
                is_transient = error_code in TRANSIENT_ERROR_CODES

                if is_transient:
                    # Transient error — mark as paused so startup resume logic retries it
                    logger.warning(
                        "[OrdersSyncResume] paused_transient_error brand=%s error_code=%s error=%s",
                        brand_id,
                        error_code,
                        sync_exc,
                    )
                    async with AsyncSessionLocal() as err_db:
                        err_result = await err_db.execute(
                            select(SyncRequest).where(SyncRequest.id == request_id)
                        )
                        err_req = err_result.scalar_one_or_none()
                        if err_req:
                            err_req.status = "paused"
                            err_req.error = f"LeafLink temporarily unavailable (HTTP {error_code})"
                            err_req.completed_at = datetime.now(timezone.utc)
                            await err_db.commit()
                else:
                    # Permanent error — mark as error
                    logger.error(
                        "[OrdersSyncResume] failed_permanent_error brand=%s error=%s",
                        brand_id,
                        sync_exc,
                        exc_info=True,
                    )
                    logger.error(
                        "[SyncWorker] sync_error id=%s brand=%s error=%s",
                        request_id,
                        brand_id,
                        sync_exc,
                        exc_info=True,
                    )
                    async with AsyncSessionLocal() as err_db:
                        err_result = await err_db.execute(
                            select(SyncRequest).where(SyncRequest.id == request_id)
                        )
                        err_req = err_result.scalar_one_or_none()
                        if err_req:
                            err_req.status = "error"
                            err_req.error = str(sync_exc)
                            err_req.completed_at = datetime.now(timezone.utc)
                            await err_db.commit()

        except Exception as loop_exc:
            logger.error(
                "[SyncWorker] loop_error error=%s",
                loop_exc,
                exc_info=True,
            )
            await asyncio.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    asyncio.run(process_sync_requests())
