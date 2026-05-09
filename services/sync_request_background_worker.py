"""
sync_request_background_worker.py — Background worker for the sync_requests queue.

Polls the sync_requests table for queued jobs, claims them, and dispatches
to process_sync_request(). Runs as an asyncio task inside the FastAPI lifespan.

Priority order (highest first):
  1. webhook_order           — real-time order updates from LeafLink webhooks
  2. webhook_product         — real-time product updates from LeafLink webhooks
  3. incremental_recent_orders — API safety-net polling (every 30 seconds)
  4. full_resync             — manual/on-demand full sync

On success: marks status=completed, sets completed_at.
On error:   increments retry_count.
  - If retry_count < max_retries: re-queues (status=queued).
  - Else: marks status=failed, stores error_message.

Also updates leaflink_webhook_events.status and processed_at when a
webhook_order or webhook_product job completes.
"""

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import AsyncSessionLocal
from models import SyncRequest, LeafLinkWebhookEvent
from services.sync_request_processor import process_sync_request

logger = logging.getLogger("sync_request_worker")

# Seconds to sleep between polls when no jobs are found
WORKER_POLL_INTERVAL = int(os.getenv("SYNC_REQUEST_WORKER_POLL_INTERVAL", "1"))

# Priority ordering for job types — lower index = higher priority
_JOB_TYPE_PRIORITY = [
    "webhook_order",
    "webhook_product",
    "incremental_recent_orders",
    "full_resync",
]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


async def _claim_next_job(db: AsyncSession) -> Optional[SyncRequest]:
    """
    Claim the highest-priority queued job.

    Selects the oldest queued job for each type in priority order,
    then claims the first one found by setting status=processing.
    """
    for job_type in _JOB_TYPE_PRIORITY:
        result = await db.execute(
            select(SyncRequest)
            .where(
                SyncRequest.status == "queued",
                SyncRequest.type == job_type,
            )
            .order_by(SyncRequest.created_at.asc())
            .limit(1)
        )
        job = result.scalar_one_or_none()
        if job:
            job.status = "processing"
            job.started_at = _utc_now()
            await db.commit()
            await db.refresh(job)
            return job

    return None


async def _mark_completed(db: AsyncSession, job_id: int) -> None:
    """Mark a sync_request as completed."""
    result = await db.execute(select(SyncRequest).where(SyncRequest.id == job_id))
    job = result.scalar_one_or_none()
    if job:
        job.status = "completed"
        job.completed_at = _utc_now()
        await db.commit()


async def _mark_failed_or_requeue(db: AsyncSession, job_id: int, error_msg: str) -> None:
    """
    On error: increment retry_count.
    If retry_count < max_retries: re-queue (status=queued).
    Else: mark status=failed with error_message.
    """
    result = await db.execute(select(SyncRequest).where(SyncRequest.id == job_id))
    job = result.scalar_one_or_none()
    if not job:
        return

    job.retry_count = (job.retry_count or 0) + 1
    max_retries = job.max_retries or 3

    if job.retry_count < max_retries:
        job.status = "queued"
        job.started_at = None
        logger.warning(
            "[SYNC_REQUEST_WORKER] requeued id=%s type=%s retry_count=%s max_retries=%s",
            job_id,
            job.type,
            job.retry_count,
            max_retries,
        )
    else:
        job.status = "failed"
        job.error_message = error_msg[:1000]
        job.completed_at = _utc_now()
        logger.error(
            "[SYNC_REQUEST_WORKER] failed id=%s type=%s retry_count=%s error=%s",
            job_id,
            job.type,
            job.retry_count,
            error_msg[:200],
        )

    await db.commit()


async def _update_webhook_event_status(
    brand_id: str,
    object_id: Optional[str],
    job_type: str,
    success: bool,
    error_msg: Optional[str] = None,
) -> None:
    """
    Update leaflink_webhook_events.status and processed_at for the matching event.

    Matches on brand_id + object_id + object_type derived from job_type.
    Only updates the most recent pending event to avoid over-updating.
    """
    if job_type not in ("webhook_order", "webhook_product"):
        return
    if not object_id:
        return

    object_type = "order" if job_type == "webhook_order" else "product"
    new_status = "processed" if success else "failed"
    now = _utc_now()

    try:
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(LeafLinkWebhookEvent)
                .where(
                    LeafLinkWebhookEvent.brand_id == brand_id,
                    LeafLinkWebhookEvent.object_id == object_id,
                    LeafLinkWebhookEvent.object_type == object_type,
                    LeafLinkWebhookEvent.status == "pending",
                )
                .order_by(LeafLinkWebhookEvent.created_at.desc())
                .limit(1)
            )
            event = result.scalar_one_or_none()
            if event:
                event.status = new_status
                event.processed_at = now
                if error_msg:
                    event.error_message = error_msg[:1000]
                await db.commit()
    except Exception as exc:
        logger.warning(
            "[SYNC_REQUEST_WORKER] webhook_event_update_failed object_id=%s error=%s",
            object_id,
            str(exc)[:200],
        )


async def run_sync_request_worker() -> None:
    """
    Main worker loop. Polls sync_requests for queued jobs and processes them.

    Runs indefinitely as an asyncio background task. Sleeps
    WORKER_POLL_INTERVAL seconds between polls when no jobs are found.
    """
    logger.info("[SYNC_REQUEST_WORKER] started poll_interval=%ss", WORKER_POLL_INTERVAL)

    while True:
        try:
            async with AsyncSessionLocal() as db:
                job = await _claim_next_job(db)

            if not job:
                await asyncio.sleep(WORKER_POLL_INTERVAL)
                continue

            job_id = job.id
            job_type = job.type or "full_resync"
            brand_id = job.brand_id
            object_id = job.object_id

            logger.info(
                "[SYNC_REQUEST_WORKER] claimed id=%s type=%s object_id=%s brand_id=%s",
                job_id,
                job_type,
                object_id,
                brand_id,
            )

            try:
                async with AsyncSessionLocal() as proc_db:
                    await process_sync_request(proc_db, job)

                async with AsyncSessionLocal() as done_db:
                    await _mark_completed(done_db, job_id)

                await _update_webhook_event_status(
                    brand_id=brand_id,
                    object_id=object_id,
                    job_type=job_type,
                    success=True,
                )

                logger.info(
                    "[SYNC_REQUEST_WORKER] completed id=%s type=%s brand_id=%s",
                    job_id,
                    job_type,
                    brand_id,
                )

            except asyncio.CancelledError:
                raise

            except Exception as exc:
                error_msg = str(exc)[:500]
                logger.error(
                    "[SYNC_REQUEST_WORKER] job_error id=%s type=%s brand_id=%s error=%s",
                    job_id,
                    job_type,
                    brand_id,
                    error_msg,
                    exc_info=True,
                )

                async with AsyncSessionLocal() as err_db:
                    await _mark_failed_or_requeue(err_db, job_id, error_msg)

                await _update_webhook_event_status(
                    brand_id=brand_id,
                    object_id=object_id,
                    job_type=job_type,
                    success=False,
                    error_msg=error_msg,
                )

        except asyncio.CancelledError:
            logger.info("[SYNC_REQUEST_WORKER] cancelled — shutting down")
            raise

        except Exception as loop_exc:
            logger.error(
                "[SYNC_REQUEST_WORKER] loop_error error=%s",
                str(loop_exc)[:300],
                exc_info=True,
            )
            await asyncio.sleep(WORKER_POLL_INTERVAL)
