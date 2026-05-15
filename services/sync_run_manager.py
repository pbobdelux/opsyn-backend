"""
sync_run_manager.py — SyncRun lifecycle management service.

Provides the authoritative interface for creating, updating, and querying
SyncRun records. All sync code should go through this module rather than
touching SyncRun directly, so that business rules (one active run per brand,
stall detection, cursor-loop detection) are enforced in one place.
"""

import hashlib
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import AsyncSessionLocal
from models import Order, SyncRun

logger = logging.getLogger("sync_run_manager")

# How long without progress before a run is considered stalled (seconds)
STALL_THRESHOLD_SECONDS = 90

# How long a running sync can go without a heartbeat before it is considered
# orphaned and forcibly transitioned to 'failed' (30 minutes)
ORPHAN_TIMEOUT_SECONDS = 1800

# Minimum pages completed before ETA is meaningful
ETA_MIN_PAGES = 3


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _cursor_hash(cursor: Optional[str]) -> Optional[str]:
    """Return a short hash of a cursor string for safe logging (never log full cursor)."""
    if not cursor:
        return None
    return hashlib.sha256(cursor.encode()).hexdigest()[:12]


# ---------------------------------------------------------------------------
# Create / fetch
# ---------------------------------------------------------------------------

async def create_sync_run(
    db: AsyncSession,
    brand_id: str,
    mode: str = "full",
) -> SyncRun:
    """
    Create a new SyncRun for brand_id.

    Raises ValueError if a run is already active (queued or syncing) for this brand.
    The caller must commit the session after this returns.
    """
    # Reject duplicate starts
    existing = await get_active_run(db, brand_id)
    if existing:
        raise ValueError(
            f"Active sync run already exists for brand={brand_id} "
            f"run_id={existing.id} status={existing.status}"
        )

    run = SyncRun(
        brand_id=brand_id,
        integration_name="leaflink",
        status="queued",
        mode=mode,
        pages_synced=0,
        orders_loaded_this_run=0,
        current_page=1,
        started_at=_utc_now(),
    )
    db.add(run)
    await db.flush()  # populate run.id without committing

    logger.info(
        "[SYNC_RUN_STATE_TRANSITION] run_id=%s brand=%s from=none to=queued mode=%s",
        run.id,
        brand_id,
        mode,
    )
    logger.info(
        "[SyncRunManager] created run_id=%s brand=%s mode=%s",
        run.id,
        brand_id,
        mode,
    )
    return run


async def get_active_run(db: AsyncSession, brand_id: str) -> Optional[SyncRun]:
    """Return the currently active (queued or syncing) SyncRun for brand_id, or None."""
    result = await db.execute(
        select(SyncRun)
        .where(
            SyncRun.brand_id == brand_id,
            SyncRun.status.in_(["queued", "syncing"]),
        )
        .order_by(SyncRun.started_at.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


async def get_last_completed_run(db: AsyncSession, brand_id: str) -> Optional[SyncRun]:
    """Return the most recently completed SyncRun for brand_id, or None."""
    result = await db.execute(
        select(SyncRun)
        .where(
            SyncRun.brand_id == brand_id,
            SyncRun.status == "completed",
        )
        .order_by(SyncRun.completed_at.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


# ---------------------------------------------------------------------------
# Progress updates
# ---------------------------------------------------------------------------

async def update_progress(
    db: AsyncSession,
    sync_run_id: int,
    pages_synced: int,
    orders_loaded: int,
    cursor: Optional[str],
    page: int,
    total_pages: Optional[int] = None,
    total_orders_available: Optional[int] = None,
) -> None:
    """
    Persist progress after each successfully fetched page.

    Updates pages_synced, orders_loaded_this_run, current_cursor, current_page,
    last_successful_cursor, last_successful_page, and last_progress_at.
    Sets status to 'syncing' if it was 'queued'.
    """
    result = await db.execute(
        select(SyncRun).where(SyncRun.id == sync_run_id)
    )
    run = result.scalar_one_or_none()
    if run is None:
        logger.error("[SyncRunManager] update_progress run_not_found run_id=%s", sync_run_id)
        return

    now = _utc_now()
    run.pages_synced = pages_synced
    run.orders_loaded_this_run = orders_loaded
    run.current_cursor = cursor
    run.current_page = page
    run.last_successful_cursor = cursor
    run.last_successful_page = page
    run.last_progress_at = now
    run.updated_at = now

    if run.status == "queued":
        run.status = "syncing"
        logger.info(
            "[SYNC_RUN_STATE_TRANSITION] run_id=%s brand=%s from=queued to=syncing"
            " pages=%s orders=%s",
            sync_run_id,
            run.brand_id,
            pages_synced,
            orders_loaded,
        )

    if total_pages is not None:
        run.total_pages = total_pages
    if total_orders_available is not None:
        run.total_orders_available = total_orders_available

    logger.info(
        "[SyncRunManager] progress_updated run_id=%s brand=%s pages=%s/%s orders=%s cursor_hash=%s",
        sync_run_id,
        run.brand_id,
        pages_synced,
        run.total_pages,
        orders_loaded,
        _cursor_hash(cursor),
    )


async def mark_completed(db: AsyncSession, sync_run_id: int) -> None:
    """Mark a SyncRun as completed."""
    result = await db.execute(
        select(SyncRun).where(SyncRun.id == sync_run_id)
    )
    run = result.scalar_one_or_none()
    if run is None:
        logger.error("[SyncRunManager] mark_completed run_not_found run_id=%s", sync_run_id)
        return

    prior_status = run.status
    now = _utc_now()
    run.status = "completed"
    run.completed_at = now
    run.updated_at = now
    run.last_error = None

    logger.info(
        "[SYNC_RUN_STATE_TRANSITION] run_id=%s brand=%s from=%s to=completed"
        " pages=%s orders=%s",
        sync_run_id,
        run.brand_id,
        prior_status,
        run.pages_synced,
        run.orders_loaded_this_run,
    )
    logger.info(
        "[SyncRunManager] completed run_id=%s brand=%s pages=%s orders=%s",
        sync_run_id,
        run.brand_id,
        run.pages_synced,
        run.orders_loaded_this_run,
    )


async def mark_stalled(
    db: AsyncSession,
    sync_run_id: int,
    reason: str,
) -> None:
    """Mark a SyncRun as stalled with a human-readable reason."""
    result = await db.execute(
        select(SyncRun).where(SyncRun.id == sync_run_id)
    )
    run = result.scalar_one_or_none()
    if run is None:
        logger.error("[SyncRunManager] mark_stalled run_not_found run_id=%s", sync_run_id)
        return

    prior_status = run.status
    now = _utc_now()
    run.status = "stalled"
    run.stalled_reason = reason
    run.completed_at = now
    run.updated_at = now

    logger.warning(
        "[SYNC_RUN_STATE_TRANSITION] run_id=%s brand=%s from=%s to=stalled reason=%s pages=%s",
        sync_run_id,
        run.brand_id,
        prior_status,
        reason,
        run.pages_synced,
    )
    logger.warning(
        "[SyncRunManager] stalled run_id=%s brand=%s reason=%s pages=%s",
        sync_run_id,
        run.brand_id,
        reason,
        run.pages_synced,
    )


async def mark_failed(
    db: AsyncSession,
    sync_run_id: int,
    error: str,
) -> None:
    """Mark a SyncRun as failed with an error message."""
    result = await db.execute(
        select(SyncRun).where(SyncRun.id == sync_run_id)
    )
    run = result.scalar_one_or_none()
    if run is None:
        logger.error("[SyncRunManager] mark_failed run_not_found run_id=%s", sync_run_id)
        return

    prior_status = run.status
    now = _utc_now()
    run.status = "failed"
    run.last_error = error
    run.error_count = (run.error_count or 0) + 1
    run.completed_at = now
    run.updated_at = now

    logger.error(
        "[SYNC_RUN_STATE_TRANSITION] run_id=%s brand=%s from=%s to=failed"
        " error=%s pages=%s",
        sync_run_id,
        run.brand_id,
        prior_status,
        error[:200],
        run.pages_synced,
    )
    logger.error(
        "[SyncRunManager] failed run_id=%s brand=%s error=%s pages=%s",
        sync_run_id,
        run.brand_id,
        error,
        run.pages_synced,
    )


# ---------------------------------------------------------------------------
# Stuck-running protection — orphaned run recovery
# ---------------------------------------------------------------------------

async def recover_orphaned_runs() -> int:
    """Scan for sync_runs stuck in 'running'/'queued'/'syncing' state for
    longer than ORPHAN_TIMEOUT_SECONDS and transition them to 'failed'.

    Called at worker startup and periodically to prevent runs from being
    permanently stuck when a worker crashes without cleaning up.

    Returns the number of runs that were recovered (transitioned to failed).
    """
    recovered = 0
    cutoff = _utc_now() - timedelta(seconds=ORPHAN_TIMEOUT_SECONDS)

    try:
        async with AsyncSessionLocal() as db:
            async with db.begin():
                result = await db.execute(
                    select(SyncRun).where(
                        SyncRun.status.in_(["queued", "syncing"]),
                    )
                )
                candidates = result.scalars().all()

                for run in candidates:
                    # Determine the last activity timestamp
                    last_activity = run.last_progress_at or run.started_at
                    if last_activity is None:
                        continue

                    # Normalize to UTC-aware for comparison
                    if last_activity.tzinfo is None:
                        last_activity = last_activity.replace(tzinfo=timezone.utc)
                    else:
                        last_activity = last_activity.astimezone(timezone.utc)

                    if last_activity < cutoff:
                        elapsed = (_utc_now() - last_activity).total_seconds()
                        prior_status = run.status
                        now = _utc_now()
                        run.status = "failed"
                        run.last_error = (
                            f"orphaned_run: no progress for {int(elapsed)}s "
                            f"(threshold={ORPHAN_TIMEOUT_SECONDS}s)"
                        )
                        run.error_count = (run.error_count or 0) + 1
                        run.completed_at = now
                        run.updated_at = now

                        logger.error(
                            "[SYNC_RUN_STATE_TRANSITION] run_id=%s brand=%s from=%s to=failed"
                            " reason=orphaned_run elapsed_seconds=%s threshold=%s",
                            run.id,
                            run.brand_id,
                            prior_status,
                            int(elapsed),
                            ORPHAN_TIMEOUT_SECONDS,
                        )
                        logger.error(
                            "[SyncRunManager] orphaned_run_recovered run_id=%s brand=%s"
                            " prior_status=%s elapsed_seconds=%s",
                            run.id,
                            run.brand_id,
                            prior_status,
                            int(elapsed),
                        )
                        recovered += 1

        if recovered > 0:
            logger.warning(
                "[SyncRunManager] orphan_recovery_complete recovered=%s",
                recovered,
            )
        else:
            logger.info("[SyncRunManager] orphan_recovery_complete recovered=0 (no orphans found)")

    except Exception as exc:
        logger.error(
            "[SyncRunManager] orphan_recovery_error error=%s",
            str(exc)[:300],
        )

    return recovered


async def mark_failed_if_running(
    sync_run_id: int,
    error: str,
) -> None:
    """Transition a sync run to 'failed' only if it is currently in an active
    state (queued, syncing).  Safe to call from error handlers and finally
    blocks — never raises, never double-fails a run that is already terminal.

    Opens its own DB session so it can be called from any context.
    """
    try:
        async with AsyncSessionLocal() as db:
            async with db.begin():
                result = await db.execute(
                    select(SyncRun).where(SyncRun.id == sync_run_id)
                )
                run = result.scalar_one_or_none()
                if run is None:
                    logger.warning(
                        "[SyncRunManager] mark_failed_if_running run_not_found run_id=%s",
                        sync_run_id,
                    )
                    return

                if run.status not in ("queued", "syncing"):
                    logger.info(
                        "[SyncRunManager] mark_failed_if_running skipped run_id=%s"
                        " current_status=%s (already terminal)",
                        sync_run_id,
                        run.status,
                    )
                    return

                prior_status = run.status
                now = _utc_now()
                run.status = "failed"
                run.last_error = error[:500]
                run.error_count = (run.error_count or 0) + 1
                run.completed_at = now
                run.updated_at = now

                logger.error(
                    "[SYNC_RUN_STATE_TRANSITION] run_id=%s brand=%s from=%s to=failed"
                    " reason=mark_failed_if_running error=%s",
                    sync_run_id,
                    run.brand_id,
                    prior_status,
                    error[:200],
                )
    except Exception as exc:
        logger.error(
            "[SyncRunManager] mark_failed_if_running_error run_id=%s error=%s",
            sync_run_id,
            str(exc)[:200],
        )


# ---------------------------------------------------------------------------
# Reset
# ---------------------------------------------------------------------------

async def reset_run(
    db: AsyncSession,
    brand_id: str,
    hard: bool = False,
) -> dict:
    """
    Clear sync state for brand_id.

    - Marks any active SyncRun as stalled (reason=manual_reset).
    - If hard=True, deletes all Order rows for the brand.
    - Returns a summary dict with order_count and action taken.
    """
    # Mark active runs as stalled
    active = await get_active_run(db, brand_id)
    if active:
        await mark_stalled(db, active.id, reason="manual_reset")
        logger.info(
            "[SyncRunManager] reset_active_run run_id=%s brand=%s hard=%s",
            active.id,
            brand_id,
            hard,
        )

    # Count orders before potential deletion
    from sqlalchemy import func
    count_result = await db.execute(
        select(func.count(Order.id)).where(Order.brand_id == brand_id)
    )
    order_count = count_result.scalar_one() or 0

    if hard:
        from sqlalchemy import delete
        await db.execute(
            delete(Order).where(Order.brand_id == brand_id)
        )
        logger.warning(
            "[SyncRunManager] hard_reset brand=%s deleted_orders=%s",
            brand_id,
            order_count,
        )
        return {
            "action": "hard_reset",
            "brand_id": brand_id,
            "orders_deleted": order_count,
            "orders_remaining": 0,
            "active_run_cleared": active is not None,
        }

    return {
        "action": "soft_reset",
        "brand_id": brand_id,
        "orders_preserved": order_count,
        "orders_remaining": order_count,
        "active_run_cleared": active is not None,
    }


# ---------------------------------------------------------------------------
# Stall detection
# ---------------------------------------------------------------------------

def _ensure_utc_for_compare(dt: datetime, field_name: str = "unknown") -> datetime:
    """Normalize a datetime to UTC-aware before arithmetic/comparison.

    Handles naive datetimes (assumes UTC) and aware datetimes (converts to UTC).
    Logs [DATETIME_NORMALIZED] when a conversion is applied.
    """
    if dt.tzinfo is None:
        normalized = dt.replace(tzinfo=timezone.utc)
        logger.info(
            "[DATETIME_NORMALIZED] field=%s original=%s (naive) normalized=%s (UTC-aware)",
            field_name,
            dt.isoformat(),
            normalized.isoformat(),
        )
        return normalized
    return dt.astimezone(timezone.utc)


def detect_stalled(sync_run: SyncRun, stall_threshold_seconds: int = STALL_THRESHOLD_SECONDS) -> bool:
    """
    Return True if the sync run has not made progress for stall_threshold_seconds.

    A run is stalled if:
    - status is 'queued' or 'syncing'
    - last_progress_at is set and older than the threshold
    """
    if sync_run.status not in ("queued", "syncing"):
        return False
    if sync_run.last_progress_at is None:
        # No progress recorded yet — check if started_at is old
        try:
            a = _utc_now()
            b = _ensure_utc_for_compare(sync_run.started_at, "started_at")
            elapsed = (a - b).total_seconds()
        except TypeError as e:
            logger.error(
                "[DATETIME_COMPARE_ERROR] a=%s tz_a=%s b=%s tz_b=%s error=%s",
                a,
                getattr(a, "tzinfo", None),
                sync_run.started_at,
                getattr(sync_run.started_at, "tzinfo", None),
                str(e),
            )
            raise
        return elapsed > stall_threshold_seconds
    try:
        a = _utc_now()
        b = _ensure_utc_for_compare(sync_run.last_progress_at, "last_progress_at")
        elapsed = (a - b).total_seconds()
    except TypeError as e:
        logger.error(
            "[DATETIME_COMPARE_ERROR] a=%s tz_a=%s b=%s tz_b=%s error=%s",
            a,
            getattr(a, "tzinfo", None),
            sync_run.last_progress_at,
            getattr(sync_run.last_progress_at, "tzinfo", None),
            str(e),
        )
        raise
    return elapsed > stall_threshold_seconds


# ---------------------------------------------------------------------------
# Serialization helper
# ---------------------------------------------------------------------------

def serialize_sync_run(run: SyncRun, is_stalled: bool = False) -> dict:
    """Serialize a SyncRun to a JSON-safe dict for API responses."""
    percent = run.percent_complete()
    eta_minutes = None
    if (
        run.status == "syncing"
        and not is_stalled
        and run.pages_synced >= ETA_MIN_PAGES
        and run.last_progress_at is not None
    ):
        eta_minutes = run.estimated_completion_minutes()

    return {
        "id": run.id,
        "brand_id": run.brand_id,
        "integration_name": run.integration_name,
        "status": "stalled" if is_stalled else run.status,
        "mode": run.mode,
        "pages_synced": run.pages_synced,
        "total_pages": run.total_pages,
        "percent_complete": round(percent, 2) if percent is not None else None,
        "orders_loaded_this_run": run.orders_loaded_this_run,
        "total_orders_available": run.total_orders_available,
        "current_page": run.current_page,
        "last_successful_page": run.last_successful_page,
        "cursor_hash": _cursor_hash(run.current_cursor),
        "last_successful_cursor_hash": _cursor_hash(run.last_successful_cursor),
        "stalled_reason": run.stalled_reason,
        "last_error": run.last_error,
        "error_count": run.error_count,
        "worker_id": run.worker_id,
        "started_at": run.started_at.isoformat() if run.started_at else None,
        "last_progress_at": run.last_progress_at.isoformat() if run.last_progress_at else None,
        "completed_at": run.completed_at.isoformat() if run.completed_at else None,
        "created_at": run.created_at.isoformat() if run.created_at else None,
        "updated_at": run.updated_at.isoformat() if run.updated_at else None,
        "estimated_completion_minutes": round(eta_minutes, 1) if eta_minutes is not None else None,
        "is_stalled": is_stalled,
    }
