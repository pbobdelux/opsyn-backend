"""
reconciliation_validator.py — Sync completion validation and gap analysis.

Provides:
  - validate_sync_reconciliation(): compares fetched vs local counts and
    determines whether a sync run truly completed or stopped early.
  - check_pagination_complete(): returns (is_complete, reason) for a SyncRun.

These utilities prevent false-success status when pagination stops before
the LeafLink estimate is reached (the 12,300 → 16,074 plateau scenario).
"""

import logging
from typing import Optional

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from models import SyncRun

logger = logging.getLogger("reconciliation_validator")

# Gap threshold: if local count is more than this % below the LeafLink
# estimate AND pagination is incomplete, report partial_reconciliation_gap.
GAP_THRESHOLD_PCT = 2.0


async def validate_sync_reconciliation(
    sync_run_id: int,
    db: AsyncSession,
) -> dict:
    """
    Validate sync completion and explain any gap between local and LeafLink counts.

    Queries:
      - sync_runs table for the run record
      - orders table for total and visible counts for the brand
      - dead_letter_line_items for dead-letter count

    Determines sync status:
      success                    — pagination reached end, gap within threshold
      partial_timeout            — pagination stopped but next_url exists (timeout)
      partial_page_limit         — pagination stopped at page limit, next_url exists
      partial_reconciliation_gap — local count < estimate by >GAP_THRESHOLD_PCT%
                                   AND next_url exists
      failed                     — fatal DB errors or explicit failure

    Args:
        sync_run_id: ID of the SyncRun to validate.
        db:          AsyncSession to use for queries.

    Returns:
        dict with all counts, comparisons, and status.
    """
    result: dict = {
        "sync_run_id": sync_run_id,
        "status": "unknown",
        "leaflink_total_estimate": 0,
        "fetched_total_this_run": 0,
        "inserted_headers": 0,
        "updated_headers": 0,
        "skipped_headers": 0,
        "total_local_orders_after": 0,
        "visible_local_orders_after": 0,
        "line_items_inserted": 0,
        "line_items_updated": 0,
        "dead_letter_count": 0,
        "gap_percentage": 0.0,
        "pagination_complete": False,
        "pagination_reason": "unknown",
        "brand_id": None,
    }

    try:
        # Load the SyncRun record
        run_result = await db.execute(
            select(SyncRun).where(SyncRun.id == sync_run_id)
        )
        run = run_result.scalar_one_or_none()

        if run is None:
            logger.error(
                "[SYNC_RECONCILIATION] run_not_found sync_run_id=%s",
                sync_run_id,
            )
            result["status"] = "failed"
            return result

        brand_id = run.brand_id
        result["brand_id"] = brand_id

        # Populate from SyncRun record
        result["leaflink_total_estimate"] = run.total_orders_available or 0
        result["fetched_total_this_run"] = run.orders_loaded_this_run or 0
        result["inserted_headers"] = run.orders_inserted or 0
        result["updated_headers"] = run.orders_updated or 0
        result["line_items_inserted"] = run.line_items_inserted or 0
        result["line_items_updated"] = run.line_items_updated or 0
        result["dead_letter_count"] = run.dead_letters_created or 0

        # Query total local orders for this brand
        try:
            total_res = await db.execute(
                text(
                    "SELECT COUNT(*) FROM orders"
                    " WHERE brand_id::text = :brand_id"
                ),
                {"brand_id": brand_id},
            )
            result["total_local_orders_after"] = total_res.scalar() or 0
        except Exception as count_exc:
            logger.warning(
                "[SYNC_RECONCILIATION] total_count_error brand=%s error=%s",
                brand_id,
                str(count_exc)[:200],
            )
            await db.rollback()

        # Query visible local orders (excluding cancelled — same filter as app)
        try:
            visible_res = await db.execute(
                text(
                    "SELECT COUNT(*) FROM orders"
                    " WHERE brand_id::text = :brand_id"
                    " AND status != 'cancelled'"
                ),
                {"brand_id": brand_id},
            )
            result["visible_local_orders_after"] = visible_res.scalar() or 0
        except Exception as vis_exc:
            logger.warning(
                "[SYNC_RECONCILIATION] visible_count_error brand=%s error=%s",
                brand_id,
                str(vis_exc)[:200],
            )
            await db.rollback()

        # Query dead-letter count for this brand (may differ from run counter)
        try:
            dl_res = await db.execute(
                text(
                    "SELECT COUNT(*) FROM dead_letter_line_items"
                    " WHERE brand_id::text = :brand_id"
                ),
                {"brand_id": brand_id},
            )
            # Use the larger of the two counts (run counter vs live table)
            live_dl_count = dl_res.scalar() or 0
            result["dead_letter_count"] = max(result["dead_letter_count"], live_dl_count)
        except Exception as dl_exc:
            logger.warning(
                "[SYNC_RECONCILIATION] dead_letter_count_error brand=%s error=%s",
                brand_id,
                str(dl_exc)[:200],
            )
            await db.rollback()

        # Check pagination completeness
        is_complete, pagination_reason = check_pagination_complete(run)
        result["pagination_complete"] = is_complete
        result["pagination_reason"] = pagination_reason

        # Calculate gap percentage
        estimate = result["leaflink_total_estimate"]
        local = result["total_local_orders_after"]
        if estimate > 0:
            gap_pct = max(0.0, (estimate - local) / estimate * 100.0)
        else:
            gap_pct = 0.0
        result["gap_percentage"] = round(gap_pct, 2)

        # Determine sync status
        if run.status == "failed":
            result["status"] = "failed"
        elif not is_complete:
            # Pagination did not reach the end — classify the reason
            if "timeout" in pagination_reason:
                result["status"] = "partial_timeout"
            elif "page_limit" in pagination_reason:
                result["status"] = "partial_page_limit"
            elif gap_pct > GAP_THRESHOLD_PCT:
                result["status"] = "partial_reconciliation_gap"
            else:
                result["status"] = "partial_reconciliation_gap"
        elif gap_pct > GAP_THRESHOLD_PCT and estimate > 0:
            # Pagination complete but gap is too large — still flag it
            result["status"] = "partial_reconciliation_gap"
        else:
            result["status"] = "success"

    except Exception as exc:
        logger.error(
            "[SYNC_RECONCILIATION] validation_error sync_run_id=%s error=%s",
            sync_run_id,
            str(exc)[:300],
        )
        result["status"] = "failed"
        return result

    logger.info(
        "[SYNC_RECONCILIATION] status=%s leaflink_estimate=%d fetched=%d"
        " inserted=%d updated=%d total_local=%d visible_local=%d"
        " gap_pct=%.1f%% dead_letters=%d pagination_complete=%s"
        " pagination_reason=%s brand=%s sync_run_id=%s",
        result["status"],
        result["leaflink_total_estimate"],
        result["fetched_total_this_run"],
        result["inserted_headers"],
        result["updated_headers"],
        result["total_local_orders_after"],
        result["visible_local_orders_after"],
        result["gap_percentage"],
        result["dead_letter_count"],
        result["pagination_complete"],
        result["pagination_reason"],
        result["brand_id"],
        sync_run_id,
    )

    return result


def check_pagination_complete(sync_run: SyncRun) -> tuple[bool, str]:
    """
    Determine whether pagination reached the true end of the result set.

    Returns (is_complete, reason) where:
      is_complete = True only if:
        - last_next_url is NULL (reached end of pagination), OR
        - pages_synced >= total_pages (if total_pages is known), OR
        - orders_loaded_this_run >= total_orders_available (if estimate is known)

      reason explains why:
        "reached_end"          — next_url is NULL (definitive end)
        "page_count_reached"   — pages_synced >= total_pages
        "orders_count_reached" — orders_loaded >= total_orders_available
        "page_limit_hit"       — stopped at page limit, next_url still present
        "timeout_occurred"     — stopped due to timeout, next_url still present
        "cursor_present"       — next_url is present, pagination incomplete
        "unknown"              — cannot determine

    Args:
        sync_run: SyncRun ORM object.

    Returns:
        (is_complete: bool, reason: str)
    """
    # Check 1: next_url is NULL → definitive end of pagination
    last_next_url = getattr(sync_run, "last_next_url", None)
    current_cursor = getattr(sync_run, "current_cursor", None)
    has_next_url = bool(last_next_url or current_cursor)

    if not has_next_url:
        return (True, "reached_end")

    # Check 2: pages_synced >= total_pages (if total_pages is known)
    pages_synced = sync_run.pages_synced or 0
    total_pages = sync_run.total_pages
    if total_pages is not None and total_pages > 0 and pages_synced >= total_pages:
        return (True, "page_count_reached")

    # Check 3: orders_loaded >= total_orders_available (if estimate is known)
    orders_loaded = sync_run.orders_loaded_this_run or 0
    total_available = sync_run.total_orders_available
    if total_available is not None and total_available > 0 and orders_loaded >= total_available:
        return (True, "orders_count_reached")

    # Pagination is incomplete — classify the reason
    run_status = sync_run.status or ""
    stalled_reason = getattr(sync_run, "stalled_reason", None) or ""
    last_error = getattr(sync_run, "last_error", None) or ""

    if "timeout" in run_status.lower() or "timeout" in stalled_reason.lower() or "timeout" in last_error.lower():
        return (False, "timeout_occurred")

    if "page_limit" in stalled_reason.lower() or "page_limit" in last_error.lower():
        return (False, "page_limit_hit")

    if has_next_url:
        return (False, "cursor_present")

    return (False, "unknown")
