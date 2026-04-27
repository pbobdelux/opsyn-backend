import base64
import logging
import time
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from auth.watchdog_auth import require_watchdog_auth
from database import get_db
from models import BrandAPICredential, Order, SyncStatus
from services.watchdog import WatchdogClient
from utils.json_utils import make_json_safe

logger = logging.getLogger("orders")

router = APIRouter(prefix="/orders", tags=["orders"])

watchdog = WatchdogClient()


@router.get("")
def get_orders():
    return {
        "items": [],
        "count": 0,
        "message": "Orders endpoint is live",
    }


@router.get("/sync/{brand_id}/status")
async def get_sync_status(
    brand_id: str,
    db: AsyncSession = Depends(get_db),
    _: None = Depends(require_watchdog_auth),
):
    """
    Return the current sync status for a brand.

    Requires ``Authorization: Bearer <OPSYN_WATCHDOG_SECRET>`` header.

    Response shape:
    {
      "onboarding_status": "connecting | syncing | processing | complete | failed",
      "total_in_database": 15921,
      "total_fetched_from_leaflink": 15921,
      "percent_complete": 100,
      "last_progress_timestamp": "2026-04-27T10:31:42Z",
      "latest_order_date": "2026-04-26T23:59:59Z",
      "oldest_order_date": "2020-01-01T00:00:00Z",
      "sync_error": null
    }
    """
    logger.info("[Watchdog] status_endpoint_hit brand=%s", brand_id)

    result = await db.execute(
        select(SyncStatus).where(SyncStatus.brand_id == brand_id)
    )
    sync_status = result.scalar_one_or_none()

    if sync_status is None:
        # No sync has been initiated yet for this brand.
        return {
            "onboarding_status": "connecting",
            "total_in_database": 0,
            "total_fetched_from_leaflink": 0,
            "percent_complete": 0,
            "last_progress_timestamp": None,
            "latest_order_date": None,
            "oldest_order_date": None,
            "sync_error": None,
        }

    def _iso(dt: datetime | None) -> str | None:
        if dt is None:
            return None
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    return {
        "onboarding_status": sync_status.status,
        "total_in_database": sync_status.total_in_database,
        "total_fetched_from_leaflink": sync_status.total_fetched,
        "percent_complete": sync_status.percent_complete,
        "last_progress_timestamp": _iso(sync_status.last_progress_timestamp),
        "latest_order_date": _iso(sync_status.latest_order_date),
        "oldest_order_date": _iso(sync_status.oldest_order_date),
        "sync_error": sync_status.sync_error,
    }


@router.get("/sync")
async def orders_sync(
    request: Request,
    brand_id: Optional[str] = Query(None, description="Brand slug or ID to filter orders (e.g., 'noble-nectar')"),
    brand_slug: Optional[str] = Query(None, description="Alias for brand_id — brand slug or ID"),
    updated_after: Optional[str] = Query(None, description="ISO timestamp - only return orders updated after this time (ignored when force_full=true)"),
    cursor: Optional[str] = Query(None, description="Pagination cursor from previous response"),
    limit: int = Query(500, ge=1, le=1000, description="Number of orders to return (default 500, max 1000)"),
    force_full: bool = Query(False, description="Force a complete backfill from LeafLink, ignoring updated_after"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get orders for incremental sync with cursor-based pagination.

    Supports efficient incremental sync by:
    - Returning only orders updated after a given timestamp
    - Using cursor-based pagination to avoid offset issues
    - Sorting by updated_at for consistent ordering
    - Including stable order ID fields for deduplication

    Query params:
    - brand_id: Brand slug or ID to filter orders (e.g., "noble-nectar")
    - brand_slug: Alias for brand_id
    - updated_after: ISO timestamp (e.g., "2026-04-25T12:30:00Z") — ignored when force_full=true
    - cursor: Pagination cursor from previous response
    - limit: Number of orders (1-1000, default 500)
    - force_full: true/false — force complete backfill from LeafLink (default false)

    Response includes:
    - orders: Array of order objects
    - sync_metadata: Sync statistics (total fetched, pages, duration, etc.)
    - next_cursor: Cursor for next page (if has_more=true)
    - has_more: Whether more orders exist
    - server_time: Current server time
    - sync_version: Sync protocol version
    - last_synced_at: Timestamp of this sync
    """
    endpoint_start = time.monotonic()

    try:
        # Resolve brand filter — accept brand_id, brand_slug, or camelCase brandId
        # from query params. brand_slug is an alias; brandId is tolerated for iOS clients.
        brand_filter: Optional[str] = (
            brand_id
            or brand_slug
            or request.query_params.get("brandId")
        )

        logger.info(
            "[OrdersSync] request_start path=%s brand=%s org=%s updated_after=%s cursor=%s limit=%s force_full=%s",
            request.url.path,
            brand_filter,
            request.headers.get("x-opsyn-org") or request.headers.get("x-org-id"),
            updated_after,
            cursor[:8] + "..." if cursor else None,
            limit,
            force_full,
        )

        # ------------------------------------------------------------------ #
        # Optional LeafLink sync before serving data                          #
        # ------------------------------------------------------------------ #
        sync_metadata: dict = {}

        if brand_filter:
            try:
                from services.leaflink_client import LeafLinkClient
                from services.leaflink_sync import sync_leaflink_orders

                # Look up credentials for this brand.
                cred_result = await db.execute(
                    select(BrandAPICredential).where(
                        BrandAPICredential.brand_id == brand_filter,
                        BrandAPICredential.integration_name == "leaflink",
                        BrandAPICredential.is_active == True,
                    )
                )
                cred = cred_result.scalar_one_or_none()

                if cred:
                    api_key: str = cred.api_key or ""
                    company_id: str = cred.company_id or ""

                    logger.info(
                        "[OrdersSync] triggering_leaflink_sync brand=%s force_full=%s",
                        brand_filter,
                        force_full,
                    )

                    # -------------------------------------------------- #
                    # Watchdog: create/reset SyncStatus and emit started  #
                    # -------------------------------------------------- #
                    _now = datetime.now(timezone.utc)
                    _ss_result = await db.execute(
                        select(SyncStatus).where(SyncStatus.brand_id == brand_filter)
                    )
                    _ss = _ss_result.scalar_one_or_none()
                    if _ss is None:
                        _ss = SyncStatus(brand_id=brand_filter)
                        db.add(_ss)
                    _ss.status = "syncing"
                    _ss.total_fetched = 0
                    _ss.percent_complete = 0
                    _ss.sync_error = None
                    _ss.last_progress_timestamp = _now
                    await db.flush()
                    await db.commit()

                    watchdog.sync_started(brand_filter)

                    client = LeafLinkClient(api_key=api_key, company_id=company_id)

                    # force_full=True → fetch all pages (unlimited); otherwise fetch recent (5 pages)
                    max_pages_arg = None if force_full else 5
                    fetch_result = client.fetch_recent_orders(
                        max_pages=max_pages_arg,
                        normalize=True,
                        brand=brand_filter,
                    )
                    orders_from_leaflink = fetch_result["orders"]
                    pages_fetched = fetch_result["pages_fetched"]

                    logger.info(
                        "[OrdersSync] leaflink_fetch_complete brand=%s orders=%s pages=%s force_full=%s",
                        brand_filter,
                        len(orders_from_leaflink),
                        pages_fetched,
                        force_full,
                    )

                    # -------------------------------------------------- #
                    # Watchdog: emit progress after fetch                 #
                    # -------------------------------------------------- #
                    _total_fetched = len(orders_from_leaflink)
                    _pct = min(50, int((_total_fetched / max(1, _total_fetched)) * 50)) if _total_fetched else 0
                    watchdog.sync_progress(
                        brand_filter,
                        total_fetched=_total_fetched,
                        total_in_database=0,
                        percent_complete=_pct,
                    )

                    async with db.begin():
                        sync_result = await sync_leaflink_orders(
                            db,
                            brand_filter,
                            orders_from_leaflink,
                            pages_fetched=pages_fetched,
                        )

                    sync_metadata["total_fetched_from_leaflink"] = sync_result.get("orders_fetched", 0)
                    sync_metadata["pages_fetched"] = sync_result.get("pages_fetched", pages_fetched)
                    sync_metadata["sync_duration_seconds"] = sync_result.get("sync_duration_seconds", 0)
                    sync_metadata["used_force_full"] = force_full

                    newest = sync_result.get("newest_order_date")
                    sync_metadata["latest_order_date"] = newest.isoformat() if newest else None

                    # -------------------------------------------------- #
                    # Watchdog: count DB total and emit completed/failed  #
                    # -------------------------------------------------- #
                    _count_res = await db.execute(
                        select(func.count(Order.id)).where(Order.brand_id == brand_filter)
                    )
                    _db_total = _count_res.scalar_one()
                    _latest_iso = sync_metadata.get("latest_order_date")

                    if sync_result.get("ok"):
                        # Update SyncStatus to complete.
                        _ss_result2 = await db.execute(
                            select(SyncStatus).where(SyncStatus.brand_id == brand_filter)
                        )
                        _ss2 = _ss_result2.scalar_one_or_none()
                        if _ss2:
                            _ss2.status = "complete"
                            _ss2.total_fetched = _total_fetched
                            _ss2.total_in_database = _db_total
                            _ss2.percent_complete = 100
                            _ss2.sync_error = None
                            _ss2.last_progress_timestamp = datetime.now(timezone.utc)
                            if newest:
                                _ss2.latest_order_date = newest
                            await db.commit()

                        watchdog.sync_completed(
                            brand_filter,
                            total_fetched=_total_fetched,
                            total_in_database=_db_total,
                            latest_order_date=_latest_iso,
                        )
                    else:
                        _err = sync_result.get("error", "unknown error")
                        # Update SyncStatus to failed.
                        _ss_result3 = await db.execute(
                            select(SyncStatus).where(SyncStatus.brand_id == brand_filter)
                        )
                        _ss3 = _ss_result3.scalar_one_or_none()
                        if _ss3:
                            _ss3.status = "failed"
                            _ss3.sync_error = _err
                            _ss3.last_progress_timestamp = datetime.now(timezone.utc)
                            await db.commit()

                        watchdog.sync_failed(
                            brand_filter,
                            error=_err,
                            total_fetched=_total_fetched,
                            total_in_database=_db_total,
                        )
                else:
                    logger.info(
                        "[OrdersSync] no_leaflink_credentials brand=%s — serving from DB only",
                        brand_filter,
                    )
                    sync_metadata["used_force_full"] = force_full

            except Exception as sync_exc:
                logger.warning(
                    "[OrdersSync] leaflink_sync_failed brand=%s error=%s — serving from DB",
                    brand_filter,
                    sync_exc,
                    exc_info=True,
                )
                sync_metadata["sync_error"] = str(sync_exc)
                sync_metadata["used_force_full"] = force_full

                # Watchdog: mark sync as failed in DB and emit event.
                try:
                    _ss_err_result = await db.execute(
                        select(SyncStatus).where(SyncStatus.brand_id == brand_filter)
                    )
                    _ss_err = _ss_err_result.scalar_one_or_none()
                    if _ss_err:
                        _ss_err.status = "failed"
                        _ss_err.sync_error = str(sync_exc)
                        _ss_err.last_progress_timestamp = datetime.now(timezone.utc)
                        await db.commit()
                except Exception:
                    pass  # Don't let watchdog DB writes mask the original error.

                watchdog.sync_failed(brand_filter, error=str(sync_exc))
        else:
            sync_metadata["used_force_full"] = force_full

        # ------------------------------------------------------------------ #
        # Parse updated_after (ignored when force_full=true)                  #
        # ------------------------------------------------------------------ #
        updated_after_dt = None
        if updated_after and not force_full:
            try:
                updated_after_dt = datetime.fromisoformat(updated_after.replace("Z", "+00:00"))
                logger.info("[OrdersSync] filter updated_after=%s", updated_after_dt.isoformat())
            except ValueError as e:
                logger.error("[OrdersSync] validation_error detail=invalid_updated_after error=%s", e)
                return {
                    "ok": False,
                    "error": "Invalid updated_after timestamp format. Use ISO 8601 (e.g., 2026-04-25T12:30:00Z)",
                    "data_source": "error",
                }
        elif force_full and updated_after:
            logger.info("[OrdersSync] force_full=true — ignoring updated_after=%s", updated_after)

        # ------------------------------------------------------------------ #
        # Decode cursor                                                        #
        # ------------------------------------------------------------------ #
        cursor_id = None
        if cursor:
            try:
                cursor_id = int(base64.b64decode(cursor).decode("utf-8"))
                logger.info("[OrdersSync] cursor_decoded cursor_id=%s", cursor_id)
            except Exception as e:
                logger.error("[OrdersSync] validation_error detail=invalid_cursor error=%s", e)
                return {
                    "ok": False,
                    "error": "Invalid cursor format",
                    "data_source": "error",
                }

        # ------------------------------------------------------------------ #
        # Build DB query                                                       #
        # ------------------------------------------------------------------ #
        query = select(Order)

        # Filter by brand when provided (brand_id column stores the brand slug/ID)
        if brand_filter:
            query = query.where(Order.brand_id == brand_filter)
            logger.info("[OrdersSync] filter brand=%s", brand_filter)

        # Filter by updated_after (skipped when force_full=true)
        if updated_after_dt:
            query = query.where(Order.updated_at >= updated_after_dt)

        # Filter by cursor (pagination)
        if cursor_id is not None:
            query = query.where(Order.id > cursor_id)

        # Sort by updated_at ascending for consistent incremental sync
        query = query.order_by(Order.updated_at.asc(), Order.id.asc())

        # Fetch limit + 1 to determine if there are more results
        query_with_limit = query.limit(limit + 1)

        result = await db.execute(query_with_limit)
        orders = result.scalars().all()

        # Determine if there are more results
        has_more = len(orders) > limit
        if has_more:
            orders = orders[:limit]

        # ------------------------------------------------------------------ #
        # Count total orders in DB for this brand                             #
        # ------------------------------------------------------------------ #
        count_query = select(func.count(Order.id))
        if brand_filter:
            count_query = count_query.where(Order.brand_id == brand_filter)
        count_result = await db.execute(count_query)
        total_in_database = count_result.scalar_one()

        # ------------------------------------------------------------------ #
        # Build response                                                       #
        # ------------------------------------------------------------------ #
        orders_data = []
        next_cursor = None

        for order in orders:
            order_dict = {
                "id": order.id,
                "external_order_id": order.external_order_id,
                "order_number": order.order_number,
                "customer_name": order.customer_name,
                "amount": float(order.amount) if order.amount else 0,
                "status": order.status,
                "brand_id": order.brand_id,
                "source": order.source,
                "review_status": order.review_status,
                "item_count": order.item_count,
                "unit_count": order.unit_count,
                "external_created_at": order.external_created_at.isoformat() if order.external_created_at else None,
                "external_updated_at": order.external_updated_at.isoformat() if order.external_updated_at else None,
                "created_at": order.created_at.isoformat() if order.created_at else None,
                "updated_at": order.updated_at.isoformat() if order.updated_at else None,
                "last_synced_at": order.last_synced_at.isoformat() if order.last_synced_at else None,
            }
            orders_data.append(order_dict)

            # Update cursor to last order's ID
            if order == orders[-1]:
                next_cursor = base64.b64encode(str(order.id).encode()).decode()

        server_time = datetime.now(timezone.utc).isoformat()

        # Populate remaining sync_metadata fields with defaults for any missing keys
        sync_metadata.setdefault("total_fetched_from_leaflink", None)
        sync_metadata.setdefault("pages_fetched", None)
        sync_metadata.setdefault("sync_duration_seconds", None)
        sync_metadata.setdefault("latest_order_date", None)
        sync_metadata["total_in_database"] = total_in_database
        sync_metadata["total_returned"] = len(orders_data)

        logger.info("[OrdersSync] db_total_orders=%s brand=%s", total_in_database, brand_filter)
        logger.info("[OrdersSync] returned_to_ios=%s brand=%s", len(orders_data), brand_filter)
        if sync_metadata.get("latest_order_date"):
            logger.info("[OrdersSync] latest_order_date=%s brand=%s", sync_metadata["latest_order_date"], brand_filter)

        logger.info(
            "[OrdersSync] response count=%s has_more=%s brand=%s total_in_db=%s",
            len(orders_data),
            has_more,
            brand_filter,
            total_in_database,
        )

        return make_json_safe({
            "ok": True,
            "data_source": "live",
            "orders": orders_data,
            "sync_metadata": sync_metadata,
            "next_cursor": next_cursor if has_more else None,
            "has_more": has_more,
            "server_time": server_time,
            "sync_version": 1,
            "last_synced_at": server_time,
        })

    except Exception as e:
        logger.error("[OrdersSync] validation_error detail=%s", e, exc_info=True)
        return {
            "ok": False,
            "error": str(e),
            "data_source": "error",
        }
