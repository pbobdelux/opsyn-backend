import base64
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import BrandAPICredential, Order
from services.watchdog_client import emit_watchdog_event
from utils.json_utils import make_json_safe

logger = logging.getLogger("orders")

router = APIRouter(prefix="/orders", tags=["orders"])


@router.get("")
def get_orders():
    return {
        "items": [],
        "count": 0,
        "message": "Orders endpoint is live",
    }


@router.get("/debug/orders-sync")
async def debug_orders_sync(
    brand: Optional[str] = Query(None, description="Brand slug to inspect (e.g. 'noble-nectar')"),
    db: AsyncSession = Depends(get_db),
):
    """
    Diagnostic endpoint — returns sync state for a given brand without triggering a sync.

    Returns:
    - backend_time: current server UTC time
    - last_leaflink_pull: last time LeafLink credentials were successfully used (last_sync_at)
    - last_twin_ingest: not yet tracked — always null
    - total_orders: total orders in DB for this brand
    - newest_order_date / oldest_order_date: date range of orders in DB
    - orders_by_status: count per status value
    - rejected_count: count of orders with status 'rejected'
    - last_error: last credential error message (if any)
    - leaflink_credentials_valid: whether an active credential exists with no last_error
    - sync_blocked: always false — blockers only affect routing, not order fetching
    """
    logger.info("[OrdersSync] debug_orders_sync brand=%s", brand)

    backend_time = datetime.now(timezone.utc).isoformat()

    # ── Credential lookup ────────────────────────────────────────────────────
    last_leaflink_pull: Optional[str] = None
    last_error: Optional[str] = None
    leaflink_credentials_valid = False

    try:
        cred_query = select(BrandAPICredential).where(
            BrandAPICredential.integration_name == "leaflink",
            BrandAPICredential.is_active == True,
        )
        if brand:
            cred_query = cred_query.where(BrandAPICredential.brand_id == brand)
        cred_query = cred_query.order_by(BrandAPICredential.last_sync_at.desc().nullslast()).limit(1)

        cred_result = await db.execute(cred_query)
        cred = cred_result.scalar_one_or_none()

        if cred:
            last_leaflink_pull = cred.last_sync_at.isoformat() if cred.last_sync_at else None
            last_error = cred.last_error
            # Credentials are valid when active and have no recorded error
            leaflink_credentials_valid = cred.is_active and not cred.last_error
            logger.info(
                "[OrdersSync] debug cred_found brand=%s last_sync_at=%s sync_status=%s has_error=%s",
                brand,
                last_leaflink_pull,
                cred.sync_status,
                bool(last_error),
            )
        else:
            logger.warning("[OrdersSync] debug no_active_credential brand=%s", brand)

    except Exception as cred_exc:
        logger.error("[OrdersSync] debug cred_lookup_failed brand=%s error=%s", brand, cred_exc)

    # ── Order stats from DB ──────────────────────────────────────────────────
    total_orders = 0
    newest_order_date: Optional[str] = None
    oldest_order_date: Optional[str] = None
    orders_by_status: dict = {}
    rejected_count = 0

    try:
        order_query = select(Order)
        if brand:
            order_query = order_query.where(Order.brand_id == brand)

        order_result = await db.execute(order_query)
        all_orders = order_result.scalars().all()
        total_orders = len(all_orders)

        newest_dt: Optional[datetime] = None
        oldest_dt: Optional[datetime] = None

        for order in all_orders:
            # Status counts
            status_key = (order.status or "unknown").lower()
            orders_by_status[status_key] = orders_by_status.get(status_key, 0) + 1
            if status_key == "rejected":
                rejected_count += 1

            # Date range — prefer external_updated_at, fall back to external_created_at
            ts = order.external_updated_at or order.external_created_at
            if ts:
                if newest_dt is None or ts > newest_dt:
                    newest_dt = ts
                if oldest_dt is None or ts < oldest_dt:
                    oldest_dt = ts

        newest_order_date = newest_dt.isoformat() if newest_dt else None
        oldest_order_date = oldest_dt.isoformat() if oldest_dt else None

        logger.info(
            "[OrdersSync] debug db_stats brand=%s total=%s newest=%s oldest=%s statuses=%s",
            brand,
            total_orders,
            newest_order_date,
            oldest_order_date,
            orders_by_status,
        )

    except Exception as stats_exc:
        logger.error("[OrdersSync] debug db_stats_failed brand=%s error=%s", brand, stats_exc)

    return make_json_safe({
        "ok": True,
        "brand": brand,
        "backend_time": backend_time,
        "last_leaflink_pull": last_leaflink_pull,
        "last_twin_ingest": None,
        "total_orders": total_orders,
        "newest_order_date": newest_order_date,
        "oldest_order_date": oldest_order_date,
        "orders_by_status": orders_by_status,
        "rejected_count": rejected_count,
        "last_error": last_error,
        "leaflink_credentials_valid": leaflink_credentials_valid,
        "sync_blocked": False,
    })


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
            cred = None
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

                    # Blockers (mapping issues) only affect routing — NEVER skip the LeafLink
                    # fetch. Always pull fresh orders from LeafLink when credentials exist.
                    logger.info(
                        "[OrdersSync] leaflink_pull_start brand=%s force_full=%s credential_source=db",
                        brand_filter,
                        force_full,
                    )
                    logger.info(
                        "[LeafLink] sync_start brand=%s credential_source=db force_full=%s",
                        brand_filter,
                        force_full,
                    )

                    # Watchdog: emit sync_started
                    _watchdog_url = os.getenv("OPSYN_WATCHDOG_WEBHOOK_URL")
                    await emit_watchdog_event(
                        event_type="sync_started",
                        brand_id=brand_filter,
                        sync_metadata={
                            "total_fetched_from_leaflink": 0,
                            "total_in_database": 0,
                            "percent_complete": 0.0,
                            "latest_order_date": None,
                            "oldest_order_date": None,
                            "sync_error": None,
                        },
                        webhook_url=_watchdog_url,
                    )

                    client = LeafLinkClient(api_key=api_key, company_id=company_id, brand_id=brand_filter)

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
                        "[OrdersSync] leaflink_pull_success brand=%s count=%s pages=%s force_full=%s",
                        brand_filter,
                        len(orders_from_leaflink),
                        pages_fetched,
                        force_full,
                    )
                    logger.info(
                        "[LeafLink] fetched=%s pages=%s brand=%s",
                        len(orders_from_leaflink),
                        pages_fetched,
                        brand_filter,
                    )

                    # Watchdog: emit sync_progress after fetch
                    await emit_watchdog_event(
                        event_type="sync_progress",
                        brand_id=brand_filter,
                        sync_metadata={
                            "total_fetched_from_leaflink": len(orders_from_leaflink),
                            "total_in_database": None,
                            "percent_complete": None,
                            "latest_order_date": None,
                            "oldest_order_date": None,
                            "sync_error": None,
                        },
                        webhook_url=_watchdog_url,
                    )

                    logger.info(
                        "[OrdersSync] db_upsert_start brand=%s orders=%s",
                        brand_filter,
                        len(orders_from_leaflink),
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

                    logger.info(
                        "[OrdersSync] db_upsert_success brand=%s inserted=%s updated=%s",
                        brand_filter,
                        sync_result.get("created", 0),
                        sync_result.get("updated", 0),
                    )
                    logger.info(
                        "[LeafLink] upserted=%s created=%s updated=%s brand=%s",
                        sync_result.get("orders_fetched", 0),
                        sync_result.get("created", 0),
                        sync_result.get("updated", 0),
                        brand_filter,
                    )

                    # Watchdog: emit sync_completed or sync_failed based on result
                    if sync_result.get("ok"):
                        await emit_watchdog_event(
                            event_type="sync_completed",
                            brand_id=brand_filter,
                            sync_metadata={
                                "total_fetched_from_leaflink": sync_result.get("orders_fetched", 0),
                                "total_in_database": sync_result.get("created", 0) + sync_result.get("updated", 0),
                                "percent_complete": 100.0,
                                "latest_order_date": sync_metadata.get("latest_order_date"),
                                "oldest_order_date": None,
                                "sync_error": None,
                            },
                            webhook_url=_watchdog_url,
                        )
                        # Mark credential as successfully used
                        from services.integration_credentials import mark_credential_success
                        await mark_credential_success(cred)
                    else:
                        logger.error(
                            "[OrdersSync] sync_failed brand=%s error=%s",
                            brand_filter,
                            sync_result.get("error"),
                        )
                        await emit_watchdog_event(
                            event_type="sync_failed",
                            brand_id=brand_filter,
                            sync_metadata={
                                "total_fetched_from_leaflink": sync_result.get("orders_fetched", 0),
                                "total_in_database": sync_result.get("created", 0) + sync_result.get("updated", 0),
                                "percent_complete": None,
                                "latest_order_date": sync_metadata.get("latest_order_date"),
                                "oldest_order_date": None,
                                "sync_error": sync_result.get("error"),
                            },
                            webhook_url=_watchdog_url,
                        )
                        # Mark credential as failed
                        from services.integration_credentials import mark_credential_invalid
                        await mark_credential_invalid(cred, sync_result.get("error") or "sync_failed")
                else:
                    logger.warning(
                        "[OrdersSync] leaflink_pull_start brand=%s — no active credential found, serving from DB cache",
                        brand_filter,
                    )
                    logger.warning(
                        "[LeafLink] credential_missing brand=%s — serving from DB only",
                        brand_filter,
                    )
                    sync_metadata["credential_missing"] = True
                    sync_metadata["used_force_full"] = force_full

            except Exception as sync_exc:
                logger.error(
                    "[OrdersSync] sync_failed brand=%s error=%s — falling back to DB cache",
                    brand_filter,
                    sync_exc,
                    exc_info=True,
                )
                sync_metadata["sync_error"] = str(sync_exc)
                sync_metadata["used_force_full"] = force_full

                # Mark credential as failed if we had one
                if cred is not None:
                    from services.integration_credentials import mark_credential_invalid
                    await mark_credential_invalid(cred, str(sync_exc))

                # Watchdog: emit sync_failed on exception
                _watchdog_url_exc = os.getenv("OPSYN_WATCHDOG_WEBHOOK_URL")
                await emit_watchdog_event(
                    event_type="sync_failed",
                    brand_id=brand_filter,
                    sync_metadata={
                        "total_fetched_from_leaflink": None,
                        "total_in_database": None,
                        "percent_complete": None,
                        "latest_order_date": None,
                        "oldest_order_date": None,
                        "sync_error": str(sync_exc),
                    },
                    webhook_url=_watchdog_url_exc,
                )
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

        # Determine data source: "live" if we fetched from LeafLink, "db_cache" if served from DB,
        # "empty" if no orders exist at all.
        source = "live" if sync_metadata.get("total_fetched_from_leaflink") else "db_cache"
        if sync_metadata.get("sync_error"):
            source = "db_cache"
        if total_in_database == 0:
            source = "empty"

        # Compute newest/oldest from the returned orders for the response log
        _newest_resp: Optional[str] = None
        _oldest_resp: Optional[str] = None
        for _od in orders_data:
            _ts = _od.get("external_updated_at") or _od.get("external_created_at")
            if _ts:
                if _newest_resp is None or _ts > _newest_resp:
                    _newest_resp = _ts
                if _oldest_resp is None or _ts < _oldest_resp:
                    _oldest_resp = _ts

        logger.info(
            "[OrdersSync] response_orders brand=%s count=%s newest=%s oldest=%s source=%s",
            brand_filter,
            len(orders_data),
            _newest_resp,
            _oldest_resp,
            source,
        )
        logger.info(
            "[OrdersSync] response count=%s has_more=%s brand=%s total_in_db=%s data_source=%s",
            len(orders_data),
            has_more,
            brand_filter,
            total_in_database,
            source,
        )
        logger.info(
            "[Orders] query_count=%s source=%s brand=%s",
            len(orders_data),
            source,
            brand_filter,
        )

        return make_json_safe({
            "ok": True,
            "source": source,
            "data_source": source,
            "count": len(orders_data),
            "synced_at": server_time,
            "orders": orders_data,
            "sync_metadata": sync_metadata,
            "next_cursor": next_cursor if has_more else None,
            "has_more": has_more,
            "server_time": server_time,
            "sync_version": 1,
            "last_synced_at": server_time,
        })

    except Exception as e:
        logger.error("[OrdersSync] fatal_error detail=%s", e, exc_info=True)

        # Never return empty on error — attempt to serve last known DB data
        fallback_synced_at = datetime.now(timezone.utc).isoformat()
        try:
            fallback_query = select(Order)
            if brand_id or brand_slug or (hasattr(request, "query_params") and request.query_params.get("brandId")):
                _brand = (
                    brand_id
                    or brand_slug
                    or request.query_params.get("brandId")
                )
                if _brand:
                    fallback_query = fallback_query.where(Order.brand_id == _brand)
            fallback_query = fallback_query.order_by(Order.updated_at.desc()).limit(limit)
            fallback_result = await db.execute(fallback_query)
            fallback_orders = fallback_result.scalars().all()

            fallback_data = []
            for order in fallback_orders:
                fallback_data.append({
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
                })

            logger.warning(
                "[OrdersSync] serving_fallback_db_data count=%s error=%s",
                len(fallback_data),
                e,
            )

            return make_json_safe({
                "ok": False,
                "status": "error",
                "source": "backend",
                "data_source": "backend",
                "message": str(e),
                "count": len(fallback_data),
                "synced_at": fallback_synced_at,
                "orders": fallback_data,
                "has_more": False,
                "next_cursor": None,
                "sync_metadata": {"sync_error": str(e)},
            })

        except Exception as fallback_exc:
            logger.error(
                "[OrdersSync] fallback_query_failed error=%s original_error=%s",
                fallback_exc,
                e,
            )
            return {
                "ok": False,
                "status": "error",
                "source": "error",
                "data_source": "error",
                "message": str(e),
                "count": 0,
                "synced_at": fallback_synced_at,
                "orders": [],
                "has_more": False,
                "next_cursor": None,
            }
