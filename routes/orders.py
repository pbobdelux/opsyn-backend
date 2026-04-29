import base64
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from database import AsyncSessionLocal, get_db
from models import BrandAPICredential, Order
from services.watchdog_client import emit_watchdog_event
from utils.json_utils import make_json_safe

logger = logging.getLogger("orders")

router = APIRouter(prefix="/orders", tags=["orders"])
debug_router = APIRouter(prefix="/debug", tags=["debug"])


@router.get("")
def get_orders():
    return {
        "items": [],
        "count": 0,
        "message": "Orders endpoint is live",
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

                    logger.info(
                        "[LeafLink] sync_start brand=%s credential_source=db force_full=%s",
                        brand_filter,
                        force_full,
                    )
                    logger.info(
                        "[OrdersSync] triggering_leaflink_sync brand=%s force_full=%s",
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
                        "[OrdersSync] leaflink_fetch_complete brand=%s orders=%s pages=%s force_full=%s",
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
                        "[OrdersSync] db_query_start operation=sync_leaflink_orders brand=%s",
                        brand_filter,
                    )
                    try:
                        sync_result = await sync_leaflink_orders(
                            db,
                            brand_filter,
                            orders_from_leaflink,
                            pages_fetched=pages_fetched,
                        )
                        await db.commit()
                        logger.info(
                            "[OrdersSync] db_query_success operation=sync_leaflink_orders brand=%s rows=%s",
                            brand_filter,
                            sync_result.get("orders_fetched", 0),
                        )
                    except Exception as db_sync_exc:
                        logger.error(
                            "[OrdersSync] db_query_error operation=sync_leaflink_orders brand=%s error=%s",
                            brand_filter,
                            db_sync_exc,
                            exc_info=True,
                        )
                        await db.rollback()
                        logger.info("[OrdersSync] rollback_executed=true brand=%s", brand_filter)
                        raise

                    sync_metadata["total_fetched_from_leaflink"] = sync_result.get("orders_fetched", 0)
                    sync_metadata["pages_fetched"] = sync_result.get("pages_fetched", pages_fetched)
                    sync_metadata["sync_duration_seconds"] = sync_result.get("sync_duration_seconds", 0)
                    sync_metadata["used_force_full"] = force_full

                    newest = sync_result.get("newest_order_date")
                    sync_metadata["latest_order_date"] = newest.isoformat() if newest else None

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
                        "[LeafLink] credential_missing brand=%s — serving from DB only",
                        brand_filter,
                    )
                    sync_metadata["credential_missing"] = True
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

                # Rollback any aborted transaction so the session is clean for
                # subsequent reads.  This is the critical fix for
                # InFailedSQLTransactionError.
                try:
                    await db.rollback()
                    logger.info("[OrdersSync] rollback_executed=true brand=%s", brand_filter)
                except Exception as rb_exc:
                    logger.warning("[OrdersSync] rollback_failed brand=%s error=%s", brand_filter, rb_exc)

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

        logger.info("[OrdersSync] db_query_start operation=fetch_orders brand=%s limit=%s", brand_filter, limit)
        result = await db.execute(query_with_limit)
        orders = result.scalars().all()
        logger.info(
            "[OrdersSync] db_query_success operation=fetch_orders brand=%s rows=%s",
            brand_filter,
            len(orders),
        )

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
        logger.info("[OrdersSync] db_query_start operation=count_orders brand=%s", brand_filter)
        count_result = await db.execute(count_query)
        total_in_database = count_result.scalar_one()
        logger.info(
            "[OrdersSync] db_query_success operation=count_orders brand=%s rows=%s",
            brand_filter,
            total_in_database,
        )

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

        source = "live" if sync_metadata.get("total_fetched_from_leaflink") else "db_cache"
        if total_in_database == 0:
            source = "empty"

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

        # Never return empty on error — attempt to serve last known DB data using
        # a FRESH session so we are not affected by any aborted transaction on
        # the original session.
        fallback_synced_at = datetime.now(timezone.utc).isoformat()
        _brand_fallback = (
            brand_id
            or brand_slug
            or (request.query_params.get("brandId") if hasattr(request, "query_params") else None)
        )

        fallback_data = []
        fallback_count = 0
        try:
            if AsyncSessionLocal is None:
                raise RuntimeError("AsyncSessionLocal not configured")

            logger.info(
                "[OrdersSync] db_query_start operation=fallback_fetch brand=%s",
                _brand_fallback,
            )
            async with AsyncSessionLocal() as fresh_session:
                fallback_query = select(Order)
                if _brand_fallback:
                    fallback_query = fallback_query.where(Order.brand_id == _brand_fallback)
                fallback_query = fallback_query.order_by(Order.updated_at.desc()).limit(limit)
                fallback_result = await fresh_session.execute(fallback_query)
                fallback_orders = fallback_result.scalars().all()

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

                fallback_count = len(fallback_data)
                logger.info(
                    "[OrdersSync] db_query_success operation=fallback_fetch brand=%s rows=%s",
                    _brand_fallback,
                    fallback_count,
                )

            logger.warning(
                "[OrdersSync] serving_fallback_db_data count=%s error=%s",
                fallback_count,
                e,
            )

            return make_json_safe({
                "ok": False,
                "sync_status": "error",
                "last_error": str(e),
                "source": "db_cache",
                "data_source": "db_cache",
                "message": str(e),
                "count": fallback_count,
                "orders_count": fallback_count,
                "synced_at": fallback_synced_at,
                "orders": fallback_data,
                "has_more": False,
                "next_cursor": None,
                "sync_metadata": {"sync_error": str(e)},
            })

        except Exception as fallback_exc:
            logger.error(
                "[OrdersSync] db_query_error operation=fallback_fetch brand=%s error=%s original_error=%s",
                _brand_fallback,
                fallback_exc,
                e,
            )
            return {
                "ok": False,
                "sync_status": "error",
                "last_error": str(e),
                "source": "error",
                "data_source": "error",
                "message": str(e),
                "orders_count": 0,
                "count": 0,
                "synced_at": fallback_synced_at,
                "orders": [],
                "has_more": False,
                "next_cursor": None,
            }


# ---------------------------------------------------------------------------
# Debug endpoint — lightweight diagnostic for the orders sync pipeline
# Registered on both:
#   GET /orders/debug/orders-sync?brand=<brand_slug>  (via router)
#   GET /debug/orders-sync?brand=<brand_slug>          (via debug_router, registered in main.py)
# ---------------------------------------------------------------------------

async def _debug_orders_sync_handler(
    brand: Optional[str],
    db: AsyncSession,
) -> dict:
    """Shared implementation for the orders-sync debug endpoint."""
    result: dict = {
        "ok": True,
        "brand": brand,
        "db_connected": False,
        "orders_total": None,
        "orders_for_brand": None,
        "credential_found": False,
        "credential_active": False,
        "last_sync_at": None,
        "last_sync_status": None,
        "last_error": None,
        "errors": [],
    }

    # ── DB connectivity + order counts ──────────────────────────────────────
    try:
        logger.info("[OrdersSync] db_query_start operation=debug_count_all brand=%s", brand)
        total_result = await db.execute(select(func.count(Order.id)))
        result["orders_total"] = total_result.scalar_one()
        result["db_connected"] = True
        logger.info(
            "[OrdersSync] db_query_success operation=debug_count_all rows=%s",
            result["orders_total"],
        )
    except Exception as exc:
        err = f"db_count_all failed: {exc}"
        logger.error("[OrdersSync] db_query_error operation=debug_count_all error=%s", exc)
        result["errors"].append(err)

    if brand:
        try:
            logger.info("[OrdersSync] db_query_start operation=debug_count_brand brand=%s", brand)
            brand_count_result = await db.execute(
                select(func.count(Order.id)).where(Order.brand_id == brand)
            )
            result["orders_for_brand"] = brand_count_result.scalar_one()
            logger.info(
                "[OrdersSync] db_query_success operation=debug_count_brand brand=%s rows=%s",
                brand,
                result["orders_for_brand"],
            )
        except Exception as exc:
            err = f"db_count_brand failed: {exc}"
            logger.error("[OrdersSync] db_query_error operation=debug_count_brand brand=%s error=%s", brand, exc)
            result["errors"].append(err)

    # ── LeafLink credential status ───────────────────────────────────────────
    if brand:
        try:
            logger.info("[OrdersSync] db_query_start operation=debug_cred_lookup brand=%s", brand)
            cred_result = await db.execute(
                select(BrandAPICredential).where(
                    BrandAPICredential.brand_id == brand,
                    BrandAPICredential.integration_name == "leaflink",
                )
            )
            cred = cred_result.scalar_one_or_none()
            if cred:
                result["credential_found"] = True
                result["credential_active"] = bool(cred.is_active)
                result["last_sync_at"] = cred.last_sync_at.isoformat() if cred.last_sync_at else None
                result["last_sync_status"] = cred.sync_status
                result["last_error"] = cred.last_error
                logger.info(
                    "[OrdersSync] db_query_success operation=debug_cred_lookup brand=%s active=%s",
                    brand,
                    cred.is_active,
                )
            else:
                logger.info("[OrdersSync] db_query_success operation=debug_cred_lookup brand=%s found=false", brand)
        except Exception as exc:
            err = f"cred_lookup failed: {exc}"
            logger.error("[OrdersSync] db_query_error operation=debug_cred_lookup brand=%s error=%s", brand, exc)
            result["errors"].append(err)

    if result["errors"]:
        result["ok"] = False

    return result


@router.get("/debug/orders-sync")
async def debug_orders_sync(
    brand: Optional[str] = Query(None, description="Brand slug to inspect (e.g., 'noble-nectar')"),
    db: AsyncSession = Depends(get_db),
):
    """
    Diagnostic endpoint for the orders sync pipeline (accessible at /orders/debug/orders-sync).

    Returns DB connectivity status, order counts, and LeafLink credential metadata.
    """
    return await _debug_orders_sync_handler(brand=brand, db=db)


@debug_router.get("/orders-sync")
async def debug_orders_sync_root(
    brand: Optional[str] = Query(None, description="Brand slug to inspect (e.g., 'noble-nectar')"),
    db: AsyncSession = Depends(get_db),
):
    """
    Diagnostic endpoint for the orders sync pipeline (accessible at /debug/orders-sync).

    Returns DB connectivity status, order counts, and LeafLink credential metadata.
    """
    return await _debug_orders_sync_handler(brand=brand, db=db)
