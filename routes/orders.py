import asyncio
import base64
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
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

# Thread pool for running synchronous LeafLink HTTP calls without blocking the event loop
_leaflink_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="leaflink-sync")

# Timeout constants
LEAFLINK_TIMEOUT_SECONDS = 5   # Max seconds to wait for LeafLink API
DB_TIMEOUT_SECONDS = 3         # Max seconds to wait for DB upsert in Phase A


async def resolve_brand_id(
    brand_filter: Optional[str],
    db: AsyncSession,
) -> tuple[Optional[str], Optional[str]]:
    """
    Resolve a brand_filter slug/ID to (brand_id, company_id) by looking up
    the active LeafLink credential in the database.

    Logs the resolution result for traceability.
    Raises HTTPException(400) if brand_filter is provided but not found.

    Returns (brand_id, company_id) — both may be None if brand_filter is None.
    """
    from fastapi import HTTPException

    if not brand_filter:
        return None, None

    try:
        cred_result = await db.execute(
            select(BrandAPICredential).where(
                BrandAPICredential.brand_id == brand_filter,
                BrandAPICredential.integration_name == "leaflink",
                BrandAPICredential.is_active == True,
            )
        )
        cred = cred_result.scalar_one_or_none()
    except Exception as exc:
        logger.error(
            "[OrdersAPI] brand_resolution db_error brand_filter=%s error=%s",
            brand_filter,
            exc,
        )
        raise HTTPException(status_code=500, detail="Database error during brand resolution")

    if cred is None:
        logger.warning(
            "[OrdersAPI] brand_resolution brand_filter=%s resolved_brand_id=None company_id=None (no active credential)",
            brand_filter,
        )
        # Return the filter as-is so the endpoint can still serve DB data
        return brand_filter, None

    logger.info(
        "[OrdersAPI] brand_resolution brand_filter=%s resolved_brand_id=%s company_id=%s",
        brand_filter,
        cred.brand_id,
        cred.company_id,
    )
    return cred.brand_id, cred.company_id


@router.get("")
def get_orders():
    return {
        "items": [],
        "count": 0,
        "message": "Orders endpoint is live",
    }


@router.get("/health")
async def orders_health(
    brand: Optional[str] = Query(None, description="Brand slug to check (e.g., 'noble-nectar')"),
    db: AsyncSession = Depends(get_db),
):
    """
    Lightweight health check for the orders subsystem.

    Returns quickly — no LeafLink calls are made. Useful for monitoring
    and frontend pre-flight checks.

    Response:
      {
        "ok": true,
        "brand": "noble-nectar",
        "db_count": <int>,
        "newest_order_date": "<ISO string | null>",
        "leaflink_configured": true/false,
        "timestamp": "<ISO datetime>"
      }
    """
    timestamp = datetime.now(timezone.utc).isoformat()

    try:
        # Count orders for this brand (or all brands if no brand specified)
        count_query = select(func.count(Order.id))
        if brand:
            count_query = count_query.where(Order.brand_id == brand)
        count_result = await db.execute(count_query)
        db_count: int = count_result.scalar_one() or 0

        # Find the newest order date
        newest_query = select(func.max(Order.external_updated_at))
        if brand:
            newest_query = newest_query.where(Order.brand_id == brand)
        newest_result = await db.execute(newest_query)
        newest_raw = newest_result.scalar_one_or_none()
        newest_order_date = newest_raw.isoformat() if newest_raw else None

        # Check if LeafLink is configured for this brand
        leaflink_configured = False
        if brand:
            cred_query = select(BrandAPICredential).where(
                BrandAPICredential.brand_id == brand,
                BrandAPICredential.integration_name == "leaflink",
                BrandAPICredential.is_active == True,
            )
            cred_result = await db.execute(cred_query)
            cred = cred_result.scalar_one_or_none()
            leaflink_configured = cred is not None and bool(cred.api_key)
        else:
            # Check if any active LeafLink credential exists
            any_cred_result = await db.execute(
                select(BrandAPICredential).where(
                    BrandAPICredential.integration_name == "leaflink",
                    BrandAPICredential.is_active == True,
                ).limit(1)
            )
            leaflink_configured = any_cred_result.scalar_one_or_none() is not None

        logger.info(
            "[OrdersAPI] health_check brand=%s db_count=%s",
            brand,
            db_count,
        )

        return make_json_safe({
            "ok": True,
            "brand": brand,
            "db_count": db_count,
            "newest_order_date": newest_order_date,
            "leaflink_configured": leaflink_configured,
            "timestamp": timestamp,
        })

    except Exception as exc:
        logger.error("[OrdersAPI] health_check_error brand=%s error=%s", brand, exc, exc_info=True)
        return make_json_safe({
            "ok": False,
            "brand": brand,
            "db_count": 0,
            "newest_order_date": None,
            "leaflink_configured": False,
            "timestamp": timestamp,
            "error": str(exc),
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

    # Resolve brand filter — accept brand_id, brand_slug, or camelCase brandId
    # from query params. brand_slug is an alias; brandId is tolerated for iOS clients.
    brand_filter: Optional[str] = (
        brand_id
        or brand_slug
        or request.query_params.get("brandId")
    )

    logger.info(
        "[OrdersAPI] request_start brand=%s",
        brand_filter,
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

    # Resolve brand_id → company_id mapping and log it for traceability
    _resolved_brand_id, _resolved_company_id = await resolve_brand_id(brand_filter, db)

    # ------------------------------------------------------------------ #
    # FORCE_DB_ONLY: emergency debug mode — skip LeafLink entirely        #
    # ------------------------------------------------------------------ #
    force_db_only = os.getenv("FORCE_DB_ONLY", "false").lower() == "true"
    if force_db_only:
        logger.info(
            "[OrdersAPI] force_db_only=true — skipping LeafLink, serving from DB brand=%s",
            brand_filter,
        )

    sync_metadata: dict = {}
    sync_error: Optional[str] = None
    live_refresh_failed: bool = False

    if brand_filter and not force_db_only:
        logger.info("[OrdersSync] phase=A start brand=%s", brand_filter)
        cred = None
        try:
            from services.leaflink_client import LeafLinkClient
            from services.leaflink_sync import sync_leaflink_orders

            logger.info("[OrdersAPI] db_fetch_start")
            logger.info("[OrdersSync] phase=A leaflink_pull_start")

            # Look up credentials for this brand using the Phase A session.
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

                # ---------------------------------------------------------- #
                # Wrap the synchronous LeafLink HTTP call in a thread so we   #
                # can apply asyncio.wait_for() timeout without blocking the   #
                # event loop. fetch_recent_orders() uses requests (sync I/O). #
                # ---------------------------------------------------------- #
                logger.info("[OrdersAPI] leaflink_refresh_start")
                loop = asyncio.get_event_loop()

                def _fetch_orders_sync():
                    return client.fetch_recent_orders(
                        max_pages=max_pages_arg,
                        normalize=True,
                        brand=brand_filter,
                    )

                try:
                    fetch_result = await asyncio.wait_for(
                        loop.run_in_executor(_leaflink_executor, _fetch_orders_sync),
                        timeout=LEAFLINK_TIMEOUT_SECONDS,
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        "[OrdersAPI] leaflink_refresh_timeout brand=%s timeout=%ss",
                        brand_filter,
                        LEAFLINK_TIMEOUT_SECONDS,
                    )
                    raise asyncio.TimeoutError(
                        f"LeafLink API did not respond within {LEAFLINK_TIMEOUT_SECONDS}s"
                    )

                orders_from_leaflink = fetch_result["orders"]
                pages_fetched = fetch_result["pages_fetched"]

                logger.info("[OrdersSync] phase=A leaflink_pull_success count=%s", len(orders_from_leaflink))
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

                logger.info("[OrdersSync] phase=A db_upsert_start")

                async def _do_db_upsert():
                    async with db.begin():
                        return await sync_leaflink_orders(
                            db,
                            brand_filter,
                            orders_from_leaflink,
                            pages_fetched=pages_fetched,
                        )

                try:
                    sync_result = await asyncio.wait_for(
                        _do_db_upsert(),
                        timeout=DB_TIMEOUT_SECONDS,
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        "[OrdersSync] phase=A db_upsert_timeout brand=%s timeout=%ss",
                        brand_filter,
                        DB_TIMEOUT_SECONDS,
                    )
                    raise asyncio.TimeoutError(
                        f"DB upsert did not complete within {DB_TIMEOUT_SECONDS}s"
                    )

                logger.info(
                    "[OrdersSync] phase=A db_upsert_success inserted=%s updated=%s",
                    sync_result.get("created", 0),
                    sync_result.get("updated", 0),
                )

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

                logger.info("[OrdersSync] phase=A success brand=%s", brand_filter)

            else:
                logger.warning(
                    "[LeafLink] credential_missing brand=%s — serving from DB only",
                    brand_filter,
                )
                sync_metadata["credential_missing"] = True
                sync_metadata["used_force_full"] = force_full
                logger.info("[OrdersSync] phase=A success brand=%s (no credential)", brand_filter)

        except asyncio.TimeoutError as timeout_exc:
            # LeafLink or DB timed out — log it, fall through to Phase B with DB fallback
            timeout_msg = str(timeout_exc)
            logger.warning(
                "[OrdersAPI] leaflink_refresh_timeout brand=%s error=%s — falling back to DB",
                brand_filter,
                timeout_msg,
            )
            try:
                await db.rollback()
                logger.info("[OrdersSync] phase=A rollback_executed (timeout)")
            except Exception as rb_exc:
                logger.warning("[OrdersSync] phase=A rollback_failed error=%s", rb_exc)

            live_refresh_failed = True
            sync_error = timeout_msg
            sync_metadata["sync_error"] = sync_error
            sync_metadata["used_force_full"] = force_full

            # Watchdog: emit sync_failed on timeout
            _watchdog_url_to = os.getenv("OPSYN_WATCHDOG_WEBHOOK_URL")
            await emit_watchdog_event(
                event_type="sync_failed",
                brand_id=brand_filter,
                sync_metadata={
                    "total_fetched_from_leaflink": None,
                    "total_in_database": None,
                    "percent_complete": None,
                    "latest_order_date": None,
                    "oldest_order_date": None,
                    "sync_error": sync_error,
                },
                webhook_url=_watchdog_url_to,
            )

            logger.info("[OrdersSync] phase=A session_discarded (timeout)")

        except Exception as phase_a_exc:
            logger.error("[OrdersSync] phase=A error=%s", phase_a_exc, exc_info=True)
            try:
                await db.rollback()
                logger.info("[OrdersSync] phase=A rollback_executed")
            except Exception as rb_exc:
                logger.warning("[OrdersSync] phase=A rollback_failed error=%s", rb_exc)

            live_refresh_failed = True
            sync_error = str(phase_a_exc)
            sync_metadata["sync_error"] = sync_error
            sync_metadata["used_force_full"] = force_full

            # Mark credential as failed if we had one — uses its own fresh session internally
            if cred is not None:
                from services.integration_credentials import mark_credential_invalid
                await mark_credential_invalid(cred, sync_error)

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
                    "sync_error": sync_error,
                },
                webhook_url=_watchdog_url_exc,
            )

            # CRITICAL: Phase A session is now in a failed/rolled-back state.
            # DO NOT query db again. Phase B will open a completely fresh session.
            logger.info("[OrdersSync] phase=A session_discarded")

    else:
        sync_metadata["used_force_full"] = force_full
        if force_db_only:
            sync_metadata["force_db_only"] = True

    # ------------------------------------------------------------------ #
    # Phase B: Read orders — ALWAYS uses a brand-new session              #
    # NEVER reuses the Phase A session, even if Phase A succeeded.        #
    # ------------------------------------------------------------------ #
    logger.info("[OrdersSync] phase=B new_session_created")

    # Parse updated_after (ignored when force_full=true)
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

    # Decode cursor
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

    async with AsyncSessionLocal() as read_session:
        try:
            logger.info("[OrdersSync] phase=B read_start brand=%s", brand_filter)
            logger.info("[OrdersAPI] db_fetch_start")

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

            result = await read_session.execute(query_with_limit)
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
            count_result = await read_session.execute(count_query)
            total_in_database = count_result.scalar_one()

            logger.info("[OrdersSync] phase=B read_success count=%s", len(orders))

            # ------------------------------------------------------------------ #
            # Build response                                                       #
            orders_data = []
            next_cursor = None

            # Track freshness dates across returned orders
            newest_order_date: Optional[datetime] = None
            oldest_order_date: Optional[datetime] = None


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

                # Track newest/oldest order dates for freshness metadata
                order_date = order.external_updated_at or order.external_created_at or order.updated_at
                if order_date is not None:
                    if newest_order_date is None or order_date > newest_order_date:
                        newest_order_date = order_date
                    if oldest_order_date is None or order_date < oldest_order_date:
                        oldest_order_date = order_date

                # Update cursor to last order's ID
                if order == orders[-1]:
                    next_cursor = base64.b64encode(str(order.id).encode()).decode()

            server_time = datetime.now(timezone.utc).isoformat()
            newest_order_date_iso = newest_order_date.isoformat() if newest_order_date else None
            oldest_order_date_iso = oldest_order_date.isoformat() if oldest_order_date else None

            logger.info(
                "[OrdersAPI] db_fetch_done count=%s newest_order_date=%s",
                len(orders_data),
                newest_order_date_iso,
            )

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

            # Determine response source:
            # - "live"              → LeafLink succeeded and data was refreshed
            # - "database_fallback" → LeafLink timed out or failed; serving cached DB data
            # - "database"          → No LeafLink call was attempted (no brand or force_db_only)
            # - "empty"             → No orders in DB at all
            fetched_from_leaflink = sync_metadata.get("total_fetched_from_leaflink")
            returning_mock = False
            returning_cache = False

            if live_refresh_failed:
                source = "database_fallback"
                returning_cache = True
            elif total_in_database == 0:
                source = "empty"
            elif fetched_from_leaflink:
                source = "live"
            else:
                source = "database"
                returning_cache = True

            sync_status = "error" if sync_error else "success"

            logger.info("[OrdersAPI] source=%s", source)
            logger.info("[OrdersAPI] db_count=%s", len(orders_data))
            logger.info("[OrdersAPI] newest_order_date=%s", newest_order_date_iso)
            logger.info("[OrdersAPI] refreshed_at=%s", server_time)
            logger.info("[OrdersAPI] returning_mock=%s", str(returning_mock).lower())
            logger.info("[OrdersAPI] returning_cache=%s", str(returning_cache).lower())

            logger.info(
                "[Orders] query_count=%s source=%s brand=%s",
                len(orders_data),
                source,
                brand_filter,
            )
            logger.info(
                "[OrdersSync] response_orders count=%s source=%s",
                len(orders_data),
                source,
            )
            logger.info(
                "[OrdersAPI] response_returned source=%s count=%s",
                source,
                len(orders_data),
            )

            # Enrich sync_metadata with brand mapping and freshness info
            sync_metadata["returning_mock"] = returning_mock
            sync_metadata["returning_cache"] = returning_cache
            sync_metadata["brand_id"] = _resolved_brand_id or brand_filter
            sync_metadata["company_id"] = _resolved_company_id

            return make_json_safe({
                "ok": True,
                "sync_status": sync_status,
                "source": source,
                "data_source": source,
                "count": len(orders_data),
                "total_in_database": total_in_database,
                "newest_order_date": newest_order_date_iso,
                "oldest_order_date": oldest_order_date_iso,
                "refreshed_at": server_time,
                "synced_at": server_time,
                "orders": orders_data,
                "sync_metadata": sync_metadata,
                "next_cursor": next_cursor if has_more else None,
                "has_more": has_more,
                "server_time": server_time,
                "sync_version": 1,
                "last_synced_at": server_time,
                "last_error": sync_error,
                "live_refresh_failed": live_refresh_failed,
                "error": sync_error if live_refresh_failed else None,
            })

        except Exception as phase_b_exc:
            logger.error("[OrdersSync] phase=B read_error=%s", phase_b_exc, exc_info=True)
            server_time = datetime.now(timezone.utc).isoformat()
            return make_json_safe({
                "ok": False,
                "sync_status": "error",
                "data_source": "error",
                "source": "error",
                "orders": [],
                "count": 0,
                "total_in_database": 0,
                "newest_order_date": None,
                "oldest_order_date": None,
                "refreshed_at": server_time,
                "synced_at": server_time,
                "server_time": server_time,
                "has_more": False,
                "next_cursor": None,
                "sync_metadata": sync_metadata,
                "last_error": str(phase_b_exc),
            })
