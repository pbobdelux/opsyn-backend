import asyncio
import base64
import logging
import os
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from database import AsyncSessionLocal, get_db
from models import BrandAPICredential, Order
from services.background_sync_manager import sync_manager
from services.watchdog_client import emit_watchdog_event
from utils.json_utils import make_json_safe

logger = logging.getLogger("orders")

router = APIRouter(prefix="/orders", tags=["orders"])

# Thread pool for running synchronous LeafLink HTTP calls without blocking the event loop
_leaflink_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="leaflink-sync")

# Timeout constants
LEAFLINK_TIMEOUT_SECONDS = 25  # Max seconds to wait for Phase 1 LeafLink fetch (3 pages)
DB_TIMEOUT_SECONDS = 10        # Max seconds to wait for DB upsert in Phase 1
BACKGROUND_SYNC_TIMEOUT = 1800 # 30 minutes for background full-sync

# Paginated sync configuration
LEAFLINK_PAGES_PER_REQUEST = 1  # Pages fetched synchronously per /sync call
LEAFLINK_PAGE_SIZE = 100        # Orders per LeafLink page


@router.get("")
async def get_orders(
    brand: str = Query(..., description="Brand slug/ID"),
    limit: int = Query(100, ge=1, le=1000, description="Orders per page (default 100, max 1000)"),
    offset: int = Query(0, ge=0, description="Offset for pagination (default 0)"),
    cursor: Optional[str] = Query(None, description="Opaque cursor for cursor-based pagination"),
    sort_by: str = Query("updated_at", description="Sort field: updated_at, created_at, external_updated_at"),
    sort_order: str = Query("desc", description="asc or desc"),
    db: AsyncSession = Depends(get_db),
):
    """
    Fetch paginated orders for a brand.

    Supports two pagination modes:
    1. Limit/Offset: Use limit and offset parameters
    2. Cursor-based: Use cursor parameter (more efficient for large datasets)

    Response includes next_cursor / prev_cursor for navigating pages.
    """
    from leaflink.orders import serialize_order

    # ------------------------------------------------------------------ #
    # Decode cursor → (cursor_id, cursor_updated_at)                      #
    # Cursor format: base64("id=<N>&updated_at=<ISO>")                    #
    # ------------------------------------------------------------------ #
    cursor_id: Optional[int] = None
    cursor_updated_at: Optional[str] = None

    if cursor:
        try:
            decoded = base64.b64decode(cursor).decode()
            parts = dict(p.split("=", 1) for p in decoded.split("&") if "=" in p)
            cursor_id = int(parts["id"]) if parts.get("id") else None
            cursor_updated_at = parts.get("updated_at") or None
        except Exception as cursor_exc:
            logger.error("[OrdersAPI] cursor_decode_error error=%s", cursor_exc)
            return {"ok": False, "error": "Invalid cursor format"}

    # ------------------------------------------------------------------ #
    # Resolve sort column                                                  #
    # ------------------------------------------------------------------ #
    if sort_by == "created_at":
        sort_col = Order.created_at
    elif sort_by == "external_updated_at":
        sort_col = Order.external_updated_at
    else:
        sort_by = "updated_at"
        sort_col = Order.updated_at

    # ------------------------------------------------------------------ #
    # Build base query                                                     #
    # ------------------------------------------------------------------ #
    from sqlalchemy.orm import selectinload as _selectinload

    query = (
        select(Order)
        .options(_selectinload(Order.lines))
        .where(Order.brand_id == brand)
    )

    if sort_order == "asc":
        query = query.order_by(sort_col.asc(), Order.id.asc())
    else:
        sort_order = "desc"
        query = query.order_by(sort_col.desc(), Order.id.desc())

    # ------------------------------------------------------------------ #
    # Apply cursor filter OR offset                                        #
    # ------------------------------------------------------------------ #
    if cursor_id and cursor_updated_at:
        if sort_order == "asc":
            query = query.where(
                (sort_col > cursor_updated_at)
                | ((sort_col == cursor_updated_at) & (Order.id > cursor_id))
            )
        else:
            query = query.where(
                (sort_col < cursor_updated_at)
                | ((sort_col == cursor_updated_at) & (Order.id < cursor_id))
            )
    else:
        query = query.offset(offset)

    # ------------------------------------------------------------------ #
    # Fetch limit + 1 to detect has_more                                  #
    # ------------------------------------------------------------------ #
    try:
        result = await db.execute(query.limit(limit + 1))
        orders = list(result.scalars().all())
    except Exception as exc:
        logger.error("[OrdersAPI] db_query_failed brand=%s error=%s", brand, exc, exc_info=True)
        return {"ok": False, "error": "Database query failed"}

    has_more = len(orders) > limit
    if has_more:
        orders = orders[:limit]

    # ------------------------------------------------------------------ #
    # Total count for the brand (separate query)                           #
    # ------------------------------------------------------------------ #
    try:
        total_count_result = await db.execute(
            select(func.count(Order.id)).where(Order.brand_id == brand)
        )
        total_in_database = total_count_result.scalar_one() or 0
    except Exception:
        total_in_database = len(orders)

    # ------------------------------------------------------------------ #
    # Build next / prev cursors                                            #
    # ------------------------------------------------------------------ #
    next_cursor: Optional[str] = None
    prev_cursor: Optional[str] = None

    if has_more and orders:
        last_order = orders[-1]
        sort_val = getattr(last_order, sort_by, None)
        sort_val_iso = sort_val.isoformat() if sort_val is not None else ""
        next_cursor = base64.b64encode(
            f"id={last_order.id}&updated_at={sort_val_iso}".encode()
        ).decode()

    if not cursor and offset > 0 and orders:
        prev_offset = max(0, offset - limit)
        prev_cursor = base64.b64encode(
            f"offset={prev_offset}".encode()
        ).decode()

    # ------------------------------------------------------------------ #
    # Serialize                                                            #
    # ------------------------------------------------------------------ #
    serialized_orders = [serialize_order(o) for o in orders]

    logger.info(
        "[OrdersAPI] get_orders brand=%s count=%s total=%s offset=%s limit=%s has_more=%s cursor=%s",
        brand,
        len(serialized_orders),
        total_in_database,
        offset,
        limit,
        has_more,
        bool(cursor),
    )

    return make_json_safe({
        "ok": True,
        "brand_id": brand,
        "orders": serialized_orders,
        "count": len(serialized_orders),
        "total_in_database": total_in_database,
        "offset": offset,
        "limit": limit,
        "has_more": has_more,
        "next_cursor": next_cursor,
        "prev_cursor": prev_cursor,
    })


@router.get("/health")
async def orders_health(
    brand: Optional[str] = Query(None, description="Brand slug to check (e.g., 'noble-nectar')"),
):
    """
    Diagnostic health check for the orders subsystem.

    Temporarily returns immediately without touching the database.
    Purpose: isolate whether hangs are caused by route registration /
    middleware or by DB access.
    """
    logger.info("[OrdersHealth] reached_before_db brand=%s", brand)
    return {
        "ok": True,
        "brand": brand,
        "message": "health route reached",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/credentials")
async def orders_credentials(
    brand: Optional[str] = Query(None, description="Brand slug to check (e.g., 'noble-nectar')"),
    db: AsyncSession = Depends(get_db),
):
    """
    Check whether LeafLink credentials are configured for a given brand.

    Returns a structured response indicating whether the credential exists,
    whether the api_key and company_id are populated, and whether the record
    is marked active. Useful for diagnosing why /orders/sync returns DB-only data.
    """
    timestamp = datetime.now(timezone.utc).isoformat()

    if not brand:
        return {
            "ok": False,
            "error": "brand query parameter is required",
            "timestamp": timestamp,
        }

    logger.info("[OrdersAPI] credentials_check brand=%s", brand)

    try:
        cred_result = await db.execute(
            select(BrandAPICredential).where(
                BrandAPICredential.brand_id == brand,
                BrandAPICredential.integration_name == "leaflink",
            )
        )
        cred = cred_result.scalar_one_or_none()
    except Exception as exc:
        logger.error("[OrdersAPI] credentials_check db_error brand=%s error=%s", brand, exc)
        return {
            "ok": False,
            "brand": brand,
            "error": "Database error during credential lookup",
            "timestamp": timestamp,
        }

    if cred is None:
        logger.warning(
            "[OrdersAPI] credentials_check brand=%s credential_exists=false",
            brand,
        )
        return {
            "ok": True,
            "brand": brand,
            "credential_exists": False,
            "has_api_key": False,
            "has_company_id": False,
            "is_active": False,
            "timestamp": timestamp,
        }

    has_api_key = bool(cred.api_key and cred.api_key.strip())
    has_company_id = bool(cred.company_id and cred.company_id.strip())

    logger.info(
        "[OrdersAPI] credentials_check brand=%s credential_exists=true has_api_key=%s has_company_id=%s is_active=%s",
        brand,
        str(has_api_key).lower(),
        str(has_company_id).lower(),
        str(cred.is_active).lower(),
    )

    return {
        "ok": True,
        "brand": brand,
        "credential_exists": True,
        "has_api_key": has_api_key,
        "has_company_id": has_company_id,
        "is_active": cred.is_active,
        "timestamp": timestamp,
    }


@router.get("/sync")
async def orders_sync(
    request: Request,
    brand_id: Optional[str] = Query(None, description="Brand slug or ID to filter orders (e.g., 'noble-nectar')"),
    brand_slug: Optional[str] = Query(None, description="Alias for brand_id — brand slug or ID"),
    updated_after: Optional[str] = Query(None, description="ISO timestamp - only return orders updated after this time (ignored when force_full=true)"),
    cursor: Optional[str] = Query(None, description="Pagination cursor from previous response (encodes LeafLink page number)"),
    limit: int = Query(500, ge=1, le=1000, description="Number of orders to return from DB (default 500, max 1000)"),
    force_full: bool = Query(False, description="Force a complete backfill from LeafLink, ignoring updated_after"),
    pages_per_request: int = Query(LEAFLINK_PAGES_PER_REQUEST, ge=1, le=20, description="LeafLink pages to fetch per request (default 3)"),
    db: AsyncSession = Depends(get_db),
):
    """
    Paginated incremental sync from LeafLink with background continuation.

    Returns sync STATUS and PROGRESS ONLY — no order data.
    Use the ``/orders`` endpoint (with pagination) to fetch actual order records.
    This endpoint is for monitoring sync progress, not retrieving orders.

    Phase 1 (synchronous, < 5 seconds):
    - Decodes cursor to determine which LeafLink page to start from
    - Fetches only ``pages_per_request`` pages (default 3 = ~300 orders)
    - Upserts those orders to DB immediately
    - Returns next_cursor so the client can call again for the next batch
    - Spawns a background task to fetch remaining pages without blocking

    Phase 2 (background, up to 30 minutes):
    - Continues fetching remaining LeafLink pages asynchronously
    - Does not block the HTTP response
    - Upserts each page to DB as it arrives

    Cursor format: base64(\"page=<N>&url=<next_url>\")
    - page: next LeafLink page number to fetch
    - url: LeafLink's own ``next`` URL for that page (avoids re-computing offset)

    Response fields (metadata only):
    - ok: boolean
    - sync_status: \"idle\" | \"syncing\" | \"complete\" | \"paused\" | \"error\"
    - background_sync_active: true if a background task is running
    - pages_synced: number of LeafLink pages fetched so far
    - total_pages: total pages reported by LeafLink
    - total_orders_in_db: COUNT(*) of orders stored for this brand
    - total_orders_available: total orders reported by LeafLink
    - percent_complete: (total_orders_in_db / total_orders_available) * 100
    - next_cursor: opaque cursor to pass on the next call
    - error: error string if something went wrong, else null

    Query params:
    - brand_id / brand_slug: Brand slug or ID (e.g., \"noble-nectar\")
    - updated_after: ISO timestamp — ignored when force_full=true
    - cursor: Pagination cursor from previous response
    - force_full: true/false — force complete backfill from LeafLink
    - pages_per_request: LeafLink pages per sync call (1-20, default 3)
    """


    # Resolve brand filter — accept brand_id, brand_slug, or camelCase brandId
    brand_filter: Optional[str] = (
        brand_id
        or brand_slug
        or request.query_params.get("brandId")
    )

    logger.info("[OrdersAPI] request_start brand=%s", brand_filter)
    logger.info(
        "[OrdersSync] request_start path=%s brand=%s org=%s updated_after=%s cursor=%s limit=%s force_full=%s pages_per_request=%s",
        request.url.path,
        brand_filter,
        request.headers.get("x-opsyn-org") or request.headers.get("x-org-id"),
        updated_after,
        cursor[:8] + "..." if cursor else None,
        limit,
        force_full,
        pages_per_request,
    )

    # ------------------------------------------------------------------ #
    # Decode cursor → (start_page, resume_url)                            #
    # Cursor format: base64("page=<N>&url=<next_url>")                    #
    # ------------------------------------------------------------------ #
    start_page = 1
    resume_url: Optional[str] = None

    if cursor:
        try:
            cursor_decoded = base64.b64decode(cursor.encode()).decode("utf-8")
            # Parse "page=N&url=..." format
            cursor_parts: dict = {}
            for part in cursor_decoded.split("&", 1):
                if "=" in part:
                    k, v = part.split("=", 1)
                    cursor_parts[k] = v
            start_page = int(cursor_parts.get("page", 1))
            resume_url = cursor_parts.get("url") or None
            logger.info(
                "[OrdersSync] cursor_decoded start_page=%s resume_url=%s",
                start_page,
                resume_url[:60] + "..." if resume_url and len(resume_url) > 60 else resume_url,
            )
        except Exception as cursor_exc:
            logger.error("[OrdersSync] cursor_decode_error error=%s", cursor_exc)
            return {
                "ok": False,
                "error": "Invalid cursor format",
                "data_source": "error",
            }

    # ------------------------------------------------------------------ #
    # Load the first active LeafLink credential from DB                   #
    # ------------------------------------------------------------------ #
    _resolved_brand_id: Optional[str] = None
    _resolved_company_id: Optional[str] = None
    _active_cred = None
    try:
        cred_result = await db.execute(
            select(BrandAPICredential).where(
                BrandAPICredential.integration_name == "leaflink",
                BrandAPICredential.is_active == True,
            ).order_by(BrandAPICredential.last_sync_at.desc().nullslast()).limit(1)
        )
        _active_cred = cred_result.scalar_one_or_none()
    except Exception as _cred_exc:
        logger.error("[OrdersSync] credential_load_error error=%s", _cred_exc)

    if _active_cred is not None:
        _resolved_brand_id = _active_cred.brand_id
        _resolved_company_id = _active_cred.company_id
        _api_key_present = bool(_active_cred.api_key and _active_cred.api_key.strip())
        logger.info("[OrdersSync] credential_source=database")
        logger.info("[OrdersSync] resolved_brand_id=%s", _resolved_brand_id)
        logger.info("[OrdersSync] resolved_company_id=%s", _resolved_company_id)
        logger.info("[OrdersSync] api_key_present=%s", str(_api_key_present).lower())
    else:
        logger.warning("[OrdersSync] credential_source=missing — no active LeafLink credential found")

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
    phase1_orders_upserted: int = 0
    total_pages_available: Optional[int] = None
    next_leaflink_url: Optional[str] = None
    next_page_number: Optional[int] = None

    # ------------------------------------------------------------------ #
    # Phase 1: Fetch LEAFLINK_PAGES_PER_REQUEST pages synchronously       #
    # ------------------------------------------------------------------ #
    if _active_cred and not force_db_only:
        cred = _active_cred
        api_key: str = cred.api_key or ""
        company_id: str = cred.company_id or ""

        _api_key_valid = bool(api_key.strip())
        _company_id_valid = bool(company_id.strip())

        if not _api_key_valid:
            logger.warning(
                "[OrdersSync] credential_invalid brand=%s api_key_missing — will serve DB only",
                _resolved_brand_id,
            )
            sync_metadata["credential_invalid"] = True
            sync_metadata["credential_error"] = f"LeafLink api_key is empty for brand {_resolved_brand_id}"
            sync_metadata["total_fetched_from_leaflink"] = None
            sync_metadata["used_force_full"] = force_full
        elif not _company_id_valid:
            logger.warning(
                "[OrdersSync] credential_invalid brand=%s company_id_missing — will serve DB only",
                _resolved_brand_id,
            )
            sync_metadata["credential_invalid"] = True
            sync_metadata["credential_error"] = f"LeafLink company_id is empty for brand {_resolved_brand_id}"
            sync_metadata["total_fetched_from_leaflink"] = None
            sync_metadata["used_force_full"] = force_full
        else:
            logger.info(
                "[OrdersSync] phase=1_start pages_to_fetch=%s start_page=%s brand=%s",
                pages_per_request,
                start_page,
                _resolved_brand_id,
            )

            _watchdog_url = os.getenv("OPSYN_WATCHDOG_WEBHOOK_URL")
            await emit_watchdog_event(
                event_type="sync_started",
                brand_id=_resolved_brand_id,
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

            try:
                from services.leaflink_client import LeafLinkClient, DEFAULT_LEAFLINK_BASE_URL
                from services.leaflink_sync import (
                    sync_leaflink_orders,
                    sync_leaflink_orders_headers_only,
                    sync_leaflink_line_items,
                )

                _api_url = f"{DEFAULT_LEAFLINK_BASE_URL}/orders-received/"
                logger.info(
                    "[OrdersSync] phase=1_fetch_init brand=%s company_id=%s api_url=%s",
                    _resolved_brand_id,
                    _resolved_company_id,
                    _api_url,
                )

                try:
                    client = LeafLinkClient(api_key=api_key, company_id=company_id, brand_id=_resolved_brand_id)
                except Exception as client_init_exc:
                    _etype = type(client_init_exc).__name__
                    _emsg = str(client_init_exc)
                    logger.error(
                        "[OrdersSync] phase=1_fetch failing_step=client_init error_type=%s error=%s brand=%s",
                        _etype, _emsg, _resolved_brand_id, exc_info=True,
                    )
                    return make_json_safe({
                        "ok": False,
                        "error": "internal_error",
                        "error_type": _etype,
                        "error_message": _emsg,
                        "failing_step": "phase_1_client_init",
                        "brand_id": _resolved_brand_id,
                        "company_id": _resolved_company_id,
                        "api_url": _api_url,
                        "traceback": traceback.format_exc(),
                    })

                loop = asyncio.get_event_loop()

                def _fetch_page_range_sync():
                    return client.fetch_orders_page_range(
                        start_page=start_page,
                        num_pages=pages_per_request,
                        page_size=LEAFLINK_PAGE_SIZE,
                        normalize=True,
                        brand=_resolved_brand_id,
                        resume_url=resume_url,
                    )

                logger.info("[OrdersSync] phase=1 leaflink_fetch_start start_page=%s num_pages=%s", start_page, pages_per_request)

                try:
                    fetch_result = await asyncio.wait_for(
                        loop.run_in_executor(_leaflink_executor, _fetch_page_range_sync),
                        timeout=LEAFLINK_TIMEOUT_SECONDS,
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        "[OrdersSync] phase=1 leaflink_timeout brand=%s timeout=%ss",
                        _resolved_brand_id,
                        LEAFLINK_TIMEOUT_SECONDS,
                    )
                    raise asyncio.TimeoutError(
                        f"LeafLink API did not respond within {LEAFLINK_TIMEOUT_SECONDS}s"
                    )
                except Exception as fetch_exc:
                    _etype = type(fetch_exc).__name__
                    _emsg = str(fetch_exc)
                    logger.error(
                        "[OrdersSync] phase=1_fetch failing_step=phase_1_fetch error_type=%s error=%s brand=%s api_url=%s",
                        _etype, _emsg, _resolved_brand_id, _api_url, exc_info=True,
                    )
                    return make_json_safe({
                        "ok": False,
                        "error": "internal_error",
                        "error_type": _etype,
                        "error_message": _emsg,
                        "failing_step": "phase_1_fetch",
                        "brand_id": _resolved_brand_id,
                        "company_id": _resolved_company_id,
                        "api_url": _api_url,
                        "traceback": traceback.format_exc(),
                    })

                orders_from_leaflink = fetch_result["orders"]
                pages_fetched_phase1 = fetch_result["pages_fetched"]
                next_leaflink_url = fetch_result["next_url"]
                next_page_number = fetch_result["next_page"]
                total_pages_available = fetch_result["total_pages"]
                total_count_leaflink = fetch_result["total_count"]

                logger.info(
                    "[OrdersSync] phase=1 leaflink_fetch_complete orders=%s pages=%s next_page=%s total_pages=%s",
                    len(orders_from_leaflink),
                    pages_fetched_phase1,
                    next_page_number,
                    total_pages_available,
                )

                # Phase 1 upsert: headers only, batched commits of 25 orders
                logger.info("[OrdersSync] phase=1 db_upsert_start count=%s", len(orders_from_leaflink))

                try:
                    sync_result = await asyncio.wait_for(
                        sync_leaflink_orders_headers_only(
                            _resolved_brand_id,
                            orders_from_leaflink,
                            pages_fetched=pages_fetched_phase1,
                        ),
                        timeout=DB_TIMEOUT_SECONDS,
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        "[OrdersSync] phase=1 db_upsert_timeout brand=%s timeout=%ss",
                        _resolved_brand_id,
                        DB_TIMEOUT_SECONDS,
                    )
                    raise asyncio.TimeoutError(
                        f"DB upsert did not complete within {DB_TIMEOUT_SECONDS}s"
                    )
                except Exception as upsert_exc:
                    _etype = type(upsert_exc).__name__
                    _emsg = str(upsert_exc)
                    logger.error(
                        "[OrdersSync] phase=1_upsert failing_step=phase_1_upsert error_type=%s error=%s brand=%s",
                        _etype, _emsg, _resolved_brand_id, exc_info=True,
                    )
                    return make_json_safe({
                        "ok": False,
                        "error": "internal_error",
                        "error_type": _etype,
                        "error_message": _emsg,
                        "failing_step": "phase_1_upsert",
                        "brand_id": _resolved_brand_id,
                        "company_id": _resolved_company_id,
                        "traceback": traceback.format_exc(),
                    })

                # Spawn background task to write line items without blocking response
                _li_brand_id = _resolved_brand_id
                _li_orders = orders_from_leaflink

                async def _background_line_items():
                    await sync_leaflink_line_items(_li_brand_id, _li_orders)

                try:
                    asyncio.create_task(_background_line_items())
                    logger.info(
                        "[OrdersSync] line_items_deferred count=%s",
                        len(orders_from_leaflink),
                    )
                except Exception as li_task_exc:
                    logger.error(
                        "[OrdersSync] line_items_task_create_error error=%s brand=%s",
                        li_task_exc, _resolved_brand_id, exc_info=True,
                    )

                phase1_orders_upserted = sync_result.get("orders_fetched", 0)
                logger.info(
                    "[OrdersSync] phase=1_complete pages_fetched=%s orders_upserted=%s",
                    pages_fetched_phase1,
                    phase1_orders_upserted,
                )

                sync_metadata["total_fetched_from_leaflink"] = phase1_orders_upserted
                sync_metadata["pages_fetched"] = pages_fetched_phase1
                sync_metadata["sync_duration_seconds"] = sync_result.get("sync_duration_seconds", 0)
                sync_metadata["used_force_full"] = force_full
                sync_metadata["current_page"] = start_page
                sync_metadata["total_pages_available"] = total_pages_available
                sync_metadata["total_count_leaflink"] = total_count_leaflink

                newest = sync_result.get("newest_order_date")
                sync_metadata["latest_order_date"] = newest.isoformat() if newest else None

                # ---------------------------------------------------------- #
                # Persist Phase 1 progress to BrandAPICredential             #
                # ---------------------------------------------------------- #
                _phase1_page = start_page + pages_fetched_phase1 - 1
                try:
                    async with AsyncSessionLocal() as _p1_sess:
                        async with _p1_sess.begin():
                            from sqlalchemy import select as _sel
                            _p1_res = await _p1_sess.execute(
                                _sel(BrandAPICredential).where(
                                    BrandAPICredential.id == cred.id,
                                )
                            )
                            _p1_cred = _p1_res.scalar_one_or_none()
                            if _p1_cred:
                                _p1_cred.last_synced_page = _phase1_page
                                _p1_cred.total_pages_available = total_pages_available
                                _p1_cred.total_orders_available = total_count_leaflink
                                _p1_cred.sync_status = "syncing" if (next_leaflink_url and next_page_number) else "idle"
                                _p1_cred.last_sync_at = datetime.now(timezone.utc)
                                _p1_cred.last_error = None
                    logger.info(
                        "[OrdersSync] phase1_progress_persisted brand=%s page=%s total_pages=%s",
                        _resolved_brand_id,
                        _phase1_page,
                        total_pages_available,
                    )
                    logger.info(
                        "[OrdersSync] persisted_total_orders_available brand=%s count=%s",
                        _resolved_brand_id,
                        total_count_leaflink,
                    )
                except Exception as _p1_persist_exc:
                    logger.error(
                        "[OrdersSync] phase1_progress_persist_error brand=%s error=%s",
                        _resolved_brand_id,
                        _p1_persist_exc,
                    )

                # ---------------------------------------------------------- #
                # Phase 2: Spawn persistent background sync for remaining     #
                # pages via BackgroundSyncManager                             #
                # ---------------------------------------------------------- #
                logger.info(
                    "[OrdersSync] phase2_check_conditions next_url=%s next_page=%s total_pages=%s",
                    bool(next_leaflink_url),
                    next_page_number,
                    total_pages_available,
                )

                pages_remaining = (total_pages_available - start_page - pages_fetched_phase1 + 1) if total_pages_available else None

                if next_leaflink_url and next_page_number and total_pages_available:
                    logger.info(
                        "[OrdersSync] phase2_before_manager_start brand=%s start_page=%s total_pages=%s pages_remaining=%s",
                        _resolved_brand_id,
                        next_page_number,
                        total_pages_available,
                        pages_remaining,
                    )

                    try:
                        logger.info(
                            "[OrdersSync] phase2_calling_manager_start_sync brand=%s api_key_len=%s company_id=%s",
                            _resolved_brand_id,
                            len(api_key) if api_key else 0,
                            company_id,
                        )

                        sync_manager.start_sync(
                            brand_id=_resolved_brand_id,
                            api_key=api_key,
                            company_id=company_id,
                            total_pages=total_pages_available,
                            start_page=next_page_number,
                            total_orders_available=total_count_leaflink,
                        )

                        logger.info(
                            "[OrdersSync] phase2_manager_started brand=%s task_created=true",
                            _resolved_brand_id,
                        )
                        sync_metadata["background_sync_active"] = True
                    except Exception as bg_task_exc:
                        logger.error(
                            "[OrdersSync] phase2_manager_error brand=%s error=%s",
                            _resolved_brand_id,
                            bg_task_exc,
                            exc_info=True,
                        )
                        sync_metadata["background_sync_active"] = False
                        sync_metadata["background_sync_error"] = str(bg_task_exc)
                else:
                    logger.info(
                        "[OrdersSync] phase2_skipped brand=%s reason=no_more_pages next_url=%s next_page=%s",
                        _resolved_brand_id,
                        bool(next_leaflink_url),
                        next_page_number,
                    )
                    sync_metadata["background_sync_active"] = False

                # Mark credential as successfully used
                from services.integration_credentials import mark_credential_success
                await mark_credential_success(cred)

                logger.info(
                    "[OrdersSync] response_returned source=live count=%s next_cursor=%s",
                    phase1_orders_upserted,
                    "present" if next_leaflink_url else "none",
                )

            except asyncio.TimeoutError as timeout_exc:
                timeout_msg = str(timeout_exc)
                logger.warning(
                    "[OrdersSync] phase=1 timeout brand=%s error=%s — falling back to DB",
                    brand_filter,
                    timeout_msg,
                )
                try:
                    await db.rollback()
                except Exception as rb_exc:
                    logger.warning("[OrdersSync] phase=1 rollback_failed error=%s", rb_exc)

                live_refresh_failed = True
                sync_error = timeout_msg
                sync_metadata["sync_error"] = sync_error
                sync_metadata["used_force_full"] = force_full

                _watchdog_url_to = os.getenv("OPSYN_WATCHDOG_WEBHOOK_URL")
                await emit_watchdog_event(
                    event_type="sync_failed",
                    brand_id=_resolved_brand_id or brand_filter,
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
            except Exception as phase1_exc:
                _p1_etype = type(phase1_exc).__name__
                _p1_emsg = str(phase1_exc)
                logger.error(
                    "[OrdersSync] phase=1 error_type=%s error=%s brand=%s",
                    _p1_etype, _p1_emsg, _resolved_brand_id, exc_info=True,
                )
                try:
                    await db.rollback()
                except Exception as rb_exc:
                    logger.warning("[OrdersSync] phase=1 rollback_failed error=%s", rb_exc)

                # Check if we already have orders in DB — if so, pause instead of error
                try:
                    async with AsyncSessionLocal() as _exc_count_sess:
                        _exc_count_res = await _exc_count_sess.execute(
                            select(func.count(Order.id)).where(Order.brand_id == _resolved_brand_id)
                        )
                        existing_count = _exc_count_res.scalar_one() or 0
                except Exception:
                    existing_count = 0

                if existing_count > 0:
                    logger.warning(
                        "[OrdersSync] leaflink_error_with_existing_data brand=%s error=%s existing_orders=%s",
                        _resolved_brand_id,
                        phase1_exc,
                        existing_count,
                    )
                    # Update credential to paused state
                    try:
                        async with AsyncSessionLocal() as _err_sess:
                            async with _err_sess.begin():
                                _err_res = await _err_sess.execute(
                                    select(BrandAPICredential).where(
                                        BrandAPICredential.id == cred.id,
                                    )
                                )
                                _err_cred = _err_res.scalar_one_or_none()
                                if _err_cred:
                                    _err_cred.sync_status = "paused"
                                    _err_cred.last_error = _p1_emsg
                    except Exception as _err_persist_exc:
                        logger.error(
                            "[OrdersSync] paused_state_persist_error brand=%s error=%s",
                            _resolved_brand_id,
                            _err_persist_exc,
                        )
                    return make_json_safe({
                        "ok": True,
                        "partial": True,
                        "message": "LeafLink temporarily unavailable, returning cached data",
                        "error": _p1_emsg,
                        "sync_status": "paused",
                        "existing_orders": existing_count,
                    })

                live_refresh_failed = True
                sync_error = _p1_emsg
                sync_metadata["sync_error"] = sync_error
                sync_metadata["error_type"] = _p1_etype
                sync_metadata["error_message"] = _p1_emsg
                sync_metadata["failing_step"] = "phase_1"
                sync_metadata["brand_id"] = _resolved_brand_id
                sync_metadata["company_id"] = _resolved_company_id
                sync_metadata["used_force_full"] = force_full

                from services.integration_credentials import mark_credential_invalid
                await mark_credential_invalid(cred, sync_error)

                _watchdog_url_exc = os.getenv("OPSYN_WATCHDOG_WEBHOOK_URL")
                await emit_watchdog_event(
                    event_type="sync_failed",
                    brand_id=_resolved_brand_id or brand_filter,
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


    else:
        sync_metadata["used_force_full"] = force_full
        if force_db_only:
            sync_metadata["force_db_only"] = True

    # ------------------------------------------------------------------ #
    # Build next_cursor for LeafLink page pagination                      #
    # ------------------------------------------------------------------ #
    next_cursor: Optional[str] = None
    if next_leaflink_url and next_page_number:
        cursor_payload = f"page={next_page_number}&url={next_leaflink_url}"
        next_cursor = base64.b64encode(cursor_payload.encode()).decode()

    # ------------------------------------------------------------------ #
    # Count total orders in DB (lightweight — no order data returned)     #
    # ------------------------------------------------------------------ #
    try:
        count_result = await db.execute(
            select(func.count(Order.id)).where(Order.brand_id == _resolved_brand_id)
        )
        total_in_database = count_result.scalar_one() or 0
    except Exception:
        total_in_database = 0

    # Determine sync_status from available state
    if sync_error:
        _sync_status = "error"
    elif sync_metadata.get("background_sync_active"):
        _sync_status = "syncing"
    elif phase1_orders_upserted > 0 or sync_metadata.get("pages_fetched"):
        _sync_status = "syncing"
    else:
        _sync_status = "idle"

    # Determine percent_complete
    _total_orders_available = sync_metadata.get("total_count_leaflink") or sync_metadata.get("total_orders_available")
    if _total_orders_available and _total_orders_available > 0:
        _percent_complete = round((total_in_database / _total_orders_available) * 100, 1)
        _percent_complete = min(100.0, _percent_complete)
    else:
        _pages_fetched = sync_metadata.get("pages_fetched", 0) or 0
        _total_pages = total_pages_available or 0
        _percent_complete = round((_pages_fetched / _total_pages) * 100, 1) if _total_pages else 0.0
        _percent_complete = min(100.0, _percent_complete)

    logger.info(
        "[OrdersSync] response_metadata_only brand=%s pages=%s/%s orders=%s percent=%.1f%%",
        _resolved_brand_id,
        sync_metadata.get("pages_fetched", 0),
        total_pages_available,
        total_in_database,
        _percent_complete,
    )

    return make_json_safe({
        "ok": True,
        "sync_status": _sync_status,
        "background_sync_active": sync_metadata.get("background_sync_active", False),
        "pages_synced": sync_metadata.get("pages_fetched", 0),
        "total_pages": total_pages_available,
        "total_orders_in_db": total_in_database,
        "total_orders_available": _total_orders_available,
        "percent_complete": _percent_complete,
        "next_cursor": next_cursor,
        "error": sync_error,
    })



@router.get("/sync/status")
async def orders_sync_status(
    brand: Optional[str] = Query(None, description="Brand slug to check (e.g., 'noble-nectar')"),
    db: AsyncSession = Depends(get_db),
):
    """
    Return the current background sync progress for a brand.

    Combines in-memory state from BackgroundSyncManager with persisted
    progress from BrandAPICredential and a live order count from the DB.

    Response fields:
      ok                           — always true unless brand is missing
      brand_id                     — echoed brand slug
      background_sync_active       — true if a background task is running
      pages_synced                 — last page number successfully synced
      total_pages                  — total pages reported by LeafLink
      percent_complete             — (pages_synced / total_pages) * 100
      total_orders_in_db           — COUNT(*) of orders for this brand
      last_synced_at               — ISO timestamp of last successful sync
      sync_status                  — "idle" | "syncing" | "paused" | "error"
      estimated_completion_minutes — rough estimate based on pages/minute
      errors                       — list of recent error strings
    """
    timestamp = datetime.now(timezone.utc).isoformat()

    if not brand:
        return {
            "ok": False,
            "error": "brand query parameter is required",
            "timestamp": timestamp,
        }

    logger.info("[OrdersSync] status_check brand=%s", brand)

    # ------------------------------------------------------------------ #
    # Load credential from DB                                             #
    # ------------------------------------------------------------------ #
    try:
        cred_result = await db.execute(
            select(BrandAPICredential).where(
                BrandAPICredential.brand_id == brand,
                BrandAPICredential.integration_name == "leaflink",
            )
        )
        cred = cred_result.scalar_one_or_none()
    except Exception as cred_exc:
        error_type = type(cred_exc).__name__
        error_msg = str(cred_exc)
        tb = traceback.format_exc()
        logger.error(
            "[OrdersSync] status_check_error brand=%s type=%s msg=%s",
            brand,
            error_type,
            error_msg,
            exc_info=True,
        )
        return {
            "ok": False,
            "brand_id": brand,
            "error": "Database error during credential lookup",
            "error_type": error_type,
            "error_message": error_msg,
            "traceback": tb,
            "timestamp": timestamp,
        }

    # ------------------------------------------------------------------ #
    # Count orders in DB for this brand                                   #
    # ------------------------------------------------------------------ #
    try:
        count_result = await db.execute(
            select(func.count(Order.id)).where(Order.brand_id == brand)
        )
        total_orders_in_db = count_result.scalar_one()
    except Exception as count_exc:
        logger.error("[OrdersSync] status_check count_error brand=%s error=%s", brand, count_exc)
        total_orders_in_db = 0

    # ------------------------------------------------------------------ #
    # Merge in-memory manager state with DB state                         #
    # ------------------------------------------------------------------ #
    manager_status = sync_manager.get_status(brand)
    bg_active = sync_manager.is_syncing(brand)

    if cred is not None:
        db_pages_synced = cred.last_synced_page or 0
        db_total_pages = cred.total_pages_available or 0
        db_sync_status = cred.sync_status or "idle"
        db_last_synced_at = cred.last_sync_at.isoformat() if cred.last_sync_at else None
        db_last_error = cred.last_error
        total_orders_available = cred.total_orders_available
    else:
        db_pages_synced = 0
        db_total_pages = 0
        db_sync_status = "idle"
        db_last_synced_at = None
        db_last_error = None
        total_orders_available = None

    # In-memory state is more current than DB when a sync is active
    db_pages_synced_raw = db_pages_synced  # preserve original stored value for logging
    pages_synced = manager_status["pages_synced"] if bg_active else db_pages_synced
    total_pages = manager_status["total_pages"] if manager_status["total_pages"] else db_total_pages

    # ------------------------------------------------------------------ #
    # Calculate inferred progress from actual order count                 #
    # ------------------------------------------------------------------ #
    # LeafLink returns 50 orders per page, so infer pages from order count
    inferred_pages_from_orders = total_orders_in_db // 50

    # Use the higher of stored/in-memory or inferred progress
    # (stored might lag behind actual if DB writes are faster than progress updates)
    displayed_pages_synced = max(pages_synced, inferred_pages_from_orders)

    # If inferred is higher, log and persist the correction
    if inferred_pages_from_orders > pages_synced:
        logger.info(
            "[OrdersSyncStatus] corrected_progress brand=%s old_page=%s inferred_page=%s displayed_page=%s total_orders=%s",
            brand,
            db_pages_synced_raw,
            inferred_pages_from_orders,
            displayed_pages_synced,
            total_orders_in_db,
        )

        # Persist the corrected progress back to DB
        try:
            async with AsyncSessionLocal() as _corr_sess:
                async with _corr_sess.begin():
                    _corr_res = await _corr_sess.execute(
                        select(BrandAPICredential).where(
                            BrandAPICredential.brand_id == brand,
                            BrandAPICredential.integration_name == "leaflink",
                        )
                    )
                    _corr_cred = _corr_res.scalar_one_or_none()
                    if _corr_cred:
                        _corr_cred.last_synced_page = inferred_pages_from_orders
                        _corr_cred.last_sync_at = datetime.now(timezone.utc)
            logger.info(
                "[OrdersSyncStatus] persisted_corrected_progress brand=%s page=%s",
                brand,
                inferred_pages_from_orders,
            )
        except Exception as _corr_exc:
            logger.error(
                "[OrdersSyncStatus] persist_correction_error brand=%s error=%s",
                brand,
                _corr_exc,
                exc_info=True,
            )

    # ------------------------------------------------------------------ #
    # Calculate percent complete based on actual order counts (preferred) #
    # ------------------------------------------------------------------ #
    effective_sync_status = None

    if total_orders_available and total_orders_available > 0:
        percent_complete = round((total_orders_in_db / total_orders_available) * 100, 1)
        percent_complete = min(100.0, percent_complete)
        logger.info(
            "[OrdersSyncStatus] percent_from_orders brand=%s orders=%s/%s percent=%.1f%%",
            brand,
            total_orders_in_db,
            total_orders_available,
            percent_complete,
        )
    else:
        # Fallback to page-based calculation if total_orders_available not available
        percent_complete = round((displayed_pages_synced / total_pages) * 100, 1) if total_pages else 0.0
        percent_complete = min(100.0, percent_complete)
        logger.info(
            "[OrdersSyncStatus] percent_from_pages brand=%s pages=%s/%s percent=%.1f%%",
            brand,
            displayed_pages_synced,
            total_pages,
            percent_complete,
        )

    # Detect completion: if we've loaded all available orders
    if total_orders_available and total_orders_in_db >= total_orders_available:
        effective_sync_status = "complete"
        bg_active = False
        percent_complete = 100.0
        logger.info(
            "[OrdersSyncStatus] detected_completion_from_orders brand=%s orders=%s/%s",
            brand,
            total_orders_in_db,
            total_orders_available,
        )
    # Fallback: detect completion from page count if order count not available
    elif total_pages and displayed_pages_synced >= total_pages:
        effective_sync_status = "complete"
        bg_active = False
        percent_complete = 100.0
        logger.info(
            "[OrdersSyncStatus] detected_completion brand=%s pages=%s/%s orders=%s",
            brand,
            displayed_pages_synced,
            total_pages,
            total_orders_in_db,
        )

    # Reconcile sync_status: if DB says syncing but manager says inactive, trust DB
    if db_sync_status == "syncing" and not bg_active:
        effective_sync_status = "syncing"
        bg_active = True  # Infer that sync is still running
    else:
        # Use effective_sync_status from above (may be "complete" if all orders/pages synced)
        if effective_sync_status is None:
            effective_sync_status = "syncing" if bg_active else db_sync_status

    # Estimate completion: 4 pages per batch, ~10 seconds per batch → 0.4 pages/s
    pages_remaining = max(total_pages - displayed_pages_synced, 0)
    estimated_completion_minutes: Optional[float] = None
    if bg_active and pages_remaining > 0:
        pages_per_second = 0.4
        estimated_completion_minutes = round(pages_remaining / pages_per_second / 60, 1)

    errors = list(manager_status.get("errors", []))
    if db_last_error and db_last_error not in errors:
        errors = [db_last_error] + errors

    logger.info(
        "[OrdersSyncStatus] response brand=%s stored_page=%s inferred_page=%s displayed_page=%s/%s percent=%.1f%% orders=%s/%s active=%s status=%s",
        brand,
        db_pages_synced_raw,
        inferred_pages_from_orders,
        displayed_pages_synced,
        total_pages,
        percent_complete,
        total_orders_in_db,
        total_orders_available,
        bg_active,
        effective_sync_status,
    )

    return make_json_safe({
        "ok": True,
        "brand_id": brand,
        "background_sync_active": bg_active,
        "pages_synced": displayed_pages_synced,
        "total_pages": total_pages,
        "total_orders_in_db": total_orders_in_db,
        "total_orders_available": total_orders_available,
        "percent_complete": percent_complete,
        "last_synced_at": db_last_synced_at,
        "sync_status": effective_sync_status,
        "estimated_completion_minutes": estimated_completion_minutes,
        "errors": errors,
    })
