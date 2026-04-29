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
def get_orders():
    return {
        "items": [],
        "count": 0,
        "message": "Orders endpoint is live",
    }


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

    Query params:
    - brand_id / brand_slug: Brand slug or ID (e.g., \"noble-nectar\")
    - updated_after: ISO timestamp — ignored when force_full=true
    - cursor: Pagination cursor from previous response
    - limit: Orders to return from DB (1-1000, default 500)
    - force_full: true/false — force complete backfill from LeafLink
    - pages_per_request: LeafLink pages per sync call (1-20, default 3)
    """
    endpoint_start = time.monotonic()

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
                                _p1_cred.sync_status = "syncing" if (next_leaflink_url and next_page_number) else "idle"
                                _p1_cred.last_sync_at = datetime.now(timezone.utc)
                                _p1_cred.last_error = None
                    logger.info(
                        "[OrdersSync] phase1_progress_persisted brand=%s page=%s total_pages=%s",
                        _resolved_brand_id,
                        _phase1_page,
                        total_pages_available,
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
                pages_remaining = (total_pages_available - start_page - pages_fetched_phase1 + 1) if total_pages_available else None

                if next_leaflink_url and next_page_number and total_pages_available:
                    logger.info(
                        "[OrdersSync] bg_continuous_sync_started brand=%s start_page=%s total_pages=%s",
                        _resolved_brand_id,
                        next_page_number,
                        total_pages_available,
                    )

                    try:
                        sync_manager.start_sync(
                            brand_id=_resolved_brand_id,
                            api_key=api_key,
                            company_id=company_id,
                            total_pages=total_pages_available,
                            start_page=next_page_number,
                        )
                        sync_metadata["background_sync_active"] = True
                    except Exception as bg_task_exc:
                        logger.error(
                            "[OrdersSync] background_task_create_error error_type=%s error=%s brand=%s",
                            type(bg_task_exc).__name__, bg_task_exc, _resolved_brand_id, exc_info=True,
                        )
                        sync_metadata["background_sync_active"] = False
                        sync_metadata["background_task_error"] = str(bg_task_exc)
                        sync_metadata["background_task_error_type"] = type(bg_task_exc).__name__
                else:
                    logger.info("[OrdersSync] no_more_pages — background sync not needed")
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
    # Phase 2 (read): Serve orders from DB — always a fresh session       #
    # ------------------------------------------------------------------ #
    logger.info("[OrdersSync] phase=2_read new_session_created")

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

    # Decode DB cursor (separate from LeafLink page cursor — this is the DB row ID cursor)
    db_cursor_id = None
    # Note: the `cursor` param is now used for LeafLink page pagination, not DB row pagination.
    # DB pagination uses the `limit` param only. If the client needs DB-level pagination,
    # they should use the returned next_cursor from the DB read below.

    async with AsyncSessionLocal() as read_session:
        try:
            logger.info("[OrdersSync] phase=2_read read_start brand=%s", brand_filter)

            query = select(Order)

            if brand_filter:
                query = query.where(Order.brand_id == brand_filter)
                logger.info("[OrdersSync] filter brand=%s", brand_filter)

            if updated_after_dt:
                query = query.where(Order.updated_at >= updated_after_dt)

            # Sort by updated_at ascending for consistent incremental sync
            query = query.order_by(Order.updated_at.asc(), Order.id.asc())

            # Fetch limit + 1 to determine if there are more DB results
            query_with_limit = query.limit(limit + 1)

            result = await read_session.execute(query_with_limit)
            orders = result.scalars().all()

            db_has_more = len(orders) > limit
            if db_has_more:
                orders = orders[:limit]

            # Count total orders in DB for this brand
            count_query = select(func.count(Order.id))
            if brand_filter:
                count_query = count_query.where(Order.brand_id == brand_filter)
            count_result = await read_session.execute(count_query)
            total_in_database = count_result.scalar_one()

            logger.info("[OrdersSync] phase=2_read read_success count=%s total_in_db=%s", len(orders), total_in_database)

            # Build response orders list
            orders_data = []
            db_next_cursor = None
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

                order_date = order.external_updated_at or order.external_created_at or order.updated_at
                if order_date is not None:
                    if newest_order_date is None or order_date > newest_order_date:
                        newest_order_date = order_date
                    if oldest_order_date is None or order_date < oldest_order_date:
                        oldest_order_date = order_date

                if order == orders[-1]:
                    db_next_cursor = base64.b64encode(str(order.id).encode()).decode()

            server_time = datetime.now(timezone.utc).isoformat()
            newest_order_date_iso = newest_order_date.isoformat() if newest_order_date else None
            oldest_order_date_iso = oldest_order_date.isoformat() if oldest_order_date else None

            # Build next_cursor for LeafLink page pagination
            leaflink_next_cursor: Optional[str] = None
            if next_leaflink_url and next_page_number:
                cursor_payload = f"page={next_page_number}&url={next_leaflink_url}"
                leaflink_next_cursor = base64.b64encode(cursor_payload.encode()).decode()

            leaflink_has_more = bool(next_leaflink_url)

            # Populate sync_metadata defaults
            sync_metadata.setdefault("total_fetched_from_leaflink", None)
            sync_metadata.setdefault("pages_fetched", None)
            sync_metadata.setdefault("sync_duration_seconds", None)
            sync_metadata.setdefault("latest_order_date", None)
            sync_metadata.setdefault("background_sync_active", False)
            sync_metadata.setdefault("current_page", start_page)
            sync_metadata.setdefault("total_pages_available", total_pages_available)
            sync_metadata["total_in_database"] = total_in_database
            sync_metadata["total_returned"] = len(orders_data)
            sync_metadata["brand_id"] = _resolved_brand_id or brand_filter
            sync_metadata["company_id"] = _resolved_company_id

            # Determine response source
            fetched_from_leaflink = sync_metadata.get("total_fetched_from_leaflink")
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

            sync_metadata["returning_cache"] = returning_cache

            sync_status = "error" if sync_error else "success"

            logger.info("[OrdersAPI] source=%s", source)
            logger.info("[OrdersAPI] db_count=%s", len(orders_data))
            logger.info("[OrdersAPI] newest_order_date=%s", newest_order_date_iso)
            logger.info("[OrdersAPI] refreshed_at=%s", server_time)
            logger.info(
                "[OrdersSync] response count=%s has_more=%s brand=%s total_in_db=%s",
                len(orders_data),
                leaflink_has_more,
                brand_filter,
                total_in_database,
            )
            logger.info(
                "[OrdersSync] response_returned source=%s count=%s next_cursor=%s",
                source,
                len(orders_data),
                leaflink_next_cursor[:8] + "..." if leaflink_next_cursor else "none",
            )

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
                "sync_metadata": {
                    **sync_metadata,
                    "current_page": start_page,
                    "pages_fetched": sync_metadata.get("pages_fetched"),
                    "total_pages_available": total_pages_available,
                    "total_fetched_from_leaflink": phase1_orders_upserted,
                    "background_sync_active": sync_metadata.get("background_sync_active", False),
                },
                "pagination": {
                    "has_more": leaflink_has_more,
                    "next_cursor": leaflink_next_cursor,
                },
                # Legacy fields kept for backwards compatibility
                "next_cursor": leaflink_next_cursor,
                "has_more": leaflink_has_more,
                "server_time": server_time,
                "sync_version": 2,
                "last_synced_at": server_time,
                "last_error": sync_error,
                "live_refresh_failed": live_refresh_failed,
                "error": sync_error if live_refresh_failed else None,
            })


        except Exception as phase2_exc:
            _p2_etype = type(phase2_exc).__name__
            _p2_emsg = str(phase2_exc)
            logger.error(
                "[OrdersSync] phase=2_read error_type=%s error=%s brand=%s",
                _p2_etype, _p2_emsg, brand_filter, exc_info=True,
            )
            server_time = datetime.now(timezone.utc).isoformat()
            return make_json_safe({
                "ok": False,
                "error": "internal_error",
                "error_type": _p2_etype,
                "error_message": _p2_emsg,
                "failing_step": "phase_2_read",
                "brand_id": _resolved_brand_id or brand_filter,
                "company_id": _resolved_company_id,
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
                "pagination": {"has_more": False, "next_cursor": None},
                "sync_metadata": sync_metadata,
                "last_error": _p2_emsg,
                "traceback": traceback.format_exc(),
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
    else:
        db_pages_synced = 0
        db_total_pages = 0
        db_sync_status = "idle"
        db_last_synced_at = None
        db_last_error = None

    # In-memory state is more current than DB when a sync is active
    pages_synced = manager_status["pages_synced"] if bg_active else db_pages_synced
    total_pages = manager_status["total_pages"] if manager_status["total_pages"] else db_total_pages

    percent_complete = round((pages_synced / total_pages) * 100, 1) if total_pages else 0.0

    # Estimate completion: 4 pages per batch, ~10 seconds per batch → 0.4 pages/s
    pages_remaining = max(total_pages - pages_synced, 0)
    estimated_completion_minutes: Optional[float] = None
    if bg_active and pages_remaining > 0:
        pages_per_second = 0.4
        estimated_completion_minutes = round(pages_remaining / pages_per_second / 60, 1)

    errors = list(manager_status.get("errors", []))
    if db_last_error and db_last_error not in errors:
        errors = [db_last_error] + errors

    # Reconcile sync_status: if manager says active but DB says idle, trust manager
    effective_sync_status = "syncing" if bg_active else db_sync_status

    logger.info(
        "[OrdersSync] status_response brand=%s active=%s pages=%s/%s percent=%.1f%% orders_in_db=%s",
        brand,
        bg_active,
        pages_synced,
        total_pages,
        percent_complete,
        total_orders_in_db,
    )

    return make_json_safe({
        "ok": True,
        "brand_id": brand,
        "background_sync_active": bg_active,
        "pages_synced": pages_synced,
        "total_pages": total_pages,
        "percent_complete": percent_complete,
        "total_orders_in_db": total_orders_in_db,
        "last_synced_at": db_last_synced_at,
        "sync_status": effective_sync_status,
        "estimated_completion_minutes": estimated_completion_minutes,
        "errors": errors,
    })
