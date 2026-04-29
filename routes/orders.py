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
LEAFLINK_TIMEOUT_SECONDS = 30  # Max seconds to wait for Phase 1 LeafLink fetch (per page batch)
DB_TIMEOUT_SECONDS = 10        # Max seconds to wait for DB upsert in Phase 1

# Background sync: no hard timeout — runs until complete or error
BACKGROUND_SYNC_PAGE_TIMEOUT = 45  # Seconds per page fetch in background (generous)


async def _background_sync_remaining_pages(
    api_key: str,
    company_id: str,
    brand_id: str,
    cred_id: int,
    resume_url: str,
    pages_already_fetched: int,
    total_pages_estimate: Optional[int],
) -> None:
    """
    Background task: fetch all remaining LeafLink pages after Phase 1 and upsert
    them to the database one page at a time.

    This runs fire-and-forget after the /orders/sync response is returned to the
    client.  It uses its own DB sessions so it is completely decoupled from the
    request lifecycle.

    Args:
        api_key: LeafLink API key for the brand.
        company_id: LeafLink company ID for the brand.
        brand_id: Brand slug / ID used for DB scoping and logging.
        cred_id: Primary key of the BrandAPICredential row to update progress on.
        resume_url: The ``next`` URL returned by Phase 1 — where background sync
            starts.
        pages_already_fetched: Number of pages Phase 1 already fetched (for
            accurate page numbering in logs).
        total_pages_estimate: Total pages estimate from Phase 1 (for logging).
    """
    from services.leaflink_client import LeafLinkClient
    from services.leaflink_sync import sync_leaflink_orders
    from sqlalchemy import select as sa_select

    logger.info(
        "[OrdersSync] background_sync_start brand=%s resume_url=%s pages_already=%s total_estimate=%s",
        brand_id,
        resume_url,
        pages_already_fetched,
        total_pages_estimate,
    )

    loop = asyncio.get_event_loop()
    client = LeafLinkClient(api_key=api_key, company_id=company_id, brand_id=brand_id)

    current_url: Optional[str] = resume_url
    page_number = pages_already_fetched + 1
    total_bg_pages = 0
    total_bg_orders = 0

    while current_url:
        # Capture for closure
        _url = current_url
        _page = page_number

        def _fetch_one_page() -> dict:
            return client.fetch_recent_orders(
                max_pages=1,
                normalize=True,
                brand=brand_id,
                start_url=_url,
            )

        try:
            fetch_result = await asyncio.wait_for(
                loop.run_in_executor(_leaflink_executor, _fetch_one_page),
                timeout=BACKGROUND_SYNC_PAGE_TIMEOUT,
            )
        except asyncio.TimeoutError:
            logger.error(
                "[OrdersSync] background_page=%s timeout brand=%s — stopping background sync",
                _page,
                brand_id,
            )
            break
        except Exception as fetch_exc:
            logger.error(
                "[OrdersSync] background_page=%s fetch_error brand=%s error=%s — stopping background sync",
                _page,
                brand_id,
                fetch_exc,
            )
            break

        page_orders = fetch_result.get("orders", [])
        next_url = fetch_result.get("next_url")

        logger.info(
            "[OrdersSync] background_page=%s fetched=%s brand=%s",
            _page,
            len(page_orders),
            brand_id,
        )

        if page_orders:
            try:
                async with AsyncSessionLocal() as bg_session:
                    async with bg_session.begin():
                        await sync_leaflink_orders(
                            bg_session,
                            brand_id,
                            page_orders,
                            pages_fetched=_page,
                        )
                total_bg_orders += len(page_orders)
            except Exception as upsert_exc:
                logger.error(
                    "[OrdersSync] background_page=%s upsert_error brand=%s error=%s",
                    _page,
                    brand_id,
                    upsert_exc,
                )
                # Continue to next page even if one upsert fails

        total_bg_pages += 1

        # Update progress on the credential row
        try:
            async with AsyncSessionLocal() as prog_session:
                async with prog_session.begin():
                    cred_result = await prog_session.execute(
                        sa_select(BrandAPICredential).where(BrandAPICredential.id == cred_id)
                    )
                    db_cred = cred_result.scalar_one_or_none()
                    if db_cred:
                        db_cred.last_synced_page = _page
        except Exception as prog_exc:
            logger.warning(
                "[OrdersSync] background_page=%s progress_update_failed brand=%s error=%s",
                _page,
                brand_id,
                prog_exc,
            )

        current_url = next_url
        page_number += 1

    # Mark sync_in_progress = False when done
    try:
        async with AsyncSessionLocal() as done_session:
            async with done_session.begin():
                cred_result = await done_session.execute(
                    sa_select(BrandAPICredential).where(BrandAPICredential.id == cred_id)
                )
                db_cred = cred_result.scalar_one_or_none()
                if db_cred:
                    db_cred.sync_in_progress = False
                    db_cred.last_synced_page = page_number - 1
    except Exception as done_exc:
        logger.warning(
            "[OrdersSync] background_sync_complete_update_failed brand=%s error=%s",
            brand_id,
            done_exc,
        )

    logger.info(
        "[OrdersSync] background_sync_complete brand=%s total_pages_synced=%s total_orders_synced=%s",
        brand_id,
        total_bg_pages,
        total_bg_orders,
    )


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
    cursor: Optional[str] = Query(None, description="Pagination cursor from previous response"),
    limit: int = Query(500, ge=1, le=1000, description="Number of orders to return (default 500, max 1000)"),
    force_full: bool = Query(False, description="Force a complete backfill from LeafLink, ignoring updated_after"),
    max_pages_per_request: int = Query(3, ge=1, le=10, description="Pages to fetch synchronously in Phase 1 (default 3, max 10). Remaining pages sync in background."),
    db: AsyncSession = Depends(get_db),
):
    """
    Get orders for incremental sync with cursor-based pagination.

    Uses a two-phase architecture to return quickly while syncing all data:

    Phase 1 (synchronous, returns to client):
      - Fetches only max_pages_per_request pages from LeafLink (default: 3 = ~300 orders)
      - Upserts those orders to the database immediately
      - Returns within 1-2 seconds with partial data + pagination metadata

    Phase 2 (background, fire-and-forget):
      - Continues fetching remaining pages after the response is sent
      - Upserts each page to the database as it arrives
      - No timeout — runs until all pages are fetched or an error occurs
      - Progress tracked in BrandAPICredential.last_synced_page

    Query params:
    - brand_id: Brand slug or ID to filter orders (e.g., "noble-nectar")
    - brand_slug: Alias for brand_id
    - updated_after: ISO timestamp (e.g., "2026-04-25T12:30:00Z") — ignored when force_full=true
    - cursor: Pagination cursor from previous response
    - limit: Number of orders to return from DB (1-1000, default 500)
    - force_full: true/false — force complete backfill from LeafLink (default false)
    - max_pages_per_request: Pages to fetch synchronously (1-10, default 3)

    Response includes:
    - orders: Array of order objects (from DB after Phase 1 upsert)
    - sync_metadata: Sync statistics including background_sync_in_progress, total_pages_estimate
    - next_cursor: Cursor for next page of DB results (if has_more=true)
    - has_more: Whether more orders exist in DB
    - background_sync_in_progress: true if background sync was triggered
    - total_pages_estimate: Estimated total pages from LeafLink API
    - pages_fetched_so_far: Pages fetched in Phase 1
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
        "[OrdersSync] request_start path=%s brand=%s org=%s updated_after=%s cursor=%s limit=%s force_full=%s max_pages_per_request=%s",
        request.url.path,
        brand_filter,
        request.headers.get("x-opsyn-org") or request.headers.get("x-org-id"),
        updated_after,
        cursor[:8] + "..." if cursor else None,
        limit,
        force_full,
        max_pages_per_request,
    )

    logger.info("[OrdersSync] requested_brand=%s", brand_filter)

    # ------------------------------------------------------------------ #
    # Load the first active LeafLink credential from DB                   #
    # (same logic as /leaflink/debug — not filtered by brand_id)          #
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
    background_sync_in_progress: bool = False
    total_pages_estimate: Optional[int] = None
    pages_fetched_so_far: int = 0
    _phase1_next_url: Optional[str] = None

    if _active_cred and not force_db_only:
        # ------------------------------------------------------------------ #
        # Phase 1: Quick synchronous fetch — only max_pages_per_request pages #
        # ------------------------------------------------------------------ #
        logger.info(
            "[OrdersSync] phase=1_start max_pages=%s brand=%s",
            max_pages_per_request,
            _resolved_brand_id,
        )
        cred = None
        try:
            from services.leaflink_client import LeafLinkClient
            from services.leaflink_sync import sync_leaflink_orders

            logger.info("[OrdersAPI] db_fetch_start")
            logger.info("[OrdersSync] phase=A leaflink_pull_start")
            logger.info("[OrdersSync] leaflink_fetch_start")

            cred = _active_cred

            api_key: str = cred.api_key or ""
            company_id: str = cred.company_id or ""

            # Validate that the credential is actually usable before hitting LeafLink
            _api_key_valid = bool(api_key.strip())
            _company_id_valid = bool(company_id.strip())

            if not _api_key_valid:
                logger.warning(
                    "[OrdersAPI] credential_invalid brand=%s api_key_missing — will serve DB only",
                    _resolved_brand_id,
                )
                sync_metadata["credential_missing"] = False
                sync_metadata["credential_invalid"] = True
                sync_metadata["credential_error"] = f"LeafLink api_key is empty for brand {_resolved_brand_id}"
                sync_metadata["total_fetched_from_leaflink"] = None
                sync_metadata["used_force_full"] = force_full
                logger.info("[OrdersSync] phase=A success brand=%s (invalid credential — no api_key)", _resolved_brand_id)
                # Fall through to Phase B (DB read) without attempting LeafLink
            elif not _company_id_valid:
                logger.warning(
                    "[OrdersAPI] credential_invalid brand=%s company_id_missing — will serve DB only",
                    _resolved_brand_id,
                )
                sync_metadata["credential_missing"] = False
                sync_metadata["credential_invalid"] = True
                sync_metadata["credential_error"] = f"LeafLink company_id is empty for brand {_resolved_brand_id}"
                sync_metadata["total_fetched_from_leaflink"] = None
                sync_metadata["used_force_full"] = force_full
                logger.info("[OrdersSync] phase=A success brand=%s (invalid credential — no company_id)", _resolved_brand_id)
                # Fall through to Phase B (DB read) without attempting LeafLink
            else:
                logger.info(
                    "[LeafLink] sync_start brand=%s credential_source=db force_full=%s",
                    _resolved_brand_id,
                    force_full,
                )
                logger.info(
                    "[OrdersSync] triggering_leaflink_sync brand=%s force_full=%s",
                    _resolved_brand_id,
                    force_full,
                )

                # Watchdog: emit sync_started
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

                client = LeafLinkClient(api_key=api_key, company_id=company_id, brand_id=_resolved_brand_id)

                # Phase 1: fetch only max_pages_per_request pages synchronously.
                # When force_full=True we still use the same limit — background sync
                # will handle the rest regardless.
                phase1_max_pages = max_pages_per_request

                # ---------------------------------------------------------- #
                # Wrap the synchronous LeafLink HTTP call in a thread so we   #
                # can apply asyncio.wait_for() timeout without blocking the   #
                # event loop. fetch_recent_orders() uses requests (sync I/O). #
                # ---------------------------------------------------------- #
                logger.info("[OrdersAPI] leaflink_refresh_start")
                loop = asyncio.get_event_loop()

                def _fetch_orders_sync():
                    return client.fetch_recent_orders(
                        max_pages=phase1_max_pages,
                        normalize=True,
                        brand=_resolved_brand_id,
                    )

                try:
                    fetch_result = await asyncio.wait_for(
                        loop.run_in_executor(_leaflink_executor, _fetch_orders_sync),
                        timeout=LEAFLINK_TIMEOUT_SECONDS,
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        "[OrdersAPI] leaflink_refresh_timeout brand=%s timeout=%ss",
                        _resolved_brand_id,
                        LEAFLINK_TIMEOUT_SECONDS,
                    )
                    raise asyncio.TimeoutError(
                        f"LeafLink API did not respond within {LEAFLINK_TIMEOUT_SECONDS}s"
                    )

                orders_from_leaflink = fetch_result["orders"]
                pages_fetched = fetch_result["pages_fetched"]
                _phase1_next_url = fetch_result.get("next_url")
                _total_count = fetch_result.get("total_count")

                # Derive total pages estimate from total_count (100 orders/page)
                if _total_count and _total_count > 0:
                    total_pages_estimate = (_total_count + 99) // 100
                pages_fetched_so_far = pages_fetched

                logger.info(
                    "[OrdersSync] phase=1_complete pages_fetched=%s orders_fetched=%s total_count=%s total_pages_estimate=%s",
                    pages_fetched,
                    len(orders_from_leaflink),
                    _total_count,
                    total_pages_estimate,
                )
                logger.info("[OrdersSync] phase=A leaflink_pull_success count=%s", len(orders_from_leaflink))
                logger.info(
                    "[OrdersSync] leaflink_fetch_complete brand=%s orders=%s pages=%s force_full=%s",
                    _resolved_brand_id,
                    len(orders_from_leaflink),
                    pages_fetched,
                    force_full,
                )
                logger.info(
                    "[LeafLink] fetched=%s pages=%s brand=%s",
                    len(orders_from_leaflink),
                    pages_fetched,
                    _resolved_brand_id,
                )
                logger.info("[OrdersSync] total_fetched_from_leaflink=%s", len(orders_from_leaflink))

                # Log per-page counts (Phase 1 fetched them all at once, so approximate)
                for _pg in range(1, pages_fetched + 1):
                    logger.info("[OrdersSync] page=%s fetched=~100 brand=%s", _pg, _resolved_brand_id)

                # Watchdog: emit sync_progress after fetch
                await emit_watchdog_event(
                    event_type="sync_progress",
                    brand_id=_resolved_brand_id,
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
                            _resolved_brand_id,
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
                        _resolved_brand_id,
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
                logger.info(
                    "[OrdersSync] upserted_to_db=%s",
                    sync_result.get("orders_fetched", 0),
                )

                sync_metadata["total_fetched_from_leaflink"] = sync_result.get("orders_fetched", 0)
                sync_metadata["pages_fetched"] = sync_result.get("pages_fetched", pages_fetched)
                sync_metadata["sync_duration_seconds"] = sync_result.get("sync_duration_seconds", 0)
                sync_metadata["used_force_full"] = force_full
                sync_metadata["pages_fetched_so_far"] = pages_fetched_so_far
                sync_metadata["total_pages_estimate"] = total_pages_estimate

                newest = sync_result.get("newest_order_date")
                sync_metadata["latest_order_date"] = newest.isoformat() if newest else None

                logger.info(
                    "[LeafLink] upserted=%s created=%s updated=%s brand=%s",
                    sync_result.get("orders_fetched", 0),
                    sync_result.get("created", 0),
                    sync_result.get("updated", 0),
                    _resolved_brand_id,
                )

                # ---------------------------------------------------------- #
                # Phase 2: Trigger background sync for remaining pages        #
                # ---------------------------------------------------------- #
                if _phase1_next_url:
                    logger.info(
                        "[OrdersSync] background_sync_triggered total_pages_estimate=%s brand=%s resume_url=%s",
                        total_pages_estimate,
                        _resolved_brand_id,
                        _phase1_next_url,
                    )
                    background_sync_in_progress = True
                    sync_metadata["background_sync_in_progress"] = True

                    # Mark credential as sync_in_progress
                    try:
                        async with AsyncSessionLocal() as _prog_session:
                            async with _prog_session.begin():
                                from sqlalchemy import select as _sa_select
                                _cred_row = (await _prog_session.execute(
                                    _sa_select(BrandAPICredential).where(BrandAPICredential.id == cred.id)
                                )).scalar_one_or_none()
                                if _cred_row:
                                    _cred_row.sync_in_progress = True
                                    _cred_row.last_synced_page = pages_fetched
                                    if total_pages_estimate:
                                        _cred_row.total_pages_estimate = total_pages_estimate
                    except Exception as _prog_exc:
                        logger.warning(
                            "[OrdersSync] background_sync_flag_failed brand=%s error=%s",
                            _resolved_brand_id,
                            _prog_exc,
                        )

                    # Fire-and-forget: schedule background task on the event loop
                    asyncio.ensure_future(
                        _background_sync_remaining_pages(
                            api_key=api_key,
                            company_id=company_id,
                            brand_id=_resolved_brand_id,
                            cred_id=cred.id,
                            resume_url=_phase1_next_url,
                            pages_already_fetched=pages_fetched,
                            total_pages_estimate=total_pages_estimate,
                        )
                    )
                else:
                    # All pages fetched in Phase 1 — no background sync needed
                    sync_metadata["background_sync_in_progress"] = False
                    logger.info(
                        "[OrdersSync] all_pages_fetched_in_phase1 brand=%s pages=%s",
                        _resolved_brand_id,
                        pages_fetched,
                    )

                # Watchdog: emit sync_completed (Phase 1 done; background may continue)
                if sync_result.get("ok"):
                    await emit_watchdog_event(
                        event_type="sync_completed",
                        brand_id=_resolved_brand_id,
                        sync_metadata={
                            "total_fetched_from_leaflink": sync_result.get("orders_fetched", 0),
                            "total_in_database": sync_result.get("created", 0) + sync_result.get("updated", 0),
                            "percent_complete": 100.0 if not _phase1_next_url else round(pages_fetched / (total_pages_estimate or pages_fetched) * 100, 1),
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
                        brand_id=_resolved_brand_id,
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

                logger.info("[OrdersSync] phase=A success brand=%s", _resolved_brand_id)

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
            logger.info("[OrdersSync] db_count_after=%s", total_in_database)

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
            sync_metadata.setdefault("background_sync_in_progress", background_sync_in_progress)
            sync_metadata.setdefault("total_pages_estimate", total_pages_estimate)
            sync_metadata.setdefault("pages_fetched_so_far", pages_fetched_so_far)
            sync_metadata["total_in_database"] = total_in_database
            sync_metadata["total_returned"] = len(orders_data)

            logger.info("[OrdersSync] db_total_orders=%s brand=%s", total_in_database, brand_filter)
            logger.info("[OrdersSync] returned_to_ios=%s brand=%s", len(orders_data), brand_filter)
            if sync_metadata.get("latest_order_date"):
                logger.info("[OrdersSync] latest_order_date=%s brand=%s", sync_metadata["latest_order_date"], brand_filter)

            logger.info(
                "[OrdersSync] response count=%s has_more=%s brand=%s total_in_db=%s background_sync=%s",
                len(orders_data),
                has_more,
                brand_filter,
                total_in_database,
                background_sync_in_progress,
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
                "background_sync_in_progress": background_sync_in_progress,
                "total_pages_estimate": total_pages_estimate,
                "pages_fetched_so_far": pages_fetched_so_far,
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
                "background_sync_in_progress": False,
                "total_pages_estimate": None,
                "pages_fetched_so_far": 0,
                "sync_metadata": sync_metadata,
                "last_error": str(phase_b_exc),
            })
