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
from models import BrandAPICredential, Order, SyncRequest
from services.watchdog_client import emit_watchdog_event
from utils.json_utils import make_json_safe

logger = logging.getLogger("orders")

router = APIRouter(prefix="/orders", tags=["orders"])


async def _get_brand_order_count(
    db: AsyncSession,
    brand_id: str,
) -> int:
    """Get total order count for a brand. Used by all endpoints."""
    try:
        result = await db.execute(
            select(func.count(Order.id)).where(Order.brand_id == brand_id)
        )
        return result.scalar_one() or 0
    except Exception as exc:
        logger.error("[OrdersAPI] order_count_error brand=%s error=%s", brand_id, exc)
        await db.rollback()
        return 0


async def _safe_db_query(
    db: AsyncSession,
    query,
    fallback_value=None,
    error_context: str = "",
):
    """
    Execute a query safely with automatic rollback on failure.

    Returns (success, result) tuple.
    """
    try:
        result = await db.execute(query)
        return True, result
    except Exception as exc:
        # Rollback the failed transaction so the session is usable again
        await db.rollback()
        logger.error(
            "[OrdersSync] safe_query_error context=%s error=%s",
            error_context,
            exc,
        )
        return False, fallback_value

async def _load_leaflink_credential(
    db: AsyncSession,
    brand_filter: Optional[str] = None,
) -> Optional["BrandAPICredential"]:
    """
    Load LeafLink credential with STRICT priority.

    PRIORITY (in order):
    1. Query DB for exact brand match FIRST
       → If found, use it and IGNORE ENV completely
    2. Only check ENV if NO DB credential exists

    NEVER:
    - Validate ENV before checking DB
    - Fail on ENV validation if DB credential exists
    - Use ENV if DB credential exists
    """

    logger.info("[CredentialResolver] requested_brand=%s", brand_filter)

    # PRIORITY 1: Query DB for exact brand match FIRST
    if brand_filter:
        logger.info("[CredentialResolver] db_lookup_start")

        try:
            result = await db.execute(
                select(BrandAPICredential).where(
                    BrandAPICredential.brand_id == brand_filter,
                    BrandAPICredential.integration_name == "leaflink",
                    BrandAPICredential.is_active == True,
                )
            )
            cred = result.scalar_one_or_none()

            if cred:
                # Found DB credential — use it and IGNORE ENV
                api_key = (cred.api_key or "").strip()

                logger.info(
                    "[CredentialResolver] db_lookup_found=true key_len=%s company_id=%s",
                    len(api_key),
                    cred.company_id,
                )

                # Validate DB key length
                if len(api_key) > 50:
                    logger.error(
                        "[CredentialResolver] db_key_invalid_length len=%s — REJECTING",
                        len(api_key),
                    )
                    raise ValueError(f"Invalid LeafLink API key length in DB: {len(api_key)}")

                # Strip whitespace
                cred.api_key = api_key
                cred.company_id = (cred.company_id or "").strip()

                # Log that ENV is ignored
                logger.info(
                    "[CredentialResolver] env_ignored=true reason=db_credential_exists"
                )
                logger.info("[CredentialResolver] final_source=db")

                return cred
            else:
                logger.info("[CredentialResolver] db_lookup_found=false")

        except ValueError as val_exc:
            # Re-raise validation errors
            logger.error(
                "[CredentialResolver] db_validation_error error=%s",
                val_exc,
            )
            raise

        except Exception as exc:
            logger.error(
                "[CredentialResolver] db_lookup_error error=%s",
                exc,
                exc_info=True,
            )
            await db.rollback()

    # PRIORITY 2: Check if ANY DB credential exists
    # If yes, do NOT use ENV
    try:
        result = await db.execute(
            select(BrandAPICredential).where(
                BrandAPICredential.integration_name == "leaflink",
                BrandAPICredential.is_active == True,
            ).limit(1)
        )
        any_db_cred = result.scalar_one_or_none()

        if any_db_cred:
            # DB credentials exist — do NOT use ENV
            logger.warning(
                "[CredentialResolver] db_credentials_exist — ignoring ENV vars"
            )
            return None

    except Exception as exc:
        logger.error(
            "[CredentialResolver] db_check_error error=%s",
            exc,
        )
        await db.rollback()

    # PRIORITY 3: ENV fallback (only if NO DB credentials exist)
    logger.info("[CredentialResolver] checking_env_fallback")

    env_api_key = os.getenv("LEAFLINK_API_KEY", "").strip()
    env_company_id = os.getenv("LEAFLINK_COMPANY_ID", "").strip()

    if env_api_key and env_company_id:
        # Validate ENV key length
        if len(env_api_key) > 50:
            logger.error(
                "[CredentialResolver] env_key_invalid_length len=%s — REJECTING",
                len(env_api_key),
            )
            raise ValueError(f"Invalid LeafLink API key length in ENV: {len(env_api_key)}")

        logger.info(
            "[CredentialResolver] env_fallback_used key_len=%s company_id=%s",
            len(env_api_key),
            env_company_id,
        )
        logger.info("[CredentialResolver] final_source=env")

        # Create temporary credential from ENV
        temp_cred = BrandAPICredential(
            brand_id=brand_filter or "default",
            integration_name="leaflink",
            api_key=env_api_key,
            company_id=env_company_id,
            is_active=True,
        )
        return temp_cred
    else:
        logger.warning(
            "[CredentialResolver] no_credentials_found — no DB, no ENV"
        )
        return None


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
    include_line_items: bool = Query(False, description="Include full line_items (default false for list mode)"),
    db: AsyncSession = Depends(get_db),
):
    """
    Fetch paginated orders for a brand.

    By default returns order headers only (lightweight list mode).
    Use include_line_items=true to get full line_items (detail mode).

    Supports two pagination modes:
    1. Limit/Offset: Use limit and offset parameters
    2. Cursor-based: Use cursor parameter (more efficient for large datasets)

    Response includes next_cursor / prev_cursor for navigating pages.
    """
    from leaflink.orders import serialize_order

    logger.info(
        "[OrdersAPI] request brand=%s limit=%s offset=%s include_line_items=%s",
        brand,
        limit,
        offset,
        include_line_items,
    )

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
    # Total count for the brand (shared helper)                            #
    # ------------------------------------------------------------------ #
    total_in_database = await _get_brand_order_count(db, brand)

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
    # Serialize — list mode (headers only) or detail mode (with           #
    # line_items) depending on include_line_items query param             #
    # ------------------------------------------------------------------ #
    if include_line_items:
        # Detail mode: full serialization with line_items
        serialized_orders = [serialize_order(o) for o in orders]
    else:
        # List mode: headers only, no line_items (lightweight)
        serialized_orders = [
            {
                "id": o.id,
                "external_id": o.external_order_id,
                "order_number": o.order_number,
                "customer_name": o.customer_name,
                "status": o.status,
                "amount": float(o.amount) if o.amount else 0,
                "item_count": o.item_count,
                "unit_count": o.unit_count,
                "review_status": o.review_status or "ok",
                "sync_status": o.sync_status or "ok",
                "last_synced_at": o.last_synced_at or o.synced_at,
                "source": o.source,
                "external_created_at": o.external_created_at,
                "external_updated_at": o.external_updated_at,
                "created_at": o.created_at,
                "updated_at": o.updated_at,
            }
            for o in orders
        ]

    logger.info(
        "[OrdersAPI] pagination brand=%s limit=%s offset=%s returned=%s total=%s has_more=%s include_line_items=%s",
        brand,
        limit,
        offset,
        len(serialized_orders),
        total_in_database,
        has_more,
        include_line_items,
    )

    return make_json_safe({
        "ok": True,
        "brand_id": brand,
        "orders": serialized_orders,
        "returned_count": len(serialized_orders),
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
    brand: Optional[str] = Query(None, description="Brand slug or ID (e.g., 'noble-nectar') — primary alias"),
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



    # Resolve brand filter — accept brand, brand_id, brand_slug, or camelCase brandId
    brand_filter: Optional[str] = (
        brand
        or brand_id
        or brand_slug
        or request.query_params.get("brandId")
    )

    logger.info(
        "[OrdersSync] request_received brand=%s force_full=%s pages_per_request=%s",
        brand_filter,
        force_full,
        pages_per_request,
    )
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
    # Load the active LeafLink credential — DB FIRST, ENV only fallback  #
    # ------------------------------------------------------------------ #
    _resolved_brand_id: Optional[str] = None
    _resolved_company_id: Optional[str] = None
    _active_cred = None
    _credential_found: bool = False

    # After loading credential
    try:
        _active_cred = await _load_leaflink_credential(db, brand_filter)
    except ValueError as _val_exc:
        logger.error(
            "[CredentialResolver] sync_credential_error error=%s",
            _val_exc,
        )
        return {
            "ok": False,
            "error": str(_val_exc),
            "brand_id": brand_filter,
            "credential_found": False,
            "sync_triggered": False,
            "worker_enqueued": False,
            "leaflink_call_attempted": False,
        }

    except Exception as _cred_exc:
        await db.rollback()
        logger.error(
            "[CredentialResolver] sync_credential_load_error brand=%s error=%s",
            brand_filter,
            _cred_exc,
            exc_info=True,
        )

    if _active_cred:
        _credential_found = True
        _resolved_brand_id = _active_cred.brand_id
        _resolved_company_id = _active_cred.company_id

        logger.info(
            "[CredentialResolver] sync_credential_loaded brand=%s key_len=%s company_id=%s",
            _resolved_brand_id,
            len((_active_cred.api_key or "").strip()),
            _resolved_company_id,
        )

    if not _credential_found:
        logger.error(
            "[CredentialResolver] sync_no_credentials_found brand=%s — cannot start sync",
            brand_filter,
        )
        return {
            "ok": False,
            "error": f"No active LeafLink credentials found for brand {brand_filter}",
            "error_detail": "Check that credentials are configured and active in the database or environment",
            "brand_id": brand_filter,
            "credential_found": False,
            "sync_triggered": False,
            "worker_enqueued": False,
            "leaflink_call_attempted": False,
            "total_orders_in_db": 0,
        }

    # Use resolved brand ID for all subsequent operations
    _effective_brand_id: Optional[str] = _resolved_brand_id



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
    _sync_triggered: bool = False
    _worker_enqueued: bool = False
    _leaflink_call_attempted: bool = False

    # ------------------------------------------------------------------ #
    # Count existing orders in DB before sync (shared helper)            #
    # ------------------------------------------------------------------ #
    _db_count_before = await _get_brand_order_count(db, _effective_brand_id)

    logger.info(
        "[OrdersSync] db_count_before brand=%s count=%s",
        _effective_brand_id,
        _db_count_before,
    )

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

                # Use stored auth scheme if available, otherwise default to Bearer
                if _active_cred and _active_cred.auth_scheme:
                    _auth_scheme = _active_cred.auth_scheme
                    logger.info(
                        "[LeafLinkAuth] using_stored_scheme scheme=%s brand=%s",
                        _auth_scheme,
                        _resolved_brand_id,
                    )
                else:
                    _auth_scheme = "Bearer"
                    logger.info(
                        "[LeafLinkAuth] using_default_scheme scheme=Bearer brand=%s",
                        _resolved_brand_id,
                    )

                try:
                    client = LeafLinkClient(
                        api_key=api_key,
                        company_id=company_id,
                        brand_id=_resolved_brand_id,
                        auth_scheme=_auth_scheme,
                    )
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

                logger.info(
                    "[OrdersSync] leaflink_call_start brand=%s start_page=%s force_full=%s",
                    _resolved_brand_id,
                    start_page,
                    force_full,
                )
                _leaflink_call_attempted = True


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
                    "[OrdersSync] leaflink_call_complete brand=%s orders=%s pages=%s total_pages=%s total_count=%s",
                    _resolved_brand_id,
                    len(orders_from_leaflink),
                    pages_fetched_phase1,
                    total_pages_available,
                    total_count_leaflink,
                )
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

                _sync_triggered = True
                logger.info(
                    "[OrdersSync] phase1_upsert_complete brand=%s upserted=%s",
                    _resolved_brand_id,
                    len(orders_from_leaflink),
                )


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
                                # Only set if column exists and value is not None — never overwrite with NULL
                                if total_count_leaflink is not None:
                                    try:
                                        _p1_cred.total_orders_available = total_count_leaflink
                                        logger.info(
                                            "[OrdersSync] persisted_total_orders_available brand=%s count=%s",
                                            _resolved_brand_id,
                                            total_count_leaflink,
                                        )
                                    except Exception:
                                        logger.debug("[OrdersSync] total_orders_available column not yet in schema")
                                _p1_cred.sync_status = "syncing" if (next_leaflink_url and next_page_number) else "idle"
                                _p1_cred.last_sync_at = datetime.now(timezone.utc)
                                _p1_cred.last_error = None
                    logger.info(
                        "[OrdersSync] phase1_progress_persisted brand=%s page=%s total_pages=%s total_orders=%s",
                        _resolved_brand_id,
                        _phase1_page,
                        total_pages_available,
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
                        "[OrdersSync] phase2_before_enqueue brand=%s start_page=%s total_pages=%s pages_remaining=%s",
                        _resolved_brand_id,
                        next_page_number,
                        total_pages_available,
                        pages_remaining,
                    )

                    try:
                        async with AsyncSessionLocal() as _enqueue_sess:
                            async with _enqueue_sess.begin():
                                _enqueue_sess.add(SyncRequest(
                                    brand_id=_resolved_brand_id,
                                    status="pending",
                                    start_page=next_page_number,
                                    total_pages=total_pages_available,
                                    total_orders_available=total_count_leaflink,
                                ))
                        logger.info(
                            "[OrdersSync] enqueued_background_sync brand=%s start_page=%s total_pages=%s",
                            _resolved_brand_id,
                            next_page_number,
                            total_pages_available,
                        )
                        sync_metadata["background_sync_active"] = True
                        _worker_enqueued = True
                        logger.info(
                            "[OrdersSync] worker_enqueued brand=%s start_page=%s total_pages=%s",
                            _resolved_brand_id,
                            next_page_number,
                            total_pages_available,
                        )

                    except Exception as enqueue_exc:
                        logger.error(
                            "[OrdersSync] enqueue_error brand=%s error=%s",
                            _resolved_brand_id,
                            enqueue_exc,
                        )
                        sync_metadata["background_sync_active"] = False
                        sync_metadata["background_sync_error"] = str(enqueue_exc)
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
                    "[OrdersSync] sync_trigger_mode brand=%s mode=worker sync_triggered=%s worker_enqueued=%s leaflink_attempted=%s",
                    _resolved_brand_id,
                    str(_sync_triggered).lower(),
                    str(_worker_enqueued).lower(),
                    str(_leaflink_call_attempted).lower(),
                )
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
    # Count total orders in DB after sync (shared helper)                #
    # ------------------------------------------------------------------ #
    total_in_database = await _get_brand_order_count(db, _effective_brand_id)

    logger.info(
        "[OrdersSync] db_count_after brand=%s count=%s",
        _effective_brand_id,
        total_in_database,
    )

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
        "[OrdersSync] sync_trigger_mode brand=%s mode=worker sync_triggered=%s worker_enqueued=%s leaflink_attempted=%s",
        _effective_brand_id,
        str(_sync_triggered).lower(),
        str(_worker_enqueued).lower(),
        str(_leaflink_call_attempted).lower(),
    )

    logger.info(
        "[OrdersSync] response brand=%s sync_status=%s total_orders_in_db=%s total_orders_available=%s sync_triggered=%s worker_enqueued=%s",
        _effective_brand_id,
        _sync_status,
        total_in_database,
        _total_orders_available,
        str(_sync_triggered).lower(),
        str(_worker_enqueued).lower(),
    )

    return make_json_safe({
        "ok": True,
        "sync_status": _sync_status,
        "background_sync_active": sync_metadata.get("background_sync_active", False),
        "sync_triggered": _sync_triggered,
        "worker_enqueued": _worker_enqueued,
        "leaflink_call_attempted": _leaflink_call_attempted,
        "credential_found": _credential_found,
        "brand_id": _effective_brand_id,
        "company_id": _resolved_company_id,
        "pages_synced": sync_metadata.get("pages_fetched", 0),
        "total_pages": total_pages_available,
        "total_orders_in_db": total_in_database,
        "total_orders_available": _total_orders_available,
        "percent_complete": _percent_complete,
        "next_cursor": next_cursor,
        "error": sync_error,
    })


@router.get("/sync/debug-credentials")
async def debug_credentials(
    brand: Optional[str] = Query(None, description="Brand slug/ID to debug"),
    db: AsyncSession = Depends(get_db),
):
    """
    Debug endpoint to diagnose credential resolution.

    Shows which credential lookup paths succeed/fail.
    """
    logger.info("[OrdersSyncCredential] debug_request brand=%s", brand)

    exact_brand_found = False
    exact_brand_cred = None

    active_leaflink_found = False
    active_leaflink_cred = None

    leaflink_debug_found = False
    leaflink_debug_cred = None

    # Try exact brand lookup
    if brand:
        try:
            result = await db.execute(
                select(BrandAPICredential).where(
                    BrandAPICredential.brand_id == brand,
                    BrandAPICredential.integration_name == "leaflink",
                    BrandAPICredential.is_active == True,
                )
            )
            exact_brand_cred = result.scalar_one_or_none()
            exact_brand_found = exact_brand_cred is not None
        except Exception as exc:
            logger.error("[OrdersSyncCredential] debug_exact_brand_error error=%s", exc)
            await db.rollback()

    # Try active LeafLink lookup
    try:
        result = await db.execute(
            select(BrandAPICredential).where(
                BrandAPICredential.integration_name == "leaflink",
                BrandAPICredential.is_active == True,
            ).order_by(BrandAPICredential.last_sync_at.desc().nullslast()).limit(1)
        )
        active_leaflink_cred = result.scalar_one_or_none()
        active_leaflink_found = active_leaflink_cred is not None
    except Exception as exc:
        logger.error("[OrdersSyncCredential] debug_active_leaflink_error error=%s", exc)
        await db.rollback()

    # Try /leaflink/debug style lookup (no is_active filter)
    try:
        result = await db.execute(
            select(BrandAPICredential).where(
                BrandAPICredential.integration_name == "leaflink",
            ).order_by(BrandAPICredential.last_sync_at.desc().nullslast()).limit(1)
        )
        leaflink_debug_cred = result.scalar_one_or_none()
        leaflink_debug_found = leaflink_debug_cred is not None
    except Exception as exc:
        logger.error("[OrdersSyncCredential] debug_leaflink_debug_error error=%s", exc)
        await db.rollback()

    # Determine which credential would be used
    final_cred = exact_brand_cred or active_leaflink_cred or leaflink_debug_cred

    return {
        "ok": True,
        "requested_brand": brand,
        "exact_brand_found": exact_brand_found,
        "exact_brand_cred": {
            "brand_id": exact_brand_cred.brand_id,
            "company_id": exact_brand_cred.company_id,
            "api_key_present": bool(exact_brand_cred.api_key),
        } if exact_brand_cred else None,
        "active_leaflink_found": active_leaflink_found,
        "active_leaflink_cred": {
            "brand_id": active_leaflink_cred.brand_id,
            "company_id": active_leaflink_cred.company_id,
            "api_key_present": bool(active_leaflink_cred.api_key),
        } if active_leaflink_cred else None,
        "leaflink_debug_found": leaflink_debug_found,
        "leaflink_debug_cred": {
            "brand_id": leaflink_debug_cred.brand_id,
            "company_id": leaflink_debug_cred.company_id,
            "api_key_present": bool(leaflink_debug_cred.api_key),
        } if leaflink_debug_cred else None,
        "final_credential": {
            "brand_id": final_cred.brand_id,
            "company_id": final_cred.company_id,
            "api_key_present": bool(final_cred.api_key),
        } if final_cred else None,
    }


@router.get("/sync/status")
async def orders_sync_status(
    brand: Optional[str] = Query(None, description="Brand slug to check (e.g., 'noble-nectar')"),
    db: AsyncSession = Depends(get_db),
):
    """

    Return the current background sync progress for a brand.

    Combines sync_requests queue state (pending/processing rows) with
    persisted progress from BrandAPICredential and a live order count from DB.

    Response fields:
      ok                           — always true unless brand is missing
      brand_id                     — echoed brand slug
      background_sync_active       — true if a worker request is pending/processing
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
        # CRITICAL: Rollback the failed transaction so the session is not poisoned
        await db.rollback()

        logger.warning(
            "[OrdersSync] credential_load_error brand=%s error=%s — trying fallback",
            brand,
            cred_exc,
        )

        # Fallback: use a FRESH session to avoid the poisoned transaction state.
        # Query without total_orders_available using raw SQL to avoid ORM selecting
        # a column that may not yet exist in the schema.
        try:
            async with AsyncSessionLocal() as fallback_db:
                from sqlalchemy import text
                _fb_result = await fallback_db.execute(
                    text("""
                        SELECT id, brand_id, integration_name, api_key, company_id,
                               is_active, sync_status, last_sync_at, last_error,
                               last_synced_page, total_pages_available
                        FROM brand_api_credentials
                        WHERE brand_id = :brand AND integration_name = 'leaflink'
                        LIMIT 1
                    """),
                    {"brand": brand},
                )
                _fb_row = _fb_result.fetchone()
                if _fb_row:
                    cred = BrandAPICredential(
                        id=_fb_row[0],
                        brand_id=_fb_row[1],
                        integration_name=_fb_row[2],
                        api_key=_fb_row[3],
                        company_id=_fb_row[4],
                        is_active=_fb_row[5],
                        sync_status=_fb_row[6],
                        last_sync_at=_fb_row[7],
                        last_error=_fb_row[8],
                        last_synced_page=_fb_row[9],
                        total_pages_available=_fb_row[10],
                        total_orders_available=None,  # Column doesn't exist yet
                    )
                else:
                    cred = None
        except Exception as fallback_exc:
            error_type = type(fallback_exc).__name__
            error_msg = str(fallback_exc)
            tb = traceback.format_exc()
            logger.error(
                "[OrdersSync] credential_fallback_error brand=%s error=%s",
                brand,
                fallback_exc,
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
    # Count orders in DB for this brand (shared helper)                  #
    # ------------------------------------------------------------------ #
    total_orders_in_db = await _get_brand_order_count(db, brand)

    logger.info(
        "[OrdersSyncStatus] db_count brand=%s count=%s",
        brand,
        total_orders_in_db,
    )

    # ------------------------------------------------------------------ #
    # Check sync_requests queue for active worker activity                #
    # ------------------------------------------------------------------ #
    try:
        queue_result = await db.execute(
            select(SyncRequest)
            .where(
                SyncRequest.brand_id == brand,
                SyncRequest.status.in_(["pending", "processing"]),
            )
            .order_by(SyncRequest.created_at.desc())
            .limit(1)
        )
        active_queue_request = queue_result.scalar_one_or_none()
    except Exception as queue_exc:
        logger.error("[OrdersSync] status_queue_check_error brand=%s error=%s", brand, queue_exc)
        active_queue_request = None

    bg_active = active_queue_request is not None

    if cred is not None:
        db_pages_synced = cred.last_synced_page or 0
        db_total_pages = cred.total_pages_available or 0
        db_sync_status = cred.sync_status or "idle"
        db_last_synced_at = cred.last_sync_at.isoformat() if cred.last_sync_at else None
        db_last_error = cred.last_error
        # Load total_orders_available safely — column may not exist yet in schema
        try:
            total_orders_available = cred.total_orders_available
        except AttributeError:
            total_orders_available = None
    else:
        db_pages_synced = 0
        db_total_pages = 0
        db_sync_status = "idle"
        db_last_synced_at = None
        db_last_error = None
        total_orders_available = None

    # In-memory state is more current than DB when a sync is active
    db_pages_synced_raw = db_pages_synced  # preserve original stored value for logging
    pages_synced = db_pages_synced
    total_pages = db_total_pages

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
    # Distinguish between actual (from LeafLink API) and estimated        #
    # (inferred from page count) totals.                                  #
    # ------------------------------------------------------------------ #
    effective_sync_status = None

    # Distinguish between actual and estimated
    total_orders_estimated: Optional[int] = None  # Fallback estimate from page count

    if cred and total_orders_available:
        # total_orders_available is the actual count from LeafLink API
        pass
    elif total_pages and total_pages > 0:
        # Estimate: assume 100 orders per page
        total_orders_estimated = total_pages * 100

    # Use actual if available, else estimated
    _total_for_percent = total_orders_available or total_orders_estimated

    if _total_for_percent and _total_for_percent > 0:
        percent_complete = round((total_orders_in_db / _total_for_percent) * 100, 1)
        percent_complete = min(100.0, percent_complete)
    else:
        percent_complete = 0.0

    logger.info(
        "[OrdersSyncStatus] progress brand=%s orders=%s/%s estimated=%s percent=%.1f%%",
        brand,
        total_orders_in_db,
        total_orders_available or total_orders_estimated,
        total_orders_estimated is not None,
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

    # Reconcile sync_status: handle partial syncs gracefully
    if total_orders_in_db > 0 and not bg_active:
        # We have orders but sync is not active
        if effective_sync_status == "complete":
            # Already detected as complete above — keep it
            pass
        elif db_sync_status == "paused":
            # Sync was paused (transient error)
            effective_sync_status = "paused"
        elif db_sync_status == "error":
            # Sync had permanent error
            effective_sync_status = "error"
        elif effective_sync_status is None:
            # Partial sync, not active, no error — infer paused
            effective_sync_status = "paused"
            logger.info(
                "[OrdersSyncStatus] inferred_paused brand=%s orders=%s/%s",
                brand,
                total_orders_in_db,
                total_orders_available,
            )
    elif db_sync_status == "syncing" and not bg_active:
        # DB says syncing but queue says inactive — trust DB
        effective_sync_status = "syncing"
        bg_active = True
    else:
        # Use effective_sync_status from above (may be "complete" if all orders/pages synced)
        if effective_sync_status is None:
            effective_sync_status = "syncing" if bg_active else db_sync_status

    # Estimate completion: 4 pages per batch, ~10 seconds per batch → 0.4 pages/s
    pages_remaining = max(total_pages - displayed_pages_synced, 0)
    estimated_completion_minutes: Optional[float] = None
    if effective_sync_status == "paused":
        # Paused due to transient error — retry time is unknown
        logger.info(
            "[OrdersSync] status_paused brand=%s error=%s",
            brand,
            db_last_error,
        )
        estimated_completion_minutes = None
    elif bg_active and pages_remaining > 0:
        pages_per_second = 0.4
        estimated_completion_minutes = round(pages_remaining / pages_per_second / 60, 1)

    errors: list[str] = []
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
        "total_orders_estimated": total_orders_estimated,
        "percent_complete": percent_complete,
        "last_synced_at": db_last_synced_at,
        "sync_status": effective_sync_status,
        "last_error": db_last_error,
        "estimated_completion_minutes": estimated_completion_minutes,
        "errors": errors,
    })
