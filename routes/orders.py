import asyncio
import base64
import logging
import os
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Header, Query, Request
from sqlalchemy import func, literal_column, select
from sqlalchemy.ext.asyncio import AsyncSession

from database import AsyncSessionLocal, get_db
from models import BrandAPICredential, Order, SyncRequest, SyncRun
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

async def resolve_leaflink_credential(
    db: AsyncSession,
    brand_id: Optional[str] = None,
    allow_env_fallback: bool = False,
) -> Optional["BrandAPICredential"]:
    """
    Resolve LeafLink credential with tenant isolation.

    Rules:
    - If brand_id is provided: DB exact tenant only, no ENV override
    - ENV fallback only if allow_env_fallback=true AND brand_id is empty
    - Never use another tenant's credential
    - Never override tenant credential with ENV

    Args:
        db: AsyncSession
        brand_id: Tenant brand ID (e.g., 'noble-nectar'). If provided, DB-only.
        allow_env_fallback: Allow ENV fallback only if brand_id is empty.

    Returns:
        BrandAPICredential or None
    """

    logger.info("[CredentialResolver] tenant_lookup_start brand=%s", brand_id)

    # TENANT-SCOPED REQUEST: DB only, no ENV
    if brand_id:
        logger.info("[CredentialResolver] env_checked=false reason=brand_scoped_tenant_request")

        try:
            result = await db.execute(
                select(BrandAPICredential).where(
                    BrandAPICredential.brand_id == brand_id,
                    BrandAPICredential.integration_name == "leaflink",
                    BrandAPICredential.is_active == True,
                ).order_by(BrandAPICredential.updated_at.desc().nullslast()).limit(1)
            )
            cred = result.scalar_one_or_none()

            if cred:
                api_key = (cred.api_key or "").strip()

                logger.info(
                    "[CredentialResolver] tenant_lookup_found=true credential_id=%s key_len=%s company_id=%s",
                    cred.id,
                    len(api_key),
                    cred.company_id,
                )

                # Validate key length
                if len(api_key) > 50 or len(api_key) < 20:
                    logger.error(
                        "[CredentialResolver] tenant_key_invalid_length len=%s — REJECTING",
                        len(api_key),
                    )
                    raise ValueError(f"Invalid LeafLink API key length: {len(api_key)}")

                # Strip whitespace
                cred.api_key = api_key
                cred.company_id = (cred.company_id or "").strip()

                logger.info("[CredentialResolver] final_source=db tenant_isolated=true")

                return cred
            else:
                logger.error(
                    "[CredentialResolver] tenant_lookup_found=false brand=%s",
                    brand_id,
                )
                return None

        except ValueError as val_exc:
            logger.error(
                "[CredentialResolver] tenant_validation_error error=%s",
                val_exc,
            )
            raise

        except Exception as exc:
            logger.error(
                "[CredentialResolver] tenant_lookup_error brand=%s error=%s",
                brand_id,
                exc,
                exc_info=True,
            )
            await db.rollback()
            return None

    # NON-TENANT REQUEST: ENV fallback allowed only if explicitly enabled
    if allow_env_fallback:
        logger.info("[CredentialResolver] env_fallback_allowed")

        env_api_key = os.getenv("LEAFLINK_API_KEY", "").strip()
        env_company_id = os.getenv("LEAFLINK_COMPANY_ID", "").strip()

        if env_api_key and env_company_id:
            if len(env_api_key) > 50 or len(env_api_key) < 20:
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

            temp_cred = BrandAPICredential(
                brand_id="default",
                integration_name="leaflink",
                api_key=env_api_key,
                company_id=env_company_id,
                is_active=True,
            )
            return temp_cred

    logger.warning("[CredentialResolver] no_credential_found")
    return None


async def _load_leaflink_credential(
    db: AsyncSession,
    brand_filter: Optional[str] = None,
) -> Optional["BrandAPICredential"]:
    """
    Deprecated: use resolve_leaflink_credential() instead.

    Thin wrapper kept for backward compatibility with any callers that have
    not yet been migrated.
    """
    return await resolve_leaflink_credential(db, brand_id=brand_filter, allow_env_fallback=False)


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
    x_opsyn_org: str | None = Header(None, description="Organization ID (UUID or org_code)"),
    brand_id: str = Query(..., description="Brand ID (UUID)"),
    limit: int = Query(100, ge=1, le=1000, description="Orders per page (default 100, max 1000)"),
    offset: int = Query(0, ge=0, description="Offset for pagination (default 0)"),
    cursor: Optional[str] = Query(None, description="Opaque cursor for cursor-based pagination"),
    sort_by: str = Query("updated_at", description="Sort field: updated_at, created_at, external_updated_at"),
    sort_order: str = Query("desc", description="asc or desc"),
    include_line_items: bool = Query(False, description="Include full line_items (default false for list mode)"),
    db: AsyncSession = Depends(get_db),
):
    """
    Fetch paginated orders for a brand from AWS/RDS database.

    Does NOT require LeafLink credentials - reads existing orders from database.
    LeafLink credentials only needed for /sync endpoints to import new orders.

    Requires:
    - X-OPSYN-ORG header: Organization ID (UUID or org_code like "noble")
    - brand_id query param: Brand ID (UUID)

    Supports two pagination modes:
    1. Limit/Offset: Use limit and offset parameters
    2. Cursor-based: Use cursor parameter (more efficient for large datasets)

    Response includes routing fields:
    - order id, customer name, address, city, state, zip
    - status, payment_status, total, item_count, unit_count
    - delivery_date if available
    """
    from leaflink.orders import serialize_order
    from services.organization_service import lookup_organization
    from sqlalchemy import and_

    # Validate required headers/params
    if not x_opsyn_org:
        logger.warning("[OrdersAPI] missing_org_id")
        return {"ok": False, "error": "X-OPSYN-ORG header is required"}

    if not brand_id:
        logger.warning("[OrdersAPI] missing_brand_id")
        return {"ok": False, "error": "brand_id query parameter is required"}

    logger.info(
        "[OrdersAPI] request org_id=%s brand_id=%s limit=%s offset=%s include_line_items=%s",
        x_opsyn_org,
        brand_id,
        limit,
        offset,
        include_line_items,
    )

    # Resolve org_id (supports UUID or org_code)
    org_lookup = await lookup_organization(db, x_opsyn_org)
    if not org_lookup.get("ok"):
        logger.warning("[OrdersAPI] org_lookup_failed error=%s", org_lookup.get("error"))
        return {"ok": False, "error": org_lookup.get("error")}

    resolved_org = org_lookup.get("organization")
    resolved_org_id = str(resolved_org.id)

    # Validate brand exists and belongs to org
    from models.auth_models import Brand
    brand_result = await db.execute(
        select(Brand).where(
            and_(Brand.id == brand_id, Brand.org_id == resolved_org_id)
        )
    )
    brand = brand_result.scalar_one_or_none()

    if not brand:
        logger.warning("[OrdersAPI] brand_not_found brand_id=%s org_id=%s", brand_id, resolved_org_id)
        return {"ok": False, "error": "Brand not found or does not belong to organization"}

    logger.info("[OrdersAPI] org_and_brand_validated org_id=%s brand_id=%s", resolved_org_id, brand_id)
    logger.info("[OrdersAPI] leaflink_credential_check skipped reason=read_endpoint")

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
    # Build base query - SCOPED BY ORG_ID AND BRAND_ID                    #
    # ------------------------------------------------------------------ #
    from sqlalchemy.orm import selectinload as _selectinload

    query = (
        select(Order)
        .options(_selectinload(Order.lines))
        .where(
            and_(
                Order.brand_id == brand_id,
            )
        )
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
        logger.error("[OrdersAPI] db_query_failed org_id=%s brand_id=%s error=%s", resolved_org_id, brand_id, exc, exc_info=True)
        return {"ok": False, "error": "Database query failed"}

    has_more = len(orders) > limit
    if has_more:
        orders = orders[:limit]

    # ------------------------------------------------------------------ #
    # Total count for the brand (scoped by brand_id)                      #
    # ------------------------------------------------------------------ #
    try:
        count_result = await db.execute(
            select(func.count(Order.id)).where(Order.brand_id == brand_id)
        )
        total_in_database = count_result.scalar_one() or 0
    except Exception as exc:
        logger.error("[OrdersAPI] order_count_error org_id=%s brand_id=%s error=%s", resolved_org_id, brand_id, exc)
        total_in_database = 0

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
        # List mode: headers only with routing fields, no line_items
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
        "[OrdersAPI] returned org_id=%s brand_id=%s count=%s total_in_db=%s latest_order_date=%s",
        resolved_org_id,
        brand_id,
        len(serialized_orders),
        total_in_database,
        serialized_orders[0]["updated_at"] if serialized_orders else None,
    )

    logger.info(
        "[OrdersAPI] pagination org_id=%s brand_id=%s limit=%s offset=%s returned=%s total=%s has_more=%s include_line_items=%s",
        resolved_org_id,
        brand_id,
        limit,
        offset,
        len(serialized_orders),
        total_in_database,
        has_more,
        include_line_items,
    )

    return make_json_safe({
        "ok": True,
        "org_id": resolved_org_id,
        "brand_id": brand_id,
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


@router.get("/sync/diagnostic")
async def orders_sync_diagnostic(
    brand: str = Query(..., description="Brand slug/ID (e.g., 'noble-nectar')"),
    db: AsyncSession = Depends(get_db),
):
    """Read-only diagnostic for order sync issues.

    Reports:
    - Current DB order count
    - Order counts by status/review_status
    - Newest/oldest order dates
    - Last sync metadata
    - Pagination state
    - Potential issues (duplicates, missing line items, mapping blocks)
    """
    try:
        # 1. Get current order count for brand
        count_result = await db.execute(
            select(func.count(Order.id)).where(Order.brand_id == brand)
        )
        total_orders = count_result.scalar() or 0

        # 2. Count by status
        status_result = await db.execute(
            select(Order.status, func.count(Order.id))
            .where(Order.brand_id == brand)
            .group_by(Order.status)
        )
        orders_by_status = {row[0]: row[1] for row in status_result.fetchall()}

        # 3. Count by review_status
        review_result = await db.execute(
            select(Order.review_status, func.count(Order.id))
            .where(Order.brand_id == brand)
            .group_by(Order.review_status)
        )
        orders_by_review_status = {row[0]: row[1] for row in review_result.fetchall()}

        # 4. Newest and oldest order dates
        date_result = await db.execute(
            select(
                func.max(Order.external_updated_at),
                func.min(Order.external_updated_at),
                func.max(Order.synced_at),
                func.min(Order.synced_at),
            ).where(Order.brand_id == brand)
        )
        newest_ext_date, oldest_ext_date, newest_sync_date, oldest_sync_date = date_result.fetchone()

        # 5. Last sync metadata
        sync_result = await db.execute(
            select(SyncRun)
            .where(SyncRun.brand_id == brand)
            .order_by(SyncRun.started_at.desc())
            .limit(1)
        )
        last_sync = sync_result.scalar_one_or_none()

        # 6. Count orders with missing line items
        missing_lines_result = await db.execute(
            select(func.count(Order.id))
            .where(
                Order.brand_id == brand,
                (Order.line_items_json == None) | (Order.line_items_json == literal_column("'[]'::jsonb"))
            )
        )
        orders_missing_line_items = missing_lines_result.scalar() or 0

        # 7. Count orders blocked by product mapping
        blocked_result = await db.execute(
            select(func.count(Order.id))
            .where(
                Order.brand_id == brand,
                Order.review_status == "blocked"
            )
        )
        orders_blocked_mapping = blocked_result.scalar() or 0

        # 8. Check for duplicate external_order_ids
        dup_result = await db.execute(
            select(Order.external_order_id, func.count(Order.id))
            .where(Order.brand_id == brand)
            .group_by(Order.external_order_id)
            .having(func.count(Order.id) > 1)
        )
        duplicate_external_ids = {row[0]: row[1] for row in dup_result.fetchall()}

        # 9. Check pagination state from last sync
        last_cursor = last_sync.current_cursor if last_sync else None
        last_page = last_sync.current_page if last_sync else None
        pages_synced = last_sync.pages_synced if last_sync else None
        total_pages = last_sync.total_pages if last_sync else None

        # 10. Check if pagination stopped early
        pagination_stopped_early = False
        pagination_reason = None
        if last_sync and last_sync.status in ("completed", "stalled"):
            if last_sync.status == "stalled":
                pagination_stopped_early = True
                pagination_reason = last_sync.last_error or "stalled"
            elif last_sync.status == "completed" and last_cursor:
                pagination_stopped_early = True
                pagination_reason = "cursor_present_at_completion"

        # 11. Get most recently synced order as a data quality sample
        # Select only columns that exist in the production schema — do NOT
        # include sync_run_id, which is mapped in the ORM but absent from the
        # production orders table (migration not yet applied).
        sample_result = await db.execute(
            select(
                Order.id,
                Order.brand_id,
                Order.external_order_id,
                Order.status,
                Order.review_status,
                Order.synced_at,
                Order.line_items_json,
                Order.external_updated_at,
                Order.created_at,
                Order.updated_at,
            )
            .where(Order.brand_id == brand)
            .order_by(Order.synced_at.desc())
            .limit(1)
        )
        sample_row = sample_result.fetchone()

        return {
            "brand": brand,
            "diagnostic": {
                "order_counts": {
                    "total_orders": total_orders,
                    "by_status": orders_by_status,
                    "by_review_status": orders_by_review_status,
                    "missing_line_items": orders_missing_line_items,
                    "blocked_by_mapping": orders_blocked_mapping,
                },
                "order_dates": {
                    "newest_external_updated_at": newest_ext_date.isoformat() if newest_ext_date else None,
                    "oldest_external_updated_at": oldest_ext_date.isoformat() if oldest_ext_date else None,
                    "newest_synced_at": newest_sync_date.isoformat() if newest_sync_date else None,
                    "oldest_synced_at": oldest_sync_date.isoformat() if oldest_sync_date else None,
                },
                "last_sync": {
                    "sync_run_id": last_sync.id if last_sync else None,
                    "status": last_sync.status if last_sync else None,
                    "mode": last_sync.mode if last_sync else None,
                    "started_at": last_sync.started_at.isoformat() if last_sync and last_sync.started_at else None,
                    "completed_at": last_sync.completed_at.isoformat() if last_sync and last_sync.completed_at else None,
                    "last_error": last_sync.last_error if last_sync else None,
                    "pages_synced": pages_synced,
                    "total_pages": total_pages,
                    "orders_loaded_this_run": last_sync.orders_loaded_this_run if last_sync else None,
                    "last_successful_sync_at": last_sync.completed_at.isoformat() if last_sync and last_sync.completed_at else None,
                },
                "pagination_state": {
                    "last_cursor": last_cursor,
                    "last_page": last_page,
                    "stopped_early": pagination_stopped_early,
                    "stopped_reason": pagination_reason,
                },
                "data_quality": {
                    "duplicate_external_order_ids": duplicate_external_ids,
                    "duplicate_count": len(duplicate_external_ids),
                },
                "sample_order": {
                    "id": sample_row.id if sample_row else None,
                    "external_order_id": sample_row.external_order_id if sample_row else None,
                    "status": sample_row.status if sample_row else None,
                    "review_status": sample_row.review_status if sample_row else None,
                    "synced_at": sample_row.synced_at.isoformat() if sample_row and sample_row.synced_at else None,
                    "line_items_count": len(sample_row.line_items_json) if sample_row and sample_row.line_items_json else 0,
                } if sample_row else None,
            },
        }

    except Exception as exc:
        logger.error("[OrdersDiagnostic] error brand=%s error=%s", brand, str(exc)[:500], exc_info=True)
        return {
            "ok": False,
            "error": str(exc)[:500],
        }


@router.get("/api-freshness")
async def orders_api_freshness(
    brand: str = Query(..., description="Brand slug/ID (e.g., 'noble-nectar')"),
    limit: int = Query(10, ge=1, le=100, description="Number of recent orders to return"),
    db: AsyncSession = Depends(get_db),
):
    """
    Verify Orders API returns freshly synced orders for a brand.

    Checks:
    - Database order count for brand
    - API returned order count
    - Latest order date
    - Latest updated_at timestamp
    - Response includes required fields
    - No stale caching
    """
    try:
        logger.info("[OrdersAPI] freshness_check brand=%s limit=%s", brand, limit)

        # 1. Get total order count for brand
        count_result = await db.execute(
            select(func.count(Order.id)).where(Order.brand_id == brand)
        )
        db_total_orders = count_result.scalar() or 0

        # 2. Get most recent orders for the brand
        orders_result = await db.execute(
            select(Order)
            .where(Order.brand_id == brand)
            .order_by(Order.updated_at.desc())
            .limit(limit)
        )
        recent_orders = orders_result.scalars().all()

        # 3. Get date range
        date_result = await db.execute(
            select(
                func.max(Order.updated_at),
                func.min(Order.updated_at),
                func.max(Order.external_updated_at),
                func.min(Order.external_updated_at),
            ).where(Order.brand_id == brand)
        )
        latest_updated_at, oldest_updated_at, latest_external_updated_at, oldest_external_updated_at = date_result.fetchone()

        # 4. Serialize recent orders with all required fields
        serialized_orders = []
        for order in recent_orders:
            serialized_orders.append({
                "id": order.id,
                "external_id": order.external_order_id,
                "order_number": order.order_number,
                "customer_name": order.customer_name,
                "status": order.status,
                "review_status": order.review_status,
                "amount": float(order.amount) if order.amount else 0,
                "item_count": order.item_count,
                "unit_count": order.unit_count,
                "sync_status": order.sync_status,
                "source": order.source,
                "external_created_at": order.external_created_at.isoformat() if order.external_created_at else None,
                "external_updated_at": order.external_updated_at.isoformat() if order.external_updated_at else None,
                "created_at": order.created_at.isoformat() if order.created_at else None,
                "updated_at": order.updated_at.isoformat() if order.updated_at else None,
                "synced_at": order.synced_at.isoformat() if order.synced_at else None,
                "last_synced_at": order.last_synced_at.isoformat() if order.last_synced_at else None,
                "line_items_count": len(order.line_items_json) if order.line_items_json else 0,
            })

        # 5. Check for orders with missing critical fields
        missing_customer_result = await db.execute(
            select(func.count(Order.id))
            .where(Order.brand_id == brand, Order.customer_name == None)
        )
        orders_missing_customer = missing_customer_result.scalar() or 0

        missing_amount_result = await db.execute(
            select(func.count(Order.id))
            .where(Order.brand_id == brand, Order.amount == None)
        )
        orders_missing_amount = missing_amount_result.scalar() or 0

        # 6. Get sync status for this brand
        sync_result = await db.execute(
            select(SyncRun)
            .where(SyncRun.brand_id == brand)
            .order_by(SyncRun.started_at.desc())
            .limit(1)
        )
        last_sync = sync_result.scalar_one_or_none()

        logger.info(
            "[OrdersAPI] freshness_check_complete brand=%s db_total=%s api_returned=%s latest_updated=%s",
            brand,
            db_total_orders,
            len(serialized_orders),
            latest_updated_at.isoformat() if latest_updated_at else None,
        )

        return {
            "ok": True,
            "brand": brand,
            "database": {
                "total_orders": db_total_orders,
                "date_range": {
                    "latest_updated_at": latest_updated_at.isoformat() if latest_updated_at else None,
                    "oldest_updated_at": oldest_updated_at.isoformat() if oldest_updated_at else None,
                    "latest_external_updated_at": latest_external_updated_at.isoformat() if latest_external_updated_at else None,
                    "oldest_external_updated_at": oldest_external_updated_at.isoformat() if oldest_external_updated_at else None,
                },
            },
            "api": {
                "returned_count": len(serialized_orders),
                "requested_limit": limit,
                "recent_orders": serialized_orders,
            },
            "freshness": {
                "latest_order_date": serialized_orders[0]["updated_at"] if serialized_orders else None,
                "latest_external_updated_at": latest_external_updated_at.isoformat() if latest_external_updated_at else None,
                "orders_are_fresh": latest_updated_at is not None and (datetime.now(timezone.utc) - latest_updated_at).total_seconds() < 3600,  # Within 1 hour
            },
            "data_quality": {
                "orders_missing_customer": orders_missing_customer,
                "orders_missing_amount": orders_missing_amount,
            },
            "last_sync": {
                "sync_run_id": last_sync.id if last_sync else None,
                "status": last_sync.status if last_sync else None,
                "completed_at": last_sync.completed_at.isoformat() if last_sync and last_sync.completed_at else None,
                "orders_loaded_this_run": last_sync.orders_loaded_this_run if last_sync else None,
                "pages_synced": last_sync.pages_synced if last_sync else None,
            },
            "response_fields": {
                "id": "Database order ID (stable, unique)",
                "external_id": "LeafLink order ID (stable, unique)",
                "order_number": "Order number from LeafLink",
                "customer_name": "Customer/account name",
                "status": "Order status (submitted, confirmed, etc)",
                "review_status": "Review status (ready, blocked, needs_review)",
                "amount": "Order total amount",
                "item_count": "Number of line items",
                "unit_count": "Total units ordered",
                "sync_status": "Sync status (ok, error, etc)",
                "external_created_at": "Order creation date from LeafLink",
                "external_updated_at": "Order update date from LeafLink",
                "updated_at": "Last database update timestamp",
                "synced_at": "When order was synced to database",
                "line_items_count": "Number of line items in order",
            },
        }

    except Exception as exc:
        logger.error("[OrdersAPI] freshness_check_error brand=%s error=%s", brand, str(exc)[:500], exc_info=True)
        return {
            "ok": False,
            "brand": brand,
            "error": str(exc)[:500],
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

    # ================================================================
    # Load the active LeafLink credential using unified resolver
    # ================================================================
    from services.credential_resolver import resolve_brand_credential

    logger.info("[SYNC] resolver_lookup_start brand=%s", brand_filter)

    # Use shared resolver with normalization
    _cred_row = await resolve_brand_credential(db, brand_filter, "leaflink")

    if not _cred_row:
        logger.error(
            "[SYNC] resolver_credential_not_found brand=%s",
            brand_filter,
        )
        return {
            "ok": False,
            "error": "tenant_leaflink_credential_not_found",
            "brand_id": brand_filter,
            "credential_found": False,
            "credential_source": "db",
            "env_checked": False,
            "sync_triggered": False,
            "worker_enqueued": False,
            "leaflink_call_attempted": False,
        }

    # Extract credential data from resolver result
    _credential_id = _cred_row[0]
    _resolved_brand_id = _cred_row[1]
    _resolved_integration = _cred_row[2]
    _resolved_company_id = _cred_row[3]
    _api_key = _cred_row[4]
    _is_active = _cred_row[5]
    _sync_status = _cred_row[6]
    _last_synced_page = _cred_row[7]

    logger.info(
        "[SYNC] resolver_credential_found brand=%s company_id=%s credential_id=%s",
        _resolved_brand_id,
        _resolved_company_id,
        _credential_id,
    )

    # Create a minimal credential object for compatibility with existing code
    # that expects a BrandAPICredential-like object
    class _CredentialProxy:
        """Lightweight proxy for credential data from resolver tuple."""
        def __init__(self, row_data):
            self.id = row_data[0]
            self.brand_id = row_data[1]
            self.integration_name = row_data[2]
            self.company_id = row_data[3]
            self.api_key = row_data[4]
            self.is_active = row_data[5]
            self.sync_status = row_data[6]
            self.last_synced_page = row_data[7]
            # Default auth_scheme to "Token" (verified by /leaflink/auth-test)
            self.auth_scheme = "Token"
            # Nullable fields
            self.total_pages_available = None
            self.total_orders_available = None

    _active_cred = _CredentialProxy(_cred_row)
    _credential_found = True

    # Ensure auth_scheme is set (default to Token if missing)
    if not hasattr(_active_cred, 'auth_scheme') or not _active_cred.auth_scheme:
        _active_cred.auth_scheme = "Token"
        logger.info("[SYNC] auth_scheme_defaulted to Token")

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

                # Phase 1 progress is tracked exclusively via SyncRun.
                # BrandAPICredential legacy fields are no longer written.


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

                # Enqueue background sync when there is a next cursor URL and page number.
                # total_pages_available may be None for cursor-based pagination (LeafLink).
                if next_leaflink_url and next_page_number:
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
                    # Paused state is tracked exclusively via SyncRun.
                    # BrandAPICredential legacy fields are no longer written.
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
    Return the current sync progress for a brand.

    Uses the SyncRun model as the authoritative source of truth.
    Falls back to the last completed run if no active run exists.

    percent_complete = pages_synced / total_pages * 100 (clamped 0-100).
    Returns null if total_pages is unknown.

    Stall detection: if last_progress_at is >90 seconds old, status=stalled,
    background_sync_active=false, and ETA is suppressed.

    ETA is only returned when: status=syncing AND not stalled AND pages_synced>=3
    AND last_progress_at is recent.

    data_mode reflects the truthfulness of the data:
      live_db       — sync completed, data is current
      partial_sync  — sync in progress
      stalled       — sync was running but has not progressed for >90s
      cached        — no active run, showing last completed run info
      failed        — last run failed
      no_data       — no runs found at all
    """
    from services.sync_run_manager import detect_stalled, serialize_sync_run

    timestamp = datetime.now(timezone.utc).isoformat()

    if not brand:
        return {
            "ok": False,
            "error": "brand query parameter is required",
            "timestamp": timestamp,
        }

    logger.info("[OrdersSyncStatus] status_check brand=%s", brand)

    # ------------------------------------------------------------------ #
    # Fetch active SyncRun (queued or syncing)                           #
    # ------------------------------------------------------------------ #
    try:
        active_run = await SyncRun.get_active_run(db, brand)
    except Exception as exc:
        logger.error("[OrdersSyncStatus] active_run_fetch_error brand=%s error=%s", brand, exc)
        await db.rollback()
        active_run = None

    # ------------------------------------------------------------------ #
    # Fetch last completed run                                            #
    # ------------------------------------------------------------------ #
    try:
        last_completed = await SyncRun.get_last_completed_run(db, brand)
    except Exception as exc:
        logger.error("[OrdersSyncStatus] last_completed_fetch_error brand=%s error=%s", brand, exc)
        await db.rollback()
        last_completed = None

    # ------------------------------------------------------------------ #
    # Count distinct orders in DB for this brand                         #
    # ------------------------------------------------------------------ #
    try:
        count_result = await db.execute(
            select(func.count(func.distinct(Order.external_order_id))).where(
                Order.brand_id == brand
            )
        )
        total_orders_in_db = count_result.scalar_one() or 0
    except Exception as exc:
        logger.error("[OrdersSyncStatus] order_count_error brand=%s error=%s", brand, exc)
        await db.rollback()
        total_orders_in_db = 0

    # ------------------------------------------------------------------ #
    # Determine which run to report on                                   #
    # ------------------------------------------------------------------ #
    run = active_run
    is_stalled = False
    data_mode = "no_data"
    background_sync_active = False
    last_successful_sync_at = None

    if last_completed:
        last_successful_sync_at = (
            last_completed.completed_at.isoformat()
            if last_completed.completed_at
            else None
        )

    if run is not None:
        is_stalled = detect_stalled(run)
        background_sync_active = not is_stalled and run.status in ("queued", "syncing")

        if is_stalled:
            data_mode = "stalled"
        elif run.status == "syncing":
            data_mode = "partial_sync"
        else:
            data_mode = "partial_sync"
    elif last_completed is not None:
        run = last_completed
        data_mode = "live_db"
        background_sync_active = False
    else:
        # Check for failed runs
        try:
            failed_result = await db.execute(
                select(SyncRun)
                .where(SyncRun.brand_id == brand, SyncRun.status == "failed")
                .order_by(SyncRun.completed_at.desc())
                .limit(1)
            )
            failed_run = failed_result.scalar_one_or_none()
        except Exception:
            await db.rollback()
            failed_run = None

        if failed_run:
            run = failed_run
            data_mode = "failed"
        else:
            data_mode = "no_data"

    # ------------------------------------------------------------------ #
    # Build response from run (if any)                                   #
    # ------------------------------------------------------------------ #
    if run is None:
        return make_json_safe({
            "ok": True,
            "brand_id": brand,
            "sync_status": "idle",
            "data_mode": "no_data",
            "background_sync_active": False,
            "pages_synced": 0,
            "total_pages": None,
            "percent_complete": None,
            "total_orders_in_db": total_orders_in_db,
            "orders_loaded_this_run": 0,
            "total_orders_available": None,
            "last_successful_sync_at": None,
            "estimated_completion_minutes": None,
            "last_error": None,
            "stalled_reason": None,
            "active_sync_run_id": None,
            "started_at": None,
            "source": "sync_run",
            "sync_run": None,
            "timestamp": timestamp,
        })

    # Compute percent_complete only when total_pages is known.
    # For cursor-based pagination (LeafLink), total_pages is None — return None
    # so callers display an indeterminate progress indicator rather than a
    # misleading value like "400%" or "4 / 1".
    percent_complete: Optional[float] = None
    if run.total_pages and run.total_pages > 0:
        raw_pct = (run.pages_synced / run.total_pages) * 100
        percent_complete = round(min(100.0, max(0.0, raw_pct)), 2)

    # ETA: only when actively syncing, not stalled, and enough data
    estimated_completion_minutes: Optional[float] = None
    if (
        not is_stalled
        and run.status == "syncing"
        and run.pages_synced >= 3
        and run.last_progress_at is not None
    ):
        eta = run.estimated_completion_minutes()
        if eta is not None:
            estimated_completion_minutes = round(eta, 1)

    # Effective status for the response
    if is_stalled:
        effective_status = "stalled"
    elif run is last_completed:
        effective_status = "completed"
    else:
        effective_status = run.status

    # active_sync_run_id is only set when reporting on an active (non-completed) run
    active_sync_run_id = run.id if active_run is not None and run is active_run else None

    logger.info(
        "[OrdersSyncStatus] response brand=%s status=%s pages=%s/%s pct=%s "
        "orders_in_db=%s stalled=%s data_mode=%s source=sync_run run_id=%s",
        brand,
        effective_status,
        run.pages_synced,
        run.total_pages,
        percent_complete,
        total_orders_in_db,
        is_stalled,
        data_mode,
        run.id,
    )

    return make_json_safe({
        "ok": True,
        "brand_id": brand,
        "sync_status": effective_status,
        "data_mode": data_mode,
        "background_sync_active": background_sync_active,
        "pages_synced": run.pages_synced,
        "total_pages": run.total_pages,
        "percent_complete": percent_complete,
        "total_orders_in_db": total_orders_in_db,
        "orders_loaded_this_run": run.orders_loaded_this_run,
        "total_orders_available": run.total_orders_available,
        "last_successful_sync_at": last_successful_sync_at,
        "estimated_completion_minutes": estimated_completion_minutes,
        "last_error": run.last_error,
        "stalled_reason": run.stalled_reason if is_stalled else None,
        "is_stalled": is_stalled,
        "active_sync_run_id": active_sync_run_id,
        "started_at": run.started_at.isoformat() if run.started_at else None,
        "source": "sync_run",
        "sync_run": serialize_sync_run(run, is_stalled=is_stalled),
        "timestamp": timestamp,
    })


@router.post("/sync/reset")
async def orders_sync_reset(
    brand: str = Query(..., description="Brand slug (e.g., 'noble-nectar')"),
    hard: bool = Query(False, description="If true, delete all orders for this brand"),
    db: AsyncSession = Depends(get_db),
):
    """
    Reset sync state for a brand.

    Stops any active sync worker, marks the active SyncRun as stalled
    (reason=manual_reset), and clears cursor/progress state.

    If hard=true, all Order rows for the brand are also deleted.
    Existing orders are preserved by default (soft reset).

    Returns a reset confirmation with order count.
    """
    from services.sync_run_manager import reset_run
    from services.background_sync_manager import sync_manager

    timestamp = datetime.now(timezone.utc).isoformat()
    logger.info("[OrdersSyncReset] reset_requested brand=%s hard=%s", brand, hard)

    # Stop any in-memory background worker
    sync_manager.stop_sync(brand)

    try:
        async with AsyncSessionLocal() as reset_db:
            async with reset_db.begin():
                result = await reset_run(reset_db, brand, hard=hard)

        logger.info(
            "[OrdersSyncReset] reset_complete brand=%s action=%s hard=%s",
            brand,
            result.get("action"),
            hard,
        )

        return make_json_safe({
            "ok": True,
            "brand_id": brand,
            "hard": hard,
            "timestamp": timestamp,
            **result,
        })

    except Exception as exc:
        logger.error("[OrdersSyncReset] reset_error brand=%s error=%s", brand, exc, exc_info=True)
        return {
            "ok": False,
            "brand_id": brand,
            "error": str(exc),
            "timestamp": timestamp,
        }


@router.post("/sync/start")
async def orders_sync_start(
    brand: str = Query(..., description="Brand slug (e.g., 'noble-nectar')"),
    mode: str = Query("full", description="Sync mode: full | incremental"),
    db: AsyncSession = Depends(get_db),
):
    """
    Start a new sync run for a brand.

    Creates a SyncRun record and enqueues a background sync.
    Rejects duplicate starts if a run is already active (queued or syncing).

    Always starts from page 1 / cursor null.
    Returns sync_run_id and initial status.
    """
    from services.sync_run_manager import create_sync_run
    from services.background_sync_manager import sync_manager
    from services.credential_resolver import resolve_brand_credential

    timestamp = datetime.now(timezone.utc).isoformat()
    logger.info("[OrdersSyncStart] start_requested brand=%s mode=%s", brand, mode)

    if mode not in ("full", "incremental"):
        return {
            "ok": False,
            "error": f"Invalid mode '{mode}'. Must be 'full' or 'incremental'.",
            "timestamp": timestamp,
        }

    # Resolve credential
    cred_row = await resolve_brand_credential(db, brand, "leaflink")
    if not cred_row:
        return {
            "ok": False,
            "brand_id": brand,
            "error": "leaflink_credential_not_found",
            "timestamp": timestamp,
        }

    api_key = cred_row[4]
    company_id = cred_row[3]

    # Create SyncRun — rejects if one is already active
    try:
        async with AsyncSessionLocal() as start_db:
            async with start_db.begin():
                run = await create_sync_run(start_db, brand, mode=mode)
                sync_run_id = run.id
    except ValueError as dup_exc:
        logger.warning("[OrdersSyncStart] duplicate_start brand=%s error=%s", brand, dup_exc)
        return {
            "ok": False,
            "brand_id": brand,
            "error": "sync_already_active",
            "detail": str(dup_exc),
            "timestamp": timestamp,
        }
    except Exception as exc:
        logger.error("[OrdersSyncStart] create_run_error brand=%s error=%s", brand, exc, exc_info=True)
        return {
            "ok": False,
            "brand_id": brand,
            "error": str(exc),
            "timestamp": timestamp,
        }

    logger.info(
        "[OrdersSyncStart] sync_run_created brand=%s run_id=%s mode=%s",
        brand,
        sync_run_id,
        mode,
    )

    # Enqueue background sync via SyncRequest (picked up by worker)
    try:
        async with AsyncSessionLocal() as enq_db:
            async with enq_db.begin():
                enq_db.add(SyncRequest(
                    brand_id=brand,
                    status="pending",
                    start_page=1,
                    total_pages=None,
                    total_orders_available=None,
                ))
        logger.info("[OrdersSyncStart] sync_request_enqueued brand=%s run_id=%s", brand, sync_run_id)
    except Exception as enq_exc:
        logger.error("[OrdersSyncStart] enqueue_error brand=%s error=%s", brand, enq_exc)

    return make_json_safe({
        "ok": True,
        "brand_id": brand,
        "sync_run_id": sync_run_id,
        "mode": mode,
        "status": "queued",
        "message": f"Sync run {sync_run_id} created and queued for brand {brand}",
        "timestamp": timestamp,
    })


@router.post("/sync/resume")
async def orders_sync_resume(
    brand: str = Query(..., description="Brand slug (e.g., 'noble-nectar')"),
    db: AsyncSession = Depends(get_db),
):
    """
    Resume sync from the last successfully persisted cursor/page.

    Never restarts at page 1 unless no prior progress exists.
    Never creates duplicate workers — rejects if a run is already active.

    Returns sync_run_id and current status.
    """
    from services.sync_run_manager import create_sync_run, get_last_completed_run
    from services.credential_resolver import resolve_brand_credential

    timestamp = datetime.now(timezone.utc).isoformat()
    logger.info("[OrdersSyncResume] resume_requested brand=%s", brand)

    # Reject if already active
    try:
        active = await SyncRun.get_active_run(db, brand)
    except Exception as exc:
        await db.rollback()
        active = None

    if active:
        return {
            "ok": False,
            "brand_id": brand,
            "error": "sync_already_active",
            "sync_run_id": active.id,
            "status": active.status,
            "detail": f"Run {active.id} is already {active.status}",
            "timestamp": timestamp,
        }

    # Find last run with progress to resume from
    try:
        last_run_result = await db.execute(
            select(SyncRun)
            .where(SyncRun.brand_id == brand)
            .order_by(SyncRun.started_at.desc())
            .limit(1)
        )
        last_run = last_run_result.scalar_one_or_none()
    except Exception as exc:
        await db.rollback()
        last_run = None

    resume_page = 1
    resume_cursor = None
    if last_run and last_run.last_successful_page:
        resume_page = last_run.last_successful_page + 1
        resume_cursor = last_run.last_successful_cursor
        logger.info(
            "[OrdersSyncResume] resuming_from brand=%s page=%s cursor_hash=%s",
            brand,
            resume_page,
            _cursor_hash_safe(resume_cursor),
        )
    else:
        logger.info("[OrdersSyncResume] no_prior_progress brand=%s starting_from_page_1", brand)

    # Resolve credential
    cred_row = await resolve_brand_credential(db, brand, "leaflink")
    if not cred_row:
        return {
            "ok": False,
            "brand_id": brand,
            "error": "leaflink_credential_not_found",
            "timestamp": timestamp,
        }

    # Create new SyncRun for the resume
    try:
        async with AsyncSessionLocal() as res_db:
            async with res_db.begin():
                run = await create_sync_run(res_db, brand, mode="incremental")
                # Pre-populate resume cursor/page
                run.current_page = resume_page
                run.current_cursor = resume_cursor
                sync_run_id = run.id
    except ValueError as dup_exc:
        return {
            "ok": False,
            "brand_id": brand,
            "error": "sync_already_active",
            "detail": str(dup_exc),
            "timestamp": timestamp,
        }
    except Exception as exc:
        logger.error("[OrdersSyncResume] create_run_error brand=%s error=%s", brand, exc, exc_info=True)
        return {
            "ok": False,
            "brand_id": brand,
            "error": str(exc),
            "timestamp": timestamp,
        }

    # Enqueue background sync request
    try:
        async with AsyncSessionLocal() as enq_db:
            async with enq_db.begin():
                enq_db.add(SyncRequest(
                    brand_id=brand,
                    status="pending",
                    start_page=resume_page,
                    total_pages=None,
                    total_orders_available=None,
                ))
    except Exception as enq_exc:
        logger.error("[OrdersSyncResume] enqueue_error brand=%s error=%s", brand, enq_exc)

    return make_json_safe({
        "ok": True,
        "brand_id": brand,
        "sync_run_id": sync_run_id,
        "mode": "incremental",
        "status": "queued",
        "resume_from_page": resume_page,
        "message": f"Resuming sync from page {resume_page} for brand {brand}",
        "timestamp": timestamp,
    })


@router.get("/sync/debug")
async def orders_sync_debug(
    brand: str = Query(..., description="Brand slug (e.g., 'noble-nectar')"),
    db: AsyncSession = Depends(get_db),
):
    """
    Full diagnostic dump for a brand's sync state.

    Returns:
    - Active SyncRun (full object)
    - Last completed SyncRun
    - Current worker state (BackgroundSyncManager)
    - Last 20 SyncRun records (page log)
    - DB order counts (distinct external_order_id)
    - Duplicate LeafLink ID count
    - Newest/oldest order dates
    - Current cursor hash
    - Last error
    """
    from services.sync_run_manager import detect_stalled, serialize_sync_run
    from services.background_sync_manager import sync_manager

    timestamp = datetime.now(timezone.utc).isoformat()
    logger.info("[OrdersSyncDebug] debug_request brand=%s", brand)

    # Active run
    try:
        active_run = await SyncRun.get_active_run(db, brand)
    except Exception as exc:
        await db.rollback()
        active_run = None

    # Last completed run
    try:
        last_completed = await SyncRun.get_last_completed_run(db, brand)
    except Exception as exc:
        await db.rollback()
        last_completed = None

    # Last 20 runs (page log)
    try:
        runs_result = await db.execute(
            select(SyncRun)
            .where(SyncRun.brand_id == brand)
            .order_by(SyncRun.started_at.desc())
            .limit(20)
        )
        recent_runs = runs_result.scalars().all()
    except Exception as exc:
        await db.rollback()
        recent_runs = []

    # Order counts
    try:
        total_count_result = await db.execute(
            select(func.count(func.distinct(Order.external_order_id))).where(
                Order.brand_id == brand
            )
        )
        total_orders_in_db = total_count_result.scalar_one() or 0
    except Exception as exc:
        await db.rollback()
        total_orders_in_db = 0

    # Duplicate external_order_id count
    try:
        from sqlalchemy import text as _text
        dup_result = await db.execute(
            _text("""
                SELECT COUNT(*) FROM (
                    SELECT external_order_id
                    FROM orders
                    WHERE brand_id = :brand
                    GROUP BY external_order_id
                    HAVING COUNT(*) > 1
                ) AS dups
            """),
            {"brand": brand},
        )
        duplicate_count = dup_result.scalar_one() or 0
    except Exception as exc:
        await db.rollback()
        duplicate_count = 0

    # Newest/oldest order dates
    try:
        from sqlalchemy import text as _text2
        dates_result = await db.execute(
            _text2("""
                SELECT
                    MAX(external_updated_at) AS newest,
                    MIN(external_updated_at) AS oldest
                FROM orders
                WHERE brand_id = :brand
            """),
            {"brand": brand},
        )
        dates_row = dates_result.fetchone()
        newest_order_date = dates_row[0].isoformat() if dates_row and dates_row[0] else None
        oldest_order_date = dates_row[1].isoformat() if dates_row and dates_row[1] else None
    except Exception as exc:
        await db.rollback()
        newest_order_date = None
        oldest_order_date = None

    # Worker state
    worker_state = sync_manager.get_status(brand)

    # Serialize runs
    is_active_stalled = detect_stalled(active_run) if active_run else False

    return make_json_safe({
        "ok": True,
        "brand_id": brand,
        "timestamp": timestamp,
        "active_sync_run": serialize_sync_run(active_run, is_stalled=is_active_stalled) if active_run else None,
        "last_completed_run": serialize_sync_run(last_completed) if last_completed else None,
        "worker_state": worker_state,
        "recent_runs": [serialize_sync_run(r, is_stalled=detect_stalled(r)) for r in recent_runs],
        "db_stats": {
            "total_orders_in_db": total_orders_in_db,
            "duplicate_external_ids": duplicate_count,
            "newest_order_date": newest_order_date,
            "oldest_order_date": oldest_order_date,
        },
        "current_cursor_hash": (
            _cursor_hash_safe(active_run.current_cursor) if active_run else None
        ),
        "last_error": active_run.last_error if active_run else (
            last_completed.last_error if last_completed else None
        ),
    })


@router.post("/sync/recover")
async def orders_sync_recover(
    brand: str = Query(..., description="Brand slug (e.g., 'noble-nectar')"),
    db: AsyncSession = Depends(get_db),
):
    """
    Attempt automatic recovery from a stalled or failed sync.

    - If stalled: resume from last good cursor
    - If failed due to cursor loop: reset cursor to last known good page
    - If state is corrupted or no prior progress: recommend reset
    - Returns exact action taken
    """
    from services.sync_run_manager import detect_stalled, create_sync_run

    timestamp = datetime.now(timezone.utc).isoformat()
    logger.info("[OrdersSyncRecover] recover_requested brand=%s", brand)

    # Get active run
    try:
        active_run = await SyncRun.get_active_run(db, brand)
    except Exception as exc:
        await db.rollback()
        active_run = None

    # Get last run of any status
    try:
        last_run_result = await db.execute(
            select(SyncRun)
            .where(SyncRun.brand_id == brand)
            .order_by(SyncRun.started_at.desc())
            .limit(1)
        )
        last_run = last_run_result.scalar_one_or_none()
    except Exception as exc:
        await db.rollback()
        last_run = None

    run_to_check = active_run or last_run

    if run_to_check is None:
        return {
            "ok": False,
            "brand_id": brand,
            "action": "none",
            "recommendation": "No sync runs found. Use POST /orders/sync/start to begin.",
            "timestamp": timestamp,
        }

    is_stalled = detect_stalled(run_to_check)
    is_cursor_loop = (
        run_to_check.stalled_reason == "cursor_loop_detected"
        or (run_to_check.last_error or "").startswith("cursor_loop")
    )
    has_good_cursor = bool(run_to_check.last_successful_cursor or run_to_check.last_successful_page)

    # Determine recovery action
    if run_to_check.status == "completed":
        return make_json_safe({
            "ok": True,
            "brand_id": brand,
            "action": "none_needed",
            "detail": f"Last run {run_to_check.id} completed successfully.",
            "timestamp": timestamp,
        })

    if is_stalled or run_to_check.status in ("stalled", "failed"):
        if is_cursor_loop and has_good_cursor:
            # Reset cursor to last known good page and resume
            action = "reset_cursor_and_resume"
            resume_page = (run_to_check.last_successful_page or 1)
            resume_cursor = run_to_check.last_successful_cursor
        elif has_good_cursor:
            # Resume from last good cursor
            action = "resume_from_last_good_cursor"
            resume_page = (run_to_check.last_successful_page or 1) + 1
            resume_cursor = run_to_check.last_successful_cursor
        else:
            # No good cursor — recommend full reset
            return make_json_safe({
                "ok": True,
                "brand_id": brand,
                "action": "recommend_reset",
                "detail": (
                    "No last-good cursor found. "
                    "Use POST /orders/sync/reset then POST /orders/sync/start?mode=full"
                ),
                "run_id": run_to_check.id,
                "run_status": run_to_check.status,
                "timestamp": timestamp,
            })

        # Create a new run resuming from the good cursor
        from services.credential_resolver import resolve_brand_credential
        cred_row = await resolve_brand_credential(db, brand, "leaflink")
        if not cred_row:
            return {
                "ok": False,
                "brand_id": brand,
                "error": "leaflink_credential_not_found",
                "timestamp": timestamp,
            }

        try:
            async with AsyncSessionLocal() as rec_db:
                async with rec_db.begin():
                    new_run = await create_sync_run(rec_db, brand, mode="incremental")
                    new_run.current_page = resume_page
                    new_run.current_cursor = resume_cursor
                    new_run_id = new_run.id
        except ValueError as dup_exc:
            return {
                "ok": False,
                "brand_id": brand,
                "error": "sync_already_active",
                "detail": str(dup_exc),
                "timestamp": timestamp,
            }

        # Enqueue
        try:
            async with AsyncSessionLocal() as enq_db:
                async with enq_db.begin():
                    enq_db.add(SyncRequest(
                        brand_id=brand,
                        status="pending",
                        start_page=resume_page,
                        total_pages=run_to_check.total_pages,
                        total_orders_available=run_to_check.total_orders_available,
                    ))
        except Exception as enq_exc:
            logger.error("[OrdersSyncRecover] enqueue_error brand=%s error=%s", brand, enq_exc)

        return make_json_safe({
            "ok": True,
            "brand_id": brand,
            "action": action,
            "new_sync_run_id": new_run_id,
            "resume_from_page": resume_page,
            "prior_run_id": run_to_check.id,
            "prior_run_status": run_to_check.status,
            "timestamp": timestamp,
        })

    # Run is queued or syncing — nothing to recover
    return make_json_safe({
        "ok": True,
        "brand_id": brand,
        "action": "none_needed",
        "detail": f"Run {run_to_check.id} is currently {run_to_check.status}. No recovery needed.",
        "sync_run_id": run_to_check.id,
        "timestamp": timestamp,
    })


def _cursor_hash_safe(cursor: Optional[str]) -> Optional[str]:
    """Safe cursor hash for use in routes/orders.py without importing sync_run_manager."""
    import hashlib
    if not cursor:
        return None
    return hashlib.sha256(cursor.encode()).hexdigest()[:12]


@router.post("/sync/leaflink")
async def sync_orders_leaflink(
    body: dict,
    db: AsyncSession = Depends(get_db),
):
    """
    Sync orders from LeafLink for a specific brand.

    Request body:
    {
        "brand_id": "380e963d-36fc-4928-a4f4-e569cd535f9e"
    }

    Response:
    {
        "ok": true,
        "brand_id": "...",
        "synced_count": 42,
        "created_count": 10,
        "updated_count": 5,
        "error_count": 0,
        "errors": []
    }

    Or on error:
    {
        "ok": false,
        "error": "clear reason here",
        "brand_id": "..."
    }
    """
    brand_id = body.get("brand_id")

    if not brand_id:
        logger.warning("[OrdersSync] missing_brand_id")
        return {"ok": False, "error": "brand_id is required"}

    logger.info("[OrdersSync] sync_start brand_id=%s", brand_id)

    try:
        # Load LeafLink credentials from database
        try:
            cred = await resolve_leaflink_credential(db, brand_id=brand_id, allow_env_fallback=False)
        except Exception as cred_exc:
            logger.error("[OrdersSync] credential_resolution_failed brand_id=%s error=%s", brand_id, str(cred_exc)[:500], exc_info=True)
            return {
                "ok": False,
                "error": f"Failed to resolve credentials: {str(cred_exc)[:200]}",
                "brand_id": brand_id,
            }

        if not cred:
            logger.warning("[OrdersSync] no_credentials brand_id=%s", brand_id)
            return {
                "ok": False,
                "error": f"No LeafLink credentials found for brand {brand_id}",
                "brand_id": brand_id,
            }

        logger.info(
            "[OrdersSync] credentials_found brand_id=%s credential_id=%s auth_scheme=%s base_url=%s",
            brand_id,
            cred.id,
            cred.auth_scheme or "default",
            cred.base_url or "default",
        )

        # Import LeafLink client
        try:
            from services.leaflink_client import LeafLinkClient
            from services.leaflink_sync import sync_leaflink_orders
        except ImportError as import_exc:
            logger.error("[OrdersSync] import_failed error=%s", str(import_exc)[:500], exc_info=True)
            return {
                "ok": False,
                "error": f"Failed to import LeafLink services: {str(import_exc)[:200]}",
                "brand_id": brand_id,
            }

        # Create LeafLink client
        try:
            logger.info(
                "[LeafLinkAuth] client_init brand_id=%s auth_scheme=%s api_key_present=%s base_url=%s",
                brand_id,
                cred.auth_scheme or "default",
                bool(cred.api_key),
                cred.base_url or "default",
            )

            client = LeafLinkClient(
                api_key=cred.api_key,
                company_id=cred.company_id,
                brand_id=brand_id,
                base_url=cred.base_url,
                auth_scheme=cred.auth_scheme,
            )
            logger.info("[OrdersSync] client_created brand_id=%s", brand_id)
        except ValueError as val_exc:
            logger.error("[OrdersSync] client_creation_failed brand_id=%s error=%s", brand_id, str(val_exc)[:500], exc_info=True)
            return {
                "ok": False,
                "error": f"Invalid LeafLink credentials: {str(val_exc)[:200]}",
                "brand_id": brand_id,
            }
        except Exception as client_exc:
            logger.error("[OrdersSync] client_creation_error brand_id=%s error=%s", brand_id, str(client_exc)[:500], exc_info=True)
            return {
                "ok": False,
                "error": f"Failed to create LeafLink client: {str(client_exc)[:200]}",
                "brand_id": brand_id,
            }

        # Fetch orders from LeafLink
        try:
            logger.info("[OrdersSync] fetching_from_leaflink brand_id=%s", brand_id)

            loop = asyncio.get_event_loop()
            fetch_result = await asyncio.wait_for(
                loop.run_in_executor(
                    _leaflink_executor,
                    lambda: client.fetch_recent_orders(normalize=True, brand=brand_id),
                ),
                timeout=300,  # 5 minute timeout
            )

            orders = fetch_result.get("orders", [])
            pages_fetched = fetch_result.get("pages_fetched", 0)

            logger.info(
                "[OrdersSync] fetched_from_leaflink brand_id=%s orders=%s pages=%s",
                brand_id,
                len(orders),
                pages_fetched,
            )
        except asyncio.TimeoutError:
            logger.error("[OrdersSync] leaflink_fetch_timeout brand_id=%s", brand_id)
            return {
                "ok": False,
                "error": "LeafLink API request timed out (5 minutes)",
                "brand_id": brand_id,
            }
        except RuntimeError as runtime_exc:
            # LeafLink API errors are wrapped in RuntimeError
            logger.error("[OrdersSync] leaflink_api_error brand_id=%s error=%s", brand_id, str(runtime_exc)[:500], exc_info=True)
            return {
                "ok": False,
                "error": f"LeafLink API error: {str(runtime_exc)[:200]}",
                "brand_id": brand_id,
            }
        except Exception as fetch_exc:
            logger.error("[OrdersSync] leaflink_fetch_failed brand_id=%s error=%s", brand_id, str(fetch_exc)[:500], exc_info=True)
            return {
                "ok": False,
                "error": f"Failed to fetch orders from LeafLink: {str(fetch_exc)[:200]}",
                "brand_id": brand_id,
            }

        # Upsert orders into database
        try:
            logger.info("[OrdersSync] upserting_to_database brand_id=%s order_count=%s", brand_id, len(orders))

            # Do NOT wrap in `async with db.begin()` — the session provided by
            # get_db() uses SQLAlchemy's autobegin, so a transaction is already
            # active by the time we reach this point.  Calling db.begin() again
            # raises "connection already initialized a Transaction()".
            # sync_leaflink_orders() is designed to work inside the caller's
            # existing transaction; it must not open its own.
            sync_result = await sync_leaflink_orders(db, brand_id, orders, pages_fetched=pages_fetched)

            if not sync_result.get("ok"):
                logger.error("[OrdersSync] sync_failed brand_id=%s error=%s", brand_id, sync_result.get("error"))
                return {
                    "ok": False,
                    "error": sync_result.get("error", "Unknown sync error"),
                    "brand_id": brand_id,
                }

            logger.info(
                "[OrdersSync] sync_complete brand_id=%s created=%s updated=%s skipped=%s",
                brand_id,
                sync_result.get("created", 0),
                sync_result.get("updated", 0),
                sync_result.get("skipped", 0),
            )

            return {
                "ok": True,
                "brand_id": brand_id,
                "synced_count": len(orders),
                "created_count": sync_result.get("created", 0),
                "updated_count": sync_result.get("updated", 0),
                "error_count": sync_result.get("error_count", 0),
                "errors": sync_result.get("errors", []),
            }

        except asyncio.TimeoutError:
            logger.error("[OrdersSync] database_sync_timeout brand_id=%s", brand_id)
            return {
                "ok": False,
                "error": "Database sync timed out",
                "brand_id": brand_id,
            }
        except Exception as sync_exc:
            logger.error("[OrdersSync] database_sync_failed brand_id=%s error=%s", brand_id, str(sync_exc)[:500], exc_info=True)
            await db.rollback()
            return {
                "ok": False,
                "error": f"Failed to sync orders to database: {str(sync_exc)[:200]}",
                "brand_id": brand_id,
            }

    except Exception as outer_exc:
        logger.error("[OrdersSync] unexpected_error brand_id=%s error=%s", brand_id, str(outer_exc)[:500], exc_info=True)
        return {
            "ok": False,
            "error": f"Unexpected error: {str(outer_exc)[:200]}",
            "brand_id": brand_id,
        }


@router.post("/sync/{brand_id}")
async def sync_orders_by_brand_id(
    brand_id: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Alias endpoint for POST /orders/sync/leaflink.
    Sync orders from LeafLink for a specific brand.

    Path parameter:
    - brand_id: Brand UUID

    Response:
    {
        "ok": true,
        "brand_id": "...",
        "synced_count": 42,
        "created_count": 10,
        "updated_count": 5,
        "error_count": 0,
        "errors": []
    }
    """
    logger.info("[OrdersSync] sync_by_brand_id_start brand_id=%s", brand_id)

    # Delegate to the main sync endpoint
    return await sync_orders_leaflink({"brand_id": brand_id}, db)
