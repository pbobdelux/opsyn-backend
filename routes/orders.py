import asyncio
import base64
import logging
import os
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Optional

import requests

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
from sqlalchemy import cast, func, literal_column, select
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.ext.asyncio import AsyncSession

from database import AsyncSessionLocal, get_db
from models import BrandAPICredential, Order, OrderLine, SyncRequest, SyncRun
from services.watchdog_client import emit_watchdog_event
from utils.json_utils import make_json_safe

logger = logging.getLogger("orders")

router = APIRouter(prefix="/api/leaflink/orders", tags=["orders"])


def _safe_col(obj: object, attr: str, default=None):
    """Safely read a possibly-deferred or missing ORM column.

    Handles three cases:
    - Column exists and is loaded → returns the value.
    - Column is deferred (not yet migrated) → SQLAlchemy raises
      ``MissingGreenlet`` in async context; we catch it and return default.
    - Attribute doesn't exist at all → ``AttributeError``; return default.
    """
    try:
        return getattr(obj, attr, default)
    except Exception:
        return default


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
    - Query uses TRIM(LOWER()) normalization on brand_id and integration_name
      to avoid case/whitespace mismatches

    Args:
        db: AsyncSession
        brand_id: Tenant brand ID (UUID). If provided, DB-only.
        allow_env_fallback: Allow ENV fallback only if brand_id is empty.

    Returns:
        BrandAPICredential or None
    """
    from services.credential_resolver import extract_db_host
    from sqlalchemy import text as _text

    _db_host = extract_db_host(os.getenv("DATABASE_URL", ""))

    logger.info(
        "[CredentialLookup] db_host=%s brand_id=%s integration_name=leaflink is_active=true",
        _db_host,
        brand_id,
    )
    logger.info(
        "[CredentialResolver] lookup_start brand_id=%s integration_name=leaflink allow_env_fallback=%s",
        brand_id,
        allow_env_fallback,
    )

    # TENANT-SCOPED REQUEST: DB only, no ENV
    if brand_id:
        logger.info("[CredentialResolver] env_checked=false reason=brand_scoped_tenant_request")

        normalized_brand = brand_id.strip().lower()

        try:
            # Use TRIM(LOWER()) normalization to avoid case/whitespace mismatches
            result = await db.execute(
                _text("""
                    SELECT
                        id,
                        brand_id,
                        integration_name,
                        company_id,
                        api_key,
                        is_active,
                        base_url,
                        auth_scheme,
                        updated_at
                    FROM brand_api_credentials
                    WHERE TRIM(LOWER(brand_id::text)) = :normalized_brand
                    AND TRIM(LOWER(integration_name)) = 'leaflink'
                    AND is_active = true
                    ORDER BY updated_at DESC NULLS LAST
                    LIMIT 1
                """),
                {"normalized_brand": normalized_brand},
            )
            row = result.fetchone()

            if row:
                # row indices: 0=id, 1=brand_id, 2=integration_name, 3=company_id,
                #              4=api_key, 5=is_active, 6=base_url, 7=auth_scheme, 8=updated_at
                api_key = (row[4] or "").strip()
                _api_key_len = len(api_key)

                logger.info(
                    "[CredentialLookup] row_count=1 is_active=%s api_key_present=%s"
                    " api_key_len=%s base_url=%s auth_scheme=%s",
                    row[5],
                    bool(api_key),
                    _api_key_len,
                    row[6],
                    row[7],
                )
                logger.info(
                    "[CredentialResolver] lookup_result brand_id=%s integration_name=leaflink"
                    " row_count=1 is_active=%s api_key_present=%s base_url=%s auth_scheme=%s",
                    brand_id,
                    row[5],
                    bool(api_key),
                    row[6],
                    row[7],
                )

                # Validate key length
                logger.info(
                    "[CredentialLookup] api_key_validation_start api_key_len=%s",
                    _api_key_len,
                )
                if _api_key_len > 200 or _api_key_len < 10:
                    logger.error(
                        "[CredentialLookup] api_key_validation_failed reason=key_length_out_of_range api_key_len=%s",
                        _api_key_len,
                    )
                    logger.error(
                        "[CredentialResolver] tenant_key_invalid_length len=%s — REJECTING",
                        _api_key_len,
                    )
                    raise ValueError(f"Invalid LeafLink API key length: {_api_key_len}")

                logger.info("[CredentialLookup] api_key_validation_passed")

                # Build a BrandAPICredential-like object from the raw row
                cred = BrandAPICredential()
                cred.id = row[0]
                cred.brand_id = row[1]
                cred.integration_name = row[2]
                cred.company_id = (row[3] or "").strip()
                cred.api_key = api_key
                cred.is_active = row[5]
                cred.base_url = row[6]
                cred.auth_scheme = row[7]

                logger.info("[CredentialResolver] final_source=db tenant_isolated=true")

                # Validate base_url from DB and log for observability
                _cred_base_url = cred.base_url or ""
                if _cred_base_url and "marketplace.leaflink.com" in _cred_base_url.lower():
                    logger.warning(
                        "[LEAFLINK_CREDENTIAL_VALIDATION] brand_id=%s base_url_from_db=%s is_valid=false reason=marketplace_domain",
                        cred.brand_id,
                        _cred_base_url[:50],
                    )
                elif not _cred_base_url:
                    logger.info(
                        "[LEAFLINK_CREDENTIAL_VALIDATION] brand_id=%s base_url_from_db=none is_valid=true reason=will_use_canonical",
                        cred.brand_id,
                    )
                else:
                    logger.info(
                        "[LEAFLINK_CREDENTIAL_VALIDATION] brand_id=%s base_url_from_db=%s is_valid=true",
                        cred.brand_id,
                        _cred_base_url[:50],
                    )

                return cred
            else:
                logger.info(
                    "[CredentialLookup] row_count=0 reason=no_rows_found brand_id=%s integration_name=leaflink",
                    brand_id,
                )
                logger.info(
                    "[CredentialResolver] lookup_result brand_id=%s integration_name=leaflink row_count=0",
                    brand_id,
                )
                logger.error(
                    "[CredentialResolver] lookup_failed brand_id=%s integration_name=leaflink reason=no_rows_found",
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
    limit: int = Query(50, ge=1, le=200, description="Orders per page (default 50, max 200)"),
    offset: int = Query(0, ge=0, description="Offset for pagination (default 0)"),
    cursor: Optional[str] = Query(None, description="Opaque cursor for cursor-based pagination"),
    sort_by: str = Query("external_created_at", description="Sort field: external_created_at, updated_at, created_at, external_updated_at"),
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
            and_(Brand.id == cast(brand_id, PG_UUID(as_uuid=False)), Brand.org_id == cast(resolved_org_id, PG_UUID(as_uuid=False)))
        )
    )
    brand = brand_result.scalar_one_or_none()

    if not brand:
        logger.warning("[OrdersAPI] brand_not_found brand_id=%s org_id=%s", brand_id, resolved_org_id)
        return {"ok": False, "error": "Brand not found or does not belong to organization"}

    logger.info("[OrdersAPI] org_and_brand_validated org_id=%s brand_id=%s", resolved_org_id, brand_id)
    logger.info("[OrdersAPI] leaflink_credential_check skipped reason=read_endpoint")

    # ------------------------------------------------------------------ #
    # Diagnostic: log row counts to trace empty-result issues             #
    # These are cheap indexed queries that run before the main query.     #
    # ------------------------------------------------------------------ #
    try:
        from sqlalchemy import text as _text_diag
        # Total rows in orders table (unfiltered)
        _total_rows_res = await db.execute(_text_diag("SELECT COUNT(*) FROM orders"))
        _total_rows = _total_rows_res.scalar() or 0

        # Rows by brand_id
        _brand_rows_res = await db.execute(
            _text_diag("SELECT COUNT(*) FROM orders WHERE brand_id = :brand_id"),
            {"brand_id": brand_id},
        )
        _brand_rows = _brand_rows_res.scalar() or 0

        # Rows by org_id
        # orders.org_id is VARCHAR(120) — no CAST to UUID needed
        _org_rows_res = await db.execute(
            _text_diag("SELECT COUNT(*) FROM orders WHERE org_id = :org_id"),
            {"org_id": resolved_org_id},
        )
        _org_rows = _org_rows_res.scalar() or 0

        # Rows matching both brand_id AND org_id (what the main query will see)
        # Both brand_id and org_id are VARCHAR(120) — no CAST needed
        _both_rows_res = await db.execute(
            _text_diag(
                "SELECT COUNT(*) FROM orders "
                "WHERE brand_id = :brand_id AND org_id = :org_id"
            ),
            {"brand_id": brand_id, "org_id": resolved_org_id},
        )
        _both_rows = _both_rows_res.scalar() or 0

        # Latest inserted order timestamp
        _latest_res = await db.execute(
            _text_diag(
                "SELECT MAX(created_at) FROM orders "
                "WHERE brand_id = :brand_id"
            ),
            {"brand_id": brand_id},
        )
        _latest_ts = _latest_res.scalar()

        logger.info(
            "[OrdersAPI][DIAGNOSTIC] org_id=%s brand_id=%s "
            "total_rows_in_table=%s rows_by_brand=%s rows_by_org=%s "
            "rows_matching_both_filters=%s latest_insert=%s",
            resolved_org_id,
            brand_id,
            _total_rows,
            _brand_rows,
            _org_rows,
            _both_rows,
            _latest_ts.isoformat() if _latest_ts and hasattr(_latest_ts, "isoformat") else _latest_ts,
        )

        if _brand_rows > 0 and _both_rows == 0:
            logger.warning(
                "[OrdersAPI][DIAGNOSTIC] FILTER_MISMATCH brand_id=%s has %s rows by brand "
                "but 0 rows matching org_id=%s — org_id filter may be wrong",
                brand_id,
                _brand_rows,
                resolved_org_id,
            )
        if _total_rows == 0:
            logger.warning(
                "[OrdersAPI][DIAGNOSTIC] EMPTY_TABLE orders table has 0 rows total"
            )
    except Exception as _diag_exc:
        logger.info("[OrdersAPI][DIAGNOSTIC] diagnostic_query_failed error=%s", str(_diag_exc)[:100])
        try:
            await db.rollback()
        except Exception:
            pass


    # ------------------------------------------------------------------ #
    # Decode cursor → (cursor_id, cursor_updated_at)                      #
    # Cursor format: base64("id=<UUID>&updated_at=<ISO>")                 #
    # ------------------------------------------------------------------ #
    cursor_id: Optional[str] = None
    cursor_updated_at: Optional[str] = None

    if cursor:
        try:
            decoded = base64.b64decode(cursor).decode()
            parts = dict(p.split("=", 1) for p in decoded.split("&") if "=" in p)
            cursor_id = parts.get("id") or None
            cursor_updated_at = parts.get("updated_at") or None
        except Exception as cursor_exc:
            logger.error("[OrdersAPI] cursor_decode_error error=%s", cursor_exc)
            return {"ok": False, "error": "Invalid cursor format"}

    # ------------------------------------------------------------------ #
    # Resolve sort column                                                  #
    # ------------------------------------------------------------------ #
    if sort_by == "external_created_at":
        sort_col = Order.external_created_at
    elif sort_by == "created_at":
        sort_col = Order.created_at
    elif sort_by == "external_updated_at":
        sort_col = Order.external_updated_at
    elif sort_by == "updated_at":
        sort_col = Order.updated_at
    else:
        # Default: newest orders first by external_created_at
        sort_by = "external_created_at"
        sort_col = Order.external_created_at

    # ------------------------------------------------------------------ #
    # Build base query - SCOPED BY ORG_ID AND BRAND_ID                    #
    # Both filters are required for multi-tenant isolation.               #
    # ------------------------------------------------------------------ #
    from sqlalchemy.orm import defer as _defer, selectinload as _selectinload

    # Defer columns that may not exist in the database yet (pending
    # migration).  Using defer() prevents SQLAlchemy from including them
    # in the SELECT list; accessing a deferred attribute on a loaded
    # object raises a lazy-load which we guard with getattr() defaults.
    _DISPATCH_AR_COLS = [
        "assigned_driver_id",
        "assigned_driver_name",
        "delivery_status",
        "delivery_date",
        "route_number",
        "route_id",
        "driver_note",
        "delivery_instructions",
        "payment_status",
        "amount_paid",
        "balance_due",
        "due_date",
        "days_overdue",
        "invoice_number",
        "ar_note",
        "sync_health",
    ]
    _defer_opts = []
    for _col in _DISPATCH_AR_COLS:
        if hasattr(Order, _col):
            _defer_opts.append(_defer(getattr(Order, _col)))

    # Order.org_id is VARCHAR(120) — compare directly as string (no UUID cast needed).
    # Casting a VARCHAR column to UUID causes "operator does not exist: uuid = character varying".
    query = (
        select(Order)
        .options(_selectinload(Order.lines), *_defer_opts)
        .where(
            and_(
                Order.org_id == resolved_org_id,
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
        _limited_query = query.limit(limit + 1)
        logger.info(
            "[OrdersAPI][SQL_DEBUG] org_id=%s brand_id=%s compiled_query=%s",
            resolved_org_id,
            brand_id,
            str(_limited_query.compile(compile_kwargs={"literal_binds": False}))[:500],
        )
        result = await db.execute(_limited_query)
        orders = list(result.scalars().all())
    except Exception as exc:
        logger.error(
            "[OrdersAPI] db_query_failed org_id=%s brand_id=%s error=%s",
            resolved_org_id,
            brand_id,
            exc,
            exc_info=True,
        )
        try:
            await db.rollback()
        except Exception:
            pass
        return {
            "ok": False,
            "error": "Database query failed — type mismatch or schema error",
            "detail": str(exc)[:500],
            "hint": (
                "Check that Order.id and OrderLine.order_id SQLAlchemy types "
                "match the actual PostgreSQL column types (UUID vs INTEGER)."
            ),
        }

    has_more = len(orders) > limit
    if has_more:
        orders = orders[:limit]

    # ------------------------------------------------------------------ #
    # Total count scoped by org_id AND brand_id                           #
    # ------------------------------------------------------------------ #
    try:
        # Order.org_id is VARCHAR(120) — compare directly as string (no UUID cast).
        count_result = await db.execute(
            select(func.count(Order.id)).where(
                and_(Order.org_id == resolved_org_id, Order.brand_id == brand_id)
            )
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
        # List mode: headers only with routing fields, no line_items.
        # Use getattr() with safe defaults for dispatch/AR columns that
        # may not exist in the database yet (deferred above).
        def _enrich_order_totals(o) -> dict:
            """Build list-mode order dict with enriched total fields."""
            # `o.amount` and `o.total_cents` are stored in cents — divide by 100
            _stored = round(float(o.amount) / 100.0, 2) if o.amount else None
            if _stored is None and o.total_cents is not None:
                try:
                    _stored = round(int(o.total_cents) / 100.0, 2)
                except (TypeError, ValueError):
                    _stored = None

            # Derive total from loaded line items (if available)
            _lines = getattr(o, "lines", None) or []
            try:
                _derived = round(
                    sum(
                        (float(line.quantity or 0)) * (float(line.unit_price or 0))
                        for line in _lines
                    ),
                    2,
                )
            except Exception:
                _derived = 0.0

            _has_stored = _stored is not None and _stored > 0
            _has_derived = _derived > 0
            if not _has_stored and not _has_derived:
                _display = 0.0
                _source = "unknown"
            elif not _has_stored:
                _display = _derived
                _source = "derived_lines"
            elif _has_derived and _stored < (_derived * 0.5):
                _display = _derived
                _source = "derived_lines"
            else:
                _display = round(_stored, 2)
                _source = "stored"

            return {
                "id": o.id,
                "external_id": o.external_order_id,
                "order_number": o.order_number,
                "customer_name": o.customer_name,
                "status": o.status,
                "amount": round(float(o.amount) / 100.0, 2) if o.amount else 0,
                # Total fields — enriched for accurate UI display
                "stored_total": _stored,
                "derived_lines_total": _derived,
                "display_total": _display,
                "total_source": _source,
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
                # Dispatch fields — guarded with _safe_col() in case the
                # migration adding these columns hasn't run yet.
                "assigned_driver_id": _safe_col(o, "assigned_driver_id"),
                "assigned_driver_name": _safe_col(o, "assigned_driver_name"),
                "delivery_status": _safe_col(o, "delivery_status", "pending"),
                "delivery_date": _safe_col(o, "delivery_date"),
                "route_number": _safe_col(o, "route_number"),
                "route_id": _safe_col(o, "route_id"),
                "driver_note": _safe_col(o, "driver_note"),
                "delivery_instructions": _safe_col(o, "delivery_instructions"),
                # AR fields — same guard.
                "payment_status": _safe_col(o, "payment_status", "unpaid"),
                "amount_paid": float(_safe_col(o, "amount_paid", 0) or 0),
                "balance_due": float(_safe_col(o, "balance_due", 0) or 0),
                "due_date": _safe_col(o, "due_date"),
                "days_overdue": _safe_col(o, "days_overdue", 0),
                "invoice_number": _safe_col(o, "invoice_number"),
                "ar_note": _safe_col(o, "ar_note"),
            }

        serialized_orders = [_enrich_order_totals(o) for o in orders]

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

    # [ORDERS_API_RESULT] Structured audit log for brand app order queries
    _filters_applied = ["org_id", "brand_id"]
    if sort_by:
        _filters_applied.append(f"sort_by={sort_by}")
    if sort_order:
        _filters_applied.append(f"sort_order={sort_order}")
    if cursor:
        _filters_applied.append("cursor")
    logger.info(
        "[ORDERS_API_RESULT] route=%s brand_id=%s org_id=%s filters_applied=%s returned_count=%s total_in_db=%s limit=%s offset=%s",
        "/orders",
        brand_id,
        resolved_org_id,
        _filters_applied,
        len(serialized_orders),
        total_in_database,
        limit,
        offset,
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
        from leaflink.orders import _DEFER_OPTS as _ll_defer_opts_fresh
        orders_result = await db.execute(
            select(Order)
            .options(*_ll_defer_opts_fresh)
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
                "orders_are_fresh": latest_updated_at is not None and (datetime.now(timezone.utc) - (latest_updated_at if latest_updated_at.tzinfo is not None else latest_updated_at.replace(tzinfo=timezone.utc))).total_seconds() < 3600,  # Within 1 hour — ensure UTC-aware before subtraction
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
            # base_url and auth_scheme from DB (indices 8 and 9)
            self.base_url = row_data[8] if len(row_data) > 8 else None
            self.auth_scheme = row_data[9] if len(row_data) > 9 and row_data[9] else "Token"
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

    # [BRAND_CONFIG_AUDIT] Log credential configuration before every sync
    # so we can verify brand/org/company alignment. Never log API keys.
    _audit_org_id = getattr(_active_cred, "org_id", None) or request.headers.get("x-opsyn-org") or request.headers.get("x-org-id")
    logger.info(
        "[BRAND_CONFIG_AUDIT] brand_id=%s org_id=%s company_id=%s auth_scheme=%s base_url=%s integration_name=%s is_active=%s",
        _active_cred.brand_id,
        _audit_org_id,
        _active_cred.company_id,
        _active_cred.auth_scheme,
        (_active_cred.base_url[:50] if _active_cred.base_url else None),
        _active_cred.integration_name,
        _active_cred.is_active,
    )



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
                from services.leaflink_client import LeafLinkClient
                from services.leaflink_sync import (
                    sync_leaflink_orders,
                    sync_leaflink_orders_headers_only,
                    sync_leaflink_line_items,
                )

                # Use stored auth scheme if available, otherwise default to Token
                if _active_cred and _active_cred.auth_scheme:
                    _auth_scheme = _active_cred.auth_scheme
                    logger.info(
                        "[LeafLinkAuth] using_stored_scheme scheme=%s brand=%s",
                        _auth_scheme,
                        _resolved_brand_id,
                    )
                else:
                    _auth_scheme = "Token"
                    logger.info(
                        "[LeafLinkAuth] using_default_scheme scheme=Token brand=%s",
                        _resolved_brand_id,
                    )

                try:
                    client = LeafLinkClient(
                        api_key=api_key,
                        company_id=company_id,
                        brand_id=_resolved_brand_id,
                        base_url=cred.base_url,
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
                        "traceback": traceback.format_exc(),
                    })

                _api_url = f"{client.base_url}/orders-received/"
                logger.info(
                    "[OrdersSync] phase=1_fetch_init brand=%s company_id=%s api_url=%s",
                    _resolved_brand_id,
                    _resolved_company_id,
                    _api_url,
                )

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

                # Log brand_id immediately before passing to the DB write function
                from services.leaflink_sync import safe_uuid_for_db as _safe_uuid_for_db
                _pre_sql_brand_id = _safe_uuid_for_db(_resolved_brand_id, "brand_id") or _resolved_brand_id
                logger.info(
                    "[BRAND_ID_BEFORE_SQL] field=brand_id value=%s function=sync_orders_leaflink->sync_leaflink_orders_headers_only",
                    _pre_sql_brand_id,
                )

                # Resolve org_id from request header for this sync call
                _sync_org_id: Optional[str] = None
                _raw_org_header = request.headers.get("x-opsyn-org") or request.headers.get("x-org-id")
                if _raw_org_header:
                    try:
                        from services.organization_service import lookup_organization as _lookup_org_sync
                        _org_res = await _lookup_org_sync(db, _raw_org_header)
                        if _org_res.get("ok"):
                            _sync_org_id = str(_org_res["organization"].id)
                            logger.info(
                                "[ORG_CONTEXT] stage=get_sync_phase1_resolve org_id=%s brand_id=%s"
                                " external_order_id=N/A",
                                _sync_org_id,
                                _pre_sql_brand_id,
                            )
                    except Exception as _org_sync_exc:
                        logger.warning(
                            "[OrdersSync] phase1_org_resolve_error x_opsyn_org=%s error=%s",
                            _raw_org_header,
                            str(_org_sync_exc)[:200],
                        )

                try:
                    sync_result = await asyncio.wait_for(
                        sync_leaflink_orders_headers_only(
                            _pre_sql_brand_id,
                            orders_from_leaflink,
                            pages_fetched=pages_fetched_phase1,
                            org_id=_sync_org_id,
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
                _li_org_id = _sync_org_id

                async def _background_line_items():
                    await sync_leaflink_line_items(_li_brand_id, _li_orders, org_id=_li_org_id)

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
    request: Request,
    body: dict,
    x_opsyn_org: str | None = Header(None, description="Organization ID (UUID or org_code)"),
    db: AsyncSession = Depends(get_db),
):
    """
    Production-grade LeafLink order sync for a specific brand.

    Multi-tenant isolation enforced:
    - X-OPSYN-ORG header required (org UUID or org_code)
    - brand_id must belong to the org from X-OPSYN-ORG
    - Credentials loaded from DB by brand_id only (no env var fallback)
    - credential.brand_id validated against request brand_id
    - final_url constructed from credential.base_url + credential.company_id
    - Orders written with org_id + brand_id

    Architecture:
    1. Validate org_id (X-OPSYN-ORG header) and brand_id
    2. Validate brand belongs to org
    3. Load credentials from DB by brand_id (no env vars)
    4. Validate credential.brand_id matches request brand_id
    5. Create SyncRun record (own transaction, committed immediately)
    6. Fetch all orders from LeafLink API (no DB transaction)
    7. Open ONE transaction for upsert phase (writes org_id + brand_id)
    8. Update SyncRun on completion or failure
    9. Return structured JSON for all outcomes — never 502

    Request body:
        { "brand_id": "..." }

    Success response:
        { "ok": true, "brand_id": "...", "org_id": "...", "sync_run_id": N,
          "fetched_count": 120, "created_count": 10, "updated_count": 110,
          "skipped_count": 0, "error_count": 0, "duration_seconds": 12.4 }

    Failure response:
        { "ok": false, "stage": "...", "brand_id": "...", "org_id": "...",
          "sync_run_id": N, "error": "...", "error_count": 1, "errors": [...] }
    """
    logger.error("[ROUTE_ACTUAL] /orders/sync/leaflink hit brand_id=%s org=%s", body.get("brand_id") if isinstance(body, dict) else None, x_opsyn_org)
    logger.error("[ROUTE DEBUG] /orders/sync/leaflink HIT brand_id=%s org=%s", body.get("brand_id") if isinstance(body, dict) else None, x_opsyn_org)

    import time as _time
    from services.leaflink_client import LeafLinkClient
    from services.leaflink_sync import sync_leaflink_orders
    from services.organization_service import lookup_organization
    from services.sync_run_manager import create_sync_run, mark_completed, mark_failed
    from models.auth_models import Brand
    from sqlalchemy import and_

    sync_wall_start = _time.monotonic()
    brand_id: Optional[str] = None
    org_id: Optional[str] = None
    sync_run_id: Optional[int] = None

    try:
        # ------------------------------------------------------------------ #
        # Stage 0: Validate org_id from X-OPSYN-ORG header                   #
        # ------------------------------------------------------------------ #
        _raw_org = x_opsyn_org or request.headers.get("x-opsyn-org") or request.headers.get("x-org-id")
        if not _raw_org:
            logger.warning("[Sync] missing_org_id")
            return make_json_safe({
                "ok": False,
                "stage": "validation",
                "error": "X-OPSYN-ORG header is required",
                "brand_id": None,
                "org_id": None,
                "sync_run_id": None,
            })

        # Resolve org (supports UUID or org_code)
        org_lookup = await lookup_organization(db, _raw_org)
        if not org_lookup.get("ok"):
            logger.warning("[Sync] org_lookup_failed org=%s error=%s", _raw_org, org_lookup.get("error"))
            return make_json_safe({
                "ok": False,
                "stage": "validation",
                "error": org_lookup.get("error"),
                "brand_id": None,
                "org_id": _raw_org,
                "sync_run_id": None,
            })

        resolved_org = org_lookup.get("organization")
        org_id = str(resolved_org.id)

        # ------------------------------------------------------------------ #
        # Stage 0b: Validate brand_id from request body                      #
        # ------------------------------------------------------------------ #
        brand_id = (body.get("brand_id") or "").strip() if isinstance(body, dict) else ""
        logger.info("[Sync] request_parsed brand_id=%s", brand_id)
        if not brand_id:
            logger.warning("[Sync] missing_brand_id org_id=%s", org_id)
            return make_json_safe({
                "ok": False,
                "stage": "validation",
                "error": "brand_id is required in request body",
                "brand_id": None,
                "org_id": org_id,
                "sync_run_id": None,
            })

        logger.info("[Sync] start org_id=%s brand_id=%s credential_source=db", org_id, brand_id)

        # ------------------------------------------------------------------ #
        # Stage 0c: Validate brand belongs to org                            #
        # ------------------------------------------------------------------ #
        brand_result = await db.execute(
            select(Brand).where(
                and_(Brand.id == cast(brand_id, PG_UUID(as_uuid=False)), Brand.org_id == cast(org_id, PG_UUID(as_uuid=False)))
            )
        )
        brand_obj = brand_result.scalar_one_or_none()
        if not brand_obj:
            logger.warning("[Sync] brand_not_found_or_wrong_org brand_id=%s org_id=%s", brand_id, org_id)
            return make_json_safe({
                "ok": False,
                "stage": "validation",
                "error": "Brand not found or does not belong to organization",
                "brand_id": brand_id,
                "org_id": org_id,
                "sync_run_id": None,
            })

        logger.info("[Sync] brand_validated org_id=%s brand_id=%s", org_id, brand_id)

        # ------------------------------------------------------------------ #
        # Stage 1: Load credentials from DB by brand_id (no env vars)        #
        # ------------------------------------------------------------------ #
        try:
            cred = await resolve_leaflink_credential(db, brand_id=brand_id, allow_env_fallback=False)
        except ValueError as val_exc:
            logger.error("[Sync] credential_validation_error brand_id=%s org_id=%s error=%s", brand_id, org_id, val_exc)
            return make_json_safe({
                "ok": False,
                "stage": "credential_lookup",
                "brand_id": brand_id,
                "org_id": org_id,
                "sync_run_id": None,
                "error": str(val_exc)[:300],
                "error_count": 1,
                "errors": [str(val_exc)[:300]],
            })
        except Exception as cred_exc:
            logger.error("[Sync] credential_lookup_error brand_id=%s org_id=%s error=%s", brand_id, org_id, cred_exc, exc_info=True)
            return make_json_safe({
                "ok": False,
                "stage": "credential_lookup",
                "brand_id": brand_id,
                "org_id": org_id,
                "sync_run_id": None,
                "error": f"Credential lookup failed: {str(cred_exc)[:200]}",
                "error_count": 1,
                "errors": [str(cred_exc)[:200]],
            })

        if not cred:
            logger.warning("[Sync] no_credentials brand_id=%s org_id=%s", brand_id, org_id)
            return make_json_safe({
                "ok": False,
                "stage": "credential_lookup",
                "brand_id": brand_id,
                "org_id": org_id,
                "sync_run_id": None,
                "error": "LeafLink credentials missing for this brand",
                "error_count": 1,
                "errors": ["LeafLink credentials missing for this brand"],
            })

        # ------------------------------------------------------------------ #
        # Stage 1b: Validate credential.brand_id matches request brand_id    #
        # (prevents cross-tenant credential access)                           #
        # ------------------------------------------------------------------ #
        _cred_brand_id = (cred.brand_id or "").strip()
        if _cred_brand_id.lower() != brand_id.lower():
            logger.error(
                "[Sync] credential_brand_mismatch requested_brand=%s credential_brand=%s org_id=%s",
                brand_id, _cred_brand_id, org_id,
            )
            return make_json_safe({
                "ok": False,
                "stage": "credential_lookup",
                "brand_id": brand_id,
                "org_id": org_id,
                "sync_run_id": None,
                "error": "Credential brand_id does not match requested brand_id",
                "error_count": 1,
                "errors": ["Credential brand_id does not match requested brand_id"],
            })

        # Log safe credential metadata — NEVER log the actual api_key value
        _api_key_val = (cred.api_key or "").strip()
        _auth_scheme = (cred.auth_scheme or "Token").strip()
        _base_url = (cred.base_url or "").strip()
        _company_id = (cred.company_id or "").strip()
        _final_url = f"{_base_url}/companies/{_company_id}/orders-received/" if _base_url and _company_id else "unknown"
        logger.info(
            "[Sync] credentials_loaded source=db org_id=%s brand_id=%s company_id=%s "
            "base_url=%s auth_scheme=%s api_key_present=%s",
            org_id,
            brand_id,
            _company_id,
            _base_url,
            _auth_scheme,
            bool(_api_key_val),
        )
        logger.info("[Sync] leaflink_fetch_start final_url=%s", _final_url)

        # ------------------------------------------------------------------ #
        # Stage 2: Create SyncRun (own transaction, committed immediately)    #
        # ------------------------------------------------------------------ #
        try:
            async with AsyncSessionLocal() as _run_db:
                async with _run_db.begin():
                    _run = await create_sync_run(_run_db, brand_id, mode="full")
                    sync_run_id = _run.id
            logger.info("[Sync] sync_run_created run_id=%s brand_id=%s org_id=%s", sync_run_id, brand_id, org_id)
        except ValueError as dup_exc:
            # Active run already exists — not a fatal error, proceed without a new run
            logger.warning("[Sync] sync_run_already_active brand_id=%s org_id=%s detail=%s", brand_id, org_id, dup_exc)
            sync_run_id = None
        except Exception as run_exc:
            logger.error("[Sync] sync_run_create_error brand_id=%s org_id=%s error=%s", brand_id, org_id, run_exc, exc_info=True)
            sync_run_id = None  # Non-fatal — continue without tracking

        # ------------------------------------------------------------------ #
        # Stage 3: Build LeafLink client (credential validation)              #
        # ------------------------------------------------------------------ #
        logger.error("[ROUTE DEBUG] about to call LeafLink client")
        try:
            client = LeafLinkClient(
                api_key=cred.api_key,
                company_id=cred.company_id,
                brand_id=brand_id,
                base_url=cred.base_url,
                auth_scheme=cred.auth_scheme,
            )
        except ValueError as val_exc:
            err_msg = str(val_exc)[:300]
            logger.error("[Sync] client_init_validation_error brand_id=%s org_id=%s error=%s", brand_id, org_id, err_msg)
            if sync_run_id:
                try:
                    async with AsyncSessionLocal() as _fail_db:
                        async with _fail_db.begin():
                            await mark_failed(_fail_db, sync_run_id, err_msg)
                except Exception:
                    pass
            return make_json_safe({
                "ok": False,
                "stage": "validation",
                "brand_id": brand_id,
                "org_id": org_id,
                "sync_run_id": sync_run_id,
                "error": f"Invalid LeafLink credentials: {err_msg}",
                "error_count": 1,
                "errors": [err_msg],
            })
        except Exception as client_exc:
            err_msg = str(client_exc)[:300]
            logger.error("[Sync] client_init_error brand_id=%s org_id=%s error=%s", brand_id, org_id, err_msg, exc_info=True)
            if sync_run_id:
                try:
                    async with AsyncSessionLocal() as _fail_db:
                        async with _fail_db.begin():
                            await mark_failed(_fail_db, sync_run_id, err_msg)
                except Exception:
                    pass
            return make_json_safe({
                "ok": False,
                "stage": "validation",
                "brand_id": brand_id,
                "org_id": org_id,
                "sync_run_id": sync_run_id,
                "error": f"Failed to initialize LeafLink client: {err_msg}",
                "error_count": 1,
                "errors": [err_msg],
            })

        # ------------------------------------------------------------------ #
        # Stage 4: Fetch orders from LeafLink (NO DB transaction open)        #
        # ------------------------------------------------------------------ #
        _fetch_url = f"{client.base_url}/companies/{client.company_id}/orders-received/"
        logger.info("[Sync] leaflink_fetch_start url=%s brand_id=%s org_id=%s", _fetch_url, brand_id, org_id)

        try:
            loop = asyncio.get_event_loop()
            fetch_result = await asyncio.wait_for(
                loop.run_in_executor(
                    _leaflink_executor,
                    lambda: client.fetch_recent_orders(normalize=True, brand=brand_id),
                ),
                timeout=300,  # 5-minute overall fetch timeout
            )
            orders = fetch_result.get("orders", [])
            pages_fetched = fetch_result.get("pages_fetched", 0)
            logger.info(
                "[Sync] leaflink_fetch_complete fetched_count=%s pages=%s brand_id=%s",
                len(orders), pages_fetched, brand_id,
            )
        except asyncio.TimeoutError:
            err_msg = "LeafLink API fetch timed out after 5 minutes"
            logger.error("[Sync] leaflink_fetch_timeout brand_id=%s", brand_id)
            if sync_run_id:
                try:
                    async with AsyncSessionLocal() as _fail_db:
                        async with _fail_db.begin():
                            await mark_failed(_fail_db, sync_run_id, err_msg)
                except Exception:
                    pass
            return make_json_safe({
                "ok": False,
                "stage": "leaflink_fetch",
                "brand_id": brand_id,
                "sync_run_id": sync_run_id,
                "error": err_msg,
                "error_count": 1,
                "errors": [err_msg],
            })
        except Exception as fetch_exc:
            err_msg = str(fetch_exc)[:300]
            logger.error("[Sync] leaflink_fetch_error brand_id=%s error=%s", brand_id, err_msg, exc_info=True)
            if sync_run_id:
                try:
                    async with AsyncSessionLocal() as _fail_db:
                        async with _fail_db.begin():
                            await mark_failed(_fail_db, sync_run_id, err_msg)
                except Exception:
                    pass

            # Detect auth/authorization failures and return a structured message
            _err_lower = err_msg.lower()
            _is_auth_error = any(
                kw in _err_lower
                for kw in ("401", "403", "unauthorized", "forbidden", "authentication", "api access")
            )
            if _is_auth_error:
                logger.warning(
                    "[Sync] leaflink_auth_failure brand_id=%s org_id=%s error=%s",
                    brand_id,
                    org_id,
                    err_msg,
                )
                return make_json_safe({
                    "ok": False,
                    "stage": "leaflink_fetch",
                    "error": "LeafLink API unauthorized. API access may not be enabled for this key.",
                    "brand_id": brand_id,
                    "org_id": org_id,
                    "sync_run_id": sync_run_id,
                    "error_count": 1,
                    "errors": [err_msg],
                })

            return make_json_safe({
                "ok": False,
                "stage": "leaflink_fetch",
                "brand_id": brand_id,
                "org_id": org_id,
                "sync_run_id": sync_run_id,
                "error": f"LeafLink API error: {err_msg}",
                "error_count": 1,
                "errors": [err_msg],
            })


        # ------------------------------------------------------------------ #
        # Stage 5: Open ONE transaction for upsert phase                      #
        # Orders are written with org_id + brand_id for tenant isolation.     #
        # ------------------------------------------------------------------ #
        logger.info("[Sync] db_transaction_start brand_id=%s org_id=%s order_count=%s", brand_id, org_id, len(orders))

        try:
            async with AsyncSessionLocal() as _upsert_db:
                async with _upsert_db.begin():
                    sync_result = await sync_leaflink_orders(
                        _upsert_db, brand_id, orders, pages_fetched=pages_fetched, org_id=org_id
                    )
            # Transaction committed automatically on context manager exit
            logger.info(
                "[Sync] upsert_complete created=%s updated=%s skipped=%s errors=%s brand_id=%s org_id=%s",
                sync_result.get("created", 0),
                sync_result.get("updated", 0),
                sync_result.get("skipped", 0),
                sync_result.get("error_count", 0),
                brand_id,
                org_id,
            )
            logger.info("[Sync] db_transaction_commit brand_id=%s org_id=%s", brand_id, org_id)
        except Exception as upsert_exc:
            err_msg = str(upsert_exc)[:300]
            logger.error("[Sync] db_upsert_error brand_id=%s org_id=%s error=%s", brand_id, org_id, err_msg, exc_info=True)
            if sync_run_id:
                try:
                    async with AsyncSessionLocal() as _fail_db:
                        async with _fail_db.begin():
                            await mark_failed(_fail_db, sync_run_id, err_msg)
                except Exception:
                    pass
            return make_json_safe({
                "ok": False,
                "stage": "db_upsert",
                "brand_id": brand_id,
                "org_id": org_id,
                "sync_run_id": sync_run_id,
                "error": f"Database upsert failed: {err_msg}",
                "error_count": 1,
                "errors": [err_msg],
            })

        # ------------------------------------------------------------------ #
        # Stage 6: Update SyncRun to completed                                #
        # ------------------------------------------------------------------ #
        if sync_run_id:
            try:
                async with AsyncSessionLocal() as _done_db:
                    async with _done_db.begin():
                        await mark_completed(_done_db, sync_run_id)
            except Exception as done_exc:
                logger.error("[Sync] sync_run_complete_error run_id=%s error=%s", sync_run_id, done_exc)

        # ------------------------------------------------------------------ #
        # Stage 7: Build and return structured success response               #
        # ------------------------------------------------------------------ #
        duration = round(_time.monotonic() - sync_wall_start, 2)
        fetched_count = sync_result.get("fetched_count", len(orders))
        inserted_count = sync_result.get("inserted_count", sync_result.get("created", 0))
        updated_count = sync_result.get("updated_count", sync_result.get("updated", 0))
        partial_count = sync_result.get("partial_count", 0)
        skipped_count = sync_result.get("skipped_count", sync_result.get("skipped", 0))
        dead_letter_count = sync_result.get("dead_letter_count", 0)
        error_count = sync_result.get("error_count", 0)
        result_errors = sync_result.get("errors", [])
        has_warning = sync_result.get("warning", False) or error_count > 0 or dead_letter_count > 0 or partial_count > 0
        ok = sync_result.get("ok", True)

        logger.info(
            "[Sync] complete org_id=%s brand_id=%s run_id=%s fetched=%s inserted=%s updated=%s partial=%s skipped=%s dead_letter=%s errors=%s duration_seconds=%s",
            org_id, brand_id, sync_run_id, fetched_count, inserted_count, updated_count,
            partial_count, skipped_count, dead_letter_count, error_count, duration,
        )
        logger.info(
            "[SYNC_FINAL_REPORT] fetched=%s inserted=%s updated=%s partial=%s skipped=%s dead_letter=%s error=%s brand_id=%s run_id=%s",
            fetched_count, inserted_count, updated_count, partial_count,
            skipped_count, dead_letter_count, error_count, brand_id, sync_run_id,
        )

        if ok:
            return make_json_safe({
                "ok": True,
                "warning": has_warning,
                "org_id": org_id,
                "brand_id": brand_id,
                "sync_run_id": sync_run_id,
                "fetched_count": fetched_count,
                "inserted_count": inserted_count,
                "updated_count": updated_count,
                "partial_count": partial_count,
                "skipped_count": skipped_count,
                "dead_letter_count": dead_letter_count,
                "error_count": error_count,
                "errors": result_errors[:5],
                "duration_seconds": duration,
            })
        else:
            # Partial failure: some orders succeeded, some failed
            if sync_run_id:
                try:
                    async with AsyncSessionLocal() as _fail_db:
                        async with _fail_db.begin():
                            await mark_failed(
                                _fail_db, sync_run_id,
                                f"partial_failure: {error_count} orders failed"
                            )
                except Exception:
                    pass
            return make_json_safe({
                "ok": False,
                "warning": True,
                "stage": "db_upsert",
                "org_id": org_id,
                "brand_id": brand_id,
                "sync_run_id": sync_run_id,
                "fetched_count": fetched_count,
                "inserted_count": inserted_count,
                "updated_count": updated_count,
                "partial_count": partial_count,
                "skipped_count": skipped_count,
                "dead_letter_count": dead_letter_count,
                "error_count": error_count,
                "error": f"All {fetched_count} orders failed to upsert",
                "errors": result_errors[:5],
                "duration_seconds": duration,
            })

    except Exception as outer_exc:
        # Absolute last-resort catch — must never return 502
        duration = round(_time.monotonic() - sync_wall_start, 2)
        err_msg = str(outer_exc)[:300]
        logger.error(
            "[Sync] unexpected_error org_id=%s brand_id=%s run_id=%s error=%s duration=%s",
            org_id, brand_id, sync_run_id, err_msg, duration, exc_info=True,
        )
        if sync_run_id:
            try:
                async with AsyncSessionLocal() as _fail_db:
                    async with _fail_db.begin():
                        await mark_failed(_fail_db, sync_run_id, err_msg)
            except Exception:
                pass
        return make_json_safe({
            "ok": False,
            "stage": "unknown",
            "org_id": org_id,
            "brand_id": brand_id,
            "sync_run_id": sync_run_id,
            "error": f"Unexpected error: {err_msg}",
            "error_count": 1,
            "errors": [err_msg],
            "duration_seconds": duration,
        })


@router.post("/sync/{brand_id}")
async def sync_orders_by_brand_id(
    brand_id: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Alias endpoint for POST /orders/sync/leaflink.
    Sync orders from LeafLink for a specific brand via path parameter.

    Path parameter:
    - brand_id: Brand UUID or slug

    Delegates to POST /orders/sync/leaflink with brand_id in body.
    """
    logger.info("[Sync] sync_by_brand_id_alias brand_id=%s", brand_id)
    return await sync_orders_leaflink({"brand_id": brand_id}, db)


@router.get("/leaflink/test-paths")
async def test_leaflink_paths(
    brand_id: str = Query(...),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Test multiple LeafLink endpoint paths to find the working one."""

    # Load credential
    cred = await resolve_leaflink_credential(db, brand_id, allow_env_fallback=False)
    if not cred:
        return {"ok": False, "error": "Credential not found"}

    api_key = cred.api_key
    company_id = cred.company_id
    auth_scheme = cred.auth_scheme or "Token"

    # Test URLs
    test_urls = [
        f"https://www.leaflink.com/api/v2/orders/",
        f"https://www.leaflink.com/api/v2/orders-received/",
        f"https://www.leaflink.com/api/v2/brands/{company_id}/orders/",
        f"https://www.leaflink.com/api/v2/companies/{company_id}/orders/",
        f"https://www.leaflink.com/api/v2/orders-received/?company={company_id}",
        f"https://www.leaflink.com/api/v2/orders/?company={company_id}",
    ]

    results = []

    for url in test_urls:
        try:
            headers = {
                "Authorization": f"{auth_scheme} {api_key}",
                "Accept": "application/json",
            }

            resp = requests.get(url, headers=headers, timeout=10, params={"limit": 1})

            results.append({
                "url": url,
                "status": resp.status_code,
                "content_type": resp.headers.get("Content-Type", ""),
                "body_preview": resp.text[:200],
                "ok": resp.status_code == 200,
            })

            logger.error(
                "[LEAFLINK_TEST] url=%s status=%s content_type=%s auth_scheme=%s",
                url,
                resp.status_code,
                resp.headers.get("Content-Type", ""),
                auth_scheme,
            )

        except Exception as exc:
            results.append({
                "url": url,
                "status": None,
                "error": str(exc),
                "ok": False,
            })
            logger.error("[LEAFLINK_TEST] url=%s error=%s", url, exc)

    # Find working endpoint
    working = [r for r in results if r.get("ok")]

    return {
        "ok": len(working) > 0,
        "brand_id": brand_id,
        "company_id": company_id,
        "results": results,
        "working_endpoint": working[0]["url"] if working else None,
    }


# =============================================================================
# New Orders API Routes — incremental sync, full-resync, sync-status, resync
# =============================================================================


@router.post("/sync")
async def api_orders_incremental_sync(
    request: Request,
    x_opsyn_org: str | None = Header(None, description="Organization ID (UUID or org_code)"),
    brand_id: str = Query(..., description="Brand ID (UUID)"),
    db: AsyncSession = Depends(get_db),
):
    """
    Incremental sync from LeafLink.

    Pulls all new/updated orders since last_successful_sync_at.
    Paginates through ALL pages (not just the first batch).

    Returns:
        { ok, total_fetched, total_inserted, total_updated, failed_count,
          last_synced_at, next_sync_at }
    """
    from services.credential_resolver import resolve_brand_credential
    from services.sync_run_manager import create_sync_run, mark_completed, mark_failed
    from services.leaflink_sync import sync_leaflink_full_resync

    timestamp = datetime.now(timezone.utc).isoformat()
    logger.info("[OrdersAPI] incremental_sync brand_id=%s org=%s", brand_id, x_opsyn_org)

    # Resolve credential
    cred_row = await resolve_brand_credential(db, brand_id, "leaflink")
    if not cred_row:
        return make_json_safe({
            "ok": False,
            "error": "leaflink_credential_not_found",
            "brand_id": brand_id,
            "timestamp": timestamp,
        })


    api_key = cred_row[4]
    company_id = cred_row[3]
    auth_scheme = cred_row[9] if len(cred_row) > 9 and cred_row[9] else "Token"
    base_url = cred_row[8] if len(cred_row) > 8 else None
    resolved_brand_id = cred_row[1]

    # Resolve org_id from X-OPSYN-ORG header so orders are inserted with the
    # correct org_id and are visible through GET /orders (which filters by org_id).
    resolved_org_id: Optional[str] = None
    if x_opsyn_org:
        try:
            from services.organization_service import lookup_organization
            _org_lookup = await lookup_organization(db, x_opsyn_org)
            if _org_lookup.get("ok"):
                resolved_org_id = str(_org_lookup["organization"].id)
                logger.info(
                    "[ORG_CONTEXT] stage=incremental_sync_resolve org_id=%s brand_id=%s"
                    " external_order_id=N/A",
                    resolved_org_id,
                    resolved_brand_id,
                )
            else:
                logger.warning(
                    "[OrdersAPI] incremental_sync org_lookup_failed x_opsyn_org=%s error=%s",
                    x_opsyn_org,
                    _org_lookup.get("error"),
                )
        except Exception as _org_exc:
            logger.warning(
                "[OrdersAPI] incremental_sync org_resolve_error x_opsyn_org=%s error=%s",
                x_opsyn_org,
                str(_org_exc)[:200],
            )

    # Create SyncRun
    sync_run_id = None
    try:
        async with AsyncSessionLocal() as _run_db:
            async with _run_db.begin():
                _run = await create_sync_run(_run_db, resolved_brand_id, mode="incremental")
                sync_run_id = _run.id
    except ValueError:
        pass  # Already active — proceed without new run
    except Exception as run_exc:
        logger.warning("[OrdersAPI] incremental_sync run_create_error error=%s", run_exc)

    try:
        result = await sync_leaflink_full_resync(
            brand_id=resolved_brand_id,
            api_key=api_key,
            company_id=company_id,
            auth_scheme=auth_scheme,
            base_url=base_url,
            sync_run_id=sync_run_id,
            org_id=resolved_org_id,
        )


        if sync_run_id:
            try:
                async with AsyncSessionLocal() as _done_db:
                    async with _done_db.begin():
                        await mark_completed(_done_db, sync_run_id)
            except Exception:
                pass

        return make_json_safe({
            "ok": result.get("ok", True),
            "brand_id": resolved_brand_id,
            "sync_run_id": sync_run_id,
            "total_fetched": result.get("total_fetched", 0),
            "total_inserted": result.get("total_inserted", 0),
            "total_updated": result.get("total_updated", 0),
            "failed_count": result.get("failed_count", 0),
            "pages_synced": result.get("pages_synced", 0),
            "duration_seconds": result.get("duration_seconds", 0),
            "total_local_orders": result.get("total_local_orders", 0),
            "last_synced_at": timestamp,
            "timestamp": timestamp,
        })

    except Exception as exc:
        err_msg = str(exc)[:300]
        logger.error("[OrdersAPI] incremental_sync error brand_id=%s error=%s", brand_id, err_msg, exc_info=True)
        if sync_run_id:
            try:
                async with AsyncSessionLocal() as _fail_db:
                    async with _fail_db.begin():
                        await mark_failed(_fail_db, sync_run_id, err_msg)
            except Exception:
                pass
        return make_json_safe({
            "ok": False,
            "brand_id": brand_id,
            "error": err_msg,
            "timestamp": timestamp,
        })


@router.post("/full-resync")
async def api_orders_full_resync(
    request: Request,
    x_opsyn_org: str | None = Header(None, description="Organization ID (UUID or org_code)"),
    brand_id: str = Query(..., description="Brand ID (UUID)"),
    db: AsyncSession = Depends(get_db),
):
    """
    Enqueue a full backfill/resync of all LeafLink orders as an async background job.

    Returns immediately with a sync_run_id and state="queued".
    Never waits for sync completion — never executes the sync loop inside the
    request thread.  The background worker picks up the SyncRequest and runs
    sync_leaflink_full_resync asynchronously.

    Returns:
        { ok, sync_run_id, state, message, brand_id, timestamp }
    """
    from services.credential_resolver import resolve_brand_credential
    from services.sync_run_manager import create_sync_run

    timestamp = datetime.now(timezone.utc).isoformat()
    logger.info("[OrdersAPI] full_resync_enqueue brand_id=%s org=%s", brand_id, x_opsyn_org)

    # Resolve credential — validate it exists before enqueuing
    cred_row = await resolve_brand_credential(db, brand_id, "leaflink")
    if not cred_row:
        return make_json_safe({
            "ok": False,
            "error": "leaflink_credential_not_found",
            "brand_id": brand_id,
            "timestamp": timestamp,
        })

    resolved_brand_id = cred_row[1]

    # Resolve org_id from X-OPSYN-ORG header — stored in SyncRequest so the
    # background worker can propagate it to every order INSERT, ensuring
    # org_id is never NULL and orders are visible through GET /orders.
    resolved_org_id: Optional[str] = None
    if x_opsyn_org:
        try:
            from services.organization_service import lookup_organization
            _org_lookup = await lookup_organization(db, x_opsyn_org)
            if _org_lookup.get("ok"):
                resolved_org_id = str(_org_lookup["organization"].id)
                logger.info(
                    "[ORG_CONTEXT] stage=full_resync_enqueue_resolve org_id=%s brand_id=%s"
                    " external_order_id=N/A",
                    resolved_org_id,
                    resolved_brand_id,
                )
            else:
                logger.warning(
                    "[OrdersAPI] full_resync org_lookup_failed x_opsyn_org=%s error=%s",
                    x_opsyn_org,
                    _org_lookup.get("error"),
                )
        except Exception as _org_exc:
            logger.warning(
                "[OrdersAPI] full_resync org_resolve_error x_opsyn_org=%s error=%s",
                x_opsyn_org,
                str(_org_exc)[:200],
            )

    # Create SyncRun record (own transaction, committed immediately)
    sync_run_id = None
    try:
        async with AsyncSessionLocal() as _run_db:
            async with _run_db.begin():
                _run = await create_sync_run(_run_db, resolved_brand_id, mode="full")
                sync_run_id = _run.id
        logger.info(
            "[OrdersAPI] full_resync sync_run_created brand_id=%s run_id=%s",
            resolved_brand_id,
            sync_run_id,
        )
    except ValueError as dup_exc:
        # Active run already exists — return its ID so the caller can poll status
        logger.warning(
            "[OrdersAPI] full_resync sync_already_active brand_id=%s detail=%s",
            resolved_brand_id,
            dup_exc,
        )
        return make_json_safe({
            "ok": False,
            "error": "sync_already_active",
            "detail": str(dup_exc),
            "brand_id": resolved_brand_id,
            "state": "already_running",
            "timestamp": timestamp,
        })
    except Exception as run_exc:
        logger.warning("[OrdersAPI] full_resync run_create_error error=%s", run_exc)
        # Non-fatal — proceed without a SyncRun record

    # Enqueue background SyncRequest — worker picks this up and runs the full resync.
    # org_id is stored in the SyncRequest row so the worker can propagate it to
    # every order INSERT without needing to re-resolve the org from the header.
    try:
        async with AsyncSessionLocal() as _enq_db:
            async with _enq_db.begin():
                _enq_db.add(SyncRequest(
                    brand_id=resolved_brand_id,
                    org_id=resolved_org_id,
                    status="pending",
                    start_page=1,
                    total_pages=None,
                    total_orders_available=None,
                ))
        logger.info(
            "[OrdersAPI] full_resync enqueued brand_id=%s run_id=%s",
            resolved_brand_id,
            sync_run_id,
        )
    except Exception as enq_exc:
        logger.error(
            "[OrdersAPI] full_resync enqueue_error brand_id=%s error=%s",
            resolved_brand_id,
            enq_exc,
        )
        return make_json_safe({
            "ok": False,
            "error": f"Failed to enqueue sync job: {str(enq_exc)[:200]}",
            "brand_id": resolved_brand_id,
            "sync_run_id": sync_run_id,
            "timestamp": timestamp,
        })

    return make_json_safe({
        "ok": True,
        "sync_run_id": sync_run_id,
        "state": "queued",
        "message": "Full resync started",
        "brand_id": resolved_brand_id,
        "timestamp": timestamp,
    })


@router.get("/sync-status")
async def api_orders_sync_status(
    brand_id: str = Query(..., description="Brand ID (UUID)"),
    db: AsyncSession = Depends(get_db),
):
    """
    Returns the current sync status for a brand.

    Non-blocking: reads from sync_metrics_snapshots cache first, then falls
    back to a lightweight SyncRun query.  Never performs full DB scans or
    dead-letter aggregations inline.

    Response includes:
    - last_successful_sync_at
    - last_attempted_sync_at
    - total_local_orders (from snapshot cache)
    - total_leaflink_estimate (if available from last sync run)
    - current_sync_state: idle/running/failed/partial/success
    - partial_sync_count (from snapshot cache)
    - failed_order_count (from snapshot cache)
    - last_error
    - data_source: "snapshot" | "sync_run" (indicates where data came from)
    """
    from services.sync_run_manager import detect_stalled
    from sqlalchemy import text as _text_ss

    timestamp = datetime.now(timezone.utc).isoformat()
    logger.info("[OrdersAPI] sync_status brand_id=%s", brand_id)

    # ------------------------------------------------------------------ #
    # Fast path: read from snapshot cache                                  #
    # sync_metrics_snapshots.brand_id is VARCHAR — no CAST needed         #
    # ------------------------------------------------------------------ #
    snapshot = None
    _snap_query_start = time.monotonic()
    logger.info(
        "[SYNC_STATUS_COUNT_QUERY] brand_id=%s source=sync_metrics_snapshots"
        " cast_mode=none (brand_id is VARCHAR)",
        brand_id,
    )
    try:
        snap_result = await db.execute(
            _text_ss(
                "SELECT total_local_orders, total_ok, total_partial, total_failed, "
                "dead_letter_count, pages_processed, records_processed, "
                "last_successful_sync_at, updated_at, sync_run_id "
                "FROM sync_metrics_snapshots "
                "WHERE brand_id = :brand_id "
                "ORDER BY updated_at DESC NULLS LAST "
                "LIMIT 1"
            ),
            {"brand_id": brand_id},
        )
        snapshot = snap_result.fetchone()
        _snap_duration_ms = round((time.monotonic() - _snap_query_start) * 1000, 1)
        logger.info(
            "[SYNC_STATUS_COUNT_RESULT] brand_id=%s snapshot_found=%s"
            " total_local_orders=%s cast_mode_used=none query_duration_ms=%s",
            brand_id,
            snapshot is not None,
            snapshot[0] if snapshot else None,
            _snap_duration_ms,
        )
    except Exception as snap_exc:
        _snap_duration_ms = round((time.monotonic() - _snap_query_start) * 1000, 1)
        logger.info(
            "[OrdersAPI] sync_status snapshot_miss brand_id=%s reason=%s duration_ms=%s",
            brand_id,
            str(snap_exc)[:100],
            _snap_duration_ms,
        )
        try:
            await db.rollback()
        except Exception:
            pass

    # ------------------------------------------------------------------ #
    # Lightweight SyncRun query (always needed for state/error/ETA)       #
    # ------------------------------------------------------------------ #
    try:
        active_run = await SyncRun.get_active_run(db, brand_id)
    except Exception:
        await db.rollback()
        active_run = None

    try:
        last_completed = await SyncRun.get_last_completed_run(db, brand_id)
    except Exception:
        await db.rollback()
        last_completed = None

    try:
        last_run_result = await db.execute(
            select(SyncRun)
            .where(SyncRun.brand_id == brand_id)
            .order_by(SyncRun.started_at.desc())
            .limit(1)
        )
        last_run = last_run_result.scalar_one_or_none()
    except Exception:
        await db.rollback()
        last_run = None

    # ------------------------------------------------------------------ #
    # Determine sync state                                                 #
    # ------------------------------------------------------------------ #
    is_stalled = detect_stalled(active_run) if active_run else False

    if active_run and not is_stalled:
        current_sync_state = "running"
    elif is_stalled:
        current_sync_state = "partial"
    elif last_run and last_run.status == "failed":
        current_sync_state = "failed"
    elif last_completed:
        # Only report "success" when the SyncRun was explicitly marked "completed"
        # by the reconciliation-aware status update. "completed" status is only
        # set when DB counts grew (reconciliation passed).
        current_sync_state = "success"
    elif last_run and last_run.status == "incomplete":
        # Reconciliation failed or partial — report the last_error for diagnosis
        current_sync_state = "partial"
    elif last_run and last_run.status == "stalled":
        current_sync_state = "partial"
    elif last_run:
        current_sync_state = "partial"
    else:
        current_sync_state = "idle"

    # ------------------------------------------------------------------ #
    # Live DB count — always authoritative for total_local_orders         #
    # brand_id is VARCHAR(120) — _get_brand_order_count uses ORM (no CAST)#
    # ------------------------------------------------------------------ #
    live_count = await _get_brand_order_count(db, brand_id)
    logger.info(
        "[SYNC_STATUS_LIVE_COUNT] brand_id=%s live_count=%s",
        brand_id,
        live_count,
    )

    # ------------------------------------------------------------------ #
    # Populate partial/failed counts from snapshot; timestamps from snap  #
    # ------------------------------------------------------------------ #
    if snapshot:
        snapshot_count = snapshot[0] or 0
        partial_sync_count = snapshot[2] or 0
        failed_order_count = snapshot[3] or 0
        snap_last_sync = snapshot[7]
        logger.info(
            "[SYNC_STATUS_SNAPSHOT_COUNT] brand_id=%s snapshot_count=%s",
            brand_id,
            snapshot_count,
        )
    else:
        partial_sync_count = 0
        failed_order_count = 0
        snap_last_sync = None

    total_local_orders = live_count
    data_source = "live"

    # ------------------------------------------------------------------ #
    # Timestamps                                                           #
    # ------------------------------------------------------------------ #
    last_successful_sync_at = None
    if snap_last_sync:
        last_successful_sync_at = snap_last_sync.isoformat() if hasattr(snap_last_sync, "isoformat") else str(snap_last_sync)
    elif last_completed and last_completed.completed_at:
        last_successful_sync_at = last_completed.completed_at.isoformat()

    last_attempted_sync_at = (
        last_run.started_at.isoformat()
        if last_run and last_run.started_at
        else None
    )
    last_error = last_run.last_error if last_run else None

    total_leaflink_estimate = last_run.total_orders_available if last_run else None

    return make_json_safe({
        "ok": True,
        "brand_id": brand_id,
        "last_successful_sync_at": last_successful_sync_at,
        "last_attempted_sync_at": last_attempted_sync_at,
        "total_local_orders": total_local_orders,
        "total_leaflink_estimate": total_leaflink_estimate,
        "current_sync_state": current_sync_state,
        "partial_sync_count": partial_sync_count,
        "failed_order_count": failed_order_count,
        "last_error": last_error,
        "active_sync_run_id": active_run.id if active_run else None,
        "pages_synced": last_run.pages_synced if last_run else 0,
        "orders_loaded_this_run": last_run.orders_loaded_this_run if last_run else 0,
        "data_source": data_source,
        "timestamp": timestamp,
    })


@router.get("/queues")
async def api_orders_queues(
    brand_id: str = Query(..., description="Brand ID (UUID)"),
    db: AsyncSession = Depends(get_db),
):
    """
    Returns queue counts for needs_review, driver_queue, and ar_queue.

    Uses correct queue logic:
    - needs_review: only orders with real sync issues (partial/failed/missing data/blockers)
    - driver_queue: approved orders with no driver assigned
    - ar_queue: delivered/completed orders with balance_due > 0
    """
    from leaflink.orders import is_needs_review, is_driver_queue, is_ar_queue, build_line_items, derive_blockers, _DEFER_OPTS as _ll_defer_opts_queues
    from sqlalchemy.orm import selectinload as _selectinload

    timestamp = datetime.now(timezone.utc).isoformat()
    logger.info("[OrdersAPI] queues brand_id=%s", brand_id)

    try:
        result = await db.execute(
            select(Order)
            .options(_selectinload(Order.lines), *_ll_defer_opts_queues)
            .where(Order.brand_id == brand_id)
        )
        orders = result.scalars().all()
    except Exception as exc:
        await db.rollback()
        return make_json_safe({"ok": False, "error": str(exc)[:200], "timestamp": timestamp})

    needs_review_count = 0
    driver_queue_count = 0
    ar_queue_count = 0

    for order in orders:
        line_items = build_line_items(order)
        blockers = derive_blockers(line_items)

        if is_needs_review(order, line_items, blockers):
            needs_review_count += 1
        if is_driver_queue(order):
            driver_queue_count += 1
        if is_ar_queue(order):
            ar_queue_count += 1

    return make_json_safe({
        "ok": True,
        "brand_id": brand_id,
        "total_orders": len(orders),
        "needs_review": needs_review_count,
        "driver_queue": driver_queue_count,
        "ar_queue": ar_queue_count,
        "timestamp": timestamp,
    })


# =============================================================================
# Sync Metrics Endpoint
# =============================================================================


@router.get("/sync-metrics")
async def api_orders_sync_metrics(
    brand_id: str = Query(..., description="Brand ID (UUID)"),
    org_id: Optional[str] = Query(None, description="Organization ID (UUID)"),
    db: AsyncSession = Depends(get_db),
):
    """
    Returns detailed sync metrics for a brand.

    Non-blocking: reads from sync_metrics_snapshots cache instead of performing
    full DB scans or dead-letter aggregations inline.  Falls back to a lightweight
    SyncRun query for timing/state fields not stored in the snapshot.

    Response:
        {
          ok, brand_id, org_id,
          total_local_orders, total_ok, total_partial, total_failed,
          dead_letter_count, last_full_sync_duration_seconds,
          average_page_duration_seconds, pages_processed, records_processed,
          records_per_second, current_sync_state,
          failure_categories: { ... },
          last_sync_at, next_sync_at,
          data_source: "snapshot" | "sync_run"
        }
    """
    from sqlalchemy import text as _text_sm
    from datetime import timedelta

    timestamp = datetime.now(timezone.utc).isoformat()
    logger.info("[OrdersAPI] sync_metrics brand_id=%s", brand_id)

    # ------------------------------------------------------------------ #
    # Fast path: read from snapshot cache (single indexed query)           #
    # ------------------------------------------------------------------ #
    snapshot = None
    try:
        snap_result = await db.execute(
            _text_sm(
                "SELECT total_local_orders, total_ok, total_partial, total_failed, "
                "dead_letter_count, count_by_failure_category, "
                "pages_processed, records_processed, sync_rate, "
                "last_successful_sync_at, updated_at, sync_run_id "
                "FROM sync_metrics_snapshots "
                "WHERE brand_id = :brand_id "
                "ORDER BY updated_at DESC NULLS LAST "
                "LIMIT 1"
            ),
            {"brand_id": brand_id},
        )
        snapshot = snap_result.fetchone()
    except Exception as snap_exc:
        logger.info(
            "[OrdersAPI] sync_metrics snapshot_miss brand_id=%s reason=%s",
            brand_id,
            str(snap_exc)[:100],
        )
        try:
            await db.rollback()
        except Exception:
            pass

    # ------------------------------------------------------------------ #
    # Lightweight SyncRun query for timing/state fields                   #
    # ------------------------------------------------------------------ #
    last_full_sync_duration_seconds = None
    average_page_duration_seconds = None
    current_sync_state = "idle"
    last_sync_at = None
    next_sync_at = None

    try:
        last_run_res = await db.execute(
            select(SyncRun)
            .where(SyncRun.brand_id == brand_id)
            .order_by(SyncRun.started_at.desc())
            .limit(1)
        )
        last_run = last_run_res.scalar_one_or_none()

        if last_run:
            current_sync_state = last_run.status or "idle"
            _last_ts = last_run.completed_at or last_run.last_progress_at

            if last_run.started_at and last_run.completed_at:
                _dur = (last_run.completed_at - last_run.started_at).total_seconds()
                last_full_sync_duration_seconds = round(_dur, 1)
                _pages = last_run.pages_synced or 0
                if _pages > 0 and _dur > 0:
                    average_page_duration_seconds = round(_dur / _pages, 2)

            if _last_ts:
                next_sync_at = (_last_ts + timedelta(minutes=30)).isoformat()
                last_sync_at = _last_ts.isoformat()
    except Exception:
        await db.rollback()

    # ------------------------------------------------------------------ #
    # Populate counts from snapshot or fall back to zeros                  #
    # ------------------------------------------------------------------ #
    data_source = "sync_run"
    _all_categories = [
        "duplicate_key", "missing_field", "malformed", "line_item_issue",
        "fk_issue", "timeout", "rate_limit", "validation", "unknown",
    ]
    failure_categories: dict[str, int] = {cat: 0 for cat in _all_categories}

    if snapshot:
        total_local_orders = snapshot[0] or 0
        total_ok = snapshot[1] or 0
        total_partial = snapshot[2] or 0
        total_failed = snapshot[3] or 0
        dead_letter_count = snapshot[4] or 0
        # Merge stored failure category breakdown if available
        stored_cats = snapshot[5]
        if isinstance(stored_cats, dict):
            for k, v in stored_cats.items():
                failure_categories[k] = failure_categories.get(k, 0) + (v or 0)
        pages_processed = snapshot[6]
        records_processed = snapshot[7]
        records_per_second = float(snapshot[8]) if snapshot[8] is not None else None
        snap_last_sync = snapshot[9]
        if snap_last_sync and not last_sync_at:
            last_sync_at = snap_last_sync.isoformat() if hasattr(snap_last_sync, "isoformat") else str(snap_last_sync)
        data_source = "snapshot"
    else:
        total_local_orders = 0
        total_ok = 0
        total_partial = 0
        total_failed = 0
        dead_letter_count = 0
        pages_processed = None
        records_processed = None
        records_per_second = None

    return make_json_safe({
        "ok": True,
        "brand_id": brand_id,
        "org_id": org_id,
        "total_local_orders": total_local_orders,
        "total_ok": total_ok,
        "total_partial": total_partial,
        "total_failed": total_failed,
        "dead_letter_count": dead_letter_count,
        "last_full_sync_duration_seconds": last_full_sync_duration_seconds,
        "average_page_duration_seconds": average_page_duration_seconds,
        "pages_processed": pages_processed,
        "records_processed": records_processed,
        "records_per_second": records_per_second,
        "current_sync_state": current_sync_state,
        "failure_categories": failure_categories,
        "last_sync_at": last_sync_at,
        "next_sync_at": next_sync_at,
        "data_source": data_source,
        "timestamp": timestamp,
    })


# =============================================================================
# Dead-Letter Analysis Endpoint
# =============================================================================


@router.get("/dead-letter-analysis")
async def api_orders_dead_letter_analysis(
    brand_id: str = Query(..., description="Brand ID (UUID)"),
    org_id: Optional[str] = Query(None, description="Organization ID (UUID)"),
    sample_limit: int = Query(10, ge=1, le=50, description="Max sample failures to return"),
    db: AsyncSession = Depends(get_db),
):
    """
    Returns a structured analysis of dead-lettered orders for a brand.

    Includes:
    - Total dead-letter count
    - Breakdown by failure stage (error_stage column)
    - Breakdown by failure category (derived from error_message)
    - Sample of recent failures with full context

    Response:
        {
          ok, brand_id,
          total_dead_letter_count,
          by_failure_stage: { stage: count, ... },
          by_failure_category: { category: count, ... },
          sample_failures: [ { id, external_order_id, order_number,
                               failure_stage, failure_category,
                               exception_type, exception_message,
                               payload_size_bytes, created_at }, ... ]
        }
    """
    from sqlalchemy import text as _text
    from services.leaflink_sync import categorize_sync_failure as _categorize

    timestamp = datetime.now(timezone.utc).isoformat()
    logger.info("[OrdersAPI] dead_letter_analysis brand_id=%s", brand_id)

    # ------------------------------------------------------------------ #
    # Total dead-letter count                                              #
    # ------------------------------------------------------------------ #
    try:
        # sync_dead_letters.brand_id is VARCHAR — no CAST needed
        total_res = await db.execute(
            _text(
                "SELECT COUNT(*) FROM sync_dead_letters "
                "WHERE brand_id = :brand_id AND resolved_at IS NULL"
            ),
            {"brand_id": brand_id},
        )
        total_dead_letter_count = total_res.scalar() or 0
    except Exception:
        await db.rollback()
        total_dead_letter_count = 0

    # ------------------------------------------------------------------ #
    # Breakdown by failure stage                                           #
    # ------------------------------------------------------------------ #
    by_failure_stage: dict[str, int] = {}
    try:
        stage_res = await db.execute(
            _text(
                "SELECT error_stage, COUNT(*) as cnt FROM sync_dead_letters "
                "WHERE brand_id = :brand_id AND resolved_at IS NULL "
                "GROUP BY error_stage ORDER BY cnt DESC"
            ),
            {"brand_id": brand_id},
        )
        for row in stage_res.fetchall():
            by_failure_stage[row[0] or "unknown"] = row[1]
    except Exception:
        await db.rollback()

    # ------------------------------------------------------------------ #
    # Breakdown by failure category (derived from error_message)          #
    # ------------------------------------------------------------------ #
    by_failure_category: dict[str, int] = {}
    sample_rows = []
    try:
        rows_res = await db.execute(
            _text(
                "SELECT id, external_id, order_number, error_stage, error_message, "
                "       raw_payload, created_at "
                "FROM sync_dead_letters "
                "WHERE brand_id = :brand_id AND resolved_at IS NULL "
                "ORDER BY created_at DESC "
                "LIMIT :lim"
            ),
            {"brand_id": brand_id, "lim": max(sample_limit, 100)},
        )
        all_rows = rows_res.fetchall()

        for row in all_rows:
            err_msg = row[4] or ""
            try:
                cat = _categorize(Exception(err_msg))
            except Exception:
                cat = "unknown"
            by_failure_category[cat] = by_failure_category.get(cat, 0) + 1

            if len(sample_rows) < sample_limit:
                # Estimate payload size from raw_payload JSON string length
                raw_payload_val = row[5]
                payload_size = len(str(raw_payload_val)) if raw_payload_val else 0

                # Infer exception type from error message heuristics
                exc_type = "Exception"
                err_lower = err_msg.lower()
                if "keyerror" in err_lower or "key error" in err_lower:
                    exc_type = "KeyError"
                elif "typeerror" in err_lower or "type error" in err_lower:
                    exc_type = "TypeError"
                elif "valueerror" in err_lower or "value error" in err_lower:
                    exc_type = "ValueError"
                elif "undefinedcolumn" in err_lower or "does not exist" in err_lower:
                    exc_type = "UndefinedColumnError"
                elif "duplicate key" in err_lower:
                    exc_type = "UniqueViolation"
                elif "foreign key" in err_lower:
                    exc_type = "ForeignKeyViolation"
                elif "not null" in err_lower or "null value" in err_lower:
                    exc_type = "NotNullViolation"

                created_at_val = row[6]
                sample_rows.append({
                    "id": row[0],
                    "external_order_id": row[1],
                    "order_number": row[2],
                    "failure_stage": row[3] or "unknown",
                    "failure_category": cat,
                    "exception_type": exc_type,
                    "exception_message": err_msg[:500],
                    "payload_size_bytes": payload_size,
                    "created_at": created_at_val.isoformat() if hasattr(created_at_val, "isoformat") else str(created_at_val),
                })
    except Exception as exc:
        await db.rollback()
        logger.error("[OrdersAPI] dead_letter_analysis_error brand_id=%s error=%s", brand_id, exc)

    return make_json_safe({
        "ok": True,
        "brand_id": brand_id,
        "org_id": org_id,
        "total_dead_letter_count": total_dead_letter_count,
        "by_failure_stage": by_failure_stage,
        "by_failure_category": by_failure_category,
        "sample_failures": sample_rows,
        "timestamp": timestamp,
    })


# =============================================================================
# Sync Status Debug Endpoint
# =============================================================================


@router.get("/sync-status-debug")
async def api_orders_sync_status_debug(
    brand_id: str = Query(..., description="Brand ID (UUID)"),
    org_id: Optional[str] = Query(None, description="Organization ID (UUID)"),
    db: AsyncSession = Depends(get_db),
):
    """
    Lightweight admin/debug endpoint that returns a comprehensive snapshot of
    the current sync state for a brand without requiring multiple API calls.

    Includes:
    - Current sync state and active run details
    - Progress (pages, cursor, records/sec)
    - Order counts by sync_status
    - Dead-letter count
    - Last completed sync details
    - Failure category breakdown
    - Health determination with warnings and errors
    """
    from datetime import timedelta
    from services.sync_run_manager import detect_stalled
    from services.leaflink_sync import categorize_sync_failure as _categorize
    from sqlalchemy import text as _text

    now = datetime.now(timezone.utc)
    timestamp = now.isoformat()
    logger.info("[OrdersAPI] sync_status_debug brand_id=%s org_id=%s", brand_id, org_id)

    # ------------------------------------------------------------------ #
    # Active sync run                                                      #
    # ------------------------------------------------------------------ #
    try:
        active_run = await SyncRun.get_active_run(db, brand_id)
    except Exception:
        await db.rollback()
        active_run = None

    # ------------------------------------------------------------------ #
    # Last completed sync run                                              #
    # ------------------------------------------------------------------ #
    try:
        last_completed = await SyncRun.get_last_completed_run(db, brand_id)
    except Exception:
        await db.rollback()
        last_completed = None

    # ------------------------------------------------------------------ #
    # Determine current state                                              #
    # ------------------------------------------------------------------ #
    is_stalled = detect_stalled(active_run) if active_run else False

    if active_run and not is_stalled:
        current_state = "syncing"
    elif is_stalled:
        current_state = "stalled"
    elif last_completed:
        current_state = "completed"
    elif active_run and active_run.status == "failed":
        current_state = "failed"
    else:
        current_state = "idle"

    # ------------------------------------------------------------------ #
    # Sync state block                                                     #
    # ------------------------------------------------------------------ #
    active_run_id = active_run.id if active_run else None
    started_at = None
    elapsed_seconds = None
    estimated_completion_seconds = None

    if active_run:
        started_at = active_run.started_at.isoformat() if active_run.started_at else None
        if active_run.started_at:
            elapsed_seconds = round((now - active_run.started_at).total_seconds(), 1)

        # Estimate completion: remaining pages × avg seconds/page
        if (
            active_run.status == "syncing"
            and not is_stalled
            and active_run.total_pages
            and active_run.pages_synced
            and elapsed_seconds
            and elapsed_seconds > 0
        ):
            avg_secs_per_page = elapsed_seconds / active_run.pages_synced
            remaining_pages = max(0, active_run.total_pages - active_run.pages_synced)
            estimated_completion_seconds = round(remaining_pages * avg_secs_per_page, 1)

    sync_state = {
        "current_state": current_state,
        "active_sync_run_id": active_run_id,
        "started_at": started_at,
        "elapsed_seconds": elapsed_seconds,
        "estimated_completion_seconds": estimated_completion_seconds,
    }

    # ------------------------------------------------------------------ #
    # Progress block — use active run if present, else last completed      #
    # ------------------------------------------------------------------ #
    ref_run = active_run or last_completed
    pages_processed = ref_run.pages_synced if ref_run else 0
    records_processed = ref_run.orders_loaded_this_run if ref_run else 0
    current_page = ref_run.current_page if ref_run else None
    current_cursor = ref_run.current_cursor if ref_run else None
    total_pages_estimate = ref_run.total_pages if ref_run else None

    records_per_second = None
    if ref_run and ref_run.started_at:
        ref_elapsed = None
        if active_run and active_run.started_at:
            ref_elapsed = (now - active_run.started_at).total_seconds()
        elif last_completed and last_completed.started_at and last_completed.completed_at:
            ref_elapsed = (last_completed.completed_at - last_completed.started_at).total_seconds()
        if ref_elapsed and ref_elapsed > 0 and records_processed:
            records_per_second = round(records_processed / ref_elapsed, 1)

    progress = {
        "pages_processed": pages_processed,
        "records_processed": records_processed,
        "records_per_second": records_per_second,
        "current_page": current_page,
        "current_cursor": current_cursor,
        "total_pages_estimate": total_pages_estimate,
    }

    # ------------------------------------------------------------------ #
    # Order counts by sync_status                                          #
    # ------------------------------------------------------------------ #
    try:
        total_res = await db.execute(
            select(func.count(Order.id)).where(Order.brand_id == brand_id)
        )
        total_local_orders = total_res.scalar_one() or 0
    except Exception:
        await db.rollback()
        total_local_orders = 0

    try:
        ok_res = await db.execute(
            select(func.count(Order.id)).where(
                Order.brand_id == brand_id,
                Order.sync_status == "ok",
            )
        )
        total_ok = ok_res.scalar_one() or 0
    except Exception:
        await db.rollback()
        total_ok = 0

    try:
        partial_res = await db.execute(
            select(func.count(Order.id)).where(
                Order.brand_id == brand_id,
                Order.sync_status == "partial",
            )
        )
        total_partial = partial_res.scalar_one() or 0
    except Exception:
        await db.rollback()
        total_partial = 0

    try:
        failed_res = await db.execute(
            select(func.count(Order.id)).where(
                Order.brand_id == brand_id,
                Order.sync_status == "failed",
            )
        )
        total_failed = failed_res.scalar_one() or 0
    except Exception:
        await db.rollback()
        total_failed = 0

    # ------------------------------------------------------------------ #
    # Dead-letter count                                                    #
    # ------------------------------------------------------------------ #
    try:
        dl_res = await db.execute(
            _text(
                "SELECT COUNT(*) FROM sync_dead_letters "
                "WHERE brand_id = :brand_id AND resolved_at IS NULL"
            ),
            {"brand_id": brand_id},
        )
        dead_letter_count = dl_res.scalar() or 0
    except Exception:
        await db.rollback()
        dead_letter_count = 0

    counts = {
        "total_local_orders": total_local_orders,
        "total_ok": total_ok,
        "total_partial": total_partial,
        "total_failed": total_failed,
        "dead_letter_count": dead_letter_count,
    }

    # ------------------------------------------------------------------ #
    # Last sync details (from last completed run)                          #
    # ------------------------------------------------------------------ #
    last_sync: dict = {}
    if last_completed:
        lc_duration = None
        if last_completed.started_at and last_completed.completed_at:
            lc_duration = round(
                (last_completed.completed_at - last_completed.started_at).total_seconds(), 1
            )
        last_sync = {
            "completed_at": last_completed.completed_at.isoformat() if last_completed.completed_at else None,
            "duration_seconds": lc_duration,
            "orders_synced": last_completed.orders_loaded_this_run or 0,
            # SyncRun does not track inserted/updated/failed separately;
            # use orders_loaded_this_run as synced and derive best-effort values
            # from current order counts as a proxy.
            "orders_inserted": None,
            "orders_updated": None,
            "orders_failed": total_failed,
        }

    # ------------------------------------------------------------------ #
    # Failure category breakdown from dead-letter error messages           #
    # ------------------------------------------------------------------ #
    _all_categories = [
        "duplicate_key", "missing_field", "malformed", "line_item_issue",
        "fk_issue", "timeout", "rate_limit", "validation", "unknown",
    ]
    failure_summary: dict[str, int] = {cat: 0 for cat in _all_categories}

    try:
        fc_res = await db.execute(
            _text(
                "SELECT error_message FROM sync_dead_letters "
                "WHERE brand_id = :brand_id AND resolved_at IS NULL "
                "LIMIT 500"
            ),
            {"brand_id": brand_id},
        )
        for (err_msg,) in fc_res.fetchall():
            try:
                cat = _categorize(Exception(err_msg or ""))
                failure_summary[cat] = failure_summary.get(cat, 0) + 1
            except Exception:
                failure_summary["unknown"] = failure_summary.get("unknown", 0) + 1
    except Exception:
        await db.rollback()

    # ------------------------------------------------------------------ #
    # Health determination                                                 #
    # ------------------------------------------------------------------ #
    warnings: list[str] = []
    errors: list[str] = []

    # Dead-letter thresholds
    if dead_letter_count > 200:
        errors.append(f"Dead-letter count critically high: {dead_letter_count} (threshold: 200)")
    elif dead_letter_count > 50:
        warnings.append(f"Dead-letter count elevated: {dead_letter_count} (threshold: 50)")

    # Failed order thresholds
    if total_failed > 200:
        errors.append(f"Failed order count critically high: {total_failed} (threshold: 200)")
    elif total_failed > 50:
        warnings.append(f"Failed order count elevated: {total_failed} (threshold: 50)")

    # Stall detection
    if is_stalled and active_run and active_run.last_progress_at:
        stall_seconds = round((now - active_run.last_progress_at).total_seconds(), 1)
        if stall_seconds > 300:
            errors.append(f"Sync stalled for {stall_seconds}s (threshold: 300s)")
        elif stall_seconds > 90:
            warnings.append(f"Sync stalled for {stall_seconds}s (threshold: 90s)")

    # Last successful sync recency
    if last_completed is None:
        errors.append("No successful sync has ever completed for this brand")
    elif last_completed.completed_at:
        age_seconds = (now - last_completed.completed_at).total_seconds()
        if age_seconds > 3600:
            warnings.append(
                f"Last successful sync completed {round(age_seconds / 60, 1)} minutes ago (threshold: 60 min)"
            )

    is_healthy = len(errors) == 0

    health = {
        "is_healthy": is_healthy,
        "warnings": warnings,
        "errors": errors,
    }

    logger.info(
        "[OrdersAPI] sync_status_debug brand_id=%s state=%s elapsed=%s pages=%s records=%s "
        "dead_letter=%s failed=%s healthy=%s warnings=%s errors=%s",
        brand_id,
        current_state,
        elapsed_seconds,
        pages_processed,
        records_processed,
        dead_letter_count,
        total_failed,
        is_healthy,
        len(warnings),
        len(errors),
    )

    return make_json_safe({
        "ok": True,
        "brand_id": brand_id,
        "org_id": org_id,
        "timestamp": timestamp,
        "sync_state": sync_state,
        "progress": progress,
        "counts": counts,
        "last_sync": last_sync,
        "failure_summary": failure_summary,
        "health": health,
    })


# =============================================================================
# Runtime Health Endpoint — lightweight, never performs heavy work
# =============================================================================


@router.get("/runtime-health")
async def api_orders_runtime_health(
    brand_id: str = Query(..., description="Brand ID (UUID)"),
    db: AsyncSession = Depends(get_db),
):
    """
    Lightweight runtime health check for the orders subsystem.

    Returns immediately (<100ms) by reading from sync_metrics_snapshots cache
    and a single lightweight SyncRun query.  Never performs full DB scans,
    dead-letter aggregations, or any heavy computation inline.

    Response fields:
    - worker_active:          True if a sync run is currently queued or syncing
    - queue_depth:            Number of pending SyncRequest rows
    - sync_running:           True if status is "syncing"
    - db_latency_ms:          Round-trip time for a trivial DB query (ms)
    - avg_processing_rate:    Records per second from last snapshot
    - dead_letter_count:      Cached dead-letter count from snapshot
    - memory_usage_mb:        Process RSS memory in MB (if available)
    - current_sync_run_id:    Active SyncRun ID (or null)
    - last_successful_insert: Timestamp of last snapshot update
    - snapshot_age_seconds:   How old the cached snapshot is
    - total_local_orders:     Cached order count from snapshot
    """
    import os as _os
    import time as _time
    from sqlalchemy import text as _text_rh

    timestamp = datetime.now(timezone.utc).isoformat()
    logger.info("[OrdersAPI] runtime_health brand_id=%s", brand_id)

    # ------------------------------------------------------------------ #
    # DB latency probe — trivial query to measure round-trip time          #
    # ------------------------------------------------------------------ #
    db_latency_ms: Optional[float] = None
    try:
        _t0 = _time.monotonic()
        await db.execute(_text_rh("SELECT 1"))
        db_latency_ms = round((_time.monotonic() - _t0) * 1000, 1)
    except Exception:
        pass

    # ------------------------------------------------------------------ #
    # Read snapshot cache (single indexed query)                           #
    # ------------------------------------------------------------------ #
    snapshot = None
    try:
        snap_result = await db.execute(
            _text_rh(
                "SELECT total_local_orders, dead_letter_count, sync_rate, "
                "last_successful_sync_at, updated_at, sync_run_id "
                "FROM sync_metrics_snapshots "
                "WHERE brand_id = :brand_id "
                "ORDER BY updated_at DESC NULLS LAST "
                "LIMIT 1"
            ),
            {"brand_id": brand_id},
        )
        snapshot = snap_result.fetchone()
    except Exception as snap_exc:
        logger.info(
            "[OrdersAPI] runtime_health snapshot_miss brand_id=%s reason=%s",
            brand_id,
            str(snap_exc)[:100],
        )
        try:
            await db.rollback()
        except Exception:
            pass

    # ------------------------------------------------------------------ #
    # Lightweight SyncRun query                                            #
    # ------------------------------------------------------------------ #
    active_run = None
    try:
        active_run = await SyncRun.get_active_run(db, brand_id)
    except Exception:
        await db.rollback()

    # ------------------------------------------------------------------ #
    # Queue depth — count pending SyncRequest rows for this brand          #
    # ------------------------------------------------------------------ #
    queue_depth = 0
    try:
        qd_result = await db.execute(
            select(func.count(SyncRequest.id)).where(
                SyncRequest.brand_id == brand_id,
                SyncRequest.status == "pending",
            )
        )
        queue_depth = qd_result.scalar_one() or 0
    except Exception:
        await db.rollback()

    # ------------------------------------------------------------------ #
    # Memory usage (best-effort, not available in all environments)        #
    # ------------------------------------------------------------------ #
    memory_usage_mb: Optional[float] = None
    try:
        import resource as _resource
        _usage = _resource.getrusage(_resource.RUSAGE_SELF)
        # ru_maxrss is in KB on Linux, bytes on macOS
        import sys as _sys
        if _sys.platform == "darwin":
            memory_usage_mb = round(_usage.ru_maxrss / (1024 * 1024), 1)
        else:
            memory_usage_mb = round(_usage.ru_maxrss / 1024, 1)
    except Exception:
        pass

    # ------------------------------------------------------------------ #
    # Populate from snapshot                                               #
    # ------------------------------------------------------------------ #
    total_local_orders = None
    dead_letter_count = None
    avg_processing_rate = None
    last_successful_insert = None
    snapshot_age_seconds = None
    snap_run_id = None

    if snapshot:
        total_local_orders = snapshot[0]
        dead_letter_count = snapshot[1]
        avg_processing_rate = float(snapshot[2]) if snapshot[2] is not None else None
        last_successful_insert = (
            snapshot[3].isoformat()
            if snapshot[3] and hasattr(snapshot[3], "isoformat")
            else None
        )
        snap_updated_at = snapshot[4]
        snap_run_id = snapshot[5]
        if snap_updated_at:
            try:
                _snap_dt = snap_updated_at if snap_updated_at.tzinfo else snap_updated_at.replace(tzinfo=timezone.utc)
                snapshot_age_seconds = round((datetime.now(timezone.utc) - _snap_dt).total_seconds(), 1)
            except Exception:
                pass

    worker_active = active_run is not None
    sync_running = active_run is not None and active_run.status == "syncing"
    current_sync_run_id = active_run.id if active_run else (
        int(snap_run_id) if snap_run_id and snap_run_id.isdigit() else None
    )

    return make_json_safe({
        "ok": True,
        "brand_id": brand_id,
        "worker_active": worker_active,
        "queue_depth": queue_depth,
        "sync_running": sync_running,
        "db_latency_ms": db_latency_ms,
        "avg_processing_rate": avg_processing_rate,
        "dead_letter_count": dead_letter_count,
        "memory_usage_mb": memory_usage_mb,
        "current_sync_run_id": current_sync_run_id,
        "last_successful_insert": last_successful_insert,
        "snapshot_age_seconds": snapshot_age_seconds,
        "total_local_orders": total_local_orders,
        "timestamp": timestamp,
    })


# =============================================================================
# Dynamic routes — MUST be registered AFTER all static routes to prevent
# /{order_id} from shadowing paths like /sync-status, /queues, /sync-metrics.
# =============================================================================


@router.get("/db-debug")
async def orders_db_debug(
    brand_id: Optional[str] = Query(None, description="Optional brand_id filter"),
    db: AsyncSession = Depends(get_db),
):
    """
    Diagnostic endpoint: raw database state for orders, order_lines, and sync_dead_letters.

    Returns counts by org_id and brand_id so operators can identify whether
    orders are being inserted with org_id=NULL (which makes them invisible to
    GET /orders, which filters by org_id).

    Does NOT require X-OPSYN-ORG — intentionally unscoped for diagnostics.

    Returns:
        {
            "ok": true,
            "raw_counts": { "total_orders", "total_order_lines", "total_dead_letters" },
            "counts_by_org_id": [{"org_id": "...", "count": N}],
            "counts_by_brand_id": [{"brand_id": "...", "count": N}],
            "latest_inserted_row": { "id", "org_id", "brand_id", "external_order_id", "created_at" },
            "latest_committed_timestamp": "...",
            "sync_metrics_snapshot": {...}
        }
    """
    from sqlalchemy import text as _text_debug

    timestamp = datetime.now(timezone.utc).isoformat()

    try:
        # 1. Raw total counts
        _total_orders_res = await db.execute(_text_debug("SELECT COUNT(*) FROM orders"))
        total_orders = _total_orders_res.scalar() or 0

        _total_lines_res = await db.execute(_text_debug("SELECT COUNT(*) FROM order_lines"))
        total_order_lines = _total_lines_res.scalar() or 0

        _total_dl_res = await db.execute(_text_debug("SELECT COUNT(*) FROM sync_dead_letters"))
        total_dead_letters = _total_dl_res.scalar() or 0

        # 2. Counts by org_id (NULL org_id is the key diagnostic signal)
        _by_org_query = "SELECT org_id::text, COUNT(*) as cnt FROM orders GROUP BY org_id ORDER BY cnt DESC LIMIT 20"
        if brand_id:
            _by_org_query = (
                "SELECT org_id::text, COUNT(*) as cnt FROM orders "
                "WHERE brand_id = :brand_id GROUP BY org_id ORDER BY cnt DESC LIMIT 20"
            )
        _by_org_res = await db.execute(
            _text_debug(_by_org_query),
            {"brand_id": brand_id} if brand_id else {},
        )
        counts_by_org_id = [
            {"org_id": row[0], "count": row[1]}
            for row in _by_org_res.fetchall()
        ]

        # 3. Counts by brand_id
        _by_brand_query = "SELECT brand_id::text, COUNT(*) as cnt FROM orders GROUP BY brand_id ORDER BY cnt DESC LIMIT 20"
        _by_brand_res = await db.execute(_text_debug(_by_brand_query))
        counts_by_brand_id = [
            {"brand_id": row[0], "count": row[1]}
            for row in _by_brand_res.fetchall()
        ]

        # 4. Latest inserted row
        _latest_query = (
            "SELECT id, org_id::text, brand_id::text, external_order_id, created_at "
            "FROM orders ORDER BY id DESC LIMIT 1"
        )
        if brand_id:
            _latest_query = (
                "SELECT id, org_id::text, brand_id::text, external_order_id, created_at "
                "FROM orders WHERE brand_id = :brand_id ORDER BY id DESC LIMIT 1"
            )
        _latest_res = await db.execute(
            _text_debug(_latest_query),
            {"brand_id": brand_id} if brand_id else {},
        )
        _latest_row = _latest_res.fetchone()
        latest_inserted_row = None
        latest_committed_timestamp = None
        if _latest_row:
            latest_inserted_row = {
                "id": _latest_row[0],
                "org_id": _latest_row[1],
                "brand_id": _latest_row[2],
                "external_order_id": _latest_row[3],
                "created_at": _latest_row[4].isoformat() if _latest_row[4] and hasattr(_latest_row[4], "isoformat") else str(_latest_row[4]),
            }
            latest_committed_timestamp = latest_inserted_row["created_at"]

        # 5. Null org_id count (critical diagnostic)
        _null_org_query = "SELECT COUNT(*) FROM orders WHERE org_id IS NULL"
        if brand_id:
            _null_org_query = "SELECT COUNT(*) FROM orders WHERE org_id IS NULL AND brand_id = :brand_id"
        _null_org_res = await db.execute(
            _text_debug(_null_org_query),
            {"brand_id": brand_id} if brand_id else {},
        )
        null_org_id_count = _null_org_res.scalar() or 0

        # 6. Sync metrics snapshot (latest per brand)
        _snap_query = (
            "SELECT brand_id::text, sync_run_id, total_local_orders, pages_processed, "
            "records_processed, updated_at "
            "FROM sync_metrics_snapshots ORDER BY updated_at DESC LIMIT 5"
        )
        if brand_id:
            _snap_query = (
                "SELECT brand_id::text, sync_run_id, total_local_orders, pages_processed, "
                "records_processed, updated_at "
                "FROM sync_metrics_snapshots WHERE brand_id = :brand_id "
                "ORDER BY updated_at DESC LIMIT 5"
            )
        try:
            _snap_res = await db.execute(
                _text_debug(_snap_query),
                {"brand_id": brand_id} if brand_id else {},
            )
            sync_metrics_snapshot = [
                {
                    "brand_id": row[0],
                    "sync_run_id": row[1],
                    "total_local_orders": row[2],
                    "pages_processed": row[3],
                    "records_processed": row[4],
                    "updated_at": row[5].isoformat() if row[5] and hasattr(row[5], "isoformat") else str(row[5]),
                }
                for row in _snap_res.fetchall()
            ]
        except Exception as _snap_exc:
            sync_metrics_snapshot = {"error": str(_snap_exc)[:200]}

        logger.info(
            "[OrdersAPI][DB_DEBUG] total_orders=%s null_org_id=%s brand_id_filter=%s"
            " counts_by_org=%s",
            total_orders,
            null_org_id_count,
            brand_id,
            counts_by_org_id,
        )

        return make_json_safe({
            "ok": True,
            "brand_id_filter": brand_id,
            "timestamp": timestamp,
            "raw_counts": {
                "total_orders": total_orders,
                "total_order_lines": total_order_lines,
                "total_dead_letters": total_dead_letters,
            },
            "null_org_id_count": null_org_id_count,
            "counts_by_org_id": counts_by_org_id,
            "counts_by_brand_id": counts_by_brand_id,
            "latest_inserted_row": latest_inserted_row,
            "latest_committed_timestamp": latest_committed_timestamp,
            "sync_metrics_snapshot": sync_metrics_snapshot,
            "diagnosis": (
                "CRITICAL: orders have org_id=NULL — they are invisible to GET /orders "
                "which filters by org_id. Trigger a new sync with X-OPSYN-ORG header set."
                if null_org_id_count > 0 and null_org_id_count == total_orders
                else (
                    f"WARNING: {null_org_id_count} of {total_orders} orders have org_id=NULL"
                    if null_org_id_count > 0
                    else "OK: all orders have org_id set"
                )
            ),
        })

    except Exception as exc:
        logger.error("[OrdersAPI][DB_DEBUG] error=%s", str(exc)[:300], exc_info=True)
        try:
            await db.rollback()
        except Exception:
            pass
        return make_json_safe({
            "ok": False,
            "error": str(exc)[:300],
            "timestamp": timestamp,
        })


@router.post("/backfill-org-ids")
async def backfill_org_ids(
    db: AsyncSession = Depends(get_db),
):
    """
    Repair historical NULL org_id orders by joining with brands table.

    UPDATE orders
    SET org_id = (
        SELECT org_id FROM brands WHERE brands.id = orders.brand_id
    )
    WHERE org_id IS NULL;

    Returns:
    - updated_count: number of rows updated
    - remaining_null_count: number of rows still NULL after update
    - sample_rows: up to 5 sample rows that were updated
    """
    from sqlalchemy import text as _text_backfill

    timestamp = datetime.now(timezone.utc).isoformat()
    logger.info("[OrdersAPI] backfill_org_ids start")

    try:
        # Count rows with NULL org_id before update
        _before_res = await db.execute(
            _text_backfill("SELECT COUNT(*) FROM orders WHERE org_id IS NULL")
        )
        null_count_before = _before_res.scalar() or 0

        if null_count_before == 0:
            logger.info("[OrdersAPI] backfill_org_ids no_null_rows_found")
            return make_json_safe({
                "ok": True,
                "updated_count": 0,
                "remaining_null_count": 0,
                "null_count_before": 0,
                "sample_rows": [],
                "message": "No orders with NULL org_id found — nothing to backfill",
                "timestamp": timestamp,
            })

        # Capture sample rows before update (for reporting)
        _sample_before_res = await db.execute(
            _text_backfill(
                "SELECT o.id, o.brand_id::text, o.external_order_id, o.created_at, "
                "b.org_id::text AS brand_org_id "
                "FROM orders o "
                "LEFT JOIN brands b ON CAST(b.id AS text) = CAST(o.brand_id AS text) "
                "WHERE o.org_id IS NULL "
                "LIMIT 5"
            )
        )
        sample_before = [
            {
                "id": row[0],
                "brand_id": row[1],
                "external_order_id": row[2],
                "created_at": row[3].isoformat() if row[3] and hasattr(row[3], "isoformat") else str(row[3]),
                "brand_org_id": row[4],
            }
            for row in _sample_before_res.fetchall()
        ]

        # Perform the backfill UPDATE
        _update_res = await db.execute(
            _text_backfill(
                "UPDATE orders "
                "SET org_id = ( "
                "    SELECT org_id FROM brands "
                "    WHERE CAST(brands.id AS text) = CAST(orders.brand_id AS text) "
                ") "
                "WHERE org_id IS NULL "
                "AND EXISTS ( "
                "    SELECT 1 FROM brands "
                "    WHERE CAST(brands.id AS text) = CAST(orders.brand_id AS text) "
                "    AND brands.org_id IS NOT NULL "
                ")"
            )
        )
        updated_count = _update_res.rowcount if hasattr(_update_res, "rowcount") else 0

        await db.commit()

        # Count remaining NULL rows after update
        _after_res = await db.execute(
            _text_backfill("SELECT COUNT(*) FROM orders WHERE org_id IS NULL")
        )
        remaining_null_count = _after_res.scalar() or 0

        logger.info(
            "[OrdersAPI] backfill_org_ids complete null_before=%s updated=%s remaining_null=%s",
            null_count_before,
            updated_count,
            remaining_null_count,
        )

        return make_json_safe({
            "ok": True,
            "null_count_before": null_count_before,
            "updated_count": updated_count,
            "remaining_null_count": remaining_null_count,
            "sample_rows": sample_before,
            "message": (
                f"Backfilled {updated_count} orders with org_id from brands table. "
                f"{remaining_null_count} rows still have NULL org_id "
                f"(brands table may be missing org_id for those brand_ids)."
                if remaining_null_count > 0
                else f"Successfully backfilled {updated_count} orders. All orders now have org_id set."
            ),
            "timestamp": timestamp,
        })

    except Exception as exc:
        logger.error("[OrdersAPI] backfill_org_ids error=%s", str(exc)[:300], exc_info=True)
        try:
            await db.rollback()
        except Exception:
            pass
        return make_json_safe({
            "ok": False,
            "error": str(exc)[:300],
            "timestamp": timestamp,
        })


@router.post("/{order_id}/resync")
async def api_order_resync(
    order_id: str,
    x_opsyn_org: str | None = Header(None, description="Organization ID (UUID or org_code)"),
    brand_id: str = Query(..., description="Brand ID (UUID)"),
    db: AsyncSession = Depends(get_db),
):
    """
    Resync a single order from LeafLink by internal order ID.

    Fetches the order from LeafLink by its external_order_id and re-upserts it.
    Returns full order detail with sync_health.
    """
    from leaflink.orders import serialize_order, build_line_items, compute_sync_health, _DEFER_OPTS as _ll_defer_opts_resync
    from services.credential_resolver import resolve_brand_credential
    from services.leaflink_sync import sync_leaflink_orders_headers_only
    from sqlalchemy.orm import selectinload as _selectinload

    timestamp = datetime.now(timezone.utc).isoformat()
    logger.info("[OrdersAPI] order_resync order_id=%s brand_id=%s", order_id, brand_id)

    # Fetch the order from DB
    try:
        result = await db.execute(
            select(Order)
            .options(_selectinload(Order.lines), *_ll_defer_opts_resync)
            .where(Order.id == order_id, Order.brand_id == brand_id)
        )
        order = result.scalar_one_or_none()
    except Exception as exc:
        await db.rollback()
        return make_json_safe({"ok": False, "error": str(exc)[:200], "timestamp": timestamp})

    if not order:
        return make_json_safe({"ok": False, "error": "Order not found", "timestamp": timestamp})

    external_order_id = order.external_order_id

    # Resolve credential
    cred_row = await resolve_brand_credential(db, brand_id, "leaflink")
    if not cred_row:
        return make_json_safe({
            "ok": False,
            "error": "leaflink_credential_not_found",
            "brand_id": brand_id,
            "timestamp": timestamp,
        })

    api_key = cred_row[4]
    company_id = cred_row[3]
    auth_scheme = cred_row[9] if len(cred_row) > 9 and cred_row[9] else "Token"
    base_url = cred_row[8] if len(cred_row) > 8 else None
    resolved_brand_id = cred_row[1]

    # Fetch the specific order from LeafLink
    try:
        from services.leaflink_client import LeafLinkClient
        client = LeafLinkClient(
            api_key=api_key,
            company_id=company_id,
            brand_id=resolved_brand_id,
            auth_scheme=auth_scheme,
            base_url=base_url,
        )

        # Fetch the order by external_order_id using offset-based lookup
        loop = asyncio.get_event_loop()
        fetch_result = await asyncio.wait_for(
            loop.run_in_executor(
                _leaflink_executor,
                lambda: client.fetch_orders_page_range(
                    start_page=1,
                    num_pages=1,
                    page_size=1,
                    normalize=True,
                    brand=resolved_brand_id,
                ),
            ),
            timeout=30,
        )

        # Try to find the specific order in the result
        # If not found in first page, we'll re-upsert from what we have in DB
        orders_from_leaflink = fetch_result.get("orders", [])
        target_order = None
        for o in orders_from_leaflink:
            if str(o.get("external_id") or o.get("id", "")) == str(external_order_id):
                target_order = o
                break

        if target_order:
            # Re-upsert the specific order — resolve org_id from header if available
            _resync_org_id: Optional[str] = None
            if x_opsyn_org:
                try:
                    from services.organization_service import lookup_organization as _lookup_org_resync
                    _resync_org_res = await _lookup_org_resync(db, x_opsyn_org)
                    if _resync_org_res.get("ok"):
                        _resync_org_id = str(_resync_org_res["organization"].id)
                except Exception:
                    pass
            await sync_leaflink_orders_headers_only(
                brand_id=resolved_brand_id,
                orders=[target_order],
                pages_fetched=1,
                org_id=_resync_org_id,
            )
            logger.info(
                "[OrdersAPI] order_resync_from_leaflink order_id=%s external_id=%s",
                order_id,
                external_order_id,
            )
        else:
            logger.info(
                "[OrdersAPI] order_resync_not_in_first_page order_id=%s external_id=%s"
                " — refreshing from DB only",
                order_id,
                external_order_id,
            )

    except Exception as fetch_exc:
        logger.warning(
            "[OrdersAPI] order_resync_fetch_error order_id=%s error=%s",
            order_id,
            str(fetch_exc)[:200],
        )
        # Continue — return current DB state even if LeafLink fetch failed

    # Re-fetch the order from DB after potential resync
    try:
        result2 = await db.execute(
            select(Order)
            .options(_selectinload(Order.lines), *_ll_defer_opts_resync)
            .where(Order.id == order_id, Order.brand_id == brand_id)
        )
        order = result2.scalar_one_or_none() or order
    except Exception:
        await db.rollback()

    line_items = build_line_items(order)
    sync_health = compute_sync_health(order, line_items)
    serialized = serialize_order(order)

    return make_json_safe({
        "ok": True,
        "brand_id": brand_id,
        "order": serialized,
        "sync_health": sync_health,
        "timestamp": timestamp,
    })


@router.get("/pagination-debug")
async def pagination_debug(
    brand_id: str = Query(..., description="Brand ID (UUID)"),
    db: AsyncSession = Depends(get_db),
):
    """
    Diagnostic endpoint for pagination issues.

    Returns:
    - latest sync run id
    - LeafLink reported count
    - last offset/page processed
    - has_next/next_url_present
    - records_seen
    - local DB count
    - gap_to_leaflink_count
    - completion_percent
    - reason if stopped early
    """
    # Query latest SyncRun for this brand
    result = await db.execute(
        select(SyncRun)
        .where(SyncRun.brand_id == brand_id)
        .order_by(SyncRun.started_at.desc())
        .limit(1)
    )
    latest_run = result.scalar_one_or_none()

    # Count local orders for this brand
    local_count_result = await db.execute(
        select(func.count(Order.id))
        .where(Order.brand_id == brand_id)
    )
    local_count = local_count_result.scalar() or 0

    if not latest_run:
        return {
            "ok": False,
            "error": "No sync runs found for this brand",
            "brand_id": brand_id,
            "local_db_count": local_count,
        }

    leaflink_total = latest_run.total_orders_available or 0
    pages_done = latest_run.current_page or 0
    records_seen = pages_done * 100
    gap = leaflink_total - local_count
    completion_percent = (records_seen / leaflink_total * 100) if leaflink_total > 0 else 0
    last_next_url = getattr(latest_run, "last_next_url", None)

    # Persistence metrics
    orders_loaded = latest_run.orders_loaded_this_run or 0
    persistence_ratio = (orders_loaded / leaflink_total) if leaflink_total > 0 else 0
    visibility_ratio = (local_count / leaflink_total) if leaflink_total > 0 else 0
    loaded_vs_visible_gap = orders_loaded - local_count

    if persistence_ratio > 0.95:
        persistence_health = "OK"
    elif persistence_ratio > 0.80:
        persistence_health = "DEGRADED"
    else:
        persistence_health = "CRITICAL"

    return {
        "ok": True,
        "brand_id": brand_id,
        "latest_sync_run_id": str(latest_run.id),
        "leaflink_reported_count": leaflink_total,
        "last_page_processed": pages_done,
        "has_next": last_next_url is not None,
        "next_url_present": last_next_url is not None,
        "records_seen_from_leaflink": records_seen,
        "local_db_count": local_count,
        "gap_to_leaflink_count": gap,
        "completion_percent": round(completion_percent, 1),
        "sync_status": latest_run.status,
        "stopped_early_reason": latest_run.last_error if latest_run.status in ("incomplete", "stalled", "failed") else None,
        "pages_synced": latest_run.pages_synced,
        "orders_loaded_this_run": orders_loaded,
        "started_at": latest_run.started_at.isoformat() if latest_run.started_at else None,
        "completed_at": latest_run.completed_at.isoformat() if latest_run.completed_at else None,
        "last_progress_at": latest_run.last_progress_at.isoformat() if latest_run.last_progress_at else None,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        # Persistence health metrics
        "persistence_ratio": round(persistence_ratio * 100, 1),
        "visibility_ratio": round(visibility_ratio * 100, 1),
        "loaded_vs_visible_gap": loaded_vs_visible_gap,
        "persistence_health": persistence_health,
    }


@router.get("/orders-health")
async def orders_health_check(
    brand_id: Optional[str] = Query(None, description="Brand ID to scope health check (optional)"),
    db: AsyncSession = Depends(get_db),
):
    """
    Health check for the orders system.

    Returns aggregate counts, newest order dates, order_lines count,
    orders-without-lines count, and a sync_status of 'current' or 'stale'.

    Logs a [ORDERS_HEALTH] line with all key metrics for easy grep.
    """
    from sqlalchemy import text as _text_health

    try:
        # Build brand filter clause
        brand_clause = "WHERE brand_id = :brand_id" if brand_id else ""
        brand_params: dict = {"brand_id": brand_id} if brand_id else {}

        # Aggregate order stats
        orders_sql = _text_health(f"""
            SELECT
                COUNT(*) AS total_orders,
                MAX(external_created_at) AS newest_external_created_at,
                MAX(synced_at) AS newest_synced_at,
                (
                    SELECT external_order_id FROM orders
                    {brand_clause}
                    ORDER BY external_created_at DESC NULLS LAST
                    LIMIT 1
                ) AS newest_external_order_id
            FROM orders
            {brand_clause}
        """)
        orders_result = await db.execute(orders_sql, brand_params)
        row = orders_result.fetchone()

        total_orders = row[0] if row else 0
        newest_external_created_at = row[1] if row else None
        newest_synced_at = row[2] if row else None
        newest_external_order_id = row[3] if row else None

        # Order lines count
        lines_sql = _text_health(
            "SELECT COUNT(*) FROM order_lines ol "
            + ("JOIN orders o ON ol.order_id = o.id WHERE o.brand_id = :brand_id" if brand_id else "")
        )
        lines_result = await db.execute(lines_sql, brand_params)
        order_lines_count = lines_result.scalar() or 0

        # Orders without any lines
        no_lines_sql = _text_health(
            "SELECT COUNT(*) FROM orders o "
            + ("WHERE o.brand_id = :brand_id AND " if brand_id else "WHERE ")
            + "NOT EXISTS (SELECT 1 FROM order_lines ol WHERE ol.order_id = o.id)"
        )
        no_lines_result = await db.execute(no_lines_sql, brand_params)
        orders_without_lines_count = no_lines_result.scalar() or 0

        # Determine sync staleness (stale if newest order is > 1 day old)
        now = datetime.now(timezone.utc)
        is_stale = True
        if newest_external_created_at is not None:
            _newest = (
                newest_external_created_at
                if newest_external_created_at.tzinfo is not None
                else newest_external_created_at.replace(tzinfo=timezone.utc)
            )
            is_stale = (now - _newest).days > 1

        sync_status = "stale" if is_stale else "current"

        logger.info(
            "[ORDERS_HEALTH] brand_id=%s total_orders=%s newest_external_created_at=%s "
            "newest_synced_at=%s order_lines_count=%s orders_without_lines=%s sync_status=%s",
            brand_id or "all",
            total_orders,
            newest_external_created_at,
            newest_synced_at,
            order_lines_count,
            orders_without_lines_count,
            sync_status,
        )

        return {
            "ok": True,
            "brand_id": brand_id,
            "total_orders": total_orders,
            "newest_external_created_at": newest_external_created_at.isoformat() if newest_external_created_at else None,
            "newest_synced_at": newest_synced_at.isoformat() if newest_synced_at else None,
            "newest_external_order_id": newest_external_order_id,
            "order_lines_count": order_lines_count,
            "orders_without_lines_count": orders_without_lines_count,
            "sync_status": sync_status,
            "timestamp": now.isoformat(),
        }

    except Exception as exc:
        logger.error("[ORDERS_HEALTH] error brand_id=%s error=%s", brand_id, str(exc)[:300], exc_info=True)
        try:
            await db.rollback()
        except Exception:
            pass
        return {"ok": False, "error": str(exc)[:300]}


async def _resolve_order_identifier_for_lines(
    db: AsyncSession,
    order_identifier: str,
) -> Optional[Order]:
    """
    Resolve an order identifier to an Order ORM object.

    Tries in order:
    1. Exact match on Order.id (internal UUID)
    2. Exact match on Order.external_order_id (LeafLink UUID)
    3. Short suffix match — last 8 chars of external_order_id (e.g. '28b6e3a4')

    Returns the Order with a synthetic `.matched_by` attribute set to
    'internal_id', 'external_order_id', or 'short_id' for logging.
    Returns None if no match found.
    """
    # 1. Try internal UUID exact match
    try:
        result = await db.execute(
            select(Order).where(Order.id == order_identifier)
        )
        order = result.scalar_one_or_none()
        if order is not None:
            order.matched_by = "internal_id"  # type: ignore[attr-defined]
            return order
    except Exception:
        await db.rollback()

    # 2. Try external_order_id exact match
    try:
        result = await db.execute(
            select(Order).where(Order.external_order_id == order_identifier)
        )
        order = result.scalar_one_or_none()
        if order is not None:
            order.matched_by = "external_order_id"  # type: ignore[attr-defined]
            return order
    except Exception:
        await db.rollback()

    # 3. Try short suffix match (last 8 chars of external_order_id)
    if len(order_identifier) <= 8:
        try:
            from sqlalchemy import text as _text_suffix
            result = await db.execute(
                _text_suffix(
                    "SELECT id FROM orders WHERE external_order_id LIKE :suffix LIMIT 2"
                ),
                {"suffix": f"%{order_identifier}"},
            )
            rows = result.fetchall()
            if len(rows) == 1:
                # Unique match — fetch the full ORM object
                matched_id = rows[0][0]
                orm_result = await db.execute(
                    select(Order).where(Order.id == matched_id)
                )
                order = orm_result.scalar_one_or_none()
                if order is not None:
                    order.matched_by = "short_id"  # type: ignore[attr-defined]
                    return order
            elif len(rows) > 1:
                logger.warning(
                    "[ORDER_LINES_RESOLVE] short_id_ambiguous order_identifier=%s matched_count=%d",
                    order_identifier,
                    len(rows),
                )
        except Exception:
            await db.rollback()

    return None


@router.get("/{order_identifier}/line-items")
async def get_order_line_items(
    order_identifier: str,
    brand_id: Optional[str] = Query(None, description="Brand ID for tenant scoping (optional but recommended)"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get line items for an order by any identifier format.

    Accepts:
    - Internal DB order UUID (Order.id)
    - external_order_id (LeafLink UUID, e.g. '9cc48b36-9b35-477a-8e66-780028b6e3a4')
    - Short display suffix (last 8 chars, e.g. '28b6e3a4')

    Returns a list of line item objects with sku, product_name, quantity,
    unit_price, line_total, product_id, brand_id, and category.

    Logs [ORDER_LINES_REQUEST], [ORDER_LINES_RESOLVE], [ORDER_LINES_RESULT],
    and [ORDER_LINES_NOT_FOUND] for easy grep/audit.
    """
    from sqlalchemy.orm import selectinload as _selectinload_lines

    logger.info("[ORDER_LINES_REQUEST] order_identifier=%s brand_id=%s", order_identifier, brand_id)

    # Resolve identifier to internal Order
    order = await _resolve_order_identifier_for_lines(db, order_identifier)

    if not order:
        logger.warning("[ORDER_LINES_NOT_FOUND] order_identifier=%s brand_id=%s", order_identifier, brand_id)
        raise HTTPException(status_code=404, detail="Order not found")

    matched_by = getattr(order, "matched_by", "unknown")
    logger.info(
        "[ORDER_LINES_RESOLVE] matched_by=%s order_db_id=%s external_order_id=%s brand_id=%s",
        matched_by,
        order.id,
        order.external_order_id,
        order.brand_id,
    )

    # Optional brand scoping — reject if order belongs to a different brand
    if brand_id and order.brand_id and str(order.brand_id) != str(brand_id):
        logger.warning(
            "[ORDER_LINES_BRAND_MISMATCH] order_identifier=%s order_brand_id=%s requested_brand_id=%s",
            order_identifier,
            order.brand_id,
            brand_id,
        )
        raise HTTPException(status_code=404, detail="Order not found")

    # Query order_lines with correct join: orders.id -> order_lines.order_id
    try:
        lines_result = await db.execute(
            select(OrderLine)
            .where(OrderLine.order_id == order.id)
            .order_by(OrderLine.created_at)
        )
        lines = lines_result.scalars().all()
    except Exception as exc:
        logger.error(
            "[ORDER_LINES_QUERY_ERROR] order_db_id=%s error=%s",
            order.id,
            str(exc)[:300],
            exc_info=True,
        )
        await db.rollback()
        raise HTTPException(status_code=500, detail="Failed to query order lines")

    logger.info(
        "[ORDER_LINES_RESULT] order_db_id=%s external_order_id=%s count=%d",
        order.id,
        order.external_order_id,
        len(lines),
    )

    return make_json_safe([
        {
            "id": line.id,
            "sku": line.sku,
            "product_name": line.product_name,
            "quantity": line.quantity,
            "unit_price": float(line.unit_price) if line.unit_price is not None else None,
            "line_total": float(line.total_price) if line.total_price is not None else None,
            "product_id": getattr(line, "mapped_product_id", None),
            "brand_id": order.brand_id,
            "category": getattr(line, "category", None),
        }
        for line in lines
    ])


@router.get("/{order_id}")
async def get_order(
    order_id: str,
    x_opsyn_org: str | None = Header(None, description="Organization ID (UUID or org_code)"),
    brand_id: str = Query(..., description="Brand ID (UUID)"),
    db: AsyncSession = Depends(get_db),
):
    """
    Fetch a single order by its internal ID for a brand.

    Requires:
    - X-OPSYN-ORG header: Organization ID (UUID or org_code like \"noble\")
    - brand_id query param: Brand ID (UUID)
    - order_id path param: Internal order ID (string)

    Returns the full order including all dispatch and AR fields.
    """
    from leaflink.orders import build_line_items, derive_blockers, derive_review_status, money_to_float, cents_to_amount, _DEFER_OPTS as _ll_defer_opts
    from services.organization_service import lookup_organization
    from sqlalchemy import and_
    from sqlalchemy.orm import selectinload as _selectinload

    if not x_opsyn_org:
        logger.warning("[OrdersAPI] get_order missing_org_id order_id=%s", order_id)
        return {"ok": False, "error": "X-OPSYN-ORG header is required"}

    if not brand_id:
        logger.warning("[OrdersAPI] get_order missing_brand_id order_id=%s", order_id)
        return {"ok": False, "error": "brand_id query parameter is required"}

    logger.info(
        "[OrdersAPI] get_order request org_id=%s brand_id=%s order_id=%s",
        x_opsyn_org,
        brand_id,
        order_id,
    )

    # Resolve org_id (supports UUID or org_code)
    org_lookup = await lookup_organization(db, x_opsyn_org)
    if not org_lookup.get("ok"):
        logger.warning("[OrdersAPI] get_order org_lookup_failed error=%s", org_lookup.get("error"))
        return {"ok": False, "error": org_lookup.get("error")}

    resolved_org = org_lookup.get("organization")
    resolved_org_id = str(resolved_org.id)

    # Validate brand exists and belongs to org
    from models.auth_models import Brand
    brand_result = await db.execute(
        select(Brand).where(
            and_(
                Brand.id == cast(brand_id, PG_UUID(as_uuid=False)),
                Brand.org_id == cast(resolved_org_id, PG_UUID(as_uuid=False)),
            )
        )
    )
    brand = brand_result.scalar_one_or_none()

    if not brand:
        logger.warning(
            "[OrdersAPI] get_order brand_not_found brand_id=%s org_id=%s",
            brand_id,
            resolved_org_id,
        )
        return {"ok": False, "error": "Brand not found or does not belong to organization"}

    # Fetch the order scoped by org_id AND brand_id for tenant isolation
    try:
        result = await db.execute(
            select(Order)
            .options(_selectinload(Order.lines), *_ll_defer_opts)
            .where(
                and_(
                    Order.id == order_id,
                    Order.org_id == resolved_org_id,  # VARCHAR(120) — no UUID cast
                    Order.brand_id == brand_id,
                )
            )
        )
        order = result.scalar_one_or_none()
    except Exception as exc:
        logger.error(
            "[OrdersAPI] get_order db_query_failed order_id=%s org_id=%s brand_id=%s error=%s",
            order_id,
            resolved_org_id,
            brand_id,
            exc,
            exc_info=True,
        )
        return {"ok": False, "error": "Database query failed"}

    if not order:
        logger.info(
            "[OrdersAPI] get_order not_found order_id=%s org_id=%s brand_id=%s",
            order_id,
            resolved_org_id,
            brand_id,
        )
        return {"ok": False, "error": "Order not found"}

    # Serialize the order with all fields including dispatch and AR
    line_items = build_line_items(order)

    amount = money_to_float(order.amount)
    if amount is None and order.total_cents is not None:
        amount = cents_to_amount(order.total_cents)

    item_count = order.item_count if order.item_count is not None else len(line_items)
    unit_count = order.unit_count if order.unit_count is not None else sum((item.get("quantity") or 0) for item in line_items)

    blockers = derive_blockers(line_items)
    review_status = derive_review_status(line_items, blockers, order)

    logger.info(
        "[OrdersAPI] get_order found order_id=%s external_id=%s delivery_status=%s payment_status=%s",
        order_id,
        order.external_order_id,
        _safe_col(order, "delivery_status", "pending"),
        _safe_col(order, "payment_status", "unpaid"),
    )

    return make_json_safe({
        "ok": True,
        "org_id": resolved_org_id,
        "brand_id": brand_id,
        "order": {
            "id": order.id,
            "external_id": order.external_order_id,
            "order_number": order.order_number,
            "customer_name": order.customer_name,
            "status": order.status,
            "amount": amount,
            "item_count": item_count,
            "unit_count": unit_count,
            "line_items": line_items,
            "review_status": review_status,
            "blockers": blockers,
            "sync_status": order.sync_status or "ok",
            "last_synced_at": order.last_synced_at or order.synced_at,
            "source": order.source,
            "external_created_at": order.external_created_at,
            "external_updated_at": order.external_updated_at,
            "created_at": order.created_at,
            "updated_at": order.updated_at,
            # Dispatch fields — guarded with _safe_col() in case the
            # migration adding these columns hasn't run yet.
            "assigned_driver_id": _safe_col(order, "assigned_driver_id"),
            "assigned_driver_name": _safe_col(order, "assigned_driver_name"),
            "delivery_status": _safe_col(order, "delivery_status", "pending"),
            "delivery_date": _safe_col(order, "delivery_date"),
            "route_number": _safe_col(order, "route_number"),
            "route_id": _safe_col(order, "route_id"),
            "driver_note": _safe_col(order, "driver_note"),
            "delivery_instructions": _safe_col(order, "delivery_instructions"),
            # AR fields — same guard.
            "payment_status": _safe_col(order, "payment_status", "unpaid"),
            "amount_paid": float(_safe_col(order, "amount_paid", 0) or 0),
            "balance_due": float(_safe_col(order, "balance_due", 0) or 0),
            "due_date": _safe_col(order, "due_date"),
            "days_overdue": _safe_col(order, "days_overdue", 0),
            "invoice_number": _safe_col(order, "invoice_number"),
            "ar_note": _safe_col(order, "ar_note"),
        },
    })
