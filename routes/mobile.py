"""
Mobile API routes — optimised for iOS app consumption.

Endpoints:
  GET /api/mobile/orders   — paginated, filtered order list for a brand.
                             raw_payload excluded by default; pass
                             ?include_raw=true to opt in.

Performance notes:
  - All filtering is applied at the SQL level (WHERE clause) — no Python-side
    filtering.  This keeps memory usage flat regardless of total order count.
  - raw_payload is a large JSONB column; excluding it by default cuts
    per-row serialisation cost significantly for large result sets.
  - Uses LIMIT n+1 trick to detect has_more without a separate COUNT query.
  - Cursor-based pagination encodes offset+limit as base64 for opaque tokens.
  - Per-endpoint timing is logged with [MOBILE_*] / [ORDERS_LIST_*] markers.
"""

import base64
import logging
import time
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from utils.json_utils import make_json_safe

logger = logging.getLogger("mobile")

router = APIRouter(prefix="/api/mobile", tags=["mobile"])

# Pagination limits
ORDERS_DEFAULT_LIMIT = 50
ORDERS_MAX_LIMIT = 200

# Timeout warning threshold (milliseconds)
ORDERS_TIMEOUT_RISK_MS = 2000


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_str(value: Any, max_len: int = 500) -> Optional[str]:
    if value is None:
        return None
    return str(value)[:max_len]


def _encode_cursor(offset: int, limit: int) -> str:
    """Encode offset+limit into an opaque base64 cursor token."""
    raw = f"offset={offset}&limit={limit}"
    return base64.b64encode(raw.encode()).decode()


def _decode_cursor(cursor: str) -> Optional[tuple]:
    """Decode a cursor token back to (offset, limit). Returns None on error."""
    try:
        decoded = base64.b64decode(cursor.encode()).decode()
        parts = dict(p.split("=", 1) for p in decoded.split("&") if "=" in p)
        offset = int(parts["offset"])
        limit = int(parts["limit"])
        return offset, limit
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Core paginated orders query — shared by /api/mobile/orders and /orders
# ---------------------------------------------------------------------------

async def _query_orders_paginated(
    db: AsyncSession,
    brand_id: str,
    limit: int,
    offset: int,
    status: Optional[str] = None,
    mapping_status: Optional[str] = None,
    search: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    include_raw: bool = False,
) -> dict:
    """
    Execute a paginated, filtered orders query entirely at the SQL level.

    All filters are applied in the WHERE clause — no Python-side filtering.
    Uses LIMIT n+1 to detect has_more without a separate COUNT query.

    Returns a dict with: items, total_count, count, has_more, next_cursor,
    offset, limit, filters_applied, duration_ms.
    """
    t_start = time.monotonic()

    # Clamp limit to allowed range
    limit = max(1, min(limit, ORDERS_MAX_LIMIT))

    # Build WHERE clause fragments and params
    where_clauses = ["brand_id = :brand_id"]
    params: dict = {"brand_id": brand_id}

    filters_applied: dict = {}

    if status:
        where_clauses.append("status = :status")
        params["status"] = status
        filters_applied["status"] = status

    if mapping_status:
        where_clauses.append("sync_status = :mapping_status")
        params["mapping_status"] = mapping_status
        filters_applied["mapping_status"] = mapping_status

    if search:
        where_clauses.append(
            "(customer_name ILIKE :search OR order_number ILIKE :search)"
        )
        params["search"] = f"%{search}%"
        filters_applied["search"] = search

    if date_from:
        where_clauses.append("created_at >= :date_from")
        params["date_from"] = date_from
        filters_applied["date_from"] = date_from

    if date_to:
        where_clauses.append("created_at <= :date_to")
        params["date_to"] = date_to
        filters_applied["date_to"] = date_to

    where_sql = " AND ".join(where_clauses)

    # Conditionally include raw_payload (large JSONB — opt-in only)
    raw_col = ",\n                    raw_payload" if include_raw else ""

    # Fetch limit+1 rows to detect has_more without a COUNT query
    fetch_limit = limit + 1
    params["fetch_limit"] = fetch_limit
    params["offset"] = offset

    main_sql = text(f"""
        SELECT
            o.id,
            o.brand_id,
            o.external_order_id,
            o.order_number,
            o.customer_name,
            o.status,
            o.total_cents,
            o.amount,
            o.item_count,
            o.unit_count,
            o.line_items_json,
            o.source,
            o.review_status,
            o.sync_status,
            o.delivery_status,
            o.payment_status,
            o.external_created_at,
            o.external_updated_at,
            o.synced_at,
            o.last_synced_at,
            o.created_at,
            o.updated_at,
            COALESCE(
                (SELECT SUM(ol.quantity * ol.unit_price)
                 FROM order_lines ol
                 WHERE ol.order_id = o.id),
                0
            ) AS derived_lines_total{(",\n                    o.raw_payload") if include_raw else ""}
        FROM orders o
        WHERE {where_sql}
        ORDER BY o.external_created_at DESC NULLS LAST, o.created_at DESC, o.id DESC
        LIMIT :fetch_limit
        OFFSET :offset
    """)


    t_query = time.monotonic()
    rows_result = await db.execute(main_sql, params)
    rows = rows_result.fetchall()
    query_ms = round((time.monotonic() - t_query) * 1000)

    # Detect has_more using the extra row
    has_more = len(rows) > limit
    if has_more:
        rows = rows[:limit]

    # Always compute total_count for the brand (fast with index on brand_id)
    t_count = time.monotonic()
    count_params = {k: v for k, v in params.items() if k not in ("fetch_limit", "offset")}
    count_result = await db.execute(
        text(f"SELECT COUNT(*) FROM orders WHERE {where_sql}"),
        count_params,
    )
    total_count = count_result.scalar() or 0
    count_ms = round((time.monotonic() - t_count) * 1000)

    # Serialise rows — include derived_lines_total from the subquery
    base_keys = [
        "id", "brand_id", "external_order_id", "order_number",
        "customer_name", "status", "total_cents", "amount",
        "item_count", "unit_count", "line_items_json", "source",
        "review_status", "sync_status", "delivery_status",
        "payment_status", "external_created_at", "external_updated_at",
        "synced_at", "last_synced_at", "created_at", "updated_at",
        "derived_lines_total",
    ]
    if include_raw:
        base_keys.append("raw_payload")

    raw_items = [dict(zip(base_keys, row)) for row in rows]

    # Enrich each item with display_total and total_source
    items = []
    for raw_item in raw_items:
        item = make_json_safe(raw_item)

        # Resolve stored_total: prefer amount, fall back to total_cents / 100
        # Both `amount` and `total_cents` are stored in cents — divide by 100
        stored_total: Optional[float] = None
        _amount = raw_item.get("amount")
        _total_cents = raw_item.get("total_cents")
        if _amount is not None:
            try:
                stored_total = round(float(_amount) / 100.0, 2)
            except (TypeError, ValueError):
                stored_total = None
        if stored_total is None and _total_cents is not None:
            try:
                stored_total = round(int(_total_cents) / 100.0, 2)
            except (TypeError, ValueError):
                stored_total = None

        derived = float(raw_item.get("derived_lines_total") or 0)

        # Compute display_total and total_source
        has_stored = stored_total is not None and stored_total > 0
        has_derived = derived > 0
        if not has_stored and not has_derived:
            display_total = 0.0
            total_source = "unknown"
        elif not has_stored:
            display_total = round(derived, 2)
            total_source = "derived_lines"
        elif has_derived and stored_total < (derived * 0.5):
            # Stored total is less than 50% of derived — likely stale or wrong field
            display_total = round(derived, 2)
            total_source = "derived_lines"
        else:
            display_total = round(stored_total, 2)
            total_source = "stored"

        item["stored_total"] = stored_total
        item["derived_lines_total"] = round(derived, 2)
        item["display_total"] = display_total
        item["total_source"] = total_source
        items.append(item)

    # Build next_cursor for the next page
    next_cursor: Optional[str] = None
    if has_more:
        next_cursor = _encode_cursor(offset + limit, limit)

    elapsed_ms = round((time.monotonic() - t_start) * 1000)

    # Timeout risk warning
    if elapsed_ms > ORDERS_TIMEOUT_RISK_MS:
        logger.warning(
            "[ORDERS_LIST_TIMEOUT_RISK] brand_id=%s elapsed_ms=%d query_ms=%d "
            "count_ms=%d limit=%d offset=%d filters=%s",
            brand_id,
            elapsed_ms,
            query_ms,
            count_ms,
            limit,
            offset,
            filters_applied,
        )

    logger.info(
        "[ORDERS_LIST_RESULT] brand_id=%s returned_count=%d total_count=%d "
        "has_more=%s duration_ms=%d query_ms=%d count_ms=%d",
        brand_id,
        len(items),
        total_count,
        has_more,
        elapsed_ms,
        query_ms,
        count_ms,
    )

    return {
        "items": items,
        "total_count": total_count,
        "count": len(items),
        "has_more": has_more,
        "next_cursor": next_cursor,
        "offset": offset,
        "limit": limit,
        "filters_applied": filters_applied,
        "duration_ms": elapsed_ms,
    }


# ---------------------------------------------------------------------------
# GET /api/mobile/orders
# ---------------------------------------------------------------------------

@router.get("/orders")
async def mobile_orders(
    brand_id: str = Query(..., description="Brand identifier"),
    limit: int = Query(ORDERS_DEFAULT_LIMIT, ge=1, le=ORDERS_MAX_LIMIT, description=f"Max rows to return (default {ORDERS_DEFAULT_LIMIT}, max {ORDERS_MAX_LIMIT})"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    cursor: Optional[str] = Query(None, description="Opaque pagination cursor (overrides offset/limit when provided)"),
    status: Optional[str] = Query(None, description="Filter by order status"),
    mapping_status: Optional[str] = Query(None, description="Filter by sync/mapping status"),
    search: Optional[str] = Query(None, description="Search customer_name or order_number (case-insensitive)"),
    date_from: Optional[str] = Query(None, description="Filter orders created on or after this ISO date/datetime"),
    date_to: Optional[str] = Query(None, description="Filter orders created on or before this ISO date/datetime"),
    include_raw: bool = Query(False, description="Include raw_payload field (large — opt-in only)"),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """
    Paginated, filtered order list for a brand — optimised for mobile list display.

    All filters are applied at the database level (SQL WHERE clause).
    Never returns the full order table — always paginated with a default
    limit of 50 and a maximum of 200 rows per request.

    Supports cursor-based pagination via the `cursor` parameter (returned
    as `next_cursor` in the response).  Cursor encodes offset+limit as an
    opaque base64 token for forward-only pagination.

    Always returns: id, brand_id, external_order_id, order_number,
    customer_name, status, total_cents, amount, item_count, unit_count,
    line_items_json, source, review_status, sync_status, delivery_status,
    payment_status, external_created_at, external_updated_at, synced_at,
    last_synced_at, created_at, updated_at.

    raw_payload is excluded by default to keep response sizes small.
    Pass ?include_raw=true to include it (e.g. for debugging).
    """
    t_start = time.monotonic()

    # Decode cursor → override offset/limit if provided
    if cursor:
        decoded = _decode_cursor(cursor)
        if decoded is None:
            return {
                "ok": False,
                "brand_id": brand_id,
                "error": "Invalid cursor format",
                "timestamp": _utc_now_iso(),
            }
        offset, limit = decoded

    logger.info(
        "[ORDERS_LIST_REQUEST] endpoint=/api/mobile/orders brand_id=%s limit=%d "
        "offset=%d cursor=%s filters={status=%s mapping_status=%s search=%s "
        "date_from=%s date_to=%s}",
        brand_id,
        limit,
        offset,
        bool(cursor),
        status,
        mapping_status,
        bool(search),
        date_from,
        date_to,
    )

    try:
        result = await _query_orders_paginated(
            db=db,
            brand_id=brand_id,
            limit=limit,
            offset=offset,
            status=status,
            mapping_status=mapping_status,
            search=search,
            date_from=date_from,
            date_to=date_to,
            include_raw=include_raw,
        )

        elapsed_ms = round((time.monotonic() - t_start) * 1000)

        return {
            "ok": True,
            "brand_id": brand_id,
            "items": result["items"],
            # Backward-compat alias
            "orders": result["items"],
            "total_count": result["total_count"],
            "count": result["count"],
            "limit": result["limit"],
            "offset": result["offset"],
            "has_more": result["has_more"],
            "next_cursor": result["next_cursor"],
            "filters_applied": result["filters_applied"],
            "include_raw": include_raw,
            "duration_ms": result["duration_ms"],
            "timestamp": _utc_now_iso(),
        }

    except Exception as exc:
        elapsed_ms = round((time.monotonic() - t_start) * 1000)
        logger.error(
            "[MOBILE_ERROR] endpoint=orders brand_id=%s elapsed_ms=%d error=%s",
            brand_id,
            elapsed_ms,
            str(exc)[:300],
            exc_info=True,
        )
        return {
            "ok": False,
            "brand_id": brand_id,
            "items": [],
            "orders": [],
            "total_count": 0,
            "count": 0,
            "limit": limit,
            "offset": offset,
            "has_more": False,
            "next_cursor": None,
            "filters_applied": {},
            "include_raw": include_raw,
            "error": str(exc)[:300],
            "timestamp": _utc_now_iso(),
        }
