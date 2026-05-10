import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import defer as _defer, selectinload

from database import get_db
from models import Order, OrderLine

logger = logging.getLogger("leaflink_orders")
router = APIRouter()


def _ensure_utc(dt: Any) -> "datetime | None":
    """Normalize any datetime value to UTC-aware.

    Handles None, ISO strings, naive datetimes, and aware datetimes.
    Returns a UTC-aware datetime or None — never a naive datetime.
    This prevents 'can't subtract offset-naive and offset-aware datetimes'
    errors when mixing datetime sources (DB, API, server clock).
    """
    if dt is None:
        return None
    if isinstance(dt, str):
        if not dt:
            return None
        try:
            dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return None
    if not isinstance(dt, datetime):
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


# Columns that may not exist in the database yet (pending migration).
# Deferring them keeps them out of the SELECT list so the query succeeds
# even before the migration runs.
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
_DEFER_OPTS = [
    _defer(getattr(Order, col))
    for col in _DISPATCH_AR_COLS
    if hasattr(Order, col)
]


def _safe_col(obj: object, attr: str, default=None):
    """Safely read a possibly-deferred or missing ORM column.

    Catches both ``AttributeError`` (column not on model) and any
    SQLAlchemy lazy-load error (``MissingGreenlet``) that occurs when
    accessing a deferred column in an async context.
    """
    try:
        return getattr(obj, attr, default)
    except Exception:
        return default


def money_to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def safe_money_to_float(value: Any, field_name: str = "amount") -> tuple[float | None, str | None]:
    """Fault-tolerant money parser — returns (value, sync_health_note).

    Returns (float, None) on success.
    Returns (0.0, note) when the value is present but unparseable.
    Returns (None, None) when the value is absent.

    This prevents a single bad money field from killing the entire order.
    """
    if value is None or value == "":
        return None, None
    try:
        return float(value), None
    except (TypeError, ValueError):
        note = f"{field_name}_invalid_value"
        logger.warning(
            "[SAFE_MONEY_FALLBACK] field=%s value=%r — defaulting to 0",
            field_name,
            str(value)[:50],
        )
        return 0.0, note


def safe_timestamp(value: Any, field_name: str = "timestamp") -> tuple["datetime | None", str | None]:
    """Fault-tolerant timestamp parser — returns (datetime, sync_health_note).

    Returns (datetime, None) on success.
    Returns (None, note) when the value is present but unparseable.
    Returns (None, None) when the value is absent.

    This prevents a single bad timestamp from killing the entire order.
    """
    if value is None or value == "":
        return None, None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc), None
        return value.astimezone(timezone.utc), None
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc), None
        except (ValueError, TypeError):
            note = f"{field_name}_invalid_timestamp"
            logger.warning(
                "[SAFE_TIMESTAMP_FALLBACK] field=%s value=%r — defaulting to None",
                field_name,
                str(value)[:50],
            )
            return None, note
    note = f"{field_name}_unexpected_type"
    logger.warning(
        "[SAFE_TIMESTAMP_FALLBACK] field=%s type=%s — defaulting to None",
        field_name,
        type(value).__name__,
    )
    return None, note


def cents_to_amount(cents: int | None) -> float | None:
    if cents is None:
        return None
    return round(cents / 100.0, 2)


def serialize_line(line: OrderLine) -> dict[str, Any]:
    unit_price = money_to_float(line.unit_price)
    total_price = money_to_float(line.total_price)

    if unit_price is None and line.unit_price_cents is not None:
        unit_price = cents_to_amount(line.unit_price_cents)

    if total_price is None and line.total_price_cents is not None:
        total_price = cents_to_amount(line.total_price_cents)

    return {
        "id": line.id,
        "sku": line.sku,
        "product_name": line.product_name,
        "quantity": line.quantity or 0,
        "unit_price": unit_price,
        "total_price": total_price,
        "mapped_product_id": line.mapped_product_id,
        "mapping_status": line.mapping_status or "unknown",
        "mapping_issue": line.mapping_issue,
    }


def serialize_json_line(item: dict[str, Any]) -> dict[str, Any]:
    quantity = item.get("quantity") or item.get("qty") or 0
    unit_price = item.get("unit_price")
    total_price = item.get("total_price")

    if unit_price is None and item.get("unit_price_cents") is not None:
        unit_price = cents_to_amount(item.get("unit_price_cents"))

    if total_price is None and item.get("total_price_cents") is not None:
        total_price = cents_to_amount(item.get("total_price_cents"))

    return {
        "id": None,
        "sku": item.get("sku"),
        "product_name": item.get("product_name") or item.get("name"),
        "quantity": quantity,
        "unit_price": unit_price,
        "total_price": total_price,
        "mapped_product_id": item.get("mapped_product_id"),
        "mapping_status": item.get("mapping_status", "unknown"),
        "mapping_issue": item.get("mapping_issue"),
    }


def build_line_items(order: Order) -> list[dict[str, Any]]:
    # Try OrderLine table first (most reliable)
    if getattr(order, "lines", None):
        return [serialize_line(line) for line in order.lines]

    # Try line_items_json (normalized data from sync)
    raw = order.line_items_json
    if isinstance(raw, list):
        return [serialize_json_line(item) for item in raw if isinstance(item, dict)]

    if isinstance(raw, dict):
        nested = raw.get("line_items")
        if isinstance(nested, list):
            return [serialize_json_line(item) for item in nested if isinstance(item, dict)]

    # Fallback: extract from raw_payload if line_items_json is empty
    if order.raw_payload and isinstance(order.raw_payload, dict):
        raw_payload = order.raw_payload
        # Try multiple field names
        candidate = (
            raw_payload.get("line_items")
            or raw_payload.get("items")
            or raw_payload.get("order_items")
            or raw_payload.get("products")
            or raw_payload.get("ordered_items")
            or []
        )
        if isinstance(candidate, list):
            items = [serialize_json_line(item) for item in candidate if isinstance(item, dict)]
            if items:
                logger.info(
                    "leaflink: build_line_items fallback_from_raw_payload order_id=%s count=%s",
                    order.id,
                    len(items),
                )
                return items

    return []


def derive_blockers(line_items: list[dict[str, Any]]) -> list[dict[str, str]]:
    blockers: list[dict[str, str]] = []

    unknown_lines = [
        item for item in line_items
        if (
            not item.get("sku")
            or item.get("mapping_status") in {"unknown", "unmapped", None}
            or item.get("mapping_issue")
        )
    ]

    if unknown_lines:
        blockers.append({
            "type": "mapping_issue",
            "message": "Unknown SKU",
        })

    return blockers


def derive_review_status(line_items: list[dict[str, Any]], blockers: list[dict[str, str]], order: Order) -> str:
    """Derive review status for an order.

    Rules (in priority order):
    1. If order.review_status is already set, use it.
    2. If there are blockers (unknown SKU, mapping issues), return "blocked".
    3. If line_items is empty AND the order sync_status is "partial" or "failed",
       return "needs_review" (real issue — child data missing).
    4. If line_items is empty but sync_status is "ok", return "ok" (not yet synced
       or genuinely has no line items — not a blocking issue).
    5. Otherwise return "ready".
    """
    if order.review_status:
        return order.review_status
    if blockers:
        return "blocked"
    if not line_items:
        # Only flag as needs_review if there's a real sync problem
        order_sync_status = getattr(order, "sync_status", "ok") or "ok"
        if order_sync_status in ("partial", "failed"):
            return "needs_review"
        # No line items but sync is ok — not a blocking issue
        return "ok"
    return "ready"


def compute_sync_health(order: Order, line_items: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute a sync_health object for an order.

    An order is considered:
    - "ok"      — all required data is present and sync succeeded
    - "partial" — header saved but some child data is missing (line items, totals, etc.)
    - "failed"  — sync explicitly failed for this order

    Fault-tolerant: any exception reading order attributes is caught and
    treated as a missing field rather than propagating to the caller.

    Returns a dict with:
        status:          "ok" | "partial" | "failed"
        missing_fields:  list of field names that are missing/empty
        last_synced_at:  datetime of last sync attempt
        last_error:      error string or null
        notes:           list of sync_health notes (e.g. invalid field values)
    """
    try:
        sync_status = getattr(order, "sync_status", "ok") or "ok"
    except Exception:
        sync_status = "ok"

    try:
        last_synced_at = _ensure_utc(
            getattr(order, "last_synced_at", None) or getattr(order, "synced_at", None)
        )
    except Exception:
        last_synced_at = None

    # Determine missing required fields — each check is individually guarded
    missing_fields: list[str] = []
    notes: list[str] = []

    try:
        customer_name = order.customer_name
        if not customer_name or customer_name == "Unknown Customer":
            missing_fields.append("customer_name")
    except Exception:
        missing_fields.append("customer_name")

    try:
        # Check for invalid money values stored as 0 due to safe_money_to_float fallback
        amount = order.amount
        total_cents = order.total_cents
        if amount is None and total_cents is None:
            missing_fields.append("amount")
        elif amount == 0 and total_cents == 0:
            # Could be a legitimate $0 order or a fallback — note it but don't block
            notes.append("amount_may_be_fallback")
    except Exception:
        missing_fields.append("amount")

    try:
        if not line_items:
            missing_fields.append("line_items")
    except Exception:
        missing_fields.append("line_items")

    try:
        if not order.status:
            missing_fields.append("status")
    except Exception:
        missing_fields.append("status")

    # Merge any sync_health_missing_fields already stored on the order
    try:
        stored_missing = getattr(order, "sync_health_missing_fields", None)
        if isinstance(stored_missing, list):
            for f in stored_missing:
                if f not in missing_fields:
                    missing_fields.append(f)
    except Exception:
        pass

    # Determine health status
    if sync_status == "failed":
        health_status = "failed"
    elif sync_status == "partial" or missing_fields:
        health_status = "partial"
    else:
        health_status = "ok"

    # Get last_error from sync_health_last_error if available, else None
    try:
        last_error = getattr(order, "sync_health_last_error", None)
    except Exception:
        last_error = None

    result = {
        "status": health_status,
        "missing_fields": missing_fields,
        "last_synced_at": last_synced_at,
        "last_error": last_error,
    }
    if notes:
        result["notes"] = notes
    return result


def is_needs_review(order: Order, line_items: list[dict[str, Any]], blockers: list[dict[str, str]]) -> bool:
    """Return True if an order should appear in the Needs Review queue.

    An order needs review ONLY if it has a real blocking issue:
    - sync_health.status is "partial" or "failed"
    - has blockers (unknown SKU, mapping issues)
    - missing required data (customer, line items, totals)
    - order has invalid/unknown status
    """
    sync_status = getattr(order, "sync_status", "ok") or "ok"
    if sync_status in ("partial", "failed"):
        return True
    if blockers:
        return True
    if not order.customer_name or order.customer_name == "Unknown Customer":
        return True
    if order.amount is None and order.total_cents is None:
        return True
    if not line_items and sync_status not in ("ok",):
        return True
    return False


def is_driver_queue(order: Order) -> bool:
    """Return True if an order should appear in the Driver Queue.

    Driver Queue includes orders that:
    - status is approved/ready for delivery (confirmed, approved, accepted)
    - no driver assigned (assigned_driver_id is null)
    - not blocked/cancelled/completed
    - delivery_status is pending
    """
    status = (order.status or "").lower()
    delivery_status = _safe_col(order, "delivery_status", "pending") or "pending"
    assigned_driver_id = _safe_col(order, "assigned_driver_id", None)

    # Only include orders that are ready for dispatch
    ready_statuses = {"confirmed", "approved", "accepted", "ready", "packed", "fulfillment"}
    if status not in ready_statuses:
        return False

    # Must not have a driver assigned
    if assigned_driver_id:
        return False

    # Must not be cancelled or completed
    if delivery_status in ("delivered", "cancelled", "failed"):
        return False

    return True


def is_ar_queue(order: Order) -> bool:
    """Return True if an order should appear in the AR Queue.

    AR Queue includes orders that:
    - status is delivered/completed OR invoice-relevant
    - unpaid/overdue/balance_due > 0
    - not $0 due
    """
    status = (order.status or "").lower()
    payment_status = _safe_col(order, "payment_status", "unpaid") or "unpaid"
    balance_due = float(_safe_col(order, "balance_due", 0) or 0)
    delivery_status = _safe_col(order, "delivery_status", "pending") or "pending"

    # Only include delivered/completed orders or invoice-relevant statuses
    ar_statuses = {"delivered", "completed", "invoiced", "confirmed", "approved"}
    if status not in ar_statuses and delivery_status != "delivered":
        return False

    # Must have a balance due
    if balance_due <= 0:
        return False

    # Must not be fully paid
    if payment_status == "paid":
        return False

    return True


def serialize_order(order: Order) -> dict[str, Any]:
    """Serialize a single Order ORM object to a JSON-safe dict.

    Reusable across endpoints — call this on each order in a paginated
    result set rather than loading all orders into memory at once.

    All datetime fields are normalized to UTC-aware via _ensure_utc() so
    callers never receive a mix of naive and aware datetimes.

    Fault-tolerant: individual field failures fall back to safe defaults
    rather than propagating exceptions to the caller.
    """
    # Build line items — fault-tolerant (returns [] on any error)
    try:
        line_items = build_line_items(order)
    except Exception as _li_exc:
        logger.warning(
            "[SERIALIZE_ORDER_FALLBACK] build_line_items failed order_id=%s error=%s",
            getattr(order, "id", "unknown"),
            str(_li_exc)[:100],
        )
        line_items = []

    # Amount — fault-tolerant with safe_money_to_float
    try:
        amount = money_to_float(order.amount)
        if amount is None and order.total_cents is not None:
            amount = cents_to_amount(order.total_cents)
    except Exception:
        amount = None

    # Item/unit counts — fault-tolerant
    try:
        item_count = order.item_count
        if item_count is None:
            item_count = len(line_items)
    except Exception:
        item_count = len(line_items)

    try:
        unit_count = order.unit_count
        if unit_count is None:
            unit_count = sum((item.get("quantity") or 0) for item in line_items)
    except Exception:
        unit_count = 0

    blockers = derive_blockers(line_items)
    review_status = derive_review_status(line_items, blockers, order)
    sync_health = compute_sync_health(order, line_items)

    # Normalize all datetime fields to UTC-aware before returning.
    # Each field is individually guarded to prevent a single bad timestamp
    # from killing the entire serialization.
    try:
        last_synced_at = _ensure_utc(order.last_synced_at or order.synced_at)
    except Exception:
        last_synced_at = None
    try:
        external_created_at = _ensure_utc(order.external_created_at)
    except Exception:
        external_created_at = None
    try:
        external_updated_at = _ensure_utc(order.external_updated_at)
    except Exception:
        external_updated_at = None
    try:
        created_at = _ensure_utc(order.created_at)
    except Exception:
        created_at = None
    try:
        updated_at = _ensure_utc(order.updated_at)
    except Exception:
        updated_at = None

    # Safe scalar fields — fall back to None/defaults on any error
    try:
        order_id = order.id
    except Exception:
        order_id = None
    try:
        external_id = order.external_order_id
    except Exception:
        external_id = None
    try:
        order_number = order.order_number
    except Exception:
        order_number = None
    try:
        customer_name = order.customer_name or "Unknown Customer"
    except Exception:
        customer_name = "Unknown Customer"
    try:
        status = order.status
    except Exception:
        status = None
    try:
        sync_status = order.sync_status or "ok"
    except Exception:
        sync_status = "ok"
    try:
        source = order.source
    except Exception:
        source = "leaflink"

    return {
        "id": order_id,
        "external_id": external_id,
        "order_number": order_number,
        "customer_name": customer_name,
        "status": status,
        "amount": amount,
        "item_count": item_count,
        "unit_count": unit_count,
        "line_items": line_items,
        "review_status": review_status,
        "blockers": blockers,
        "sync_status": sync_status,
        "sync_health": sync_health,
        "last_synced_at": last_synced_at,
        "source": source,
        "external_created_at": external_created_at,
        "external_updated_at": external_updated_at,
        "created_at": created_at,
        "updated_at": updated_at,
        # Dispatch fields — use _safe_col() to handle deferred columns
        # that may not exist in the database yet (pending migration).
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
    }


@router.get("/orders")
async def get_orders(db: AsyncSession = Depends(get_db)):
    logger.info("[OrdersAPI] request endpoint=/leaflink/orders")
    logger.info("leaflink: get_orders request start")

    # ------------------------------------------------------------------ #
    # Diagnostics: log actual PostgreSQL column types for orders /        #
    # order_lines so type mismatches are visible in logs before the query #
    # ------------------------------------------------------------------ #
    try:
        from sqlalchemy import text as _text_diag
        _col_type_res = await db.execute(
            _text_diag(
                """
                SELECT table_name, column_name, data_type, udt_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name IN ('orders', 'order_lines')
                  AND column_name IN ('id', 'order_id')
                ORDER BY table_name, column_name
                """
            )
        )
        _col_rows = _col_type_res.fetchall()
        for _row in _col_rows:
            logger.info(
                "[OrdersAPI][SCHEMA_DIAG] table=%s column=%s pg_type=%s udt=%s "
                "sqlalchemy_model_type=%s",
                _row[0],
                _row[1],
                _row[2],
                _row[3],
                (
                    type(Order.id.property.columns[0].type).__name__
                    if _row[0] == "orders" and _row[1] == "id"
                    else type(OrderLine.order_id.property.columns[0].type).__name__
                    if _row[0] == "order_lines" and _row[1] == "order_id"
                    else "n/a"
                ),
            )
    except Exception as _diag_exc:
        logger.warning(
            "[OrdersAPI][SCHEMA_DIAG] schema_diagnostic_failed error=%s",
            str(_diag_exc)[:200],
        )

    try:
        logger.info("leaflink: get_orders db_query_start")
        _query = (
            select(Order)
            .options(selectinload(Order.lines), *_DEFER_OPTS)
            .order_by(Order.external_updated_at.desc().nullslast(), Order.updated_at.desc())
        )
        logger.info(
            "[OrdersAPI][SQL_DEBUG] compiled_query=%s",
            str(_query.compile(compile_kwargs={"literal_binds": False}))[:500],
        )
        result = await db.execute(_query)
        orders = result.scalars().all()
    except Exception as exc:
        logger.error(
            "leaflink: get_orders db_query_failed error=%s",
            exc,
            exc_info=True,
        )
        # Roll back the failed transaction so the session is reusable
        try:
            await db.rollback()
        except Exception:
            pass
        return {
            "ok": False,
            "success": False,
            "error": "Database query failed — type mismatch or schema error",
            "detail": str(exc)[:500],
            "hint": (
                "Check that Order.id and OrderLine.order_id SQLAlchemy types "
                "match the actual PostgreSQL column types (UUID vs INTEGER). "
                "See [OrdersAPI][SCHEMA_DIAG] log lines for column type details."
            ),
        }


    # Count total orders in DB (all brands)
    try:
        total_count_result = await db.execute(select(func.count(Order.id)))
        total_in_database = total_count_result.scalar_one() or 0
    except Exception:
        total_in_database = len(orders)

    results: list[dict[str, Any]] = []
    review_status_counts: dict[str, int] = {}

    # Track freshness dates across all orders
    newest_order_date: datetime | None = None
    oldest_order_date: datetime | None = None

    for order in orders:
        serialized = serialize_order(order)
        review_status = serialized["review_status"]
        review_status_counts[review_status] = review_status_counts.get(review_status, 0) + 1

        # Track newest/oldest order dates
        order_date = order.external_updated_at or order.external_created_at or order.updated_at
        if order_date is not None:
            # Normalize to UTC-aware before comparison to avoid offset-naive vs
            # offset-aware TypeError when mixing datetime sources.
            if order_date.tzinfo is None:
                order_date = order_date.replace(tzinfo=timezone.utc)
            else:
                order_date = order_date.astimezone(timezone.utc)
            if newest_order_date is None or order_date > newest_order_date:
                newest_order_date = order_date
            if oldest_order_date is None or order_date < oldest_order_date:
                oldest_order_date = order_date

        results.append(serialized)

    refreshed_at = datetime.now(timezone.utc).isoformat()
    newest_order_date_iso = newest_order_date.isoformat() if newest_order_date else None
    oldest_order_date_iso = oldest_order_date.isoformat() if oldest_order_date else None

    logger.info("[OrdersAPI] db_count=%s", len(results))
    logger.info("[OrdersAPI] newest_order_date=%s", newest_order_date_iso)
    logger.info("[OrdersAPI] refreshed_at=%s", refreshed_at)
    logger.info("[OrdersAPI] returning_mock=false")
    logger.info("[OrdersAPI] returning_cache=false")

    if not results:
        logger.info("leaflink: get_orders no_orders_found (not an error)")
    else:
        logger.info(
            "leaflink: get_orders results count=%s review_status_distribution=%s",
            len(results),
            review_status_counts,
        )

    return {
        "ok": True,
        "success": True,
        "count": len(results),
        "total_in_database": total_in_database,
        "newest_order_date": newest_order_date_iso,
        "oldest_order_date": oldest_order_date_iso,
        "refreshed_at": refreshed_at,
        "source": "database",
        "orders": results,
        "sync_metadata": {
            "returning_mock": False,
            "returning_cache": False,
        },
    }



@router.get("/orders/id/{order_id}")
async def get_order_detail(order_id: str, db: AsyncSession = Depends(get_db)):
    logger.info("[OrdersAPI] request endpoint=/leaflink/orders/id/%s", order_id)
    logger.info("leaflink: get_order_detail request order_id=%s", order_id)
    result = await db.execute(
        select(Order)
        .options(selectinload(Order.lines))
        .where(Order.id == order_id)
    )
    order = result.scalar_one_or_none()

    if not order:
        logger.info("leaflink: get_order_detail not_found order_id=%s", order_id)
        return {
            "ok": False,
            "success": False,
            "error": "Order not found",
        }

    logger.info(
        "leaflink: get_order_detail found order_id=%s external_id=%s status=%s",
        order_id,
        order.external_order_id,
        order.status,
    )

    line_items = build_line_items(order)

    amount = money_to_float(order.amount)
    if amount is None and order.total_cents is not None:
        amount = cents_to_amount(order.total_cents)

    item_count = order.item_count if order.item_count is not None else len(line_items)
    unit_count = order.unit_count if order.unit_count is not None else sum((item.get("quantity") or 0) for item in line_items)

    blockers = derive_blockers(line_items)
    review_status = derive_review_status(line_items, blockers, order)

    logger.info(
        "leaflink: get_order_detail serialized order_id=%s line_items=%s review_status=%s",
        order_id,
        len(line_items),
        review_status,
    )

    refreshed_at = datetime.now(timezone.utc).isoformat()
    # Normalize order_date to UTC-aware before calling .isoformat() to avoid
    # TypeError when mixing naive and aware datetimes.
    order_date = _ensure_utc(
        order.external_updated_at or order.external_created_at or order.updated_at
    )
    newest_order_date_iso = order_date.isoformat() if order_date else None

    logger.info("[OrdersAPI] db_count=1")
    logger.info("[OrdersAPI] newest_order_date=%s", newest_order_date_iso)
    logger.info("[OrdersAPI] refreshed_at=%s", refreshed_at)
    logger.info("[OrdersAPI] returning_mock=false")
    logger.info("[OrdersAPI] returning_cache=false")

    return {
        "ok": True,
        "success": True,
        "count": 1,
        "total_in_database": 1,
        "newest_order_date": newest_order_date_iso,
        "oldest_order_date": newest_order_date_iso,
        "refreshed_at": refreshed_at,
        "source": "database",
        "sync_metadata": {
            "returning_mock": False,
            "returning_cache": False,
        },
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
            "last_synced_at": _ensure_utc(order.last_synced_at or order.synced_at),
            "source": order.source,
            "external_created_at": _ensure_utc(order.external_created_at),
            "external_updated_at": _ensure_utc(order.external_updated_at),
            "created_at": _ensure_utc(order.created_at),
            "updated_at": _ensure_utc(order.updated_at),
        }
    }


@router.get("/leaflink/orders")
async def get_orders_legacy(db: AsyncSession = Depends(get_db)):
    """
    Backward-compatible route so older frontend code that still calls
    /leaflink/orders keeps working while you transition to /orders.
    """
    return await get_orders(db)