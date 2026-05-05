import asyncio
import hashlib
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Optional
from uuid import UUID

from sqlalchemy import delete, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from database import AsyncSessionLocal, get_schema_column_types, has_column
from models import Order, OrderLine
from utils.json_utils import make_json_safe

if TYPE_CHECKING:
    from services.background_sync_manager import BackgroundSyncManager

logger = logging.getLogger("leaflink_sync")


def _cursor_hash(cursor: Optional[str]) -> Optional[str]:
    """Return a short hash of a cursor for safe logging."""
    if not cursor:
        return None
    return hashlib.sha256(cursor.encode()).hexdigest()[:12]

# Thread pool for running synchronous LeafLink HTTP calls without blocking the event loop
_leaflink_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="leaflink-bg-sync")

# Number of order headers committed per batch in Phase 1
HEADER_BATCH_SIZE = 25


def utc_now() -> datetime:
    """Return current time as timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


def parse_dt(val: Any) -> datetime | None:
    """Parse an ISO datetime string to a datetime object, or pass through if already datetime."""
    if isinstance(val, str):
        try:
            return datetime.fromisoformat(val.replace("Z", "+00:00"))
        except Exception:
            return None
    if isinstance(val, datetime):
        return val
    return None


def normalize_datetime(dt: Any) -> datetime | None:
    """Strictly enforce UTC-aware datetime, handling all input types.

    ALWAYS returns either a timezone-aware UTC datetime or None.
    Never returns naive datetimes.
    """
    if dt is None or dt == "":
        return None

    # Handle ISO datetime strings
    if isinstance(dt, str):
        try:
            # Parse ISO string (handles Z suffix and +00:00)
            parsed = datetime.fromisoformat(dt.replace("Z", "+00:00"))
            # Ensure UTC-aware
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except (ValueError, TypeError):
            return None

    # Handle datetime objects
    if isinstance(dt, datetime):
        # If naive, assume UTC and make aware
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        # If aware, convert to UTC
        return dt.astimezone(timezone.utc)

    # Unsupported type
    return None


def safe_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def safe_uuid(value: str | None) -> str | None:
    """Cast string to UUID and back to ensure DB compatibility.

    Validates that the value is a well-formed UUID and returns it in canonical
    lowercase hyphenated form.  If the value is not a valid UUID it is returned
    as-is so that non-UUID brand/org identifiers still work.
    """
    if not value:
        return None
    try:
        return str(UUID(value))
    except (ValueError, TypeError):
        return value  # Return as-is if not a valid UUID


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def safe_decimal(value: Any) -> Decimal | None:
    try:
        if value is None or value == "":
            return None
        return Decimal(str(value))
    except Exception:
        return None


def decimal_to_cents(value: Decimal | None) -> int | None:
    if value is None:
        return None
    return int((value * 100).quantize(Decimal("1")))


def normalize_line_items(raw_line_items: Any) -> list[dict[str, Any]]:
    if not raw_line_items:
        return []

    if isinstance(raw_line_items, dict):
        nested = raw_line_items.get("line_items")
        if isinstance(nested, list):
            raw_line_items = nested
        else:
            return []

    if not isinstance(raw_line_items, list):
        return []

    normalized: list[dict[str, Any]] = []

    for item in raw_line_items:
        if not isinstance(item, dict):
            continue

        sku = safe_str(item.get("sku") or item.get("product_sku") or item.get("external_sku"))
        product_name = safe_str(item.get("product_name") or item.get("name") or item.get("product"))
        quantity = safe_int(item.get("quantity") or item.get("qty") or item.get("units"), default=0)

        unit_price = safe_decimal(item.get("unit_price"))
        if unit_price is None and item.get("unit_price_cents") is not None:
            cents = safe_int(item.get("unit_price_cents"), default=0)
            unit_price = Decimal(cents) / Decimal("100")

        total_price = safe_decimal(item.get("total_price"))
        if total_price is None and item.get("total_price_cents") is not None:
            cents = safe_int(item.get("total_price_cents"), default=0)
            total_price = Decimal(cents) / Decimal("100")

        if total_price is None and unit_price is not None and quantity:
            total_price = unit_price * Decimal(quantity)

        mapping_status = safe_str(item.get("mapping_status")) or ("unknown" if not sku else "unmapped")
        mapping_issue = safe_str(item.get("mapping_issue"))
        if not sku and not mapping_issue:
            mapping_issue = "Unknown SKU"

        normalized.append(
            {
                "sku": sku,
                "product_name": product_name,
                "quantity": quantity,
                "unit_price": unit_price,
                "total_price": total_price,
                "unit_price_cents": decimal_to_cents(unit_price),
                "total_price_cents": decimal_to_cents(total_price),
                "mapped_product_id": safe_str(item.get("mapped_product_id")),
                "mapping_status": mapping_status,
                "mapping_issue": mapping_issue,
                "raw_payload": item,
            }
        )

    return normalized


def derive_review_status(line_items: list[dict[str, Any]]) -> str:
    if not line_items:
        return "needs_review"

    for item in line_items:
        if (
            not item.get("sku")
            or item.get("mapping_status") in {"unknown", "unmapped", None}
            or item.get("mapping_issue")
        ):
            return "blocked"

    return "ready"





async def sync_leaflink_orders(
    db: AsyncSession,
    brand_id: str,
    orders: list[dict],
    pages_fetched: int = 0,
    org_id: Optional[str] = None,
) -> dict[str, Any]:
    """Upsert *pre-fetched* LeafLink orders and their line items.

    The caller is responsible for:
    - Credential lookup and API fetch (before calling this function)
    - Committing or rolling back the session after this function returns

    This function performs only DB writes (no credential lookup, no HTTP).
    It must NOT call db.commit(), db.rollback(), or db.begin() — the caller
    owns the transaction lifecycle.

    Args:
        db: Active async database session.
        brand_id: Brand UUID used to scope orders.
        orders: Pre-fetched, normalised order dicts from LeafLink.
        pages_fetched: Number of API pages retrieved (for metadata).
        org_id: Organization UUID for multi-tenant isolation. Written to every
                order row so GET /orders can filter by org_id AND brand_id.
    """
    sync_start = time.monotonic()

    logger.info(
        "[DB] upserting_orders count=%s brand_id=%s org_id=%s",
        len(orders),
        brand_id,
        org_id,
    )

    using_mock = any(isinstance(o, dict) and o.get("mock_data") for o in orders)
    if using_mock:
        logger.warning(
            "leaflink: sync_using_mock_data brand_id=%s — real API unavailable, MOCK_MODE active",
            brand_id,
        )

    created = 0
    updated = 0
    skipped = 0
    total_lines_written = 0
    errors: list[str] = []
    newest_order_date: datetime | None = None

    # ------------------------------------------------------------------
    # Upsert orders and write line items using the caller's session.
    # No begin() here — the route handler owns the transaction lifecycle.
    # Per-order error handling: log, skip, continue — never crash the loop.
    # ------------------------------------------------------------------
    for o in orders:
        order_external_id_for_log = "unknown"
        try:
            if not isinstance(o, dict):
                skipped += 1
                continue

            external_id = safe_str(o.get("external_id"))
            order_external_id_for_log = external_id or "missing"
            if not external_id:
                skipped += 1
                continue

            customer_name = safe_str(o.get("customer_name")) or "Unknown Customer"
            status = (safe_str(o.get("status")) or "submitted").lower()
            order_number = safe_str(o.get("order_number"))

            # Try multiple field names with fallbacks.
            # total_amount is the primary key set by the normalized LeafLink client;
            # the remaining fields cover raw/un-normalized payloads.
            amount_decimal = safe_decimal(
                o.get("total_amount")  # Primary: from normalized client
                or o.get("amount")
                or o.get("total")
                or o.get("subtotal")
                or o.get("price")
            )

            if amount_decimal is None:
                logger.warning(
                    "leaflink: sync_amount_missing external_id=%s — no pricing field found in order",
                    external_id,
                )

            total_cents = decimal_to_cents(amount_decimal) or 0

            item_count = safe_int(o.get("item_count"), default=0)
            unit_count = safe_int(o.get("unit_count"), default=0)

            raw_line_items = o.get("line_items", [])
            normalized_line_items = normalize_line_items(raw_line_items)

            if item_count == 0:
                item_count = len(normalized_line_items)

            if unit_count == 0:
                unit_count = sum(item.get("quantity", 0) or 0 for item in normalized_line_items)

            review_status = derive_review_status(normalized_line_items)
            # Normalize now() to guarantee a timezone-aware UTC datetime
            now = normalize_datetime(utc_now())

            raw_payload = o.get("raw_payload") if isinstance(o.get("raw_payload"), dict) else o

            existing_result = await db.execute(
                select(Order).where(
                    Order.brand_id == brand_id,
                    Order.external_order_id == external_id,
                )
            )
            existing = existing_result.scalar_one_or_none()

            # Map external timestamps from LeafLink payload.
            # LeafLink uses "created" and "modified" fields; fall back to common alternatives.
            created_raw = o.get("created") or o.get("created_at") or o.get("external_created_at")
            modified_raw = o.get("modified") or o.get("updated") or o.get("updated_at") or o.get("external_updated_at")

            external_created_at = normalize_datetime(created_raw)
            external_updated_at = normalize_datetime(modified_raw)

            # Track the newest order date for the response.
            for ts_dt in (external_updated_at, external_created_at):
                if ts_dt:
                    if newest_order_date is None or ts_dt > newest_order_date:
                        newest_order_date = ts_dt
                    break

            # Build a dict of all datetime fields for validation and logging
            order_dt_fields = {
                "synced_at": now,
                "last_synced_at": now,
                "external_created_at": external_created_at,
                "external_updated_at": external_updated_at,
            }

            # Log normalized datetimes for the first 3 orders processed
            order_count = created + updated
            if order_count < 3:
                for field, value in order_dt_fields.items():
                    if isinstance(value, datetime):
                        logger.info(
                            "[DT_NORMALIZED] field=%s tzinfo=%s",
                            field,
                            value.tzinfo,
                        )

            # Validate all datetime fields are timezone-aware before DB write
            for field_name, value in order_dt_fields.items():
                if isinstance(value, datetime):
                    if value.tzinfo is None:
                        logger.error(
                            "[DT_VALIDATION_FAILED] field=%s value=%s tzinfo=None",
                            field_name,
                            value,
                        )
                        raise ValueError(f"Naive datetime in {field_name}: {value}")

            if existing:
                existing.customer_name = customer_name
                existing.status = status
                existing.order_number = order_number
                existing.total_cents = total_cents
                existing.amount = amount_decimal
                existing.item_count = item_count
                existing.unit_count = unit_count
                existing.line_items_json = make_json_safe(normalized_line_items)
                existing.raw_payload = make_json_safe(raw_payload)
                existing.review_status = review_status
                existing.sync_status = "ok"
                existing.synced_at = normalize_datetime(now)
                existing.last_synced_at = normalize_datetime(now)
                existing.external_created_at = normalize_datetime(external_created_at)
                existing.external_updated_at = normalize_datetime(external_updated_at)
                # Always stamp org_id so existing rows get backfilled on re-sync
                if org_id:
                    existing.org_id = org_id
                order_row = existing
                updated += 1
            else:
                order_row = Order(
                    org_id=org_id,
                    brand_id=brand_id,
                    external_order_id=external_id,
                    order_number=order_number,
                    customer_name=customer_name,
                    status=status,
                    total_cents=total_cents,
                    amount=amount_decimal,
                    item_count=item_count,
                    unit_count=unit_count,
                    line_items_json=make_json_safe(normalized_line_items),
                    raw_payload=make_json_safe(raw_payload),
                    source="leaflink",
                    review_status=review_status,
                    sync_status="ok",
                    synced_at=normalize_datetime(now),
                    last_synced_at=normalize_datetime(now),
                    external_created_at=normalize_datetime(external_created_at),
                    external_updated_at=normalize_datetime(external_updated_at),
                )
                db.add(order_row)
                # Flush to get the auto-generated order_row.id before writing lines.
                await db.flush()
                created += 1

            # Delete stale line items then insert fresh ones.
            await db.execute(
                delete(OrderLine).where(OrderLine.order_id == order_row.id)
            )

            line_now = normalize_datetime(utc_now())
            for item in normalized_line_items:
                # Validate line item datetime fields before insert
                for field_name, value in [("created_at", line_now), ("updated_at", line_now)]:
                    if isinstance(value, datetime) and value.tzinfo is None:
                        logger.error(
                            "[DT_VALIDATION_FAILED] field=%s value=%s tzinfo=None",
                            field_name,
                            value,
                        )
                        raise ValueError(f"Naive datetime in OrderLine {field_name}: {value}")
                db.add(
                    OrderLine(
                        order_id=order_row.id,
                        sku=item.get("sku"),
                        product_name=item.get("product_name"),
                        quantity=item.get("quantity"),
                        unit_price=item.get("unit_price"),
                        total_price=item.get("total_price"),
                        unit_price_cents=item.get("unit_price_cents"),
                        total_price_cents=item.get("total_price_cents"),
                        mapped_product_id=item.get("mapped_product_id"),
                        mapping_status=item.get("mapping_status"),
                        mapping_issue=item.get("mapping_issue"),
                        raw_payload=make_json_safe(item.get("raw_payload")),
                        created_at=line_now,
                        updated_at=line_now,
                    )
                )

            total_lines_written += len(normalized_line_items)

        except Exception as order_exc:
            # Per-order failure: log, record error, skip this order, continue
            err_msg = f"order={order_external_id_for_log} error={str(order_exc)[:300]}"
            logger.error(
                "leaflink: upsert_order_failed brand_id=%s %s",
                brand_id,
                err_msg,
                exc_info=True,
            )
            if len(errors) < 5:
                errors.append(err_msg)
            skipped += 1
            continue

    sync_duration = round(time.monotonic() - sync_start, 2)
    error_count = len(errors)
    all_failed = (created == 0 and updated == 0 and error_count > 0 and len(orders) > 0)

    logger.info(
        "leaflink: upsert_complete org_id=%s brand_id=%s created=%s updated=%s skipped=%s errors=%s duration=%ss",
        org_id, brand_id, created, updated, skipped, error_count, sync_duration,
    )

    return {
        "ok": not all_failed,
        "orders_fetched": len(orders),
        "created": created,
        "updated": updated,
        "skipped": skipped,
        "error_count": error_count,
        "line_items_written": total_lines_written,
        "newest_order_date": newest_order_date,
        "pages_fetched": pages_fetched,
        "sync_duration_seconds": sync_duration,
        "errors": errors,
        "mock_data": using_mock,
        "message": f"Synced {len(orders)} orders: {created} created, {updated} updated, {skipped} skipped, {error_count} errors",
    }


async def sync_leaflink_orders_headers_only(
    brand_id: str,
    orders: list[dict],
    pages_fetched: int = 0,
    batch_size: int = HEADER_BATCH_SIZE,
    org_id: Optional[str] = None,
) -> dict[str, Any]:
    """Upsert ONLY order headers (Order table) for Phase 1 — no line items.

    Opens its own DB sessions and commits in small batches of ``batch_size``
    so that no single transaction holds locks for more than a handful of rows.
    Line item data is preserved in ``line_items_json`` on the Order row so the
    background worker can read it back without re-fetching from LeafLink.

    All header batches are committed BEFORE spawning the fire-and-forget asyncio
    task that writes OrderLine rows, guaranteeing the parent Order rows exist in
    the database when the background worker queries for them.

    Returns a summary dict compatible with the existing sync_result contract.
    """
    logger.info("[ORDER_SAVE_START] entering order save function brand=%s fetched=%s", brand_id, len(orders))

    sync_start = time.monotonic()

    # Cast brand_id and org_id to canonical UUID form so that PostgreSQL UUID
    # columns never receive a plain character-varying expression.
    brand_id_value = safe_uuid(brand_id)
    org_id_value = safe_uuid(org_id) if org_id else None

    total_created = 0
    total_updated = 0
    total_skipped = 0
    errors: list[str] = []
    newest_order_date: datetime | None = None
    fetched_count = len(orders)

    # Split orders into batches of batch_size
    batches = [
        orders[i : i + batch_size]
        for i in range(0, len(orders), batch_size)
    ]

    logger.info(
        "[OrdersSync] sync_start brand=%s total_orders=%s batch_size=%s total_batches=%s",
        brand_id_value,
        len(orders),
        batch_size,
        len(batches),
    )

    # ------------------------------------------------------------------
    # Phase 1: Upsert all order headers and commit each batch before
    # spawning the line-item background task.  This guarantees that every
    # Order row is visible to subsequent DB sessions when the background
    # worker looks them up by (brand_id, external_order_id).
    # ------------------------------------------------------------------
    for batch_num, batch in enumerate(batches, 1):
        batch_start = time.monotonic()
        current_batch_size = len(batch)
        batch_created = 0
        batch_updated = 0
        batch_skipped = 0

        try:
            async with AsyncSessionLocal() as db:
                async with db.begin():
                    for o in batch:
                        if not isinstance(o, dict):
                            batch_skipped += 1
                            continue

                        # Map external_order_id: use order["id"] as primary source
                        # (LeafLink raw payloads carry the order PK in "id").
                        external_id = safe_str(o.get("id") or o.get("external_id") or o.get("external_order_id"))
                        if not external_id:
                            logger.error(
                                "[OrdersSync] skip_no_external_id batch=%s order_number=%s",
                                batch_num,
                                safe_str(o.get("order_number")),
                            )
                            batch_skipped += 1
                            continue

                        customer_name = safe_str(o.get("customer_name")) or "Unknown Customer"
                        status = (safe_str(o.get("status")) or "submitted").lower()
                        order_number = safe_str(o.get("order_number"))

                        amount_decimal = safe_decimal(
                            o.get("total_amount")
                            or o.get("amount")
                            or o.get("total")
                            or o.get("subtotal")
                            or o.get("price")
                        )
                        total_cents = decimal_to_cents(amount_decimal) or 0

                        item_count = safe_int(o.get("item_count"), default=0)
                        unit_count = safe_int(o.get("unit_count"), default=0)

                        # Normalise line items and store as JSON — do NOT write
                        # OrderLine rows here; that is deferred to the background worker.
                        raw_line_items = o.get("line_items", [])
                        normalized_line_items = normalize_line_items(raw_line_items)

                        if item_count == 0:
                            item_count = len(normalized_line_items)
                        if unit_count == 0:
                            unit_count = sum(
                                item.get("quantity", 0) or 0
                                for item in normalized_line_items
                            )

                        review_status = derive_review_status(normalized_line_items)
                        now = utc_now()
                        raw_payload = (
                            o.get("raw_payload")
                            if isinstance(o.get("raw_payload"), dict)
                            else o
                        )

                        # Map external timestamps from LeafLink payload.
                        # LeafLink uses "created" and "modified" fields; fall back
                        # to common alternatives so the mapping is robust.
                        created_raw = o.get("created") or o.get("created_at") or o.get("external_created_at")
                        modified_raw = o.get("modified") or o.get("updated") or o.get("updated_at") or o.get("external_updated_at")

                        external_created_at = normalize_datetime(created_raw)
                        external_updated_at = normalize_datetime(modified_raw)

                        for ts_dt in (external_updated_at, external_created_at):
                            if ts_dt:
                                if newest_order_date is None or ts_dt > newest_order_date:
                                    newest_order_date = ts_dt
                                break

                        existing_result = await db.execute(
                            select(Order).where(
                                Order.brand_id == brand_id_value,
                                Order.external_order_id == external_id,
                            )
                        )
                        existing = existing_result.scalar_one_or_none()

                        if existing:
                            # Use raw SQL UPDATE with CAST so PostgreSQL receives
                            # explicit type coercion for UUID columns instead of
                            # rejecting a character-varying bound parameter.
                            update_stmt = """
UPDATE orders SET
    org_id = CAST(:org_id AS uuid),
    customer_name = :customer_name,
    status = :status,
    order_number = :order_number,
    total_cents = :total_cents,
    amount = :amount,
    item_count = :item_count,
    unit_count = :unit_count,
    line_items_json = :line_items_json,
    raw_payload = :raw_payload,
    review_status = :review_status,
    sync_status = :sync_status,
    synced_at = :synced_at,
    last_synced_at = :last_synced_at,
    external_created_at = :external_created_at,
    external_updated_at = :external_updated_at,
    updated_at = :updated_at
WHERE CAST(brand_id AS uuid) = CAST(:brand_id AS uuid) AND external_order_id = :external_order_id
"""
                            # Serialize JSON fields for raw SQL binding — asyncpg
                            # cannot encode Python list/dict directly; it needs strings.
                            line_items_json_str = json.dumps(make_json_safe(normalized_line_items)) if normalized_line_items else None
                            raw_payload_str = json.dumps(make_json_safe(raw_payload)) if raw_payload else None

                            now = datetime.now(timezone.utc)

                            update_params = {
                                "org_id": org_id_value,
                                "brand_id": brand_id_value,
                                "external_order_id": external_id,
                                "customer_name": customer_name,
                                "status": status,
                                "order_number": order_number,
                                "total_cents": total_cents,
                                "amount": amount_decimal,
                                "item_count": item_count,
                                "unit_count": unit_count,
                                "line_items_json": line_items_json_str,
                                "raw_payload": raw_payload_str,
                                "review_status": review_status,
                                "sync_status": "ok",
                                "synced_at": normalize_datetime(now),
                                "last_synced_at": normalize_datetime(now),
                                "external_created_at": normalize_datetime(external_created_at),
                                "external_updated_at": normalize_datetime(external_updated_at),
                                "updated_at": normalize_datetime(now),
                            }

                            # Log normalized datetimes for first 3 orders per batch
                            if batch_created + batch_updated < 3:
                                for field, value in update_params.items():
                                    if isinstance(value, datetime):
                                        logger.info(
                                            "[DT_NORMALIZED] field=%s tzinfo=%s",
                                            field,
                                            value.tzinfo,
                                        )

                            # Validate all datetime params are timezone-aware
                            for field_name, value in update_params.items():
                                if isinstance(value, datetime):
                                    if value.tzinfo is None:
                                        logger.error(
                                            "[DT_VALIDATION_FAILED] field=%s value=%s tzinfo=None",
                                            field_name,
                                            value,
                                        )
                                        raise ValueError(f"Naive datetime in {field_name}: {value}")

                            await db.execute(
                                text(update_stmt),
                                update_params,
                            )
                            batch_updated += 1
                        else:
                            # Use raw SQL INSERT with CAST so PostgreSQL receives
                            # explicit type coercion for UUID columns instead of
                            # rejecting a character-varying bound parameter.
                            insert_stmt = """
INSERT INTO orders (
    org_id, brand_id, external_order_id, order_number, customer_name, status,
    total_cents, amount, item_count, unit_count, line_items_json, raw_payload,
    source, review_status, sync_status, synced_at, last_synced_at,
    external_created_at, external_updated_at, created_at, updated_at
) VALUES (
    CAST(:org_id AS uuid), CAST(:brand_id AS uuid), :external_order_id, :order_number,
    :customer_name, :status, :total_cents, :amount, :item_count, :unit_count,
    :line_items_json, :raw_payload, :source, :review_status, :sync_status,
    :synced_at, :last_synced_at, :external_created_at, :external_updated_at,
    :created_at, :updated_at
)
"""
                            # Serialize JSON fields for raw SQL binding — asyncpg
                            # cannot encode Python list/dict directly; it needs strings.
                            line_items_json_str = json.dumps(make_json_safe(normalized_line_items)) if normalized_line_items else None
                            raw_payload_str = json.dumps(make_json_safe(raw_payload)) if raw_payload else None

                            now = datetime.now(timezone.utc)

                            insert_params = {
                                "org_id": org_id_value,
                                "brand_id": brand_id_value,
                                "external_order_id": external_id,
                                "order_number": order_number,
                                "customer_name": customer_name,
                                "status": status,
                                "total_cents": total_cents,
                                "amount": amount_decimal,
                                "item_count": item_count,
                                "unit_count": unit_count,
                                "line_items_json": line_items_json_str,
                                "raw_payload": raw_payload_str,
                                "source": "leaflink",
                                "review_status": review_status,
                                "sync_status": "ok",
                                "synced_at": normalize_datetime(now),
                                "last_synced_at": normalize_datetime(now),
                                "external_created_at": normalize_datetime(external_created_at),
                                "external_updated_at": normalize_datetime(external_updated_at),
                                "created_at": normalize_datetime(now),
                                "updated_at": normalize_datetime(now),
                            }

                            # Log normalized datetimes for first 3 orders per batch
                            if batch_created + batch_updated < 3:
                                for field, value in insert_params.items():
                                    if isinstance(value, datetime):
                                        logger.info(
                                            "[DT_NORMALIZED] field=%s tzinfo=%s",
                                            field,
                                            value.tzinfo,
                                        )

                            # Validate all datetime params are timezone-aware
                            for field_name, value in insert_params.items():
                                if isinstance(value, datetime):
                                    if value.tzinfo is None:
                                        logger.error(
                                            "[DT_VALIDATION_FAILED] field=%s value=%s tzinfo=None",
                                            field_name,
                                            value,
                                        )
                                        raise ValueError(f"Naive datetime in {field_name}: {value}")

                            await db.execute(
                                text(insert_stmt),
                                insert_params,
                            )
                            batch_created += 1

                # Explicit commit guarantee
                await db.commit()

            # Batch committed successfully (async with db.begin() exited cleanly)
            batch_duration = round(time.monotonic() - batch_start, 2)
            logger.info(
                "[ORDER_NORMALIZED] normalized=%s batch=%s/%s",
                batch_created + batch_updated,
                batch_num,
                len(batches),
            )
            logger.info(
                "[ORDER_SAVE_RESULT] inserted=%s updated=%s batch=%s/%s",
                batch_created,
                batch_updated,
                batch_num,
                len(batches),
            )
            logger.info("[ORDER_SAVE_COMMIT] brand_id=%s created=%s updated=%s batch=%s/%s", brand_id_value, batch_created, batch_updated, batch_num, len(batches))
            logger.info(
                "[OrdersSync] batch_committed batch=%s/%s brand=%s created=%s updated=%s skipped=%s duration=%ss",
                batch_num,
                len(batches),
                brand_id_value,
                batch_created,
                batch_updated,
                batch_skipped,
                batch_duration,
            )
            total_created += batch_created
            total_updated += batch_updated
            total_skipped += batch_skipped

        except Exception as batch_exc:
            batch_duration = round(time.monotonic() - batch_start, 2)
            err_msg = str(batch_exc)
            logger.error(
                "[OrdersSync] batch_failed batch=%s/%s brand=%s batch_size=%s error=%s duration=%ss",
                batch_num,
                len(batches),
                brand_id_value,
                current_batch_size,
                err_msg,
                batch_duration,
                exc_info=True,
            )
            errors.append(err_msg)
            total_skipped += current_batch_size
            continue

    sync_duration = round(time.monotonic() - sync_start, 2)

    # Emit critical alert if we fetched orders but saved none at all
    if fetched_count > 0 and total_created == 0 and total_updated == 0:
        logger.error(
            "[CRITICAL] orders_not_saved fetched=%s but saved=0 brand=%s",
            fetched_count,
            brand_id_value,
        )

    logger.info(
        "[OrdersSync] all_batches_complete brand=%s total_batches=%s total_created=%s total_updated=%s total_skipped=%s sync_duration=%ss",
        brand_id_value,
        len(batches),
        total_created,
        total_updated,
        total_skipped,
        sync_duration,
    )

    # ------------------------------------------------------------------
    # Phase 2: All order header batches are now committed to the DB.
    # Spawn the line-item background task AFTER the commit loop so that
    # every Order row is guaranteed to exist when the worker queries for
    # it by (brand_id, external_order_id).
    # ------------------------------------------------------------------
    async def _background_line_items() -> None:
        await sync_leaflink_line_items(brand_id_value, orders, org_id=org_id_value)

    asyncio.create_task(_background_line_items())

    return {
        "ok": len(errors) == 0,
        "orders_fetched": fetched_count,
        "created": total_created,
        "updated": total_updated,
        "skipped": total_skipped,
        "line_items_written": 0,
        "newest_order_date": newest_order_date,
        "pages_fetched": pages_fetched,
        "sync_duration_seconds": sync_duration,
        "errors": errors,
        "mock_data": any(isinstance(o, dict) and o.get("mock_data") for o in orders),
        "message": f"Upserted {total_created + total_updated} order headers ({total_created} new, {total_updated} updated)",
    }


async def sync_leaflink_line_items(
    brand_id: str,
    orders: list[dict],
    org_id: Optional[str] = None,
) -> dict[str, Any]:
    """Write OrderLine rows for a list of pre-fetched orders (background Phase 2).

    For each order this function:
      1. Resolves the external_id using the same priority as the header sync
         (order["id"] → "external_id" → "external_order_id").
      2. Checks that at least one committed Order row exists for this brand
         before processing any line items (safety guard).
      3. Looks up the committed Order row in the DB by (brand_id, external_order_id).
      4. If the row is NOT found, logs [LINE_ITEM_SKIP] and skips — the order
         was never committed, so inserting line items would violate the FK.
      5. If found, reads line_items_json from the DB row (authoritative source),
         deletes stale OrderLine rows, and inserts fresh ones.
      6. Logs [LINE_ITEM_SAVE] with the count of inserted rows.

    Runs in its own session per order so a single failure never aborts the rest.
    """
    from sqlalchemy import func

    # Safety guard: verify orders exist before processing line items
    logger.info("[LINE_ITEM_CHECK_START] checking for parent orders brand=%s", brand_id)

    bg_start = time.monotonic()
    total_lines_written = 0
    errors: list[str] = []

    # Cast brand_id to canonical UUID form to match what was written in Phase 1.
    brand_id_value = safe_uuid(brand_id)
    org_id_value = safe_uuid(org_id) if org_id else None

    try:
        async with AsyncSessionLocal() as check_db:
            result = await check_db.execute(
                select(func.count()).select_from(Order).where(
                    Order.brand_id == brand_id_value,
                    Order.source == "leaflink"
                )
            )
            order_count = result.scalar_one()
    except Exception as e:
        logger.error("[LINE_ITEM_CHECK_ERROR] failed to count orders: %s", e)
        order_count = 0

    logger.info("[LINE_ITEM_CHECK_RESULT] found %s orders brand=%s", order_count, brand_id_value)

    if order_count == 0:
        logger.error("[LINE_ITEM_BLOCKED] no orders in DB — aborting line item sync brand=%s", brand_id_value)
        return {
            "ok": False,
            "lines_written": 0,
            "errors": ["No parent orders found in database"],
            "duration_seconds": 0,
        }

    for o in orders:
        if not isinstance(o, dict):
            continue

        # Use the same external_id resolution as sync_leaflink_orders_headers_only
        # so the DB lookup always matches the key that was written during Phase 1.
        external_id = safe_str(o.get("id") or o.get("external_id") or o.get("external_order_id"))
        if not external_id:
            continue

        try:
            async with AsyncSessionLocal() as db:
                async with db.begin():
                    leaflink_order_id = safe_str(external_id)
                    result = await db.execute(
                        select(Order).where(
                            Order.brand_id == brand_id_value,
                            Order.external_order_id == leaflink_order_id,
                        )
                    )
                    order_row = result.scalar_one_or_none()

                    if order_row is None:
                        # Order was not committed — skip to avoid FK violation.
                        logger.warning(
                            "[LINE_ITEM_ORDER_LOOKUP_FAIL] leaflink_order_id=%s",
                            leaflink_order_id,
                        )
                        logger.warning(
                            "[LINE_ITEM_SKIP] reason=order_not_found external_id=%s brand=%s",
                            leaflink_order_id,
                            brand_id_value,
                        )
                        continue

                    logger.info(
                        "[LINE_ITEM_ORDER_LOOKUP] leaflink_order_id=%s matched_order_id=%s",
                        leaflink_order_id,
                        order_row.id,
                    )

                    # Read line items from the DB row (written during Phase 1)
                    # rather than from the raw in-memory dict, so we use the
                    # already-normalised, JSON-safe representation.
                    db_line_items = order_row.line_items_json
                    if isinstance(db_line_items, list):
                        normalized_line_items = db_line_items
                    else:
                        # Fallback: normalise from the raw order dict
                        raw_line_items = o.get("line_items", [])
                        normalized_line_items = normalize_line_items(raw_line_items)

                    if not normalized_line_items:
                        continue

                    # Columns that may not exist in all environments
                    optional_columns = [
                        "packed_qty",
                        "unit_price_cents",
                        "total_price_cents",
                    ]

                    # Check which optional columns exist in the database schema
                    enabled_columns = {}
                    for col in optional_columns:
                        enabled_columns[col] = has_column("order_lines", col)

                    logger.info(
                        "[LINE_ITEM_COLUMNS_ENABLED] packed_qty=%s unit_price_cents=%s total_price_cents=%s",
                        str(enabled_columns.get("packed_qty", False)).lower(),
                        str(enabled_columns.get("unit_price_cents", False)).lower(),
                        str(enabled_columns.get("total_price_cents", False)).lower(),
                    )

                    # Base columns (always present in order_lines)
                    insert_columns = [
                        "order_id",
                        "sku",
                        "product_name",
                        "quantity",
                        "unit_price",
                        "total_price",
                        "mapped_product_id",
                        "mapping_status",
                        "mapping_issue",
                        "raw_payload",
                        "created_at",
                        "updated_at",
                    ]

                    # Add optional columns only if they exist in the schema
                    for col in optional_columns:
                        if enabled_columns.get(col, False):
                            insert_columns.append(col)

                    columns_str = ", ".join(insert_columns)
                    placeholders = ", ".join([f":{col}" for col in insert_columns])
                    line_insert_stmt = f"""
                        INSERT INTO public.order_lines ({columns_str})
                        VALUES ({placeholders})
                    """

                    # Replace stale line items with the current set
                    await db.execute(
                        delete(OrderLine).where(OrderLine.order_id == order_row.id)
                    )

                    inserted_count = 0
                    for line_number, item in enumerate(normalized_line_items, 1):
                        order_id_value = order_row.id
                        sku = item.get("sku")

                        # Pre-insert validation: skip items missing required fields
                        if not order_id_value or not sku:
                            logger.error(
                                "[LINE_ITEM_SKIP] order_id=%s line_number=%s sku=%s — missing required field",
                                order_id_value,
                                line_number,
                                sku,
                            )
                            continue

                        # Fail-safe: one bad line item must not crash the entire batch
                        try:
                            raw_payload_val = item.get("raw_payload")
                            raw_payload_str = json.dumps(make_json_safe(raw_payload_val)) if raw_payload_val is not None else None

                            # Base params (always present)
                            insert_params: dict[str, Any] = {
                                "order_id": order_id_value,
                                "sku": sku,
                                "product_name": item.get("product_name"),
                                "quantity": item.get("quantity"),
                                "unit_price": item.get("unit_price"),
                                "total_price": item.get("total_price"),
                                "mapped_product_id": item.get("mapped_product_id"),
                                "mapping_status": item.get("mapping_status"),
                                "mapping_issue": item.get("mapping_issue"),
                                "raw_payload": raw_payload_str,
                                "created_at": normalize_datetime(utc_now()),
                                "updated_at": normalize_datetime(utc_now()),
                            }

                            # Add optional params only if columns exist
                            if enabled_columns.get("packed_qty", False):
                                insert_params["packed_qty"] = 0
                            if enabled_columns.get("unit_price_cents", False):
                                insert_params["unit_price_cents"] = item.get("unit_price_cents")
                            if enabled_columns.get("total_price_cents", False):
                                insert_params["total_price_cents"] = item.get("total_price_cents")

                            # Log normalized datetimes for first 3 line items
                            if line_number <= 3:
                                for field, value in insert_params.items():
                                    if isinstance(value, datetime):
                                        logger.info(
                                            "[DT_NORMALIZED] field=%s tzinfo=%s",
                                            field,
                                            value.tzinfo,
                                        )

                            # Validate all datetime params are timezone-aware
                            for field_name, value in insert_params.items():
                                if isinstance(value, datetime):
                                    if value.tzinfo is None:
                                        logger.error(
                                            "[DT_VALIDATION_FAILED] field=%s value=%s tzinfo=None",
                                            field_name,
                                            value,
                                        )
                                        raise ValueError(f"Naive datetime in {field_name}: {value}")

                            await db.execute(text(line_insert_stmt), insert_params)
                            inserted_count += 1
                        except Exception as line_error:
                            logger.error(
                                "[LINE_ITEM_ERROR] order_id=%s line_number=%s sku=%s error=%s",
                                order_id_value,
                                line_number,
                                sku,
                                str(line_error),
                                exc_info=True,
                            )
                            # Continue to next line item instead of crashing batch
                            continue

                    logger.info("[LINE_ITEM_SAVE] inserted=%s external_id=%s", inserted_count, external_id)
                    logger.info("[LINE_ITEM_SAVE_COMMIT] brand_id=%s created=%s updated=0", brand_id_value, inserted_count)

        except Exception as line_exc:
            err_msg = str(line_exc)
            logger.error(
                "[OrdersSync] line_items_error brand=%s external_id=%s error=%s",
                brand_id_value,
                external_id,
                err_msg,
                exc_info=True,
            )
            errors.append(err_msg)

    bg_duration = round(time.monotonic() - bg_start, 2)
    logger.info(
        "[OrdersSync] line_items_complete brand=%s lines_written=%s duration=%ss errors=%s",
        brand_id_value,
        total_lines_written,
        bg_duration,
        len(errors),
    )

    return {
        "ok": len(errors) == 0,
        "lines_written": total_lines_written,
        "errors": errors,
        "duration_seconds": bg_duration,
    }


# ---------------------------------------------------------------------------
# Background continuous sync (Phase 2)
# ---------------------------------------------------------------------------

# Batch size bounds for adaptive pacing
_BG_BATCH_MIN = 3
_BG_BATCH_MAX = 5
_BG_BATCH_DEFAULT = 4

# Retry configuration
MAX_RETRIES = 3
INITIAL_BACKOFF_SECONDS = 2
MAX_BACKOFF_SECONDS = 60
BACKOFF_MULTIPLIER = 2.0

# Transient error codes (retry these)
TRANSIENT_ERROR_CODES = {429, 502, 503, 504}

# Thresholds (seconds) for adaptive batch sizing
_BG_BATCH_FAST_THRESHOLD = 5.0   # < 5 s → increase batch size
_BG_BATCH_SLOW_THRESHOLD = 15.0  # > 15 s → decrease batch size

# Hard timeout for the entire background sync run (30 minutes)
_BG_SYNC_TIMEOUT = 1800


async def sync_leaflink_background_continuous(
    brand_id: str,
    api_key: str,
    company_id: str,
    start_page: int,
    total_pages: Optional[int],
    manager: Optional["BackgroundSyncManager"] = None,
    total_orders_available: Optional[int] = None,
    sync_run_id: Optional[int] = None,
    auth_scheme: str = "Token",
    base_url: Optional[str] = None,
) -> None:
    """
    Fetch all remaining LeafLink pages in adaptive batches and upsert to DB.

    Designed to run as a fire-and-forget asyncio task after Phase 1 completes.
    Progress is persisted exclusively to the SyncRun table after every page so
    the sync can resume from the last completed page if the service restarts.

    Batch size adapts based on observed fetch latency:
      - < 5 s per batch  → increase to _BG_BATCH_MAX (5 pages)
      - > 15 s per batch → decrease to _BG_BATCH_MIN (3 pages)
      - otherwise        → keep at _BG_BATCH_DEFAULT (4 pages)

    Cursor-loop detection: if the same cursor is returned twice in a row the
    run is marked stalled with reason=cursor_loop_detected.

    Args:
        brand_id:               Brand UUID.
        api_key:                LeafLink API key (from DB credential).
        company_id:             LeafLink company ID (from DB credential).
        start_page:             First page to fetch (Phase 1 already fetched pages before this).
        total_pages:            Total pages reported by LeafLink API, or None for cursor-based pagination.
        manager:                Optional BackgroundSyncManager instance for in-memory tracking.
        total_orders_available: Total orders reported by LeafLink (for progress display).
        sync_run_id:            Optional SyncRun.id to persist progress against.
        base_url:               LeafLink base URL (from DB credential, required).
    """
    from services.leaflink_client import LeafLinkClient
    from services.sync_run_manager import (
        update_progress as _srm_update_progress,
        mark_completed as _srm_mark_completed,
        mark_stalled as _srm_mark_stalled,
        mark_failed as _srm_mark_failed,
    )

    try:
        bg_start = time.monotonic()
        current_page = start_page
        batch_size = _BG_BATCH_DEFAULT
        total_orders_synced = 0
        resume_url: Optional[str] = None
        _prev_cursor: Optional[str] = None  # for cursor-loop detection

        async def _persist_run_progress(
            pages_synced: int,
            orders_loaded: int,
            cursor: Optional[str],
            page: int,
            total_pages_val: Optional[int] = None,
        ) -> None:
            """Persist progress to SyncRun (if sync_run_id set) and BackgroundSyncManager."""
            if sync_run_id:
                try:
                    async with AsyncSessionLocal() as _prog_db:
                        async with _prog_db.begin():
                            await _srm_update_progress(
                                _prog_db,
                                sync_run_id=sync_run_id,
                                pages_synced=pages_synced,
                                orders_loaded=orders_loaded,
                                cursor=cursor,
                                page=page,
                                total_pages=total_pages_val,
                                total_orders_available=total_orders_available,
                            )
                except Exception as _prog_exc:
                    logger.error(
                        "[OrdersSync] sync_run_progress_persist_error run_id=%s error=%s",
                        sync_run_id,
                        _prog_exc,
                    )

        try:
            client = LeafLinkClient(
                api_key=api_key,
                company_id=company_id,
                brand_id=brand_id,
                auth_scheme=auth_scheme,
                base_url=base_url,
            )
        except Exception as client_exc:
            logger.error(
                "[OrdersSync] bg_sync_client_init_error brand=%s error=%s",
                brand_id,
                client_exc,
            )
            if sync_run_id:
                try:
                    async with AsyncSessionLocal() as _fail_db:
                        async with _fail_db.begin():
                            await _srm_mark_failed(_fail_db, sync_run_id, str(client_exc))
                except Exception:
                    pass
            return

        loop = asyncio.get_event_loop()

        async def _fetch_with_retry(
            start_page: int,
            num_pages: int,
            max_retries: int = MAX_RETRIES,
        ) -> dict:
            """Fetch pages with exponential backoff retry on transient errors."""

            backoff_seconds = INITIAL_BACKOFF_SECONDS
            last_error = None

            for attempt in range(max_retries + 1):
                try:
                    _capture_page = start_page
                    _capture_url = resume_url
                    _capture_num = num_pages

                    result = await loop.run_in_executor(
                        _leaflink_executor,
                        lambda: client.fetch_orders_page_range(
                            start_page=_capture_page,
                            num_pages=_capture_num,
                            page_size=HEADER_BATCH_SIZE * 4,  # 100 orders per page
                            normalize=True,
                            brand=brand_id,
                            resume_url=_capture_url,
                        ),
                    )

                    return result

                except Exception as fetch_exc:
                    last_error = fetch_exc
                    error_code = getattr(fetch_exc, "status_code", None)

                    # Check if error is transient
                    is_transient = error_code in TRANSIENT_ERROR_CODES

                    if not is_transient or attempt >= max_retries:
                        # Not transient or out of retries — give up
                        logger.error(
                            "[OrdersSync] fetch_failed brand=%s page=%s error_code=%s transient=%s attempt=%s/%s error=%s",
                            brand_id,
                            start_page,
                            error_code,
                            is_transient,
                            attempt + 1,
                            max_retries + 1,
                            fetch_exc,
                        )
                        raise

                    # Transient error — retry with backoff
                    logger.warning(
                        "[OrdersSync] fetch_transient_error brand=%s page=%s error_code=%s attempt=%s/%s backoff_seconds=%s",
                        brand_id,
                        start_page,
                        error_code,
                        attempt + 1,
                        max_retries + 1,
                        backoff_seconds,
                    )

                    await asyncio.sleep(backoff_seconds)
                    backoff_seconds = min(backoff_seconds * BACKOFF_MULTIPLIER, MAX_BACKOFF_SECONDS)

            raise last_error

        # When total_pages is None (cursor-based pagination), loop until the API
        # returns no next_url. When total_pages is known, also enforce the page bound.
        while total_pages is None or current_page <= total_pages:
            # Hard timeout guard
            elapsed_total = time.monotonic() - bg_start
            if elapsed_total > _BG_SYNC_TIMEOUT:
                pages_done = current_page - start_page
                pages_total = (total_pages - start_page + 1) if total_pages is not None else "?"
                logger.warning(
                    "[OrdersSync] bg_sync_timeout brand=%s elapsed=%.1fs pages_done=%s/%s",
                    brand_id,
                    elapsed_total,
                    pages_done,
                    pages_total,
                )
                break

            batch_start = time.monotonic()

            # ------------------------------------------------------------------ #
            # Fetch a batch of pages from LeafLink (with retry on transient err) #
            # ------------------------------------------------------------------ #
            try:
                fetch_result = await _fetch_with_retry(
                    start_page=current_page,
                    num_pages=batch_size,
                )
            except Exception as fetch_exc:
                error_code = getattr(fetch_exc, "status_code", None)
                is_transient = error_code in TRANSIENT_ERROR_CODES
                _err_str = str(fetch_exc)

                if is_transient:
                    # Transient error exhausted all retries — mark as paused, not failed
                    logger.warning(
                        "[OrdersSync] sync_paused_transient_error brand=%s error_code=%s error=%s",
                        brand_id,
                        error_code,
                        fetch_exc,
                    )

                    if sync_run_id:
                        try:
                            async with AsyncSessionLocal() as _stall_db:
                                async with _stall_db.begin():
                                    await _srm_mark_stalled(
                                        _stall_db,
                                        sync_run_id,
                                        f"transient_error_http_{error_code}",
                                    )
                        except Exception:
                            pass

                    # Return gracefully — worker will retry on next poll
                    return
                else:
                    # Permanent error — detect auth failures specifically before propagating
                    _err_lower = _err_str.lower()
                    if "status=401" in _err_lower or "auth failed" in _err_lower or "authentication failed" in _err_lower:
                        logger.error(
                            "[LeafLinkSync] auth_failed id=%s status=401 error=%s",
                            sync_run_id,
                            _err_str[:300],
                        )
                        _err_str = f"LeafLink authentication failed (401): {_err_str[:300]}"
                    elif "status=403" in _err_lower or "forbidden" in _err_lower or "invalid_token" in _err_lower:
                        logger.error(
                            "[LeafLinkSync] forbidden id=%s status=403 error=%s",
                            sync_run_id,
                            _err_str[:300],
                        )
                        _err_str = f"LeafLink access forbidden (403): {_err_str[:300]}"
                    else:
                        logger.error(
                            "[LeafLinkSync] api_error id=%s error=%s",
                            sync_run_id,
                            _err_str[:500],
                        )

                    logger.error(
                        "[OrdersSync] sync_failed_permanent_error brand=%s error=%s",
                        brand_id,
                        fetch_exc,
                        exc_info=True,
                    )

                    if sync_run_id:
                        try:
                            async with AsyncSessionLocal() as _fail_db2:
                                async with _fail_db2.begin():
                                    await _srm_mark_failed(_fail_db2, sync_run_id, _err_str[:500])
                        except Exception:
                            pass

                    raise

            batch_orders = fetch_result.get("orders", [])
            pages_fetched_this_batch = fetch_result.get("pages_fetched", batch_size)
            next_cursor = fetch_result.get("next_url")
            next_page = fetch_result.get("next_page")

            # ------------------------------------------------------------------ #
            # Cursor-loop detection: same cursor returned twice → stalled         #
            # ------------------------------------------------------------------ #
            next_cursor_hash = _cursor_hash(next_cursor)

            if next_cursor and next_cursor == _prev_cursor:
                _loop_reason = "cursor_loop_detected"
                logger.error(
                    "[OrdersSync] cursor_loop_detected brand=%s page=%s cursor_hash=%s — marking stalled",
                    brand_id,
                    current_page,
                    next_cursor_hash,
                )
                if sync_run_id:
                    try:
                        async with AsyncSessionLocal() as _loop_db:
                            async with _loop_db.begin():
                                await _srm_mark_stalled(_loop_db, sync_run_id, _loop_reason)
                    except Exception:
                        pass
                return

            _prev_cursor = next_cursor
            resume_url = next_cursor

            # ------------------------------------------------------------------ #
            # Upsert order headers (headers-only, line items deferred)            #
            # ------------------------------------------------------------------ #
            if batch_orders:
                try:
                    persist_result = await sync_leaflink_orders_headers_only(
                        brand_id=brand_id,
                        orders=batch_orders,
                        pages_fetched=pages_fetched_this_batch,
                    )
                    # Line items are deferred inside sync_leaflink_orders_headers_only
                    # via asyncio.create_task — no need to spawn a second task here.
                    # persist_result captured but logging removed to avoid potential exceptions

                except Exception as upsert_exc:
                    logger.error(
                        "[OrdersSync] bg_batch_upsert_error brand=%s page=%s error=%s",
                        brand_id,
                        current_page,
                        upsert_exc,
                        exc_info=True,
                    )

            total_orders_synced += len(batch_orders)
            # When total_pages is None (cursor-based), fall back to current_page
            last_completed_page = (next_page - 1) if next_page else (total_pages or current_page)

            # ------------------------------------------------------------------ #
            # Persist progress to DB (SyncRun) and update in-memory state        #
            # ------------------------------------------------------------------ #
            if manager:
                manager.record_page_complete(brand_id, last_completed_page)

            await _persist_run_progress(
                pages_synced=last_completed_page,
                orders_loaded=total_orders_synced,
                cursor=next_cursor,
                page=last_completed_page,
                total_pages_val=total_pages,
            )

            # Log sampled progress every 10 pages
            if total_pages and last_completed_page % 10 == 0:
                logger.info(
                    "[LeafLinkSync] progress id=%s page=%s/%s orders=%s",
                    sync_run_id,
                    last_completed_page,
                    total_pages,
                    total_orders_synced,
                )

            # ------------------------------------------------------------------ #
            # Adaptive batch sizing based on fetch latency                        #
            # ------------------------------------------------------------------ #
            batch_seconds = (time.monotonic() - batch_start)
            if batch_seconds < _BG_BATCH_FAST_THRESHOLD:
                batch_size = min(batch_size + 1, _BG_BATCH_MAX)
            elif batch_seconds > _BG_BATCH_SLOW_THRESHOLD:
                batch_size = max(batch_size - 1, _BG_BATCH_MIN)

            # Advance page pointer
            if next_page:
                current_page = next_page
            else:
                # No more pages
                break

            if not resume_url:
                # LeafLink returned no next URL — we've reached the end
                break

        # ---------------------------------------------------------------------- #
        # Sync complete                                                           #
        # ---------------------------------------------------------------------- #
        total_duration = round(time.monotonic() - bg_start, 1)
        # When total_pages is None (cursor-based), final_page is simply the last page visited
        final_page = (current_page - 1) if total_pages is None else min(current_page - 1, total_pages)

        # CRITICAL: Do NOT mark completed if next_cursor exists — pagination is incomplete.
        # A non-None next_cursor means LeafLink has more pages; the loop exited early
        # (e.g. timeout) rather than reaching the true end of the result set.
        if next_cursor:
            logger.warning(
                "[LeafLinkSync] stopped_with_cursor id=%s brand=%s total_loaded=%s cursor_hash=%s",
                sync_run_id,
                brand_id,
                total_orders_synced,
                _cursor_hash(next_cursor),
            )
            if sync_run_id:
                try:
                    async with AsyncSessionLocal() as _stall_db:
                        async with _stall_db.begin():
                            await _srm_mark_stalled(
                                _stall_db,
                                sync_run_id,
                                f"pagination_incomplete: next_cursor_present after {final_page} pages",
                            )
                except Exception as _stall_exc:
                    logger.error(
                        "[LeafLinkSync] mark_stalled_error id=%s error=%s",
                        sync_run_id,
                        _stall_exc,
                    )
        else:
            # No next cursor — pagination complete, safe to mark completed
            logger.info(
                "[LeafLinkSync] completed_no_cursor id=%s brand=%s total_loaded=%s",
                sync_run_id,
                brand_id,
                total_orders_synced,
            )
            if sync_run_id:
                try:
                    async with AsyncSessionLocal() as _done_db:
                        async with _done_db.begin():
                            await _srm_mark_completed(_done_db, sync_run_id)
                except Exception as _done_exc:
                    logger.error(
                        "[LeafLinkSync] mark_completed_error id=%s error=%s",
                        sync_run_id,
                        _done_exc,
                    )

        logger.info(
            "[OrdersSync] bg_sync_complete brand=%s final_page=%s total_pages=%s "
            "total_orders=%s duration_seconds=%s sync_run_id=%s",
            brand_id,
            final_page,
            total_pages,
            total_orders_synced,
            total_duration,
            sync_run_id,
        )

    except Exception as e:
        logger.error(
            "[LeafLinkSync] FATAL ERROR id=%s error=%s",
            sync_run_id,
            str(e)[:500],
            exc_info=True,
        )
        sys.stdout.flush()
        raise
