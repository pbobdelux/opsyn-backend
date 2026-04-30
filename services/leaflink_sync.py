import asyncio
import logging
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Optional

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from database import AsyncSessionLocal
from models import Order, OrderLine
from utils.json_utils import make_json_safe

if TYPE_CHECKING:
    from services.background_sync_manager import BackgroundSyncManager

logger = logging.getLogger("leaflink_sync")

# Number of order headers committed per batch in Phase 1
HEADER_BATCH_SIZE = 25


def utc_now() -> datetime:
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


def safe_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


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
) -> dict[str, Any]:
    """Upsert *pre-fetched* LeafLink orders and their line items.

    The caller is responsible for:
    - Credential lookup and API fetch (before calling this function)
    - Owning the surrounding ``async with db.begin():`` transaction block

    This function performs only DB writes (no credential lookup, no HTTP).
    It must be called inside an active transaction — do NOT call db.commit()
    or db.rollback() here.

    Args:
        db: Active async database session (inside a transaction).
        brand_id: Brand slug / ID used to scope orders.
        orders: Pre-fetched, normalised order dicts from LeafLink.
        pages_fetched: Number of API pages retrieved (for metadata).
    """
    sync_start = time.monotonic()
    logger.info("[LeafLinkSync] upsert_start transaction_active=%s", db.in_transaction())
    logger.info(
        "[LeafLink] sync_start brand=%s orders=%s pages_fetched=%s",
        brand_id,
        len(orders),
        pages_fetched,
    )
    logger.info("leaflink: sync_start brand_id=%s orders=%s pages_fetched=%s", brand_id, len(orders), pages_fetched)
    logger.info("leaflink: sync_json_sanitized brand_id=%s", brand_id)

    using_mock = any(isinstance(o, dict) and o.get("mock_data") for o in orders)
    if using_mock:
        logger.warning(
            "leaflink: sync_using_mock_data brand_id=%s — real API unavailable, MOCK_MODE active",
            brand_id,
        )
    else:
        logger.info(
            "leaflink: sync_using_real_api_data brand_id=%s orders_fetched=%s",
            brand_id,
            len(orders),
        )

    logger.info("leaflink: sync_orders_processed brand_id=%s count=%s", brand_id, len(orders))

    created = 0
    updated = 0
    skipped = 0
    total_lines_written = 0
    errors: list[str] = []
    newest_order_date: datetime | None = None

    # ------------------------------------------------------------------
    # Upsert orders and write line items using the caller's transaction.
    # No begin() here — the route handler owns the transaction.
    # ------------------------------------------------------------------
    try:
        logger.info(
            "leaflink: sync_api_response brand_id=%s orders_count=%s mock_data=%s",
            brand_id,
            len(orders),
            using_mock,
        )

        for o in orders:
            if not isinstance(o, dict):
                skipped += 1
                continue

            external_id = safe_str(o.get("external_id"))
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

            # Log the mapping result
            if amount_decimal is not None:
                logger.info(
                    "leaflink: sync_amount_mapped external_id=%s amount=%s",
                    external_id,
                    amount_decimal,
                )
            else:
                logger.warning(
                    "leaflink: sync_amount_missing external_id=%s — no pricing field found in order",
                    external_id,
                )

            total_cents = decimal_to_cents(amount_decimal) or 0

            item_count = safe_int(o.get("item_count"), default=0)
            unit_count = safe_int(o.get("unit_count"), default=0)

            raw_line_items = o.get("line_items", [])
            normalized_line_items = normalize_line_items(raw_line_items)

            if normalized_line_items:
                logger.info(
                    "leaflink: sync_line_items_extracted external_id=%s count=%s unit_count=%s",
                    external_id,
                    len(normalized_line_items),
                    sum(item.get("quantity", 0) or 0 for item in normalized_line_items),
                )
            else:
                logger.warning(
                    "leaflink: sync_line_items_empty external_id=%s — no line items found in order",
                    external_id,
                )

            if item_count == 0:
                item_count = len(normalized_line_items)

            if unit_count == 0:
                unit_count = sum(item.get("quantity", 0) or 0 for item in normalized_line_items)

            review_status = derive_review_status(normalized_line_items)
            now = utc_now()

            raw_payload = o.get("raw_payload") if isinstance(o.get("raw_payload"), dict) else o

            existing_result = await db.execute(
                select(Order).where(
                    Order.brand_id == brand_id,
                    Order.external_order_id == external_id,
                )
            )
            existing = existing_result.scalar_one_or_none()

            external_created_at = parse_dt(o.get("created_at"))
            external_updated_at = parse_dt(o.get("updated_at"))

            # Track the newest order date for the response.
            for ts_dt in (external_updated_at, external_created_at):
                if ts_dt:
                    if newest_order_date is None or ts_dt > newest_order_date:
                        newest_order_date = ts_dt
                    break

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
                existing.synced_at = now
                existing.last_synced_at = now
                existing.external_created_at = external_created_at
                existing.external_updated_at = external_updated_at
                order_row = existing
                updated += 1
            else:
                order_row = Order(
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
                    synced_at=now,
                    last_synced_at=now,
                    external_created_at=external_created_at,
                    external_updated_at=external_updated_at,
                )
                db.add(order_row)
                # Flush to get the auto-generated order_row.id before writing lines.
                await db.flush()
                created += 1

            # Delete stale line items then insert fresh ones.
            await db.execute(
                delete(OrderLine).where(OrderLine.order_id == order_row.id)
            )

            for item in normalized_line_items:
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
                    )
                )

            total_lines_written += len(normalized_line_items)

        logger.info(
            "leaflink: sync_line_items_written brand_id=%s count=%s",
            brand_id,
            total_lines_written,
        )
        sync_duration = round(time.monotonic() - sync_start, 2)
        logger.info(
            "[LeafLink] upserted=%s created=%s updated=%s skipped=%s brand=%s",
            len(orders),
            created,
            updated,
            skipped,
            brand_id,
        )
        logger.info(
            "[LeafLink] sync_complete duration=%ss brand=%s pages=%s",
            sync_duration,
            brand_id,
            pages_fetched,
        )
        logger.info(
            "leaflink: sync_complete brand_id=%s fetched=%s created=%s updated=%s skipped=%s lines_written=%s pages_fetched=%s duration_seconds=%s mock_data=%s",
            brand_id,
            len(orders),
            created,
            updated,
            skipped,
            total_lines_written,
            pages_fetched,
            sync_duration,
            using_mock,
        )

        return {
            "ok": True,
            "orders_fetched": len(orders),
            "created": created,
            "updated": updated,
            "skipped": skipped,
            "line_items_written": total_lines_written,
            "newest_order_date": newest_order_date,
            "pages_fetched": pages_fetched,
            "sync_duration_seconds": sync_duration,
            "errors": errors,
            "mock_data": using_mock,
            "message": f"Synced {len(orders)} orders and wrote {total_lines_written} line items",
        }

    except Exception as e:
        sync_duration = round(time.monotonic() - sync_start, 2)
        logger.error(
            "leaflink: sync_failed brand_id=%s error=%s duration_seconds=%s",
            brand_id,
            e,
            sync_duration,
            exc_info=True,
        )
        return {
            "ok": False,
            "error": str(e),
            "orders_fetched": len(orders),
            "created": created,
            "updated": updated,
            "skipped": skipped,
            "line_items_written": total_lines_written,
            "newest_order_date": newest_order_date,
            "pages_fetched": pages_fetched,
            "sync_duration_seconds": sync_duration,
            "errors": [str(e)],
        }


async def sync_leaflink_orders_headers_only(
    brand_id: str,
    orders: list[dict],
    pages_fetched: int = 0,
) -> dict[str, Any]:
    """Upsert ONLY order headers (Order table) for Phase 1 — no line items.

    Opens its own DB sessions and commits in small batches of HEADER_BATCH_SIZE
    so that no single transaction holds locks for more than a handful of rows.
    Line item data is preserved in ``line_items_json`` on the Order row so the
    background worker can read it back without re-fetching from LeafLink.

    Returns a summary dict compatible with the existing sync_result contract.
    """
    sync_start = time.monotonic()
    logger.info(
        "[LeafLinkSync] headers_only_start brand=%s orders=%s pages_fetched=%s",
        brand_id,
        len(orders),
        pages_fetched,
    )

    created = 0
    updated = 0
    skipped = 0
    errors: list[str] = []
    newest_order_date: datetime | None = None

    # Split orders into batches of HEADER_BATCH_SIZE
    batches = [
        orders[i : i + HEADER_BATCH_SIZE]
        for i in range(0, len(orders), HEADER_BATCH_SIZE)
    ]

    for batch in batches:
        batch_start = time.monotonic()
        batch_size = len(batch)
        logger.info("[OrdersSync] upsert_batch_start size=%s", batch_size)

        try:
            async with AsyncSessionLocal() as db:
                async with db.begin():
                    for o in batch:
                        if not isinstance(o, dict):
                            skipped += 1
                            continue

                        external_id = safe_str(o.get("external_id"))
                        if not external_id:
                            skipped += 1
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

                        external_created_at = parse_dt(o.get("created_at"))
                        external_updated_at = parse_dt(o.get("updated_at"))

                        for ts_dt in (external_updated_at, external_created_at):
                            if ts_dt:
                                if newest_order_date is None or ts_dt > newest_order_date:
                                    newest_order_date = ts_dt
                                break

                        existing_result = await db.execute(
                            select(Order).where(
                                Order.brand_id == brand_id,
                                Order.external_order_id == external_id,
                            )
                        )
                        existing = existing_result.scalar_one_or_none()

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
                            existing.synced_at = now
                            existing.last_synced_at = now
                            existing.external_created_at = external_created_at
                            existing.external_updated_at = external_updated_at
                            updated += 1
                        else:
                            db.add(
                                Order(
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
                                    synced_at=now,
                                    last_synced_at=now,
                                    external_created_at=external_created_at,
                                    external_updated_at=external_updated_at,
                                )
                            )
                            created += 1

        except Exception as batch_exc:
            err_msg = str(batch_exc)
            logger.error(
                "[OrdersSync] upsert_batch_error brand=%s batch_size=%s error=%s",
                brand_id,
                batch_size,
                err_msg,
                exc_info=True,
            )
            errors.append(err_msg)
            skipped += batch_size
            continue

        batch_ms = round((time.monotonic() - batch_start) * 1000)
        logger.info("[OrdersSync] upsert_batch_done size=%s duration_ms=%s", batch_size, batch_ms)

    sync_duration = round(time.monotonic() - sync_start, 2)
    logger.info(
        "[LeafLinkSync] headers_only_complete brand=%s created=%s updated=%s skipped=%s duration=%ss",
        brand_id,
        created,
        updated,
        skipped,
        sync_duration,
    )

    return {
        "ok": len(errors) == 0,
        "orders_fetched": len(orders),
        "created": created,
        "updated": updated,
        "skipped": skipped,
        "line_items_written": 0,
        "newest_order_date": newest_order_date,
        "pages_fetched": pages_fetched,
        "sync_duration_seconds": sync_duration,
        "errors": errors,
        "mock_data": any(isinstance(o, dict) and o.get("mock_data") for o in orders),
        "message": f"Upserted {created + updated} order headers ({created} new, {updated} updated)",
    }


async def sync_leaflink_line_items(
    brand_id: str,
    orders: list[dict],
) -> dict[str, Any]:
    """Write OrderLine rows for a list of pre-fetched orders (background Phase 2).

    Reads the order IDs from the DB (by brand_id + external_order_id), then
    deletes stale OrderLine rows and inserts fresh ones.  Runs in its own
    session so it never blocks the HTTP response.
    """
    bg_start = time.monotonic()
    total_lines_written = 0
    errors: list[str] = []

    logger.info(
        "[OrdersSync] line_items_deferred count=%s",
        len(orders),
    )

    for o in orders:
        if not isinstance(o, dict):
            continue

        external_id = safe_str(o.get("external_id"))
        if not external_id:
            continue

        raw_line_items = o.get("line_items", [])
        normalized_line_items = normalize_line_items(raw_line_items)

        if not normalized_line_items:
            continue

        try:
            async with AsyncSessionLocal() as db:
                async with db.begin():
                    result = await db.execute(
                        select(Order).where(
                            Order.brand_id == brand_id,
                            Order.external_order_id == external_id,
                        )
                    )
                    order_row = result.scalar_one_or_none()
                    if order_row is None:
                        logger.warning(
                            "[OrdersSync] line_items_order_not_found brand=%s external_id=%s",
                            brand_id,
                            external_id,
                        )
                        continue

                    await db.execute(
                        delete(OrderLine).where(OrderLine.order_id == order_row.id)
                    )

                    for item in normalized_line_items:
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
                            )
                        )

                    total_lines_written += len(normalized_line_items)

        except Exception as line_exc:
            err_msg = str(line_exc)
            logger.error(
                "[OrdersSync] line_items_error brand=%s external_id=%s error=%s",
                brand_id,
                external_id,
                err_msg,
                exc_info=True,
            )
            errors.append(err_msg)

    bg_duration = round(time.monotonic() - bg_start, 2)
    logger.info(
        "[OrdersSync] line_items_complete brand=%s lines_written=%s duration=%ss errors=%s",
        brand_id,
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
    total_pages: int,
    manager: Optional["BackgroundSyncManager"] = None,
    total_orders_available: Optional[int] = None,
) -> None:
    """
    Fetch all remaining LeafLink pages in adaptive batches and upsert to DB.

    Designed to run as a fire-and-forget asyncio task after Phase 1 completes.
    Progress is persisted to BrandAPICredential after every batch so the sync
    can resume from the last completed page if the service restarts.

    Batch size adapts based on observed fetch latency:
      - < 5 s per batch  → increase to _BG_BATCH_MAX (5 pages)
      - > 15 s per batch → decrease to _BG_BATCH_MIN (3 pages)
      - otherwise        → keep at _BG_BATCH_DEFAULT (4 pages)

    Args:
        brand_id:    Brand slug / ID.
        api_key:     LeafLink API key.
        company_id:  LeafLink company ID.
        start_page:  First page to fetch (Phase 1 already fetched pages before this).
        total_pages: Total pages reported by LeafLink API.
        manager:     Optional BackgroundSyncManager instance for in-memory tracking.
    """
    from services.leaflink_client import LeafLinkClient

    logger.info(
        "[OrdersSync] bg_continuous_sync_started brand=%s start_page=%s total_pages=%s",
        brand_id,
        start_page,
        total_pages,
    )

    bg_start = time.monotonic()
    current_page = start_page
    batch_size = _BG_BATCH_DEFAULT
    total_orders_synced = 0
    resume_url: Optional[str] = None

    logger.info(
        "[OrdersSync] bg_sync_start brand=%s start_page=%s total_pages=%s",
        brand_id,
        start_page,
        total_pages,
    )

    try:
        client = LeafLinkClient(api_key=api_key, company_id=company_id, brand_id=brand_id)
    except Exception as client_exc:
        logger.error(
            "[OrdersSync] bg_sync_client_init_error brand=%s error=%s",
            brand_id,
            client_exc,
        )
        if manager:
            await manager.persist_progress(
                brand_id=brand_id,
                last_synced_page=start_page - 1,
                total_pages=total_pages,
                sync_status="error",
                error=str(client_exc),
                total_orders_available=total_orders_available,
            )
        return

    loop = asyncio.get_event_loop()

    while current_page <= total_pages:
        # Hard timeout guard
        elapsed_total = time.monotonic() - bg_start
        if elapsed_total > _BG_SYNC_TIMEOUT:
            logger.warning(
                "[OrdersSync] bg_sync_timeout brand=%s elapsed=%.1fs pages_done=%s/%s",
                brand_id,
                elapsed_total,
                current_page - start_page,
                total_pages - start_page + 1,
            )
            break

        logger.info(
            "[OrdersSync] bg_batch_start page=%s batch_size=%s",
            current_page,
            batch_size,
        )
        batch_start = time.monotonic()

        # ------------------------------------------------------------------ #
        # Fetch a batch of pages from LeafLink (runs in thread pool)          #
        # ------------------------------------------------------------------ #
        _capture_page = current_page
        _capture_url = resume_url
        _capture_batch = batch_size

        def _fetch_batch():
            return client.fetch_orders_page_range(
                start_page=_capture_page,
                num_pages=_capture_batch,
                page_size=HEADER_BATCH_SIZE * 4,  # 100 orders per page
                normalize=True,
                brand=brand_id,
                resume_url=_capture_url,
            )

        try:
            fetch_result = await loop.run_in_executor(None, _fetch_batch)
        except Exception as fetch_exc:
            err_msg = str(fetch_exc)
            logger.error(
                "[OrdersSync] bg_batch_fetch_error brand=%s page=%s error=%s",
                brand_id,
                current_page,
                err_msg,
                exc_info=True,
            )
            if manager:
                manager.record_page_complete(brand_id, current_page - 1, error=err_msg)
            # Skip this batch and advance to avoid infinite loop
            current_page += batch_size
            await asyncio.sleep(2)
            continue

        batch_orders = fetch_result.get("orders", [])
        pages_fetched_this_batch = fetch_result.get("pages_fetched", batch_size)
        resume_url = fetch_result.get("next_url")
        next_page = fetch_result.get("next_page")

        batch_fetch_ms = round((time.monotonic() - batch_start) * 1000)
        logger.info(
            "[OrdersSync] bg_batch_complete page=%s orders=%s duration_ms=%s",
            current_page,
            len(batch_orders),
            batch_fetch_ms,
        )

        # ------------------------------------------------------------------ #
        # Upsert order headers (headers-only, line items deferred)            #
        # ------------------------------------------------------------------ #
        if batch_orders:
            try:
                await sync_leaflink_orders_headers_only(
                    brand_id=brand_id,
                    orders=batch_orders,
                    pages_fetched=pages_fetched_this_batch,
                )

                # Defer line items to a separate fire-and-forget task
                _li_orders = batch_orders

                async def _write_line_items():
                    await sync_leaflink_line_items(brand_id, _li_orders)

                asyncio.create_task(_write_line_items())

            except Exception as upsert_exc:
                logger.error(
                    "[OrdersSync] bg_batch_upsert_error brand=%s page=%s error=%s",
                    brand_id,
                    current_page,
                    upsert_exc,
                    exc_info=True,
                )

        total_orders_synced += len(batch_orders)
        last_completed_page = (next_page - 1) if next_page else total_pages

        # ------------------------------------------------------------------ #
        # Persist progress to DB                                              #
        # ------------------------------------------------------------------ #
        if manager:
            manager.record_page_complete(brand_id, last_completed_page)
            await manager.persist_progress(
                brand_id=brand_id,
                last_synced_page=last_completed_page,
                total_pages=total_pages,
                sync_status="syncing",
                total_orders_available=total_orders_available,
            )

        percent = round((last_completed_page / total_pages) * 100, 1) if total_pages else 0
        logger.info(
            "[OrdersSync] bg_batch_complete page=%s total_pages=%s percent=%.1f%%",
            last_completed_page,
            total_pages,
            percent,
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
    final_page = min(current_page - 1, total_pages)

    if manager:
        await manager.persist_progress(
            brand_id=brand_id,
            last_synced_page=final_page,
            total_pages=total_pages,
            sync_status="complete",
            total_orders_available=total_orders_available,
        )

    logger.info(
        "[OrdersSync] bg_sync_complete brand=%s final_page=%s total_pages=%s total_orders=%s duration_seconds=%s",
        brand_id,
        final_page,
        total_pages,
        total_orders_synced,
        total_duration,
    )
