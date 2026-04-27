import logging
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from models import Order, OrderLine
from utils.json_utils import make_json_safe

logger = logging.getLogger("leaflink_sync")


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
    oldest_order_date: datetime | None = None

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

            # Track the newest and oldest order dates for the response.
            for ts_dt in (external_updated_at, external_created_at):
                if ts_dt:
                    if newest_order_date is None or ts_dt > newest_order_date:
                        newest_order_date = ts_dt
                    if oldest_order_date is None or ts_dt < oldest_order_date:
                        oldest_order_date = ts_dt
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
        logger.info(
            "leaflink: sync_date_range brand_id=%s oldest_order_date=%s newest_order_date=%s",
            brand_id,
            oldest_order_date.isoformat() if oldest_order_date else None,
            newest_order_date.isoformat() if newest_order_date else None,
        )
        logger.info(
            "leaflink: upsert_behavior brand_id=%s — orders upserted by (brand_id, external_order_id); "
            "raw_payload preserved on every sync; line items deleted and re-inserted (not merged)",
            brand_id,
        )

        return {
            "ok": True,
            "orders_fetched": len(orders),
            "created": created,
            "updated": updated,
            "skipped": skipped,
            "line_items_written": total_lines_written,
            "newest_order_date": newest_order_date,
            "oldest_order_date": oldest_order_date,
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
            "oldest_order_date": oldest_order_date,
            "pages_fetched": pages_fetched,
            "sync_duration_seconds": sync_duration,
            "errors": [str(e)],
        }
