import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from models import BrandAPICredential, Order, OrderLine
from services.leaflink_client import LeafLinkClient

logger = logging.getLogger("leaflink_sync")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


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


async def sync_leaflink_orders(db: AsyncSession, brand_id: str) -> dict[str, Any]:
    try:
        result = await db.execute(
            select(BrandAPICredential).where(
                BrandAPICredential.brand_id == brand_id,
                BrandAPICredential.integration_name == "leaflink",
                BrandAPICredential.is_active == True,
            )
        )
        cred = result.scalar_one_or_none()

        if not cred:
            return {"ok": False, "error": "No active LeafLink credentials found"}

        client = LeafLinkClient(
            api_key=cred.api_key,
            company_id=cred.company_id,
        )

        orders = client.fetch_recent_orders(max_pages=5, normalize=True)

        created = 0
        updated = 0
        skipped = 0
        total_lines_written = 0

        logger.info("LeafLink returned %s orders for brand_id=%s", len(orders), brand_id)

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

            amount_decimal = safe_decimal(
                o.get("amount", o.get("total_amount"))
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
            now = utc_now()

            raw_payload = o.get("raw_payload") if isinstance(o.get("raw_payload"), dict) else o

            existing_result = await db.execute(
                select(Order).where(
                    Order.brand_id == brand_id,
                    Order.external_order_id == external_id,
                )
            )
            existing = existing_result.scalar_one_or_none()

            external_created_at = o.get("created_at")
            external_updated_at = o.get("updated_at")

            if existing:
                existing.customer_name = customer_name
                existing.status = status
                existing.order_number = order_number
                existing.total_cents = total_cents
                existing.amount = amount_decimal
                existing.item_count = item_count
                existing.unit_count = unit_count
                existing.line_items_json = raw_line_items
                existing.raw_payload = raw_payload
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
                    line_items_json=raw_line_items,
                    raw_payload=raw_payload,
                    source="leaflink",
                    review_status=review_status,
                    sync_status="ok",
                    synced_at=now,
                    last_synced_at=now,
                    external_created_at=external_created_at,
                    external_updated_at=external_updated_at,
                )
                db.add(order_row)
                await db.flush()
                created += 1

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
                        raw_payload=item.get("raw_payload"),
                    )
                )

            total_lines_written += len(normalized_line_items)

        await db.commit()

        return {
            "ok": True,
            "fetched": len(orders),
            "created": created,
            "updated": updated,
            "skipped": skipped,
            "lines_written": total_lines_written,
            "message": f"Synced {len(orders)} orders and wrote {total_lines_written} line items",
        }

    except Exception as e:
        logger.exception("LeafLink sync failed")
        await db.rollback()
        return {"ok": False, "error": str(e)}