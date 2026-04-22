import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import Order, BrandAPICredential
from services.leaflink_client import LeafLinkClient

logger = logging.getLogger("leaflink_sync")


def utc_now():
    return datetime.now(timezone.utc)


# -------------------------------
# 🔥 CORE FIX: DATA EXTRACTION
# -------------------------------
def extract_order_totals(order: Dict[str, Any]):
    total_cents = 0
    item_count = 0
    unit_count = 0

    line_items = order.get("line_items") or []

    for item in line_items:
        qty = item.get("quantity") or 0
        item_count += 1
        unit_count += qty

        # PRIORITY 1: line_total
        if item.get("line_total"):
            total_cents += int(float(item["line_total"]) * 100)
            continue

        # PRIORITY 2: sale_price.amount
        sale_price = item.get("sale_price")
        if isinstance(sale_price, dict) and sale_price.get("amount"):
            total_cents += int(float(sale_price["amount"]) * qty * 100)
            continue

        # PRIORITY 3: price
        if item.get("price"):
            total_cents += int(float(item["price"]) * qty * 100)

    return total_cents, item_count, unit_count


# -------------------------------
# MAIN SYNC FUNCTION
# -------------------------------
async def sync_leaflink_orders(db: AsyncSession, brand_id: str) -> Dict[str, Any]:
    try:
        # get credentials
        result = await db.execute(
            select(BrandAPICredential).where(
                BrandAPICredential.brand_id == brand_id,
                BrandAPICredential.integration_name == "leaflink",
                BrandAPICredential.is_active == True,
            )
        )
        cred = result.scalar_one_or_none()

        if not cred:
            return {"ok": False, "error": "No LeafLink credentials found"}

        client = LeafLinkClient(
            api_key=cred.api_key,
            company_id=cred.company_id,
        )

        orders = await client.fetch_recent_orders()

        created = 0
        updated = 0
        skipped = 0

        for o in orders:
            external_id = str(o.get("id"))

            total_cents, item_count, unit_count = extract_order_totals(o)

            # customer
            customer_name = None
            if o.get("customer"):
                customer_name = o["customer"].get("name")

            if not customer_name:
                customer_name = o.get("customer_name")

            if not customer_name:
                customer_name = "Unknown Customer"

            # check existing
            existing = await db.execute(
                select(Order).where(
                    Order.brand_id == brand_id,
                    Order.external_order_id == external_id,
                )
            )
            existing = existing.scalar_one_or_none()

            if existing:
                existing.customer_name = customer_name
                existing.status = o.get("status")
                existing.total_cents = total_cents
                existing.item_count = item_count
                existing.unit_count = unit_count
                existing.raw_payload = o
                existing.synced_at = utc_now()
                updated += 1
            else:
                db.add(
                    Order(
                        brand_id=brand_id,
                        external_order_id=external_id,
                        customer_name=customer_name,
                        status=o.get("status"),
                        total_cents=total_cents,
                        item_count=item_count,
                        unit_count=unit_count,
                        source="leaflink",
                        raw_payload=o,
                        external_created_at=o.get("created_at"),
                        external_updated_at=o.get("updated_at"),
                        synced_at=utc_now(),
                    )
                )
                created += 1

        await db.commit()

        return {
            "ok": True,
            "fetched": len(orders),
            "created": created,
            "updated": updated,
            "skipped": skipped,
            "message": f"Synced {len(orders)} orders",
        }

    except Exception as e:
        logger.exception("LeafLink sync failed")
        return {"ok": False, "error": str(e)}