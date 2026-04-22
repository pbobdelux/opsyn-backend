from datetime import datetime, timezone
from typing import Dict, Any

import logging
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import BrandAPICredential, Order
from leaflink_client import LeafLinkClient

logger = logging.getLogger("leaflink_sync")


def utc_now():
    return datetime.now(timezone.utc)


async def sync_leaflink_orders(db: AsyncSession, brand_id: str):
    now = utc_now()

    result = await db.execute(
        select(BrandAPICredential).where(
            BrandAPICredential.brand_id == brand_id,
            BrandAPICredential.integration_name == "leaflink",
            BrandAPICredential.is_active == True,
        )
    )
    cred = result.scalar_one_or_none()

    if not cred or not cred.api_key:
        raise Exception(f"No active LeafLink credentials for {brand_id}")

    try:
        client = LeafLinkClient(
            api_key=cred.api_key,
            base_url=cred.base_url,
            company_id=cred.company_id,
        )
        orders = client.fetch_recent_orders(max_pages=5, normalize=True)

        created = 0
        updated = 0
        skipped = 0

        for o in orders:
            if not isinstance(o, dict):
                skipped += 1
                continue

            external_id = o.get("external_id")
            if not external_id:
                skipped += 1
                continue

            customer_name = o.get("customer_name") or "Unknown Customer"
            status = o.get("status") or "submitted"
            order_number = o.get("order_number")

            try:
                total_cents = int(float(o.get("total_amount", 0) or 0) * 100)
            except (TypeError, ValueError):
                total_cents = 0

            try:
                item_count = int(o.get("item_count", 0) or 0)
            except (TypeError, ValueError):
                item_count = 0

            try:
                unit_count = int(o.get("unit_count", 0) or 0)
            except (TypeError, ValueError):
                unit_count = 0

            line_items = o.get("line_items", [])
            raw_payload = o.get("raw_payload") or o

            result = await db.execute(
                select(Order).where(
                    Order.brand_id == brand_id,
                    Order.external_order_id == str(external_id),
                    Order.source == "leaflink",
                )
            )
            existing = result.scalar_one_or_none()

            if existing:
                existing.customer_name = customer_name
                existing.status = status
                existing.total_cents = total_cents
                existing.order_number = order_number
                existing.item_count = item_count
                existing.unit_count = unit_count
                existing.line_items_json = line_items
                existing.raw_payload = raw_payload
                existing.synced_at = now
                updated += 1
            else:
                db.add(
                    Order(
                        brand_id=brand_id,
                        external_order_id=str(external_id),
                        order_number=order_number,
                        customer_name=customer_name,
                        status=status,
                        total_cents=total_cents,
                        item_count=item_count,
                        unit_count=unit_count,
                        line_items_json=line_items,
                        raw_payload=raw_payload,
                        source="leaflink",
                        synced_at=now,
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
            "message": f"Synced {created + updated} orders",
        }

    except Exception as e:
        await db.rollback()
        logger.exception("LeafLink sync failed")
        return {"ok": False, "error": str(e)}