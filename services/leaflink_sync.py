from datetime import datetime, timezone
from typing import Any

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

    # Get active LeafLink credentials for this brand from DB
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
        # LeafLinkClient is sync; do NOT await fetch_recent_orders
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
            status = (o.get("status") or "submitted").strip().lower()
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

            external_created_at = None
            external_updated_at = None

            # Optional datetime parsing if values are present and valid ISO strings
            created_at_raw = o.get("created_at") or o.get("submitted_at")
            updated_at_raw = o.get("updated_at")

            try:
                if created_at_raw:
                    external_created_at = datetime.fromisoformat(
                        str(created_at_raw).replace("Z", "+00:00")
                    )
            except Exception:
                external_created_at = None

            try:
                if updated_at_raw:
                    external_updated_at = datetime.fromisoformat(
                        str(updated_at_raw).replace("Z", "+00:00")
                    )
            except Exception:
                external_updated_at = None

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

                if hasattr(existing, "order_number"):
                    existing.order_number = order_number

                if hasattr(existing, "item_count"):
                    existing.item_count = item_count

                if hasattr(existing, "unit_count"):
                    existing.unit_count = unit_count

                if hasattr(existing, "line_items_json"):
                    existing.line_items_json = line_items

                if hasattr(existing, "raw_payload"):
                    existing.raw_payload = raw_payload

                if hasattr(existing, "external_created_at"):
                    existing.external_created_at = external_created_at

                if hasattr(existing, "external_updated_at"):
                    existing.external_updated_at = external_updated_at

                existing.synced_at = now
                updated += 1
            else:
                payload = {
                    "brand_id": brand_id,
                    "external_order_id": str(external_id),
                    "customer_name": customer_name,
                    "status": status,
                    "total_cents": total_cents,
                    "source": "leaflink",
                    "synced_at": now,
                }

                if hasattr(Order, "order_number"):
                    payload["order_number"] = order_number

                if hasattr(Order, "item_count"):
                    payload["item_count"] = item_count

                if hasattr(Order, "unit_count"):
                    payload["unit_count"] = unit_count

                if hasattr(Order, "line_items_json"):
                    payload["line_items_json"] = line_items

                if hasattr(Order, "raw_payload"):
                    payload["raw_payload"] = raw_payload

                if hasattr(Order, "external_created_at"):
                    payload["external_created_at"] = external_created_at

                if hasattr(Order, "external_updated_at"):
                    payload["external_updated_at"] = external_updated_at

                db.add(Order(**payload))
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
        logger.exception("LeafLink sync failed for brand_id=%s", brand_id)
        return {"ok": False, "error": str(e)}