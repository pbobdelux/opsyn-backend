from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional
import httpx
import logging
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import BrandAPICredential, Order
from leaflink_client import LeafLinkClient

logger = logging.getLogger("leaflink_sync")

def utc_now():
    return datetime.now(timezone.utc)

def _clean_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        return value or None
    return str(value).strip() or None

def _first_non_empty(*values: Any) -> Optional[str]:
    for value in values:
        cleaned = _clean_str(value)
        if cleaned:
            return cleaned
    return None

def _to_decimal(value: Any) -> Optional[Decimal]:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    if isinstance(value, str):
        stripped = value.strip().replace("$", "").replace(",", "")
        if not stripped:
            return None
        try:
            return Decimal(stripped)
        except:
            return None
    return None

def _to_int(value: Any, default: int = 0) -> int:
    if value is None or value == "":
        return default
    try:
        return int(float(str(value).replace(",", "")))
    except:
        return default

def normalize_status(raw_status: Any) -> str:
    status = _clean_str(raw_status)
    if not status:
        return "unknown"
    s = status.lower()
    if s in {"submitted", "pending", "new"}:
        return "submitted"
    if s in {"accepted", "approved", "confirmed"}:
        return "accepted"
    if s in {"ready", "packed"}:
        return "ready"
    if s in {"shipped", "fulfilled"}:
        return "shipped"
    if s in {"complete", "completed"}:
        return "complete"
    if s in {"cancelled", "canceled"}:
        return "cancelled"
    return s

async def sync_leaflink_orders(db: AsyncSession, brand_id: str):
    now = utc_now()

    # Fetch LeafLink credentials for this brand
    result = await db.execute(
        select(BrandAPICredential).where(
            BrandAPICredential.brand_id == brand_id,
            BrandAPICredential.integration_name == "leaflink",
            BrandAPICredential.is_active == True,
        )
    )
    cred = result.scalar_one_or_none()

    if not cred or not cred.api_key:
        raise Exception(f"No active LeafLink credentials found for brand {brand_id}")

    try:
        client = LeafLinkClient(api_key=cred.api_key, base_url=cred.base_url)
        raw_orders = await client.fetch_recent_orders(max_pages=5, normalize=False)

        created = 0
        updated = 0

        for raw in raw_orders:
            if not isinstance(raw, dict):
                continue

            external_id = _first_non_empty(
                raw.get("id"), raw.get("number"), raw.get("short_id"), raw.get("uuid")
            )
            if not external_id:
                continue

            customer_name = _first_non_empty(
                raw.get("customer_name"),
                raw.get("buyer_name"),
                raw.get("dispensary_name"),
                raw.get("store_name"),
            ) or "Unknown Customer"

            status = normalize_status(raw.get("status") or raw.get("classification"))
            total_cents = int((_to_decimal(raw.get("total_amount")) or 0) * 100)

            # Check if order already exists
            existing_result = await db.execute(
                select(Order).where(
                    Order.brand_id == brand_id,
                    Order.external_order_id == str(external_id),
                    Order.source == "leaflink",
                )
            )
            existing_order = existing_result.scalar_one_or_none()

            if existing_order:
                existing_order.customer_name = customer_name
                existing_order.status = status
                existing_order.total_cents = total_cents
                existing_order.synced_at = now
                updated += 1
            else:
                new_order = Order(
                    brand_id=brand_id,
                    external_order_id=str(external_id),
                    customer_name=customer_name,
                    status=status,
                    total_cents=total_cents,
                    source="leaflink",
                    synced_at=now,
                )
                db.add(new_order)
                created += 1

        await db.commit()

        return {
            "ok": True,
            "fetched": len(raw_orders),
            "created": created,
            "updated": updated,
            "message": f"Successfully synced {created + updated} orders for brand {brand_id}",
        }

    except Exception as e:
        await db.rollback()
        logger.exception("LeafLink sync failed for brand %s", brand_id)
        return {"ok": False, "error": str(e)}