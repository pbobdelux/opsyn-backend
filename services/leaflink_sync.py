import logging
from datetime import datetime, timezone
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import BrandAPICredential, Order

logger = logging.getLogger("opsyn-backend")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None

    if isinstance(value, datetime):
        return value

    if isinstance(value, str):
        value = value.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(value)
        except:
            return None

    return None


def parse_money_to_cents(value: Any) -> int:
    if value is None:
        return 0

    if isinstance(value, (int, float)):
        return int(float(value) * 100)

    if isinstance(value, str):
        cleaned = value.replace("$", "").replace(",", "").strip()
        try:
            return int(float(cleaned) * 100)
        except:
            return 0

    return 0


def normalize_order(raw: dict[str, Any], brand_id: str):
    return {
        "brand_id": brand_id,
        "external_order_id": str(
            raw.get("id")
            or raw.get("uuid")
            or raw.get("order_id")
            or raw.get("number")
        ),
        "customer_name": raw.get("customer_name"),
        "status": str(raw.get("status") or raw.get("state") or "unknown"),
        "total_cents": parse_money_to_cents(
            raw.get("total") or raw.get("amount") or raw.get("grand_total")
        ),
        "source": "leaflink",
        "raw_payload": raw,
        "external_created_at": parse_datetime(raw.get("created_at")),
        "external_updated_at": parse_datetime(raw.get("updated_at")),
        "synced_at": utc_now(),
    }


async def get_active_leaflink_credentials(db: AsyncSession, brand_id: str):
    stmt = select(BrandAPICredential).where(
        BrandAPICredential.brand_id == brand_id,
        BrandAPICredential.integration_name == "leaflink",
        BrandAPICredential.is_active.is_(True),
    )
    result = await db.execute(stmt)
    return result.scalar_one_or_none()


async def fetch_leaflink_orders(credentials: BrandAPICredential):
    base_url = (credentials.base_url or "https://app.leaflink.com/api/v2").rstrip("/")
    url = f"{base_url}/orders"

    headers = {"Accept": "application/json"}

    if credentials.vendor_key:
        headers["X-LeafLink-Vendor-Key"] = credentials.vendor_key

    if credentials.api_key:
        headers["Authorization"] = f"Bearer {credentials.api_key}"

    async with httpx.AsyncClient(timeout=45.0) as client:
        response = await client.get(url, headers=headers)

    response.raise_for_status()

    payload = response.json()

    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        if isinstance(payload.get("results"), list):
            return payload["results"]
        if isinstance(payload.get("data"), list):
            return payload["data"]
        if isinstance(payload.get("orders"), list):
            return payload["orders"]

    return []


async def upsert_orders(db: AsyncSession, brand_id: str, raw_orders):
    count = 0

    for raw in raw_orders:
        normalized = normalize_order(raw, brand_id)

        stmt = select(Order).where(
            Order.brand_id == normalized["brand_id"],
            Order.external_order_id == normalized["external_order_id"],
        )
        result = await db.execute(stmt)
        existing = result.scalar_one_or_none()

        if existing:
            existing.customer_name = normalized["customer_name"]
            existing.status = normalized["status"]
            existing.total_cents = normalized["total_cents"]
            existing.raw_payload = normalized["raw_payload"]
        else:
            order = Order(**normalized)
            db.add(order)

        count += 1

    return count


async def sync_leaflink_orders(db: AsyncSession, brand_id: str):
    credentials = await get_active_leaflink_credentials(db, brand_id)

    if not credentials:
        raise ValueError("No active LeafLink credentials found")

    try:
        raw_orders = await fetch_leaflink_orders(credentials)
        synced = await upsert_orders(db, brand_id, raw_orders)

        await db.commit()

        return {
            "ok": True,
            "orders_received": len(raw_orders),
            "orders_synced": synced,
        }

    except Exception as e:
        await db.rollback()
        logger.exception("LeafLink sync failed")
        raise e