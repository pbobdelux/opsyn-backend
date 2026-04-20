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
    if not value or not isinstance(value, str):
        return None

    try:
        if value.endswith("Z"):
            value = value.replace("Z", "+00:00")
        return datetime.fromisoformat(value)
    except Exception:
        return None


def parse_total_cents(raw: dict[str, Any]) -> int | None:
    possible_values = [
        raw.get("total_cents"),
        raw.get("total"),
        raw.get("grand_total"),
        raw.get("amount"),
    ]

    for value in possible_values:
        if value is None:
            continue

        try:
            if isinstance(value, int):
                return value
            if isinstance(value, float):
                return int(round(value * 100))
            if isinstance(value, str):
                cleaned = value.replace("$", "").replace(",", "").strip()
                if "." in cleaned:
                    return int(round(float(cleaned) * 100))
                return int(cleaned)
        except Exception:
            continue

    return None


def extract_customer_name(raw: dict[str, Any]) -> str | None:
    customer = raw.get("customer")
    if isinstance(customer, dict):
        if customer.get("name"):
            return str(customer["name"])

    account = raw.get("account")
    if isinstance(account, dict):
        if account.get("name"):
            return str(account["name"])

    dispensary = raw.get("dispensary")
    if isinstance(dispensary, dict):
        if dispensary.get("name"):
            return str(dispensary["name"])

    if raw.get("customer_name"):
        return str(raw["customer_name"])

    return None


def normalize_order(raw: dict[str, Any], brand_id: str) -> dict[str, Any]:
    external_order_id = raw.get("id") or raw.get("uuid") or raw.get("order_id")
    if external_order_id is None:
        raise ValueError("Order payload missing id/order_id")

    return {
        "brand_id": brand_id,
        "external_order_id": str(external_order_id),
        "customer_name": extract_customer_name(raw),
        "status": str(raw.get("status") or raw.get("state") or "unknown"),
        "total_cents": parse_total_cents(raw),
        "source": "leaflink",
        "raw_payload": raw,
        "external_created_at": parse_datetime(raw.get("created_at")),
        "external_updated_at": parse_datetime(raw.get("updated_at")),
        "synced_at": utc_now(),
    }


async def get_active_leaflink_credentials(db: AsyncSession, brand_id: str) -> BrandAPICredential | None:
    stmt = select(BrandAPICredential).where(
        BrandAPICredential.brand_id == brand_id,
        BrandAPICredential.integration_name == "leaflink",
        BrandAPICredential.is_active.is_(True),
    )
    result = await db.execute(stmt)
    return result.scalar_one_or_none()


async def fetch_leaflink_orders(credentials: BrandAPICredential) -> list[dict[str, Any]]:
    """
    IMPORTANT:
    This is the one function you may need to adjust if your exact working LeafLink
    auth/header pattern differs.

    Start with this version first. If LeafLink rejects auth, only edit this function.
    """

    base_url = (credentials.base_url or "https://app.leaflink.com/api/v2").rstrip("/")
    url = f"{base_url}/orders"

    headers = {
        "Accept": "application/json",
    }

    if credentials.vendor_key:
        headers["X-LeafLink-Vendor-Key"] = credentials.vendor_key

    if credentials.api_key:
        headers["Authorization"] = f"Bearer {credentials.api_key}"

    async with httpx.AsyncClient(timeout=45.0) as client:
        response = await client.get(url, headers=headers)
        response.raise_for_status()
        payload = response.json()

    if isinstance(payload, dict):
        if isinstance(payload.get("results"), list):
            return payload["results"]
        if isinstance(payload.get("data"), list):
            return payload["data"]

    if isinstance(payload, list):
        return payload

    return []


async def upsert_order(db: AsyncSession, normalized: dict[str, Any]) -> Order:
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
        existing.source = normalized["source"]
        existing.raw_payload = normalized["raw_payload"]
        existing.external_created_at = normalized["external_created_at"]
        existing.external_updated_at = normalized["external_updated_at"]
        existing.synced_at = normalized["synced_at"]
        return existing

    order = Order(**normalized)
    db.add(order)
    return order


async def sync_leaflink_orders_for_brand(db: AsyncSession, brand_id: str) -> dict[str, Any]:
    credentials = await get_active_leaflink_credentials(db, brand_id)
    if credentials is None:
        raise ValueError(f"No active LeafLink credentials found for brand_id={brand_id}")

    credentials.sync_status = "syncing"
    credentials.last_error = None
    await db.commit()

    try:
        raw_orders = await fetch_leaflink_orders(credentials)

        synced_count = 0
        for raw in raw_orders:
            if not isinstance(raw, dict):
                continue

            normalized = normalize_order(raw, brand_id)
            await upsert_order(db, normalized)
            synced_count += 1

        credentials.sync_status = "ok"
        credentials.last_sync_at = utc_now()
        credentials.last_error = None
        await db.commit()

        return {
            "ok": True,
            "brand_id": brand_id,
            "fetched": len(raw_orders),
            "synced": synced_count,
        }

    except Exception as exc:
        credentials.sync_status = "error"
        credentials.last_error = str(exc)
        await db.commit()
        logger.exception("LeafLink sync failed for brand_id=%s", brand_id)
        raise