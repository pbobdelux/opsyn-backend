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
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None

        if raw.endswith("Z"):
            raw = raw.replace("Z", "+00:00")

        try:
            dt = datetime.fromisoformat(raw)
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            return None

    return None


def parse_money_to_cents(value: Any) -> int:
    if value is None:
        return 0

    if isinstance(value, int):
        return value

    if isinstance(value, float):
        return int(round(value * 100))

    if isinstance(value, str):
        cleaned = value.replace("$", "").replace(",", "").strip()
        if not cleaned:
            return 0
        try:
            return int(round(float(cleaned) * 100))
        except ValueError:
            return 0

    return 0


def extract_total_cents(raw: dict[str, Any]) -> int:
    candidates = [
        raw.get("total_cents"),
        raw.get("total"),
        raw.get("grand_total"),
        raw.get("amount"),
        raw.get("subtotal"),
    ]

    for candidate in candidates:
        cents = parse_money_to_cents(candidate)
        if cents:
            return cents

    return 0


def extract_customer_name(raw: dict[str, Any]) -> str | None:
    customer = raw.get("customer")
    if isinstance(customer, dict) and customer.get("name"):
        return str(customer.get("name"))

    account = raw.get("account")
    if isinstance(account, dict) and account.get("name"):
        return str(account.get("name"))

    dispensary = raw.get("dispensary")
    if isinstance(dispensary, dict) and dispensary.get("name"):
        return str(dispensary.get("name"))

    if raw.get("customer_name"):
        return str(raw.get("customer_name"))

    return None


def normalize_leaflink_order(raw: dict[str, Any], brand_id: str) -> dict[str, Any]:
    external_order_id = (
        raw.get("id")
        or raw.get("uuid")
        or raw.get("order_id")
        or raw.get("number")
    )

    return {
        "brand_id": brand_id,
        "external_order_id": str(external_order_id) if external_order_id is not None else None,
        "customer_name": extract_customer_name(raw),
        "status": str(raw.get("status") or raw.get("state") or "unknown"),
        "total_cents": extract_total_cents(raw),
        "source": "leaflink",
        "raw_payload": raw,
        "external_created_at": parse_datetime(raw.get("created_at")),
        "external_updated_at": parse_datetime(raw.get("updated_at")),
        "synced_at": utc_now(),
    }


async def get_active_leaflink_credentials(
    db: AsyncSession,
    brand_id: str,
) -> BrandAPICredential | None:
    stmt = select(BrandAPICredential).where(
        BrandAPICredential.brand_id == brand_id,
        BrandAPICredential.integration_name == "leaflink",
        BrandAPICredential.is_active.is_(True),
    )
    result = await db.execute(stmt)
    return result.scalar_one_or_none()


async def fetch_leaflink_orders(
    credentials: BrandAPICredential,
) -> list[dict[str, Any]]:
    base_url = (credentials.base_url or "https://app.leaflink.com/api/v2").rstrip("/")
    url = f"{base_url}/orders"

    headers: dict[str, str] = {
        "Accept": "application/json",
    }

    if getattr(credentials, "vendor_key", None):
        headers["X-LeafLink-Vendor-Key"] = credentials.vendor_key

    if getattr(credentials, "api_key", None):
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

    logger.warning("Unexpected LeafLink payload shape: %s", type(payload).__name__)
    return []


async def upsert_leaflink_orders(
    db: AsyncSession,
    brand_id: str,
    raw_orders: list[dict[str, Any]],
) -> int:
    synced_count = 0

    for raw in raw_orders:
        if not isinstance(raw, dict):
            continue

        normalized = normalize_leaflink_order(raw, brand_id)

        if not normalized["external_order_id"]:
            logger.warning("Skipping LeafLink order with no id: %s", raw)
            continue

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
        else:
            order = Order(
                brand_id=normalized["brand_id"],
                external_order_id=normalized["external_order_id"],
                customer_name=normalized["customer_name"],
                status=normalized["status"],
                total_cents=normalized["total_cents"],
                source=normalized["source"],
                raw_payload=normalized["raw_payload"],
                external_created_at=normalized["external_created_at"],
                external_updated_at=normalized["external_updated_at"],
                synced_at=normalized["synced_at"],
            )
            db.add(order)

        synced_count += 1

    return synced_count


async def sync_leaflink_orders(db: AsyncSession, brand_id: str) -> dict[str, Any]:
    credentials = await get_active_leaflink_credentials(db, brand_id)

    if not credentials:
        raise ValueError(f"No active LeafLink credential found for brand_id={brand_id}")

    try:
        credentials.sync_status = "syncing"
        credentials.last_error = None
        await db.commit()

        raw_orders = await fetch_leaflink_orders(credentials)
        synced_count = await upsert_leaflink_orders(db, brand_id, raw_orders)

        credentials.sync_status = "ok"
        credentials.last_sync_at = utc_now()
        credentials.last_error = None
        await db.commit()

        return {
            "ok": True,
            "brand_id": brand_id,
            "source": "leaflink",
            "orders_received": len(raw_orders),
            "orders_synced": synced_count,
        }

    except Exception as exc:
        await db.rollback()

        credentials.sync_status = "error"
        credentials.last_error = str(exc)
        await db.commit()

        logger.exception("LeafLink sync failed for brand_id=%s", brand_id)
        raise