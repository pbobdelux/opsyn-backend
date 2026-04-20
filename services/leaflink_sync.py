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
            logger.warning("Could not parse datetime: %r", value)
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
            logger.warning("Could not parse money value: %r", value)
            return 0

    return 0


def extract_total_cents(raw: dict[str, Any]) -> int:
    for candidate in [
        raw.get("total_cents"),
        raw.get("total"),
        raw.get("grand_total"),
        raw.get("amount"),
        raw.get("subtotal"),
    ]:
        cents = parse_money_to_cents(candidate)
        if cents:
            return cents
    return 0


def extract_customer_name(raw: dict[str, Any]) -> str | None:
    customer = raw.get("customer")
    if isinstance(customer, dict) and customer.get("name"):
        return str(customer["name"])

    account = raw.get("account")
    if isinstance(account, dict) and account.get("name"):
        return str(account["name"])

    dispensary = raw.get("dispensary")
    if isinstance(dispensary, dict) and dispensary.get("name"):
        return str(dispensary["name"])

    if raw.get("customer_name"):
        return str(raw["customer_name"])

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
    logger.info("Looking up active LeafLink credentials for brand_id=%s", brand_id)

    stmt = select(BrandAPICredential).where(
        BrandAPICredential.brand_id == brand_id,
        BrandAPICredential.integration_name == "leaflink",
        BrandAPICredential.is_active.is_(True),
    )
    result = await db.execute(stmt)
    credential = result.scalar_one_or_none()

    if credential is None:
        logger.error("No active LeafLink credential found for brand_id=%s", brand_id)
    else:
        logger.info(
            "Found active LeafLink credential for brand_id=%s base_url=%s has_vendor_key=%s has_api_key=%s",
            brand_id,
            getattr(credential, "base_url", None),
            bool(getattr(credential, "vendor_key", None)),
            bool(getattr(credential, "api_key", None)),
        )

    return credential


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

    logger.info("Requesting LeafLink orders from url=%s", url)

    async with httpx.AsyncClient(timeout=45.0) as client:
        response = await client.get(url, headers=headers)

    logger.info("LeafLink response status=%s", response.status_code)
    logger.info("LeafLink response content-type=%s", response.headers.get("content-type"))

    if response.status_code >= 400:
        body_preview = response.text[:1000]
        logger.error("LeafLink error body preview: %s", body_preview)
        response.raise_for_status()

    try:
        payload = response.json()
    except Exception:
        logger.exception("Failed to decode LeafLink response as JSON. Body preview: %s", response.text[:1000])
        raise

    if isinstance(payload, list):
        logger.info("LeafLink payload is a list with %s items", len(payload))
        return payload

    if isinstance(payload, dict):
        if isinstance(payload.get("results"), list):
            logger.info("LeafLink payload uses results[] with %s items", len(payload["results"]))
            return payload["results"]
        if isinstance(payload.get("data"), list):
            logger.info("LeafLink payload uses data[] with %s items", len(payload["data"]))
            return payload["data"]
        if isinstance(payload.get("orders"), list):
            logger.info("LeafLink payload uses orders[] with %s items", len(payload["orders"]))
            return payload["orders"])

    logger.warning("Unexpected LeafLink payload shape: %s", type(payload).__name__)
    logger.warning("Payload preview: %s", str(payload)[:1000])
    return []


async def upsert_leaflink_orders(
    db: AsyncSession,
    brand_id: str,
    raw_orders: list[dict[str, Any]],
) -> int:
    logger.info("Beginning upsert for %s raw orders", len(raw_orders))
    synced_count = 0

    for raw in raw_orders:
        if not isinstance(raw, dict):
            logger.warning("Skipping non-dict order payload: %r", raw)
            continue

        normalized = normalize_leaflink_order(raw, brand_id)

        if not normalized["external_order_id"]:
            logger.warning("Skipping LeafLink order with no external id: %s", raw)
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

    logger.info("Finished upsert loop; synced_count=%s", synced_count)
    return synced_count


async def sync_leaflink_orders(db: AsyncSession, brand_id: str) -> dict[str, Any]:
    logger.info("Starting LeafLink sync for brand_id=%s", brand_id)

    credentials = await get_active_leaflink_credentials(db, brand_id)
    if not credentials:
        raise ValueError(f"No active LeafLink credential found for brand_id={brand_id}")

    try:
        credentials.sync_status = "syncing"
        credentials.last_error = None
        await db.commit()
        logger.info("Credential status set to syncing for brand_id=%s", brand_id)

        raw_orders = await fetch_leaflink_orders(credentials)
        synced_count = await upsert_leaflink_orders(db, brand_id, raw_orders)

        credentials.sync_status = "ok"
        credentials.last_sync_at = utc_now()
        credentials.last_error = None
        await db.commit()

        logger.info(
            "LeafLink sync completed successfully for brand_id=%s orders_received=%s orders_synced=%s",
            brand_id,
            len(raw_orders),
            synced_count,
        )

        return {
            "ok": True,
            "brand_id": brand_id,
            "source": "leaflink",
            "orders_received": len(raw_orders),
            "orders_synced": synced_count,
        }

    except Exception as exc:
        logger.exception("LeafLink sync failed for brand_id=%s", brand_id)

        await db.rollback()

        credentials.sync_status = "error"
        credentials.last_error = str(exc)
        await db.commit()

        raise


async def sync_leaflink_orders_for_brand(db: AsyncSession, brand_id: str) -> dict[str, Any]:
    return await sync_leaflink_orders(db, brand_id)