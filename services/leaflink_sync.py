from __future__ import annotations

from datetime import datetime, timezone
import logging
from typing import Any

import httpx
from sqlalchemy import select

from models import BrandAPICredential, Order

logger = logging.getLogger("opsyn-backend.leaflink_sync")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_dt(value: Any) -> datetime | None:
    if not value:
        return None

    if isinstance(value, datetime):
        return value

    if not isinstance(value, str):
        return None

    text = value.strip()
    if not text:
        return None

    try:
        if text.endswith("Z"):
            text = text.replace("Z", "+00:00")
        return datetime.fromisoformat(text)
    except Exception:
        return None


def cents_from_amount(value: Any) -> int:
    if value is None:
        return 0

    try:
        # Handles strings like "123.45" and ints/floats
        return int(round(float(value) * 100))
    except Exception:
        return 0


def extract_order_list(payload: Any) -> list[dict]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]

    if not isinstance(payload, dict):
        return []

    # Common patterns across APIs
    for key in ("results", "data", "orders", "objects"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]

    return []


def extract_next_url(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None

    next_value = payload.get("next")
    if isinstance(next_value, str) and next_value.strip():
        return next_value.strip()

    return None


def extract_external_order_id(order_data: dict) -> str:
    candidates = [
        order_data.get("id"),
        order_data.get("short_id"),
        order_data.get("uid"),
        order_data.get("number"),
    ]
    for value in candidates:
        if value is not None and str(value).strip():
            return str(value).strip()

    raise ValueError("LeafLink order missing external id")


def extract_customer_name(order_data: dict) -> str | None:
    candidates = [
        order_data.get("customer_display_name"),
        order_data.get("customer_name"),
        order_data.get("buyer_name"),
        order_data.get("dispensary_name"),
        order_data.get("retailer_name"),
        order_data.get("ship_to_name"),
    ]
    for value in candidates:
        if isinstance(value, str) and value.strip():
            return value.strip()

    customer = order_data.get("customer")
    if isinstance(customer, dict):
        for key in ("display_name", "name", "business_name"):
            value = customer.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

    buyer = order_data.get("buyer")
    if isinstance(buyer, dict):
        for key in ("display_name", "name", "business_name"):
            value = buyer.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

    return None


def extract_status(order_data: dict) -> str:
    candidates = [
        order_data.get("status"),
        order_data.get("fulfillment_status"),
        order_data.get("state"),
    ]
    for value in candidates:
        if isinstance(value, str) and value.strip():
            return value.strip()

    return "unknown"


def extract_total_cents(order_data: dict) -> int:
    candidates = [
        order_data.get("total"),
        order_data.get("total_price"),
        order_data.get("total_amount"),
        order_data.get("subtotal"),
        order_data.get("subtotal_price"),
    ]
    for value in candidates:
        cents = cents_from_amount(value)
        if cents > 0:
            return cents

    pricing = order_data.get("pricing")
    if isinstance(pricing, dict):
        for key in ("total", "total_price", "subtotal"):
            cents = cents_from_amount(pricing.get(key))
            if cents > 0:
                return cents

    return 0


def extract_created_at(order_data: dict) -> datetime | None:
    candidates = [
        order_data.get("created_at"),
        order_data.get("date_created"),
        order_data.get("submitted_at"),
    ]
    for value in candidates:
        parsed = parse_dt(value)
        if parsed:
            return parsed
    return None


def extract_updated_at(order_data: dict) -> datetime | None:
    candidates = [
        order_data.get("updated_at"),
        order_data.get("modified_at"),
        order_data.get("last_updated"),
    ]
    for value in candidates:
        parsed = parse_dt(value)
        if parsed:
            return parsed
    return None


async def fetch_leaflink_orders(
    base_url: str,
    api_key: str,
    company_id: str | None = None,
) -> list[dict]:
    normalized_base = base_url.strip().rstrip("/")
    url = f"{normalized_base}/orders-received/"

    headers = {
        "Authorization": f"App {api_key}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    params: dict[str, Any] = {}
    if company_id:
        params["company"] = company_id

    all_orders: list[dict] = []

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        page_num = 0

        while url:
            page_num += 1
            logger.info("LeafLink fetch page %s: %s", page_num, url)

            response = await client.get(url, headers=headers, params=params if page_num == 1 else None)

            content_type = response.headers.get("content-type", "")
            text_preview = response.text[:1000] if response.text else ""

            if response.status_code >= 400:
                raise RuntimeError(
                    f"LeafLink API error {response.status_code} for {url}. "
                    f"Response preview: {text_preview}"
                )

            if "application/json" not in content_type.lower():
                raise RuntimeError(
                    f"LeafLink returned non-JSON response for {url}. "
                    f"Content-Type: {content_type}. Preview: {text_preview}"
                )

            payload = response.json()
            orders = extract_order_list(payload)
            all_orders.extend(orders)

            next_url = extract_next_url(payload)
            if next_url:
                url = next_url
            else:
                break

    return all_orders


async def sync_leaflink_orders(db, brand_id: str) -> dict:
    credential_stmt = select(BrandAPICredential).where(
        BrandAPICredential.brand_id == brand_id,
        BrandAPICredential.integration_name == "leaflink",
    )
    credential_result = await db.execute(credential_stmt)
    credential = credential_result.scalar_one_or_none()

    if credential is None:
        raise RuntimeError(f"No LeafLink credentials found for brand_id={brand_id}")

    if not credential.is_active:
        raise RuntimeError(f"LeafLink credentials are inactive for brand_id={brand_id}")

    if not credential.api_key:
        raise RuntimeError(f"LeafLink api_key is missing for brand_id={brand_id}")

    credential.sync_status = "syncing"
    credential.last_error = None
    await db.commit()

    now = utc_now()

    try:
        raw_orders = await fetch_leaflink_orders(
            base_url=credential.base_url or "https://app.leaflink.com/api/v2",
            api_key=credential.api_key,
            company_id=credential.company_id,
        )

        external_ids = []
        for item in raw_orders:
            try:
                external_ids.append(extract_external_order_id(item))
            except Exception:
                continue

        existing_orders_map: dict[str, Order] = {}
        if external_ids:
            existing_stmt = select(Order).where(
                Order.brand_id == brand_id,
                Order.source == "leaflink",
                Order.external_order_id.in_(external_ids),
            )
            existing_result = await db.execute(existing_stmt)
            existing_orders = existing_result.scalars().all()
            existing_orders_map = {
                str(order.external_order_id): order for order in existing_orders
            }

        created_count = 0
        updated_count = 0
        skipped_count = 0

        for raw in raw_orders:
            try:
                external_order_id = extract_external_order_id(raw)
            except Exception:
                skipped_count += 1
                logger.warning("Skipping LeafLink order with no usable external id: %s", raw)
                continue

            customer_name = extract_customer_name(raw)
            status = extract_status(raw)
            total_cents = extract_total_cents(raw)
            external_created_at = extract_created_at(raw)
            external_updated_at = extract_updated_at(raw)

            existing = existing_orders_map.get(external_order_id)

            if existing:
                existing.customer_name = customer_name
                existing.status = status
                existing.total_cents = total_cents
                existing.external_created_at = external_created_at
                existing.external_updated_at = external_updated_at
                existing.synced_at = now
                updated_count += 1
            else:
                order = Order(
                    brand_id=brand_id,
                    external_order_id=external_order_id,
                    customer_name=customer_name,
                    status=status,
                    total_cents=total_cents,
                    source="leaflink",
                    external_created_at=external_created_at,
                    external_updated_at=external_updated_at,
                    synced_at=now,
                )
                db.add(order)
                created_count += 1

        credential.sync_status = "ok"
        credential.last_sync_at = now
        credential.last_error = None

        await db.commit()

        return {
            "ok": True,
            "brand_id": brand_id,
            "source": "leaflink",
            "fetched": len(raw_orders),
            "created": created_count,
            "updated": updated_count,
            "skipped": skipped_count,
            "last_sync_at": now.isoformat(),
        }

    except Exception as exc:
        credential.sync_status = "error"
        credential.last_error = str(exc)
        await db.commit()
        logger.exception("LeafLink sync failed for brand_id=%s", brand_id)

        return {
            "ok": False,
            "brand_id": brand_id,
            "source": "leaflink",
            "error": str(exc),
            "last_sync_at": now.isoformat(),
        }