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
        raw = value.strip()
        if not raw:
            return None
        raw = raw.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(raw)
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
    for candidate in (
        raw.get("total_cents"),
        raw.get("total"),
        raw.get("grand_total"),
        raw.get("amount"),
        raw.get("subtotal"),
    ):
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
        or raw.get("short_id")
    )

    return {
        "brand_id": brand_id,
        "external_order_id": str(external_order_id) if external_order_id is not None else None,
        "customer_name": extract_customer_name(raw),
        "status": str(raw.get("status") or raw.get("state") or "unknown"),
        "total_cents": extract_total_cents(raw),
        "source": "leaflink",
        "raw_payload": raw,
        "external_created_at": parse_datetime(raw.get("created_at") or raw.get("created_on")),
        "external_updated_at": parse_datetime(raw.get("updated_at") or raw.get("modified")),
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


def build_headers(credentials: BrandAPICredential) -> dict[str, str]:
    headers: dict[str, str] = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "opsyn-backend",
    }

    # LeafLink legacy v2 expects: Authorization: App <API_KEY>
    if getattr(credentials, "api_key", None):
        headers["Authorization"] = f"App {credentials.api_key}"

    # Keep vendor key too in case this credential set needs it.
    if getattr(credentials, "vendor_key", None):
        headers["X-LeafLink-Vendor-Key"] = credentials.vendor_key

    return headers


def candidate_order_urls(credentials: BrandAPICredential) -> list[str]:
    raw_base = (credentials.base_url or "").strip()

    candidates: list[str] = []

    # Legacy v2 documented brand orders endpoint, trailing slash required.
    candidates.append("https://api.leaflink.com/orders-received/")
    candidates.append("https://api.leaflink.com/api/v2/orders-received/")

    if raw_base:
        base = raw_base.rstrip("/")

        # If saved base is already /api/v2, append the brand endpoint with trailing slash.
        if base.endswith("/api/v2"):
            candidates.append(f"{base}/orders-received/")
        else:
            candidates.append(f"{base}/orders-received/")
            candidates.append(f"{base}/api/v2/orders-received/")

    seen: set[str] = set()
    deduped: list[str] = []
    for url in candidates:
        if url not in seen:
            seen.add(url)
            deduped.append(url)

    return deduped


def extract_orders_from_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]

    if isinstance(payload, dict):
        for key in ("results", "data", "orders"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]

    return []


async def fetch_leaflink_orders(credentials: BrandAPICredential) -> list[dict[str, Any]]:
    headers = build_headers(credentials)
    urls = candidate_order_urls(credentials)

    last_error: Exception | None = None

    async with httpx.AsyncClient(timeout=45.0, follow_redirects=True) as client:
        for url in urls:
            logger.info("Trying LeafLink orders endpoint: %s", url)
            logger.info(
                "Auth present=%s vendor_key present=%s",
                bool(headers.get("Authorization")),
                bool(headers.get("X-LeafLink-Vendor-Key")),
            )

            try:
                response = await client.get(url, headers=headers)
                logger.info(
                    "LeafLink response from %s -> status=%s content_type=%s",
                    url,
                    response.status_code,
                    response.headers.get("content-type"),
                )

                if response.status_code in (400, 404):
                    body_preview = response.text[:500]
                    logger.warning("LeafLink path/auth issue on %s -> %s", url, body_preview)
                    last_error = RuntimeError(f"LeafLink HTTP {response.status_code} from {url}: {body_preview}")
                    continue

                response.raise_for_status()

                try:
                    payload = response.json()
                except Exception as exc:
                    raise RuntimeError(
                        f"LeafLink returned non-JSON response from {url}: {response.text[:500]}"
                    ) from exc

                orders = extract_orders_from_payload(payload)
                logger.info("LeafLink parsed %s orders from %s", len(orders), url)
                return orders

            except httpx.HTTPStatusError as exc:
                last_error = exc
                body_preview = exc.response.text[:500] if exc.response is not None else ""
                logger.error(
                    "LeafLink HTTP error on %s -> status=%s body=%s",
                    url,
                    exc.response.status_code if exc.response is not None else "unknown",
                    body_preview,
                )
                raise RuntimeError(
                    f"LeafLink HTTP {exc.response.status_code} from {url}: {body_preview}"
                ) from exc

            except Exception as exc:
                last_error = exc
                logger.exception("LeafLink request failed for %s", url)
                raise

    if last_error is not None:
        raise RuntimeError(f"LeafLink orders fetch failed after trying all endpoints: {last_error}")

    raise RuntimeError("LeafLink orders fetch failed: no candidate endpoints were available")


async def upsert_leaflink_orders(
    db: AsyncSession,
    brand_id: str,
    raw_orders: list[dict[str, Any]],
) -> int:
    synced_count = 0

    for raw in raw_orders:
        normalized = normalize_leaflink_order(raw, brand_id)

        if not normalized["external_order_id"]:
            logger.warning("Skipping LeafLink order with no external_order_id: %s", raw)
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
            db.add(
                Order(
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
            )

        synced_count += 1

    return synced_count


async def sync_leaflink_orders(db: AsyncSession, brand_id: str) -> dict[str, Any]:
    credentials = await get_active_leaflink_credentials(db, brand_id)
    if not credentials:
        raise ValueError(f"No active LeafLink credentials found for brand_id={brand_id}")

    try:
        if hasattr(credentials, "sync_status"):
            credentials.sync_status = "syncing"
        if hasattr(credentials, "last_error"):
            credentials.last_error = None
        await db.commit()

        raw_orders = await fetch_leaflink_orders(credentials)
        synced_count = await upsert_leaflink_orders(db, brand_id, raw_orders)

        if hasattr(credentials, "sync_status"):
            credentials.sync_status = "ok"
        if hasattr(credentials, "last_sync_at"):
            credentials.last_sync_at = utc_now()
        if hasattr(credentials, "last_error"):
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

        try:
            if hasattr(credentials, "sync_status"):
                credentials.sync_status = "error"
            if hasattr(credentials, "last_error"):
                credentials.last_error = str(exc)
            await db.commit()
        except Exception:
            await db.rollback()

        logger.exception("LeafLink sync failed for brand_id=%s", brand_id)
        raise


async def sync_leaflink_orders_for_brand(db: AsyncSession, brand_id: str) -> dict[str, Any]:
    return await sync_leaflink_orders(db, brand_id)