from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional

import httpx
import logging
from sqlalchemy import select

from models import BrandAPICredential, Order

logger = logging.getLogger("leaflink_sync")


def utc_now():
    return datetime.now(timezone.utc)


def _clean_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        return value or None
    value = str(value).strip()
    return value or None


def _first_non_empty(*values: Any) -> Optional[str]:
    for value in values:
        cleaned = _clean_str(value)
        if cleaned:
            return cleaned
    return None


def _dig(data: Any, *path: str) -> Any:
    cur = data
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def _to_decimal(value: Any) -> Optional[Decimal]:
    if value is None or value == "":
        return None

    if isinstance(value, bool):
        return None

    if isinstance(value, Decimal):
        return value

    if isinstance(value, (int, float)):
        return Decimal(str(value))

    if isinstance(value, str):
        stripped = value.strip().replace("$", "").replace(",", "")
        if not stripped:
            return None
        try:
            return Decimal(stripped)
        except (InvalidOperation, ValueError):
            return None

    if isinstance(value, dict):
        for candidate in (
            value.get("amount"),
            value.get("value"),
            value.get("total"),
            value.get("price"),
        ):
            dec = _to_decimal(candidate)
            if dec is not None:
                return dec

    return None


def _to_cents(value: Any) -> int:
    dec = _to_decimal(value)
    if dec is None:
        return 0
    return int((dec * Decimal("100")).quantize(Decimal("1")))


def _to_int(value: Any, default: int = 0) -> int:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip().replace(",", "")
        if not stripped:
            return default
        try:
            return int(float(stripped))
        except ValueError:
            return default
    if isinstance(value, dict):
        for candidate in (
            value.get("amount"),
            value.get("value"),
            value.get("quantity"),
            value.get("qty"),
        ):
            parsed = _to_int(candidate, default=-999999)
            if parsed != -999999:
                return parsed
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
    if s in {"ready", "packed", "pick_ready"}:
        return "ready"
    if s in {"shipped", "fulfilled", "out_for_delivery", "delivered"}:
        return "shipped"
    if s in {"complete", "completed", "closed"}:
        return "complete"
    if s in {"cancelled", "canceled", "void"}:
        return "cancelled"

    return s


def extract_customer_name(order: Dict[str, Any]) -> str:
    return (
        _first_non_empty(
            order.get("customer_display_name"),
            order.get("customer_name"),
            order.get("buyer_name"),
            order.get("account_name"),
            order.get("dispensary_name"),
            order.get("store_name"),
            _dig(order, "buyer", "display_name"),
            _dig(order, "buyer", "name"),
            _dig(order, "buyer", "business_name"),
            _dig(order, "customer", "display_name"),
            _dig(order, "customer", "name"),
            _dig(order, "customer", "business_name"),
            _dig(order, "retailer", "display_name"),
            _dig(order, "retailer", "name"),
            _dig(order, "dispensary", "display_name"),
            _dig(order, "dispensary", "name"),
            _dig(order, "store", "name"),
            _dig(order, "account", "name"),
            _dig(order, "corporate_address", "name"),
            order.get("name"),
        )
        or "Unknown Customer"
    )


def extract_external_id(order: Dict[str, Any]) -> Optional[str]:
    return _first_non_empty(
        order.get("id"),
        order.get("uuid"),
        order.get("uid"),
        order.get("number"),
        order.get("order_number"),
        order.get("display_id"),
        order.get("short_id"),
    )


def extract_order_number(order: Dict[str, Any]) -> Optional[str]:
    return _first_non_empty(
        order.get("order_short_number"),
        order.get("short_id"),
        order.get("number"),
        order.get("order_number"),
        order.get("display_id"),
        order.get("order_buyer_number"),
        order.get("order_seller_number"),
        order.get("id"),
        order.get("uuid"),
    )


def extract_status(order: Dict[str, Any]) -> str:
    raw_status = _first_non_empty(
        order.get("status"),
        order.get("classification"),
        order.get("order_status"),
        _dig(order, "status", "name") if isinstance(order.get("status"), dict) else None,
        _dig(order, "fulfillment", "status"),
        _dig(order, "shipping", "status"),
    )
    return normalize_status(raw_status)


def extract_line_items(order: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw_lines = (
        order.get("line_items")
        or order.get("items")
        or order.get("products")
        or order.get("ordered_items")
        or order.get("lines")
        or []
    )

    if not isinstance(raw_lines, list):
        return []

    normalized_lines: List[Dict[str, Any]] = []

    for line in raw_lines:
        if not isinstance(line, dict):
            continue

        quantity = _to_int(
            line.get("quantity")
            or line.get("qty")
            or line.get("units")
            or line.get("count")
            or line.get("bulk_units")
            or line.get("bulk_units_decimal")
            or line.get("ordered_quantity"),
            0,
        )

        unit_price = _to_decimal(
            line.get("ordered_unit_price")
            or line.get("sale_price")
            or line.get("unit_price")
            or line.get("price")
            or line.get("wholesale_price")
            or line.get("retail_price")
            or line.get("suggested_wholesale_price")
            or _dig(line, "pricing", "unit_price")
        )

        line_total = _to_decimal(
            line.get("line_total")
            or line.get("total_price")
            or line.get("total")
            or _dig(line, "pricing", "total")
        )

        if line_total is None and unit_price is not None and quantity:
            line_total = unit_price * Decimal(quantity)

        normalized_lines.append(
            {
                "external_id": _first_non_empty(
                    line.get("id"),
                    line.get("uuid"),
                    line.get("sku"),
                ),
                "sku": _first_non_empty(
                    line.get("sku"),
                    _dig(line, "product", "sku"),
                    _dig(line, "variant", "sku"),
                    _dig(line, "frozen_data", "product", "sku"),
                ),
                "name": _first_non_empty(
                    line.get("name"),
                    line.get("product_name"),
                    _dig(line, "product", "name"),
                    _dig(line, "variant", "name"),
                    _dig(line, "frozen_data", "product", "name"),
                )
                or "Unknown Item",
                "quantity": quantity,
                "unit_price": float(unit_price) if unit_price is not None else None,
                "line_total": float(line_total) if line_total is not None else None,
                "raw_payload": line,
            }
        )

    return normalized_lines


def extract_total_cents(order: Dict[str, Any], lines: List[Dict[str, Any]]) -> int:
    direct_candidates = [
        order.get("total"),
        order.get("grand_total"),
        order.get("subtotal"),
        order.get("amount_total"),
        order.get("total_price"),
        order.get("price_total"),
        order.get("payment_balance"),
        _dig(order, "pricing", "total"),
        _dig(order, "pricing", "grand_total"),
        _dig(order, "totals", "total"),
        _dig(order, "totals", "grand_total"),
        _dig(order, "totals", "subtotal"),
        _dig(order, "summary", "total"),
    ]

    for candidate in direct_candidates:
        cents = _to_cents(candidate)
        if cents > 0:
            return cents

    summed_cents = 0
    found_any = False

    for line in lines:
        line_total = line.get("line_total")
        if line_total is not None:
            summed_cents += _to_cents(line_total)
            found_any = True

    if found_any:
        return summed_cents

    return 0


# ==============================
# FETCH FROM LEAFLINK
# ==============================
async def fetch_leaflink_orders(base_url: str, api_key: str):
    url = f"{base_url.rstrip('/')}/orders-received/"

    headers = {
        "Authorization": f"App {api_key}",
        "Accept": "application/json",
    }

    all_orders = []

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        while url:
            response = await client.get(url, headers=headers)

            if response.status_code >= 400:
                raise Exception(
                    f"LeafLink API error {response.status_code} - {response.text[:500]}"
                )

            data = response.json()

            if isinstance(data, list):
                all_orders.extend(data)
                break

            if isinstance(data, dict):
                results = data.get("results") or data.get("data") or data.get("orders") or []
                if not isinstance(results, list):
                    results = []
                all_orders.extend(results)
                url = data.get("next")
            else:
                break

    return all_orders


# ==============================
# SYNC FUNCTION
# ==============================
async def sync_leaflink_orders(db, brand_id: str):
    now = utc_now()

    result = await db.execute(
        select(BrandAPICredential).where(
            BrandAPICredential.brand_id == brand_id,
            BrandAPICredential.integration_name == "leaflink",
        )
    )
    cred = result.scalar_one_or_none()

    if not cred:
        raise Exception(f"No LeafLink credentials for {brand_id}")

    if not cred.api_key:
        raise Exception("Missing API key")

    try:
        raw_orders = await fetch_leaflink_orders(
            cred.base_url or "https://app.leaflink.com/api/v2",
            cred.api_key,
        )

        created = 0
        updated = 0
        skipped = 0

        for o in raw_orders:
            if not isinstance(o, dict):
                skipped += 1
                continue

            external_id = extract_external_id(o)
            if not external_id:
                skipped += 1
                continue

            customer_name = extract_customer_name(o)
            status = extract_status(o)
            order_number = extract_order_number(o)
            line_items = extract_line_items(o)
            total_cents = extract_total_cents(o, line_items)

            result = await db.execute(
                select(Order).where(
                    Order.brand_id == brand_id,
                    Order.external_order_id == str(external_id),
                    Order.source == "leaflink",
                )
            )
            existing = result.scalar_one_or_none()

            item_count_value = len(line_items)
            unit_count_value = sum(_to_int(li.get("quantity"), 0) for li in line_items)

            if existing:
                existing.customer_name = customer_name
                existing.status = status
                existing.total_cents = total_cents
                existing.synced_at = now

                if hasattr(existing, "order_number"):
                    existing.order_number = order_number

                if hasattr(existing, "item_count"):
                    existing.item_count = item_count_value

                if hasattr(existing, "unit_count"):
                    existing.unit_count = unit_count_value

                if hasattr(existing, "raw_payload"):
                    existing.raw_payload = o

                if hasattr(existing, "line_items_json"):
                    existing.line_items_json = line_items

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
                    payload["item_count"] = item_count_value

                if hasattr(Order, "unit_count"):
                    payload["unit_count"] = unit_count_value

                if hasattr(Order, "raw_payload"):
                    payload["raw_payload"] = o

                if hasattr(Order, "line_items_json"):
                    payload["line_items_json"] = line_items

                db.add(Order(**payload))
                created += 1

        await db.commit()

        return {
            "ok": True,
            "fetched": len(raw_orders),
            "created": created,
            "updated": updated,
            "skipped": skipped,
        }

    except Exception as e:
        await db.rollback()
        logger.exception("LeafLink sync failed for brand_id=%s", brand_id)

        return {
            "ok": False,
            "error": str(e),
        }