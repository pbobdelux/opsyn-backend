import os
from typing import Any, Dict, List, Optional

import requests


LEAFLINK_BASE_URL = os.getenv("LEAFLINK_BASE_URL", "https://app.leaflink.com/api/v2").strip().rstrip("/")
LEAFLINK_API_KEY = os.getenv("LEAFLINK_API_KEY", "").strip()
LEAFLINK_COMPANY_ID = os.getenv("LEAFLINK_COMPANY_ID", "").strip()
LEAFLINK_API_VERSION = os.getenv("LEAFLINK_API_VERSION", "").strip()
LEAFLINK_USER_AGENT = os.getenv("LEAFLINK_USER_AGENT", "opsyn-backend").strip()


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None or value == "":
        return default

    if isinstance(value, bool):
        return default

    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, str):
        try:
            return float(value.replace(",", "").replace("$", "").strip())
        except ValueError:
            return default

    if isinstance(value, dict):
        for key in ("amount", "value", "total", "price"):
            if key in value:
                return _safe_float(value.get(key), default)

    return default


def _safe_int(value: Any, default: int = 0) -> int:
    if value is None or value == "":
        return default

    if isinstance(value, bool):
        return default

    if isinstance(value, int):
        return value

    if isinstance(value, float):
        return int(value)

    if isinstance(value, str):
        try:
            return int(float(value.replace(",", "").strip()))
        except ValueError:
            return default

    if isinstance(value, dict):
        for key in ("amount", "value", "quantity", "qty"):
            if key in value:
                return _safe_int(value.get(key), default)

    return default


def _first_non_empty(*values: Any) -> Any:
    for v in values:
        if v is not None and v != "":
            return v
    return None


class LeafLinkClient:
    def __init__(self) -> None:
        if not LEAFLINK_BASE_URL:
            raise ValueError("Missing LEAFLINK_BASE_URL")
        if not LEAFLINK_API_KEY:
            raise ValueError("Missing LEAFLINK_API_KEY")
        if not LEAFLINK_COMPANY_ID:
            raise ValueError("Missing LEAFLINK_COMPANY_ID")

        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"App {LEAFLINK_API_KEY}",
            "User-Agent": LEAFLINK_USER_AGENT,
        })

        if LEAFLINK_API_VERSION:
            self.session.headers["LeafLink-Version"] = LEAFLINK_API_VERSION

    def _get_raw(self, path: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
        url = f"{LEAFLINK_BASE_URL}/{path.lstrip('/')}"
        return self.session.get(url, params=params, timeout=45)

    def list_orders(
        self,
        page: int = 1,
        page_size: int = 100,
        status: Optional[str] = None,
    ) -> Any:
        params: Dict[str, Any] = {
            "page": page,
            "page_size": page_size,
        }
        if status:
            params["status"] = status

        resp = self._get_raw(f"companies/{LEAFLINK_COMPANY_ID}/orders-received/", params=params)
        content_type = resp.headers.get("Content-Type", "")

        if resp.ok and "application/json" in content_type.lower():
            return resp.json()

        raise RuntimeError(
            f"url={resp.url} status={resp.status_code} content_type={content_type} body={resp.text[:220]}"
        )

    def _extract_customer_name(self, raw: Dict[str, Any]) -> str:
        buyer = raw.get("buyer") if isinstance(raw.get("buyer"), dict) else {}
        customer = raw.get("customer") if isinstance(raw.get("customer"), dict) else {}
        dispensary = raw.get("dispensary") if isinstance(raw.get("dispensary"), dict) else {}
        retailer = raw.get("retailer") if isinstance(raw.get("retailer"), dict) else {}
        store = raw.get("store") if isinstance(raw.get("store"), dict) else {}
        corporate_address = raw.get("corporate_address") if isinstance(raw.get("corporate_address"), dict) else {}
        delivery_address = raw.get("delivery_address") if isinstance(raw.get("delivery_address"), dict) else {}

        name = _first_non_empty(
            raw.get("customer_name"),
            raw.get("buyer_name"),
            raw.get("dispensary_name"),
            raw.get("retailer_name"),
            buyer.get("display_name"),
            buyer.get("name"),
            buyer.get("business_name"),
            customer.get("display_name"),
            customer.get("name"),
            customer.get("business_name"),
            dispensary.get("display_name"),
            dispensary.get("name"),
            retailer.get("display_name"),
            retailer.get("name"),
            store.get("display_name"),
            store.get("name"),
            corporate_address.get("name"),
            delivery_address.get("name"),
            corporate_address.get("city"),
        )

        return str(name) if name else "Unknown Customer"

    def _extract_line_items(self, raw: Dict[str, Any]) -> List[Dict[str, Any]]:
        candidate = (
            raw.get("line_items")
            or raw.get("items")
            or raw.get("products")
            or raw.get("ordered_items")
            or []
        )

        if not isinstance(candidate, list):
            return []

        normalized: List[Dict[str, Any]] = []

        for item in candidate:
            if not isinstance(item, dict):
                continue

            product = item.get("product") if isinstance(item.get("product"), dict) else {}
            frozen_product = (
                item.get("frozen_data", {}).get("product")
                if isinstance(item.get("frozen_data"), dict) and isinstance(item.get("frozen_data", {}).get("product"), dict)
                else {}
            )

            quantity = _safe_int(
                _first_non_empty(
                    item.get("quantity"),
                    item.get("qty"),
                    item.get("units"),
                    item.get("unit_count"),
                    item.get("bulk_units"),
                    item.get("ordered_quantity"),
                ),
                0,
            )

            unit_price = _safe_float(
                _first_non_empty(
                    item.get("ordered_unit_price"),
                    item.get("sale_price"),
                    item.get("unit_price"),
                    item.get("price"),
                    item.get("wholesale_price"),
                    item.get("retail_price"),
                    item.get("suggested_wholesale_price"),
                ),
                0.0,
            )

            line_total_raw = _first_non_empty(
                item.get("line_total"),
                item.get("total_price"),
                item.get("total"),
                item.get("extended_total"),
            )

            if line_total_raw is None:
                line_total = round(unit_price * quantity, 2)
            else:
                line_total = _safe_float(line_total_raw, 0.0)

            normalized.append({
                "external_id": str(_first_non_empty(item.get("id"), item.get("uuid"), item.get("sku")) or ""),
                "sku": str(_first_non_empty(item.get("sku"), product.get("sku"), frozen_product.get("sku")) or ""),
                "name": str(
                    _first_non_empty(
                        item.get("name"),
                        item.get("product_name"),
                        product.get("name"),
                        frozen_product.get("name"),
                    ) or "Unknown Item"
                ),
                "quantity": quantity,
                "unit_price": unit_price,
                "line_total": line_total,
                "raw_payload": item,
            })

        return normalized

    def _extract_total_amount(self, raw: Dict[str, Any], line_items: List[Dict[str, Any]]) -> float:
        direct = _first_non_empty(
            raw.get("total_amount"),
            raw.get("total"),
            raw.get("grand_total"),
            raw.get("order_total"),
            raw.get("subtotal"),
            raw.get("amount"),
            raw.get("payment_balance"),
        )

        direct_value = _safe_float(direct, 0.0)
        if direct_value > 0:
            return round(direct_value, 2)

        computed = round(sum(float(li.get("line_total") or 0.0) for li in line_items), 2)
        return computed

    def _normalize_order(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        line_items = self._extract_line_items(raw)
        item_count = len(line_items)
        unit_count = sum(_safe_int(li.get("quantity"), 0) for li in line_items)

        external_id = str(_first_non_empty(
            raw.get("id"),
            raw.get("uuid"),
            raw.get("number"),
            raw.get("short_id"),
            raw.get("external_id"),
        ) or "")

        order_number = str(_first_non_empty(
            raw.get("order_short_number"),
            raw.get("short_id"),
            raw.get("number"),
            raw.get("order_number"),
            raw.get("display_id"),
            raw.get("id"),
        ) or external_id)

        status = str(_first_non_empty(
            raw.get("classification"),
            raw.get("status"),
            raw.get("state"),
            raw.get("fulfillment_status"),
            "unknown",
        )).strip().lower()

        submitted_at = _first_non_empty(
            raw.get("submitted_at"),
            raw.get("created_at"),
            raw.get("created"),
        )

        updated_at = _first_non_empty(
            raw.get("modified"),
            raw.get("updated_at"),
            raw.get("submitted_at"),
            raw.get("created_at"),
        )

        return {
            "external_id": external_id,
            "order_number": order_number,
            "customer_name": self._extract_customer_name(raw),
            "status": status,
            "currency": _first_non_empty(raw.get("currency"), "USD"),
            "submitted_at": submitted_at,
            "created_at": submitted_at,
            "updated_at": updated_at,
            "total_amount": self._extract_total_amount(raw, line_items),
            "item_count": item_count,
            "unit_count": unit_count,
            "line_items": line_items,
            "raw_payload": raw,
        }

    def fetch_recent_orders(self, max_pages: int = 5, normalize: bool = False) -> List[Dict[str, Any]]:
        all_orders: List[Dict[str, Any]] = []

        for page in range(1, max_pages + 1):
            payload = self.list_orders(page=page, page_size=100)

            if isinstance(payload, list):
                results = payload
                next_url = None
            elif isinstance(payload, dict):
                results = (
                    payload.get("results")
                    or payload.get("data")
                    or payload.get("orders")
                    or []
                )
                next_url = payload.get("next")
            else:
                raise RuntimeError(f"Unexpected LeafLink response type: {type(payload).__name__}")

            if not isinstance(results, list):
                raise RuntimeError(f"Unexpected LeafLink results type: {type(results).__name__}")

            if not results:
                break

            if normalize:
                for raw in results:
                    if isinstance(raw, dict):
                        all_orders.append(self._normalize_order(raw))
            else:
                for raw in results:
                    if isinstance(raw, dict):
                        all_orders.append(raw)

            if not next_url:
                break

        return all_orders