import os
from typing import Any, Dict, List, Optional

import requests


LEAFLINK_BASE_URL = os.getenv("LEAFLINK_BASE_URL", "https://app.leaflink.com/api/v2").strip().rstrip("/")
LEAFLINK_API_KEY = os.getenv("LEAFLINK_API_KEY", "").strip()
LEAFLINK_COMPANY_ID = os.getenv("LEAFLINK_COMPANY_ID", "").strip()
LEAFLINK_API_VERSION = os.getenv("LEAFLINK_API_VERSION", "").strip()
LEAFLINK_USER_AGENT = os.getenv("LEAFLINK_USER_AGENT", "opsyn-backend").strip()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _first_non_empty(*values: Any) -> Any:
    for v in values:
        if v is not None and v != "":
            return v
    return None


def _as_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    return []


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
        )
        return str(name) if name else "Unknown Customer"

    def _extract_line_items(self, raw: Dict[str, Any]) -> List[Dict[str, Any]]:
        candidate_lists = [
            raw.get("line_items"),
            raw.get("items"),
            raw.get("products"),
            raw.get("order_items"),
        ]

        for candidate in candidate_lists:
            if isinstance(candidate, list):
                cleaned: List[Dict[str, Any]] = []
                for item in candidate:
                    if isinstance(item, dict):
                        cleaned.append(item)
                return cleaned

        return []

    def _extract_item_and_unit_counts(self, raw: Dict[str, Any], line_items: List[Dict[str, Any]]) -> Dict[str, int]:
        explicit_item_count = _first_non_empty(
            raw.get("item_count"),
            raw.get("items_count"),
            raw.get("product_count"),
            raw.get("line_item_count"),
        )

        explicit_unit_count = _first_non_empty(
            raw.get("unit_count"),
            raw.get("units_count"),
            raw.get("total_units"),
            raw.get("quantity"),
        )

        item_count = _safe_int(explicit_item_count, default=len(line_items))

        if explicit_unit_count is not None:
            unit_count = _safe_int(explicit_unit_count, default=0)
        else:
            total_units = 0
            for item in line_items:
                qty = _first_non_empty(
                    item.get("quantity"),
                    item.get("qty"),
                    item.get("units"),
                    item.get("unit_count"),
                    item.get("ordered_quantity"),
                )
                total_units += _safe_int(qty, default=0)
            unit_count = total_units

        return {
            "item_count": item_count,
            "unit_count": unit_count,
        }

    def _extract_total_amount(self, raw: Dict[str, Any], line_items: List[Dict[str, Any]]) -> float:
        amount = _first_non_empty(
            raw.get("total_amount"),
            raw.get("total"),
            raw.get("grand_total"),
            raw.get("order_total"),
            raw.get("subtotal"),
            raw.get("amount"),
        )

        amount_value = _safe_float(amount, default=0.0)
        if amount_value > 0:
            return amount_value

        calculated = 0.0
        for item in line_items:
            line_total = _first_non_empty(
                item.get("total"),
                item.get("line_total"),
                item.get("extended_total"),
                item.get("subtotal"),
            )
            if line_total is not None:
                calculated += _safe_float(line_total, default=0.0)
                continue

            price = _first_non_empty(
                item.get("price"),
                item.get("unit_price"),
                item.get("sale_price"),
            )
            qty = _first_non_empty(
                item.get("quantity"),
                item.get("qty"),
                item.get("units"),
                item.get("unit_count"),
            )
            calculated += _safe_float(price, default=0.0) * _safe_int(qty, default=0)

        return calculated

    def _normalize_order(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        line_items = self._extract_line_items(raw)
        counts = self._extract_item_and_unit_counts(raw, line_items)
        total_amount = self._extract_total_amount(raw, line_items)

        external_id = str(_first_non_empty(
            raw.get("id"),
            raw.get("uuid"),
            raw.get("number"),
            raw.get("short_id"),
            raw.get("external_id"),
        ) or "")

        order_number = str(_first_non_empty(
            raw.get("number"),
            raw.get("short_id"),
            raw.get("external_id"),
            raw.get("id"),
        ) or external_id)

        status = str(_first_non_empty(
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
            raw.get("updated_at"),
            raw.get("modified"),
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
            "total_amount": total_amount,
            "item_count": counts["item_count"],
            "unit_count": counts["unit_count"],
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