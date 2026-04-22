import os
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional

import requests


LEAFLINK_BASE_URL = os.getenv("LEAFLINK_BASE_URL", "https://app.leaflink.com/api/v2").strip().rstrip("/")
LEAFLINK_API_KEY = os.getenv("LEAFLINK_API_KEY", "").strip()
LEAFLINK_COMPANY_ID = os.getenv("LEAFLINK_COMPANY_ID", "").strip()
LEAFLINK_API_VERSION = os.getenv("LEAFLINK_API_VERSION", "").strip()
LEAFLINK_USER_AGENT = os.getenv("LEAFLINK_USER_AGENT", "opsyn-backend").strip()


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


def _coerce_decimal(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None

    if isinstance(value, bool):
        return None

    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, str):
        stripped = value.strip().replace("$", "").replace(",", "")
        if not stripped:
            return None
        try:
            return float(Decimal(stripped))
        except (InvalidOperation, ValueError):
            return None

    return None


def _coerce_int(value: Any, default: int = 0) -> int:
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
    return default


class LeafLinkClient:
    def __init__(self) -> None:
        if not LEAFLINK_BASE_URL:
            raise ValueError("Missing LEAFLINK_BASE_URL")
        if not LEAFLINK_API_KEY:
            raise ValueError("Missing LEAFLINK_API_KEY")
        if not LEAFLINK_COMPANY_ID:
            raise ValueError("Missing LEAFLINK_COMPANY_ID")

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": f"App {LEAFLINK_API_KEY}",
                "User-Agent": LEAFLINK_USER_AGENT,
            }
        )

        if LEAFLINK_API_VERSION:
            self.session.headers["LeafLink-Version"] = LEAFLINK_API_VERSION

    def _get_raw(self, path: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
        url = f"{LEAFLINK_BASE_URL}/{path.lstrip('/')}"
        resp = self.session.get(url, params=params, timeout=45)
        return resp

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
            f"url={resp.url} status={resp.status_code} content_type={content_type} body={resp.text[:500]}"
        )

    def fetch_recent_orders(self, max_pages: int = 5, normalize: bool = False) -> List[Dict[str, Any]]:
        all_orders: List[Dict[str, Any]] = []

        for page in range(1, max_pages + 1):
            payload = self.list_orders(page=page, page_size=100)

            if isinstance(payload, list):
                results = payload
                next_url = None
            elif isinstance(payload, dict):
                results = payload.get("results") or payload.get("data") or payload.get("orders") or []
                next_url = payload.get("next")
            else:
                raise RuntimeError(f"Unexpected LeafLink response type: {type(payload).__name__}")

            if not isinstance(results, list):
                raise RuntimeError(f"Unexpected LeafLink results type: {type(results).__name__}")

            if not results:
                break

            if normalize:
                all_orders.extend([self.normalize_order(order) for order in results])
            else:
                all_orders.extend(results)

            if not next_url:
                break

        return all_orders

    def normalize_order(self, order: Dict[str, Any]) -> Dict[str, Any]:
        lines = self.extract_line_items(order)

        customer_name = self.extract_customer_name(order)
        order_number = self.extract_order_number(order)
        status_raw = self.extract_status(order)
        total_amount = self.extract_total_amount(order, lines)
        item_count = len(lines)
        unit_count = sum(_coerce_int(line.get("quantity"), 0) for line in lines)

        normalized = {
            "source": "leaflink",
            "external_id": _first_non_empty(
                order.get("id"),
                order.get("uuid"),
                order.get("pk"),
            ),
            "order_number": order_number,
            "customer_name": customer_name or "Unknown Customer",
            "status": self.normalize_status(status_raw),
            "status_raw": status_raw,
            "total_amount": total_amount if total_amount is not None else 0.0,
            "item_count": item_count,
            "unit_count": unit_count,
            "currency": _first_non_empty(
                order.get("currency"),
                _dig(order, "totals", "currency"),
                "USD",
            ),
            "submitted_at": _first_non_empty(
                order.get("submitted_at"),
                order.get("created_at"),
                order.get("date_created"),
                order.get("created"),
            ),
            "updated_at": _first_non_empty(
                order.get("updated_at"),
                order.get("modified"),
                order.get("last_modified"),
            ),
            "ship_date": _first_non_empty(
                order.get("ship_date"),
                order.get("delivery_date"),
                order.get("requested_ship_date"),
            ),
            "notes": _first_non_empty(
                order.get("notes"),
                order.get("buyer_notes"),
                order.get("internal_notes"),
            ),
            "line_items": lines,
            "raw_payload": order,
        }

        return normalized

    def extract_customer_name(self, order: Dict[str, Any]) -> Optional[str]:
        return _first_non_empty(
            _dig(order, "buyer", "display_name"),
            _dig(order, "buyer", "name"),
            _dig(order, "customer", "display_name"),
            _dig(order, "customer", "name"),
            _dig(order, "retailer", "display_name"),
            _dig(order, "retailer", "name"),
            _dig(order, "dispensary", "display_name"),
            _dig(order, "dispensary", "name"),
            _dig(order, "store", "name"),
            _dig(order, "account", "name"),
            order.get("buyer_name"),
            order.get("customer_name"),
            order.get("account_name"),
            order.get("dispensary_name"),
            order.get("store_name"),
            order.get("name"),
        )

    def extract_order_number(self, order: Dict[str, Any]) -> Optional[str]:
        return _first_non_empty(
            order.get("number"),
            order.get("order_number"),
            order.get("display_id"),
            order.get("external_order_number"),
            order.get("short_id"),
            order.get("name"),
            order.get("id"),
            order.get("uuid"),
        )

    def extract_status(self, order: Dict[str, Any]) -> Optional[str]:
        return _first_non_empty(
            order.get("status"),
            order.get("order_status"),
            _dig(order, "status", "name") if isinstance(order.get("status"), dict) else None,
            _dig(order, "fulfillment", "status"),
            _dig(order, "shipping", "status"),
        )

    def normalize_status(self, status: Optional[str]) -> str:
        raw = (status or "").strip().lower()

        if raw in {"submitted", "pending", "new"}:
            return "submitted"
        if raw in {"accepted", "confirmed", "approved"}:
            return "accepted"
        if raw in {"ready", "packed", "pick_ready"}:
            return "ready"
        if raw in {"shipped", "fulfilled", "out_for_delivery", "delivered"}:
            return "shipped"
        if raw in {"complete", "completed", "closed"}:
            return "complete"
        if raw in {"cancelled", "canceled", "void"}:
            return "cancelled"

        return raw or "unknown"

    def extract_total_amount(self, order: Dict[str, Any], lines: Optional[List[Dict[str, Any]]] = None) -> Optional[float]:
        candidates = [
            order.get("total"),
            order.get("grand_total"),
            order.get("subtotal"),
            order.get("amount_total"),
            order.get("total_price"),
            order.get("price_total"),
            _dig(order, "pricing", "total"),
            _dig(order, "pricing", "grand_total"),
            _dig(order, "totals", "total"),
            _dig(order, "totals", "grand_total"),
            _dig(order, "totals", "subtotal"),
            _dig(order, "summary", "total"),
        ]

        for candidate in candidates:
            amount = _coerce_decimal(candidate)
            if amount is not None:
                return round(amount, 2)

        if lines:
            summed = 0.0
            found_any = False
            for line in lines:
                line_total = _coerce_decimal(line.get("line_total"))
                if line_total is None:
                    line_total = _coerce_decimal(line.get("total_price"))
                if line_total is None:
                    qty = _coerce_int(line.get("quantity"), 0)
                    unit_price = _coerce_decimal(line.get("unit_price"))
                    if unit_price is not None and qty:
                        line_total = unit_price * qty
                if line_total is not None:
                    summed += float(line_total)
                    found_any = True

            if found_any:
                return round(summed, 2)

        return None

    def extract_line_items(self, order: Dict[str, Any]) -> List[Dict[str, Any]]:
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

            quantity = _coerce_int(
                line.get("quantity")
                or line.get("qty")
                or line.get("units")
                or line.get("count"),
                0,
            )

            unit_price = _coerce_decimal(
                line.get("unit_price")
                or line.get("price")
                or _dig(line, "pricing", "unit_price")
            )

            line_total = _coerce_decimal(
                line.get("line_total")
                or line.get("total_price")
                or line.get("total")
                or _dig(line, "pricing", "total")
            )

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
                    ),
                    "name": _first_non_empty(
                        line.get("name"),
                        line.get("product_name"),
                        _dig(line, "product", "name"),
                        _dig(line, "variant", "name"),
                    )
                    or "Unknown Item",
                    "brand": _first_non_empty(
                        line.get("brand"),
                        _dig(line, "product", "brand"),
                        _dig(line, "brand", "name"),
                    ),
                    "quantity": quantity,
                    "unit_price": round(unit_price, 2) if unit_price is not None else None,
                    "line_total": round(line_total, 2) if line_total is not None else None,
                    "raw_payload": line,
                }
            )

        return normalized_lines