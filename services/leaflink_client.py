import logging
import os
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger("leaflink_client")

DEFAULT_LEAFLINK_BASE_URL = os.getenv("LEAFLINK_BASE_URL", "https://app.leaflink.com/api/v2").strip().rstrip("/")
DEFAULT_LEAFLINK_API_KEY = os.getenv("LEAFLINK_API_KEY", "").strip()
DEFAULT_LEAFLINK_COMPANY_ID = os.getenv("LEAFLINK_COMPANY_ID", "").strip()
LEAFLINK_API_VERSION = os.getenv("LEAFLINK_API_VERSION", "").strip()
LEAFLINK_USER_AGENT = os.getenv("LEAFLINK_USER_AGENT", "opsyn-backend").strip()
MOCK_MODE = os.getenv("MOCK_MODE", "false").lower() == "true"


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
    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        company_id: Optional[str] = None,
    ) -> None:
        self.base_url = (base_url or DEFAULT_LEAFLINK_BASE_URL).strip().rstrip("/")
        self.api_key = (api_key or DEFAULT_LEAFLINK_API_KEY).strip()
        self.company_id = str(company_id or DEFAULT_LEAFLINK_COMPANY_ID).strip()

        if not self.base_url:
            logger.warning("leaflink: client_init missing LEAFLINK_BASE_URL")
            raise ValueError("Missing LEAFLINK_BASE_URL")
        if not self.api_key:
            logger.warning("leaflink: client_init missing LEAFLINK_API_KEY")
            raise ValueError("Missing LEAFLINK_API_KEY")
        if not self.company_id:
            logger.warning("leaflink: client_init missing LEAFLINK_COMPANY_ID")
            raise ValueError("Missing LEAFLINK_COMPANY_ID")

        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"App {self.api_key}",
            "User-Agent": LEAFLINK_USER_AGENT,
        })

        if LEAFLINK_API_VERSION:
            self.session.headers["LeafLink-Version"] = LEAFLINK_API_VERSION

        logger.info(
            "leaflink: client_init base_url=%s company_id=%s api_key_set=%s",
            self.base_url,
            self.company_id,
            bool(self.api_key),
        )

    def _get_raw(self, path: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
        url = f"{self.base_url}/{path.lstrip('/')}"
        safe_params = {k: v for k, v in (params or {}).items() if k not in ("api_key", "token", "secret")}
        logger.info("leaflink: API request method=GET url=%s params=%s", url, safe_params)
        try:
            resp = self.session.get(url, params=params, timeout=45)
        except Exception as exc:
            logger.error("leaflink: API request failed url=%s error=%s", url, exc)
            raise
        content_type = resp.headers.get("Content-Type", "")
        logger.info(
            "leaflink: API response url=%s status=%s content_type=%s size=%s",
            url,
            resp.status_code,
            content_type,
            len(resp.content),
        )
        if "application/json" not in content_type.lower():
            logger.warning(
                "leaflink: API response is not JSON url=%s status=%s content_type=%s body_preview=%s",
                url,
                resp.status_code,
                content_type,
                resp.text[:200],
            )
        if not resp.ok:
            logger.error(
                "leaflink: API HTTP error url=%s status=%s body_preview=%s",
                url,
                resp.status_code,
                resp.text[:200],
            )
        return resp

    def list_orders(
        self,
        page: int = 1,
        page_size: int = 100,
        status: Optional[str] = None,
    ) -> Any:
        logger.info(
            "leaflink: list_orders start company_id=%s page=%s page_size=%s status=%s",
            self.company_id,
            page,
            page_size,
            status,
        )
        params: Dict[str, Any] = {
            "page": page,
            "page_size": page_size,
            "include_children": "line_items,customer,sales_reps",
        }
        if status:
            params["status"] = status

        resp = self._get_raw(f"companies/{self.company_id}/orders-received/", params=params)
        content_type = resp.headers.get("Content-Type", "")

        if resp.ok and "application/json" in content_type.lower():
            data = resp.json()
            result_type = type(data).__name__
            result_count = len(data) if isinstance(data, list) else len(data.get("results") or data.get("data") or data.get("orders") or [])
            logger.info(
                "leaflink: list_orders success page=%s result_type=%s result_count=%s",
                page,
                result_type,
                result_count,
            )
            return data

        logger.error(
            "leaflink: list_orders failed status=%s content_type=%s body_preview=%s",
            resp.status_code,
            content_type,
            resp.text[:220],
        )
        raise RuntimeError(
            f"LeafLink API error: status={resp.status_code} content_type={content_type} body={resp.text[:220]}"
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
        # Try multiple field names for line items (LeafLink API variations)
        candidate = (
            raw.get("line_items")
            or raw.get("items")
            or raw.get("order_items")
            or raw.get("products")
            or raw.get("ordered_items")
            or raw.get("line_item_list")
            or raw.get("order_lines")
            or []
        )

        # Log if we found line items
        if candidate:
            logger.info(
                "leaflink: extract_line_items found candidate count=%s",
                len(candidate) if isinstance(candidate, list) else 1,
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
                    item.get("bulk_units_decimal"),
                    item.get("ordered_quantity"),
                ),
                0,
            )

            sale_price = _safe_float(item.get("sale_price"), 0.0)
            ordered_unit_price = _safe_float(item.get("ordered_unit_price"), 0.0)
            fallback_price = _safe_float(
                _first_non_empty(
                    item.get("unit_price"),
                    item.get("price"),
                    item.get("wholesale_price"),
                    item.get("retail_price"),
                    item.get("suggested_wholesale_price"),
                ),
                0.0,
            )

            unit_price = sale_price if sale_price > 0 else (ordered_unit_price if ordered_unit_price > 0 else fallback_price)

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
        logger.info(
            "leaflink: fetch_recent_orders start max_pages=%s normalize=%s mock_mode=%s",
            max_pages,
            normalize,
            MOCK_MODE,
        )
        all_orders: List[Dict[str, Any]] = []

        try:
            for page in range(1, max_pages + 1):
                logger.info("leaflink: fetch_recent_orders fetching page=%s", page)
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

                logger.info(
                    "leaflink: fetch_recent_orders page=%s results_count=%s has_next=%s",
                    page,
                    len(results),
                    bool(next_url),
                )

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

        except Exception as exc:
            logger.error(
                "leaflink: fetch_recent_orders API call failed error=%s mock_mode=%s",
                exc,
                MOCK_MODE,
            )
            if MOCK_MODE:
                logger.warning(
                    "leaflink: fetch_recent_orders API failed, falling back to mock data (MOCK_MODE=true)"
                )
                mock_orders: List[Dict[str, Any]] = [{"mock_data": True}]
                return mock_orders
            raise

        total = len(all_orders)
        logger.info(
            "leaflink: fetch_recent_orders complete total_orders=%s normalize=%s",
            total,
            normalize,
        )
        if normalize:
            logger.info("leaflink: fetch_recent_orders normalized_order_count=%s", total)

        return all_orders