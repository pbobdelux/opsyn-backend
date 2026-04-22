import os
from typing import Any, Dict, List, Optional
import httpx
import logging
from datetime import datetime, timezone

logger = logging.getLogger("leaflink_client")

LEAFLINK_BASE_URL = os.getenv("LEAFLINK_BASE_URL", "https://app.leaflink.com/api/v2").strip().rstrip("/")

class LeafLinkClient:
    def __init__(self, api_key: str, base_url: Optional[str] = None):
        self.base_url = (base_url or LEAFLINK_BASE_URL).rstrip("/")
        if not api_key:
            raise ValueError("LeafLink API key is required")
       
        self.api_key = api_key
        self.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"App {api_key}",
            "User-Agent": "opsyn-backend/1.0",
        }

    async def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base_url}/{path.lstrip('/')}"
        async with httpx.AsyncClient(timeout=60.0) as client:
            try:
                resp = await client.get(url, headers=self.headers, params=params)
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPStatusError as e:
                logger.error(f"LeafLink API error {e.response.status_code} for {url}: {e.response.text[:500]}")
                raise
            except Exception as e:
                logger.error(f"LeafLink request failed for {url}: {e}")
                raise

    async def list_orders(
        self,
        page: int = 1,
        page_size: int = 100,
    ) -> List[Dict[str, Any]]:
        """Fetch orders-received with line items included."""
        company_id = os.getenv("LEAFLINK_COMPANY_ID", "").strip()
        if not company_id:
            raise ValueError("LEAFLINK_COMPANY_ID environment variable is required")

        params = {
            "page": page,
            "page_size": page_size,
            "include_children": "line_items,customer",   # This is the key fix
        }

        path = f"companies/{company_id}/orders-received/"
        all_orders: List[Dict[str, Any]] = []

        while path:
            data = await self._get(path, params=params)
            if isinstance(data, list):
                all_orders.extend(data)
                break
            elif isinstance(data, dict):
                results = data.get("results") or data.get("data") or data.get("orders") or []
                all_orders.extend(results if isinstance(results, list) else [])
                path = data.get("next")
            else:
                break
            params = {}  # clear for pagination

        return all_orders

    def _extract_customer_name(self, raw: Dict[str, Any]) -> str:
        candidates = [
            raw.get("customer_display_name"),
            raw.get("customer_name"),
            raw.get("buyer_name"),
            raw.get("dispensary_name"),
            raw.get("store_name"),
        ]
        for nested in ["buyer", "customer", "dispensary", "retailer", "store"]:
            obj = raw.get(nested)
            if isinstance(obj, dict):
                candidates.extend([
                    obj.get("display_name"),
                    obj.get("name"),
                    obj.get("business_name"),
                ])
        for c in candidates:
            if c and str(c).strip():
                return str(c).strip()
        return "Unknown Customer"

    def _extract_line_items(self, raw: Dict[str, Any]) -> List[Dict[str, Any]]:
        raw_lines = (
            raw.get("line_items")
            or raw.get("items")
            or raw.get("products")
            or raw.get("ordered_items")
            or []
        )
        if not isinstance(raw_lines, list):
            return []

        normalized = []
        for item in raw_lines:
            if not isinstance(item, dict):
                continue

            quantity = int(item.get("quantity") or item.get("qty") or item.get("units") or 0)

            # Prefer sale_price, then ordered_unit_price, then fallback
            unit_price = float(
                item.get("sale_price")
                or item.get("ordered_unit_price")
                or item.get("unit_price")
                or item.get("price")
                or 0.0
            )

            line_total = float(
                item.get("line_total")
                or item.get("total")
                or (unit_price * quantity)
            )

            normalized.append({
                "external_id": str(item.get("id") or item.get("sku") or ""),
                "sku": str(item.get("sku") or ""),
                "name": str(item.get("name") or item.get("product_name") or "Unknown Item"),
                "quantity": quantity,
                "unit_price": round(unit_price, 2),
                "line_total": round(line_total, 2),
                "raw_payload": item,
            })
        return normalized

    def _normalize_order(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        line_items = self._extract_line_items(raw)
        total_amount = sum(li.get("line_total", 0) for li in line_items)

        return {
            "external_id": str(raw.get("id") or raw.get("number") or raw.get("short_id") or ""),
            "order_number": str(raw.get("order_number") or raw.get("number") or raw.get("short_id") or ""),
            "customer_name": self._extract_customer_name(raw),
            "status": str(raw.get("status") or raw.get("classification") or "unknown").lower(),
            "total_amount": round(total_amount, 2),
            "currency": "USD",
            "submitted_at": raw.get("submitted_at") or raw.get("created_at"),
            "updated_at": raw.get("updated_at") or raw.get("modified"),
            "item_count": len(line_items),
            "unit_count": sum(li.get("quantity", 0) for li in line_items),
            "line_items": line_items,
            "raw_payload": raw,
        }

    async def fetch_recent_orders(self, max_pages: int = 5, normalize: bool = True) -> List[Dict[str, Any]]:
        all_raw = []
        for page in range(1, max_pages + 1):
            orders = await self.list_orders(page=page, page_size=100)
            all_raw.extend(orders)
            if len(orders) < 100:
                break

        if normalize:
            return [self._normalize_order(raw) for raw in all_raw if isinstance(raw, dict)]
        return all_raw