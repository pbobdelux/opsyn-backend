import os
from typing import Any, Dict, List, Optional
import httpx
import logging
from datetime import datetime, timezone

logger = logging.getLogger("leaflink_client")

LEAFLINK_BASE_URL = os.getenv("LEAFLINK_BASE_URL", "https://app.leaflink.com/api/v2").strip().rstrip("/")

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

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
                logger.error(f"LeafLink API error {e.response.status_code} for {url}: {e.response.text[:300]}")
                raise
            except Exception as e:
                logger.error(f"LeafLink request failed for {url}: {e}")
                raise

    async def list_orders(
        self,
        page: int = 1,
        page_size: int = 100,
        status: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch orders from LeafLink."""
        params: Dict[str, Any] = {"page": page, "page_size": page_size}
        if status:
            params["status"] = status

        all_orders: List[Dict[str, Any]] = []
        path = f"companies/{os.getenv('LEAFLINK_COMPANY_ID', '')}/orders-received/"

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
            params = {}  # Reset for next page

        return all_orders

    def _extract_customer_name(self, raw: Dict[str, Any]) -> str:
        candidates = [
            raw.get("customer_display_name"),
            raw.get("customer_name"),
            raw.get("buyer_name"),
            raw.get("dispensary_name"),
            raw.get("store_name"),
            raw.get("account_name"),
        ]
        for nested in ["buyer", "customer", "dispensary", "retailer", "store"]:
            obj = raw.get(nested)
            if isinstance(obj, dict):
                candidates.extend([
                    obj.get("display_name"),
                    obj.get("name"),
                    obj.get("business_name"),
                ])
        return next((str(c).strip() for c in candidates if c and str(c).strip()), "Unknown Customer")

    def _extract_line_items(self, raw: Dict[str, Any]) -> List[Dict[str, Any]]:
        raw_lines = (
            raw.get("line_items") or raw.get("items") or raw.get("products") or raw.get("ordered_items") or []
        )
        if not isinstance(raw_lines, list):
            return []

        normalized = []
        for item in raw_lines:
            if not isinstance(item, dict):
                continue
            quantity = int(item.get("quantity") or item.get("qty") or item.get("units") or 0)
            unit_price =