import os
from typing import Any, Dict, List, Optional

import requests


LEAFLINK_BASE_URL = os.getenv("LEAFLINK_BASE_URL", "https://app.leaflink.com/api/v2").strip().rstrip("/")
LEAFLINK_VENDOR_KEY = os.getenv("LEAFLINK_VENDOR_KEY", "").strip()
LEAFLINK_USER_KEY = os.getenv("LEAFLINK_USER_KEY", "").strip()


class LeafLinkClient:
    def __init__(self) -> None:
        if not LEAFLINK_VENDOR_KEY:
            raise ValueError("Missing LEAFLINK_VENDOR_KEY")
        if not LEAFLINK_USER_KEY:
            raise ValueError("Missing LEAFLINK_USER_KEY")
        if not LEAFLINK_BASE_URL:
            raise ValueError("Missing LEAFLINK_BASE_URL")

        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Api-Key {LEAFLINK_VENDOR_KEY}",
            "User-Key": LEAFLINK_USER_KEY,
        })

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{LEAFLINK_BASE_URL}/{path.lstrip('/')}"
        resp = self.session.get(url, params=params, timeout=45)

        if not resp.ok:
            raise RuntimeError(
                f"LeafLink GET failed | url={resp.url} | status={resp.status_code} | body={resp.text[:500]}"
            )

        return resp.json()

    def list_orders(
        self,
        page: int = 1,
        page_size: int = 100,
        status: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "page": page,
            "page_size": page_size,
        }
        if status:
            params["status"] = status

        return self._get("orders/", params=params)

    def fetch_recent_orders(self, max_pages: int = 5) -> List[Dict[str, Any]]:
        all_orders: List[Dict[str, Any]] = []

        for page in range(1, max_pages + 1):
            payload = self.list_orders(page=page, page_size=100)

            results = payload.get("results") or payload.get("data") or []
            if not results:
                break

            all_orders.extend(results)

            next_url = payload.get("next")
            if not next_url:
                break

        return all_orders