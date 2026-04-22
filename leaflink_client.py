import os
from typing import Any, Dict, List, Optional

import requests


LEAFLINK_BASE_URL = os.getenv("LEAFLINK_BASE_URL", "https://api.leaflink.com").strip().rstrip("/")
LEAFLINK_VENDOR_KEY = os.getenv("LEAFLINK_VENDOR_KEY", "").strip()
LEAFLINK_USER_KEY = os.getenv("LEAFLINK_USER_KEY", "").strip()
LEAFLINK_COMPANY_ID = os.getenv("LEAFLINK_COMPANY_ID", "").strip()
LEAFLINK_COMPANY_SLUG = os.getenv("LEAFLINK_COMPANY_SLUG", "").strip()
LEAFLINK_API_VERSION = os.getenv("LEAFLINK_API_VERSION", "").strip()


class LeafLinkClient:
    def __init__(self) -> None:
        if not LEAFLINK_BASE_URL:
            raise ValueError("Missing LEAFLINK_BASE_URL")

        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
        })

        # Current docs say bearer JWT for main API.
        # Your existing keys may still map to a legacy/private integration,
        # so we keep the old headers too for now while debugging.
        if LEAFLINK_VENDOR_KEY:
            self.session.headers["Authorization"] = f"Api-Key {LEAFLINK_VENDOR_KEY}"
        if LEAFLINK_USER_KEY:
            self.session.headers["User-Key"] = LEAFLINK_USER_KEY
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
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "page": page,
            "page_size": page_size,
        }
        if status:
            params["status"] = status

        candidate_paths: List[str] = [
            "orders",
            "orders/",
        ]

        if LEAFLINK_COMPANY_ID:
            candidate_paths.extend([
                "orders",
                "orders/",
            ])
            params["company_id"] = LEAFLINK_COMPANY_ID

        if LEAFLINK_COMPANY_SLUG:
            params["company_slug"] = LEAFLINK_COMPANY_SLUG

        errors: List[str] = []

        for path in candidate_paths:
            resp = self._get_raw(path, params=params)
            content_type = resp.headers.get("Content-Type", "")

            if resp.ok and "application/json" in content_type.lower():
                return resp.json()

            errors.append(
                f"url={resp.url} status={resp.status_code} content_type={content_type} body={resp.text[:180]}"
            )

        raise RuntimeError("LeafLink orders lookup failed | " + " || ".join(errors))

    def fetch_recent_orders(self, max_pages: int = 5) -> List[Dict[str, Any]]:
        all_orders: List[Dict[str, Any]] = []

        for page in range(1, max_pages + 1):
            payload = self.list_orders(page=page, page_size=100)

            results = payload.get("results") or payload.get("data") or payload.get("orders") or []
            if not results:
                break

            all_orders.extend(results)

            next_url = payload.get("next")
            if not next_url:
                break

        return all_orders