import os
from typing import Any, Dict, List, Optional

import requests


LEAFLINK_BASE_URL = os.getenv("LEAFLINK_BASE_URL", "https://app.leaflink.com/api/v2").strip().rstrip("/")
LEAFLINK_VENDOR_KEY = os.getenv("LEAFLINK_VENDOR_KEY", "").strip()
LEAFLINK_USER_KEY = os.getenv("LEAFLINK_USER_KEY", "").strip()
LEAFLINK_COMPANY_ID = os.getenv("LEAFLINK_COMPANY_ID", "").strip()
LEAFLINK_COMPANY_SLUG = os.getenv("LEAFLINK_COMPANY_SLUG", "").strip()


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

    def _get_raw(self, path: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
        url = f"{LEAFLINK_BASE_URL}/{path.lstrip('/')}"
        return self.session.get(url, params=params, timeout=45)

    def _get_json(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        resp = self._get_raw(path, params=params)
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

        candidate_paths: List[str] = [
            "orders/",
        ]

        if LEAFLINK_COMPANY_ID:
            candidate_paths.extend([
                f"companies/{LEAFLINK_COMPANY_ID}/orders/",
                f"company/{LEAFLINK_COMPANY_ID}/orders/",
            ])

        if LEAFLINK_COMPANY_SLUG:
            candidate_paths.extend([
                f"companies/{LEAFLINK_COMPANY_SLUG}/orders/",
                f"company/{LEAFLINK_COMPANY_SLUG}/orders/",
            ])

        errors: List[str] = []

        for path in candidate_paths:
            resp = self._get_raw(path, params=params)

            content_type = resp.headers.get("Content-Type", "")
            is_json = "application/json" in content_type.lower()

            if resp.ok and is_json:
                return resp.json()

            errors.append(
                f"path={path} url={resp.url} status={resp.status_code} content_type={content_type} body={resp.text[:180]}"
            )

        raise RuntimeError("LeafLink orders lookup failed across candidate paths | " + " || ".join(errors))

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