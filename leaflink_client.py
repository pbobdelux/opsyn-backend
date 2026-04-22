import os
import base64
from typing import Any, Dict, List, Optional

import requests


LEAFLINK_BASE_URL = os.getenv("LEAFLINK_BASE_URL", "https://app.leaflink.com/api/v2").strip().rstrip("/")
LEAFLINK_VENDOR_KEY = os.getenv("LEAFLINK_VENDOR_KEY", "").strip()
LEAFLINK_USER_KEY = os.getenv("LEAFLINK_USER_KEY", "").strip()
LEAFLINK_COMPANY_ID = os.getenv("LEAFLINK_COMPANY_ID", "").strip()
LEAFLINK_USER_AGENT = os.getenv("LEAFLINK_USER_AGENT", "opsyn-backend").strip()


class LeafLinkClient:
    def __init__(self) -> None:
        if not LEAFLINK_VENDOR_KEY or not LEAFLINK_USER_KEY:
            raise ValueError("Missing LeafLink credentials")

        # 🔥 MANUAL BASIC AUTH (THIS IS THE FIX)
        token = base64.b64encode(
            f"{LEAFLINK_VENDOR_KEY}:{LEAFLINK_USER_KEY}".encode()
        ).decode()

        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Basic {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": LEAFLINK_USER_AGENT,
        })

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None):
        url = f"{LEAFLINK_BASE_URL}/{path.lstrip('/')}"
        return self.session.get(url, params=params, timeout=45)

    def list_orders(self, page=1, page_size=100):
        params = {
            "page": page,
            "page_size": page_size,
        }

        paths = [
            f"companies/{LEAFLINK_COMPANY_ID}/orders-received/",
            f"companies/{LEAFLINK_COMPANY_ID}/orders-received",
            "orders-received/",
            "orders-received",
        ]

        errors = []

        for path in paths:
            r = self._get(path, params)

            if r.ok and "application/json" in r.headers.get("Content-Type", ""):
                return r.json()

            errors.append(
                f"{r.url} | {r.status_code} | {r.text[:200]}"
            )

        raise RuntimeError("LeafLink failed | " + " || ".join(errors))

    def fetch_recent_orders(self, max_pages=5):
        all_orders = []

        for page in range(1, max_pages + 1):
            data = self.list_orders(page=page)

            results = data.get("results") or data.get("orders") or []

            if not results:
                break

            all_orders.extend(results)

            if not data.get("next"):
                break

        return all_orders