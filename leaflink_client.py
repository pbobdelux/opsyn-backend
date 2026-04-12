import os
import requests
from requests.auth import HTTPBasicAuth


LEAFLINK_BASE_URL = os.getenv("LEAFLINK_BASE_URL", "https://app.leaflink.com/api/v2").strip().rstrip("/")
LEAFLINK_VENDOR_KEY = os.getenv("LEAFLINK_VENDOR_KEY", "").strip()
LEAFLINK_USER_KEY = os.getenv("LEAFLINK_USER_KEY", "").strip()


def _preview(text: str, limit: int = 300) -> str:
    if not text:
        return ""
    return text[:limit]


def _call(path: str):
    url = f"{LEAFLINK_BASE_URL}{path}"

    try:
        res = requests.get(
            url,
            auth=HTTPBasicAuth(LEAFLINK_VENDOR_KEY, LEAFLINK_USER_KEY),
            timeout=30,
        )

        return {
            "url": url,
            "status_code": res.status_code,
            "response_preview": _preview(res.text),
        }

    except Exception as e:
        return {
            "url": url,
            "error": str(e),
        }


def test_connection():
    paths = [
        "/orders-received",
        "/products",
        "/companies",
    ]

    return {
        "base_url": LEAFLINK_BASE_URL,
        "auth_mode": "basic_auth_vendor_user",
        "has_vendor_key": bool(LEAFLINK_VENDOR_KEY),
        "has_user_key": bool(LEAFLINK_USER_KEY),
        "results": [_call(p) for p in paths],
    }