import os
import requests
from requests.auth import HTTPBasicAuth


def _get_base_url() -> str:
    base = os.getenv("LEAFLINK_BASE_URL", "https://api.leaflink.com/v2").strip().rstrip("/")
    return base


def _get_vendor_key() -> str:
    return os.getenv("LEAFLINK_VENDOR_KEY", "").strip()


def _get_user_key() -> str:
    return os.getenv("LEAFLINK_USER_KEY", "").strip()


def _preview(text: str, limit: int = 300) -> str:
    if not text:
        return ""
    return text[:limit]


def _call(path: str):
    base_url = _get_base_url()
    vendor_key = _get_vendor_key()
    user_key = _get_user_key()
    url = f"{base_url}{path}"

    try:
        res = requests.get(
            url,
            auth=HTTPBasicAuth(vendor_key, user_key),
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
    base_url = _get_base_url()
    vendor_key = _get_vendor_key()
    user_key = _get_user_key()

    paths = [
        "/orders",
        "/products",
        "/companies",
    ]

    return {
        "base_url": base_url,
        "auth_mode": "basic_auth_vendor_user",
        "has_vendor_key": bool(vendor_key),
        "has_user_key": bool(user_key),
        "results": [_call(p) for p in paths],
    }