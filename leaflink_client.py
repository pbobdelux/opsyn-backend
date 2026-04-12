import os
import requests

LEAFLINK_BASE_URL = os.getenv("LEAFLINK_BASE_URL", "https://app.leaflink.com/api/v2").strip().rstrip("/")
LEAFLINK_API_KEY = os.getenv("LEAFLINK_API_KEY", "").strip()
LEAFLINK_VENDOR_KEY = os.getenv("LEAFLINK_VENDOR_KEY", "").strip()
LEAFLINK_USER_KEY = os.getenv("LEAFLINK_USER_KEY", "").strip()


def _preview(text: str, limit: int = 300) -> str:
    if not text:
        return ""
    return text[:limit]


def _headers() -> dict:
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    if LEAFLINK_API_KEY:
        headers["Authorization"] = f"Bearer {LEAFLINK_API_KEY}"

    if LEAFLINK_VENDOR_KEY:
        headers["X-Vendor-API-Key"] = LEAFLINK_VENDOR_KEY

    if LEAFLINK_USER_KEY:
        headers["X-User-API-Key"] = LEAFLINK_USER_KEY

    return headers


def _call(path: str):
    url = f"{LEAFLINK_BASE_URL}{path}"

    try:
        res = requests.get(
            url,
            headers=_headers(),
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
        "auth_mode": "headers",
        "has_api_key": bool(LEAFLINK_API_KEY),
        "has_vendor_key": bool(LEAFLINK_VENDOR_KEY),
        "has_user_key": bool(LEAFLINK_USER_KEY),
        "results": [_call(p) for p in paths],
    }