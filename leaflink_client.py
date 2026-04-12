import os
import requests
from requests.auth import HTTPBasicAuth

LEAFLINK_VENDOR_KEY = os.getenv("LEAFLINK_VENDOR_KEY", "").strip()
LEAFLINK_USER_KEY = os.getenv("LEAFLINK_USER_KEY", "").strip()

BASE_URL = "https://vendor-api.leaflink.com"  # ✅ DIFFERENT BASE URL

def _call(path: str):
    url = f"{BASE_URL}{path}"

    try:
        res = requests.get(
            url,
            auth=HTTPBasicAuth(LEAFLINK_VENDOR_KEY, LEAFLINK_USER_KEY),
            timeout=30
        )

        return {
            "url": url,
            "status_code": res.status_code,
            "response_preview": res.text[:300]
        }

    except Exception as e:
        return {"error": str(e)}

def test_connection():
    paths = [
        "/orders-received",   # 🔥 REAL endpoint
        "/orders",            # 🔥 REAL endpoint
        "/products",          # 🔥 REAL endpoint
        "/inventory"          # 🔥 REAL endpoint
    ]

    return {
        "auth": "basic_vendor_api",
        "results": [_call(p) for p in paths]
    }