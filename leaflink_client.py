import os
import requests

LEAFLINK_API_KEY = os.getenv("LEAFLINK_API_KEY", "").strip()
LEAFLINK_BASE_URL = "https://api.leaflink.com"

def _call(path: str):
    headers = {
        "Authorization": f"Bearer {LEAFLINK_API_KEY}",
        "Accept": "application/json",
    }

    url = f"{LEAFLINK_BASE_URL}{path}"

    try:
        res = requests.get(url, headers=headers, timeout=30)
        return {
            "url": url,
            "status_code": res.status_code,
            "response_preview": res.text[:300]
        }
    except Exception as e:
        return {"error": str(e)}

def test_connection():
    paths = [
        "/v1/user",          # ✅ WHO AM I
        "/v1/accounts",      # ✅ ACCOUNT ACCESS
        "/v1/brands",        # ✅ BRANDS
        "/v1/products",      # ✅ PRODUCTS
    ]

    return {
        "auth": "Bearer",
        "results": [_call(p) for p in paths]
    }