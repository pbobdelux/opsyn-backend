import os
import requests

LEAFLINK_API_KEY = os.getenv("LEAFLINK_API_KEY", "").strip()
LEAFLINK_BASE_URL = os.getenv("LEAFLINK_BASE_URL", "https://api.leaflink.com").strip()

def _call(path: str, auth_type: str):
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    if auth_type == "Token":
        headers["Authorization"] = f"Token {LEAFLINK_API_KEY}"
    elif auth_type == "App":
        headers["Authorization"] = f"App {LEAFLINK_API_KEY}"
    else:
        headers["Authorization"] = LEAFLINK_API_KEY

    url = f"{LEAFLINK_BASE_URL}{path}"

    try:
        response = requests.get(url, headers=headers, timeout=30)
        return {
            "url": url,
            "status_code": response.status_code,
            "response_preview": response.text[:500]
        }
    except Exception as e:
        return {
            "url": url,
            "error": str(e)
        }

def test_connection():
    paths = [
        "/api/v2/orders-received",
        "/api/v2/buyer/orders",
        "/api/v2/products",
        "/api/v2/companies"
    ]

    return {
        "base_url": LEAFLINK_BASE_URL,
        "tests": {
            "Token": [_call(path, "Token") for path in paths],
            "App": [_call(path, "App") for path in paths],
        }
    }