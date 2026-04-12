import os
import requests


LEAFLINK_BASE_URL = os.getenv("LEAFLINK_BASE_URL", "https://api.leaflink.com").rstrip("/")
LEAFLINK_API_KEY = os.getenv("LEAFLINK_API_KEY", "")


def test_connection():
    if not LEAFLINK_API_KEY:
        return {"error": "Missing LEAFLINK_API_KEY"}

    headers = {
        "Authorization": f"App {LEAFLINK_API_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    test_urls = [
        f"{LEAFLINK_BASE_URL}/api/v2/orders-received/",
        f"{LEAFLINK_BASE_URL}/api/v2/orders-received",
        f"{LEAFLINK_BASE_URL}/api/v2/products/",
        f"{LEAFLINK_BASE_URL}/api/v2/products",
    ]

    results = []

    for url in test_urls:
        try:
            response = requests.get(url, headers=headers, timeout=30)
            results.append(
                {
                    "url": url,
                    "status_code": response.status_code,
                    "response_preview": response.text[:500],
                }
            )
        except Exception as e:
            results.append(
                {
                    "url": url,
                    "error": str(e),
                }
            )

    return {
        "base_url": LEAFLINK_BASE_URL,
        "auth_header_preview": f"App {LEAFLINK_API_KEY[:6]}...",
        "results": results,
    }