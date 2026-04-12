import os
import requests


LEAFLINK_API_KEY = os.getenv("LEAFLINK_API_KEY", "").strip()


def test_connection():
    if not LEAFLINK_API_KEY:
        return {"error": "Missing LEAFLINK_API_KEY"}

    headers = {
        "Authorization": f"Token {LEAFLINK_API_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    test_urls = [
        "https://api.leaflink.com/api/v2/orders-received/",
        "https://api.leaflink.com/api/v2/products/",
        "https://api.leaflink.com/api/v2/buyer/orders/",
        "https://api.leaflink.com/api/v2/companies/",
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
        "auth_type": "Token",
        "key_preview": LEAFLINK_API_KEY[:6] + "...",
        "results": results,
    }