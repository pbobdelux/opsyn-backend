import os
import requests


LEAFLINK_API_KEY = os.getenv("LEAFLINK_API_KEY", "").strip()


def test_connection():
    headers = {
        "Authorization": f"Token {LEAFLINK_API_KEY}",
        "Accept": "application/json",
    }

    url = "https://api.leaflink.com/api/v2/"

    try:
        response = requests.get(url, headers=headers, timeout=30)

        return {
            "status_code": response.status_code,
            "response": response.text[:1000],
        }

    except Exception as e:
        return {"error": str(e)}