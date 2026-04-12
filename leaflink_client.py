import requests
import os

BASE_URL = os.getenv("LEAFLINK_BASE_URL")
API_KEY = os.getenv("LEAFLINK_API_KEY")


def get_headers():
    return {
        "Authorization": f"Api-Key {API_KEY}",
        "Content-Type": "application/json"
    }


def test_connection():
    url = f"{BASE_URL}/v2/orders/"

    response = requests.get(url, headers=get_headers())

    return {
        "status_code": response.status_code,
        "response": response.text
    }