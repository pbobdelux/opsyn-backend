import os
import requests

BASE_URL = os.getenv("LEAFLINK_BASE_URL")
API_KEY = os.getenv("LEAFLINK_API_KEY")
API_VERSION = os.getenv("LEAFLINK_API_VERSION")


def get_headers():
    return {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
        "LeafLink-Version": API_VERSION,
    }


def test_connection():
    url = f"{BASE_URL}/orders-received?page=1&page_size=1"
    response = requests.get(url, headers=get_headers())

    print("Status Code:", response.status_code)
    print("Response Text:", response.text)

    return response.json()