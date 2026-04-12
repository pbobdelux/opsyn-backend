import os
import requests


LEAFLINK_API_KEY = os.getenv("LEAFLINK_API_KEY", "").strip()


def get_bearer_token():
    url = "https://api.leaflink.com/v1/oauth/token"

    payload = {
        "grant_type": "client_credentials",
        "client_id": LEAFLINK_API_KEY,
        "client_secret": LEAFLINK_API_KEY,
    }

    headers = {
        "Content-Type": "application/json",
    }

    response = requests.post(url, json=payload, headers=headers)

    return {
        "status_code": response.status_code,
        "response": response.text[:500],
    }


def test_connection():
    token_response = get_bearer_token()

    return {
        "step": "token_request",
        "result": token_response,
    }