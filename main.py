from fastapi import FastAPI
import requests
import os

app = FastAPI()

LEAFLINK_BASE_URL = os.getenv("LEAFLINK_BASE_URL", "https://app.leaflink.com/api/v2")
LEAFLINK_API_KEY = os.getenv("LEAFLINK_API_KEY", "")


@app.get("/")
def root():
    return {"status": "Opsyn backend is live"}


@app.get("/leaflink/orders")
def get_leaflink_orders():
    try:
        url = f"{LEAFLINK_BASE_URL}/orders"

        response = requests.get(
            url,
            headers={
                "Authorization": f"App {LEAFLINK_API_KEY}"
            },
            timeout=10
        )

        return {
            "status_code": response.status_code,
            "data": response.json() if response.status_code == 200 else response.text
        }

    except Exception as e:
        return {"error": str(e)}