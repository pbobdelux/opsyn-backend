import os
import requests
from sqlalchemy.orm import Session
from models import Order, BrandCredential

LEAFLINK_BASE = "https://app.leaflink.com/api/v2"


def fetch_leaflink_orders(cred: BrandCredential):
    headers = {
        "Authorization": f"Api-Key {cred.api_key}",
        "Content-Type": "application/json",
    }

    # IMPORTANT: correct endpoint
    url = f"{LEAFLINK_BASE}/companies/{cred.company_id}/orders"

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise Exception(f"LeafLink error {response.status_code}: {response.text}")

    return response.json()


def sync_leaflink_orders(db: Session, brand_id: str):
    cred = (
        db.query(BrandCredential)
        .filter(
            BrandCredential.brand_id == brand_id,
            BrandCredential.integration_name == "leaflink",
            BrandCredential.is_active == True,
        )
        .first()
    )

    if not cred:
        raise Exception("No active LeafLink credentials found")

    data = fetch_leaflink_orders(cred)

    orders = data.get("results", data)

    synced = 0

    for o in orders:
        order = Order(
            brand_id=brand_id,
            external_id=str(o.get("id")),
            raw_json=o,
        )
        db.merge(order)
        synced += 1

    db.commit()

    return {
        "ok": True,
        "brand_id": brand_id,
        "synced": synced,
    }