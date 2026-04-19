import requests
import json
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from models import UploadedOrder


def sync_leaflink_orders(db: Session, api_key: str):
    url = "https://api.leaflink.com/v2/orders/"

    headers = {
        "Authorization": f"Bearer {api_key}"
    }

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print("LeafLink fetch failed:", response.text)
        return

    orders = response.json().get("results", [])

    now = datetime.now(timezone.utc)

    for o in orders:
        leaflink_id = str(o.get("id"))

        existing = db.query(UploadedOrder).filter_by(id=leaflink_id).first()

        if existing:
            existing.status = o.get("status")
            existing.leaflink_raw = o
            existing.leaflink_last_synced_at = now
            existing.stale = False
            existing.needs_reconciliation = False

        else:
            new_order = UploadedOrder(
                id=leaflink_id,
                customer_name=o.get("buyer", {}).get("name"),
                status=o.get("status"),
                raw_text=json.dumps(o),
                leaflink_raw=o,
                leaflink_last_synced_at=now,
                stale=False,
                needs_reconciliation=False,
            )
            db.add(new_order)

    db.commit()

    print(f"Synced {len(orders)} orders at {now}")