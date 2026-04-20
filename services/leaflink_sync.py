import requests
import json
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from models import UploadedOrder


def sync_leaflink_orders(db: Session, api_key: str):
    try:
        url = "https://api.leaflink.com/v2/orders/"

        headers = {
            "Authorization": f"Bearer {api_key}"
        }

        # 🔒 Add timeout so it doesn't hang
        response = requests.get(url, headers=headers, timeout=30)

        if response.status_code != 200:
            print(f"[LeafLink] Fetch failed: {response.status_code} - {response.text}")
            return {
                "ok": False,
                "error": f"LeafLink returned {response.status_code}"
            }

        data = response.json()
        orders = data.get("results", [])

        now = datetime.now(timezone.utc)
        synced_count = 0

        for o in orders:
            try:
                leaflink_id = str(o.get("id"))

                if not leaflink_id:
                    continue

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

                synced_count += 1

            except Exception as order_error:
                print(f"[LeafLink] Failed to process order: {order_error}")
                continue

        db.commit()

        print(f"[LeafLink] Synced {synced_count} orders at {now}")

        return {
            "ok": True,
            "synced": synced_count
        }

    except Exception as e:
        print(f"[LeafLink] CRITICAL ERROR: {e}")

        # 🔥 rollback prevents DB corruption on failure
        try:
            db.rollback()
        except:
            pass

        return {
            "ok": False,
            "error": str(e)
        }