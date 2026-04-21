from datetime import datetime, timezone
import httpx
import logging
from sqlalchemy import select
from models import BrandAPICredential, Order

logger = logging.getLogger("leaflink_sync")


def utc_now():
    return datetime.now(timezone.utc)


# ==============================
# FETCH FROM LEAFLINK
# ==============================
async def fetch_leaflink_orders(base_url: str, api_key: str):
    url = f"{base_url.rstrip('/')}/orders-received/"

    headers = {
        "Authorization": f"App {api_key}",  # ✅ FIXED AUTH
        "Accept": "application/json",
    }

    all_orders = []

    async with httpx.AsyncClient(timeout=30.0) as client:
        while url:
            response = await client.get(url, headers=headers)

            if response.status_code >= 400:
                raise Exception(
                    f"LeafLink API error {response.status_code} - {response.text}"
                )

            data = response.json()

            # Handle multiple response formats
            if isinstance(data, list):
                all_orders.extend(data)
                break

            if isinstance(data, dict):
                results = data.get("results") or data.get("data") or []
                all_orders.extend(results)

                url = data.get("next")
            else:
                break

    return all_orders


# ==============================
# SYNC FUNCTION
# ==============================
async def sync_leaflink_orders(db, brand_id: str):
    now = utc_now()

    # Get credentials
    result = await db.execute(
        select(BrandAPICredential).where(
            BrandAPICredential.brand_id == brand_id,
            BrandAPICredential.integration_name == "leaflink",
        )
    )
    cred = result.scalar_one_or_none()

    if not cred:
        raise Exception(f"No LeafLink credentials for {brand_id}")

    if not cred.api_key:
        raise Exception("Missing API key")

    try:
        raw_orders = await fetch_leaflink_orders(
            cred.base_url or "https://app.leaflink.com/api/v2",
            cred.api_key,
        )

        created = 0
        updated = 0

        for o in raw_orders:
            external_id = str(o.get("id") or o.get("uid") or o.get("number"))

            if not external_id:
                continue

            result = await db.execute(
                select(Order).where(
                    Order.brand_id == brand_id,
                    Order.external_order_id == external_id,
                    Order.source == "leaflink",
                )
            )
            existing = result.scalar_one_or_none()

            customer = (
                o.get("customer_display_name")
                or o.get("customer_name")
                or "Unknown"
            )

            status = o.get("status") or "unknown"
            total = int(float(o.get("total") or 0) * 100)

            if existing:
                existing.customer_name = customer
                existing.status = status
                existing.total_cents = total
                existing.synced_at = now
                updated += 1
            else:
                db.add(
                    Order(
                        brand_id=brand_id,
                        external_order_id=external_id,
                        customer_name=customer,
                        status=status,
                        total_cents=total,
                        source="leaflink",
                        synced_at=now,
                    )
                )
                created += 1

        await db.commit()

        return {
            "ok": True,
            "fetched": len(raw_orders),
            "created": created,
            "updated": updated,
        }

    except Exception as e:
        await db.rollback()

        return {
            "ok": False,
            "error": str(e),
        }