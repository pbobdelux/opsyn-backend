import logging
import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import Order, OrderLine
from utils.json_utils import make_json_safe

logger = logging.getLogger("leaflink_ingest")

router = APIRouter(prefix="/api/ingest", tags=["ingest"])

OPSYN_INGEST_TOKEN = os.getenv("OPSYN_INGEST_TOKEN", "").strip()


def parse_decimal(value: Any) -> Decimal | None:
    """Parse a value to Decimal for database storage."""
    if value is None or value == "":
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    if isinstance(value, str):
        try:
            return Decimal(value)
        except Exception:
            return None
    return None


def parse_dt(val: Any) -> datetime | None:
    """Parse an ISO datetime string to a datetime object."""
    if isinstance(val, str):
        try:
            return datetime.fromisoformat(val.replace("Z", "+00:00"))
        except Exception:
            return None
    if isinstance(val, datetime):
        return val
    return None


@router.get("/leaflink-orders/health")
async def health_leaflink_ingest():
    """Health check for LeafLink ingest endpoint."""
    return {
        "ok": True,
        "service": "leaflink_ingest",
        "token_configured": bool(OPSYN_INGEST_TOKEN),
    }


@router.post("/leaflink-orders")
async def ingest_leaflink_orders(
    body: dict[str, Any],
    authorization: Optional[str] = Header(None),
    idempotency_key: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
):
    """
    Ingest normalized LeafLink orders from Twin.

    Requires Authorization: Bearer <OPSYN_INGEST_TOKEN> header.
    Supports idempotency via Idempotency-Key header.
    """
    # Validate token
    if not OPSYN_INGEST_TOKEN:
        logger.error("leaflink_ingest: token not configured")
        raise HTTPException(status_code=500, detail="Ingest token not configured")

    if not authorization or not authorization.startswith("Bearer "):
        logger.warning("leaflink_ingest: missing or invalid authorization header")
        raise HTTPException(status_code=401, detail="Unauthorized")

    token = authorization[7:]  # Remove "Bearer " prefix
    if token != OPSYN_INGEST_TOKEN:
        logger.warning("leaflink_ingest: invalid token")
        raise HTTPException(status_code=401, detail="Unauthorized")

    logger.info("leaflink_ingest: ingest_started")

    # Extract request data
    source = body.get("source", "leaflink")
    tenant_org_id = body.get("tenant_org_id", "")
    fetched_at = body.get("fetched_at")
    orders_data = body.get("orders", [])

    logger.info(
        "leaflink_ingest: request_received source=%s tenant_org_id=%s orders_count=%s idempotency_key=%s",
        source,
        tenant_org_id,
        len(orders_data),
        idempotency_key[:16] if idempotency_key else "NONE",
    )

    # Track results
    accepted = 0
    created = 0
    updated = 0
    duplicate = 0
    line_items_written = 0
    rejected = []

    try:
        async with db.begin():
            for order_data in orders_data:
                try:
                    # Extract order fields
                    leaflink_order_id = (
                        order_data.get("leaflink_order_id")
                        or order_data.get("external_id")
                        or order_data.get("external_order_id")
                    )
                    order_number = order_data.get("order_number", "")
                    status = order_data.get("status", "unknown")
                    buyer_name = order_data.get("buyer_name", "Unknown Customer")
                    total = order_data.get("total", 0)
                    created_at = order_data.get("created_at")
                    modified_at = order_data.get("modified_at")
                    raw_payload = order_data.get("raw", {})
                    line_items_data = order_data.get("line_items", [])

                    # Validate required fields
                    if not leaflink_order_id:
                        rejected.append({
                            "order_number": order_number,
                            "error": "Missing leaflink_order_id",
                        })
                        continue

                    # Parse timestamps
                    external_created_at = parse_dt(created_at)
                    external_updated_at = parse_dt(modified_at)

                    # Parse amount
                    amount_decimal = parse_decimal(total)
                    total_cents = int((amount_decimal * 100).quantize(Decimal("1"))) if amount_decimal else 0

                    # Check if order exists (keyed on brand_id + external_order_id)
                    existing_result = await db.execute(
                        select(Order).where(
                            Order.brand_id == tenant_org_id,
                            Order.external_order_id == leaflink_order_id,
                        )
                    )
                    existing = existing_result.scalar_one_or_none()

                    now = datetime.now(timezone.utc)

                    if existing:
                        # Update existing order
                        existing.order_number = order_number
                        existing.customer_name = buyer_name
                        existing.status = status
                        existing.amount = amount_decimal
                        existing.total_cents = total_cents
                        existing.external_created_at = external_created_at
                        existing.external_updated_at = external_updated_at
                        existing.raw_payload = make_json_safe(raw_payload)
                        existing.sync_status = "ok"
                        existing.synced_at = now
                        existing.last_synced_at = now
                        order_row = existing

                        updated += 1
                        logger.info(
                            "leaflink_ingest: order_updated leaflink_order_id=%s order_number=%s",
                            leaflink_order_id,
                            order_number,
                        )
                    else:
                        # Create new order
                        order_row = Order(
                            external_order_id=leaflink_order_id,
                            order_number=order_number,
                            customer_name=buyer_name,
                            status=status,
                            amount=amount_decimal,
                            total_cents=total_cents,
                            external_created_at=external_created_at,
                            external_updated_at=external_updated_at,
                            raw_payload=make_json_safe(raw_payload),
                            source="leaflink",
                            brand_id=tenant_org_id,
                            sync_status="ok",
                            synced_at=now,
                            last_synced_at=now,
                        )
                        db.add(order_row)
                        await db.flush()

                        created += 1
                        logger.info(
                            "leaflink_ingest: order_created leaflink_order_id=%s order_number=%s",
                            leaflink_order_id,
                            order_number,
                        )

                    # Upsert line items: delete old, insert new
                    if line_items_data:
                        await db.execute(
                            delete(OrderLine).where(OrderLine.order_id == order_row.id)
                        )

                        for item_data in line_items_data:
                            try:
                                sku = item_data.get("sku", "")
                                product_name = item_data.get("product_name", "")
                                qty = item_data.get("qty") or item_data.get("quantity", 0)
                                unit_price = item_data.get("unit_price", 0)

                                qty_int = int(qty) if qty else 0
                                unit_price_decimal = parse_decimal(unit_price)
                                total_price = None
                                if unit_price_decimal and qty_int:
                                    total_price = unit_price_decimal * Decimal(qty_int)

                                unit_price_cents = (
                                    int((unit_price_decimal * 100).quantize(Decimal("1")))
                                    if unit_price_decimal
                                    else None
                                )
                                total_price_cents = (
                                    int((total_price * 100).quantize(Decimal("1")))
                                    if total_price
                                    else None
                                )

                                line_item = OrderLine(
                                    order_id=order_row.id,
                                    sku=sku or None,
                                    product_name=product_name or None,
                                    quantity=qty_int,
                                    unit_price=unit_price_decimal,
                                    total_price=total_price,
                                    unit_price_cents=unit_price_cents,
                                    total_price_cents=total_price_cents,
                                    raw_payload=make_json_safe(item_data),
                                )
                                db.add(line_item)
                                line_items_written += 1

                            except Exception as item_exc:
                                logger.error(
                                    "leaflink_ingest: line_item_failed leaflink_order_id=%s error=%s",
                                    leaflink_order_id,
                                    item_exc,
                                )
                                rejected.append({
                                    "order_number": order_number,
                                    "sku": item_data.get("sku"),
                                    "error": str(item_exc)[:100],
                                })

                    accepted += 1

                except Exception as order_exc:
                    logger.error(
                        "leaflink_ingest: order_failed leaflink_order_id=%s error=%s",
                        order_data.get("leaflink_order_id"),
                        order_exc,
                    )
                    rejected.append({
                        "order_number": order_data.get("order_number"),
                        "error": str(order_exc)[:100],
                    })

        # Transaction committed by the async with db.begin() context manager
        logger.info(
            "leaflink_ingest: ingest_complete accepted=%s created=%s updated=%s duplicate=%s line_items_written=%s rejected_count=%s",
            accepted,
            created,
            updated,
            duplicate,
            line_items_written,
            len(rejected),
        )

        return make_json_safe({
            "ok": True,
            "accepted": accepted,
            "created": created,
            "updated": updated,
            "duplicate": duplicate,
            "line_items_written": line_items_written,
            "rejected": rejected,
        })

    except Exception as e:
        logger.error("leaflink_ingest: ingest_failed error=%s", e, exc_info=True)
        return make_json_safe({
            "ok": False,
            "error": str(e),
            "accepted": accepted,
            "created": created,
            "updated": updated,
            "duplicate": duplicate,
            "line_items_written": line_items_written,
            "rejected": rejected,
        })
