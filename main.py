from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal
import logging
import os
from typing import Any, Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from ai.twin_ai import handle_twin_sync
from database import Base, engine, get_db
from models import Order, OrderLine
from routes.drivers import router as drivers_router
from routes.routes_dispatch import router as routes_router

logger = logging.getLogger("opsyn-backend")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

APP_NAME = "Opsyn Backend"
APP_VERSION = "1.0.0"
APP_ENV = os.getenv("RAILWAY_ENVIRONMENT", os.getenv("ENVIRONMENT", "local"))

# =============================================================================
# Auth and Brand Configuration
# =============================================================================
PINS = {
    "1234": {"is_super_admin": False, "org_id": "org_onboarding", "allowed_brands": ["noble-nectar"]},
    "1263": {"is_super_admin": True, "org_id": "org_onboarding", "allowed_brands": ["noble-nectar", "test-brand"]},
    "0420": {"is_super_admin": True, "org_id": "org_onboarding", "allowed_brands": ["noble-nectar", "test-brand"]},
}

ACTIVE_BRAND = {"org_onboarding": "noble-nectar"}

BRANDS = {
    "noble-nectar": {"brand_id": "noble-nectar", "brand_name": "Noble Nectar"},
    "test-brand": {"brand_id": "test-brand", "brand_name": "Test Brand"},
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_active_brand_for_org(org_id: str) -> Optional[str]:
    return ACTIVE_BRAND.get(org_id)


def get_brand_name(brand_id: Optional[str]) -> Optional[str]:
    if not brand_id:
        return None
    brand = BRANDS.get(brand_id)
    return brand["brand_name"] if brand else brand_id


def money_to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def cents_to_amount(cents: int | None) -> float | None:
    if cents is None:
        return None
    return round(cents / 100.0, 2)


def serialize_db_line(line: Any) -> dict[str, Any]:
    unit_price = money_to_float(getattr(line, "unit_price", None))
    total_price = money_to_float(getattr(line, "total_price", None))

    unit_price_cents = getattr(line, "unit_price_cents", None)
    total_price_cents = getattr(line, "total_price_cents", None)

    if unit_price is None and unit_price_cents is not None:
        unit_price = cents_to_amount(unit_price_cents)

    if total_price is None and total_price_cents is not None:
        total_price = cents_to_amount(total_price_cents)

    quantity = getattr(line, "quantity", None) or 0
    pulled_qty = getattr(line, "pulled_qty", 0) or 0
    packed_qty = getattr(line, "packed_qty", 0) or 0
    remaining_qty = max(0, quantity - pulled_qty)

    return {
        "id": getattr(line, "id", None),
        "sku": getattr(line, "sku", None),
        "product_name": getattr(line, "product_name", None),
        "quantity": quantity,
        "pulled_qty": pulled_qty,
        "packed_qty": packed_qty,
        "remaining_qty": remaining_qty,
        "unit_price": unit_price,
        "total_price": total_price,
        "mapped_product_id": getattr(line, "mapped_product_id", None),
        "mapping_status": getattr(line, "mapping_status", None) or "unknown",
        "mapping_issue": getattr(line, "mapping_issue", None),
    }


def serialize_json_line(item: dict[str, Any]) -> dict[str, Any]:
    quantity = item.get("quantity")
    if quantity is None:
        quantity = item.get("qty", 0)

    unit_price = item.get("unit_price")
    total_price = item.get("total_price")

    if unit_price is None and item.get("unit_price_cents") is not None:
        unit_price = cents_to_amount(item.get("unit_price_cents"))

    if total_price is None and item.get("total_price_cents") is not None:
        total_price = cents_to_amount(item.get("total_price_cents"))

    pulled_qty = item.get("pulled_qty", 0) or 0
    packed_qty = item.get("packed_qty", 0) or 0
    remaining_qty = max(0, (quantity or 0) - pulled_qty)

    return {
        "id": item.get("id"),
        "sku": item.get("sku"),
        "product_name": item.get("product_name") or item.get("name"),
        "quantity": quantity or 0,
        "pulled_qty": pulled_qty,
        "packed_qty": packed_qty,
        "remaining_qty": remaining_qty,
        "unit_price": unit_price,
        "total_price": total_price,
        "mapped_product_id": item.get("mapped_product_id"),
        "mapping_status": item.get("mapping_status", "unknown"),
        "mapping_issue": item.get("mapping_issue"),
    }


def build_line_items(order: Order) -> list[dict[str, Any]]:
    lines = getattr(order, "lines", None)
    if lines:
        return [serialize_db_line(line) for line in lines]

    raw = getattr(order, "line_items_json", None)
    if isinstance(raw, list):
        return [serialize_json_line(item) for item in raw if isinstance(item, dict)]

    if isinstance(raw, dict):
        nested = raw.get("line_items")
        if isinstance(nested, list):
            return [serialize_json_line(item) for item in nested if isinstance(item, dict)]

    return []


def derive_blockers(line_items: list[dict[str, Any]]) -> list[dict[str, str]]:
    blockers: list[dict[str, str]] = []

    unknown_lines = [
        item for item in line_items
        if (
            not item.get("sku")
            or item.get("mapping_status") in {"unknown", "unmapped", None}
            or item.get("mapping_issue")
        )
    ]

    if unknown_lines:
        blockers.append({
            "type": "mapping_issue",
            "message": "Unknown SKU",
        })

    return blockers


def derive_review_status(order: Order, line_items: list[dict[str, Any]], blockers: list[dict[str, str]]) -> str:
    if getattr(order, "review_status", None):
        return order.review_status
    if blockers:
        return "blocked"
    if not line_items:
        return "needs_review"
    return "ready"


def serialize_order(order: Order, org_id: str, brand_id: str) -> dict[str, Any]:
    line_items = build_line_items(order)

    amount = money_to_float(getattr(order, "amount", None))
    if amount is None:
        amount = cents_to_amount(getattr(order, "total_cents", None))

    item_count = getattr(order, "item_count", None)
    if item_count is None:
        item_count = len(line_items)

    unit_count = getattr(order, "unit_count", None)
    if unit_count is None:
        unit_count = sum((item.get("quantity") or 0) for item in line_items)

    blockers = derive_blockers(line_items)
    review_status = derive_review_status(order, line_items, blockers)

    external_created_at = getattr(order, "external_created_at", None)
    created_at = external_created_at or getattr(order, "created_at", None)
    updated_at = getattr(order, "updated_at", None)
    last_synced_at = getattr(order, "last_synced_at", None) or getattr(order, "synced_at", None)

    return {
        "id": str(order.id),
        "external_id": order.external_order_id,
        "order_number": getattr(order, "order_number", None) or order.external_order_id,
        "org_id": org_id,
        "brand_id": brand_id,
        "customer_name": order.customer_name or "Unknown Customer",
        "status": order.status or "submitted",
        "review_status": review_status,
        "blockers": blockers,
        "amount": amount or 0.0,
        "currency": "USD",
        "created_at": created_at.isoformat() if created_at else None,
        "updated_at": updated_at.isoformat() if updated_at else None,
        "source": order.source,
        "item_count": item_count or 0,
        "unit_count": unit_count or 0,
        "line_items": line_items,
        "sync_status": getattr(order, "sync_status", "ok") or "ok",
        "last_synced_at": last_synced_at.isoformat() if last_synced_at else None,
    }


# =============================================================================
# Lifespan
# =============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"Starting {APP_NAME} v{APP_VERSION} in {APP_ENV}")
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("Connected to AWS RDS Postgres successfully")
    except Exception as e:
        logger.error(f"Failed to connect to AWS RDS: {e}")
    yield
    logger.info(f"Shutting down {APP_NAME}")


app = FastAPI(title=APP_NAME, version=APP_VERSION, lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(drivers_router)
app.include_router(routes_router)

# =============================================================================
# Auth Endpoints
# =============================================================================
@app.post("/auth/pin-login")
def pin_login(data: dict):
    pin = data.get("pin")
    org_id = data.get("org_id")
    user = PINS.get(pin)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid PIN")
    return {
        "ok": True,
        "is_super_admin": user.get("is_super_admin", False),
        "signed_in_org_id": org_id,
        "resolved_brand_id": ACTIVE_BRAND.get(org_id),
        "allowed_brands": user.get("allowed_brands", []),
    }


@app.get("/auth/brand-context")
def brand_context(org_id: str):
    active = ACTIVE_BRAND.get(org_id)
    active_name = get_brand_name(active)
    return {
        "ok": True,
        "signed_in_org_id": org_id,
        "resolved_brand_id": active,
        "available_brands": [
            {"brand_id": "noble-nectar", "brand_name": "Noble Nectar"},
            {"brand_id": "test-brand", "brand_name": "Test Brand"},
        ],
        "active_binding": {
            "brand_id": active,
            "brand_name": active_name,
            "is_default": True,
            "is_active": True,
        },
    }


@app.post("/auth/brand-context/select")
def select_brand(data: dict):
    brand_id = data.get("brand_id")
    org_id = data.get("org_id")
    if brand_id not in BRANDS:
        raise HTTPException(status_code=404, detail="Brand not found")
    ACTIVE_BRAND[org_id] = brand_id
    return {"ok": True, "resolved_brand_id": brand_id}


# =============================================================================
# Orders Endpoints - DB-backed normalized response
# =============================================================================
@app.get("/orders")
@app.get("/api/orders")
@app.get("/api/v1/orders")
async def get_orders(
    org_id: Optional[str] = Query(default=None),
    brand_id: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    effective_org_id = org_id or "org_onboarding"
    effective_brand_id = brand_id or get_active_brand_for_org(effective_org_id)

    if not effective_brand_id:
        return {
            "ok": True,
            "org_id": effective_org_id,
            "brand_id": None,
            "brand_name": None,
            "count": 0,
            "summary": {
                "all": 0,
                "ready": 0,
                "needs_review": 0,
                "blocked": 0,
                "total_amount": 0.0,
                "currency": "USD",
            },
            "orders": [],
        }

    query_stmt = (
        select(Order)
        .options(selectinload(Order.lines))
        .where(Order.brand_id == effective_brand_id)
    )

    if status and status.lower() != "all":
        query_stmt = query_stmt.where(Order.status.ilike(f"%{status}%"))

    if q:
        like_term = f"%{q}%"
        query_stmt = query_stmt.where(
            (Order.customer_name.ilike(like_term))
            | (Order.external_order_id.ilike(like_term))
            | (Order.order_number.ilike(like_term))
        )

    query_stmt = query_stmt.order_by(Order.external_updated_at.desc(), Order.updated_at.desc())

    result = await db.execute(query_stmt)
    orders_db = result.scalars().all()

    order_list = [serialize_order(o, effective_org_id, effective_brand_id) for o in orders_db]

    total_amount = round(sum((o.get("amount") or 0.0) for o in order_list), 2)

    return {
        "ok": True,
        "org_id": effective_org_id,
        "brand_id": effective_brand_id,
        "brand_name": get_brand_name(effective_brand_id),
        "count": len(order_list),
        "summary": {
            "all": len(order_list),
            "ready": len([o for o in order_list if o.get("review_status") == "ready"]),
            "needs_review": len([o for o in order_list if o.get("review_status") == "needs_review"]),
            "blocked": len([o for o in order_list if o.get("review_status") == "blocked"]),
            "total_amount": total_amount,
            "currency": "USD",
        },
        "orders": order_list,
    }


@app.get("/orders/{order_id}")
async def get_order_detail(
    order_id: int,
    org_id: Optional[str] = Query(default=None),
    brand_id: Optional[str] = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    effective_org_id = org_id or "org_onboarding"
    effective_brand_id = brand_id or get_active_brand_for_org(effective_org_id)

    query_stmt = (
        select(Order)
        .options(selectinload(Order.lines))
        .where(Order.id == order_id)
    )

    if effective_brand_id:
        query_stmt = query_stmt.where(Order.brand_id == effective_brand_id)

    result = await db.execute(query_stmt)
    order = result.scalar_one_or_none()

    if not order:
        raise HTTPException(status_code=404, detail="Order not found")

    return {
        "ok": True,
        "order": serialize_order(order, effective_org_id, effective_brand_id or order.brand_id),
    }


# =============================================================================
# Order Line Fulfillment Endpoints
# =============================================================================
@app.post("/orders/{order_id}/lines/{line_id}/mark-pulled")
async def mark_line_pulled(
    order_id: int,
    line_id: int,
    data: dict,
    org_id: Optional[str] = Query(default=None),
    brand_id: Optional[str] = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    """Mark an order line as pulled. Increments pulled_qty by the amount specified."""
    effective_org_id = org_id or "org_onboarding"
    effective_brand_id = brand_id or get_active_brand_for_org(effective_org_id)

    order_stmt = select(Order).where(Order.id == order_id)
    if effective_brand_id:
        order_stmt = order_stmt.where(Order.brand_id == effective_brand_id)

    order_result = await db.execute(order_stmt)
    order = order_result.scalar_one_or_none()

    if not order:
        raise HTTPException(status_code=404, detail="Order not found")

    line_stmt = select(OrderLine).where(
        (OrderLine.id == line_id) & (OrderLine.order_id == order_id)
    )
    line_result = await db.execute(line_stmt)
    line = line_result.scalar_one_or_none()

    if not line:
        raise HTTPException(status_code=404, detail="Order line not found")

    pull_amount = data.get("amount", 0)
    if pull_amount <= 0:
        raise HTTPException(status_code=400, detail="Pull amount must be greater than 0")

    max_pullable = (line.quantity or 0) - (line.pulled_qty or 0)
    if pull_amount > max_pullable:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot pull {pull_amount}. Max pullable: {max_pullable}"
        )

    line.pulled_qty = (line.pulled_qty or 0) + pull_amount
    line.updated_at = datetime.now(timezone.utc)

    await db.commit()
    await db.refresh(line)

    return {
        "ok": True,
        "line_id": line.id,
        "pulled_qty": line.pulled_qty,
        "packed_qty": line.packed_qty,
        "remaining_qty": max(0, (line.quantity or 0) - line.pulled_qty),
    }


@app.post("/orders/{order_id}/lines/{line_id}/mark-packed")
async def mark_line_packed(
    order_id: int,
    line_id: int,
    data: dict,
    org_id: Optional[str] = Query(default=None),
    brand_id: Optional[str] = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    """Mark an order line as packed. Increments packed_qty by the amount specified."""
    effective_org_id = org_id or "org_onboarding"
    effective_brand_id = brand_id or get_active_brand_for_org(effective_org_id)

    order_stmt = select(Order).where(Order.id == order_id)
    if effective_brand_id:
        order_stmt = order_stmt.where(Order.brand_id == effective_brand_id)

    order_result = await db.execute(order_stmt)
    order = order_result.scalar_one_or_none()

    if not order:
        raise HTTPException(status_code=404, detail="Order not found")

    line_stmt = select(OrderLine).where(
        (OrderLine.id == line_id) & (OrderLine.order_id == order_id)
    )
    line_result = await db.execute(line_stmt)
    line = line_result.scalar_one_or_none()

    if not line:
        raise HTTPException(status_code=404, detail="Order line not found")

    pack_amount = data.get("amount", 0)
    if pack_amount <= 0:
        raise HTTPException(status_code=400, detail="Pack amount must be greater than 0")

    max_packable = (line.pulled_qty or 0) - (line.packed_qty or 0)
    if pack_amount > max_packable:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot pack {pack_amount}. Max packable: {max_packable}"
        )

    line.packed_qty = (line.packed_qty or 0) + pack_amount
    line.updated_at = datetime.now(timezone.utc)

    await db.commit()
    await db.refresh(line)

    return {
        "ok": True,
        "line_id": line.id,
        "pulled_qty": line.pulled_qty,
        "packed_qty": line.packed_qty,
        "remaining_qty": max(0, (line.quantity or 0) - line.pulled_qty),
    }


@app.get("/master-pick-list")
async def get_master_pick_list(
    org_id: Optional[str] = Query(default=None),
    brand_id: Optional[str] = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    """
    Returns aggregated pick list: SKU + product name grouped with total remaining qty.
    Remaining qty = quantity - pulled_qty for each line.
    """
    effective_org_id = org_id or "org_onboarding"
    effective_brand_id = brand_id or get_active_brand_for_org(effective_org_id)

    if not effective_brand_id:
        return {
            "ok": True,
            "org_id": effective_org_id,
            "brand_id": None,
            "pick_list": [],
        }

    query_stmt = (
        select(
            OrderLine.sku,
            OrderLine.product_name,
            func.sum(OrderLine.quantity - OrderLine.pulled_qty).label("remaining_qty"),
            func.count(OrderLine.id).label("line_count"),
        )
        .join(Order)
        .where(Order.brand_id == effective_brand_id)
        .group_by(OrderLine.sku, OrderLine.product_name)
        .having(func.sum(OrderLine.quantity - OrderLine.pulled_qty) > 0)
        .order_by(OrderLine.sku)
    )

    result = await db.execute(query_stmt)
    rows = result.all()

    pick_list = [
        {
            "sku": row.sku,
            "product_name": row.product_name,
            "remaining_qty": int(row.remaining_qty) if row.remaining_qty else 0,
            "line_count": row.line_count,
        }
        for row in rows
    ]

    return {
        "ok": True,
        "org_id": effective_org_id,
        "brand_id": effective_brand_id,
        "pick_list": pick_list,
    }


# =============================================================================
# Legacy compatibility route
# =============================================================================
@app.get("/leaflink/orders")
async def get_leaflink_orders_legacy(
    org_id: Optional[str] = Query(default=None),
    brand_id: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    return await get_orders(
        org_id=org_id,
        brand_id=brand_id,
        status=status,
        q=q,
        db=db,
    )


# =============================================================================
# Sync Endpoint - Twin AI as the brain
# =============================================================================
@app.post("/sync/leaflink/run")
async def run_leaflink_sync(
    org_id: str = Query(default="org_onboarding"),
    brand_id: Optional[str] = Query(default=None),
    x_opsyn_secret: Optional[str] = Header(default=None),
    db: AsyncSession = Depends(get_db),
):
    if x_opsyn_secret != os.getenv("OPSYN_SYNC_SECRET"):
        raise HTTPException(status_code=401, detail="Unauthorized")

    effective_brand_id = brand_id or get_active_brand_for_org(org_id)
    result = await handle_twin_sync(db, org_id, effective_brand_id)
    return result


@app.get("/health")
async def health():
    return {
        "ok": True,
        "service": APP_NAME,
        "version": APP_VERSION,
        "environment": APP_ENV,
        "timestamp": utc_now_iso(),
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)))