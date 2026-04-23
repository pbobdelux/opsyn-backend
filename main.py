from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal
import logging
import os
import traceback
from typing import Any, Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from ai.twin_ai import handle_twin_sync
from database import Base, engine, get_db
from models import Order, OrderLine
from routes.drivers import router as drivers_router
from routes.routes_dispatch import router as routes_dispatch_router
from services.tenant_auth import get_authenticated_org

logger = logging.getLogger("opsyn-backend")
logging.basicConfig(
level=logging.INFO,
format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

APP_NAME = "Opsyn Backend"
APP_VERSION = "1.0.0"
APP_ENV = os.getenv("RAILWAY_ENVIRONMENT", os.getenv("ENVIRONMENT", "local"))

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
return BRANDS.get(brand_id, {}).get("brand_name", brand_id)

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

def serialize_line(line: Any) -> dict[str, Any]:
quantity = getattr(line, "quantity", 0) or 0
pulled = getattr(line, "pulled_qty", 0) or 0
packed = getattr(line, "packed_qty", 0) or 0

```
unit_price = money_to_float(getattr(line, "unit_price", None))
total_price = money_to_float(getattr(line, "total_price", None))

unit_price_cents = getattr(line, "unit_price_cents", None)
total_price_cents = getattr(line, "total_price_cents", None)

if unit_price is None and unit_price_cents is not None:
    unit_price = cents_to_amount(unit_price_cents)
if total_price is None and total_price_cents is not None:
    total_price = cents_to_amount(total_price_cents)

return {
    "id": getattr(line, "id", None),
    "sku": getattr(line, "sku", None),
    "product_name": getattr(line, "product_name", None),
    "quantity": quantity,
    "pulled_qty": pulled,
    "packed_qty": packed,
    "remaining_qty": max(0, quantity - pulled),
    "unit_price": unit_price,
    "total_price": total_price,
}
```

def serialize_order(order: Any, org_id: str, brand_id: Optional[str]) -> dict[str, Any]:
lines = [serialize_line(line) for line in getattr(order, "lines", [])]

```
amount = money_to_float(getattr(order, "amount", None))
if amount is None:
    amount = cents_to_amount(getattr(order, "total_cents", None)) or 0.0

created_at = getattr(order, "created_at", None)
updated_at = getattr(order, "updated_at", None)

return {
    "id": str(getattr(order, "id", "")),
    "external_id": getattr(order, "external_order_id", None),
    "order_number": getattr(order, "order_number", None) or getattr(order, "external_order_id", None),
    "org_id": org_id,
    "brand_id": brand_id,
    "customer_name": getattr(order, "customer_name", None),
    "status": getattr(order, "status", None),
    "amount": amount,
    "line_items": lines,
    "created_at": created_at.isoformat() if created_at else None,
    "updated_at": updated_at.isoformat() if updated_at else None,
    "item_count": len(lines),
    "unit_count": sum((item.get("quantity") or 0) for item in lines),
}
```

@asynccontextmanager
async def lifespan(app: FastAPI):
logger.info(f"Starting {APP_NAME} v{APP_VERSION} in {APP_ENV}")
try:
async with engine.begin() as conn:
await conn.run_sync(Base.metadata.create_all)
logger.info("Connected to Postgres successfully")
except Exception as exc:
logger.critical("DATABASE CONNECTION FAILED - SHUTTING DOWN")
logger.critical(str(exc))
raise RuntimeError("Database connection failed")
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

@app.middleware("http")
async def log_requests(request: Request, call_next):
logger.info(f"{request.method} {request.url}")
response = await call_next(request)
logger.info(f"{response.status_code}")
return response

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
logger.error("UNHANDLED ERROR")
logger.error(str(exc))
logger.error(traceback.format_exc())
return JSONResponse(
status_code=500,
content={"ok": False, "error": "internal_server_error"},
)

app.include_router(drivers_router, prefix="/organizations/{org_id}")
app.include_router(routes_dispatch_router, prefix="/organizations/{org_id}")

@app.post("/auth/pin-login")
def pin_login(data: dict):
pin = data.get("pin")
org_id = data.get("org_id") or "org_onboarding"
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
org_id = data.get("org_id") or "org_onboarding"
if brand_id not in BRANDS:
raise HTTPException(status_code=404, detail="Brand not found")
ACTIVE_BRAND[org_id] = brand_id
return {"ok": True, "resolved_brand_id": brand_id}

@app.get("/orders")
@app.get("/api/orders")
@app.get("/api/v1/orders")
async def get_orders(
org_id: Optional[str] = Query(default=None),
brand_id: Optional[str] = Query(default=None),
status: Optional[str] = Query(default=None),
q: Optional[str] = Query(default=None),
x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
db: AsyncSession = Depends(get_db),
):
if x_opsyn_org and x_opsyn_secret:
effective_org_id = await get_authenticated_org(x_opsyn_org, x_opsyn_secret, db)
else:
effective_org_id = org_id or "org_onboarding"

```
effective_brand_id = brand_id or get_active_brand_for_org(effective_org_id)

stmt = (
    select(Order)
    .options(selectinload(Order.lines))
    .order_by(Order.updated_at.desc())
)

if effective_brand_id:
    stmt = stmt.where(Order.brand_id == effective_brand_id)

if status and status.lower() != "all":
    stmt = stmt.where(Order.status.ilike(f"%{status}%"))

if q:
    like_term = f"%{q}%"
    stmt = stmt.where(
        (Order.customer_name.ilike(like_term))
        | (Order.external_order_id.ilike(like_term))
        | (Order.order_number.ilike(like_term))
    )

result = await db.execute(stmt)
orders = result.scalars().all()

return {
    "ok": True,
    "org_id": effective_org_id,
    "brand_id": effective_brand_id,
    "brand_name": get_brand_name(effective_brand_id),
    "count": len(orders),
    "orders": [serialize_order(order, effective_org_id, effective_brand_id) for order in orders],
}
```

@app.get("/orders/{order_id}")
async def get_order_detail(
order_id: int,
org_id: Optional[str] = Query(default=None),
brand_id: Optional[str] = Query(default=None),
x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
db: AsyncSession = Depends(get_db),
):
if x_opsyn_org and x_opsyn_secret:
effective_org_id = await get_authenticated_org(x_opsyn_org, x_opsyn_secret, db)
else:
effective_org_id = org_id or "org_onboarding"

```
effective_brand_id = brand_id or get_active_brand_for_org(effective_org_id)

stmt = (
    select(Order)
    .options(selectinload(Order.lines))
    .where(Order.id == order_id)
)

if effective_brand_id:
    stmt = stmt.where(Order.brand_id == effective_brand_id)

result = await db.execute(stmt)
order = result.scalar_one_or_none()

if not order:
    raise HTTPException(status_code=404, detail="Order not found")

return {
    "ok": True,
    "order": serialize_order(order, effective_org_id, effective_brand_id or getattr(order, "brand_id", None)),
}
```

@app.post("/orders/{order_id}/lines/{line_id}/mark-pulled")
async def mark_line_pulled(
order_id: int,
line_id: int,
data: dict,
org_id: Optional[str] = Query(default=None),
brand_id: Optional[str] = Query(default=None),
x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
db: AsyncSession = Depends(get_db),
):
if x_opsyn_org and x_opsyn_secret:
effective_org_id = await get_authenticated_org(x_opsyn_org, x_opsyn_secret, db)
else:
effective_org_id = org_id or "org_onboarding"

```
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
    raise HTTPException(status_code=400, detail=f"Cannot pull {pull_amount}. Max pullable: {max_pullable}")

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
```

@app.post("/orders/{order_id}/lines/{line_id}/mark-packed")
async def mark_line_packed(
order_id: int,
line_id: int,
data: dict,
org_id: Optional[str] = Query(default=None),
brand_id: Optional[str] = Query(default=None),
x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
db: AsyncSession = Depends(get_db),
):
if x_opsyn_org and x_opsyn_secret:
effective_org_id = await get_authenticated_org(x_opsyn_org, x_opsyn_secret, db)
else:
effective_org_id = org_id or "org_onboarding"

```
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
    raise HTTPException(status_code=400, detail=f"Cannot pack {pack_amount}. Max packable: {max_packable}")

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
```

@app.get("/master-pick-list")
async def get_master_pick_list(
org_id: Optional[str] = Query(default=None),
brand_id: Optional[str] = Query(default=None),
x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
db: AsyncSession = Depends(get_db),
):
if x_opsyn_org and x_opsyn_secret:
effective_org_id = await get_authenticated_org(x_opsyn_org, x_opsyn_secret, db)
else:
effective_org_id = org_id or "org_onboarding"

```
effective_brand_id = brand_id or get_active_brand_for_org(effective_org_id)

if not effective_brand_id:
    return {
        "ok": True,
        "org_id": effective_org_id,
        "brand_id": None,
        "pick_list": [],
    }

stmt = (
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

result = await db.execute(stmt)
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
```

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

@app.post("/sync/leaflink/run")
async def run_leaflink_sync(
org_id: str = Query(default="org_onboarding"),
brand_id: Optional[str] = Query(default=None),
x_opsyn_secret: Optional[str] = Header(default=None),
db: AsyncSession = Depends(get_db),
):
expected = os.getenv("OPSYN_SYNC_SECRET")

```
if not expected:
    logger.critical("OPSYN_SYNC_SECRET NOT SET")
    raise HTTPException(status_code=500, detail="Server misconfigured")

if x_opsyn_secret != expected:
    logger.warning("Unauthorized sync attempt")
    raise HTTPException(status_code=401, detail="Unauthorized")

try:
    effective_brand_id = brand_id or get_active_brand_for_org(org_id)
    result = await handle_twin_sync(db, org_id, effective_brand_id)
    return result
except Exception as exc:
    logger.error("SYNC FAILURE")
    logger.error(str(exc))
    logger.error(traceback.format_exc())
    raise HTTPException(status_code=500, detail="Sync failed")
```

@app.get("/health")
async def health():
return {
"ok": True,
"service": APP_NAME,
"version": APP_VERSION,
"environment": APP_ENV,
"timestamp": utc_now_iso(),
}

if **name** == "**main**":
import uvicorn

```
uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
```
