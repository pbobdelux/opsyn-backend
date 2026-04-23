from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal
import logging
import os
import traceback
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
from routes.routes_dispatch import router as routes_dispatch_router
from services.tenant_auth import get_authenticated_org

# =============================================================================

# Logging

# =============================================================================

logger = logging.getLogger("opsyn-backend")
logging.basicConfig(
level=logging.INFO,
format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

APP_NAME = "Opsyn Backend"
APP_VERSION = "1.0.0"
APP_ENV = os.getenv("RAILWAY_ENVIRONMENT", os.getenv("ENVIRONMENT", "local"))

# =============================================================================

# Auth / Brand

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

def utc_now_iso():
return datetime.now(timezone.utc).isoformat()

def get_active_brand_for_org(org_id: str):
return ACTIVE_BRAND.get(org_id)

def get_brand_name(brand_id):
if not brand_id:
return None
return BRANDS.get(brand_id, {}).get("brand_name", brand_id)

# =============================================================================

# Serialization

# =============================================================================

def money_to_float(v):
if v is None:
return None
if isinstance(v, Decimal):
return float(v)
try:
return float(v)
except:
return None

def cents_to_amount(c):
if c is None:
return None
return round(c / 100.0, 2)

def serialize_line(line):
quantity = getattr(line, "quantity", 0)
pulled = getattr(line, "pulled_qty", 0)
packed = getattr(line, "packed_qty", 0)

```
return {
    "id": line.id,
    "sku": line.sku,
    "product_name": line.product_name,
    "quantity": quantity,
    "pulled_qty": pulled,
    "packed_qty": packed,
    "remaining_qty": max(0, quantity - pulled),
    "unit_price": money_to_float(line.unit_price),
    "total_price": money_to_float(line.total_price),
}
```

def serialize_order(order, org_id, brand_id):
lines = [serialize_line(l) for l in getattr(order, "lines", [])]

```
return {
    "id": str(order.id),
    "order_number": order.external_order_id,
    "customer_name": order.customer_name,
    "status": order.status,
    "amount": money_to_float(order.amount) or 0,
    "line_items": lines,
    "created_at": order.created_at.isoformat() if order.created_at else None,
    "updated_at": order.updated_at.isoformat() if order.updated_at else None,
}
```

# =============================================================================

# Lifespan

# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
logger.info(f"Starting {APP_NAME} in {APP_ENV}")
try:
async with engine.begin() as conn:
await conn.run_sync(Base.metadata.create_all)
logger.info("DB connected")
except Exception as e:
logger.critical("DB FAILED")
logger.critical(str(e))
raise RuntimeError("DB failed")
yield

app = FastAPI(title=APP_NAME, lifespan=lifespan)

# =============================================================================

# Middleware

# =============================================================================

app.add_middleware(
CORSMiddleware,
allow_origins=["*"],
allow_credentials=True,
allow_methods=["*"],
allow_headers=["*"],
)

@app.middleware("http")
async def log_requests(request, call_next):
logger.info(f"{request.method} {request.url}")
response = await call_next(request)
logger.info(f"{response.status_code}")
return response

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
logger.error(str(exc))
logger.error(traceback.format_exc())
return {"ok": False, "error": "internal_error"}

# =============================================================================

# Routers

# =============================================================================

app.include_router(drivers_router, prefix="/organizations/{org_id}")
app.include_router(routes_dispatch_router, prefix="/organizations/{org_id}")

# =============================================================================

# Orders

# =============================================================================

@app.get("/orders")
async def get_orders(
org_id: Optional[str] = Query(default=None),
brand_id: Optional[str] = Query(default=None),
x_opsyn_org: Optional[str] = Header(default=None),
x_opsyn_secret: Optional[str] = Header(default=None),
db: AsyncSession = Depends(get_db),
):
if x_opsyn_org and x_opsyn_secret:
org_id = await get_authenticated_org(x_opsyn_org, x_opsyn_secret, db)
else:
org_id = org_id or "org_onboarding"

```
brand_id = brand_id or get_active_brand_for_org(org_id)

stmt = select(Order).options(selectinload(Order.lines))
result = await db.execute(stmt)
orders = result.scalars().all()

return {
    "ok": True,
    "orders": [serialize_order(o, org_id, brand_id) for o in orders],
}
```

# =============================================================================

# Sync

# =============================================================================

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
    raise HTTPException(500, "Server misconfigured")

if x_opsyn_secret != expected:
    raise HTTPException(401, "Unauthorized")

try:
    brand_id = brand_id or get_active_brand_for_org(org_id)
    return await handle_twin_sync(db, org_id, brand_id)
except Exception as e:
    logger.error(str(e))
    logger.error(traceback.format_exc())
    raise HTTPException(500, "Sync failed")
```

# =============================================================================

# Health

# =============================================================================

@app.get("/health")
async def health():
return {
"ok": True,
"env": APP_ENV,
"time": utc_now_iso(),
}
