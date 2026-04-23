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

# =========================

# Logging

# =========================

logger = logging.getLogger("opsyn-backend")
logging.basicConfig(
level=logging.INFO,
format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

APP_NAME = "Opsyn Backend"
APP_VERSION = "1.0.0"
APP_ENV = os.getenv("RAILWAY_ENVIRONMENT", os.getenv("ENVIRONMENT", "local"))

# =========================

# Auth / Brand

# =========================

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

# =========================

# Helpers

# =========================

def money_to_float(value: Any) -> float | None:
if value is None:
return None
if isinstance(value, Decimal):
return float(value)
try:
return float(value)
except:
return None

def serialize_line(line: Any) -> dict:
quantity = getattr(line, "quantity", 0) or 0
pulled = getattr(line, "pulled_qty", 0) or 0
packed = getattr(line, "packed_qty", 0) or 0

```
return {
    "id": getattr(line, "id", None),
    "sku": getattr(line, "sku", None),
    "product_name": getattr(line, "product_name", None),
    "quantity": quantity,
    "pulled_qty": pulled,
    "packed_qty": packed,
    "remaining_qty": max(0, quantity - pulled),
    "unit_price": money_to_float(getattr(line, "unit_price", None)),
    "total_price": money_to_float(getattr(line, "total_price", None)),
}
```

def serialize_order(order: Any, org_id: str, brand_id: Optional[str]) -> dict:
lines = [serialize_line(l) for l in getattr(order, "lines", [])]

```
return {
    "id": str(getattr(order, "id", "")),
    "order_number": getattr(order, "external_order_id", None),
    "customer_name": getattr(order, "customer_name", None),
    "status": getattr(order, "status", None),
    "amount": money_to_float(getattr(order, "amount", None)) or 0,
    "line_items": lines,
}
```

# =========================

# Lifespan

# =========================

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

# =========================

# Middleware

# =========================

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
logger.error(str(exc))
logger.error(traceback.format_exc())
return JSONResponse(status_code=500, content={"ok": False, "error": "internal_error"})

# =========================

# Routers

# =========================

app.include_router(drivers_router, prefix="/organizations/{org_id}")
app.include_router(routes_dispatch_router, prefix="/organizations/{org_id}")

# =========================

# Orders

# =========================

@app.get("/orders")
async def get_orders(
org_id: Optional[str] = Query(None),
brand_id: Optional[str] = Query(None),
db: AsyncSession = Depends(get_db),
):
org_id = org_id or "org_onboarding"
brand_id = brand_id or get_active_brand_for_org(org_id)

```
result = await db.execute(select(Order).options(selectinload(Order.lines)))
orders = result.scalars().all()

return {"ok": True, "orders": [serialize_order(o, org_id, brand_id) for o in orders]}
```

# =========================

# Sync

# =========================

@app.post("/sync/leaflink/run")
async def run_leaflink_sync(
org_id: str = Query("org_onboarding"),
brand_id: Optional[str] = Query(None),
x_opsyn_secret: Optional[str] = Header(None),
db: AsyncSession = Depends(get_db),
):
if x_opsyn_secret != os.getenv("OPSYN_SYNC_SECRET"):
raise HTTPException(401, "Unauthorized")

```
return await handle_twin_sync(db, org_id, brand_id or get_active_brand_for_org(org_id))
```

# =========================

# Health

# =========================

@app.get("/health")
async def health():
return {"ok": True}

# =========================

# Run

# =========================

if **name** == "**main**":
import uvicorn
uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
