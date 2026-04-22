from fastapi import FastAPI, Request, HTTPException, Query, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import os
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import engine, Base, get_db
from models import Order

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
    return brand.get("brand_name") if brand else brand_id

# =============================================================================
# Lifespan
# =============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"🚀 Starting {APP_NAME} v{APP_VERSION} in {APP_ENV}")
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("✅ Connected to AWS RDS Postgres successfully")
    except Exception as e:
        logger.error(f"❌ Failed to connect to AWS RDS: {e}")
    yield
    logger.info(f"🛑 Shutting down {APP_NAME}")

app = FastAPI(title=APP_NAME, version=APP_VERSION, lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
# Orders Endpoint - Real DB Query
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

    query = select(Order).where(Order.brand_id == effective_brand_id)
    if status and status.lower() != "all":
        query = query.where(Order.status.ilike(f"%{status}%"))

    result = await db.execute(query)
    orders_db = result.scalars().all()

    order_list = []
    for o in orders_db:
        order_list.append({
            "id": str(o.id),
            "order_number": getattr(o, "order_number", None) or o.external_order_id,
            "org_id": effective_org_id,
            "brand_id": effective_brand_id,
            "customer_name": o.customer_name or "Unknown Customer",
            "status": o.status or "submitted",
            "review_status": "ready",
            "amount