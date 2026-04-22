from contextlib import asynccontextmanager
from datetime import datetime, timezone
import logging
import os
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import Base, engine, get_db
from models import BrandAPICredential, Order
from opsyn.routers.derived import router as derived_router
from services.leaflink_sync import sync_leaflink_orders

import models  # noqa: F401

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("opsyn-backend")

APP_NAME = "Opsyn Backend"
APP_VERSION = os.getenv("APP_VERSION", "1.0.0")
APP_ENV = os.getenv("RAILWAY_ENVIRONMENT", os.getenv("ENVIRONMENT", "local"))
PORT = int(os.getenv("PORT", "8000"))
TWIN_INGEST_SECRET = os.getenv("TWIN_INGEST_SECRET", "").strip()

# TEMP TESTING BRIDGE:
# Map signed-in org_onboarding to the seeded demo/test tenant that has data.
TEST_BRAND_OVERRIDES = {
    "org_onboarding": "test-brand",
}


def resolve_brand_id(brand_id: str | None) -> str | None:
    if not brand_id:
        return brand_id
    return TEST_BRAND_OVERRIDES.get(brand_id, brand_id)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def database_configured() -> bool:
    return bool(os.getenv("DATABASE_URL"))


def parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None

    text = value.strip()
    if not text:
        return None

    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)
    except Exception:
        return None


def dollars_to_cents(value: Any) -> int:
    if value is None:
        return 0
    try:
        return int(round(float(value) * 100))
    except Exception:
        return 0


def stub_envelope(key: str, value: Any, brand_id: str) -> dict:
    effective_brand_id = resolve_brand_id(brand_id) or brand_id
    return {
        "ok": True,
        "brand_id": effective_brand_id,
        key: value,
    }


class LeafLinkCredentialUpsertRequest(BaseModel):
    base_url: str | None = "https://app.leaflink.com/api/v2"
    api_key: str
    vendor_key: str | None = None
    company_id: str | None = None
    is_active: bool = True


class LeafLinkSyncRequest(BaseModel):
    brand_id: str


class TwinOrderItem(BaseModel):
    sku: str | None = None
    product_name: str | None = None
    quantity: float | int | None = None
    unit_price: float | int | None = None
    line_total: float | int | None = None


class TwinOrderRecord(BaseModel):
    external_order_id: str = Field(..., min_length=1)
    customer_name: str | None = None
    status: str | None = "unknown"
    total: float | int | None = None
    total_cents: int | None = None
    external_created_at: str | None = None
    external_updated_at: str | None = None
    notes: str | None = None
    items: list[TwinOrderItem] = Field(default_factory=list)
    raw_payload: dict[str, Any] | None = None


class TwinOrdersIngestRequest(BaseModel):
    brand_id: str
    source: str = "twin"
    orders: list[TwinOrderRecord] = Field(default_factory=list)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting %s v%s in %s on port %s", APP_NAME, APP_VERSION, APP_ENV, PORT)

    app.state.db_ready = False
    app.state.db_error = None

    if not database_configured() or engine is None:
        app.state.db_error = "DATABASE_URL is not configured"
        logger.warning(app.state.db_error)
        yield
        return

    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        app.state.db_ready = True
        logger.info("Database connected and tables ensured")
    except Exception as exc:
        app.state.db_error = str(exc)
        logger.exception("Database startup failed")

    yield

    logger.info("Shutting down %s", APP_NAME)


app = FastAPI(
    title=APP_NAME,
    version=APP_VERSION,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(derived_router)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled error on %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=500,
        content={
            "ok": False,
            "error": "internal_server_error",
            "message": str(exc),
            "path": request.url.path,
            "timestamp": utc_now_iso(),
        },
    )


@app.get("/")
async def root():
    return {
        "ok": True,
        "name": APP_NAME,
        "version": APP_VERSION,
        "environment": APP_ENV,
        "message": "Opsyn backend Twin-brain mode is live",
        "database_configured": database_configured(),
        "database_connected": getattr(app.state, "db_ready", False),
        "database_error": getattr(app.state, "db_error", None),
        "twin_ingest_configured": bool(TWIN_INGEST_SECRET),
        "timestamp": utc_now_iso(),
    }


@app.get("/health")
async def health():
    return {
        "ok": True,
        "status": "healthy",
        "database_configured": database_configured(),
        "database_connected": getattr(app.state, "db_ready", False),
        "database_error": getattr(app.state, "db_error", None),
        "twin_ingest_configured": bool(TWIN_INGEST_SECRET),
        "timestamp": utc_now_iso(),
    }


@app.get("/test123")
async def test123():
    return {"working": True}


@app.get("/debug/routes")
async def debug_routes():
    return {
        "ok": True,
        "routes": sorted(
            [
                {
                    "path": getattr(route, "path", None),
                    "name": getattr(route, "name", None),
                    "methods": sorted(list(getattr(route, "methods", []) or [])),
                }
                for route in app.routes
            ],
            key=lambda x: x["path"] or "",
        ),
        "timestamp": utc_now_iso(),
    }


@app.post("/brands/{brand_id}/credentials/leaflink")
async def upsert_leaflink_credentials(
    brand_id: str,
    payload: LeafLinkCredentialUpsertRequest,
    db: AsyncSession = Depends(get_db),
):
    effective_brand_id = resolve_brand_id(brand_id) or brand_id

    stmt = select(BrandAPICredential).where(
        BrandAPICredential.brand_id == effective_brand_id,
        BrandAPICredential.integration_name == "leaflink",
    )
    result = await db.execute(stmt)
    existing = result.scalar_one_or_none()

    if existing:
        existing.base_url = payload.base_url
        existing.api_key = payload.api_key
        existing.vendor_key = payload.vendor_key
        existing.company_id = payload.company_id
        existing.is_active = payload.is_active
        existing.last_error = None
        existing.sync_status = "idle"
        action = "updated"
    else:
        existing = BrandAPICredential(
            brand_id=effective_brand_id,
            integration_name="leaflink",
            base_url=payload.base_url,
            api_key=payload.api_key,
            vendor_key=payload.vendor_key,
            company_id=payload.company_id,
            is_active=payload.is_active,
            sync_status="idle",
        )
        db.add(existing)
        action = "created"

    await db.commit()
    await db.refresh(existing)

    return {
        "ok": True,
        "action": action,
        "credential_id": existing.id,
        "brand_id": existing.brand_id,
        "integration_name": existing.integration_name,
        "is_active": existing.is_active,
    }


@app.get("/brands/{brand_id}/credentials/leaflink")
async def get_leaflink_credentials_status(
    brand_id: str,
    db: AsyncSession = Depends(get_db),
):
    effective_brand_id = resolve_brand_id(brand_id) or brand_id

    stmt = select(BrandAPICredential).where(
        BrandAPICredential.brand_id == effective_brand_id,
        BrandAPICredential.integration_name == "leaflink",
    )
    result = await db.execute(stmt)
    credential = result.scalar_one_or_none()

    if credential is None:
        raise HTTPException(status_code=404, detail="LeafLink credentials not found")

    return {
        "ok": True,
        "brand_id": credential.brand_id,
        "integration_name": credential.integration_name,
        "base_url": credential.base_url,
        "company_id": credential.company_id,
        "is_active": credential.is_active,
        "sync_status": credential.sync_status,
        "last_sync_at": credential.last_sync_at.isoformat() if credential.last_sync_at else None,
        "last_error": credential.last_error,
        "has_api_key": bool(credential.api_key),
        "has_vendor_key": bool(credential.vendor_key),
    }


@app.post("/sync/orders/{brand_id}")
async def sync_orders_for_brand(
    brand_id: str,
    db: AsyncSession = Depends(get_db),
):
    effective_brand_id = resolve_brand_id(brand_id) or brand_id
    try:
        result = await sync_leaflink_orders(db, effective_brand_id)
        return result
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/sync/leaflink")
async def sync_leaflink_compat(
    payload: LeafLinkSyncRequest,
    db: AsyncSession = Depends(get_db),
):
    effective_brand_id = resolve_brand_id(payload.brand_id) or payload.brand_id
    try:
        result = await sync_leaflink_orders(db, effective_brand_id)
        return result
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/ingest/twin/orders")
async def ingest_twin_orders(
    payload: TwinOrdersIngestRequest,
    db: AsyncSession = Depends(get_db),
    x_opsyn_secret: str | None = Header(default=None),
):
    if not TWIN_INGEST_SECRET:
        raise HTTPException(status_code=500, detail="TWIN_INGEST_SECRET is not configured")

    if x_opsyn_secret != TWIN_INGEST_SECRET:
        raise HTTPException(status_code=401, detail="Invalid ingest secret")

    effective_brand_id = resolve_brand_id(payload.brand_id) or payload.brand_id

    now = utc_now()
    created = 0
    updated = 0
    skipped = 0

    for incoming in payload.orders:
        external_order_id = (incoming.external_order_id or "").strip()
        if not external_order_id:
            skipped += 1
            continue

        stmt = select(Order).where(
            Order.brand_id == effective_brand_id,
            Order.external_order_id == external_order_id,
        )
        result = await db.execute(stmt)
        existing = result.scalar_one_or_none()

        total_cents = incoming.total_cents
        if total_cents is None:
            total_cents = dollars_to_cents(incoming.total)

        external_created_at = parse_dt(incoming.external_created_at)
        external_updated_at = parse_dt(incoming.external_updated_at)

        customer_name = (incoming.customer_name or "Unknown").strip() or "Unknown"
        status = (incoming.status or "unknown").strip() or "unknown"

        if existing:
            existing.customer_name = customer_name
            existing.status = status
            existing.total_cents = total_cents
            existing.external_created_at = external_created_at or existing.external_created_at
            existing.external_updated_at = external_updated_at or existing.external_updated_at
            existing.synced_at = now
            existing.source = payload.source or existing.source or "twin"
            updated += 1
        else:
            order = Order(
                brand_id=effective_brand_id,
                external_order_id=external_order_id,
                customer_name=customer_name,
                status=status,
                total_cents=total_cents,
                source=payload.source or "twin",
                external_created_at=external_created_at,
                external_updated_at=external_updated_at,
                synced_at=now,
            )
            db.add(order)
            created += 1

    await db.commit()

    return {
        "ok": True,
        "brand_id": effective_brand_id,
        "source": payload.source,
        "received": len(payload.orders),
        "created": created,
        "updated": updated,
        "skipped": skipped,
        "timestamp": utc_now_iso(),
    }


@app.get("/orders")
async def get_orders(
    brand_id: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    effective_brand_id = resolve_brand_id(brand_id)

    if effective_brand_id:
        stmt = (
            select(Order)
            .where(Order.brand_id == effective_brand_id)
            .order_by(Order.id.desc())
            .limit(limit)
        )
    else:
        stmt = select(Order).order_by(Order.id.desc()).limit(limit)

    result = await db.execute(stmt)
    orders = result.scalars().all()

    return {
        "ok": True,
        "brand_id": effective_brand_id,
        "count": len(orders),
        "orders": [
            {
                "id": order.id,
                "brand_id": order.brand_id,
                "external_order_id": order.external_order_id,
                "customer_name": order.customer_name,
                "status": order.status,
                "total_cents": order.total_cents,
                "source": order.source,
                "external_created_at": order.external_created_at.isoformat() if order.external_created_at else None,
                "external_updated_at": order.external_updated_at.isoformat() if order.external_updated_at else None,
                "synced_at": order.synced_at.isoformat() if order.synced_at else None,
            }
            for order in orders
        ],
    }


# Stub endpoints so the iOS app gets 200 + empty collections instead of 404s.
# NOTE: /customers and /routes are handled by opsyn.routers.derived.
# For testing, derived.py should also use org_onboarding -> test-brand mapping if needed.


@app.get("/drivers")
async def list_drivers(brand_id: str = Query(..., description="Brand scope")):
    return stub_envelope("drivers", [], brand_id)


@app.get("/billing")
async def list_billing(brand_id: str = Query(..., description="Brand scope")):
    return stub_envelope("billing", [], brand_id)


@app.get("/api/inventory")
async def list_inventory(brand_id: str = Query(..., description="Brand scope")):
    return stub_envelope("items", [], brand_id)


@app.get("/api/packages")
async def list_packages(brand_id: str = Query(..., description="Brand scope")):
    return stub_envelope("packages", [], brand_id)


@app.get("/api/packages/{package_id}")
async def get_package(
    package_id: str,
    brand_id: str = Query(..., description="Brand scope"),
):
    effective_brand_id = resolve_brand_id(brand_id) or brand_id
    return {
        "ok": True,
        "brand_id": effective_brand_id,
        "package": None,
    }


@app.get("/mappings")
async def list_mappings(brand_id: str = Query(..., description="Brand scope")):
    return stub_envelope("mappings", [], brand_id)


@app.get("/drafts")
async def list_drafts(brand_id: str = Query(..., description="Brand scope")):
    return stub_envelope("drafts", [], brand_id)


@app.get("/production/batches")
async def list_production_batches(brand_id: str = Query(..., description="Brand scope")):
    return stub_envelope("batches", [], brand_id)


@app.get("/api")
async def api_root():
    return {"ok": True, "message": "Opsyn API root", "timestamp": utc_now_iso()}


@app.get("/api/v1")
async def api_v1_root():
    return {"ok": True, "message": "Opsyn API v1", "timestamp": utc_now_iso()}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=PORT, log_level="info")