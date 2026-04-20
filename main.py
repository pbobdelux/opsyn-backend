from contextlib import asynccontextmanager
from datetime import datetime, timezone
import logging
import os

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import Base, engine, get_db
from models import BrandAPICredential, Order
from services.leaflink_sync import sync_leaflink_orders_for_brand

# IMPORTANT:
# keep this import so SQLAlchemy sees all models before create_all
import models  # noqa: F401

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("opsyn-backend")

# -----------------------------------------------------------------------------
# App metadata
# -----------------------------------------------------------------------------
APP_NAME = "Opsyn Backend"
APP_VERSION = os.getenv("APP_VERSION", "1.0.0")
APP_ENV = os.getenv("RAILWAY_ENVIRONMENT", os.getenv("ENVIRONMENT", "local"))
PORT = int(os.getenv("PORT", "8000"))

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def database_configured() -> bool:
    return bool(os.getenv("DATABASE_URL"))


# -----------------------------------------------------------------------------
# Request models
# -----------------------------------------------------------------------------
class LeafLinkCredentialUpsertRequest(BaseModel):
    base_url: str | None = "https://app.leaflink.com/api/v2"
    api_key: str
    vendor_key: str | None = None
    is_active: bool = True


class LeafLinkSyncRequest(BaseModel):
    brand_id: str


# -----------------------------------------------------------------------------
# Lifespan
# -----------------------------------------------------------------------------
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


# -----------------------------------------------------------------------------
# App
# -----------------------------------------------------------------------------
app = FastAPI(
    title=APP_NAME,
    version=APP_VERSION,
    lifespan=lifespan,
)

# -----------------------------------------------------------------------------
# CORS
# -----------------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------------------------------------------------------
# Error handler
# -----------------------------------------------------------------------------
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

# -----------------------------------------------------------------------------
# System endpoints
# -----------------------------------------------------------------------------
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
        "timestamp": utc_now_iso(),
    }


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

# -----------------------------------------------------------------------------
# Credential management
# -----------------------------------------------------------------------------
@app.post("/brands/{brand_id}/credentials/leaflink")
async def upsert_leaflink_credentials(
    brand_id: str,
    payload: LeafLinkCredentialUpsertRequest,
    db: AsyncSession = Depends(get_db),
):
    stmt = select(BrandAPICredential).where(
        BrandAPICredential.brand_id == brand_id,
        BrandAPICredential.integration_name == "leaflink",
    )
    result = await db.execute(stmt)
    existing = result.scalar_one_or_none()

    if existing:
        existing.base_url = payload.base_url
        existing.api_key = payload.api_key
        existing.vendor_key = payload.vendor_key
        existing.is_active = payload.is_active
        existing.last_error = None
        existing.sync_status = "idle"
        action = "updated"
    else:
        existing = BrandAPICredential(
            brand_id=brand_id,
            integration_name="leaflink",
            base_url=payload.base_url,
            api_key=payload.api_key,
            vendor_key=payload.vendor_key,
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
    stmt = select(BrandAPICredential).where(
        BrandAPICredential.brand_id == brand_id,
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
        "is_active": credential.is_active,
        "sync_status": credential.sync_status,
        "last_sync_at": credential.last_sync_at.isoformat() if credential.last_sync_at else None,
        "last_error": credential.last_error,
        "has_api_key": bool(credential.api_key),
        "has_vendor_key": bool(credential.vendor_key),
    }

# -----------------------------------------------------------------------------
# Sync endpoints
# -----------------------------------------------------------------------------
@app.post("/sync/orders/{brand_id}")
async def sync_orders_for_brand(
    brand_id: str,
    db: AsyncSession = Depends(get_db),
):
    try:
        result = await sync_leaflink_orders_for_brand(db, brand_id)
        return result
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/sync/leaflink")
async def sync_leaflink_compat(
    payload: LeafLinkSyncRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Compatibility route for the existing sync worker.
    The worker already calls /sync/leaflink, so this route forwards
    that request into the real brand-based sync function.
    """
    try:
        result = await sync_leaflink_orders_for_brand(db, payload.brand_id)
        return result
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

# -----------------------------------------------------------------------------
# Orders endpoint
# -----------------------------------------------------------------------------
@app.get("/orders")
async def get_orders(
    brand_id: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(Order).order_by(Order.id.desc()).limit(limit)

    if brand_id:
        stmt = (
            select(Order)
            .where(Order.brand_id == brand_id)
            .order_by(Order.id.desc())
            .limit(limit)
        )

    result = await db.execute(stmt)
    orders = result.scalars().all()

    return {
        "ok": True,
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

# -----------------------------------------------------------------------------
# API root helpers
# -----------------------------------------------------------------------------
@app.get("/api")
async def api_root():
    return {"ok": True, "message": "Opsyn API root", "timestamp": utc_now_iso()}


@app.get("/api/v1")
async def api_v1_root():
    return {"ok": True, "message": "Opsyn API v1", "timestamp": utc_now_iso()}

# -----------------------------------------------------------------------------
# Local run
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, log_level="info")