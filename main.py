from contextlib import asynccontextmanager
from datetime import datetime, timezone
import logging
import os
import traceback
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import BrandAPICredential
from leaflink.orders import router as leaflink_orders_router
from routes.ai import router as ai_router
from routes.crm import router as crm_router
from routes.leaflink_debug import router as leaflink_debug_router
from utils.json_utils import make_json_safe

logger = logging.getLogger("opsyn-backend")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

APP_NAME = "Opsyn Backend"
APP_ENV = os.getenv("ENVIRONMENT", "production")

DRIVERS = {}


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


async def _create_assistant_tables() -> None:
    """Create assistant tables if they don't exist (idempotent)."""
    try:
        from database import Base, engine

        if engine is None:
            logger.warning("startup: DATABASE_URL not set — skipping assistant table creation")
            return

        # Import assistant models so SQLAlchemy registers them with Base.metadata
        import models.assistant_models  # noqa: F401

        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        logger.info("startup: assistant tables created/verified")
    except Exception as exc:
        logger.error("startup: failed to create assistant tables: %s", exc)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"Starting {APP_NAME} in {APP_ENV}")
    await _create_assistant_tables()
    yield
    logger.info("Shutting down")


app = FastAPI(title=APP_NAME, lifespan=lifespan)

# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------
from routes.assistant import router as assistant_router  # noqa: E402
from routes.ingest import router as ingest_router  # noqa: E402

app.include_router(assistant_router)
app.include_router(ingest_router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------
app.include_router(ai_router, prefix="/ai")
app.include_router(crm_router)
app.include_router(leaflink_orders_router)
app.include_router(leaflink_debug_router, prefix="/leaflink")


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
    return JSONResponse(
        status_code=500,
        content={"ok": False, "error": "internal_error"},
    )


@app.get("/")
def root():
    return {"ok": True, "service": APP_NAME, "env": APP_ENV}


@app.get("/health")
def health():
    return {"ok": True, "status": "healthy", "time": utc_now_iso()}


@app.get("/orders")
def get_orders():
    return {"ok": True, "orders": []}


@app.post("/orders")
def receive_orders(body: dict):
    return {"ok": True, "received": True}


@app.post("/error")
def receive_error(body: dict):
    logger.error(f"Client reported error: {body}")
    return {"ok": True, "received": True}


@app.post("/api/twin/leaflink/snapshot")
def receive_leaflink_snapshot(body: dict):
    return {"ok": True, "received": True}


@app.get("/organizations/{org_id}/drivers")
def get_drivers(org_id: str):
    return {
        "ok": True,
        "org_id": org_id,
        "drivers": DRIVERS.get(org_id, []),
    }


@app.post("/organizations/{org_id}/drivers")
def create_driver(org_id: str, body: dict):
    required = ["name", "email", "phone", "pin"]
    missing = [field for field in required if not body.get(field)]

    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Missing required driver fields: {', '.join(missing)}",
        )

    driver = {
        "id": f"driver_{len(DRIVERS.get(org_id, [])) + 1}",
        "org_id": org_id,
        "name": body.get("name"),
        "email": body.get("email"),
        "phone": body.get("phone"),
        "pin": body.get("pin"),
        "license_plate": body.get("license_plate"),
        "notes": body.get("notes"),
        "is_active": body.get("is_active", True),
        "created_at": utc_now_iso(),
    }

    DRIVERS.setdefault(org_id, []).append(driver)

    return {
        "ok": True,
        "message": "Driver created",
        "driver": driver,
    }


@app.post("/sync/leaflink/run")
async def sync_leaflink(
    x_opsyn_secret: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
):
    """
    Sync LeafLink orders for all active brands.
    Requires X-OPSYN-SECRET header for external calls.
    Internal worker calls can omit the header.
    """
    expected = os.getenv("OPSYN_SYNC_SECRET")

    # Require auth for external calls; allow internal calls without header
    if expected and x_opsyn_secret != expected:
        logger.warning("leaflink: sync_endpoint unauthorized attempt")
        raise HTTPException(status_code=401, detail="Unauthorized")

    logger.info("leaflink: sync_endpoint called")

    try:
        from services.leaflink_sync import sync_leaflink_orders
        from services.leaflink_client import LeafLinkClient

        all_results = []
        total_created = 0
        total_updated = 0
        total_skipped = 0
        total_lines = 0

        async with db.begin():
            # Step 1: Resolve active credentials inside the transaction.
            cred_result = await db.execute(
                select(BrandAPICredential).where(
                    BrandAPICredential.integration_name == "leaflink",
                    BrandAPICredential.is_active == True,
                )
            )
            credentials = cred_result.scalars().all()

            if not credentials:
                logger.warning("leaflink: sync_endpoint no active credentials found")
                return {
                    "ok": False,
                    "error": "No active LeafLink credentials found",
                }

            logger.info("leaflink: sync_endpoint found %s active credentials", len(credentials))

            for cred in credentials:
                # Extract scalar values while the ORM object is still in scope.
                brand_id: str = cred.brand_id
                api_key: str | None = cred.api_key
                company_id: str | None = cred.company_id

                logger.info(
                    "leaflink: sync_endpoint syncing brand_id=%s",
                    brand_id,
                )

                try:
                    # Step 2: Fetch orders from the LeafLink API (pure HTTP, no DB).
                    logger.info("leaflink: sync_client_init brand_id=%s", brand_id)
                    client = LeafLinkClient(api_key=api_key, company_id=company_id)
                    logger.info("leaflink: sync_api_call_start brand_id=%s", brand_id)
                    orders = client.fetch_recent_orders(max_pages=5, normalize=True)

                    # Step 3: Upsert orders and line items inside the same transaction.
                    result = await sync_leaflink_orders(db, brand_id, orders)

                    # Step 4: Update credential sync status inside the same transaction.
                    cred.sync_status = "ok" if result.get("ok") else "failed"
                    cred.last_sync_at = datetime.now(timezone.utc)
                    if not result.get("ok"):
                        cred.last_error = result.get("error", "Unknown error")
                    else:
                        cred.last_error = None

                    logger.info(
                        "leaflink: sync_endpoint brand_id=%s result=%s created=%s updated=%s skipped=%s lines=%s",
                        brand_id,
                        "ok" if result.get("ok") else "failed",
                        result.get("created", 0),
                        result.get("updated", 0),
                        result.get("skipped", 0),
                        result.get("line_items_written", 0),
                    )

                    total_created += result.get("created", 0)
                    total_updated += result.get("updated", 0)
                    total_skipped += result.get("skipped", 0)
                    total_lines += result.get("line_items_written", 0)

                    all_results.append({
                        "brand_id": brand_id,
                        "ok": result.get("ok"),
                        "orders_fetched": result.get("orders_fetched", 0),
                        "created": result.get("created", 0),
                        "updated": result.get("updated", 0),
                        "skipped": result.get("skipped", 0),
                        "line_items_written": result.get("line_items_written", 0),
                        "error": result.get("error"),
                    })

                except Exception as brand_exc:
                    logger.error(
                        "leaflink: sync_endpoint brand_id=%s error=%s",
                        brand_id,
                        brand_exc,
                        exc_info=True,
                    )

                    # Mark credential as failed inside the same transaction.
                    cred.sync_status = "failed"
                    cred.last_sync_at = datetime.now(timezone.utc)
                    cred.last_error = str(brand_exc)

                    all_results.append({
                        "brand_id": brand_id,
                        "ok": False,
                        "error": str(brand_exc),
                    })

        # Transaction commits here — return results after the block.
        logger.info(
            "leaflink: sync_endpoint complete total_created=%s total_updated=%s total_skipped=%s total_lines=%s",
            total_created,
            total_updated,
            total_skipped,
            total_lines,
        )

        return make_json_safe({
            "ok": True,
            "total_created": total_created,
            "total_updated": total_updated,
            "total_skipped": total_skipped,
            "total_lines_written": total_lines,
            "brands_synced": len([r for r in all_results if r.get("ok")]),
            "brands_failed": len([r for r in all_results if not r.get("ok")]),
            "results": all_results,
        })

    except Exception as e:
        logger.error(
            "leaflink: sync_endpoint failed error=%s",
            e,
            exc_info=True,
        )
        return make_json_safe({
            "ok": False,
            "error": str(e),
        })




@app.post("/leaflink/sync-now")
async def sync_leaflink_now(
    x_opsyn_secret: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
):
    """
    Manually trigger LeafLink sync for all active brands.
    Requires X-OPSYN-SECRET header.

    Returns a structured response with aggregate counts:
      { "ok": true, "orders_fetched": X, "created": X, "updated": X, "line_items_written": X }
    """
    expected = os.getenv("OPSYN_SYNC_SECRET")

    if not expected or x_opsyn_secret != expected:
        logger.warning("leaflink: sync_now unauthorized attempt")
        raise HTTPException(status_code=401, detail="Unauthorized")

    logger.info("leaflink: sync_now manual trigger")

    try:
        from services.leaflink_sync import sync_leaflink_orders
        from services.leaflink_client import LeafLinkClient

        total_orders_fetched = 0
        total_created = 0
        total_updated = 0
        total_line_items_written = 0
        newest_order_date = None

        async with db.begin():
            # Step 1: Resolve active credentials inside the transaction.
            cred_result = await db.execute(
                select(BrandAPICredential).where(
                    BrandAPICredential.integration_name == "leaflink",
                    BrandAPICredential.is_active == True,
                )
            )
            credentials = cred_result.scalars().all()

            if not credentials:
                logger.warning("leaflink: sync_now no active credentials found")
                return {"ok": False, "error": "No active LeafLink credentials found"}

            for cred in credentials:
                # Extract scalar values while the ORM object is still in scope.
                brand_id: str = cred.brand_id
                api_key: str | None = cred.api_key
                company_id: str | None = cred.company_id

                logger.info("leaflink: sync_now syncing brand_id=%s", brand_id)

                # Step 2: Fetch orders from the LeafLink API (pure HTTP, no DB).
                logger.info("leaflink: sync_client_init brand_id=%s", brand_id)
                client = LeafLinkClient(api_key=api_key, company_id=company_id)
                logger.info("leaflink: sync_api_call_start brand_id=%s", brand_id)
                orders = client.fetch_recent_orders(max_pages=5, normalize=True)

                # Step 3: Upsert orders and line items inside the same transaction.
                result = await sync_leaflink_orders(db, brand_id, orders)

                # Step 4: Persist credential sync status inside the same transaction.
                cred.sync_status = "ok" if result.get("ok") else "failed"
                cred.last_sync_at = datetime.now(timezone.utc)
                if not result.get("ok"):
                    cred.last_error = result.get("error", "Unknown error")
                else:
                    cred.last_error = None

                if result.get("ok"):
                    total_orders_fetched += result.get("orders_fetched", 0)
                    total_created += result.get("created", 0)
                    total_updated += result.get("updated", 0)
                    total_line_items_written += result.get("line_items_written", 0)
                    result_date = result.get("newest_order_date")
                    if result_date and (newest_order_date is None or result_date > newest_order_date):
                        newest_order_date = result_date

        # Transaction commits here — return results after the block.
        logger.info(
            "leaflink: sync_now complete orders_fetched=%s created=%s updated=%s line_items_written=%s",
            total_orders_fetched,
            total_created,
            total_updated,
            total_line_items_written,
        )

        return make_json_safe({
            "ok": True,
            "orders_fetched": total_orders_fetched,
            "created": total_created,
            "updated": total_updated,
            "skipped": 0,
            "line_items_written": total_line_items_written,
            "newest_order_date": newest_order_date,
            "errors": [],
        })

    except Exception as e:
        logger.error("leaflink: sync_now failed error=%s", e, exc_info=True)
        return make_json_safe({
            "ok": False,
            "error": str(e),
            "orders_fetched": 0,
            "created": 0,
            "updated": 0,
            "skipped": 0,
            "line_items_written": 0,
            "newest_order_date": None,
            "errors": [str(e)],
        })


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8000)),
    )
