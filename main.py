from contextlib import asynccontextmanager
from datetime import datetime, timezone
import asyncio
import logging
import os
import traceback
from concurrent.futures import ThreadPoolExecutor
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
from routes.health import router as health_router
from routes.integrations import router as integrations_router
from routes.integrations_health import router as integrations_health_router
from routes.leaflink_debug import router as leaflink_debug_router
from routes.orders import router as orders_router
from routes.voice import router as voice_router
from routes.voice_brain import router as voice_brain_router
from routes.debug import router as debug_router
from utils.json_utils import make_json_safe

logger = logging.getLogger("opsyn-backend")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

# Thread pool for running synchronous LeafLink HTTP calls off the event loop.
# This prevents requests.get() pagination from blocking async request handling.
_leaflink_executor = ThreadPoolExecutor(max_workers=4)

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


async def _verify_database_schema() -> None:
    """
    Verify that the database has the expected schema columns.
    Logs the database host and all columns in brand_api_credentials.
    """
    from sqlalchemy import text
    from database import engine

    if engine is None:
        logger.warning("[Startup] DATABASE_URL not set — skipping schema verification")
        return

    try:
        async with engine.connect() as conn:
            # Get database host from connection
            result = await conn.execute(text("SELECT current_database(), inet_server_addr()"))
            db_name, db_host = result.fetchone()
            logger.info("[Startup] database_url_host=%s database_name=%s", db_host or "localhost", db_name)

            # Get all columns in brand_api_credentials
            result = await conn.execute(text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'brand_api_credentials'
                ORDER BY column_name
            """))
            columns = [row[0] for row in result.fetchall()]
            logger.info("[Startup] brand_api_credentials_columns=%s", ",".join(columns))

            # Check for required columns
            required_columns = [
                'id', 'brand_id', 'integration_name', 'api_key', 'company_id',
                'is_active', 'sync_status', 'last_sync_at', 'last_error',
                'last_synced_page', 'total_pages_available', 'total_orders_available'
            ]
            missing = [col for col in required_columns if col not in columns]

            if missing:
                logger.error(
                    "[Startup] schema_verification_failed missing_columns=%s",
                    ",".join(missing),
                )
            else:
                logger.info("[Startup] schema_verification_passed all_required_columns_present")

    except Exception as exc:
        logger.error("[Startup] schema_verification_error error=%s", exc, exc_info=True)


async def _resume_interrupted_syncs() -> None:
    """
    On startup, re-enqueue any syncs that were in-progress when the service
    last shut down by inserting a pending SyncRequest row for each interrupted
    credential.  The opsyn-sync-worker will pick these up and resume from
    last_synced_page + 1.

    A sync is considered interrupted when:
      - sync_status = "syncing"
      - last_synced_page is not None
      - total_pages_available is not None
      - last_synced_page < total_pages_available
    """
    try:
        from database import AsyncSessionLocal
        from models import BrandAPICredential, SyncRequest

        if AsyncSessionLocal is None:
            logger.warning("startup: DATABASE_URL not set — skipping sync resume check")
            return

        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(BrandAPICredential).where(
                    BrandAPICredential.integration_name == "leaflink",
                    BrandAPICredential.is_active == True,
                    BrandAPICredential.sync_status == "syncing",
                )
            )
            interrupted = result.scalars().all()

        resumable = [
            c for c in interrupted
            if (
                c.last_synced_page is not None
                and c.total_pages_available is not None
                and c.last_synced_page < c.total_pages_available
                and c.api_key
                and c.company_id
            )
        ]

        if not resumable:
            logger.info("startup: no interrupted syncs to resume")
            return

        for cred in resumable:
            resume_from = (cred.last_synced_page or 0) + 1
            logger.info(
                "startup: re_enqueuing_interrupted_sync brand=%s from_page=%s total_pages=%s",
                cred.brand_id,
                resume_from,
                cred.total_pages_available,
            )
            try:
                async with AsyncSessionLocal() as enqueue_sess:
                    async with enqueue_sess.begin():
                        enqueue_sess.add(SyncRequest(
                            brand_id=cred.brand_id,
                            status="pending",
                            start_page=resume_from,
                            total_pages=cred.total_pages_available,
                            total_orders_available=cred.total_orders_available,
                        ))
                logger.info(
                    "startup: enqueued_interrupted_sync brand=%s start_page=%s",
                    cred.brand_id,
                    resume_from,
                )
            except Exception as enqueue_exc:
                logger.error(
                    "startup: enqueue_interrupted_sync_failed brand=%s error=%s",
                    cred.brand_id,
                    enqueue_exc,
                )

    except Exception as exc:
        logger.error("startup: resume_interrupted_syncs_failed error=%s", exc, exc_info=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan: startup and shutdown."""

    # PRODUCTION FAIL-FAST: Ensure AWS RDS is used
    environment = os.getenv("ENVIRONMENT", "development").lower()
    database_url = os.getenv("DATABASE_URL", "")

    if environment == "production":
        if not database_url:
            logger.error("[BOOT_DB] PRODUCTION mode but DATABASE_URL is empty")
            raise RuntimeError("Production DATABASE_URL is not set")

        # Check if using AWS RDS
        if "rds.amazonaws.com" not in database_url:
            logger.error(
                "[BOOT_DB] PRODUCTION mode but DATABASE_URL is not AWS RDS canonical database"
            )
            raise RuntimeError(
                "Production DATABASE_URL is not AWS RDS canonical database. "
                "Expected host to contain 'rds.amazonaws.com'"
            )

        logger.info("[BOOT_DB] PRODUCTION mode using AWS RDS canonical database")

    # Log active database connection at startup
    if database_url:
        try:
            from urllib.parse import urlparse as _urlparse
            _parsed = _urlparse(database_url)
            db_host = _parsed.hostname or "unknown"
            db_port = _parsed.port or 5432
            db_name = _parsed.path.lstrip("/") or "unknown"

            # Detect provider
            if "rds.amazonaws.com" in db_host:
                provider = "aws-rds"
            elif "railway" in db_host:
                provider = "railway"
            else:
                provider = "unknown"

            logger.info(
                "[BOOT_DB] host=%s db=%s provider=%s",
                db_host,
                db_name,
                provider,
            )
        except Exception as exc:
            logger.error("[BOOT_DB] failed_to_parse_database_url error=%s", exc)
    else:
        logger.error("[BOOT_DB] DATABASE_URL not set or empty")

    logger.info(f"Starting {APP_NAME} in {APP_ENV}")

    await _create_assistant_tables()
    from services.migration_runner import run_migrations
    applied = await run_migrations()
    logger.info("[Startup] migrations_complete count=%s", len(applied))

    # Refresh connection pool after migrations so new connections see updated schema
    if applied:
        from database import refresh_connection_pool
        await refresh_connection_pool()

    # Verify database schema and log connection details
    await _verify_database_schema()

    await _resume_interrupted_syncs()
    route_count = len(app.routes)
    logger.info("[Startup] routes_registered count=%s", route_count)

    # Log all registered routes
    logger.info("[OrdersRoutes] registered_routes:")
    for route in app.routes:
        if hasattr(route, "path") and "/orders" in route.path:
            methods = getattr(route, "methods", ["GET"])
            logger.info(
                "[OrdersRoutes] path=%s methods=%s endpoint=%s",
                route.path,
                methods,
                getattr(route, "endpoint", "unknown"),
            )

    yield
    logger.info("Shutting down")


app = FastAPI(title=APP_NAME, lifespan=lifespan)

# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------
from routes.assistant import router as assistant_router  # noqa: E402
from routes.ingest import router as ingest_router  # noqa: E402
from routes.watchdog import router as watchdog_router  # noqa: E402

app.include_router(assistant_router)
app.include_router(ingest_router)
app.include_router(watchdog_router)

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
app.include_router(health_router)
app.include_router(integrations_router)
app.include_router(integrations_health_router)
app.include_router(leaflink_orders_router)
app.include_router(leaflink_debug_router, prefix="/leaflink")
app.include_router(orders_router)
app.include_router(voice_router)
app.include_router(voice_brain_router)
app.include_router(debug_router)
logger.info("[Routes] registered debug routes")


@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f"{request.method} {request.url}")
    response = await call_next(request)
    logger.info(f"{response.status_code}")
    return response


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    tb = traceback.format_exc()
    logger.error("[GlobalException] error_type=%s error=%s", type(exc).__name__, exc)
    logger.error("[GlobalException] traceback:\n%s", tb)
    return JSONResponse(
        status_code=500,
        content={
            "ok": False,
            "error": "internal_error",
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "path": str(request.url.path),
        },
    )


@app.get("/")
def root():
    return {"ok": True, "service": APP_NAME, "env": APP_ENV}


@app.get("/health")
def health():
    return {"ok": True, "status": "healthy", "time": utc_now_iso()}


@app.get("/ping")
def ping():
    logger.info("[Ping] reached")
    return {"ok": True, "timestamp": utc_now_iso()}


@app.get("/debug/routes")
def debug_routes():
    routes = []
    for route in app.routes:
        methods = list(getattr(route, "methods", None) or [])
        path = getattr(route, "path", None)
        if path is not None:
            routes.append({"path": path, "methods": methods})
    count = len(routes)
    logger.info("[RoutesDebug] reached count=%s", count)
    return {"ok": True, "routes": routes, "count": count}


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

    header_present = x_opsyn_secret is not None
    header_matches = x_opsyn_secret == expected if expected else False

    logger.info(
        "[SyncAuth] request_received header_present=%s header_matches=%s",
        header_present,
        header_matches,
    )

    # Require auth for external calls; allow internal calls without header
    if expected and x_opsyn_secret != expected:
        logger.warning(
            "[SyncAuth] unauthorized attempt expected_set=%s header_present=%s header_matches=%s",
            bool(expected),
            header_present,
            header_matches,
        )
        raise HTTPException(status_code=401, detail="Unauthorized")

    logger.info("[SyncAuth] authorized")
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
                    logger.info("[SyncEndpoint] sync_start brand=%s", brand_id)

                    # Step 2: Fetch orders from the LeafLink API (pure HTTP, no DB).
                    # Run the synchronous requests.get() calls in a thread pool so
                    # they do not block the event loop while paginating LeafLink.
                    logger.info("leaflink: sync_client_init brand_id=%s", brand_id)
                    client = LeafLinkClient(api_key=api_key, company_id=company_id, brand_id=brand_id)
                    logger.info("[SyncEndpoint] leaflink_fetch_start")
                    loop = asyncio.get_event_loop()
                    fetch_result = await asyncio.wait_for(
                        loop.run_in_executor(
                            _leaflink_executor,
                            lambda: client.fetch_recent_orders(normalize=True, brand=brand_id),
                        ),
                        timeout=300,
                    )
                    orders = fetch_result["orders"]
                    pages_fetched = fetch_result["pages_fetched"]
                    logger.info("[SyncEndpoint] leaflink_fetch_done pages=%s", pages_fetched)

                    # Step 3: Upsert orders and line items inside the same transaction.
                    result = await sync_leaflink_orders(db, brand_id, orders, pages_fetched=pages_fetched)

                    # Step 4: Update credential sync status inside the same transaction.
                    cred.sync_status = "ok" if result.get("ok") else "failed"
                    cred.last_sync_at = datetime.now(timezone.utc)
                    if not result.get("ok"):
                        cred.last_error = result.get("error", "Unknown error")
                    else:
                        cred.last_error = None

                    logger.info(
                        "[IntegrationCredentials] %s brand_id=%s",
                        "marked_success" if result.get("ok") else "marked_invalid",
                        brand_id,
                    )
                    logger.info(
                        "leaflink: sync_endpoint brand_id=%s result=%s created=%s updated=%s skipped=%s lines=%s pages=%s",
                        brand_id,
                        "ok" if result.get("ok") else "failed",
                        result.get("created", 0),
                        result.get("updated", 0),
                        result.get("skipped", 0),
                        result.get("line_items_written", 0),
                        result.get("pages_fetched", 0),
                    )
                    logger.info("[SyncEndpoint] sync_complete brand=%s", brand_id)

                    total_created += result.get("created", 0)
                    total_updated += result.get("updated", 0)
                    total_skipped += result.get("skipped", 0)
                    total_lines += result.get("line_items_written", 0)

                    all_results.append({
                        "brand_id": brand_id,
                        "ok": result.get("ok"),
                        "orders_fetched": result.get("orders_fetched", 0),
                        "pages_fetched": result.get("pages_fetched", 0),
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
                logger.info("[SyncEndpoint] sync_start brand=%s", brand_id)

                # Step 2: Fetch orders from the LeafLink API (pure HTTP, no DB).
                # Run the synchronous requests.get() calls in a thread pool so
                # they do not block the event loop while paginating LeafLink.
                logger.info("leaflink: sync_client_init brand_id=%s", brand_id)
                client = LeafLinkClient(api_key=api_key, company_id=company_id, brand_id=brand_id)
                logger.info("[SyncEndpoint] leaflink_fetch_start")
                loop = asyncio.get_event_loop()
                fetch_result = await asyncio.wait_for(
                    loop.run_in_executor(
                        _leaflink_executor,
                        lambda: client.fetch_recent_orders(normalize=True, brand=brand_id),
                    ),
                    timeout=300,
                )
                orders = fetch_result["orders"]
                pages_fetched = fetch_result["pages_fetched"]
                logger.info("[SyncEndpoint] leaflink_fetch_done pages=%s", pages_fetched)

                # Step 3: Upsert orders and line items inside the same transaction.
                result = await sync_leaflink_orders(db, brand_id, orders, pages_fetched=pages_fetched)

                # Step 4: Persist credential sync status inside the same transaction.
                cred.sync_status = "ok" if result.get("ok") else "failed"
                cred.last_sync_at = datetime.now(timezone.utc)
                if not result.get("ok"):
                    cred.last_error = result.get("error", "Unknown error")
                else:
                    cred.last_error = None

                logger.info(
                    "[IntegrationCredentials] %s brand_id=%s",
                    "marked_success" if result.get("ok") else "marked_invalid",
                    brand_id,
                )
                logger.info("[SyncEndpoint] sync_complete brand=%s", brand_id)

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
