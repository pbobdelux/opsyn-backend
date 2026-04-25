from contextlib import asynccontextmanager
from datetime import datetime, timezone
import logging
import os
import traceback
from typing import Optional

from fastapi import FastAPI, HTTPException, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

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

app.include_router(assistant_router)

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
def sync_leaflink(x_opsyn_secret: Optional[str] = Header(None)):
    expected = os.getenv("OPSYN_SYNC_SECRET")

    if expected and x_opsyn_secret != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")

    return {"ok": True, "message": "Sync endpoint reachable"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8000)),
    )