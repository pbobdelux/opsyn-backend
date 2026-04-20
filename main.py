from fastapi import FastAPI, APIRouter, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import os
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

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
# Database (AWS RDS Postgres) - Async Setup
# -----------------------------------------------------------------------------
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    logger.warning("DATABASE_URL not set! Database connections will fail.")

# Create async engine for AWS RDS Postgres
engine = create_async_engine(
    DATABASE_URL,
    echo=False,                    # Set to True only for debugging
    pool_pre_ping=True,            # Helps with stale connections on Railway/AWS
    pool_size=10,
    max_overflow=20,
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    expire_on_commit=False,
    class_=AsyncSession,
)

# Base for all models
class Base(DeclarativeBase):
    pass

# -----------------------------------------------------------------------------
# Lifespan (Startup / Shutdown)
# -----------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting %s v%s in %s", APP_NAME, APP_VERSION, APP_ENV)
    logger.info("PORT=%s", PORT)

    # Test database connection on startup
    try:
        async with engine.begin() as conn:
            await conn.run_sync(lambda sync_conn: Base.metadata.create_all(sync_conn))  # Optional: create tables if missing
        logger.info("✅ Connected to AWS RDS Postgres successfully")
    except Exception as e:
        logger.error("❌ Failed to connect to AWS RDS: %s", e)

    yield

    # Cleanup
    await engine.dispose()
    logger.info("Shutting down %s", APP_NAME)

# -----------------------------------------------------------------------------
# FastAPI App
# -----------------------------------------------------------------------------
app = FastAPI(
    title=APP_NAME,
    version=APP_VERSION,
    lifespan=lifespan,
)

# -----------------------------------------------------------------------------
# CORS (tighten later for production)
# -----------------------------------------------------------------------------
ALLOWED_ORIGINS = os.getenv("CORS_ORIGINS", "*")
origins = ["*"] if ALLOWED_ORIGINS == "*" else [o.strip() for o in ALLOWED_ORIGINS.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

# -----------------------------------------------------------------------------
# Global exception handler
# -----------------------------------------------------------------------------
@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled error on %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=500,
        content={
            "ok": False,
            "error": "internal_server_error",
            "message": "An unexpected server error occurred.",
            "path": str(request.url.path),
            "timestamp": utc_now_iso(),
        },
    )

# -----------------------------------------------------------------------------
# Dependency: Database Session
# -----------------------------------------------------------------------------
async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()

# -----------------------------------------------------------------------------
# Root & System Endpoints
# -----------------------------------------------------------------------------
@app.get("/", tags=["system"])
async def root():
    return {
        "ok": True,
        "name": APP_NAME,
        "version": APP_VERSION,
        "environment": APP_ENV,
        "message": "Opsyn backend is live",
        "timestamp": utc_now_iso(),
        "database": "connected" if DATABASE_URL else "not_configured",
    }

@app.get("/health", tags=["system"])
async def health():
    return {
        "ok": True,
        "status": "healthy",
        "timestamp": utc_now_iso(),
        "database_connected": bool(DATABASE_URL),
    }

@app.get("/ready", tags=["system"])
async def ready():
    return {
        "ok": True,
        "status": "ready",
        "timestamp": utc_now_iso(),
    }

# -----------------------------------------------------------------------------
# API Routers
# -----------------------------------------------------------------------------
api = APIRouter(prefix="/api", tags=["api"])
v1 = APIRouter(prefix="/v1", tags=["api-v1"])

@api.get("")
async def api_root():
    return {"ok": True, "message": "Opsyn API root", "version": APP_VERSION, "timestamp": utc_now_iso()}

@v1.get("")
async def v1_root():
    return {"ok": True, "message": "Opsyn API v1", "timestamp": utc_now_iso()}

# Example placeholder endpoints
@v1.get("/status")
async def status():
    return {
        "ok": True,
        "app": APP_NAME,
        "environment": APP_ENV,
        "version": APP_VERSION,
        "timestamp": utc_now_iso(),
    }

# -----------------------------------------------------------------------------
# Register routers
# -----------------------------------------------------------------------------
api.include_router(v1)
app.include_router(api)

# -----------------------------------------------------------------------------
# Run (for local development only)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=PORT,
        log_level="info",
        reload=True if APP_ENV == "local" else False,
    )