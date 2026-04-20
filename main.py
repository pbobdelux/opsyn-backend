from fastapi import FastAPI, APIRouter, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import os
import logging
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

app = FastAPI(
    title=APP_NAME,
    version=APP_VERSION,
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
            "message": str(exc),
            "path": request.url.path,
            "timestamp": utc_now_iso(),
        },
    )

# -----------------------------------------------------------------------------
# Root & debug endpoints
# -----------------------------------------------------------------------------
@app.get("/", tags=["system"])
async def root():
    return {
        "ok": True,
        "name": APP_NAME,
        "version": APP_VERSION,
        "environment": APP_ENV,
        "message": "Opsyn backend boot-safe mode is live",
        "timestamp": utc_now_iso(),
    }

@app.get("/health", tags=["system"])
async def health():
    return {
        "ok": True,
        "status": "healthy",
        "timestamp": utc_now_iso(),
    }

@app.get("/ping", tags=["system"])
async def ping():
    return {
        "ok": True,
        "message": "pong",
        "timestamp": utc_now_iso(),
    }

@app.get("/env", tags=["system"])
async def env_check():
    return {
        "ok": True,
        "environment": APP_ENV,
        "port": PORT,
        "app_version": APP_VERSION,
        "database_url_present": bool(os.getenv("DATABASE_URL")),
        "timestamp": utc_now_iso(),
    }

@app.get("/debug/routes", tags=["system"])
async def debug_routes():
    return {
        "ok": True,
        "routes": [
            {
                "path": getattr(route, "path", None),
                "name": getattr(route, "name", None),
                "methods": sorted(list(getattr(route, "methods", []) or [])),
            }
            for route in app.routes
        ],
    }

# -----------------------------------------------------------------------------
# API routers
# -----------------------------------------------------------------------------
api = APIRouter(prefix="/api", tags=["api"])
v1 = APIRouter(prefix="/v1", tags=["api-v1"])

@api.get("")
async def api_root():
    return {
        "ok": True,
        "message": "Opsyn API root",
        "timestamp": utc_now_iso(),
    }

@v1.get("")
async def api_v1_root():
    return {
        "ok": True,
        "message": "Opsyn API v1",
        "timestamp": utc_now_iso(),
    }

api.include_router(v1)
app.include_router(api)

# -----------------------------------------------------------------------------
# Local run
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, log_level="info")