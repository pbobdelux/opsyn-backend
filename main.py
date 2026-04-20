from fastapi import FastAPI, APIRouter, Request
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
# Helpers
# -----------------------------------------------------------------------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

# -----------------------------------------------------------------------------
# Lifespan (safe)
# -----------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Backend starting")
    yield
    logger.info("Backend shutting down")

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
    logger.exception("Unhandled error")
    return JSONResponse(
        status_code=500,
        content={"ok": False, "error": str(exc)},
    )

# -----------------------------------------------------------------------------
# System endpoints
# -----------------------------------------------------------------------------
@app.get("/")
async def root():
    return {"ok": True, "message": "backend live"}

@app.get("/health")
async def health():
    return {"ok": True}

@app.get("/ping")
async def ping():
    return {"ok": True}

# -----------------------------------------------------------------------------
# 🔥 ADD ORDERS ROUTE (THIS IS THE FIX)
# -----------------------------------------------------------------------------
@app.get("/orders")
async def get_orders():
    return {
        "ok": True,
        "orders": [
            {
                "id": 1,
                "customer": "Test Dispensary",
                "total": 420,
                "status": "delivered",
            },
            {
                "id": 2,
                "customer": "Sample Shop",
                "total": 1337,
                "status": "pending",
            },
        ],
    }

# -----------------------------------------------------------------------------
# API routers
# -----------------------------------------------------------------------------
api = APIRouter(prefix="/api")
v1 = APIRouter(prefix="/v1")

@v1.get("")
async def api_v1_root():
    return {"ok": True, "message": "api v1"}

api.include_router(v1)
app.include_router(api)

# -----------------------------------------------------------------------------
# Run
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=PORT)