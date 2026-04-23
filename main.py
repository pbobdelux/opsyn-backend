from contextlib import asynccontextmanager
from datetime import datetime, timezone
import logging
import os
import traceback
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# =========================
# Logging
# =========================
logger = logging.getLogger("opsyn-backend")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

APP_NAME = "Opsyn Backend"
APP_ENV = os.getenv("ENVIRONMENT", "production")


# =========================
# Helpers
# =========================
def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


# =========================
# Lifespan
# =========================
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"Starting {APP_NAME} in {APP_ENV}")
    yield
    logger.info("Shutting down")


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
    return JSONResponse(
        status_code=500,
        content={"ok": False, "error": "internal_error"},
    )


# =========================
# Routes
# =========================
@app.get("/")
def root():
    return {
        "ok": True,
        "service": APP_NAME,
        "env": APP_ENV,
    }


@app.get("/health")
def health():
    return {
        "ok": True,
        "status": "healthy",
        "time": utc_now_iso(),
    }


@app.get("/orders")
def get_orders():
    return {
        "ok": True,
        "orders": [],
    }


@app.post("/sync/leaflink/run")
def sync_leaflink(x_opsyn_secret: Optional[str] = Header(None)):
    expected = os.getenv("OPSYN_SYNC_SECRET")

    if expected and x_opsyn_secret != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")

    return {
        "ok": True,
        "message": "Sync endpoint reachable",
    }


# =========================
# Run
# =========================
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8000)),
    )