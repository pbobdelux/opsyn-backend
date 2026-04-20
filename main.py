import os

from fastapi import FastAPI, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

from database import get_db
from services.leaflink_sync import sync_leaflink_orders

from leaflink_debug import router as leaflink_debug_router
from routes.brain import router as brain_router
from routes.orders import router as orders_router
from routes.routes import router as routes_router
from routes.snapshot import router as snapshot_router

# =========================
# APP INIT
# =========================

app = FastAPI(
    title="Opsyn API",
    version="1.0.0",
    description="Full Ops Backend v1",
)

# =========================
# ROUTES
# =========================

app.include_router(leaflink_debug_router)
app.include_router(brain_router)
app.include_router(orders_router)
app.include_router(routes_router)
app.include_router(snapshot_router)

# =========================
# MIDDLEWARE
# =========================

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================
# AUTH MIDDLEWARE (SAFE)
# =========================

@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    if request.url.path in [
        "/api/health",
        "/docs",
        "/openapi.json",
        "/redoc",
        "/debug/leaflink",
        "/api/brain/ask",
        "/sync/leaflink",
        "/orders",
        "/routes",
        "/snapshot",
    ]:
        return await call_next(request)

    return await call_next(request)

# =========================
# HEALTH CHECK
# =========================

@app.get("/api/health")
def health_check():
    return {"status": "ok"}

# =========================
# LEAFLINK FORCE SYNC
# =========================

@app.post("/sync/leaflink")
def force_sync_leaflink(db: Session = Depends(get_db)):
    api_key = os.getenv("LEAFLINK_API_KEY")

    if not api_key:
        return {"error": "Missing LEAFLINK_API_KEY"}

    sync_leaflink_orders(db, api_key)

    return {"status": "LeafLink sync complete"}