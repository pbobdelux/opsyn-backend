from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

# =========================
# ROUTE IMPORTS
# =========================

from leaflink_debug import router as leaflink_debug_router
from routes.brain import router as brain_router

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

# =========================
# MIDDLEWARE
# =========================

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================
# AUTH / REQUEST MIDDLEWARE
# =========================

@app.middleware("http")
async def auth_middleware(request: Request, call_next):

    # ✅ SAFE ALLOWLIST (NO BLOCKING)
    if request.url.path in [
        "/api/health",
        "/docs",
        "/openapi.json",
        "/redoc",
        "/debug/leaflink",
        "/api/brain/ask",
    ]:
        return await call_next(request)

    # 👉 Add real auth logic here later if needed

    return await call_next(request)

# =========================
# HEALTH CHECK
# =========================

@app.get("/api/health")
def health_check():
    return {"status": "ok"}