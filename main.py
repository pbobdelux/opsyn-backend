from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

# ✅ LeafLink Debug Import
from leaflink_debug import router as leaflink_debug_router

# =========================
# APP INIT
# =========================

app = FastAPI(
    title="Opsyn API",
    version="1.0.0",
    description="Full Ops Backend v1",
)

# ✅ Attach LeafLink debug route
app.include_router(leaflink_debug_router)

# =========================
# MIDDLEWARE
# =========================

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def auth_middleware(request: Request, call_next):

    # ✅ SAFE ALLOWLIST (NO BLOCKING)
    if request.url.path in [
        "/api/health",
        "/docs",
        "/openapi.json",
        "/redoc",
        "/debug/leaflink",
    ]:
        return await call_next(request)

    # 👉 KEEP YOUR EXISTING AUTH LOGIC BELOW THIS LINE
    # (if you had token checks, they go here)

    return await call_next(request)