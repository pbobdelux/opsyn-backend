# =========================
# OPSYN BACKEND — MINIMAL SAFE MODE
# =========================

from fastapi import FastAPI

# Initialize app
app = FastAPI(
    title="Opsyn API (Safe Mode)",
    version="1.0.0",
    description="Minimal test server to verify Railway deployment",
)

# =========================
# ROOT
# =========================

@app.get("/")
def root():
    return {
        "status": "ok",
        "message": "Opsyn backend is running"
    }

# =========================
# HEALTH CHECK
# =========================

@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "service": "opsyn-backend"
    }

# =========================
# TEST ENDPOINTS
# =========================

@app.get("/orders")
def orders():
    return {
        "status": "ok",
        "endpoint": "orders",
        "message": "Orders endpoint working"
    }

@app.get("/routes")
def routes():
    return {
        "status": "ok",
        "endpoint": "routes",
        "message": "Routes endpoint working"
    }

@app.get("/snapshot")
def snapshot():
    return {
        "status": "ok",
        "endpoint": "snapshot",
        "message": "Snapshot endpoint working"
    }