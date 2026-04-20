# =========================
# OPSYN BACKEND — CLEAN BASELINE
# =========================

from fastapi import FastAPI

app = FastAPI(
    title="Opsyn Backend",
    version="1.0.0",
)

# Root endpoint
@app.get("/")
def root():
    return {
        "status": "ok",
        "message": "Opsyn backend is live"
    }

# Health check (this is the only thing you need working right now)
@app.get("/api/health")
def health():
    return {
        "status": "ok"
    }