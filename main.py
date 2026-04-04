from fastapi import FastAPI, HTTPException, Request, BackgroundTasks, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from models import *
from config import config
from sessions import store
from workflows import (
    plan_testing_session,
    execute_testing_session,
    plan_routing,
    execute_routing,
    plan_audit,
    analyze_inventory,
    get_mock_package,
)

import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import openpyxl
from io import BytesIO

# =========================
# DATABASE
# =========================

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    connect_args={
        "sslmode": "require",
        "connect_timeout": 10,
    },
)

SessionLocal = sessionmaker(bind=engine)

# =========================
# APP
# =========================

app = FastAPI(
    title="Opsyn API",
    version="1.0.0",
    description="METRC Ops Hub Backend",
)

@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)

# =========================
# MIDDLEWARE
# =========================

app.add_middleware(
    CORSMiddleware,
    allow_origins=config.CORS_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    if request.url.path in [
        "/api/health",
        "/docs",
        "/openapi.json",
        "/redoc",
        "/test-db",
        "/upload-order",
    ]:
        return await call_next(request)

    api_key = request.headers.get("X-API-Key")
    if not api_key or api_key != config.API_KEY:
        return JSONResponse(
            status_code=401,
            content={
                "error": "Invalid or missing API key"
            },
        )

    return await call_next(request)

# =========================
# HEALTH
# =========================

@app.get("/api/health")
def health():
    return {"status": "ok"}

# =========================
# DB TEST
# =========================

@app.get("/test-db")
def test_db():
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            return {"connected": True}
    except Exception as e:
        return {"error": str(e)}

# =========================
# EXCEL UPLOAD (REAL FEATURE)
# =========================

@app.post("/upload-order")
async def upload_order(file: UploadFile = File(...)):
    contents = await file.read()

    try:
        workbook = openpyxl.load_workbook(BytesIO(contents))
        sheet = workbook.active

        rows = []
        for row in sheet.iter_rows(values_only=True):
            rows.append([str(cell) if cell else "" for cell in row])

        preview = rows[:10]

        db = SessionLocal()
        try:
            new_order = UploadedOrder(
                customer_name="Parsed Excel Order",
                source_filename=file.filename,
                status="uploaded",
                raw_text=str(rows[:50]),
            )
            db.add(new_order)
            db.commit()
            db.refresh(new_order)

            return {
                "success": True,
                "order_id": new_order.id,
                "rows_detected": len(rows),
                "preview": preview,
            }
        finally:
            db.close()

    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }