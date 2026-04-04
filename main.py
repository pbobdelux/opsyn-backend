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
from io import BytesIO
import openpyxl

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

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
            content={"error": "Invalid or missing API key"},
        )

    return await call_next(request)

# =========================
# HELPERS
# =========================

def normalize_cell(value):
    if value is None:
        return ""
    return str(value).strip()

def row_is_empty(row):
    return all(not normalize_cell(cell) for cell in row)

def detect_header_row(rows):
    keywords = ["product", "item", "sku", "quantity", "qty", "units", "customer"]
    for idx, row in enumerate(rows[:10]):
        joined = " ".join(normalize_cell(c).lower() for c in row)
        if any(k in joined for k in keywords):
            return idx
    return 0

def parse_order_rows(rows):
    if not rows:
        return {"headers": [], "items": []}

    header_idx = detect_header_row(rows)
    headers = [normalize_cell(c) for c in rows[header_idx]]

    items = []
    for raw_row in rows[header_idx + 1:]:
        if row_is_empty(raw_row):
            continue

        values = [normalize_cell(c) for c in raw_row]
        item = {}

        for i, header in enumerate(headers):
            key = header.lower().strip().replace(" ", "_")
            if not key:
                key = f"column_{i+1}"
            item[key] = values[i] if i < len(values) else ""

        items.append(item)

    return {"headers": headers, "items": items}

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
            return {"connected": True, "result": [row[0] for row in result]}
    except Exception as e:
        return {"error": str(e)}

# =========================
# EXCEL ORDER UPLOAD + PARSE
# =========================

@app.post("/upload-order")
async def upload_order(file: UploadFile = File(...)):
    contents = await file.read()

    try:
        workbook = openpyxl.load_workbook(BytesIO(contents), data_only=True)
        sheet = workbook.active

        rows = []
        for row in sheet.iter_rows(values_only=True):
            rows.append(list(row))

        parsed = parse_order_rows(rows)

        db = SessionLocal()
        try:
            new_order = UploadedOrder(
                customer_name="Parsed Excel Order",
                source_filename=file.filename,
                status="parsed",
                raw_text=str(parsed["items"][:50]),
            )
            db.add(new_order)
            db.commit()
            db.refresh(new_order)

            return {
                "success": True,
                "order_id": new_order.id,
                "filename": file.filename,
                "headers": parsed["headers"],
                "items_detected": len(parsed["items"]),
                "preview_items": parsed["items"][:10],
            }
        finally:
            db.close()

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
        }

# =========================
# AI CHAT
# =========================

@app.post("/api/ai/chat")
def ai_chat(req: ChatRequest):
    screen = req.context.screen if req.context else "dashboard"
    msg = req.message.lower()

    if "testing" in msg or "test" in msg:
        ai_msg = "You have packages that may need testing. Start workflow?"
        actions = [NextAction(action="start_testing", label="Start Testing")]
    elif "route" in msg:
        ai_msg = "You have deliveries ready. Optimize routes?"
        actions = [NextAction(action="optimize_routes", label="Optimize Routes")]
    else:
        ai_msg = "I can help with testing, routing, and inventory."
        actions = []

    return {
        "action_type": "ai_chat",
        "status": "completed",
        "result_summary": {"message": ai_msg},
        "next_actions": [a.model_dump() for a in actions],
        "context": {"screen": screen},
    }

# =========================
# BASIC TESTING WORKFLOW
# =========================

@app.post("/api/workflows/testing/start")
def testing_start(req: TestingStartRequest):
    session = store.create("testing", {"scanned_tags": req.scanned_tags})
    return {
        "action_type": "testing_workflow",
        "status": "completed",
        "result_summary": {"tags": req.scanned_tags},
        "context": {
            "session_id": session.id,
            "workflow_type": session.workflow_type,
        },
    }

# =========================
# DOCUMENTS
# =========================

@app.get("/api/documents/{doc_id}/download")
def document_download(doc_id: str):
    return {
        "document_id": doc_id,
        "url": f"https://example.com/{doc_id}.pdf",
    }

@app.get("/api/documents/session/{session_id}")
def session_documents(session_id: str):
    return {
        "documents": [],
        "session_id": session_id,
    }