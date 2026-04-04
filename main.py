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
import re
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
    keywords = [
        "product", "item", "sku", "quantity", "qty",
        "units", "customer", "price", "cost"
    ]
    for idx, row in enumerate(rows[:10]):
        joined = " ".join(normalize_cell(c).lower() for c in row)
        if any(k in joined for k in keywords):
            return idx
    return 0

def clean_key(header, index):
    key = normalize_cell(header).lower()
    key = re.sub(r"[^a-z0-9]+", "_", key).strip("_")
    if not key:
        key = f"column_{index + 1}"
    return key

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
            key = clean_key(header, i)
            item[key] = values[i] if i < len(values) else ""

        items.append(item)

    return {"headers": headers, "items": items}

def find_best_key(keys, candidates):
    for candidate in candidates:
        for key in keys:
            if key == candidate:
                return key
    for candidate in candidates:
        for key in keys:
            if candidate in key:
                return key
    return None

def parse_number(value):
    text_value = normalize_cell(value).replace(",", "").replace("$", "")
    if not text_value:
        return None
    try:
        if "." in text_value:
            return float(text_value)
        return int(text_value)
    except Exception:
        return None

def normalize_order_items(items):
    normalized = []

    for row in items:
        keys = list(row.keys())

        product_key = find_best_key(
            keys,
            ["product", "item", "product_name", "item_name", "sku", "description"]
        )
        qty_key = find_best_key(
            keys,
            ["quantity", "qty", "units", "unit_qty", "count"]
        )
        price_key = find_best_key(
            keys,
            ["price", "unit_price", "cost", "rate", "amount"]
        )
        customer_key = find_best_key(
            keys,
            ["customer", "customer_name", "account", "store", "client"]
        )

        product_name = row.get(product_key, "") if product_key else ""
        quantity = parse_number(row.get(qty_key, "")) if qty_key else None
        price = parse_number(row.get(price_key, "")) if price_key else None
        customer_name = row.get(customer_key, "") if customer_key else ""

        if not product_name and quantity is None and price is None:
            continue

        normalized.append({
            "product_name": product_name,
            "quantity": quantity,
            "unit_price": price,
            "customer_name": customer_name,
            "raw_row": row,
        })

    return normalized

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
# EXCEL ORDER UPLOAD + NORMALIZATION
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
        normalized_items = normalize_order_items(parsed["items"])

        customer_name = "Parsed Excel Order"
        for item in normalized_items:
            if item.get("customer_name"):
                customer_name = item["customer_name"]
                break

        db = SessionLocal()
        try:
            new_order = UploadedOrder(
                customer_name=customer_name,
                source_filename=file.filename,
                status="normalized",
                raw_text=str(normalized_items[:50]),
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
                "normalized_count": len(normalized_items),
                "normalized_preview": normalized_items[:10],
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