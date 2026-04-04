from fastapi import FastAPI, Request, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from models import *
from config import config

import os
from io import BytesIO
import re
from difflib import SequenceMatcher

import openpyxl
from sqlalchemy import create_engine, text, select
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
    seed_product_catalog()

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
        "/catalog",
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

def simplify_name(name: str) -> str:
    name = name.lower().strip()
    name = re.sub(r"[^a-z0-9]+", " ", name)
    return re.sub(r"\s+", " ", name).strip()

def match_product_name(product_name: str, catalog_rows: list[ProductCatalog]):
    incoming = simplify_name(product_name)
    if not incoming:
        return {
            "matched": False,
            "catalog_id": None,
            "sku": None,
            "catalog_product_name": None,
            "match_score": 0.0,
        }

    best = None
    best_score = 0.0

    for row in catalog_rows:
        candidate = simplify_name(row.product_name)
        score = SequenceMatcher(None, incoming, candidate).ratio()

        if incoming == candidate:
            score = 1.0

        if row.sku and simplify_name(row.sku) == incoming:
            score = 1.0

        if score > best_score:
            best_score = score
            best = row

    if best and best_score >= 0.72:
        return {
            "matched": True,
            "catalog_id": best.id,
            "sku": best.sku,
            "catalog_product_name": best.product_name,
            "match_score": round(best_score, 3),
        }

    return {
        "matched": False,
        "catalog_id": None,
        "sku": None,
        "catalog_product_name": None,
        "match_score": round(best_score, 3),
    }

def seed_product_catalog():
    db = SessionLocal()
    try:
        existing = db.execute(select(ProductCatalog)).scalars().first()
        if existing:
            return

        seed_rows = [
            ProductCatalog(sku="JEFE-2G-DISP", product_name="Jefe 2G Disposable", brand="Jefe", unit_price=13.0),
            ProductCatalog(sku="JEFE-5G-DISP", product_name="Jefe 5G Disposable", brand="Jefe", unit_price=25.0),
            ProductCatalog(sku="NN-1G-CART", product_name="Noble Nectar 1G Cart", brand="Noble Nectar", unit_price=10.0),
            ProductCatalog(sku="NN-2G-CART", product_name="Noble Nectar 2G Cart", brand="Noble Nectar", unit_price=13.0),
            ProductCatalog(sku="NN-LR-DISP", product_name="Noble Nectar Live Resin Disposable", brand="Noble Nectar", unit_price=18.0),
        ]
        db.add_all(seed_rows)
        db.commit()
    finally:
        db.close()

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
# PRODUCT CATALOG
# =========================

@app.get("/catalog")
def get_catalog():
    db = SessionLocal()
    try:
        rows = db.execute(select(ProductCatalog).order_by(ProductCatalog.product_name)).scalars().all()
        return {
            "count": len(rows),
            "items": [
                {
                    "id": row.id,
                    "sku": row.sku,
                    "product_name": row.product_name,
                    "brand": row.brand,
                    "unit_price": row.unit_price,
                    "active": row.active,
                }
                for row in rows
            ],
        }
    finally:
        db.close()

# =========================
# EXCEL ORDER UPLOAD + MATCHING
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

        db = SessionLocal()
        try:
            catalog_rows = db.execute(select(ProductCatalog).where(ProductCatalog.active == "yes")).scalars().all()

            matched_items = []
            matched_count = 0
            unmatched_count = 0

            for item in normalized_items:
                match = match_product_name(item.get("product_name", ""), catalog_rows)

                combined = {
                    "product_name": item.get("product_name"),
                    "quantity": item.get("quantity"),
                    "unit_price": item.get("unit_price"),
                    "customer_name": item.get("customer_name"),
                    "matched": match["matched"],
                    "catalog_id": match["catalog_id"],
                    "matched_sku": match["sku"],
                    "matched_product_name": match["catalog_product_name"],
                    "match_score": match["match_score"],
                    "raw_row": item.get("raw_row"),
                }

                if match["matched"]:
                    matched_count += 1
                    if combined["unit_price"] is None:
                        catalog_hit = next((c for c in catalog_rows if c.id == match["catalog_id"]), None)
                        if catalog_hit:
                            combined["unit_price"] = catalog_hit.unit_price
                else:
                    unmatched_count += 1

                matched_items.append(combined)

            customer_name = "Parsed Excel Order"
            for item in matched_items:
                if item.get("customer_name"):
                    customer_name = item["customer_name"]
                    break

            new_order = UploadedOrder(
                customer_name=customer_name,
                source_filename=file.filename,
                status="matched",
                raw_text=str(matched_items[:50]),
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
                "matched_count": matched_count,
                "unmatched_count": unmatched_count,
                "matched_preview": matched_items[:10],
            }
        finally:
            db.close()

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
        }