from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
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
    description="METRC Ops Hub Backend - Twin/Rork Contract",
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
        "/test-create-order",
    ]:
        return await call_next(request)

    api_key = request.headers.get("X-API-Key")
    if not api_key or api_key != config.API_KEY:
        return JSONResponse(
            status_code=401,
            content={
                "action_type": "auth",
                "status": "failed",
                "error": {
                    "code": "UNAUTHORIZED",
                    "message": "Invalid or missing X-API-Key",
                },
            },
        )

    return await call_next(request)

# =========================
# HELPERS
# =========================

def resp(action_type, status, **kwargs):
    return WorkflowResponse(
        action_type=action_type,
        status=status,
        **kwargs,
    ).dict()

def ctx(session):
    return {
        "session_id": session.id,
        "workflow_type": session.workflow_type,
    }

# =========================
# HEALTH
# =========================

@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "version": "1.0.0",
        "service": "opsyn",
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

    return resp(
        "ai_chat",
        WorkflowStatus.completed,
        result_summary={"message": ai_msg},
        next_actions=actions,
        context={"screen": screen},
    )

# =========================
# TEST DB
# =========================

@app.get("/test-db")
def test_db():
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            return {"database": "connected", "result": [row[0] for row in result]}
    except Exception as e:
        return {"error": str(e)}

# =========================
# CREATE ORDER TEST
# =========================

@app.post("/test-create-order")
def test_create_order():
    db = SessionLocal()
    try:
        new_order = UploadedOrder(
            customer_name="Test Customer",
            source_filename="test.xlsx",
            status="uploaded",
            raw_text="Test order",
        )
        db.add(new_order)
        db.commit()
        db.refresh(new_order)

        return {
            "success": True,
            "order_id": new_order.id,
        }
    finally:
        db.close()

# =========================
# BASIC WORKFLOW (TESTING)
# =========================

@app.post("/api/workflows/testing/start")
def testing_start(req: TestingStartRequest):
    session = store.create("testing", {"scanned_tags": req.scanned_tags})
    return resp(
        "testing_workflow",
        WorkflowStatus.completed,
        result_summary={"tags": req.scanned_tags},
        context=ctx(session),
    )

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