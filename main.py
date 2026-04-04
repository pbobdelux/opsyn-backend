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

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    connect_args={
        "sslmode": "require",
        "connect_timeout": 10,
    },
)
app = FastAPI(
    title="Opsyn API",
    version="1.0.0",
    description="METRC Ops Hub Backend - Twin/Rork Contract",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=config.CORS_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    if request.url.path in ["/api/health", "/docs", "/openapi.json", "/redoc", "/test-db"]:
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


@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "version": "1.0.0",
        "service": "opsyn",
    }


@app.post("/api/ai/chat")
def ai_chat(req: ChatRequest):
    screen = req.context.screen if req.context else "dashboard"
    msg = req.message.lower()

    if "testing" in msg or "test" in msg:
        ai_msg = (
            "You have 5 active packages that may need testing. "
            "3 concentrates and 2 vapes. Want me to start a testing workflow?"
        )
        actions = [NextAction(action="start_testing", label="Start Testing Workflow")]
    elif "package" in msg or "inventory" in msg:
        ai_msg = (
            "Current inventory: 5 active packages totaling 250 units across vapes, "
            "concentrates, and edibles. All in compliance."
        )
        actions = [NextAction(action="view_inventory", label="View Full Inventory")]
    elif "route" in msg or "delivery" in msg:
        ai_msg = (
            "15 open orders worth $24K awaiting delivery. "
            "9 Mango orders on production hold until Apr 8. "
            "6 orders ready for routing."
        )
        actions = [NextAction(action="optimize_routes", label="Optimize Routes")]
    else:
        ai_msg = (
            "I can help with testing submissions, inventory checks, "
            "route optimization, and compliance audits. What would you like to do?"
        )
        actions = [
            NextAction(action="start_testing", label="Testing"),
            NextAction(action="optimize_routes", label="Routes"),
            NextAction(action="start_audit", label="Audit"),
        ]

    return resp(
        "ai_chat",
        WorkflowStatus.completed,
        result_summary={"message": ai_msg},
        next_actions=actions,
        context={"screen": screen, "conversation_id": req.conversation_id},
    )


@app.post("/api/ai/command")
def ai_command(req: CommandRequest):
    msg = req.message.lower()
    items = req.context.selected_items if req.context and req.context.selected_items else []

    if "test" in msg and items:
        session = store.create("testing", {"scanned_tags": items})
        session.answers = {
            "q_lab": "Scissortail Labs",
            "q_same_day": True,
            "q_driver": "Preston Anderson",
        }
        plan_testing_session(session)

        return resp(
            "testing_workflow",
            WorkflowStatus.plan_ready,
            requires_confirmation=True,
            plan_summary=(
                f"Submit {len(items)} packages to Scissortail Labs. "
                f"{len(items)} parent, {len(items)} test, {len(items)} reserve samples."
            ),
            steps=[Step(**s) for s in session.steps],
            warnings=[Warning(**w) for w in session.warnings],
            context=ctx(session),
        )

    elif "route" in msg or "optimize" in msg:
        session = store.create("routing", {"date": "2026-04-04"})
        plan_routing(session)

        return resp(
            "route_optimization",
            WorkflowStatus.plan_ready,
            requires_confirmation=True,
            plan_summary=session.data.get("plan_summary", "3 routes optimized"),
            steps=[Step(**s) for s in session.steps],
            warnings=[Warning(**w) for w in session.warnings],
            context=ctx(session),
        )

    return resp(
        "ai_command",
        WorkflowStatus.completed,
        result_summary={
            "message": (
                f"Command received: {req.message}. "
                "Use specific intents like 'submit for testing' or 'optimize routes'."
            )
        },
    )


@app.post("/api/workflows/testing/start")
def testing_start(req: TestingStartRequest):
    session = store.create("testing", {"scanned_tags": req.scanned_tags})
    warnings = []

    for tag in req.scanned_tags:
        pkg = get_mock_package(tag)
        if pkg["qty"] < 10:
            warnings.append(
                Warning(
                    type="low_inventory",
                    message=f"Package {tag} has only {pkg['qty']} {pkg['uom']}",
                    severity=Severity.warning,
                    affected_item_ids=[tag],
                )
            )

    session.status = "questions_needed"
    session.questions = [
        {
            "id": "q_lab",
            "text": "Which lab?",
            "type": "select",
            "options": ["Scissortail Labs", "Other"],
            "default": "Scissortail Labs",
        },
        {
            "id": "q_same_day",
            "text": "Same-day transfer?",
            "type": "boolean",
            "default": True,
        },
        {
            "id": "q_driver",
            "text": "Driver name?",
            "type": "text",
            "default": "Preston Anderson",
        },
    ]

    return resp(
        "testing_workflow",
        WorkflowStatus.questions_needed,
        requires_questions=True,
        questions=[Question(**q) for q in session.questions],
        plan_summary=(
            f"{len(req.scanned_tags)} packages found. "
            "Answer questions to generate split plan."
        ),
        warnings=warnings,
        context=ctx(session),
    )


@app.post("/api/workflows/testing/answer")
def testing_answer(req: TestingAnswerRequest):
    session = store.get(req.session_id)
    if not session:
        raise HTTPException(404, "Session not found")

    session.answers = req.answers
    plan_testing_session(session)

    return resp(
        "testing_workflow",
        WorkflowStatus.plan_ready,
        requires_confirmation=True,
        plan_summary=(
            f"Create {len(session.data['scanned_tags'])} parent tags, "
            f"test + reserve samples. Generate chain of custody for "
            f"{session.data.get('lab', 'lab')}."
        ),
        steps=[Step(**s) for s in session.steps],
        warnings=[Warning(**w) for w in session.warnings],
        context=ctx(session),
    )


@app.post("/api/workflows/testing/confirm")
def testing_confirm(req: ConfirmRequest, bg: BackgroundTasks):
    session = store.get(req.session_id)
    if not session:
        raise HTTPException(404, "Session not found")

    session.status = "executing"
    bg.add_task(execute_testing_session, req.session_id)

    return resp(
        "testing_workflow",
        WorkflowStatus.executing,
        context=ctx(session),
    )


@app.get("/api/workflows/testing/{session_id}/status")
def testing_status(session_id: str):
    session = store.get(session_id)
    if not session:
        raise HTTPException(404, "Session not found")

    desc = (
        session.steps[session.current_step - 1]["action"]
        if session.current_step > 0 and session.current_step <= len(session.steps)
        else ""
    )
    pct = int((session.current_step / max(session.total_steps, 1)) * 100)

    return ProgressResponse(
        session_id=session_id,
        status=session.status,
        current_step=session.current_step,
        total_steps=session.total_steps,
        step_description=desc,
        percent_complete=pct,
        estimated_remaining_seconds=max(
            0, (session.total_steps - session.current_step) * 1
        ),
    ).dict()


@app.get("/api/workflows/testing/{session_id}/result")
def testing_result(session_id: str):
    session = store.get(session_id)
    if not session:
        raise HTTPException(404, "Session not found")

    if session.status != "completed":
        return resp("testing_workflow", session.status, context=ctx(session))

    return resp(
        "testing_workflow",
        WorkflowStatus.completed,
        result_summary=session.result,
        steps=[Step(**s) for s in session.steps],
        context=ctx(session),
        next_actions=[
            NextAction(action="add_to_routing", label="Add Lab Drop-off to Route"),
            NextAction(action="view_transfer", label="View Transfer"),
        ],
    )


@app.post("/api/workflows/routing/optimize")
def routing_optimize(req: RoutingOptimizeRequest):
    session = store.create(
        "routing",
        {"date": req.date, "max_drivers": req.max_drivers},
    )
    plan_routing(session)

    return resp(
        "route_optimization",
        WorkflowStatus.plan_ready,
        requires_confirmation=True,
        plan_summary=(
            "3 routes for 14 stops. OKC Metro (5 stops, $9.7K), "
            "Tulsa Metro (6, $28.5K), Tulsa East (3, $9.6K)."
        ),
        steps=[Step(**s) for s in session.steps],
        warnings=[Warning(**w) for w in session.warnings],
        context=ctx(session),
    )


@app.post("/api/workflows/routing/add-stop")
def routing_add_stop(req: AddStopRequest):
    return resp(
        "stop_addition",
        WorkflowStatus.completed,
        result_summary={
            "message": f"Order {req.order_id} added to routing queue for {req.date or 'next available'}"
        },
    )


@app.post("/api/workflows/routing/confirm")
def routing_confirm(req: ConfirmRequest, bg: BackgroundTasks):
    session = store.get(req.session_id)
    if not session:
        raise HTTPException(404, "Session not found")

    bg.add_task(execute_routing, req.session_id)

    return resp(
        "route_optimization",
        WorkflowStatus.executing,
        context=ctx(session),
    )


@app.get("/api/workflows/routing/{session_id}/status")
def routing_status(session_id: str):
    session = store.get(session_id)
    if not session:
        raise HTTPException(404, "Session not found")

    pct = int((session.current_step / max(session.total_steps, 1)) * 100)

    return ProgressResponse(
        session_id=session_id,
        status=session.status,
        current_step=session.current_step,
        total_steps=session.total_steps,
        percent_complete=pct,
    ).dict()


@app.post("/api/workflows/audit/start")
def audit_start(req: AuditStartRequest):
    session = store.create(
        "audit",
        {
            "scanned_tags": req.scanned_tags,
            "expected_counts": req.expected_counts,
        },
    )
    plan_audit(session)

    mismatches = len(
        [s for s in session.steps if s.get("action") == "package_adjustment"]
    )

    return resp(
        "rapid_audit",
        WorkflowStatus.plan_ready,
        requires_confirmation=True if mismatches > 0 else False,
        plan_summary=(
            f"{mismatches} of {len(req.scanned_tags)} packages have discrepancies."
            if mismatches
            else f"All {len(req.scanned_tags)} packages match."
        ),
        steps=[Step(**s) for s in session.steps],
        warnings=[Warning(**w) for w in session.warnings],
        context=ctx(session),
    )


@app.post("/api/workflows/audit/confirm")
def audit_confirm(req: ConfirmRequest):
    session = store.get(req.session_id)
    if not session:
        raise HTTPException(404, "Session not found")

    for step in session.steps:
        step["status"] = "completed"

    session.status = "completed"
    session.result = {
        "adjustments_made": len(session.steps),
        "all_reconciled": True,
    }

    return resp(
        "rapid_audit",
        WorkflowStatus.completed,
        result_summary=session.result,
        context=ctx(session),
    )


@app.get("/api/workflows/audit/{session_id}/result")
def audit_result(session_id: str):
    session = store.get(session_id)
    if not session:
        raise HTTPException(404, "Session not found")

    return resp(
        "rapid_audit",
        session.status,
        result_summary=session.result,
        steps=[Step(**s) for s in session.steps],
        warnings=[Warning(**w) for w in session.warnings],
        context=ctx(session),
    )


@app.post("/api/workflows/inventory/analyze")
def inventory_analyze(req: InventoryAnalyzeRequest):
    result = analyze_inventory(req.tag)
    warnings = [Warning(**i) for i in result["issues"]]

    return resp(
        "inventory_analysis",
        WorkflowStatus.completed,
        result_summary={
            "package": result["package"],
            "recommendations": result["recommendations"],
        },
        warnings=warnings,
    )


@app.post("/api/workflows/inventory/fix")
def inventory_fix(req: InventoryFixRequest):
    session = store.get(req.session_id)
    if not session:
        raise HTTPException(404, "Session not found")

    return resp(
        "inventory_fix",
        WorkflowStatus.completed,
        requires_confirmation=True,
        plan_summary=f"Apply {len(req.fixes)} inventory fixes",
        result_summary={"fixes_applied": len(req.fixes)},
    )


@app.post("/api/scanning/validate")
def scan_validate(req: ScanValidateRequest):
    valid = len(req.tag) >= 14 and req.tag.startswith("1A4")
    pkg = get_mock_package(req.tag) if valid else None

    return resp(
        "scan_validate",
        WorkflowStatus.completed,
        result_summary={
            "tag": req.tag,
            "valid": valid,
            "format_ok": valid,
            "package": pkg,
            "message": f"Valid tag: {pkg['item']}" if valid and pkg else "Invalid tag format",
        },
    )


@app.post("/api/scanning/bulk-validate")
def scan_bulk_validate(req: BulkValidateRequest):
    results = []
    dupes = set()

    for tag in req.tags:
        is_dupe = tag in dupes
        dupes.add(tag)
        valid = len(tag) >= 14 and tag.startswith("1A4")
        pkg = get_mock_package(tag) if valid else None
        results.append(
            {
                "tag": tag,
                "valid": valid and not is_dupe,
                "duplicate": is_dupe,
                "package": pkg,
            }
        )

    valid_count = sum(1 for r in results if r["valid"])

    return resp(
        "bulk_validate",
        WorkflowStatus.completed,
        result_summary={
            "total": len(req.tags),
            "valid": valid_count,
            "invalid": len(req.tags) - valid_count,
            "results": results,
        },
        warnings=[
            Warning(
                type="tag_duplicate",
                message="Duplicate tag detected",
                severity=Severity.warning,
            )
        ]
        if any(r["duplicate"] for r in results)
        else [],
    )


@app.get("/api/scanning/queue/{session_id}")
def scan_queue(session_id: str):
    session = store.get(session_id)
    if not session:
        raise HTTPException(404, "Session not found")

    tags = session.data.get("scanned_tags", [])
    items = [
        {
            "tag": t,
            "package": get_mock_package(t),
            "status": "validated",
        }
        for t in tags
    ]

    return resp(
        "scan_queue",
        WorkflowStatus.completed,
        result_summary={"items": items, "count": len(items)},
        context=ctx(session),
    )


@app.get("/api/documents/{doc_id}/download")
def document_download(doc_id: str):
    return {
        "document_id": doc_id,
        "url": f"https://s3.amazonaws.com/opsyn-docs/{doc_id}.pdf",
        "message": "Mock PDF URL - replace with S3 in production",
    }


@app.get("/api/documents/session/{session_id}")
def session_documents(session_id: str):
    session = store.get(session_id)
    if not session:
        raise HTTPException(404, "Session not found")

    return {
        "documents": session.documents or [],
        "session_id": session_id,
    }
@app.get("/test-db")
def test_db():
    try:
        with engine.connect() as conn:
            result = conn.exec_driver_sql("SELECT 1")
            return {"database": "connected", "result": [row[0] for row in result]}
    except Exception as e:
        return {"error": str(e)}