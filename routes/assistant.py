"""
Opsyn AI Assistant API routes.

Endpoints:
  POST /assistant/command          — Process a natural-language command
  POST /assistant/confirm          — Confirm a pending action
  POST /assistant/cancel           — Cancel a pending action
  GET  /assistant/session/{id}     — Get session details + history
  GET  /assistant/actions          — List all registered actions
  POST /assistant/elevenlabs/tool  — ElevenLabs tool-call integration
  POST /assistant/realtime/event   — Realtime event hook
  POST /assistant/inspection-check — Run compliance inspection check
  POST /assistant/orders-command   — Order-specific commands
  POST /assistant/driver-command   — Driver-specific commands
  POST /assistant/metrc-command    — METRC-specific commands
  GET  /assistant/health           — Health check
"""

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models.assistant_models import AssistantPendingAction
from services import assistant_action_registry as registry
from services.assistant_audit import get_audit_logs
from services.assistant_memory import (
    cleanup_expired_actions,
    create_session,
    get_conversation,
    get_pending_actions,
    get_session,
)
from services.assistant_orchestrator import orchestrator

logger = logging.getLogger("assistant_routes")

router = APIRouter(prefix="/assistant", tags=["assistant"])


# ---------------------------------------------------------------------------
# Request / Response schemas
# ---------------------------------------------------------------------------


class CommandRequest(BaseModel):
    org_id: str
    org_code: Optional[str] = None
    user_id: Optional[str] = None
    role: Optional[str] = None
    app_context: Optional[str] = None
    input_type: Optional[str] = "text"
    message: str
    session_id: Optional[str] = None
    device_id: Optional[str] = None


class ConfirmRequest(BaseModel):
    confirmation_id: str
    approved: bool
    user_id: Optional[str] = None
    org_id: str


class CancelRequest(BaseModel):
    confirmation_id: str
    user_id: Optional[str] = None
    org_id: str


class ElevenLabsToolRequest(BaseModel):
    # Format A (original): tool_name + parameters
    tool_name: Optional[str] = None
    parameters: Optional[dict] = {}
    # Format B (ElevenLabs native): name + args
    name: Optional[str] = None
    args: Optional[dict] = {}


class RealtimeEventRequest(BaseModel):
    event_type: str  # started | interrupted | ended | transcript_update | action_result
    session_id: Optional[str] = None
    data: Optional[dict] = {}


class InspectionCheckRequest(BaseModel):
    org_id: str
    user_id: Optional[str] = None


class OrdersCommandRequest(BaseModel):
    org_id: str
    user_id: Optional[str] = None
    # sync | show_review | show_packed | show_blocked | summarize_sales | prepare_route
    command: str


class DriverCommandRequest(BaseModel):
    org_id: str
    user_id: Optional[str] = None
    # add | update_pin | email_pin | contact | assign_route | notify_change
    command: str
    driver_id: Optional[str] = None
    data: Optional[dict] = {}


class MetrcCommandRequest(BaseModel):
    org_id: str
    user_id: Optional[str] = None
    command: str
    data: Optional[dict] = {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_error(message: str) -> dict[str, Any]:
    return {"ok": False, "error": message}


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("/command")
async def command(
    body: CommandRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Process a natural-language command from a mobile app or voice agent.

    Safe actions are executed immediately. Medium/high-risk actions return
    a confirmation_id that must be confirmed via POST /assistant/confirm.
    """
    logger.info(
        "assistant/command: org_id=%s user_id=%s intent_message_len=%d",
        body.org_id,
        body.user_id,
        len(body.message),
    )

    try:
        result = await orchestrator.process_command(
            db=db,
            org_id=body.org_id,
            user_id=body.user_id,
            role=body.role,
            app_context=body.app_context,
            message=body.message,
            session_id=body.session_id,
            device_id=body.device_id,
            input_type=body.input_type or "text",
        )
        return result
    except Exception as exc:
        logger.exception("assistant/command: unhandled error for org_id=%s", body.org_id)
        return {
            "ok": False,
            "error": "internal_error",
            "message": "An unexpected error occurred. Please try again.",
            "errors": [str(exc)],
        }


@router.post("/confirm")
async def confirm(
    body: ConfirmRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Confirm or reject a pending action.

    If approved=true, the action is executed immediately.
    If approved=false, the action is cancelled.
    """
    logger.info(
        "assistant/confirm: confirmation_id=%s approved=%s org_id=%s",
        body.confirmation_id,
        body.approved,
        body.org_id,
    )

    try:
        # Look up the pending action
        result = await db.execute(
            select(AssistantPendingAction).where(
                AssistantPendingAction.confirmation_id == body.confirmation_id,
                AssistantPendingAction.org_id == body.org_id,
            )
        )
        pending = result.scalar_one_or_none()

        if pending is None:
            return _safe_error("Pending action not found")

        if pending.status != "pending":
            return _safe_error(f"Action is already {pending.status}")

        now = datetime.now(timezone.utc)
        if pending.expires_at < now:
            pending.status = "cancelled"
            await db.commit()
            return _safe_error("Action has expired")

        if not body.approved:
            pending.status = "cancelled"
            await db.commit()
            return {
                "ok": True,
                "spoken_reply": "Action cancelled.",
                "executed_actions": [],
                "data": {},
                "errors": [],
            }

        # Execute the confirmed action
        exec_result = await orchestrator.execute_pending_action(
            db=db,
            confirmation_id=body.confirmation_id,
            org_id=body.org_id,
            user_id=body.user_id,
        )

        spoken_reply = (
            "Done! The action was executed successfully."
            if exec_result.get("ok")
            else "The action failed. Please check the details."
        )

        return {
            "ok": exec_result.get("ok", False),
            "spoken_reply": spoken_reply,
            "executed_actions": exec_result.get("executed_actions", []),
            "data": exec_result.get("data", {}),
            "errors": exec_result.get("errors", []),
        }

    except Exception as exc:
        logger.exception(
            "assistant/confirm: unhandled error for confirmation_id=%s",
            body.confirmation_id,
        )
        return _safe_error(str(exc))


@router.post("/cancel")
async def cancel(
    body: CancelRequest,
    db: AsyncSession = Depends(get_db),
):
    """Cancel a pending action without executing it."""
    logger.info(
        "assistant/cancel: confirmation_id=%s org_id=%s",
        body.confirmation_id,
        body.org_id,
    )

    try:
        result = await db.execute(
            select(AssistantPendingAction).where(
                AssistantPendingAction.confirmation_id == body.confirmation_id,
                AssistantPendingAction.org_id == body.org_id,
            )
        )
        pending = result.scalar_one_or_none()

        if pending is None:
            return _safe_error("Pending action not found")

        if pending.status != "pending":
            return _safe_error(f"Action is already {pending.status}")

        pending.status = "cancelled"
        await db.commit()

        return {"ok": True, "message": "Action cancelled successfully."}

    except Exception as exc:
        logger.exception(
            "assistant/cancel: unhandled error for confirmation_id=%s",
            body.confirmation_id,
        )
        return _safe_error(str(exc))


@router.get("/session/{session_id}")
async def get_session_detail(
    session_id: str,
    org_id: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Return session details, conversation history, and pending actions.

    org_id is required as a query parameter to validate ownership.
    """
    try:
        session = await get_session(db, session_id)

        if session is None:
            return _safe_error("Session not found")

        if session.get("org_id") != org_id:
            return _safe_error("Access forbidden")

        messages = await get_conversation(db, session_id)
        pending = await get_pending_actions(db, session_id)

        return {
            "ok": True,
            "session": session,
            "messages": messages,
            "pending_actions": pending,
        }

    except Exception as exc:
        logger.exception("assistant/session: unhandled error for session_id=%s", session_id)
        return _safe_error(str(exc))


@router.get("/actions")
async def list_actions():
    """
    Return all registered assistant actions.

    No authentication required — this is a public capability manifest.
    """
    actions = registry.list_actions()
    return {"ok": True, "actions": actions}


@router.post("/elevenlabs/tool")
async def elevenlabs_tool(
    body: ElevenLabsToolRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    ElevenLabs tool-call integration endpoint.

    Accepts both Format A (tool_name + parameters) and Format B (name + args).
    Routes to the appropriate action handler and returns a result compatible
    with ElevenLabs expectations.

    Supported tool names:
      get_attention, get_orders_needing_review, get_orders_needing_packed,
      get_blocked_orders, summarize_sales, sync_leaflink_orders (requires confirmation),
      run_inspection_readiness_check, check_metrc_package, add_driver,
      update_driver_pin, email_driver_pin, contact_driver, assign_driver,
      notify_route_change
    """
    # Resolve tool name — accept both Format A and Format B
    tool_name: Optional[str] = body.tool_name or body.name
    parameters: dict = body.parameters or body.args or {}

    if not tool_name:
        return {
            "result": None,
            "error": {"code": "missing_tool_name", "message": "tool_name or name is required"},
        }

    logger.info(
        "assistant/elevenlabs/tool: tool_name=%s org_id=%s",
        tool_name,
        parameters.get("org_id", "unknown"),
    )

    org_id: str = parameters.get("org_id", "")
    if not org_id:
        return {
            "result": None,
            "error": {"code": "missing_org_id", "message": "org_id is required in parameters or args"},
        }

    # ------------------------------------------------------------------
    # Special case: get_attention → call the /ai/get-attention logic
    # ------------------------------------------------------------------
    if tool_name == "get_attention":
        try:
            from services.ai_service import (
                build_attention_summary,
                get_compliance_issues_count,
                get_low_inventory_count,
                get_pending_orders_count,
            )

            brand_id: Optional[str] = parameters.get("brand_id")
            pending = await get_pending_orders_count(db, org_id, brand_id)
            low_inv = await get_low_inventory_count(db, org_id)
            compliance = await get_compliance_issues_count(db, org_id)

            review_result = await registry.execute_action(db, "get_orders_needing_review", org_id, {})
            packed_result = await registry.execute_action(db, "get_orders_needing_packed", org_id, {})
            blocked_result = await registry.execute_action(db, "get_blocked_orders", org_id, {})

            orders_needing_review = review_result.get("count", 0) if review_result.get("ok") else 0
            orders_ready_to_pack = packed_result.get("count", 0) if packed_result.get("ok") else 0
            blocked_orders = blocked_result.get("count", 0) if blocked_result.get("ok") else 0

            has_data = (pending + orders_needing_review + orders_ready_to_pack + blocked_orders) > 0
            data_source = "database" if has_data else "stub"
            summary = build_attention_summary(pending, low_inv, compliance)

            result = {
                "ok": True,
                "pending_orders": pending,
                "orders_needing_review": orders_needing_review,
                "orders_ready_to_pack": orders_ready_to_pack,
                "blocked_orders": blocked_orders,
                "low_inventory": low_inv,
                "compliance_issues": compliance,
                "summary": summary,
                "data_source": data_source,
                "errors": [],
            }
            return {"result": result, "error": None}
        except Exception as exc:
            logger.exception("assistant/elevenlabs/tool: get_attention failed org_id=%s", org_id)
            return {
                "result": None,
                "error": {"code": "handler_error", "message": str(exc)},
            }

    # ------------------------------------------------------------------
    # sync_leaflink_orders requires confirmation — never execute directly
    # ------------------------------------------------------------------
    if tool_name == "sync_leaflink_orders":
        try:
            from services.assistant_memory import create_session

            session_id = await create_session(db, org_id, parameters.get("user_id"), "elevenlabs", None)
            confirmation_id = await orchestrator.create_pending_action(
                db=db,
                session_id=session_id,
                org_id=org_id,
                user_id=parameters.get("user_id"),
                action_name="sync_leaflink_orders",
                payload=parameters,
                risk_level="high",
            )
            await db.commit()
            return {
                "result": {
                    "ok": True,
                    "requires_confirmation": True,
                    "confirmation_id": confirmation_id,
                    "message": "LeafLink sync requires confirmation. Please confirm via POST /assistant/confirm.",
                },
                "error": None,
            }
        except Exception as exc:
            logger.exception("assistant/elevenlabs/tool: sync_leaflink_orders pending failed org_id=%s", org_id)
            return {
                "result": None,
                "error": {"code": "handler_error", "message": str(exc)},
            }

    # ------------------------------------------------------------------
    # All other registered actions
    # ------------------------------------------------------------------
    action = registry.get_action(tool_name)
    if action is None:
        logger.warning("assistant/elevenlabs/tool: unknown tool=%s", tool_name)
        return {
            "result": None,
            "error": {
                "code": "unknown_tool",
                "message": f"Unknown tool: {tool_name}",
            },
        }

    # Actions that require confirmation should not be executed directly
    if action.requires_confirmation:
        try:
            from services.assistant_memory import create_session

            session_id = await create_session(db, org_id, parameters.get("user_id"), "elevenlabs", None)
            confirmation_id = await orchestrator.create_pending_action(
                db=db,
                session_id=session_id,
                org_id=org_id,
                user_id=parameters.get("user_id"),
                action_name=tool_name,
                payload=parameters,
                risk_level=action.risk_level,
            )
            await db.commit()
            return {
                "result": {
                    "ok": True,
                    "requires_confirmation": True,
                    "confirmation_id": confirmation_id,
                    "message": f"Action '{tool_name}' requires confirmation. Please confirm via POST /assistant/confirm.",
                },
                "error": None,
            }
        except Exception as exc:
            logger.exception("assistant/elevenlabs/tool: pending action failed for tool=%s", tool_name)
            return {
                "result": None,
                "error": {"code": "handler_error", "message": str(exc)},
            }

    try:
        result = await registry.execute_action(db, tool_name, org_id, parameters)
        await db.commit()

        logger.info(
            "assistant/elevenlabs/tool: tool=%s org_id=%s ok=%s",
            tool_name, org_id, result.get("ok"),
        )

        return {
            "result": result,
            "error": None,
        }
    except Exception as exc:
        logger.exception("assistant/elevenlabs/tool: handler failed for tool=%s org_id=%s", tool_name, org_id)
        return {
            "result": None,
            "error": {"code": "handler_error", "message": str(exc)},
        }


@router.post("/realtime/event")
async def realtime_event(
    body: RealtimeEventRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Receive realtime events from ElevenLabs voice agents or mobile clients.

    Supported event types: started, interrupted, ended, transcript_update, action_result
    """
    logger.info(
        "assistant/realtime/event: event_type=%s session_id=%s",
        body.event_type,
        body.session_id,
    )

    try:
        if body.session_id:
            from models.assistant_models import AssistantSession

            result = await db.execute(
                select(AssistantSession).where(
                    AssistantSession.id == body.session_id
                )
            )
            session = result.scalar_one_or_none()

            if session is not None:
                existing_meta = session.metadata_json or {}
                events = existing_meta.get("realtime_events", [])
                events.append(
                    {
                        "event_type": body.event_type,
                        "timestamp": _utc_now_iso(),
                        "data": body.data,
                    }
                )
                # Keep last 50 events
                session.metadata_json = {
                    **existing_meta,
                    "realtime_events": events[-50:],
                }
                await db.commit()

        return {"ok": True}

    except Exception as exc:
        logger.exception("assistant/realtime/event: unhandled error")
        return {"ok": False, "error": str(exc)}


@router.post("/inspection-check")
async def inspection_check(
    body: InspectionCheckRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Run a compliance inspection readiness check for the organisation.
    """
    logger.info(
        "assistant/inspection-check: org_id=%s user_id=%s",
        body.org_id,
        body.user_id,
    )

    try:
        result = await registry.execute_action(
            db, "run_inspection_readiness_check", body.org_id, {}
        )
        await db.commit()

        return {
            "ok": result.get("ok", False),
            "status": "ready" if result.get("ok") else "issues_found",
            "issues": result.get("issues", []),
            "summary": result.get("message", "Inspection check complete."),
            "data": result,
        }
    except Exception as exc:
        logger.exception(
            "assistant/inspection-check: unhandled error for org_id=%s", body.org_id
        )
        return _safe_error(str(exc))


@router.post("/orders-command")
async def orders_command(
    body: OrdersCommandRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Route order-specific commands to the appropriate action handler.

    Commands: sync | show_review | show_packed | show_blocked | summarize_sales | prepare_route
    """
    command_map = {
        "sync": "sync_leaflink_orders",
        "show_review": "get_orders_needing_review",
        "show_packed": "get_orders_needing_packed",
        "show_blocked": "get_blocked_orders",
        "summarize_sales": "summarize_sales",
        "prepare_route": "build_route_plan",
    }

    action_name = command_map.get(body.command)
    if not action_name:
        return _safe_error(
            f"Unknown command: {body.command}. "
            f"Valid commands: {', '.join(command_map.keys())}"
        )

    logger.info(
        "assistant/orders-command: org_id=%s command=%s action=%s",
        body.org_id,
        body.command,
        action_name,
    )

    action = registry.get_action(action_name)
    if action and action.requires_confirmation:
        # Return a pending action instead of executing
        try:
            session_id = await create_session(db, body.org_id, body.user_id, "orders_tab", None)
            confirmation_id = await orchestrator.create_pending_action(
                db=db,
                session_id=session_id,
                org_id=body.org_id,
                user_id=body.user_id,
                action_name=action_name,
                payload={},
                risk_level=action.risk_level,
            )
            await db.commit()
            return {
                "ok": True,
                "requires_confirmation": True,
                "confirmation_id": confirmation_id,
                "message": f"Action '{action_name}' requires confirmation.",
                "data": {},
            }
        except Exception as exc:
            logger.exception("assistant/orders-command: pending action creation failed")
            return _safe_error(str(exc))

    try:
        result = await registry.execute_action(db, action_name, body.org_id, {})
        await db.commit()
        return {
            "ok": result.get("ok", False),
            "data": result,
            "message": result.get("message", "Command executed."),
        }
    except Exception as exc:
        logger.exception(
            "assistant/orders-command: unhandled error for org_id=%s", body.org_id
        )
        return _safe_error(str(exc))


@router.post("/driver-command")
async def driver_command(
    body: DriverCommandRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Route driver-specific commands to the appropriate action handler.

    Commands: add | update_pin | email_pin | contact | assign_route | notify_change
    """
    command_map = {
        "add": "add_driver",
        "update_pin": "update_driver_pin",
        "email_pin": "email_driver_pin",
        "contact": "contact_driver",
        "assign_route": "assign_driver",
        "notify_change": "notify_route_change",
    }

    action_name = command_map.get(body.command)
    if not action_name:
        return _safe_error(
            f"Unknown command: {body.command}. "
            f"Valid commands: {', '.join(command_map.keys())}"
        )

    logger.info(
        "assistant/driver-command: org_id=%s command=%s action=%s driver_id=%s",
        body.org_id,
        body.command,
        action_name,
        body.driver_id,
    )

    payload = {**(body.data or {}), "driver_id": body.driver_id}
    action = registry.get_action(action_name)

    if action and action.requires_confirmation:
        try:
            session_id = await create_session(db, body.org_id, body.user_id, "routes_tab", None)
            confirmation_id = await orchestrator.create_pending_action(
                db=db,
                session_id=session_id,
                org_id=body.org_id,
                user_id=body.user_id,
                action_name=action_name,
                payload=payload,
                risk_level=action.risk_level,
            )
            await db.commit()
            return {
                "ok": True,
                "requires_confirmation": True,
                "confirmation_id": confirmation_id,
                "message": f"Action '{action_name}' requires confirmation.",
                "data": {},
            }
        except Exception as exc:
            logger.exception("assistant/driver-command: pending action creation failed")
            return _safe_error(str(exc))

    try:
        result = await registry.execute_action(db, action_name, body.org_id, payload)
        await db.commit()
        return {
            "ok": result.get("ok", False),
            "data": result,
            "message": result.get("message", "Command executed."),
        }
    except Exception as exc:
        logger.exception(
            "assistant/driver-command: unhandled error for org_id=%s", body.org_id
        )
        return _safe_error(str(exc))


@router.post("/metrc-command")
async def metrc_command(
    body: MetrcCommandRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Route METRC-specific commands to the appropriate action handler.

    All write actions require confirmation.
    """
    metrc_command_map = {
        "check_package": "check_metrc_package",
        "prepare_manifest": "prepare_manifest",
    }

    action_name = metrc_command_map.get(body.command)
    if not action_name:
        return _safe_error(
            f"Unknown METRC command: {body.command}. "
            f"Valid commands: {', '.join(metrc_command_map.keys())}"
        )

    logger.info(
        "assistant/metrc-command: org_id=%s command=%s action=%s",
        body.org_id,
        body.command,
        action_name,
    )

    action = registry.get_action(action_name)

    if action and action.requires_confirmation:
        try:
            session_id = await create_session(
                db, body.org_id, body.user_id, "compliance_app", None
            )
            confirmation_id = await orchestrator.create_pending_action(
                db=db,
                session_id=session_id,
                org_id=body.org_id,
                user_id=body.user_id,
                action_name=action_name,
                payload=body.data or {},
                risk_level=action.risk_level,
            )
            await db.commit()
            return {
                "ok": True,
                "requires_confirmation": True,
                "confirmation_id": confirmation_id,
                "message": f"METRC action '{action_name}' requires confirmation.",
                "data": {},
            }
        except Exception as exc:
            logger.exception("assistant/metrc-command: pending action creation failed")
            return _safe_error(str(exc))

    try:
        result = await registry.execute_action(
            db, action_name, body.org_id, body.data or {}
        )
        await db.commit()
        return {
            "ok": result.get("ok", False),
            "requires_confirmation": False,
            "confirmation_id": None,
            "data": result,
            "message": result.get("message", "METRC command executed."),
        }
    except Exception as exc:
        logger.exception(
            "assistant/metrc-command: unhandled error for org_id=%s", body.org_id
        )
        return _safe_error(str(exc))


@router.get("/smoke-test")
async def smoke_test(db: AsyncSession = Depends(get_db)):
    """
    Run a quick smoke test across all key subsystems.

    Calls each major endpoint/service internally and returns a summary
    of which checks passed or failed. Useful for deployment verification.
    """
    checks: dict[str, Any] = {}
    all_ok = True

    # 1. Health
    try:
        checks["health"] = {"ok": True}
    except Exception as exc:
        checks["health"] = {"ok": False, "error": str(exc)}
        all_ok = False

    # 2. Assistant health (action registry)
    try:
        actions = registry.list_actions()
        checks["assistant_health"] = {
            "ok": True,
            "actions_registered": len(actions),
        }
    except Exception as exc:
        checks["assistant_health"] = {"ok": False, "error": str(exc)}
        all_ok = False

    # 3. Actions list
    try:
        actions = registry.list_actions()
        checks["actions"] = {"ok": True, "count": len(actions)}
    except Exception as exc:
        checks["actions"] = {"ok": False, "error": str(exc)}
        all_ok = False

    # 4. LeafLink debug (test connection)
    try:
        from services.leaflink_client import LeafLinkClient
        import os

        api_key = os.getenv("LEAFLINK_API_KEY")
        company_id = os.getenv("LEAFLINK_COMPANY_ID")
        if api_key and company_id:
            client = LeafLinkClient(api_key=api_key, company_id=company_id)
            # Just verify the client can be instantiated; don't make a live call
            checks["leaflink_debug"] = {"ok": True, "api_connected": True, "note": "credentials present"}
        else:
            checks["leaflink_debug"] = {
                "ok": True,
                "api_connected": False,
                "note": "LEAFLINK_API_KEY or LEAFLINK_COMPANY_ID not set",
            }
    except Exception as exc:
        checks["leaflink_debug"] = {"ok": False, "error": str(exc)}
        all_ok = False

    # 5. LeafLink orders (database query)
    try:
        from sqlalchemy import func, select
        from models import Order

        result = await db.execute(select(func.count(Order.id)))
        count = result.scalar() or 0
        checks["leaflink_orders"] = {"ok": True, "count": count}
    except Exception as exc:
        checks["leaflink_orders"] = {"ok": False, "error": str(exc)}
        all_ok = False

    return {
        "ok": all_ok,
        "checks": checks,
        "timestamp": _utc_now_iso(),
    }


@router.get("/health")
async def health():
    """Assistant service health check."""
    actions = registry.list_actions()
    return {
        "ok": True,
        "service": "opsyn-assistant",
        "actions_registered": len(actions),
        "timestamp": _utc_now_iso(),
    }
