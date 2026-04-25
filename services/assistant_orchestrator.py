"""
Main orchestration engine for the Opsyn AI assistant.

Responsibilities:
  - Classify natural-language intent via keyword matching (MVP)
  - Determine overall risk level for a set of actions
  - Execute safe actions immediately
  - Create pending-action records for medium/high-risk actions
  - Execute confirmed pending actions
  - Compose structured spoken + screen replies
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models.assistant_models import AssistantPendingAction, new_uuid
from services import assistant_action_registry as registry
from services.assistant_audit import log_action
from services.assistant_memory import add_message, create_session

logger = logging.getLogger("assistant_orchestrator")

# ---------------------------------------------------------------------------
# Intent classification — keyword-based MVP
# ---------------------------------------------------------------------------

INTENT_MAP: list[tuple[str, list[str], list[str]]] = [
    # (intent_name, keywords, suggested_actions)
    (
        "get_attention",
        [
            "what needs my attention",
            "what should i focus on",
            "what is wrong today",
            "what needs fixed",
            "give me my morning report",
            "morning report",
            "what needs attention",
            "what do i need to fix",
        ],
        ["get_attention"],
    ),
    (
        "sync_orders",
        ["sync", "refresh", "update orders", "pull orders", "fetch orders"],
        ["sync_leaflink_orders"],
    ),
    (
        "list_orders",
        ["show orders", "list orders", "pending orders", "orders needing review",
         "review orders", "get orders", "what orders"],
        ["get_orders_needing_review"],
    ),
    (
        "packed_orders",
        ["packed", "pack orders", "ready to pack", "needs packed", "packing"],
        ["get_orders_needing_packed"],
    ),
    (
        "blocked_orders",
        ["blocked", "blocked orders", "stuck orders"],
        ["get_blocked_orders"],
    ),
    (
        "sales_summary",
        ["sales", "revenue", "summarize", "summary", "how much", "total sales"],
        ["summarize_sales"],
    ),
    (
        "build_route",
        ["build route", "plan route", "route plan", "create route", "make route"],
        ["build_route_plan"],
    ),
    (
        "assign_driver",
        ["assign driver", "assign a driver", "put driver on route"],
        ["assign_driver"],
    ),
    (
        "add_driver",
        ["add driver", "new driver", "create driver", "register driver"],
        ["add_driver"],
    ),
    (
        "update_driver_pin",
        ["update pin", "change pin", "reset pin", "new pin"],
        ["update_driver_pin"],
    ),
    (
        "email_driver_pin",
        ["email pin", "send pin", "resend pin"],
        ["email_driver_pin"],
    ),
    (
        "contact_driver",
        ["contact driver", "message driver", "text driver", "call driver"],
        ["contact_driver"],
    ),
    (
        "notify_route_change",
        ["notify driver", "route change", "notify change", "alert driver"],
        ["notify_route_change"],
    ),
    (
        "check_metrc",
        ["metrc", "package", "check package", "lookup package", "tag"],
        ["check_metrc_package"],
    ),
    (
        "prepare_manifest",
        ["manifest", "prepare manifest", "transfer manifest", "submit manifest"],
        ["prepare_manifest"],
    ),
    (
        "check_compliance",
        ["compliance", "inspection", "audit", "readiness", "inspection check",
         "compliance status", "ready for inspection"],
        ["run_inspection_readiness_check"],
    ),
    (
        "compliance_tasks",
        ["task list", "compliance tasks", "what do i need to do", "todo"],
        ["create_compliance_task_list"],
    ),
    (
        "corrective_action",
        ["corrective action", "capa", "fix compliance", "draft capa"],
        ["draft_corrective_action"],
    ),
    (
        "explain_order",
        ["explain order", "what is this order", "order status", "why is order"],
        ["explain_order_status"],
    ),
    (
        "explain_compliance",
        ["explain compliance", "what is the risk", "compliance risk", "why is this flagged"],
        ["explain_compliance_risk"],
    ),
]


def classify_intent(message: str) -> dict[str, Any]:
    """
    Classify the user's intent from a natural-language message.

    Uses simple keyword matching (MVP). Returns the best-matching intent
    with its suggested actions and a confidence score.

    Returns
    -------
    {
        "intent": str,
        "confidence": float,   # 0.0 – 1.0
        "suggested_actions": list[str],
    }
    """
    lower = message.lower().strip()

    best_intent = "unknown"
    best_actions: list[str] = []
    best_score = 0

    for intent_name, keywords, actions in INTENT_MAP:
        for kw in keywords:
            if kw in lower:
                # Longer keyword matches are more specific → higher score
                score = len(kw)
                if score > best_score:
                    best_score = score
                    best_intent = intent_name
                    best_actions = actions

    confidence = min(1.0, best_score / 20.0) if best_score > 0 else 0.0

    return {
        "intent": best_intent,
        "confidence": round(confidence, 2),
        "suggested_actions": best_actions,
    }


def determine_risk_level(action_names: list[str]) -> str:
    """
    Return the highest risk level across all named actions.

    safe < medium < high
    """
    level = "safe"
    for name in action_names:
        action = registry.get_action(name)
        if action is None:
            continue
        if action.risk_level == "high":
            return "high"
        if action.risk_level == "medium":
            level = "medium"
    return level


# ---------------------------------------------------------------------------
# Orchestrator class
# ---------------------------------------------------------------------------


class AssistantOrchestrator:
    """Main orchestration engine for the AI assistant."""

    async def classify_intent(self, message: str) -> dict[str, Any]:
        """Classify user intent from natural language."""
        return classify_intent(message)

    async def determine_risk_level(self, actions: list[str]) -> str:
        """Determine overall risk level for a list of action names."""
        return determine_risk_level(actions)

    async def execute_safe_actions(
        self,
        db: AsyncSession,
        org_id: str,
        user_id: str | None,
        session_id: str | None,
        actions: list[str],
        payload: dict,
    ) -> dict[str, Any]:
        """
        Execute all safe actions immediately.

        Returns
        -------
        {
            "ok": bool,
            "executed_actions": list[str],
            "data": dict,
            "errors": list[str],
        }
        """
        executed: list[str] = []
        data: dict[str, Any] = {}
        errors: list[str] = []

        for action_name in actions:
            try:
                result = await registry.execute_action(db, action_name, org_id, payload)
                executed.append(action_name)
                data[action_name] = result

                # Audit log
                status = "success" if result.get("ok") else "failure"
                await log_action(
                    db=db,
                    org_id=org_id,
                    user_id=user_id,
                    session_id=session_id,
                    action_name=action_name,
                    risk_level="safe",
                    request={"action": action_name, "payload_keys": list(payload.keys())},
                    result=result,
                    status=status,
                    error=result.get("error") if not result.get("ok") else None,
                )
            except Exception as exc:
                logger.exception("execute_safe_actions: %s raised", action_name)
                errors.append(f"{action_name}: {exc}")
                await log_action(
                    db=db,
                    org_id=org_id,
                    user_id=user_id,
                    session_id=session_id,
                    action_name=action_name,
                    risk_level="safe",
                    request={"action": action_name},
                    result=None,
                    status="failure",
                    error=str(exc),
                )

        try:
            await db.commit()
        except Exception as exc:
            logger.error("execute_safe_actions: commit failed: %s", exc)
            await db.rollback()
            errors.append(f"commit_failed: {exc}")

        return {
            "ok": len(errors) == 0,
            "executed_actions": executed,
            "data": data,
            "errors": errors,
        }

    async def create_pending_action(
        self,
        db: AsyncSession,
        session_id: str,
        org_id: str,
        user_id: str | None,
        action_name: str,
        payload: dict,
        risk_level: str,
    ) -> str:
        """
        Create a pending action record awaiting user confirmation.

        Returns the confirmation_id.
        """
        confirmation_id = new_uuid()
        now = datetime.now(timezone.utc)
        pending = AssistantPendingAction(
            id=new_uuid(),
            confirmation_id=confirmation_id,
            session_id=session_id,
            org_id=org_id,
            user_id=user_id,
            action_name=action_name,
            payload_json=payload,
            risk_level=risk_level,
            status="pending",
            expires_at=now + timedelta(minutes=15),
        )
        db.add(pending)
        await db.flush()
        logger.info(
            "orchestrator: created pending action confirmation_id=%s action=%s risk=%s",
            confirmation_id,
            action_name,
            risk_level,
        )
        return confirmation_id

    async def execute_pending_action(
        self,
        db: AsyncSession,
        confirmation_id: str,
        org_id: str,
        user_id: str | None,
    ) -> dict[str, Any]:
        """
        Execute a previously created pending action after user confirmation.

        Returns
        -------
        {
            "ok": bool,
            "executed_actions": list[str],
            "data": dict,
            "errors": list[str],
        }
        """
        try:
            result = await db.execute(
                select(AssistantPendingAction).where(
                    AssistantPendingAction.confirmation_id == confirmation_id,
                    AssistantPendingAction.org_id == org_id,
                )
            )
            pending = result.scalar_one_or_none()

            if pending is None:
                return {"ok": False, "error": "Pending action not found", "errors": ["not_found"]}

            now = datetime.now(timezone.utc)

            if pending.status != "pending":
                return {
                    "ok": False,
                    "error": f"Action is already {pending.status}",
                    "errors": [f"status_{pending.status}"],
                }

            if pending.expires_at < now:
                pending.status = "cancelled"
                await db.commit()
                return {"ok": False, "error": "Action has expired", "errors": ["expired"]}

            # Mark as approved before executing
            pending.status = "approved"
            await db.flush()

            action_result = await registry.execute_action(
                db, pending.action_name, org_id, pending.payload_json or {}
            )

            exec_status = "success" if action_result.get("ok") else "failure"
            pending.status = "executed" if action_result.get("ok") else "failed"
            pending.executed_at = datetime.now(timezone.utc)
            if not action_result.get("ok"):
                pending.error_message = action_result.get("error", "Unknown error")

            await log_action(
                db=db,
                org_id=org_id,
                user_id=user_id,
                session_id=pending.session_id,
                action_name=pending.action_name,
                risk_level=pending.risk_level,
                request={
                    "confirmation_id": confirmation_id,
                    "action": pending.action_name,
                    "payload_keys": list((pending.payload_json or {}).keys()),
                },
                result=action_result,
                status=exec_status,
                error=action_result.get("error") if not action_result.get("ok") else None,
            )

            await db.commit()

            return {
                "ok": action_result.get("ok", False),
                "executed_actions": [pending.action_name],
                "data": {pending.action_name: action_result},
                "errors": [action_result.get("error")] if not action_result.get("ok") else [],
            }

        except Exception as exc:
            logger.exception("execute_pending_action failed for confirmation_id=%s", confirmation_id)
            try:
                await db.rollback()
            except Exception:
                pass
            return {"ok": False, "error": str(exc), "errors": [str(exc)]}

    async def process_command(
        self,
        db: AsyncSession,
        org_id: str,
        user_id: str | None,
        role: str | None,
        app_context: str | None,
        message: str,
        session_id: str | None,
        device_id: str | None,
        input_type: str = "text",
    ) -> dict[str, Any]:
        """
        Main entry point: process a natural-language command end-to-end.

        Flow:
          1. Create or reuse session
          2. Store user message
          3. Classify intent
          4. Determine risk level
          5a. Safe → execute immediately, store assistant reply
          5b. Medium/High → create pending action, return confirmation_id
          6. Return structured response

        Returns
        -------
        {
            "ok": bool,
            "session_id": str,
            "spoken_reply": str,
            "screen_reply": str,
            "intent": str,
            "confidence": float,
            "risk_level": str,
            "requires_confirmation": bool,
            "confirmation_id": str | None,
            "proposed_actions": list[str],
            "data": dict,
            "errors": list[str],
        }
        """
        errors: list[str] = []

        # ------------------------------------------------------------------
        # 1. Session management
        # ------------------------------------------------------------------
        try:
            if not session_id:
                session_id = await create_session(
                    db, org_id, user_id, app_context, device_id
                )
                await db.commit()
        except Exception as exc:
            logger.error("process_command: session creation failed: %s", exc)
            errors.append(f"session_error: {exc}")
            session_id = session_id or "no_session"

        # ------------------------------------------------------------------
        # 2. Store user message
        # ------------------------------------------------------------------
        await add_message(db, session_id, "user", message, input_type)

        # ------------------------------------------------------------------
        # 3. Classify intent
        # ------------------------------------------------------------------
        intent_result = classify_intent(message)
        intent = intent_result["intent"]
        confidence = intent_result["confidence"]
        suggested_actions = intent_result["suggested_actions"]

        logger.info(
            "process_command: org_id=%s intent=%s confidence=%.2f actions=%s",
            org_id,
            intent,
            confidence,
            suggested_actions,
        )

        # ------------------------------------------------------------------
        # 4. Determine risk level
        # ------------------------------------------------------------------
        risk_level = determine_risk_level(suggested_actions)

        # ------------------------------------------------------------------
        # 5. Execute or queue
        # ------------------------------------------------------------------
        requires_confirmation = False
        confirmation_id: str | None = None
        data: dict[str, Any] = {}
        spoken_reply = ""
        screen_reply = ""

        if not suggested_actions or intent == "unknown":
            spoken_reply = (
                "I'm not sure what you'd like to do. "
                "Try asking me to show orders, sync LeafLink, add a driver, or check compliance."
            )
            screen_reply = spoken_reply
            await add_message(db, session_id, "assistant", spoken_reply, "text")
            try:
                await db.commit()
            except Exception:
                pass
            return {
                "ok": True,
                "session_id": session_id,
                "spoken_reply": spoken_reply,
                "screen_reply": screen_reply,
                "intent": intent,
                "confidence": confidence,
                "risk_level": risk_level,
                "requires_confirmation": False,
                "confirmation_id": None,
                "proposed_actions": suggested_actions,
                "data": {},
                "errors": errors,
            }

        # Check if any action requires confirmation
        any_requires_confirmation = any(
            (a := registry.get_action(name)) is not None and a.requires_confirmation
            for name in suggested_actions
        )

        if risk_level == "safe" and not any_requires_confirmation:
            # Execute immediately
            exec_result = await self.execute_safe_actions(
                db, org_id, user_id, session_id, suggested_actions, {}
            )
            data = exec_result.get("data", {})
            errors.extend(exec_result.get("errors", []))

            spoken_reply = _build_spoken_reply(intent, risk_level, exec_result)
            screen_reply = _build_screen_reply(intent, risk_level, exec_result, data)
        else:
            # Queue for confirmation — create one pending action per suggested action
            requires_confirmation = True
            confirmation_ids: list[str] = []

            for action_name in suggested_actions:
                try:
                    cid = await self.create_pending_action(
                        db=db,
                        session_id=session_id,
                        org_id=org_id,
                        user_id=user_id,
                        action_name=action_name,
                        payload={},
                        risk_level=risk_level,
                    )
                    confirmation_ids.append(cid)
                except Exception as exc:
                    logger.error("process_command: create_pending_action failed: %s", exc)
                    errors.append(str(exc))

            # Return the first confirmation_id as the primary one
            confirmation_id = confirmation_ids[0] if confirmation_ids else None

            action_label = ", ".join(suggested_actions)
            spoken_reply = (
                f"This action ({action_label}) requires your confirmation because it is "
                f"a {risk_level}-risk operation. Please confirm to proceed."
            )
            screen_reply = (
                f"⚠️ Confirmation required for: {action_label}\n"
                f"Risk level: {risk_level.upper()}\n"
                f"Confirmation ID: {confirmation_id}"
            )

            try:
                await db.commit()
            except Exception as exc:
                logger.error("process_command: commit failed after pending action: %s", exc)
                errors.append(str(exc))

        # ------------------------------------------------------------------
        # 6. Store assistant reply
        # ------------------------------------------------------------------
        await add_message(db, session_id, "assistant", spoken_reply, "text")
        try:
            await db.commit()
        except Exception:
            pass

        return {
            "ok": True,
            "session_id": session_id,
            "spoken_reply": spoken_reply,
            "screen_reply": screen_reply,
            "intent": intent,
            "confidence": confidence,
            "risk_level": risk_level,
            "requires_confirmation": requires_confirmation,
            "confirmation_id": confirmation_id,
            "proposed_actions": suggested_actions,
            "data": data,
            "errors": errors,
        }


# ---------------------------------------------------------------------------
# Reply builders
# ---------------------------------------------------------------------------


def _build_spoken_reply(
    intent: str,
    risk_level: str,
    exec_result: dict[str, Any],
) -> str:
    if not exec_result.get("ok"):
        return "I ran into an issue completing that request. Please check the details and try again."

    # For get_attention, use the spoken_reply from the report itself
    if intent == "get_attention":
        attention_data = exec_result.get("data", {}).get("get_attention", {})
        spoken = attention_data.get("spoken_reply")
        if spoken:
            return spoken

    replies = {
        "list_orders": "Here are the orders that need your attention.",
        "packed_orders": "Here are the orders ready to be packed.",
        "blocked_orders": "Here are the blocked orders.",
        "sales_summary": "Here's your sales summary.",
        "check_compliance": "Here's your compliance readiness status.",
        "compliance_tasks": "Here's your compliance task list.",
        "explain_order": "Here's the order status breakdown.",
        "explain_compliance": "Here's the compliance risk explanation.",
    }
    return replies.get(intent, "Done! Here are the results.")


def _build_screen_reply(
    intent: str,
    risk_level: str,
    exec_result: dict[str, Any],
    data: dict[str, Any],
) -> str:
    if not exec_result.get("ok"):
        errors = exec_result.get("errors", [])
        return f"Error: {'; '.join(errors) if errors else 'Unknown error'}"

    executed = exec_result.get("executed_actions", [])
    return f"Executed: {', '.join(executed)}" if executed else "No actions executed."


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

orchestrator = AssistantOrchestrator()
