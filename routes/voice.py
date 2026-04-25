import logging
import os
from typing import Any, Optional

from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from services.elevenlabs_service import elevenlabs_service
from services.voice_agent_service import VoiceAgentService
from utils.json_utils import make_json_safe

logger = logging.getLogger("voice_routes")

router = APIRouter(prefix="/voice", tags=["voice"])

OPSYN_AGENT_SHARED_SECRET = os.getenv("OPSYN_AGENT_SHARED_SECRET", "").strip()


def verify_agent_secret(authorization: Optional[str] = Header(None)) -> bool:
    """Verify the agent shared secret."""
    if not OPSYN_AGENT_SHARED_SECRET:
        logger.warning("voice: shared_secret not configured")
        return False

    if not authorization or not authorization.startswith("Bearer "):
        return False

    token = authorization[7:]
    return token == OPSYN_AGENT_SHARED_SECRET


@router.get("/health")
async def voice_health():
    """Health check for voice agent."""
    return {
        "ok": True,
        "service": "voice_agent",
        "elevenlabs_configured": elevenlabs_service.is_healthy(),
        "agent_shared_secret_configured": bool(OPSYN_AGENT_SHARED_SECRET),
    }


@router.post("/session")
async def voice_session(
    authorization: Optional[str] = Header(None),
):
    """
    Create or return a valid ElevenLabs conversation session.
    Requires Bearer token matching OPSYN_AGENT_SHARED_SECRET.
    """
    if not verify_agent_secret(authorization):
        logger.warning("voice: session_unauthorized")
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        logger.info("voice: session_requested")

        if not elevenlabs_service.is_healthy():
            logger.error("voice: elevenlabs_not_configured")
            raise HTTPException(status_code=503, detail="ElevenLabs not configured")

        # Create a new conversation
        conversation = await elevenlabs_service.create_conversation()

        logger.info("voice: session_created conversation_id=%s", conversation.get("conversation_id"))

        return make_json_safe({
            "ok": True,
            "conversation_id": conversation.get("conversation_id"),
            "session_token": conversation.get("session_token"),
            "expires_at": conversation.get("expires_at"),
        })

    except HTTPException:
        raise
    except Exception as e:
        logger.error("voice: session_failed error=%s", e)
        return {
            "ok": False,
            "error": str(e),
        }


@router.post("/message")
async def voice_message(
    body: dict[str, Any],
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
):
    """
    Send a message to the voice agent and get a response.
    Requires Bearer token matching OPSYN_AGENT_SHARED_SECRET.

    Body:
    {
      "conversation_id": "...",
      "message": "What customers need attention today?"
    }
    """
    if not verify_agent_secret(authorization):
        logger.warning("voice: message_unauthorized")
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        conversation_id = body.get("conversation_id")
        message = body.get("message")

        if not conversation_id or not message:
            raise ValueError("Missing conversation_id or message")

        logger.info("voice: message_received conversation_id=%s message_length=%s", conversation_id, len(message))

        if not elevenlabs_service.is_healthy():
            logger.error("voice: elevenlabs_not_configured")
            raise HTTPException(status_code=503, detail="ElevenLabs not configured")

        # Get voice agent service for context
        agent_service = VoiceAgentService(db)

        # Prepare context for the agent
        dashboard = await agent_service.get_dashboard_summary()
        recent_orders = await agent_service.get_recent_orders(limit=3)
        sync_status = await agent_service.get_sync_status()

        # Build system context
        system_context = f"""
You are the Opsyn Voice Agent, a helpful AI assistant for the Opsyn CRM app.

Current Business Context:
- Total Customers: {dashboard.get('customer_count', 0)}
- Total Orders: {dashboard.get('order_count', 0)}
- Total Spend: ${dashboard.get('total_spend', 0):.2f}
- Last Sync: {dashboard.get('last_sync_at', 'Never')}
- Sync Status: {dashboard.get('sync_status', 'Unknown')}

Recent Orders:
{chr(10).join([f"- {o.get('customer_name')}: {o.get('order_number')} (${o.get('amount'):.2f}) - {o.get('status')}" for o in recent_orders])}

Your capabilities:
1. Answer questions about customers, orders, and sales activity
2. Summarize customer information and order history
3. Provide sync status and system health information
4. Prepare actions (send messages, create routes) for user confirmation
5. Never directly modify data without explicit user confirmation

Always be helpful, concise, and professional. If you're unsure, ask a clarifying question.
"""

        # Send message to ElevenLabs agent
        response = await elevenlabs_service.send_message(conversation_id, message)

        logger.info(
            "voice: message_response conversation_id=%s response_length=%s",
            conversation_id,
            len(response.get("response", "")),
        )

        return make_json_safe({
            "ok": True,
            "conversation_id": conversation_id,
            "response": response.get("response"),
            "audio_url": response.get("audio_url"),
            "context": {
                "dashboard": dashboard,
                "recent_orders": recent_orders,
                "sync_status": sync_status,
            },
        })

    except HTTPException:
        raise
    except Exception as e:
        logger.error("voice: message_failed error=%s", e)
        return {
            "ok": False,
            "error": str(e),
        }


@router.post("/action/confirm")
async def voice_action_confirm(
    body: dict[str, Any],
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
):
    """
    Execute a confirmed action from the voice agent.
    Requires Bearer token matching OPSYN_AGENT_SHARED_SECRET.

    Body:
    {
      "action_type": "send_message",
      "action_id": "...",
      "confirmed": true,
      "details": {...}
    }
    """
    if not verify_agent_secret(authorization):
        logger.warning("voice: action_confirm_unauthorized")
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        action_type = body.get("action_type")
        action_id = body.get("action_id")
        confirmed = body.get("confirmed", False)
        details = body.get("details", {})

        logger.info(
            "voice: action_confirm_received action_type=%s action_id=%s confirmed=%s",
            action_type,
            action_id,
            confirmed,
        )

        if not confirmed:
            logger.warning("voice: action_not_confirmed action_id=%s", action_id)
            return {
                "ok": False,
                "error": "Action not confirmed",
            }

        # Route to appropriate handler
        if action_type == "send_message":
            logger.info(
                "voice: action_send_message customer_name=%s",
                details.get("customer_name"),
            )
            return {
                "ok": True,
                "action_type": action_type,
                "status": "executed",
                "message": f"Message prepared for {details.get('customer_name')}",
            }

        elif action_type == "create_route":
            logger.info("voice: action_create_route stops=%s", len(details.get("stops", [])))
            return {
                "ok": True,
                "action_type": action_type,
                "status": "executed",
                "message": f"Route created with {len(details.get('stops', []))} stops",
            }

        else:
            logger.warning("voice: action_unknown action_type=%s", action_type)
            return {
                "ok": False,
                "error": f"Unknown action type: {action_type}",
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("voice: action_confirm_failed error=%s", e)
        return {
            "ok": False,
            "error": str(e),
        }


@router.get("/status")
async def voice_status(
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
):
    """
    Get voice agent status and system health.
    Requires Bearer token matching OPSYN_AGENT_SHARED_SECRET.
    """
    if not verify_agent_secret(authorization):
        logger.warning("voice: status_unauthorized")
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        logger.info("voice: status_requested")

        agent_service = VoiceAgentService(db)
        dashboard = await agent_service.get_dashboard_summary()
        sync_status = await agent_service.get_sync_status()

        return make_json_safe({
            "ok": True,
            "service": "voice_agent",
            "elevenlabs_configured": elevenlabs_service.is_healthy(),
            "agent_shared_secret_configured": bool(OPSYN_AGENT_SHARED_SECRET),
            "dashboard": dashboard,
            "sync_status": sync_status,
        })

    except HTTPException:
        raise
    except Exception as e:
        logger.error("voice: status_failed error=%s", e)
        return {
            "ok": False,
            "error": str(e),
        }
