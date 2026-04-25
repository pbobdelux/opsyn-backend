import logging
import os
from typing import Any, Optional
from datetime import datetime, timezone, timedelta
import uuid

from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from services.twin_voice_service import twin_voice_service
from services.elevenlabs_stt_tts_service import elevenlabs_stt_tts_service
from utils.json_utils import make_json_safe

logger = logging.getLogger("voice_brain")

router = APIRouter(prefix="/voice-brain", tags=["voice-brain"])

OPSYN_AGENT_SHARED_SECRET = os.getenv("OPSYN_AGENT_SHARED_SECRET", "").strip()
OPSYN_BACKEND_URL = os.getenv("OPSYN_BACKEND_URL", "").strip()

# In-memory session store (can be moved to Redis/Postgres later)
VOICE_SESSIONS = {}


def verify_agent_secret(authorization: Optional[str] = Header(None)) -> bool:
    """Verify the agent shared secret."""
    if not OPSYN_AGENT_SHARED_SECRET:
        logger.warning("voice_brain: shared_secret not configured")
        return False

    if not authorization or not authorization.startswith("Bearer "):
        return False

    token = authorization[7:]
    return token == OPSYN_AGENT_SHARED_SECRET


@router.get("/health")
async def voice_health():
    """Health check for voice brain (no auth required)."""
    return {
        "ok": True,
        "service": "voice_brain",
        "version": "1.0",
    }


@router.get("/status")
async def voice_status(
    authorization: Optional[str] = Header(None),
):
    """Get voice brain status and configuration."""
    if not verify_agent_secret(authorization):
        logger.warning("voice_brain: status_unauthorized")
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        logger.info("voice_brain: status_requested")

        return make_json_safe({
            "ok": True,
            "service": "voice_brain",
            "twin_brain_configured": twin_voice_service.is_healthy(),
            "elevenlabs_stt_configured": elevenlabs_stt_tts_service.is_stt_healthy(),
            "elevenlabs_tts_configured": elevenlabs_stt_tts_service.is_tts_healthy(),
            "opsyn_shared_secret_set": bool(OPSYN_AGENT_SHARED_SECRET),
            "opsyn_backend_url_set": bool(OPSYN_BACKEND_URL),
        })

    except Exception as e:
        logger.error("voice_brain: status_failed error=%s", e, exc_info=True)
        return {
            "ok": False,
            "error": str(e),
        }


@router.post("/session")
async def voice_session(
    body: dict[str, Any],
    authorization: Optional[str] = Header(None),
):
    """
    Create a new voice session.
    Requires Bearer token matching OPSYN_AGENT_SHARED_SECRET.

    Body:
    {
      "tenant_org_id": "noble-nectar-llc"
    }
    """
    if not verify_agent_secret(authorization):
        logger.warning("voice_brain: session_unauthorized")
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        tenant_org_id = body.get("tenant_org_id", "")

        if not tenant_org_id:
            raise ValueError("Missing tenant_org_id")

        logger.info("voice_brain: session_requested tenant_org_id=%s", tenant_org_id)

        # Create session
        session_id = str(uuid.uuid4())
        expires_at = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()

        VOICE_SESSIONS[session_id] = {
            "tenant_org_id": tenant_org_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "expires_at": expires_at,
        }

        logger.info("voice_brain: session_created session_id=%s", session_id[:8] + "...")

        return make_json_safe({
            "ok": True,
            "session_id": session_id,
            "expires_at": expires_at,
            "data_source": "live",
        })

    except Exception as e:
        logger.error("voice_brain: session_failed error=%s", e, exc_info=True)
        return {
            "ok": False,
            "error": str(e),
            "data_source": "error",
        }


@router.post("/message")
async def voice_message(
    body: dict[str, Any],
    authorization: Optional[str] = Header(None),
):
    """
    Send a message to the voice brain.
    Requires Bearer token matching OPSYN_AGENT_SHARED_SECRET.

    Body:
    {
      "session_id": "...",
      "tenant_org_id": "noble-nectar-llc",
      "user_text": "What customers need attention today?",
      "audio_base64": null,
      "audio_mime": "audio/wav",
      "context": {},
      "return_audio": false
    }
    """
    if not verify_agent_secret(authorization):
        logger.warning("voice_brain: message_unauthorized")
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        session_id = body.get("session_id")
        tenant_org_id = body.get("tenant_org_id", "")
        user_text = body.get("user_text")
        audio_base64 = body.get("audio_base64")
        audio_mime = body.get("audio_mime", "audio/wav")
        context = body.get("context", {})
        return_audio = body.get("return_audio", False)

        if not session_id or not tenant_org_id:
            raise ValueError("Missing session_id or tenant_org_id")

        logger.info(
            "voice_brain: message_received session_id=%s tenant_org_id=%s has_text=%s has_audio=%s",
            session_id[:8] + "...",
            tenant_org_id,
            bool(user_text),
            bool(audio_base64),
        )

        # Transcribe audio if needed
        if not user_text and audio_base64:
            try:
                logger.info("voice_brain: transcribing_audio")
                user_text = await elevenlabs_stt_tts_service.transcribe_audio(audio_base64, audio_mime)
                logger.info("voice_brain: transcription_complete text_length=%s", len(user_text))
            except Exception as e:
                logger.error("voice_brain: transcription_failed error=%s", e)
                return {
                    "ok": False,
                    "error": "Audio transcription failed",
                    "data_source": "error",
                }

        if not user_text:
            raise ValueError("No user_text or audio_base64 provided")

        # Check if Twin is configured
        if not twin_voice_service.is_healthy():
            logger.error("voice_brain: twin_not_configured")
            return {
                "ok": False,
                "error_code": "twin_brain_not_configured",
                "message": "Twin Voice Brain is not configured.",
                "data_source": "error",
            }

        # Send to Twin
        logger.info("voice_brain: sending_to_twin")
        twin_result = await twin_voice_service.send_message(
            message=user_text,
            tenant_org_id=tenant_org_id,
            context=context,
            opsyn_backend_url=OPSYN_BACKEND_URL,
        )

        if not twin_result.get("ok"):
            logger.error("voice_brain: twin_failed status=%s", twin_result.get("status"))
            return {
                "ok": False,
                "error": "Twin Voice Brain failed",
                "data_source": "error",
            }

        # Parse Twin output
        output = twin_result.get("output", {})
        speak_text = output.get("speak_text", "")
        display_text = output.get("display_text", "")
        kind = output.get("kind", "answer")
        data = output.get("data", {})
        proposed_plan = output.get("proposed_plan")
        needs = output.get("needs")

        # Synthesize speech if requested
        audio_base64_response = None
        if return_audio and speak_text:
            try:
                logger.info("voice_brain: synthesizing_speech")
                audio_base64_response = await elevenlabs_stt_tts_service.synthesize_speech(speak_text)
                logger.info("voice_brain: synthesis_complete audio_length=%s", len(audio_base64_response))
            except Exception as e:
                logger.error("voice_brain: synthesis_failed error=%s", e)
                # Don't fail the whole request if TTS fails

        logger.info("voice_brain: message_complete session_id=%s kind=%s", session_id[:8] + "...", kind)

        response = {
            "ok": True,
            "session_id": session_id,
            "kind": kind,
            "speak_text": speak_text,
            "display_text": display_text,
            "data": data,
            "data_source": "live",
        }

        if proposed_plan:
            response["proposed_plan"] = proposed_plan
        if needs:
            response["needs"] = needs
        if audio_base64_response:
            response["audio_base64"] = audio_base64_response

        return make_json_safe(response)

    except Exception as e:
        logger.error("voice_brain: message_failed error=%s", e, exc_info=True)
        return {
            "ok": False,
            "error": str(e),
            "data_source": "error",
        }


@router.post("/callback")
async def voice_brain_callback(
    body: dict[str, Any],
):
    """
    Receive callbacks from Twin Voice Brain.
    No authentication required for now (Twin will call this).

    Body:
    {
      "run_id": "...",
      "status": "completed",
      "output": {...},
      "error": null
    }
    """
    try:
        run_id = body.get("run_id", "unknown")
        status = body.get("status", "unknown")

        logger.info(
            "voice_brain: callback_received run_id=%s status=%s",
            run_id[:8] + "..." if len(run_id) > 8 else run_id,
            status,
        )

        # Log full payload for debugging (safe - no secrets in callback)
        logger.debug("voice_brain: callback_payload=%s", body)

        # TODO: Store callback result in database or cache for later retrieval
        # For now, just acknowledge receipt

        return make_json_safe({
            "ok": True,
            "received": True,
            "run_id": run_id,
        })

    except Exception as e:
        logger.error("voice_brain: callback_failed error=%s", e, exc_info=True)
        return {
            "ok": False,
            "error": str(e),
        }


@router.post("/action/confirm")
async def voice_action_confirm(
    body: dict[str, Any],
    authorization: Optional[str] = Header(None),
):
    """
    Confirm and execute a voice action.
    Requires Bearer token matching OPSYN_AGENT_SHARED_SECRET.

    Body:
    {
      "session_id": "...",
      "tenant_org_id": "noble-nectar-llc",
      "plan_id": "...",
      "confirm_token": "...",
      "payload": {},
      "return_audio": false
    }
    """
    if not verify_agent_secret(authorization):
        logger.warning("voice_brain: action_confirm_unauthorized")
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        session_id = body.get("session_id")
        tenant_org_id = body.get("tenant_org_id", "")
        plan_id = body.get("plan_id")
        confirm_token = body.get("confirm_token")
        payload = body.get("payload", {})
        return_audio = body.get("return_audio", False)

        if not session_id or not tenant_org_id or not plan_id:
            raise ValueError("Missing session_id, tenant_org_id, or plan_id")

        logger.info(
            "voice_brain: action_confirm_received session_id=%s plan_id=%s",
            session_id[:8] + "...",
            plan_id[:8] + "...",
        )

        # Send confirmation to Twin
        if not twin_voice_service.is_healthy():
            logger.error("voice_brain: twin_not_configured")
            return {
                "ok": False,
                "error_code": "twin_brain_not_configured",
                "message": "Twin Voice Brain is not configured.",
                "data_source": "error",
            }

        # Build confirmation message
        confirmation_message = f"Confirm action: {plan_id}"

        twin_result = await twin_voice_service.send_message(
            message=confirmation_message,
            tenant_org_id=tenant_org_id,
            context={
                "mode": "confirm",
                "plan_id": plan_id,
                "confirm_token": confirm_token,
                "payload": payload,
            },
            opsyn_backend_url=OPSYN_BACKEND_URL,
        )

        if not twin_result.get("ok"):
            logger.error("voice_brain: action_confirm_twin_failed")
            return {
                "ok": False,
                "error": "Twin Voice Brain failed",
                "data_source": "error",
            }

        output = twin_result.get("output", {})
        speak_text = output.get("speak_text", "")
        display_text = output.get("display_text", "")
        kind = output.get("kind", "executed")

        # Synthesize speech if requested
        audio_base64_response = None
        if return_audio and speak_text:
            try:
                audio_base64_response = await elevenlabs_stt_tts_service.synthesize_speech(speak_text)
            except Exception as e:
                logger.error("voice_brain: action_confirm_synthesis_failed error=%s", e)

        logger.info("voice_brain: action_confirm_complete session_id=%s kind=%s", session_id[:8] + "...", kind)

        response = {
            "ok": True,
            "session_id": session_id,
            "kind": kind,
            "speak_text": speak_text,
            "display_text": display_text,
            "data_source": "live",
        }

        if audio_base64_response:
            response["audio_base64"] = audio_base64_response

        return make_json_safe(response)

    except Exception as e:
        logger.error("voice_brain: action_confirm_failed error=%s", e, exc_info=True)
        return {
            "ok": False,
            "error": str(e),
            "data_source": "error",
        }
