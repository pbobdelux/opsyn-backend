"""
Session and conversation memory management for the Opsyn AI assistant.

Handles creation and retrieval of sessions, messages, and pending actions.
All functions are safe — they never raise; errors are caught and logged.
"""

import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from models.assistant_models import (
    AssistantMessage,
    AssistantPendingAction,
    AssistantSession,
    new_uuid,
)

logger = logging.getLogger("assistant_memory")


async def create_session(
    db: AsyncSession,
    org_id: str,
    user_id: str | None,
    app_context: str | None,
    device_id: str | None,
) -> str:
    """
    Create a new assistant session and return its session_id.

    Raises RuntimeError on database failure (callers should handle this).
    """
    session_id = new_uuid()
    session = AssistantSession(
        id=session_id,
        org_id=org_id,
        user_id=user_id,
        app_context=app_context,
        device_id=device_id,
    )
    db.add(session)
    await db.flush()
    logger.info(
        "memory: created session_id=%s org_id=%s app_context=%s",
        session_id,
        org_id,
        app_context,
    )
    return session_id


async def get_session(db: AsyncSession, session_id: str) -> dict[str, Any] | None:
    """
    Return session details as a dict, or None if not found.
    """
    try:
        result = await db.execute(
            select(AssistantSession).where(AssistantSession.id == session_id)
        )
        session = result.scalar_one_or_none()
        if session is None:
            return None
        return _serialize_session(session)
    except Exception as exc:
        logger.error("memory: get_session failed for session_id=%s: %s", session_id, exc)
        return None


async def add_message(
    db: AsyncSession,
    session_id: str,
    role: str,
    content: str,
    input_type: str = "text",
) -> None:
    """
    Append a message to the conversation history for a session.

    Never raises — errors are caught and logged.
    """
    try:
        msg = AssistantMessage(
            id=new_uuid(),
            session_id=session_id,
            role=role,
            content=content,
            input_type=input_type,
        )
        db.add(msg)
        await db.flush()
    except Exception as exc:
        logger.error(
            "memory: add_message failed for session_id=%s role=%s: %s",
            session_id,
            role,
            exc,
        )


async def get_conversation(
    db: AsyncSession,
    session_id: str,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """
    Return the most recent messages for a session, oldest first.

    Returns an empty list on any database error.
    """
    try:
        result = await db.execute(
            select(AssistantMessage)
            .where(AssistantMessage.session_id == session_id)
            .order_by(AssistantMessage.created_at.asc())
            .limit(limit)
        )
        rows = result.scalars().all()
        return [_serialize_message(m) for m in rows]
    except Exception as exc:
        logger.error(
            "memory: get_conversation failed for session_id=%s: %s", session_id, exc
        )
        return []


async def get_pending_actions(
    db: AsyncSession,
    session_id: str,
) -> list[dict[str, Any]]:
    """
    Return all pending (non-expired) actions for a session.

    Returns an empty list on any database error.
    """
    try:
        now = datetime.now(timezone.utc)
        result = await db.execute(
            select(AssistantPendingAction)
            .where(
                AssistantPendingAction.session_id == session_id,
                AssistantPendingAction.status == "pending",
                AssistantPendingAction.expires_at > now,
            )
            .order_by(AssistantPendingAction.created_at.desc())
        )
        rows = result.scalars().all()
        return [_serialize_pending_action(a) for a in rows]
    except Exception as exc:
        logger.error(
            "memory: get_pending_actions failed for session_id=%s: %s", session_id, exc
        )
        return []


async def cleanup_expired_actions(db: AsyncSession) -> int:
    """
    Mark all expired pending actions as cancelled.

    Returns the number of actions cleaned up.
    Never raises — errors are caught and logged.
    """
    try:
        now = datetime.now(timezone.utc)
        result = await db.execute(
            select(AssistantPendingAction).where(
                AssistantPendingAction.status == "pending",
                AssistantPendingAction.expires_at <= now,
            )
        )
        expired = result.scalars().all()
        count = 0
        for action in expired:
            action.status = "cancelled"
            count += 1
        if count:
            await db.flush()
            logger.info("memory: cleaned up %d expired pending actions", count)
        return count
    except Exception as exc:
        logger.error("memory: cleanup_expired_actions failed: %s", exc)
        return 0


# ---------------------------------------------------------------------------
# Serializers
# ---------------------------------------------------------------------------


def _serialize_session(session: AssistantSession) -> dict[str, Any]:
    return {
        "id": session.id,
        "org_id": session.org_id,
        "user_id": session.user_id,
        "app_context": session.app_context,
        "device_id": session.device_id,
        "created_at": session.created_at.isoformat() if session.created_at else None,
        "updated_at": session.updated_at.isoformat() if session.updated_at else None,
        "metadata": session.metadata_json,
    }


def _serialize_message(msg: AssistantMessage) -> dict[str, Any]:
    return {
        "id": msg.id,
        "session_id": msg.session_id,
        "role": msg.role,
        "content": msg.content,
        "input_type": msg.input_type,
        "created_at": msg.created_at.isoformat() if msg.created_at else None,
    }


def _serialize_pending_action(action: AssistantPendingAction) -> dict[str, Any]:
    return {
        "id": action.id,
        "confirmation_id": action.confirmation_id,
        "session_id": action.session_id,
        "org_id": action.org_id,
        "user_id": action.user_id,
        "action_name": action.action_name,
        "risk_level": action.risk_level,
        "status": action.status,
        "created_at": action.created_at.isoformat() if action.created_at else None,
        "expires_at": action.expires_at.isoformat() if action.expires_at else None,
    }
