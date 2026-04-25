"""
Audit logging for the Opsyn AI assistant layer.

All assistant action executions are recorded here for compliance,
debugging, and analytics. Logs are append-only and never expose secrets.
"""

import logging
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models.assistant_models import AssistantAuditLog, new_uuid

logger = logging.getLogger("assistant_audit")


async def log_action(
    db: AsyncSession,
    org_id: str,
    user_id: str | None,
    session_id: str | None,
    action_name: str,
    risk_level: str,
    request: dict,
    result: dict | None,
    status: str,
    error: str | None = None,
) -> None:
    """
    Append an audit log entry for an assistant action execution.

    Never raises — any database error is caught and logged server-side
    so that a logging failure never disrupts the caller.

    Parameters
    ----------
    db:          Async SQLAlchemy session.
    org_id:      Organisation identifier.
    user_id:     User identifier (may be None).
    session_id:  Assistant session ID (may be None).
    action_name: Name of the action that was executed.
    risk_level:  safe | medium | high
    request:     Sanitised request payload (no secrets).
    result:      Execution result dict (may be None on failure).
    status:      "success" | "failure"
    error:       Human-readable error message (may be None).
    """
    try:
        entry = AssistantAuditLog(
            id=new_uuid(),
            org_id=org_id,
            user_id=user_id,
            session_id=session_id,
            action_name=action_name,
            risk_level=risk_level,
            request_json=request or {},
            result_json=result,
            status=status,
            error_message=error,
        )
        db.add(entry)
        await db.flush()
        logger.info(
            "audit: org_id=%s user_id=%s action=%s risk=%s status=%s",
            org_id,
            user_id,
            action_name,
            risk_level,
            status,
        )
    except Exception as exc:
        logger.error(
            "audit: failed to write log for action=%s org_id=%s: %s",
            action_name,
            org_id,
            exc,
        )


async def get_audit_logs(
    db: AsyncSession,
    org_id: str,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """
    Return the most recent audit log entries for an organisation.

    Returns an empty list on any database error.
    """
    try:
        result = await db.execute(
            select(AssistantAuditLog)
            .where(AssistantAuditLog.org_id == org_id)
            .order_by(AssistantAuditLog.created_at.desc())
            .limit(limit)
        )
        rows = result.scalars().all()
        return [_serialize_log(row) for row in rows]
    except Exception as exc:
        logger.error("audit: get_audit_logs failed for org_id=%s: %s", org_id, exc)
        return []


async def get_action_history(
    db: AsyncSession,
    session_id: str,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """
    Return the action history for a specific assistant session.

    Returns an empty list on any database error.
    """
    try:
        result = await db.execute(
            select(AssistantAuditLog)
            .where(AssistantAuditLog.session_id == session_id)
            .order_by(AssistantAuditLog.created_at.desc())
            .limit(limit)
        )
        rows = result.scalars().all()
        return [_serialize_log(row) for row in rows]
    except Exception as exc:
        logger.error(
            "audit: get_action_history failed for session_id=%s: %s", session_id, exc
        )
        return []


def _serialize_log(log: AssistantAuditLog) -> dict[str, Any]:
    return {
        "id": log.id,
        "org_id": log.org_id,
        "user_id": log.user_id,
        "session_id": log.session_id,
        "action_name": log.action_name,
        "risk_level": log.risk_level,
        "status": log.status,
        "error_message": log.error_message,
        "created_at": log.created_at.isoformat() if log.created_at else None,
    }
