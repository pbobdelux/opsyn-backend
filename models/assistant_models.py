"""
SQLAlchemy models for the Opsyn AI assistant layer.

Tables:
  - assistant_sessions       — conversation sessions per org/user/device
  - assistant_messages       — individual messages within a session
  - assistant_pending_actions — actions awaiting user confirmation
  - assistant_audit_logs     — immutable audit trail for all executed actions
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, ForeignKey, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from database import Base


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def new_uuid() -> str:
    return str(uuid.uuid4())


class AssistantSession(Base):
    __tablename__ = "assistant_sessions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    org_id: Mapped[str] = mapped_column(String(120), index=True, nullable=False)
    user_id: Mapped[str | None] = mapped_column(String(120), nullable=True)
    # brand_app | pos_app | grow_app | compliance_app | routes_tab | orders_tab
    app_context: Mapped[str | None] = mapped_column(String(80), nullable=True)
    device_id: Mapped[str | None] = mapped_column(String(120), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now
    )
    metadata_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    messages: Mapped[list["AssistantMessage"]] = relationship(
        "AssistantMessage",
        back_populates="session",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    pending_actions: Mapped[list["AssistantPendingAction"]] = relationship(
        "AssistantPendingAction",
        back_populates="session",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    audit_logs: Mapped[list["AssistantAuditLog"]] = relationship(
        "AssistantAuditLog",
        back_populates="session",
        passive_deletes=True,
    )


class AssistantMessage(Base):
    __tablename__ = "assistant_messages"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    session_id: Mapped[str] = mapped_column(
        ForeignKey("assistant_sessions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    # user | assistant
    role: Mapped[str] = mapped_column(String(20), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    # text | voice
    input_type: Mapped[str] = mapped_column(String(20), nullable=False, default="text")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )

    session: Mapped["AssistantSession"] = relationship(
        "AssistantSession", back_populates="messages"
    )


class AssistantPendingAction(Base):
    __tablename__ = "assistant_pending_actions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    confirmation_id: Mapped[str] = mapped_column(
        String(36), unique=True, index=True, nullable=False, default=new_uuid
    )
    session_id: Mapped[str] = mapped_column(
        ForeignKey("assistant_sessions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    org_id: Mapped[str] = mapped_column(String(120), index=True, nullable=False)
    user_id: Mapped[str | None] = mapped_column(String(120), nullable=True)
    action_name: Mapped[str] = mapped_column(String(120), nullable=False)
    payload_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    # safe | medium | high
    risk_level: Mapped[str] = mapped_column(String(20), nullable=False, default="medium")
    # pending | approved | cancelled | executed | failed
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    executed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    session: Mapped["AssistantSession"] = relationship(
        "AssistantSession", back_populates="pending_actions"
    )


class AssistantAuditLog(Base):
    __tablename__ = "assistant_audit_logs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    org_id: Mapped[str] = mapped_column(String(120), index=True, nullable=False)
    user_id: Mapped[str | None] = mapped_column(String(120), nullable=True)
    session_id: Mapped[str | None] = mapped_column(
        ForeignKey("assistant_sessions.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    action_name: Mapped[str] = mapped_column(String(120), nullable=False)
    risk_level: Mapped[str] = mapped_column(String(20), nullable=False)
    request_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    result_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    # success | failure
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )

    session: Mapped["AssistantSession | None"] = relationship(
        "AssistantSession", back_populates="audit_logs"
    )
