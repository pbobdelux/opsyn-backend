from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel
from sqlalchemy import DateTime, Integer, String, Text, Float
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


# =========================
# SQLALCHEMY DATABASE MODELS
# =========================

class Base(DeclarativeBase):
    pass


class UploadedOrder(Base):
    __tablename__ = "uploaded_orders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    customer_name: Mapped[str] = mapped_column(String(255), nullable=False)
    source_filename: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(50), default="uploaded")
    raw_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class ProductCatalog(Base):
    __tablename__ = "product_catalog"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    sku: Mapped[str] = mapped_column(String(100), unique=True, nullable=False, index=True)
    product_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    brand: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    unit_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    active: Mapped[str] = mapped_column(String(10), default="yes")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class ConfirmedOrderLine(Base):
    __tablename__ = "confirmed_order_lines"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    uploaded_order_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    sku: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    product_name: Mapped[str] = mapped_column(String(255), nullable=False)
    quantity: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    unit_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    customer_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    source_row_index: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


# =========================
# ENUMS
# =========================

class WorkflowStatus(str, Enum):
    questions_needed = "questions_needed"
    plan_ready = "plan_ready"
    executing = "executing"
    completed = "completed"
    failed = "failed"


class Severity(str, Enum):
    info = "info"
    warning = "warning"
    error = "error"


# =========================
# COMMON RESPONSE MODELS
# =========================

class Warning(BaseModel):
    type: str
    message: str
    severity: Severity
    affected_item_ids: Optional[List[str]] = None


class Step(BaseModel):
    action: str
    status: Optional[str] = "pending"
    description: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class Question(BaseModel):
    id: str
    text: str
    type: str
    options: Optional[List[str]] = None
    default: Optional[Any] = None


class NextAction(BaseModel):
    action: str
    label: str


class WorkflowResponse(BaseModel):
    action_type: str
    status: str
    requires_confirmation: Optional[bool] = False
    requires_questions: Optional[bool] = False
    plan_summary: Optional[str] = None
    result_summary: Optional[Dict[str, Any]] = None
    steps: Optional[List[Step]] = None
    warnings: Optional[List[Warning]] = None
    questions: Optional[List[Question]] = None
    next_actions: Optional[List[NextAction]] = None
    context: Optional[Dict[str, Any]] = None


class ProgressResponse(BaseModel):
    session_id: str
    status: str
    current_step: int
    total_steps: int
    step_description: Optional[str] = ""
    percent_complete: int
    estimated_remaining_seconds: Optional[int] = 0


# =========================
# AI / COMMAND MODELS
# =========================

class ChatContext(BaseModel):
    screen: Optional[str] = None


class ChatRequest(BaseModel):
    message: str
    conversation_id: Optional[str] = None
    context: Optional[ChatContext] = None


class CommandContext(BaseModel):
    selected_items: Optional[List[str]] = None


class CommandRequest(BaseModel):
    message: str
    context: Optional[CommandContext] = None


# =========================
# WORKFLOW REQUEST MODELS
# =========================

class ContextObj(BaseModel):
    user: Optional[str] = None
    facility: Optional[str] = None


class TestingStartRequest(BaseModel):
    scanned_tags: List[str]


class TestingAnswerRequest(BaseModel):
    session_id: str
    answers: Dict[str, Any]


class ConfirmRequest(BaseModel):
    session_id: str


class RoutingOptimizeRequest(BaseModel):
    date: str
    max_drivers: int = 4
    max_per_route: float = 7.5
    depot: Optional[Dict[str, str]] = None
    context: Optional[ContextObj] = None


class AddStopRequest(BaseModel):
    session_id: Optional[str] = None
    order_id: str
    date: Optional[str] = None
    context: Optional[ContextObj] = None


class AuditStartRequest(BaseModel):
    scanned_tags: List[str]
    expected_counts: Dict[str, float]
    context: Optional[ContextObj] = None


class InventoryAnalyzeRequest(BaseModel):
    tag: str
    context: Optional[ContextObj] = None


class InventoryFixRequest(BaseModel):
    session_id: str
    fixes: List[Dict[str, Any]]


class ScanValidateRequest(BaseModel):
    tag: str
    context: Optional[ContextObj] = None


class BulkValidateRequest(BaseModel):
    tags: List[str]
    context: Optional[ContextObj] = None


# =========================
# ORDER REVIEW REQUEST MODELS
# =========================

class ReviewCorrection(BaseModel):
    row_index: int
    catalog_id: int


class ConfirmOrderRequest(BaseModel):
    uploaded_order_id: int
    corrections: List[ReviewCorrection] = []