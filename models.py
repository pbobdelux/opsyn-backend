from pydantic import BaseModel
from typing import Optional, List, Dict, Any

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, Integer, DateTime, Text
from datetime import datetime


# =========================
# SQLAlchemy Base (DATABASE)
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


# =========================
# Pydantic Models (API)
# =========================

class ContextObj(BaseModel):
    user: Optional[str] = None
    facility: Optional[str] = None


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