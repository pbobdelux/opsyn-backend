from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
from enum import Enum


class WorkflowStatus(str, Enum):
    initiated = "initiated"
    questions_needed = "questions_needed"
    plan_ready = "plan_ready"
    awaiting_confirmation = "awaiting_confirmation"
    executing = "executing"
    completed = "completed"
    failed = "failed"
    cancelled = "cancelled"
    expired = "expired"


class StepStatus(str, Enum):
    pending = "pending"
    in_progress = "in_progress"
    completed = "completed"
    failed = "failed"
    skipped = "skipped"


class Severity(str, Enum):
    info = "info"
    warning = "warning"
    critical = "critical"


class QuestionType(str, Enum):
    select = "select"
    multi_select = "multi_select"
    text = "text"
    number = "number"
    boolean = "boolean"
    date = "date"


class Step(BaseModel):
    step_id: int
    action: str
    details: Dict[str, Any] = Field(default_factory=dict)
    status: StepStatus = StepStatus.pending
    completed_at: Optional[str] = None


class Warning(BaseModel):
    type: str
    message: str
    severity: Severity = Severity.warning
    affected_item_ids: Optional[List[str]] = None
    recommended_action: Optional[str] = None


class NextAction(BaseModel):
    action: str
    label: str


class Question(BaseModel):
    id: str
    text: str
    type: QuestionType
    options: Optional[List[str]] = None
    default: Optional[Any] = None


class ContextObj(BaseModel):
    screen: Optional[str] = None
    selected_items: Optional[List[str]] = None
    active_session_id: Optional[str] = None
    conversation_id: Optional[str] = None
    user_role: Optional[str] = None
    facility_license: Optional[str] = None


class WorkflowResponse(BaseModel):
    action_type: str
    status: WorkflowStatus
    requires_confirmation: bool = False
    requires_questions: bool = False
    questions: List[Question] = Field(default_factory=list)
    plan_summary: Optional[str] = None
    steps: List[Step] = Field(default_factory=list)
    warnings: List[Warning] = Field(default_factory=list)
    context: Dict[str, Any] = Field(default_factory=dict)
    result_summary: Optional[Dict[str, Any]] = None
    next_actions: List[NextAction] = Field(default_factory=list)
    ai_message: Optional[str] = None


class ProgressResponse(BaseModel):
    session_id: str
    status: str
    current_step: int = 0
    total_steps: int = 0
    step_description: Optional[str] = None
    percent_complete: int = 0
    estimated_remaining_seconds: Optional[int] = None
    completed_steps: List[Step] = Field(default_factory=list)


class ChatRequest(BaseModel):
    message: str
    context: Optional[ContextObj] = None
    conversation_id: Optional[str] = None


class CommandRequest(BaseModel):
    message: str
    context: Optional[ContextObj] = None
    conversation_id: Optional[str] = None


class TestingStartRequest(BaseModel):
    scanned_tags: List[str]
    context: Optional[ContextObj] = None


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