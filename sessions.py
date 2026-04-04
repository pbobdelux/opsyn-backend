import uuid
import threading
from datetime import datetime, timedelta
from typing import Dict, Any, Optional


class Session:
    def __init__(self, workflow_type: str, data: Optional[Dict[str, Any]] = None):
        self.id = str(uuid.uuid4())
        self.workflow_type = workflow_type
        self.status = "initiated"
        self.data = data or {}
        self.warnings = []
        self.questions = []
        self.answers = {}
        self.result = None
        self.documents = []
        self.current_step = 0
        self.total_steps = 0
        self.steps = []
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        self.expires_at = datetime.utcnow() + timedelta(hours=24)

    def is_expired(self) -> bool:
        return datetime.utcnow() > self.expires_at

    def to_dict(self):
        return {
            "session_id": self.id,
            "workflow_type": self.workflow_type,
            "status": self.status,
            "current_step": self.current_step,
            "total_steps": self.total_steps,
        }


class SessionStore:
    def __init__(self):
        self._sessions: Dict[str, Session] = {}
        self._lock = threading.Lock()

    def create(self, workflow_type: str, data: Optional[Dict[str, Any]] = None) -> Session:
        with self._lock:
            session = Session(workflow_type, data)
            self._sessions[session.id] = session
            return session

    def get(self, session_id: str) -> Optional[Session]:
        with self._lock:
            session = self._sessions.get(session_id)
            if session and session.is_expired():
                del self._sessions[session_id]
                return None
            return session

    def update(self, session_id: str, **kwargs):
        with self._lock:
            session = self._sessions.get(session_id)
            if session:
                for k, v in kwargs.items():
                    setattr(session, k, v)
                session.updated_at = datetime.utcnow()

    def cleanup_expired(self):
        with self._lock:
            expired = [sid for sid, s in self._sessions.items() if s.is_expired()]
            for sid in expired:
                del self._sessions[sid]


store = SessionStore()