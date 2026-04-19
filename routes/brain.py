from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional, Dict

router = APIRouter(prefix="/api/brain", tags=["brain"])

class BrainRequest(BaseModel):
    question: str
    context: Optional[Dict] = {}

@router.post("/ask")
def ask_opsyn(data: BrainRequest):

    question = data.question.lower()
    context = data.context or {}

    screen = context.get("screen", "unknown")
    user_type = context.get("user_type", "unknown")
    name = context.get("name", "Operator")

    # 🔥 SIMPLE INTELLIGENCE (we'll evolve this fast)
    if "what should i do next" in question:

        if user_type == "driver":
            return {
                "response": f"{name}, check your next stop and confirm payment + signature.",
                "actions": [
                    {"label": "Go to Route", "screen": "route"},
                    {"label": "Open Collections", "screen": "collections"}
                ]
            }

        if user_type == "compliance":
            return {
                "response": f"{name}, focus on your highest priority CAPA or expired training.",
                "actions": [
                    {"label": "Open CAPAs", "screen": "capas"},
                    {"label": "Review Training", "screen": "training"}
                ]
            }

    # fallback
    return {
        "response": f"{name}, I'm learning your workflow. Ask me anything.",
        "actions": []
    }