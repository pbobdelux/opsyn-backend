from fastapi import APIRouter
from typing import Dict, Any
from services.brain import process_brain_request

router = APIRouter()

@router.post("/api/brain/ask")
async def ask_opsyn(payload: Dict[str, Any]):
    return await process_brain_request(payload)