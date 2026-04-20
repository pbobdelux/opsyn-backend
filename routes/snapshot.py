from fastapi import APIRouter

router = APIRouter(prefix="/snapshot", tags=["snapshot"])

@router.get("")
def get_snapshot():
    return {
        "status": "ok",
        "message": "Snapshot endpoint is live"
    }