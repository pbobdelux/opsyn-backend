from fastapi import APIRouter

router = APIRouter(prefix="/routes", tags=["routes"])

@router.get("")
def get_routes():
    return {
        "items": [],
        "count": 0,
        "message": "Routes endpoint is live"
    }