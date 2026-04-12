from fastapi import APIRouter
from leaflink_client import test_connection

router = APIRouter()


@router.get("/debug/leaflink")
def debug_leaflink():
    return test_connection()