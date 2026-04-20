from fastapi import APIRouter

router = APIRouter()


@router.get("/debug/leaflink")
def debug_leaflink():
    try:
        from leaflink_client import test_connection
        return test_connection()
    except Exception as e:
        return {
            "ok": False,
            "error": str(e),
            "message": "LeafLink debug route failed"
        }