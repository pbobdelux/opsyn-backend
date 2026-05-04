import logging
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from database import get_db
from services.auth_service import passcode_login
from utils.json_utils import make_json_safe

logger = logging.getLogger("auth_routes")

router = APIRouter(prefix="/auth", tags=["auth"])


class PasscodeLoginRequest(BaseModel):
    passcode: str
    app_id: str


@router.post("/passcode-login")
async def login_with_passcode(
    request: PasscodeLoginRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Authenticate employee with passcode.

    Request:
    {
        "passcode": "1234",
        "app_id": "brand_app"
    }

    Response:
    {
        "ok": true,
        "employee": {id, name, email},
        "organization": {id, slug, name},
        "brand": {id, slug, name},
        "app_access": {app_id, role},
        "tenant_context": {org_id, brand_id, app_id, role}
    }
    """
    try:
        logger.info("[Auth] passcode_login_request app_id=%s", request.app_id)

        result = await passcode_login(db, request.passcode, request.app_id)

        if not result.get("ok"):
            logger.warning("[Auth] login_failed error=%s", result.get("error"))
            raise HTTPException(status_code=401, detail=result.get("error"))

        logger.info("[Auth] login_success app_id=%s", request.app_id)

        return make_json_safe(result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Auth] login_error error=%s", str(e)[:500], exc_info=True)
        raise HTTPException(status_code=500, detail="Login failed")


@router.post("/seed/preston")
async def seed_preston_endpoint(db: AsyncSession = Depends(get_db)):
    """
    Manual endpoint to seed Preston Anderson passcode.

    Use this to manually trigger seeding if automatic seed fails.
    """
    try:
        logger.info("[Auth] manual_seed_requested")

        from services.seed_auth import seed_preston_anderson

        result = await seed_preston_anderson(db)

        logger.info("[Auth] manual_seed_complete ok=%s", result.get("ok"))

        return result

    except Exception as e:
        logger.error("[Auth] manual_seed_failed error=%s", str(e)[:500], exc_info=True)
        raise HTTPException(status_code=500, detail="Seed failed")


@router.get("/health")
async def auth_health():
    """Health check for auth endpoints."""
    return {"ok": True, "service": "auth"}
