import logging
from sqlalchemy.ext.asyncio import AsyncSession
from services.auth_service import (
    setup_employee_passcode,
    grant_brand_access,
    grant_app_access,
)

logger = logging.getLogger("seed_auth")


async def seed_preston_anderson(db: AsyncSession) -> dict:
    """
    Seed Preston Anderson with passcode and app access.

    This function is idempotent — if the passcode or access records already
    exist the underlying service calls will return an error and the seed
    will report the reason without raising.

    Returns:
        {ok: bool, message: str}
    """
    try:
        logger.info("[Seed] starting_preston_anderson_setup")

        # 1. Set up passcode
        result = await setup_employee_passcode(
            db,
            email="preston@noble420.com",
            passcode="1234",
        )

        if not result.get("ok"):
            logger.error("[Seed] passcode_setup_failed error=%s", result.get("error"))
            return result

        employee_id = result.get("employee_id")
        logger.info("[Seed] passcode_created employee_id=%s", employee_id)

        # 2. Grant brand access
        result = await grant_brand_access(
            db,
            employee_id=employee_id,
            brand_slug="noble-nectar",
            role="admin",
        )

        if not result.get("ok"):
            logger.error("[Seed] brand_access_failed error=%s", result.get("error"))
            return result

        logger.info("[Seed] brand_access_granted")

        # 3. Grant app access for all apps
        apps = ["brand_app", "driver_app", "crm_app"]
        for app_id in apps:
            result = await grant_app_access(
                db,
                employee_id=employee_id,
                app_id=app_id,
                role="admin",
            )

            if not result.get("ok"):
                logger.error(
                    "[Seed] app_access_failed app_id=%s error=%s",
                    app_id,
                    result.get("error"),
                )
                return result

            logger.info("[Seed] app_access_granted app_id=%s", app_id)

        logger.info("[Seed] preston_anderson_setup_complete")

        return {
            "ok": True,
            "message": "Preston Anderson setup complete",
            "employee_id": employee_id,
        }

    except Exception as e:
        logger.error("[Seed] setup_failed error=%s", str(e)[:500], exc_info=True)
        return {"ok": False, "error": str(e)[:500]}
