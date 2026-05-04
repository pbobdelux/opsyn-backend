import logging
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from services.auth_service import (
    setup_employee_passcode,
    grant_brand_access,
    grant_app_access,
)
from models.auth_models import Employee, EmployeePasscode

logger = logging.getLogger("seed_auth")


async def seed_preston_anderson(db: AsyncSession) -> dict:
    """
    Seed Preston Anderson with passcode and app access.

    This function is idempotent — checks if passcode already exists
    before creating a new one.

    Returns:
        {ok: bool, message: str, employee_id: str}
    """
    try:
        logger.info("[Seed] starting_preston_anderson_setup")

        # 1. Look up employee by email
        email = "preston@noble420.com"
        logger.info("[Seed] looking_up_employee email=%s", email)

        # First, verify we can query the employees table
        from sqlalchemy import text
        result = await db.execute(text("SELECT COUNT(*) FROM employees"))
        employee_count = result.scalar() or 0
        logger.info("[Seed] employees_table_count=%d", employee_count)

        # Get all emails for debugging
        result = await db.execute(text("SELECT email FROM employees ORDER BY email"))
        all_emails = [row[0] for row in result.fetchall()]
        logger.info("[Seed] all_employee_emails=%s", all_emails)

        # Now look up Preston
        result = await db.execute(
            select(Employee).where(Employee.email == email)
        )
        employee = result.scalar_one_or_none()

        if not employee:
            logger.error(
                "[Seed] employee_not_found email=%s available_emails=%s",
                email,
                all_emails,
            )
            return {
                "ok": False,
                "error": f"Employee not found: {email}",
                "available_emails": all_emails,
            }

        logger.info(
            "[Seed] employee_found employee_id=%s name=%s %s",
            employee.id,
            employee.first_name,
            employee.last_name,
        )

        # 2. Check if passcode already exists
        logger.info("[Seed] checking_existing_passcodes employee_id=%s", employee.id)

        result = await db.execute(
            select(EmployeePasscode).where(
                EmployeePasscode.employee_id == employee.id,
                EmployeePasscode.is_active == True,  # noqa: E712
            )
        )
        existing_passcode = result.scalar_one_or_none()

        if existing_passcode:
            logger.info(
                "[Seed] passcode_already_exists employee_id=%s passcode_id=%s",
                employee.id,
                existing_passcode.id,
            )
            return {
                "ok": True,
                "message": "Passcode already exists",
                "employee_id": employee.id,
            }

        # 3. Set up passcode
        logger.info("[Seed] creating_passcode employee_id=%s", employee.id)

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

        # 4. Grant brand access
        logger.info("[Seed] granting_brand_access employee_id=%s brand=noble-nectar", employee_id)

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

        # 5. Grant app access
        apps = ["brand_app", "driver_app", "crm_app"]
        for app_id in apps:
            logger.info("[Seed] granting_app_access employee_id=%s app_id=%s", employee_id, app_id)

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

        logger.info("[Seed] preston_anderson_setup_complete employee_id=%s", employee_id)

        return {
            "ok": True,
            "message": "Preston Anderson setup complete",
            "employee_id": employee_id,
        }

    except Exception as e:
        logger.error("[Seed] setup_failed error=%s", str(e)[:500], exc_info=True)
        return {"ok": False, "error": str(e)[:500]}
