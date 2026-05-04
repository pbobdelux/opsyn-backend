import logging
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession
from database import get_db
from models.auth_models import Employee
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


@router.get("/debug/employees")
async def debug_employees(db: AsyncSession = Depends(get_db)):
    """
    Debug endpoint to inspect employee table in connected database.

    Returns all employees and their details.
    """
    try:
        logger.info("[Auth] debug_employees_requested")

        # Get current database
        result = await db.execute(text("SELECT current_database()"))
        current_db = result.scalar()

        # Get all employees
        result = await db.execute(
            select(Employee).order_by(Employee.email)
        )
        employees = result.scalars().all()

        employees_data = []
        for emp in employees:
            employees_data.append({
                "id": emp.id,
                "email": emp.email,
                "name": f"{emp.first_name} {emp.last_name}",
                "org_id": emp.org_id,
                "is_active": emp.is_active,
            })

        logger.info(
            "[Auth] debug_employees_complete database=%s count=%d",
            current_db,
            len(employees_data),
        )

        return {
            "ok": True,
            "database": current_db,
            "employee_count": len(employees_data),
            "employees": employees_data,
        }

    except Exception as e:
        logger.error("[Auth] debug_employees_failed error=%s", str(e)[:500], exc_info=True)
        return {
            "ok": False,
            "error": str(e)[:500],
            "database": "unknown",
            "employee_count": 0,
            "employees": [],
        }


@router.get("/debug/db")
async def debug_db(db: AsyncSession = Depends(get_db)):
    """
    Debug endpoint to inspect full database state for auth tables.

    Returns counts and IDs for employees, passcodes, app access, and brand access.
    """
    try:
        logger.info("[Auth] debug_db_requested")

        # Get current database
        result = await db.execute(text("SELECT current_database()"))
        current_db = result.scalar()

        # Count employees
        result = await db.execute(text("SELECT COUNT(*) FROM employees"))
        employee_count = result.scalar() or 0

        # Count passcodes
        result = await db.execute(text("SELECT COUNT(*) FROM employee_passcodes"))
        passcode_count = result.scalar() or 0

        # Count app access
        result = await db.execute(text("SELECT COUNT(*) FROM employee_app_access"))
        app_access_count = result.scalar() or 0

        # Count brand access
        result = await db.execute(text("SELECT COUNT(*) FROM employee_brand_access"))
        brand_access_count = result.scalar() or 0

        # Get all employee emails
        result = await db.execute(text("SELECT id, email FROM employees ORDER BY email"))
        employees = [{"id": row[0], "email": row[1]} for row in result.fetchall()]

        # Get all passcode employee IDs
        result = await db.execute(text("SELECT id, employee_id, is_active FROM employee_passcodes"))
        passcodes = [{"id": row[0], "employee_id": row[1], "is_active": row[2]} for row in result.fetchall()]

        logger.info(
            "[Auth] debug_db_complete database=%s employees=%d passcodes=%d app_access=%d brand_access=%d",
            current_db,
            employee_count,
            passcode_count,
            app_access_count,
            brand_access_count,
        )

        return {
            "ok": True,
            "database": current_db,
            "employee_count": employee_count,
            "passcode_count": passcode_count,
            "app_access_count": app_access_count,
            "brand_access_count": brand_access_count,
            "employees": employees,
            "passcodes": passcodes,
        }

    except Exception as e:
        logger.error("[Auth] debug_db_failed error=%s", str(e)[:500], exc_info=True)
        return {
            "ok": False,
            "error": str(e)[:500],
        }


@router.get("/health")
async def auth_health():
    """Health check for auth endpoints."""
    return {"ok": True, "service": "auth"}
