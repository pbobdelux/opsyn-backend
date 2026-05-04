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
async def debug_database(db: AsyncSession = Depends(get_db)):
    """
    Comprehensive database diagnostics endpoint.

    Returns:
    - current_database: Name of connected database
    - current_schema: Current schema
    - database_host: Host (password hidden)
    - employee_count: Total employees
    - employee_emails: List of all employee emails
    - passcode_count: Total passcodes
    - app_access_count: Total app access records
    - brand_access_count: Total brand access records
    """
    try:
        logger.info("[Auth] debug_db_requested")

        # Get current database
        result = await db.execute(text("SELECT current_database()"))
        current_db = result.scalar()

        # Get current schema
        result = await db.execute(text("SELECT current_schema()"))
        current_schema = result.scalar()

        # Get database host from connection
        result = await db.execute(text("SELECT inet_server_addr()"))
        db_host = result.scalar()

        # Count employees
        result = await db.execute(text("SELECT COUNT(*) FROM employees"))
        employee_count = result.scalar() or 0

        # Get all employee emails
        result = await db.execute(text("SELECT email FROM employees ORDER BY email"))
        employee_emails = [row[0] for row in result.fetchall()]

        # Count passcodes
        result = await db.execute(text("SELECT COUNT(*) FROM employee_passcodes"))
        passcode_count = result.scalar() or 0

        # Count app access
        result = await db.execute(text("SELECT COUNT(*) FROM employee_app_access"))
        app_access_count = result.scalar() or 0

        # Count brand access
        result = await db.execute(text("SELECT COUNT(*) FROM employee_brand_access"))
        brand_access_count = result.scalar() or 0

        # Get DATABASE_URL from environment (with password hidden)
        import os
        db_url = os.getenv("DATABASE_URL", "NOT_SET")
        if db_url != "NOT_SET":
            # Hide password
            if "@" in db_url:
                parts = db_url.split("@")
                user_part = parts[0].split("://")[1]
                user_only = user_part.split(":")[0]
                db_url_safe = f"postgresql://{user_only}:***@{parts[1]}"
            else:
                db_url_safe = db_url
        else:
            db_url_safe = "NOT_SET"

        logger.info(
            "[Auth] debug_db_complete database=%s host=%s employees=%d passcodes=%d",
            current_db,
            db_host,
            employee_count,
            passcode_count,
        )

        return {
            "ok": True,
            "database": {
                "current_database": current_db,
                "current_schema": current_schema,
                "database_host": str(db_host) if db_host else "unknown",
                "database_url": db_url_safe,
            },
            "tables": {
                "employee_count": employee_count,
                "employee_emails": employee_emails,
                "passcode_count": passcode_count,
                "app_access_count": app_access_count,
                "brand_access_count": brand_access_count,
            },
            "expected": {
                "employee_count": 1,
                "employee_emails": ["preston@noble420.com"],
                "passcode_count": 1,
                "app_access_count": 3,
                "brand_access_count": 1,
            },
            "status": {
                "employees_match": employee_count == 1 and "preston@noble420.com" in employee_emails,
                "passcodes_match": passcode_count == 1,
                "app_access_match": app_access_count == 3,
                "brand_access_match": brand_access_count == 1,
                "all_match": (
                    employee_count == 1
                    and "preston@noble420.com" in employee_emails
                    and passcode_count == 1
                    and app_access_count == 3
                    and brand_access_count == 1
                ),
            },
        }

    except Exception as e:
        logger.error("[Auth] debug_db_failed error=%s", str(e)[:500], exc_info=True)
        return {
            "ok": False,
            "error": str(e)[:500],
            "database": {
                "current_database": "unknown",
                "current_schema": "unknown",
                "database_host": "unknown",
                "database_url": "unknown",
            },
            "tables": {
                "employee_count": 0,
                "employee_emails": [],
                "passcode_count": 0,
                "app_access_count": 0,
                "brand_access_count": 0,
            },
            "status": {
                "all_match": False,
            },
        }


@router.get("/health")
async def auth_health():
    """Health check for auth endpoints."""
    return {"ok": True, "service": "auth"}
