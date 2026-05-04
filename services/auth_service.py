import logging
import uuid
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from utils.auth_utils import hash_passcode, verify_passcode
from models.auth_models import (
    Employee,
    EmployeePasscode,
    EmployeeBrandAccess,
    EmployeeAppAccess,
    Organization,
    Brand,
)

logger = logging.getLogger("auth_service")


async def setup_employee_passcode(
    db: AsyncSession,
    email: str,
    passcode: str,
) -> dict:
    """
    Set up initial passcode for an employee.

    Args:
        db: Database session
        email: Employee email
        passcode: Plain text passcode (will be hashed)

    Returns:
        {ok: bool, employee_id: str, message: str}
    """
    try:
        # Find employee by email
        result = await db.execute(
            select(Employee).where(Employee.email == email)
        )
        employee = result.scalar_one_or_none()

        if not employee:
            logger.error("[Auth] employee_not_found email=%s", email)
            return {"ok": False, "error": "Employee not found"}

        # Hash passcode
        passcode_hash = hash_passcode(passcode)

        # Create passcode record
        passcode_id = str(uuid.uuid4())
        new_passcode = EmployeePasscode(
            id=passcode_id,
            employee_id=employee.id,
            passcode_hash=passcode_hash,
            is_active=True,
        )

        db.add(new_passcode)
        await db.commit()

        logger.info("[Auth] passcode_created employee_id=%s", employee.id)

        return {
            "ok": True,
            "employee_id": employee.id,
            "message": f"Passcode set for {employee.email}",
        }

    except Exception as e:
        logger.error("[Auth] setup_passcode_failed error=%s", str(e)[:500], exc_info=True)
        await db.rollback()
        return {"ok": False, "error": str(e)[:500]}


async def grant_brand_access(
    db: AsyncSession,
    employee_id: str,
    brand_slug: str,
    role: str = "admin",
) -> dict:
    """
    Grant employee access to a brand.

    Args:
        db: Database session
        employee_id: Employee ID
        brand_slug: Brand slug
        role: Access role (admin, editor, viewer)

    Returns:
        {ok: bool, message: str}
    """
    try:
        # Find brand by slug
        result = await db.execute(
            select(Brand).where(Brand.slug == brand_slug)
        )
        brand = result.scalar_one_or_none()

        if not brand:
            logger.error("[Auth] brand_not_found slug=%s", brand_slug)
            return {"ok": False, "error": "Brand not found"}

        # Create brand access record
        access_id = str(uuid.uuid4())
        brand_access = EmployeeBrandAccess(
            id=access_id,
            employee_id=employee_id,
            brand_id=brand.id,
            role=role,
            is_active=True,
        )

        db.add(brand_access)
        await db.commit()

        logger.info(
            "[Auth] brand_access_granted employee_id=%s brand_id=%s role=%s",
            employee_id,
            brand.id,
            role,
        )

        return {
            "ok": True,
            "message": f"Brand access granted: {brand_slug}",
        }

    except Exception as e:
        logger.error("[Auth] grant_brand_access_failed error=%s", str(e)[:500], exc_info=True)
        await db.rollback()
        return {"ok": False, "error": str(e)[:500]}


async def grant_app_access(
    db: AsyncSession,
    employee_id: str,
    app_id: str,
    role: str = "admin",
) -> dict:
    """
    Grant employee access to an app.

    Args:
        db: Database session
        employee_id: Employee ID
        app_id: App ID (brand_app, driver_app, crm_app)
        role: Access role (admin, editor, viewer)

    Returns:
        {ok: bool, message: str}
    """
    try:
        # Create app access record
        access_id = str(uuid.uuid4())
        app_access = EmployeeAppAccess(
            id=access_id,
            employee_id=employee_id,
            app_id=app_id,
            role=role,
            is_active=True,
        )

        db.add(app_access)
        await db.commit()

        logger.info(
            "[Auth] app_access_granted employee_id=%s app_id=%s role=%s",
            employee_id,
            app_id,
            role,
        )

        return {
            "ok": True,
            "message": f"App access granted: {app_id}",
        }

    except Exception as e:
        logger.error("[Auth] grant_app_access_failed error=%s", str(e)[:500], exc_info=True)
        await db.rollback()
        return {"ok": False, "error": str(e)[:500]}


async def passcode_login(
    db: AsyncSession,
    passcode: str,
    app_id: str,
) -> dict:
    """
    Authenticate employee with passcode and return tenant context.

    Args:
        db: Database session
        passcode: Plain text passcode
        app_id: App ID (brand_app, driver_app, crm_app)

    Returns:
        {
            ok: bool,
            employee: {id, name, email},
            organization: {id, slug, name},
            brand: {id, slug, name},
            app_access: {app_id, role},
            tenant_context: {org_id, brand_id, app_id, role},
        }
    """
    try:
        logger.info("[Auth] passcode_login_attempt app_id=%s", app_id)

        # Find all active passcodes and check for a match
        result = await db.execute(
            select(EmployeePasscode).where(
                EmployeePasscode.is_active == True  # noqa: E712
            )
        )
        passcodes = result.scalars().all()

        matching_passcode = None
        for pc in passcodes:
            if verify_passcode(passcode, pc.passcode_hash):
                matching_passcode = pc
                break

        if not matching_passcode:
            logger.warning("[Auth] passcode_invalid")
            return {"ok": False, "error": "Invalid passcode"}

        # Get employee
        result = await db.execute(
            select(Employee).where(Employee.id == matching_passcode.employee_id)
        )
        employee = result.scalar_one_or_none()

        if not employee or not employee.is_active:
            logger.warning(
                "[Auth] employee_inactive employee_id=%s",
                employee.id if employee else "unknown",
            )
            return {"ok": False, "error": "Employee is inactive"}

        # Get organization
        result = await db.execute(
            select(Organization).where(Organization.id == employee.org_id)
        )
        org = result.scalar_one_or_none()

        if not org or not org.is_active:
            logger.warning(
                "[Auth] org_inactive org_id=%s",
                org.id if org else "unknown",
            )
            return {"ok": False, "error": "Organization is inactive"}

        # Get app access
        result = await db.execute(
            select(EmployeeAppAccess).where(
                EmployeeAppAccess.employee_id == employee.id,
                EmployeeAppAccess.app_id == app_id,
                EmployeeAppAccess.is_active == True,  # noqa: E712
            )
        )
        app_access = result.scalar_one_or_none()

        if not app_access:
            logger.warning(
                "[Auth] app_access_denied employee_id=%s app_id=%s",
                employee.id,
                app_id,
            )
            return {"ok": False, "error": f"No access to {app_id}"}

        # Get brand access (first active brand for this employee)
        result = await db.execute(
            select(EmployeeBrandAccess)
            .where(
                EmployeeBrandAccess.employee_id == employee.id,
                EmployeeBrandAccess.is_active == True,  # noqa: E712
            )
            .limit(1)
        )
        brand_access = result.scalar_one_or_none()

        brand = None
        if brand_access:
            result = await db.execute(
                select(Brand).where(Brand.id == brand_access.brand_id)
            )
            brand = result.scalar_one_or_none()

        logger.info(
            "[Auth] passcode_login_success employee_id=%s app_id=%s",
            employee.id,
            app_id,
        )

        return {
            "ok": True,
            "employee": {
                "id": employee.id,
                "name": f"{employee.first_name} {employee.last_name}",
                "email": employee.email,
            },
            "organization": {
                "id": org.id,
                "slug": org.slug,
                "name": org.name,
            },
            "brand": {
                "id": brand.id if brand else None,
                "slug": brand.slug if brand else None,
                "name": brand.name if brand else None,
            } if brand else None,
            "app_access": {
                "app_id": app_access.app_id,
                "role": app_access.role,
            },
            "tenant_context": {
                "org_id": org.id,
                "brand_id": brand.id if brand else None,
                "app_id": app_access.app_id,
                "role": app_access.role,
            },
        }

    except Exception as e:
        logger.error("[Auth] passcode_login_failed error=%s", str(e)[:500], exc_info=True)
        return {"ok": False, "error": str(e)[:500]}
