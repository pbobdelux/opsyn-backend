import logging
import uuid
from uuid import UUID
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
from services.organization_service import lookup_organization

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
        logger.info("[Auth] setup_passcode_start email=%s", email)

        # Find employee by email
        logger.info("[Auth] querying_employee email=%s", email)

        result = await db.execute(
            select(Employee).where(Employee.email == email)
        )
        employee = result.scalar_one_or_none()

        if not employee:
            logger.error("[Auth] employee_not_found email=%s", email)
            return {"ok": False, "error": "Employee not found"}

        logger.info(
            "[Auth] employee_found employee_id=%s name=%s %s",
            employee.id,
            employee.first_name,
            employee.last_name,
        )

        # Hash passcode
        logger.info("[Auth] hashing_passcode")
        passcode_hash = hash_passcode(passcode)
        logger.info("[Auth] passcode_hashed hash_length=%d", len(passcode_hash))

        # Create passcode record
        passcode_id = str(uuid.uuid4())
        new_passcode = EmployeePasscode(
            id=passcode_id,
            employee_id=employee.id,
            passcode_hash=passcode_hash,
            is_active=True,
        )

        logger.info(
            "[Auth] inserting_passcode passcode_id=%s employee_id=%s",
            passcode_id,
            employee.id,
        )

        db.add(new_passcode)
        await db.commit()

        logger.info(
            "[Auth] passcode_created passcode_id=%s employee_id=%s",
            passcode_id,
            employee.id,
        )

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
    org_id: str | None = None,
) -> dict:
    """
    Authenticate employee with passcode and return tenant context.

    Args:
        db: Database session
        passcode: Plain text passcode
        app_id: App ID (brand_app, driver_app, crm_app)
        org_id: Optional organization identifier (UUID or org_code like "noble")

    Returns:
        {
            ok: bool,
            employee: {id, name, email},
            organization: {id, org_code, slug, name},
            brand: {id, slug, name},
            app_access: {app_id, role},
            tenant_context: {org_id, brand_id, app_id, role},
        }
    """
    try:
        logger.info("[Auth] passcode_login_attempt app_id=%s org_id=%s", app_id, org_id)

        # Validate app_id is supported
        SUPPORTED_APPS = ["brand_app", "driver_app", "crm_app"]
        if app_id not in SUPPORTED_APPS:
            logger.warning("[Auth] unsupported_app_id app_id=%s", app_id)
            return {"ok": False, "error": f"Unsupported app_id: {app_id}. Supported: {', '.join(SUPPORTED_APPS)}"}

        logger.info("[Auth] app_id_supported app_id=%s", app_id)

        # Find all active passcodes
        logger.info("[Auth] querying_active_passcodes")

        result = await db.execute(
            select(EmployeePasscode).where(
                EmployeePasscode.is_active == True  # noqa: E712
            )
        )
        passcodes = result.scalars().all()

        logger.info("[Auth] found_passcodes count=%d", len(passcodes))

        # Find matching passcode
        matching_passcode = None
        for pc in passcodes:
            logger.debug("[Auth] comparing_passcode passcode_id=%s", pc.id)

            if verify_passcode(passcode, pc.passcode_hash):
                logger.info("[Auth] passcode_match passcode_id=%s", pc.id)
                matching_passcode = pc
                break

        if not matching_passcode:
            logger.warning("[Auth] passcode_invalid no_match")
            return {"ok": False, "error": "Invalid passcode"}

        logger.info("[Auth] passcode_verified passcode_id=%s", matching_passcode.id)

        # Get employee
        employee_id_str = str(matching_passcode.employee_id)
        logger.info("[Auth] extracted_employee_id employee_id=%s", employee_id_str)

        try:
            employee_uuid = UUID(employee_id_str)
            logger.info("[Auth] using_uuid employee_id=%s", employee_uuid)
        except (ValueError, TypeError) as uuid_exc:
            logger.error(
                "[Auth] invalid_employee_id_format employee_id=%s error=%s",
                employee_id_str,
                str(uuid_exc)[:200],
            )
            return {"ok": False, "error": "Invalid employee_id format"}

        logger.info("[Auth] querying_employee employee_uuid=%s", employee_uuid)

        result = await db.execute(
            select(Employee).where(Employee.id == employee_uuid)
        )
        employee = result.scalar_one_or_none()

        if not employee or not employee.is_active:
            logger.warning(
                "[Auth] employee_inactive employee_id=%s",
                employee.id if employee else "unknown",
            )
            return {"ok": False, "error": "Employee is inactive"}

        # VALIDATE EMPLOYEE NAME IS NOT EMPTY OR PLACEHOLDER
        employee_first_name = (employee.first_name or "").strip()
        employee_last_name = (employee.last_name or "").strip()

        if not employee_first_name or not employee_last_name:
            logger.warning(
                "[Auth] employee_missing_name employee_id=%s first_name=%s last_name=%s email=%s",
                employee.id,
                employee_first_name or "MISSING",
                employee_last_name or "MISSING",
                employee.email,
            )
            # Use email as fallback if name is missing
            employee_name = employee.email
        else:
            employee_name = f"{employee_first_name} {employee_last_name}"

        logger.info(
            "[Auth] employee_verified employee_id=%s name=%s email=%s",
            employee.id,
            employee_name,
            employee.email,
        )

        # Get organization — support both UUID and org_code
        if org_id:
            # Caller supplied an org identifier: validate it and confirm it matches the employee
            logger.info("[Auth] org_id_provided identifier=%s", org_id)
            org_lookup_result = await lookup_organization(db, org_id)
            if not org_lookup_result.get("ok"):
                logger.warning("[Auth] org_lookup_failed error=%s", org_lookup_result.get("error"))
                return {"ok": False, "error": org_lookup_result.get("error")}

            provided_org = org_lookup_result.get("organization")
            employee_org_uuid = str(employee.org_id)

            if str(provided_org.id) != employee_org_uuid:
                logger.warning(
                    "[Auth] org_mismatch employee_org=%s provided_org=%s",
                    employee_org_uuid,
                    provided_org.id,
                )
                return {"ok": False, "error": "Employee does not belong to this organization"}

            org = provided_org
            logger.info("[Auth] org_resolved_from_identifier org_id=%s org_code=%s", org.id, org.org_code)
        else:
            # No org_id supplied — derive from the employee record
            org_lookup_result = await lookup_organization(db, str(employee.org_id))
            if not org_lookup_result.get("ok"):
                logger.warning("[Auth] org_lookup_failed error=%s", org_lookup_result.get("error"))
                return {"ok": False, "error": org_lookup_result.get("error")}

            org = org_lookup_result.get("organization")
            logger.info("[Auth] org_resolved_from_employee org_id=%s org_code=%s", org.id, org.org_code)

        if not org.is_active:
            logger.warning("[Auth] org_inactive org_id=%s", org.id)
            return {"ok": False, "error": "Organization is inactive"}

        logger.info("[Auth] org_verified org_id=%s org_code=%s slug=%s", org.id, org.org_code, org.slug)

        # Get app access
        logger.info(
            "[Auth] querying_app_access employee_id=%s app_id=%s",
            employee.id,
            app_id,
        )

        result = await db.execute(
            select(EmployeeAppAccess).where(
                EmployeeAppAccess.employee_id == employee_uuid,
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

        logger.info("[Auth] app_access_verified app_id=%s role=%s", app_id, app_access.role)

        # Get brand access (first active brand for this employee)
        logger.info("[Auth] querying_brand_access employee_id=%s", employee.id)

        result = await db.execute(
            select(EmployeeBrandAccess)
            .where(
                EmployeeBrandAccess.employee_id == employee_uuid,
                EmployeeBrandAccess.is_active == True,  # noqa: E712
            )
            .limit(1)
        )
        brand_access = result.scalar_one_or_none()

        brand = None
        brand_id = None
        if brand_access:
            brand_id_str = str(brand_access.brand_id)
            logger.info("[Auth] querying_brand brand_id=%s", brand_id_str)

            try:
                brand_uuid = UUID(brand_id_str)
                logger.info("[Auth] using_uuid brand_id=%s", brand_uuid)
            except (ValueError, TypeError) as uuid_exc:
                logger.error(
                    "[Auth] invalid_brand_id_format brand_id=%s error=%s",
                    brand_id_str,
                    str(uuid_exc)[:200],
                )
                return {"ok": False, "error": "Invalid brand_id format"}

            result = await db.execute(
                select(Brand).where(Brand.id == brand_uuid)
            )
            brand = result.scalar_one_or_none()

            if brand:
                brand_id = str(brand.id)
                logger.info("[Auth] brand_verified brand_id=%s slug=%s", brand.id, brand.slug)
            else:
                logger.warning("[Auth] brand_not_found brand_id=%s", brand_id_str)
        else:
            logger.info("[Auth] no_brand_access employee_id=%s", employee.id)

        # BUILD TENANT CONTEXT
        tenant_context = {
            "org_id": str(org.id),
            "brand_id": brand_id,
            "app_id": app_access.app_id,
            "role": app_access.role,
        }

        logger.info(
            "[Auth] passcode_login_success employee_id=%s employee_name=%s app_id=%s org_id=%s brand_id=%s role=%s",
            employee.id,
            employee_name,
            app_access.app_id,
            org.id,
            brand_id,
            app_access.role,
        )

        return {
            "ok": True,
            "employee": {
                "id": str(employee.id),
                "name": employee_name,  # DYNAMIC FROM DATABASE
                "email": employee.email,
            },
            "organization": {
                "id": str(org.id),
                "org_code": org.org_code,
                "slug": org.slug,
                "name": org.name,
            },
            "brand": {
                "id": brand_id,
                "slug": brand.slug if brand else None,
                "name": brand.name if brand else None,
            } if brand else None,
            "app_access": {
                "app_id": app_access.app_id,
                "role": app_access.role,
            },
            "tenant_context": tenant_context,
        }

    except Exception as e:
        logger.error("[Auth] passcode_login_failed error=%s", str(e)[:500], exc_info=True)
        return {"ok": False, "error": str(e)[:500]}
