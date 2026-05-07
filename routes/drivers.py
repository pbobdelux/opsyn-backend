import logging
import random
from datetime import datetime, timezone
from typing import Any, Optional

import bcrypt
from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel, field_validator
from sqlalchemy import func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models.driver import Driver
from utils.json_utils import make_json_safe

logger = logging.getLogger("drivers")

router = APIRouter(prefix="/drivers", tags=["drivers"])


# =============================================================================
# Schemas
# =============================================================================


class DriverCreate(BaseModel):
    name: str
    email: Optional[str] = None
    phone: Optional[str] = None
    license_plate: Optional[str] = None
    vehicle_type: Optional[str] = None
    notes: Optional[str] = None
    preferences: Optional[dict] = None

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("name is required")
        if len(v) > 255:
            raise ValueError("name must be 255 characters or fewer")
        return v

    @field_validator("email")
    @classmethod
    def validate_email(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        v = v.strip()
        if len(v) > 255:
            raise ValueError("email must be 255 characters or fewer")
        return v

    @field_validator("phone")
    @classmethod
    def validate_phone(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        if len(v) > 50:
            raise ValueError("phone must be 50 characters or fewer")
        return v

    @field_validator("license_plate")
    @classmethod
    def validate_license_plate(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        if len(v) > 50:
            raise ValueError("license_plate must be 50 characters or fewer")
        return v

    @field_validator("vehicle_type")
    @classmethod
    def validate_vehicle_type(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        if len(v) > 50:
            raise ValueError("vehicle_type must be 50 characters or fewer")
        return v

    @field_validator("preferences")
    @classmethod
    def validate_preferences(cls, v: Optional[Any]) -> Optional[dict]:
        if v is None:
            return v
        if not isinstance(v, dict):
            raise ValueError("preferences must be a JSON object")
        return v


class DriverUpdate(BaseModel):
    name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    license_plate: Optional[str] = None
    vehicle_type: Optional[str] = None
    notes: Optional[str] = None
    preferences: Optional[dict] = None
    status: Optional[str] = None

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        v = v.strip()
        if not v:
            raise ValueError("name cannot be empty")
        if len(v) > 255:
            raise ValueError("name must be 255 characters or fewer")
        return v

    @field_validator("email")
    @classmethod
    def validate_email(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        v = v.strip()
        if len(v) > 255:
            raise ValueError("email must be 255 characters or fewer")
        return v

    @field_validator("phone")
    @classmethod
    def validate_phone(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        if len(v) > 50:
            raise ValueError("phone must be 50 characters or fewer")
        return v

    @field_validator("license_plate")
    @classmethod
    def validate_license_plate(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        if len(v) > 50:
            raise ValueError("license_plate must be 50 characters or fewer")
        return v

    @field_validator("vehicle_type")
    @classmethod
    def validate_vehicle_type(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        if len(v) > 50:
            raise ValueError("vehicle_type must be 50 characters or fewer")
        return v

    @field_validator("preferences")
    @classmethod
    def validate_preferences(cls, v: Optional[Any]) -> Optional[dict]:
        if v is None:
            return v
        if not isinstance(v, dict):
            raise ValueError("preferences must be a JSON object")
        return v

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        if v not in ("active", "inactive"):
            raise ValueError("status must be 'active' or 'inactive'")
        return v


class PasscodeReset(BaseModel):
    passcode: str

    @field_validator("passcode")
    @classmethod
    def validate_passcode(cls, v: str) -> str:
        if not v.isdigit():
            raise ValueError("passcode must contain only numeric digits (0-9)")
        if len(v) < 4 or len(v) > 8:
            raise ValueError("passcode must be between 4 and 8 digits")
        return v


# =============================================================================
# Helpers
# =============================================================================


def _serialize_driver(driver: Driver) -> dict:
    """
    Convert a Driver ORM instance to a JSON-safe dict.

    NEVER includes passcode_hash — that field is intentionally omitted.
    Uses make_json_safe() to handle UUID and datetime serialization.
    """
    deactivated_at = getattr(driver, "deactivated_at", None)
    return make_json_safe({
        "id": driver.id,
        "org_id": driver.org_id,
        "name": driver.name or "",
        "email": driver.email,
        "phone": driver.phone,
        "status": driver.status or "active",
        "license_plate": driver.license_plate,
        "vehicle_type": driver.vehicle_type,
        "notes": driver.notes,
        "preferences": driver.preferences if driver.preferences is not None else {},
        "created_at": driver.created_at,
        "updated_at": driver.updated_at,
        "deactivated_at": deactivated_at,
    })


def _hash_passcode(plain: str) -> str:
    """Hash a plain-text passcode with bcrypt (cost=12). Never logs the plain value."""
    return bcrypt.hashpw(plain.encode(), bcrypt.gensalt(rounds=12)).decode()


async def _get_org_from_header(
    x_opsyn_org: Optional[str],
    x_opsyn_secret: Optional[str],
    db: AsyncSession,
) -> str:
    """
    Resolve and authenticate the org from X-OPSYN-ORG / X-OPSYN-SECRET headers.

    Returns the authenticated org_id string.
    Raises HTTP 401 if the header is missing or credentials are invalid.
    """
    from services.tenant_auth import get_authenticated_org

    if not x_opsyn_org:
        raise HTTPException(status_code=401, detail="X-OPSYN-ORG header is required")

    # get_authenticated_org validates both headers against the DB.
    # Pass x_opsyn_org as the path org_id so the grace-period logic works.
    return await get_authenticated_org(x_opsyn_org, x_opsyn_secret, x_opsyn_org, db)


async def _get_driver_or_404(db: AsyncSession, driver_id: str, org_id: str) -> Driver:
    """Fetch a driver by ID scoped to org_id, or raise 404."""
    result = await db.execute(
        select(Driver).where(
            Driver.id == driver_id,
            Driver.org_id == org_id,
        )
    )
    driver = result.scalar_one_or_none()
    if driver is None:
        raise HTTPException(status_code=404, detail="Driver not found")
    return driver


# =============================================================================
# Endpoints
# =============================================================================


@router.get("")
async def list_drivers(
    x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
    x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
    status: Optional[str] = Query(default="active", description="Filter by status"),
    search: Optional[str] = Query(default=None, description="Search name, email, or phone"),
    limit: int = Query(default=100, ge=1, le=1000, description="Pagination limit"),
    offset: int = Query(default=0, ge=0, description="Pagination offset"),
    db: AsyncSession = Depends(get_db),
):
    """List all drivers for the org with optional filtering and pagination."""
    org_id = await _get_org_from_header(x_opsyn_org, x_opsyn_secret, db)

    query = select(Driver).where(Driver.org_id == org_id)

    if status:
        query = query.where(Driver.status == status)

    if search:
        term = f"%{search}%"
        query = query.where(
            or_(
                Driver.name.ilike(term),
                Driver.email.ilike(term),
                Driver.phone.ilike(term),
            )
        )

    # Total count (before pagination)
    count_query = select(func.count()).select_from(query.subquery())
    total_result = await db.execute(count_query)
    total = total_result.scalar_one()

    # Paginated results
    query = query.order_by(Driver.created_at.desc()).offset(offset).limit(limit)
    result = await db.execute(query)
    drivers = result.scalars().all()

    serialized = [_serialize_driver(d) for d in drivers]

    return {
        "ok": True,
        "org_id": org_id,
        "drivers": serialized,
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@router.post("")
async def create_driver(
    body: DriverCreate,
    x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
    x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
    db: AsyncSession = Depends(get_db),
):
    """Create a new driver scoped to the org."""
    org_id = await _get_org_from_header(x_opsyn_org, x_opsyn_secret, db)

    driver = Driver(
        org_id=org_id,
        name=body.name,
        email=body.email or None,
        phone=body.phone or None,
        license_plate=body.license_plate or None,
        vehicle_type=body.vehicle_type or None,
        notes=body.notes or None,
        preferences=body.preferences if body.preferences is not None else {},
        status="active",
    )
    db.add(driver)

    try:
        await db.commit()
        await db.refresh(driver)
    except IntegrityError as exc:
        await db.rollback()
        err = str(exc).lower()
        if "uq_drivers_org_email" in err or "unique" in err and "email" in err:
            raise HTTPException(
                status_code=409,
                detail="A driver with this email already exists in your organization",
            )
        logger.error("[DRIVER_CREATED] db_integrity_error org_id=%s error=%s", org_id, exc)
        raise HTTPException(status_code=400, detail="Could not create driver due to a data conflict")
    except Exception as exc:
        await db.rollback()
        logger.error("[DRIVER_CREATED] unexpected_error org_id=%s error=%s", org_id, exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Unexpected error creating driver")

    logger.info(
        "[DRIVER_CREATED] org_id=%s driver_id=%s name=%s",
        org_id,
        driver.id,
        driver.name,
    )

    return {"ok": True, "driver": _serialize_driver(driver)}


@router.get("/{driver_id}")
async def get_driver(
    driver_id: str,
    x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
    x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
    db: AsyncSession = Depends(get_db),
):
    """Get a single driver by ID, scoped to the org."""
    org_id = await _get_org_from_header(x_opsyn_org, x_opsyn_secret, db)
    driver = await _get_driver_or_404(db, driver_id, org_id)

    logger.info("[DRIVER_FETCHED] org_id=%s driver_id=%s", org_id, driver_id)

    return {"ok": True, "driver": _serialize_driver(driver)}


@router.patch("/{driver_id}")
async def update_driver(
    driver_id: str,
    body: DriverUpdate,
    x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
    x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
    db: AsyncSession = Depends(get_db),
):
    """Update driver fields. Only provided fields are changed."""
    org_id = await _get_org_from_header(x_opsyn_org, x_opsyn_secret, db)
    driver = await _get_driver_or_404(db, driver_id, org_id)

    updated_fields: list[str] = []

    if body.name is not None:
        driver.name = body.name
        updated_fields.append("name")

    if body.email is not None:
        # Check email uniqueness within org before applying
        if body.email != driver.email:
            existing = await db.execute(
                select(Driver).where(
                    Driver.org_id == org_id,
                    Driver.email == body.email,
                    Driver.id != driver.id,
                )
            )
            if existing.scalar_one_or_none() is not None:
                raise HTTPException(
                    status_code=409,
                    detail="A driver with this email already exists in your organization",
                )
        driver.email = body.email
        updated_fields.append("email")

    if body.phone is not None:
        driver.phone = body.phone
        updated_fields.append("phone")

    if body.license_plate is not None:
        driver.license_plate = body.license_plate
        updated_fields.append("license_plate")

    if body.vehicle_type is not None:
        driver.vehicle_type = body.vehicle_type
        updated_fields.append("vehicle_type")

    if body.notes is not None:
        driver.notes = body.notes
        updated_fields.append("notes")

    if body.preferences is not None:
        driver.preferences = body.preferences
        updated_fields.append("preferences")

    if body.status is not None:
        driver.status = body.status
        updated_fields.append("status")
        if body.status == "inactive":
            driver.deactivated_at = datetime.now(timezone.utc)
        elif body.status == "active":
            driver.deactivated_at = None

    driver.updated_at = datetime.now(timezone.utc)

    try:
        await db.commit()
        await db.refresh(driver)
    except IntegrityError as exc:
        await db.rollback()
        err = str(exc).lower()
        if "uq_drivers_org_email" in err or "unique" in err and "email" in err:
            raise HTTPException(
                status_code=409,
                detail="A driver with this email already exists in your organization",
            )
        logger.error("[DRIVER_UPDATED] db_integrity_error org_id=%s driver_id=%s error=%s", org_id, driver_id, exc)
        raise HTTPException(status_code=400, detail="Could not update driver due to a data conflict")
    except Exception as exc:
        await db.rollback()
        logger.error("[DRIVER_UPDATED] unexpected_error org_id=%s driver_id=%s error=%s", org_id, driver_id, exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Unexpected error updating driver")

    logger.info(
        "[DRIVER_UPDATED] org_id=%s driver_id=%s fields=%s",
        org_id,
        driver_id,
        ",".join(updated_fields) if updated_fields else "none",
    )

    return {"ok": True, "driver": _serialize_driver(driver)}


@router.delete("/{driver_id}")
async def delete_driver(
    driver_id: str,
    x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
    x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
    db: AsyncSession = Depends(get_db),
):
    """Soft-delete a driver by setting status=inactive and deactivated_at=now()."""
    org_id = await _get_org_from_header(x_opsyn_org, x_opsyn_secret, db)
    driver = await _get_driver_or_404(db, driver_id, org_id)

    driver.status = "inactive"
    driver.deactivated_at = datetime.now(timezone.utc)
    driver.updated_at = datetime.now(timezone.utc)

    try:
        await db.commit()
    except Exception as exc:
        await db.rollback()
        logger.error("[DRIVER_DELETED] unexpected_error org_id=%s driver_id=%s error=%s", org_id, driver_id, exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Unexpected error deleting driver")

    logger.info("[DRIVER_DELETED] org_id=%s driver_id=%s", org_id, driver_id)

    return {"ok": True, "driver_id": driver_id, "status": "inactive"}


@router.post("/{driver_id}/generate-passcode")
async def generate_passcode(
    driver_id: str,
    x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
    x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
    db: AsyncSession = Depends(get_db),
):
    """
    Generate a random 6-digit numeric passcode, hash it with bcrypt, and store it.

    The plain passcode is returned ONCE in the response and is NEVER logged.
    """
    org_id = await _get_org_from_header(x_opsyn_org, x_opsyn_secret, db)
    driver = await _get_driver_or_404(db, driver_id, org_id)

    # Generate a cryptographically random 6-digit passcode (000000–999999)
    plain_passcode = f"{random.SystemRandom().randint(0, 999999):06d}"

    # Hash with bcrypt cost=12 — never store or log the plain value
    driver.passcode_hash = _hash_passcode(plain_passcode)
    driver.updated_at = datetime.now(timezone.utc)

    try:
        await db.commit()
    except Exception as exc:
        await db.rollback()
        logger.error(
            "[DRIVER_PASSCODE] action=generate_failed org_id=%s driver_id=%s error=%s",
            org_id, driver_id, exc, exc_info=True,
        )
        raise HTTPException(status_code=500, detail="Unexpected error generating passcode")

    # Log generation — NEVER log the plain passcode
    logger.info("[DRIVER_PASSCODE] action=generated org_id=%s driver_id=%s", org_id, driver_id)

    return {"ok": True, "passcode": plain_passcode}


@router.post("/{driver_id}/reset-passcode")
async def reset_passcode(
    driver_id: str,
    body: PasscodeReset,
    x_opsyn_org: Optional[str] = Header(default=None, alias="x-opsyn-org"),
    x_opsyn_secret: Optional[str] = Header(default=None, alias="x-opsyn-secret"),
    db: AsyncSession = Depends(get_db),
):
    """
    Admin sets a new passcode manually.

    The passcode is hashed with bcrypt and stored. It is NEVER returned or logged.
    """
    org_id = await _get_org_from_header(x_opsyn_org, x_opsyn_secret, db)
    driver = await _get_driver_or_404(db, driver_id, org_id)

    # Hash with bcrypt cost=12 — never store or log the plain value
    driver.passcode_hash = _hash_passcode(body.passcode)
    driver.updated_at = datetime.now(timezone.utc)

    try:
        await db.commit()
    except Exception as exc:
        await db.rollback()
        logger.error(
            "[DRIVER_PASSCODE] action=reset_failed org_id=%s driver_id=%s error=%s",
            org_id, driver_id, exc, exc_info=True,
        )
        raise HTTPException(status_code=500, detail="Unexpected error resetting passcode")

    # Log reset — NEVER log the plain passcode
    logger.info("[DRIVER_PASSCODE] action=reset org_id=%s driver_id=%s", org_id, driver_id)

    return {"ok": True}
