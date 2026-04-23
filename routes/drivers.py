import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, field_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import Driver
from services.twin_events import send_driver_event

logger = logging.getLogger("drivers")

router = APIRouter(prefix="/drivers", tags=["drivers"])

# Org metadata used in Twin event payloads.  Extend as needed.
_ORG_META: dict[str, dict] = {
    "org_onboarding": {"name": "Opsyn Onboarding", "code": "org_onboarding"},
}


def _org_name(org_id: str) -> str:
    return _ORG_META.get(org_id, {}).get("name", org_id)


def _org_code(org_id: str) -> str:
    return _ORG_META.get(org_id, {}).get("code", org_id)


# =============================================================================
# Schemas
# =============================================================================

class DriverCreate(BaseModel):
    name: str
    # Email is required for new drivers and must be a valid format.
    email: str
    # PIN is required for new drivers and must be exactly 4 numeric digits.
    # The client (Opsyn Brand app) is solely responsible for generating and
    # supplying the PIN — the backend never generates one.
    pin: str
    phone: Optional[str] = None
    license_plate: Optional[str] = None
    status: Optional[str] = "available"

    @field_validator("email")
    @classmethod
    def validate_email(cls, v: str) -> str:
        v = v.strip()
        if len(v) < 5 or len(v) > 254:
            raise ValueError("Email must be between 5 and 254 characters")
        if "@" not in v:
            raise ValueError("Email must contain '@'")
        local, _, domain = v.partition("@")
        if not local or not domain or "." not in domain:
            raise ValueError("Email must be a valid address (e.g. user@example.com)")
        return v

    @field_validator("pin")
    @classmethod
    def validate_pin(cls, v: str) -> str:
        if len(v) != 4 or not v.isdigit():
            logger.warning("DriverCreate PIN validation failed: must be exactly 4 numeric digits")
            raise ValueError("PIN must be exactly 4 numeric digits")
        return v


class DriverUpdate(BaseModel):
    name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    license_plate: Optional[str] = None
    status: Optional[str] = None
    pin: Optional[str] = None

    @field_validator("email")
    @classmethod
    def validate_email(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        v = v.strip()
        if len(v) < 5 or len(v) > 254:
            raise ValueError("Email must be between 5 and 254 characters")
        if "@" not in v:
            raise ValueError("Email must contain '@'")
        local, _, domain = v.partition("@")
        if not local or not domain or "." not in domain:
            raise ValueError("Email must be a valid address (e.g. user@example.com)")
        return v

    @field_validator("pin")
    @classmethod
    def validate_pin(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        if len(v) != 4 or not v.isdigit():
            logger.warning("DriverUpdate PIN validation failed: must be exactly 4 numeric digits")
            raise ValueError("PIN must be exactly 4 numeric digits")
        return v


# =============================================================================
# Serialization
# =============================================================================

def serialize_driver(driver: Driver) -> dict:
    """
    Convert a Driver ORM instance to a plain dict.

    All attribute accesses use getattr with safe defaults so that legacy rows
    missing newer columns (e.g. email) never cause a 500.
    """
    created_at = getattr(driver, "created_at", None)
    updated_at = getattr(driver, "updated_at", None)
    return {
        "id": getattr(driver, "id", None),
        "org_id": getattr(driver, "org_id", None),
        "name": getattr(driver, "name", None) or "",
        "email": getattr(driver, "email", None),
        "phone": getattr(driver, "phone", None),
        "license_plate": getattr(driver, "license_plate", None),
        "status": getattr(driver, "status", None) or "available",
        "pin": getattr(driver, "pin", None),
        "created_at": created_at.isoformat() if created_at else None,
        "updated_at": updated_at.isoformat() if updated_at else None,
    }


def _safe_serialize(driver: Driver) -> dict:
    """Serialize a driver, returning a minimal safe dict on any error.

    Guaranteed to never raise — any exception is caught, logged with the
    driver_id (no sensitive data), and a complete fallback dict is returned
    so the caller can always include this driver in the response.
    """
    driver_id = getattr(driver, "id", None)
    try:
        return serialize_driver(driver)
    except Exception as exc:
        logger.error("driver_id=%s serialization failed: %s", driver_id, exc)
        return {
            "id": driver_id,
            "org_id": getattr(driver, "org_id", None),
            "name": getattr(driver, "name", None) or "",
            "email": None,
            "phone": None,
            "license_plate": None,
            "status": "available",
            "pin": None,
            "created_at": None,
            "updated_at": None,
        }


# =============================================================================
# Endpoints
# =============================================================================

@router.post("")
async def create_driver(
    org_id: str,
    body: DriverCreate,
    db: AsyncSession = Depends(get_db),
):
    driver = Driver(
        org_id=org_id,
        name=body.name,
        email=body.email,
        phone=body.phone,
        license_plate=body.license_plate,
        status=body.status or "available",
        pin=body.pin,
    )
    db.add(driver)
    await db.commit()
    await db.refresh(driver)

    logger.info("driver_id=%s created with PIN set", driver.id)

    serialized = _safe_serialize(driver)

    # Fire Twin event (fire-and-forget — never raises)
    await send_driver_event(
        event_type="driver_created",
        org_id=org_id,
        org_name=_org_name(org_id),
        org_code=_org_code(org_id),
        driver_id=driver.id,
        driver_name=driver.name,
        driver_email=driver.email,
        driver_phone=driver.phone,
        driver_pin=driver.pin,
    )

    return {"ok": True, "driver": serialized}


@router.get("")
async def list_drivers(
    org_id: str,
    db: AsyncSession = Depends(get_db),
):
    """List all drivers for an organisation.

    This endpoint is designed to be bulletproof:
    - A failed database query returns an empty drivers list (never a 500).
    - A driver row that fails to serialize is skipped with an error log;
      all other drivers are still returned.
    - Null / missing fields on legacy rows are handled by _safe_serialize.
    - The response shape ``{"ok": true, "org_id": ..., "count": N, "drivers": [...]}``
      is guaranteed regardless of what goes wrong.
    """
    logger.info("list_drivers called for org_id=%s", org_id)

    # ------------------------------------------------------------------
    # 1. Fetch rows — isolate DB errors so they never propagate as 500s.
    # ------------------------------------------------------------------
    raw_drivers: list = []
    try:
        result = await db.execute(
            select(Driver).where(Driver.org_id == org_id).order_by(Driver.created_at.desc())
        )
        raw_drivers = list(result.scalars().all())
    except Exception as exc:
        logger.error("list_drivers database error for org_id=%s: %s", org_id, exc)
        # Return a valid empty response — do not re-raise.
        return {"ok": True, "org_id": org_id, "count": 0, "drivers": []}

    # ------------------------------------------------------------------
    # 2. Serialize each driver individually so one bad row can't abort
    #    the entire response.
    # ------------------------------------------------------------------
    serialized: list[dict] = []
    for driver in raw_drivers:
        driver_id = getattr(driver, "id", None)
        try:
            serialized.append(_safe_serialize(driver))
        except Exception as exc:
            # _safe_serialize itself should never raise, but guard anyway.
            logger.error("driver_id=%s serialization failed: %s", driver_id, exc)

    # ------------------------------------------------------------------
    # 3. Always return the guaranteed response shape.
    # ------------------------------------------------------------------
    logger.info("list_drivers returning count=%d for org_id=%s", len(serialized), org_id)
    return {
        "ok": True,
        "org_id": org_id,
        "count": len(serialized),
        "drivers": serialized,
    }


@router.get("/{driver_id}")
async def get_driver(
    org_id: str,
    driver_id: int,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(Driver).where(Driver.id == driver_id, Driver.org_id == org_id)
    )
    driver = result.scalar_one_or_none()
    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found")
    return {"ok": True, "driver": _safe_serialize(driver)}


@router.patch("/{driver_id}")
async def update_driver(
    org_id: str,
    driver_id: int,
    body: DriverUpdate,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(Driver).where(Driver.id == driver_id, Driver.org_id == org_id)
    )
    driver = result.scalar_one_or_none()
    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found")

    pin_changed = body.pin is not None and body.pin != driver.pin

    if body.name is not None:
        driver.name = body.name
    if body.email is not None:
        driver.email = body.email
    if body.phone is not None:
        driver.phone = body.phone
    if body.license_plate is not None:
        driver.license_plate = body.license_plate
    if body.status is not None:
        driver.status = body.status
    if body.pin is not None:
        driver.pin = body.pin

    driver.updated_at = datetime.now(timezone.utc)
    await db.commit()
    await db.refresh(driver)

    if pin_changed:
        logger.info("driver_id=%s PIN updated", driver.id)

    serialized = _safe_serialize(driver)

    # Fire Twin event only when the PIN was actually changed
    if pin_changed:
        await send_driver_event(
            event_type="driver_pin_reset",
            org_id=org_id,
            org_name=_org_name(org_id),
            org_code=_org_code(org_id),
            driver_id=driver.id,
            driver_name=driver.name,
            driver_email=driver.email,
            driver_phone=driver.phone,
            driver_pin=driver.pin,
        )
        logger.info("driver_id=%s PIN updated, Twin event fired", driver.id)

    return {"ok": True, "driver": serialized}
