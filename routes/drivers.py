from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, field_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import Driver

router = APIRouter(prefix="/drivers", tags=["drivers"])


# =============================================================================
# Schemas
# =============================================================================

class DriverCreate(BaseModel):
    name: str
    phone: Optional[str] = None
    license_plate: Optional[str] = None
    status: Optional[str] = "available"


class DriverUpdate(BaseModel):
    name: Optional[str] = None
    phone: Optional[str] = None
    license_plate: Optional[str] = None
    status: Optional[str] = None
    pin: Optional[str] = None

    @field_validator("pin")
    @classmethod
    def validate_pin(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        if len(v) != 4 or not v.isdigit():
            raise ValueError("PIN must be exactly 4 numeric digits")
        return v


def serialize_driver(driver: Driver) -> dict:
    return {
        "id": driver.id,
        "org_id": driver.org_id,
        "name": driver.name,
        "phone": driver.phone,
        "license_plate": driver.license_plate,
        "status": driver.status,
        "pin": driver.pin,
        "created_at": driver.created_at.isoformat() if driver.created_at else None,
        "updated_at": driver.updated_at.isoformat() if driver.updated_at else None,
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
        phone=body.phone,
        license_plate=body.license_plate,
        status=body.status or "available",
    )
    db.add(driver)
    await db.commit()
    await db.refresh(driver)
    return {"ok": True, "driver": serialize_driver(driver)}


@router.get("")
async def list_drivers(
    org_id: str,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(Driver).where(Driver.org_id == org_id).order_by(Driver.created_at.desc())
    )
    drivers = result.scalars().all()
    return {
        "ok": True,
        "org_id": org_id,
        "count": len(drivers),
        "drivers": [serialize_driver(d) for d in drivers],
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
    return {"ok": True, "driver": serialize_driver(driver)}


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

    if body.name is not None:
        driver.name = body.name
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
    return {"ok": True, "driver": serialize_driver(driver)}
