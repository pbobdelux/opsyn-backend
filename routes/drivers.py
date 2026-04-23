import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import Driver

router = APIRouter(tags=["drivers"])


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class DriverCreate(BaseModel):
    name: str
    email: Optional[str] = None
    phone: Optional[str] = None
    active: bool = True
    pin: Optional[str] = None


class DriverUpdate(BaseModel):
    name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    active: Optional[bool] = None
    pin: Optional[str] = None


class DriverResponse(BaseModel):
    id: str
    org_id: str
    name: str
    email: Optional[str]
    phone: Optional[str]
    active: bool
    pin: Optional[str]
    created_at: str
    updated_at: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def serialize_driver(driver: Driver) -> dict:
    return {
        "id": driver.id,
        "org_id": driver.org_id,
        "name": driver.name,
        "email": driver.email,
        "phone": driver.phone,
        "active": driver.active,
        "pin": driver.pin,
        "created_at": driver.created_at.isoformat() if driver.created_at else None,
        "updated_at": driver.updated_at.isoformat() if driver.updated_at else None,
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post(
    "/organizations/{org_id}/drivers",
    status_code=status.HTTP_201_CREATED,
)
async def create_driver(
    org_id: str,
    body: DriverCreate,
    db: AsyncSession = Depends(get_db),
):
    driver = Driver(
        id=str(uuid.uuid4()),
        org_id=org_id,
        name=body.name,
        email=body.email,
        phone=body.phone,
        active=body.active,
        pin=body.pin,
    )
    db.add(driver)
    await db.commit()
    await db.refresh(driver)
    return {"ok": True, "driver": serialize_driver(driver)}


@router.get("/organizations/{org_id}/drivers")
async def list_drivers(
    org_id: str,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(Driver)
        .where(Driver.org_id == org_id)
        .order_by(Driver.name)
    )
    drivers = result.scalars().all()
    return {
        "ok": True,
        "org_id": org_id,
        "count": len(drivers),
        "drivers": [serialize_driver(d) for d in drivers],
    }


@router.get("/organizations/{org_id}/drivers/{driver_id}")
async def get_driver(
    org_id: str,
    driver_id: str,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(Driver).where(Driver.id == driver_id, Driver.org_id == org_id)
    )
    driver = result.scalar_one_or_none()
    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found")
    return {"ok": True, "driver": serialize_driver(driver)}


@router.patch("/organizations/{org_id}/drivers/{driver_id}")
async def update_driver(
    org_id: str,
    driver_id: str,
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
    if body.email is not None:
        driver.email = body.email
    if body.phone is not None:
        driver.phone = body.phone
    if body.active is not None:
        driver.active = body.active
    if body.pin is not None:
        driver.pin = body.pin

    driver.updated_at = datetime.now(timezone.utc)
    await db.commit()
    await db.refresh(driver)
    return {"ok": True, "driver": serialize_driver(driver)}
