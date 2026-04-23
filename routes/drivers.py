"""
routes/drivers.py — Driver management endpoints (CRUD per organization).

Endpoints:
  POST  /organizations/{org_id}/drivers                        — Create driver
  GET   /organizations/{org_id}/drivers                        — List drivers
  PATCH /organizations/{org_id}/drivers/{driver_id}            — Edit driver
  POST  /organizations/{org_id}/drivers/{driver_id}/set-pin    — Set/rotate PIN
"""
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from auth_utils import hash_pin
from database import get_db
from models import Driver, DriverAuth, Organization

router = APIRouter(tags=["drivers"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _serialize_driver(driver: Driver) -> dict:
    return {
        "id": str(driver.id),
        "org_id": str(driver.org_id),
        "brand_id": str(driver.brand_id) if driver.brand_id else None,
        "full_name": driver.full_name,
        "phone": driver.phone,
        "email": driver.email,
        "status": driver.status,
        "availability": driver.availability,
        "employee_code": driver.employee_code,
        "created_at": driver.created_at.isoformat() if driver.created_at else None,
        "updated_at": driver.updated_at.isoformat() if driver.updated_at else None,
    }


async def _get_org_or_404(org_id: str, db: AsyncSession) -> Organization:
    try:
        org_uuid = uuid.UUID(org_id)
    except ValueError:
        raise HTTPException(status_code=422, detail="org_id must be a valid UUID")

    stmt = select(Organization).where(Organization.id == org_uuid)
    result = await db.execute(stmt)
    org = result.scalar_one_or_none()
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")
    return org


async def _get_driver_or_404(driver_id: str, org_id: uuid.UUID, db: AsyncSession) -> Driver:
    try:
        driver_uuid = uuid.UUID(driver_id)
    except ValueError:
        raise HTTPException(status_code=422, detail="driver_id must be a valid UUID")

    stmt = select(Driver).where(Driver.id == driver_uuid, Driver.org_id == org_id)
    result = await db.execute(stmt)
    driver = result.scalar_one_or_none()
    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found in this organization")
    return driver


# ---------------------------------------------------------------------------
# POST /organizations/{org_id}/drivers
# ---------------------------------------------------------------------------

@router.post("/organizations/{org_id}/drivers", status_code=201)
async def create_driver(
    org_id: str,
    data: dict,
    db: AsyncSession = Depends(get_db),
):
    """
    Create a new driver in the given organization.

    Request body:
      - full_name (str, required)
      - phone (str, optional)
      - email (str, optional)
      - brand_id (str UUID, optional)
      - employee_code (str, optional)
    """
    org = await _get_org_or_404(org_id, db)

    full_name: str | None = data.get("full_name")
    if not full_name or not full_name.strip():
        raise HTTPException(status_code=400, detail="full_name is required")

    brand_id: uuid.UUID | None = None
    if data.get("brand_id"):
        try:
            brand_id = uuid.UUID(data["brand_id"])
        except ValueError:
            raise HTTPException(status_code=422, detail="brand_id must be a valid UUID")

    driver = Driver(
        org_id=org.id,
        brand_id=brand_id,
        full_name=full_name.strip(),
        phone=data.get("phone"),
        email=data.get("email"),
        employee_code=data.get("employee_code"),
    )
    db.add(driver)
    await db.commit()
    await db.refresh(driver)

    return _serialize_driver(driver)


# ---------------------------------------------------------------------------
# GET /organizations/{org_id}/drivers
# ---------------------------------------------------------------------------

@router.get("/organizations/{org_id}/drivers")
async def list_drivers(
    org_id: str,
    status: str = Query(default="active", description="Filter by status: active, inactive, all"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """
    List drivers for an organization.

    Query params:
      - status: active | inactive | all  (default: active)
      - limit: max records to return (default: 50, max: 200)
      - offset: pagination offset (default: 0)
    """
    org = await _get_org_or_404(org_id, db)

    base_stmt = select(Driver).where(Driver.org_id == org.id)

    if status and status.lower() != "all":
        base_stmt = base_stmt.where(Driver.status == status.lower())

    # Total count
    count_stmt = select(func.count()).select_from(base_stmt.subquery())
    count_result = await db.execute(count_stmt)
    total = count_result.scalar_one()

    # Paginated results
    page_stmt = base_stmt.order_by(Driver.created_at.desc()).limit(limit).offset(offset)
    drivers_result = await db.execute(page_stmt)
    drivers = drivers_result.scalars().all()

    return {
        "drivers": [_serialize_driver(d) for d in drivers],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


# ---------------------------------------------------------------------------
# PATCH /organizations/{org_id}/drivers/{driver_id}
# ---------------------------------------------------------------------------

@router.patch("/organizations/{org_id}/drivers/{driver_id}")
async def update_driver(
    org_id: str,
    driver_id: str,
    data: dict,
    db: AsyncSession = Depends(get_db),
):
    """
    Edit a driver's profile.

    Request body (all fields optional):
      - full_name
      - phone
      - email
      - status (active | inactive)
      - availability (available | unavailable | on_duty)
      - brand_id (UUID string or null)
      - employee_code
    """
    org = await _get_org_or_404(org_id, db)
    driver = await _get_driver_or_404(driver_id, org.id, db)

    if "full_name" in data:
        full_name = data["full_name"]
        if not full_name or not str(full_name).strip():
            raise HTTPException(status_code=400, detail="full_name cannot be empty")
        driver.full_name = str(full_name).strip()

    if "phone" in data:
        driver.phone = data["phone"]

    if "email" in data:
        driver.email = data["email"]

    if "status" in data:
        allowed_statuses = {"active", "inactive"}
        if data["status"] not in allowed_statuses:
            raise HTTPException(
                status_code=400,
                detail=f"status must be one of: {', '.join(sorted(allowed_statuses))}",
            )
        driver.status = data["status"]

    if "availability" in data:
        allowed_avail = {"available", "unavailable", "on_duty"}
        if data["availability"] not in allowed_avail:
            raise HTTPException(
                status_code=400,
                detail=f"availability must be one of: {', '.join(sorted(allowed_avail))}",
            )
        driver.availability = data["availability"]

    if "brand_id" in data:
        if data["brand_id"] is None:
            driver.brand_id = None
        else:
            try:
                driver.brand_id = uuid.UUID(data["brand_id"])
            except ValueError:
                raise HTTPException(status_code=422, detail="brand_id must be a valid UUID or null")

    if "employee_code" in data:
        driver.employee_code = data["employee_code"]

    driver.updated_at = datetime.now(timezone.utc)
    await db.commit()
    await db.refresh(driver)

    return _serialize_driver(driver)


# ---------------------------------------------------------------------------
# POST /organizations/{org_id}/drivers/{driver_id}/set-pin
# ---------------------------------------------------------------------------

@router.post("/organizations/{org_id}/drivers/{driver_id}/set-pin")
async def set_driver_pin(
    org_id: str,
    driver_id: str,
    data: dict,
    db: AsyncSession = Depends(get_db),
):
    """
    Set or rotate a driver's PIN.

    Request body:
      - pin (str, required) — plain-text PIN, will be bcrypt-hashed before storage

    The PIN is never stored in plain text. If the driver already has a PIN,
    it is replaced and pin_last_rotated_at is updated.
    """
    org = await _get_org_or_404(org_id, db)
    driver = await _get_driver_or_404(driver_id, org.id, db)

    pin: str | None = data.get("pin")
    if not pin or not str(pin).strip():
        raise HTTPException(status_code=400, detail="pin is required")

    pin_hash = hash_pin(str(pin))
    now = datetime.now(timezone.utc)

    # Fetch existing auth row (if any)
    auth_stmt = select(DriverAuth).where(DriverAuth.driver_id == driver.id)
    auth_result = await db.execute(auth_stmt)
    driver_auth = auth_result.scalar_one_or_none()

    if driver_auth:
        driver_auth.pin_hash = pin_hash
        driver_auth.pin_last_rotated_at = now
    else:
        driver_auth = DriverAuth(
            driver_id=driver.id,
            pin_hash=pin_hash,
            pin_last_rotated_at=now,
        )
        db.add(driver_auth)

    await db.commit()

    return {
        "ok": True,
        "driver_id": str(driver.id),
        "message": "PIN set successfully",
    }
