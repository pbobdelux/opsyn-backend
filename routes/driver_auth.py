"""
routes/driver_auth.py — Driver authentication endpoints.

Endpoints:
  POST /auth/driver/login        — Login with company_code + PIN
  POST /auth/driver/refresh      — Refresh access token
  POST /auth/driver/logout       — Stateless logout (client-side)
  GET  /me/driver-context        — Get current driver context from token
"""
import uuid
from datetime import datetime, timezone

import jwt
from fastapi import APIRouter, Depends, HTTPException, Header
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from auth_utils import (
    create_access_token,
    create_refresh_token,
    decode_token,
    verify_pin,
)
from database import get_db
from models import Driver, DriverAuth, Organization

router = APIRouter(tags=["driver-auth"])


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


def _serialize_org(org: Organization) -> dict:
    return {
        "id": str(org.id),
        "name": org.name,
        "company_code": org.company_code,
        "status": org.status,
    }


async def _get_driver_from_token(
    authorization: str | None,
    db: AsyncSession,
) -> tuple[Driver, Organization]:
    """
    Extract and validate the Bearer token from the Authorization header.
    Returns (driver, organization) or raises HTTPException.
    """
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")

    token = authorization.removeprefix("Bearer ").strip()

    try:
        payload = decode_token(token)
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

    if payload.get("token_type") != "access":
        raise HTTPException(status_code=401, detail="Expected an access token")

    driver_id_str = payload.get("driver_id")
    if not driver_id_str:
        raise HTTPException(status_code=401, detail="Malformed token: missing driver_id")

    try:
        driver_id = uuid.UUID(driver_id_str)
    except ValueError:
        raise HTTPException(status_code=401, detail="Malformed token: invalid driver_id")

    stmt = (
        select(Driver)
        .options(selectinload(Driver.org), selectinload(Driver.auth))
        .where(Driver.id == driver_id)
    )
    result = await db.execute(stmt)
    driver = result.scalar_one_or_none()

    if not driver:
        raise HTTPException(status_code=401, detail="Driver not found")

    if driver.status != "active":
        raise HTTPException(status_code=403, detail="Driver account is inactive")

    return driver, driver.org


# ---------------------------------------------------------------------------
# POST /auth/driver/login
# ---------------------------------------------------------------------------

@router.post("/auth/driver/login")
async def driver_login(data: dict, db: AsyncSession = Depends(get_db)):
    """
    Authenticate a driver using company_code + PIN.

    Request body:
      - company_code (str, required)
      - pin (str, required)
      - brand_id (str, optional)

    Returns access_token, refresh_token, driver info, and org info.
    """
    company_code: str | None = data.get("company_code")
    pin: str | None = data.get("pin")
    brand_id_str: str | None = data.get("brand_id")

    if not company_code or not pin:
        raise HTTPException(status_code=400, detail="company_code and pin are required")

    # Resolve organization by company_code
    org_stmt = select(Organization).where(
        Organization.company_code == company_code,
        Organization.status == "active",
    )
    org_result = await db.execute(org_stmt)
    org = org_result.scalar_one_or_none()

    if not org:
        raise HTTPException(status_code=401, detail="Invalid company code or organization inactive")

    # Find active drivers in this org that have a PIN set
    drivers_stmt = (
        select(Driver)
        .options(selectinload(Driver.auth))
        .where(Driver.org_id == org.id, Driver.status == "active")
    )
    drivers_result = await db.execute(drivers_stmt)
    drivers = drivers_result.scalars().all()

    # Find the driver whose PIN matches
    matched_driver: Driver | None = None
    for driver in drivers:
        if driver.auth and verify_pin(pin, driver.auth.pin_hash):
            matched_driver = driver
            break

    if not matched_driver:
        raise HTTPException(status_code=401, detail="Invalid PIN")

    # Update last_login_at
    matched_driver.auth.last_login_at = datetime.now(timezone.utc)
    await db.commit()
    await db.refresh(matched_driver)

    # Resolve brand
    brand: dict | None = None
    if brand_id_str:
        brand = {"brand_id": brand_id_str}
    elif matched_driver.brand_id:
        brand = {"brand_id": str(matched_driver.brand_id)}

    access_token = create_access_token(
        driver_id=str(matched_driver.id),
        org_id=str(org.id),
    )
    refresh_token = create_refresh_token(driver_id=str(matched_driver.id))

    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer",
        "driver": _serialize_driver(matched_driver),
        "organization": _serialize_org(org),
        "brand": brand,
    }


# ---------------------------------------------------------------------------
# POST /auth/driver/refresh
# ---------------------------------------------------------------------------

@router.post("/auth/driver/refresh")
async def driver_refresh_token(data: dict, db: AsyncSession = Depends(get_db)):
    """
    Exchange a refresh token for a new access token.

    Request body:
      - refresh_token (str, required)
    """
    refresh_token: str | None = data.get("refresh_token")
    if not refresh_token:
        raise HTTPException(status_code=400, detail="refresh_token is required")

    try:
        payload = decode_token(refresh_token)
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Refresh token has expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid refresh token")

    if payload.get("token_type") != "refresh":
        raise HTTPException(status_code=400, detail="Expected a refresh token")

    driver_id_str = payload.get("driver_id")
    if not driver_id_str:
        raise HTTPException(status_code=401, detail="Malformed token: missing driver_id")

    try:
        driver_id = uuid.UUID(driver_id_str)
    except ValueError:
        raise HTTPException(status_code=401, detail="Malformed token: invalid driver_id")

    # Verify driver still exists and is active
    stmt = select(Driver).where(Driver.id == driver_id)
    result = await db.execute(stmt)
    driver = result.scalar_one_or_none()

    if not driver or driver.status != "active":
        raise HTTPException(status_code=401, detail="Driver not found or inactive")

    access_token = create_access_token(
        driver_id=str(driver.id),
        org_id=str(driver.org_id),
    )

    return {
        "access_token": access_token,
        "token_type": "bearer",
    }


# ---------------------------------------------------------------------------
# POST /auth/driver/logout
# ---------------------------------------------------------------------------

@router.post("/auth/driver/logout")
async def driver_logout():
    """
    Stateless logout — tokens are invalidated client-side by discarding them.
    """
    return {"ok": True}


# ---------------------------------------------------------------------------
# GET /me/driver-context
# ---------------------------------------------------------------------------

@router.get("/me/driver-context")
async def get_driver_context(
    authorization: str | None = Header(default=None),
    db: AsyncSession = Depends(get_db),
):
    """
    Return the current driver's context (driver, org, brand) from a valid access token.

    Requires: Authorization: Bearer <access_token>
    """
    driver, org = await _get_driver_from_token(authorization, db)

    brand: dict | None = None
    if driver.brand_id:
        brand = {"brand_id": str(driver.brand_id)}

    return {
        "driver": _serialize_driver(driver),
        "organization": _serialize_org(org),
        "brand": brand,
        "role": "driver",
    }
