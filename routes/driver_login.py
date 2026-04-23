"""
routes/driver_login.py — Driver authentication endpoints for Opsyn Driver App.

Endpoints:
  POST /driver-auth/login   — org_code + PIN → JWT token
  GET  /driver-auth/me      — validate token, return driver context
  POST /driver-auth/logout  — stateless logout (client discards token)
  GET  /driver/routes/active — return active route or null (never 404)
"""

import logging
import uuid
from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException
from jose import JWTError
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from auth_utils import create_access_token, decode_token, verify_pin
from database import get_db
from models import Driver, DriverAuth, Organization, Route, RouteStop

logger = logging.getLogger("opsyn-backend.driver-login")

router = APIRouter(tags=["driver-auth"])

# ---------------------------------------------------------------------------
# Request / Response schemas
# ---------------------------------------------------------------------------


class LoginRequest(BaseModel):
    org_code: str
    pin: str


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _serialize_driver(driver: Driver) -> dict:
    """Return a minimal driver dict safe for API responses."""
    return {
        "id": str(driver.id),
        "full_name": driver.full_name,
        "is_active": driver.status == "active",
    }


def _serialize_org(org: Organization) -> dict:
    """Return a minimal organization dict safe for API responses."""
    return {
        "id": str(org.id),
        "name": org.name,
        "org_code": org.org_code,
    }


def _serialize_route_summary(route: Optional[Route]) -> Optional[dict]:
    """
    Return a compact route summary dict, or None if no route is provided.

    stop_count is derived from the already-loaded stops relationship.
    """
    if route is None:
        return None

    route_date_str: Optional[str] = None
    if route.route_date is not None:
        # route_date is a Python date object (SQLAlchemy Date column)
        route_date_str = route.route_date.isoformat()

    stop_count = len(route.stops) if route.stops is not None else 0

    return {
        "id": str(route.id),
        "status": route.status,
        "route_date": route_date_str,
        "stop_count": stop_count,
    }


async def _find_active_route_for_driver(
    driver_id: uuid.UUID, db: AsyncSession
) -> Optional[Route]:
    """
    Query the routes table for the most recent active route assigned to driver_id.

    Active statuses: draft, assigned, in_progress.

    Returns None gracefully if:
    - No active route exists for this driver.
    - The routes table does not yet exist (routing PR #2 not yet merged).
    """
    active_statuses = ("draft", "assigned", "in_progress")

    try:
        stmt = (
            select(Route)
            .options(selectinload(Route.stops))
            .where(
                Route.driver_id == driver_id,
                Route.status.in_(active_statuses),
            )
            .order_by(Route.updated_at.desc())
            .limit(1)
        )
        result = await db.execute(stmt)
        return result.scalar_one_or_none()
    except Exception as exc:
        # Gracefully handle the case where the routes table doesn't exist yet.
        logger.warning(
            "active_route lookup failed (routes table may not exist yet): %s", exc
        )
        return None


async def _get_driver_from_token(
    authorization: Optional[str], db: AsyncSession
) -> tuple[Driver, Organization]:
    """
    Extract and validate a Bearer token from the Authorization header.

    Returns (driver, organization) on success.
    Raises HTTP 401 on any failure (missing header, bad token, inactive driver, etc.).
    """
    credentials_error = HTTPException(
        status_code=401,
        detail="Invalid or expired token",
        headers={"WWW-Authenticate": "Bearer"},
    )

    if not authorization or not authorization.startswith("Bearer "):
        raise credentials_error

    token = authorization.removeprefix("Bearer ").strip()

    try:
        payload = decode_token(token)
    except JWTError:
        raise credentials_error

    driver_id_str: Optional[str] = payload.get("driver_id")
    org_id_str: Optional[str] = payload.get("org_id")

    if not driver_id_str or not org_id_str:
        raise credentials_error

    try:
        driver_id = uuid.UUID(driver_id_str)
        org_id = uuid.UUID(org_id_str)
    except ValueError:
        raise credentials_error

    # Fetch driver with auth relationship
    driver_result = await db.execute(
        select(Driver)
        .options(selectinload(Driver.auth))
        .where(Driver.id == driver_id)
    )
    driver: Optional[Driver] = driver_result.scalar_one_or_none()

    if driver is None or driver.status != "active":
        raise credentials_error

    # Fetch organization
    org_result = await db.execute(
        select(Organization).where(Organization.id == org_id, Organization.is_active == True)
    )
    org: Optional[Organization] = org_result.scalar_one_or_none()

    if org is None:
        raise credentials_error

    return driver, org


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("/driver-auth/login")
async def driver_login(body: LoginRequest, db: AsyncSession = Depends(get_db)):
    """
    Authenticate a driver using org_code + PIN.

    Returns a JWT access token and driver/org/active_route context on success.
    Always returns 401 with a generic message on any failure to avoid leaking
    which field was wrong.
    """
    invalid_error = HTTPException(
        status_code=401,
        detail="Invalid org_code or PIN",
    )

    # 1. Look up organization by org_code
    org_result = await db.execute(
        select(Organization).where(
            Organization.org_code == body.org_code,
            Organization.is_active == True,
        )
    )
    org: Optional[Organization] = org_result.scalar_one_or_none()

    if org is None:
        raise invalid_error

    # 2. Find an active driver in this org that has a PIN set.
    #    We load all active drivers with their auth records and verify the PIN
    #    against each one. In practice there will be one driver per PIN login
    #    attempt (the driver knows their own PIN), but the schema allows multiple
    #    drivers per org. We match the first driver whose PIN verifies.
    drivers_result = await db.execute(
        select(Driver)
        .options(selectinload(Driver.auth))
        .where(Driver.org_id == org.id, Driver.status == "active")
    )
    active_drivers: list[Driver] = list(drivers_result.scalars().all())

    matched_driver: Optional[Driver] = None
    for candidate in active_drivers:
        if candidate.auth is None:
            continue
        if verify_pin(body.pin, candidate.auth.pin_hash):
            matched_driver = candidate
            break

    if matched_driver is None:
        raise invalid_error

    # 3. Issue JWT
    token = create_access_token(
        driver_id=str(matched_driver.id),
        org_id=str(org.id),
    )

    # 4. Fetch active route (optional — graceful if table doesn't exist)
    active_route = await _find_active_route_for_driver(matched_driver.id, db)

    return {
        "token": token,
        "driver": _serialize_driver(matched_driver),
        "organization": _serialize_org(org),
        "active_route": _serialize_route_summary(active_route),
    }


@router.get("/driver-auth/me")
async def driver_me(
    authorization: Optional[str] = Header(default=None),
    db: AsyncSession = Depends(get_db),
):
    """
    Return the authenticated driver's context.

    Requires: Authorization: Bearer <token>
    """
    driver, org = await _get_driver_from_token(authorization, db)
    active_route = await _find_active_route_for_driver(driver.id, db)

    return {
        "driver": _serialize_driver(driver),
        "organization": _serialize_org(org),
        "active_route": _serialize_route_summary(active_route),
    }


@router.post("/driver-auth/logout")
async def driver_logout(
    authorization: Optional[str] = Header(default=None),
    db: AsyncSession = Depends(get_db),
):
    """
    Stateless logout — validates the token then instructs the client to discard it.

    The server does not maintain a token blocklist; the client is responsible
    for discarding the token after receiving this response.
    """
    # Validate the token so we don't return 200 for garbage requests
    await _get_driver_from_token(authorization, db)
    return {"ok": True}


@router.get("/driver/routes/active")
async def driver_active_route(
    authorization: Optional[str] = Header(default=None),
    db: AsyncSession = Depends(get_db),
):
    """
    Return the driver's current active route, or null if none exists.

    IMPORTANT: This endpoint ALWAYS returns HTTP 200. It never returns 404.
    A missing route is represented as {"route": null}.
    """
    driver, _org = await _get_driver_from_token(authorization, db)
    active_route = await _find_active_route_for_driver(driver.id, db)

    return {"route": _serialize_route_summary(active_route)}
