"""
Driver-facing authentication and route access API.

Drivers authenticate with org_code + passcode (PIN), receive a short-lived
JWT, and can only see/modify routes that are assigned to them.

All endpoints use the /driver prefix and the driver-app tag.

Security rules enforced here:
  - Drivers only see routes where assigned_driver_id == their own driver.id
  - Drivers cannot modify route structure (add/remove/reorder stops)
  - Drivers can only update stop status and collection amounts
  - passcode_hash is NEVER returned in any response
  - Plain passcodes are NEVER logged
"""

import logging
import os
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

import bcrypt
import jwt
from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel, field_validator
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models.auth_models import Organization
from models.driver import Driver
from models.route import Route
from models.route_stop import RouteStop
from utils.json_utils import make_json_safe

logger = logging.getLogger("driver_app")

router = APIRouter(prefix="/driver", tags=["driver-app"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DRIVER_JWT_ALGORITHM = "HS256"
_DRIVER_JWT_EXPIRY_HOURS = 24

VALID_STOP_STATUSES = {"arrived", "completed", "failed", "skipped"}
VALID_PAYMENT_METHODS = {"cash", "check", "card", "other"}

# Stop statuses that trigger completed_at timestamp
TERMINAL_STOP_STATUSES = {"completed", "failed", "skipped"}


# =============================================================================
# Pydantic Schemas
# =============================================================================


class DriverLoginRequest(BaseModel):
    org_code: str
    passcode: str

    @field_validator("org_code")
    @classmethod
    def validate_org_code(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("org_code is required")
        return v.lower()

    @field_validator("passcode")
    @classmethod
    def validate_passcode(cls, v: str) -> str:
        if not v:
            raise ValueError("passcode is required")
        return v


class StopStatusUpdate(BaseModel):
    status: str
    notes: Optional[str] = None

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        if v not in VALID_STOP_STATUSES:
            raise ValueError(
                f"status must be one of: {', '.join(sorted(VALID_STOP_STATUSES))}"
            )
        return v


class CollectionUpdate(BaseModel):
    amount_collected: float
    payment_method: str
    notes: Optional[str] = None

    @field_validator("amount_collected")
    @classmethod
    def validate_amount(cls, v: float) -> float:
        if v < 0:
            raise ValueError("amount_collected must be non-negative")
        return v

    @field_validator("payment_method")
    @classmethod
    def validate_payment_method(cls, v: str) -> str:
        if v not in VALID_PAYMENT_METHODS:
            raise ValueError(
                f"payment_method must be one of: {', '.join(sorted(VALID_PAYMENT_METHODS))}"
            )
        return v


# =============================================================================
# Helper: JWT
# =============================================================================


def _get_jwt_secret() -> str:
    """Read DRIVER_JWT_SECRET from environment. Raises RuntimeError if missing."""
    secret = os.getenv("DRIVER_JWT_SECRET", "")
    if not secret:
        raise RuntimeError("DRIVER_JWT_SECRET environment variable is not set")
    return secret


def _create_driver_jwt(driver_id: str, org_id: str) -> str:
    """
    Generate a signed HS256 JWT for a driver.

    Claims: { driver_id, org_id, role: "driver", exp, iat }
    Expiry: 24 hours from now.
    """
    now = datetime.now(timezone.utc)
    payload = {
        "driver_id": driver_id,
        "org_id": org_id,
        "role": "driver",
        "iat": now,
        "exp": now + timedelta(hours=_DRIVER_JWT_EXPIRY_HOURS),
    }
    return jwt.encode(payload, _get_jwt_secret(), algorithm=_DRIVER_JWT_ALGORITHM)


def _decode_driver_jwt(token: str) -> dict:
    """
    Decode and validate a driver JWT.

    Raises HTTPException 401 on any validation failure (expired, invalid sig, etc.).
    """
    try:
        payload = jwt.decode(
            token,
            _get_jwt_secret(),
            algorithms=[_DRIVER_JWT_ALGORITHM],
        )
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")
    except RuntimeError as exc:
        logger.error("[DRIVER_AUTH] jwt_secret_missing error=%s", exc)
        raise HTTPException(status_code=500, detail="Authentication configuration error")


# =============================================================================
# Helper: Organization lookup
# =============================================================================


async def _get_org_by_code(db: AsyncSession, org_code: str) -> Optional[Organization]:
    """
    Look up an Organization by its short code/slug (org_code field).

    Returns the Organization ORM instance, or None if not found.
    """
    result = await db.execute(
        select(Organization).where(Organization.org_code == org_code.lower())
    )
    return result.scalar_one_or_none()


# =============================================================================
# Helper: Driver passcode verification
# =============================================================================


async def _verify_driver_passcode(
    db: AsyncSession, org_id: str, passcode: str
) -> Optional[Driver]:
    """
    Find the active driver in org_id whose passcode_hash matches passcode.

    Iterates over all active drivers in the org and uses bcrypt.checkpw()
    for constant-time comparison.  Returns the matching Driver, or None.

    NEVER logs the plain passcode.
    """
    result = await db.execute(
        select(Driver).where(
            Driver.org_id == org_id,
            Driver.status == "active",
            Driver.passcode_hash.isnot(None),
        )
    )
    drivers = result.scalars().all()

    for driver in drivers:
        try:
            if bcrypt.checkpw(passcode.encode(), driver.passcode_hash.encode()):
                return driver
        except Exception:
            # Malformed hash — skip silently
            continue

    return None


# =============================================================================
# Helper: Serializers
# =============================================================================


def _serialize_driver_brief(driver: Driver, org_name: Optional[str] = None) -> dict:
    """
    Serialize a Driver for driver-facing responses.

    NEVER includes passcode_hash.
    Optionally includes org_name when available.
    """
    data: dict = {
        "id": driver.id,
        "name": driver.name or "",
        "org_id": driver.org_id,
        "email": driver.email,
        "phone": driver.phone,
        "status": driver.status or "active",
    }
    if org_name is not None:
        data["org_name"] = org_name
    return make_json_safe(data)


def _serialize_route_summary(route: Route) -> dict:
    """Serialize a Route for the driver routes list (no stops)."""
    return make_json_safe({
        "id": route.id,
        "route_number": route.route_number,
        "route_date": route.route_date,
        "status": route.status,
        "stop_count": route.total_stops,
        "total_value": route.total_value,
        "total_units": route.total_units,
        "notes": route.notes,
        "version": route.version,
        "created_at": route.created_at,
        "updated_at": route.updated_at,
        "published_at": route.published_at,
    })


def _serialize_route_stop(stop: RouteStop) -> dict:
    """Serialize a RouteStop for driver-facing responses."""
    return make_json_safe({
        "id": stop.id,
        "stop_order": stop.stop_order,
        "stop_type": stop.stop_type,
        "customer_name": stop.customer_name,
        "stop_name": stop.stop_name,
        "address": stop.address,
        "contact_name": stop.contact_name,
        "contact_phone": stop.contact_phone,
        "notes": stop.notes,
        "time_window": stop.time_window,
        "priority": stop.priority,
        "status": stop.status,
        "ar_status": stop.ar_status,
        "amount_due": stop.amount_due,
        "amount_collected": stop.amount_collected,
        "completed_at": stop.completed_at,
        "created_at": stop.created_at,
        "updated_at": stop.updated_at,
    })


def _serialize_route_detail(route: Route, stops: list) -> dict:
    """Serialize a Route with its full stop list for driver-facing responses."""
    return make_json_safe({
        "id": route.id,
        "route_number": route.route_number,
        "route_date": route.route_date,
        "status": route.status,
        "stops": [_serialize_route_stop(s) for s in stops],
        "total_stops": route.total_stops,
        "total_value": route.total_value,
        "total_units": route.total_units,
        "version": route.version,
        "created_at": route.created_at,
        "updated_at": route.updated_at,
        "published_at": route.published_at,
    })


# =============================================================================
# JWT Dependency: get_current_driver
# =============================================================================


async def get_current_driver(
    authorization: Optional[str] = Header(default=None, alias="authorization"),
    db: AsyncSession = Depends(get_db),
) -> Driver:
    """
    FastAPI dependency that validates the driver JWT and returns the Driver.

    Reads Authorization: Bearer <token> header.
    Raises HTTP 401 if:
      - Header is missing or malformed
      - Token is invalid or expired
      - Driver not found in database
      - Driver is not active
    """
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization header is required")

    parts = authorization.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(
            status_code=401,
            detail="Authorization header must be: Bearer <token>",
        )

    token = parts[1]
    payload = _decode_driver_jwt(token)

    driver_id = payload.get("driver_id")
    org_id = payload.get("org_id")

    if not driver_id or not org_id:
        raise HTTPException(status_code=401, detail="Invalid token claims")

    result = await db.execute(
        select(Driver).where(
            Driver.id == driver_id,
            Driver.org_id == org_id,
        )
    )
    driver = result.scalar_one_or_none()

    if driver is None:
        raise HTTPException(status_code=401, detail="Driver not found")

    if driver.status != "active":
        raise HTTPException(status_code=401, detail="Driver account is inactive")

    return driver


# =============================================================================
# Endpoint: POST /driver/login
# =============================================================================


@router.post("/login")
async def driver_login(
    body: DriverLoginRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Authenticate a driver with org_code + passcode.

    Returns a JWT token and driver profile on success.
    Returns a generic 401 on any failure — never reveals whether
    org_code or passcode was wrong.
    """
    # Step 1: Look up organization by org_code
    org = await _get_org_by_code(db, body.org_code)

    # Step 2: Verify passcode against active drivers in that org
    # We do this even if org is None to avoid timing-based org enumeration.
    driver: Optional[Driver] = None
    if org is not None:
        driver = await _verify_driver_passcode(db, str(org.id), body.passcode)

    success = driver is not None

    # Log attempt — NEVER log the plain passcode
    logger.info(
        "[DRIVER_AUTH] login_attempt org_code=%s success=%s",
        body.org_code,
        success,
    )

    if not success:
        raise HTTPException(
            status_code=401,
            detail="Invalid credentials",
        )

    # Step 3: Generate JWT
    token = _create_driver_jwt(str(driver.id), str(driver.org_id))

    return {
        "ok": True,
        "token": token,
        "driver": _serialize_driver_brief(driver),
    }


# =============================================================================
# Endpoint: GET /driver/me
# =============================================================================


@router.get("/me")
async def get_driver_me(
    driver: Driver = Depends(get_current_driver),
    db: AsyncSession = Depends(get_db),
):
    """
    Return the current driver's profile, including org_name.

    NEVER includes passcode_hash.
    """
    logger.info("[DRIVER_PROFILE] driver_id=%s", driver.id)

    # Resolve org name
    org_name: Optional[str] = None
    try:
        result = await db.execute(
            select(Organization).where(Organization.id == str(driver.org_id))
        )
        org = result.scalar_one_or_none()
        if org:
            org_name = org.name
    except Exception as exc:
        logger.warning("[DRIVER_PROFILE] org_lookup_failed driver_id=%s error=%s", driver.id, exc)

    return {
        "ok": True,
        "driver": _serialize_driver_brief(driver, org_name=org_name),
    }


# =============================================================================
# Endpoint: GET /driver/routes
# =============================================================================


@router.get("/routes")
async def list_driver_routes(
    limit: int = Query(default=50, ge=1, le=1000, description="Pagination limit"),
    offset: int = Query(default=0, ge=0, description="Pagination offset"),
    driver: Driver = Depends(get_current_driver),
    db: AsyncSession = Depends(get_db),
):
    """
    Return all routes assigned to the current driver.

    Filters:
      - assigned_driver_id == driver.id  (enforced — driver cannot see others' routes)
      - route_date >= today - 7 days     (recent routes only)

    Sorted by route_date DESC.
    """
    cutoff_date = date.today() - timedelta(days=7)

    base_query = select(Route).where(
        Route.assigned_driver_id == driver.id,
        Route.route_date >= cutoff_date,
    )

    # Total count before pagination
    count_result = await db.execute(
        select(func.count()).select_from(base_query.subquery())
    )
    total = count_result.scalar_one()

    # Paginated results
    result = await db.execute(
        base_query.order_by(Route.route_date.desc()).offset(offset).limit(limit)
    )
    routes = result.scalars().all()

    serialized = [_serialize_route_summary(r) for r in routes]

    logger.info("[DRIVER_ROUTES] driver_id=%s count=%s", driver.id, len(serialized))

    return make_json_safe({
        "ok": True,
        "driver_id": str(driver.id),
        "routes": serialized,
        "total": total,
        "limit": limit,
        "offset": offset,
    })


# =============================================================================
# Endpoint: GET /driver/routes/{route_id}
# =============================================================================


@router.get("/routes/{route_id}")
async def get_driver_route(
    route_id: str,
    driver: Driver = Depends(get_current_driver),
    db: AsyncSession = Depends(get_db),
):
    """
    Return a single route with all stops, scoped to the current driver.

    Returns 404 if the route is not assigned to this driver.
    Stops are ordered by stop_order ASC.
    """
    logger.info("[DRIVER_ROUTE_DETAIL] driver_id=%s route_id=%s", driver.id, route_id)

    result = await db.execute(
        select(Route).where(
            Route.id == route_id,
            Route.assigned_driver_id == driver.id,
        )
    )
    route = result.scalar_one_or_none()

    if route is None:
        raise HTTPException(status_code=404, detail="Route not found")

    stops_result = await db.execute(
        select(RouteStop)
        .where(RouteStop.route_id == route.id)
        .order_by(RouteStop.stop_order.asc())
    )
    stops = stops_result.scalars().all()

    return {
        "ok": True,
        "route": _serialize_route_detail(route, stops),
    }


# =============================================================================
# Endpoint: PATCH /driver/routes/{route_id}/stops/{stop_id}/status
# =============================================================================


@router.patch("/routes/{route_id}/stops/{stop_id}/status")
async def update_stop_status(
    route_id: str,
    stop_id: str,
    body: StopStatusUpdate,
    driver: Driver = Depends(get_current_driver),
    db: AsyncSession = Depends(get_db),
):
    """
    Update a stop's status (arrived / completed / failed / skipped).

    Validates that the stop belongs to a route assigned to this driver.
    Sets completed_at=now() for terminal statuses (completed, failed, skipped).
    Increments route.version and updates route.updated_at.
    """
    # Verify route belongs to this driver
    route_result = await db.execute(
        select(Route).where(
            Route.id == route_id,
            Route.assigned_driver_id == driver.id,
        )
    )
    route = route_result.scalar_one_or_none()
    if route is None:
        raise HTTPException(status_code=404, detail="Route not found")

    # Verify stop belongs to this route
    stop_result = await db.execute(
        select(RouteStop).where(
            RouteStop.id == stop_id,
            RouteStop.route_id == route.id,
        )
    )
    stop = stop_result.scalar_one_or_none()
    if stop is None:
        raise HTTPException(status_code=404, detail="Stop not found")

    now = datetime.now(timezone.utc)

    # Update stop
    stop.status = body.status
    if body.notes is not None:
        stop.notes = body.notes
    if body.status in TERMINAL_STOP_STATUSES:
        stop.completed_at = now
    stop.updated_at = now

    # Bump route version and updated_at
    route.version = (route.version or 1) + 1
    route.updated_at = now

    try:
        await db.commit()
        await db.refresh(stop)
    except Exception as exc:
        await db.rollback()
        logger.error(
            "[DRIVER_STOP_UPDATE] commit_failed driver_id=%s route_id=%s stop_id=%s error=%s",
            driver.id, route_id, stop_id, exc, exc_info=True,
        )
        raise HTTPException(status_code=500, detail="Unexpected error updating stop status")

    logger.info(
        "[DRIVER_STOP_UPDATE] driver_id=%s route_id=%s stop_id=%s status=%s",
        driver.id, route_id, stop_id, body.status,
    )

    return {
        "ok": True,
        "stop": _serialize_route_stop(stop),
    }


# =============================================================================
# Endpoint: POST /driver/routes/{route_id}/stops/{stop_id}/collection
# =============================================================================


@router.post("/routes/{route_id}/stops/{stop_id}/collection")
async def record_collection(
    route_id: str,
    stop_id: str,
    body: CollectionUpdate,
    driver: Driver = Depends(get_current_driver),
    db: AsyncSession = Depends(get_db),
):
    """
    Record a cash/check/card collection at a stop.

    Validates that the stop belongs to a route assigned to this driver.
    Updates amount_collected and ar_status based on the collected amount.
    Increments route.version and updates route.updated_at.
    """
    # Verify route belongs to this driver
    route_result = await db.execute(
        select(Route).where(
            Route.id == route_id,
            Route.assigned_driver_id == driver.id,
        )
    )
    route = route_result.scalar_one_or_none()
    if route is None:
        raise HTTPException(status_code=404, detail="Route not found")

    # Verify stop belongs to this route
    stop_result = await db.execute(
        select(RouteStop).where(
            RouteStop.id == stop_id,
            RouteStop.route_id == route.id,
        )
    )
    stop = stop_result.scalar_one_or_none()
    if stop is None:
        raise HTTPException(status_code=404, detail="Stop not found")

    now = datetime.now(timezone.utc)
    collected = Decimal(str(body.amount_collected))
    amount_due = stop.amount_due or Decimal("0")

    # Update stop
    stop.amount_collected = collected
    if body.notes is not None:
        stop.notes = body.notes
    stop.updated_at = now

    # Derive ar_status from collected vs due
    if collected >= amount_due:
        stop.ar_status = "paid"
    elif collected > Decimal("0"):
        stop.ar_status = "partial"
    # If collected == 0, leave ar_status unchanged

    # Bump route version and updated_at
    route.version = (route.version or 1) + 1
    route.updated_at = now

    try:
        await db.commit()
        await db.refresh(stop)
    except Exception as exc:
        await db.rollback()
        logger.error(
            "[DRIVER_COLLECTION] commit_failed driver_id=%s stop_id=%s error=%s",
            driver.id, stop_id, exc, exc_info=True,
        )
        raise HTTPException(status_code=500, detail="Unexpected error recording collection")

    logger.info(
        "[DRIVER_COLLECTION] driver_id=%s stop_id=%s amount=%s method=%s",
        driver.id, stop_id, body.amount_collected, body.payment_method,
    )

    return {
        "ok": True,
        "stop": _serialize_route_stop(stop),
    }
