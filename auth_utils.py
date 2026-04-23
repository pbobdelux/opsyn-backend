"""
auth_utils.py — PIN hashing and JWT token utilities for Opsyn driver auth.
"""
import os
import secrets
from datetime import datetime, timedelta, timezone

import bcrypt
import jwt

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
JWT_SECRET: str = os.getenv("JWT_SECRET") or secrets.token_hex(32)
JWT_ALGORITHM: str = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES: int = 60
REFRESH_TOKEN_EXPIRE_DAYS: int = 7


# ---------------------------------------------------------------------------
# PIN hashing
# ---------------------------------------------------------------------------

def hash_pin(pin: str) -> str:
    """Hash a PIN using bcrypt with 12 salt rounds. Returns the hash string."""
    salt = bcrypt.gensalt(rounds=12)
    hashed = bcrypt.hashpw(pin.encode("utf-8"), salt)
    return hashed.decode("utf-8")


def verify_pin(pin: str, pin_hash: str) -> bool:
    """Verify a plain-text PIN against a bcrypt hash. Returns True if valid."""
    try:
        return bcrypt.checkpw(pin.encode("utf-8"), pin_hash.encode("utf-8"))
    except Exception:
        return False


# ---------------------------------------------------------------------------
# JWT tokens
# ---------------------------------------------------------------------------

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def create_access_token(driver_id: str, org_id: str) -> str:
    """
    Create a short-lived JWT access token for a driver.

    Payload includes: driver_id, org_id, role, token_type, exp, iat.
    """
    now = _utc_now()
    payload = {
        "driver_id": driver_id,
        "org_id": org_id,
        "role": "driver",
        "token_type": "access",
        "iat": now,
        "exp": now + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def create_refresh_token(driver_id: str) -> str:
    """
    Create a long-lived JWT refresh token for a driver.

    Payload includes: driver_id, token_type, exp, iat.
    """
    now = _utc_now()
    payload = {
        "driver_id": driver_id,
        "token_type": "refresh",
        "iat": now,
        "exp": now + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def decode_token(token: str) -> dict:
    """
    Decode and verify a JWT token.

    Raises jwt.ExpiredSignatureError if expired.
    Raises jwt.InvalidTokenError for any other validation failure.
    Returns the decoded payload dict.
    """
    return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
