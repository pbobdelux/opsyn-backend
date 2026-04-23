"""
auth_utils.py — Driver authentication utilities for Opsyn Backend.

Provides:
- PIN hashing and verification (bcrypt via passlib)
- JWT access token creation and decoding (python-jose)
"""

import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from jose import JWTError, jwt
from passlib.context import CryptContext

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SECRET_KEY: str = os.getenv("JWT_SECRET_KEY", "opsyn-driver-secret-change-in-production")
ALGORITHM: str = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES: int = 60

# ---------------------------------------------------------------------------
# PIN hashing (bcrypt)
# ---------------------------------------------------------------------------

_pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_pin(pin: str) -> str:
    """Hash a plain-text PIN using bcrypt. Returns the hashed string."""
    return _pwd_context.hash(pin)


def verify_pin(pin: str, pin_hash: str) -> bool:
    """Verify a plain-text PIN against a bcrypt hash. Returns True if valid."""
    return _pwd_context.verify(pin, pin_hash)


# ---------------------------------------------------------------------------
# JWT tokens
# ---------------------------------------------------------------------------


def create_access_token(driver_id: str, org_id: str) -> str:
    """
    Create a signed JWT access token for a driver.

    Payload includes:
      - driver_id: str (UUID)
      - org_id: str (UUID)
      - role: "driver"
      - token_type: "access"
      - iat: issued-at (UTC)
      - exp: expiry (UTC, ACCESS_TOKEN_EXPIRE_MINUTES from now)
    """
    now = datetime.now(timezone.utc)
    expire = now + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    payload = {
        "driver_id": driver_id,
        "org_id": org_id,
        "role": "driver",
        "token_type": "access",
        "iat": int(now.timestamp()),
        "exp": int(expire.timestamp()),
    }

    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def decode_token(token: str) -> dict:
    """
    Decode and validate a JWT access token.

    Returns the payload dict on success.
    Raises jose.JWTError on invalid or expired tokens.
    """
    return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
