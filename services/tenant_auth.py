"""
Tenant authentication and authorization for multi-tenant API security.

Every protected endpoint must:
  1. Call get_authenticated_org() to establish tenant identity from headers.
  2. Call verify_tenant_access() to confirm the authenticated org matches
     the org_id in the request path.

All queries must then use the authenticated org_id — never the raw path value.
"""

import hashlib
import hmac
import logging

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import TenantCredential

logger = logging.getLogger("tenant_auth")


def _hash_secret(secret: str) -> str:
    """SHA-256 hex digest of the raw secret.

    Stored secrets are hashed with this function at enrollment time.
    Incoming secrets are hashed before comparison so the raw value is
    never held in memory longer than necessary and never written to logs.
    """
    return hashlib.sha256(secret.encode()).hexdigest()


async def get_authenticated_org(
    x_opsyn_org: str,
    x_opsyn_secret: str,
    db: AsyncSession,
) -> str:
    """Validate X-OPSYN-ORG + X-OPSYN-SECRET headers against the database.

    Returns the authenticated org_id on success.
    Raises HTTP 401 for missing/invalid credentials.
    Raises HTTP 403 for inactive tenants.

    This function is intentionally called with explicit arguments rather than
    as a FastAPI Depends so that callers can pass the already-resolved db
    session and header values — keeping the dependency graph flat and making
    the auth call explicit and auditable at each endpoint.
    """
    if not x_opsyn_org or not x_opsyn_secret:
        logger.warning("tenant_auth: missing auth headers")
        raise HTTPException(status_code=401, detail="Missing authentication headers")

    result = await db.execute(
        select(TenantCredential).where(TenantCredential.org_id == x_opsyn_org)
    )
    credential: TenantCredential | None = result.scalar_one_or_none()

    if credential is None:
        logger.warning("tenant_auth: invalid credentials for org_id=%s", x_opsyn_org)
        raise HTTPException(status_code=401, detail="Invalid credentials")

    # Constant-time comparison to prevent timing attacks
    incoming_hash = _hash_secret(x_opsyn_secret)
    if not hmac.compare_digest(incoming_hash, credential.api_secret):
        logger.warning("tenant_auth: invalid credentials for org_id=%s", x_opsyn_org)
        raise HTTPException(status_code=401, detail="Invalid credentials")

    if not credential.is_active:
        logger.warning("tenant_auth: inactive tenant org_id=%s", x_opsyn_org)
        raise HTTPException(status_code=403, detail="Tenant account is inactive")

    logger.info("tenant_auth: org_id=%s authenticated successfully", x_opsyn_org)
    return credential.org_id


def verify_tenant_access(authenticated_org_id: str, requested_org_id: str) -> None:
    """Assert that the authenticated tenant matches the path org_id.

    Raises HTTP 403 on mismatch — never 404, to avoid leaking the existence
    of other tenants' resources.
    """
    if authenticated_org_id != requested_org_id:
        logger.warning(
            "tenant_auth: org mismatch - authenticated=%s requested=%s",
            authenticated_org_id,
            requested_org_id,
        )
        raise HTTPException(status_code=403, detail="Access forbidden")
